use std::{fmt, num, str};

use bytes::{BufMut, BytesMut};
use subslice::SubsliceExt;
use tokio::codec::{Encoder, Decoder};
use tokio::io;

use super::RespValue;

const CRLF_NEWLINE: &[u8; 2] = &[b'\r', b'\n'];
const SIMPLE_STRING_CHAR:  u8 = b'+';
const ERROR_CHAR:          u8 = b'-';
const INTEGER_CHAR:        u8 = b':';
const BULK_STRING_CHAR:    u8 = b'$';
const ARRAY_CHAR:          u8 = b'*';

#[derive(Debug)]
pub enum RespMsgError {
    InvalidPrefixByte(u8),
    InvalidInteger(num::ParseIntError),
    InvalidUtf8String(str::Utf8Error),
    SimpleStringContainCrlf,
    MissingBulkStringFinalCrlf,
    IoError(io::Error),
}

impl fmt::Display for RespMsgError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use RespMsgError::*;
        match self {
            InvalidPrefixByte(byte) => write!(fmt, "invalid prefix byte: {:?}", byte),
            InvalidInteger(error) => write!(fmt, "invalid integer: {}", error),
            InvalidUtf8String(error) => write!(fmt, "invalid utf8 string: {}", error),
            SimpleStringContainCrlf => write!(fmt, "simple string contain crlf"),
            MissingBulkStringFinalCrlf => write!(fmt, "missing bulk string final crlf"),
            IoError(error) => write!(fmt, "io error: {}", error),
        }
    }
}

impl std::error::Error for RespMsgError {}

impl From<io::Error> for RespMsgError {
    fn from(error: io::Error) -> RespMsgError {
        RespMsgError::IoError(error)
    }
}

impl From<io::ErrorKind> for RespMsgError {
    fn from(error: io::ErrorKind) -> RespMsgError {
        RespMsgError::IoError(error.into())
    }
}

impl From<num::ParseIntError> for RespMsgError {
    fn from(error: num::ParseIntError) -> RespMsgError {
        RespMsgError::InvalidInteger(error)
    }
}

impl From<str::Utf8Error> for RespMsgError {
    fn from(error: str::Utf8Error) -> RespMsgError {
        RespMsgError::InvalidUtf8String(error)
    }
}

fn decode_until_crlf(buf: &[u8]) -> Option<&[u8]> {
    buf.find(CRLF_NEWLINE).map(|off| buf.split_at(off).0)
}

fn decode_simple_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string)?;
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::SimpleString(string.to_owned()), advance)))
        },
        None => Ok(None),
    }
}

fn decode_error(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string)?;
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::Error(string.to_owned()), advance)))
        },
        None => Ok(None),
    }
}

fn decode_integer(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string)?;
            let integer = i64::from_str_radix(string, 10)?;
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::Integer(integer), advance)))
        },
        None => Ok(None),
    }
}

fn decode_bulk_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string)?;
            let length = i64::from_str_radix(string, 10)?;

            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            let buf = &buf[advance..];

            match length {
                len if len < 0 => Ok(Some((RespValue::Nil, advance))),
                _ => {
                    // FIXME handle overflows !!!
                    if buf.len() as i64 >= length + CRLF_NEWLINE.len() as i64 {
                        let bytes = match decode_until_crlf(buf) {
                            Some(bytes_string) => bytes_string.to_vec(),
                            None => return Err(RespMsgError::MissingBulkStringFinalCrlf),
                        };

                        let advance = advance + bytes.len() + CRLF_NEWLINE.len();
                        Ok(Some((RespValue::BulkString(bytes), advance)))

                    }
                    else {
                        Ok(None)
                    }
                },
            }
        },
        None => Ok(None),
    }
}

fn decode_array(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string)?;
            let length = i64::from_str_radix(string, 10)?;

            let mut advance = bytes_string.len() + CRLF_NEWLINE.len();

            match length {
                len if len < 0 => Ok(Some((RespValue::Nil, advance))),
                _ => {
                    let mut array = Vec::with_capacity(length as usize);
                    for _ in 0..length {
                        match decode_message(&buf[advance..]) {
                            Ok(Some((msg, adv))) => {
                                array.push(msg);
                                advance += adv;
                            },
                            Ok(None) => return Ok(None),
                            Err(e) => return Err(e),
                        }
                    }

                    Ok(Some((RespValue::Array(array), advance)))
                },
            }
        },
        None => Ok(None),
    }
}

fn decode_message(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    if buf.is_empty() { return Ok(None) }

    let result = match buf[0] {
        SIMPLE_STRING_CHAR => decode_simple_string(&buf[1..]),
        ERROR_CHAR         => decode_error(&buf[1..]),
        INTEGER_CHAR       => decode_integer(&buf[1..]),
        BULK_STRING_CHAR   => decode_bulk_string(&buf[1..]),
        ARRAY_CHAR         => decode_array(&buf[1..]),
        invalid_byte       => Err(RespMsgError::InvalidPrefixByte(invalid_byte)),
    };

    match result {
        Ok(Some((msg, advance))) => Ok(Some((msg, advance + 1))),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Default)]
pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = RespMsgError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match decode_message(buf) {
            Ok(Some((msg, advance))) => {
                buf.split_to(advance);
                Ok(Some(msg))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder for RespCodec {
    type Item = RespValue;
    type Error = RespMsgError;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match msg {
            RespValue::SimpleString(string) => {
                if string.as_bytes().find(CRLF_NEWLINE).is_some() {
                    return Err(RespMsgError::SimpleStringContainCrlf)
                }

                buf.reserve(1 + string.len() + CRLF_NEWLINE.len());

                buf.put_u8(SIMPLE_STRING_CHAR);
                buf.put(string);
                buf.put(&CRLF_NEWLINE[..]);

                Ok(())
            },
            RespValue::Error(string) => {
                if string.as_bytes().find(CRLF_NEWLINE).is_some() {
                    return Err(RespMsgError::SimpleStringContainCrlf)
                }

                buf.reserve(1 + string.len() + CRLF_NEWLINE.len());

                buf.put_u8(ERROR_CHAR);
                buf.put(string);
                buf.put(&CRLF_NEWLINE[..]);

                Ok(())
            },
            RespValue::Integer(integer) => {
                let integer_string = integer.to_string();
                buf.reserve(1 + integer_string.len() + CRLF_NEWLINE.len());

                buf.put_u8(INTEGER_CHAR);
                buf.put(integer_string);
                buf.put(&CRLF_NEWLINE[..]);

                Ok(())
            },
            RespValue::BulkString(bytes_string) => {
                let length = bytes_string.len();
                let integer_string = length.to_string();
                buf.reserve(1 + integer_string.len() + length + CRLF_NEWLINE.len() * 2);

                buf.put_u8(BULK_STRING_CHAR);
                buf.put(integer_string);
                buf.put(&CRLF_NEWLINE[..]);
                buf.put(bytes_string);
                buf.put(&CRLF_NEWLINE[..]);

                Ok(())
            },
            RespValue::Array(array) => {
                let length = array.len();
                let integer_string = length.to_string();
                buf.reserve(1 + integer_string.len() + CRLF_NEWLINE.len());

                buf.put_u8(ARRAY_CHAR);
                buf.put(integer_string);
                buf.put(&CRLF_NEWLINE[..]);

                for msg in array {
                    self.encode(msg, buf)?;
                }

                Ok(())
            },
            RespValue::Nil => {
                // We chose to use the Bulk String to represent nil values.
                let integer_string = "-1";
                buf.reserve(1 + integer_string.len() + CRLF_NEWLINE.len());

                buf.put_u8(BULK_STRING_CHAR);
                buf.put(integer_string);
                buf.put(&CRLF_NEWLINE[..]);

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_simple_string() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::SimpleString("kiki".to_owned());
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_error() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Error("whoops, it is and error".to_owned());
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_integer() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Integer(12);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let mut buf = BytesMut::new();

        let inmsg = RespValue::Integer(-10);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_bulk_string() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::BulkString(vec![]);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let mut buf = BytesMut::new();

        let inmsg = RespValue::BulkString(vec![1, 2, 3, 4, 5, 35, 70]);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_array() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Array(vec![]);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let inmsg = RespValue::Array(vec![RespValue::BulkString(b"hello".to_vec())]);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let inmsg = RespValue::Array(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(b"hello".to_vec()),
            RespValue::Array(vec![RespValue::Integer(45)]),
        ]);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_nil() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Nil;
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_simple_string() {
        let mut buf = BytesMut::new();

        let inmsg1 = RespValue::SimpleString("kiki".to_owned());
        let inmsg2 = RespValue::SimpleString("kiki".to_owned());
        let inmsg3 = RespValue::SimpleString("kiki".to_owned());

        RespCodec.encode(inmsg1.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg2.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg3.clone(), &mut buf).unwrap();

        let outmsg1 = RespCodec.decode(&mut buf).unwrap();
        let outmsg2 = RespCodec.decode(&mut buf).unwrap();
        let outmsg3 = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg1), outmsg1);
        assert_eq!(Some(inmsg2), outmsg2);
        assert_eq!(Some(inmsg3), outmsg3);
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_error() {
        let mut buf = BytesMut::new();

        let inmsg1 = RespValue::Error("whoops, it is and error".to_owned());
        let inmsg2 = RespValue::Error("another error".to_owned());
        let inmsg3 = RespValue::Error("again and again, another one".to_owned());

        RespCodec.encode(inmsg1.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg2.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg3.clone(), &mut buf).unwrap();

        let outmsg1 = RespCodec.decode(&mut buf).unwrap();
        let outmsg2 = RespCodec.decode(&mut buf).unwrap();
        let outmsg3 = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg1), outmsg1);
        assert_eq!(Some(inmsg2), outmsg2);
        assert_eq!(Some(inmsg3), outmsg3);
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_integer() {
        let mut buf = BytesMut::new();

        let inmsg1 = RespValue::Integer(12);
        let inmsg2 = RespValue::Integer(-50);
        let inmsg3 = RespValue::Integer(2535);

        RespCodec.encode(inmsg1.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg2.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg3.clone(), &mut buf).unwrap();

        let outmsg1 = RespCodec.decode(&mut buf).unwrap();
        let outmsg2 = RespCodec.decode(&mut buf).unwrap();
        let outmsg3 = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg1), outmsg1);
        assert_eq!(Some(inmsg2), outmsg2);
        assert_eq!(Some(inmsg3), outmsg3);
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_bulk_string() {
        let mut buf = BytesMut::new();

        let inmsg1 = RespValue::BulkString(vec![8, 7, 6, 5, 4]);
        let inmsg2 = RespValue::BulkString(vec![1, 2, 3, 4, 5, 35, 70]);
        let inmsg3 = RespValue::BulkString(vec![]);

        RespCodec.encode(inmsg1.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg2.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg3.clone(), &mut buf).unwrap();

        let outmsg1 = RespCodec.decode(&mut buf).unwrap();
        let outmsg2 = RespCodec.decode(&mut buf).unwrap();
        let outmsg3 = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg1), outmsg1);
        assert_eq!(Some(inmsg2), outmsg2);
        assert_eq!(Some(inmsg3), outmsg3);
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_mixed() {
        let mut buf = BytesMut::new();

        let inmsg1 = RespValue::SimpleString("kiki".to_owned());
        let inmsg2 = RespValue::Error("whoops, it is and error".to_owned());
        let inmsg3 = RespValue::Integer(12);
        let inmsg4 = RespValue::BulkString(vec![8, 7, 6, 5, 4]);
        let inmsg5 = RespValue::BulkString(vec![1, 2, 3, 4, 5, 35, 70]);
        let inmsg6 = RespValue::Array(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(b"hello".to_vec()),
            RespValue::Array(vec![RespValue::Integer(45)]),
        ]);

        RespCodec.encode(inmsg1.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg2.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg3.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg4.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg5.clone(), &mut buf).unwrap();
        RespCodec.encode(inmsg6.clone(), &mut buf).unwrap();

        let outmsg1 = RespCodec.decode(&mut buf).unwrap();
        let outmsg2 = RespCodec.decode(&mut buf).unwrap();
        let outmsg3 = RespCodec.decode(&mut buf).unwrap();
        let outmsg4 = RespCodec.decode(&mut buf).unwrap();
        let outmsg5 = RespCodec.decode(&mut buf).unwrap();
        let outmsg6 = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg1), outmsg1);
        assert_eq!(Some(inmsg2), outmsg2);
        assert_eq!(Some(inmsg3), outmsg3);
        assert_eq!(Some(inmsg4), outmsg4);
        assert_eq!(Some(inmsg5), outmsg5);
        assert_eq!(Some(inmsg6), outmsg6);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_simple_string() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::SimpleString("kiki".to_owned());

        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();

        let buf2 = buf.split_off(2);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(None, outmsg);

        buf.unsplit(buf2);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_bulk_string() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::BulkString(vec![1, 2, 3, 4, 5, 35, 70]);

        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();

        let buf2 = buf.split_off(5);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(None, outmsg);

        buf.unsplit(buf2);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_array() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Array(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(b"hello".to_vec()),
            RespValue::Array(vec![RespValue::Integer(45)]),
        ]);

        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();

        let buf2 = buf.split_off(15);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(None, outmsg);

        buf.unsplit(buf2);
        let buf2 = buf.split_off(32);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(None, outmsg);

        buf.unsplit(buf2);
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }
}
