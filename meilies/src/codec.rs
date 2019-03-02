use std::str;

use bytes::{BufMut, BytesMut};
use subslice::SubsliceExt;
use tokio::codec::{Encoder, Decoder};
use tokio::io;

const CRLF_NEWLINE: &[u8; 2] = &[b'\r', b'\n'];
const SIMPLE_STRING_CHAR:  u8 = b'+';
const ERROR_CHAR:          u8 = b'-';
const INTEGER_CHAR:        u8 = b':';
const BULK_STRING_CHAR:    u8 = b'$';
const ARRAY_CHAR:          u8 = b'*';

// For Simple Strings the first byte of the reply is "+"
// For Errors the first byte of the reply is "-"
// For Integers the first byte of the reply is ":"
// For Bulk Strings the first byte of the reply is "$"
// For Arrays the first byte of the reply is "*"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

#[derive(Debug)]
pub enum RespMsgError {
    InvalidPrefixByte(u8),
    SimpleStringContainCrlf,
    MissingBulkStringFinalCrlf,
    IoError(io::Error),
}

impl From<io::Error> for RespMsgError {
    fn from(error: io::Error) -> RespMsgError {
        RespMsgError::IoError(error)
    }
}

fn decode_until_crlf(buf: &[u8]) -> Option<&[u8]> {
    buf.find(CRLF_NEWLINE).map(|off| buf.split_at(off).0)
}

fn decode_simple_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string).unwrap().to_owned();
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::SimpleString(string), advance)))
        },
        None => Ok(None),
    }
}

fn decode_error(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string).unwrap().to_owned();
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::Error(string), advance)))
        },
        None => Ok(None),
    }
}

fn decode_integer(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string).unwrap();
            let integer = i64::from_str_radix(string, 10).unwrap();
            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            Ok(Some((RespValue::Integer(integer), advance)))
        },
        None => Ok(None),
    }
}

fn decode_bulk_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespMsgError> {
    match decode_until_crlf(buf) {
        Some(bytes_string) => {
            let string = str::from_utf8(bytes_string).unwrap();
            let length = i64::from_str_radix(string, 10).unwrap();

            let advance = bytes_string.len() + CRLF_NEWLINE.len();
            let buf = &buf[advance..];

            match length {
                len if len < 0 => Ok(Some((RespValue::BulkString(None), advance))),
                _ => {
                    // FIXME handle overflows !!!
                    if buf.len() as i64 >= length + CRLF_NEWLINE.len() as i64 {
                        let bytes = match decode_until_crlf(buf) {
                            Some(bytes_string) => bytes_string.to_vec(),
                            None => return Err(RespMsgError::MissingBulkStringFinalCrlf),
                        };

                        let advance = advance + bytes.len() + CRLF_NEWLINE.len();
                        Ok(Some((RespValue::BulkString(Some(bytes)), advance)))

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
            let string = str::from_utf8(bytes_string).unwrap();
            let length = i64::from_str_radix(string, 10).unwrap();

            let mut advance = bytes_string.len() + CRLF_NEWLINE.len();

            match length {
                len if len < 0 => Ok(Some((RespValue::Array(None), advance))),
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

                    Ok(Some((RespValue::Array(Some(array)), advance)))
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

#[derive(Default)]
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
                match bytes_string {
                    Some(bytes_string) => {
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
                    None => {
                        let integer_string = "-1";
                        buf.reserve(1 + integer_string.len() + CRLF_NEWLINE.len());

                        buf.put_u8(BULK_STRING_CHAR);
                        buf.put(integer_string);
                        buf.put(&CRLF_NEWLINE[..]);

                        Ok(())
                    }
                }
            },
            RespValue::Array(array) => {
                match array {
                    Some(array) => {
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
                    None => {
                        let integer_string = "-1";
                        buf.reserve(1 + integer_string.len() + CRLF_NEWLINE.len());

                        buf.put_u8(ARRAY_CHAR);
                        buf.put(integer_string);
                        buf.put(&CRLF_NEWLINE[..]);

                        Ok(())
                    }
                }
            },
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

        let inmsg = RespValue::BulkString(None);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let mut buf = BytesMut::new();

        let inmsg = RespValue::BulkString(Some(vec![1, 2, 3, 4, 5, 35, 70]));
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());
    }

    #[test]
    fn one_array() {
        let mut buf = BytesMut::new();

        let inmsg = RespValue::Array(None);
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let mut buf = BytesMut::new();

        let inmsg = RespValue::Array(Some(vec![]));
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let inmsg = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"hello".to_vec()))]));
        RespCodec.encode(inmsg.clone(), &mut buf).unwrap();
        let outmsg = RespCodec.decode(&mut buf).unwrap();

        assert_eq!(Some(inmsg), outmsg);
        assert!(buf.is_empty());

        let inmsg = RespValue::Array(Some(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(Some(b"hello".to_vec())),
            RespValue::Array(Some(vec![RespValue::Integer(45)])),
        ]));
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

        let inmsg1 = RespValue::BulkString(None);
        let inmsg2 = RespValue::BulkString(Some(vec![1, 2, 3, 4, 5, 35, 70]));
        let inmsg3 = RespValue::BulkString(Some(vec![]));

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
        let inmsg4 = RespValue::BulkString(None);
        let inmsg5 = RespValue::BulkString(Some(vec![1, 2, 3, 4, 5, 35, 70]));
        let inmsg6 = RespValue::Array(Some(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(Some(b"hello".to_vec())),
            RespValue::Array(Some(vec![RespValue::Integer(45)])),
        ]));

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

        let inmsg = RespValue::BulkString(Some(vec![1, 2, 3, 4, 5, 35, 70]));

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

        let inmsg = RespValue::Array(Some(vec![
            RespValue::SimpleString("hello".to_owned()),
            RespValue::Error("what the f*ck!".to_owned()),
            RespValue::Integer(25),
            RespValue::BulkString(Some(b"hello".to_vec())),
            RespValue::Array(Some(vec![RespValue::Integer(45)])),
        ]));

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
