use std::string::FromUtf8Error;
use std::fmt;
use super::RespValue;

pub trait FromResp: Sized {
    type Error;
    fn from_resp(value: RespValue) -> Result<Self, Self::Error>;
}

impl FromResp for RespValue {
    type Error = (); // FIXME replace with never (!)

    fn from_resp(value: RespValue) -> Result<Self, <Self as FromResp>::Error> {
        Ok(value)
    }
}

#[derive(Debug)]
pub enum RespStringConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
}

impl fmt::Display for RespStringConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespStringConvertError::*;
        match self {
            InvalidRespType => {
                write!(f, "invalid RESP type found, expected String, Error or BulkString")
            },
            InvalidUtf8String(e) => write!(f, "invalid UTF8 string; {}", e),
        }
    }
}

impl std::error::Error for RespStringConvertError {}

impl FromResp for String {
    type Error = RespStringConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespStringConvertError::*;
        match value {
            RespValue::SimpleString(string) => Ok(string),
            RespValue::Error(string) => Ok(string),
            RespValue::BulkString(bytes) => String::from_utf8(bytes).map_err(InvalidUtf8String),
            _ => Err(InvalidRespType),
        }
    }
}

#[derive(Debug)]
pub enum RespIntConvertError {
    InvalidRespType,
}

impl fmt::Display for RespIntConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RespIntConvertError::InvalidRespType => {
                write!(f, "invalid RESP type found, expected Integer")
            },
        }
    }
}

impl std::error::Error for RespIntConvertError {}

impl FromResp for i64 {
    type Error = RespIntConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::Integer(integer) => Ok(integer),
            _ => Err(RespIntConvertError::InvalidRespType),
        }
    }
}

#[derive(Debug)]
pub enum RespBytesConvertError {
    InvalidRespType,
}

impl fmt::Display for RespBytesConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RespBytesConvertError::InvalidRespType => {
                write!(f, "invalid RESP type found, expected String, Error or BulkString")
            },
        }
    }
}

impl std::error::Error for RespBytesConvertError {}

impl FromResp for Vec<u8> {
    type Error = RespBytesConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::SimpleString(string) => Ok(string.into_bytes()),
            RespValue::Error(string) => Ok(string.into_bytes()),
            RespValue::BulkString(bytes) => Ok(bytes),
            _ => Err(RespBytesConvertError::InvalidRespType),
        }
    }
}

#[derive(Debug)]
pub enum RespVecConvertError<E> {
    InvalidRespType,
    InnerRespConvertError(E),
}

impl<E: fmt::Display> fmt::Display for RespVecConvertError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespVecConvertError::*;
        match self {
            InvalidRespType => {
                write!(f, "invalid RESP type found, expected Array")
            },
            InnerRespConvertError(e) => {
                write!(f, "inner RESP type convertion error: {}", e)
            },
        }
    }
}

impl<E: fmt::Display + fmt::Debug> std::error::Error for RespVecConvertError<E> {}

impl<T: FromResp> FromResp for Vec<T> {
    type Error = RespVecConvertError<T::Error>;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespVecConvertError::*;
        match value {
            RespValue::Array(array) => {
                let result: Result<Vec<_>, _> = array.into_iter().map(|e| T::from_resp(e)).collect();
                result.map_err(InnerRespConvertError)
            },
            _ => Err(InvalidRespType),
        }
    }
}

impl<T: FromResp> FromResp for Option<T> {
    type Error = T::Error;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::Nil => Ok(None),
            other => T::from_resp(other).map(Some),
        }
    }
}

impl<T: FromResp> FromResp for Result<T, String> {
    type Error = T::Error;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::Error(string) => Ok(Err(string)),
            other => T::from_resp(other).map(Ok),
        }
    }
}
