use std::string::FromUtf8Error;
use crate::codec::RespValue;

pub trait FromResp: Sized {
    type Error;
    fn from_resp(value: RespValue) -> Result<Self, Self::Error>;
}

impl FromResp for RespValue {
    type Error = (); // FIXME replace with never (!)

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        Ok(value)
    }
}

#[derive(Debug)]
pub enum RespStringConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
}

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
