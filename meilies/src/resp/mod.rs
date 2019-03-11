use std::{str, fmt};

mod codec;
mod from_resp;

pub use self::codec::{RespCodec, RespMsgError};
pub use self::from_resp::{
    FromResp,
    RespStringConvertError,
    RespIntConvertError,
    RespBytesConvertError,
    RespVecConvertError,
};

#[derive(Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    pub fn string(string: impl fmt::Display) -> RespValue {
        RespValue::SimpleString(string.to_string())
    }

    pub fn error(string: impl fmt::Display) -> RespValue {
        RespValue::Error(string.to_string())
    }

    pub fn bulk_string(string: impl Into<Vec<u8>>) -> RespValue {
        RespValue::BulkString(string.into())
    }
}

impl PartialEq<&'_ str> for RespValue {
    fn eq(&self, other: &&'_ str) -> bool {
        match self {
            RespValue::SimpleString(string) => string == other,
            RespValue::Error(error) => error == other,
            RespValue::BulkString(bytes) => bytes.as_slice() == other.as_bytes(),
            _ => false,
        }
    }
}

impl PartialEq<str> for RespValue {
    fn eq(&self, other: &str) -> bool {
        self == &other
    }
}

impl PartialEq<String> for RespValue {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

impl fmt::Debug for RespValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RespValue::SimpleString(string) => {
                fmt.debug_tuple("SimpleString").field(&string).finish()
            },
            RespValue::Error(string) => {
                fmt.debug_tuple("Error").field(&string).finish()
            },
            RespValue::Integer(integer) => {
                fmt.debug_tuple("Integer").field(&integer).finish()
            },
            RespValue::BulkString(value) => {
                let mut dbg = fmt.debug_tuple("BulkString");

                match (value, str::from_utf8(value)) {
                    (_, Ok(value)) => dbg.field(&value),
                    (value, Err(_)) => dbg.field(&value),
                };

                dbg.finish()
            },
            RespValue::Array(elements) => {
                fmt.debug_tuple("Array").field(&elements).finish()
            },
            RespValue::Nil => {
                fmt.debug_tuple("Nil").finish()
            },
        }
    }
}
