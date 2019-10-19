use std::fmt;
use std::str::FromStr;
use std::string::FromUtf8Error;

use crate::resp::{FromResp, RespStringConvertError, RespValue};

pub const ALL_STREAMS: &str = "$all";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(String);

impl StreamName {
    pub fn all() -> StreamName {
        StreamName(String::from(ALL_STREAMS))
    }

    pub fn new(name: String) -> Result<StreamName, StreamNameError> {
        if name.is_empty() {
            return Err(StreamNameError::EmptyName);
        }

        if name.contains(':') {
            return Err(StreamNameError::ContainColon);
        }

        Ok(StreamName(name))
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_bytes()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<[u8]> for StreamName {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug)]
pub enum RespStreamNameConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
    InnerStreamNameConvertError(StreamNameError),
}

impl fmt::Display for RespStreamNameConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespStreamNameConvertError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type found, expected String"),
            InvalidUtf8String(e) => write!(f, "invalid UTF8 string; {}", e),
            InnerStreamNameConvertError(e) => write!(f, "inner StreamName convert error: {}", e),
        }
    }
}

impl FromResp for StreamName {
    type Error = RespStreamNameConvertError;
    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespStreamNameConvertError::*;
        match String::from_resp(value) {
            Ok(string) => StreamName::from_str(&string).map_err(InnerStreamNameConvertError),
            Err(RespStringConvertError::InvalidRespType) => Err(InvalidRespType),
            Err(RespStringConvertError::InvalidUtf8String(error)) => Err(InvalidUtf8String(error)),
        }
    }
}

impl FromStr for StreamName {
    type Err = StreamNameError;

    fn from_str(s: &str) -> Result<StreamName, Self::Err> {
        StreamName::new(s.to_owned())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StreamNameError {
    EmptyName,
    ContainColon,
}

impl fmt::Display for StreamNameError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StreamNameError::EmptyName => f.write_str("stream name is empty"),
            StreamNameError::ContainColon => f.write_str("stream name contains a colon (:)"),
        }
    }
}

impl PartialEq<&'_ str> for StreamName {
    fn eq(&self, other: &&'_ str) -> bool {
        self.0.eq(other)
    }
}
