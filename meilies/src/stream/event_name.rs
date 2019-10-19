use std::fmt;
use std::str::FromStr;
use std::string::FromUtf8Error;

use crate::resp::{FromResp, RespStringConvertError, RespValue};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventName(String);

impl EventName {
    pub fn new(name: String) -> Result<EventName, EventNameError> {
        if name.is_empty() {
            return Err(EventNameError::EmptyName);
        }

        Ok(EventName(name))
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

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub enum RespEventNameConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
    InnerEventNameConvertError(EventNameError),
}

impl fmt::Display for RespEventNameConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespEventNameConvertError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type found, expected String"),
            InvalidUtf8String(e) => write!(f, "invalid UTF8 string; {}", e),
            InnerEventNameConvertError(e) => write!(f, "inner EventName convert error: {}", e),
        }
    }
}

impl FromResp for EventName {
    type Error = RespEventNameConvertError;
    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespEventNameConvertError::*;
        match String::from_resp(value) {
            Ok(string) => EventName::new(string).map_err(InnerEventNameConvertError),
            Err(RespStringConvertError::InvalidRespType) => Err(InvalidRespType),
            Err(RespStringConvertError::InvalidUtf8String(error)) => Err(InvalidUtf8String(error)),
        }
    }
}

impl FromStr for EventName {
    type Err = EventNameError;

    fn from_str(s: &str) -> Result<EventName, Self::Err> {
        EventName::new(s.to_owned())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EventNameError {
    EmptyName,
}

impl fmt::Display for EventNameError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventNameError::EmptyName => f.write_str("Event name is empty"),
        }
    }
}

impl std::error::Error for EventNameError {}
