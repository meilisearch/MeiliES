use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

use crate::resp::{RespValue, FromResp, RespStringConvertError};
use crate::stream::{StreamName, StreamNameError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StartReadFrom {
    EventNumber(u64),
    End,
}

impl StartReadFrom {
    pub fn map<F: FnOnce(u64) -> u64>(self, f: F) -> StartReadFrom {
        match self {
            StartReadFrom::EventNumber(number) => StartReadFrom::EventNumber(f(number)),
            StartReadFrom::End => StartReadFrom::End,
        }
    }
}

impl Into<Option<u64>> for StartReadFrom {
    fn into(self) -> Option<u64> {
        match self {
            StartReadFrom::EventNumber(number) => Some(number),
            StartReadFrom::End => None,
        }
    }
}

impl From<Option<u64>> for StartReadFrom {
    fn from(option: Option<u64>) -> StartReadFrom {
        match option {
            Some(number) => StartReadFrom::EventNumber(number),
            None => StartReadFrom::End,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stream {
    pub name: StreamName,
    pub from: StartReadFrom,
}

impl Stream {
    pub fn all(from: StartReadFrom) -> Stream {
        Stream::new(StreamName::all(), from)
    }

    pub fn new(name: StreamName, from: StartReadFrom) -> Stream {
        Stream { name, from }
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream(\"{}\")", self)
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.from {
            StartReadFrom::EventNumber(number) => write!(f, "{}:{}", self.name, number),
            StartReadFrom::End => write!(f, "{}", self.name),
        }
    }
}

impl Into<RespValue> for Stream {
    fn into(self) -> RespValue {
        let text = match self.from {
            StartReadFrom::EventNumber(number) => format!("{}:{}", self.name, number),
            StartReadFrom::End => format!("{}", self.name),
        };

        RespValue::BulkString(text.into_bytes())
    }
}

#[derive(Debug)]
pub enum RespStreamConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
    InnerStreamConvertError(ParseStreamError),
}

impl fmt::Display for RespStreamConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespStreamConvertError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type found, expected String"),
            InvalidUtf8String(e) => write!(f, "invalid UTF8 string; {}", e),
            InnerStreamConvertError(e) => write!(f, "inner Stream convert error: {}", e),
        }
    }
}

impl FromResp for Stream {
    type Error = RespStreamConvertError;
    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespStreamConvertError::*;
        match String::from_resp(value) {
            Ok(string) => Stream::from_str(&string).map_err(InnerStreamConvertError),
            Err(RespStringConvertError::InvalidRespType) => Err(InvalidRespType),
            Err(RespStringConvertError::InvalidUtf8String(error)) => Err(InvalidUtf8String(error)),
        }
    }
}

impl From<StreamName> for Stream {
    fn from(name: StreamName) -> Stream {
        Stream { name, from: StartReadFrom::End }
    }
}

impl FromStr for Stream {
    type Err = ParseStreamError;

    fn from_str(s: &str) -> Result<Stream, Self::Err> {
        use ParseStreamError::*;

        let mut split = s.split(':');
        match (split.next(), split.next(), split.next()) {
            (Some(name), None, None) => {
                let name = StreamName::from_str(name).map_err(StreamNameError)?;
                Ok(Stream::from(name))
            },
            (Some(name), Some(from), None) => {
                let name = StreamName::new(name.to_owned()).map_err(StreamNameError)?;
                let number = u64::from_str_radix(from, 10).map_err(StartFromError)?;
                Ok(Stream { name, from: StartReadFrom::EventNumber(number) })
            },
            (_, _, _) => Err(FormatError),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseStreamError {
    StreamNameError(StreamNameError),
    StartFromError(ParseIntError),
    FormatError,
}

impl fmt::Display for ParseStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ParseStreamError::*;

        match self {
            StreamNameError(e) => write!(f, "stream not properly formatted; {}", e),
            StartFromError(e) => write!(f, "stream \"start from\" not properly formatted; {}", e),
            FormatError => f.write_str("stream is not properly formatted"),
        }
    }
}
