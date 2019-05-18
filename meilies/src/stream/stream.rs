use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

use crate::resp::{RespValue, FromResp, RespStringConvertError};
use crate::stream::{StreamName, StreamNameError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadPosition {
    EventNumber(u64),
    End,
}

impl ReadPosition {
    pub fn map<F: FnOnce(u64) -> u64>(self, f: F) -> ReadPosition {
        match self {
            ReadPosition::EventNumber(number) => ReadPosition::EventNumber(f(number)),
            ReadPosition::End => ReadPosition::End,
        }
    }
}

impl Into<Option<u64>> for ReadPosition {
    fn into(self) -> Option<u64> {
        match self {
            ReadPosition::EventNumber(number) => Some(number),
            ReadPosition::End => None,
        }
    }
}

impl From<Option<u64>> for ReadPosition {
    fn from(option: Option<u64>) -> ReadPosition {
        match option {
            Some(number) => ReadPosition::EventNumber(number),
            None => ReadPosition::End,
        }
    }
}

impl From<u64> for ReadPosition {
    fn from(number: u64) -> ReadPosition {
        ReadPosition::EventNumber(number)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stream {
    pub name: StreamName,
    pub from: ReadPosition,
    pub to: Option<u64>,
}

impl Stream {
    pub fn all(from: ReadPosition, to: Option<u64>) -> Stream {
        Stream::new(StreamName::all(), from, to)
    }

    pub fn new(name: StreamName, from: ReadPosition, to: Option<u64>) -> Stream {
        Stream { name, from, to }
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream(\"{}\")", self)
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (self.to, self.from) {
            (Some(to), ReadPosition::EventNumber(from)) => write!(f, "{}:{}:{}", self.name, from, to),
            (None, ReadPosition::EventNumber(from)) => write!(f, "{}:{}", self.name, from),
            _ => write!(f, "{}", self.name),
        }
    }
}

impl Into<RespValue> for Stream {
    fn into(self) -> RespValue {
        let text = match (self.to, self.from) {
            (Some(to), ReadPosition::EventNumber(from)) => format!("{}:{}:{}", self.name, from, to),
            (None, ReadPosition::EventNumber(from)) => format!("{}:{}", self.name, from),
            _ => format!("{}", self.name),
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
        Stream { name, from: ReadPosition::End , to: None}
    }
}

impl FromStr for Stream {
    type Err = ParseStreamError;

    fn from_str(s: &str) -> Result<Stream, Self::Err> {
        use ParseStreamError::*;

        let mut split = s.split(':');
        match (split.next(), split.next(), split.next(), split.next()) {
            (Some(name), None, None, None) => {
                let name = StreamName::from_str(name).map_err(StreamNameError)?;
                Ok(Stream::from(name))
            },
            (Some(name), Some(from), None, None) => {
                let name = StreamName::new(name.to_owned()).map_err(StreamNameError)?;
                let number = u64::from_str_radix(from, 10).map_err(StartFromError)?;
                Ok(Stream { name, from: ReadPosition::EventNumber(number), to: None})
            },
            (Some(name), Some(from), Some(to), None) => {
                let name = StreamName::new(name.to_owned()).map_err(StreamNameError)?;
                let from = u64::from_str_radix(from, 10).map_err(StartFromError)?;
                let to = u64::from_str_radix(to, 10).map_err(EndToError)?;
                if from >= to {
                    return Err(BoundsError);
                }
                Ok(Stream { name, from: ReadPosition::EventNumber(from), to: Some(to)})
            },
            (_, _, _, _) => Err(FormatError),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseStreamError {
    StreamNameError(StreamNameError),
    StartFromError(ParseIntError),
    EndToError(ParseIntError),
    BoundsError,
    FormatError,
}

impl fmt::Display for ParseStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ParseStreamError::*;

        match self {
            StreamNameError(e) => write!(f, "stream not properly formatted; {}", e),
            StartFromError(e) => write!(f, "stream \"start from\" not properly formatted; {}", e),
            EndToError(e) => write!(f, "stream \"end to\" not properly formatted; {}", e),
            BoundsError => f.write_str("The end bound cannot be before or equal the start bound"),
            FormatError => f.write_str("stream is not properly formatted"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_from_str() {
        let test_stream1 = Stream::from_str("default").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadPosition::End, None);
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:0").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadPosition::EventNumber(0), None);
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadPosition::EventNumber(5), None);
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:0:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadPosition::EventNumber(0), Some(5));
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:1:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadPosition::EventNumber(1), Some(5));
        assert_eq!(test_stream1, test_stream2);


        let result = Stream::from_str("default:");
        assert!(result.is_err());

        let result = Stream::from_str("default:-1");
        assert!(result.is_err());

        let result = Stream::from_str("default::0");
        assert!(result.is_err());

        let result = Stream::from_str("default:0:");
        assert!(result.is_err());

        let result = Stream::from_str("default:0:");
        assert!(result.is_err());

        let result = Stream::from_str("default:0:-1");
        assert!(result.is_err());

        let result = Stream::from_str("default:0:0");
        assert!(result.is_err());

        let result = Stream::from_str("default:1:0");
        assert!(result.is_err());
    }


}
