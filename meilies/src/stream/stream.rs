use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

use crate::resp::{RespValue, FromResp, RespStringConvertError};
use crate::stream::{StreamName, StreamNameError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadRange {
   ReadFromUntil(u64, u64),
   ReadFrom(u64),
   ReadFromEnd,
}

impl ReadRange {
    pub fn from(&self) -> Option<u64> {
        match self {
            ReadRange::ReadFromUntil(from, _) | ReadRange::ReadFrom(from) => Some(*from),
            _ => None
        }
    }

    pub fn to(&self) -> Option<u64> {
        match self {
            ReadRange::ReadFromUntil(_, to) => Some(*to),
            _ => None
        }
    }
}

impl fmt::Display for ReadRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReadRange::ReadFromUntil(from, to) => write!(f, ":{}:{}", from, to),
            ReadRange::ReadFrom(from) => write!(f, ":{}", from),
            ReadRange::ReadFromEnd => write!(f, ""),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stream {
    pub name: StreamName,
    pub range: ReadRange,
}

impl Stream {
    pub fn all(range: ReadRange) -> Stream {
        Stream::new(StreamName::all(), range)
    }

    pub fn new(name: StreamName, range: ReadRange) -> Stream {
        Stream { name, range }
    }

    pub fn new_from_to(name: StreamName, from: Option<u64>, to: Option<u64>) -> Stream {
        let range = match (from, to) {
            (Some(from), Some(to)) => ReadRange::ReadFromUntil(from, to),
            (Some(from), None) => ReadRange::ReadFrom(from),
            (_, _) => ReadRange::ReadFromEnd,
        };
        Stream { name, range }
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream(\"{}\")", self)
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.range {
            ReadRange::ReadFromUntil(from, to) => write!(f, "{}:{}:{}", self.name, from, to),
            ReadRange::ReadFrom(from) => write!(f, "{}:{}", self.name, from),
            ReadRange::ReadFromEnd => write!(f, "{}", self.name),
        }
    }
}

impl Into<RespValue> for Stream {
    fn into(self) -> RespValue {
        let text = match self.range {
            ReadRange::ReadFromUntil(from, to) => format!("{}:{}:{}", self.name, from, to),
            ReadRange::ReadFrom(from) => format!("{}:{}", self.name, from),
            ReadRange::ReadFromEnd => format!("{}", self.name),
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
        Stream { name, range: ReadRange::ReadFromEnd }
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
                Ok(Stream { name, range: ReadRange::ReadFrom(number)})
            },
            (Some(name), Some(from), Some(to), None) => {
                let name = StreamName::new(name.to_owned()).map_err(StreamNameError)?;
                let from = u64::from_str_radix(from, 10).map_err(StartFromError)?;
                let to = u64::from_str_radix(to, 10).map_err(EndToError)?;
                if from >= to {
                    return Err(BoundsError);
                }
                Ok(Stream { name, range: ReadRange::ReadFromUntil(from, to) })
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
            BoundsError => f.write_str("The end bound must be greater than the start bound"),
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
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadRange::ReadFromEnd);
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:0").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadRange::ReadFrom(0));
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadRange::ReadFrom(5));
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:0:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadRange::ReadFromUntil(0, 5));
        assert_eq!(test_stream1, test_stream2);

        let test_stream1 = Stream::from_str("default:1:5").unwrap();
        let test_stream2 = Stream::new(StreamName::new("default".to_owned()).unwrap(), ReadRange::ReadFromUntil(1, 5));
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
