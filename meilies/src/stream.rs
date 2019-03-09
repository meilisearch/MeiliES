use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventNumber(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(String);

impl StreamName {
    pub fn new(name: String) -> Result<StreamName, StreamNameError> {
        if name.is_empty() {
            return Err(StreamNameError::EmptyName)
        }

        if name.contains(':') {
            return Err(StreamNameError::ContainColon)
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
        f.write_str(self.as_str())
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
            StreamNameError::ContainColon => f.write_str("stream name contain colon (:)"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StartReadFrom {
    EventNumber(u64),
    End,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stream {
    pub name: StreamName,
    pub from: StartReadFrom,
}

impl From<StreamName> for Stream {
    fn from(name: StreamName) -> Stream {
        Stream { name, from: StartReadFrom::End }
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
