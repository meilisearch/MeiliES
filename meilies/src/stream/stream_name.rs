use std::fmt;
use std::str::FromStr;

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
