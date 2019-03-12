use std::str::FromStr;
use std::{fmt, str, string};

use crate::resp::{RespValue, FromResp};
use crate::stream::{Stream, EventData, StreamName, StreamNameError, ParseStreamError};

#[derive(Debug)]
pub enum Command {
    Publish { stream: StreamName, event: EventData },
    Subscribe { streams: Vec<Stream> },
}

impl Into<RespValue> for Command {
    fn into(self) -> RespValue {
        match self {
            Command::Publish { stream, event } => {
                RespValue::Array(vec![
                    RespValue::bulk_string(&"publish"[..]),
                    RespValue::bulk_string(stream.into_bytes()),
                    RespValue::bulk_string(event.0),
                ])
            },
            Command::Subscribe { streams } => {
                let streams = streams.into_iter().map(|s| RespValue::bulk_string(s.to_string()));
                let command = RespValue::bulk_string(&"subscribe"[..]);
                let args = Some(command).into_iter().chain(streams).collect();

                RespValue::Array(args)
            }
        }
    }
}

#[derive(Debug)]
pub enum RespCommandConvertError {
    InvalidRespType,
    MissingCommandName,
    UnknownCommand(String),
    InvalidStream(ParseStreamError),
    InvalidNumberOfArguments { expected: usize },
    InvalidUtf8String(str::Utf8Error),
}

impl From<str::Utf8Error> for RespCommandConvertError {
    fn from(error: str::Utf8Error) -> RespCommandConvertError {
        RespCommandConvertError::InvalidUtf8String(error)
    }
}

impl From<string::FromUtf8Error> for RespCommandConvertError {
    fn from(error: string::FromUtf8Error) -> RespCommandConvertError {
        RespCommandConvertError::InvalidUtf8String(error.utf8_error())
    }
}

impl From<ParseStreamError> for RespCommandConvertError {
    fn from(error: ParseStreamError) -> RespCommandConvertError {
        RespCommandConvertError::InvalidStream(error)
    }
}

impl From<StreamNameError> for RespCommandConvertError {
    fn from(error: StreamNameError) -> RespCommandConvertError {
        RespCommandConvertError::InvalidStream(ParseStreamError::StreamNameError(error))
    }
}

impl fmt::Display for RespCommandConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespCommandConvertError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type, expected array of bulk string"),
            InvalidStream(e) => write!(f, "invalid stream; {}", e),
            UnknownCommand(command) => write!(f, "command {:?} not found", command),
            MissingCommandName => write!(f, "missing command name"),
            InvalidNumberOfArguments { expected } => {
                write!(f, "invalid number of arguments (expected {})", expected)
            },
            InvalidUtf8String(error) => write!(f, "invalid utf8 string: {}", error),
        }
    }
}

impl FromResp for Command {
    type Error = RespCommandConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespCommandConvertError::*;

        let args = match Vec::<Vec<u8>>::from_resp(value) {
            Ok(args) => args,
            Err(e) => return Err(InvalidRespType),
        };

        Command::from_args(args)
    }
}

impl Command {
    pub fn from_args(args: Vec<Vec<u8>>) -> Result<Command, RespCommandConvertError> {
        use RespCommandConvertError::*;

        let mut args = args.into_iter();

        let command = match args.next() {
            Some(command) => str::from_utf8(&command)?.to_lowercase(),
            None => return Err(MissingCommandName),
        };

        match command.as_str() {
            "publish" => {
                match (args.next(), args.next(), args.next()) {
                    (Some(stream), Some(event), None) => {
                        let text = str::from_utf8(&stream)?;
                        let stream = StreamName::from_str(text)?;
                        Ok(Command::Publish { stream, event: EventData(event) })
                    },
                    _ => Err(InvalidNumberOfArguments { expected: 2 })
                }
            },
            "subscribe" => {
                let mut streams = Vec::new();
                for bytes in args {
                    let text = str::from_utf8(&bytes)?;
                    let stream = Stream::from_str(&text)?;
                    streams.push(stream);
                }
                Ok(Command::Subscribe { streams })
            },
            _unknown => Err(UnknownCommand(command)),
        }
    }
}
