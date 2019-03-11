use std::str::FromStr;
use std::{fmt, str, string};

use crate::resp::{RespValue, FromResp};
use crate::stream::{Stream, StreamName, StreamNameError, ParseStreamError};

pub enum Command {
    Publish { stream: StreamName, event: Vec<u8> },
    Subscribe { streams: Vec<Stream> },
}

impl fmt::Debug for Command {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Command::Publish { stream, event } => {
                let mut dbg = fmt.debug_struct("Publish");
                dbg.field("stream", &stream);
                match str::from_utf8(&event) {
                    Ok(event) => dbg.field("event", &event),
                    Err(_) => dbg.field("event", &event),
                };
                dbg.finish()
            },
            Command::Subscribe { streams } => {
                fmt.debug_struct("Subscribe")
                    .field("streams", &streams)
                    .finish()
            }
        }
    }
}

impl FromResp for Command {
    type Error = CommandError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        let mut args = match Vec::<Vec<u8>>::from_resp(value) {
            Ok(args) => args,
            Err(e) => return Err(CommandError::InvalidRespType),
        };

        let mut args = args.drain(..);

        let command = match args.next() {
            Some(command) => str::from_utf8(&command)?.to_lowercase(),
            None => return Err(CommandError::MissingCommandName),
        };

        match command.as_str() {
            "publish" => {
                match (args.next(), args.next(), args.next()) {
                    (Some(stream), Some(event), None) => {
                        let text = str::from_utf8(&stream)?;
                        let stream = StreamName::from_str(text)?;
                        Ok(Command::Publish { stream, event })
                    },
                    _ => Err(CommandError::InvalidNumberOfArguments { expected: 2 })
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
            _ => Err(CommandError::CommandNotFound),
        }
    }
}

#[derive(Debug)]
pub enum CommandError {
    InvalidRespType,
    InvalidStream(ParseStreamError),
    CommandNotFound,
    MissingCommandName,
    InvalidNumberOfArguments { expected: usize },
    InvalidUtf8String(str::Utf8Error),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CommandError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type, expected array of bulk string"),
            InvalidStream(e) => write!(f, "invalid stream; {}", e),
            CommandNotFound => write!(f, "command not found"),
            MissingCommandName => write!(f, "missing command name"),
            InvalidNumberOfArguments { expected } => {
                write!(f, "invalid number of arguments (expected {})", expected)
            },
            InvalidUtf8String(error) => write!(f, "invalid utf8 string: {}", error),
        }
    }
}

impl From<str::Utf8Error> for CommandError {
    fn from(error: str::Utf8Error) -> CommandError {
        CommandError::InvalidUtf8String(error)
    }
}

impl From<string::FromUtf8Error> for CommandError {
    fn from(error: string::FromUtf8Error) -> CommandError {
        CommandError::InvalidUtf8String(error.utf8_error())
    }
}

impl From<ParseStreamError> for CommandError {
    fn from(error: ParseStreamError) -> CommandError {
        CommandError::InvalidStream(error)
    }
}

impl From<StreamNameError> for CommandError {
    fn from(error: StreamNameError) -> CommandError {
        CommandError::InvalidStream(ParseStreamError::StreamNameError(error))
    }
}
