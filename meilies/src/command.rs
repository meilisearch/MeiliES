use std::{fmt, str, string};
use crate::codec::RespValue;

pub fn arguments_from_resp_value(value: RespValue) -> Result<Vec<Vec<u8>>, ()> {
    match value {
        RespValue::Array(Some(array)) => {
            let mut args = Vec::new();

            for value in array {
                match value {
                    RespValue::BulkString(Some(buffer)) => {
                        args.push(buffer);
                    },
                    _ => return Err(()),
                }
            }

            Ok(args)
        },
        _ => Err(())
    }
}

pub enum Command {
    Publish { stream: String, event: Vec<u8> },
    Subscribe { stream: String },
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
            Command::Subscribe { stream } => {
                fmt.debug_struct("Subscribe")
                    .field("stream", &stream)
                    .finish()
            }
        }
    }
}

#[derive(Debug)]
pub enum CommandError {
    CommandNotFound,
    MissingCommandName,
    InvalidNumberOfArguments { expected: usize },
    InvalidUtf8String(str::Utf8Error),
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

impl Command {
    pub fn from_args(mut args: Vec<Vec<u8>>) -> Result<Command, CommandError> {
        let mut args = args.drain(..);

        let command = match args.next() {
            Some(command) => str::from_utf8(&command)?.to_lowercase(),
            None => return Err(CommandError::MissingCommandName),
        };

        match command.as_str() {
            "publish" => {
                match (args.next(), args.next(), args.next()) {
                    (Some(stream), Some(event), None) => {
                        let stream = String::from_utf8(stream)?;
                        Ok(Command::Publish { stream, event })
                    },
                    _ => Err(CommandError::InvalidNumberOfArguments { expected: 2 })
                }
            },
            "subscribe" => {
                match (args.next(), args.next()) {
                    (Some(stream), None) => {
                        let stream = String::from_utf8(stream)?;
                        Ok(Command::Subscribe { stream })
                    },
                    _ => Err(CommandError::InvalidNumberOfArguments { expected: 2 })
                }
            },
            _ => Err(CommandError::CommandNotFound),
        }
    }
}
