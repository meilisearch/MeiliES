use std::{fmt, str, string};

pub enum Command {
    Publish { stream: String, event: Vec<u8> },
    Subscribe { streams: Vec<(String, i64)> },
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

#[derive(Debug)]
pub enum CommandError {
    InvalidStreamName,
    CommandNotFound,
    MissingCommandName,
    InvalidNumberOfArguments { expected: usize },
    InvalidUtf8String(str::Utf8Error),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommandError::InvalidStreamName => {
                write!(f, "invalid stream name")
            },
            CommandError::CommandNotFound => {
                write!(f, "command not found")
            },
            CommandError::MissingCommandName => {
                write!(f, "missing command name")
            },
            CommandError::InvalidNumberOfArguments { expected } => {
                write!(f, "invalid number of arguments (expected {})", expected)
            },
            CommandError::InvalidUtf8String(error) => {
                write!(f, "invalid utf8 string: {}", error)
            },
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
                        if stream.contains(&b':') {
                            return Err(CommandError::InvalidStreamName)
                        }

                        let stream = String::from_utf8(stream)?;
                        Ok(Command::Publish { stream, event })
                    },
                    _ => Err(CommandError::InvalidNumberOfArguments { expected: 2 })
                }
            },
            "subscribe" => {
                let mut streams = Vec::new();
                for mut stream in args {
                    match stream.iter().position(|c| *c == b':') {
                        Some(colon_offset) => {
                            let from = stream.split_off(colon_offset + 1);
                            stream.pop(); // remove the colon itself

                            let stream = String::from_utf8(stream)?;

                            let from = str::from_utf8(&from)?;
                            let from = i64::from_str_radix(from, 10).unwrap();

                            streams.push((stream, from));
                        },
                        None => {
                            let stream = String::from_utf8(stream)?;
                            streams.push((stream, -1));
                        }
                    }
                }
                Ok(Command::Subscribe { streams })
            },
            _ => Err(CommandError::CommandNotFound),
        }
    }
}
