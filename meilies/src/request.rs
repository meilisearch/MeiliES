use std::fmt;
use crate::stream::{Stream, StreamName, EventData};
use crate::resp::{RespValue, FromResp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Subscribe { streams: Vec<Stream> },
    Publish { stream: StreamName, event: EventData },
}

impl Into<RespValue> for Request {
    fn into(self) -> RespValue {
        match self {
            Request::Subscribe { streams } => {
                let command = RespValue::bulk_string(&"subscribe"[..]);
                let streams = streams.into_iter().map(Into::into);
                let args = Some(command).into_iter().chain(streams).collect();
                RespValue::Array(args)
            },
            Request::Publish { stream, event } => {
                RespValue::Array(vec![
                    RespValue::bulk_string(&"publish"[..]),
                    RespValue::bulk_string(stream.to_string()),
                    RespValue::bulk_string(event.0),
                ])
            }
        }
    }
}

#[derive(Debug)]
pub enum RespRequestConvertError {
    InvalidCommandRespType,
    InvalidArgumentRespType,
    MissingCommandName,
    UnknownCommandName,
    MissingArgument,
    TooManyArguments,
}

impl fmt::Display for RespRequestConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespRequestConvertError::*;
        match self {
            InvalidCommandRespType => write!(f, "Invalid command resp type"),
            InvalidArgumentRespType => write!(f, "Invalid argument resp type"),
            MissingCommandName => write!(f, "Missing command name"),
            UnknownCommandName => write!(f, "Unknown command name"),
            MissingArgument => write!(f, "Missing argument"),
            TooManyArguments => write!(f, "Too many arguments"),
        }
    }
}

impl FromResp for Request {
    type Error = RespRequestConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespRequestConvertError::*;

        let mut iter = match value {
            RespValue::Array(array) => array.into_iter(),
            otherwise => return Err(InvalidCommandRespType),
        };

        let command = iter.next().map(String::from_resp)
            .ok_or(MissingCommandName)?
            .map_err(|_| InvalidArgumentRespType)?;

        match command.as_str() {
            "subscribe" => {
                let streams: Result<_, _> = iter.map(Stream::from_resp).collect();
                let streams = streams.map_err(|_| InvalidArgumentRespType)?;
                Ok(Request::Subscribe { streams })
            },
            "publish" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                let event = iter.next().map(EventData::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Request::Publish { stream, event })
            },
            _otherwise => Err(UnknownCommandName),
        }
    }
}
