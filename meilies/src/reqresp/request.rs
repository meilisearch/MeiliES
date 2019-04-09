use std::fmt;
use crate::stream::{Stream, StartReadFrom, StreamName, EventData, EventName};
use crate::stream::ALL_STREAMS;
use crate::resp::{RespValue, FromResp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    SubscribeAll { from: StartReadFrom },
    Subscribe { streams: Vec<Stream> },
    Publish { stream: StreamName, event_name: EventName, event_data: EventData },
    LastEventNumber { stream: StreamName },
    StreamNames,
}

impl Into<RespValue> for Request {
    fn into(self) -> RespValue {
        match self {
            Request::SubscribeAll { from } => {
                let command = RespValue::bulk_string(&"subscribe"[..]);
                let all = Stream::all(from).into();
                RespValue::Array(vec![command, all])
            },
            Request::Subscribe { streams } => {
                let command = RespValue::bulk_string(&"subscribe"[..]);
                let streams = streams.into_iter().map(Into::into);
                let args = Some(command).into_iter().chain(streams).collect();
                RespValue::Array(args)
            },
            Request::Publish { stream, event_name, event_data } => {
                RespValue::Array(vec![
                    RespValue::bulk_string(&"publish"[..]),
                    RespValue::bulk_string(stream.to_string()),
                    RespValue::bulk_string(event_name.to_string()),
                    RespValue::bulk_string(event_data.0),
                ])
            },
            Request::LastEventNumber { stream } => {
                RespValue::Array(vec![
                    RespValue::bulk_string(&"last-event-number"[..]),
                    RespValue::bulk_string(stream.to_string()),
                ])
            },
            Request::StreamNames => {
                RespValue::Array(vec![
                    RespValue::bulk_string(&"stream-names"[..]),
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
            _otherwise => return Err(InvalidCommandRespType),
        };

        let command = iter.next().map(String::from_resp)
            .ok_or(MissingCommandName)?
            .map_err(|_| InvalidArgumentRespType)?;

        match command.as_str() {
            "subscribe" => {
                let streams: Result<Vec<_>, _> = iter.map(Stream::from_resp).collect();
                let streams = streams.map_err(|_| InvalidArgumentRespType)?;

                if let Some(stream) = streams.iter().find(|s| s.name == ALL_STREAMS) {
                    return Ok(Request::SubscribeAll { from: stream.from })
                }

                Ok(Request::Subscribe { streams })
            },
            "publish" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                let event_name = iter.next().map(EventName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                let event_data = iter.next().map(EventData::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Request::Publish { stream, event_name, event_data })
            },
            "last-event-number" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Request::LastEventNumber { stream })
            },
            "stream-names" => {
                Ok(Request::StreamNames)
            }
            _otherwise => Err(UnknownCommandName),
        }
    }
}
