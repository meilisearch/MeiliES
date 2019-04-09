use std::fmt;
use crate::stream::{StreamName, EventNumber, EventData, EventName};
use crate::resp::{RespValue, FromResp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Subscribed { stream: StreamName },
    Event { stream: StreamName, number: EventNumber, event_name: EventName, event_data: EventData },
    LastEventNumber { stream: StreamName, number: Option<EventNumber> },
    StreamNames { streams: Vec<StreamName> },
}

impl Into<RespValue> for Response {
    fn into(self) -> RespValue {
        match self {
            Response::Ok => {
                RespValue::string("OK")
            },
            Response::Subscribed { stream } => {
                RespValue::Array(vec![
                    RespValue::string("subscribed"),
                    RespValue::string(stream),
                ])
            },
            Response::Event { stream, number, event_name, event_data } => {
                RespValue::Array(vec![
                    RespValue::string("event"),
                    RespValue::string(stream),
                    RespValue::Integer(number.0 as i64),
                    RespValue::string(event_name),
                    RespValue::bulk_string(event_data.0),
                ])
            },
            Response::LastEventNumber { stream, number } => {
                let number = match number {
                    Some(number) => RespValue::Integer(number.0 as i64),
                    None => RespValue::Nil,
                };

                RespValue::Array(vec![
                    RespValue::string("last-event-number"),
                    RespValue::string(stream),
                    number,
                ])
            },
            Response::StreamNames { streams } => {
                let command = RespValue::string("stream-names");
                let streams = streams.into_iter().map(|s| RespValue::SimpleString(s.into_inner()));
                let args = Some(command).into_iter().chain(streams).collect();
                RespValue::Array(args)
            }
        }
    }
}

#[derive(Debug)]
pub enum RespResponseConvertError {
    InvalidResponseRespType,
    InvalidArgumentRespType,
    MissingTypeName,
    UnknownTypeName,
    MissingArgument,
    TooManyArguments,
}

impl fmt::Display for RespResponseConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespResponseConvertError::*;
        match self {
            InvalidResponseRespType => write!(f, "Invalid response resp type"),
            InvalidArgumentRespType => write!(f, "Invalid argument resp type"),
            MissingTypeName => write!(f, "Missing type name"),
            UnknownTypeName => write!(f, "Unknown type name"),
            MissingArgument => write!(f, "Missing argument"),
            TooManyArguments => write!(f, "Too many arguments"),
        }
    }
}

impl FromResp for Response {
    type Error = RespResponseConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespResponseConvertError::*;

        let mut iter = match value {
            RespValue::SimpleString(ref text) if text == "OK" => return Ok(Response::Ok),
            RespValue::Array(array) => array.into_iter(),
            _otherwise => return Err(InvalidResponseRespType),
        };

        let response_type = iter.next().map(String::from_resp)
            .ok_or(MissingTypeName)?
            .map_err(|_| InvalidArgumentRespType)?;

        match response_type.as_str() {
            "subscribed" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Response::Subscribed { stream })
            },
            "event" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                let number = iter.next().map(EventNumber::from_resp)
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

                Ok(Response::Event { stream, number, event_name, event_data })
            },
            "last-event-number" => {
                let stream = iter.next().map(StreamName::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                let number = iter.next().map(FromResp::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Response::LastEventNumber { stream, number })
            },
            "stream-names" => {
                match iter.map(StreamName::from_resp).collect() {
                    Ok(streams) => Ok(Response::StreamNames { streams }),
                    Err(_) => Err(InvalidArgumentRespType),
                }
            }
            _otherwise => Err(UnknownTypeName),
        }
    }
}
