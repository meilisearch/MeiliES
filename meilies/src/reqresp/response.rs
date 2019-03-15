use std::fmt;
use crate::stream::{StreamName, EventNumber, EventData};
use crate::resp::{RespValue, FromResp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Subscribed { stream: StreamName },
    Event { stream: StreamName, number: EventNumber, event: EventData },
    LastEventNumber { stream: StreamName, number: Option<EventNumber> },
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
            Response::Event { stream, number, event } => {
                RespValue::Array(vec![
                    RespValue::string("event"),
                    RespValue::string(stream),
                    RespValue::Integer(number.0 as i64),
                    RespValue::bulk_string(event.0),
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

                let event = iter.next().map(EventData::from_resp)
                    .ok_or(MissingArgument)?
                    .map_err(|_| InvalidArgumentRespType)?;

                if iter.next().is_some() {
                    return Err(TooManyArguments)
                }

                Ok(Response::Event { stream, number, event })
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
            }
            _otherwise => Err(UnknownTypeName),
        }
    }
}
