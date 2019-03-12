use crate::stream::{Stream, EventNumber};
use crate::event_data::EventData;
use crate::resp::{RespValue, FromResp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    SubscribedTo(Vec<Stream>),
    Event(Stream, EventNumber, EventData),
}

#[derive(Debug)]
pub enum RespMessageConvertError {
    InvalidMessageType(String),
    InvalidRespValue(String),
    MissingMessageElement,
}

impl FromResp for Message {
    type Error = RespMessageConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespMessageConvertError::*;

        let mut args = match Vec::<RespValue>::from_resp(value) {
            Ok(args) => args.into_iter(),
            Err(e) => return Err(InvalidRespValue(format!("invalid type found, expected Array"))),
        };

        let value = args.next().ok_or(MissingMessageElement)?;
        let message_type = String::from_resp(value)
            .map_err(|e| InvalidRespValue(e.to_string()))?;

        match message_type.as_str() {
            "subscribed" => {
                let value = args.next().ok_or(MissingMessageElement)?;
                let streams = Vec::<Stream>::from_resp(value)
                    .map_err(|e| InvalidRespValue(e.to_string()))?;

                Ok(Message::SubscribedTo(streams))
            },
            "event" => {
                let value = args.next().ok_or(MissingMessageElement)?;
                let stream = Stream::from_resp(value)
                    .map_err(|e| InvalidRespValue(e.to_string()))?;

                let value = args.next().ok_or(MissingMessageElement)?;
                let event_number = EventNumber::from_resp(value)
                    .map_err(|e| InvalidRespValue(e.to_string()))?;

                let value = args.next().ok_or(MissingMessageElement)?;
                let event = Vec::<u8>::from_resp(value)
                    .map_err(|e| InvalidRespValue(e.to_string()))?;

                Ok(Message::Event(stream, event_number, EventData(event)))
            },
            _unknown => Err(InvalidMessageType(message_type)),
        }
    }
}

impl Into<RespValue> for Message {
    fn into(self) -> RespValue {
        match self {
            Message::SubscribedTo(streams) => {
                RespValue::Array(vec![
                    RespValue::string("subscribed"),
                    RespValue::Array(streams.into_iter().map(RespValue::string).collect()),
                ])
            },
            Message::Event(stream, EventNumber(event_number), EventData(value)) => {
                RespValue::Array(vec![
                    RespValue::string("event"),
                    RespValue::string(stream),
                    RespValue::Integer(event_number as i64),
                    RespValue::bulk_string(value),
                ])
            }
        }
    }
}
