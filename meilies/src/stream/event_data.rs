use crate::resp::{FromResp, RespBytesConvertError, RespValue};
use std::{fmt, str};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventData(pub Vec<u8>);

impl fmt::Debug for EventData {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let event = &self.0;
        let mut dbg = fmt.debug_tuple("EventData");
        match str::from_utf8(event) {
            Ok(event) => dbg.field(&event),
            Err(_) => dbg.field(event),
        };
        dbg.finish()
    }
}

impl FromResp for EventData {
    type Error = RespBytesConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::SimpleString(string) => Ok(EventData(string.into_bytes())),
            RespValue::Error(string) => Ok(EventData(string.into_bytes())),
            RespValue::BulkString(bytes) => Ok(EventData(bytes)),
            _ => Err(RespBytesConvertError::InvalidRespType),
        }
    }
}
