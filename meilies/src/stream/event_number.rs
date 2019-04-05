use std::convert::TryFrom;
use core::array::TryFromSliceError;
use crate::resp::{RespValue, FromResp, RespIntConvertError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventNumber(pub u64);

impl EventNumber {
    pub const fn zero() -> EventNumber {
        EventNumber(0)
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> EventNumber {
        EventNumber(u64::from_be_bytes(bytes))
    }

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub const fn next(self) -> EventNumber {
        EventNumber(self.0 + 1)
    }
}

impl TryFrom<&[u8]> for EventNumber {
    type Error = TryFromSliceError;

    fn try_from(slice: &[u8]) -> Result<EventNumber, Self::Error> {
        TryFrom::try_from(slice).map(EventNumber::from_be_bytes)
    }
}

impl FromResp for EventNumber {
    type Error = RespIntConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        i64::from_resp(value).map(|i| EventNumber(i as u64))
    }
}
