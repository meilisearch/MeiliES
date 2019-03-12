use crate::resp::{RespValue, FromResp, RespIntConvertError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventNumber(pub u64);

impl FromResp for EventNumber {
    type Error = RespIntConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        i64::from_resp(value).map(|i| EventNumber(i as u64))
    }
}
