use std::ptr::{copy_nonoverlapping, read_unaligned};
use std::string::FromUtf8Error;
use std::fmt;
use std::convert::TryInto;


use rand::Rng;

use crate::resp::{RespValue, FromResp, RespStringConvertError};


#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRef {
    event_number: u64,
    snapshot_hash: u64
}

impl SnapshotRef {
    pub fn new(event_number: u64) -> SnapshotRef {
        let mut rng = rand::thread_rng();
        let snapshot_hash: u64 = rng.gen();
        SnapshotRef { event_number, snapshot_hash}
    }

    pub fn to_be_bytes(self) -> [u8; 16] {
        let mut dest = [0u8; 16];
        let event_number_bytes = self.event_number.to_be_bytes();
        let snapshot_hash_bytes = self.snapshot_hash.to_be_bytes();

        unsafe {
            let dest = dest.as_mut_ptr() as *mut [u8; 8];
            let src = event_number_bytes.as_ptr() as *const [u8; 8];
            copy_nonoverlapping(src, dest, 1);

            let dest = dest.add(1);
            let src = snapshot_hash_bytes.as_ptr() as *const [u8; 8];
            copy_nonoverlapping(src, dest, 1);
        }

        dest
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> SnapshotRef {
        let event_number_bytes;
        let snapshot_hash_bytes;

        unsafe {
            let bytes = bytes.as_ptr() as *const [u8; 8];
            event_number_bytes = read_unaligned(bytes);
            let bytes = bytes.add(1);
            snapshot_hash_bytes = read_unaligned(bytes);
        }

        let event_number = u64::from_be_bytes(event_number_bytes);
        let snapshot_hash = u64::from_be_bytes(snapshot_hash_bytes);

        SnapshotRef { event_number, snapshot_hash }
    }
}

impl Into<RespValue> for SnapshotRef {
    fn into(self) -> RespValue {
        RespValue::BulkString(self.to_be_bytes().to_vec())
    }
}

#[derive(Debug)]
pub enum RespSnapshotRefConvertError {
    InvalidRespType,
    InvalidUtf8String(FromUtf8Error),
    InnerSnapshotRefConvertError(core::array::TryFromSliceError),
}

impl fmt::Display for RespSnapshotRefConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RespSnapshotRefConvertError::*;
        match self {
            InvalidRespType => write!(f, "invalid RESP type found, expected String"),
            InvalidUtf8String(e) => write!(f, "invalid UTF8 string; {}", e),
            InnerSnapshotRefConvertError(e) => write!(f, "inner SnapshotRef convert error: {}", e),
        }
    }
}

impl std::error::Error for RespSnapshotRefConvertError {}

impl FromResp for SnapshotRef {
    type Error = RespSnapshotRefConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespSnapshotRefConvertError::*;
        match String::from_resp(value) {
            Ok(string) => {
                let slice: &[u8] = string.as_ref();
                let array = slice.try_into().map_err(|e| RespSnapshotRefConvertError::InnerSnapshotRefConvertError(e))?;
                Ok(SnapshotRef::from_be_bytes(array))
            },
            Err(RespStringConvertError::InvalidRespType) => Err(InvalidRespType),
            Err(RespStringConvertError::InvalidUtf8String(error)) => Err(InvalidUtf8String(error)),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_ref() {
        let snap = SnapshotRef {
            event_number: u64::min_value(),
            snapshot_hash: u64::max_value()
        };

        let snap_bytes = snap.to_be_bytes();
        let snap2 = SnapshotRef::from_be_bytes(snap_bytes);

        assert_eq!(snap, snap2);
    }

    #[test]
    fn snapshot_ref_min() {
        let snap = SnapshotRef {
            event_number: u64::min_value(),
            snapshot_hash: u64::min_value()
        };

        let snap_bytes = snap.to_be_bytes();
        let snap2 = SnapshotRef::from_be_bytes(snap_bytes);

        assert_eq!(snap, snap2);
    }

    #[test]
    fn snapshot_ref_max() {
        let snap = SnapshotRef {
            event_number: u64::max_value(),
            snapshot_hash: u64::max_value()
        };

        let snap_bytes = snap.to_be_bytes();
        let snap2 = SnapshotRef::from_be_bytes(snap_bytes);

        assert_eq!(snap, snap2);
    }
}
