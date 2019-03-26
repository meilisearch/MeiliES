use std::fmt;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventId([u8; 8]);

impl From<u64> for EventId {
    fn from(id: u64) -> EventId {
        EventId(id.to_be_bytes())
    }
}

impl Into<u64> for EventId {
    fn into(self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

impl AsRef<[u8]> for EventId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", Into::<u64>::into(*self))
    }
}
