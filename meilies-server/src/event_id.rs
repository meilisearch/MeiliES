use std::{fmt, mem};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventId([u8; 8]);

impl EventId {
    pub fn from_raw(slice: &[u8]) -> Result<EventId, ()> {
        if slice.len() != 8 { return Err(()) }

        let ptr = slice.as_ptr() as *const [u8; 8];
        unsafe { Ok(EventId(*ptr)) }
    }
}

impl From<u64> for EventId {
    fn from(id: u64) -> EventId {
        EventId(unsafe { mem::transmute(id.to_be()) })
    }
}

impl Into<u64> for EventId {
    fn into(self) -> u64 {
        u64::from_be(unsafe { mem::transmute(self.0) })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inverse() {
        for x in 0..=100_000 {
            let x2 = EventId::from(x).into();
            assert_eq!(x, x2);
        }
    }
}
