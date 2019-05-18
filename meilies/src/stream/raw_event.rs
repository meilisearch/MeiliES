use std::error::Error;

use super::{EventName, EventData};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawEvent<T>(pub T);

impl<T: AsRef<[u8]>> RawEvent<T> {
    pub fn new(content: T) -> RawEvent<T> {
        RawEvent(content)
    }

    fn name_size(&self) -> usize {
        // FIXME: prefer using TryFrom
        let mut event_name_size: [u8; 8] = [0; 8];
        for (i, b) in self.0.as_ref().iter().enumerate() {
            if i == 8 {
                break
            }
            event_name_size[i] = *b;
        }
        usize::from_be_bytes(event_name_size)
    }

    // FIXME: Prefer using a typed Error
    pub fn name(&self) -> Result<EventName, Box<Error>> {
        let name_size = self.name_size();
        let raw_name = &self.0.as_ref()[8..(8 + name_size)];
        let name = String::from_utf8(raw_name.to_owned())?;

        Ok(EventName::new(name)?)
    }

    pub fn data(&self) -> EventData {
        let name_size = self.name_size();
        let raw_name = &self.0.as_ref()[(8 + name_size)..];

        EventData(raw_name.to_owned())
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for RawEvent<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
