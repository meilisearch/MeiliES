use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use log::{info};
use sled::{Db, Tree, IVec, Event, ConfigBuilder, Result};
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::stream::{EventNumber, RawEvent, StreamName, EventData, SnapshotRef};
use meilies::reqresp::Response;

const LAST_SNAPSHOT_REF_KEY: &str = "last_snapshot_ref";
const LAST_SNAPSHOTED_MUMBER_KEY: &str = "last_snapshoted_number";
const LAST_EVENT_MUMBER_KEY: &str = "last_event_number";

#[derive(Clone)]
pub struct StoreConfig {
    compression_factor: Option<i32>,
    snapshot_min_frequency: Option<usize>,
}

pub struct StoreConfigBuilder(StoreConfig);

impl StoreConfigBuilder {
    pub fn new() -> StoreConfigBuilder {
        let config = StoreConfig {
            compression_factor: None,
            snapshot_min_frequency: None,
        };
        StoreConfigBuilder(config)
    }

    pub fn compression_factor(mut self, num: i32) -> StoreConfigBuilder {
        self.0.compression_factor = Some(num);
        self
    }

    pub fn snapshot_min_frequency(mut self, num: usize) -> StoreConfigBuilder {
        self.0.snapshot_min_frequency = Some(num);
        self
    }

    pub fn build(self) -> StoreConfig {
        self.0
    }
}

#[derive(Clone)]
pub struct StreamStore {
    database: Db,
    config: StoreConfig,
}

impl StreamStore {
    pub fn new<P: AsRef<Path>>(db_path: P, config: StoreConfig) -> Result<StreamStore> {
        let mut builder = ConfigBuilder::new().path(db_path);

        if let Some(compression_factor) = config.compression_factor {
            builder = builder.use_compression(true).compression_factor(compression_factor);
        }

        let database = Db::start(builder.build())?;

        Ok(StreamStore { database, config })
    }

    /// Get the list of all streams names.
    pub fn get_stream_names(&self) -> Vec<String> {
        self.database.tree_names()
            .into_iter()
            .filter_map(|n| String::from_utf8(n).ok())
            .filter(|n| n.as_str().starts_with("event_"))
            .map(|n| n.split_at(7).1.to_string())
            .collect()
    }

    fn get_event_tree(&self, stream_name: &str) -> Result<Arc<Tree>> {
        let stream_name = format!("event_{}", stream_name);
        self.database.open_tree(stream_name.into_bytes())
    }

    fn get_snapshot_tree(&self, stream_name: &str) -> Result<Arc<Tree>> {
        let stream_name = format!("snapshot_{}", stream_name);
        self.database.open_tree(stream_name.into_bytes())
    }

    fn get_info_tree(&self, stream_name: &str) -> Result<Arc<Tree>> {
        let stream_name = format!("info_{}", stream_name);
        self.database.open_tree(stream_name.into_bytes())
    }

    /// Get the number of last event received for a stream.
    pub fn last_event_number(&self, stream_name: &str) -> Result<Option<u64>> {
        let info = self.get_info_tree(stream_name)?;
        let result = match info.get(LAST_EVENT_MUMBER_KEY)? {
            Some(v) => v,
            None => return Ok(None)
        };

        let array = match result.as_ref().try_into() {
            Ok(v) => v,
            Err(_) => return Ok(None)
        };

        let number = u64::from_be_bytes(array);
        Ok(Some(number))
    }

    /// Get the number of last event snapshoted.
    ///
    /// This could be possible only if the snapshot function is called, so only
    /// if snapshot options are set
    pub fn last_snapshoted_number(&self, stream_name: &str) -> Result<Option<u64>> {
        let info = self.get_info_tree(stream_name)?;
        let result = match info.get(LAST_SNAPSHOTED_MUMBER_KEY)? {
            Some(v) => v,
            None => return Ok(None)
        };

        let array = match result.as_ref().try_into() {
            Ok(v) => v,
            Err(_) => return Ok(None)
        };

        let number = u64::from_be_bytes(array);
        Ok(Some(number))
    }

    pub fn last_snapshoted_ref(&self, stream_name: &str) -> Result<Option<SnapshotRef>> {
        let info = self.get_info_tree(stream_name)?;
        let result = match info.get(LAST_SNAPSHOT_REF_KEY)? {
            Some(v) => v,
            None => return Ok(None)
        };

        let array = match result.as_ref().try_into() {
            Ok(v) => v,
            Err(_) => return Ok(None)
        };

        Ok(Some(SnapshotRef::from_be_bytes(array)))
    }

    /// Set the number of last event received for a stream.
    fn update_last_event_number(
        &self,
        stream_name: &str,
        number: u64
    ) -> Result<()> {
        let info_tree = self.get_info_tree(stream_name)?;
        info_tree.set(LAST_EVENT_MUMBER_KEY, &number.to_be_bytes())?;
        Ok(())
    }

    /// Set the number of last event snapshoted.
    ///
    /// This could be possible only if the snapshot function is called, so only
    /// if snapshot options are set
    fn update_last_snapshot_number(
        &self,
        stream_name: &str,
        number: u64
    ) -> Result<()> {
        let info_tree = self.get_info_tree(stream_name)?;
        info_tree.set(LAST_SNAPSHOTED_MUMBER_KEY, &number.to_be_bytes())?;
        Ok(())
    }

    /// Set the number of last event snapshoted.
    ///
    /// This could be possible only if the snapshot function is called, so only
    /// if snapshot options are set
    fn update_last_snapshot_ref(
        &self,
        stream_name: &str,
        snapshot_ref: SnapshotRef,
    ) -> Result<()> {
        let info_tree = self.get_info_tree(stream_name)?;
        info_tree.set(LAST_SNAPSHOT_REF_KEY, &snapshot_ref.to_be_bytes())?;
        Ok(())
    }

    /// Send the last snapshot saved.
    ///
    /// Will return `Ok(None)` if they are no snapshot saved yet. Return an
    ///`u64` witch is the last event number on snapshot. Use this number to
    /// continue to retrieve events after snapshot.
    // @todo Replace sender by return iterators
    // @body Do not take the sender as parametter but respond an Iterator over
    // the `tree.range()` Iterator
    pub fn send_last_snapshot(
        &self,
        stream_name: &str,
        mut sender: mpsc::Sender<std::result::Result<Response, String>>
    ) -> std::result::Result<Option<u64>, Box<Error>> {
        info!("send_last_snapshot - stream_name: {}", stream_name);

        let number = match self.last_snapshoted_number(stream_name)? {
            Some(v) => v,
            None => return Ok(None),
        };
        info!("send_last_snapshot - last_snapshot_number: {}", number);
        let snapshot_tree = match self.get_snapshot_tree(stream_name) {
            Ok(v) => v,
            Err(_) => return Ok(None)
        };
        let stream_name = StreamName::new(stream_name.to_string())?;
        match snapshot_tree.get(number.to_be_bytes())? {
            Some(snap) => {
                let snapshot = Response::Snapshot {
                    stream: stream_name.clone(),
                    number: EventNumber(number),
                    data: EventData(snap.to_vec()),
                };
                match sender.send(Ok(snapshot)).wait() {
                    Ok(s) => sender = s,
                    Err(err) => {
                        info!("encountered closed channel");
                        return Err(err.into());
                    }
                }
            },
            None => return Ok(None)
        }
        Ok(Some(number))
    }

    /// Send all event since the number passed as parmeter.
    /// Will send events one by one. Will return a `Ok(None)` if the stream does
    /// not exist.  Return an `u64` witch is the last event number on events.
    // - Will return `Some(x)` if the end range is not reached.
    // - Will return `None` if the end range is reached.
    // @todo Replace sender by return iterators
    // @body Do not take the sender as parametter but respond an Iterator over
    // the `tree.range()` Iterator
    pub fn send_event(
        &self,
        stream_name: &str,
        from: u64,
        to: Option<u64>,
        mut sender: mpsc::Sender<std::result::Result<Response, String>>
    ) -> std::result::Result<Option<u64>, Box<Error>> {
        info!("send_event_since - stream_name: {} - from: {} to: {:?}", stream_name, from, to);
        let mut number = EventNumber(from);
        let from_number = EventNumber(from);
        let to_number = match to {
            Some(to) => EventNumber(to),
            None => EventNumber(u64::max_value()),
        };
        let event_tree = self.get_event_tree(stream_name)?;
        let stream_name = StreamName::new(stream_name.to_string())?;

        for result in event_tree.range(from_number.to_be_bytes()..to_number.to_be_bytes()) {
            let (key, value) = result?;
            info!("send_event_since - key: {:?}", key);
            number = EventNumber::try_from(key.as_slice())?;
            info!("send_event_since - EventNumber: {:?}", number);

            if let Some(to) = to {
                if from >= to {
                    return Ok(None)
                }
            }

            let raw_event = RawEvent::new(value);
            let event = Response::Event {
                stream: stream_name.clone(),
                number,
                event_name: raw_event.name()?,
                event_data: raw_event.data(),
            };

            match sender.send(Ok(event)).wait() {
                Ok(s) => sender = s,
                Err(err) => {
                    info!("encountered closed channel");
                    return Err(err.into());
                }
            }
        }
        Ok(Some(number.0))
    }

    /// Subscribe to a stream.
    ///
    /// Will send all new events received on this stream.
    /// Warn: This process is blocking you should put in on a thread
    // @todo Replace sender by return iterators
    // @body Do not take the sender as parametter but respond an Iterator over
    // the `tree.watch_prefix()` Iterator
    // @todo Use the Futures to no be blocking
    // @body Wait that sled implement Future and use it to not block the thread
    pub fn send_subscribed(
        &self,
        stream_name: &str,
        from: u64,
        to: Option<u64>,
        mut sender: mpsc::Sender<std::result::Result<Response, String>>
    ) -> std::result::Result<(), Box<Error>> {
        info!("send_subscribed - stream_name: {} - from: {} to: {:?}", stream_name, from, to);
        let from_number = EventNumber(from);
        let event_tree = self.get_event_tree(stream_name)?;
        let stream_name = StreamName::new(stream_name.to_string())?;
        let watcher = event_tree.watch_prefix(vec![]);
        for event in watcher {
            if let Event::Set(key, value) = event {
                let number = EventNumber::try_from(key.as_slice())?;
                if let Some(to) = to {
                    if from_number.0 >= to {
                        return Ok(())
                    }
                }
                if number >= from_number {
                    let raw_event = RawEvent::new(value);
                    let event = Response::Event {
                        stream: stream_name.clone(),
                        number,
                        event_name: raw_event.name()?,
                        event_data: raw_event.data(),
                    };

                    match sender.send(Ok(event)).wait() {
                        Ok(s) => sender = s,
                        Err(err) => {
                            info!("encountered closed channel");
                            return Err(err.into());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn subscribe_to(
        &self,
        stream_name: &str,
        from: u64,
        to: Option<u64>,
        sender: mpsc::Sender<std::result::Result<Response, String>>
    ) -> std::result::Result<(), Box<Error>> {
        let sender_sub = sender.clone();
        if let Some(from) = self.send_event(stream_name, from, to, sender)? {
            self.send_subscribed(stream_name, from, to, sender_sub)?;
        }
        Ok(())
    }

    /// Add new event on stream.
    ///
    /// Will create a snapshot and depreciate all events if proper config are
    /// set.
    pub fn save_event(
        &self,
        stream_name: &str,
        event_name: &str,
        event_data: Vec<u8>,
    ) -> std::result::Result<(), Box<Error>> {
        let event_tree = self.get_event_tree(stream_name).unwrap();

        let event_number = self.new_event_number(stream_name)?;
        let raw_length = event_name.len().to_be_bytes();
        let raw_name = event_name.as_bytes();
        let raw_data = event_data;

        let mut raw_event = Vec::new();
        raw_event.extend_from_slice(&raw_length);
        raw_event.extend_from_slice(&raw_name);
        raw_event.extend_from_slice(&raw_data);

        event_tree.set(event_number.to_be_bytes(), raw_event.clone())?;
        info!("event saved {:?} {:?} {:?}", stream_name, event_name, event_number);
        self.update_last_event_number(stream_name, event_number.0)?;
        Ok(())
    }

    pub fn save_snapshot(
        &self,
        stream_name: &str,
        event_number: u64,
        snapshot_data: &[u8],
    )  -> std::result::Result<(), Box<Error>> {
        info!("create_snapshot - stream_name: {} - event_number: {}", stream_name, event_number);
        let last_snapshot_number = self.last_snapshoted_number(stream_name).unwrap_or(Some(0)).unwrap_or(0);
        let new_snapshot_number = event_number;
        if new_snapshot_number <= last_snapshot_number {
            return Err("Last snapshot is most recent that the new one".into())
        }
        let snapshot_tree = self.get_snapshot_tree(stream_name)?;
        snapshot_tree.set(event_number.to_be_bytes(), snapshot_data)?;
        self.update_last_snapshot_number(stream_name, new_snapshot_number)?;
        Ok(())
    }

    fn new_event_number(&self, stream_name: &str) -> sled::Result<EventNumber> {
        let event_tree = self.get_info_tree(stream_name).unwrap();
        let new_value = event_tree.update_and_fetch(LAST_EVENT_MUMBER_KEY, |previous| {
            let previous = previous.map(|s| EventNumber::try_from(s).unwrap());
            let new = previous.map_or(EventNumber::zero(), EventNumber::next);
            let slice = &new.to_be_bytes()[..];
            Some(IVec::from(slice))
        })?;

        Ok(EventNumber::try_from(new_value.unwrap().as_ref()).unwrap())
    }
}
