use std::collections::HashSet;
use std::iter::FromIterator;

use futures::{Future, Poll, Async, Stream, Sink};
use futures::task;
use meilies::codec::{RespValue, FromResp};
use meilies::stream::{Stream as EsStream, EventNumber};
use log::{info, warn};

use super::{RespConnection, RespConnectionReader};

pub struct SubConnection {
    connection: RespConnection,
}

impl SubConnection {
    pub fn new(connection: RespConnection) -> SubConnection {
        SubConnection { connection }
    }

    pub fn subscribe_to(self, stream: EsStream) -> impl Future<Item=SubStream, Error=()> {
        let command = RespValue::Array(vec![
            RespValue::bulk_string("subscribe"),
            RespValue::bulk_string(stream.to_string()),
        ]);

        self.connection
            .send(command)
            .map_err(|e| eprintln!("error: {:?}", e))
            .and_then(|framed| {
                let (_writer, reader) = framed.split();
                Ok(SubStream {
                    streams: HashSet::new(),
                    pending: HashSet::from_iter(Some(stream)),
                    connection: reader,
                })
            })
    }
}

pub struct SubStream {
    streams: HashSet<EsStream>,
    pending: HashSet<EsStream>,
    connection: RespConnectionReader,
}

impl Stream for SubStream {
    type Item = (EsStream, EventNumber, Vec<u8>);
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.connection
            .poll()
            .map_err(|_| ())
            .and_then(|async_| {
                let array: Vec<RespValue> = match async_ {
                    Async::Ready(Some(value)) => FromResp::from_resp(value)?,
                    Async::Ready(None) => return Ok(Async::Ready(None)),
                    Async::NotReady => return Ok(Async::NotReady),
                };

                let mut iter = array.into_iter();

                let type_ = match iter.next() {
                    Some(type_) => type_,
                    None => return Err(()),
                };

                if type_ == "subscribed" {

                    let streams: Vec<EsStream> = match iter.next() {
                        Some(value) => FromResp::from_resp(value)?,
                        None => return Err(()),
                    };

                    for stream in streams {
                        match self.pending.take(&stream) {
                            Some(stream) => {
                                info!("Pended \"{}\" subscription accepted", stream);
                                self.streams.insert(stream);
                            },
                            None => warn!("Received a non-pended subscription to \"{}\"", stream),
                        }
                    }

                    task::current().notify();

                    Ok(Async::NotReady)
                }
                else if type_ == "event" {

                    let stream: EsStream = match iter.next() {
                        Some(value) => FromResp::from_resp(value).map_err(|_| ())?,
                        None => return Err(()),
                    };

                    let event_number: EventNumber = match iter.next() {
                        Some(value) => FromResp::from_resp(value)?,
                        None => return Err(()),
                    };

                    let event: Vec<u8> = match iter.next() {
                        Some(value) => FromResp::from_resp(value)?,
                        None => return Err(()),
                    };

                    Ok(Async::Ready(Some((stream, event_number, event))))
                }
                else {
                    Err(())
                }
            })
    }
}
