use std::collections::HashSet;
use std::iter::FromIterator;
use std::str::FromStr;

use futures::{Future, Poll, Async, Stream, Sink};
use futures::task;
use meilies::codec::RespValue;
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
                let array = match async_ {
                    Async::Ready(Some(RespValue::Array(array))) => array,
                    Async::Ready(Some(other)) => return Err(()),
                    Async::Ready(None) => return Ok(Async::Ready(None)),
                    Async::NotReady => return Ok(Async::NotReady),
                };

                let mut iter = array.into_iter();

                let type_ = match iter.next() {
                    Some(type_) => type_,
                    None => unimplemented!(),
                };

                if type_ == "subscribed" {
                    let streams = match iter.next() {
                        Some(RespValue::Array(array)) => array,
                        _ => unimplemented!(),
                    };

                    for stream in streams {
                        let stream = match stream {
                            RespValue::SimpleString(string) => EsStream::from_str(&string).unwrap(),
                            other => unimplemented!(),
                        };

                        match self.pending.take(&stream) {
                            Some(stream) => {
                                info!("Pended \"{}\" subscription accepted", stream);
                                self.streams.insert(stream);
                            },
                            None => {
                                warn!("Received non-pended \"{}\" subscription", stream);
                                unimplemented!()
                            },
                        }
                    }

                    task::current().notify();

                    Ok(Async::NotReady)
                }
                else if type_ == "event" {
                    let stream = match iter.next() {
                        Some(RespValue::SimpleString(string)) => EsStream::from_str(&string).unwrap(),
                        _ => unimplemented!(),
                    };

                    let event_number = match iter.next() {
                        Some(RespValue::Integer(integer)) => EventNumber(integer as u64),
                        _ => unimplemented!(),
                    };

                    let event = match iter.next() {
                        Some(RespValue::BulkString(bytes)) => bytes,
                        _ => unimplemented!(),
                    };

                    Ok(Async::Ready(Some((stream, event_number, event))))
                }
                else {
                    unimplemented!()
                }
            })
    }
}
