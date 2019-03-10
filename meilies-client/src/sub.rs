use futures::{Future, Poll, Async, Stream, Sink};
use meilies::codec::{RespValue, RespMsgError, FromResp};
use meilies::stream::{Stream as EsStream, EventNumber};
use tokio::sync::mpsc;
use tokio::prelude::stream;
use log::{info, warn, error};

use super::{RespConnection, RespConnectionReader};

pub fn sub_stream(connection: RespConnection) -> (SubscriptionController, SubscriptionStream) {
    let (writer, reader) = connection.split();
    let (sender, receiver) = mpsc::unbounded_channel();

    let x = receiver
        .map_err(|_| RespMsgError::IoError(unimplemented!()))
        .forward(writer)
        .map(|_| ())
        .map_err(|e| eprintln!("error; {}", e));

    tokio::spawn(x);

    let controller = SubscriptionController { sender };
    let sub_stream = SubscriptionStream { connection: reader };

    (controller, sub_stream)
}

#[derive(Clone)]
pub struct SubscriptionController {
    sender: mpsc::UnboundedSender<RespValue>,
}

impl SubscriptionController {
    pub fn subscribe_to(&mut self, stream: EsStream) {
        let command = RespValue::Array(vec![
            RespValue::bulk_string("subscribe"),
            RespValue::bulk_string(stream.to_string()),
        ]);

        if let Err(e) = self.sender.try_send(command) {
            error!("error; {}", e);
        }
    }
}

pub struct SubscriptionStream {
    connection: RespConnectionReader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    SubscribedTo(Vec<EsStream>),
    Event(EsStream, EventNumber, Vec<u8>),
}

impl Stream for SubscriptionStream {
    type Item = Message;
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

                    let message = Message::SubscribedTo(streams);
                    Ok(Async::Ready(Some(message)))
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

                    let message = Message::Event(stream, event_number, event);
                    Ok(Async::Ready(Some(message)))
                }
                else {
                    Err(())
                }
            })
    }
}
