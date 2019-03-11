use std::net::SocketAddr;
use std::io;

use futures::{Future, Poll, Async, Stream};
use meilies::resp::{RespValue, RespMsgError, FromResp};
use meilies::stream::{Stream as EsStream, EventNumber};
use tokio::sync::mpsc;
use log::error;

use super::{connect, RespConnectionReader};

pub fn sub_connect(
    addr: &SocketAddr
) -> impl Future<Item=(SubController, SubStream), Error=io::Error>
{
    connect(&addr)
        .map(|connection| {
            let (writer, reader) = connection.split();
            let (sender, receiver) = mpsc::unbounded_channel();

            let x = receiver
                .map_err(|e| RespMsgError::IoError(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                .forward(writer)
                .map(|_| ())
                .map_err(|e| error!("error; {}", e));

            tokio::spawn(x);

            let controller = SubController { sender };
            let sub_stream = SubStream { connection: reader };

            (controller, sub_stream)
        })
}

#[derive(Clone)]
pub struct SubController {
    sender: mpsc::UnboundedSender<RespValue>,
}

impl SubController {
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

pub struct SubStream {
    connection: RespConnectionReader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    SubscribedTo(Vec<EsStream>),
    Event(EsStream, EventNumber, Vec<u8>),
}

#[derive(Debug)]
pub enum ProtocolError {
    RespMsgError(RespMsgError),
    RespConvertError,
    InvalidMessageType(RespValue),
    MissingMessageElement,
}

impl Stream for SubStream {
    type Item = Message;
    type Error = ProtocolError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::ProtocolError::*;

        self.connection
            .poll()
            .map_err(RespMsgError)
            .and_then(|async_| {
                let array: Vec<RespValue> = match async_ {
                    Async::Ready(Some(value)) => {
                        FromResp::from_resp(value).map_err(|_| RespConvertError)?
                    },
                    Async::Ready(None) => return Ok(Async::Ready(None)),
                    Async::NotReady => return Ok(Async::NotReady),
                };

                let mut iter = array.into_iter();

                let type_ = match iter.next() {
                    Some(type_) => type_,
                    None => return Err(MissingMessageElement),
                };

                if type_ == "subscribed" {
                    let streams: Vec<EsStream> = match iter.next() {
                        Some(val) => FromResp::from_resp(val).map_err(|_| RespConvertError)?,
                        None => return Err(MissingMessageElement),
                    };

                    let message = Message::SubscribedTo(streams);
                    Ok(Async::Ready(Some(message)))
                }
                else if type_ == "event" {
                    let stream: EsStream = match iter.next() {
                        Some(value) => FromResp::from_resp(value).map_err(|_| RespConvertError)?,
                        None => return Err(MissingMessageElement),
                    };

                    let event_number: EventNumber = match iter.next() {
                        Some(val) => FromResp::from_resp(val).map_err(|_| RespConvertError)?,
                        None => return Err(MissingMessageElement),
                    };

                    let event: Vec<u8> = match iter.next() {
                        Some(value) => FromResp::from_resp(value).map_err(|_| RespConvertError)?,
                        None => return Err(MissingMessageElement),
                    };

                    let message = Message::Event(stream, event_number, event);
                    Ok(Async::Ready(Some(message)))
                }
                else {
                    Err(InvalidMessageType(type_))
                }
            })
    }
}
