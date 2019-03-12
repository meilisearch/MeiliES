use std::net::SocketAddr;
use std::io;

use futures::{Future, Poll, Async, Stream};
use meilies::resp::{RespValue, RespMsgError, FromResp};
use meilies::stream::{Stream as EsStream, EventNumber};
use meilies::event_data::EventData;
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
                .map_err(|e| error!("{}", e))
                .map(|_| ());

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
            error!("{}", e);
        }
    }
}

pub struct SubStream {
    connection: RespConnectionReader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    SubscribedTo(Vec<EsStream>),
    Event(EsStream, EventNumber, EventData),
}

#[derive(Debug)]
pub enum RespMessageConvertError {
    InvalidMessageType(String),
    InvalidRespValue,
    MissingMessageElement,
}

impl FromResp for Message {
    type Error = RespMessageConvertError;

    fn from_resp(value: RespValue) -> Result<Self, Self::Error> {
        use RespMessageConvertError::*;

        let mut args = match Vec::<RespValue>::from_resp(value) {
            Ok(args) => args.into_iter(),
            Err(e) => return Err(InvalidRespValue),
        };

        let message_type: String = match args.next() {
            Some(val) => FromResp::from_resp(val).map_err(|e| InvalidRespValue)?,
            None => return Err(MissingMessageElement),
        };

        match message_type.as_str() {
            "subscribed" => {
                let streams: Vec<EsStream> = match args.next() {
                    Some(val) => FromResp::from_resp(val).map_err(|e| InvalidRespValue)?,
                    None => return Err(MissingMessageElement),
                };

                Ok(Message::SubscribedTo(streams))
            },
            "event" => {
                let stream: EsStream = match args.next() {
                    Some(val) => FromResp::from_resp(val).map_err(|e| InvalidRespValue)?,
                    None => return Err(MissingMessageElement),
                };

                let event_number: EventNumber = match args.next() {
                    Some(val) => FromResp::from_resp(val).map_err(|e| InvalidRespValue)?,
                    None => return Err(MissingMessageElement),
                };

                let event: Vec<u8> = match args.next() {
                    Some(val) => FromResp::from_resp(val).map_err(|e| InvalidRespValue)?,
                    None => return Err(MissingMessageElement),
                };

                Ok(Message::Event(stream, event_number, EventData(event)))
            },
            _unknown => Err(InvalidMessageType(message_type)),
        }
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    RespMsgError(RespMsgError),
    RespConvertError(RespMessageConvertError),
}

impl Stream for SubStream {
    type Item = Result<Message, String>;
    type Error = ProtocolError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        use self::ProtocolError::*;

        self.connection
            .poll()
            .map_err(RespMsgError)
            .and_then(|async_| {
                match async_ {
                    Async::Ready(Some(v)) => {
                        let message = FromResp::from_resp(v).map_err(RespConvertError)?;
                        Ok(Async::Ready(message))
                    },
                    Async::Ready(None) => return Ok(Async::Ready(None)),
                    Async::NotReady => return Ok(Async::NotReady),
                }
            })
    }
}
