use std::net::SocketAddr;
use std::io;

use futures::{Future, Poll, Async, Stream};
use meilies::resp::{RespMsgError, FromResp};
use meilies::stream::{Message, RespMessageConvertError, Stream as EsStream};
use meilies::command::Command;
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
                .map(Into::into)
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
    sender: mpsc::UnboundedSender<Command>,
}

impl SubController {
    pub fn subscribe_to(&mut self, stream: EsStream) {
        let command = Command::Subscribe { streams: vec![stream] };

        if let Err(e) = self.sender.try_send(command) {
            error!("{}", e);
        }
    }
}

pub struct SubStream {
    connection: RespConnectionReader,
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
