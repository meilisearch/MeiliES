use std::collections::HashMap;
use std::net::SocketAddr;
use std::io;

use futures::{Future, Poll, Async, AsyncSink, Stream, Sink};
use log::{error, warn};
use meilies::command::Command;
use meilies::resp::{RespValue, RespMsgError, FromResp};
use meilies::stream::{Message, RespMessageConvertError, Stream as EsStream, StreamName};
use tokio::sync::mpsc;
use futures::stream::SplitStream;
use tokio_retry::RetryIf;

use super::{connect, retry_strategy, must_retry, SteelConnection};

#[derive(Default)]
struct StreamContext {
    reconnected: bool,
    position: Option<u64>,
}

pub struct EventStream {
    state: HashMap<StreamName, StreamContext>,
    connection: SteelConnection,
}

impl EventStream {
    fn connect(addr: SocketAddr) -> impl Future<Item=EventStream, Error=tokio_retry::Error<io::Error>> {
        RetryIf::spawn(retry_strategy(), move || {
            warn!("Connecting to {}", addr);
            connect(&addr)
                .map(move |connection| {
                    let connection = SteelConnection::new(addr, connection);
                    EventStream { state: HashMap::new(), connection }
                })
        }, must_retry)
    }

    fn send_stream_subscriptions(&mut self) -> Result<(), RespMsgError> {
        // Now that a new connection has been successfully established
        // we can re-send our subscriptions with the appropriate event number.

        let mut streams = Vec::with_capacity(self.state.len());

        for (name, context) in &mut self.state {
            context.reconnected = true;
            let stream = EsStream { name: name.clone(), from: context.position.into() };
            streams.push(stream);
        }

        let subscription = Command::Subscribe { streams };
        self.start_send(subscription.into())?;
        self.poll_complete()?;

        Ok(())
    }
}

impl Stream for EventStream {
    type Item = RespValue;
    type Error = RespMsgError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let result = match self.connection.poll() {
            Ok(Async::Ready(Some(item))) => {
                match FromResp::from_resp(item.clone()) {
                    Ok(Message::Event(stream_name, number, _)) => {
                        self.state.entry(stream_name).or_default().position = Some(number.0 + 1);
                    },
                    Ok(Message::SubscribedTo { stream }) => {
                        // if we were already subscribed to a stream and we are reconnecting
                        // we do not return the message validating a subscription to the user
                        if self.state.get(&stream).map_or(false, |c| c.reconnected) {
                            return self.poll();
                        }
                    },
                    _otherwise => (),
                }

                Ok(Async::Ready(Some(item)))
            },
            otherwise => otherwise,
        };

        if self.connection.has_been_reconnected() {
            self.send_stream_subscriptions()?;
        }

        result
    }
}

impl Sink for EventStream {
    type SinkItem = RespValue;
    type SinkError = RespMsgError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        if let Ok(Command::Subscribe { streams }) = Command::from_resp(item.clone()) {
            for EsStream { name, from } in streams {
                self.state.entry(name).or_default().position = from.into();
            }
        }

        let result = self.connection.start_send(item);

        if self.connection.has_been_reconnected() {
            self.send_stream_subscriptions()?;
        }

        result
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        let result = self.connection.poll_complete();

        if self.connection.has_been_reconnected() {
            self.send_stream_subscriptions()?;
        }

        result
    }
}

pub fn sub_connect(
    addr: SocketAddr
) -> impl Future<Item=(SubController, SubStream), Error=tokio_retry::Error<io::Error>>
{
    EventStream::connect(addr)
        .map_err(|e| dbg!(e))
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
    connection: SplitStream<EventStream>,
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
