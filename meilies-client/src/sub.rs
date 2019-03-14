use std::collections::HashMap;
use std::net::SocketAddr;
use std::io;

use futures::{future, Future, Poll, Async, AsyncSink, Stream, Sink};
use log::{error, warn};
use meilies::command::Command;
use meilies::resp::{RespCodec, RespValue, RespMsgError, FromResp};
use meilies::stream::{
    Message, StartReadFrom, RespMessageConvertError, Stream as EsStream, StreamName, EventNumber
};
use tokio::net::{TcpStream, ConnectFuture};
use tokio::sync::mpsc;
use tokio::codec::Framed;
use futures::stream::SplitStream;
use tokio_retry::{Retry, strategy::FibonacciBackoff};
use tokio_retry::Error as TrError;

use super::{connect, RespConnection, RespConnectionReader};

enum Connection {
    Connected(RespConnection),
    Connecting(Box<Future<Item=RespConnection, Error=io::Error> + Send>),
}

fn retry_strategy() -> std::iter::Take<FibonacciBackoff> {
    FibonacciBackoff::from_millis(100).take(50)
}

pub struct EventStream {
    state: HashMap<StreamName, Option<u64>>,
    addr: SocketAddr,
    connection: Connection,
}

impl EventStream {
    fn connect(addr: SocketAddr) -> impl Future<Item=EventStream, Error=tokio_retry::Error<io::Error>> {
        Retry::spawn(retry_strategy(), move || {
            warn!("Trying to connect to {}...", addr);

            connect(&addr)
                .map(move |connection| {
                    EventStream {
                        state: HashMap::new(),
                        addr: addr,
                        connection: Connection::Connected(connection),
                    }
                })
        })
    }
}

impl Stream for EventStream {
    type Item = RespValue;
    type Error = RespMsgError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match &mut self.connection {
            Connection::Connected(connection) => {
                match connection.poll() {
                    Ok(Async::Ready(Some(item))) => {

                        match FromResp::from_resp(item.clone()) {
                            Ok(Message::Event(stream_name, number, _)) => {
                                self.state.insert(stream_name, Some(number.0 + 1));
                            },
                            Ok(Message::SubscribedTo { streams }) => {
                                // if we were already subscribed to a stream and we are reconnecting
                                // we do not return the message validating a subscription to the user
                                if self.state.contains_key(&streams[0]) {
                                    return self.poll();
                                }
                            },
                            _otherwise => (),
                        }

                        Ok(Async::Ready(Some(item)))
                    },
                    Ok(Async::Ready(None)) => {
                        error!("Connection closed");

                        let addr = self.addr;
                        let retry = Retry::spawn(retry_strategy(), move || {
                                warn!("Trying to reconnect to {}...", addr);
                                TcpStream::connect(&addr).map(|conn| Framed::new(conn, RespCodec))
                            })
                            .map_err(|error| match error {
                                TrError::OperationError(e) => e,
                                TrError::TimerError(e) => io::Error::new(io::ErrorKind::Other, e),
                            });

                        self.connection = Connection::Connecting(Box::new(retry));
                        self.poll()
                    },
                    Err(error) => Err(error),
                    otherwise => otherwise,
                }
            },
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        self.connection = Connection::Connected(connection);

                        // Now that a new connection have been successfully established
                        // we can re-send our subscriptions with the appropriate event number,
                        // this means the event number just after the last event number we received.

                        let mut streams = Vec::with_capacity(self.state.len());

                        for (name, &from) in &self.state {
                            let stream = EsStream { name: name.clone(), from: from.into() };
                            streams.push(stream);
                        }

                        let subscription = Command::Subscribe { streams };
                        self.start_send(subscription.into())?;
                        self.poll_complete()?;

                        self.poll()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}

impl Sink for EventStream {
    type SinkItem = RespValue;
    type SinkError = RespMsgError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        if let Ok(Command::Subscribe { streams }) = Command::from_resp(item.clone()) {
            for EsStream { name, from } in streams {
                self.state.insert(name, from.into());
            }
        }

        match &mut self.connection {
            Connection::Connected(connection) => connection.start_send(item), // TODO check if that can be done
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        self.connection = Connection::Connected(connection);
                        self.start_send(item)
                    },
                    Ok(Async::NotReady) => Ok(AsyncSink::NotReady(item)),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        match &mut self.connection {
            Connection::Connected(connection) => connection.poll_complete(), // TODO check if that can be done
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        self.connection = Connection::Connected(connection);
                        self.poll_complete()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}

pub fn sub_connect(
    addr: SocketAddr
) -> impl Future<Item=(SubController, SubStream), Error=tokio_retry::Error<io::Error>>
{
    let strategy = FibonacciBackoff::from_millis(100);

    let event_stream = EventStream::connect(addr).map_err(|e| dbg!(e));

    event_stream.map(|connection| {
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
