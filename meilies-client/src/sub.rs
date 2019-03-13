use std::collections::HashMap;
use std::net::SocketAddr;
use std::io;

use futures::{future, Future, Poll, Async, AsyncSink, Stream, Sink};
use log::error;
use meilies::command::Command;
use meilies::resp::{RespCodec, RespMsgError, FromResp};
use meilies::stream::{
    Message, StartReadFrom, RespMessageConvertError, Stream as EsStream, StreamName, EventNumber
};
use tokio::net::{TcpStream, ConnectFuture};
use tokio::sync::mpsc;
use tokio::codec::Framed;
use futures::stream::SplitStream;
use tokio_retry::{Retry, strategy::FibonacciBackoff};

use super::{connect, RespConnection, RespConnectionReader};

enum Connection {
    Connected(RespConnection),
    Connecting(Box<Future<Item=TcpStream, Error=io::Error> + Send>), // FIXME: Use a struct instead
}

impl Stream for Connection {
    type Item = <RespConnection as Stream>::Item;
    type Error = <RespConnection as Stream>::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match self {
            Connection::Connected(connection) => {
                match connection.poll() {
                    Ok(Async::Ready(None)) => {
                        eprintln!("Nothing to see here");
                        let addr = connection.get_ref().peer_addr().unwrap();

                        println!("addr: {:?}", addr);

                        let strategy = FibonacciBackoff::from_millis(100);
                        let retry = Retry::spawn(strategy, move || {
                                eprintln!("Trying to reconnect to the server...");
                                TcpStream::connect(&addr)
                            })
                            .map_err(|e| unimplemented!());

                        *self = Connection::Connecting(Box::new(retry));
                        self.poll()
                    },
                    Err(error) => {
                        eprintln!("Connection error: {}", error);
                        Err(error)
                    },
                    otherwise => otherwise,
                }
            },
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(conn)) => {
                        println!("JUST CONNECTED for Stream::poll!");
                        *self = Connection::Connected(Framed::new(conn, RespCodec));

                        // send subscription!!

                        self.poll()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}

impl Sink for Connection {
    type SinkItem = <RespConnection as Sink>::SinkItem;
    type SinkError = <RespConnection as Sink>::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        match self {
            Connection::Connected(connection) => connection.start_send(item),
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(conn)) => {
                        println!("JUST CONNECTED for Sink::start_send!");
                        *self = Connection::Connected(Framed::new(conn, RespCodec));
                        self.start_send(item)
                    },
                    Ok(Async::NotReady) => Ok(AsyncSink::NotReady(item)),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        match self {
            Connection::Connected(connection) => connection.poll_complete(),
            Connection::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(conn)) => {
                        println!("JUST CONNECTED for Sink::poll_complete!");
                        *self = Connection::Connected(Framed::new(conn, RespCodec));
                        self.poll_complete()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}

pub struct EventStream {
    state: HashMap<StreamName, Option<EventNumber>>,
    addr: SocketAddr,
    inner: Connection,
}

impl EventStream {
    fn connect(addr: SocketAddr) -> impl Future<Item=EventStream, Error=io::Error> {
        connect(&addr)
            .map(move |connection| {
                EventStream {
                    state: HashMap::new(),
                    addr: addr,
                    inner: Connection::Connected(connection),
                }
            })
    }
}

impl Stream for EventStream {
    type Item = <RespConnection as Stream>::Item;
    type Error = <RespConnection as Stream>::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        println!("poll");

        match self.inner.poll() {
            Ok(Async::Ready(Some(item))) => {

                if let Ok(Message::Event(stream, number, _)) = FromResp::from_resp(item.clone()) {
                    let EsStream { name, from } = stream;
                    self.state.insert(name, Some(number));
                    println!("Stream: state is {:?}", self.state);
                }

                Ok(Async::Ready(Some(item)))
            },
            otherwise => otherwise
        }
    }
}

impl Sink for EventStream {
    type SinkItem = <RespConnection as Sink>::SinkItem;
    type SinkError = <RespConnection as Sink>::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        println!("start_send");

        if let Ok(Command::Subscribe { streams }) = Command::from_resp(item.clone()) {
            for EsStream { name, from } in streams {
                let number = match from {
                    StartReadFrom::EventNumber(number) => Some(EventNumber(number)),
                    StartReadFrom::End => None,
                };
                self.state.insert(name, number);
            }
            println!("Sink: state is {:?}", self.state);
        }

        match self.inner.start_send(item) {
            Err(error) => {
                eprintln!("WOWOOW!!! {:?}", error);
                Err(error)
            },
            otherwise => otherwise,
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        println!("poll_complete");
        self.inner.poll_complete()
    }
}

pub fn sub_connect(
    addr: SocketAddr
) -> impl Future<Item=(SubController, SubStream), Error=tokio_retry::Error<io::Error>>
{
    let strategy = FibonacciBackoff::from_millis(100);

    let retry = Retry::spawn(strategy, move || {
        EventStream::connect(addr).map_err(|e| dbg!(e))
    });

    retry.map(|connection| {
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
