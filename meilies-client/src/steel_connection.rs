use std::net::SocketAddr;
use std::{io, mem};

use futures::{Future, Async, AsyncSink, Stream, Sink};
use log::{error, warn, info};
use meilies::resp::{RespValue, RespMsgError};
use tokio_retry::{RetryIf, strategy::FibonacciBackoff};
use tokio_retry::Error as TrError;

use super::{connect, RespConnection};

pub struct SteelConnection {
    addr: SocketAddr,
    reconnected: bool,
    conn_state: ConnState,
}

enum ConnState {
    Connected(RespConnection),
    Connecting(Box<Future<Item=RespConnection, Error=io::Error> + Send>),
}

impl SteelConnection {
    pub fn new(addr: SocketAddr, connection: RespConnection) -> SteelConnection {
        SteelConnection { addr, reconnected: false, conn_state: ConnState::Connected(connection) }
    }

    /// Returns `true` if the connection has been reconnected since the last time called.
    pub fn has_been_reconnected(&mut self) -> bool {
        mem::replace(&mut self.reconnected, false)
    }
}

pub fn retry_strategy() -> std::iter::Take<FibonacciBackoff> {
    FibonacciBackoff::from_millis(100).take(50)
}

pub fn must_retry(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    e.kind() == BrokenPipe || e.kind() == ConnectionRefused
}

fn retry_future(addr: SocketAddr) -> Box<Future<Item=RespConnection, Error=io::Error> + Send> {
    let retry = RetryIf::spawn(retry_strategy(), move || {
            warn!("Reconnecting to {}", addr);
            connect(&addr)
        }, must_retry)
        .map_err(|error| match error {
            TrError::OperationError(e) => e,
            TrError::TimerError(e) => io::Error::new(io::ErrorKind::Other, e),
        });

    Box::new(retry)
}

impl Stream for SteelConnection {
    type Item = RespValue;
    type Error = RespMsgError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match &mut self.conn_state {
            ConnState::Connected(connection) => {
                match connection.poll() {
                    Ok(Async::Ready(Some(item))) => Ok(Async::Ready(Some(item))),
                    Ok(Async::Ready(None)) => {
                        error!("Connection closed with {}", self.addr);
                        self.conn_state = ConnState::Connecting(retry_future(self.addr));
                        self.poll()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => {
                        match error {
                            RespMsgError::IoError(ref e) if must_retry(e) => {
                                self.conn_state = ConnState::Connecting(retry_future(self.addr));
                                self.poll()
                            },
                            otherwise => Err(otherwise),
                        }
                    },
                }
            },
            ConnState::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        info!("Successfully reconnected to {}", self.addr);
                        self.reconnected = true;
                        self.conn_state = ConnState::Connected(connection);
                        self.poll()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}

impl Sink for SteelConnection {
    type SinkItem = RespValue;
    type SinkError = RespMsgError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        match &mut self.conn_state {
            ConnState::Connected(connection) => {
                connection.start_send(item) // TODO check if that can be done
                // unimplemented!()
            },
            ConnState::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        info!("Successfully reconnected to {}", self.addr);
                        self.reconnected = true;
                        self.conn_state = ConnState::Connected(connection);
                        self.start_send(item)
                    },
                    Ok(Async::NotReady) => Ok(AsyncSink::NotReady(item)),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        match &mut self.conn_state {
            ConnState::Connected(connection) => {
                connection.poll_complete() // TODO check if that can be done
                // unimplemented!()
            },
            ConnState::Connecting(connect) => {
                match connect.poll() {
                    Ok(Async::Ready(connection)) => {
                        info!("Successfully reconnected to {}", self.addr);
                        self.reconnected = true;
                        self.conn_state = ConnState::Connected(connection);
                        self.poll_complete()
                    },
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(error) => Err(error.into()),
                }
            },
        }
    }
}
