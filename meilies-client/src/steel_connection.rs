use std::net::SocketAddr;
use std::{io, mem};

use futures::{Future, Async, AsyncSink, Stream, Sink};
use log::{error, warn, info};
use meilies::reqresp::{Request, RequestMsgError, Response, ResponseMsgError};
use tokio_retry::{Retry, strategy::FibonacciBackoff};
use tokio_retry::Error as TrError;

use super::{connect, ClientConnection};

/// A connection that try to reconnect when disconnected.
///
/// It will keep the stream states (e.g. the stream position).
pub struct SteelConnection {
    addr: SocketAddr,
    reconnected: bool,
    conn_state: ConnState,
}

enum ConnState {
    Connected(ClientConnection),
    Connecting(Box<Future<Item=ClientConnection, Error=io::Error> + Send>),
}

impl SteelConnection {
    /// Create a new steel connection.
    pub fn new(addr: SocketAddr, connection: ClientConnection) -> SteelConnection {
        SteelConnection { addr, reconnected: false, conn_state: ConnState::Connected(connection) }
    }

    /// Returns `true` if the connection has been reconnected since the last time called.
    pub fn has_been_reconnected(&mut self) -> bool {
        mem::replace(&mut self.reconnected, false)
    }
}

/// The retry strategy used to reconnect.
pub fn retry_strategy() -> std::iter::Take<FibonacciBackoff> {
    FibonacciBackoff::from_millis(100).take(50)
}

fn retry_future(addr: SocketAddr) -> Box<Future<Item=ClientConnection, Error=io::Error> + Send> {
    let retry = Retry::spawn(retry_strategy(), move || {
            warn!("Reconnecting to {}", addr);
            connect(&addr)
        })
        .map_err(|error| match error {
            TrError::OperationError(e) => e,
            TrError::TimerError(e) => io::Error::new(io::ErrorKind::Other, e),
        });

    Box::new(retry)
}

impl Stream for SteelConnection {
    type Item = Result<Response, String>;
    type Error = ResponseMsgError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match &mut self.conn_state {
            ConnState::Connected(connection) => {
                match connection.poll() {
                    Ok(Async::Ready(None)) => {
                        error!("Connection closed with {}", self.addr);
                        self.conn_state = ConnState::Connecting(retry_future(self.addr));
                        self.poll()
                    },
                    Err(error) => {
                        use ResponseMsgError::RespMsgError;
                        use meilies::resp::RespMsgError::IoError;

                        match error {
                            RespMsgError(IoError(e)) => {
                                error!("Connection error with {}; {}", self.addr, e);
                                self.conn_state = ConnState::Connecting(retry_future(self.addr));
                                self.poll()
                            },
                            otherwise => Err(otherwise),
                        }
                    },
                    otherwise => otherwise,
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
    type SinkItem = Request;
    type SinkError = RequestMsgError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        match &mut self.conn_state {
            ConnState::Connected(connection) => {
                // `start_send` can't trigger any network error. As the name suggests,
                // this method only _begins_ the process of sending the item.
                connection.start_send(item)
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
                match connection.poll_complete() {
                    Err(error) => {
                        use RequestMsgError::RespMsgError;
                        use meilies::resp::RespMsgError::IoError;

                        match error {
                            RespMsgError(IoError(e)) => {
                                error!("Connection error with {}; {}", self.addr, e);
                            self.conn_state = ConnState::Connecting(retry_future(self.addr));
                            self.poll_complete()
                            },
                            otherwise => Err(otherwise),
                        }
                    },
                    otherwise => otherwise,
                }
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
