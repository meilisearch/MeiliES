use std::collections::HashMap;
use std::net::SocketAddr;
use std::{fmt, io};

use futures::{Future, Poll, Async, AsyncSink, Stream, Sink};
use log::{error, warn};
use meilies::resp::RespMsgError;
use meilies::reqresp::{Request, RequestMsgError, Response, ResponseMsgError};
use meilies::stream::{Stream as EsStream, StreamName};
use tokio::sync::mpsc;
use futures::stream::SplitStream;
use tokio_retry::Retry;

use super::{connect, retry_strategy, SteelConnection};

#[derive(Debug, Default)]
struct StreamContext {
    reconnected: bool,
    position_start: Option<u64>,
    position_end: Option<u64>,
}

/// A tokio Stream that reconnect when the connection is lost.
///
/// It preferable to use `sub_connect` to get a `SubController` and `SubStream` tuple.
pub struct EventStream {
    state: HashMap<StreamName, StreamContext>,
    connection: SteelConnection,
}

impl EventStream {
    fn connect(addr: SocketAddr) -> impl Future<Item=EventStream, Error=tokio_retry::Error<io::Error>> {
        Retry::spawn(retry_strategy(), move || {
            warn!("Connecting to {}", addr);
            connect(&addr)
                .map(move |connection| {
                    let connection = SteelConnection::new(addr, connection);
                    EventStream { state: HashMap::new(), connection }
                })
        })
    }

    fn send_stream_subscriptions(&mut self) -> Result<(), ProtocolError> {
        // Now that a new connection has been successfully established
        // we can re-send our subscriptions with the appropriate event number.

        let mut streams = Vec::with_capacity(self.state.len());

        for (name, context) in &mut self.state {
            context.reconnected = true;
            let stream = EsStream { name: name.clone(), from: context.position_start.into(), to: context.position_end.into() };
            streams.push(stream);
        }

        let subscription = Request::Subscribe { streams };
        self.start_send(subscription)?;
        self.poll_complete()?;

        Ok(())
    }
}

impl Stream for EventStream {
    type Item = Result<Response, String>;
    type Error = ProtocolError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let result = match self.connection.poll() {
            Ok(Async::Ready(Some(item))) => {
                match &item {
                    Ok(Response::Event { stream, number, .. }) => {
                        self.state.entry(stream.clone()).or_default().position_start = Some(number.0 + 1);
                    },
                    Ok(Response::Subscribed { stream }) => {
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

        result.map_err(ProtocolError::ResponseMsgError)
    }
}

impl Sink for EventStream {
    type SinkItem = Request;
    type SinkError = ProtocolError;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        if let Request::Subscribe { streams } = &item {
            for EsStream { name, from, to } in streams {
                self.state.entry(name.clone()).or_default().position_start = (*from).into();
                self.state.entry(name.clone()).or_default().position_end = (*to).into();
            }
        }

        let result = self.connection.start_send(item);

        if self.connection.has_been_reconnected() {
            self.send_stream_subscriptions()?;
        }

        result.map_err(ProtocolError::RequestMsgError)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        let result = self.connection.poll_complete();

        if self.connection.has_been_reconnected() {
            self.send_stream_subscriptions()?;
        }

        result.map_err(ProtocolError::RequestMsgError)
    }
}

/// Open a sup connection with a server.
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
                .map_err(|e| {
                    let error = RespMsgError::IoError(io::Error::new(io::ErrorKind::BrokenPipe, e));
                    ProtocolError::RequestMsgError(RequestMsgError::RespMsgError(error))
                })
                .map(Into::into)
                .forward(writer)
                .map_err(|e| error!("{:?}", e))
                .map(|_| ());

            tokio::spawn(x);

            let controller = SubController { sender };
            let sub_stream = SubStream { connection: reader };

            (controller, sub_stream)
        })
}

/// A sub controller control which streams to connect to.
#[derive(Clone)]
pub struct SubController {
    sender: mpsc::UnboundedSender<Request>,
}

impl SubController {
    /// Ask the server to send events of the given stream.
    pub fn subscribe_to(&mut self, stream: EsStream) {
        let command = Request::Subscribe { streams: vec![stream] };

        if let Err(e) = self.sender.try_send(command) {
            error!("{}", e);
        }
    }
}

/// A tokio Stream that returns every event received on all subscribed streams.
pub struct SubStream {
    connection: SplitStream<EventStream>,
}

#[derive(Debug)]
pub enum ProtocolError {
    ResponseMsgError(ResponseMsgError),
    RequestMsgError(RequestMsgError),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProtocolError::ResponseMsgError(error) => write!(f, "{}", error),
            ProtocolError::RequestMsgError(error) => write!(f, "{}", error),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl Stream for SubStream {
    type Item = Result<Response, String>;
    type Error = ProtocolError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.connection.poll()
    }
}
