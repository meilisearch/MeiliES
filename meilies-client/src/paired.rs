use std::net::SocketAddr;
use std::fmt;

use async_std::io;
use async_std::net::TcpStream;

use meilies::reqresp::ClientCodec;
use meilies::stream::{StreamName, EventNumber, EventData, EventName};
use meilies::reqresp::{Request, RequestMsgError};
use meilies::reqresp::{Response, ResponseMsgError};

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures_codec::Framed;

/// A paired connection returns a response to each message send, it is sequential.
/// This connection is used to publish events to streams.
pub struct PairedConnection {
    conn: Framed<TcpStream, ClientCodec>,
}

#[derive(Debug)]
pub enum PairedConnectionError {
    ServerSide(String),
    ConnectionClosed,
    RequestMsgError(RequestMsgError),
    ResponseMsgError(ResponseMsgError),
    InvalidServerResponse(Response),
}

impl fmt::Display for PairedConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PairedConnectionError::*;

        match self {
            ServerSide(error) => write!(f, "server side error: {}", error),
            ConnectionClosed => write!(f, "connection closed"),
            RequestMsgError(error) => write!(f, "invalid Request: {}", error),
            ResponseMsgError(error) => write!(f, "invalid Response received: {}", error),
            InvalidServerResponse(response) => {
                write!(f, "invalid server response received: {:?}", response)
            },
        }
    }
}

impl PairedConnection {
    /// Open a framed paired connection with a server.
    pub async fn connect(addr: &SocketAddr) -> io::Result<PairedConnection> {
        let stream = TcpStream::connect(addr).await?;
        let framed = Framed::new(stream, ClientCodec);
        let paired = PairedConnection { conn: framed };

        Ok(paired)
    }

    /// Publish an event to a stream, specifying the event name and data.
    pub async fn publish(
        mut self,
        stream: StreamName,
        event_name: EventName,
        event_data: EventData,
    ) -> Result<PairedConnection, PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::Publish { stream, event_name, event_data };
        self.conn.send(command).await.unwrap();

        let response = match self.conn.next().await {
            Some(Ok(response)) => response,
            Some(Err(error)) => return Err(ResponseMsgError(error)),
            None => return Err(PairedConnectionError::ConnectionClosed),
        };

        match response {
            Ok(Response::Ok) => Ok(self),
            Ok(response) => Err(InvalidServerResponse(response)),
            Err(error) => Err(ServerSide(error)),
        }
    }

    /// Request the last event number that the stream is at.
    ///
    /// Returns `None` if the stream does not contain any event.
    pub async fn last_event_number(
        mut self,
        stream: StreamName,
    ) -> Result<(StreamName, Option<EventNumber>, PairedConnection), PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::LastEventNumber { stream };
        self.conn.send(command).await.unwrap();

        let response = match self.conn.next().await {
            Some(Ok(response)) => response,
            Some(Err(error)) => return Err(ResponseMsgError(error)),
            None => return Err(PairedConnectionError::ConnectionClosed),
        };

        match response {
            Ok(Response::LastEventNumber { stream, number }) => {
                Ok((stream, number, self))
            },
            Ok(response) => Err(InvalidServerResponse(response)),
            Err(error) => Err(ServerSide(error)),
        }
    }

    /// Request the list of stream names
    ///
    /// Returns an empty Vec if the database does not contain any stream.
    pub async fn stream_names(
        mut self
    ) -> Result<(Vec<StreamName>, PairedConnection), PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::StreamNames;
        self.conn.send(command).await.unwrap();

        let response = match self.conn.next().await {
            Some(Ok(response)) => response,
            Some(Err(error)) => return Err(ResponseMsgError(error)),
            None => return Err(PairedConnectionError::ConnectionClosed),
        };

        match response {
            Ok(Response::StreamNames { streams }) => Ok((streams, self)),
            Ok(response) => Err(InvalidServerResponse(response)),
            Err(error) => Err(ServerSide(error)),
        }
    }
}
