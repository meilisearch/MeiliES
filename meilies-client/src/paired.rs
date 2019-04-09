use std::net::SocketAddr;
use std::{fmt, io};

use futures::{Future, Stream, Sink};
use tokio_retry::Retry;
use log::warn;
use meilies::stream::{StreamName, EventNumber, EventData, EventName};
use meilies::reqresp::{Request, RequestMsgError};
use meilies::reqresp::{Response, ResponseMsgError};

use crate::steel_connection::retry_strategy;
use super::{connect, SteelConnection};

/// Open a framed paired connection with a server.
pub fn paired_connect(
    addr: SocketAddr
) -> impl Future<Item=PairedConnection, Error=tokio_retry::Error<io::Error>>
{
    PairedConnection::connect(addr)
}

/// A paired connection returns a response to each message send, it is sequential.
/// This connection is used to publish events to streams.
pub struct PairedConnection {
    connection: SteelConnection,
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
    pub fn connect(addr: SocketAddr) -> impl Future<Item=PairedConnection, Error=tokio_retry::Error<io::Error>> {
        Retry::spawn(retry_strategy(), move || {
            warn!("Connecting to {}", addr);
            connect(&addr)
                .map(move |connection| {
                    let connection = SteelConnection::new(addr, connection);
                    PairedConnection { connection }
                })
        })
    }

    /// Publish an event to a stream, specifying the event name and data.
    pub fn publish(
        self,
        stream: StreamName,
        event_name: EventName,
        event_data: EventData,
    ) -> impl Future<Item=PairedConnection, Error=PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::Publish { stream, event_name, event_data };

        self.connection
            .send(command)
            .map_err(RequestMsgError)
            .and_then(|framed| framed.into_future().map_err(|(e, _)| ResponseMsgError(e)))
            .and_then(|(first, connection)| {
                match first.ok_or(ConnectionClosed)? {
                    Ok(Response::Ok) => Ok(PairedConnection { connection }),
                    Ok(response) => Err(InvalidServerResponse(response)),
                    Err(error) => Err(ServerSide(error)),
                }
            })
    }

    /// Request the last event number that the stream is at.
    ///
    /// Returns `None` if the stream does not contain any event.
    pub fn last_event_number(
        self,
        stream: StreamName
    ) -> impl Future<Item=(StreamName, Option<EventNumber>, PairedConnection), Error=PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::LastEventNumber { stream };

        self.connection
            .send(command)
            .map_err(RequestMsgError)
            .and_then(|framed| framed.into_future().map_err(|(e, _)| ResponseMsgError(e)))
            .and_then(|(first, connection)| {
                match first.ok_or(ConnectionClosed)? {
                    Ok(Response::LastEventNumber { stream, number }) => Ok((stream, number, PairedConnection { connection })),
                    Ok(response) => Err(InvalidServerResponse(response)),
                    Err(error) => Err(ServerSide(error)),
                }
            })
    }

    /// Request the list of stream names
    ///
    /// Returns an empty Vec if the database does not contain any stream.
    pub fn stream_names(
        self,
    ) -> impl Future<Item=(Vec<StreamName>, PairedConnection), Error=PairedConnectionError>
    {
        use PairedConnectionError::*;

        let command = Request::StreamNames;

        self.connection
            .send(command)
            .map_err(RequestMsgError)
            .and_then(|framed| framed.into_future().map_err(|(e, _)| ResponseMsgError(e)))
            .and_then(|(first, connection)| {
                match first.ok_or(ConnectionClosed)? {
                    Ok(Response::StreamNames { streams }) => Ok((streams, PairedConnection { connection })),
                    Ok(response) => Err(InvalidServerResponse(response)),
                    Err(error) => Err(ServerSide(error)),
                }
            })
    }
}
