use std::net::SocketAddr;
use std::{fmt, io};

use futures::{Future, Stream, Sink};
use meilies::stream::{StreamName, EventData};
use meilies::reqresp::{Request, RequestMsgError};
use meilies::reqresp::{Response, ResponseMsgError};

use super::{connect, SteelConnection};

pub fn paired_connect(addr: SocketAddr) -> impl Future<Item=PairedConnection, Error=io::Error> {
    connect(&addr)
        .map(move |connection| {
            PairedConnection::new(SteelConnection::new(addr, connection))
        })
}

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
    pub fn new(connection: SteelConnection) -> PairedConnection {
        PairedConnection { connection }
    }

    pub fn publish(
        self,
        stream: StreamName,
        event: Vec<u8>
    ) -> impl Future<Item=PairedConnection, Error=PairedConnectionError>
    {
        use PairedConnectionError::*;

        let event = EventData(event);
        let command = Request::Publish { stream, event };
        let command = command.into();

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
}
