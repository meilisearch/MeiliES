use std::net::SocketAddr;
use std::{fmt, io};

use futures::{Future, Stream, Sink};
use meilies::resp::{FromResp, RespMsgError};
use meilies::stream::{StreamName, EventData};
use meilies::command::Command;

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
    RespMsgError(RespMsgError),
    InvalidServerMessage(String),
}

impl fmt::Display for PairedConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PairedConnectionError::*;

        match self {
            ServerSide(error) => write!(f, "server side error: {}", error),
            ConnectionClosed => write!(f, "connection closed"),
            RespMsgError(error) => write!(f, "invalid RESP message received: {}", error),
            InvalidServerMessage(string) => {
                write!(f, "invalid server message received: {:?}", string)
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
        let command = Command::Publish { stream, event };
        let command = command.into();

        self.connection
            .send(command)
            .map_err(RespMsgError)
            .and_then(|framed| framed.into_future().map_err(|(e, _)| RespMsgError(e)))
            .and_then(|(first, connection)| {
                let result = match first {
                    Some(first) => Result::<String, String>::from_resp(first).unwrap(),
                    None => return Err(ConnectionClosed),
                };

                match result {
                    Ok(ref string) if string == "OK" => Ok(PairedConnection { connection }),
                    Ok(string) => Err(InvalidServerMessage(string)),
                    Err(error) => Err(ServerSide(error))
                }
            })
    }
}
