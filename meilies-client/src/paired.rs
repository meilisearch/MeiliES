use std::net::SocketAddr;
use std::io;

use futures::{Future, Stream, Sink};
use meilies::resp::RespValue;
use meilies::stream::{StreamName, EventData};
use meilies::command::Command;
use log::error;

use super::{connect, RespConnection};

pub fn paired_connect(addr: &SocketAddr) -> impl Future<Item=PairedConnection, Error=io::Error> {
    connect(&addr).map(PairedConnection::new)
}

pub struct PairedConnection {
    connection: RespConnection,
}

impl PairedConnection {
    pub fn new(connection: RespConnection) -> PairedConnection {
        PairedConnection { connection }
    }

    pub fn publish(self, stream: StreamName, event: Vec<u8>) -> impl Future<Item=PairedConnection, Error=()> {
        let event = EventData(event);
        let command = Command::Publish { stream, event };
        let command = command.into();

        self.connection
            .send(command)
            .map_err(|e| eprintln!("error: {:?}", e))
            .and_then(|framed| framed.into_future().map_err(|_| ()))
            .and_then(|(first, connection)| {
                match first {
                    Some(ref value) if value.is_ok() => {
                        Ok(PairedConnection { connection })
                    },
                    otherwise => {
                        error!("error: {:?}", otherwise);
                        return Err(())
                    }
                }
            })
    }
}
