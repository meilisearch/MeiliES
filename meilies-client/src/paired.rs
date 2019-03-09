use futures::{Future, Stream, Sink};
use meilies::codec::RespValue;
use meilies::stream::StreamName;

use super::RespConnection;

pub struct PairedConnection {
    connection: RespConnection,
}

impl PairedConnection {
    pub fn new(connection: RespConnection) -> PairedConnection {
        PairedConnection { connection }
    }

    pub fn publish(self, stream: StreamName, event: Vec<u8>) -> impl Future<Item=PairedConnection, Error=()> {
        let command = RespValue::Array(vec![
            RespValue::bulk_string("publish"),
            RespValue::bulk_string(stream.to_string()),
            RespValue::bulk_string(event),
        ]);

        self.connection
            .send(command)
            .map_err(|e| eprintln!("error: {:?}", e))
            .and_then(|framed| framed.into_future().map_err(|_| ()))
            .and_then(|(first_msg, connection)| {
                match first_msg {
                    Some(RespValue::SimpleString(ref text)) if text == "OK" => {
                        Ok(PairedConnection { connection })
                    },
                    e => {
                        eprintln!("error: {:?}", e);
                        return Err(())
                    },
                }
            })
    }
}
