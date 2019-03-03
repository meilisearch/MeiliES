use std::env;

use tokio::codec::Decoder;
use tokio::net::TcpStream;
use tokio::prelude::*;
use futures::stream;
use log::error;

use meilies::codec::{RespValue, RespCodec};

fn main() {
    let _ = stderrlog::new().color(stderrlog::ColorChoice::Never).verbosity(2).init();

    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error parsing addr; {}", e),
    };

    let socket = TcpStream::connect(&addr);

    let client = socket
        .map_err(|e| error!("error accepting socket; {}", e))
        .and_then(|socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();

            let command = RespValue::Array(Some(vec![
                RespValue::bulk_string(Some("subscribe")),
                RespValue::bulk_string(Some("my-little-stream")),
            ]));

            let writes = writer.send(command);

            let messages = reader.for_each(|value| {
                println!("received: {:?}", value);
                Ok(())
            }).then(|_| Ok(()));

            tokio::spawn(messages);

            writes.then(|_| Ok(()))
        });

    tokio::run(client);
}
