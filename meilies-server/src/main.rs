use std::env;
use std::time::Instant;

use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::codec::Decoder;
use log::{info, error};
use sled::Db;

use meilies::codec::{RespCodec, RespValue, RespMsgError};
use meilies::command::{Command, arguments_from_resp_value};

fn main() {
    let _ = env_logger::init();

    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error pasing addr; {}", e),
    };

    let now = Instant::now();
    let db = Db::start_default("test-db").unwrap();
    info!("sled loaded in {:.2?}", now.elapsed());

    let listener = TcpListener::bind(&addr).unwrap();

    println!("server is running on {}", addr);

    let server = listener
        .incoming()
        .map_err(|e| error!("error accepting socket; {}", e))
        .for_each(move |socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();

            let _db = db.clone();
            let responses = reader.map(|value| {

                println!("received: {:?}", value);

                let args = arguments_from_resp_value(value).unwrap();
                let command = Command::from_args(args).unwrap();

                println!("command: {:?}", command);

                Ok(RespValue::SimpleString("OK".to_string()))

            })
            .map_err(|e| {
                // FIXME return the error to the client
                println!("{:?}", e);
                e
            });

            let writes = responses.fold(writer, |writer, result: Result<_, RespMsgError>| {
                match result {
                    Ok(value) => writer.send(value),
                    Err(e) => writer.send(RespValue::Error(format!("{:?}", e))),
                }
            });

            let msg = writes.then(|_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(server)
}
