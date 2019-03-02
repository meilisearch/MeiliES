use std::{env, str};
use std::time::Instant;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use futures::future::poll_fn;
use futures::future::Either;
use log::{info, error};
use sled::{Db, Event};
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::codec::{RespCodec, RespValue, RespMsgError};
use meilies::command::{Command, arguments_from_resp_value};

enum CommandReturn {
    Publish,
    Subscribe(mpsc::Receiver<(Vec<u8>, Vec<u8>)>),
}

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

            let db = db.clone();
            let responses = reader.map(move |value| {

                println!("received: {:?}", value);

                let args = arguments_from_resp_value(value).unwrap();
                let command = Command::from_args(args).unwrap();

                println!("command: {:?}", command);

                match command {
                    Command::Publish { stream, event } => {
                        let tree = db.open_tree(stream.into_bytes()).unwrap();

                        let unique_id = db.generate_id().unwrap() as u64;
                        let mut unique_id_buff = Vec::new();
                        let _ = unique_id_buff.write_u64::<BigEndian>(unique_id);

                        tree.set(unique_id_buff, event).unwrap();

                        Ok(CommandReturn::Publish)
                    },
                    Command::Subscribe { stream } => {
                        let db = db.clone();
                        let (mut tx, rx) = mpsc::channel(100);

                        tokio::spawn(poll_fn(move || {
                            let stream = stream.clone(); // o_O wtf !!!?

                            tokio_threadpool::blocking(|| {
                                let tree = db.open_tree(stream.into_bytes()).unwrap();

                                for event in tree.watch_prefix(vec![]) {
                                    if let Event::Set(k, v) = event {
                                        tx.try_send((k, v.to_vec())).unwrap();
                                    }
                                }
                            }).map_err(|e| error!("{}", e))
                        }));

                        Ok(CommandReturn::Subscribe(rx))
                    }
                }

            })
            .map_err(|e| {
                // FIXME return the error to the client
                println!("{:?}", e);
                e
            });

            let writes = responses.fold(writer, |writer, result: Result<_, RespMsgError>| {
                let command_return = match result {
                    Ok(command_return) => command_return,
                    Err(e) => return Either::A(writer.send(RespValue::error(e))),
                };

                match command_return {
                    CommandReturn::Publish => Either::A(writer.send(RespValue::string("OK"))),
                    CommandReturn::Subscribe(receiver) => {
                        let keys_values = receiver
                            .map(|(k, v)| {
                                let key = k.as_slice().read_u64::<BigEndian>().unwrap();
                                let value = str::from_utf8(&v).unwrap();
                                RespValue::SimpleString(format!("{} -- {}", key, value))
                            })
                            .map_err(|e| std::io::ErrorKind::Interrupted);
                        Either::B(writer.send_all(keys_values).map(|(s, _)| s))
                    }
                }
            });

            let msg = writes.then(|_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(server)
}
