use std::ops::{Bound::Excluded, Bound::Unbounded};
use std::time::Instant;
use std::{env, str};

use futures::future::poll_fn;
use futures::future::Either;
use log::{info, error};
use sled::{Db, Event};
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

mod event_id;

use meilies::codec::{RespCodec, RespValue};
use meilies::command::{Command, CommandError, arguments_from_resp_value};
use event_id::EventId;

enum CommandReturn {
    Publish,
    Subscribe(mpsc::UnboundedReceiver<(EventId, Vec<u8>)>),
}

fn main() {
    let _ = env_logger::init();

    let addr = env::args().nth(1).unwrap_or("127.0.0.1:6480".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error parsing addr; {}", e),
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

                let args = match arguments_from_resp_value(value) {
                    Ok(args) => args,
                    Err(_) => return Err(CommandError::CommandNotFound),
                };

                let command = match Command::from_args(args) {
                    Ok(command) => command,
                    Err(e) => return Err(e),
                };

                println!("command: {:?}", command);

                match command {
                    Command::Publish { stream, event } => {
                        let tree = db.open_tree(stream.into_bytes()).unwrap();

                        let event_id = EventId::from(db.generate_id().unwrap());
                        tree.set(event_id, event).unwrap();

                        Ok(CommandReturn::Publish)
                    },
                    Command::Subscribe { stream, from } => {
                        let db = db.clone();
                        let (mut tx, rx) = mpsc::unbounded_channel();

                        tokio::spawn(poll_fn(move || {
                            let stream = stream.clone(); // o_O wtf !!!?

                            tokio_threadpool::blocking(|| {
                                let tree = db.open_tree(stream.into_bytes()).unwrap();

                                let mut watcher = tree.watch_prefix(vec![]);
                                let mut event_number = 0;

                                if from >= 0 {

                                    let mut last_loop_event_id = None;
                                    let mut has_more_events = true;

                                    while has_more_events {

                                        // reset the watcher at each new loop
                                        watcher = tree.watch_prefix(vec![]);

                                        // if this is not the first time: skip the last unique id seen
                                        let range = match last_loop_event_id.take() {
                                            Some(id) => (Excluded(id), Unbounded),
                                            None     => (Unbounded,    Unbounded),
                                        };

                                        has_more_events = false;

                                        for result in tree.range(range) {
                                            has_more_events = true;

                                            let (k, v) = result.unwrap();
                                            let event_id = EventId::from_raw(&k).expect("BE event id");
                                            last_loop_event_id = Some(k);

                                            if event_number >= from {
                                                if tx.start_send((event_id, v.to_vec())).is_err() {
                                                    // The only way a send can fail is because
                                                    // of a closed channel
                                                    info!("encountered closed channel");
                                                    break
                                                }
                                            }

                                            event_number += 1;
                                        }
                                    }
                                }

                                for event in watcher {
                                    if let Event::Set(k, v) = event {
                                        if event_number >= from {
                                            let event_id = EventId::from_raw(&k).unwrap();
                                            if tx.start_send((event_id, v.to_vec())).is_err() {
                                                // The only way a send can fail is because
                                                // of a closed channel
                                                info!("encountered closed channel");
                                                break
                                            }
                                        }

                                        event_number += 1;
                                    }
                                }
                            })
                            .map_err(|e| error!("{}", e))
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

            let writes = responses.fold(writer, |writer, result: Result<_, CommandError>| {
                let command_return = match result {
                    Ok(command_return) => command_return,
                    Err(e) => return Either::A(writer.send(RespValue::error(e))),
                };

                match command_return {
                    CommandReturn::Publish => Either::A(writer.send(RespValue::string("OK"))),
                    CommandReturn::Subscribe(receiver) => {
                        let keys_values = receiver
                            .map(|(event_id, v)| {
                                let value = str::from_utf8(&v).unwrap();
                                RespValue::SimpleString(format!("{:?} -- {}", event_id, value))
                            })
                            .map_err(|e| {
                                eprintln!("error: {}", e);
                                std::io::ErrorKind::Interrupted
                            });
                        Either::B(writer.send_all(keys_values).map(|(s, _)| s))
                    }
                }
            });

            let msg = writes.then(|_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(server)
}
