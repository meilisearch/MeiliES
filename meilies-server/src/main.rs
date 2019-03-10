use std::ops::{Bound::Excluded, Bound::Unbounded};
use std::time::Instant;
use std::{env, fmt};
use std::sync::Arc;

use futures::future::poll_fn;
use log::{info, error};
use sled::{Db, Tree, Event};
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::codec::{RespCodec, RespMsgError, RespValue};
use meilies::command::{Command, CommandError};
use meilies::stream::{Stream as EsStream, StartReadFrom};
use event_id::EventId;

mod event_id;

#[derive(Debug)]
enum Error<Actual=()> {
    InvalidRequest(RequestError),
    InvalidCommand(CommandError),
    CommandFailed(CommandError),
    InternalError(sled::Error<Actual>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidRequest(e) => write!(f, "invalid request; {}", e),
            Error::InvalidCommand(e) => write!(f, "invalid command; {}", e),
            Error::CommandFailed(e) => write!(f, "command failed; {}", e),
            Error::InternalError(e) => write!(f, "internal error; {}", e),
        }
    }
}

impl<A> From<sled::Error<A>> for Error<A> {
    fn from(error: sled::Error<A>) -> Error<A> {
        Error::InternalError(error)
    }
}

#[derive(Debug)]
enum RequestError {
    NotAnArrayOfBulkStrings,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestError::NotAnArrayOfBulkStrings => {
                write!(f, "requests must be of the type bulk strings array")
            },
        }
    }
}

fn resp_into_arguments(value: RespValue) -> Result<Vec<Vec<u8>>, RequestError> {
    let array = match value {
        RespValue::Array(array) => array,
        _ => return Err(RequestError::NotAnArrayOfBulkStrings),
    };

    let mut args = Vec::with_capacity(array.len());

    for value in array {
        match value {
            RespValue::BulkString(buffer) => args.push(buffer),
            _ => return Err(RequestError::NotAnArrayOfBulkStrings),
        }
    }

    Ok(args)
}

fn send_stream_events(
    stream: EsStream,
    tree: Arc<Tree>,
    mut sender: mpsc::UnboundedSender<RespValue>,
) {
    info!("spawning a blocking subscription for {}", stream);

    let subscribed = RespValue::Array(vec![
        RespValue::SimpleString("subscribed".to_string()),
        RespValue::Array(vec![RespValue::string(stream.clone())]),
    ]);

    if sender.start_send(subscribed).is_err() {
        info!("encountered closed channel");
    }

    let mut watcher = tree.watch_prefix(vec![]);
    let mut event_number = 0;
    let mut last_loop_event_id = None;
    let mut has_more_events = true;

    while has_more_events {
        // reset the watcher at each new loop
        watcher = tree.watch_prefix(vec![]);

        // if this is not the first iteration: skip the last unique id seen
        let range = match last_loop_event_id.take() {
            Some(id) => (Excluded(id), Unbounded),
            None     => (Unbounded,    Unbounded),
        };

        has_more_events = false;

        for result in tree.range(range) {
            has_more_events = true;

            let (key, value) = match result {
                Ok(key_value) => key_value,
                Err(e) => return error!("error while iterating on tree; {}", e),
            };

            last_loop_event_id = Some(key);

            let is_accepted = match stream.from {
                StartReadFrom::EventNumber(number) => event_number >= number,
                StartReadFrom::End => false,
            };

            if is_accepted {
                let response = RespValue::Array(vec![
                    RespValue::string("event"),
                    RespValue::string(stream.clone()),
                    RespValue::Integer(event_number as i64),
                    RespValue::bulk_string(value.to_vec()),
                ]);

                // the only possible error is a closed channel
                if sender.start_send(response).is_err() {
                    info!("encountered closed channel");
                    break
                }
            }

            event_number += 1;
        }
    }

    for event in watcher {
        if let Event::Set(_, value) = event {

            let is_accepted = match stream.from {
                StartReadFrom::EventNumber(number) => event_number >= number,
                StartReadFrom::End => true,
            };

            if is_accepted {
                let response = RespValue::Array(vec![
                    RespValue::string("event"),
                    RespValue::string(stream.clone()),
                    RespValue::Integer(event_number as i64),
                    RespValue::bulk_string(value),
                ]);

                // the only possible error is a closed channel
                if sender.start_send(response).is_err() {
                    info!("encountered closed channel");
                    break
                }
            }

            event_number += 1;
        }
    }
}

fn main() {
    let _ = env_logger::init();

    let addr = env::args().nth(1).unwrap_or("127.0.0.1:6480".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error parsing addr {:?}; {}", addr, e),
    };

    let now = Instant::now();
    let db = match Db::start_default("test-db") {
        Ok(db) => db,
        Err(e) => return error!("error opening database; {}", e),
    };
    info!("kv-store loaded in {:.2?}", now.elapsed());

    let listener = match TcpListener::bind(&addr) {
        Ok(listener) => listener,
        Err(e) => return error!("error binding address; {}", e),
    };
    println!("server is listening on {}", addr);

    let server = listener
        .incoming()
        .map_err(|e| error!("error accepting socket; {}", e))
        .for_each(move |socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();
            let (sender, receiver) = mpsc::unbounded_channel();

            let db = db.clone();

            let requests = reader
                .map(move |value| {
                    let args = match resp_into_arguments(value) {
                        Ok(args) => args,
                        Err(e) => return Err(Error::InvalidRequest(e)),
                    };

                    Command::from_args(args).map_err(Error::InvalidCommand)
                })
                .for_each(move |msg: Result<Command, Error>| {
                    match msg {
                        Ok(Command::Subscribe { streams }) => {
                            for stream in streams {
                                let sender = sender.clone();
                                let stream_name = stream.name.clone();
                                let tree = db.open_tree(stream_name.into_bytes()).unwrap();

                                tokio::spawn(poll_fn(move || {
                                    let sender = sender.clone();
                                    let stream = stream.clone();
                                    let tree = tree.clone();

                                    tokio_threadpool::blocking(move || {
                                        send_stream_events(stream, tree, sender)
                                    })
                                    .map_err(|e| error!("error; {}", e))
                                }));
                            }
                        },
                        Ok(Command::Publish { stream, event }) => {
                            let db = db.clone();
                            let mut sender = sender.clone();

                            let tree = db.open_tree(stream.into_bytes()).unwrap();

                            let event_id = EventId::from(db.generate_id().unwrap());
                            tree.set(event_id, event).unwrap();

                            if sender.start_send(RespValue::string("OK")).is_err() {
                                info!("encountered closed channel");
                            }
                        }
                        Err(e) => error!("error; {}", e),
                    }

                    future::ok(())
                })
                .map_err(|e| error!("error; {}", e));

            let responses = receiver
                .map_err(|e| RespMsgError::IoError(unimplemented!()))
                .forward(writer)
                .map_err(|e| error!("error; {}", e))
                .map(|_| ());

            tokio::spawn(requests);
            tokio::spawn(responses);

            future::ok(())
        });

    tokio::run(server)
}
