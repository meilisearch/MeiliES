use std::ops::{Bound::Excluded, Bound::Unbounded};
use std::time::Instant;
use std::{env, fmt};
use std::sync::Arc;

use futures::future::poll_fn;
use futures::future::Either;
use log::{info, error};
use sled::{Db, Event, IVec};
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::codec::{RespCodec, RespValue};
use meilies::command::{Command, CommandError};
use meilies::stream::{self as es_stream, EventNumber, StartReadFrom};
use event_id::EventId;

mod event_id;

enum CommandReturn {
    Publish,
    Subscribe {
        streams: Vec<es_stream::Stream>,
        events: mpsc::UnboundedReceiver<(es_stream::Stream, EventNumber, IVec)>,
    },
}

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

// TODO introduce a Context type???
fn execute_command(db: Db, command: Command) -> Result<CommandReturn, Error> {
    match command {
        Command::Publish { stream, event } => {
            let tree = db.open_tree(stream.into_bytes())?;

            let event_id = EventId::from(db.generate_id()?);
            tree.set(event_id, event)?;

            Ok(CommandReturn::Publish)
        },
        Command::Subscribe { streams } => {
            let (tx, rx) = mpsc::unbounded_channel();
            let mut streams_names = Vec::with_capacity(streams.len());

            for stream in streams.clone() {
                streams_names.push(stream.name.clone());
                let tree = db.open_tree(stream.name.clone().into_bytes())?;
                let mut tx = tx.clone();

                tokio::spawn(poll_fn(move || {
                    tokio_threadpool::blocking(|| {
                        info!("spawning a blocking subscription");

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

                                let (k, v) = match result {
                                    Ok(key_value) => key_value,
                                    Err(e) => return error!("error while iterating on tree; {}", e),
                                };

                                last_loop_event_id = Some(k);

                                let is_accepted = match stream.from {
                                    StartReadFrom::EventNumber(number) => event_number >= number,
                                    StartReadFrom::End => true,
                                };

                                if is_accepted {
                                    let event_number = EventNumber(event_number);
                                    // the only possible error is a closed channel
                                    if tx.start_send((stream.clone(), event_number, v)).is_err() {
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
                                    let event_number = EventNumber(event_number);
                                    let value = IVec::Remote { buf: Arc::from(value) };
                                    // the only possible error is a closed channel
                                    if tx.start_send((stream.clone(), event_number, value)).is_err() {
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
            }

            Ok(CommandReturn::Subscribe { streams, events: rx })
        }
    }
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
    info!("kv-store loaded in {:.2?}", now.elapsed());

    let listener = TcpListener::bind(&addr).unwrap();
    println!("server is listening on {}", addr);

    let server = listener
        .incoming()
        .map_err(|e| error!("error accepting socket; {}", e))
        .for_each(move |socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();

            let db = db.clone();
            let responses = reader.map(move |value| {
                let args = match resp_into_arguments(value) {
                    Ok(args) => args,
                    Err(e) => return Err(Error::InvalidRequest(e)),
                };

                let command = match Command::from_args(args) {
                    Ok(command) => command,
                    Err(e) => return Err(Error::InvalidCommand(e)),
                };

                execute_command(db.clone(), command)
            })
            .map_err(|e| {
                // FIXME return the error to the client
                println!("{:?}", e);
                e
            });

            let writes = responses.fold(writer, |writer, result| {
                let command_return = match result {
                    Ok(command_return) => command_return,
                    Err(e) => return Either::A(writer.send(RespValue::error(e))),
                };

                match command_return {
                    CommandReturn::Publish => Either::A(writer.send(RespValue::string("OK"))),
                    CommandReturn::Subscribe { streams, events } => {
                        let events = events
                            .map(|(stream, event_number, v)| {
                                let event_text = RespValue::SimpleString("event".to_owned());
                                let stream = RespValue::bulk_string(stream.to_string());
                                let event_number = RespValue::Integer(event_number.0 as i64);
                                let value = RespValue::bulk_string(v.to_vec());

                                RespValue::Array(vec![event_text, stream, event_number, value])
                            })
                            .map_err(|e| {
                                eprintln!("error: {}", e);
                                std::io::ErrorKind::Interrupted
                            });

                        let subscribed = RespValue::Array(vec![
                            RespValue::SimpleString("subscribed".to_string()),
                            RespValue::Array(streams.into_iter().map(|s| RespValue::bulk_string(s.to_string())).collect()),
                        ]);

                        let responses = writer
                            .send(subscribed)
                            .and_then(|writer| writer.send_all(events).map(|(s, _)| s));

                        Either::B(responses)
                    }
                }
            });

            let msg = writes.then(|_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(server)
}
