use std::ops::{Bound::Excluded, Bound::Unbounded};
use std::sync::Arc;
use std::time::Instant;
use std::{env, io, fmt};

use futures::future::poll_fn;
use log::{info, error};
use sled::{Db, Tree, Event};
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::command::{Command, RespCommandConvertError};
use meilies::stream::{Stream as EsStream, StartReadFrom};
use event_id::EventId;
use meilies::resp::{
    RespCodec,
    RespMsgError,
    RespValue,
    FromResp,
    RespVecConvertError,
    RespBytesConvertError,
};

mod event_id;

#[derive(Debug)]
enum Error<Actual=()> {
    RespMsgError(RespMsgError),
    InvalidRequest,
    RespCommandConvertError(RespCommandConvertError),
    InternalError(sled::Error<Actual>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::RespMsgError(e) => write!(f, "invalid RESP message; {}", e),
            Error::InvalidRequest => write!(f, "invalid request"),
            Error::RespCommandConvertError(e) => write!(f, "invalid command; {}", e),
            Error::InternalError(e) => write!(f, "internal error; {}", e),
        }
    }
}

impl<A> From<sled::Error<A>> for Error<A> {
    fn from(error: sled::Error<A>) -> Error<A> {
        Error::InternalError(error)
    }
}

impl<Actual> From<RespVecConvertError<RespBytesConvertError>> for Error<Actual> {
    fn from(_: RespVecConvertError<RespBytesConvertError>) -> Error<Actual> {
        Error::InvalidRequest
    }
}

fn send_stream_events(
    stream: EsStream,
    tree: Arc<Tree>,
    mut sender: mpsc::UnboundedSender<RespValue>,
) -> sled::Result<(), ()>
{
    info!("spawning a blocking subscription for {}", stream);

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

            let (key, value) = result?;
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

    Ok(())
}

fn handle_command(
    command: Command,
    db: Db,
    mut sender: mpsc::UnboundedSender<RespValue>
) -> Result<(), Error>
{
    match command {
        Command::Subscribe { streams } => {
            for stream in streams {
                let mut sender = sender.clone();
                let stream_name = stream.name.clone();

                let tree = db.open_tree(stream_name.into_bytes())?;

                let subscribed = RespValue::Array(vec![
                    RespValue::string("subscribed"),
                    RespValue::Array(vec![RespValue::string(stream.clone())]),
                ]);

                if sender.start_send(subscribed).is_err() {
                    info!("encountered closed channel");
                }

                tokio::spawn(poll_fn(move || {
                    let mut sender = sender.clone();
                    let stream = stream.clone();
                    let tree = tree.clone();

                    tokio_threadpool::blocking(move || {
                        if let Err(e) = send_stream_events(stream, tree, sender.clone()) {
                            if sender.start_send(RespValue::error(e)).is_err() {
                                info!("encountered closed channel");
                            }
                        }
                    })
                    .map_err(|e| error!("error; {}", e))
                }));
            }
        },
        Command::Publish { stream, event } => {
            let tree = db.open_tree(stream.into_bytes())?;
            let event_id = EventId::from(db.generate_id()?);

            if let Err(e) = tree.set(event_id, event.0) {
                return Err(Error::InternalError(e))
            }

            if sender.start_send(RespValue::string("OK")).is_err() {
                info!("encountered closed channel");
            }
        }
    }

    Ok(())
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

            let mut error_sender = sender.clone();

            let db = db.clone();
            let requests = reader
                .map_err(Error::RespMsgError)
                .and_then(|value| {
                    Command::from_resp(value).map_err(Error::RespCommandConvertError)
                })
                .for_each(move |command| {
                    let db = db.clone();
                    let sender = sender.clone();
                    future::result(handle_command(command, db, sender))
                })
                .or_else(move |error| {
                    error!("error; {}", error);
                    if error_sender.start_send(RespValue::error(error)).is_err() {
                        info!("encountered closed channel");
                    }

                    future::ok(())
                });

            let responses = receiver
                .map_err(|e| RespMsgError::IoError(io::Error::new(io::ErrorKind::BrokenPipe, e)))
                .forward(writer)
                .map_err(|e| error!("error; {}", e))
                .map(|_| ());

            tokio::spawn(requests);
            tokio::spawn(responses);

            future::ok(())
        });

    tokio::run(server)
}
