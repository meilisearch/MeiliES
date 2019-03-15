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

use meilies::reqresp::{ServerCodec, Request, Response};
use meilies::reqresp::{RequestMsgError, ResponseMsgError};
use meilies::stream::{EventData, EventNumber, Stream as EsStream, StartReadFrom};
use event_id::EventId;
use meilies::resp::{
    RespMsgError,
    RespVecConvertError,
    RespBytesConvertError,
};

mod event_id;

#[derive(Debug)]
enum Error<Actual=()> {
    RequestMsgError(RequestMsgError),
    InvalidRequest,
    InternalError(sled::Error<Actual>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::RequestMsgError(e) => write!(f, "invalid request message; {}", e),
            Error::InvalidRequest => write!(f, "invalid request"),
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
    mut sender: mpsc::UnboundedSender<Result<Response, String>>,
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
                let stream = stream.name.clone();
                let number = EventNumber(event_number);
                let event = EventData(value.to_vec());
                let event = Response::Event { stream, number, event };

                // the only possible error is a closed channel
                if sender.start_send(Ok(event)).is_err() {
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
                let stream = stream.name.clone();
                let number = EventNumber(event_number);
                let event = EventData(value);
                let event = Response::Event { stream, number, event };

                // the only possible error is a closed channel
                if sender.start_send(Ok(event)).is_err() {
                    info!("encountered closed channel");
                    break
                }
            }

            event_number += 1;
        }
    }

    Ok(())
}

fn handle_request(
    request: Request,
    db: Db,
    mut sender: mpsc::UnboundedSender<Result<Response, String>>
) -> Result<(), Error>
{
    match request {
        Request::Subscribe { streams } => {
            for stream in streams {
                let mut sender = sender.clone();

                let tree = db.open_tree(stream.name.clone().into_bytes())?;

                let subscribed = Response::Subscribed { stream: stream.name.clone() };
                if sender.start_send(Ok(subscribed)).is_err() {
                    info!("encountered closed channel");
                }

                tokio::spawn(poll_fn(move || {
                    let mut sender = sender.clone();
                    let stream = stream.clone();
                    let tree = tree.clone();

                    tokio_threadpool::blocking(move || {
                        if let Err(e) = send_stream_events(stream, tree, sender.clone()) {
                            if sender.start_send(Err(e.to_string())).is_err() {
                                info!("encountered closed channel");
                            }
                        }
                    })
                    .map_err(|e| error!("error; {}", e))
                }));
            }
        },
        Request::Publish { stream, event } => {
            let tree = db.open_tree(stream.into_bytes())?;
            let event_id = EventId::from(db.generate_id()?);

            if let Err(e) = tree.set(event_id, event.0) {
                return Err(Error::InternalError(e))
            }

            if sender.start_send(Ok(Response::Ok)).is_err() {
                info!("encountered closed channel");
            }
        },
        Request::LastEventNumber { stream } => {
            let tree = db.open_tree(stream.clone().into_bytes())?;
            let number = tree.len().checked_sub(1).map(|x| EventNumber(x as u64));

            let last_event_number = Response::LastEventNumber { stream, number };
            if sender.start_send(Ok(last_event_number)).is_err() {
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
            let framed = ServerCodec::default().framed(socket);
            let (writer, reader) = framed.split();
            let (sender, receiver) = mpsc::unbounded_channel();

            let mut error_sender = sender.clone();

            let db = db.clone();
            let requests = reader
                .map_err(Error::RequestMsgError)
                .for_each(move |request| {
                    let db = db.clone();
                    let sender = sender.clone();
                    future::result(handle_request(request, db, sender))
                })
                .or_else(move |error| {
                    error!("error; {}", error);
                    if error_sender.start_send(Err(error.to_string())).is_err() {
                        info!("encountered closed channel");
                    }

                    future::ok(())
                });

            let responses = receiver
                .map_err(|e| {
                    let error = RespMsgError::IoError(io::Error::new(io::ErrorKind::BrokenPipe, e));
                    ResponseMsgError::RespMsgError(error)
                })
                .forward(writer)
                .map_err(|error| {
                    use ResponseMsgError::RespMsgError;
                    use crate::RespMsgError::IoError;

                    match error {
                        RespMsgError(IoError(ref e)) if e.kind() == io::ErrorKind::BrokenPipe => {
                            info!("{}", e);
                        },
                        other => error!("{}", other),
                    }
                })
                .map(drop);

            tokio::spawn(requests);
            tokio::spawn(responses);

            future::ok(())
        });

    tokio::run(server)
}
