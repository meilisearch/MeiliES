use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::ops::{Bound::Excluded, Bound::Unbounded};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use futures::future::poll_fn;
use log::{info, error};
use sled::{Db, Tree, Event, ConfigBuilder};
use structopt::StructOpt;
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::reqresp::{ServerCodec, Request, Response};
use meilies::reqresp::{RequestMsgError, ResponseMsgError};
use meilies::stream::{RawEvent, EventNumber, Stream as EsStream, StartReadFrom};
use event_id::EventId;
use meilies::resp::{RespMsgError, RespVecConvertError, RespBytesConvertError};

mod event_id;

#[derive(Debug, StructOpt)]
#[structopt(name = "meilies-server", about = "Start the server")]
struct Opt {
    /// Server hostname.
    #[structopt(short = "h", long = "hostname", default_value = "127.0.0.1")]
    hostname: String,

    /// Server port.
    #[structopt(short = "p", long = "port", default_value = "6480")]
    port: u16,

    /// Database path
    #[structopt(long = "db-path", parse(from_os_str), default_value = "/var/lib/meilies")]
    db_path: PathBuf,
}

#[derive(Debug)]
enum Error {
    RequestMsgError(RequestMsgError),
    InvalidRequest,
    InternalError(sled::Error),
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

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Error {
        Error::InternalError(error)
    }
}

impl From<RespVecConvertError<RespBytesConvertError>> for Error {
    fn from(_: RespVecConvertError<RespBytesConvertError>) -> Error {
        Error::InvalidRequest
    }
}

fn send_stream_events(
    stream: EsStream,
    tree: Arc<Tree>,
    mut sender: mpsc::Sender<Result<Response, String>>,
) -> sled::Result<()>
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
                let event = RawEvent::new(value);
                let event_name = match event.name() {
                    Ok(name) => name,
                    Err(err) => {
                        error!("impossible to parse the event name; {}", err);
                        return Err(IoError::new(ErrorKind::InvalidData, err.to_string()).into())
                    }
                };
                let event_data = event.data();
                let event = Response::Event { stream, number, event_name, event_data };

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
                let event = RawEvent::new(value);
                let event_name = match event.name() {
                    Ok(name) => name,
                    Err(err) => {
                        error!("impossible to parse the event name; {}", err);
                        return Err(IoError::new(ErrorKind::InvalidData, err.to_string()).into())
                    }
                };
                let event_data = event.data();
                let event = Response::Event { stream, number, event_name, event_data };

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
    mut sender: mpsc::Sender<Result<Response, String>>
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
        Request::Publish { stream, event_name, event_data } => {
            let event_id = EventId::from(db.generate_id()?);
            info!("{:?} {:?} {:?}", stream, event_name, event_id);

            let tree = db.open_tree(stream.into_bytes())?;
            let raw_length = event_name.as_str().len().to_be_bytes();
            let raw_name = event_name.as_str().as_bytes();
            let raw_data = event_data.0;

            let mut raw_event = Vec::new();
            raw_event.extend_from_slice(&raw_length);
            raw_event.extend_from_slice(&raw_name);
            raw_event.extend_from_slice(&raw_data);

            if let Err(e) = tree.set(event_id, raw_event) {
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

#[cfg(feature = "sentry")]
fn init_sentry() {
    let guard = sentry::init(sentry::ClientOptions::default());

    if guard.is_enabled() {
        eprintln!("I am sentrified! 🎉");
    }

    sentry::integrations::panic::register_panic_handler();
    sentry::integrations::env_logger::init(None, Default::default());
}

fn main() {
    #[cfg(feature = "sentry")]
    init_sentry();

    #[cfg(not(feature = "sentry"))]
    let _ = env_logger::init();

    let opt = Opt::from_args();

    let addr = match opt.hostname.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error parsing addr {:?}; {}", opt.hostname, e),
    };

    let addr = SocketAddr::new(addr, opt.port);

    let now = Instant::now();

    let config = ConfigBuilder::new()
         .path(opt.db_path)
         .use_compression(true)
         .compression_factor(22)
         .build();

    let db = match Db::start(config) {
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
            let (sender, receiver) = mpsc::channel(10);

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
                    let error = RespMsgError::IoError(IoError::new(ErrorKind::BrokenPipe, e));
                    ResponseMsgError::RespMsgError(error)
                })
                .forward(writer)
                .map_err(|error| {
                    use ResponseMsgError::RespMsgError;
                    use crate::RespMsgError::IoError;

                    match error {
                        RespMsgError(IoError(ref e)) if e.kind() == ErrorKind::BrokenPipe => {
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
