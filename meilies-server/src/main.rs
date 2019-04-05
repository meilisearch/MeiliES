use std::convert::TryFrom;
use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
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
use meilies::stream::{RawEvent, EventNumber, Stream as EsStream, StreamName as EsStreamName, StartReadFrom};
use meilies::resp::{RespMsgError, RespVecConvertError, RespBytesConvertError};

fn new_event_number(numbers: &Tree, name: &EsStreamName) -> sled::Result<EventNumber> {
    let mut current = numbers.get(name)?;

    loop {
        let previous = current.as_ref().map(|s| EventNumber::try_from(s.as_ref()).unwrap());

        let new = previous.map_or(EventNumber::zero(), EventNumber::next);
        let new_vec = new.to_be_bytes().to_vec();

        let previous = previous.map(EventNumber::to_be_bytes);
        let previous = previous.as_ref().map(AsRef::as_ref);

        match numbers.cas(name, previous, Some(new_vec))? {
            Ok(()) => return Ok(new),
            Err(new_current) => current = new_current,
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "meilies-server", about = "Start the server")]
struct Opt {
    /// Server hostname.
    #[structopt(short = "h", long = "hostname", default_value = "127.0.0.1")]
    hostname: String,

    /// Server port.
    #[structopt(short = "p", long = "port", default_value = "6480")]
    port: u16,

    /// Specify the zstd compression factor (irreversible)
    #[structopt(long = "compression-factor")]
    compression_factor: Option<i32>,

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
    info!("blocking subscription on {} spawned", stream);

    match stream.from {
        StartReadFrom::EventNumber(num) => {
            let mut next_number = EventNumber(num);
            let mut watcher = tree.watch_prefix(vec![]);

            for result in tree.scan(next_number.to_be_bytes()) {
                let (key, value) = result?;
                let number = EventNumber::try_from(key.as_slice()).unwrap();

                let raw_event = RawEvent::new(value);
                let event = Response::Event {
                    stream: stream.name.clone(),
                    number,
                    event_name: raw_event.name().unwrap(),
                    event_data: raw_event.data(),
                };

                match sender.send(Ok(event)).wait() {
                    Ok(s) => sender = s,
                    Err(_) => {
                        info!("encountered closed channel");
                        return Ok(());
                    }
                }

                next_number = number.next();
                watcher = tree.watch_prefix(vec![]);
            }

            for event in watcher {
                if let Event::Set(key, value) = event {
                    let number = EventNumber::try_from(key.as_slice()).unwrap();
                    if number >= next_number {
                        let raw_event = RawEvent::new(value);
                        let event = Response::Event {
                            stream: stream.name.clone(),
                            number,
                            event_name: raw_event.name().unwrap(),
                            event_data: raw_event.data(),
                        };

                        match sender.send(Ok(event)).wait() {
                            Ok(s) => sender = s,
                            Err(_) => {
                                info!("encountered closed channel");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        },
        StartReadFrom::End => {
            let watcher = tree.watch_prefix(vec![]);

            for event in watcher {
                if let Event::Set(key, value) = event {
                    let raw_event = RawEvent::new(value);
                    let event = Response::Event {
                        stream: stream.name.clone(),
                        number: EventNumber::try_from(key.as_slice()).unwrap(),
                        event_name: raw_event.name().unwrap(),
                        event_data: raw_event.data(),
                    };

                    match sender.send(Ok(event)).wait() {
                        Ok(s) => sender = s,
                        Err(_) => {
                            info!("encountered closed channel");
                            return Ok(());
                        }
                    }
                }
            }
        },
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
        Request::SubscribeAll { from } => {
            let tree_names = db.tree_names().into_iter().filter(|n| n != b"__sled__default");
            let stream_strings = tree_names.into_iter().map(|b| String::from_utf8(b).unwrap());
            let stream_names = stream_strings.map(|s| EsStreamName::new(s).unwrap());
            let all_streams: Vec<_> = stream_names.map(|n| EsStream::new(n, from)).collect();

            for stream in all_streams {
                let tree = db.open_tree(stream.name.clone().into_bytes())?;

                let subscribed = Response::Subscribed { stream: stream.name.clone() };
                match sender.send(Ok(subscribed)).wait() {
                    Ok(s) => sender = s,
                    Err(_) => {
                        info!("encountered closed channel");
                        return Ok(())
                    }
                }

                let sender = sender.clone();
                tokio::spawn(poll_fn(move || {
                    let mut sender = sender.clone();
                    let stream = stream.clone();
                    let tree = tree.clone();

                    tokio_threadpool::blocking(move || {
                        if let Err(e) = send_stream_events(stream, tree, sender.clone()) {
                            match sender.send(Err(e.to_string())).wait() {
                                Ok(s) => sender = s,
                                Err(_) => info!("encountered closed channel"),
                            }
                        }
                    })
                    .map_err(|e| error!("error; {}", e))
                }));
            }
        }
        Request::Subscribe { streams } => {
            for stream in streams {
                let mut sender = sender.clone();

                let tree = db.open_tree(stream.name.clone().into_bytes())?;

                let subscribed = Response::Subscribed { stream: stream.name.clone() };
                match sender.send(Ok(subscribed)).wait() {
                    Ok(s) => sender = s,
                    Err(_) => {
                        info!("encountered closed channel");
                        return Ok(())
                    },
                }

                tokio::spawn(poll_fn(move || {
                    let mut sender = sender.clone();
                    let stream = stream.clone();
                    let tree = tree.clone();

                    tokio_threadpool::blocking(move || {
                        if let Err(e) = send_stream_events(stream, tree, sender.clone()) {
                            match sender.send(Err(e.to_string())).wait() {
                                Ok(s) => sender = s,
                                Err(_) => info!("encountered closed channel"),
                            }
                        }
                    })
                    .map_err(|e| error!("error; {}", e))
                }));
            }
        },
        Request::Publish { stream, event_name, event_data } => {
            let tree = db.open_tree(stream.clone().into_bytes())?;

            let event_number = new_event_number(&db, &stream)?;
            let raw_length = event_name.as_str().len().to_be_bytes();
            let raw_name = event_name.as_str().as_bytes();
            let raw_data = event_data.0;

            let mut raw_event = Vec::new();
            raw_event.extend_from_slice(&raw_length);
            raw_event.extend_from_slice(&raw_name);
            raw_event.extend_from_slice(&raw_data);

            if let Err(e) = tree.set(event_number.to_be_bytes(), raw_event) {
                return Err(Error::InternalError(e))
            }

            db.flush()?;

            info!("{:?} {:?} {:?}", stream, event_name, event_number);

            if sender.send(Ok(Response::Ok)).wait().is_err() {
                info!("encountered closed channel");
            }
        },
        Request::LastEventNumber { stream } => {
            let key = db.get(&stream)?;
            let number = key.map(|k| EventNumber::try_from(k.as_ref()).unwrap());

            let last_event_number = Response::LastEventNumber { stream, number };
            if sender.send(Ok(last_event_number)).wait().is_err() {
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

    let mut builder = ConfigBuilder::new().path(opt.db_path);

    if let Some(compression_factor) = opt.compression_factor {
        builder = builder.use_compression(true).compression_factor(compression_factor);
    }

    let config = builder.build();

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

            let error_sender = sender.clone();

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
                    if error_sender.send(Err(error.to_string())).wait().is_err() {
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
