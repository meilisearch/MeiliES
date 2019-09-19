use std::convert::TryFrom;
use std::fmt;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use async_std::net::TcpListener;
use futures::executor::ThreadPool;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use futures_codec::Framed;
use log::{info, error};
use sled::{Db, Tree, IVec, Event, ConfigBuilder};
use structopt::StructOpt;

use meilies::reqresp::{ServerCodec, Request, Response};
use meilies::reqresp::RequestMsgError;
use meilies::stream::{RawEvent, EventNumber, Stream as EsStream, StreamName as EsStreamName, ReadRange};
use meilies::resp::{RespVecConvertError, RespBytesConvertError};

fn new_event_number(numbers: &Tree, name: &EsStreamName) -> sled::Result<EventNumber> {
    let new_value = numbers.update_and_fetch(name, |previous| {
        let previous = previous.map(|s| EventNumber::try_from(s).unwrap());
        let new = previous.map_or(EventNumber::zero(), EventNumber::next);
        let slice = &new.to_be_bytes()[..];
        Some(IVec::from(slice))
    })?;

    Ok(EventNumber::try_from(new_value.unwrap().as_ref()).unwrap())
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

    /// Disable vigil initialization.
    #[structopt(long = "no-vigil")]
    no_vigil: bool,

    /// Disable sentry initialization.
    #[structopt(long = "no-sentry")]
    no_sentry: bool,

    /// Database path
    #[structopt(long = "db-path", parse(from_os_str), default_value = "/var/lib/meilies")]
    db_path: PathBuf,
}

#[derive(Debug)]
enum Error {
    RequestMsgError(RequestMsgError),
    InvalidRequest,
    InternalError(sled::Error),
    IoError(IoError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::RequestMsgError(e) => write!(f, "invalid request message; {}", e),
            Error::InvalidRequest => write!(f, "invalid request"),
            Error::InternalError(e) => write!(f, "internal error; {}", e),
            Error::IoError(e) => write!(f, "io error; {}", e),
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

impl From<IoError> for Error {
    fn from(error: IoError) -> Error {
        Error::IoError(error)
    }
}

async fn send_stream_events(
    stream: EsStream,
    tree: Arc<Tree>,
    mut sender: mpsc::Sender<Result<Response, String>>,
) -> sled::Result<()>
{
    info!("blocking subscription on {} spawned", stream);

    match stream.range {
        ReadRange::ReadFrom(from) => {
            let mut next_number = EventNumber(from);
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

                if sender.send(Ok(event)).await.is_err() {
                    info!("encountered closed channel");
                    return Ok(());
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

                        if sender.send(Ok(event)).await.is_err() {
                            info!("encountered closed channel");
                            return Ok(());
                        }
                    }
                }
            }
        },
        ReadRange::ReadFromUntil(from, to) => {
            let mut next_number = EventNumber(from);
            let to_event_number = EventNumber(to);
            let mut watcher = tree.watch_prefix(vec![]);

            for result in tree.range(next_number.to_be_bytes()..to_event_number.to_be_bytes()) {
                let (key, value) = result?;
                let number = EventNumber::try_from(key.as_slice()).unwrap();

                let raw_event = RawEvent::new(value);
                let event = Response::Event {
                    stream: stream.name.clone(),
                    number,
                    event_name: raw_event.name().unwrap(),
                    event_data: raw_event.data(),
                };

                if sender.send(Ok(event)).await.is_err() {
                    info!("encountered closed channel");
                    return Ok(());
                }

                next_number = number.next();
                if next_number >= to_event_number {
                    return Ok(());
                }
                watcher = tree.watch_prefix(vec![]);
            }

            for event in watcher {
                if let Event::Set(key, value) = event {
                    let number = EventNumber::try_from(key.as_slice()).unwrap();
                    if number >= to_event_number {
                        return Ok(());
                    }
                    if number >= next_number {
                        let raw_event = RawEvent::new(value);
                        let event = Response::Event {
                            stream: stream.name.clone(),
                            number,
                            event_name: raw_event.name().unwrap(),
                            event_data: raw_event.data(),
                        };

                        if sender.send(Ok(event)).await.is_err() {
                            info!("encountered closed channel");
                            return Ok(());
                        }
                    }
                }
            }
        },
        ReadRange::ReadFromEnd => {
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

                    if sender.send(Ok(event)).await.is_err() {
                        info!("encountered closed channel");
                        return Ok(());
                    }
                }
            }
        },
    }

    Ok(())
}

async fn handle_request(
    request: Request,
    db: Db,
    mut sender: mpsc::Sender<Result<Response, String>>,
) -> Result<(), Error>
{
    match request {
        Request::SubscribeAll { range } => {
            let tree_names = db.tree_names().into_iter().filter(|n| n != b"__sled__default");
            let stream_strings = tree_names.into_iter().map(|b| String::from_utf8(b).unwrap());
            let stream_names = stream_strings.map(|s| EsStreamName::new(s).unwrap());
            let all_streams: Vec<_> = stream_names.map(|n| EsStream::new(n, range)).collect();

            for stream in all_streams {
                let sender = sender.clone();
                let tree = db.open_tree(stream.name.clone().into_bytes())?;

                thread::Builder::new().spawn(|| {
                    ThreadPool::new().unwrap().run(async {
                        let mut sender = sender;

                        let subscribed = Response::Subscribed { stream: stream.name.clone() };
                        if sender.send(Ok(subscribed)).await.is_err() {
                            info!("encountered closed channel");
                            return;
                        }

                        if let Err(e) = send_stream_events(stream, tree, sender.clone()).await {
                            if sender.send(Err(e.to_string())).await.is_err() {
                                info!("encountered closed channel");
                                return;
                            }
                        }
                    });
                })?;
            }
        }
        Request::Subscribe { streams } => {
            for stream in streams {
                let sender = sender.clone();
                let tree = db.open_tree(stream.name.clone().into_bytes())?;

                thread::Builder::new().spawn(|| {
                    ThreadPool::new().unwrap().run(async {
                        let mut sender = sender;

                        let subscribed = Response::Subscribed { stream: stream.name.clone() };
                        if sender.send(Ok(subscribed)).await.is_err() {
                            info!("encountered closed channel");
                            return;
                        }

                        if let Err(e) = send_stream_events(stream, tree, sender.clone()).await {
                            if sender.send(Err(e.to_string())).await.is_err() {
                                info!("encountered closed channel");
                                return;
                            }
                        }
                    });
                })?;
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

            info!("{:?} {:?} {:?}", stream, event_name, event_number);

            if sender.send(Ok(Response::Ok)).await.is_err() {
                info!("encountered closed channel");
            }
        },
        Request::LastEventNumber { stream } => {
            let key = db.get(&stream)?;
            let number = key.map(|k| EventNumber::try_from(k.as_ref()).unwrap());

            let last_event_number = Response::LastEventNumber { stream, number };
            if sender.send(Ok(last_event_number)).await.is_err() {
                info!("encountered closed channel");
            }
        },
        Request::StreamNames => {
            let tree_names = db.tree_names().into_iter().filter(|n| n != b"__sled__default");
            let stream_strings = tree_names.into_iter().map(|b| String::from_utf8(b).unwrap());
            let stream_names = stream_strings.map(|s| EsStreamName::new(s).unwrap()).collect();
            let streams = Response::StreamNames { streams: stream_names };

            if sender.send(Ok(streams)).await.is_err() {
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

#[cfg(feature = "vigil")]
fn init_vigil() {
    use vigil::Reporter;
    use std::{env, time::Duration};

    let endpoint = env::var("VIGIL_ENDPOINT").expect("VIGIL_ENDPOINT");
    let token = env::var("VIGIL_TOKEN").expect("VIGIL_TOKEN");
    let probe = env::var("VIGIL_PROBE").expect("VIGIL_PROBE");
    let node = env::var("VIGIL_NODE").expect("VIGIL_NODE");
    let replica = env::var("VIGIL_REPLICA").expect("VIGIL_REPLICA");

    let reporter = Reporter::new(&endpoint, &token)
        .probe_id(&probe)
        .node_id(&node)
        .replica_id(&replica)
        .interval(Duration::from_secs(10))
        .build();

    reporter.run().expect("Can not start vigil");

    eprintln!("I am vigiled! 🎉");
}

fn main() {
    let opt = Opt::from_args();

    #[cfg(feature = "sentry")]
    { if !opt.no_sentry { init_sentry(); } }

    #[cfg(feature = "vigil")]
    { if !opt.no_vigil { init_vigil(); } }

    if !cfg!(feature = "sentry") || opt.no_sentry {
        let _ = env_logger::init();
    }

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

    let mut pool = ThreadPool::new().unwrap();
    let cloned_pool = pool.clone();

    pool.run(async move {
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => return error!("error binding address; {}", e),
        };
        println!("server is listening on {}", addr);

        listener
            .incoming()
            .for_each_concurrent(None, |result| async {
                let socket = match result {
                    Ok(socket) => socket,
                    Err(e) => return error!("error; {}", e),
                };

                let framed = Framed::new(socket, ServerCodec);
                let (mut writer, mut reader) = framed.split();
                let (mut sender, mut receiver) = mpsc::channel(10);

                let db = db.clone();
                let mut error_sender = sender.clone();

                cloned_pool.spawn_ok(async move {
                    while let Some(result) = reader.next().await {
                        let result = match result {
                            Ok(request) => handle_request(request, db.clone(), sender.clone()).await,
                            Err(e) => Err(Error::RequestMsgError(e)),
                        };

                        if let Err(e) = result {
                            error!("error; {}", e);
                            if error_sender.send(Err(e.to_string())).await.is_err() {
                                info!("encountered closed channel");
                            }
                        }
                    }
                });

                cloned_pool.spawn_ok(async move {
                    while let Some(result) = receiver.next().await {
                        match result {
                            Ok(msg) => {
                                if let Err(e) = writer.send(Ok(msg)).await {
                                    error!("error; {}", e)
                                }
                            },
                            Err(e) => info!("{}", e),
                        }
                    }
                });
            })
            .await

    });
}
