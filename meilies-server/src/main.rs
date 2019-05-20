

use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use log::{info, error, warn};

use structopt::StructOpt;
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::sync::mpsc;

use meilies::reqresp::{ServerCodec, Request, Response};
use meilies::reqresp::{RequestMsgError, ResponseMsgError};
use meilies::stream::{EventNumber, Stream as EsStream, StreamName as EsStreamName, ReadPosition};
use meilies::resp::{RespMsgError, RespVecConvertError, RespBytesConvertError};

use meilies_server::{StreamStore, StoreConfigBuilder};

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

    /// Specify the frequency of snapshot are created
    ///
    /// Optional. By default they will be no other snapshots
    #[structopt(long = "snapshot-frequency")]
    snapshot_frequency: Option<usize>,

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

impl std::error::Error for Error {}

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

fn send_stream_events(
    stream: EsStream,
    store: Arc<StreamStore>,
    sender: mpsc::Sender<Result<Response, String>>,
) -> sled::Result<()>
{
    info!("blocking subscription on {} spawned", stream);

    match (stream.from, stream.to) {
        (ReadPosition::EventNumber(from), to) => {
            if let Err(err) = store.subscribe_to(stream.name.as_str(), from, to, sender) {
                warn!("Error during susbcription; {:?}", err);
            }
        },
        (ReadPosition::End, _) => {
            if let Err(err) = store.send_subscribed(stream.name.as_str(), 0, None, sender) {
                warn!("Error during susbcription from end; {:?}", err);
            }
        },
    }

    Ok(())
}
fn handle_request(
    request: Request,
    store: Arc<StreamStore>,
    sender: mpsc::Sender<Result<Response, String>>
) -> Result<(), Error>
{
    info!("handle request");
    match request {
        Request::SubscribeAll { from, to } => {
            let streams = store.get_stream_names().into_iter()
                .filter_map(|n| EsStreamName::new(n).ok())
                .map(|n| EsStream::new(n, from, to));

            for stream in streams {
                let sender = sender.clone();
                let store = store.clone();

                thread::Builder::new().spawn(|| {
                    let mut sender = sender;

                    let subscribed = Response::Subscribed { stream: stream.name.clone() };
                    match sender.send(Ok(subscribed)).wait() {
                        Ok(s) => sender = s,
                        Err(_) => {
                            warn!("encountered closed channel");
                            return;
                        }
                    }

                    if let Err(e) = send_stream_events(stream, store, sender.clone()) {
                        if let Err(_) = sender.send(Err(e.to_string())).wait() {
                            warn!("encountered closed channel");
                            return;
                        }
                    }
                })?;
            }
        }
        Request::Subscribe { streams } => {
            for stream in streams {
                let sender = sender.clone();
                let store = store.clone();

                thread::Builder::new().spawn(|| {
                    let mut sender = sender;

                    let subscribed = Response::Subscribed { stream: stream.name.clone() };
                    match sender.send(Ok(subscribed)).wait() {
                        Ok(s) => sender = s,
                        Err(_) => {
                            info!("encountered closed channel");
                            return;
                        }
                    }

                    if let Err(e) = send_stream_events(stream, store, sender.clone()) {
                        if let Err(_) = sender.send(Err(e.to_string())).wait() {
                            info!("encountered closed channel");
                            return;
                        }
                    }
                })?;
            }
        },
        Request::Publish { stream, event_name, event_data } => {
            if let Err(e) = store.save_event(stream.as_str(), event_name.as_str(), event_data.0) {
                if let Err(_) = sender.send(Err(e.to_string())).wait() {
                    warn!("Error during pushing a new event; {:?}", e);
                }
            } else {
                if sender.send(Ok(Response::Ok)).wait().is_err() {
                    warn!("encountered closed channel");
                }
            }
        },
        Request::RequestSnapshot { stream, number } => {
            unreachable!("TODO")
        },
        Request::PublishSnapshot { stream, snapshot_ref, data } => {
            unreachable!("TODO")
        },
        Request::LastEventNumber { stream } => {
            let response = match store.last_event_number(stream.as_str())? {
                Some(v) => {
                    let number = EventNumber(v);
                    Response::LastEventNumber { stream: stream, number: Some(number) }
                },
                None => {
                    Response::LastEventNumber { stream: stream, number: None }
                }
            };

            if sender.send(Ok(response)).wait().is_err() {
                warn!("encountered closed channel");
            }
        },
        Request::StreamNames => {
            let stream_names = store.get_stream_names().into_iter()
                .filter_map(|n| EsStreamName::new(n).ok()).collect();

            let streams = Response::StreamNames { streams: stream_names };

            if sender.send(Ok(streams)).wait().is_err() {
                warn!("encountered closed channel");
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

    let mut builder = StoreConfigBuilder::new();

    if let Some(compression_factor) = opt.compression_factor {
        builder = builder.compression_factor(compression_factor);
    }

    if let Some(snapshot_frequency) = opt.snapshot_frequency {
        builder = builder.snapshot_min_frequency(snapshot_frequency);
    }

    let store = Arc::new(StreamStore::new(opt.db_path, builder.build()).unwrap());

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

            let store = store.clone();
            let requests = reader
                .map_err(Error::RequestMsgError)
                .for_each(move |request| {
                    let store = store.clone();
                    let sender = sender.clone();
                    future::result(handle_request(request, store, sender))
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
