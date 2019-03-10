use std::str::FromStr;
use std::net::ToSocketAddrs;

use futures::stream::Stream;
use log::error;
use structopt::StructOpt;
use tokio::prelude::*;
use tokio_retry::{Retry, strategy::ExponentialBackoff};

use meilies_client::{sub_connect, paired_connect};
use meilies::stream::{StreamName, Stream as EsStream};

#[derive(Debug, StructOpt)]
#[structopt(name = "meilies-cli", about = "A basic cli for MeiliES.")]
struct Opt {
    /// Server hostname.
    #[structopt(short = "h", long = "hostname", default_value = "127.0.0.1")]
    hostname: String,

    /// Server port.
    #[structopt(short = "p", long = "port", default_value = "6480")]
    port: u16,

    /// Command and arguments that will be sent to the server.
    cmd_args: Vec<String>,
}

fn main() {
    let _ = stderrlog::new().color(stderrlog::ColorChoice::Never).verbosity(2).init();

    let opt = Opt::from_args();
    let addr = (opt.hostname.as_str(), opt.port);
    let addr = match addr.to_socket_addrs().map(|addrs| addrs.filter(|a| a.is_ipv4()).next()) {
        Ok(Some(addr)) => addr,
        Ok(None) => return error!("impossible to dns resolve addr; {:?}", addr),
        Err(e) => return error!("error parsing addr; {}", e),
    };

    let retry_strategy = ExponentialBackoff::from_millis(10).take(5);

    let cmd_args: Vec<_> = opt.cmd_args.iter().map(|s| s.as_str()).collect();
    let fut = match cmd_args.as_slice() {
        &["subscribe", stream] => {
            eprintln!("Reading events... (press Ctrl-C to quit)");

            let stream = EsStream::from_str(stream).unwrap();

            let fut = Retry::spawn(retry_strategy, move || {
                let stream = stream.clone();

                sub_connect(&addr)
                    .map_err(|e| eprintln!("{}", e))
                    .and_then(|conn| conn.subscribe_to(stream))
                    .and_then(|msgs| msgs.for_each(|msg| {
                        println!("{:?}", msg);
                        future::ok(())
                    }))
                    .map(|()| println!("Connection closed by the server"))
            })
            .map_err(|e| eprintln!("{:?}", e));

            future::Either::A(fut)
        },
        &["publish", stream, event] => {
            let stream = StreamName::from_str(stream).unwrap();
            let event = event.as_bytes().to_vec();

            let fut = Retry::spawn(retry_strategy, move || {
                let stream = stream.clone();
                let event = event.clone();

                paired_connect(&addr)
                    .map_err(|e| eprintln!("{}", e))
                    .and_then(|conn| conn.publish(stream, event))
                    .and_then(|_conn| future::ok(()))
                    .map(|()| println!("Event sent to the stream"))
            })
            .map_err(|e| eprintln!("{:?}", e));

            future::Either::B(fut)
        }
        unknow => return eprintln!("invalid command: {:?}", unknow),
    };

    tokio::run(fut);
}
