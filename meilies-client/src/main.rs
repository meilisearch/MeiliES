use std::str::FromStr;
use std::net::ToSocketAddrs;

use log::error;
use structopt::StructOpt;
use tokio::prelude::*;
use futures::stream::Stream;

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

    let cmd_args: Vec<_> = opt.cmd_args.iter().map(|s| s.as_str()).collect();
    let client = match cmd_args.as_slice() {
        &["subscribe", stream] => {
            eprintln!("Reading events... (press Ctrl-C to quit)");

            let stream = EsStream::from_str(stream).unwrap();

            let client = sub_connect(&addr)
                .map_err(|e| eprintln!("{}", e))
                .and_then(|conn| conn.subscribe_to(stream))
                .and_then(|msgs| msgs.for_each(|msg| {
                    println!("{:?}", msg);
                    future::ok(())
                }));

            future::Either::A(client)
        },
        &["publish", stream, event] => {
            let stream = StreamName::from_str(stream).unwrap();
            let event = event.as_bytes().to_vec();

            let client = paired_connect(&addr)
                .map_err(|e| eprintln!("{}", e))
                .and_then(|conn| conn.publish(stream, event))
                .and_then(|_conn| future::ok(()));

            future::Either::B(client)
        }
        _ => unimplemented!(),
    };

    tokio::run(client);
}
