use std::net::ToSocketAddrs;

use futures::future::Either;
use futures::{future, Future, Stream};
use log::{error, info};
use meilies::reqresp::Response;
use meilies::stream::Stream as EsStream;
use meilies_client::{paired_connect, sub_connect};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "meilies-transhumance",
    about = "A basic migration tool for MeiliES.",
    author
)]
struct Opt {
    /// Source server address (i.e. localhost:6480).
    #[structopt(long = "src-server")]
    src_server: String,

    /// Destination server address (i.e. localhost:6481).
    #[structopt(long = "dst-server")]
    dst_server: String,

    /// List of streams to migrate from the source server to the destination one
    /// (i.e. hello:10, super-stream).
    ///
    /// Warning: if you want to migrate all the events from a stream
    /// you must specify `:0` after the stream name.
    streams: Vec<EsStream>,
}

fn main() {
    let _ = stderrlog::new().verbosity(2).init();

    let opt = Opt::from_args();

    let src_server = match opt
        .src_server
        .to_socket_addrs()
        .map(|addrs| addrs.filter(|a| a.is_ipv4()).next())
    {
        Ok(Some(addr)) => addr,
        Ok(None) => {
            return error!(
                "impossible to dns resolve the source addr; {:?}",
                opt.src_server
            )
        }
        Err(e) => return error!("error parsing addr; {}", e),
    };

    let dst_server = match opt
        .dst_server
        .to_socket_addrs()
        .map(|addrs| addrs.filter(|a| a.is_ipv4()).next())
    {
        Ok(Some(addr)) => addr,
        Ok(None) => {
            return error!(
                "impossible to dns resolve the destination addr; {:?}",
                opt.dst_server
            )
        }
        Err(e) => return error!("error parsing addr; {}", e),
    };

    if src_server == dst_server {
        return error!("the source and destination can not be the same");
    }

    let fut = sub_connect(src_server)
        .map_err(|e| error!("{}", e))
        .and_then(move |(mut ctrl, msgs)| {
            for stream in opt.streams {
                ctrl.subscribe_to(stream);
            }

            paired_connect(dst_server)
                .map_err(|e| error!("{}", e))
                .and_then(|dst_conn| {
                    msgs.map_err(|e| error!("{}", e))
                        .fold(dst_conn, move |dst_conn, msg| match msg {
                            Ok(Response::Event {
                                stream,
                                number,
                                event_name,
                                event_data,
                            }) => {
                                info!("{:?} {:?} {:?}", stream, event_name, number);
                                Either::A(
                                    dst_conn
                                        .publish(stream, event_name, event_data)
                                        .map_err(|e| error!("{}", e)),
                                )
                            }
                            Ok(response) => {
                                info!("{:?}", response);
                                Either::B(future::ok(dst_conn))
                            }
                            Err(error) => {
                                error!("{}", error);
                                Either::B(future::ok(dst_conn))
                            }
                        })
                })
        })
        .and_then(|_| Err(println!("Connection closed by the server")));

    tokio::run(fut);
}
