use std::net::ToSocketAddrs;

use meilies::stream::Stream as EsStream;
use meilies::reqresp::Response;
use meilies_client::{sub_connect, PairedConnection};
use futures::executor::ThreadPool;
use futures::StreamExt;
use structopt::StructOpt;
use log::{info, error};

#[derive(Debug, StructOpt)]
#[structopt(name = "meilies-transhumance", about = "A basic migration tool for MeiliES.")]
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

    let src_server = match opt.src_server.to_socket_addrs().map(|addrs| addrs.filter(|a| a.is_ipv4()).next()) {
        Ok(Some(addr)) => addr,
        Ok(None) => return error!("impossible to dns resolve the source addr; {:?}", opt.src_server),
        Err(e) => return error!("error parsing addr; {}", e),
    };

    let dst_server = match opt.dst_server.to_socket_addrs().map(|addrs| addrs.filter(|a| a.is_ipv4()).next()) {
        Ok(Some(addr)) => addr,
        Ok(None) => return error!("impossible to dns resolve the destination addr; {:?}", opt.dst_server),
        Err(e) => return error!("error parsing addr; {}", e),
    };

    if src_server == dst_server {
        return error!("the source and destination can not be the same")
    }

    let pool = ThreadPool::new().unwrap();

    pool.clone().run(async move {
        let (mut ctrl, mut stream) = sub_connect(&pool, src_server).await.unwrap();

        for stream in opt.streams {
            ctrl.subscribe_to(stream).await.unwrap();
        }

        let mut dst_conn = PairedConnection::connect(&dst_server).await.unwrap();

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Ok(Response::Event { stream, number, event_name, event_data })) => {
                    info!("{:?} {:?} {:?}", stream, event_name, number);
                    dst_conn = dst_conn.publish(stream, event_name, event_data).await.unwrap();
                },
                Ok(Ok(otherwise)) => println!("{:?}", otherwise),
                Ok(Err(e)) => error!("{}", e),
                Err(e) => error!("{}", e),
            }
        }

        println!("Connection closed by the server");
    });
}
