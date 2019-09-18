use std::net::ToSocketAddrs;

use futures::executor::ThreadPool;
use futures::stream::StreamExt;

use meilies::reqresp::Request;
use meilies::resp::{RespValue, FromResp};
use meilies::stream::Stream as EsStream;
use meilies_client::{sub_connect, PairedConnection};

use log::error;
use structopt::StructOpt;

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
    let _ = stderrlog::new().verbosity(2).init();

    let opt = Opt::from_args();
    let addr = (opt.hostname.as_str(), opt.port);
    let addr = match addr.to_socket_addrs().map(|addrs| addrs.filter(|a| a.is_ipv4()).next()) {
        Ok(Some(addr)) => addr,
        Ok(None) => return error!("impossible to dns resolve addr; {:?}", addr),
        Err(e) => return error!("error parsing addr; {}", e),
    };

    let args = opt.cmd_args.into_iter().map(RespValue::bulk_string).collect();
    let args = RespValue::Array(args);
    let command = match Request::from_resp(args) {
        Ok(command) => command,
        Err(e) => return error!("{}", e),
    };

    let pool = ThreadPool::new().unwrap();

    pool.clone().run(async move {
        match command {
            Request::SubscribeAll { range } => {
                let (mut ctrl, mut stream) = sub_connect(&pool, addr).await.unwrap();

                ctrl.subscribe_to(EsStream::all(range)).await.unwrap();

                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(response) => println!("{:?}", response),
                        Err(error) => eprintln!("Error: {}", error),
                    }
                }

                println!("Connection closed by the server");
            },
            Request::Subscribe { streams } => {
                let (mut ctrl, mut stream) = sub_connect(&pool, addr).await.unwrap();

                for stream in streams {
                    ctrl.subscribe_to(stream).await.unwrap();
                }

                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(response) => println!("{:?}", response),
                        Err(error) => eprintln!("Error: {}", error),
                    }
                }

                println!("Connection closed by the server");

            },
            Request::Publish { stream, event_name, event_data } => {
                let conn = PairedConnection::connect(&addr).await.unwrap();
                if let Err(e) = conn.publish(stream, event_name, event_data).await {
                    error!("{}", e);
                }
            },
            Request::LastEventNumber { stream } => {
                let conn = PairedConnection::connect(&addr).await.unwrap();
                match conn.last_event_number(stream).await {
                    Ok((stream, number, _conn)) => println!("{} - {:?}", stream, number),
                    Err(e) => error!("{}", e),
                }
            },
            Request::StreamNames => {
                let conn = PairedConnection::connect(&addr).await.unwrap();
                match conn.stream_names().await {
                    Ok((streams, _conn)) => println!("{:?}", streams),
                    Err(e) => error!("{}", e),
                }
            }
        }
    });
}
