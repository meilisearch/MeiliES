use std::io::Write;
use std::net::ToSocketAddrs;
use std::process::{Command, Stdio};

use futures::executor::ThreadPool;
use futures::StreamExt;
use meilies::reqresp::Response;
use meilies::stream::Stream as EsStream;
use meilies_client::sub_connect;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "meilies-inspect",
    about = "A cli to inspect MeiliES events by executing a command on each event.",
)]
struct Opt {
    /// Server hostname.
    #[structopt(short = "h", long = "hostname", default_value = "127.0.0.1")]
    hostname: String,

    /// Server port.
    #[structopt(short = "p", long = "port", default_value = "6480")]
    port: u16,

    /// Stream to follow on which events need to be inspected.
    #[structopt(long = "stream", parse(try_from_str))]
    esstream: EsStream,

    /// Command and arguments that will interpret the event data piped in stdin.
    ///
    /// `MEILIES_STREAM_NAME` contains the stream name.
    /// `MEILIES_EVENT_NAME` contains the event name.
    /// `MEILIES_EVENT_NUMBER` contains the event number.
    command: String,
}

fn main() {
    let Opt { hostname, port, esstream, command } = Opt::from_args();

    let addr = (hostname.as_str(), port);
    let addr = match addr.to_socket_addrs().map(|addrs| addrs.filter(|a| a.is_ipv4()).next()) {
        Ok(Some(addr)) => addr,
        Ok(None) => panic!("impossible to dns resolve addr; {:?}", addr),
        Err(e) => panic!("error parsing addr; {}", e),
    };

    let pool = ThreadPool::new().unwrap();

    pool.clone().run(async move {
        let (mut ctrl, mut stream) = sub_connect(&pool, addr).await.unwrap();

        ctrl.subscribe_to(esstream).await.unwrap();

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Ok(Response::Event { stream, number, event_name, event_data })) => {
                    eprintln!("processing event number {}", number.0);

                    let result = Command::new("/bin/bash")
                        .arg("-c")
                        .arg(&command)
                        .stdin(Stdio::piped())
                        .env("MEILIES_STREAM_NAME",  stream.into_inner())
                        .env("MEILIES_EVENT_NAME",   event_name.into_inner())
                        .env("MEILIES_EVENT_NUMBER", number.0.to_string())
                        .spawn();

                    let mut child = match result {
                        Ok(child) => child,
                        Err(e) => return eprintln!("{}", e),
                    };

                    let data = event_data.0.as_slice();
                    if let Err(e) = child.stdin.as_mut().unwrap().write_all(data) {
                        return eprintln!("{}", e);
                    }

                    let output = match child.wait_with_output() {
                        Ok(output) => output,
                        Err(e) => return eprintln!("{}", e),
                    };

                    if !output.status.success() {
                        return eprintln!("command execution was not successful")
                    }
                },
                Ok(Ok(otherwise)) => println!("{:?}", otherwise),
                Ok(Err(e)) => return eprintln!("{}", e),
                Err(e) => return eprintln!("{}", e),
            }
        }

        println!("Connection closed by the server");
    });
}
