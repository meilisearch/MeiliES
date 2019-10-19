use std::io::{Error, ErrorKind, Write};
use std::net::ToSocketAddrs;
use std::process::{Command, Stdio};

use futures::future::{self, Future};
use futures::stream::Stream;
use meilies::reqresp::Response;
use meilies::stream::Stream as EsStream;
use meilies_client::sub_connect;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "meilies-inspect",
    about = "A cli to inspect MeiliES events by executing a command on each event."
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
    stream: EsStream,

    /// Command and arguments that will interpret the event data piped in stdin.
    ///
    /// `MEILIES_STREAM_NAME` contains the stream name.
    /// `MEILIES_EVENT_NAME` contains the event name.
    /// `MEILIES_EVENT_NUMBER` contains the event number.
    command: String,
}

fn future_io_err<T, E>(error: E) -> future::FutureResult<T, std::io::Error>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    future::err(Error::new(ErrorKind::Other, error))
}

fn main() {
    let Opt {
        hostname,
        port,
        stream,
        command,
    } = Opt::from_args();

    let addr = (hostname.as_str(), port);
    let addr = match addr
        .to_socket_addrs()
        .map(|addrs| addrs.filter(|a| a.is_ipv4()).next())
    {
        Ok(Some(addr)) => addr,
        Ok(None) => panic!("impossible to dns resolve addr; {:?}", addr),
        Err(e) => panic!("error parsing addr; {}", e),
    };

    let fut = sub_connect(addr)
        .map_err(|e| eprintln!("{}", e))
        .and_then(move |(mut ctrl, msgs)| {
            ctrl.subscribe_to(stream);
            msgs.map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
                .for_each(move |msg| match msg {
                    Ok(Response::Event {
                        stream,
                        number,
                        event_name,
                        event_data,
                    }) => {
                        eprintln!("processing event number {}", number.0);

                        let result = Command::new("/bin/bash")
                            .arg("-c")
                            .arg(&command)
                            .stdin(Stdio::piped())
                            .env("MEILIES_STREAM_NAME", stream.into_inner())
                            .env("MEILIES_EVENT_NAME", event_name.into_inner())
                            .env("MEILIES_EVENT_NUMBER", number.0.to_string())
                            .spawn();

                        let mut child = match result {
                            Ok(child) => child,
                            Err(e) => return future::err(e),
                        };

                        let data = event_data.0.as_slice();
                        if let Err(e) = child.stdin.as_mut().unwrap().write_all(data) {
                            return future::err(e);
                        }

                        let output = match child.wait_with_output() {
                            Ok(output) => output,
                            Err(e) => return future::err(e),
                        };

                        if !output.status.success() {
                            return future_io_err("command execution was not successful");
                        }

                        future::ok(())
                    }
                    Ok(_response) => future::ok(()),
                    Err(error) => future_io_err(format!("Error: {}", error)),
                })
                .map_err(|e| eprintln!("{}", e))
        })
        .and_then(|_| {
            println!("Connection closed by the server");
            Err(())
        });

    tokio::run(fut);
}
