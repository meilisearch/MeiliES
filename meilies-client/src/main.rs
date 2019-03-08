use std::net::ToSocketAddrs;

use log::error;
use structopt::StructOpt;
use tokio::codec::Decoder;
use tokio::net::TcpStream;
use tokio::prelude::*;

use meilies::codec::{RespValue, RespMsgError, RespCodec};

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

    let socket = TcpStream::connect(&addr);

    let client = socket
        .map_err(|e| error!("error accepting socket; {}", e))
        .and_then(|socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();

            let must_listen = match opt.cmd_args.get(0) {
                Some(s) if s == "subscribe" => true,
                _ => false,
            };

            let args = opt.cmd_args.into_iter().map(RespValue::bulk_string).collect();
            let command = RespValue::Array(args);

            let writes = writer.send(command);

            let reader = if must_listen {
                Box::new(reader) as Box<Stream<Item=_, Error=_> + Send>
            } else {
                Box::new(reader.take(1)) as Box<Stream<Item=_, Error=_> + Send>
            };

            let messages = reader.for_each(|value| {
                match value {
                    RespValue::SimpleString(ref s) if s == "OK" => {
                        println!("received OK");
                        Ok(())
                    }
                    RespValue::Error(e) => {
                        eprintln!("error: {}", e);
                        Err(RespMsgError::SimpleStringContainCrlf)
                    },
                    other => {
                        println!("received: {:?}", other);
                        Ok(())
                    }
                }
            }).then(|_| Ok(()));

            tokio::spawn(messages);

            writes.then(|_| Ok(()))
        });

    tokio::run(client);
}
