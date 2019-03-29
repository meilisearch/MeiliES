use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::Future;
use tokio::codec::{Decoder, Framed};
use tokio::net::TcpStream;
use meilies::reqresp::ClientCodec;
use log::warn;

mod sub;
mod paired;
mod steel_connection;

pub use self::sub::{sub_connect, SubStream, SubController, ProtocolError};
pub use self::paired::{paired_connect, PairedConnection};
use self::steel_connection::{retry_strategy, SteelConnection};

pub type ClientConnection = Framed<TcpStream, ClientCodec>;
pub type ClientConnectionWriter = SplitSink<Framed<TcpStream, ClientCodec>>;
pub type ClientConnectionReader = SplitStream<Framed<TcpStream, ClientCodec>>;

/// Open a framed connection with a server using RESP
pub fn connect(addr: &SocketAddr) -> impl Future<Item=ClientConnection, Error=io::Error> {
    TcpStream::connect(addr)
        .map(|socket| {
            let duration = Duration::from_millis(50);
            if let Err(e) = socket.set_keepalive(Some(duration)) {
                warn!("set_keepalive error; {}", e);
            }

            ClientCodec::default().framed(socket)
        })
}
