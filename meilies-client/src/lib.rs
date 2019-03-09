use std::io;
use std::net::SocketAddr;

use futures::stream::{SplitSink, SplitStream};
use futures::{Future, Poll, Async, Stream, Sink};
use tokio::codec::{Decoder, Framed};
use tokio::net::TcpStream;
use meilies::codec::{RespCodec, RespValue};
use meilies::stream::{Stream as EsStream, EventNumber};

mod sub;

pub use self::sub::{SubStream, SubConnection};

pub type RespConnection = Framed<TcpStream, RespCodec>;
pub type RespConnectionWriter = SplitSink<Framed<TcpStream, RespCodec>>;
pub type RespConnectionReader = SplitStream<Framed<TcpStream, RespCodec>>;

pub fn connect(addr: &SocketAddr) -> impl Future<Item=RespConnection, Error=io::Error> {
    TcpStream::connect(addr).map(|socket| RespCodec::default().framed(socket))
}

pub fn sub_connect(addr: &SocketAddr) -> impl Future<Item=SubConnection, Error=io::Error> {
    connect(&addr).map(SubConnection::new)
}
