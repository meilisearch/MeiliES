use std::io;
use std::net::SocketAddr;

use futures::stream::{SplitSink, SplitStream};
use futures::{Future, Poll, Async, Stream, Sink};
use tokio::codec::{Decoder, Framed};
use tokio::net::TcpStream;
use meilies::codec::{RespCodec, RespValue};
use meilies::stream::{Stream as EsStream, EventNumber};

pub type RespConnection = Framed<TcpStream, RespCodec>;
pub type RespConnectionWriter = SplitSink<Framed<TcpStream, RespCodec>>;
pub type RespConnectionReader = SplitStream<Framed<TcpStream, RespCodec>>;

pub fn connect(addr: &SocketAddr) -> impl Future<Item=RespConnection, Error=io::Error> {
    TcpStream::connect(addr).map(|socket| RespCodec::default().framed(socket))
}

pub fn sub_connect(addr: &SocketAddr) -> impl Future<Item=SubConnection, Error=io::Error> {
    connect(&addr).map(SubConnection::new)
}

pub struct SubConnection {
    connection: RespConnection,
}

impl SubConnection {
    pub fn new(connection: RespConnection) -> SubConnection {
        SubConnection { connection }
    }

    pub fn subscribe_to(self, stream: EsStream) -> impl Future<Item=SubStream, Error=()> {
        let command = RespValue::Array(vec![
            RespValue::bulk_string("subscribe"),
            RespValue::bulk_string(stream.to_string()),
        ]);

        self.connection
            .send(command)
            .map_err(|e| eprintln!("error: {:?}", e))
            .and_then(|framed| {
                let (_writer, reader) = framed.split();
                reader.into_future().map_err(|_| ())
            })
            .and_then(|(first_msg, reader)| {
                let array = match first_msg {
                    Some(RespValue::Array(array)) => array,
                    _ => return Err(()),
                };

                let mut iter = array.into_iter();
                match (iter.next(), iter.next(), iter.next()) {
                    (Some(RespValue::SimpleString(type_)), Some(RespValue::Array(streams)), None) => {
                        if type_ == "subscribed" {
                            println!("subscribed to {:?}", streams);
                            Ok(SubStream { stream, connection: reader })
                        } else {
                            Err(())
                        }
                    },
                    _ => Err(()),
                }
            })
    }
}

pub struct SubStream {
    stream: EsStream,
    connection: RespConnectionReader,
}

impl Stream for SubStream {
    type Item = (EsStream, EventNumber, Vec<u8>);
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let array = match self.connection.poll().map_err(|_| ()) {
            Ok(Async::Ready(Some(RespValue::Array(array)))) => array,
            Ok(Async::Ready(Some(other))) => return Err(()),
            Ok(Async::Ready(None)) => return Ok(Async::Ready(None)),
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(e) => return Err(e),
        };

        let mut iter = array.into_iter();
        match (iter.next(), iter.next(), iter.next(), iter.next(), iter.next()) {
            (Some(type_), Some(stream), Some(RespValue::Integer(number)), Some(RespValue::BulkString(event)), None) => {
                if type_ == RespValue::SimpleString("event".into())
                && stream == RespValue::bulk_string(self.stream.to_string()) {
                    println!("event from {:?}", stream);
                    Ok(Async::Ready(Some((self.stream.clone(), EventNumber(number as u64), event))))
                } else {
                    Err(())
                }
            },
            _ => Err(()),
        }
    }
}
