use std::{env, str};
use std::time::{Instant, Duration};
use std::io::BufReader;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use futures::sync::mpsc::{channel, Sender, Receiver};
use futures::future::poll_fn;
use tokio::io::{self, lines, write_all};
use tokio::net::TcpListener;
use tokio::timer::Interval;
use tokio::prelude::*;
use tokio::codec::{Encoder, Decoder};
use tokio::sync::mpsc;
use bytes::{BufMut, BytesMut};
use log::{info, error};
use sled::{Db, Event};

use meilies::codec::{RespCodec, RespValue, RespMsgError};

fn main() {
    let _ = env_logger::init();

    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(e) => return error!("error pasing addr; {}", e),
    };

    let now = Instant::now();
    let db = Db::start_default("test-db").unwrap();
    info!("sled loaded in {:.2?}", now.elapsed());

    let listener = TcpListener::bind(&addr).unwrap();

    println!("server is running on {}", addr);

    let server = listener
        .incoming()
        .map_err(|e| error!("error accepting socket; {}", e))
        .for_each(move |socket| {
            let framed = RespCodec::default().framed(socket);
            let (writer, reader) = framed.split();

            let db = db.clone();
            let responses = reader.map(move |line| {

                Ok(line)

                // let mut parts = line.split(|c| c == ' ' || c == '\n');

                // match (parts.next(), parts.next()) {
                //     (Some("SUBSCRIBE"), Some(stream_name)) => {

                //         println!("subscription to {:?}", stream_name);

                //         let db = db.clone();
                //         let stream_name = stream_name.as_bytes().to_vec();
                //         let (mut tx, rx) = mpsc::channel(100);

                //         tokio::spawn(poll_fn(move || {
                //             let stream_name = stream_name.clone(); // o_O wtf !!!?
                //             tokio_threadpool::blocking(|| {
                //                 let tree = db.open_tree(stream_name).unwrap();
                //                 for result in tree.iter() {
                //                     let (k, v) = result.unwrap();
                //                     tx.try_send((k, v.to_vec())).unwrap();
                //                 }
                //                 for event in tree.watch_prefix(vec![]) {
                //                     if let Event::Set(k, v) = event {
                //                         tx.try_send((k, v.to_vec())).unwrap();
                //                     }
                //                 }
                //             }).map_err(|e| error!("{}", e))
                //         }));

                //         (line, Some(rx))
                //     },
                //     (Some("PUBLISH"), Some(stream_name)) => {
                //         if let Some(content) = parts.next() {

                //             println!("publishing to {:?}", stream_name);

                //             let stream_name = stream_name.as_bytes().to_vec();
                //             let tree = db.open_tree(stream_name).unwrap();

                //             let unique_id = db.generate_id().unwrap() as u64;
                //             let mut unique_id_buff = Vec::new();
                //             let _ = unique_id_buff.write_u64::<BigEndian>(unique_id);

                //             let content = content.as_bytes().to_vec();
                //             tree.set(unique_id_buff, content).unwrap();
                //         }

                //         (line, None)
                //     }
                //     _ => {
                //         // TODO return a real error
                //         panic!("whoops: {:?}", line)
                //     },
                // }
            });

            let writes = responses.fold(writer, |writer, response: Result<RespValue, RespMsgError>| {
                // match rx {
                //     Some(rx) => {
                //         let keys_values = rx
                //             .map(|(k, v)| {
                //                 let key = k.as_slice().read_u64::<BigEndian>().unwrap();
                //                 let value = str::from_utf8(&v).unwrap();
                //                 format!("{} -- {}", key, value)
                //             })
                //             .map_err(|e| std::io::ErrorKind::Interrupted);
                //         writer.send_all(keys_values).map(|(s, _)| s).boxed()
                //     },
                //     None => writer.send(response).boxed(),
                // }

                writer.send(response.unwrap())
            });

            let msg = writes.then(|_| Ok(()));
            tokio::spawn(msg)
        });

    tokio::run(server)
}
