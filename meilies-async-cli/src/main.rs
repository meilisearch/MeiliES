use std::collections::HashMap;
use std::{fmt, mem};
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::time::{Instant, Duration};

use async_std::io::{self, BufReader, BufRead};
use async_std::net::{TcpStream, ToSocketAddrs};

use meilies::reqresp::{ClientCodec, Request, Response};
use meilies::stream::{StreamName, Stream as EsStream};

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::future::Either;
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt, FusedStream};
use futures::stream;
use futures::task::{Poll, Context};
use futures_codec::Framed;
use futures_timer::Interval;

#[derive(Clone)]
struct StreamController(mpsc::Sender<Request>);

struct StreamConnection(mpsc::Receiver<io::Result<Result<Response, String>>>);

fn into_io_error(error: impl fmt::Display) -> std::io::Error {
    io::Error::new(io::ErrorKind::Other, error.to_string())
}

async fn initiate_connection(
    stream: TcpStream,
    creceiver: &mut mpsc::Receiver<Request>,
    ssender: &mut mpsc::Sender<io::Result<Result<Response, String>>>,
    subscriptions: &mut HashMap<StreamName, (Option<u64>, Option<u64>)>,
) -> async_std::io::Result<()>
{
    let framed = Framed::new(stream, ClientCodec);
    let (mut ssink, sstream) = framed.split();

    // initiate subscriptions
    let mut streams = Vec::with_capacity(subscriptions.len());
    for (name, (from, to)) in subscriptions.iter() {
        let stream = EsStream::new_from_to(name.clone(), *from, *to);
        streams.push(stream);
    }
    ssink.send(Request::Subscribe { streams }).await.map_err(into_io_error)?;

    let duration = Duration::from_secs(3);
    let pings = Interval::new(duration).map(|_| Request::StreamNames);
    let mut last_message = Instant::now();

    let tosend = stream::select(pings, creceiver).map(Either::Left);
    let received = sstream.map(Either::Right);
    let mut events = stream::select(tosend, received);

    while let Some(either) = events.next().await {
        match either {
            // messages to send to the server, comming either
            // from the client or after a timeout (ping)
            Either::Left(message) => {
                // do not send a ping if a message was sent recently
                if message == Request::StreamNames && last_message.elapsed() < duration {
                    continue
                }

                // save that new subscription in case that meilies-server stop responding
                // and did not sent us any event. This way we will be able to re-subscribe.
                if let Request::Subscribe { ref streams } = message {
                    for EsStream { name, range } in streams {
                        let range = (range.from(), range.to());
                        subscriptions.insert(name.clone(), range);
                    }
                }

                ssink.send(message).await.map_err(into_io_error)?
            },
            // messages received from the server and
            // to forward to the client
            Either::Right(message) => {
                let message = message.map_err(into_io_error)?;

                // If we receive a new event we should store it event number this way,
                // in case of re-subscription, we must subscribe from the next event number
                if let Ok(Response::Event { ref stream, ref number, .. }) = message {
                    let (_, to) = subscriptions.remove(&stream).unwrap_or_default();
                    let range = (Some(number.0 + 1), to);
                    subscriptions.insert(stream.clone(), range);
                }

                println!("subscriptions: {:?}", subscriptions);

                if let Err(error) = ssender.send(Ok(message)).await {
                    if error.is_disconnected() { break }
                    // TODO must handle is_full
                }
            },
        }

        last_message = Instant::now();
    }

    Ok(())
}

pub struct Fibonacci {
    curr: u32,
    next: u32,
}

impl Fibonacci {
    pub fn new() -> Fibonacci {
        Fibonacci { curr: 1, next: 1 }
    }
}

impl Iterator for Fibonacci {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        let new_next = self.curr + self.next;
        self.curr = self.next;
        self.next = new_next;
        Some(self.curr)
    }
}

fn new_backoff() -> impl Iterator<Item=u32> {
    use std::iter::{once, repeat};
    // fib(21) = 10946
    once(0).chain(Fibonacci::new().take(21)).chain(repeat(21))
}

async fn new_stream_connection(
    pool: &ThreadPool,
    addr: SocketAddr,
) -> io::Result<(StreamController, StreamConnection)>
{
    // 'c' stands for client and 's' stands for server
    let (csender, creceiver) = mpsc::channel(100);
    let (ssender, sreceiver) = mpsc::channel(100);

    pool.spawn_ok(async move {
        let mut ssender = ssender;
        let mut creceiver = creceiver;
        let mut backoff = new_backoff();
        let mut subs = HashMap::new();

        while let Some(mul) = backoff.next() {
            println!("Retrying connection with {}", addr);
            let dur = Duration::from_millis(100) * mul;
            if dur != Duration::from_secs(0) {
                let _ = futures_timer::Delay::new(dur).await;
            }

            let stream = match TcpStream::connect(addr).await {
                Ok(stream) => stream,
                Err(e) => {
                    if let Err(e) = ssender.send(Err(e)).await {
                        if e.is_disconnected() { break }
                        if e.is_full() { eprintln!("{}", e) }
                    }
                    continue
                }
            };

            println!("Connected to {}", addr);
            let _ = mem::replace(&mut backoff, new_backoff());

            if let Err(e) = initiate_connection(stream, &mut creceiver, &mut ssender, &mut subs).await {
                if let Err(e) = ssender.send(Err(e)).await {
                    if e.is_disconnected() { break }
                    if e.is_full() { eprintln!("{}", e) }
                }
            }
        }

        panic!("Impossible to reconnect to {}", addr);
    });

    let controller = StreamController(csender);
    let stream = StreamConnection(sreceiver);

    Ok((controller, stream))
}

impl StreamController {
    async fn send(&mut self, request: Request) -> Result<(), mpsc::SendError> {
        self.0.send(request).await
    }
}

impl Stream for StreamConnection {
    type Item = io::Result<Result<Response, String>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|x| &mut x.0).poll_next(cx) }
    }
}

impl FusedStream for StreamConnection {
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

fn main() -> async_std::io::Result<()> {
    let mut pool = ThreadPool::new().unwrap();
    let cloned_pool = pool.clone();
    let addr = "127.0.0.1:6480".parse().unwrap();

    pool.run(async {
        let pool = cloned_pool;
        let (mut ctrl, mut stream) = new_stream_connection(&pool, addr).await?;

        let name = EsStream::from_str("hello:0").unwrap();
        ctrl.send(Request::Subscribe { streams: vec![name] }).await.unwrap();

        while let Some(response) = stream.next().await {
            println!("received: {:?}", response);
        }

        Ok(())
    })
}
