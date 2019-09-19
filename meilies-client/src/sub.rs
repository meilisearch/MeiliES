use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Instant, Duration};

use async_std::io;
use async_std::net::TcpStream;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::future::Either;
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt, FusedStream};
use futures::stream;
use futures::task::{Poll, Context};

use futures_codec::Framed;
use futures_timer::Interval;

use log::{error, warn};

use meilies::reqresp::{ClientCodec, Request, Response};
use meilies::stream::{StreamName, Stream as EsStream};

fn into_io_error(error: impl fmt::Display) -> std::io::Error {
    io::Error::new(io::ErrorKind::Other, error.to_string())
}

struct SubsState {
    range: (Option<u64>, Option<u64>),
    resubscribed: bool,
}

async fn inner_connect(
    stream: TcpStream,
    creceiver: &mut mpsc::Receiver<Request>,
    ssender: &mut mpsc::Sender<io::Result<Result<Response, String>>>,
    subscriptions: &mut HashMap<StreamName, SubsState>,
) -> async_std::io::Result<()>
{
    let framed = Framed::new(stream, ClientCodec);
    let (mut ssink, sstream) = framed.split();

    // initiate subscriptions
    let mut streams = Vec::with_capacity(subscriptions.len());
    for (name, state) in subscriptions.iter_mut() {
        state.resubscribed = true;
        let (from, to) = state.range;
        let stream = EsStream::new_from_to(name.clone(), from, to);
        streams.push(stream);
    }
    ssink.send(Request::Subscribe { streams }).await.map_err(into_io_error)?;

    let duration = Duration::from_secs(20);
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
                // do not send a ping if a message has been sent recently
                if message == Request::StreamNames && last_message.elapsed() < duration {
                    continue
                }

                // save that new subscription in case that meilies-server stop responding
                // and did not sent us any event. This way we will be able to re-subscribe.
                if let Request::Subscribe { ref streams } = message {
                    for EsStream { name, range } in streams {
                        let range = (range.from(), range.to());
                        let state = SubsState { range, resubscribed: false };
                        subscriptions.insert(name.clone(), state);
                    }
                }

                ssink.send(message).await.map_err(into_io_error)?
            },
            // messages received from the server and
            // to forward to the client
            Either::Right(message) => {
                let message = message.map_err(into_io_error)?;

                if let Ok(Response::Subscribed { ref stream }) = message {
                    // do not show re-subscriptions to the user
                    if let Some(SubsState { resubscribed: true, .. }) = subscriptions.get(stream) {
                        continue;
                    }
                }

                // If we receive a new event we should store its event number, this way,
                // in case of re-subscription, we must subscribe from the next event number
                if let Ok(Response::Event { ref stream, ref number, .. }) = message {
                    match subscriptions.remove_entry(&stream) {
                        Some((stream, state)) => {
                            let (_, to) = state.range;
                            let range = (Some(number.0 + 1), to);
                            let state = SubsState { range, ..state };
                            subscriptions.insert(stream, state);
                        },
                        None => warn!("received event from not subscribed stream {}", stream),
                    }
                }

                if let Err(error) = ssender.send(Ok(message)).await {
                    // user disconnect the SubStream
                    if error.is_disconnected() { break }
                    if error.is_full() { warn!("{}", error); }
                }
            },
        }

        last_message = Instant::now();
    }

    Ok(())
}

pub async fn sub_connect(
    pool: &ThreadPool,
    addr: SocketAddr,
) -> io::Result<(SubController, SubStream)>
{
    // 'c' stands for client and 's' stands for server
    let (csender, creceiver) = mpsc::channel(100); // SubController -> this reactor
    let (ssender, sreceiver) = mpsc::channel(0); // this reactor -> SubStream

    pool.spawn_ok(async move {
        let mut creceiver = creceiver;
        let mut ssender = ssender;
        let mut subs = HashMap::new();
        let mut backoff = crate::backoff::new();

        loop {
            let result = match TcpStream::connect(addr).await {
                Ok(stream) => {
                    backoff = crate::backoff::new();
                    inner_connect(stream, &mut creceiver, &mut ssender, &mut subs).await
                },
                Err(e) => Err(e),
            };

            if let Err(e) = result {
                if let Err(e) = ssender.send(Err(e)).await {
                    // user disconnect the SubStream
                    if e.is_disconnected() { return }
                    if e.is_full() { warn!("{}", e) }
                }
            }

            match backoff.next() {
                Some(mul) => {
                    let dur = Duration::from_millis(100) * mul;
                    let _ = futures_timer::Delay::new(dur).await;
                    warn!("Retrying connection with {}", addr);
                },
                None => break,
            }
        }

        error!("Could not connect to {}", addr);
    });

    let controller = SubController(csender);
    let stream = SubStream(sreceiver);
    Ok((controller, stream))
}

#[derive(Clone)]
pub struct SubController(mpsc::Sender<Request>);

impl SubController {
    pub async fn subscribe_to(&mut self, stream: EsStream) -> Result<(), ()> {
        let request = Request::Subscribe { streams: vec![stream] };
        self.0.send(request).await.map_err(drop)
    }
}

pub struct SubStream(mpsc::Receiver<io::Result<Result<Response, String>>>);

impl Stream for SubStream {
    type Item = io::Result<Result<Response, String>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|x| &mut x.0).poll_next(cx) }
    }
}

impl FusedStream for SubStream {
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}
