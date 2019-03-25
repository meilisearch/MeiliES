<p align="center">
  <img alt="meilies" src="https://user-images.githubusercontent.com/3610253/54926921-925b3500-4f11-11e9-8cbc-0f287f48108a.png" width="230">
</p>

<h3 align="center">MeiliES</h3>

<p align="center">
  A Rust based event store using the Redis protocol
</p>

<p align="center">
  <a href="https://dev.azure.com/thomas0884/thomas/_build/latest?definitionId=10&branchName=master">
    <img alt="Build Status" src="https://dev.azure.com/thomas0884/thomas/_apis/build/status/meilisearch.MeiliES?branchName=master">
  </a>

  <a href="https://deps.rs/repo/github/meilisearch/MeiliES">
    <img alt="Dependency Status" src="https://deps.rs/repo/github/meilisearch/MeiliES/status.svg">
  </a>
</p>

<p align="center">
  <a href="https://crates.io/crates/meilies-client">
    <img alt="meilies-client crate" src="https://img.shields.io/crates/v/meilies-client.svg">
  </a>

  <a href="https://docs.rs/meilies-client">
    <img alt="meilies-client documentation" src="https://docs.rs/meilies-client/badge.svg">
  </a>
</p>

## Introduction

MeiliES is an _Event Sourcing database_ that uses the _RESP_ (REdis Serialization Protocol) to communicate.
This way it is possible to create clients by reusing the already available protocol. For example, it is possible to use [the official Redis command line interface](https://redis.io/topics/rediscli) program to communicate with MeiliES.

An event store is like a Kafka or a Rabbit MQ but it stores events on disk indefinitely. The first purpose of the server is to publish events of a stream to all subscribed clients, note that events are saved in reception order. A client can also specify from which event number (incrementing) it wants to read, therefore it is possible to recover from crashing by reading and reconstructing a state with only new events.

[![MeiliES demo](https://asciinema.org/a/2o58381Fy9C4nk9yNg3EHjwH1.svg)](https://asciinema.org/a/2o58381Fy9C4nk9yNg3EHjwH1?speed=2)

## Features

- Event publication
- TCP stream subscriptions from an optional event number
- Resilient connections (reconnecting when closed)
- Redis based protocol
- Full Rust, using [sled as the internal storage](http://sled.rs)
- Takes near 2min to compile

## Building MeiliES

To run MeiliES you will need Rust, you can install it by following the steps on https://rustup.rs.
Once you have Rust in your `PATH` you can clone and build the MeiliES binaries.

```bash
git clone https://github.com/meilisearch/MeiliES.git
cd MeiliES
cargo install --path meilies-server
cargo install --path meilies-cli
```

## Basic Event Store Usage

Once MeiliES is installed and available in your `PATH`, you can run it by executing the following command.

```bash
meilies-server --db-path my-little-db.edb
```

There is now a MeiliES server running on your machine and listening on `127.0.0.1:6480`.
In another terminal window, you can specify to a client to listen to only new events.

```bash
meilies-cli subscribe 'my-little-stream'
```

And in another one again you can send new events.
All clients which are subscribed to that same stream will see the events.

```bash
meilies-cli publish 'my-little-stream' 'my-event-name' 'Hello World!'
meilies-cli publish 'my-little-stream' 'my-event-name' 'Hello Cathy!'
meilies-cli publish 'my-little-stream' 'my-event-name' 'Hello Kevin!'
meilies-cli publish 'my-little-stream' 'my-event-name' 'Hello Donut!'
```

But that is not a really interesting usage of Event Sourcing, right?!
Let's do a more interesting usage of it.

## Real Event Store Usage

MeiliES stores all the events of all the streams that were sent by all the clients in the order they were received.

So let's check that and specify to one client the point in time where we want to start reading events.
Once there are no more events in the stream, the server starts sending events at the moment it receives them.

We can do that by prepending the start event number separated by a colon.

```bash
meilies-cli subscribe 'my-little-stream:0'
```

In this example, we will start reading from the first event of the stream.
But we can also specify to start reading from the third event too.

```bash
meilies-cli subscribe 'my-little-stream:2'
```

Or from events which do not exists yet!
Try sending events to this stream and you will see: only events from the fifth one will appear.

```bash
meilies-cli subscribe 'my-little-stream:5'
```

## Current Limitations

The current implementation has some limitations related to the whole number of streams subscribed. The problem is that one thread is spawned for each stream and for each client. For example, if two clients subscribe to the same stream, the server will spawn two threads, one for each client instead of spawning only one thread and sending new events to a clients pool.

Even uglier, if a client is closing the connection, the spawned threads will not stop immediately but after some stream activity.

## Support

For commercial support, drop us an email at bonjour@meilisearch.com.
