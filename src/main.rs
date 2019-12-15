#![type_length_limit = "1556548"]

extern crate async_std;
extern crate futures;
#[macro_use]
extern crate log;

use std::{
    collections::hash_map::{Entry, HashMap},
    sync::Arc,
};
use std::io::Write;
use std::time::Duration;

use async_std::{
    io::BufReader,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
};
use async_std::future;
use async_std::future::TimeoutError;
use async_std::task::{sleep, spawn};
use async_std::task;
use env_logger::Builder;
use futures::{AsyncReadExt, Future};
use futures::{
    future::{Fuse, FusedFuture, FutureExt},
    pin_mut,
    select,
    stream::{FusedStream, FuturesUnordered, Stream, StreamExt},
};
use futures::channel::mpsc;
use futures::future::join_all;
use futures::future::TryFutureExt;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use log::LevelFilter;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

// utils start
fn init_logging() {
    let mut builder = Builder::from_default_env();
    builder.filter_level(LevelFilter::Debug);
    builder.format(|buf, record| {
        let file = match (record.file(), record.line()) {
            (Some(f), Some(l)) => f.to_owned() + ":" + &l.to_string(),
            _ => "".to_string()
        };
        writeln!(buf, "{} {} {:?} {} - {}",
                 buf.timestamp_millis(),
                 file,
                 std::thread::current().id(), record.level(), record.args())
    });
    builder.format_timestamp_micros();
    builder.init();
}

fn spawn_and_log_error<F>(id: String, fut: F) -> task::JoinHandle<()>
    where F: Future<Output=Result<()>> + Send + 'static, {
    task::spawn(async move {
        if let Err(e) = fut.await {
            warn!("Future {} ended with error {}", id, e)
        }
    })
}

// utils end

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let (broker_sender, broker_receiver) = mpsc::unbounded();
    let broker_handle = task::spawn(broker_loop(broker_receiver));
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await { // TODO: how to shutdown this?
        let stream = stream?;
        let id = stream.peer_addr()?;
        info!("Accepting from: {}", id);
        spawn_and_log_error(format!("reader:{}", id), connection_loop(broker_sender.clone(), stream));
    }
    info!("Dropping broker");
    drop(broker_sender);
    broker_handle.await?;
    Ok(())
}

// reader
async fn connection_loop(
    mut broker: Sender<Event>,
    stream: TcpStream,
) -> Result<()> {
    let stream = Arc::new(stream);
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    let name = match lines.next().await {
        None => Err("peer disconnected immediately")?,
        Some(line) => line?,
    };
    debug!("name = {}", name);
    // dropping this should kill writer actor
    let (_shutdown_sender, shutdown_receiver) = mpsc::unbounded::<Void>();
    broker.send(Event::NewPeer {
        name: name.clone(),
        stream: Arc::clone(&stream),
        shutdown: shutdown_receiver,
    }).await?;

    while let Some(line) = lines.next().await {
        let line = line?;
        if let Some(idx) = line.find(':') {
            let (dest, msg) = (&line[..idx], line[idx + 1..].trim());
            let (dest, mut msg) = line.split_at(idx);
            msg = &msg[1..];
            let dest: Vec<String> = dest.split(',').map(|name| name.trim().to_string()).collect();
            let msg: String = msg.to_string();
            broker.send(Event::Message {
                from: name.clone(),
                to: dest,
                msg,
            }).await.unwrap(); // awaiting on what? broker or receivers?
        } else if line == "/w" {
            broker.send(Event::ListAllUsers {
                from: name.clone(),
            }).await.unwrap();
        } else if line == "/q" {
            debug!("Client {} requested quit", name);
            break;
        }
    }
    debug!("Reader {} is exitting", name);
    Ok(())
}

// broker

#[derive(Debug)]
enum Void {} // 1

#[derive(Debug)]
enum Event {
    NewPeer {
        name: String,
        stream: Arc<TcpStream>,
        shutdown: Receiver<Void>,
    },
    Message {
        from: String,
        to: Vec<String>,
        msg: String,
    },
    ListAllUsers {
        from: String,
    },
}

async fn broker_loop(mut events: Receiver<Event>) -> Result<()> {
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();
    let mut writers = Vec::new();
//    let mut shutdowns = FuturesUnordered::<Option<Void>>::new();

    let mut events = events.fuse();
    loop {
        select! {
            event =  events.next() => match event {
                Some(event) => {
                    matchEvent(event, &mut peers, &mut writers).await?
                    }
                None => break,
            }
        }
    }

//    while let Some(event) = events.next().await { // TODO: listen on all shutdown channels, purge
//        matchEvent(event, &mut peers, &mut writers).await;
//    }
    info!("Dropping peers");
    drop(peers);
    // all task spawned by this should be waited for
    for writer in writers {
        writer.await;
    }
    Ok(())
}

async fn matchEvent(
    event: Event,
    peers: & mut HashMap<String, Sender<String>>,
    writers: & mut Vec<task::JoinHandle<()>>,
) -> Result<()> {
    match event {
        Event::NewPeer { name, stream, shutdown } => {
            let id = format!("writer:{}", name);
            match peers.entry(name) {
                Entry::Occupied(..) => (), // TODO: update to new stream
                Entry::Vacant(entry) => {
                    let (client_sender, client_receiver) = mpsc::unbounded();
                    entry.insert(client_sender);
                    let writer_actor =
                        connection_writer_loop(id.clone(), client_receiver, stream, shutdown);
                    let handle = spawn_and_log_error(id, writer_actor);
                    writers.push(handle);
                }
            }
        }
        Event::Message { from, to, msg } => {
            for receiver in to {
                let mut sentToChannel: bool = false;
                if let Some(receiverChannel) = peers.get_mut(&receiver) {
                    let msg = format!("from {}: {}", from, msg);
                    // TODO spawn instead?
                    sentToChannel = match receiverChannel.send(msg).await {
                        Ok(_) => { true }
                        Err(_) => false,
                    }
                }
                if !sentToChannel {
                    // send not found to sender of this message
                    if let Some(senderChannel) = peers.get_mut(&from) {
                        let msg = format!("not found: {}", receiver);
                        senderChannel.send(msg).await?
                    } else {
                        warn!("Sender '{}' not found, dropping message", from);
                    }
                }
            }
        }
        Event::ListAllUsers { from } => {
            let allUsers: String = peers.keys().into_iter()
                .map(|x| x.as_str()).collect::<Vec<&str>>().join(",");
            if let Some(senderChannel) = peers.get_mut(&from) {
                senderChannel.send(allUsers).await?
            } else {
                warn!("Sender '{}' not found, dropping message", from);
            }
        }
    }
    Ok(())
}


// writer
async fn connection_writer_loop(
    name: String,
    mut messages: Receiver<String>,
    stream: Arc<TcpStream>,
    shutdown: Receiver<Void>,
) -> Result<()> {
    let mut stream = &*stream;
    let mut messages = messages.fuse();// must be fused before loop
    let mut shutdown = shutdown.fuse();
    loop {
        select! {
            msg = messages.next().fuse() => match msg {
                Some(msg) => {
                stream.write_all(msg.as_bytes()).await?;
                stream.write_all(b"\n").await?;
                },
                None => break, // TODO: how is reader notified?
            },
            void = shutdown.next().fuse() => match void {
                Some(void) => match void {}, // should never happen
                None => break, // shutdown channel closed
            }
        }
    }
    info!("{} exitting", name);
//    while let Some(msg) = messages.next().await {
//        stream.write_all(msg.as_bytes()).await?;
//    }
    Ok(())
}

#[async_std::main]
async fn main() -> Result<()> {
    init_logging();
    let fut = accept_loop("127.0.0.1:8080");
    task::block_on(fut);
//    let result: Result<i32, i32> = sleepus3().await;
    info!("Finished");

//    task::sleep(Duration::from_secs(5)).await;
    Ok(())
}
