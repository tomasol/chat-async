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
    let (mut broker_sender, broker_receiver) = mpsc::unbounded();
    let broker_handle = task::spawn(broker_loop(broker_receiver));
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await { // TODO: how to shutdown this?
        let stream = stream?;
        let stream = Arc::new(stream);
        let id = stream.peer_addr()?;
        broker_sender.send(Event::NewClient {
            id: id.to_string(),
            stream,
            broker_sender: broker_sender.clone(),
        }).await?;
    }
    info!("Dropping broker");
    drop(broker_sender);
    broker_handle.await?;
    Ok(())
}

// reader
async fn connection_loop(
    id: String,
    mut broker: Sender<Event>,
    stream: Arc<TcpStream>,
    _shutdown_sender: Sender<Void>,
) -> Result<()> {
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();
    let name = match lines.next().await {
        None => Err("peer disconnected immediately")?,
        Some(line) => line?,
    };
    debug!("name = {}", name);

    broker.send(Event::NewPeer {
        id: id.clone(),
        name: name.clone(),
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
                id: id.clone(),
                to_names: dest,
                msg,
            }).await.unwrap(); // awaiting on what? broker or receivers?
        } else if line == "/w" {
            broker.send(Event::ListAllUsers {
                id: id.clone(),
            }).await.unwrap();
        } else if line == "/q" {
            debug!("Client {} requested quit", name);
            break;
        }
    }
    debug!("Reader {} is exitting", id);
    // TODO: send event also on error
    broker.send(Event::DisconnectedPeer { id }).await?;
    // dropping shutdown_sender should kill writer actor
    Ok(())
}

// broker

#[derive(Debug)]
enum Void {}

#[derive(Debug)]
enum Event {
    NewClient {
        id: String,
        stream: Arc<TcpStream>,
        broker_sender: Sender<Event>,
    },
    NewPeer {
        id: String,
        name: String,
    },
    Message {
        id: String,
        to_names: Vec<String>,
        msg: String,
    },
    ListAllUsers {
        id: String,
    },
    DisconnectedPeer {
        id: String
    },
}

type WriterEntry = (String, Sender<String>);

async fn broker_loop(events: Receiver<Event>) -> Result<()> {
    let mut ids_to_writer_entries: HashMap<String, WriterEntry> = HashMap::new();
    let mut names_to_ids: HashMap<String, String> = HashMap::new();
    let mut writers = Vec::new();

    let mut events = events.fuse();
    loop {
        select! {
            event =  events.next() => match event {
                Some(event) => {
                    match_event(event, &mut ids_to_writer_entries,
                     &mut names_to_ids, &mut writers).await?
                    }
                None => break,
            }
        }
    }

//    while let Some(event) = events.next().await {
//        matchEvent(event, &mut peers, &mut writers).await;
//    }
    info!("Dropping peers");
    drop(ids_to_writer_entries);
    // all task spawned by this should be waited for
    for writer in writers {
        writer.await;
    }
    Ok(())
}

async fn match_event(
    event: Event,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    names_to_ids: &mut HashMap<String, String>,
    writers: &mut Vec<task::JoinHandle<()>>,
) -> Result<()> {
    debug!("{:?}", event);
    match event {
        Event::NewClient { id, stream, broker_sender } => {
            info!("Accepting from: {}", id);
            match ids_to_writer_entries.entry(id.clone()) {
                Entry::Occupied(..) => (), // TODO
                Entry::Vacant(entry) => {
                    // start reader that can send events to broker
                    let (shutdown_sender, shutdown_receiver) = mpsc::unbounded();
                    spawn_and_log_error(
                        format!("reader:{}", id),
                        connection_loop(id.clone(), broker_sender, stream.clone(), shutdown_sender));
                    // start writer that receives messages from broker
                    let (writer_sender, writer_receiver) = mpsc::unbounded();
                    entry.insert(("unknown".to_string(), writer_sender));
                    error!("inserted peers[{}]", id);

                    let writer_actor =
                        connection_writer_loop(id.clone(), writer_receiver, stream, shutdown_receiver);
                    let handle = spawn_and_log_error(id, writer_actor);
                    writers.push(handle);
                }
            }
        }
        Event::NewPeer { id, name } => {
            info!("Id: '{}' name: '{}'", id, name);
            names_to_ids.insert(name.clone(), id.clone());
            if let Some((old_name, writer)) = ids_to_writer_entries.remove(&id) {
                debug!("Id: '{}' updating old name '{}' to '{}'", id, old_name, name);
                ids_to_writer_entries.insert(id, (name, writer));
            }
        }
        Event::Message { id, to_names: toNames, msg } => {
            for receiverName in toNames {
                let mut sent_to_channel: bool = false;
                if let Some(receiver_id) = names_to_ids.get(receiverName.as_str()) {
                    if let Some((sender_name, receiver_channel)) =
                    ids_to_writer_entries.get_mut(receiver_id.as_str()) {
                        let msg = format!("Got message from '{}': {}", sender_name, msg);
                        // TODO spawn instead?
                        sent_to_channel = match receiver_channel.send(msg).await {
                            Ok(_) => { true }
                            Err(_) => false,
                        }
                    } else {
                        error!("Cannot find name of sender with id:'{}'", id);
                    }
                }
                if !sent_to_channel {
                    // send not found to sender of this message, send back warning
                    if let Some((_, sender_channel)) = ids_to_writer_entries.get_mut(id.as_str()) {
                        let msg = format!("not found: {}", receiverName);
                        sender_channel.send(msg).await?
                    } else {
                        warn!("Sender id:'{}' not found, dropping message", id);
                    }
                }
            }
        }
        Event::ListAllUsers { id } => {
            let all_users: String = names_to_ids.keys().into_iter()
                .map(|x| x.as_str()).collect::<Vec<&str>>().join(",");
            debug!("All users: {}", all_users);
            if let Some((_, writer_sender)) = ids_to_writer_entries.get_mut(id.as_str()) {
                writer_sender.send(all_users).await?
            } else {
                warn!("Cannot find writer for id:'{}', dropping message", id);
            }
        }
        Event::DisconnectedPeer { id } => {
            debug!("Removing id:'{}'", id);
            if let Some((name, _)) = ids_to_writer_entries.remove(&id) {
                names_to_ids.remove(&name);
            }
        }
    }
    Ok(())
}

// writer
async fn connection_writer_loop(
    name: String,
    messages: Receiver<String>,
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
    task::block_on(fut)
}
