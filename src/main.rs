#![feature(async_closure)]
extern crate async_std;
extern crate futures;
#[macro_use]
extern crate log;

use std::io::Write;
use std::net::Shutdown;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::hash_map::{Entry, HashMap},
    sync::Arc,
};

use async_std::task;

use async_std::{
    io::BufReader,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
};
use env_logger::Builder;
use futures::channel::mpsc;
use std::sync::Weak;

use futures::select;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::Future;
use log::LevelFilter;

use streamunordered::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

// utils start
fn init_logging() -> Result<()> {
    let mut builder = Builder::from_default_env();
    builder.filter_level(LevelFilter::Debug);
    builder.format(|buf, record| {
        let file = match (record.file(), record.line()) {
            (Some(f), Some(l)) => f.to_owned() + ":" + &l.to_string(),
            _ => "".to_string(),
        };
        writeln!(
            buf,
            "{} {} {:?} {} - {}",
            buf.timestamp_micros(),
            file,
            std::thread::current().id(),
            record.level(),
            record.args()
        )
    });
    builder.format_timestamp_micros();
    builder.try_init().or_else(|e| {
        debug!("Logging not initialized: {}", e);
        Ok(())
    })
}

static GLOBAL_TASK_COUNT: AtomicUsize = AtomicUsize::new(0);

fn spawn_and_log_error<F>(id: String, fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    info!("Future {} started", id);
    GLOBAL_TASK_COUNT.fetch_add(1, Ordering::SeqCst);
    task::spawn(async move {
        GLOBAL_TASK_COUNT.fetch_sub(1, Ordering::SeqCst);
        if let Err(e) = fut.await {
            warn!("Future {} ended with error {}", id, e);
        } else {
            info!("Future {} ended", id);
        }
    })
}

// utils end

async fn accept_loop(addr: impl ToSocketAddrs, shutdown_receiver: Receiver<Void>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let (mut broker_sender, broker_receiver) = mpsc::unbounded();
    // Broker will listen on combined receiver and can pass internal sender to its subactors.
    // Closing broker_sender will notify broker to shut down.
    let broker_handle = task::spawn(broker_loop(broker_receiver));
    let mut incoming = listener.incoming().fuse();
    let mut shutdown_receiver = shutdown_receiver.fuse();
    // listen on both shutdown and new tcp client connection
    loop {
        select! {
            msg = incoming.next() => match msg {
                Some(stream) => {
                    let stream = stream?;
                    let stream = Arc::new(stream);
                    let id = stream.peer_addr()?;
                    broker_sender
                        .send(Event::NewClient {
                            id: id.to_string(),
                            stream,
                        })
                        .await?;
                },
                None => {
                    debug!("TCP listener closed");
                    break;
                },
            },
            void = shutdown_receiver.next() => match void {
                Some(void) => match void {}, // nothing can happen here
                None => {
                    debug!("Accept loop recievied shutdown request");
                    break;
                },
            }
        }
    }
    info!("Shutting down accept loop");
    drop(broker_sender);
    broker_handle.await?;
    Ok(())
}

// reader
async fn reader_loop(id: String, mut broker: Sender<Event>, stream: Arc<TcpStream>) -> Result<()> {
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();
    let name = match lines.next().await {
        None => Err("peer disconnected immediately")?,
        Some(line) => line?,
    };
    debug!("Reader {}: setting name to '{}'", id, name);
    broker
        .send(Event::NewPeer {
            id: id.clone(),
            name: name.clone(),
        })
        .await?;

    while let Some(line) = lines.next().await {
        let line = line?;
        if let Some(idx) = line.find(':') {
            let (dest, mut msg) = line.split_at(idx);
            msg = &msg[1..];
            let dest: Vec<String> = dest
                .split(',')
                .map(|name| name.trim().to_string())
                .collect();
            let msg: String = msg.to_string();
            broker
                .send(Event::Message {
                    id: id.clone(),
                    to_names: dest,
                    msg,
                })
                .await?; // awaiting on what? broker or receivers?
        } else if line == "/w" {
            broker.send(Event::ListAllUsers { id: id.clone() }).await?;
        } else if line == "/q" {
            debug!("Client {} requested quit", name);
            break;
        }
    }
    Ok(())
}

// broker

#[derive(Debug)]
enum Void {} // https://doc.rust-lang.org/nomicon/exotic-sizes.html#empty-types

#[derive(Debug)]
enum Event {
    NewClient {
        id: String,
        stream: Arc<TcpStream>,
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
        id: String,
    },
}

type WriterEntry = (String, Sender<String>);

async fn broker_loop(broker_receiver: Receiver<Event>) -> Result<()> {
    let mut ids_to_writer_entries: HashMap<String, WriterEntry> = Default::default();
    let mut names_to_ids: HashMap<String, String> = Default::default();
    let mut task_handles: Vec<task::JoinHandle<()>> = Default::default();
    let (internal_sender, internal_receiver) = mpsc::unbounded();
    let mut combined: StreamUnordered<Receiver<Event>> = Default::default();
    combined.push(broker_receiver);
    combined.push(internal_receiver);
    let mut open_tcp_connections: HashMap<String, Weak<TcpStream>> = Default::default();

    while let Some((v, _stream_token)) = combined.next().await {
        if let StreamYield::Item(event) = v {
            debug!("{:?}", event);
            match event {
                Event::NewClient { id, stream } => new_client(
                    id,
                    stream,
                    &internal_sender,
                    &mut ids_to_writer_entries,
                    &mut task_handles,
                    &mut open_tcp_connections,
                ),
                Event::NewPeer { id, name } => {
                    new_peer(id, name, &mut ids_to_writer_entries, &mut names_to_ids)
                }
                Event::Message { id, to_names, msg } => {
                    message(
                        id,
                        to_names,
                        msg,
                        &mut ids_to_writer_entries,
                        &mut names_to_ids,
                    )
                    .await
                }
                Event::ListAllUsers { id } => {
                    list_all_users(id, &mut ids_to_writer_entries, &mut names_to_ids).await
                }
                Event::DisconnectedPeer { id } => disconnected_peer(
                    id,
                    &mut ids_to_writer_entries,
                    &mut names_to_ids,
                    &mut open_tcp_connections,
                ),
            }?;
        } else if let StreamYield::Finished(_finished_stream) = v {
            debug!("Broker recievied shutdown request");
            break;
        }
    }
    // Shut down open tcp streams
    for (id, weak_tcp_stream) in open_tcp_connections {
        match weak_tcp_stream.upgrade() {
            Some(tcp_stream) => {
                debug!("Broker closing tcp stream {}", id);
                if let Err(e) = tcp_stream.shutdown(Shutdown::Both) {
                    debug!("Broker closing tcp stream {} failed: {:?}", id, e);
                }
            }
            _ => {}
        }
    }
    // Drop writer_senders so that writer_loop actors which have writer_receiver will shut down.
    drop(ids_to_writer_entries);

    info!("Waiting for task_handles to finish");
    for task_handle in task_handles {
        task_handle.await;
    }
    Ok(())
}

// Spawn both reader_loop and writer_loop for this tcp stream.
fn new_client(
    id: String,
    stream: Arc<TcpStream>,
    broker_sender: &Sender<Event>,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    task_handles: &mut Vec<task::JoinHandle<()>>,
    open_tcp_connections: &mut HashMap<String, Weak<TcpStream>>,
) -> Result<()> {
    info!("Accepting from: {}", id);
    open_tcp_connections.insert(id.clone(), Arc::downgrade(&stream));
    match ids_to_writer_entries.entry(id.clone()) {
        Entry::Occupied(..) => Ok(()), // TODO same id was already taken?
        Entry::Vacant(entry) => {
            // start reader that can send events to broker

            let future = {
                let mut broker_sender = broker_sender.clone();
                let id = id.clone();
                let stream = stream.clone();
                (async move || {
                    let _ = reader_loop(id.clone(), broker_sender.clone(), stream).await;
                    // no matter how reader finishes, send disconnected event
                    broker_sender.send(Event::DisconnectedPeer { id }).await?;
                    Ok(())
                })()
            };

            let reader_task_handle = spawn_and_log_error(format!("reader: '{}'", id), future);
            task_handles.push(reader_task_handle);

            // start writer that receives messages from broker
            let (writer_sender, writer_receiver) = mpsc::unbounded();
            entry.insert(("unknown".to_string(), writer_sender));

            let writer_task_handle = spawn_and_log_error(
                format!("writer: '{}'", id),
                writer_loop(writer_receiver, stream),
            );
            task_handles.push(writer_task_handle);
            Ok(())
        }
    }
}

fn new_peer(
    id: String,
    name: String,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    names_to_ids: &mut HashMap<String, String>,
) -> Result<()> {
    info!("Id: '{}' name: '{}'", id, name);
    names_to_ids.insert(name.clone(), id.clone());
    if let Some((old_name, writer)) = ids_to_writer_entries.remove(&id) {
        debug!(
            "Id: '{}' updating old name '{}' to '{}'",
            id, old_name, name
        );
        ids_to_writer_entries.insert(id, (name, writer));
    }
    Ok(())
}

async fn message(
    id: String,
    to_names: Vec<String>,
    msg: String,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    names_to_ids: &mut HashMap<String, String>,
) -> Result<()> {
    for receiver_name in to_names {
        let mut sent_to_channel: bool = false;
        if let Some(receiver_id) = names_to_ids.get(receiver_name.as_str()) {
            if let Some((sender_name, receiver_channel)) =
                ids_to_writer_entries.get_mut(receiver_id.as_str())
            {
                let msg = format!("Got message from '{}': {}", sender_name, msg);
                sent_to_channel = match receiver_channel.send(msg).await {
                    Ok(_) => true,
                    Err(_) => false,
                }
            } else {
                error!("Cannot find name of sender with id:'{}'", id);
            }
        }
        if !sent_to_channel {
            // send not found to sender of this message, send back warning
            if let Some((_, sender_channel)) = ids_to_writer_entries.get_mut(id.as_str()) {
                let msg = format!("not found: {}", receiver_name);
                sender_channel.send(msg).await?
            } else {
                warn!("Sender id:'{}' not found, dropping message", id);
            }
        }
    }
    Ok(())
}

async fn list_all_users(
    id: String,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    names_to_ids: &mut HashMap<String, String>,
) -> Result<()> {
    let all_users: String = names_to_ids
        .keys()
        .into_iter()
        .map(|x| x.as_str())
        .collect::<Vec<&str>>()
        .join(",");
    debug!("All users: {}", all_users);
    if let Some((_, writer_sender)) = ids_to_writer_entries.get_mut(id.as_str()) {
        writer_sender.send(all_users).await?
    } else {
        warn!("Cannot find writer for id:'{}', dropping message", id);
    }
    Ok(())
}

fn disconnected_peer(
    id: String,
    ids_to_writer_entries: &mut HashMap<String, WriterEntry>,
    names_to_ids: &mut HashMap<String, String>,
    open_tcp_connections: &mut HashMap<String, Weak<TcpStream>>,
) -> Result<()> {
    debug!("Removing id:'{}'", id);
    // free writer_sender so that writer dies.
    if let Some((name, _)) = ids_to_writer_entries.remove(&id) {
        names_to_ids.remove(&name);
    }
    open_tcp_connections.remove(&id);
    Ok(())
}

// writer
async fn writer_loop(mut messages: Receiver<String>, stream: Arc<TcpStream>) -> Result<()> {
    let mut stream = &*stream;
    while let Some(msg) = messages.next().await {
        stream.write_all(msg.as_bytes()).await?;
        stream.write_all(b"\n").await?;
    }
    Ok(())
}

#[async_std::main]
async fn main() -> Result<()> {
    init_logging().expect("Cannot init logging");
    let (_shutdown_sender, shutdown_receiver) = mpsc::unbounded();
    let fut = accept_loop("127.0.0.1:8080", shutdown_receiver);
    task::block_on(fut)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn client_server() -> Result<()> {
        init_logging().expect("Cannot init logging");
        let fut = async || -> Result<()> {
            let addr = "127.0.0.1:8081";
            let (shutdown_sender, shutdown_receiver) = mpsc::unbounded();
            let accept_loop = spawn_and_log_error(
                "accept_loop".to_string(),
                accept_loop(addr, shutdown_receiver),
            );
            // start client
            let stream = TcpStream::connect(addr).await?;
            let (reader, mut writer) = (&stream, &stream);
            writer.write_all(b"joe\n").await?;
            writer.write_all(b"/w\n").await?;
            let mut lines_from_server = BufReader::new(reader).lines();
            let line = match lines_from_server.next().await {
                Some(line) => line?,
                _ => panic!("no line"),
            };
            assert_eq!("joe", line);
            drop(shutdown_sender);
            debug!("Waiting for accept loop to finish");
            accept_loop.await;
            debug!("Finished");
            Ok(())
        }();
        task::block_on(fut)?;
        Ok(())
    }

    async fn receive_test_combined(mut combined: StreamUnordered<Receiver<String>>) -> bool {
        let mut msgs: usize = 0;
        let mut broken_streams = 0;
        while let Some((v, _stream_token)) = combined.next().await {
            if let StreamYield::Item(v) = v {
                debug!("got {}", v);
                msgs += 1;
            } else if let StreamYield::Finished(_finished_stream) = v {
                // TODO: drop stream
                //finished_stream.remove()
                debug!("Stream finished");
                broken_streams += 1;
                if broken_streams == 2 {
                    break;
                }
            }
        }
        msgs == 2
    }

    async fn streamunordered_async() -> Result<()> {
        let mut combined: StreamUnordered<Receiver<String>> = Default::default();
        let (mut msg_sender1, msg_receiver1): (Sender<String>, Receiver<String>) =
            mpsc::unbounded();
        let (mut msg_sender2, msg_receiver2): (Sender<String>, Receiver<String>) =
            mpsc::unbounded();
        combined.push(msg_receiver1);
        combined.push(msg_receiver2);

        let spawned = task::spawn(receive_test_combined(combined));

        msg_sender1.send("test1".to_string()).await?;
        drop(msg_sender1);
        task::sleep(Duration::from_millis(100)).await;
        msg_sender2.send("test2".to_string()).await?;
        drop(msg_sender2);

        assert!(spawned.await);
        Ok(())
    }

    #[test]
    fn streamunordered() {
        init_logging().expect("Cannot init logging");
        task::block_on(streamunordered_async()).expect("Cannot execute test");
        assert_eq!(GLOBAL_TASK_COUNT.load(Ordering::SeqCst), 0);
    }
}
