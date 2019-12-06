extern crate async_std;
extern crate futures;
#[macro_use]
extern crate log;

use std::time::Duration;
use async_std::future;
use async_std::future::TimeoutError;
use async_std::task;
use env_logger::Builder;
use futures::future::join_all;
use std::io::Write;
use log::LevelFilter;
use std::thread;
use futures::future::TryFutureExt;
use futures::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
                 thread::current().id(), record.level(), record.args())
    });
    builder.format_timestamp_micros();
    builder.init();
}

struct SleepPrint<Fut> {
    sleep: Fut,
}

impl<Fut: Future<Output=()>> Future for SleepPrint<Fut> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let sleep: Pin<&mut Fut> = unsafe { self.map_unchecked_mut(|s| &mut s.sleep) };
        sleep.poll(cx)
    }
}

struct DoNothing;

impl Future for DoNothing {
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

fn sleepus() -> impl std::future::Future<Output=()> {
    async {
        for i in 1..=15 {
            info!("Sleepus {}", i);
            SleepPrint {
                sleep: task::sleep(Duration::from_millis(500)),
            }.await;
            info!("Sleepus {} finished", i);
        }
    }
}

async fn interruptus() {
    for i in 1..=5 {
        info!("Interruptus {}", i);
        task::sleep(Duration::from_millis(1000)).await;
        info!("Interruptus {} finished", i);
    }
}

async fn vector_timeout_cancel() {
    let mut futures = vec!();
    for i in 0..6 {
        let future = async move {
            info!("{} Go to sleep", i);
            task::sleep(Duration::from_secs(i as u64)).await;
            info!("{} Woken up", i);
            i
        };

//        let _future2 = future.and_then( | result | async move {result} );
//        futures.push(future::timeout(Duration::from_secs(3), future));
        futures.push(future);
    }
// TODO:cancel
//    let joined: Vec<Result<i32, TimeoutError>> = join_all(futures).await;
//    let filtered = joined.iter()
//        .filter(|item| item.is_ok())
//        .map(|item| item.unwrap());
//    let sum: i32 = filtered.sum();

    let all = join_all(futures);
    let joined_result: Result<Vec<i32>, TimeoutError> =
        future::timeout(Duration::from_secs(3), all).await;

    let joined = joined_result.unwrap();
    let filtered = joined.iter();
    let sum: i32 = filtered.sum();
    info!("sum is {}", sum);
}

async fn wait_for_interruptus() {
    init_logging();
    let sleepus = task::spawn(sleepus());
    interruptus().await;
    info!("wait_for_interruptus finishes");
}

#[async_std::main]
async fn main() -> Result<(), TimeoutError> {
    wait_for_interruptus().await;
    task::sleep(Duration::from_millis(1000)).await;
    Ok(())
}
