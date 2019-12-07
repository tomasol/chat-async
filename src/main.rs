#![type_length_limit = "1556548"]

extern crate async_std;
extern crate futures;
#[macro_use]
extern crate log;

use std::time::Duration;
use async_std::task::{sleep, spawn};
use async_std::future;
use async_std::future::TimeoutError;
use async_std::task;
use env_logger::Builder;
use futures::future::join_all;
use std::io::Write;
use log::LevelFilter;
use futures::future::TryFutureExt;
use futures::stream::TryStreamExt;
use futures::{Future, AsyncReadExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project_lite::pin_project;


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

pin_project! {
    struct TwoFutures<Fut1, Fut2> {
        first_done: bool,
        #[pin]
        first: Fut1,
        #[pin]
        second: Fut2,
    }
}

impl<Fut1: Future, Fut2: Future> Future for TwoFutures<Fut1, Fut2> {
    type Output = Fut2::Output;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        if !*this.first_done {
            // keep polling first until it is ready
            if let Poll::Ready(_) = this.first.poll(ctx) {
                *this.first_done = true;
            }
        }
        if *this.first_done {
            this.second.poll(ctx)
        } else {
            Poll::Pending
        }
    }
}

fn sleepus2() -> impl Future<Output=()> {
    TwoFutures {
        first_done: false,
        first: task::sleep(Duration::from_millis(3000)),
        second: async { println!("Hello TwoFutures"); },
    }
}

fn sleepus3() -> impl Future<Output=(std::result::Result<i32, i32>)> {
//async fn sleepus3() -> Result<i32, i32> {
    let future = async {
        info!("start");
        Ok::<i32, i32>(1)
    };
    let future = future.and_then(|x| async move { Ok::<i32, i32>(x + 3) });


    let future = future.and_then(|x| async move {
        info!("Got {}", x);
        task::sleep(Duration::from_secs(1)).await;
        Ok::<i32, i32>(-1)
    });
    future
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

#[async_std::main]
async fn main() -> Result<(), TimeoutError> {
    init_logging();

    let result: Result<i32, i32> = sleepus3().await;
    info!("Finished {:?}", result);

//    task::sleep(Duration::from_secs(5)).await;
    Ok(())
}
