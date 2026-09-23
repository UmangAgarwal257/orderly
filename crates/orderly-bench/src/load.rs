use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use orderly_core::{Engine, NewOrder, OrderType, Side};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Parser, Debug)]
#[command(name = "orderly-load")]
struct Args {
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = 5)]
    duration_secs: u64,
    #[arg(long, default_value_t = 10_000)]
    seed_levels: u64,
}

enum Cmd {
    Submit(NewOrder, oneshot::Sender<()>),
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let (tx, mut rx) = mpsc::channel::<Cmd>(4096);
    let engine = Arc::new(Mutex::new(Engine::new()));

    tokio::spawn({
        let engine = engine.clone();
        async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::Submit(order, ack) => {
                        let mut eng = engine.lock().await;
                        let _ = eng.submit(order);
                        let _ = ack.send(());
                    }
                }
            }
        }
    });

    {
        let mut eng = engine.lock().await;
        for i in 0..args.seed_levels {
            let price = 100 + (i % 50);
            let _ = eng.submit(NewOrder {
                side: Side::Ask,
                order_type: OrderType::Limit { price },
                qty: 10,
                client_order_id: None,
            });
            let _ = eng.submit(NewOrder {
                side: Side::Bid,
                order_type: OrderType::Limit { price },
                qty: 10,
                client_order_id: None,
            });
        }
    }

    let hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram"),
    ));
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let mut tasks = Vec::new();

    for t in 0..args.concurrency {
        let tx = tx.clone();
        let hist = hist.clone();
        tasks.push(tokio::spawn(async move {
            let mut count = 0u64;
            while Instant::now() < deadline {
                let side = if t % 2 == 0 { Side::Bid } else { Side::Ask };
                let price = 100 + (count % 50) as u64;
                let order = NewOrder {
                    side,
                    order_type: OrderType::Limit { price },
                    qty: 1,
                    client_order_id: None,
                };
                let (ack_tx, ack_rx) = oneshot::channel();
                let start = Instant::now();
                if tx.send(Cmd::Submit(order, ack_tx)).await.is_err() {
                    break;
                }
                if ack_rx.await.is_ok() {
                    let us = start.elapsed().as_micros() as u64;
                    hist.lock().await.record(us).ok();
                    count += 1;
                }
            }
            count
        }));
    }

    let mut total = 0u64;
    for t in tasks {
        total += t.await.unwrap_or(0);
    }

    let hist = hist.lock().await;
    let elapsed = args.duration_secs as f64;
    println!("concurrency: {}", args.concurrency);
    println!("duration_secs: {}", args.duration_secs);
    println!("orders_completed: {}", total);
    println!("orders_per_sec: {:.2}", total as f64 / elapsed);
    println!("latency_us p50: {}", hist.value_at_quantile(0.50));
    println!("latency_us p95: {}", hist.value_at_quantile(0.95));
    println!("latency_us p99: {}", hist.value_at_quantile(0.99));
}
