use std::collections::VecDeque;
use std::sync::Arc;

use orderly_core::{BookDelta, Engine, EngineError, NewOrder, OrderBookSnapshot, OrderId, SubmitResult, Trade};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

const TRADE_HISTORY: usize = 10_000;

pub enum EngineCommand {
    Submit {
        order: NewOrder,
        respond_to: oneshot::Sender<Result<SubmitResult, EngineError>>,
    },
    Cancel {
        order_id: OrderId,
        respond_to: oneshot::Sender<Result<(), EngineError>>,
    },
    Snapshot {
        depth: usize,
        respond_to: oneshot::Sender<OrderBookSnapshot>,
    },
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    Trade(Trade),
    BookUpdate { snapshot: OrderBookSnapshot },
}

#[derive(Clone)]
pub struct EngineHandle {
    pub cmd_tx: mpsc::Sender<EngineCommand>,
    pub events: broadcast::Sender<MarketEvent>,
    pub trades: Arc<Mutex<VecDeque<Trade>>>,
}

impl EngineHandle {
    pub async fn submit(&self, order: NewOrder) -> Result<SubmitResult, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(EngineCommand::Submit {
                order,
                respond_to: tx,
            })
            .await
            .map_err(|_| EngineError::InvalidOrder("engine stopped".into()))?;
        rx.await
            .map_err(|_| EngineError::InvalidOrder("engine dropped response".into()))?
    }

    pub async fn cancel(&self, order_id: OrderId) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(EngineCommand::Cancel {
                order_id,
                respond_to: tx,
            })
            .await
            .map_err(|_| EngineError::InvalidOrder("engine stopped".into()))?;
        rx.await
            .map_err(|_| EngineError::InvalidOrder("engine dropped response".into()))?
    }

    pub async fn snapshot(&self, depth: usize) -> OrderBookSnapshot {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(EngineCommand::Snapshot {
                depth,
                respond_to: tx,
            })
            .await
            .expect("engine running");
        rx.await.expect("engine response")
    }
}

pub fn spawn_engine_task() -> EngineHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<EngineCommand>(1024);
    let (events, _) = broadcast::channel(4096);
    let trades = Arc::new(Mutex::new(VecDeque::with_capacity(TRADE_HISTORY)));

    let events_tx = events.clone();
    let trades_store = trades.clone();

    tokio::spawn(async move {
        let mut engine = Engine::new();
        let book_depth = 50;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                EngineCommand::Submit { order, respond_to } => {
                    let result = engine.submit(order);
                    if let Ok((submit, _deltas)) = &result {
                        for trade in &submit.trades {
                            push_trade(&trades_store, trade.clone());
                            let _ = events_tx.send(MarketEvent::Trade(trade.clone()));
                        }
                        publish_book(&engine, &events_tx, book_depth);
                    }
                    let _ = respond_to.send(result.map(|(s, _)| s));
                }
                EngineCommand::Cancel { order_id, respond_to } => {
                    let result = engine.cancel(order_id).map(|_deltas: Vec<BookDelta>| ());
                    if result.is_ok() {
                        publish_book(&engine, &events_tx, book_depth);
                    }
                    let _ = respond_to.send(result);
                }
                EngineCommand::Snapshot { depth, respond_to } => {
                    let snap = engine.snapshot(depth);
                    let _ = respond_to.send(snap);
                }
            }
        }
    });

    EngineHandle {
        cmd_tx,
        events,
        trades,
    }
}

fn publish_book(engine: &Engine, events: &broadcast::Sender<MarketEvent>, depth: usize) {
    let snapshot = engine.snapshot(depth);
    let _ = events.send(MarketEvent::BookUpdate { snapshot });
}

fn push_trade(store: &Arc<Mutex<VecDeque<Trade>>>, trade: Trade) {
    if let Ok(mut q) = store.try_lock() {
        if q.len() >= TRADE_HISTORY {
            q.pop_front();
        }
        q.push_back(trade);
    }
}
