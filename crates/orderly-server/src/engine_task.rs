use std::collections::VecDeque;

use orderly_core::{
    BookDelta, Engine, EngineError, NewOrder, OrderBookSnapshot, OrderId, OrderRecord,
    SubmitResult, Trade,
};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::config::DEFAULT_BOOK_DEPTH;

const TRADE_HISTORY: usize = 10_000;

enum EngineCommand {
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
    GetOrder {
        order_id: OrderId,
        respond_to: oneshot::Sender<Option<OrderRecord>>,
    },
    RecentTrades {
        limit: usize,
        respond_to: oneshot::Sender<Vec<Trade>>,
    },
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    Trade(Trade),
    BookDelta(BookDelta),
}

#[derive(Clone)]
pub struct EngineHandle {
    cmd_tx: mpsc::Sender<EngineCommand>,
    events: broadcast::Sender<MarketEvent>,
}

impl EngineHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<MarketEvent> {
        self.events.subscribe()
    }

    pub async fn submit(&self, order: NewOrder) -> Result<SubmitResult, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(EngineCommand::Submit {
            order,
            respond_to: tx,
        })
        .await?;
        rx.await
            .map_err(|_| EngineError::InvalidOrder("engine dropped response".into()))?
    }

    pub async fn cancel(&self, order_id: OrderId) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(EngineCommand::Cancel {
            order_id,
            respond_to: tx,
        })
        .await?;
        rx.await
            .map_err(|_| EngineError::InvalidOrder("engine dropped response".into()))?
    }

    pub async fn snapshot(&self, depth: usize) -> OrderBookSnapshot {
        let (tx, rx) = oneshot::channel();
        self.send(EngineCommand::Snapshot {
            depth,
            respond_to: tx,
        })
        .await
        .expect("engine running");
        rx.await.expect("engine response")
    }

    pub async fn get_order(&self, order_id: OrderId) -> Option<OrderRecord> {
        let (tx, rx) = oneshot::channel();
        self.send(EngineCommand::GetOrder {
            order_id,
            respond_to: tx,
        })
        .await
        .expect("engine running");
        rx.await.expect("engine response")
    }

    pub async fn recent_trades(&self, limit: usize) -> Vec<Trade> {
        let (tx, rx) = oneshot::channel();
        self.send(EngineCommand::RecentTrades {
            limit,
            respond_to: tx,
        })
        .await
        .expect("engine running");
        rx.await.expect("engine response")
    }

    async fn send(&self, cmd: EngineCommand) -> Result<(), EngineError> {
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| EngineError::InvalidOrder("engine stopped".into()))
    }
}

pub fn spawn_engine_task() -> EngineHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<EngineCommand>(4096);
    let (events, _) = broadcast::channel(8192);

    let events_tx = events.clone();

    tokio::spawn(async move {
        let mut engine = Engine::new();
        let mut recent_trades: VecDeque<Trade> = VecDeque::with_capacity(TRADE_HISTORY);

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                EngineCommand::Submit { order, respond_to } => {
                    let result = engine.submit(order);
                    if let Ok((submit, deltas)) = &result {
                        for trade in &submit.trades {
                            record_trade(&mut recent_trades, trade.clone());
                            let _ = events_tx.send(MarketEvent::Trade(trade.clone()));
                        }
                        for delta in deltas {
                            let _ = events_tx.send(MarketEvent::BookDelta(delta.clone()));
                        }
                    }
                    let _ = respond_to.send(result.map(|(s, _)| s));
                }
                EngineCommand::Cancel {
                    order_id,
                    respond_to,
                } => {
                    let result = engine.cancel(order_id);
                    if let Ok(deltas) = &result {
                        for delta in deltas {
                            let _ = events_tx.send(MarketEvent::BookDelta(delta.clone()));
                        }
                    }
                    let _ = respond_to.send(result.map(|_| ()));
                }
                EngineCommand::Snapshot { depth, respond_to } => {
                    let snap = engine.snapshot(depth);
                    let _ = respond_to.send(snap);
                }
                EngineCommand::GetOrder {
                    order_id,
                    respond_to,
                } => {
                    let _ = respond_to.send(engine.get_order(order_id));
                }
                EngineCommand::RecentTrades { limit, respond_to } => {
                    let start = recent_trades.len().saturating_sub(limit);
                    let slice: Vec<Trade> = recent_trades.iter().skip(start).cloned().collect();
                    let _ = respond_to.send(slice);
                }
            }
        }
    });

    EngineHandle { cmd_tx, events }
}

fn record_trade(recent: &mut VecDeque<Trade>, trade: Trade) {
    if recent.len() >= TRADE_HISTORY {
        recent.pop_front();
    }
    recent.push_back(trade);
}

pub async fn initial_book_snapshot(handle: &EngineHandle) -> OrderBookSnapshot {
    handle.snapshot(DEFAULT_BOOK_DEPTH).await
}
