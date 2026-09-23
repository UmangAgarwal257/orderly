use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OrderType {
    Limit { price: u64 },
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: u64,
    pub taker_order_id: OrderId,
    pub maker_order_id: OrderId,
    pub price: u64,
    pub qty: u64,
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: u64,
    pub qty: u64,
    pub order_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: OrderId,
    pub side: Side,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub qty_original: u64,
    pub qty_remaining: u64,
    pub filled_qty: u64,
    pub client_order_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewOrder {
    pub side: Side,
    pub order_type: OrderType,
    pub qty: u64,
    pub client_order_id: Option<u64>,
    /// Max maker touches per submit; defaults to `limits::DEFAULT_MAX_MATCHES`.
    pub max_matches: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub filled_qty: u64,
    pub remaining_qty: u64,
    pub cancelled_qty: u64,
    pub trades: Vec<Trade>,
    pub reject_reason: Option<String>,
    pub client_order_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookDeltaKind {
    Add,
    Update,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookDelta {
    pub side: Side,
    pub price: u64,
    pub qty: u64,
    pub order_count: u32,
    pub kind: BookDeltaKind,
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    #[error("order not found")]
    OrderNotFound,
    #[error("order {0} is not cancellable in status {1:?}")]
    NotCancellable(u64, OrderStatus),
    #[error("invalid order: {0}")]
    InvalidOrder(String),
}
