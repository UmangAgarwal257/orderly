use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::types::{BookLevel, OrderBookSnapshot, OrderId, Side};

#[derive(Debug, Clone)]
pub(crate) struct RestingOrder {
    pub id: OrderId,
    pub side: Side,
    pub price: u64,
    pub qty_remaining: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AskKey {
    price: u64,
    sequence: u64,
}

impl Ord for AskKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price
            .cmp(&other.price)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for AskKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BidKey {
    price: u64,
    sequence: u64,
}

impl Ord for BidKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .price
            .cmp(&self.price)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for BidKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub(crate) struct OrderBook {
    asks: BTreeMap<AskKey, OrderId>,
    bids: BTreeMap<BidKey, OrderId>,
    orders: HashMap<OrderId, RestingOrder>,
}

impl OrderBook {
    pub(crate) fn insert(&mut self, order: RestingOrder) {
        let id = order.id;
        let side = order.side;
        let price = order.price;
        let sequence = order.sequence;
        self.orders.insert(id, order);
        match side {
            Side::Ask => {
                self.asks.insert(AskKey { price, sequence }, id);
            }
            Side::Bid => {
                self.bids.insert(BidKey { price, sequence }, id);
            }
        }
    }

    pub(crate) fn get(&self, id: OrderId) -> Option<&RestingOrder> {
        self.orders.get(&id)
    }

    pub(crate) fn get_mut(&mut self, id: OrderId) -> Option<&mut RestingOrder> {
        self.orders.get_mut(&id)
    }

    pub(crate) fn remove(&mut self, id: OrderId) -> Option<RestingOrder> {
        let order = self.orders.remove(&id)?;
        match order.side {
            Side::Ask => {
                self.asks.remove(&AskKey {
                    price: order.price,
                    sequence: order.sequence,
                });
            }
            Side::Bid => {
                self.bids.remove(&BidKey {
                    price: order.price,
                    sequence: order.sequence,
                });
            }
        }
        Some(order)
    }

    pub(crate) fn best_ask_id(&self) -> Option<OrderId> {
        self.asks.values().next().copied()
    }

    pub(crate) fn best_bid_id(&self) -> Option<OrderId> {
        self.bids.values().next().copied()
    }

    pub(crate) fn level_at_price(&self, side: Side, price: u64) -> BookLevel {
        let (qty, count) = self
            .orders
            .values()
            .filter(|o| o.side == side && o.price == price)
            .fold((0u64, 0u32), |(q, c), o| (q + o.qty_remaining, c + 1));
        BookLevel {
            price,
            qty,
            order_count: count,
        }
    }

    pub(crate) fn snapshot(&self, depth: usize) -> OrderBookSnapshot {
        OrderBookSnapshot {
            bids: aggregate_bids(&self.orders, depth),
            asks: aggregate_asks(&self.orders, depth),
        }
    }
}

fn aggregate_bids(orders: &HashMap<OrderId, RestingOrder>, depth: usize) -> Vec<BookLevel> {
    let mut levels: BTreeMap<u64, (u64, u32)> = BTreeMap::new();
    for o in orders.values().filter(|o| o.side == Side::Bid) {
        let e = levels.entry(o.price).or_insert((0, 0));
        e.0 += o.qty_remaining;
        e.1 += 1;
    }
    let mut out: Vec<BookLevel> = levels
        .into_iter()
        .map(|(price, (qty, order_count))| BookLevel {
            price,
            qty,
            order_count,
        })
        .collect();
    out.sort_by_key(|level| std::cmp::Reverse(level.price));
    out.truncate(depth);
    out
}

fn aggregate_asks(orders: &HashMap<OrderId, RestingOrder>, depth: usize) -> Vec<BookLevel> {
    let mut levels: BTreeMap<u64, (u64, u32)> = BTreeMap::new();
    for o in orders.values().filter(|o| o.side == Side::Ask) {
        let e = levels.entry(o.price).or_insert((0, 0));
        e.0 += o.qty_remaining;
        e.1 += 1;
    }
    let mut out: Vec<BookLevel> = levels
        .into_iter()
        .map(|(price, (qty, order_count))| BookLevel {
            price,
            qty,
            order_count,
        })
        .collect();
    out.sort_by_key(|level| level.price);
    out.truncate(depth);
    out
}
