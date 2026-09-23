use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::book::{OrderBook, RestingOrder};
use crate::limits::{DEFAULT_MAX_MATCHES, MAX_ORDER_PRICE, MAX_ORDER_QTY};
use crate::types::{
    BookDelta, BookDeltaKind, EngineError, NewOrder, OrderBookSnapshot, OrderId, OrderRecord,
    OrderStatus, OrderType, Side, SubmitResult, Trade,
};

#[derive(Debug)]
struct OrderMeta {
    status: OrderStatus,
    side: Side,
    order_type: OrderType,
    qty_original: u64,
    qty_remaining: u64,
    client_order_id: Option<u64>,
}

#[derive(Debug)]
pub struct Engine {
    book: OrderBook,
    orders: HashMap<OrderId, OrderMeta>,
    next_order_id: u64,
    next_trade_id: u64,
    next_sequence: u64,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Self {
            book: OrderBook::default(),
            orders: HashMap::new(),
            next_order_id: 1,
            next_trade_id: 1,
            next_sequence: 1,
        }
    }

    pub fn submit(
        &mut self,
        request: NewOrder,
    ) -> Result<(SubmitResult, Vec<BookDelta>), EngineError> {
        validate_new_order(&request)?;

        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let client_order_id = request.client_order_id;
        self.orders.insert(
            order_id,
            OrderMeta {
                status: OrderStatus::New,
                side: request.side,
                order_type: request.order_type,
                qty_original: request.qty,
                qty_remaining: request.qty,
                client_order_id,
            },
        );

        let mut trades = Vec::new();
        let mut deltas = Vec::new();
        let max_matches = request.max_matches.unwrap_or(DEFAULT_MAX_MATCHES);

        self.match_order(order_id, max_matches, &mut trades, &mut deltas)?;

        let mut reject_reason = None;
        let mut cancelled_qty = 0u64;

        let remaining_before_reject = self
            .orders
            .get(&order_id)
            .map(|m| m.qty_remaining)
            .unwrap_or(0);

        if remaining_before_reject > 0 {
            match self.orders.get(&order_id).map(|m| m.order_type) {
                Some(OrderType::Market) => {
                    cancelled_qty = remaining_before_reject;
                    let m = self
                        .orders
                        .get_mut(&order_id)
                        .ok_or(EngineError::OrderNotFound)?;
                    if trades.is_empty() {
                        m.status = OrderStatus::Rejected;
                    } else {
                        m.status = OrderStatus::PartiallyFilled;
                    }
                    m.qty_remaining = 0;
                    reject_reason = Some("market order unfilled remainder cancelled".into());
                }
                Some(OrderType::Limit { .. }) => {
                    self.rest_limit(order_id, &mut deltas)?;
                }
                None => return Err(EngineError::OrderNotFound),
            }
        }

        let meta = self
            .orders
            .get(&order_id)
            .ok_or(EngineError::OrderNotFound)?;
        let filled_qty = trades.iter().map(|t| t.qty).sum();
        let result = SubmitResult {
            order_id,
            status: meta.status,
            filled_qty,
            remaining_qty: meta.qty_remaining,
            cancelled_qty,
            trades,
            reject_reason,
            client_order_id: meta.client_order_id,
        };

        Ok((result, deltas))
    }

    fn match_order(
        &mut self,
        taker_id: OrderId,
        max_matches: u32,
        trades: &mut Vec<Trade>,
        deltas: &mut Vec<BookDelta>,
    ) -> Result<(), EngineError> {
        let mut matches = 0u32;

        while matches < max_matches {
            let (taker_side, taker_remaining, limit_price) = {
                let taker = self
                    .orders
                    .get(&taker_id)
                    .ok_or(EngineError::OrderNotFound)?;
                if taker.qty_remaining == 0 {
                    break;
                }
                let limit_price = match taker.order_type {
                    OrderType::Limit { price } => price,
                    OrderType::Market => match taker.side {
                        Side::Bid => u64::MAX,
                        Side::Ask => 0,
                    },
                };
                (taker.side, taker.qty_remaining, limit_price)
            };

            let maker_id = match taker_side {
                Side::Bid => self.book.best_ask_id(),
                Side::Ask => self.book.best_bid_id(),
            };

            let maker_id = match maker_id {
                Some(id) => id,
                None => break,
            };

            let (maker_price, maker_side, maker_qty) = {
                let maker = self
                    .book
                    .get(maker_id)
                    .ok_or_else(|| EngineError::InvalidOrder("book inconsistent".into()))?;
                (maker.price, maker.side, maker.qty_remaining)
            };

            let crosses = match taker_side {
                Side::Bid => limit_price >= maker_price,
                Side::Ask => limit_price <= maker_price,
            };
            if !crosses {
                break;
            }

            let fill_qty = taker_remaining.min(maker_qty);

            let trade = Trade {
                id: self.next_trade_id,
                taker_order_id: taker_id,
                maker_order_id: maker_id,
                price: maker_price,
                qty: fill_qty,
                timestamp_ns: now_ns(),
            };
            self.next_trade_id += 1;
            trades.push(trade);
            matches += 1;

            {
                let taker_meta = self
                    .orders
                    .get_mut(&taker_id)
                    .ok_or(EngineError::OrderNotFound)?;
                taker_meta.qty_remaining -= fill_qty;
                taker_meta.status = if taker_meta.qty_remaining == 0 {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
            }

            let maker_remaining = {
                let resting = self
                    .book
                    .get_mut(maker_id)
                    .ok_or_else(|| EngineError::InvalidOrder("book inconsistent".into()))?;
                resting.qty_remaining -= fill_qty;
                resting.qty_remaining
            };

            if maker_remaining == 0 {
                self.book.remove(maker_id);
                let maker_meta = self
                    .orders
                    .get_mut(&maker_id)
                    .ok_or(EngineError::OrderNotFound)?;
                maker_meta.qty_remaining = 0;
                maker_meta.status = OrderStatus::Filled;
                deltas.push(level_delta_after_remove(
                    maker_side,
                    maker_price,
                    &self.book,
                ));
            } else {
                let maker_meta = self
                    .orders
                    .get_mut(&maker_id)
                    .ok_or(EngineError::OrderNotFound)?;
                maker_meta.qty_remaining = maker_remaining;
                maker_meta.status = OrderStatus::PartiallyFilled;
                deltas.push(level_delta(
                    maker_side,
                    maker_price,
                    &self.book,
                    BookDeltaKind::Update,
                ));
            }
        }

        Ok(())
    }

    fn rest_limit(
        &mut self,
        order_id: OrderId,
        deltas: &mut Vec<BookDelta>,
    ) -> Result<(), EngineError> {
        let meta = self
            .orders
            .get(&order_id)
            .ok_or(EngineError::OrderNotFound)?;
        let OrderType::Limit { price } = meta.order_type else {
            return Ok(());
        };
        if meta.qty_remaining == 0 {
            return Ok(());
        }

        let sequence = self.next_sequence;
        self.next_sequence += 1;

        let resting = RestingOrder {
            id: order_id,
            side: meta.side,
            price,
            qty_remaining: meta.qty_remaining,
            sequence,
        };
        self.book.insert(resting);

        let kind = {
            let level = self.book.level_at_price(meta.side, price);
            if level.order_count == 1 {
                BookDeltaKind::Add
            } else {
                BookDeltaKind::Update
            }
        };
        deltas.push(level_delta(meta.side, price, &self.book, kind));
        Ok(())
    }

    pub fn cancel(&mut self, order_id: OrderId) -> Result<Vec<BookDelta>, EngineError> {
        let meta = self
            .orders
            .get(&order_id)
            .ok_or(EngineError::OrderNotFound)?;

        match meta.status {
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected => {
                return Err(EngineError::NotCancellable(order_id.0, meta.status));
            }
            OrderStatus::New | OrderStatus::PartiallyFilled => {}
        }

        if self.book.get(order_id).is_none() {
            return Err(EngineError::NotCancellable(order_id.0, meta.status));
        }

        let removed = self
            .book
            .remove(order_id)
            .ok_or_else(|| EngineError::InvalidOrder("order missing from book".into()))?;
        let price = removed.price;
        let side = removed.side;

        let meta = self
            .orders
            .get_mut(&order_id)
            .ok_or(EngineError::OrderNotFound)?;
        meta.status = OrderStatus::Cancelled;
        meta.qty_remaining = 0;

        let delta = level_delta_after_remove(side, price, &self.book);
        Ok(vec![delta])
    }

    pub fn get_order(&self, order_id: OrderId) -> Option<OrderRecord> {
        self.orders.get(&order_id).map(|m| {
            let filled_qty = m.qty_original.saturating_sub(m.qty_remaining);
            OrderRecord {
                order_id,
                side: m.side,
                order_type: m.order_type,
                status: m.status,
                qty_original: m.qty_original,
                qty_remaining: m.qty_remaining,
                filled_qty,
                client_order_id: m.client_order_id,
            }
        })
    }

    pub fn snapshot(&self, depth: usize) -> OrderBookSnapshot {
        self.book.snapshot(depth)
    }
}

fn validate_new_order(request: &NewOrder) -> Result<(), EngineError> {
    if request.qty == 0 {
        return Err(EngineError::InvalidOrder(
            "quantity must be positive".into(),
        ));
    }
    if request.qty > MAX_ORDER_QTY {
        return Err(EngineError::InvalidOrder(format!(
            "quantity exceeds max {}",
            MAX_ORDER_QTY
        )));
    }
    if let OrderType::Limit { price } = request.order_type {
        if price == 0 {
            return Err(EngineError::InvalidOrder(
                "limit price must be positive".into(),
            ));
        }
        if price > MAX_ORDER_PRICE {
            return Err(EngineError::InvalidOrder(format!(
                "price exceeds max {}",
                MAX_ORDER_PRICE
            )));
        }
    }
    Ok(())
}

fn level_delta(side: Side, price: u64, book: &OrderBook, kind: BookDeltaKind) -> BookDelta {
    let level = book.level_at_price(side, price);
    BookDelta {
        side,
        price,
        qty: level.qty,
        order_count: level.order_count,
        kind,
    }
}

fn level_delta_after_remove(side: Side, price: u64, book: &OrderBook) -> BookDelta {
    let level = book.level_at_price(side, price);
    let kind = if level.order_count == 0 {
        BookDeltaKind::Remove
    } else {
        BookDeltaKind::Update
    };
    BookDelta {
        side,
        price,
        qty: level.qty,
        order_count: level.order_count,
        kind,
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NewOrder;

    fn limit(side: Side, price: u64, qty: u64) -> NewOrder {
        NewOrder {
            side,
            order_type: OrderType::Limit { price },
            qty,
            client_order_id: None,
            max_matches: None,
        }
    }

    fn market(side: Side, qty: u64) -> NewOrder {
        NewOrder {
            side,
            order_type: OrderType::Market,
            qty,
            client_order_id: None,
            max_matches: None,
        }
    }

    #[test]
    fn fifo_at_same_price() {
        let mut engine = Engine::new();
        let (a, _) = engine.submit(limit(Side::Ask, 100, 10)).unwrap();
        let (b, _) = engine.submit(limit(Side::Ask, 100, 20)).unwrap();
        let (taker, _) = engine.submit(limit(Side::Bid, 100, 15)).unwrap();

        assert_eq!(taker.trades.len(), 2);
        assert_eq!(taker.trades[0].maker_order_id, a.order_id);
        assert_eq!(taker.trades[0].qty, 10);
        assert_eq!(taker.trades[1].maker_order_id, b.order_id);
        assert_eq!(taker.trades[1].qty, 5);
    }

    #[test]
    fn partial_fill_across_levels() {
        let mut engine = Engine::new();
        engine.submit(limit(Side::Bid, 99, 5)).unwrap();
        engine.submit(limit(Side::Bid, 98, 5)).unwrap();
        let (res, _) = engine.submit(limit(Side::Ask, 97, 12)).unwrap();

        assert_eq!(res.trades.len(), 2);
        assert_eq!(res.filled_qty, 10);
        assert_eq!(res.status, OrderStatus::PartiallyFilled);
        assert_eq!(res.remaining_qty, 2);
    }

    #[test]
    fn resting_limit_then_match() {
        let mut engine = Engine::new();
        let (bid, _) = engine.submit(limit(Side::Bid, 100, 50)).unwrap();
        assert_eq!(bid.status, OrderStatus::New);

        let (ask, _) = engine.submit(limit(Side::Ask, 100, 30)).unwrap();
        assert_eq!(ask.status, OrderStatus::Filled);
        assert_eq!(
            engine.get_order(bid.order_id).unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn market_cancels_unfilled_remainder() {
        let mut engine = Engine::new();
        engine.submit(limit(Side::Ask, 100, 5)).unwrap();
        let (res, _) = engine.submit(market(Side::Bid, 20)).unwrap();
        assert_eq!(res.filled_qty, 5);
        assert_eq!(res.cancelled_qty, 15);
        assert_eq!(res.status, OrderStatus::PartiallyFilled);
        assert!(res.reject_reason.is_some());
    }

    #[test]
    fn market_empty_book_rejected() {
        let mut engine = Engine::new();
        let (res, _) = engine.submit(market(Side::Bid, 10)).unwrap();
        assert_eq!(res.status, OrderStatus::Rejected);
        assert_eq!(res.filled_qty, 0);
    }

    #[test]
    fn cancel_resting() {
        let mut engine = Engine::new();
        let (o, _) = engine.submit(limit(Side::Bid, 100, 10)).unwrap();
        engine.cancel(o.order_id).unwrap();
        assert_eq!(
            engine.get_order(o.order_id).unwrap().status,
            OrderStatus::Cancelled
        );
    }

    #[test]
    fn cancel_filled_fails() {
        let mut engine = Engine::new();
        engine.submit(limit(Side::Ask, 100, 10)).unwrap();
        let (bid, _) = engine.submit(limit(Side::Bid, 100, 10)).unwrap();
        let err = engine.cancel(bid.order_id).unwrap_err();
        assert_eq!(
            err,
            EngineError::NotCancellable(bid.order_id.0, OrderStatus::Filled)
        );
    }

    #[test]
    fn client_order_id_round_trip() {
        let mut engine = Engine::new();
        let order = NewOrder {
            side: Side::Bid,
            order_type: OrderType::Limit { price: 50 },
            qty: 1,
            client_order_id: Some(42),
            max_matches: None,
        };
        let (res, _) = engine.submit(order).unwrap();
        assert_eq!(res.client_order_id, Some(42));
        assert_eq!(
            engine.get_order(res.order_id).unwrap().client_order_id,
            Some(42)
        );
    }
}
