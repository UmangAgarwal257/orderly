use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use orderly_core::{
    EngineError, NewOrder, OrderBookSnapshot, OrderId, OrderStatus, OrderType, Side, SubmitResult,
    Trade,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::engine_task::{EngineHandle, MarketEvent};

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/orders", post(place_order))
        .route("/v1/orders/{id}", delete(cancel_order))
        .route("/v1/orderbook", get(get_orderbook))
        .route("/v1/trades", get(get_trades))
        .route("/v1/ws", get(ws_handler))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Debug, Deserialize)]
struct PlaceOrderRequest {
    side: Side,
    #[serde(rename = "type")]
    order_type: String,
    price: Option<u64>,
    qty: u64,
    client_order_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct PlaceOrderResponse {
    order_id: u64,
    status: OrderStatus,
    filled_qty: u64,
    remaining_qty: u64,
    trades: Vec<Trade>,
    reject_reason: Option<String>,
}

async fn place_order(
    State(state): State<AppState>,
    Json(body): Json<PlaceOrderRequest>,
) -> Result<Json<PlaceOrderResponse>, ApiError> {
    let order_type = match body.order_type.as_str() {
        "limit" => {
            let price = body
                .price
                .ok_or_else(|| ApiError::bad_request("limit orders require price"))?;
            OrderType::Limit { price }
        }
        "market" => OrderType::Market,
        other => return Err(ApiError::bad_request(format!("unknown order type: {other}"))),
    };

    let order = NewOrder {
        side: body.side,
        order_type,
        qty: body.qty,
        client_order_id: body.client_order_id,
    };

    let result = state.engine.submit(order).await?;
    Ok(Json(PlaceOrderResponse::from(result)))
}

impl From<SubmitResult> for PlaceOrderResponse {
    fn from(r: SubmitResult) -> Self {
        Self {
            order_id: r.order_id.0,
            status: r.status,
            filled_qty: r.filled_qty,
            remaining_qty: r.remaining_qty,
            trades: r.trades,
            reject_reason: r.reject_reason,
        }
    }
}

async fn cancel_order(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    state.engine.cancel(OrderId(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct OrderbookQuery {
    depth: Option<usize>,
}

async fn get_orderbook(
    State(state): State<AppState>,
    Query(q): Query<OrderbookQuery>,
) -> Json<OrderBookSnapshot> {
    let depth = q.depth.unwrap_or(20).min(100);
    Json(state.engine.snapshot(depth).await)
}

#[derive(Debug, Deserialize)]
struct TradesQuery {
    limit: Option<usize>,
}

async fn get_trades(
    State(state): State<AppState>,
    Query(q): Query<TradesQuery>,
) -> Json<Vec<Trade>> {
    let limit = q.limit.unwrap_or(100).min(1000);
    let trades = state.engine.trades.lock().await;
    let start = trades.len().saturating_sub(limit);
    Json(trades.iter().skip(start).cloned().collect())
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

#[derive(Debug, Deserialize)]
struct WsClientMessage {
    op: String,
    channels: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "channel", rename_all = "lowercase")]
enum WsServerMessage {
    Trade { data: Trade },
    Book { data: OrderBookSnapshot },
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.engine.events.subscribe();

    let depth = 20usize;
    let initial = state.engine.snapshot(depth).await;
    let hello = WsServerMessage::Book { data: initial };
    if send_json(&mut sender, &hello).await.is_err() {
        return;
    }

    let mut want_trades = true;
    let mut want_book = true;

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(msg) = serde_json::from_str::<WsClientMessage>(&text) {
                            if msg.op == "subscribe" {
                                let ch = msg.channels.unwrap_or_default();
                                if !ch.is_empty() {
                                    want_trades = ch.iter().any(|c| c == "trades");
                                    want_book = ch.iter().any(|c| c == "book");
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            evt = events.recv() => {
                match evt {
                    Ok(MarketEvent::Trade(trade)) if want_trades => {
                        let msg = WsServerMessage::Trade { data: trade };
                        if send_json(&mut sender, &msg).await.is_err() {
                            break;
                        }
                    }
                    Ok(MarketEvent::BookUpdate { snapshot }) if want_book => {
                        let msg = WsServerMessage::Book { data: snapshot };
                        if send_json(&mut sender, &msg).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn send_json(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &WsServerMessage,
) -> Result<(), ()> {
    let text = serde_json::to_string(msg).map_err(|_| ())?;
    sender.send(Message::Text(text.into())).await.map_err(|_| ())
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
}

impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::OrderNotFound => Self {
                status: StatusCode::NOT_FOUND,
                message: err.to_string(),
            },
            EngineError::NotCancellable(_, _) => Self {
                status: StatusCode::CONFLICT,
                message: err.to_string(),
            },
            EngineError::InvalidOrder(_) => Self {
                status: StatusCode::BAD_REQUEST,
                message: err.to_string(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}
