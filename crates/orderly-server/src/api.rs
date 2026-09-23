use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use orderly_core::{
    EngineError, NewOrder, OrderBookSnapshot, OrderId, OrderRecord, OrderStatus, OrderType, Side,
    SubmitResult, Trade,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::config::{
    dev_cors_enabled, DEFAULT_BOOK_DEPTH, DEFAULT_TRADES_QUERY, MAX_BOOK_DEPTH, MAX_TRADES_QUERY,
};
use crate::engine_task::{initial_book_snapshot, EngineHandle, MarketEvent};

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
}

pub fn router(state: AppState) -> Router {
    let cors = if dev_cors_enabled() {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_headers(Any)
    } else {
        CorsLayer::new()
            .allow_origin([
                HeaderValue::from_static("http://localhost:3000"),
                HeaderValue::from_static("http://127.0.0.1:3000"),
            ])
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_headers(Any)
    };

    Router::new()
        .route("/health", get(health))
        .route("/v1/orders", post(place_order))
        .route("/v1/orders/{id}", get(get_order).delete(cancel_order))
        .route("/v1/orderbook", get(get_orderbook))
        .route("/v1/trades", get(get_trades))
        .route("/v1/ws", get(ws_handler))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Debug, Deserialize)]
struct PlaceOrderRequest {
    side: Side,
    qty: u64,
    client_order_id: Option<u64>,
    #[serde(flatten)]
    order_type: OrderType,
}

#[derive(Debug, Serialize)]
struct PlaceOrderResponse {
    order_id: u64,
    status: OrderStatus,
    filled_qty: u64,
    remaining_qty: u64,
    cancelled_qty: u64,
    trades: Vec<Trade>,
    reject_reason: Option<String>,
    client_order_id: Option<u64>,
}

async fn place_order(
    State(state): State<AppState>,
    Json(body): Json<PlaceOrderRequest>,
) -> Result<Json<PlaceOrderResponse>, ApiError> {
    let order = NewOrder {
        side: body.side,
        order_type: body.order_type,
        qty: body.qty,
        client_order_id: body.client_order_id,
        max_matches: None,
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
            cancelled_qty: r.cancelled_qty,
            trades: r.trades,
            reject_reason: r.reject_reason,
            client_order_id: r.client_order_id,
        }
    }
}

async fn get_order(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<OrderRecord>, ApiError> {
    match state.engine.get_order(OrderId(id)).await {
        Some(record) => Ok(Json(record)),
        None => Err(ApiError::not_found("order not found")),
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
    let depth = q.depth.unwrap_or(DEFAULT_BOOK_DEPTH).min(MAX_BOOK_DEPTH);
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
    let limit = q
        .limit
        .unwrap_or(DEFAULT_TRADES_QUERY)
        .min(MAX_TRADES_QUERY);
    Json(state.engine.recent_trades(limit).await)
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
    BookDelta { data: orderly_core::BookDelta },
    Book { data: OrderBookSnapshot },
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.engine.subscribe();

    let initial = initial_book_snapshot(&state.engine).await;
    if send_json(&mut sender, &WsServerMessage::Book { data: initial })
        .await
        .is_err()
    {
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
                        if send_json(&mut sender, &WsServerMessage::Trade { data: trade }).await.is_err() {
                            break;
                        }
                    }
                    Ok(MarketEvent::BookDelta(delta)) if want_book => {
                        if send_json(&mut sender, &WsServerMessage::BookDelta { data: delta }).await.is_err() {
                            break;
                        }
                    }
                    Ok(MarketEvent::Trade(_) | MarketEvent::BookDelta(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "websocket client lagged on market events");
                        continue;
                    }
                    Err(_) => break,
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
    sender
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
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

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
}

impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::OrderNotFound => Self::not_found(err.to_string()),
            EngineError::NotCancellable(_, _) => Self {
                status: StatusCode::CONFLICT,
                message: err.to_string(),
            },
            EngineError::InvalidOrder(_) => Self::bad_request(err.to_string()),
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
