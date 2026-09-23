use futures_util::StreamExt;
use orderly_server::api::{router, AppState};
use orderly_server::engine_task::spawn_engine_task;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let engine = spawn_engine_task();
    let app = router(AppState { engine });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{}", addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (base, handle)
}

#[tokio::test]
async fn place_orders_and_trade_via_rest() {
    let (base, server) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let ask = client
        .post(format!("{}/v1/orders", base))
        .json(&json!({
            "side": "ask",
            "type": "limit",
            "price": 100,
            "qty": 10
        }))
        .send()
        .await
        .unwrap();
    assert!(ask.status().is_success());

    let bid = client
        .post(format!("{}/v1/orders", base))
        .json(&json!({
            "side": "bid",
            "type": "limit",
            "price": 100,
            "qty": 10
        }))
        .send()
        .await
        .unwrap();
    assert!(bid.status().is_success());
    let body = bid.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["status"], "filled");
    assert!(body["trades"].as_array().unwrap().len() >= 1);

    let trades = client
        .get(format!("{}/v1/trades", base))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(trades.as_array().unwrap().len() >= 1);

    server.abort();
}

#[tokio::test]
async fn websocket_receives_trade() {
    let (base, server) = spawn_test_server().await;
    let ws_url = base.replace("http://", "ws://") + "/v1/ws";
    let client = reqwest::Client::new();

    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("ws connect");
    let _ = ws.next().await;

    client
        .post(format!("{}/v1/orders", base))
        .json(&json!({
            "side": "ask",
            "type": "limit",
            "price": 200,
            "qty": 5
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/v1/orders", base))
        .json(&json!({
            "side": "bid",
            "type": "limit",
            "price": 200,
            "qty": 5
        }))
        .send()
        .await
        .unwrap();

    let mut saw_trade = false;
    for _ in 0..10 {
        if let Some(Ok(WsMessage::Text(text))) = ws.next().await {
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            if v.get("channel") == Some(&json!("trade")) {
                saw_trade = true;
                break;
            }
        }
    }
    assert!(saw_trade, "expected trade on websocket");

    server.abort();
}
