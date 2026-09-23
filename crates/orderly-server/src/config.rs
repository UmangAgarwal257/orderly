/// Default L2 depth for snapshots and WebSocket bootstrap.
pub const DEFAULT_BOOK_DEPTH: usize = 20;
pub const MAX_BOOK_DEPTH: usize = 100;
pub const MAX_TRADES_QUERY: usize = 1_000;
pub const DEFAULT_TRADES_QUERY: usize = 100;

pub fn dev_cors_enabled() -> bool {
    std::env::var("ORDERLY_DEV_CORS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}
