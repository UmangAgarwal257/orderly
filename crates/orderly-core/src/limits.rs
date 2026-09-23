/// Maximum base quantity per order (inclusive).
pub const MAX_ORDER_QTY: u64 = 1_000_000_000;
/// Maximum price tick (inclusive).
pub const MAX_ORDER_PRICE: u64 = 1_000_000_000_000;
/// Cap fills per submit to bound work (similar to OpenBook `limit` / CU guards).
pub const DEFAULT_MAX_MATCHES: u32 = 1_024;
