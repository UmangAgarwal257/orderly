use criterion::{black_box, criterion_group, criterion_main, Criterion};
use orderly_core::{Engine, NewOrder, OrderType, Side};

fn bench_match_throughput(c: &mut Criterion) {
    c.bench_function("match_10k_limits", |b| {
        b.iter(|| {
            let mut engine = Engine::new();
            for i in 0..10_000u64 {
                let price = 100 + (i % 10);
                let _ = engine.submit(NewOrder {
                    side: Side::Ask,
                    order_type: OrderType::Limit { price },
                    qty: 1,
                    client_order_id: None,
                });
            }
            for i in 0..10_000u64 {
                let price = 100 + (i % 10);
                let _ = black_box(engine.submit(NewOrder {
                    side: Side::Bid,
                    order_type: OrderType::Limit { price },
                    qty: 1,
                    client_order_id: None,
                }));
            }
        });
    });
}

criterion_group!(benches, bench_match_throughput);
criterion_main!(benches);
