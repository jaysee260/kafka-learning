//! Stage 2: analytics service. Keeps running revenue totals.
//!
//!     cargo run --bin analytics
//!
//! Mirrors `shop/consumers/analytics.py`. Prints a small report every 10
//! orders. Like inventory, state is in memory: stage 4 has you rebuild it
//! from scratch by resetting this group's offsets.

use std::collections::HashMap;

use shop::consumer::run_consumer;
use shop::events::{OrderPlaced, cents};

const REPORT_EVERY: u64 = 10;

/// All the running totals, grouped in one struct. Python uses four
/// globals; bundling them makes it obvious what state the service owns.
#[derive(Default)]
struct Stats {
    order_count: u64,
    revenue_cents: i64,
    revenue_by_customer: HashMap<String, i64>,
    units_by_sku: HashMap<String, i64>,
}

impl Stats {
    /// Fold one order into the totals. `&mut self` means this method is
    /// allowed to change the struct.
    fn record(&mut self, order: &OrderPlaced) {
        self.order_count += 1;
        self.revenue_cents += order.total_cents;
        // `*map.entry(k).or_insert(0) += n` is Rust's `counter[k] += n`.
        *self.revenue_by_customer.entry(order.customer_id.clone()).or_insert(0) += order.total_cents;
        for item in &order.items {
            *self.units_by_sku.entry(item.sku.clone()).or_insert(0) += item.qty;
        }
    }

    fn report(&self) {
        let (top_customer, top_spend) = top(&self.revenue_by_customer);
        let (top_sku, top_units) = top(&self.units_by_sku);
        println!(
            "--- {} orders | revenue {} | top customer {} ({}) | best seller {} ({} units) ---",
            self.order_count,
            cents(self.revenue_cents),
            top_customer,
            cents(top_spend),
            top_sku,
            top_units,
        );
    }
}

/// The key with the biggest value, like `Counter.most_common(1)[0]`.
/// (Ties may pick a different key than Python, since HashMap is unordered.)
fn top(counts: &HashMap<String, i64>) -> (&str, i64) {
    counts
        .iter()
        .max_by_key(|&(_key, &value)| value)
        .map(|(key, &value)| (key.as_str(), value))
        .unwrap_or(("-", 0))
}

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let mut stats = Stats::default(); // all zeros and empty maps

    run_consumer("analytics", |order, _msg| {
        stats.record(order);
        if stats.order_count % REPORT_EVERY == 0 {
            stats.report();
        }
    })
    .await
}
