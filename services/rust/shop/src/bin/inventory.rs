//! Stage 2: inventory service. Decrements stock for every item ordered.
//!
//!     cargo run --bin inventory
//!     GROUP_ID=inventory-rust cargo run --bin inventory
//!
//! Mirrors `shop/consumers/inventory.py`, and prints identical lines.
//!
//! By default it joins the consumer group `inventory`, the same one the
//! Python service uses. Run one of each and they split the 3 partitions
//! between them: one group, two languages. Set GROUP_ID to anything else
//! and it becomes an independent reader that sees every order (stage 7).
//!
//! Stock lives in memory, so it resets when the service restarts. That is
//! deliberate: in stage 4 you rebuild state by replaying the topic.

use std::collections::HashMap;

use shop::consumer::run_consumer;
use shop::events::CATALOG;

const STARTING_STOCK: i64 = 200;
const LOW_STOCK: i64 = 20;

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let group_id = std::env::var("GROUP_ID").unwrap_or_else(|_| "inventory".to_string());

    // sku -> units left. Python keeps this in a module-level global; here
    // it's a local variable that the closure below captures and mutates.
    // Only the consume loop touches it, so there's no need for a lock.
    let mut stock: HashMap<String, i64> = CATALOG
        .iter()
        .map(|&(sku, _name, _price)| (sku.to_string(), STARTING_STOCK))
        .collect();

    // `|order, msg| { ... }` is a closure: an inline function. `move` would
    // copy/move captured variables in; we don't need it because `stock`
    // outlives the call to run_consumer, so the closure can just borrow it.
    run_consumer(&group_id, |order, msg| {
        use rdkafka::Message; // brings .partition() and .offset() into scope

        for item in &order.items {
            // Python would raise KeyError on an unknown SKU. `entry` inserts
            // a starting value if the SKU is missing, then gives us a
            // mutable reference to the count either way.
            let left = stock.entry(item.sku.clone()).or_insert(STARTING_STOCK);
            *left -= item.qty;

            let flag = if *left <= LOW_STOCK { "  LOW STOCK" } else { "" };
            println!(
                "[p{} @{}] {} -{} -> {} left{}",
                msg.partition(),
                msg.offset(),
                item.sku,
                item.qty,
                left,
                flag
            );
        }
    })
    .await
}
