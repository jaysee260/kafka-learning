//! Stage 2: notification service. "Emails" each customer a confirmation.
//!
//!     cargo run --bin notifications
//!
//! Mirrors `shop/consumers/notifications.py`. A real version would call an
//! email provider. Think about what happens if that call succeeds but the
//! service crashes before the offset is committed: the customer gets the
//! email twice. Stage 4 digs into this.

use shop::consumer::run_consumer;
use shop::events::cents;

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    // This service keeps no state, so the closure captures nothing.
    // `_msg` is unused; the leading underscore tells Rust that's on purpose.
    run_consumer("notifications", |order, _msg| {
        // Python: ", ".join(f"{i['qty']}x {i['name']}" for i in order["items"])
        let lines: Vec<String> = order.items.iter().map(|i| format!("{}x {}", i.qty, i.name)).collect();

        println!(
            "To: {}@example.com | Thanks for order {}: {} ({})",
            order.customer_id,
            order.short_id(),
            lines.join(", "),
            cents(order.total_cents),
        );
    })
    .await
}
