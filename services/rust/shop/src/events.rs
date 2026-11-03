//! Event shapes and a generator for fake orders.
//!
//! Mirrors `services/python/shop/events.py`. The JSON on the wire is
//! identical, which is the whole point: the Python producer's messages can
//! be read here, and this producer's messages can be read by Python.

use chrono::Utc;
use rand::RngExt;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};

pub const CUSTOMERS: [&str; 8] = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"];

/// (sku, name, unit price in cents). Money is kept in integer cents to avoid
/// floating point rounding surprises.
///
/// Python uses a dict here. A fixed array of tuples is the simplest Rust
/// equivalent for a small, never-changing table.
pub const CATALOG: [(&str, &str, i64); 5] = [
    ("SKU-MUG", "Coffee mug", 1200),
    ("SKU-TEE", "T-shirt", 2500),
    ("SKU-HAT", "Baseball cap", 1800),
    ("SKU-STK", "Sticker pack", 500),
    ("SKU-BAG", "Tote bag", 3000),
];

// ---------------------------------------------------------------------------
// The event shape
// ---------------------------------------------------------------------------
//
// In Python an event is a dict, and you discover a missing field when
// `order["items"]` raises KeyError halfway through handling it. In Rust we
// describe the shape once, and serde checks the WHOLE message when it is
// parsed: you get either a fully valid `OrderPlaced` or an `Err`, never a
// half-valid one.
//
// `#[derive(Serialize, Deserialize)]` generates the JSON conversion code
// for us. Field names become JSON keys as-is.
//
// serde ignores JSON keys that aren't listed here. So a producer can ADD a
// field without breaking this code, but renaming or removing one that we
// list would make parsing fail (see "Check your understanding" in stage 7).

/// One OrderPlaced event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPlaced {
    /// `type` is a reserved word in Rust, so the field is called
    /// `event_type` in code and serde maps it to the JSON key "type".
    #[serde(rename = "type")]
    pub event_type: String,
    pub order_id: String,
    pub customer_id: String,
    pub items: Vec<Item>,
    pub total_cents: i64,
    /// ISO 8601 timestamp, kept as a string to match the Python output
    /// exactly. (chrono can parse it if a service ever needs a real date.)
    pub placed_at: String,
}

/// One line of an order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub sku: String,
    pub name: String,
    pub qty: i64,
    pub unit_price_cents: i64,
}

impl OrderPlaced {
    /// The first 8 characters of the order id, used in log lines.
    /// Same as `order["order_id"][:8]` in Python.
    pub fn short_id(&self) -> &str {
        // `.get(..8)` returns None instead of panicking if the id is
        // shorter than 8 bytes, so a weird id can't crash the service.
        self.order_id.get(..8).unwrap_or(&self.order_id)
    }
}

/// Build one random OrderPlaced event.
pub fn make_order_placed() -> OrderPlaced {
    let mut rng = rand::rng();

    // Pick 1 to 3 distinct products, each with a quantity of 1 to 4.
    let how_many = rng.random_range(1..=3);
    let items: Vec<Item> = CATALOG
        .sample(&mut rng, how_many)
        .map(|&(sku, name, price)| Item {
            sku: sku.to_string(),
            name: name.to_string(),
            qty: rng.random_range(1..=4),
            unit_price_cents: price,
        })
        .collect();

    OrderPlaced {
        event_type: "OrderPlaced".to_string(),
        order_id: uuid::Uuid::new_v4().to_string(),
        customer_id: CUSTOMERS.choose(&mut rng).unwrap().to_string(),
        total_cents: items.iter().map(|i| i.qty * i.unit_price_cents).sum(),
        items,
        placed_at: Utc::now().to_rfc3339(),
    }
}

/// Kafka only stores bytes. We decide how to turn objects into bytes.
pub fn serialize(event: &OrderPlaced) -> Vec<u8> {
    // Serializing a plain struct to JSON can't fail, so unwrap is safe.
    serde_json::to_vec(event).expect("OrderPlaced is always valid JSON")
}

/// The inverse of `serialize()`.
///
/// Python's version raises on bad input. Here the possibility of failure
/// is part of the return type: callers MUST deal with the `Err` case,
/// even if only by passing it up with `?`.
pub fn deserialize(raw: &[u8]) -> Result<OrderPlaced, serde_json::Error> {
    serde_json::from_slice(raw)
}

/// Format integer cents as dollars, like Python's `f"${amount / 100:,.2f}"`:
/// `cents(123456)` is `"$1,234.56"`.
pub fn cents(amount: i64) -> String {
    let sign = if amount < 0 { "-" } else { "" };
    let amount = amount.abs();
    let dollars = (amount / 100).to_string();

    // Insert a comma every three digits, counting from the right.
    let mut grouped = String::new();
    for (i, digit) in dollars.chars().enumerate() {
        if i > 0 && (dollars.len() - i).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    format!("{sign}${grouped}.{:02}", amount % 100)
}

// `cargo test` runs these. Tests usually live right next to the code.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cents_matches_python_formatting() {
        assert_eq!(cents(0), "$0.00");
        assert_eq!(cents(505), "$5.05");
        assert_eq!(cents(123456), "$1,234.56");
        assert_eq!(cents(100000000), "$1,000,000.00");
    }

    #[test]
    fn round_trips_through_json() {
        let order = make_order_placed();
        let back = deserialize(&serialize(&order)).unwrap();
        assert_eq!(back.order_id, order.order_id);
        assert_eq!(back.total_cents, order.total_cents);
    }

    #[test]
    fn reads_json_written_by_python() {
        // Exactly what `json.dumps(make_order_placed())` produces.
        let raw = br#"{"type": "OrderPlaced", "order_id": "4f1c2b9e-0000-4000-8000-000000000000",
            "customer_id": "alice", "items": [{"sku": "SKU-MUG", "name": "Coffee mug", "qty": 2,
            "unit_price_cents": 1200}], "total_cents": 2400, "placed_at": "2026-11-03T21:00:00+00:00"}"#;
        let order = deserialize(raw).unwrap();
        assert_eq!(order.short_id(), "4f1c2b9e");
        assert_eq!(order.items[0].qty, 2);
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(deserialize(b"{this is not valid json").is_err());
    }
}
