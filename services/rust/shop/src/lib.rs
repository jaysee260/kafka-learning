//! Shared code for the Rust shop services.
//!
//! This is the Rust counterpart of the Python `shop` package. Each service
//! is a small program in `src/bin/`, and everything they have in common
//! lives in this library:
//!
//! | Rust module          | Python equivalent      | What it holds                      |
//! | -------------------- | ---------------------- | ---------------------------------- |
//! | [`config`]           | `shop/config.py`       | broker address, topic names        |
//! | [`events`]           | `shop/events.py`       | the OrderPlaced shape, fake orders |
//! | [`consumer`]         | `shop/consumer.py`     | the reusable consume loop          |
//!
//! A program in `src/bin/` uses this library as `shop::...`, exactly the way
//! a Python service does `from shop.config import ...`.

pub mod config;
pub mod consumer;
pub mod events;

/// The error type our functions return.
///
/// `Box<dyn std::error::Error>` means "any kind of error". It lets us use
/// `?` on Kafka errors, JSON errors and I/O errors alike without defining
/// our own error enum. Real services often use the `anyhow` crate for
/// this; a plain Box keeps the dependency list short.
pub type Error = Box<dyn std::error::Error>;
