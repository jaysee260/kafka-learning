//! What the publishers and the consumers both need.

pub mod events;
pub mod kafka_io;
pub mod pi;

/// A setting from the environment, or its default.
pub fn setting(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| default.to_string())
}
