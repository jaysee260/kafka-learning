//! Sends an urgent phone alert when our pool finds a block.
//!
//! The events come from the block_watch publisher. The plumbing is in
//! shared/: kafka_io.rs (reading from Redpanda) and ntfy.rs (reaching your
//! phone).

use std::time::Duration;

use anyhow::Result;
use shared::events::Event;
use shared::kafka_io::new_events;
use shared::ntfy::{Alert, send_to_phone};
use shared::setting;

const ONE_DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// What to tell the phone about one event, or None to stay quiet.
fn what_happened(event: &Event) -> Option<Alert> {
    match event {
        Event::BlockFound { height, miner, .. } => Some(Alert {
            title: "BLOCK FOUND".to_string(),
            message: match miner {
                Some(miner) => format!("{miner} solved block {height}"),
                None => format!("Block {height} pays your address"),
            },
            urgent: true,
        }),

        // Best shares are best_share_alerts' job.
        Event::BestShare { .. } => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = setting("TOPIC", "home.mining.block-found");
    let group = setting("GROUP", "block-alerts");

    // "earliest": even on its very first run, never skip a block.
    let mut events = new_events(&[topic.as_str()], &group, "earliest")?;

    while let Some((event, happened_at)) = events.next().await {
        if let Some(alert) = what_happened(&event) {
            send_to_phone(&alert, happened_at, ONE_DAY).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_found_block_is_urgent_and_names_the_miner_when_known() {
        let event = Event::BlockFound {
            source: "ckpool".to_string(),
            height: 950123,
            miner: Some("humble_nano3s".to_string()),
        };
        let alert = what_happened(&event).unwrap();
        assert_eq!(
            (alert.title.as_str(), alert.message.as_str()),
            ("BLOCK FOUND", "humble_nano3s solved block 950123")
        );
        assert!(alert.urgent);

        let event = Event::BlockFound { source: "bitcoin-core".to_string(), height: 950123, miner: None };
        assert_eq!(what_happened(&event).unwrap().message, "Block 950123 pays your address");
    }

    #[test]
    fn best_shares_are_somebody_elses_job() {
        let event = Event::BestShare {
            source: "ckpool".to_string(),
            miner: "humble_nano3s".to_string(),
            difficulty: 2100000000,
            previous: Some(1728223514),
            initial: false,
        };
        assert_eq!(what_happened(&event), None);
    }
}
