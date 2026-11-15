//! Sends a phone alert when a miner sets a new best share, or the pool
//! finds a block.
//!
//! The events come from mining_watch.rs. The plumbing is in kafka_io.rs
//! (reading from Redpanda) and ntfy.rs (reaching your phone).

use std::time::Duration;

use anyhow::Result;
use home::ckpool::short;
use home::events::Event;
use home::kafka_io::new_events;
use home::ntfy::{Alert, send_to_phone};
use home::setting;

/// A miner that just joined beats its own record every few minutes at first.
const WORTH_A_BUZZ: u64 = 1_000_000;
const ONE_DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// What to tell the phone about one mining event, or None to stay quiet.
fn what_happened(event: &Event) -> Option<Alert> {
    match event {
        // The first reading for a miner is "how things stood", not news.
        Event::BestShare { initial: true, .. } => None,
        Event::BestShare { difficulty, .. } if *difficulty < WORTH_A_BUZZ => None,

        Event::BestShare { miner, difficulty, previous, .. } => Some(Alert {
            title: if miner == "pool" { "New pool record".to_string() } else { format!("New best share: {miner}") },
            message: format!("{}, up from {}", short(*difficulty), short(previous.unwrap_or(0))),
            urgent: false,
        }),

        Event::BlockFound { height, miner, .. } => Some(Alert {
            title: "BLOCK FOUND".to_string(),
            message: match miner {
                Some(miner) => format!("{miner} solved block {height}"),
                None => format!("Block {height} pays your address"),
            },
            urgent: true,
        }),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let topics = setting("EVENT_TOPICS", "home.mining.best-share,home.mining.block-found");
    let topics: Vec<&str> = topics.split(',').collect();
    let group = setting("GROUP", "mining-alerts");

    // "earliest": even on its very first run, never skip a block.
    let mut events = new_events(&topics, &group, "earliest")?;

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

    fn best_share(miner: &str, difficulty: u64, previous: Option<u64>, initial: bool) -> Event {
        Event::BestShare { source: "ckpool".to_string(), miner: miner.to_string(), difficulty, previous, initial }
    }

    fn said(event: &Event) -> (String, String) {
        let alert = what_happened(event).unwrap();
        (alert.title, alert.message)
    }

    #[test]
    fn new_best_share_names_the_miner() {
        let event = best_share("humble_nano3s", 2100000000, Some(1728223514), false);
        assert_eq!(said(&event), ("New best share: humble_nano3s".to_string(), "2.10G, up from 1.73G".to_string()));
    }

    #[test]
    fn pool_record_gets_its_own_title() {
        let event = best_share("pool", 140000000000, Some(133656151619), false);
        assert_eq!(said(&event), ("New pool record".to_string(), "140.00G, up from 133.66G".to_string()));
    }

    #[test]
    fn baselines_and_small_numbers_stay_quiet() {
        assert_eq!(what_happened(&best_share("humble_nano3s", 2100000000, None, true)), None);
        assert_eq!(what_happened(&best_share("new_miner", 4096, Some(512), false)), None);
    }

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
    fn events_read_the_same_as_they_are_written() {
        let on_the_wire = r#"{"event_id": "x", "source": "ckpool", "kind": "best-share", "miner": "pool",
                              "difficulty": 133656151619, "previous": null, "initial": true, "observed_at": "now"}"#;
        let event: Event = serde_json::from_str(on_the_wire).unwrap();
        assert_eq!(event, best_share("pool", 133656151619, None, true));
        assert!(serde_json::from_str::<Event>(r#"{"kind": "contact", "state": "ON"}"#).is_err());
    }
}
