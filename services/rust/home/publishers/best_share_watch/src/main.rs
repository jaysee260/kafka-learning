//! Publishes an event when a miner beats its own best share.
//!
//! ckpool, the mining pool on the Pi, keeps a scoreboard: every miner's
//! best share so far. We look at it every ten minutes and write down only
//! what went up. The best_share_alerts consumer turns that into a phone
//! alert.
//!
//! The plumbing is in ckpool.rs next to this file (reading the scoreboard)
//! and in shared/ (the Pi and Redpanda).

use std::time::Duration;

use anyhow::Result;
use shared::events::Event;
use shared::kafka_io::{self, Publisher};
use shared::setting;

mod ckpool;

const TEN_MINUTES: Duration = Duration::from_secs(10 * 60);

/// The event to publish, or None if this is not a new record.
fn new_best_share(miner: &str, difficulty: u64, last_reported: Option<&Event>) -> Option<Event> {
    let previous = match last_reported {
        Some(Event::BestShare { difficulty, .. }) => Some(*difficulty),
        _ => None,
    };
    if difficulty <= previous.unwrap_or(0) {
        return None;
    }

    Some(Event::BestShare {
        source: "ckpool".to_string(),
        miner: miner.to_string(),
        difficulty,
        previous,
        // The first reading for a miner is "how things stood", not news.
        initial: previous.is_none(),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = setting("TOPIC", "home.mining.best-share"); // one row per miner, plus "pool"
    let hub = Publisher::new()?;

    // The topic keeps the latest event per miner forever. Reading it back
    // is how this service remembers what it has already reported.
    let mut best_reported = kafka_io::table(&topic)?;

    let mut every_ten_minutes = tokio::time::interval(TEN_MINUTES);
    loop {
        every_ten_minutes.tick().await;

        let best_shares = match ckpool::read_best_shares().await {
            Ok(best_shares) => best_shares,
            Err(problem) => {
                println!("could not read the Pi ({problem:#}), trying again in ten minutes");
                continue;
            }
        };

        for (miner, difficulty) in best_shares {
            if let Some(event) = new_best_share(&miner, difficulty, best_reported.get(&miner)) {
                hub.publish(&topic, &miner, &event).await?;
                best_reported.insert(miner, event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reported(difficulty: u64) -> Event {
        new_best_share("humble_duo1", difficulty, None).unwrap()
    }

    #[test]
    fn first_reading_is_a_baseline() {
        let event = new_best_share("humble_duo1", 486149527, None).unwrap();
        assert!(matches!(event, Event::BestShare { initial: true, previous: None, .. }));
    }

    #[test]
    fn beating_the_record_is_news() {
        let event = new_best_share("humble_duo1", 500000000, Some(&reported(486149527))).unwrap();
        assert!(matches!(
            event,
            Event::BestShare { initial: false, difficulty: 500000000, previous: Some(486149527), .. }
        ));
    }

    #[test]
    fn same_or_lower_is_not_news() {
        assert_eq!(new_best_share("humble_duo1", 486149527, Some(&reported(486149527))), None);
        assert_eq!(new_best_share("humble_duo1", 1, Some(&reported(486149527))), None);
    }

    #[test]
    fn a_miner_with_no_shares_yet_is_not_news() {
        assert_eq!(new_best_share("new_miner", 0, None), None);
    }
}
