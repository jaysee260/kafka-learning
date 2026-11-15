//! Watches the mining pool on the Pi and writes down what is news.
//!
//! Two things are news, and each has its own way of reaching us:
//!
//!   a new best share   we look at ckpool's scoreboard every ten minutes
//!   a block we found   Bitcoin Core announces every new block the moment it
//!                      arrives, and we check whether its reward pays us
//!
//! Each one becomes an event in Redpanda. mining_alerts.rs turns those
//! events into phone alerts.
//!
//! The plumbing is in ckpool.rs (reading the Pi), bitcoin_core.rs (block
//! announcements) and kafka_io.rs (Redpanda).

use std::time::Duration;

use anyhow::Result;
use home::bitcoin_core::{self, PayoutAddress};
use home::events::Event;
use home::kafka_io::{self, Publisher};
use home::{ckpool, setting};

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
    let best_shares = setting("BEST_SHARES_TOPIC", "home.mining.best-share"); // one row per miner, plus "pool"
    let blocks_found = setting("BLOCKS_FOUND_TOPIC", "home.mining.block-found"); // one row per block height
    let our_address = PayoutAddress::parse(&setting("POOL_ADDRESS", ""))?;
    let hub = Publisher::new()?;

    // Both topics keep the latest event per key forever. Reading them back
    // is how this service remembers what it has already reported.
    let mut best_reported = kafka_io::table(&best_shares)?;
    let mut blocks_reported = kafka_io::table(&blocks_found)?;

    let mut new_blocks = bitcoin_core::new_blocks();
    let mut every_ten_minutes = tokio::time::interval(TEN_MINUTES);

    loop {
        tokio::select! {
            Some(block) = new_blocks.recv() => {
                let height = block.height.to_string();
                if !block.pays(&our_address) {
                    println!("block {height} is somebody else's");
                } else if !blocks_reported.contains_key(&height) {
                    let miner = ckpool::who_solved(block.height).await;
                    let event = Event::BlockFound { source: "bitcoin-core".to_string(), height: block.height, miner };
                    hub.publish(&blocks_found, &height, &event).await?;
                    blocks_reported.insert(height, event);
                }
            }

            _ = every_ten_minutes.tick() => {
                let scoreboard = match ckpool::read().await {
                    Ok(scoreboard) => scoreboard,
                    Err(problem) => {
                        println!("could not read the Pi ({problem:#}), trying again in ten minutes");
                        continue;
                    }
                };

                for (miner, difficulty) in scoreboard.best_shares {
                    if let Some(event) = new_best_share(&miner, difficulty, best_reported.get(&miner)) {
                        hub.publish(&best_shares, &miner, &event).await?;
                        best_reported.insert(miner, event);
                    }
                }

                // The safety net. A block announcement is not repeated, so
                // one made while we were not listening would be lost.
                // ckpool's log still has it.
                for block in scoreboard.blocks {
                    let height = block.height.to_string();
                    if !blocks_reported.contains_key(&height) {
                        let event = Event::BlockFound { source: "ckpool".to_string(), height: block.height, miner: Some(block.miner) };
                        hub.publish(&blocks_found, &height, &event).await?;
                        blocks_reported.insert(height, event);
                    }
                }
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
