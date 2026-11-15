//! Publishes an event when our pool finds a block.
//!
//! Bitcoin Core announces every new block the moment it arrives. For each
//! one we check whether its reward pays our address. If it does, the block
//! is ours. The block_alerts consumer turns that into a phone alert.
//!
//! The plumbing is next to this file in bitcoin_core.rs (the block
//! announcements) and ckpool_log.rs (ckpool's log), and in shared/.

use std::time::Duration;

use anyhow::Result;
use shared::events::Event;
use shared::kafka_io::{self, Publisher};
use shared::setting;

mod bitcoin_core;
mod ckpool_log;

use bitcoin_core::PayoutAddress;

const TEN_MINUTES: Duration = Duration::from_secs(10 * 60);

fn block_found(source: &str, height: u64, miner: Option<String>) -> Event {
    Event::BlockFound { source: source.to_string(), height, miner }
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = setting("TOPIC", "home.mining.block-found"); // one row per block height
    let our_address = PayoutAddress::parse(&setting("POOL_ADDRESS", ""))?;
    let hub = Publisher::new()?;

    // The topic keeps every block we ever reported. Reading it back is how
    // this service remembers them, so no block is reported twice.
    let mut already_reported = kafka_io::table(&topic)?;

    let mut new_blocks = bitcoin_core::new_blocks();
    let mut every_ten_minutes = tokio::time::interval(TEN_MINUTES);

    loop {
        let found = tokio::select! {
            // The fast way: Bitcoin Core just announced a block.
            Some(block) = new_blocks.recv() => {
                if block.pays(&our_address) {
                    let miner = ckpool_log::who_solved(block.height).await;
                    vec![block_found("bitcoin-core", block.height, miner)]
                } else {
                    println!("block {} is somebody else's", block.height);
                    vec![]
                }
            }

            // The safety net. An announcement is not repeated, so one made
            // while we were not listening would be lost. ckpool's log
            // still has it.
            _ = every_ten_minutes.tick() => match ckpool_log::solved_blocks().await {
                Ok(solved) => solved.into_iter().map(|b| block_found("ckpool", b.height, Some(b.miner))).collect(),
                Err(problem) => {
                    println!("could not read the Pi ({problem:#}), trying again in ten minutes");
                    vec![]
                }
            },
        };

        for event in found {
            let Event::BlockFound { height, .. } = &event else { continue };
            let height = height.to_string();
            if !already_reported.contains_key(&height) {
                hub.publish(&topic, &height, &event).await?;
                already_reported.insert(height, event);
            }
        }
    }
}
