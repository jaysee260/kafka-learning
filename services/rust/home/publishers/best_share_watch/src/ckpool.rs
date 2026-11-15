//! Reading ckpool's scoreboard off the Pi.
//!
//! ckpool is the solo mining pool on the Pi. Once a minute it rewrites one
//! small file per payout address with every miner's best share.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use shared::pi::{ckpool_folder, on_the_pi};

/// Miner name -> best share ever, plus "pool" for the best of everyone,
/// as it stands on the Pi right now.
pub async fn read_best_shares() -> Result<BTreeMap<String, u64>> {
    // The stats folder is root-only, and the shell has to be root too or
    // the * would not find anything.
    let stats = on_the_pi(&format!("sudo -n sh -c 'cat {}/users/*'", ckpool_folder())).await?;
    best_shares(&stats)
}

/// `stats` is the text of all the per-address files, one after another.
pub fn best_shares(stats: &str) -> Result<BTreeMap<String, u64>> {
    let mut best = BTreeMap::new();
    let mut pool_best = 0;

    for address in serde_json::Deserializer::from_str(stats).into_iter::<Value>() {
        let address = address.context("ckpool's stats were cut off mid-file")?;
        pool_best = pool_best.max(number(&address["bestever"]));

        for worker in address["worker"].as_array().into_iter().flatten() {
            // ckpool calls a miner "<payout address>.<name>".
            let full_name = worker["workername"].as_str().unwrap_or("unnamed");
            let miner = full_name.split_once('.').map_or(full_name, |(_address, name)| name);
            let best_ever = number(&worker["bestever"]);
            best.entry(miner.to_string()).and_modify(|b: &mut u64| *b = (*b).max(best_ever)).or_insert(best_ever);
        }
    }

    best.insert("pool".to_string(), pool_best);
    Ok(best)
}

fn number(value: &Value) -> u64 {
    value.as_f64().unwrap_or(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two of ckpool's per-address files, one after another, as `cat` gives them.
    const STATS: &str = r#"
    {
        "bestever": 1728223514,
        "worker": [
            {"workername": "bc1qmine.humble_nano3s", "bestshare": 1728223514.13, "bestever": 1728223514},
            {"workername": "bc1qmine.humble_gamma1", "bestshare": 302186441.53, "bestever": 302186441}
        ]
    }
    {
        "bestever": 133656151619,
        "worker": []
    }"#;

    #[test]
    fn best_share_per_miner_and_for_the_whole_pool() {
        let best = best_shares(STATS).unwrap();
        assert_eq!(best["humble_nano3s"], 1728223514);
        assert_eq!(best["humble_gamma1"], 302186441);
        assert_eq!(best["pool"], 133656151619);
        assert_eq!(best.len(), 3);
    }

    #[test]
    fn half_written_stats_are_an_error_not_a_guess() {
        assert!(best_shares(&STATS[..200]).is_err());
    }
}
