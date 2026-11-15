//! What ckpool's log on the Pi says about blocks.
//!
//! When the pool solves a block, ckpool adds a line to its log naming the
//! miner. Bitcoin Core tells us faster, but only ckpool knows who it was,
//! and its log is still there if we were not listening at the time.

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Result;
use regex::Regex;
use shared::pi::{ckpool_folder, on_the_pi};

#[derive(Debug, PartialEq)]
pub struct SolvedBlock {
    pub height: u64,
    pub miner: String,
}

/// Every block the log says this pool has solved.
pub async fn solved_blocks() -> Result<Vec<SolvedBlock>> {
    // grep exits 1 when it finds nothing, which is the normal case here.
    let lines = format!("grep -a 'Solved and confirmed block' {}/ckpool.log || true", ckpool_folder());
    Ok(blocks_found(&on_the_pi(&lines).await?))
}

/// The miner ckpool credits with a block, once ckpool has logged it.
pub async fn who_solved(height: u64) -> Option<String> {
    // ckpool writes its log line a moment after Bitcoin Core announces the block.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let solved = solved_blocks().await.ok()?;
    solved.into_iter().find(|block| block.height == height).map(|block| block.miner)
}

pub fn blocks_found(log: &str) -> Vec<SolvedBlock> {
    static SOLVED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"Solved and confirmed block (?<height>\d+) by (?<worker>\S+)").unwrap());

    SOLVED
        .captures_iter(log)
        .filter_map(|found| {
            // ckpool calls a miner "<payout address>.<name>".
            let worker = &found["worker"];
            let miner = worker.split_once('.').map_or(worker, |(_address, name)| name);
            Some(SolvedBlock { height: found["height"].parse().ok()?, miner: miner.to_string() })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOG: &str = "
[2026-11-15 21:11:55.307] Block hash changed to 00000000000000000000bd80
[2026-11-16 03:02:01.120] Possible block solve diff 160000000000000.000000 !
[2026-11-16 03:02:01.480] BLOCK ACCEPTED!
[2026-11-16 03:02:01.481] Solved and confirmed block 950123 by bc1qmine.humble_nano3s
";

    #[test]
    fn solved_blocks_come_from_the_log() {
        assert_eq!(blocks_found(LOG), vec![SolvedBlock { height: 950123, miner: "humble_nano3s".to_string() }]);
        assert_eq!(blocks_found(""), vec![]);
    }
}
