//! Reading ckpool's scoreboard off the Pi, over SSH.
//!
//! ckpool is the solo mining pool on the Pi. Once a minute it rewrites one
//! small file per payout address with every miner's best share, and it adds
//! a line to its log when the pool solves a block. Nothing is installed on
//! the Pi for this: we log in, read those, and log out.

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::Value;
use tokio::process::Command;

use shared::setting;

pub struct Scoreboard {
    /// Miner name -> best share ever, plus "pool" for the best of everyone.
    pub best_shares: BTreeMap<String, u64>,
    /// Every block ckpool's log says this pool solved.
    pub blocks: Vec<SolvedBlock>,
}

#[derive(Debug, PartialEq)]
pub struct SolvedBlock {
    pub height: u64,
    pub miner: String,
}

/// Logs in to the Pi and reads the scoreboard as it stands right now.
pub async fn read() -> Result<Scoreboard> {
    let folder = setting("CKPOOL_LOG_DIR", "/var/log/ckpool");
    // The stats folder is root-only, and the shell has to be root too or
    // the * would not find anything.
    let stats = on_the_pi(&format!("sudo -n sh -c 'cat {folder}/users/*'")).await?;
    // grep exits 1 when it finds nothing, which is the normal case here.
    let log = on_the_pi(&format!("grep -a 'Solved and confirmed block' {folder}/ckpool.log || true")).await?;
    Ok(Scoreboard { best_shares: best_shares(&stats)?, blocks: blocks_found(&log) })
}

/// The miner ckpool credits with a block, once ckpool has logged it.
pub async fn who_solved(height: u64) -> Option<String> {
    // ckpool writes its log line a moment after Bitcoin Core announces the block.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let scoreboard = read().await.ok()?;
    scoreboard.blocks.into_iter().find(|block| block.height == height).map(|block| block.miner)
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

/// The blocks this pool has solved, from ckpool's own log lines.
pub fn blocks_found(log: &str) -> Vec<SolvedBlock> {
    static SOLVED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"Solved and confirmed block (?<height>\d+) by (?<worker>\S+)").unwrap());

    SOLVED
        .captures_iter(log)
        .filter_map(|found| {
            let worker = &found["worker"];
            let miner = worker.split_once('.').map_or(worker, |(_address, name)| name);
            Some(SolvedBlock { height: found["height"].parse().ok()?, miner: miner.to_string() })
        })
        .collect()
}

/// Runs one command on the Pi and returns what it printed.
async fn on_the_pi(command: &str) -> Result<String> {
    let ssh = Command::new("ssh").args(ssh_options()).arg(pi()).arg(command).output();
    let done = tokio::time::timeout(Duration::from_secs(60), ssh).await.context("the Pi took too long")??;
    if !done.status.success() {
        bail!("ssh: {}", String::from_utf8_lossy(&done.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&done.stdout).into_owned())
}

/// The login on the Pi, like "admin@192.168.1.20".
pub fn pi() -> String {
    setting("CKPOOL_SSH", "admin@raspibolt.local")
}

/// How every ssh call here is made: no questions asked, one key, and only
/// to the Pi we already know, never to a stranger.
pub fn ssh_options() -> Vec<String> {
    let mut options =
        vec!["-o".to_string(), "BatchMode=yes".to_string(), "-o".to_string(), "ConnectTimeout=10".to_string()];
    let key = setting("CKPOOL_SSH_KEY", "");
    if !key.is_empty() {
        options.extend(["-i".to_string(), key, "-o".to_string(), "IdentitiesOnly=yes".to_string()]);
    }
    let known_hosts = setting("CKPOOL_KNOWN_HOSTS", "");
    if !known_hosts.is_empty() {
        options.extend(["-o".to_string(), format!("UserKnownHostsFile={known_hosts}")]);
        options.extend(["-o".to_string(), "StrictHostKeyChecking=yes".to_string()]);
    }
    options
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

    const LOG: &str = "
[2026-11-15 21:11:55.307] Block hash changed to 00000000000000000000bd80
[2026-11-16 03:02:01.120] Possible block solve diff 160000000000000.000000 !
[2026-11-16 03:02:01.480] BLOCK ACCEPTED!
[2026-11-16 03:02:01.481] Solved and confirmed block 950123 by bc1qmine.humble_nano3s
";

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

    #[test]
    fn solved_blocks_come_from_the_log() {
        assert_eq!(blocks_found(LOG), vec![SolvedBlock { height: 950123, miner: "humble_nano3s".to_string() }]);
        assert_eq!(blocks_found(""), vec![]);
    }
}
