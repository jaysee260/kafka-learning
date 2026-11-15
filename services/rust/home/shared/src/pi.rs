//! Reaching the Pi over SSH.
//!
//! Nothing is installed on the Pi for the mining services. They log in,
//! run one read-only command, and log out.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

use crate::setting;

/// Runs one command on the Pi and returns what it printed.
pub async fn on_the_pi(command: &str) -> Result<String> {
    let ssh = Command::new("ssh").args(ssh_options()).arg(login()).arg(command).output();
    let done = tokio::time::timeout(Duration::from_secs(60), ssh).await.context("the Pi took too long")??;
    if !done.status.success() {
        bail!("ssh: {}", String::from_utf8_lossy(&done.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&done.stdout).into_owned())
}

/// The folder ckpool writes its stats and log to.
pub fn ckpool_folder() -> String {
    setting("CKPOOL_LOG_DIR", "/var/log/ckpool")
}

/// The login on the Pi, like "admin@192.168.1.20".
pub fn login() -> String {
    setting("CKPOOL_SSH", "admin@raspibolt.local")
}

/// How every ssh call is made: no questions asked, one key, and only to
/// the Pi we already know, never to a stranger.
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
