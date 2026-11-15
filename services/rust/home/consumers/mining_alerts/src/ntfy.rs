//! Reaching your phone, by way of the ntfy server in the hub.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use shared::setting;

/// What the phone shows.
#[derive(Debug, PartialEq)]
pub struct Alert {
    pub title: String,
    pub message: String,
    /// Asks the phone to make more noise than usual.
    pub urgent: bool,
}

/// Keeps trying until ntfy takes the alert, or it is too old to matter.
///
/// Kafka can promise we see every event. It cannot promise ntfy is up. If
/// we gave up after one failed try, an ntfy restart would swallow alerts
/// silently. If we retried forever, we would sit on one old event while
/// newer ones queue up.
///
/// So: retry, but only while the event is still news. The same rule covers
/// this service having been down: when it resumes, events older than
/// `still_news_for` are dropped instead of sent late.
pub async fn send_to_phone(alert: &Alert, happened_at: f64, still_news_for: Duration) {
    let mut wait = Duration::from_secs(1);
    loop {
        let age = now() - happened_at;
        if age > still_news_for.as_secs_f64() {
            println!("dropped, {age:.0}s old: {}: {}", alert.title, alert.message);
            return;
        }
        match post(alert).await {
            Ok(()) => {
                println!("sent: {}: {}", alert.title, alert.message);
                return;
            }
            // Covers both "ntfy is not answering" and "ntfy said no", such
            // as a wrong password.
            Err(problem) => {
                println!("ntfy problem ({problem}), trying again in {}s", wait.as_secs());
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(15));
            }
        }
    }
}

async fn post(alert: &Alert) -> Result<(), reqwest::Error> {
    reqwest::Client::new()
        .post(setting("NTFY_URL", "http://localhost:2586"))
        .basic_auth(setting("NTFY_USER", "hub-publisher"), Some(setting("NTFY_PASSWORD", "")))
        .timeout(Duration::from_secs(10))
        .json(&json!({
            "topic": setting("NTFY_TOPIC", "home-alerts"),
            "title": alert.title,
            "message": alert.message,
            // ntfy priorities run from 1 to 5. 3 is a normal buzz, 5 keeps buzzing.
            "priority": if alert.urgent { 5 } else { 3 },
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.0, |since| since.as_secs_f64())
}
