//! The mining events, as they travel through Redpanda.

use serde::{Deserialize, Serialize};

/// One record on a `home.mining.*` topic. On the wire it is JSON with a
/// `kind` field saying which of the two it is:
///
///   {"kind": "best-share", "miner": "humble_nano3s", "difficulty": 1728223514, ...}
///   {"kind": "block-found", "height": 950123, ...}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Event {
    BestShare {
        source: String,
        /// A miner's name, or "pool" for the best of everyone.
        miner: String,
        difficulty: u64,
        previous: Option<u64>,
        /// The first reading for a miner is "how things stood", not news.
        initial: bool,
    },
    BlockFound {
        /// Who noticed: "bitcoin-core" (its block announcement) or "ckpool" (its log).
        source: String,
        height: u64,
        /// Only ckpool knows which miner it was, so this can be missing.
        miner: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_read_the_same_as_they_are_written() {
        let on_the_wire = r#"{"event_id": "x", "source": "ckpool", "kind": "best-share", "miner": "pool",
                              "difficulty": 133656151619, "previous": null, "initial": true, "observed_at": "now"}"#;
        let event: Event = serde_json::from_str(on_the_wire).unwrap();
        let expected = Event::BestShare {
            source: "ckpool".to_string(),
            miner: "pool".to_string(),
            difficulty: 133656151619,
            previous: None,
            initial: true,
        };
        assert_eq!(event, expected);
        assert!(serde_json::from_str::<Event>(r#"{"kind": "contact", "state": "ON"}"#).is_err());
    }
}
