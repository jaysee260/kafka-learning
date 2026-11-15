//! Reading from and writing to Redpanda.
//!
//! Two ways of reading, because the two kinds of topic want different things:
//!
//!   new_events()   a stream: "tell me what happens from now on"
//!   table()        a table:  "give me the current value for every key"
//!
//! And one way of writing:
//!
//!   Publisher      "write this event down"

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde_json::{Value, json};
use tokio::signal::unix::{Signal, SignalKind, signal};

use crate::events::Event;
use crate::setting;

const TIMEOUT: Duration = Duration::from_secs(10);

fn brokers() -> String {
    setting("BOOTSTRAP_SERVERS", "localhost:19092")
}

/// A stream of new events. Ask it for the `next()` one, forever.
pub struct Events {
    consumer: StreamConsumer,
    /// The event handed out last, which is finished once we are asked for another.
    handed_out: Option<(String, i32, i64)>,
    asked_to_stop: Signal,
}

/// `first_run_starts_at` only matters the first time this group ever runs,
/// when it has no saved position. "latest" means start with whatever
/// happens next; "earliest" replays the topic's history. After that first
/// run the group resumes from its own saved offsets.
pub fn new_events(topics: &[&str], group: &str, first_run_starts_at: &str) -> Result<Events> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", group)
        .set("auto.offset.reset", first_run_starts_at)
        // At-least-once: an offset is saved only after the event has been
        // dealt with. The client commits in the background, but only the
        // offsets we have explicitly stored.
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()?;
    consumer.subscribe(topics)?;
    println!("[{group}] watching {}", topics.join(", "));

    // `docker stop` sends SIGTERM. Hearing it lets us leave the group cleanly.
    Ok(Events { consumer, handed_out: None, asked_to_stop: signal(SignalKind::terminate())? })
}

impl Events {
    /// The next event and when it happened (the record's Kafka timestamp,
    /// in seconds). `None` means we were asked to stop.
    pub async fn next(&mut self) -> Option<(Event, f64)> {
        loop {
            // Being asked for the next event means the last one was handled.
            if let Some((topic, partition, offset)) = self.handed_out.take() {
                if let Err(problem) = self.consumer.store_offset(&topic, partition, offset) {
                    println!("could not save my place in {topic}: {problem}");
                }
            }

            let record = tokio::select! {
                record = self.consumer.recv() => record,
                _ = self.asked_to_stop.recv() => return None,
            };
            let record = match record {
                Ok(record) => record,
                Err(problem) => {
                    println!("trouble reading from Redpanda: {problem}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            self.handed_out = Some((record.topic().to_string(), record.partition(), record.offset()));
            let happened_at = record.timestamp().to_millis().map_or_else(now, |ms| ms as f64 / 1000.0);
            match serde_json::from_slice(record.payload().unwrap_or_default()) {
                Ok(event) => return Some((event, happened_at)),
                Err(_) => println!("skipping a record that is not a mining event ({} offset {})", record.topic(), record.offset()),
            }
        }
    }
}

/// Key -> latest event, from a compacted topic.
///
/// A compacted topic keeps at least the latest record for each key. So
/// reading it from the beginning and remembering the last value per key
/// rebuilds the full table, however long this service was away.
///
/// It reads differently from new_events(): it wants the WHOLE topic from
/// offset 0, every time. So it picks its partitions itself with assign()
/// instead of joining a group with subscribe(), and never saves an offset.
pub fn table(topic: &str) -> Result<HashMap<String, Event>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        // The client insists on a group id, but with assign() and no
        // commits the group is never actually used.
        .set("group.id", format!("{topic}-reader"))
        .set("enable.auto.commit", "false")
        .create()?;

    let metadata = consumer.fetch_metadata(Some(topic), TIMEOUT)?;
    let about_topic = metadata.topics().first().context("no answer about the topic")?;
    if let Some(problem) = about_topic.error() {
        bail!("{topic}: {problem:?}");
    }

    // Start every partition at the beginning, and note where each one ends
    // right now. Only once we have read that far is the table complete.
    let mut from_the_start = TopicPartitionList::new();
    let mut ends = HashMap::new();
    for partition in about_topic.partitions() {
        from_the_start.add_partition_offset(topic, partition.id(), Offset::Beginning)?;
        let (start, end) = consumer.fetch_watermarks(topic, partition.id(), TIMEOUT)?;
        if end > start {
            ends.insert(partition.id(), end);
        }
    }
    consumer.assign(&from_the_start)?;

    let mut rows = HashMap::new();
    let give_up = Instant::now() + Duration::from_secs(30);
    while !ends.is_empty() {
        if Instant::now() > give_up {
            bail!("{topic}: could not read it to the end");
        }
        let Some(record) = consumer.poll(Duration::from_secs(1)) else { continue };
        let record = record?;

        if let Some(key) = record.key() {
            let key = String::from_utf8_lossy(key).into_owned();
            match record.payload().map(serde_json::from_slice) {
                Some(Ok(event)) => rows.insert(key, event),
                // A key with no value is a "tombstone": how a key is
                // deleted from a compacted topic.
                None => rows.remove(&key),
                // Not one of ours. Leave the row as it was.
                Some(Err(_)) => None,
            };
        }
        if record.offset() + 1 >= ends[&record.partition()] {
            ends.remove(&record.partition());
        }
    }

    println!("loaded {} rows from {topic}", rows.len());
    Ok(rows)
}

/// Writes events down.
pub struct Publisher {
    producer: FutureProducer,
}

impl Publisher {
    pub fn new() -> Result<Publisher> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers())
            // The broker drops a retried send it already has, so a network
            // hiccup cannot write the same event twice.
            .set("enable.idempotence", "true")
            .create()?;
        Ok(Publisher { producer })
    }

    /// Writes one event down, and returns only once the broker has it.
    ///
    /// Records with the same key always land in the same partition, so
    /// they stay in order.
    pub async fn publish(&self, topic: &str, key: &str, event: &Event) -> Result<()> {
        // Every event travels with an id and the time we saw it.
        let mut record = json!({ "event_id": uuid::Uuid::new_v4().to_string() });
        if let (Value::Object(record), Value::Object(event)) = (&mut record, serde_json::to_value(event)?) {
            record.extend(event);
            record.insert("observed_at".to_string(), json!(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)));
        }
        let record = record.to_string();

        self.producer
            .send(FutureRecord::to(topic).key(key).payload(&record), Timeout::After(Duration::from_secs(30)))
            .await
            .map_err(|(problem, _record)| anyhow!("{topic}: could not publish ({problem})"))?;
        println!("published to {topic}: {key} {record}");
        Ok(())
    }
}

fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.0, |since| since.as_secs_f64())
}
