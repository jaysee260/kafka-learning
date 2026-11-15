//! Create the topics this project uses.
//!
//!     cargo run --bin create_topics
//!
//! Mirrors `shop/create_topics.py`. Brokers can auto-create topics on first
//! write, but then you get default settings (often 1 partition). Creating
//! them explicitly lets us choose the partition count, which matters for
//! stage 3.

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::types::RDKafkaErrorCode;
use shop::config::{ORDERS_DLQ_TOPIC, ORDERS_PARTITIONS, ORDERS_TOPIC, PAYMENTS_TOPIC, bootstrap_servers};

#[tokio::main]
async fn main() -> Result<(), shop::Error> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers())
        .create()?;

    // Replication 1 because we run a single broker. In production you would
    // use 3 so a broker can die without losing data.
    let topics = [
        NewTopic::new(ORDERS_TOPIC, ORDERS_PARTITIONS, TopicReplication::Fixed(1)),
        NewTopic::new(PAYMENTS_TOPIC, ORDERS_PARTITIONS, TopicReplication::Fixed(1)),
        NewTopic::new(ORDERS_DLQ_TOPIC, 1, TopicReplication::Fixed(1)),
    ];

    // One request creates all three. The outer `?` fails only if the request
    // itself failed; each topic then gets its own Ok or Err.
    let results = admin.create_topics(&topics, &AdminOptions::new()).await?;
    for result in results {
        match result {
            Ok(name) => println!("created  {name}"),
            // Running this twice is fine: "already exists" isn't an error
            // for us. Matching on the error code is how Rust says
            // `if "TOPIC_ALREADY_EXISTS" in str(exc)`.
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => println!("exists   {name}"),
            Err((name, code)) => return Err(format!("failed to create {name}: {code}").into()),
        }
    }

    // Ask the broker what it now knows about our topics.
    let metadata = admin.inner().fetch_metadata(None, Duration::from_secs(10))?;
    let mut names: Vec<&str> = topics.iter().map(|t| t.name).collect();
    names.sort();
    for name in names {
        if let Some(topic) = metadata.topics().iter().find(|t| t.name() == name) {
            println!("  {name}: {} partition(s)", topic.partitions().len());
        }
    }
    Ok(())
}
