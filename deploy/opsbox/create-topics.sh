#!/bin/bash
# Creates the home event hub topics. Runs inside the Redpanda image as the
# one-shot `hub-topics` service, so `docker compose up -d` takes care of it.
# Safe to rerun: topics that already exist are left exactly as they are.
#
# Naming: <domain>.<source>.<kind>, each with a ".dlq" partner for messages
# that could not be processed.
#
# Partitions: every event is keyed by device id, so one device's events
# always share a partition and stay in order. Three partitions on the two
# busiest topics is far more than a home needs for throughput; it is there
# so you can watch keys spread across partitions. Dead-letter topics get
# one, since order across devices doesn't matter for a pile of failures.
set -euo pipefail

BROKERS="${BROKERS:-redpanda:9092}"

#       topic              partitions   extra topic settings
TOPICS=(
  "home.ring.motion        3"
  "home.ring.contact       3"
  "home.ring.ding          1"
  "home.ring.alarm         1"
  "home.ring.battery       1"

  # A lookup table, not an event stream: one record per device, keyed by
  # device id, holding its name and model. "compact" changes what the
  # broker throws away. A normal topic drops records by age; a compacted
  # one keeps the LATEST record for every key forever and drops only the
  # older ones it replaced. Read it from the start and you get the current
  # description of every device.
  "home.ring.devices       1            cleanup.policy=compact"
)

existing="$(rpk topic list -X brokers="$BROKERS" | awk 'NR > 1 { print $1 }')"

create() {
  local topic="$1" partitions="$2" config="${3:-}"
  if grep -qxF "$topic" <<<"$existing"; then
    echo "exists   $topic"
  else
    rpk topic create "$topic" -p "$partitions" -r 1 ${config:+-c "$config"} \
      -X brokers="$BROKERS" >/dev/null
    echo "created  $topic ($partitions partitions${config:+, $config})"
  fi
}

for entry in "${TOPICS[@]}"; do
  read -r topic partitions config <<<"$entry"
  create "$topic" "$partitions" "$config"
  create "$topic.dlq" 1
done

# The bridge's own memory: the last value it forwarded for each MQTT topic,
# so it can tell a change from a repeat even after a restart. Compacted for
# the same reason as the devices topic: only the latest value per key
# matters. Not meant for consumers, hence the underscore.
create "_ring-bridge.last-forwarded" 1 "cleanup.policy=compact"

# Mining (see services/rust/home/publishers). Both are compacted, so
# the latest record per key is kept forever. That makes each topic double
# as the watcher's memory:
#   best-share   key = miner name    -> each miner's current record
#   block-found  key = block height  -> every block the pool ever solved
create "home.mining.best-share" 1 "cleanup.policy=compact"
create "home.mining.block-found" 1 "cleanup.policy=compact"
