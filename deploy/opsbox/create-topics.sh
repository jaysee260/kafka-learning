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

#       topic              partitions
TOPICS=(
  "home.ring.motion        3"
  "home.ring.contact       3"
  "home.ring.ding          1"
  "home.ring.alarm         1"
  "home.ring.battery       1"
)

existing="$(rpk topic list -X brokers="$BROKERS" | awk 'NR > 1 { print $1 }')"

create() {
  local topic="$1" partitions="$2"
  if grep -qxF "$topic" <<<"$existing"; then
    echo "exists   $topic"
  else
    rpk topic create "$topic" -p "$partitions" -r 1 -X brokers="$BROKERS" >/dev/null
    echo "created  $topic ($partitions partitions)"
  fi
}

for entry in "${TOPICS[@]}"; do
  read -r topic partitions <<<"$entry"
  create "$topic" "$partitions"
  create "$topic.dlq" 1
done
