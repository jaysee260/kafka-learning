# Always-on event hub on a homelab box

The repo-root `docker-compose.yml` runs everything on your laptop and only
answers on `localhost`. This folder is a second, separate deployment: one
Redpanda broker and Redpanda Console on a machine that stays on, reachable
from any other machine on your LAN. The Python services in this repo work
against either one; you choose with `BOOTSTRAP_SERVERS`.

It also carries the first real event source: Ring devices, by way of MQTT.
That part is described in [Home events](#home-events-ring-to-mqtt-to-redpanda)
below, and the learning stages don't depend on it.

## Start it (on the server)

```bash
cd deploy/opsbox
cp .env.example .env         # set REDPANDA_ADVERTISED_HOST to the server's LAN IP,
                             # and make up the three MQTT passwords
docker compose up -d
docker compose ps            # wait until redpanda shows "healthy"
```

The containers use `restart: unless-stopped`, so they come back after a
crash or a reboot. Data lives in Docker volumes (`event-hub_redpanda-data`
and friends) and survives `docker compose down`.

## Use it (from any machine on the LAN)

Replace `192.168.1.246` with your server's address.

```bash
cd services/python
source .venv/bin/activate
export BOOTSTRAP_SERVERS=192.168.1.246:19092

python -m shop.create_topics             # once; safe to rerun
python -m shop.producer                  # terminal 1
python -m shop.consumers.order_logger    # terminal 2 (export the variable there too)
```

Console is at `http://192.168.1.246:8080`.

`rpk` commands from the main README work on the server unchanged
(`docker exec -it redpanda rpk topic list`), since the container is still
named `redpanda`.

## Why the broker needs to know its own address

This trips up almost everyone the first time they move Kafka off
`localhost`, so it's worth understanding.

A Kafka client does not simply talk to the address you give it. That
address is only the **bootstrap** address, and connecting is two steps:

1. The client connects to the bootstrap address and asks for cluster
   metadata: which brokers exist and which broker leads each partition.
2. The broker replies with each broker's **advertised** address. The client
   then opens new connections to those addresses and does all its
   producing and consuming there.

With several brokers this is how a client finds the right one for each
partition. With one broker it still happens, and it has a consequence: the
address the broker advertises must be reachable *from where the client
runs*.

The laptop setup advertises `localhost:19092`. If the server did the same,
a client on your laptop would complete step 1, be told "the broker is at
localhost:19092", and then try to connect to *the laptop's own*
localhost. The symptom is a client that seems to connect and then hangs or
logs `Connection refused` for `localhost:19092`.

So this deployment has two **listeners**, each a port with its own
advertised address, one per audience:

| Listener | Listens on | Advertises | Who uses it |
| --- | --- | --- | --- |
| `internal` | `0.0.0.0:9092` (container network only) | `redpanda:9092` | Console and any other container in this compose project |
| `external` | `0.0.0.0:19092` (published on the host) | `<REDPANDA_ADVERTISED_HOST>:19092` | your code, on the server or any LAN machine |

Containers reach the broker by its container name, which only resolves
inside Docker's network. Everything else reaches it by the host's LAN
address. The broker decides which advertised address to hand back based on
which listener the client came in on.

One side effect: on the server itself, use the LAN address too
(`BOOTSTRAP_SERVERS=192.168.1.246:19092`), not `localhost:19092`.
Localhost would get you through step 1, and step 2 would then go to the
LAN address anyway.

You can see the advertised address a client gets back:

```bash
docker exec redpanda rpk cluster info -X brokers=192.168.1.246:19092
```

## Home events: Ring to MQTT to Redpanda

```
Ring cloud ──> ring-mqtt ──> Mosquitto ──> ring-bridge ──> Redpanda
               (adapter)     (MQTT)        (Redpanda       home.ring.*
                                            Connect)
```

| Service | What it does |
| --- | --- |
| `ring-mqtt` | [ring-mqtt](https://github.com/tsightler/ring-mqtt) holds a connection to Ring's cloud and republishes device events as MQTT messages under `ring/#`. |
| `mosquitto` | The MQTT broker. Passwords required; `mosquitto/acl` says who can read and write what. |
| `ring-bridge` | A Redpanda Connect pipeline (`connect/ring-bridge.yaml`) that subscribes to the Ring MQTT topics and produces to Redpanda. |
| `hub-topics` | Runs once on `docker compose up`, creates the topics below (`create-topics.sh`), and exits. |

### Why two brokers

MQTT and Kafka solve different halves of the problem. MQTT is what
devices speak: tiny messages pushed to whoever is subscribed right now,
with no history. Kafka is a log: every event is kept, in order, and any
number of consumers can read it at their own pace or replay it later. So
devices publish to Mosquitto, and the bridge copies what matters into
Redpanda, where you can build on it.

### One-time Ring sign-in

ring-mqtt needs to be linked to your Ring account once. Until it is, it
serves a sign-in page and publishes nothing.

1. Open `http://192.168.1.246:55123` from a machine on the LAN.
2. Enter your Ring email and password, then the 2FA code Ring sends you.
3. ring-mqtt saves a token, connects, and shuts the sign-in page down.

Check that it worked:

```bash
docker logs ring-mqtt --tail 30      # should list your locations and devices
```

The sign-in page is plain HTTP, so your password crosses the LAN
unencrypted. If you'd rather it didn't, run the sign-in over SSH on the
server instead:

```bash
docker compose stop ring-mqtt
docker compose run --rm --entrypoint /app/ring-mqtt/init-ring-mqtt.js ring-mqtt
docker compose start ring-mqtt
```

### Topics

Names follow `<domain>.<source>.<kind>`. Every record's key is the Ring
device id, so one device's events always land in the same partition and
stay in order.

| Topic | Partitions | Carries | From MQTT |
| --- | --- | --- | --- |
| `home.ring.motion` | 3 | motion started / stopped | `.../motion/state` |
| `home.ring.contact` | 3 | door, window and tilt sensors opened / closed | `.../contact/state`, `.../zone/state` |
| `home.ring.ding` | 1 | doorbell pressed | `.../ding/state` |
| `home.ring.alarm` | 1 | alarm mode, siren, smoke, CO, flood, freeze | `.../alarm/state` and similar |
| `home.ring.battery` | 1 | battery level and status | `.../info/state` |
| `home.ring.devices` | 1, compacted | one record per device: name, model, category | `homeassistant/sensor/.../<device>_info/config` |

Each one has a `.dlq` partner with one partition (`home.ring.motion.dlq`
and so on). When the bridge can't turn an MQTT message into an event, it
writes the original payload there with a `bridge_error` header saying why,
and carries on. Nothing is silently dropped and one bad message doesn't
block the rest.

An event looks like this:

```json
{
  "event_id": "39e9aca9-8ceb-4e1d-b445-f9c83ecacc8b",
  "source": "ring",
  "kind": "motion",
  "device_id": "<ring device id>",
  "location_id": "<ring location id>",
  "category": "camera",
  "entity": "motion",
  "state": "ON",
  "initial": false,
  "observed_at": "2026-11-15T20:09:08.913610645Z"
}
```

Battery events have `battery_level` and `battery_status` instead of
`state`. The MQTT topic the event came from is on the record as a
`mqtt_topic` header.

### State in, events out

ring-mqtt was built for Home Assistant, so it publishes *state*, and
repeats it freely: everything is resent six times in the three minutes
after it starts, and device info every five minutes after that. Hearing
"the door is closed" again is harmless to a dashboard, but it is noise in
an event log.

So the bridge only forwards changes. It remembers the last value it
forwarded for each MQTT topic and drops a message that matches it. Two
consequences worth knowing:

- The very first record for each device is a *baseline*: how things stood
  when the bridge first saw the device, not something that just happened.
  Those records carry `"initial": true`; every later one carries
  `"initial": false`. A consumer that only cares about things happening
  skips the initial ones.
- Motion and dings come as pairs: `ON` when it starts, `OFF` when Ring
  considers it over.

The bridge's memory has to outlive the bridge, or every restart would
produce a fresh round of baselines and mislabel the next real change as
one. So it is kept in memory and also logged to a compacted topic,
`_ring-bridge.last-forwarded` (key: MQTT topic, value: last forwarded
value). After a restart the bridge reads it back. Kafka Streams keeps its
state the same way, and calls such a topic a changelog. The underscore
marks it as plumbing rather than a topic for consumers.

### Device names: a lookup topic

Events carry a device id, not a name. Names live in `home.ring.devices`:

```json
{
  "device_id": "5c475ecce0b8",
  "source": "ring",
  "name": "Front Door",
  "model": "Doorbell Gen 2",
  "manufacturer": "Ring",
  "category": "camera",
  "location_id": "<ring location id>",
  "updated_at": "2026-11-15T20:18:53.830764344Z"
}
```

This topic is **compacted**. A normal topic is a history: records are
kept for a while and then dropped by age. A compacted topic is a table:
the broker keeps the latest record for each key indefinitely and discards
only the older ones it replaced. Rename a device in the Ring app, restart
ring-mqtt, and a new record with the same key supersedes the old one.

A consumer that wants names reads `home.ring.devices` from the beginning
into a dictionary keyed by device id, then looks each event's `device_id`
up in it. That is a stream-table join, and it is why both sides use the
same key.

```bash
docker exec redpanda rpk topic consume home.ring.devices -o start -f '%k  %v\n'
```

Two devices can share a name (a doorbell and a door sensor both called
"Front Door"); `category` tells them apart.

### Watching it

In Console (`http://192.168.1.246:8080`), open a `home.ring.*` topic. Or
from a terminal on the server:

```bash
# The Kafka side: what the bridge produced
docker exec redpanda rpk topic consume home.ring.motion -o start

# The MQTT side: everything ring-mqtt publishes (Ctrl+C to stop)
docker exec mosquitto sh -c \
  'mosquitto_sub -u viewer -P "$MQTT_VIEWER_PASSWORD" -t "ring/#" -v -T "ring/+/+/+/snapshot/image"'

docker logs -f ring-bridge           # bridge errors show up here
```

From another machine, any MQTT client can connect to `192.168.1.246:1883`
as `viewer` with the `MQTT_VIEWER_PASSWORD` from `.env`.

### Changing the bridge

`connect/ring-bridge.yaml` is the whole pipeline: which MQTT topics it
subscribes to, how a topic maps to a kind, the shape of the record, and
the dropping of repeats.
The file next to it holds unit tests that run without any broker:

```bash
docker compose run --rm ring-bridge test /connect/ring-bridge.yaml
docker compose restart ring-bridge   # pick up an edited pipeline
```

To add a topic, add it to `create-topics.sh` and run
`docker compose up -d hub-topics`.

### Delivery guarantees, briefly

- **MQTT to Kafka is at-least-once.** The bridge acknowledges an MQTT
  message only after Redpanda has accepted it. If it crashes in between,
  Mosquitto delivers the message again and you get a duplicate with a new
  `event_id`. Consumers that care must cope with that.
- **A bridge restart doesn't lose events.** It connects with a fixed
  client id and a persistent session, so Mosquitto queues events for it
  while it is away (up to 1000 by default).
- **Device records need ring-mqtt to announce them.** It does so when it
  starts, so `docker compose restart ring-mqtt` refreshes
  `home.ring.devices` after you add or rename a device.
- **A ring-mqtt outage does lose events.** If ring-mqtt or the internet
  connection is down, Ring's cloud does not replay what you missed.

## Things to know

- **No authentication on Redpanda or Console.** Anyone on your LAN can
  read and write topics and use Console, which now includes reading when
  your doors open. That's fine for a home network; don't port-forward
  these ports or put them on a network you don't trust. The ports are
  published on IPv4 only, so a public IPv6 address on the host doesn't
  expose them.
- **MQTT does have passwords, for a reason.** ring-mqtt acts on commands
  published to its MQTT topics (arm, disarm, unlock), so write access to
  `ring/#` is control of the house. Only the `ring-mqtt` account has it.
  The bridge is one-way, so nothing written to Redpanda can reach Ring.
- **The Ring token is a credential.** After sign-in it lives in the
  `event-hub_ring-mqtt-data` volume and gives full access to the Ring
  account without a password or 2FA. Treat backups of that volume
  accordingly.
- **Dev-container mode.** The broker skips fsync, so an unclean power loss
  can lose the last few seconds of writes. Before anything you care about
  depends on this broker, drop `--mode dev-container` and read the
  [production settings](https://docs.redpanda.com/current/deploy/redpanda/manual/production/).
- **Memory.** The broker is told to use 1 GB (`--memory 1G`) with a 1.5 GB
  container limit; Console and ring-mqtt are limited to 512 MB each, the
  bridge to 256 MB and Mosquitto to 128 MB. In practice the three new
  services use about 120 MB between them.
- **Disk.** Topics keep data for 7 days by default with no size cap. That
  is tiny for this project's traffic. Set `retention.bytes` on a topic
  before pointing anything high-volume at it.
- **The advertised address must not change.** Give the server a static IP
  or a DHCP reservation. If it changes, update `.env` and run
  `docker compose up -d`.

## Stopping and resetting

```bash
docker compose down        # stop; topics, offsets and the Ring sign-in are kept
docker compose down -v     # stop and DELETE all topics and offsets, AND the Ring
                           # sign-in (you would have to sign in again)

docker compose stop ring-mqtt mosquitto ring-bridge   # pause just the Ring side
```
