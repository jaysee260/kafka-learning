# Always-on event hub on a homelab box

The repo-root `docker-compose.yml` runs everything on your laptop and only
answers on `localhost`. This folder is a second, separate deployment: one
Redpanda broker and Redpanda Console on a machine that stays on, reachable
from any other machine on your LAN. The Python services in this repo work
against either one; you choose with `BOOTSTRAP_SERVERS`.

It also carries the first real event source: Ring devices, by way of MQTT.
That part is described in [Home events](#home-events-ring-to-mqtt-to-redpanda)
below, and the learning stages don't depend on it. The last piece is
[ntfy](#phone-alerts-ntfy), which turns an HTTP request into a push
notification on your phone.

## Start it (on the server)

```bash
cd deploy/opsbox
cp .env.example .env         # set REDPANDA_ADVERTISED_HOST to the server's LAN IP,
                             # and make up the MQTT and ntfy passwords
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

## Phone alerts: ntfy

[ntfy](https://ntfy.sh) is a small server that turns an HTTP POST into a
push notification. It is the last hop of a pipeline that doesn't exist yet:

```
home.ring.contact / home.ring.ding  ->  a consumer you write  ->  ntfy  ->  your phone
```

The consumer reads events from Redpanda, decides which ones deserve an
alert, and posts a line of text to ntfy. Nothing publishes to ntfy yet
except the test script below.

- Address: `http://192.168.1.246:2586` on the LAN (web page and API).
  This deployment is also published at `https://ntfy.humblehome.net`; see
  [Reaching it from outside](#reaching-it-from-outside).
- Topic: `home-alerts`. ntfy topics are created by using them; the
  accounts below are allowed any topic starting with `home-`.
- Messages are kept for 72 hours, so a phone that was out of reach
  catches up when it reconnects.

### Accounts

Everything is denied unless an account is granted it. There are two
accounts, created from `.env` by `ntfy/create-users.sh` every time you run
`docker compose up -d`:

| Account | Password in `.env` | May do |
|---|---|---|
| `hub-publisher` | `NTFY_PUBLISHER_PASSWORD` | publish to `home-*`, cannot read |
| `phone` | `NTFY_PHONE_PASSWORD` | read `home-*`, cannot publish |

To change a password, edit `.env` and run `docker compose up -d`.

### Subscribe on your phone

1. Install the ntfy app ([Android](https://play.google.com/store/apps/details?id=io.heckel.ntfy),
   [F-Droid](https://f-droid.org/en/packages/io.heckel.ntfy/),
   [iOS](https://apps.apple.com/us/app/ntfy/id1625396347)).
2. Tap **+**, enter the topic `home-alerts`, turn on **Use another
   server** and enter the server address (see below for which one).
3. The app asks for a login: user `phone`, password is
   `NTFY_PHONE_PASSWORD` from `.env`.
4. On Android, allow the app to ignore battery optimization when it asks.
   Otherwise Android may put it to sleep and alerts arrive late.

**Which address.** The phone only gets alerts while it can reach the
server:

- `http://192.168.1.246:2586` works on your home Wi-Fi only. Alerts sent
  while you are out arrive when you get home.
- A public HTTPS address (here `https://ntfy.humblehome.net`) works
  anywhere, with nothing extra on the phone. Use this one if you have it.
- Without a public address, the server's [Tailscale](https://tailscale.com)
  address works anywhere as long as Tailscale is connected on the phone.

**Android and iOS differ.** The Android app keeps its own connection open
to the server, so alerts are instant with no outside service involved. iOS
does not allow that: an iPhone only wakes the app when Apple's push
service says so, which a self-hosted server can't do by itself. The fix
is `NTFY_UPSTREAM_BASE_URL: https://ntfy.sh` on the `ntfy` service, together
with a public `NTFY_BASE_URL`. The server then sends ntfy.sh a "wake up" signal
(a message id, not the message text) and the iPhone fetches the text from
this server. That is switched off here; turn it on if an iPhone needs
alerts.

### Reaching it from outside

ntfy speaks plain HTTP on port 2586. To use it away from home it needs
HTTPS and a public name, and this folder doesn't provide either. Put it
behind whatever reverse proxy you already run and set `NTFY_BASE_URL` in
`.env` to the resulting address.

Here that is `https://ntfy.humblehome.net`, with no router ports opened:

```
phone -> Cloudflare (TLS) -> Cloudflare Tunnel -> Nginx Proxy Manager -> opsbox:2586
```

Three things make it work:

- **A proxy host** in Nginx Proxy Manager for the name, forwarding to
  `http://192.168.1.246:2586` with websockets on, and in its Advanced
  settings `proxy_buffering off; proxy_request_buffering off;
  proxy_read_timeout 3m; proxy_send_timeout 3m;`. The phone app holds one
  long connection open and messages trickle down it. With buffering on,
  nginx would sit on them.
- **A tunnel route and a DNS record** at Cloudflare: the name is a proxied
  CNAME to the tunnel, and the tunnel forwards it to the proxy.
- **`NTFY_BEHIND_PROXY`** in `docker-compose.yml`. ntfy limits requests and
  failed logins per client address, and behind a proxy every request
  seems to come from the proxy. This makes ntfy read the real client from
  the `X-Forwarded-For` header instead, so one stranger guessing passwords
  locks out only themselves.

Being public changes who can knock on the door, not who gets in: every
topic still needs a login, and there is no sign-up.

### Send a notification

```bash
./ntfy/send-test.sh                        # a test message
./ntfy/send-test.sh "Front door opened"    # your own text
```

From code it is one HTTP request with basic auth:

```python
import requests

requests.post(
    "http://192.168.1.246:2586/home-alerts",
    data="Front door opened",
    headers={"Title": "Door", "Priority": "high", "Tags": "door"},
    auth=("hub-publisher", NTFY_PUBLISHER_PASSWORD),
)
```

`Title`, `Priority` (`min` to `max`) and `Tags` (emoji short names) are
optional; the [publishing docs](https://docs.ntfy.sh/publish/) list the
rest.

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
- **ntfy's own port is plain HTTP.** Anyone who can capture traffic on
  your LAN could read a password sent to port 2586. Use the HTTPS address
  on phones and browsers and keep port 2586 for things running on the
  server itself.
- **ntfy's login page is on the internet.** It is protected by the two
  passwords in `.env` and by ntfy's limit on failed logins, so keep those
  passwords long. Cloudflare terminates TLS, which means it can read the
  alert text passing through.
- **Dev-container mode.** The broker skips fsync, so an unclean power loss
  can lose the last few seconds of writes. Before anything you care about
  depends on this broker, drop `--mode dev-container` and read the
  [production settings](https://docs.redpanda.com/current/deploy/redpanda/manual/production/).
- **Memory.** The broker is told to use 1 GB (`--memory 1G`) with a 1.5 GB
  container limit; Console and ring-mqtt are limited to 512 MB each, the
  bridge to 256 MB, and Mosquitto and ntfy to 128 MB each. In practice the
  Ring services use about 120 MB between them and ntfy about 20 MB.
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
                           # sign-in (you would have to sign in again), AND
                           # ntfy's stored messages (its accounts come back
                           # from .env on the next start)

docker compose stop ring-mqtt mosquitto ring-bridge   # pause just the Ring side
```
