# Always-on Redpanda on a homelab box

The repo-root `docker-compose.yml` runs everything on your laptop and only
answers on `localhost`. This folder is a second, separate deployment: one
Redpanda broker and Redpanda Console on a machine that stays on, reachable
from any other machine on your LAN. The Python services in this repo work
against either one; you choose with `BOOTSTRAP_SERVERS`.

## Start it (on the server)

```bash
cd deploy/opsbox
cp .env.example .env         # set REDPANDA_ADVERTISED_HOST to the server's LAN IP
docker compose up -d
docker compose ps            # wait until redpanda shows "healthy"
```

Both containers use `restart: unless-stopped`, so they come back after a
crash or a reboot. Data lives in the `event-hub_redpanda-data` Docker
volume and survives `docker compose down`.

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

## Things to know

- **No authentication.** Anyone on your LAN can read and write topics and
  use Console. That's fine for a home network; don't port-forward these
  ports or put them on a network you don't trust. The ports are published
  on IPv4 only, so a public IPv6 address on the host doesn't expose them.
- **Dev-container mode.** The broker skips fsync, so an unclean power loss
  can lose the last few seconds of writes. Before anything you care about
  depends on this broker, drop `--mode dev-container` and read the
  [production settings](https://docs.redpanda.com/current/deploy/redpanda/manual/production/).
- **Memory.** The broker is told to use 1 GB (`--memory 1G`) with a 1.5 GB
  container limit; Console is limited to 512 MB.
- **Disk.** Topics keep data for 7 days by default with no size cap. That
  is tiny for this project's traffic. Set `retention.bytes` on a topic
  before pointing anything high-volume at it.
- **The advertised address must not change.** Give the server a static IP
  or a DHCP reservation. If it changes, update `.env` and run
  `docker compose up -d`.

## Stopping and resetting

```bash
docker compose down        # stop; topics and offsets are kept
docker compose down -v     # stop and DELETE all topics and offsets
```
