#!/bin/sh
# Builds Mosquitto's password file from the environment, then starts the
# broker. Doing it at startup keeps .env as the one place passwords live:
# change one there, run `docker compose up -d`, and it takes effect.
set -eu

: "${MQTT_RING_PASSWORD:?set MQTT_RING_PASSWORD in .env}"
: "${MQTT_BRIDGE_PASSWORD:?set MQTT_BRIDGE_PASSWORD in .env}"
: "${MQTT_VIEWER_PASSWORD:?set MQTT_VIEWER_PASSWORD in .env}"

AUTH=/mosquitto/auth
mkdir -p "$AUTH"
rm -f "$AUTH/passwd"

mosquitto_passwd -b -c "$AUTH/passwd" ring-mqtt "$MQTT_RING_PASSWORD"
mosquitto_passwd -b "$AUTH/passwd" bridge "$MQTT_BRIDGE_PASSWORD"
mosquitto_passwd -b "$AUTH/passwd" viewer "$MQTT_VIEWER_PASSWORD"

# Mosquitto wants these files owned by its own user and private.
cp /hub/acl "$AUTH/acl"
chown -R mosquitto:mosquitto "$AUTH"
chmod 0700 "$AUTH"
chmod 0600 "$AUTH/passwd" "$AUTH/acl"

exec /usr/sbin/mosquitto -c /hub/mosquitto.conf
