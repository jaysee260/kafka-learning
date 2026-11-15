#!/bin/sh
# Creates the ntfy accounts and their topic permissions from the
# environment, then exits. It runs as the one-shot `ntfy-users` service
# after the server is up, and again on every `docker compose up -d`, so
# .env stays the one place passwords live: change one there, run
# `docker compose up -d`, and it takes effect.
#
# The server denies everything by default (NTFY_AUTH_DEFAULT_ACCESS in
# docker-compose.yml), so an account can only do what is granted here.
set -eu

: "${NTFY_PUBLISHER_PASSWORD:?set NTFY_PUBLISHER_PASSWORD in .env}"
: "${NTFY_PHONE_PASSWORD:?set NTFY_PHONE_PASSWORD in .env}"

# set_user NAME PASSWORD: create the account if it is missing, then set the
# password either way so a changed .env value is picked up.
set_user() {
  NTFY_PASSWORD="$2" ntfy user add --ignore-exists "$1"
  NTFY_PASSWORD="$2" ntfy user change-pass "$1"
}

# Whatever sends alerts (a consumer reading home.ring.* topics): may
# publish to home-* topics and nothing else. It cannot read them back.
set_user hub-publisher "$NTFY_PUBLISHER_PASSWORD"
ntfy access hub-publisher 'home-*' write-only

# The phone app and the web page: may read home-* topics, cannot publish.
set_user phone "$NTFY_PHONE_PASSWORD"
ntfy access phone 'home-*' read-only

# Print the resulting accounts and permissions (no passwords) to the log.
ntfy access
