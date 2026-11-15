#!/bin/sh
# Sends one notification to the home-alerts topic as the hub-publisher
# account, the same way a consumer would. Run it on the server:
#
#   ./ntfy/send-test.sh                       default text
#   ./ntfy/send-test.sh "Front door opened"   your own text
#
# The password is read from .env and handed to curl on stdin, so it never
# shows up in the process list or your shell history.
set -eu

cd "$(dirname "$0")/.."
PASSWORD=$(sed -n 's/^NTFY_PUBLISHER_PASSWORD=//p' .env)
: "${PASSWORD:?NTFY_PUBLISHER_PASSWORD is missing from .env}"

printf 'user = "hub-publisher:%s"\n' "$PASSWORD" |
  curl -fsS -K - \
    -H "Title: Event hub test" \
    -H "Tags: white_check_mark" \
    -d "${1:-Test notification from the event hub. If you can read this on your phone, ntfy works.}" \
    "http://127.0.0.1:2586/home-alerts"
echo
