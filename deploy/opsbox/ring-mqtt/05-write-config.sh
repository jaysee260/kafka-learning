#!/command/with-contenv sh
# ring-mqtt only reads its settings from /data/config.json, never from
# environment variables. This runs inside the container before ring-mqtt
# starts and writes that file from the environment, so the MQTT password
# lives in .env like every other secret here.
#
# It does not touch /data/ring-state.json, which holds the Ring login
# token that ring-mqtt saves after you sign in.
set -eu

: "${MQTT_RING_PASSWORD:?set MQTT_RING_PASSWORD in .env}"

node -e '
const fs = require("fs")
const url = new URL("mqtt://mosquitto:1883")
url.username = "ring-mqtt"
url.password = process.env.MQTT_RING_PASSWORD   // percent-encoded for us
fs.writeFileSync("/data/config.json", JSON.stringify({
  mqtt_url: url.href,
  mqtt_options: "",
  livestream_user: "",
  livestream_pass: "",
  disarm_code: "",
  // Doorbells and cameras are where motion and ding events come from.
  enable_cameras: true,
  enable_modes: false,
  enable_panic: false,
  hass_topic: "homeassistant/status",
  ring_topic: "ring",
  location_ids: []
}, null, 2) + "\n", { mode: 0o600 })
'
echo "ring-mqtt config written to /data/config.json"
