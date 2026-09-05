#!/usr/bin/env bash
# Rural 4G profile: steady degradation on a cellular uplink.
# ~3.5% loss, 100ms latency +/-75ms jitter, ~2Mbps up / 5Mbps down.
#
# netem only shapes a container's own egress traffic, so "up" (the field
# client's uplink) is shaped directly on UPLINK_CONTAINER, and "down" is
# approximated by rate-limiting DOWNLINK_CONTAINER's (the LiveKit server's)
# egress toward the room: coarse, since it applies to every participant in
# the room, but good enough for a single-client proof of concept.
set -euo pipefail

UPLINK_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-60}"
DOWNLINK_CONTAINER="${3:-livekit-server}"
DURATION="${DURATION_SECONDS}s"

UPLINK_NAME="pumba-rural4g-up-$$"
DOWNLINK_NAME="pumba-rural4g-down-$$"

# Pumba restores the netem rules it applied when it receives SIGTERM, so the
# cleanup is `docker stop`, not `docker kill`. Without this trap an interrupted
# run leaves BOTH qdiscs applied -- and this profile shapes livekit-server as
# well as the client, so a leaked downlink cap degrades every participant in
# every later run until the containers are recreated.
cleanup() {
  docker stop "${UPLINK_NAME}" "${DOWNLINK_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "== Rural 4G: uplink shaping (loss+jitter+2Mbps) on ${UPLINK_CONTAINER} for ${DURATION} =="
docker run --rm \
  --name "${UPLINK_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba:1.2.1 \
  netem --duration "${DURATION}" combine \
    --delay --delay-time 100 --delay-jitter 75 \
    --loss --loss-percent 3.5 \
    --rate --rate-value 2mbit \
    -- "${UPLINK_CONTAINER}" &
UPLINK_PID=$!

echo "== Rural 4G: downlink cap (5Mbps) on ${DOWNLINK_CONTAINER} for ${DURATION} =="
docker run --rm \
  --name "${DOWNLINK_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba:1.2.1 \
  netem --duration "${DURATION}" rate --rate 5mbit \
    "${DOWNLINK_CONTAINER}" &
DOWNLINK_PID=$!

uplink_status=0
downlink_status=0
wait "${UPLINK_PID}" || uplink_status=$?
wait "${DOWNLINK_PID}" || downlink_status=$?
# `wait pid1 pid2` only reports the LAST-listed job's exit status in bash --
# waiting on each separately so a failed uplink shaping command doesn't get
# silently masked by a successful downlink one (or vice versa).
if [ "${uplink_status}" -ne 0 ] || [ "${downlink_status}" -ne 0 ]; then
  echo "!! rural_4g: uplink_status=${uplink_status} downlink_status=${downlink_status}" >&2
  exit 1
fi
echo "== Rural 4G profile done =="
