#!/usr/bin/env bash
# Rural 4G profile: steady degradation on a cellular uplink.
# ~3.5% loss, 100ms latency +/-75ms jitter, ~2Mbps up / 5Mbps down.
#
# netem only shapes a container's own egress traffic, so "up" (the field
# client's uplink) is shaped directly on UPLINK_CONTAINER, and "down" is
# approximated by rate-limiting DOWNLINK_CONTAINER's (the LiveKit server's)
# egress toward the room — coarse, since it applies to every participant in
# the room, but good enough for a single-client proof of concept.
set -euo pipefail

UPLINK_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-60}"
DOWNLINK_CONTAINER="${3:-livekit-server}"
DURATION="${DURATION_SECONDS}s"

echo "== Rural 4G: uplink shaping (loss+jitter+2Mbps) on ${UPLINK_CONTAINER} for ${DURATION} =="
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba \
  netem --duration "${DURATION}" combine \
    --delay --delay-time 100 --delay-jitter 75 \
    --loss --loss-percent 3.5 \
    --rate --rate-value 2mbit \
    -- "${UPLINK_CONTAINER}" &
UPLINK_PID=$!

echo "== Rural 4G: downlink cap (5Mbps) on ${DOWNLINK_CONTAINER} for ${DURATION} =="
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba \
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
