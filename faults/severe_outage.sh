#!/usr/bin/env bash
# Severe outage profile: one long full-loss window, long enough to actually
# trip LiveKit's own reconnect logic. gateway_dropout's short 4-5s outages
# were verified to only show up as a connection-quality dip and concealed
# audio -- never a real reconnect. This profile exists to exercise and
# measure the quick/full-reconnect mechanism itself.
set -euo pipefail

TARGET_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-35}"
OUTAGE_SECONDS="${3:-25}"

CONTAINER_NAME="pumba-severe-outage-$$"

# Pumba restores the netem rules it applied when it receives SIGTERM, so the
# cleanup is `docker stop`, not `docker kill`. Without this trap an interrupted
# run (Ctrl-C, or the suite above it dying) leaves the qdisc applied to the
# container indefinitely -- silently degrading every later profile and every
# later run until the container is recreated. gateway_dropout.sh already did
# this; the other profiles did not.
cleanup() {
  docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if [ "${OUTAGE_SECONDS}" -ge "${DURATION_SECONDS}" ]; then
  echo "OUTAGE_SECONDS must be less than DURATION_SECONDS (need time left to observe recovery)" >&2
  exit 1
fi

echo "== Severe outage: ${OUTAGE_SECONDS}s of 100% loss on ${TARGET_CONTAINER}, then $((DURATION_SECONDS - OUTAGE_SECONDS))s to observe recovery =="
# Both phases backgrounded and joined via `wait`, not run as a foreground
# `docker run`/`sleep`: bash defers a trapped signal until a FOREGROUND
# command finishes on its own, so a SIGTERM aimed at this script during either
# phase did not reach `cleanup` until that phase's own duration elapsed --
# leaving the outage fully applied for up to its remaining length. `wait` on a
# backgrounded job is interrupted immediately, matching rural_4g.sh.
docker run --rm \
  --name "${CONTAINER_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba:1.2.1 \
  netem --duration "${OUTAGE_SECONDS}s" loss --percent 100 \
  -- "${TARGET_CONTAINER}" &
PUMBA_PID=$!
wait "${PUMBA_PID}"

sleep "$((DURATION_SECONDS - OUTAGE_SECONDS))" &
SLEEP_PID=$!
wait "${SLEEP_PID}"
echo "== Severe outage profile done =="
