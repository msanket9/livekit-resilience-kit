#!/usr/bin/env bash
# Congested WiFi profile (stretch): bursty contention on a shared AP.
# ~1.5% loss (correlated, to make it bursty rather than uniformly random),
# 30ms latency +/-30ms jitter, 10Mbps shared cap.
set -euo pipefail

TARGET_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-60}"
DURATION="${DURATION_SECONDS}s"

CONTAINER_NAME="pumba-congested-wifi-$$"

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

echo "== Congested WiFi: loss+jitter+10Mbps cap on ${TARGET_CONTAINER} for ${DURATION} =="
docker run --rm \
  --name "${CONTAINER_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba \
  netem --duration "${DURATION}" combine \
    --delay --delay-time 30 --delay-jitter 30 \
    --loss --loss-percent 1.5 --loss-correlation 25 \
    --rate --rate-value 10mbit \
    -- "${TARGET_CONTAINER}"

echo "== Congested WiFi profile done =="
