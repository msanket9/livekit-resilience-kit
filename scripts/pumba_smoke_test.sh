#!/usr/bin/env bash
# Day-1 smoke test: prove Pumba can inject a network fault into a running
# LiveKit client container. Run this while `docker compose up` is up.
set -euo pipefail

TARGET_CONTAINER="${1:-client-b}"
DELAY_MS="${2:-200}"
DURATION="${3:-20s}"
PING_TARGET="${4:-livekit-server}"

echo "== Baseline: ping from ${TARGET_CONTAINER} to ${PING_TARGET} =="
docker exec "${TARGET_CONTAINER}" ping -c 4 "${PING_TARGET}"

echo
echo "== Injecting ${DELAY_MS}ms delay into ${TARGET_CONTAINER} via Pumba for ${DURATION} =="
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba \
  netem --duration "${DURATION}" delay --time "${DELAY_MS}" "${TARGET_CONTAINER}" &
PUMBA_PID=$!

sleep 2

echo
echo "== While fault is active: ping from ${TARGET_CONTAINER} to ${PING_TARGET} =="
docker exec "${TARGET_CONTAINER}" ping -c 4 "${PING_TARGET}"

wait "${PUMBA_PID}"

echo
echo "== After fault clears: ping from ${TARGET_CONTAINER} to ${PING_TARGET} =="
docker exec "${TARGET_CONTAINER}" ping -c 4 "${PING_TARGET}"
