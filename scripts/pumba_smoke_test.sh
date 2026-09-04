#!/usr/bin/env bash
# Day-1 smoke test: prove Pumba can inject a network fault into a running
# LiveKit client container. Run this while `docker compose up` is up.
set -euo pipefail

TARGET_CONTAINER="${1:-client-b}"
DELAY_MS="${2:-200}"
DURATION="${3:-20s}"
PING_TARGET="${4:-livekit-server}"

CONTAINER_NAME="pumba-smoke-test-$$"

# This script injects a REAL netem fault, so it needs the same cleanup guarantee
# as faults/*.sh: Pumba restores the rules it applied on SIGTERM, so the cleanup
# is `docker stop`, not `docker kill`. Without it, interrupting the smoke test
# leaves the delay applied to the container indefinitely -- which is especially
# easy to miss here, because this is the script someone runs first, before they
# know what the baseline is supposed to look like.
cleanup() {
  docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "== Baseline: ping from ${TARGET_CONTAINER} to ${PING_TARGET} =="
docker exec "${TARGET_CONTAINER}" ping -c 4 "${PING_TARGET}"

echo
echo "== Injecting ${DELAY_MS}ms delay into ${TARGET_CONTAINER} via Pumba for ${DURATION} =="
docker run --rm \
  --name "${CONTAINER_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba:1.2.1 \
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
