#!/usr/bin/env bash
# Cellular gateway dropout profile: periodic full outages, mirroring a PUSR
# M100-style cellular gateway going dark and reconnecting mid-session.
# 100% loss for a few seconds, repeating every 30-60s for the test duration.
#
# Doesn't rely on GNU `timeout` (absent on macOS by default) — instead runs
# pumba in the background and stops its container once the overall test
# duration elapses.
set -euo pipefail

TARGET_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-180}"
DROPOUT_SECONDS="${3:-4}"
INTERVAL_SECONDS="${4:-45}"

CONTAINER_NAME="pumba-gateway-dropout-$$"

cleanup() {
  docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "== Gateway dropout: ${DROPOUT_SECONDS}s of 100% loss every ${INTERVAL_SECONDS}s on ${TARGET_CONTAINER}, for ${DURATION_SECONDS}s total =="
docker run --rm \
  --name "${CONTAINER_NAME}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba:1.2.1 \
  --interval "${INTERVAL_SECONDS}s" \
  netem --duration "${DROPOUT_SECONDS}s" loss --percent 100 \
  -- "${TARGET_CONTAINER}" &
PUMBA_PID=$!

# Backgrounded and joined via `wait`, not run as a foreground `sleep`: bash
# defers a trapped signal until a FOREGROUND command finishes on its own, but
# interrupts `wait` on a backgrounded job immediately. Measured directly: a
# foreground `sleep` here left the fault fully applied for up to 55s after a
# SIGTERM aimed at this script; this is the same pattern rural_4g.sh already
# used (its two `wait "${PID}"` calls), which measured under 2s.
sleep "${DURATION_SECONDS}" &
SLEEP_PID=$!
wait "${SLEEP_PID}"
cleanup
wait "${PUMBA_PID}" 2>/dev/null || true

echo "== Gateway dropout profile done =="
