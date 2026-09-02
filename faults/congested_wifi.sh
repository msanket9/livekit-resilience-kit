#!/usr/bin/env bash
# Congested WiFi profile (stretch): bursty contention on a shared AP.
# ~1.5% loss (correlated, to make it bursty rather than uniformly random),
# 30ms latency +/-30ms jitter, 10Mbps shared cap.
set -euo pipefail

TARGET_CONTAINER="${1:-client-a}"
DURATION_SECONDS="${2:-60}"
DURATION="${DURATION_SECONDS}s"

echo "== Congested WiFi: loss+jitter+10Mbps cap on ${TARGET_CONTAINER} for ${DURATION} =="
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba \
  netem --duration "${DURATION}" combine \
    --delay --delay-time 30 --delay-jitter 30 \
    --loss --loss-percent 1.5 --loss-correlation 25 \
    --rate --rate-value 10mbit \
    -- "${TARGET_CONTAINER}"

echo "== Congested WiFi profile done =="
