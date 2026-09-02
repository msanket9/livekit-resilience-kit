#!/usr/bin/env bash
# Clean baseline: no fault injected. Exists so the report generator can run
# every profile (including "no fault") through the same interface.
set -euo pipefail

DURATION_SECONDS="${1:-60}"

echo "== Clean baseline: no fault injected, waiting ${DURATION_SECONDS}s =="
sleep "${DURATION_SECONDS}"
echo "== Clean baseline done =="
