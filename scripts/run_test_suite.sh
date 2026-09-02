#!/usr/bin/env bash
# Runs each fault profile back-to-back against the already-running stack,
# recording a {run_id, profile, start_ts, end_ts} manifest line per profile
# to data/run_manifest.jsonl so generate_report.py can slice the event logs
# by time window. Non-destructive: never clears existing data files.
set -euo pipefail

FAULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../faults" && pwd)"
DATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../data" && pwd)"
MANIFEST="${DATA_DIR}/run_manifest.jsonl"

TARGET_CONTAINER="${TARGET_CONTAINER:-client-a}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
# shellcheck disable=SC2206
PROFILES=(${PROFILES:-clean rural_4g gateway_dropout congested_wifi severe_outage})

now() { python3 -c "import time; print(time.time())"; }

RUN_ID="$(python3 -c "import time; print(int(time.time()))")"
echo "== Run suite: run_id=${RUN_ID} profiles=[${PROFILES[*]}] duration=${DURATION_SECONDS}s each, target=${TARGET_CONTAINER} =="

for profile in "${PROFILES[@]}"; do
  script="${FAULTS_DIR}/${profile}.sh"
  if [ ! -x "${script}" ]; then
    echo "skipping unknown profile: ${profile}" >&2
    continue
  fi

  echo
  echo "-- profile: ${profile} --"
  start_ts="$(now)"
  if [ "${profile}" = "clean" ]; then
    "${script}" "${DURATION_SECONDS}"
  else
    "${script}" "${TARGET_CONTAINER}" "${DURATION_SECONDS}"
  fi
  end_ts="$(now)"

  python3 -c "
import json
with open('${MANIFEST}', 'a') as f:
    f.write(json.dumps({
        'run_id': '${RUN_ID}',
        'profile': '${profile}',
        'duration_s': ${DURATION_SECONDS},
        'start_ts': ${start_ts},
        'end_ts': ${end_ts},
    }) + '\n')
"
done

echo
echo "== Suite done. Generate the report with: =="
echo "   python3 scripts/generate_report.py --run-id ${RUN_ID}"
