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
# Quiet gap between profiles, outside every measured window. Profiles used to
# run truly back-to-back, which let one profile's tail bleed into the next
# one's head: LiveKit's bandwidth estimator ramps back up over seconds, the
# subscriber's jitter buffer is still draining, and a track torn down at the
# end of a fault may not be resubscribed yet. All of that landed inside the
# next profile's numbers and was attributed to the next profile's fault.
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-10}"
# shellcheck disable=SC2206
PROFILES=(${PROFILES:-clean rural_4g gateway_dropout congested_wifi severe_outage})

now() { python3 -c "import time; print(time.time())"; }

CURRENT_FAULT_PID=""
INTERRUPTED=0

# Each fault script traps its own signals and restores the netem rules it
# applied. That covers Ctrl-C, which the terminal delivers to the whole process
# group. It does NOT cover a SIGTERM delivered to this script alone -- verified:
# killing the suite that way left two pumba containers running and 166ms of
# injected latency still applied to client-a, which would then silently degrade
# every later run. So the suite forwards the signal to the fault script it is
# currently waiting on, and sweeps any container that outlived its parent.
suite_cleanup() {
  if [ -n "${CURRENT_FAULT_PID}" ]; then
    kill -TERM "${CURRENT_FAULT_PID}" 2>/dev/null || true
    wait "${CURRENT_FAULT_PID}" 2>/dev/null || true
    CURRENT_FAULT_PID=""
  fi
  if [ "${INTERRUPTED}" -eq 1 ]; then
    leftover="$(docker ps -q --filter 'name=^pumba-' 2>/dev/null || true)"
    if [ -n "${leftover}" ]; then
      echo "!! stopping pumba container(s) that outlived their fault script" >&2
      # shellcheck disable=SC2086
      docker stop ${leftover} >/dev/null 2>&1 || true
    fi
  fi
}
on_signal() { INTERRUPTED=1; suite_cleanup; exit 130; }
trap suite_cleanup EXIT
trap on_signal INT TERM

RUN_ID="$(python3 -c "import time; print(int(time.time()))")"
echo "== Run suite: run_id=${RUN_ID} profiles=[${PROFILES[*]}] duration=${DURATION_SECONDS}s each, cooldown=${COOLDOWN_SECONDS}s, target=${TARGET_CONTAINER} =="

first_profile=1

for profile in "${PROFILES[@]}"; do
  script="${FAULTS_DIR}/${profile}.sh"
  if [ ! -x "${script}" ]; then
    echo "skipping unknown profile: ${profile}" >&2
    continue
  fi

  if [ "${first_profile}" -eq 0 ] && [ "${COOLDOWN_SECONDS}" -gt 0 ]; then
    echo "-- cooldown ${COOLDOWN_SECONDS}s (not measured) --"
    sleep "${COOLDOWN_SECONDS}"
  fi
  first_profile=0

  echo
  echo "-- profile: ${profile} --"
  start_ts="$(now)"
  # Backgrounded so the suite keeps its own PID for the running fault script and
  # can forward a signal to it (see suite_cleanup); `wait` still makes this
  # sequential, exactly as before.
  if [ "${profile}" = "clean" ]; then
    "${script}" "${DURATION_SECONDS}" &
  else
    "${script}" "${TARGET_CONTAINER}" "${DURATION_SECONDS}" &
  fi
  CURRENT_FAULT_PID=$!
  profile_ok=1
  wait "${CURRENT_FAULT_PID}" || profile_ok=0
  CURRENT_FAULT_PID=""
  end_ts="$(now)"

  if [ "${profile_ok}" -eq 0 ]; then
    # e.g. severe_outage needs DURATION_SECONDS greater than its own outage
    # window -- one profile failing (bad params, transient docker/pumba
    # error) shouldn't lose the rest of the suite or its manifest entries.
    echo "!! profile ${profile} failed -- skipping, rest of the suite continues" >&2
    continue
  fi

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
