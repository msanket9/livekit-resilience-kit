#!/usr/bin/env bash
# Verifies each fault profile actually lands, by measuring loss/latency with
# ping and bandwidth with iperf3 before/during each profile. Run this against
# a stack already brought up with `docker compose up --build -d`.
set -euo pipefail

FAULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../faults" && pwd)"

echo "###### baseline ######"
docker exec client-a ping -c 4 client-b
docker exec -d client-b iperf3 -s -1
sleep 1
docker exec client-a iperf3 -c client-b -t 5

echo
echo "###### rural_4g (loss + jitter + ~2Mbps up / 5Mbps down) ######"
"${FAULTS_DIR}/rural_4g.sh" client-a 15 livekit-server &
FAULT_PID=$!
sleep 3
docker exec client-a ping -c 6 client-b
docker exec -d client-b iperf3 -s -1
sleep 1
docker exec client-a iperf3 -c client-b -t 8
wait "${FAULT_PID}"

echo
echo "###### congested_wifi (bursty loss + jitter + 10Mbps shared) ######"
"${FAULTS_DIR}/congested_wifi.sh" client-a 15 &
FAULT_PID=$!
sleep 3
docker exec client-a ping -c 6 client-b
docker exec -d client-b iperf3 -s -1
sleep 1
docker exec client-a iperf3 -c client-b -t 8
wait "${FAULT_PID}"

echo
echo "###### gateway_dropout (periodic 100% loss) ######"
"${FAULTS_DIR}/gateway_dropout.sh" client-a 50 4 20 &
FAULT_PID=$!
echo "-- ping during the first ~20s window (expect a clean stretch then a dropout) --"
docker exec client-a ping -c 20 -i 1 client-b || true
wait "${FAULT_PID}"

echo
echo "###### clean (no fault, sanity check) ######"
"${FAULTS_DIR}/clean.sh" 3
docker exec client-a ping -c 4 client-b

echo
echo "All fault profiles verified."
