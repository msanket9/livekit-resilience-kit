# LiveKit Resilience Kit

**Work in progress.** Tests how a LiveKit deployment behaves under degraded network
conditions — packet loss, jitter, bandwidth caps, cellular-gateway-style drop/reconnect —
not just under scale. Full write-up lands once the report generator is in place.

## Local stack

Brings up a local LiveKit server (dev mode) plus two containerized Python clients
(`client-a` publishes a synthetic sine-tone audio track, `client-b` subscribes to it).

```bash
docker compose up --build
```

Watch `client-a` log a published track and `client-b` log `track_subscribed`.

```bash
docker compose down
```

## Fault injection

Network faults are injected with [Pumba](https://github.com/alexei-led/pumba) against a
running client container. Each profile in [`faults/`](faults/) is a standalone script:

```bash
./faults/clean.sh [duration_seconds]
./faults/rural_4g.sh [uplink_container] [duration_seconds] [downlink_container]
./faults/gateway_dropout.sh [target_container] [duration_seconds] [dropout_seconds] [interval_seconds]
./faults/congested_wifi.sh [target_container] [duration_seconds]
```

| Profile | Loss | Jitter | Latency | Bandwidth | Notes |
|---|---|---|---|---|---|
| `clean` | 0% | 0ms | ~20ms | unrestricted | baseline, no fault |
| `rural_4g` | 3.5% | ±75ms | 100ms | 2 Mbps up / 5 Mbps down | steady degradation |
| `gateway_dropout` | periodic 100% loss, 4s every 45s | — | — | — | mirrors a cellular gateway going dark and reconnecting |
| `congested_wifi` | 1.5% (correlated/bursty) | ±30ms | 30ms | 10 Mbps shared | stretch profile |

With the stack up, verify a profile actually lands (ping for loss/latency, `iperf3` for
bandwidth):

```bash
./scripts/verify_fault_profiles.sh
```

(`./scripts/pumba_smoke_test.sh` runs a single minimal delay injection if you just want a
quick sanity check instead of the full profile sweep.)

## Metrics capture

While the stack is up, LiveKit's own signals are captured as JSON lines under `./data/`:

- `data/client-a-events.jsonl`, `data/client-b-events.jsonl` — one line per client-side
  event: `connect_start`/`connected` (time-to-first-connect), `connection_quality_changed`,
  `reconnecting`/`reconnected` (ICE-restart proxy), `track_subscribed`/`track_unsubscribed`,
  `disconnected`, and `freeze` (a gap over `FREEZE_THRESHOLD_MS`, default 300ms, between
  consecutive frames on a subscribed audio track).
- `data/webhooks.jsonl` — LiveKit server webhooks (`room_started`, `participant_joined`,
  `track_published`, `participant_left`, `room_finished`, ...), verified and logged by the
  `webhook-receiver` service.

These files accumulate for the life of a `docker compose up` session — run
`rm -f data/*.jsonl` (or `docker compose down && docker compose up`) before a fresh test
run if you want a clean slate. A report generator that turns these into a clean-vs-degraded
comparison is next.
