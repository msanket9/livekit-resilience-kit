# LiveKit Resilience Kit

**Work in progress.** Tests how a LiveKit deployment behaves under degraded network
conditions — packet loss, jitter, bandwidth caps, cellular-gateway-style drop/reconnect —
not just under scale. Full write-up lands once metrics capture and the report generator
are in place.

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
