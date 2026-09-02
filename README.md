# LiveKit Resilience Kit

**Work in progress.** Tests how a LiveKit deployment behaves under degraded network
conditions — packet loss, jitter, bandwidth caps, cellular-gateway-style drop/reconnect —
not just under scale.

## Why this exists

LiveKit's own tooling (`lk load-test`, `lk perf agent-load-test`) tests **scale**: how many
participants, how many rooms, how much throughput a deployment can handle, assuming the
network itself is clean. It doesn't test **degradation** — what actually happens to
connection quality, reconnect time, and audio continuity when the network is bad. That's a
routine condition for any deployment off a clean fiber/office link: call centers on
congested lines, field-ops apps on cellular, IoT gateways that blink in and out.

I've lived that gap. Running Saafwater's IoT platform — 80+ ESP32/Modbus field devices on
cellular backhaul across Goa — meant constantly debugging PUSR M100 cellular gateways going
randomly offline. I built a synthetic evaluation harness that cut incident-detection time
from 4+ hours to under 5 minutes. This project applies the same instinct — test the real
failure mode, not just the happy path — to LiveKit's real-time transport layer, using
[Pumba](https://github.com/alexei-led/pumba) (proven Docker chaos-engineering) for fault
injection and LiveKit's own signals for the evidence.

### How this differs from LiveKit's own tooling

LiveKit already ships real resilience *mechanisms* — this kit doesn't duplicate them, it
verifies them against actual bad-network conditions instead of assuming they hold:

| | LiveKit's own tooling | This kit |
|---|---|---|
| `lk load-test` / `lk perf agent-load-test` | Scale: concurrent publishers/subscribers per room, CPU/bandwidth per SFU node, on an assumed-clean network ([benchmark docs](https://docs.livekit.io/transport/self-hosting/benchmark/)) | Degradation: real packet loss, jitter, bandwidth caps, and cellular-style drop/reconnect injected into real client containers |
| `ConnectionQuality` | A live signal computed in production from packet loss, video-layer delivery, and bitrate — jitter/RTT are explicitly excluded from the score ([LiveKit KB](https://kb.livekit.io/articles/2455399507-how-is-connection-quality-determined)) | Captured and correlated against the specific fault that caused it, per profile, in a comparison report |
| RED + Opus FEC | Enabled by default; LiveKit says this lets audio tolerate "~20–30% packet loss without retransmission" on Chromium/native SDKs ([LiveKit blog](https://livekit.com/blog/audio-quality)) | Measured directly via WebRTC's own `concealed_samples` stat — how much audio is actually being concealed under a given fault, not just the vendor's claim |
| Reconnect logic | A two-tier quick-reconnect (resume signaling + ICE restart) escalating to full reconnect, built into every client SDK ([Swift SDK guide](https://livekit-client-sdk-swift.mintlify.app/guides/reconnection)) | Counted and timed per fault profile — including the finding that a short ~4-5s outage degrades quality and conceals audio without tripping a full reconnect at all |

None of this is a knock on LiveKit — RED, adaptive reconnect, and `ConnectionQuality` are
solid engineering. But there's no first-party tool in LiveKit's own ecosystem that
deliberately breaks the network to confirm those mechanisms actually hold up in practice
(their GitHub org's own testing tools — `livekit-cli`'s load-tester and `chrometester` — are
both scale-oriented, not fault-injection). That's the gap this fills.

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
./faults/severe_outage.sh [target_container] [duration_seconds] [outage_seconds]
```

| Profile | Loss | Jitter | Latency | Bandwidth | Notes |
|---|---|---|---|---|---|
| `clean` | 0% | 0ms | ~20ms | unrestricted | baseline, no fault |
| `rural_4g` | 3.5% | ±75ms | 100ms | 2 Mbps up / 5 Mbps down | steady degradation |
| `gateway_dropout` | periodic 100% loss, 4s every 45s | — | — | — | mirrors a cellular gateway going dark and reconnecting |
| `congested_wifi` | 1.5% (correlated/bursty) | ±30ms | 30ms | 10 Mbps shared | stretch profile |
| `severe_outage` | one continuous 100% loss window, 25s by default | — | — | — | long enough to actually trip LiveKit's reconnect logic — `gateway_dropout`'s short outages never do (see below) |

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
  `disconnected`, and `track_stats` (polled every `AUDIO_STATS_POLL_SECONDS`, default 2s —
  WebRTC's own `concealed_samples`/`total_freeze_duration` for a subscribed audio track).
  Frame-arrival gaps turned out not to be a usable freeze signal: Opus packet-loss
  concealment keeps synthesizing filler frames on schedule during a real outage, so
  `concealed_samples` — not gap detection — is what actually reflects audio loss.
- `data/webhooks.jsonl` — LiveKit server webhooks (`room_started`, `participant_joined`,
  `track_published`, `participant_left`, `room_finished`, ...), verified and logged by the
  `webhook-receiver` service.

These files accumulate for the life of a `docker compose up` session — run
`rm -f data/*.jsonl` (or `docker compose down && docker compose up`) before a fresh test
run if you want a clean slate.

## Running the full suite + report

With the stack up, run every profile back-to-back and generate a clean-vs-degraded
comparison:

```bash
./scripts/run_test_suite.sh          # DURATION_SECONDS=60 PROFILES="clean rural_4g ..." to override
python3 scripts/generate_report.py   # writes reports/report-<run_id>.{json,md,html}
```

Sample output:

| Profile | Duration | Quality (Excellent/Good/Poor/Lost) | Reconnects | Avg recovery | Concealed audio |
|---|---|---|---|---|---|
| clean | 45s | 100.0% / 0% / 0% / 0% | 0 | — | 1.45s |
| rural_4g | 45s | 100.0% / 0% / 0% / 0% | 0 | — | 0.15s |
| gateway_dropout | 45s | 50.0% / 25.0% / 25.0% / 0% | 0 | — | 3.91s |
| congested_wifi | 45s | 100.0% / 0% / 0% / 0% | 0 | — | 0.0s |
| severe_outage | 45s | 66.7% / 0% / 0% / 33.3% | 1 | 18.48s | 19.3s |

A few things worth noting in that data:

- A short ~4s `gateway_dropout` never trips LiveKit's client-side reconnect logic (0
  reconnects) — it only shows up as a connection-quality dip and concealed audio. It took a
  continuous 25s outage (`severe_outage`) to actually force a `reconnecting` → `reconnected`
  cycle, which then took ~18.5s to recover — real numbers for a mechanism LiveKit ships but
  doesn't otherwise expose.
- `concealed_samples` is a more sensitive signal than the connection-quality label for
  milder profiles: `rural_4g` shows measurable concealed audio (0.15s) while its quality
  label stayed "Excellent" the whole window — consistent with LiveKit's own `ConnectionQuality`
  scorer excluding jitter/RTT from its score (see "Why this exists" above).
- Computing concealed-audio duration correctly across a `severe_outage` window took a fix:
  a full reconnect re-subscribes to a **new track SID** whose cumulative WebRTC counters
  reset to 0, so a naive "last value at window end minus last value at window start" diff
  crosses that SID boundary, goes negative, and silently reports 0 — hiding the ~19s of real
  concealment that happened on the old track right before it was torn down. The report
  generator now diffs each track SID separately and sums across SID changes within a window.

## Measuring RED's actual effect

LiveKit publishes tracks with RED (redundant audio encoding) enabled by default, and its
[blog](https://livekit.com/blog/audio-quality) claims this "lets the audio stream tolerate
~20–30% packet loss without retransmission." Rather than take that on faith, `client-a` can
force RED on or off (`TrackPublishOptions.red`, a proto3 optional the client normally never
touches, so it silently follows LiveKit's own default) and the same fault profile can be run
against both, for a real before/after:

```bash
docker compose up --build -d
./scripts/run_test_suite.sh                                         # RED at LiveKit's default (on)
RED_ENABLED=false docker compose up -d --force-recreate --no-deps client-a
./scripts/run_test_suite.sh                                         # RED forced off
python3 scripts/generate_report.py --run-id <first_run_id>
python3 scripts/generate_report.py --run-id <second_run_id>
```

Note RED only protects against *partial* packet loss (redundant copies ride in later
packets) — it can't help against a full blackout like `gateway_dropout`/`severe_outage`, so
`rural_4g` or `congested_wifi` are the right profiles for this comparison, not those. Actual
measured result, same `rural_4g` profile, 60s each, only RED toggled:

| RED | Quality (Excellent/Good/Poor/Lost) | Concealed audio |
|---|---|---|
| on (default) | 100.0% / 0% / 0% / 0% | 0.27s |
| off | 50.0% / 50.0% / 0% / 0% | 2.15s |

RED cut concealed audio by roughly 8x under identical injected loss/jitter/bandwidth, and
kept `ConnectionQuality` steady at "Excellent" instead of dropping half the time to "Good."
The vendor claim holds up — and now there's a measured number behind it instead of just a
blog post.
