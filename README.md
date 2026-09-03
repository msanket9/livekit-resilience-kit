# LiveKit Resilience Kit

**Work in progress.** Tests how a LiveKit deployment behaves under degraded network
conditions — packet loss, jitter, bandwidth caps, cellular-gateway-style drop/reconnect —
not just under scale.

- [Why this exists](#why-this-exists)
- [Requirements](#requirements)
- [Quickstart](#quickstart)
- [Repo layout](#repo-layout)
- [Fault profiles](#fault-profiles)
- [Metrics capture](#metrics-capture)
- [Running the full suite + report](#running-the-full-suite--report)
- [Measuring RED's actual effect](#measuring-reds-actual-effect)
- [Agent leg](#agent-leg)
- [Status](#status)

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

## Requirements

- Docker + Docker Compose (this was built and tested against Docker 29 / Compose v5)
- Python 3.12+ on the host, stdlib only — `scripts/generate_report.py` and the fault
  scripts don't need a virtualenv or any pip installs
- Everything else (LiveKit server, Pumba, the Python clients) runs in containers

## Quickstart

```bash
docker compose up --build -d                # LiveKit server + webhook receiver + client-a (publisher) + client-b (subscriber) + agent
./scripts/run_test_suite.sh                  # runs every fault profile back-to-back, ~5 min at the defaults
python3 scripts/generate_report.py           # writes reports/report-<run_id>.{json,md,html} -- open the .html in a browser
docker compose down
```

That's the whole loop: bring the stack up, run the suite, read the report. Everything
below is what each piece actually does and the real findings from running it.

## Repo layout

```
client/            Python LiveKit client (publisher or subscriber persona, by env var)
agent/               synthetic LiveKit Agent -- local VAD turn detection + synthetic response
webhook_receiver/   verifies + logs LiveKit server webhooks
config/livekit.yaml  local dev-mode server config (devkey/secret, webhook target)
faults/             one Pumba-driven fault-profile script per scenario
scripts/             run_test_suite.sh (orchestrator), generate_report.py, one-off smoke/verify scripts
data/                JSONL event/webhook logs + the run manifest (gitignored, generated)
reports/             generated report-<run_id>.{json,md,html} (gitignored, generated)
```

## Fault profiles

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

Sample output (all five profiles, one run, agent attached — full table, scroll right in the
`.html` version for the agent columns):

| Profile | Quality (Excellent/Good/Poor/Lost) | Reconnects | Avg recovery | Concealed audio | Agent turns (ok/failed) | Avg turn length |
|---|---|---|---|---|---|---|
| clean | 100.0% / 0% / 0% / 0% | 0 | — | 0.05s | 5/0 | 4.74s |
| rural_4g | 100.0% / 0% / 0% / 0% | 0 | — | 0.18s | 7/0 | 4.53s |
| gateway_dropout | 50.0% / 25.0% / 25.0% / 0% | 0 | — | 3.98s | 6/0 | 4.74s |
| congested_wifi | 100.0% / 0% / 0% / 0% | 0 | — | 0.01s | 6/0 | 4.59s |
| severe_outage | 66.7% / 0% / 0% / 33.3% | 1 | 16.86s | 19.98s | 1/3 | 4.51s |

A few things worth noting in that data:

- A short ~4s `gateway_dropout` never trips LiveKit's client-side reconnect logic (0
  reconnects) — it only shows up as a connection-quality dip and concealed audio. It took a
  continuous 25s outage (`severe_outage`) to actually force a `reconnecting` → `reconnected`
  cycle, which then took ~17s to recover — real numbers for a mechanism LiveKit ships but
  doesn't otherwise expose. That same outage is also the only profile where the agent's
  turn detection actually fails (1 ok / 3 failed) — see [Agent leg](#agent-leg).
- `concealed_samples` is a more sensitive signal than the connection-quality label for
  milder profiles: `rural_4g` shows measurable concealed audio (0.18s) while its quality
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

## Agent leg

The `agent` service is a **real** `livekit-agents` worker — automatic dispatch, same
`JobContext`/room API a production agent uses — but it is explicitly **not a
conversational agent**. It stands in for an STT→LLM→TTS pipeline using:

- **Turn detection**: local Silero VAD (`livekit-plugins-silero`), fully offline, no API
  key.
- **"Response"**: a synthetic tone published the instant end-of-speech is detected, in
  place of a real TTS reply.

This exists because the original plan (dispatch a real agent, measure response latency and
turn-taking under faults) was blocked on STT/LLM/TTS provider API keys nobody had supplied
yet — so it's built to measure exactly what the brief asked for (response latency,
turn-taking failures, recovery after a drop) without needing any of them. Swapping in real
STT/LLM/TTS plugins later is a drop-in change to `agent/agent.py`, not a redesign.

Two things had to be verified empirically before this worked at all, not assumed:

1. **A continuous tone isn't speech.** `client-a` originally published a continuous 440Hz
   sine wave (used for every earlier metric in this README) — fed through real Silero VAD,
   its speech probability never exceeded 0.005 (activation threshold is 0.5). A voice
   activity detector correctly refuses to treat a pure tone as speech, so the agent would
   never have anything to detect. Fixed by having `client-a` speak a short phrase with
   `espeak-ng` (fully offline, no API key) on a loop with pauses between — verified that
   *this* correctly drives VAD probability to 0.95+ immediately.
2. **Automatic dispatch is a room-creation-time event.** The agent worker takes a couple
   seconds to register with the LiveKit server after its container starts (plugin
   preload, ONNX init). If a client creates the room before that finishes, the agent never
   gets dispatched into it — dispatch doesn't retroactively fire for a worker that
   registers late. Fixed with a Compose healthcheck on the agent's worker HTTP port, gating
   `client-a`/`client-b` on `condition: service_healthy` so the room is never created before
   the agent can be dispatched into it.

Real measured result from the table above: every profile completes all its turns cleanly
(0 failures) **except** `severe_outage`, which fails 3 of 4 (VAD never gets the frames to
detect end-of-speech during a 25s blackout — a genuine turn-taking failure, not a fault in
the harness). A separate, more tightly-timed `gateway_dropout` run (interval=15s instead of
the default 45s, so multiple dropouts land clearly mid-window instead of once at a profile
boundary) shows the *other* failure mode: turns don't fail, but get truncated —

| Profile | Agent turns (ok/failed) | Avg turn length | Concealed audio |
|---|---|---|---|
| clean | 6/0 | 4.24s | 0.02s |
| gateway_dropout (interval=15s) | 5/0 | 3.28s | 12.47s |

— a real utterance the agent should hear as one continuous turn gets cut short mid-sentence
when frames stop arriving partway through it. Response latency itself stayed flat across
every profile (sub-millisecond) — expected, since this synthetic agent's "response" is
locally generated and doesn't depend on receiving anything back over the network; the
network-sensitive signals here are turn completion and turn length, not response latency.

Two bugs surfaced building this, both fixed before trusting the numbers above:
- The first version measured "response latency" as time-to-*finish-playing* the whole 1s
  response tone (always ≥1000ms) instead of time-to-*first-frame-pushed*. Fixed by timing
  the first frame specifically.
- A turn that starts in one profile's window but completes a moment into the next one was
  originally counted as a failure in the first window, purely because of where the fault
  boundary happened to fall — not a real failure. Fixed by pairing turns across the full
  event stream and attributing each one to whichever window it *started* in, the same fix
  already applied to `concealed_samples` across a track-SID change.

## Status

Done: local stack, all five fault profiles (verified against real ping/iperf3
measurements, not just their config), client + webhook metrics capture, the run
orchestrator and report generator, the RED on/off comparison, and the synthetic agent leg
above.

Not done: swapping the synthetic agent's local VAD + synthetic-tone stand-in for real
STT/LLM/TTS plugins — a drop-in change once API keys are available, not a redesign. Video
testing, multi-region testing, and Prometheus/Grafana export are explicitly out of scope
for this version (audio-only, single-machine, JSON/HTML reports are enough to prove the
concept).
