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

These files are append-only and accumulate across runs. That is deliberate — the report
slices them by the `run_id` time windows in `data/run_manifest.jsonl`, so an old run stays
reproducible — but it means anything scanning the full stream has to be run-aware. Run
`rm -f data/*.jsonl` (or `docker compose down && docker compose up`) if you want a clean
slate.

## Running the full suite + report

With the stack up, run every profile back-to-back and generate a clean-vs-degraded
comparison:

```bash
./scripts/run_test_suite.sh          # DURATION_SECONDS=60 COOLDOWN_SECONDS=10 PROFILES="clean rural_4g ..." to override
python3 scripts/generate_report.py   # writes reports/report-<run_id>.{json,md,html}
```

The metric logic has its own regression suite. Every case in it is a bug that actually
shipped, and each one failed silently as a wrong number rather than an error, which is the
failure mode worth pinning down in a harness whose entire output is measurements:

```bash
python3 scripts/test_generate_report.py   # no dependencies, exits non-zero on failure
```

Sample output (all five profiles, one run, 35s each, agent attached — abbreviated; the
full table is 11 columns wide, scroll right in the `.html` version for the agent columns):

| Profile | Quality (Excellent/Good/Poor/Lost) | Reconnects | Avg recovery | Concealed audio | Agent turns (detected, ok/failed) | Avg turn length |
|---|---|---|---|---|---|---|
| clean | 100.0% / 0% / 0% / 0% | 0 | — | 0.01s | 7 (7/0) | 3.97s |
| rural_4g | 100.0% / 0% / 0% / 0% | 0 | — | 0.89s | 6 (6/0) | 3.99s |
| gateway_dropout | 57.4% / 28.4% / 14.2% / 0% | 0 | — | 4.24s | 5 (5/0) | 3.93s |
| congested_wifi | 100.0% / 0% / 0% / 0% | 0 | — | 0.0s | 6 (6/0) | 3.96s |
| severe_outage | 45.1% / 0% / 0% / 54.9% | 1 | 13.42s | 19.41s | 2 (2/0) | 2.62s |

The quality percentages are **time-weighted** — the share of the window actually spent at
each level. `connection_quality_changed` fires only on a change, so counting the events
weights the result by number of transitions instead, and the two are simply unrelated
quantities. In the `severe_outage` window above, event counting reads 50.0% LOST against
54.9% by time; across the eleven recorded `severe_outage` windows the event-count reading
lands on 50.0% ten times and 100.0% once, while the time-weighted figure ranges from 41.2%
to 55.3%. The error is not consistently in one direction — an earlier revision of this
README said the event-count version flattered the result, and that is only true of the
windows it happened to be written from. It is unstable in both directions, which is the
actual reason not to report it.

A few things worth noting in that data:

- A short ~4s `gateway_dropout` never trips LiveKit's client-side reconnect logic (0
  reconnects) — it only shows up as a connection-quality dip and concealed audio. It took a
  continuous 25s outage (`severe_outage`) to actually force a `reconnecting` → `reconnected`
  cycle, which then took ~12s to recover — real numbers for a mechanism LiveKit ships but
  doesn't otherwise expose.
- `concealed_samples` is a more sensitive signal than the connection-quality label for
  milder profiles: `rural_4g` shows measurable concealed audio while its quality label
  stayed "Excellent" the whole window — consistent with LiveKit's own `ConnectionQuality`
  scorer excluding jitter/RTT from its score (see "Why this exists" above).
- **Turn count, not turn failure, is what degrades.** `turns_failed` counts turns the VAD
  opened but never completed, and across the 68 recorded profile windows it has been
  non-zero exactly once. When audio stops arriving the VAD has nothing to fail on — it
  simply never opens a turn. `severe_outage` above shows 2 turns where `clean` shows 7, and
  that drop is the real signal. The report shows detected count alongside the ok/failed
  split for exactly this reason; the split alone reads "2 (2/0)" and looks like a clean
  sweep.
- Computing concealed-audio duration correctly across a `severe_outage` window took a fix:
  a full reconnect re-subscribes to a **new track SID** whose cumulative WebRTC counters
  reset to 0, so a naive "last value at window end minus last value at window start" diff
  crosses that SID boundary, goes negative, and silently reports 0 — hiding the ~19s of real
  concealment that happened on the old track right before it was torn down. The report
  generator now diffs each track SID separately and sums across SID changes within a window.
- An interrupted run never leaves a fault applied. Each fault script traps its signals and
  lets Pumba restore the netem rules it set; the suite additionally forwards a signal to
  the fault script it is waiting on and sweeps any Pumba container that outlived its
  parent. Without the second half, killing the suite directly (rather than Ctrl-C, which
  the terminal sends to the whole process group) left 166ms of injected latency applied to
  `client-a` indefinitely, silently degrading every later run.
- Profiles are separated by a `COOLDOWN_SECONDS` (default 10s) gap that sits outside every
  measured window. Back-to-back profiles let one fault's tail land in the next profile's
  numbers: LiveKit's bandwidth estimator ramps back up over seconds, the jitter buffer is
  still draining, and a track torn down at the end of a fault may not be resubscribed yet.

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
`rural_4g` or `congested_wifi` are the right profiles for this comparison, not those.

**This harness cannot currently resolve RED's effect, and it is worth being straight about
that.** Three 60s `rural_4g` runs per arm, only RED toggled, concealed audio in seconds:

| RED | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| on (default) | 1.08s | 1.74s | 0.02s | 1.08s |
| off | 2.28s | 0.99s | 1.37s | 1.37s |

The medians differ by 1.3x, but the distributions overlap: the best RED-off run beat two of
the three RED-on runs. Run-to-run variance under 3.5% *random* loss is simply larger than
the effect at this sample size and duration, so no honest conclusion about RED can be drawn
from it either way. An earlier revision of this README reported an 8x reduction here. That
came from a single run per arm and does not survive repetition — it is withdrawn.

Resolving this properly needs the measurement to change, not just more patience: many more
repetitions per arm, longer windows, and ideally a deterministic loss pattern rather than a
random one, so both arms see the same losses. LiveKit's own
[claim](https://livekit.com/blog/audio-quality) that RED lets audio tolerate "~20–30% packet
loss without retransmission" is also made at far higher loss rates than the 3.5% this
profile injects, where there is more for RED to recover. The `RED_ENABLED` toggle and the
comparison harness are real and work; the experiment run through them is underpowered.

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

What the agent leg actually detects under fault, in order of how reliably it fires:

1. **Fewer turns.** The dominant signal. `severe_outage` yields 2 turns where `clean`
   yields 5 — during a blackout the VAD receives no frames, so it never opens a turn at
   all.
2. **Shorter turns.** A real utterance the agent should hear as one continuous turn gets
   cut short mid-sentence when frames stop arriving partway through it. A tightly-timed
   `gateway_dropout` run (interval=15s instead of the default 45s, so multiple dropouts
   land mid-window rather than once at a profile boundary) isolates this:

   | Profile | Agent turns (ok/failed) | Avg turn length | Concealed audio |
   |---|---|---|---|
   | clean | 6/0 | 4.24s | 0.02s |
   | gateway_dropout (interval=15s) | 5/0 | 3.28s | 12.47s |

3. **Outright turn failure** — a turn the VAD opens and never completes. This is the
   rarest of the three: non-zero in 1 of 68 recorded profile windows (a `rural_4g` run
   that failed 1 of 10 turns). Real, but not something to build a demo around. (Two
   earlier example runs cited here in a previous revision are no longer in the manifest
   this repo ships — this figure is recomputed from the data that's actually here.)

**Response latency** is reported as the gap from the speaker's last word to the agent's
first response frame — VAD detection hold included. It reads ~577ms and is nearly constant
across every profile, which is honest rather than surprising: Silero's default
`min_silence_duration` is 0.55s and dominates the number, and this synthetic agent's
"response" is generated locally, so nothing in the path depends on the network. The
per-event JSON breaks out `publish_latency_ms` and `vad_silence_hold_ms` separately so a
regression in the agent's own code path stays visible against that fixed hold. Once real
STT/LLM/TTS plugins are swapped in, this is the column that starts moving.

One caveat is guarded rather than assumed away. The VAD measures silence in *audio* time,
so if frames stop arriving altogether it stalls instead of accumulating silence, and
inferring wall-clock end-of-speech from it would understate the real wait. Each event
therefore also carries `since_last_frame_ms`, the wall-clock gap since the VAD last
received a frame — a large value means that row's latency is distorted by a stall rather
than a genuinely fast response. Across the 3,931 recorded turns, 3,863 stay within
±10ms; the rest is not scattered noise but a single contiguous run (every turn of every
profile in one specific run) reading close to **-1000ms**. That run coincided with heavy
concurrent load on the host running the suite, which is the more likely explanation than
a code defect — but it is a real recorded value, not a hypothetical, and it is evidence
the guard *can* go meaningfully negative rather than only large-positive, which is worth
knowing before trusting a "the guard has never fired" claim at face value.

Three bugs surfaced building this, all fixed before trusting the numbers above:
- The first version measured "response latency" as time-to-*finish-playing* the whole 1s
  response tone (always ≥1000ms) instead of time-to-*first-frame-pushed*.
- The fix for that overcorrected: timing from the moment the `END_OF_SPEECH` event was
  *consumed* excluded the VAD's own detection hold, leaving a metric that measured only
  frame allocation. It read 0.40–1.18ms across a whole run with no separation between the
  clean profile and a 25s outage. Now measured from the inferred end of speech
  (`event.silence_duration` before the event fires), which is the gap a caller actually
  experiences.
- A turn that starts in one profile's window but completes a moment into the next one was
  originally counted as a failure in the first window, purely because of where the fault
  boundary happened to fall — not a real failure. Fixed by pairing turns across the full
  event stream and attributing each one to whichever window it *started* in, the same fix
  already applied to `concealed_samples` across a track-SID change.
- That pairing then had to be bounded to the run. The data logs are append-only across
  runs, so a turn left open at the end of one run stayed open and swallowed the *next*
  run's first `response_published`, anchoring a real turn at a timestamp outside every
  window of the run it belonged to. It cost the first profile of most runs one completed
  turn — `clean` read 5 where 6 had actually happened. Reconnect and agent-drop pairing had
  the same hole. The bound is the *next run's* start_ts from the manifest, not the current
  run's own end_ts: `severe_outage` is the last profile in the suite, its agent recovery
  takes 9-16s, and in three of the twelve recorded runs that recovery genuinely lands
  after the window's own end_ts (by 0.6-3.7s) — which is what made the tighter bound wrong
  for exactly those three, reporting "none recovered in-run" for a drop the agent
  demonstrably came back from.

The VAD model is loaded once per worker process via `WorkerOptions(prewarm_fnc=...)`, not
per track subscription. `silero.VAD.load()` is a blocking call whose own docstring points
at prewarm; loading it inside the per-track handler put a synchronous model load on the
event loop at every reconnect, which is exactly when the agent-recovery metric is timed.

## Status

Done: local stack, all five fault profiles (verified against real ping/iperf3
measurements, not just their config), client + webhook metrics capture, the run
orchestrator and report generator, and the synthetic agent leg above.

Built but underpowered: the RED on/off comparison. The toggle and the harness around it
work; the experiment run through them cannot resolve RED's effect above run-to-run
variance at three 60s runs per arm. See
[Measuring RED's actual effect](#measuring-reds-actual-effect) — it needs more repetitions
and a deterministic loss pattern before it says anything.

Not done: swapping the synthetic agent's local VAD + synthetic-tone stand-in for real
STT/LLM/TTS plugins — a drop-in change once API keys are available, not a redesign. Video
testing, multi-region testing, and Prometheus/Grafana export are explicitly out of scope
for this version (audio-only, single-machine, JSON/HTML reports are enough to prove the
concept).
