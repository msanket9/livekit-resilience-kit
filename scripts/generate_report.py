#!/usr/bin/env python3
"""Turns the raw JSONL event logs into a clean-vs-degraded comparison report.

Usage:
  python3 scripts/generate_report.py [--run-id RUN_ID]

Reads data/run_manifest.jsonl for a run's profile time windows (default: the
most recent run_id present), slices data/client-a-events.jsonl,
data/client-b-events.jsonl by each window, and writes
reports/report-<run_id>.{json,md,html}.
"""

import argparse
import html
import json
import os
from collections import defaultdict

QUALITY_LABELS = {"0": "POOR", "1": "GOOD", "2": "EXCELLENT", "3": "LOST"}
QUALITY_ORDER = ("EXCELLENT", "GOOD", "POOR", "LOST")
SAMPLE_RATE = 48000

def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def load_manifest(path, run_id):
    entries = read_jsonl(path)
    if not entries:
        raise SystemExit(f"no run manifest found at {path} -- run scripts/run_test_suite.sh first")
    if run_id is None:
        run_id = entries[-1]["run_id"]
    profiles = [e for e in entries if e["run_id"] == run_id]
    if not profiles:
        raise SystemExit(f"no manifest entries for run_id={run_id}")
    return run_id, profiles


def in_window(events, start_ts, end_ts):
    return [e for e in events if start_ts <= e["ts"] <= end_ts]


def _quality_label(event):
    return QUALITY_LABELS.get(event["quality"], event["quality"])


def quality_distribution(client_b_events, start_ts, end_ts, participant="client-a"):
    """Time-weighted share of [start_ts, end_ts] spent at each ConnectionQuality
    level, for `participant` as observed by client-b.

    connection_quality_changed fires only on a *change*, so counting events
    weights the result by number of transitions rather than by time -- and the
    two disagree badly. A severe_outage window that went LOST once and stayed
    there for 55% of its duration reads as "33.3% LOST" by event count, because
    the window happened to contain three transitions and one of them was the
    drop. Integrating the duration actually held at each level instead.

    The level in force at start_ts is carried forward from the last change
    before the window, so a window with no transitions at all reads as "steady
    at X" rather than "no data". If nothing preceded the window the level is
    genuinely unknown, and is reported as UNKNOWN rather than silently
    attributed to a level.
    """
    quality_events = sorted(
        (
            e
            for e in client_b_events
            if e["event"] == "connection_quality_changed" and e.get("participant") == participant
        ),
        key=lambda e: e["ts"],
    )

    prior = [e for e in quality_events if e["ts"] < start_ts]
    current = _quality_label(prior[-1]) if prior else "UNKNOWN"

    durations = defaultdict(float)
    cursor = start_ts
    transitions = 0
    for e in in_window(quality_events, start_ts, end_ts):
        durations[current] += e["ts"] - cursor
        cursor = e["ts"]
        current = _quality_label(e)
        transitions += 1
    durations[current] += end_ts - cursor

    total = sum(durations.values())
    return {
        "seconds": {k: round(v, 2) for k, v in durations.items()},
        "window_seconds": round(total, 2),
        "transitions": transitions,
        "percent": {k: round(100 * v / total, 1) for k, v in durations.items()} if total else {},
    }


def reconnect_stats(client_a_events, start_ts, end_ts):
    events = sorted(in_window(client_a_events, start_ts, end_ts), key=lambda e: e["ts"])
    pending = []
    recoveries = []
    for e in events:
        if e["event"] == "reconnecting":
            pending.append(e["ts"])
        elif e["event"] == "reconnected" and pending:
            started = pending.pop(0)
            recoveries.append(e["ts"] - started)
    return {
        "reconnect_count": len(recoveries) + len(pending),
        "recovered_count": len(recoveries),
        "avg_recovery_time_s": round(sum(recoveries) / len(recoveries), 2) if recoveries else None,
        "unrecovered_within_window": len(pending),
    }


def _sid_delta(events_for_sid, start_ts, end_ts, field):
    """Delta of a cumulative per-track-sid counter within [start_ts, end_ts],
    baselined against the last sample before start_ts (0 if the sid has no
    sample before the window, i.e. it was (re)subscribed inside it)."""
    events_for_sid = sorted(events_for_sid, key=lambda e: e["ts"])
    relevant = [e for e in events_for_sid if e["ts"] <= end_ts]
    if not relevant or relevant[-1]["ts"] < start_ts:
        return 0
    before = [e for e in relevant if e["ts"] < start_ts]
    baseline = before[-1].get(field, 0) if before else 0
    return max(0, relevant[-1].get(field, 0) - baseline)


def concealment_stats(client_b_events, start_ts, end_ts, participant="client-a"):
    """Sum of concealed-sample deltas within [start_ts, end_ts] for one
    participant's track, computed per subscribed-track-sid and summed
    across sids.

    Since the agent leg was added, client-b subscribes to TWO audio tracks
    (client-a's speech and the agent's response tone) and polls track_stats
    on both. Filtering to participant="client-a" matters: rural_4g shapes
    livekit-server's own egress (its downlink-cap half), which affects every
    track relayed through the server -- including the agent's response
    track -- so an unfiltered sum would silently blend concealment from a
    track that isn't the one this metric is meant to measure.

    A full LiveKit reconnect re-subscribes to a NEW track sid whose
    cumulative WebRTC counters reset to 0. Naively diffing "last value at
    end" minus "last value at start" across that sid change goes negative
    and gets clamped to 0 -- silently hiding real concealment that happened
    on the old track right before it was torn down. Diffing per-sid and
    summing avoids that."""
    stats_events = [
        e for e in client_b_events if e["event"] == "track_stats" and e.get("participant") == participant
    ]
    by_sid = defaultdict(list)
    for e in stats_events:
        by_sid[e["track_sid"]].append(e)

    concealed_total = sum(
        _sid_delta(events, start_ts, end_ts, "concealed_samples") for events in by_sid.values()
    )
    silent_total = sum(
        _sid_delta(events, start_ts, end_ts, "silent_concealed_samples") for events in by_sid.values()
    )
    return {
        "concealed_samples": concealed_total,
        "silent_concealed_samples": silent_total,
        "concealed_seconds_approx": round(concealed_total / SAMPLE_RATE, 2),
    }


def _pair_agent_turns(agent_events):
    """Pairs speech_start -> speech_end -> response_published across the
    FULL event stream, tagging each turn with its speech_start timestamp.

    Deliberately NOT window-restricted: a turn that starts just before a
    fault profile's window ends and completes a moment later (a real,
    successful turn) must not be counted as failed just because a profile
    boundary happened to fall in the middle of it -- the same class of
    boundary-truncation bug already hit and fixed for concealed_samples
    across a track-SID change. Pairing over the whole stream and filtering
    by start_ts afterward avoids it.

    A speech_end with no open speech_start means the log simply begins
    mid-utterance (the agent's VAD was already inside a turn when logging
    started -- real data shows 27 speech_end against 26 speech_start). That
    turn is anchored at its speech_end rather than discarded, which would
    also have thrown away the perfectly good response_published that
    followed it."""
    events = sorted(agent_events, key=lambda e: e["ts"])
    turns = []
    open_start = None
    open_duration = None
    for e in events:
        if e["event"] == "speech_start":
            if open_start is not None:
                turns.append({"start_ts": open_start, "duration": open_duration, "latency_ms": None})
            open_start = e["ts"]
            open_duration = None
        elif e["event"] == "speech_end":
            if open_start is None:
                open_start = e["ts"]
            open_duration = e.get("speech_duration")
        elif e["event"] == "response_published" and open_start is not None:
            turns.append(
                {"start_ts": open_start, "duration": open_duration, "latency_ms": e.get("response_latency_ms")}
            )
            open_start = None
            open_duration = None
    if open_start is not None:
        turns.append({"start_ts": open_start, "duration": open_duration, "latency_ms": None})
    return turns


def agent_stats(agent_events, start_ts, end_ts):
    """A turn belongs to a profile window if it STARTED within
    [start_ts, end_ts], regardless of when it completed. A turn only counts
    as completed once speech_start -> speech_end -> response_published all
    land -- one that never gets a response_published (VAD never detected
    end-of-speech, or a new speech_start interrupted it first) is a genuine
    turn-taking failure.

    avg_speech_duration_s is worth watching on its own, not just
    success/failure: under packet loss the VAD can still complete a turn but
    detect a truncated one -- a real utterance the agent should have heard
    as one continuous turn gets cut short mid-sentence when frames stop
    arriving mid-utterance. That shows up as a lower average duration during
    a fault window, not as a failed/unmatched turn.

    turns_detected is reported alongside the success/failure split for the
    same reason: across every run so far a degraded profile shows up as
    *fewer turns detected*, not as failed ones -- when the audio stops
    arriving the VAD has nothing to fail on, it simply never opens a turn.
    A row reading "1/0" is far worse than one reading "5/0", and the
    ok/failed split alone does not say so."""
    turns = [t for t in _pair_agent_turns(agent_events) if start_ts <= t["start_ts"] <= end_ts]
    completed = [t for t in turns if t["latency_ms"] is not None]
    durations = [t["duration"] for t in completed if t["duration"] is not None]
    return {
        "turns_detected": len(turns),
        "turns_completed": len(completed),
        "turns_failed": len(turns) - len(completed),
        "avg_speech_duration_s": round(sum(durations) / len(durations), 2) if durations else None,
        "avg_response_latency_ms": round(sum(t["latency_ms"] for t in completed) / len(completed), 2)
        if completed
        else None,
    }


def _webhook_ts(e):
    """Best estimate of when the LiveKit server actually emitted this event.

    Two imperfect readings are available. `received_ts` is when this box logged
    the POST: precise (float), but inflated by any delivery lag -- and rural_4g
    rate-limits livekit-server's own egress, which shapes webhook delivery too.
    `created_at` is the server's own timestamp: authoritative, but Unix
    *seconds*, truncated, so the true event time lies somewhere in
    [created_at, created_at + 1).

    Using created_at alone is wrong at a window edge. An event genuinely 0.3s
    into a profile window truncates to the second below it, falls before
    start_ts, and is counted in no window at all -- silently losing exactly the
    server-observed rejoin this metric exists to catch. Measured on real data,
    delivery here is prompt: the gap between the two readings never exceeds the
    truncation itself.

    So: when received_ts falls inside the second created_at names, delivery was
    prompt and received_ts is simply the more precise reading of the same
    instant -- use it. Only when received_ts lands outside that second has
    delivery actually lagged, and the server's own (coarser) timestamp is the
    better answer."""
    received = e.get("received_ts")
    raw_created = e.get("event", {}).get("created_at")
    try:
        created = float(raw_created) if raw_created is not None else None
    except (TypeError, ValueError):
        created = None

    if created is None:
        return received if received is not None else 0
    if received is not None and created <= received < created + 1:
        return received
    return created


def webhook_rejoin_stats(webhook_events, start_ts, end_ts, participant="client-a"):
    """Server-side (LiveKit webhook) cross-check of participant/track churn
    within [start_ts, end_ts] -- a signal independent of the client's own
    self-reported reconnecting/reconnected events, since the server is the
    authority on whether a participant actually left and rejoined the room.
    Only room_started's initial joins land before any profile window starts,
    so a non-zero participant_joined count here means a real server-observed
    rejoin happened during this specific fault.

    Deduplicated by the server-assigned event id: LiveKit retries a webhook
    delivery that fails or times out, and a retry carries the same id. Without
    this a delivery retried during a fault would be counted as a second
    server-observed rejoin -- inflating the one signal in the report that
    exists specifically to be the trustworthy cross-check."""
    seen = set()
    events = []
    for e in webhook_events:
        if not (start_ts <= _webhook_ts(e) <= end_ts):
            continue
        event_id = e.get("event", {}).get("id")
        if event_id is not None:
            if event_id in seen:
                continue
            seen.add(event_id)
        events.append(e)

    def kind(e):
        return e.get("event", {}).get("event")

    def event_participant(e):
        return e.get("event", {}).get("participant", {}).get("identity")

    def count(event_kind):
        return sum(1 for e in events if kind(e) == event_kind and event_participant(e) == participant)

    return {
        "participant_joined": count("participant_joined"),
        "participant_left": count("participant_left"),
        "track_published": count("track_published"),
        "track_unpublished": count("track_unpublished"),
    }


def _pair_agent_drops(agent_events, recovery_deadline):
    """Pairs each time the agent lost its subscription to client-a's track
    (track_unsubscribed) with the agent's next successfully completed turn
    (response_published) that follows -- the agent's own "time-to-recover
    after a drop", distinct from reconnect_stats: that measures the
    client's connection-level recovery, this measures whether the agent's
    turn-taking pipeline actually resumed working.

    Pairing runs over the FULL stream rather than a single profile window, for
    the same boundary-truncation reason as _pair_agent_turns: a drop late in
    one profile that recovers early in the next really did recover, and the
    elapsed time is real. It is bounded at `recovery_deadline`, the end of the
    run being reported. The data logs are append-only across runs by design, so
    without that bound a drop left unrecovered at the end of one run pairs with
    the first response of the *next* run -- reporting the gap between two runs
    as agent recovery time. A response after the run ended is not this run's
    recovery.

    A drop that never recovers is emitted with recovery_s=None rather than
    dropped on the floor. The previous version only appended on a successful
    pairing, so a drop the agent never came back from contributed nothing to
    drop_count -- the worst possible outcome rendered as no outcome at all, and
    inconsistent with reconnect_stats, which has always tracked its unrecovered
    pending list."""
    events = sorted(agent_events, key=lambda e: e["ts"])
    drops = []
    pending_drop_ts = None
    for e in events:
        if e["ts"] > recovery_deadline:
            break
        if e["event"] == "track_unsubscribed" and e.get("participant") == "client-a":
            if pending_drop_ts is not None:
                drops.append({"drop_ts": pending_drop_ts, "recovery_s": None})
            pending_drop_ts = e["ts"]
        elif e["event"] == "response_published" and pending_drop_ts is not None:
            drops.append({"drop_ts": pending_drop_ts, "recovery_s": e["ts"] - pending_drop_ts})
            pending_drop_ts = None
    if pending_drop_ts is not None:
        drops.append({"drop_ts": pending_drop_ts, "recovery_s": None})
    return drops


def agent_recovery_stats(agent_events, start_ts, end_ts, run_end_ts):
    """A drop belongs to a profile window if the track_unsubscribed that
    started it happened within [start_ts, end_ts], regardless of when
    recovery completed (the same "belongs to where it started" convention
    as agent_stats). Recovery is only counted if it landed before the run
    ended -- see _pair_agent_drops."""
    drops = [
        d for d in _pair_agent_drops(agent_events, run_end_ts) if start_ts <= d["drop_ts"] <= end_ts
    ]
    recoveries = [d["recovery_s"] for d in drops if d["recovery_s"] is not None]
    return {
        "drop_count": len(drops),
        "recovered_count": len(recoveries),
        "unrecovered_within_run": len(drops) - len(recoveries),
        "avg_recovery_s": round(sum(recoveries) / len(recoveries), 2) if recoveries else None,
    }


def time_to_first_connect(events_by_client, run_start_ts):
    """Time-to-first-connect for the session that was actually live during
    this run: the LAST `connected` at or before the run started.

    Taking the first `connected` in the file reports a connect from whatever
    container lifetime happened to write the log first -- possibly days old
    and belonging to an entirely different run, since the suite never clears
    the append-only data logs. age_s_at_run_start is carried alongside so a
    stale figure is visible as stale rather than quietly wrong."""
    result = {}
    for identity, events in events_by_client.items():
        candidates = sorted(
            (e for e in events if e["event"] == "connected" and e["ts"] <= run_start_ts),
            key=lambda e: e["ts"],
        )
        if candidates:
            chosen = candidates[-1]
            result[identity] = {
                "ms": round(chosen.get("time_to_first_connect_ms", 0), 1),
                "age_s_at_run_start": round(run_start_ts - chosen["ts"], 1),
            }
    return result


def build_summary(run_id, manifest_entries, client_a_events, client_b_events, agent_events, webhook_events):
    entries = sorted(manifest_entries, key=lambda e: e["start_ts"])
    run_end_ts = max(e["end_ts"] for e in entries)
    profiles = []
    for entry in entries:
        start_ts, end_ts = entry["start_ts"], entry["end_ts"]
        profiles.append(
            {
                "profile": entry["profile"],
                "duration_s": entry["duration_s"],
                "start_ts": start_ts,
                "end_ts": end_ts,
                "quality": quality_distribution(client_b_events, start_ts, end_ts),
                "reconnects": reconnect_stats(client_a_events, start_ts, end_ts),
                "concealment": concealment_stats(client_b_events, start_ts, end_ts),
                "agent": agent_stats(agent_events, start_ts, end_ts),
                "agent_recovery": agent_recovery_stats(agent_events, start_ts, end_ts, run_end_ts),
                "webhook_rejoins": webhook_rejoin_stats(webhook_events, start_ts, end_ts),
            }
        )
    return {
        "run_id": run_id,
        "time_to_first_connect": time_to_first_connect(
            {"client-a": client_a_events, "client-b": client_b_events}, entries[0]["start_ts"]
        ),
        "profiles": profiles,
    }


COLUMNS = (
    "Profile",
    "Duration",
    "Quality (Excellent/Good/Poor/Lost)",
    "Reconnects",
    "Avg recovery",
    "Server rejoins",
    "Concealed audio",
    "Agent turns (detected, ok/failed)",
    "Avg turn length",
    "Avg response latency",
    "Agent recovery",
)


def format_row(p):
    """The 11 rendered cells for one profile, as plain strings.

    Single source of truth for both renderers: render_markdown and
    render_html previously carried a byte-identical 15-line copy of this
    formatting, so every column change had to be made twice and stayed
    correct only by luck."""
    pct = p["quality"]["percent"]
    if set(pct) <= {"UNKNOWN"}:
        quality_str = "n/a (no quality sample before window)"
    else:
        quality_str = " / ".join(f"{pct.get(k, 0)}%" for k in QUALITY_ORDER)

    rc = p["reconnects"]
    recovery = f"{rc['avg_recovery_time_s']}s" if rc["avg_recovery_time_s"] is not None else "—"

    ag = p["agent"]
    turn_len = f"{ag['avg_speech_duration_s']}s" if ag["avg_speech_duration_s"] is not None else "—"
    latency = f"{ag['avg_response_latency_ms']}ms" if ag["avg_response_latency_ms"] is not None else "—"

    agrec = p["agent_recovery"]
    if agrec["drop_count"] == 0:
        agent_recovery_str = "—"
    elif agrec["avg_recovery_s"] is not None:
        agent_recovery_str = f"{agrec['drop_count']}x, avg {agrec['avg_recovery_s']}s"
        if agrec["unrecovered_within_run"]:
            agent_recovery_str += f" ({agrec['unrecovered_within_run']} unrecovered in-run)"
    else:
        agent_recovery_str = f"{agrec['drop_count']}x, none recovered in-run"

    return [
        p["profile"],
        f"{p['duration_s']}s",
        quality_str,
        str(rc["reconnect_count"]),
        recovery,
        str(p["webhook_rejoins"]["participant_joined"]),
        f"{p['concealment']['concealed_seconds_approx']}s",
        f"{ag['turns_detected']} ({ag['turns_completed']}/{ag['turns_failed']})",
        turn_len,
        latency,
        agent_recovery_str,
    ]


def _ttfc_str(ttfc):
    return ", ".join(f"{k}={v['ms']}ms" for k, v in ttfc.items())


def render_markdown(summary):
    lines = [f"# LiveKit Resilience Report — run {summary['run_id']}", ""]
    lines.append("**Time to first connect:** " + _ttfc_str(summary["time_to_first_connect"]))
    lines.append("")
    lines.append("| " + " | ".join(COLUMNS) + " |")
    lines.append("|" + "---|" * len(COLUMNS))
    for p in summary["profiles"]:
        lines.append("| " + " | ".join(format_row(p)) + " |")
    return "\n".join(lines) + "\n"


def render_html(summary):
    header = "".join(f"<th>{html.escape(c)}</th>" for c in COLUMNS)
    rows = "".join(
        "\n        <tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in format_row(p)) + "</tr>"
        for p in summary["profiles"]
    )
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>LiveKit Resilience Report — {html.escape(str(summary['run_id']))}</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.3rem; }}
  .table-scroll {{ overflow-x: auto; margin-top: 1rem; }}
  table {{ border-collapse: collapse; background: white; white-space: nowrap; }}
  th, td {{ border: 1px solid #ddd; padding: 0.5rem 0.8rem; text-align: left; }}
  th {{ background: #f0f0f0; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  .meta {{ color: #555; }}
</style>
</head>
<body>
  <h1>LiveKit Resilience Report — run {html.escape(str(summary['run_id']))}</h1>
  <p class="meta">Time to first connect: {html.escape(_ttfc_str(summary['time_to_first_connect']))}</p>
  <div class="table-scroll">
  <table>
    <tr>{header}</tr>{rows}
  </table>
  </div>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "reports"))
    args = parser.parse_args()

    manifest_path = os.path.join(args.data_dir, "run_manifest.jsonl")
    run_id, manifest_entries = load_manifest(manifest_path, args.run_id)

    client_a_events = read_jsonl(os.path.join(args.data_dir, "client-a-events.jsonl"))
    client_b_events = read_jsonl(os.path.join(args.data_dir, "client-b-events.jsonl"))
    agent_events = read_jsonl(os.path.join(args.data_dir, "agent-events.jsonl"))
    webhook_events = read_jsonl(os.path.join(args.data_dir, "webhooks.jsonl"))

    summary = build_summary(
        run_id, manifest_entries, client_a_events, client_b_events, agent_events, webhook_events
    )

    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, f"report-{run_id}.json")
    md_path = os.path.join(args.out_dir, f"report-{run_id}.md")
    html_path = os.path.join(args.out_dir, f"report-{run_id}.html")

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    with open(md_path, "w") as f:
        f.write(render_markdown(summary))
    with open(html_path, "w") as f:
        f.write(render_html(summary))

    print(f"wrote {json_path}\nwrote {md_path}\nwrote {html_path}")


if __name__ == "__main__":
    main()
