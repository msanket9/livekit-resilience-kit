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
import json
import os
from collections import defaultdict

QUALITY_LABELS = {"0": "POOR", "1": "GOOD", "2": "EXCELLENT", "3": "LOST"}
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


def quality_distribution(client_b_events, start_ts, end_ts):
    """Count connection_quality_changed samples for client-a as observed by
    client-b within [start_ts, end_ts]. connection_quality_changed only
    fires on a *change*, so a profile whose quality never changes during the
    window produces zero in-window events -- that's carried forward as a
    single sample of whatever quality was last known before the window
    started, so a steady window reads as "steady at X", not "no data"."""
    quality_events = [
        e
        for e in client_b_events
        if e["event"] == "connection_quality_changed" and e.get("participant") == "client-a"
    ]
    counts = defaultdict(int)

    prior = [e for e in quality_events if e["ts"] < start_ts]
    if prior:
        counts[QUALITY_LABELS.get(prior[-1]["quality"], prior[-1]["quality"])] += 1

    for e in in_window(quality_events, start_ts, end_ts):
        counts[QUALITY_LABELS.get(e["quality"], e["quality"])] += 1

    total = sum(counts.values())
    return {
        "counts": dict(counts),
        "total_samples": total,
        "percent": {k: round(100 * v / total, 1) for k, v in counts.items()} if total else {},
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
    by start_ts afterward avoids it."""
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
        elif e["event"] == "speech_end" and open_start is not None:
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
    a fault window, not as a failed/unmatched turn."""
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


def webhook_rejoin_stats(webhook_events, start_ts, end_ts, participant="client-a"):
    """Server-side (LiveKit webhook) cross-check of participant/track churn
    within [start_ts, end_ts] -- a signal independent of the client's own
    self-reported reconnecting/reconnected events, since the server is the
    authority on whether a participant actually left and rejoined the room.
    Only room_started's initial joins land before any profile window starts,
    so a non-zero participant_joined count here means a real server-observed
    rejoin happened during this specific fault."""
    events = [e for e in webhook_events if start_ts <= e.get("received_ts", 0) <= end_ts]

    def kind(e):
        return e.get("event", {}).get("event")

    def event_participant(e):
        return e.get("event", {}).get("participant", {}).get("identity")

    def track_participant(e):
        return e.get("event", {}).get("participant", {}).get("identity")

    return {
        "participant_joined": sum(1 for e in events if kind(e) == "participant_joined" and event_participant(e) == participant),
        "participant_left": sum(1 for e in events if kind(e) == "participant_left" and event_participant(e) == participant),
        "track_published": sum(1 for e in events if kind(e) == "track_published" and track_participant(e) == participant),
        "track_unpublished": sum(1 for e in events if kind(e) == "track_unpublished" and track_participant(e) == participant),
    }


def _pair_agent_drops(agent_events):
    """Pairs each time the agent lost its subscription to client-a's track
    (track_unsubscribed) with the agent's next successfully completed turn
    (response_published) that follows -- the agent's own "time-to-recover
    after a drop", distinct from reconnect_stats: that measures the
    client's connection-level recovery, this measures whether the agent's
    turn-taking pipeline actually resumed working. Computed over the FULL
    stream, not window-restricted, for the same boundary-truncation reason
    as _pair_agent_turns."""
    events = sorted(agent_events, key=lambda e: e["ts"])
    drops = []
    pending_drop_ts = None
    for e in events:
        if e["event"] == "track_unsubscribed" and e.get("participant") == "client-a":
            pending_drop_ts = e["ts"]
        elif e["event"] == "response_published" and pending_drop_ts is not None:
            drops.append({"drop_ts": pending_drop_ts, "recovery_s": e["ts"] - pending_drop_ts})
            pending_drop_ts = None
    return drops


def agent_recovery_stats(agent_events, start_ts, end_ts):
    """A drop belongs to a profile window if the track_unsubscribed that
    started it happened within [start_ts, end_ts], regardless of when
    recovery completed (the same "belongs to where it started" convention
    as agent_stats)."""
    drops = [d for d in _pair_agent_drops(agent_events) if start_ts <= d["drop_ts"] <= end_ts]
    recoveries = [d["recovery_s"] for d in drops]
    return {
        "drop_count": len(drops),
        "avg_recovery_s": round(sum(recoveries) / len(recoveries), 2) if recoveries else None,
    }


def time_to_first_connect(events_by_client):
    result = {}
    for identity, events in events_by_client.items():
        for e in events:
            if e["event"] == "connected":
                result[identity] = round(e.get("time_to_first_connect_ms", 0), 1)
                break
    return result


def build_summary(run_id, manifest_entries, client_a_events, client_b_events, agent_events, webhook_events):
    profiles = []
    for entry in sorted(manifest_entries, key=lambda e: e["start_ts"]):
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
                "agent_recovery": agent_recovery_stats(agent_events, start_ts, end_ts),
                "webhook_rejoins": webhook_rejoin_stats(webhook_events, start_ts, end_ts),
            }
        )
    return {
        "run_id": run_id,
        "time_to_first_connect_ms": time_to_first_connect(
            {"client-a": client_a_events, "client-b": client_b_events}
        ),
        "profiles": profiles,
    }


def render_markdown(summary):
    lines = [f"# LiveKit Resilience Report — run {summary['run_id']}", ""]
    ttfc = summary["time_to_first_connect_ms"]
    lines.append("**Time to first connect:** " + ", ".join(f"{k}={v}ms" for k, v in ttfc.items()))
    lines.append("")
    lines.append(
        "| Profile | Duration | Quality (Excellent/Good/Poor/Lost) | Reconnects | Avg recovery | "
        "Server rejoins | Concealed audio | Agent turns (ok/failed) | Avg turn length | "
        "Avg response latency | Agent recovery |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for p in summary["profiles"]:
        pct = p["quality"]["percent"]
        quality_str = " / ".join(f"{pct.get(k, 0)}%" for k in ("EXCELLENT", "GOOD", "POOR", "LOST"))
        rc = p["reconnects"]
        recovery = f"{rc['avg_recovery_time_s']}s" if rc["avg_recovery_time_s"] is not None else "—"
        concealed = f"{p['concealment']['concealed_seconds_approx']}s"
        ag = p["agent"]
        turns = f"{ag['turns_completed']}/{ag['turns_failed']}"
        turn_len = f"{ag['avg_speech_duration_s']}s" if ag["avg_speech_duration_s"] is not None else "—"
        latency = f"{ag['avg_response_latency_ms']}ms" if ag["avg_response_latency_ms"] is not None else "—"
        wh = p["webhook_rejoins"]
        server_rejoins = str(wh["participant_joined"])
        agrec = p["agent_recovery"]
        agent_recovery_str = (
            f"{agrec['drop_count']}x, avg {agrec['avg_recovery_s']}s" if agrec["avg_recovery_s"] is not None else "—"
        )
        lines.append(
            f"| {p['profile']} | {p['duration_s']}s | {quality_str} | {rc['reconnect_count']} | {recovery} | "
            f"{server_rejoins} | {concealed} | {turns} | {turn_len} | {latency} | {agent_recovery_str} |"
        )
    return "\n".join(lines) + "\n"


def render_html(summary):
    ttfc = summary["time_to_first_connect_ms"]
    ttfc_str = ", ".join(f"{k}={v}ms" for k, v in ttfc.items())
    rows = []
    for p in summary["profiles"]:
        pct = p["quality"]["percent"]
        quality_str = " / ".join(f"{pct.get(k, 0)}%" for k in ("EXCELLENT", "GOOD", "POOR", "LOST"))
        rc = p["reconnects"]
        recovery = f"{rc['avg_recovery_time_s']}s" if rc["avg_recovery_time_s"] is not None else "—"
        concealed = f"{p['concealment']['concealed_seconds_approx']}s"
        ag = p["agent"]
        turns = f"{ag['turns_completed']}/{ag['turns_failed']}"
        turn_len = f"{ag['avg_speech_duration_s']}s" if ag["avg_speech_duration_s"] is not None else "—"
        latency = f"{ag['avg_response_latency_ms']}ms" if ag["avg_response_latency_ms"] is not None else "—"
        wh = p["webhook_rejoins"]
        server_rejoins = str(wh["participant_joined"])
        agrec = p["agent_recovery"]
        agent_recovery_str = (
            f"{agrec['drop_count']}x, avg {agrec['avg_recovery_s']}s" if agrec["avg_recovery_s"] is not None else "—"
        )
        rows.append(
            f"""
        <tr>
          <td>{p['profile']}</td>
          <td>{p['duration_s']}s</td>
          <td>{quality_str}</td>
          <td>{rc['reconnect_count']}</td>
          <td>{recovery}</td>
          <td>{server_rejoins}</td>
          <td>{concealed}</td>
          <td>{turns}</td>
          <td>{turn_len}</td>
          <td>{latency}</td>
          <td>{agent_recovery_str}</td>
        </tr>"""
        )
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>LiveKit Resilience Report — {summary['run_id']}</title>
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
  <h1>LiveKit Resilience Report — run {summary['run_id']}</h1>
  <p class="meta">Time to first connect: {ttfc_str}</p>
  <div class="table-scroll">
  <table>
    <tr>
      <th>Profile</th><th>Duration</th><th>Quality (Excellent/Good/Poor/Lost)</th><th>Reconnects</th>
      <th>Avg recovery</th><th>Server rejoins</th><th>Concealed audio</th><th>Agent turns (ok/failed)</th>
      <th>Avg turn length</th><th>Avg response latency</th><th>Agent recovery</th>
    </tr>
    {''.join(rows)}
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
