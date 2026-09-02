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


def last_value_at_or_before(track_stats_events, ts, field):
    candidates = [e for e in track_stats_events if e["ts"] <= ts]
    if not candidates:
        return 0
    return max(candidates, key=lambda e: e["ts"]).get(field, 0)


def concealment_stats(client_b_events, start_ts, end_ts):
    stats_events = [e for e in client_b_events if e["event"] == "track_stats"]
    concealed_delta = max(
        0,
        last_value_at_or_before(stats_events, end_ts, "concealed_samples")
        - last_value_at_or_before(stats_events, start_ts, "concealed_samples"),
    )
    silent_delta = max(
        0,
        last_value_at_or_before(stats_events, end_ts, "silent_concealed_samples")
        - last_value_at_or_before(stats_events, start_ts, "silent_concealed_samples"),
    )
    return {
        "concealed_samples": concealed_delta,
        "silent_concealed_samples": silent_delta,
        "concealed_seconds_approx": round(concealed_delta / SAMPLE_RATE, 2),
    }


def time_to_first_connect(events_by_client):
    result = {}
    for identity, events in events_by_client.items():
        for e in events:
            if e["event"] == "connected":
                result[identity] = round(e.get("time_to_first_connect_ms", 0), 1)
                break
    return result


def build_summary(run_id, manifest_entries, client_a_events, client_b_events):
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
    lines.append("| Profile | Duration | Quality (Excellent/Good/Poor/Lost) | Reconnects | Avg recovery | Concealed audio |")
    lines.append("|---|---|---|---|---|---|")
    for p in summary["profiles"]:
        pct = p["quality"]["percent"]
        quality_str = " / ".join(f"{pct.get(k, 0)}%" for k in ("EXCELLENT", "GOOD", "POOR", "LOST"))
        rc = p["reconnects"]
        recovery = f"{rc['avg_recovery_time_s']}s" if rc["avg_recovery_time_s"] is not None else "—"
        concealed = f"{p['concealment']['concealed_seconds_approx']}s"
        lines.append(
            f"| {p['profile']} | {p['duration_s']}s | {quality_str} | {rc['reconnect_count']} | {recovery} | {concealed} |"
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
        rows.append(
            f"""
        <tr>
          <td>{p['profile']}</td>
          <td>{p['duration_s']}s</td>
          <td>{quality_str}</td>
          <td>{rc['reconnect_count']}</td>
          <td>{recovery}</td>
          <td>{concealed}</td>
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
  table {{ border-collapse: collapse; margin-top: 1rem; background: white; }}
  th, td {{ border: 1px solid #ddd; padding: 0.5rem 0.8rem; text-align: left; }}
  th {{ background: #f0f0f0; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  .meta {{ color: #555; }}
</style>
</head>
<body>
  <h1>LiveKit Resilience Report — run {summary['run_id']}</h1>
  <p class="meta">Time to first connect: {ttfc_str}</p>
  <table>
    <tr><th>Profile</th><th>Duration</th><th>Quality (Excellent/Good/Poor/Lost)</th><th>Reconnects</th><th>Avg recovery</th><th>Concealed audio</th></tr>
    {''.join(rows)}
  </table>
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

    summary = build_summary(run_id, manifest_entries, client_a_events, client_b_events)

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
