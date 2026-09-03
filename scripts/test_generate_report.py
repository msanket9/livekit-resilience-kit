#!/usr/bin/env python3
"""Regression tests for the report generator's metric logic.

Every case here is a bug that was actually shipped and is now fixed. The metrics
are the whole product, and each of these failed quietly -- a wrong number, never
an error -- so they are worth pinning down.

Run with:  python3 scripts/test_generate_report.py
No dependencies beyond the standard library; exits non-zero on failure.
"""

import html
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import generate_report as g

FAILURES = []


def check(name, got, want):
    if got == want:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}\n          got:  {got!r}\n          want: {want!r}")
        FAILURES.append(name)


def q(ts, quality, participant="client-a"):
    return {"ts": ts, "event": "connection_quality_changed", "participant": participant, "quality": quality}


print("quality is time-weighted, not transition-counted")
# EXCELLENT until t=102, LOST for the rest of a [100, 110] window.
events = [q(0, "2"), q(102, "3")]
check("held level dominates", g.quality_distribution(events, 100, 110)["percent"],
      {"EXCELLENT": 20.0, "LOST": 80.0})
# A window with no transitions reads as steady at the carried-forward level.
check("steady window", g.quality_distribution([q(0, "2")], 100, 110)["percent"], {"EXCELLENT": 100.0})
# Nothing before the window: unknown, not silently attributed to a level.
check("no prior sample", g.quality_distribution([], 100, 110)["percent"], {"UNKNOWN": 100.0})
check("transitions counted separately", g.quality_distribution(events, 100, 110)["transitions"], 1)

print("agent turns")
# The log can begin mid-utterance (real data: 27 speech_end vs 26 speech_start).
# That turn, and the good response after it, must not be discarded.
orphan = [{"ts": 1, "event": "speech_end", "speech_duration": 4.0},
          {"ts": 2, "event": "response_published", "response_latency_ms": 700.0}]
check("orphan speech_end kept", g.agent_stats(orphan, 0, 10)["turns_completed"], 1)
# A turn interrupted by a new speech_start never completed: a real failure.
interrupted = [{"ts": 1, "event": "speech_start"}, {"ts": 5, "event": "speech_start"},
               {"ts": 6, "event": "speech_end", "speech_duration": 1.0},
               {"ts": 7, "event": "response_published", "response_latency_ms": 600.0}]
check("interrupted turn is a failure", g.agent_stats(interrupted, 0, 10)["turns_failed"], 1)
# Turns belong to the window they STARTED in, even if they finish after it.
straddle = [{"ts": 9, "event": "speech_start"}, {"ts": 11, "event": "speech_end", "speech_duration": 2.0},
            {"ts": 12, "event": "response_published", "response_latency_ms": 600.0}]
check("turn straddling a boundary is not a failure", g.agent_stats(straddle, 0, 10)["turns_failed"], 0)

print("agent drops")
RUN_END = 1000
# A drop that never recovers is the worst outcome; it must not render as no outcome.
check("unrecovered drop is counted",
      g.agent_recovery_stats([{"ts": 5, "event": "track_unsubscribed", "participant": "client-a"}], 0, 10, RUN_END),
      {"drop_count": 1, "recovered_count": 0, "unrecovered_within_run": 1, "avg_recovery_s": None})
mixed = [{"ts": 1, "event": "track_unsubscribed", "participant": "client-a"},
         {"ts": 3, "event": "response_published", "response_latency_ms": 1.0},
         {"ts": 5, "event": "track_unsubscribed", "participant": "client-a"}]
check("one recovered, one not", g.agent_recovery_stats(mixed, 0, 10, RUN_END),
      {"drop_count": 2, "recovered_count": 1, "unrecovered_within_run": 1, "avg_recovery_s": 2.0})
# The data logs are append-only across runs: a drop at the end of one run must
# not pair with the first response of the next.
cross_run = [{"ts": 5, "event": "track_unsubscribed", "participant": "client-a"},
             {"ts": 5000, "event": "response_published", "response_latency_ms": 1.0}]
check("no pairing past the end of the run",
      g.agent_recovery_stats(cross_run, 0, 10, RUN_END)["avg_recovery_s"], None)

print("server webhooks")
def wh(received, created, event="participant_joined", eid="EV_1"):
    return {"received_ts": received,
            "event": {"event": event, "id": eid, "created_at": str(created),
                      "participant": {"identity": "client-a"}}}
# LiveKit retries a failed delivery with the same event id.
check("retry deduped", g.webhook_rejoin_stats([wh(5, 5), wh(6, 5)], 0, 10)["participant_joined"], 1)
# created_at is truncated to the second: an event 0.3s into a window must not
# fall out of every window.
check("event just inside a window edge is kept",
      g.webhook_rejoin_stats([wh(100.3, 100)], 100.25, 135.0)["participant_joined"], 1)
# Genuinely delayed delivery falls back to the server's own timestamp.
check("delayed delivery attributed by created_at",
      g.webhook_rejoin_stats([wh(140.0, 100)], 99.0, 135.0)["participant_joined"], 1)
check("distinct events both counted",
      g.webhook_rejoin_stats([wh(5, 5, eid="EV_1"), wh(6, 6, eid="EV_2")], 0, 10)["participant_joined"], 2)

print("time to first connect")
# Append-only logs mean an old connect from an earlier container lifetime is
# still in the file; the run's own connect is the last one before it started.
connects = [{"ts": 10, "event": "connected", "time_to_first_connect_ms": 999.0},
            {"ts": 900, "event": "connected", "time_to_first_connect_ms": 50.0}]
check("uses this run's connect", g.time_to_first_connect({"client-a": connects}, 1000)["client-a"]["ms"], 50.0)
check("staleness is visible",
      g.time_to_first_connect({"client-a": connects}, 1000)["client-a"]["age_s_at_run_start"], 100.0)

print("renderers share one source of truth")
summary = g.build_summary(
    "test",
    [{"run_id": "test", "profile": "clean", "duration_s": 10, "start_ts": 100, "end_ts": 110}],
    [], [q(0, "2")], [], [],
)
md_cells = [c.strip() for line in g.render_markdown(summary).splitlines()
            if line.startswith("| ") and "---" not in line
            for c in line.strip("|").split("|")]
html_cells = [html.unescape(c) for c in re.findall(r"<t[dh]>(.*?)</t[dh]>", g.render_html(summary))]
check("markdown and html agree cell for cell", md_cells, html_cells)
check("column count matches header", len(g.format_row(summary["profiles"][0])), len(g.COLUMNS))
# A profile with no data at all must still render, not crash.
check("empty profile renders", g.format_row(summary["profiles"][0])[0], "clean")

print("rendering says what was actually measured")
def row_for(agent_recovery):
    p = {"profile": "x", "duration_s": 10, "quality": {"percent": {"EXCELLENT": 100.0}},
         "reconnects": {"reconnect_count": 0, "avg_recovery_time_s": None},
         "concealment": {"concealed_seconds_approx": 0.0},
         "agent": {"turns_detected": 0, "turns_completed": 0, "turns_failed": 0,
                   "avg_speech_duration_s": None, "avg_response_latency_ms": None},
         "agent_recovery": agent_recovery, "webhook_rejoins": {"participant_joined": 0}}
    return g.format_row(p)[-1]
# "none recovered" alone would read as a permanent failure; the run simply ended.
check("unrecovered is scoped to the run",
      row_for({"drop_count": 1, "recovered_count": 0, "unrecovered_within_run": 1, "avg_recovery_s": None}),
      "1x, none recovered in-run")
check("partial recovery is scoped too",
      row_for({"drop_count": 2, "recovered_count": 1, "unrecovered_within_run": 1, "avg_recovery_s": 2.0}),
      "2x, avg 2.0s (1 unrecovered in-run)")
check("no drops renders as a dash",
      row_for({"drop_count": 0, "recovered_count": 0, "unrecovered_within_run": 0, "avg_recovery_s": None}), "—")

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
    sys.exit(1)
print("all checks passed")
