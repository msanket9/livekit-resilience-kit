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
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import generate_report as g

FAILURES = []
FOREVER = float("inf")


def check(name, got, want):
    if got == want:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}\n          got:  {got!r}\n          want: {want!r}")
        FAILURES.append(name)


def q(ts, quality, participant="client-a"):
    return {"ts": ts, "event": "connection_quality_changed", "participant": participant, "quality": quality}


def turns(events, run_start=0, deadline=FOREVER):
    return g.pair_agent_turns(events, run_start, deadline)


def drops(events, run_start=0, deadline=FOREVER):
    return g.pair_agent_drops(events, run_start, deadline)


print("quality is time-weighted, not transition-counted")
# EXCELLENT until t=102, LOST for the rest of a [100, 110] window.
events = g.index_quality([q(0, "2"), q(102, "3")])
check("held level dominates", g.quality_distribution(events, 100, 110)["percent"],
      {"EXCELLENT": 20.0, "LOST": 80.0})
# A window with no transitions reads as steady at the carried-forward level.
check("steady window", g.quality_distribution(g.index_quality([q(0, "2")]), 100, 110)["percent"],
      {"EXCELLENT": 100.0})
# Nothing before the window: unknown, not silently attributed to a level.
check("no prior sample", g.quality_distribution([], 100, 110)["percent"], {"UNKNOWN": 100.0})
check("transitions counted separately", g.quality_distribution(events, 100, 110)["transitions"], 1)
# The index filters to the participant under test, not whoever else is in the room.
check("other participants ignored",
      g.quality_distribution(g.index_quality([q(0, "2"), q(102, "3", participant="agent")]), 100, 110)["percent"],
      {"EXCELLENT": 100.0})

print("agent turns")
# The log can begin mid-utterance (real data: 27 speech_end vs 26 speech_start).
# That turn, and the good response after it, must not be discarded.
orphan = [{"ts": 1, "event": "speech_end", "speech_duration": 4.0},
          {"ts": 2, "event": "response_published", "response_latency_ms": 700.0}]
check("orphan speech_end kept", g.agent_stats(turns(orphan), 0, 10)["turns_completed"], 1)
# A turn interrupted by a new speech_start never completed: a real failure.
interrupted = [{"ts": 1, "event": "speech_start"}, {"ts": 5, "event": "speech_start"},
               {"ts": 6, "event": "speech_end", "speech_duration": 1.0},
               {"ts": 7, "event": "response_published", "response_latency_ms": 600.0}]
check("interrupted turn is a failure", g.agent_stats(turns(interrupted), 0, 10)["turns_failed"], 1)
# Turns belong to the window they STARTED in, even if they finish after it.
straddle = [{"ts": 9, "event": "speech_start"}, {"ts": 11, "event": "speech_end", "speech_duration": 2.0},
            {"ts": 12, "event": "response_published", "response_latency_ms": 600.0}]
check("turn straddling a boundary is not a failure", g.agent_stats(turns(straddle), 0, 10)["turns_failed"], 0)
# The append-only log carries the previous run's dangling speech_start. Left
# unbounded it swallows this run's first response and anchors a real turn
# outside every window of the run it belongs to -- measured on the recorded
# data as the clean profile losing one completed turn in most runs.
stale_open = [{"ts": 1, "event": "speech_start"},
              {"ts": 101, "event": "speech_end", "speech_duration": 4.0},
              {"ts": 102, "event": "response_published", "response_latency_ms": 600.0}]
check("a previous run's open turn does not swallow this run's first response",
      g.agent_stats(turns(stale_open, run_start=100), 100, 110),
      {"turns_detected": 1, "turns_completed": 1, "turns_failed": 0,
       "avg_speech_duration_s": 4.0, "avg_response_latency_ms": 600.0})
check("unbounded pairing is what loses it",
      g.agent_stats(turns(stale_open), 100, 110)["turns_detected"], 0)
# ...and symmetrically, this run's dangling turn must not pair with the next run's.
check("no pairing into the next run",
      g.agent_stats(turns([{"ts": 105, "event": "speech_start"},
                           {"ts": 5000, "event": "response_published", "response_latency_ms": 1.0}],
                          run_start=100, deadline=1000), 100, 110)["turns_failed"], 1)

print("agent drops")
# A drop that never recovers is the worst outcome; it must not render as no outcome.
check("unrecovered drop is counted",
      g.agent_recovery_stats(drops([{"ts": 5, "event": "track_unsubscribed", "participant": "client-a"}]), 0, 10),
      {"drop_count": 1, "recovered_count": 0, "unrecovered_within_run": 1, "avg_recovery_s": None})
mixed = [{"ts": 1, "event": "track_unsubscribed", "participant": "client-a"},
         {"ts": 3, "event": "response_published", "response_latency_ms": 1.0},
         {"ts": 5, "event": "track_unsubscribed", "participant": "client-a"}]
check("one recovered, one not", g.agent_recovery_stats(drops(mixed), 0, 10),
      {"drop_count": 2, "recovered_count": 1, "unrecovered_within_run": 1, "avg_recovery_s": 2.0})
# The data logs are append-only across runs: a drop at the end of one run must
# not pair with the first response of the next.
cross_run = [{"ts": 5, "event": "track_unsubscribed", "participant": "client-a"},
             {"ts": 5000, "event": "response_published", "response_latency_ms": 1.0}]
check("no pairing past the deadline",
      g.agent_recovery_stats(drops(cross_run, deadline=1000), 0, 10)["avg_recovery_s"], None)
# severe_outage is the last profile in the suite and its drop recovers 12-16s
# later, i.e. after the run's own end_ts. Bounding at end_ts reported "none
# recovered in-run" for a drop the agent demonstrably came back from; the
# bound is the next run's start.
late_recovery = [{"ts": 105, "event": "track_unsubscribed", "participant": "client-a"},
                 {"ts": 122, "event": "response_published", "response_latency_ms": 1.0}]
check("recovery just past the run's last window still counts",
      g.agent_recovery_stats(drops(late_recovery, run_start=100, deadline=300), 100, 110)["avg_recovery_s"], 17.0)
# Only losing client-a's track is a drop; the agent's own track churn is not.
check("another participant's unsubscribe is not a drop",
      g.agent_recovery_stats(drops([{"ts": 5, "event": "track_unsubscribed", "participant": "client-b"}]), 0, 10)["drop_count"], 0)

print("connection-level reconnects")
def rc(events, run_start=0, deadline=FOREVER):
    return g.pair_reconnects(events, run_start, deadline)
# A reconnect that starts inside a window and completes after it recovered; the
# per-window slice used to score it as unrecovered. Real recovery measures
# ~11.7s, so a window edge lands inside that range routinely.
late = [{"ts": 108, "event": "reconnecting"}, {"ts": 120, "event": "reconnected"}]
check("reconnect completing after the window still counts",
      g.reconnect_stats(rc(late), 100, 110),
      {"reconnect_count": 1, "recovered_count": 1, "avg_recovery_time_s": 12.0, "unrecovered_within_run": 0})
check("a reconnect that never completes is still counted",
      g.reconnect_stats(rc([{"ts": 105, "event": "reconnecting"}]), 100, 110)["unrecovered_within_run"], 1)
# The previous run's dangling `reconnecting` must not eat this run's `reconnected`.
check("no pairing with a previous run's reconnect",
      g.reconnect_stats(rc([{"ts": 1, "event": "reconnecting"}, {"ts": 105, "event": "reconnecting"},
                            {"ts": 108, "event": "reconnected"}], run_start=100), 100, 110)["recovered_count"], 1)

print("concealed audio")
def ts_event(ts, sid, concealed, participant="client-a"):
    return {"ts": ts, "event": "track_stats", "participant": participant, "track_sid": sid,
            "concealed_samples": concealed, "silent_concealed_samples": 0}
def sub_event(ts, sid, participant="client-a"):
    return {"ts": ts, "event": "track_subscribed", "participant": participant, "sid": sid, "kind": "1"}
# A full reconnect resubscribes to a NEW sid whose counters restart at 0.
# Diffing last-at-end minus last-at-start across the sid change goes negative
# and clamps to 0, hiding real concealment on the track being torn down.
resub = [ts_event(99, "SID_1", 1000), ts_event(104, "SID_1", 5000),
         ts_event(106, "SID_2", 0), ts_event(109, "SID_2", 2000)]
check("concealment survives a track resubscribe",
      g.concealment_stats(g.index_track_stats(resub), 100, 110)["concealed_samples"], 6000)
# The agent's own response track is relayed through the same shaped server leg.
check("only the participant under test is summed",
      g.concealment_stats(g.index_track_stats(resub + [ts_event(99, "SID_3", 0, participant="agent"),
                                                       ts_event(109, "SID_3", 99999, participant="agent")]),
                          100, 110)["concealed_samples"], 6000)
# A sid whose counters really did start at 0 inside the window (a reconnect
# mid-fault) must be baselined at 0, or the concealment right after the
# reconnect is undercounted.
inside = [sub_event(105, "SID_N"), ts_event(106, "SID_N", 0), ts_event(109, "SID_N", 2000)]
check("a track subscribed inside the window is baselined at zero",
      g.concealment_stats(g.index_track_stats(inside), 100, 110)["concealed_samples"], 2000)
# A track subscribed BEFORE the window but not yet polled is the opposite case:
# assuming 0 charges this window with everything concealed since the subscribe.
# The damaging version is a resubscribe during the cooldown between profiles,
# whose whole purpose is to sit outside every measured window.
before = [sub_event(95, "SID_P"), ts_event(102, "SID_P", 3000), ts_event(108, "SID_P", 5000)]
check("a track subscribed before the window is not charged for the gap",
      g.concealment_stats(g.index_track_stats(before), 100, 110)["concealed_samples"], 2000)
# A sid polled before the window still uses that sample, subscribe time or not.
spanning = [sub_event(95, "SID_Q"), ts_event(99, "SID_Q", 1000), ts_event(108, "SID_Q", 5000)]
check("a pre-window sample is still the preferred baseline",
      g.concealment_stats(g.index_track_stats(spanning), 100, 110)["concealed_samples"], 4000)

print("server webhooks")
def wh(received, created, event="participant_joined", eid="EV_1"):
    return {"received_ts": received,
            "event": {"event": event, "id": eid, "created_at": str(created),
                      "participant": {"identity": "client-a"}}}
# LiveKit retries a failed delivery with the same event id.
check("retry deduped", g.webhook_rejoin_stats(g.index_webhooks([wh(5, 5), wh(6, 5)]), 0, 10)["participant_joined"], 1)
# Deduplication is global, not per-window: a retry that resolves into a later
# window than its original was previously counted in both.
check("retry deduped across windows",
      g.webhook_rejoin_stats(g.index_webhooks([wh(5, 5), wh(5000, 5)]), 4000, 6000)["participant_joined"], 0)
# created_at is truncated to the second: an event 0.3s into a window must not
# fall out of every window.
check("event just inside a window edge is kept",
      g.webhook_rejoin_stats(g.index_webhooks([wh(100.3, 100)]), 100.25, 135.0)["participant_joined"], 1)
# Genuinely delayed delivery falls back to the server's own timestamp.
check("delayed delivery attributed by created_at",
      g.webhook_rejoin_stats(g.index_webhooks([wh(140.0, 100)]), 99.0, 135.0)["participant_joined"], 1)
check("distinct events both counted",
      g.webhook_rejoin_stats(g.index_webhooks([wh(5, 5, eid="EV_1"), wh(6, 6, eid="EV_2")]), 0, 10)["participant_joined"], 2)
check("every counted kind is reported",
      sorted(g.webhook_rejoin_stats([], 0, 10)),
      ["participant_joined", "participant_left", "track_published", "track_unpublished"])

print("time to first connect")
# Append-only logs mean an old connect from an earlier container lifetime is
# still in the file; the run's own connect is the last one before it started.
connects = [{"ts": 10, "event": "connected", "time_to_first_connect_ms": 999.0},
            {"ts": 900, "event": "connected", "time_to_first_connect_ms": 50.0}]
check("uses this run's connect", g.time_to_first_connect({"client-a": connects}, 1000)["client-a"]["ms"], 50.0)
check("staleness is computed",
      g.time_to_first_connect({"client-a": connects}, 1000)["client-a"]["age_s_at_run_start"], 100.0)
# ...and actually rendered. It was stored in the JSON and dropped on the floor
# by the renderer, so the one thing it exists to expose was invisible.
check("staleness is rendered", g._ttfc_str({"client-a": {"ms": 50.0, "age_s_at_run_start": 100.0}}),
      "client-a=50.0ms (measured 100.0s before run start)")

print("manifest bounds pairing at the next run, not this run's end")
with tempfile.TemporaryDirectory() as d:
    path = os.path.join(d, "run_manifest.jsonl")
    with open(path, "w") as f:
        for row in ({"run_id": "A", "profile": "clean", "duration_s": 10, "start_ts": 100, "end_ts": 110},
                    {"run_id": "A", "profile": "severe_outage", "duration_s": 10, "start_ts": 120, "end_ts": 130},
                    {"run_id": "B", "profile": "clean", "duration_s": 10, "start_ts": 200, "end_ts": 210}):
            f.write(json.dumps(row) + "\n")
    check("deadline is the next run's start", g.load_manifest(path, "A")[2], 200)
    check("newest run is unbounded", g.load_manifest(path, "B")[2], FOREVER)
    check("default run_id is the last one recorded", g.load_manifest(path, None)[0], "B")

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
def row(agent_recovery=None, reconnects=None, quality=None):
    p = {"profile": "x", "duration_s": 10,
         "quality": {"percent": quality or {"EXCELLENT": 100.0}},
         "reconnects": reconnects or {"reconnect_count": 0, "recovered_count": 0,
                                      "avg_recovery_time_s": None, "unrecovered_within_run": 0},
         "concealment": {"concealed_seconds_approx": 0.0},
         "agent": {"turns_detected": 0, "turns_completed": 0, "turns_failed": 0,
                   "avg_speech_duration_s": None, "avg_response_latency_ms": None},
         "agent_recovery": agent_recovery or {"drop_count": 0, "recovered_count": 0,
                                              "unrecovered_within_run": 0, "avg_recovery_s": None},
         "webhook_rejoins": {"participant_joined": 0}}
    return g.format_row(p)
# "none recovered" alone would read as a permanent failure; the run simply ended.
check("unrecovered is scoped to the run",
      row(agent_recovery={"drop_count": 1, "recovered_count": 0, "unrecovered_within_run": 1, "avg_recovery_s": None})[-1],
      "1x, none recovered in-run")
check("partial recovery is scoped too",
      row(agent_recovery={"drop_count": 2, "recovered_count": 1, "unrecovered_within_run": 1, "avg_recovery_s": 2.0})[-1],
      "2x, avg 2.0s (1 unrecovered in-run)")
check("no drops renders as a dash", row()[-1], "—")
# The reconnect column printed a bare dash for a reconnect that never
# completed, which reads exactly like "nothing happened".
check("an unrecovered reconnect is visible in the cell",
      row(reconnects={"reconnect_count": 1, "recovered_count": 0,
                      "avg_recovery_time_s": None, "unrecovered_within_run": 1})[4],
      "none recovered in-run")
# ...without repeating the count column sitting right next to it.
check("a recovered reconnect stays a plain average",
      row(reconnects={"reconnect_count": 1, "recovered_count": 1,
                      "avg_recovery_time_s": 11.67, "unrecovered_within_run": 0})[4],
      "11.67s")
check("a partly recovered reconnect says how many did not",
      row(reconnects={"reconnect_count": 2, "recovered_count": 1,
                      "avg_recovery_time_s": 11.67, "unrecovered_within_run": 1})[4],
      "11.67s (1 unrecovered in-run)")
# A partly-unknown window rendered only the known levels, so the four
# percentages silently summed to less than 100 with nothing saying why.
check("a partly unknown window says so",
      row(quality={"EXCELLENT": 60.0, "UNKNOWN": 40.0})[2], "60.0% / 0% / 0% / 0% (+40.0% unknown)")
check("a wholly unknown window is not dressed up as data",
      row(quality={"UNKNOWN": 100.0})[2], "n/a (no quality sample before window)")

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
    sys.exit(1)
print("all checks passed")
