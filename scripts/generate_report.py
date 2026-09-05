#!/usr/bin/env python3
"""Turns the raw JSONL event logs into a clean-vs-degraded comparison report.

Usage:
  python3 scripts/generate_report.py [--run-id RUN_ID]

Reads data/run_manifest.jsonl for a run's profile time windows (default: the
most recent run_id present), slices data/client-a-events.jsonl,
data/client-b-events.jsonl by each window, and writes
reports/report-<run_id>.{json,md,html}.

Two conventions run through every metric here:

  * An episode (a turn, a reconnect, an agent drop) belongs to the profile
    window it STARTED in, however long it took to finish. Slicing the raw
    stream per window first and pairing afterwards would score a turn that
    straddles a profile boundary as a failure, which it isn't.

  * Pairing is nonetheless bounded to the run being reported: `[run_start,
    next_run_start)`. The data logs are append-only across runs by design, so
    unbounded pairing lets one run's dangling episode swallow the next run's
    first completion.
"""

import argparse
import bisect
import html
import json
import os
import sys
from collections import defaultdict

QUALITY_LABELS = {"0": "POOR", "1": "GOOD", "2": "EXCELLENT", "3": "LOST"}
QUALITY_ORDER = ("EXCELLENT", "GOOD", "POOR", "LOST")
SAMPLE_RATE = 48000


def read_jsonl(path):
    """Parses one JSON object per non-blank line.

    A malformed line is skipped with a warning rather than aborting the whole
    file. These logs are explicitly designed to be read while their writer is
    still appending (client.py's EVENT_LOG_PATH comment says as much, so a
    report can be generated against a live stack), so a torn final line from
    an in-progress write is a realistic outcome, not a contrived one -- and
    every one of the four files this function reads is read in FULL regardless
    of which run_id was requested. One bad line from a run nobody cares about
    used to permanently block generating a report for any run, including the
    newest one."""
    if not os.path.exists(path):
        return []
    records = []
    with open(path) as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"warning: skipping malformed JSON at {path}:{lineno}: {e}", file=sys.stderr)
    return records


def load_manifest(path, run_id):
    """Returns (run_id, this run's profile entries, pairing_deadline).

    pairing_deadline is the start of the next run recorded in the manifest, or
    infinity if this is the newest run. It is the bound every cross-window
    pairing in this file uses. The manifest is the principled source for it:
    the previous version bounded pairing at the run's own end_ts, which is too
    tight -- a severe_outage drop late in the final profile recovers 12-16s
    later, i.e. after end_ts, and was reported as "none recovered in-run" in
    three of the recorded runs when the agent had in fact recovered."""
    entries = read_jsonl(path)
    if not entries:
        raise SystemExit(f"no run manifest found at {path} -- run scripts/run_test_suite.sh first")
    if run_id is None:
        # The latest run by start_ts, not the last line in the file. The suite
        # appends in order so the two normally agree, but a manifest that has
        # been concatenated, restored, or hand-edited would otherwise silently
        # report an older run as "the most recent".
        run_id = max(entries, key=lambda e: e["start_ts"])["run_id"]
    profiles = [e for e in entries if e["run_id"] == run_id]
    if not profiles:
        raise SystemExit(f"no manifest entries for run_id={run_id}")
    run_end_ts = max(e["end_ts"] for e in profiles)
    later_starts = [e["start_ts"] for e in entries if e["start_ts"] > run_end_ts]
    return run_id, profiles, min(later_starts) if later_starts else float("inf")


def in_window(events, start_ts, end_ts):
    """Slices a TIME-SORTED events list to [start_ts, end_ts] via bisect.

    Its one caller (webhook_rejoin_stats) passes index_webhooks' output, which
    dedups and sorts across the WHOLE cross-run webhooks.jsonl (dedup has to be
    global, since a retry's duplicate can land in a different window than its
    original) -- so this is the same shape quality_distribution already fixed
    with bisect: a sorted, ever-growing, cross-run list where only a small
    slice matters for one window. A plain linear scan here was the one
    remaining place in the file whose per-report cost keeps climbing as
    webhooks.jsonl (explicitly append-only, never trimmed) accumulates more
    runs, instead of being bounded by the window being asked about."""
    lo = bisect.bisect_left(events, start_ts, key=lambda e: e["ts"])
    hi = bisect.bisect_right(events, end_ts, key=lambda e: e["ts"])
    return events[lo:hi]


def _by_ts(events):
    return sorted(events, key=lambda e: e["ts"])


def _started_in(episodes, start_ts, end_ts):
    return [ep for ep in episodes if start_ts <= ep["start_ts"] <= end_ts]


def _mean(values, digits=2):
    return round(sum(values) / len(values), digits) if values else None


def _pair_episodes(events, open_event, close_event, is_open=None):
    """Pairs an opening event with the next closing one, over an already
    run-bounded, time-sorted stream.

    Shared by turns, reconnects and agent drops, which are the same shape: an
    episode opens, and either closes or doesn't. An episode that never closes
    is emitted with close=None rather than dropped -- the worst outcome must
    not render as no outcome. A second opening event closes the first as
    unfinished.

    `is_open` optionally filters which opening events count (agent drops only
    care about losing client-a's track)."""
    episodes = []
    open_ts = None
    for e in events:
        kind = e["event"]
        if kind == open_event and (is_open is None or is_open(e)):
            if open_ts is not None:
                episodes.append({"start_ts": open_ts, "close_ts": None})
            open_ts = e["ts"]
        elif kind == close_event and open_ts is not None:
            episodes.append({"start_ts": open_ts, "close_ts": e["ts"]})
            open_ts = None
    if open_ts is not None:
        episodes.append({"start_ts": open_ts, "close_ts": None})
    return episodes


def _quality_label(event):
    return QUALITY_LABELS.get(event["quality"], event["quality"])


def index_quality(client_b_events, participant="client-a"):
    """Time-sorted connection_quality_changed events for one participant,
    built once per run rather than re-filtered and re-sorted per window."""
    return _by_ts(
        e
        for e in client_b_events
        if e["event"] == "connection_quality_changed" and e.get("participant") == participant
    )


def quality_distribution(quality_events, start_ts, end_ts):
    """Time-weighted share of [start_ts, end_ts] spent at each ConnectionQuality
    level, given the indexed events from index_quality().

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
    attributed to a level."""
    timeline = [e["ts"] for e in quality_events]
    first = bisect.bisect_left(timeline, start_ts)
    current = _quality_label(quality_events[first - 1]) if first else "UNKNOWN"

    durations = defaultdict(float)
    cursor = start_ts
    transitions = 0
    for e in quality_events[first : bisect.bisect_right(timeline, end_ts)]:
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


def pair_reconnects(client_a_events, run_start_ts, deadline_ts):
    """Connection-level reconnect episodes for the run, paired once.

    Pairing used to happen on a per-window slice, which made a reconnect that
    started 2s before a window closed and completed 12s later read as
    unrecovered -- the same boundary-truncation class already fixed for agent
    turns. Severe outage recovery measures 11.7s, so the window edge is well
    inside the range where this bites. Bounding at run_start also stops a
    reconnect left open at the end of one run from pairing with the first
    `reconnected` of the next."""
    events = _by_ts(
        e
        for e in client_a_events
        if e["event"] in ("reconnecting", "reconnected") and run_start_ts <= e["ts"] < deadline_ts
    )
    return [
        {"start_ts": ep["start_ts"], "recovery_s": None if ep["close_ts"] is None else ep["close_ts"] - ep["start_ts"]}
        for ep in _pair_episodes(events, "reconnecting", "reconnected")
    ]


def reconnect_stats(reconnects, start_ts, end_ts):
    """A reconnect belongs to the window its `reconnecting` fell in."""
    episodes = _started_in(reconnects, start_ts, end_ts)
    recoveries = [r["recovery_s"] for r in episodes if r["recovery_s"] is not None]
    return {
        "reconnect_count": len(episodes),
        "recovered_count": len(recoveries),
        "avg_recovery_time_s": _mean(recoveries),
        "unrecovered_within_run": len(episodes) - len(recoveries),
    }


def index_track_stats(client_b_events, participant="client-a"):
    """Per subscribed-track sid: its time-sorted track_stats samples, and when
    the track was subscribed.

    Filtering to participant="client-a" matters: since the agent leg was added
    client-b subscribes to TWO audio tracks (client-a's speech and the agent's
    response tone), and rural_4g shapes livekit-server's own egress, which
    affects every track relayed through the server -- so an unfiltered sum
    would blend in concealment from a track this metric isn't measuring.

    The subscribe timestamp is carried because _sid_delta cannot pick a correct
    baseline without it -- see there."""
    subscribed_at = {}
    by_sid = defaultdict(list)
    for e in client_b_events:
        if e.get("participant") != participant:
            continue
        if e["event"] == "track_subscribed":
            sid = e.get("sid")
            if sid is not None:
                subscribed_at.setdefault(sid, e["ts"])
        elif e["event"] == "track_stats":
            by_sid[e["track_sid"]].append(e)
    return {
        sid: {"samples": _by_ts(samples), "subscribed_at": subscribed_at.get(sid)}
        for sid, samples in by_sid.items()
    }


def _sid_delta(track, start_ts, end_ts, field):
    """Delta of a cumulative per-track-sid counter within [start_ts, end_ts].

    The baseline is the last sample before start_ts. When there is no such
    sample the right baseline depends on WHY, and the two cases pull in
    opposite directions:

      * The track was (re)subscribed inside this window -- the severe_outage
        case, where a full reconnect produces a new sid. Its WebRTC counters
        genuinely started at 0 here, so 0 is the correct baseline and anything
        else undercounts the concealment right after the reconnect.

      * The track was subscribed BEFORE the window and simply had not been
        polled yet. Assuming 0 then charges this window with everything
        concealed since the subscribe. The damaging version of that is a track
        resubscribed during the cooldown between profiles: the whole point of
        the cooldown is that it sits outside every measured window, and a 0
        baseline would post its concealment to the next profile's fault. The
        first in-window sample is the closest honest baseline, which caps the
        error at one poll interval instead of the whole cooldown.

    The subscribe timestamp from index_track_stats is what separates them."""
    samples = track["samples"]
    timeline = [e["ts"] for e in samples]
    last = bisect.bisect_right(timeline, end_ts)
    if not last or timeline[last - 1] < start_ts:
        return 0
    first = bisect.bisect_left(timeline, start_ts)
    if first:
        baseline = samples[first - 1].get(field, 0)
    elif track["subscribed_at"] is not None and track["subscribed_at"] < start_ts:
        # first < last here: a sample at or after start_ts must exist, or the
        # timeline[last - 1] < start_ts guard above would already have returned.
        baseline = samples[first].get(field, 0)
    else:
        baseline = 0
    return max(0, samples[last - 1].get(field, 0) - baseline)


def concealment_stats(track_stats_by_sid, start_ts, end_ts):
    """Sum of concealed-sample deltas within [start_ts, end_ts], computed
    per subscribed-track sid and summed across sids.

    A full LiveKit reconnect re-subscribes to a NEW track sid whose cumulative
    WebRTC counters reset to 0. Naively diffing "last value at end" minus "last
    value at start" across that sid change goes negative and gets clamped to 0
    -- silently hiding real concealment that happened on the old track right
    before it was torn down. Diffing per-sid and summing avoids that."""
    concealed_total = sum(
        _sid_delta(track, start_ts, end_ts, "concealed_samples")
        for track in track_stats_by_sid.values()
    )
    silent_total = sum(
        _sid_delta(track, start_ts, end_ts, "silent_concealed_samples")
        for track in track_stats_by_sid.values()
    )
    return {
        "concealed_samples": concealed_total,
        "silent_concealed_samples": silent_total,
        "concealed_seconds_approx": round(concealed_total / SAMPLE_RATE, 2),
    }


def _from_source(agent_events, run_start_ts, deadline_ts, source):
    """The run's agent events for one speaker, time-sorted.

    `e.get("source", source)` rather than `e.get("source")`: the agent only
    started stamping turn events with the speaker they came from after this
    filter was added, and every recorded log predates it. Defaulting a missing
    field to the requested source keeps those logs reading exactly as they did,
    while new logs filter for real. Verified byte-identical across all recorded
    runs."""
    return _by_ts(
        e
        for e in agent_events
        if run_start_ts <= e["ts"] < deadline_ts and e.get("source", source) == source
    )


def pair_agent_turns(agent_events, run_start_ts, deadline_ts, source="client-a"):
    """Pairs speech_start -> speech_end -> response_published once per run,
    tagging each turn with its speech_start timestamp.

    Deliberately NOT window-restricted: a turn that starts just before a fault
    profile's window ends and completes a moment later (a real, successful
    turn) must not be counted as failed just because a profile boundary fell in
    the middle of it.

    It IS run-restricted, which it previously was not, and that was a real bug.
    A turn left open at the end of one run stayed open across the append-only
    log and swallowed the *next* run's first response_published, anchoring a
    genuine turn at a timestamp outside every window of the run it belonged to.
    Measured on the recorded data: the clean profile lost one real completed
    turn (5 instead of 6) in seven of the twelve suite runs.

    A speech_end with no open speech_start means the log simply begins
    mid-utterance (the agent's VAD was already inside a turn when logging
    started). That turn is anchored at its speech_end rather than discarded,
    which would also have thrown away the perfectly good response_published
    that followed it."""
    events = _from_source(agent_events, run_start_ts, deadline_ts, source)
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


def agent_stats(turns, start_ts, end_ts):
    """A turn belongs to the profile window it STARTED in. A turn only counts
    as completed once speech_start -> speech_end -> response_published all
    land -- one that never gets a response_published (VAD never detected
    end-of-speech, or a new speech_start interrupted it first) is a genuine
    turn-taking failure.

    avg_speech_duration_s is worth watching on its own, not just
    success/failure: under packet loss the VAD can still complete a turn but
    detect a truncated one, which shows up as a lower average duration during a
    fault window rather than as a failed turn.

    turns_detected is reported alongside the success/failure split for the same
    reason: across every run so far a degraded profile shows up as *fewer turns
    detected*, not as failed ones -- when the audio stops arriving the VAD has
    nothing to fail on, it simply never opens a turn."""
    window_turns = _started_in(turns, start_ts, end_ts)
    completed = [t for t in window_turns if t["latency_ms"] is not None]
    durations = [t["duration"] for t in completed if t["duration"] is not None]
    return {
        "turns_detected": len(window_turns),
        "turns_completed": len(completed),
        "turns_failed": len(window_turns) - len(completed),
        "avg_speech_duration_s": _mean(durations),
        "avg_response_latency_ms": _mean([t["latency_ms"] for t in completed]),
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


def index_webhooks(webhook_events, participant="client-a"):
    """One time-sorted (ts, kind) list for the participant, deduplicated by the
    server-assigned event id.

    LiveKit retries a webhook delivery that fails or times out, and a retry
    carries the same id. Deduplication is global rather than per-window --
    doing it per-window meant a retry that resolved to a different window from
    its original was still counted twice, inflating the one signal in the
    report that exists specifically to be the trustworthy cross-check."""
    seen = set()
    indexed = []
    for e in webhook_events:
        payload = e.get("event", {})
        if payload.get("participant", {}).get("identity") != participant:
            continue
        event_id = payload.get("id")
        if event_id is not None:
            if event_id in seen:
                continue
            seen.add(event_id)
        indexed.append({"ts": _webhook_ts(e), "kind": payload.get("event")})
    return _by_ts(indexed)


def webhook_rejoin_stats(webhook_index, start_ts, end_ts):
    """Server-side (LiveKit webhook) cross-check of participant/track churn
    within [start_ts, end_ts] -- a signal independent of the client's own
    self-reported reconnecting/reconnected events, since the server is the
    authority on whether a participant actually left and rejoined the room.
    Only room_started's initial joins land before any profile window starts, so
    a non-zero participant_joined count here means a real server-observed
    rejoin happened during this specific fault."""
    counts = defaultdict(int)
    for e in in_window(webhook_index, start_ts, end_ts):
        counts[e["kind"]] += 1
    return {
        kind: counts[kind]
        for kind in ("participant_joined", "participant_left", "track_published", "track_unpublished")
    }


def pair_agent_drops(agent_events, run_start_ts, deadline_ts, source="client-a"):
    """Pairs each time the agent lost its subscription to client-a's track
    (track_unsubscribed) with the agent's next successfully completed turn
    (response_published) -- the agent's own "time-to-recover after a drop",
    distinct from reconnect_stats: that measures the client's connection-level
    recovery, this measures whether the agent's turn-taking pipeline actually
    resumed working.

    Pairing runs over the whole run rather than a single profile window, for
    the same boundary-truncation reason as pair_agent_turns, and is bounded to
    the run so a drop left unrecovered at the end of one run cannot pair with
    the first response of the next.

    The bound is the *next run's start*, not this run's end_ts. The tighter
    bound was itself wrong: severe_outage is the last profile in the suite and
    its drop recovers 12-16s later, past end_ts, so three of the recorded runs
    reported "none recovered in-run" for a drop the agent demonstrably came
    back from."""
    events = _from_source(agent_events, run_start_ts, deadline_ts, source)
    return [
        {"drop_ts": ep["start_ts"], "recovery_s": None if ep["close_ts"] is None else ep["close_ts"] - ep["start_ts"]}
        for ep in _pair_episodes(
            events,
            "track_unsubscribed",
            "response_published",
            is_open=lambda e: e.get("participant") == source,
        )
    ]


def agent_recovery_stats(drops, start_ts, end_ts):
    """A drop belongs to the profile window its track_unsubscribed fell in,
    regardless of when recovery completed."""
    episodes = [d for d in drops if start_ts <= d["drop_ts"] <= end_ts]
    recoveries = [d["recovery_s"] for d in episodes if d["recovery_s"] is not None]
    return {
        "drop_count": len(episodes),
        "recovered_count": len(recoveries),
        "unrecovered_within_run": len(episodes) - len(recoveries),
        "avg_recovery_s": _mean(recoveries),
    }


def time_to_first_connect(events_by_client, run_start_ts):
    """Time-to-first-connect for the session that was actually live during
    this run: the LAST `connected` at or before the run started.

    Taking the first `connected` in the file reports a connect from whatever
    container lifetime happened to write the log first -- possibly days old and
    belonging to an entirely different run, since the suite never clears the
    append-only data logs. age_s_at_run_start is carried alongside, and
    rendered, so a stale figure is visible as stale rather than quietly
    wrong."""
    result = {}
    for identity, events in events_by_client.items():
        candidates = _by_ts(e for e in events if e["event"] == "connected" and e["ts"] <= run_start_ts)
        if candidates:
            chosen = candidates[-1]
            result[identity] = {
                "ms": round(chosen.get("time_to_first_connect_ms", 0), 1),
                "age_s_at_run_start": round(run_start_ts - chosen["ts"], 1),
            }
    return result


def build_summary(
    run_id,
    manifest_entries,
    client_a_events,
    client_b_events,
    agent_events,
    webhook_events,
    pairing_deadline=float("inf"),
):
    """Everything that can be derived once per run is derived once per run.

    Each metric used to re-filter, re-sort and re-pair the entire event stream
    for every profile window, so a five-profile run walked each log five times
    over. The indexes and episode lists below are built once and only sliced
    per window."""
    entries = sorted(manifest_entries, key=lambda e: e["start_ts"])
    # A manifest entry with end_ts < start_ts (a hand-edited manifest, or a
    # backward host/VM clock step between run_test_suite.sh's two `now()`
    # calls) is not merely unusual data -- every metric here divides by
    # (end_ts - start_ts) somewhere, and a negative window produces a
    # negative/negative division that comes out as a confident, plausible
    # -looking percentage instead of an error. Rejecting it here means a
    # corrupted manifest entry is loud, not a clean-looking wrong row.
    for e in entries:
        if e["end_ts"] < e["start_ts"]:
            raise SystemExit(
                f"manifest entry for run_id={e['run_id']} profile={e['profile']} has "
                f"end_ts ({e['end_ts']}) before start_ts ({e['start_ts']}) -- refusing "
                "to generate a report from an inverted time window"
            )
    run_start_ts = entries[0]["start_ts"]

    quality_events = index_quality(client_b_events)
    track_stats_by_sid = index_track_stats(client_b_events)
    webhook_index = index_webhooks(webhook_events)
    reconnects = pair_reconnects(client_a_events, run_start_ts, pairing_deadline)
    turns = pair_agent_turns(agent_events, run_start_ts, pairing_deadline)
    drops = pair_agent_drops(agent_events, run_start_ts, pairing_deadline)

    profiles = [
        {
            "profile": entry["profile"],
            "duration_s": entry["duration_s"],
            "start_ts": entry["start_ts"],
            "end_ts": entry["end_ts"],
            "quality": quality_distribution(quality_events, entry["start_ts"], entry["end_ts"]),
            "reconnects": reconnect_stats(reconnects, entry["start_ts"], entry["end_ts"]),
            "concealment": concealment_stats(track_stats_by_sid, entry["start_ts"], entry["end_ts"]),
            "agent": agent_stats(turns, entry["start_ts"], entry["end_ts"]),
            "agent_recovery": agent_recovery_stats(drops, entry["start_ts"], entry["end_ts"]),
            "webhook_rejoins": webhook_rejoin_stats(webhook_index, entry["start_ts"], entry["end_ts"]),
        }
        for entry in entries
    ]
    return {
        "run_id": run_id,
        "time_to_first_connect": time_to_first_connect(
            {"client-a": client_a_events, "client-b": client_b_events}, run_start_ts
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


def _recovery_cell(count, avg_s, unrecovered, show_count=True):
    """Renders a recovery column, for both the connection-level and the agent
    one.

    An episode that never recovered has to stay visible. The reconnect column
    used to print a bare dash for it, which reads exactly like "nothing
    happened" -- the same failure mode already fixed once for the agent-recovery
    column, and the reason both columns now share this formatter.

    show_count is off for the reconnect column only, which sits next to its own
    count column and would otherwise repeat it."""
    if count == 0:
        return "—"
    if avg_s is None:
        return f"{count}x, none recovered in-run" if show_count else "none recovered in-run"
    cell = f"{count}x, avg {avg_s}s" if show_count else f"{avg_s}s"
    if unrecovered:
        cell += f" ({unrecovered} unrecovered in-run)"
    return cell


def format_row(p):
    """The 11 rendered cells for one profile, as plain strings.

    Single source of truth for both renderers: render_markdown and render_html
    previously carried a byte-identical 15-line copy of this formatting, so
    every column change had to be made twice and stayed correct only by luck."""
    pct = p["quality"]["percent"]
    unknown = pct.get("UNKNOWN", 0)
    if set(pct) <= {"UNKNOWN"}:
        quality_str = "n/a (no quality sample before window)"
    else:
        quality_str = " / ".join(f"{pct.get(k, 0)}%" for k in QUALITY_ORDER)
        # A partly-unknown window used to render only the known levels, so the
        # four percentages silently summed to less than 100 with nothing saying
        # why.
        if unknown:
            quality_str += f" (+{unknown}% unknown)"

    rc = p["reconnects"]
    ag = p["agent"]
    agrec = p["agent_recovery"]
    turn_len = f"{ag['avg_speech_duration_s']}s" if ag["avg_speech_duration_s"] is not None else "—"
    latency = f"{ag['avg_response_latency_ms']}ms" if ag["avg_response_latency_ms"] is not None else "—"

    return [
        p["profile"],
        f"{p['duration_s']}s",
        quality_str,
        str(rc["reconnect_count"]),
        _recovery_cell(
            rc["reconnect_count"], rc["avg_recovery_time_s"], rc["unrecovered_within_run"], show_count=False
        ),
        str(p["webhook_rejoins"]["participant_joined"]),
        f"{p['concealment']['concealed_seconds_approx']}s",
        f"{ag['turns_detected']} ({ag['turns_completed']}/{ag['turns_failed']})",
        turn_len,
        latency,
        _recovery_cell(agrec["drop_count"], agrec["avg_recovery_s"], agrec["unrecovered_within_run"]),
    ]


def _ttfc_str(ttfc):
    """Renders the age alongside the figure. The age was computed and stored in
    the JSON but never rendered, so the one thing it exists to expose -- that
    the number may belong to a container lifetime older than the run -- was
    invisible in the report a human actually reads."""
    return ", ".join(
        f"{k}={v['ms']}ms (measured {v['age_s_at_run_start']}s before run start)"
        for k, v in ttfc.items()
    )


def render_markdown(summary):
    lines = [f"# LiveKit Resilience Report: run {summary['run_id']}", ""]
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
<title>LiveKit Resilience Report: {html.escape(str(summary['run_id']))}</title>
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
  <h1>LiveKit Resilience Report: run {html.escape(str(summary['run_id']))}</h1>
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
    run_id, manifest_entries, pairing_deadline = load_manifest(manifest_path, args.run_id)

    summary = build_summary(
        run_id,
        manifest_entries,
        read_jsonl(os.path.join(args.data_dir, "client-a-events.jsonl")),
        read_jsonl(os.path.join(args.data_dir, "client-b-events.jsonl")),
        read_jsonl(os.path.join(args.data_dir, "agent-events.jsonl")),
        read_jsonl(os.path.join(args.data_dir, "webhooks.jsonl")),
        pairing_deadline=pairing_deadline,
    )

    os.makedirs(args.out_dir, exist_ok=True)
    written = []
    for suffix, payload in (
        ("json", json.dumps(summary, indent=2)),
        ("md", render_markdown(summary)),
        ("html", render_html(summary)),
    ):
        path = os.path.join(args.out_dir, f"report-{run_id}.{suffix}")
        with open(path, "w") as f:
            f.write(payload)
        written.append(path)

    print("\n".join(f"wrote {p}" for p in written))


if __name__ == "__main__":
    main()
