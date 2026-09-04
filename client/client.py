"""Minimal LiveKit test client: joins a room, optionally publishes a looped
speech-like utterance (via espeak-ng), and logs connection/track/quality/freeze
events both to stdout and as structured JSON lines for later analysis.

Configured entirely via environment variables so the same image can play the
publisher or subscriber persona in docker-compose:

  LIVEKIT_URL          ws(s):// URL of the LiveKit server (default: ws://livekit-server:7880)
  LIVEKIT_API_KEY      default: devkey
  LIVEKIT_API_SECRET   default: secret
  ROOM_NAME            default: resilience-test
  PARTICIPANT_IDENTITY required
  PUBLISH_AUDIO        "true" to publish the speech loop, otherwise subscribe-only
  EVENT_LOG_PATH          where to append JSON-line events (default: /data/events.jsonl)
  AUDIO_STATS_POLL_SECONDS  how often to poll subscribed-audio freeze/concealment stats (default: 2)
  RED_ENABLED             "default" (leave LiveKit's own default, which is enabled), "true",
                           or "false" -- lets a suite run compare concealment with RED forced
                           off against the baseline
  SPEECH_PHRASE           utterance to speak on loop (default: a short test phrase)
  PAUSE_SECONDS           silence between utterances, giving a subscriber's VAD real
                           speech boundaries to detect (default: 2)
"""

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
import wave

import numpy as np
from livekit import api, rtc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("client")

LIVEKIT_URL = os.getenv("LIVEKIT_URL", "ws://livekit-server:7880")
API_KEY = os.getenv("LIVEKIT_API_KEY", "devkey")
API_SECRET = os.getenv("LIVEKIT_API_SECRET", "secret")
ROOM_NAME = os.getenv("ROOM_NAME", "resilience-test")
IDENTITY = os.environ["PARTICIPANT_IDENTITY"]
PUBLISH_AUDIO = os.getenv("PUBLISH_AUDIO", "false").lower() == "true"
EVENT_LOG_PATH = os.getenv("EVENT_LOG_PATH", "/data/events.jsonl")
AUDIO_STATS_POLL_SECONDS = float(os.getenv("AUDIO_STATS_POLL_SECONDS", "2"))
RED_ENABLED = os.getenv("RED_ENABLED", "default").lower()
if RED_ENABLED not in ("default", "true", "false"):
    raise SystemExit(
        f"RED_ENABLED must be one of default/true/false, got {RED_ENABLED!r}. "
        "Failing loudly rather than falling through to the default: a typo here "
        "would silently produce a RED-on run labelled as the RED-off arm of the "
        "comparison, which is exactly the measurement the toggle exists for."
    )
SPEECH_PHRASE = os.getenv(
    "SPEECH_PHRASE", "Testing the LiveKit resilience kit, one two three four five."
)
CONNECT_MAX_RETRIES = int(os.getenv("CONNECT_MAX_RETRIES", "10"))
CONNECT_RETRY_BACKOFF_S = float(os.getenv("CONNECT_RETRY_BACKOFF_S", "2"))
PAUSE_SECONDS = float(os.getenv("PAUSE_SECONDS", "2"))
if PAUSE_SECONDS < 0:
    raise SystemExit(
        f"PAUSE_SECONDS must be >= 0, got {PAUSE_SECONDS}. A negative value crashes "
        "np.zeros() inside the publish task after the room connection is already up, "
        "which used to leave client-a connected and looking healthy while silently "
        "never publishing any audio for the rest of the run."
    )
ESPEAK_RATE_WPM = int(os.getenv("ESPEAK_RATE_WPM", "150"))

SAMPLE_RATE = 48000
NUM_CHANNELS = 1
FRAME_MS = 10
SAMPLES_PER_FRAME = SAMPLE_RATE * FRAME_MS // 1000
# How long a track's stats poller keeps failing before it gives up, expressed in
# SECONDS rather than as a poll count. A count silently couples the give-up
# threshold to AUDIO_STATS_POLL_SECONDS: the previous "10 consecutive failures"
# meant 20s at the default 2s interval, i.e. shorter than severe_outage's own
# 25s blackout, so raising the poll interval to reduce log volume would have
# made the poller abandon the track partway through the very fault it exists to
# measure. Sixty seconds is comfortably longer than any profile's outage.
STATS_POLL_GIVE_UP_SECONDS = 60.0


# Opened once and line-buffered rather than reopened per event: the stats
# poller writes a record per subscribed track every AUDIO_STATS_POLL_SECONDS
# for the whole run, and line buffering still flushes each event immediately,
# so a report can be generated against a live stack.
os.makedirs(os.path.dirname(EVENT_LOG_PATH) or ".", exist_ok=True)
_event_log = open(EVENT_LOG_PATH, "a", buffering=1)


def log_event(event: str, **fields) -> None:
    record = {"ts": time.time(), "identity": IDENTITY, "event": event, **fields}
    _event_log.write(json.dumps(record) + "\n")


def make_token() -> str:
    grants = api.VideoGrants(room_join=True, room=ROOM_NAME)
    return (
        api.AccessToken(API_KEY, API_SECRET)
        .with_identity(IDENTITY)
        .with_name(IDENTITY)
        .with_grants(grants)
        .to_jwt()
    )


def synthesize_speech(phrase: str) -> np.ndarray:
    """Generate a short utterance with espeak-ng (fully offline, no API key
    or model download) and resample it to SAMPLE_RATE.

    A continuous tone doesn't register as speech to a voice-activity
    detector -- verified directly: 3s of a pure 440Hz tone through Silero
    VAD never crossed a 0.005 speech probability (activation threshold is
    0.5), while a real espeak-ng utterance hit 0.95+ immediately. Real
    speech-shaped audio is what gives a subscriber's VAD actual utterance
    boundaries to detect.
    """
    with tempfile.NamedTemporaryFile(suffix=".wav") as tmp:
        subprocess.run(
            ["espeak-ng", "-s", str(ESPEAK_RATE_WPM), "-w", tmp.name, phrase],
            check=True,
            capture_output=True,
        )
        with wave.open(tmp.name, "rb") as w:
            raw = w.readframes(w.getnframes())
            src_rate = w.getframerate()

    samples = np.frombuffer(raw, dtype=np.int16).astype(np.float64)
    if src_rate == SAMPLE_RATE:
        return samples.astype(np.int16)
    n_target = int(len(samples) * SAMPLE_RATE / src_rate)
    resampled = np.interp(
        np.linspace(0, len(samples), n_target, endpoint=False),
        np.arange(len(samples)),
        samples,
    )
    return resampled.astype(np.int16)


async def capture_paced(
    source: rtc.AudioSource, samples: np.ndarray, deadline: float | None = None
) -> float:
    """Push `samples` into `source` as real-time-paced frames, returning the
    deadline the next call should continue from.

    Pacing is against an ABSOLUTE per-frame deadline, not a fixed
    `await asyncio.sleep(FRAME_MS / 1000)` after each frame. A fixed sleep does
    not produce a 10ms cadence: event-loop timer granularity plus the FFI
    round-trip inside capture_frame stack on top of it. Measured in this
    container, 500 back-to-back `asyncio.sleep(0.01)` calls take 6.11s of wall
    clock to cover 5.00s of audio -- 22% slower than real time, before
    capture_frame is even in the loop.

    That matters well beyond cosmetics. A source fed slower than real time
    underruns; the subscriber's jitter buffer runs dry; WebRTC fills the gap
    with packet-loss concealment. That puts a floor under `concealed_samples`
    that has nothing to do with the injected fault -- and concealed_samples is
    the single most load-bearing number this kit reports. Corroborating sign
    from a real run: the espeak utterance is 4.36s long but the agent's VAD
    measured it at 4.64s on the clean profile.

    Tracking an absolute deadline keeps the cadence honest and lets one slow
    frame be absorbed by the next sleep instead of accumulating. If we fall
    more than a frame behind, the deadline resets to now rather than bursting
    frames to "catch up", which would only trade an underrun for an overrun.
    """
    if deadline is None:
        deadline = time.monotonic()
    frame_s = FRAME_MS / 1000

    for i in range(0, len(samples), SAMPLES_PER_FRAME):
        chunk = samples[i : i + SAMPLES_PER_FRAME]
        frame = rtc.AudioFrame.create(SAMPLE_RATE, NUM_CHANNELS, SAMPLES_PER_FRAME)
        fsamples = np.frombuffer(frame.data, dtype=np.int16)
        fsamples[: len(chunk)] = chunk
        fsamples[len(chunk) :] = 0
        await source.capture_frame(frame)

        deadline += frame_s
        drift = deadline - time.monotonic()
        if drift > 0:
            await asyncio.sleep(drift)
        elif drift < -frame_s:
            deadline = time.monotonic()
    return deadline


async def publish_speech_loop(room: rtc.Room) -> None:
    source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
    track = rtc.LocalAudioTrack.create_audio_track("speech-loop", source)
    options = rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    if RED_ENABLED in ("true", "false"):
        options.red = RED_ENABLED == "true"
    await room.local_participant.publish_track(track, options)
    log.info("published speech-loop audio track (red=%s)", RED_ENABLED)

    utterance = synthesize_speech(SPEECH_PHRASE)
    silence = np.zeros(int(SAMPLE_RATE * PAUSE_SECONDS), dtype=np.int16)
    loop_samples = np.concatenate([utterance, silence])
    log.info(
        "speech loop ready: utterance=%.1fs pause=%.1fs phrase=%r",
        len(utterance) / SAMPLE_RATE,
        PAUSE_SECONDS,
        SPEECH_PHRASE,
    )

    # One deadline carried across loop iterations, so the pacing does not
    # silently reset (and re-accumulate drift) at every utterance boundary.
    deadline = None
    while True:
        deadline = await capture_paced(source, loop_samples, deadline)


async def poll_track_stats(track: rtc.Track, publication: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant) -> None:
    """Periodically poll WebRTC inbound-RTP stats for a subscribed audio
    track and log freeze/concealment metrics.

    Gaps between raw frame arrivals are NOT a reliable freeze signal here:
    Opus packet-loss concealment synthesizes filler frames on schedule during
    a real network outage, so the frame stream doesn't visibly stall even
    when audio is actually being lost. WebRTC's own inbound-RTP stats
    (total_freeze_duration, concealed_samples) account for this correctly.

    total_freeze_duration_s and jitter_buffer_delay_s are both WebRTC
    cumulative counters (they only grow), not instantaneous values — a run's
    freeze total is the last-polled value, and average buffer delay is
    jitter_buffer_delay_s / jitter_buffer_emitted_count.
    """
    consecutive_failures = 0
    # Wall-clock time the current failure streak started, not the poll count.
    # `consecutive_failures * AUDIO_STATS_POLL_SECONDS` was meant to approximate
    # elapsed time but IS a poll count in disguise: at AUDIO_STATS_POLL_SECONDS
    # >= STATS_POLL_GIVE_UP_SECONDS, the very first failure already satisfies
    # `1 * interval >= 60`, so a single transient hiccup gives up immediately
    # with zero actual retries -- reintroducing, via a different mechanism, the
    # exact "one failed poll ends collection permanently" bug this give-up
    # logic exists to avoid. Tracking real elapsed time is correct regardless
    # of how AUDIO_STATS_POLL_SECONDS is tuned.
    failures_since = None
    while True:
        await asyncio.sleep(AUDIO_STATS_POLL_SECONDS)
        try:
            stats = await track.get_stats()
        except Exception:
            # A single failed poll used to end stats collection for this track
            # permanently -- and a fault window is exactly when a poll is most
            # likely to fail, so the metric went quiet precisely where it
            # mattered. Keep polling; give up only if the track is clearly gone.
            consecutive_failures += 1
            if failures_since is None:
                failures_since = time.time()
            elapsed = time.time() - failures_since
            log.warning(
                "get_stats failed for participant=%s (%d consecutive)",
                participant.identity,
                consecutive_failures,
                exc_info=consecutive_failures == 1,
            )
            if elapsed >= STATS_POLL_GIVE_UP_SECONDS:
                log.error(
                    "giving up stats polling for participant=%s sid=%s after %d consecutive failures (%.0fs)",
                    participant.identity,
                    publication.sid,
                    consecutive_failures,
                    elapsed,
                )
                log_event(
                    "track_stats_abandoned",
                    participant=participant.identity,
                    track_sid=publication.sid,
                    consecutive_failures=consecutive_failures,
                )
                return
            continue
        consecutive_failures = 0
        failures_since = None

        for stat in stats:
            if stat.WhichOneof("stats") != "inbound_rtp":
                continue
            inbound = stat.inbound_rtp.inbound
            log_event(
                "track_stats",
                participant=participant.identity,
                track_sid=publication.sid,
                total_freeze_duration_s=inbound.total_freeze_duration,
                freeze_count=inbound.freeze_count,
                concealed_samples=inbound.concealed_samples,
                silent_concealed_samples=inbound.silent_concealed_samples,
                jitter_buffer_delay_s=inbound.jitter_buffer_delay,
                jitter_buffer_emitted_count=inbound.jitter_buffer_emitted_count,
            )


def _on_publish_task_done(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("publish loop died", exc_info=exc)
        log_event("publish_loop_failed", error=repr(exc))


async def main() -> None:
    room = rtc.Room()
    stats_tasks: dict[str, asyncio.Task] = {}
    # Pollers that have been cancelled but not yet awaited. A bare task.cancel()
    # with nobody awaiting the result lets the task's own teardown race the loop
    # shutting down -- the "Task was destroyed but it is pending!" failure the
    # agent already guards against with _cancel_and_wait. Both the unsubscribe
    # and the disconnect paths cancelled and then dropped the handle, so the
    # only tasks the shutdown gather ever saw were the ones still live. Retiring
    # them keeps every task reachable until it has actually been awaited.
    retired_tasks: set[asyncio.Task] = set()

    def retire(task: asyncio.Task) -> None:
        task.cancel()
        retired_tasks.add(task)
        task.add_done_callback(retired_tasks.discard)

    @room.on("connection_state_changed")
    def on_connection_state_changed(state: rtc.ConnectionState) -> None:
        log.info("connection_state_changed: %s", state)
        log_event("connection_state_changed", state=str(state))

    @room.on("connection_quality_changed")
    def on_connection_quality_changed(
        participant: rtc.Participant, quality: rtc.ConnectionQuality
    ) -> None:
        log.info(
            "connection_quality_changed: participant=%s quality=%s",
            participant.identity,
            quality,
        )
        log_event(
            "connection_quality_changed",
            participant=participant.identity,
            quality=str(quality),
        )

    @room.on("reconnecting")
    def on_reconnecting() -> None:
        log.warning("reconnecting")
        log_event("reconnecting")

    @room.on("reconnected")
    def on_reconnected() -> None:
        log.warning("reconnected")
        log_event("reconnected")

    @room.on("participant_connected")
    def on_participant_connected(participant: rtc.RemoteParticipant) -> None:
        log.info("participant_connected: %s", participant.identity)
        log_event("participant_connected", participant=participant.identity)

    @room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        log.info(
            "track_subscribed: participant=%s kind=%s sid=%s",
            participant.identity,
            track.kind,
            publication.sid,
        )
        log_event(
            "track_subscribed",
            participant=participant.identity,
            kind=str(track.kind),
            sid=publication.sid,
        )
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            stats_tasks[publication.sid] = asyncio.create_task(
                poll_track_stats(track, publication, participant)
            )

    @room.on("track_unsubscribed")
    def on_track_unsubscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        log.info("track_unsubscribed: participant=%s sid=%s", participant.identity, publication.sid)
        log_event("track_unsubscribed", participant=participant.identity, sid=publication.sid)
        task = stats_tasks.pop(publication.sid, None)
        if task:
            retire(task)

    @room.on("disconnected")
    def on_disconnected(reason: object = None) -> None:
        log.warning("disconnected: reason=%s", reason)
        log_event("disconnected", reason=str(reason))
        # A disconnect fires no track_unsubscribed for tracks that were still
        # live, so without this a poller outlives the track it belongs to and
        # keeps calling get_stats on it until the give-up threshold trips. The
        # agent already did this on its own disconnect; the client did not.
        for sid in list(stats_tasks):
            retire(stats_tasks.pop(sid))

    log.info("connecting to %s as %s (room=%s, publish=%s)", LIVEKIT_URL, IDENTITY, ROOM_NAME, PUBLISH_AUDIO)
    connect_start = time.time()
    log_event("connect_start")
    # docker-compose's `condition: service_started` for livekit-server (and,
    # up the chain, the agent's own healthcheck racing its actual dispatch
    # readiness) does not guarantee the server has actually finished binding
    # its port by the time this container starts. A bare, unguarded connect()
    # used to propagate straight out of main() on that race -- and with no
    # restart policy on this container, that crashed it for the rest of a
    # multi-profile suite with nothing downstream noticing. Retried with a
    # bounded backoff instead of failing on the first attempt; a genuine
    # misconfiguration (bad URL, bad credentials) still fails loudly once
    # CONNECT_MAX_RETRIES is exhausted, rather than retrying forever.
    for attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            await room.connect(LIVEKIT_URL, make_token())
            break
        except Exception as exc:
            if attempt >= CONNECT_MAX_RETRIES:
                log.error("giving up connecting to %s after %d attempts: %r", LIVEKIT_URL, attempt, exc)
                log_event("connect_failed", attempts=attempt, error=repr(exc))
                raise
            log.warning(
                "connect attempt %d/%d to %s failed: %r, retrying in %.1fs",
                attempt, CONNECT_MAX_RETRIES, LIVEKIT_URL, exc, CONNECT_RETRY_BACKOFF_S,
            )
            log_event("connect_retry", attempt=attempt, error=repr(exc))
            await asyncio.sleep(CONNECT_RETRY_BACKOFF_S)
    time_to_first_connect_ms = (time.time() - connect_start) * 1000
    log.info("connected to room %s (time_to_first_connect_ms=%.0f)", room.name, time_to_first_connect_ms)
    log_event("connected", room=room.name, time_to_first_connect_ms=time_to_first_connect_ms)

    publish_task = None
    if PUBLISH_AUDIO:
        # Held in a local for the lifetime of main(): asyncio keeps only a weak
        # reference to a running task, so a bare create_task() whose handle is
        # discarded can be garbage-collected mid-run. The done callback matters
        # just as much -- without anything awaiting or inspecting it, an
        # exception inside the publish loop vanished silently and the run
        # carried on producing a report from a stream that had stopped.
        publish_task = asyncio.create_task(publish_speech_loop(room))
        publish_task.add_done_callback(_on_publish_task_done)

    # Keep the client alive to observe events / faults.
    try:
        await asyncio.Event().wait()
    finally:
        pending = list(stats_tasks.values()) + list(retired_tasks)
        if publish_task is not None:
            pending.append(publish_task)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
