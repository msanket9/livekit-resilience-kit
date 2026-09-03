"""Synthetic LiveKit Agent: a real livekit-agents worker that stands in for
an STT->LLM->TTS pipeline using local Silero VAD for turn detection and a
synthetic response tone in place of a real TTS reply.

This is explicitly NOT a conversational agent -- it's a latency/turn-taking
probe, built so the fault profiles can measure agent-specific impact
(response latency, turn-taking failures, recovery after a drop) without
needing any STT/LLM/TTS provider API key. VAD runs entirely locally.

Uses automatic dispatch (default agent_name=""): the worker registers with
the local LiveKit server and gets a job whenever a new room is created, no
explicit dispatch call needed -- it joins whatever room client-a creates.

Env vars:
  LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET   same as client.py
  EVENT_LOG_PATH   where to append JSON-line events (default: /data/agent-events.jsonl)
"""

import asyncio
import json
import logging
import os
import time

import numpy as np
from livekit import rtc
from livekit.agents import AutoSubscribe, JobContext, JobProcess, WorkerOptions, cli
from livekit.plugins import silero

# No logging.basicConfig() here: livekit-agents' cli.run_app() already
# configures the worker process's own (structured JSON) logging. Adding a
# second handler on top of it doesn't fail anything, but it does render
# every log line 2-3x -- verified in data/agent-events.jsonl that this was
# purely a duplicate-handler rendering issue, not a duplicate task/event
# bug (exact expected event counts, no double-published turns).
log = logging.getLogger("agent")

LIVEKIT_URL = os.getenv("LIVEKIT_URL", "ws://livekit-server:7880")
API_KEY = os.getenv("LIVEKIT_API_KEY", "devkey")
API_SECRET = os.getenv("LIVEKIT_API_SECRET", "secret")
EVENT_LOG_PATH = os.getenv("EVENT_LOG_PATH", "/data/agent-events.jsonl")

SAMPLE_RATE = 48000
NUM_CHANNELS = 1
FRAME_MS = 10
SAMPLES_PER_FRAME = SAMPLE_RATE * FRAME_MS // 1000
RESPONSE_TONE_HZ = 880
RESPONSE_DURATION_S = 1.0


# Opened once and line-buffered rather than reopened per event; line buffering
# still flushes each record immediately, so a report can be generated against a
# live stack.
os.makedirs(os.path.dirname(EVENT_LOG_PATH) or ".", exist_ok=True)
_event_log = open(EVENT_LOG_PATH, "a", buffering=1)


def log_event(identity: str, event: str, **fields) -> None:
    record = {"ts": time.time(), "identity": identity, "event": event, **fields}
    _event_log.write(json.dumps(record) + "\n")


def make_response_tone() -> np.ndarray:
    n_samples = int(SAMPLE_RATE * RESPONSE_DURATION_S)
    t = np.arange(n_samples) * (2 * np.pi * RESPONSE_TONE_HZ / SAMPLE_RATE)
    return (np.sin(t) * 8000).astype(np.int16)


async def push_samples(source: rtc.AudioSource, samples: np.ndarray, on_first_frame=None) -> None:
    """Push samples as real-time-paced frames. If given, on_first_frame() is
    called right after the FIRST frame is captured -- not after the whole
    clip finishes -- since "response latency" means time-to-first-frame, not
    time-to-finish-playing (a 1s response tone would otherwise always read
    back as >=1000ms regardless of how quickly the response actually
    started).

    Pacing is against an absolute per-frame deadline rather than a fixed
    `asyncio.sleep(FRAME_MS / 1000)` after each frame, for the same reason as
    the publisher's own loop in client.py: a fixed sleep runs ~22% slower than
    real time here once event-loop timer granularity and the FFI round-trip are
    counted, which underfeeds the source and makes the subscriber conceal audio
    that was never actually lost.
    """
    frame_s = FRAME_MS / 1000
    deadline = time.monotonic()

    for i in range(0, len(samples), SAMPLES_PER_FRAME):
        chunk = samples[i : i + SAMPLES_PER_FRAME]
        frame = rtc.AudioFrame.create(SAMPLE_RATE, NUM_CHANNELS, SAMPLES_PER_FRAME)
        fsamples = np.frombuffer(frame.data, dtype=np.int16)
        fsamples[: len(chunk)] = chunk
        fsamples[len(chunk) :] = 0
        await source.capture_frame(frame)
        if i == 0 and on_first_frame is not None:
            on_first_frame()

        deadline += frame_s
        drift = deadline - time.monotonic()
        if drift > 0:
            await asyncio.sleep(drift)
        elif drift < -frame_s:
            deadline = time.monotonic()


async def watch_turns(
    identity: str,
    vad: "silero.VAD",
    track: rtc.Track,
    response_source: rtc.AudioSource,
    response_tone: np.ndarray,
) -> None:
    """Feed a subscribed audio track into local VAD and log turn boundaries.

    On END_OF_SPEECH, immediately publishes a synthetic response tone -- the
    stand-in for a real TTS reply -- and logs how long the caller waited for
    it.

    `vad` is the prewarmed, process-wide model (see prewarm()), not one loaded
    here. VAD.load() is a blocking call -- its own docstring says to run it in
    a prewarm mechanism -- and this function runs once per track subscription,
    so loading inside it put a synchronous model load on the event loop at
    every reconnect. Measured at ~30ms, small, but it lands at exactly the
    moment the agent-recovery metric is being timed.
    """
    vad_stream = vad.stream()
    audio_stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)

    # Wall-clock time the VAD last actually received a frame. The VAD measures
    # silence in AUDIO time, so when frames stop arriving entirely it does not
    # accumulate silence -- it simply stalls. That makes silence_duration an
    # unreliable basis for inferring wall-clock end-of-speech during exactly
    # the faults this kit injects. Logging the wall-clock gap alongside the
    # latency lets a distorted reading be recognised as distorted instead of
    # being read as a genuine sub-second response during a blackout.
    last_frame_at = {"ts": time.time()}

    async def feed() -> None:
        async for event in audio_stream:
            last_frame_at["ts"] = time.time()
            vad_stream.push_frame(event.frame)
        vad_stream.end_input()

    async def consume() -> None:
        async for event in vad_stream:
            if event.type.name == "START_OF_SPEECH":
                log.info("speech_start")
                log_event(identity, "speech_start")
            elif event.type.name == "END_OF_SPEECH":
                log.info("speech_end: duration=%.2fs", event.speech_duration)
                log_event(identity, "speech_end", speech_duration=event.speech_duration)

                # The VAD only fires END_OF_SPEECH after holding
                # min_silence_duration (0.55s by default) of silence, so by the
                # time this event is consumed the speaker actually stopped
                # talking event.silence_duration ago. Measuring from *here* to
                # the first response frame therefore measured nothing: across
                # a whole run it read 0.40-1.18ms with no separation between
                # the clean profile and a 25s outage, because all it timed was
                # allocating one AudioFrame. What a caller experiences is the
                # gap from their last word to the first sound back, so that is
                # what response_latency_ms now reports -- VAD hold included.
                #
                # Caveat worth knowing before trusting it under fault: the VAD
                # measures silence in audio time, so if frames stop arriving
                # altogether it stalls rather than accumulating silence. This
                # inference then understates the wall-clock wait. That is what
                # since_last_frame_ms below is for -- a large value means this
                # latency reading is distorted by a stall, not a real fast
                # response.
                event_consumed_at = time.time()
                speech_ended_at = event_consumed_at - (event.silence_duration or 0.0)
                # Snapshot the stall gap HERE, not after push_samples returns:
                # feed() keeps running while the response is being published, so
                # reading last_frame_at afterwards measures the ~1s spent
                # publishing and comes out negative.
                since_last_frame_ms = (event_consumed_at - last_frame_at["ts"]) * 1000
                first_frame_ts = {}

                def _mark_first_frame() -> None:
                    first_frame_ts["ts"] = time.time()

                await push_samples(response_source, response_tone, on_first_frame=_mark_first_frame)
                response_latency_ms = (first_frame_ts["ts"] - speech_ended_at) * 1000
                # The publish-side half on its own, kept so a regression in the
                # agent's own code path stays visible separately from the VAD's
                # fixed detection hold, which would otherwise dominate it.
                publish_latency_ms = (first_frame_ts["ts"] - event_consumed_at) * 1000
                log.info(
                    "response_published: latency_ms=%.0f (publish %.2fms, vad hold %.0fms)",
                    response_latency_ms,
                    publish_latency_ms,
                    (event_consumed_at - speech_ended_at) * 1000,
                )
                log_event(
                    identity,
                    "response_published",
                    response_latency_ms=response_latency_ms,
                    publish_latency_ms=publish_latency_ms,
                    vad_silence_hold_ms=(event_consumed_at - speech_ended_at) * 1000,
                    since_last_frame_ms=since_last_frame_ms,
                )

    feed_task = asyncio.create_task(feed())
    consume_task = asyncio.create_task(consume())
    try:
        # gather() propagates the first exception but leaves the other task
        # running, so a failure in one half used to leak the other for the
        # lifetime of the worker. Cancelling both explicitly in the finally
        # covers that as well as ordinary cancellation from a resubscribe.
        await asyncio.gather(feed_task, consume_task)
    finally:
        for task in (feed_task, consume_task):
            task.cancel()
        await asyncio.gather(feed_task, consume_task, return_exceptions=True)
        # Track resubscription (e.g. after a full LiveKit reconnect -- a new
        # track sid, verified to actually happen under severe_outage) cancels
        # this task. Without explicitly closing these, VAD's own internal
        # background tasks (metrics/main loop) get abandoned rather than
        # shut down, which asyncio logs as "Task was destroyed but it is
        # pending!" -- noisy, and a real per-reconnect resource leak over a
        # long-running agent.
        await vad_stream.aclose()
        await audio_stream.aclose()


def prewarm(proc: JobProcess) -> None:
    """Load the Silero VAD model once per worker process, before any job runs.

    silero.VAD.load() is blocking ("It is recommended to call this method
    inside your prewarm mechanism", per the plugin's own docstring). It used to
    be called inside watch_turns, i.e. once per track subscription, which put a
    synchronous load on the event loop every time a track was resubscribed
    after a reconnect."""
    proc.userdata["vad"] = silero.VAD.load()


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
    identity = ctx.room.local_participant.identity
    vad = ctx.proc.userdata["vad"]
    log.info("agent connected to room %s as %s", ctx.room.name, identity)

    @ctx.room.on("connection_state_changed")
    def on_connection_state_changed(state: rtc.ConnectionState) -> None:
        log_event(identity, "connection_state_changed", state=str(state))

    @ctx.room.on("connection_quality_changed")
    def on_connection_quality_changed(
        participant: rtc.Participant, quality: rtc.ConnectionQuality
    ) -> None:
        log_event(
            identity,
            "connection_quality_changed",
            participant=participant.identity,
            quality=str(quality),
        )

    @ctx.room.on("reconnecting")
    def on_reconnecting() -> None:
        log.warning("reconnecting")
        log_event(identity, "reconnecting")

    @ctx.room.on("reconnected")
    def on_reconnected() -> None:
        log.warning("reconnected")
        log_event(identity, "reconnected")

    response_source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
    response_track = rtc.LocalAudioTrack.create_audio_track("agent-response", response_source)
    await ctx.room.local_participant.publish_track(
        response_track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    )
    response_tone = make_response_tone()
    log.info("agent response track published")

    watch_tasks: dict[str, asyncio.Task] = {}
    # asyncio holds only a weak reference to a running task, so a create_task()
    # whose handle is dropped can be garbage-collected before it finishes. These
    # cleanup tasks close VAD streams, so losing one reintroduces the very leak
    # _cancel_and_wait exists to prevent. Held until they complete.
    cleanup_tasks: set[asyncio.Task] = set()

    def _spawn_cleanup(task: asyncio.Task) -> None:
        cleanup = asyncio.create_task(_cancel_and_wait(task))
        cleanup_tasks.add(cleanup)
        cleanup.add_done_callback(cleanup_tasks.discard)

    @ctx.room.on("disconnected")
    def on_disconnected(reason: object = None) -> None:
        log.warning("disconnected: reason=%s", reason)
        log_event(identity, "disconnected", reason=str(reason))
        # A disconnect fires no track_unsubscribed for tracks that were still
        # live, so without this the watch task (and its VAD stream) survives
        # the room it belonged to.
        for sid, task in list(watch_tasks.items()):
            watch_tasks.pop(sid, None)
            _spawn_cleanup(task)

    @ctx.room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        log.info("track_subscribed: participant=%s kind=%s", participant.identity, track.kind)
        log_event(identity, "track_subscribed", participant=participant.identity, sid=publication.sid)
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            watch_tasks[publication.sid] = asyncio.create_task(
                watch_turns(identity, vad, track, response_source, response_tone)
            )

    @ctx.room.on("track_unsubscribed")
    def on_track_unsubscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        log.info("track_unsubscribed: participant=%s sid=%s", participant.identity, publication.sid)
        log_event(identity, "track_unsubscribed", participant=participant.identity, sid=publication.sid)
        task = watch_tasks.pop(publication.sid, None)
        if task:
            _spawn_cleanup(task)


async def _cancel_and_wait(task: asyncio.Task) -> None:
    """Cancel a watch_turns task and actually wait for it -- a bare
    task.cancel() with nobody awaiting the result lets the task's `finally`
    cleanup (closing the VAD stream) race the task's own destruction, which
    is what produced "Task was destroyed but it is pending!" during a real
    reconnect test."""
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception("error cleaning up a watch_turns task")


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            prewarm_fnc=prewarm,
            ws_url=LIVEKIT_URL,
            api_key=API_KEY,
            api_secret=API_SECRET,
        )
    )
