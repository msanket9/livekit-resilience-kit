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
from livekit.agents import AutoSubscribe, JobContext, WorkerOptions, cli
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


def log_event(identity: str, event: str, **fields) -> None:
    record = {"ts": time.time(), "identity": identity, "event": event, **fields}
    with open(EVENT_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


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
    started)."""
    for i in range(0, len(samples), SAMPLES_PER_FRAME):
        chunk = samples[i : i + SAMPLES_PER_FRAME]
        frame = rtc.AudioFrame.create(SAMPLE_RATE, NUM_CHANNELS, SAMPLES_PER_FRAME)
        fsamples = np.frombuffer(frame.data, dtype=np.int16)
        fsamples[: len(chunk)] = chunk
        fsamples[len(chunk) :] = 0
        await source.capture_frame(frame)
        if i == 0 and on_first_frame is not None:
            on_first_frame()
        await asyncio.sleep(FRAME_MS / 1000)


async def watch_turns(
    identity: str,
    track: rtc.Track,
    response_source: rtc.AudioSource,
    response_tone: np.ndarray,
) -> None:
    """Feed a subscribed audio track into local VAD and log turn boundaries.

    On END_OF_SPEECH, immediately publishes a synthetic response tone -- the
    stand-in for a real TTS reply -- and logs the latency from turn-end to
    first response frame pushed.
    """
    vad = silero.VAD.load()
    vad_stream = vad.stream()
    audio_stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)

    async def feed() -> None:
        async for event in audio_stream:
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

                response_start = time.time()
                first_frame_ts = {}

                def _mark_first_frame() -> None:
                    first_frame_ts["ts"] = time.time()

                await push_samples(response_source, response_tone, on_first_frame=_mark_first_frame)
                response_latency_ms = (first_frame_ts["ts"] - response_start) * 1000
                log.info("response_published: latency_ms=%.0f", response_latency_ms)
                log_event(identity, "response_published", response_latency_ms=response_latency_ms)

    try:
        await asyncio.gather(feed(), consume())
    finally:
        # Track resubscription (e.g. after a full LiveKit reconnect -- a new
        # track sid, verified to actually happen under severe_outage) cancels
        # this task. Without explicitly closing these, VAD's own internal
        # background tasks (metrics/main loop) get abandoned rather than
        # shut down, which asyncio logs as "Task was destroyed but it is
        # pending!" -- noisy, and a real per-reconnect resource leak over a
        # long-running agent.
        await vad_stream.aclose()
        await audio_stream.aclose()


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
    identity = ctx.room.local_participant.identity
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

    @ctx.room.on("disconnected")
    def on_disconnected(reason: object = None) -> None:
        log.warning("disconnected: reason=%s", reason)
        log_event(identity, "disconnected", reason=str(reason))

    response_source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
    response_track = rtc.LocalAudioTrack.create_audio_track("agent-response", response_source)
    await ctx.room.local_participant.publish_track(
        response_track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    )
    response_tone = make_response_tone()
    log.info("agent response track published")

    watch_tasks: dict[str, asyncio.Task] = {}

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
                watch_turns(identity, track, response_source, response_tone)
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
            asyncio.create_task(_cancel_and_wait(task))


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
            ws_url=LIVEKIT_URL,
            api_key=API_KEY,
            api_secret=API_SECRET,
        )
    )
