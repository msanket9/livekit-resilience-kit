"""Minimal LiveKit test client: joins a room, optionally publishes a synthetic
sine-tone audio track, and logs connection/track/quality/freeze events both
to stdout and as structured JSON lines for later analysis.

Configured entirely via environment variables so the same image can play the
publisher or subscriber persona in docker-compose:

  LIVEKIT_URL          ws(s):// URL of the LiveKit server (default: ws://livekit-server:7880)
  LIVEKIT_API_KEY      default: devkey
  LIVEKIT_API_SECRET   default: secret
  ROOM_NAME            default: resilience-test
  PARTICIPANT_IDENTITY required
  PUBLISH_AUDIO        "true" to publish a sine-tone track, otherwise subscribe-only
  EVENT_LOG_PATH          where to append JSON-line events (default: /data/events.jsonl)
  AUDIO_STATS_POLL_SECONDS  how often to poll subscribed-audio freeze/concealment stats (default: 2)
  RED_ENABLED             "default" (leave LiveKit's own default, which is enabled), "true",
                           or "false" -- lets a suite run compare concealment with RED forced
                           off against the baseline
"""

import asyncio
import json
import logging
import os
import time

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

SAMPLE_RATE = 48000
NUM_CHANNELS = 1
FRAME_MS = 10
SAMPLES_PER_FRAME = SAMPLE_RATE * FRAME_MS // 1000
TONE_HZ = 440


def log_event(event: str, **fields) -> None:
    record = {"ts": time.time(), "identity": IDENTITY, "event": event, **fields}
    with open(EVENT_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def make_token() -> str:
    grants = api.VideoGrants(room_join=True, room=ROOM_NAME)
    return (
        api.AccessToken(API_KEY, API_SECRET)
        .with_identity(IDENTITY)
        .with_name(IDENTITY)
        .with_grants(grants)
        .to_jwt()
    )


async def publish_sine_tone(room: rtc.Room) -> None:
    source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
    track = rtc.LocalAudioTrack.create_audio_track("sine-tone", source)
    options = rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    if RED_ENABLED in ("true", "false"):
        options.red = RED_ENABLED == "true"
    await room.local_participant.publish_track(track, options)
    log.info("published sine-tone audio track (red=%s)", RED_ENABLED)

    phase = 0.0
    phase_step = 2 * np.pi * TONE_HZ / SAMPLE_RATE
    while True:
        frame = rtc.AudioFrame.create(SAMPLE_RATE, NUM_CHANNELS, SAMPLES_PER_FRAME)
        samples = np.frombuffer(frame.data, dtype=np.int16)
        t = phase + phase_step * np.arange(SAMPLES_PER_FRAME)
        samples[:] = (np.sin(t) * 8000).astype(np.int16)
        phase = t[-1] + phase_step
        await source.capture_frame(frame)
        await asyncio.sleep(FRAME_MS / 1000)


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
    while True:
        await asyncio.sleep(AUDIO_STATS_POLL_SECONDS)
        try:
            stats = await track.get_stats()
        except Exception:
            log.exception("get_stats failed for participant=%s", participant.identity)
            return

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


async def main() -> None:
    room = rtc.Room()
    stats_tasks: dict[str, asyncio.Task] = {}

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
            task.cancel()

    @room.on("disconnected")
    def on_disconnected(reason: object = None) -> None:
        log.warning("disconnected: reason=%s", reason)
        log_event("disconnected", reason=str(reason))

    log.info("connecting to %s as %s (room=%s, publish=%s)", LIVEKIT_URL, IDENTITY, ROOM_NAME, PUBLISH_AUDIO)
    connect_start = time.time()
    log_event("connect_start")
    await room.connect(LIVEKIT_URL, make_token())
    time_to_first_connect_ms = (time.time() - connect_start) * 1000
    log.info("connected to room %s (time_to_first_connect_ms=%.0f)", room.name, time_to_first_connect_ms)
    log_event("connected", room=room.name, time_to_first_connect_ms=time_to_first_connect_ms)

    if PUBLISH_AUDIO:
        asyncio.create_task(publish_sine_tone(room))

    # Keep the client alive to observe events / faults.
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
