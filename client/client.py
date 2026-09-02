"""Minimal LiveKit test client: joins a room, optionally publishes a synthetic
sine-tone audio track, and logs connection/track/quality events to stdout.

Configured entirely via environment variables so the same image can play the
publisher or subscriber persona in docker-compose:

  LIVEKIT_URL          ws(s):// URL of the LiveKit server (default: ws://livekit-server:7880)
  LIVEKIT_API_KEY      default: devkey
  LIVEKIT_API_SECRET   default: secret
  ROOM_NAME            default: resilience-test
  PARTICIPANT_IDENTITY required
  PUBLISH_AUDIO        "true" to publish a sine-tone track, otherwise subscribe-only
"""

import asyncio
import logging
import os

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

SAMPLE_RATE = 48000
NUM_CHANNELS = 1
FRAME_MS = 10
SAMPLES_PER_FRAME = SAMPLE_RATE * FRAME_MS // 1000
TONE_HZ = 440


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
    await room.local_participant.publish_track(track, options)
    log.info("published sine-tone audio track")

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


async def main() -> None:
    room = rtc.Room()

    @room.on("connection_state_changed")
    def on_connection_state_changed(state: rtc.ConnectionState) -> None:
        log.info("connection_state_changed: %s", state)

    @room.on("connection_quality_changed")
    def on_connection_quality_changed(
        participant: rtc.Participant, quality: rtc.ConnectionQuality
    ) -> None:
        log.info(
            "connection_quality_changed: participant=%s quality=%s",
            participant.identity,
            quality,
        )

    @room.on("participant_connected")
    def on_participant_connected(participant: rtc.RemoteParticipant) -> None:
        log.info("participant_connected: %s", participant.identity)

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

    @room.on("disconnected")
    def on_disconnected(reason: object = None) -> None:
        log.warning("disconnected: reason=%s", reason)

    log.info("connecting to %s as %s (room=%s, publish=%s)", LIVEKIT_URL, IDENTITY, ROOM_NAME, PUBLISH_AUDIO)
    await room.connect(LIVEKIT_URL, make_token())
    log.info("connected to room %s", room.name)

    if PUBLISH_AUDIO:
        asyncio.create_task(publish_sine_tone(room))

    # Keep the client alive to observe events / faults.
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
