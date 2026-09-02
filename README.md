# LiveKit Resilience Kit

**Work in progress.** Tests how a LiveKit deployment behaves under degraded network
conditions — packet loss, jitter, bandwidth caps, cellular-gateway-style drop/reconnect —
not just under scale. Full write-up lands once the fault profiles, metrics capture, and
report generator are in place.

## Day 1: local stack

Brings up a local LiveKit server (dev mode) plus two containerized Python clients
(`client-a` publishes a synthetic sine-tone audio track, `client-b` subscribes to it),
and a smoke test proving Pumba can inject network faults into a client container.

```bash
docker compose up --build
```

Watch `client-a` log a published track and `client-b` log `track_subscribed`. Then, with
the stack still running, in another terminal:

```bash
./scripts/pumba_smoke_test.sh
```

This pings from inside `client-b`'s container before and after Pumba applies a `netem`
delay, so you can see the fault actually land.

```bash
docker compose down
```
