"""Minimal HTTP endpoint that receives, verifies, and logs LiveKit server
webhooks (room_started, participant_joined, track_published, ...) as JSON
lines, so a later report generator can correlate them with client-side event
logs and fault-injection windows.
"""

import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.protobuf.json_format import MessageToDict
from livekit.api.access_token import TokenVerifier
from livekit.api.webhook import WebhookReceiver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("webhook_receiver")

API_KEY = os.getenv("LIVEKIT_API_KEY", "devkey")
API_SECRET = os.getenv("LIVEKIT_API_SECRET", "secret")
LOG_PATH = os.getenv("WEBHOOK_LOG_PATH", "/data/webhooks.jsonl")
PORT = int(os.getenv("PORT", "8080"))

receiver = WebhookReceiver(TokenVerifier(API_KEY, API_SECRET))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        if self.path == "/healthz":
            self._respond(200, b"ok")
        else:
            self._respond(404, b"not found")

    def do_POST(self):
        if self.path != "/webhook":
            self._respond(404, b"not found")
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        auth_token = self.headers.get("Authorization", "")

        try:
            event = receiver.receive(body, auth_token)
        except Exception:
            log.exception("failed to verify/parse webhook payload")
            self._respond(400, b"invalid webhook payload")
            return

        record = {
            "received_ts": time.time(),
            "event": MessageToDict(event, preserving_proto_field_name=True),
        }
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")

        log.info("logged webhook event: %s", record["event"].get("event"))
        self._respond(200, b"ok")

    def _respond(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    log.info("webhook receiver listening on :%d, logging to %s", PORT, LOG_PATH)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
