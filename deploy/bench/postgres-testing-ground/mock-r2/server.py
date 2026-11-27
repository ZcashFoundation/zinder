#!/usr/bin/env python3
"""Mock S3 / Cloudflare R2 settled-segment cache with fault injection."""

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
import re
import threading
import time
from urllib.parse import urlparse

VALID_MODES = frozenset({"normal", "miss_404", "outage_503", "timeout_ms", "corrupt_payload"})
KEY_RE = re.compile(r"^[A-Za-z0-9._/-]{1,128}$")
RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")


class SegmentCacheState:
    """Thread-safe in-memory object store, metrics, and fault configuration."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.fault_mode = "normal"
        self.delay_ms = 0
        self.metrics = {
            "puts": 0,
            "hits": 0,
            "misses": 0,
            "range_hits": 0,
            "outage_503": 0,
            "timeout_injections": 0,
            "corrupt_injections": 0,
            "bytes_served": 0,
        }


STATE = SegmentCacheState()


def validate_key(raw_key: str) -> str | None:
    if not raw_key or ".." in raw_key or raw_key.startswith("/") or "//" in raw_key:
        return None
    if not KEY_RE.match(raw_key):
        return None
    return raw_key


def parse_fault_payload(body: bytes) -> tuple[str, int]:
    decoded = json.loads(body.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("fault payload must be a JSON object")
    mode = decoded.get("mode")
    delay_ms = decoded.get("delay_ms", 0)
    if not isinstance(mode, str) or mode not in VALID_MODES:
        raise ValueError(f"mode must be one of {sorted(VALID_MODES)}")
    if isinstance(delay_ms, bool) or not isinstance(delay_ms, int) or not (0 <= delay_ms <= 30_000):
        raise ValueError("delay_ms must be an integer between 0 and 30000")
    return mode, delay_ms


class MockR2Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(encoded)

    def _read_body(self) -> bytes | None:
        raw_len = self.headers.get("Content-Length")
        if raw_len is None:
            return b""
        try:
            length = int(raw_len)
        except ValueError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid Content-Length"})
            return None
        if length < 0 or length > 16 * 1024 * 1024:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Content-Length out of range"})
            return None
        return self.rfile.read(length)

    def do_GET(self) -> None:
        self._handle_read(head_only=False)

    def do_HEAD(self) -> None:
        self._handle_read(head_only=True)

    def do_PUT(self) -> None:
        path = urlparse(self.path).path
        if not path.startswith("/segments/"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown path"})
            return
        key = validate_key(path[len("/segments/"):])
        if key is None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid segment key"})
            return
        body = self._read_body()
        if body is None:
            return
        digest = hashlib.sha256(body).hexdigest()
        existing_digest = None
        put_outcome = "created"
        with STATE.lock:
            existing = STATE.objects.get(key)
            if existing is not None:
                existing_body, existing_digest = existing
                if existing_digest != digest or existing_body != body:
                    put_outcome = "conflict"
                else:
                    put_outcome = "idempotent"
            else:
                STATE.objects[key] = (body, digest)
                STATE.metrics["puts"] += 1
        if put_outcome == "conflict":
            self._send_json(
                HTTPStatus.CONFLICT,
                {
                    "error": "segment key already exists with different payload",
                    "key": key,
                    "existing_sha256": existing_digest,
                    "requested_sha256": digest,
                },
            )
            return
        if put_outcome == "idempotent":
            self._send_json(
                HTTPStatus.OK,
                {"key": key, "bytes": len(body), "sha256": digest, "idempotent": True},
            )
            return
        self._send_json(HTTPStatus.CREATED, {"key": key, "bytes": len(body), "sha256": digest})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path != "/fault":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown path"})
            return
        body = self._read_body()
        if body is None:
            return
        try:
            mode, delay_ms = parse_fault_payload(body)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        with STATE.lock:
            STATE.fault_mode = mode
            STATE.delay_ms = delay_ms
        self._send_json(HTTPStatus.OK, {"mode": mode, "delay_ms": delay_ms})

    def _handle_read(self, *, head_only: bool) -> None:
        path = urlparse(self.path).path
        if path == "/healthz":
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if path == "/metrics":
            with STATE.lock:
                snapshot = {
                    "objects": len(STATE.objects),
                    "fault_mode": STATE.fault_mode,
                    "delay_ms": STATE.delay_ms,
                    **STATE.metrics,
                }
            self._send_json(HTTPStatus.OK, snapshot)
            return
        if path == "/fault":
            with STATE.lock:
                snapshot = {"mode": STATE.fault_mode, "delay_ms": STATE.delay_ms}
            self._send_json(HTTPStatus.OK, snapshot)
            return

        if path.startswith("/origin/segments/"):
            key = validate_key(path[len("/origin/segments/"):])
            self._serve_segment(key, bypass_faults=True, head_only=head_only)
            return
        if path.startswith("/segments/"):
            key = validate_key(path[len("/segments/"):])
            self._serve_segment(key, bypass_faults=False, head_only=head_only)
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown path"})

    def _serve_segment(self, key: str | None, *, bypass_faults: bool, head_only: bool) -> None:
        if key is None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid segment key"})
            return
        with STATE.lock:
            stored = STATE.objects.get(key)
            mode = "normal" if bypass_faults else STATE.fault_mode
            delay_ms = 0 if bypass_faults else STATE.delay_ms
            if not bypass_faults:
                if mode == "miss_404":
                    STATE.metrics["misses"] += 1
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "injected miss_404"})
                    return
                if mode == "outage_503":
                    STATE.metrics["outage_503"] += 1
                    self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "injected outage_503"})
                    return
                if stored is None:
                    STATE.metrics["misses"] += 1
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "segment not cached"})
                    return
            elif stored is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "origin segment not found"})
                return
        if mode == "timeout_ms" and delay_ms > 0:
            with STATE.lock:
                STATE.metrics["timeout_injections"] += 1
            time.sleep(delay_ms / 1000.0)

        payload, digest = stored
        if mode == "corrupt_payload" and payload:
            payload = bytes([payload[0] ^ 0xFF]) + payload[1:]
            with STATE.lock:
                STATE.metrics["corrupt_injections"] += 1

        self._write_segment_bytes(payload, digest, count_cache_hit=not bypass_faults, head_only=head_only)

    def _write_segment_bytes(
        self,
        payload: bytes,
        digest: str,
        *,
        count_cache_hit: bool,
        head_only: bool,
    ) -> None:
        total = len(payload)
        range_header = self.headers.get("Range")
        if range_header:
            matched = RANGE_RE.match(range_header.strip())
            if matched is None:
                self._send_json(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, {"error": "invalid range"})
                return
            start = int(matched.group(1))
            end = int(matched.group(2)) if matched.group(2) else max(total - 1, 0)
            if total == 0 or start >= total or end < start:
                self._send_json(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, {"error": "range out of bounds"})
                return
            end = min(end, total - 1)
            chunk = payload[start : end + 1]
            with STATE.lock:
                if count_cache_hit:
                    STATE.metrics["range_hits"] += 1
                STATE.metrics["bytes_served"] += len(chunk)
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("ETag", f'"{digest}"')
            self.send_header("X-Zinder-Segment-Sha256", digest)
            self.end_headers()
            if not head_only:
                self.wfile.write(chunk)
            return

        with STATE.lock:
            if count_cache_hit:
                STATE.metrics["hits"] += 1
            STATE.metrics["bytes_served"] += total
        self.send_response(HTTPStatus.OK)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(total))
        self.send_header("ETag", f'"{digest}"')
        self.send_header("X-Zinder-Segment-Sha256", digest)
        self.end_headers()
        if not head_only:
            self.wfile.write(payload)


def main() -> None:
    port = int(os.environ.get("MOCK_R2_PORT", "9000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), MockR2Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
