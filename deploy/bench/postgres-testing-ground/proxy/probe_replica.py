#!/usr/bin/env python3
"""Stateless read-only probe replica for non-sticky L7 and R2 fallback checks."""

import base64
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
import socket
import struct
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
import urllib.request

REPLICA_ID = os.environ.get("REPLICA_ID", "probe-reader-unknown")
PROBE_PORT = int(os.environ.get("PROBE_PORT", "8081"))
PGBOUNCER_HOST = os.environ.get("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.environ.get("PGBOUNCER_PORT", "6432"))
MOCK_R2_BASE = os.environ.get("MOCK_R2_BASE", "http://mock-r2:9000").rstrip("/")
SAMPLE_BLOCK_HEIGHT = int(os.environ.get("SAMPLE_BLOCK_HEIGHT", "2"))
SAMPLE_BLOCK_HASH_HEX = os.environ.get(
    "SAMPLE_BLOCK_HASH_HEX",
    "0202020202020202020202020202020202020202020202020202020202020202",
)


def is_rootfs_read_only() -> bool:
    try:
        with open("/zinder-ro-probe", "wb") as handle:
            handle.write(b"x")
    except OSError:
        return True
    try:
        os.unlink("/zinder-ro-probe")
    except OSError:
        pass
    return False


def local_database_entries() -> list[str]:
    found: list[str] = []
    for root in ("/tmp",):
        if not os.path.isdir(root):
            continue
        for entry in os.listdir(root):
            if entry.endswith((".rocksdb", ".zinder-store", ".sqlite", ".sqlite3", ".db")):
                found.append(os.path.join(root, entry))
    return sorted(found)


def pgbouncer_tcp_reachable() -> bool:
    try:
        with socket.create_connection((PGBOUNCER_HOST, PGBOUNCER_PORT), timeout=1.0):
            return True
    except OSError:
        return False


def encode_varint(number: int) -> bytes:
    out = bytearray()
    remaining = number
    while True:
        chunk = remaining & 0x7F
        remaining >>= 7
        if remaining:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def encode_block_id_proto(height: int, hash_hex: str) -> bytes:
    raw_hash = bytes.fromhex(hash_hex)
    return b"\x08" + encode_varint(height) + b"\x12" + encode_varint(len(raw_hash)) + raw_hash


def frame_grpc_web_response(message_bytes: bytes, *, base64_text: bool) -> bytes:
    data_frame = b"\x00" + struct.pack(">I", len(message_bytes)) + message_bytes
    trailers = b"grpc-status:0\r\n"
    trailer_frame = b"\x80" + struct.pack(">I", len(trailers)) + trailers
    framed = data_frame + trailer_frame
    return base64.b64encode(framed) if base64_text else framed


def fetch_segment_once(url: str, timeout_sec: float) -> tuple[bytes, str]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        expected_sha = resp.headers.get("X-Zinder-Segment-Sha256", "")
        payload = resp.read()
    actual_sha = hashlib.sha256(payload).hexdigest()
    if not expected_sha or actual_sha != expected_sha:
        raise ValueError("digest_mismatch")
    return payload, actual_sha


class ProbeReplicaHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("X-Zinder-Replica", REPLICA_ID)
        self.end_headers()
        self.wfile.write(encoded)

    def _read_body(self) -> bytes:
        raw_len = self.headers.get("Content-Length")
        if not raw_len:
            return b""
        return self.rfile.read(max(0, int(raw_len)))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ("/healthz", "/readyz"):
            ro = is_rootfs_read_only()
            db_files = local_database_entries()
            pg_ok = pgbouncer_tcp_reachable()
            status = HTTPStatus.OK if (ro and not db_files) else HTTPStatus.SERVICE_UNAVAILABLE
            self._send_json(
                status,
                {
                    "status": "ok" if status == HTTPStatus.OK else "unhealthy",
                    "replica_id": REPLICA_ID,
                    "readonly_rootfs": ro,
                    "local_db_files": db_files,
                    "pgbouncer_tcp_reachable": pg_ok,
                },
            )
            return

        if parsed.path in ("/whoami", "/api/v1/status"):
            self._send_json(
                HTTPStatus.OK,
                {
                    "replica_id": REPLICA_ID,
                    "height": SAMPLE_BLOCK_HEIGHT,
                    "hash": SAMPLE_BLOCK_HASH_HEX,
                    "readonly_rootfs": is_rootfs_read_only(),
                },
            )
            return

        if parsed.path.startswith("/probe/r2-segment/"):
            key = parsed.path[len("/probe/r2-segment/"):]
            params = parse_qs(parsed.query)
            timeout_ms = int(params.get("timeout_ms", ["300"])[0])
            timeout_sec = max(0.05, min(timeout_ms / 1000.0, 10.0))
            self._probe_r2_segment(key, timeout_sec)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown route", "replica_id": REPLICA_ID})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown gRPC-Web method"})
            return
        body = self._read_body()
        content_type = self.headers.get("Content-Type", "").lower()
        is_text = "grpc-web-text" in content_type
        raw_frame = base64.b64decode(body) if is_text else body
        if len(raw_frame) < 5 or raw_frame[0] != 0:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid grpc-web request frame"})
            return
        msg_len = struct.unpack(">I", raw_frame[1:5])[0]
        if len(raw_frame) != 5 + msg_len:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "truncated grpc-web request frame"})
            return

        response_proto = encode_block_id_proto(SAMPLE_BLOCK_HEIGHT, SAMPLE_BLOCK_HASH_HEX)
        response_wire = frame_grpc_web_response(response_proto, base64_text=is_text)
        response_ct = (
            "application/grpc-web-text+proto" if is_text else "application/grpc-web+proto"
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", response_ct)
        self.send_header("Content-Length", str(len(response_wire)))
        self.send_header("X-Zinder-Replica", REPLICA_ID)
        self.end_headers()
        self.wfile.write(response_wire)

    def _probe_r2_segment(self, key: str, timeout_sec: float) -> None:
        try:
            payload, sha256 = fetch_segment_once(f"{MOCK_R2_BASE}/segments/{key}", timeout_sec)
            self._send_json(
                HTTPStatus.OK,
                {
                    "replica_id": REPLICA_ID,
                    "key": key,
                    "source": "r2_cache",
                    "fallback_reason": None,
                    "sha256": sha256,
                    "bytes": len(payload),
                },
            )
            return
        except HTTPError as exc:
            reason = f"http_{exc.code}"
        except (URLError, socket.timeout, TimeoutError):
            reason = "timeout"
        except ValueError as exc:
            reason = str(exc)

        try:
            payload, sha256 = fetch_segment_once(
                f"{MOCK_R2_BASE}/origin/segments/{key}", max(timeout_sec, 2.0)
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.BAD_GATEWAY,
                {
                    "replica_id": REPLICA_ID,
                    "key": key,
                    "error": f"origin fallback failed after {reason}: {exc}",
                },
            )
            return

        self._send_json(
            HTTPStatus.OK,
            {
                "replica_id": REPLICA_ID,
                "key": key,
                "source": "primary_fallback",
                "fallback_reason": reason,
                "sha256": sha256,
                "bytes": len(payload),
            },
        )


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PROBE_PORT), ProbeReplicaHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
