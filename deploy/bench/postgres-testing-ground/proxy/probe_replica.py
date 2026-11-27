#!/usr/bin/env python3
"""Stateless read-only probe replica for non-sticky L7 (REST, gRPC-Web, native h2c) and R2 fallback checks."""

import base64
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
import socket
import socketserver
import struct
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
import urllib.request

REPLICA_ID = os.environ.get("REPLICA_ID", "probe-reader-unknown")
PROBE_PORT = int(os.environ.get("PROBE_PORT", "8081"))
PROBE_GRPC_PORT = int(os.environ.get("PROBE_GRPC_PORT", "8082"))
PGBOUNCER_HOST = os.environ.get("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.environ.get("PGBOUNCER_PORT", "6432"))
MOCK_R2_BASE = os.environ.get("MOCK_R2_BASE", "http://mock-r2:9000").rstrip("/")
SAMPLE_BLOCK_HEIGHT = int(os.environ.get("SAMPLE_BLOCK_HEIGHT", "2"))
SAMPLE_BLOCK_HASH_HEX = os.environ.get(
    "SAMPLE_BLOCK_HASH_HEX",
    "0202020202020202020202020202020202020202020202020202020202020202",
)
GET_LATEST_BLOCK_PATH = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock"

H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
H2_FRAME_DATA = 0x00
H2_FRAME_HEADERS = 0x01
H2_FRAME_PRIORITY = 0x02
H2_FRAME_RST_STREAM = 0x03
H2_FRAME_SETTINGS = 0x04
H2_FRAME_PING = 0x06
H2_FRAME_GOAWAY = 0x07
H2_FRAME_WINDOW_UPDATE = 0x08
H2_FRAME_CONTINUATION = 0x09

H2_FLAG_END_STREAM = 0x01
H2_FLAG_ACK = 0x01
H2_FLAG_END_HEADERS = 0x04
H2_FLAG_PADDED = 0x08
H2_FLAG_PRIORITY = 0x20

# RFC 7541 Appendix A static table (1-based indices)
HPACK_STATIC_TABLE: tuple[tuple[str, str], ...] = (
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
)

# RFC 7541 Appendix B Huffman table: (bit_length, code)
HPACK_HUFFMAN_CODES: tuple[tuple[int, int], ...] = (
    (13, 0x1ff8), (23, 0x7fffd8), (28, 0xfffffe2), (28, 0xfffffe3), (28, 0xfffffe4), (28, 0xfffffe5), (28, 0xfffffe6), (28, 0xfffffe7),
    (28, 0xfffffe8), (24, 0xffffea), (30, 0x3ffffffc), (28, 0xfffffe9), (28, 0xfffffea), (30, 0x3ffffffd), (28, 0xfffffeb), (28, 0xfffffec),
    (28, 0xfffffed), (28, 0xfffffee), (28, 0xfffffef), (28, 0xffffff0), (28, 0xffffff1), (28, 0xffffff2), (30, 0x3ffffffe), (28, 0xffffff3),
    (28, 0xffffff4), (28, 0xffffff5), (28, 0xffffff6), (28, 0xffffff7), (28, 0xffffff8), (28, 0xffffff9), (28, 0xffffffa), (28, 0xffffffb),
    (6, 0x14), (10, 0x3f8), (10, 0x3f9), (12, 0xffa), (13, 0x1ff9), (6, 0x15), (8, 0xf8), (11, 0x7fa),
    (10, 0x3fa), (10, 0x3fb), (8, 0xf9), (11, 0x7fb), (8, 0xfa), (6, 0x16), (6, 0x17), (6, 0x18),
    (5, 0x0), (5, 0x1), (5, 0x2), (6, 0x19), (6, 0x1a), (6, 0x1b), (6, 0x1c), (6, 0x1d),
    (6, 0x1e), (6, 0x1f), (7, 0x5c), (8, 0xfb), (15, 0x7ffc), (6, 0x20), (12, 0xffb), (10, 0x3fc),
    (13, 0x1ffa), (6, 0x21), (7, 0x5d), (7, 0x5e), (7, 0x5f), (7, 0x60), (7, 0x61), (7, 0x62),
    (7, 0x63), (7, 0x64), (7, 0x65), (7, 0x66), (7, 0x67), (7, 0x68), (7, 0x69), (7, 0x6a),
    (7, 0x6b), (7, 0x6c), (7, 0x6d), (7, 0x6e), (7, 0x6f), (7, 0x70), (7, 0x71), (7, 0x72),
    (8, 0xfc), (7, 0x73), (8, 0xfd), (13, 0x1ffb), (19, 0x7fff0), (13, 0x1ffc), (14, 0x3ffc), (6, 0x22),
    (15, 0x7ffd), (5, 0x3), (6, 0x23), (5, 0x4), (6, 0x24), (5, 0x5), (6, 0x25), (6, 0x26),
    (6, 0x27), (5, 0x6), (7, 0x74), (7, 0x75), (6, 0x28), (6, 0x29), (6, 0x2a), (5, 0x7),
    (6, 0x2b), (7, 0x76), (6, 0x2c), (5, 0x8), (5, 0x9), (6, 0x2d), (7, 0x77), (7, 0x78),
    (7, 0x79), (7, 0x7a), (7, 0x7b), (15, 0x7ffe), (11, 0x7fc), (14, 0x3ffd), (13, 0x1ffd), (28, 0xffffffc),
    (20, 0xfffe6), (22, 0x3fffd2), (20, 0xfffe7), (20, 0xfffe8), (22, 0x3fffd3), (22, 0x3fffd4), (22, 0x3fffd5), (23, 0x7fffd9),
    (22, 0x3fffd6), (23, 0x7fffda), (23, 0x7fffdb), (23, 0x7fffdc), (23, 0x7fffdd), (23, 0x7fffde), (24, 0xffffeb), (23, 0x7fffdf),
    (24, 0xffffec), (24, 0xffffed), (22, 0x3fffd7), (23, 0x7fffe0), (24, 0xffffee), (23, 0x7fffe1), (23, 0x7fffe2), (23, 0x7fffe3),
    (23, 0x7fffe4), (21, 0x1fffdc), (22, 0x3fffd8), (23, 0x7fffe5), (22, 0x3fffd9), (23, 0x7fffe6), (23, 0x7fffe7), (24, 0xffffef),
    (22, 0x3fffda), (21, 0x1fffdd), (20, 0xfffe9), (22, 0x3fffdb), (22, 0x3fffdc), (23, 0x7fffe8), (23, 0x7fffe9), (21, 0x1fffde),
    (23, 0x7fffea), (22, 0x3fffdd), (22, 0x3fffde), (24, 0xfffff0), (21, 0x1fffdf), (22, 0x3fffdf), (23, 0x7fffeb), (23, 0x7fffec),
    (21, 0x1fffe0), (21, 0x1fffe1), (22, 0x3fffe0), (21, 0x1fffe2), (23, 0x7fffed), (22, 0x3fffe1), (23, 0x7fffee), (23, 0x7fffef),
    (20, 0xfffea), (22, 0x3fffe2), (22, 0x3fffe3), (22, 0x3fffe4), (23, 0x7ffff0), (22, 0x3fffe5), (22, 0x3fffe6), (23, 0x7ffff1),
    (26, 0x3ffffe0), (26, 0x3ffffe1), (20, 0xfffeb), (19, 0x7fff1), (22, 0x3fffe7), (23, 0x7ffff2), (22, 0x3fffe8), (25, 0x1ffffec),
    (26, 0x3ffffe2), (26, 0x3ffffe3), (26, 0x3ffffe4), (27, 0x7ffffde), (27, 0x7ffffdf), (26, 0x3ffffe5), (24, 0xfffff1), (25, 0x1ffffed),
    (19, 0x7fff2), (21, 0x1fffe3), (26, 0x3ffffe6), (27, 0x7ffffe0), (27, 0x7ffffe1), (26, 0x3ffffe7), (27, 0x7ffffe2), (24, 0xfffff2),
    (21, 0x1fffe4), (21, 0x1fffe5), (26, 0x3ffffe8), (26, 0x3ffffe9), (28, 0xffffffd), (27, 0x7ffffe3), (27, 0x7ffffe4), (27, 0x7ffffe5),
    (20, 0xfffec), (24, 0xfffff3), (20, 0xfffed), (21, 0x1fffe6), (22, 0x3fffe9), (21, 0x1fffe7), (21, 0x1fffe8), (23, 0x7ffff3),
    (22, 0x3fffea), (22, 0x3fffeb), (25, 0x1ffffee), (25, 0x1ffffef), (24, 0xfffff4), (24, 0xfffff5), (26, 0x3ffffea), (23, 0x7ffff4),
    (26, 0x3ffffeb), (27, 0x7ffffe6), (26, 0x3ffffec), (26, 0x3ffffed), (27, 0x7ffffe7), (27, 0x7ffffe8), (27, 0x7ffffe9), (27, 0x7ffffea),
    (27, 0x7ffffeb), (28, 0xffffffe), (27, 0x7ffffec), (27, 0x7ffffed), (27, 0x7ffffee), (27, 0x7ffffef), (27, 0x7fffff0), (26, 0x3ffffee),
    (30, 0x3fffffff),
)


def _build_huffman_trie() -> list[object]:
    root: list[object] = [None, None]
    for sym, (nbits, code) in enumerate(HPACK_HUFFMAN_CODES):
        node = root
        for shift in range(nbits - 1, -1, -1):
            bit = (code >> shift) & 1
            if node[bit] is None:
                node[bit] = [None, None]
            node = node[bit]  # type: ignore[assignment]
        node.append(sym)
    return root


HPACK_HUFFMAN_TRIE = _build_huffman_trie()


def decode_huffman(raw: bytes) -> bytes:
    out = bytearray()
    node = HPACK_HUFFMAN_TRIE
    bits_since_emit = 0
    cur_bits = 0
    for byte in raw:
        for shift in range(7, -1, -1):
            bit = (byte >> shift) & 1
            cur_bits = (cur_bits << 1) | bit
            bits_since_emit += 1
            nxt = node[bit]
            if nxt is None:
                raise ValueError("invalid HPACK Huffman sequence")
            node = nxt  # type: ignore[assignment]
            if len(node) == 3:
                sym = int(node[2])  # type: ignore[arg-type]
                if sym == 256:
                    raise ValueError("EOS symbol encountered in HPACK Huffman string")
                out.append(sym)
                node = HPACK_HUFFMAN_TRIE
                bits_since_emit = 0
                cur_bits = 0
    if bits_since_emit > 7 or (bits_since_emit > 0 and cur_bits != (1 << bits_since_emit) - 1):
        raise ValueError("invalid HPACK Huffman padding")
    return bytes(out)


def decode_hpack_int(buf: bytes, idx: int, prefix_bits: int) -> tuple[int, int]:
    if idx >= len(buf):
        raise ValueError("unexpected end of HPACK integer")
    mask = (1 << prefix_bits) - 1
    number = buf[idx] & mask
    idx += 1
    if number < mask:
        return number, idx
    shift = 0
    while idx < len(buf):
        b = buf[idx]
        idx += 1
        number += (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return number, idx
        shift += 7
    raise ValueError("truncated HPACK multi-byte integer")


def decode_hpack_str(buf: bytes, idx: int) -> tuple[str, int]:
    if idx >= len(buf):
        raise ValueError("unexpected end of HPACK string")
    huffman = bool(buf[idx] & 0x80)
    length, idx = decode_hpack_int(buf, idx, 7)
    end = idx + length
    if end > len(buf):
        raise ValueError("truncated HPACK string payload")
    raw = buf[idx:end]
    decoded = decode_huffman(raw) if huffman else raw
    return decoded.decode("utf-8", errors="replace"), end


def encode_hpack_int(prefix_val: int, number: int, prefix_bits: int) -> bytes:
    mask = (1 << prefix_bits) - 1
    if number < mask:
        return bytes([prefix_val | number])
    out = bytearray([prefix_val | mask])
    remaining = number - mask
    while remaining >= 128:
        out.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    out.append(remaining)
    return bytes(out)


def encode_hpack_str(text: str) -> bytes:
    raw = text.encode("utf-8")
    return encode_hpack_int(0x00, len(raw), 7) + raw


def encode_hpack_literal(name: str, header_val: str) -> bytes:
    return b"\x00" + encode_hpack_str(name) + encode_hpack_str(header_val)


class HpackDecoder:
    """RFC 7541 HPACK decoder with connection-scoped dynamic table."""

    def __init__(self, max_size: int = 4096) -> None:
        self.max_size = max_size
        self.dyn: list[tuple[str, str]] = []
        self.dyn_bytes = 0

    def _lookup(self, index: int) -> tuple[str, str]:
        if index == 0:
            raise ValueError("HPACK index 0 is invalid")
        if index <= len(HPACK_STATIC_TABLE):
            return HPACK_STATIC_TABLE[index - 1]
        dyn_idx = index - len(HPACK_STATIC_TABLE) - 1
        if 0 <= dyn_idx < len(self.dyn):
            return self.dyn[dyn_idx]
        raise ValueError(f"HPACK index out of range: {index}")

    def _insert(self, name: str, header_val: str) -> None:
        entry_size = len(name.encode("utf-8")) + len(header_val.encode("utf-8")) + 32
        while self.dyn and (self.dyn_bytes + entry_size > self.max_size):
            old_name, old_val = self.dyn.pop()
            self.dyn_bytes -= len(old_name.encode("utf-8")) + len(old_val.encode("utf-8")) + 32
        if entry_size <= self.max_size:
            self.dyn.insert(0, (name, header_val))
            self.dyn_bytes += entry_size

    def _set_max_size(self, new_max: int) -> None:
        self.max_size = new_max
        while self.dyn and self.dyn_bytes > self.max_size:
            old_name, old_val = self.dyn.pop()
            self.dyn_bytes -= len(old_name.encode("utf-8")) + len(old_val.encode("utf-8")) + 32

    def decode_block(self, block: bytes) -> list[tuple[str, str]]:
        headers: list[tuple[str, str]] = []
        idx = 0
        while idx < len(block):
            first = block[idx]
            if first & 0x80:
                index, idx = decode_hpack_int(block, idx, 7)
                headers.append(self._lookup(index))
            elif first & 0x40:
                name_idx, idx = decode_hpack_int(block, idx, 6)
                name, idx = (
                    (self._lookup(name_idx)[0], idx)
                    if name_idx > 0
                    else decode_hpack_str(block, idx)
                )
                header_val, idx = decode_hpack_str(block, idx)
                self._insert(name, header_val)
                headers.append((name, header_val))
            elif first & 0x20:
                new_max, idx = decode_hpack_int(block, idx, 5)
                self._set_max_size(new_max)
            else:
                name_idx, idx = decode_hpack_int(block, idx, 4)
                name, idx = (
                    (self._lookup(name_idx)[0], idx)
                    if name_idx > 0
                    else decode_hpack_str(block, idx)
                )
                header_val, idx = decode_hpack_str(block, idx)
                headers.append((name, header_val))
        return headers


def pack_h2_frame(frame_type: int, flags: int, stream_id: int, payload: bytes) -> bytes:
    header = struct.pack(">IBI", (len(payload) << 8) | (frame_type & 0xFF), flags, stream_id & 0x7FFFFFFF)
    return header + payload


def recv_exact(sock: socket.socket, count: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            if not buf:
                return None
            raise EOFError("unexpected EOF reading HTTP/2 frame")
        buf.extend(chunk)
    return bytes(buf)


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


def frame_grpc_length_prefixed(message_bytes: bytes) -> bytes:
    return b"\x00" + struct.pack(">I", len(message_bytes)) + message_bytes


def frame_grpc_web_response(message_bytes: bytes, *, base64_text: bool) -> bytes:
    data_frame = frame_grpc_length_prefixed(message_bytes)
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


class H2cGrpcRequestHandler(socketserver.BaseRequestHandler):
    """Handles HTTP/2 prior-knowledge (h2c) native gRPC streams over one TCP connection."""

    def handle(self) -> None:
        conn: socket.socket = self.request
        conn.settimeout(15.0)
        try:
            preface = recv_exact(conn, len(H2_PREFACE))
        except (EOFError, OSError):
            return
        if preface != H2_PREFACE:
            return

        write_lock = threading.Lock()
        decoder = HpackDecoder()
        streams: dict[int, dict[str, object]] = {}

        def send_frame(frame_type: int, flags: int, stream_id: int, payload: bytes) -> None:
            raw = pack_h2_frame(frame_type, flags, stream_id, payload)
            with write_lock:
                conn.sendall(raw)

        # Send server connection preface (empty SETTINGS).
        send_frame(H2_FRAME_SETTINGS, 0, 0, b"")

        while True:
            try:
                hdr_bytes = recv_exact(conn, 9)
            except (EOFError, OSError, TimeoutError):
                break
            if hdr_bytes is None:
                break
            word0, flags, raw_stream = struct.unpack(">IBI", hdr_bytes)
            length = word0 >> 8
            frame_type = word0 & 0xFF
            stream_id = raw_stream & 0x7FFFFFFF
            try:
                payload = recv_exact(conn, length) if length > 0 else b""
            except (EOFError, OSError, TimeoutError):
                break
            if payload is None:
                break

            if frame_type == H2_FRAME_SETTINGS:
                if not (flags & H2_FLAG_ACK):
                    send_frame(H2_FRAME_SETTINGS, H2_FLAG_ACK, 0, b"")
                continue
            if frame_type == H2_FRAME_PING:
                if not (flags & H2_FLAG_ACK):
                    send_frame(H2_FRAME_PING, H2_FLAG_ACK, 0, payload)
                continue
            if frame_type == H2_FRAME_GOAWAY:
                break
            if frame_type == H2_FRAME_RST_STREAM:
                streams.pop(stream_id, None)
                continue
            if frame_type in (H2_FRAME_PRIORITY, H2_FRAME_WINDOW_UPDATE):
                continue

            if frame_type == H2_FRAME_HEADERS:
                pos = 0
                end = len(payload)
                if flags & H2_FLAG_PADDED:
                    pad_len = payload[0]
                    pos += 1
                    end -= pad_len
                if flags & H2_FLAG_PRIORITY:
                    pos += 5
                state = streams.setdefault(
                    stream_id,
                    {
                        "header_frags": bytearray(),
                        "headers": {},
                        "body": bytearray(),
                        "end_stream": False,
                        "headers_done": False,
                    },
                )
                state["header_frags"].extend(payload[pos:end])  # type: ignore[union-attr]
                if flags & H2_FLAG_END_STREAM:
                    state["end_stream"] = True
                if flags & H2_FLAG_END_HEADERS:
                    decoded = decoder.decode_block(bytes(state["header_frags"]))  # type: ignore[arg-type]
                    hdr_map: dict[str, str] = {}
                    for k, v in decoded:
                        hdr_map[k] = v
                    state["headers"] = hdr_map
                    state["headers_done"] = True
                    if state["end_stream"]:
                        self._dispatch_h2_stream(stream_id, streams.pop(stream_id), send_frame)
                continue

            if frame_type == H2_FRAME_CONTINUATION:
                state = streams.get(stream_id)
                if state is None:
                    continue
                state["header_frags"].extend(payload)  # type: ignore[union-attr]
                if flags & H2_FLAG_END_HEADERS:
                    decoded = decoder.decode_block(bytes(state["header_frags"]))  # type: ignore[arg-type]
                    hdr_map = {}
                    for k, v in decoded:
                        hdr_map[k] = v
                    state["headers"] = hdr_map
                    state["headers_done"] = True
                    if state["end_stream"]:
                        self._dispatch_h2_stream(stream_id, streams.pop(stream_id), send_frame)
                continue

            if frame_type == H2_FRAME_DATA:
                state = streams.get(stream_id)
                if state is None:
                    continue
                pos = 0
                end = len(payload)
                if flags & H2_FLAG_PADDED:
                    pad_len = payload[0]
                    pos += 1
                    end -= pad_len
                state["body"].extend(payload[pos:end])  # type: ignore[union-attr]
                if length > 0:
                    inc = struct.pack(">I", length)
                    send_frame(H2_FRAME_WINDOW_UPDATE, 0, 0, inc)
                    send_frame(H2_FRAME_WINDOW_UPDATE, 0, stream_id, inc)
                if flags & H2_FLAG_END_STREAM:
                    state["end_stream"] = True
                    if state["headers_done"]:
                        self._dispatch_h2_stream(stream_id, streams.pop(stream_id), send_frame)

    def _dispatch_h2_stream(
        self,
        stream_id: int,
        state: dict[str, object],
        send_frame: object,
    ) -> None:
        headers: dict[str, str] = state["headers"]  # type: ignore[assignment]
        body = bytes(state["body"])  # type: ignore[arg-type]
        method = headers.get(":method", "")
        path = headers.get(":path", "").split("?", 1)[0]
        content_type = headers.get("content-type", "").lower()

        def emit_grpc_status(grpc_code: str, grpc_message: str | None = None) -> None:
            initial = b"\x88" + encode_hpack_literal("content-type", "application/grpc") + encode_hpack_literal("x-zinder-replica", REPLICA_ID)
            send_frame(H2_FRAME_HEADERS, H2_FLAG_END_HEADERS, stream_id, initial)  # type: ignore[operator]
            trailers = encode_hpack_literal("grpc-status", grpc_code)
            if grpc_message:
                trailers += encode_hpack_literal("grpc-message", grpc_message)
            send_frame(
                H2_FRAME_HEADERS,
                H2_FLAG_END_STREAM | H2_FLAG_END_HEADERS,
                stream_id,
                trailers,
            )  # type: ignore[operator]

        if method != "POST" or path != GET_LATEST_BLOCK_PATH:
            emit_grpc_status("12", "unknown gRPC method")
            return
        if not (content_type == "application/grpc" or content_type.startswith("application/grpc+") or content_type.startswith("application/grpc;")):
            resp_hdrs = (
                encode_hpack_literal(":status", "415")
                + encode_hpack_literal("content-type", "text/plain")
                + encode_hpack_literal("x-zinder-replica", REPLICA_ID)
            )
            send_frame(
                H2_FRAME_HEADERS,
                H2_FLAG_END_STREAM | H2_FLAG_END_HEADERS,
                stream_id,
                resp_hdrs,
            )  # type: ignore[operator]
            return
        if len(body) < 5 or body[0] != 0:
            emit_grpc_status("3", "invalid gRPC request frame")
            return
        msg_len = struct.unpack(">I", body[1:5])[0]
        if len(body) != 5 + msg_len:
            emit_grpc_status("3", "truncated gRPC request frame")
            return

        response_proto = encode_block_id_proto(SAMPLE_BLOCK_HEIGHT, SAMPLE_BLOCK_HASH_HEX)
        response_data = frame_grpc_length_prefixed(response_proto)
        initial = (
            b"\x88"
            + encode_hpack_literal("content-type", "application/grpc")
            + encode_hpack_literal("x-zinder-replica", REPLICA_ID)
        )
        send_frame(H2_FRAME_HEADERS, H2_FLAG_END_HEADERS, stream_id, initial)  # type: ignore[operator]
        send_frame(H2_FRAME_DATA, 0, stream_id, response_data)  # type: ignore[operator]
        send_frame(
            H2_FRAME_HEADERS,
            H2_FLAG_END_STREAM | H2_FLAG_END_HEADERS,
            stream_id,
            encode_hpack_literal("grpc-status", "0"),
        )  # type: ignore[operator]


class ThreadingTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


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
        if self.command != "HEAD":
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
        if parsed.path != GET_LATEST_BLOCK_PATH:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown gRPC-Web method"})
            return
        body = self._read_body()
        content_type = self.headers.get("Content-Type", "").lower()
        if not ("grpc-web" in content_type):
            self._send_json(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {"error": "native application/grpc requires HTTP/2 h2c upstream port"},
            )
            return
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
    grpc_server = ThreadingTcpServer(("0.0.0.0", PROBE_GRPC_PORT), H2cGrpcRequestHandler)
    grpc_thread = threading.Thread(target=grpc_server.serve_forever, daemon=True)
    grpc_thread.start()
    http_server = ThreadingHTTPServer(("0.0.0.0", PROBE_PORT), ProbeReplicaHandler)
    http_server.serve_forever()


if __name__ == "__main__":
    main()
