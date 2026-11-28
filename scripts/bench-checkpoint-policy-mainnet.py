#!/usr/bin/env python3
"""Run the checkpoint-policy comparison on the archived #479 Mainnet window."""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading

ARCHIVE_SHA256 = "da252aaa5ca976f98060a512db0345a3564a53a9328d3acb97fd2717f5b6d655"
FIRST, LAST = 1_730_000, 1_731_999
REPO = Path(__file__).resolve().parent.parent


def read_reply(path):
    reply = json.loads(path.read_text())
    return reply.get("result", reply)


def block_hash(path):
    with path.open("rb") as stream:
        header = stream.read(140)
        marker = stream.read(1)
        width = {253: 2, 254: 4, 255: 8}.get(marker[0], 0)
        length_bytes = stream.read(width)
        length = int.from_bytes(length_bytes, "little") if width else marker[0]
        solution = stream.read(length)
    first_hash = hashlib.sha256(header + marker + length_bytes + solution).digest()
    return hashlib.sha256(first_hash).digest()[::-1].hex()


class FrozenWindow:
    def __init__(self, root):
        self.blocks = {height: root / f"{height}.bin" for height in range(FIRST, LAST + 1)}
        self.hashes = {height: block_hash(path) for height, path in self.blocks.items()}
        checks = [read_reply(root / f"{height}-z_gettreestate.json") for height in (FIRST - 1, LAST)]
        self.checkpoints = {key: checkpoint for checkpoint in checks
                            for key in (str(checkpoint["height"]), checkpoint["hash"])}
        self.chain_info = read_reply(root / "getblockchaininfo.json")
        pages = [read_reply(root / f"{pool}-subtrees.json") for pool in ("sapling", "orchard")]
        self.subtrees = {(page["pool"], page["start_index"], len(page["subtrees"])): page
                         for page in pages}
        if self.hashes[LAST] != checks[-1]["hash"]:
            raise ValueError("archived block and checkpoint disagree at the window tip")

    def reply(self, method, params):
        match method:
            case "getblock" if params[1] == 0:
                return self.blocks[int(params[0])].read_bytes().hex()
            case "getblockhash":
                return self.hashes[int(params[0])]
            case "getbestblockheightandhash":
                return {"height": LAST, "hash": list(bytes.fromhex(self.hashes[LAST])[::-1])}
            case "getblockchaininfo":
                return self.chain_info
            case "z_gettreestate":
                return self.checkpoints[params[0]]
            case "z_getsubtreesbyindex":
                return self.subtrees[tuple(params)]
            case _:
                raise KeyError(method)

    def rpc(self, call):
        envelope = {"jsonrpc": "2.0", "id": call.get("id")}
        try:
            envelope["result"] = self.reply(call["method"], call.get("params", []))
        except (IndexError, KeyError, ValueError):
            envelope["error"] = {"code": -32602, "message": "Outside frozen Mainnet window"}
        return envelope


def handler_for(window):
    class FrozenRpcHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            call = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            reply = [window.rpc(entry) for entry in call] if isinstance(call, list) else window.rpc(call)
            encoded = json.dumps(reply, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return FrozenRpcHandler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True, help="z470-window-1730000.tar.zst")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--binary", type=Path, help="use a prebuilt release zinder-bench binary")
    args = parser.parse_args()
    if not args.archive.is_file():
        parser.error("Mainnet re-verification requires the archived #479 window")
    scratch = REPO / ".tmp"
    scratch.mkdir(exist_ok=True)
    report = args.report.resolve()
    if report.exists() or not report.parent.is_dir():
        parser.error("--report must be a fresh path with an existing parent")

    if args.binary:
        command = [args.binary.resolve()]
    else:
        command = ["cargo", "run", "--quiet", "--release", "-p", "zinder-bench", "--"]
    env = os.environ.copy()
    env.setdefault("CARGO_PROFILE_RELEASE_LTO", "false")
    env.setdefault("CARGO_TARGET_DIR", str(scratch / "target"))
    with tempfile.TemporaryDirectory(prefix="checkpoint-policy-", dir=scratch) as directory:
        with args.archive.open("rb") as archive:
            if hashlib.file_digest(archive, "sha256").hexdigest() != ARCHIVE_SHA256:
                parser.error(f"Mainnet archive does not match SHA-256 {ARCHIVE_SHA256}")
            archive.seek(0)
            subprocess.run(["tar", "--zstd", "-xf", "-", "-C", directory], stdin=archive, check=True)
        window = FrozenWindow(Path(directory) / "window-1730000")
        with ThreadingHTTPServer(("127.0.0.1", 0), handler_for(window)) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                raw_report = Path(directory) / "comparison.json"
                subprocess.run([
                    *command, "rocksdb-checkpoint-policy-comparison",
                    "--frozen-mainnet-rpc", f"127.0.0.1:{server.server_port}",
                    "--scratch-directory", directory,
                    "--report", raw_report,
                ], cwd=REPO, env=env, check=True)
                comparison = json.loads(raw_report.read_text())
                comparison["archive_sha256"] = ARCHIVE_SHA256
                with report.open("x") as stream:
                    json.dump(comparison, stream, indent=2)
                    stream.write("\n")
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    main()
