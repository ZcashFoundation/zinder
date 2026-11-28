#!/usr/bin/env python3
"""Compare uninterrupted Testnet recovery with reopening after a partial compact-block stream."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import runpy
import sqlite3
import sys
import tempfile
from threading import Lock

import grpc
import compact_formats_pb2
import service_pb2 as pb

from subtree_proxy import READ_METHODS, SERVICE, RawReader, serve
from coverage_evidence import zkool_version
from wallet_fixture import loopback


HERE = Path(__file__).resolve().parent
BENCH = runpy.run_path(str(HERE.parent / "run-benchmark-campaign.py"))
RESTORE = runpy.run_path(str(HERE.parent / "run-zkool-restore.py"))
MARKER = "controlled interruption after a partial compact-block range"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def fixture_live(reader, fixture, revision):
    tip = pb.BlockID.FromString(next(reader.read("GetLatestBlock", b"")))
    info = pb.LightdInfo.FromString(next(reader.read("GetLightdInfo", b"")))
    require(info.gitCommit == revision and info.chainName == fixture["network"], "server identity changed")
    require(info.blockHeight == tip.height == fixture["tip"] and tip.hash[::-1].hex() == fixture["tip_hash"],
            "server does not serve the frozen fence")


class RangeObserver:
    def __init__(self, reader, interrupt_after=None):
        self.reader = reader
        self.interrupt_after = interrupt_after
        self.fault = None
        self.methods = Counter()
        self.range_requests = []
        self.lock = Lock()

    def handle(self, method, request, context):
        with self.lock:
            self.methods[method] += 1
        rows = self.reader.read(method, request)
        try:
            if method != "GetBlockRange":
                yield from rows
                return
            block_range = pb.BlockRange.FromString(request)
            with self.lock:
                self.range_requests.append([block_range.start.height, block_range.end.height])
            for offset, row in enumerate(rows):
                if self.interrupt_after is not None and offset <= self.interrupt_after:
                    height = compact_formats_pb2.CompactBlock.FromString(row).height
                    require(height == block_range.start.height + offset, "nonconsecutive fixture range")
                    with self.lock:
                        interrupt = offset == self.interrupt_after and self.fault is None
                        if interrupt:
                            self.fault = {"start": block_range.start.height, "end": block_range.end.height,
                                          "delivered": offset, "last_delivered": height - 1}
                    if interrupt:
                        context.abort(grpc.StatusCode.UNAVAILABLE, MARKER)
                yield row
        finally:
            if hasattr(rows, "cancel"):
                rows.cancel()


def persisted_state(directory, report):
    with sqlite3.connect((directory / "zkool.db").as_uri() + "?mode=ro", uri=True) as db:
        txs = db.execute("SELECT hex(txid), height, value FROM transactions ORDER BY txid, height").fetchall()
        notes = db.execute("SELECT pool, height, hex(cmx), value FROM notes ORDER BY pool, height, cmx").fetchall()
    require(report["history_entries"] == len(txs) > 0 and report["received_notes"] == len(notes) > 0,
            "expected populated wallet history and notes")
    fingerprint = hashlib.sha256(json.dumps([txs, notes], separators=(",", ":")).encode()).hexdigest()
    return {**report, "ledger_sha256": fingerprint}


def run_arm(args, fixture, reader, out, fault):
    observer = RangeObserver(reader, 64 if fault else None)
    server, port = serve(observer.handle)
    endpoint = f"http://127.0.0.1:{port}"
    out.mkdir(mode=0o700)
    traces = {}
    def invoke(step, argv, online):
        cmd = [str(args.client), "testnet-read", *argv]
        if online:
            cmd = [sys.executable, str(HERE.parent / "trace-wallet-endpoint.py"),
                   "--endpoint-port", str(port), "--output", str(out / step), "--", *cmd]
        with (out / f"{step}.stdout").open("wb") as stdout, (out / f"{step}.stderr").open("wb") as stderr:
            exit_code = BENCH["run_command"](cmd, stdout, stderr, 360)
        if online:
            trace = json.loads((out / step / "report.json").read_text())
            require(trace["status"] == "passed" and trace["fallback_used"] is False, "wallet endpoint trace failed")
            traces[step] = {"exit": trace["command_exit"], "connects": len(trace["connections"]),
                            "trace_sha256": BENCH["sha256"](out / step / "connect.log")}
            return trace["command_exit"]
        return exit_code

    try:
        with tempfile.TemporaryDirectory(dir=out) as scratch:
            db = Path(scratch)
            rc = invoke("restore", ["sync", endpoint, str(fixture["birthday"]), RESTORE["SYNTHETIC_SEED"], scratch], True)
            if fault:
                require(observer.fault is not None, "wallet never reached the injected interruption")
                require(rc != 0 and MARKER in (out / "restore.stderr").read_text(), "wallet did not surface the fault")
                require(invoke("interrupted-state", ["report", scratch], False) == 0, "interrupted DB cannot reopen")
                saved_height = json.loads((out / "interrupted-state.stdout").read_text())["synced_height"]
                require(saved_height == observer.fault["last_delivered"] < fixture["tip"],
                        "interrupted wallet failed to persist the delivered prefix")
                observer.fault["wallet_height_before_resume"] = saved_height
                before_resume = len(observer.range_requests)
                require(invoke("resume", ["resume", endpoint, scratch], True) == 0, "fresh process failed to resume")
                require(any(saved_height < start <= end == fixture["tip"]
                            for start, end in observer.range_requests[before_resume:]),
                        "resumed process did not request the remaining compact-block range")
            else:
                require(rc == 0, "control restore failed")
            require(invoke("reopen", ["report", scratch], False) == 0, "failed to reopen persisted wallet")
            state = persisted_state(db, json.loads((out / "reopen.stdout").read_text()))
            require(state["synced_height"] == fixture["tip"], "wallet stopped short of the fence")
    finally:
        server.stop(0).wait()
    return {"wallet": state, "fault": observer.fault, "methods": dict(observer.methods), "online_traces": traces,
            "compact_block_ranges": observer.range_requests}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--client-sha256", required=True)
    parser.add_argument("--client-version", help="source version for a newly built Zkool engine")
    parser.add_argument("--revision", required=True, help="full selected Zinder release commit")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    version = zkool_version(args.client_sha256, args.client_version)
    args.client = args.client.resolve()
    endpoint = loopback(args.endpoint)
    require(len(args.revision) == 40 and all(c in "0123456789abcdef" for c in args.revision),
            "expected a full release commit")
    require(BENCH["sha256"](args.client) == args.client_sha256, "client executable pin mismatch")
    fixture_path = HERE / "subtree-testnet.json"
    fixture = json.loads(fixture_path.read_text())
    os.umask(0o077)
    args.output = args.output.resolve()
    args.output.mkdir(mode=0o700)
    os.environ["ZINDER_ZKOOL_USE_INTERNAL"] = "false"
    with grpc.insecure_channel(endpoint.removeprefix("http://")) as channel:
        reader = RawReader(channel)
        fixture_live(reader, fixture, args.revision)
        control = run_arm(args, fixture, reader, args.output / "control", False)
        interrupted = run_arm(args, fixture, reader, args.output / "interrupted", True)
        fixture_live(reader, fixture, args.revision)
    require(control["wallet"] == interrupted["wallet"], "resumed wallet history, notes or balance differs from control")
    require(BENCH["sha256"](args.client) == args.client_sha256, "client executable changed")
    result = {"schema_version": 1, "status": "pass", "revision": args.revision, "client_sha256": args.client_sha256,
              "client_version": version,
              "fixture_sha256": BENCH["sha256"](fixture_path), "birthday": fixture["birthday"], "tip": fixture["tip"],
              "tip_hash": fixture["tip_hash"], "control": control, "interrupted": interrupted}
    result["observed_methods"] = sorted(READ_METHODS)
    result["descriptor_sha256"] = hashlib.sha256(SERVICE.file.serialized_pb).hexdigest()
    result["runner_sha256"] = BENCH["sha256"](Path(__file__))
    (args.output / "receipt.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
