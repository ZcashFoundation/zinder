#!/usr/bin/env python3
"""Compare a funded Zingo restore with two Sapling completion-hash wire encodings."""
import argparse
from collections import Counter
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import signal
import subprocess
import sys
from threading import Lock
from urllib.parse import urlsplit

import grpc
import service_pb2 as pb
import service_pb2_grpc
from subtree_proxy import READ_METHODS, RawReader, serve


HERE = Path(__file__).resolve().parent
TRACE = HERE.parent / "trace-wallet-endpoint.py"
FIXTURE = HERE / "subtree-testnet.json"
FIXTURE_FIELDS = ("network", "tip", "tip_hash", "birthday", "sapling_roots", "cli_revision",
                  "cli_sha256", "expected_outputs", "expected_confirmed_zat", "history_sha256",
                  "ztreamer_witness", "expected_spendable_zat")


def check(condition, message):
    if not condition:
        raise ValueError(message)


def sanitized_fixture(fixture):
    """Return only the public fixture fields, so private additions never leave the run."""
    check(isinstance(fixture, dict) and set(fixture) == set(FIXTURE_FIELDS), "unexpected fixture fields")
    return {key: fixture[key] for key in FIXTURE_FIELDS}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def check_server(channel, fixture, revision):
    stub = service_pb2_grpc.CompactTxStreamerStub(channel)
    tip = stub.GetLatestBlock(pb.ChainSpec(), timeout=10)
    info = stub.GetLightdInfo(pb.Empty(), timeout=10)
    check(info.gitCommit == revision and info.chainName == fixture["network"], "wallet service identity mismatch")
    check(tip.hash[::-1].hex() == fixture["tip_hash"] and tip.height == info.blockHeight == fixture["tip"], "wrong fence")
    rows = list(stub.GetSubtreeRoots(pb.GetSubtreeRootsArg(), timeout=15))
    check(len(rows) == len(fixture["sapling_roots"]) > 0, "missing expected Sapling rows")
    for row, expected in zip(rows, fixture["sapling_roots"]):
        check([row.rootHash.hex(), row.completingBlockHeight, row.completingBlockHash.hex()] == expected, "reference row mismatch")
        block = stub.GetBlock(pb.BlockID(height=row.completingBlockHeight), timeout=15)
        check(block.height == row.completingBlockHeight and block.hash[::-1] == row.completingBlockHash, "completion block mismatch")
    witness = fixture["ztreamer_witness"]["row"]
    check(bytes.fromhex(witness["zebra_display_hex_as_bytes"])[::-1].hex() == witness["ztreamer_bytes"], "invalid ztreamer witness")
    return {"commit": info.gitCommit, "version": info.version, "network": info.chainName, "height": tip.height, "hash": tip.hash[::-1].hex()}


def subtree_wire(raw, expected, reverse):
    row = pb.SubtreeRoot.FromString(raw)
    source = row.completingBlockHash
    check([row.rootHash.hex(), row.completingBlockHeight, source.hex()] == expected, "unrecognized subtree row")
    check(len(source) == 32 and source[::-1] != source and raw.count(source) == 1, "ambiguous completion field")
    offset = raw.index(source)
    output = raw[:offset] + source[::-1] + raw[offset + 32:] if reverse else raw
    decoded = pb.SubtreeRoot.FromString(output)
    check(decoded.completingBlockHash == (source[::-1] if reverse else source), "wrong emitted hash")
    decoded.completingBlockHash = source
    check(decoded == row, "changed another protobuf field")
    return output, {"height": row.completingBlockHeight, "offset": offset, "input_sha256": sha(raw), "output_sha256": sha(output),
                    "unchanged_prefix_sha256": sha(raw[:offset]), "unchanged_suffix_sha256": sha(raw[offset+32:])}


def frame_digest(digest, message):
    digest.update(len(message).to_bytes(8, "big"))
    digest.update(message)


class Experiment:
    """Memoize finite upstream replies so repeated reads use the same protobuf bytes."""

    def __init__(self, channel, fixture):
        self.reader = RawReader(channel)
        self.fixture = fixture
        self.cache = {}
        self.lock = Lock()
        self.mode = "reference"
        self.phase = ""
        self.events = []

    def handle(self, name, request, context):
        event = {"phase": self.phase, "method": name, "request_sha256": sha(request), "rows": 0}
        self.events.append(event)
        if name == "GetMempoolStream":
            upstream = self.reader.read(name, request)
            context.add_callback(upstream.cancel)
            context.send_initial_metadata(())
            for _ in upstream:
                event["rows"] += 1
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "frozen fixture has a mempool entry")
            return
        key = (name, request)
        with self.lock:
            rows = self.cache.get(key)
        if rows is None:
            fetched = tuple(self.reader.read(name, request))
            with self.lock:
                rows = self.cache.setdefault(key, fetched)
            check(rows == fetched, "mutable fixture response")
        if name == "GetSubtreeRoots":
            req = pb.GetSubtreeRootsArg.FromString(request)
            event.update(pool=req.shieldedProtocol, start=req.startIndex, limit=req.maxEntries)
        sapling = event.get("pool") == pb.sapling
        if sapling:
            event["wire"] = []
            if self.mode == "one-row":
                rows = rows[:1]
        source = hashlib.sha256()
        emitted = hashlib.sha256()
        for i, raw in enumerate(rows):
            output = raw
            if sapling:
                expected = self.fixture["sapling_roots"][event["start"] + i]
                output, proof = subtree_wire(raw, expected, self.mode == "reversed")
                if self.mode == "invalid-root" and event["start"] + i == 0:
                    row = pb.SubtreeRoot.FromString(raw)
                    check(raw.count(row.rootHash) == 1, "ambiguous root field")
                    output = raw.replace(row.rootHash, bytes([255]) * 32, 1)
                    proof["output_sha256"] = sha(output)
                event["wire"].append(proof)
            frame_digest(source, raw)
            frame_digest(emitted, output)
            event["rows"] += 1
            yield output
        event.update(source_sha256=source.hexdigest(), emitted_sha256=emitted.hexdigest())


def command(argv, env, root, label, timeout=400, expected_sync_error=None):
    with (root / (label + ".stdout")).open("wb") as out, (root / (label + ".stderr")).open("wb") as err:
        proc = subprocess.Popen(argv, env=env, stdout=out, stderr=err, start_new_session=True)
        try:
            status = proc.wait(timeout=timeout)
        except BaseException:
            with suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)
            raise
    check(status == 0, f"{label} exited {status}. Inspect the private stderr file.")
    stderr = (root / (label + ".stderr")).read_bytes()
    if expected_sync_error is None:
        check(b"error: sync error" not in stderr.lower(), f"{label} reported a sync error")
    else:
        check(expected_sync_error in stderr.lower(), f"{label} omitted expected wallet rejection")
    return (root / (label + ".stdout")).read_bytes()


def offline_state(cli, wallet, stage, root, env, fixture):
    offline = [cli, "--chain", "testnet", "--offline", "--data-dir", str(wallet)]
    balance = command(offline + ["balance"], env, root, stage + "-balance").decode()
    history = command(offline + ["transactions"], env, root, stage + "-history")
    spendable = json.loads(command(offline + ["spendable_balance"], env, root, stage + "-spendable"))
    confirmed = {pool: int(n.replace("_", "")) for pool, n in re.findall(r"^\s*confirmed_([a-z]+)_balance: ([0-9_]+)$", balance, re.M)}
    check(confirmed == fixture["expected_confirmed_zat"], "unexpected pool balance")
    check(sha(history) == fixture["history_sha256"], "funded history mismatch")
    check(spendable["spendable_balance"] == fixture["expected_spendable_zat"], "spendable balance mismatch")
    return {"confirmed_zat": confirmed, "spendable_zat": spendable["spendable_balance"], "history_sha256": sha(history)}


def restore(experiment, mode, port, cli, output, env, fixture):
    root = output / mode
    root.mkdir(mode=0o700)
    experiment.mode = mode
    wallet = root / "wallet"
    check(not wallet.exists(), "wallet destination exists")
    base = [cli, "--chain", "testnet", "--server", f"http://127.0.0.1:{port}", "--data-dir", str(wallet)]
    public_vector = runpy.run_path(str(HERE.parent / "run-zkool-restore.py"))["SYNTHETIC_SEED"]
    states = {}
    traces = {}
    for stage in (("restore",) if mode == "invalid-root" else ("restore", "restart")):
        experiment.phase = f"{mode}/{stage}"
        trace = root / (stage + "-trace")
        args = [sys.executable, str(TRACE), "--endpoint-port", str(port), "--output", str(trace), "--", *base]
        seed_env = env
        if stage == "restore":
            args.extend(["--birthday", str(fixture["birthday"])])
            seed_env = dict(env, ZINGO_SEED=public_vector)
        expected_error = b"caused by: server returned invalid subtree root." if mode == "invalid-root" else None
        status = json.loads(command(args + ["--waitsync", "sync", "status"], seed_env, root, stage, expected_sync_error=expected_error))
        trace_report = json.loads((trace / "report.json").read_text())
        check(trace_report["status"] == "passed" and trace_report["fallback_used"] is False, "unattributed wallet connection")
        traces[stage] = sha((trace / "connect.log").read_bytes())
        if expected_error:
            check(status["total_blocks_scanned"] == 0 and status["total_outputs_scanned"] == 0, "wallet accepted the invalid root")
            pages = [e for e in experiment.events if e["phase"] == experiment.phase and e.get("pool") == pb.sapling]
            check(any(p["start"] == 0 and p["rows"] > 0 for p in pages), "negative control did not serve faulty row")
            return {"observed_error": "server returned invalid subtree root", "blocks_scanned": 0,
                    "cli_exit": trace_report["command_exit"], "target_only_process_connects": True, "trace_sha256": traces[stage]}
        check((wallet / "zingo-wallet.dat").is_file(), "persistent Zingo wallet file missing")
        check(status["scan_ranges"] == [{"priority":"Scanned","start_block":str(fixture["birthday"]),"end_block":str(fixture["tip"])}], "incomplete window")
        check(status["total_outputs_scanned"] == fixture["expected_outputs"] and status["percentage_total_outputs_scanned"] == 100, "incomplete output scan")
        states[stage] = offline_state(cli, wallet, stage, root, env, fixture)
        states[stage].update(blocks=status["total_blocks_scanned"], outputs=status["total_outputs_scanned"])
    check(states["restart"] == states["restore"], "wallet state changed on reopen")
    events = [e for e in experiment.events if e["phase"] == mode + "/restore"]
    pages = [e for e in events if e.get("pool") == pb.sapling]
    count = len(fixture["sapling_roots"])
    expected = [(i, 1) for i in range(count)] if mode == "one-row" else [(0, count)]
    check([(e["start"], e["rows"]) for e in pages] == expected + [(count, 0)], "wallet did not consume all populated Sapling pages and terminal empty page")
    return {"states": states, "restore_method_calls": dict(Counter(e["method"] for e in events)), "sapling_pages": pages,
            "restart_method_calls": dict(Counter(e["method"] for e in experiment.events if e["phase"] == mode + "/restart")),
            "target_only_process_connects": True, "connect_trace_sha256": traces}


def compare_rpc_bytes(events):
    compared = {}
    for stage in ("restore", "restart"):
        signatures = {}
        for mode in ("reference", "reversed"):
            calls = Counter()
            for event in events:
                if event["phase"] != f"{mode}/{stage}":
                    continue
                name = event["method"]
                if name == "GetMempoolStream":
                    check(event["rows"] == 0, "mempool stream was not empty")
                    digest = sha(b"")
                else:
                    digest = event["source_sha256"]
                    changed = mode == "reversed" and event.get("pool") == pb.sapling and event["rows"] > 0
                    check((digest != event["emitted_sha256"]) == changed, "unexpected wire intervention")
                calls[(name, event["request_sha256"], event["rows"], digest)] += 1
            signatures[mode] = calls
        reference, reversed_arm = signatures["reference"], signatures["reversed"]
        check(set(reference) == set(reversed_arm), f"{stage} A/B request or reference response bytes differ")
        for call in reference:
            if call[0] != "GetLatestBlock":
                check(reference[call] == reversed_arm[call], f"{stage} A/B invocation counts differ for {call[0]}")
        compared[stage] = {
            "identical_request_and_response_signatures": len(reference),
            "same_invocation_counts": reference == reversed_arm,
            "total_calls": {mode: sum(calls.values()) for mode, calls in signatures.items()},
            "tip_polls": {mode: sum(n for key, n in calls.items() if key[0] == "GetLatestBlock")
                          for mode, calls in signatures.items()},
        }
    return compared


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="frozen http://127.0.0.1:PORT wallet endpoint")
    parser.add_argument("--release-commit", required=True, help="full expected GetLightdInfo.gitCommit")
    parser.add_argument("--client", required=True, type=Path, help="fixture-pinned Testnet Zingo CLI")
    parser.add_argument("--fixture", type=Path, default=FIXTURE, help="pinned public-vector fixture, including wallet build digest")
    parser.add_argument("--output", required=True, type=Path, help="new private evidence directory")
    parser.add_argument("--pagination", action="store_true", help="also check a one-row Sapling response per request")
    parser.add_argument("--invalid-root-control", action="store_true", help="require the wallet to reject an invalid Sapling root encoding")
    args = parser.parse_args()
    url = urlsplit(args.endpoint)
    check(url.scheme == "http" and url.hostname == "127.0.0.1" and url.port and url.path in ("", "/") and not url.username and not url.password and not url.query and not url.fragment, "require loopback http endpoint")
    check(re.fullmatch(r"[a-f0-9]{40}", args.release_commit), "expected full source revision")
    fixture_bytes = args.fixture.read_bytes()
    fixture = json.loads(fixture_bytes)
    cli = str(args.client.resolve())
    inputs = [Path(cli), HERE / "subtree-ab.py", HERE / "subtree_proxy.py",
              TRACE, HERE.parent / "run-zkool-restore.py", Path(pb.__file__), Path(service_pb2_grpc.__file__)]
    pins = {p: sha(p.read_bytes()) for p in inputs}
    pins[args.fixture] = sha(fixture_bytes)
    check(pins[Path(cli)] == fixture["cli_sha256"], "wallet binary mismatch")
    os.umask(0o077)
    output = args.output.resolve()
    output.mkdir(mode=0o700)
    preserved = output / "fixture.json"
    preserved.write_text(json.dumps(sanitized_fixture(fixture), indent=2, sort_keys=True) + "\n")
    preserved_sha256 = sha(preserved.read_bytes())
    env = dict(os.environ)
    env.pop("ZINGO_SEED", None)
    with grpc.insecure_channel(url.netloc) as channel:
        identity = check_server(channel, fixture, args.release_commit)
        experiment = Experiment(channel, fixture)
        server, port = serve(experiment.handle)
        arms = {}
        negative = None
        try:
            for mode in (["reference", "reversed", "one-row"] if args.pagination else ["reference", "reversed"]):
                arms[mode] = restore(experiment, mode, port, cli, output, env, fixture)
                save(output / mode / "summary.json", arms[mode])
                check(arms[mode]["states"] == arms["reference"]["states"], "wallet outcome differs across arms")
            matched = compare_rpc_bytes(experiment.events)
            databases = [(output / mode / "wallet" / "zingo-wallet.dat").stat() for mode in arms]
            check(len({(item.st_dev, item.st_ino) for item in databases}) == len(arms), "wallet files share storage")
            if args.invalid_root_control:
                negative = restore(experiment, "invalid-root", port, cli, output, env, fixture)
            check(check_server(channel, fixture, args.release_commit) == identity, "upstream identity changed")
        finally:
            server.stop(0).wait()
            save(output / "wallet-rpcs.json", experiment.events)
    check(all(sha(p.read_bytes()) == pin for p, pin in pins.items()), "source or fixture changed during experiment")
    check(sha(preserved.read_bytes()) == preserved_sha256, "preserved fixture changed during experiment")
    save(output / "summary.json", {"schema": 1, "server": identity, "fixture_sha256": preserved_sha256,
         "selected_fixture_sha256": pins[args.fixture],
         "wallet": {"revision": fixture["cli_revision"], "sha256": fixture["cli_sha256"]}, "arms": arms,
         "primary_wire_comparison": matched, "negative_control": negative, "observed_methods": sorted(READ_METHODS),
         "distinct_wallet_files": len(databases),
         "input_sha256": {p.name: pin for p, pin in pins.items()}})


if __name__ == "__main__":
    main()
