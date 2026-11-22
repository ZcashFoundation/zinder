#!/usr/bin/env python3
"""Compare two installed wallet readers on one private, deterministic RocksDB fixture."""
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen


SCRIPTS = Path(__file__).resolve().parent
POLICY = SCRIPTS / "benchmarks/wallet-reader/policy.json"


def write(path, data):
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def checksum(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def executable(value):
    path = shutil.which(value)
    if path is None:
        raise ValueError(f"executable not found: {value}")
    return Path(path).resolve()


def positive(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return value


def start(stack, command, log, environment):
    stream = stack.enter_context(log.open("wb"))
    process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT, env=environment)

    def stop():
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    stack.callback(stop)
    return process


def wait_ready(probe, process, description):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{description} exited {process.returncode}; see its log")
        if probe():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {description}")


def ready_url(url):
    try:
        with urlopen(url, timeout=1) as response:
            return response.status == 200
    except (URLError, TimeoutError):
        return False


def reader_ports():
    with socket.socket() as grpc, socket.socket() as ops:
        grpc.bind(("127.0.0.1", 0))
        ops.bind(("127.0.0.1", 0))
        return grpc.getsockname()[1], ops.getsockname()[1]


def start_reader(stack, name, binary, scratch, evidence, ready, environment, cpu_list):
    grpc, ops = reader_ports()
    config = scratch / f"{name}.toml"
    config.write_text(f"""[network]
name = "zcash-regtest"
[storage]
path = {json.dumps(ready['canonical_path'])}
secondary_path = {json.dumps(str(scratch / (name + '-secondary')))}
raw_blob_policy = {json.dumps(ready['raw_blob_retention'])}
[ingest_control]
addr = {json.dumps(ready['control_endpoint'])}
[compat]
listen_addr = "127.0.0.1:{grpc}"
reorg_window_blocks = {int(ready['reorg_window'])}
stale_tip_limit_ms = 0
serving = "compact-blocks"
[ops]
listen_addr = "127.0.0.1:{ops}"
[security]
allow_public_bind = false
""", encoding="utf-8")
    process = start(stack, ["taskset", "-c", cpu_list, binary, "--config", config],
                    evidence / f"{name}.log", environment)
    wait_ready(lambda: ready_url(f"http://127.0.0.1:{ops}/readyz"), process, name)
    return {"endpoint": f"http://127.0.0.1:{grpc}", "pid": str(process.pid),
            "executable": str(binary)}


def range_oracle(directory, first, last):
    jobs = []
    for start_height, end in ((first, last), (last, first)):
        digest = hashlib.sha256()
        size = 0
        step = 1 if start_height <= end else -1
        for height in range(start_height, end + step, step):
            row = (directory / "rows" / f"{height}.bin").read_bytes()
            digest.update(len(row).to_bytes(4, "big"))
            digest.update(row)
            size += len(row)
        jobs.append({"start": start_height, "end": end, "blocks": last - first + 1,
                     "bytes": size, "framed_sha256": digest.hexdigest()})
    return {"schema": "zinder-bench/raw-range-oracle/v1", "jobs": jobs}


def run(args):
    binaries = {name: executable(getattr(args, name)) for name in ("baseline", "candidate", "bench", "writer")}
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True)
    inputs = [str(Path(__file__).resolve()), str(SCRIPTS / "compare-benchmark-campaign.py")]
    identity = {}
    for name, binary in binaries.items():
        identity[name] = {"path": str(binary), "sha256": checksum(binary)}
        info = binary.with_name("BUILD-INFO.json")
        if name in ("baseline", "candidate") and info.is_file():
            build = json.loads(info.read_text())
            entry = next((r for r in build["binaries"] if r["name"] == binary.name), None)
            if entry is None or entry["sha256"] != identity[name]["sha256"]:
                raise ValueError(f"{name} binary differs from its BUILD-INFO.json")
            identity[name]["build"] = build
    write(output / "binaries.json", identity)
    policy = output / "policy.json"
    shutil.copyfile(args.policy, policy)
    inputs.extend([str(output / "binaries.json"), str(policy)])
    allowed = sorted(os.sched_getaffinity(0))
    workers = min(args.workers, max(1, len(allowed) // 2))
    reader_cpus = allowed[-workers:]
    client_cpus = allowed[:workers]
    reader_list = ",".join(map(str, reader_cpus))
    client_list = ",".join(map(str, client_cpus))
    environment = {**os.environ, "TOKIO_WORKER_THREADS": str(workers), "RAYON_NUM_THREADS": str(workers)}
    with tempfile.TemporaryDirectory(prefix="wallet-reader-") as tmp, ExitStack() as children:
        scratch = Path(tmp)
        environment["TMPDIR"] = tmp
        fixture = scratch / "source"
        writer = start(children, [binaries["writer"], "--directory", fixture, "--blocks", str(args.blocks),
                                  "--spends-per-block", str(args.spends_per_block)], output / "writer.log", environment)
        ready_file = fixture / "ready.json"
        wait_ready(ready_file.is_file, writer, "fixture writer")
        ready = json.loads(ready_file.read_text())
        if ready["initial_height"] != args.blocks or ready["spends_per_height"] != args.spends_per_block:
            raise ValueError("writer constructed a different fixture")
        arms = {name: start_reader(children, name, binaries[name], scratch, output, ready, environment, reader_list)
                for name in ("baseline", "candidate")}
        cases = {}
        for name, end in (("frontier", args.blocks), ("historical_control", args.width)):
            oracle = output / f"{name}-oracle.json"
            jobs = range_oracle(fixture, end - args.width + 1, end)
            write(oracle, jobs)
            inputs.append(str(oracle))
            cases[name] = {
                "command": [str(binaries["bench"]), "raw-range-probe", "--endpoint", "{endpoint}",
                            "--oracle", str(oracle), "--clients", str(args.clients),
                            "--repetitions", str(args.repetitions), "--warmup-repetitions", "1",
                            "--server-pid", "{pid}", "--server-executable", "{executable}", "--report", "{report}"],
                "expect": {"/verified": True, "/chain/tip_unchanged": True, "/totals/requests": args.clients * args.repetitions * 2,
                           "/totals/blocks": args.width * args.clients * args.repetitions * 2,
                           "/totals/bytes": jobs["jobs"][0]["bytes"] * args.clients * args.repetitions * 2},
                "match": ["/oracle/sha256", "/totals/bytes"],
            }
        campaign = {"environment": {"tip_height": args.blocks,
                    "tip_hash": bytes.fromhex(ready["initial_hash"])[::-1].hex(),
                    "network": "zcash-regtest", "protocol_chain": "test", "serving": "compact-blocks", "cache": "warm",
                    "spends_per_block": args.spends_per_block, "reader_worker_threads": workers,
                    "reader_cpus": reader_cpus, "client_cpus": client_cpus},
                    "expect_from_environment": {
                        "/chain/name": "protocol_chain",
                        "/chain/tip_before/height": "tip_height", "/chain/tip_after/height": "tip_height",
                        "/chain/tip_before/hash": "tip_hash", "/chain/tip_after/hash": "tip_hash"},
                    "inputs": inputs, "arms": arms, "cases": cases, "repeats": args.pairs,
                    "order": "alternating", "timeout_seconds": args.period_timeout}
        manifest = output / "campaign.json"
        write(manifest, campaign)
        trials = output / "trials"
        completed = subprocess.run(["taskset", "-c", client_list, sys.executable, SCRIPTS / "run-benchmark-campaign.py", manifest,
                                    "--output", trials], stdout=sys.stderr, check=False)
    if not (trials / "provenance.json").exists():
        return completed.returncode or 4
    decision = subprocess.run([sys.executable, SCRIPTS / "compare-benchmark-campaign.py", trials,
                               "--policy", policy, "--output", output / "decision.json"], check=False)
    return decision.returncode or (4 if completed.returncode else 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="baseline zinder-compat-lightwalletd binary")
    parser.add_argument("--candidate", required=True, help="candidate zinder-compat-lightwalletd binary")
    parser.add_argument("--bench", default="zinder-bench", help="benchmark client")
    parser.add_argument("--writer", default="wallet-reader-fixture", help="fixture writer example")
    parser.add_argument("--output", required=True, type=Path, help="new directory for reports and raw observations")
    parser.add_argument("--policy", type=Path, default=POLICY, help="metric tolerances for this comparison")
    parser.add_argument("--blocks", type=positive, default=128)
    parser.add_argument("--width", type=positive, default=16, help="blocks per request")
    parser.add_argument("--spends-per-block", type=positive, default=16)
    parser.add_argument("--clients", type=positive, default=4)
    parser.add_argument("--repetitions", type=positive, default=256, help="bidirectional passes per client and period")
    parser.add_argument("--pairs", type=positive, default=6)
    parser.add_argument("--workers", type=positive, default=4, help="Tokio and Rayon threads per reader")
    parser.add_argument("--period-timeout", type=positive, default=120, help="seconds per arm in one pair")
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("the local process CPU comparison requires Linux")
    if args.blocks <= 2 * args.width:
        parser.error("--blocks must exceed twice --width for a disjoint historical control")
    try:
        return run(args)
    except (OSError, KeyError, ValueError, RuntimeError) as error:
        parser.exit(2, f"{parser.prog}: {error}\n")


if __name__ == "__main__":
    sys.exit(main())
