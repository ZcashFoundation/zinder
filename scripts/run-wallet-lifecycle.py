#!/usr/bin/env python3
"""Assert real wallet lifecycle steps and retain bounded, redacted receipts."""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import os
import signal
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import urlsplit


sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location(
    "benchmark_campaign", Path(__file__).with_name("run-benchmark-campaign.py")
)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)
STEPS = (
    "bootstrap", "restore", "transparent-discovery", "shielded-discovery",
    "send", "pending", "mined", "scanned", "restart", "resume",
)
STATES = {"completed", "skipped", "blocked", "unsupported", "no-evidence"}


def pins(entries):
    if not isinstance(entries, dict) or not entries:
        raise ValueError("nonempty input pins required")
    observed = {}
    for path, expected in entries.items():
        if not isinstance(expected, str) or not benchmark.re.fullmatch(r"[a-f0-9]{64}", expected):
            raise ValueError("invalid SHA-256 pin")
        if not Path(path).is_absolute():
            raise ValueError("input paths must be absolute")
        actual = benchmark.sha256(path)
        if actual != expected:
            raise ValueError("input digest mismatch")
        observed[path] = actual
    return observed


def validate(manifest):
    if type(manifest.get("schema_version")) is not int or manifest["schema_version"] not in {1, 2}:
        raise ValueError("schema_version must be 1 or 2")
    if not benchmark.positive_int(manifest.get("timeout_seconds")):
        raise ValueError("timeout_seconds must be a positive integer")
    cases = manifest["cases"]
    if not isinstance(cases, dict) or not cases:
        raise ValueError("nonempty cases required")
    for name, case in cases.items():
        if not benchmark.NAME.fullmatch(name) or name.casefold() in benchmark.CAMPAIGN_FILES:
            raise ValueError("invalid case name")
        for key in ("wallet", "version", "variant"):
            if not isinstance(case[key], str) or not benchmark.NAME.fullmatch(case[key]):
                raise ValueError("wallet, version, and variant must be safe identifiers")
        if case["network"] not in {"zcash-regtest", "zcash-testnet", "zcash-mainnet"}:
            raise ValueError("invalid network")
        if case.get("restart_scope", "unspecified") not in {"wallet-reopen", "process-restart", "unspecified"}:
            raise ValueError("invalid restart_scope")
        if case["implementation"] not in {"app", "engine", "simulated-profile"}:
            raise ValueError("invalid implementation")
        if case["interface"] not in {"lightwalletd", "native-wallet-query"}:
            raise ValueError("invalid interface")
        url = urlsplit(case["endpoint"])
        if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError("endpoint must be an HTTP URL without credentials, query, or fragment")
        if case.get("require_no_fallback") not in (True, False) or type(case.get("require_no_fallback")) is not bool:
            raise ValueError("require_no_fallback boolean required")
        if case["wallet"].lower() == "zkool" and case["variant"] not in {"internal-enabled", "external-only"}:
            raise ValueError("Zkool requires separate internal-enabled and external-only cases")
        if not isinstance(case.get("steps"), dict) or set(case["steps"]) - set(STEPS):
            raise ValueError("unknown lifecycle step")
        if "input_paths" in case.get("cleanup", {}):
            dependencies = case["cleanup"]["input_paths"]
            if not isinstance(dependencies, list) or not all(
                isinstance(path, str) and path in case["inputs"] for path in dependencies
            ):
                raise ValueError("cleanup input_paths must name pinned case inputs")
        for step in (*case["steps"].values(), *(case[k] for k in ("prepare", "cleanup") if k in case)):
            command = step["command"]
            if not isinstance(command, list) or not command or not all(isinstance(arg, str) for arg in command):
                raise ValueError("command must be a nonempty argv array")
            if "{report}" not in command:
                raise ValueError("command must receive {report}")
            expected = step.get("expect")
            if not isinstance(expected, dict) or not expected or not all(benchmark.is_pointer(p) for p in expected):
                raise ValueError("every command requires nonempty expect JSON-pointer assertions")
            if any(p in {"/status", "/endpoint", "/fallback_used"} for p in expected):
                raise ValueError("expect must assert domain results beyond the receipt envelope")


def digest_token(token):
    return isinstance(token, str) and benchmark.re.fullmatch(r"[a-f0-9]{64}", token) is not None


def height_number(number):
    return type(number) is int and 0 <= number <= 2**32 - 1


def retain_fields(report, key, required, optional=()):
    observed = report[key]
    if not isinstance(observed, dict):
        raise ValueError("evidence object required")
    return {field: observed[field] for field in required} | {
        field: observed[field] for field in optional if field in observed
    }


def typed_evidence(report, name):
    retained = {}
    if report.get("assertions_enabled") is not True or report.get("assertion_mode") != "always-on":
        raise ValueError("always-on domain checks required")
    retained.update({"assertions_enabled": True, "assertion_mode": "always-on"})
    if name in {"send", "pending", "mined", "scanned"}:
        transaction = retain_fields(report, "transaction", ("fingerprint_sha256", "observation"))
        observations = {"send": "submitted", "pending": "wallet-pending", "mined": "canonical-mined", "scanned": "wallet-scanned"}
        if not digest_token(transaction["fingerprint_sha256"]) or transaction["observation"] != observations[name]:
            raise ValueError("transaction observation required")
        if name in {"mined", "scanned"}:
            mined_height = report["transaction"]["mined_height"]
            if not height_number(mined_height) or mined_height == 0:
                raise ValueError("canonical mined height required")
            transaction["mined_height"] = mined_height
        retained["transaction"] = transaction
    if name == "restore":
        fingerprints = tuple(f"{side}_{domain}_fingerprint_sha256" for domain in ("database", "wallet", "activity") for side in ("source", "restored"))
        restore = retain_fields(report, "restore", (*fingerprints, "fresh_database", "restored_activity_count"))
        if not all(digest_token(restore[field]) for field in fingerprints):
            raise ValueError("restore fingerprints required")
        if restore["fresh_database"] is not True or not benchmark.positive_int(restore["restored_activity_count"]):
            raise ValueError("fresh populated database restore required")
        if restore["source_database_fingerprint_sha256"] == restore["restored_database_fingerprint_sha256"]:
            raise ValueError("independent restore database required")
        for domain in ("wallet", "activity"):
            if restore[f"source_{domain}_fingerprint_sha256"] != restore[f"restored_{domain}_fingerprint_sha256"]:
                raise ValueError("restore identity or activity mismatch")
        retained["restore"] = restore
    if name == "restart" and report.get("restart_scope") == "process-restart":
        restart = retain_fields(report, "restart", ("before_process_id", "after_process_id", "before_height", "after_height"))
        if not all(benchmark.positive_int(restart[field]) for field in ("before_process_id", "after_process_id")):
            raise ValueError("observed process identifiers required")
        if restart["before_process_id"] == restart["after_process_id"]:
            raise ValueError("different wallet process required")
        if not all(height_number(restart[field]) for field in ("before_height", "after_height")) or restart["after_height"] < restart["before_height"]:
            raise ValueError("restart must preserve persisted progress")
        retained["restart"] = restart
    if name == "resume":
        progress = retain_fields(report, "progress", ("before_height", "after_height", "new_block_mined_after_restart"))
        if not all(height_number(progress[field]) for field in ("before_height", "after_height")):
            raise ValueError("observed resume heights required")
        if progress["after_height"] <= progress["before_height"] or progress["new_block_mined_after_restart"] is not True:
            raise ValueError("post-restart new block progress required")
        retained["progress"] = progress
    if "endpoint_evidence" in report:
        endpoint = retain_fields(report, "endpoint_evidence", ("scope", "target_connected", "unexpected_connections"), ("trace_fingerprint_sha256",))
        if endpoint["scope"] not in {"configured", "process-trace", "local-only"} or type(endpoint["target_connected"]) is not bool or type(endpoint["unexpected_connections"]) is not int or endpoint["unexpected_connections"] < 0:
            raise ValueError("invalid endpoint evidence")
        if endpoint["scope"] == "local-only" and endpoint["target_connected"]:
            raise ValueError("local-only steps cannot claim a target connection")
        if "trace_fingerprint_sha256" in endpoint and not digest_token(endpoint["trace_fingerprint_sha256"]):
            raise ValueError("invalid trace fingerprint")
        if endpoint["scope"] == "process-trace" and "trace_fingerprint_sha256" not in endpoint:
            raise ValueError("trace input fingerprint required")
        retained["endpoint_evidence"] = endpoint
    return retained


def qualification(evidence, case, complete):
    steps = {step["step"]: step for step in evidence["steps"]}
    eligible = complete and case.get("schema_version") == 2 and case["implementation"] != "simulated-profile"
    reports = list(steps.values()) + [evidence[key] for key in ("prepare", "cleanup") if key in evidence]
    observations = [step.get("endpoint_evidence", {}) for step in steps.values()]
    connected = any(observation.get("target_connected") is True for observation in observations)
    target = eligible and case["require_no_fallback"] and connected and all(
        report.get("fallback_used") is False
        and report.get("endpoint_evidence", {}).get("unexpected_connections", 0) == 0
        for report in reports
    )
    # A socket trace is an additional claim; configured endpoint evidence remains explicit.
    trace = target and all(
        step.get("endpoint_evidence", {}).get("scope") in {"process-trace", "local-only"}
        and digest_token(step["endpoint_evidence"].get("trace_fingerprint_sha256"))
        and step["endpoint_evidence"]["unexpected_connections"] == 0
        for step in steps.values()
    )
    transactions = [steps.get(name, {}).get("transaction", {}) for name in ("send", "pending", "mined", "scanned")]
    linked = eligible and all(transaction.get("fingerprint_sha256") for transaction in transactions) and len({t["fingerprint_sha256"] for t in transactions}) == 1
    linked = bool(linked and transactions[2]["mined_height"] == transactions[3]["mined_height"])
    restored = eligible and "restore" in steps.get("restore", {})
    restart = steps.get("restart", {}).get("restart", {})
    process = eligible and case.get("restart_scope") == "process-restart" and bool(restart)
    progress = steps.get("resume", {}).get("progress", {})
    advanced = process and bool(progress) and progress["before_height"] >= restart["after_height"]
    flags = {
        "workflow_completed": complete,
        "target_connection_observed": bool(connected),
        "target_endpoint_certified": bool(target), "endpoint_trace_certified": bool(trace),
        "transaction_linkage_certified": linked, "restore_certified": bool(restored),
        "process_restart_certified": bool(process), "resume_progress_certified": bool(advanced),
        "wallet_reopen_certified": eligible and case.get("restart_scope") == "wallet-reopen",
        "endpoint_evidence_scope": "process-trace" if trace else "configured" if target else "unqualified",
    }
    flags["certified"] = bool(target and linked and restored and advanced)
    flags["qualification_problems"] = [name for name in (
        "target_endpoint_certified", "transaction_linkage_certified", "restore_certified", "process_restart_certified", "resume_progress_certified"
    ) if not flags[name]]
    return flags


def execute(step, case, root, name, timeout):
    directory = root / name
    directory.mkdir()
    report_path = directory / "adapter.json"
    replacements = {"{report}": str(report_path), "{scratch}": str(root / "scratch"), "{endpoint}": case["endpoint"]}
    command = [replacements.get(arg, arg) for arg in step["command"]]
    receipt = {
        "command_sha256": hashlib.sha256(benchmark.json.dumps(command).encode()).hexdigest(),
        "step": name, "status": "failed", "checks": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    started = time.monotonic()
    try:
        binary = shutil.which(command[0])
        if binary is None:
            raise ValueError("adapter executable missing")
        command[0] = str(Path(binary).resolve())
        receipt["executable_sha256"] = benchmark.sha256(command[0])
        if case["inputs"].get(command[0]) != receipt["executable_sha256"]:
            raise ValueError("adapter executable must be pinned in inputs")
        # Adapter logs may contain seed phrases or RPC authorization. Retain assertions only.
        inputs = case["inputs"]
        if name == "cleanup" and "input_paths" in step:
            required = set(step["input_paths"]) | {arg for arg in command if arg in inputs}
            inputs = {path: inputs[path] for path in required}
        pins(inputs)
        receipt["inputs_verified"] = True
        case["_command_entered"] = True
        receipt["command_entered"] = True
        with open(os.devnull, "wb") as sink:
            code = benchmark.run_command(command, sink, sink, timeout)
        receipt["exit_code"] = code
        if code:
            raise ValueError("adapter exited unsuccessfully")
        report = benchmark.read_json(report_path)
        state = report["status"]
        if state not in STATES:
            raise ValueError("invalid adapter status")
        if state != "completed":
            reason = report.get("reason_code", "adapter-reported-incomplete")
            if not isinstance(reason, str) or not benchmark.NAME.fullmatch(reason):
                raise ValueError("invalid reason code")
            receipt["reason_code"] = reason
            receipt["status"] = state
            return receipt
        if report["endpoint"] != case["endpoint"] or type(report["fallback_used"]) is not bool:
            raise ValueError("endpoint evidence missing or inconsistent")
        if case["require_no_fallback"] and report["fallback_used"]:
            raise ValueError("fallback violates case policy")
        if name == "restart":
            scope = report.get("restart_scope", "unspecified")
            if scope != case.get("restart_scope", "unspecified"):
                raise ValueError("restart evidence scope does not match requested scope")
            receipt["restart_scope"] = scope
        if case.get("schema_version") == 2:
            receipt.update(typed_evidence(report, name))
        for pointer, expected in step["expect"].items():
            passed = benchmark.json_equal(benchmark.at_pointer(report, pointer), expected)
            receipt["checks"].append({"pointer": pointer, "passed": passed})
        if not all(check["passed"] for check in receipt["checks"]):
            raise ValueError("domain assertion failed")
        receipt["status"] = "completed"
        receipt["fallback_used"] = report["fallback_used"]
    except subprocess.TimeoutExpired:
        receipt["problem"] = "adapter timeout; process group killed"
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        receipt["problem"] = "adapter failed, missing report, or unsatisfied assertion"
    finally:
        receipt["elapsed_seconds"] = time.monotonic() - started
        # Never publish raw adapter reports, arbitrary exception strings, or argv.
        if report_path.is_file():
            report_path.unlink()
        benchmark.write_json(directory / "receipt.json", receipt)
    return receipt


def inputs_unchanged(case, evidence):
    try:
        pins(case["inputs"])
        return True
    except (OSError, KeyError, ValueError, TypeError):
        evidence["postflight"] = "immutable inputs changed or unavailable"
        return False


def run_case(case, root, timeout):
    case["_command_entered"] = False
    root.mkdir()
    (root / "scratch").mkdir(mode=0o700)
    evidence = {k: case[k] for k in (
        "wallet", "version", "variant", "network", "endpoint", "interface", "implementation", "require_no_fallback"
    )}
    evidence.update({"schema_version": case["schema_version"], "certified": False, "steps": [], "restart_scope": case.get("restart_scope", "unspecified")})
    ready = True
    finished = False
    try:
        try:
            evidence["inputs_sha256"] = pins(case["inputs"])
            if case["client"]["path"] not in case["inputs"] or case["client"]["sha256"] != case["inputs"][case["client"]["path"]]:
                raise ValueError("client must be an immutable input")
        except (OSError, KeyError, ValueError, TypeError):
            ready = False
            evidence["preflight"] = "blocked: missing or mismatched immutable input/client"
        if ready and "prepare" in case:
            evidence["prepare"] = execute(case["prepare"], case, root, "prepare", timeout)
            ready = evidence["prepare"]["status"] == "completed"
        for name in STEPS:
            if name not in case["steps"]:
                receipt = {"step": name, "status": "no-evidence"}
                ready = False
            elif not ready:
                receipt = {"step": name, "status": "blocked"}
            else:
                receipt = execute(case["steps"][name], case, root, name, timeout)
                ready = receipt["status"] == "completed"
            evidence["steps"].append(receipt)
        if not inputs_unchanged(case, evidence):
            ready = False
        finished = True
    finally:
        cleanup_ok = "cleanup" not in case
        try:
            if "cleanup" in case and case.get("_command_entered", False):
                evidence["cleanup"] = execute(case["cleanup"], case, root, "cleanup", timeout)
                cleanup_ok = evidence["cleanup"]["status"] == "completed"
            elif "cleanup" in case:
                evidence["cleanup"] = {"status": "skipped", "reason_code": "no-pinned-command-entered"}
        finally:
            shutil.rmtree(root / "scratch")
            if evidence.get("cleanup", {}).get("command_entered") and not inputs_unchanged(case, evidence):
                ready = False
            complete = (
                finished and ready and cleanup_ok and len(evidence["steps"]) == len(STEPS)
                and all(step["status"] == "completed" for step in evidence["steps"])
            )
            evidence.update(qualification(evidence, case, complete))
            benchmark.write_json(root / "receipt.json", evidence)
    return evidence


def interrupted(*_):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="new private evidence directory")
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        manifest = benchmark.read_json(args.manifest)
        validate(manifest)
        output = args.output.resolve()
        output.mkdir(mode=0o700, parents=True)
        source_git = ["git", "-C", str(Path(__file__).resolve().parent.parent)]
        benchmark.write_json(output / "provenance.json", {
            "schema_version": manifest["schema_version"], "manifest_sha256": benchmark.sha256(args.manifest),
            "runner_sha256": benchmark.sha256(__file__),
            "benchmark_runner_sha256": benchmark.sha256(benchmark.__file__),
            "source_revision": subprocess.check_output(source_git + ["rev-parse", "HEAD"], text=True).strip(),
            "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(source_git + ["diff", "HEAD"])).hexdigest(),
        })
        cases = {name: run_case(dict(case, schema_version=manifest["schema_version"]), output / name, manifest["timeout_seconds"]) for name, case in manifest["cases"].items()}
        certified = all(case["certified"] for case in cases.values())
        benchmark.write_json(output / "summary.json", {"schema_version": manifest["schema_version"], "certified": certified, "cases": cases})
        return 0 if certified else 1
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.CalledProcessError):
        print("Invalid lifecycle manifest or unavailable evidence directory; no certification", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
