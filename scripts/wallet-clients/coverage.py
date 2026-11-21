#!/usr/bin/env python3
"""Collect and compare release-bound wallet correctness observations."""

import argparse
import json
import re
import runpy
import subprocess
import sys
from pathlib import Path

from coverage_payment_recovery import recovery_actions
from coverage_roundtrip import roundtrip_actions
from coverage_evidence import digest, interrupted_actions, load, native_expiry_actions, reorg_actions, require, subtree_actions


ROOT = Path(__file__).resolve().parents[2]
BASELINE = Path(__file__).with_name("coverage-v0.22.0.json")
PROTOCOLS = {
    "lightwalletd": ("compat/lightwalletd/service.proto", "CompactTxStreamer"),
    "native-wallet-query": ("zinder/v1/wallet/wallet.proto", "WalletQuery"),
}
GATE = runpy.run_path(str(ROOT / "scripts/run-wallet-lifecycle.py"))
STEPS = GATE["STEPS"]
QUALIFICATIONS = ("target_endpoint_certified", "transaction_linkage_certified",
                  "restore_certified", "process_restart_certified", "resume_progress_certified")
IDENTITY = ("wallet", "version", "variant", "implementation", "interface", "network", "fixture")
HANDLING = {"read", "validated", "stored", "compared", "ignored", "unresolved"}
OUTCOMES = {"pass", "fail", "unexercised"}


def git(*args):
    completed = subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True)
    require(completed.returncode == 0, f"missing local Git object {args[-1]}")
    return completed.stdout


def selected_release(tag, revision=None):
    require(re.fullmatch(r"v\d+\.\d+\.\d+(?:[-+][\w.-]+)?", tag), "invalid release tag")
    commit = git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}").strip()
    require(revision is None or revision == commit, "selected tag and commit disagree")
    return release_identity(tag, commit)


def inventories(revision):
    result = {}
    for interface, (source, service) in PROTOCOLS.items():
        proto = git("show", f"{revision}:crates/zinder-proto/proto/{source}")
        body = re.search(r"^service " + service + r"\s*\{(.*?)^\}", proto, re.M | re.S)
        require(body, f"missing service {service}")
        result[interface] = sorted(re.findall(r"^\s*rpc\s+(\w+)\s*\(", body[1], re.M))
    return result


def release_identity(tag, revision):
    require(re.fullmatch(r"v\d+\.\d+\.\d+(?:[-+][\w.-]+)?", tag), "invalid release tag")
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "release requires full commit")
    return {"tag": tag, "commit": revision}


def closed(obj, keys, where):
    require(isinstance(obj, dict) and set(obj) <= set(keys), f"unexpected fields in {where}")


def required_text(obj, keys, where):
    require(all(isinstance(obj.get(key), str) and obj[key].strip() for key in keys),
            f"missing text in {where}")


def validate(matrix):
    closed(matrix, {"schema_version", "release", "inventory", "scenarios"}, "matrix")
    require(type(matrix["schema_version"]) is int and matrix["schema_version"] == 1,
            "unsupported matrix schema")
    closed(matrix["release"], {"tag", "commit"}, "release")
    release_identity(matrix["release"]["tag"], matrix["release"]["commit"])
    inventory = matrix["inventory"]
    require(set(inventory) == set(PROTOCOLS), "missing protocol inventory")
    for methods in inventory.values():
        require(isinstance(methods, list) and methods and
                all(isinstance(m, str) and re.fullmatch(r"[A-Z][A-Za-z0-9]+", m) for m in methods)
                and len(set(methods)) == len(methods), "invalid method inventory")
    seen = set()
    require(isinstance(matrix["scenarios"], list) and matrix["scenarios"], "no scenarios")
    for row in matrix["scenarios"]:
        closed(row, {"id", *IDENTITY, "action", "status", "method_scope", "assertions",
                     "methods", "evidence", "client_sha256", "observed_methods", "accounting_zat",
                     "return_accounting_zat"}, "scenario")
        required_text(row, ("id", *IDENTITY, "action"), "scenario")
        ident = row["id"]
        require(re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", ident) and ident not in seen,
                f"invalid or duplicate scenario {ident}")
        seen.add(ident)
        if "client_sha256" in row:
            require(re.fullmatch(r"[0-9a-f]{64}", row["client_sha256"]), f"{ident}: bad client digest")
        require(row["implementation"] in {"engine", "app", "cli", "contract"},
                f"{ident}: invalid implementation")
        require(row["method_scope"] in {"complete", "partial", "none"}, f"{ident}: invalid scope")
        require(row["status"] in OUTCOMES, f"{ident}: invalid result")
        checks = row["assertions"]
        require(isinstance(checks, dict) and checks and all(
            re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", k) and v in OUTCOMES
            for k, v in checks.items()), f"{ident}: missing or invalid checks")
        if row["status"] == "pass":
            require("fail" not in checks.values() and "pass" in checks.values(),
                    f"{ident}: passing result contradicts assertions")
        interface = row["interface"]
        require(interface in inventory and isinstance(row["methods"], dict), f"{ident}: interface")
        require(row["method_scope"] != "none" or not row["methods"],
                f"{ident}: method attribution without observation")
        if "observed_methods" in row:
            monitored = row["observed_methods"]
            require(row["method_scope"] == "partial" and isinstance(monitored, list) and monitored and
                    len(set(monitored)) == len(monitored) and set(monitored) <= set(inventory[interface])
                    and set(row["methods"]) <= set(monitored), f"{ident}: invalid monitored method set")
        for method, obs in row["methods"].items():
            require(method in inventory[interface], f"{ident}: unknown {interface} method {method}")
            closed(obs, {"calls", "outcomes", "fields"}, method)
            n = obs["calls"]
            require(type(n) is int and n > 0, f"{ident}: calls must be a positive count")
            if "outcomes" in obs:
                statuses = obs["outcomes"]
                require(isinstance(statuses, dict) and all(
                    re.fullmatch(r"[a-z][a-z0-9_]*", k) and type(v) is int and v > 0
                    for k, v in statuses.items()) and sum(statuses.values()) <= n,
                    f"{ident}: invalid method outcomes")
            for field, claim in obs.get("fields", {}).items():
                closed(claim, {"handling", "assertion"}, field)
                require(re.fullmatch(r"[A-Za-z][A-Za-z0-9_.]*", field)
                        and claim["handling"] in HANDLING and claim["assertion"] in checks,
                        f"{ident}: field claim requires a named check")
        for field in ("accounting_zat", "return_accounting_zat"):
            if field not in row:
                continue
            accounting = row[field]
            allowed = {"amount", "fee", "change", "balance_after", "spendable_after"}
            if field == "return_accounting_zat":
                require("accounting_zat" in row, f"{ident}: return payment without outbound accounting")
                allowed.update(("debit", "credit"))
            closed(accounting, allowed, "wallet accounting")
            require("amount" in accounting and "balance_after" in accounting and
                    all(type(v) is int and v >= 0 for v in accounting.values()), f"{ident}: invalid wallet accounting")
        evidence = row["evidence"]
        closed(evidence, {"description", "sha256", "related_sha256", "local_status_rpc_during_outage"},
               "evidence pointer")
        if "local_status_rpc_during_outage" in evidence:
            require(evidence["local_status_rpc_during_outage"] in {"available", "unavailable", "wallet-rpc-error"},
                    f"{ident}: unknown local status RPC observation")
        if "related_sha256" in evidence:
            sources = evidence["related_sha256"]
            closed(sources, {"repeat-receipt", "canonical-audit"}, "related evidence")
            require(all(re.fullmatch(r"[0-9a-f]{64}", s) for s in sources.values()), "invalid related evidence hash")
        required_text(evidence, ["description"], "evidence pointer")
        require(re.fullmatch(r"[0-9a-f]{64}", evidence["sha256"]), f"{ident}: missing receipt hash")
    return matrix


def lifecycle(manifest_path, summary_path, ready_path, release):
    manifest, summary, ready = map(load, (manifest_path, summary_path, ready_path))
    provenance = load(Path(summary_path).with_name("provenance.json"))
    require(manifest["schema_version"] == summary["schema_version"] == 2, "use a schema-v2 gate")
    require(provenance["manifest_sha256"] == digest(manifest_path), "manifest provenance mismatch")
    require(ready["zinder_commit"] == release["commit"], "fixture revision mismatch")
    require(set(manifest["cases"]) == set(summary["cases"]), "gate case set disagrees with manifest")
    for name, case in summary["cases"].items():
        spec = manifest["cases"][name]
        keys = ("wallet", "version", "variant", "implementation", "network", "interface", "endpoint")
        require(all(case[k] == spec[k] for k in keys), "gate identity disagrees with manifest")
        require(case["implementation"] in {"app", "engine"}, "use a real client lifecycle")
        pins = case["inputs_sha256"]
        require(pins == spec["inputs"], "gate input set differs from manifest")
        require(digest(ready_path) in pins.values() and spec["client"]["sha256"] in pins.values(),
                "gate did not pin selected fixture and client")
        addr = "lightwalletd" if case["interface"] == "lightwalletd" else "native_wallet_query"
        require(ready[addr] == case["endpoint"] and ready["network"] == case["network"],
                "gate endpoint or network differs from fixture")
        steps = {step["step"]: step for step in case["steps"]}
        assertions = {}
        for key in STEPS:
            step = steps.get(key, {})
            if step.get("status") == "completed":
                GATE["typed_evidence"](step, key)
                passed = (step.get("assertion_mode") == "always-on" and
                          step.get("assertions_enabled") is True and step.get("inputs_verified") is True
                          and step.get("fallback_used") is False and step.get("checks")
                          and all(c["passed"] is True for c in step["checks"]))
                assertions[key] = "pass" if passed else "fail"
            else:
                assertions[key] = "fail" if step.get("status") == "failed" else "unexercised"
        flags = GATE["qualification"](case, case, all(v == "pass" for v in assertions.values())
                                     and case.get("workflow_completed") is True)
        require(all(flags[k] == case.get(k) for k in (*QUALIFICATIONS, "certified")),
                "recorded qualification disagrees with typed evidence")
        for key in QUALIFICATIONS:
            assertions[key.replace("_", "-")] = "pass" if case.get(key) is True else "unexercised"
        passed = case["certified"] is True and all(v == "pass" for v in assertions.values())
        yield {
            "id": f"{name}-funded-lifecycle", **{k: case[k] for k in keys if k != "endpoint"},
            "client_sha256": spec["client"]["sha256"],
            "fixture": f"disposable-{case['network']}-{ready['activation_schedule_sha256']}",
            "action": "Funded restore, discovery, send, confirmation, restart and resume",
            "status": "pass" if passed else "fail", "method_scope": "none", "methods": {},
            "assertions": assertions,
            "evidence": {"description": "Pinned schema-v2 lifecycle summary", "sha256": digest(summary_path)},
        }


def compare(baseline, current, prefixes=()):
    changes = []
    for iface, methods in baseline["inventory"].items():
        for lost in sorted(set(methods) - set(current["inventory"][iface])):
            changes.append(f"{iface}: removed method {lost}")
    by_id = {r["id"]: r for r in current["scenarios"]}
    wanted = [r for r in baseline["scenarios"] if not prefixes or r["id"].startswith(tuple(prefixes))]
    for prefix in prefixes:
        require(any(r["id"].startswith(prefix) for r in wanted), f"baseline has no scenario matching {prefix!r}")
    for old in wanted:
        ident = old["id"]
        new = by_id.get(ident)
        if not new:
            changes.append(f"{ident}: missing scenario")
            continue
        mismatched = [k for k in (*IDENTITY, "client_sha256") if new.get(k) != old.get(k)]
        if mismatched:
            changes.append(f"{ident}: incomparable " + ", ".join(mismatched))
            continue
        if old["status"] == "pass" and new["status"] != "pass":
            changes.append(f"{ident}: outcome {new['status']}")
        if old["method_scope"] == "complete" and new["method_scope"] != "complete":
            changes.append(f"{ident}: lost complete observation")
        if new["method_scope"] != "complete":
            for lost in sorted(set(old.get("observed_methods", [])) - set(new.get("observed_methods", []))):
                changes.append(f"{ident}: lost observation of {lost}")
        for key, direction in (("accounting_zat", "wallet"), ("return_accounting_zat", "return")):
            for field, amount in old.get(key, {}).items():
                if new.get(key, {}).get(field) != amount:
                    changes.append(f"{ident}: changed {direction} accounting for {field}")
        for check, status in old["assertions"].items():
            if status == "pass" and new["assertions"].get(check) != "pass":
                changes.append(f"{ident}: lost passing assertion {check}")
        for method, before in old["methods"].items():
            after = new["methods"].get(method)
            if not after:
                changes.append(f"{ident}: lost observed method {method}")
                continue
            if "ok" in before.get("outcomes", {}) and "ok" not in after.get("outcomes", {}):
                changes.append(f"{ident}: lost successful {method} response")
            for code in sorted(set(before.get("outcomes", {})) - set(after.get("outcomes", {})) - {"ok"}):
                changes.append(f"{ident}: lost observed {method} status {code}")
            for field, prior in before.get("fields", {}).items():
                if old["assertions"][prior["assertion"]] != "pass":
                    continue
                later = after.get("fields", {}).get(field)
                if later != prior or new["assertions"].get(prior["assertion"]) != "pass":
                    changes.append(f"{ident}: changed {method}.{field} evidence")
    return changes


def render(matrix):
    text = [f"# Wallet correctness coverage for {matrix['release']['tag']}", "",
            f"Server commit `{matrix['release']['commit']}`. Matrix schema 1.", "",
            "## Actions and wallet assertions", "",
            "| Scenario | Implementation and version | Action | Result | Passing assertions |",
            "|---|---|---|---|---|"]
    for row in matrix["scenarios"]:
        checks = ", ".join(k for k, v in row["assertions"].items() if v == "pass")
        text.append(f"| `{row['id']}` | {row['wallet']} {row['version']} ({row['implementation']}) "
                    f"| {row['action']} | {row['status']} | {checks or 'None'} |")
    for interface, methods in matrix["inventory"].items():
        text.extend(["", f"## {interface}", "",
                     "Each cell refers to the scenario in the preceding table. A call count records "
                     "method attribution. A field's handling appears only with its named assertion. "
                     "An uncalled method was monitored throughout the action and saw zero calls. "
                     "Other omitted methods have unknown wallet use. "
                     "Methods of the other interface are inapplicable.", "",
                     "| Method | Real-client observations | Direct contract observations |",
                     "|---|---|---|"])
        for m in methods:
            cells = [[], []]
            for row in matrix["scenarios"]:
                if row["interface"] != interface:
                    continue
                obs = row["methods"].get(m)
                if not obs and row["method_scope"] != "complete" and m not in row.get("observed_methods", []):
                    continue
                cell = f"`{row['id']}`"
                if obs:
                    cell += f" ×{obs['calls']}"
                    if obs.get("outcomes"):
                        cell += " (" + ", ".join(f"{v} {k}" for k, v in obs["outcomes"].items()) + ")"
                    for field, claim in obs.get("fields", {}).items():
                        check = claim["assertion"]
                        cell += f"<br>`{field}` {claim['handling']} ({check}, {row['assertions'][check]})"
                else:
                    cell += " uncalled"
                cells[row["implementation"] == "contract"].append(cell)
            text.append(f"| `{m}` | {'<br>'.join(cells[0]) or 'Unobserved'} "
                        f"| {'<br>'.join(cells[1]) or 'No observation'} |")
    return "\n".join(text) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect", help="import gate receipts and scoped observations")
    collect.add_argument("--release", required=True)
    collect.add_argument("--revision", help="verify the commit of the selected local tag")
    collect.add_argument("--lifecycle", nargs=3, action="append", default=[],
                         metavar=("MANIFEST", "SUMMARY", "READY"))
    collect.add_argument("--observations", type=Path, action="append", default=[],
                         help="release-bound observation file (schema_version, release, scenarios)")
    collect.add_argument("--subtree-run", type=Path, help="private subtree-ab.py output directory")
    collect.add_argument("--interrupted-result", type=Path, help="private interrupted-zkool.py receipt.json")
    collect.add_argument("--roundtrip-pair", nargs=2, type=Path, metavar=("FIRST", "SECOND"),
                         help="receipts from two independent signed Regtest round trips")
    collect.add_argument("--roundtrip-audit", type=Path, help="optional independent canonical audit of that pair")
    collect.add_argument("--recovery-result", type=Path, action="append", default=[], metavar="RECEIPT",
                         help="one stopped-wallet, shallow rollback, or live native reconnect receipt (repeatable)")
    collect.add_argument("--native-expiry-result", type=Path, help="private native-expiry.py receipt.json")
    collect.add_argument("--reorg-result", type=Path, help="private reorg-zkool.py receipt.json")
    collect.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("check", help="validate a matrix and compare its inventory to its tagged protocols")
    check.add_argument("matrix", nargs="?", type=Path, default=BASELINE)
    diff = commands.add_parser("compare", help="exit 1 on lost coverage or incomparable scenarios")
    diff.add_argument("current", type=Path)
    diff.add_argument("--baseline", type=Path, default=BASELINE)
    diff.add_argument("--scenario-prefix", action="append", default=[], help="compare just the matching baseline cases")
    doc = commands.add_parser("render", help="write the human-readable matrix to stdout")
    doc.add_argument("matrix", nargs="?", type=Path, default=BASELINE)
    args = parser.parse_args()
    if args.command == "collect":
        release = selected_release(args.release, args.revision)
        rows = [row for files in args.lifecycle for row in lifecycle(*files, release)]
        if args.subtree_run:
            rows.extend(subtree_actions(args.subtree_run, release))
        if args.interrupted_result:
            rows.extend(interrupted_actions(args.interrupted_result, release))
        require(not args.roundtrip_audit or args.roundtrip_pair, "roundtrip canonical audit requires paired receipts")
        if args.roundtrip_pair:
            rows.extend(roundtrip_actions(args.roundtrip_pair, release, args.roundtrip_audit))
        for path in args.recovery_result:
            rows.extend(recovery_actions(path, release))
        if args.native_expiry_result:
            rows.extend(native_expiry_actions(args.native_expiry_result, release))
        if args.reorg_result:
            rows.extend(reorg_actions(args.reorg_result, release))
        for path in args.observations:
            part = load(path)
            require(part["schema_version"] == 1 and part["release"] == release,
                    f"{path}: incompatible release or schema")
            rows.extend(part["scenarios"])
        data = validate(dict(schema_version=1, release=release, inventory=inventories(release["commit"]),
                             scenarios=sorted(rows, key=lambda r: r["id"])))
        with args.output.open("x") as output:
            json.dump(data, output, indent=2)
            output.write("\n")
    elif args.command == "check":
        matrix = validate(load(args.matrix))
        release = selected_release(matrix["release"]["tag"], matrix["release"]["commit"])
        require(matrix["inventory"] == inventories(release["commit"]), "recorded protocol inventory mismatch")
        print(f"{len(matrix['scenarios'])} scenarios with matching tagged protocol inventories")
    elif args.command == "compare":
        losses = compare(validate(load(args.baseline)), validate(load(args.current)), args.scenario_prefix)
        print("\n".join(losses) if losses else "No coverage lost.")
        return int(bool(losses))
    else:
        print(render(validate(load(args.matrix))), end="")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"coverage input error: {error}", file=sys.stderr)
        sys.exit(2)
