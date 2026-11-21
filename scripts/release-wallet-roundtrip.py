#!/usr/bin/env python3
"""Qualify a release candidate on two fresh deterministic Regtest wallet round trips.

The candidate archive must be built from the release tag it names. Two isolated
chains are provisioned, each wallet case runs on its own chain, and the paired
receipts plus the canonical audit are compared with the four versioned
round-trip rows in the release coverage matrix. Only allowlisted aggregate
files are published, including when a stage fails.
"""
import argparse
import copy
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
CAMPAIGN = SCRIPTS / "run-wallet-roundtrip-regtest.py"
WALLET_CLIENTS = SCRIPTS / "wallet-clients"
DEFAULT_PORTS = (51500, 51600)
WALLET_PORT_OFFSET = 82
CAMPAIGN_ATTEMPTS = 2
IMAGE_NAMES = ("compat-lightwalletd", "ingest", "projector", "query")
CANDIDATE_BINARIES = tuple("zinder-" + name for name in IMAGE_NAMES)
ROUNDTRIP_ROW_IDS = (
    "roundtrip-outbound-zingo-send",
    "roundtrip-outbound-zallet-receive",
    "roundtrip-return-zallet-send",
    "roundtrip-return-zingo-receive",
)
RECEIPT_KEYS = {
    "schema_version", "status", "network", "inputs", "independent_wallets", "wallet_identity_sha256",
    "reopen_ledger_verified", "funding", "pending_certified", "legs", "fixture_recipe",
    "harness_sha256", "invocation_harnesses",
}
AUDIT_KEYS = {
    "schema_version", "passed", "independent_chains", "distinct_wallet_identities",
    "independent_transactions", "runs", "matching_outcomes",
}
AUDIT_RUN_KEYS = {"receipt_sha256", "ready_sha256", "canonical_txid_fingerprints", "end_height"}
PAIRED_KEYS = {"status", "same_wallet_outcomes", "independent_fixtures"}
CLEANUP_KEYS = {"first", "second"}
FORBIDDEN_KEYS = {
    "mnemonic", "seed", "seed_phrase", "private_key", "spending_key", "viewing_key", "password",
    "cookie", "token", "credentials", "authorization", "txid", "txids", "address", "addresses",
    "raw_transaction", "raw_block", "journal", "wallet_files", "keys",
}
EVIDENCE_ALLOWLIST = (
    "summary.json",
    "gate.json",
    "comparison.json",
    "rows.json",
    "paired.json",
    "canonical-audit.json",
    "cleanup.json",
    "first/receipt.json",
    "second/receipt.json",
)
FULL_COMMIT = re.compile(r"[0-9a-f]{40}")
IMMUTABLE_IMAGE = re.compile(r"(?:sha256:[0-9a-f]{64}|[a-z0-9][^\s]*@sha256:[0-9a-f]{64})")
SAFE_MEMBER = re.compile(r"[A-Za-z0-9._-]+")


class GateBlocked(Exception):
    """An explicit operator gate is unresolved; the candidate must not pass."""


class CampaignFailed(RuntimeError):
    def __init__(self, message, campaign_state, attempts):
        super().__init__(message)
        self.campaign_state = campaign_state
        self.attempts = attempts


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def read_json(path):
    with Path(path).open("rb") as source:
        return json.load(source)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as dest:
        json.dump(value, dest, indent=2, sort_keys=True)
        dest.write("\n")
        dest.flush()
        os.fsync(dest.fileno())
    temporary.replace(path)


def repository_path(path):
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def scan_keys(value, location="evidence"):
    if isinstance(value, dict):
        blocked = sorted(set(value) & FORBIDDEN_KEYS)
        require(not blocked, f"{location}: aggregate evidence contains blocked keys: {', '.join(blocked)}")
        for key, item in value.items():
            scan_keys(item, f"{location}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            scan_keys(item, f"{location}[{index}]")


def extract(archive, destination):
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        require(members, "archive is empty")
        for member in members:
            parts = Path(member.name).parts
            require(not member.name.startswith("/") and ".." not in parts,
                    "archive member escapes its extraction root")
            require(member.isfile() or member.isdir(), "archive contains a non-regular entry")
        try:
            bundle.extractall(destination, filter="data")
        except TypeError:
            bundle.extractall(destination)


def validate_ports(port_bases):
    first, second = port_bases
    require(first != second and min(port_bases) >= 1024, "choose two distinct unprivileged port bases")
    offsets = (2, 5, 6, 7, 10, 32, 67, WALLET_PORT_OFFSET)
    ports = [base + offset for base in port_bases for offset in offsets]
    require(max(ports) <= 65535 and len(set(ports)) == len(ports),
            "port bases must leave disjoint fixture and wallet ports below 65535")
    return list(port_bases)


def load_candidate(archive, archive_sha256, tag, commit, workspace):
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    archive = Path(archive).resolve()
    require(FULL_COMMIT.fullmatch(commit or ""), "candidate commit must be 40 lowercase hex characters")
    require(re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", tag or ""),
            "candidate tag must be a complete v-prefixed SemVer tag")
    if not archive.is_file():
        raise GateBlocked("candidate archive is not available at the supplied path")
    observed_archive_sha = digest(archive)
    if archive_sha256:
        require(re.fullmatch(r"[0-9a-f]{64}", archive_sha256), "candidate archive pin is not a SHA-256 digest")
        require(observed_archive_sha == archive_sha256, "candidate archive differs from its pinned SHA-256")
    extracted = workspace / "candidate"
    extracted.mkdir()
    extract(archive, extracted)
    builds = sorted(extracted.glob("*/BUILD-INFO.json"))
    require(len(builds) == 1, "candidate archive must contain exactly one BUILD-INFO.json")
    build_root = builds[0].parent
    build = read_json(builds[0])
    require(build.get("schema_version") == 1, "candidate BUILD-INFO schema is not supported")
    require(build.get("tag") == tag and build.get("commit") == commit,
            "candidate archive BUILD-INFO does not name the requested tag and commit")
    require(build.get("version") == tag[1:], "candidate archive BUILD-INFO version differs from its tag")
    listed = {row.get("name"): row for row in build.get("binaries", []) if isinstance(row, dict)}
    binaries = {}
    for name in CANDIDATE_BINARIES:
        require(name in listed, f"candidate archive lacks {name}")
        binary = build_root / "bin" / name
        require(binary.is_file(), f"candidate archive lacks the {name} executable")
        sha256 = digest(binary)
        require(sha256 == listed[name].get("sha256"),
                f"{name}: executable digest differs from the BUILD-INFO pin")
        binaries[name] = {"sha256": sha256, "size": binary.stat().st_size}
    return {
        "tag": tag,
        "commit": commit,
        "version": build["version"],
        "archive": str(archive),
        "archive_sha256": observed_archive_sha,
        "archive_pin": "pinned-input" if archive_sha256 else "candidate-build-artifact",
        "build_info_sha256": digest(builds[0]),
        "bin_dir": build_root / "bin",
        "binaries": binaries,
    }


def load_clients(bundle, bundle_sha256, workspace):
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    bundle = Path(bundle).resolve()
    if not bundle.is_file():
        raise GateBlocked(
            "operator gate: the pinned wallet client bundle is not available. "
            "Publish the checksummed bundle described in scripts/release-wallet-roundtrip.md "
            "and configure WALLET_ROUNDTRIP_CLIENT_BUNDLE_URL and "
            "WALLET_ROUNDTRIP_CLIENT_BUNDLE_SHA256 for the release workflow.")
    require(re.fullmatch(r"[0-9a-f]{64}", bundle_sha256 or ""),
            "client bundle pin must be a SHA-256 digest")
    observed_sha = digest(bundle)
    require(observed_sha == bundle_sha256, "client bundle differs from its pinned SHA-256")
    extracted = workspace / "clients"
    extracted.mkdir()
    extract(bundle, extracted)
    manifests = sorted(extracted.glob("inputs.json")) + sorted(extracted.glob("*/inputs.json"))
    require(len(manifests) == 1, "client bundle must contain exactly one inputs.json")
    root = manifests[0].parent
    pins = read_json(manifests[0])
    require(pins.get("schema_version") == 1, "client bundle inputs schema is not supported")
    clients = {}
    for role in ("zingo", "zallet"):
        entry = pins.get(role)
        require(isinstance(entry, dict), f"client bundle lacks the {role} pin")
        name = entry.get("binary")
        require(isinstance(name, str) and SAFE_MEMBER.fullmatch(name), f"{role} binary name is not a file name")
        source = entry.get("source")
        require(isinstance(source, str) and FULL_COMMIT.fullmatch(source),
                f"{role} pin must record a full 40-character source commit")
        expected = entry.get("sha256")
        require(isinstance(expected, str) and re.fullmatch(r"[0-9a-f]{64}", expected),
                f"{role} pin must record a SHA-256 digest")
        binary = root / name
        require(binary.is_file(), f"client bundle lacks the {role} executable")
        observed = digest(binary)
        require(observed == expected, f"{role} executable differs from its bundle pin")
        clients[role] = {"path": binary, "sha256": observed, "source": source}
    return {"bundle_sha256": observed_sha, "inputs_sha256": digest(manifests[0]), "clients": clients}


def load_images(path):
    images = read_json(path)
    require(set(images) == set(IMAGE_NAMES),
            "--images must map ingest/projector/query/compat-lightwalletd to immutable images")
    for name, reference in images.items():
        require(isinstance(reference, str) and IMMUTABLE_IMAGE.fullmatch(reference),
                f"{name}: image reference must be an immutable digest")
    return {name: images[name] for name in IMAGE_NAMES}


def coverage_module():
    """Load the versioned coverage validator and comparator in place."""
    if str(WALLET_CLIENTS) not in sys.path:
        sys.path.insert(0, str(WALLET_CLIENTS))
    import coverage as coverage_matrix

    return coverage_matrix


def load_baseline(path):
    path = Path(path).resolve()
    require(path.is_file(), "versioned coverage matrix is missing")
    matrix = read_json(path)
    coverage_module().validate(matrix)
    require(matrix.get("schema_version") == 1, "versioned coverage matrix schema is not supported")
    release = matrix["release"]
    rows = {row["id"]: row for row in matrix["scenarios"] if row["id"] in ROUNDTRIP_ROW_IDS}
    require(set(rows) == set(ROUNDTRIP_ROW_IDS),
            "versioned coverage matrix must contain the four round-trip rows")
    return {
        "matrix": repository_path(path),
        "matrix_sha256": digest(path),
        "tag": release["tag"],
        "commit": release["commit"],
        "inventory": copy.deepcopy(matrix["inventory"]),
        "matrix_data": matrix,
        "rows": rows,
    }


def preflight(options, workspace):
    candidate = load_candidate(
        options.candidate_archive, options.archive_sha256, options.tag, options.commit,
        workspace / "candidate-work")
    clients = load_clients(options.client_bundle, options.client_bundle_sha256,
                           workspace / "client-work")
    images = load_images(options.images)
    baseline = load_baseline(options.baseline_matrix)
    port_bases = validate_ports(options.port_bases)
    gate = {
        "schema_version": 1,
        "status": "ready",
        "candidate": {key: candidate[key] for key in (
            "tag", "commit", "version", "archive_sha256", "archive_pin", "build_info_sha256", "binaries")},
        "clients": {
            "bundle_sha256": clients["bundle_sha256"],
            "inputs_sha256": clients["inputs_sha256"],
            "zingo": {key: clients["clients"]["zingo"][key] for key in ("source", "sha256")},
            "zallet": {key: clients["clients"]["zallet"][key] for key in ("source", "sha256")},
        },
        "images": images,
        "baseline": {key: baseline[key] for key in ("matrix", "matrix_sha256", "tag", "commit")},
        "port_bases": port_bases,
    }
    return candidate, clients["clients"], baseline, gate


def port_collision(campaign_state):
    for log in sorted(Path(campaign_state).glob("*-up.log")):
        try:
            if "Address already in use" in log.read_text(errors="replace"):
                return True
        except OSError:
            continue
    return False


def run_campaign(options, candidate, clients):
    state = Path(options.state).resolve()
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    attempts = []
    campaign_state = None
    for attempt in range(1, CAMPAIGN_ATTEMPTS + 1):
        campaign_state = state / f"campaign-{attempt}"
        require(not campaign_state.exists(), "campaign state already exists; choose a fresh private directory")
        command = [
            sys.executable, "-B", str(CAMPAIGN),
            "--state", str(campaign_state),
            "--port-bases", *(str(base) for base in options.port_bases),
            "--images", str(Path(options.images).resolve()),
            "--bin-dir", str(candidate["bin_dir"]),
            "--revision", candidate["commit"],
            "--zingo", str(clients["zingo"]["path"]),
            "--zallet-backend", str(clients["zallet"]["path"]),
        ]
        log = state / f"campaign-{attempt}.log"
        with log.open("x") as output, subprocess.Popen(
                command, stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as proc:
            try:
                code = proc.wait()
            except KeyboardInterrupt:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                proc.wait()
                raise
        attempts.append({"attempt": attempt, "exit_code": code,
                         "port_collision": port_collision(campaign_state)})
        if code == 0:
            cleanup = read_json(campaign_state / "cleanup.json")
            require(cleanup == {"first": "removed", "second": "removed"},
                    "both disposable fixtures must be removed after the campaign")
            return campaign_state, attempts
        if not attempts[-1]["port_collision"]:
            break
        if attempt < CAMPAIGN_ATTEMPTS:
            time.sleep(2)
    summary = ", ".join(f"attempt {row['attempt']} exited {row['exit_code']}" for row in attempts)
    raise CampaignFailed(f"two-chain campaign did not complete: {summary}", campaign_state, attempts)


def bind_campaign_evidence(campaign_state, candidate, clients, commit):
    """Require both receipts to name the candidate archive and both client pins."""
    receipt_pins = {name: candidate["binaries"]["zinder-" + name]["sha256"] for name in IMAGE_NAMES}
    for label in ("first", "second"):
        receipt = read_json(Path(campaign_state) / label / "receipt.json")
        require(receipt["inputs"]["revision"] == commit,
                f"{label} receipt names a different candidate revision")
        require(receipt["fixture_recipe"]["release_binaries"] == receipt_pins,
                f"{label} receipt ran different candidate binaries than the release archive")
        require(receipt["inputs"]["zingo_sha256"] == clients["zingo"]["sha256"]
                and receipt["inputs"]["zallet_sha256"] == clients["zallet"]["sha256"],
                f"{label} receipt ran wallet clients that differ from the pinned bundle")
    return {
        "tag": candidate["tag"],
        "commit": candidate["commit"],
        "archive_sha256": candidate["archive_sha256"],
        "binaries": {name: candidate["binaries"][name]["sha256"] for name in CANDIDATE_BINARIES},
        "zingo_sha256": clients["zingo"]["sha256"],
        "zallet_sha256": clients["zallet"]["sha256"],
    }


def project_rows(campaign_state, tag, commit):
    if str(WALLET_CLIENTS) not in sys.path:
        sys.path.insert(0, str(WALLET_CLIENTS))
    from coverage_roundtrip import roundtrip_actions

    receipts = [campaign_state / "first" / "receipt.json", campaign_state / "second" / "receipt.json"]
    audit = campaign_state / "canonical-audit.json"
    for path in (*receipts, audit):
        require(path.is_file(), f"campaign evidence is incomplete: {path.name}")
    for path in receipts:
        require(read_json(path)["inputs"]["revision"] == commit, "receipt names a different candidate revision")
    rows = list(roundtrip_actions(receipts, {"tag": tag, "commit": commit}, audit))
    require(len(rows) == len(ROUNDTRIP_ROW_IDS) and
            {row["id"] for row in rows} == set(ROUNDTRIP_ROW_IDS),
            "candidate evidence does not project into the four versioned round-trip rows")
    return rows


def compare_rows(candidate_rows, baseline, tag, commit):
    """Compare the four projected rows with the versioned matrix, strictly.

    The versioned coverage validator and comparator decide this comparison, so
    every baseline field stays binding: wallet and client identity, fixture,
    action, status, assertions, and every wallet accounting value, including
    fees, balances, change, and spendability. A release that intends to change
    one of those values updates the versioned matrix in the same reviewed
    change; the gate never accepts a silent difference.
    """
    coverage_matrix = coverage_module()
    current = {
        "schema_version": 1,
        "release": {"tag": tag, "commit": commit},
        "inventory": copy.deepcopy(baseline["inventory"]),
        "scenarios": candidate_rows,
    }
    expected_ids = set(ROUNDTRIP_ROW_IDS)
    observed = {row["id"]: row for row in candidate_rows}
    issues = []
    changes = []
    valid_matrix = True
    try:
        coverage_matrix.validate(current)
    except ValueError as error:
        valid_matrix = False
        issues.append({"problem": f"candidate rows are not a valid coverage matrix: {error}"})
    else:
        for row_id in sorted(expected_ids - set(observed)):
            issues.append({"row": row_id, "problem": "candidate evidence lacks the versioned row"})
        for row_id in sorted(set(observed) - expected_ids):
            issues.append({"row": row_id, "problem": "candidate evidence adds an unknown row"})
        for row_id in ROUNDTRIP_ROW_IDS:
            expected = baseline["rows"][row_id]
            actual = observed.get(row_id)
            if actual is None:
                continue
            if actual.get("status") != "pass":
                issues.append({"row": row_id, "field": "status", "candidate": actual.get("status")})
            if actual.get("action") != expected.get("action"):
                issues.append({"row": row_id, "field": "action", "baseline": expected.get("action"),
                               "candidate": actual.get("action")})
            for name, status in sorted(actual.get("assertions", {}).items()):
                if status != "pass":
                    issues.append({"row": row_id, "assertion": name, "candidate": status})
            if set(actual.get("assertions", {})) != set(expected.get("assertions", {})):
                issues.append({"row": row_id, "field": "assertions",
                               "problem": "candidate assertion set differs from the versioned row"})
            if set(actual.get("accounting_zat", {})) != set(expected.get("accounting_zat", {})):
                issues.append({"row": row_id, "field": "accounting_zat",
                               "problem": "candidate accounting fields differ from the versioned row"})
        changes = coverage_matrix.compare(baseline["matrix_data"], current, ("roundtrip-",))
        issues.extend({"problem": change} for change in changes)
    observed_snapshot = {
        row_id: {
            "client_sha256": row.get("client_sha256"),
            "fixture": row.get("fixture"),
            "status": row.get("status"),
            "accounting_zat": row.get("accounting_zat", {}),
        }
        for row_id, row in sorted(observed.items())
    }
    return {
        "schema_version": 1,
        "baseline": {"tag": baseline["tag"], "commit": baseline["commit"],
                     "matrix": baseline["matrix"], "matrix_sha256": baseline["matrix_sha256"],
                     "rows": len(baseline["rows"])},
        "candidate": {"tag": tag, "commit": commit, "rows": len(observed)},
        "compatible": not issues,
        "checks": {
            "valid_matrix": valid_matrix,
            "four_versioned_rows": set(observed) == expected_ids,
            "row_identity": not any("incomparable" in change for change in changes)
                            and not any(issue.get("field") == "action" for issue in issues),
            "client_inputs": not any("incomparable" in change and "client_sha256" in change
                                     for change in changes),
            "fixture_identity": not any("incomparable" in change and "fixture" in change
                                        for change in changes),
            "assertions": not any("lost passing assertion" in change for change in changes),
            "payment_amounts": not any("changed wallet accounting for amount" in change for change in changes),
            "wallet_accounting": not any("changed wallet accounting for" in change for change in changes),
            "full_outcomes": not any("outcome" in change or "lost passing assertion" in change
                                     for change in changes),
        },
        "issues": issues,
        "changes": changes,
        "observed": observed_snapshot,
        "rows": [{
            "id": row_id,
            "action": actual.get("action"),
            "status": actual.get("status"),
            "assertions": len(actual.get("assertions", {})),
        } for row_id in ROUNDTRIP_ROW_IDS if (actual := observed.get(row_id))],
    }


def host_runtime():
    packages = []
    for name in ("grpcio", "protobuf", "toml", "mnemonic"):
        try:
            packages.append({"name": name, "version": importlib.metadata.version(name)})
        except importlib.metadata.PackageNotFoundError:
            packages.append({"name": name, "version": None})
    return {"platform": platform.platform(), "python": platform.python_version(), "packages": packages}


def harness_pins():
    sources = [Path(__file__), CAMPAIGN, SCRIPTS / "run-wallet-roundtrip.py",
               SCRIPTS / "regtest-fixture.py", SCRIPTS / "regtest-fixture-relay.py",
               *(WALLET_CLIENTS / name for name in (
                   "audit-wallet-roundtrip.py", "coverage.py", "coverage_evidence.py",
                   "coverage_roundtrip.py", "roundtrip_fixture.py", "roundtrip_wallets.py", "wallet_fixture.py"))]
    return {repository_path(path): digest(path) for path in sources}


def sanitized_error(error, state):
    text = " ".join(str(error).split())
    for path, token in ((state, "<state>"), (ROOT, "<workspace>")):
        if path:
            text = text.replace(str(path), token)
    return text[:600]


def publish_evidence(evidence, summary, gate, comparison, rows_document, campaign_state):
    evidence = Path(evidence).resolve()
    require(not evidence.exists() or not any(evidence.iterdir()),
            "evidence directory must be new or empty")
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    withheld = []
    collected = {}

    def collect(relative, value, checker, label):
        try:
            scan_keys(value, relative)
            checker(value)
        except (TypeError, ValueError):
            withheld.append({"file": relative, "reason": f"{label} did not match the aggregate allowlist"})
            return
        collected[relative] = value

    if campaign_state is not None:
        paired = campaign_state / "paired.json"
        if paired.is_file():
            collect("paired.json", read_json(paired),
                    lambda value: require(set(value) == PAIRED_KEYS and value.get("status") == "passed",
                                          "paired receipt is not a completed Regtest comparison"),
                    "paired receipt")
        for label in ("first", "second"):
            receipt = campaign_state / label / "receipt.json"
            if receipt.is_file():
                collect(f"{label}/receipt.json", read_json(receipt),
                        lambda value: require(set(value) == RECEIPT_KEYS and value.get("status") == "passed",
                                              "receipt is not a completed aggregate wallet receipt"),
                        "wallet receipt")
        audit = campaign_state / "canonical-audit.json"
        if audit.is_file():
            collect("canonical-audit.json", read_json(audit), check_audit, "canonical audit")
        cleanup = campaign_state / "cleanup.json"
        if cleanup.is_file():
            collect("cleanup.json", read_json(cleanup),
                    lambda value: require(set(value) == CLEANUP_KEYS, "cleanup report has an unexpected shape"),
                    "cleanup report")
    summary.setdefault("safety", {})["withheld"] = withheld
    if summary.get("status") == "passed":
        required = {"paired.json", "canonical-audit.json", "cleanup.json",
                    "first/receipt.json", "second/receipt.json"}
        absent = sorted(required - set(collected))
        if withheld or absent:
            summary["status"] = "failed"
            summary["error"] = "allowlisted aggregate evidence is incomplete"
            summary["safety"]["missing"] = absent
    written = []

    def emit(relative, value):
        scan_keys(value, relative)
        target = evidence / relative
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        write_json(target, value)
        written.append(relative)

    emit("summary.json", summary)
    emit("gate.json", gate)
    if comparison is not None:
        emit("comparison.json", comparison)
    if rows_document is not None:
        emit("rows.json", rows_document)
    for relative in sorted(collected):
        emit(relative, collected[relative])
    require(set(written) <= set(EVIDENCE_ALLOWLIST), "evidence writer produced a file outside the allowlist")
    manifest = {
        "schema_version": 1,
        "allowlist": list(EVIDENCE_ALLOWLIST),
        "files": {relative: digest(evidence / relative) for relative in sorted(written)},
    }
    write_json(evidence / "MANIFEST.json", manifest)
    present = {str(path.relative_to(evidence)) for path in evidence.rglob("*") if path.is_file()}
    require(present <= set(EVIDENCE_ALLOWLIST) | {"MANIFEST.json"},
            "evidence directory contains a file outside the allowlist")
    return manifest


def check_audit(audit):
    require(set(audit) == AUDIT_KEYS and audit.get("passed") is True,
            "canonical audit has an unexpected shape")
    require((audit.get("independent_chains"), audit.get("distinct_wallet_identities"),
             audit.get("independent_transactions")) == (2, 4, 4),
            "canonical audit does not prove two independent chains and four transfers")
    require(set(audit.get("runs", {})) == {"first", "second"}, "canonical audit lacks both runs")
    for run in audit["runs"].values():
        require(isinstance(run, dict) and set(run) == AUDIT_RUN_KEYS,
                "canonical audit run has an unexpected shape")
        require(set(run["canonical_txid_fingerprints"]) == {"outbound", "return"},
                "canonical audit does not fingerprint both payment legs")


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-archive", type=Path, required=True,
                        help="release candidate archive built from --tag and --commit")
    parser.add_argument("--archive-sha256", help="optional pinned SHA-256 of the candidate archive")
    parser.add_argument("--tag", required=True, help="release tag the candidate names")
    parser.add_argument("--commit", required=True, help="full release commit the candidate names")
    parser.add_argument("--client-bundle", type=Path, required=True,
                        help="operator-provisioned pinned wallet client bundle")
    parser.add_argument("--client-bundle-sha256", required=True,
                        help="pinned SHA-256 of the wallet client bundle")
    parser.add_argument("--images", type=Path, required=True,
                        help="JSON map of the four immutable runtime image shells")
    parser.add_argument("--baseline-matrix", type=Path, required=True,
                        help="versioned coverage matrix holding the four round-trip rows")
    parser.add_argument("--state", type=Path, required=True,
                        help="new private directory for the campaign and its raw state")
    parser.add_argument("--evidence", type=Path, required=True,
                        help="new directory for allowlisted aggregate evidence")
    parser.add_argument("--port-bases", type=int, nargs=2, default=list(DEFAULT_PORTS),
                        metavar=("FIRST", "SECOND"))
    options = parser.parse_args()

    def interrupt(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    state = Path(options.state).resolve()
    summary = {
        "schema_version": 1,
        "status": "failed",
        "stage": "preflight",
        "candidate": {"tag": options.tag, "commit": options.commit},
        "safety": {"allowlist": list(EVIDENCE_ALLOWLIST)},
        "host_runtime": host_runtime(),
        "harness_sha256": harness_pins(),
    }
    gate = {"schema_version": 1, "status": "blocked", "reason": "preflight did not complete"}
    comparison = rows_document = campaign_state = None
    code = 1
    try:
        candidate, clients, baseline, gate = preflight(options, _workdir(options))
        summary["candidate"] = {key: candidate[key] for key in (
            "tag", "commit", "version", "archive_sha256", "archive_pin", "binaries")}
        summary["gate"] = gate
        summary["stage"] = "campaign"
        campaign_state, attempts = run_campaign(options, candidate, clients)
        summary["campaign"] = {"fixtures": 2, "independent_chains": True, "fixtures_removed": True,
                               "attempts": attempts}
        summary["stage"] = "comparison"
        binding = bind_campaign_evidence(campaign_state, candidate, clients, options.commit)
        rows = project_rows(campaign_state, options.tag, options.commit)
        comparison = compare_rows(rows, baseline, options.tag, options.commit)
        rows_document = {"schema_version": 1, "candidate": {"tag": options.tag, "commit": options.commit},
                         "binding": binding, "rows": rows}
        summary["comparison"] = {
            "compatible": comparison["compatible"],
            "issues": len(comparison["issues"]),
            "changes": len(comparison["changes"]),
            "rows": len(comparison["rows"]),
            "baseline_tag": comparison["baseline"]["tag"],
        }
        summary["binding"] = {"candidate_binaries": True, "client_bundle": True}
        require(comparison["compatible"], "candidate evidence differs from the four versioned rows")
        summary["status"] = "passed"
        summary["stage"] = "evidence"
        code = 0
    except GateBlocked as error:
        summary["status"] = "blocked"
        summary["error"] = sanitized_error(error, state)
        gate = {"schema_version": 1, "status": "blocked", "reason": summary["error"]}
        code = 2
    except (OSError, RuntimeError, ValueError) as error:
        if isinstance(error, CampaignFailed):
            campaign_state = error.campaign_state
            summary["campaign"] = {"fixtures": 2, "independent_chains": False, "attempts": error.attempts}
        summary["error"] = sanitized_error(error, state)
        code = 1
    finally:
        try:
            manifest = publish_evidence(options.evidence, summary, gate, comparison, rows_document,
                                        campaign_state)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            print(f"evidence publication failed: {sanitized_error(error, state)}", file=sys.stderr)
            if summary["status"] == "passed":
                summary["status"] = "failed"
                summary["error"] = "allowlisted evidence publication failed"
                code = 1
        else:
            print(f"published {len(manifest['files'])} allowlisted aggregate files")
            if summary["status"] == "blocked":
                code = 2
            elif summary["status"] != "passed":
                code = 1
    result = summary["status"]
    print(f"release wallet round trip gate: {result}. Evidence: {options.evidence}")
    if result != "passed":
        print(f"reason: {summary.get('error', 'see evidence summary')}", file=sys.stderr)
    sys.exit(code)


def _workdir(options):
    # Candidate and client bundles are extracted under the private state directory.
    work = Path(options.state).resolve() / "inputs"
    if work.exists():
        raise ValueError("state directory already exists; choose a fresh private directory")
    work.mkdir(parents=True, mode=0o700)
    return work


if __name__ == "__main__":
    main()
