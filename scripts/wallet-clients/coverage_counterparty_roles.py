#!/usr/bin/env python3
"""Import one engine's counterparty payment-role receipt into coverage rows.

`payment-roles.py` writes one private receipt per selected engine. Each receipt
covers three signed Regtest transfers: the engine pays an independently keyed
Zingo wallet, a second funded Zingo wallet pays the engine, and the engine
spends the received funds back. This module yields the engine's and the Zingo
counterparty's own observed behaviour separately for every transfer and actor.

A row claims only what the receipt recorded for that actor and leg:

- outbound, engine actor: payer fee, debit, sent-history row, balance;
- outbound, Zingo actor: exact payee amount, decrypted memo, receive history;
- inbound, engine actor: exact payee amount, memo, balance, receive history;
- inbound, Zingo actor: payer fee, balance, pending, sent history;
- spendback, engine actor: payer fee, debit, sent-history row, balance;
- spendback, Zingo actor: exact payee amount, decrypted memo, receive history.

Zally has no public sent-transaction list and no fee accessor. Its receipt must
therefore record `null` for the engine payer's sent history and must label the
fee as derived from the persisted balance delta. A sent row or a wallet-exposed
fee for Zally is rejected rather than imported. The derived fee is not copied
into `accounting_zat.fee`; the row says `payer-fee-residual-only` and
`payer-fee-reconciled` stays unexercised.

Publication gates are evaluated for the receipt. A row can pass only when the
receipt binds an immutable readiness record, the selected release, the release
binaries and client inputs before and after the run, the client source
documents, a measured teardown, and the live server identity that the engine
used. A missing gate emits a documented `unexercised` row; a contradictory
receipt raises. Provisional receipts that predate the portable pins, teardown,
or endpoint evidence are imported as unexercised by default and rejected by
`allow_provisional=False`.

For zcash-devtool the live server gate requires the two-phase probe record: the
before-admission and after-use GetLightdInfo revisions and readiness anchor
blocks, the wallet's own get-info URI and chain tip for each role, every online
command's connect trace to the wallet endpoint, and the three endpoint negative
controls. A configured-only or partial record stays unexercised; a record whose
measurements contradict each other raises.

The producer records these fields, which this importer consumes:

    provenance.engine        exact engine build identity, label and digests
    provenance.zingo         Zingo binary version, source commit and digest
    endpoint_evidence        live server identity and anchor evidence
    engine_pays.pending      node mempool and both wallets before mining
    engine_pays.reopen       reopened sent/receive rows and balance check
    engine_receives.pending  node mempool and both wallets for the inbound leg
    engine_receives.reopen   reopened rows for the inbound and return legs
    engine_receives.received_note, received_note_spent, unspent_after
    engine_pays.leg_observations.outbound
    engine_receives.leg_observations.inbound / .return
    cleanup.verified         measured removal and remaining project resources
    pins.client_inputs_sha256 and client_inputs_sha256_after

A verified version requires a registered executable digest and matching source
inputs, or a Zally Cargo build record binding those inputs to the output binary.
Strict collection checks this evidence independently for each wallet.

The producer records each leg's balances with the phase that measured them:
`engine_pays.leg_observations.outbound`, and
`engine_receives.leg_observations.inbound` / `.return`. Every nested block needs
`engine_balance_after_zat`, `zingo_balance_after_zat`,
`observed_after_confirmations`, `engine_height`, and `zingo_height`. Both wallet
heights must equal the inclusion height plus nine. The inbound block also needs
`before_return: true`. Nested balances must agree with the flat balances the
receipt records; a conflict is rejected. The `outbound` and `return` blocks add
the confirmation-phase assertion. Without them the flat balances remain usable
but that assertion stays unexercised.

Without `engine_receives.leg_observations.inbound`, the two inbound actor rows
are excluded with an explicit reason instead of deriving a balance from the
later return. An archival top-level `leg_observations` object remains supported
for receipts produced before the nested fields, including any wallet-pending or
history observation the producer did not record:

    "leg_observations": {
      "<leg>": {
        "node_mempool": bool,
        "engine_pending": bool,
        "zingo_pending": bool,
        "engine_balance_after_zat": int,
        "zingo_balance_after_zat": int,
        "engine_history_entries_after_reopen": int | null,
        "zingo_history_entries_after_reopen": int | null
      }
    }

Such archival balances are used, but `leg-balance-observed-after-confirmations`
stays unexercised because the receipt did not record the phase. If both the
nested and archival sources exist, they must agree.

The importer never exports a transaction ID, address, memo, endpoint, wallet
path, or host path. Only digests, heights, amounts, and counts leave the
receipt.

Run the audit export:

    python3 coverage_counterparty_roles.py --release v0.22.0 \
      --evidence "$evidence_root" \
      --output "$report_dir"
"""
import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

from counterparty_builds import matches_build, matches_local_zally_build
from coverage_evidence import digest, load, require


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
TAG = re.compile(r"v\d+\.\d+\.\d+(?:[-+][\w.-]+)?")
COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
LOOPBACK = re.compile(r"http://127\.0\.0\.1:\d{2,5}")
SERVICES = ("compat-lightwalletd", "ingest", "projector", "query")
IDENTITY_KEYS = {"lightwalletd": "lightwalletd",
                 "native-wallet-query": "native_wallet_query"}
ACTIVATION = ("Overwinter", "Sapling", "Blossom", "Heartwood", "Canopy",
              "NU5", "NU6", "NU6.1", "NU6.2", "NU6.3")
LEGS = ("outbound", "inbound", "spendback")
ACTORS = ("engine", "zingo")
EXPECTED_AMOUNTS = {"engine_pays", "engine_receives", "engine_returns"}
EXPECTED_MEMOS = EXPECTED_AMOUNTS
OBSERVATION_KEYS = {
    "node_mempool", "engine_pending", "zingo_pending",
    "engine_balance_after_zat", "zingo_balance_after_zat",
    "engine_history_entries_after_reopen", "zingo_history_entries_after_reopen",
}
PENDING_KEYS = {"node_mempool", "engine_pending", "zingo_pending"}
BALANCE_KEYS = {"engine_balance_after_zat", "zingo_balance_after_zat"}
HISTORY_KEYS = {"engine_history_entries_after_reopen",
                "zingo_history_entries_after_reopen"}
NESTED_OBSERVATION_KEYS = {
    "outbound": {"engine_balance_after_zat", "zingo_balance_after_zat",
                 "observed_after_confirmations"},
    "inbound": {"engine_balance_after_zat", "zingo_balance_after_zat",
                "observed_after_confirmations", "before_return"},
    "return": {"engine_balance_after_zat", "zingo_balance_after_zat",
               "observed_after_confirmations"},
}
REQUIRED_PINS = ("release", "release_commit", "fixture_ready_sha256", "node_image",
                 "activation_schedule_sha256", "engine_binary_sha256", "zingo_sha256",
                 "fixture_project", "fixture_base", "fixture_node_rpc")
DEVTOOL_LIVE_SCOPE = "live-probe-before-and-after"
DEVTOOL_ROLES = ("engine-pays", "engine-receives")
DEVTOOL_WALLET_PHASES = {
    "engine-pays": ("before_admission", "after_use"),
    "engine-receives": ("after_use",),
}
DEVTOOL_NEGATIVES = {
    "negative-stale-revision": "revision",
    "negative-wrong-chain": "network",
    "negative-stale-anchor": "anchor",
}

ENGINES = {
    "devtool": {
        "interface": "lightwalletd", "implementation": "cli",
        "variant": "sapling-cli", "binary": ("zcash-devtool",),
        "driver": None,
    },
    "vizor": {
        "interface": "lightwalletd", "implementation": "engine",
        "variant": "rust-engine", "binary": ("vizor-payment", "engine-binary"),
        "driver": "vizor-payment.rs",
    },
    "zally": {
        "interface": "native-wallet-query", "implementation": "engine",
        "variant": "typed-headless", "binary": ("zally-payment-harness", "engine-binary"),
        "driver": "zally-payment.rs",
    },
}
ZINGO = {
    "wallet": "zingo", "interface": "lightwalletd", "implementation": "engine",
    "variant": "nakednet-cli", "binary": ("zingo-cli", "zingo-binary"),
}
FEE_SOURCES = {
    "vizor": "wallet confirmed network fee",
    "devtool": "wallet transaction fee_paid",
    "zally": "derived from persisted balance delta minus payment amount",
}


def token(value, where):
    require(isinstance(value, str) and SHA256.fullmatch(value), f"{where}: missing sha256")


def commit(value, where):
    require(isinstance(value, str) and COMMIT.fullmatch(value), f"{where}: missing commit")


def fingerprint(txid):
    require(isinstance(txid, str) and SHA256.fullmatch(txid), "invalid transaction id")
    return hashlib.sha256(bytes.fromhex(txid)).hexdigest()


def release_identity(release):
    require(isinstance(release, dict) and set(release) == {"tag", "commit"},
            "release: expected tag and commit")
    require(isinstance(release["tag"], str) and TAG.fullmatch(release["tag"]),
            "release: invalid tag")
    commit(release["commit"], "release")


def selected_release(tag, revision=None):
    require(isinstance(tag, str) and TAG.fullmatch(tag), "release: invalid tag")
    require(revision is None or (isinstance(revision, str) and COMMIT.fullmatch(revision)),
            "release: invalid commit")
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"],
        capture_output=True, text=True)
    require(completed.returncode == 0, f"release: missing local tag {tag}")
    resolved = completed.stdout.strip()
    commit(resolved, "release")
    require(revision is None or revision == resolved, "release: tag and commit disagree")
    return {"tag": tag, "commit": resolved}


def gate(status, reason=None):
    return {"status": status, "reason": reason}


def loopback(value, where):
    require(isinstance(value, str) and LOOPBACK.fullmatch(value), f"{where}: invalid endpoint")


def local_sources(engine):
    """Names and local copies of the client sources the producer pins."""
    documents = [("payment-roles.py", HERE / "payment-roles.py"),
                 ("counterparty_builds.py", HERE / "counterparty_builds.py"),
                 (f"{engine}-payment.py", HERE / f"{engine}-payment.py"),
                 ("zingo-payment.py", HERE / "zingo-payment.py"),
                 ("roundtrip_wallets.py", HERE / "roundtrip_wallets.py"),
                 ("wallet_fixture.py", HERE / "wallet_fixture.py"),
                 ("regtest-fixture.py", ROOT / "scripts/regtest-fixture.py")]
    driver = ENGINES[engine]["driver"]
    if driver:
        documents.append((driver, HERE / driver))
    if engine in {"devtool", "zally"}:
        documents.append(("Cargo.lock", None))
    if engine == "devtool":
        documents.append(("lightwalletd-identity.py", HERE / "lightwalletd-identity.py"))
        documents.append(("trace-wallet-endpoint.py",
                          ROOT / "scripts/trace-wallet-endpoint.py"))
    if engine == "zally":
        documents.append(("native-anchor.py", HERE / "native-anchor.py"))
    return documents


def find_pin(inputs, names):
    for key, value in inputs.items():
        require(isinstance(key, str) and key, "client inputs: invalid pin name")
        token(value, "client inputs")
        if Path(key).name in names or key in names:
            return key, value
    return None


def pin_value(inputs, names):
    found = find_pin(inputs, names) if inputs is not None else None
    return None if found is None else found[1]


def basic(result, engine):
    require(result.get("schema_version") == 1, f"{engine}: unsupported receipt schema")
    require(result.get("status") == "passed", f"{engine}: receipt did not pass")
    pins = result.get("pins")
    require(isinstance(pins, dict), f"{engine}: missing pins")
    missing = [key for key in REQUIRED_PINS if key not in pins]
    require(not missing, f"{engine}: missing receipt pin " + ", ".join(missing))
    commit(pins["release_commit"], f"{engine} release")
    return pins


def readiness(path, pins, engine, release):
    """Check the receipt against the immutable readiness record it names."""
    token(pins.get("fixture_ready_sha256"), f"{engine} readiness pin")
    project = pins.get("fixture_project")
    require(isinstance(project, str) and project and "/" not in project and ".." not in project,
            f"{engine}: invalid fixture project")
    ready_path = Path(path).parent / "fixtures" / project / "ready.initial.json"
    require(ready_path.is_file(), f"{engine}: immutable readiness record is missing")
    require(digest(ready_path) == pins["fixture_ready_sha256"],
            f"{engine}: readiness digest differs from the receipt pin")
    ready = load(ready_path)
    require(ready.get("schema_version") == 1, f"{engine}: unsupported readiness schema")
    require(ready.get("zinder_commit") == pins.get("release_commit") == release["commit"],
            f"{engine}: readiness commit differs from the selected release")
    require(pins.get("release") == release["tag"], f"{engine}: receipt release differs")
    require(ready.get("network") == "zcash-regtest", f"{engine}: fixture is not Regtest")
    require(ready.get("project") == project, f"{engine}: readiness project differs")
    require(type(ready.get("port_base")) is int and ready["port_base"] > 1024,
            f"{engine}: invalid fixture port base")
    require(pins.get("fixture_base") == ready["port_base"], f"{engine}: fixture base differs")
    loopback(ready.get("node_rpc"), f"{engine} node")
    require(pins.get("fixture_node_rpc") == ready["node_rpc"], f"{engine}: node endpoint differs")
    require(type(ready.get("current_tip")) is int and ready["current_tip"] > 0,
            f"{engine}: missing readiness anchor height")
    token(ready.get("current_hash"), f"{engine} anchor")
    token(ready.get("activation_schedule_sha256"), f"{engine} schedule")
    require(pins.get("activation_schedule_sha256") == ready["activation_schedule_sha256"],
            f"{engine}: activation schedule differs")
    require(IMAGE.fullmatch(pins.get("node_image", "")), f"{engine}: invalid node image pin")
    require(ready.get("node_image") == pins["node_image"], f"{engine}: node image differs")
    require(set(ready.get("images", {})) == set(SERVICES), f"{engine}: incomplete image ids")
    for value in ready["images"].values():
        require(IMAGE.fullmatch(value), f"{engine}: invalid image id")
    require(set(ready.get("image_references", {})) == set(SERVICES),
            f"{engine}: incomplete image references")
    version = release["tag"].lstrip("v")
    for value in ready["image_references"].values():
        require(isinstance(value, str) and value.rsplit(":", 1)[-1] == version,
                f"{engine}: image reference does not select the release")
    token(ready.get("image_manifest_sha256"), f"{engine} image manifest")
    binaries = ready.get("binaries")
    require(isinstance(binaries, dict) and set(binaries) == set(SERVICES),
            f"{engine}: incomplete release binaries")
    for name, entry in binaries.items():
        require(isinstance(entry, dict) and isinstance(entry.get("source"), str)
                and entry["source"], f"{engine}: missing {name} source")
        token(entry.get("sha256"), f"{engine} {name} binary")
    require(set(ready.get("activation_heights", {})) == set(ACTIVATION),
            f"{engine}: incomplete activation schedule")
    require(all(type(value) is int and value > 0
                for value in ready["activation_heights"].values()),
            f"{engine}: invalid activation height")
    isolation = ready.get("isolation")
    require(isinstance(isolation, dict) and isolation.get("peer_count") == 0
            and isolation.get("default_ipv4_route") is False,
            f"{engine}: fixture is not peer-isolated")
    health = ready.get("health")
    require(isinstance(health, dict) and set(health) == set(SERVICES),
            f"{engine}: incomplete service health")
    for name, entry in health.items():
        require(entry.get("git_commit") == release["commit"]
                and entry.get("network") == "zcash-regtest"
                and entry.get("version") == version, f"{engine}: {name} identity differs")
    state = ready.get("readiness")
    require(isinstance(state, dict) and set(state) == set(SERVICES),
            f"{engine}: incomplete readiness state")
    for name, entry in state.items():
        require(entry.get("status") == "ready"
                and type(entry.get("current_height")) is int
                and type(entry.get("target_height")) is int
                and entry["current_height"] >= entry["target_height"],
                f"{engine}: {name} was not ready")
    require(isinstance(ready.get("raw_blob_policy"), str) and ready["raw_blob_policy"],
            f"{engine}: missing raw blob policy")
    return ready_path, ready


def pin_stability(result, engine, pins):
    """Check the portable pre-run and post-run client input pins."""
    inputs = pins.get("client_inputs_sha256")
    after = result.get("client_inputs_sha256_after")
    stable = result.get("inputs_stable")
    if inputs is None:
        return gate("unexercised", "receipt records no portable client input pins"), None
    require(isinstance(inputs, dict) and inputs, f"{engine}: invalid client input pins")
    for key, value in inputs.items():
        require(isinstance(key, str) and key, f"{engine}: invalid client input name")
        token(value, f"{engine} client input")
    if after is None:
        return gate("unexercised", "receipt records no post-run client input pins"), inputs
    require(isinstance(after, dict) and set(after) == set(inputs),
            f"{engine}: post-run client inputs differ from the pre-run pins")
    for key, value in after.items():
        token(value, f"{engine} post-run client input")
    require(stable is not False, f"{engine}: the receipt invalidated its client inputs")
    require(after == inputs, f"{engine}: post-run client inputs differ from the pre-run pins")
    if stable is not True:
        return gate("unexercised", "receipt does not assert stable client inputs"), inputs
    return gate("pass"), inputs


def source_gate(engine, pins, inputs):
    if inputs is None:
        return gate("unexercised", "receipt records no portable client input pins")
    expected = local_sources(engine)
    missing = [name for name, _ in expected if find_pin(inputs, (name,)) is None]
    if missing:
        return gate("unexercised", "pinned source set is missing " + ", ".join(missing))
    for name, local in expected:
        pinned = find_pin(inputs, (name,))[1]
        if local is None:
            continue
        if not local.is_file():
            return gate("unexercised", f"pinned {name} is unavailable for verification")
        if digest(local) != pinned:
            return gate("unexercised", f"pinned {name} differs from the current source")
    pinned = find_pin(inputs, ENGINES[engine]["binary"])
    if pinned is None:
        return gate("unexercised", "pinned input set is missing the engine binary")
    if pinned[1] != pins["engine_binary_sha256"]:
        raise ValueError(f"{engine}: engine binary pin differs from the receipt")
    zingo = find_pin(inputs, ZINGO["binary"])
    if zingo is None:
        return gate("unexercised", "pinned input set is missing the Zingo binary")
    if zingo[1] != pins["zingo_sha256"]:
        raise ValueError(f"{engine}: Zingo binary pin differs from the receipt")
    return gate("pass")


def binary_gate(engine, ready, inputs):
    if inputs is None:
        return gate("unexercised", "receipt records no portable client input pins")
    for name in SERVICES:
        pinned = find_pin(inputs, (f"zinder-{name}", name))
        if pinned is None:
            return gate("unexercised", f"pinned input set is missing release binary {name}")
        if pinned[1] != ready["binaries"][name]["sha256"]:
            raise ValueError(f"{engine}: release binary pin for {name} differs from readiness")
    return gate("pass")


def teardown_gate(result, engine):
    cleanup = result.get("cleanup")
    if not isinstance(cleanup, dict):
        return gate("unexercised", "receipt records no teardown")
    if cleanup.get("kept_by_request") is True:
        return gate("unexercised", "fixture was kept by request")
    if cleanup.get("reused_fixture") is True:
        return gate("unexercised", "fixture was reused and not removed by this run")
    if cleanup.get("fixture_removed") is not True:
        return gate("unexercised", "fixture removal is not recorded")
    if cleanup.get("verified") is not True:
        return gate("unexercised", "teardown verification is not measured")
    remaining = cleanup.get("remaining_resources")
    if not isinstance(remaining, dict) or set(remaining) != {"container", "network", "volume"}:
        return gate("unexercised", "remaining fixture resources are not measured")
    if not all(type(value) is int and value >= 0 for value in remaining.values()):
        return gate("unexercised", "remaining fixture resources are not measured")
    if cleanup.get("error"):
        return gate("unexercised", "teardown recorded an error")
    require(all(value == 0 for value in remaining.values()),
            f"{engine}: fixture resources remain after verified teardown")
    return gate("pass")


def structured_identity(block, ready, engine, interface):
    """Check a producer server-info record against the immutable anchor."""
    require(isinstance(block, dict), f"{engine}: invalid {interface} server identity")
    require(block.get("ok") is True, f"{engine}: {interface} identity probe did not pass")
    identity_commit = block.get("build_git_commit", block.get("git_commit"))
    require(identity_commit == block.get("expected_revision", identity_commit),
            f"{engine}: {interface} identity reports contradictory revisions")
    require(identity_commit == ready["zinder_commit"],
            f"{engine}: {interface} commit differs from the selected release")
    require(block.get("revision_verified", True) is True,
            f"{engine}: {interface} revision is not verified")
    network = block.get("network", block.get("chain_name"))
    require(network in {"zcash-regtest", "test"},
            f"{engine}: {interface} network differs from the fixture")
    require(block.get("network_verified", True) is True,
            f"{engine}: {interface} network is not verified")
    require(block.get("anchor_verified") is True,
            f"{engine}: {interface} anchor is not verified")
    require(block.get("anchor_height") == block.get("expected_height", block.get("anchor_height")),
            f"{engine}: {interface} anchor height is contradictory")
    require(block.get("anchor_hash") == block.get("expected_hash", block.get("anchor_hash")),
            f"{engine}: {interface} anchor hash is contradictory")
    require(block.get("anchor_height") == ready["current_tip"]
            and block.get("anchor_hash") == ready["current_hash"],
            f"{engine}: {interface} anchor differs from the immutable readiness record")


def devtool_probe(block, ready, engine, phase):
    """Check one live lightwalletd probe against the readiness anchor."""
    require(isinstance(block, dict), f"{engine}: invalid {phase} lightwalletd probe")
    require(block.get("scope") == "live-probe",
            f"{engine}: {phase} record is not a live probe")
    require(block.get("phase") == phase,
            f"{engine}: {phase} record reports another phase")
    require(block.get("command_exit") == 0 and block.get("ok") is True,
            f"{engine}: {phase} probe did not pass")
    structured_identity(block, ready, engine, "lightwalletd")
    require(block.get("endpoint") == ready["lightwalletd"],
            f"{engine}: {phase} probe dialled another endpoint")
    require(block.get("chain_name") == "test"
            and block.get("expected_chain") == "test",
            f"{engine}: {phase} probe did not check the fixture chain")
    require(block.get("expected_revision") == ready["zinder_commit"],
            f"{engine}: {phase} probe did not check the selected revision")
    require(block.get("expected_height") == ready["current_tip"]
            and block.get("expected_hash") == ready["current_hash"],
            f"{engine}: {phase} probe did not check the readiness anchor")
    height = block.get("served_height")
    require(type(height) is int and height >= ready["current_tip"],
            f"{engine}: {phase} probe served an invalid height")
    require(block.get("served_tip_height") == height
            and block.get("tip_verified") is True,
            f"{engine}: {phase} probe saw a lagging tip")
    provenance = block.get("provenance")
    require(isinstance(provenance, dict)
            and provenance.get("payloads_recorded") is False,
            f"{engine}: {phase} probe claims payload capture")
    require(isinstance(provenance.get("transport"), str)
            and "GetLightdInfo" in provenance["transport"],
            f"{engine}: {phase} probe did not record the identity transport")


def devtool_controls(evidence, ready, engine, endpoint):
    """Check the live comparisons can fail for the intended reason only."""
    controls = evidence.get("negative_controls")
    if controls is None:
        return gate("unexercised", "receipt records no endpoint negative controls")
    require(isinstance(controls, list), f"{engine}: invalid endpoint negative controls")
    require(len(controls) == len(DEVTOOL_NEGATIVES),
            f"{engine}: endpoint negative controls are incomplete")
    found = {}
    for control in controls:
        require(isinstance(control, dict), f"{engine}: invalid endpoint negative control")
        phase = control.get("phase")
        require(phase in DEVTOOL_NEGATIVES,
                f"{engine}: unknown endpoint negative control")
        require(phase not in found, f"{engine}: duplicate endpoint negative control")
        found[phase] = control
    require(set(found) == set(DEVTOOL_NEGATIVES),
            f"{engine}: endpoint negative controls are incomplete")
    for phase, intended in DEVTOOL_NEGATIVES.items():
        control = found[phase]
        require(control.get("command_exit") == 1 and control.get("ok") is False,
                f"{engine}: {phase} did not reject the endpoint")
        flags = {name: control.get(f"{name}_verified")
                 for name in ("revision", "network", "anchor")}
        require(all(isinstance(value, bool) for value in flags.values()),
                f"{engine}: {phase} did not record its comparisons")
        require([name for name, value in flags.items() if not value] == [intended],
                f"{engine}: {phase} rejected more than the intended comparison")
        require(control.get("endpoint") == endpoint,
                f"{engine}: {phase} dialled another endpoint")
        require(control.get("build_git_commit") == ready["zinder_commit"],
                f"{engine}: {phase} saw another build")
        require(control.get("chain_name") == "test",
                f"{engine}: {phase} saw another chain")
        require(control.get("tip_verified") is True,
                f"{engine}: {phase} saw a lagging tip")
        if intended == "revision":
            commit(control.get("expected_revision"), f"{engine} {phase}")
            require(control.get("expected_revision") != ready["zinder_commit"],
                    f"{engine}: {phase} expected the selected revision")
        else:
            require(control.get("expected_revision") == ready["zinder_commit"],
                    f"{engine}: {phase} expected another revision")
        if intended == "network":
            require(control.get("expected_chain") != "test",
                    f"{engine}: {phase} expected the fixture chain")
        else:
            require(control.get("expected_chain") == "test",
                    f"{engine}: {phase} expected another chain")
        require(control.get("expected_height") == ready["current_tip"]
                and control.get("expected_hash") == ready["current_hash"],
                f"{engine}: {phase} expected another anchor")
        if intended == "anchor":
            require((control.get("anchor_height"), control.get("anchor_hash"))
                    != (ready["current_tip"], ready["current_hash"]),
                    f"{engine}: {phase} accepted the readiness anchor")
        else:
            require(control.get("anchor_height") == ready["current_tip"]
                    and control.get("anchor_hash") == ready["current_hash"],
                    f"{engine}: {phase} saw another anchor")
    return gate("pass")


def devtool_probe_gate(result, evidence, engine):
    """Bind the out-of-band identity probe to a pinned client input."""
    provenance = evidence.get("provenance")
    if provenance is None:
        return gate("unexercised", "receipt records no endpoint probe provenance")
    require(isinstance(provenance, dict), f"{engine}: invalid endpoint probe provenance")
    probe = provenance.get("probe")
    require(isinstance(probe, str)
            and Path(probe).name == "lightwalletd-identity.py",
            f"{engine}: endpoint probe is not the identity probe")
    token(provenance.get("probe_sha256"), f"{engine} endpoint probe")
    inputs = result.get("pins", {}).get("client_inputs_sha256")
    pinned = find_pin(inputs, ("lightwalletd-identity.py",)) if inputs else None
    if pinned is None:
        return gate("unexercised", "receipt records no pinned endpoint probe")
    require(pinned[1] == provenance["probe_sha256"],
            f"{engine}: endpoint probe digest differs from the pinned input")
    return gate("pass")


def devtool_server_gate(result, evidence, ready, engine):
    """Check the two-phase live lightwalletd identity and wallet traces."""
    scope = evidence.get("scope")
    block = evidence.get("lightwalletd_server_info")
    if block is None:
        if scope == "configured":
            return gate("unexercised", "receipt records only a configured endpoint")
        return gate("unexercised", "receipt records no live server identity")
    require(scope == DEVTOOL_LIVE_SCOPE,
            f"{engine}: live identity is not a before-and-after probe")
    require(evidence.get("anchor_verified") is True,
            f"{engine}: endpoint evidence does not assert the anchor")
    observations = evidence.get("observations")
    if not isinstance(observations, dict):
        return gate("unexercised",
                    "receipt records no endpoint before and after observations")
    require(set(observations) == {"before_admission", "after_use"},
            f"{engine}: endpoint observations are incomplete")
    before, after = observations["before_admission"], observations["after_use"]
    devtool_probe(before, ready, engine, "before-admission")
    devtool_probe(after, ready, engine, "after-use")
    require(after["served_height"] >= before["served_height"],
            f"{engine}: lightwalletd served height regressed between probes")
    for key in ("build_git_commit", "chain_name", "anchor_height", "anchor_hash",
                "expected_revision", "expected_chain", "expected_height",
                "expected_hash", "endpoint"):
        require(before.get(key) == after.get(key),
                f"{engine}: lightwalletd identity changed between probes")
    for key in ("build_git_commit", "chain_name", "anchor_height", "anchor_hash",
                "expected_revision", "served_height", "served_tip_height", "phase", "ok"):
        require(block.get(key) == after.get(key),
                f"{engine}: live identity contradicts the after-use probe")
    devtool_probe(block, ready, engine, "after-use")

    endpoint = evidence.get("wallet_endpoint")
    if endpoint is None:
        return gate("unexercised", "receipt records no wallet-observed endpoint")
    loopback(endpoint, f"{engine} wallet endpoint")
    require(endpoint == ready["lightwalletd"] and after["endpoint"] == endpoint,
            f"{engine}: wallet endpoint differs from the readiness endpoint")
    port = int(endpoint.rsplit(":", 1)[1])

    info = evidence.get("wallet_server_info")
    if info is None:
        return gate("unexercised", "receipt records no wallet-observed server identity")
    require(isinstance(info, dict) and set(info) == set(DEVTOOL_ROLES),
            f"{engine}: wallet-observed server identity is incomplete")
    probes = {"before_admission": before, "after_use": after}
    for role, phases in DEVTOOL_WALLET_PHASES.items():
        entry = info.get(role)
        require(isinstance(entry, dict) and set(entry) == set(phases),
                f"{engine}: {role} wallet server identity is incomplete")
        for phase in phases:
            observed = entry[phase]
            require(isinstance(observed, dict),
                    f"{engine}: invalid wallet server identity")
            require(observed.get("server_uri") == endpoint,
                    f"{engine}: {role} {phase} reports another server")
            require(observed.get("chain_name") == "test",
                    f"{engine}: {role} {phase} reports another chain")
            require(type(observed.get("chain_tip_height")) is int
                    and observed["chain_tip_height"] == probes[phase]["served_height"],
                    f"{engine}: {role} {phase} reports another chain tip")
    require(info["engine-pays"]["after_use"]["chain_tip_height"]
            >= info["engine-pays"]["before_admission"]["chain_tip_height"],
            f"{engine}: the payer wallet saw a regressed chain tip")

    commands = evidence.get("wallet_commands")
    if commands is None:
        return gate("unexercised", "receipt records no wallet endpoint trace")
    require(isinstance(commands, dict) and set(commands) == set(DEVTOOL_ROLES),
            f"{engine}: wallet endpoint traces are incomplete")
    for role in DEVTOOL_ROLES:
        trace = commands[role]
        require(isinstance(trace, dict), f"{engine}: invalid wallet endpoint trace")
        require(trace.get("bound") is True,
                f"{engine}: {role} commands are not bound to the wallet endpoint")
        online = trace.get("online_commands")
        traced = trace.get("traced_commands")
        reports = trace.get("reports")
        require(type(online) is int and online > 0,
                f"{engine}: {role} recorded no online commands")
        require(type(traced) is int and traced == online,
                f"{engine}: {role} online commands are not all traced")
        require(isinstance(reports, list) and len(reports) == online,
                f"{engine}: {role} trace reports disagree with the online commands")
        require(trace.get("endpoint") == f"127.0.0.1:{port}",
                f"{engine}: {role} trace names another endpoint")
        for report in reports:
            require(isinstance(report, dict), f"{engine}: invalid trace report")
            require(type(report.get("connections")) is int
                    and report["connections"] >= 1,
                    f"{engine}: {role} command reached no endpoint")
            require(report.get("endpoint") == f"loopback:{port}",
                    f"{engine}: {role} command reached another endpoint")
            token(report.get("connect_log_sha256"), f"{engine} connect log")
            token(report.get("report_sha256"), f"{engine} trace report")
            require(isinstance(report.get("command"), str) and report["command"],
                    f"{engine}: trace report lacks a command")

    controls = devtool_controls(evidence, ready, engine, endpoint)
    if controls["status"] != "pass":
        return controls
    return devtool_probe_gate(result, evidence, engine)


def server_gate(result, ready, interface, engine, release, source_ok):
    """Check the live server identity the engine used for this receipt."""
    evidence = result.get("endpoint_evidence")
    if isinstance(evidence, dict):
        scope = evidence.get("scope")
        require(isinstance(scope, str) and scope, f"{engine}: endpoint evidence lacks a scope")
        if interface == "native-wallet-query":
            block = evidence.get("native_server_info", evidence.get("native"))
            if block is None:
                return gate("unexercised", "receipt records no native server identity")
            structured_identity(block, ready, engine, interface)
            require(evidence.get("anchor_verified") is True,
                    f"{engine}: receipt does not assert the native anchor")
            return gate("pass")
        if engine == "devtool":
            return devtool_server_gate(result, evidence, ready, engine)
        block = evidence.get("lightwalletd_server_info", evidence.get("server_info"))
        if block is not None:
            structured_identity(block, ready, engine, interface)
            return gate("pass")
        if scope == "driver-verified" and engine == "vizor":
            require(evidence.get("anchor_verified") is True,
                    f"{engine}: driver endpoint evidence does not assert the anchor")
            if source_ok != "pass":
                return gate("unexercised", "driver endpoint evidence is not source-verified")
            return gate("pass")
        if scope == "configured":
            return gate("unexercised", "receipt records only a configured endpoint")
        return gate("unexercised", "receipt records no live server identity")
    identity = result.get("server_identity")
    if identity is None:
        return gate("unexercised", "receipt records no live server identity")
    require(isinstance(identity, dict), f"{engine}: invalid live server identity")
    block = identity.get(IDENTITY_KEYS[interface])
    if block is None:
        return gate("unexercised", f"receipt records no {interface} server identity")
    require(isinstance(block, dict), f"{engine}: invalid {interface} server identity")
    if interface == "lightwalletd":
        require(block.get("chain_name") == "test",
                f"{engine}: lightwalletd chain name differs from the fixture")
        require(block.get("git_commit") == release["commit"],
                f"{engine}: lightwalletd commit differs from the selected release")
        require(block.get("anchor_height") == ready["current_tip"]
                and block.get("anchor_hash") == ready["current_hash"],
                f"{engine}: lightwalletd anchor differs from the immutable readiness record")
    else:
        require(block.get("git_commit") == release["commit"],
                f"{engine}: native endpoint commit differs from the selected release")
        require(block.get("network") == "zcash-regtest",
                f"{engine}: native endpoint network differs")
        require(block.get("loopback") is True, f"{engine}: native endpoint is not loopback")
    return gate("pass")


def validate_leg_observations(result, engine):
    value = result.get("leg_observations")
    if value is None:
        return {}
    require(isinstance(value, dict) and set(value) <= set(LEGS),
            f"{engine}: invalid leg observation set")
    for leg, entry in value.items():
        require(isinstance(entry, dict) and set(entry) <= OBSERVATION_KEYS,
                f"{engine}: invalid {leg} observation fields")
        for key, item in entry.items():
            if key in PENDING_KEYS:
                require(isinstance(item, bool), f"{engine}: {leg} {key} is not boolean")
            elif key in BALANCE_KEYS:
                require(item is None or (type(item) is int and item >= 0),
                        f"{engine}: {leg} {key} is not a balance")
            elif key in HISTORY_KEYS:
                require(item is None or (type(item) is int and item >= 0),
                        f"{engine}: {leg} {key} is not a history count")
    return value


def history(value, where):
    if value is None:
        return None
    require(type(value) is int and value == 1, f"{where}: history count is not one")
    return value


def history_status(value):
    return "unexercised" if value is None else "pass"


def pending_row(entry, names):
    if entry is None:
        return "unexercised"
    require(isinstance(entry, dict), "pending observation is not an object")
    for name in names:
        value = entry.get(name)
        if value is None:
            continue
        require(isinstance(value, dict), "pending wallet observation is not an object")
        require(isinstance(value.get("row_observed"), bool),
                "pending wallet row_observed is not boolean")
        if value["row_observed"] is not True:
            continue
        require(isinstance(value.get("pending"), bool),
                "pending wallet observation is not boolean")
        return "pass" if value["pending"] else "unexercised"
    return "unexercised"


def node_pending(entry):
    return "pass" if isinstance(entry, dict) and entry.get("node_mempool") is True \
        else "unexercised"


def observation_height(entry, engine):
    if not isinstance(entry, dict):
        return
    value = entry.get("observed_height")
    if value is None:
        return
    require(type(value) is int and value > 0, f"{engine}: invalid pending observation height")


def memo_status(value, where):
    if value is None:
        return "unexercised"
    require(isinstance(value, bool), f"{where}: memo flag is not boolean")
    return "pass" if value else "unexercised"


def fee_claim(engine, value, amount, where):
    require(isinstance(value, dict), f"{where}: missing payer fee")
    require(type(value.get("amount_zat")) is int and value["amount_zat"] > 0,
            f"{where}: missing payment amount")
    require(value["amount_zat"] == amount, f"{where}: payer fee amount differs from the leg")
    require(type(value.get("fee_zat")) is int and value["fee_zat"] > 0,
            f"{where}: missing payer fee value")
    require(type(value.get("debit_zat")) is int
            and value["debit_zat"] == amount + value["fee_zat"],
            f"{where}: payer debit does not include the amount and fee")
    delta = value.get("ledger_delta_zat")
    require(delta is None or (type(delta) is int and delta == -value["debit_zat"]),
            f"{where}: payer ledger delta does not reconcile the debit")
    source = value.get("fee_source")
    require(source == FEE_SOURCES[engine], f"{where}: wrong fee source for {engine}")
    if "proposal_fee_zat" in value:
        proposal = value["proposal_fee_zat"]
        require(engine == "vizor" and type(proposal) is int and proposal == value["fee_zat"],
                f"{where}: proposal and confirmed fees differ")
    return {"amount": amount, "fee": value["fee_zat"],
            "exposed": engine != "zally", "ledger": delta is not None}


def inclusion(value, where):
    require(isinstance(value, dict), f"{where}: missing inclusion")
    token(value.get("fingerprint_sha256"), f"{where} fingerprint")
    require(type(value.get("mined_height")) is int and value["mined_height"] > 0,
            f"{where}: invalid mined height")
    require(value.get("active_chain_block") is True, f"{where}: transaction left the active chain")
    require(value.get("occurrences_in_block") == 1,
            f"{where}: transaction does not appear exactly once")
    return value


def validated_inclusion(txid, value, where):
    record = inclusion(value, where)
    require(fingerprint(txid) == record["fingerprint_sha256"],
            f"{where}: fingerprint does not bind the signed transaction")
    return record


def funding(value, where):
    require(isinstance(value, dict), f"{where}: missing funding")
    token(value.get("tx_fingerprint_sha256"), f"{where} fingerprint")
    require(type(value.get("mined_height")) is int and value["mined_height"] > 0,
            f"{where}: invalid funding height")
    require(type(value.get("spendable_zat")) is int and value["spendable_zat"] > 0,
            f"{where}: missing funding balance")
    return value


def ledger(result, engine, ready):
    """Check the three signed transfers and each leg's recorded accounting."""
    pins = result["pins"]
    token(pins.get("engine_binary_sha256"), f"{engine} engine binary")
    token(pins.get("zingo_sha256"), f"{engine} Zingo binary")
    amounts = result.get("amounts_zat")
    require(isinstance(amounts, dict) and set(amounts) == EXPECTED_AMOUNTS,
            f"{engine}: incomplete payment amounts")
    require(all(type(value) is int and value > 0 for value in amounts.values()),
            f"{engine}: invalid payment amount")
    memos = result.get("memos")
    require(isinstance(memos, dict) and set(memos) == EXPECTED_MEMOS,
            f"{engine}: incomplete payment memos")
    require(all(isinstance(value, str) and value for value in memos.values()),
            f"{engine}: invalid payment memo")
    require(len(set(memos.values())) == len(memos), f"{engine}: reused payment memo")

    pays = result.get("engine_pays")
    receives = result.get("engine_receives")
    require(isinstance(pays, dict) and isinstance(receives, dict),
            f"{engine}: incomplete role receipt")
    require(pays.get("status") == "passed" and receives.get("status") == "passed",
            f"{engine}: a payment role did not pass")
    require(pays.get("direction") == "engine-pays-zingo"
            and receives.get("direction") == "zingo-pays-engine",
            f"{engine}: payment direction differs")
    require(pays.get("amount_zat") == amounts["engine_pays"],
            f"{engine}: outbound amount differs from the receipt")
    require(receives.get("receive_amount_zat") == amounts["engine_receives"],
            f"{engine}: inbound amount differs from the receipt")
    require(receives.get("return_amount_zat") == amounts["engine_returns"],
            f"{engine}: return amount differs from the receipt")
    require(receives.get("received_funds_spent") is True,
            f"{engine}: the return payment did not follow the received funds")

    out_txid = pays.get("txid")
    in_txid = receives.get("receive_txid")
    back_txid = receives.get("return_txid")
    for txid, where in ((out_txid, "outbound"), (in_txid, "inbound"),
                        (back_txid, "spendback")):
        require(isinstance(txid, str) and SHA256.fullmatch(txid),
                f"{engine}: missing {where} transaction")
    out = validated_inclusion(out_txid, pays.get("node_inclusion"), f"{engine} outbound")
    chain = receives.get("node_inclusion")
    require(isinstance(chain, list) and len(chain) == 2,
            f"{engine}: inbound and return inclusions are incomplete")
    inbound = validated_inclusion(in_txid, chain[0], f"{engine} inbound")
    spendback = validated_inclusion(back_txid, chain[1], f"{engine} spendback")
    require(inbound["mined_height"] < spendback["mined_height"],
            f"{engine}: inbound and return heights are out of order")
    require(len({out["fingerprint_sha256"], inbound["fingerprint_sha256"],
                 spendback["fingerprint_sha256"]}) == 3,
            f"{engine}: a signed transfer is reused across legs")

    engine_funding = funding(result.get("engine_payer_funding"), f"{engine} engine funding")
    zingo_funding = funding(result.get("zingo_payer_funding"), f"{engine} Zingo funding")
    require(engine_funding["mined_height"] < out["mined_height"],
            f"{engine}: outbound payment precedes its funding")
    require(zingo_funding["mined_height"] < inbound["mined_height"],
            f"{engine}: inbound payment precedes its funding")
    require(len({engine_funding["tx_fingerprint_sha256"],
                 zingo_funding["tx_fingerprint_sha256"],
                 out["fingerprint_sha256"], inbound["fingerprint_sha256"],
                 spendback["fingerprint_sha256"]}) == 5,
            f"{engine}: funding or payment fingerprints are reused")

    payer = fee_claim(engine, pays.get("payer_fee"), amounts["engine_pays"],
                      f"{engine} outbound payer")
    returned = fee_claim(engine, receives.get("engine_return_fee"),
                         amounts["engine_returns"], f"{engine} return payer")
    zingo_fee = receives.get("zingo_payer_fee_zat")
    require(type(zingo_fee) is int and zingo_fee > 0,
            f"{engine}: missing Zingo payer fee")
    for key in ("payer_balance_after_zat", "payee_balance_after_zat"):
        require(type(pays.get(key)) is int and pays[key] >= 0,
                f"{engine}: missing outbound balance")
    for key in ("engine_balance_after_zat", "zingo_balance_after_zat"):
        require(type(receives.get(key)) is int and receives[key] >= 0,
                f"{engine}: missing final balance")
    require(pays["payee_balance_after_zat"] >= amounts["engine_pays"],
            f"{engine}: payee balance does not include the outbound payment")
    require(receives["zingo_balance_after_zat"] >= amounts["engine_returns"],
            f"{engine}: returning payee balance does not include the return payment")

    if "fixture_isolation" in result:
        require(result["fixture_isolation"] == ready["isolation"],
                f"{engine}: receipt isolation differs from readiness")
    if "raw_blob_policy" in result:
        require(result["raw_blob_policy"] == ready["raw_blob_policy"],
                f"{engine}: receipt raw blob policy differs from readiness")

    engine_history = pays.get("engine_history_entries_after_reopen")
    if engine == "zally":
        require(engine_history is None,
                f"{engine}: receipt claims an unavailable sent history row")
    elif engine_history is not None:
        history(engine_history, f"{engine} outbound engine")
    zingo_history = pays.get("zingo_history_entries_after_reopen")
    if zingo_history is not None:
        history(zingo_history, f"{engine} outbound Zingo")

    note = receives.get("received_note")
    if note is not None:
        require(isinstance(note, dict), f"{engine}: invalid received note")
        token(note.get("tx_fingerprint_sha256"), f"{engine} received note")
        require(note["tx_fingerprint_sha256"] == inbound["fingerprint_sha256"],
                f"{engine}: received note does not belong to the inbound transfer")
        require(type(note.get("value_zat")) is int
                and note["value_zat"] == amounts["engine_receives"],
                f"{engine}: received note value differs from the payment")
        require(note.get("output_index") is None
                or (type(note.get("output_index")) is int and note["output_index"] >= 0),
                f"{engine}: invalid received note index")
    spent = receives.get("received_note_spent")
    if spent is not None:
        require(spent in {"receive row spent_our_inputs",
                          "received note absent from the unspent set; "
                          "return change row present",
                          "ledger-derived (received row plus return sent row)"},
                f"{engine}: received note spend evidence is missing")
    unspent = receives.get("unspent_after")
    if unspent is not None:
        require(isinstance(unspent, list), f"{engine}: invalid unspent note set")
        for entry in unspent:
            require(isinstance(entry, dict), f"{engine}: invalid unspent note")
            token(entry.get("tx_fingerprint_sha256"), f"{engine} unspent note")
            require(type(entry.get("value_zat")) is int and entry["value_zat"] >= 0,
                    f"{engine}: invalid unspent note value")

    return {
        "amounts": amounts, "memos": memos,
        "outbound": {"amount": amounts["engine_pays"], "fee": payer,
                     "tx": out, "engine_balance": pays["payer_balance_after_zat"],
                     "zingo_balance": pays["payee_balance_after_zat"],
                     "memo_verified": pays.get("memo_verified_by_payee"),
                     "engine_history": engine_history, "zingo_history": zingo_history,
                     "pending": pays.get("pending"), "reopen": pays.get("reopen")},
        "inbound": {"amount": amounts["engine_receives"], "tx": inbound,
                    "zingo_fee": zingo_fee,
                    "memo_verified": receives.get("memo_verified_by_engine"),
                    "pending": receives.get("pending"), "reopen": receives.get("reopen"),
                    "note": note, "note_spent": spent, "unspent": unspent},
        "spendback": {"amount": amounts["engine_returns"], "fee": returned,
                      "tx": spendback,
                      "engine_balance": receives["engine_balance_after_zat"],
                      "zingo_balance": receives["zingo_balance_after_zat"],
                      "memo_verified": receives.get("return_memo_verified_by_zingo")},
    }


def nested_observations(result, engine, facts):
    """Read the producer's per-leg balances with the phase that measured them."""
    normalized = {}
    owners = (
        ("engine_pays", result["engine_pays"], {"outbound"}),
        ("engine_receives", result["engine_receives"], {"inbound", "return"}),
    )
    for owner_name, owner, allowed in owners:
        value = owner.get("leg_observations")
        if value is None:
            continue
        require(isinstance(value, dict) and value,
                f"{engine}: {owner_name} leg observations are empty")
        require(set(value) <= allowed,
                f"{engine}: {owner_name} leg observations contain an unexpected leg")
        for producer_leg, entry in value.items():
            leg = "spendback" if producer_leg == "return" else producer_leg
            require(leg not in normalized,
                    f"{engine}: {producer_leg} leg observation is duplicated")
            require(isinstance(entry, dict),
                    f"{engine}: {producer_leg} leg observation is not an object")
            require(set(entry) <= NESTED_OBSERVATION_KEYS[producer_leg] | {"engine_height", "zingo_height"},
                    f"{engine}: {producer_leg} leg observation has unexpected fields")
            for key in BALANCE_KEYS:
                require(type(entry.get(key)) is int and entry[key] >= 0,
                        f"{engine}: {producer_leg} {key} is not a balance")
            confirmations = entry.get("observed_after_confirmations")
            require(type(confirmations) is int and confirmations == 10,
                    f"{engine}: {producer_leg} observation lacks its confirmation phase")
            expected_height = facts[leg]["tx"]["mined_height"] + confirmations - 1
            for key in ("engine_height", "zingo_height"):
                require(type(entry.get(key)) is int and entry[key] == expected_height,
                        f"{engine}: {leg} {key} does not match the ten-confirmation height")
            if producer_leg == "inbound":
                require(entry.get("before_return") is True,
                        f"{engine}: inbound balance observation is not before the return")
                phase = {"after_confirmations": confirmations, "before_return": True}
            else:
                require("before_return" not in entry,
                        f"{engine}: {producer_leg} balance observation has an ambiguous phase")
                phase = {"after_confirmations": confirmations}
            normalized[leg] = {
                "engine_balance": entry["engine_balance_after_zat"],
                "zingo_balance": entry["zingo_balance_after_zat"],
                "phase": phase,
            }
    return normalized


def balance_observations(result, engine, facts):
    """Normalize per-leg history, pending, and balance observations."""
    raw = validate_leg_observations(result, engine)
    nested = nested_observations(result, engine, facts)
    out, inbound, back = facts["outbound"], facts["inbound"], facts["spendback"]
    out_obs, in_obs = raw.get("outbound", {}), raw.get("inbound", {})
    back_obs = raw.get("spendback", {})

    def merged(leg, flat_engine, flat_zingo):
        entry = raw.get(leg, {})
        raw_engine = entry.get("engine_balance_after_zat")
        raw_zingo = entry.get("zingo_balance_after_zat")
        found = nested.get(leg)
        if found is not None:
            engine_value, zingo_value = found["engine_balance"], found["zingo_balance"]
            if raw_engine is not None:
                require(raw_engine == engine_value,
                        f"{engine}: {leg} engine balance conflicts with the nested observation")
            if raw_zingo is not None:
                require(raw_zingo == zingo_value,
                        f"{engine}: {leg} Zingo balance conflicts with the nested observation")
            if flat_engine is not None:
                require(flat_engine == engine_value,
                        f"{engine}: {leg} engine balance conflicts with the receipt")
            if flat_zingo is not None:
                require(flat_zingo == zingo_value,
                        f"{engine}: {leg} Zingo balance conflicts with the receipt")
            return engine_value, zingo_value, found["phase"]
        return (raw_engine if raw_engine is not None else flat_engine,
                raw_zingo if raw_zingo is not None else flat_zingo, None)

    out_engine, out_zingo, out_phase = merged(
        "outbound", out["engine_balance"], out["zingo_balance"])
    in_engine, in_zingo, in_phase = merged("inbound", None, None)
    back_engine, back_zingo, back_phase = merged(
        "spendback", back["engine_balance"], back["zingo_balance"])
    if in_engine is not None:
        require(in_engine >= inbound["amount"],
                f"{engine}: inbound engine balance is below the received payment")

    out_reopen = out["reopen"] if isinstance(out["reopen"], dict) else {}
    in_reopen = inbound["reopen"] if isinstance(inbound["reopen"], dict) else {}
    if engine == "zally" and isinstance(in_reopen.get("engine_rows"), dict):
        require(in_reopen["engine_rows"].get("return") is None,
                f"{engine}: receipt claims an unavailable sent history row")
    engine_out = history(out["engine_history"], f"{engine} outbound engine")
    if engine_out is None:
        engine_out = history(out_reopen.get("engine_sent_row"), f"{engine} outbound engine")
    zingo_out = history(out["zingo_history"], f"{engine} outbound Zingo")
    if zingo_out is None:
        zingo_out = history(out_reopen.get("zingo_payee_row"), f"{engine} outbound Zingo")
    engine_in = history(in_reopen.get("engine_rows", {}).get("receive")
                        if isinstance(in_reopen.get("engine_rows"), dict) else None,
                        f"{engine} inbound engine")
    zingo_in = history(in_reopen.get("zingo_rows", {}).get("receive")
                       if isinstance(in_reopen.get("zingo_rows"), dict) else None,
                       f"{engine} inbound Zingo")
    engine_back = history(in_reopen.get("engine_rows", {}).get("return")
                          if isinstance(in_reopen.get("engine_rows"), dict) else None,
                          f"{engine} return engine")
    zingo_back = history(in_reopen.get("zingo_rows", {}).get("return")
                         if isinstance(in_reopen.get("zingo_rows"), dict) else None,
                         f"{engine} return Zingo")

    for leg, key, current in (
            ("outbound", "engine_history_entries_after_reopen", engine_out),
            ("outbound", "zingo_history_entries_after_reopen", zingo_out),
            ("inbound", "engine_history_entries_after_reopen", engine_in),
            ("inbound", "zingo_history_entries_after_reopen", zingo_in),
            ("spendback", "engine_history_entries_after_reopen", engine_back),
            ("spendback", "zingo_history_entries_after_reopen", zingo_back)):
        entry = raw.get(leg, {})
        if key not in entry or entry[key] is None:
            continue
        seen = history(entry[key], f"{engine} {leg} {key}")
        if current is not None:
            require(seen == current, f"{engine}: {leg} history observation disagrees")
        current = seen
        if leg == "outbound" and key.startswith("engine"):
            engine_out = current
        elif leg == "outbound":
            zingo_out = current
        elif leg == "inbound" and key.startswith("engine"):
            engine_in = current
        elif leg == "inbound":
            zingo_in = current
        elif key.startswith("engine"):
            engine_back = current
        else:
            zingo_back = current

    out_pending = out["pending"]
    in_pending = inbound["pending"]
    observation_height(out_pending, engine)
    observation_height(in_pending, engine)
    for entry in (out_pending, in_pending):
        if isinstance(entry, dict):
            action = entry.get("action")
            require(action is None or isinstance(action, str),
                    f"{engine}: invalid pending observation action")

    engine_out_pending = ("pass" if out_obs.get("engine_pending") is True
                          else "unexercised")
    zingo_out_pending = ("pass" if out_obs.get("zingo_pending") is True
                         else "unexercised")
    engine_in_pending = ("pass" if in_obs.get("engine_pending") is True
                         else "unexercised")
    zingo_in_pending = ("pass" if in_obs.get("zingo_pending") is True
                        else "unexercised")
    engine_back_pending = ("pass" if back_obs.get("engine_pending") is True
                           else "unexercised")
    zingo_back_pending = ("pass" if back_obs.get("zingo_pending") is True
                          else "unexercised")
    if "engine_pending" not in out_obs:
        engine_out_pending = pending_row(out_pending, ("engine_wallet",))
    if "zingo_pending" not in out_obs:
        zingo_out_pending = pending_row(out_pending, ("zingo_payee_wallet", "zingo_wallet"))
    if "engine_pending" not in in_obs:
        engine_in_pending = pending_row(in_pending, ("engine_payee_wallet",))
    if "zingo_pending" not in in_obs:
        zingo_in_pending = pending_row(in_pending, ("zingo_payer_wallet",))

    node_out = "pass" if out_obs.get("node_mempool") is True else node_pending(out_pending)
    node_in = "pass" if in_obs.get("node_mempool") is True else node_pending(in_pending)
    node_back = "pass" if back_obs.get("node_mempool") is True else "unexercised"

    reopen_balance = "unexercised"
    balances = []
    for record in (out_reopen, in_reopen):
        if "balance" in record:
            require(isinstance(record["balance"], bool),
                    f"{engine}: reopen balance flag is not boolean")
            balances.append(record["balance"])
    if balances:
        require(all(balances), f"{engine}: a reopened wallet balance changed")
        reopen_balance = "pass"

    return {
        "engine_out_history": engine_out, "zingo_out_history": zingo_out,
        "engine_in_history": engine_in, "zingo_in_history": zingo_in,
        "engine_back_history": engine_back, "zingo_back_history": zingo_back,
        "engine_out_pending": engine_out_pending, "zingo_out_pending": zingo_out_pending,
        "engine_in_pending": engine_in_pending, "zingo_in_pending": zingo_in_pending,
        "engine_back_pending": engine_back_pending,
        "zingo_back_pending": zingo_back_pending,
        "node_out": node_out, "node_in": node_in, "node_back": node_back,
        "engine_out_balance": out_engine, "zingo_out_balance": out_zingo,
        "engine_in_balance": in_engine, "zingo_in_balance": in_zingo,
        "engine_back_balance": back_engine, "zingo_back_balance": back_zingo,
        "outbound_phase": out_phase, "inbound_phase": in_phase,
        "spendback_phase": back_phase, "reopen_balance": reopen_balance,
    }


def proof_gates(result, engine, ready, release, inputs):
    return {"stability": pin_stability(result, engine, result["pins"])[0],
            "sources": source_gate(engine, result["pins"], inputs),
            "binaries": binary_gate(engine, ready, inputs),
            "teardown": teardown_gate(result, engine)}


def schedule_label(ready):
    extras = [f"{name}@{ready['activation_heights'][name]}" for name in ACTIVATION
              if ready["activation_heights"][name] != 1]
    return "all-at-1" if not extras else "all-at-1-except-" + ",".join(extras)


def fixture_text(engine, actor, ready, pins):
    """Return the stable comparison recipe, not one run's unique identities."""
    counterpart = (f"Zingo CLI nakednet-test-mode {pins['zingo_sha256']}"
                   if actor == "engine" else
                   f"{engine} {ENGINES[engine]['interface']} "
                   f"{pins['engine_binary_sha256']}")
    return (f"Peer-isolated zcash-regtest, schedule {schedule_label(ready)} "
            f"(config {ready['activation_schedule_sha256']}), "
            f"start height {ready['current_tip']}, node {ready['node_image']}, "
            f"counterpart {counterpart}")


def binary_version(sha):
    return "binary " + sha[:12]


def version_claim(result, engine, pins, inputs, receipt_dir):
    """Bind the engine's version label to its pinned build identity."""
    fallback = {"version": binary_version(pins["engine_binary_sha256"]),
                "binary-only-version-claim": "pass",
                "source-version-registered": "unexercised",
                "source-version-binding-verified": "unexercised"}
    provenance = result.get("provenance")
    if provenance is None:
        return fallback
    require(isinstance(provenance, dict) and set(provenance) <= {"engine", "zingo"},
            f"{engine}: unexpected provenance fields")
    block = provenance.get("engine")
    if not isinstance(block, dict):
        return fallback
    require(block.get("engine_binary_sha256") == pins["engine_binary_sha256"],
            f"{engine}: provenance binary differs from the pinned engine")
    if block.get("label_verified") is not True:
        return fallback
    if inputs is None:
        return fallback
    if "local_build_sha256" in block:
        build_path = receipt_dir / "zally-build.json"
        require(engine == "zally" and build_path.is_file()
                and block["local_build_sha256"] == pin_value(inputs, ("zally-build.json",)) == digest(build_path),
                f"{engine}: local build evidence missing or changed")
        require(matches_local_zally_build(block, load(build_path), pin_value(inputs, ("zally-payment.py",))),
                f"{engine}: local build does not match the executable and source")
    else:
        require(matches_build(engine, pins["engine_binary_sha256"], block),
                f"{engine}: verified label does not match a registered executable and source")
    label = block.get("label")
    require(isinstance(label, str) and label.strip(), f"{engine}: verified label is empty")
    for key in ("rustc", "build_command"):
        require(isinstance(block.get(key), str) and block[key].strip(),
                f"{engine}: verified label lacks {key}")
    if engine == "vizor":
        commit(block.get("source_commit"), f"{engine} source")
        token(block.get("archive_sha256"), f"{engine} archive")
        require(block.get("driver_sha256") == pin_value(inputs, ("vizor-payment.rs",)),
                f"{engine}: provenance driver differs from the pinned driver")
        require(block.get("provenance_sha256") in inputs.values(),
                f"{engine}: source record is not pinned")
        token(block.get("provenance_sha256"), f"{engine} provenance")
        for key in ("lock_sha256", "build_log_sha256"):
            if block.get(key) is not None:
                token(block[key], f"{engine} {key}")
    else:
        require(block.get("git_metadata") is True and block.get("clean") is True,
                f"{engine}: verified label lacks a clean git worktree")
        commit(block.get("commit"), f"{engine} source")
        require(block.get("lock_sha256") in inputs.values(),
                f"{engine}: source lockfile is not pinned")
        token(block.get("lock_sha256"), f"{engine} lockfile")
        if engine == "zally":
            require(block.get("harness_sha256") == pin_value(inputs, ("zally-payment.rs",)),
                    f"{engine}: provenance harness differs from the pinned harness")
    return dict(fallback, version=label,
                **{"binary-only-version-claim": "unexercised",
                   "source-version-registered": "pass",
                   "source-version-binding-verified": "pass"})


def zingo_version_claim(result, pins):
    fallback = {"version": binary_version(pins["zingo_sha256"]),
                "binary-only-version-claim": "pass",
                "source-version-registered": "unexercised",
                "source-version-binding-verified": "unexercised"}
    provenance = result.get("provenance")
    if not isinstance(provenance, dict):
        return fallback
    block = provenance.get("zingo")
    if not isinstance(block, dict):
        return fallback
    require(block.get("sha256") == pins["zingo_sha256"],
            "zingo: provenance digest differs from the pinned client")
    if block.get("label_verified") is not True:
        return fallback
    require(matches_build("zingo", pins["zingo_sha256"], block),
            "zingo: verified label does not match a registered executable and source")
    commit(block.get("source"), "zingo source")
    require(isinstance(block.get("version"), str) and block["version"].strip(),
            "zingo: verified label has no binary version")
    require(isinstance(block.get("label"), str) and block["label"].strip(),
            "zingo: verified label is empty")
    return dict(fallback, version=block["version"],
                **{"binary-only-version-claim": "unexercised",
                   "source-version-registered": "pass",
                   "source-version-binding-verified": "pass"})


def common_checks(proofs, server, version):
    checks = {
        "selected-release-bound": "pass",
        "immutable-readiness-verified": "pass",
        "fixture-peer-isolated": "pass",
        "receipt-recorded-active-chain-membership": "pass",
        "independent-active-chain-audit": "unexercised",
        "method-attribution-traced": "unexercised",
        "funding-precedes-payment": "pass",
        "distinct-signed-transfers": "pass",
        "same-signed-transfer-as-counterparty": "pass",
        "client-inputs-stable-before-and-after": proofs["stability"]["status"],
        "source-documents-verified": proofs["sources"]["status"],
        "interface-binding-source-verified": proofs["sources"]["status"],
        "release-binaries-pinned-before-and-after": proofs["binaries"]["status"],
        "teardown-measured": proofs["teardown"]["status"],
        "live-server-identity-bound": server["status"],
    }
    checks.update({key: value for key, value in version.items() if key != "version"})
    return checks


def build_row(engine, actor, leg, action, accounting, checks, fixture, client_sha,
              version, receipt_sha, status):
    return {
        "id": f"counterparty-{engine}-{leg}-{actor}",
        "wallet": engine if actor == "engine" else ZINGO["wallet"],
        "version": version,
        "variant": ENGINES[engine]["variant"] if actor == "engine" else ZINGO["variant"],
        "implementation": (ENGINES[engine]["implementation"] if actor == "engine"
                           else ZINGO["implementation"]),
        "interface": (ENGINES[engine]["interface"] if actor == "engine"
                      else ZINGO["interface"]),
        "network": "zcash-regtest",
        "client_sha256": client_sha,
        "fixture": fixture,
        "action": action,
        "status": status,
        "method_scope": "none",
        "methods": {},
        "assertions": checks,
        "accounting_zat": accounting,
        "evidence": {
            "description": f"{engine.capitalize()} counterparty payment roles / receipt.json",
            "sha256": receipt_sha,
        },
    }


def excluded(engine, leg, actor, reason):
    return {"engine": engine, "leg": leg, "actor": actor, "reason": reason}


def privacy(rows, exclusions, result, ready):
    """Refuse an export that carries a private receipt identifier."""
    secrets = []
    for key in ("txid", "receive_txid", "return_txid"):
        value = (result.get("engine_pays", {}).get(key)
                 or result.get("engine_receives", {}).get(key))
        if isinstance(value, str):
            secrets.append(value)
    secrets.extend(value for value in result.get("memos", {}).values()
                   if isinstance(value, str))
    for entry in ready.get("binaries", {}).values():
        if isinstance(entry.get("source"), str):
            secrets.append(entry["source"])
    text = json.dumps({"rows": rows, "excluded": exclusions})
    for secret in secrets:
        require(secret not in text, "row export leaks a private receipt value")
    for forbidden in ("http://", "/home/", ".tmp/", "uregtest1", "utest1"):
        require(forbidden not in text, f"row export carries {forbidden}")


def note_evidence(facts, engine):
    inbound = facts["inbound"]
    if inbound["note"] is None or inbound["note_spent"] is None:
        return "unexercised"
    if inbound["note_spent"] == "ledger-derived (received row plus return sent row)":
        return "unexercised"
    if inbound["note_spent"] not in {"receive row spent_our_inputs",
                                     "received note absent from the unspent set; "
                                     "return change row present"}:
        return "unexercised"
    unspent = inbound["unspent"]
    require(isinstance(unspent, list), f"{engine}: spent note has no unspent set")
    if any(entry["tx_fingerprint_sha256"] == inbound["tx"]["fingerprint_sha256"]
           for entry in unspent):
        return "unexercised"
    return "pass"


def counterparty_import(path, release, allow_provisional=True):
    """Import one passed receipt. Return rows, exclusions, and proof gaps."""
    release_identity(release)
    path = Path(path)
    try:
        result = load(path)
    except OSError:
        raise ValueError("receipt could not be read") from None
    except json.JSONDecodeError:
        raise ValueError("receipt is not valid JSON") from None
    require(isinstance(result, dict), "receipt: expected an object")
    engine = result.get("engine")
    require(engine in ENGINES, "receipt: unknown counterparty engine")
    pins = basic(result, engine)
    require(pins.get("release") == release["tag"]
            and pins.get("release_commit") == release["commit"],
            f"{engine}: receipt is not bound to the selected release")
    ready_path, ready = readiness(path, pins, engine, release)
    facts = ledger(result, engine, ready)
    observed = balance_observations(result, engine, facts)
    proofs = proof_gates(result, engine, ready, release, pins.get("client_inputs_sha256"))
    server = server_gate(result, ready, ENGINES[engine]["interface"], engine, release,
                         proofs["sources"]["status"])
    reasons = sorted({value["reason"] for value in proofs.values()
                      if value["status"] != "pass"} | (
        {server["reason"]} if server["status"] != "pass" else set()))
    receipt_sha = digest(path)
    engine_version = version_claim(result, engine, pins,
                                   pins.get("client_inputs_sha256"), path.parent)
    zingo_version = zingo_version_claim(result, pins)
    labels = result.get("labels_verified")
    if labels is not None:
        require(isinstance(labels, bool), f"{engine}: labels_verified is not boolean")
        if labels is True:
            require(engine_version["source-version-registered"] == "pass"
                    and zingo_version["source-version-registered"] == "pass",
                    f"{engine}: labels_verified claims an unbound label")
    engine_checks = common_checks(proofs, server, engine_version)
    zingo_checks = common_checks(proofs, server, zingo_version)

    rows = []
    exclusions = []
    for actor, version in ((engine, engine_version), ("zingo", zingo_version)):
        if version["source-version-binding-verified"] != "pass":
            reasons.append(f"{actor}: executable source version is unverified")

    def emit(actor, leg, action, accounting, checks, version):
        status = "unexercised" if reasons else "pass"
        sha = pins["engine_binary_sha256"] if actor == "engine" else pins["zingo_sha256"]
        rows.append(build_row(
            engine, actor, leg, action, accounting, checks,
            fixture_text(engine, actor, ready, pins), sha, version["version"],
            receipt_sha, status))

    out = facts["outbound"]
    accounting = {"amount": out["amount"], "balance_after": observed["engine_out_balance"]}
    if out["fee"]["exposed"]:
        accounting["fee"] = out["fee"]["fee"]
    checks = {
        "payer-debit-equals-amount-plus-fee": "pass",
        "payer-fee-reconciled-with-wallet-fee":
            "pass" if out["fee"]["exposed"] else "unexercised",
        "wallet-reported-payer-fee": "pass" if out["fee"]["exposed"] else "unexercised",
        "payer-fee-residual-only": "unexercised" if out["fee"]["exposed"] else "pass",
        "payer-ledger-delta-reconciled": "pass" if out["fee"]["ledger"] else "unexercised",
        "own-sent-history-row-after-new-process": history_status(observed["engine_out_history"]),
        "reopen-balance-unchanged": observed["reopen_balance"],
        "own-balance-after-payment-recorded": "pass",
        "own-pending-state-observed": observed["engine_out_pending"],
        "node-mempool-pending-observed": observed["node_out"],
        "leg-balance-observed-after-confirmations":
            "pass" if observed["outbound_phase"] else "unexercised",
        "sender-memo-retention-observed": "unexercised",
    }
    emit("engine", "outbound",
         f"Pay an independently keyed Zingo wallet {out['amount']:,} zats on the isolated "
         "Regtest fixture", accounting, dict(engine_checks, **checks), engine_version)

    accounting = {"amount": out["amount"], "balance_after": observed["zingo_out_balance"]}
    checks = {
        "payee-amount-exact": "pass",
        "payee-memo-decrypted": memo_status(out["memo_verified"],
                                            f"{engine} outbound payee memo"),
        "own-receive-history-row-after-new-process": history_status(observed["zingo_out_history"]),
        "reopen-balance-unchanged": observed["reopen_balance"],
        "own-balance-after-payment-recorded": "pass",
        "own-pending-state-observed": observed["zingo_out_pending"],
        "node-mempool-pending-observed": observed["node_out"],
        "leg-balance-observed-after-confirmations":
            "pass" if observed["outbound_phase"] else "unexercised",
        "recipient-fee-not-applicable": "pass",
    }
    emit("zingo", "outbound",
         f"Receive {out['amount']:,} zats from the {engine} engine and verify the payment memo",
         accounting, dict(zingo_checks, **checks), zingo_version)

    inbound = facts["inbound"]
    engine_balance = observed["engine_in_balance"]
    zingo_balance = observed["zingo_in_balance"]
    if engine_balance is None:
        exclusions.append(excluded(
            engine, "inbound", "engine",
            "receipt records no engine wallet balance after the inbound payment; "
            "the recorded balances are after the later return payment"))
    if zingo_balance is None:
        exclusions.append(excluded(
            engine, "inbound", "zingo",
            "receipt records no Zingo payer wallet balance after the inbound payment; "
            "the recorded balances are after the later return payment"))
    if engine_balance is not None:
        checks = {
            "payee-amount-exact": "pass",
            "payee-memo-decrypted": memo_status(inbound["memo_verified"],
                                                f"{engine} inbound payee memo"),
            "own-receive-history-row-after-new-process":
                history_status(observed["engine_in_history"]),
            "reopen-balance-unchanged": observed["reopen_balance"],
            "own-balance-after-payment-recorded": "pass",
            "own-pending-state-observed": observed["engine_in_pending"],
            "node-mempool-pending-observed": observed["node_in"],
            "leg-balance-observed-after-confirmations":
                "pass" if observed["inbound_phase"] else "unexercised",
            "leg-balance-observed-before-return":
                "pass" if observed["inbound_phase"]
                and observed["inbound_phase"].get("before_return") is True
                else "unexercised",
            "received-funds-spent-reconciles": "pass",
            "received-note-identity-bound":
                "pass" if inbound["note"] is not None else "unexercised",
            "note-level-spend-linkage": note_evidence(facts, engine),
        }
        emit("engine", "inbound",
             f"Receive {inbound['amount']:,} zats from a second funded Zingo wallet",
             {"amount": inbound["amount"], "balance_after": engine_balance},
             dict(engine_checks, **checks), engine_version)
    if zingo_balance is not None:
        checks = {
            "payer-debit-equals-amount-plus-fee": "unexercised",
            "wallet-reported-payer-fee": "pass",
            "payer-ledger-delta-reconciled": "unexercised",
            "own-sent-history-row-after-new-process":
                history_status(observed["zingo_in_history"]),
            "reopen-balance-unchanged": observed["reopen_balance"],
            "own-balance-after-payment-recorded": "pass",
            "own-pending-state-observed": observed["zingo_in_pending"],
            "node-mempool-pending-observed": observed["node_in"],
            "leg-balance-observed-after-confirmations":
                "pass" if observed["inbound_phase"] else "unexercised",
            "leg-balance-observed-before-return":
                "pass" if observed["inbound_phase"]
                and observed["inbound_phase"].get("before_return") is True
                else "unexercised",
            "sender-memo-retention-observed": "unexercised",
        }
        emit("zingo", "inbound",
             f"Pay the {engine} engine {inbound['amount']:,} zats from a separately "
             "funded Zingo wallet",
             {"amount": inbound["amount"], "fee": inbound["zingo_fee"],
              "balance_after": zingo_balance},
             dict(zingo_checks, **checks), zingo_version)

    back = facts["spendback"]
    accounting = {"amount": back["amount"], "balance_after": observed["engine_back_balance"]}
    if back["fee"]["exposed"]:
        accounting["fee"] = back["fee"]["fee"]
    checks = {
        "payer-debit-equals-amount-plus-fee": "pass",
        "payer-fee-reconciled-with-wallet-fee":
            "pass" if back["fee"]["exposed"] else "unexercised",
        "wallet-reported-payer-fee": "pass" if back["fee"]["exposed"] else "unexercised",
        "payer-fee-residual-only": "unexercised" if back["fee"]["exposed"] else "pass",
        "payer-ledger-delta-reconciled": "pass" if back["fee"]["ledger"] else "unexercised",
        "own-sent-history-row-after-new-process":
            history_status(observed["engine_back_history"]),
        "reopen-balance-unchanged": observed["reopen_balance"],
        "own-balance-after-payment-recorded": "pass",
        "own-pending-state-observed": observed["engine_back_pending"],
        "node-mempool-pending-observed": observed["node_back"],
        "leg-balance-observed-after-confirmations":
            "pass" if observed["spendback_phase"] else "unexercised",
        "sender-memo-retention-observed": "unexercised",
    }
    emit("engine", "spendback",
         f"Spend the received funds back to the Zingo payer as {back['amount']:,} zats",
         accounting, dict(engine_checks, **checks), engine_version)

    accounting = {"amount": back["amount"], "balance_after": observed["zingo_back_balance"]}
    checks = {
        "payee-amount-exact": "pass",
        "payee-memo-decrypted": memo_status(back["memo_verified"],
                                            f"{engine} return payee memo"),
        "own-receive-history-row-after-new-process":
            history_status(observed["zingo_back_history"]),
        "reopen-balance-unchanged": observed["reopen_balance"],
        "own-balance-after-payment-recorded": "pass",
        "own-pending-state-observed": observed["zingo_back_pending"],
        "node-mempool-pending-observed": observed["node_back"],
        "leg-balance-observed-after-confirmations":
            "pass" if observed["spendback_phase"] else "unexercised",
        "recipient-fee-not-applicable": "pass",
    }
    emit("zingo", "spendback",
         f"Receive the engine's {back['amount']:,} zat return and verify the return memo",
         accounting, dict(zingo_checks, **checks), zingo_version)

    privacy(rows, exclusions, result, ready)
    if not allow_provisional and any(row["status"] != "pass" for row in rows):
        raise ValueError(f"{engine}: provisional receipt is not admissible")
    return {"engine": engine, "rows": rows, "excluded": exclusions,
            "provisional": reasons, "receipt_sha256": receipt_sha,
            "ready_sha256": pins["fixture_ready_sha256"], "ready": ready,
            "observation_phases": {
                leg: observed[f"{leg}_phase"] for leg in LEGS
                if observed[f"{leg}_phase"] is not None},
            "private_identity": {
                "release": release,
                "readiness_sha256": digest(ready_path),
                "anchor": {"height": ready["current_tip"],
                           "hash": ready["current_hash"]},
                "engine_binary_sha256": pins["engine_binary_sha256"],
                "zingo_sha256": pins["zingo_sha256"],
            },
            "pins": pins, "facts": facts}


def counterparty_actions(path, release, allow_provisional=True):
    """Return schema-1 rows for one selected-engine receipt."""
    return counterparty_import(path, release, allow_provisional=allow_provisional)["rows"]


def import_evidence(root, release, allow_provisional=True, receipts=None, attempts=None):
    """Import every engine receipt under an evidence root and record exclusions."""
    release_identity(release)
    root = Path(root)
    overrides = dict(receipts or {})
    require(set(overrides) <= set(ENGINES), "receipt override names an unknown engine")
    attempted = dict(attempts or {})
    require(set(attempted) <= set(ENGINES), "attempt override names an unknown engine")
    base = root / "evidence" if (root / "evidence").is_dir() else root
    rows, exclusions, provisional, imported = [], [], [], {}
    for engine in sorted(ENGINES):
        path = Path(overrides[engine]) if engine in overrides \
            else base / engine / "receipt.json"
        if not path.is_file():
            exclusions.append({"engine": engine, "scope": "engine",
                               "reason": f"no receipt at evidence/{engine}/receipt.json"})
            continue
        try:
            part = counterparty_import(path, release, allow_provisional=allow_provisional)
        except ValueError as error:
            exclusions.append({"engine": engine, "scope": "engine", "reason": str(error)})
            continue
        rows.extend(part["rows"])
        exclusions.extend(part["excluded"])
        provisional.extend(part["provisional"])
        imported[engine] = {
            "receipt_sha256": part["receipt_sha256"],
            "ready_sha256": part["ready_sha256"],
            "release_commit": part["private_identity"]["release"]["commit"],
            "anchor": part["private_identity"]["anchor"],
            "provisional": part["provisional"],
            "observation_phases": part["observation_phases"],
        }
    rejected = []
    for engine, value in sorted(attempted.items()):
        path = Path(value)
        record = {"engine": engine, "receipt_sha256": None, "status": None}
        if path.is_file():
            record["receipt_sha256"] = digest(path)
            try:
                body = load(path)
            except (OSError, json.JSONDecodeError):
                body = None
            if isinstance(body, dict):
                record["status"] = body.get("status")
        try:
            part = counterparty_import(path, release, allow_provisional=allow_provisional)
        except ValueError as error:
            record["reason"] = str(error)
        else:
            if all(row["status"] == "pass" for row in part["rows"]):
                record["reason"] = "importable"
            else:
                record["provisional"] = part["provisional"]
                record["reason"] = ("provisional: " + "; ".join(part["provisional"])
                                    if part["provisional"] else
                                    "provisional: unexercised rows")
        rejected.append(record)
    return {"schema_version": 1, "kind": "counterparty-payment-roles", "release": release,
            "scenarios": rows, "excluded": exclusions, "attempts": rejected,
            "provisional": sorted(set(provisional)), "receipts": imported,
            "limits": [
                "Private audit example from one passed receipt per engine; not a publication.",
                "Regtest only; the fixture is peer-isolated and changes no public network.",
                "Block membership is producer-asserted; no independent inclusion audit exists.",
                "No per-method RPC attribution is claimed.",
                "A version is claimed only when its label is bound to the pinned build.",
            ]}


def render_notes(payload):
    scenarios = payload["scenarios"]
    passed = sum(1 for row in scenarios if row["status"] == "pass")
    unexercised = len(scenarios) - passed
    lines = [f"# Counterparty payment-role rows for {payload['release']['tag']}", "",
             f"Release commit `{payload['release']['commit']}`. Private audit export, "
             "not a publication artifact.", "",
             f"Rows: {len(scenarios)} ({passed} pass, {unexercised} unexercised). "
             f"Excluded results: {len(payload['excluded'])}.", "",
             "Row status covers the publication gates. Actor-specific assertions that the "
             "receipt does not measure remain unexercised.", ""]
    leg_rows = sum(1 for entry in payload["excluded"] if entry.get("leg"))
    if leg_rows:
        lines.extend([f"Each imported receipt defines six leg/actor rows. {leg_rows} actor "
                      "rows are excluded because the receipt does not record their "
                      "post-payment balance; they are not derived from a later balance.", ""])
    lines.extend(["| Engine | Leg | Actor | Status | Evidence |", "|---|---|---|---|---|"])
    for row in scenarios:
        _, engine, leg, actor = row["id"].split("-")
        lines.append(f"| {engine} | {leg} | {actor} | {row['status']} | "
                     f"`{row['evidence']['sha256']}` |")
    lines.extend(["", "## Excluded results", ""])
    if payload["excluded"]:
        for entry in payload["excluded"]:
            scope = (f"{entry.get('leg')}/{entry.get('actor')}"
                     if entry.get("leg") else "engine")
            lines.append(f"- **{entry['engine']} {scope}**: {entry['reason']}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Provisional receipt gaps", ""])
    if payload.get("receipts"):
        for engine, entry in sorted(payload["receipts"].items()):
            reasons = entry.get("provisional", [])
            if reasons:
                for reason in reasons:
                    lines.append(f"- `{engine}`: {reason}")
            else:
                lines.append(f"- `{engine}`: none; every row passed its publication gates.")
    else:
        lines.append("- No passing receipt was imported.")
    lines.extend(["", "## Attempted receipts not imported", ""])
    if payload.get("attempts"):
        for entry in payload["attempts"]:
            digest_hint = (f"receipt `{entry['receipt_sha256']}`"
                           if entry.get("receipt_sha256") else "unreadable receipt")
            lines.append(f"- `{entry['engine']}` ({entry.get('status') or 'unknown'}, "
                         f"{digest_hint}): {entry['reason']}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Receipts", ""])
    for engine, entry in sorted(payload["receipts"].items()):
        anchor = entry.get("anchor", {})
        lines.append(f"- `{engine}`: receipt `{entry['receipt_sha256']}`, readiness "
                     f"`{entry['ready_sha256']}`, anchor {anchor.get('height')}/"
                     f"`{anchor.get('hash')}`.")
    lines.extend(["", "## Measured balance phases", ""])
    if payload.get("receipts"):
        for engine, entry in sorted(payload["receipts"].items()):
            phases = entry.get("observation_phases", {})
            if not phases:
                lines.append(f"- `{engine}`: no nested leg observation; flat balances only, "
                             "so the confirmation phase stays unexercised.")
                continue
            parts = []
            for leg in ("outbound", "inbound", "spendback"):
                phase = phases.get(leg)
                if not phase:
                    continue
                label = f"{leg} after {phase['after_confirmations']} confirmations"
                if phase.get("before_return") is True:
                    label += " before the return"
                parts.append(label)
            lines.append(f"- `{engine}`: " + "; ".join(parts) + ".")
    else:
        lines.append("- None.")
    lines.extend(["", "## Fields this importer requires for a passing row", "",
                  "The producer must record pre-run and post-run client input pins "
                  "(`pins.client_inputs_sha256`, `client_inputs_sha256_after`, "
                  "`inputs_stable: true`), a measured teardown "
                  "(`cleanup.verified: true` with zero `remaining_resources`), and "
                  "`endpoint_evidence` for the engine's interface. Inbound actor rows need "
                  "`engine_receives.leg_observations.inbound` with both balances, "
                  "`observed_after_confirmations`, and `before_return: true`. The nested "
                  "`outbound` and `return` blocks add the phase assertion; without them the "
                  "flat balances remain usable but the phase stays unexercised.", "",
                  "## Scope", "",
                  "No transaction ID, address, memo, endpoint, wallet path, or host path "
                  "appears in these rows. Block membership and the actor balances are the "
                  "receipt's recorded observations. Zally rows keep the derived fee out of "
                  "`accounting_zat.fee`, mark `payer-fee-residual-only`, and leave "
                  "`payer-fee-reconciled` and the sent-history row unexercised.", ""])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, help="selected local release tag")
    parser.add_argument("--revision", help="expected commit of the selected tag")
    parser.add_argument("--evidence", required=True, type=Path,
                        help="private other-roles evidence root")
    parser.add_argument("--receipt", action="append", default=[], metavar="ENGINE=PATH",
                        help="override one engine receipt path")
    parser.add_argument("--attempt", action="append", default=[], metavar="ENGINE=PATH",
                        help="record a rejected attempt without importing its rows")
    parser.add_argument("--output", required=True, type=Path,
                        help="directory for observations.json and NOTES.md")
    parser.add_argument("--strict", action="store_true",
                        help="reject a provisional receipt instead of importing it")
    args = parser.parse_args()
    release = selected_release(args.release, args.revision)
    overrides, attempts = {}, {}
    for values, target in ((args.receipt, overrides), (args.attempt, attempts)):
        for value in values:
            engine, separator, path = value.partition("=")
            require(separator and engine in ENGINES and path,
                    "receipt override must be ENGINE=PATH for a known engine")
            target[engine] = path
    payload = import_evidence(args.evidence, release, allow_provisional=not args.strict,
                              receipts=overrides, attempts=attempts)
    if payload["scenarios"]:
        sys.path.insert(0, str(HERE))
        import coverage as cov
        cov.validate({"schema_version": 1, "release": release,
                      "inventory": cov.inventories(release["commit"]),
                      "scenarios": payload["scenarios"]})
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    (output / "observations.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n")
    (output / "NOTES.md").write_text(render_notes(payload))
    print(json.dumps({"release": release, "rows": len(payload["scenarios"]),
                      "excluded": len(payload["excluded"]),
                      "provisional": len(payload["provisional"]),
                      "output": "observations.json and NOTES.md"}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"counterparty roles input error: {error}", file=sys.stderr)
        sys.exit(2)
