#!/usr/bin/env python3
"""Check counterparty receipt binding, actor rows, and fail-closed imports.

Runs with or without `python -O`; the importer and these checks use `require`
and `unittest` assertions, never bare `assert` statements.
"""
import ast
import copy
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import coverage as cov
from coverage_evidence import digest

import counterparty_builds
import coverage_counterparty_roles as roles


HERE = Path(__file__).resolve().parent
RELEASE = {"tag": "v0.22.0", "commit": "9" * 40}
ENGINES = ("devtool", "vizor", "zally")
PROJECTS = {"devtool": "zinder-fixture-pay-devtool-23300",
            "vizor": "zinder-fixture-pay-vizor-51000",
            "zally": "zinder-fixture-pay-zally-51200"}
BASES = {"devtool": 23300, "vizor": 51000, "zally": 51200}
AMOUNTS = {"engine_pays": 1_000_000, "engine_receives": 600_000,
           "engine_returns": 300_000}
MEMOS = {"engine_pays": "zinder-payment-roles/v1/engine-pays",
         "engine_receives": "zinder-payment-roles/v1/engine-receives",
         "engine_returns": "zinder-payment-roles/v1/engine-spends-received"}
BINARY_SHA = {name: hashlib.sha256(("release/" + name).encode()).hexdigest()
              for name in roles.SERVICES}
ENGINE_SHA = {engine: hashlib.sha256(("engine/" + engine).encode()).hexdigest()
              for engine in ENGINES}
ZINGO_SHA = hashlib.sha256(b"zingo").hexdigest()
SCHEDULES = {engine: hashlib.sha256(("schedule/" + engine).encode()).hexdigest()
             for engine in ENGINES}
NODE_IMAGE = "sha256:" + hashlib.sha256(b"node").hexdigest()
TREES = {
    "outbound": (hashlib.sha256(b"tx/outbound").hexdigest(), 252),
    "inbound": (hashlib.sha256(b"tx/inbound").hexdigest(), 375),
    "spendback": (hashlib.sha256(b"tx/spendback").hexdigest(), 386),
}
FUNDING = {"engine": (hashlib.sha256(b"tx/funding-engine").hexdigest(), 151),
           "zingo": (hashlib.sha256(b"tx/funding-zingo").hexdigest(), 365)}
LABELS = {"devtool": "zcash-devtool c8322f7e", "vizor": "Vizor v0.0.65",
          "zally": "Zally 31ee6c3a"}
ZINGO_VERSION = "Zingo CLI 0.1.1"
VIZOR_COMMIT = "d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6"
DEVTOOL_COMMIT = "c8322f7e71ae46ab24801a721523ba83b27d911b"
ZALLY_COMMIT = "31ee6c3ae09e109d189c6dcd691b192fb4c5b29e"
ZINGO_COMMIT = "81d799775f11e882896e8710e8af12d6d4d29a41"
PROBE_SHA = digest(Path(__file__).with_name("lightwalletd-identity.py"))


def token(label):
    return hashlib.sha256(label.encode()).hexdigest()


def variant_project(engine, variant):
    return PROJECTS[engine] + (f"-{variant}" if variant else "")


def variant_base(engine, variant):
    return BASES[engine] + (10 if variant else 0)


def ready(engine, variant="", release=RELEASE, schedule=None):
    project, base = variant_project(engine, variant), variant_base(engine, variant)
    activation = {name: 2 if engine == "vizor" and name == "NU6.3" else 1
                  for name in roles.ACTIVATION}
    if schedule:
        activation.update(schedule)
    schedule_digest = token(
        f"schedule/{engine}/{json.dumps(activation, sort_keys=True)}")
    version = release["tag"].lstrip("v")
    entry = lambda name: {"status": "ready", "cause": "ready",
                          "current_height": 150, "target_height": 150}
    return {
        "schema_version": 1, "project": project, "port_base": base,
        "network": "zcash-regtest", "zinder_commit": release["commit"],
        "images": {name: "sha256:" + token("image/" + name) for name in roles.SERVICES},
        "image_references": {name: f"offline-pin/zinder-{name}:{version}"
                             for name in roles.SERVICES},
        "image_manifest_sha256": token("image-manifest"),
        "binaries": {name: {"source": f"/private/release/zinder-{name}",
                            "sha256": BINARY_SHA[name]} for name in roles.SERVICES},
        "node_image": NODE_IMAGE, "activation_heights": activation,
        "activation_schedule_sha256": schedule_digest,
        "raw_blob_policy": "transactions",
        "node_rpc": f"http://127.0.0.1:{base + 32}",
        "lightwalletd": f"http://127.0.0.1:{base + 67}",
        "native_wallet_query": f"http://127.0.0.1:{base + 2}",
        "current_tip": 150,
        "current_hash": token(f"anchor/{engine}/{variant or 'a'}"),
        "isolation": {"peer_count": 0, "default_ipv4_route": False,
                      "internal_networks": [token("network/" + engine)]},
        "health": {name: {"status": "alive", "service": "zinder-" + name,
                          "version": version, "git_commit": release["commit"],
                          "network": "zcash-regtest", "capabilities": []}
                   for name in roles.SERVICES},
        "readiness": {name: entry(name) for name in roles.SERVICES},
    }


def fee(engine, amount, value, source):
    return {"amount_zat": amount, "fee_zat": value, "debit_zat": amount + value,
            "fee_source": source,
            "ledger_delta_zat": None if engine == "zally" else -(amount + value)}


def vizor_source_pins():
    root = "/private/source/vizor/rust/"
    return {root + name: token(value) for name, value in (
        ("src/lib.rs", "lib"), ("Cargo.toml", "manifest"), ("Cargo.lock", "vizor-lock"))}


def provenance(engine, sources, engine_sha=None):
    record = {"engine_binary_sha256": (engine_sha or ENGINE_SHA)[engine],
              "rustc": "rustc 1.100.0",
              "source": f"/private/source/{engine}", "label": LABELS[engine],
              "label_verified": (engine_sha or ENGINE_SHA)[engine] == ENGINE_SHA[engine],
              "build_command": "cargo build --locked"}
    if engine == "vizor":
        record.update({"source_commit": VIZOR_COMMIT,
                       "archive_sha256": token("vizor-archive"),
                       **counterparty_builds.vizor_source_fields(record["source"], vizor_source_pins()),
                       "driver_sha256": digest(dict(sources)["vizor-payment.rs"]),
                       "provenance_sha256": token("vizor-provenance"),
                       "build_log_sha256": token("vizor-build-log")})
    else:
        record.update({"git_metadata": True, "clean": True,
                       "commit": DEVTOOL_COMMIT if engine == "devtool" else ZALLY_COMMIT,
                       "lock_sha256": token(f"{engine}-lock")})
        if engine == "zally":
            record["harness_sha256"] = digest(dict(sources)["zally-payment.rs"])
    return record


def zingo_provenance(zingo_sha=ZINGO_SHA):
    return {"sha256": zingo_sha, "version": ZINGO_VERSION, "source": ZINGO_COMMIT,
            "label": "Zingo CLI from Zingolib 81d7997", "label_verified": zingo_sha == ZINGO_SHA}


def endpoint(engine, commit=RELEASE["commit"], anchor=None):
    anchor = anchor or token(f"anchor/{engine}/a")
    if engine == "vizor":
        return {"scope": "driver-verified",
                "lightwalletd_identity": "every command checks GetLightdInfo and the anchor",
                "anchor_verified": True}
    if engine == "devtool":
        return {"scope": "configured",
                "lightwalletd_identity": "loopback endpoint bound to the fixture receipt",
                "anchor_verified": None}
    return {"scope": "native-probe",
            "native_server_info": {
                "ok": True, "build_git_commit": commit,
                "expected_revision": commit, "revision_verified": True,
                "network": "zcash-regtest", "network_verified": True,
                "anchor_verified": True, "anchor_height": 150,
                "expected_height": 150, "anchor_hash": anchor,
                "expected_hash": anchor, "contract_revision": 15,
                "service_name": "zinder-query"},
            "anchor_verified": True}


def live_devtool_endpoint(ready_data, before_height=150, after_height=396):
    """Build the two-phase live probe evidence the devtool producer records."""
    endpoint = ready_data["lightwalletd"]
    port = int(endpoint.rsplit(":", 1)[1])
    commit = ready_data["zinder_commit"]
    anchor_height = ready_data["current_tip"]
    anchor_hash = ready_data["current_hash"]
    provenance = {
        "transport": "grpc insecure loopback GetLightdInfo/GetBlock/GetLatestBlock",
        "payloads_recorded": False,
    }

    def probe(phase, height):
        return {
            "ok": True, "command_exit": 0, "scope": "live-probe", "phase": phase,
            "endpoint": endpoint,
            "build_git_commit": commit, "expected_revision": commit,
            "revision_verified": True,
            "chain_name": "test", "expected_chain": "test",
            "network_verified": True,
            "anchor_height": anchor_height, "expected_height": anchor_height,
            "anchor_hash": anchor_hash, "expected_hash": anchor_hash,
            "anchor_verified": True,
            "served_height": height, "served_tip_height": height,
            "served_tip_hash": token(f"tip/{height}"), "tip_verified": True,
            "provenance": dict(provenance),
        }

    before = probe("before-admission", before_height)
    after = probe("after-use", after_height)
    stale_revision = probe("negative-stale-revision", before_height)
    stale_revision.update({"ok": False, "command_exit": 1,
                           "revision_verified": False,
                           "expected_revision": "a" * 40})
    wrong_chain = probe("negative-wrong-chain", before_height)
    wrong_chain.update({"ok": False, "command_exit": 1,
                        "network_verified": False, "expected_chain": "main"})
    stale_anchor = probe("negative-stale-anchor", before_height)
    stale_anchor.update({"ok": False, "command_exit": 1,
                         "anchor_verified": False,
                         "anchor_height": anchor_height - 1,
                         "anchor_hash": token("stale-anchor")})

    def wallet(height):
        return {"server_uri": endpoint, "chain_name": "test",
                "chain_tip_height": height}

    def reports(role):
        commands = ("init", "sync", "get-info", "sync", "send", "sync", "sync",
                    "get-info")
        return [{"command": command,
                 "connect_log_sha256": token(f"connect/{role}/{index}"),
                 "report_sha256": token(f"report/{role}/{index}"),
                 "connections": 1, "endpoint": f"loopback:{port}"}
                for index, command in enumerate(commands)]

    def binding(role):
        commands = reports(role)
        return {"scope": "Linux process tree connect syscalls per online CLI invocation",
                "bound": True, "endpoint": f"127.0.0.1:{port}",
                "online_commands": len(commands),
                "traced_commands": len(commands), "reports": commands}

    return {
        "scope": "live-probe-before-and-after",
        "lightwalletd_identity": "live GetLightdInfo and readiness anchor before and after",
        "lightwalletd_server_info": after,
        "observations": {"before_admission": before, "after_use": after},
        "negative_controls": [stale_revision, wrong_chain, stale_anchor],
        "wallet_endpoint": endpoint,
        "wallet_server_info": {
            "engine-pays": {"before_admission": wallet(before_height),
                            "after_use": wallet(after_height)},
            "engine-receives": {"after_use": wallet(after_height)},
        },
        "wallet_commands": {"engine-pays": binding("engine-pays"),
                            "engine-receives": binding("engine-receives")},
        "anchor_verified": True,
        "provenance": {
            "probe": "scripts/wallet-clients/lightwalletd-identity.py",
            "probe_sha256": PROBE_SHA,
            "transport": "grpc insecure loopback GetLightdInfo/GetBlock",
            "scope": "server identity and anchor only; no wallet payloads",
        },
    }


def receipt(engine, ready_path, ready_data, sources, release=RELEASE,
            engine_sha=None, zingo_sha=None):
    engine_hashes = engine_sha or ENGINE_SHA
    zingo_hash = zingo_sha or ZINGO_SHA
    project, base = ready_data["project"], ready_data["port_base"]
    pins = {name: digest(path) for name, path in sources if path is not None}
    pins["Cargo.lock"] = token(f"{engine}-lock")
    if engine == "vizor":
        pins["engine-source.json"] = token("vizor-provenance")
        pins.update(vizor_source_pins())
    if engine == "devtool":
        pins["lightwalletd-identity.py"] = PROBE_SHA
    for name in roles.SERVICES:
        pins[f"zinder-{name}"] = BINARY_SHA[name]
    pins[roles.ENGINES[engine]["binary"][0]] = engine_hashes[engine]
    pins["zingo-cli"] = zingo_hash
    out_txid, out_height = TREES["outbound"]
    in_txid, in_height = TREES["inbound"]
    back_txid, back_height = TREES["spendback"]
    if engine == "zally":
        out_value, out_source = 10_000, roles.FEE_SOURCES["zally"]
        back_value, back_source = 10_000, roles.FEE_SOURCES["zally"]
    elif engine == "devtool":
        out_value, out_source = 25_000, "wallet transaction fee_paid"
        back_value, back_source = 10_000, "wallet transaction fee_paid"
    else:
        out_value, out_source = 25_000, "wallet confirmed network fee"
        back_value, back_source = 10_000, "wallet confirmed network fee"
    out_fee = fee(engine, AMOUNTS["engine_pays"], out_value, out_source)
    back_fee = fee(engine, AMOUNTS["engine_returns"], back_value, back_source)
    if engine == "vizor":
        out_fee["proposal_fee_zat"] = out_value
        back_fee["proposal_fee_zat"] = back_value
    payer_balance = 500_000_000 - out_fee["debit_zat"]
    history = None if engine == "zally" else 1
    engine_wallet = None if engine == "zally" else {
        "row_observed": True, "pending": True}
    engine_rows = {"receive": 1}
    if engine != "zally":
        engine_rows["return"] = 1
    reopen = {"balance": True, "engine_rows": engine_rows,
              "zingo_rows": {"receive": 1, "return": 1}}
    note_spent = ("received note absent from the unspent set; return change row present"
                  if engine == "zally" else
                  "ledger-derived (received row plus return sent row)")
    return {
        "schema_version": 1, "engine": engine, "status": "passed", "roles": "both",
        "amounts_zat": dict(AMOUNTS), "memos": dict(MEMOS),
        "pins": {
            "release": release["tag"], "release_commit": release["commit"],
            "fixture_ready_sha256": digest(ready_path), "node_image": NODE_IMAGE,
            "activation_schedule_sha256": ready_data["activation_schedule_sha256"],
            "engine_binary_sha256": engine_hashes[engine], "zingo_sha256": zingo_hash,
            "fixture_project": project, "fixture_base": base,
            "fixture_node_rpc": f"http://127.0.0.1:{base + 32}",
            "images_reference_sha256": token("images/" + engine),
            "client_inputs_sha256": pins,
        },
        "client_inputs_sha256_after": copy.deepcopy(pins),
        "inputs_stable": True,
        "cleanup": {"fixture_removed": True, "owned": True, "verified": True,
                    "remaining_resources": {"container": 0, "network": 0, "volume": 0}},
        "provenance": {"engine": provenance(engine, sources, engine_hashes),
                       "zingo": zingo_provenance(zingo_hash)},
        "labels_verified": engine_hashes[engine] == ENGINE_SHA[engine] and zingo_hash == ZINGO_SHA,
        "endpoint_evidence": endpoint(engine, release["commit"],
                                      ready_data["current_hash"]),
        "fixture_isolation": ready_data["isolation"],
        "raw_blob_policy": "transactions",
        "engine_payer_funding": {"tx_fingerprint_sha256": FUNDING["engine"][0],
                                 "mined_height": FUNDING["engine"][1],
                                 "spendable_zat": 500_000_000},
        "zingo_payer_funding": {"tx_fingerprint_sha256": FUNDING["zingo"][0],
                                "mined_height": FUNDING["zingo"][1],
                                "spendable_zat": 499_985_000},
        "engine_pays": {
            "status": "passed", "direction": "engine-pays-zingo", "txid": out_txid,
            "amount_zat": AMOUNTS["engine_pays"], "memo_verified_by_payee": True,
            "payer_fee": out_fee, "payer_balance_after_zat": payer_balance,
            "payee_balance_after_zat": 1_000_000,
            "pending": {"node_mempool": True, "observed_height": 251,
                        "engine_wallet": engine_wallet,
                        "zingo_payee_wallet": {"row_observed": True, "pending": True}},
            "leg_observations": {"outbound": {
                "engine_balance_after_zat": payer_balance,
                "zingo_balance_after_zat": 1_000_000,
                "engine_height": out_height + 9, "zingo_height": out_height + 9,
                "observed_after_confirmations": 10}},
            "reopen": {"balance": True, "engine_sent_row": history,
                       "zingo_payee_row": 1},
            "reopened_ledger_verified": engine != "zally",
            "engine_history_entries_after_reopen": history,
            "zingo_history_entries_after_reopen": 1,
            "node_inclusion": {"fingerprint_sha256": roles.fingerprint(out_txid),
                               "mined_height": out_height, "active_chain_block": True,
                               "occurrences_in_block": 1},
        },
        "engine_receives": {
            "status": "passed", "direction": "zingo-pays-engine",
            "receive_txid": in_txid, "return_txid": back_txid,
            "receive_amount_zat": AMOUNTS["engine_receives"],
            "return_amount_zat": AMOUNTS["engine_returns"],
            "zingo_payer_fee_zat": 10_000,
            "engine_return_fee": back_fee,
            "memo_verified_by_engine": engine != "zally",
            "engine_memo_status": (None if engine != "zally" else
                                   "sender recorded the memo; the receiving wallet "
                                   "stored no memo for the output"),
            "return_memo_verified_by_zingo": True,
            "leg_observations": {
                "inbound": {"engine_balance_after_zat": 600_000,
                            "zingo_balance_after_zat": 499_375_000,
                            "engine_height": in_height + 9, "zingo_height": in_height + 9,
                            "observed_after_confirmations": 10,
                            "before_return": True},
                "return": {"engine_balance_after_zat": 290_000,
                           "zingo_balance_after_zat": 499_675_000,
                           "engine_height": back_height + 9, "zingo_height": back_height + 9,
                           "observed_after_confirmations": 10}},
            "pending": {"node_mempool": True, "observed_height": 374,
                        "zingo_payer_wallet": {"row_observed": True, "pending": True},
                        "engine_payee_wallet": engine_wallet},
            "received_note": {"tx_fingerprint_sha256": roles.fingerprint(in_txid),
                              "output_index": 0, "value_zat": 600_000,
                              "pool": "sapling"},
            "received_note_spent": note_spent,
            "unspent_after": ([{"tx_fingerprint_sha256": roles.fingerprint(back_txid),
                                "value_zat": 290_000, "pool": "sapling"}]
                              if engine == "zally" else []),
            "received_funds_spent": True, "reopen": reopen,
            "reopened_ledger_verified": engine != "zally",
            "engine_balance_after_zat": 290_000,
            "zingo_balance_after_zat": 499_675_000,
            "node_inclusion": [
                {"fingerprint_sha256": roles.fingerprint(in_txid),
                 "mined_height": in_height, "active_chain_block": True,
                 "occurrences_in_block": 1},
                {"fingerprint_sha256": roles.fingerprint(back_txid),
                 "mined_height": back_height, "active_chain_block": True,
                 "occurrences_in_block": 1},
            ],
        },
        "evidence_limits": {"network": "isolated zcash-regtest fixture owned by this run",
                            "public_network_payments": "none",
                            "production_capacity": "not measured"},
    }


class Campaign:
    def __init__(self, root, variant="", release=RELEASE, schedule=None,
                 engine_sha=None, zingo_sha=None):
        self.root = root
        self.variant = variant
        self.release = release
        self.schedule = schedule
        self.engine_sha = dict(engine_sha or ENGINE_SHA)
        self.zingo_sha = zingo_sha or ZINGO_SHA
        self.sources = {}
        for engine in ENGINES:
            self.sources[engine] = self.copy_sources(engine)
        self.patcher = mock.patch.object(roles, "local_sources",
                                         lambda engine: self.sources[engine])
        self.patcher.start()
        self.baselines = {}
        self.receipts = {}
        self.reset()

    def stop(self):
        self.patcher.stop()

    def copy_sources(self, engine):
        target = self.root / "sources" / engine
        target.mkdir(parents=True)
        copied = []
        for name, local in roles.local_sources(engine):
            if local is None:
                copied.append((name, None))
                continue
            destination = target / name
            shutil.copy2(local, destination)
            copied.append((name, destination))
        return copied

    def evidence(self, engine):
        return self.root / "evidence" / engine

    def ready_data(self, engine):
        return ready(engine, self.variant, self.release, self.schedule)

    def ready_path(self, engine):
        return (self.evidence(engine) / "fixtures"
                / variant_project(engine, self.variant) / "ready.initial.json")

    def receipt_path(self, engine):
        return self.evidence(engine) / "receipt.json"

    def write_ready(self, engine):
        path = self.ready_path(engine)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.ready_data(engine), indent=2, sort_keys=True) + "\n")

    def write_receipt(self, engine, value):
        path = self.receipt_path(engine)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

    def reset(self):
        for engine in ENGINES:
            self.write_ready(engine)
            self.baselines[engine] = receipt(
                engine, self.ready_path(engine), self.ready_data(engine),
                self.sources[engine], self.release, self.engine_sha, self.zingo_sha)
            self.receipts[engine] = copy.deepcopy(self.baselines[engine])
            self.write_receipt(engine, self.receipts[engine])

    def mutate(self, engine, change):
        value = copy.deepcopy(self.baselines[engine])
        change(value)
        self.write_receipt(engine, value)

    def mutate_ready(self, engine, change):
        path = self.ready_path(engine)
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        value = copy.deepcopy(self.baselines[engine])
        value["pins"]["fixture_ready_sha256"] = digest(path)
        self.write_receipt(engine, value)

    def strip_provisional(self, engine):
        def change(value):
            value["pins"].pop("client_inputs_sha256")
            value.pop("client_inputs_sha256_after")
            value.pop("inputs_stable")
            value["cleanup"] = {"fixture_removed": True}
            value.pop("endpoint_evidence")
            value.pop("provenance")
            value.pop("labels_verified")
        self.mutate(engine, change)


class CounterpartyCase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        registry = {}
        for engine in ENGINES:
            block = provenance(engine, roles.local_sources(engine))
            keys = ("label", "commit", "lock_sha256")
            if engine == "vizor":
                keys = ("label", "source_commit", "archive_sha256", "driver_sha256", "lock_sha256", "rust_source_sha256", "source_manifest_sha256")
            record = {key: block[key] for key in keys}
            if engine == "zally":
                record["harness_sha256"] = block["harness_sha256"]
            registry[ENGINE_SHA[engine]] = dict(record, engine=engine)
        zingo = zingo_provenance()
        registry[ZINGO_SHA] = {key: zingo[key] for key in ("label", "source", "version")}
        registry[ZINGO_SHA]["engine"] = "zingo"
        self.enterContext(mock.patch.dict(counterparty_builds.REGISTERED_BUILDS, registry))
        self.campaign = Campaign(self.root)
        self.addCleanup(self.campaign.stop)

    def second_campaign(self, **settings):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        campaign = Campaign(Path(temporary.name), **settings)
        self.addCleanup(campaign.stop)
        return campaign

    def reject(self, engine, change):
        self.campaign.reset()
        self.campaign.mutate(engine, change)
        with self.assertRaises(ValueError):
            roles.counterparty_actions(self.campaign.receipt_path(engine), RELEASE)


class CounterpartyRows(CounterpartyCase):
    def rows(self, engine):
        return roles.counterparty_actions(self.campaign.receipt_path(engine), RELEASE)

    def matrix(self, rows, release=RELEASE):
        return {"schema_version": 1, "release": release,
                "inventory": {"lightwalletd": ["GetBlock"],
                              "native-wallet-query": ["VisibleTipBlock"]},
                "scenarios": rows}

    def validate(self, rows):
        return cov.validate(self.matrix(rows))["scenarios"]

    def test_each_leg_and_actor_is_a_separate_row(self):
        rows = self.rows("vizor")
        self.assertEqual([row["id"] for row in rows], [
            "counterparty-vizor-outbound-engine",
            "counterparty-vizor-outbound-zingo",
            "counterparty-vizor-inbound-engine",
            "counterparty-vizor-inbound-zingo",
            "counterparty-vizor-spendback-engine",
            "counterparty-vizor-spendback-zingo",
        ])
        self.assertEqual([row["wallet"] for row in rows],
                         ["vizor", "zingo", "vizor", "zingo", "vizor", "zingo"])
        self.assertEqual([row["interface"] for row in rows], ["lightwalletd"] * 6)
        zally = self.rows("zally")
        self.assertEqual([row["interface"] for row in zally],
                         ["native-wallet-query", "lightwalletd",
                          "native-wallet-query", "lightwalletd",
                          "native-wallet-query", "lightwalletd"])
        devtool = self.rows("devtool")
        self.assertEqual({row["implementation"] for row in devtool}, {"cli", "engine"})
        self.assertEqual(devtool[0]["variant"], "sapling-cli")
        for row in rows + zally:
            self.assertEqual(row["method_scope"], "none")
            self.assertEqual(row["methods"], {})
            self.assertNotIn("observed_methods", row)
            self.assertEqual(row["status"], "pass")
            self.assertEqual(row["network"], "zcash-regtest")
            self.assertEqual(row["assertions"]["source-version-registered"], "pass")
            self.assertEqual(row["assertions"]["binary-only-version-claim"], "unexercised")
            self.assertNotIn("/home/", json.dumps(row))
            self.assertNotIn(".tmp", json.dumps(row))
            self.assertNotIn("http://", json.dumps(row))
            self.assertNotIn("uregtest", json.dumps(row))
        self.assertEqual(rows[0]["version"], LABELS["vizor"])
        self.assertEqual(rows[1]["version"], ZINGO_VERSION)
        self.assertEqual(zally[0]["version"], LABELS["zally"])
        self.assertEqual(devtool[0]["version"], LABELS["devtool"])
        self.validate(rows)

    def test_independent_ready_and_block_hashes_keep_comparable_rows(self):
        second = self.second_campaign(variant="b")
        for engine in ("vizor", "zally"):
            first_rows = roles.counterparty_actions(
                self.campaign.receipt_path(engine), RELEASE)
            second_rows = roles.counterparty_actions(second.receipt_path(engine), RELEASE)
            first_ready = roles.load(self.campaign.ready_path(engine))
            second_ready = roles.load(second.ready_path(engine))
            self.assertNotEqual(first_ready["current_hash"], second_ready["current_hash"])
            self.assertNotEqual(roles.digest(self.campaign.ready_path(engine)),
                                roles.digest(second.ready_path(engine)))
            self.assertEqual([row["fixture"] for row in first_rows],
                             [row["fixture"] for row in second_rows])
            self.assertEqual([row["version"] for row in first_rows],
                             [row["version"] for row in second_rows])
            self.assertEqual([row["client_sha256"] for row in first_rows],
                             [row["client_sha256"] for row in second_rows])
            self.assertEqual(cov.compare(self.matrix(first_rows),
                                         self.matrix(second_rows)), [])

    def test_new_release_with_same_recipe_keeps_comparable_rows(self):
        release = {"tag": "v0.23.0", "commit": "8" * 40}
        second = self.second_campaign(variant="b", release=release)
        for engine in ("vizor", "zally"):
            first_rows = roles.counterparty_actions(
                self.campaign.receipt_path(engine), RELEASE)
            second_rows = roles.counterparty_actions(second.receipt_path(engine), release)
            self.assertEqual([row["fixture"] for row in first_rows],
                             [row["fixture"] for row in second_rows])
            self.assertEqual(cov.compare(self.matrix(first_rows),
                                         self.matrix(second_rows, release)), [])

    def test_changed_schedule_is_incomparable(self):
        second = self.second_campaign(variant="b", schedule={"NU6.3": 3})
        for engine in ("vizor", "zally"):
            first_rows = roles.counterparty_actions(
                self.campaign.receipt_path(engine), RELEASE)
            second_rows = roles.counterparty_actions(second.receipt_path(engine), RELEASE)
            changes = cov.compare(self.matrix(first_rows), self.matrix(second_rows))
            self.assertTrue(any("incomparable" in change and "fixture" in change
                                for change in changes), changes)

    def test_changed_client_is_incomparable(self):
        engine_sha = dict(ENGINE_SHA)
        engine_sha["vizor"] = token("engine/vizor/changed")
        second = self.second_campaign(variant="b", engine_sha=engine_sha)
        first_rows = roles.counterparty_actions(self.campaign.receipt_path("vizor"),
                                                RELEASE)
        second_rows = roles.counterparty_actions(second.receipt_path("vizor"), RELEASE)
        changes = cov.compare(self.matrix(first_rows), self.matrix(second_rows))
        self.assertTrue(any("client_sha256" in change for change in changes), changes)
        self.assertTrue(any("fixture" in change for change in changes), changes)
        zingo = self.second_campaign(variant="b", zingo_sha=token("zingo/changed"))
        zingo_rows = roles.counterparty_actions(zingo.receipt_path("vizor"), RELEASE)
        changes = cov.compare(self.matrix(first_rows), self.matrix(zingo_rows))
        self.assertTrue(any("client_sha256" in change for change in changes), changes)
        self.assertTrue(any("fixture" in change for change in changes), changes)

    def test_private_identity_keeps_readiness_release_and_anchor(self):
        part = roles.counterparty_import(self.campaign.receipt_path("vizor"), RELEASE)
        ready = roles.load(self.campaign.ready_path("vizor"))
        identity = part["private_identity"]
        self.assertEqual(identity["release"], RELEASE)
        self.assertEqual(identity["readiness_sha256"],
                         roles.digest(self.campaign.ready_path("vizor")))
        self.assertEqual(identity["anchor"],
                         {"height": ready["current_tip"], "hash": ready["current_hash"]})
        row = part["rows"][0]
        self.assertNotIn(ready["current_hash"], row["fixture"])
        self.assertNotIn(identity["readiness_sha256"], row["fixture"])
        self.assertNotIn(RELEASE["tag"], row["fixture"])
        self.assertIn(ready["node_image"], row["fixture"])
        self.assertIn(str(ready["current_tip"]), row["fixture"])
        self.assertIn("zcash-regtest", row["fixture"])
    def test_actor_accounting_uses_only_measured_fields(self):
        rows = {row["id"]: row for row in self.rows("vizor")}
        self.assertEqual(rows["counterparty-vizor-outbound-engine"]["accounting_zat"],
                         {"amount": 1_000_000, "fee": 25_000, "balance_after": 498_975_000})
        self.assertEqual(rows["counterparty-vizor-outbound-zingo"]["accounting_zat"],
                         {"amount": 1_000_000, "balance_after": 1_000_000})
        self.assertEqual(rows["counterparty-vizor-inbound-engine"]["accounting_zat"],
                         {"amount": 600_000, "balance_after": 600_000})
        self.assertEqual(rows["counterparty-vizor-inbound-zingo"]["accounting_zat"],
                         {"amount": 600_000, "fee": 10_000, "balance_after": 499_375_000})
        self.assertEqual(rows["counterparty-vizor-spendback-engine"]["accounting_zat"],
                         {"amount": 300_000, "fee": 10_000, "balance_after": 290_000})
        self.assertEqual(rows["counterparty-vizor-spendback-zingo"]["accounting_zat"],
                         {"amount": 300_000, "balance_after": 499_675_000})
        zally = {row["id"]: row for row in self.rows("zally")}
        self.assertEqual(zally["counterparty-zally-outbound-engine"]["accounting_zat"],
                         {"amount": 1_000_000, "balance_after": 498_990_000})
        self.assertNotIn("fee", zally["counterparty-zally-outbound-engine"]["accounting_zat"])
        self.assertEqual(
            zally["counterparty-zally-outbound-engine"]["assertions"]
            ["payer-fee-residual-only"], "pass")
        self.assertEqual(
            zally["counterparty-zally-outbound-engine"]["assertions"]
            ["payer-fee-reconciled-with-wallet-fee"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-outbound-engine"]["assertions"]
            ["wallet-reported-payer-fee"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-outbound-engine"]["assertions"]
            ["own-sent-history-row-after-new-process"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-inbound-engine"]["assertions"]
            ["payee-memo-decrypted"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-inbound-engine"]["assertions"]
            ["note-level-spend-linkage"], "pass")
        vizor = {row["id"]: row for row in self.rows("vizor")}
        self.assertEqual(
            vizor["counterparty-vizor-outbound-engine"]["assertions"]
            ["wallet-reported-payer-fee"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-inbound-engine"]["assertions"]
            ["note-level-spend-linkage"], "unexercised")

    def test_pending_history_and_memo_are_per_actor(self):
        vizor = {row["id"]: row for row in self.rows("vizor")}
        self.assertEqual(
            vizor["counterparty-vizor-outbound-engine"]["assertions"]
            ["own-pending-state-observed"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-outbound-zingo"]["assertions"]
            ["own-pending-state-observed"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-inbound-engine"]["assertions"]
            ["own-pending-state-observed"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-inbound-zingo"]["assertions"]
            ["own-sent-history-row-after-new-process"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-spendback-engine"]["assertions"]
            ["own-sent-history-row-after-new-process"], "pass")
        self.assertEqual(
            vizor["counterparty-vizor-spendback-engine"]["assertions"]
            ["own-pending-state-observed"], "unexercised")
        self.assertEqual(
            vizor["counterparty-vizor-spendback-engine"]["assertions"]
            ["node-mempool-pending-observed"], "unexercised")
        zally = {row["id"]: row for row in self.rows("zally")}
        self.assertEqual(
            zally["counterparty-zally-outbound-engine"]["assertions"]
            ["own-pending-state-observed"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-inbound-engine"]["assertions"]
            ["own-pending-state-observed"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-spendback-engine"]["assertions"]
            ["own-sent-history-row-after-new-process"], "unexercised")
        self.assertEqual(
            zally["counterparty-zally-inbound-zingo"]["assertions"]
            ["own-pending-state-observed"], "pass")

    def test_inbound_rows_are_excluded_when_leg_balances_are_absent(self):
        def change(value):
            value["engine_receives"].pop("leg_observations")
        self.campaign.mutate("vizor", change)
        part = roles.counterparty_import(self.campaign.receipt_path("vizor"), RELEASE)
        self.assertEqual([row["id"] for row in part["rows"]], [
            "counterparty-vizor-outbound-engine",
            "counterparty-vizor-outbound-zingo",
            "counterparty-vizor-spendback-engine",
            "counterparty-vizor-spendback-zingo",
        ])
        self.assertEqual(part["excluded"], [
            {"engine": "vizor", "leg": "inbound", "actor": "engine",
             "reason": "receipt records no engine wallet balance after the inbound "
                       "payment; the recorded balances are after the later return payment"},
            {"engine": "vizor", "leg": "inbound", "actor": "zingo",
             "reason": "receipt records no Zingo payer wallet balance after the inbound "
                       "payment; the recorded balances are after the later return payment"},
        ])
        self.assertEqual(
            part["rows"][0]["assertions"]["own-pending-state-observed"], "pass")

    def test_nested_leg_observations_expose_phase_and_before_return(self):
        part = roles.counterparty_import(self.campaign.receipt_path("zally"), RELEASE)
        self.assertEqual(part["observation_phases"], {
            "outbound": {"after_confirmations": 10},
            "inbound": {"after_confirmations": 10, "before_return": True},
            "spendback": {"after_confirmations": 10},
        })
        by_id = {row["id"]: row for row in part["rows"]}
        self.assertEqual(len(by_id), 6)
        for row in by_id.values():
            self.assertEqual(
                row["assertions"]["leg-balance-observed-after-confirmations"], "pass")
        for actor in ("engine", "zingo"):
            self.assertEqual(
                by_id[f"counterparty-zally-inbound-{actor}"]["assertions"]
                ["leg-balance-observed-before-return"], "pass")

    def test_archival_flat_balances_import_without_a_phase(self):
        def change(value):
            value["engine_pays"].pop("leg_observations")
            value["engine_receives"].pop("leg_observations")
        self.campaign.mutate("vizor", change)
        part = roles.counterparty_import(self.campaign.receipt_path("vizor"), RELEASE)
        self.assertEqual(part["observation_phases"], {})
        self.assertEqual(len(part["rows"]), 4)
        self.assertEqual(len(part["excluded"]), 2)
        for row in part["rows"]:
            self.assertEqual(row["status"], "pass")
            self.assertEqual(
                row["assertions"]["leg-balance-observed-after-confirmations"],
                "unexercised")

    def test_archival_top_level_balances_are_used_without_a_phase(self):
        def change(value):
            value["engine_pays"].pop("leg_observations")
            value["engine_receives"].pop("leg_observations")
            value["leg_observations"] = {"inbound": {
                "engine_balance_after_zat": 600_000,
                "zingo_balance_after_zat": 499_375_000}}
        self.campaign.mutate("vizor", change)
        part = roles.counterparty_import(self.campaign.receipt_path("vizor"), RELEASE)
        self.assertEqual(part["observation_phases"], {})
        self.assertEqual(len(part["rows"]), 6)
        for row in part["rows"]:
            self.assertEqual(
                row["assertions"]["leg-balance-observed-after-confirmations"],
                "unexercised")
        for actor in ("engine", "zingo"):
            self.assertEqual(
                {row["id"]: row for row in part["rows"]}
                [f"counterparty-vizor-inbound-{actor}"]["assertions"]
                ["leg-balance-observed-before-return"], "unexercised")

    def test_provisional_receipt_is_documented_and_strict_rejects(self):
        self.campaign.strip_provisional("zally")
        part = roles.counterparty_import(self.campaign.receipt_path("zally"), RELEASE)
        self.assertEqual(len(part["rows"]), 6)
        self.assertEqual(part["provisional"], [
            "receipt records no live server identity",
            "receipt records no portable client input pins",
            "teardown verification is not measured",
            "zally: executable source version is unverified",
            "zingo: executable source version is unverified",
        ])
        for row in part["rows"]:
            self.assertEqual(row["status"], "unexercised")
            for key in ("client-inputs-stable-before-and-after",
                        "source-documents-verified",
                        "release-binaries-pinned-before-and-after",
                        "teardown-measured", "live-server-identity-bound"):
                self.assertEqual(row["assertions"][key], "unexercised")
        with self.assertRaises(ValueError):
            roles.counterparty_import(self.campaign.receipt_path("zally"), RELEASE,
                                      allow_provisional=False)

    def test_endpoint_evidence_is_engine_specific(self):
        devtool = roles.counterparty_import(self.campaign.receipt_path("devtool"), RELEASE)
        self.assertEqual({row["status"] for row in devtool["rows"]}, {"unexercised"})
        self.assertIn("receipt records only a configured endpoint", devtool["provisional"])
        self.assertEqual(
            devtool["rows"][0]["assertions"]["live-server-identity-bound"], "unexercised")
        zally = roles.counterparty_import(self.campaign.receipt_path("zally"), RELEASE)
        self.assertEqual({row["status"] for row in zally["rows"]}, {"pass"})
        self.assertEqual(zally["rows"][0]["assertions"]["live-server-identity-bound"], "pass")
        self.campaign.reset()
        self.campaign.mutate("zally",
                             lambda r: r["endpoint_evidence"].pop("native_server_info"))
        part = roles.counterparty_import(self.campaign.receipt_path("zally"), RELEASE)
        self.assertIn("receipt records no native server identity", part["provisional"])

    def test_devtool_live_endpoint_evidence_admits_strict_import(self):
        self.campaign.mutate("devtool", lambda receipt: receipt.update(
            endpoint_evidence=live_devtool_endpoint(
                self.campaign.ready_data("devtool"))))
        part = roles.counterparty_import(self.campaign.receipt_path("devtool"), RELEASE,
                                         allow_provisional=False)
        self.assertEqual(part["provisional"], [])
        self.assertEqual([row["status"] for row in part["rows"]], ["pass"] * 6)
        for row in part["rows"]:
            self.assertEqual(row["assertions"]["live-server-identity-bound"], "pass")
            self.assertEqual(row["assertions"]["method-attribution-traced"],
                             "unexercised")
        text = json.dumps(part["rows"])
        self.assertNotIn("http://", text)
        self.assertNotIn("lightwalletd-identity.py", text)
        self.assertNotIn("/home/", text)

    def test_devtool_live_endpoint_missing_claims_downgrade(self):
        cases = (
            ("observations",
             "receipt records no endpoint before and after observations"),
            ("lightwalletd_server_info",
             "receipt records no live server identity"),
            ("wallet_endpoint",
             "receipt records no wallet-observed endpoint"),
            ("wallet_server_info",
             "receipt records no wallet-observed server identity"),
            ("wallet_commands",
             "receipt records no wallet endpoint trace"),
            ("negative_controls",
             "receipt records no endpoint negative controls"),
            ("provenance",
             "receipt records no endpoint probe provenance"),
        )
        for key, reason in cases:
            with self.subTest(missing=key):
                payload = copy.deepcopy(self.campaign.baselines["devtool"])
                payload["endpoint_evidence"] = live_devtool_endpoint(
                    self.campaign.ready_data("devtool"))
                payload["endpoint_evidence"].pop(key)
                self.campaign.write_receipt("devtool", payload)
                part = roles.counterparty_import(
                    self.campaign.receipt_path("devtool"), RELEASE)
                self.assertEqual({row["status"] for row in part["rows"]},
                                 {"unexercised"})
                self.assertIn(reason, part["provisional"])
                with self.assertRaisesRegex(ValueError, "provisional receipt"):
                    roles.counterparty_import(
                        self.campaign.receipt_path("devtool"), RELEASE,
                        allow_provisional=False)

    def test_changed_trace_source_downgrades_the_devtool_claim(self):
        payload = copy.deepcopy(self.campaign.baselines["devtool"])
        payload["endpoint_evidence"] = live_devtool_endpoint(
            self.campaign.ready_data("devtool"))
        self.campaign.write_receipt("devtool", payload)
        trace = dict(self.campaign.sources["devtool"])["trace-wallet-endpoint.py"]
        trace.write_text(trace.read_text() + "\n# changed\n")
        part = roles.counterparty_import(self.campaign.receipt_path("devtool"), RELEASE)
        self.assertIn("pinned trace-wallet-endpoint.py differs from the current source",
                      part["provisional"])
        for row in part["rows"]:
            self.assertEqual(row["status"], "unexercised")
            self.assertEqual(row["assertions"]["source-documents-verified"],
                             "unexercised")
            self.assertEqual(row["assertions"]["live-server-identity-bound"], "pass")
        with self.assertRaisesRegex(ValueError, "provisional receipt"):
            roles.counterparty_import(self.campaign.receipt_path("devtool"), RELEASE,
                                      allow_provisional=False)

    def test_devtool_live_endpoint_without_a_pinned_probe_downgrades(self):
        payload = copy.deepcopy(self.campaign.baselines["devtool"])
        payload["endpoint_evidence"] = live_devtool_endpoint(
            self.campaign.ready_data("devtool"))
        payload["pins"]["client_inputs_sha256"].pop("lightwalletd-identity.py")
        payload["client_inputs_sha256_after"].pop("lightwalletd-identity.py")
        self.campaign.write_receipt("devtool", payload)
        part = roles.counterparty_import(self.campaign.receipt_path("devtool"), RELEASE)
        self.assertIn("receipt records no pinned endpoint probe", part["provisional"])
        self.assertEqual(part["rows"][0]["assertions"]["live-server-identity-bound"],
                         "unexercised")

    def test_provenance_labels_bind_to_pinned_builds(self):
        rows = self.rows("vizor")
        self.assertEqual(rows[0]["version"], "Vizor v0.0.65")
        self.assertEqual(rows[0]["assertions"]["source-version-binding-verified"], "pass")
        self.campaign.mutate("zally", lambda r: (
            r["provenance"]["engine"].update(label_verified=False),
            r.update(labels_verified=False)))
        rows = self.rows("zally")
        self.assertEqual(rows[0]["version"], "binary " + ENGINE_SHA["zally"][:12])
        self.assertEqual(rows[0]["assertions"]["source-version-registered"], "unexercised")
        self.campaign.mutate("zally", lambda r: (
            r["provenance"]["zingo"].update(label_verified=False),
            r.update(labels_verified=False)))
        rows = self.rows("zally")
        self.assertEqual(rows[1]["version"], "binary " + ZINGO_SHA[:12])

    def test_verified_labels_require_an_independently_registered_executable_and_source(self):
        for engine in ENGINES:
            for field in ("label", "source_commit" if engine == "vizor" else "commit", "lock_sha256"):
                with self.subTest(engine=engine, field=field):
                    self.reject(engine, lambda r: r["provenance"]["engine"].update({field: token("other")}))
            changed = self.second_campaign(engine_sha=ENGINE_SHA | {engine: token("unregistered")})
            rows = roles.counterparty_actions(changed.receipt_path(engine), RELEASE)
            self.assertEqual(rows[0]["version"], "binary " + token("unregistered")[:12])
            changed.mutate(engine, lambda r: r["provenance"]["engine"].update(label_verified=True))
            with self.assertRaisesRegex(ValueError, "registered executable"):
                roles.counterparty_actions(changed.receipt_path(engine), RELEASE)
        self.reject("vizor", lambda r: r["provenance"]["zingo"].update(source="f" * 40))

    def test_vizor_requires_the_selected_source_tree_and_the_observed_proposal(self):
        for src in (None, "/different/source"):
            self.reject("vizor", lambda r: r["provenance"]["engine"].update(source=src))
        for field in ("src/lib.rs", "Cargo.toml", "Cargo.lock"):
            def change(r):
                for pins in (r["pins"]["client_inputs_sha256"], r["client_inputs_sha256_after"]):
                    pins["/private/source/vizor/rust/" + field] = token("changed")
            self.reject("vizor", change)
        self.reject("vizor", lambda r: r["engine_pays"]["payer_fee"].pop("proposal_fee_zat"))

    def test_local_zally_build_record_binds_compiler_output_and_inputs(self):
        engine = "zally"
        sha = token("new local build")
        run = self.second_campaign(engine_sha=ENGINE_SHA | {engine: sha})
        path = run.receipt_path(engine)
        receipt = roles.load(path)
        block = receipt["provenance"]["engine"]
        pins = receipt["pins"]["client_inputs_sha256"]
        build = {"target": "zally-payment-harness", "engine_binary_sha256": sha,
                 "commit": block["commit"], "lock_sha256": block["lock_sha256"],
                 "harness_sha256": block["harness_sha256"],
                 "builder_sha256": roles.pin_value(pins, ("zally-payment.py",))}
        sidecar = path.with_name("zally-build.json")

        def record(data):
            sidecar.write_text(json.dumps(data))
            pins[sidecar.name] = block["local_build_sha256"] = roles.digest(sidecar)
            receipt["client_inputs_sha256_after"] = dict(pins)
            receipt["labels_verified"] = block["label_verified"] = True
            run.write_receipt(engine, receipt)

        record(build)
        rows = roles.counterparty_import(path, RELEASE, allow_provisional=False)["rows"]
        self.assertEqual(rows[0]["version"], LABELS[engine])
        for key in build:
            record(build | {key: token("wrong")})
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "local build"):
                roles.counterparty_actions(path, RELEASE)
        record(build)
        sidecar.unlink()
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            roles.counterparty_actions(path, RELEASE)

    def test_fees_require_the_correct_actor_source_and_consistent_proposal(self):
        for engine in ENGINES:
            for field in ("engine_pays", "engine_receives"):
                key = "payer_fee" if field == "engine_pays" else "engine_return_fee"
                for other in ENGINES:
                    if other != engine:
                        self.reject(engine, lambda r: r[field][key].update(fee_source=roles.FEE_SOURCES[other]))
                self.reject(engine, lambda r: r[field][key].update(proposal_fee_zat=1))

    def test_import_evidence_counts_excluded_engines_and_identical_rows(self):
        self.campaign.receipt_path("devtool").unlink()
        payload = roles.import_evidence(self.root / "evidence", RELEASE)
        self.assertEqual(len(payload["scenarios"]), 12)
        self.assertEqual(payload["excluded"], [
            {"engine": "devtool", "scope": "engine",
             "reason": "no receipt at evidence/devtool/receipt.json"},
        ])
        self.assertEqual(payload["release"], RELEASE)
        self.validate(payload["scenarios"])
        self.assertEqual(payload["scenarios"],
                         roles.counterparty_actions(self.campaign.receipt_path("vizor"),
                                                    RELEASE)
                         + roles.counterparty_actions(self.campaign.receipt_path("zally"),
                                                      RELEASE))

    def test_export_is_privacy_safe_and_the_guard_stays_on(self):
        payload = roles.import_evidence(self.root, RELEASE)
        text = json.dumps(payload)
        source = json.loads(self.campaign.receipt_path("vizor").read_text())
        for value in MEMOS.values():
            self.assertNotIn(value, text)
        for key in ("txid", "receive_txid", "return_txid"):
            for value in (source["engine_pays"].get(key),
                          source["engine_receives"].get(key)):
                if value:
                    self.assertNotIn(value, text)
        self.assertNotIn("/home/", text)
        self.assertNotIn("/private/", text)
        leaked = copy.deepcopy(source)
        with self.assertRaises(ValueError):
            roles.privacy([{"memo": leaked["memos"]["engine_pays"]}], [], leaked,
                          roles.load(self.campaign.ready_path("vizor")))

    def test_module_uses_require_not_bare_assertions(self):
        tree = ast.parse((HERE / "coverage_counterparty_roles.py").read_text())
        self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(tree)))


class NegativeControls(CounterpartyCase):
    def reject_ready(self, engine, change):
        self.campaign.reset()
        self.campaign.mutate_ready(engine, change)
        with self.assertRaises(ValueError):
            roles.counterparty_actions(self.campaign.receipt_path(engine), RELEASE)

    def test_receipt_identity_and_status_controls(self):
        mutations = (
            lambda r: r.update(schema_version=2),
            lambda r: r.update(status="failed"),
            lambda r: r.update(engine="unknown"),
            lambda r: r["pins"].update(release="v0.22.1"),
            lambda r: r["pins"].update(release_commit="8" * 40),
            lambda r: r["pins"].update(engine_binary_sha256="short"),
            lambda r: r["pins"].update(zingo_sha256="0" * 63),
            lambda r: r["pins"].update(fixture_ready_sha256="0" * 64),
            lambda r: r["pins"].pop("fixture_project"),
            lambda r: r["pins"].update(fixture_base=51001),
            lambda r: r["pins"].update(fixture_node_rpc="http://127.0.0.1:1/other"),
            lambda r: r["pins"].update(node_image="sha256:short"),
            lambda r: r["pins"].update(activation_schedule_sha256="0" * 64),
            lambda r: r.update(amounts_zat={"engine_pays": 1}),
            lambda r: r.update(memos={"engine_pays": MEMOS["engine_pays"]}),
            lambda r: r["memos"].update(engine_pays=r["memos"]["engine_receives"]),
            lambda r: r["engine_pays"].update(amount_zat=1),
            lambda r: r["engine_pays"].update(status="failed"),
            lambda r: r["engine_pays"].update(direction="zingo-pays-engine"),
            lambda r: r["engine_receives"].update(receive_amount_zat=1),
            lambda r: r["engine_receives"].update(return_amount_zat=1),
            lambda r: r["engine_receives"].update(received_funds_spent=False),
            lambda r: r["engine_pays"].update(txid="0" * 64),
            lambda r: r["engine_receives"].update(receive_txid="short"),
            lambda r: r.update(fixture_isolation={"peer_count": 1}),
            lambda r: r.update(raw_blob_policy="compact"),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("vizor", change)

    def test_readiness_controls(self):
        mutations = (
            lambda r: r.update(zinder_commit="8" * 40),
            lambda r: r.update(project="other"),
            lambda r: r.update(port_base=51001),
            lambda r: r.update(network="testnet"),
            lambda r: r.update(node_image="sha256:" + "0" * 64),
            lambda r: r.update(activation_schedule_sha256="0" * 64),
            lambda r: r["isolation"].update(peer_count=1),
            lambda r: r["isolation"].update(default_ipv4_route=True),
            lambda r: r["images"].pop("query"),
            lambda r: r["image_references"].update(query="offline-pin/zinder-query:0.22.1"),
            lambda r: r["binaries"].pop("ingest"),
            lambda r: r["binaries"]["ingest"].update(sha256="short"),
            lambda r: r["health"]["query"].update(git_commit="8" * 40),
            lambda r: r["health"]["query"].update(version="0.22.1"),
            lambda r: r["readiness"]["query"].update(status="not_ready"),
            lambda r: r["readiness"]["query"].update(current_height=149),
            lambda r: r["activation_heights"].pop("NU6.3"),
            lambda r: r.update(current_tip=0),
            lambda r: r.update(current_hash="short"),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject_ready("vizor", change)

    def test_accepts_unified_zinder_readiness_and_rejects_mixed_or_unready_roles(self):
        self.campaign.reset()
        ready_path = self.campaign.ready_path("vizor")
        legacy_ready = json.loads(ready_path.read_text())
        version = RELEASE["tag"].lstrip("v")
        zinder_sha = token("binary/zinder")
        unified = copy.deepcopy(legacy_ready)
        unified["images"] = {"zinder": "sha256:" + token("image/zinder")}
        unified["image_references"] = {"zinder": f"offline-pin/zinder:{version}"}
        unified["binaries"] = {"zinder": {"source": "/private/release/zinder", "sha256": zinder_sha}}
        unified["health"] = {
            proc: {
                "status": "alive",
                "service": "zinder",
                "version": version,
                "git_commit": RELEASE["commit"],
                "network": "zcash-regtest",
                "capabilities": [],
            }
            for proc in ("index", "serve")
        }
        unified["readiness"] = {
            "index": {
                "status": "ready",
                "phase": "ready",
                "current_height": 150,
                "target_height": 150,
                "roles": [{"role": "ingest", "ready": True, "state": "ready"}],
            },
            "serve": {
                "status": "ready",
                "phase": "ready",
                "current_height": 150,
                "target_height": None,
                "roles": [
                    {"role": "query", "ready": True, "state": "ready"},
                    {"role": "compat-lightwalletd", "ready": True, "state": "ready"},
                ],
            },
        }
        self.campaign.mutate_ready("vizor", lambda r: (r.clear(), r.update(copy.deepcopy(unified))))
        receipt_doc = json.loads(self.campaign.receipt_path("vizor").read_text())
        receipt_doc["pins"]["client_inputs_sha256"]["zinder"] = zinder_sha
        receipt_doc["client_inputs_sha256_after"]["zinder"] = zinder_sha
        self.campaign.write_receipt("vizor", receipt_doc)
        rows = roles.counterparty_actions(self.campaign.receipt_path("vizor"), RELEASE)
        self.assertTrue(rows)

        mixed = copy.deepcopy(unified)
        mixed["readiness"] = copy.deepcopy(legacy_ready["readiness"])
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "fixtures" / legacy_ready["project"] / "ready.initial.json"
            p.parent.mkdir(parents=True)
            p.write_text(json.dumps(mixed))
            pins = {
                "fixture_project": legacy_ready["project"],
                "fixture_ready_sha256": digest(p),
                "release_commit": RELEASE["commit"],
                "release": RELEASE["tag"],
                "fixture_base": legacy_ready["port_base"],
                "fixture_node_rpc": legacy_ready["node_rpc"],
                "activation_schedule_sha256": legacy_ready["activation_schedule_sha256"],
                "node_image": legacy_ready["node_image"],
            }
            with self.assertRaisesRegex(ValueError, "incomplete readiness state"):
                roles.readiness(Path(td) / "receipt.json", pins, "vizor", RELEASE)

            unready_role = copy.deepcopy(unified)
            unready_role["readiness"]["serve"]["roles"][1]["ready"] = False
            p.write_text(json.dumps(unready_role))
            pins["fixture_ready_sha256"] = digest(p)
            with self.assertRaisesRegex(ValueError, "compat-lightwalletd was not ready"):
                roles.readiness(Path(td) / "receipt.json", pins, "vizor", RELEASE)

            missing_roles = copy.deepcopy(unified)
            missing_roles["readiness"]["serve"].pop("roles")
            p.write_text(json.dumps(missing_roles))
            pins["fixture_ready_sha256"] = digest(p)
            with self.assertRaisesRegex(ValueError, "invalid serve roles"):
                roles.readiness(Path(td) / "receipt.json", pins, "vizor", RELEASE)

    def test_ledger_and_inclusion_controls(self):
        mutations = (
            lambda r: r["engine_pays"]["payer_fee"].update(debit_zat=1_000_001),
            lambda r: r["engine_pays"]["payer_fee"].update(fee_zat=0),
            lambda r: r["engine_pays"]["payer_fee"].update(ledger_delta_zat=-1),
            lambda r: r["engine_pays"]["payer_fee"].update(fee_source="other"),
            lambda r: r["engine_receives"]["engine_return_fee"].update(debit_zat=1),
            lambda r: r["engine_receives"].update(zingo_payer_fee_zat=0),
            lambda r: r["engine_pays"].update(payer_balance_after_zat=-1),
            lambda r: r["engine_receives"].update(engine_balance_after_zat=-1),
            lambda r: r["engine_pays"].update(payee_balance_after_zat=1),
            lambda r: r["engine_receives"].update(zingo_balance_after_zat=1),
            lambda r: r["engine_pays"]["node_inclusion"].update(active_chain_block=False),
            lambda r: r["engine_pays"]["node_inclusion"].update(occurrences_in_block=2),
            lambda r: r["engine_pays"]["node_inclusion"].update(mined_height=0),
            lambda r: r["engine_pays"]["node_inclusion"].update(
                fingerprint_sha256="0" * 64),
            lambda r: r["engine_receives"]["node_inclusion"].pop(),
            lambda r: r["engine_receives"]["node_inclusion"].reverse(),
            lambda r: r["engine_receives"]["node_inclusion"][1].update(
                fingerprint_sha256=r["engine_receives"]["node_inclusion"][0]
                ["fingerprint_sha256"]),
            lambda r: r["engine_payer_funding"].update(mined_height=300),
            lambda r: r["zingo_payer_funding"].update(mined_height=400),
            lambda r: r["engine_payer_funding"].update(
                tx_fingerprint_sha256=r["zingo_payer_funding"]["tx_fingerprint_sha256"]),
            lambda r: r["engine_pays"]["pending"]["engine_wallet"].update(pending="yes"),
            lambda r: r["engine_pays"]["pending"].update(observed_height=0),
            lambda r: r["engine_receives"]["pending"]["zingo_payer_wallet"].update(
                row_observed="yes"),
            lambda r: r["engine_receives"]["reopen"]["engine_rows"].update(receive=2),
            lambda r: r["engine_receives"]["reopen"].update(balance="yes"),
            lambda r: r["engine_receives"]["reopen"].update(balance=False),
            lambda r: r["engine_receives"]["received_note"].update(
                tx_fingerprint_sha256="0" * 64),
            lambda r: r["engine_receives"]["received_note"].update(value_zat=1),
            lambda r: r["engine_receives"].update(
                received_note_spent="not observed"),
            lambda r: r["engine_receives"].update(
                unspent_after=[{"tx_fingerprint_sha256": "short", "value_zat": 1}]),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("vizor", change)

    def test_zally_cannot_claim_exposed_fee_or_sent_history(self):
        self.reject("zally",
                    lambda r: r["engine_pays"].update(
                        engine_history_entries_after_reopen=1))
        self.reject("zally",
                    lambda r: r["engine_pays"]["payer_fee"].update(
                        fee_source="wallet confirmed network fee"))
        self.reject("zally",
                    lambda r: r["engine_receives"]["engine_return_fee"].update(
                        fee_source="wallet transaction fee_paid"))
        self.reject("zally",
                    lambda r: r["engine_receives"]["reopen"]["engine_rows"].update(
                        {"return": 1}))

    def test_pin_teardown_and_endpoint_contradictions_reject(self):
        mutations = (
            lambda r: r.update(inputs_stable=False),
            lambda r: r.update(client_inputs_sha256_after={}),
            lambda r: r["client_inputs_sha256_after"].update(
                {"payment-roles.py": "0" * 64}),
            lambda r: r["pins"].update(client_inputs_sha256={"payment-roles.py": "bad"}),
            lambda r: r["cleanup"].update({"fixture_removed": True, "verified": True,
                                           "remaining_resources": {
                                               "container": 1, "network": 0,
                                               "volume": 0}}),
            lambda r: r["endpoint_evidence"].update(anchor_verified=False),
            lambda r: r["provenance"]["engine"].update(engine_binary_sha256="0" * 64),
            lambda r: r["provenance"]["engine"].update(label=""),
            lambda r: r["provenance"]["zingo"].update(sha256="0" * 64),
            lambda r: r["provenance"]["zingo"].update(source="short"),
            lambda r: r.update(labels_verified="yes"),
            lambda r: r["provenance"]["engine"].update(label_verified=False),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("vizor", change)

    def test_native_identity_and_provenance_controls(self):
        mutations = (
            lambda r: r["endpoint_evidence"]["native_server_info"].update(ok=False),
            lambda r: r["endpoint_evidence"]["native_server_info"].update(
                build_git_commit="8" * 40),
            lambda r: r["endpoint_evidence"]["native_server_info"].update(
                anchor_hash="0" * 64),
            lambda r: r["endpoint_evidence"]["native_server_info"].update(
                network="testnet"),
            lambda r: r["endpoint_evidence"]["native_server_info"].update(
                anchor_verified=False),
            lambda r: r["endpoint_evidence"].update(anchor_verified=False),
            lambda r: r["provenance"]["engine"].update(commit="short"),
            lambda r: r["provenance"]["engine"].update(clean=False),
            lambda r: r["provenance"]["engine"].update(lock_sha256="0" * 64),
            lambda r: r["provenance"]["engine"].update(harness_sha256="0" * 64),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("zally", change)

    def test_labels_verified_cannot_cover_an_unbound_label(self):
        def change(receipt):
            receipt["pins"].pop("client_inputs_sha256")
            receipt.pop("client_inputs_sha256_after")
            receipt.pop("inputs_stable")
        self.reject("vizor", change)

    def test_incomplete_teardown_is_downgraded_not_passed(self):
        cases = (
            ({"fixture_removed": True}, "teardown verification is not measured"),
            ({"fixture_removed": True, "verified": True},
             "remaining fixture resources are not measured"),
            ({"fixture_removed": True, "verified": True, "remaining_resources": {}},
             "remaining fixture resources are not measured"),
            ({"fixture_removed": True, "verified": True,
              "remaining_resources": {"container": "0", "network": 0, "volume": 0}},
             "remaining fixture resources are not measured"),
            ({"fixture_removed": False, "verified": True},
             "fixture removal is not recorded"),
            ({"fixture_removed": False, "kept_by_request": True},
             "fixture was kept by request"),
            ({"reused_fixture": True, "verified": None},
             "fixture was reused and not removed by this run"),
        )
        for cleanup, reason in cases:
            with self.subTest(reason=reason):
                self.campaign.reset()
                self.campaign.mutate("vizor", lambda r, c=cleanup: r.update(cleanup=c))
                part = roles.counterparty_import(self.campaign.receipt_path("vizor"),
                                                 RELEASE)
                self.assertEqual(part["rows"][0]["status"], "unexercised")
                self.assertIn(reason, part["provisional"])

    def test_nested_observation_contradictions_reject(self):
        inbound = lambda r: r["engine_receives"]["leg_observations"]["inbound"]
        returned = lambda r: r["engine_receives"]["leg_observations"]["return"]
        outbound = lambda r: r["engine_pays"]["leg_observations"]["outbound"]
        mutations = (
            lambda r: inbound(r).update(engine_balance_after_zat=-1),
            lambda r: inbound(r).pop("zingo_balance_after_zat"),
            lambda r: inbound(r).pop("observed_after_confirmations"),
            lambda r: inbound(r).update(observed_after_confirmations=0),
            lambda r: inbound(r).update(observed_after_confirmations=11),
            lambda r: inbound(r).pop("engine_height"),
            lambda r: inbound(r).pop("zingo_height"),
            lambda r: inbound(r).update(engine_height=inbound(r)["engine_height"] + 1),
            lambda r: inbound(r).update(zingo_height=inbound(r)["zingo_height"] - 1),
            lambda r: inbound(r).update(before_return=False),
            lambda r: inbound(r).pop("before_return"),
            lambda r: returned(r).update(before_return=True),
            lambda r: r["engine_receives"]["leg_observations"].update(unknown={}),
            lambda r: inbound(r).update(unknown=True),
            lambda r: outbound(r).update(engine_balance_after_zat=1),
            lambda r: returned(r).update(engine_balance_after_zat=1),
            lambda r: r["engine_pays"].update(leg_observations={"inbound": {}}),
            lambda r: r["engine_receives"].update(leg_observations={"outbound": {}}),
            lambda r: r.update(leg_observations={
                "inbound": {"engine_balance_after_zat": 1}}),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("vizor", change)

    def test_failed_and_malformed_receipts_are_excluded_or_rejected(self):
        self.campaign.reset()
        self.campaign.mutate("devtool", lambda r: r.update(status="failed"))
        payload = roles.import_evidence(self.root, RELEASE)
        self.assertEqual([entry for entry in payload["excluded"]
                          if entry["engine"] == "devtool"],
                         [{"engine": "devtool", "scope": "engine",
                           "reason": "devtool: receipt did not pass"}])
        self.campaign.receipt_path("devtool").write_text("not json")
        payload = roles.import_evidence(self.root, RELEASE)
        self.assertEqual([entry for entry in payload["excluded"]
                          if entry["engine"] == "devtool"],
                         [{"engine": "devtool", "scope": "engine",
                           "reason": "receipt is not valid JSON"}])

    def test_devtool_cannot_borrow_vizors_driver_endpoint_claim(self):
        self.campaign.mutate("devtool", lambda receipt: receipt.update(endpoint_evidence=endpoint("vizor")))
        receipt = self.campaign.receipt_path("devtool")
        with self.assertRaisesRegex(ValueError, "provisional receipt"):
            roles.counterparty_import(receipt, RELEASE, allow_provisional=False)

    def live_devtool(self, change):
        evidence = live_devtool_endpoint(self.campaign.ready_data("devtool"))
        change(evidence)
        return lambda receipt: receipt.update(endpoint_evidence=evidence)

    def test_devtool_live_endpoint_contradictions_reject(self):
        mutations = (
            lambda live: live.update(scope="configured"),
            lambda live: live.update(anchor_verified=False),
            lambda live: live.update(wallet_endpoint="http://127.0.0.1:23368"),
            lambda live: live["observations"].pop("before_admission"),
            lambda live: live["observations"].update(extra={}),
            lambda live: live["observations"]["before_admission"].update(
                phase="after-use"),
            lambda live: live["observations"]["before_admission"].update(
                build_git_commit="8" * 40),
            lambda live: live["observations"]["before_admission"].update(
                chain_name="main"),
            lambda live: live["observations"]["before_admission"].update(
                anchor_hash="0" * 64),
            lambda live: live["observations"]["before_admission"].update(ok=False),
            lambda live: live["observations"]["before_admission"].update(
                command_exit=1),
            lambda live: live["observations"]["before_admission"].update(
                served_tip_height=151),
            lambda live: live["observations"]["after_use"].update(served_height=100),
            lambda live: live["observations"]["after_use"].update(served_height=400),
            lambda live: live["observations"]["before_admission"]["provenance"].update(
                payloads_recorded=True),
            lambda live: live["observations"]["before_admission"]["provenance"].update(
                transport="something else"),
            lambda live: live["lightwalletd_server_info"].update(served_height=151),
            lambda live: live["lightwalletd_server_info"].update(phase="before-admission"),
            lambda live: live["wallet_server_info"]["engine-pays"][
                "before_admission"].update(server_uri="http://127.0.0.1:23368"),
            lambda live: live["wallet_server_info"]["engine-pays"][
                "before_admission"].update(chain_name="main"),
            lambda live: live["wallet_server_info"]["engine-pays"][
                "after_use"].update(chain_tip_height=150),
            lambda live: live["wallet_server_info"].pop("engine-receives"),
            lambda live: live["wallet_commands"]["engine-pays"].update(bound=False),
            lambda live: live["wallet_commands"]["engine-pays"].update(
                online_commands=9),
            lambda live: live["wallet_commands"]["engine-pays"].update(
                traced_commands=7),
            lambda live: live["wallet_commands"]["engine-pays"].update(
                endpoint="127.0.0.1:23368"),
            lambda live: live["wallet_commands"]["engine-pays"]["reports"][
                0].update(endpoint="loopback:23368"),
            lambda live: live["wallet_commands"]["engine-pays"]["reports"][
                0].update(connections=0),
            lambda live: live["wallet_commands"]["engine-pays"]["reports"][
                0].pop("connect_log_sha256"),
            lambda live: live["wallet_commands"]["engine-pays"]["reports"][
                0].pop("report_sha256"),
            lambda live: live["negative_controls"].pop(),
            lambda live: live["negative_controls"][0].update(ok=True),
            lambda live: live["negative_controls"][0].update(command_exit=0),
            lambda live: live["negative_controls"][0].update(network_verified=False),
            lambda live: live["negative_controls"][0].update(
                expected_revision=live["lightwalletd_server_info"]["build_git_commit"]),
            lambda live: live["negative_controls"][1].update(expected_chain="test"),
            lambda live: live["negative_controls"][2].update(
                anchor_height=live["lightwalletd_server_info"]["anchor_height"],
                anchor_hash=live["lightwalletd_server_info"]["anchor_hash"]),
            lambda live: live["negative_controls"][0].update(
                endpoint="http://127.0.0.1:23368"),
            lambda live: live["negative_controls"][0].update(tip_verified=False),
            lambda live: live["provenance"].update(probe_sha256="0" * 64),
            lambda live: live["provenance"].update(probe="other-probe.py"),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=index):
                self.reject("devtool", self.live_devtool(mutation))

    def test_missing_driver_source_cannot_verify_the_live_endpoint(self):
        driver = dict(self.campaign.sources["vizor"])["vizor-payment.rs"]
        driver.unlink()
        receipt = self.campaign.receipt_path("vizor")
        part = roles.counterparty_import(receipt, RELEASE)
        for row in part["rows"]:
            self.assertEqual(row["assertions"]["source-documents-verified"], "unexercised")
            self.assertEqual(row["assertions"]["live-server-identity-bound"], "unexercised")
        with self.assertRaisesRegex(ValueError, "provisional receipt"):
            roles.counterparty_import(receipt, RELEASE, allow_provisional=False)

    def test_stale_source_pin_downgrades_instead_of_claiming(self):
        self.campaign.reset()
        source = self.campaign.sources["vizor"][0][1]
        source.write_text(source.read_text() + "\n# changed\n")
        part = roles.counterparty_import(self.campaign.receipt_path("vizor"), RELEASE)
        self.assertEqual(part["rows"][0]["status"], "unexercised")
        self.assertEqual(
            part["rows"][0]["assertions"]["source-documents-verified"], "unexercised")
        self.assertIn("pinned payment-roles.py differs from the current source",
                      part["provisional"])
        self.assertEqual(
            part["rows"][0]["assertions"]["live-server-identity-bound"], "unexercised")


if __name__ == "__main__":
    unittest.main()
