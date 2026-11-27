#!/usr/bin/env python3
"""Check Zkool/Cake campaign binding, actor rows, and fail-closed imports."""
import copy
import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import coverage as cov
from coverage_evidence import digest
from coverage_zkool_payments import actions, case_ledger


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RELEASE = {"tag": "v0.22.0", "commit": "9" * 40}
ZKOOL_SHA = "98a03216d9975b4e1a485c1c2cf08ab9bc3a46e7b0716d83a2ad742ded74cafb"
CAKE_SHA = "13749e919020c9430792004287139480690918774cbc7fadfe594a658994a263"
ZALLET_SHA = "0851230a1971b6f3c12c9b42141b4d1c289384d78176dee67d2c87ed4a1d1508"
CASES = ("zkool-internal", "zkool-external", "cake")
LEGS = ("outbound", "sweep", "inbound", "spendback")
SERVICES = ("ingest", "projector", "query", "compat-lightwalletd")
ACTIVATION = {"Overwinter": 1, "Sapling": 1, "Blossom": 1, "Heartwood": 1, "Canopy": 1,
              "NU5": 1, "NU6": 1, "NU6.1": 1, "NU6.2": 1, "NU6.3": 250}
MEMOS = {"outbound": "zinder/zkool-cake-v1/outbound", "sweep": "zinder/zkool-cake-v1/sweep",
         "inbound": "zinder/zkool-cake-v1/inbound", "spendback": "zinder/zkool-cake-v1/spendback"}
VARIANTS = {"zkool-internal": "internal-enabled", "zkool-external": "external-only",
            "cake": "cake-engine-internal-enabled"}
OUTCOME_LEGS = ("tx_fingerprint", "mined_height", "amount_zat", "fee_zat", "memo_verified",
                "payer_before_zat", "payee_before_zat", "payer_after_zat", "payee_after_zat")

def token(label):
    return hashlib.sha256(label.encode()).hexdigest()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


UPSTREAM = {
    "zkool": {
        "version": "6.37.0", "executable_sha256": ZKOOL_SHA, "driver_sha256": "ce7fa2df0c398796e3c661720fef02b68f41f62468e419bc111ea4f21016e14a",
        "repository": "https://github.com/hhanh00/zkool2",
        "source_commit": "d80892f163bc650e3b453f448da8c83eb5c32ddc",
        "workspace_lock_sha256": "9fdaffdaab1e6eabe0a78ffc0669349f05893054eb83d0f0abd3f2a49a05fd0c",
        "toolchain": "cargo +1.99.0 / rustc 1.99.0",
        "build": "cargo +1.99.0 build --example zkool_payment",
    },
    "cake": {
        "version": "6.5.4-cb3e96f1", "executable_sha256": CAKE_SHA,
        "repository": "https://github.com/cake-tech/zkool2",
        "source_commit": "cb3e96f10e58a8fc8632b40636a0c64aa21fb653",
        "workspace_lock_sha256": "e58b4de00913ddcb33e0d4af56936872140cafa4e97407babffddb379de0a9a2",
        "toolchain": "cargo +1.99.0 / rustc 1.99.0",
        "build": "cargo +1.99.0 build --locked --bin zkool_graphql",
    },
    "zallet": {
        "executable_sha256": ZALLET_SHA,
        "repository": "https://github.com/zcash/zallet",
        "source_commit": "32488c5554e5e0275c3313b2fc06c00e6780746a",
        "source_lock_sha256": "bffcf554690cb9c24200e9bac9bb1668f63312d5af742ff6f7faaa51d09a332d",
        "toolchain": "cargo +1.95.0 launcher / cargo +1.100.0 zinder backend",
        "build": "cargo +1.100.0 build --manifest-path backends/zinder/Cargo.toml",
    },
}

def pins():
    harness = {name: token("harness/" + name) for name in (
        "zkool-payment-matrix.py", "zkool-payment.py", "cake-payment.py", "zkool-payment.rs",
        "roundtrip_wallets.py", "roundtrip_fixture.py", "wallet_fixture.py",
        "trace-wallet-endpoint.py", "run-wallet-roundtrip-regtest.py",
        "regtest-fixture.py", "regtest-fixture-relay.py")}
    return {"harness_sha256": harness,
            "executables_sha256": {name: entry["executable_sha256"]
                                   for name, entry in UPSTREAM.items()},
            "fixture_ready_sha256": token("fixture-ready")}


def leg(name, height, payer_before, amount, fee, payee_before, sender_spendable,
        receiver_spendable):
    txid = hashlib.sha256(("operation/" + name).encode()).hexdigest()
    fingerprint = hashlib.sha256(bytes.fromhex(txid)).hexdigest()
    return {"tx_fingerprint": fingerprint, "mined_height": height,
            "amount_zat": amount, "fee_zat": fee, "memo": MEMOS[name], "memo_verified": True,
            "payer_before_zat": payer_before, "payee_before_zat": payee_before,
            "payer_after_zat": payer_before - amount - fee,
            "payee_after_zat": payee_before + amount,
            "payer_scanned_height": height + 9, "payee_scanned_height": height + 9,
            "sender_spendable_zat": sender_spendable,
            "receiver_spendable_zat": receiver_spendable}


def legs():
    outbound = leg("outbound", 363, 500_000_000, 10_000_000, 2_000, 0,
                   489_998_000, 10_000_000)
    sweep = leg("sweep", 373, outbound["payer_after_zat"], 488_998_000, 2_000,
                outbound["payee_after_zat"], 998_000, 498_998_000)
    inbound = leg("inbound", 383, sweep["payee_after_zat"], 6_000_000, 20_000,
                  sweep["payer_after_zat"], 492_978_000, 6_998_000)
    spendback = leg("spendback", 393, inbound["payee_after_zat"], 3_000_000, 2_000,
                    inbound["payer_after_zat"], 3_996_000, 495_978_000)
    return {"outbound": outbound, "sweep": sweep, "inbound": inbound, "spendback": spendback}


def actor():
    pending = {"outbound": (False, True), "sweep": (False, True),
               "inbound": (True, True), "spendback": (False, True)}
    return {"pending_requirement": "node_mempool_only", "sender_pending_claimed": False,
            "legs": {name: {"node_mempool": True,
                            "wallet_under_test_pending_observed": pending[name][0],
                            "counterpart_pending_observed": pending[name][1]}
                     for name in LEGS}}


def preflight():
    anchor = {"height": 350, "hash": token("anchor")}
    return {
        "network": "zcash-regtest", "revision": RELEASE["commit"], "release": RELEASE["tag"],
        "anchor": anchor, "activation_heights": dict(ACTIVATION),
        "activation_schedule_sha256": token("schedule"), "port_base": 50600, "peer_count": 0,
        "node_tip": {"height": 393, "hash": token("node-tip")},
        "native_tip": {"height": 393, "hash": token("native-tip")},
        "node_image": "sha256:" + token("node-image"),
        "image_references": {s: f"offline-pin/zinder-{s}:0.22.0" for s in SERVICES},
        "images": {s: "sha256:" + token("image/" + s) for s in SERVICES},
        "image_manifest_sha256": token("image-manifest"), "release_binaries": {},
        "raw_blob_policy": "all",
    }


def fixture_recipe(pre):
    return {"starting_height": pre["node_tip"]["height"],
            "activation_schedule_sha256": pre["activation_schedule_sha256"],
            "activation_heights": pre["activation_heights"],
            "node_image": pre["node_image"], "image_references": pre["image_references"],
            "images": pre["images"], "raw_blob_policy": pre["raw_blob_policy"],
            "release_binaries": pre["release_binaries"]}


def result(case, spent_spend_height=True):
    endpoint = f"http://127.0.0.1:{50667 + 100 * CASES.index(case)}"
    counterpart = f"http://127.0.0.1:{50682 + 100 * CASES.index(case)}"
    if case == "cake":
        evidence = {"scope": "process-trace", "target_connected": True, "endpoint": endpoint,
                    "processes": 1, "trace_fingerprint_sha256": token("trace-fingerprint"),
                    "traces": [{"endpoint": endpoint, "connect_log_sha256": token("connect")}]}
    else:
        evidence = {"scope": "process-trace", "target_connected": True, "endpoint": endpoint,
                    "operations": 2, "trace_fingerprint_sha256": token("trace-fingerprint"),
                    "traces": [{"operation": "create", "endpoint": endpoint,
                                "connect_log_sha256": token("connect/create")},
                               {"operation": "send", "endpoint": endpoint,
                                "connect_log_sha256": token("connect/send")}]}
    note = {"created_height": 383, "value_zat": 6_000_000}
    if spent_spend_height:
        note["spend_height"] = 393
    return {
        "funding": {"height": 261, "spendable_zat": 499_985_000, "pools": ["sapling"]},
        "incoming_note_spent": {"notes": 1, "spent": [note], "spending_height": 393},
        "reopen_ledger_verified": True, "restart_verified": True,
        "restart_memo_exposure": {"outbound": True, "sweep": True, "inbound": True,
                                  "spendback": case != "cake"},
        "wallet_after_reopen_zat": 3_996_000, "counterpart_after_reopen_zat": 495_978_000,
        "endpoint_evidence": evidence, "counterpart_endpoint": counterpart,
        "counterpart_endpoint_evidence": {"scope": "persisted-config",
                                          "endpoint": counterpart,
                                          "config_sha256": token("zallet-config")},
    }


def project_outcome(receipt):
    body = receipt["result"]
    return {
        "first_incoming_note_spent": body["incoming_note_spent"],
        "restart": {"reopen_ledger_verified": body["reopen_ledger_verified"],
                    "restart_verified": body["restart_verified"],
                    "wallet_after_reopen_zat": body["wallet_after_reopen_zat"],
                    "counterpart_after_reopen_zat": body["counterpart_after_reopen_zat"],
                    "memo_exposure": body["restart_memo_exposure"]},
        "legs": {name: {key: receipt["legs"][name][key] for key in OUTCOME_LEGS}
                 for name in LEGS},
        "actor_observations": receipt["actor_observations"],
    }


def project_provenance(receipt, images=True):
    pre = receipt["preflight"]
    fixture = {"network": pre["network"], "anchor": pre["anchor"],
               "starting_height": pre["node_tip"]["height"],
               "node_image": pre["node_image"],
               "image_references": pre["image_references"],
               "image_manifest_sha256": pre["image_manifest_sha256"],
               "activation_schedule_sha256": pre["activation_schedule_sha256"],
               "raw_blob_policy": pre["raw_blob_policy"]}
    if images:
        fixture["images"] = pre["images"]
    return {
        "release": receipt["release"], "upstream": receipt["upstream"],
        "fixture": fixture,
        "wallets": receipt["wallets"],
        "wallet_identity_fingerprint": receipt["wallet_identity"],
        "counterpart_identity_fingerprint": receipt["counterpart_identity"],
        "pins": receipt["pins"], "harness_sha256": receipt["harness_sha256"],
        "binaries": receipt["binaries"],
    }


def survivors():
    return {"scope": "owned-case-processes", "found": 0, "stopped": 0, "remaining": 0,
            "errors": []}


def measure(legacy=False):
    value = {"fixture_started": True, "down_error": None,
             "project_resources_remaining": 0, "relay_running": False,
             "fixture_removed": True, "retry": False}
    if not legacy:
        value["wallet_survivors"] = survivors()
    return value


def wallet_cleanup():
    return {"wallet_stopped": True, "counterpart_stopped": True, "errors": [],
            "counterpart": {"scope": "daemon-identity", "processes_remaining": 0, "pid": None},
            "wallet": {"scope": "no-resident-process", "processes_remaining": 0},
            "verified": True}


class Campaign:
    def __init__(self, root, legacy=False):
        self.root = root
        self.legacy = legacy
        self.receipts = {case: self.write_case(case) for case in CASES}
        self.cleanup = {case: measure(legacy) for case in CASES}
        self.save()

    def write_case(self, case):
        state = self.root / case
        state.mkdir(parents=True)
        pre = preflight()
        write_json(state / "preflight.json", pre)
        journal_legs = legs()
        journal_observations = {}
        journal_operations = {}
        pending_observations = {
            "outbound": (False, True), "sweep": (False, True),
            "inbound": (True, True), "spendback": (False, True),
        }
        for name, entry in journal_legs.items():
            txid = hashlib.sha256(("operation/" + name).encode()).hexdigest()
            journal_observations[name] = {
                **{key: entry[key] for key in ("amount_zat", "fee_zat", "memo", "memo_verified",
                                               "payer_before_zat", "payer_after_zat", "payee_before_zat",
                                               "payee_after_zat", "sender_spendable_zat",
                                               "receiver_spendable_zat", "payer_scanned_height",
                                               "payee_scanned_height")},
                "txid": txid, "block": {"height": entry["mined_height"]},
            }
            journal_operations[name] = {"txid": txid}
            wallet_pending, counterpart_pending = pending_observations[name]
            journal_observations[name + "-pending"] = {
                "node_mempool": True,
                "payer": wallet_pending if name != "inbound" else counterpart_pending,
                "payee": counterpart_pending if name != "inbound" else wallet_pending,
            }
        journal_observations["incoming-note-spent"] = result(case)["incoming_note_spent"]
        write_json(state / "run.json", {"schema_version": 1, "case": case,
                                         "observations": journal_observations,
                                         "operations": journal_operations})
        write_json(state / "cleanup.json", wallet_cleanup())
        (state / "zallet").mkdir()
        (state / "zallet" / "zallet.toml").write_text('network = "regtest"\n')
        engine = "zkool" if case.startswith("zkool") else "cake"
        traces = []
        for index in range(1, 3 if engine == "zkool" else 2):
            trace = state / "evidence" / engine / f"trace-{index}"
            trace.mkdir(parents=True, exist_ok=True)
            write_json(trace / "report.json", {"status": "passed", "fallback_used": False})
            (trace / "connect.log").write_text("connect\n")
            traces.append({"report": f"evidence/{engine}/trace-{index}/report.json",
                           "connect_log": f"evidence/{engine}/trace-{index}/connect.log",
                           "report_sha256": digest(trace / "report.json"),
                           "connect_log_sha256": digest(trace / "connect.log")})
        snapshot = pins()
        receipt = {
            "status": "passed", "schema_version": 1, "case": case, "network": "regtest",
            "release": {"tag": RELEASE["tag"], "revision": RELEASE["commit"]},
            "upstream": copy.deepcopy(UPSTREAM), "pins": {"pre": snapshot, "post": snapshot},
            "harness_sha256": snapshot["harness_sha256"],
            "binaries": snapshot["executables_sha256"],
            "inputs": {"case": case, "ready_sha256": snapshot["fixture_ready_sha256"],
                       "revision": RELEASE["commit"], "release": RELEASE["tag"],
                       "outbound_zat": 10_000_000, "inbound_zat": 6_000_000,
                       "spendback_zat": 3_000_000, "pins_sha256": fingerprint(snapshot)},
            "preflight": pre, "fixture_recipe": fixture_recipe(pre),
            "wallets": {"under_test": VARIANTS[case], "counterpart": "ordinary-zallet"},
            "wallet_identity": token("wallet/" + case),
            "counterpart_identity": token("zallet/" + case),
            "legs": journal_legs, "actor_observations": actor(), "result": result(case),
            "cleanup": wallet_cleanup(),
            "evidence": {"preflight_sha256": digest(state / "preflight.json"),
                         "journal_sha256": digest(state / "run.json"),
                         "cleanup_sha256": digest(state / "cleanup.json"),
                         "counterpart_config": {"path": "zallet/zallet.toml",
                                                "sha256": digest(state / "zallet" / "zallet.toml")},
                         "traces": traces},
            "limits": ["Regtest only; the fixture is peer-isolated."],
        }
        write_json(state / "receipt.json", receipt)
        return receipt

    def save(self):
        write_json(self.root / "cleanup.json", self.cleanup)
        files = []
        for path in sorted(self.root.rglob("*")):
            relative = str(path.relative_to(self.root))
            if not path.is_file() or relative in {"summary.json", "evidence-index.json"}:
                continue
            files.append({"path": relative, "bytes": path.stat().st_size,
                          "sha256": digest(path)})
        write_json(self.root / "evidence-index.json",
                   {"schema_version": 1, "files": files, "count": len(files)})
        cases = {}
        for case in CASES:
            receipt = self.receipts[case]
            state = self.root / case
            cases[case] = {
                "status": "passed", "receipt": f"{case}/receipt.json",
                "receipt_sha256": digest(state / "receipt.json"),
                "case_cleanup": f"{case}/cleanup.json",
                "case_cleanup_sha256": digest(state / "cleanup.json"),
                "cleanup": self.cleanup[case], "outcome": project_outcome(receipt),
                "provenance": project_provenance(receipt, not self.legacy)}
        summary = {
            "schema_version": 1, "kind": "zkool-cake-payment-campaign", "status": "passed",
            "release": {"tag": RELEASE["tag"], "revision": RELEASE["commit"]},
            "port_bases": {"zkool-internal": 50600, "zkool-external": 50700, "cake": 50800},
            "cases": cases, "cleanup": self.cleanup, "failures": {},
            "limits": ["Regtest only; the fixture is peer-isolated."],
            "evidence": {"index": "evidence-index.json",
                         "index_sha256": digest(self.root / "evidence-index.json"),
                         "cleanup": "cleanup.json",
                         "cleanup_sha256": digest(self.root / "cleanup.json"),
                         "receipts": {case: cases[case]["receipt_sha256"] for case in CASES}},
        }
        write_json(self.root / "summary.json", summary)
        return self.root / "summary.json"

    def mutate(self, case, change):
        receipt = copy.deepcopy(self.receipts[case])
        change(receipt)
        self.receipts[case] = receipt
        write_json(self.root / case / "receipt.json", receipt)
        self.save()

    def mutate_file(self, relative, change):
        path = self.root / relative
        value = path.read_text()
        path.write_text(change(value))
        self.save()

    def mutate_journal(self, case, change):
        path = self.root / case / "run.json"
        journal = json.loads(path.read_text())
        change(journal)
        write_json(path, journal)
        self.receipts[case]["evidence"]["journal_sha256"] = digest(path)
        write_json(self.root / case / "receipt.json", self.receipts[case])
        self.save()

    def mutate_summary(self, change):
        summary = json.loads((self.root / "summary.json").read_text())
        change(summary)
        write_json(self.root / "summary.json", summary)


class ZkoolPaymentImport(unittest.TestCase):
    def setUp(self):
        self.fresh()

    def fresh(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.campaign = Campaign(self.root)
        self.summary = self.root / "summary.json"

    def rows(self):
        return actions(self.summary, RELEASE)

    def validate(self, rows):
        matrix = {"schema_version": 1, "release": RELEASE,
                  "inventory": {"lightwalletd": ["GetBlock"],
                                "native-wallet-query": ["VisibleTipBlock"]},
                  "scenarios": rows}
        return cov.validate(matrix)

    def rejects(self):
        return self.assertRaises(ValueError)

    def test_cake_spendback_rejects_early_wallet_accounting(self):
        for key, value, message in (
            ("receiver_spendable_zat", 492_978_000, "not fully spendable"),
            ("sender_spendable_zat", 3_995_999, "not fully spendable"),
            ("payer_scanned_height", 401, "ten confirmations"),
            ("payee_scanned_height", 403, "ten confirmations"),
        ):
            with self.subTest(key=key):
                receipt = self.campaign.receipts["cake"]
                with patch.dict(receipt["legs"]["spendback"], {key: value}):
                    with self.assertRaisesRegex(ValueError, message):
                        case_ledger(receipt, receipt["inputs"])

    def test_actor_rows_bind_legs_pins_and_payer_accounting(self):
        rows = self.rows()
        self.assertEqual(len(self.validate(rows)["scenarios"]), 24)
        self.assertEqual(len({r["id"] for r in rows}), 24)
        for case in CASES:
            by_id = {r["id"]: r for r in rows}
            for leg in LEGS:
                for role in ("payer", "recipient"):
                    row = by_id[f"zkool-cake-{case}-{leg}-{role}"]
                    tested = (role == "payer") != (leg == "inbound")
                    self.assertEqual(row["interface"], "lightwalletd" if tested else "native-wallet-query")
                    self.assertEqual(row["method_scope"], "none")
                    self.assertFalse(row["methods"])
                    entry = self.campaign.receipts[case]["legs"][leg]
                    self.assertEqual(row["accounting_zat"]["fee"], entry["fee_zat"] if role == "payer" else 0)
                    self.assertEqual(row["evidence"]["sha256"], digest(self.root / case / "receipt.json"))
        self.assertEqual(rows, self.rows())

    def test_receiver_pending_flags_change_with_the_observed_actor(self):
        def swap(receipt):
            receipt["actor_observations"]["legs"]["outbound"].update(
                wallet_under_test_pending_observed=True,
                counterpart_pending_observed=False)
        self.campaign.mutate("zkool-internal", swap)
        self.campaign.mutate_journal(
            "zkool-internal",
            lambda journal: journal["observations"]["outbound-pending"].update(
                payer=True, payee=False))
        rows = self.rows()
        self.assertEqual(rows[0]["assertions"]["own-pending-observed"], "pass")
        self.assertEqual(rows[1]["assertions"]["own-pending-observed"], "unexercised")

    def test_unregistered_digest_drops_the_version_claim(self):
        unknown = token("rebuilt-cake")
        self.campaign.mutate("cake", lambda receipt: (
            receipt["binaries"].update(cake=unknown),
            receipt["pins"]["pre"]["executables_sha256"].update(cake=unknown),
            receipt["pins"]["post"]["executables_sha256"].update(cake=unknown),
            receipt["inputs"].update(
                pins_sha256=fingerprint(receipt["pins"]["pre"])),
            receipt["upstream"].update(cake={"executable_sha256": unknown})))
        row = next(r for r in self.rows() if r["wallet"] == "cake")
        self.assertEqual(row["version"], "binary " + unknown[:12])
        self.assertNotIn("cake@", row["fixture"])
        self.validate(self.rows())

    def test_unverified_version_claim_rejects(self):
        unknown = token("rebuilt-cake")
        self.campaign.mutate("cake", lambda receipt: (
            receipt["binaries"].update(cake=unknown),
            receipt["pins"]["pre"]["executables_sha256"].update(cake=unknown),
            receipt["pins"]["post"]["executables_sha256"].update(cake=unknown),
            receipt["inputs"].update(
                pins_sha256=fingerprint(receipt["pins"]["pre"])),
            receipt["upstream"]["cake"].update(executable_sha256=unknown)))
        with self.rejects():
            self.rows()

    def test_case_set_shape_and_release_reject(self):
        mutations = (
            lambda s: s.pop("cases"),
            lambda s: s["cases"].pop("cake"),
            lambda s: s["cases"].update(extra=copy.deepcopy(s["cases"]["cake"])),
            lambda s: s["cases"].update(
                {"zkool-internal": copy.deepcopy(s["cases"]["zkool-external"]),
                 "zkool-external": copy.deepcopy(s["cases"]["zkool-internal"])}),
            lambda s: s["cases"]["cake"].update(status="failed"),
            lambda s: s.update(status="failed"),
            lambda s: s.update(failures={"cake": "boom"}),
            lambda s: s.update(kind="other-campaign"),
            lambda s: s["release"].update(tag="v0.22.1"),
            lambda s: s["release"].update(revision="8" * 40),
            lambda s: s["limits"].clear(),
            lambda s: s["port_bases"].update(cake=50600),
            lambda s: s["cases"]["cake"].update(receipt="zkool-internal/receipt.json"),
            lambda s: s["cases"]["cake"].update(case_cleanup="cake/receipt.json"),
            lambda s: s["cases"]["cake"]["cleanup"].update(fixture_removed=False),
            lambda s: s["cases"]["cake"]["cleanup"].update(project_resources_remaining=1),
            lambda s: s["cases"]["cake"]["cleanup"].update(relay_running=True),
            lambda s: s["cases"]["cake"]["cleanup"].update(down_error="down failed"),
            lambda s: s["cases"]["cake"]["cleanup"].update(retry=True),
            lambda s: s["cleanup"]["cake"].update(retry=True),
            lambda s: s["evidence"].update(index_sha256="0" * 64),
            lambda s: s["evidence"].update(cleanup_sha256="0" * 64),
            lambda s: s["evidence"]["receipts"].update(cake="0" * 64),
            lambda s: s["cases"]["cake"].update(receipt_sha256="0" * 64),
            lambda s: s["cases"]["cake"].update(case_cleanup_sha256="0" * 64),
            lambda s: s["cases"]["cake"].pop("provenance"),
            lambda s: s["cases"]["cake"].pop("outcome"),
            lambda s: s["cases"]["cake"]["provenance"]["fixture"]["images"].update(
                query="sha256:" + "0" * 64),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.fresh()
                self.campaign.mutate_summary(change)
                with self.rejects():
                    self.rows()

    def test_evidence_index_rejects_unsafe_or_changed_entries(self):
        for change in ("changed", "count", "escape"):
            with self.subTest(change=change):
                self.fresh()
                index = self.root / "evidence-index.json"
                body = json.loads(index.read_text())
                if change == "changed":
                    body["files"][0]["sha256"] = "0" * 64
                elif change == "count":
                    body["files"].pop()
                else:
                    body["files"].append({"path": "../escape", "bytes": 1,
                                          "sha256": "0" * 64})
                    body["count"] += 1
                write_json(index, body)
                self.campaign.mutate_summary(
                    lambda summary: summary["evidence"].update(
                        index_sha256=digest(index)))
                with self.rejects():
                    self.rows()

    def test_accepts_unified_zinder_image_layout_and_rejects_mixed(self):
        import coverage_zkool_payments as zp
        unified_pre = preflight()
        unified_pre["images"] = {"zinder": "sha256:" + token("image/zinder")}
        unified_pre["image_references"] = {"zinder": "offline-pin/zinder:0.22.0"}
        zp.preflight(unified_pre, RELEASE)
        zp.recipe(fixture_recipe(unified_pre), unified_pre)

        mixed_pre = copy.deepcopy(unified_pre)
        mixed_pre["image_references"] = {
            s: f"offline-pin/zinder-{s}:0.22.0" for s in SERVICES
        }
        with self.assertRaisesRegex(ValueError, "image references and IDs"):
            zp.preflight(mixed_pre, RELEASE)

    def test_pins_inputs_and_release_binding_reject(self):
        mutations = (
            lambda r: r["pins"]["post"]["executables_sha256"].update(zkool="0" * 64),
            lambda r: r["pins"].pop("post"),
            lambda r: r["binaries"].update(zkool="0" * 64),
            lambda r: r["inputs"].update(outbound_zat=10_000_001),
            lambda r: r["inputs"].update(inbound_zat=0),
            lambda r: r["inputs"].update(pins_sha256="0" * 64),
            lambda r: r["inputs"].update(ready_sha256="0" * 64),
            lambda r: r["release"].update(revision="8" * 40),
            lambda r: r["upstream"]["zkool"].update(version="6.99.0"),
            lambda r: r["upstream"]["zkool"].update(source_commit="0" * 40),
            lambda r: r["upstream"]["zkool"].update(workspace_lock_sha256="0" * 64),
            lambda r: r["upstream"]["zkool"].update(executable_sha256="0" * 64),
            lambda r: r["preflight"]["image_references"].update(
                ingest="offline-pin/zinder-ingest:0.22.1"),
            lambda r: r["preflight"]["activation_heights"].update({"NU6.3": 1}),
            lambda r: r["preflight"].update(raw_blob_policy="compact"),
            lambda r: r["preflight"].update(peer_count=1),
            lambda r: r["preflight"].update(node_image="sha256:short"),
            lambda r: r["fixture_recipe"].update(starting_height=1),
            lambda r: r["wallets"].update(counterpart="zingo-cli"),
            lambda r: r["wallets"].update(under_test="external-only"),
            lambda r: r.update(counterpart_identity=r["wallet_identity"]),
            lambda r: r["cleanup"].update(verified=False),
            lambda r: r["cleanup"]["counterpart"].update(processes_remaining=1),
            lambda r: r["cleanup"].update(wallet_stopped=False),
            lambda r: r["evidence"].update(preflight_sha256="0" * 64),
            lambda r: r["evidence"]["counterpart_config"].update(
                path="other/zallet.toml"),
            lambda r: r["evidence"]["traces"][0].update(report="../../escape.json"),
            lambda r: r["result"]["endpoint_evidence"].update(target_connected=False),
            lambda r: r["result"]["counterpart_endpoint_evidence"].update(scope="other"),
            lambda r: r["inputs"].update(revision="8" * 40),
            lambda r: r.update(network="testnet"),
            lambda r: r["pins"]["pre"]["harness_sha256"].update(
                {"scripts/other.py": "0" * 64}),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.fresh()
                self.campaign.mutate("zkool-internal", change)
                with self.rejects():
                    self.rows()

    def test_missing_required_runtime_harness_pin_rejects(self):
        def remove_pin(receipt):
            receipt["pins"]["pre"]["harness_sha256"].pop("regtest-fixture.py")
            receipt["inputs"]["pins_sha256"] = fingerprint(receipt["pins"]["pre"])

        self.campaign.mutate("zkool-internal", remove_pin)
        with self.rejects():
            self.rows()

    def test_trace_attribution_and_evidence_files_reject(self):
        self.campaign.mutate_file(
            "zkool-internal/evidence/zkool/trace-1/report.json",
            lambda text: json.dumps({"status": "passed", "fallback_used": True}))
        with self.rejects():
            self.rows()

    def test_balance_note_restart_and_actor_inconsistency_reject(self):
        mutations = (
            lambda r: r["legs"]["outbound"].update(payer_after_zat=489_998_001),
            lambda r: r["legs"]["sweep"].update(payee_after_zat=498_998_001),
            lambda r: r["legs"]["inbound"].update(payee_before_zat=998_001),
            lambda r: r["legs"]["inbound"].update(amount_zat=6_000_001),
            lambda r: r["legs"]["sweep"].update(mined_height=383),
            lambda r: r["legs"]["spendback"].update(
                tx_fingerprint=r["legs"]["inbound"]["tx_fingerprint"]),
            lambda r: r["legs"]["outbound"].update(fee_zat=0),
            lambda r: r["legs"]["outbound"].update(memo="other"),
            lambda r: r["legs"]["inbound"].update(receiver_spendable_zat=1),
            lambda r: r["result"]["incoming_note_spent"]["spent"][0].update(
                created_height=384),
            lambda r: r["result"]["incoming_note_spent"]["spent"][0].update(value_zat=1),
            lambda r: r["result"]["incoming_note_spent"]["spent"][0].update(
                spend_height=392),
            lambda r: r["result"]["incoming_note_spent"].update(spending_height=392),
            lambda r: r["result"]["incoming_note_spent"].update(notes=2),
            lambda r: r["result"]["incoming_note_spent"].update(spent=[]),
            lambda r: r["result"].update(wallet_after_reopen_zat=3_996_001),
            lambda r: r["result"].update(counterpart_after_reopen_zat=495_978_001),
            lambda r: r["result"].update(restart_verified=False),
            lambda r: r["result"].update(reopen_ledger_verified=False),
            lambda r: r["result"]["restart_memo_exposure"].update(outbound=False),
            lambda r: r["result"]["restart_memo_exposure"].update(inbound=False),
            lambda r: r["actor_observations"]["legs"]["outbound"].update(node_mempool=False),
            lambda r: r["actor_observations"].update(sender_pending_claimed=True),
            lambda r: r["actor_observations"].update(pending_requirement="wallet-pending"),
            lambda r: r["actor_observations"]["legs"]["outbound"].update(
                wallet_under_test_pending_observed="yes"),
            lambda r: r["actor_observations"]["legs"].pop("spendback"),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.fresh()
                self.campaign.mutate("zkool-internal", change)
                with self.rejects():
                    self.rows()

    def test_receipt_cannot_restate_hashed_journal_observations(self):
        mutations = (
            lambda r: r["legs"]["outbound"].update(tx_fingerprint=token("other-tx")),
            lambda r: r["legs"]["outbound"].update(mined_height=364),
            lambda r: r["actor_observations"]["legs"]["outbound"].update(
                counterpart_pending_observed=False),
            lambda r: r["result"]["incoming_note_spent"]["spent"][0].update(value_zat=5_999_999),
        )
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                self.fresh()
                self.campaign.mutate("zkool-internal", change)
                with self.rejects():
                    self.rows()

    def test_fee_change_that_reconciles_balances_still_changes_projection(self):
        self.fresh()

        def change(receipt):
            leg = receipt["legs"]["spendback"]
            leg["fee_zat"] += 1_000
            leg["payer_after_zat"] -= 1_000
            receipt["result"]["wallet_after_reopen_zat"] -= 1_000

        self.campaign.mutate("zkool-internal", change)
        receipt = self.campaign.receipts["zkool-internal"]
        leg = receipt["legs"]["spendback"]
        self.assertEqual(leg["payer_after_zat"],
                         leg["payer_before_zat"] - leg["amount_zat"] - leg["fee_zat"])
        summary = json.loads((self.root / "summary.json").read_text())
        self.assertEqual(summary["cases"]["zkool-internal"]["outcome"]["legs"]["spendback"]["fee_zat"],
                         3_000)
        with self.rejects():
            self.rows()

    def test_receipt_and_summary_disagreement_rejects(self):
        summary = json.loads(self.summary.read_text())
        summary["cases"]["zkool-internal"]["outcome"]["legs"]["outbound"]["fee_zat"] = 2_001
        write_json(self.summary, summary)
        with self.rejects():
            self.rows()

    def test_case_process_survivors_reject(self):
        self.campaign.cleanup["cake"]["wallet_survivors"].update(found=1, remaining=1)
        self.campaign.save()
        with self.rejects():
            self.rows()

    def test_campaign_without_optional_provenance_and_survivor_fields_imports(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        campaign = Campaign(Path(temporary.name), legacy=True)
        rows = actions(campaign.root / "summary.json", RELEASE)
        self.assertEqual(len(rows), 24)
        self.assertEqual([row["status"] for row in rows], ["pass"] * 24)

    def test_substituted_case_receipts_reject(self):
        first = self.root / "zkool-internal" / "receipt.json"
        second = self.root / "zkool-external" / "receipt.json"
        left, right = first.read_bytes(), second.read_bytes()
        first.write_bytes(right)
        second.write_bytes(left)
        summary = json.loads(self.summary.read_text())
        for case, path in (("zkool-internal", first), ("zkool-external", second)):
            summary["cases"][case]["receipt_sha256"] = digest(path)
            summary["evidence"]["receipts"][case] = digest(path)
        summary["evidence"]["index_sha256"] = digest(self.root / "evidence-index.json")
        write_json(self.summary, summary)
        with self.rejects():
            self.rows()


if __name__ == "__main__":
    unittest.main()
