#!/usr/bin/env python3
"""Check recovery receipt binding, actor rows, and unexercised gaps."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import coverage as cov
from coverage_evidence import digest
from coverage_payment_recovery import recovery_actions


RELEASE = {"tag": "v1.0.0", "commit": "1" * 40}
ZINGO = "2" * 64
ZALLET = "3" * 64
SCHEDULE = "4" * 64
NODE_IMAGE = "sha256:" + "5" * 64
BINARIES = ("compat-lightwalletd", "ingest", "projector", "query")
MEMO = "zinder-payment-recovery-v1/outbound"
ACCOUNTING = {"amount_zat": 10_000_000, "fee_zat": 10_000,
              "payee_after_zat": 10_000_000, "payee_credit_zat": 10_000_000,
              "payer_after_zat": 489_975_000, "payer_debit_zat": 10_010_000}
RETURN_ACCOUNTING = {"amount_zat": 4_000_000, "fee_zat": 25_000,
                     "payee_after_zat": 493_975_000, "payee_credit_zat": 4_000_000,
                     "payer_after_zat": 5_975_000, "payer_debit_zat": 4_025_000}


def token(label):
    return hashlib.sha256(label.encode()).hexdigest()


def surfaces(height, label):
    block = token(f"surface-{label}-{height}")
    tip = {"height": height, "hash": block}
    return {"height": height, "hash": block,
            "surfaces": {"lightwalletd": dict(tip), "native": dict(tip)}}


def leg(txid_label, height, accounting, surface_height):
    return {"tx_fingerprint": token(txid_label), "mined_height": height,
            "block_hash": token(f"block-{txid_label}"),
            "pending": dict.fromkeys(("node_mempool", "payee", "payer"), True),
            "surfaces": surfaces(surface_height, txid_label), "accounting": accounting}


def base(case):
    fixture = {"release_revision": RELEASE["commit"], "raw_blob_policy": "all",
               "configuration_sha256": token("configuration"), "activation_schedule_sha256": SCHEDULE, "ready_sha256": token("ready"),
               "node_image": NODE_IMAGE, "release_binary_sha256": dict.fromkeys(BINARIES, token("bin")),
               "anchor": {"height": 150, "hash": token("anchor")},
               "image_ids": dict.fromkeys(BINARIES, NODE_IMAGE), "project": f"disposable-{case}"}
    anchor = fixture["anchor"]
    sources = ["payment-recovery.py", "regtest-fixture.py", "regtest-fixture-relay.py",
               "run-wallet-roundtrip.py", "roundtrip_fixture.py", "roundtrip_wallets.py", "wallet_fixture.py",
               "run-wallet-roundtrip-regtest.py", "coverage_roundtrip.py", "coverage_evidence.py",
               "docker-compose.yml", "regtest-nu6-at-one.toml", "service_pb2.py", "compact_formats_pb2.py",
               "wallet_pb2.py", "server_info_pb2.py"]
    if case == "transport":
        sources.append("transport-payment.py")
    pins = {"wallets": {"zingo": ZINGO, "zallet_backend": ZALLET},
            "harness": {name: token(name) for name in sources}}
    pins["harness"]["regtest-nu6-at-one.toml"] = SCHEDULE
    return {
        "schema_version": 1, "case": case, "status": "passed", "network": "zcash-regtest",
        "fixture": fixture, "pins": {"pre": copy.deepcopy(pins), "post": pins},
        "clients": {"payer": "zingo-cli", "payee": "zallet-zinder",
                    "zingo_sha256": ZINGO, "zallet_sha256": ZALLET,
                    "wallet_identity_sha256": [token("zingo-wallet"), token("zallet-wallet")]},
        "funding": {"source": "regtest-coinbase-and-shield", "spendable_after_zat": 499_985_000,
                    "mined_height": 200, "tx_fingerprint": token("funding")},
        "initial_wallet_heights": {"zingo": 150, "zallet": 150},
        "preflight": {"compat_tip": dict(anchor), "native_tip": dict(anchor),
                      "node_tip": dict(anchor), "sha256": token("preflight")},
        "limits": ["Regtest only; the fixture is peer-isolated and changes no public network."],
        "cleanup": {"fixture_removed": True, "project_resources_remaining": 0},
        "scenario": {"outbound": dict(leg("outbound", 210, ACCOUNTING, 210),
                                      spendable_after_zat=10_000_000),
                     "return": dict(leg("return", 220, RETURN_ACCOUNTING, 220),
                                    spendable_after_zat=493_975_000),
                     "persistence": {"zingo_total_zat": 493_975_000, "zallet_total_zat": 5_975_000,
                                     "zingo_height": 230, "zallet_height": 230}},
    }


def disconnect_receipt():
    result = base("disconnect")
    mined = {"height": 210, "block_hash": result["scenario"]["outbound"]["block_hash"],
             "confirmations": 10}
    disconnect = {"last_online_heights": {"zallet": 209, "zingo": 209},
                  "mempool_before_mining": True, "mined_while_disconnected": mined,
                  "node_tip_while_disconnected": 219,
                  "surfaces_while_disconnected": surfaces(219, "disconnect"),
                  "wallet_scan_delta": {"GetLatestBlock/ok": 5},
                  "zallet": "daemon stopped and its process identity was cleared",
                  "zallet_process_absent": True,
                  "zingo": "not invoked; the CLI has no resident process"}
    result["disconnect"] = disconnect
    result["scenario"]["disconnect"] = copy.deepcopy(disconnect)
    result["scenario"]["reconnect"] = {"zingo_height": 219, "zallet_height": 219,
                                       "raw_sha256": token("signed-bytes")}
    result["assertions"] = dict.fromkeys((
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "no_wallet_scan_during_disconnect",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "received_funds_spendable", "transfer_mined_and_canonical_while_disconnected",
        "wallets_disconnected_while_pending"), True)
    return result


def rollback_receipt():
    result = base("rollback")
    outbound = result["scenario"]["outbound"]
    outbound["surfaces"] = surfaces(212, "rollback")
    orphan = {"old_height": 210, "old_block_hash": outbound["block_hash"],
              "old_block_state": {"header_available": False},
              "old_chain_tip_status": "unsupported", "tx_absent_from_active_chain": True,
              "automatically_readded_to_mempool": False, "restart_used": False,
              "readded_after_recovery_attempts": False,
              "empty_replacement_height": 210,
              "empty_replacement_hash": token("empty-replacement"),
              "empty_replacement_excludes_tx": True}
    remine = {"tx_fingerprint": outbound["tx_fingerprint"], "old_height": 210,
              "new_height": 211, "old_block_hash": outbound["block_hash"],
              "new_block_hash": token("remine-block"),
              "raw_sha256": token("raw-remine"), "raw_sha256_after": token("raw-remine"),
              "membership_count": 1, "empty_replacement": True, "height_changed": True}
    result["scenario"].update({
        "orphan": orphan,
        "readmission": {"method": "sendrawtransaction-same-bytes", "mempool_present": True,
                        "raw_sha256": token("raw-remine")},
        "remine": remine,
        "replacement": {"tip_height": 212, "tip_hash": token("replacement-tip"),
                        "surfaces": {"lightwalletd": {"height": 212, "hash": token("replacement-tip")},
                                     "native": {"height": 212, "hash": token("replacement-tip")}},
                        "wallet_old_height": 210, "wallet_new_height": 211,
                        "wallet_observed_height_change": True},
        "accounting_after_remine": copy.deepcopy(ACCOUNTING),
        "accounting_equal_before_and_after": True,
        "maturity": {"spendable_after_zat": 10_000_000},
    })
    result["scenario"]["return"]["mined_height"] = 224
    result["scenario"]["return"]["surfaces"] = surfaces(224, "return")
    result["assertions"] = dict.fromkeys((
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "old_block_removed_from_active_chain",
        "orphaned_transfer_absent_from_active_chain",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "readmission_observed", "received_funds_spendable", "remine_same_signed_bytes",
        "replacement_block_contains_transfer_once", "empty_replacement_excludes_transfer",
        "remine_at_different_height"), True)
    return result


def transport_receipt():
    result = base("transport")
    result["clients"]["native_endpoint"] = "private-loopback-native-relay"
    result["transport"] = {"endpoint": "private-loopback-native-relay", "only_native_route": True,
                           "port_base": 21000,
                           "started": {"endpoint": "same-loopback-native-relay"},
                           "restored": {"endpoint": "same-loopback-native-relay"}}
    outage = {"native_route_reachable": False, "direct_native_reachable": True,
              "fixture_relay_unchanged": True,
              "relay": {"exit_code": -15, "exit_signal": "SIGTERM"},
              "zallet_process_unchanged": True,
              "zallet_status_while_down": {"available": False, "error": "WalletRpcError"},
              "inclusion": {"height": 210,
                            "block_hash": result["scenario"]["outbound"]["block_hash"],
                            "confirmations": 10},
              "node_tip": 219, "transfer_in_mempool_after_mining": False,
              "surfaces": surfaces(219, "outage")}
    result["outage"] = outage
    result["scenario"]["outage"] = copy.deepcopy(outage)
    result["scenario"]["outbound"].update({
        "received_amount_zat": 10_000_000, "received_memos": [MEMO],
        "memo_verified": True, "fully_synced_height": 219})
    result["scenario"]["restore"] = {"endpoint": "same-loopback-native-relay",
                                     "same_process_reconnect": True}
    result["same_process"] = {"height": 210, "process_identical": True,
                              "reconnected": True, "state": "confirmed_exactly_once"}
    result["assertions"] = dict.fromkeys((
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "memo_verified", "native_route_down_before_reconnect",
        "node_and_other_wallet_routes_stayed_up", "only_the_private_native_route_was_stopped",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "received_funds_spendable", "same_endpoint_restored",
        "same_process_reconnected_without_restart",
        "transfer_mined_and_canonical_while_route_down",
        "zallet_process_alive_and_identical_through_outage"), True)
    return result


CASES = {"disconnect": disconnect_receipt, "rollback": rollback_receipt,
         "transport": transport_receipt}


class RecoveryImport(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.paths, self.receipts = {}, {}
        for case, make in CASES.items():
            self.receipts[case] = make()
            self.paths[case] = self.write(case, self.receipts[case])

    def write(self, case, receipt):
        path = self.root / f"{case}.json"
        path.write_text(json.dumps(receipt))
        return path

    def rows(self, case):
        return list(recovery_actions(self.paths[case], RELEASE))

    def test_comparison_distinguishes_fixture_inputs_and_release_under_test(self):
        case = "disconnect"
        initial = self.receipts[case]

        def matrix(doc, release=RELEASE):
            return {"inventory": {}, "release": release,
                    "scenarios": list(recovery_actions(self.write(case, doc), release))}

        baseline = matrix(initial)
        for key, changed in (("image_ids", dict(initial["fixture"]["image_ids"], query="sha256:" + token("other"))),
                             ("configuration_sha256", token("different configuration")),
                             ("release_binary_sha256", dict(initial["fixture"]["release_binary_sha256"], ingest=token("rebuilt")))):
            with self.subTest(key=key):
                other = copy.deepcopy(initial)
                other["fixture"][key] = changed
                self.assertEqual(len(cov.compare(baseline, matrix(other))), 2)
                other["fixture"]["release_revision"] = "a" * 40
                new_release = {"tag": "v1.1.0", "commit": "a" * 40}
                self.assertEqual(bool(cov.compare(baseline, matrix(other, new_release))), key != "release_binary_sha256")
        other = copy.deepcopy(initial)
        other["fixture"]["project"] = "another-project"
        other["fixture"]["ready_sha256"] = token("new readiness")
        self.assertEqual(cov.compare(baseline, matrix(other)), [])
        for key in ("configuration_sha256", "image_ids"):
            other = copy.deepcopy(initial)
            del other["fixture"][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                matrix(other)

    def test_supervised_case_requires_the_final_cooperative_result_for_this_receipt(self):
        case = "disconnect"
        doc = self.receipts[case]
        doc["supervised_project"] = doc["fixture"]["project"] = "owned-project"
        for pin in doc["pins"].values():
            pin["harness"]["run-wallet-roundtrip-regtest.py"] = token("supervisor")
        path = self.write(case, doc)
        with self.assertRaisesRegex(ValueError, "missing final campaign"):
            self.rows(case)
        record = {"schema_version": 1, "status": "passed", "case": case, "project": "owned-project",
                  "receipt": {"status": "passed", "sha256": digest(path)},
                  "supervision": {"kind": "cooperative", "exit_code": 0},
                  "cleanup": {"resources_remaining": 0, "problems": [], "repaired": False}}
        sidecar = path.with_name("supervision.json")
        sidecar.write_text(json.dumps(record))
        self.assertEqual(len(self.rows(case)), 2)
        for edit in ({"status": "failed"}, {"case": "rollback"}, {"project": "unrelated"},
                     {"receipt": {"status": "passed", "sha256": "0" * 64}},
                     {"supervision": {"kind": "timeout", "exit_code": 0}},
                     {"cleanup": {"resources_remaining": 0, "problems": [], "repaired": True}}):
            with self.subTest(edit=edit):
                sidecar.write_text(json.dumps(record | edit))
                with self.assertRaises(ValueError):
                    self.rows(case)

    def test_standalone_receipts_require_every_shared_runtime_input(self):
        for case, original in self.receipts.items():
            for name in original["pins"]["pre"]["harness"]:
                with self.subTest(case=case, name=name):
                    receipt = copy.deepcopy(original)
                    for snapshot in receipt["pins"].values():
                        del snapshot["harness"][name]
                    self.write(case, receipt)
                    with self.assertRaisesRegex(ValueError, "incomplete recovery harness pins"):
                        self.rows(case)

    def test_pinned_config_must_match_the_fixture_schedule(self):
        for case, receipt in self.receipts.items():
            for snapshot in receipt["pins"].values():
                snapshot["harness"]["regtest-nu6-at-one.toml"] = token("other schedule")
            self.write(case, receipt)
            with self.subTest(case=case), self.assertRaisesRegex(ValueError, "different activation schedule"):
                self.rows(case)

    def test_incomplete_or_changed_input_pins_cannot_qualify(self):
        def remove_source(receipt):
            for snapshot in receipt["pins"].values():
                del snapshot["harness"]["roundtrip_wallets.py"]

        for case in self.receipts:
            for mutate in (lambda r: r.pop("pins"),
                           lambda r: r["pins"]["post"]["wallets"].update(zingo=token("changed")),
                           remove_source):
                r = copy.deepcopy(self.receipts[case])
                mutate(r)
                self.write(case, r)
                with self.subTest(case=case), self.assertRaises(ValueError):
                    self.rows(case)

    def test_both_wallet_rows_fingerprint_the_bytes_actually_read(self):
        path = self.paths["disconnect"]
        original_sha = digest(path)
        rows = recovery_actions(path, RELEASE)
        first = next(rows)
        path.write_text("{}")
        second = next(rows)
        self.assertEqual([r["evidence"]["sha256"] for r in (first, second)], [original_sha] * 2)

    def test_actor_rows_keep_scope_none_and_validate(self):
        disconnect, rollback, transport = (self.rows("disconnect"), self.rows("rollback"),
                                           self.rows("transport"))
        self.assertEqual([r["id"] for r in disconnect],
                         ["payment-recovery-disconnect-zingo", "payment-recovery-disconnect-zallet"])
        self.assertEqual([r["interface"] for r in disconnect],
                         ["lightwalletd", "native-wallet-query"])
        self.assertEqual(len(rollback), 2)
        self.assertEqual([r["id"] for r in transport], ["payment-recovery-transport-zallet"])
        for row in disconnect + rollback + transport:
            self.assertEqual(row["method_scope"], "none")
            self.assertEqual(row["methods"], {})
            self.assertEqual(row["status"], "pass")
            self.assertNotIn("uregtest1", json.dumps(row))
        matrix = {"schema_version": 1, "release": RELEASE,
                  "inventory": {"lightwalletd": ["GetLatestBlock"],
                                "native-wallet-query": ["VisibleTipBlock"]},
                  "scenarios": disconnect + rollback + transport}
        self.assertEqual(len(cov.validate(matrix)["scenarios"]), 5)
        self.assertEqual(disconnect[0]["evidence"]["sha256"], digest(self.paths["disconnect"]))

    def test_local_status_rpc_availability_is_diagnostic(self):
        baseline = {"inventory": {}, "scenarios": self.rows("transport")}
        for status, expected in (
            ({"available": False, "error": "WalletRpcError"}, "wallet-rpc-error"),
            ({"available": False, "error": "TimeoutError"}, "unavailable"),
            ({"available": True, "fully_synced_height": 209}, "available"),
        ):
            with self.subTest(status=status):
                receipt = copy.deepcopy(self.receipts["transport"])
                for outage in (receipt["outage"], receipt["scenario"]["outage"]):
                    outage["zallet_status_while_down"] = status
                self.write("transport", receipt)
                rows = self.rows("transport")
                self.assertEqual(rows[0]["evidence"]["local_status_rpc_during_outage"], expected)
                self.assertEqual(cov.compare(baseline, {"inventory": {}, "scenarios": rows}), [])
                for outage in (receipt["outage"], receipt["scenario"]["outage"]):
                    outage["native_route_reachable"] = True
                self.write("transport", receipt)
                with self.assertRaisesRegex(ValueError, "not isolated"):
                    self.rows("transport")

    def test_reconciled_return_fee_change_is_visible_for_every_case(self):
        for case in CASES:
            with self.subTest(case=case):
                baseline = {"inventory": {}, "scenarios": self.rows(case)}
                receipt = copy.deepcopy(self.receipts[case])
                paid = receipt["scenario"]["return"]["accounting"]
                paid["fee_zat"] += 5000
                paid["payer_debit_zat"] += 5000
                paid["payer_after_zat"] -= 5000
                receipt["scenario"]["persistence"]["zallet_total_zat"] -= 5000
                self.write(case, receipt)
                current = {"inventory": {}, "scenarios": self.rows(case)}
                changes = cov.compare(baseline, current)
                for field in ("fee", "debit", "balance_after"):
                    self.assertIn(f"payment-recovery-{case}-zallet: changed return accounting for {field}", changes)
                self.assertEqual(next(row for row in current["scenarios"] if row["wallet"] == "zallet")
                                 ["return_accounting_zat"]["fee"], 30_000)
                for row in current["scenarios"]:
                    if row["wallet"] == "zingo":
                        self.assertEqual(row["return_accounting_zat"]["credit"], 4_000_000)
                        self.assertNotIn("fee", row["return_accounting_zat"])

    def test_actor_mechanisms_are_not_interchanged(self):
        by_id = {r["id"]: r for r in self.rows("disconnect") + self.rows("rollback")
                 + self.rows("transport")}
        zingo = by_id["payment-recovery-disconnect-zingo"]
        zallet = by_id["payment-recovery-disconnect-zallet"]
        self.assertEqual(zingo["assertions"]["one-shot-cli-not-invoked-while-pending"], "pass")
        self.assertEqual(zingo["assertions"]["offline-process-identity"], "unexercised")
        self.assertEqual(zallet["assertions"]["daemon-process-identity-cleared-while-pending"], "pass")
        self.assertEqual(zallet["assertions"]["no-wallet-scan-during-disconnect"], "unexercised")
        self.assertEqual(zallet["assertions"]["native-route-outage"], "unexercised")
        transport = by_id["payment-recovery-transport-zallet"]
        self.assertEqual(transport["assertions"]["graceful-wallet-offline"], "unexercised")
        self.assertEqual(transport["assertions"]["same-process-reconnected-without-restart"], "pass")
        self.assertEqual(transport["accounting_zat"]["spendable_after"], 10_000_000)
        for role in ("payment-recovery-rollback-zingo", "payment-recovery-rollback-zallet"):
            self.assertEqual(by_id[role]["assertions"]["old-chain-tip-status"], "unexercised")
            self.assertEqual(by_id[role]["assertions"]["wallet-followed-replacement-branch"], "pass")

    def test_nested_transaction_height_balance_and_process_changes_reject(self):
        mutations = (
            ("rollback", lambda r: r["scenario"]["remine"].update(tx_fingerprint=token("other"))),
            ("disconnect", lambda r: (r["disconnect"]["mined_while_disconnected"].update(height=211),
                                      r["scenario"]["disconnect"]["mined_while_disconnected"].update(height=211))),
            ("disconnect", lambda r: r["scenario"]["persistence"].update(zingo_total_zat=493_975_001)),
            ("transport", lambda r: r["same_process"].update(process_identical=False)),
            ("transport", lambda r: r["outage"].update(zallet_process_unchanged=False)),
            ("rollback", lambda r: r["scenario"]["remine"].update(raw_sha256=token("other"))),
        )
        for case, mutate in mutations:
            with self.subTest(case=case):
                receipt = copy.deepcopy(self.receipts[case])
                mutate(receipt)
                path = self.write(case, receipt)
                with self.assertRaises(ValueError):
                    list(recovery_actions(path, RELEASE))

    def test_release_linkage_accounting_and_receipt_claims_reject(self):
        mutations = (
            lambda r: r["fixture"].update(release_revision="f" * 40),
            lambda r: r["clients"].update(zingo_sha256="bad"),
            lambda r: r["fixture"].update(activation_schedule_sha256="short"),
            lambda r: r["funding"].update(tx_fingerprint=r["scenario"]["outbound"]["tx_fingerprint"]),
            lambda r: r["scenario"]["outbound"]["accounting"].update(payer_debit_zat=10_010_001),
            lambda r: r.update(status="failed"),
            lambda r: r["assertions"].update(fee_reconciles=False),
            lambda r: r["assertions"].pop("fee_reconciles"),
            lambda r: r["limits"].clear(),
            lambda r: r["cleanup"].update(project_resources_remaining=1),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                receipt = copy.deepcopy(self.receipts["disconnect"])
                mutate(receipt)
                path = self.write("disconnect", receipt)
                with self.assertRaises(ValueError):
                    list(recovery_actions(path, RELEASE))

    def test_candidate_server_change_preserves_comparable_fixture(self):
        baseline = self.rows("transport")[0]
        case = self.receipts["transport"]
        case["fixture"]["release_binary_sha256"]["query"] = token("new server")
        case["fixture"]["release_revision"] = "9" * 40
        release = {"tag": "v1.1.0", "commit": "9" * 40}
        current, = recovery_actions(self.write("transport", case), release)
        self.assertEqual(baseline["fixture"], current["fixture"])
        self.assertNotEqual(baseline["evidence"]["sha256"], current["evidence"]["sha256"])

    def test_remined_height_and_block_hash_must_match_the_rollback_contract(self):
        baseline = self.receipts["rollback"]["scenario"]
        old, block_hash = baseline["remine"]["old_height"], baseline["remine"]["new_block_hash"]

        def remined(height, digest=block_hash, empty_replacement=True, returned=None):
            case = copy.deepcopy(self.receipts["rollback"])
            scenario = case["scenario"]
            scenario["remine"].update(new_height=height, new_block_hash=digest,
                                      empty_replacement=empty_replacement,
                                      height_changed=height != old)
            scenario["replacement"].update(wallet_new_height=height,
                                           wallet_observed_height_change=height != old)
            if height > scenario["replacement"]["tip_height"]:
                tip = {"height": height, "hash": token(f"tip-{height}")}
                scenario["replacement"].update(tip_height=height, tip_hash=tip["hash"],
                                               surfaces={"lightwalletd": dict(tip),
                                                         "native": dict(tip)})
            if returned is not None:
                scenario["return"].update(mined_height=returned,
                                          surfaces=surfaces(returned, "return"))
            return case

        cases = (
            {"label": "negative height", "height": -1},
            {"label": "height beyond the next block", "height": old + 2},
            {"label": "empty replacement at the old height", "height": old},
            {"label": "readmission at the next height", "height": old + 1,
             "empty_replacement": False},
            {"label": "remine at the return height", "height": old + 1, "returned": old + 1},
            {"label": "remine after the return height", "height": old + 2, "returned": old + 1},
            {"label": "boolean height", "height": True},
            {"label": "uppercase block hash", "height": old + 1, "digest": block_hash.upper()},
            {"label": "short block hash", "height": old + 1, "digest": block_hash[:-1]},
            {"label": "non-hex block hash", "height": old + 1, "digest": "not-a-block-hash"},
        )
        for mutation in cases:
            with self.subTest(label=mutation["label"]):
                self.write("rollback", remined(**{k: v for k, v in mutation.items() if k != "label"}))
                with self.assertRaises(ValueError):
                    self.rows("rollback")

    def test_automatic_readmission_in_a_new_block_at_the_same_height(self):
        case = self.receipts["rollback"]
        scenario = case["scenario"]
        scenario["remine"].update(new_height=210, empty_replacement=False, height_changed=False)
        scenario["replacement"].update(wallet_new_height=210, wallet_observed_height_change=False)
        scenario["orphan"].update(old_block_state={"header_available": True, "height": 210, "confirmations": -1},
                                 old_chain_tip_status=None, automatically_readded_to_mempool=True,
                                 readded_after_recovery_attempts=True)
        sha = scenario["remine"]["raw_sha256"]
        scenario["readmission"] = {"method": "node-reorg-readmission", "mempool_present": True,
                                   "original_raw_sha256": sha, "observed_raw_sha256": sha}
        for key in ("empty_replacement_excludes_transfer", "remine_at_different_height"):
            del case["assertions"][key]
        self.write("rollback", case)
        rows = self.rows("rollback")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["assertions"]["remine-at-different-height"], "unexercised")
        self.assertEqual(rows[0]["assertions"]["old-chain-tip-status"], "unexercised")

    def test_transport_accepts_only_the_expected_fixture_memo(self):
        case = self.receipts["transport"]
        out = case["scenario"]["outbound"]
        out.pop("received_memos")
        out["received_memo"] = {"expected_present": True, "expected_sha256": token(MEMO)}
        self.write("transport", case)
        self.assertEqual(len(self.rows("transport")), 1)
        out["received_memo"]["expected_sha256"] = token("unrelated")
        self.write("transport", case)
        with self.assertRaises(ValueError):
            self.rows("transport")

    def test_unknown_case_rejects(self):
        receipt = copy.deepcopy(self.receipts["disconnect"])
        receipt["case"] = "unknown"
        path = self.write("unknown", receipt)
        with self.assertRaises(ValueError):
            list(recovery_actions(path, RELEASE))


if __name__ == "__main__":
    unittest.main()
