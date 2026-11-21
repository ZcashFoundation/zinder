#!/usr/bin/env python3
"""Regression rules for preserved subtree fixtures and composed native expiry identity."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

import coverage as cov
from coverage_evidence import digest, native_expiry_actions, subtree_actions


RELEASE = {"tag": "v1.0.0", "commit": "a" * 40}


def default_fixture():
    return cov.load(Path(cov.__file__).with_name("subtree-testnet.json"))


def custom_fixture():
    fixture = copy.deepcopy(default_fixture())
    fixture["cli_revision"] = "b" * 40
    fixture["cli_sha256"] = "c" * 64
    return fixture


def subtree_state(fixture):
    return {"blocks": fixture["tip"] - fixture["birthday"] + 1, "outputs": fixture["expected_outputs"],
            "history_sha256": fixture["history_sha256"], "confirmed_zat": fixture["expected_confirmed_zat"],
            "spendable_zat": fixture["expected_spendable_zat"]}


def subtree_arm(fixture):
    state = subtree_state(fixture)
    return {"states": {"restore": state, "restart": state}, "target_only_process_connects": True,
            "restore_method_calls": {"GetSubtreeRoots": 2}, "restart_method_calls": {"GetLatestBlock": 1}}


def subtree_events(fixture, mode):
    count = len(fixture["sapling_roots"])
    return [{"phase": f"{mode}/restore", "method": "GetSubtreeRoots", "request_sha256": "1" * 64,
             "pool": 0, "start": 0, "rows": count, "source_sha256": "2" * 64, "emitted_sha256": "3" * 64},
            {"phase": f"{mode}/restore", "method": "GetSubtreeRoots", "request_sha256": "4" * 64,
             "pool": 0, "start": count, "rows": 0, "source_sha256": "5" * 64, "emitted_sha256": "5" * 64}]


def write_subtree_run(directory, fixture, preserve=True, report_digest=None):
    directory.mkdir()
    if preserve:
        (directory / "fixture.json").write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    fixture_path = directory / "fixture.json" if preserve else Path(cov.__file__).with_name("subtree-testnet.json")
    fixture_sha256 = digest(fixture_path) if report_digest is None else report_digest
    events = [event for mode in ("reference", "reversed") for event in subtree_events(fixture, mode)]
    events.extend({"phase": f"{mode}/restart", "method": "GetLatestBlock", "request_sha256": "6" * 64,
                   "rows": 1, "source_sha256": "7" * 64, "emitted_sha256": "7" * 64}
                  for mode in ("reference", "reversed"))
    (directory / "wallet-rpcs.json").write_text(json.dumps(events))
    report = {"schema": 1, "fixture_sha256": fixture_sha256,
              "server": {"commit": RELEASE["commit"], "version": RELEASE["tag"][1:], "height": fixture["tip"],
                         "hash": fixture["tip_hash"], "network": fixture["network"]},
              "wallet": {"sha256": fixture["cli_sha256"], "revision": fixture["cli_revision"]},
              "arms": {"reference": subtree_arm(fixture), "reversed": subtree_arm(fixture)},
              "primary_wire_comparison": {
                  "restore": {"identical_request_and_response_signatures": 2,
                              "total_calls": {"reference": 2, "reversed": 2}},
                  "restart": {"identical_request_and_response_signatures": 1,
                              "total_calls": {"reference": 1, "reversed": 1}}},
              "distinct_wallet_files": 2, "observed_methods": ["GetSubtreeRoots", "GetLatestBlock"]}
    (directory / "summary.json").write_text(json.dumps(report))


class EvidenceTests(unittest.TestCase):
    def test_custom_fixture_preserved_with_the_experiment_is_collected(self):
        fixture = custom_fixture()
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / "subtree"
            write_subtree_run(run, fixture)
            rows = list(subtree_actions(run, RELEASE))
        self.assertEqual({row["client_sha256"] for row in rows}, {fixture["cli_sha256"]})
        self.assertEqual({row["wallet"] for row in rows}, {"zingo"})

    def test_changed_wallet_revision_has_its_own_version_identity(self):
        fixture = custom_fixture()
        fixture["cli_revision"] = "a" * 40
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / "subtree"
            write_subtree_run(run, fixture)
            rows = list(subtree_actions(run, RELEASE))
        self.assertEqual({row["version"] for row in rows}, {"source " + "a" * 12})

    def test_missing_preserved_fixture_falls_back_to_the_pinned_baseline(self):
        fixture = default_fixture()
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / "subtree"
            write_subtree_run(run, fixture, preserve=False)
            rows = list(subtree_actions(run, RELEASE))
        self.assertEqual({row["client_sha256"] for row in rows}, {fixture["cli_sha256"]})

    def test_preserved_fixture_digest_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / "subtree"
            write_subtree_run(run, custom_fixture(), report_digest="d" * 64)
            with self.assertRaisesRegex(ValueError, "frozen fixture mismatch"):
                list(subtree_actions(run, RELEASE))

    def test_tampered_preserved_fixture_is_rejected(self):
        fixture = custom_fixture()
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / "subtree"
            write_subtree_run(run, fixture)
            fixture["expected_outputs"] += 1
            (run / "fixture.json").write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
            with self.assertRaisesRegex(ValueError, "frozen fixture mismatch"):
                list(subtree_actions(run, RELEASE))

    def native_receipt(self, backend="e" * 64, driver="f" * 64, feature="bounded-scan-certification"):
        start, anchor = 149, 150

        def call(method, count, code="ok", error_class="none"):
            return {"method": method, "count": count, "grpc_code": code, "error_class": error_class}

        state = {"fixture": {"network": "zcash-regtest", "release_revision": RELEASE["commit"],
                             "release_version": RELEASE["tag"][1:], "anchor_height": anchor,
                             "raw_blob_policy": "all"},
                 "client": {"name": "zallet", "driver_sha256": driver, "backend_sha256": backend,
                            "driver_feature": feature},
                 "attempts": [{"chain_epoch_id": "first", "requested_start_height_inclusive": start,
                               "requested_end_height_inclusive": anchor},
                              {"chain_epoch_id": "second", "requested_start_height_inclusive": start,
                               "requested_end_height_inclusive": anchor + 1}],
                 "persisted_heights": list(range(start, anchor + 2)),
                 "expired_source": "IndexerError::ChainEpochPinUnavailable",
                 "actions": {"initialize": [call("FullBlocksInRange", 1)],
                             "acquire-and-pause": [call("FullBlocksInRange", 1)],
                             "stale-read-and-retry": [
                                 call("FullBlocksInRange", 1, "failed_precondition",
                                      "CHAIN_EPOCH_PIN_UNAVAILABLE"),
                                 call("FullBlocksInRange", 1)]},
                 "trace_sha256": {"initialize": "1" * 64, "scan": "2" * 64},
                 "fixture_teardown_completed": True,
                 "assertions": dict.fromkeys(("expired_metadata_unchanged", "expired_scan_work_still_pending",
                                              "complete_retry_on_new_epoch", "retry_hashes_match_canonical",
                                              "retry_scan_work_completed"), True)}
        return dict(schema_version=1, status="passed", case="zallet-bounded-scan-expiry",
                    interface="native-wallet-query", **state)

    def collect_native(self, receipt):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "receipt.json"
            path.write_text(json.dumps(receipt))
            return list(native_expiry_actions(path, RELEASE))

    def matrix(self, rows):
        return cov.validate({"schema_version": 1, "release": RELEASE,
                             "inventory": {"lightwalletd": ["GetBlock"],
                                           "native-wallet-query": ["FullBlocksInRange"]},
                             "scenarios": rows})

    def test_native_identity_uses_the_ordinary_backend_and_driver(self):
        rows = self.collect_native(self.native_receipt())
        self.assertEqual([row["id"] for row in rows],
                         ["zallet-bounded-initialize", "zallet-bounded-acquire-and-pause",
                          "zallet-bounded-stale-read-and-retry"])
        self.assertEqual(rows[0]["client_sha256"], "e" * 64)
        self.assertIn("f" * 64, rows[0]["variant"])
        self.assertIn("bounded-scan-certification", rows[0]["variant"])

    def test_same_driver_with_a_different_backend_is_incomparable(self):
        before = self.matrix(self.collect_native(self.native_receipt()))
        after = self.matrix(self.collect_native(self.native_receipt(backend="9" * 64)))
        self.assertIn("incomparable client_sha256", "\n".join(cov.compare(before, after)))

    def test_changed_driver_or_flags_is_incomparable(self):
        before = self.matrix(self.collect_native(self.native_receipt()))
        for change in (dict(driver="8" * 64), dict(feature="other-wallet-flags")):
            with self.subTest(change=change):
                after = self.matrix(self.collect_native(self.native_receipt(**change)))
                self.assertIn("incomparable variant", "\n".join(cov.compare(before, after)))

    def test_native_scan_range_must_match_the_referenced_fixture_anchor(self):
        receipt = self.native_receipt()
        receipt["fixture"]["anchor_height"] += 1
        with self.assertRaisesRegex(ValueError, "anchor"):
            self.collect_native(receipt)

    def test_native_receipt_requires_both_digests(self):
        receipt = self.native_receipt()
        del receipt["client"]["backend_sha256"]
        with self.assertRaises(KeyError):
            self.collect_native(receipt)

    def test_native_receipt_requires_complete_actions_assertions_and_traces(self):
        for label, mutate in (
                ("incomplete cleanup", lambda receipt: receipt.update(fixture_teardown_completed=False)),
                ("absent cleanup", lambda receipt: receipt.pop("fixture_teardown_completed")),
                ("missing phase", lambda receipt: receipt["actions"].pop("acquire-and-pause")),
                ("unexpected phase", lambda receipt: receipt["actions"].update(extra=[])),
                ("empty calls", lambda receipt: receipt["actions"].__setitem__("initialize", [])),
                ("zero count", lambda receipt: receipt["actions"]["initialize"][0].update(count=0)),
                ("empty assertions", lambda receipt: receipt["assertions"].clear()),
                ("missing assertion", lambda receipt: receipt["assertions"].pop("retry_scan_work_completed")),
                ("false assertion", lambda receipt: receipt["assertions"].update(retry_scan_work_completed=False)),
                ("missing trace", lambda receipt: receipt["trace_sha256"].pop("scan")),
                ("one attempt", lambda receipt: receipt["attempts"].pop())):
            receipt = self.native_receipt()
            mutate(receipt)
            with self.subTest(label), self.assertRaises(ValueError):
                self.collect_native(receipt)

if __name__ == "__main__":
    unittest.main()
