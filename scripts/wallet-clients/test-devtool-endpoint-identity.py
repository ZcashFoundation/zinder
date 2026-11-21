#!/usr/bin/env python3
"""Check the devtool lightwalletd identity probe and its strict import gate.

The checks exercise comparison logic and receipt shape only. They never dial a
server; the live run records its own observations.
"""
import importlib.util
import unittest
from pathlib import Path

import coverage_counterparty_roles as roles

HERE = Path(__file__).resolve().parent


def load(name):
    path = HERE / (name + ".py")
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load("lightwalletd-identity")
roles_runner = load("payment-roles")

REVISION = "999d51ab835037a2466763ca1a1e7739a26b72c1"
STALE = "6b4e686b6b2e6b5e8d7e6f5a4b3c2d1e0f9a8b7c"
ANCHOR = "f8c105c0f6308acf628a6f74634c1ef90cd59e72aa7bf4ba3fea33b1ae1b4728"
OTHER = "ab" * 32
READY = {"zinder_commit": REVISION, "current_tip": 150, "current_hash": ANCHOR}


def observation(**changes):
    base = {
        "chain_name": "test",
        "build_git_commit": REVISION,
        "version": "0.22.0",
        "vendor": "Zinder",
        "sapling_activation_height": 1,
        "consensus_branch_id": "e9ff75a6",
        "upgrade_name": "",
        "lightwallet_protocol_version": "1.0",
        "served_height": 150,
        "served_tip_height": 150,
        "served_tip_hash": ANCHOR,
        "anchor_height": 150,
        "anchor_hash": ANCHOR,
    }
    base.update(changes)
    return base


def probe(**changes):
    record = identity.evaluate(observation(**changes), "test", REVISION, 150, ANCHOR)
    record.update({"scope": "live-probe", "endpoint": "http://127.0.0.1:24767",
                   "provenance": {"transport": "grpc", "payloads_recorded": False}})
    record.setdefault("phase", "before-admission")
    return record


class ProbeComparison(unittest.TestCase):
    def test_matching_observation_passes_every_check(self):
        record = probe()
        self.assertTrue(record["ok"])
        self.assertTrue(record["network_verified"])
        self.assertTrue(record["revision_verified"])
        self.assertTrue(record["anchor_verified"])
        self.assertEqual(record["anchor_hash"], ANCHOR)

    def test_stale_revision_is_rejected(self):
        record = identity.evaluate(observation(), "test", STALE, 150, ANCHOR)
        self.assertFalse(record["ok"])
        self.assertFalse(record["revision_verified"])
        self.assertTrue(record["network_verified"])
        self.assertTrue(record["anchor_verified"])

    def test_wrong_chain_name_is_rejected(self):
        record = identity.evaluate(observation(), "main", REVISION, 150, ANCHOR)
        self.assertFalse(record["ok"])
        self.assertFalse(record["network_verified"])
        self.assertTrue(record["revision_verified"])
        self.assertTrue(record["anchor_verified"])

    def test_shifted_anchor_is_rejected(self):
        record = identity.evaluate(observation(anchor_height=149, anchor_hash=OTHER),
                                   "test", REVISION, 150, ANCHOR)
        self.assertFalse(record["ok"])
        self.assertFalse(record["anchor_verified"])
        self.assertTrue(record["revision_verified"])
        self.assertTrue(record["network_verified"])

    def test_served_height_and_tip_hash_are_recorded(self):
        record = probe(served_height=380, served_tip_height=380,
                       served_tip_hash=OTHER)
        self.assertEqual(record["served_height"], 380)
        self.assertEqual(record["served_tip_hash"], OTHER)
        self.assertTrue(record["ok"])


class StrictImport(unittest.TestCase):
    def test_structured_identity_accepts_the_probe_record(self):
        roles.structured_identity(probe(), READY, "devtool", "lightwalletd")

    def test_structured_identity_rejects_a_stale_record(self):
        with self.assertRaises(ValueError):
            roles.structured_identity(probe(build_git_commit=STALE), READY,
                                      "devtool", "lightwalletd")

    def test_structured_identity_rejects_a_shifted_anchor(self):
        with self.assertRaises(ValueError):
            roles.structured_identity(probe(anchor_hash=OTHER), READY,
                                      "devtool", "lightwalletd")


class ReceiptShape(unittest.TestCase):
    def binding(self, bound=True, endpoint="127.0.0.1:24767"):
        return {"scope": "connect syscalls", "online_commands": 2, "traced_commands": 2,
                "bound": bound, "endpoint": endpoint, "reports": []}

    def wallet_view(self, height):
        return {"server_uri": "http://127.0.0.1:24767", "chain_name": "test",
                "chain_tip_height": height}

    def live(self):
        before = probe(phase="before-admission")
        after = probe(phase="after-use", served_height=380, served_tip_height=380)
        return {"before": before, "after": after, "endpoint": "http://127.0.0.1:24767",
                "negative_controls": [probe()],
                "wallet_commands": {"engine-pays": self.binding()},
                "wallet_identity": {
                    "engine-pays": {"before_admission": self.wallet_view(150),
                                    "after_use": self.wallet_view(380)}}}

    def test_scoped_record_carries_both_observations(self):
        evidence = roles_runner.endpoint_evidence("devtool", None, self.live())
        self.assertEqual(evidence["scope"], "live-probe-before-and-after")
        self.assertTrue(evidence["anchor_verified"])
        self.assertEqual(evidence["lightwalletd_server_info"]["phase"], "after-use")
        self.assertEqual(set(evidence["observations"]),
                         {"before_admission", "after_use"})
        self.assertEqual(evidence["wallet_server_info"]["engine-pays"]["after_use"]
                         ["chain_tip_height"], 380)
        self.assertFalse(evidence["provenance"]["scope"].startswith("driver"))

    def test_missing_after_probe_is_refused(self):
        live = self.live()
        live.pop("after")
        with self.assertRaises(ValueError):
            roles_runner.endpoint_evidence("devtool", None, live)

    def test_wallet_view_of_another_server_is_refused(self):
        live = self.live()
        live["wallet_identity"]["engine-pays"]["after_use"]["server_uri"] = \
            "http://127.0.0.1:24768"
        with self.assertRaises(ValueError):
            roles_runner.endpoint_evidence("devtool", None, live)

    def wallet(self, binding):
        return type("Wallet", (), {"endpoint_binding": lambda self: binding})()

    def test_wallet_binding_requires_every_command_traced(self):
        with self.assertRaises(ValueError):
            roles_runner.wallet_endpoint_binding(
                {"engine-pays": self.wallet(self_binding(False))})

    def test_wallet_binding_rejects_mixed_endpoints(self):
        with self.assertRaises(ValueError):
            roles_runner.wallet_endpoint_binding({
                "engine-pays": self.wallet(self_binding(True)),
                "engine-receives": self.wallet(self_binding(True, "127.0.0.1:24768"))})

    def test_wallet_binding_summarizes_bound_wallets(self):
        bound = roles_runner.wallet_endpoint_binding(
            {"engine-pays": self.wallet(self_binding(True))})
        self.assertTrue(bound["engine-pays"]["bound"])


def self_binding(bound, endpoint="127.0.0.1:24767"):
    return {"bound": bound, "endpoint": endpoint}


if __name__ == "__main__":
    unittest.main()
