#!/usr/bin/env python3
"""Check the release wallet round-trip gate without Docker or a chain."""
import argparse
import copy
import hashlib
import importlib.util
import io
import subprocess
import sys
import json
from pathlib import Path
import socket
import tempfile
import tarfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_wallet", ROOT / "scripts/release-wallet-roundtrip.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
BASELINE = ROOT / "scripts/wallet-clients/coverage-v0.22.0.json"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_archive(path, members):
    with tarfile.open(path, "w:gz") as archive:
        for name, content in members.items():
            source = Path(content)
            archive.add(source, arcname=name)


def make_candidate(root, tag="v9.9.9", commit="a" * 40, version="9.9.9", tamper=None):
    build = root / f"zinder-{version}-x86_64-v3-unknown-linux-gnu"
    (build / "bin").mkdir(parents=True)
    binaries = []
    for name in gate.CANDIDATE_BINARIES:
        binary = build / "bin" / name
        binary.write_bytes(("binary-" + name).encode())
        binaries.append({"name": name, "sha256": digest(binary), "size": binary.stat().st_size})
    if tamper:
        (build / "bin" / tamper).write_bytes(b"tampered")
    info = {"schema_version": 1, "version": version, "tag": tag, "commit": commit, "binaries": binaries}
    (build / "BUILD-INFO.json").write_text(json.dumps(info) + "\n")
    archive = root / "candidate.tar.gz"
    write_archive(archive, {build.name: build})
    return archive


def make_clients(root, zingo_sha=None, zallet_sha=None):
    bundle_root = root / "clients"
    bundle_root.mkdir(parents=True)
    pins = {"schema_version": 1, "zingo": {"binary": "zingo-cli", "source": "b" * 40},
            "zallet": {"binary": "zallet-zinder", "source": "c" * 40}}
    for role, name in (("zingo", "zingo-cli"), ("zallet", "zallet-zinder")):
        binary = bundle_root / name
        binary.write_bytes(role.encode())
        pins[role]["sha256"] = digest(binary)
    pins["zingo"]["sha256"] = zingo_sha or pins["zingo"]["sha256"]
    pins["zallet"]["sha256"] = zallet_sha or pins["zallet"]["sha256"]
    (bundle_root / "inputs.json").write_text(json.dumps(pins) + "\n")
    archive = root / "clients.tar.gz"
    write_archive(archive, {"clients": bundle_root})
    return archive


def completed_campaign(root):
    campaign = root / "campaign"
    (campaign / "first").mkdir(parents=True)
    (campaign / "second").mkdir()
    receipt = {key: None for key in gate.RECEIPT_KEYS}
    receipt.update(status="passed", schema_version=1, network="regtest")
    (campaign / "first" / "receipt.json").write_text(json.dumps(receipt))
    (campaign / "second" / "receipt.json").write_text(json.dumps(receipt))
    (campaign / "paired.json").write_text(json.dumps(
        {"status": "passed", "same_wallet_outcomes": True, "independent_fixtures": True}))
    (campaign / "cleanup.json").write_text(json.dumps({"first": "removed", "second": "removed"}))
    audit = {
        "schema_version": 1, "passed": True, "independent_chains": 2,
        "distinct_wallet_identities": 4, "independent_transactions": 4,
        "runs": {
            "first": {"receipt_sha256": "1" * 64, "ready_sha256": "2" * 64, "end_height": 281,
                      "canonical_txid_fingerprints": {"outbound": "3" * 64, "return": "4" * 64}},
            "second": {"receipt_sha256": "5" * 64, "ready_sha256": "6" * 64, "end_height": 281,
                       "canonical_txid_fingerprints": {"outbound": "7" * 64, "return": "8" * 64}},
        },
        "matching_outcomes": {"schema_version": 1, "network": "regtest", "leg_outcomes": {},
                              "binaries": {"revision": "a" * 40}, "harness_sha256": {}},
    }
    (campaign / "canonical-audit.json").write_text(json.dumps(audit))
    (campaign / "first" / "run.json").write_text(json.dumps({"wallets": ["private"]}))
    (campaign / "first-wallet.log").write_text("private wallet output\n")
    return campaign


class RoundTripGateTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)

    def baseline(self):
        return gate.load_baseline(BASELINE)

    def candidate_rows(self):
        return copy.deepcopy(list(self.baseline()["rows"].values()))

    def compare(self, rows):
        return gate.compare_rows(rows, self.baseline(), "v9.9.9", "a" * 40)

    def test_versioned_matrix_holds_the_four_round_trip_rows(self):
        baseline = self.baseline()
        self.assertEqual(set(baseline["rows"]), set(gate.ROUNDTRIP_ROW_IDS))
        self.assertEqual(len(gate.ROUNDTRIP_ROW_IDS), 4)

    def test_matching_candidate_rows_compare_strictly_without_changes(self):
        comparison = self.compare(self.candidate_rows())
        self.assertTrue(comparison["compatible"])
        self.assertEqual(comparison["issues"], [])
        self.assertEqual(comparison["changes"], [])
        self.assertEqual(len(comparison["rows"]), 4)
        self.assertTrue(all(comparison["checks"].values()))

    def test_every_changed_accounting_value_fails_the_gate(self):
        cases = {
            "fee": ("roundtrip-outbound-zingo-send", "fee", 10001),
            "balance": ("roundtrip-outbound-zallet-receive", "balance_after", 1),
            "change": ("roundtrip-return-zallet-send", "change", 5975001),
            "spendable": ("roundtrip-outbound-zallet-receive", "spendable_after", 1),
            "amount": ("roundtrip-return-zingo-receive", "amount", 1),
        }
        for label, (row_id, field, value) in cases.items():
            with self.subTest(label=label):
                rows = self.candidate_rows()
                row = next(row for row in rows if row["id"] == row_id)
                row["accounting_zat"][field] = value
                comparison = self.compare(rows)
                self.assertFalse(comparison["compatible"])
                self.assertTrue(any("changed wallet accounting for " + field in change
                                    for change in comparison["changes"]))
        self.assertTrue(self.compare(self.candidate_rows())["checks"]["wallet_accounting"])

    def test_changed_client_binary_or_fixture_identity_cannot_certify(self):
        cases = {}
        zingo = self.candidate_rows()
        next(row for row in zingo if row["wallet"] == "zingo")["client_sha256"] = "0" * 64
        cases["zingo"] = zingo
        zallet = self.candidate_rows()
        next(row for row in zallet if row["wallet"] == "zallet")["client_sha256"] = "0" * 64
        cases["zallet"] = zallet
        fixture = self.candidate_rows()
        fixture[0]["fixture"] = fixture[0]["fixture"].replace(" at 150", " at 151")
        cases["fixture"] = fixture
        version = self.candidate_rows()
        version[0]["version"] = "binary 000000000000"
        cases["version"] = version
        interface = self.candidate_rows()
        interface[3]["interface"] = "native-wallet-query"
        cases["identity"] = interface
        for label, rows in cases.items():
            with self.subTest(label=label):
                comparison = self.compare(rows)
                self.assertFalse(comparison["compatible"])
                self.assertTrue(any("incomparable" in change for change in comparison["changes"]))

    def test_missing_row_extra_row_and_failed_outcomes_fail_the_gate(self):
        cases = {}
        cases["missing"] = self.candidate_rows()[:-1]
        extra = self.candidate_rows()
        extra.append(dict(extra[0], id="roundtrip-extra-row"))
        cases["extra"] = extra
        status = self.candidate_rows()
        status[0]["status"] = "fail"
        cases["status"] = status
        action = self.candidate_rows()
        action[0]["action"] = "Receive a different payment"
        cases["action"] = action
        assertion_fail = self.candidate_rows()
        assertion_fail[0]["assertions"]["independent-repeat"] = "fail"
        cases["assertion-fail"] = assertion_fail
        assertion_lost = self.candidate_rows()
        assertion_lost[2]["assertions"].pop("signed-payment")
        cases["assertion-lost"] = assertion_lost
        assertion_extra = self.candidate_rows()
        assertion_extra[1]["assertions"]["new-unversioned-check"] = "pass"
        cases["assertion-extra"] = assertion_extra
        accounting_extra = self.candidate_rows()
        accounting_extra[1]["accounting_zat"]["spendable_after"] = 1
        cases["accounting-extra"] = accounting_extra
        for label, rows in cases.items():
            with self.subTest(label=label):
                comparison = self.compare(rows)
                self.assertFalse(comparison["compatible"])
                self.assertTrue(comparison["issues"])

    def test_versioned_matrix_validation_rejects_malformed_rows(self):
        matrix = json.loads(BASELINE.read_text())
        row = next(row for row in matrix["scenarios"] if row["id"] == "roundtrip-outbound-zingo-send")
        row["status"] = "maybe"
        path = self.root / "invalid-matrix.json"
        path.write_text(json.dumps(matrix))
        with self.assertRaisesRegex(ValueError, "invalid result"):
            gate.load_baseline(path)

    def test_candidate_archive_binds_tag_commit_and_every_required_binary(self):
        archive = make_candidate(self.root)
        candidate = gate.load_candidate(archive, digest(archive), "v9.9.9", "a" * 40, self.root / "work")
        self.assertEqual(set(candidate["binaries"]), set(gate.CANDIDATE_BINARIES))
        self.assertEqual(candidate["archive_pin"], "pinned-input")
        with self.assertRaisesRegex(ValueError, "differs from its pinned"):
            gate.load_candidate(archive, "0" * 64, "v9.9.9", "a" * 40, self.root / "work-pin")

    def test_candidate_archive_rejects_wrong_identity_and_tampered_binary(self):
        archive = make_candidate(self.root / "one")
        with self.assertRaisesRegex(ValueError, "does not name the requested"):
            gate.load_candidate(archive, None, "v9.9.8", "a" * 40, self.root / "wrong-tag")
        tampered = make_candidate(self.root / "two", tamper="zinder-ingest")
        with self.assertRaisesRegex(ValueError, "differs from the BUILD-INFO pin"):
            gate.load_candidate(tampered, None, "v9.9.9", "a" * 40, self.root / "tampered")

    def test_archive_rejects_members_outside_the_extraction_root(self):
        archive = self.root / "escape.tar.gz"
        with tarfile.open(archive, "w:gz") as bundle:
            member = tarfile.TarInfo("../outside")
            member.size = 1
            bundle.addfile(member, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "escapes its extraction root"):
            gate.extract(archive, self.root / "extract")

    def test_client_bundle_requires_pinned_digests(self):
        archive = make_clients(self.root)
        clients = gate.load_clients(archive, digest(archive), self.root / "work")
        self.assertEqual(clients["clients"]["zingo"]["source"], "b" * 40)
        tampered = make_clients(self.root / "bad", zingo_sha="0" * 64)
        with self.assertRaisesRegex(ValueError, "differs from its bundle pin"):
            gate.load_clients(tampered, digest(tampered), self.root / "work-bad")

    def test_missing_client_bundle_is_an_explicit_operator_gate(self):
        with self.assertRaisesRegex(gate.GateBlocked, "operator gate"):
            gate.load_clients(self.root / "absent.tar.gz", "0" * 64, self.root / "work-absent")

    def binding_inputs(self):
        binaries = {name: hashlib.sha256(("binary-" + name).encode()).hexdigest()
                    for name in gate.CANDIDATE_BINARIES}
        receipt_pins = {name: binaries["zinder-" + name] for name in gate.IMAGE_NAMES}
        candidate = {"tag": "v9.9.9", "commit": "a" * 40, "archive_sha256": "b" * 64,
                     "binaries": {name: {"sha256": sha} for name, sha in binaries.items()}}
        clients = {"zingo": {"sha256": "c" * 64}, "zallet": {"sha256": "d" * 64}}
        state = self.root / "bound-campaign"
        for label in ("first", "second"):
            (state / label).mkdir(parents=True, exist_ok=True)
            (state / label / "receipt.json").write_text(json.dumps({
                "inputs": {"revision": "a" * 40, "zingo_sha256": "c" * 64, "zallet_sha256": "d" * 64},
                "fixture_recipe": {"release_binaries": receipt_pins},
            }))
        return state, candidate, clients, binaries

    def test_receipts_bind_candidate_binaries_and_both_client_pins(self):
        state, candidate, clients, binaries = self.binding_inputs()
        binding = gate.bind_campaign_evidence(state, candidate, clients, "a" * 40)
        self.assertEqual(binding["binaries"], binaries)
        self.assertEqual(binding["zingo_sha256"], "c" * 64)
        self.assertEqual(binding["zallet_sha256"], "d" * 64)

    def test_receipts_that_ran_other_binaries_or_clients_cannot_certify(self):
        state, candidate, clients, _ = self.binding_inputs()
        cases = {}
        binaries = dict(candidate["binaries"])
        binaries["zinder-ingest"] = {"sha256": "0" * 64}
        cases["binary"] = (dict(candidate, binaries=binaries), clients, "release archive")
        cases["zingo"] = (candidate, {"zingo": {"sha256": "0" * 64}, "zallet": clients["zallet"]},
                          "pinned bundle")
        cases["zallet"] = (candidate, {"zingo": clients["zingo"], "zallet": {"sha256": "0" * 64}},
                           "pinned bundle")
        for label, (other_candidate, other_clients, message) in cases.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, message):
                    gate.bind_campaign_evidence(state, other_candidate, other_clients, "a" * 40)
        with self.assertRaisesRegex(ValueError, "candidate revision"):
            gate.bind_campaign_evidence(state, candidate, clients, "e" * 40)

    def test_images_must_be_immutable_digests(self):
        images = self.root / "images.json"
        good = {name: "sha256:" + "a" * 64 for name in gate.IMAGE_NAMES}
        images.write_text(json.dumps(good))
        self.assertEqual(gate.load_images(images), good)
        good["ingest"] = "ghcr.io/zcashfoundation/zinder-ingest:latest"
        images.write_text(json.dumps(good))
        with self.assertRaisesRegex(ValueError, "immutable digest"):
            gate.load_images(images)

    def test_port_bases_must_not_collide(self):
        self.assertEqual(gate.validate_ports((51500, 51600)), [51500, 51600])
        self.assertEqual(gate.CAMPAIGN_ATTEMPTS, 2)
        with self.assertRaises(ValueError):
            gate.validate_ports((51500, 51580))
        with self.assertRaises(ValueError):
            gate.validate_ports((51500, 51500))

    def test_transient_port_collision_is_recognized_for_one_retry(self):
        campaign = self.root / "campaign-1"
        outer = self.root / "campaign-1.log"
        outer.write_text("[Errno 98] Address already in use\n")
        self.assertTrue(gate.port_collision(campaign, outer))
        outer.write_text("unrelated failure\n")
        self.assertFalse(gate.port_collision(campaign, outer))
        campaign.mkdir()
        (campaign / "second-up.log").write_text("[Errno 98] Address already in use\n")
        self.assertTrue(gate.port_collision(campaign, outer))
        (campaign / "second-up.log").write_text("wallet mismatch\n")
        self.assertFalse(gate.port_collision(campaign, outer))

    def test_occupied_loopback_port_retries_once_before_any_fixture_is_created(self):
        state = self.root / "runs"
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            first = occupied.getsockname()[1] - 2
            options = argparse.Namespace(state=state, images=self.root / "unused", port_bases=(first, 2048))
            clients = {wallet: {"path": self.root / wallet} for wallet in ("zingo", "zallet")}
            with patch.object(gate.time, "sleep") as backoff, self.assertRaises(gate.CampaignFailed) as failure:
                gate.run_campaign(options, {"bin_dir": self.root, "commit": "a" * 40}, clients)
        backoff.assert_called_once()
        self.assertEqual(failure.exception.attempts,
                         [{"attempt": i, "exit_code": 1, "port_collision": True} for i in (1, 2)])
        for i in (1, 2):
            self.assertFalse((state / f"campaign-{i}").exists())
            self.assertIn("Address already in use", (state / f"campaign-{i}.log").read_text())

    def test_provisioning_failures_publish_only_hashed_aggregates_and_stop_before_preflight(self):
        for stage in ("client-bundle", "python-runtime", "protocol-descriptors", "operator-setup"):
            with self.subTest(stage=stage):
                state, evidence = self.root / "private", self.root / stage
                command = [sys.executable, "-B", str(ROOT / "scripts/release-wallet-roundtrip.py"),
                           "--provisioning-failure", stage, "--tag", "v0.22.0", "--commit", "a" * 40,
                           "--candidate-archive", "absent", "--client-bundle", "absent",
                           "--client-bundle-sha256", "", "--images", "absent", "--baseline-matrix", "absent",
                           "--state", str(state), "--evidence", str(evidence)]
                run = subprocess.run(command, capture_output=True, text=True, timeout=10)
                self.assertEqual(run.returncode, 2, run.stderr)
                self.assertFalse(state.exists())
                self.assertEqual({p.name for p in evidence.iterdir()}, {"summary.json", "gate.json", "MANIFEST.json"})
                summary = gate.read_json(evidence / "summary.json")
                self.assertEqual((summary["status"], summary["stage"]), ("blocked", stage))
                self.assertEqual(gate.read_json(evidence / "gate.json")["status"], "blocked")
                self.assertEqual(gate.read_json(evidence / "MANIFEST.json")["files"],
                                 {f: digest(evidence / f) for f in ("summary.json", "gate.json")})

    def test_release_gate_stays_standalone_until_its_inputs_exist(self):
        import re
        release = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertNotRegex(release, r"^  wallet-roundtrip:\n")
        publish_job = re.search(r"^  publish-sdk-crates:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
                                release, re.M | re.S)
        self.assertIsNotNone(publish_job)
        self.assertNotIn("- wallet-roundtrip", publish_job.group(1))
        self.assertIn("scripts/release-wallet-roundtrip.md", release)
        action = (ROOT / "scripts/release-wallet-action/action.yml").read_text()
        self.assertIn("--port-bases 51500 51600", action)
        self.assertIn("--baseline-matrix", action)
        self.assertIn('"$EVIDENCE"', action)
        self.assertIn("if: ${{ !cancelled() }}", action)
        self.assertIn('--provisioning-failure "$FAILED_STAGE"', action)
        for step, stage in (("bundle", "client-bundle"), ("runtime", "python-runtime"),
                            ("descriptors", "protocol-descriptors"), ("operator", "operator-setup")):
            self.assertIn(f"id: {step}", action)
            self.assertIn(f"steps.{step}.outcome == 'failure' && '{stage}'", action)
        self.assertIn("if: always()", action)
        workflow = (ROOT / ".github/workflows/release-wallet-roundtrip.yml").read_text()
        self.assertIn("  workflow_call:", workflow)
        self.assertIn("  workflow_dispatch:", workflow)
        self.assertIn("uses: ./scripts/release-wallet-action", workflow)
        self.assertIn("timeout-minutes: 120", workflow)
        self.assertIn("actions: read", workflow)
        self.assertIn("github-token: ${{ github.token }}", workflow)
        self.assertIn("Require the gate harness at the candidate commit", workflow)
        runbook = (ROOT / "scripts/release-wallet-roundtrip.md").read_text()
        self.assertIn("## Activate the pre-publication gate", runbook)
        self.assertIn("WALLET_ROUNDTRIP_CLIENT_BUNDLE_URL", runbook)
        self.assertIn("uses: ./scripts/release-wallet-action", runbook)
        self.assertIn("timeout-minutes: 120", runbook)
        self.assertIn("- wallet-roundtrip", runbook)
        self.assertIn("runs-on: ubuntu-latest", runbook)

    def test_evidence_publishes_only_allowlisted_aggregates(self):
        evidence = self.root / "evidence"
        summary = {"schema_version": 1, "status": "passed"}
        rows = {"schema_version": 1, "candidate": {"tag": "v9.9.9", "commit": "a" * 40},
                "binding": {"binaries": {"zinder-ingest": "b" * 64},
                            "zingo_sha256": "c" * 64, "zallet_sha256": "d" * 64},
                "rows": [{"id": "row"}]}
        result = gate.publish_evidence(evidence, summary, {"schema_version": 1, "status": "ready"},
                                       {"compatible": True}, rows, completed_campaign(self.root))
        present = {str(path.relative_to(evidence)) for path in evidence.rglob("*") if path.is_file()}
        self.assertEqual(present, set(gate.EVIDENCE_ALLOWLIST) | {"MANIFEST.json"})
        self.assertEqual(set(result["files"]), set(gate.EVIDENCE_ALLOWLIST))
        self.assertNotIn("first/run.json", present)
        for relative, sha256 in result["files"].items():
            self.assertEqual(digest(evidence / relative), sha256)
        published = json.loads((evidence / "rows.json").read_text())
        self.assertEqual(published["binding"], rows["binding"])
        self.assertEqual(published["rows"], rows["rows"])

    def test_evidence_rejects_rows_with_a_blocked_field(self):
        rows = {"schema_version": 1, "binding": {"address": "not allowed"}, "rows": []}
        with self.assertRaisesRegex(ValueError, "blocked keys"):
            gate.publish_evidence(self.root / "evidence-private", {"schema_version": 1, "status": "passed"},
                                  {"schema_version": 1, "status": "ready"}, {"compatible": True}, rows,
                                  completed_campaign(self.root / "private"))

    def test_failed_campaign_still_publishes_a_completed_receipt(self):
        campaign = completed_campaign(self.root)
        (campaign / "second" / "receipt.json").unlink()
        (campaign / "canonical-audit.json").unlink()
        evidence = self.root / "evidence-partial"
        summary = {"schema_version": 1, "status": "failed", "stage": "campaign",
                   "error": "two-chain campaign did not complete"}
        result = gate.publish_evidence(evidence, summary, {"schema_version": 1, "status": "ready"},
                                       None, None, campaign)
        self.assertIn("first/receipt.json", result["files"])
        self.assertNotIn("second/receipt.json", result["files"])
        self.assertNotIn("canonical-audit.json", result["files"])

    def test_evidence_withholds_a_receipt_with_a_blocked_field(self):
        campaign = completed_campaign(self.root)
        paired = json.loads((campaign / "paired.json").read_text())
        paired["mnemonic"] = "not allowed"
        (campaign / "paired.json").write_text(json.dumps(paired))
        evidence = self.root / "evidence-blocked"
        summary = {"schema_version": 1, "status": "passed"}
        result = gate.publish_evidence(evidence, summary, {"schema_version": 1, "status": "ready"},
                                       {"compatible": True}, [], campaign)
        self.assertEqual(summary["status"], "failed")
        self.assertNotIn("paired.json", result["files"])
        self.assertEqual(summary["safety"]["withheld"][0]["file"], "paired.json")

    def test_failed_stage_still_publishes_summary_and_gate(self):
        evidence = self.root / "evidence-failed"
        summary = {"schema_version": 1, "status": "blocked", "error": "operator gate",
                   "host_runtime": gate.host_runtime()}
        result = gate.publish_evidence(evidence, summary, {"schema_version": 1, "status": "blocked"},
                                       None, None, None)
        self.assertEqual(set(result["files"]), {"summary.json", "gate.json"})
        self.assertEqual(json.loads((evidence / "summary.json").read_text())["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
