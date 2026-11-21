#!/usr/bin/env python3
"""Focused checks for the counterparty payment-role runner's reproducibility.

The runner must stay reproducible across machines: it has no host- or
checkout-specific default paths, it binds the receipt to the release tag the
caller selects (resolved to a commit), and it honestly records the Zally
residual fee and per-actor pending gaps instead of folding them into a pass.
These tests exercise that logic without starting a fixture.
"""
import json
import runpy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parent
MODULE = runpy.run_path(str(SCRIPTS / "payment-roles.py"))
V0_22_0_COMMIT = "999d51ab835037a2466763ca1a1e7739a26b72c1"


class ReproducibilityTests(unittest.TestCase):
    def test_release_tag_resolves_to_commit(self):
        self.assertEqual(MODULE["resolve_tag"]("v0.22.0"), V0_22_0_COMMIT)
        with self.assertRaises(ValueError):
            MODULE["resolve_tag"]("v0.99.0-not-a-tag")

    def test_revision_must_match_the_resolved_tag(self):
        reconcile = MODULE["reconcile_revision"]
        self.assertEqual(reconcile("v0.22.0", None), V0_22_0_COMMIT)
        self.assertEqual(reconcile("v0.22.0", V0_22_0_COMMIT), V0_22_0_COMMIT)
        with self.assertRaises(ValueError):
            reconcile("v0.22.0", "0" * 40)

    def test_images_filename_derives_from_the_tag(self):
        filename = MODULE["images_filename"]
        self.assertEqual(filename("v0.22.0"), "images-v0.22.0.json")
        self.assertEqual(filename("v0.23.0"), "images-v0.23.0.json")
        self.assertNotEqual(filename("v0.22.0"), filename("v0.23.0"))

    def test_release_must_name_a_tag(self):
        for ref in ("HEAD", V0_22_0_COMMIT, "refs/heads/main"):
            with self.subTest(ref=ref), self.assertRaisesRegex(ValueError, "invalid release tag"):
                MODULE["resolve_tag"](ref)

    def test_fixture_up_uses_selected_state_root_and_keeps_exact_readiness_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state, evidence = root / "state", root / "evidence"
            evidence.mkdir()
            images = root / "images.json"
            images.write_text("{}")
            ready_bytes = b'{ "project": "owned" }\n'

            def provision(command, **kwargs):
                self.assertEqual(command[command.index("--state-root") + 1], str(state))
                (state / "owned").mkdir(parents=True)
                (state / "owned/ready.initial.json").write_bytes(ready_bytes)

            with patch("subprocess.run", side_effect=provision):
                result, snapshot = MODULE["fixture_up"](state, 24300, evidence, "owned",
                    root / "node.toml", images, root / "bin", V0_22_0_COMMIT, "images-v0.22.0.json")
            self.assertEqual(result, json.loads(ready_bytes))
            self.assertEqual(snapshot, evidence / "fixtures/owned/ready.initial.json")
            self.assertEqual(snapshot.read_bytes(), ready_bytes)

    def test_required_inputs_are_engine_specific(self):
        required = MODULE["engine_required_inputs"]
        vizor = SimpleNamespace(images=None, bin_dir=None, zingo=None, evidence=None,
                                vizor_engine=None, vizor_source=None, vizor_provenance=None,
                                devtool=None, devtool_source=None,
                                zally_source=None, zally_binary=None,
                                native_probe_python=None, native_proto_dir=None)
        self.assertEqual(required("vizor", vizor),
                         ["images", "bin_dir", "zingo", "evidence",
                          "vizor_engine", "vizor_source", "vizor_provenance"])
        zally = SimpleNamespace(**{**vars(vizor),
                                   "images": "i", "bin_dir": "b", "zingo": "z", "evidence": "e",
                                   "zally_source": "s", "zally_binary": "h",
                                   "native_probe_python": "p", "native_proto_dir": "d"})
        self.assertEqual(required("zally", zally), [])
        # devtool inputs are not required when a different engine is selected.
        devtool_only = SimpleNamespace(images="i", bin_dir="b", zingo="z", evidence="e",
                                       vizor_engine=None, vizor_source=None, vizor_provenance=None,
                                       devtool=None, devtool_source=None,
                                       zally_source=None, zally_binary=None,
                                       native_probe_python=None, native_proto_dir=None)
        self.assertNotIn("devtool", required("vizor", devtool_only))

    def test_zally_scope_names_residual_fee_and_pending(self):
        evidence = {entry["capability"]: entry for entry in MODULE["missing_api_evidence"]("zally")}
        self.assertIn("payer fee read", evidence)
        self.assertIn("residual-only", evidence["payer fee read"]["detail"])
        self.assertIn("wallet shielded pending state", evidence)
        self.assertEqual(evidence["wallet shielded pending state"]["status"],
                         "unexercised (no public API)")
        self.assertIn("outgoing transaction history", evidence)

    def test_source_has_no_host_specific_defaults(self):
        source = (SCRIPTS / "payment-roles.py").read_text()
        for marker in ("/home/ubuntu/", "RELEASE_BIN", "ZALLY_SOURCE", "ZALLY_BUILD",
                       "ZALLY_BINARY", "VIZOR_ENGINE", "REVISION = ", "STATE = ",
                       "IMAGES = {", "global ZINGO"):
            self.assertNotIn(marker, source, f"host-specific default {marker!r} survives")
        self.assertIn('"--release-tag", required=True', source)
        self.assertIn('"--revision"', source)


if __name__ == "__main__":
    unittest.main()
