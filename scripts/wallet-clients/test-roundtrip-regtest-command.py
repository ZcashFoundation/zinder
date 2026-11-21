#!/usr/bin/env python3
"""Check orchestration and cleanup without starting a chain."""
import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


source = Path(__file__).resolve().parents[1] / "run-wallet-roundtrip-regtest.py"
spec = importlib.util.spec_from_file_location("roundtrip_regtest", source)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


class RepeatCommandTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.state = Path(scratch.name) / "new"
        self.args = argparse.Namespace(state=self.state, port_bases=(32000, 32100), images="images",
                                       bin_dir="bin", revision="a" * 40, zingo="zingo", zallet_backend="zallet")
        self.calls = []
        self.projects = []
        self.fail = None

    def command(self, script, args, log):
        self.calls.append((script, args))
        if script == tool.FIXTURE and args[0] == "up":
            project = args[args.index("--project") + 1]
            self.projects.append(project)
            fixture = self.state / "projects" / project
            fixture.mkdir(parents=True)
            (fixture / "state.json").write_text("{}")
            self.assertEqual(args[args.index("--raw-blob-policy") + 1], "all")
            self.assertEqual(args[args.index("--node-config") + 1].name, "regtest-nu6-at-one.toml")
        if log.name == self.fail:
            raise RuntimeError("requested fixture test failure")
        log.write_text("{}")

    def campaign(self):
        with patch.object(tool, "check_ports"), patch.object(tool, "execute", side_effect=self.command):
            tool.campaign(self.args)

    def test_runs_two_independent_wallets_then_compares_audits_and_cleans(self):
        self.campaign()
        self.assertEqual(len(set(self.projects)), 2)
        stages = [(p.name, args[0]) for p, args in self.calls]
        self.assertEqual([verb for _, verb in stages], ["up", "regtest", "up", "regtest", "compare", "--revision", "down", "down"])
        self.assertEqual([args[args.index("--wallet-rpc-port") + 1] for p, args in self.calls
                          if p == tool.WALLET and args[0] == "regtest"], [32082, 32182])
        self.assertTrue((self.state / "paired.json").is_file())
        self.assertEqual(json.loads((self.state / "cleanup.json").read_bytes()), {"first": "removed", "second": "removed"})

    def test_partial_second_fixture_startup_cleans_only_owned_projects(self):
        self.fail = "second-up.log"
        with self.assertRaisesRegex(RuntimeError, "requested fixture test failure"):
            self.campaign()
        downs = [args for p, args in self.calls if p == tool.FIXTURE and args[0] == "down"]
        self.assertEqual([a[a.index("--project") + 1] for a in downs], list(reversed(self.projects)))
        self.assertTrue(all(a[a.index("--state-root") + 1] == self.state / "projects" for a in downs))
        self.assertFalse(any(p == tool.AUDIT for p, _ in self.calls))

    def test_first_teardown_error_still_attempts_second_cleanup(self):
        self.fail = "second-down.log"
        with self.assertRaisesRegex(RuntimeError, "fixture cleanup incomplete"):
            self.campaign()
        receipt = json.loads((self.state / "cleanup.json").read_bytes())
        self.assertTrue(receipt["second"].startswith("cleanup failed"))
        self.assertEqual(receipt["first"], "removed")

    def test_existing_state_or_colliding_wallet_port_fails_before_provisioning(self):
        self.state.mkdir()
        with self.assertRaises(FileExistsError):
            self.campaign()
        self.assertEqual(self.calls, [])
        with self.assertRaises(ValueError):
            tool.check_ports((32000, 32080))  # first wallet port equals second native port


if __name__ == "__main__":
    unittest.main()
