#!/usr/bin/env python3
"""Check orchestration, bounded stages, and cleanup without starting a chain."""
import argparse
import importlib.util
import json
from pathlib import Path
import contextlib
import io
import sys
import tempfile
import time
import types
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
        self.root = Path(scratch.name)
        self.state = self.root / "new"
        self.args = argparse.Namespace(state=self.state, port_bases=(32000, 32100), images="images",
                                       bin_dir="bin", revision="a" * 40, zingo="zingo", zallet_backend="zallet",
                                       stage_timeout=120, stage_grace=2)
        self.calls = []
        self.projects = []
        self.events = []
        self.fail = None
        self.interrupt = None
        self.leftover = False
        self.daemon = False

    def command(self, script, args, log, timeout, grace):
        self.calls.append((script, args))
        expected_timeout = tool.CLEANUP_TIMEOUT if args[0] == "down" else self.args.stage_timeout
        self.assertEqual((timeout, grace), (expected_timeout, self.args.stage_grace))
        if script == tool.FIXTURE and args[0] == "up":
            project = args[args.index("--project") + 1]
            self.projects.append(project)
            fixture = self.state / "projects" / project
            fixture.mkdir(parents=True)
            (fixture / "state.json").write_text("{}")
            self.assertEqual(args[args.index("--raw-blob-policy") + 1], "all")
            self.assertEqual(args[args.index("--node-config") + 1].name, "regtest-nu6-at-one.toml")
        if script == tool.FIXTURE and args[0] == "down":
            self.events.append(("down", args[args.index("--project") + 1]))
        if script == tool.WALLET and args[0] == "regtest" and self.daemon:
            label = log.name.split("-")[0]
            directory = self.state / label / "zallet"
            directory.mkdir(parents=True, mode=0o700)
            (directory / "daemon.pid").write_text("{}")
            (directory / "zallet.toml").write_text("")
            (directory / "network.json").write_text(
                json.dumps({"network": "regtest", "endpoint": "http://127.0.0.1:32002"}))
        log.write_text("{}")
        if log.name == self.interrupt:
            raise KeyboardInterrupt
        if log.name == self.fail:
            raise RuntimeError("requested fixture test failure")

    def readback(self, project):
        if self.leftover and project == self.projects[0]:
            raise RuntimeError("owned Docker resources remain")

    def campaign(self):
        with patch.object(tool, "check_ports"), patch.object(tool, "execute", side_effect=self.command), \
                patch.object(tool, "require_project_gone", side_effect=self.readback), \
                patch.object(tool, "stop_owned_relay"):
            tool.campaign(self.args)

    def test_runs_two_independent_wallets_then_compares_audits_and_cleans(self):
        self.campaign()
        self.assertEqual(len(set(self.projects)), 2)
        stages = [(p.name, args[0]) for p, args in self.calls]
        self.assertEqual([verb for _, verb in stages],
                         ["up", "regtest", "up", "regtest", "compare", "--revision", "down", "down"])
        self.assertEqual([args[args.index("--wallet-rpc-port") + 1] for p, args in self.calls
                          if p == tool.WALLET and args[0] == "regtest"], [32082, 32182])
        self.assertTrue((self.state / "paired.json").is_file())
        self.assertEqual(json.loads((self.state / "cleanup.json").read_bytes()),
                         {"first": "removed", "second": "removed"})

    def test_missing_binary_pins_fail_before_provisioning(self):
        argv = [str(source), "--state", str(self.state), "--images", "images", "--port-bases", "32000", "32100",
                "--revision", "a"*40, "--zingo", "zingo", "--zallet-backend", "zallet"]
        with patch.object(sys, "argv", argv), patch.object(tool, "campaign") as campaign, \
                contextlib.redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit) as stopped:
            tool.main()
        self.assertEqual(stopped.exception.code, 2)
        self.assertIn("required: --bin-dir", stderr.getvalue())
        campaign.assert_not_called()
        self.assertFalse(self.state.exists())

    def test_stage_timeout_is_configurable_and_defaults_beyond_a_regtest_stage(self):
        argv = [str(source), "--state", str(self.state), "--images", "images", "--port-bases", "32000", "32100",
                "--revision", "a"*40, "--zingo", "zingo", "--zallet-backend", "zallet", "--bin-dir", "bin",
                "--stage-timeout", "123", "--stage-grace", "4"]
        with patch.object(sys, "argv", argv), patch.object(tool, "campaign") as campaign:
            tool.main()
        self.assertEqual((campaign.call_args.args[0].stage_timeout, campaign.call_args.args[0].stage_grace), (123, 4))
        self.assertGreaterEqual(tool.DEFAULT_STAGE_TIMEOUT, 3600)

    def test_nonpositive_stage_bounds_fail_before_provisioning(self):
        self.args.stage_timeout = 0
        with self.assertRaisesRegex(RuntimeError, "must be positive"):
            self.campaign()
        self.assertFalse(self.state.exists())

    def test_partial_second_fixture_startup_cleans_only_owned_projects(self):
        self.fail = "second-up.log"
        with self.assertRaisesRegex(RuntimeError, "requested fixture test failure"):
            self.campaign()
        downs = [args for p, args in self.calls if p == tool.FIXTURE and args[0] == "down"]
        self.assertEqual([a[a.index("--project") + 1] for a in downs], list(reversed(self.projects)))
        self.assertTrue(all(a[a.index("--state-root") + 1] == self.state / "projects" for a in downs))
        self.assertFalse(any(p == tool.AUDIT for p, _ in self.calls))

    def test_interrupt_during_second_stage_still_cleans_both_fixtures(self):
        self.interrupt = "second-wallet.log"
        with self.assertRaises(KeyboardInterrupt):
            self.campaign()
        receipt = json.loads((self.state / "cleanup.json").read_bytes())
        self.assertEqual(receipt, {"first": "removed", "second": "removed"})
        self.assertEqual(len([1 for event, _ in self.events if event == "down"]), 2)

    def test_missing_state_after_successful_audit_cannot_qualify(self):
        original = self.command

        def lose_state(script, args, log, timeout, grace):
            original(script, args, log, timeout, grace)
            if script == tool.AUDIT:
                (self.state / "projects" / self.projects[0] / "state.json").unlink()

        self.command = lose_state
        with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
            self.campaign()
        self.assertEqual(json.loads((self.state / "cleanup.json").read_bytes()),
                         {"first": "no fixture started", "second": "removed"})

    def test_first_teardown_error_still_attempts_second_cleanup(self):
        self.fail = "second-down.log"
        with self.assertRaisesRegex(RuntimeError, "fixture cleanup incomplete"):
            self.campaign()
        receipt = json.loads((self.state / "cleanup.json").read_bytes())
        self.assertTrue(receipt["second"].startswith("cleanup failed"))
        self.assertEqual(receipt["first"], "removed")

    def test_readback_leftovers_fail_cleanup_without_other_fixture_damage(self):
        self.leftover = True
        with self.assertRaisesRegex(RuntimeError, "fixture cleanup incomplete"):
            self.campaign()
        receipt = json.loads((self.state / "cleanup.json").read_bytes())
        self.assertTrue(receipt["first"].startswith("cleanup failed"))
        self.assertIn("owned Docker resources remain", receipt["first"])
        self.assertEqual(receipt["second"], "removed")

    def test_leftover_owned_zallet_is_closed_before_its_fixture_teardown(self):
        self.daemon = True
        self.interrupt = "second-up.log"
        events = self.events

        class FakeZallet:
            def __init__(self, binary, directory, network, endpoint, birthday, rpc_port, warmup_timeout=2400):
                self.directory = Path(directory)
                self.stopped = False
                self.events = events

            def _owned_identity(self):
                return None if self.stopped else (4242, 99)

            def close(self):
                self.stopped = True
                self.events.append(("close", self.directory.parent.name))

        fake = types.ModuleType("roundtrip_wallets")
        fake.Zallet = FakeZallet
        with patch.dict(sys.modules, {"roundtrip_wallets": fake}):
            with self.assertRaises(KeyboardInterrupt):
                self.campaign()
        self.assertIn(("close", "first"), self.events)
        self.assertLess(self.events.index(("close", "first")),
                        self.events.index(("down", self.projects[0])))
        self.assertIn(("down", self.projects[1]), self.events)

    def test_existing_state_or_colliding_wallet_port_fails_before_provisioning(self):
        self.state.mkdir()
        with self.assertRaises(FileExistsError):
            self.campaign()
        self.assertEqual(self.calls, [])
        with self.assertRaises(ValueError):
            tool.check_ports((32000, 32080))  # first wallet port equals second native port

    def test_hung_stage_is_killed_with_its_process_group(self):
        script = self.root / "hang.py"
        script.write_text(
            "import os, signal, subprocess, sys, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
            "with open(sys.argv[1], 'w') as out:\n"
            "    out.write(f'{os.getpid()} {child.pid}')\n"
            "time.sleep(60)\n")
        pids = self.root / "hang.pids"
        with self.assertRaisesRegex(RuntimeError, "stage limit"):
            tool.execute(script, [pids], self.root / "hang.log", timeout=0.5, grace=0.2)
        leader, descendant = map(int, pids.read_text().split())
        self.assertFalse(tool.process_alive(leader))
        self.assertFalse(tool.process_alive(descendant))

    def test_child_ignoring_termination_is_reaped_when_leader_exits_first(self):
        script, marker = self.root / "leader.py", self.root / "child.pid"
        script.write_text(
            "import signal, subprocess, sys, time\n"
            "child = subprocess.Popen([sys.executable, '-c', "
            "'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)'])\n"
            "with open(sys.argv[1], 'w') as f: f.write(str(child.pid))\n"
            "time.sleep(30)\n")
        with self.assertRaisesRegex(RuntimeError, "stage limit"):
            tool.execute(script, [marker], self.root / "group.log", timeout=.5, grace=.3)
        pid = int(marker.read_text())
        for _ in range(50):
            if not tool.process_alive(pid):
                break
            time.sleep(.01)
        self.assertFalse(tool.process_alive(pid))

    def test_signalled_stage_reports_its_exit_and_log(self):
        script = self.root / "stopped.py"
        script.write_text("import os, signal\nos.kill(os.getpid(), signal.SIGTERM)\n")
        with self.assertRaisesRegex(RuntimeError, "exit -15"):
            tool.execute(script, [], self.root / "stopped.log", timeout=10, grace=1)


if __name__ == "__main__":
    unittest.main()
