#!/usr/bin/env python3
"""Exercise the paired benchmark runner through its CLI."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).with_name("run-benchmark-campaign.py").resolve()


class BenchmarkCampaignTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.output = self.root / "evidence"
        self.campaign = {"arms": {"before": {"name": "before"}, "after": {"name": "after"}}, "cases": {}}

    def run_campaign(self, status):
        manifest = self.root / "campaign.json"
        manifest.write_text(json.dumps(self.campaign), encoding="utf-8")
        completed = subprocess.run(
            [sys.executable, str(RUNNER), str(manifest), "--output", str(self.output)],
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, status, completed.stderr)
        return completed

    def read_json(self, path):
        return json.loads((self.output / path).read_text(encoding="utf-8"))

    def test_pairs_preserve_order_and_native_reports(self):
        (self.root / "heights.txt").write_text("10\n", encoding="utf-8")
        self.campaign["inputs"] = ["heights.txt"]
        self.campaign["cases"]["range"] = {
            "command": [
                sys.executable,
                "-c",
                "import json, sys; print(json.dumps({'arm': sys.argv[1], 'blocks': [10]}))",
                "{name}",
            ],
            "expect": {"/blocks/0": 10},
            "match": ["/blocks"],
        }

        self.run_campaign(0)
        summary = self.read_json("summary.json")
        self.assertEqual([pair["order"] for pair in summary], [
            ["before", "after"], ["after", "before"], ["after", "before"], ["before", "after"],
        ])
        self.assertTrue(all(pair["mismatched_fields"] == [] for pair in summary))
        self.assertEqual(self.read_json("range/round-1/after/report.json"), {"arm": "after", "blocks": [10]})
        provenance = self.read_json("provenance.json")
        self.assertIn(str(self.root / "heights.txt"), provenance["inputs_sha256"])
        self.assertIn(str(Path(sys.executable).absolute()), provenance["executables_sha256"])

    def test_failures_keep_reports_and_run_later_pairs(self):
        self.campaign["repeats"] = 1
        self.campaign["cases"] = {
            "mismatch": {
                "command": [sys.executable, "-c", "import json, sys; print(json.dumps({'arm': sys.argv[1]}))", "{name}"],
                "match": ["/arm"],
            },
            "failed": {
                "command": [
                    sys.executable,
                    "-c",
                    "from pathlib import Path; import sys; Path(sys.argv[1]).mkdir(); "
                    "Path(sys.argv[2]).write_text('{\"blocks\": 1}'); raise SystemExit(7)",
                    "{scratch}",
                    "{report}",
                ],
            },
            "last": {"command": [sys.executable, "-c", "print('{\"blocks\": 1}')"]},
        }

        self.run_campaign(1)
        first, second, third = self.read_json("summary.json")
        self.assertEqual(first["mismatched_fields"], ["/arm"])
        self.assertEqual(second["runs"]["before"]["problems"], ["command exited 7"])
        self.assertEqual(third["runs"]["before"]["problems"], [])
        self.assertEqual(self.read_json("failed/round-1/before/report.json"), {"blocks": 1})
        self.assertTrue((self.output / "failed/round-1/before/scratch").is_dir())

    def test_json_comparisons_distinguish_booleans_from_numbers(self):
        before = {"ready": True, "stats": {"calls": [False]}, "blocks": 1}
        after = {"ready": 1, "stats": {"calls": [0]}, "blocks": 1.0}
        self.campaign["arms"]["before"]["report_payload"] = json.dumps(before)
        self.campaign["arms"]["after"]["report_payload"] = json.dumps(after)
        self.campaign["repeats"] = 1
        self.campaign["cases"]["json-types"] = {
            "command": [sys.executable, "-c", "import sys; print(sys.argv[1])", "{report_payload}"],
            "expect": {"/ready": True, "/stats": before["stats"], "/blocks": 1},
            "match": ["/ready", "/stats", "/blocks"],
        }

        self.run_campaign(1)
        pair, = self.read_json("summary.json")
        self.assertEqual(pair["runs"]["before"]["problems"], [])
        self.assertEqual(pair["runs"]["after"]["problems"], [
            "unexpected value at /ready", "unexpected value at /stats",
        ])
        self.assertEqual(pair["mismatched_fields"], ["/ready", "/stats"])

    def test_refuses_to_overwrite_evidence(self):
        self.campaign["cases"]["range"] = {"command": [sys.executable, "-c", "print('{\"blocks\": 1}')"]}
        self.output.mkdir()
        existing = self.output / "summary.json"
        existing.write_text("retained evidence", encoding="utf-8")

        self.run_campaign(2)
        self.assertEqual(existing.read_text(encoding="utf-8"), "retained evidence")

    def test_reserved_case_name_fails_preflight(self):
        self.campaign["cases"]["summary.json"] = {"command": [sys.executable, "-c", "print('{}')"]}

        completed = self.run_campaign(2)
        self.assertIn("reserved for campaign metadata", completed.stderr)
        self.assertFalse(self.output.exists())

    def test_nonfinite_reports_are_failed_trials(self):
        self.campaign["repeats"] = 1
        for name, number in [("nan", "NaN"), ("overflow", "1e400")]:
            self.campaign["cases"][name] = {
                "command": [sys.executable, "-c", f"print('{{\"blocks\": {number}}}')"],
                "match": ["/blocks"],
            }

        self.run_campaign(1)
        summary = self.read_json("summary.json")
        self.assertEqual(len(summary), 2)
        for pair in summary:
            self.assertIn("non-finite JSON number", pair["runs"]["before"]["problems"][0])
            self.assertTrue((self.output / pair["runs"]["before"]["path"] / "stdout.log").exists())

    @unittest.skipUnless(os.name == "posix", "requires POSIX symlinks and executable scripts")
    def test_invokes_the_selected_symlink(self):
        executable = self.root / "client"
        executable.write_text(
            f"#!{sys.executable}\nimport json, sys; print(json.dumps({{'path': sys.argv[0]}}))\n",
            encoding="utf-8",
        )
        executable.chmod(0o700)
        alias = self.root / "client-alias"
        alias.symlink_to(executable)
        self.campaign["repeats"] = 1
        self.campaign["cases"]["alias"] = {"command": [str(alias)], "expect": {"/path": str(alias)}}

        self.run_campaign(0)
        self.assertIn(str(alias), self.read_json("provenance.json")["executables_sha256"])

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_timeout_stops_processes_started_by_a_wrapper(self):
        marker = self.root / "child-still-running"
        child = "import sys, time; from pathlib import Path; time.sleep(1.5); Path(sys.argv[1]).touch()"
        wrapper = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}, sys.argv[1]]); time.sleep(30)"
        self.campaign["repeats"] = 1
        self.campaign["timeout_seconds"] = 1
        self.campaign["cases"]["timed"] = {"command": [sys.executable, "-c", wrapper, str(marker)]}

        self.run_campaign(1)
        summary = self.read_json("summary.json")
        self.assertIn("command timed out", summary[0]["runs"]["before"]["problems"][0])
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
