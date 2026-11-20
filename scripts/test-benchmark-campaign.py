#!/usr/bin/env python3
"""Exercise the paired benchmark runner through its CLI."""

import json
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
        self.assertIn(str(Path(sys.executable).resolve()), provenance["executables_sha256"])

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

    def test_refuses_to_overwrite_evidence(self):
        self.campaign["cases"]["range"] = {"command": [sys.executable, "-c", "print('{\"blocks\": 1}')"]}
        self.output.mkdir()
        existing = self.output / "summary.json"
        existing.write_text("retained evidence", encoding="utf-8")

        self.run_campaign(2)
        self.assertEqual(existing.read_text(encoding="utf-8"), "retained evidence")


if __name__ == "__main__":
    unittest.main()
