#!/usr/bin/env python3
"""Exercise the paired benchmark comparison through its CLI on synthetic campaign evidence."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


COMPARE = Path(__file__).with_name("compare-benchmark-campaign.py").resolve()
ARMS = ["before", "after"]


def order(repetition):
    return ARMS if repetition % 4 in (0, 1) else ARMS[::-1]


class CompareTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.evidence = self.root / "evidence"
        self.policy = {
            "baseline": "before",
            "candidate": "after",
            "metrics": [{"name": "latency", "pointer": "/ms", "direction": "lower_is_better",
                         "tolerance_relative": 0.05}],
        }

    def write(self, path, payload):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def build(self, cases, repeats=4):
        """cases: {name: [(before, after), ...]} one tuple per repetition."""
        self.write(self.evidence / "provenance.json", {
            "campaign": {"repeats": repeats, "arms": {arm: {} for arm in ARMS},
                         "cases": {name: {} for name in cases}},
            "host": "test-host", "machine": "x86_64",
        })
        summary = []
        for name, rounds in cases.items():
            for repetition, values in enumerate(rounds, start=1):
                runs = {}
                for arm, value in zip(ARMS, values):
                    path = f"{name}/round-{repetition}/{arm}"
                    self.write(self.evidence / path / "report.json", {"ms": value})
                    runs[arm] = {"path": path, "problems": [], "matched_fields": {}}
                summary.append({"case": name, "repetition": repetition, "order": order(repetition),
                                "runs": runs, "mismatched_fields": []})
        self.write(self.evidence / "summary.json", summary)
        return summary

    def compare(self, status, extra=()):
        self.write(self.root / "policy.json", self.policy)
        completed = subprocess.run(
            [sys.executable, str(COMPARE), str(self.evidence), "--policy", str(self.root / "policy.json"), *extra],
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(completed.returncode, status, completed.stderr)
        return json.loads(completed.stdout) if completed.stdout else None

    def issue_kinds(self, report):
        return {issue["kind"] for issue in report["issues"]}

    def test_regression_keeps_pair_and_median_deltas(self):
        self.build({"range": [(100, 120), (100, 118), (100, 125), (100, 122)]})
        report = self.compare(1)
        self.assertEqual(report["verdict"], "regression")
        metric = report["metrics"][0]
        self.assertEqual([pair["delta"] for pair in metric["pairs"]], [20, 18, 25, 22])
        self.assertEqual(metric["median_delta"], 21)
        self.assertAlmostEqual(metric["median_relative_delta"], 0.21)
        self.assertEqual(metric["pairs"][1]["order"], ["after", "before"])
        self.assertIn("not a claim about other hosts", report["scope"]["claim"])

    def test_direction_flips_regression_to_improvement(self):
        self.build({"range": [(100, 120)] * 4})
        self.policy["metrics"][0]["direction"] = "higher_is_better"
        self.assertEqual(self.compare(0)["verdict"], "improvement")

    def test_changes_inside_tolerance_are_not_clear(self):
        self.build({"range": [(100, 103), (100, 97), (100, 104), (100, 101)]})
        self.assertEqual(self.compare(0)["verdict"], "no_clear_change")

    def test_absolute_and_relative_tolerance_must_both_be_exceeded(self):
        self.build({"range": [(100, 110)] * 4})
        self.policy["metrics"][0]["tolerance_absolute"] = 15
        self.assertEqual(self.compare(0)["verdict"], "no_clear_change")

    def test_single_outlier_pair_does_not_regress(self):
        self.build({"range": [(100, 100), (100, 100), (100, 150), (100, 101)]})
        report = self.compare(0)
        self.assertEqual(report["verdict"], "no_clear_change")
        self.assertEqual(report["metrics"][0]["pairs_worse"], 1)

    def test_opposing_pairs_are_mixed(self):
        self.build({"range": [(100, 130), (100, 130), (100, 70), (100, 70)]})
        self.assertEqual(self.compare(3)["verdict"], "mixed")

    def test_regression_and_improvement_across_cases_is_mixed(self):
        self.build({"slow": [(100, 130)] * 4, "fast": [(100, 70)] * 4})
        report = self.compare(3)
        self.assertEqual({item["case"]: item["verdict"] for item in report["metrics"]},
                         {"slow": "regression", "fast": "improvement"})

    def test_metric_can_scope_to_cases_and_unassessed_cases_are_reported(self):
        self.build({"slow": [(100, 130)] * 4, "other": [(100, 100)] * 4})
        self.policy["metrics"][0]["cases"] = ["slow"]
        report = self.compare(1)
        self.assertEqual(report["scope"]["cases_without_metrics"], ["other"])

    def test_missing_pair_is_incomplete(self):
        summary = self.build({"range": [(100, 100)] * 4})
        self.write(self.evidence / "summary.json", summary[:3])
        report = self.compare(4)
        self.assertEqual(report["verdict"], "invalid")
        self.assertIn("missing_pair", self.issue_kinds(report))

    def test_unequal_arms_and_run_problems_are_invalid(self):
        summary = self.build({"range": [(100, 100)] * 4})
        del summary[0]["runs"]["after"]
        summary[1]["runs"]["before"]["problems"] = ["command exited 7"]
        summary[2]["mismatched_fields"] = ["/blocks"]
        self.write(self.evidence / "summary.json", summary)
        report = self.compare(4)
        self.assertLessEqual({"unequal_arms", "run_problems", "mismatched_fields"}, self.issue_kinds(report))

    def test_broken_order_is_invalid(self):
        summary = self.build({"range": [(100, 100)] * 4})
        summary[1]["order"] = ARMS
        self.write(self.evidence / "summary.json", summary)
        self.assertIn("unexpected_order", self.issue_kinds(self.compare(4)))

    def test_missing_invalid_and_zero_metrics_are_invalid(self):
        self.build({"range": [(100, 100)] * 4})
        self.write(self.evidence / "range/round-1/after/report.json", {})
        self.write(self.evidence / "range/round-2/after/report.json", {"ms": "fast"})
        self.write(self.evidence / "range/round-3/before/report.json", {"ms": 0})
        self.write(self.evidence / "range/round-4/after/report.json", {"ms": True})
        report = self.compare(4)
        reasons = {issue["reason"] for issue in report["issues"] if issue["kind"] == "invalid_metric"}
        self.assertEqual(reasons, {"metric missing", "metric is not a finite number", "metric is zero"})

    def test_zero_allowed_when_metric_opts_in(self):
        self.build({"range": [(0, 0)] * 4})
        self.policy["metrics"][0] = {"name": "errors", "pointer": "/ms", "direction": "lower_is_better",
                                     "tolerance_absolute": 0, "allow_zero": True}
        self.assertEqual(self.compare(0)["verdict"], "no_clear_change")

    def test_policy_errors_are_usage_errors(self):
        self.build({"range": [(100, 100)] * 4})
        del self.policy["metrics"][0]["tolerance_relative"]
        self.compare(2)
        self.policy["metrics"][0]["tolerance_relative"] = 0.05
        self.policy["candidate"] = "before"
        self.compare(2)
        self.policy["candidate"] = "after"
        self.policy["metrics"][0]["direction"] = "faster"
        self.compare(2)

    def test_unknown_arm_is_invalid(self):
        self.build({"range": [(100, 100)] * 4})
        self.policy["candidate"] = "later"
        self.assertIn("unknown_arm", self.issue_kinds(self.compare(4)))

    def test_output_file_is_written_once(self):
        self.build({"range": [(100, 100)] * 4})
        target = self.root / "decision.json"
        self.compare(0, ["--output", str(target)])
        self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["schema"], "zinder.benchmark-comparison/v1")
        self.compare(2, ["--output", str(target)])

    def test_reads_evidence_written_by_the_runner(self):
        script = ("import json, sys; print(json.dumps({'ms': 100 if sys.argv[1] == 'before' else 130}))")
        campaign = {"arms": {"before": {"name": "before"}, "after": {"name": "after"}},
                    "cases": {"range": {"command": [sys.executable, "-c", script, "{name}"]}}}
        self.write(self.root / "campaign.json", campaign)
        runner = COMPARE.with_name("run-benchmark-campaign.py")
        subprocess.run([sys.executable, str(runner), str(self.root / "campaign.json"), "--output", str(self.evidence)],
                       cwd=self.root, capture_output=True, check=True, timeout=60)
        self.assertEqual(self.compare(1)["verdict"], "regression")


if __name__ == "__main__":
    unittest.main()
