#!/usr/bin/env python3
"""Exercise the publication census at its operator CLI."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


COMMAND = Path(__file__).with_name("report-publication-delay.py")
APPEND = "canonical_live_append_committed"
REPLACE = "canonical_live_replacement_committed"
FORK = "canonical_source_fork_discovered"
PUBLISH = "wallet_serving_pair_publisher_published"
PROJECT = "wallet_projection_following_applied"
SELECT = "wallet_serving_pair_publisher_generation_selected"
START = "wallet_serving_pair_publisher_generation_wait_started"
END = "wallet_serving_pair_publisher_generation_wait_ended"


def ts(second):
    return f"2026-11-20T12:00:{second:02d}Z"


def trace(t, name, **fields):
    message = f'{ts(t)} INFO example: event="{name}"'
    return json.dumps({"message": message + "".join(f" {k}={v}" for k, v in fields.items())})


def identity(byte):
    return f"BlockHash({[byte] * 32})"


def commit(t, seq, height, previous=None, parent=None):
    fields = dict(chain_epoch=seq, chain_event_sequence=seq, visible_tip_height=height,
                  visible_tip_hash=identity(seq), sequence_digest=[seq + 10] * 32, source_tip_height=height)
    if previous:
        fields.update(previous_tip_height=previous[0], previous_tip_hash=identity(previous[1]))
    if parent:
        fields.update(common_parent_height=parent[0], common_parent_hash=identity(parent[1]))
    return trace(t, REPLACE if previous else APPEND, **fields)


def observed(t, seq, height, name=PUBLISH, legacy=False, **updates):
    fields = dict(chain_epoch=seq, event_sequence=seq, visible_height=height)
    if not legacy:
        fields.update(visible_tip_hash=identity(seq), sequence_digest=[seq + 10] * 32,
                      process_id=42 if name == PUBLISH else 43)
    if name == PUBLISH:
        fields["generation"] = 1
    return trace(t, name, **(fields | updates))


def select(t, seq):
    return trace(t, SELECT, process_id=42, chain_epoch=seq, event_sequence=seq, generation=1)


class PublicationDelay(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.scope = dict(release="example", deployment="deployment-a", replica="one", store_lineage="volume-a",
                          event_schema="fence", coverage_start=ts(0), coverage_end=ts(59), coverage_gaps=[],
                          writer=dict(lifetime="writer-a", log="service.jsonl"),
                          reader=dict(lifetime="reader-a", log="service.jsonl", role="query", pid=42))

    def run_report(self, rows, start=1, end=20, through=40, valid=True):
        scope = self.directory / "scope.json"
        scope.write_text(json.dumps(self.scope))
        (self.directory / "service.jsonl").write_text("\n".join(rows) + "\n")
        command = [sys.executable, str(COMMAND), str(scope), "--start", ts(start), "--end", ts(end)]
        if through is not None:
            command.extend(["--through", ts(through)])
        process = subprocess.run(command, capture_output=True, text=True)
        if valid:
            self.assertEqual(process.returncode, 0, process.stderr)
            return json.loads(process.stdout)
        self.assertEqual(process.returncode, 2)
        self.assertEqual(process.stdout, "")
        self.assertNotIn("Traceback", process.stderr)
        return process.stderr

    def test_same_height_fork_resolves_only_surviving_fences_and_cutoff_is_exclusive(self):
        a, b = commit(1, 1, 100), commit(3, 2, 101)
        c = commit(8, 3, 101, previous=(101, 2), parent=(100, 1))
        publication = observed(20, 3, 101)
        result = self.run_report([publication, c, a, b, b, publication])
        self.assertEqual(result["at_cutoff"]["counts"], dict(exact=0, coalesced=0, unmatched=0,
                                                             superseded=1, pending=2))
        summary = result["as_of"]
        self.assertEqual(summary["committed_observed"], sum(summary["counts"].values()))
        self.assertEqual(summary["delay_seconds"]["first_covering"],
                         dict(n=2, p50=15.5, p90=18.3, p99=18.93, max=19))
        self.assertEqual([r["outcome"] for r in result["observations"]], ["coalesced", "superseded", "exact"])
        self.assertEqual(result["coverage"]["events"][PUBLISH]["duplicates"], 1)
        self.assertEqual(result["coverage"]["events"][APPEND]["duplicates"], 1)

    def test_legacy_discovery_must_match_committed_replacement_unambiguously(self):
        self.scope["event_schema"] = "v0.17.0"
        del self.scope["reader"]["pid"]
        rows = [commit(1, 1, 100), commit(3, 2, 101), commit(10, 3, 101, previous=(101, 2)),
                observed(12, 3, 101, legacy=True)]
        discovery = dict(common_parent_height=100, common_parent_hash=identity(1), local_tip_height=101,
                         source_tip_height=101, replacement_tip_height=101, replacement_block_count=1)
        known = self.run_report(rows + [trace(9, FORK, **discovery)])
        self.assertEqual(known["as_of"]["counts"]["superseded"], 1)
        self.assertIsNone(known["exact_fence_stage_seconds"])
        self.assertIsNone(known["reader_waits"])
        for discoveries in ([], [trace(8, FORK, **discovery), trace(9, FORK, **discovery)]):
            summary = self.run_report(rows + discoveries)["as_of"]
            self.assertEqual(summary["counts"]["unmatched"], 2)
            self.assertEqual(summary["unverified_transitions"], [3])

    def test_missing_sequence_breaks_ancestry_and_denominator(self):
        summary = self.run_report([commit(2, 1, 100), commit(8, 3, 102), observed(10, 3, 102)])["as_of"]
        self.assertTrue(summary["writer_gap_touches_cohort"])
        self.assertEqual(summary["missing_writer_ranges"], [[2, 2]])
        self.assertEqual(summary["counts"]["unmatched"], 1)
        self.assertEqual(summary["counts"]["exact"], 1)

    def test_conflicting_fence_or_reversed_clock_cannot_measure_a_delay(self):
        c = commit(8, 1, 100)
        for p in [observed(6, 1, 100), observed(9, 1, 100, visible_tip_hash=identity(9)),
                  observed(9, 1, 100, sequence_digest=[42] * 32), observed(9, 1, 100, chain_epoch=2)]:
            summary = self.run_report([c, p])["as_of"]
            self.assertEqual(summary["counts"]["unmatched"], 1)
            self.assertEqual(summary["delay_seconds"]["exact"]["n"], 0)

    def test_censored_tail_and_restarts(self):
        rows = [commit(1, 1, 100), observed(2, 1, 100), commit(9, 2, 101), observed(40, 2, 101)]
        self.scope["coverage_end"], self.scope["ends_at_restart"] = ts(40), True
        report = self.run_report(rows, through=None)
        self.assertEqual(report["as_of"]["counts"]["pending"], 1)
        after = self.run_report(rows)["observations"][-1]
        self.assertEqual((after["outcome"], after["reason"]), ("unmatched", "process_interval_ended"))
        self.assertIn("coverage", self.run_report(rows, through=41, valid=False))
        self.assertIn("PID", self.run_report(rows + [observed(11, 2, 101, process_id=20)], valid=False))

    def test_ambiguous_lifetimes_and_explicit_gaps_fail_without_successful_output(self):
        rows = [commit(1, 1, 100), observed(2, 1, 100)]
        for collision in [commit(3, 1, 100), observed(3, 1, 101), observed(3, 0, 100)]:
            self.run_report(rows + [collision], valid=False)
        self.scope["coverage_gaps"] = [[ts(3), ts(4)]]
        self.assertIn("coverage gap", self.run_report(rows, valid=False))

    def test_optional_stages_omit_unproved_matches_and_preserve_overlap(self):
        self.scope["projector"] = dict(lifetime="projector-a", pid=43, log="service.jsonl")
        rows = [commit(1, 1, 100), select(4, 1), observed(6, 1, 100, PROJECT), observed(7, 1, 100),
                commit(8, 2, 101), select(9, 2), select(10, 2),
                observed(11, 2, 101, PROJECT, sequence_digest=[1] * 32), observed(12, 2, 101)]
        report = self.run_report(rows)
        stages = report["exact_fence_stage_seconds"]
        self.assertEqual(stages["commit_to_projection"]["n"], 1)
        self.assertEqual(stages["commit_to_projection"]["p50"], 5)
        self.assertEqual(stages["selection_to_publication"]["p50"], 3)
        self.assertEqual(stages["selection_to_publication"]["n"], 1)
        self.assertEqual(stages["projection_to_selection"]["n"], 0)

    def test_wait_intervals_use_owner_elapsed_and_keep_unpaired_events(self):
        rows = [commit(1, 1, 100), observed(12, 1, 100),
                trace(3, START, reason='"retention_window"', chain_epoch=1, event_sequence=1, process_id=42),
                trace(10, END, reason='"retention_window"', outcome='"available"', elapsed_seconds=6.8, process_id=42),
                trace(11, START, reason='"in_flight_requests"', chain_epoch=1, event_sequence=1, process_id=42)]
        intervals = self.run_report(rows)["reader_waits"]
        self.assertEqual(intervals["complete"][0]["elapsed_seconds"], 6.8)
        self.assertEqual(intervals["complete"][0]["start_sequence"], 1)
        self.assertEqual(intervals["unpaired_events"], 1)

    def test_version_one_rejects_constant_epoch_offset(self):
        rows = [commit(1, 5, 100).replace("chain_epoch=5", "chain_epoch=10"),
                commit(2, 6, 101).replace("chain_epoch=6", "chain_epoch=11"),
                observed(9, 6, 101, chain_epoch=11)]
        self.assertIn("epoch and sequence", self.run_report(rows, valid=False))

    def test_first_publication_wins_on_legitimate_same_fence_republication(self):
        c = commit(1, 3, 100)
        p = observed(2, 3, 100)
        later = observed(8, 3, 100, generation=2)
        unrelated = json.dumps({"message": "unrelated " + "x" * (1024 * 1024)})
        report = self.run_report([later, unrelated, p, c, p])
        self.assertEqual(report["as_of"]["counts"]["exact"], 1)
        self.assertEqual(report["as_of"]["delay_seconds"]["exact"]["max"], 1)
        self.assertEqual(report["coverage"]["events"][PUBLISH], dict(n=2, duplicates=1, last=ts(8)))

    def test_malformed_recognized_fields_are_rejected(self):
        c = commit(1, 1, 100)
        p = observed(2, 1, 100)
        for bad in [p.replace("generation=1", "generation=1 generation=2"),
                    p.replace("visible_height=100", "visible_height=4294967296"),
                    p.replace("visible_height=100", "visible_height=-1"),
                    p.replace("]) ", "])trailing ", 1), p.replace("[1,", "[999,", 1)]:
            self.run_report([c, bad], valid=False)

    def test_bad_input_and_silent_sources_fail_closed(self):
        self.run_report([commit(1, 1, 100)], valid=False)
        error = self.run_report([commit(1, 1, 100), '{"sensitive": "never expose payload"}'], valid=False)
        self.assertNotIn("never expose", error)
        self.run_report([commit(1, 1, 100), observed(2, 1, 100, legacy=True)], valid=False)


if __name__ == "__main__":
    unittest.main()
