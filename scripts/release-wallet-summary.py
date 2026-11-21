#!/usr/bin/env python3
"""Render the allowlisted release wallet gate summary as Markdown for a job summary."""
from pathlib import Path
import json
import sys


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: release-wallet-summary.py SUMMARY_JSON")
    summary = json.loads(Path(sys.argv[1]).read_text())
    comparison = summary.get("comparison", {})
    candidate = summary.get("candidate", {})
    print("### Release wallet round-trip gate")
    print()
    print(f"- Status: `{summary.get('status')}`")
    print(f"- Stage: `{summary.get('stage')}`")
    print(f"- Candidate: `{candidate.get('tag')}` at `{candidate.get('commit')}`")
    print(f"- Versioned baseline: `{comparison.get('baseline_tag', 'not reached')}`")
    print(f"- Four versioned rows: {comparison.get('rows', 0)}")
    print(f"- Comparison issues: {comparison.get('issues', 'not reached')}")
    print(f"- Strict comparison changes: {comparison.get('changes', 'not reached')}")
    if summary.get("error"):
        print(f"- Reason: {summary['error']}")
    withheld = summary.get("safety", {}).get("withheld", [])
    if withheld:
        print(f"- Withheld aggregate files: {len(withheld)}")


if __name__ == "__main__":
    main()
