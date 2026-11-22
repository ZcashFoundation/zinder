#!/usr/bin/env python3
"""Decide a paired benchmark campaign: regression, improvement, mixed, or no clear change.

Reads the evidence directory written by run-benchmark-campaign.py (provenance.json,
summary.json, and each run's report.json) and a caller-owned policy that names the
baseline and candidate arms and, per metric, a JSON pointer into the report, an
explicit direction, and the tolerance below which a difference is not meaningful.

Exit status: 0 improvement or no clear change, 1 regression, 2 usage error,
3 mixed signals, 4 incomplete or invalid evidence (takes precedence).
"""

import argparse
import importlib.util
import json
import math
from pathlib import Path
import statistics
import sys


SCHEMA = "zinder.benchmark-comparison/v1"
DIRECTIONS = {"lower_is_better": 1, "higher_is_better": -1}
EXIT = {"improvement": 0, "no_clear_change": 0, "regression": 1, "mixed": 3, "invalid": 4}
RUNNER = Path(__file__).with_name("run-benchmark-campaign.py")


def load_runner():
    spec = importlib.util.spec_from_file_location("run_benchmark_campaign", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load_runner()


def is_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def read_policy(path):
    policy = runner.read_json(path)
    if not isinstance(policy, dict):
        raise ValueError("policy must be a JSON object")
    baseline, candidate = policy.get("baseline"), policy.get("candidate")
    if not isinstance(baseline, str) or not isinstance(candidate, str) or baseline == candidate:
        raise ValueError("policy needs distinct baseline and candidate arm names")
    metrics = policy.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        raise ValueError("policy needs a nonempty metrics array")
    seen = set()
    for metric in metrics:
        if not isinstance(metric, dict):
            raise ValueError("each metric must be an object")
        name = metric.get("name")
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError(f"metric names must be unique nonempty strings: {name!r}")
        seen.add(name)
        if not runner.is_pointer(metric.get("pointer")):
            raise ValueError(f"{name}: pointer must be a JSON pointer")
        if metric.get("direction") not in DIRECTIONS:
            raise ValueError(f"{name}: direction must be one of {sorted(DIRECTIONS)}")
        tolerances = [key for key in ("tolerance_absolute", "tolerance_relative") if key in metric]
        if not tolerances:
            raise ValueError(f"{name}: set tolerance_absolute and/or tolerance_relative")
        for key in tolerances:
            if not is_number(metric[key]) or metric[key] < 0:
                raise ValueError(f"{name}: {key} must be a finite number >= 0")
        cases = metric.get("cases")
        if cases is not None and (not isinstance(cases, list) or not cases or
                                  not all(isinstance(case, str) for case in cases)):
            raise ValueError(f"{name}: cases must be a nonempty array of case names")
        if not isinstance(metric.get("allow_zero", False), bool):
            raise ValueError(f"{name}: allow_zero must be a boolean")
    min_pairs = policy.get("min_pairs", 2)
    if not runner.positive_int(min_pairs):
        raise ValueError("min_pairs must be a positive integer")
    return baseline, candidate, metrics, min_pairs


def expected_order(arms, repetition):
    return arms if repetition % 4 in (0, 1) else arms[::-1]


def check_structure(evidence, campaign, summary, baseline, candidate, min_pairs, issues):
    """Validate the matched design; returns {(case, repetition): pair} for usable pairs."""
    arms = list(campaign["arms"])
    cases = list(campaign["cases"])
    repeats = campaign.get("repeats", 4)
    for arm in (baseline, candidate):
        if arm not in arms:
            issues.append({"kind": "unknown_arm", "arm": arm, "arms": arms})
    if set(arms) != {baseline, candidate}:
        return {}
    if repeats < min_pairs:
        issues.append({"kind": "too_few_planned_pairs", "repeats": repeats, "min_pairs": min_pairs})
    if not isinstance(summary, list):
        issues.append({"kind": "summary_not_a_list"})
        return {}

    pairs = {}
    for pair in summary:
        try:
            key = (pair["case"], pair["repetition"])
        except (KeyError, TypeError):
            issues.append({"kind": "malformed_pair"})
            continue
        if key in pairs:
            issues.append({"kind": "duplicate_pair", "case": key[0], "repetition": key[1]})
        pairs[key] = pair

    usable = {}
    for case in cases:
        for repetition in range(1, repeats + 1):
            pair = pairs.get((case, repetition))
            where = {"case": case, "repetition": repetition}
            if pair is None:
                issues.append({"kind": "missing_pair", **where})
                continue
            runs = pair.get("runs")
            if not isinstance(runs, dict) or set(runs) != {baseline, candidate}:
                issues.append({"kind": "unequal_arms", **where, "arms": sorted(runs) if isinstance(runs, dict) else None})
                continue
            if pair.get("order") != expected_order(arms, repetition):
                issues.append({"kind": "unexpected_order", **where, "order": pair.get("order")})
                continue
            bad = False
            for arm in (baseline, candidate):
                problems = runs[arm].get("problems")
                if problems != []:
                    issues.append({"kind": "run_problems", **where, "arm": arm, "problems": problems})
                    bad = True
            if pair.get("mismatched_fields") != []:
                issues.append({"kind": "mismatched_fields", **where, "fields": pair.get("mismatched_fields")})
                bad = True
            if not bad:
                usable[case, repetition] = pair
    for case, repetition in pairs.keys() - {(c, r) for c in cases for r in range(1, repeats + 1)}:
        issues.append({"kind": "unplanned_pair", "case": case, "repetition": repetition})
    return usable


def read_metric(evidence, run, pointer, allow_zero):
    """Return (value, reason); reason is set when the observation is unusable."""
    path = (evidence / run["path"]).resolve()
    if evidence.resolve() not in path.parents:
        return None, "run path escapes the evidence directory"
    try:
        value = runner.at_pointer(runner.read_json(path / "report.json"), pointer)
    except (OSError, ValueError):
        return None, "report unreadable"
    except (IndexError, KeyError):
        return None, "metric missing"
    if not is_number(value):
        return None, "metric is not a finite number"
    if value < 0 or (value == 0 and not allow_zero):
        return None, "metric is negative" if value < 0 else "metric is zero"
    return value, None


def meaningful(delta, baseline, metric):
    """True when |delta| exceeds every configured tolerance."""
    absolute = metric.get("tolerance_absolute")
    relative = metric.get("tolerance_relative")
    if absolute is not None and not abs(delta) > absolute:
        return False
    return relative is None or abs(delta) > relative * baseline


def classify_pair(delta, baseline, metric):
    if not meaningful(delta, baseline, metric):
        return "neutral"
    return "worse" if delta * DIRECTIONS[metric["direction"]] > 0 else "better"


def assess_metric(evidence, case, metric, usable, repeats, min_pairs, issues):
    name = metric["name"]
    where = {"case": case, "metric": name}
    pairs = []
    for repetition in range(1, repeats + 1):
        pair = usable.get((case, repetition))
        if pair is None:
            continue
        values = {}
        for role, arm in (("baseline", metric["_baseline"]), ("candidate", metric["_candidate"])):
            value, reason = read_metric(evidence, pair["runs"][arm], metric["pointer"], metric.get("allow_zero", False))
            if reason:
                issues.append({"kind": "invalid_metric", **where, "repetition": repetition, "arm": arm, "reason": reason})
            values[role] = value
        if None in values.values():
            continue
        delta = values["candidate"] - values["baseline"]
        entry = {
            "repetition": repetition,
            "order": pair["order"],
            "baseline": values["baseline"],
            "candidate": values["candidate"],
            "delta": delta,
            "outcome": classify_pair(delta, values["baseline"], metric),
        }
        if values["baseline"] > 0:
            entry["relative_delta"] = delta / values["baseline"]
        pairs.append(entry)

    assessed = {**where, "direction": metric["direction"], "pairs": pairs}
    for key in ("tolerance_absolute", "tolerance_relative"):
        if key in metric:
            assessed[key] = metric[key]
    if len(pairs) < repeats or len(pairs) < min_pairs:
        issues.append({"kind": "incomplete_metric", **where, "observed_pairs": len(pairs), "planned_pairs": repeats})
        assessed["verdict"] = "invalid"
        return assessed

    median_baseline = statistics.median(entry["baseline"] for entry in pairs)
    median_delta = statistics.median(entry["delta"] for entry in pairs)
    assessed["median_baseline"] = median_baseline
    assessed["median_delta"] = median_delta
    if median_baseline > 0:
        assessed["median_relative_delta"] = median_delta / median_baseline
    worse = sum(entry["outcome"] == "worse" for entry in pairs)
    better = sum(entry["outcome"] == "better" for entry in pairs)
    assessed["pairs_worse"], assessed["pairs_better"] = worse, better
    median_outcome = classify_pair(median_delta, median_baseline, metric)
    if worse and better:
        verdict = "mixed"
    elif worse and median_outcome == "worse":
        verdict = "regression"
    elif better and median_outcome == "better":
        verdict = "improvement"
    else:
        verdict = "no_clear_change"
    assessed["verdict"] = verdict
    return assessed


def overall(verdicts):
    if "invalid" in verdicts:
        return "invalid"
    kinds = set(verdicts)
    if "mixed" in kinds or {"regression", "improvement"} <= kinds:
        return "mixed"
    for kind in ("regression", "improvement"):
        if kind in kinds:
            return kind
    return "no_clear_change"


def compare(evidence, policy_path):
    baseline, candidate, metrics, min_pairs = read_policy(policy_path)
    provenance = runner.read_json(evidence / "provenance.json")
    summary = runner.read_json(evidence / "summary.json")
    campaign = provenance["campaign"]
    repeats = campaign.get("repeats", 4)
    cases = list(campaign["cases"])
    issues = []
    usable = check_structure(evidence, campaign, summary, baseline, candidate, min_pairs, issues)

    for metric in metrics:
        unknown = set(metric.get("cases") or []) - set(cases)
        if unknown:
            raise ValueError(f"{metric['name']}: unknown cases {sorted(unknown)}")
        metric["_baseline"], metric["_candidate"] = baseline, candidate

    assessments = []
    covered = set()
    for metric in metrics:
        for case in metric.get("cases") or cases:
            covered.add(case)
            if usable or not issues:
                assessments.append(assess_metric(evidence, case, metric, usable, repeats, min_pairs, issues))
    verdicts = [item["verdict"] for item in assessments]
    verdict = "invalid" if issues else overall(verdicts)
    if not assessments:
        verdict = "invalid"
    return {
        "schema": SCHEMA,
        "verdict": verdict,
        "baseline": baseline,
        "candidate": candidate,
        "scope": {
            "cases_assessed": sorted(covered),
            "cases_without_metrics": [case for case in cases if case not in covered],
            "pairs_per_case": repeats,
            "host": provenance.get("host"),
            "machine": provenance.get("machine"),
            "claim": "Applies only to the cases, arms, inputs, and host recorded in this campaign; "
                     "it is not a claim about other hosts, networks, or fleets.",
        },
        "issues": issues,
        "metrics": assessments,
    }


def render(report):
    lines = [f"{report['verdict']}: {report['candidate']} vs baseline {report['baseline']}"]
    for item in report["metrics"]:
        median = item.get("median_relative_delta")
        shown = f"{median:+.1%}" if median is not None else f"{item.get('median_delta')}"
        lines.append(f"  {item['case']}/{item['metric']}: {item['verdict']} (median delta {shown}, "
                     f"{item.get('pairs_worse', 0)} worse, {item.get('pairs_better', 0)} better of {len(item['pairs'])})")
    for issue in report["issues"]:
        lines.append(f"  issue: {json.dumps(issue, sort_keys=True)}")
    unassessed = report["scope"]["cases_without_metrics"]
    if unassessed:
        lines.append(f"  not assessed (no metric): {', '.join(unassessed)}")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("evidence", type=Path, help="output directory of run-benchmark-campaign.py")
    parser.add_argument("--policy", required=True, type=Path, help="JSON naming baseline, candidate, and metrics")
    parser.add_argument("--output", type=Path, help="also write the decision JSON to this new file")
    args = parser.parse_args(argv)
    try:
        report = compare(args.evidence, args.policy)
    except (AttributeError, KeyError, OSError, TypeError, ValueError) as error:
        parser.error(str(error))
    document = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if args.output:
        try:
            with args.output.open("x", encoding="utf-8") as sink:
                sink.write(document)
        except OSError as error:
            parser.error(str(error))
    sys.stdout.write(document)
    print(render(report), file=sys.stderr)
    return EXIT[report["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
