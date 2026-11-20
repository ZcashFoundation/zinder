#!/usr/bin/env python3
"""Check operator queries and the metrics reference against Zinder's emitters."""

import json
import re
import unittest
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NAME = r"zinder_[a-zA-Z0-9_]+"
QUOTED_NAME = rf'"({NAME})"'
CONSTANT = re.compile(rf"\bconst (\w+):\s*&(?:'static\s+)?str\s*=\s*{QUOTED_NAME}")
REGISTER = re.compile(r"\b(counter|gauge|histogram)!\s*\(\s*")
NAME_ARG = re.compile(r'("[^"]+"|[\w:]+|metric_names\.\w+\(\))\s*(?=[,)])')
RPC_PAIR = re.compile(rf"RpcMetricNames::for_service\(\s*{QUOTED_NAME},\s*{QUOTED_NAME}")
BUCKET_MATCHER = re.compile(r"set_buckets_for_metric\(\s*Matcher::(\w+)\(([^.]+)\.to_owned\(\)")
BENCH = ROOT / "services/zinder-bench/src/report.rs"
SMOKE = ROOT / "scripts/observability-smoke.sh"
RECORDER = ROOT / "crates/zinder-runtime/src/metrics.rs"


def production_source(path):
    source = re.sub(r"(?m)^\s*//[^\n]*", "", path.read_text())
    return re.sub(r"(?ms)^#\[cfg\(test\)\]\s*mod \w+ \{.*?^\}", "", source)


def name_from_arg(arg, local_constants, constants):
    arg = arg.strip()
    if arg.startswith('"') and arg.endswith('"'):
        return arg[1:-1]
    arg = arg.rsplit("::", 1)[-1]
    if arg in local_constants:
        return local_constants[arg]
    if len(constants.get(arg, ())) == 1:
        return next(iter(constants[arg]))
    raise ValueError(f"unresolved metric name {arg!r}")


def metric_inventory():
    sources = {
        path: production_source(path)
        for directory in ("crates", "services")
        for path in (ROOT / directory).glob("*/src/**/*.rs")
        if not path.stem.endswith("_tests") and path.stem != "tests"
    }
    constants = defaultdict(set)
    rpc_pairs = []
    for source in sources.values():
        for key, name in CONSTANT.findall(source):
            constants[key].add(name)
        rpc_pairs.extend(RPC_PAIR.findall(source))

    registrations = defaultdict(set)
    for path, source in sources.items():
        local_constants = dict(CONSTANT.findall(source))
        for call in REGISTER.finditer(source):
            argument = NAME_ARG.match(source, call.end())
            if not argument:
                raise ValueError(f"{path.relative_to(ROOT)}: unsupported metric argument in "
                                 f"{source[call.start():call.end() + 80].strip()}")
            kind, arg = call[1], argument[1]
            if arg == "metric_names.duration_seconds()":
                names = [duration for duration, _ in rpc_pairs]
            elif arg == "metric_names.request_total()":
                names = [total for _, total in rpc_pairs]
            else:
                names = [name_from_arg(arg, local_constants, constants)]
            for name in names:
                registrations[name].add(kind)
        # Process gauges render on scrape so unavailable samples can disappear.
        for name in re.findall(rf"\bappend_gauge\(\s*exposition,\s*{QUOTED_NAME}", source):
            registrations[name].add("gauge")

    bucketed = set()
    for mode, arg in BUCKET_MATCHER.findall(sources[RECORDER]):
        pattern = name_from_arg(arg, {}, constants)
        if mode == "Full":
            bucketed.add(pattern)
        elif mode == "Suffix":
            bucketed.update(name for name in registrations if name.endswith(pattern))
        else:
            raise ValueError(f"unsupported bucket matcher {mode}")
    return registrations, bucketed


def exported_series(registrations, bucketed):
    series = set()
    for name, kinds in registrations.items():
        if "histogram" in kinds:
            series.update([name + "_count", name + "_sum"])
            series.add(name + "_bucket" if name in bucketed else name)
        else:
            series.add(name)
    return series


def metric_names(text):
    return set(re.findall(rf"\b{NAME}", text))


def documented_metrics(reference):
    return set(re.findall(
        rf"^\| `({NAME})` \| (counter|gauge|histogram) \|",
        reference,
        flags=re.MULTILINE,
    ))


def dashboard_expressions(node):
    if isinstance(node, list):
        for entry in node:
            yield from dashboard_expressions(entry)
    elif isinstance(node, dict):
        for key, field in node.items():
            if key in ("expr", "query") and isinstance(field, str):
                yield field
            else:
                yield from dashboard_expressions(field)


class MetricNameTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registrations, bucketed = metric_inventory()
        cls.series = exported_series(cls.registrations, bucketed)

    def test_one_kind_per_registered_name(self):
        mixed = {name: kinds for name, kinds in self.registrations.items() if len(kinds) > 1}
        self.assertEqual(mixed, {})

    def test_reference_lists_registered_names_and_kinds(self):
        reference = (ROOT / "docs/reference/metrics.md").read_text()
        registered = {(name, kind) for name, kinds in self.registrations.items() for kind in kinds}
        self.assertSetEqual(documented_metrics(reference), registered)

    def test_names_and_derived_series_match_exactly(self):
        gauge = "zinder_readiness_state"
        histogram = "zinder_query_request_duration_seconds"
        summary = "zinder_query_compact_block_range_block_count"
        series = exported_series(
            {gauge: {"gauge"}, histogram: {"histogram"}, summary: {"histogram"}}, {histogram}
        )
        self.assertEqual(series, {
            gauge, histogram + "_bucket", histogram + "_count", histogram + "_sum",
            summary, summary + "_count", summary + "_sum",
        })
        self.assertEqual(metric_names(f"max({gauge}_)"), {gauge + "_"})
        reference = f"| `{gauge}` | gauge |\nThe `zinder_wallet_*` family."
        self.assertEqual(documented_metrics(reference), {(gauge, "gauge")})

    def test_consumers_query_exported_series(self):
        dashboards = sorted((ROOT / "observability/grafana/dashboards").glob("*.json"))
        self.assertTrue(dashboards)
        consumers = {}
        for dashboard in dashboards:
            expressions = dashboard_expressions(json.loads(dashboard.read_text()))
            consumers[dashboard] = metric_names("\n".join(expressions))

        smoke = SMOKE.read_text()
        shell_functions = set(re.findall(r"^(\w+)\(\) \{", smoke, re.MULTILINE))
        consumers[SMOKE] = metric_names(smoke) - shell_functions
        consumers[BENCH] = set(re.findall(QUOTED_NAME, production_source(BENCH)))
        for path, names in consumers.items():
            with self.subTest(consumer=str(path.relative_to(ROOT))):
                self.assertTrue(names)
                self.assertEqual(sorted(names - self.series), [])

    def test_bucket_matcher_accepts_a_multiline_metric_name(self):
        source = '''
            .set_buckets_for_metric(
                Matcher::Full(
                    "zinder_wallet_serving_pair_publisher_generation_wait_duration_seconds"
                        .to_owned(),
                ),
                buckets,
            )
        '''
        [(mode, arg)] = BUCKET_MATCHER.findall(source)
        self.assertEqual(mode, "Full")
        self.assertEqual(name_from_arg(arg, {}, {}),
                         "zinder_wallet_serving_pair_publisher_generation_wait_duration_seconds")


if __name__ == "__main__":
    unittest.main()
