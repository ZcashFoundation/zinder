#!/usr/bin/env python3
"""Exercise the shipped scrape jobs, rule files, and Grafana query with promtool."""

import json
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "/etc/prometheus/prometheus.yml"
ROLES = ["zinder-ingest", "zinder-projector", "zinder-query", "zinder-compat-lightwalletd"]


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


def promql_labels(labels):
    return "{" + ",".join(f"{key}={json.dumps(val)}" for key, val in sorted(labels.items())) + "}"


def rule_test(rule_files, targets, panel):
    query = next(labels for labels in targets if labels["service"] == "zinder-query")
    inputs = [
        {"series": "up" + promql_labels(labels),
         "values": "1 0 0 0 0 stale _ _ _ _ 1" if labels == query else "1+0x10"}
        for labels in targets
    ]
    # A scrape for a different instance cannot hide the expected target's disappearance.
    inputs.append({"series": "up" + promql_labels(query | {"instance": "unexpected:9106"}),
                   "values": "1+0x10"})
    checks = []
    for instant, status, alert in [
        ("0s", 1, None), ("45s", 0, "ZinderTargetDown"),
        ("2m", -1, "ZinderTargetMissing"), ("150s", 1, None),
    ]:
        samples = [
            {"labels": promql_labels(labels), "value": status if labels == query else 1}
            for labels in targets
        ]
        # Arithmetic drops the recording name so this also checks a raw panel expression.
        checks.append({"expr": f"({panel}) * 1", "eval_time": instant, "exp_samples": samples})
        checks.append({
            "expr": 'ALERTS{alertname=~"ZinderTargetDown|ZinderTargetMissing",alertstate="firing"}',
            "eval_time": instant,
            "exp_samples": ([{"labels": "ALERTS" + promql_labels(
                query | {"alertname": alert, "alertstate": "firing", "severity": "critical"}),
                              "value": 1}] if alert else []),
        })
    # Removing a whole job on reload can leave its last good sample within
    # the default five-minute lookback without an explicit stale marker.
    tests = [{"interval": "15s", "input_series": inputs, "promql_expr_test": checks}]
    for last_up in (0, 1):
        removed = [
            {"series": "up" + promql_labels(labels),
             "values": f"{last_up} _x8 1" if labels == query else "1+0x10"}
            for labels in targets
        ]
        tests.append({"interval": "15s", "input_series": removed,
                      "promql_expr_test": [check for check in checks
                                           if check["eval_time"] in ("2m", "150s")]})
    return {"rule_files": rule_files, "evaluation_interval": "15s",
            "group_eval_order": ["zinder-compose-targets", "zinder-smoke-targets",
                                 "zinder-explorer-target", "zinder-readiness"],
            "tests": tests}


def check(compose_args, service, roles, panel):
    config = json.loads(output("docker", "compose", *compose_args, "config", "--format", "json"))
    prometheus = config["services"][service]
    command = ["docker", "run", "--rm", "-i", "--network", "none", "--entrypoint", "/bin/promtool"]
    for mount in prometheus["volumes"]:
        if mount["type"] == "bind":
            command.extend(["-v", f"{mount['source']}:{mount['target']}:ro"])
    image = prometheus["image"]
    validation = output(*command, image, "check", "config", CONFIG)
    rule_files = re.findall(r"^Checking (.+)$", validation, flags=re.MULTILINE)[1:]
    if not rule_files:
        raise ValueError(f"{service} loaded no rule files")
    targets = []
    for role in roles:
        found = json.loads(output(*command, image, "check", "service-discovery", "--timeout=1s",
                                  CONFIG, role))
        if len(found) != 1 or found[0]["labels"]["__metrics_path__"] != "/metrics":
            raise ValueError(f"expected one /metrics target for {role}")
        targets.append({k: v for k, v in found[0]["labels"].items() if not k.startswith("__")})
    with tempfile.TemporaryDirectory() as directory:
        fixture = Path(directory) / "targets.yml"
        fixture.write_text(json.dumps(rule_test(rule_files, targets, panel)))
        output(*command, "-v", f"{fixture}:/targets.yml:ro", image, "test", "rules", "/targets.yml")
    print(f"{service}: verified healthy, down, missing, and recovered across {len(roles)} targets")


if __name__ == "__main__":
    dashboard = json.loads((ROOT / "observability/grafana/dashboards/zinder-overview.json").read_text())
    panel = next(p for p in dashboard["panels"] if p["title"] == "Scrape Targets")["targets"][0]["expr"]
    compose = ["--env-file", "deploy/.env.regtest", "-f", "deploy/docker-compose.yml"]
    check(compose, "zinder-prometheus", ROLES, panel)
    check(compose + ["-f", "deploy/docker-compose.explorer.yml"], "zinder-prometheus",
          ROLES + ["zinder-explorer"], panel)
    check(["-f", "docker-compose.observability.yml"], "prometheus", ROLES, panel)
