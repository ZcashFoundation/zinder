#!/usr/bin/env python3
"""Run paired benchmark commands on prepared targets and retain their reports."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time


NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]*")
PARAMETER = re.compile(r"\{([a-zA-Z][a-zA-Z0-9_]*)\}")
OUTPUT_ARGUMENTS = {"{report}": "report.json", "{scratch}": "scratch"}


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def sha256(path):
    with open(path, "rb") as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def positive_int(number):
    return type(number) is int and number > 0


def is_pointer(pointer):
    return isinstance(pointer, str) and pointer.startswith("/")


def read_campaign(path):
    campaign = json.loads(path.read_text(encoding="utf-8"))
    repeats = campaign.get("repeats", 4)
    timeout = campaign.get("timeout_seconds", 600)
    if not positive_int(repeats) or not positive_int(timeout):
        raise ValueError("repeats and timeout_seconds must be positive integers")

    arms = campaign["arms"]
    cases = campaign["cases"]
    if not isinstance(arms, dict) or len(arms) != 2 or not all(isinstance(arm, dict) for arm in arms.values()):
        raise ValueError("arms must contain two named objects")
    if not isinstance(cases, dict) or not cases:
        raise ValueError("cases must contain at least one named case")
    for name in (*arms, *cases):
        if not NAME.fullmatch(name):
            raise ValueError(f"invalid arm or case name {name!r}")

    commands = {}
    executables = {}
    for name, case in cases.items():
        template = case["command"]
        if not isinstance(template, list) or not template or not all(isinstance(arg, str) for arg in template):
            raise ValueError(f"{name}: command must be a nonempty array of strings")
        match = case.get("match", [])
        expected = case.get("expect", {})
        if not isinstance(match, list) or not all(is_pointer(key) for key in match):
            raise ValueError(f"{name}: match must be an array of JSON pointers")
        if not isinstance(expected, dict) or not all(is_pointer(key) for key in expected):
            raise ValueError(f"{name}: expect must be an object keyed by JSON pointer")
        for arm, parameters in arms.items():
            command = []
            for arg in template:
                parameter = PARAMETER.fullmatch(arg)
                if parameter and arg not in OUTPUT_ARGUMENTS:
                    arg = parameters[parameter[1]]
                    if not isinstance(arg, str) or arg in OUTPUT_ARGUMENTS:
                        raise ValueError(f"{arm}: {parameter[1]} must be a string other than an output placeholder")
                command.append(arg)
            binary = shutil.which(command[0])
            if binary is None:
                raise ValueError(f"{name}/{arm}: executable not found: {command[0]}")
            command[0] = str(Path(binary).resolve())
            if command[0] not in executables:
                executables[command[0]] = sha256(command[0])
            commands[name, arm] = command

    input_paths = campaign.get("inputs", [])
    if not isinstance(input_paths, list) or not all(isinstance(path, str) for path in input_paths):
        raise ValueError("inputs must be an array of file paths")
    inputs = {str(Path(path).resolve()): sha256(path) for path in input_paths}
    return campaign, repeats, timeout, commands, executables, inputs


def at_pointer(document, pointer):
    for token in pointer[1:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(document, list) and re.fullmatch(r"0|[1-9][0-9]*", key):
            document = document[int(key)]
        elif isinstance(document, dict):
            document = document[key]
        else:
            raise KeyError(pointer)
    return document


def run_case(command, directory, timeout, case):
    directory.mkdir(parents=True)
    report_path = directory / "report.json"
    stdout_path = directory / "stdout.log"
    file_report = "{report}" in command
    command = [str(directory / OUTPUT_ARGUMENTS[arg]) if arg in OUTPUT_ARGUMENTS else arg for arg in command]
    started_at = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    problems = []
    exit_code = None
    with stdout_path.open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
        try:
            exit_code = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=timeout).returncode
            if exit_code:
                problems.append(f"command exited {exit_code}")
        except subprocess.TimeoutExpired:
            problems.append(f"command timed out after {timeout} seconds")
        except OSError as error:
            problems.append(f"could not start command: {error.strerror}")

    match = {}
    try:
        report = json.loads((report_path if file_report else stdout_path).read_text(encoding="utf-8"))
        if not isinstance(report, (dict, list)) or not report:
            raise ValueError("expected a nonempty JSON object or array")
        if not file_report:
            shutil.copyfile(stdout_path, report_path)
    except (OSError, ValueError) as error:
        problems.append(f"could not read report: {error}")
    else:
        expected = case.get("expect", {})
        for pointer in sorted(set(case.get("match", [])) | expected.keys()):
            try:
                observed = at_pointer(report, pointer)
            except (IndexError, KeyError):
                problems.append(f"report is missing {pointer}")
                continue
            if pointer in expected and observed != expected[pointer]:
                problems.append(f"unexpected value at {pointer}")
            if pointer in case.get("match", []):
                match[pointer] = observed

    observation = {
        "started_at": started_at,
        "elapsed_seconds": time.monotonic() - start,
        "command": command,
        "exit_code": exit_code,
        "matched_fields": match,
        "problems": problems,
    }
    write_json(directory / "run.json", observation)
    return observation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path, help="JSON campaign with two arms and shared cases")
    parser.add_argument("--output", required=True, type=Path, help="new directory for raw reports and run metadata")
    args = parser.parse_args()
    try:
        campaign, repeats, timeout, commands, executables, inputs = read_campaign(args.campaign)
        output = args.output.resolve()
        output.mkdir(mode=0o700, parents=True)
    except (AttributeError, KeyError, OSError, TypeError, ValueError) as error:
        parser.error(str(error))

    arms = list(campaign["arms"])
    write_json(output / "provenance.json", {
        "campaign": campaign,
        "runner_sha256": sha256(__file__),
        "executables_sha256": executables,
        "inputs_sha256": inputs,
        "host": platform.platform(),
        "machine": platform.machine(),
        "client_cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
    })

    summary = []
    for name, case in campaign["cases"].items():
        for repetition in range(1, repeats + 1):
            order = arms if repetition % 4 in (0, 1) else arms[::-1]
            pair = {"case": name, "repetition": repetition, "order": order, "runs": {}}
            for arm in order:
                run_path = Path(name) / f"round-{repetition}" / arm
                print(f"{name} round {repetition} {arm}", file=sys.stderr, flush=True)
                observation = run_case(commands[name, arm], output / run_path, timeout, case)
                pair["runs"][arm] = {"path": str(run_path), "problems": observation["problems"]}
                pair["runs"][arm]["matched_fields"] = observation["matched_fields"]
            first, second = (pair["runs"][arm]["matched_fields"] for arm in arms)
            pair["mismatched_fields"] = sorted(pointer for pointer in first.keys() & second.keys()
                                               if first[pointer] != second[pointer])
            summary.append(pair)
            write_json(output / "summary.json", summary)

    failed = [pair for pair in summary if pair["mismatched_fields"] or
              any(run["problems"] for run in pair["runs"].values())]
    print(f"{len(summary)} paired trials, {len(failed)} with errors or mismatches. Reports in {output}")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
