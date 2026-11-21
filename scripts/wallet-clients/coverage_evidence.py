"""Read private experiment receipts and emit privacy-safe action and method observations."""
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


def require(ok, message):
    if not ok:
        raise ValueError(message)


def load(path):
    return json.loads(Path(path).read_bytes())


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def subtree_fixture(directory, report):
    """Prefer the fixture preserved with the run, and accept the pinned default for older runs."""
    preserved = directory / "fixture.json"
    path = preserved if preserved.is_file() else Path(__file__).with_name("subtree-testnet.json")
    raw = path.read_bytes()
    require(report["fixture_sha256"] == hashlib.sha256(raw).hexdigest(), "subtree release or frozen fixture mismatch")
    return json.loads(raw)


def subtree_actions(directory, release):
    path = directory / "summary.json"
    report = load(path)
    trace = load(directory / "wallet-rpcs.json")
    fixture = subtree_fixture(directory, report)
    server = report["server"]
    require(report["schema"] == 1
            and server["commit"] == release["commit"] and server["version"] == release["tag"][1:]
            and server["height"] == fixture["tip"] and server["hash"] == fixture["tip_hash"]
            and server["network"] == fixture["network"], "subtree release or frozen fixture mismatch")
    require(report["wallet"] == {"sha256": fixture["cli_sha256"], "revision": fixture["cli_revision"]},
            "subtree wallet identity mismatch")
    revision = fixture["cli_revision"]
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "subtree wallet source revision missing")
    version = "8.0.0" if revision == "81d799775f11e882896e8710e8af12d6d4d29a41" else f"source {revision[:12]}"
    common = dict(wallet="zingo", version=version, variant="nakednet-cli", implementation="engine",
                  interface="lightwalletd", network="testnet", client_sha256=fixture["cli_sha256"],
                  fixture=f"Testnet birthday {fixture['birthday']} through {fixture['tip']}/{fixture['tip_hash']}",
                  method_scope="partial", status="pass",
                  evidence={"description": "Frozen subtree A/B / summary.json", "sha256": digest(path)})
    if report.get("observed_methods"):
        common["observed_methods"] = report["observed_methods"]
    expected_state = dict(blocks=fixture["tip"] - fixture["birthday"] + 1, outputs=fixture["expected_outputs"],
                          history_sha256=fixture["history_sha256"], confirmed_zat=fixture["expected_confirmed_zat"],
                          spendable_zat=fixture["expected_spendable_zat"])
    require({"reference", "reversed"} <= set(report["arms"]) <= {"reference", "reversed", "one-row"},
            "subtree result missing paired arms")
    for mode, arm in report["arms"].items():
        for phase in ("restore", "restart"):
            require(arm["states"][phase] == expected_state and arm["target_only_process_connects"] is True,
                    "subtree wallet state or endpoint mismatch")
            events = [e for e in trace if e["phase"] == f"{mode}/{phase}"]
            counts = Counter(e["method"] for e in events)
            require(counts and dict(counts) == arm[f"{phase}_method_calls"], "subtree trace count mismatch")
            methods = {m: {"calls": n} for m, n in sorted(counts.items())}
            assertions = dict.fromkeys(("history", "confirmed-balances", "spendable-funds",
                                        "scan-range-and-outputs", "target-only-connections"), "pass")
            if phase == "restore":
                pages = [e for e in events if e.get("pool") == 0 and e["method"] == "GetSubtreeRoots"]
                n = len(fixture["sapling_roots"])
                expected = [(i, 1) for i in range(n)] if mode == "one-row" else [(0, n)]
                require([(e["start"], e["rows"]) for e in pages] == expected + [(n, 0)],
                        "missing populated or terminal Sapling page")
                assertions["populated-sapling-pagination"] = "pass"
                if mode in ("reference", "reversed"):
                    comparison = report["primary_wire_comparison"][phase]
                    require(comparison["identical_request_and_response_signatures"] > 0 and
                            comparison["total_calls"][mode] == len(events) and
                            report["distinct_wallet_files"] == len(report["arms"]), "missing paired wire or DB proof")
                    assertions["hash-order-preserves-funded-restore"] = "pass"
                    methods["GetSubtreeRoots"]["fields"] = {
                        "SubtreeRoot.completingBlockHash": {"handling": "ignored",
                                                           "assertion": "hash-order-preserves-funded-restore"}}
            else:
                assertions["persisted-state-after-process-reopen"] = "pass"
            yield dict(**common, id=f"zingo-subtree-{mode}-{phase}", action=f"{phase.capitalize()} with {mode} Sapling rows",
                       assertions=assertions, methods=methods)
    negative = report.get("negative_control")
    if negative is not None:
        events = [e for e in trace if e["phase"] == "invalid-root/restore"]
        require(negative["observed_error"] == "server returned invalid subtree root" and
                negative["blocks_scanned"] == 0 and negative["target_only_process_connects"] is True and
                any(e.get("pool") == 0 and e["rows"] > 0 for e in events), "invalid-root control not exercised")
        methods = {m: {"calls": n} for m, n in sorted(Counter(e["method"] for e in events).items())}
        methods["GetSubtreeRoots"]["fields"] = {"SubtreeRoot.rootHash": {
            "handling": "validated", "assertion": "invalid-sapling-root-rejected"}}
        yield dict(**common, id="zingo-subtree-invalid-root", action="Restore with an invalid Sapling root encoding",
                   assertions={"invalid-sapling-root-rejected": "pass", "no-blocks-scanned": "pass"}, methods=methods)


def reorg_actions(path, release):
    result = load(path)
    fixture, client = result["fixture"], result["client"]
    require(result["schema_version"] == 1 and result["case"] == "zkool-ironwood-coinbase-reorg"
            and result["interface"] == "lightwalletd" and result["status"] == "passed"
            and fixture["release_revision"] == release["commit"] and
            fixture["release_version"] == release["tag"][1:], "reorg did not pass on selected release")
    checks = {
        "bootstrap": [],
        "scan-surviving-receipt": [],
        "scan-orphan-candidate": [],
        "follow-replacement": ["orphan_removed_from_confirmed_history", "owned_note_and_balance_reverted",
                               "surviving_receipt_preserved"],
        "restart-and-advance": ["rollback_persisted_after_new_process_and_next_block"],
    }
    reached_tip = result["first_reorg_sync_reached_tip"]
    expected_actions = set(checks) if reached_tip else {*checks, "resume-replacement"}
    require(isinstance(reached_tip, bool) and expected_actions == set(result["actions"]), "incomplete reorg actions")
    for phase, selected in checks.items():
        action = result["actions"][phase]
        require(action["client_connected_only_to_target"] is True, "unattributed reorg calls")
        assertions = {"target-only-connections": "pass"}
        for check in selected:
            require(result["assertions"][check] is True, f"missing reorg assertion {check}")
            assertions[check.replace("_", "-")] = "pass"
        states = result["wallet_states"]
        if phase == "scan-surviving-receipt":
            require(states["surviving-receipt"]["received_notes"] == 1, "first receipt missing")
            assertions["first-owned-note-discovered"] = "pass"
        if phase == "scan-orphan-candidate":
            require(states["before-reorg"]["received_notes"] == 2 and states["before-reorg"]["ironwood_balance"]
                    > states["surviving-receipt"]["ironwood_balance"], "second receipt missing")
            assertions["second-owned-note-and-balance"] = "pass"
        calls = list(action["methods"])
        if phase == "follow-replacement":
            if not reached_tip:
                resume = result["actions"]["resume-replacement"]
                require(resume["client_connected_only_to_target"] is True, "unattributed reorg catchup")
                calls.extend(resume["methods"])
            state = states["after-first-reorg-sync" if reached_tip else "after-reorg-catchup"]
            require(state["scanned_height"] == result["fork"]["new_tip"], "replacement chain tip not reached")
            assertions["replacement-tip-reached"] = "pass"
        methods = {}
        for call in calls:
            obs = methods.setdefault(call["method"], {"calls": 0, "outcomes": {}})
            obs["calls"] += call["count"]
            code = call["grpc_code"]
            obs["outcomes"][code] = obs["outcomes"].get(code, 0) + call["count"]
        yield dict(id=f"zkool-reorg-{phase}", wallet=client["name"], version=client["version"],
                   variant=client["variant"], implementation="engine", client_sha256=client["binary_sha256"],
                   interface=result["interface"], network=fixture["network"],
                   fixture=f"disposable-{fixture['network']}-{fixture['activation_schedule_sha256']}",
                   action=phase.replace("-", " ").capitalize() + " on the private Ironwood fork",
                   status="pass", method_scope="partial", assertions=assertions, methods=methods,
                   evidence={"description": "Disposable Zkool reorg / receipt.json", "sha256": digest(path)})


def interrupted_actions(path, release):
    result = load(path)
    fixture_path = Path(__file__).with_name("subtree-testnet.json")
    fixture = load(fixture_path)
    require(result["schema_version"] == 1 and result["status"] == "pass" and
            result["revision"] == release["commit"] and result["fixture_sha256"] == digest(fixture_path)
            and all(result[k] == fixture[k] for k in ("tip", "tip_hash", "birthday")),
            "interrupted restore fixture or release mismatch")
    control, interrupted = result["control"], result["interrupted"]
    state = control["wallet"]
    require(state == interrupted["wallet"] and state["synced_height"] == fixture["tip"] and
            state["history_entries"] > 0 and state["received_notes"] > 0 and
            len(state["ledger_sha256"]) == 64 and control["fault"] is None, "wallet recovered different state")
    fault = interrupted["fault"]
    require(fault["delivered"] == 64 and fault["start"] + fault["delivered"] == fault["last_delivered"] + 1
            and fault["last_delivered"] < fault["end"] and fault["wallet_height_before_resume"] < fixture["tip"],
            "missing partial-stream evidence")
    full_range = [fixture["birthday"] + 1, fixture["tip"]]
    require(fault["start"] == full_range[0] and fault["end"] == full_range[1]
            and fault["wallet_height_before_resume"] == fault["last_delivered"]
            and control["compact_block_ranges"] == [full_range]
            and interrupted["compact_block_ranges"] == [full_range, [fault["last_delivered"] + 1, fixture["tip"]]],
            "resumed range does not continue from the persisted last delivered block")
    for phase, arm in (("control", control), ("interrupted", interrupted)):
        traces = arm["online_traces"]
        require(set(traces) == ({"restore", "resume"} if phase == "interrupted" else {"restore"})
                and all(t["connects"] > 0 and len(t["trace_sha256"]) == 64 for t in traces.values())
                and ((traces["restore"]["exit"] != 0 and traces["resume"]["exit"] == 0)
                     if phase == "interrupted" else traces["restore"]["exit"] == 0),
                "missing traced wallet invocation")
        checks = dict.fromkeys(("persisted-wallet-ledger", "history-and-notes", "pool-balances",
                                "scanned-tip", "target-only-connections"), "pass")
        methods = {m: {"calls": n} for m, n in sorted(arm["methods"].items())}
        if phase == "interrupted":
            require(methods["GetBlockRange"]["calls"] >= 2, "wallet never requested another range")
            checks.update(dict.fromkeys(("partial-range-failure", "fresh-process-resumes", "next-range-from-persisted-block", "matches-control"), "pass"))
        yield dict(id=f"zkool-compact-stream-{phase}", wallet="zkool", version="6.37.0", variant="external-only",
                   implementation="engine", interface="lightwalletd", network="testnet", client_sha256=result["client_sha256"],
                   fixture=f"Testnet birthday {fixture['birthday']} through {fixture['tip']}/{fixture['tip_hash']}",
                   action="Restore through the frozen fence" if phase == "control" else
                          "Interrupt a compact-block stream and resume the persisted wallet in a fresh process",
                   status="pass", method_scope="partial", assertions=checks, methods=methods,
                   **({"observed_methods": result["observed_methods"]} if result.get("observed_methods") else {}),
                   evidence={"description": "Compact stream interruption / receipt.json", "sha256": digest(path)})


def native_expiry_actions(path, release):
    result = load(path)
    fixture, client = result["fixture"], result["client"]
    require(result["schema_version"] == 1 and result["status"] == "passed" and
            result["case"] == "zallet-bounded-scan-expiry" and result["interface"] == "native-wallet-query" and
            fixture["release_revision"] == release["commit"] and fixture["release_version"] == release["tag"][1:] and
            fixture["raw_blob_policy"] == "all", "native expiry did not pass on selected all-retention release")
    require(re.fullmatch(r"[0-9a-f]{64}", client["driver_sha256"]) and
            re.fullmatch(r"[0-9a-f]{64}", client["backend_sha256"]),
            "native expiry requires driver and ordinary backend digests")
    identity = dict(wallet="zallet", version="ZIT pinned driver", implementation="engine",
                    variant=f"{client['driver_feature']} driver {client['driver_sha256']}",
                    client_sha256=client["backend_sha256"])
    first, retry = result["attempts"]
    require(first["chain_epoch_id"] != retry["chain_epoch_id"] and
            first["requested_start_height_inclusive"] == retry["requested_start_height_inclusive"] and
            result["persisted_heights"] == list(range(retry["requested_start_height_inclusive"],
                                                     retry["requested_end_height_inclusive"] + 1)) and
            result["expired_source"] == "IndexerError::ChainEpochPinUnavailable", "native range retry mismatch")
    for phase, calls in result["actions"].items():
        methods = {}
        for call in calls:
            obs = methods.setdefault(call["method"], {"calls": 0, "outcomes": {}})
            n, code = call["count"], call["grpc_code"]
            obs["calls"] += n
            obs["outcomes"][code] = obs["outcomes"].get(code, 0) + n
        assertions = {"target-only-connections": "pass"}
        if phase == "stale-read-and-retry":
            require(any(c["method"] == "FullBlocksInRange" and c["grpc_code"] == "failed_precondition"
                        and c["error_class"] == "CHAIN_EPOCH_PIN_UNAVAILABLE" for c in calls)
                    and methods["FullBlocksInRange"]["outcomes"].get("ok", 0) > 0, "missing native stale/fresh calls")
            for key, passed in result["assertions"].items():
                require(passed is True, f"missing native expiry check {key}")
                assertions[key.replace("_", "-")] = "pass"
            assertions["typed-expired-view-error"] = "pass"
        yield dict(id=f"zallet-bounded-{phase}", **identity, interface=result["interface"],
                   network=fixture["network"], fixture=f"disposable native all-retention at height {fixture['anchor_height']}",
                   action=phase.replace("-", " ").capitalize() + " during bounded native scan", status="pass",
                   method_scope="partial", methods=methods, assertions=assertions,
                   evidence={"description": "Native bounded-scan expiry / receipt.json", "sha256": digest(path)})
