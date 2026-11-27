"""Import each wallet's payer or recipient observations across four signed Regtest payments."""
import hashlib
import json
import re
import runpy
from pathlib import Path

from coverage_evidence import digest, load, require

LEGS = ("outbound", "sweep", "inbound", "spendback")
CASES = ("zkool-internal", "zkool-external", "cake")
CLIENTS = ("zkool", "cake", "zallet")
WALLETS = {"zkool-internal": ("zkool", "internal-enabled"),
           "zkool-external": ("zkool", "external-only"),
           "cake": ("cake", "cake-engine-internal-enabled")}
PAYER = {"outbound": "wallet", "sweep": "wallet", "inbound": "counterpart",
         "spendback": "wallet"}
MEMOS = {"outbound": "zinder/zkool-cake-v1/outbound",
         "sweep": "zinder/zkool-cake-v1/sweep",
         "inbound": "zinder/zkool-cake-v1/inbound",
         "spendback": "zinder/zkool-cake-v1/spendback"}
SERVICES = ("ingest", "projector", "query", "compat-lightwalletd")
UNIFIED_SERVICES = ("zinder",)
ALLOWED_SERVICE_SETS = (set(UNIFIED_SERVICES), set(SERVICES))
ACTIVATION = {"Overwinter": 1, "Sapling": 1, "Blossom": 1, "Heartwood": 1,
              "Canopy": 1, "NU5": 1, "NU6": 1, "NU6.1": 1, "NU6.2": 1, "NU6.3": 250}
OUTCOME_LEGS = ("tx_fingerprint", "mined_height", "amount_zat", "fee_zat",
                "memo_verified", "payer_before_zat", "payee_before_zat",
                "payer_after_zat", "payee_after_zat")
CLEANUP_KEYS = {"fixture_started", "down_error", "project_resources_remaining",
                "relay_running", "fixture_removed", "retry"}
CLEANUP_OPTIONAL = {"wallet_survivors", "wallet_survivors_retry", "upgraded",
                    "measure_error", "skipped"}
UPSTREAM_KEYS = {"executable_sha256", "version", "repository", "source_commit",
                 "workspace_lock_sha256", "source_lock_sha256", "toolchain",
                 "build", "driver_sha256"}
SUMMARY_KEYS = {"schema_version", "kind", "status", "release", "port_bases", "cases",
                "cleanup", "failures", "limits", "evidence"}
CASE_KEYS = {"status", "receipt", "receipt_sha256", "case_cleanup",
             "case_cleanup_sha256", "cleanup", "outcome", "provenance"}
EVIDENCE_KEYS = {"index", "index_sha256", "cleanup", "cleanup_sha256", "receipts"}
RECEIPT_EVIDENCE_KEYS = {"preflight_sha256", "journal_sha256", "cleanup_sha256",
                         "counterpart_config", "traces"}
RESULT_KEYS = {"funding", "incoming_note_spent", "reopen_ledger_verified",
               "restart_verified", "restart_memo_exposure",
               "wallet_after_reopen_zat", "counterpart_after_reopen_zat",
               "endpoint_evidence", "counterpart_endpoint",
               "counterpart_endpoint_evidence"}
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
LOOPBACK = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}")
TAG = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?")

REFERENCE_BUILDS = runpy.run_path(str(Path(__file__).with_name("zkool-payment-matrix.py")))["UPSTREAM"]
REQUIRED_HARNESS = {
    "zkool-payment-matrix.py", "zkool-payment.py", "cake-payment.py", "zkool-payment.rs",
    "roundtrip_wallets.py", "roundtrip_fixture.py", "wallet_fixture.py",
    "trace-wallet-endpoint.py", "run-wallet-roundtrip-regtest.py", "regtest-fixture.py",
    "regtest-fixture-relay.py",
}


def token(value, where):
    require(isinstance(value, str) and SHA256.fullmatch(value), f"{where}: missing sha256")


def exact(obj, keys, where):
    require(isinstance(obj, dict) and set(obj) == set(keys),
            f"{where}: fields differ from the case recipe")


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def release_identity(value):
    exact(value, {"tag", "commit"}, "release")
    require(isinstance(value["tag"], str) and TAG.fullmatch(value["tag"]), "invalid release tag")
    require(isinstance(value["commit"], str) and COMMIT.fullmatch(value["commit"]),
            "invalid release commit")


def safe_relative(root, value, where):
    require(isinstance(value, str) and value and "\\" not in value
            and not value.startswith("/") and ".." not in Path(value).parts,
            f"{where}: unsafe relative path")
    target = (root / value).resolve()
    require(target.is_relative_to(root.resolve()) and target.is_file(),
            f"{where}: missing evidence file")
    return target


def pins(snapshot, where):
    exact(snapshot, {"harness_sha256", "executables_sha256", "fixture_ready_sha256"}, where)
    harness = snapshot["harness_sha256"]
    require(isinstance(harness, dict) and harness, f"{where}: missing harness pins")
    require(all(isinstance(name, str) and name and Path(name).name == name for name in harness),
            f"{where}: harness pin carries a path")
    require(all(isinstance(value, str) and SHA256.fullmatch(value) for value in harness.values()),
            f"{where}: invalid harness pin")
    require(REQUIRED_HARNESS <= set(harness), f"{where}: required runtime harness pin is missing")
    executables = snapshot["executables_sha256"]
    exact(executables, CLIENTS, f"{where} executable pins")
    for name, value in executables.items():
        token(value, f"{where} {name} executable pin")
    token(snapshot["fixture_ready_sha256"], f"{where} fixture readiness pin")
    return snapshot


def client_claims(executables, upstream):
    """Bind each client pin to a recorded build identity and its version claim."""
    exact(upstream, CLIENTS, "upstream identity")
    claims = {}
    for name in CLIENTS:
        entry = upstream[name]
        require(isinstance(entry, dict) and entry, f"{name}: missing upstream identity")
        require(set(entry) <= UPSTREAM_KEYS, f"{name}: unexpected upstream fields")
        pinned = executables[name]
        require(entry.get("executable_sha256", pinned) == pinned,
                f"{name}: recorded build differs from the executable pin")
        ref = REFERENCE_BUILDS[name]
        if not ({"version", "source_commit", "repository"} & entry.keys()):
            claims[name] = {"version": None, "commit": None}
            continue
        require(pinned == ref["executable_sha256"], f"{name}: unknown binary with a build claim")
        for field in ("version", "source_commit", "repository", "workspace_lock_sha256",
                      "source_lock_sha256", "driver_sha256"):
            require(entry.get(field) == ref.get(field), f"{name}: build identity changed ({field})")
        claims[name] = {"version": ref.get("version"), "commit": ref.get("source_commit")}
    return claims


def preflight(value, release):
    """Validate the fixture identity recorded for one case."""
    require(value["network"] == "zcash-regtest" and value["revision"] == release["commit"]
            and value["release"] == release["tag"], "fixture release or network mismatch")
    require(value["raw_blob_policy"] == "all", "fixture does not retain full blocks")
    require(value["peer_count"] == 0, "fixture was not peer-isolated")
    require(type(value["port_base"]) is int and value["port_base"] > 1024,
            "fixture port base is missing")
    anchor = value["anchor"]
    exact(anchor, {"height", "hash"}, "fixture anchor")
    require(type(anchor["height"]) is int and anchor["height"] > 0, "invalid fixture anchor")
    token(anchor["hash"], "fixture anchor")
    token(value["activation_schedule_sha256"], "fixture activation schedule")
    exact(value["activation_heights"], ACTIVATION, "fixture activation schedule")
    require(all(type(height) is int and height == ACTIVATION[name]
                for name, height in value["activation_heights"].items()),
            "fixture activation heights differ from NU6.3 at 250")
    token(value["image_manifest_sha256"], "fixture image manifest")
    require(IMAGE.fullmatch(value["node_image"]), "invalid fixture node image")
    for field in ("image_references", "images"):
        require(isinstance(value[field], dict) and set(value[field]) in ALLOWED_SERVICE_SETS,
                f"fixture {field}")
        require(all(isinstance(item, str) and item.strip() for item in value[field].values()),
                f"fixture {field} is incomplete")
    require(set(value["image_references"]) == set(value["images"]),
            "fixture image references and IDs disagree")
    version = release["tag"].lstrip("v")
    require(all(reference.rsplit(":", 1)[-1] == version
                for reference in value["image_references"].values()),
            "fixture images are not the selected release")
    require(value["release_binaries"] in ({}, None),
            "case ran on overridden binaries, not release images")
    for tip in ("node_tip", "native_tip"):
        row = value[tip]
        exact(row, {"height", "hash"}, f"fixture {tip}")
        require(type(row["height"]) is int and row["height"] >= anchor["height"],
                f"fixture {tip} is behind the anchor")
        token(row["hash"], f"fixture {tip}")
    return value


def recipe(value, pre):
    exact(value, {"starting_height", "activation_schedule_sha256", "activation_heights",
                  "node_image", "image_references", "images", "raw_blob_policy",
                  "release_binaries"}, "fixture recipe")
    require(value["starting_height"] == pre["node_tip"]["height"],
            "fixture recipe start height differs from preflight")
    require(value["activation_schedule_sha256"] == pre["activation_schedule_sha256"]
            and value["activation_heights"] == pre["activation_heights"],
            "fixture recipe activation differs from preflight")
    for field in ("node_image", "image_references", "images", "raw_blob_policy",
                  "release_binaries"):
        require(value[field] == pre[field], f"fixture recipe {field} differs from preflight")
    return value


def cleanup_row(value, where, passed):
    require(isinstance(value, dict) and CLEANUP_KEYS <= set(value)
            <= CLEANUP_KEYS | CLEANUP_OPTIONAL, f"{where}: unexpected fields")
    for key in ("measure_error", "upgraded", "skipped", "wallet_survivors_retry"):
        require(key not in value, f"{where}: cleanup could not be completed")
    require(value.get("fixture_started") is True, f"{where}: fixture did not start")
    require(value.get("down_error") is None, f"{where}: fixture teardown failed")
    require(value.get("project_resources_remaining") == 0, f"{where}: owned resources remain")
    require(value.get("relay_running") is False, f"{where}: fixture relay remains")
    require(value.get("fixture_removed") is True, f"{where}: fixture was not removed")
    if passed:
        require(value.get("retry") is False, f"{where}: a passed case needed a cleanup retry")
    survivors = value.get("wallet_survivors")
    if survivors is not None:
        exact(survivors, {"scope", "found", "stopped", "remaining", "errors"},
              f"{where} wallet survivors")
        require(survivors["scope"] == "owned-case-processes"
                and survivors["found"] == 0 and survivors["remaining"] == 0
                and survivors["errors"] == [] and type(survivors["stopped"]) is int
                and survivors["stopped"] >= 0, f"{where}: case processes survived")
    return value


def receipt_document(path, where):
    value = load(path)
    require(value.get("schema_version") == 1 and value.get("status") == "passed"
            and value.get("network") == "regtest", f"{where}: not a passed Regtest receipt")
    return value


def case_receipt(root, case, release):
    """Verify one case receipt and its private evidence, then return it."""
    state = root / case
    receipt = receipt_document(state / "receipt.json", f"{case} receipt")
    require(receipt.get("case") == case, f"{case}: receipt belongs to another case")
    exact(receipt["release"], {"tag", "revision"}, f"{case} release")
    require(receipt["release"] == {"tag": release["tag"], "revision": release["commit"]},
            f"{case}: receipt release differs from the selected release")

    snapshots = receipt["pins"]
    exact(snapshots, {"pre", "post"}, f"{case} pins")
    before = pins(snapshots["pre"], f"{case} pre-run pins")
    after = pins(snapshots["post"], f"{case} post-run pins")
    require(before == after, f"{case}: client or fixture pins changed during the run")
    require(receipt.get("binaries") == before["executables_sha256"],
            f"{case}: receipt binaries differ from the pins")
    require(receipt.get("harness_sha256") == before["harness_sha256"],
            f"{case}: receipt harness differs from the pins")

    inputs = receipt["inputs"]
    exact(inputs, {"case", "ready_sha256", "revision", "release", "outbound_zat",
                   "inbound_zat", "spendback_zat", "pins_sha256"}, f"{case} inputs")
    require(inputs["case"] == case and inputs["revision"] == release["commit"]
            and inputs["release"] == release["tag"],
            f"{case}: inputs are for another case or release")
    require(type(inputs["ready_sha256"]) is str
            and inputs["ready_sha256"] == before["fixture_ready_sha256"],
            f"{case}: input readiness differs from the pins")
    require(inputs["pins_sha256"] == fingerprint(before),
            f"{case}: input pin digest differs from the recorded pins")
    for leg in ("outbound", "inbound", "spendback"):
        require(type(inputs[f"{leg}_zat"]) is int and inputs[f"{leg}_zat"] > 0,
                f"{case}: missing {leg} amount in the pinned recipe")

    _, variant = WALLETS[case]
    require(receipt.get("wallets") == {"under_test": variant, "counterpart": "ordinary-zallet"},
            f"{case}: unexpected wallet or counterpart role")
    for field in ("wallet_identity", "counterpart_identity"):
        token(receipt.get(field), f"{case} {field}")
    require(receipt["wallet_identity"] != receipt["counterpart_identity"],
            f"{case}: payer and payee share a wallet identity")

    claims = client_claims(before["executables_sha256"], receipt["upstream"])

    fixture = preflight(receipt["preflight"], release)
    recipe(receipt["fixture_recipe"], fixture)

    result = receipt["result"]
    exact(result, RESULT_KEYS, f"{case} result")
    funding = result["funding"]
    require(funding.get("height") == 261, f"{case}: funding height differs from the case recipe")
    require(type(funding.get("spendable_zat")) is int
            and funding["spendable_zat"] >= inputs["outbound_zat"] + 200_000,
            f"{case}: funding did not cover the outbound payment")
    require(result["reopen_ledger_verified"] is True and result["restart_verified"] is True,
            f"{case}: both wallets were not reopened and verified")
    for field in ("wallet_after_reopen_zat", "counterpart_after_reopen_zat"):
        require(type(result[field]) is int and result[field] >= 0, f"{case}: invalid {field}")
    endpoint = result["endpoint_evidence"]
    require(endpoint.get("target_connected") is True and endpoint.get("scope") == "process-trace",
            f"{case}: wallet endpoint binding is missing")
    require(isinstance(endpoint.get("endpoint"), str)
            and LOOPBACK.fullmatch(endpoint["endpoint"]),
            f"{case}: wallet endpoint is not loopback")
    counts = [endpoint.get(name) for name in ("operations", "processes")]
    require(sum(type(value) is int and value > 0 for value in counts) == 1,
            f"{case}: traced wallet operation count is missing")
    token(endpoint.get("trace_fingerprint_sha256"), f"{case} trace fingerprint")
    traced = endpoint.get("traces")
    require(isinstance(traced, list) and traced, f"{case}: no traced wallet operations")
    for row in traced:
        require(set(row) in ({"operation", "endpoint", "connect_log_sha256"},
                             {"endpoint", "connect_log_sha256"}),
                f"{case}: unexpected fields in an endpoint trace")
        require(row["endpoint"] == endpoint["endpoint"], f"{case}: trace endpoint differs")
        token(row["connect_log_sha256"], f"{case} trace log")
    counterpart = result["counterpart_endpoint_evidence"]
    exact(counterpart, {"scope", "endpoint", "config_sha256"}, f"{case} counterpart endpoint")
    require(counterpart["scope"] == "persisted-config"
            and counterpart["endpoint"] == result["counterpart_endpoint"]
            and LOOPBACK.fullmatch(counterpart["endpoint"]),
            f"{case}: counterpart endpoint is not bound to its persisted configuration")
    token(counterpart["config_sha256"], f"{case} counterpart configuration")

    cleanup = receipt["cleanup"]
    exact(cleanup, {"wallet_stopped", "counterpart_stopped", "errors", "counterpart",
                    "wallet", "verified"}, f"{case} wallet cleanup")
    require(cleanup["verified"] is True and cleanup["wallet_stopped"] is True
            and cleanup["counterpart_stopped"] is True and cleanup["errors"] == [],
            f"{case}: wallet shutdown was not verified")
    require(cleanup["wallet"].get("processes_remaining") == 0,
            f"{case}: wallet process remains after the case")
    exact(cleanup["counterpart"], {"scope", "processes_remaining", "pid"},
          f"{case} counterpart shutdown")
    require(cleanup["counterpart"]["scope"] == "daemon-identity"
            and cleanup["counterpart"]["processes_remaining"] == 0
            and cleanup["counterpart"]["pid"] is None,
            f"{case}: counterpart process remains after the case")

    evidence = receipt["evidence"]
    exact(evidence, RECEIPT_EVIDENCE_KEYS, f"{case} evidence")
    require(evidence["preflight_sha256"] == digest(state / "preflight.json"),
            f"{case}: preflight evidence changed")
    require(evidence["journal_sha256"] == digest(state / "run.json"),
            f"{case}: journal evidence changed")
    journal = load(state / "run.json")
    require_journal_bindings(receipt, journal, case)
    require(evidence["cleanup_sha256"] == digest(state / "cleanup.json"),
            f"{case}: wallet cleanup evidence changed")
    exact(evidence["counterpart_config"], {"path", "sha256"}, f"{case} counterpart config")
    require(evidence["counterpart_config"]["path"] == "zallet/zallet.toml",
            f"{case}: counterpart configuration path differs from the recipe")
    require(evidence["counterpart_config"]["sha256"] == digest(state / "zallet" / "zallet.toml"),
            f"{case}: counterpart configuration evidence changed")
    traces = evidence["traces"]
    require(isinstance(traces, list) and traces, f"{case}: no verified endpoint traces")
    for row in traces:
        exact(row, {"report", "connect_log", "report_sha256", "connect_log_sha256"},
              f"{case} trace evidence")
        report = safe_relative(state, row["report"], f"{case} trace report")
        connect = safe_relative(state, row["connect_log"], f"{case} trace log")
        require(digest(report) == row["report_sha256"], f"{case}: endpoint trace report changed")
        require(digest(connect) == row["connect_log_sha256"], f"{case}: endpoint trace log changed")
        values = load(report)
        require(values.get("status") == "passed" and values.get("fallback_used") is False,
                f"{case}: endpoint attribution failed")
    return receipt, claims


def require_journal_bindings(receipt, journal, case):
    """Bind receipt claims to the separately hashed run journal."""
    require(isinstance(journal.get("observations"), dict)
            and isinstance(journal.get("operations"), dict),
            f"{case}: journal observations or operations are missing")
    require(set(receipt["legs"]) == set(LEGS), f"{case}: receipt legs are incomplete")
    for name in LEGS:
        leg = receipt["legs"][name]
        observed = journal["observations"].get(name)
        operation = journal["operations"].get(name)
        require(isinstance(observed, dict) and isinstance(operation, dict),
                f"{case}/{name}: journal leg is missing")
        txid = operation.get("txid")
        require(isinstance(txid, str) and re.fullmatch(r"[0-9a-f]{64}", txid)
                and observed.get("txid") == txid,
                f"{case}/{name}: journal transaction identity is inconsistent")
        require(hashlib.sha256(bytes.fromhex(txid)).hexdigest() == leg["tx_fingerprint"],
                f"{case}/{name}: transaction fingerprint differs from journal")
        require(observed.get("block", {}).get("height") == leg["mined_height"],
                f"{case}/{name}: mined height differs from journal")
        for field in ("amount_zat", "fee_zat", "memo", "memo_verified",
                      "payer_before_zat", "payer_after_zat", "payee_before_zat",
                      "payee_after_zat", "sender_spendable_zat", "receiver_spendable_zat",
                      "payer_scanned_height", "payee_scanned_height"):
            require(observed.get(field) == leg.get(field),
                    f"{case}/{name}: {field} differs from journal")
        pending = journal["observations"].get(name + "-pending")
        actor = receipt["actor_observations"]["legs"].get(name)
        require(isinstance(pending, dict) and isinstance(actor, dict),
                f"{case}/{name}: pending journal evidence is missing")
        wallet_side, counterpart = (("payee", "payer") if name == "inbound"
                                    else ("payer", "payee"))
        require(actor["node_mempool"] is pending.get("node_mempool") is True
                and actor["wallet_under_test_pending_observed"] is pending.get(wallet_side)
                and actor["counterpart_pending_observed"] is pending.get(counterpart),
                f"{case}/{name}: pending actor mapping differs from journal")

    spent = receipt["result"]["incoming_note_spent"]
    journal_spent = journal["observations"].get("incoming-note-spent")
    require(isinstance(journal_spent, dict)
            and spent == journal_spent,
            f"{case}: incoming-note spend differs from journal")


def case_ledger(receipt, inputs):
    """Check the four legs, their balances, note spend, and reopened ledger."""
    result = receipt["result"]
    legs = {}
    for name in LEGS:
        raw = receipt["legs"][name]
        require(isinstance(raw, dict) and set(OUTCOME_LEGS) <= set(raw)
                <= set(OUTCOME_LEGS) | {"memo", "sender_spendable_zat", "receiver_spendable_zat",
                                        "payer_scanned_height", "payee_scanned_height"},
                f"{name}: unexpected fields in the leg record")
        require(raw.get("memo") == MEMOS[name], f"{name}: memo label differs from the campaign")
        for field in ("sender_spendable_zat", "receiver_spendable_zat"):
            require(type(raw.get(field)) is int and raw[field] >= 0,
                    f"{name}: missing {field}")
        require(type(raw["mined_height"]) is int and raw["mined_height"] > 0,
                f"{name}: invalid mined height")
        token(raw["tx_fingerprint"], f"{name} transaction fingerprint")
        for field in ("amount_zat", "fee_zat", "payer_before_zat", "payee_before_zat",
                      "payer_after_zat", "payee_after_zat"):
            require(type(raw[field]) is int and raw[field] >= 0, f"{name}: invalid {field}")
        require(raw["amount_zat"] > 0 and raw["fee_zat"] > 0, f"{name}: missing amount or fee")
        require(raw["memo_verified"] is True, f"{name}: recipient did not decrypt the memo")
        require(raw["payer_after_zat"] == raw["payer_before_zat"] - raw["amount_zat"]
                - raw["fee_zat"], f"{name}: payer debit does not reconcile")
        require(raw["payee_after_zat"] == raw["payee_before_zat"] + raw["amount_zat"],
                f"{name}: payee credit does not reconcile")
        for actor, spendable in (("payer", "sender_spendable_zat"), ("payee", "receiver_spendable_zat")):
            height = raw.get(f"{actor}_scanned_height")
            require(type(height) is int and height == raw["mined_height"] + 9,
                    f"{name}: {actor} has not scanned exactly ten confirmations")
            require(raw[spendable] == raw[f"{actor}_after_zat"],
                    f"{name}: {actor} balance is not fully spendable at ten confirmations")
        legs[name] = raw

    require(set(receipt["legs"]) == set(LEGS), "case legs are missing or substituted")
    require(legs["outbound"]["amount_zat"] == inputs["outbound_zat"]
            and legs["inbound"]["amount_zat"] == inputs["inbound_zat"]
            and legs["spendback"]["amount_zat"] == inputs["spendback_zat"],
            "leg amounts differ from the pinned case recipe")
    require(legs["sweep"]["amount_zat"] > 0, "sweep amount is not positive")
    heights = [legs[name]["mined_height"] for name in LEGS]
    require(heights == sorted(heights) and len(set(heights)) == len(LEGS),
            "legs do not follow one mined timeline")
    require(len({legs[name]["tx_fingerprint"] for name in LEGS}) == len(LEGS),
            "legs reuse a transaction fingerprint")

    wallet, peer = {}, {}
    for name in LEGS:
        if PAYER[name] == "wallet":
            wallet[name] = (legs[name]["payer_before_zat"], legs[name]["payer_after_zat"])
            peer[name] = (legs[name]["payee_before_zat"], legs[name]["payee_after_zat"])
        else:
            wallet[name] = (legs[name]["payee_before_zat"], legs[name]["payee_after_zat"])
            peer[name] = (legs[name]["payer_before_zat"], legs[name]["payer_after_zat"])
    require(peer["outbound"][0] == 0, "outbound recipient was not a fresh wallet")
    previous = None
    for name in LEGS:
        if previous is not None:
            require(wallet[name][0] == wallet[previous][1],
                    f"{name}: wallet balance does not follow the previous leg")
            require(peer[name][0] == peer[previous][1],
                    f"{name}: counterpart balance does not follow the previous leg")
        previous = name
    require(wallet["sweep"][1] < legs["spendback"]["amount_zat"],
            "sweep left enough change for the spendback to avoid the first incoming note")
    require(legs["inbound"]["receiver_spendable_zat"] >= legs["spendback"]["amount_zat"],
            "first incoming note was not spendable for the spendback")

    spent = result["incoming_note_spent"]
    exact(spent, {"notes", "spent", "spending_height"}, "first incoming note spend")
    require(type(spent["notes"]) is int and spent["notes"] > 0
            and isinstance(spent["spent"], list) and len(spent["spent"]) == spent["notes"],
            "first incoming note spend count is inconsistent")
    require(spent["spending_height"] == legs["spendback"]["mined_height"],
            "spendback height differs from the recorded note spend")
    consumed = 0
    for row in spent["spent"]:
        require(set(row) <= {"created_height", "value_zat", "spend_height"},
                "unexpected fields in the note spend record")
        require(set(row) >= {"created_height", "value_zat"}, "note spend record is incomplete")
        require(type(row["created_height"]) is int and type(row["value_zat"]) is int
                and row["value_zat"] > 0, "invalid spent note record")
        require(row["created_height"] == legs["inbound"]["mined_height"],
                "spent note was not created by the inbound transaction")
        if "spend_height" in row:
            require(row["spend_height"] == spent["spending_height"],
                    "spent note height differs from the spendback")
        consumed += row["value_zat"]
    require(consumed <= legs["inbound"]["amount_zat"],
            "spent notes exceed the inbound payment")
    require(consumed >= legs["spendback"]["amount_zat"],
            "spent notes do not cover the spendback")

    require(result["wallet_after_reopen_zat"] == wallet["spendback"][1],
            "wallet balance changed after restart")
    require(result["counterpart_after_reopen_zat"] == peer["spendback"][1],
            "counterpart balance changed after restart")
    exposure = result["restart_memo_exposure"]
    exact(exposure, set(LEGS), "reopened memo exposure")
    require(all(type(value) is bool for value in exposure.values()),
            "invalid reopened memo exposure")
    for name in ("outbound", "sweep", "inbound"):
        require(exposure[name] is True, f"{name}: memo was lost after restart")
    return legs, wallet, peer, exposure


def actor_observations(receipt):
    actor = receipt["actor_observations"]
    exact(actor, {"pending_requirement", "sender_pending_claimed", "legs"},
          "actor observations")
    require(actor["pending_requirement"] == "node_mempool_only",
            "pending requirement is not node-mempool observation")
    require(actor["sender_pending_claimed"] is False,
            "receipt claims sender pending")
    exact(actor["legs"], set(LEGS), "actor observations")
    for name in LEGS:
        row = actor["legs"][name]
        exact(row, {"node_mempool", "wallet_under_test_pending_observed",
                    "counterpart_pending_observed"}, f"{name} actor observation")
        require(row["node_mempool"] is True, f"{name}: node did not observe the transfer")
        for field in ("wallet_under_test_pending_observed", "counterpart_pending_observed"):
            require(type(row[field]) is bool, f"{name}: invalid {field}")
    return actor


def outcome(receipt):
    result = receipt["result"]
    return {
        "first_incoming_note_spent": result["incoming_note_spent"],
        "restart": {
            "reopen_ledger_verified": result["reopen_ledger_verified"],
            "restart_verified": result["restart_verified"],
            "wallet_after_reopen_zat": result["wallet_after_reopen_zat"],
            "counterpart_after_reopen_zat": result["counterpart_after_reopen_zat"],
            "memo_exposure": result["restart_memo_exposure"],
        },
        "legs": {name: {key: receipt["legs"][name][key] for key in OUTCOME_LEGS}
                 for name in LEGS},
        "actor_observations": receipt["actor_observations"],
    }


def provenance(receipt):
    pre = receipt["preflight"]
    return {
        "release": receipt["release"],
        "upstream": receipt["upstream"],
        "fixture": {
            "network": pre["network"],
            "anchor": pre["anchor"],
            "starting_height": pre["node_tip"]["height"],
            "node_image": pre["node_image"],
            "image_references": pre["image_references"],
            "images": pre["images"],
            "image_manifest_sha256": pre["image_manifest_sha256"],
            "activation_schedule_sha256": pre["activation_schedule_sha256"],
            "raw_blob_policy": pre["raw_blob_policy"],
        },
        "wallets": receipt["wallets"],
        "wallet_identity_fingerprint": receipt["wallet_identity"],
        "counterpart_identity_fingerprint": receipt["counterpart_identity"],
        "pins": receipt["pins"],
        "harness_sha256": receipt["harness_sha256"],
        "binaries": receipt["binaries"],
    }


def provenance_matches(value, expected):
    require(isinstance(value, dict) and set(value) == set(expected),
            "campaign provenance fields differ from the receipt")
    for key in value:
        if key != "fixture":
            require(value[key] == expected[key],
                    f"campaign provenance {key} differs from the receipt")
    fixture, wanted = value["fixture"], expected["fixture"]
    require(isinstance(fixture, dict) and set(wanted) - {"images"} <= set(fixture) <= set(wanted),
            "campaign fixture provenance differs from the receipt")
    for key in fixture:
        require(fixture[key] == wanted[key], f"campaign fixture {key} differs from the receipt")


def evidence_index(root, value, receipts):
    exact(value, {"schema_version", "files", "count"}, "evidence index")
    require(value["schema_version"] == 1, "unsupported evidence index schema")
    require(type(value["count"]) is int and value["count"] == len(value["files"])
            and value["count"] > 0, "evidence index count is inconsistent")
    indexed = {}
    for row in value["files"]:
        exact(row, {"path", "bytes", "sha256"}, "evidence index entry")
        require(type(row["bytes"]) is int and row["bytes"] >= 0, "invalid evidence size")
        token(row["sha256"], "evidence index entry")
        require(row["path"] not in indexed, "duplicate evidence index entry")
        target = safe_relative(root, row["path"], "evidence index entry")
        require(target.stat().st_size == row["bytes"] and digest(target) == row["sha256"],
                "evidence index entry changed")
        indexed[row["path"]] = row["sha256"]
    for case, sha in receipts.items():
        path = f"{case}/receipt.json"
        if path in indexed:
            require(indexed[path] == sha, f"{path}: index and summary disagree")
    return indexed


def campaign(summary_path, summary, release):
    """Validate the campaign summary and every case it references."""
    root = summary_path.parent.resolve()
    exact(summary, SUMMARY_KEYS, "campaign summary")
    require(summary["schema_version"] == 1, "unsupported campaign schema")
    require(summary["kind"] == "zkool-cake-payment-campaign", "unexpected campaign kind")
    require(summary["status"] == "passed", "campaign did not pass")
    require(summary["failures"] == {}, "campaign recorded failures")
    require(isinstance(summary["limits"], list) and summary["limits"]
            and all(isinstance(line, str) and line.strip() for line in summary["limits"]),
            "campaign limits are missing")
    exact(summary["release"], {"tag", "revision"}, "campaign release")
    require(summary["release"] == {"tag": release["tag"], "revision": release["commit"]},
            "campaign release differs from the selected release")

    bases = summary["port_bases"]
    require(isinstance(bases, dict) and set(bases) == set(CASES),
            "campaign port bases are incomplete")
    require(all(type(base) is int and 1024 < base < 65000 for base in bases.values())
            and len(set(bases.values())) == len(CASES), "campaign port bases are invalid")
    require(isinstance(summary["cases"], dict) and set(summary["cases"]) == set(CASES),
            "campaign cases are missing or substituted")
    require(isinstance(summary["cleanup"], dict)
            and set(summary["cleanup"]) == set(CASES), "campaign cleanup is missing")
    require(set(summary["evidence"]) == EVIDENCE_KEYS, "campaign evidence is incomplete")
    evidence = summary["evidence"]
    require(evidence["index"] == "evidence-index.json"
            and evidence["cleanup"] == "cleanup.json", "campaign evidence paths differ")
    for field in ("index_sha256", "cleanup_sha256"):
        token(evidence[field], f"campaign {field}")
    require(evidence["index_sha256"] == digest(root / "evidence-index.json"),
            "campaign evidence index changed")
    require(evidence["cleanup_sha256"] == digest(root / "cleanup.json"),
            "campaign cleanup evidence changed")
    require(load(root / "cleanup.json") == summary["cleanup"],
            "campaign cleanup file differs from the summary")
    exact(evidence["receipts"], CASES, "campaign receipt digests")

    receipts = {}
    for case in CASES:
        row = summary["cases"][case]
        exact(row, CASE_KEYS, f"{case} summary row")
        require(row["status"] == "passed", f"{case}: campaign case did not pass")
        require(row["receipt"] == f"{case}/receipt.json"
                and row["case_cleanup"] == f"{case}/cleanup.json",
                f"{case}: campaign paths differ from the case recipe")
        token(row["receipt_sha256"], f"{case} receipt")
        token(row["case_cleanup_sha256"], f"{case} cleanup")
        receipt_path = safe_relative(root, row["receipt"], f"{case} receipt")
        require(digest(receipt_path) == row["receipt_sha256"],
                f"{case}: receipt digest differs from the summary")
        require(evidence["receipts"][case] == row["receipt_sha256"],
                f"{case}: campaign receipt digests disagree")
        cleanup_path = safe_relative(root, row["case_cleanup"], f"{case} cleanup")
        require(digest(cleanup_path) == row["case_cleanup_sha256"],
                f"{case}: case cleanup digest differs from the summary")
        cleanup_row(row["cleanup"], f"{case} case cleanup", True)
        require(summary["cleanup"][case] == row["cleanup"],
                f"{case}: campaign and case cleanup disagree")

        receipt, claims = case_receipt(root, case, release)
        require(load(cleanup_path) == receipt["cleanup"],
                f"{case}: case cleanup file differs from the receipt")
        require(receipt["evidence"]["cleanup_sha256"] == row["case_cleanup_sha256"],
                f"{case}: receipt cleanup digest differs from the summary")
        provenance_matches(row["provenance"], provenance(receipt))
        require(row["outcome"] == outcome(receipt),
                f"{case}: campaign outcome differs from the receipt")
        actor_observations(receipt)
        ledger = case_ledger(receipt, receipt["inputs"])
        receipts[case] = {"receipt": receipt, "claims": claims, "ledger": ledger,
                          "sha256": row["receipt_sha256"]}
    evidence_index(root, load(root / "evidence-index.json"), evidence["receipts"])
    return receipts


def actor_rows(case, observation):
    record = observation["receipt"]
    engine, variant = WALLETS[case]
    fixture = record["preflight"]
    for leg in LEGS:
        detail = record["legs"][leg]
        pending = record["actor_observations"]["legs"][leg]
        for actor in (engine, "zallet"):
            under_test = actor == engine
            payer = under_test != (leg == "inbound")
            side, role = ("payer", "payer") if payer else ("payee", "recipient")
            peer = "zallet" if under_test else engine
            build = observation["claims"][actor]
            binary = record["binaries"][actor]
            version = build.get("version") or build.get("commit") or "binary " + binary[:12]
            observed = pending["wallet_under_test_pending_observed" if under_test else "counterpart_pending_observed"]
            checks = {"confirmed-in-wallet-history": "pass", "retained-confirmation-after-restart": "pass",
                      "own-pending-observed": "pass" if observed else "unexercised",
                      "node-observed-pending": "pass"}
            if payer:
                checks.update({"signed-and-broadcast": "pass", "debit-includes-reported-fee": "pass"})
            else:
                checks.update({"credited-received-amount": "pass", "decrypted-received-memo": "pass"})
            if under_test and leg == "inbound":
                checks["incoming-note-spendable"] = "pass"
            if under_test and leg == "spendback":
                checks["spent-identified-incoming-note"] = "pass"
            checks["target-only-online-connection-trace" if under_test else
                   "native-wallet-query-config-verified"] = "pass"
            yield {"id": f"zkool-cake-{case}-{leg}-{role}", "wallet": actor, "version": version,
                   "variant": variant if under_test else "ordinary", "implementation": "engine" if under_test else "app",
                   "interface": "lightwalletd" if under_test else "native-wallet-query", "network": "regtest",
                   "client_sha256": binary, "method_scope": "none", "methods": {}, "status": "pass",
                   "fixture": (f"Regtest anchor height {fixture['anchor']['height']}, node {fixture['node_image']}, "
                               f"schedule {fixture['activation_schedule_sha256']}, all-block retention, "
                               f"counterparty {peer} {record['binaries'][peer]}, engine profile {variant}"),
                   "action": f"{'Send' if payer else 'Receive'} the {leg} payment and verify its persisted ledger entry",
                   "assertions": checks,
                   "accounting_zat": {"amount": detail["amount_zat"], "fee": detail["fee_zat"] if payer else 0,
                                      "balance_after": detail[side + "_after_zat"],
                                      "spendable_after": detail[("sender" if payer else "receiver") + "_spendable_zat"]},
                   "evidence": {"description": f"{case}/receipt.json includes both wallet records for the {leg} payment",
                                "sha256": observation["sha256"]}}



def actions(summary_path, release):
    path = Path(summary_path)
    release_identity(release)
    cases = campaign(path, load(path), release)
    return [row for name in CASES for row in actor_rows(name, cases[name])]
