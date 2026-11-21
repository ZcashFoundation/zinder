"""Project real wallet recovery receipts into privacy-safe coverage rows.

Each receipt records an outbound signed payment from Zingo to Zallet, a
return payment from Zallet to Zingo, and the recovery event between them.
Both wallets see the same two payments, so a disconnect or rollback receipt
yields one row per wallet. The transport receipt yields only the Zallet row:
Zingo was the sending counterpart and stayed on its own lightwalletd route,
so it did not experience the private native-route outage.

Each row records the amount and resulting balance in both payment directions,
with the fee on the wallet that paid it.
"""
import hashlib
import json
from pathlib import Path
import re

from coverage_evidence import require

COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
BINARIES = ("compat-lightwalletd", "ingest", "projector", "query")
ACCOUNTING = {"amount_zat", "fee_zat", "payee_after_zat", "payee_credit_zat",
              "payer_after_zat", "payer_debit_zat"}
WALLETS = {"zingo": ("lightwalletd", "engine", "nakednet-cli"),
           "zallet": ("native-wallet-query", "app", "ordinary")}
RECEIPT_ASSERTIONS = {
    "disconnect": {
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "no_wallet_scan_during_disconnect",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "received_funds_spendable", "transfer_mined_and_canonical_while_disconnected",
        "wallets_disconnected_while_pending",
    },
    "transport": {
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "memo_verified", "native_route_down_before_reconnect",
        "node_and_other_wallet_routes_stayed_up", "only_the_private_native_route_was_stopped",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "received_funds_spendable", "same_endpoint_restored",
        "same_process_reconnected_without_restart",
        "transfer_mined_and_canonical_while_route_down",
        "zallet_process_alive_and_identical_through_outage",
    },
}
SHARED = {
    "canonical-block-and-hash-verified": "pass",
    "exactly-once-credit": "pass",
    "exactly-once-debit": "pass",
    "fee-reconciles": "pass",
    "received-funds-spendable": "pass",
    "persisted-across-wallet-restart": "pass",
    "canonical-signed-payment-linkage": "pass",
    "monetary-accounting-reconciled": "pass",
}


def token(value, where):
    require(isinstance(value, str) and SHA256.fullmatch(value), f"{where}: missing sha256")


def receipt(result, release, case):
    """Check one passed Regtest receipt and bind its release, binaries, and fixture."""
    fixture, clients = result["fixture"], result["clients"]
    require(result["schema_version"] == 1 and result["case"] == case
            and result["network"] == "zcash-regtest" and result["status"] == "passed",
            f"{case}: not a passed Regtest recovery receipt")
    require(isinstance(release.get("commit"), str) and COMMIT.fullmatch(release["commit"]),
            "release: missing commit")
    require(fixture["release_revision"] == release["commit"], f"{case}: release mismatch")
    require(fixture["raw_blob_policy"] == "all", f"{case}: fixture does not retain all blocks")
    token(fixture["activation_schedule_sha256"], f"{case} schedule")
    token(fixture["ready_sha256"], f"{case} readiness")
    token(result["preflight"]["sha256"], f"{case} preflight")
    require(IMAGE.fullmatch(fixture["node_image"]), f"{case}: invalid node image")
    require(set(fixture["release_binary_sha256"]) == set(BINARIES), f"{case}: incomplete binary pins")
    for name in BINARIES:
        token(fixture["release_binary_sha256"][name], f"{case} {name} binary")
    require(clients["payer"] == "zingo-cli" and clients["payee"] == "zallet-zinder",
            f"{case}: unexpected wallet roles")
    for name in ("zingo_sha256", "zallet_sha256"):
        token(clients[name], f"{case} {name}")
    require("pins" in result, "missing pre-run and post-run wallet and harness pins")
    pins = result["pins"]
    require(pins["pre"] == pins["post"], "inputs changed during the run")
    require(pins["pre"]["wallets"] == {"zingo": clients["zingo_sha256"], "zallet_backend": clients["zallet_sha256"]},
            "wallet pins differ from the receipt")
    harness = pins["pre"]["harness"]
    required = {"payment-recovery.py", "regtest-fixture.py", "regtest-fixture-relay.py",
                "run-wallet-roundtrip.py", "roundtrip_fixture.py", "roundtrip_wallets.py", "wallet_fixture.py",
                "run-wallet-roundtrip-regtest.py", "coverage_roundtrip.py", "coverage_evidence.py",
                "docker-compose.yml", "regtest-nu6-at-one.toml", "compact_formats_pb2.py", "service_pb2.py",
                "wallet_pb2.py", "server_info_pb2.py"}
    if case == "transport":
        required.add("transport-payment.py")
    require(required <= harness.keys(), "incomplete recovery harness pins")
    require(harness["regtest-nu6-at-one.toml"] == fixture["activation_schedule_sha256"],
            "recovery fixture used a different activation schedule")
    for name, value in harness.items():
        token(value, name)
    identities = clients["wallet_identity_sha256"]
    require(len(identities) == 2 and len(set(identities)) == 2
            and all(isinstance(v, str) and SHA256.fullmatch(v) for v in identities),
            f"{case}: distinct wallet identities required")
    anchor = fixture["anchor"]
    require(type(anchor["height"]) is int and anchor["height"] > 0, f"{case}: invalid anchor height")
    token(anchor["hash"], f"{case} anchor")
    for name in ("compat_tip", "native_tip", "node_tip"):
        require(result["preflight"][name] == anchor, f"{case}: {name} differs from the frozen anchor")
    require(result["initial_wallet_heights"] == {"zingo": anchor["height"], "zallet": anchor["height"]},
            f"{case}: wallets did not start at the frozen anchor")
    require(result["cleanup"]["fixture_removed"] is True
            and result["cleanup"]["project_resources_remaining"] == 0,
            f"{case}: fixture cleanup is incomplete")
    require(any("peer-isolated" in line for line in result["limits"]),
            f"{case}: fixture is not declared peer-isolated")
    return result


def ledger(result):
    """Check monetary accounting and canonical signed-payment linkage."""
    scenario = result["scenario"]
    funding, outbound, returned = result["funding"], scenario["outbound"], scenario["return"]
    persistence = scenario["persistence"]
    require(funding["source"] == "regtest-coinbase-and-shield"
            and type(funding["mined_height"]) is int
            and type(funding["spendable_after_zat"]) is int and funding["spendable_after_zat"] > 0,
            "missing Regtest funding")
    token(funding["tx_fingerprint"], "funding transaction")
    for name, leg in (("outbound", outbound), ("return", returned)):
        token(leg["tx_fingerprint"], f"{name} transaction")
        token(leg["block_hash"], f"{name} block")
        require(type(leg["mined_height"]) is int and leg["mined_height"] > 0, f"{name}: invalid height")
        require(set(leg["pending"]) == {"node_mempool", "payee", "payer"}
                and all(value is True for value in leg["pending"].values()),
                f"{name}: missing pre-mining observation")
        surface = leg["surfaces"]
        require(surface["height"] >= leg["mined_height"] and SHA256.fullmatch(surface["hash"])
                and set(surface["surfaces"]) == {"lightwalletd", "native"}
                and all(s["height"] == surface["height"] and s["hash"] == surface["hash"]
                        for s in surface["surfaces"].values()), f"{name}: inconsistent served surfaces")
    require(len({funding["tx_fingerprint"], outbound["tx_fingerprint"], returned["tx_fingerprint"]}) == 3,
            "reused transfer across funding and payments")
    require(funding["mined_height"] < outbound["mined_height"] < returned["mined_height"],
            "payment heights do not follow funding")
    first, second = outbound["accounting"], returned["accounting"]
    for leg in (first, second):
        require(set(leg) == ACCOUNTING and all(type(v) is int and v >= 0 for v in leg.values()),
                "invalid wallet accounting")
        require(leg["amount_zat"] > 0 and leg["fee_zat"] > 0, "missing amount or sender fee")
        require(leg["payee_credit_zat"] == leg["amount_zat"], "recipient credit differs from amount")
        require(leg["payer_debit_zat"] == leg["amount_zat"] + leg["fee_zat"],
                "sender debit does not include the fee")
    require(first["payee_after_zat"] == first["amount_zat"], "first recipient balance mismatch")
    require(first["payer_after_zat"] == funding["spendable_after_zat"] - first["payer_debit_zat"],
            "first sender balance does not reconcile")
    require(second["payer_after_zat"] == first["payee_after_zat"] - second["payer_debit_zat"],
            "return sender balance does not reconcile")
    require(second["payee_after_zat"] == first["payer_after_zat"] + second["payee_credit_zat"],
            "return recipient balance does not reconcile")
    spendable = outbound.get("spendable_after_zat",
                             scenario.get("maturity", {}).get("spendable_after_zat"))
    require(type(spendable) is int and spendable >= first["amount_zat"],
            "received payment is not spendable")
    require(returned["spendable_after_zat"] == second["payee_after_zat"],
            "returned balance is not spendable")
    require(set(persistence) == {"zingo_total_zat", "zallet_total_zat", "zingo_height",
                                 "zallet_height"}, "invalid persisted wallet state")
    require(persistence["zingo_total_zat"] == second["payee_after_zat"]
            and persistence["zallet_total_zat"] == second["payer_after_zat"],
            "persisted balances disagree with accounting")
    require(persistence["zingo_height"] >= returned["mined_height"]
            and persistence["zallet_height"] >= returned["mined_height"],
            "persisted wallets did not reach the return height")
    return outbound, {
        "zingo": {
            "accounting_zat": {"amount": first["amount_zat"], "fee": first["fee_zat"],
                               "balance_after": first["payer_after_zat"]},
            "return_accounting_zat": {"amount": second["amount_zat"], "credit": second["payee_credit_zat"],
                                      "balance_after": second["payee_after_zat"],
                                      "spendable_after": returned["spendable_after_zat"]},
        },
        "zallet": {
            "accounting_zat": {"amount": first["amount_zat"], "balance_after": first["payee_after_zat"],
                               "spendable_after": spendable},
            "return_accounting_zat": {"amount": second["amount_zat"], "fee": second["fee_zat"],
                                      "debit": second["payer_debit_zat"], "balance_after": second["payer_after_zat"]},
        },
    }


def disconnect_evidence(result, outbound):
    evidence = result["disconnect"]
    require(evidence == result["scenario"]["disconnect"], "disconnect record changed between views")
    mined = evidence["mined_while_disconnected"]
    require(evidence["mempool_before_mining"] is True
            and set(evidence["last_online_heights"]) == {"zingo", "zallet"}
            and all(value == mined["height"] - 1 for value in evidence["last_online_heights"].values()),
            "pending transfer was not observed before the wallets left")
    require(isinstance(evidence["zingo"], str) and "not invoked" in evidence["zingo"]
            and isinstance(evidence["zallet"], str) and "process identity was cleared" in evidence["zallet"]
            and evidence["zallet_process_absent"] is True,
            "wallet absence was not proven per actor")
    require(mined["height"] == outbound["mined_height"]
            and mined["block_hash"] == outbound["block_hash"], "mined block differs from the payment")
    require(type(mined["confirmations"]) is int and mined["confirmations"] >= 10
            and evidence["node_tip_while_disconnected"] - mined["height"] + 1 >= mined["confirmations"],
            "payment did not reach ten confirmations while the wallets were absent")
    scans = evidence["wallet_scan_delta"]
    require(isinstance(scans, dict) and scans
            and all(name.startswith("GetLatestBlock/") and type(count) is int and count > 0
                    for name, count in scans.items()),
            "wallet scan occurred during the disconnect window")
    surfaces = evidence["surfaces_while_disconnected"]
    require(surfaces["height"] == evidence["node_tip_while_disconnected"]
            and surfaces["hash"] == surfaces["surfaces"]["lightwalletd"]["hash"]
            == surfaces["surfaces"]["native"]["hash"], "disconnected surfaces disagree")
    reconnect = result["scenario"]["reconnect"]
    require(reconnect["zingo_height"] >= mined["height"] + 9
            and reconnect["zallet_height"] >= mined["height"] + 9,
            "a wallet did not rescan the blocks mined during its absence")
    if reconnect["raw_sha256"] is not None:
        token(reconnect["raw_sha256"], "reconnected signed bytes")


def rollback_evidence(result, outbound):
    scenario = result["scenario"]
    orphan, readmission, remine = scenario["orphan"], scenario["readmission"], scenario["remine"]
    replacement = scenario["replacement"]
    require(orphan["old_height"] == outbound["mined_height"]
            and orphan["old_block_hash"] == outbound["block_hash"],
            "orphaned block differs from the outbound inclusion")
    require(orphan["tx_absent_from_active_chain"] is True, "payment remained on the active chain")
    old = orphan["old_block_state"]
    require(old["header_available"] is False or (
        old["header_available"] is True and old["height"] == orphan["old_height"]
        and type(old["confirmations"]) is int and old["confirmations"] < 0),
        "retained orphan header must identify an inactive block")
    require(orphan["old_chain_tip_status"] in {None, "unsupported", "invalid", "valid-fork"},
            "unexpected old chain tip status")
    old_height, new_height = remine["old_height"], remine["new_height"]
    require(type(old_height) is int and type(new_height) is int
            and old_height > 0 and new_height in {old_height, old_height + 1},
            "remined height is not the invalidated height or its successor")
    require(new_height < scenario["return"]["mined_height"],
            "remined payment does not precede the return payment")
    if remine["empty_replacement"]:
        token(orphan["empty_replacement_hash"], "empty replacement")
        require(orphan["empty_replacement_height"] == orphan["old_height"]
                and type(orphan["empty_replacement_height"]) is int
                and orphan["empty_replacement_hash"] != orphan["old_block_hash"]
                and orphan["empty_replacement_excludes_tx"] is True
                and orphan["readded_after_recovery_attempts"] is False,
                "empty replacement did not exclude the orphaned transfer")
        require(new_height == old_height + 1 and remine["height_changed"] is True,
                "remined payment did not change height after the empty replacement")
    else:
        require(new_height == old_height and remine["height_changed"] is False,
                "readmitted payment was not remined at the invalidated height")
    require(readmission["mempool_present"] is True
            and readmission["method"] in {"sendrawtransaction-same-bytes", "node-reorg-readmission"},
            "missing transfer readmission")
    for name in ("raw_sha256", "raw_sha256_after"):
        token(remine[name], f"remine {name}")
    if "observed_raw_sha256" in readmission or "original_raw_sha256" in readmission:
        readmitted = readmission["observed_raw_sha256"]
        require(readmitted == readmission["original_raw_sha256"], "readmission changed the signed bytes")
    else:
        readmitted = readmission["raw_sha256"]
    require(readmitted == remine["raw_sha256"] == remine["raw_sha256_after"],
            "remined bytes differ from the readmitted signed bytes")
    require(remine["tx_fingerprint"] == outbound["tx_fingerprint"],
            "remined payment identity changed")
    token(remine["new_block_hash"], "remine block")
    require(remine["old_height"] == orphan["old_height"]
            and remine["old_block_hash"] == orphan["old_block_hash"]
            and remine["new_block_hash"] != remine["old_block_hash"],
            "payment was not remined on a new branch")
    require(remine["membership_count"] == 1, "remined payment is not included exactly once")
    require(scenario["accounting_after_remine"] == outbound["accounting"]
            and scenario["accounting_equal_before_and_after"] is True,
            "accounting changed across the rollback")
    require(scenario["maturity"]["spendable_after_zat"] >= outbound["accounting"]["amount_zat"],
            "remined payment is not spendable")
    require(replacement["wallet_old_height"] == orphan["old_height"]
            and replacement["wallet_new_height"] == remine["new_height"]
            and replacement["wallet_observed_height_change"] is remine["height_changed"],
            "wallets did not observe the replacement height")
    require(replacement["tip_height"] > remine["old_height"] and replacement["tip_height"] >= remine["new_height"],
            "replacement branch is not longer than the remined block")
    surfaces = replacement["surfaces"]
    require(surfaces["lightwalletd"] == surfaces["native"]
            and surfaces["native"]["height"] == replacement["tip_height"]
            and surfaces["native"]["hash"] == replacement["tip_hash"],
            "replacement surfaces disagree")
    return remine, orphan


def transport_evidence(result, outbound):
    outage, same, transport = result["outage"], result["same_process"], result["transport"]
    require(outage == result["scenario"]["outage"], "outage record changed between views")
    require(transport["only_native_route"] is True and type(transport["port_base"]) is int,
            "transport receipt does not own one private route")
    require(transport["started"]["endpoint"] == transport["restored"]["endpoint"],
            "native route endpoint changed")
    restore = result["scenario"]["restore"]
    require(restore["same_process_reconnect"] is True
            and restore["endpoint"] == transport["restored"]["endpoint"],
            "restored route is not the same endpoint")
    require(outage["native_route_reachable"] is False
            and outage["direct_native_reachable"] is True
            and outage["fixture_relay_unchanged"] is True,
            "native route outage was not isolated")
    require(outage["relay"]["exit_signal"] == "SIGTERM" and outage["relay"]["exit_code"] != 0,
            "native route relay was not stopped")
    require(outage["zallet_process_unchanged"] is True, "Zallet process changed during the outage")
    status = outage["zallet_status_while_down"]
    require(type(status.get("available")) is bool, "missing local wallet status RPC observation")
    if status["available"]:
        observation = "available"
    else:
        require(isinstance(status.get("error"), str) and status["error"], "missing local wallet RPC error")
        observation = "wallet-rpc-error" if status["error"] == "WalletRpcError" else "unavailable"
    inclusion = outage["inclusion"]
    require(inclusion["height"] == outbound["mined_height"]
            and inclusion["block_hash"] == outbound["block_hash"],
            "mined block differs from the payment")
    require(type(inclusion["confirmations"]) is int and inclusion["confirmations"] >= 10
            and outage["node_tip"] - inclusion["height"] + 1 >= inclusion["confirmations"],
            "payment did not reach ten confirmations while the route was down")
    require(outage["transfer_in_mempool_after_mining"] is False,
            "mined transfer remained in the node mempool")
    surfaces = outage["surfaces"]
    require(surfaces["height"] == outage["node_tip"]
            and surfaces["hash"] == surfaces["surfaces"]["lightwalletd"]["hash"]
            == surfaces["surfaces"]["native"]["hash"], "outage surfaces disagree")
    require(same["reconnected"] is True and same["process_identical"] is True
            and same["height"] == outbound["mined_height"]
            and same["state"] == "confirmed_exactly_once" and "restart_control" not in result,
            "Zallet did not reconnect in the same process")
    received = result["scenario"]["outbound"]
    expected_memo = "zinder-payment-recovery-v1/outbound"
    if "received_memo" in received:
        memo_matches = received["received_memo"] == {
            "expected_sha256": hashlib.sha256(expected_memo.encode()).hexdigest(), "expected_present": True}
    else:
        memo_matches = expected_memo in received.get("received_memos", [])
    require(received["received_amount_zat"] == outbound["accounting"]["amount_zat"]
            and received["memo_verified"] is True and memo_matches
            and received["fully_synced_height"] >= outage["node_tip"],
            "reconnected Zallet did not observe the full payment")
    return observation


def expected_assertions(result, case, remine=None):
    require(set(result["assertions"]) == expected_names(case, remine)
            and all(value is True for value in result["assertions"].values()),
            f"{case}: receipt assertions changed or are unproven")
    return {name.replace("_", "-"): "pass" for name in result["assertions"]}


def expected_names(case, remine=None):
    if case != "rollback":
        return RECEIPT_ASSERTIONS[case]
    names = {
        "canonical_block_and_hash_verified", "exactly_once_credit", "exactly_once_debit",
        "fee_reconciles", "old_block_removed_from_active_chain",
        "orphaned_transfer_absent_from_active_chain",
        "pending_observed_by_node_and_both_wallets", "persisted_across_wallet_restart",
        "readmission_observed", "received_funds_spendable", "remine_same_signed_bytes",
        "replacement_block_contains_transfer_once",
    }
    if remine["empty_replacement"]:
        names.add("empty_replacement_excludes_transfer")
    if remine["height_changed"]:
        names.add("remine_at_different_height")
    return names


def row(result, receipt_sha, case, role, action, assertions, accounting):
    fixture, clients = result["fixture"], result["clients"]
    peer = "zallet" if role == "zingo" else "zingo"
    interface, implementation, variant = WALLETS[role]
    client_sha = clients[f"{role}_sha256"]
    return {
        "id": f"payment-recovery-{case}-{role}",
        "wallet": role, "version": "binary " + client_sha[:12],
        "variant": variant, "implementation": implementation, "interface": interface,
        "network": result["network"], "client_sha256": client_sha,
        "fixture": (f"Regtest height {fixture['anchor']['height']}, schedule "
                    f"{fixture['activation_schedule_sha256']}, {fixture['node_image']}, "
                    f"all-block retention, peer {peer} {clients[f'{peer}_sha256']}"),
        "action": action, "status": "pass", "method_scope": "none", "methods": {},
        "assertions": assertions, **accounting[role],
        "evidence": {"description": f"{case.capitalize()} wallet recovery / receipt.json",
                     "sha256": receipt_sha},
    }


def verify_case_supervision(path, result, receipt_sha):
    project = result.get("supervised_project")
    record_path = Path(path).with_name("supervision.json")
    if project is None and not record_path.exists():
        return
    require(record_path.is_file(), "missing final campaign supervision record")
    record = json.loads(record_path.read_bytes())
    require(record.get("schema_version") == 1 and record.get("case") == result["case"]
            and record.get("status") == "passed"
            and (project is None or record.get("project") == project == result["fixture"].get("project"))
            and record.get("receipt") == {"status": "passed", "sha256": receipt_sha},
            "campaign supervision rejected this case or recorded a different receipt")
    stage, cleanup = record["supervision"], record["cleanup"]
    require(stage.get("kind") == "cooperative" and stage.get("exit_code") == 0
            and cleanup.get("resources_remaining") == 0 and cleanup.get("problems") == []
            and cleanup.get("repaired") is False, "campaign supervision or teardown failed")


def recovery_actions(path, release):
    """Yield schema-1 rows from a disconnect, rollback, or transport receipt."""
    raw = Path(path).read_bytes()
    result = json.loads(raw)
    receipt_sha = hashlib.sha256(raw).hexdigest()
    case = result["case"]
    verify_case_supervision(path, result, receipt_sha)
    receipt(result, release, case)
    outbound, accounting = ledger(result)
    if case == "disconnect":
        disconnect_evidence(result, outbound)
        checks = expected_assertions(result, case)
        for role, mechanism, scan, identity in (
            ("zingo", "one-shot-cli-not-invoked-while-pending", "pass", "unexercised"),
            ("zallet", "daemon-process-identity-cleared-while-pending", "unexercised", "pass"),
        ):
            assertions = dict(SHARED, **checks)
            assertions.update({
                "wallet-rescanned-blocks-mined-during-absence": "pass",
                mechanism: "pass",
                "no-wallet-scan-during-disconnect": scan,
                "offline-process-identity": identity,
                "native-route-outage": "unexercised",
            })
            action = ("Remain an uninvoked one-shot CLI while a pending payment is mined, "
                      "then rescan it" if role == "zingo" else
                      "Stop the daemon while a pending payment is mined, then reopen and recover the confirmed payment")
            yield row(result, receipt_sha, case, role, action, assertions, accounting)
    elif case == "rollback":
        remine, orphan = rollback_evidence(result, outbound)
        checks = expected_assertions(result, case, remine)
        for role in ("zingo", "zallet"):
            assertions = dict(SHARED, **checks)
            assertions.update({
                "wallet-followed-replacement-branch": "pass",
                "empty-replacement-excludes-transfer":
                    "pass" if remine["empty_replacement"] else "unexercised",
                "remine-at-different-height":
                    "pass" if remine["height_changed"] else "unexercised",
                "old-chain-tip-status":
                    "unexercised" if orphan["old_chain_tip_status"] in {None, "unsupported"} else "pass",
                "deep-reorg-and-finalization": "unexercised",
                "native-route-outage": "unexercised",
            })
            yield row(result, receipt_sha, case, role,
                      "Follow a shallow reorg that orphaned and remined the same signed payment",
                      assertions, accounting)
    elif case == "transport":
        status_observation = transport_evidence(result, outbound)
        checks = expected_assertions(result, case)
        assertions = dict(SHARED, **checks)
        assertions.update({
            "graceful-wallet-offline": "unexercised",
            "wallet-process-restart-during-outage": "unexercised",
        })
        observed = row(result, receipt_sha, case, "zallet",
                       "Reconnect the same Zallet process after its private native route was lost "
                       "during a pending payment", assertions, accounting)
        observed["evidence"]["local_status_rpc_during_outage"] = status_observation
        yield observed
    else:
        raise ValueError(f"unsupported recovery case {case!r}")
