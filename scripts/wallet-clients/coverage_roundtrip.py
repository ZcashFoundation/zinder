"""Project a paired signed Regtest payment into payer and payee coverage rows."""
import re

from coverage_evidence import digest, load, require


def sha_token(value):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{64}", value) is not None


def check_receipt(receipt, revision):
    cfg, recipe = receipt["inputs"], receipt["fixture_recipe"]
    require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1 and
            cfg["revision"] == revision and cfg["network"] == recipe["network"] == receipt["network"] == "regtest"
            and recipe["raw_blob_policy"] == "all", "round trip release or fixture mismatch")
    require(receipt["status"] == "passed" and all(receipt[k] is True for k in (
        "pending_certified", "independent_wallets", "reopen_ledger_verified")), "incomplete signed round trip")
    ids = receipt["wallet_identity_sha256"]
    require(len(ids) == 2 and len(set(ids)) == 2 and all(sha_token(v) for v in ids), "distinct wallet identities required")
    require(all(sha_token(cfg[k]) for k in ("ready_sha256", "zingo_sha256", "zallet_sha256")) and
            sha_token(recipe["genesis"]) and sha_token(recipe["activation_schedule_sha256"])
            and re.fullmatch(r"sha256:[0-9a-f]{64}", recipe["node_image"])
            and type(recipe["starting_height"]) is int and recipe["starting_height"] >= 0,
            "missing pinned inputs or fixture recipe")
    require(set(recipe["release_binaries"]) == {"compat-lightwalletd", "ingest", "projector", "query"}
            and all(sha_token(v) for v in recipe["release_binaries"].values()), "invalid release binary pins")
    require(receipt["invocation_harnesses"] and receipt["harness_sha256"]
            and all(sha_token(v) for v in receipt["harness_sha256"].values()), "missing harness pins")
    require(set(receipt["legs"]) == {"outbound", "return"}, "expected two payment legs")
    out, back = receipt["legs"]["outbound"], receipt["legs"]["return"]
    funding = receipt["funding"]
    require(funding["source"] == "regtest-coinbase-and-shield" and type(funding["spendable_after_zat"]) is int
            and type(funding["mined_height"]) is int and funding["mined_height"] > 0, "missing Regtest funding")
    for leg, key in ((out, "outbound_zat"), (back, "return_zat")):
        require(all(type(leg[k]) is int and leg[k] >= 0 for k in (
            "amount_zat", "fee_zat", "mined_height", "payee_after_zat", "payer_after_zat", "spendable_after_zat"))
            and leg["amount_zat"] == cfg[key] > 0 and leg["fee_zat"] > 0 and leg["memo_verified"] is True,
            "missing recipient amount, decrypted memo, or sender fee")
        require(set(leg["pending"]) == {"payer", "payee", "node_mempool"}
                and all(s is True for s in leg["pending"].values()), "missing pre-mining wallet or node observation")
    txs = [funding["tx_fingerprint"], out["tx_fingerprint"], back["tx_fingerprint"]]
    require(len(set(txs)) == 3 and all(sha_token(t) for t in txs)
            and funding["mined_height"] < out["mined_height"] < back["mined_height"], "unlinked funding and transfers")
    require(out["payer_after_zat"] == funding["spendable_after_zat"] - out["amount_zat"] - out["fee_zat"]
            and out["spendable_after_zat"] == out["payee_after_zat"] == out["amount_zat"] and
            back["payer_after_zat"] == out["payee_after_zat"] - back["amount_zat"] - back["fee_zat"] and
            type(back["change_zat"]) is int and back["change_zat"] == back["payer_after_zat"] and
            back["payee_after_zat"] == out["payer_after_zat"] + back["amount_zat"] and
            0 <= back["spendable_after_zat"] <= back["payee_after_zat"], "round trip balance or change mismatch")


def wallet_outcomes(legs):
    fields = ("amount_zat", "fee_zat", "memo_verified", "payer_after_zat", "payee_after_zat",
              "spendable_after_zat", "change_zat", "pending")
    return {name: {field: leg[field] for field in fields} for name, leg in legs.items()}


def compare_runs(left, right):
    for result in (left, right):
        check_receipt(result, left["inputs"]["revision"])
    require(left["inputs"]["ready_sha256"] != right["inputs"]["ready_sha256"], "reuse of the same fixture receipt")
    require(len({r["legs"][k]["tx_fingerprint"] for r in (left, right) for k in ("outbound", "return")}) == 4,
            "reused transfer across independent round trips")
    require(set(left["wallet_identity_sha256"]).isdisjoint(right["wallet_identity_sha256"]),
            "wallet identity reused across fixtures")
    for result in (left, right):
        require(all(h == result["harness_sha256"] for h in result["invocation_harnesses"]),
                "compared run spans different harness code")
    for name in ("zingo_sha256", "zallet_sha256", "revision", "outbound_zat", "return_zat"):
        require(left["inputs"][name] == right["inputs"][name], "incomparable " + name)
    require(left["fixture_recipe"] == right["fixture_recipe"] and left["harness_sha256"] == right["harness_sha256"],
            "paired fixture configuration or harness differs")
    require(wallet_outcomes(left["legs"]) == wallet_outcomes(right["legs"]), "independent wallet outcomes differ")
    return {"status": "passed", "same_wallet_outcomes": True, "independent_fixtures": True}


def audit_pair(audit, receipts):
    require(type(audit["schema_version"]) is int and audit["schema_version"] == 1 and audit["passed"] is True and
            (audit["independent_chains"], audit["distinct_wallet_identities"], audit["independent_transactions"])
            == (2, 4, 4), "expected canonical audit of two independent round trips")
    runs = {r["receipt_sha256"]: r for r in audit["runs"].values()}
    require(len(audit["runs"]) == 2 and set(receipts) == set(runs), "canonical audit does not bind both receipts")
    for sha, r in receipts.items():
        audit_run = runs[sha]
        require(audit_run["ready_sha256"] == r["inputs"]["ready_sha256"] and
                audit_run["end_height"] >= r["legs"]["return"]["mined_height"] and
                audit_run["canonical_txid_fingerprints"] == {k: leg["tx_fingerprint"] for k, leg in r["legs"].items()},
                "canonical audit disagrees with fixture or transfer identity")
        recorded = audit["matching_outcomes"]["leg_outcomes"]
        expected = wallet_outcomes(r["legs"])
        for name, values in expected.items():
            # Earlier schema-v1 audits also carried a shared height for each leg.
            if "mined_height" in recorded[name]:
                values["mined_height"] = r["legs"][name]["mined_height"]
        require(recorded == expected, "canonical audit accounting differs")


def roundtrip_actions(paths, release, audit_path=None):
    two = [load(p) for p in paths]
    require(two[0]["inputs"]["revision"] == release["commit"], "round trip release mismatch")
    compare_runs(*two)
    hashes = [digest(p) for p in paths]
    sources = {"repeat-receipt": hashes[1]}
    if audit_path:
        audit_pair(load(audit_path), dict(zip(hashes, two)))
        sources["canonical-audit"] = digest(audit_path)
    r, cfg, recipe = two[0], two[0]["inputs"], two[0]["fixture_recipe"]
    for role, peer, interface, implementation, variant in (
        ("zingo", "zallet", "lightwalletd", "engine", "nakednet-cli"),
        ("zallet", "zingo", "native-wallet-query", "app", "ordinary"),
    ):
        for direction, leg in r["legs"].items():
            sending = (direction == "outbound") == (role == "zingo")
            action = "send" if sending else "receive"
            checks = ["independent-repeat", "pending-wallet-state", "node-mempool",
                      "wallet-confirmed-at-mined-height", "persisted-transaction-and-balance"]
            accounting = {"amount": leg["amount_zat"]}
            if sending:
                checks += ["signed-payment", "sender-debit-includes-fee"]
                accounting.update(fee=leg["fee_zat"], balance_after=leg["payer_after_zat"])
                if direction == "return":
                    checks += ["spend-received-funds", "change-reconciled"]
                    accounting["change"] = leg["change_zat"]
            else:
                checks += ["received-amount", "decrypted-memo", "recipient-credit"]
                accounting.update(balance_after=leg["payee_after_zat"], spendable_after=leg["spendable_after_zat"])
                if direction == "outbound":
                    checks.append("received-funds-spendable")
            if audit_path:
                checks.append("independent-canonical-block-membership")
            yield dict(id=f"roundtrip-{direction}-{role}-{action}", wallet=role, version="binary " + cfg[role + "_sha256"][:12],
                       interface=interface, implementation=implementation, variant=variant, network="zcash-regtest",
                       client_sha256=cfg[role + "_sha256"], method_scope="none", methods={}, status="pass",
                       fixture=(f"Regtest {recipe['genesis']} at {recipe['starting_height']}, schedule "
                                f"{recipe['activation_schedule_sha256']}, {recipe['node_image']}, {peer} {cfg[peer + '_sha256']}"),
                       action=f"{action.capitalize()} the {direction} payment {'to' if sending else 'from'} {peer.capitalize()}",
                       assertions=dict.fromkeys(checks, "pass"), accounting_zat=accounting,
                       evidence={"description": "Independent Regtest round trips / first receipt.json",
                                 "sha256": hashes[0], "related_sha256": sources})
