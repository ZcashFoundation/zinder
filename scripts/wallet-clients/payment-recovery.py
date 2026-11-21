#!/usr/bin/env python3
"""Verify Zingo CLI and ordinary Zallet recovery from pending disconnects and shallow Regtest rollbacks.

Each case owns one disposable Regtest fixture. The disconnect case stops the wallets while a
signed transfer is pending, mines it while they are absent, and reconnects them. The rollback
case invalidates the block containing a signed transfer, clears the owned node's mempool so a
replacement block cannot contain it, and readmits the identical signed bytes at a different
height. Both cases verify the canonical block, exactly-once credit, debit, and fee, spendability,
and persistence across wallet process restarts.
"""
import argparse
from contextlib import ExitStack
from hashlib import sha256
from importlib import import_module
from ipaddress import ip_network
import json
import os
from pathlib import Path
import re
import runpy
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener

SCRIPTS = Path(__file__).resolve().parent.parent
CLIENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(CLIENTS))

from roundtrip_fixture import NodeRpcError, rpc as node_rpc
from roundtrip_wallets import Zallet, Zingo
from wallet_fixture import loopback

RUNNER = runpy.run_path(str(SCRIPTS / "run-wallet-roundtrip.py"))
Journal = RUNNER["Journal"]
RoundTrip = RUNNER["RoundTrip"]
wait_for = RUNNER["wait_for"]

# The round-trip campaign already owns bounded stages and ownership-checked cleanup. Reuse those
# helpers instead of inventing a second signal and readback protocol for supervised cases.
SUPERVISOR = runpy.run_path(str(SCRIPTS / "run-wallet-roundtrip-regtest.py"))
wait_for_owned_exit = SUPERVISOR["wait_for_owned_exit"]
stop_group = SUPERVISOR["stop_group"]
stop_owned_process = SUPERVISOR["stop_owned_process"]
execute_owned = SUPERVISOR["execute"]
close_owned_wallet = SUPERVISOR["close_owned_wallet"]
owned_relay = SUPERVISOR["owned_relay"]
stop_owned_relay = SUPERVISOR["stop_owned_relay"]

OUTBOUND = RUNNER["OUTBOUND"]
RETURN = RUNNER["RETURN"]
TXID = re.compile(r"[a-f0-9]{64}")
HASH = re.compile(r"[a-f0-9]{64}")
MEMO_OUTBOUND = "zinder-payment-recovery-v1/outbound"
MEMO_RETURN = "zinder-payment-recovery-v1/return"
NODE_CONFIG = SCRIPTS.parent / "deploy/zebra/regtest-nu6-at-one.toml"
CASES = ("disconnect", "rollback")
DEFAULT_PORT_BASES = {"disconnect": 50000, "rollback": 50100}
PORT_OFFSETS = (2, 5, 6, 7, 10, 32, 67, 82)
LIGHTWALLETD_SCAN_METHODS = {"GetBlock", "GetBlockRange", "GetTransaction", "FindBlocks", "GetTreeState"}
PROJECT_PREFIX = "zinder-fixture-payment-recovery-"
PROJECT = re.compile(r"zinder-fixture-payment-recovery-(?:disconnect|rollback|transport)-[a-f0-9]{12}")
DEFAULT_CASE_TIMEOUT = 7200
DEFAULT_CASE_GRACE = 30
CLEANUP_TIMEOUT = SUPERVISOR["CLEANUP_TIMEOUT"]
HARNESS_SOURCES = (
    Path(__file__).resolve(),
    SCRIPTS / "regtest-fixture.py",
    SCRIPTS / "regtest-fixture-relay.py",
    SCRIPTS / "run-wallet-roundtrip.py",
    SCRIPTS / "run-wallet-roundtrip-regtest.py",
    CLIENTS / "roundtrip_fixture.py",
    CLIENTS / "roundtrip_wallets.py",
    CLIENTS / "wallet_fixture.py",
    CLIENTS / "coverage_roundtrip.py",
    CLIENTS / "coverage_evidence.py",
    SCRIPTS.parent / "deploy/docker-compose.yml",
    NODE_CONFIG,
)
PROTO_MODULES = ("compact_formats_pb2", "service_pb2", "zinder.v1.wallet.wallet_pb2",
                 "zinder.v1.ops.server_info_pb2")


require = RUNNER["require"]
digest = RUNNER["digest"]
write_json = RUNNER["write_json"]


def file_pin(path, label):
    target = Path(path).resolve()
    require(target.is_file(), f"{label} is not a readable file")
    return digest(target)


def input_pins(wallets, sources):
    return {
        "wallets": {name: file_pin(path, name) for name, path in wallets},
        "harness": {Path(path).name: file_pin(path, Path(path).name) for path in sources},
    }


def project_name(case):
    require(case in (*CASES, "transport"), "unsupported recovery case")
    return f"{PROJECT_PREFIX}{case}-{secrets.token_hex(6)}"


def case_project(case, state_root, name=None):
    """Resolve one fresh fixture identity that the case and its supervisor both know."""
    name = project_name(case) if name is None else name
    require(isinstance(name, str) and PROJECT.fullmatch(name), "invalid fixture project name")
    require(name.startswith(f"{PROJECT_PREFIX}{case}-"),
            "fixture project belongs to a different recovery case")
    directory = Path(state_root) / name
    require(not directory.exists(), "fixture project state already exists; refusing to reuse it")
    return name, directory


def fixture_configuration(directory):
    """Hash the rendered Compose configuration and the files read by its services."""
    model = json.loads((directory / "compose.json").read_bytes())
    del model["name"]
    for section in ("services", "networks", "volumes"):
        for resource in model[section].values():
            resource["labels"].pop("io.zinder.fixture.owner")
    for network in model["networks"].values():
        for subnet in network["ipam"]["config"]:
            # The isolated fixture allocates a free IPv4 subnet for each run.
            network_range = ip_network(subnet["subnet"])
            subnet["subnet"] = f"IPv{network_range.version}/{network_range.prefixlen}"
    del model["services"]["zinder-projector"]["environment"]["ZINDER_PROJECTOR__BUILD_OWNER_HEX"]
    node_mounts = model["services"]["node"]["volumes"]
    node_config = directory / "zebra.toml"
    expected = f"{node_config}:/etc/zebrad/zebrad.toml:ro"
    require(expected in node_mounts, "missing fixture node configuration mount")
    node_mounts[node_mounts.index(expected)] = f"{digest(node_config)}:/etc/zebrad/zebrad.toml:ro"
    for service in model["services"].values():
        for mount in service.get("volumes", []):
            if not isinstance(mount, dict) or mount.get("type") != "bind":
                continue
            source = Path(mount["source"])
            if mount["target"] == "/etc/zinder/config.toml":
                mount["source"] = digest(source)
            elif source.parent in (directory / "tokens", directory / "binaries"):
                # Credentials are ephemeral. Executable bytes have separate release pins.
                mount["source"] = str(source.relative_to(directory))
            else:
                raise ValueError("unrecognized bind mount in recovery fixture")
    return sha256(json.dumps(model, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def project_resource_count(project):
    """Read back only this Compose project's containers, networks, and volumes."""
    require(isinstance(project, str) and project, "invalid project identity")
    count = 0
    for listing in (("ps", "-aq"), ("network", "ls", "-q"), ("volume", "ls", "-q")):
        output = subprocess.check_output(
            ["docker", *listing, "--filter", f"label=com.docker.compose.project={project}"],
            text=True, timeout=60)
        count += len(output.split())
    return count


def memo_evidence(memos, expected):
    require(isinstance(expected, str) and expected, "invalid expected fixture memo")
    require(isinstance(memos, (list, tuple)), "wallet did not report decrypted memos")
    require(sum(1 for memo in memos if memo == expected) == 1,
            "the expected fixture memo is not present exactly once")
    return {"expected_sha256": sha256(expected.encode()).hexdigest(), "expected_present": True}


def tx_fingerprint(txid):
    require(isinstance(txid, str) and TXID.fullmatch(txid), "invalid transaction ID")
    return sha256(bytes.fromhex(txid)).hexdigest()


def address_fingerprint(address):
    require(isinstance(address, str) and address, "invalid wallet address")
    return sha256(address.encode()).hexdigest()


def transaction_rows(snapshot, txid):
    return [row for row in snapshot["transactions"] if row["txid"] == txid]


def require_single_confirmed(snapshot, txid, height):
    rows = transaction_rows(snapshot, txid)
    require(len(rows) == 1, "wallet history does not contain exactly one transfer entry")
    row = rows[0]
    require(row["height"] == height and not row["pending"],
            "wallet transfer is not confirmed at the expected canonical height")
    return row


def assert_transfer_accounting(before_payer, after_payer, before_payee, after_payee, amount, fee):
    require(type(amount) is int and amount > 0, "invalid transfer amount")
    require(type(fee) is int and fee > 0, "sender lacks usable fee evidence")
    require(after_payer == before_payer - amount - fee, "payer debit and fee do not reconcile exactly once")
    require(after_payee == before_payee + amount, "payee credit does not reconcile exactly once")
    return {"amount_zat": amount, "fee_zat": fee, "payer_debit_zat": amount + fee,
            "payee_credit_zat": amount, "payer_after_zat": after_payer,
            "payee_after_zat": after_payee}


def canonical_summary(node, txid, expected_height=None):
    require(isinstance(txid, str) and TXID.fullmatch(txid), "invalid transaction ID")
    transaction = node("getrawtransaction", txid, 1)
    require(transaction.get("txid") == txid, "node transaction identity differs")
    height = transaction.get("height")
    block_hash = transaction.get("blockhash")
    require(type(height) is int and isinstance(block_hash, str) and HASH.fullmatch(block_hash),
            "transaction is not confirmed in the active chain")
    require(transaction.get("confirmations", 0) > 0, "transaction has no active-chain confirmation")
    if expected_height is not None:
        require(height == expected_height, "transaction height differs from expectation")
    require(node("getblockhash", height) == block_hash, "transaction block is not the active block")
    block = node("getblock", block_hash, 1)
    require(block.get("hash") == block_hash and block.get("height") == height, "block identity differs")
    require(block.get("tx", []).count(txid) == 1,
            "transaction membership in the canonical block is not exactly once")
    return {"height": height, "block_hash": block_hash, "confirmations": transaction["confirmations"]}


def assert_remine_identity(before, after, old_block_hash, new_block_hash):
    original_raw = before.get("raw_sha256")
    observed_raw = after.get("raw_sha256_after")
    require(isinstance(original_raw, str) and HASH.fullmatch(original_raw),
            "the original signed transaction digest is missing or invalid")
    require(isinstance(observed_raw, str) and HASH.fullmatch(observed_raw),
            "the independently observed remined transaction digest is missing or invalid")
    require(before["tx_fingerprint"] == after["tx_fingerprint"],
            "replacement is not the same signed transaction")
    require(old_block_hash != new_block_hash, "replacement reused the invalidated block hash")
    require(original_raw == observed_raw,
            "replacement signed bytes differ from the original transfer")
    return {"same_signed_transfer": True, "distinct_block_hash": True,
            "original_raw_sha256": original_raw, "observed_raw_sha256": observed_raw}


def derived_ports(bases, offsets=PORT_OFFSETS):
    require(all(type(base) is int and 1024 <= base <= 65535 - max(offsets) for base in bases),
            "choose nonprivileged port bases with all derived ports in the TCP range")
    ports = [base + offset for base in bases for offset in offsets]
    require(len(set(ports)) == len(ports), "fixture and wallet ports overlap")
    return ports


def check_ports(bases, offsets=PORT_OFFSETS):
    ports = derived_ports(bases, offsets)
    for port in ports:
        with socket.socket() as candidate:
            candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                candidate.bind(("127.0.0.1", port))
            except OSError as error:
                raise ValueError(f"port {port} is unavailable") from error
    return ports


def wallet_scan_delta(before, after):
    delta = {}
    for key in set(before) | set(after):
        change = after.get(key, 0) - before.get(key, 0)
        require(change >= 0, "compat wallet RPC counter reset")
        if change:
            delta[f"{key[0]}/{key[1]}"] = change
    scanned = {name: count for name, count in delta.items()
               if name.split("/", 1)[0] in LIGHTWALLETD_SCAN_METHODS}
    require(not scanned, f"a wallet scanned through lightwalletd during the offline window: {scanned}")
    return delta


def validate_receipt(receipt, case):
    require(receipt.get("schema_version") == 1 and receipt.get("case") == case,
            "unexpected recovery receipt identity")
    require(receipt.get("status") in {"passed", "failed"}, "unexpected recovery receipt status")
    require(receipt.get("network") == "zcash-regtest", "recovery receipt must describe Regtest")
    require(isinstance(receipt.get("assertions"), dict), "recovery receipt assertions must be an object")
    if receipt["status"] == "passed":
        require(receipt["assertions"], "a passed recovery receipt has no assertions")
        require(all(value is True for value in receipt["assertions"].values()),
                "a passed recovery receipt has an unproven assertion")
        require(receipt.get("cleanup", {}).get("project_resources_remaining") == 0,
                "a passed recovery receipt left owned Docker resources")
    return receipt


class Recovery:
    def __init__(self, case, args):
        self.case = case
        self.args = args
        self.output = args.output.resolve()
        self.private = self.output / "private"
        self.project = None
        self.project_dir = None
        self.ready = None
        self.node = None
        self.journal = None
        self.zingo = None
        self.zallet = None
        self.trip = None
        self.payer = None
        self.payee = None
        self.pins = None
        self.receipt = {
            "schema_version": 1,
            "case": case,
            "status": "failed",
            "network": "zcash-regtest",
            "assertions": {},
            "limits": [
                "Regtest only; the fixture is peer-isolated and no public network or service is changed.",
                "The Zingo CLI has no resident process, so its disconnect window is the interval in which it is not invoked; the Zallet daemon is stopped and its process identity is proven gone.",
                "Wallet-reported fees are the debit evidence; the harness does not reconstruct the fee from previous outputs.",
                "The rollback may restart the owned node after invalidation when the node readmits the transfer, so the replacement block can exclude it. That fixture control is not a production expectation.",
                "The wallet-serving indexer retains the last fully served branch until a longer replacement branch exists, so the wallets observe the replacement height rather than an intermediate unconfirmed tip.",
                "The shallow rollback stays inside the fixture reorg window and does not exercise deep reorgs or finalization.",
                "Memos are fixed harness labels and carry no user data.",
            ],
        }

        if getattr(args, "project", None):
            self.receipt["supervised_project"] = args.project

    def rpc(self, method, *params):
        require(self.node is not None, "fixture is not ready")
        return node_rpc(self.node, method, *params)

    def wait(self, label, inspect, timeout=120, interval=2):
        try:
            return wait_for(label, timeout, interval, inspect)
        except TimeoutError as error:
            raise RuntimeError(f"{label}: timed out; private evidence is preserved") from error

    def close_wallets(self):
        try:
            wallet = self.zallet
            if wallet is None:
                return
            identity = wallet._owned_identity()
            try:
                wallet.close()
            finally:
                if identity is not None:
                    pid = identity[0]

                    def read_identity():
                        return pid, wallet._process_start(pid)

                    if read_identity() == identity:
                        stop_owned_process(identity, read_identity, DEFAULT_CASE_GRACE)
                        require(read_identity() != identity, "owned Zallet daemon is still running")
                    if wallet.process is not None and wallet.process.pid == pid:
                        wallet.process.wait(timeout=DEFAULT_CASE_GRACE)
                        wallet.process = None
        finally:
            if self.zingo is not None:
                self.zingo.close()

    def start(self):
        require(self.private.is_dir(), "private evidence directory is missing")
        state_root = self.private / "projects"
        self.project, self.project_dir = case_project(
            self.case, state_root, getattr(self.args, "project", None))
        arguments = ["up", "--project", self.project, "--state-root", state_root,
                     "--port-base", self.args.port_base, "--images", self.args.images,
                     "--revision", self.args.revision, "--node-config", NODE_CONFIG,
                     "--raw-blob-policy", "all", "--tip", 150]
        if self.args.bin_dir:
            arguments.extend(["--bin-dir", self.args.bin_dir])
        run_fixture(arguments, self.private / "up.log", 600)
        ready_path = self.project_dir / "ready.initial.json"
        require(ready_path.is_file(), "fixture did not publish an immutable readiness receipt")
        shutil.copyfile(ready_path, self.private / "fixture-initial.json")
        os.chmod(self.private / "fixture-initial.json", 0o600)
        ready = json.loads((self.private / "fixture-initial.json").read_text())
        require(ready["network"] == "zcash-regtest" and ready["zinder_commit"] == self.args.revision,
                "fixture network or release differs from the request")
        require(ready["port_base"] == self.args.port_base and ready["raw_blob_policy"] == "all",
                "fixture port base or raw-blob policy differs from the request")
        require(ready["isolation"]["peer_count"] == 0 and ready["isolation"]["default_ipv4_route"] is False,
                "fixture is not peer-isolated")
        self.ready = ready
        self.node = ready["node_rpc"]
        self.receipt["fixture"] = {
            "project": self.project,
            "release_revision": ready["zinder_commit"],
            "ready_sha256": digest(self.private / "fixture-initial.json"),
            "node_image": ready["node_image"],
            "image_ids": ready["images"],
            "configuration_sha256": fixture_configuration(self.project_dir),
            "release_binary_sha256": {name: binary["sha256"] for name, binary in ready["binaries"].items()},
            "activation_schedule_sha256": ready["activation_schedule_sha256"],
            "raw_blob_policy": ready["raw_blob_policy"],
            "anchor": {"height": ready["current_tip"], "hash": ready["current_hash"]},
        }
        from roundtrip_fixture import verify
        preflight = verify(ready, "regtest", self.args.revision)
        write_json(self.private / "preflight.json", preflight)
        self.receipt["preflight"] = {
            "sha256": digest(self.private / "preflight.json"),
            "full_block_sha256": preflight["full_block_sha256"],
            "node_tip": preflight["node_tip"],
            "native_tip": preflight["native_tip"],
            "compat_tip": preflight["compat_tip"],
        }

    def stop(self):
        if self.project_dir is None or not (self.project_dir / "state.json").is_file():
            return {"fixture_removed": False, "reason": "no owned fixture started",
                    "project_resources_remaining": self.project_resources()}
        run_fixture(["down", "--project", self.project, "--state-root", self.private / "projects"],
                    self.private / "down.log", 180)
        remaining = self.project_resources()
        return {"fixture_removed": remaining == 0, "project_resources_remaining": remaining}

    def project_resources(self):
        if self.project is None:
            return 0
        return project_resource_count(self.project)

    def harness_sources(self):
        return (*HARNESS_SOURCES, *(Path(import_module(name).__file__) for name in PROTO_MODULES))

    def wallet_inputs(self):
        return (("zingo", self.args.zingo), ("zallet_backend", self.args.zallet_backend))

    def pin_inputs(self):
        return input_pins(self.wallet_inputs(), self.harness_sources())

    def verify_pins(self):
        require(self.pins is not None, "input pins were not recorded")
        current = input_pins(self.wallet_inputs(), self.harness_sources())
        self.receipt["pins"]["post"] = current
        if self.ready is not None:
            require(fixture_configuration(self.project_dir) == self.receipt["fixture"]["configuration_sha256"],
                    "effective fixture configuration changed during the run")
        require(current == self.pins, "wallet executable or harness source changed during the run")
        return current

    def prepare_wallets(self):
        anchor = self.receipt["fixture"]["anchor"]["height"]
        require(self.rpc("getblockcount") == anchor, "fixture tip changed before wallet initialization")
        config = {"network": "regtest", "case": self.case, "revision": self.args.revision,
                  "ready_sha256": digest(self.private / "fixture-initial.json"),
                  "zingo_sha256": digest(self.args.zingo), "zallet_sha256": digest(self.args.zallet_backend)}
        self.journal = Journal(self.private / "run.json", config)
        data = self.journal.data
        if "birthday_anchor" not in data:
            data["birthday_anchor"] = anchor
            data["birthday"] = max(1, anchor - 10)
            self.journal.save()
        birthday = data["birthday"]
        self.zingo = Zingo(self.args.zingo.resolve(), self.private / "zingo", "regtest",
                           self.ready["lightwalletd"], birthday)
        self.zallet = Zallet(self.args.zallet_backend.resolve(), self.private / "zallet", "regtest",
                             self.ready["native_wallet_query"], birthday,
                             self.args.port_base + 82, warmup_timeout=self.args.wait_seconds)
        self.trip = RoundTrip("regtest", self.ready, node_rpc, self.journal, self.zingo,
                              self.zallet, self.args.wait_seconds)
        self.payer = self.zingo.prepare()
        self.payee = self.zallet.prepare()
        require(self.payer != self.payee, "payer and payee are the same wallet")
        identities = [address_fingerprint(self.payer), address_fingerprint(self.payee)]
        if "wallets" in data:
            require(data["wallets"] == identities, "wallet identities changed")
        else:
            data["wallets"] = identities
            self.journal.save()
        first = self.wait_height("initial wallet scan", anchor)
        require(all(snapshot["height"] == anchor for snapshot in first.values()),
                "initial wallet height differs from the frozen fixture anchor")
        self.receipt["clients"] = {
            "zingo_sha256": config["zingo_sha256"],
            "zallet_sha256": config["zallet_sha256"],
            "payer": "zingo-cli",
            "payee": "zallet-zinder",
            "wallet_identity_sha256": identities,
        }
        self.receipt["initial_wallet_heights"] = {
            "zingo": first["zingo"]["height"], "zallet": first["zallet"]["height"],
        }

    def balances(self):
        return {"zingo": self.zingo.snapshot(), "zallet": self.zallet.snapshot()}

    def fund(self):
        self.trip.fund(self.zingo)
        funding = self.journal.data["observations"]["funding"]
        self.receipt["funding"] = {
            "tx_fingerprint": tx_fingerprint(funding["txid"]),
            "mined_height": funding["block"]["height"],
            "spendable_after_zat": funding["spendable_zat"],
            "source": "regtest-coinbase-and-shield",
        }

    def broadcast(self, name, sender, receiver, address, amount, memo):
        def reconcile():
            payer, payee = sender.snapshot(), receiver.snapshot()
            candidates = [row["txid"] for row in payee["transactions"]
                          if row["amount_zat"] == amount and memo in row["memos"]
                          and transaction_rows(payer, row["txid"])]
            require(len(candidates) <= 1, "ambiguous transaction reconciliation")
            return candidates[0] if candidates else None
        return self.journal.once(name, {"address": address, "amount_zat": amount, "memo": memo},
                                 lambda: sender.send(address, amount, memo), reconcile=reconcile)

    def observe_pending(self, txid, sender, receiver, label):
        seen = {"node_mempool": False, "payer": False, "payee": False}
        def inspect():
            seen["node_mempool"] = seen["node_mempool"] or txid in self.rpc("getrawmempool")
            for party, wallet in (("payer", sender), ("payee", receiver)):
                rows = transaction_rows(wallet.snapshot(), txid)
                require(len(rows) <= 1, "wallet returned duplicate transaction entries")
                seen[party] = seen[party] or bool(rows and rows[0]["pending"])
            return all(seen.values())
        self.wait(label + " pending observation", inspect, timeout=60, interval=2)
        return seen

    def raw_transaction_hex(self, txid, block_hash=None):
        raw = None
        try:
            raw = self.rpc("getrawtransaction", txid, 0)
        except NodeRpcError as error:
            if error.code != -5:
                raise
        if isinstance(raw, str) and re.fullmatch(r"[a-f0-9]+", raw):
            return raw
        if block_hash is None:
            try:
                transaction = self.rpc("getrawtransaction", txid, 1)
            except NodeRpcError:
                transaction = {}
            block_hash = transaction.get("blockhash")
        if block_hash:
            try:
                block = self.rpc("getblock", block_hash, 2)
            except NodeRpcError:
                block = None
            for candidate in (block or {}).get("tx", []):
                if isinstance(candidate, dict) and candidate.get("txid") == txid:
                    raw = candidate.get("hex")
                    if isinstance(raw, str) and re.fullmatch(r"[a-f0-9]+", raw):
                        return raw
        raise ValueError("node did not expose the signed transaction bytes")

    def capture_raw(self, txid, name, block_hash=None):
        raw = self.raw_transaction_hex(txid, block_hash)
        value = {"hex": raw, "sha256": sha256(bytes.fromhex(raw)).hexdigest()}
        self.journal.data.setdefault("raw_transactions", {})[name] = value
        self.journal.save()
        return value

    def raw_digest(self, txid, block_hash=None):
        try:
            raw = self.raw_transaction_hex(txid, block_hash)
        except ValueError:
            return None
        return sha256(bytes.fromhex(raw)).hexdigest()

    def transaction_active(self, txid):
        try:
            transaction = self.rpc("getrawtransaction", txid, 1)
        except NodeRpcError as error:
            if error.code == -5:
                return False
            raise
        return bool(transaction.get("blockhash")) and transaction.get("confirmations", 0) > 0

    def canonical(self, txid, height=None):
        return canonical_summary(self.rpc, txid, height)

    def wait_confirmed(self, txid, height, label, timeout=None):
        timeout = timeout or self.args.wait_seconds
        def inspect():
            zingo_snapshot, zallet_snapshot = self.zingo.snapshot(), self.zallet.snapshot()
            rows = [transaction_rows(zingo_snapshot, txid), transaction_rows(zallet_snapshot, txid)]
            if all(len(row) == 1 for row in rows) and all(
                    row[0]["height"] == height and not row[0]["pending"] for row in rows):
                return {"zingo": zingo_snapshot, "zallet": zallet_snapshot}
            return None
        return self.wait(label, inspect, timeout=timeout, interval=3)

    def wait_height(self, label, height):
        def inspect():
            zingo, zallet = self.zingo.snapshot(), self.zallet.snapshot()
            if all(isinstance(snapshot["height"], int) and snapshot["height"] >= height
                   for snapshot in (zingo, zallet)):
                return {"zingo": zingo, "zallet": zallet}
            return None
        return self.wait(label, inspect, timeout=self.args.wait_seconds, interval=3)

    def wait_spendable(self, wallet, amount, label):
        def inspect():
            snapshot = wallet.snapshot()
            return snapshot if snapshot["spendable_zat"] >= amount else None
        return self.wait(label, inspect, timeout=self.args.wait_seconds, interval=3)

    def wallet_surfaces(self):
        import grpc
        import service_pb2 as compat
        from zinder.v1.wallet import wallet_pb2 as native

        def call(channel, package, method, message, response):
            return channel.unary_unary(
                f"/{package}/{method}", request_serializer=type(message).SerializeToString,
                response_deserializer=response.FromString)(message, timeout=10)

        with grpc.insecure_channel(urlsplit(self.ready["lightwalletd"]).netloc) as channel:
            service = compat.DESCRIPTOR.services_by_name["CompactTxStreamer"].full_name
            tip = call(channel, service, "GetLatestBlock", compat.ChainSpec(), compat.BlockID)
            compat_tip = {"height": tip.height, "hash": tip.hash[::-1].hex()}
        with grpc.insecure_channel(urlsplit(self.ready["native_wallet_query"]).netloc) as channel:
            service = native.DESCRIPTOR.services_by_name["WalletQuery"].full_name
            tip = call(channel, service, "VisibleTipBlock", native.VisibleTipBlockRequest(),
                       native.VisibleTipBlockResponse).visible_tip_block
            native_tip = {"height": tip.height, "hash": tip.block_hash}
        return {"lightwalletd": compat_tip, "native": native_tip}

    def wait_surfaces(self, height, block_hash, label="wallet surfaces", timeout=240):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                surfaces = self.wallet_surfaces()
            except Exception as error:  # Transient gRPC or relay errors are expected during a rollback.
                last = f"{type(error).__name__}: {error}"
            else:
                if all(row["height"] == height and row["hash"] == block_hash
                       for row in surfaces.values()):
                    return surfaces
                last = surfaces
            time.sleep(2)
        raise RuntimeError(f"{label}: did not publish {height} {block_hash}; last={last}")

    def wait_tip(self, label):
        tip_height = self.rpc("getblockcount")
        tip_hash = self.rpc("getblockhash", tip_height)
        surfaces = self.wait_surfaces(tip_height, tip_hash, label)
        return {"height": tip_height, "hash": tip_hash, "surfaces": surfaces}

    def assert_outbound(self, before, confirmed, txid, height):
        zingo, zallet = confirmed["zingo"], confirmed["zallet"]
        sent = require_single_confirmed(zingo, txid, height)
        received = require_single_confirmed(zallet, txid, height)
        fee = sent["fee_zat"]
        require(received["amount_zat"] == OUTBOUND and MEMO_OUTBOUND in received["memos"],
                "recipient amount or decrypted memo differs from the outbound transfer")
        return assert_transfer_accounting(before["zingo"]["total_zat"], zingo["total_zat"],
                                          before["zallet"]["total_zat"], zallet["total_zat"],
                                          OUTBOUND, fee)

    def assert_return(self, before, confirmed, txid, height):
        zingo, zallet = confirmed["zingo"], confirmed["zallet"]
        received = require_single_confirmed(zingo, txid, height)
        sent = require_single_confirmed(zallet, txid, height)
        fee = sent["fee_zat"]
        require(received["amount_zat"] == RETURN and MEMO_RETURN in received["memos"],
                "recipient amount or decrypted memo differs from the return transfer")
        return assert_transfer_accounting(before["zallet"]["total_zat"], zallet["total_zat"],
                                          before["zingo"]["total_zat"], zingo["total_zat"],
                                          RETURN, fee)

    def settle_return(self, after_outbound, legs):
        return_txid = self.broadcast("return", self.zallet, self.zingo, self.payer, RETURN, MEMO_RETURN)
        pending = self.observe_pending(return_txid, self.zallet, self.zingo, "return")
        self.trip.mine_to("return", 10)
        inclusion = self.canonical(return_txid)
        surfaces = self.wait_tip("return surfaces")
        confirmed = self.wait_confirmed(return_txid, inclusion["height"], "return confirmation")
        accounting = self.assert_return(after_outbound, confirmed, return_txid, inclusion["height"])
        expected = accounting["payee_after_zat"]
        returned = self.wait_spendable(self.zingo, expected, "Zingo return spendability")
        require(returned["spendable_zat"] == expected,
                "Zingo spendable funds differ from its reconciled post-return balance")
        legs["return"] = (return_txid, inclusion)
        return {"tx_fingerprint": tx_fingerprint(return_txid), "mined_height": inclusion["height"],
                "block_hash": inclusion["block_hash"], "pending": pending,
                "surfaces": surfaces, "accounting": accounting,
                "spendable_after_zat": returned["spendable_zat"]}

    def persist(self, final, legs):
        self.zingo.restart()
        self.zallet.restart()
        reopened = self.balances()
        require(reopened["zingo"]["total_zat"] == final["zingo"]["total_zat"]
                and reopened["zallet"]["total_zat"] == final["zallet"]["total_zat"],
                "wallet balances changed on reopen")
        for name, (txid, inclusion) in legs.items():
            require_single_confirmed(reopened["zingo"], txid, inclusion["height"])
            require_single_confirmed(reopened["zallet"], txid, inclusion["height"])
            require(self.rpc("getblockhash", inclusion["height"]) == inclusion["block_hash"],
                    f"{name}: confirmed block changed during the run")
        return {"zingo_total_zat": reopened["zingo"]["total_zat"],
                "zallet_total_zat": reopened["zallet"]["total_zat"],
                "zingo_height": reopened["zingo"]["height"],
                "zallet_height": reopened["zallet"]["height"]}

    def lightwalletd_counts(self):
        url = loopback(self.ready["compat_ops"]) + "/metrics"
        with build_opener(ProxyHandler({})).open(url, timeout=10) as response:
            text = response.read().decode("utf-8")
        counts = {}
        for line in text.splitlines():
            if not line.startswith("zinder_wallet_rpc_requests_total{"):
                continue
            labels = dict(re.findall(r'(\w+)="([^"]*)"', line))
            if labels.get("surface") != "lightwalletd":
                continue
            counts[(labels.get("method"), labels.get("grpc_code"))] = int(float(line.rsplit(" ", 1)[1]))
        return counts

    def assert_no_wallet_scan(self, before, after, label):
        try:
            return wallet_scan_delta(before, after)
        except ValueError as error:
            raise ValueError(f"{label}: {error}") from error

    def restart_owned_node(self):
        operator = runpy.run_path(str(SCRIPTS / "regtest-fixture.py"))
        state = json.loads((self.project_dir / "state.json").read_text())
        with (self.private / "node-restart.log").open("a", encoding="utf-8") as log:
            subprocess.run(["docker", "restart", state["node_container"]],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        operator["start_relay"](self.project_dir, state, serving=True)

    def block_state(self, block_hash):
        try:
            header = self.rpc("getblockheader", block_hash)
        except NodeRpcError:
            return {"header_available": False}
        return {"header_available": True, "height": header.get("height"),
                "confirmations": header.get("confirmations")}

    def chain_tip_status(self, block_hash):
        try:
            tips = self.rpc("getchaintips")
        except NodeRpcError as error:
            if error.code == -32601:
                return "unsupported"
            raise
        for tip in tips:
            if tip.get("hash") == block_hash:
                return tip.get("status")
        return None

    def wait_mempool(self, txid, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if txid in self.rpc("getrawmempool"):
                return True
            time.sleep(1)
        return False

    def readmit(self, txid, raw):
        if self.wait_mempool(txid, 5):
            method = "node-reorg-readmission"
        else:
            self.rpc("sendrawtransaction", raw["hex"])
            require(self.wait_mempool(txid, 20),
                    "the identical signed transfer was not readmitted to the node mempool")
            method = "sendrawtransaction-same-bytes"
        mempool_raw = self.rpc("getrawtransaction", txid, 0)
        require(isinstance(mempool_raw, str)
                and sha256(bytes.fromhex(mempool_raw)).hexdigest() == raw["sha256"],
                "readmitted bytes differ from the original signed transfer")
        return {"method": method, "mempool_present": True, "raw_sha256": raw["sha256"]}

    def run_disconnect(self):
        self.start()
        self.prepare_wallets()
        self.fund()
        before = self.balances()
        txid = self.broadcast("outbound", self.zingo, self.zallet, self.payee, OUTBOUND, MEMO_OUTBOUND)
        pending = self.observe_pending(txid, self.zingo, self.zallet, "outbound")
        try:
            raw = self.capture_raw(txid, "outbound")
        except ValueError:
            raw = None
        pre_mining_tip = self.rpc("getblockcount")
        last_online = self.wait_height("last online wallet scan", pre_mining_tip)
        require(all(snapshot["height"] == pre_mining_tip for snapshot in last_online.values()),
                "last online wallet height differs from the pre-mining tip")
        counts_before = self.lightwalletd_counts()
        require(txid in self.rpc("getrawmempool"),
                "transfer left the node mempool before the disconnect window")
        self.zallet.close()
        require(self.zallet._owned_identity() is None,
                "Zallet daemon did not stop for the disconnect window")
        require(txid in self.rpc("getrawmempool"),
                "transfer left the node mempool after the wallets disconnected")
        disconnect = {
            "zingo": "not invoked; the CLI has no resident process",
            "zallet": "daemon stopped and its process identity was cleared",
            "mempool_before_mining": True,
            "last_online_heights": {"zingo": last_online["zingo"]["height"],
                                    "zallet": last_online["zallet"]["height"]},
        }
        self.receipt["disconnect"] = disconnect
        self.trip.mine_to("outbound-while-disconnected", 10)
        inclusion = self.canonical(txid, pre_mining_tip + 1)
        if raw is None:
            try:
                raw = self.capture_raw(txid, "outbound", inclusion["block_hash"])
            except ValueError:
                raw = {"hex": None, "sha256": None, "unavailable": True}
        surfaces = self.wait_tip("disconnected wallet surfaces")
        require(self.zallet._owned_identity() is None,
                "Zallet restarted during the disconnect window")
        require(self.rpc("getblockcount") >= inclusion["height"] + 9,
                "the transfer did not reach ten confirmations while the wallets were absent")
        counts_after = self.lightwalletd_counts()
        disconnect.update({
            "mined_while_disconnected": inclusion,
            "surfaces_while_disconnected": surfaces,
            "node_tip_while_disconnected": self.rpc("getblockcount"),
            "wallet_scan_delta": self.assert_no_wallet_scan(counts_before, counts_after, "disconnect"),
            "zallet_process_absent": True,
        })
        self.zingo.prepare()
        self.zallet.prepare()
        confirmed = self.wait_confirmed(txid, inclusion["height"], "outbound reconnect")
        accounting = self.assert_outbound(before, confirmed, txid, inclusion["height"])
        settled = self.wait_height("reconnect settled", inclusion["height"] + 9)
        spendable = self.wait_spendable(self.zallet, OUTBOUND, "Zallet outbound spendability")
        after_outbound = self.balances()
        legs = {"outbound": (txid, inclusion)}
        return_leg = self.settle_return(after_outbound, legs)
        final = self.balances()
        persistence = self.persist(final, legs)
        require(settled["zingo"]["height"] >= inclusion["height"] + 9,
                "Zingo did not rescan the blocks mined during its disconnect window")
        self.receipt.update({
            "status": "passed",
            "scenario": {
                "outbound": {"tx_fingerprint": tx_fingerprint(txid), "mined_height": inclusion["height"],
                             "block_hash": inclusion["block_hash"], "pending": pending,
                             "surfaces": surfaces, "accounting": accounting,
                             "spendable_after_zat": spendable["spendable_zat"]},
                "disconnect": disconnect,
                "reconnect": {"zingo_height": settled["zingo"]["height"],
                              "zallet_height": settled["zallet"]["height"],
                              "raw_sha256": raw.get("sha256")},
                "return": return_leg,
                "persistence": persistence,
            },
            "assertions": {
                "pending_observed_by_node_and_both_wallets": True,
                "wallets_disconnected_while_pending": True,
                "transfer_mined_and_canonical_while_disconnected": True,
                "no_wallet_scan_during_disconnect": True,
                "canonical_block_and_hash_verified": True,
                "exactly_once_credit": True,
                "exactly_once_debit": True,
                "fee_reconciles": True,
                "received_funds_spendable": True,
                "persisted_across_wallet_restart": True,
            },
        })

    def run_rollback(self):
        self.start()
        self.prepare_wallets()
        self.fund()
        before = self.balances()
        txid = self.broadcast("outbound", self.zingo, self.zallet, self.payee, OUTBOUND, MEMO_OUTBOUND)
        pending = self.observe_pending(txid, self.zingo, self.zallet, "outbound")
        try:
            raw = self.capture_raw(txid, "outbound")
        except ValueError:
            raw = None
        self.trip.mine_to("outbound-before-rollback", 2)
        pre = self.canonical(txid)
        if raw is None:
            raw = self.capture_raw(txid, "outbound", pre["block_hash"])
        require(isinstance(raw.get("hex"), str), "the rollback requires the original signed transaction bytes")
        pre_surfaces = self.wait_tip("pre-rollback surfaces")
        pre_confirmed = self.wait_confirmed(txid, pre["height"], "pre-rollback confirmation")
        pre_accounting = self.assert_outbound(before, pre_confirmed, txid, pre["height"])
        pre_ledger = self.balances()
        old_hash, old_height = pre["block_hash"], pre["height"]
        require(old_height == self.rpc("getblockcount") - 1,
                "expected the signed transfer one block below the pre-rollback tip")
        require(self.rpc("getblockhash", old_height) == old_hash,
                "invalidated block changed before the rollback")
        self.rpc("invalidateblock", old_hash)
        require(self.rpc("getblockcount") == old_height - 1,
                "invalidation did not return the node to the fork point")
        orphan = {"old_height": old_height, "old_block_hash": old_hash,
                  "old_block_state": self.block_state(old_hash),
                  "old_chain_tip_status": self.chain_tip_status(old_hash),
                  "tx_absent_from_active_chain": not self.transaction_active(txid)}
        require(orphan["tx_absent_from_active_chain"],
                "the signed transfer remained in the active chain after invalidation")
        auto_readded = txid in self.rpc("getrawmempool")
        orphan["automatically_readded_to_mempool"] = auto_readded
        if auto_readded:
            self.restart_owned_node()
            after_restart = self.rpc("getblockcount")
            orphan["node_tip_after_restart"] = after_restart
            if after_restart != old_height - 1:
                require(after_restart >= old_height,
                        "owned node restart did not preserve either branch")
                require(self.rpc("getblockhash", old_height) == old_hash,
                        "owned node restart did not preserve the invalidated block identity")
                self.rpc("invalidateblock", old_hash)
                require(self.rpc("getblockcount") == old_height - 1,
                        "re-invalidation did not return the node to the fork point")
            orphan["restart_used"] = True
        else:
            orphan["restart_used"] = False
        readded = txid in self.rpc("getrawmempool")
        orphan["readded_after_recovery_attempts"] = readded
        if readded:
            observed_raw = self.raw_digest(txid)
            require(isinstance(observed_raw, str) and observed_raw == raw["sha256"],
                    "readmitted bytes differ from the original signed transfer")
            readmission = {"method": "node-reorg-readmission", "mempool_present": True,
                           "original_raw_sha256": raw["sha256"],
                           "observed_raw_sha256": observed_raw}
            self.trip.mine_to("rollback-remine-same-height", 3)
            post = self.canonical(txid)
            require(post["height"] == old_height,
                    "the readmitted transfer was not remined at the invalidated height")
            empty_replacement = False
        else:
            self.trip.mine_to("rollback-empty-replacement", 1)
            empty_height = self.rpc("getblockcount")
            empty_hash = self.rpc("getblockhash", empty_height)
            empty_block = self.rpc("getblock", empty_hash, 1)
            require(empty_height == old_height and empty_hash != old_hash,
                    "the empty replacement did not replace the invalidated height")
            require(txid not in empty_block["tx"],
                    "the replacement block unexpectedly contained the orphaned transfer")
            require(not self.transaction_active(txid),
                    "the signed transfer remained active after the empty replacement")
            orphan.update({"empty_replacement_height": empty_height,
                           "empty_replacement_hash": empty_hash,
                           "empty_replacement_excludes_tx": True})
            readmission = self.readmit(txid, raw)
            self.trip.mine_to("rollback-remine", 2)
            post = self.canonical(txid)
            require(post["height"] == old_height + 1,
                    "the signed transfer was not remined at a different height")
            empty_replacement = True
        require(post["block_hash"] != old_hash,
                "the remined transfer reused the invalidated block hash")
        post_block = self.rpc("getblock", post["block_hash"], 1)
        require(post_block["tx"].count(txid) == 1,
                "the remined transfer is not included exactly once")
        remine = {"tx_fingerprint": tx_fingerprint(txid), "old_height": old_height,
                  "new_height": post["height"], "old_block_hash": old_hash,
                  "new_block_hash": post["block_hash"], "raw_sha256": raw["sha256"],
                  "raw_sha256_after": self.raw_digest(txid, post["block_hash"]),
                  "membership_count": post_block["tx"].count(txid),
                  "empty_replacement": empty_replacement,
                  "height_changed": post["height"] != old_height}
        assert_remine_identity({"tx_fingerprint": tx_fingerprint(txid), "raw_sha256": raw["sha256"]},
                               remine, old_hash, post["block_hash"])
        tip_height = self.rpc("getblockcount")
        tip_hash = self.rpc("getblockhash", tip_height)
        require(tip_height > old_height, "replacement branch did not become longer")
        post_surfaces = self.wait_surfaces(tip_height, tip_hash, "post-rollback surfaces")
        settled = self.wait_height("replacement branch settled", tip_height)
        post_confirmed = self.wait_confirmed(txid, post["height"], "post-rollback confirmation")
        post_accounting = self.assert_outbound(before, post_confirmed, txid, post["height"])
        post_ledger = self.balances()
        require(post_ledger["zingo"]["total_zat"] == pre_ledger["zingo"]["total_zat"]
                and post_ledger["zallet"]["total_zat"] == pre_ledger["zallet"]["total_zat"],
                "the rollback applied the signed transfer more than once")
        self.trip.mine_to("outbound-maturity", 10)
        mature = self.wait_spendable(self.zallet, OUTBOUND, "Zallet remine spendability")
        require(mature["spendable_zat"] >= OUTBOUND,
                "the remined transfer is not spendable in Zallet")
        legs = {"outbound": (txid, post)}
        return_leg = self.settle_return(post_ledger, legs)
        final = self.balances()
        persistence = self.persist(final, legs)
        require(settled["zingo"]["height"] >= tip_height,
                "Zingo did not follow the replacement branch")
        assertions = {
            "pending_observed_by_node_and_both_wallets": True,
            "old_block_removed_from_active_chain": True,
            "orphaned_transfer_absent_from_active_chain": True,
            "readmission_observed": True,
            "remine_same_signed_bytes": True,
            "replacement_block_contains_transfer_once": True,
            "canonical_block_and_hash_verified": True,
            "exactly_once_credit": True,
            "exactly_once_debit": True,
            "fee_reconciles": True,
            "received_funds_spendable": True,
            "persisted_across_wallet_restart": True,
        }
        if empty_replacement:
            assertions["empty_replacement_excludes_transfer"] = True
        if post["height"] != old_height:
            assertions["remine_at_different_height"] = True
        self.receipt.update({
            "status": "passed",
            "scenario": {
                "outbound": {"tx_fingerprint": tx_fingerprint(txid), "mined_height": old_height,
                             "block_hash": old_hash, "pending": pending,
                             "surfaces": pre_surfaces, "accounting": pre_accounting},
                "orphan": orphan,
                "readmission": readmission,
                "remine": remine,
                "replacement": {"tip_height": tip_height, "tip_hash": tip_hash,
                                "surfaces": post_surfaces,
                                "wallet_old_height": old_height,
                                "wallet_new_height": post["height"],
                                "wallet_observed_height_change": post["height"] != old_height},
                "accounting_after_remine": post_accounting,
                "accounting_equal_before_and_after": True,
                "maturity": {"spendable_after_zat": mature["spendable_zat"]},
                "return": return_leg,
                "persistence": persistence,
            },
            "assertions": assertions,
        })

    def execute(self, exercise):
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.private.mkdir(mode=0o700)
        try:
            self.pins = self.pin_inputs()
            self.receipt["pins"] = {"pre": self.pins}
            try:
                exercise()
            finally:
                _, interrupted = finish_before_cancellation(self.teardown)
                if interrupted is not None:
                    raise interrupted
            self.verify_pins()
            self.check_result()
        except BaseException as error:
            self.receipt.update(status="failed", error=f"{type(error).__name__}: see private/failure.log")
            (self.private / "failure.log").write_text(traceback.format_exc())
            raise
        finally:
            write_json(self.output / "receipt.json", self.receipt)

    def teardown(self):
        try:
            self.close_wallets()
        finally:
            self.record_teardown()

    def record_teardown(self):
        try:
            self.receipt["cleanup"] = self.stop()
        except Exception:
            self.receipt["cleanup"] = {"fixture_removed": False, "error": "see private/failure.log"}
            raise

    def check_result(self):
        validate_receipt(self.receipt, self.case)
        require(self.receipt["cleanup"]["fixture_removed"], "fixture cleanup incomplete")


def run_case(case, args):
    recovery = Recovery(case, args)
    recovery.execute(recovery.run_disconnect if case == "disconnect" else recovery.run_rollback)


def case_command(case, args):
    arguments = [case, "--output", str(args.output), "--port-base", str(args.port_base),
                 "--images", str(args.images), "--revision", args.revision,
                 "--zingo", str(args.zingo), "--zallet-backend", str(args.zallet_backend),
                 "--wait-seconds", str(args.wait_seconds)]
    if getattr(args, "project", None):
        arguments.extend(["--project", args.project])
    if args.bin_dir:
        arguments.extend(["--bin-dir", str(args.bin_dir)])
    return [sys.executable, "-B", str(Path(__file__).resolve()), *arguments]


class SupervisionInterrupted(RuntimeError):
    """Carry the failed outcome out of a case without losing the original interruption."""

    def __init__(self, original, outcome):
        super().__init__(f"campaign interrupted while supervising {outcome['case']}")
        self.original = original
        self.outcome = outcome


def finish_before_cancellation(action, *args):
    """Finish a bounded cleanup operation and return the first cancellation received."""
    interrupted = None

    def record(signum, _frame):
        nonlocal interrupted
        if interrupted is None:
            if signum == signal.SIGINT:
                interrupted = KeyboardInterrupt()
            else:
                interrupted = SystemExit("payment recovery interrupted")

    with ExitStack() as handlers:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous = signal.signal(signum, record)
            handlers.callback(signal.signal, signum, previous)
        result = action(*args)
    return result, interrupted


def run_fixture(arguments, log, timeout):
    command = [sys.executable, "-B", *(["-O"] * sys.flags.optimize),
               str(SCRIPTS / "regtest-fixture.py"), *map(str, arguments)]
    result = launch_bounded(command, log, timeout, DEFAULT_CASE_GRACE, new_session=False)
    require(not result["timeout"], f"fixture {arguments[0]} exceeded {timeout}s; see {log}")
    require(result["exit_code"] == 0, f"fixture {arguments[0]} failed; see {log}")


def launch_case(command, log, timeout, grace):
    return launch_bounded(command, log, timeout, grace, new_session=True)


def launch_bounded(command, log, timeout, grace, *, new_session):
    # Each fixture command has its own group but stays inside its case's session.
    with log.open("x", encoding="utf-8") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=new_session, process_group=None if new_session else 0)
        try:
            exit_code = wait_for_owned_exit(process, timeout, grace, whole_session=new_session)
        except subprocess.TimeoutExpired:
            _, interrupted = finish_before_cancellation(stop_group, process, signal.SIGTERM, grace, new_session)
            if interrupted is not None:
                raise interrupted
            return {"exit_code": process.returncode, "timeout": True}
        except BaseException:
            finish_before_cancellation(stop_group, process, signal.SIGINT, grace, new_session)
            raise
    return {"exit_code": exit_code, "timeout": False}


def recover_case(case, child, args):
    """Recover only the exact wallet, relay, and Compose project this case was launched with."""
    private = child.output / "private"
    fixture = private / "projects" / child.project
    recovery = {"wallet_stopped": False, "fixture_torn_down": False, "relay_stopped": False,
                "resources_remaining": None, "problems": [], "repaired": False}

    def attempt(label, action):
        try:
            return action()
        except Exception as error:  # A cleanup failure must never mask the case outcome.
            recovery["problems"].append(f"{label}: {type(error).__name__}: {error}")
            return None

    recovery["wallet_stopped"] = bool(attempt(
        "owned wallet cleanup",
        lambda: close_owned_wallet(private, child.port_base, args.zallet_backend, args.case_grace)))

    def teardown_fixture():
        if not (fixture / "state.json").is_file():
            return False
        if project_resource_count(child.project) == 0:
            return False
        execute_owned(SCRIPTS / "regtest-fixture.py",
                      ["down", "--state-root", private / "projects", "--project", child.project],
                      child.output / "recovery.log", CLEANUP_TIMEOUT, args.case_grace)
        return True

    recovery["fixture_torn_down"] = bool(attempt("fixture teardown", teardown_fixture))

    def teardown_relay():
        if owned_relay(fixture) is None:
            return False
        stop_owned_relay(fixture, args.case_grace)
        return True

    recovery["relay_stopped"] = bool(attempt("owned relay cleanup", teardown_relay))

    remaining = attempt("owned resource readback", lambda: project_resource_count(child.project))
    recovery["resources_remaining"] = remaining
    if remaining:
        recovery["problems"].append(f"owned Docker resources remain for {child.project}")
    recovery["repaired"] = any((recovery["wallet_stopped"], recovery["fixture_torn_down"],
                                recovery["relay_stopped"]))
    return recovery


def read_case_receipt(path):
    if not path.is_file():
        return None, "case did not publish a receipt"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (OSError, ValueError) as error:
        return None, f"case receipt is unreadable: {type(error).__name__}: {error}"


def supervision_result(case, exit_code, timeout, interrupted, receipt, receipt_error, recovery):
    if interrupted is not None:
        return "failed", "interrupted", f"{type(interrupted).__name__}: {interrupted}"
    if timeout:
        return "failed", "timeout", "case exceeded its supervision timeout"
    if exit_code != 0:
        return "failed", "failed", f"case stopped with exit {exit_code}"
    if receipt_error is not None:
        return "failed", "failed", receipt_error
    try:
        validate_receipt(receipt, case)
    except ValueError as error:
        return "failed", "failed", f"receipt contract failed: {error}"
    if receipt["status"] != "passed":
        return "failed", "failed", receipt.get("error") or "case receipt reports failure"
    if recovery["problems"] or recovery["resources_remaining"] != 0:
        return "failed", "failed", "case cleanup is incomplete; parent recovery was required"
    if recovery["repaired"]:
        return "failed", "failed", "case left owned resources that parent recovery removed"
    return "passed", "cooperative", None


def supervision_record(case, outcome):
    return {
        "schema_version": 1, "case": case, "status": outcome["status"],
        "project": outcome["project"],
        "supervision": {"kind": outcome["supervision"], "exit_code": outcome["exit_code"],
                        "case_timeout": outcome["case_timeout"],
                        "case_grace": outcome["case_grace"]},
        "receipt": {"status": outcome["receipt_status"],
                    "sha256": outcome["receipt_sha256"]},
        "cleanup": outcome["cleanup"], "error": outcome["error"],
    }


def supervision_failure_receipt(case, outcome):
    remaining = outcome["cleanup"]["resources_remaining"]
    return {
        "schema_version": 1, "case": case, "status": "failed", "network": "zcash-regtest",
        "source": "campaign-supervision", "assertions": {}, "project": outcome["project"],
        "error": outcome["error"] or "case did not publish a passed receipt",
        "supervision": {"kind": outcome["supervision"], "exit_code": outcome["exit_code"],
                        "case_timeout": outcome["case_timeout"],
                        "case_grace": outcome["case_grace"]},
        "cleanup": {"fixture_removed": remaining == 0,
                    "project_resources_remaining": remaining,
                    "recovered": outcome["cleanup"]["repaired"],
                    "problems": outcome["cleanup"]["problems"]},
        "limits": ["Supervision receipt only; the case did not publish wallet evidence."],
    }


def supervise_case(case, child, args, root):
    log = root / f"{case}.log"
    timeout = False
    interrupted = None
    launch_error = None
    exit_code = None
    try:
        launched = launch_case(case_command(case, child), log, args.case_timeout, args.case_grace)
        exit_code, timeout = launched["exit_code"], launched["timeout"]
    except (KeyboardInterrupt, SystemExit) as error:
        interrupted = error
        launch_error = f"{type(error).__name__}: {error}"
    except BaseException as error:
        launch_error = f"{type(error).__name__}: {error}"
    cleanup, during_cleanup = finish_before_cancellation(recover_case, case, child, args)
    if interrupted is None:
        interrupted = during_cleanup
    receipt_path = child.output / "receipt.json"
    receipt, receipt_error = read_case_receipt(receipt_path)
    status, kind, reason = supervision_result(case, exit_code, timeout, interrupted, receipt,
                                              receipt_error, cleanup)
    outcome = {
        "case": case,
        "project": child.project,
        "exit_code": exit_code,
        "status": status,
        "supervision": kind,
        "error": launch_error or reason,
        "case_timeout": args.case_timeout,
        "case_grace": args.case_grace,
        "receipt_sha256": digest(receipt_path) if receipt_path.is_file() else None,
        "receipt_status": receipt.get("status") if isinstance(receipt, dict) else None,
        "cleanup": cleanup,
        "log": log.name,
    }
    if status == "failed" and not receipt_path.exists():
        child.output.mkdir(mode=0o700, parents=True, exist_ok=True)
        write_json(receipt_path, supervision_failure_receipt(case, outcome))
        outcome["receipt_sha256"] = digest(receipt_path)
        outcome["receipt_status"] = "failed"
    child.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_json(child.output / "supervision.json", supervision_record(case, outcome))
    if interrupted is not None:
        raise SupervisionInterrupted(interrupted, outcome)
    return outcome


def campaign(args):
    require(args.case_timeout > 0 and args.case_grace > 0, "case timeout and grace must be positive")
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True)
    require(not any(root.iterdir()), "campaign output directory must be new and empty")
    bases = args.port_bases or [DEFAULT_PORT_BASES[case] for case in CASES]
    require(len(bases) == 2 and bases[0] != bases[1], "supply two distinct port bases")
    check_ports(bases)
    results = {}
    stopped = None
    deferred = None
    try:
        for case, base in zip(CASES, bases, strict=True):
            child = argparse.Namespace(**vars(args))
            child.output = root / case
            child.port_base = base
            child.project = project_name(case)
            require(not child.output.exists(), "case output already exists; refusing to reuse it")
            try:
                results[case] = supervise_case(case, child, args, root)
            except SupervisionInterrupted as interrupted:
                results[case] = interrupted.outcome
                stopped, deferred = case, interrupted.original
                break
            if results[case]["supervision"] in {"timeout", "interrupted"}:
                stopped = case
                break
    finally:
        for case in CASES:
            results.setdefault(case, {
                "case": case, "exit_code": None, "status": "skipped", "supervision": "not-run",
                "error": f"campaign stopped after {stopped}; this case was not launched",
                "cleanup": {"resources_remaining": None, "problems": [], "repaired": False},
            })
        summary = {"schema_version": 1, "release": args.revision,
                   "port_bases": dict(zip(CASES, bases)), "case_timeout": args.case_timeout,
                   "case_grace": args.case_grace, "cases": results}
        write_json(root / "summary.json", summary)
    if deferred is not None:
        raise deferred
    require(all(row["exit_code"] == 0 and row["status"] == "passed" for row in results.values()),
            "one or more payment recovery cases failed; see the named receipts and supervision records")
    print(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for case in CASES:
        command = commands.add_parser(case, help=f"run the {case} recovery case on one fresh fixture")
        command.add_argument("--output", type=Path, required=True, help="new private evidence directory")
        command.add_argument("--port-base", type=int, default=DEFAULT_PORT_BASES[case])
        command.add_argument("--wait-seconds", type=int, default=2400)
        command.add_argument("--images", type=Path, required=True)
        command.add_argument("--bin-dir", type=Path, required=True)
        command.add_argument("--revision", required=True)
        command.add_argument("--zingo", type=Path, required=True)
        command.add_argument("--zallet-backend", type=Path, required=True)
        command.add_argument("--project", help="supervisor-selected fixture project identity")
    campaign_parser = commands.add_parser("campaign", help="run both cases on separate fixtures in sequence")
    campaign_parser.add_argument("--output", type=Path, required=True)
    campaign_parser.add_argument("--port-bases", nargs=2, type=int,
                                 default=[DEFAULT_PORT_BASES[case] for case in CASES])
    campaign_parser.add_argument("--wait-seconds", type=int, default=2400)
    campaign_parser.add_argument("--case-timeout", type=int, default=DEFAULT_CASE_TIMEOUT,
                                 help="maximum seconds for one recovery case (default: %(default)s)")
    campaign_parser.add_argument("--case-grace", type=int, default=DEFAULT_CASE_GRACE,
                                 help="seconds to wait after each termination signal (default: %(default)s)")
    campaign_parser.add_argument("--images", type=Path, required=True)
    campaign_parser.add_argument("--bin-dir", type=Path, required=True)
    campaign_parser.add_argument("--revision", required=True)
    campaign_parser.add_argument("--zingo", type=Path, required=True)
    campaign_parser.add_argument("--zallet-backend", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit("payment recovery interrupted"))
    if args.action == "campaign":
        campaign(args)
        return
    require(re.fullmatch(r"[a-f0-9]{40}", args.revision) and args.wait_seconds > 0,
            "invalid revision or timeout")
    check_ports([args.port_base])
    run_case(args.action, args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        sys.exit(f"payment recovery failed: {error}")
