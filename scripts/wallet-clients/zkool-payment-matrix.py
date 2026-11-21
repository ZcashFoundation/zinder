#!/usr/bin/env python3
"""Two-party Regtest payments for Zkool (both address variants) and Cake.

Each case funds the wallet under test from its own disposable Regtest fixture,
pays an independently keyed ordinary Zallet, receives a signed payment back
with a memo, and spends that first incoming note back to Zallet. It then
reopens both wallets and re-checks balances, histories, and memos.

The fixture schedule is NU6.3 at height 250, matching both engines' pinned
Regtest parameters. Zallet is configured with the same schedule.

Every client executable, adapter, fixture readiness receipt, and fixture image
is pinned before the case runs and again afterwards. A completed case is only
reused when those pins and its recorded evidence still match; otherwise a fresh
case directory is required.

  campaign  start one owned fixture per case, run it, remove and verify it
  case      run one case against an already running owned fixture

No operator wallet, public chain, or shared checkout state is used.
"""
import argparse
import fcntl
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import tomllib
from contextlib import closing
from decimal import Decimal
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(HERE))

from roundtrip_fixture import (FULL_BLOCK_CAPS, GENESIS, NodeRpcError, anchor, http_json,  # noqa: E402
                               rpc)

from roundtrip_wallets import Zallet, pin_directory, require, write_private  # noqa: E402
from wallet_fixture import loopback  # noqa: E402

RELEASE = "v0.22.0"
NU63_HEIGHT = 250
TARGET_TIP = 260
FUNDING_HEIGHT = 261
OUTBOUND = 10_000_000
INBOUND = 6_000_000
SPENDBACK = 3_000_000
SWEEP_MARGIN = 1_000_000
MEMO_OUT = "zinder/zkool-cake-v1/outbound"
MEMO_SWEEP = "zinder/zkool-cake-v1/sweep"
MEMO_IN = "zinder/zkool-cake-v1/inbound"
MEMO_BACK = "zinder/zkool-cake-v1/spendback"
LEGS = ("outbound", "sweep", "inbound", "spendback")
TXID = re.compile(r"[0-9a-f]{64}")
HASH = re.compile(r"[0-9a-f]{64}")
BRANCHES = ("5ba81b19", "76b809bb", "2bb40e60", "f5b9230b", "e9ff75a6",
            "c2d6d0b4", "c8e71055", "4dec4df0", "5437f330", "37a5165b")
SERVICES = ("ingest", "projector", "query", "compat-lightwalletd")
PORT_OFFSETS = (2, 5, 6, 7, 10, 32, 67)
WALLET_PORT_OFFSET = 82
# The readiness receipt names every local port; the offsets are the fixture's
# own reservation, not a caller-supplied hint.
ENDPOINT_OFFSETS = {
    "native_wallet_query": 2,
    "query_ops": 6,
    "compat_ops": 7,
    "node_rpc": 32,
    "lightwalletd": 67,
}
CASE_STOP_GRACE = 10
CASES = ("zkool-internal", "zkool-external", "cake")
ZKOOL_CASES = {"zkool-internal": "internal-enabled", "zkool-external": "external-only"}
ZKOOL_SEEDS = {"zkool-internal": "zero", "zkool-external": "sevenf"}


UPSTREAM = {
    "zkool": {
        "version": "6.37.0",
        "executable_sha256": "98a03216d9975b4e1a485c1c2cf08ab9bc3a46e7b0716d83a2ad742ded74cafb",
        "driver_sha256": "ce7fa2df0c398796e3c661720fef02b68f41f62468e419bc111ea4f21016e14a",
        "repository": "https://github.com/hhanh00/zkool2",
        "source_commit": "d80892f163bc650e3b453f448da8c83eb5c32ddc",
        "workspace_lock_sha256": "9fdaffdaab1e6eabe0a78ffc0669349f05893054eb83d0f0abd3f2a49a05fd0c",
        "toolchain": "cargo +1.99.0 / rustc 1.99.0 (b940084d7 2026-09-28)",
        "build": "cargo +1.99.0 build --example zkool_payment",
    },
    "cake": {
        "version": "6.5.4-cb3e96f1",
        "executable_sha256": "13749e919020c9430792004287139480690918774cbc7fadfe594a658994a263",
        "repository": "https://github.com/cake-tech/zkool2",
        "source_commit": "cb3e96f10e58a8fc8632b40636a0c64aa21fb653",
        "workspace_lock_sha256": "e58b4de00913ddcb33e0d4af56936872140cafa4e97407babffddb379de0a9a2",
        "toolchain": "cargo +1.99.0 / rustc 1.99.0 (b940084d7 2026-09-28)",
        "build": "cargo +1.99.0 build --locked --bin zkool_graphql --no-default-features "
                 "--features graphql,bundled-sapling-params",
    },
    "zallet": {
        "repository": "https://github.com/zcash/zallet",
        "executable_sha256": "0851230a1971b6f3c12c9b42141b4d1c289384d78176dee67d2c87ed4a1d1508",
        "source_commit": "32488c5554e5e0275c3313b2fc06c00e6780746a",
        "source_lock_sha256": "bffcf554690cb9c24200e9bac9bb1668f63312d5af742ff6f7faaa51d09a332d",
        "toolchain": "cargo +1.95.0 launcher / cargo +1.100.0 zinder backend",
        "build": "cargo +1.100.0 build --manifest-path backends/zinder/Cargo.toml "
                 "--no-default-features --locked",
    },
}

LIMITS = [
    "Regtest only; the fixture is peer-isolated, has no default IPv4 route, and "
    "no public network or service is changed.",
    "The counterparty is ordinary Zallet, not Zingo: Zingo activates NU6.3 at "
    "height 1 on Regtest and cannot follow the NU6.3-at-250 fixture.",
    "Sender-side pending is not claimed. The harness requires node-mempool "
    "observation and records each wallet's own pending observation as an "
    "observation, not as a passing condition.",
    "Wallet-reported fees are the debit evidence; the harness does not "
    "reconstruct the fee from previous outputs.",
    "The Cake Flutter application, Dart bridge, mobile platforms, hardware "
    "signing, and the application's own keystore are not covered.",
    "Memos are fixed harness labels and carry no user or operator data.",
]


# -- small utilities --------------------------------------------------------
def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def fingerprint(text):
    return hashlib.sha256(text.encode()).hexdigest()


def tx_fingerprint(txid):
    require(isinstance(txid, str) and TXID.fullmatch(txid), "invalid transaction ID")
    return hashlib.sha256(bytes.fromhex(txid)).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as dest:
        json.dump(value, dest, indent=2, sort_keys=True)
        dest.write("\n")
        dest.flush()
        os.fsync(dest.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def main_harness():
    """Reuse the tested process, port, and owner-token lifecycle helpers."""
    name = "wallet_roundtrip_regtest"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / "run-wallet-roundtrip-regtest.py")
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    try:
        spec.loader.exec_module(loaded)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return loaded


def terminate_group(process, grace=10):
    main_harness().stop_group(process, signal.SIGTERM, grace)


def run_capture(command, timeout, env=None):
    """Run a command in its own session and bound both output and wall clock."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        terminate_group(process)
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        raise TimeoutError(f"{Path(command[0]).name} exceeded {timeout}s") from error
    except BaseException:
        terminate_group(process)
        raise
    return process.returncode, stdout, stderr


def execute(command, log, timeout):
    """Run an owned helper with a bounded wait and no orphan on timeout."""
    with log.open("x") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=True,
                                   env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            terminate_group(process)
            raise TimeoutError(f"{Path(command[1]).name} exceeded {timeout}s; see {log}") from error
        except BaseException:
            terminate_group(process)
            raise
        if process.returncode:
            raise RuntimeError(f"{Path(command[1]).name} stopped with exit "
                               f"{process.returncode}; see {log}")


def harness(trace_tool):
    return {path.name: digest(path) for path in (
        Path(__file__), HERE / "zkool-payment.py", HERE / "cake-payment.py",
        HERE / "zkool-payment.rs", HERE / "roundtrip_wallets.py",
        HERE / "roundtrip_fixture.py", HERE / "wallet_fixture.py", Path(trace_tool),
        SCRIPTS / "run-wallet-roundtrip-regtest.py", SCRIPTS / "regtest-fixture.py",
        SCRIPTS / "regtest-fixture-relay.py")}


def pin_snapshot(args, ready_path):
    return {
        "harness_sha256": harness(args.trace_tool),
        "executables_sha256": {
            "zkool": digest(args.zkool_bin),
            "cake": digest(args.cake_engine),
            "zallet": digest(args.zallet_backend),
        },
        "fixture_ready_sha256": digest(ready_path),
    }


def build_identities(pins):
    identities = {}
    for name, executable in pins["executables_sha256"].items():
        reference = UPSTREAM[name]
        if executable == reference["executable_sha256"]:
            identities[name] = reference
        else:
            identities[name] = {"executable_sha256": executable}
    return identities


def verify_release(tag, revision):
    require(re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?", tag),
            "expected a Zinder release tag")
    result = subprocess.run(["git", "-C", str(SCRIPTS.parent), "rev-parse", "--verify",
                             f"refs/tags/{tag}^{{commit}}"], capture_output=True, text=True, timeout=20)
    require(result.returncode == 0 and result.stdout.strip() == revision,
            "the selected local release tag must point to the requested revision")


def verify_pins(before, after):
    require(before == after, "client, adapter, or fixture ready pins changed during the run")
    require(set(before) == {"harness_sha256", "executables_sha256", "fixture_ready_sha256"},
            "unexpected pin shape")
    require(set(before["executables_sha256"]) == {"zkool", "cake", "zallet"},
            "unexpected client executable pin shape")
    require(all(HASH.fullmatch(value) for value in before["executables_sha256"].values()),
            "invalid client executable pin")
    require(all(HASH.fullmatch(value) for value in before["harness_sha256"].values()),
            "invalid harness pin")
    require(HASH.fullmatch(before["fixture_ready_sha256"]), "invalid fixture ready pin")
    return before


def derived_ports(bases):
    ports = []
    for base in bases:
        require(type(base) is int and 1024 <= base and base + WALLET_PORT_OFFSET <= 65535,
                "invalid port base")
        ports.extend(base + offset for offset in (*PORT_OFFSETS, WALLET_PORT_OFFSET))
    require(len(set(ports)) == len(ports), "fixture and wallet ports overlap")
    require(all(1 <= port <= 65535 for port in ports), "derived port is outside the TCP range")
    return ports


def check_ports(bases):
    ports = derived_ports(bases)
    for port in ports:
        with socket.socket() as candidate:
            candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                candidate.bind(("127.0.0.1", port))
            except OSError as error:
                raise ValueError(f"port {port} is unavailable") from error
    return ports


def verify_endpoints(ready, base=None):
    """Bind every receipt endpoint to this fixture's explicit loopback reservation."""
    endpoints = {}
    for name in ENDPOINT_OFFSETS:
        if name not in ready:
            raise ValueError(f"fixture receipt is missing {name}")
        url = loopback(ready[name])
        port = urlsplit(url).port
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError(f"fixture {name} port is outside the local service range")
        endpoints[name] = url
    ports = [urlsplit(url).port for url in endpoints.values()]
    if len(set(ports)) != len(ports):
        raise ValueError("fixture endpoints overlap")
    reservation = base if base is not None else ready.get("port_base")
    if base is not None and ready.get("port_base") not in (None, base):
        raise ValueError("fixture port base differs from the reservation")
    if reservation is not None:
        if not (type(reservation) is int and 1024 <= reservation
                and reservation + WALLET_PORT_OFFSET <= 65535):
            raise ValueError("fixture port base is invalid")
        derived_ports((reservation,))
        for name, offset in ENDPOINT_OFFSETS.items():
            if endpoints[name] != f"http://127.0.0.1:{reservation + offset}":
                raise ValueError(f"fixture {name} differs from the port reservation")
    return endpoints


# -- fixture verification ---------------------------------------------------
def verify_fixture(ready, args, base=None):
    """Check node, both Zinder wallet interfaces, images, schedule, and anchor."""
    endpoints = verify_endpoints(ready, base)
    import grpc
    import compact_formats_pb2 as compact
    import service_pb2 as compat
    from zinder.v1.wallet import wallet_pb2 as native

    revision = args.revision
    require(ready["network"] == "zcash-regtest" and ready["zinder_commit"] == revision,
            "fixture network or commit mismatch")
    require(ready.get("raw_blob_policy") == "all", "fixture must retain full blocks for Zallet")
    if base is not None:
        require(ready.get("port_base") == base, "fixture port base differs from the reservation")
    references = ready.get("image_references")
    require(isinstance(references, dict) and set(references) == set(SERVICES),
            "fixture image references are incomplete")
    version = args.release.lstrip("v")
    require(all(str(reference).rsplit(":", 1)[-1] == version for reference in references.values()),
            f"fixture images are not {args.release}")
    require(isinstance(ready.get("images"), dict) and set(ready["images"]) == set(SERVICES),
            "fixture image IDs are incomplete")
    require(isinstance(ready.get("node_image"), str) and ready["node_image"].startswith("sha256:"),
            "fixture node image identity is missing")
    if args.images is not None:
        require(json.loads(args.images.read_text()) == references,
                "fixture image references differ from the supplied image map")
        require(digest(args.images) == ready.get("image_manifest_sha256"),
                "fixture image manifest digest differs")
    if args.node_config is not None:
        require(digest(args.node_config) == ready.get("activation_schedule_sha256"),
                "fixture activation schedule differs from the chosen node config")

    height, block_hash = anchor(ready)
    node = endpoints["node_rpc"]
    require(rpc(node, "getblockhash", 0) == GENESIS["regtest"], "wrong node genesis hash")
    require(rpc(node, "getblockhash", height) == block_hash,
            "node anchor differs from the receipt")
    actual = {row["name"]: row["activationheight"]
              for row in rpc(node, "getblockchaininfo")["upgrades"].values()}
    expected = dict.fromkeys(("Overwinter", "Sapling", "Blossom", "Heartwood", "Canopy", "NU5",
                              "NU6", "NU6.1", "NU6.2"), 1)
    expected["NU6.3"] = NU63_HEIGHT
    require(actual == expected, "live fixture does not use the NU6.3-at-250 Regtest schedule")
    require(len(rpc(node, "getpeerinfo")) == 0 and
            ready["isolation"]["default_ipv4_route"] is False,
            "Regtest fixture is not isolated")
    for key, service in (("compat_ops", "zinder-compat-lightwalletd"),
                         ("query_ops", "zinder-query")):
        health = http_json(endpoints[key], "/healthz")
        require((health.get("service"), health.get("status"), health.get("network"),
                 health.get("git_commit")) == (service, "alive", "zcash-regtest", revision),
                f"{service} identity mismatch")

    def call(channel, package, method, message, response, streaming=False):
        invoke = channel.unary_stream if streaming else channel.unary_unary
        return invoke(f"/{package}/{method}", request_serializer=type(message).SerializeToString,
                      response_deserializer=response.FromString)(message, timeout=20)

    with grpc.insecure_channel(urlsplit(endpoints["lightwalletd"]).netloc) as channel:
        service = compat.DESCRIPTOR.services_by_name["CompactTxStreamer"].full_name
        info = call(channel, service, "GetLightdInfo", compat.Empty(), compat.LightdInfo)
        require(info.gitCommit == revision and info.chainName == "test",
                "wrong lightwalletd server")
        block = call(channel, service, "GetBlock", compat.BlockID(height=height),
                     compact.CompactBlock)
        require(block.height == height and block.hash[::-1].hex() == block_hash,
                "lightwalletd anchor mismatch")
        tip = call(channel, service, "GetLatestBlock", compat.ChainSpec(), compat.BlockID)
        require(tip.height >= height and
                rpc(node, "getblockhash", tip.height) == tip.hash[::-1].hex(),
                "serving tip is behind the anchor or on another chain")

    with grpc.insecure_channel(urlsplit(endpoints["native_wallet_query"]).netloc) as channel:
        service = native.DESCRIPTOR.services_by_name["WalletQuery"].full_name
        info = call(channel, service, "ServerInfo", native.ServerInfoRequest(),
                    native.ServerInfoResponse).info.common
        require((info.service_name, info.network, info.build_git_commit) ==
                ("zinder-query", "zcash-regtest", revision),
                "wrong native wallet-query server")
        require(FULL_BLOCK_CAPS <= set(info.capabilities),
                "native query lacks full-block capabilities")
        rows = call(channel, service, "FullBlocksInRange",
                    native.FullBlocksInRangeRequest(start_height=height, end_height=height),
                    native.FullBlocksInRangeChunk, streaming=True)
        first = next(rows, None)
        require(first is not None and next(rows, None) is None,
                "native query returned the wrong block count")
        full = first.full_block
        require(full.height == height and full.block_hash == block_hash and
                full.payload_bytes == bytes.fromhex(rpc(node, "getblock", block_hash, 0)),
                "native full block differs from the node at the anchor")
        visible = call(channel, service, "VisibleTipBlock", native.VisibleTipBlockRequest(),
                       native.VisibleTipBlockResponse).visible_tip_block
        require(visible.height >= height and
                rpc(node, "getblockhash", visible.height) == visible.block_hash,
                "native query tip is behind the anchor or on another chain")
    return {
        "network": "zcash-regtest",
        "revision": revision,
        "release": args.release,
        "anchor": {"height": height, "hash": block_hash},
        "activation_heights": actual,
        "activation_schedule_sha256": ready.get("activation_schedule_sha256"),
        "port_base": ready.get("port_base"),
        "peer_count": 0,
        "node_tip": {"height": tip.height, "hash": tip.hash[::-1].hex()},
        "native_tip": {"height": visible.height, "hash": visible.block_hash},
        "node_image": ready.get("node_image"),
        "image_references": references,
        "images": ready["images"],
        "image_manifest_sha256": ready.get("image_manifest_sha256"),
        "release_binaries": ready.get("binaries") or {},
        "raw_blob_policy": ready["raw_blob_policy"],
    }


# -- journal ---------------------------------------------------------------
class Journal:
    def __init__(self, path, config):
        self.path = path
        if path.exists():
            self.data = json.loads(path.read_text())
            require(self.data["config"] == config,
                    "state belongs to a different case, fixture, or pin set")
        else:
            self.data = {"schema_version": 1, "config": config, "operations": {},
                         "observations": {}, "mine_targets": {}}
            self.save()

    def save(self):
        write_json(self.path, self.data)

    def observe(self, key, value):
        self.data["observations"][key] = value
        self.save()
        return value


class ZalletPayment(Zallet):
    """Ordinary Zallet with the fixture's NU6.3-at-250 Regtest schedule."""

    def __init__(self, backend, directory, network, endpoint, birthday, rpc_port,
                 warmup_timeout=2400, command_timeout=60):
        binary = str(Path(backend).resolve())
        directory = Path(directory).resolve()
        config = directory / "zallet.toml"
        if not config.exists():
            pin_directory(directory, network, endpoint)
            import toml
            code, example, _ = run_capture(
                [binary, "--datadir=" + str(directory), "example-config", "-o", "-",
                 "--this-is-beta-code-and-you-will-need-to-recreate-the-example-later"],
                timeout=command_timeout)
            require(code == 0, "Zallet example-config failed")
            conf = tomllib.loads(example)
            conf["backend"] = "zinder"
            conf["zinder"] = {"wallet_query_endpoint": endpoint}
            conf["consensus"] = {"network": "test" if network == "testnet" else "regtest"}
            if network == "regtest":
                conf["consensus"]["regtest_nuparams"] = [
                    f"{branch}:{NU63_HEIGHT if branch == BRANCHES[-1] else 1}"
                    for branch in BRANCHES]
            conf["keystore"]["encryption_identity"] = str(directory / "identity.txt")
            conf["rpc"]["bind"] = [f"127.0.0.1:{rpc_port}"]
            conf["rpc"]["auth"] = [{"user": "roundtrip", "password": secrets.token_hex(32)}]
            write_private(config, toml.dumps(conf))
        super().__init__(backend, directory, network, endpoint, birthday, rpc_port,
                         warmup_timeout=warmup_timeout)

    def snapshot(self, target=None):
        # The native wallet query follows the tip itself; no target is needed.
        return super().snapshot()


class Case:
    def __init__(self, args, ready, journal, wallet, zallet):
        self.args, self.ready, self.journal = args, ready, journal
        self.wallet, self.zallet = wallet, zallet
        self.regtest = True

    def node(self, method, *params):
        return rpc(self.ready["node_rpc"], method, *params)

    def tip(self):
        return self.node("getblockcount")

    def mine(self, name, count):
        targets = self.journal.data["mine_targets"]
        tip = self.tip()
        if name not in targets:
            targets[name] = tip + count
            self.journal.save()
        if tip < targets[name]:
            self.node("generate", targets[name] - tip)
        return self.tip()

    def wait(self, label, inspect, timeout=None):
        end = time.monotonic() + (timeout or self.args.wait_seconds)
        while True:
            value = inspect()
            if value:
                return value
            require(time.monotonic() < end,
                    f"{label}: timed out; rerun with the same state directory")
            time.sleep(2)

    def mined(self, txid):
        def check():
            if txid in self.node("getrawmempool"):
                return None
            try:
                tx = self.node("getrawtransaction", txid, 1)
            except NodeRpcError as error:
                if error.code == -5:
                    return None
                raise
            if not tx.get("blockhash") or not tx.get("confirmations", 0):
                return None
            require(tx["txid"] == txid and
                    self.node("getblockhash", tx["height"]) == tx["blockhash"],
                    "transaction is outside the active chain")
            return {"height": tx["height"], "hash": tx["blockhash"]}
        return self.wait("canonical inclusion", check)

    def note_spent(self, wallet, incoming_txid, spending_txid, spending_height):
        """Prove the spending transaction consumed a note created by the incoming one."""
        if isinstance(wallet, CakePayment):
            spends = wallet.gql(
                "query($t:String!){transactionById(idAccount:1,txid:$t)"
                "{spends{id height pool value tx{id txid}}}}",
                {"t": spending_txid})["transactionById"]["spends"]
            rows = wallet.gql("{transactionsByAccount(idAccount:1){id txid}}")[
                "transactionsByAccount"]
            incoming = [row["id"] for row in rows if row["txid"] == incoming_txid]
            require(len(incoming) == 1,
                    "incoming transaction is missing from the Cake history")
            hits = [row for row in spends if row["tx"]["id"] == incoming[0]]
            return {"notes": len(hits),
                    "spent": [{"created_height": row["height"],
                               "value_zat": int(Decimal(row["value"]) * 100_000_000)}
                              for row in hits],
                    "spending_height": spending_height}
        db_uri = Path(wallet.database).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(db_uri, uri=True)) as connection:
            rows = connection.execute(
                "SELECT note.height, note.value, spend.height FROM notes AS note "
                "JOIN transactions AS incoming ON incoming.id_tx = note.tx "
                "JOIN spends AS spend ON spend.id_note = note.id_note "
                "JOIN transactions AS outgoing ON outgoing.id_tx = spend.tx "
                "WHERE incoming.txid = ? AND outgoing.txid = ? AND spend.height = ?",
                (bytes.fromhex(incoming_txid)[::-1], bytes.fromhex(spending_txid)[::-1],
                 spending_height)).fetchall()
        return {"notes": len(rows), "spending_height": spending_height,
                "spent": [{"created_height": height, "value_zat": value, "spend_height": spend}
                          for height, value, spend in rows]}

    def transfer(self, name, sender, receiver, address, amount, memo):
        observations = self.journal.data["observations"]
        if name in observations:
            # Completed and verified by an earlier invocation in this same
            # pinned state directory.
            return observations[name]
        before_key = name + "-before"
        if before_key not in observations:
            tip = self.tip()
            a, b = sender.snapshot(tip), receiver.snapshot(tip)
            require(a["spendable_zat"] >= amount, "insufficient spendable funds for " + name)
            if name == "outbound":
                require(b["total_zat"] == 0,
                        "expected a fresh recipient with no previous balance")
            observations[before_key] = {"payer": a["total_zat"], "payee": b["total_zat"],
                                        "payer_spendable": a["spendable_zat"],
                                        "height": tip}
            self.journal.save()
        before = observations[before_key]
        operations = self.journal.data["operations"]
        if name in operations:
            require("txid" in operations[name],
                    f"{name}: prior submission has no transaction ID; inspect the private wallet history")
            txid = operations[name]["txid"]
        else:
            operations[name] = {"intent": {"address": address, "amount_zat": amount,
                                           "memo": memo}}
            self.journal.save()
            txid = sender.send(address, amount, memo)
            require(isinstance(txid, str) and TXID.fullmatch(txid),
                    f"{name}: invalid transaction ID")
            operations[name]["txid"] = txid
            self.journal.save()
        if name + "-pending" not in observations:
            seen = {"node_mempool": txid in self.node("getrawmempool"),
                    "payer": False, "payee": False}
            end = time.monotonic() + 30
            while time.monotonic() < end:
                current = [self.wallet_tx(w.snapshot(self.tip()), txid)
                           for w in (sender, receiver)]
                seen["payer"] |= bool(current[0] and current[0]["pending"])
                seen["payee"] |= bool(current[1] and current[1]["pending"])
                if seen["node_mempool"] and seen["payer"] and seen["payee"]:
                    break
                time.sleep(2)
            require(seen["node_mempool"],
                    f"{name}: node did not observe the signed transaction")
            observations[name + "-pending"] = seen
            self.journal.save()
        self.mine(name, 10)
        block = self.mined(txid)

        def checked():
            tip = self.tip()
            a, b = sender.snapshot(tip), receiver.snapshot(tip)
            sent, received = self.wallet_tx(a, txid), self.wallet_tx(b, txid)
            if not all(t and not t["pending"] and t["height"] == block["height"]
                       for t in (sent, received)):
                return None
            fee = sent["fee_zat"]
            require(type(fee) is int and fee > 0,
                    f"{name}: sender lacks usable fee evidence")
            require(received["amount_zat"] == amount and memo in received["memos"],
                    f"{name}: recipient amount or decrypted memo mismatch")
            require(a["total_zat"] == before["payer"] - amount - fee,
                    f"{name}: payer debit does not reconcile")
            require(b["total_zat"] == before["payee"] + amount,
                    f"{name}: payee credit does not reconcile")
            return {"txid": txid, "block": block, "amount_zat": amount, "fee_zat": fee,
                    "memo": memo, "memo_verified": True, "payer_before_zat": before["payer"],
                    "payee_before_zat": before["payee"], "payer_after_zat": a["total_zat"],
                    "payee_after_zat": b["total_zat"],
                    "sender_spendable_zat": a["spendable_zat"],
                    "receiver_spendable_zat": b["spendable_zat"]}
        observations[name] = self.wait("wallet accounting for " + name, checked)
        self.journal.save()

    @staticmethod
    def wallet_tx(snapshot, txid):
        matches = [row for row in snapshot["transactions"]
                   if txid in (row["txid"], row.get("txid_alt"))]
        require(len(matches) <= 1, "wallet returned duplicate transaction entries")
        return matches[0] if matches else None

    def run(self):
        wallet, zallet = self.wallet, self.zallet
        address, counterpart = wallet.prepare(self.tip()), zallet.prepare()
        require(address != counterpart, "payer and payee are the same wallet")
        if "funding" not in self.journal.data["observations"]:
            require(self.tip() == TARGET_TIP, "funding requires the fresh fixture tip")
            self.node("generatetoaddress", 1, address)
            self.node("generate", 101)
            funded = self.wait("wallet funding and spendability", lambda: (
                lambda s: s if s["spendable_zat"] >= OUTBOUND + 200_000 else None)(
                    wallet.snapshot(self.tip())))
            self.journal.data["observations"]["funding"] = {
                "height": FUNDING_HEIGHT, "spendable_zat": funded["spendable_zat"],
                "pools": funded["pools"]}
            self.journal.data["mine_targets"]["funding"] = FUNDING_HEIGHT
            self.journal.save()
        funding = self.journal.data["observations"]["funding"]
        if funding["height"] != FUNDING_HEIGHT:
            raise RuntimeError("funding height differs from the pinned case recipe")
        self.transfer("outbound", wallet, zallet, counterpart, OUTBOUND, MEMO_OUT)
        observations = self.journal.data["observations"]
        if "sweep-amount" not in observations:
            # Drain the funding change so the spendback cannot avoid the first
            # incoming note. The residue is below SPENDBACK.
            held = wallet.snapshot(self.tip())["spendable_zat"]
            require(held > SWEEP_MARGIN, "wallet has no spendable funds to sweep")
            observations["sweep-amount"] = {"amount_zat": held - SWEEP_MARGIN,
                                            "held_zat": held}
            self.journal.save()
        sweep = observations["sweep-amount"]["amount_zat"]
        require(sweep > 0, "sweep amount is not positive")
        self.transfer("sweep", wallet, zallet, counterpart, sweep, MEMO_SWEEP)
        self.transfer("inbound", zallet, wallet, address, INBOUND, MEMO_IN)
        spendable = observations["inbound"]["receiver_spendable_zat"]
        require(spendable >= SPENDBACK, "first incoming note is not spendable")
        self.transfer("spendback", wallet, zallet, counterpart, SPENDBACK, MEMO_BACK)
        spent = self.note_spent(wallet, observations["inbound"]["txid"],
                                observations["spendback"]["txid"],
                                observations["spendback"]["block"]["height"])
        require(spent["notes"] >= 1 and all(
            note["created_height"] == observations["inbound"]["block"]["height"]
            and note["value_zat"] > 0 for note in spent["spent"]),
                "first incoming note was not spent by the spendback")
        observations["incoming-note-spent"] = spent
        self.journal.save()

        wallet.restart(self.tip())
        zallet.restart()
        tip = self.tip()
        a, b = wallet.snapshot(tip), zallet.snapshot(tip)
        observations = self.journal.data["observations"]
        require(a["total_zat"] == observations["spendback"]["payer_after_zat"],
                "wallet balance changed on reopen")
        require(b["total_zat"] == observations["spendback"]["payee_after_zat"],
                "counterpart balance changed on reopen")
        memo_exposure = {}
        for leg in LEGS:
            row = observations[leg]
            reopened = set()
            for holding in (a, b):
                tx = self.wallet_tx(holding, row["txid"])
                require(tx and tx["height"] == row["block"]["height"] and not tx["pending"],
                        "reopened wallet lost a confirmed transfer")
                reopened.update(tx["memos"])
            if leg in ("outbound", "sweep", "inbound"):
                require(row["memo"] in reopened,
                        "reopened wallets lost the decrypted memo")
            memo_exposure[leg] = row["memo"] in reopened
            require(self.node("getblockhash", row["block"]["height"]) == row["block"]["hash"],
                    "confirmed block changed during the run")
        evidence = wallet.endpoint_evidence()
        require(evidence["target_connected"], "wallet endpoint binding missing")
        require(zallet.endpoint == self.ready["native_wallet_query"],
                "counterpart endpoint differs from the fixture receipt")
        return {"reopen_ledger_verified": True, "restart_verified": True,
                "funding": funding,
                "wallet_after_reopen_zat": a["total_zat"],
                "counterpart_after_reopen_zat": b["total_zat"],
                "incoming_note_spent": spent, "restart_memo_exposure": memo_exposure,
                "endpoint_evidence": evidence,
                "counterpart_endpoint": zallet.endpoint,
                "counterpart_endpoint_evidence": {
                    "scope": "persisted-config", "endpoint": zallet.endpoint,
                    "config_sha256": digest(zallet.directory / "zallet.toml")}}


def wallet_for(args, ready, journal, birthday):
    network, endpoint = "regtest", ready["lightwalletd"]
    if args.case in ZKOOL_CASES:
        return ZkoolPayment(args.zkool_bin, journal.path.parent / "zkool", network, endpoint,
                            birthday, ZKOOL_CASES[args.case], args.trace_tool,
                            journal.path.parent / "evidence" / "zkool",
                            ZKOOL_SEEDS[args.case])
    return CakePayment(args.cake_engine, args.trace_tool, journal.path.parent / "cake", network,
                       endpoint, birthday, journal.path.parent / "evidence" / "cake",
                       use_internal=True)


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)
    return loaded


ZkoolPayment = module("zkool_payment", "zkool-payment.py").ZkoolPayment
CakePayment = module("cake_payment", "cake-payment.py").CakePayment


# -- actor observations -----------------------------------------------------
def actor_observations(journal):
    """Keep each actor's own pending observation without claiming sender pending."""
    legs = {}
    for leg in LEGS:
        pending = journal.data["observations"].get(leg + "-pending", {})
        require(pending.get("node_mempool") is True,
                f"{leg}: no node-mempool observation")
        if leg == "inbound":
            wallet_side, counterpart = pending.get("payee"), pending.get("payer")
        else:
            wallet_side, counterpart = pending.get("payer"), pending.get("payee")
        legs[leg] = {
            "node_mempool": True,
            "wallet_under_test_pending_observed": bool(wallet_side),
            "counterpart_pending_observed": bool(counterpart),
        }
    return {
        "pending_requirement": "node_mempool_only",
        "sender_pending_claimed": False,
        "legs": legs,
    }


def trace_evidence(state):
    rows = []
    for report in sorted((state / "evidence").glob("*/trace-*/report.json")):
        trace = report.parent
        values = json.loads(report.read_text())
        require(values.get("status") == "passed" and values.get("fallback_used") is False,
                "wallet endpoint attribution failed")
        connect = trace / "connect.log"
        require(connect.is_file(), "wallet trace is missing its connect log")
        rows.append({
            "report": str(report.relative_to(state)),
            "connect_log": str(connect.relative_to(state)),
            "report_sha256": digest(report),
            "connect_log_sha256": digest(connect),
        })
    require(rows, "no wallet endpoint traces were recorded")
    return rows


def shutdown_evidence(wallet, zallet):
    evidence = {"wallet_stopped": False, "counterpart_stopped": False, "errors": []}
    try:
        zallet.close()
    except Exception as error:
        evidence["errors"].append(f"counterpart: {error}")
    try:
        wallet.close()
    except Exception as error:
        evidence["errors"].append(f"wallet: {error}")
    identity = zallet._owned_identity()
    evidence["counterpart"] = {
        "scope": "daemon-identity",
        "processes_remaining": 0 if identity is None else 1,
        "pid": None if identity is None else identity[0],
    }
    evidence["counterpart_stopped"] = identity is None
    wallet_report = (wallet.shutdown_evidence() if hasattr(wallet, "shutdown_evidence")
                     else {"scope": "no-resident-process", "processes_remaining": 0})
    evidence["wallet"] = wallet_report
    evidence["wallet_stopped"] = wallet_report.get("processes_remaining") == 0
    evidence["verified"] = (not evidence["errors"] and evidence["wallet_stopped"] and
                            evidence["counterpart_stopped"])
    return evidence


# -- completed case reuse ---------------------------------------------------
def verify_completed_case(state, config, pins):
    """Reuse only a passed receipt whose pins and evidence still verify."""
    receipt_path = state / "receipt.json"
    require(receipt_path.is_file(), "completed case receipt is missing; use a fresh case")
    receipt = json.loads(receipt_path.read_text())
    require(receipt.get("schema_version") == 1 and receipt.get("status") == "passed",
            "completed case is not a passed schema-1 receipt; use a fresh case")
    require(receipt.get("case") == config["case"] and receipt.get("network") == "regtest",
            "completed case identity differs; use a fresh case")
    require(receipt.get("inputs") == config,
            "completed case input pins differ; use a fresh case")
    snapshot = receipt.get("pins") or {}
    verify_pins(pins, snapshot.get("pre"))
    verify_pins(pins, snapshot.get("post"))
    require(receipt.get("upstream") == build_identities(pins),
            "completed case build identities differ; use a fresh case")
    require(receipt.get("harness_sha256") == pins["harness_sha256"],
            "completed case harness pin differs; use a fresh case")
    require(receipt.get("binaries") == pins["executables_sha256"],
            "completed case client executable pins differ; use a fresh case")

    evidence = receipt.get("evidence") or {}
    require(evidence.get("preflight_sha256") == digest(state / "preflight.json"),
            "completed case preflight evidence changed; use a fresh case")
    require(evidence.get("journal_sha256") == digest(state / "run.json"),
            "completed case journal evidence changed; use a fresh case")
    cleanup_path = state / "cleanup.json"
    require(cleanup_path.is_file() and evidence.get("cleanup_sha256") == digest(cleanup_path),
            "completed case wallet cleanup evidence changed; use a fresh case")
    cleanup = json.loads(cleanup_path.read_text())
    require(cleanup.get("verified") is True,
            "completed case did not verify wallet shutdown; use a fresh case")
    counterpart = evidence.get("counterpart_config")
    require(isinstance(counterpart, dict) and counterpart.get("sha256") ==
            digest(state / "zallet" / "zallet.toml"),
            "completed case counterpart configuration changed; use a fresh case")
    traces = evidence.get("traces")
    require(isinstance(traces, list) and traces,
            "completed case has no verified endpoint traces; use a fresh case")
    for row in traces:
        report, connect = state / row["report"], state / row["connect_log"]
        require(report.is_file() and digest(report) == row["report_sha256"],
                "completed case trace report changed; use a fresh case")
        require(connect.is_file() and digest(connect) == row["connect_log_sha256"],
                "completed case trace log changed; use a fresh case")
    require(receipt.get("result", {}).get("endpoint_evidence", {}).get(
        "target_connected") is True, "completed case endpoint evidence is missing")
    return receipt


# -- one case ---------------------------------------------------------------
def run_case(args):
    ready_path = args.ready.resolve()
    ready = json.loads(ready_path.read_text())
    base = None
    if args.wallet_rpc_port is not None:
        base = ready.get("port_base")
        require(type(base) is int, "fixture receipt is missing its port base")
    verify_endpoints(ready, base)
    state = args.state.resolve()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(state.stat().st_mode & 0o077 == 0,
            "state directory must be private (mode 0700)")
    pins_pre = pin_snapshot(args, ready_path)
    config = {
        "case": args.case,
        "ready_sha256": pins_pre["fixture_ready_sha256"],
        "revision": args.revision,
        "release": args.release,
        "outbound_zat": OUTBOUND,
        "inbound_zat": INBOUND,
        "spendback_zat": SPENDBACK,
        "pins_sha256": fingerprint(json.dumps(pins_pre, sort_keys=True)),
    }
    with (state / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit("this case is already running") from error
        journal = Journal(state / "run.json", config)
        receipt = state / "receipt.json"
        if receipt.exists():
            verify_completed_case(state, config, pins_pre)
            print(receipt)
            return receipt
        preflight = verify_fixture(ready, args, base=base)
        require(preflight["node_tip"]["height"] >= TARGET_TIP,
                "fixture tip is below the case recipe")
        require(ready.get("binaries") in ({}, None),
                "case must run on release images, not overridden binaries")
        if args.wallet_rpc_port is not None:
            require(args.wallet_rpc_port == preflight["port_base"] + WALLET_PORT_OFFSET,
                    "wallet RPC port differs from the fixture port base")
        write_json(state / "preflight.json", preflight)
        birthday = max(1, preflight["node_tip"]["height"] - 10)
        wallet = zallet = None
        cleanup = {}
        original = None
        try:
            wallet = wallet_for(args, ready, journal, birthday)
            zallet = ZalletPayment(args.zallet_backend, state / "zallet", "regtest",
                                   ready["native_wallet_query"], birthday,
                                   args.wallet_rpc_port,
                                   warmup_timeout=args.wait_seconds)
            result = Case(args, ready, journal, wallet, zallet).run()
        except BaseException as error:
            original = error
            raise
        finally:
            if wallet is None:
                cleanup = {"verified": False, "errors": ["wallet was not constructed"]}
            elif zallet is None:
                try:
                    wallet.close()
                    report = (wallet.shutdown_evidence()
                              if hasattr(wallet, "shutdown_evidence")
                              else {"processes_remaining": 0})
                    cleanup = {"verified": report.get("processes_remaining") == 0,
                               "wallet_stopped": report.get("processes_remaining") == 0,
                               "wallet": report,
                               "errors": ["counterpart was not constructed"]}
                except Exception as error:
                    cleanup = {"verified": False,
                               "errors": [f"wallet: {error}",
                                          "counterpart was not constructed"]}
            else:
                cleanup = shutdown_evidence(wallet, zallet)
            write_json(state / "cleanup.json", cleanup)
            if not cleanup.get("verified"):
                message = ("wallet shutdown was not verified: " +
                           "; ".join(cleanup.get("errors") or ["unknown"]))
                if original is not None:
                    original.add_note(message)
                else:
                    raise RuntimeError(message)
        pins_post = pin_snapshot(args, ready_path)
        verify_pins(pins_pre, pins_post)
        evidence = wallet.endpoint_evidence()
        require(evidence["target_connected"], "wallet endpoint binding missing")
        result["endpoint_evidence"] = evidence
        result["counterpart_endpoint"] = zallet.endpoint
        result["counterpart_endpoint_evidence"] = {
            "scope": "persisted-config", "endpoint": zallet.endpoint,
            "config_sha256": digest(zallet.directory / "zallet.toml")}
        observations = journal.data["observations"]
        legs = {}
        for name in LEGS:
            row = observations[name]
            legs[name] = {**{k: v for k, v in row.items() if k not in ("txid", "block")},
                          "tx_fingerprint": tx_fingerprint(row["txid"]),
                          "mined_height": row["block"]["height"]}
        traces = trace_evidence(state)
        output = {
            "status": "passed", "schema_version": 1, "case": args.case,
            "network": "regtest",
            "release": {"tag": args.release, "revision": args.revision},
            "upstream": build_identities(pins_pre),
            "inputs": config,
            "pins": {"pre": pins_pre, "post": pins_post},
            "harness_sha256": pins_pre["harness_sha256"],
            "binaries": pins_pre["executables_sha256"],
            "preflight": preflight,
            "wallets": {"under_test": getattr(wallet, "variant",
                                              "cake-engine-internal-enabled"),
                        "counterpart": "ordinary-zallet"},
            "wallet_identity": fingerprint(getattr(wallet, "address", "")),
            "counterpart_identity": fingerprint(zallet.address),
            "legs": legs,
            "actor_observations": actor_observations(journal),
            "result": result,
            "evidence": {
                "preflight_sha256": digest(state / "preflight.json"),
                "journal_sha256": digest(state / "run.json"),
                "cleanup_sha256": digest(state / "cleanup.json"),
                "counterpart_config": {
                    "path": "zallet/zallet.toml",
                    "sha256": digest(zallet.directory / "zallet.toml"),
                },
                "traces": traces,
            },
            "cleanup": cleanup,
            "fixture_recipe": {
                "starting_height": preflight["node_tip"]["height"],
                "activation_schedule_sha256": preflight["activation_schedule_sha256"],
                "activation_heights": preflight["activation_heights"],
                "node_image": preflight["node_image"],
                "image_references": preflight["image_references"],
                "images": preflight["images"],
                "raw_blob_policy": preflight["raw_blob_policy"],
                "release_binaries": preflight["release_binaries"],
            },
            "limits": LIMITS,
        }
        write_json(receipt, output)
        print(receipt)
        return receipt


# -- campaign ---------------------------------------------------------------
def project_resources(project):
    count = 0
    for kind in ("container", "network", "volume"):
        flags = "-aq" if kind == "container" else "-q"
        result = subprocess.run(
            ["docker", kind, "ls", flags, "--filter",
             f"label=com.docker.compose.project={project}"],
            capture_output=True, text=True, timeout=30, check=False)
        require(result.returncode == 0, f"cannot inspect owned {kind} resources")
        count += len(result.stdout.split())
    return count


def relay_running(fixture):
    """Report only this fixture's exact relay: script, relay file, and owner token."""
    return main_harness().owned_relay(Path(fixture)) is not None


def _owned_case_identity(pid, root, executables, trace_tool):
    """Identify one live process whose executable and arguments prove case ownership."""
    proc = Path("/proc") / str(pid)
    try:
        if proc.stat().st_uid != os.geteuid():
            return None
        argv = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        if not argv:
            return None
        executable = (proc / "exe").resolve(strict=True)
        started = int((proc / "stat").read_bytes().rsplit(b")", 1)[1].split()[19])
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError, IndexError):
        return None
    is_trace = len(argv) > 1 and Path(os.fsdecode(argv[1])) == trace_tool
    if executable not in executables and not is_trace:
        return None
    for argument in argv:
        text = os.fsdecode(argument)
        if text.startswith("--") and "=" in text:
            text = text.split("=", 1)[1]
        if text.startswith("/"):
            path = Path(text)
            if path == root or root in path.parents:
                return pid, started
    return None


def case_processes(root, args):
    """Find only processes that reference this case state through an owned executable."""
    root = Path(root).resolve()
    executables = {Path(path).resolve() for path in
                   (args.zkool_bin, args.cake_engine, args.zallet_backend)}
    trace_tool = Path(args.trace_tool).resolve()
    found = {}
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return found
    for entry in entries:
        if entry.name.isdigit():
            identity = _owned_case_identity(int(entry.name), root, executables, trace_tool)
            if identity is not None:
                found[identity[0]] = identity
    return found


def close_case_processes(args, case, state, base, grace=CASE_STOP_GRACE):
    """Stop detached wallets and traces left by a hard-killed case; prove none remain."""
    root = Path(state) / case
    if not root.is_dir():
        return {"scope": "owned-case-processes", "found": 0, "stopped": 0,
                "remaining": 0, "errors": []}
    harness = main_harness()
    executables = {Path(path).resolve() for path in
                   (args.zkool_bin, args.cake_engine, args.zallet_backend)}
    trace_tool = Path(args.trace_tool).resolve()
    errors = []
    initial = case_processes(root, args)
    stopped = 0
    if base is not None:
        try:
            if harness.close_owned_wallet(Path(state) / case, base, args.zallet_backend, grace):
                stopped += 1
        except (Exception, KeyboardInterrupt) as error:
            errors.append(f"owned Zallet cleanup failed: {error}")
    for _ in range(4):
        identities = case_processes(root, args)
        if not identities:
            break
        for pid, identity in identities.items():
            try:
                harness.stop_owned_process(
                    identity, lambda pid=pid: _owned_case_identity(
                        pid, root, executables, trace_tool), min(grace, 10))
                stopped += 1
            except (Exception, KeyboardInterrupt) as error:
                errors.append(f"owned case process cleanup failed: {error}")
    remaining = len(case_processes(root, args))
    return {"scope": "owned-case-processes", "found": len(initial), "stopped": stopped,
            "remaining": remaining, "errors": errors}


class FixtureRunner:
    """Owned fixture lifecycle with bounded waits and measured cleanup."""

    def __init__(self, args, state):
        self.args, self.state = args, state

    def up(self, case, project, base):
        fixture = self.state / "projects" / project
        execute([sys.executable, "-B", str(SCRIPTS / "regtest-fixture.py"), "up",
                 "--state-root", str(self.state / "projects"), "--project", project,
                 "--port-base", str(base), "--images", str(self.args.images),
                 "--revision", self.args.revision,
                 "--node-config", str(self.args.node_config),
                 "--raw-blob-policy", "all", "--tip", str(TARGET_TIP)],
                self.state / f"{case}-up.log", timeout=self.args.fixture_timeout)
        require((fixture / "ready.initial.json").is_file(),
                f"{case}: fixture did not publish an immutable readiness receipt")

    def measure(self, case, project, retry=False):
        fixture = self.state / "projects" / project
        row = {"fixture_started": (fixture / "state.json").is_file(),
               "down_error": None, "project_resources_remaining": None,
               "relay_running": None, "fixture_removed": False,
               "retry": retry}
        log = self.state / f"{case}-down{'-retry' if retry else ''}.log"
        try:
            if row["fixture_started"]:
                execute([sys.executable, "-B", str(SCRIPTS / "regtest-fixture.py"), "down",
                         "--state-root", str(self.state / "projects"),
                         "--project", project], log, timeout=self.args.fixture_timeout)
        except BaseException as error:
            row["down_error"] = f"{type(error).__name__}: {error}"
        try:
            if retry:
                main_harness().stop_owned_relay(fixture, CASE_STOP_GRACE)
            row["project_resources_remaining"] = project_resources(project)
            row["relay_running"] = relay_running(fixture)
        except BaseException as error:
            row["measure_error"] = f"{type(error).__name__}: {error}"
        row["fixture_removed"] = (row["down_error"] is None and row["project_resources_remaining"] == 0 and
                                  row["relay_running"] is False)
        if row["fixture_removed"]:
            (fixture / "state.json").unlink(missing_ok=True)
        return row


def case_command(args, case, ready, case_state, base):
    return [sys.executable, "-B", str(Path(__file__)), "case", "--case", case,
            "--ready", str(ready), "--revision", args.revision, "--release", args.release,
            "--state", str(case_state), "--trace-tool", str(args.trace_tool),
            "--zkool-bin", str(args.zkool_bin), "--cake-engine", str(args.cake_engine),
            "--zallet-backend", str(args.zallet_backend),
            "--wallet-rpc-port", str(base + WALLET_PORT_OFFSET),
            "--wait-seconds", str(args.wait_seconds), "--images", str(args.images),
            "--node-config", str(args.node_config)]


def default_case_runner(args, case, ready, case_state, base):
    execute(case_command(args, case, ready, case_state, base),
            args.state / f"{case}-case.log", timeout=args.case_timeout)


def privacy_safe_outcome(receipt):
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
        "legs": {name: {key: value for key, value in receipt["legs"][name].items()
                        if key in ("tx_fingerprint", "mined_height", "amount_zat",
                                   "fee_zat", "memo_verified", "payer_before_zat",
                                   "payee_before_zat", "payer_after_zat",
                                   "payee_after_zat")}
                 for name in LEGS},
        "actor_observations": receipt["actor_observations"],
    }


def privacy_safe_provenance(receipt):
    preflight = receipt["preflight"]
    return {
        "release": receipt["release"],
        "upstream": receipt["upstream"],
        "fixture": {
            "network": preflight["network"],
            "anchor": preflight["anchor"],
            "starting_height": preflight["node_tip"]["height"],
            "node_image": preflight["node_image"],
            "image_references": preflight["image_references"],
            "images": preflight["images"],
            "image_manifest_sha256": preflight["image_manifest_sha256"],
            "activation_schedule_sha256": preflight["activation_schedule_sha256"],
            "raw_blob_policy": preflight["raw_blob_policy"],
        },
        "wallets": receipt["wallets"],
        "wallet_identity_fingerprint": receipt["wallet_identity"],
        "counterpart_identity_fingerprint": receipt["counterpart_identity"],
        "pins": receipt["pins"],
        "harness_sha256": receipt["harness_sha256"],
        "binaries": receipt["binaries"],
    }


def build_summary(args, bases, results, failures, cleanup):
    cases = {}
    for case in args.cases:
        row = {"status": "failed", "cleanup": cleanup.get(case, {}),
               "failure": failures.get(case)}
        survivors = cleanup.get(case, {}).get("wallet_survivors") or {}
        if (case in results and cleanup.get(case, {}).get("fixture_removed") is True
                and not survivors.get("found") and not survivors.get("remaining")):
            receipt_path = args.state / case / "receipt.json"
            row = {
                "status": "passed",
                "receipt": f"{case}/receipt.json",
                "receipt_sha256": digest(receipt_path),
                "case_cleanup": f"{case}/cleanup.json",
                "case_cleanup_sha256": digest(args.state / case / "cleanup.json"),
                "cleanup": cleanup[case],
                "outcome": privacy_safe_outcome(results[case]),
                "provenance": privacy_safe_provenance(results[case]),
            }
        cases[case] = row
    passed = bool(cases) and all(row["status"] == "passed" for row in cases.values())
    passed = passed and all(row["cleanup"].get("fixture_removed") is True
                            for row in cases.values())
    return {
        "schema_version": 1,
        "kind": "zkool-cake-payment-campaign",
        "status": "passed" if passed and not failures else "failed",
        "release": {"tag": args.release, "revision": args.revision},
        "port_bases": bases,
        "cases": cases,
        "cleanup": cleanup,
        "failures": failures,
        "limits": LIMITS,
    }


def evidence_index(state):
    patterns = ["cleanup.json", "projects/*/ready.initial.json", "projects/*/failure.log"]
    for case in CASES:
        patterns.extend(f"{case}-{stage}.log" for stage in ("up", "case", "down", "down-retry"))
        patterns.extend(f"{case}/{name}.json" for name in ("preflight", "run", "receipt", "cleanup"))
        adapter = "cake" if case == "cake" else "zkool"
        patterns.extend(f"{case}/evidence/{adapter}/trace-*/{name}"
                        for name in ("report.json", "connect.log"))
    rows = []
    for path in sorted(path for pattern in patterns for path in state.glob(pattern)):
        require(path.resolve() == path.absolute() and path.is_file(),
                "indexed evidence must be a regular file in the campaign directory")
        rows.append({"path": str(path.relative_to(state)), "bytes": path.stat().st_size,
                     "sha256": digest(path)})
    require(rows, "campaign produced no durable evidence")
    return rows


def run_campaign(args, fixture_factory=FixtureRunner, case_runner=default_case_runner):
    state = args.state.resolve()
    state.mkdir(mode=0o700, parents=True, exist_ok=False)
    require(state.stat().st_mode & 0o077 == 0,
            "campaign directory must be private (mode 0700)")
    bases = dict(zip(CASES, args.port_bases, strict=True))
    require(len(set(args.port_bases)) == len(args.port_bases), "port bases must be distinct")
    check_ports(args.port_bases)
    fixture = fixture_factory(args, state)
    results, failures, cleanup = {}, {}, {}
    owned = []
    interrupted = False
    fatal = False
    for index, case in enumerate(args.cases):
        project = "zinder-fixture-payment-" + secrets.token_hex(6)
        fixture_dir = state / "projects" / project
        case_state = state / case
        owned.append((case, project, fixture_dir))
        try:
            fixture.up(case, project, bases[case])
            ready = fixture_dir / "ready.initial.json"
            case_runner(args, case, ready, case_state, bases[case])
            receipt = json.loads((case_state / "receipt.json").read_text())
            require(receipt.get("status") == "passed",
                    f"{case}: receipt did not pass")
            results[case] = receipt
        except KeyboardInterrupt as error:
            failures[case] = f"interrupted: {error}"
            interrupted = True
            fatal = True
        except BaseException as error:
            failures[case] = f"{type(error).__name__}: {error}"
        survivors = close_case_processes(args, case, state, bases[case])
        cleanup[case] = fixture.measure(case, project)
        cleanup[case]["wallet_survivors"] = survivors
        if survivors["found"] or survivors["remaining"] or survivors["errors"]:
            # A hard-killed case can outlive its own cleanup. A case with an
            # owned survivor is never published as passed.
            cleanup[case]["fixture_removed"] = False
            results.pop(case, None)
            failures[case] = (failures.get(case, "") + " wallet cleanup failed").strip()
            fatal = True
        if cleanup[case]["fixture_removed"] is not True:
            results.pop(case, None)
            failures[case] = (failures.get(case, "") + " cleanup failed").strip()
            fatal = True
        if fatal:
            for later in args.cases[index + 1:]:
                failures.setdefault(later, "not run after an interruption or cleanup failure")
                cleanup.setdefault(later, {"fixture_removed": True,
                                           "skipped": True,
                                           "project_resources_remaining": 0})
            break
    for case, project, fixture_dir in reversed(owned):
        if cleanup.get(case, {}).get("fixture_removed") is not True:
            previous = cleanup.get(case, {})
            retry_survivors = close_case_processes(args, case, state, bases[case])
            retry = fixture.measure(case, project, retry=True)
            retry["wallet_survivors"] = (previous.get("wallet_survivors")
                                         or retry_survivors)
            retry["wallet_survivors_retry"] = retry_survivors
            if (previous.get("wallet_survivors", {}).get("found")
                    or retry_survivors["found"] or retry_survivors["remaining"] or retry_survivors["errors"]):
                retry["fixture_removed"] = False
            retry["upgraded"] = False
            cleanup[case] = retry
            if retry["fixture_removed"]:
                # A deferred retry never upgrades a case: the case was not
                # counted while its fixture still existed.
                failures[case] = failures.get(case, "cleanup failed") + " (removed on retry)"
    write_json(state / "cleanup.json", cleanup)
    index = evidence_index(state)
    write_json(state / "evidence-index.json", {"schema_version": 1, "files": index,
                                               "count": len(index)})
    summary = build_summary(args, bases, results, failures, cleanup)
    summary["evidence"] = {
        "index": "evidence-index.json",
        "index_sha256": digest(state / "evidence-index.json"),
        "cleanup": "cleanup.json",
        "cleanup_sha256": digest(state / "cleanup.json"),
        "receipts": {case: digest(state / case / "receipt.json")
                     for case in results},
    }
    write_json(state / "summary.json", summary)
    if interrupted:
        raise KeyboardInterrupt("campaign interrupted; see summary.json")
    if summary["status"] != "passed":
        raise RuntimeError(json.dumps({"failures": failures,
                                       "cleanup": {case: row.get("fixture_removed")
                                                   for case, row in cleanup.items()}}))
    print(json.dumps({"status": summary["status"], "summary": str(state / "summary.json"),
                      "receipts": summary["evidence"]["receipts"]}, indent=2))
    return summary


# -- CLI --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    case_parser = commands.add_parser("case", help="run one case against an existing fixture")
    case_parser.add_argument("--case", choices=CASES, required=True)
    case_parser.add_argument("--ready", type=Path, required=True,
                             help="immutable fixture readiness receipt")
    case_parser.add_argument("--state", type=Path, required=True,
                             help="private case directory; reuse to resume")
    case_parser.add_argument("--wallet-rpc-port", type=int, required=True)
    case_parser.add_argument("--images", type=Path,
                             help="optional fixture image map to bind against the receipt")
    case_parser.add_argument("--node-config", type=Path,
                             help="optional NU6.3-at-250 Regtest node configuration to bind")
    campaign_parser = commands.add_parser(
        "campaign", help="run the three cases on owned fixtures and clean up")
    campaign_parser.add_argument("--state", type=Path, required=True,
                                 help="new private campaign directory")
    campaign_parser.add_argument("--images", type=Path, required=True,
                                 help="fixture image map")
    campaign_parser.add_argument("--node-config", type=Path, required=True,
                                 help="NU6.3-at-250 Regtest node configuration")
    campaign_parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    campaign_parser.add_argument("--port-bases", nargs=3, type=int,
                                 default=[50600, 50700, 50800],
                                 metavar=("ZKOOL_INTERNAL", "ZKOOL_EXTERNAL", "CAKE"))
    for target in (case_parser, campaign_parser):
        target.add_argument("--revision", required=True,
                            help="expected full Zinder release commit")
        target.add_argument("--release", default=RELEASE,
                            help="expected fixture image release tag")
        target.add_argument("--zkool-bin", type=Path, required=True,
                            help="pinned zkool_payment example")
        target.add_argument("--cake-engine", type=Path, required=True,
                            help="pinned zkool_graphql engine")
        target.add_argument("--zallet-backend", type=Path, required=True,
                            help="ordinary Zallet executable")
        target.add_argument("--trace-tool", type=Path, required=True,
                            help="Zinder trace-wallet-endpoint.py")
        target.add_argument("--wait-seconds", type=int, default=2400)
        target.add_argument("--case-timeout", type=int, default=7200)
        target.add_argument("--fixture-timeout", type=int, default=900)
    args = parser.parse_args()
    os.umask(0o077)
    require(re.fullmatch("[0-9a-f]{40}", args.revision), "invalid revision")
    verify_release(args.release, args.revision)
    require(args.wait_seconds > 0 and args.case_timeout > 0 and args.fixture_timeout > 0,
            "timeouts must be positive")
    for path in (args.zkool_bin, args.cake_engine, args.zallet_backend, args.trace_tool):
        require(Path(path).is_file(), f"missing client or harness file: {path}")
    selected = (args.case,) if args.action == "case" else args.cases
    if "cake" in selected:
        try:
            importlib.import_module("cryptography.hazmat.primitives.asymmetric.ec")
        except ImportError as error:
            raise ValueError("Cake requires the Python cryptography package for local GraphQL authentication") from error
    if args.action == "case":
        run_case(args)
    else:
        os.umask(0o077)
        run_campaign(args)


if __name__ == "__main__":
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt as error:
        sys.exit(f"zkool/cake two-party payment interrupted: {error}")
    except (OSError, RuntimeError, ValueError, TimeoutError) as error:
        sys.exit(f"zkool/cake two-party payment failed: {error}")
