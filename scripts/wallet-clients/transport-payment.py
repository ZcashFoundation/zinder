#!/usr/bin/env python3
"""Prove one payment survives loss of the native wallet transport without restarting Zallet.

The runner starts one disposable Regtest fixture, puts a private loopback relay in front of the
fixture's native WalletQuery endpoint, and points the ordinary Zallet daemon at that relay. After
the node and both real wallets report a single signed transfer pending, it stops only the extra
relay, mines while that route is down, and restores the same loopback address. The same Zallet
process must then see exactly one confirmed transfer at the canonical height. A client restart is
a separate recovery control and never substitutes for uninterrupted reconnect.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

CLIENTS = Path(__file__).resolve().parent
SCRIPTS = CLIENTS.parent
ROUTE_OFFSET = 90
RELAY_SCRIPT = SCRIPTS / "regtest-fixture-relay.py"


def load_recovery():
    spec = importlib.util.spec_from_file_location("zinder_payment_recovery",
                                                  CLIENTS / "payment-recovery.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RECOVERY = load_recovery()
require = RECOVERY.require
digest = RECOVERY.digest
write_json = RECOVERY.write_json
tx_fingerprint = RECOVERY.tx_fingerprint
memo_evidence = RECOVERY.memo_evidence
OUTBOUND = RECOVERY.OUTBOUND
MEMO_OUTBOUND = RECOVERY.MEMO_OUTBOUND
transaction_rows = RECOVERY.transaction_rows


ROUTE_OFFSETS = (*RECOVERY.PORT_OFFSETS, ROUTE_OFFSET)


def reachable(port):
    try:
        with socket.create_connection(("127.0.0.1", port), 1):
            return True
    except OSError:
        return False


def scanned_through(height, target):
    return type(height) is int and type(target) is int and 0 <= target <= height


class Transport(RECOVERY.Recovery):
    def __init__(self, args):
        super().__init__("transport", args)
        self.direct_native = None
        self.route_port = args.port_base + ROUTE_OFFSET
        self.endpoint = f"http://127.0.0.1:{self.route_port}"
        self.route_path = self.private / "route.json"
        self.route_owner = "transport-" + secrets.token_hex(8)
        self.route_process = None
        self.receipt["limits"] = [
            "Regtest only; the fixture is peer-isolated and no public network or service is changed.",
            "Only the private loopback relay for the native WalletQuery endpoint is stopped. The node, the fixture relay, and the lightwalletd route stay up.",
            "The Zallet daemon is not stopped during the outage. Its owned process identity must be unchanged before and after the transfer is mined.",
            "A wallet restart after the outage is a separate recovery control and never marks uninterrupted-process reconnect passed.",
            "Wallet-reported fees are the debit evidence; the harness does not reconstruct the fee from previous outputs.",
            "Memos are fixed harness labels and carry no user data.",
        ]
        self.receipt["transport"] = {
            "endpoint": "private-loopback-native-relay",
            "port_base": args.port_base,
            "only_native_route": True,
        }

    def native_port(self):
        parts = urlsplit(self.direct_native)
        require(parts.scheme == "http" and parts.hostname == "127.0.0.1" and parts.port,
                "fixture native endpoint is not an explicit loopback URL")
        return parts.port

    def harness_sources(self):
        return (*super().harness_sources(), Path(__file__).resolve())

    def start_route(self):
        require(self.route_process is None, "private native relay is already running")
        route = [[self.route_port, "127.0.0.1", self.native_port()]]
        write_json(self.route_path, route)
        with (self.private / "route.log").open("ab") as log:
            self.route_process = subprocess.Popen(
                [sys.executable, "-B", str(RELAY_SCRIPT), str(self.route_path), self.route_owner],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            require(self.route_process.poll() is None,
                    "private native relay exited before accepting connections")
            if reachable(self.route_port):
                return {"endpoint": "same-loopback-native-relay", "target": "fixture-native-wallet-query"}
            time.sleep(0.1)
        raise TimeoutError("private native relay did not start listening")

    def stop_route(self):
        process = self.route_process
        require(process is not None, "no owned private native relay to stop")
        command = Path(f"/proc/{process.pid}/cmdline").read_bytes().split(b"\0")
        require(self.route_owner.encode() in command and str(RELAY_SCRIPT).encode() in command,
                "refusing to signal a changed private native relay")
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            self.route_process = None
            raise RuntimeError("private native relay ignored SIGTERM")
        self.route_process = None
        require(not reachable(self.route_port),
                "private native relay still accepts connections after it was stopped")
        return {"exit_signal": "SIGTERM", "exit_code": process.returncode}

    def fixture_relay(self):
        state = json.loads((self.project_dir / "state.json").read_text())
        pid, owner = state.get("relay_pid"), state.get("owner")
        require(type(pid) is int and pid > 0 and isinstance(owner, str) and owner,
                "fixture relay identity is missing")
        command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        require(owner.encode() in command and str(RELAY_SCRIPT).encode() in command,
                "fixture relay is no longer the owned process")
        return pid

    def prepare_wallets(self):
        direct = self.ready["native_wallet_query"]
        self.ready["native_wallet_query"] = self.endpoint
        try:
            super().prepare_wallets()
        finally:
            self.ready["native_wallet_query"] = direct
        self.receipt["clients"].update({
            "native_endpoint": "private-loopback-native-relay",
            "zallet_route": "stopped-and-restored-on-the-same-loopback-address",
        })

    def zallet_status(self, identity):
        require(self.zallet._owned_identity() == identity,
                "Zallet process identity changed while the native route was down")
        try:
            status = self.zallet._rpc("getwalletstatus")
        except Exception as error:
            return {"available": False, "error": type(error).__name__}
        return {"available": True, "fully_synced_height": status.get("fully_synced_height")}

    def wait_process_confirmed(self, txid, height, outage_tip, identity=None):
        """Wait for the decrypted payment and the wallet scan through the outage tip."""
        deadline = time.monotonic() + self.args.wait_seconds
        last = None
        while time.monotonic() < deadline:
            if identity is not None:
                require(self.zallet._owned_identity() == identity,
                        "Zallet process exited or changed during transport loss")
            try:
                snapshot = self.zallet._snapshot_once()
            except Exception as error:
                last = f"{type(error).__name__}: {error}"
            else:
                rows = transaction_rows(snapshot, txid)
                require(len(rows) <= 1, "Zallet returned duplicate transfer entries")
                if len(rows) == 1 and rows[0]["height"] == height and not rows[0]["pending"]:
                    if (scanned_through(snapshot["height"], outage_tip)
                            and rows[0]["amount_zat"] == OUTBOUND and MEMO_OUTBOUND in rows[0]["memos"]):
                        return snapshot
                    last = {"amount_zat": rows[0]["amount_zat"],
                            "expected_memo_present": MEMO_OUTBOUND in rows[0]["memos"],
                            "scanned_height": snapshot["height"], "outage_tip": outage_tip,
                            "height": rows[0]["height"]}
                else:
                    last = {"rows": len(rows)}
            time.sleep(3)
        raise TimeoutError(f"Zallet did not reconcile the transfer after reconnect; last={last}")

    def run_transport(self):
        self.start()
        self.direct_native = self.ready["native_wallet_query"]
        fixture_relay = self.fixture_relay()
        started = self.start_route()
        self.receipt["transport"]["started"] = started
        self.prepare_wallets()
        self.fund()
        before = self.balances()
        txid = self.broadcast("outbound", self.zingo, self.zallet, self.payee, OUTBOUND, MEMO_OUTBOUND)
        pending = self.observe_pending(txid, self.zingo, self.zallet, "outbound")
        try:
            raw = self.capture_raw(txid, "outbound")
        except ValueError:
            raw = {"sha256": None}
        last_online = self.balances()
        require(txid in self.rpc("getrawmempool"),
                "transfer left the node mempool before the transport outage")
        zallet_identity = self.zallet._owned_identity()
        require(zallet_identity is not None, "Zallet daemon identity is not owned")
        require(before["zallet"]["total_zat"] == last_online["zallet"]["total_zat"],
                "Zallet balance changed before the transport outage")
        killed = self.stop_route()
        require(not reachable(self.route_port), "native route is still reachable after the stop")
        require(reachable(self.native_port()),
                "the fixture's direct native endpoint was lost with the private relay")
        require(self.fixture_relay() == fixture_relay,
                "the fixture relay changed while only the private native relay was stopped")
        require(self.zallet._owned_identity() == zallet_identity,
                "Zallet process changed when the native route was stopped")
        down = {
            "relay": killed,
            "native_route_reachable": False,
            "direct_native_reachable": True,
            "fixture_relay_unchanged": True,
            "zallet_process_unchanged": True,
        }
        self.receipt["outage"] = down
        self.receipt["assertions"].update({
            "only_the_private_native_route_was_stopped": True,
            "node_and_other_wallet_routes_stayed_up": True,
        })
        self.trip.mine_to("outbound-while-native-transport-down", 10)
        inclusion = self.canonical(txid)
        surfaces = self.wait_tip("wallet surfaces while native transport down")
        status_down = self.zallet_status(zallet_identity)
        require(self.fixture_relay() == fixture_relay,
                "the fixture relay changed while the native route was down")
        require(not reachable(self.route_port), "native route recovered before it was restored")
        require(reachable(self.native_port()),
                "the direct native endpoint stopped serving while the route was down")
        require(self.zallet._owned_identity() == zallet_identity,
                "Zallet process exited while the transfer was mined")
        down.update({
            "inclusion": inclusion,
            "surfaces": surfaces,
            "node_tip": self.rpc("getblockcount"),
            "zallet_status_while_down": status_down,
            "transfer_in_mempool_after_mining": txid in self.rpc("getrawmempool"),
        })
        restored = self.start_route()
        require(reachable(self.route_port), "restored native route does not accept connections")
        require(self.zallet._owned_identity() == zallet_identity,
                "Zallet process exited before reconnecting")
        self.receipt["transport"]["restored"] = restored
        try:
            same_process = self.wait_process_confirmed(
                txid, inclusion["height"], down["node_tip"], identity=zallet_identity)
        except Exception as error:
            changed = self.zallet._owned_identity() != zallet_identity
            self.receipt["same_process"] = {
                "reconnected": False,
                "process_exited_or_changed": changed,
                "error": f"{type(error).__name__}: {error}",
            }
            self.receipt["assertions"]["same_process_reconnected_without_restart"] = False
            self.receipt["restart_control"] = self.restart_control(txid, inclusion["height"], down["node_tip"])
            raise
        require(self.zallet._owned_identity() == zallet_identity,
                "Zallet process changed before accounting")
        confirmed = self.wait_confirmed(txid, inclusion["height"], "both wallets after route restore")
        require(self.zallet._owned_identity() == zallet_identity,
                "Zallet process changed after the same process confirmed the transfer")
        sent = transaction_rows(confirmed["zingo"], txid)
        self.receipt["assertions"]["same_process_reconnected_without_restart"] = True
        self.receipt["same_process"] = {"reconnected": True, "process_identical": True}
        received = transaction_rows(same_process, txid)
        require(len(sent) == 1 and len(received) == 1,
                "wallet history does not contain exactly one transfer entry")
        require(received[0]["amount_zat"] == OUTBOUND and MEMO_OUTBOUND in received[0]["memos"],
                "reconnected Zallet amount or decrypted memo differs from the transfer")
        accounting = self.assert_outbound(before, {"zingo": confirmed["zingo"], "zallet": same_process},
                                          txid, inclusion["height"])
        spendable = self.wait_spendable(self.zallet, OUTBOUND, "Zallet spendability after reconnect")
        after_outbound = self.balances()
        legs = {"outbound": (txid, inclusion)}
        return_leg = self.settle_return(after_outbound, legs)
        final = self.balances()
        persistence = self.persist(final, legs)
        require(self.ready["native_wallet_query"] == self.direct_native,
                "fixture native endpoint changed during the run")
        self.receipt.update({
            "status": "passed",
            "same_process": {"reconnected": True, "process_identical": True,
                             "height": inclusion["height"], "state": "confirmed_exactly_once"},
            "scenario": {
                "outbound": {
                    "tx_fingerprint": tx_fingerprint(txid),
                    "mined_height": inclusion["height"],
                    "block_hash": inclusion["block_hash"],
                    "raw_sha256": raw.get("sha256"),
                    "pending": pending,
                    "surfaces": surfaces,
                    "accounting": accounting,
                    "received_amount_zat": received[0]["amount_zat"],
                    "received_memo": memo_evidence(received[0]["memos"], MEMO_OUTBOUND),
                    "fully_synced_height": same_process["height"],
                    "memo_verified": True,
                    "spendable_after_zat": spendable["spendable_zat"],
                },
                "outage": down,
                "restore": {"endpoint": "same-loopback-native-relay",
                            "same_process_reconnect": True},
                "return": return_leg,
                "persistence": persistence,
            },
            "assertions": {
                "pending_observed_by_node_and_both_wallets": True,
                "only_the_private_native_route_was_stopped": True,
                "node_and_other_wallet_routes_stayed_up": True,
                "native_route_down_before_reconnect": True,
                "transfer_mined_and_canonical_while_route_down": True,
                "zallet_process_alive_and_identical_through_outage": True,
                "same_endpoint_restored": True,
                "same_process_reconnected_without_restart": True,
                "canonical_block_and_hash_verified": True,
                "exactly_once_credit": True,
                "exactly_once_debit": True,
                "fee_reconciles": True,
                "memo_verified": True,
                "received_funds_spendable": True,
                "persisted_across_wallet_restart": True,
            },
        })

    def close_wallets(self):
        try:
            super().close_wallets()
        finally:
            if self.route_process is not None:
                self.stop_route()

    def check_result(self):
        super().check_result()
        validate_receipt(self.receipt)

    def restart_control(self, txid, height, outage_tip):
        try:
            self.zallet.restart()
            self.wait_process_confirmed(txid, height, outage_tip)
            return {"confirmed_after_restart": True, "height": height}
        except Exception as error:
            return {"confirmed_after_restart": False,
                    "error": f"{type(error).__name__}: {error}"}


def validate_receipt(receipt):
    require(receipt.get("schema_version") == 1 and receipt.get("case") == "transport",
            "unexpected transport receipt identity")
    require(receipt.get("network") == "zcash-regtest",
            "transport receipt must describe Regtest")
    require(receipt.get("status") in {"passed", "failed"},
            "unexpected transport receipt status")
    require(isinstance(receipt.get("assertions"), dict),
            "transport receipt assertions must be an object")
    if receipt["status"] == "passed":
        scenario = receipt.get("scenario", {})
        require(scanned_through(scenario.get("outbound", {}).get("fully_synced_height"),
                                scenario.get("outage", {}).get("node_tip")),
                "Zallet did not scan through the outage tip")
        require(receipt["assertions"], "a passed transport receipt has no assertions")
        require(all(value is True for value in receipt["assertions"].values()),
                "a passed transport receipt has an unproven assertion")
        require(receipt.get("same_process", {}).get("reconnected") is True,
                "a passed transport receipt did not prove same-process reconnect")
        require(receipt.get("cleanup", {}).get("project_resources_remaining") == 0,
                "a passed transport receipt left owned Docker resources")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new private evidence directory")
    parser.add_argument("--port-base", type=int, default=20900)
    parser.add_argument("--wait-seconds", type=int, default=900)
    parser.add_argument("--images", type=Path, required=True, help="fixture operator image map")
    parser.add_argument("--bin-dir", type=Path, required=True, help="release binaries to mount over those images")
    parser.add_argument("--revision", required=True, help="full published Zinder commit")
    parser.add_argument("--zingo", type=Path, required=True, help="pinned Zingo CLI")
    parser.add_argument("--zallet-backend", type=Path, required=True,
                        help="ordinary zallet-zinder executable")
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit("transport payment interrupted"))
    require(len(args.revision) == 40
            and all(character in "0123456789abcdef" for character in args.revision),
            "invalid revision")
    require(args.wait_seconds > 0, "timeout must be positive")
    RECOVERY.check_ports([args.port_base], ROUTE_OFFSETS)
    transport = Transport(args)
    transport.execute(transport.run_transport)
    print(transport.output / "receipt.json")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        sys.exit(f"transport payment failed: {error}")
