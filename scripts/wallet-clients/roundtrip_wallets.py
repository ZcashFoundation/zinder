"""Pinned Zingo CLI and ordinary Zallet adapters for independent two-wallet payments."""
import base64
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import subprocess
import time
import tomllib
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from wallet_fixture import loopback


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def write_private(path, text):
    tmp = path.with_name(path.name + "." + secrets.token_hex(4))
    with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as f:
        f.write(text if isinstance(text, bytes) else text.encode())
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def pin_directory(directory, network, endpoint):
    require(network in {"testnet", "regtest"}, "unsupported wallet network")
    loopback(endpoint)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not directory.is_symlink() and directory.stat().st_mode & 0o077 == 0,
            "wallet directory must be private")
    identity = {"network": network, "endpoint": endpoint}
    p = directory / "network.json"
    if p.exists():
        require(json.loads(p.read_text()) == identity, "wallet network or endpoint differs")
    else:
        require(not any(directory.iterdir()), "wallet directory has no network identity")
        write_private(p, json.dumps(identity))


def shielded_address(address, network):
    require(isinstance(address, str) and address.startswith(
        "uregtest1" if network == "regtest" else "utest1"), "wrong network or address type")
    return address


def valid_txid(txid):
    require(isinstance(txid, str) and re.fullmatch(r"[0-9a-f]{64}", txid), "invalid wallet transaction ID")
    return txid


def zingo_transactions(display, messages):
    memo_by_tx = {}
    for transfer in messages["value_transfers"]:
        memo_by_tx.setdefault(transfer["txid"], []).extend(transfer["memos"])
    entries = []
    for block in re.split(r"(?m)^\{\s*\n(?=    txid: )", display)[1:]:
        fields = dict(re.findall(r"(?m)^    (txid|blockheight|status|kind|fee|value): ([^\n]+)$", block))
        txid = valid_txid(fields["txid"])
        confirmed = fields["status"] == "confirmed"
        entries.append({"txid": txid, "height": int(fields["blockheight"]) if confirmed else None,
                        "pending": fields["status"] in {"transmitted", "mempool", "pending"},
                        "amount_zat": int(fields["value"].replace("_", "")),
                        "kind": fields["kind"], "fee_zat": int(fields["fee"]) if fields["fee"].isdigit() else None,
                        "memos": memo_by_tx.get(txid, []), "change_zat": None})
    require(display.count("\n    txid: ") == len(entries), "unparsed Zingo transaction")
    return entries


class Zingo:
    def __init__(self, binary, directory, network, endpoint, birthday):
        self.binary = str(Path(binary).resolve())
        require(not Path(directory).is_symlink(), "refusing a symlinked wallet directory")
        self.directory = Path(directory).resolve()
        self.network, self.endpoint, self.birthday = network, endpoint, birthday
        pin_directory(self.directory, network, endpoint)

    def _cli(self, *args, online=False):
        if (self.directory / "receiving-address").exists():
            require((self.directory / "zingo-wallet.dat").is_file(), "persisted Zingo wallet file missing")
        argv = [self.binary, "--chain", self.network, "--data-dir", str(self.directory),
                "--birthday", str(self.birthday)]
        argv += ["--server", self.endpoint, "--waitsync"] if online else ["--offline"]
        env = {k: v for k, v in os.environ.items() if not k.startswith("ZINGO_")}
        result = subprocess.run([*argv, *args], capture_output=True, text=True, env=env, timeout=270)
        require(result.returncode == 0 and "error: sync error" not in result.stderr.lower(),
                "Zingo command failed: " + args[0])
        return result.stdout

    def prepare(self):
        address = shielded_address(json.loads(self._cli("addresses", online=True))[0]["encoded_address"], self.network)
        saved = self.directory / "receiving-address"
        if saved.exists():
            require(saved.read_text() == address, "Zingo receiving address changed")
        else:
            write_private(saved, address)
        if self.network == "testnet":
            backup = self.directory / "initial-wallet-backup.dat"
            if not backup.exists():
                write_private(backup, (self.directory / "zingo-wallet.dat").read_bytes())
        return address

    def transparent_address(self):
        return json.loads(self._cli("t_addresses"))[0]["encoded_address"]

    def snapshot(self):
        sync = json.loads(self._cli("sync", "status", online=True))
        height = max((int(r["end_block"]) for r in sync["scan_ranges"] if r["priority"] == "Scanned"),
                     default=self.birthday - 1)
        balances = re.findall(r"(?m)^\s*confirmed_(?:transparent|sapling|orchard|ironwood)_balance: ([\d_]+)$",
                              self._cli("balance"))
        require(len(balances) == 4, "incomplete Zingo pool balances")
        spendable = json.loads(self._cli("spendable_balance"))["spendable_balance"]
        return {"height": height, "total_zat": sum(map(int, balances)), "spendable_zat": spendable,
                "transactions": zingo_transactions(self._cli("transactions"), json.loads(self._cli("messages")))}

    def _broadcast(self, *args):
        result = json.loads(self._cli(*args, online=True))
        require(len(result.get("txids", [])) == 1, "expected exactly one Zingo transaction")
        return valid_txid(result["txids"][0])

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        return self._broadcast("quicksend", shielded_address(address, self.network), str(amount_zat), memo)

    def shield(self):
        require(self.network == "regtest", "coinbase setup requires disposable Regtest")
        return self._broadcast("quickshield")

    def restart(self):
        # Each CLI invocation opens the persisted wallet in a new process.
        return self.prepare()

    def close(self):
        pass


class WalletRpcError(RuntimeError):
    def __init__(self, method, error):
        self.method = method
        self.code = error["code"]
        super().__init__(f"wallet RPC {method}: {self.code}: {error.get('message')}")


class Zallet:
    def __init__(self, backend, directory, network, endpoint, birthday, rpc_port, warmup_timeout=2400):
        self.binary = str(Path(backend).resolve())
        require(not Path(directory).is_symlink(), "refusing a symlinked wallet directory")
        self.directory = Path(directory).resolve()
        self.network, self.endpoint = network, endpoint
        require(warmup_timeout > 0, "wallet warmup timeout must be positive")
        self.warmup_timeout = warmup_timeout
        pin_directory(self.directory, network, endpoint)
        require(type(rpc_port) is int and 1024 <= rpc_port <= 65535, "invalid wallet RPC port")
        self.rpc_url = f"http://127.0.0.1:{rpc_port}"
        self.config = self.directory / "zallet.toml"
        self.argv = [self.binary, "--datadir=" + str(self.directory), "--config=" + str(self.config)]
        self.process = None
        self.account = self.address = None
        if not self.config.exists():
            import toml
            example = subprocess.check_output([self.binary, "--datadir=" + str(self.directory), "example-config",
                                               "-o", "-", "--this-is-beta-code-and-you-will-need-to-recreate-the-example-later"],
                                              text=True, stderr=subprocess.DEVNULL)
            conf = tomllib.loads(example)
            conf["backend"] = "zinder"
            conf["zinder"] = {"wallet_query_endpoint": endpoint}
            conf["consensus"] = {"network": "test" if network == "testnet" else "regtest"}
            if network == "regtest":
                branches = "5ba81b19 76b809bb 2bb40e60 f5b9230b e9ff75a6 c2d6d0b4 c8e71055 4dec4df0 5437f330 37a5165b"
                conf["consensus"]["regtest_nuparams"] = [b + ":1" for b in branches.split()]
            conf["keystore"]["encryption_identity"] = str(self.directory / "identity.txt")
            conf["rpc"]["bind"] = [f"127.0.0.1:{rpc_port}"]
            conf["rpc"]["auth"] = [{"user": "roundtrip", "password": secrets.token_hex(32)}]
            write_private(self.config, toml.dumps(conf))
        conf = tomllib.loads(self.config.read_text())
        require(conf["rpc"]["bind"] == [f"127.0.0.1:{rpc_port}"] and conf["zinder"]["wallet_query_endpoint"] == endpoint,
                "Zallet persisted configuration differs")
        self.auth = base64.b64encode(("roundtrip:" + conf["rpc"]["auth"][0]["password"]).encode()).decode()
        self.opener = build_opener(ProxyHandler({}))

    def _command(self, *command, stdin=None):
        args = [*self.argv, *command]
        if stdin is not None:
            args = ["/usr/bin/script", "--echo=never", "-qec", shlex.join(args), "/dev/null"]
        result = subprocess.run(args, input=stdin, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                timeout=45)
        require(result.returncode == 0, "Zallet initialization failed: " + command[0])

    def _rpc(self, method, params=()):
        payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}).encode()
        request = Request(self.rpc_url, data=payload, headers={"Content-Type": "application/json",
                                                             "Authorization": "Basic " + self.auth})
        try:
            with self.opener.open(request, timeout=15) as response:
                value = json.load(response)
        except HTTPError as http_error:
            with http_error:
                try:
                    value = json.load(http_error)
                except (ValueError, UnicodeError):
                    raise http_error from None
            if not isinstance(value, dict) or not value.get("error"):
                raise
        if value.get("error"):
            raise WalletRpcError(method, value["error"])
        return value["result"]

    def _process_start(self, pid):
        proc = Path(f"/proc/{pid}")
        try:
            if proc.stat().st_uid != os.geteuid() or not (proc / "exe").samefile(self.binary):
                return None
            argv = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
            if argv != [os.fsencode(v) for v in [*self.argv, "start"]]:
                return None
            return int((proc / "stat").read_bytes().rsplit(b")", 1)[1].split()[19])
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            return None

    def _owned_identity(self):
        path = self.directory / "daemon.pid"
        if not path.exists():
            return None
        record = json.loads(path.read_text())
        pid = record if type(record) is int else record["pid"]
        require(type(pid) is int and pid > 0, "invalid daemon identity")
        start = self._process_start(pid)
        if start is None or (type(record) is not int and start != record["start_ticks"]):
            return None
        return pid, start

    def _record_identity(self, pid, start):
        write_private(self.directory / "daemon.pid", json.dumps({"pid": pid, "start_ticks": start}))

    def _new_child_identity(self, child):
        deadline = time.monotonic() + 5
        while child.poll() is None:
            start = self._process_start(child.pid)
            if start is not None:
                return child.pid, start
            if time.monotonic() >= deadline:
                raise TimeoutError("new Zallet child did not acquire the expected process identity")
            time.sleep(.05)
        raise RuntimeError(f"new Zallet child exited with status {child.returncode}; see its private log")

    def _wait_for_rpc(self, identity):
        end = time.monotonic() + 60
        while time.monotonic() < end:
            require(self._owned_identity() == identity, "Zallet daemon exited or changed; see its private log")
            try:
                self._rpc("getwalletstatus")
            except WalletRpcError as error:
                if error.code not in {-10, -28}:
                    raise
            except HTTPError:
                raise
            except OSError:
                pass
            else:
                require(self._owned_identity() == identity, "Zallet process changed during startup")
                return
            time.sleep(0.3)
        raise TimeoutError("Zallet wallet RPC did not become ready")

    def _start(self):
        identity = self._owned_identity()
        if identity is not None:
            self._wait_for_rpc(identity)
            # Bind legacy PID-only records after the existing daemon authenticates.
            self._record_identity(*identity)
            return
        with (self.directory / "daemon.log").open("ab") as log:
            child = subprocess.Popen([*self.argv, "start"], stdout=log, stderr=log, start_new_session=True)
        self.process = child
        try:
            identity = self._new_child_identity(child)
            self._record_identity(*identity)
            self._wait_for_rpc(identity)
        except BaseException as error:
            # Until start succeeds, only this Popen child belongs to this attempt.
            try:
                if child.poll() is None:
                    child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                self.process = None
            except (OSError, subprocess.TimeoutExpired) as cleanup_error:
                error.add_note(f"Zallet child {child.pid} cleanup failed: {cleanup_error}")
            raise

    def _initialize(self):
        identity = self.directory / "identity.txt"
        db = self.directory / "wallet.db"
        ready = self.directory / "seed-ready"
        backup = self.directory / "seed-backup.age"
        if ready.exists():
            require(identity.is_file() and db.is_file() and backup.is_file(), "persisted native wallet or backup missing")
            return
        if not identity.exists():
            require(not db.exists(), "native wallet lost its encryption identity")
            self._command("generate-encryption-identity")
        if not db.exists():
            self._command("init-wallet-encryption")
        intent = self.directory / "seed-init.intent"
        seed_present = False
        if intent.exists():
            # Recover a crash between committing the seed and exporting the backup.
            self._start()
            try:
                fingerprints = self._rpc("getwalletinfo")["mnemonic_seedfps"]
                require(len(fingerprints) <= 1, "ambiguous prior seed initialization")
                seed_present = bool(fingerprints)
            finally:
                self.close()
        else:
            write_private(intent, "1")
        if not seed_present:
            if self.network == "testnet":
                from mnemonic import Mnemonic
                seed = Mnemonic("english").generate(strength=256)
                self._command("import-mnemonic", stdin=(seed + "\n").encode())
            else:
                self._command("generate-mnemonic")
        encrypted = subprocess.run([*self.argv, "export-mnemonic", "--armor"], capture_output=True, timeout=45)
        require(encrypted.returncode == 0 and encrypted.stdout.lstrip().startswith(b"-----BEGIN AGE ENCRYPTED FILE-----"),
                "encrypted native seed backup failed")
        write_private(backup, encrypted.stdout.decode("ascii"))
        write_private(ready, "1")

    def prepare(self):
        self._initialize()
        self._start()
        accounts = self._rpc("z_listaccounts", [False])
        require(len(accounts) <= 1, "unexpected additional wallet account")
        self.account = (accounts[0] if accounts else self._rpc("z_getnewaccount", ["roundtrip"]))["account_uuid"]
        saved_account = self.directory / "account-id"
        if saved_account.exists():
            require(saved_account.read_text() == self.account, "Zallet account identity changed")
        else:
            write_private(saved_account, self.account)
        addr = self.directory / "receiving-address"
        if not addr.exists():
            ua = self._rpc("z_getaddressforaccount", [self.account, ["orchard"], None])["address"]
            write_private(addr, shielded_address(ua, self.network))
        self.address = shielded_address(addr.read_text(), self.network)
        return self.address

    def snapshot(self):
        deadline = time.monotonic() + self.warmup_timeout
        while True:
            try:
                return self._snapshot_once()
            except WalletRpcError as error:
                if error.code != -10:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Zallet snapshot still unavailable after {self.warmup_timeout}s: {error}") from error
                time.sleep(min(5, remaining))

    def _snapshot_once(self):
        status = self._rpc("getwalletstatus")
        records = self._rpc("z_listtransactions", [self.account])
        balance = self._rpc("z_getbalanceforaccount", [self.account, 10])
        txs = []
        for tx in records:
            delta = tx["account_balance_delta"]
            outputs = tx["outputs"]
            receive = [o for o in outputs if o["to_account"] == self.account and not o["is_change"]]
            sent = [o for o in outputs if o["from_account"] == self.account and o["to_account"] != self.account]
            txs.append({"txid": valid_txid(tx["txid"]), "height": tx["mined_height"],
                        "pending": tx["mined_height"] is None and not tx["expired_unmined"],
                        "amount_zat": sum(o["value"] for o in (receive if delta >= 0 else sent)),
                        "delta_zat": delta, "fee_zat": tx.get("fee_paid"),
                        "kind": "received" if delta >= 0 else "sent",
                        "memos": [o["memo"] for o in outputs if o.get("memo")],
                        "change_zat": sum(o["value"] for o in outputs if o["is_change"] and o["to_account"] == self.account)})
        return {"height": status.get("fully_synced_height"), "total_zat": sum(t["delta_zat"] for t in txs if t["height"] is not None),
                "spendable_zat": sum(p["valueZat"] for p in balance["pools"].values()), "transactions": txs}

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        dest = {"address": shielded_address(address, self.network), "amount": amount_zat / 100000000, "memo": memo.encode().hex()}
        opid = self._rpc("z_sendmany", [self.address, [dest], 10, None, "FullPrivacy"])
        write_private(self.directory / "last-operation", opid)
        end = time.monotonic() + 240
        while time.monotonic() < end:
            results = self._rpc("z_getoperationstatus", [[opid]])
            if results and results[0]["status"] not in {"queued", "executing"}:
                require(results[0]["status"] == "success", "Zallet transfer failed; inspect saved operation ID")
                return valid_txid(results[0]["result"]["txid"])
            time.sleep(0.5)
        raise RuntimeError("Zallet operation pending; inspect saved operation ID")

    def close(self):
        identity = self._owned_identity()
        if identity is not None:
            try:
                self._rpc("stop")
            except WalletRpcError as error:
                if error.code != -32601:
                    raise
                # stop is Regtest-only. The pidfd stays bound across PID reuse.
                pid, _ = identity
                try:
                    fd = os.pidfd_open(pid)
                except ProcessLookupError:
                    fd = None
                if fd is not None:
                    try:
                        require(self._owned_identity() == identity, "refusing to signal a changed Zallet process")
                        try:
                            signal.pidfd_send_signal(fd, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                    finally:
                        os.close(fd)
            until = time.monotonic() + 45
            while self._owned_identity() == identity and time.monotonic() < until:
                time.sleep(.25)
            require(self._owned_identity() != identity, "Zallet daemon failed to stop")
        if self.process is not None:
            self.process.wait(timeout=5)
            self.process = None

    def restart(self):
        self.close()
        return self.prepare()
