#!/usr/bin/env python3
"""Two-party payment adapter for Cake Wallet's pinned Zcash engine.

Drives the stock local wallet GraphQL API of the pinned `zkool_graphql`
executable with a per-run ES256 token. Every engine process runs under the
connection trace so the adapter can prove it reached only the fixture
lightwalletd endpoint.

Reused by `zkool-payment-matrix.py`; not run on its own.
"""
import base64
import hashlib
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
from decimal import Decimal
from urllib.error import URLError
from urllib.request import ProxyHandler, Request, build_opener

from roundtrip_wallets import pin_directory, require, shielded_address, valid_txid, write_private

HTTP = build_opener(ProxyHandler({}))
ZATS = Decimal(100_000_000)
STOP_GRACE = 20


def _lifecycle():
    """Reuse the tested bounded process-group and pidfd lifecycle helpers."""
    name = "wallet_roundtrip_regtest"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).resolve().parent.parent / "run-wallet-roundtrip-regtest.py"
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    try:
        spec.loader.exec_module(loaded)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return loaded


def _credentials(folder):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, utils

    key_path = folder / "jwt.key"
    if key_path.exists():
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    else:
        key = ec.generate_private_key(ec.SECP256R1())
        write_private(key_path, key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode())
    write_private(folder / "jwt.pub", key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode())
    if not (folder / "db.password").exists():
        write_private(folder / "db.password", secrets.token_urlsafe(48))

    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")

    header = encode(b'{"alg":"ES256","typ":"JWT"}')
    claims = {"exp": int(time.time()) + 7200, "sub": 0, "write": True}
    signing = header + b"." + encode(json.dumps(claims).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + encode(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode()


def _engine_pid(first, executable):
    todo = [first]
    while todo:
        pid = todo.pop()
        proc = Path("/proc") / str(pid)
        try:
            if (proc / "exe").resolve(strict=True) == executable:
                return pid
            todo.extend(map(int, (proc / f"task/{pid}/children").read_text().split()))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return None


def _process_identity(pid, executable):
    """Pin an exact local engine process, never a bare recycled PID."""
    if not pid:
        return None
    proc = Path("/proc") / str(pid)
    try:
        if proc.stat().st_uid != os.geteuid():
            return None
        if (proc / "exe").resolve(strict=True) != executable:
            return None
        return pid, int((proc / "stat").read_bytes().rsplit(b")", 1)[1].split()[19])
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError, IndexError):
        return None


class CakePayment:
    def __init__(self, engine, trace_tool, directory, network, endpoint, birthday, evidence,
                 use_internal=True):
        require(network == "regtest", "Cake two-party payments are exercised on Regtest only")
        self.engine = Path(engine).resolve(strict=True)
        self.trace_tool = Path(trace_tool).resolve(strict=True)
        self.directory = Path(directory).resolve()
        self.network, self.endpoint, self.birthday = network, endpoint, birthday
        self.use_internal = use_internal
        self.evidence = Path(evidence)
        self.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(not self.directory.is_symlink(), "refusing a symlinked wallet directory")
        pin_directory(self.directory, network, endpoint)
        require(not (self.directory / "db.password").exists() or (self.directory / "jwt.pub").is_file(),
                "Cake credentials are incomplete")
        self.token = _credentials(self.directory)
        self.port = None
        self.process = None
        self.pid = None
        self.identity = None
        self.trace = None
        self.traces = []
        self.address = None
        self.account = None
        self.database = self.directory / "cake.db"
        self.log = None
        self.started = False
        self.last_pid = None
        self.last_identity = None

    # -- GraphQL ------------------------------------------------------------
    def _post(self, url, document):
        from urllib.error import HTTPError
        request = Request(url, json.dumps(document).encode(),
                          {"Content-Type": "application/json", "Authorization": "Bearer " + self.token})
        try:
            with HTTP.open(request, timeout=180) as response:
                return json.load(response)
        except HTTPError as error:
            with error:
                body = error.read().decode("utf-8", "replace")[:800]
            raise RuntimeError(f"Cake GraphQL HTTP {error.code}: {body}") from None

    def gql(self, query, variables=None):
        response = self._post(f"http://127.0.0.1:{self.port}/graphql",
                              {"query": query, "variables": variables or {}})
        require(not response.get("errors"),
                "Cake GraphQL error: " + "; ".join(e["message"][:200] for e in response.get("errors", [])))
        return response["data"]

    # -- server lifecycle ---------------------------------------------------
    def _start(self, label):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        stamp = f"{label}-{time.time_ns()}"
        trace = self.evidence / ("trace-" + stamp)
        command = [sys.executable, str(self.trace_tool), "--endpoint-port",
                   str(int(self.endpoint.rsplit(":", 1)[1])), "--output", str(trace), "--",
                   str(self.engine), "--coin", "2", "--db-path", str(self.directory / "cake.db"),
                   "--db-password-file", str(self.directory / "db.password"),
                   "--lwd-url", self.endpoint, "--bind-address", "127.0.0.1",
                   "--port", str(self.port), "--jwt-public-key-file", str(self.directory / "jwt.pub")]
        self.log = (self.evidence / (stamp + ".log")).open("wb")
        self.trace = trace
        self.started = False
        try:
            self.process = subprocess.Popen(command, cwd=self.directory, stdout=self.log,
                                            stderr=self.log, start_new_session=True)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                require(self.process.poll() is None,
                        "Cake engine exited before readiness; see private log")
                try:
                    self.gql("{__typename}")
                    self.pid = _engine_pid(self.process.pid, self.engine)
                    require(self.pid, "could not identify the Cake engine process")
                    self.identity = self._engine_identity(self.pid)
                    require(self.identity is not None,
                            "could not pin the Cake engine process identity")
                    self.started = True
                    self.last_pid = self.pid
                    self.last_identity = self.identity
                    return
                except (URLError, OSError, KeyError):
                    time.sleep(0.3)
            raise TimeoutError("Cake GraphQL did not become ready")
        except BaseException:
            # A partial start must not leave the traced engine or its children.
            self._terminate()
            raise

    def _engine_identity(self, pid):
        return _process_identity(pid, self.engine)

    def _pidfd_signal(self, identity, signum):
        """Signal only the exact pinned process, without a bare-PID fallback."""
        pid, _start = identity
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            return False
        try:
            if self._engine_identity(pid) != identity:
                return False
            signal.pidfd_send_signal(fd, signum)
            return True
        except ProcessLookupError:
            return False
        finally:
            os.close(fd)

    def _terminate(self):
        identity = getattr(self, "identity", None) or getattr(self, "last_identity", None)
        process = getattr(self, "process", None)
        if identity is None and process is not None:
            identity = self._engine_identity(_engine_pid(process.pid, self.engine))
        if process is not None:
            try:
                _lifecycle().stop_group(process, signal.SIGTERM, STOP_GRACE)
            except (OSError, subprocess.TimeoutExpired):
                pass
            self.process = None
        if identity is not None and self._engine_identity(identity[0]) == identity:
            try:
                _lifecycle().stop_owned_process(
                    identity, lambda: self._engine_identity(identity[0]), STOP_GRACE)
            except (OSError, RuntimeError):
                pass
        if self.log is not None and not self.log.closed:
            self.log.close()
        self.started = False
        self.process = None
        self.pid = None
        self.identity = None

    def _engine_alive(self, identity):
        return identity is not None and self._engine_identity(identity[0]) == identity

    def _stop(self):
        trace, started = self.trace, self.started
        try:
            if started and self.process is not None and self.identity is not None:
                # End the engine first so strace can flush and its supervisor can write the report.
                self._pidfd_signal(self.identity, signal.SIGTERM)
                try:
                    self.process.wait(timeout=STOP_GRACE)
                except subprocess.TimeoutExpired:
                    self._pidfd_signal(self.identity, signal.SIGKILL)
                    self.process.wait(timeout=STOP_GRACE)
        finally:
            self._terminate()
        if started and trace is not None:
            report_path = trace / "report.json"
            require(report_path.exists(), "Cake engine trace did not finish")
            report = json.loads(report_path.read_text())
            require(report.get("status") == "passed" and report.get("fallback_used") is False,
                    "Cake endpoint attribution failed")
            self.traces.append({"endpoint": self.endpoint,
                                "connect_log_sha256": hashlib.sha256(
                                    (trace / "connect.log").read_bytes()).hexdigest()})
        self.trace = None

    def shutdown_evidence(self):
        return {"scope": "process-tree",
                "processes_remaining": 1 if self._engine_alive(self.last_identity) else 0,
                "engine_pid": self.last_pid}

    # -- wallet -------------------------------------------------------------
    def _sync(self, target):
        deadline = time.monotonic() + 600
        while True:
            height = self.gql("mutation{synchronizeAccount(idAccount:1,fast:false)}")["synchronizeAccount"]
            if height >= target:
                return height
            require(time.monotonic() < deadline, "Cake sync did not reach the requested height")
            time.sleep(1)

    def _account(self):
        accounts = self.gql("{accounts{id seed}}")["accounts"]
        require(len(accounts) <= 1, "unexpected additional Cake account")
        if accounts:
            require(accounts[0]["id"] == 1, "Cake account identity changed")
            return 1
        data = {"name": "Payment", "birth": self.birthday, "aindex": 0, "pools": 15,
                "useInternal": self.use_internal}
        created = self.gql("mutation($a:NewAccount!){createAccount(newAccount:$a)}",
                           {"a": data})["createAccount"]
        require(created == 1, "expected account 1 in unused Cake database")
        return created

    def prepare(self, target=None):
        if self.process is None:
            self._start("server")
        self.account = self._account()
        if target is not None:
            self._sync(target)
        addresses = self.gql("{addressByAccount(idAccount:1,pools:15){transparent sapling ua}}")["addressByAccount"]
        require(all(addresses.values()), "Cake returned an incomplete address set")
        address = shielded_address(addresses["ua"], self.network)
        saved = self.directory / "receiving-address"
        if saved.exists():
            require(saved.read_text() == address, "Cake receiving address changed")
        else:
            write_private(saved, address)
        self.addresses = addresses
        self.address = address
        return address

    def transparent_address(self):
        return self.addresses["transparent"]

    def snapshot(self, target):
        height = self._sync(target)
        data = self.gql("{balanceByAccount(idAccount:1){height transparent sapling orchard ironwood total} "
                        "transactionsByAccount(idAccount:1){id txid height value fee}}")
        balance = data["balanceByAccount"]
        transactions = []
        for row in data["transactionsByAccount"]:
            txid = valid_txid(row["txid"])
            value = int(Decimal(row["value"]) * ZATS)
            fee = int(Decimal(row["fee"]) * ZATS)
            block = row["height"] if row["height"] > 0 else None
            memos = []
            if value > 0:
                memos = self.gql("query($i:Int!){memosByTransaction(idAccount:1,idTransaction:$i)}",
                                 {"i": row["id"]})["memosByTransaction"]
            transactions.append({
                # Cake can keep a mined transaction in unconfirmedByAccount, so
                # a positive height is the only reliable confirmation signal.
                "txid": txid, "height": block, "pending": block is None,
                "amount_zat": abs(value), "delta_zat": value,
                "fee_zat": fee if fee > 0 else None,
                "kind": "received" if value > 0 else "sent",
                "memos": memos, "change_zat": None,
            })
        pools = {name: int(Decimal(balance[name]) * ZATS)
                 for name in ("transparent", "sapling", "orchard", "ironwood")}
        return {"height": height, "total_zat": pools["transparent"] + pools["sapling"] + pools["orchard"]
                + pools["ironwood"], "spendable_zat": int(Decimal(balance["total"]) * ZATS),
                "transactions": transactions, "pools": pools}

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        payment = {"recipients": [{"address": shielded_address(address, self.network),
                                   "amount": str(Decimal(amount_zat) / ZATS), "memo": memo or None}],
                   "srcPools": 15}
        txid = self.gql("mutation($p:Payment!){pay(idAccount:1,payment:$p)}", {"p": payment})["pay"]
        return valid_txid(txid)

    def restart(self, target=None):
        self._stop()
        return self.prepare(target)

    def close(self):
        self._stop()

    def endpoint_evidence(self):
        require(self.traces, "no traced Cake engine processes")
        return {"scope": "process-trace", "target_connected": True, "endpoint": self.endpoint,
                "processes": len(self.traces),
                "trace_fingerprint_sha256": hashlib.sha256(
                    json.dumps(self.traces, sort_keys=True).encode()).hexdigest(),
                "traces": self.traces}
