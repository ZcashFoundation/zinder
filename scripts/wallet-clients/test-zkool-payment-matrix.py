#!/usr/bin/env python3
"""Bounded regression tests for the Zkool/Cake payment matrix.

Runs with or without ``python -O``; control-flow checks use ``require`` and
``unittest`` assertions, not bare ``assert`` statements.
"""
import importlib.util
import json
from contextlib import closing
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def stop_process(process):
    if process is None:
        return
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


MATRIX = load("zkool_payment_matrix_test", "zkool-payment-matrix.py")
CAKE = sys.modules["cake_payment"]
REVISION = "999d51ab835037a2466763ca1a1e7739a26b72c1"


def pins(marker="0"):
    digest = marker * 64
    return {
        "harness_sha256": {"zkool-payment-matrix.py": digest,
                           "zkool-payment.py": digest,
                           "cake-payment.py": digest},
        "executables_sha256": {"zkool": digest, "cake": digest, "zallet": digest},
        "fixture_ready_sha256": digest,
    }


def valid_receipt(state, case="zkool-internal", marker="0"):
    value = pins(marker)
    trace = state / "evidence" / "zkool" / "trace-1"
    trace.mkdir(parents=True)
    report = trace / "report.json"
    report.write_text(json.dumps({"status": "passed", "fallback_used": False}))
    connect = trace / "connect.log"
    connect.write_text("connect\n")
    (state / "zallet").mkdir()
    (state / "zallet" / "zallet.toml").write_text("config\n")
    MATRIX.write_json(state / "preflight.json", {"network": "zcash-regtest"})
    MATRIX.write_json(state / "run.json", {"config": {}})
    cleanup = {"verified": True, "wallet_stopped": True, "counterpart_stopped": True}
    MATRIX.write_json(state / "cleanup.json", cleanup)
    config = {"case": case, "ready_sha256": value["fixture_ready_sha256"],
              "revision": REVISION, "release": MATRIX.RELEASE,
              "outbound_zat": MATRIX.OUTBOUND, "inbound_zat": MATRIX.INBOUND,
              "spendback_zat": MATRIX.SPENDBACK,
              "pins_sha256": MATRIX.fingerprint(json.dumps(value, sort_keys=True))}
    receipt = {
        "schema_version": 1,
        "status": "passed",
        "case": case,
        "network": "regtest",
        "release": {"tag": MATRIX.RELEASE, "revision": REVISION},
        "upstream": MATRIX.build_identities(value),
        "inputs": config,
        "pins": {"pre": value, "post": value},
        "harness_sha256": value["harness_sha256"],
        "binaries": value["executables_sha256"],
        "preflight": {
            "network": "zcash-regtest",
            "images": {name: "sha256:" + "2" * 64 for name in MATRIX.SERVICES},
            "anchor": {"height": 260, "hash": "1" * 64},
            "node_tip": {"height": 260, "hash": "1" * 64},
            "node_image": "sha256:" + "2" * 64,
            "image_references": {name: f"offline-pin/zinder-{name}:0.22.0"
                                 for name in MATRIX.SERVICES},
            "image_manifest_sha256": "3" * 64,
            "activation_schedule_sha256": "4" * 64,
            "raw_blob_policy": "all",
        },
        "wallets": {"under_test": "internal-enabled", "counterpart": "ordinary-zallet"},
        "wallet_identity": "5" * 64,
        "counterpart_identity": "6" * 64,
        "legs": {name: {"tx_fingerprint": "7" * 64, "mined_height": 300,
                        "amount_zat": 1, "fee_zat": 1, "memo_verified": True,
                        "payer_before_zat": 2, "payee_before_zat": 0,
                        "payer_after_zat": 1, "payee_after_zat": 1}
                 for name in MATRIX.LEGS},
        "actor_observations": {
            "pending_requirement": "node_mempool_only",
            "sender_pending_claimed": False,
            "legs": {name: {"node_mempool": True,
                            "wallet_under_test_pending_observed": False,
                            "counterpart_pending_observed": True}
                     for name in MATRIX.LEGS},
        },
        "result": {
            "reopen_ledger_verified": True,
            "restart_verified": True,
            "wallet_after_reopen_zat": 1,
            "counterpart_after_reopen_zat": 1,
            "incoming_note_spent": {"notes": 1, "spending_height": 320,
                                    "spent": [{"created_height": 310,
                                               "spend_height": 320,
                                               "value_zat": 6_000_000}]},
            "restart_memo_exposure": {name: True for name in MATRIX.LEGS},
            "endpoint_evidence": {"target_connected": True},
        },
        "evidence": {
            "preflight_sha256": MATRIX.digest(state / "preflight.json"),
            "journal_sha256": MATRIX.digest(state / "run.json"),
            "cleanup_sha256": MATRIX.digest(state / "cleanup.json"),
            "counterpart_config": {"path": "zallet/zallet.toml",
                                   "sha256": MATRIX.digest(state / "zallet" / "zallet.toml")},
            "traces": [{"report": "evidence/zkool/trace-1/report.json",
                        "connect_log": "evidence/zkool/trace-1/connect.log",
                        "report_sha256": MATRIX.digest(report),
                        "connect_log_sha256": MATRIX.digest(connect)}],
        },
    }
    MATRIX.write_json(state / "receipt.json", receipt)
    return receipt, config, value


def campaign_args(state, cases=("zkool-internal",), trace_tool=Path("/nonexistent/trace")):
    return types.SimpleNamespace(
        state=state, cases=list(cases), port_bases=[50600, 50700, 50800],
        images=Path("/nonexistent/images.json"), node_config=Path("/nonexistent/node.toml"),
        revision=REVISION, release=MATRIX.RELEASE,
        zkool_bin=Path("/nonexistent/zkool"), cake_engine=Path("/nonexistent/cake"),
        zallet_backend=Path("/nonexistent/zallet"),
        trace_tool=trace_tool, wait_seconds=1,
        case_timeout=30, fixture_timeout=30,
    )


class FakeFixture:
    def __init__(self, state, removed=True):
        self.state, self.removed = state, removed
        self.calls = []

    def up(self, case, project, base):
        self.calls.append(("up", case, project, base))
        target = self.state / "projects" / project
        target.mkdir(parents=True)
        (target / "state.json").write_text(json.dumps({"project": project}))
        (target / "ready.initial.json").write_text("{}")

    def measure(self, case, project, retry=False):
        self.calls.append(("measure", case, project, retry))
        return {"fixture_started": True,
                "project_resources_remaining": 0 if self.removed else 1,
                "relay_running": False,
                "fixture_removed": self.removed,
                "retry": retry}


class MatrixTests(unittest.TestCase):

    def test_missing_cake_auth_dependency_is_reported_before_fixture_start(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "campaign"
            source = str(Path(__file__).resolve())
            argv = ["wallet-payments", "campaign", "--state", str(state),
                    "--revision", "a" * 40, "--images", source, "--node-config", source,
                    "--zkool-bin", source, "--cake-engine", source,
                    "--zallet-backend", source, "--trace-tool", source]
            with (patch.object(MATRIX, "verify_release"),
                  patch.object(MATRIX.os, "umask"),
                  patch.object(MATRIX.importlib, "import_module", side_effect=ImportError),
                  patch.object(MATRIX, "run_campaign") as start):
                with patch.object(sys, "argv", argv):
                    with self.assertRaisesRegex(ValueError, "Cake requires.*cryptography"):
                        MATRIX.main()
                start.assert_not_called()
                self.assertFalse(state.exists())
                with patch.object(sys, "argv", [*argv, "--cases", "zkool-internal"]):
                    MATRIX.main()
                start.assert_called_once()

    def test_execute_timeout_kills_whole_group(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            pidfile = root / "grandchild.pid"
            script = (
                "import subprocess,sys,time\n"
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
                "open(sys.argv[1],'w').write(str(child.pid))\n"
                "time.sleep(60)\n")
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                MATRIX.execute([sys.executable, "-c", script, str(pidfile)],
                               root / "run.log", timeout=2)
            self.assertLess(time.monotonic() - started, 30)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not pidfile.is_file():
                time.sleep(0.05)
            self.assertTrue(pidfile.is_file())
            pid = int(pidfile.read_text())
            while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
                time.sleep(0.05)
            self.assertFalse(Path(f"/proc/{pid}").exists(),
                             "timed-out child left an owned process")

    def test_source_identity_is_bound_to_the_observed_executable(self):
        selected = {"executables_sha256": {name: row["executable_sha256"] for name, row in MATRIX.UPSTREAM.items()}}
        self.assertEqual(MATRIX.build_identities(selected), MATRIX.UPSTREAM)
        selected["executables_sha256"]["cake"] = "0" * 64
        self.assertEqual(MATRIX.build_identities(selected)["cake"], {"executable_sha256": "0" * 64})

    def test_release_tag_and_revision_must_match(self):
        with self.assertRaisesRegex(RuntimeError, "tag must point"):
            MATRIX.verify_release("v0.22.0", "0" * 40)

    def test_pins_must_match_exactly(self):
        before = pins("a")
        after = pins("b")
        with self.assertRaises(RuntimeError):
            MATRIX.verify_pins(before, after)
        self.assertEqual(MATRIX.verify_pins(before, pins("a")), before)

    def test_completed_case_without_pins_requires_fresh_case(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            receipt, config, value = valid_receipt(state)
            del receipt["pins"]
            MATRIX.write_json(state / "receipt.json", receipt)
            with self.assertRaises(RuntimeError):
                MATRIX.verify_completed_case(state, config, value)

    def test_completed_case_reuse_binds_all_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            _, config, value = valid_receipt(state)
            self.assertEqual(MATRIX.verify_completed_case(state, config, value)["case"],
                             "zkool-internal")
            (state / "evidence/zkool/trace-1/connect.log").write_text("changed\n")
            with self.assertRaises(RuntimeError):
                MATRIX.verify_completed_case(state, config, value)

    @patch.object(MATRIX, "check_ports", lambda _: None)
    def test_campaign_never_counts_case_with_failed_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "campaign"
            args = campaign_args(state)

            def runner(_args, case, _ready, case_state, _base):
                case_state.mkdir(parents=True)
                MATRIX.write_json(case_state / "receipt.json", {"status": "passed"})
                MATRIX.write_json(case_state / "cleanup.json", {"verified": True})

            with self.assertRaises(RuntimeError):
                MATRIX.run_campaign(
                    args, fixture_factory=lambda _a, s: FakeFixture(s, removed=False),
                    case_runner=runner)
            summary = json.loads((state / "summary.json").read_text())
            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["cases"]["zkool-internal"]["status"], "failed")
            self.assertIs(summary["cases"]["zkool-internal"]["cleanup"]["fixture_removed"],
                          False)
            self.assertEqual(summary["evidence"]["receipts"], {})

    @patch.object(MATRIX, "check_ports", lambda _: None)
    def test_campaign_counts_only_verified_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "campaign"
            args = campaign_args(state)

            def runner(_args, case, _ready, case_state, _base):
                case_state.mkdir(parents=True)
                valid_receipt(case_state, case=case)

            summary = MATRIX.run_campaign(
                args, fixture_factory=lambda _a, s: FakeFixture(s, removed=True),
                case_runner=runner)
            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["cases"]["zkool-internal"]["status"], "passed")
            self.assertIs(summary["cases"]["zkool-internal"]["cleanup"]["fixture_removed"], True)
            self.assertIn("receipt_sha256", summary["cases"]["zkool-internal"])

    @patch.object(MATRIX, "check_ports", lambda _: None)
    def test_campaign_stops_after_interruption(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "campaign"
            args = campaign_args(state, cases=("zkool-internal", "cake"))
            fixture = FakeFixture(state, removed=True)

            def runner(_args, _case, _ready, _case_state, _base):
                raise KeyboardInterrupt("test interruption")

            with self.assertRaises(KeyboardInterrupt):
                MATRIX.run_campaign(args, fixture_factory=lambda _a, _s: fixture,
                                    case_runner=runner)
            summary = json.loads((state / "summary.json").read_text())
            self.assertEqual(summary["status"], "failed")
            self.assertIn("interrupted", summary["failures"]["zkool-internal"])
            self.assertEqual(summary["failures"]["cake"],
                             "not run after an interruption or cleanup failure")
            self.assertFalse((state / "cake" / "receipt.json").exists())

    @patch.object(MATRIX, "check_ports", lambda _: None)
    def test_summary_is_privacy_safe_and_allowlisted(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "campaign"
            args = campaign_args(state)

            def runner(_args, case, _ready, case_state, _base):
                case_state.mkdir(parents=True)
                valid_receipt(case_state, case=case)

            summary = MATRIX.run_campaign(
                args, fixture_factory=lambda _a, s: FakeFixture(s, removed=True),
                case_runner=runner)
            text = json.dumps(summary)
            self.assertNotIn("uregtest1", text)
            self.assertNotIn('"txid"', text)
            self.assertNotIn('"address"', text)
            case = summary["cases"]["zkool-internal"]
            for key in ("status", "receipt", "receipt_sha256", "cleanup", "outcome",
                        "provenance"):
                self.assertIn(key, case)
            index = {row["path"]: row["sha256"]
                     for row in json.loads((state / "evidence-index.json").read_text())["files"]}
            self.assertEqual(index[case["receipt"]], case["receipt_sha256"])
            self.assertIn("zkool-internal/evidence/zkool/trace-1/connect.log", index)
            self.assertNotIn("zkool-internal/zallet/zallet.toml", index)
            self.assertIs(case["outcome"]["actor_observations"]["sender_pending_claimed"],
                          False)
            self.assertEqual(case["outcome"]["first_incoming_note_spent"]["notes"], 1)

    def test_notes_must_link_to_the_exact_spendback_even_in_same_block(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "notes?.db"
            incoming, spendback, unrelated = [bytes(range(n, n + 32)) for n in (0, 1, 2)]
            with closing(MATRIX.sqlite3.connect(db)) as conn, conn:
                conn.executescript("""
                    CREATE TABLE transactions (id_tx INTEGER, txid BLOB);
                    CREATE TABLE notes (id_note INTEGER, tx INTEGER, height INTEGER, value INTEGER);
                    CREATE TABLE spends (id_note INTEGER, tx INTEGER, height INTEGER);
                    INSERT INTO notes VALUES (10, 1, 383, 6000000);
                    INSERT INTO spends VALUES (10, 2, 393);
                """)
                conn.executemany("INSERT INTO transactions VALUES (?, ?)",
                                 enumerate((v[::-1] for v in (incoming, spendback, unrelated)), 1))
            wallet = types.SimpleNamespace(database=db)
            case = MATRIX.Case(None, None, None, wallet, None)
            self.assertEqual(case.note_spent(wallet, incoming.hex(), spendback.hex(), 393)["spent"],
                             [{"created_height": 383, "value_zat": 6000000, "spend_height": 393}])
            self.assertEqual(case.note_spent(wallet, incoming.hex(), unrelated.hex(), 393)["notes"], 0)
            self.assertEqual(case.note_spent(wallet, incoming.hex(), spendback.hex(), 394)["notes"], 0)

    def test_cake_partial_start_stops_owned_group(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            trace_pid = root / "trace.pid"
            engine_pid = root / "engine.pid"
            engine = root / "engine"
            engine.write_text(
                "#!/usr/bin/env python3\n"
                "import os,time\n"
                f"open({str(engine_pid)!r},'w').write(str(os.getpid()))\n"
                "time.sleep(60)\n")
            engine.chmod(0o755)
            trace = root / "trace.py"
            trace.write_text(
                "#!/usr/bin/env python3\n"
                "import os,subprocess,sys\n"
                f"open({str(trace_pid)!r},'w').write(str(os.getpid()))\n"
                "cut=sys.argv.index('--')\n"
                "raise SystemExit(subprocess.run(sys.argv[cut+1:]).returncode)\n")
            trace.chmod(0o755)
            cake = MATRIX.CakePayment.__new__(MATRIX.CakePayment)
            cake.engine = engine.resolve()
            cake.trace_tool = trace.resolve()
            cake.directory = root
            cake.endpoint = "http://127.0.0.1:50667"
            cake.evidence = root / "evidence"
            cake.evidence.mkdir()
            cake.process = None
            cake.pid = None
            cake.trace = None
            cake.traces = []
            cake.port = None
            cake.log = None
            cake.started = False
            cake.last_pid = None

            def not_ready(_query):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not (
                        trace_pid.is_file() and engine_pid.is_file()):
                    time.sleep(0.02)
                raise __import__("urllib.error", fromlist=["URLError"]).URLError("not ready")

            cake.gql = not_ready
            fake_time = types.SimpleNamespace(
                time_ns=lambda: 1,
                monotonic=iter([0.0, 0.0, 10_000.0, 10_001.0, 10_002.0]).__next__,
                sleep=lambda _seconds: None)
            original_time = CAKE.time
            CAKE.time = fake_time
            try:
                with self.assertRaises(TimeoutError):
                    cake._start("partial")
            finally:
                CAKE.time = original_time
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not (
                    trace_pid.is_file() and engine_pid.is_file()):
                time.sleep(0.05)
            for pidfile in (trace_pid, engine_pid):
                self.assertTrue(pidfile.is_file(), f"{pidfile.name} was not started")
                pid = int(pidfile.read_text())
                while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
                    time.sleep(0.05)
                self.assertFalse(Path(f"/proc/{pid}").exists(),
                                 f"partial Cake start left process {pid}")

    def test_terminate_group_kills_stubborn_descendant_after_leader_exit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            pidfile = root / "grandchild.pid"
            script = (
                "import subprocess,sys,time\n"
                "child=subprocess.Popen([sys.executable,'-c',"
                "'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "time.sleep(60)'])\n"
                "open(sys.argv[1],'w').write(str(child.pid))\n"
                "time.sleep(60)\n")
            sibling = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                       start_new_session=True)
            leader = subprocess.Popen([sys.executable, "-c", script, str(pidfile)],
                                      start_new_session=True)
            grandchild = None
            try:
                self.assertTrue(wait_until(pidfile.is_file), "leader never wrote its child")
                grandchild = int(pidfile.read_text())
                MATRIX.terminate_group(leader, grace=2)
                self.assertIsNotNone(leader.returncode)
                self.assertTrue(wait_until(lambda: not Path(f"/proc/{grandchild}").exists(), 5),
                                "stubborn descendant survived its leader")
                self.assertIsNone(sibling.poll(), "an unrelated sibling was signalled")
            finally:
                stop_process(leader)
                stop_process(sibling)
                if grandchild is not None:
                    try:
                        os.kill(grandchild, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_relay_running_requires_the_exact_owned_relay(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fixture = root / "projects" / "zinder-fixture-payment-exact"
            fixture.mkdir(parents=True)
            owner = "a" * 32
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            relay_file = fixture / "relay.json"
            relay_file.write_text(json.dumps([[port, "127.0.0.1", 9]]))
            state_file = fixture / "state.json"
            relay = subprocess.Popen(
                [sys.executable, str(MATRIX.SCRIPTS / "regtest-fixture-relay.py"),
                 str(relay_file), owner],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            fake = None
            try:
                state_file.write_text(json.dumps(
                    {"relay_pid": relay.pid, "owner": owner, "project": fixture.name}))
                self.assertTrue(wait_until(lambda: MATRIX.relay_running(fixture)),
                                "the exact relay was not recognised")
                state_file.write_text(json.dumps(
                    {"relay_pid": relay.pid, "owner": "b" * 32, "project": fixture.name}))
                self.assertFalse(MATRIX.relay_running(fixture),
                                 "a different owner token was accepted")
                state_file.write_text(json.dumps(
                    {"relay_pid": relay.pid, "owner": owner, "project": fixture.name}))
                self.assertFalse(MATRIX.relay_running(root / "elsewhere"))
                # The project name appears in a bare argument, but this is not
                # the fixture's own relay process.
                fake = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)", str(fixture)],
                    start_new_session=True)
                state_file.write_text(json.dumps(
                    {"relay_pid": fake.pid, "owner": owner, "project": fixture.name}))
                self.assertFalse(MATRIX.relay_running(fixture),
                                 "a process that only mentions the project was accepted")
                state_file.write_text("{")
                self.assertFalse(MATRIX.relay_running(fixture))
                state_file.unlink()
                self.assertFalse(MATRIX.relay_running(fixture))
            finally:
                stop_process(relay)
                stop_process(fake)

    def test_receipt_endpoints_must_be_owned_loopback_ports(self):
        base = 50600
        ready = {name: f"http://127.0.0.1:{base + offset}"
                 for name, offset in MATRIX.ENDPOINT_OFFSETS.items()}
        self.assertEqual(MATRIX.verify_endpoints(ready, base)["node_rpc"],
                         f"http://127.0.0.1:{base + 32}")
        invalid = {
            "node_rpc": "http://0.0.0.0:50632",
            "lightwalletd": "http://192.0.2.10:50667",
            "native_wallet_query": "https://127.0.0.1:50602",
            "query_ops": "http://localhost:50606",
            "compat_ops": "http://127.0.0.1:50607/healthz",
        }
        for name, value in invalid.items():
            candidate = dict(ready, **{name: value})
            with self.assertRaises(ValueError, msg=name):
                MATRIX.verify_endpoints(candidate, base)
        for name, value in (("node_rpc", "http://127.0.0.1"),
                            ("node_rpc", "http://127.0.0.1:80"),
                            ("lightwalletd", ready["node_rpc"])):
            candidate = dict(ready, **{name: value})
            with self.assertRaises(ValueError, msg=f"{name}={value}"):
                MATRIX.verify_endpoints(candidate, base)
        with self.assertRaises(ValueError):
            MATRIX.verify_endpoints(ready, base + 1)
        with self.assertRaises(ValueError):
            MATRIX.verify_endpoints(ready, 1000)
        args = types.SimpleNamespace(revision=REVISION, release=MATRIX.RELEASE)
        with self.assertRaises(ValueError):
            MATRIX.verify_fixture(dict(ready, node_rpc="http://203.0.113.5:50632"),
                                  args, base=base)

    def test_cake_stale_pid_is_never_signalled_without_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            engine = root / "sleep"
            shutil.copy2("/bin/sleep", engine)
            cake = MATRIX.CakePayment.__new__(MATRIX.CakePayment)
            cake.engine = engine.resolve()
            cake.process = types.SimpleNamespace(pid=0)
            process = subprocess.Popen([str(engine), "60"])
            try:
                cake.process.pid = process.pid
                actual = cake._engine_identity(process.pid)
                self.assertIsNotNone(actual)
                stale = (process.pid, actual[1] + 1)
                self.assertFalse(cake._pidfd_signal(stale, signal.SIGTERM))
                self.assertIsNone(process.poll(), "a stale identity was signalled")
                self.assertTrue(cake._pidfd_signal(actual, signal.SIGTERM))
                self.assertEqual(process.wait(timeout=10), -signal.SIGTERM)
            finally:
                stop_process(process)

    @patch.object(MATRIX, "check_ports", lambda _: None)
    def test_campaign_never_passes_with_a_surviving_case_process(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / "campaign"
            trace_tool = root / "trace.py"
            trace_tool.write_text("import time\ntime.sleep(60)\n")
            engine = root / "tail"
            shutil.copy2("/bin/tail", engine)
            args = campaign_args(state, trace_tool=trace_tool)
            args.cake_engine = engine
            survivor = []
            outsider = []

            def runner(_args, case, _ready, case_state, _base):
                case_state.mkdir(parents=True)
                valid_receipt(case_state, case=case)
                (case_state / "cake.db").write_text("detached wallet\n")
                survivor.append(subprocess.Popen(
                    [sys.executable, str(trace_tool), "--endpoint-port", "1",
                     "--output", str(case_state / "evidence" / "cake" / "trace-1")],
                    start_new_session=True))
                survivor.append(subprocess.Popen(
                    [str(engine), "-f", str(case_state / "cake.db")],
                    start_new_session=True))
                outsider.append(subprocess.Popen(
                    [sys.executable, str(trace_tool), "--endpoint-port", "1",
                     "--output", str(root / "unowned" / "trace-1")],
                    start_new_session=True))

            try:
                with self.assertRaises(RuntimeError):
                    MATRIX.run_campaign(
                        args, fixture_factory=lambda _a, s: FakeFixture(s, removed=True),
                        case_runner=runner)
                summary = json.loads((state / "summary.json").read_text())
                self.assertEqual(summary["status"], "failed")
                self.assertEqual(summary["cases"]["zkool-internal"]["status"], "failed")
                cleanup = summary["cases"]["zkool-internal"]["cleanup"]
                self.assertIs(cleanup["fixture_removed"], False)
                self.assertEqual(cleanup["wallet_survivors"]["found"], 2)
                self.assertEqual(cleanup["wallet_survivors"]["remaining"], 0)
                self.assertEqual(summary["evidence"]["receipts"], {})
                for process in survivor:
                    self.assertIsNotNone(process.poll(), "the owned survivor was not stopped")
                self.assertIsNone(outsider[0].poll(), "an unowned process was stopped")
            finally:
                for process in (*survivor, *outsider):
                    stop_process(process)


if __name__ == "__main__":
    unittest.main(verbosity=2)
