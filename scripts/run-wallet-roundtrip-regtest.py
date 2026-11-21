#!/usr/bin/env python3
"""Run and audit a signed wallet round trip on two fresh, disposable Regtest fixtures."""
import argparse
import json
import os
from pathlib import Path
import secrets
import select
import signal
import socket
import subprocess
import sys
import time


SCRIPTS = Path(__file__).resolve().parent
FIXTURE = SCRIPTS / "regtest-fixture.py"
WALLET = SCRIPTS / "run-wallet-roundtrip.py"
AUDIT = SCRIPTS / "wallet-clients/audit-wallet-roundtrip.py"
CLIENTS = SCRIPTS / "wallet-clients"
WALLET_PORT_OFFSET = 82
DEFAULT_STAGE_TIMEOUT = 5400
DEFAULT_STAGE_GRACE = 30
CLEANUP_TIMEOUT = 600
DOCKER_TIMEOUT = 60


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def check_ports(bases):
    offsets = (2, 5, 6, 7, 10, 32, 67, WALLET_PORT_OFFSET)
    ports = [base + offset for base in bases for offset in offsets]
    if min(bases) < 1024 or max(ports) > 65535 or len(set(ports)) != len(ports):
        raise ValueError("choose two distinct port bases with disjoint fixture and wallet ports")
    for port in ports:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))


def process_alive(pid):
    try:
        state = (Path("/proc") / str(pid) / "stat").read_bytes().rsplit(b")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
        return False
    return state != b"Z"


def kill_remaining_members(leader, membership, grace):
    # The unreaped leader reserves the group or session ID while members exit.
    deadline = time.monotonic() + grace
    while True:
        live = False
        for entry in Path("/proc").iterdir():
            if not entry.name.isdecimal():
                continue
            pid = int(entry.name)
            try:
                if membership(pid) != leader or not process_alive(pid):
                    continue
                fd = os.pidfd_open(pid)
                try:
                    if membership(pid) == leader:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                        live = True
                finally:
                    os.close(fd)
            except ProcessLookupError:
                continue
        if not live:
            return
        require(time.monotonic() < deadline, "owned processes remain after SIGKILL")
        time.sleep(.02)


def stop_group(proc, first, grace, whole_session=False):
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, first)
        deadline = time.monotonic() + grace
        # Keep the leader unreaped so its process group cannot be recycled during escalation.
        while time.monotonic() < deadline and not os.waitid(
                os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT):
            time.sleep(.05)
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        kill_remaining_members(proc.pid, os.getsid if whole_session else os.getpgid, grace)
    finally:
        proc.wait(timeout=grace)


def execute(script, args, log, timeout, grace):
    cmd = [sys.executable, "-B", *(["-O"] * sys.flags.optimize), str(script), *map(str, args)]
    with log.open("x") as output:
        proc = subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_group(proc, signal.SIGTERM, grace)
            raise RuntimeError(f"{script.name} exceeded its {timeout}s stage limit; see {log}") from None
        except BaseException:
            stop_group(proc, signal.SIGINT, grace)
            raise
    if code:
        raise RuntimeError(f"{script.name} stopped with exit {code}. See {log}")


def stop_owned_process(identity, read_identity, grace):
    try:
        fd = os.pidfd_open(identity[0])
    except ProcessLookupError:
        return
    try:
        if read_identity() != identity:
            return
        exited = select.poll()
        exited.register(fd, select.POLLIN)
        for sig in (signal.SIGTERM, signal.SIGKILL):
            signal.pidfd_send_signal(fd, sig)
            if exited.poll(int(grace * 1000)):
                return
        raise RuntimeError("owned process did not exit before the cleanup deadline")
    except ProcessLookupError:
        pass
    finally:
        os.close(fd)


def close_owned_wallet(wallet, base, binary, grace):
    """Stop an exact detached Zallet left by an interrupted stage, without signalling anything else."""
    directory = wallet / "zallet"
    if not (directory / "daemon.pid").is_file() or not (directory / "zallet.toml").is_file():
        return False
    network = json.loads((directory / "network.json").read_text())
    sys.path.insert(0, str(CLIENTS))
    from roundtrip_wallets import Zallet
    client = Zallet(Path(binary).resolve(), directory, network["network"], network["endpoint"], 1,
                    base + WALLET_PORT_OFFSET, warmup_timeout=60)
    identity = client._owned_identity()
    if identity is None:
        return False
    try:
        client.close()
    except (OSError, RuntimeError):
        stop_owned_process(identity, client._owned_identity, grace)
    require(client._owned_identity() != identity, "owned Zallet daemon did not stop")
    return True


def owned_relay(fixture):
    """Identify the fixture's detached relay by its owner token, never by a bare PID."""
    try:
        state = json.loads((fixture / "state.json").read_text())
    except (OSError, ValueError):
        return None
    pid, owner = state.get("relay_pid"), state.get("owner")
    if type(pid) is not int or pid <= 0 or not isinstance(owner, str) or not owner:
        return None
    try:
        proc = Path("/proc") / str(pid)
        if proc.stat().st_uid != os.geteuid():
            return None
        argv = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    expected = [os.fsencode(SCRIPTS / "regtest-fixture-relay.py"), os.fsencode(fixture / "relay.json"), owner.encode()]
    if argv[1:] != expected or not process_alive(pid):
        return None
    return pid, owner


def stop_owned_relay(fixture, grace):
    """Stop the fixture's own relay if a failed teardown left it running."""
    identity = owned_relay(fixture)
    if identity is None:
        return
    stop_owned_process(identity, lambda: owned_relay(fixture), grace)


def require_project_gone(project):
    """Read back only this Compose project's containers, networks, and volumes."""
    remaining = {}
    for kind, listing in (("container", ("ps", "-aq")), ("network", ("network", "ls", "-q")),
                          ("volume", ("volume", "ls", "-q"))):
        result = subprocess.run(["docker", *listing, "--filter", f"label=com.docker.compose.project={project}"],
                                capture_output=True, text=True, timeout=DOCKER_TIMEOUT)
        require(result.returncode == 0, f"docker {kind} readback failed for owned project {project}")
        if result.stdout.split():
            remaining[kind] = result.stdout.split()
    require(not remaining, f"owned Docker resources remain for {project}: {sorted(remaining)}")


def clean_fixture(label, project, fixture, operator_args, base, args, state, statuses):
    """Always attempt the owned Zallet, relay, and Compose project; never mask an earlier failure."""
    problems = []
    try:
        close_owned_wallet(state / label, base, args.zallet_backend, args.stage_grace)
    except (Exception, KeyboardInterrupt) as error:
        problems.append(f"owned Zallet cleanup failed: {error}")
    started = (fixture / "state.json").is_file()
    if started:
        try:
            execute(FIXTURE, ["down", *operator_args, "--project", project], state / f"{label}-down.log",
                    CLEANUP_TIMEOUT, args.stage_grace)
        except (Exception, KeyboardInterrupt) as error:
            problems.append(f"fixture teardown failed: {error}")
    try:
        stop_owned_relay(fixture, args.stage_grace)
        require_project_gone(project)
    except (Exception, KeyboardInterrupt) as error:
        problems.append(str(error))
    statuses[label] = "removed" if started else "no fixture started"
    if problems:
        statuses[label] = "cleanup failed: " + "; ".join(problems)


def campaign(args):
    require(args.stage_timeout > 0 and args.stage_grace > 0, "stage timeout and grace must be positive")
    state = args.state.resolve()
    check_ports(args.port_bases)
    state.mkdir(mode=0o700, parents=True, exist_ok=False)
    operator_args = ["--state-root", state / "projects"]
    owned = []
    audit_args = []
    receipts = []
    try:
        for label, base in zip(("first", "second"), args.port_bases, strict=True):
            project = "zinder-fixture-roundtrip-" + secrets.token_hex(8)
            fixture = state / "projects" / project
            owned.append((label, project, fixture, base))
            up = ["up", *operator_args, "--project", project, "--port-base", base, "--images", args.images,
                  "--revision", args.revision, "--node-config", SCRIPTS.parent / "deploy/zebra/regtest-nu6-at-one.toml",
                  "--raw-blob-policy", "all", "--tip", 150, "--bin-dir", args.bin_dir]
            execute(FIXTURE, up, state / f"{label}-up.log", args.stage_timeout, args.stage_grace)
            ready = fixture / "ready.initial.json"
            wallet = state / label
            execute(WALLET, ["regtest", "--ready", ready, "--state", wallet, "--revision", args.revision,
                             "--zingo", args.zingo, "--zallet-backend", args.zallet_backend,
                             "--wallet-rpc-port", base + WALLET_PORT_OFFSET],
                    state / f"{label}-wallet.log", args.stage_timeout, args.stage_grace)
            receipts.append(wallet / "receipt.json")
            audit_args.extend(["--run", wallet, ready])
        execute(WALLET, ["compare", *receipts], state / "compare.log", args.stage_timeout, args.stage_grace)
        (state / "compare.log").rename(state / "paired.json")
        execute(AUDIT, ["--revision", args.revision, *audit_args, "--output", state / "canonical-audit.json"],
                state / "audit.log", args.stage_timeout, args.stage_grace)
    finally:
        original = sys.exception()
        statuses = {}
        for label, project, fixture, base in reversed(owned):
            clean_fixture(label, project, fixture, operator_args, base, args, state, statuses)
        (state / "cleanup.json").write_text(json.dumps(statuses, indent=2) + "\n")
        message = f"fixture cleanup incomplete. See {state / 'cleanup.json'}"
        if original is None:
            require(statuses == {"first": "removed", "second": "removed"}, message)
        elif any(value.startswith("cleanup failed") for value in statuses.values()):
            original.add_note(message)
    print(f"Two Regtest round trips passed and both fixtures were removed. Evidence: {state}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True, help="new private evidence directory")
    parser.add_argument("--port-bases", nargs=2, type=int, required=True, metavar=("FIRST", "SECOND"))
    parser.add_argument("--images", type=Path, required=True, help="fixture operator image map")
    parser.add_argument("--bin-dir", type=Path, required=True, help="release binaries to mount over the image set")
    parser.add_argument("--revision", required=True, help="expected full Zinder release commit")
    parser.add_argument("--zingo", type=Path, required=True)
    parser.add_argument("--zallet-backend", type=Path, required=True)
    parser.add_argument("--stage-timeout", type=int, default=DEFAULT_STAGE_TIMEOUT,
                        help="maximum seconds for one fixture or wallet stage (default: %(default)s)")
    parser.add_argument("--stage-grace", type=int, default=DEFAULT_STAGE_GRACE,
                        help="seconds to wait after each termination signal (default: %(default)s)")
    args = parser.parse_args()
    os.umask(0o077)
    def interrupt(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    campaign(args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as error:
        sys.exit(f"Regtest round trip failed: {error}")
