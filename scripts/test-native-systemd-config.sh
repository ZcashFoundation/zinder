#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
native="$repo_root/deploy/systemd/native"

# Check the shipped env file and the per-unit settings independently. A wildcard,
# non-loopback IP, hostname, or unknown value fails closed in this test.
python3 - "$native" <<'PY'
import pathlib
import re
import sys

native = pathlib.Path(sys.argv[1])
env = native / "zinder.env"
if "ZINDER_SECURITY__ALLOW_PUBLIC_BIND=false" not in env.read_text().splitlines():
    sys.exit("Native defaults must explicitly disable public binds")

ports = set()
for path in [env, *sorted(native.glob("*.service"))]:
    listeners = 0
    for line in path.read_text().splitlines():
        if line.lstrip().startswith("#"):
            continue
        for match in re.finditer(r"ZINDER_\w+__(LISTEN_ADDR|ALLOW_PUBLIC_BIND)=([^\s]*)", line):
            key, setting = match.groups()
            if key == "ALLOW_PUBLIC_BIND":
                if setting != "false":
                    sys.exit(f"{path.name}: public bind enabled")
                continue
            address, sep, port = setting.rpartition(":")
            if not sep or address not in ("127.0.0.1", "[::1]") or not port.isdecimal():
                sys.exit(f"{path.name}: expected a loopback socket in {setting!r}")
            if not 0 < int(port) <= 65535 or int(port) in ports:
                sys.exit(f"{path.name}: invalid or duplicate port {port}")
            ports.add(int(port))
            listeners += 1
    if listeners == 0:
        sys.exit(f"{path.name}: no explicit listener settings found")
print("Native systemd configuration uses distinct loopback listeners")
PY
