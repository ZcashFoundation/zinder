#!/usr/bin/env bash
# Verify an already-running loopback route with explicit trust and controls.
set -euo pipefail
if [[ $# -ne 4 ]]; then
  echo "usage: $0 <tls-directory> <tls-port> <direct-grpc-port> <new-evidence-directory>" >&2
  exit 2
fi
tls=$(realpath "$1")
port=$2
direct=$3
out=$4
[[ $port =~ ^[0-9]+$ && $direct =~ ^[0-9]+$ ]] || exit 2
umask 077
mkdir -- "$out"
grpcurl=${GRPCURL_BIN:-grpcurl}
rpc=cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
tip_rpc=cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock
"$grpcurl" -version >"$out/grpcurl-version.txt" 2>&1
openssl version >"$out/openssl-version.txt"
openssl x509 -in "$tls/server.pem" -noout -subject -issuer -serial -dates -fingerprint -sha256 -ext subjectAltName >"$out/certificate.txt"
timeout 15 openssl s_client -connect "127.0.0.1:$port" -servername localhost \
  -CAfile "$tls/ca.pem" -verify_hostname localhost -verify_return_error -alpn h2 \
  </dev/null 2>&1 | awk '/Verification:|Verified peername:|ALPN protocol:|Verify return code:|Protocol:|verify error/ {print}' >"$out/handshake.log"
rg -q 'ALPN protocol: h2' "$out/handshake.log"
"$grpcurl" -max-time 10 -cacert "$tls/ca.pem" -authority localhost -d '{}' \
  "127.0.0.1:$port" "$rpc" >"$out/trusted.json" 2>"$out/trusted.stderr"
"$grpcurl" -max-time 10 -plaintext -d '{}' "127.0.0.1:$direct" "$rpc" \
  >"$out/direct.json" 2>"$out/direct.stderr"
"$grpcurl" -max-time 10 -cacert "$tls/ca.pem" -authority localhost -d '{}' \
  "127.0.0.1:$port" "$tip_rpc" >"$out/trusted-tip.json" 2>"$out/trusted-tip.stderr"
"$grpcurl" -max-time 10 -plaintext -d '{}' "127.0.0.1:$direct" "$tip_rpc" \
  >"$out/direct-tip.json" 2>"$out/direct-tip.stderr"
# Negatives must fail for the intended reason, not just a closed listener.
if "$grpcurl" -max-time 10 -authority localhost -d '{}' "127.0.0.1:$port" "$rpc" >"$out/untrusted.log" 2>&1; then
  echo 'untrusted CA accepted' >&2; exit 1
fi
rg -qi 'unknown authority|unknown issuer' "$out/untrusted.log"
if "$grpcurl" -max-time 10 -cacert "$tls/ca.pem" -authority wrong.invalid -d '{}' "127.0.0.1:$port" "$rpc" >"$out/hostname.log" 2>&1; then
  echo 'wrong hostname accepted' >&2; exit 1
fi
# Keep SNI valid so the server supplies its certificate, then independently
# verify the wrong hostname instead of confusing SNI refusal with name checks.
if timeout 15 openssl s_client -connect "127.0.0.1:$port" -servername localhost \
  -CAfile "$tls/ca.pem" -verify_hostname wrong.invalid -verify_return_error -alpn h2 \
  </dev/null 2>&1 | awk '/Verification:|verify error|Verification error/ {print}' >"$out/hostname-verification.log"; then
  echo 'wrong certificate hostname accepted' >&2; exit 1
fi
rg -qi 'hostname mismatch' "$out/hostname-verification.log"
if "$grpcurl" -max-time 5 -plaintext -d '{}' "127.0.0.1:$port" "$rpc" >"$out/plaintext.log" 2>&1; then
  echo 'plaintext accepted on TLS listener' >&2; exit 1
fi
# Recheck the positive to exclude a dead listener explaining the controls.
"$grpcurl" -max-time 10 -cacert "$tls/ca.pem" -authority localhost -d '{}' \
  "127.0.0.1:$port" "$rpc" >"$out/trusted-after.json"
"$grpcurl" -max-time 10 -cacert "$tls/ca.pem" -authority localhost -d '{}' \
  "127.0.0.1:$port" "$tip_rpc" >"$out/trusted-tip-after.json" 2>"$out/trusted-tip-after.stderr"
python3 - "$out" "$port" "$direct" <<'PY'
import base64
import json
from pathlib import Path
import sys
out = Path(sys.argv[1])
a, b, c = (json.loads((out / name).read_text()) for name in ('trusted.json', 'direct.json', 'trusted-after.json'))
identity_fields = (
    'vendor', 'version', 'gitCommit', 'chainName', 'saplingActivationHeight',
    'consensusBranchId', 'lightwalletProtocolVersion', 'taddrSupport',
)
for reply in (a, b, c):
    if any(field not in reply for field in identity_fields):
        raise SystemExit('GetLightdInfo is missing a pinned identity field')
    if reply['taddrSupport'] is not True:
        raise SystemExit('wallet serving candidate must support transparent history')
identity = {field: a[field] for field in identity_fields}
if any({field: reply[field] for field in identity_fields} != identity for reply in (b, c)):
    raise SystemExit('TLS/direct server identity changed')

def height(reply, field):
    raw = reply.get(field)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw).isascii() or not str(raw).isdecimal():
        raise SystemExit(f'missing or invalid {field}')
    return int(raw)

def tip(reply):
    try:
        block_hash = base64.b64decode(reply.get('hash', ''), validate=True)
    except (ValueError, TypeError) as error:
        raise SystemExit(f'invalid tip hash: {error}') from error
    if len(block_hash) != 32:
        raise SystemExit('tip hash must contain 32 bytes')
    return height(reply, 'height'), block_hash.hex()

tips = [tip(json.loads((out / name).read_text())) for name in (
    'trusted-tip.json', 'direct-tip.json', 'trusted-tip-after.json',
)]
if tips[1:] != tips[:1] * 2:
    raise SystemExit('TLS/direct tip height or hash changed; use a frozen fixture')
if any(height(reply, 'blockHeight') != tips[0][0] for reply in (a, b, c)):
    raise SystemExit('GetLightdInfo height differs from the pinned tip')
(out / 'result.json').write_text(json.dumps({
    'status': 'passed', 'endpoint': f'https://localhost:{sys.argv[2]}',
    'direct_endpoint': f'http://127.0.0.1:{sys.argv[3]}',
    'trust': 'explicit lab CA', 'hostname': 'localhost', 'alpn': 'h2',
    'untrusted_ca_rejected': True, 'wrong_hostname_rejected': True,
    'plaintext_rejected': True, 'direct_identity_equal': True,
    'identity': identity, 'tip_height': tips[0][0], 'tip_hash': tips[0][1],
    'direct_tip_equal': True,
    'estimated_heights': [reply.get('estimatedHeight') for reply in (a, b, c)],
    'client': 'grpcurl', 'fallback_used': False,
    'wallet_app_no_fallback': 'not assessed',
}, indent=2) + '\n')
PY
printf 'TLS controls passed: %s/result.json\n' "$out"
