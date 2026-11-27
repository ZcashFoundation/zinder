#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/deploy/bench/postgres-testing-ground/docker-compose.yml"

POSTGRES_VERSIONS="18,17"
ZINDER_IMAGE=""
REPORT_DIR=""

RUN_STATUS="failed"
CURRENT_STAGE="initialization"
FAIL_EXIT_CODE=0

usage() {
  cat <<'EOF'
Usage: scripts/run-postgres-testing-ground.sh [options]

Options:
  --postgres-versions LIST   Comma-separated PG versions to test (default: 18,17; supported: 18, 17)
  --zinder-image IMAGE       Optional Zinder OCI image (e.g. zinder:local) for read-only --readers-only config check
  --report-dir DIR           Directory for report.json and report.md (default: .tmp/postgres-testing-ground-<utc>)
  -h, --help                 Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --postgres-versions)
      POSTGRES_VERSIONS="${2:?missing value for --postgres-versions}"
      shift 2
      ;;
    --zinder-image)
      ZINDER_IMAGE="${2:?missing value for --zinder-image}"
      shift 2
      ;;
    --report-dir)
      REPORT_DIR="${2:?missing value for --report-dir}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${REPORT_DIR}" ]]; then
  REPORT_DIR="${REPO_ROOT}/.tmp/postgres-testing-ground-${TIMESTAMP_UTC}"
fi
mkdir -p "${REPORT_DIR}"
REPORT_DIR="$(cd "${REPORT_DIR}" && pwd)"

# Clear any prior reports or partial stage files so a failing rerun never leaves stale PASSED output.
rm -f "${REPORT_DIR}/report.json" "${REPORT_DIR}/report.md" "${REPORT_DIR}/zinder-image.json" "${REPORT_DIR}"/pg-*.json

PROJECT_NAME="zinder-pg-testing-ground-$$"
export ZINDER_PG_TESTING_GROUND_PROJECT="${PROJECT_NAME}"

write_final_reports() {
  local exit_code="$1"
  python3 - "${REPORT_DIR}" "${TIMESTAMP_UTC}" "${RUN_STATUS}" "${CURRENT_STAGE}" "${exit_code}" <<'PY' || true
import glob
import json
import os
import sys

report_dir, timestamp_utc, run_status, current_stage, raw_code = sys.argv[1:]
exit_code = int(raw_code)
pg_reports = []
for path in sorted(glob.glob(os.path.join(report_dir, "pg-*.json"))):
    try:
        with open(path, encoding="utf-8") as fh:
            pg_reports.append(json.load(fh))
    except Exception:
        pass

zinder_image_path = os.path.join(report_dir, "zinder-image.json")
zinder_image_report = None
if os.path.exists(zinder_image_path):
    try:
        with open(zinder_image_path, encoding="utf-8") as fh:
            zinder_image_report = json.load(fh)
    except Exception:
        pass

failed_stage = None if run_status == "passed" else current_stage
summary = {
    "status": run_status,
    "failed_stage": failed_stage,
    "exit_code": exit_code,
    "generated_at_utc": timestamp_utc,
    "postgres_runs": pg_reports,
    "zinder_image_check": zinder_image_report,
}
with open(os.path.join(report_dir, "report.json"), "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, sort_keys=True)

lines = [
    "# Stateless-Topology Testing Ground Report",
    "",
    f"- Status: **{run_status.upper()}**",
    f"- Generated (UTC): `{timestamp_utc}`",
    f"- Exit code: `{exit_code}`",
]
if failed_stage is not None:
    lines.append(f"- Failed stage: `{failed_stage}`")
lines.extend([
    "",
    "## PostgreSQL + PgBouncer + Mock R2 + Caddy Results",
    "",
    "| PG Version | Pool Mode | Pool Size | Distinct Backend PIDs | HTTP Replicas | gRPC-Web Replicas | Native h2c gRPC Replicas | R2 Fault Fallbacks |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |",
])
for item in pg_reports:
    pool = item["pgbouncer_transaction_pool"]
    l7 = item["l7_round_robin"]
    fallbacks = ", ".join(f"{k}->{v}" for k, v in sorted(item["r2_fault_fallbacks"].items()))
    lines.append(
        f"| {item['postgres_version']} | `{pool['pool_mode']}` | {pool['default_pool_size']} | "
        f"{pool['distinct_backend_pids_observed']} | {', '.join(sorted(set(l7['http_replicas'])))} | "
        f"{', '.join(sorted(set(l7['grpc_web_replicas'])))} | {', '.join(sorted(set(l7.get('grpc_h2c_replicas', []))))} | {fallbacks} |"
    )

if zinder_image_report is not None:
    lines.extend([
        "",
        "## Stateless Reader Image Config Validation",
        "",
        f"- Image: `{zinder_image_report['image']}`",
        f"- Validated `--readers-only` presets (`read_only: true`): `{', '.join(zinder_image_report['validated_presets'])}`",
    ])

with open(os.path.join(report_dir, "report.md"), "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
print(f"Wrote reports ({run_status}) to {report_dir}/report.json and {report_dir}/report.md")
PY
}

on_exit() {
  local exit_code="$?"
  set +e
  if [[ "${RUN_STATUS}" == "passed" ]]; then
    exit_code=0
  elif [[ "${exit_code}" -eq 0 && "${FAIL_EXIT_CODE}" -ne 0 ]]; then
    exit_code="${FAIL_EXIT_CODE}"
  elif [[ "${exit_code}" -eq 0 ]]; then
    exit_code=1
  fi
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
  write_final_reports "${exit_code}"
  exit "${exit_code}"
}
trap on_exit EXIT
trap 'FAIL_EXIT_CODE=130; exit 130' INT
trap 'FAIL_EXIT_CODE=143; exit 143' TERM

image_for_pg_version() {
  case "$1" in
    18)
      echo "postgres:18.7-bookworm@sha256:1757c628698dd1ca67ff7a5fc386bfd91040afc62a45484c346c788b0cbe5402"
      ;;
    17)
      echo "postgres:17.10-bookworm@sha256:9b18b78397054fce88a9552e9d5a3ad5bb7fd258c5b3cc1c5028e46373d6ea8f"
      ;;
    *)
      echo "unsupported postgres version '$1' (expected 18 or 17)" >&2
      return 2
      ;;
  esac
}

allocate_free_ports() {
  python3 - <<'PY'
import socket

socks = []
ports = []
for _ in range(5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    ports.append(s.getsockname()[1])
    socks.append(s)
for s in socks:
    s.close()
print(" ".join(str(p) for p in ports))
PY
}

verify_compose_definitions() {
  CURRENT_STAGE="compose_config_validation"
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" config --quiet
}

verify_postgres_version() {
  local pg_ver="$1"
  local pg_image
  CURRENT_STAGE="pg-${pg_ver}_image_resolution"
  pg_image="$(image_for_pg_version "${pg_ver}")"

  read -r pg_port pgb_port r2_port http_port grpc_port <<<"$(allocate_free_ports)"
  export ZINDER_PG_TESTING_GROUND_POSTGRES_IMAGE="${pg_image}"
  export ZINDER_PG_TG_POSTGRES_PORT="${pg_port}"
  export ZINDER_PG_TG_PGBOUNCER_PORT="${pgb_port}"
  export ZINDER_PG_TG_R2_PORT="${r2_port}"
  export ZINDER_PG_TG_HTTP_PORT="${http_port}"
  export ZINDER_PG_TG_GRPC_PORT="${grpc_port}"

  CURRENT_STAGE="pg-${pg_ver}_compose_up"
  echo "[pg-${pg_ver}] Starting stateless-topology testing ground (${pg_image})..."
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d --wait

  CURRENT_STAGE="pg-${pg_ver}_durability_and_roles"
  echo "[pg-${pg_ver}] Checking PostgreSQL durability & least-privilege roles..."
  local pg_settings
  pg_settings="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_pg_superuser_local_only postgres \
    psql -w -U postgres -d zinder_testing -Atc \
    "SELECT current_setting('fsync'), current_setting('full_page_writes'), current_setting('synchronous_commit'), current_setting('password_encryption'), current_setting('data_checksums');")"
  if [[ "${pg_settings}" != "on|on|on|scram-sha-256|on" ]]; then
    echo "unexpected PostgreSQL settings: ${pg_settings}" >&2
    return 1
  fi

  local scram_count
  scram_count="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_pg_superuser_local_only postgres \
    psql -w -U postgres -d zinder_testing -Atc \
    "SELECT count(*) FROM pg_authid WHERE rolname IN ('zinder_schema_owner','zinder_writer','zinder_reader','zinder_admin') AND rolpassword LIKE 'SCRAM-SHA-256\$%';")"
  if [[ "${scram_count}" != "4" ]]; then
    echo "expected 4 SCRAM-SHA-256 role verifiers, got ${scram_count}" >&2
    return 1
  fi

  local probe_label
  probe_label="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_reader_local_only postgres \
    psql -w -h pgbouncer -p 6432 -U zinder_reader -d zinder_testing -Atc \
    "SELECT epoch_label FROM zinder.topology_probe WHERE id = 1;")"
  if [[ "${probe_label}" != "pgbouncer-transaction-pool-ready" ]]; then
    echo "unexpected reader query via PgBouncer: ${probe_label}" >&2
    return 1
  fi

  if docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_reader_local_only postgres \
    psql -w -h pgbouncer -p 6432 -U zinder_reader -d zinder_testing -Atc \
    "INSERT INTO zinder.topology_probe (id, epoch_label) VALUES (99, 'forbidden');" >/dev/null 2>&1; then
    echo "zinder_reader unexpectedly succeeded inserting into zinder.topology_probe" >&2
    return 1
  fi

  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_writer_local_only postgres \
    psql -w -h pgbouncer -p 6432 -U zinder_writer -d zinder_testing -Atc \
    "INSERT INTO zinder.topology_probe (id, epoch_label) VALUES (2, 'writer-ok'); DELETE FROM zinder.topology_probe WHERE id = 2;" >/dev/null

  CURRENT_STAGE="pg-${pg_ver}_pgbouncer_pooling"
  echo "[pg-${pg_ver}] Checking PgBouncer transaction pool config & concurrency..."
  local pgb_cfg
  pgb_cfg="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_admin_local_only pgbouncer \
    psql -w -h 127.0.0.1 -p 6432 -U zinder_admin -d pgbouncer -Atc "SHOW CONFIG;" | tr '\r' '\n')"
  grep -q '^pool_mode|transaction|' <<<"${pgb_cfg}"
  grep -q '^default_pool_size|4|' <<<"${pgb_cfg}"
  grep -q '^max_client_conn|100|' <<<"${pgb_cfg}"
  grep -q '^auth_type|scram-sha-256|' <<<"${pgb_cfg}"
  grep -q '^max_prepared_statements|0|' <<<"${pgb_cfg}"

  local probe_output probe_rows distinct_pids max_server_conns
  probe_output="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_reader_local_only postgres \
    bash -euo pipefail -c '
      pids=()
      for i in 1 2 3 4 5 6 7 8; do
        psql -v ON_ERROR_STOP=1 -w -h pgbouncer -p 6432 -U zinder_reader -d zinder_testing -Atc \
          "SELECT pg_backend_pid(), count(*) FROM zinder.topology_probe; SELECT pg_sleep(0.05); SELECT pg_backend_pid();" &
        pids+=("$!")
      done
      for pid in "${pids[@]}"; do
        wait "${pid}"
      done
    ' | tr -d '\r')"
  probe_rows="$(awk -F'|' 'NF == 2 {print $1}' <<<"${probe_output}")"
  if [[ "$(wc -l <<<"${probe_rows}")" -ne 8 ]]; then
    echo "PgBouncer concurrent pooling expected 8 probe rows, got:" >&2
    printf '%s\n' "${probe_output}" >&2
    return 1
  fi
  distinct_pids="$(sort -u <<<"${probe_rows}" | wc -l)"
  max_server_conns="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_admin_local_only pgbouncer \
    psql -w -h 127.0.0.1 -p 6432 -U zinder_admin -d pgbouncer -Atc "SHOW POOLS;" | awk -F'|' '$1 == "zinder_testing" {print $5 + $6 + $7; exit}')"
  if [[ -z "${distinct_pids}" || "${distinct_pids}" -lt 1 || "${distinct_pids}" -gt 4 ]]; then
    echo "PgBouncer concurrent pooling distinct backend pids (${distinct_pids}) out of range 1..=4" >&2
    return 1
  fi
  if [[ -n "${max_server_conns}" && "${max_server_conns}" -gt 4 ]]; then
    echo "PgBouncer server connections (${max_server_conns}) exceeded default_pool_size=4" >&2
    return 1
  fi

  CURRENT_STAGE="pg-${pg_ver}_r2_and_l7_proxy"
  echo "[pg-${pg_ver}] Checking Mock R2 immutability/HEAD/faults & Caddy REST/gRPC-Web/h2c routing..."
  local version_report="${REPORT_DIR}/pg-${pg_ver}.json"
  python3 - "${PROJECT_NAME}" "${COMPOSE_FILE}" "${pg_ver}" "${r2_port}" "${http_port}" "${grpc_port}" "${distinct_pids}" "${max_server_conns:-0}" "${version_report}" <<'PY'
import base64
import hashlib
import http.client
import json
import socket
import struct
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

(
    project_name,
    compose_file,
    pg_ver,
    r2_port_str,
    http_port_str,
    grpc_port_str,
    distinct_pids,
    max_server_conns,
    out_path,
) = sys.argv[1:]

r2_port = int(r2_port_str)
r2_base = f"http://127.0.0.1:{r2_port}"
http_base = f"http://127.0.0.1:{http_port_str}"
grpc_base = f"http://127.0.0.1:{grpc_port_str}"

def http_json(method: str, url: str, body_obj: dict | None = None) -> tuple[int, dict, dict]:
    data = json.dumps(body_obj).encode("utf-8") if body_obj is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8")
        return err.code, dict(err.headers), json.loads(raw)

def assert_head_bodyless_persistent(conn: http.client.HTTPConnection, path: str, expected_status: int, headers: dict | None = None) -> None:
    conn.request("HEAD", path, headers=headers or {})
    resp = conn.getresponse()
    body = resp.read()
    assert resp.status == expected_status, f"HEAD {path}: expected {expected_status}, got {resp.status}"
    assert body == b"", f"HEAD {path}: expected empty body, got {len(body)} bytes: {body!r}"

# 1. Inspect probe containers for ReadOnlyRootfs and zero persistent volumes.
for svc in ("probe-reader-a", "probe-reader-b"):
    cid = subprocess.check_output(
        ["docker", "compose", "-p", project_name, "-f", compose_file, "ps", "-q", svc],
        text=True,
    ).strip()
    inspected = json.loads(subprocess.check_output(["docker", "inspect", cid], text=True))[0]
    assert inspected["HostConfig"]["ReadonlyRootfs"] is True, f"{svc} must have ReadonlyRootfs=true"
    binds = [m for m in inspected["Mounts"] if m["Type"] != "bind" or not m["Source"].endswith("probe_replica.py")]
    assert not binds, f"{svc} must have zero persistent volumes, got {binds}"

# 2. Check /healthz & /readyz across proxy (readyz must verify PgBouncer TCP reachability).
for path in ("/healthz", "/readyz"):
    code, _, body = http_json("GET", f"{http_base}{path}")
    assert code == 200 and body["readonly_rootfs"] is True and body["local_db_files"] == [], body
    if path == "/readyz":
        assert body["pgbouncer_tcp_reachable"] is True, body

# 3. Mock R2 immutable PUT / GET / Range / HEAD persistence / fault injection.
payload = b"zinder-settled-segment-0001-" * 8
expected_sha = hashlib.sha256(payload).hexdigest()
req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", data=payload, method="PUT")
with urllib.request.urlopen(req, timeout=5.0) as resp:
    assert resp.status == 201
    put_body = json.loads(resp.read().decode("utf-8"))
    assert put_body["sha256"] == expected_sha

# Identical PUT retry is idempotent (200) and does not double-count puts.
_, _, metrics_after_first_put = http_json("GET", f"{r2_base}/metrics")
retry_req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", data=payload, method="PUT")
with urllib.request.urlopen(retry_req, timeout=5.0) as resp:
    assert resp.status == 200
    retry_body = json.loads(resp.read().decode("utf-8"))
    assert retry_body["sha256"] == expected_sha and retry_body.get("idempotent") is True

# Conflicting PUT to an existing key fails with 409 Conflict without mutating origin payload or puts.
conflict_req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", data=b"tampered-payload", method="PUT")
try:
    urllib.request.urlopen(conflict_req, timeout=5.0)
    raise AssertionError("conflicting PUT to existing segment key must return 409")
except urllib.error.HTTPError as err:
    assert err.code == 409, f"expected 409 Conflict, got {err.code}"
    conflict_body = json.loads(err.read().decode("utf-8"))
    assert conflict_body["existing_sha256"] == expected_sha
_, _, metrics_after_conflict = http_json("GET", f"{r2_base}/metrics")
assert metrics_after_conflict["puts"] == metrics_after_first_put["puts"], (metrics_after_first_put, metrics_after_conflict)

# Interrupted PUT must not persist a truncated immutable object and must allow a complete retry.
truncated_key = "seg-truncated.bin"
with socket.create_connection(("127.0.0.1", r2_port), timeout=5.0) as sock:
    sock.sendall(
        (
            f"PUT /segments/{truncated_key} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{r2_port}\r\n"
            "Content-Length: 64\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        + b"0123456789"
    )
    sock.shutdown(socket.SHUT_WR)
    raw_reply = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        raw_reply += chunk
    if raw_reply:
        status_line = raw_reply.split(b"\r\n", 1)[0]
        assert b" 400 " in status_line, status_line
try:
    urllib.request.urlopen(urllib.request.Request(f"{r2_base}/segments/{truncated_key}"), timeout=5.0)
    raise AssertionError("truncated PUT unexpectedly persisted segment object")
except urllib.error.HTTPError as err:
    assert err.code == 404, f"expected 404 after truncated PUT, got {err.code}"
complete_truncated_payload = b"zinder-complete-segment-after-interrupted-upload-00000000000000!"
assert len(complete_truncated_payload) == 64
with urllib.request.urlopen(
    urllib.request.Request(
        f"{r2_base}/segments/{truncated_key}",
        data=complete_truncated_payload,
        method="PUT",
    ),
    timeout=5.0,
) as resp:
    assert resp.status == 201, f"expected 201 on complete retry, got {resp.status}"

with urllib.request.urlopen(urllib.request.Request(f"{r2_base}/segments/seg-0001.bin"), timeout=5.0) as resp:
    assert resp.status == 200
    assert resp.read() == payload
    assert resp.headers.get("X-Zinder-Segment-Sha256") == expected_sha

range_req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", headers={"Range": "bytes=0-15"})
with urllib.request.urlopen(range_req, timeout=5.0) as resp:
    assert resp.status == 206
    assert resp.read() == payload[:16]
    assert resp.headers.get("Content-Range") == f"bytes 0-15/{len(payload)}"

# HEAD requests over one persistent HTTP/1.1 connection must write zero body bytes on errors and success.
conn = http.client.HTTPConnection("127.0.0.1", r2_port, timeout=5.0)
try:
    assert_head_bodyless_persistent(conn, "/segments/seg-0001.bin", 200)
    assert_head_bodyless_persistent(conn, "/segments/missing-segment.bin", 404)
    assert_head_bodyless_persistent(conn, "/segments/seg-0001.bin", 416, headers={"Range": "bytes=99999-100000"})
    http_json("POST", f"{r2_base}/fault", {"mode": "miss_404", "delay_ms": 0})
    assert_head_bodyless_persistent(conn, "/segments/seg-0001.bin", 404)
    http_json("POST", f"{r2_base}/fault", {"mode": "outage_503", "delay_ms": 0})
    assert_head_bodyless_persistent(conn, "/segments/seg-0001.bin", 503)
    http_json("POST", f"{r2_base}/fault", {"mode": "normal", "delay_ms": 0})
    # Persistent connection must remain cleanly usable for a subsequent GET after error HEAD responses.
    conn.request("GET", "/segments/seg-0001.bin")
    follow_resp = conn.getresponse()
    follow_body = follow_resp.read()
    assert follow_resp.status == 200 and follow_body == payload
finally:
    conn.close()

# Invalid /fault must fail closed (400) without mutating fault state.
for bad in ({"mode": "bogus"}, {"mode": "timeout_ms", "delay_ms": -1}):
    bad_code, _, _ = http_json("POST", f"{r2_base}/fault", bad)
    assert bad_code == 400, f"expected 400 on invalid fault {bad}, got {bad_code}"
code, _, fault_state = http_json("GET", f"{r2_base}/fault")
assert code == 200 and fault_state == {"mode": "normal", "delay_ms": 0}

code, _, hit_probe = http_json("GET", f"{http_base}/probe/r2-segment/seg-0001.bin?timeout_ms=250")
assert code == 200 and hit_probe["source"] == "r2_cache" and hit_probe["sha256"] == expected_sha

fallback_results = {}
for fault_cfg, expected_reason in [
    ({"mode": "miss_404", "delay_ms": 0}, "http_404"),
    ({"mode": "outage_503", "delay_ms": 0}, "http_503"),
    ({"mode": "timeout_ms", "delay_ms": 800}, "timeout"),
    ({"mode": "corrupt_payload", "delay_ms": 0}, "digest_mismatch"),
]:
    c, _, _ = http_json("POST", f"{r2_base}/fault", fault_cfg)
    assert c == 200
    c, _, fb = http_json("GET", f"{http_base}/probe/r2-segment/seg-0001.bin?timeout_ms=150")
    assert c == 200 and fb["source"] == "primary_fallback", fb
    assert fb["fallback_reason"] == expected_reason and fb["sha256"] == expected_sha, fb
    fallback_results[fault_cfg["mode"]] = fb["fallback_reason"]

http_json("POST", f"{r2_base}/fault", {"mode": "normal", "delay_ms": 0})
_, _, r2_metrics = http_json("GET", f"{r2_base}/metrics")
assert r2_metrics["hits"] >= 3 and r2_metrics["range_hits"] >= 1
assert r2_metrics["misses"] >= 2 and r2_metrics["outage_503"] >= 2
assert r2_metrics["timeout_injections"] >= 1 and r2_metrics["corrupt_injections"] >= 1

# 4. Caddy round-robin over HTTP REST and gRPC-Web (binary + text).
http_replicas = []
for _ in range(10):
    _, hdrs, body = http_json("GET", f"{http_base}/whoami")
    http_replicas.append(hdrs.get("X-Zinder-Replica") or body["replica_id"])
assert set(http_replicas) == {"probe-reader-a", "probe-reader-b"}, http_replicas

grpc_web_replicas = []
frame = b"\x00\x00\x00\x00\x00"
for ct, raw in [
    ("application/grpc-web+proto", frame),
    ("application/grpc-web-text+proto", base64.b64encode(frame)),
] * 3:
    req = urllib.request.Request(
        f"{grpc_base}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock",
        data=raw,
        headers={"Content-Type": ct},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        assert resp.status == 200
        assert ct.split("+", 1)[0] in resp.headers.get("Content-Type", "")
        wire = resp.read()
        decoded = base64.b64decode(wire) if "grpc-web-text" in ct else wire
        assert decoded[0] == 0
        msg_len = struct.unpack(">I", decoded[1:5])[0]
        payload_end = 5 + msg_len
        assert msg_len > 0 and len(decoded) >= payload_end, len(decoded)
        trailer_bytes = decoded[payload_end:]
        assert len(trailer_bytes) >= 5 and trailer_bytes[0] == 0x80, f"missing/invalid grpc-web trailers frame: {trailer_bytes!r}"
        trailer_len = struct.unpack(">I", trailer_bytes[1:5])[0]
        assert len(trailer_bytes) == 5 + trailer_len, f"incomplete/trailing grpc-web trailers frame: {trailer_bytes!r}"
        trailer_map = {}
        for line in trailer_bytes[5:].decode("ascii").split("\r\n"):
            if line:
                name, val = line.split(":", 1)
                trailer_map[name.strip().lower()] = val.strip()
        assert trailer_map.get("grpc-status") == "0", trailer_map
        grpc_web_replicas.append(resp.headers.get("X-Zinder-Replica"))

assert set(grpc_web_replicas) == {"probe-reader-a", "probe-reader-b"}, grpc_web_replicas

# 5. Native HTTP/2 h2c gRPC (application/grpc + HTTP trailers) through Caddy :9067.
grpc_h2c_replicas = []
with tempfile.TemporaryDirectory() as tmpd:
    req_bin = f"{tmpd}/grpc-req.bin"
    with open(req_bin, "wb") as fh:
        fh.write(frame)
    for idx in range(6):
        hdr_file = f"{tmpd}/hdr-{idx}.txt"
        body_file = f"{tmpd}/body-{idx}.bin"
        status_code = subprocess.check_output(
            [
                "curl",
                "-sS",
                "--http2-prior-knowledge",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/grpc",
                "-H",
                "TE: trailers",
                "--data-binary",
                f"@{req_bin}",
                "-D",
                hdr_file,
                "-o",
                body_file,
                "-w",
                "%{http_code}",
                f"{grpc_base}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock",
            ],
            text=True,
        ).strip()
        assert status_code == "200", f"h2c gRPC call {idx} returned HTTP {status_code}"
        raw_hdrs = open(hdr_file, encoding="utf-8", errors="replace").read().lower()
        assert "content-type: application/grpc" in raw_hdrs, raw_hdrs
        assert "grpc-status: 0" in raw_hdrs, f"missing trailing grpc-status: 0 in:\n{raw_hdrs}"
        replica_hdr = None
        for line in raw_hdrs.splitlines():
            if line.startswith("x-zinder-replica:"):
                replica_hdr = line.split(":", 1)[1].strip()
        assert replica_hdr in {"probe-reader-a", "probe-reader-b"}, raw_hdrs
        body_bytes = open(body_file, "rb").read()
        assert len(body_bytes) >= 5 and body_bytes[0] == 0
        msg_len = struct.unpack(">I", body_bytes[1:5])[0]
        assert len(body_bytes) == 5 + msg_len and msg_len > 0
        grpc_h2c_replicas.append(replica_hdr)

assert set(grpc_h2c_replicas) == {"probe-reader-a", "probe-reader-b"}, grpc_h2c_replicas

report = {
    "postgres_version": pg_ver,
    "pg_durability_scram_checksums": "passed",
    "pgbouncer_transaction_pool": {
        "pool_mode": "transaction",
        "default_pool_size": 4,
        "distinct_backend_pids_observed": int(distinct_pids),
        "max_pool_servers": int(max_server_conns),
    },
    "mock_r2_metrics": r2_metrics,
    "r2_fault_fallbacks": fallback_results,
    "l7_round_robin": {
        "http_replicas": http_replicas,
        "grpc_web_replicas": grpc_web_replicas,
        "grpc_h2c_replicas": grpc_h2c_replicas,
    },
}
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(report, fh, indent=2, sort_keys=True)
PY

  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null
}

verify_zinder_image_readers_only() {
  CURRENT_STAGE="zinder_image_readers_only_validation"
  local zinder_out="${REPORT_DIR}/zinder-image.json"
  python3 - "${ZINDER_IMAGE}" "${zinder_out}" <<'PY'
import json
import subprocess
import sys

image, out_path = sys.argv[1:]
validated_presets = []
for preset in ("light-wallet", "wallet", "explorer"):
    subprocess.check_call(
        [
            "docker", "run", "--rm", "--read-only",
            "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m",
            "-e", f"ZINDER_DEPLOYMENT__PROFILE={preset}",
            "-e", "ZINDER_NETWORK__NAME=zcash-regtest",
            "-e", "ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232",
            "-e", "ZINDER_NODE__AUTH__METHOD=none",
            "-e", "ZINDER_STORAGE__DATA_DIR=/tmp/zinder-reader",
            "-e", "ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS=120000",
            "-e", "ZINDER_INGEST_CONTROL__ADDR=http://127.0.0.1:9100",
            "-e", "ZINDER_OPS__LISTEN_ADDR=0.0.0.0:9108",
            "-e", "ZINDER_COMPAT__LISTEN_ADDR=0.0.0.0:9067",
            "-e", "ZINDER_QUERY__LISTEN_ADDR=0.0.0.0:9102",
            "-e", "ZINDER_EXPLORER__LISTEN_ADDR=0.0.0.0:9068",
            "-e", "ZINDER_CIPHERSCAN__LISTEN_ADDR=0.0.0.0:9070",
            "-e", "ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true",
            image, "config", "validate", "--preset", preset, "--readers-only",
        ],
        stdout=subprocess.DEVNULL,
    )
    validated_presets.append(preset)

with open(out_path, "w", encoding="utf-8") as fh:
    json.dump({"image": image, "validated_presets": validated_presets}, fh, indent=2, sort_keys=True)
PY
}

cd "${REPO_ROOT}"

CURRENT_STAGE="postgres_version_validation"
IFS=',' read -r -a VERSION_ARRAY <<<"${POSTGRES_VERSIONS}"
EFFECTIVE_VERSIONS=()
for raw_ver in "${VERSION_ARRAY[@]}"; do
  pg_ver="$(echo "${raw_ver}" | tr -d '[:space:]')"
  if [[ -n "${pg_ver}" ]]; then
    EFFECTIVE_VERSIONS+=("${pg_ver}")
  fi
done
if (( ${#EFFECTIVE_VERSIONS[@]} == 0 )); then
  echo "--postgres-versions must specify at least one version (supported: 18, 17)" >&2
  exit 2
fi

verify_compose_definitions

for pg_ver in "${EFFECTIVE_VERSIONS[@]}"; do
  verify_postgres_version "${pg_ver}"
done

if [[ -n "${ZINDER_IMAGE}" ]]; then
  echo "[readers] Verifying ${ZINDER_IMAGE} --readers-only presets under --read-only..."
  verify_zinder_image_readers_only
fi

CURRENT_STAGE="complete"
RUN_STATUS="passed"
