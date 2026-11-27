#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/deploy/bench/postgres-testing-ground/docker-compose.yml"

POSTGRES_VERSIONS="18,17"
ZINDER_IMAGE=""
REPORT_DIR=""

usage() {
  cat <<'EOF'
Usage: scripts/run-postgres-testing-ground.sh [options]

Options:
  --postgres-versions LIST   Comma-separated PG versions to test (default: 18,17; supported: 18, 17)
  --zinder-image IMAGE       Optional Zinder OCI image (e.g. zinder:local) to verify --readers-only config & compose profile
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

PROJECT_NAME="zinder-pg-testing-ground-$$"
export ZINDER_PG_TESTING_GROUND_PROJECT="${PROJECT_NAME}"

cleanup() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --profile readers down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

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
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" config --quiet
  ZINDER_IMAGE="${ZINDER_IMAGE:-zinder:local}" \
    docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --profile readers config --quiet
}

verify_postgres_version() {
  local pg_ver="$1"
  local pg_image
  pg_image="$(image_for_pg_version "${pg_ver}")"

  read -r pg_port pgb_port r2_port http_port grpc_port <<<"$(allocate_free_ports)"
  export ZINDER_PG_TESTING_GROUND_POSTGRES_IMAGE="${pg_image}"
  export ZINDER_PG_TG_POSTGRES_PORT="${pg_port}"
  export ZINDER_PG_TG_PGBOUNCER_PORT="${pgb_port}"
  export ZINDER_PG_TG_R2_PORT="${r2_port}"
  export ZINDER_PG_TG_HTTP_PORT="${http_port}"
  export ZINDER_PG_TG_GRPC_PORT="${grpc_port}"

  echo "[pg-${pg_ver}] Starting stateless-topology testing ground (${pg_image})..."
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --profile readers down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d --wait

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

  echo "[pg-${pg_ver}] Checking PgBouncer transaction pool config & concurrency..."
  local pgb_cfg
  pgb_cfg="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_admin_local_only pgbouncer \
    psql -w -h 127.0.0.1 -p 6432 -U zinder_admin -d pgbouncer -Atc "SHOW CONFIG;" | tr '\r' '\n')"
  grep -q '^pool_mode|transaction|' <<<"${pgb_cfg}"
  grep -q '^default_pool_size|4|' <<<"${pgb_cfg}"
  grep -q '^max_client_conn|100|' <<<"${pgb_cfg}"
  grep -q '^auth_type|scram-sha-256|' <<<"${pgb_cfg}"
  grep -q '^max_prepared_statements|0|' <<<"${pgb_cfg}"

  local max_server_conns distinct_pids
  distinct_pids="$(docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T -e PGPASSWORD=zinder_reader_local_only postgres \
    bash -c '
      pids=""
      for i in 1 2 3 4 5 6 7 8; do
        psql -w -h pgbouncer -p 6432 -U zinder_reader -d zinder_testing -Atc \
          "SELECT pg_backend_pid(), count(*) FROM zinder.topology_probe; SELECT pg_sleep(0.05); SELECT pg_backend_pid();" &
      done
      wait
    ' | awk -F'|' 'NF == 2 {print $1}' | sort -u | wc -l)"
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

  echo "[pg-${pg_ver}] Checking read-only probe replicas, Caddy non-sticky L7 routing & Mock R2 faults..."
  local version_report="${REPORT_DIR}/pg-${pg_ver}.json"
  python3 - "${PROJECT_NAME}" "${pg_ver}" "${r2_port}" "${http_port}" "${grpc_port}" "${distinct_pids}" "${max_server_conns:-0}" "${version_report}" <<'PY'
import base64
import hashlib
import json
import struct
import subprocess
import sys
import urllib.error
import urllib.request

(
    project_name,
    pg_ver,
    r2_port,
    http_port,
    grpc_port,
    distinct_pids,
    max_server_conns,
    out_path,
) = sys.argv[1:]

r2_base = f"http://127.0.0.1:{r2_port}"
http_base = f"http://127.0.0.1:{http_port}"
grpc_base = f"http://127.0.0.1:{grpc_port}"

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

# 1. Inspect probe containers for ReadOnlyRootfs and zero persistent volumes.
for svc in ("probe-reader-a", "probe-reader-b"):
    cid = subprocess.check_output(
        ["docker", "compose", "-p", project_name, "-f",
         f"{sys.path[0] or '.'}/deploy/bench/postgres-testing-ground/docker-compose.yml" if False else "deploy/bench/postgres-testing-ground/docker-compose.yml",
         "ps", "-q", svc],
        text=True,
    ).strip()
    inspected = json.loads(subprocess.check_output(["docker", "inspect", cid], text=True))[0]
    assert inspected["HostConfig"]["ReadonlyRootfs"] is True, f"{svc} must have ReadonlyRootfs=true"
    binds = [m for m in inspected["Mounts"] if m["Type"] != "bind" or not m["Source"].endswith("probe_replica.py")]
    assert not binds, f"{svc} must have zero persistent volumes, got {binds}"

# 2. Check /healthz & /readyz across proxy.
for path in ("/healthz", "/readyz"):
    code, _, body = http_json("GET", f"{http_base}{path}")
    assert code == 200 and body["readonly_rootfs"] is True and body["local_db_files"] == [], body

# 3. Mock R2 PUT / GET / Range / fault validation & fallback paths.
payload = b"zinder-settled-segment-0001-" * 8
expected_sha = hashlib.sha256(payload).hexdigest()
req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", data=payload, method="PUT")
with urllib.request.urlopen(req, timeout=5.0) as resp:
    assert resp.status == 201
    put_body = json.loads(resp.read().decode("utf-8"))
    assert put_body["sha256"] == expected_sha

with urllib.request.urlopen(urllib.request.Request(f"{r2_base}/segments/seg-0001.bin"), timeout=5.0) as resp:
    assert resp.status == 200
    assert resp.read() == payload
    assert resp.headers.get("X-Zinder-Segment-Sha256") == expected_sha

range_req = urllib.request.Request(f"{r2_base}/segments/seg-0001.bin", headers={"Range": "bytes=0-15"})
with urllib.request.urlopen(range_req, timeout=5.0) as resp:
    assert resp.status == 206
    assert resp.read() == payload[:16]
    assert resp.headers.get("Content-Range") == f"bytes 0-15/{len(payload)}"

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
assert r2_metrics["hits"] >= 2 and r2_metrics["range_hits"] >= 1
assert r2_metrics["misses"] >= 1 and r2_metrics["outage_503"] >= 1
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
        wire = resp.read()
        decoded = base64.b64decode(wire) if "grpc-web-text" in ct else wire
        assert decoded[0] == 0
        msg_len = struct.unpack(">I", decoded[1:5])[0]
        assert msg_len > 0 and len(decoded) >= 5 + msg_len
        grpc_web_replicas.append(resp.headers.get("X-Zinder-Replica"))

assert set(grpc_web_replicas) == {"probe-reader-a", "probe-reader-b"}, grpc_web_replicas

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
    },
}
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(report, fh, indent=2, sort_keys=True)
PY

  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --profile readers down -v --remove-orphans >/dev/null
}

verify_zinder_image_readers_only() {
  local zinder_out="${REPORT_DIR}/zinder-image.json"
  python3 - "${ZINDER_IMAGE}" "${PROJECT_NAME}" "${COMPOSE_FILE}" "${zinder_out}" <<'PY'
import json
import os
import subprocess
import sys

image, project_name, compose_file, out_path = sys.argv[1:]
env = dict(os.environ)
env["ZINDER_IMAGE"] = image
rendered = subprocess.check_output(
    ["docker", "compose", "-p", project_name, "-f", compose_file, "--profile", "readers", "config", "--format", "json"],
    env=env,
    text=True,
)
cfg = json.loads(rendered)
profile_services = {}
for svc in ("zinder-reader-a", "zinder-reader-b"):
    spec = cfg["services"][svc]
    assert spec.get("read_only") is True, f"{svc} must set read_only: true"
    assert not spec.get("volumes"), f"{svc} must declare zero persistent volumes"
    cmd = spec.get("command", [])
    assert "--readers-only" in cmd, f"{svc} must pass --readers-only"
    profile_services[svc] = {"read_only": True, "command": cmd}

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
    json.dump({"image": image, "profile_services": profile_services, "validated_presets": validated_presets}, fh, indent=2, sort_keys=True)
PY
}

cd "${REPO_ROOT}"
verify_compose_definitions

IFS=',' read -r -a VERSION_ARRAY <<<"${POSTGRES_VERSIONS}"
for raw_ver in "${VERSION_ARRAY[@]}"; do
  pg_ver="$(echo "${raw_ver}" | tr -d '[:space:]')"
  [[ -n "${pg_ver}" ]] || continue
  verify_postgres_version "${pg_ver}"
done

if [[ -n "${ZINDER_IMAGE}" ]]; then
  echo "[readers] Verifying ${ZINDER_IMAGE} --readers-only compose profile and presets..."
  verify_zinder_image_readers_only
fi

python3 - "${REPORT_DIR}" "${TIMESTAMP_UTC}" <<'PY'
import glob
import json
import os
import sys

report_dir, timestamp_utc = sys.argv[1:]
pg_reports = []
for path in sorted(glob.glob(os.path.join(report_dir, "pg-*.json"))):
    with open(path, encoding="utf-8") as fh:
        pg_reports.append(json.load(fh))

zinder_image_path = os.path.join(report_dir, "zinder-image.json")
zinder_image_report = None
if os.path.exists(zinder_image_path):
    with open(zinder_image_path, encoding="utf-8") as fh:
        zinder_image_report = json.load(fh)

summary = {
    "status": "passed",
    "generated_at_utc": timestamp_utc,
    "postgres_runs": pg_reports,
    "zinder_image_check": zinder_image_report,
}
with open(os.path.join(report_dir, "report.json"), "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, sort_keys=True)

lines = [
    "# Stateless-Topology Testing Ground Report",
    "",
    f"- Status: **PASSED**",
    f"- Generated (UTC): `{timestamp_utc}`",
    "",
    "## PostgreSQL + PgBouncer + Mock R2 + Caddy Results",
    "",
    "| PG Version | Pool Mode | Pool Size | Distinct Backend PIDs | HTTP Replicas | gRPC-Web Replicas | R2 Fault Fallbacks |",
    "| --- | --- | --- | --- | --- | --- | --- |",
]
for item in pg_reports:
    pool = item["pgbouncer_transaction_pool"]
    l7 = item["l7_round_robin"]
    fallbacks = ", ".join(f"{k}->{v}" for k, v in sorted(item["r2_fault_fallbacks"].items()))
    lines.append(
        f"| {item['postgres_version']} | `{pool['pool_mode']}` | {pool['default_pool_size']} | "
        f"{pool['distinct_backend_pids_observed']} | {', '.join(sorted(set(l7['http_replicas'])))} | "
        f"{', '.join(sorted(set(l7['grpc_web_replicas'])))} | {fallbacks} |"
    )

if zinder_image_report is not None:
    lines.extend([
        "",
        "## Stateless Reader Container Image Check",
        "",
        f"- Image: `{zinder_image_report['image']}`",
        f"- Validated `--readers-only` presets (`read_only: true`): `{', '.join(zinder_image_report['validated_presets'])}`",
    ])

with open(os.path.join(report_dir, "report.md"), "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
print(f"Wrote reports to {report_dir}/report.json and {report_dir}/report.md")
PY
