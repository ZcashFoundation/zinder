#!/usr/bin/env bash
# observability-smoke: runs Zinder binaries against a local Zebra to exercise
# the observability stack (Prometheus + Grafana + scrape pipeline).
#
# Defaults assume a regtest Zebra at 127.0.0.1:39232 with basic auth disabled.
# To run against a Z3 testnet stack instead:
#
#   export ZINDER_OBSERVABILITY_NETWORK=zcash-testnet
#   export ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:18232
#   # Extract the cookie from Z3's shared volume into a host file:
#   docker run --rm -v z3-testnet-cookie:/auth:ro alpine cat /auth/.cookie \
#     > "${ZINDER_OBSERVABILITY_WORK_DIR:-/tmp}/z3-testnet.cookie"
#   export ZINDER_NODE__AUTH__METHOD=cookie
#   export ZINDER_NODE__AUTH__PATH="${ZINDER_OBSERVABILITY_WORK_DIR:-/tmp}/z3-testnet.cookie"
#   ./scripts/observability-smoke.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.observability.yml"
PROJECT_NAME="${ZINDER_OBSERVABILITY_PROJECT:-zinder-observability}"
WORK_DIR="${ZINDER_OBSERVABILITY_WORK_DIR:-${ROOT_DIR}/.tmp/observability}"
CONFIG_DIR="${WORK_DIR}/config"
LOG_DIR="${WORK_DIR}/logs"
PIDS_FILE="${WORK_DIR}/pids"
REPORT_DIR="${WORK_DIR}/reports"
WORK_DIR_MARKER="${WORK_DIR}/.zinder-observability-smoke-workdir"

NETWORK="${ZINDER_OBSERVABILITY_NETWORK:-${ZINDER_NETWORK:-zcash-regtest}}"
NODE_ADDR="${ZINDER_OBSERVABILITY_NODE_ADDR:-${ZINDER_NODE__JSON_RPC_ADDR:-http://127.0.0.1:39232}}"
NODE_AUTH_USERNAME="${ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME:-${ZINDER_NODE__AUTH__USERNAME:-zebra}}"
NODE_AUTH_PASSWORD="${ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD:-${ZINDER_NODE__AUTH__PASSWORD:-zebra}}"

PROMETHEUS_PORT="${ZINDER_PROMETHEUS_PORT:-9095}"
PROMETHEUS_STACK_LABEL="${ZINDER_OBSERVABILITY_PROMETHEUS_STACK_LABEL:-zinder-local}"
GRAFANA_PORT="${ZINDER_GRAFANA_PORT:-3002}"
INGEST_OPS_ADDR="${ZINDER_OBSERVABILITY_INGEST_OPS_ADDR:-0.0.0.0:9190}"
COMPAT_OPS_ADDR="${ZINDER_OBSERVABILITY_COMPAT_OPS_ADDR:-0.0.0.0:9192}"
QUERY_OPS_ADDR="${ZINDER_OBSERVABILITY_QUERY_OPS_ADDR:-0.0.0.0:9193}"
PROJECTOR_OPS_ADDR="${ZINDER_OBSERVABILITY_PROJECTOR_OPS_ADDR:-0.0.0.0:9194}"
INGEST_CONTROL_ADDR="${ZINDER_OBSERVABILITY_INGEST_CONTROL_ADDR:-${ZINDER_OBSERVABILITY_WRITER_STATUS_ADDR:-127.0.0.1:9100}}"
COMPAT_GRPC_ADDR="${ZINDER_OBSERVABILITY_COMPAT_GRPC_ADDR:-127.0.0.1:9067}"
QUERY_GRPC_ADDR="${ZINDER_OBSERVABILITY_QUERY_GRPC_ADDR:-127.0.0.1:9102}"

BULK_CATCHUP_BLOCKS="${ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS:-50}"
SOURCE_SEGMENT_MAX_BLOCKS="${ZINDER_OBSERVABILITY_SOURCE_SEGMENT_MAX_BLOCKS:-16}"
SOURCE_SEGMENT_TARGET_RESPONSE_BYTES="${ZINDER_OBSERVABILITY_SOURCE_SEGMENT_TARGET_RESPONSE_BYTES:-33554432}"
SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS="${ZINDER_OBSERVABILITY_SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS:-12}"
SOURCE_FETCH_MAX_IN_FLIGHT_BYTES="${ZINDER_OBSERVABILITY_SOURCE_FETCH_MAX_IN_FLIGHT_BYTES:-402653184}"
BLOCK_PREPARE_CONCURRENCY="${ZINDER_OBSERVABILITY_BLOCK_PREPARE_CONCURRENCY:-16}"
TIP_FOLLOW_POLL_INTERVAL_MS="${ZINDER_OBSERVABILITY_TIP_FOLLOW_POLL_INTERVAL_MS:-1000}"
GENERATE_BLOCKS="${ZINDER_OBSERVABILITY_GENERATE_BLOCKS:-0}"
RESET_WORK_DIR="${ZINDER_OBSERVABILITY_RESET:-1}"
CALIBRATION_RUNS="${ZINDER_OBSERVABILITY_RUNS:-5}"
LIGHTWALLETD_TESTCLIENT="${ZINDER_OBSERVABILITY_LIGHTWALLETD_TESTCLIENT:-0}"
LIGHTWALLETD_REPO="${ZINDER_OBSERVABILITY_LIGHTWALLETD_REPO:-}"
PROJECTOR_BUILD_OWNER_HEX="${ZINDER_OBSERVABILITY_PROJECTOR_BUILD_OWNER_HEX:-0f0e0d0c0b0a09080706050403020100}"
CERTIFY_TOPOLOGY="${ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY:-0}"
COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS="${ZINDER_OBSERVABILITY_COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS:-4}"
CERTIFICATION_LAG_BLOCKS="${ZINDER_OBSERVABILITY_CERTIFICATION_LAG_BLOCKS:-$((COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS + 1))}"

INGEST_CONFIG="${CONFIG_DIR}/zinder-ingest.toml"
PROJECTOR_CONFIG="${CONFIG_DIR}/zinder-projector.toml"
COMPAT_CONFIG="${CONFIG_DIR}/zinder-compat-lightwalletd.toml"
QUERY_CONFIG="${CONFIG_DIR}/zinder-query.toml"

BULK_CATCHUP_SECONDS="null"
RESTORE_STATUS="blocked"
RESTORE_ERROR_CLASS="coherent_canonical_wallet_bundle_unimplemented"
REPORT_JSON_PATH=""
REPORT_MARKDOWN_PATH=""
RUN_ID=""
EVIDENCE_DIR=""
EVIDENCE_EVENTS_FILE=""
EVIDENCE_SAMPLE_INDEX=0
SUSPENDED_PROJECTOR_PID=""
INVALIDATED_REORG_HASH=""
REGTEST_MUTATION_PREFLIGHT_COMPLETE=0
TRAFFIC_READY_READINESS_CAUSES='^(ready|cursor_at_risk|serving_pair_stale)$'
READINESS_WARNING_CAUSES='^(cursor_at_risk|serving_pair_stale)$'

log() {
  printf '[zinder-observability] %s\n' "$*"
}

die() {
  printf '[zinder-observability] error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage: scripts/observability-smoke.sh [run|calibrate|snapshot|stop]

Commands:
  run        Start Prometheus/Grafana, bulk catch up from a checkpoint, record
             that restore is blocked until coherent canonical-plus-wallet
             bundles exist, start ingest/projector/compat
             services, generate traffic, print Prometheus evidence, and write
             a readiness report.
             This is the default command.
  calibrate  Run the same smoke multiple times and write an aggregate baseline
             report. Set ZINDER_OBSERVABILITY_RUNS to control the run count.
  snapshot  Query the currently running Prometheus and service /metrics surfaces.
  stop      Stop local Zinder service processes and the observability compose stack.

Default local node:
  ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:39232
  ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME=zebra
  ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD=zebra

Optional upstream lightwalletd client check:
  ZINDER_OBSERVABILITY_LIGHTWALLETD_TESTCLIENT=1
  ZINDER_OBSERVABILITY_LIGHTWALLETD_REPO=/path/to/lightwalletd

Optional complete-topology certification (run command, regtest only):
  ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY=1

  This pauses the projector while mining a bounded lag window, resumes it,
  restarts compat/projector/ingest one at a time, and forces a one-block reorg.
  Every phase must restore the complete readiness chain before the run passes.

Optional standalone regtest tip advancement:
  ZINDER_OBSERVABILITY_GENERATE_BLOCKS=<positive integer>

  Mutation is disabled by default. Both mutation paths verify the selected
  node's regtest activation fingerprint and required RPC methods first.

Snapshot an existing deployment Compose Prometheus instead of the local stack:
  ZINDER_PROMETHEUS_PORT=19095
  ZINDER_OBSERVABILITY_PROMETHEUS_STACK_LABEL=zinder
USAGE
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

require_commands() {
  require_command cargo
  require_command curl
  require_command docker
  require_command grpcurl
  require_command jq
  require_command python3
  require_command ps
}

validate_harness_configuration() {
  if [[ "$CERTIFY_TOPOLOGY" != "0" && "$CERTIFY_TOPOLOGY" != "1" ]]; then
    die "ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY must be 0 or 1"
  fi
  if ! [[ "$COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS" =~ ^[0-9]+$ ]]; then
    die "ZINDER_OBSERVABILITY_COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS must be a non-negative integer"
  fi
  if ! [[ "$CERTIFICATION_LAG_BLOCKS" =~ ^[1-9][0-9]*$ ]]; then
    die "ZINDER_OBSERVABILITY_CERTIFICATION_LAG_BLOCKS must be a positive integer"
  fi
  if ! [[ "$GENERATE_BLOCKS" =~ ^[0-9]+$ ]]; then
    die "ZINDER_OBSERVABILITY_GENERATE_BLOCKS must be a non-negative integer"
  fi
  if [[ "$CERTIFY_TOPOLOGY" == "1" && "$NETWORK" != "zcash-regtest" ]]; then
    die "complete-topology certification is allowed only with ZINDER_OBSERVABILITY_NETWORK=zcash-regtest"
  fi
  if (( GENERATE_BLOCKS > 0 )) && [[ "$NETWORK" != "zcash-regtest" ]]; then
    die "block generation is allowed only with ZINDER_OBSERVABILITY_NETWORK=zcash-regtest"
  fi
}

docker_compose() {
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

local_url_addr() {
  local listen_addr="$1"
  printf '127.0.0.1:%s' "${listen_addr##*:}"
}

toml_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "$value"
}

json_rpc() {
  local method="$1"
  local params="${2:-[]}"

  curl -fsS \
    -u "${NODE_AUTH_USERNAME}:${NODE_AUTH_PASSWORD}" \
    -H 'content-type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":\"zinder-observability\",\"method\":\"${method}\",\"params\":${params}}" \
    "$NODE_ADDR"
}

node_tip_height() {
  local response
  response="$(json_rpc getblockcount '[]')"
  if jq -e '.error? != null' >/dev/null <<<"$response"; then
    jq -r '.error.message // .error' <<<"$response" >&2
    return 1
  fi
  jq -er '.result | numbers' <<<"$response"
}

wait_http() {
  local name="$1"
  local url="$2"
  local timeout_seconds="${3:-60}"
  local deadline=$((SECONDS + timeout_seconds))

  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      die "${name} did not become reachable at ${url}"
    fi
    sleep 1
  done
}

service_ops_addr() {
  case "$1" in
    zinder-ingest) printf '%s' "$INGEST_OPS_ADDR" ;;
    zinder-projector) printf '%s' "$PROJECTOR_OPS_ADDR" ;;
    zinder-query) printf '%s' "$QUERY_OPS_ADDR" ;;
    zinder-compat-lightwalletd) printf '%s' "$COMPAT_OPS_ADDR" ;;
    *) die "unknown Zinder service: $1" ;;
  esac
}

service_ready_url() {
  printf 'http://%s/readyz' "$(local_url_addr "$(service_ops_addr "$1")")"
}

readiness_cause_label() {
  jq -er '
    .cause
    | if type == "string" then .
      elif type == "object" and length == 1 then keys[0]
      else error("unrecognized readiness cause shape")
      end
  '
}

initialize_run_evidence() {
  local run_suffix
  run_suffix="$(date -u +"%Y%m%dT%H%M%SZ")"
  if [[ -n "${CALIBRATION_RUN_INDEX:-}" ]]; then
    run_suffix="${run_suffix}-run-${CALIBRATION_RUN_INDEX}"
  fi
  RUN_ID="${NETWORK}-${run_suffix}"
  EVIDENCE_DIR="${REPORT_DIR}/${RUN_ID}-evidence"
  EVIDENCE_EVENTS_FILE="${EVIDENCE_DIR}/events.jsonl"
  mkdir -p "${EVIDENCE_DIR}/readiness" "${EVIDENCE_DIR}/metrics" "${EVIDENCE_DIR}/grpc" "${EVIDENCE_DIR}/node" "${EVIDENCE_DIR}/logs"
  : >"$EVIDENCE_EVENTS_FILE"
}

record_evidence_event() {
  local phase="$1"
  local status="$2"
  local detail="${3:-}"
  jq -cn \
    --arg captured_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg phase "$phase" \
    --arg status "$status" \
    --arg detail "$detail" \
    '{captured_at: $captured_at, phase: $phase, status: $status, detail: $detail}' \
    >>"$EVIDENCE_EVENTS_FILE"
}

capture_readiness_sample() {
  local label="$1"
  local sample_prefix sample_file sample_rows_file service url body_file http_status metrics_url
  EVIDENCE_SAMPLE_INDEX=$((EVIDENCE_SAMPLE_INDEX + 1))
  sample_prefix="$(printf '%03d' "$EVIDENCE_SAMPLE_INDEX")-${label}"
  sample_file="${EVIDENCE_DIR}/readiness/${sample_prefix}.json"
  sample_rows_file="$(mktemp "${WORK_DIR}/readiness-sample.XXXXXX")"

  for service in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd; do
    url="$(service_ready_url "$service")"
    body_file="$(mktemp "${WORK_DIR}/readiness-body.XXXXXX")"
    if ! http_status="$(curl -sS --output "$body_file" --write-out '%{http_code}' "$url" 2>/dev/null)"; then
      http_status="0"
    else
      http_status="$((10#${http_status}))"
    fi
    if jq -e . >/dev/null 2>&1 <"$body_file"; then
      jq -cn \
        --arg service "$service" \
        --arg url "$url" \
        --argjson http_status "${http_status:-0}" \
        --slurpfile body "$body_file" \
        '{service: $service, url: $url, http_status: $http_status, body: $body[0]}' \
        >>"$sample_rows_file"
    else
      jq -cn \
        --arg service "$service" \
        --arg url "$url" \
        --argjson http_status "${http_status:-0}" \
        --rawfile body "$body_file" \
        '{service: $service, url: $url, http_status: $http_status, body_text: $body}' \
        >>"$sample_rows_file"
    fi
    rm -f "$body_file"
    metrics_url="http://$(local_url_addr "$(service_ops_addr "$service")")/metrics"
    curl -fsS "$metrics_url" >"${EVIDENCE_DIR}/metrics/${sample_prefix}-${service}.prom" \
      2>/dev/null || rm -f "${EVIDENCE_DIR}/metrics/${sample_prefix}-${service}.prom"
  done

  jq -s \
    --arg captured_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg label "$label" \
    '{captured_at: $captured_at, label: $label, services: map({key: .service, value: del(.service)}) | from_entries}' \
    "$sample_rows_file" >"$sample_file"
  rm -f "$sample_rows_file"
  log "readiness evidence: ${sample_file}"
}

wait_service_ready() {
  local service="$1"
  local timeout_seconds="${2:-300}"
  local deadline=$((SECONDS + timeout_seconds))
  local url response cause pid
  url="$(service_ready_url "$service")"

  while true; do
    if response="$(curl -fsS "$url" 2>/dev/null)" &&
      [[ "$(jq -r '.status // empty' <<<"$response")" == "ready" ]]; then
      cause="$(readiness_cause_label <<<"$response")"
      if [[ "$cause" =~ $TRAFFIC_READY_READINESS_CAUSES ]]; then
        return 0
      fi
    fi
    pid="$(current_process_pid "$service")"
    if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      tail -n 120 "${LOG_DIR}/${service}.log" >&2 || true
      die "${service} exited before becoming traffic-ready"
    fi
    if (( SECONDS >= deadline )); then
      printf '%s\n' "${response:-<unavailable>}" >&2
      die "${service} did not become traffic-ready at ${url}"
    fi
    sleep 1
  done
}

wait_complete_readiness() {
  local label="$1"
  local timeout_seconds="${2:-900}"
  wait_service_ready zinder-ingest "$timeout_seconds"
  wait_service_ready zinder-projector "$timeout_seconds"
  wait_service_ready zinder-query "$timeout_seconds"
  wait_service_ready zinder-compat-lightwalletd "$timeout_seconds"
  capture_readiness_sample "$label"
  record_evidence_event "$label" passed "all four services traffic-ready"
}

wait_service_readiness_cause() {
  local service="$1"
  local expected_cause="$2"
  local timeout_seconds="${3:-180}"
  local deadline=$((SECONDS + timeout_seconds))
  local url response cause
  url="$(service_ready_url "$service")"

  while true; do
    if response="$(curl -sS "$url" 2>/dev/null)" &&
      cause="$(readiness_cause_label <<<"$response" 2>/dev/null)" &&
      [[ "$cause" == "$expected_cause" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf '%s\n' "${response:-<unavailable>}" >&2
      die "${service} did not report readiness cause ${expected_cause} at ${url}"
    fi
    sleep 1
  done
}

reload_prometheus() {
  curl -fsS -X POST "http://127.0.0.1:${PROMETHEUS_PORT}/-/reload" >/dev/null 2>&1 || true
}

prometheus_query() {
  local query="$1"
  curl -fsS --get "http://127.0.0.1:${PROMETHEUS_PORT}/api/v1/query" \
    --data-urlencode "query=${query}"
}

prometheus_max_value() {
  local query="$1"
  local response

  if ! response="$(prometheus_query "$query" 2>/dev/null)"; then
    printf 'null'
    return 0
  fi

  jq -r '
    if .status != "success" or (.data.result | length) == 0 then
      "null"
    else
      ([.data.result[].value[1] | tonumber] | max)
    end
  ' <<<"$response"
}

prometheus_sample_count() {
  local query="$1"
  local response

  if ! response="$(prometheus_query "$query" 2>/dev/null)"; then
    printf '0'
    return 0
  fi

  jq -r '
    if .status != "success" then
      0
    else
      (.data.result | length)
    end
  ' <<<"$response"
}

wait_prometheus_samples() {
  local name="$1"
  local query="$2"
  local timeout_seconds="${3:-45}"
  local deadline=$((SECONDS + timeout_seconds))
  local response

  while true; do
    if response="$(prometheus_query "$query" 2>/dev/null)" &&
      jq -e '.status == "success" and (.data.result | length > 0)' >/dev/null <<<"$response"; then
      return 0
    fi

    if (( SECONDS >= deadline )); then
      log "${name}: no Prometheus samples yet for query: ${query}"
      return 1
    fi
    sleep 1
  done
}

prepare_work_dir() {
  [[ "$WORK_DIR" == /* ]] ||
    die "ZINDER_OBSERVABILITY_WORK_DIR must be an absolute path"
  case "$WORK_DIR" in
    / | /tmp | /private/tmp | "$ROOT_DIR")
      die "refusing unsafe ZINDER_OBSERVABILITY_WORK_DIR: ${WORK_DIR}"
      ;;
  esac
  [[ ! -L "$WORK_DIR" ]] ||
    die "ZINDER_OBSERVABILITY_WORK_DIR must not be a symbolic link"
  [[ ! -e "$WORK_DIR" || -d "$WORK_DIR" ]] ||
    die "ZINDER_OBSERVABILITY_WORK_DIR must be absent or a directory"

  if [[ -d "$WORK_DIR" && ! -f "$WORK_DIR_MARKER" ]]; then
    if [[ -n "$(find "$WORK_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
      die "refusing to reset an unmarked non-empty observability work directory: ${WORK_DIR}"
    fi
  fi
  mkdir -p "$WORK_DIR"
  [[ ! -e "$WORK_DIR_MARKER" || -f "$WORK_DIR_MARKER" ]] ||
    die "observability work-directory marker is not a regular file"
  : >"$WORK_DIR_MARKER"
  if [[ "$RESET_WORK_DIR" == "1" ]]; then
    rm -rf \
      "${WORK_DIR}/zinder-store" \
      "${WORK_DIR}/wallet" \
      "${WORK_DIR}/projector-canonical-secondary" \
      "${WORK_DIR}/query-canonical-secondary" \
      "${WORK_DIR}/query-wallet-secondary" \
      "${WORK_DIR}/compat-canonical-secondary" \
      "${WORK_DIR}/compat-wallet-secondary" \
      "$CONFIG_DIR" \
      "$LOG_DIR"
  fi
  mkdir -p "$CONFIG_DIR" "$LOG_DIR" "$REPORT_DIR"
  : >"$PIDS_FILE"
}

write_configs() {
  local storage_path wallet_path projector_canonical_secondary_path
  local query_canonical_secondary_path query_wallet_secondary_path
  local compat_canonical_secondary_path compat_wallet_secondary_path
  storage_path="$(toml_escape "${WORK_DIR}/zinder-store")"
  wallet_path="$(toml_escape "${WORK_DIR}/wallet")"
  projector_canonical_secondary_path="$(toml_escape "${WORK_DIR}/projector-canonical-secondary")"
  query_canonical_secondary_path="$(toml_escape "${WORK_DIR}/query-canonical-secondary")"
  query_wallet_secondary_path="$(toml_escape "${WORK_DIR}/query-wallet-secondary")"
  compat_canonical_secondary_path="$(toml_escape "${WORK_DIR}/compat-canonical-secondary")"
  compat_wallet_secondary_path="$(toml_escape "${WORK_DIR}/compat-wallet-secondary")"
  local node_addr node_username node_password network
  node_addr="$(toml_escape "$NODE_ADDR")"
  node_username="$(toml_escape "$NODE_AUTH_USERNAME")"
  node_password="$(toml_escape "$NODE_AUTH_PASSWORD")"
  network="$(toml_escape "$NETWORK")"

  cat >"$INGEST_CONFIG" <<EOF
[network]
name = "${network}"

[node]
json_rpc_addr = "${node_addr}"
request_timeout_secs = 30
max_response_bytes = 67108864

[node.auth]
method = "basic"
username = "${node_username}"
password = "${node_password}"

[storage]
path = "${storage_path}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.construction]
source_segment_max_blocks = ${SOURCE_SEGMENT_MAX_BLOCKS}
source_segment_target_response_bytes = ${SOURCE_SEGMENT_TARGET_RESPONSE_BYTES}
source_fetch_max_in_flight_requests = ${SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS}
source_fetch_max_in_flight_bytes = ${SOURCE_FETCH_MAX_IN_FLIGHT_BYTES}
block_prepare_concurrency = ${BLOCK_PREPARE_CONCURRENCY}

[ingest.follow]
poll_interval_ms = ${TIP_FOLLOW_POLL_INTERVAL_MS}
lag_threshold_blocks = 2

[ingest.run_overrides]
checkpoint_height = ${CHECKPOINT_HEIGHT}

[ingest_control]
listen_addr = "${INGEST_CONTROL_ADDR}"

[ops]
listen_addr = ""

[security]
allow_public_bind = true
EOF

  cat >"$PROJECTOR_CONFIG" <<EOF
[network]
name = "${network}"

[storage]
canonical_path = "${storage_path}"
canonical_secondary_path = "${projector_canonical_secondary_path}"

[wallet]
path = "${wallet_path}"

[projector]
reorg_window_blocks = 100
build_owner_hex = "${PROJECTOR_BUILD_OWNER_HEX}"
lease_duration_seconds = 14400

[ingest_control]
addr = "http://${INGEST_CONTROL_ADDR}"

[ops]
listen_addr = "${PROJECTOR_OPS_ADDR}"

[node]
json_rpc_addr = "${node_addr}"
request_timeout_secs = 30
max_response_bytes = 67108864

[node.auth]
method = "basic"
username = "${node_username}"
password = "${node_password}"

[security]
allow_public_bind = true
EOF

  cat >"$QUERY_CONFIG" <<EOF
[network]
name = "${network}"

[storage]
path = "${storage_path}"
secondary_path = "${query_canonical_secondary_path}"

[wallet]
path = "${wallet_path}"
secondary_path = "${query_wallet_secondary_path}"

[ingest_control]
addr = "http://${INGEST_CONTROL_ADDR}"

[query]
listen_addr = "${QUERY_GRPC_ADDR}"
reorg_window_blocks = 100

[ops]
listen_addr = ""

[node]
json_rpc_addr = "${node_addr}"
request_timeout_secs = 30
max_response_bytes = 16777216

[node.auth]
method = "basic"
username = "${node_username}"
password = "${node_password}"

[security]
allow_public_bind = true
EOF

  cat >"$COMPAT_CONFIG" <<EOF
[network]
name = "${network}"

[storage]
path = "${storage_path}"
secondary_path = "${compat_canonical_secondary_path}"
secondary_catchup_interval_ms = 250
secondary_replica_lag_threshold_chain_epochs = ${COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS}

[wallet]
path = "${wallet_path}"
secondary_path = "${compat_wallet_secondary_path}"

[ingest_control]
addr = "http://${INGEST_CONTROL_ADDR}"

[compat]
listen_addr = "${COMPAT_GRPC_ADDR}"

[ops]
listen_addr = ""

[node]
json_rpc_addr = "${node_addr}"
request_timeout_secs = 30
max_response_bytes = 16777216

[node.auth]
method = "basic"
username = "${node_username}"
password = "${node_password}"

[security]
allow_public_bind = true
EOF
}

service_executable() {
  case "$1" in
    zinder-ingest) printf '%s' "${ROOT_DIR}/target/debug/zinder-ingest" ;;
    zinder-projector) printf '%s' "${ROOT_DIR}/target/debug/zinder-projector" ;;
    zinder-query) printf '%s' "${ROOT_DIR}/target/debug/zinder-query" ;;
    zinder-compat-lightwalletd) printf '%s' "${ROOT_DIR}/target/debug/zinder-compat-lightwalletd" ;;
    *) return 1 ;;
  esac
}

tracked_process_is_expected() {
  local name="$1"
  local pid="$2"
  local expected_executable process_command process_group
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  expected_executable="$(service_executable "$name")" || return 1
  process_command="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  process_group="$(ps -p "$pid" -o pgid= 2>/dev/null | tr -d '[:space:]')" || return 1
  [[ "$process_command" == "$expected_executable" || "$process_command" == "${expected_executable} "* ]] &&
    [[ "$process_group" == "$pid" ]]
}

require_expected_tracked_process() {
  local name="$1"
  local pid="$2"
  tracked_process_is_expected "$name" "$pid" ||
    die "refusing to signal unverified PID ${pid} from the ${name} PID-file entry"
}

stop_services() {
  if [[ ! -f "$PIDS_FILE" ]]; then
    return 0
  fi

  while read -r name pid; do
    [[ -n "${name:-}" && -n "${pid:-}" ]] || continue
    if kill -0 "$pid" >/dev/null 2>&1; then
      require_expected_tracked_process "$name" "$pid"
      log "stopping ${name} (${pid})"
      kill -TERM -- "-${pid}" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
    fi
  done <"$PIDS_FILE"

  while read -r name pid; do
    [[ -n "${name:-}" && -n "${pid:-}" ]] || continue
    wait_process_exit "$name" "$pid" 10 || force_stop_process "$name" "$pid"
  done <"$PIDS_FILE"

  rm -f "$PIDS_FILE"
}

current_process_pid() {
  local name="$1"
  awk -v target="$name" '$1 == target { pid = $2 } END { if (pid != "") print pid }' "$PIDS_FILE"
}

wait_process_exit() {
  local name="$1"
  local pid="$2"
  local timeout_seconds="${3:-10}"
  local deadline=$((SECONDS + timeout_seconds))
  while kill -0 "$pid" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.1
  done
  log "${name} (${pid}) stopped"
}

force_stop_process() {
  local name="$1"
  local pid="$2"
  require_expected_tracked_process "$name" "$pid"
  log "force-stopping ${name} (${pid})"
  kill -KILL -- "-${pid}" >/dev/null 2>&1 || kill -KILL "$pid" >/dev/null 2>&1 || true
  wait_process_exit "$name" "$pid" 5 || die "${name} (${pid}) did not stop after SIGKILL"
}

stop_process() {
  local name="$1"
  local pid pids_file_next
  pid="$(current_process_pid "$name")"
  [[ -n "$pid" ]] || die "no tracked process named ${name}"
  if kill -0 "$pid" >/dev/null 2>&1; then
    require_expected_tracked_process "$name" "$pid"
    log "stopping ${name} (${pid})"
    kill -TERM -- "-${pid}" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
    wait_process_exit "$name" "$pid" 30 || force_stop_process "$name" "$pid"
  fi
  pids_file_next="$(mktemp "${WORK_DIR}/pids.XXXXXX")"
  awk -v target="$name" -v target_pid="$pid" \
    '!($1 == target && $2 == target_pid)' "$PIDS_FILE" >"$pids_file_next"
  mv "$pids_file_next" "$PIDS_FILE"
}

start_process() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  local pid

  log "starting ${name}; log: ${log_file}"
  printf '\n[%s] starting %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$name" >>"$log_file"
  if ! pid="$(
    python3 - "$log_file" "$@" <<'PY'
import os
import sys

log_path = sys.argv[1]
argv = sys.argv[2:]

pid = os.fork()
if pid != 0:
    print(pid)
    raise SystemExit(0)

os.setsid()
preserved_env = {
    "HOME": os.environ.get("HOME", ""),
    "PATH": os.environ.get("PATH", ""),
    "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
    "RUST_LOG": os.environ.get("RUST_LOG", "info"),
}
os.environ.clear()
os.environ.update({key: value for key, value in preserved_env.items() if value})
log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
stdin_fd = os.open(os.devnull, os.O_RDONLY)
os.dup2(stdin_fd, 0)
os.dup2(log_fd, 1)
os.dup2(log_fd, 2)
os.close(stdin_fd)
os.close(log_fd)
os.execvp(argv[0], argv)
PY
  )"; then
    die "failed to launch ${name}"
  fi
  printf '%s %s\n' "$name" "$pid" >>"$PIDS_FILE"

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    tail -n 120 "$log_file" >&2 || true
    die "${name} exited during startup"
  fi
}

start_service() {
  case "$1" in
    zinder-ingest)
      start_process zinder-ingest \
        "${ROOT_DIR}/target/debug/zinder-ingest" \
        --config "$INGEST_CONFIG" \
        --ops-listen-addr "$INGEST_OPS_ADDR"
      ;;
    zinder-projector)
      start_process zinder-projector \
        "${ROOT_DIR}/target/debug/zinder-projector" \
        --config "$PROJECTOR_CONFIG" \
        --ops-listen-addr "$PROJECTOR_OPS_ADDR"
      ;;
    zinder-query)
      start_process zinder-query \
        "${ROOT_DIR}/target/debug/zinder-query" \
        --config "$QUERY_CONFIG" \
        --ops-listen-addr "$QUERY_OPS_ADDR"
      ;;
    zinder-compat-lightwalletd)
      start_process zinder-compat-lightwalletd \
        "${ROOT_DIR}/target/debug/zinder-compat-lightwalletd" \
        --config "$COMPAT_CONFIG" \
        --ops-listen-addr "$COMPAT_OPS_ADDR"
      ;;
    *) die "unknown Zinder service: $1" ;;
  esac
}

restart_service() {
  local service="$1"
  capture_readiness_sample "before-restart-${service}"
  stop_process "$service"
  capture_readiness_sample "stopped-${service}"
  start_service "$service"
  wait_complete_readiness "after-restart-${service}" 900
}

resume_suspended_projector() {
  if [[ -n "$SUSPENDED_PROJECTOR_PID" ]] && kill -0 "$SUSPENDED_PROJECTOR_PID" >/dev/null 2>&1; then
    if tracked_process_is_expected zinder-projector "$SUSPENDED_PROJECTOR_PID"; then
      kill -CONT -- "-${SUSPENDED_PROJECTOR_PID}" >/dev/null 2>&1 || \
        kill -CONT "$SUSPENDED_PROJECTOR_PID" >/dev/null 2>&1 || true
    else
      log "refusing to resume unverified suspended-projector PID ${SUSPENDED_PROJECTOR_PID}"
    fi
  fi
  SUSPENDED_PROJECTOR_PID=""
}

cleanup_on_exit() {
  local exit_status=$?
  local reconsider_response
  resume_suspended_projector
  if [[ -n "$INVALIDATED_REORG_HASH" ]]; then
    if reconsider_response="$(json_rpc reconsiderblock "[\"${INVALIDATED_REORG_HASH}\"]" 2>/dev/null)"; then
      printf '%s\n' "$reconsider_response" >"${EVIDENCE_DIR}/node/reorg-cleanup-reconsider-result.json"
      if jq -e '.error == null' >/dev/null <<<"$reconsider_response"; then
        log "reconsidered the invalidated regtest block during cleanup"
      else
        log "the node rejected invalidated-block cleanup"
      fi
    else
      log "could not reconsider the invalidated regtest block during cleanup"
    fi
    INVALIDATED_REORG_HASH=""
  fi
  if (( exit_status != 0 )) && [[ -n "$EVIDENCE_DIR" && -d "$EVIDENCE_DIR" ]]; then
    set +e
    record_evidence_event run failed "smoke or certification command exited nonzero"
    capture_readiness_sample failed-final
    if compgen -G "${LOG_DIR}/*.log" >/dev/null; then
      cp "${LOG_DIR}"/*.log "${EVIDENCE_DIR}/logs/"
    fi
    git -C "$ROOT_DIR" status --short >"${EVIDENCE_DIR}/git-status.txt"
    jq -s '.' "$EVIDENCE_EVENTS_FILE" >"${EVIDENCE_DIR}/events.json"
    set -e
  fi
  return "$exit_status"
}

zinder_process_env() {
  env -i \
    "HOME=${HOME:-}" \
    "PATH=${PATH:-}" \
    "TMPDIR=${TMPDIR:-/tmp}" \
    "RUST_LOG=${RUST_LOG:-info}" \
    "$@"
}

build_binaries() {
  local log_file="${LOG_DIR}/build.log"
  log "building Zinder service binaries; log: ${log_file}"
  if ! (
    cd "$ROOT_DIR"
    cargo build -p zinder-ingest -p zinder-projector -p zinder-query -p zinder-compat-lightwalletd
  ) >"$log_file" 2>&1; then
    tail -n 160 "$log_file" >&2 || true
    die "service binary build failed"
  fi
}

run_bulk_catchup_seed() {
  local log_file="${LOG_DIR}/bulk-catchup.log"
  local started_at ended_at
  log "running phase-driven ingest until target_height ${BULK_CATCHUP_TO_HEIGHT} from checkpoint ${CHECKPOINT_HEIGHT}; log: ${log_file}"
  started_at="$(python3 - <<'PY'
import time
print(time.time())
PY
)"
  # The ingest loop honours --target-height and exits cleanly when
  # reached. The same binary handles bulk-catchup and tip-follow in one
  # process per ADR-0015; the long-running invocation below omits the
  # flag so it continues into FollowingTip after the seed completes.
  if ! (
    cd "$ROOT_DIR"
    zinder_process_env "${ROOT_DIR}/target/debug/zinder-ingest" \
      --config "$INGEST_CONFIG" --target-height "$BULK_CATCHUP_TO_HEIGHT"
  ) >"$log_file" 2>&1; then
    tail -n 160 "$log_file" >&2 || true
    die "checkpoint bulk catchup failed"
  fi
  ended_at="$(python3 - <<'PY'
import time
print(time.time())
PY
)"
  BULK_CATCHUP_SECONDS="$(python3 - "$started_at" "$ended_at" <<'PY'
import sys
started_at = float(sys.argv[1])
ended_at = float(sys.argv[2])
print(f"{ended_at - started_at:.6f}")
PY
)"
  log "checkpoint bulk catchup completed in ${BULK_CATCHUP_SECONDS}s"
}

record_restore_unavailability() {
  log "restore remains blocked: a coherent canonical-plus-wallet bundle restore is not implemented"
}

lightwalletd_grpc() {
  local request_json="$1"
  local method="$2"
  grpcurl -plaintext \
    -import-path "${ROOT_DIR}/crates/zinder-proto/proto/compat/lightwalletd" \
    -proto service.proto \
    -d "$request_json" \
    "$COMPAT_GRPC_ADDR" \
    "$method"
}

native_wallet_grpc() {
  local request_json="$1"
  local method="$2"
  grpcurl -plaintext \
    -import-path "${ROOT_DIR}/crates/zinder-proto/proto" \
    -proto zinder/v1/wallet/wallet.proto \
    -d "$request_json" \
    "$QUERY_GRPC_ADDR" \
    "$method"
}

canonical_control_grpc() {
  grpcurl -plaintext \
    -import-path "${ROOT_DIR}/crates/zinder-proto/proto" \
    -proto zinder/v1/ingest/ingest.proto \
    -d '{}' \
    "$INGEST_CONTROL_ADDR" \
    zinder.v1.ingest.CanonicalControl/WriterStatus
}

require_json_rpc_result() {
  local method="$1"
  local params="${2:-[]}"
  local response
  response="$(json_rpc "$method" "$params")" || die "${method} JSON-RPC transport failed"
  if jq -e '.error? != null' >/dev/null <<<"$response"; then
    die "${method} JSON-RPC failed: $(jq -r '.error.message // .error' <<<"$response")"
  fi
  jq -c '.result' <<<"$response"
}

require_regtest_mutation_preflight() {
  local blockchain_info discovery
  if [[ "$REGTEST_MUTATION_PREFLIGHT_COMPLETE" == "1" ]]; then
    return 0
  fi
  [[ "$NETWORK" == "zcash-regtest" ]] ||
    die "node mutation is allowed only with ZINDER_OBSERVABILITY_NETWORK=zcash-regtest"

  blockchain_info="$(json_rpc getblockchaininfo '[]')" ||
    die "could not verify the selected node before regtest mutation"
  discovery="$(json_rpc rpc.discover '[]')" ||
    die "could not verify the selected node RPC capabilities before regtest mutation"
  printf '%s\n' "$blockchain_info" >"${EVIDENCE_DIR}/node/mutation-preflight-blockchain-info.json"
  printf '%s\n' "$discovery" >"${EVIDENCE_DIR}/node/mutation-preflight-openrpc.json"

  # Zebra exposes both testnet and regtest as the BIP70 name "test". Default
  # regtest activates every advertised upgrade at height 1, so the activation
  # schedule is the fail-closed discriminator available through JSON-RPC.
  jq -e '
    .error == null and
    .result.chain == "test" and
    (.result.upgrades | type == "object" and length > 0) and
    ([.result.upgrades[].activationheight] | all(. == 1)) and
    ([.result.upgrades[].name] | index("Sapling") != null) and
    ([.result.upgrades[].name] | index("NU5") != null)
  ' >/dev/null <<<"$blockchain_info" ||
    die "selected node does not match Zebra's default regtest activation fingerprint"

  jq -e '
    .error == null and
    ([.result.methods[].name] as $methods |
      ["generate", "getblockcount", "getblockhash", "getblockchaininfo",
       "invalidateblock", "reconsiderblock", "rpc.discover"] |
      all(. as $required | $methods | index($required) != null))
  ' >/dev/null <<<"$discovery" ||
    die "selected node does not advertise every RPC required for regtest mutation"

  REGTEST_MUTATION_PREFLIGHT_COMPLETE=1
  record_evidence_event mutation-preflight passed \
    "configured zcash-regtest; remote BIP70 test chain has all upgrades at height 1 and required mutation RPCs"
}

wait_node_tip() {
  local expected_height="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  local observed_height=""
  while true; do
    observed_height="$(node_tip_height 2>/dev/null || true)"
    if [[ "$observed_height" == "$expected_height" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      die "node tip did not reach exact height ${expected_height}; observed ${observed_height:-unavailable}"
    fi
    sleep 1
  done
}

wait_compat_tip() {
  local target_height="$1"
  local timeout_seconds="${2:-90}"
  local deadline=$((SECONDS + timeout_seconds))
  local response

  while true; do
    if response="$(lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock 2>/dev/null)" &&
      jq -e --argjson height "$target_height" '.height | tonumber >= $height' >/dev/null <<<"$response"; then
      return 0
    fi

    if (( SECONDS >= deadline )); then
      log "latest lightwalletd-compatible response:"
      printf '%s\n' "${response:-<unavailable>}"
      die "lightwalletd compatibility surface did not reach visible height ${target_height}"
    fi
    sleep 1
  done
}

run_grpc_call() {
  local name="$1"
  shift
  local output_file="${LOG_DIR}/grpc-${name}.json"
  if "$@" >"$output_file" 2>&1; then
    log "gRPC ${name} ok; output: ${output_file}"
  else
    tail -n 80 "$output_file" >&2 || true
    die "gRPC ${name} failed"
  fi
}

run_controlled_projector_lag() {
  local projector_pid before_tip generated after_tip
  projector_pid="$(current_process_pid zinder-projector)"
  [[ -n "$projector_pid" ]] || die "projector is not tracked for the lag phase"
  require_expected_tracked_process zinder-projector "$projector_pid"
  if (( CERTIFICATION_LAG_BLOCKS <= COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS )); then
    die "ZINDER_OBSERVABILITY_CERTIFICATION_LAG_BLOCKS must exceed the compat replica-lag threshold"
  fi

  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-before-projector-lag.json"
  capture_readiness_sample before-projector-lag
  kill -STOP -- "-${projector_pid}" >/dev/null 2>&1 || kill -STOP "$projector_pid" || \
    die "could not suspend zinder-projector (${projector_pid})"
  SUSPENDED_PROJECTOR_PID="$projector_pid"
  record_evidence_event projector-lag suspended "pid=${projector_pid}"

  before_tip="$(node_tip_height)"
  generated="$(require_json_rpc_result generate "[${CERTIFICATION_LAG_BLOCKS}]")"
  printf '%s\n' "$generated" >"${EVIDENCE_DIR}/node/projector-lag-generated-blocks.json"
  after_tip="$(node_tip_height)"
  if (( after_tip < before_tip + CERTIFICATION_LAG_BLOCKS )); then
    die "lag phase did not advance Zebra by ${CERTIFICATION_LAG_BLOCKS} blocks"
  fi
  TARGET_TIP_HEIGHT="$after_tip"

  wait_service_readiness_cause zinder-compat-lightwalletd replica_lagging 240
  local unavailable_output="${EVIDENCE_DIR}/grpc/compact-tx-streamer-during-projector-lag.txt"
  if lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock \
    >"$unavailable_output" 2>&1; then
    die "CompactTxStreamer admitted a new request while replica_lagging blocked traffic"
  fi
  grep -Fq "Unavailable" "$unavailable_output" || {
    tail -n 80 "$unavailable_output" >&2 || true
    die "CompactTxStreamer lag rejection did not return gRPC Unavailable"
  }
  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-during-projector-lag.json"
  jq -e \
    --slurpfile before "${EVIDENCE_DIR}/grpc/writer-status-before-projector-lag.json" \
    --argjson height "$TARGET_TIP_HEIGHT" \
    '(.fence.chainEpochId | tonumber) > ($before[0].fence.chainEpochId | tonumber) and
     (.fence.eventSequence | tonumber) > ($before[0].fence.eventSequence | tonumber) and
     (.fence.visibleTipHeight | tonumber) >= $height' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/writer-status-during-projector-lag.json" ||
    die "writer fence did not advance while the projector was suspended"
  capture_readiness_sample projector-lag-observed
  record_evidence_event projector-lag observed \
    "compat replica_lagging after node advanced from ${before_tip} to ${after_tip}"

  resume_suspended_projector
  wait_compat_tip "$TARGET_TIP_HEIGHT" 300
  wait_complete_readiness projector-lag-recovered 300
  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-after-projector-lag.json"
  jq -e \
    --slurpfile during "${EVIDENCE_DIR}/grpc/writer-status-during-projector-lag.json" \
    '(.fence.chainEpochId | tonumber) >= ($during[0].fence.chainEpochId | tonumber) and
     (.fence.eventSequence | tonumber) >= ($during[0].fence.eventSequence | tonumber) and
     .fence.canonicalSequenceDigest == $during[0].fence.canonicalSequenceDigest' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/writer-status-after-projector-lag.json" ||
    die "recovered topology is not bound to the writer fence observed during projector lag"
}

run_restart_certification() {
  local service
  for service in zinder-compat-lightwalletd zinder-query zinder-projector zinder-ingest; do
    restart_service "$service"
    canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-after-restart-${service}.json"
    native_wallet_grpc '{}' zinder.v1.wallet.WalletQuery/ServerInfo \
      >"${EVIDENCE_DIR}/grpc/native-server-info-after-restart-${service}.json"
    lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo \
      >"${EVIDENCE_DIR}/grpc/lightd-info-after-restart-${service}.json"
  done
}

run_one_block_reorg_certification() {
  local pre_reorg_tip invalidated_hash invalidated_response generated post_reorg_tip replacement_hash
  pre_reorg_tip="$(node_tip_height)"
  invalidated_hash="$(require_json_rpc_result getblockhash "[${pre_reorg_tip}]" | jq -er '.')"
  jq -cn \
    --argjson height "$pre_reorg_tip" \
    --arg hash "$invalidated_hash" \
    '{height: $height, hash: $hash}' \
    >"${EVIDENCE_DIR}/node/reorg-invalidated-block.json"
  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-before-reorg.json"
  lightwalletd_grpc "{\"height\":${pre_reorg_tip}}" cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock \
    >"${EVIDENCE_DIR}/grpc/compact-block-before-reorg.json"
  jq -e --argjson height "$pre_reorg_tip" \
    '(.height | tonumber) == $height and (.hash | strings | length > 0)' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/compact-block-before-reorg.json" ||
    die "compatibility GetBlock did not expose the pre-reorg tip hash"

  capture_readiness_sample before-one-block-reorg
  INVALIDATED_REORG_HASH="$invalidated_hash"
  invalidated_response="$(require_json_rpc_result invalidateblock "[\"${invalidated_hash}\"]")"
  printf '%s\n' "$invalidated_response" >"${EVIDENCE_DIR}/node/reorg-invalidate-result.json"
  wait_node_tip "$((pre_reorg_tip - 1))" 60

  generated="$(require_json_rpc_result generate '[2]')"
  printf '%s\n' "$generated" >"${EVIDENCE_DIR}/node/reorg-replacement-blocks.json"
  post_reorg_tip="$(node_tip_height)"
  if (( post_reorg_tip <= pre_reorg_tip )); then
    die "replacement branch did not become longer than the invalidated branch"
  fi
  replacement_hash="$(require_json_rpc_result getblockhash "[${pre_reorg_tip}]" | jq -er '.')"
  [[ "$replacement_hash" != "$invalidated_hash" ]] ||
    die "node kept the invalidated hash at height ${pre_reorg_tip}"
  jq -cn \
    --argjson height "$pre_reorg_tip" \
    --arg hash "$replacement_hash" \
    '{height: $height, hash: $hash}' \
    >"${EVIDENCE_DIR}/node/reorg-replacement-block.json"
  TARGET_TIP_HEIGHT="$post_reorg_tip"

  wait_compat_tip "$TARGET_TIP_HEIGHT" 300
  wait_complete_readiness one-block-reorg-recovered 300
  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-after-reorg.json"
  lightwalletd_grpc "{\"height\":${pre_reorg_tip}}" cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock \
    >"${EVIDENCE_DIR}/grpc/compact-block-after-reorg.json"
  jq -e \
    --slurpfile before "${EVIDENCE_DIR}/grpc/compact-block-before-reorg.json" \
    --argjson height "$pre_reorg_tip" \
    '(.height | tonumber) == $height and
     (.hash | strings | length > 0) and
     .hash != $before[0].hash' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/compact-block-after-reorg.json" ||
    die "compatibility GetBlock did not replace the invalidated block hash"
  jq -e \
    --slurpfile before "${EVIDENCE_DIR}/grpc/writer-status-before-reorg.json" \
    --argjson height "$TARGET_TIP_HEIGHT" \
    '(.fence.chainEpochId | tonumber) > ($before[0].fence.chainEpochId | tonumber) and
     (.fence.eventSequence | tonumber) > ($before[0].fence.eventSequence | tonumber) and
     (.fence.visibleTipHeight | tonumber) >= $height and
     .fence.visibleTipHash != $before[0].fence.visibleTipHash and
     .fence.canonicalSequenceDigest != $before[0].fence.canonicalSequenceDigest' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/writer-status-after-reorg.json" ||
    die "writer fence did not authenticate the replacement branch"
  lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo \
    >"${EVIDENCE_DIR}/grpc/lightd-info-after-reorg.json"
  jq -e --argjson height "$TARGET_TIP_HEIGHT" '.blockHeight | tonumber >= $height' \
    >/dev/null <"${EVIDENCE_DIR}/grpc/lightd-info-after-reorg.json" || \
    die "GetLightdInfo did not reach the post-reorg tip ${TARGET_TIP_HEIGHT}"
  record_evidence_event one-block-reorg passed \
    "replaced hash ${invalidated_hash} with ${replacement_hash} at height ${pre_reorg_tip}; replacement tip ${post_reorg_tip}"

  require_json_rpc_result reconsiderblock "[\"${invalidated_hash}\"]" \
    >"${EVIDENCE_DIR}/node/reorg-reconsider-result.json"
  INVALIDATED_REORG_HASH=""
}

run_complete_topology_certification() {
  if [[ "$CERTIFY_TOPOLOGY" != "1" ]]; then
    record_evidence_event complete-topology skipped \
      "set ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY=1 to enable regtest mutations"
    return 0
  fi
  if [[ "$NETWORK" != "zcash-regtest" ]]; then
    die "complete-topology certification is allowed only with ZINDER_OBSERVABILITY_NETWORK=zcash-regtest"
  fi
  require_regtest_mutation_preflight

  log "running opt-in complete-topology certification"
  run_controlled_projector_lag
  run_restart_certification
  run_one_block_reorg_certification
  wait_complete_readiness complete-topology-final 300
  record_evidence_event complete-topology passed \
    "lag, ordered restarts, and one-block reorg recovered to full readiness"
}

maybe_generate_blocks() {
  TARGET_TIP_HEIGHT="$BULK_CATCHUP_TO_HEIGHT"

  if [[ "$GENERATE_BLOCKS" == "0" ]]; then
    log "skipping standalone regtest block generation"
    return 0
  fi

  local before response after
  require_regtest_mutation_preflight
  before="$(node_tip_height)"
  log "requesting ${GENERATE_BLOCKS} regtest block(s) from upstream node"
  response="$(require_json_rpc_result generate "[${GENERATE_BLOCKS}]")"
  printf '%s\n' "$response" >"${EVIDENCE_DIR}/node/standalone-generated-blocks.json"

  after="$(node_tip_height)"
  if (( after > before )); then
    TARGET_TIP_HEIGHT="$after"
    log "node tip advanced from ${before} to ${after}; waiting for tip-follow commit"
    wait_compat_tip "$TARGET_TIP_HEIGHT" 120
  else
    log "node tip did not advance; ingest commit metrics may remain idle"
  fi
}

generate_traffic() {
  local range_end="$TARGET_TIP_HEIGHT"
  local range_start="$BULK_CATCHUP_FROM_HEIGHT"
  local range_limit=$((range_start + 2))
  if (( range_end > range_limit )); then
    range_end="$range_limit"
  fi

  wait_compat_tip "$TARGET_TIP_HEIGHT" 90

  run_grpc_call native-server-info \
    native_wallet_grpc '{}' zinder.v1.wallet.WalletQuery/ServerInfo
  run_grpc_call compat-latest-block \
    lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock
  run_grpc_call compat-block \
    lightwalletd_grpc "{\"height\":${TARGET_TIP_HEIGHT}}" cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock
  run_grpc_call compat-block-range \
    lightwalletd_grpc "{\"start\":{\"height\":${range_start}},\"end\":{\"height\":${range_end}}}" cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange
  run_grpc_call compat-latest-tree-state \
    lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestTreeState
  run_grpc_call compat-lightd-info \
    lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
}

run_lightwalletd_testclient() {
  if [[ "$LIGHTWALLETD_TESTCLIENT" != "1" ]]; then
    return 0
  fi

  require_command go

  if [[ -z "$LIGHTWALLETD_REPO" ]]; then
    die "ZINDER_OBSERVABILITY_LIGHTWALLETD_REPO is required when ZINDER_OBSERVABILITY_LIGHTWALLETD_TESTCLIENT=1"
  fi
  if [[ ! -d "${LIGHTWALLETD_REPO}/testclient" ]]; then
    die "lightwalletd testclient directory not found under ${LIGHTWALLETD_REPO}"
  fi

  case "$COMPAT_GRPC_ADDR" in
    127.0.0.1:9067 | localhost:9067) ;;
    *)
      die "lightwalletd testclient hard-codes localhost:9067; set ZINDER_OBSERVABILITY_COMPAT_GRPC_ADDR=127.0.0.1:9067"
      ;;
  esac

  local range_start range_end range_limit log_file
  range_start="$BULK_CATCHUP_FROM_HEIGHT"
  range_end="$TARGET_TIP_HEIGHT"
  range_limit=$((range_start + 2))
  if (( range_end > range_limit )); then
    range_end="$range_limit"
  fi
  log_file="${LOG_DIR}/lightwalletd-testclient.log"

  log "running upstream lightwalletd testclient; log: ${log_file}"
  if ! (
    cd "$LIGHTWALLETD_REPO"
    go run ./testclient -op getlightdinfo -iterations 1 -v
    go run ./testclient -op getblock -iterations 1 -v "$TARGET_TIP_HEIGHT"
    go run ./testclient -op getblockrange -iterations 1 -v "$range_start" "$range_end"
  ) >"$log_file" 2>&1; then
    tail -n 120 "$log_file" >&2 || true
    die "upstream lightwalletd testclient failed"
  fi
  log "upstream lightwalletd testclient ok"
}

print_query_summary() {
  local name="$1"
  local query="$2"
  local response sample_count

  if ! response="$(prometheus_query "$query" 2>/dev/null)"; then
    printf '%-48s unavailable\n' "$name"
    return 0
  fi

  sample_count="$(jq -r '.data.result | length' <<<"$response")"
  printf '%-48s %s sample(s)\n' "$name" "$sample_count"
  jq -r '
    .data.result[:6][]
    | "  " + (
        .metric
        | to_entries
        | map(select(.key != "__name__"))
        | map("\(.key)=\(.value)")
        | join(",")
      ) + " => " + .value[1]
  ' <<<"$response"
}

snapshot() {
  local ingest_ops_url_addr projector_ops_url_addr query_ops_url_addr compat_ops_url_addr
  ingest_ops_url_addr="$(local_url_addr "$INGEST_OPS_ADDR")"
  projector_ops_url_addr="$(local_url_addr "$PROJECTOR_OPS_ADDR")"
  query_ops_url_addr="$(local_url_addr "$QUERY_OPS_ADDR")"
  compat_ops_url_addr="$(local_url_addr "$COMPAT_OPS_ADDR")"

  log "service endpoints"
  printf '  Prometheus: http://127.0.0.1:%s\n' "$PROMETHEUS_PORT"
  printf '  Grafana:    http://127.0.0.1:%s (admin/admin unless overridden)\n' "$GRAFANA_PORT"
  printf '  Ingest ops: http://%s/metrics\n' "$ingest_ops_url_addr"
  printf '  Projector ops: http://%s/metrics\n' "$projector_ops_url_addr"
  printf '  Query ops: http://%s/metrics\n' "$query_ops_url_addr"
  printf '  Compat ops: http://%s/metrics\n' "$compat_ops_url_addr"
  printf '  Logs:       %s\n' "$LOG_DIR"

  log "Prometheus evidence"
  print_query_summary "targets up" "up{stack=\"${PROMETHEUS_STACK_LABEL}\"}"
  print_query_summary "build info" 'zinder_build_info'
  print_query_summary "readiness states" 'zinder_readiness_state'
  print_query_summary "readiness sync lag" 'zinder_readiness_sync_lag_blocks'
  print_query_summary "readiness replica lag" 'zinder_readiness_replica_lag_chain_epochs'
  print_query_summary "node requests" 'sum by (service, method, status, error_class) (zinder_node_request_total)'
  print_query_summary "canonical committed rate 5m" 'sum(rate(zinder_ingest_commit_batch_block_count_sum{status="ok"}[5m]))'
  print_query_summary "canonical committed rate 15m" 'sum(rate(zinder_ingest_commit_batch_block_count_sum{status="ok"}[15m]))'
  print_query_summary "canonical committed rate 30m" 'sum(rate(zinder_ingest_commit_batch_block_count_sum{status="ok"}[30m]))'
  print_query_summary "canonical committed rate 60m" 'sum(rate(zinder_ingest_commit_batch_block_count_sum{status="ok"}[1h]))'
  print_query_summary "canonical writer height" 'zinder_ingest_writer_tip_height'
  print_query_summary "canonical lag" 'zinder_ingest_canonical_lag_blocks'
  print_query_summary "source request average 15m" 'sum by (operation) (rate(zinder_ingest_source_request_duration_seconds_sum[15m])) / sum by (operation) (rate(zinder_ingest_source_request_duration_seconds_count[15m]))'
  print_query_summary "node request p95 15m" 'histogram_quantile(0.95, sum by (le, method) (rate(zinder_node_request_duration_seconds_bucket[15m])))'
  print_query_summary "bulk stage p95 15m" 'histogram_quantile(0.95, sum by (le, stage) (rate(zinder_ingest_bulk_pipeline_stage_duration_seconds_bucket[15m])))'
  print_query_summary "bulk queue bytes" 'zinder_ingest_bulk_pipeline_queue_bytes'
  print_query_summary "bulk reorder bytes" 'zinder_ingest_bulk_pipeline_reorder_buffer_bytes'
  print_query_summary "bulk watermark blocked" 'sum by (stage) (rate(zinder_ingest_bulk_pipeline_watermark_blocked_total[15m]))'
  print_query_summary "memory pressure 15m" 'avg_over_time(zinder_ingest_memory_pressure_ratio[15m])'
  print_query_summary "materialized-view replay state" 'zinder_materialized_view_replay_budget_state'
  print_query_summary "materialized-view replay effective batch" 'zinder_materialized_view_replay_effective_batch_blocks'
  print_query_summary "ingest commits" 'sum by (service, status, error_class) (zinder_ingest_commit_duration_seconds_count)'
  print_query_summary "ingest writer progress" 'zinder_ingest_writer_chain_epoch_id'
  print_query_summary "ingest writer status requests" 'sum by (service, status, error_class) (zinder_ingest_writer_status_request_total)'
  print_query_summary "ingest writer status available" 'zinder_ingest_writer_status_available'
  print_query_summary "compat wallet-serving pair publications" 'zinder_compat_lightwalletd_wallet_serving_pair_publisher_publications_total'
  print_query_summary "compat wallet-serving pair convergence" 'sum by (outcome) (zinder_compat_lightwalletd_wallet_serving_pair_publisher_convergence_total)'
  print_query_summary "compat wallet-serving pair replica lag" 'zinder_compat_lightwalletd_wallet_serving_pair_publisher_replica_lag_chain_epochs'
  print_query_summary "compat writer-status requests" 'sum by (status, error_class) (zinder_compat_lightwalletd_writer_status_total)'
  print_query_summary "compat writer-status available" 'zinder_compat_lightwalletd_writer_status_available'
  print_query_summary "node rpc p95" 'histogram_quantile(0.95, sum by (le, method) (rate(zinder_node_request_duration_seconds_bucket[15m])))'
  print_query_summary "store reads" 'sum by (service, operation, table, caller, status) (zinder_store_read_duration_seconds_count)'
  print_query_summary "store read p95" 'histogram_quantile(0.95, sum by (le, operation, table, caller) (rate(zinder_store_read_duration_seconds_bucket[15m])))'
  print_query_summary "visibility seeks" 'sum by (service, artifact_family) (zinder_store_visibility_seek_total)'
  print_query_summary "rocksdb properties" 'zinder_store_rocksdb_property'
}

wait_no_traffic_blocking_readiness() {
  local timeout_seconds="${1:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  local blocking_services targets_up
  while true; do
    blocking_services="$(prometheus_max_value "sum(zinder_readiness_state{cause!~\"${TRAFFIC_READY_READINESS_CAUSES}\"} == 1) or vector(0)")"
    targets_up="$(prometheus_max_value "sum(up{stack=\"${PROMETHEUS_STACK_LABEL}\"})")"
    if jq -en \
      --arg blocking "$blocking_services" \
      --arg targets "$targets_up" \
      '($blocking | tonumber) == 0 and ($targets | tonumber) == 4' \
      >/dev/null 2>&1; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      die "traffic-blocking readiness remained active or scrape targets were missing: blocking=${blocking_services}, targets_up=${targets_up}"
    fi
    sleep 1
  done
}

archive_runtime_evidence() {
  local service metrics_url commit_sha git_describe node_discovery
  for service in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd; do
    metrics_url="http://$(local_url_addr "$(service_ops_addr "$service")")/metrics"
    curl -fsS "$metrics_url" >"${EVIDENCE_DIR}/metrics/${service}.prom"
  done
  canonical_control_grpc >"${EVIDENCE_DIR}/grpc/writer-status-final.json"
  native_wallet_grpc '{}' zinder.v1.wallet.WalletQuery/ServerInfo \
    >"${EVIDENCE_DIR}/grpc/native-server-info-final.json"
  lightwalletd_grpc '{}' cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo \
    >"${EVIDENCE_DIR}/grpc/lightd-info-final.json"
  node_discovery="$(json_rpc rpc.discover '[]')" || die "could not archive Zebra OpenRPC discovery"
  printf '%s\n' "$node_discovery" >"${EVIDENCE_DIR}/node/openrpc-discovery.json"
  if [[ "$CERTIFY_TOPOLOGY" == "1" ]] &&
    ! jq -e '.error == null and (.result.info.version | strings | length > 0)' \
      >/dev/null <"${EVIDENCE_DIR}/node/openrpc-discovery.json"; then
    die "complete-topology certification requires Zebra version evidence from rpc.discover"
  fi

  cp "${LOG_DIR}"/*.log "${EVIDENCE_DIR}/logs/"
  commit_sha="$(git -C "$ROOT_DIR" rev-parse HEAD)"
  git_describe="$(git -C "$ROOT_DIR" describe --always --dirty --tags 2>/dev/null || printf '%s' "$commit_sha")"
  git -C "$ROOT_DIR" status --short >"${EVIDENCE_DIR}/git-status.txt"
  jq -s '.' "$EVIDENCE_EVENTS_FILE" >"${EVIDENCE_DIR}/events.json"
  jq -n \
    --arg run_id "$RUN_ID" \
    --arg generated_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg network "$NETWORK" \
    --arg node_json_rpc_addr "$NODE_ADDR" \
    --argjson node_tip_height "$(node_tip_height)" \
    --arg commit_sha "$commit_sha" \
    --arg git_describe "$git_describe" \
    --argjson topology_certification_enabled "$([[ "$CERTIFY_TOPOLOGY" == "1" ]] && printf true || printf false)" \
    --slurpfile node_discovery "${EVIDENCE_DIR}/node/openrpc-discovery.json" \
    --slurpfile lightd_info "${EVIDENCE_DIR}/grpc/lightd-info-final.json" \
    --slurpfile writer_status "${EVIDENCE_DIR}/grpc/writer-status-final.json" \
    --slurpfile events "${EVIDENCE_DIR}/events.json" \
    '{
      run_id: $run_id,
      generated_at: $generated_at,
      network: $network,
      node: {
        json_rpc_addr: $node_json_rpc_addr,
        tip_height: $node_tip_height,
        discovery: $node_discovery[0]
      },
      source: {
        commit_sha: $commit_sha,
        git_describe: $git_describe
      },
      topology_certification_enabled: $topology_certification_enabled,
      final: {
        writer_status: $writer_status[0],
        lightd_info: $lightd_info[0]
      },
      events: $events[0],
      artifacts: {
        readiness: "readiness/",
        metrics: "metrics/",
        grpc: "grpc/",
        node: "node/",
        logs: "logs/",
        git_status: "git-status.txt"
      }
    }' >"${EVIDENCE_DIR}/manifest.json"
  log "evidence manifest: ${EVIDENCE_DIR}/manifest.json"
}

write_readiness_report() {
  local generated_at
  local latest_json="${REPORT_DIR}/latest-readiness.json"
  local latest_markdown="${REPORT_DIR}/latest-readiness.md"
  local report_ingest_metrics_url
  local report_projector_metrics_url
  local report_query_metrics_url
  local report_compat_metrics_url
  local report_targets_up
  local report_traffic_blocking_services
  local report_readiness_warning_services
  local report_readiness_sync_lag_blocks
  local report_readiness_replica_lag_chain_epochs
  local report_canonical_writer_height
  local report_canonical_lag_blocks
  local report_canonical_rate_blocks_per_second
  local report_materialized_view_replay_height
  local report_materialized_view_replay_tip_height
  local report_materialized_view_replay_lag_blocks
  local report_materialized_view_replay_rate_blocks_per_second
  local report_materialized_view_replay_phase_gate
  local report_materialized_view_replay_caught_up
  local report_memory_pressure_ratio
  local report_node_rpc_p95_seconds
  local report_store_read_p95_seconds
  local report_compat_wallet_serving_pair_publisher_publication_count
  local report_compat_wallet_serving_pair_publisher_replica_lag_chain_epochs
  local report_rocksdb_pending_compaction_bytes
  local report_rocksdb_running_compactions
  local report_rocksdb_property_samples
  local report_ingest_commit_count
  local report_node_request_count

  generated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  [[ -n "$RUN_ID" ]] || die "run evidence identity was not initialized"
  REPORT_JSON_PATH="${REPORT_DIR}/${RUN_ID}.json"
  REPORT_MARKDOWN_PATH="${REPORT_DIR}/${RUN_ID}.md"
  report_ingest_metrics_url="http://$(local_url_addr "$INGEST_OPS_ADDR")/metrics"
  report_projector_metrics_url="http://$(local_url_addr "$PROJECTOR_OPS_ADDR")/metrics"
  report_query_metrics_url="http://$(local_url_addr "$QUERY_OPS_ADDR")/metrics"
  report_compat_metrics_url="http://$(local_url_addr "$COMPAT_OPS_ADDR")/metrics"
  report_targets_up="$(prometheus_max_value "sum(up{stack=\"${PROMETHEUS_STACK_LABEL}\"})")"
  report_traffic_blocking_services="$(prometheus_max_value "sum(zinder_readiness_state{cause!~\"${TRAFFIC_READY_READINESS_CAUSES}\"} == 1) or vector(0)")"
  report_readiness_warning_services="$(prometheus_max_value "sum(zinder_readiness_state{cause=~\"${READINESS_WARNING_CAUSES}\"} == 1) or vector(0)")"
  report_readiness_sync_lag_blocks="$(prometheus_max_value 'max(zinder_readiness_sync_lag_blocks)')"
  report_readiness_replica_lag_chain_epochs="$(prometheus_max_value 'max(zinder_readiness_replica_lag_chain_epochs)')"
  report_canonical_writer_height="$(prometheus_max_value 'max(zinder_ingest_writer_tip_height)')"
  report_canonical_lag_blocks="$(prometheus_max_value 'max(zinder_ingest_canonical_lag_blocks)')"
  report_canonical_rate_blocks_per_second="$(prometheus_max_value 'sum(rate(zinder_ingest_commit_batch_block_count_sum{status="ok"}[5m]))')"
  report_materialized_view_replay_height="$(prometheus_max_value 'max(zinder_materialized_view_replay_height)')"
  report_materialized_view_replay_tip_height="$(prometheus_max_value 'max(zinder_materialized_view_replay_tip_height)')"
  report_materialized_view_replay_lag_blocks="$(prometheus_max_value 'max(zinder_materialized_view_replay_lag_blocks)')"
  report_materialized_view_replay_rate_blocks_per_second="$(prometheus_max_value 'sum(rate(zinder_materialized_view_replay_blocks_total{status="ok"}[5m]))')"
  report_materialized_view_replay_phase_gate="$(prometheus_max_value 'max(zinder_materialized_view_replay_phase_gate)')"
  report_materialized_view_replay_caught_up="$(prometheus_max_value 'max(zinder_materialized_view_replay_caught_up)')"
  report_memory_pressure_ratio="$(prometheus_max_value 'max(zinder_ingest_memory_pressure_ratio)')"
  report_node_rpc_p95_seconds="$(prometheus_max_value 'max(histogram_quantile(0.95, sum by (le, method) (rate(zinder_node_request_duration_seconds_bucket[15m]))))')"
  report_store_read_p95_seconds="$(prometheus_max_value 'max(histogram_quantile(0.95, sum by (le, operation, table, caller) (rate(zinder_store_read_duration_seconds_bucket[15m]))))')"
  report_compat_wallet_serving_pair_publisher_publication_count="$(prometheus_max_value 'sum(zinder_compat_lightwalletd_wallet_serving_pair_publisher_publications_total)')"
  report_compat_wallet_serving_pair_publisher_replica_lag_chain_epochs="$(prometheus_max_value 'max(zinder_compat_lightwalletd_wallet_serving_pair_publisher_replica_lag_chain_epochs)')"
  report_rocksdb_pending_compaction_bytes="$(prometheus_max_value 'max(zinder_store_rocksdb_property{property="rocksdb.estimate-pending-compaction-bytes"})')"
  report_rocksdb_running_compactions="$(prometheus_max_value 'max(zinder_store_rocksdb_property{property="rocksdb.num-running-compactions"})')"
  report_rocksdb_property_samples="$(prometheus_sample_count 'zinder_store_rocksdb_property')"
  report_ingest_commit_count="$(prometheus_max_value 'sum(zinder_ingest_commit_duration_seconds_count)')"
  report_node_request_count="$(prometheus_max_value 'sum(zinder_node_request_total)')"

  export REPORT_GENERATED_AT="$generated_at"
  export REPORT_RUN_ID="$RUN_ID"
  export REPORT_NETWORK="$NETWORK"
  export REPORT_NODE_ADDR="$NODE_ADDR"
  REPORT_NODE_TIP_HEIGHT="$(node_tip_height)"
  export REPORT_NODE_TIP_HEIGHT
  export REPORT_CHECKPOINT_HEIGHT="$CHECKPOINT_HEIGHT"
  export REPORT_BULK_CATCHUP_FROM_HEIGHT="$BULK_CATCHUP_FROM_HEIGHT"
  export REPORT_BULK_CATCHUP_TO_HEIGHT="$BULK_CATCHUP_TO_HEIGHT"
  export REPORT_BULK_CATCHUP_BLOCKS="$((BULK_CATCHUP_TO_HEIGHT - CHECKPOINT_HEIGHT))"
  export REPORT_BULK_CATCHUP_SECONDS="$BULK_CATCHUP_SECONDS"
  export REPORT_RESTORE_STATUS="$RESTORE_STATUS"
  export REPORT_RESTORE_ERROR_CLASS="$RESTORE_ERROR_CLASS"
  export REPORT_PROMETHEUS_URL="http://127.0.0.1:${PROMETHEUS_PORT}"
  export REPORT_GRAFANA_URL="http://127.0.0.1:${GRAFANA_PORT}"
  export REPORT_INGEST_METRICS_URL="$report_ingest_metrics_url"
  export REPORT_PROJECTOR_METRICS_URL="$report_projector_metrics_url"
  export REPORT_QUERY_METRICS_URL="$report_query_metrics_url"
  export REPORT_COMPAT_METRICS_URL="$report_compat_metrics_url"
  export REPORT_TARGETS_UP="$report_targets_up"
  export REPORT_TRAFFIC_BLOCKING_SERVICES="$report_traffic_blocking_services"
  export REPORT_READINESS_WARNING_SERVICES="$report_readiness_warning_services"
  export REPORT_READINESS_SYNC_LAG_BLOCKS="$report_readiness_sync_lag_blocks"
  export REPORT_READINESS_REPLICA_LAG_CHAIN_EPOCHS="$report_readiness_replica_lag_chain_epochs"
  export REPORT_CANONICAL_WRITER_HEIGHT="$report_canonical_writer_height"
  export REPORT_CANONICAL_LAG_BLOCKS="$report_canonical_lag_blocks"
  export REPORT_CANONICAL_RATE_BLOCKS_PER_SECOND="$report_canonical_rate_blocks_per_second"
  export REPORT_MATERIALIZED_VIEW_REPLAY_HEIGHT="$report_materialized_view_replay_height"
  export REPORT_MATERIALIZED_VIEW_REPLAY_TIP_HEIGHT="$report_materialized_view_replay_tip_height"
  export REPORT_MATERIALIZED_VIEW_REPLAY_LAG_BLOCKS="$report_materialized_view_replay_lag_blocks"
  export REPORT_MATERIALIZED_VIEW_REPLAY_RATE_BLOCKS_PER_SECOND="$report_materialized_view_replay_rate_blocks_per_second"
  export REPORT_MATERIALIZED_VIEW_REPLAY_PHASE_GATE="$report_materialized_view_replay_phase_gate"
  export REPORT_MATERIALIZED_VIEW_REPLAY_CAUGHT_UP="$report_materialized_view_replay_caught_up"
  export REPORT_MEMORY_PRESSURE_RATIO="$report_memory_pressure_ratio"
  export REPORT_NODE_RPC_P95_SECONDS="$report_node_rpc_p95_seconds"
  export REPORT_STORE_READ_P95_SECONDS="$report_store_read_p95_seconds"
  export REPORT_COMPAT_WALLET_SERVING_PAIR_PUBLICATION_COUNT="$report_compat_wallet_serving_pair_publisher_publication_count"
  export REPORT_COMPAT_WALLET_SERVING_PAIR_REPLICA_LAG_CHAIN_EPOCHS="$report_compat_wallet_serving_pair_publisher_replica_lag_chain_epochs"
  export REPORT_ROCKSDB_PENDING_COMPACTION_BYTES="$report_rocksdb_pending_compaction_bytes"
  export REPORT_ROCKSDB_RUNNING_COMPACTIONS="$report_rocksdb_running_compactions"
  export REPORT_ROCKSDB_PROPERTY_SAMPLES="$report_rocksdb_property_samples"
  export REPORT_INGEST_COMMIT_COUNT="$report_ingest_commit_count"
  export REPORT_NODE_REQUEST_COUNT="$report_node_request_count"

  python3 - "$REPORT_JSON_PATH" "$REPORT_MARKDOWN_PATH" <<'PY'
import json
import os
import sys
from pathlib import Path

json_path = Path(sys.argv[1])
markdown_path = Path(sys.argv[2])


def metric(name):
    value = os.environ[name]
    if value == "null" or value == "":
        return None
    if any(ch in value for ch in (".", "e", "E")):
        return float(value)
    return int(value)


report = {
    "run_id": os.environ["REPORT_RUN_ID"],
    "generated_at": os.environ["REPORT_GENERATED_AT"],
    "network": os.environ["REPORT_NETWORK"],
    "node": {
        "json_rpc_addr": os.environ["REPORT_NODE_ADDR"],
        "tip_height": metric("REPORT_NODE_TIP_HEIGHT"),
    },
    "checkpoint": {
        "height": metric("REPORT_CHECKPOINT_HEIGHT"),
        "bulk_catchup_from_height": metric("REPORT_BULK_CATCHUP_FROM_HEIGHT"),
        "bulk_catchup_to_height": metric("REPORT_BULK_CATCHUP_TO_HEIGHT"),
        "bulk_catchup_blocks": metric("REPORT_BULK_CATCHUP_BLOCKS"),
    },
    "measurements": {
        "bulk_catchup_seconds": metric("REPORT_BULK_CATCHUP_SECONDS"),
        "targets_up": metric("REPORT_TARGETS_UP"),
        "traffic_blocking_services": metric("REPORT_TRAFFIC_BLOCKING_SERVICES"),
        "readiness_warning_services": metric("REPORT_READINESS_WARNING_SERVICES"),
        "readiness_sync_lag_blocks": metric("REPORT_READINESS_SYNC_LAG_BLOCKS"),
        "readiness_replica_lag_chain_epochs": metric("REPORT_READINESS_REPLICA_LAG_CHAIN_EPOCHS"),
        "canonical_writer_height": metric("REPORT_CANONICAL_WRITER_HEIGHT"),
        "canonical_lag_blocks": metric("REPORT_CANONICAL_LAG_BLOCKS"),
        "canonical_rate_blocks_per_second": metric("REPORT_CANONICAL_RATE_BLOCKS_PER_SECOND"),
        "materialized_view_replay_height": metric("REPORT_MATERIALIZED_VIEW_REPLAY_HEIGHT"),
        "materialized_view_replay_tip_height": metric("REPORT_MATERIALIZED_VIEW_REPLAY_TIP_HEIGHT"),
        "materialized_view_replay_lag_blocks": metric("REPORT_MATERIALIZED_VIEW_REPLAY_LAG_BLOCKS"),
        "materialized_view_replay_rate_blocks_per_second": metric("REPORT_MATERIALIZED_VIEW_REPLAY_RATE_BLOCKS_PER_SECOND"),
        "materialized_view_replay_phase_gate": metric("REPORT_MATERIALIZED_VIEW_REPLAY_PHASE_GATE"),
        "materialized_view_replay_caught_up": metric("REPORT_MATERIALIZED_VIEW_REPLAY_CAUGHT_UP"),
        "memory_pressure_ratio": metric("REPORT_MEMORY_PRESSURE_RATIO"),
        "node_rpc_p95_max_seconds": metric("REPORT_NODE_RPC_P95_SECONDS"),
        "store_read_p95_max_seconds": metric("REPORT_STORE_READ_P95_SECONDS"),
        "compat_wallet_serving_pair_publisher_publication_count": metric("REPORT_COMPAT_WALLET_SERVING_PAIR_PUBLICATION_COUNT"),
        "compat_wallet_serving_pair_publisher_replica_lag_chain_epochs": metric("REPORT_COMPAT_WALLET_SERVING_PAIR_REPLICA_LAG_CHAIN_EPOCHS"),
        "rocksdb_pending_compaction_bytes": metric("REPORT_ROCKSDB_PENDING_COMPACTION_BYTES"),
        "rocksdb_running_compactions": metric("REPORT_ROCKSDB_RUNNING_COMPACTIONS"),
        "rocksdb_property_samples": metric("REPORT_ROCKSDB_PROPERTY_SAMPLES"),
        "ingest_commit_count": metric("REPORT_INGEST_COMMIT_COUNT"),
        "node_request_count": metric("REPORT_NODE_REQUEST_COUNT"),
    },
    "restore": {
        "status": os.environ["REPORT_RESTORE_STATUS"],
        "error_class": os.environ["REPORT_RESTORE_ERROR_CLASS"],
    },
    "endpoints": {
        "prometheus": os.environ["REPORT_PROMETHEUS_URL"],
        "grafana": os.environ["REPORT_GRAFANA_URL"],
        "ingest_metrics": os.environ["REPORT_INGEST_METRICS_URL"],
        "projector_metrics": os.environ["REPORT_PROJECTOR_METRICS_URL"],
        "query_metrics": os.environ["REPORT_QUERY_METRICS_URL"],
        "compat_metrics": os.environ["REPORT_COMPAT_METRICS_URL"],
    },
}

json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

measurements = report["measurements"]
lines = [
    f"# Zinder Readiness Report: {report['network']}",
    "",
    f"- Run ID: `{report['run_id']}`",
    f"- Generated: `{report['generated_at']}`",
    f"- Node tip: `{report['node']['tip_height']}`",
    f"- Checkpoint height: `{report['checkpoint']['height']}`",
    f"- Bulk-catchup range: `{report['checkpoint']['bulk_catchup_from_height']}..{report['checkpoint']['bulk_catchup_to_height']}`",
    f"- Bulk-catchup duration: `{measurements['bulk_catchup_seconds']}` seconds",
    f"- Targets up: `{measurements['targets_up']}`",
    f"- Traffic-blocking services: `{measurements['traffic_blocking_services']}`",
    f"- Readiness warning services: `{measurements['readiness_warning_services']}`",
    f"- Sync lag: `{measurements['readiness_sync_lag_blocks']}` blocks",
    f"- Replica lag: `{measurements['readiness_replica_lag_chain_epochs']}` chain epochs",
    f"- Canonical writer height: `{measurements['canonical_writer_height']}`",
    f"- Canonical lag: `{measurements['canonical_lag_blocks']}` blocks",
    f"- Canonical rate: `{measurements['canonical_rate_blocks_per_second']}` blocks/second",
    f"- Materialized-view replay height: `{measurements['materialized_view_replay_height']}`",
    f"- Materialized-view replay tip: `{measurements['materialized_view_replay_tip_height']}`",
    f"- Materialized-view replay lag: `{measurements['materialized_view_replay_lag_blocks']}` blocks",
    f"- Materialized-view replay rate: `{measurements['materialized_view_replay_rate_blocks_per_second']}` blocks/second",
    f"- Materialized-view phase gate: `{measurements['materialized_view_replay_phase_gate']}`",
    f"- Materialized views caught up: `{measurements['materialized_view_replay_caught_up']}`",
    f"- Memory pressure ratio: `{measurements['memory_pressure_ratio']}`",
    f"- Node RPC p95 max: `{measurements['node_rpc_p95_max_seconds']}` seconds",
    f"- Store read p95 max: `{measurements['store_read_p95_max_seconds']}` seconds",
    f"- Compat wallet-serving pair publications: `{measurements['compat_wallet_serving_pair_publisher_publication_count']}`",
    f"- Compat wallet-serving pair replica lag: `{measurements['compat_wallet_serving_pair_publisher_replica_lag_chain_epochs']}` chain epochs",
    f"- RocksDB pending compaction bytes: `{measurements['rocksdb_pending_compaction_bytes']}`",
    f"- RocksDB running compactions: `{measurements['rocksdb_running_compactions']}`",
    f"- RocksDB property samples: `{measurements['rocksdb_property_samples']}`",
    f"- Restore: `{report['restore']['status']}`",
    "",
    "## Endpoints",
    "",
    f"- Prometheus: {report['endpoints']['prometheus']}",
    f"- Grafana: {report['endpoints']['grafana']}",
    f"- Ingest metrics: {report['endpoints']['ingest_metrics']}",
    f"- Projector metrics: {report['endpoints']['projector_metrics']}",
    f"- Compat metrics: {report['endpoints']['compat_metrics']}",
    "",
]
markdown_path.write_text("\n".join(lines), encoding="utf-8")
PY

  cp "$REPORT_JSON_PATH" "$latest_json"
  cp "$REPORT_MARKDOWN_PATH" "$latest_markdown"
  log "readiness report: ${REPORT_JSON_PATH}"
  log "readiness summary: ${REPORT_MARKDOWN_PATH}"
}

write_calibration_report() {
  local aggregate_suffix aggregate_json aggregate_markdown latest_json latest_markdown
  aggregate_suffix="$(date -u +"%Y%m%dT%H%M%SZ")"
  aggregate_json="${REPORT_DIR}/calibration-${NETWORK}-${aggregate_suffix}.json"
  aggregate_markdown="${REPORT_DIR}/calibration-${NETWORK}-${aggregate_suffix}.md"
  latest_json="${REPORT_DIR}/latest-calibration.json"
  latest_markdown="${REPORT_DIR}/latest-calibration.md"

  python3 - "$aggregate_json" "$aggregate_markdown" "$@" <<'PY'
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

aggregate_json = Path(sys.argv[1])
aggregate_markdown = Path(sys.argv[2])
report_paths = [Path(path) for path in sys.argv[3:]]
reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]


def percentile(values, percentile_value):
    if not values:
        return None
    sorted_values = sorted(values)
    index = max(0, math.ceil((percentile_value / 100) * len(sorted_values)) - 1)
    return sorted_values[min(index, len(sorted_values) - 1)]


def measurement_values(name):
    values = []
    for report in reports:
        value = report["measurements"].get(name)
        if value is not None:
            values.append(value)
    return values


metric_names = [
    "bulk_catchup_seconds",
    "node_rpc_p95_max_seconds",
    "store_read_p95_max_seconds",
    "compat_wallet_serving_pair_publisher_replica_lag_chain_epochs",
    "readiness_sync_lag_blocks",
    "readiness_replica_lag_chain_epochs",
    "rocksdb_pending_compaction_bytes",
]

summary = {}
for name in metric_names:
    values = measurement_values(name)
    summary[name] = {
        "samples": len(values),
        "p50": statistics.median(values) if values else None,
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "worst_case": max(values) if values else None,
    }

aggregate = {
    "generated_at": datetime.now(timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z"),
    "network": reports[0]["network"] if reports else None,
    "run_count": len(reports),
    "runs": [
        {
            "path": str(path),
            "run_id": report["run_id"],
            "checkpoint_height": report["checkpoint"]["height"],
            "bulk_catchup_to_height": report["checkpoint"]["bulk_catchup_to_height"],
            "bulk_catchup_seconds": report["measurements"]["bulk_catchup_seconds"],
            "store_read_p95_max_seconds": report["measurements"]["store_read_p95_max_seconds"],
            "compat_wallet_serving_pair_publisher_publication_count": report["measurements"]["compat_wallet_serving_pair_publisher_publication_count"],
            "restore_status": report["restore"]["status"],
        }
        for path, report in zip(report_paths, reports)
    ],
    "summary": summary,
}

aggregate_json.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    f"# Zinder Calibration Baseline: {aggregate['network']}",
    "",
    f"- Generated: `{aggregate['generated_at']}`",
    f"- Runs: `{aggregate['run_count']}`",
    "",
    "| Metric | Samples | P50 | P95 | P99 | Worst case |",
    "| --- | ---: | ---: | ---: | ---: | ---: |",
]
for name in metric_names:
    metric = summary[name]
    lines.append(
        f"| `{name}` | {metric['samples']} | {metric['p50']} | {metric['p95']} | "
        f"{metric['p99']} | {metric['worst_case']} |"
    )
lines.append("")
aggregate_markdown.write_text("\n".join(lines), encoding="utf-8")
PY

  cp "$aggregate_json" "$latest_json"
  cp "$aggregate_markdown" "$latest_markdown"
  log "calibration report: ${aggregate_json}"
  log "calibration summary: ${aggregate_markdown}"
}

run_stack() {
  require_commands
  validate_harness_configuration

  local tip_height effective_bulk_catchup_blocks
  log "checking node ${NODE_ADDR} on ${NETWORK}"
  tip_height="$(node_tip_height)" || die "node did not return a tip height"
  if (( tip_height < 2 )); then
    die "node tip ${tip_height} is too low for checkpoint bulk catchup"
  fi

  effective_bulk_catchup_blocks="$BULK_CATCHUP_BLOCKS"
  if (( effective_bulk_catchup_blocks >= tip_height )); then
    effective_bulk_catchup_blocks=$((tip_height - 1))
  fi
  if (( effective_bulk_catchup_blocks < 1 )); then
    die "effective bulk catchup window must be at least one block"
  fi

  CHECKPOINT_HEIGHT=$((tip_height - effective_bulk_catchup_blocks))
  BULK_CATCHUP_FROM_HEIGHT=$((CHECKPOINT_HEIGHT + 1))
  BULK_CATCHUP_TO_HEIGHT="$tip_height"
  export CHECKPOINT_HEIGHT BULK_CATCHUP_FROM_HEIGHT BULK_CATCHUP_TO_HEIGHT TARGET_TIP_HEIGHT

  stop_services
  prepare_work_dir
  initialize_run_evidence
  write_configs
  build_binaries

  log "starting Prometheus and Grafana"
  docker_compose up -d
  wait_http prometheus "http://127.0.0.1:${PROMETHEUS_PORT}/-/healthy" 90
  reload_prometheus
  wait_http grafana "http://127.0.0.1:${GRAFANA_PORT}/api/health" 120

  run_bulk_catchup_seed
  record_restore_unavailability

  # Long-running ingest. No --target-height here, so the loop
  # runs indefinitely (transitioning to FollowingTip once the store
  # catches up to the upstream tip).
  start_service zinder-ingest
  wait_http zinder-ingest "http://$(local_url_addr "$INGEST_OPS_ADDR")/healthz" 60

  # The projector is the sole wallet primary owner. Wait for its exact
  # canonical/wallet publication before opening either reader's secondaries.
  start_service zinder-projector
  wait_http zinder-projector "http://$(local_url_addr "$PROJECTOR_OPS_ADDR")/readyz" 900

  start_service zinder-query
  start_service zinder-compat-lightwalletd
  wait_complete_readiness initial-topology-ready 900

  maybe_generate_blocks
  generate_traffic
  run_lightwalletd_testclient
  run_complete_topology_certification

  wait_prometheus_samples "target scrape" "up{stack=\"${PROMETHEUS_STACK_LABEL}\"}" 45 || true
  wait_prometheus_samples "readiness metric" 'zinder_readiness_state' 45 || true
  wait_prometheus_samples "compat wallet-serving-pair publication metric" 'zinder_compat_lightwalletd_wallet_serving_pair_publisher_publications_total' 45 || true
  wait_prometheus_samples "compat wallet-serving-pair convergence metric" 'zinder_compat_lightwalletd_wallet_serving_pair_publisher_convergence_total' 45 || true
  wait_prometheus_samples "store read metric" 'zinder_store_read_duration_seconds_count' 45 || true
  wait_prometheus_samples "rocksdb property metric" 'zinder_store_rocksdb_property' 45 || true
  wait_prometheus_samples "ingest commit metric" 'zinder_ingest_commit_duration_seconds_count' 45 || true
  wait_prometheus_samples "ingest writer progress metric" 'zinder_ingest_writer_chain_epoch_id' 45 || true
  wait_prometheus_samples "ingest writer status metric" 'zinder_ingest_writer_status_request_total' 45 || true

  wait_complete_readiness final-traffic-ready 300
  wait_no_traffic_blocking_readiness 60
  snapshot
  archive_runtime_evidence
  write_readiness_report

  log "smoke run is still running so dashboards stay inspectable"
  log "stop it with: scripts/observability-smoke.sh stop"
}

calibrate_stack() {
  require_commands

  if ! [[ "$CALIBRATION_RUNS" =~ ^[0-9]+$ ]] || (( CALIBRATION_RUNS < 1 )); then
    die "ZINDER_OBSERVABILITY_RUNS must be a positive integer"
  fi

  local reports=()
  local run_index
  for ((run_index = 1; run_index <= CALIBRATION_RUNS; run_index++)); do
    log "calibration run ${run_index}/${CALIBRATION_RUNS}"
    CALIBRATION_RUN_INDEX="$run_index"
    run_stack
    reports+=("$REPORT_JSON_PATH")
  done

  unset CALIBRATION_RUN_INDEX
  write_calibration_report "${reports[@]}"
}

stop_stack() {
  require_command ps
  stop_services
  if [[ -f "$COMPOSE_FILE" ]]; then
    docker_compose down
  fi
}

main() {
  local command="${1:-run}"
  trap cleanup_on_exit EXIT
  case "$command" in
    run)
      run_stack
      ;;
    calibrate)
      if [[ "$CERTIFY_TOPOLOGY" == "1" ]]; then
        die "complete-topology certification is available only through the run command"
      fi
      calibrate_stack
      ;;
    snapshot | status)
      require_commands
      snapshot
      ;;
    stop)
      require_command docker
      stop_stack
      ;;
    -h | --help | help)
      usage
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
