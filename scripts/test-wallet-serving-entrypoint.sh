#!/usr/bin/env bash
# The evaluated entrypoint functions read the settings this script assigns.
# shellcheck disable=SC2034
set -euo pipefail

# Exercises the readiness decisions of the wallet-serving runtime entrypoint.
# The entrypoint runs its topology at load time, so this test evaluates only
# its readiness functions, with the probe transport and process control
# replaced by stubs.

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
entrypoint="$repository_root/deploy/wallet-serving-runtime-entrypoint"

test_failure() {
  printf 'wallet-serving entrypoint test failed: %s\n' "$*" >&2
  exit 1
}

entrypoint_function() {
  awk -v name="$1" '
    $0 ~ "^" name "\\(\\) \\{$" { printing = 1 }
    printing { print }
    printing && /^}$/ { exit }
  ' "$entrypoint"
}

for function_name in \
  readiness_cause_is_terminal \
  readiness_cause_is_writer_halt \
  classify_probe_failure \
  probe_readiness \
  observe_plane_readiness \
  await_readiness; do
  definition="$(entrypoint_function "$function_name")"
  [ -n "$definition" ] || test_failure "entrypoint no longer defines $function_name"
  eval "$definition"
done

# Settings and state the extracted functions read from the entrypoint.
probe_connect_timeout_seconds=2
probe_max_time_seconds=5
poll_interval_seconds=15
unready_report_interval_seconds=300
probe_halted=0
response_separator='
'

fake_status=""
fake_body=""
fail_message=""
sleep_count=0

# Stands in for curl's `-w` output: the body, the separator, then the status
# code and connect time.
curl() {
  printf '%s%s%s 0.001' "$fake_body" "$response_separator" "$fake_status"
}

log() {
  :
}

assert_started_processes_alive() {
  :
}

sleep_interruptibly() {
  sleep_count=$((sleep_count + 1))
  [ "$sleep_count" -lt 3 ] || test_failure "await_readiness kept polling a plane that should have returned"
}

fail() {
  fail_message="$*"
  return 1
}

serve() {
  fake_status="$1"
  fake_body="$2"
}

halted_body='{"ready":false,"cause":{"reorg_window_exceeded":{"depth":1001,"configured":1000}},"current_height":4200}'
schema_body='{"ready":false,"cause":"schema_mismatch"}'
syncing_body='{"ready":false,"cause":{"syncing":{"lag_blocks":12}}}'
ready_body='{"ready":true,"cause":"ready"}'

readiness_cause_is_terminal schema_mismatch \
  || test_failure "schema_mismatch must stay terminal"
readiness_cause_is_terminal node_capability_missing \
  || test_failure "node_capability_missing must stay terminal"
if readiness_cause_is_terminal reorg_window_exceeded; then
  test_failure "a parked writer must not terminate the container"
fi
readiness_cause_is_writer_halt reorg_window_exceeded \
  || test_failure "reorg_window_exceeded must be served as a writer halt"
if readiness_cause_is_writer_halt syncing; then
  test_failure "syncing is not a writer halt"
fi

serve 503 "$halted_body"
probe_readiness 127.0.0.1:9106
# shellcheck disable=SC2154 # assigned by the evaluated probe_readiness
[ "$probe_cause" = "reorg_window_exceeded" ] \
  || test_failure "probe parsed cause '$probe_cause' from a parked writer's readiness"

serve 503 "$halted_body"
observe_plane_readiness zinder-query 127.0.0.1:9106 \
  || test_failure "a plane serving the writer halt must count as serving"
[ "$probe_halted" -eq 1 ] || test_failure "a served writer halt must be reported"

serve 503 "$syncing_body"
if observe_plane_readiness zinder-query 127.0.0.1:9106; then
  test_failure "a syncing plane must count as unready"
fi
[ "$probe_halted" -eq 0 ] || test_failure "a syncing plane is not halted"

serve 200 "$ready_body"
observe_plane_readiness zinder-query 127.0.0.1:9106 \
  || test_failure "a ready plane must count as serving"
[ "$probe_halted" -eq 0 ] || test_failure "a ready plane is not halted"

serve 503 "$schema_body"
fail_message=""
observe_plane_readiness zinder-query 127.0.0.1:9106 || true
case "$fail_message" in
  *schema_mismatch*) ;;
  *) test_failure "a terminal cause must end the container" ;;
esac

serve 503 "$halted_body"
sleep_count=0
fail_message=""
await_readiness zinder-ingest 127.0.0.1:9105 60 \
  || test_failure "startup must continue past a parked writer"
[ -z "$fail_message" ] || test_failure "startup failed on a parked writer: $fail_message"
[ "$sleep_count" -eq 0 ] || test_failure "startup waited on a parked writer"

# The container healthcheck runs as its own process, so it is exercised with a
# stub curl on PATH that serves the fixture status and body.
healthcheck="$repository_root/deploy/wallet-serving-healthcheck"
stub_bin="$(mktemp -d)"
trap 'rm -rf "$stub_bin"' EXIT
cat >"$stub_bin/curl" <<'STUB'
#!/bin/sh
[ "${HEALTHCHECK_STUB_UNREACHABLE:-0}" -eq 0 ] || exit 7
printf '%s\n%s' "$HEALTHCHECK_STUB_BODY" "$HEALTHCHECK_STUB_STATUS"
STUB
chmod +x "$stub_bin/curl"

run_healthcheck() {
  HEALTHCHECK_STUB_STATUS="$1" HEALTHCHECK_STUB_BODY="$2" \
    HEALTHCHECK_STUB_UNREACHABLE="${3:-0}" PATH="$stub_bin:$PATH" sh "$healthcheck"
}

run_healthcheck 200 "$ready_body" \
  || test_failure "the healthcheck must pass a ready query plane"
run_healthcheck 503 "$halted_body" \
  || test_failure "the healthcheck must pass a query plane serving the writer halt"
if run_healthcheck 503 "$syncing_body"; then
  test_failure "the healthcheck must fail a syncing query plane"
fi
if run_healthcheck 503 "$schema_body"; then
  test_failure "the healthcheck must fail a schema mismatch"
fi
if run_healthcheck 200 "$ready_body" 1; then
  test_failure "the healthcheck must fail an unreachable query plane"
fi

printf 'wallet-serving entrypoint tests passed\n'
