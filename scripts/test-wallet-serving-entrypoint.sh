#!/usr/bin/env bash
# The evaluated entrypoint functions read the settings this script assigns.
# shellcheck disable=SC2034
set -euo pipefail

# Exercises the readiness, recurrence, and environment-scoping decisions of the
# wallet-serving runtime entrypoint. The entrypoint runs its topology at load
# time, so this test evaluates only its functions and settings, with the probe
# transport and process control replaced by stubs.

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
  readiness_cause_is_projector_park \
  plane_reports_projector_park \
  query_unready_behind_projector_park \
  classify_probe_failure \
  probe_readiness \
  observe_plane_readiness \
  await_readiness \
  update_plane_recurrence \
  zinder_variables_outside \
  start_runtime_process \
  require_positive_seconds \
  require_known_entrypoint_settings; do
  definition="$(entrypoint_function "$function_name")"
  [ -n "$definition" ] || test_failure "entrypoint no longer defines $function_name"
  eval "$definition"
done

entrypoint_setting() {
  sed -n "s/^$1=\"\\{0,1\\}\\([^\"]*\\)\"\\{0,1\\}\$/\\1/p" "$entrypoint"
}

# The configuration path lists and recurrence budgets come from the entrypoint
# itself, so this test exercises the values the image ships.
ingest_config_paths="$(entrypoint_setting ingest_config_paths)"
projector_config_paths="$(entrypoint_setting projector_config_paths)"
query_config_paths="$(entrypoint_setting query_config_paths)"
entrypoint_config_paths="$(entrypoint_setting entrypoint_config_paths)"
recurrence_clear_window_seconds="$(entrypoint_setting recurrence_clear_window_seconds)"
recurrence_episode_budget="$(entrypoint_setting recurrence_episode_budget)"
recurrence_cumulative_unready_seconds_budget="$(
  entrypoint_setting recurrence_cumulative_unready_seconds_budget
)"
recurrence_min_answered_episode_seconds="$(
  entrypoint_setting recurrence_min_answered_episode_seconds
)"
for shipped_setting in \
  "$ingest_config_paths" "$projector_config_paths" "$query_config_paths" \
  "$entrypoint_config_paths" \
  "$recurrence_clear_window_seconds" "$recurrence_episode_budget" \
  "$recurrence_cumulative_unready_seconds_budget" \
  "$recurrence_min_answered_episode_seconds"; do
  [ -n "$shipped_setting" ] || test_failure "entrypoint no longer sets a setting this test reads"
done

# Settings and state the extracted functions read from the entrypoint.
probe_connect_timeout_seconds=2
probe_max_time_seconds=5
poll_interval_seconds=15
unready_report_interval_seconds=300
probe_halted=0
probe_parked=0
response_separator='
'

fake_status=""
fake_body=""
fake_projector_status=""
fake_projector_body=""
fail_message=""
sleep_count=0
projector_ops_fixture_addr=127.0.0.1:9110

# Stands in for curl's `-w` output: the body, the separator, then the status
# code and connect time. The projector's ops address serves its own fixture
# once one is set, and every other address serves the shared one.
curl() {
  for curl_argument in "$@"; do
    curl_url="$curl_argument"
  done
  case "$curl_url" in
    *"$projector_ops_fixture_addr"*)
      if [ -n "$fake_projector_body" ]; then
        printf '%s%s%s 0.001' "$fake_projector_body" "$response_separator" \
          "$fake_projector_status"
        return 0
      fi
      ;;
    *) ;;
  esac
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

serve_projector() {
  fake_projector_status="$1"
  fake_projector_body="$2"
}

halted_body='{"ready":false,"cause":{"reorg_window_exceeded":{"depth":1001,"configured":1000}},"current_height":4200}'
schema_body='{"ready":false,"cause":"schema_mismatch"}'
syncing_body='{"ready":false,"cause":{"syncing":{"lag_blocks":12}}}'
ready_body='{"ready":true,"cause":"ready"}'
parked_body='{"ready":false,"cause":{"wallet_transition_limit_exceeded":{"required_bytes":541122672,"configured_bytes":536870912}},"current_height":4602673,"target_height":4602680}'
lagging_body='{"ready":false,"cause":{"replica_lagging":{"lag_chain_epochs":5}},"current_height":4602673}'

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
if readiness_cause_is_terminal wallet_transition_limit_exceeded; then
  test_failure "a parked projector must not terminate the container"
fi
readiness_cause_is_projector_park wallet_transition_limit_exceeded \
  || test_failure "wallet_transition_limit_exceeded must be served as a projector park"
if readiness_cause_is_projector_park reorg_window_exceeded; then
  test_failure "a writer halt is not a projector park"
fi
if readiness_cause_is_writer_halt wallet_transition_limit_exceeded; then
  test_failure "a projector park is not a writer halt"
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

serve 503 "$parked_body"
probe_readiness 127.0.0.1:9110
[ "$probe_cause" = "wallet_transition_limit_exceeded" ] \
  || test_failure "probe parsed cause '$probe_cause' from a parked projector's readiness"

serve 503 "$parked_body"
observe_plane_readiness zinder-projector 127.0.0.1:9110 \
  || test_failure "a parked projector must count as serving"
[ "$probe_parked" -eq 1 ] || test_failure "a projector park must be reported"
[ "$probe_halted" -eq 0 ] || test_failure "a projector park is not a writer halt"

serve 503 "$syncing_body"
observe_plane_readiness zinder-projector 127.0.0.1:9110 || true
[ "$probe_parked" -eq 0 ] || test_failure "a syncing projector is not parked"

serve 503 "$parked_body"
sleep_count=0
fail_message=""
await_readiness zinder-projector 127.0.0.1:9110 60 \
  || test_failure "startup must continue past a parked projector"
[ -z "$fail_message" ] || test_failure "startup failed on a parked projector: $fail_message"
[ "$sleep_count" -eq 0 ] || test_failure "startup waited on a parked projector"

# Query behind a parked projector never publishes a newer pair, so startup
# continues once its answered refusal is the park's.
serve 503 "$lagging_body"
serve_projector 503 "$parked_body"
sleep_count=0
fail_message=""
await_readiness zinder-query 127.0.0.1:9106 60 127.0.0.1:9110 \
  || test_failure "startup must continue past query behind a parked projector"
[ -z "$fail_message" ] \
  || test_failure "startup failed on query behind a parked projector: $fail_message"
[ "$sleep_count" -eq 0 ] || test_failure "startup waited on query behind a parked projector"

# Without the park, a lagging query still has to admit traffic in time.
serve 503 "$lagging_body"
serve_projector 503 "$syncing_body"
unexcused_query_message="$(
  # The evaluated await_readiness calls this stand-in, which ends the subshell.
  # shellcheck disable=SC2329
  fail() {
    printf '%s' "$*"
    exit 1
  }
  await_readiness zinder-query 127.0.0.1:9106 0 127.0.0.1:9110
)" || true
case "$unexcused_query_message" in
  *"zinder-query stayed unready"*replica_lagging*) ;;
  *) test_failure "query behind a syncing projector ended the wait: '$unexcused_query_message'" ;;
esac
serve_projector "" ""

# In the serving loop, an answered query refusal counts as serving while the
# projector is parked, and only then.
serve 503 "$lagging_body"
if observe_plane_readiness zinder-query 127.0.0.1:9106; then
  test_failure "a lagging query plane must count as unready on its own"
fi
query_unready_behind_projector_park 1 \
  || test_failure "an answered query refusal behind a parked projector must count as serving"
if query_unready_behind_projector_park 0; then
  test_failure "a query refusal without a parked projector must stay unready"
fi
probe_http_status=""
if query_unready_behind_projector_park 1; then
  test_failure "an unreachable query plane must stay unready behind a parked projector"
fi

# The park excuses only query: an unready ingest plane observed while the
# projector is parked still counts as unready.
serve 503 "$syncing_body"
if observe_plane_readiness zinder-ingest 127.0.0.1:9105; then
  test_failure "a syncing ingest plane must count as unready while the projector is parked"
fi

# Recurrence counters as "<episode_count> <flap_unready_since_epoch>
# <flap_episode_counted> <cumulative_unready_seconds> <flap_ready_since_epoch>".
recurrence_state=""
recurrence_escalate=0

reset_recurrence() {
  recurrence_state="0 0 0 0 0"
  recurrence_escalate=0
}

# Feeds one poll to update_plane_recurrence: ready, unreachable, epoch.
poll_recurrence() {
  # shellcheck disable=SC2046,SC2086 # the counters are separate arguments
  set -- $(update_plane_recurrence zinder-ingest "$1" "$2" "$3" $recurrence_state "fixture report")
  recurrence_state="$1 $2 $3 $4 $5"
  recurrence_escalate="$7"
}

recurrence_episode_count() {
  printf '%s' "${recurrence_state%% *}"
}

# A brief answered drain at every block is not an episode and spends only the
# cumulative budget.
reset_recurrence
poll_epoch=1000
for _ in $(seq 1 40); do
  poll_recurrence 0 0 "$poll_epoch"
  poll_recurrence 1 0 $((poll_epoch + 15))
  [ "$recurrence_escalate" -eq 0 ] || test_failure "brief answered drains escalated"
  poll_epoch=$((poll_epoch + 75))
done
[ "$(recurrence_episode_count)" -eq 0 ] \
  || test_failure "brief answered drains counted as episodes"

# A plane that is unready most of the time in brief answered drains escalates
# on cumulative unready time.
reset_recurrence
poll_epoch=1000
drain_cycles=0
while [ "$recurrence_escalate" -eq 0 ] && [ "$drain_cycles" -lt 100 ]; do
  poll_recurrence 0 0 "$poll_epoch"
  poll_recurrence 0 0 $((poll_epoch + 15))
  poll_recurrence 0 0 $((poll_epoch + 30))
  poll_recurrence 1 0 $((poll_epoch + 45))
  poll_epoch=$((poll_epoch + 60))
  drain_cycles=$((drain_cycles + 1))
done
[ "$recurrence_escalate" -eq 1 ] \
  || test_failure "brief answered drains 75% of the time never escalated"
[ "$(recurrence_episode_count)" -eq 0 ] \
  || test_failure "drains shorter than the minimum episode length counted as episodes"
[ "$drain_cycles" -eq $((recurrence_cumulative_unready_seconds_budget / 45)) ] \
  || test_failure "brief answered drains escalated after $drain_cycles cycles"

# An answered drain that lasts the minimum episode length counts.
reset_recurrence
poll_recurrence 0 0 2000
poll_recurrence 0 0 $((2000 + recurrence_min_answered_episode_seconds - 1))
[ "$(recurrence_episode_count)" -eq 0 ] \
  || test_failure "an answered drain counted before the minimum episode length"
poll_recurrence 0 0 $((2000 + recurrence_min_answered_episode_seconds))
[ "$(recurrence_episode_count)" -eq 1 ] \
  || test_failure "a sustained answered drain did not count as an episode"

# A drain that reaches the minimum episode length on its recovery poll counts.
reset_recurrence
poll_recurrence 0 0 2500
poll_recurrence 0 0 2545
poll_recurrence 1 0 $((2500 + recurrence_min_answered_episode_seconds))
[ "$(recurrence_episode_count)" -eq 1 ] \
  || test_failure "a drain that recovered at the minimum episode length did not count"

# An unreachable plane counts at once, and repeated short wedges escalate.
reset_recurrence
poll_epoch=3000
for _ in $(seq 1 "$recurrence_episode_budget"); do
  poll_recurrence 0 1 "$poll_epoch"
  poll_recurrence 1 0 $((poll_epoch + 15))
  poll_epoch=$((poll_epoch + 60))
done
[ "$(recurrence_episode_count)" -eq "$recurrence_episode_budget" ] \
  || test_failure "unreachable polls did not each count as an episode"
[ "$recurrence_escalate" -eq 1 ] \
  || test_failure "repeated unreachable episodes did not escalate"

# A brief answered drain restarts the sustained-recovery clock, and a full
# clear window afterwards clears the recorded episode and drain time.
reset_recurrence
poll_recurrence 0 1 4000
poll_recurrence 1 0 4015
poll_recurrence 0 0 4300
poll_recurrence 1 0 4315
poll_recurrence 1 0 $((4015 + recurrence_clear_window_seconds))
[ "$(recurrence_episode_count)" -eq 1 ] \
  || test_failure "a brief drain did not restart the sustained-recovery clock"
poll_recurrence 1 0 $((4315 + recurrence_clear_window_seconds))
[ "$recurrence_state" = "0 0 0 0 4315" ] \
  || test_failure "a full clear window left recurrence state '$recurrence_state'"

for invalid_seconds in "" 0 010 1.5 -1 soon; do
  fail_message=""
  require_positive_seconds TEST_DEADLINE_SECONDS "$invalid_seconds" || true
  [ -n "$fail_message" ] || test_failure "a deadline of '$invalid_seconds' was accepted"
done
fail_message=""
require_positive_seconds TEST_DEADLINE_SECONDS 86400 \
  || test_failure "a deadline of 86400 was rejected: $fail_message"

# A misspelled entrypoint setting stops the container instead of leaving its
# default in place.
fail_message=""
ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE=86400 \
  require_known_entrypoint_settings || true
case "$fail_message" in
  *ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE) ;;
  *) test_failure "a misspelled entrypoint setting was accepted: '$fail_message'" ;;
esac
fail_message=""
ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS=86400 \
  ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS=43200 \
  require_known_entrypoint_settings || true
[ -z "$fail_message" ] \
  || test_failure "documented entrypoint settings were rejected: $fail_message"

# Each process starts with the variables its binary accepts. A stub setpriv on
# PATH records the environment variable names each process receives.
scoping_root="$(mktemp -d)"
trap 'rm -rf "$scoping_root"' EXIT
mkdir "$scoping_root/bin" "$scoping_root/received"
cat >"$scoping_root/bin/setpriv" <<'STUB'
#!/bin/sh
shift 4
env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' \
  | sort >"$SCOPING_RECEIVED_DIR/$(basename "$1")"
STUB
chmod +x "$scoping_root/bin/setpriv"

# The subshell keeps the fixture environment out of the rest of this test.
(
  export SCOPING_RECEIVED_DIR="$scoping_root/received"
  export ZINDER_NETWORK__NAME=zcash-testnet
  export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232
  export ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS=300000
  export ZINDER_WALLET__SECONDARY_PATH=/var/lib/zinder/query/wallet-secondary
  export ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS=500
  export ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS=48
  export ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER=128849018880
  export ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS=8
  export ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS=86400
  export ZINDER_EXPLORER__LISTEN_ADDR=127.0.0.1:9068
  export INGEST_CONTROL_BEARER_TOKEN=fixture-token
  export CHECKPOINT_BEARER_TOKEN=fixture-token
  stub_path="$scoping_root/bin:$PATH"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-ingest /etc/zinder/ingest.toml "$ingest_config_paths"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-projector /etc/zinder/projector.toml "$projector_config_paths"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-query /etc/zinder/query.toml "$query_config_paths"
  wait
)

# Asserts which processes received a variable: "<variable> <process>...".
assert_received_by() {
  variable_name="$1"
  shift
  for process_name in zinder-ingest zinder-projector zinder-query; do
    received_file="$scoping_root/received/$process_name"
    [ -f "$received_file" ] || test_failure "$process_name did not start"
    expected=0
    for owner in "$@"; do
      [ "$owner" != "$process_name" ] || expected=1
    done
    if grep -qx "$variable_name" "$received_file"; then
      [ "$expected" -eq 1 ] || test_failure "$process_name received $variable_name"
    else
      [ "$expected" -eq 0 ] || test_failure "$process_name did not receive $variable_name"
    fi
  done
}

assert_received_by ZINDER_NETWORK__NAME zinder-ingest zinder-projector zinder-query
assert_received_by ZINDER_NODE__JSON_RPC_ADDR zinder-ingest zinder-projector zinder-query
assert_received_by ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES \
  zinder-ingest zinder-projector zinder-query
assert_received_by ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES zinder-ingest
assert_received_by ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS zinder-query
assert_received_by ZINDER_WALLET__SECONDARY_PATH zinder-query
assert_received_by ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS zinder-ingest
assert_received_by ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS zinder-ingest
assert_received_by ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER zinder-projector
assert_received_by ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES zinder-projector zinder-query
assert_received_by ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS zinder-query
assert_received_by ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS
assert_received_by ZINDER_EXPLORER__LISTEN_ADDR
assert_received_by INGEST_CONTROL_BEARER_TOKEN
assert_received_by CHECKPOINT_BEARER_TOKEN

unread_names="$(
  ZINDER_EXPLORER__LISTEN_ADDR=127.0.0.1:9068 \
    ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS=43200 \
    ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS=8 \
    ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES=64 \
    ZINDER_STORAGE__CANONICAL_PAHT=/misspelled \
    zinder_variables_outside \
    "$ingest_config_paths $projector_config_paths $query_config_paths $entrypoint_config_paths" \
    | sort | tr '\n' ' '
)"
[ "$unread_names" = "ZINDER_EXPLORER__LISTEN_ADDR ZINDER_STORAGE__CANONICAL_PAHT " ] \
  || test_failure "variables no process reads were reported as '$unread_names'"

# The container healthcheck runs as its own process, so it is exercised with a
# stub curl on PATH that serves the fixture status and body.
healthcheck="$repository_root/deploy/wallet-serving-healthcheck"
stub_bin="$(mktemp -d)"
trap 'rm -rf "$scoping_root" "$stub_bin"' EXIT
cat >"$stub_bin/curl" <<'STUB'
#!/bin/sh
[ "${HEALTHCHECK_STUB_UNREACHABLE:-0}" -eq 0 ] || exit 7
for argument in "$@"; do
  url="$argument"
done
case "$url" in
  *:9110/readyz)
    printf '%s' "$HEALTHCHECK_STUB_PROJECTOR_BODY"
    exit 0
    ;;
  *) ;;
esac
printf '%s\n%s' "$HEALTHCHECK_STUB_BODY" "$HEALTHCHECK_STUB_STATUS"
STUB
chmod +x "$stub_bin/curl"

# Serves the query status and body, whether query is unreachable, and the
# projector body.
run_healthcheck() {
  HEALTHCHECK_STUB_STATUS="$1" HEALTHCHECK_STUB_BODY="$2" \
    HEALTHCHECK_STUB_UNREACHABLE="${3:-0}" \
    HEALTHCHECK_STUB_PROJECTOR_BODY="${4:-$ready_body}" \
    PATH="$stub_bin:$PATH" sh "$healthcheck"
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
run_healthcheck 503 "$lagging_body" 0 "$parked_body" \
  || test_failure "the healthcheck must pass a query plane behind a parked projector"
if run_healthcheck 503 "$lagging_body" 0 "$syncing_body"; then
  test_failure "the healthcheck must fail a lagging query plane behind a syncing projector"
fi
if run_healthcheck 503 "$lagging_body" 1 "$parked_body"; then
  test_failure "the healthcheck must fail an unreachable query plane behind a parked projector"
fi

printf 'wallet-serving entrypoint tests passed\n'
