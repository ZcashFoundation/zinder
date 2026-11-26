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
  readiness_cause_is_store_damage \
  readiness_cause_is_projector_park \
  readiness_cause_is_upstream_outage \
  ingest_node_outage \
  update_node_unreachable_outage \
  reader_unready_behind_projector_park \
  wallet_reader_names \
  start_wallet_serving \
  require_projector_park_for_stopped_readers \
  classify_probe_failure \
  probe_readiness \
  plane_admits_traffic \
  resolve_reader_listen_addrs \
  observe_plane_readiness \
  await_readiness \
  assert_child_alive \
  check_recurrence_budget \
  update_plane_recurrence \
  zinder_variables_outside \
  start_runtime_process \
  require_positive_seconds \
  zinder_setting \
  zinder_setting_is_set \
  ingest_config_disables_explorer_views \
  resolve_deployment_profile \
  zinder_fallback_positions \
  require_fallback_indexer_endpoints \
  require_known_entrypoint_settings \
  resolve_lightwalletd_setting \
  require_distinct_reader_secondaries; do
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
compat_config_paths="$(entrypoint_setting compat_config_paths)"
entrypoint_config_paths="$(entrypoint_setting entrypoint_config_paths)"
ingest_ops_addr="$(entrypoint_setting ingest_ops_addr)"
query_ops_addr="$(entrypoint_setting query_ops_addr)"
compat_ops_addr="$(entrypoint_setting compat_ops_addr)"
recurrence_clear_window_seconds="$(entrypoint_setting recurrence_clear_window_seconds)"
recurrence_episode_budget="$(entrypoint_setting recurrence_episode_budget)"
recurrence_cumulative_unready_seconds_budget="$(
  entrypoint_setting recurrence_cumulative_unready_seconds_budget
)"
recurrence_min_answered_episode_seconds="$(
  entrypoint_setting recurrence_min_answered_episode_seconds
)"
serving_unready_tolerance_seconds="$(entrypoint_setting serving_unready_tolerance_seconds)"
node_unreachable_tolerance_seconds="$(entrypoint_setting node_unreachable_tolerance_seconds)"
for shipped_setting in \
  "$ingest_config_paths" "$projector_config_paths" "$query_config_paths" \
  "$compat_config_paths" "$entrypoint_config_paths" "$compat_ops_addr" \
  "$recurrence_clear_window_seconds" "$recurrence_episode_budget" \
  "$recurrence_cumulative_unready_seconds_budget" \
  "$recurrence_min_answered_episode_seconds" \
  "$serving_unready_tolerance_seconds" "$node_unreachable_tolerance_seconds"; do
  [ -n "$shipped_setting" ] || test_failure "entrypoint no longer sets a setting this test reads"
done

# Settings and state the extracted functions read from the entrypoint.
probe_connect_timeout_seconds=2
probe_max_time_seconds=5
poll_interval_seconds=15
unready_report_interval_seconds=300
probe_halted=0
probe_parked=0
lightwalletd_enabled=1
response_separator='
'

scoping_root="$(mktemp -d)"
trap 'rm -rf "$scoping_root"' EXIT
resolve_reader_listen_addrs
fake_grpc_status=0
grpc_calls="$scoping_root/grpc-calls"

# Called in a subshell, so capture the listener address on disk.
# shellcheck disable=SC2329
zinder-query() {
  [ "$#" -eq 2 ] && [ "$1" = "grpc-healthcheck" ] && [ "${TOKIO_WORKER_THREADS:-}" = 1 ] || return 77
  printf '%s\n' "$2" >>"$grpc_calls"
  printf 'fixture status %s\n' "$fake_grpc_status" >&2
  return "$fake_grpc_status"
}

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
damaged_body='{"ready":false,"phase":"following_tip","cause":{"canonical_store_damaged":{"height":1687104,"pool":"orchard"}},"current_height":4602673}'
schema_body='{"ready":false,"cause":"schema_mismatch"}'
syncing_body='{"ready":false,"cause":{"syncing":{"lag_blocks":12}}}'
ready_body='{"ready":true,"cause":"ready"}'
parked_body='{"ready":false,"cause":{"wallet_transition_limit_exceeded":{"required_bytes":541122672,"configured_bytes":536870912}},"current_height":4602673,"target_height":4602680}'
lagging_body='{"ready":false,"cause":{"replica_lagging":{"lag_chain_epochs":5}},"current_height":4602673}'
ingest_node_unavailable_body='{"status":"not_ready","phase":"following_tip","cause":{"node_unavailable":{"failure_class":"node_unreachable","last_reason":"connection refused","consecutive_failures":4,"outage_seconds":45}},"current_height":4602673}'
ingest_upstream_not_ready_body='{"status":"not_ready","phase":"following_tip","cause":{"upstream_not_ready":{"upstream_committed_height":4602600,"upstream_health":{"source":"zebra_ready_endpoint","reason":"syncing"}}},"current_height":4602673}'
# Query keeps admitting traffic through a node outage and lists the outage
# under `degraded`, after the top-level cause and under its own key.
degraded_ready_body='{"status":"ready","cause":"ready","current_height":4602673,"served_epoch_age_seconds":900,"degraded":[{"input":"node_source","dependency_cause":{"node_unavailable":{"failure_class":"node_unreachable","last_reason":"connection refused","consecutive_failures":4,"outage_seconds":45}}},{"input":"ingest_control","dependency_cause":{"upstream_not_ready":{"upstream_health":{"source":"zebra_ready_endpoint","reason":"syncing"}}}}]}'
degraded_lagging_body='{"status":"not_ready","cause":{"replica_lagging":{"lag_chain_epochs":5}},"current_height":4602673,"degraded":[{"input":"node_source","dependency_cause":"starting"},{"input":"ingest_control","dependency_cause":"ingest_control_unavailable"}]}'

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

# Ingest that found a damaged row in the canonical store, and the readers that
# refuse wallets over it, keep the topology up: every start finds the same
# row, so a restart would only repeat the refusal.
if readiness_cause_is_terminal canonical_store_damaged; then
  test_failure "canonical store damage must not terminate the container"
fi
readiness_cause_is_store_damage canonical_store_damaged \
  || test_failure "canonical_store_damaged must be served as store damage"
for other_cause in storage_unavailable writer_status_unavailable reorg_window_exceeded syncing; do
  if readiness_cause_is_store_damage "$other_cause"; then
    test_failure "$other_cause is not canonical store damage"
  fi
done
serve 503 "$damaged_body"
probe_readiness 127.0.0.1:9105
# shellcheck disable=SC2154 # assigned by the evaluated probe_readiness
[ "$probe_cause" = "canonical_store_damaged" ] \
  || test_failure "probe parsed cause '$probe_cause' from a damaged store's readiness"
for damaged_role in zinder-ingest zinder-query zinder-compat-lightwalletd; do
  serve 503 "$damaged_body"
  observe_plane_readiness "$damaged_role" 127.0.0.1:9105 \
    || test_failure "$damaged_role reporting canonical store damage must count as serving"
  # shellcheck disable=SC2154 # assigned by the evaluated observe_plane_readiness
  [ "$probe_damaged" -eq 1 ] || test_failure "$damaged_role's store damage must be reported"
  [ "$probe_halted" -eq 0 ] || test_failure "store damage is not a writer halt"
done
serve 503 "$damaged_body"
sleep_count=0
fail_message=""
await_readiness zinder-ingest 127.0.0.1:9105 60 \
  || test_failure "startup must continue past a damaged canonical store"
[ -z "$fail_message" ] || test_failure "startup failed on a damaged canonical store: $fail_message"
[ "$sleep_count" -eq 0 ] || test_failure "startup waited on a damaged canonical store"

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

# A reader's startup cannot converge a serving pair behind a parked projector
# and would exit, restarting the container into the same park, so startup
# leaves both readers stopped when the projector's startup wait ended on the
# park. Otherwise query and compatibility start together, unless compatibility
# is turned off. The subshell keeps the process stand-in and its state out of
# the rest of this test.
(
  wallet_serving_deadline_seconds=60
  started_binaries=""
  awaited_phase_starts=""
  # The evaluated start_wallet_serving calls these stand-ins.
  # shellcheck disable=SC2329
  start_runtime_process() {
    started_binaries="$started_binaries ${1##*/}"
    : &
  }
  # shellcheck disable=SC2329
  await_readiness() {
    awaited_phase_starts="$awaited_phase_starts $1@$4"
  }

  query_pid=""
  compat_pid=""
  projector_park_reported=0
  serve 503 "$parked_body"
  probe_readiness 127.0.0.1:9110
  start_wallet_serving || test_failure "startup failed behind a parked projector: $fail_message"
  [ -z "$started_binaries" ] || test_failure "'$started_binaries' started behind a parked projector"
  [ -z "$query_pid$compat_pid" ] || test_failure "startup recorded a reader process behind a parked projector"
  [ "$projector_park_reported" -eq 1 ] \
    || test_failure "the serving loop would report the park startup already logged"

  serve 200 "$ready_body"
  probe_readiness 127.0.0.1:9110
  start_wallet_serving || test_failure "startup failed behind a ready projector: $fail_message"
  [ "$started_binaries" = " zinder-query zinder-compat-lightwalletd" ] \
    || test_failure "startup started '$started_binaries' behind a ready projector"
  [ -n "$query_pid" ] && [ -n "$compat_pid" ] \
    || test_failure "startup did not record both reader processes"
  # Both waits measure the one serving deadline from the same start.
  case "$awaited_phase_starts" in
    " zinder-query@"[0-9]*" zinder-compat-lightwalletd@"[0-9]*)
      query_phase_start="${awaited_phase_starts#* zinder-query@}"
      query_phase_start="${query_phase_start%% *}"
      [ "$awaited_phase_starts" = " zinder-query@$query_phase_start zinder-compat-lightwalletd@$query_phase_start" ] \
        || test_failure "the readers' serving deadlines started apart: '$awaited_phase_starts'"
      ;;
    *) test_failure "startup awaited '$awaited_phase_starts' instead of both readers" ;;
  esac
  wait

  lightwalletd_enabled=0
  started_binaries=""
  awaited_phase_starts=""
  query_pid=""
  compat_pid=""
  start_wallet_serving || test_failure "startup failed without compatibility: $fail_message"
  [ "$started_binaries" = " zinder-query" ] \
    || test_failure "startup started '$started_binaries' with compatibility turned off"
  [ -z "$compat_pid" ] && [[ "$awaited_phase_starts" != *"zinder-compat-lightwalletd"* ]] \
    || test_failure "startup recorded or probed a compatibility process it turned off"
  wait
)

# A healthy ops socket is sufficient for the construction processes. Readers
# must also answer grpc.health.v1 on the configured serving listener.
# shellcheck disable=SC2154 # the evaluated probe functions set these fields
(
  resolve_reader_listen_addrs
  [ "$query_grpc_addr" = '[::]:9102' ] && [ "$compat_grpc_addr" = '[::]:9067' ] \
    || test_failure "the probe defaults differ from the installed reader addresses"
  ZINDER_Query__Listen_Addr='127.0.0.1:9901' ZINDER_COMPAT__LISTEN_ADDR='[::1]:9902' resolve_reader_listen_addrs
  : >"$grpc_calls"
  serve 200 "$degraded_ready_body"
  for role in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd; do
    observe_plane_readiness "$role" 127.0.0.1:9106 \
      || test_failure "$role failed its steady probe: $probe_report"
  done
  [ "$(cat "$grpc_calls")" = $'127.0.0.1:9901\n[::1]:9902' ] && [ "$probe_unreachable" -eq 0 ] \
    || test_failure "the probes did not check both reader listeners on their configured ports"

  for fake_grpc_status in 3 1; do
    if observe_plane_readiness zinder-query 127.0.0.1:9106; then
      test_failure "ops-ready query admitted traffic after gRPC exit $fake_grpc_status"
    fi
    [ "$probe_http_status" = 200 ] && [ "$probe_body" = "$degraded_ready_body" ] && [ "$probe_cause" = ready ] \
      || test_failure "the gRPC probe replaced the ops dependency report"
    [[ "$probe_report" = *"fixture status $fake_grpc_status"* ]] \
      || test_failure "the failed serving probe omitted its diagnostic"
    case "$fake_grpc_status/$probe_unreachable" in
      3/0 | 1/1) ;;
      *) test_failure "gRPC exit $fake_grpc_status gave unreachable=$probe_unreachable" ;;
    esac
    if reader_unready_behind_projector_park 1; then
      test_failure "a park excused a gRPC failure on an ops-ready reader"
    fi
  done

  : >"$grpc_calls"
  serve 503 "$halted_body"
  observe_plane_readiness zinder-query 127.0.0.1:9106 \
    || test_failure "an unprobed canonical halt failed the supervisor"
  [ "$probe_http_status" = 503 ] && [ "$probe_halted" -eq 1 ] &&
    [ "$probe_cause" = reorg_window_exceeded ] && [ ! -s "$grpc_calls" ] \
    || test_failure "the halt lost its HTTP cause or unnecessarily probed the listener"

  serve 200 "$ready_body"
  sleep_count=0
  # The extracted startup wait retries after a failed serving check.
  # shellcheck disable=SC2329
  sleep_interruptibly() {
    sleep_count=$((sleep_count + 1))
    [ "$sleep_count" -eq 1 ] || test_failure "startup did not recover on the second serving check"
    fake_grpc_status=0
  }
  fake_grpc_status=1
  await_readiness zinder-compat-lightwalletd "$compat_ops_addr" 60 \
    || test_failure "startup failed a recovered reader"
  [ "$sleep_count" -eq 1 ] && [ "$(cat "$grpc_calls")" = $'[::1]:9902\n[::1]:9902' ] \
    || test_failure "startup let ops-ready compatibility through before the serving check succeeded"
)

run_serving_topology() {
  eval "$(entrypoint_function log)"
  # These stand-ins are called by the evaluated topology.
  # shellcheck disable=SC2329
  start_runtime_process() { : & }
  # shellcheck disable=SC2329
  fail() { log "$*"; exit 1; }
  lightwalletd_enabled=0
  canonical_construction_deadline_seconds=60
  wallet_construction_deadline_seconds=60
  wallet_serving_deadline_seconds=60
  serve 200 "$degraded_ready_body"
  eval "$(sed -n '/^ingest_ops_addr=/,/^projector_park_reported=0/p' "$entrypoint")"
  eval "$(sed -n '/^log "starting canonical construction/,$p' "$entrypoint")"
}

# Exercise the foreground topology from its first child start into the serving
# loop. Query answers gRPC at startup, then alternates timeouts with
# recoveries while ops remains healthy, until the recurrence policy restarts it.
serving_loop_events() (
  local fake_grpc_status=0 serving_polls=0
  # shellcheck disable=SC2329
  sleep_interruptibly() {
    serving_polls=$((serving_polls + 1))
    [ "$serving_polls" -lt $((2 * recurrence_episode_budget)) ] || exit 0
    fake_grpc_status=$((serving_polls % 2))
  }
  run_serving_topology
)

: >"$grpc_calls"
if events="$(serving_loop_events 2>&1)"; then
  test_failure "the serving loop missed a wedged query listener with healthy ops"
fi
[ "$(wc -l <"$grpc_calls")" -eq $((2 * recurrence_episode_budget)) ] &&
  [ "$(sort -u "$grpc_calls")" = "$query_grpc_addr" ] &&
  [[ "$events" = *"event=supervisor_restart_decided role=zinder-query reason=recurrence_budget episode_count=$recurrence_episode_budget "* ]] &&
  [[ "$(grep -c 'event=supervisor_restart_decided' <<<"$events")" = 1 ]] \
  || test_failure "the serving loop probed a disabled reader or missed the query outage: $events"

# A plane awaited against a serving deadline that started earlier fails once
# that shared deadline passes, without spending a fresh one.
serve 503 "$syncing_body"
deadline_message="$(
  # The evaluated await_readiness calls these stand-ins.
  # shellcheck disable=SC2329
  fail() {
    printf '%s' "$*"
    exit 0
  }
  # shellcheck disable=SC2329
  sleep_interruptibly() {
    printf 'waited'
    exit 0
  }
  await_readiness zinder-compat-lightwalletd "$compat_ops_addr" 60 "$(($(date +%s) - 60))"
)"
case "$deadline_message" in
  "event=supervisor_restart_decided role=zinder-compat-lightwalletd reason=unready_timeout zinder-compat-lightwalletd stayed unready for 60s:"*) ;;
  *) test_failure "a reader past the shared serving deadline did not fail at once: '$deadline_message'" ;;
esac

# Readers that startup left stopped count as serving while the projector
# reports the park or stops answering, and the container restarts once the
# projector answers with any other state.
fail_message=""
require_projector_park_for_stopped_readers 1 0 "$parked_body" \
  || test_failure "a parked projector must keep the readers stopped: $fail_message"
require_projector_park_for_stopped_readers 0 1 "unreachable" \
  || test_failure "an unreachable projector must not restart the container: $fail_message"
[ -z "$fail_message" ] || test_failure "stopped readers failed the container: $fail_message"
require_projector_park_for_stopped_readers 0 0 "$syncing_body" || true
case "$fail_message" in
  *"no longer reports the wallet transition park"*zinder-compat-lightwalletd*syncing*) ;;
  *) test_failure "a projector that left the park kept the readers stopped: '$fail_message'" ;;
esac

# In the serving loop, an answered reader refusal counts as serving while the
# projector is parked, and only then.
serve 503 "$lagging_body"
if observe_plane_readiness zinder-query 127.0.0.1:9106; then
  test_failure "a lagging query plane must count as unready on its own"
fi
reader_unready_behind_projector_park 1 \
  || test_failure "an answered reader refusal behind a parked projector must count as serving"
if reader_unready_behind_projector_park 0; then
  test_failure "a reader refusal without a parked projector must stay unready"
fi
probe_http_status=""
if reader_unready_behind_projector_park 1; then
  test_failure "an unreachable reader plane must stay unready behind a parked projector"
fi

# The park excuses only query: an unready ingest plane observed while the
# projector is parked still counts as unready.
serve 503 "$syncing_body"
if observe_plane_readiness zinder-ingest 127.0.0.1:9105; then
  test_failure "a syncing ingest plane must count as unready while the projector is parked"
fi
[ -z "$(ingest_node_outage "$probe_cause" "$probe_body" "")" ] || test_failure "a syncing plane is not an upstream outage"

# An ingest plane that reports its node unavailable or unsynchronized is
# unready, and the probe names the upstream outage so the serving loop can
# keep it out of the restart budgets.
readiness_cause_is_upstream_outage node_unavailable \
  || test_failure "node_unavailable must be an upstream outage"
readiness_cause_is_upstream_outage upstream_not_ready \
  || test_failure "upstream_not_ready must be an upstream outage"
for other_cause in syncing replica_lagging storage_unavailable reorg_window_exceeded unreachable; do
  if readiness_cause_is_upstream_outage "$other_cause"; then
    test_failure "$other_cause is not an upstream outage"
  fi
done
if readiness_cause_is_terminal node_unavailable || readiness_cause_is_terminal upstream_not_ready; then
  test_failure "an upstream outage must not terminate the container"
fi
for outage_body in "$ingest_node_unavailable_body" "$ingest_upstream_not_ready_body"; do
  serve 503 "$outage_body"
  if observe_plane_readiness zinder-ingest 127.0.0.1:9105; then
    test_failure "an ingest plane in an upstream outage must still count as unready"
  fi
  [ "$(ingest_node_outage "$probe_cause" "$probe_body" "")" = "$probe_cause" ] \
    || test_failure "the probe did not name the upstream outage in: $outage_body"
done
# During preparation of H+1 the heights may still describe the completed
# iteration at H. Only the current wait reason qualifies a writer mempool relay.
ingest_mempool_gated_syncing_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null,"wait_reason":"mempool_hydration_at_tip"}},"current_height":4602673,"target_height":4602673}'
ingest_preparing_syncing_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null,"wait_reason":"canonical_progress"}},"current_height":4602673,"target_height":4602673}'
ingest_trailing_syncing_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null,"wait_reason":"canonical_progress"}},"current_height":4602672,"target_height":4602673}'
ingest_without_wait_reason='{"status":"not_ready","cause":{"syncing":{"lag_blocks":null}},"current_height":4602673,"target_height":4602673}'
query_relays_writer_upstream_not_ready_body="$degraded_ready_body"
query_relays_writer_mempool_node_unavailable_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"ingest_control","dependency_cause":{"node_unavailable":{"failure_class":"stream_disconnected","last_reason":"the live mempool index is hydrating from the node","consecutive_failures":3,"outage_seconds":30}}}]}'
query_node_not_ready_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"node_source","dependency_cause":{"upstream_not_ready":{"upstream_health":{"source":"zebra_ready_endpoint","reason":"syncing"}}}}]}'
query_node_unavailable_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"node_source","dependency_cause":{"node_unavailable":{"failure_class":"node_unreachable","last_reason":"connection refused","consecutive_failures":4,"outage_seconds":45}}}]}'
for ingest_syncing_body in "$ingest_mempool_gated_syncing_body" "$ingest_trailing_syncing_body"; do
  relayed_outage="$(ingest_node_outage syncing "$ingest_syncing_body" \
    "$query_relays_writer_upstream_not_ready_body")"
  [ "$relayed_outage" = "upstream_not_ready" ] \
    || test_failure "a syncing ingest whose writer reports its node unsynchronized must be an upstream outage, got '$relayed_outage' behind '$ingest_syncing_body'"
done
relayed_outage="$(ingest_node_outage syncing \
  "$ingest_mempool_gated_syncing_body" "$query_relays_writer_mempool_node_unavailable_body")"
[ "$relayed_outage" = "node_unavailable" ] \
  || test_failure "a caught-up ingest whose writer mempool cannot reach its node must be an unreachable-node outage, got '$relayed_outage'"
for unproven_body in "$ingest_preparing_syncing_body" "$ingest_trailing_syncing_body" \
  "$ingest_without_wait_reason" \
  '{"cause":{"syncing":{"lag_blocks":null}},"degraded":[{"input":"node_source","dependency_cause":{"syncing":{"wait_reason":"mempool_hydration_at_tip"}}}]}'; do
  relayed_outage="$(ingest_node_outage syncing \
    "$unproven_body" "$query_relays_writer_mempool_node_unavailable_body")"
  [ "$relayed_outage" = "mempool_wait_unconfirmed" ] \
    || test_failure "a syncing writer without an at-tip wait must spend the ordinary budgets, got '$relayed_outage' behind '$unproven_body'"
done
for unrelated_body in "$ready_body" "$query_node_not_ready_body" "$query_node_unavailable_body" \
  "$degraded_lagging_body" ""; do
  relayed_outage="$(ingest_node_outage syncing \
    "$ingest_mempool_gated_syncing_body" "$unrelated_body")"
  [ -z "$relayed_outage" ] \
    || test_failure "a syncing ingest is an upstream outage only when its writer reports its node, got '$relayed_outage' from '$unrelated_body'"
done
for unrelated_cause in ready storage_unavailable replica_lagging starting unreachable; do
  for relaying_body in "$query_relays_writer_upstream_not_ready_body" \
    "$query_relays_writer_mempool_node_unavailable_body"; do
    relayed_outage="$(ingest_node_outage "$unrelated_cause" \
      "$ingest_mempool_gated_syncing_body" "$relaying_body")"
    [ -z "$relayed_outage" ] \
      || test_failure "a writer node outage must not hide ingest's $unrelated_cause, got '$relayed_outage'"
  done
done

# Drive the shipped serving loop with an epoch and writer state per poll.
# Query relays the writer mempool outage through every tip and lag transition.
ingest_outage_events() (
  local -a polls=("$@")
  local poll_index=0 epoch=1000 ingest_status=200 ingest_json="$ready_body" query_json="$ready_body"
  # shellcheck disable=SC2329
  sleep_interruptibly() {
    [ "$poll_index" -lt "${#polls[@]}" ] || exit 0
    epoch="${polls[poll_index]%/*}"
    ingest_status=503
    query_json="$query_relays_writer_mempool_node_unavailable_body"
    case "${polls[poll_index]#*/}" in
      tip) ingest_json="$ingest_mempool_gated_syncing_body" ;;
      lag) ingest_json="$ingest_trailing_syncing_body" ;;
      preparing) ingest_json="$ingest_preparing_syncing_body" ;;
      recovered) ingest_json="$ready_body"; ingest_status=200; query_json="$ready_body" ;;
      node_down) ingest_json="$ingest_node_unavailable_body"; query_json="$ready_body" ;;
      node_syncing) ingest_json="$ingest_trailing_syncing_body"; query_json="$query_relays_writer_upstream_not_ready_body" ;;
      *) test_failure "unknown fixture poll ${polls[poll_index]}" ;;
    esac
    poll_index=$((poll_index + 1))
  }
  # shellcheck disable=SC2329
  date() { printf '%s\n' "$epoch"; }
  # shellcheck disable=SC2329
  curl() {
    local body="$ready_body" status=200
    case "${*: -1}" in
      "http://$ingest_ops_addr/readyz") body="$ingest_json"; status="$ingest_status" ;;
      "http://$query_ops_addr/readyz") body="$query_json" ;;
    esac
    printf '%s%s%s 0.001' "$body" "$response_separator" "$status"
  }
  run_serving_topology
)

expect_ingest_restart() {
  local expected_epoch="$1" expected_reason="$2" events
  shift 2
  if events="$(ingest_outage_events "$@" 2>&1)"; then
    test_failure "ingest stayed up past the $expected_reason deadline: $events"
  fi
  [[ "$events" = *"$expected_epoch zinder-wallet-serving-runtime: event=supervisor_restart_decided role=zinder-ingest reason=$expected_reason "* ]] \
    || test_failure "expected an ingest $expected_reason restart at $expected_epoch: $events"
  printf '%s\n' "$events"
}

outage_start=5000
outage_deadline=$((outage_start + node_unreachable_tolerance_seconds))
[ "$node_unreachable_tolerance_seconds" -gt "$serving_unready_tolerance_seconds" ] \
  || test_failure "the unreachable-node tolerance must outlast the unready tolerance"
for node_outage in tip node_down; do
  expect_ingest_restart "$outage_deadline" node_timeout "$outage_start/$node_outage" \
    "$((outage_start + serving_unready_tolerance_seconds))/$node_outage" \
    "$((outage_deadline - 1))/$node_outage" "$outage_deadline/$node_outage" >/dev/null
done

events="$(ingest_outage_events "$outage_start/node_syncing" "$outage_deadline/node_syncing" 2>&1)" \
  || test_failure "a reachable, unsynchronized writer node exhausted a restart budget: $events"

# H/H is stale throughout this H+1 preparation. The stalled writer must
# count a recurrence episode and exhaust the ordinary contiguous tolerance.
lag_start=$((outage_start + 15))
lag_deadline=$((lag_start + serving_unready_tolerance_seconds))
events="$(expect_ingest_restart "$lag_deadline" unready_timeout "$outage_start/tip" \
  "$lag_start/preparing" "$((lag_deadline - 1))/preparing" "$lag_deadline/preparing")"
[[ "$events" = *"zinder-ingest flap episode 1 began"* ]] \
  || test_failure "a stalled canonical writer skipped the episode budget: $events"

# A completed lag interval can separately exhaust the cumulative budget.
lag_end=$((lag_start + recurrence_cumulative_unready_seconds_budget))
events="$(expect_ingest_restart "$lag_end" recurrence_budget "$outage_start/tip" \
  "$lag_start/preparing" "$lag_end/tip")"
[[ "$events" = *"episode_count=1 unready_seconds=$recurrence_cumulative_unready_seconds_budget "* ]] \
  || test_failure "the resumed mempool wait did not account for the canonical lag: $events"

# Two short lags exclude 75 seconds from the clock. The original outage
# still reaches its deadline and is logged only once.
resumed_deadline=$((outage_deadline + 75))
events="$(expect_ingest_restart "$resumed_deadline" node_timeout "$outage_start/tip" \
  "5030/lag" "5045/lag" "5075/tip" "5090/lag" "5120/tip" \
  "$outage_deadline/tip" "$((resumed_deadline - 1))/tip" "$resumed_deadline/tip")"
[[ "$(grep -c 'zinder-ingest cannot reach its node' <<<"$events")" = 1 ]] &&
  [[ "$events" = *"spent ${node_unreachable_tolerance_seconds}s waiting"* ]] \
  || test_failure "the clock lost its original outage or charged paused time: $events"

# Recovery while paused clears both the running time and the outage start.
second_start=$((outage_deadline - 10))
second_deadline=$((second_start + node_unreachable_tolerance_seconds))
events="$(expect_ingest_restart "$second_deadline" node_timeout "$outage_start/tip" \
  "$((second_start - 30))/lag" "$((second_start - 15))/recovered" "$second_start/tip" \
  "$((second_deadline - 1))/tip" "$second_deadline/tip")"
[[ "$(grep -c 'zinder-ingest cannot reach its node' <<<"$events")" = 2 ]] \
  || test_failure "a recovered node kept its old outage: $events"

# The `degraded` list follows the top-level cause and names each dependency
# cause under its own key, so the probe parses the plane's own cause.
serve 200 "$degraded_ready_body"
probe_readiness 127.0.0.1:9106
[ "$probe_cause" = "ready" ] \
  || test_failure "probe parsed cause '$probe_cause' from a degraded ready plane"
observe_plane_readiness zinder-query 127.0.0.1:9106 \
  || test_failure "a degraded plane that admits traffic must count as serving"
serve 503 "$degraded_lagging_body"
probe_readiness 127.0.0.1:9106
[ "$probe_cause" = "replica_lagging" ] \
  || test_failure "probe parsed cause '$probe_cause' from a lagging plane with degraded dependencies"

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

# Restore the entrypoint's foreground decision and teardown in a subshell
# whose only runtime PID, if present, belongs to the fixture.
load_supervisor_fixture() {
  for name in log fail stop_processes alive_process_labels any_process_alive \
    await_processes_exit assert_started_processes_alive; do
    eval "$(entrypoint_function "$name")"
  done
  ingest_pid="" projector_pid="" query_pid="" compat_pid=""
  shutdown_grace_seconds=1
}

recurrence_events() (
  load_supervisor_fixture
  recurrence_state="$1 0 0 $2 0"
  shift 2
  for ready_poll in "$@"; do
    # shellcheck disable=SC2046,SC2086 # the poll and recurrence counters are separate arguments
    set -- $(update_plane_recurrence zinder-query "${ready_poll%/*}" 0 "${ready_poll#*/}" $recurrence_state "fixture report")
    recurrence_state="$1 $2 $3 $4 $5"
    check_recurrence_budget zinder-query "$7" "$1" "$4" "$6" "fixture report"
  done
)

events="$(recurrence_events 0 0 0/1000 0/1005 1/1015 "1/$((1015 + recurrence_clear_window_seconds))" 2>&1)" \
  || test_failure "a short recovered drain restarted query: $events"
event_fields="event=supervisor_readiness_episode role=zinder-query"
[[ "$events" = *"$event_fields transition=begin episode_count=0 unready_seconds=0 counted=0"* ]] &&
  [[ "$events" = *"$event_fields transition=end episode_count=0 unready_seconds=15 counted=0"* ]] &&
  [[ "$events" = *"$event_fields transition=clear episode_count=0 unready_seconds=0"* ]] &&
  [[ "$(grep -c 'event=supervisor_readiness_episode' <<<"$events")" = 3 ]] &&
  [[ "$events" != *"event=supervisor_restart_decided"* ]] \
  || test_failure "incorrect events for a recovered short drain: $events"

previous_unready_seconds=$((recurrence_cumulative_unready_seconds_budget - 10))
events="$(recurrence_events 0 "$previous_unready_seconds" 0/1000 0/1015 2>&1)" \
  || test_failure "an active 15-second drain spent the completed-time budget: $events"
[[ "$events" != *"event=supervisor_restart_decided"* ]] \
  || test_failure "a decision was logged before the drain ended: $events"
if events="$(recurrence_events 0 "$previous_unready_seconds" 0/1000 0/1015 1/1015 2>&1)"; then
  test_failure "the completed drain over budget left the supervisor running"
fi
[[ "$events" = *"$event_fields transition=end episode_count=0 unready_seconds=$((previous_unready_seconds + 15)) counted=0"* ]] &&
  [[ "$events" = *"event=supervisor_restart_decided role=zinder-query reason=recurrence_budget episode_count=0 unready_seconds=$((previous_unready_seconds + 15)) "* ]] &&
  [[ "$(grep -c 'event=supervisor_restart_decided' <<<"$events")" = 1 ]] \
  || test_failure "incorrect decision for completed drains over budget: $events"

# The last answered episode can trigger the episode budget before recovery.
# Its elapsed time belongs in the human report, while the seconds against the
# cumulative budget still come only from the preceding completed intervals.
previous_episodes=$((recurrence_episode_budget - 1))
completed_seconds=$((previous_episodes * recurrence_min_answered_episode_seconds))
decision_epoch=$((1000 + recurrence_min_answered_episode_seconds))
if events="$(recurrence_events "$previous_episodes" "$completed_seconds" 0/1000 "0/$decision_epoch" 2>&1)"; then
  test_failure "the final active episode over budget left the supervisor running"
fi
[[ "$events" = *"$event_fields transition=begin episode_count=$previous_episodes unready_seconds=$completed_seconds counted=0"* ]] &&
  [[ "$events" != *"transition=end"* ]] &&
  [[ "$events" = *"reason=recurrence_budget episode_count=$recurrence_episode_budget unready_seconds=$completed_seconds "* ]] &&
  [[ "$events" = *"$recurrence_episode_budget episode(s), $((completed_seconds + recurrence_min_answered_episode_seconds))s unready"* ]] &&
  [[ "$(grep -c 'event=supervisor_restart_decided' <<<"$events")" = 1 ]] \
  || test_failure "incorrect decision for the final active episode: $events"

reader_exit_events() (
  load_supervisor_fixture
  (exit 42) &
  query_pid=$!
  wait "$query_pid" || true
  assert_started_processes_alive "$1"
)
for phase in startup serving; do
  if events="$(reader_exit_events "$phase" 2>&1)"; then
    test_failure "query exit during $phase left the supervisor running"
  fi
  [[ "$events" = *"event=supervisor_child_exited role=zinder-query phase=$phase"* ]] &&
    [[ "$events" = *"event=supervisor_restart_decided role=zinder-query reason=child_exit"* ]] &&
    [[ "$(grep -c 'event=supervisor_child_exited' <<<"$events")" = 1 ]] &&
    [[ "$(grep -c 'event=supervisor_restart_decided' <<<"$events")" = 1 ]] \
    || test_failure "incorrect supervisor events for query exit during $phase: $events"
done

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
  ZINDER_WALLET_SERVING__LIGHTWALLETD=false \
  require_known_entrypoint_settings || true
[ -z "$fail_message" ] \
  || test_failure "documented entrypoint settings were rejected: $fail_message"

# Compatibility runs unless the setting is false; only true and false, in any
# letter case, are accepted.
for lightwalletd_value in true TRUE True; do
  lightwalletd_enabled=0
  fail_message=""
  resolve_lightwalletd_setting "$lightwalletd_value" || true
  [ -z "$fail_message" ] && [ "$lightwalletd_enabled" -eq 1 ] \
    || test_failure "ZINDER_WALLET_SERVING__LIGHTWALLETD=$lightwalletd_value did not turn compatibility on"
done
for lightwalletd_value in false FALSE False; do
  lightwalletd_enabled=1
  fail_message=""
  resolve_lightwalletd_setting "$lightwalletd_value" || true
  [ -z "$fail_message" ] && [ "$lightwalletd_enabled" -eq 0 ] \
    || test_failure "ZINDER_WALLET_SERVING__LIGHTWALLETD=$lightwalletd_value did not turn compatibility off"
done
for lightwalletd_value in "" 0 1 no off yes; do
  fail_message=""
  resolve_lightwalletd_setting "$lightwalletd_value" || true
  case "$fail_message" in
    "ZINDER_WALLET_SERVING__LIGHTWALLETD must be true or false"*) ;;
    *) test_failure "ZINDER_WALLET_SERVING__LIGHTWALLETD='$lightwalletd_value' was accepted" ;;
  esac
done
lightwalletd_enabled=1

# A secondary path variable reaches both readers, which must not share a
# secondary root, so it is refused while compatibility runs.
fail_message=""
ZINDER_STORAGE__SECONDARY_PATH=/var/lib/zinder/shared-secondary \
  require_distinct_reader_secondaries || true
case "$fail_message" in
  "ZINDER_STORAGE__SECONDARY_PATH would give zinder-query and zinder-compat-lightwalletd the same secondary root"*) ;;
  *) test_failure "a canonical secondary path was accepted with both readers running: '$fail_message'" ;;
esac
fail_message=""
ZINDER_Wallet__Secondary_Path=/var/lib/zinder/shared-secondary \
  require_distinct_reader_secondaries || true
case "$fail_message" in
  "ZINDER_WALLET__SECONDARY_PATH would give zinder-query and zinder-compat-lightwalletd the same secondary root"*) ;;
  *) test_failure "a mixed-case wallet secondary path was accepted with both readers running: '$fail_message'" ;;
esac
fail_message=""
lightwalletd_enabled=0
ZINDER_STORAGE__SECONDARY_PATH=/var/lib/zinder/query/canonical-secondary \
  require_distinct_reader_secondaries \
  || test_failure "a secondary path was refused with compatibility turned off: $fail_message"
lightwalletd_enabled=1
require_distinct_reader_secondaries \
  || test_failure "the readers' own secondary roots were refused: $fail_message"
[ -z "$fail_message" ] || test_failure "the secondary guard failed without a path variable: $fail_message"

# Every fallback node endpoint needs its indexer stream, as [node] does; a
# position is configured by any of its variables.
fail_message=""
ZINDER_NODE_FALLBACKS__2__JSON_RPC_ADDR=http://127.0.0.1:28232 \
  ZINDER_NODE_FALLBACKS__2__AUTH__METHOD=cookie \
  require_fallback_indexer_endpoints || true
case "$fail_message" in
  ZINDER_NODE_FALLBACKS__2__INDEXER_GRPC_ADDR\ is\ required:*) ;;
  *) test_failure "a fallback endpoint without an indexer URL was accepted: '$fail_message'" ;;
esac
fail_message=""
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://127.0.0.1:28232 \
  ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR=http://127.0.0.1:28155 \
  require_fallback_indexer_endpoints \
  || test_failure "a fallback endpoint with an indexer URL was rejected: $fail_message"

# The binaries read variable names in any letter case after `ZINDER_`, so the
# requirement matches them the same way: a mixed-case fallback still needs its
# indexer URL, and a mixed-case indexer URL satisfies it.
fail_message=""
ZINDER_Node_Fallbacks__3__Json_Rpc_Addr=http://127.0.0.1:28232 \
  require_fallback_indexer_endpoints || true
case "$fail_message" in
  ZINDER_NODE_FALLBACKS__3__INDEXER_GRPC_ADDR\ is\ required:*) ;;
  *) test_failure "a mixed-case fallback endpoint without an indexer URL was accepted: '$fail_message'" ;;
esac
fail_message=""
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://127.0.0.1:28232 \
  ZINDER_node_fallbacks__1__indexer_grpc_addr=http://127.0.0.1:28155 \
  require_fallback_indexer_endpoints \
  || test_failure "a mixed-case fallback indexer URL was rejected: $fail_message"
  [ "$(ZINDER_Node__Indexer_Grpc_Addr=http://127.0.0.1:18155 zinder_setting node.indexer_grpc_addr)" \
  = http://127.0.0.1:18155 ] \
  || test_failure "a mixed-case [node] indexer URL was not found"

# Deployment profile resolution defaults to wallet when ingest.toml has
# explorer_views = false and ZINDER_INGEST__EXPLORER_VIEWS is not true, allows
# wallet-archive, rejects light-wallet, and rejects combining a profile with
# Explorer views.
test_deployment_profile_resolution() {
  local wallet_ingest_toml="$scoping_root/ingest-wallet.toml"
  local explorer_ingest_toml="$scoping_root/ingest-explorer.toml"
  cp "$repository_root/deploy/config/wallet-serving/ingest.toml" "$wallet_ingest_toml"
  sed 's/^explorer_views = false$/explorer_views = true/' "$wallet_ingest_toml" >"$explorer_ingest_toml"

  local saved_ingest_config_path="${ingest_config_path:-}"
  ingest_config_path="$wallet_ingest_toml"

  # Default is wallet when explorer_views = false in ingest.toml.
  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
  fail_message=""
  resolve_deployment_profile \
    || test_failure "default profile failed: $fail_message"
  [ "${ZINDER_DEPLOYMENT__PROFILE:-}" = "wallet" ] \
    || test_failure "expected default ZINDER_DEPLOYMENT__PROFILE=wallet, got '${ZINDER_DEPLOYMENT__PROFILE:-}'"

  # wallet and wallet-archive overrides succeed when Explorer views are off.
  for valid_profile in wallet wallet-archive; do
    unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
    export ZINDER_DEPLOYMENT__PROFILE="$valid_profile"
    fail_message=""
    resolve_deployment_profile \
      || test_failure "profile $valid_profile failed: $fail_message"
    [ "${ZINDER_DEPLOYMENT__PROFILE:-}" = "$valid_profile" ] \
      || test_failure "expected ZINDER_DEPLOYMENT__PROFILE=$valid_profile, got '${ZINDER_DEPLOYMENT__PROFILE:-}'"
  done

  # light-wallet and invalid/empty profiles fail closed.
  for rejected_profile in light-wallet "" unknown; do
    unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
    fail_message=""
    ZINDER_DEPLOYMENT__PROFILE="$rejected_profile" resolve_deployment_profile || true
    [ -n "$fail_message" ] || test_failure "profile '$rejected_profile' was accepted"
    if [ "$rejected_profile" = "light-wallet" ]; then
      case "$fail_message" in
        *zinder-projector*zinder-query*) ;;
        *) test_failure "light-wallet rejection lacked operator guidance: '$fail_message'" ;;
      esac
    fi
  done

  # ZINDER_INGEST__EXPLORER_VIEWS=true leaves profile unset by default, and
  # rejects explicit ZINDER_DEPLOYMENT__PROFILE.
  for truthy_views in true TRUE 1 on yes; do
    unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
    export ZINDER_INGEST__EXPLORER_VIEWS="$truthy_views"
    fail_message=""
    resolve_deployment_profile \
      || test_failure "explorer_views=$truthy_views without profile failed: $fail_message"
    [ -z "${ZINDER_DEPLOYMENT__PROFILE:-}" ] \
      || test_failure "explorer_views=$truthy_views unexpectedly defaulted ZINDER_DEPLOYMENT__PROFILE to '$ZINDER_DEPLOYMENT__PROFILE'"

    fail_message=""
    ZINDER_DEPLOYMENT__PROFILE=wallet resolve_deployment_profile || true
    case "$fail_message" in
      *"cannot be combined with Explorer views"*) ;;
      *) test_failure "explorer_views=$truthy_views with wallet profile was accepted: '$fail_message'" ;;
    esac
  done

  # ZINDER_INGEST__EXPLORER_VIEWS=false still defaults to wallet on the
  # wallet-serving ingest.toml.
  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
  export ZINDER_INGEST__EXPLORER_VIEWS=false
  fail_message=""
  resolve_deployment_profile \
    || test_failure "explorer_views=false failed: $fail_message"
  [ "${ZINDER_DEPLOYMENT__PROFILE:-}" = "wallet" ] \
    || test_failure "explorer_views=false did not default to wallet"

  # Explorer image (ingest.toml has explorer_views = true) leaves profile unset
  # and rejects explicit profiles unless Explorer views are disabled.
  ingest_config_path="$explorer_ingest_toml"
  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
  fail_message=""
  resolve_deployment_profile \
    || test_failure "explorer ingest.toml without profile failed: $fail_message"
  [ -z "${ZINDER_DEPLOYMENT__PROFILE:-}" ] \
    || test_failure "explorer ingest.toml unexpectedly defaulted profile to '$ZINDER_DEPLOYMENT__PROFILE'"

  for rejected_profile in wallet wallet-archive light-wallet; do
    fail_message=""
    ZINDER_DEPLOYMENT__PROFILE="$rejected_profile" resolve_deployment_profile || true
    [ -n "$fail_message" ] \
      || test_failure "explorer ingest.toml accepted ZINDER_DEPLOYMENT__PROFILE=$rejected_profile"
  done

  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
  export ZINDER_INGEST__EXPLORER_VIEWS=false
  export ZINDER_DEPLOYMENT__PROFILE=wallet-archive
  fail_message=""
  resolve_deployment_profile \
    || test_failure "explorer ingest.toml with views=false and wallet-archive failed: $fail_message"

  unset ZINDER_DEPLOYMENT__PROFILE
  fail_message=""
  ZINDER_DEPLOYMENT__PROFILE=light-wallet resolve_deployment_profile || true
  case "$fail_message" in
    *zinder-projector*zinder-query*) ;;
    *) test_failure "explorer ingest.toml with views=false accepted light-wallet: '$fail_message'" ;;
  esac

  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS ZINDER_STORAGE__RAW_BLOB_POLICY
  # Explicit transactions/all overrides leave ZINDER_DEPLOYMENT__PROFILE unset
  # so existing pre-profile volumes reopen in place.
  ingest_config_path="$wallet_ingest_toml"
  for legacy_policy in transactions all; do
    unset ZINDER_DEPLOYMENT__PROFILE
    fail_message=""
    ZINDER_STORAGE__RAW_BLOB_POLICY="$legacy_policy" resolve_deployment_profile \
      || test_failure "ZINDER_STORAGE__RAW_BLOB_POLICY=$legacy_policy without profile failed: $fail_message"
    [ -z "${ZINDER_DEPLOYMENT__PROFILE:-}" ] \
      || test_failure "ZINDER_STORAGE__RAW_BLOB_POLICY=$legacy_policy unexpectedly injected ZINDER_DEPLOYMENT__PROFILE=$ZINDER_DEPLOYMENT__PROFILE"
  done

  for bad_policy in none "" unknown; do
    unset ZINDER_DEPLOYMENT__PROFILE
    fail_message=""
    ZINDER_STORAGE__RAW_BLOB_POLICY="$bad_policy" resolve_deployment_profile || true
    [ -n "$fail_message" ] \
      || test_failure "ZINDER_STORAGE__RAW_BLOB_POLICY='$bad_policy' without profile unexpectedly succeeded"
  done

  ingest_config_path="$saved_ingest_config_path"
}
test_deployment_profile_resolution

# Each process starts with the variables its binary accepts. A stub setpriv on
# PATH records the environment variable names each process receives.
mkdir "$scoping_root/bin" "$scoping_root/received"
cat >"$scoping_root/bin/setpriv" <<'STUB'
#!/bin/sh
shift 4
env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' \
  | sort >"$SCOPING_RECEIVED_DIR/$(basename "$1")"
STUB
chmod +x "$scoping_root/bin/setpriv"

# The subshell keeps the fixture environment out of the rest of this test.
# shellcheck disable=SC2030,SC2031
(
  export SCOPING_RECEIVED_DIR="$scoping_root/received"
  export ZINDER_DEPLOYMENT__PROFILE=wallet-archive
  export ZINDER_NETWORK__NAME=zcash-testnet
  export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232
  export ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://127.0.0.1:28232
  export ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS=300000
  export ZINDER_WALLET__SECONDARY_PATH=/var/lib/zinder/query/wallet-secondary
  export ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS=500
  export ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS=48
  export ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER=128849018880
  export ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES=64
  export ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS=8
  export ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS=40
  export ZINDER_COMPAT__PING_VERY_INSECURE=false
  export ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS=86400
  export ZINDER_WALLET_SERVING__LIGHTWALLETD=true
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
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-compat-lightwalletd /etc/zinder/compat-lightwalletd.toml \
    "$compat_config_paths"
  wait
)

# Asserts which processes received a variable: "<variable> <process>...".
assert_received_by() {
  variable_name="$1"
  shift
  for process_name in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd; do
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

assert_received_by ZINDER_DEPLOYMENT__PROFILE \
  zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_NETWORK__NAME \
  zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_NODE__JSON_RPC_ADDR zinder-ingest zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR \
  zinder-ingest zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES \
  zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES zinder-ingest
assert_received_by ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS \
  zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_WALLET__SECONDARY_PATH zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS zinder-ingest
assert_received_by ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS zinder-ingest
assert_received_by ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER zinder-projector
assert_received_by ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES \
  zinder-projector zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS zinder-query
assert_received_by ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS zinder-compat-lightwalletd
assert_received_by ZINDER_COMPAT__PING_VERY_INSECURE zinder-compat-lightwalletd
assert_received_by ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS
assert_received_by ZINDER_WALLET_SERVING__LIGHTWALLETD
assert_received_by ZINDER_EXPLORER__LISTEN_ADDR
assert_received_by INGEST_CONTROL_BEARER_TOKEN
assert_received_by CHECKPOINT_BEARER_TOKEN

# Legacy pre-profile volumes override storage.raw_blob_policy directly and must
# leave ZINDER_DEPLOYMENT__PROFILE unset while routing raw_blob_policy to all
# four runtimes.
rm -f "$scoping_root/received"/*
# shellcheck disable=SC2030,SC2031
(
  export SCOPING_RECEIVED_DIR="$scoping_root/received"
  ingest_config_path="$repository_root/deploy/config/wallet-serving/ingest.toml"
  unset ZINDER_DEPLOYMENT__PROFILE ZINDER_INGEST__EXPLORER_VIEWS
  export ZINDER_STORAGE__RAW_BLOB_POLICY=transactions
  resolve_deployment_profile
  [ -z "${ZINDER_DEPLOYMENT__PROFILE:-}" ] \
    || test_failure "legacy transactions override unexpectedly injected ZINDER_DEPLOYMENT__PROFILE"
  export ZINDER_NETWORK__NAME=zcash-testnet
  export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232
  export ZINDER_WALLET__SECONDARY_PATH=/var/lib/zinder/query/wallet-secondary
  export INGEST_CONTROL_BEARER_TOKEN=fixture-token
  export CHECKPOINT_BEARER_TOKEN=fixture-token
  stub_path="$scoping_root/bin:$PATH"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-ingest /etc/zinder/ingest.toml "$ingest_config_paths"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-projector /etc/zinder/projector.toml "$projector_config_paths"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-query /etc/zinder/query.toml "$query_config_paths"
  PATH="$stub_path" start_runtime_process \
    /usr/local/bin/zinder-compat-lightwalletd /etc/zinder/compat-lightwalletd.toml \
    "$compat_config_paths"
  wait
)
assert_received_by ZINDER_STORAGE__RAW_BLOB_POLICY \
  zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd
assert_received_by ZINDER_DEPLOYMENT__PROFILE

unread_names="$(
  ZINDER_EXPLORER__LISTEN_ADDR=127.0.0.1:9068 \
    ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS=43200 \
    ZINDER_WALLET_SERVING__LIGHTWALLETD=true \
    ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS=8 \
    ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS=40 \
    ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES=64 \
    ZINDER_STORAGE__CANONICAL_PAHT=/misspelled \
    zinder_variables_outside \
    "$ingest_config_paths $projector_config_paths $query_config_paths $compat_config_paths $entrypoint_config_paths" \
    | sort | tr '\n' ' '
)"
[ "$unread_names" = "ZINDER_EXPLORER__LISTEN_ADDR ZINDER_STORAGE__CANONICAL_PAHT " ] \
  || test_failure "variables no process reads were reported as '$unread_names'"

# With compatibility turned off, its own settings reach no process, so the
# entrypoint reports them as unread.
unread_names="$(
  ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS=40 \
    ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS=8 \
    zinder_variables_outside \
    "$ingest_config_paths $projector_config_paths $query_config_paths $entrypoint_config_paths" \
    | sort | tr '\n' ' '
)"
[ "$unread_names" = "ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS " ] \
  || test_failure "compatibility settings without compatibility were reported as '$unread_names'"

# The container healthcheck runs as its own process, so it is exercised with a
# stub curl on PATH that serves the fixture status and body.
healthcheck="$repository_root/deploy/wallet-serving-healthcheck"
stub_bin="$(mktemp -d)"
trap 'rm -rf "$scoping_root" "$stub_bin"' EXIT
cat >"$stub_bin/curl" <<'STUB'
#!/bin/sh
for argument in "$@"; do
  url="$argument"
done
case "$url" in
  *:9110/readyz)
    printf '%s' "$HEALTHCHECK_STUB_PROJECTOR_BODY"
    exit 0
    ;;
  *:9107/readyz)
    # An unreachable reader still prints the `-w` trailer, as curl does.
    if [ "${HEALTHCHECK_STUB_COMPAT_UNREACHABLE:-0}" -ne 0 ]; then
      printf '\n000'
      exit 7
    fi
    printf '%s\n%s' "$HEALTHCHECK_STUB_COMPAT_BODY" "$HEALTHCHECK_STUB_COMPAT_STATUS"
    exit 0
    ;;
  *) ;;
esac
if [ "${HEALTHCHECK_STUB_UNREACHABLE:-0}" -ne 0 ]; then
  printf '\n000'
  exit 7
fi
printf '%s\n%s' "$HEALTHCHECK_STUB_BODY" "$HEALTHCHECK_STUB_STATUS"
STUB
chmod +x "$stub_bin/curl"

# Serves the query status and body, whether query is unreachable, and the
# projector body. Compatibility answers ready unless run_compat_healthcheck
# sets it.
run_healthcheck() {
  HEALTHCHECK_STUB_STATUS="$1" HEALTHCHECK_STUB_BODY="$2" \
    HEALTHCHECK_STUB_UNREACHABLE="${3:-0}" \
    HEALTHCHECK_STUB_PROJECTOR_BODY="${4:-$ready_body}" \
    HEALTHCHECK_STUB_COMPAT_STATUS="${HEALTHCHECK_STUB_COMPAT_STATUS:-200}" \
    HEALTHCHECK_STUB_COMPAT_BODY="${HEALTHCHECK_STUB_COMPAT_BODY:-$ready_body}" \
    PATH="$stub_bin:$PATH" sh "$healthcheck"
}

# Serves the compatibility status and body and whether it is unreachable
# behind a ready query plane, then the projector body.
run_compat_healthcheck() {
  HEALTHCHECK_STUB_COMPAT_STATUS="$1" HEALTHCHECK_STUB_COMPAT_BODY="$2" \
    HEALTHCHECK_STUB_COMPAT_UNREACHABLE="${3:-0}" \
    run_healthcheck 200 "$ready_body" 0 "${4:-$ready_body}"
}

run_healthcheck 200 "$ready_body" \
  || test_failure "the healthcheck must pass a ready query plane"
run_healthcheck 503 "$halted_body" \
  || test_failure "the healthcheck must pass a query plane serving the writer halt"
run_healthcheck 503 "$damaged_body" \
  || test_failure "the healthcheck must pass a query plane refusing a damaged canonical store"
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
run_healthcheck 200 "$ready_body" 1 "$parked_body" \
  || test_failure "the healthcheck must pass a parked projector that startup left without query"
run_healthcheck 200 "$degraded_ready_body" \
  || test_failure "the healthcheck must pass a query plane serving through a node outage"
if run_healthcheck 503 "$degraded_lagging_body"; then
  test_failure "the healthcheck must fail a lagging query plane whatever its dependencies report"
fi

# Query and compatibility serve the same pair, so the container is healthy
# only while both serve, unless compatibility is turned off.
run_compat_healthcheck 200 "$ready_body" \
  || test_failure "the healthcheck must pass two ready readers"
run_compat_healthcheck 503 "$halted_body" \
  || test_failure "the healthcheck must pass a compatibility plane serving the writer halt"
run_compat_healthcheck 503 "$damaged_body" \
  || test_failure "the healthcheck must pass a compatibility plane refusing a damaged canonical store"
if run_compat_healthcheck 503 "$syncing_body"; then
  test_failure "the healthcheck must fail a syncing compatibility plane behind a ready query plane"
fi
if run_compat_healthcheck 200 "$ready_body" 1; then
  test_failure "the healthcheck must fail an unreachable compatibility plane"
fi
run_compat_healthcheck 503 "$lagging_body" 0 "$parked_body" \
  || test_failure "the healthcheck must pass a compatibility plane behind a parked projector"
run_compat_healthcheck 200 "$ready_body" 1 "$parked_body" \
  || test_failure "the healthcheck must pass a parked projector that startup left without compatibility"
for lightwalletd_value in false FALSE; do
  ZINDER_WALLET_SERVING__LIGHTWALLETD="$lightwalletd_value" \
    run_compat_healthcheck 200 "$ready_body" 1 \
    || test_failure "the healthcheck must ignore compatibility turned off by '$lightwalletd_value'"
done
if ZINDER_WALLET_SERVING__LIGHTWALLETD=true run_compat_healthcheck 503 "$syncing_body"; then
  test_failure "the healthcheck must check compatibility when it is turned on"
fi

printf 'wallet-serving entrypoint tests passed\n'
