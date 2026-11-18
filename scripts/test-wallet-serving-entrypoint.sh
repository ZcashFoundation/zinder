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
  readiness_cause_is_upstream_outage \
  writer_node_outage_behind_syncing_ingest \
  readiness_body_height \
  update_node_unreachable_outage \
  query_unready_behind_projector_park \
  start_wallet_serving \
  require_projector_park_for_stopped_query \
  classify_probe_failure \
  probe_readiness \
  observe_plane_readiness \
  await_readiness \
  update_plane_recurrence \
  zinder_variables_outside \
  start_runtime_process \
  require_positive_seconds \
  require_fallback_indexer_endpoints \
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
serving_unready_tolerance_seconds="$(entrypoint_setting serving_unready_tolerance_seconds)"
node_unreachable_tolerance_seconds="$(entrypoint_setting node_unreachable_tolerance_seconds)"
for shipped_setting in \
  "$ingest_config_paths" "$projector_config_paths" "$query_config_paths" \
  "$entrypoint_config_paths" \
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
probe_upstream_outage=0
probe_node_unreachable=0
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

# Query's startup cannot converge a serving pair behind a parked projector and
# would exit, restarting the container into the same park, so startup leaves
# query stopped when the projector's startup wait ended on the park. The
# subshell keeps the process stand-in and its state out of the rest of this
# test.
(
  query_ops_addr=127.0.0.1:9106
  wallet_serving_deadline_seconds=60
  started_binaries=""
  # The evaluated start_wallet_serving calls this stand-in.
  # shellcheck disable=SC2329
  start_runtime_process() {
    started_binaries="$started_binaries ${1##*/}"
    : &
  }

  query_pid=""
  projector_park_reported=0
  serve 503 "$parked_body"
  probe_readiness 127.0.0.1:9110
  start_wallet_serving || test_failure "startup failed behind a parked projector: $fail_message"
  [ -z "$started_binaries" ] || test_failure "zinder-query started behind a parked projector"
  [ -z "$query_pid" ] || test_failure "startup recorded a query process behind a parked projector"
  [ "$projector_park_reported" -eq 1 ] \
    || test_failure "the serving loop would report the park startup already logged"

  serve 200 "$ready_body"
  probe_readiness 127.0.0.1:9110
  sleep_count=0
  fail_message=""
  start_wallet_serving || test_failure "startup failed behind a ready projector: $fail_message"
  [ "$started_binaries" = " zinder-query" ] \
    || test_failure "startup started '$started_binaries' behind a ready projector"
  [ -n "$query_pid" ] || test_failure "startup did not record the query process"
  wait
)

# Query that startup left stopped counts as serving while the projector
# reports the park or stops answering, and the container restarts once the
# projector answers with any other state.
fail_message=""
require_projector_park_for_stopped_query 1 0 "$parked_body" \
  || test_failure "a parked projector must keep query stopped: $fail_message"
require_projector_park_for_stopped_query 0 1 "unreachable" \
  || test_failure "an unreachable projector must not restart the container: $fail_message"
[ -z "$fail_message" ] || test_failure "a stopped query failed the container: $fail_message"
require_projector_park_for_stopped_query 0 0 "$syncing_body" || true
case "$fail_message" in
  *"no longer reports the wallet transition park"*syncing*) ;;
  *) test_failure "a projector that left the park kept query stopped: '$fail_message'" ;;
esac

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
[ "$probe_upstream_outage" -eq 0 ] || test_failure "a syncing plane is not an upstream outage"

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
  [ "$probe_upstream_outage" -eq 1 ] \
    || test_failure "the probe did not name the upstream outage in: $outage_body"
done
# While Zebra resyncs, or while only the node's mempool feed fails, ingest's
# admission state reports `syncing` and hides its node probe. Query relays the
# writer's own observations under its `ingest_control` dependency, so the
# serving loop still recognizes an unsynchronized node behind any syncing
# ingest. A relayed mempool `node_unavailable` starts the unreachable-node
# clock only behind the mempool gate's `syncing` with no known lag: a writer
# that trails a reachable node also keeps its mempool hydrating, and stays on
# the ordinary unready tolerance. None of this applies to an unrelated ingest
# cause or to query's own view of the node.
ingest_mempool_gated_syncing_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null}},"current_height":4602673,"target_height":4602673}'
ingest_trailing_syncing_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":40}},"current_height":4602633,"target_height":4602673}'
query_relays_writer_upstream_not_ready_body="$degraded_ready_body"
query_relays_writer_mempool_node_unavailable_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"ingest_control","dependency_cause":{"node_unavailable":{"failure_class":"stream_disconnected","last_reason":"the live mempool index is hydrating from the node","consecutive_failures":3,"outage_seconds":30}}}]}'
query_node_not_ready_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"node_source","dependency_cause":{"upstream_not_ready":{"upstream_health":{"source":"zebra_ready_endpoint","reason":"syncing"}}}}]}'
query_node_unavailable_body='{"status":"ready","cause":"ready","current_height":4602673,"degraded":[{"input":"node_source","dependency_cause":{"node_unavailable":{"failure_class":"node_unreachable","last_reason":"connection refused","consecutive_failures":4,"outage_seconds":45}}}]}'
for ingest_syncing_body in "$ingest_mempool_gated_syncing_body" "$ingest_trailing_syncing_body"; do
  relayed_outage="$(writer_node_outage_behind_syncing_ingest syncing "$ingest_syncing_body" \
    "$query_relays_writer_upstream_not_ready_body")"
  [ "$relayed_outage" = "upstream_not_ready" ] \
    || test_failure "a syncing ingest whose writer reports its node unsynchronized must be an upstream outage, got '$relayed_outage' behind '$ingest_syncing_body'"
done
relayed_outage="$(writer_node_outage_behind_syncing_ingest syncing \
  "$ingest_mempool_gated_syncing_body" "$query_relays_writer_mempool_node_unavailable_body")"
[ "$relayed_outage" = "node_unavailable" ] \
  || test_failure "a caught-up ingest whose writer mempool cannot reach its node must be an unreachable-node outage, got '$relayed_outage'"
relayed_outage="$(writer_node_outage_behind_syncing_ingest syncing \
  "$ingest_trailing_syncing_body" "$query_relays_writer_mempool_node_unavailable_body")"
[ -z "$relayed_outage" ] \
  || test_failure "a trailing ingest whose mempool waits for canonical ingest must stay on the unready tolerance, got '$relayed_outage'"
# The mempool gate also reports no lag over the one block the default follow
# threshold admits, so only equal heights prove that canonical ingest caught up.
ingest_one_block_behind_gated_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null}},"current_height":4602672,"target_height":4602673}'
ingest_gated_without_target_body='{"status":"not_ready","phase":"following_tip","cause":{"syncing":{"lag_blocks":null}},"current_height":4602673}'
for unproven_body in "$ingest_one_block_behind_gated_body" "$ingest_gated_without_target_body"; do
  relayed_outage="$(writer_node_outage_behind_syncing_ingest syncing \
    "$unproven_body" "$query_relays_writer_mempool_node_unavailable_body")"
  [ -z "$relayed_outage" ] \
    || test_failure "a gated ingest that has not proven it caught up must stay on the unready tolerance, got '$relayed_outage' behind '$unproven_body'"
done
[ "$(readiness_body_height "$ingest_one_block_behind_gated_body" target_height)" = "4602673" ] \
  || test_failure "the target height was not read from an ingest body"
[ -z "$(readiness_body_height "$ingest_gated_without_target_body" target_height)" ] \
  || test_failure "a body without a target height must not report one"
for unrelated_body in "$ready_body" "$query_node_not_ready_body" "$query_node_unavailable_body" \
  "$degraded_lagging_body" ""; do
  relayed_outage="$(writer_node_outage_behind_syncing_ingest syncing \
    "$ingest_mempool_gated_syncing_body" "$unrelated_body")"
  [ -z "$relayed_outage" ] \
    || test_failure "a syncing ingest is an upstream outage only when its writer reports its node, got '$relayed_outage' from '$unrelated_body'"
done
for unrelated_cause in storage_unavailable replica_lagging starting unreachable; do
  for relaying_body in "$query_relays_writer_upstream_not_ready_body" \
    "$query_relays_writer_mempool_node_unavailable_body"; do
    relayed_outage="$(writer_node_outage_behind_syncing_ingest "$unrelated_cause" \
      "$ingest_mempool_gated_syncing_body" "$relaying_body")"
    [ -z "$relayed_outage" ] \
      || test_failure "a writer node outage must not hide ingest's $unrelated_cause, got '$relayed_outage'"
  done
done

# Only an unreachable node starts the restart clock: a node that answers but is
# not synchronized proves the ingest node client works.
serve 503 "$ingest_node_unavailable_body"
observe_plane_readiness zinder-ingest 127.0.0.1:9105 || true
[ "$probe_node_unreachable" -eq 1 ] \
  || test_failure "the probe did not name an unreachable node"
serve 503 "$ingest_upstream_not_ready_body"
observe_plane_readiness zinder-ingest 127.0.0.1:9105 || true
[ "$probe_node_unreachable" -eq 0 ] \
  || test_failure "an unsynchronized node that answers must not count as unreachable"
serve 200 "$ready_body"
observe_plane_readiness zinder-ingest 127.0.0.1:9105 \
  || test_failure "a ready ingest plane must count as serving"
[ "$probe_upstream_outage" -eq 0 ] || test_failure "a ready plane is not an upstream outage"
[ "$probe_node_unreachable" -eq 0 ] || test_failure "a ready plane does not report an unreachable node"

# An unreachable node escalates only after its own tolerance, which is much
# longer than the contiguous-unready tolerance, so a long node outage does not
# restart the container into a full outage, while an ingest wedged in its own
# node client is still restarted eventually.
[ "$node_unreachable_tolerance_seconds" -gt "$serving_unready_tolerance_seconds" ] \
  || test_failure "the unreachable-node tolerance must outlast the unready tolerance"
outage_state="0 0"
poll_node_unreachable() {
  # shellcheck disable=SC2046,SC2086 # the counters are separate arguments
  set -- $(update_node_unreachable_outage "$1" "$2" ${outage_state%% *} "fixture report")
  outage_state="$1 $2"
}
poll_node_unreachable 1 5000
[ "$outage_state" = "5000 0" ] || test_failure "an unreachable-node outage started as '$outage_state'"
poll_node_unreachable 1 $((5000 + serving_unready_tolerance_seconds))
[ "${outage_state##* }" -eq 0 ] \
  || test_failure "an unreachable-node outage escalated at the ordinary unready tolerance"
poll_node_unreachable 1 $((5000 + node_unreachable_tolerance_seconds - 1))
[ "${outage_state##* }" -eq 0 ] \
  || test_failure "an unreachable-node outage escalated before its tolerance"
poll_node_unreachable 1 $((5000 + node_unreachable_tolerance_seconds))
[ "${outage_state##* }" -eq 1 ] \
  || test_failure "an unreachable-node outage that outlasted its tolerance did not escalate"
outage_state="0 0"
poll_node_unreachable 1 7000
poll_node_unreachable 0 7015
[ "$outage_state" = "0 0" ] || test_failure "a cleared unreachable-node outage left state '$outage_state'"
poll_node_unreachable 1 $((7030 + node_unreachable_tolerance_seconds - 1))
[ "$outage_state" = "$((7030 + node_unreachable_tolerance_seconds - 1)) 0" ] \
  || test_failure "a new unreachable-node outage did not restart its clock: '$outage_state'"

# The `degraded` list follows the top-level cause and names each dependency
# cause under its own key, so the probe parses the plane's own cause.
serve 200 "$degraded_ready_body"
probe_readiness 127.0.0.1:9106
[ "$probe_cause" = "ready" ] \
  || test_failure "probe parsed cause '$probe_cause' from a degraded ready plane"
observe_plane_readiness zinder-query 127.0.0.1:9106 \
  || test_failure "a degraded plane that admits traffic must count as serving"
[ "$probe_upstream_outage" -eq 0 ] \
  || test_failure "a degraded query plane is not an ingest upstream outage"
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
assert_received_by ZINDER_NODE__JSON_RPC_ADDR zinder-ingest zinder-query
assert_received_by ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR zinder-ingest zinder-query
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
# An unreachable query still prints the `-w` trailer, as curl does.
if [ "${HEALTHCHECK_STUB_UNREACHABLE:-0}" -ne 0 ]; then
  printf '\n000'
  exit 7
fi
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
run_healthcheck 200 "$ready_body" 1 "$parked_body" \
  || test_failure "the healthcheck must pass a parked projector that startup left without query"
run_healthcheck 200 "$degraded_ready_body" \
  || test_failure "the healthcheck must pass a query plane serving through a node outage"
if run_healthcheck 503 "$degraded_lagging_body"; then
  test_failure "the healthcheck must fail a lagging query plane whatever its dependencies report"
fi

printf 'wallet-serving entrypoint tests passed\n'
