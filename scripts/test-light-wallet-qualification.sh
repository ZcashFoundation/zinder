#!/usr/bin/env bash
# Compare light-wallet and wallet serving with the release binaries on
# the same disposable Regtest chain, premined as in the parity workflow.
# The node container is stopped for the outage checks and restarted on exit.
set -euo pipefail

if [[ $# != 3 ]]; then
  echo "usage: $0 <binary-directory> <disposable-node-container> <wallet-serving-grpc-url>" >&2
  exit 2
fi
bin_dir=$1
container=$2
wallet_url=$3
rpc_url=${ZINDER_NODE__JSON_RPC_ADDR:?set the Regtest JSON-RPC URL}
if [[ ! "$rpc_url" =~ ^http://127\.0\.0\.1:[0-9]+/?$ || ! "$wallet_url" =~ ^http://127\.0\.0\.1:[0-9]+/?$ ]]; then
  echo "the parity fixture requires loopback node and wallet endpoints" >&2
  exit 2
fi
if [[ ${ZINDER_NETWORK:-zcash-regtest} != zcash-regtest ]]; then
  echo "the light-wallet harness requires zcash-regtest" >&2
  exit 2
fi
# Binaries read network.name from --network. The bare key belongs to testkit.
unset ZINDER_NETWORK
for binary in zinder-ingest zinder-compat-lightwalletd; do
  [[ -x "$bin_dir/$binary" ]] || { echo "missing binary: $bin_dir/$binary" >&2; exit 2; }
done

# NEXTEST_ARCHIVE_FILE runs the tests from an archive of the compat package's
# acceptance tests instead of building them.
if [[ -n "${NEXTEST_ARCHIVE_FILE:-}" ]]; then
  nextest_run=(cargo-nextest nextest run --archive-file "$NEXTEST_ARCHIVE_FILE" --workspace-remap "$PWD")
else
  nextest_run=(cargo nextest run --locked -p zinder-compat-lightwalletd --test acceptance)
fi

# Both nodes can identify Regtest as BIP70 "test". The matrix activation
# heights distinguish its disposable chain from the public test network.
rpc_request='{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}'
rpc_args=(-fsS --max-time 5 -H 'content-type: application/json' -d "$rpc_request")
chain_fence() {
  jq -er 'select(.error == null) | .result |
    select((.chain == "test" or .chain == "regtest") and
      .upgrades["77190ad9"].activationheight == 140 and
      .upgrades["37a5165b"].activationheight == 6) |
    [.blocks, .bestblockhash] | @tsv'
}
node_id=$(docker inspect -f '{{.Id}}' "$container")
fence=$(curl "${rpc_args[@]}" "$rpc_url" | chain_fence)
container_fence=$(docker exec "$node_id" curl "${rpc_args[@]}" http://127.0.0.1:39232 | chain_fence)
if [[ "$container_fence" != "$fence" ]]; then
  echo "the disposable container and the supplied Regtest endpoint have different tips" >&2
  exit 2
fi
read -r tip _ <<<"$fence"
if ((tip < 120)); then
  echo "the parity chain requires mature outputs paying the broadcast test key" >&2
  exit 2
fi

root=$(mktemp -d)
pids=()
restart_node=false
cleanup() {
  local status=$?
  trap - EXIT
  if [[ "${#pids[@]}" -gt 0 ]]; then
    kill "${pids[@]}" 2>/dev/null || true
    for pid in "${pids[@]}"; do wait "$pid" || true; done
  fi
  if "$restart_node"; then
    docker start "$node_id" >/dev/null || status=1
  fi
  if [[ "$status" != 0 ]]; then
    for log in "$root"/*.log; do
      if [[ -f "$log" ]]; then echo "$log" >&2; tail -n 20 "$log" >&2; fi
    done
  fi
  rm -rf -- "$root"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_ready() {
  local pid=$1 port=$2 stopped=$3
  for _ in {1..90}; do
    kill -0 "$pid" || return 1
    if curl -fs --max-time 2 "http://127.0.0.1:$port/readyz" | jq -e \
      --argjson tip "$tip" --argjson stopped "$stopped" \
      '.status == "ready" and .current_height == $tip and
        (any(.degraded[]?; .input == "node_source") == $stopped)' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "port $port did not serve height $tip with node_source degraded=$stopped" >&2
  return 1
}

start_candidate() {
  local policy=$1 port=$2
  local -a ingest_selection compat_selection
  if [[ $policy == none ]]; then
    ingest_selection=(ZINDER_DEPLOYMENT__PROFILE=light-wallet)
    compat_selection=(ZINDER_DEPLOYMENT__PROFILE=light-wallet)
  else
    ingest_selection=("ZINDER_STORAGE__RAW_BLOB_POLICY=$policy" ZINDER_INGEST__RUN_OVERRIDES__COVERAGE=wallet-serving)
    compat_selection=("ZINDER_STORAGE__RAW_BLOB_POLICY=$policy" ZINDER_COMPAT__SERVING=light-wallet)
  fi
  env -u ZINDER_DEPLOYMENT__PROFILE -u ZINDER_STORAGE__RAW_BLOB_POLICY -u ZINDER_INGEST__RUN_OVERRIDES__COVERAGE \
    "${ingest_selection[@]}" \
    ZINDER_INGEST_CONTROL__LISTEN_ADDR="127.0.0.1:$port" \
    ZINDER_OPS__LISTEN_ADDR="127.0.0.1:$((port + 1))" \
    "$bin_dir/zinder-ingest" --network zcash-regtest \
      --storage-path "$root/$policy-canonical" >"$root/$policy-ingest.log" 2>&1 &
  pids+=("$!")
  wait_ready "$!" "$((port + 1))" false

  env -u ZINDER_DEPLOYMENT__PROFILE -u ZINDER_STORAGE__RAW_BLOB_POLICY -u ZINDER_COMPAT__SERVING \
    "${compat_selection[@]}" \
    ZINDER_INGEST_CONTROL__ADDR="http://127.0.0.1:$port" \
    ZINDER_COMPAT__STALE_TIP_LIMIT_MS=0 \
    ZINDER_OPS__LISTEN_ADDR="127.0.0.1:$((port + 3))" \
    "$bin_dir/zinder-compat-lightwalletd" --network zcash-regtest \
      --listen-addr "127.0.0.1:$((port + 2))" \
      --canonical-primary-path "$root/$policy-canonical" \
      --canonical-secondary-root "$root/$policy-secondary" >"$root/$policy-compat.log" 2>&1 &
  pids+=("$!")
  compat_pids[$policy]=$!
  wait_ready "$!" "$((port + 3))" false
}

run_tests() {
  local policy=$1 port=$2 stopped=$3
  local reference=${4:-$wallet_url} filter=${5:-'test(/^live::light_wallet::/) or test(tree_state_by_hash_matches_reference) or test(unknown_tree_state_queries_match_reference_status)'}
  echo "Checking raw_blob_policy=$policy, node_stopped=$stopped, tip=$tip"
  ZINDER_TEST_LIVE=1 ZINDER_NETWORK=zcash-regtest \
    ZINDER_TEST_LIGHT_WALLET_RAW_BLOB_POLICY="$policy" \
    ZINDER_TEST_LIGHT_WALLET_NODE_STOPPED="$stopped" \
    ZINDER_TEST_PARITY_ZINDER_ADDR="http://127.0.0.1:$((port + 2))" \
    ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR="$reference" \
    "${nextest_run[@]}" \
      --profile=ci-live --run-ignored=all --retries 0 --test-threads 1 -E "$filter"
}

declare -A ports=([none]=19100 [transactions]=19110) compat_pids
for policy in none transactions; do
  start_candidate "$policy" "${ports[$policy]}"
done
for policy in none transactions; do
  run_tests "$policy" "${ports[$policy]}" false
done

# All three wallet endpoints observe the same signed spend before the node is stopped.
broadcast='test(accepted_send_transaction_surfaces_in_non_empty_mempool_methods)'
ZINDER_TEST_LIGHT_WALLET_RETAINED_ADDR="http://127.0.0.1:$((ports[transactions] + 2))" \
  run_tests none "${ports[none]}" false "$wallet_url" "$broadcast"

restart_node=true
docker stop "$node_id" >/dev/null
if curl "${rpc_args[@]}" "$rpc_url" >/dev/null 2>&1; then
  echo "the selected Regtest RPC still answers after stopping $container" >&2
  exit 1
fi
for policy in none transactions; do
  wait_ready "${compat_pids[$policy]}" "$((ports[$policy] + 3))" true
  run_tests "$policy" "${ports[$policy]}" true
done
