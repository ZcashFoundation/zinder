#!/usr/bin/env bash
set -euo pipefail

# Exercises the supervision decisions of the wallet-serving Explorer runtime
# entrypoint. Each case runs a copy of the entrypoint whose wallet-serving
# entrypoint, healthcheck, and Explorer binary are stubs in a scratch
# directory, with chown and setpriv replaced as well, so it needs neither root
# nor a container.

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
entrypoint="$repository_root/deploy/wallet-serving-explorer-runtime-entrypoint"
fragment="$repository_root/deploy/materialize-indexer-tls"

test_failure() {
  printf 'wallet-serving Explorer entrypoint test failed: %s\n' "$*" >&2
  exit 1
}

work="$(mktemp -d)"
wrapper_pid=""
cleanup() {
  if [[ -n "$wrapper_pid" ]]; then
    kill -s KILL "$wrapper_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

stub_bin="$work/bin"
mkdir -p "$stub_bin"
cat >"$stub_bin/chown" <<'STUB'
#!/bin/sh
exit 0
STUB
# Drops the privilege options and runs the command as the current user.
cat >"$stub_bin/setpriv" <<'STUB'
#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    --*) shift ;;
    *) break ;;
  esac
done
exec "$@"
STUB

# Each runtime stub records its environment, then serves until SIGTERM or
# exits as its mode selects.
cat >"$work/wallet-serving" <<'STUB'
#!/bin/sh
env >"$STUB_WORK/wallet.env"
trap 'printf terminated >"$STUB_WORK/wallet.stopped"; exit 0' TERM
case "$STUB_WALLET_MODE" in
  fail-before-ready) exit 3 ;;
esac
: >"$STUB_WORK/ready"
elapsed=0
while true; do
  sleep 0.1
  elapsed=$((elapsed + 1))
  if [ "$STUB_WALLET_MODE" = exit-after-ready ] && [ -e "$STUB_WORK/explorer.env" ] \
    && [ "$elapsed" -ge 10 ]; then
    exit 0
  fi
done
STUB
cat >"$work/healthcheck" <<'STUB'
#!/bin/sh
[ -e "$STUB_WORK/ready" ]
STUB
cat >"$work/explorer" <<'STUB'
#!/bin/sh
case "$*" in
  *--print-config*)
    env >"$STUB_WORK/explorer-preflight.env"
    [ "$STUB_EXPLORER_MODE" != invalid-config ] || exit 2
    exit 0
    ;;
esac
printf 'start\n' >>"$STUB_WORK/explorer.starts"
[ -e "$STUB_WORK/ready" ] || printf 'started before wallet serving was ready\n' >"$STUB_WORK/explorer.early"
printf '%s\n' "$*" >"$STUB_WORK/explorer.args"
env >"$STUB_WORK/explorer.env"
trap 'printf terminated >"$STUB_WORK/explorer.stopped"; exit 0' TERM
case "$STUB_EXPLORER_MODE" in
  crash) sleep 1; exit 5 ;;
esac
while true; do
  sleep 0.1
done
STUB
chmod +x "$stub_bin/chown" "$stub_bin/setpriv" "$work/wallet-serving" "$work/healthcheck" "$work/explorer"

# Rewrites one exact line in a copied file and fails when it is missing, so a
# change to the entrypoint cannot silently bypass the scratch locations.
rewrite_line() {
  file="$1"
  original="$2"
  replacement="$3"
  grep -qxF -- "$original" "$file" || test_failure "$file no longer contains: $original"
  awk -v original="$original" -v replacement="$replacement" \
    '$0 == original { print replacement; next } { print }' "$file" >"$file.rewritten"
  mv "$file.rewritten" "$file"
}

test_fragment="$work/materialize-indexer-tls"
cp "$fragment" "$test_fragment"
rewrite_line "$test_fragment" \
  'indexer_tls_tmpfs_parent="/dev/shm"' "indexer_tls_tmpfs_parent=\"$work/no-tmpfs\""
rewrite_line "$test_fragment" \
  'indexer_tls_fallback_parent="/run"' "indexer_tls_fallback_parent=\"$work/run\""

test_entrypoint="$work/wallet-serving-explorer-runtime-entrypoint"
cp "$entrypoint" "$test_entrypoint"
rewrite_line "$test_entrypoint" \
  '. /usr/local/lib/zinder/materialize-indexer-tls' ". $test_fragment"
rewrite_line "$test_entrypoint" \
  'wallet_serving_entrypoint="/usr/local/bin/zinder-wallet-serving-runtime-entrypoint"' \
  "wallet_serving_entrypoint=\"$work/wallet-serving\""
rewrite_line "$test_entrypoint" \
  'wallet_serving_healthcheck="/usr/local/bin/zinder-wallet-serving-healthcheck"' \
  "wallet_serving_healthcheck=\"$work/healthcheck\""
rewrite_line "$test_entrypoint" \
  'explorer_binary="/usr/local/bin/zinder-explorer"' "explorer_binary=\"$work/explorer\""
rewrite_line "$test_entrypoint" \
  'explorer_secondary_path="/var/lib/zinder/explorer/secondary"' \
  "explorer_secondary_path=\"$work/volume/explorer/secondary\""
rewrite_line "$test_entrypoint" 'poll_interval_seconds=15' 'poll_interval_seconds=1'
rewrite_line "$test_entrypoint" 'shutdown_grace_seconds=25' 'shutdown_grace_seconds=5'
rewrite_line "$test_entrypoint" 'explorer_restart_initial_seconds=15' 'explorer_restart_initial_seconds=1'
rewrite_line "$test_entrypoint" 'explorer_restart_max_seconds=300' 'explorer_restart_max_seconds=8'
# An unprivileged test cannot raise the open-file limit past its hard limit.
rewrite_line "$test_entrypoint" 'nofile_limit=65536' "nofile_limit=$(ulimit -n)"
chmod +x "$test_entrypoint"

newline='
'
key_pem="-----BEGIN PRIVATE KEY-----${newline}Y2xpZW50LWtleQ==${newline}-----END PRIVATE KEY-----"

reset_case() {
  rm -rf "$work/case" "$work/run" "$work/volume"
  mkdir -p "$work/case" "$work/run"
}

# Starts the entrypoint copy in the background with a scrubbed environment plus
# the given assignments.
start_entrypoint() {
  env -i \
    PATH="$stub_bin:/usr/bin:/bin" \
    STUB_WORK="$work/case" \
    STUB_WALLET_MODE="${wallet_mode:-serve}" \
    STUB_EXPLORER_MODE="${explorer_mode:-serve}" \
    "$@" \
    "$test_entrypoint" 2>"$work/case/stderr" &
  wrapper_pid="$!"
}

# Waits for the entrypoint to exit and stores its status in wrapper_status.
await_entrypoint_exit() {
  wrapper_status=0
  for _ in $(seq 1 150); do
    if ! kill -0 "$wrapper_pid" 2>/dev/null; then
      wait "$wrapper_pid" || wrapper_status="$?"
      wrapper_pid=""
      return 0
    fi
    sleep 0.1
  done
  test_failure "the entrypoint did not exit: $(cat "$work/case/stderr")"
}

await_file() {
  for _ in $(seq 1 150); do
    [[ -e "$1" ]] && return 0
    sleep 0.1
  done
  test_failure "$1 never appeared: $(cat "$work/case/stderr")"
}

# Wallet serving starts first, the Explorer after the healthcheck passes, each
# with only the variables it owns, and SIGTERM stops both cleanly.
reset_case
wallet_mode=serve explorer_mode=serve start_entrypoint \
  ZINDER_NETWORK__NAME=zcash-testnet \
  ZINDER_NODE__JSON_RPC_ADDR=http://node:18232 \
  ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM="$key_pem" \
  ZINDER_INGEST__REORG_WINDOW_BLOCKS=1000 \
  ZINDER_PROJECTOR__BUILD_OWNER_HEX=00112233445566778899aabbccddeeff \
  ZINDER_EXPLORER__BEARER_TOKEN_PATH=/run/secrets/explorer.token \
  INGEST_CONTROL_BEARER_TOKEN=control-secret \
  CHECKPOINT_BEARER_TOKEN=checkpoint-secret \
  RUST_LOG=info
await_file "$work/case/explorer.env"
[[ ! -e "$work/case/explorer.early" ]] || test_failure "the Explorer started before wallet serving was ready"
[[ -d "$work/volume/explorer/secondary" ]] || test_failure "the Explorer secondary directory was not created"
[[ "$(cat "$work/case/explorer.args")" == "--config /etc/zinder/explorer.toml" ]] \
  || test_failure "the Explorer was not started with its baked config"

wallet_env="$work/case/wallet.env"
explorer_env="$work/case/explorer.env"
for expected in ZINDER_NETWORK__NAME ZINDER_NODE__JSON_RPC_ADDR ZINDER_INGEST__REORG_WINDOW_BLOCKS \
  ZINDER_PROJECTOR__BUILD_OWNER_HEX INGEST_CONTROL_BEARER_TOKEN CHECKPOINT_BEARER_TOKEN; do
  grep -q "^${expected}=" "$wallet_env" || test_failure "wallet serving did not receive $expected"
done
if grep -q '^ZINDER_EXPLORER__' "$wallet_env"; then
  test_failure "an Explorer variable reached the wallet-serving processes"
fi
for expected in ZINDER_NETWORK__NAME ZINDER_NODE__JSON_RPC_ADDR ZINDER_EXPLORER__BEARER_TOKEN_PATH RUST_LOG; do
  grep -q "^${expected}=" "$explorer_env" || test_failure "the Explorer did not receive $expected"
done
for unexpected in ZINDER_INGEST__ ZINDER_PROJECTOR__ INGEST_CONTROL_BEARER_TOKEN CHECKPOINT_BEARER_TOKEN; do
  for runtime_env in "$explorer_env" "$work/case/explorer-preflight.env"; do
    if grep -q "^${unexpected}" "$runtime_env"; then
      test_failure "$unexpected reached $runtime_env"
    fi
  done
done
grep -q '^ZINDER_EXPLORER__BEARER_TOKEN_PATH=' "$work/case/explorer-preflight.env" \
  || test_failure "the Explorer configuration check did not receive its variables"
for runtime_env in "$wallet_env" "$explorer_env"; do
  if grep -q '_PEM=' "$runtime_env" || grep -q 'BEGIN' "$runtime_env"; then
    test_failure "indexer TLS PEM text reached $runtime_env"
  fi
  grep -qxF "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=$work/run/zinder-indexer-tls/client-key.pem" \
    "$runtime_env" || test_failure "the materialized client key path did not reach $runtime_env"
done

kill -s TERM "$wrapper_pid"
await_entrypoint_exit
[[ "$wrapper_status" -eq 0 ]] || test_failure "a forwarded SIGTERM exited $wrapper_status"
[[ -e "$work/case/wallet.stopped" ]] || test_failure "SIGTERM did not reach wallet serving"
[[ -e "$work/case/explorer.stopped" ]] || test_failure "SIGTERM did not reach the Explorer"

# A wallet-serving topology that fails before serving ends the container with
# its status and never starts the Explorer.
reset_case
wallet_mode=fail-before-ready start_entrypoint
await_entrypoint_exit
[[ "$wrapper_status" -eq 3 ]] || test_failure "a failed wallet-serving start exited $wrapper_status, expected 3"
[[ ! -e "$work/case/explorer.env" ]] || test_failure "the Explorer started after wallet serving failed"

# An Explorer configuration the binary rejects fails the container before wallet
# serving starts.
reset_case
wallet_mode=serve explorer_mode=invalid-config start_entrypoint
await_entrypoint_exit
[[ "$wrapper_status" -eq 1 ]] || test_failure "a rejected Explorer config exited $wrapper_status, expected 1"
[[ ! -e "$work/case/wallet.env" ]] || test_failure "wallet serving started after the Explorer config was rejected"
grep -q 'zinder-explorer rejected its configuration' "$work/case/stderr" \
  || test_failure "the rejected Explorer config was not logged"

# An Explorer that keeps exiting is restarted while wallet serving keeps
# running, and SIGTERM still stops both cleanly.
reset_case
wallet_mode=serve explorer_mode=crash start_entrypoint
for _ in $(seq 1 150); do
  [[ -e "$work/case/explorer.starts" ]] && [[ "$(wc -l <"$work/case/explorer.starts")" -ge 3 ]] && break
  sleep 0.1
done
[[ "$(wc -l <"$work/case/explorer.starts")" -ge 3 ]] \
  || test_failure "a crashing Explorer was not restarted: $(cat "$work/case/stderr")"
kill -0 "$wrapper_pid" 2>/dev/null || test_failure "an Explorer exit ended the container"
[[ ! -e "$work/case/wallet.stopped" ]] || test_failure "an Explorer exit stopped wallet serving"
grep -q 'zinder-explorer exited with status 5 .*; wallet serving continues; restarting zinder-explorer in 1s' \
  "$work/case/stderr" || test_failure "the first Explorer exit was not logged: $(cat "$work/case/stderr")"
grep -q 'restarting zinder-explorer in 2s' "$work/case/stderr" \
  || test_failure "the Explorer restart delay did not grow: $(cat "$work/case/stderr")"
kill -s TERM "$wrapper_pid"
await_entrypoint_exit
[[ "$wrapper_status" -eq 0 ]] || test_failure "SIGTERM during Explorer restarts exited $wrapper_status"
[[ -e "$work/case/wallet.stopped" ]] || test_failure "SIGTERM did not reach wallet serving"

# A wallet-serving exit stops the Explorer, and a zero status still fails the
# container because only a forwarded signal ends wallet serving cleanly.
reset_case
wallet_mode=exit-after-ready explorer_mode=serve start_entrypoint
await_entrypoint_exit
[[ "$wrapper_status" -eq 1 ]] || test_failure "a wallet-serving exit ended the container with $wrapper_status"
[[ -e "$work/case/explorer.stopped" ]] || test_failure "a wallet-serving exit left the Explorer running"

printf 'wallet-serving Explorer entrypoint tests passed\n'
