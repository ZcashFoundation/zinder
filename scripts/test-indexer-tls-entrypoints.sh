#!/usr/bin/env bash
set -euo pipefail

# Exercises indexer TLS key materialization in the two root-run image
# entrypoints. Each case runs a copy of an entrypoint whose fragment path and
# file locations point into a scratch directory, with chown, setpriv, and the
# filesystem-type probe replaced by stubs, so it needs neither root nor a
# container.

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
fragment="$repository_root/deploy/materialize-indexer-tls"
canonical_entrypoint="$repository_root/deploy/canonical-runtime-entrypoint"
wallet_entrypoint="$repository_root/deploy/wallet-serving-runtime-entrypoint"

test_failure() {
  printf 'indexer TLS entrypoint test failed: %s\n' "$*" >&2
  exit 1
}

work="$(mktemp -d)"
trap 'chmod -R u+rwx "$work" 2>/dev/null || true; rm -rf "$work"' EXIT

stub_bin="$work/bin"
mkdir -p "$stub_bin"
cat >"$stub_bin/chown" <<'STUB'
#!/bin/sh
exit 0
STUB
# Records the environment the runtime process would receive.
cat >"$stub_bin/setpriv" <<'STUB'
#!/bin/sh
env >"$STUB_ENV_DUMP"
STUB
# Reports the filesystem type the test selects for the tmpfs candidate.
cat >"$stub_bin/stat" <<'STUB'
#!/bin/sh
printf '%s\n' "$STUB_TMPFS_TYPE"
STUB
chmod +x "$stub_bin/chown" "$stub_bin/setpriv" "$stub_bin/stat"

# Rewrites one exact line in a copied file and fails when it is missing, so a
# change to the entrypoints cannot silently bypass the scratch locations.
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
  'indexer_tls_tmpfs_parent="/dev/shm"' "indexer_tls_tmpfs_parent=\"$work/shm\""
rewrite_line "$test_fragment" \
  'indexer_tls_fallback_parent="/run"' "indexer_tls_fallback_parent=\"$work/run\""

test_canonical="$work/canonical-runtime-entrypoint"
cp "$canonical_entrypoint" "$test_canonical"
rewrite_line "$test_canonical" \
  '. /usr/local/lib/zinder/materialize-indexer-tls' ". $test_fragment"

# The wallet-serving entrypoint starts its topology after the TLS block, so the
# copy keeps the script up to the block's closing `fi` and then records the
# environment and the shell's initial environment instead.
test_wallet="$work/wallet-serving-runtime-entrypoint"
awk '
  { print }
  /^materialize_indexer_tls$/ { in_block = 1 }
  in_block && /^fi$/ { exit }
' "$wallet_entrypoint" >"$test_wallet"
grep -qx 'fi' "$test_wallet" || test_failure "wallet-serving entrypoint has no indexer TLS block"
# shellcheck disable=SC2016 # matches the literal re-exec arguments
grep -qF '"$0" "$@"' "$test_wallet" || test_failure "wallet-serving entrypoint no longer re-executes itself"
rewrite_line "$test_wallet" \
  '. /usr/local/lib/zinder/materialize-indexer-tls' ". $test_fragment"
cat >>"$test_wallet" <<'TAIL'
env >"$STUB_ENV_DUMP"
tr '\0' '\n' <"/proc/$$/environ" >"$STUB_ENV_DUMP.initial"
TAIL
chmod +x "$test_canonical" "$test_wallet"

newline='
'
ca_pem="-----BEGIN CERTIFICATE-----${newline}Y2EtY2VydGlmaWNhdGU=${newline}-----END CERTIFICATE-----"
client_pem="-----BEGIN CERTIFICATE-----${newline}Y2xpZW50LWNlcnRpZmljYXRl${newline}-----END CERTIFICATE-----"
key_pem="-----BEGIN PRIVATE KEY-----${newline}Y2xpZW50LWtleQ==${newline}-----END PRIVATE KEY-----"

env_dump="$work/env"
stderr_log="$work/stderr"
directory_name="zinder-indexer-tls"

reset_locations() {
  chmod -R u+rwx "$work/shm" "$work/run" 2>/dev/null || true
  rm -rf "$work/shm" "$work/run" "$env_dump" "$env_dump.initial" "$stderr_log"
  mkdir -p "$work/shm" "$work/run"
}

# Runs an entrypoint copy with a scrubbed environment plus the given
# assignments, recording its stderr.
run_entrypoint() {
  entrypoint="$1"
  tmpfs_type="$2"
  shift 2
  env -i \
    PATH="$stub_bin:/usr/bin:/bin" \
    STUB_ENV_DUMP="$env_dump" \
    STUB_TMPFS_TYPE="$tmpfs_type" \
    "$@" \
    sh "$entrypoint" --config /etc/zinder/ingest.toml 2>"$stderr_log"
}

assert_no_pem_variable() {
  dump="$1"
  if grep -q '_PEM=' "$dump"; then
    test_failure "a *_PEM variable reached the runtime environment in $dump"
  fi
  if grep -q 'BEGIN' "$dump"; then
    test_failure "PEM text reached the runtime environment in $dump"
  fi
}

assert_materialized() {
  parent="$1"
  directory="$parent/$directory_name"
  [ -d "$directory" ] && [ ! -L "$directory" ] || test_failure "$directory was not created as a directory"
  [ "$(stat_mode "$directory")" = "711" ] || test_failure "$directory mode is $(stat_mode "$directory")"
  for material in ca-cert client-cert client-key; do
    file="$directory/$material.pem"
    [ -f "$file" ] || test_failure "$file was not written"
    [ "$(stat_mode "$file")" = "400" ] || test_failure "$file mode is $(stat_mode "$file")"
  done
  [ "$(cat "$directory/client-key.pem")" = "$key_pem" ] \
    || test_failure "the client key file does not hold the supplied PEM"
  grep -qxF "ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH=$directory/ca-cert.pem" "$env_dump" \
    || test_failure "the CA path variable was not exported"
  grep -qxF "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH=$directory/client-cert.pem" "$env_dump" \
    || test_failure "the client certificate path variable was not exported"
  grep -qxF "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=$directory/client-key.pem" "$env_dump" \
    || test_failure "the client key path variable was not exported"
}

stat_mode() {
  /usr/bin/stat -c %a "$1"
}

all_pem_variables=(
  "ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM=$ca_pem"
  "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM=$client_pem"
  "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM=$key_pem"
)

# Canonical image: materializes on tmpfs and execs ingest without *_PEM.
reset_locations
run_entrypoint "$test_canonical" tmpfs "${all_pem_variables[@]}" \
  || test_failure "canonical entrypoint failed: $(cat "$stderr_log")"
assert_materialized "$work/shm"
assert_no_pem_variable "$env_dump"
grep -q "tmpfs at $work/shm/$directory_name" "$stderr_log" \
  || test_failure "the chosen tmpfs location was not logged"

# A stale directory, or a symlink planted in its place, is replaced and never
# followed.
reset_locations
mkdir -p "$work/shm/$directory_name" "$work/elsewhere"
printf 'stale\n' >"$work/shm/$directory_name/stale.pem"
run_entrypoint "$test_canonical" tmpfs "${all_pem_variables[@]}" \
  || test_failure "canonical entrypoint failed over a stale directory: $(cat "$stderr_log")"
[ ! -e "$work/shm/$directory_name/stale.pem" ] || test_failure "the stale directory was reused"
reset_locations
mkdir -p "$work/elsewhere"
ln -s "$work/elsewhere" "$work/shm/$directory_name"
run_entrypoint "$test_canonical" tmpfs "${all_pem_variables[@]}" \
  || test_failure "canonical entrypoint failed over a planted symlink: $(cat "$stderr_log")"
assert_materialized "$work/shm"
[ -z "$(ls -A "$work/elsewhere")" ] || test_failure "materialization followed a planted symlink"

# Without a writable tmpfs the files go to /run, and the log says they may be
# on disk.
reset_locations
run_entrypoint "$test_canonical" ext2/ext3 "${all_pem_variables[@]}" \
  || test_failure "canonical entrypoint failed on the /run fallback: $(cat "$stderr_log")"
assert_materialized "$work/run"
grep -q "may be on disk" "$stderr_log" || test_failure "the /run fallback was not logged"

# With neither location writable, startup fails closed.
if [ "$(id -u)" -ne 0 ]; then
  reset_locations
  chmod 0555 "$work/run"
  if run_entrypoint "$test_canonical" ext2/ext3 "${all_pem_variables[@]}"; then
    test_failure "canonical entrypoint started without a writable location"
  fi
  [ ! -e "$env_dump" ] || test_failure "ingest started without a writable location"
fi

# Setting both forms of one item is refused before anything starts.
reset_locations
if run_entrypoint "$test_canonical" tmpfs "${all_pem_variables[@]}" \
  ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=/run/secrets/client.key; then
  test_failure "canonical entrypoint accepted both CLIENT_KEY_PEM and CLIENT_KEY_PATH"
fi
grep -q "set either ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM or ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH" \
  "$stderr_log" || test_failure "the conflict error does not name both variables"
[ ! -e "$env_dump" ] || test_failure "ingest started despite the conflict"

# Values whose line breaks were collapsed, or that are not PEM, are refused.
reset_locations
if run_entrypoint "$test_canonical" tmpfs \
  "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM=-----BEGIN PRIVATE KEY----- Y2xpZW50LWtleQ== -----END PRIVATE KEY-----"; then
  test_failure "canonical entrypoint accepted PEM text without line breaks"
fi
grep -q "without line breaks" "$stderr_log" || test_failure "the collapsed PEM error is unclear"
if grep -q "Y2xpZW50LWtleQ" "$stderr_log"; then
  test_failure "the collapsed PEM error printed the key"
fi
reset_locations
if run_entrypoint "$test_canonical" tmpfs "ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM=not a certificate"; then
  test_failure "canonical entrypoint accepted a value that is not PEM"
fi
grep -q "lacks a -----BEGIN line" "$stderr_log" || test_failure "the non-PEM error is unclear"

# An empty *_PEM variable writes nothing and still never reaches ingest.
reset_locations
run_entrypoint "$test_canonical" tmpfs ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM= \
  || test_failure "canonical entrypoint failed on an empty variable: $(cat "$stderr_log")"
[ ! -e "$work/shm/$directory_name" ] || test_failure "an empty variable created the directory"
assert_no_pem_variable "$env_dump"

# Without *_PEM variables the entrypoint leaves configured paths untouched.
reset_locations
run_entrypoint "$test_canonical" tmpfs \
  ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=/run/secrets/client.key \
  || test_failure "canonical entrypoint failed without TLS variables: $(cat "$stderr_log")"
grep -qxF "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=/run/secrets/client.key" "$env_dump" \
  || test_failure "a mounted key path did not reach ingest"
[ ! -e "$work/shm/$directory_name" ] || test_failure "the directory was created without *_PEM variables"

# Wallet-serving image: materializes once, then re-executes itself so the
# supervising shell's initial environment carries no *_PEM variable either.
reset_locations
run_entrypoint "$test_wallet" tmpfs "${all_pem_variables[@]}" \
  || test_failure "wallet-serving entrypoint failed: $(cat "$stderr_log")"
assert_materialized "$work/shm"
assert_no_pem_variable "$env_dump"
assert_no_pem_variable "$env_dump.initial"
[ "$(grep -c "writing indexer TLS material" "$stderr_log")" -eq 1 ] \
  || test_failure "the re-executed wallet-serving entrypoint materialized twice"

reset_locations
run_entrypoint "$test_wallet" tmpfs ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM= \
  || test_failure "wallet-serving entrypoint failed on an empty variable: $(cat "$stderr_log")"
assert_no_pem_variable "$env_dump"
assert_no_pem_variable "$env_dump.initial"

reset_locations
if run_entrypoint "$test_wallet" tmpfs "${all_pem_variables[@]}" \
  ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH=/run/secrets/ca.pem; then
  test_failure "wallet-serving entrypoint accepted both CA_CERT_PEM and CA_CERT_PATH"
fi
[ ! -e "$env_dump" ] || test_failure "wallet serving started despite the conflict"

printf 'indexer TLS entrypoint tests passed\n'
