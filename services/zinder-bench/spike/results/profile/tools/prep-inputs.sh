#!/usr/bin/env bash
# Rebuild the Spike A replay inputs for the profiling window (Testnet).
#   H0 = 4600000, fixture 4600001..4620122.
# Usage: prep-inputs.sh canonical|wallet|capture|prepare
# The node is used read-only; the cookie is referenced by path only.
set -euo pipefail
P=/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit/.tmp/profile
H=$P/h0
B=$P/target/release/zinder-bench
I=$P/target/release/zinder-ingest
L=$P/logs
COOKIE=/home/ubuntu/.zakura/testnet/cookie/.cookie
H0=4600000
T=4620122
FIX=$H/fixture-$((H0 + 1))-$T
mkdir -p "$H" "$L"
guard() {
  local free
  free=$(df --output=avail -BG / | tail -1 | tr -dc 0-9)
  if [ "$free" -lt 60 ]; then echo "abort: ${free} GB free" >&2; exit 3; fi
}
guard
case "${1:?stage}" in
canonical)
  ZINDER_NODE__AUTH__METHOD=cookie ZINDER_NODE__AUTH__PATH=$COOKIE \
  ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:49230 \
  /usr/bin/time -v -o "$L/canonical-h0.time" "$I" --network zcash-testnet --node-source zebra-json-rpc \
    --json-rpc-addr http://127.0.0.1:49232 --node-auth-method cookie --node-auth-path "$COOKIE" \
    --storage-path "$H/canonical-h0.rocksdb" --wallet-serving --target-height $H0 \
    --ingest-control-listen-addr 127.0.0.1:19300 --ops-listen-addr 127.0.0.1:19305 \
    > "$L/canonical-h0.log" 2>&1
  ;;
wallet)
  /usr/bin/time -v -o "$L/wallet-h0.time" "$B" rocksdb-wallet-rebuild --network zcash-testnet \
    --json-rpc-addr http://127.0.0.1:49232 --node-auth-cookie "$COOKIE" \
    --canonical-store "$H/canonical-h0.rocksdb" --wallet-store "$H/wallet-h0.rocksdb" \
    > "$L/wallet-h0.json" 2> "$L/wallet-h0.log"
  ;;
capture)
  /usr/bin/time -v -o "$L/capture.time" "$B" capture --network zcash-testnet \
    --json-rpc-addr http://127.0.0.1:49232 --node-auth-cookie "$COOKIE" \
    --from-height $((H0 + 1)) --to-height $T --out "$FIX" > "$L/capture.log" 2>&1
  ;;
prepare)
  /usr/bin/time -v -o "$L/prepare.time" "$B" spike-combined-commit prepare --fixture "$FIX" \
    --canonical-h0 "$H/canonical-h0.rocksdb" --wallet-h0 "$H/wallet-h0.rocksdb" \
    --out-dir "$H/golden" --report "$L/prepare.json" --label profile-h0-$H0 2> "$L/prepare.log"
  ;;
*) echo "unknown stage" >&2; exit 2 ;;
esac
guard
