#!/usr/bin/env bash
# Paired no-hold density controls: same input stores, blocks, multiplier, and phase timing as the
# density runs, with no readers in the middle phase. Starts after run_rest.sh finishes.
set -u
cd /home/ubuntu/.t3/worktrees/zinder/spike-in-process-serving
R=/home/ubuntu/.t3/worktrees/zinder/spike-in-process-serving/.tmp/spike-results
BIN=./target/release/zinder-bench
COOKIE=/home/ubuntu/.zakura/testnet/cookie/.cookie
REV=$(git rev-parse HEAD)
H0=.tmp/testnet-h4580000
COMMON=(--network zcash-testnet --json-rpc-addr http://127.0.0.1:49232 --node-auth-cookie $COOKIE)
log() { echo "$(date -u +%FT%TZ) $*" >> $R/sequence.log; }
host() { { date -u +%FT%TZ; uptime; df -B1 / | tail -1; } >> $R/host-load.log; }
until grep -q "sequence done" $R/sequence.log; do sleep 10; done
run() {
  local name=$1 need=$2; shift 2
  local M=$R/$name
  local free=$(( $(df -B1 --output=avail / | tail -1) / 1073741824 ))
  if [ $free -lt $need ]; then log "$name skipped: free ${free} GiB < ${need} GiB"; return; fi
  rm -rf $M/work; mkdir -p $M; host
  log "$name start (free ${free} GiB)"
  $BIN rocksdb-in-process-serving "${COMMON[@]}" --canonical-source $H0/canonical.rocksdb --wallet-source $H0/wallet.rocksdb \
    --work-dir $M/work --out-dir $M/out --replay-end-height 4599999 --software-revision "$REV" "$@" > $M/stderr.log 2>&1
  log "$name exit $?"
  du -sb $M/work > $M/work-du.txt 2>&1; host
  rm -rf $M/work
}
run density-x60-control 18 --plan $R/plan-density-control.json --density-multiplier-override 60
run density-x10-control 8 --plan $R/plan-density-control.json --density-multiplier-override 10
log "controls done"
