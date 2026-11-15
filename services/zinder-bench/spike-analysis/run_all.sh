#!/usr/bin/env bash
# Spike B measured sequence (Testnet). Runs strictly one after another so no two
# measurements share the host. The node cookie is passed by path only.
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

log "sequence start rev=$REV"
# Arm 3: whole-Testnet-chain wallet rebuild from the private hard-link copy of the data-prep canonical store.
if [ "${SKIP_ARM3:-0}" != 1 ]; then
  mkdir -p $R/arm3; host
  rm -rf .tmp/arm3/wallet.rocksdb .tmp/arm3/wallet.rocksdb.projection-load-staging .tmp/arm3/wallet.rocksdb.build-lock
  log "arm3 start"
  /usr/bin/time -v $BIN rocksdb-wallet-rebuild "${COMMON[@]}" --canonical-store .tmp/arm3/work/canonical \
    --wallet-store .tmp/arm3/wallet.rocksdb > $R/arm3/rebuild.json 2> $R/arm3/rebuild.log
  log "arm3 exit $?"; host
  du -sb .tmp/arm3/wallet.rocksdb > $R/arm3/wallet-store-du.txt 2>&1
  rm -rf .tmp/arm3/wallet.rocksdb .tmp/arm3/wallet.rocksdb.projection-load-staging
fi

run() { # name, extra args...
  local name=$1; shift
  local M=$R/$name
  rm -rf $M/work; mkdir -p $M; host
  log "$name start"
  $BIN rocksdb-in-process-serving "${COMMON[@]}" --canonical-source $H0/canonical.rocksdb --wallet-source $H0/wallet.rocksdb \
    --work-dir $M/work --out-dir $M/out --replay-end-height 4599999 --software-revision "$REV" "$@" > $M/stderr.log 2>&1
  log "$name exit $?"
  du -sb $M/work > $M/work-du.txt 2>&1; host
  rm -rf $M/work
}
[ "${SKIP_RUN1:-0}" != 1 ] && run run1-1x --plan-preset measured
[ "${SKIP_D10:-0}" != 1 ] && run density-x10 --plan-preset density --density-multiplier-override 10
[ "${SKIP_D60:-0}" != 1 ] && run density-x60 --plan-preset density --density-multiplier-override 60
log "sequence done"
