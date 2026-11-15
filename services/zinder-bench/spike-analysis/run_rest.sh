#!/usr/bin/env bash
# Continues the measured sequence after Run 1 with a free-disk guard before each density run.
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
RUN1_PID=${RUN1_PID:?}
while kill -0 $RUN1_PID 2>/dev/null; do sleep 5; done
log "run1-1x exit (process ended; observed by run_rest.sh)"
du -sb $R/run1-1x/work > $R/run1-1x/work-du.txt 2>&1; host
rm -rf $R/run1-1x/work
run() { # name, min free GiB, extra args...
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
[ "${SKIP_D10:-0}" != 1 ] && run density-x10 8 --plan-preset density --density-multiplier-override 10
[ "${SKIP_D60:-0}" != 1 ] && run density-x60 18 --plan-preset density --density-multiplier-override 60
[ "${SKIP_REPEAT:-0}" != 1 ] && run repeat-scan-load 8 --plan $R/plan-repeat-scan-load.json
log "sequence done"
