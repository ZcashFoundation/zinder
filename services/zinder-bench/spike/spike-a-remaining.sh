#!/usr/bin/env bash
# Spike A: the stages after Arm 2, in order, one process at a time. Throwaway.
set -euo pipefail
WT=/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit
S=$WT/services/zinder-bench/spike/spike-a-final-run.sh
R=$WT/.tmp/spike-results
OUT=$R/reports
WORK=$R/work
LOGS=$R/logs
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOGS/timeline.txt"; }
free_gb() { df --output=avail -BG / | tail -1 | tr -dc 0-9; }
# The host disk is shared; stop instead of filling it.
need_free() {
  local want=$1
  if [ "$(free_gb)" -lt "$want" ] && [ -d "$WT/.tmp/spike-a/h0/wallet-h0.rocksdb" ]; then
    rm -rf "$WT/.tmp/spike-a/h0/wallet-h0.rocksdb" "$WT/.tmp/spike-a/h0/wallet-h0.rocksdb.build-lock"
    note "deleted H0 wallet store (already imported into the goldens) to free space"
  fi
  if [ "$(free_gb)" -lt "$want" ]; then
    note "stopping: $(free_gb) GB free, need $want GB"
    exit 3
  fi
}

# Arm 2 at T with density lanes (the replaced window 4619123..4620122 is sparse).
for K in 4 7; do
  [ -f "$OUT/arm2-density$K-combined.json" ] || "$S" arm2-density $K combined
done

# The Arm 1 stores and the reference wallet are no longer needed.
rm -rf "$WORK/arm1-canonical-only" "$WORK/arm1-canonical-only.secondary" "$WORK/arm1-canonical-only.control" \
  "$WORK/reference-wallet-T" "$WORK/reference-wallet-T.build-lock" "$WORK/reference-wallet-T.projection-load-staging" \
  "$WORK/dataprep-wallet-copy" "$WORK/arm2-canonical-only.secondary"
note "deleted arm1-canonical-only and reference-wallet-T stores"

# Arm 2 over the densest 1000-block Testnet window.
need_free 15
[ -f "$OUT/arm1-repeat-to-4618271-combined.json" ] || "$S" dense-prep
[ -f "$OUT/arm2-dense-x1-combined.json" ] || "$S" arm2-dense 1 5 --cold-readback
[ -f "$OUT/arm2-dense-x2-combined.json" ] || "$S" arm2-dense 2 5
[ -f "$OUT/arm2-dense-x4-combined.json" ] || "$S" arm2-dense 4 3
HELD=$(jq '[.replacements[].ingest_held_bytes]|max' "$OUT/arm2-dense-x1-combined.json")
CAP=$(( 2147483648 / HELD ))
[ "$CAP" -gt 60 ] && CAP=60
note "ingest cap multiplier $CAP from held bytes $HELD"
if [ "$CAP" -gt 4 ] && [ ! -f "$OUT/arm2-dense-x$CAP-combined.json" ]; then "$S" arm2-dense $CAP 2; fi
rm -rf "$WORK/arm1-combined" "$WORK/arm1-combined.secondary" "$WORK/arm1-combined.control" \
  "$WORK/arm2-combined.secondary" "$WORK/dense-combined" "$WORK/dense-combined.secondary" "$WORK/dense-combined.control"
note "deleted arm1-combined and dense-combined stores"

# Density arm (Arm 1 repeated with lanes clamped at the 2 MB consensus block size).
for K in 4 7; do
  need_free 25
  [ -f "$OUT/compare-density$K.json" ] || "$S" density $K
done

# Arm 3: about 10x background write volume in mirror families (11 lanes,
# estimated from the Arm 1 byte mix), then two sensitivity runs.
LANES=${LANES:-11}
need_free 30
[ -f "$OUT/compare-arm3.json" ] || "$S" arm3 $LANES
need_free 30
[ -f "$OUT/arm3-budget-canonical-combined.json" ] || "$S" arm3-one combined $LANES canonical arm3-budget-canonical
# Wallet-family debt only: lanes chosen so the wallet mirror volume is about
# 10x the whole commit volume, to see whether canonical commits stall on it.
WLANES=$(jq -r '((10 * .batch.total_bytes.total / .batch.wallet_bytes.total) | ceil)' "$OUT/arm1-combined.json")
note "wallet-debt lanes $WLANES"
need_free 30
[ -f "$OUT/arm3-wallet-debt-combined.json" ] || PFAM=wallet "$S" arm3-one combined $WLANES summed arm3-wallet-debt
note "remaining stages done"
