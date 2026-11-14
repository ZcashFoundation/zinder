#!/usr/bin/env bash
# Spike A measured run (Testnet). Throwaway.
#
# Inputs already prepared in this worktree:
#   H0 canonical : $H/canonical-h0.rocksdb     (zinder-ingest --wallet-serving --target-height 4570122)
#   H0 wallet    : $H/wallet-h0.rocksdb        (zinder-bench rocksdb-wallet-rebuild over the H0 canonical)
#   Fixture      : $H/fixture-4570123-4620122  (zinder-bench capture, blocks H0+1..T, T = 4620122)
#   Goldens      : $H/golden/{canonical-only,combined}.golden (spike-combined-commit prepare)
#
# Every arm copies a golden store (SSTs hard-linked) into $WORK and never
# writes the golden. Each command writes a JSON report that refuses to
# overwrite; delete a report to rerun its arm.
set -euo pipefail

WT=/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit
B=$WT/.tmp/spike-a/bin/zinder-bench
H=$WT/.tmp/spike-a/h0
FIX=$H/fixture-4570123-4620122
G=$H/golden
OUT=${OUT:-$WT/.tmp/spike-a/measured}
WORK=$OUT/work
COOKIE=/home/ubuntu/.zakura/testnet/cookie/.cookie
DATAPREP=/home/ubuntu/.t3/worktrees/zinder/t3-c67a34ca-dd54fadf1533b1ea/.tmp/spike-data
SPIKE="$B spike-combined-commit"
mkdir -p "$OUT/logs" "$WORK"

golden() { if [ "$1" = combined ]; then echo "$G/combined.golden"; else echo "$G/canonical-only.golden"; fi; }

# Arm 1: one synced commit per block, H0+1..T, with a secondary in its own
# process catching up every second (reader behavior), then once after the replay.
for MODE in canonical-only combined; do
  EXTRA=""; [ "$MODE" = combined ] && EXTRA="--cold-readback"
  $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" --work "$WORK/arm1-$MODE" --mode $MODE \
    --secondary-dir "$WORK/arm1-$MODE.secondary" --secondary-poll-ms 1000 \
    --samples "$OUT/arm1-$MODE.samples.jsonl" --report "$OUT/arm1-$MODE.json" \
    --label arm1 $EXTRA 2> "$OUT/logs/arm1-$MODE.log"
done
$SPIKE compare --baseline "$OUT/arm1-canonical-only.json" --combined "$OUT/arm1-combined.json" \
  --report "$OUT/compare-arm1.json"

# Correctness reference 1: the projector's cold build (build_wallet_from_canonical)
# over the canonical-only replay result at T.
$B rocksdb-wallet-rebuild --network zcash-testnet --json-rpc-addr http://127.0.0.1:49232 \
  --node-auth-cookie "$COOKIE" --canonical-store "$WORK/arm1-canonical-only" \
  --wallet-store "$WORK/reference-wallet-T" > "$OUT/reference-wallet-T.json" 2> "$OUT/logs/reference-wallet-T.log"
$SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" --reference-wallet "$WORK/reference-wallet-T" \
  --baseline "$WORK/arm1-canonical-only" --report "$OUT/verify-arm1.json" 2> "$OUT/logs/verify-arm1.log"
# Correctness reference 2: data prep's wallet at T (projector cold build plus
# projector following), opened only through a hard-linked copy.
$SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" --reference-wallet "$DATAPREP/wallet.rocksdb" \
  --reference-copy "$WORK/dataprep-wallet-copy" --report "$OUT/verify-arm1-dataprep.json" \
  2> "$OUT/logs/verify-arm1-dataprep.log"

# Arm 2: five 1000-block replacements at T through each path, in place on the
# Arm 1 stores, with a secondary process timed after each batch.
for MODE in canonical-only combined; do
  $SPIKE reorg --fixture "$FIX" --store "$WORK/arm1-$MODE" --mode $MODE --count 5 --depth 1000 \
    --secondary-dir "$WORK/arm2-$MODE.secondary" --report "$OUT/arm2-$MODE.json" --label arm2 \
    2> "$OUT/logs/arm2-$MODE.log"
done
$SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" --reference-wallet "$WORK/reference-wallet-T" \
  --baseline "$WORK/arm1-canonical-only" --report "$OUT/verify-arm2.json" 2> "$OUT/logs/verify-arm2.log"

# Arm 3: Arm 1 plus a background writer adding mirror lanes of every committed
# block (unsynced, WAL on). Check pressure_to_commit_bytes_ratio in the report
# and raise --pressure-lanes until it is about 9 (total volume about 10x).
for MODE in canonical-only combined; do
  $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" --work "$WORK/arm3-$MODE" --mode $MODE \
    --pressure-lanes ${PRESSURE_LANES:-9} --samples "$OUT/arm3-$MODE.samples.jsonl" \
    --report "$OUT/arm3-$MODE.json" --label arm3 2> "$OUT/logs/arm3-$MODE.log"
  rm -rf "$WORK/arm3-$MODE"
done

# Density arm: every block's canonical rows and wallet row operations (puts,
# deletes, planner point reads, digest folds) repeated in mirror families inside
# the same synced batch. 4 ~ Q3 2026 Mainnet mean transactions per block over
# this Testnet range's mean; 7 ~ the September 2026 Mainnet daily peak.
for K in ${DENSITY:-4 7}; do
  for MODE in canonical-only combined; do
    $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" --work "$WORK/density$K-$MODE" --mode $MODE \
      --density-multiplier $K --samples "$OUT/density$K-$MODE.samples.jsonl" \
      --report "$OUT/density$K-$MODE.json" --label density-x$K 2> "$OUT/logs/density$K-$MODE.log"
    rm -rf "$WORK/density$K-$MODE"
  done
  $SPIKE compare --baseline "$OUT/density$K-canonical-only.json" --combined "$OUT/density$K-combined.json" \
    --report "$OUT/compare-density$K.json"
done
du -sh "$WORK" "$OUT"
