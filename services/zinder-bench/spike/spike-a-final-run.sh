#!/usr/bin/env bash
# Spike A final measured run (Testnet). Throwaway.
#
# Usage: spike-a-final-run.sh <stage> [args]
#   arm1                 Arm 1 in both modes, then compare
#   reference            reference wallet at T (projector cold build) and both verifies
#   arm2                 Arm 2 (5 x 1000) in both modes in place, then verify
#   arm2-density K MODE  Arm 2 at density K on a hard-linked copy of the Arm 1 store
#   density K            Arm 1 at density K in both modes, then compare
#   arm3 LANES [PROFILE] Arm 3 with LANES pressure lanes in both modes (PROFILE: summed|canonical)
#   arm3-one MODE LANES PROFILE TAG
#
# Inputs (prepared earlier in this worktree, never written):
#   H0 canonical, H0 wallet, fixture H0+1..T, goldens under .tmp/spike-a/h0.
# Every report refuses to overwrite.
set -euo pipefail

WT=/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit
R=$WT/.tmp/spike-results
B=$R/bin/zinder-bench
H=$WT/.tmp/spike-a/h0
FIX=$H/fixture-4570123-4620122
G=$H/golden
OUT=$R/reports
LOGS=$R/logs
WORK=$R/work
COOKIE=/home/ubuntu/.zakura/testnet/cookie/.cookie
DATAPREP=/home/ubuntu/.t3/worktrees/zinder/t3-c67a34ca-dd54fadf1533b1ea/.tmp/spike-data
SPIKE="$B spike-combined-commit"
mkdir -p "$OUT" "$LOGS" "$WORK"

golden() { if [ "$1" = combined ]; then echo "$G/combined.golden"; else echo "$G/canonical-only.golden"; fi; }
stamp() { date -u +%FT%TZ; }
run() { # name, command...
  local name=$1; shift
  echo "$(stamp) start $name" >> "$LOGS/timeline.txt"
  /usr/bin/time -v -o "$LOGS/$name.time" "$@" 2> "$LOGS/$name.log"
  echo "$(stamp) end $name" >> "$LOGS/timeline.txt"
}

stage=${1:?stage}; shift || true
case "$stage" in
arm1)
  for MODE in canonical-only combined; do
    EXTRA=""; [ "$MODE" = combined ] && EXTRA="--cold-readback"
    run arm1-$MODE $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" --work "$WORK/arm1-$MODE" \
      --mode $MODE --secondary-dir "$WORK/arm1-$MODE.secondary" --secondary-poll-ms 1000 \
      --samples "$OUT/arm1-$MODE.samples.jsonl" --report "$OUT/arm1-$MODE.json" --label arm1 $EXTRA
  done
  run compare-arm1 $SPIKE compare --baseline "$OUT/arm1-canonical-only.json" \
    --combined "$OUT/arm1-combined.json" --report "$OUT/compare-arm1.json"
  ;;
reference)
  # Reference 1: build_wallet_from_canonical over the canonical-only replay result at T.
  echo "$(stamp) start reference-wallet-T" >> "$LOGS/timeline.txt"
  /usr/bin/time -v -o "$LOGS/reference-wallet-T.time" $B rocksdb-wallet-rebuild --network zcash-testnet \
    --json-rpc-addr http://127.0.0.1:49232 --node-auth-cookie "$COOKIE" \
    --canonical-store "$WORK/arm1-canonical-only" --wallet-store "$WORK/reference-wallet-T" \
    > "$OUT/reference-wallet-T.json" 2> "$LOGS/reference-wallet-T.log"
  echo "$(stamp) end reference-wallet-T" >> "$LOGS/timeline.txt"
  run verify-arm1 $SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" \
    --reference-wallet "$WORK/reference-wallet-T" --baseline "$WORK/arm1-canonical-only" \
    --report "$OUT/verify-arm1.json"
  # Reference 2: data prep's projector-built and projector-followed wallet at T,
  # opened only through a hard-linked copy.
  run verify-arm1-dataprep $SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" \
    --reference-wallet "$DATAPREP/wallet.rocksdb" --reference-copy "$WORK/dataprep-wallet-copy" \
    --report "$OUT/verify-arm1-dataprep.json"
  ;;
arm2)
  for MODE in canonical-only combined; do
    run arm2-$MODE $SPIKE reorg --fixture "$FIX" --store "$WORK/arm1-$MODE" --mode $MODE --count 5 \
      --depth 1000 --secondary-dir "$WORK/arm2-$MODE.secondary" --report "$OUT/arm2-$MODE.json" --label arm2
  done
  run verify-arm2 $SPIKE verify --fixture "$FIX" --combined "$WORK/arm1-combined" \
    --reference-wallet "$WORK/reference-wallet-T" --baseline "$WORK/arm1-canonical-only" \
    --report "$OUT/verify-arm2.json"
  ;;
arm2-density)
  K=${1:?multiplier}; MODE=${2:?mode}
  run arm2-density$K-$MODE $SPIKE reorg --fixture "$FIX" --golden "$WORK/arm1-$MODE" \
    --work "$WORK/arm2-density$K-$MODE" --mode $MODE --count 5 --depth 1000 --density-multiplier $K \
    --secondary-dir "$WORK/arm2-density$K-$MODE.secondary" --report "$OUT/arm2-density$K-$MODE.json" \
    --label arm2-density-x$K
  rm -rf "$WORK/arm2-density$K-$MODE" "$WORK/arm2-density$K-$MODE.secondary"
  ;;
dense-prep)
  # Combined store whose newest 1000 blocks are the densest 1000-block Testnet
  # window of the fixture (4617272..4618271, 197.7 MB of raw blocks). The
  # replay doubles as a repeat of Arm 1 combined over 48,149 blocks.
  run dense-prep-combined $SPIKE replay --fixture "$FIX" --golden "$(golden combined)" \
    --work "$WORK/dense-combined" --mode combined --through-height 4618271 \
    --secondary-dir "$WORK/dense-combined.secondary" --secondary-poll-ms 1000 \
    --samples "$OUT/arm1-repeat-to-4618271-combined.samples.jsonl" \
    --report "$OUT/arm1-repeat-to-4618271-combined.json" --label arm1-repeat-partial
  ;;
arm2-dense)
  K=${1:?multiplier}; COUNT=${2:-5}; shift 2 || true
  run arm2-dense-x$K $SPIKE reorg --fixture "$FIX" --golden "$WORK/dense-combined" \
    --work "$WORK/arm2-dense-x$K" --mode combined --count $COUNT --depth 1000 --density-multiplier $K \
    --secondary-dir "$WORK/arm2-dense-x$K.secondary" --report "$OUT/arm2-dense-x$K-combined.json" \
    --label arm2-dense-x$K "$@"
  rm -rf "$WORK/arm2-dense-x$K" "$WORK/arm2-dense-x$K.secondary"
  ;;
density)
  # Lanes per block are clamped so no scaled block exceeds the 2,000,000-byte
  # consensus maximum (CLAMP=none disables the clamp; TAG names the reports).
  K=${1:?multiplier}; CLAMP=${CLAMP:-2000000}; TAG=${TAG:-density$K}
  CLAMP_ARGS=""; [ "$CLAMP" != none ] && CLAMP_ARGS="--density-max-block-bytes $CLAMP"
  MODES=${MODES:-canonical-only combined}
  for MODE in $MODES; do
    run $TAG-$MODE $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" \
      --work "$WORK/$TAG-$MODE" --mode $MODE --density-multiplier $K $CLAMP_ARGS \
      --secondary-dir "$WORK/$TAG-$MODE.secondary" --secondary-poll-ms 1000 \
      --samples "$OUT/$TAG-$MODE.samples.jsonl" --report "$OUT/$TAG-$MODE.json" \
      --label $TAG
    rm -rf "$WORK/$TAG-$MODE" "$WORK/$TAG-$MODE.secondary" "$WORK/$TAG-$MODE.control"
  done
  if [ -f "$OUT/$TAG-canonical-only.json" ] && [ -f "$OUT/$TAG-combined.json" ]; then
    run compare-$TAG $SPIKE compare --baseline "$OUT/$TAG-canonical-only.json" \
      --combined "$OUT/$TAG-combined.json" --report "$OUT/compare-$TAG.json"
  fi
  ;;
arm3-one)
  MODE=${1:?mode}; LANES=${2:?lanes}; PROFILE=${3:-summed}; TAG=${4:-arm3}
  run $TAG-$MODE $SPIKE replay --fixture "$FIX" --golden "$(golden $MODE)" --work "$WORK/$TAG-$MODE" \
    --mode $MODE --pressure-lanes $LANES --budget-profile $PROFILE --pressure-families ${PFAM:-all} \
    --secondary-dir "$WORK/$TAG-$MODE.secondary" --secondary-poll-ms 1000 \
    --samples "$OUT/$TAG-$MODE.samples.jsonl" --report "$OUT/$TAG-$MODE.json" --label $TAG
  rm -rf "$WORK/$TAG-$MODE" "$WORK/$TAG-$MODE.secondary"
  ;;
arm3)
  LANES=${1:?lanes}
  for MODE in canonical-only combined; do "$0" arm3-one $MODE $LANES summed arm3; done
  run compare-arm3 $SPIKE compare --baseline "$OUT/arm3-canonical-only.json" \
    --combined "$OUT/arm3-combined.json" --report "$OUT/compare-arm3.json"
  ;;
*) echo "unknown stage $stage" >&2; exit 2 ;;
esac
du -sh "$WORK" >> "$LOGS/disk.txt" 2>&1 || true
df -h / | tail -1 >> "$LOGS/disk.txt"
