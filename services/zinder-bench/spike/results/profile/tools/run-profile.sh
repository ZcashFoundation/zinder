#!/usr/bin/env bash
# Record one spike replay under perf (Testnet).
# Usage: run-profile.sh MODE TAG [THROUGH_HEIGHT] [-- extra perf record args]
#   MODE: canonical-only | combined
# Default recording: cycles at 999 Hz, frame-pointer call graphs, monotonic
# clock. With extra args (for example probe events), they replace the
# default event list.
set -euo pipefail
P=/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit/.tmp/profile
B=$P/target/release/zinder-bench
FIX=$P/h0/fixture-4600001-4620122
MODE=${1:?mode}; TAG=${2:?tag}; shift 2
THROUGH=""
if [ "${1:-}" != "" ] && [ "${1:-}" != "--" ]; then THROUGH=$1; shift; fi
[ "${1:-}" = "--" ] && shift
EVENTS=("$@")
[ ${#EVENTS[@]} -eq 0 ] && EVENTS=(-e cycles -F 999)
OUT=$P/runs/$TAG
W=$P/work/$TAG
mkdir -p "$OUT" "$P/work"
free=$(df --output=avail -BG / | tail -1 | tr -dc 0-9)
if [ "$free" -lt 60 ]; then echo "abort: ${free} GB free" >&2; exit 3; fi

cat > "$OUT/replay.sh" <<EOF
#!/usr/bin/env bash
exec "$B" spike-combined-commit replay --fixture "$FIX" --golden "$P/h0/golden/$MODE.golden" \\
  --work "$W" --mode $MODE --progress-every 1 --no-perf-context \\
  --samples "$OUT/samples.jsonl" --report "$OUT/report.json" --label profile-$TAG \\
  ${THROUGH:+--through-height $THROUGH} 2> "$OUT/replay.log"
EOF
chmod +x "$OUT/replay.sh"
offset() { python3 -c 'import time; print(time.clock_gettime(time.CLOCK_REALTIME) - time.clock_gettime(time.CLOCK_MONOTONIC))'; }
offset > "$OUT/clock-offsets.txt"
echo "sudo -n perf record ${EVENTS[*]} --call-graph fp -k CLOCK_MONOTONIC -m 2048 -o $OUT/perf.data -- sudo -n -u ubuntu $OUT/replay.sh" > "$OUT/command.txt"
/usr/bin/time -v -o "$OUT/perf-record.time" sudo -n perf record "${EVENTS[@]}" --call-graph fp -k CLOCK_MONOTONIC \
  -m 2048 -o "$OUT/perf.data" -- sudo -n -u ubuntu "$OUT/replay.sh" 2> "$OUT/perf-record.log"
offset >> "$OUT/clock-offsets.txt"
sudo -n chown ubuntu:ubuntu "$OUT/perf.data"
du -sh "$W" > "$OUT/work-size.txt" 2>&1 || true
rm -rf "$W" "$W.control" "$W.secondary"
df -h / | tail -1 >> "$OUT/work-size.txt"
