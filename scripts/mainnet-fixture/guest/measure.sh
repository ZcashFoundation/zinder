#!/usr/bin/env bash
# measure.sh <label> <outdir> [systemd-run -p props...] -- <command...>
# Runs the command as a transient systemd service (accounting on), records
# rusage, cgroup CPU/IO/memory, and per-device bytes from /proc/diskstats.
set -euo pipefail
label=$1 out=$2; shift 2
props=(); while [[ $1 != -- ]]; do props+=("$1"); shift; done; shift
mkdir -p "$out"; chmod 0777 "$out"
snap() { awk '$3 ~ /^nvme[0-9]+n[0-9]+$/ {print $3, $6*512, $10*512}' /proc/diskstats | sort; }
snap > "$out/$label.disk0"
unit="zfx-m-$label"
set +e
# systemd-run prints the unit's CPU time, memory peak, and IO bytes on exit.
systemd-run --pipe --wait --unit "$unit" -p IOAccounting=yes -p MemoryAccounting=yes \
  -p CPUAccounting=yes "${props[@]}" /usr/bin/python3 /opt/zfx/rusage.py "$out/$label.rusage.json" -- "$@" \
  > "$out/$label.stdout" 2> "$out/$label.stderr"
rc=$?
set -e
snap > "$out/$label.disk1"
grep -E '^(Finished with result|Main processes terminated|Service runtime|CPU time consumed|Memory peak|IO bytes)' "$out/$label.stderr" \
  | sed 's/: /=/' > "$out/$label.cgroup" || true
systemctl reset-failed "$unit" 2>/dev/null || true
disk=$(join "$out/$label.disk0" "$out/$label.disk1" | awk 'BEGIN{printf "["} {printf "%s{\"device\":\"%s\",\"read_bytes\":%d,\"written_bytes\":%d}", (NR>1?",":""), $1, $4-$2, $5-$3} END{printf "]"}')
cg=$(awk -F= '{gsub(/"/,"",$2); printf "%s\"%s\":\"%s\"", (NR>1?",":""), $1, $2} BEGIN{printf "{"} END{printf "}"}' "$out/$label.cgroup")
ru=$(cat "$out/$label.rusage.json" 2>/dev/null || echo null)
jq -n --arg label "$label" --argjson rc $rc --argjson ru "$ru" --argjson disk "$disk" --argjson cg "$cg" \
  '{label:$label, exit_code:$rc, rusage:$ru, cgroup:$cg, device_io:$disk}' > "$out/$label.json"
cat "$out/$label.json"
exit $rc
