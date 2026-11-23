#!/usr/bin/env bash
# measure.sh <label> <outdir> [systemd-run -p props...] -- <command...>
# Runs the command as a transient systemd service (accounting on), records
# rusage, cgroup CPU/IO/memory, and per-device bytes from /proc/diskstats.
set -euo pipefail
label=$1 out=$2; shift 2
props=(); while [[ $1 != -- ]]; do props+=("$1"); shift; done; shift
# Root writes the receipts here while the measured command may run as a
# service user that writes only its rusage file, so the directory is root-owned
# and that one file is created for the user in advance.
install -d -m 0755 "$out"
for f in "$out/$label".{disk0,disk1,stdout,stderr,cgroup,json,rusage.json}; do
  [[ ! -L $f ]] || { echo "refusing symlink $f" >&2; exit 1; }
done
run_user=$(printf '%s\n' "${props[@]}" | sed -n 's/^User=//p' | tail -1)
owner=root
if [[ -n $run_user ]] && id -u "$run_user" >/dev/null 2>&1; then owner=$run_user; fi
install -m 0644 -o "$owner" /dev/null "$out/$label.rusage.json"
snap() { awk '$3 ~ /^nvme[0-9]+n[0-9]+$/ {print $3, $6*512, $10*512}' /proc/diskstats | sort; }
snap > "$out/$label.disk0"
unit="zfx-m-$label"
set +e
# systemd-run prints the unit's CPU time, memory peak, and IO bytes on exit.
systemd-run --pipe --wait --unit "$unit" -p IOAccounting=yes -p MemoryAccounting=yes \
  -p CPUAccounting=yes "${props[@]}" /usr/bin/python3 "${ZFX_RUSAGE:-/opt/zfx/rusage.py}" "$out/$label.rusage.json" -- "$@" \
  > "$out/$label.stdout" 2> "$out/$label.stderr"
rc=$?
set -e
snap > "$out/$label.disk1"
# systemd versions differ in indentation and wording (IO bytes / IO Bytes).
sed -nE 's/^[[:space:]]*(Finished with result|Main processes terminated with|Service runtime|CPU time consumed|Memory peak|IO [Bb]ytes( read| written)?): (.*)$/\1\t\3/p' \
  "$out/$label.stderr" > "$out/$label.cgroup" || true
systemctl reset-failed "$unit" 2>/dev/null || true
disk=$(join "$out/$label.disk0" "$out/$label.disk1" | awk 'BEGIN{printf "["} {printf "%s{\"device\":\"%s\",\"read_bytes\":%d,\"written_bytes\":%d}", (NR>1?",":""), $1, $4-$2, $5-$3} END{printf "]"}')
cg=$(jq -Rn '[inputs | split("\t") | {(.[0]): .[1]}] | add // {}' "$out/$label.cgroup" 2>/dev/null || echo '{}')
# The service can fail before rusage.py reports (a bad User, say), leaving the
# pre-created file empty; record null then, and always exit with systemd's status.
ru=null
if [[ -s $out/$label.rusage.json ]] && jq -e . "$out/$label.rusage.json" >/dev/null 2>&1; then
  ru=$(cat "$out/$label.rusage.json")
fi
jq -n --arg label "$label" --argjson rc "$rc" --argjson ru "$ru" --argjson disk "$disk" --argjson cg "$cg" \
  '{label:$label, exit_code:$rc, rusage:$ru, cgroup:$cg, device_io:$disk}' > "$out/$label.json" \
  || jq -n --arg label "$label" --argjson rc "$rc" '{label:$label, exit_code:$rc, rusage:null}' > "$out/$label.json"
cat "$out/$label.json"
exit "$rc"
