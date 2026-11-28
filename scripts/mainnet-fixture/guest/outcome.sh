# shellcheck shell=bash disable=SC2154
# outcome.sh: sourced by restore-guest.sh. Records an attempt's outcome once.
#
# The caller sets out (the attempt directory), attempt, step, finished=false,
# and serving_started=false, defines log, and installs `trap on_exit EXIT`.
# outcome.json is written atomically: {"attempt", "status": "succeeded" |
# "failed", "step", "reason", "receipt"}.

write_outcome() {
  python3 -I - "$out" "$attempt" "$1" "$2" "$3" "$4" <<'EOF'
import json, os, sys
out, attempt, status, step, reason, receipt_path = sys.argv[1:]
receipt = None
if receipt_path and os.path.exists(receipt_path):
    try:
        receipt = json.load(open(receipt_path))
    except ValueError:
        receipt = None
outcome = {'attempt': attempt, 'status': status, 'step': step or None, 'reason': reason or None, 'receipt': receipt}
partial = os.path.join(out, 'outcome.json.partial')
with open(partial, 'w') as f:
    json.dump(outcome, f)
    f.write('\n')
    f.flush()
    os.fsync(f.fileno())
os.replace(partial, os.path.join(out, 'outcome.json'))
EOF
}

# Runs on every exit that did not record success. A host that failed its
# checks, including a failed write of the success outcome, must not keep
# serving wallets.
on_exit() {
  local code=$?
  [[ $finished == true ]] && return
  if [[ $serving_started == true ]]; then systemctl stop zfx-fixture-compat 2>/dev/null || true; fi
  write_outcome failed "$step" "$(tail -n 1 "$out/guest.log" 2>/dev/null | cut -d' ' -f2-) (exit $code)" "$out/receipt.json.partial" || true
}

# Success counts only once its outcome is on disk.
finish_success() {
  write_outcome succeeded "" "" "$1"
  finished=true
}
