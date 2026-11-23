#!/usr/bin/env bash
# Validates a Compact Block Bench (CBB) interoperability smoke report.
#
# CBB emits one summary per --endpoint, in argument order. The smoke runs one
# client per listener over the smallest deterministic post-activation workload,
# so every summary must show exactly one configured and connected wallet,
# exactly one successful call of the expected method, and no RPC, connection,
# or task error. A non-null `probe_error` is a failed GetLightdInfo probe; a
# tree-only run skips the probe and still serializes null.
#
# Usage:
#   validate-cbb-interop-report.sh REPORT.json EXPECTED_METHOD NAME...
#
# Exit codes:
#   0  the report satisfies the smoke contract
#   1  the report violates it
#   2  the invocation is unusable

set -euo pipefail

usage() {
  echo >&2 "usage: $0 REPORT.json EXPECTED_METHOD NAME..."
  exit 2
}

fail() {
  echo >&2 "CBB interoperability report is invalid: $*"
  exit 1
}

[[ "$#" -ge 3 ]] || usage
command -v jq >/dev/null 2>&1 || fail "jq is required"

report_path="$1"
expected_method="$2"
shift 2
[[ -f "$report_path" ]] || fail "report does not exist: $report_path"
[[ -s "$report_path" ]] || fail "report is empty: $report_path"

expected_names="$(printf '%s\n' "$@" | jq -R . | jq -s .)"

if ! jq -e --arg method "$expected_method" --argjson names "$expected_names" '
    def one_successful_call($summary; $name):
      ($summary.name == $name)
      and ($summary.wallets_configured == 1)
      and ($summary.wallets_connected == 1)
      and ($summary.wallet_connection_errors == 0)
      and ($summary.wallet_task_errors == 0)
      and ($summary | has("probe_error"))
      and ($summary.probe_error == null)
      and ($summary.total_requests == 1)
      and ($summary.errors == 0)
      and ($summary.error_rate == 0)
      and ($summary.error_samples == [])
      and ($summary.methods | length == 1)
      and ($summary.methods[0].method == $method)
      and ($summary.methods[0].count == 1)
      and ($summary.methods[0].errors == 0);
    (type == "array")
    and (length == ($names | length))
    and all(to_entries[]; one_successful_call(.value; $names[.key]))
  ' "$report_path" >/dev/null; then
  fail "${report_path} does not show exactly one successful ${expected_method} call, one connected wallet, and no errors for: $*"
fi
