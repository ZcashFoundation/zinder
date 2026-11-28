#!/usr/bin/env bash
# Exercises validate-cbb-interop-report.sh against CBB-shaped stub reports.
#
# The stubs mirror the JSON CBB serializes at the pinned revision. Each
# negative case matches one failure mode the parity smoke must catch, so the
# assertions cannot silently weaken into a check that accepts any report.

set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-cbb-interop-report.sh"
scratch_directory="$(mktemp -d)"
trap 'rm -rf "$scratch_directory"' EXIT

fail() {
  echo >&2 "CBB interoperability report validator test failed: $*"
  exit 1
}

report() {
  jq -n --arg method "$1" '["zinder", "reference"] | map({
    name: .,
    total_requests: 1,
    errors: 0,
    error_rate: 0,
    methods: [{method: $method, count: 1, errors: 0}],
    error_samples: [],
    wallets_configured: 1,
    wallets_connected: 1,
    wallet_connection_errors: 0,
    wallet_task_errors: 0,
    probe_error: null
  })' | jq "$2"
}

accept() {
  local label="$1" path="$2" method="$3"
  "${validator}" "${path}" "${method}" zinder reference >/dev/null ||
    fail "rejected ${label}"
}

reject() {
  local label="$1" path="$2" method="$3"
  if "${validator}" "${path}" "${method}" zinder reference >/dev/null 2>&1; then
    fail "accepted ${label}"
  fi
}

report GetBlockRange . >"${scratch_directory}/range.json"
accept "a single-client block-range report" "${scratch_directory}/range.json" GetBlockRange
report GetTreeState . >"${scratch_directory}/tree.json"
accept "a single-client tree report" "${scratch_directory}/tree.json" GetTreeState

expect_rejection() {
  local label="$1" method="$2" filter="$3"
  report "${method}" "${filter}" >"${scratch_directory}/case.json"
  reject "${label}" "${scratch_directory}/case.json" "${method}"
}

expect_rejection "a failed wallet connection" GetBlockRange \
  '.[0].wallets_connected = 0 | .[0].wallet_connection_errors = 1
   | .[0].total_requests = 0 | .[0].methods = []'
expect_rejection "a wallet task error" GetBlockRange \
  '.[1].wallet_task_errors = 1'
expect_rejection "a failed probe" GetBlockRange \
  '.[0].probe_error = "GetLightdInfo failed: Unavailable"'
expect_rejection "a missing probe result" GetBlockRange \
  'del(.[0].probe_error)'
expect_rejection "a second measured request" GetBlockRange \
  '.[0].total_requests = 2 | .[0].methods[0].count = 2'
expect_rejection "an RPC error" GetTreeState \
  '.[1].errors = 1 | .[1].error_rate = 1 | .[1].error_samples = [["timeout", 1]]
   | .[1].methods[0].errors = 1'
expect_rejection "the wrong method" GetBlockRange \
  '.[0].methods[0].method = "GetTreeState"'
expect_rejection "a second endpoint summary" GetBlockRange \
  '.[2] = .[0]'
expect_rejection "the wrong endpoint order" GetBlockRange \
  '.[0].name = "reference" | .[1].name = "zinder"'
expect_rejection "a summary object instead of an array" GetBlockRange \
  '.[0]'

if "${validator}" "${scratch_directory}/range.json" >/dev/null 2>&1; then
  fail "accepted an invocation without an expected method"
fi

echo "CBB interoperability report validator test passed"
