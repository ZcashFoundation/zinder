#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
observer="$repository_root/deploy/capture-container-resource-evidence.sh"
scratch_directory="$(mktemp -d)"
trap 'rm -rf "$scratch_directory"' EXIT

fail() {
  printf 'container resource evidence test failed: %s\n' "$*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"

fake_cgroup_path="$scratch_directory/cgroup-v2"
fake_cgroup_membership_path="$scratch_directory/cgroup-membership"
fake_storage_path="$scratch_directory/storage"
results_path="$scratch_directory/results"
ready_path="$scratch_directory/child-ready"
release_path="$scratch_directory/release-child"
child_started_at_path="$scratch_directory/child-started-at"
evidence_path="$results_path/rocksdb-client.resources.json"

mkdir -p "$fake_cgroup_path" "$fake_storage_path" "$results_path"
printf 'memory io\n' >"$fake_cgroup_path/cgroup.controllers"
printf '1024\n' >"$fake_cgroup_path/memory.current"
printf '2048\n' >"$fake_cgroup_path/memory.peak"
printf '0::/\n' >"$fake_cgroup_membership_path"

export TEST_CGROUP_PATH="$fake_cgroup_path"
export TEST_STORAGE_PATH="$fake_storage_path"
export TEST_READY_PATH="$ready_path"
export TEST_RELEASE_PATH="$release_path"
export TEST_CHILD_STARTED_AT_PATH="$child_started_at_path"

# The variables in the child program intentionally expand in the child shell.
# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="rocksdb-canonical-replay-storage-client" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-01" \
ZINDER_BENCH_RESOURCE_STORAGE_PATH="$fake_storage_path" \
ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS="0.02" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$fake_cgroup_path" \
ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH="$fake_cgroup_membership_path" \
ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE="true" \
  "$observer" /bin/sh -c '
    clock_reading="$(date -u "+%s %N" 2>/dev/null || true)"
    clock_seconds="${clock_reading% *}"
    clock_nanoseconds="${clock_reading#* }"
    case "$clock_seconds:$clock_nanoseconds" in
      *[!0-9:]* | :*) child_started_at_unix_millis="$(date -u "+%s")000" ;;
      *:[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])
        child_started_at_unix_millis="$(printf "%s%.3s" "$clock_seconds" "$clock_nanoseconds")"
        ;;
      *) child_started_at_unix_millis="$(date -u "+%s")000" ;;
    esac
    printf "%s\n" "$child_started_at_unix_millis" >"$TEST_CHILD_STARTED_AT_PATH"
    printf "4096\n" >"$TEST_CGROUP_PATH/memory.current"
    printf "8192\n" >"$TEST_CGROUP_PATH/memory.peak"
    dd if=/dev/zero of="$TEST_STORAGE_PATH/facts.bin" bs=4096 count=16 >/dev/null 2>&1
    : >"$TEST_READY_PATH"
    while [ ! -e "$TEST_RELEASE_PATH" ]; do
      sleep 0.01
    done
    exit 23
  ' &
observer_pid="$!"

for _ in $(seq 1 500); do
  if [ -e "$ready_path" ]; then
    break
  fi
  sleep 0.01
done
[ -e "$ready_path" ] || fail "fake child did not become ready"
[ ! -e "$evidence_path" ] || fail "evidence became visible before the child completed"

# Leave the high-water values visible across multiple observation intervals.
sleep 0.2
: >"$release_path"

set +e
wait "$observer_pid"
observer_exit_status="$?"
set -e
[ "$observer_exit_status" -eq 23 ] \
  || fail "observer did not preserve child exit status 23 (got $observer_exit_status)"

jq -e '
  .evidence_format_version == 1
  and .measurement_kind == "container-resource-observation"
  and .component_id == "rocksdb-canonical-replay-storage-client"
  and .trial_id == "trial-01"
  and .sample_interval_seconds == 0.02
  and .child_exit_status == 23
  and .peak_memory_bytes == 8192
  and .sampled_memory_current_peak_bytes == 4096
  and .sampled_storage_peak_bytes >= 65536
  and .sources.cgroup_namespace.support == "verified-private"
  and .sources.cgroup_namespace.kind == "proc-self-cgroup-v2"
  and .sources.memory_peak.support == "exact"
  and .sources.memory_current.support == "exact"
  and .sources.storage.support == "sampled"
  and (.started_at | test("Z$"))
  and (.completed_at | test("Z$"))
  and .completed_at_unix_millis >= .started_at_unix_millis
  and (.samples | length >= 2)
  and all(.samples[]; (.observed_at_unix_millis | type) == "number")
' "$evidence_path" >/dev/null || fail "resource evidence did not match its contract"

child_started_at_unix_millis="$(sed -n '1p' "$child_started_at_path")"
jq -e --argjson child_started_at_unix_millis "$child_started_at_unix_millis" '
  .samples[0].observed_at_unix_millis <= $child_started_at_unix_millis
' "$evidence_path" >/dev/null \
  || fail "first resource sample did not precede child launch"

unsupported_cgroup_path="$scratch_directory/unsupported-cgroup-v2"
unsupported_evidence_path="$results_path/unsupported.resources.json"
mkdir -p "$unsupported_cgroup_path"
printf 'io\n' >"$unsupported_cgroup_path/cgroup.controllers"

ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$unsupported_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="postgres-canonical-replay-storage-client" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-02" \
ZINDER_BENCH_RESOURCE_STORAGE_PATH="" \
ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS="0.01" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$unsupported_cgroup_path" \
  "$observer" /bin/sh -c 'exit 0'

jq -e '
  .child_exit_status == 0
  and .sample_interval_seconds == 0.01
  and .peak_memory_bytes == null
  and .sampled_memory_current_peak_bytes == null
  and .sampled_storage_peak_bytes == null
  and .sources.memory_peak.support == "unsupported"
  and .sources.memory_current.support == "unsupported"
  and .sources.storage.support == "unsupported"
  and all(.samples[]; .memory_current_bytes == null and .storage_bytes == null)
' "$unsupported_evidence_path" >/dev/null \
  || fail "unsupported-source evidence did not match its contract"

required_memory_evidence_path="$results_path/required-memory.resources.json"
required_memory_child_marker="$scratch_directory/required-memory-child"
required_memory_log="$scratch_directory/required-memory.log"
set +e
# The positional parameter intentionally expands in the child shell.
# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$required_memory_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="required-memory" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-required-memory" \
ZINDER_BENCH_RESOURCE_REQUIRE_EXACT_MEMORY="true" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$unsupported_cgroup_path" \
ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH="$fake_cgroup_membership_path" \
ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE="true" \
  "$observer" /bin/sh -c ': >"$1"' child "$required_memory_child_marker" \
  >"$required_memory_log" 2>&1
required_memory_exit_status="$?"
set -e
[ "$required_memory_exit_status" -eq 64 ] \
  || fail "missing required memory sources did not fail configuration"
[ ! -e "$required_memory_child_marker" ] \
  || fail "child ran before required memory sources were admitted"
[ ! -e "$required_memory_evidence_path" ] \
  || fail "failed resource preflight published evidence"
grep -Fq "required cgroup-v2 memory.current and memory.peak sources are unavailable" \
  "$required_memory_log" \
  || fail "missing-memory preflight did not explain the unavailable sources"

required_storage_evidence_path="$results_path/required-storage.resources.json"
required_storage_child_marker="$scratch_directory/required-storage-child"
required_storage_log="$scratch_directory/required-storage.log"
missing_storage_path="$scratch_directory/missing-storage"
set +e
# The positional parameter intentionally expands in the child shell.
# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$required_storage_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="required-storage" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-required-storage" \
ZINDER_BENCH_RESOURCE_STORAGE_PATH="$missing_storage_path" \
ZINDER_BENCH_RESOURCE_REQUIRE_EXACT_MEMORY="true" \
ZINDER_BENCH_RESOURCE_REQUIRE_SAMPLED_STORAGE="true" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$fake_cgroup_path" \
ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH="$fake_cgroup_membership_path" \
ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE="true" \
  "$observer" /bin/sh -c ': >"$1"' child "$required_storage_child_marker" \
  >"$required_storage_log" 2>&1
required_storage_exit_status="$?"
set -e
[ "$required_storage_exit_status" -eq 64 ] \
  || fail "missing required storage source did not fail configuration"
[ ! -e "$required_storage_child_marker" ] \
  || fail "child ran before required storage was admitted"
[ ! -e "$required_storage_evidence_path" ] \
  || fail "failed storage preflight published evidence"
grep -Fq "required storage path cannot be sampled" "$required_storage_log" \
  || fail "missing-storage preflight did not explain the unavailable source"

host_cgroup_membership_path="$scratch_directory/host-cgroup-membership"
printf '0::/system.slice/docker-host.scope\n' >"$host_cgroup_membership_path"
required_namespace_evidence_path="$results_path/required-namespace.resources.json"
required_namespace_child_marker="$scratch_directory/required-namespace-child"
required_namespace_log="$scratch_directory/required-namespace.log"
set +e
# The positional parameter intentionally expands in the child shell.
# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$required_namespace_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="required-namespace" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-required-namespace" \
ZINDER_BENCH_RESOURCE_REQUIRE_EXACT_MEMORY="true" \
ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE="true" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$fake_cgroup_path" \
ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH="$host_cgroup_membership_path" \
  "$observer" /bin/sh -c ': >"$1"' child "$required_namespace_child_marker" \
  >"$required_namespace_log" 2>&1
required_namespace_exit_status="$?"
set -e
[ "$required_namespace_exit_status" -eq 64 ] \
  || fail "host cgroup namespace did not fail configuration"
[ ! -e "$required_namespace_child_marker" ] \
  || fail "child ran before private cgroup namespace was admitted"
[ ! -e "$required_namespace_evidence_path" ] \
  || fail "failed cgroup-namespace preflight published evidence"
grep -Fq "required private cgroup-v2 namespace is unavailable" "$required_namespace_log" \
  || fail "cgroup-namespace preflight did not explain the invalid scope"

race_evidence_path="$results_path/exclusive.resources.json"
race_release_path="$scratch_directory/release-race"
race_a_ready_path="$scratch_directory/race-a-ready"
race_b_ready_path="$scratch_directory/race-b-ready"

# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$race_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="race-a" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-race" \
ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS="0.01" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$unsupported_cgroup_path" \
  "$observer" /bin/sh -c '
    : >"$1"
    while [ ! -e "$2" ]; do sleep 0.01; done
  ' child "$race_a_ready_path" "$race_release_path" \
  >"$scratch_directory/race-a.log" 2>&1 &
race_a_pid="$!"

# shellcheck disable=SC2016
ZINDER_BENCH_RESOURCE_EVIDENCE_PATH="$race_evidence_path" \
ZINDER_BENCH_RESOURCE_COMPONENT_ID="race-b" \
ZINDER_BENCH_RESOURCE_TRIAL_ID="trial-race" \
ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS="0.01" \
ZINDER_BENCH_RESOURCE_CGROUP_PATH="$unsupported_cgroup_path" \
  "$observer" /bin/sh -c '
    : >"$1"
    while [ ! -e "$2" ]; do sleep 0.01; done
  ' child "$race_b_ready_path" "$race_release_path" \
  >"$scratch_directory/race-b.log" 2>&1 &
race_b_pid="$!"

for _ in $(seq 1 500); do
  if [ -e "$race_a_ready_path" ] && [ -e "$race_b_ready_path" ]; then
    break
  fi
  sleep 0.01
done
[ -e "$race_a_ready_path" ] && [ -e "$race_b_ready_path" ] \
  || fail "exclusive-publication children did not become ready"
: >"$race_release_path"

set +e
wait "$race_a_pid"
race_a_exit_status="$?"
wait "$race_b_pid"
race_b_exit_status="$?"
set -e

if ! { [ "$race_a_exit_status" -eq 0 ] && [ "$race_b_exit_status" -eq 74 ]; } \
  && ! { [ "$race_a_exit_status" -eq 74 ] && [ "$race_b_exit_status" -eq 0 ]; }; then
  fail "exclusive publication did not select one writer (got $race_a_exit_status and $race_b_exit_status)"
fi
jq -e '
  .trial_id == "trial-race"
  and (.component_id == "race-a" or .component_id == "race-b")
' "$race_evidence_path" >/dev/null \
  || fail "exclusive publication did not leave one complete evidence artifact"

passthrough_marker="$scratch_directory/passthrough-marker"
set +e
(
  unset ZINDER_BENCH_RESOURCE_EVIDENCE_PATH
  # shellcheck disable=SC2016
  "$observer" /bin/sh -c 'printf "passed\n" >"$1"; exit 17' child "$passthrough_marker"
)
passthrough_exit_status="$?"
set -e
[ "$passthrough_exit_status" -eq 17 ] \
  || fail "transparent mode did not preserve child exit status 17"
[ "$(sed -n '1p' "$passthrough_marker")" = "passed" ] \
  || fail "transparent mode did not execute the child command"

if find "$results_path" -type f \( -name '.*.tmp.*' -o -name '.*.samples.*' \) \
  | grep -q .; then
  fail "temporary evidence files remained after atomic publication"
fi

printf 'container resource evidence test passed\n'
