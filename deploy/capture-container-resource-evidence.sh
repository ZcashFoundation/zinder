#!/bin/sh
set -u

EVIDENCE_FORMAT_VERSION=1

evidence_path="${ZINDER_BENCH_RESOURCE_EVIDENCE_PATH:-}"

# This image entrypoint remains transparent outside measured benchmark runs.
if [ -z "$evidence_path" ]; then
  exec "$@"
fi

fail_configuration() {
  printf 'container resource evidence configuration error: %s\n' "$*" >&2
  exit 64
}

if [ "$#" -eq 0 ]; then
  fail_configuration "a child command is required"
fi

component_id="${ZINDER_BENCH_RESOURCE_COMPONENT_ID:-}"
trial_id="${ZINDER_BENCH_RESOURCE_TRIAL_ID:-}"
storage_path="${ZINDER_BENCH_RESOURCE_STORAGE_PATH:-}"
sample_interval_seconds="${ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS:-1}"
cgroup_path="${ZINDER_BENCH_RESOURCE_CGROUP_PATH:-/sys/fs/cgroup}"
cgroup_membership_path="${ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH:-/proc/self/cgroup}"
require_exact_memory="${ZINDER_BENCH_RESOURCE_REQUIRE_EXACT_MEMORY:-false}"
require_sampled_storage="${ZINDER_BENCH_RESOURCE_REQUIRE_SAMPLED_STORAGE:-false}"
require_private_cgroup_namespace="${ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE:-false}"

case "$component_id" in
  '' | *[!A-Za-z0-9._-]*)
    fail_configuration "ZINDER_BENCH_RESOURCE_COMPONENT_ID must be a non-empty evidence identifier"
    ;;
esac

case "$trial_id" in
  '' | *[!A-Za-z0-9._-]*)
    fail_configuration "ZINDER_BENCH_RESOURCE_TRIAL_ID must be a non-empty evidence identifier"
    ;;
esac

if ! LC_ALL=C awk -v interval="$sample_interval_seconds" 'BEGIN {
  exit !(interval ~ /^(0|[1-9][0-9]*)([.][0-9]+)?$/ && interval > 0)
}'; then
  fail_configuration "ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS must be greater than zero"
fi

case "$require_exact_memory" in
  true | false) ;;
  *)
    fail_configuration "ZINDER_BENCH_RESOURCE_REQUIRE_EXACT_MEMORY must be true or false"
    ;;
esac

case "$require_sampled_storage" in
  true | false) ;;
  *)
    fail_configuration "ZINDER_BENCH_RESOURCE_REQUIRE_SAMPLED_STORAGE must be true or false"
    ;;
esac

case "$require_private_cgroup_namespace" in
  true | false) ;;
  *)
    fail_configuration "ZINDER_BENCH_RESOURCE_REQUIRE_PRIVATE_CGROUP_NAMESPACE must be true or false"
    ;;
esac

contains_control_character() {
  LC_ALL=C printf '%s' "$1" | LC_ALL=C grep -q '[[:cntrl:]]'
}

if contains_control_character "$evidence_path"; then
  fail_configuration "ZINDER_BENCH_RESOURCE_EVIDENCE_PATH must not contain control characters"
fi
if contains_control_character "$storage_path"; then
  fail_configuration "ZINDER_BENCH_RESOURCE_STORAGE_PATH must not contain control characters"
fi
if contains_control_character "$cgroup_path"; then
  fail_configuration "ZINDER_BENCH_RESOURCE_CGROUP_PATH must not contain control characters"
fi
if contains_control_character "$cgroup_membership_path"; then
  fail_configuration "ZINDER_BENCH_RESOURCE_CGROUP_MEMBERSHIP_PATH must not contain control characters"
fi

evidence_directory="$(dirname "$evidence_path")"
evidence_basename="$(basename "$evidence_path")"

if [ ! -d "$evidence_directory" ]; then
  fail_configuration "evidence directory does not exist: $evidence_directory"
fi
if [ ! -w "$evidence_directory" ]; then
  fail_configuration "evidence directory is not writable: $evidence_directory"
fi
if [ -e "$evidence_path" ]; then
  fail_configuration "evidence path already exists: $evidence_path"
fi

samples_path="$(mktemp "$evidence_directory/.${evidence_basename}.samples.XXXXXX")" \
  || fail_configuration "could not create the resource sample file"
evidence_temporary_path="$(mktemp "$evidence_directory/.${evidence_basename}.tmp.XXXXXX")" || {
  rm -f "$samples_path"
  fail_configuration "could not create the temporary evidence file"
}

child_pid=""
sleep_pid=""

# ShellCheck cannot see function references embedded in trap actions.
# shellcheck disable=SC2329
cleanup_temporary_files() {
  rm -f "$samples_path" "$evidence_temporary_path"
}

# shellcheck disable=SC2329
forward_signal() {
  signal_name="$1"
  if [ -n "$child_pid" ]; then
    kill -s "$signal_name" "$child_pid" 2>/dev/null || true
  fi
  if [ -n "$sleep_pid" ]; then
    kill -s TERM "$sleep_pid" 2>/dev/null || true
  fi
}

trap cleanup_temporary_files EXIT
trap 'forward_signal HUP' HUP
trap 'forward_signal INT' INT
trap 'forward_signal QUIT' QUIT
trap 'forward_signal TERM' TERM

timestamp_rfc3339() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

# Truncate a padded %N in the shell: some date implementations drop the leading
# zeros of the nanoseconds before applying a %3N precision.
timestamp_unix_millis() {
  clock_reading="$(date -u '+%s %N' 2>/dev/null || true)"
  clock_seconds="${clock_reading% *}"
  clock_nanoseconds="${clock_reading#* }"
  case "$clock_seconds:$clock_nanoseconds" in
    *[!0-9:]* | :*)
      printf '%s000\n' "$(date -u '+%s')"
      ;;
    *:[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])
      printf '%s%.3s\n' "$clock_seconds" "$clock_nanoseconds"
      ;;
    *)
      printf '%s000\n' "$(date -u '+%s')"
      ;;
  esac
}

read_nonnegative_integer() {
  integer_path="$1"
  integer_value=""

  if [ ! -r "$integer_path" ]; then
    return 1
  fi
  IFS= read -r integer_value <"$integer_path" || [ -n "$integer_value" ] || return 1
  case "$integer_value" in
    '' | *[!0-9]*)
      return 1
      ;;
  esac

  printf '%s\n' "$integer_value"
}

read_storage_bytes() {
  if [ -z "$storage_path" ] || [ ! -e "$storage_path" ]; then
    return 1
  fi

  storage_kibibytes="$(du -sk "$storage_path" 2>/dev/null | awk 'NR == 1 { print $1 }')"
  case "$storage_kibibytes" in
    '' | *[!0-9]*)
      return 1
      ;;
  esac

  printf '%s\n' "$((storage_kibibytes * 1024))"
}

write_json_string() {
  printf '"'
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
  printf '"'
}

memory_current_support="unsupported"
memory_peak_support="unsupported"
storage_support="unsupported"
cgroup_namespace_support="unverified"
sampled_memory_current_peak_bytes=""
peak_memory_bytes=""
sampled_storage_peak_bytes=""

if [ -r "$cgroup_membership_path" ] \
  && [ "$(sed -n '1,$p' "$cgroup_membership_path" 2>/dev/null)" = "0::/" ]; then
  cgroup_namespace_support="verified-private"
fi

sample_resources() {
  memory_current_bytes="null"
  memory_peak_sample_bytes=""
  storage_bytes="null"

  if [ -r "$cgroup_path/cgroup.controllers" ] \
    && current_bytes="$(read_nonnegative_integer "$cgroup_path/memory.current")"; then
    memory_current_support="exact"
    memory_current_bytes="$current_bytes"
    if [ -z "$sampled_memory_current_peak_bytes" ] \
      || [ "$current_bytes" -gt "$sampled_memory_current_peak_bytes" ]; then
      sampled_memory_current_peak_bytes="$current_bytes"
    fi
  fi

  if [ -r "$cgroup_path/cgroup.controllers" ] \
    && memory_peak_sample_bytes="$(read_nonnegative_integer "$cgroup_path/memory.peak")"; then
    memory_peak_support="exact"
    if [ -z "$peak_memory_bytes" ] \
      || [ "$memory_peak_sample_bytes" -gt "$peak_memory_bytes" ]; then
      peak_memory_bytes="$memory_peak_sample_bytes"
    fi
  fi

  if current_storage_bytes="$(read_storage_bytes)"; then
    storage_support="sampled"
    storage_bytes="$current_storage_bytes"
    if [ -z "$sampled_storage_peak_bytes" ] \
      || [ "$current_storage_bytes" -gt "$sampled_storage_peak_bytes" ]; then
      sampled_storage_peak_bytes="$current_storage_bytes"
    fi
  fi

  observed_at_unix_millis="$(timestamp_unix_millis)"
  printf '{"observed_at_unix_millis":%s,"memory_current_bytes":%s,"storage_bytes":%s}\n' \
    "$observed_at_unix_millis" \
    "$memory_current_bytes" \
    "$storage_bytes" >>"$samples_path"
}

json_number_or_null() {
  if [ -n "$1" ]; then
    printf '%s' "$1"
  else
    printf 'null'
  fi
}

started_at="$(timestamp_rfc3339)"
started_at_unix_millis="$(timestamp_unix_millis)"

# Establish report-window coverage before the child can record its own start.
# Parent and child scheduling order is otherwise nondeterministic after spawn.
sample_resources
if [ "$require_exact_memory" = true ] \
  && { [ "$memory_current_support" != exact ] || [ "$memory_peak_support" != exact ]; }; then
  fail_configuration "required cgroup-v2 memory.current and memory.peak sources are unavailable"
fi
if [ "$require_sampled_storage" = true ] && [ "$storage_support" != sampled ]; then
  fail_configuration "required storage path cannot be sampled: $storage_path"
fi
if [ "$require_private_cgroup_namespace" = true ] \
  && [ "$cgroup_namespace_support" != verified-private ]; then
  fail_configuration "required private cgroup-v2 namespace is unavailable"
fi
"$@" &
child_pid="$!"

while kill -0 "$child_pid" 2>/dev/null; do
  sleep "$sample_interval_seconds" &
  sleep_pid="$!"
  wait "$sleep_pid" 2>/dev/null || true
  sleep_pid=""
  if kill -0 "$child_pid" 2>/dev/null; then
    sample_resources
  fi
done

wait "$child_pid"
child_exit_status="$?"
child_pid=""

# Capture final cgroup and storage values after the child has released resources.
sample_resources
completed_at="$(timestamp_rfc3339)"
completed_at_unix_millis="$(timestamp_unix_millis)"

write_evidence() {
  {
    printf '{\n'
    printf '  "evidence_format_version": %s,\n' "$EVIDENCE_FORMAT_VERSION"
    printf '  "measurement_kind": "container-resource-observation",\n'
    printf '  "component_id": '
    write_json_string "$component_id"
    printf ',\n'
    printf '  "trial_id": '
    write_json_string "$trial_id"
    printf ',\n'
    printf '  "sample_interval_seconds": %s,\n' "$sample_interval_seconds"
    printf '  "started_at": '
    write_json_string "$started_at"
    printf ',\n'
    printf '  "started_at_unix_millis": %s,\n' "$started_at_unix_millis"
    printf '  "completed_at": '
    write_json_string "$completed_at"
    printf ',\n'
    printf '  "completed_at_unix_millis": %s,\n' "$completed_at_unix_millis"
    printf '  "child_exit_status": %s,\n' "$child_exit_status"
    printf '  "peak_memory_bytes": '
    json_number_or_null "$peak_memory_bytes"
    printf ',\n'
    printf '  "sampled_memory_current_peak_bytes": '
    json_number_or_null "$sampled_memory_current_peak_bytes"
    printf ',\n'
    printf '  "sampled_storage_peak_bytes": '
    json_number_or_null "$sampled_storage_peak_bytes"
    printf ',\n'
    printf '  "sources": {\n'
    printf '    "cgroup_namespace": {"support":"%s","kind":"proc-self-cgroup-v2","path":' \
      "$cgroup_namespace_support"
    write_json_string "$cgroup_membership_path"
    printf '},\n'
    printf '    "memory_peak": {"support":"%s","kind":"cgroup-v2-memory.peak","path":' \
      "$memory_peak_support"
    write_json_string "$cgroup_path/memory.peak"
    printf '},\n'
    printf '    "memory_current": {"support":"%s","kind":"cgroup-v2-memory.current","path":' \
      "$memory_current_support"
    write_json_string "$cgroup_path/memory.current"
    printf '},\n'
    printf '    "storage": {"support":"%s","kind":"du-allocated-kibibytes","path":' \
      "$storage_support"
    if [ -n "$storage_path" ]; then
      write_json_string "$storage_path"
    else
      printf 'null'
    fi
    printf '}\n'
    printf '  },\n'
    printf '  "samples": [\n'

    is_first_sample=true
    while IFS= read -r resource_sample; do
      if [ "$is_first_sample" = true ]; then
        is_first_sample=false
      else
        printf ',\n'
      fi
      printf '    %s' "$resource_sample"
    done <"$samples_path"
    printf '\n  ]\n'
    printf '}\n'
  } >"$evidence_temporary_path" || return 1

  chmod 0644 "$evidence_temporary_path"
}

if ! write_evidence \
  || ! ln "$evidence_temporary_path" "$evidence_path" \
  || ! rm "$evidence_temporary_path"; then
  printf 'container resource evidence write failed: %s\n' "$evidence_path" >&2
  if [ "$child_exit_status" -ne 0 ]; then
    exit "$child_exit_status"
  fi
  exit 74
fi

rm -f "$samples_path"
exit "$child_exit_status"
