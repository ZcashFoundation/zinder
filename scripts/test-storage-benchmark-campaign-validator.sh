#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-storage-benchmark-campaign.sh"
storage_lifecycle_validator="$repository_root/scripts/validate-rocksdb-storage-lifecycle-report.sh"
scratch_directory="$(mktemp -d)"
trap 'rm -rf "$scratch_directory"' EXIT

fail() {
  echo >&2 "storage benchmark campaign validator test failed: $*"
  exit 1
}

producer_report_format_version="$(sed -n \
  's/^pub const REPORT_FORMAT_VERSION: u32 = \([0-9][0-9]*\);$/\1/p' \
  "$repository_root/services/zinder-bench/src/report.rs")"
[[ "$producer_report_format_version" == "1" ]] ||
  fail "canonical-replay-storage producer report format version must stay v1"
producer_canonical_fixture_report_format_version="$(sed -n \
  's/^pub const ROCKSDB_CANONICAL_FIXTURE_REPLAY_REPORT_FORMAT_VERSION: u32 = \([0-9][0-9]*\);$/\1/p' \
  "$repository_root/services/zinder-bench/src/report.rs")"
[[ "$producer_canonical_fixture_report_format_version" == "1" ]] ||
  fail "rocksdb-canonical-fixture-replay producer report format version must be v1"
producer_lifecycle_report_format_version="$(sed -n \
  's/^pub const ROCKSDB_STORAGE_LIFECYCLE_REPORT_FORMAT_VERSION: u32 = \([0-9][0-9]*\);$/\1/p' \
  "$repository_root/services/zinder-bench/src/report.rs")"
[[ "$producer_lifecycle_report_format_version" == "1" ]] ||
  fail "rocksdb-storage-lifecycle producer report format version must be v1"
producer_fixture_format_version="$(sed -n \
  's/^pub const FIXTURE_FORMAT_VERSION: u32 = \([0-9][0-9]*\);$/\1/p' \
  "$repository_root/services/zinder-bench/src/fixture.rs")"
[[ "$producer_fixture_format_version" == "1" ]] ||
  fail "producer fixture format version must be the exact v1 campaign contract"
grep -Fq ".report_format_version == $producer_report_format_version" \
  "$validator" ||
  fail "$(basename "$validator") does not require producer report format $producer_report_format_version"
grep -Fq ".report_format_version == $producer_lifecycle_report_format_version" \
  "$storage_lifecycle_validator" ||
  fail "$(basename "$storage_lifecycle_validator") does not support producer report format $producer_lifecycle_report_format_version"
grep -Fq ".fixture.fixture_format_version == $producer_fixture_format_version" \
  "$validator" ||
  fail "campaign validator does not require producer fixture format $producer_fixture_format_version"

producer_store_version() {
  local constant="$1" source="$2"
  sed -nE "s/^pub const ${constant}: u16 = ([A-Z0-9_]+);$/\1/p" "$repository_root/$source"
}

check_lifecycle_pin() {
  local field="$1" expected="$2" pinned
  [[ "$expected" =~ ^[0-9]+$ ]] || fail "could not resolve producer version for $field"
  pinned="$(sed -nE \
    -e "s/^[[:space:]]+and [.]$field == ([0-9]+)[)]?[[:space:]]*$/\1/p" \
    "$storage_lifecycle_validator")"
  [[ "$pinned" == "$expected" ]] ||
    fail "lifecycle validator pins $field to ${pinned:-<missing>}, producer requires $expected"
}

projection_source=crates/zinder-wallet-projection/src/lib.rs
projection_schema="$(producer_store_version WALLET_PROJECTION_SCHEMA_VERSION "$projection_source")"
wallet_schema="$(producer_store_version WALLET_ROCKSDB_SCHEMA_VERSION crates/zinder-wallet-rocksdb/src/store.rs)"
if [[ "$wallet_schema" == "WALLET_PROJECTION_SCHEMA_VERSION" ]]; then
  wallet_schema="$projection_schema"
fi

check_lifecycle_pin canonical_store_schema_version \
  "$(producer_store_version CANONICAL_STORE_SCHEMA_VERSION crates/zinder-store/src/canonical_store/mod.rs)"
check_lifecycle_pin wallet_store_schema_version "$wallet_schema"
check_lifecycle_pin wallet_projection_schema_version "$projection_schema"
check_lifecycle_pin wallet_value_encoding_version \
  "$(producer_store_version WALLET_PROJECTION_VALUE_ENCODING_VERSION "$projection_source")"

write_report() {
  local candidate="$1"
  local trial_id="$2"
  local started_at="$3"
  local completed_at="$4"
  local report_path="$5"
  jq -n \
    --arg candidate "$candidate" \
    --arg trial_id "$trial_id" \
    --argjson started_at "$started_at" \
    --argjson completed_at "$completed_at" '
    def rocksdb_storage: {
      engine: "rocksdb",
      storage_schema_version: 1,
      ingestion_mode: "sorted-external-sst",
      durability_mode: "external-sst-ingest-with-synchronous-completion-marker",
      database_io_mode: "buffered",
      external_sst_io_mode: "buffered",
      compression: "snappy",
      external_sst_bytes: 7000,
      rocksdb_resource_budget: {
        block_cache_bytes: 67108864,
        max_wal_bytes: 33554432,
        max_open_files: 128,
        write_buffer_bytes: 8388608,
        max_write_buffer_count: 2,
        max_background_jobs: 2,
        memtable_budget_bytes: 16777216,
        statistics_level: "tickers"
      }
    };
    def postgres_storage: {
      engine: "postgres",
      storage_schema_version: 1,
      ingestion_mode: "binary-copy-single-load-transaction-with-deferred-index",
      tables_logged: true,
      replay_envelope_compression: "lz4",
      replay_table_bytes: 6000,
      index_bytes: 1000,
      wal_bytes: 9000,
      server_settings: {
        server_version: "18.4",
        server_version_number: 180004,
        max_connections: 50,
        shared_buffers_bytes: 2147483648,
        effective_cache_size_bytes: 6442450944,
        maintenance_work_mem_bytes: 1073741824,
        work_mem_bytes: 67108864,
        max_wal_size_bytes: 17179869184,
        min_wal_size_bytes: 2147483648,
        checkpoint_timeout_seconds: 900,
        checkpoint_completion_target: 0.9,
        wal_compression: "on",
        password_encryption_default: "scram-sha-256",
        max_worker_processes: 6,
        max_parallel_workers: 6,
        max_parallel_maintenance_workers: 4,
        track_io_timing: true,
        huge_pages: "try",
        fsync: true,
        full_page_writes: true,
        synchronous_commit: "on",
        wal_level: "replica",
        data_checksums: true
      },
      benchmark_runtime: {
        database_image_reference: ("postgres@sha256:" + ("b" * 64)),
        client_cpu_limit_cores: 2,
        client_memory_limit_bytes: 8589934592,
        database_cpu_limit_cores: 6,
        database_memory_limit_bytes: 8589934592
      }
    };
    ($candidate == "rocksdb-canonical-replay-storage") as $is_rocksdb
    | {
        contract_identity: "benchmark-report",
        report_format_version: 1,
        measurement_kind: "canonical-replay-storage",
        provenance: {
          benchmark_version: "0.1.0",
          software_revision: "0123456789abcdef",
          run: {
            trial_id: $trial_id,
            fixture_cache_policy: "warm",
            started_at_unix_millis: $started_at,
            completed_at_unix_millis: $completed_at
          },
          runner: {
            id: "linux-amd64-c8-m16-nvme-01",
            cpu_limit_cores: 8,
            memory_limit_bytes: 17179869184,
            storage_class: "local-nvme"
          },
          image_reference: ("sha256:" + ("a" * 64)),
          target_os: "linux",
          target_arch: "x86_64"
        },
        fixture: {
          contract_identity: "canonical-fixture",
          fixture_format_version: 1,
          canonical_artifact_schema_version: 1,
          canonical_block_facts_digest_evidence: {
            block_digest_version: 1,
            sequence_digest_version: 1,
            block_count: 10,
            sequence_digest_sha256: ("c" * 64)
          },
          tip_hash_hex: ("d" * 64),
          digest_sha256: ("e" * 64),
          network: "zcash-regtest",
          from_height: 1,
          to_height: 10,
          block_count: 10,
          workload_density: {block_count: 10},
          segment_count: 1
        },
        storage_candidate: {
          id: $candidate,
          canonical_engine: (if $is_rocksdb then "rocksdb" else "postgres" end),
          canonical_model: "block-granular-canonical-replays",
          diagnostic_projection_engine: null,
          topology: (if $is_rocksdb then "rocksdb" else "postgres-diagnostic" end)
        },
        round_trip: {
          scope: "block-local-canonical-replay",
          block_prepare_concurrency: 16,
          wall_clock_seconds: (if $is_rocksdb then 10 else 8 end),
          storage_initialization_wall_clock_seconds: 0.5,
          replay_preparation_wall_clock_seconds: 1,
          replay_persistence_wall_clock_seconds: 2,
          index_construction_wall_clock_seconds: 1,
          storage_optimization_wall_clock_seconds: 0.5,
          validation_wall_clock_seconds: 1,
          publication_wall_clock_seconds: 0.5,
          fresh_reader_validation_wall_clock_seconds: 0.5,
          storage_measurement_wall_clock_seconds: 0.5,
          unattributed_wall_clock_seconds: (if $is_rocksdb then 2.5 else 0.5 end),
          first_height: 1,
          first_hash_hex: ("f" * 64),
          tip_height: 10,
          tip_hash_hex: ("d" * 64),
          block_count: 10,
          blocks_per_second: (if $is_rocksdb then 1 else 1.25 end),
          logical_replay_bytes: 4096,
          physical_storage_bytes: 8192,
          persisted_sequence_digest: {
            block_digest_version: 1,
            sequence_digest_version: 1,
            block_count: 10,
            sha256: ("c" * 64)
          },
          fixture_sequence_digest_match: true,
          replay_format_version: 1,
          semantic_replay_validated: true,
          storage: (if $is_rocksdb then rocksdb_storage else postgres_storage end),
          benchmark_client_peak_rss: {bytes: null, source: "unavailable"}
        }
      }
  ' >"$report_path"
}

write_resource_evidence() {
  local component_id="$1"
  local trial_id="$2"
  local report_started_at="$3"
  local report_completed_at="$4"
  local sample_offset_millis="$5"
  local memory_base_bytes="$6"
  local component_peak_memory_bytes="$7"
  local storage_base_bytes="$8"
  local storage_required="$9"
  local evidence_path="${10}"
  jq -n \
    --arg component_id "$component_id" \
    --arg trial_id "$trial_id" \
    --argjson report_started_at "$report_started_at" \
    --argjson report_completed_at "$report_completed_at" \
    --argjson sample_offset_millis "$sample_offset_millis" \
    --argjson memory_base_bytes "$memory_base_bytes" \
    --argjson component_peak_memory_bytes "$component_peak_memory_bytes" \
    --argjson storage_base_bytes "$storage_base_bytes" \
    --argjson storage_required "$storage_required" '
    [range(-1; 12) as $sample_index
      | {
          observed_at_unix_millis: (
            $report_started_at + ($sample_index * 100) + $sample_offset_millis
          ),
          memory_current_bytes: (
            if $sample_index == 11 then
              $component_peak_memory_bytes - 1
            else
              $memory_base_bytes + (($sample_index + 1) * 10)
            end
          ),
          storage_bytes: (
            if $storage_required then
              if $sample_index == 11 then
                $storage_base_bytes + 999999
              else
                $storage_base_bytes + (($sample_index + 1) * 100)
              end
            else
              null
            end
          )
        }
    ] as $samples
    | {
        evidence_format_version: 1,
        measurement_kind: "container-resource-observation",
        component_id: $component_id,
        trial_id: $trial_id,
        sample_interval_seconds: 0.1,
        started_at: "1970-01-01T00:00:00Z",
        started_at_unix_millis: ($report_started_at - 200),
        completed_at: "1970-01-01T00:00:01Z",
        completed_at_unix_millis: ($report_completed_at + 1200),
        child_exit_status: 0,
        peak_memory_bytes: $component_peak_memory_bytes,
        sampled_memory_current_peak_bytes: ([$samples[].memory_current_bytes] | max),
        sampled_storage_peak_bytes: (
          if $storage_required then [$samples[].storage_bytes] | max else null end
        ),
        sources: {
          cgroup_namespace: {
            support: "verified-private",
            kind: "proc-self-cgroup-v2",
            path: "/proc/self/cgroup"
          },
          memory_peak: {
            support: "exact",
            kind: "cgroup-v2-memory.peak",
            path: "/sys/fs/cgroup/memory.peak"
          },
          memory_current: {
            support: "exact",
            kind: "cgroup-v2-memory.current",
            path: "/sys/fs/cgroup/memory.current"
          },
          storage: {
            support: (if $storage_required then "sampled" else "unsupported" end),
            kind: "du-allocated-kibibytes",
            path: (
              if $component_id == "rocksdb-canonical-replay-storage-client" then
                "/var/lib/zinder"
              elif $component_id == "postgres-canonical-replay-storage-database" then
                "/var/lib/postgresql"
              else
                null
              end
            )
          }
        },
        samples: $samples
      }
  ' >"$evidence_path"
}

create_valid_campaign() {
  local campaign_directory="$1"
  mkdir -p "$campaign_directory"
  printf 'rocksdb_report\trocksdb_resources\tpostgres_report\tpostgres_client_resources\tpostgres_database_resources\n' \
    >"$campaign_directory/campaign.tsv"
  local trial_number
  for trial_number in 1 2 3 4 5; do
    local trial_id
    local base_timestamp
    local rocksdb_started
    local rocksdb_completed
    local postgres_started
    local postgres_completed
    local rocksdb_resources
    local postgres_client_resources
    local postgres_database_resources
    trial_id="$(printf 'trial-%02d' "$trial_number")"
    base_timestamp=$((100000 + trial_number * 10000))
    if ((trial_number % 2 == 1)); then
      rocksdb_started=$base_timestamp
      rocksdb_completed=$((base_timestamp + 1000))
      postgres_started=$((base_timestamp + 2000))
      postgres_completed=$((base_timestamp + 3000))
    else
      postgres_started=$base_timestamp
      postgres_completed=$((base_timestamp + 1000))
      rocksdb_started=$((base_timestamp + 2000))
      rocksdb_completed=$((base_timestamp + 3000))
    fi
    write_report \
      "rocksdb-canonical-replay-storage" \
      "$trial_id" \
      "$rocksdb_started" \
      "$rocksdb_completed" \
      "$campaign_directory/rocksdb-canonical-replay-storage-$trial_id.json"
    write_report \
      "postgres-canonical-replay-storage" \
      "$trial_id" \
      "$postgres_started" \
      "$postgres_completed" \
      "$campaign_directory/postgres-canonical-replay-storage-$trial_id.json"
    rocksdb_resources="rocksdb-canonical-replay-storage-client-$trial_id.resources.json"
    postgres_client_resources="postgres-canonical-replay-storage-client-$trial_id.resources.json"
    postgres_database_resources="postgres-canonical-replay-storage-database-$trial_id.resources.json"
    write_resource_evidence \
      "rocksdb-canonical-replay-storage-client" \
      "$trial_id" \
      "$rocksdb_started" \
      "$rocksdb_completed" \
      0 \
      1000 \
      900000000 \
      1000 \
      true \
      "$campaign_directory/$rocksdb_resources"
    write_resource_evidence \
      "postgres-canonical-replay-storage-client" \
      "$trial_id" \
      "$postgres_started" \
      "$postgres_completed" \
      0 \
      200 \
      800000000 \
      0 \
      false \
      "$campaign_directory/$postgres_client_resources"
    write_resource_evidence \
      "postgres-canonical-replay-storage-database" \
      "$trial_id" \
      "$postgres_started" \
      "$postgres_completed" \
      40 \
      500 \
      700000000 \
      2000 \
      true \
      "$campaign_directory/$postgres_database_resources"
    printf 'rocksdb-canonical-replay-storage-%s.json\t%s\tpostgres-canonical-replay-storage-%s.json\t%s\t%s\n' \
      "$trial_id" \
      "$rocksdb_resources" \
      "$trial_id" \
      "$postgres_client_resources" \
      "$postgres_database_resources" >>"$campaign_directory/campaign.tsv"
  done
}

mutate_json_artifact() {
  local artifact_path="$1"
  local jq_filter="$2"
  local replacement_path="$artifact_path.replacement"
  jq "$jq_filter" "$artifact_path" >"$replacement_path"
  mv "$replacement_path" "$artifact_path"
}

mutate_report() {
  mutate_json_artifact "$@"
}

shift_resource_evidence() {
  local evidence_path="$1"
  local timestamp_delta_millis="$2"
  mutate_json_artifact \
    "$evidence_path" \
    ".started_at_unix_millis += $timestamp_delta_millis
     | .completed_at_unix_millis += $timestamp_delta_millis
     | .samples |= map(
         .observed_at_unix_millis += $timestamp_delta_millis
       )"
}

expect_failure() {
  local case_name="$1"
  local campaign_directory="$2"
  if "$validator" "$campaign_directory/campaign.tsv" >/dev/null 2>&1; then
    fail "$case_name was accepted"
  fi
}

valid_campaign="$scratch_directory/valid"
create_valid_campaign "$valid_campaign"
mutate_report \
  "$valid_campaign/rocksdb-canonical-replay-storage-trial-02.json" \
  '.round_trip.storage.external_sst_bytes = 7100'
mutate_report \
  "$valid_campaign/postgres-canonical-replay-storage-trial-02.json" \
  '.round_trip.storage.replay_table_bytes = 6100
   | .round_trip.storage.index_bytes = 1100
   | .round_trip.storage.wal_bytes = 9100'
valid_summary="$($validator "$valid_campaign/campaign.tsv")"
[[ "$(jq -r '.campaign.trial_count' <<<"$valid_summary")" == "5" ]] \
  || fail "valid campaign did not report five trials"
[[ "$(jq -r '.campaign.trials | map(.arm_order) | join(",")' <<<"$valid_summary")" == "rocksdb-first,postgres-first,rocksdb-first,postgres-first,rocksdb-first" ]] \
  || fail "valid campaign did not preserve derived alternating order"
[[ "$(jq -r '.campaign.trials[0].arms.rocksdb.report_sha256 | test("^[0-9a-f]{64}$")' <<<"$valid_summary")" == "true" ]] \
  || fail "valid campaign did not identify the RocksDB report hash"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.report_path | endswith("postgres-canonical-replay-storage-trial-01.json")' <<<"$valid_summary")" == "true" ]] \
  || fail "valid campaign did not identify the PostgreSQL report path"
[[ "$(jq -r '.campaign.trials[0].arms.rocksdb.resource_evidence.artifact_sha256 | test("^[0-9a-f]{64}$")' <<<"$valid_summary")" == "true" ]] \
  || fail "valid campaign did not identify the RocksDB resource evidence hash"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.resource_evidence.database.artifact_path | endswith("postgres-canonical-replay-storage-database-trial-01.resources.json")' <<<"$valid_summary")" == "true" ]] \
  || fail "valid campaign did not identify the PostgreSQL database resource evidence path"
[[ "$(jq -r '[
    .campaign.trials[0].arms.rocksdb.resource_evidence.maximum_observed_report_window_sample_gap_millis,
    .campaign.trials[0].arms.postgres.resource_evidence.client.maximum_observed_report_window_sample_gap_millis,
    .campaign.trials[0].arms.postgres.resource_evidence.database.maximum_observed_report_window_sample_gap_millis
  ] | unique | join(",")' <<<"$valid_summary")" == "100" ]] \
  || fail "valid campaign did not report the observed resource sample cadence"
[[ "$(jq -r '.campaign.trials[0].arms.rocksdb.sampled_whole_arm_memory_peak_bytes' <<<"$valid_summary")" == "1110" ]] \
  || fail "RocksDB normalized memory did not use report-window memory.current samples"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.sampled_whole_arm_memory_peak_bytes' <<<"$valid_summary")" == "910" ]] \
  || fail "PostgreSQL normalized memory summed independent component peaks"
[[ "$(jq -r '.campaign.trials[0].arms.rocksdb.sampled_whole_arm_storage_peak_bytes' <<<"$valid_summary")" == "2100" ]] \
  || fail "RocksDB normalized storage did not use report-window samples"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.sampled_whole_arm_storage_peak_bytes' <<<"$valid_summary")" == "3000" ]] \
  || fail "PostgreSQL normalized storage did not use database report-window samples"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.memory_alignment.aligned_sample_pair_count' <<<"$valid_summary")" == "11" ]] \
  || fail "PostgreSQL normalized memory did not align every client sample exactly once"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.memory_alignment.maximum_timestamp_delta_millis' <<<"$valid_summary")" == "60" ]] \
  || fail "PostgreSQL normalized memory reported the wrong alignment delta"
[[ "$(jq -r '.candidates[] | select(.candidate == "rocksdb-canonical-replay-storage") | .sampled_whole_arm_memory_peak_bytes.median' <<<"$valid_summary")" == "1110" ]] \
  || fail "RocksDB candidate summary omitted normalized memory statistics"
[[ "$(jq -r '.candidates[] | select(.candidate == "postgres-canonical-replay-storage") | .sampled_whole_arm_storage_peak_bytes.maximum' <<<"$valid_summary")" == "3000" ]] \
  || fail "PostgreSQL candidate summary omitted normalized storage statistics"

nearest_alignment_campaign="$scratch_directory/nearest-alignment"
cp -R "$valid_campaign" "$nearest_alignment_campaign"
mutate_json_artifact \
  "$nearest_alignment_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.samples += [
      {
        observed_at_unix_millis: 112470,
        memory_current_bytes: 6000,
        storage_bytes: 2400
      },
      {
        observed_at_unix_millis: 112489,
        memory_current_bytes: 5000,
        storage_bytes: 2400
      }
    ]
   | .samples |= sort_by(.observed_at_unix_millis)'
nearest_alignment_summary="$($validator "$nearest_alignment_campaign/campaign.tsv")"
[[ "$(jq -r '.campaign.trials[0].arms.postgres.sampled_whole_arm_memory_peak_bytes' <<<"$nearest_alignment_summary")" == "5260" ]] \
  || fail "PostgreSQL memory did not select the nearest database sample"

fractional_cpu_campaign="$scratch_directory/fractional-cpu"
cp -R "$valid_campaign" "$fractional_cpu_campaign"
for report_path in "$fractional_cpu_campaign"/*-canonical-replay-storage-trial-*.json; do
  mutate_report "$report_path" '.provenance.runner.cpu_limit_cores = 0.3'
done
for report_path in "$fractional_cpu_campaign"/postgres-canonical-replay-storage-trial-*.json; do
  mutate_report \
    "$report_path" \
    '.round_trip.storage.benchmark_runtime.client_cpu_limit_cores = 0.1
     | .round_trip.storage.benchmark_runtime.database_cpu_limit_cores = 0.2'
done
"$validator" "$fractional_cpu_campaign/campaign.tsv" >/dev/null \
  || fail "valid fractional PostgreSQL CPU partition was rejected"

copied_campaign="$scratch_directory/copied"
cp -R "$valid_campaign" "$copied_campaign"
cp "$copied_campaign/rocksdb-canonical-replay-storage-trial-01.json" \
  "$copied_campaign/rocksdb-canonical-replay-storage-trial-05.json"
expect_failure "copied report" "$copied_campaign"

trial_mismatch_campaign="$scratch_directory/trial-mismatch"
cp -R "$valid_campaign" "$trial_mismatch_campaign"
mutate_report \
  "$trial_mismatch_campaign/postgres-canonical-replay-storage-trial-03.json" \
  '.provenance.run.trial_id = "different-trial"'
expect_failure "paired trial mismatch" "$trial_mismatch_campaign"

wrong_version_campaign="$scratch_directory/wrong-version"
cp -R "$valid_campaign" "$wrong_version_campaign"
mutate_report "$wrong_version_campaign/rocksdb-canonical-replay-storage-trial-02.json" '.report_format_version = 5'
expect_failure "wrong report version" "$wrong_version_campaign"

missing_report_identity_campaign="$scratch_directory/missing-report-identity"
cp -R "$valid_campaign" "$missing_report_identity_campaign"
mutate_report \
  "$missing_report_identity_campaign/rocksdb-canonical-replay-storage-trial-02.json" \
  'del(.contract_identity)'
expect_failure "missing report contract identity" "$missing_report_identity_campaign"

unrecognized_report_identity_campaign="$scratch_directory/unrecognized-report-identity"
cp -R "$valid_campaign" "$unrecognized_report_identity_campaign"
mutate_report \
  "$unrecognized_report_identity_campaign/postgres-canonical-replay-storage-trial-02.json" \
  '.contract_identity = "zinder-benchmark-report"'
expect_failure "unrecognized report contract identity" "$unrecognized_report_identity_campaign"

missing_fixture_identity_campaign="$scratch_directory/missing-fixture-identity"
cp -R "$valid_campaign" "$missing_fixture_identity_campaign"
mutate_report \
  "$missing_fixture_identity_campaign/rocksdb-canonical-replay-storage-trial-03.json" \
  'del(.fixture.contract_identity)'
expect_failure "missing fixture contract identity" "$missing_fixture_identity_campaign"

unrecognized_fixture_identity_campaign="$scratch_directory/unrecognized-fixture-identity"
cp -R "$valid_campaign" "$unrecognized_fixture_identity_campaign"
mutate_report \
  "$unrecognized_fixture_identity_campaign/postgres-canonical-replay-storage-trial-03.json" \
  '.fixture.contract_identity = "zinder-bench-fixture-manifest"'
expect_failure "unrecognized fixture contract identity" "$unrecognized_fixture_identity_campaign"

fixture_v2_campaign="$scratch_directory/fixture-v2"
cp -R "$valid_campaign" "$fixture_v2_campaign"
mutate_report \
  "$fixture_v2_campaign/rocksdb-canonical-replay-storage-trial-03.json" \
  '.fixture.fixture_format_version = 2'
expect_failure "obsolete fixture format v2" "$fixture_v2_campaign"

wrong_rocksdb_schema_campaign="$scratch_directory/wrong-rocksdb-schema"
cp -R "$valid_campaign" "$wrong_rocksdb_schema_campaign"
mutate_report \
  "$wrong_rocksdb_schema_campaign/rocksdb-canonical-replay-storage-trial-01.json" \
  '.round_trip.storage.storage_schema_version = 2'
expect_failure "wrong RocksDB storage schema" "$wrong_rocksdb_schema_campaign"

wrong_postgres_schema_campaign="$scratch_directory/wrong-postgres-schema"
cp -R "$valid_campaign" "$wrong_postgres_schema_campaign"
mutate_report \
  "$wrong_postgres_schema_campaign/postgres-canonical-replay-storage-trial-01.json" \
  '.round_trip.storage.storage_schema_version = 2'
expect_failure "wrong PostgreSQL storage schema" "$wrong_postgres_schema_campaign"

missing_replay_evidence_campaign="$scratch_directory/missing-replay-evidence"
cp -R "$valid_campaign" "$missing_replay_evidence_campaign"
mutate_report \
  "$missing_replay_evidence_campaign/postgres-canonical-replay-storage-trial-02.json" \
  'del(.round_trip.replay_format_version)'
expect_failure "missing replay format evidence" "$missing_replay_evidence_campaign"

wrong_replay_format_campaign="$scratch_directory/wrong-replay-format"
cp -R "$valid_campaign" "$wrong_replay_format_campaign"
mutate_report \
  "$wrong_replay_format_campaign/rocksdb-canonical-replay-storage-trial-03.json" \
  '.round_trip.replay_format_version = 99'
expect_failure "unsupported replay format" "$wrong_replay_format_campaign"

unvalidated_semantic_replay_campaign="$scratch_directory/unvalidated-semantic-replay"
cp -R "$valid_campaign" "$unvalidated_semantic_replay_campaign"
mutate_report \
  "$unvalidated_semantic_replay_campaign/rocksdb-canonical-replay-storage-trial-04.json" \
  '.round_trip.semantic_replay_validated = false'
expect_failure "unvalidated semantic replay" "$unvalidated_semantic_replay_campaign"

wrong_topology_campaign="$scratch_directory/wrong-topology"
cp -R "$valid_campaign" "$wrong_topology_campaign"
mutate_report \
  "$wrong_topology_campaign/postgres-canonical-replay-storage-trial-04.json" \
  '.storage_candidate.topology = "rocksdb"'
expect_failure "wrong topology" "$wrong_topology_campaign"

null_configuration_campaign="$scratch_directory/null-configuration"
cp -R "$valid_campaign" "$null_configuration_campaign"
mutate_report \
  "$null_configuration_campaign/rocksdb-canonical-replay-storage-trial-01.json" \
  '.round_trip.storage.rocksdb_resource_budget = null'
expect_failure "null RocksDB configuration" "$null_configuration_campaign"

zero_storage_campaign="$scratch_directory/zero-storage"
cp -R "$valid_campaign" "$zero_storage_campaign"
mutate_report \
  "$zero_storage_campaign/postgres-canonical-replay-storage-trial-02.json" \
  '.round_trip.physical_storage_bytes = 0'
expect_failure "zero physical storage" "$zero_storage_campaign"

missing_database_image_campaign="$scratch_directory/missing-database-image"
cp -R "$valid_campaign" "$missing_database_image_campaign"
mutate_report \
  "$missing_database_image_campaign/postgres-canonical-replay-storage-trial-05.json" \
  '.round_trip.storage.benchmark_runtime.database_image_reference = null'
expect_failure "missing PostgreSQL database image" "$missing_database_image_campaign"

overlapping_campaign="$scratch_directory/overlapping"
cp -R "$valid_campaign" "$overlapping_campaign"
mutate_report \
  "$overlapping_campaign/postgres-canonical-replay-storage-trial-01.json" \
  '.provenance.run.started_at_unix_millis = 110500
   | .provenance.run.completed_at_unix_millis = 111500'
shift_resource_evidence \
  "$overlapping_campaign/postgres-canonical-replay-storage-client-trial-01.resources.json" \
  -1500
shift_resource_evidence \
  "$overlapping_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  -1500
expect_failure "overlapping paired arms" "$overlapping_campaign"

cross_trial_overlap_campaign="$scratch_directory/cross-trial-overlap"
cp -R "$valid_campaign" "$cross_trial_overlap_campaign"
mutate_report \
  "$cross_trial_overlap_campaign/postgres-canonical-replay-storage-trial-02.json" \
  '.provenance.run.started_at_unix_millis = 112500
   | .provenance.run.completed_at_unix_millis = 113500'
shift_resource_evidence \
  "$cross_trial_overlap_campaign/postgres-canonical-replay-storage-client-trial-02.resources.json" \
  -7500
shift_resource_evidence \
  "$cross_trial_overlap_campaign/postgres-canonical-replay-storage-database-trial-02.resources.json" \
  -7500
expect_failure "overlapping chronological trials" "$cross_trial_overlap_campaign"

chronological_order_campaign="$scratch_directory/chronological-order"
cp -R "$valid_campaign" "$chronological_order_campaign"
mutate_report \
  "$chronological_order_campaign/rocksdb-canonical-replay-storage-trial-02.json" \
  '.provenance.run.started_at_unix_millis = 120000
   | .provenance.run.completed_at_unix_millis = 121000'
mutate_report \
  "$chronological_order_campaign/postgres-canonical-replay-storage-trial-02.json" \
  '.provenance.run.started_at_unix_millis = 122000
   | .provenance.run.completed_at_unix_millis = 123000'
mutate_report \
  "$chronological_order_campaign/postgres-canonical-replay-storage-trial-03.json" \
  '.provenance.run.started_at_unix_millis = 130000
   | .provenance.run.completed_at_unix_millis = 131000'
mutate_report \
  "$chronological_order_campaign/rocksdb-canonical-replay-storage-trial-03.json" \
  '.provenance.run.started_at_unix_millis = 132000
   | .provenance.run.completed_at_unix_millis = 133000'
shift_resource_evidence \
  "$chronological_order_campaign/rocksdb-canonical-replay-storage-client-trial-02.resources.json" \
  -2000
shift_resource_evidence \
  "$chronological_order_campaign/postgres-canonical-replay-storage-client-trial-02.resources.json" \
  2000
shift_resource_evidence \
  "$chronological_order_campaign/postgres-canonical-replay-storage-database-trial-02.resources.json" \
  2000
shift_resource_evidence \
  "$chronological_order_campaign/rocksdb-canonical-replay-storage-client-trial-03.resources.json" \
  2000
shift_resource_evidence \
  "$chronological_order_campaign/postgres-canonical-replay-storage-client-trial-03.resources.json" \
  -2000
shift_resource_evidence \
  "$chronological_order_campaign/postgres-canonical-replay-storage-database-trial-03.resources.json" \
  -2000
{
  printf 'rocksdb_report\trocksdb_resources\tpostgres_report\tpostgres_client_resources\tpostgres_database_resources\n'
  for trial_number in 1 3 2 4 5; do
    trial_id="$(printf 'trial-%02d' "$trial_number")"
    printf 'rocksdb-canonical-replay-storage-%s.json\trocksdb-canonical-replay-storage-client-%s.resources.json\tpostgres-canonical-replay-storage-%s.json\tpostgres-canonical-replay-storage-client-%s.resources.json\tpostgres-canonical-replay-storage-database-%s.resources.json\n' \
      "$trial_id" \
      "$trial_id" \
      "$trial_id" \
      "$trial_id" \
      "$trial_id"
  done
} >"$chronological_order_campaign/campaign.tsv"
expect_failure "nonalternating chronological arm order" "$chronological_order_campaign"

missing_phase_campaign="$scratch_directory/missing-phase"
cp -R "$valid_campaign" "$missing_phase_campaign"
mutate_report \
  "$missing_phase_campaign/rocksdb-canonical-replay-storage-trial-03.json" \
  'del(.round_trip.validation_wall_clock_seconds)'
expect_failure "missing phase timing" "$missing_phase_campaign"

wrong_range_campaign="$scratch_directory/wrong-range"
cp -R "$valid_campaign" "$wrong_range_campaign"
mutate_report \
  "$wrong_range_campaign/postgres-canonical-replay-storage-trial-04.json" \
  '.round_trip.first_height = 2'
expect_failure "inconsistent persisted range" "$wrong_range_campaign"

wrong_digest_version_campaign="$scratch_directory/wrong-digest-version"
cp -R "$valid_campaign" "$wrong_digest_version_campaign"
mutate_report \
  "$wrong_digest_version_campaign/rocksdb-canonical-replay-storage-trial-05.json" \
  '.round_trip.persisted_sequence_digest.sequence_digest_version = 2'
expect_failure "inconsistent digest version" "$wrong_digest_version_campaign"

wrong_throughput_campaign="$scratch_directory/wrong-throughput"
cp -R "$valid_campaign" "$wrong_throughput_campaign"
mutate_report \
  "$wrong_throughput_campaign/postgres-canonical-replay-storage-trial-01.json" \
  '.round_trip.blocks_per_second = 99'
expect_failure "inconsistent throughput" "$wrong_throughput_campaign"

missing_resource_evidence_campaign="$scratch_directory/missing-resource-evidence"
cp -R "$valid_campaign" "$missing_resource_evidence_campaign"
printf 'rocksdb_report\tpostgres_report\n' >"$missing_resource_evidence_campaign/campaign.tsv"
expect_failure "ledger without resource evidence columns" "$missing_resource_evidence_campaign"

missing_resource_campaign="$scratch_directory/missing-resource"
cp -R "$valid_campaign" "$missing_resource_campaign"
rm "$missing_resource_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json"
expect_failure "missing resource artifact" "$missing_resource_campaign"

duplicate_artifact_campaign="$scratch_directory/duplicate-artifact"
cp -R "$valid_campaign" "$duplicate_artifact_campaign"
duplicate_ledger_row="$(sed -n '2p' "$duplicate_artifact_campaign/campaign.tsv")"
printf '%s\n' "$duplicate_ledger_row" >>"$duplicate_artifact_campaign/campaign.tsv"
expect_failure "duplicate ledger artifacts" "$duplicate_artifact_campaign"

malformed_resource_campaign="$scratch_directory/malformed-resource"
cp -R "$valid_campaign" "$malformed_resource_campaign"
printf '{\n' \
  >"$malformed_resource_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json"
expect_failure "malformed resource artifact" "$malformed_resource_campaign"

multiple_resource_values_campaign="$scratch_directory/multiple-resource-values"
cp -R "$valid_campaign" "$multiple_resource_values_campaign"
printf '\n{}\n' \
  >>"$multiple_resource_values_campaign/postgres-canonical-replay-storage-client-trial-01.resources.json"
expect_failure "multiple resource evidence values" "$multiple_resource_values_campaign"

wrong_resource_version_campaign="$scratch_directory/wrong-resource-version"
cp -R "$valid_campaign" "$wrong_resource_version_campaign"
mutate_json_artifact \
  "$wrong_resource_version_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.evidence_format_version = 2'
expect_failure "wrong resource evidence version" "$wrong_resource_version_campaign"

wrong_resource_component_campaign="$scratch_directory/wrong-resource-component"
cp -R "$valid_campaign" "$wrong_resource_component_campaign"
mutate_json_artifact \
  "$wrong_resource_component_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.component_id = "postgres-canonical-replay-storage-client"'
expect_failure "wrong resource component" "$wrong_resource_component_campaign"

wrong_resource_trial_campaign="$scratch_directory/wrong-resource-trial"
cp -R "$valid_campaign" "$wrong_resource_trial_campaign"
mutate_json_artifact \
  "$wrong_resource_trial_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.trial_id = "different-trial"'
expect_failure "wrong resource trial" "$wrong_resource_trial_campaign"

failed_child_campaign="$scratch_directory/failed-child"
cp -R "$valid_campaign" "$failed_child_campaign"
mutate_json_artifact \
  "$failed_child_campaign/postgres-canonical-replay-storage-client-trial-01.resources.json" \
  '.child_exit_status = 7'
expect_failure "failed benchmark child" "$failed_child_campaign"

unsupported_memory_campaign="$scratch_directory/unsupported-memory"
cp -R "$valid_campaign" "$unsupported_memory_campaign"
mutate_json_artifact \
  "$unsupported_memory_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.sources.memory_current.support = "unsupported"'
expect_failure "unsupported cgroup memory source" "$unsupported_memory_campaign"

unverified_cgroup_namespace_campaign="$scratch_directory/unverified-cgroup-namespace"
cp -R "$valid_campaign" "$unverified_cgroup_namespace_campaign"
mutate_json_artifact \
  "$unverified_cgroup_namespace_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.sources.cgroup_namespace.support = "unverified"'
expect_failure "unverified component cgroup namespace" "$unverified_cgroup_namespace_campaign"

missing_rocksdb_storage_campaign="$scratch_directory/missing-rocksdb-storage"
cp -R "$valid_campaign" "$missing_rocksdb_storage_campaign"
mutate_json_artifact \
  "$missing_rocksdb_storage_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.sources.storage.support = "unsupported"
   | .sources.storage.path = null
   | .sampled_storage_peak_bytes = null
   | .samples |= map(.storage_bytes = null)'
expect_failure "missing RocksDB sampled storage" "$missing_rocksdb_storage_campaign"

narrow_rocksdb_storage_campaign="$scratch_directory/narrow-rocksdb-storage"
cp -R "$valid_campaign" "$narrow_rocksdb_storage_campaign"
mutate_json_artifact \
  "$narrow_rocksdb_storage_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.sources.storage.path = "/var/lib/zinder/benchmark-store"'
expect_failure "narrow RocksDB storage root" "$narrow_rocksdb_storage_campaign"

missing_postgres_storage_campaign="$scratch_directory/missing-postgres-storage"
cp -R "$valid_campaign" "$missing_postgres_storage_campaign"
mutate_json_artifact \
  "$missing_postgres_storage_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.sources.storage.support = "unsupported"
   | .sources.storage.path = null
   | .sampled_storage_peak_bytes = null
   | .samples |= map(.storage_bytes = null)'
expect_failure "missing PostgreSQL sampled storage" "$missing_postgres_storage_campaign"

narrow_postgres_storage_campaign="$scratch_directory/narrow-postgres-storage"
cp -R "$valid_campaign" "$narrow_postgres_storage_campaign"
mutate_json_artifact \
  "$narrow_postgres_storage_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.sources.storage.path = "/var/lib/postgresql/18/docker"'
expect_failure "narrow PostgreSQL storage root" "$narrow_postgres_storage_campaign"

uncovered_window_campaign="$scratch_directory/uncovered-window"
cp -R "$valid_campaign" "$uncovered_window_campaign"
mutate_json_artifact \
  "$uncovered_window_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.samples |= map(select(.observed_at_unix_millis < 111000))
   | .sampled_memory_current_peak_bytes = ([.samples[].memory_current_bytes] | max)
   | .sampled_storage_peak_bytes = ([.samples[].storage_bytes] | max)'
expect_failure "resource samples without report-window coverage" "$uncovered_window_campaign"

sample_gap_campaign="$scratch_directory/sample-gap"
cp -R "$valid_campaign" "$sample_gap_campaign"
mutate_json_artifact \
  "$sample_gap_campaign/rocksdb-canonical-replay-storage-client-trial-01.resources.json" \
  '.samples |= map(select(
      .observed_at_unix_millis < 110300
      or .observed_at_unix_millis > 110800
    ))
   | .sampled_memory_current_peak_bytes = ([.samples[].memory_current_bytes] | max)
   | .sampled_storage_peak_bytes = ([.samples[].storage_bytes] | max)'
expect_failure "resource report-window sample gap" "$sample_gap_campaign"

malformed_sample_campaign="$scratch_directory/malformed-sample"
cp -R "$valid_campaign" "$malformed_sample_campaign"
mutate_json_artifact \
  "$malformed_sample_campaign/postgres-canonical-replay-storage-client-trial-01.resources.json" \
  '.samples[2].memory_current_bytes = null'
expect_failure "malformed resource sample" "$malformed_sample_campaign"

within_trial_interval_campaign="$scratch_directory/within-trial-interval"
cp -R "$valid_campaign" "$within_trial_interval_campaign"
mutate_json_artifact \
  "$within_trial_interval_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.sample_interval_seconds = 0.2'
expect_failure "within-trial resource interval mismatch" "$within_trial_interval_campaign"

across_campaign_interval_campaign="$scratch_directory/across-campaign-interval"
cp -R "$valid_campaign" "$across_campaign_interval_campaign"
for evidence_path in \
  "$across_campaign_interval_campaign/rocksdb-canonical-replay-storage-client-trial-05.resources.json" \
  "$across_campaign_interval_campaign/postgres-canonical-replay-storage-client-trial-05.resources.json" \
  "$across_campaign_interval_campaign/postgres-canonical-replay-storage-database-trial-05.resources.json"; do
  mutate_json_artifact "$evidence_path" '.sample_interval_seconds = 0.2'
done
expect_failure "campaign resource interval mismatch" "$across_campaign_interval_campaign"

unalignable_campaign="$scratch_directory/unalignable"
cp -R "$valid_campaign" "$unalignable_campaign"
mutate_report \
  "$unalignable_campaign/postgres-canonical-replay-storage-trial-01.json" \
  '.provenance.run.completed_at_unix_millis = 112100'
mutate_json_artifact \
  "$unalignable_campaign/postgres-canonical-replay-storage-database-trial-01.resources.json" \
  '.samples |= map(select(.observed_at_unix_millis != 112040))'
expect_failure "unalignable PostgreSQL resource samples" "$unalignable_campaign"

write_lifecycle_evidence() {
  local directory="$1"
  local report_format_version="$2"
  local coverage="$3"
  local checkpoint_count="$4"
  local publication_mode="$5"
  local cold_validation_seconds="$6"
  mkdir -p "$directory"
  jq -n \
    --argjson report_format_version "$report_format_version" \
    --arg coverage "$coverage" \
    --argjson checkpoint_count "$checkpoint_count" \
    --arg publication_mode "$publication_mode" \
    --argjson cold_validation_seconds "$cold_validation_seconds" '
    def canonical_budget: {
      block_cache_bytes: 536870912,
      max_wal_bytes: 268435456,
      max_open_files: 512,
      write_buffer_bytes: 16777216,
      max_write_buffer_count: 2,
      max_background_jobs: 2,
      memtable_budget_bytes: 268435456,
      statistics_level: "tickers"
    };
    def wallet_budget: {
      block_cache_bytes: 268435456,
      max_wal_bytes: 33554432,
      max_open_files: 512,
      write_buffer_bytes: 16777216,
      max_write_buffer_count: 4,
      max_background_jobs: 2,
      memtable_budget_bytes: 536870912,
      statistics_level: "tickers"
    };
    def empty_sort($mem): {
      record_count: 0,
      initial_run_count: 0,
      merge_pass_count: 0,
      peak_accounted_sort_memory_bytes: 0,
      max_accounted_sort_memory_bytes: $mem,
      peak_temporary_file_bytes: 0,
      max_temporary_file_bytes: 206158430208,
      final_run_file_bytes: 0
    };
    {
      contract_identity: "benchmark-report",
      report_format_version: $report_format_version,
      measurement_kind: "rocksdb-storage-lifecycle",
      provenance: {
        benchmark_version: "0.22.0",
        software_revision: ("0" * 40),
        run: {
          trial_id: "trial-lifecycle-01",
          fixture_cache_policy: null,
          started_at_unix_millis: 100000,
          completed_at_unix_millis: 101000
        },
        runner: {
          id: "local-docker-desktop",
          cpu_limit_cores: 10,
          memory_limit_bytes: 10737418240,
          storage_class: "local-nvme"
        },
        image_reference: ("sha256:" + ("a" * 64)),
        target_os: "linux",
        target_arch: "x86_64"
      },
      storage_candidate: {
        id: "rocksdb-storage-lifecycle",
        canonical_engine: "rocksdb",
        canonical_model: "canonical-replay-storage",
        topology: "rocksdb"
      },
      source: {
        family: "zebra-json-rpc",
        network: "zcash-testnet",
        network_upgrade_activation_count: 11,
        network_upgrade_activations_fingerprint_version: 1,
        network_upgrade_activations_fingerprint_hex: ("b" * 64),
        source_tip_at_freeze: {height: 205, hash_hex: ("c" * 64)},
        fixed_build_tip: {height: 205, hash_hex: ("c" * 64)},
        source_tip_after_canonical_load: {height: 205, hash_hex: ("c" * 64)}
      },
      contracts: {
        canonical_store_identity: "canonical",
        canonical_store_schema_version: 2,
        wallet_store_identity: "wallet",
        wallet_store_schema_version: 1,
        wallet_projection_schema_version: 1,
        wallet_value_encoding_version: 1
      },
      resource_limits: (
        {
          request_timeout_seconds: 30,
          max_response_bytes: 67108864,
          source_segment_target_response_bytes: 33554432,
          source_segment_max_blocks: 64,
          source_fetch_max_in_flight_requests: 12,
          source_fetch_max_in_flight_bytes: 167772160,
          block_prepare_concurrency: 10,
          block_prepare_memory_watermark_bytes: 167772160,
          canonical_rocksdb: canonical_budget,
          wallet_rocksdb: wallet_budget,
          supported_reorg_depth: 100,
          wallet_max_outpoint_sort_memory_bytes: 4294967296,
          wallet_max_secondary_sort_memory_bytes_per_sorter: 1073741824,
          wallet_max_temporary_file_bytes_per_sorter: 206158430208,
          wallet_sst_target_logical_bytes: 134217728,
          wallet_max_accounted_reorg_undo_bytes: 536870912,
          tree_state_checkpoint_coverage: $coverage,
          canonical_publication_mode: $publication_mode
        }
      ),
      acceptance: {
        canonical_storage_ready: {
          scope: "canonical-storage-ready",
          wall_clock_seconds: 6.0,
          thresholds: null
        },
        wallet_storage_ready: {
          scope: "wallet-storage-ready",
          wall_clock_seconds: 3.0,
          thresholds: null
        }
      },
      phase_durations: {
        source_discovery_seconds: 0.5,
        canonical_store_initialization_seconds: 0.5,
        canonical_source_load_seconds: 2.0,
        canonical_cold_validation_seconds: $cold_validation_seconds,
        canonical_ready_publication_seconds: 0.5,
        canonical_cold_reopen_seconds: 0.5,
        wallet_build_seconds: 2.0,
        final_cold_reopen_seconds: 0.5,
        total_seconds: 9.5
      },
      canonical_storage_ready: (
        {
          scope: "canonical-storage-ready",
          first_retained_block: {height: 1, hash_hex: ("d" * 64)},
          visible_tip: {height: 205, hash_hex: ("c" * 64)},
          visible_epoch_id: 1,
          visible_event_sequence: 1,
          block_count: 205,
          transaction_count: 205,
          subtree_root_count: 0,
          replay_format_version: 1,
          sequence_digest: {
            block_digest_version: 1,
            sequence_digest_version: 1,
            block_count: 205,
            sha256: ("e" * 64)
          },
          logical_replay_bytes: 10000,
          logical_storage_bytes: 20000,
          sst_file_bytes: 12000,
          sst_file_count: 6,
          physical_store_bytes: 15000,
          database_io_mode: "buffered",
          source_tip_checkpoint_authenticated: true,
          cold_reopen_evidence_match: true,
          tree_state_checkpoint_count: $checkpoint_count
        }
      ),
      wallet_storage_ready: {
        scope: "wallet-storage-ready",
        source_epoch_id: 1,
        source_tip: {height: 205, hash_hex: ("c" * 64)},
        source_event_sequence: 1,
        source_sequence_digest: {
          block_digest_version: 1,
          sequence_digest_version: 1,
          block_count: 205,
          sha256: ("e" * 64)
        },
        wallet_projection_digest_hex: ("f" * 64),
        row_counts: {
          transparent_unspent_output_count: 0,
          transparent_unspent_output_by_address_count: 0,
          transparent_spent_output_count: 0,
          transparent_address_transaction_count: 0,
          transparent_address_balance_count: 0,
          reorg_undo_count: 100
        },
        utxo_summary: {
          utxo_count: 0,
          total_value_zat: 0,
          commitment_scheme: "lthash16-chacha12",
          commitment_accumulator_hex: ("00" * 2048),
          commitment_display_digest_hex: ("1" * 64)
        },
        scanned_block_count: 205,
        scanned_transaction_count: 205,
        historical_prevout_read_count: 0,
        construction: {
          outpoint_sort: empty_sort(4294967296),
          address_index_sort: empty_sort(1073741824),
          address_transaction_sort: empty_sort(1073741824),
          peak_accounted_reorg_undo_bytes: 1024,
          max_accounted_reorg_undo_bytes: 536870912,
          cold_validation_peak_accounted_reorg_undo_bytes: 1024,
          cold_validation_max_accounted_reorg_undo_bytes: 536870912,
          cold_validation_random_read_count: 0,
          logical_row_bytes: 2048,
          sst_file_bytes: 1024,
          sst_file_count: 2
        },
        phase_durations: {
          store_initialization_seconds: 0.1,
          canonical_scan_seconds: 0.2,
          outpoint_sort_seconds: 0.1,
          outpoint_merge_seconds: 0.1,
          secondary_row_derivation_seconds: 0.1,
          logical_evidence_seconds: 0.1,
          row_load_seconds: 0.1,
          flush_and_cold_reopen_seconds: 0.1,
          cold_validation_seconds: 0.2,
          cold_validation_primary_output_scan_seconds: 0.05,
          cold_validation_address_index_and_balance_readback_seconds: 0.05,
          cold_validation_address_transaction_readback_seconds: 0.05,
          cold_validation_reorg_undo_readback_seconds: 0.05,
          ready_publication_seconds: 0.1,
          total_seconds: 2.0
        },
        build_stages: [
          {stage: "store_initialization", seconds: 0.1},
          {stage: "canonical_scan", seconds: 0.2},
          {stage: "outpoint_run_spill", seconds: 0.05},
          {stage: "outpoint_sort", seconds: 0.1},
          {stage: "outpoint_drain", seconds: 0.05},
          {stage: "primary_sst_write", seconds: 0.05},
          {stage: "address_index_run_spill", seconds: 0.02},
          {stage: "address_transaction_run_spill", seconds: 0.03},
          {stage: "secondary_address_derivation", seconds: 0.08},
          {stage: "reorg_undo_write", seconds: 0.02},
          {stage: "logical_evidence", seconds: 0.1},
          {stage: "row_load", seconds: 0.1},
          {stage: "flush_and_close", seconds: 0.05},
          {stage: "cold_reopen", seconds: 0.05},
          {stage: "cold_validation_family_readback", seconds: 0.15},
          {stage: "cold_validation_reorg_undo_readback", seconds: 0.05},
          {stage: "ready_publication", seconds: 0.1}
        ],
        physical_store_bytes: 4096,
        cold_reopen_evidence_match: true,
        canonical_fence_match: true
      },
      benchmark_client_peak_rss: {
        bytes: 1048576,
        source: "proc_status_vmhwm"
      }
    }
  ' >"$directory/report.json"

  jq -n '
    {
      evidence_format_version: 1,
      measurement_kind: "container-resource-observation",
      component_id: "rocksdb-storage-lifecycle",
      trial_id: "trial-lifecycle-01",
      sample_interval_seconds: 1,
      started_at: "1970-01-01T00:00:00Z",
      started_at_unix_millis: 99000,
      completed_at: "1970-01-01T00:00:02Z",
      completed_at_unix_millis: 102000,
      child_exit_status: 0,
      peak_memory_bytes: 104857600,
      sampled_memory_current_peak_bytes: 100000000,
      sampled_storage_peak_bytes: 32768,
      sources: {
        cgroup_namespace: {
          support: "verified-private",
          kind: "proc-self-cgroup-v2",
          path: "/proc/self/cgroup"
        },
        memory_peak: {
          support: "exact",
          kind: "cgroup-v2-memory.peak",
          path: "/sys/fs/cgroup/memory.peak"
        },
        memory_current: {
          support: "exact",
          kind: "cgroup-v2-memory.current",
          path: "/sys/fs/cgroup/memory.current"
        },
        storage: {
          support: "sampled",
          kind: "du-allocated-kibibytes",
          path: "/var/lib/zinder"
        }
      },
      samples: [
        {observed_at_unix_millis: 99500, memory_current_bytes: 50000000, storage_bytes: 16384},
        {observed_at_unix_millis: 101500, memory_current_bytes: 100000000, storage_bytes: 32768}
      ]
    }
  ' >"$directory/resources.json"
}

validate_lifecycle_case() {
  local directory="$1"
  shift
  "$storage_lifecycle_validator" \
    "$directory/report.json" \
    "$directory/resources.json" \
    205 \
    "sha256:$(printf 'a%.0s' {1..64})" \
    "$(printf '0%.0s' {1..40})" \
    "trial-lifecycle-01" \
    "zcash-testnet" \
    10 \
    10737418240 \
    "local-docker-desktop" \
    "local-nvme" \
    "$@"
}

lifecycle_every_block="$scratch_directory/lifecycle-every-block"
write_lifecycle_evidence "$lifecycle_every_block" 1 "every-block" 206 "trusted-fresh" 0
validate_lifecycle_case "$lifecycle_every_block" >/dev/null \
  || fail "valid v1 every-block trusted-fresh lifecycle report was rejected without explicit args"
validate_lifecycle_case "$lifecycle_every_block" "every-block" >/dev/null \
  || fail "valid v1 every-block trusted-fresh lifecycle report was rejected with matching coverage arg"
validate_lifecycle_case "$lifecycle_every_block" "every-block" "trusted-fresh" >/dev/null \
  || fail "valid v1 every-block trusted-fresh lifecycle report was rejected with matching mode arg"
if validate_lifecycle_case "$lifecycle_every_block" "cadence-and-tip" >/dev/null 2>&1; then
  fail "every-block lifecycle report was accepted when cadence-and-tip was expected"
fi
if validate_lifecycle_case "$lifecycle_every_block" "every-block" "cold-certified" >/dev/null 2>&1; then
  fail "trusted-fresh lifecycle report was accepted when cold-certified was expected"
fi

lifecycle_sparse="$scratch_directory/lifecycle-sparse"
write_lifecycle_evidence "$lifecycle_sparse" 1 "cadence-and-tip" 4 "trusted-fresh" 0
validate_lifecycle_case "$lifecycle_sparse" >/dev/null \
  || fail "valid v1 cadence-and-tip trusted-fresh lifecycle report was rejected without explicit args"
validate_lifecycle_case "$lifecycle_sparse" "cadence-and-tip" >/dev/null \
  || fail "valid v1 cadence-and-tip trusted-fresh lifecycle report was rejected with matching coverage arg"
validate_lifecycle_case "$lifecycle_sparse" "cadence-and-tip" "trusted-fresh" >/dev/null \
  || fail "valid v1 cadence-and-tip trusted-fresh lifecycle report was rejected with matching mode arg"
if validate_lifecycle_case "$lifecycle_sparse" "every-block" >/dev/null 2>&1; then
  fail "cadence-and-tip lifecycle report was accepted when every-block was expected"
fi

lifecycle_cold_certified="$scratch_directory/lifecycle-cold-certified"
write_lifecycle_evidence "$lifecycle_cold_certified" 1 "every-block" 206 "cold-certified" 1.0
validate_lifecycle_case "$lifecycle_cold_certified" >/dev/null \
  || fail "valid v1 cold-certified lifecycle report was rejected without explicit args"
validate_lifecycle_case "$lifecycle_cold_certified" "every-block" >/dev/null \
  || fail "valid v1 cold-certified lifecycle report was rejected with matching coverage arg"
validate_lifecycle_case "$lifecycle_cold_certified" "every-block" "cold-certified" >/dev/null \
  || fail "valid v1 cold-certified lifecycle report was rejected with matching mode arg"
if validate_lifecycle_case "$lifecycle_cold_certified" "every-block" "trusted-fresh" >/dev/null 2>&1; then
  fail "cold-certified lifecycle report was accepted when trusted-fresh was expected"
fi

lifecycle_trusted_nonzero_cold="$scratch_directory/lifecycle-trusted-nonzero-cold"
write_lifecycle_evidence "$lifecycle_trusted_nonzero_cold" 1 "every-block" 206 "trusted-fresh" 1.0
if validate_lifecycle_case "$lifecycle_trusted_nonzero_cold" >/dev/null 2>&1; then
  fail "v1 trusted-fresh lifecycle report with nonzero canonical_cold_validation_seconds was accepted"
fi
if validate_lifecycle_case "$lifecycle_trusted_nonzero_cold" "every-block" "trusted-fresh" >/dev/null 2>&1; then
  fail "v1 trusted-fresh lifecycle report with nonzero canonical_cold_validation_seconds was accepted with explicit mode"
fi

lifecycle_invalid_mode="$scratch_directory/lifecycle-invalid-mode"
write_lifecycle_evidence "$lifecycle_invalid_mode" 1 "every-block" 206 "unknown-mode" 0
if validate_lifecycle_case "$lifecycle_invalid_mode" >/dev/null 2>&1; then
  fail "v1 lifecycle report with invalid canonical_publication_mode was accepted"
fi

lifecycle_missing_mode="$scratch_directory/lifecycle-missing-mode"
write_lifecycle_evidence "$lifecycle_missing_mode" 1 "every-block" 206 "trusted-fresh" 0
mutate_report "$lifecycle_missing_mode/report.json" 'del(.resource_limits.canonical_publication_mode)'
if validate_lifecycle_case "$lifecycle_missing_mode" >/dev/null 2>&1; then
  fail "v1 lifecycle report missing canonical_publication_mode was accepted"
fi

lifecycle_mismatched_count="$scratch_directory/lifecycle-mismatched-count"
write_lifecycle_evidence "$lifecycle_mismatched_count" 1 "every-block" 4 "trusted-fresh" 0
if validate_lifecycle_case "$lifecycle_mismatched_count" "every-block" >/dev/null 2>&1; then
  fail "every-block lifecycle report with sparse checkpoint count was accepted"
fi

lifecycle_missing_count="$scratch_directory/lifecycle-missing-count"
write_lifecycle_evidence "$lifecycle_missing_count" 1 "every-block" 206 "trusted-fresh" 0
mutate_report "$lifecycle_missing_count/report.json" 'del(.canonical_storage_ready.tree_state_checkpoint_count)'
if validate_lifecycle_case "$lifecycle_missing_count" >/dev/null 2>&1; then
  fail "v1 lifecycle report omitting tree_state_checkpoint_count was accepted"
fi

expect_lifecycle_rejected() {
  local name="$1" mutation="$2"
  local directory="$scratch_directory/lifecycle-$name"
  write_lifecycle_evidence "$directory" 1 "every-block" 206 "trusted-fresh" 0
  mutate_report "$directory/report.json" "$mutation"
  if validate_lifecycle_case "$directory" >/dev/null 2>&1; then
    fail "lifecycle report $name was accepted"
  fi
}
expect_lifecycle_rejected wrong-canonical-store-schema '.contracts.canonical_store_schema_version = 1'
expect_lifecycle_rejected wrong-store-schema '.contracts.wallet_store_schema_version = 2'
expect_lifecycle_rejected wrong-commitment-scheme '.wallet_storage_ready.utxo_summary.commitment_scheme = "lthash16"'
expect_lifecycle_rejected wrong-projection-schema '.contracts.wallet_projection_schema_version = 2'
expect_lifecycle_rejected legacy-diagnostic-projection-engine '.storage_candidate.diagnostic_projection_engine = null'
expect_lifecycle_rejected missing-build-stages 'del(.wallet_storage_ready.build_stages)'
expect_lifecycle_rejected reordered-build-stages '.wallet_storage_ready.build_stages |= (.[0:1] + [.[2], .[1]] + .[3:])'
expect_lifecycle_rejected unknown-build-stage '.wallet_storage_ready.build_stages[0].stage = "unexpected_stage"'
expect_lifecycle_rejected excess-build-stages-total '.wallet_storage_ready.build_stages[0].seconds = 5.0'
expect_lifecycle_rejected legacy-wallet-wal-budget '.resource_limits.wallet_rocksdb.max_wal_bytes = 268435456'
expect_lifecycle_rejected legacy-canonical-cache-budget '.resource_limits.canonical_rocksdb.block_cache_bytes = 67108864'
expect_lifecycle_rejected legacy-sorter-temp-budget '.resource_limits.wallet_max_temporary_file_bytes_per_sorter = 68719476736 | .wallet_storage_ready.construction.outpoint_sort.max_temporary_file_bytes = 68719476736 | .wallet_storage_ready.construction.address_index_sort.max_temporary_file_bytes = 68719476736 | .wallet_storage_ready.construction.address_transaction_sort.max_temporary_file_bytes = 68719476736'
for obsolete_version in 4 5 6; do
  expect_lifecycle_rejected "obsolete-version-$obsolete_version" ".report_format_version = $obsolete_version"
done

printf 'storage benchmark campaign validator tests passed\n'
