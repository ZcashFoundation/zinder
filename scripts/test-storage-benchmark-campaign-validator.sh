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
[[ "$producer_report_format_version" =~ ^[1-9][0-9]*$ ]] ||
  fail "could not resolve the producer report format version"
producer_fixture_format_version="$(sed -n \
  's/^pub const FIXTURE_FORMAT_VERSION: u32 = \([0-9][0-9]*\);$/\1/p' \
  "$repository_root/services/zinder-bench/src/fixture.rs")"
[[ "$producer_fixture_format_version" == "3" ]] ||
  fail "producer fixture format version must be the exact v3 campaign contract"
for report_validator in "$validator" "$storage_lifecycle_validator"; do
  grep -Fq ".report_format_version == $producer_report_format_version" \
    "$report_validator" ||
    fail "$(basename "$report_validator") does not require producer report format $producer_report_format_version"
done
grep -Fq ".fixture.fixture_format_version == $producer_fixture_format_version" \
  "$validator" ||
  fail "campaign validator does not require producer fixture format $producer_fixture_format_version"

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
        report_format_version: 4,
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
          fixture_format_version: 3,
          canonical_artifact_schema_version: 18,
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
          topology: (if $is_rocksdb then "rocksdb-single-host" else "postgres-scale-out" end)
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
  '.storage_candidate.topology = "rocksdb-single-host"'
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

printf 'storage benchmark campaign validator tests passed\n'
