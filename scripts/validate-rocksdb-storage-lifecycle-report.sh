#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo >&2 "RocksDB storage lifecycle evidence rejected: $*"
  exit 1
}

usage() {
  echo >&2 \
    "usage: $0 REPORT RESOURCE_EVIDENCE EXPECTED_TIP EXPECTED_IMAGE EXPECTED_REVISION EXPECTED_TRIAL EXPECTED_NETWORK EXPECTED_CPU_LIMIT_CORES EXPECTED_MEMORY_LIMIT_BYTES EXPECTED_RUNNER_ID EXPECTED_STORAGE_CLASS"
  exit 2
}

[[ "$#" -eq 11 ]] || usage

report_path="$1"
resource_evidence_path="$2"
expected_tip_height="$3"
expected_image_reference="$4"
expected_software_revision="$5"
expected_trial_id="$6"
expected_network="$7"
expected_cpu_limit_cores="$8"
expected_memory_limit_bytes="$9"
expected_runner_id="${10}"
expected_storage_class="${11}"

command -v jq >/dev/null 2>&1 || fail "jq is required"
for evidence_file in "$report_path" "$resource_evidence_path"; do
  [[ -f "$evidence_file" && ! -L "$evidence_file" && -s "$evidence_file" ]] || fail \
    "evidence must be a non-empty regular file, not a symlink: $evidence_file"
done
[[ "$expected_tip_height" =~ ^[1-9][0-9]*$ && "$expected_tip_height" -le 4294967295 ]] || fail \
  "expected tip must be a nonzero u32"
[[ "$expected_image_reference" =~ ^sha256:[0-9a-f]{64}$ ]] || fail \
  "expected image must be an immutable Docker image ID"
[[ "$expected_software_revision" =~ ^[0-9a-f]{40}$|^[0-9a-f]{64}$ ]] || fail \
  "expected software revision must be a full hexadecimal object ID"
[[ "$expected_trial_id" =~ ^[[:alnum:]][[:alnum:]._-]*$ ]] || fail \
  "expected trial ID is not a valid evidence identifier"
[[ "$expected_network" == "zcash-testnet" ]] || fail \
  "this validator accepts only the testnet lifecycle contract"
[[ "$expected_cpu_limit_cores" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail \
  "expected CPU limit must be a positive number"
[[ "$expected_memory_limit_bytes" =~ ^[1-9][0-9]*$ ]] || fail \
  "expected memory limit must be a positive integer"
[[ "$expected_runner_id" =~ ^[[:alnum:]][[:alnum:]._-]*$ ]] || fail \
  "expected runner ID is not a valid evidence identifier"
[[ "$expected_storage_class" =~ ^[[:alnum:]][[:alnum:]._-]*$ ]] || fail \
  "expected storage class is not a valid evidence identifier"

jq -e \
  --slurpfile resources "$resource_evidence_path" \
  --argjson expected_tip_height "$expected_tip_height" \
  --arg expected_image_reference "$expected_image_reference" \
  --arg expected_software_revision "$expected_software_revision" \
  --arg expected_trial_id "$expected_trial_id" \
  --arg expected_network "$expected_network" \
  --argjson expected_cpu_limit_cores "$expected_cpu_limit_cores" \
  --argjson expected_memory_limit_bytes "$expected_memory_limit_bytes" \
  --arg expected_runner_id "$expected_runner_id" \
  --arg expected_storage_class "$expected_storage_class" '
  def exact_keys($expected): (keys | sort) == ($expected | sort);
  def nonnegative_integer:
    type == "number" and . >= 0 and . == floor;
  def positive_integer:
    nonnegative_integer and . > 0;
  def positive_number:
    type == "number" and . > 0;
  def clamp($value; $minimum; $maximum):
    [$maximum, ([$minimum, $value] | max)] | min;
  def duration:
    type == "number" and . >= 0;
  def hex_bytes($bytes):
    type == "string" and length == ($bytes * 2) and test("^[0-9a-f]+$");
  def utc_timestamp:
    type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$");
  def block_id:
    exact_keys(["hash_hex", "height"])
    and (.height | nonnegative_integer)
    and (.hash_hex | hex_bytes(32));
  def sequence_digest:
    exact_keys(["block_count", "block_digest_version", "sequence_digest_version", "sha256"])
    and .block_digest_version == 1
    and .sequence_digest_version == 1
    and (.block_count | positive_integer)
    and (.sha256 | hex_bytes(32));
  def resource_budget:
    exact_keys([
      "block_cache_bytes",
      "max_background_jobs",
      "max_open_files",
      "max_wal_bytes",
      "max_write_buffer_count",
      "memtable_budget_bytes",
      "statistics_level",
      "write_buffer_bytes"
    ])
    and (.block_cache_bytes | positive_integer)
    and (.max_wal_bytes | positive_integer)
    and (.max_open_files | positive_integer)
    and (.write_buffer_bytes | positive_integer)
    and (.max_write_buffer_count | positive_integer)
    and (.max_background_jobs | positive_integer)
    and (.memtable_budget_bytes | positive_integer)
    and .statistics_level == "tickers";
  def thresholds($wall_clock_seconds):
    exact_keys([
      "hard_limit_met",
      "hard_limit_seconds",
      "target_met",
      "target_seconds"
    ])
    and (.target_seconds | positive_number)
    and (.hard_limit_seconds | positive_number)
    and .target_seconds <= .hard_limit_seconds
    and (.target_met | type == "boolean")
    and (.hard_limit_met | type == "boolean")
    and .target_met == ($wall_clock_seconds <= .target_seconds)
    and .hard_limit_met == ($wall_clock_seconds <= .hard_limit_seconds)
    and .hard_limit_met;
  def acceptance($scope):
    exact_keys(["scope", "thresholds", "wall_clock_seconds"])
    and .scope == $scope
    and (.wall_clock_seconds | duration)
    and (.wall_clock_seconds as $wall_clock_seconds
      | .thresholds == null or (.thresholds | thresholds($wall_clock_seconds)));
  def sort_evidence($memory_limit; $temporary_file_limit):
    exact_keys([
      "final_run_file_bytes",
      "initial_run_count",
      "max_accounted_sort_memory_bytes",
      "max_temporary_file_bytes",
      "merge_pass_count",
      "peak_accounted_sort_memory_bytes",
      "peak_temporary_file_bytes",
      "record_count"
    ])
    and ([
      .record_count,
      .initial_run_count,
      .merge_pass_count,
      .peak_accounted_sort_memory_bytes,
      .max_accounted_sort_memory_bytes,
      .peak_temporary_file_bytes,
      .max_temporary_file_bytes,
      .final_run_file_bytes
    ] | all(.[]; nonnegative_integer))
    and .max_accounted_sort_memory_bytes == $memory_limit
    and .peak_accounted_sort_memory_bytes <= .max_accounted_sort_memory_bytes
    and .max_temporary_file_bytes == $temporary_file_limit
    and .peak_temporary_file_bytes <= .max_temporary_file_bytes
    and .final_run_file_bytes <= .peak_temporary_file_bytes
    and .initial_run_count <= .record_count
    and (
      if .record_count == 0 then
        .initial_run_count == 0
        and .merge_pass_count == 0
        and .peak_accounted_sort_memory_bytes == 0
        and .peak_temporary_file_bytes == 0
        and .final_run_file_bytes == 0
      else
        .initial_run_count > 0 and .final_run_file_bytes > 0
      end
    );
  def row_counts:
    exact_keys([
      "reorg_undo_count",
      "transparent_address_balance_count",
      "transparent_address_transaction_count",
      "transparent_spent_output_count",
      "transparent_unspent_output_by_address_count",
      "transparent_unspent_output_count"
    ])
    and ([.[]] | all(.[]; nonnegative_integer));
  def wallet_phase_durations:
    exact_keys([
      "canonical_scan_seconds",
      "cold_validation_address_index_and_balance_readback_seconds",
      "cold_validation_address_transaction_readback_seconds",
      "cold_validation_primary_output_scan_seconds",
      "cold_validation_reorg_undo_readback_seconds",
      "cold_validation_seconds",
      "flush_and_cold_reopen_seconds",
      "logical_evidence_seconds",
      "outpoint_merge_seconds",
      "outpoint_sort_seconds",
      "ready_publication_seconds",
      "row_load_seconds",
      "secondary_row_derivation_seconds",
      "store_initialization_seconds",
      "total_seconds"
    ])
    and ([.[]] | all(.[]; duration))
    and .total_seconds >= (
      .store_initialization_seconds
      + .canonical_scan_seconds
      + .outpoint_sort_seconds
      + .outpoint_merge_seconds
      + .secondary_row_derivation_seconds
      + .logical_evidence_seconds
      + .row_load_seconds
      + .flush_and_cold_reopen_seconds
      + .cold_validation_seconds
      + .ready_publication_seconds
    )
    and (.cold_validation_seconds as $cold_validation
      | [
          .cold_validation_primary_output_scan_seconds,
          .cold_validation_address_index_and_balance_readback_seconds,
          .cold_validation_address_transaction_readback_seconds,
          .cold_validation_reorg_undo_readback_seconds
        ]
      | all(. <= $cold_validation));
  def report_phase_durations:
    exact_keys([
      "canonical_cold_reopen_seconds",
      "canonical_cold_validation_seconds",
      "canonical_ready_publication_seconds",
      "canonical_source_load_seconds",
      "canonical_store_initialization_seconds",
      "final_cold_reopen_seconds",
      "source_discovery_seconds",
      "total_seconds",
      "wallet_build_seconds"
    ])
    and ([.[]] | all(.[]; duration))
    and .total_seconds >= (
      .source_discovery_seconds
      + .canonical_store_initialization_seconds
      + .canonical_source_load_seconds
      + .canonical_cold_validation_seconds
      + .canonical_ready_publication_seconds
      + .canonical_cold_reopen_seconds
      + .wallet_build_seconds
      + .final_cold_reopen_seconds
    );
  def sample:
    exact_keys(["memory_current_bytes", "observed_at_unix_millis", "storage_bytes"])
    and (.observed_at_unix_millis | positive_integer)
    and (.memory_current_bytes | nonnegative_integer)
    and (.storage_bytes | nonnegative_integer);

  . as $report
  | ($resources | length) == 1
  and ($resources[0] as $resource
    | $report
    | exact_keys([
      "acceptance",
      "benchmark_client_peak_rss",
      "canonical_storage_ready",
      "contract_identity",
      "contracts",
      "measurement_kind",
      "phase_durations",
      "provenance",
      "report_format_version",
      "resource_limits",
      "source",
      "storage_candidate",
      "wallet_storage_ready"
    ])
    and .contract_identity == "benchmark-report"
    and .report_format_version == 4
    and .measurement_kind == "rocksdb-storage-lifecycle"
    and (.storage_candidate
      | exact_keys([
          "canonical_engine",
          "canonical_model",
          "diagnostic_projection_engine",
          "id",
          "topology"
        ])
      and .id == "rocksdb-storage-lifecycle"
      and .canonical_engine == "rocksdb"
      and .canonical_model == "canonical-replay-storage"
      and .diagnostic_projection_engine == null
      and .topology == "rocksdb-single-host")
    and (.provenance
      | exact_keys([
          "benchmark_version",
          "image_reference",
          "run",
          "runner",
          "software_revision",
          "target_arch",
          "target_os"
        ])
      and (.benchmark_version | type == "string" and length > 0)
      and .software_revision == $expected_software_revision
      and .image_reference == $expected_image_reference
      and .target_os == "linux"
      and (.target_arch | type == "string" and length > 0)
      and (.run
        | exact_keys([
            "completed_at_unix_millis",
            "fixture_cache_policy",
            "started_at_unix_millis",
            "trial_id"
          ])
        and .trial_id == $expected_trial_id
        and .fixture_cache_policy == null
        and (.started_at_unix_millis | positive_integer)
        and (.completed_at_unix_millis | positive_integer)
        and .completed_at_unix_millis >= .started_at_unix_millis)
      and (.runner
        | exact_keys(["cpu_limit_cores", "id", "memory_limit_bytes", "storage_class"])
        and .id == $expected_runner_id
        and (.cpu_limit_cores | positive_number)
        and .cpu_limit_cores == $expected_cpu_limit_cores
        and (.memory_limit_bytes | positive_integer)
        and .memory_limit_bytes == $expected_memory_limit_bytes
        and .storage_class == $expected_storage_class))
    and (.source
      | exact_keys([
          "family",
          "fixed_build_tip",
          "network",
          "network_upgrade_activation_count",
          "network_upgrade_activations_fingerprint_hex",
          "network_upgrade_activations_fingerprint_version",
          "source_tip_after_canonical_load",
          "source_tip_at_freeze"
        ])
      and .family == "zebra-json-rpc"
      and .network == $expected_network
      and (.network_upgrade_activation_count | positive_integer)
      and .network_upgrade_activations_fingerprint_version == 1
      and (.network_upgrade_activations_fingerprint_hex | hex_bytes(32))
      and (.source_tip_at_freeze | block_id)
      and (.fixed_build_tip | block_id)
      and (.source_tip_after_canonical_load | block_id)
      and .fixed_build_tip.height == $expected_tip_height
      and .source_tip_at_freeze.height >= .fixed_build_tip.height
      and .source_tip_after_canonical_load.height >= .fixed_build_tip.height)
    and (.contracts
      | exact_keys([
          "canonical_store_identity",
          "canonical_store_schema_version",
          "wallet_projection_schema_version",
          "wallet_store_identity",
          "wallet_store_schema_version",
          "wallet_value_encoding_version"
        ])
      and .canonical_store_identity == "canonical"
      and .canonical_store_schema_version == 4
      and .wallet_store_identity == "wallet"
      and .wallet_store_schema_version == 1
      and .wallet_projection_schema_version == 1
      and .wallet_value_encoding_version == 2)
    and (.resource_limits
      | exact_keys([
          "block_prepare_concurrency",
          "block_prepare_memory_watermark_bytes",
          "canonical_rocksdb",
          "max_response_bytes",
          "request_timeout_seconds",
          "source_fetch_max_in_flight_bytes",
          "source_fetch_max_in_flight_requests",
          "source_segment_max_blocks",
          "source_segment_target_response_bytes",
          "supported_reorg_depth",
          "wallet_max_accounted_reorg_undo_bytes",
          "wallet_max_outpoint_sort_memory_bytes",
          "wallet_max_secondary_sort_memory_bytes_per_sorter",
          "wallet_max_temporary_file_bytes_per_sorter",
          "wallet_rocksdb",
          "wallet_sst_target_logical_bytes"
        ])
      and ([
        .request_timeout_seconds,
        .max_response_bytes,
        .source_segment_target_response_bytes,
        .source_segment_max_blocks,
        .source_fetch_max_in_flight_requests,
        .source_fetch_max_in_flight_bytes,
        .block_prepare_concurrency,
        .block_prepare_memory_watermark_bytes,
        .supported_reorg_depth,
        .wallet_max_outpoint_sort_memory_bytes,
        .wallet_max_secondary_sort_memory_bytes_per_sorter,
        .wallet_max_temporary_file_bytes_per_sorter,
        .wallet_sst_target_logical_bytes,
        .wallet_max_accounted_reorg_undo_bytes
      ] | all(.[]; positive_integer))
      and .source_segment_target_response_bytes == ([.max_response_bytes, 33554432] | min)
      and .source_segment_max_blocks == 64
      and .source_fetch_max_in_flight_requests == 12
      and .source_fetch_max_in_flight_bytes == ([
        .max_response_bytes,
        clamp((($report.provenance.runner.memory_limit_bytes / 64) | floor); 134217728; 402653184)
      ] | max)
      and .block_prepare_concurrency == ([
        16,
        ($report.provenance.runner.cpu_limit_cores | ceil)
      ] | min)
      and .block_prepare_memory_watermark_bytes == clamp(
        (($report.provenance.runner.memory_limit_bytes / 64) | floor);
        134217728;
        536870912
      )
      and (.canonical_rocksdb | resource_budget)
      and (.wallet_rocksdb | resource_budget))
    and (.acceptance
      | exact_keys(["canonical_storage_ready", "wallet_storage_ready"])
      and (.canonical_storage_ready | acceptance("canonical-storage-ready"))
      and (.wallet_storage_ready | acceptance("wallet-storage-ready")))
    and (.phase_durations | report_phase_durations)
    and (.canonical_storage_ready
      | exact_keys([
          "block_count",
          "cold_reopen_evidence_match",
          "database_io_mode",
          "first_retained_block",
          "logical_replay_bytes",
          "logical_storage_bytes",
          "physical_store_bytes",
          "replay_format_version",
          "scope",
          "sequence_digest",
          "source_tip_checkpoint_authenticated",
          "sst_file_bytes",
          "sst_file_count",
          "subtree_root_count",
          "transaction_count",
          "visible_epoch_id",
          "visible_event_sequence",
          "visible_tip",
          "workload"
        ])
      and .scope == "canonical-storage-ready"
      and .workload == "wallet"
      and (.first_retained_block | block_id)
      and .first_retained_block.height == 1
      and (.visible_tip | block_id)
      and .visible_tip == $report.source.fixed_build_tip
      and .visible_epoch_id == 1
      and .visible_event_sequence == 1
      and (.block_count | positive_integer)
      and .block_count == .visible_tip.height
      and (.transaction_count | positive_integer)
      and (.subtree_root_count | nonnegative_integer)
      and .replay_format_version == 1
      and (.sequence_digest | sequence_digest)
      and .sequence_digest.block_count == .block_count
      and (.logical_replay_bytes | positive_integer)
      and (.logical_storage_bytes | positive_integer)
      and (.sst_file_bytes | positive_integer)
      and (.sst_file_count | positive_integer)
      and (.physical_store_bytes | positive_integer)
      and (.database_io_mode | type == "string" and length > 0)
      and .source_tip_checkpoint_authenticated == true
      and .cold_reopen_evidence_match == true)
    and (.wallet_storage_ready
      | exact_keys([
          "canonical_fence_match",
          "cold_reopen_evidence_match",
          "construction",
          "historical_prevout_read_count",
          "phase_durations",
          "physical_store_bytes",
          "wallet_projection_digest_hex",
          "row_counts",
          "scanned_block_count",
          "scanned_transaction_count",
          "scope",
          "source_epoch_id",
          "source_event_sequence",
          "source_sequence_digest",
          "source_tip",
          "utxo_summary"
        ])
      and .scope == "wallet-storage-ready"
      and .source_epoch_id == $report.canonical_storage_ready.visible_epoch_id
      and .source_event_sequence == $report.canonical_storage_ready.visible_event_sequence
      and .source_tip == $report.canonical_storage_ready.visible_tip
      and .source_sequence_digest == $report.canonical_storage_ready.sequence_digest
      and (.wallet_projection_digest_hex | hex_bytes(32))
      and (.row_counts | row_counts)
      and .row_counts.transparent_unspent_output_by_address_count
        == .row_counts.transparent_unspent_output_count
      and .row_counts.transparent_address_balance_count
        <= .row_counts.transparent_unspent_output_count
      and .row_counts.transparent_address_transaction_count
        <= (.row_counts.transparent_unspent_output_count
          + (2 * .row_counts.transparent_spent_output_count))
      and .row_counts.reorg_undo_count
        == ([.scanned_block_count, $report.resource_limits.supported_reorg_depth] | min)
      and (.utxo_summary
        | exact_keys([
            "commitment_accumulator_hex",
            "commitment_display_digest_hex",
            "commitment_scheme",
            "total_value_zat",
            "utxo_count"
          ])
        and (.utxo_count | nonnegative_integer)
        and .utxo_count == $report.wallet_storage_ready.row_counts.transparent_unspent_output_count
        and (.total_value_zat | nonnegative_integer)
        and .commitment_scheme == "lthash16"
        and (.commitment_accumulator_hex | hex_bytes(2048))
        and (.commitment_display_digest_hex | hex_bytes(32)))
      and .scanned_block_count == $report.canonical_storage_ready.block_count
      and .scanned_transaction_count == $report.canonical_storage_ready.transaction_count
      and .historical_prevout_read_count == 0
      and (.construction
        | exact_keys([
            "address_index_sort",
            "address_transaction_sort",
            "cold_validation_max_accounted_reorg_undo_bytes",
            "cold_validation_peak_accounted_reorg_undo_bytes",
            "cold_validation_random_read_count",
            "logical_row_bytes",
            "max_accounted_reorg_undo_bytes",
            "outpoint_sort",
            "peak_accounted_reorg_undo_bytes",
            "sst_file_bytes",
            "sst_file_count"
          ])
        and (.outpoint_sort
          | sort_evidence(
              $report.resource_limits.wallet_max_outpoint_sort_memory_bytes;
              $report.resource_limits.wallet_max_temporary_file_bytes_per_sorter
            ))
        and (.address_index_sort
          | sort_evidence(
              $report.resource_limits.wallet_max_secondary_sort_memory_bytes_per_sorter;
              $report.resource_limits.wallet_max_temporary_file_bytes_per_sorter
            ))
        and (.address_transaction_sort
          | sort_evidence(
              $report.resource_limits.wallet_max_secondary_sort_memory_bytes_per_sorter;
              $report.resource_limits.wallet_max_temporary_file_bytes_per_sorter
            ))
        and .outpoint_sort.record_count
          == ($report.wallet_storage_ready.row_counts.transparent_unspent_output_count
            + (2 * $report.wallet_storage_ready.row_counts.transparent_spent_output_count))
        and .address_index_sort.record_count
          == $report.wallet_storage_ready.row_counts.transparent_unspent_output_count
        and .address_transaction_sort.record_count == .outpoint_sort.record_count
        and .max_accounted_reorg_undo_bytes
          == $report.resource_limits.wallet_max_accounted_reorg_undo_bytes
        and (.peak_accounted_reorg_undo_bytes | nonnegative_integer)
        and .peak_accounted_reorg_undo_bytes <= .max_accounted_reorg_undo_bytes
        and .cold_validation_max_accounted_reorg_undo_bytes
          == $report.resource_limits.wallet_max_accounted_reorg_undo_bytes
        and (.cold_validation_peak_accounted_reorg_undo_bytes | nonnegative_integer)
        and .cold_validation_peak_accounted_reorg_undo_bytes
          <= .cold_validation_max_accounted_reorg_undo_bytes
        and .cold_validation_random_read_count == 0
        and (.logical_row_bytes | positive_integer)
        and (.sst_file_bytes | positive_integer)
        and (.sst_file_count | positive_integer))
      and (.phase_durations | wallet_phase_durations)
      and .phase_durations.total_seconds == $report.phase_durations.wallet_build_seconds
      and (.physical_store_bytes | positive_integer)
      and .cold_reopen_evidence_match == true
      and .canonical_fence_match == true)
    and (.benchmark_client_peak_rss
      | exact_keys(["bytes", "source"])
      and (.bytes | positive_integer)
      and .bytes <= $report.provenance.runner.memory_limit_bytes
      and .source == "proc_status_vmhwm")
    and .acceptance.canonical_storage_ready.wall_clock_seconds >= (
      .phase_durations.source_discovery_seconds
      + .phase_durations.canonical_store_initialization_seconds
      + .phase_durations.canonical_source_load_seconds
      + .phase_durations.canonical_cold_validation_seconds
      + .phase_durations.canonical_ready_publication_seconds
      + .phase_durations.canonical_cold_reopen_seconds
    )
    and .acceptance.wallet_storage_ready.wall_clock_seconds >= (
      .phase_durations.wallet_build_seconds
      + .phase_durations.final_cold_reopen_seconds
    )
    and .phase_durations.total_seconds >= (
      .acceptance.canonical_storage_ready.wall_clock_seconds
      + .acceptance.wallet_storage_ready.wall_clock_seconds
    )
    # Phase durations come from a monotonic clock. Unix timestamps are
    # provenance and resource-correlation boundaries; wall-clock adjustments
    # make arithmetic comparisons between those two clock domains invalid.
    and ($resource
      | exact_keys([
          "child_exit_status",
          "completed_at",
          "completed_at_unix_millis",
          "component_id",
          "evidence_format_version",
          "measurement_kind",
          "peak_memory_bytes",
          "sample_interval_seconds",
          "sampled_memory_current_peak_bytes",
          "sampled_storage_peak_bytes",
          "samples",
          "sources",
          "started_at",
          "started_at_unix_millis",
          "trial_id"
        ])
      and .evidence_format_version == 1
      and .measurement_kind == "container-resource-observation"
      and .component_id == "rocksdb-storage-lifecycle"
      and .trial_id == $expected_trial_id
      and (.sample_interval_seconds | positive_number)
      and (.started_at | utc_timestamp)
      and (.completed_at | utc_timestamp)
      and (.started_at_unix_millis | positive_integer)
      and (.completed_at_unix_millis | positive_integer)
      and .completed_at_unix_millis >= .started_at_unix_millis
      and .started_at_unix_millis <= $report.provenance.run.started_at_unix_millis
      and .completed_at_unix_millis >= $report.provenance.run.completed_at_unix_millis
      and .child_exit_status == 0
      and (.peak_memory_bytes | positive_integer)
      and (.sampled_memory_current_peak_bytes | positive_integer)
      and .sampled_memory_current_peak_bytes <= .peak_memory_bytes
      and .peak_memory_bytes <= $report.provenance.runner.memory_limit_bytes
      and (.sampled_storage_peak_bytes | positive_integer)
      and .sampled_storage_peak_bytes >= (
        $report.canonical_storage_ready.physical_store_bytes
        + $report.wallet_storage_ready.physical_store_bytes
      )
      and (.sources
        | exact_keys(["cgroup_namespace", "memory_current", "memory_peak", "storage"])
        and (.cgroup_namespace
          | exact_keys(["kind", "path", "support"])
          and .support == "verified-private"
          and .kind == "proc-self-cgroup-v2"
          and .path == "/proc/self/cgroup")
        and (.memory_peak
          | exact_keys(["kind", "path", "support"])
          and .support == "exact"
          and .kind == "cgroup-v2-memory.peak"
          and .path == "/sys/fs/cgroup/memory.peak")
        and (.memory_current
          | exact_keys(["kind", "path", "support"])
          and .support == "exact"
          and .kind == "cgroup-v2-memory.current"
          and .path == "/sys/fs/cgroup/memory.current")
        and (.storage
          | exact_keys(["kind", "path", "support"])
          and .support == "sampled"
          and .kind == "du-allocated-kibibytes"
          and .path == "/var/lib/zinder"))
      and (.samples | type == "array" and length >= 2)
      and (.samples | all(.[]; sample))
      and ([.samples[].observed_at_unix_millis] == ([.samples[].observed_at_unix_millis] | sort))
      and .samples[0].observed_at_unix_millis <= $report.provenance.run.started_at_unix_millis
      and .samples[-1].observed_at_unix_millis >= $report.provenance.run.completed_at_unix_millis
      and ([.samples[].memory_current_bytes] | max) == .sampled_memory_current_peak_bytes
      and ([.samples[].storage_bytes] | max) == .sampled_storage_peak_bytes))
  ' \
  "$report_path" >/dev/null || fail \
  "report and container-resource evidence do not satisfy the closed storage contract"

echo "RocksDB storage lifecycle evidence passed"
