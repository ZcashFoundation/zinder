# Metrics and logs

This page lists the Prometheus metrics and structured log events each Zinder
runtime emits, and the signals worth alerting on. It is for operators who build
dashboards and alerts, and for contributors who add a metric.

Each runtime serves metrics at `/metrics` on its operational endpoint,
`ops.listen_addr`, in Prometheus text format. Keep that endpoint private, as
[Service operations](../architecture/service-operations.md#security-boundary)
describes.

Conventions:

- Labels are closed sets. A label never carries a path, address, hash,
  transaction ID, error text, or user input.
- `service` and `network` labels appear only on `zinder_build_info` and the
  `zinder_readiness_*` and `zinder_materialized_view_identity_info` series.
  Your scrape configuration supplies the target identity for the rest.
- A histogram whose name ends in `_duration_seconds` exports Prometheus
  buckets, and so does
  `zinder_grpc_request_receive_slots_held_at_admission`. Other histograms
  export as summaries with quantiles.
- A series appears only after its first sample. For example, a runtime that
  has not broadcast yet exports no broadcast series.
- Each runtime folds new histogram samples into their buckets or summaries
  every five seconds, so its metrics memory stays bounded when nothing
  scrapes `/metrics`.

## Signals worth alerting on

The rules in `observability/prometheus/rules/zinder-readiness.yml` load into
both the reference Compose Prometheus and the local
[observability smoke](../../observability/README.md).

| Alert | Fires when | What it means |
| --- | --- | --- |
| `ZinderTargetDown` | A scrape target is down for 30 seconds. | The process or its operational endpoint is gone. |
| `ZinderServiceNotReady` | `zinder_readiness_state` reports a refusing cause for 1 minute. | The runtime refuses traffic. Read its `/readyz`. |
| `ZinderReadinessWarning` | `cursor_at_risk` for 5 minutes. | A chain-event consumer is close to losing its cursor. |
| `ZinderServingPairStale` | `serving_pair_stale` for 1 minute. | A reader serves an older attested chain view and stops when the staleness ceiling passes. |
| `ZinderDependencyDegraded` | `zinder_readiness_dependency_state` reports a refusing cause, other than a writer halt, for 2 minutes. | A reader stays ready, but its broadcasts or mempool calls fail. |
| `ZinderServedChainStale` | `zinder_readiness_served_epoch_age_seconds` is above 1,800 for 5 minutes, outside Regtest. | The served chain stopped advancing. Wallets build transactions that expire. |
| `ZinderReplicaLag` | `zinder_readiness_replica_lag_chain_epochs` is above 0 for 2 minutes. | A reader trails the writer. |
| `ZinderNodeRpcErrors` | `zinder_node_request_total{status!="ok"}` grows for 2 minutes. | Node calls fail. The `error_class` label names how. |
| `ZinderWalletQueryP95High` | p95 of `zinder_query_request_duration_seconds` is above 250 ms for 5 minutes. | Wallet reads are slow. |
| `ZinderRocksDbCompactionPressure` | Pending compaction bytes are above 256 MiB for 5 minutes. | Compaction falls behind writes. |
| `ZinderRocksDbCompactionRunningTooLong` | More than four compactions run for 15 minutes. | Compaction is saturated. |
| `ZinderStoreWalGrowth` | A store's WAL is above 75% of its limit for 5 minutes. | Flushes stall. See [Bulk catch-up resource tuning](../runbooks/bulk-catchup-resource-tuning.md). |
| `ZinderStartupOpenStorageSlow` | p95 of the `open_storage` startup phase is above 60 seconds. | Opening storage is slow, usually from WAL replay or disk contention. |

A wallet-serving reader answers 200 on `/readyz` during a node outage, so watch
the dependency gauge and the served-chain age, not only HTTP status.

Other signals that need attention when they keep rising:

- `zinder_ingest_node_endpoint_active{position="0"} == 0`: ingest runs on a
  fallback node and stays there until that node fails or ingest restarts.
- `zinder_ingest_tree_state_verification_mismatches_total` or
  `zinder_ingest_tree_state_densification_strides_total{outcome="integrity_failure"}`
  above 0: the canonical store is damaged. Rebuild it.
- `zinder_query_tree_state_replay_refusals_total`: wallets request old tree
  states faster than the host can rebuild them.
- `zinder_query_broadcast_budget_refusals_total`: clients send many large
  transactions, or a slow node holds broadcasts until they time out.
- `zinder_compat_block_range_stream_refusals_total`, or
  `zinder_compat_block_range_slot_wait_duration_seconds{outcome="admitted"}`
  samples near 10 seconds: wallets wait for a block-range stream slot. Raise
  `compat.max_concurrent_block_range_streams` if memory allows, or limit
  streams per client at your proxy.
- `zinder_grpc_request_receive_refusals_total`,
  `zinder_grpc_request_receive_timeouts_total`, and
  `zinder_grpc_response_send_stalls_total` while traffic is light: clients
  open streams and send or read slowly. Limit connections per client at your
  proxy. Behind a proxy, also set the listener's
  `trust_proxy_client_address`, as
  [Exposing over TLS](../runbooks/exposing-over-tls.md#keep-wallets-apart-in-the-receive-limit)
  describes, so that the receive limit tells the proxy's clients apart.
- `zinder_grpc_proxy_client_address_fallbacks_total` rising: requests reach a
  listener with `trust_proxy_client_address` set without one valid client
  address header, so they count against the proxy's own connection. Check
  that the proxy replaces the header on every request, and that nothing else
  reaches the listener. The supervisor's headerless loopback health checks
  use their own connection and leave this counter unchanged.
- `zinder_wallet_serving_missing_table_file_pairs_total` rising with most
  publications: store files are replaced faster than serving pairs, as during
  a manual compaction.

## Every runtime

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_build_info` | gauge | `service`, `version`, `git_commit`, `network` | Always 1. Identifies the build. |
| `zinder_readiness_state` | gauge | `service`, `network`, `cause` | 1 for the current [readiness cause](readiness.md#readiness-causes), 0 for the others. |
| `zinder_readiness_dependency_state` | gauge | `service`, `network`, `input`, `cause` | 1 for each dependency's current cause. Includes a writer halt read through a dependency. |
| `zinder_readiness_node_failure_class` | gauge | `service`, `network`, `class` | 1 for the failure class of a current `node_unavailable` cause. |
| `zinder_readiness_sync_lag_blocks` | gauge | `service`, `network` | `lag_blocks` of a current `syncing` cause, otherwise 0. |
| `zinder_readiness_replica_lag_chain_epochs` | gauge | `service`, `network` | Lag of a current `replica_lagging` or `serving_pair_stale` cause, otherwise 0. |
| `zinder_readiness_served_epoch_age_seconds` | gauge | `service`, `network` | Wallet-serving readers: seconds since the writer committed the served chain state. |
| `zinder_materialized_view_identity_info` | gauge | `service`, `network`, `identity` | 1 for each Explorer view store identity the runtime admitted. |
| `zinder_startup_phase_duration_seconds` | histogram | `phase`, `outcome` | Duration of each [startup phase](../architecture/service-operations.md). |

The projector exports only these series. Its wallet store exports no
`zinder_store_*` metrics.

## zinder-ingest

### Following the chain

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_ingest_canonical_tip_height` | gauge | | Height of the store's visible tip. |
| `zinder_ingest_canonical_lag_blocks` | gauge | | Node tip height minus the store's tip height. |
| `zinder_ingest_canonical_chain_epoch` | gauge | | Current chain epoch, the store version readers pin. |
| `zinder_ingest_canonical_chain_event_sequence` | gauge | | Sequence number of the latest chain event. |
| `zinder_ingest_canonical_live_appends_total` | counter | | Blocks appended while following. |
| `zinder_ingest_canonical_live_replacements_total` | counter | | Reorg replacements committed. |
| `zinder_ingest_canonical_live_prepare_seconds` | histogram | | Time to prepare one appended block. |
| `zinder_ingest_canonical_live_commit_seconds` | histogram | | Time to commit one appended block. |
| `zinder_ingest_canonical_live_replacement_prepare_seconds` | histogram | | Time to prepare one replacement block. |
| `zinder_ingest_canonical_live_replacement_commit_seconds` | histogram | | Time to commit one replacement. |
| `zinder_ingest_canonical_fork_discoveries_total` | counter | | Forks found and prepared for replacement. |
| `zinder_ingest_canonical_fork_discovery_seconds` | histogram | | Time to find where a fork starts. |
| `zinder_ingest_canonical_fork_trigger_block_reads_total` | counter | | Node block reads whose parent did not match the store's tip. |
| `zinder_ingest_canonical_fork_discovery_block_reads_total` | counter | | Node block reads made while searching for a fork point. |
| `zinder_ingest_canonical_replacement_source_block_reads_total` | counter | | Node block reads made to build a replacement. |
| `zinder_ingest_canonical_replacement_source_checkpoint_reads_total` | counter | | Node tree-state reads made to verify a replacement. |
| `zinder_ingest_canonical_source_recoveries_total` | counter | `failure_class` | Follow iterations that failed on a node error and retried. A short outage may appear only here. |
| `zinder_ingest_source_request_total` | counter | `operation`, `status`, `error_class` | Node requests made by ingest. |
| `zinder_ingest_source_request_duration_seconds` | histogram | `operation`, `status`, `error_class` | Duration of ingest's node requests. |
| `zinder_ingest_source_retry_total` | counter | `operation` | Node requests retried within one call. |
| `zinder_ingest_source_fetch_active` | gauge | `operation` | Node fetches in flight. |
| `zinder_ingest_historical_work_gate_open` | gauge | | 1 when ingest follows the tip and, if Explorer views are enabled, they have caught up, so backfills, tree-state densification, and tree-state verification may run. |
| `zinder_ingest_tree_state_densification_strides_total` | counter | `outcome` | Settled 100-block strides the densification pass finished: `densified` when the writer committed it, even with no rows left to add, or `integrity_failure` when its stored blocks or checkpoints are damaged or do not replay to each other. Any `integrity_failure` is store damage. The pass skips that stride and holds its recorded progress below it, so it examines the stride again after ingest restarts. |
| `zinder_ingest_tree_state_densification_remaining_strides` | gauge | | Strides the pass has found in its current scan chunk and not finished yet. |
| `zinder_ingest_tree_state_densification_watermark_height` | gauge | | Height through which the pass has recorded every settled stride as satisfying the selection. |
| `zinder_ingest_tree_state_densification_rows_written_total` | counter | | Tree-state checkpoint rows the pass wrote. |
| `zinder_ingest_tree_state_densification_replayed_commitments_total` | counter | | Note commitments the pass appended while replaying strides. |
| `zinder_ingest_tree_state_densification_stride_duration_seconds` | histogram | | Time to replay one stride. |
| `zinder_ingest_tree_state_densification_retries_total` | counter | | Steps the pass retried after a store or writer error. |
| `zinder_ingest_tree_state_verification_rows_total` | counter | | Stored tree-state checkpoint rows the verification pass checked and found intact. |
| `zinder_ingest_tree_state_verification_mismatches_total` | counter | | Rows or table blocks that failed verification, including recorded damage a restart confirmed and a recorded row a restart found gone. Any increase is store damage: ingest reports `canonical_store_damaged` and the pass stops. |
| `zinder_ingest_tree_state_verification_watermark_height` | gauge | | Height the current verification cycle continues from. It returns to 0 when a cycle ends. |
| `zinder_ingest_tree_state_verification_cycles_total` | counter | | Cycles that checked every settled row. The time between increments is the pass's detection bound. |
| `zinder_ingest_tree_state_verification_retries_total` | counter | | Steps the verification pass retried after a store error, such as a table file compacted away under its secondary. Rows a failed step checked first are counted in `rows_total` and not checked again. |
| `zinder_explorer_views_admitted` | gauge | | 1 when ingest admitted its Explorer view store. |
| `zinder_ingest_raw_blob_disabled_total` | counter | `table` | Rows stored without raw bytes under the raw-blob policy. |

### Fresh construction

These series describe the first sync. They stay flat while ingest follows.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_ingest_canonical_construction_phase_duration_seconds` | histogram | `phase` | Duration of each completed construction phase: `construction_range` (pick the build tip), `block_load` (fetch, prepare, and load every block), `source_families` (check subtree roots and the tip against the node), and `publication`. One sample per phase per run; read `_sum`, because a long run exceeds the largest bucket. |
| `zinder_ingest_canonical_block_construction_stage_duration_seconds` | histogram | `stage`, `status` | Duration of each block-preparation stage. |
| `zinder_ingest_canonical_block_load_stage_duration_seconds` | histogram | `stage` | Per-block time in the single-threaded loader that follows preparation. `receive_wait` is time spent idle waiting for the next prepared block; when it dominates, the node or preparation is the bottleneck, not the loader. The other stages are `commitment_extract`, `tree_hashing_append`, `position`, `tree_hashing_checkpoint` (checkpoint heights only), and `tree_hashing_final_roots` (explorer builds only). The stages do not overlap, and the `tree_hashing_*` stages are all of the loader's commitment-tree hashing: sum them for its serial hashing cost. |
| `zinder_store_canonical_block_load_stage_duration_seconds` | histogram | `stage` | Per-block time the store spends on a loaded block: `validate`, `encode` (the compact block, tree-state checkpoint, and final-root values, once each), `sequence` (counts and the ordered fact digest), and `write` (hand the rows to the staged files and sort runs). |
| `zinder_store_canonical_block_load_finish_duration_seconds` | histogram | `step`, `family` | Time per column family to finish the load after the last block: `sst_finish` closes a height-ordered family's last file, `sort_merge` merges a hash-keyed family's sorted runs and writes its files, and `ingest` hands the family's files to RocksDB. One sample per step per run; read `_sum`. |
| `zinder_ingest_canonical_historical_prevout_reads_total` | counter | | Always 0. Marks a read construction never makes: spent outputs from earlier blocks. |
| `zinder_ingest_canonical_cross_block_wallet_reads_total` | counter | | Always 0. Marks a read construction never makes: wallet data across blocks. |
| `zinder_ingest_bulk_pipeline_queue_bytes` | gauge | `stage` | Bytes reserved by a pipeline stage. |
| `zinder_ingest_bulk_pipeline_active` | gauge | `stage` | Reservations held by a pipeline stage. |
| `zinder_ingest_bulk_pipeline_queue_depth` | gauge | `stage` | Items queued in a pipeline stage. |
| `zinder_ingest_bulk_pipeline_reorder_buffer_blocks` | gauge | `stage` | Blocks waiting to be emitted in height order. |
| `zinder_ingest_bulk_pipeline_reorder_buffer_bytes` | gauge | `stage` | Bytes waiting to be emitted in height order. |
| `zinder_ingest_bulk_pipeline_watermark_blocked_total` | counter | `stage` | Reservations refused because the stage's byte limit was reached. |
| `zinder_ingest_bulk_pipeline_watermark_oversized_total` | counter | `stage`, `reason` | Single items larger than the stage's byte limit. |
| `zinder_ingest_bulk_pipeline_head_of_line_wait_seconds` | histogram | `stage` | Time the pipeline waited for the next block in height order. |
| `zinder_ingest_source_fetch_queue_requests` | gauge | | Node segment requests in flight. |
| `zinder_ingest_source_fetch_queue_bytes` | gauge | | Response bytes reserved for those requests. |
| `zinder_ingest_source_fetch_request_reservation_bytes` | gauge | | Bytes reserved for the latest segment request. |
| `zinder_ingest_source_segment_next_blocks` | gauge | | Blocks the next segment request will ask for. |
| `zinder_ingest_source_segment_max_blocks` | gauge | | Upper bound on blocks per segment request. |
| `zinder_ingest_source_segment_target_response_payload_bytes` | gauge | | Target response size used to size segments. |
| `zinder_ingest_source_segment_initial_probe_in_flight` | gauge | | 1 while the first sizing request is in flight. |
| `zinder_ingest_source_segment_reassembly_segments` | gauge | | Fetched segments waiting to be emitted in order. |
| `zinder_ingest_source_segment_reassembly_bytes` | gauge | | Bytes of those segments. |
| `zinder_ingest_source_segment_connected_blocks_total` | counter | | Blocks received in segments. |
| `zinder_ingest_source_segment_response_payload_bytes` | histogram | | Response size of each segment. |
| `zinder_ingest_source_segment_reserved_response_bytes` | histogram | | Bytes reserved for each segment request. |
| `zinder_ingest_source_segment_reservation_undersized_total` | counter | | Segments larger than their reservation. |
| `zinder_ingest_source_segment_sizing_adjustment_total` | counter | `reason` | Changes to the segment size. |
| `zinder_ingest_source_segment_prefetch_retained_completed_segments_total` | counter | `reason` | Fetched segments kept after a size change. |
| `zinder_ingest_source_segment_prefetch_retained_in_flight_segments_total` | counter | `reason` | In-flight segments kept after a size change. |
| `zinder_ingest_source_segment_prefetch_retained_completed_response_bytes_total` | counter | `reason` | Bytes of fetched segments kept after a size change. |
| `zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_segments_total` | counter | `reason` | Always 0. Construction keeps prefetched work after a size change. |
| `zinder_ingest_source_segment_disjoint_prefetch_discarded_in_flight_segments_total` | counter | `reason` | Always 0, as above. |
| `zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_response_bytes_total` | counter | `reason` | Always 0, as above. |
| `zinder_ingest_source_segment_disjoint_prefetch_refetched_blocks_total` | counter | `reason` | Always 0, as above. |
| `zinder_store_canonical_publication_family_scan_duration_seconds` | histogram | `family` | Time to scan one column family while publishing the built store. |
| `zinder_store_canonical_publication_family_scan_rows_total` | counter | `family` | Rows scanned during that check. |
| `zinder_store_canonical_publication_family_scan_logical_bytes_total` | counter | `family` | Bytes scanned during that check. |

### Fallback nodes

These series appear only with `[node_fallbacks.N]` configured. `position` is
0 for `[node]` and `N` for `[node_fallbacks.N]`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_ingest_node_endpoint_active` | gauge | `position` | 1 for the endpoint ingest reads from. |
| `zinder_ingest_node_endpoint_state` | gauge | `position`, `state` | 1 for the endpoint's state: `active`, `pending`, `standby`, `standby_diverged`, `ineligible`, `unreachable`, or `excluded`. |
| `zinder_ingest_node_endpoint_switches_total` | counter | `from_position`, `to_position`, `failure_class` | Completed switches, by the failure that caused them. |

### Mempool

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_mempool_lifecycle_state` | gauge | `status` | 1 for the current state: `hydrating`, `serving`, `rebuild_required`, or `halted`. `halted` follows a writer park. |
| `zinder_mempool_entries` | gauge | | Current mempool entries. |
| `zinder_mempool_snapshot_age_seconds` | histogram | | Age of the served snapshot when a page is read. |
| `zinder_mempool_source_tip_changes_total` | counter | | Mempool source generations ended by a tip change. |
| `zinder_mempool_source_errors_total` | counter | `kind`, `failure_class` | Mempool source errors. |
| `zinder_mempool_hydration_failures_total` | counter | `backend`, `reason` | Failed loads of a mempool snapshot. |
| `zinder_mempool_events_retained` | gauge | | Retained mempool events after a retention pass. |
| `zinder_mempool_event_retention_oldest_sequence` | gauge | | Sequence number of the oldest retained event. |
| `zinder_mempool_event_retention_oldest_age_seconds` | gauge | | Age of the oldest retained event. |
| `zinder_mempool_events_pruned_total` | counter | `kind` | Events deleted by retention. |
| `zinder_mempool_event_pruning_duration_seconds` | histogram | `status` | Duration of a retention step. `status` is `budget_exhausted`, `reached_head`, `reached_unexpired_event`, `retention_disabled`, or `error`. |
| `zinder_mempool_event_retention_examined_events` | histogram | | Events examined by a retention step. |
| `zinder_mempool_event_retention_examined_encoded_bytes` | histogram | | Bytes examined by a retention step. |

### Explorer view replay

Ingest builds the Explorer views. `consumer` names a view.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_materialized_view_replay_height` | gauge | `consumer` or none | Height replayed, per view and overall. |
| `zinder_materialized_view_replay_tip_height` | gauge | | Canonical tip height replay is moving toward. |
| `zinder_materialized_view_replay_lag_blocks` | gauge | `consumer` or none | Blocks between the canonical tip and the replayed height. |
| `zinder_materialized_view_replay_caught_up` | gauge | | 1 when every view has replayed every canonical block. |
| `zinder_materialized_view_replay_paused` | gauge | | 1 while replay is paused for any reason. |
| `zinder_materialized_view_replay_phase_gate` | gauge | | 1 while replay waits for the first sync to finish. |
| `zinder_materialized_view_replay_volume_space_low` | gauge | | 1 while replay is paused for low disk space. |
| `zinder_materialized_view_replay_volume_available_bytes` | gauge | | Last sample of free space on the store volume. |
| `zinder_materialized_view_replay_volume_min_free_bytes` | gauge | | Configured `ingest.explorer_views_min_free_bytes`. |
| `zinder_materialized_view_replay_policy` | gauge | `policy` | 1 for the configured policy: `canonical-first` or `continuous`. |
| `zinder_materialized_view_replay_budget_state` | gauge | `state` | 1 for the current budget state: `normal`, `degraded`, or `paused`. |
| `zinder_materialized_view_replay_budget_seconds` | gauge | | Replay poll interval, or 0 while paused. |
| `zinder_materialized_view_replay_effective_batch_blocks` | gauge | | Blocks replayed per batch under the current budget. |
| `zinder_materialized_view_replay_memory_budget_bytes` | gauge | | Memory budget under the current state. |
| `zinder_materialized_view_replay_events_total` | counter | `status`, `error_class` | Replay batches. |
| `zinder_materialized_view_replay_blocks_total` | counter | `status`, `error_class`, or `consumer` | Blocks replayed, overall and per view. |
| `zinder_materialized_view_replay_dispatches_total` | counter | `consumer` | Chain-event dispatches to a view. |
| `zinder_materialized_view_replay_stage_duration_seconds` | histogram | `stage`, `status`, `error_class` | Duration of each replay stage. |
| `zinder_materialized_view_dispatch_duration_seconds` | histogram | `consumer`, `source` | Time a view took to apply a dispatch. |
| `zinder_materialized_view_write_operations_total` | counter | `consumer`, `source` | Write operations to the view store. |
| `zinder_materialized_view_write_bytes_total` | counter | `consumer`, `source` | Bytes written to the view store. |
| `zinder_materialized_view_tailer_ticks_total` | counter | `status`, `error_class` | Iterations of the loop that follows new chain events. |
| `zinder_materialized_view_tailer_tick_duration_seconds` | histogram | `status`, `error_class` | Duration of each iteration. |
| `zinder_materialized_view_prevout_resolutions_total` | counter | `source`, `cache` | Spent outputs resolved, by where they were found: `range`, `cache`, or `canonical`. |
| `zinder_materialized_view_prevout_block_reads_total` | counter | `cache` | Canonical blocks read to resolve spent outputs. |
| `zinder_materialized_view_prevout_cache_outputs` | gauge | `cache` | Unspent outputs held by the `replay` or `ranking_build` cache. |
| `zinder_materialized_view_prevout_cache_evicted_outputs_total` | counter | `cache` | Outputs evicted to stay within the cache bound. |
| `zinder_materialized_view_transparent_address_ranking_build_pending` | gauge | | 1 while a transparent-address ranking build is pending. |
| `zinder_materialized_view_transparent_address_ranking_build_height` | gauge | | Height the ranking build has applied. |
| `zinder_materialized_view_transparent_address_ranking_build_target_height` | gauge | | Height the ranking build is moving toward. |
| `zinder_materialized_view_replay_waiting_for_ranking_build` | gauge | | 1 while replay waits for the ranking build to catch up. |
| `zinder_materialized_view_rocksdb_property` | gauge | `consumer`, `property`, `store_role` | RocksDB properties of the view store. |

A `canonical` share of `zinder_materialized_view_prevout_resolutions_total`
that stays high after warm-up, with a rising eviction count, means the cache
is too small for the range being replayed.

### Process memory

Ingest samples its cgroup and process memory every second. A series is
missing when the host does not expose its source.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_ingest_memory_current_bytes` | gauge | | cgroup `memory.current`. |
| `zinder_ingest_memory_max_bytes` | gauge | | cgroup `memory.max`. |
| `zinder_ingest_memory_high_bytes` | gauge | | cgroup `memory.high`. |
| `zinder_ingest_memory_swap_current_bytes` | gauge | | cgroup swap in use. |
| `zinder_ingest_memory_working_set_bytes` | gauge | | `memory.current` minus inactive file cache. |
| `zinder_ingest_memory_cgroup_anon_bytes` | gauge | | cgroup anonymous memory. |
| `zinder_ingest_memory_cgroup_file_bytes` | gauge | | cgroup file cache. |
| `zinder_ingest_memory_cgroup_active_file_bytes` | gauge | | cgroup active file cache. |
| `zinder_ingest_memory_cgroup_inactive_file_bytes` | gauge | | cgroup inactive file cache. |
| `zinder_ingest_memory_cgroup_kernel_bytes` | gauge | | cgroup kernel memory. |
| `zinder_ingest_memory_cgroup_slab_bytes` | gauge | | cgroup slab memory. |
| `zinder_ingest_process_rss_bytes` | gauge | | Process resident memory. |
| `zinder_ingest_process_rss_anon_bytes` | gauge | | Process anonymous resident memory. |
| `zinder_ingest_memory_pressure_ratio` | gauge | | Anonymous memory divided by `memory.high`, or `memory.max` when unset. |
| `zinder_ingest_memory_current_pressure_ratio` | gauge | | `memory.current` divided by the same limit. |

## zinder-query and zinder-compat-lightwalletd

Both runtimes serve wallet reads through the same code and export the same
series. Compatibility in compact-blocks mode has no serving pair and no
broadcast, so it exports none of those series.

### Wallet requests

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_query_request_total` | counter | `operation`, `status`, `error_class` | Wallet reads. One protocol request can record several reads. |
| `zinder_query_request_duration_seconds` | histogram | `operation`, `status`, `error_class` | Duration of each read, including any tree-state rebuild and the node call of a broadcast. |
| `zinder_query_compact_block_range_block_count` | histogram | `status` | Blocks each compact-block range read asked for. |
| `zinder_query_tree_state_reads_total` | counter | `source`, `height_band` | Served tree-state reads. `source` is `stored` for a height with a stored checkpoint and `replayed` for a rebuilt one. |
| `zinder_query_tree_state_replayed_blocks` | histogram | `height_band` | Blocks replayed to answer each served tree-state read, from 0 to 99. A stored checkpoint records 0. |
| `zinder_query_tree_state_replayed_commitments` | histogram | `pool`, `height_band` | Note commitments appended to each pool (`sapling`, `orchard`, `ironwood`) to answer each served tree-state read. A stored checkpoint records 0. |
| `zinder_query_tree_state_replay_admissions_total` | counter | | Tree-state rebuilds that took a rebuild slot. |
| `zinder_query_tree_state_replay_refusals_total` | counter | `reason` | Tree-state reads refused a rebuild slot: `wait_expired` when no slot freed within the wait, `waiters_full` when every waiting place was taken. |
| `zinder_query_tree_state_replay_wait_duration_seconds` | histogram | | Time each admitted or refused rebuild waited for a rebuild slot. |
| `zinder_query_tree_state_replay_duration_seconds` | histogram | `height_band` | Time each completed rebuild took, from the start of its storage read until it answered: reading and replaying compact blocks, and any wait for a blocking thread. |
| `zinder_query_compact_block_range_stream_refusals_total` | counter | | Native `CompactBlocksInRange` streams refused because 100 were already open. |
| `zinder_query_broadcast_budget_refusals_total` | counter | | Broadcasts refused because 16 MiB of transactions were already in flight. |

`height_band` is the first height of the read's 250,000-block band, such as
`1750000`. Rebuild time can follow note commitments more than blocks, so
compare `zinder_query_tree_state_replayed_commitments` with the rebuild
duration in dense bands such as the 2022 spam range. The share of reads served
from a stored checkpoint, per band, is
`sum by (height_band) (rate(zinder_query_tree_state_reads_total{source="stored"}[5m])) / sum by (height_band) (rate(zinder_query_tree_state_reads_total[5m]))`.

`operation` names the read, such as `compact_blocks_in_range` or
`tree_state_at`. `status` is `ok` or `error`. `error_class` is `none` on
success and otherwise the error's snake-case name, such as
`chain_epoch_pin_unavailable`. A storage failure is `canonical_store` or
`wallet_store`, after the store that failed. A streaming read records once, when the stream
is established. The native gRPC adapter also records `chain_events` and the
mempool proxies, with the gRPC code name as `error_class`.

### Serving pair

A reader serves from a published pair of read-only store copies and refreshes
it as the writer commits.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_wallet_serving_writer_status_total` | counter | `status` | Reads of the writer's status RPC. |
| `zinder_wallet_serving_writer_status_duration_seconds` | histogram | `status` | Duration of those reads. |
| `zinder_wallet_serving_writer_status_available` | gauge | | 1 when the latest status read succeeded. |
| `zinder_wallet_serving_pair_publisher_refresh_total` | counter | `status`, `error_class` | Refresh attempts. |
| `zinder_wallet_serving_pair_publisher_convergence_total` | counter | `outcome` | Rounds spent bringing a candidate pair to the writer's state. |
| `zinder_wallet_serving_pair_publisher_publications_total` | counter | | Pairs published. |
| `zinder_wallet_serving_pair_publisher_replica_lag_chain_epochs` | gauge | | Chain epochs between the writer and the published pair. |
| `zinder_wallet_serving_pair_publisher_catchup_duration_seconds` | histogram | `status` | Time to catch a candidate up. |
| `zinder_wallet_serving_pair_publisher_cold_open_duration_seconds` | histogram | `status` | Time to open a new pair slot from the stores. |
| `zinder_wallet_serving_pair_publisher_teardown_duration_seconds` | histogram | `status` | Time to close a retired pair. |
| `zinder_wallet_serving_pair_publisher_candidate_prepared_total` | counter | `kind` | Candidates prepared: `cold` for a new slot, `recycled` for a reused one. More than four `cold` means refreshes keep failing. |
| `zinder_wallet_serving_pair_publisher_open_generations` | gauge | | Pair slots holding open stores, at most four. |
| `zinder_wallet_serving_pair_publisher_retained_pairs` | gauge | | Replaced pairs kept for one minute for pinned reads. |
| `zinder_wallet_serving_pair_publisher_pinned_retired_pairs` | gauge | | Retired pairs that requests still hold. |
| `zinder_wallet_serving_pair_publisher_generation_wait_total` | counter | `reason` | Refreshes that published nothing because every slot was taken: `retention_window` or `in_flight_requests`. |
| `zinder_wallet_serving_pair_publisher_revoked_pairs_total` | counter | | Retired pairs whose reads were revoked after 30 seconds. |
| `zinder_wallet_serving_missing_table_file_pairs_total` | counter | | Pairs replaced because compaction deleted a file they needed. |

### Broadcast endpoints

`position` is 0 for `[node]` and `N` for `[node_fallbacks.N]`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_wallet_broadcast_endpoint_attempts_total` | counter | `position`, `outcome` | Broadcasts sent to an endpoint, by result, such as `accepted`, `rejected`, or `node_unreachable`. |
| `zinder_wallet_broadcast_endpoint_state` | gauge | `position`, `state` | 1 for the endpoint's state: `verified`, `unverified`, or `excluded`. |
| `zinder_wallet_broadcast_endpoint_switches_total` | counter | `from_position`, `to_position`, `cause` | Changes of the preferred endpoint: `node_unreachable` or `bad_consensus_branch`. |

### Public gRPC listeners

`zinder-explorer` exports these series too. `pool` is `request` for unary and
server-streaming calls, and `client_stream` for client-streaming and
bidirectional calls.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_grpc_request_receive_refusals_total` | counter | `pool` | Requests refused with `UNAVAILABLE` because the pool was full. |
| `zinder_grpc_request_receive_timeouts_total` | counter | `pool` | Requests cancelled because the request or the next client message took more than 20 seconds. |
| `zinder_grpc_request_receive_evictions_total` | counter | `pool` | Requests cancelled to give their slot to a client holding fewer. |
| `zinder_grpc_request_receive_slots_held` | gauge | `pool` | Slots the pool holds now. `avg_over_time` over a range gives the mean number of slots held during it. |
| `zinder_grpc_request_receive_slots_held_at_admission` | histogram | `pool` | Slots the pool held once it admitted a request, the request's own included. Its buckets are 1, 2, 4, 8, 16, 24, 31, and 32, so with the default 32 slots a sample above `le="31"` is a request that left the pool full, by taking its last free slot or another client's slot. |
| `zinder_grpc_request_receive_slot_hold_duration_seconds` | histogram | `pool` | Time an admitted request held its slot, until its request arrived, its client stream ended, or another client's request took the slot. Each hold is recorded whole when it ends, so `_sum` does not show how many slots were held during a range; `zinder_grpc_request_receive_slots_held` does. |
| `zinder_grpc_proxy_client_address_fallbacks_total` | counter | `reason` | Requests on a listener with `trust_proxy_client_address` set that used the transport peer because the `x-zinder-client-address` header was `missing`, `repeated` (sent more than once or as a list), or `invalid` (not one IP address and port). Excludes headerless loopback `grpc.health.v1.Health/Check` calls. |
| `zinder_grpc_response_send_stalls_total` | counter | | Responses ended because the client stopped reading for 60 seconds. |

## zinder-compat-lightwalletd

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_compat_serving_info` | gauge | `serving` | 1 for the serving mode: `wallet` or `compact-blocks`. |
| `zinder_compat_block_range_streams_open` | gauge | | Block-range streams holding a slot of `compat.max_concurrent_block_range_streams`. |
| `zinder_compat_block_range_streams_admitted_total` | counter | | Block-range requests that took a slot. |
| `zinder_compat_block_range_stream_refusals_total` | counter | `reason` | Block-range requests refused with `UNAVAILABLE`: `wait_timeout` when no slot returned within 10 seconds, `waiters_full` when as many requests already waited as the limit has slots. |
| `zinder_compat_block_range_slot_wait_duration_seconds` | histogram | `outcome` | Time a block-range request waited before it took a slot (`admitted`), was refused (`refused`), or its client went away (`cancelled`). |
| `zinder_compat_block_range_stage_duration_seconds` | histogram | `stage` | Time one chunk of a block-range stream spent reading from storage (`read`), converting to lightwalletd messages (`convert`), or, for each chunk after the first, waiting for room for it before its read (`send`). The adapter holds two chunks of a stream, so a chunk waits until the gRPC server has taken every block of the chunk two before it to send. A later read that finds a height outside retained history and gives way to a read of heights an earlier read checked records a `read` sample for each. In compact-blocks mode `read` is the storage read alone; in wallet mode it is the wallet query, including its wait for a blocking thread, and ends when the query returns, so it leaves out the conversion's wait for one. |
| `zinder_compat_stale_tip_refusals_total` | counter | `method` | `GetLatestBlock` (`get_latest_block`) and `GetLightdInfo` (`get_lightd_info`) calls refused with `UNAVAILABLE` because the served tip was older than `compat.stale_tip_limit_ms` and the writer's node did not vouch for it. |
| `zinder_compat_block_range_reorg_endings_total` | counter | `cause` | Streams ended early by a reorg: `chain_replaced` or `tip_lowered`. Clients retry. |
| `zinder_compat_compact_refresh_failures_total` | counter | `cause` | Compact-blocks mode: failed refreshes, by readiness cause. |

## zinder-explorer

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_explorer_request_total` | counter | `operation`, `status`, `error_class` | Explorer requests. |
| `zinder_explorer_request_duration_seconds` | histogram | `operation`, `status`, `error_class` | Duration of Explorer requests. |
| `zinder_materialized_views_secondary_catchup_retries_total` | counter | | Catch-ups retried because compaction removed a file mid-read. |

## Node client

Any runtime that calls a node exports these series. `source` is
`zebra_json_rpc` or `zebra_indexer_grpc`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_node_request_total` | counter | `source`, `method`, `status`, `error_class` | Node requests. |
| `zinder_node_request_duration_seconds` | histogram | `source`, `method`, `status`, `error_class` | Duration of node requests. |
| `zinder_node_request_admission_wait_seconds` | histogram | `source`, `method` | Time an indexer block request waited for a request slot. |
| `zinder_node_active_requests` | gauge | `source`, `method` | Indexer requests in flight. |
| `zinder_node_queued_requests` | gauge | `source`, `method` | Indexer requests waiting for a slot. |
| `zinder_node_response_bytes_total` | counter | `source`, `method`, `encoding` | Bytes received from the indexer stream. |
| `zinder_node_block_decode_stage_duration_seconds` | histogram | `source`, `stage` | Time to decode node blocks, per stage. |
| `zinder_node_source_segment_split_total` | counter | `source`, `reason` | Block-range requests split because the response was too large. |
| `zinder_node_source_segment_offending_range_retry_requests_total` | counter | `source`, `reason` | Requests made after those splits. |
| `zinder_node_source_segment_offending_range_retry_blocks_total` | counter | `source`, `reason` | Blocks requested again after those splits. |
| `zinder_transport_reconnect_total` | counter | `peer`, `event`, `reason` | Node transport reconnects. |

## Storage

The `store_role` label identifies the RocksDB handle or shared serving cache
within each process.

| Runtime | Roles |
| --- | --- |
| `zinder-ingest` | `canonical_primary` (including wallet-only ingest), `canonical_secondary` (materialized-view replay), `tree_state_verification_secondary`, and `materialized_view_primary` |
| `zinder-projector` | `wallet_primary` |
| `zinder-explorer` | `materialized_view_secondary` |
| `zinder-query`, wallet-mode `zinder-compat-lightwalletd` | `canonical_secondary` and `wallet_secondary` |
| Compact-blocks-mode `zinder-compat-lightwalletd` | `canonical_secondary` |

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `zinder_store_block_cache_capacity_bytes` | gauge | `store_role` | Block cache capacity. |
| `zinder_store_block_cache_usage_bytes` | gauge | `store_role` | Block cache use. It can exceed capacity by pinned index and filter blocks. |
| `zinder_store_block_cache_pinned_usage_bytes` | gauge | `store_role` | Pinned block cache use. |
| `zinder_store_block_cache_entries` | gauge | `store_role`, `entry_role` | Serving readers only. Number of cached entries by RocksDB role, such as `DataBlock`, `IndexBlock`, or `FilterBlock`. |
| `zinder_store_block_cache_entry_bytes` | gauge | `store_role`, `entry_role` | Serving readers only. Approximate cache charge by entry role. RocksDB rounds each role's charge before export. |
| `zinder_store_memtable_budget_bytes` | gauge | `store_role` | Memtable budget. Readers sum it over open pair slots. |
| `zinder_store_memtable_budget_usage_bytes` | gauge | `store_role` | Memtable use. Readers sum it over open pair slots. |
| `zinder_store_table_reader_memory_bytes` | gauge | `store_role` | Readers only: table-reader memory outside the block cache, sampled at most every 15 seconds. |
| `zinder_store_wal_bytes` | gauge | `store_role` | Live WAL size. |
| `zinder_store_wal_bytes_limit` | gauge | `store_role` | Configured WAL limit. Only primary opens apply it. |
| `zinder_store_max_background_jobs` | gauge | `store_role` | Configured background job limit on a primary. |
| `zinder_store_rocksdb_io_mode` | gauge | `store_role`, `mode` | 1 for the I/O mode: `direct` or `buffered`. |
| `zinder_store_rocksdb_property` | gauge | `store_role`, `cf`, `property` | RocksDB properties, such as pending compaction bytes. |
| `zinder_store_rocksdb_ticker` | gauge | `store_role`, `ticker` | RocksDB statistics tickers, when statistics are on. |

Ingest samples the canonical primary after bulk loads, while following the
tip, and after checkpoint densification commits. The projector samples the
wallet primary during following. Commit bursts refresh the writer gauges at
most once per second. Writers sample WAL size at most once every 15 seconds.

For each data column family, `rocksdb.num-files-at-level0` includes zero.
Levels 1 through 6 acquire a file-count series when they hold SSTs.
When a level empties, its previous count clears to zero. The `cf` label is
`__db__` for database-wide write pressure.

Inspect `rocksdb.aggregated-table-properties` offline for index and filter
sizes. RocksDB can open every uncached SST to collect them. Per-level
compression ratios also belong in offline inspection because RocksDB visits
every file's in-memory metadata. Replay and materialized-view stores retain
their existing estimates of live data size, total SST size, and table-reader
memory.

Serving publishers report the cache footprint, cache entries, tickers, and
summed instance memory. Compact-block mode serves only the canonical store.
Cache usage includes pinned bytes. Each store shares one cache and one set of tickers
across generations. Recycling a generation preserves its reads in the
cumulative totals. Serving publishers sample cache composition on the blocking
pool, using one handle per cache at the 15-second instance-memory cadence.

`zinder_store_rocksdb_ticker` uses upstream names for its `ticker` label.
The store's `statistics_level` must be `tickers` or `full`.

| To measure | Tickers |
| --- | --- |
| File opens and errors | `rocksdb.no.file.opens`, `rocksdb.no.file.errors` |
| Bytes served from cache | `rocksdb.block.cache.bytes.read` |
| Where point reads hit | `rocksdb.memtable.hit`, `rocksdb.l0.hit`, `rocksdb.l1.hit`, `rocksdb.l2andup.hit` |
| Iterator work | `rocksdb.number.db.seek`, `rocksdb.number.db.next`, `rocksdb.db.iter.bytes.read` |
| Readahead | `rocksdb.prefetch.bytes`, `rocksdb.prefetch.bytes.useful`, `rocksdb.readahead.trimmed` |

## Structured logs

Every service binary writes text logs to stderr. `RUST_LOG` selects them, in
[`EnvFilter` directive syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives),
such as `info,zinder::ingest=debug`. An unset or blank `RUST_LOG` selects
`info`. A value that does not parse stops the binary before it reads
configuration, with `RUST_LOG="<value>" is not a valid log filter`.

Each log event carries an `event` field and a tracing target such as
`zinder::ingest`. Automate on those fields, not on message text. Events carry
bounded values only: heights, epochs, sequence numbers, durations, and labels.
Events that name a node endpoint use its `position`, and events that carry an
endpoint URL, such as a node's or another Zinder runtime's, redact its user
information and query.

| Event | Runtime | Meaning |
| --- | --- | --- |
| `rocksdb_direct_io_open_failed` | All store runtimes | The direct I/O open failed. The `error` field holds the RocksDB error, and the process retries with buffered I/O. |
| `rocksdb_io_mode` | All store runtimes | A successful open resolved to `mode=direct` or `mode=buffered`. |
| `canonical_writer_started` | ingest | The writer started. |
| `canonical_ready_store_reopened` | ingest | An existing store was reopened. |
| `canonical_empty_construction_staging_removed`, `canonical_unpublished_construction_restarted` | ingest | A first sync restarted from scratch. |
| `canonical_live_append_committed`, `canonical_live_replacement_committed` | ingest | A block or a reorg replacement was committed. |
| `canonical_source_recovery` | ingest | A node outage started or changed class. |
| `upstream_health_probe_started`, `upstream_health_endpoint_unreachable` | ingest | The node health probe started, or cannot reach its endpoint. |
| `node_endpoint_admitted`, `node_endpoint_excluded`, `node_endpoint_unreachable`, `node_endpoint_skipped`, `node_endpoint_deferred` | ingest | Startup checks of fallback nodes. |
| `node_endpoint_state_changed`, `node_endpoint_switched`, `node_endpoint_switch_unavailable` | ingest | Fallback node states and switches. |
| `mempool_source_selected` | ingest | Each endpoint's mempool backend, `streaming` or `polling`. |
| `materialized_view_replay_volume_space_low`, `materialized_view_replay_volume_space_recovered` | ingest | View replay paused or resumed for disk space. |
| `materialized_view_replay_ranking_build_stalled` | ingest | Replay stopped waiting for a ranking build that made no progress for 5 minutes. |
| `tree_state_verification_cycle_completed`, `tree_state_verification_mismatch` | ingest | The verification pass checked every settled tree-state row, or found a damaged one, with its `height`, `pool`, and `reason`. |
| `tree_state_verification_recorded_damage_reported`, `tree_state_verification_recorded_damage_cleared`, `tree_state_verification_recorded_damage_discarded` | ingest | A start reported damage the pass recorded before, a recheck found that row intact, or the record named another store. |
| `canonical_secondary_behind_writer` | projector | A convergence attempt timed out. |
| `wallet_serving_pair_publisher_published` | query, compatibility | A pair was published. Match it with `canonical_live_append_committed` to measure block-to-wallet delay. |
| `wallet_serving_pair_publisher_refresh_failed` | query, compatibility | A refresh failed. Routine lag logs at debug level. |
| `wallet_serving_pair_publisher_generation_opened` | query, compatibility | A pair slot was opened cold. |
| `wallet_serving_pair_table_file_missing`, `wallet_serving_pair_reads_revoked` | query, compatibility | A pair lost a file to compaction, or its reads were revoked. |
| `wallet_node_verified`, `wallet_node_verification_failed`, `wallet_node_excluded`, `wallet_node_reverification_inconclusive` | query, compatibility | Broadcast endpoint verification. |
| `broadcast_endpoint_switched`, `broadcast_reported_accepted_after_unreachable_endpoint` | query, compatibility | A broadcast moved endpoints, or a duplicate answer was reported as accepted. |
| `compat_started` | compatibility | Startup, with its stream limits. |
| `compat_block_range_ended_by_reorg` | compatibility | A block-range stream ended at a reorg. |
| `compat_block_range_continued_at_newer_epoch` | compatibility | Debug level: a stream continued across a publication. |
| `compact_serving_refresh_failed` | compatibility | Compact-blocks mode refresh failed. |
| `ops_endpoint_started` | all | The operational endpoint is listening. |

Startup phases log under the `zinder::startup` target, as
[Service operations](../architecture/service-operations.md) describes.
