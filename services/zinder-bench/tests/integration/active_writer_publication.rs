#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{num::NonZeroU32, time::Duration};

use eyre::Result;
use zinder_bench::active_writer_publication::{
    ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
    ActiveWriterPublicationConfig, ActiveWriterPublicationReport, ByteParityReport,
    ModeDifferential, ModeReport, WorkloadCoverageScope, WriterMode, run_active_writer_publication,
    validate_config,
};
use zinder_store::RocksDbIoMode;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_all_matched_writer_modes_across_a_4096_block_segment_boundary() -> Result<()> {
    let config = ActiveWriterPublicationConfig {
        base_blocks: 4_098,
        live_blocks: 24,
        reorg_window_blocks: 4,
        segment_blocks: NonZeroU32::new(4_096).unwrap_or(NonZeroU32::MIN),
        dense_block_cadence: 16,
        dense_txs_per_block: 1,
        commit_interval: Duration::from_millis(80),
        slow_reader_frame_delay: Duration::from_millis(1),
        mempool_churn_sizes: vec![50, 500],
        quiescent_rounds: 6,
        io_mode: RocksDbIoMode::Direct,
        ..ActiveWriterPublicationConfig::default()
    };
    validate_config(&config)?;

    let report = run_active_writer_publication(&config).await?;
    report.validate(&config)?;

    assert_eq!(report.contract_identity, ACTIVE_WRITER_PUBLICATION_CONTRACT);
    assert_eq!(
        report.report_format_version,
        ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION
    );
    assert_eq!(report.workload.canonical_segment_blocks, 4_096);
    assert_eq!(
        report.workload.coverage_scope,
        WorkloadCoverageScope::SettledSegmentCrossing
    );
    assert_eq!(report.workload.crossed_segment_boundary_height, Some(4_096));
    assert_eq!(report.workload.probe_anchor_height, 4_096);
    assert_eq!(report.workload.final_tip_height, 4_122);
    assert_eq!(report.workload.dense_orchard_actions_per_tx, 2);
    assert!(report.workload.transparent_spend_txs > 0);
    assert_eq!(report.workload.requested_io_mode, "direct");
    assert_eq!(report.workload.request_trace_digest.len(), 64);

    assert_wallet_enabled_parity(&report.byte_parity);
    assert_differential_baselines(&report.differentials_vs_quiescent);
    assert_4096_boundary_baselines_and_modes(&report, &config)
}

fn assert_4096_boundary_baselines_and_modes(
    report: &ActiveWriterPublicationReport,
    config: &ActiveWriterPublicationConfig,
) -> Result<()> {
    assert_mode_report_invariants(&report.canonical_quiescent_control, config, None, false)?;
    assert_eq!(report.modes.len(), 3);
    for mode_report in &report.modes {
        assert_mode_report_invariants(
            mode_report,
            config,
            None,
            mode_report.mode.has_wallet_serving(),
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_buffered_scratch_dir_and_scaled_orchard_actions_without_parity_loss() -> Result<()>
{
    let scratch = tempfile::tempdir()?;
    let config = ActiveWriterPublicationConfig {
        base_blocks: 30,
        live_blocks: 8,
        reorg_window_blocks: 3,
        dense_block_cadence: 4,
        dense_txs_per_block: 1,
        dense_orchard_actions_per_tx: 8,
        tail_probe_blocks: 8,
        commit_interval: Duration::from_millis(12),
        slow_reader_frame_delay: Duration::from_millis(2),
        mempool_churn_sizes: vec![20, 120],
        quiescent_rounds: 3,
        scratch_dir: Some(scratch.path().to_path_buf()),
        io_mode: RocksDbIoMode::Buffered,
        ..ActiveWriterPublicationConfig::default()
    };
    validate_config(&config)?;

    let report = run_active_writer_publication(&config).await?;
    report.validate(&config)?;

    assert_eq!(report.workload.canonical_segment_blocks, 4_096);
    assert_eq!(
        report.workload.coverage_scope,
        WorkloadCoverageScope::LiveTailOnly
    );
    assert_eq!(report.workload.crossed_segment_boundary_height, None);
    assert_eq!(report.workload.probe_anchor_height, 27);
    assert_eq!(report.workload.dense_orchard_actions_per_tx, 8);
    assert_eq!(report.workload.requested_io_mode, "buffered");
    assert!(report.workload.transparent_spend_txs > 0);
    assert_wallet_enabled_parity(&report.byte_parity);
    assert_differential_baselines(&report.differentials_vs_quiescent);

    assert_mode_report_invariants(
        &report.canonical_quiescent_control,
        &config,
        Some("buffered"),
        false,
    )?;
    for mode_report in &report.modes {
        assert_mode_report_invariants(
            mode_report,
            &config,
            Some("buffered"),
            mode_report.mode.has_wallet_serving(),
        )?;
    }
    Ok(())
}

fn assert_wallet_enabled_parity(byte_parity: &ByteParityReport) {
    assert!(
        byte_parity.all_matched,
        "byte parity mismatches: {:?}",
        byte_parity.mismatches
    );
    assert_eq!(
        byte_parity.mismatches,
        [] as [zinder_bench::active_writer_publication::ParityMismatch; 0]
    );
    assert_eq!(byte_parity.digests.len(), 4);

    let wallet_armed_count = byte_parity
        .digests
        .iter()
        .filter(|digest| digest.wallet_taddress_balance_sha256.is_some())
        .count();
    assert_eq!(wallet_armed_count, 2);

    for digest in &byte_parity.digests {
        let expect_wallet =
            digest.mode.has_wallet_serving() && digest.label != "canonical_quiescent_control";
        if expect_wallet {
            assert!(digest.wallet_taddress_balance_sha256.is_some());
            assert!(digest.wallet_address_utxos_sha256.is_some());
            assert!(digest.wallet_transactions_sha256.is_some());
        } else {
            assert!(digest.wallet_taddress_balance_sha256.is_none());
            assert!(digest.wallet_address_utxos_sha256.is_none());
            assert!(digest.wallet_transactions_sha256.is_none());
        }
    }
}

fn assert_differential_baselines(differentials: &[ModeDifferential]) {
    assert_eq!(differentials.len(), 2);
    for diff in differentials {
        assert!(diff.latest_block_rpc_p50_delta_ms.is_some());
        assert!(diff.slow_boundary_completion_p50_delta_ms.is_some());
        assert!(diff.tree_state_p95_delta_ms.is_some());
        if diff.mode.has_wallet_serving() {
            assert!(diff.wallet_taddress_balance_p50_delta_ms.is_some());
            assert!(diff.wallet_address_utxos_p50_delta_ms.is_some());
            assert!(diff.wallet_get_transaction_p50_delta_ms.is_some());
        } else {
            assert!(diff.wallet_taddress_balance_p50_delta_ms.is_none());
            assert!(diff.wallet_address_utxos_p50_delta_ms.is_none());
            assert!(diff.wallet_get_transaction_p50_delta_ms.is_none());
        }
        match diff.mode {
            WriterMode::IngestActive => {
                assert_eq!(diff.baseline_label, "canonical_quiescent_control");
            }
            WriterMode::IngestAndProjector => {
                assert_eq!(diff.baseline_label, "quiescent_control");
            }
            WriterMode::QuiescentControl => {}
        }
    }
}

fn assert_mode_io_and_wallet_invariants(
    mode_report: &ModeReport,
    expected_io_mode: Option<&str>,
    expect_wallet_serving: bool,
) {
    assert_eq!(
        mode_report.io_modes.canonical_secondaries.len(),
        if expect_wallet_serving { 4 } else { 2 }
    );
    if let Some(expected) = expected_io_mode {
        for mode in &mode_report.io_modes.canonical_secondaries {
            assert_eq!(mode, expected);
        }
        for mode in &mode_report.io_modes.wallet_secondaries {
            assert_eq!(mode, expected);
        }
        if let Some(primary) = mode_report.io_modes.canonical_primary.as_deref() {
            assert_eq!(primary, expected);
        }
        if let Some(projector_sec) = mode_report.io_modes.projector_secondary.as_deref() {
            assert_eq!(projector_sec, expected);
        }
    }
    if expect_wallet_serving {
        assert_eq!(mode_report.io_modes.wallet_secondaries.len(), 4);
        assert!(mode_report.wallet_rpc_ms.taddress_balance_ms.samples > 0);
        assert!(mode_report.wallet_rpc_ms.address_utxos_ms.samples > 0);
        assert!(mode_report.wallet_rpc_ms.get_transaction_ms.samples > 0);
    } else {
        assert_eq!(mode_report.io_modes.wallet_secondaries, [] as [String; 0]);
        assert_eq!(mode_report.wallet_rpc_ms.taddress_balance_ms.samples, 0);
        assert_eq!(mode_report.wallet_rpc_ms.address_utxos_ms.samples, 0);
        assert_eq!(mode_report.wallet_rpc_ms.get_transaction_ms.samples, 0);
    }
}

fn assert_mode_report_invariants(
    mode_report: &ModeReport,
    config: &ActiveWriterPublicationConfig,
    expected_io_mode: Option<&str>,
    expect_wallet_serving: bool,
) -> Result<()> {
    assert!(mode_report.workload_duration_ms > 0.0);
    assert_eq!(mode_report.errors.unexpected_grpc_errors, 0);
    assert!(mode_report.latest_block_rpc_ms.samples > 0);
    assert!(mode_report.catalog_range.completion_ms.samples > 0);
    assert!(mode_report.slow_boundary_range.completion_ms.samples > 0);
    assert!(mode_report.tree_state_ms.samples > 0);
    assert!(
        mode_report
            .tail_refill
            .cold_after_advance
            .completion_ms
            .samples
            > 0
    );
    assert!(mode_report.tail_refill.warm_repeat.completion_ms.samples > 0);
    assert!(mode_report.tail_refill.peak_tail_response_bytes > 0);
    assert_mode_io_and_wallet_invariants(mode_report, expected_io_mode, expect_wallet_serving);

    assert_eq!(
        mode_report.mempool_churn.len(),
        config.mempool_churn_sizes.len()
    );
    if mode_report.mode != WriterMode::QuiescentControl {
        assert_eq!(mode_report.ingest.blocks_committed, config.live_blocks);
        assert!(mode_report.io_modes.canonical_primary.is_some());
        assert_eq!(
            mode_report.commit_to_served_tip_ms.samples,
            usize::try_from(config.live_blocks)?
        );
        for tier in &mode_report.mempool_churn {
            assert!(tier.writer_overlap_verified);
            assert!(tier.commits_observed_during_tier > 0);
        }
    }
    if mode_report.mode == WriterMode::IngestAndProjector {
        assert!(mode_report.io_modes.wallet_primary.is_some());
        assert!(mode_report.io_modes.projector_secondary.is_some());
        assert!(mode_report.projector.transitions_applied >= config.live_blocks);
        assert!(mode_report.rocksdb_activity.projector_secondary.bytes_read > 0);
    }
    Ok(())
}
