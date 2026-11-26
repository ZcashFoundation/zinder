#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{num::NonZeroU32, time::Duration};

use eyre::Result;
use zinder_bench::active_writer_publication::{
    ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
    ActiveWriterPublicationConfig, ModeReport, WriterMode, run_active_writer_publication,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_all_four_matched_writer_modes_across_a_4096_block_segment_boundary() -> Result<()> {
    let config = ActiveWriterPublicationConfig {
        base_blocks: 4_092,
        live_blocks: 14,
        reorg_window_blocks: 4,
        segment_blocks: NonZeroU32::new(4_096).unwrap_or(NonZeroU32::MIN),
        dense_block_cadence: 16,
        dense_txs_per_block: 1,
        commit_interval: Duration::from_millis(6),
        slow_reader_frame_delay: Duration::from_millis(1),
        mempool_churn_sizes: vec![50, 500],
        quiescent_rounds: 4,
    };

    let report = run_active_writer_publication(&config).await?;
    assert_eq!(report.contract_identity, ACTIVE_WRITER_PUBLICATION_CONTRACT);
    assert_eq!(
        report.report_format_version,
        ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION
    );
    assert_eq!(report.workload.crossed_segment_boundary_height, 4_096);
    assert_eq!(report.workload.final_tip_height, 4_106);
    assert!(report.workload.sparse_block_count > 0);
    assert!(report.workload.dense_block_count > 0);
    assert_eq!(report.modes.len(), 4);
    assert_eq!(report.differentials_vs_quiescent.len(), 3);
    assert!(
        report.byte_parity.all_matched,
        "byte parity mismatches: {:?}",
        report.byte_parity.mismatches
    );

    for mode_report in &report.modes {
        assert_mode_report_invariants(mode_report, &config)?;
    }
    Ok(())
}

fn assert_mode_report_invariants(
    mode_report: &ModeReport,
    config: &ActiveWriterPublicationConfig,
) -> Result<()> {
    assert_eq!(
        mode_report.errors.unexpected_grpc_errors, 0,
        "unexpected grpc errors in {:?}: {:?}",
        mode_report.mode, mode_report.errors.unexpected_error_samples
    );
    assert!(
        !mode_report.io_modes.canonical_secondaries.is_empty(),
        "missing secondary I/O mode in {:?}",
        mode_report.mode
    );
    for io_mode in &mode_report.io_modes.canonical_secondaries {
        assert!(io_mode == "direct" || io_mode == "buffered");
    }
    assert!(mode_report.catalog_range.completion_ms.samples > 0);
    assert!(mode_report.catalog_range.first_frame_ms.samples > 0);
    assert!(
        mode_report
            .tail_refill
            .cold_after_advance
            .completion_ms
            .samples
            > 0
    );
    assert!(mode_report.slow_boundary_range.completion_ms.samples > 0);
    assert!(mode_report.tree_state_ms.samples > 0);

    match mode_report.mode {
        WriterMode::QuiescentControl => {
            assert_eq!(mode_report.ingest.blocks_committed, 0);
            assert_eq!(mode_report.writer_fsyncs.canonical_wal_synced, 0);
            assert_eq!(mode_report.node_rpcs.total, 0);
        }
        WriterMode::IngestActive => {
            assert_eq!(mode_report.ingest.blocks_committed, config.live_blocks);
            assert_eq!(
                mode_report.ingest.live_prepare_ms.samples,
                usize::try_from(config.live_blocks)?
            );
            assert_eq!(
                mode_report.ingest.live_commit_ms.samples,
                usize::try_from(config.live_blocks)?
            );
            assert!(mode_report.commit_to_served_tip_ms.samples > 0);
            assert!(mode_report.writer_fsyncs.canonical_wal_synced > 0);
            assert!(mode_report.node_rpcs.total > 0);
        }
        WriterMode::IngestAndProjector => {
            assert_eq!(mode_report.ingest.blocks_committed, config.live_blocks);
            assert!(mode_report.projector.transitions_applied >= config.live_blocks);
            assert!(mode_report.writer_fsyncs.canonical_wal_synced > 0);
            assert!(mode_report.writer_fsyncs.wallet_wal_synced > 0);
            assert!(mode_report.io_modes.wallet_primary.is_some());
            assert!(mode_report.node_rpcs.total > 0);
        }
        WriterMode::IngestProjectorAndPublisher => {
            assert_eq!(mode_report.ingest.blocks_committed, config.live_blocks);
            assert!(mode_report.projector.transitions_applied >= config.live_blocks);
            assert!(mode_report.publisher.segments_published >= 1);
            assert!(mode_report.publisher.total_ms > 0.0);
            assert!(mode_report.io_modes.publisher_secondary.is_some());
            assert!(mode_report.writer_fsyncs.canonical_wal_synced > 0);
            assert!(mode_report.writer_fsyncs.wallet_wal_synced > 0);
            assert!(mode_report.node_rpcs.total > 0);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_slow_readers_and_mempool_churn_without_parity_loss() -> Result<()> {
    let config = ActiveWriterPublicationConfig {
        base_blocks: 60,
        live_blocks: 12,
        reorg_window_blocks: 4,
        segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
        dense_block_cadence: 3,
        dense_txs_per_block: 1,
        commit_interval: Duration::from_millis(12),
        slow_reader_frame_delay: Duration::from_millis(4),
        mempool_churn_sizes: vec![50, 500],
        quiescent_rounds: 3,
    };

    let report = run_active_writer_publication(&config).await?;
    assert!(report.byte_parity.all_matched);

    for mode_report in &report.modes {
        assert_eq!(mode_report.errors.unexpected_grpc_errors, 0);
        assert_eq!(mode_report.mempool_churn.len(), 2);
        assert_eq!(mode_report.mempool_churn[0].batch_size, 50);
        assert_eq!(mode_report.mempool_churn[1].batch_size, 500);
        assert!(
            mode_report.mempool_churn[0]
                .mempool_tx_stream
                .frames_delivered
                > 0
        );
        assert!(
            mode_report.mempool_churn[1]
                .mempool_tx_stream
                .frames_delivered
                > 0
        );
        assert!(mode_report.slow_boundary_range.frames_delivered > 0);
    }
    Ok(())
}
