#![allow(
    missing_docs,
    reason = "Heavy perf acceptance tests describe behavior by function name."
)]

use std::time::Duration;

use eyre::Result;
use zinder_bench::active_writer_publication::{
    ActiveWriterPublicationConfig, WorkloadCoverageScope, WriterMode,
    run_active_writer_publication, validate_config,
};
use zinder_store::RocksDbIoMode;

#[ignore = "heavy perf benchmark; run with --profile=ci-perf --run-ignored=only"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_multi_hundred_mib_load_triggers_rocksdb_flushes_with_byte_parity() -> Result<()>
{
    let config = ActiveWriterPublicationConfig {
        base_blocks: 258,
        live_blocks: 210,
        reorg_window_blocks: 4,
        dense_block_cadence: 1,
        dense_txs_per_block: 2,
        dense_orchard_actions_per_tx: 900,
        tail_probe_blocks: 210,
        commit_interval: Duration::from_millis(2),
        slow_reader_frame_delay: Duration::from_millis(0),
        mempool_churn_sizes: vec![10, 50],
        quiescent_rounds: 2,
        scratch_dir: None,
        io_mode: RocksDbIoMode::Direct,
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
    assert!(report.workload.crossed_segment_boundary_height.is_none());
    assert_eq!(report.workload.probe_anchor_height, 254);

    assert!(report.byte_parity.all_matched);
    for arm in std::iter::once(&report.canonical_quiescent_control).chain(report.modes.iter()) {
        assert!(
            arm.tail_refill.peak_tail_response_bytes > 0,
            "{} reported empty tail responses",
            arm.mode.as_str()
        );
    }

    for arm in report
        .modes
        .iter()
        .filter(|arm| arm.mode != WriterMode::QuiescentControl)
    {
        assert!(
            arm.rocksdb_activity.canonical_writer.flush_write_bytes > 0,
            "{} canonical writer did not flush SSTs under sustained multi-hundred-MiB load",
            arm.mode.as_str()
        );
    }
    Ok(())
}
