#![allow(
    missing_docs,
    reason = "Heavy perf acceptance tests describe behavior by function name."
)]

use std::{num::NonZeroU32, time::Duration};

use eyre::Result;
use zinder_bench::active_writer_publication::{
    ActiveWriterPublicationConfig, WriterMode, run_active_writer_publication, validate_config,
};
use zinder_store::RocksDbIoMode;

#[ignore = "heavy perf benchmark; run with --profile=ci-perf --run-ignored=only"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encoded_tail_evicts_at_64mib_and_triggers_rocksdb_flushes() -> Result<()> {
    let config = ActiveWriterPublicationConfig {
        base_blocks: 258,
        live_blocks: 210,
        reorg_window_blocks: 4,
        segment_blocks: NonZeroU32::new(256).unwrap_or(NonZeroU32::MIN),
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
        publish_workers: None,
        require_tail_eviction: true,
    };
    validate_config(&config)?;

    let report = run_active_writer_publication(&config).await?;
    report.validate(&config)?;

    assert!(report.byte_parity.all_matched);
    let evicted = [
        &report.canonical_quiescent_control,
        &report.published_quiescent_control,
    ]
    .into_iter()
    .chain(report.modes.iter())
    .any(|arm| {
        arm.tail_refill.encoded_tail_byte_cap_evictions > 0
            && arm.tail_refill.byte_cap_exceeded
            && arm.tail_refill.encoded_tail_byte_capped_fills > 0
            && arm.tail_refill.peak_encoded_tail_bytes > 0
    });
    assert!(
        evicted,
        "expected >64 MiB EncodedTail eviction in at least one arm"
    );

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
