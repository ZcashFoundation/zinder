//! Cold and warm reads of one retained subtree-root page.

use std::{num::NonZeroU32, sync::Arc};

use clap::Args;
use serde::Serialize;
use sha2::{Digest, Sha256};
use zinder_bench::BenchError;
use zinder_core::{
    MAX_SUBTREE_ROOTS_PER_REQUEST, ShieldedProtocol, SubtreeRootArtifact, SubtreeRootIndex,
    SubtreeRootRange,
};
use zinder_query::{CanonicalReader, RocksDbCanonicalReader, read_on_blocking_thread};
use zinder_store::RocksDbStatisticsLevel;

use crate::rocksdb_read_bench::{
    ReadCounters, ReadEnvironment, ReadStoreArgs, ReadyFenceSummary, TimedRead, measure,
    measure_reader,
};

#[derive(Args)]
pub(crate) struct SubtreeArgs {
    #[command(flatten)]
    pub(crate) store: ReadStoreArgs,
    /// Shielded pool to read.
    #[arg(long, default_value = "sapling", value_parser = parse_protocol)]
    protocol: ShieldedProtocol,
    /// First index to read. Defaults to the first root completed within retained history.
    #[arg(long)]
    start_index: Option<u32>,
    /// Maximum roots in the page, capped at the visible tree size.
    #[arg(long, default_value = "1000", value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_SUBTREE_ROOTS_PER_REQUEST)))]
    max_entries: u32,
}

fn parse_protocol(name: &str) -> Result<ShieldedProtocol, String> {
    ShieldedProtocol::from_rpc_pool_name(name)
        .ok_or_else(|| "expected sapling, orchard, or ironwood".to_owned())
}

#[derive(Serialize)]
pub(crate) struct SubtreeReport {
    contract_identity: &'static str,
    report_format_version: u16,
    #[serde(flatten)]
    environment: ReadEnvironment,
    ready_fence: ReadyFenceSummary,
    protocol: &'static str,
    start_index: u32,
    max_entries: u32,
    returned_roots: u32,
    output_digest_sha256: String,
    direct: SubtreeCase,
    canonical_reader: SubtreeCase,
}

#[derive(Serialize)]
struct SubtreeCase {
    fresh_secondary_open_seconds: f64,
    cold: SubtreeMeasurement,
    warm: SubtreeMeasurement,
}

#[derive(Serialize)]
struct SubtreeMeasurement {
    seconds: f64,
    #[serde(flatten)]
    counters: ReadCounters,
}

impl From<TimedRead<Vec<SubtreeRootArtifact>>> for SubtreeMeasurement {
    fn from(sample: TimedRead<Vec<SubtreeRootArtifact>>) -> Self {
        Self {
            seconds: sample.seconds,
            counters: sample.counters,
        }
    }
}

pub(crate) async fn run(args: &SubtreeArgs) -> Result<SubtreeReport, BenchError> {
    let (store, environment) = args.store.prepare()?;
    let (direct, direct_open) = store.open("direct", None)?;
    let ready = direct.ready_evidence();
    let start = args.start_index.unwrap_or_else(|| {
        direct
            .build_plan()
            .history_predecessor()
            .tip_metadata()
            .completed_subtree_count(args.protocol)
    });
    let completed = direct
        .chain_epoch()?
        .tip_metadata
        .completed_subtree_count(args.protocol);
    let count = NonZeroU32::new(args.max_entries.min(completed.saturating_sub(start))).ok_or_else(
        || {
            BenchError::invalid_argument(format!(
                "{} has no completed root at index {start} (completed {completed})",
                args.protocol.rpc_pool_name()
            ))
        },
    )?;
    let range = SubtreeRootRange::new(args.protocol, SubtreeRootIndex::new(start), count);
    let level = store.budget.statistics_level;
    let direct_read = || measure(&direct, level, || Ok(direct.subtree_roots(range)?));
    let cold = direct_read()?;
    let warm = direct_read()?;
    drop(direct);
    let (secondary, reader_open) = store.open("reader", Some(&ready))?;
    let reader = Arc::new(
        RocksDbCanonicalReader::new(secondary)
            .map_err(|error| BenchError::canonical_read(error.to_string()))?,
    );
    let reader_cold = read_page(&reader, level, range).await?;
    let reader_warm = read_page(&reader, level, range).await?;
    if cold.rows.len() != range.into_iter().len()
        || [&warm.rows, &reader_cold.rows, &reader_warm.rows]
            .into_iter()
            .any(|roots| *roots != cold.rows)
    {
        return Err(BenchError::report_format(
            "subtree-root page is incomplete or differs between reads",
        ));
    }
    let output_digest_sha256 = page_digest(&cold.rows, range)?;
    Ok(SubtreeReport {
        contract_identity: "rocksdb-subtree-roots",
        report_format_version: 1,
        environment,
        ready_fence: ready.into(),
        protocol: args.protocol.rpc_pool_name(),
        start_index: start,
        max_entries: args.max_entries,
        returned_roots: count.get(),
        output_digest_sha256,
        direct: SubtreeCase {
            fresh_secondary_open_seconds: direct_open,
            cold: cold.into(),
            warm: warm.into(),
        },
        canonical_reader: SubtreeCase {
            fresh_secondary_open_seconds: reader_open,
            cold: reader_cold.into(),
            warm: reader_warm.into(),
        },
    })
}

async fn read_page(
    reader: &Arc<RocksDbCanonicalReader>,
    level: RocksDbStatisticsLevel,
    range: SubtreeRootRange,
) -> Result<TimedRead<Vec<SubtreeRootArtifact>>, BenchError> {
    measure_reader(Arc::clone(reader), level, move |reader| {
        read_on_blocking_thread(reader.subtree_roots(range))
    })
    .await
}

fn page_digest(
    roots: &[SubtreeRootArtifact],
    range: SubtreeRootRange,
) -> Result<String, BenchError> {
    let mut digest = Sha256::new();
    digest.update(b"zinder-bench-subtree-roots-v1\0");
    for (root, index) in roots.iter().zip(range) {
        if root.protocol != range.protocol || root.subtree_index != index {
            return Err(BenchError::report_format(
                "subtree-root page contains an unexpected index or pool",
            ));
        }
        digest.update([root.protocol.id()]);
        digest.update(index.value().to_be_bytes());
        digest.update(root.root_hash.as_bytes());
        digest.update(root.completing_block_height.value().to_be_bytes());
        digest.update(root.completing_block_hash.as_bytes());
    }
    Ok(hex::encode(digest.finalize()))
}
