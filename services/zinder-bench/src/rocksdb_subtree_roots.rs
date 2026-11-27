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
    ReadCounters, ReadEnvironment, ReadStore, ReadStoreArgs, ReadyFenceSummary, TimedRead, measure,
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
    #[arg(long, default_value_t = MAX_SUBTREE_ROOTS_PER_REQUEST, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_SUBTREE_ROOTS_PER_REQUEST)))]
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
    #[serde(flatten)]
    page: SubtreePage,
}

#[derive(Serialize)]
struct SubtreePage {
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
    io_mode: &'static str,
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
    let page = measure_page(&store, args.protocol, args.start_index, args.max_entries).await?;
    Ok(SubtreeReport {
        contract_identity: "rocksdb-subtree-roots",
        report_format_version: 1,
        environment,
        page,
    })
}

async fn measure_page(
    store: &ReadStore,
    protocol: ShieldedProtocol,
    start_index: Option<u32>,
    max_entries: u32,
) -> Result<SubtreePage, BenchError> {
    let (direct, direct_open) = store.open("direct", None)?;
    let direct_io_mode = direct.io_mode().as_str();
    let ready = direct.ready_evidence();
    let start = start_index.unwrap_or_else(|| {
        direct
            .build_plan()
            .history_predecessor()
            .tip_metadata()
            .completed_subtree_count(protocol)
    });
    let completed = direct
        .chain_epoch()?
        .tip_metadata
        .completed_subtree_count(protocol);
    let count =
        NonZeroU32::new(max_entries.min(completed.saturating_sub(start))).ok_or_else(|| {
            BenchError::invalid_argument(format!(
                "{} has no completed root at index {start} (completed {completed})",
                protocol.rpc_pool_name()
            ))
        })?;
    let range = SubtreeRootRange::new(protocol, SubtreeRootIndex::new(start), count);
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
    Ok(SubtreePage {
        ready_fence: ready.into(),
        protocol: protocol.rpc_pool_name(),
        start_index: start,
        max_entries,
        returned_roots: count.get(),
        output_digest_sha256,
        direct: SubtreeCase {
            io_mode: direct_io_mode,
            fresh_secondary_open_seconds: direct_open,
            cold: cold.into(),
            warm: warm.into(),
        },
        canonical_reader: SubtreeCase {
            io_mode: reader.secondary().io_mode().as_str(),
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

#[cfg(test)]
mod tests {
    use eyre::{Result, eyre};
    use zinder_core::{
        BlockHeight, BlockId, CommitmentTreeFrontier, CommitmentTreeFrontiers,
        CompactBlockArtifact, CompactChainMetadata, Network, SUBTREE_LEAF_COUNT, SubtreeRootHash,
    };
    use zinder_store::{CanonicalBuildSubtreeRoot, RawBlobRetention, RocksDbResourceBudget};
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, frontier_of_size,
        sample_regtest_upgrade_activations,
    };

    use super::*;

    #[tokio::test]
    async fn subtree_page_starts_after_the_predecessor_and_clamps_to_the_visible_tip() -> Result<()>
    {
        let scratch = tempfile::tempdir()?;
        let (writer, root) = retained_sapling_subtree()?;
        let store = ReadStore {
            primary: writer.canonical_primary_path().to_path_buf(),
            secondary_root: scratch.path().to_path_buf(),
            activations: sample_regtest_upgrade_activations(),
            raw_blob_retention: RawBlobRetention::None,
            reorg_policy: writer.canonical_primary()?.reorg_policy(),
            budget: zinder_store::RocksDbResourceBudget {
                preferred_io_mode: zinder_store::RocksDbIoMode::Buffered,
                ..RocksDbResourceBudget::for_local_tests()
            },
            evict_page_cache: true,
        };
        let page = measure_page(&store, ShieldedProtocol::Sapling, None, 4).await?;
        assert_eq!(page.direct.io_mode, "buffered");
        assert_eq!(page.canonical_reader.io_mode, "buffered");
        assert_eq!(page.start_index, 1);
        assert_eq!(page.max_entries, 4);
        assert_eq!(page.returned_roots, 1);
        assert_eq!(
            page.output_digest_sha256,
            page_digest(
                std::slice::from_ref(&root),
                SubtreeRootRange::new(
                    ShieldedProtocol::Sapling,
                    SubtreeRootIndex::new(1),
                    NonZeroU32::MIN,
                )
            )?
        );

        let empty_secondary = tempfile::tempdir()?;
        let empty_pool = measure_page(
            &ReadStore {
                secondary_root: empty_secondary.path().to_path_buf(),
                ..store
            },
            ShieldedProtocol::Orchard,
            None,
            4,
        )
        .await;
        assert!(matches!(
            empty_pool,
            Err(BenchError::InvalidArgument { reason })
                if reason == "orchard has no completed root at index 0 (completed 0)"
        ));
        Ok(())
    }

    fn retained_sapling_subtree() -> Result<(CanonicalWriterFixture, SubtreeRootArtifact)> {
        let floor = BlockHeight::new(2);
        let tip = BlockHeight::new(3);
        let mut chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::None)
            .extend_blocks(tip.value());
        for (height, completed) in [(floor, 1), (tip, 2)] {
            let block = chain
                .block_at(height)
                .ok_or_else(|| eyre!("missing fixture block"))?;
            let tree_size = completed * SUBTREE_LEAF_COUNT;
            let compact = CompactBlockArtifact::empty(
                BlockId::new(height, block.hash),
                block.parent_hash,
                block.block_time_seconds,
                CompactChainMetadata {
                    sapling_commitment_tree_size: tree_size,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            );
            let frontiers = CommitmentTreeFrontiers::from_validated_parts(
                Some(frontier_of_size(ShieldedProtocol::Sapling, tree_size)?),
                Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
                None,
            )?;
            chain = chain
                .with_compact_block_artifact(compact)
                .with_commitment_tree_frontiers_at(height, frontiers);
        }
        let root = SubtreeRootArtifact::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(1),
            SubtreeRootHash::from_bytes([0x17; 32]),
            tip,
            chain
                .tip_hash()
                .ok_or_else(|| eyre!("missing fixture tip"))?,
        );
        let baseline = CanonicalBaseline::new(&chain)
            .with_history_floor(floor)
            .with_subtree_root(CanonicalBuildSubtreeRoot {
                protocol: root.protocol,
                subtree_index: root.subtree_index,
                root_hash: root.root_hash,
                completing_block_height: root.completing_block_height,
            });
        let writer =
            CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?;
        Ok((writer, root))
    }
}
