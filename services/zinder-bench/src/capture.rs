//! Fixed-range capture: pull raw source payloads from a live Zebra node and
//! write them into a deterministic fixture directory.

use std::{
    collections::HashMap,
    future::Future,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt as _;
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockFactsSequenceDigestVersion, Network, NetworkUpgradeActivations, ShieldedProtocol,
    SubtreeRootIndex, wire::encode_zinder_native_chain_name,
};
use zinder_ingest::{RawBlobPolicy, prepare_canonical_block};
use zinder_source::{
    NodeAuth, NodeSource, SourceBlock, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION;

use crate::{
    error::BenchError,
    fixture::{
        ActivationRecord, CanonicalBlockFactsDigestEvidence, CapturedBlockId, CapturedSubtreeRoot,
        FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION, FixtureManifest, SegmentDescriptor,
        SubtreeRootSet, WorkloadDensity, write_segment,
    },
};

const SUBTREE_ROOT_PAGE_SIZE: u32 = 1_000;

/// Inputs for one fixed-range capture.
#[derive(Clone, Debug)]
pub struct CaptureConfig {
    /// Network the node answers for.
    pub network: Network,
    /// Node JSON-RPC base URL.
    pub json_rpc_addr: String,
    /// Node authentication mode.
    pub node_auth: NodeAuth,
    /// First block height to capture.
    pub from_height: BlockHeight,
    /// Last block height to capture.
    pub to_height: BlockHeight,
    /// Blocks written per segment file.
    pub segment_blocks: NonZeroU32,
    /// Concurrent block fetches issued against the node.
    pub fetch_concurrency: NonZeroU32,
    /// Concurrent block-local preparations used to measure captured blocks.
    pub prepare_concurrency: NonZeroU32,
    /// Per-request source timeout.
    pub request_timeout: Duration,
    /// Maximum JSON-RPC response body size.
    pub max_response_bytes: NonZeroU64,
    /// Destination fixture directory.
    pub output_directory: PathBuf,
}

/// Captures the configured block range into a fixture directory and returns the
/// manifest that was written.
pub async fn capture_fixed_range(config: CaptureConfig) -> Result<FixtureManifest, BenchError> {
    if config.from_height.value() > config.to_height.value() {
        return Err(BenchError::invalid_argument(
            "from_height must be less than or equal to to_height",
        ));
    }
    std::fs::create_dir_all(&config.output_directory)
        .map_err(|source| BenchError::io(&config.output_directory, source))?;

    let source = ZebraJsonRpcSource::with_options(
        config.network,
        &config.json_rpc_addr,
        config.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: config.request_timeout,
            max_response_bytes: config.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;

    let activations =
        zinder_source::discover_network_upgrade_activations(&source, "zinder-bench").await?;
    let activation_records = activations
        .activations()
        .iter()
        .map(|activation| ActivationRecord {
            branch_id: activation.branch_id.value(),
            activation_height: activation.activation_height.value(),
            name: activation.name.clone(),
        })
        .collect();

    let CapturedSegments {
        segments,
        tip_hash_hex,
        workload_density,
        canonical_block_facts_digest_evidence,
        block_ids_by_height,
    } = capture_segments(&source, &config, &activations).await?;

    let subtree_roots = capture_subtree_root_set(
        &source,
        config.network,
        config.to_height,
        &block_ids_by_height,
    )
    .await?;

    let block_count = config
        .to_height
        .value()
        .saturating_sub(config.from_height.value())
        .saturating_add(1);
    let manifest = FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(config.network).to_owned(),
        from_height: config.from_height.value(),
        to_height: config.to_height.value(),
        block_count,
        workload_density,
        canonical_artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence,
        tip_hash_hex,
        network_upgrade_activations: activation_records,
        segments,
        subtree_roots,
    };
    manifest.write(&config.output_directory)?;
    Ok(manifest)
}

struct CapturedSegments {
    segments: Vec<SegmentDescriptor>,
    tip_hash_hex: String,
    workload_density: WorkloadDensity,
    canonical_block_facts_digest_evidence: CanonicalBlockFactsDigestEvidence,
    block_ids_by_height: HashMap<u32, BlockId>,
}

/// Density and canonical-fact digest evidence computed while parsing fixture blocks once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureBlockMeasurements {
    /// Workload density derived from the prepared block-local facts.
    pub workload_density: WorkloadDensity,
    block_digests: Vec<CanonicalBlockFactsDigest>,
}

struct FixtureBlockMeasurement {
    workload_density: WorkloadDensity,
    block_digest: CanonicalBlockFactsDigest,
}

impl FixtureBlockMeasurements {
    /// Builds constant-size digest evidence for exactly these measured blocks.
    pub fn canonical_block_facts_digest_evidence(
        &self,
    ) -> Result<CanonicalBlockFactsDigestEvidence, BenchError> {
        digest_evidence(&self.block_digests)
    }
}

async fn capture_segments(
    source: &ZebraJsonRpcSource,
    config: &CaptureConfig,
    activations: &NetworkUpgradeActivations,
) -> Result<CapturedSegments, BenchError> {
    let mut segments = Vec::new();
    let mut tip_hash_hex = String::new();
    let mut segment_index = 0_u32;
    let mut segment_from = config.from_height.value();
    let mut workload_density = WorkloadDensity::default();
    let mut sequence_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    let mut block_ids_by_height = HashMap::new();
    while segment_from <= config.to_height.value() {
        let segment_started = Instant::now();
        let segment_to = segment_end_height(segment_from, config.segment_blocks, config.to_height);
        let fetch_started = Instant::now();
        let blocks: Arc<[SourceBlock]> =
            fetch_segment_blocks(source, segment_from, segment_to, config.fetch_concurrency)
                .await?
                .into();
        let fetch_seconds = fetch_started.elapsed().as_secs_f64();
        let prepare_started = Instant::now();
        let measurements = measure_fixture_blocks_bounded(
            Arc::clone(&blocks),
            activations,
            config.prepare_concurrency,
        )
        .await?;
        let prepare_seconds = prepare_started.elapsed().as_secs_f64();
        workload_density.merge(measurements.workload_density);
        for digest in measurements.block_digests {
            append_sequence_digest(&mut sequence_builder, digest)?;
        }
        for block in blocks.iter() {
            let block_id = BlockId::new(block.height, block.hash);
            if block_ids_by_height
                .insert(block.height.value(), block_id)
                .is_some()
            {
                return Err(BenchError::fixture_format(format!(
                    "captured block height {} appears more than once",
                    block.height.value()
                )));
            }
        }
        if let Some(last) = blocks.last()
            && last.height.value() == config.to_height.value()
        {
            tip_hash_hex = hex::encode(last.hash.as_bytes());
        }
        let segment_write_started = Instant::now();
        let descriptor = write_segment(&config.output_directory, segment_index, &blocks)?;
        let segment_write_seconds = segment_write_started.elapsed().as_secs_f64();
        tracing::info!(
            target: "zinder::bench",
            event = "segment_captured",
            index = descriptor.index,
            from_height = descriptor.from_height,
            to_height = descriptor.to_height,
            block_count = descriptor.block_count,
            fetch_seconds,
            prepare_seconds,
            segment_write_seconds,
            segment_seconds = segment_started.elapsed().as_secs_f64(),
            prepare_concurrency = config.prepare_concurrency.get(),
            "captured source segment"
        );
        segments.push(descriptor);
        segment_index = segment_index.saturating_add(1);
        segment_from = segment_to.saturating_add(1);
    }
    Ok(CapturedSegments {
        segments,
        tip_hash_hex,
        workload_density,
        canonical_block_facts_digest_evidence: digest_evidence_from_sequence(
            sequence_builder.finish(),
        ),
        block_ids_by_height,
    })
}

/// Parses captured blocks once to compute workload density and canonical-fact digests.
pub fn measure_fixture_blocks(
    blocks: &[SourceBlock],
    activations: &NetworkUpgradeActivations,
) -> Result<FixtureBlockMeasurements, BenchError> {
    let mut density = WorkloadDensity::default();
    let mut block_digests = Vec::with_capacity(blocks.len());
    for block in blocks {
        let measurement = measure_fixture_block(block, activations)?;
        density.merge(measurement.workload_density);
        block_digests.push(measurement.block_digest);
    }
    Ok(FixtureBlockMeasurements {
        workload_density: density,
        block_digests,
    })
}

/// Parses captured blocks concurrently while preserving their source order in
/// the canonical-fact sequence digest.
async fn measure_fixture_blocks_bounded(
    blocks: Arc<[SourceBlock]>,
    activations: &NetworkUpgradeActivations,
    prepare_concurrency: NonZeroU32,
) -> Result<FixtureBlockMeasurements, BenchError> {
    let activations = Arc::new(activations.clone());
    let block_count = blocks.len();
    let tasks = (0..block_count).map(|block_index| {
        let blocks = Arc::clone(&blocks);
        let activations = Arc::clone(&activations);
        async move {
            tokio::task::spawn_blocking(move || {
                measure_fixture_block(&blocks[block_index], &activations)
            })
            .await
            .map_err(|source| {
                BenchError::canonical_replay_storage_preparation_task(source.to_string())
            })?
        }
    });
    collect_fixture_block_measurements(tasks, prepare_concurrency, block_count).await
}

async fn collect_fixture_block_measurements<F>(
    tasks: impl IntoIterator<Item = F>,
    prepare_concurrency: NonZeroU32,
    block_count: usize,
) -> Result<FixtureBlockMeasurements, BenchError>
where
    F: Future<Output = Result<FixtureBlockMeasurement, BenchError>>,
{
    let concurrency = usize::try_from(prepare_concurrency.get()).unwrap_or(usize::MAX);
    let outcomes = futures_util::stream::iter(tasks).buffered(concurrency);
    futures_util::pin_mut!(outcomes);
    let mut workload_density = WorkloadDensity::default();
    let mut block_digests = Vec::with_capacity(block_count);
    while let Some(measurement) = outcomes.next().await {
        let measurement = measurement?;
        workload_density.merge(measurement.workload_density);
        block_digests.push(measurement.block_digest);
    }
    Ok(FixtureBlockMeasurements {
        workload_density,
        block_digests,
    })
}

fn measure_fixture_block(
    block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
) -> Result<FixtureBlockMeasurement, BenchError> {
    let prepared = prepare_canonical_block(block, activations, RawBlobPolicy::None)?;
    let transaction_count = u32::try_from(prepared.facts.transactions.len()).map_err(|_| {
        BenchError::fixture_format("block transaction count exceeds u32".to_owned())
    })?;
    let transparent_input_count = prepared
        .facts
        .transactions
        .iter()
        .map(|transaction| transaction.transparent_inputs.len())
        .sum::<usize>();
    let transparent_input_count = u32::try_from(transparent_input_count).map_err(|_| {
        BenchError::fixture_format("block transparent input count exceeds u32".to_owned())
    })?;
    let transparent_output_count = u32::try_from(
        prepared
            .facts
            .transactions
            .iter()
            .map(|transaction| transaction.transparent_outputs.len())
            .sum::<usize>(),
    )
    .map_err(|_| {
        BenchError::fixture_format("block transparent output count exceeds u32".to_owned())
    })?;
    let raw_block_bytes = u64::try_from(block.raw_block_bytes.len()).unwrap_or(u64::MAX);
    let reference_encoding = prepared
        .facts
        .reference_encoding(CanonicalBlockFactsDigestVersion::CURRENT);
    Ok(FixtureBlockMeasurement {
        workload_density: WorkloadDensity {
            block_count: 1,
            raw_block_bytes,
            transaction_count: u64::from(transaction_count),
            transparent_input_count: u64::from(transparent_input_count),
            transparent_output_count: u64::from(transparent_output_count),
            blocks_with_transparent_inputs: u32::from(transparent_input_count > 0),
            blocks_with_transparent_outputs: u32::from(transparent_output_count > 0),
            max_raw_block_bytes_per_block: raw_block_bytes,
            max_transactions_per_block: transaction_count,
            max_transparent_inputs_per_block: transparent_input_count,
            max_transparent_outputs_per_block: transparent_output_count,
        },
        block_digest: reference_encoding.digest(),
    })
}

fn digest_evidence(
    block_digests: &[CanonicalBlockFactsDigest],
) -> Result<CanonicalBlockFactsDigestEvidence, BenchError> {
    let mut sequence_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    for digest in block_digests {
        append_sequence_digest(&mut sequence_builder, *digest)?;
    }
    Ok(digest_evidence_from_sequence(sequence_builder.finish()))
}

fn append_sequence_digest(
    sequence_builder: &mut CanonicalBlockFactsSequenceDigestBuilder,
    digest: CanonicalBlockFactsDigest,
) -> Result<(), BenchError> {
    sequence_builder.try_append(digest).map_err(|source| {
        BenchError::fixture_format(format!(
            "canonical block facts sequence digest failed: {source}"
        ))
    })
}

fn digest_evidence_from_sequence(
    sequence_digest: CanonicalBlockFactsSequenceDigest,
) -> CanonicalBlockFactsDigestEvidence {
    CanonicalBlockFactsDigestEvidence {
        block_digest_version: CanonicalBlockFactsDigestVersion::CURRENT.value(),
        sequence_digest_version: sequence_digest.version().value(),
        block_count: sequence_digest.block_count(),
        sequence_digest_sha256: hex::encode(sequence_digest.as_bytes()),
    }
}

const fn segment_end_height(
    segment_from: u32,
    segment_blocks: NonZeroU32,
    to_height: BlockHeight,
) -> u32 {
    let last = segment_from.saturating_add(segment_blocks.get().saturating_sub(1));
    if last < to_height.value() {
        last
    } else {
        to_height.value()
    }
}

async fn fetch_segment_blocks(
    source: &ZebraJsonRpcSource,
    segment_from: u32,
    segment_to: u32,
    fetch_concurrency: NonZeroU32,
) -> Result<Vec<SourceBlock>, BenchError> {
    let heights = segment_from..=segment_to;
    let ordered = futures_util::stream::iter(heights)
        .map(|height| source.fetch_block_at(BlockHeight::new(height)))
        .buffered(fetch_concurrency.get() as usize)
        .collect::<Vec<_>>()
        .await;
    let mut blocks = Vec::with_capacity(ordered.len());
    for outcome in ordered {
        blocks.push(outcome?);
    }
    Ok(blocks)
}

async fn capture_subtree_roots<S: NodeSource>(
    source: &S,
    network: Network,
    protocol: ShieldedProtocol,
    to_height: BlockHeight,
    block_ids: &mut SubtreeCompletionBlockIds<'_>,
) -> Result<Vec<CapturedSubtreeRoot>, BenchError> {
    let Some(page_size) = NonZeroU32::new(SUBTREE_ROOT_PAGE_SIZE) else {
        return Ok(Vec::new());
    };
    let mut records = Vec::new();
    let mut next_index = 0_u32;
    loop {
        let response = source
            .fetch_subtree_roots(protocol, SubtreeRootIndex::new(next_index), page_size)
            .await?;
        if response.protocol != protocol
            || response.start_index != SubtreeRootIndex::new(next_index)
        {
            return Err(BenchError::fixture_format(format!(
                "{protocol:?} subtree-root response does not match requested protocol and start index {next_index}"
            )));
        }
        let returned = response.subtree_roots.len();
        if returned == 0 {
            break;
        }
        let mut passed_range = false;
        for root in response.subtree_roots {
            if root.subtree_index != SubtreeRootIndex::new(next_index) {
                return Err(BenchError::fixture_format(format!(
                    "{protocol:?} subtree-root response has index {}, expected {next_index}",
                    root.subtree_index.value()
                )));
            }
            if root.completing_block_height.value() > to_height.value() {
                passed_range = true;
                break;
            }
            let completing_block = capture_completing_block_id(
                source,
                network,
                root.completing_block_height,
                block_ids.captured_range,
                block_ids.fetched_by_height,
            )
            .await?;
            records.push(CapturedSubtreeRoot {
                protocol: protocol.rpc_pool_name().to_owned(),
                index: root.subtree_index.value(),
                root_hash_hex: hex::encode(root.root_hash.as_bytes()),
                completing_block: CapturedBlockId {
                    height: completing_block.height.value(),
                    hash_hex: hex::encode(completing_block.hash.as_bytes()),
                },
            });
            next_index = root.subtree_index.value().checked_add(1).ok_or_else(|| {
                BenchError::fixture_format(format!(
                    "{protocol:?} subtree-root index exceeds the fixture format"
                ))
            })?;
        }
        if passed_range || returned < page_size.get() as usize {
            break;
        }
    }
    Ok(records)
}

struct SubtreeCompletionBlockIds<'a> {
    captured_range: &'a HashMap<u32, BlockId>,
    fetched_by_height: &'a mut HashMap<u32, BlockId>,
}

async fn capture_subtree_root_set<S: NodeSource>(
    source: &S,
    network: Network,
    to_height: BlockHeight,
    captured_block_ids: &HashMap<u32, BlockId>,
) -> Result<SubtreeRootSet, BenchError> {
    let mut fetched_by_height = HashMap::new();
    let mut block_ids = SubtreeCompletionBlockIds {
        captured_range: captured_block_ids,
        fetched_by_height: &mut fetched_by_height,
    };
    Ok(SubtreeRootSet {
        sapling: capture_subtree_roots(
            source,
            network,
            ShieldedProtocol::Sapling,
            to_height,
            &mut block_ids,
        )
        .await?,
        orchard: capture_subtree_roots(
            source,
            network,
            ShieldedProtocol::Orchard,
            to_height,
            &mut block_ids,
        )
        .await?,
        ironwood: capture_subtree_roots(
            source,
            network,
            ShieldedProtocol::Ironwood,
            to_height,
            &mut block_ids,
        )
        .await?,
    })
}

async fn capture_completing_block_id<S: NodeSource>(
    source: &S,
    network: Network,
    height: BlockHeight,
    captured_block_ids: &HashMap<u32, BlockId>,
    completing_block_ids: &mut HashMap<u32, BlockId>,
) -> Result<BlockId, BenchError> {
    if let Some(block_id) = completing_block_ids.get(&height.value()) {
        return Ok(*block_id);
    }
    let block = source.fetch_block_at(height).await?;
    if block.network != network || block.height != height {
        return Err(BenchError::fixture_format(format!(
            "subtree completion block request for {network:?} height {} returned {:?} height {}",
            height.value(),
            block.network,
            block.height.value()
        )));
    }
    let block_id = BlockId::new(block.height, block.hash);
    if captured_block_ids
        .get(&height.value())
        .is_some_and(|captured| captured != &block_id)
    {
        return Err(BenchError::fixture_format(format!(
            "subtree completion block at height {} differs from the captured fixture range",
            height.value()
        )));
    }
    completing_block_ids.insert(height.value(), block_id);
    Ok(block_id)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error, num::NonZeroU32, sync::Arc};

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use serde_json::Value;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CanonicalBlockFactsDigest,
        CanonicalBlockFactsDigestVersion, Network, ShieldedProtocol, SubtreeRootHash,
        SubtreeRootIndex,
    };
    use zinder_source::{
        NodeCapabilities, NodeSource, SourceBlock, SourceError, SourceSubtreeRoot,
        SourceSubtreeRoots,
    };
    use zinder_testkit::sample_regtest_upgrade_activations;

    use super::{
        FixtureBlockMeasurement, SubtreeCompletionBlockIds, capture_subtree_roots,
        collect_fixture_block_measurements, measure_fixture_blocks, measure_fixture_blocks_bounded,
    };

    const REGTEST_BLOCK_1: &str =
        include_str!("../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
    const REGTEST_BLOCK_603: &str =
        include_str!("../../zinder-ingest/tests/fixtures/z3-regtest-ironwood-block-603.json");

    #[derive(Clone)]
    struct SubtreeCaptureSource {
        completing_block: SourceBlock,
        requested_block_heights: Arc<Mutex<Vec<BlockHeight>>>,
    }

    #[async_trait]
    impl NodeSource for SubtreeCaptureSource {
        fn capabilities(&self) -> NodeCapabilities {
            NodeCapabilities::default()
        }

        async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
            self.requested_block_heights.lock().push(height);
            if height == self.completing_block.height {
                Ok(self.completing_block.clone())
            } else {
                Err(SourceError::BlockUnavailable {
                    height,
                    reason: "test source has only the subtree completion block".to_owned(),
                })
            }
        }

        async fn tip_id(&self) -> Result<BlockId, SourceError> {
            Ok(BlockId::new(
                self.completing_block.height,
                self.completing_block.hash,
            ))
        }

        async fn fetch_subtree_roots(
            &self,
            protocol: ShieldedProtocol,
            start_index: SubtreeRootIndex,
            _max_entries: NonZeroU32,
        ) -> Result<SourceSubtreeRoots, SourceError> {
            let roots = (start_index == SubtreeRootIndex::new(0))
                .then(|| {
                    SourceSubtreeRoot::new(
                        SubtreeRootIndex::new(0),
                        SubtreeRootHash::from_bytes([0x51; 32]),
                        self.completing_block.height,
                    )
                })
                .into_iter()
                .collect::<Vec<_>>();
            Ok(SourceSubtreeRoots::new(protocol, start_index, roots))
        }
    }

    #[tokio::test]
    async fn subtree_capture_fetches_and_records_the_exact_historical_completing_block()
    -> Result<(), Box<dyn Error>> {
        let completing_block = source_block(REGTEST_BLOCK_1)?;
        let requested_block_heights = Arc::new(Mutex::new(Vec::new()));
        let source = SubtreeCaptureSource {
            completing_block: completing_block.clone(),
            requested_block_heights: Arc::clone(&requested_block_heights),
        };
        let captured_range = HashMap::new();
        let mut fetched_by_height = HashMap::new();
        let mut block_ids = SubtreeCompletionBlockIds {
            captured_range: &captured_range,
            fetched_by_height: &mut fetched_by_height,
        };
        let records = capture_subtree_roots(
            &source,
            Network::ZcashRegtest,
            ShieldedProtocol::Sapling,
            BlockHeight::new(603),
            &mut block_ids,
        )
        .await?;

        assert_eq!(
            requested_block_heights.lock().as_slice(),
            [BlockHeight::new(1)]
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].completing_block.height, 1);
        assert_eq!(
            records[0].completing_block.hash_hex,
            hex::encode(completing_block.hash.as_bytes())
        );
        Ok(())
    }

    #[tokio::test]
    async fn subtree_capture_rejects_a_completion_identity_that_disagrees_with_the_segment()
    -> Result<(), Box<dyn Error>> {
        let completing_block = source_block(REGTEST_BLOCK_1)?;
        let source = SubtreeCaptureSource {
            completing_block: completing_block.clone(),
            requested_block_heights: Arc::new(Mutex::new(Vec::new())),
        };
        let captured_block_ids = HashMap::from([(
            completing_block.height.value(),
            BlockId::new(completing_block.height, BlockHash::from_bytes([0x9a; 32])),
        )]);
        let mut fetched_by_height = HashMap::new();
        let mut block_ids = SubtreeCompletionBlockIds {
            captured_range: &captured_block_ids,
            fetched_by_height: &mut fetched_by_height,
        };
        let error = capture_subtree_roots(
            &source,
            Network::ZcashRegtest,
            ShieldedProtocol::Sapling,
            BlockHeight::new(603),
            &mut block_ids,
        )
        .await
        .err()
        .ok_or("a completion identity mismatch must be rejected")?;

        assert!(
            error
                .to_string()
                .contains("differs from the captured fixture range")
        );
        Ok(())
    }

    #[tokio::test]
    async fn bounded_measurements_equal_serial_measurements() -> Result<(), Box<dyn Error>> {
        let blocks = vec![
            source_block(REGTEST_BLOCK_1)?,
            source_block(REGTEST_BLOCK_603)?,
        ];
        let activations = sample_regtest_upgrade_activations();
        let serial = measure_fixture_blocks(&blocks, &activations)?;
        let concurrency = NonZeroU32::new(2).ok_or("2 must be non-zero")?;

        let bounded =
            measure_fixture_blocks_bounded(Arc::from(blocks), &activations, concurrency).await?;

        assert_eq!(bounded, serial);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_measurements_preserve_input_order_after_out_of_order_completion()
    -> Result<(), Box<dyn Error>> {
        let (completion_sender, mut completion_receiver) = tokio::sync::mpsc::unbounded_channel();
        let task_completion_sender = completion_sender.clone();
        let tasks =
            [(0_u8, 30_u64), (1, 1), (2, 10)]
                .into_iter()
                .map(move |(index, delay_millis)| {
                    let completion_sender = task_completion_sender.clone();
                    async move {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_millis)).await;
                        completion_sender.send(index).map_err(|source| {
                            crate::BenchError::invalid_argument(source.to_string())
                        })?;
                        Ok(FixtureBlockMeasurement {
                            workload_density: crate::fixture::WorkloadDensity {
                                block_count: 1,
                                raw_block_bytes: u64::from(index) + 1,
                                ..crate::fixture::WorkloadDensity::default()
                            },
                            block_digest: digest_for_index(index),
                        })
                    }
                });
        drop(completion_sender);
        let concurrency = NonZeroU32::new(3).ok_or("3 must be non-zero")?;

        let measurements = collect_fixture_block_measurements(tasks, concurrency, 3).await?;
        let mut completion_order = Vec::new();
        while let Some(index) = completion_receiver.recv().await {
            completion_order.push(index);
        }

        assert_eq!(completion_order, [1, 2, 0]);
        assert_eq!(
            measurements.block_digests,
            [
                digest_for_index(0),
                digest_for_index(1),
                digest_for_index(2)
            ]
        );
        assert_eq!(measurements.workload_density.raw_block_bytes, 6);
        Ok(())
    }

    fn digest_for_index(index: u8) -> CanonicalBlockFactsDigest {
        CanonicalBlockFactsDigest::from_reference_encoding(
            CanonicalBlockFactsDigestVersion::CURRENT,
            &[index],
        )
    }

    fn source_block(encoded_fixture: &str) -> Result<SourceBlock, Box<dyn Error>> {
        let fixture: Value = serde_json::from_str(encoded_fixture)?;
        let height = fixture
            .get("height")
            .and_then(Value::as_u64)
            .and_then(|height| u32::try_from(height).ok())
            .ok_or("fixture height must fit in u32")?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex must be a string")?;
        Ok(SourceBlock::from_raw_block_bytes(
            Network::ZcashRegtest,
            BlockHeight::new(height),
            hex::decode(raw_block_hex)?,
        )?)
    }
}
