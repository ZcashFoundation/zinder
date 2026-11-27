//! Compact-block range measurements through the admitted canonical secondary
//! API and through the canonical reader `zinder-query` serves from.

use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};
use zinder_bench::BenchError;
use zinder_compat_lightwalletd::{BLOCK_RANGE_CHUNK_MAX_BYTES, BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES};
use zinder_core::{BlockHeight, BlockHeightRange, CompactBlockArtifact};
use zinder_query::{
    CanonicalReader, DEFAULT_MAX_COMPACT_BLOCK_RANGE, RocksDbCanonicalReader,
    read_on_blocking_thread,
};
use zinder_store::{CanonicalStoreReadyEvidence, HeightOrder, RocksDbStatisticsLevel};

use crate::rocksdb_read_bench::{
    ReadCounters, ReadEnvironment, ReadStore, ReadStoreArgs, ReadyFenceSummary, TimedRead, measure,
    measure_reader,
};

const RANGE_BLOCK_COUNTS: [u32; 3] = [128, 512, 1_024];
const OUTPUT_DIGEST_DOMAIN: &[u8] = b"zinder-bench-compact-block-range-output-v2\0";

#[derive(Serialize)]
pub(crate) struct RocksDbCompactBlockRangeReport {
    contract_identity: &'static str,
    report_format_version: u16,
    #[serde(flatten)]
    environment: ReadEnvironment,
    ready_fence: ReadyFenceSummary,
    cases: Vec<CompactBlockRangeCase>,
    bounded_cases: Vec<CompactBlockRangeCase>,
}

#[derive(Serialize)]
struct CompactBlockRangeCase {
    io_mode: &'static str,
    order: &'static str,
    max_bytes: Option<usize>,
    requested_block_count: u32,
    range_start_height: u32,
    range_end_height: u32,
    fresh_secondary_open_seconds: f64,
    cold: CompactBlockRangeMeasurement,
    warm: CompactBlockRangeMeasurement,
    canonical_reader: CanonicalReaderRangeCase,
}

/// The same range read through `zinder_query::CanonicalReader` on a second
/// fresh secondary, including the hop to the blocking pool that
/// `zinder-query` makes for every read.
#[derive(Serialize)]
struct CanonicalReaderRangeCase {
    io_mode: &'static str,
    fresh_secondary_open_seconds: f64,
    cold: CompactBlockRangeMeasurement,
    warm: CompactBlockRangeMeasurement,
}

#[derive(Serialize)]
struct CompactBlockRangeMeasurement {
    seconds: f64,
    blocks_per_second: f64,
    #[serde(flatten)]
    counters: ReadCounters,
    output: CompactBlockRangeOutputEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct CompactBlockRangeOutputEvidence {
    block_count: u32,
    first_height: u32,
    last_height: u32,
    stored_bytes: u64,
    structured_artifact_bytes: u64,
    digest_sha256: String,
}

#[derive(Clone, Copy)]
struct RangeRead {
    block_count: u32,
    order: HeightOrder,
    max_bytes: Option<usize>,
}

pub(crate) async fn run_rocksdb_compact_block_range(
    args: &ReadStoreArgs,
) -> Result<RocksDbCompactBlockRangeReport, BenchError> {
    let (store, environment) = args.prepare()?;
    let mut ready = None;
    let mut cases = Vec::new();
    for block_count in RANGE_BLOCK_COUNTS {
        let request = RangeRead {
            block_count,
            order: HeightOrder::Ascending,
            max_bytes: None,
        };
        cases.push(measure_case(&store, &mut ready, request).await?);
    }
    let mut bounded_cases = Vec::new();
    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        for max_bytes in [
            BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES,
            BLOCK_RANGE_CHUNK_MAX_BYTES,
        ] {
            let request = RangeRead {
                block_count: DEFAULT_MAX_COMPACT_BLOCK_RANGE.get(),
                order,
                max_bytes: Some(max_bytes),
            };
            bounded_cases.push(measure_case(&store, &mut ready, request).await?);
        }
    }
    let ready = ready.ok_or_else(|| BenchError::report_format("no compact-block cases"))?;
    Ok(RocksDbCompactBlockRangeReport {
        contract_identity: "rocksdb-compact-block-range",
        report_format_version: 4,
        environment,
        ready_fence: ready.into(),
        cases,
        bounded_cases,
    })
}

async fn measure_case(
    store: &ReadStore,
    fence: &mut Option<CanonicalStoreReadyEvidence>,
    request: RangeRead,
) -> Result<CompactBlockRangeCase, BenchError> {
    let order = match request.order {
        HeightOrder::Ascending => "ascending",
        HeightOrder::Descending => "descending",
    };
    let case = format!(
        "range-{}-{order}-{}",
        request.block_count,
        request.max_bytes.unwrap_or_default()
    );
    let (secondary, open_seconds) = store.open(&case, fence.as_ref())?;
    let io_mode = secondary.io_mode().as_str();
    let ready = secondary.ready_evidence();
    *fence = Some(ready);
    let range = range_ending_at_ready_tip(&ready, request.block_count)?;
    let direct = || {
        let sample = measure(&secondary, store.budget.statistics_level, || {
            Ok(match request.max_bytes {
                Some(bytes) => {
                    secondary.compact_blocks_in_range_up_to(range, request.order, bytes)?
                }
                None => secondary.compact_blocks_in_range(range)?,
            })
        })?;
        request.measurement(sample, range)
    };
    let cold = direct()?;
    let warm = direct()?;
    drop(secondary);
    let (secondary, reader_open_seconds) = store.open(&format!("{case}-reader"), Some(&ready))?;
    let reader = Arc::new(
        RocksDbCanonicalReader::new(secondary)
            .map_err(|error| BenchError::canonical_read(error.to_string()))?,
    );
    let reader_cold = request
        .reader_measurement(&reader, range, store.budget.statistics_level)
        .await?;
    let reader_warm = request
        .reader_measurement(&reader, range, store.budget.statistics_level)
        .await?;
    if [&warm.output, &reader_cold.output, &reader_warm.output]
        .into_iter()
        .any(|output| *output != cold.output)
    {
        return Err(BenchError::report_format(format!(
            "compact-block outputs differ for {case}"
        )));
    }
    Ok(CompactBlockRangeCase {
        io_mode,
        order,
        max_bytes: request.max_bytes,
        requested_block_count: request.block_count,
        range_start_height: range.start.value(),
        range_end_height: range.end.value(),
        fresh_secondary_open_seconds: open_seconds,
        cold,
        warm,
        canonical_reader: CanonicalReaderRangeCase {
            io_mode: reader.secondary().io_mode().as_str(),
            fresh_secondary_open_seconds: reader_open_seconds,
            cold: reader_cold,
            warm: reader_warm,
        },
    })
}

impl RangeRead {
    async fn reader_measurement(
        self,
        reader: &Arc<RocksDbCanonicalReader>,
        range: BlockHeightRange,
        level: RocksDbStatisticsLevel,
    ) -> Result<CompactBlockRangeMeasurement, BenchError> {
        let sample = measure_reader(Arc::clone(reader), level, move |reader| {
            self.max_bytes.map_or_else(
                || read_on_blocking_thread(reader.compact_blocks_in_range(range)),
                |bytes| {
                    read_on_blocking_thread(
                        reader.compact_blocks_in_range_up_to(range, self.order, bytes),
                    )
                },
            )
        })
        .await?;
        self.measurement(sample, range)
    }

    fn measurement(
        self,
        sample: TimedRead<Vec<CompactBlockArtifact>>,
        range: BlockHeightRange,
    ) -> Result<CompactBlockRangeMeasurement, BenchError> {
        let TimedRead {
            rows,
            seconds,
            counters,
        } = sample;
        let (returned, count) = if self.max_bytes.is_some() {
            let count = u32::try_from(rows.len())
                .map_err(|_| BenchError::report_format("oversized compact-block chunk"))?;
            if count == 0 || count > self.block_count {
                return Err(BenchError::report_format(
                    "bounded read returned an invalid row count",
                ));
            }
            let returned = match self.order {
                HeightOrder::Ascending => BlockHeightRange::inclusive(
                    range.start,
                    BlockHeight::new(range.start.value() + (count - 1)),
                ),
                HeightOrder::Descending => BlockHeightRange::inclusive(
                    BlockHeight::new(range.end.value() - (count - 1)),
                    range.end,
                ),
            };
            (returned, count)
        } else {
            (range, self.block_count)
        };
        let output = compact_block_range_output(&rows, returned, count)?;
        if count > 1
            && self
                .max_bytes
                .is_some_and(|max| u128::from(output.stored_bytes) > max as u128)
        {
            return Err(BenchError::report_format(
                "multi-block chunk exceeds its stored byte budget",
            ));
        }
        Ok(CompactBlockRangeMeasurement {
            seconds,
            blocks_per_second: f64::from(count) / seconds.max(f64::EPSILON),
            counters,
            output,
        })
    }
}

fn range_ending_at_ready_tip(
    ready: &CanonicalStoreReadyEvidence,
    block_count: u32,
) -> Result<BlockHeightRange, BenchError> {
    let start = block_count
        .checked_sub(1)
        .and_then(|offset| ready.visible_tip.height.value().checked_sub(offset))
        .ok_or_else(|| BenchError::invalid_argument("requested range underflows block height"))?;
    if start < ready.first_retained_block.height.value() {
        return Err(BenchError::invalid_argument(format!(
            "requested {block_count}-block range starts before retained canonical history"
        )));
    }
    Ok(BlockHeightRange::inclusive(
        BlockHeight::new(start),
        ready.visible_tip.height,
    ))
}

fn compact_block_range_output(
    blocks: &[CompactBlockArtifact],
    range: BlockHeightRange,
    expected_block_count: u32,
) -> Result<CompactBlockRangeOutputEvidence, BenchError> {
    let observed_count = u32::try_from(blocks.len())
        .map_err(|_| BenchError::report_format("compact-block range exceeds u32::MAX rows"))?;
    if observed_count != expected_block_count {
        return Err(BenchError::report_format(format!(
            "compact-block range returned {observed_count} rows; expected {expected_block_count}"
        )));
    }

    let mut digest = StructuredArtifactDigest::new();
    let mut stored_bytes = 0_u64;
    for (height, block) in range.into_iter().zip(blocks) {
        if block.height() != height {
            return Err(BenchError::report_format(format!(
                "compact-block range returned height {} where {} was expected",
                block.height().value(),
                height.value()
            )));
        }
        digest.update_block(block)?;
        stored_bytes += u64::try_from(zinder_store::encode_compact_block_artifact(block).len())
            .map_err(|_| {
                BenchError::report_format("stored compact block exceeds u64::MAX bytes")
            })?;
    }

    Ok(CompactBlockRangeOutputEvidence {
        block_count: observed_count,
        first_height: range.start.value(),
        last_height: range.end.value(),
        stored_bytes,
        structured_artifact_bytes: digest.structured_artifact_bytes,
        digest_sha256: hex::encode(digest.hasher.finalize()),
    })
}

struct StructuredArtifactDigest {
    hasher: Sha256,
    structured_artifact_bytes: u64,
}

impl StructuredArtifactDigest {
    fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(OUTPUT_DIGEST_DOMAIN);
        Self {
            hasher,
            structured_artifact_bytes: 0,
        }
    }

    fn update(&mut self, bytes: &[u8]) -> Result<(), BenchError> {
        let byte_count = u64::try_from(bytes.len()).map_err(|_| {
            BenchError::report_format("structured compact-block field exceeds u64::MAX bytes")
        })?;
        self.structured_artifact_bytes = self
            .structured_artifact_bytes
            .checked_add(byte_count)
            .ok_or_else(|| {
                BenchError::report_format("structured compact-block byte total exceeds u64::MAX")
            })?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn update_count(&mut self, count: usize) -> Result<(), BenchError> {
        let count = u64::try_from(count).map_err(|_| {
            BenchError::report_format("structured compact-block count exceeds u64::MAX")
        })?;
        self.update(&count.to_be_bytes())
    }

    fn update_block(&mut self, block: &CompactBlockArtifact) -> Result<(), BenchError> {
        self.update(&block.height().value().to_be_bytes())?;
        self.update(&block.block_hash().as_bytes())?;
        self.update(&block.previous_block_hash().as_bytes())?;
        self.update(&block.time().to_be_bytes())?;
        let metadata = block.chain_metadata();
        self.update(&metadata.sapling_commitment_tree_size.to_be_bytes())?;
        self.update(&metadata.orchard_commitment_tree_size.to_be_bytes())?;
        self.update(&metadata.ironwood_commitment_tree_size.to_be_bytes())?;
        self.update_count(block.transactions().len())?;
        for transaction in block.transactions() {
            self.update(&transaction.index.to_be_bytes())?;
            self.update(&transaction.transaction_id.as_bytes())?;
            match transaction.data.fee_zat {
                Some(fee_zat) => {
                    self.update(&[1])?;
                    self.update(&fee_zat.to_be_bytes())?;
                }
                None => self.update(&[0])?,
            }
            self.update_count(transaction.data.sapling_spends.len())?;
            for spend in &transaction.data.sapling_spends {
                self.update(&spend.nullifier)?;
            }
            self.update_count(transaction.data.sapling_outputs.len())?;
            for output in &transaction.data.sapling_outputs {
                self.update(&output.commitment)?;
                self.update(&output.ephemeral_key)?;
                self.update(&output.ciphertext)?;
            }
            self.update_count(transaction.data.orchard_actions.len())?;
            for action in &transaction.data.orchard_actions {
                self.update(&action.nullifier)?;
                self.update(&action.commitment)?;
                self.update(&action.ephemeral_key)?;
                self.update(&action.ciphertext)?;
            }
            self.update_count(transaction.data.ironwood_actions.len())?;
            for action in &transaction.data.ironwood_actions {
                self.update(&action.nullifier)?;
                self.update(&action.commitment)?;
                self.update(&action.ephemeral_key)?;
                self.update(&action.ciphertext)?;
            }
            self.update_count(transaction.data.transparent_inputs.len())?;
            for input in &transaction.data.transparent_inputs {
                self.update(&input.previous_transaction_id.as_bytes())?;
                self.update(&input.previous_output_index.to_be_bytes())?;
            }
            self.update_count(transaction.data.transparent_outputs.len())?;
            for output in &transaction.data.transparent_outputs {
                self.update(&output.value_zat.to_be_bytes())?;
                self.update_count(output.script_pub_key.len())?;
                self.update(&output.script_pub_key)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHash, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
        CompactSaplingSpend, CompactShieldedAction, CompactTransaction, CompactTransactionData,
        CompactTransparentInput, CompactTransparentOutput, TransactionId,
    };

    use super::{BenchError, BlockHeight, BlockHeightRange, ReadStore, compact_block_range_output};

    fn empty_block(height: u32, hash_byte: u8, parent_byte: u8, time: u32) -> CompactBlockArtifact {
        CompactBlockArtifact::empty(
            BlockId::new(
                BlockHeight::new(height),
                BlockHash::from_bytes([hash_byte; 32]),
            ),
            BlockHash::from_bytes([parent_byte; 32]),
            time,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    /// The canonical-reader measurement reads only a secondary at the READY
    /// fence the direct measurement read, so a writer that moved between the
    /// two opens fails the case instead of timing another chain.
    #[test]
    fn the_reader_measurement_refuses_a_secondary_at_another_ready_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain = zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest)
            .with_raw_blob_retention(zinder_store::RawBlobRetention::None)
            .extend_blocks(4);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut writer = zinder_testkit::CanonicalWriterFixture::publish(
            &zinder_testkit::CanonicalBaseline::new(&chain).through(BlockHeight::new(3)),
            &activations,
        )?;
        let canonical_store = writer.canonical_primary_path().to_path_buf();
        let reorg_policy = writer.canonical_primary()?.reorg_policy();
        let secondaries = tempfile::TempDir::new()?;
        let store = ReadStore {
            primary: canonical_store,
            secondary_root: secondaries.path().to_owned(),
            activations,
            reorg_policy,
            raw_blob_retention: zinder_store::RawBlobRetention::None,
            budget: zinder_store::RocksDbResourceBudget::canonical_reader_defaults(),
            evict_page_cache: false,
        };
        let (direct, _) = store.open("direct", None)?;
        let ready = direct.ready_evidence();
        store.open("same-fence", Some(&ready))?;
        writer.append_live_block(&chain)?;
        let moved = store.open("moved-fence", Some(&ready));
        assert!(
            matches!(moved, Err(BenchError::ReportFormat { .. })),
            "a secondary at a moved fence must fail the case"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bounded_chunks_attribute_one_oversized_block_on_both_read_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        use zinder_store::{HeightOrder, RocksDbStatisticsLevel};
        let chain = zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest)
            .with_raw_blob_retention(zinder_store::RawBlobRetention::None)
            .extend_blocks(4);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let writer = zinder_testkit::CanonicalWriterFixture::publish(
            &zinder_testkit::CanonicalBaseline::new(&chain),
            &activations,
        )?;
        let scratch = tempfile::TempDir::new()?;
        let mut store = ReadStore {
            primary: writer.canonical_primary_path().to_path_buf(),
            secondary_root: scratch.path().into(),
            activations,
            reorg_policy: writer.canonical_primary()?.reorg_policy(),
            raw_blob_retention: zinder_store::RawBlobRetention::None,
            budget: zinder_store::RocksDbResourceBudget::canonical_reader_defaults(),
            evict_page_cache: false,
        };
        for (level, order, block_count) in [
            (RocksDbStatisticsLevel::Full, HeightOrder::Descending, 3),
            (RocksDbStatisticsLevel::Tickers, HeightOrder::Ascending, 3),
            (RocksDbStatisticsLevel::Off, HeightOrder::Ascending, 2),
        ] {
            store.budget.statistics_level = level;
            let request = super::RangeRead {
                block_count,
                order,
                max_bytes: Some(1),
            };
            let case = super::measure_case(&store, &mut None, request).await?;
            let expected_height = match order {
                HeightOrder::Ascending => case.range_start_height,
                HeightOrder::Descending => case.range_end_height,
            };
            for measurement in [
                case.cold,
                case.warm,
                case.canonical_reader.cold,
                case.canonical_reader.warm,
            ] {
                assert_eq!(measurement.output.block_count, 1);
                assert_eq!(measurement.output.first_height, expected_height);
                assert!(measurement.output.stored_bytes > 1);
                let counters = serde_json::to_value(measurement.counters)?;
                assert_eq!(
                    counters["ticker_deltas"].is_object(),
                    level != RocksDbStatisticsLevel::Off
                );
                assert_eq!(
                    counters["histogram_deltas"].is_object(),
                    level == RocksDbStatisticsLevel::Full
                );
                let io = &counters["perf_context"];
                assert!(
                    io["block_cache_hit_count"].as_u64().unwrap_or_default()
                        + io["block_read_count"].as_u64().unwrap_or_default()
                        > 0
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn bounded_chunks_keep_two_blocks_with_an_exact_budget_or_room_left()
    -> Result<(), Box<dyn std::error::Error>> {
        use zinder_store::{HeightOrder, encode_compact_block_artifact};

        let chain = zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest)
            .with_raw_blob_retention(zinder_store::RawBlobRetention::None)
            .extend_blocks(3);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let writer = zinder_testkit::CanonicalWriterFixture::publish(
            &zinder_testkit::CanonicalBaseline::new(&chain),
            &activations,
        )?;
        let scratch = tempfile::TempDir::new()?;
        let store = ReadStore {
            primary: writer.canonical_primary_path().to_path_buf(),
            secondary_root: scratch.path().into(),
            activations,
            reorg_policy: writer.canonical_primary()?.reorg_policy(),
            raw_blob_retention: zinder_store::RawBlobRetention::None,
            budget: zinder_store::RocksDbResourceBudget::canonical_reader_defaults(),
            evict_page_cache: false,
        };

        let blocks = chain.compact_block_artifacts();
        let [first, middle, last] = blocks.as_slice() else {
            return Err("the fixture must contain three compact blocks".into());
        };
        for (order, pair, excluded) in [
            (HeightOrder::Ascending, [first, middle], last),
            (HeightOrder::Descending, [middle, last], first),
        ] {
            let stored_bytes = pair
                .iter()
                .map(|block| encode_compact_block_artifact(block).len())
                .sum::<usize>();
            let one_byte_short_of_three =
                stored_bytes + encode_compact_block_artifact(excluded).len() - 1;
            for budget in [stored_bytes, one_byte_short_of_three] {
                let request = super::RangeRead {
                    block_count: 3,
                    order,
                    max_bytes: Some(budget),
                };
                let case = super::measure_case(&store, &mut None, request).await?;
                assert_eq!(
                    (
                        case.cold.output.block_count,
                        case.cold.output.first_height,
                        case.cold.output.last_height,
                        case.cold.output.stored_bytes,
                    ),
                    (
                        2,
                        pair[0].height().value(),
                        pair[1].height().value(),
                        u64::try_from(stored_bytes)?
                    ),
                    "{order:?} with a {budget}-byte budget"
                );
                assert_eq!(case.cold.output, case.warm.output);
                assert_eq!(case.cold.output, case.canonical_reader.cold.output);
                assert_eq!(case.cold.output, case.canonical_reader.warm.output);
            }
        }
        Ok(())
    }

    #[test]
    fn output_digest_binds_order_identity_time_and_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = empty_block(10, 1, 0, 100);
        let second = empty_block(11, 4, 1, 101);
        let range = BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(11));

        let evidence = compact_block_range_output(&[first.clone(), second.clone()], range, 2)?;
        let repeated = compact_block_range_output(&[first, second], range, 2)?;
        let changed = compact_block_range_output(
            &[empty_block(10, 1, 0, 100), empty_block(11, 4, 1, 102)],
            range,
            2,
        )?;

        assert_eq!(evidence, repeated);
        assert_ne!(evidence, changed);
        assert_eq!(evidence.structured_artifact_bytes, 184);
        Ok(())
    }

    #[test]
    fn output_digest_binds_every_transaction_field() -> Result<(), Box<dyn std::error::Error>> {
        let block = structured_block(0x51)?;
        let changed = structured_block(0x52)?;
        let range = BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(10));

        let evidence = compact_block_range_output(&[block], range, 1)?;
        let changed = compact_block_range_output(&[changed], range, 1)?;

        assert_ne!(evidence.digest_sha256, changed.digest_sha256);
        assert_eq!(
            evidence.structured_artifact_bytes,
            changed.structured_artifact_bytes
        );
        Ok(())
    }

    fn structured_block(
        script_byte: u8,
    ) -> Result<CompactBlockArtifact, Box<dyn std::error::Error>> {
        Ok(CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(10), BlockHash::from_bytes([1; 32])),
            BlockHash::from_bytes([0; 32]),
            100,
            vec![CompactTransaction {
                index: 0,
                transaction_id: TransactionId::from_bytes([2; 32]),
                data: CompactTransactionData {
                    fee_zat: Some(1_000),
                    sapling_spends: vec![CompactSaplingSpend { nullifier: [3; 32] }],
                    sapling_outputs: vec![CompactSaplingOutput {
                        commitment: [4; 32],
                        ephemeral_key: [5; 32],
                        ciphertext: [6; 52],
                    }],
                    orchard_actions: vec![CompactShieldedAction {
                        nullifier: [7; 32],
                        commitment: [8; 32],
                        ephemeral_key: [9; 32],
                        ciphertext: [10; 52],
                    }],
                    ironwood_actions: vec![CompactShieldedAction {
                        nullifier: [11; 32],
                        commitment: [12; 32],
                        ephemeral_key: [13; 32],
                        ciphertext: [14; 52],
                    }],
                    transparent_inputs: vec![CompactTransparentInput {
                        previous_transaction_id: TransactionId::from_bytes([15; 32]),
                        previous_output_index: 1,
                    }],
                    transparent_outputs: vec![CompactTransparentOutput {
                        value_zat: 500,
                        script_pub_key: vec![script_byte],
                    }],
                },
            }],
            CompactChainMetadata {
                sapling_commitment_tree_size: 1,
                orchard_commitment_tree_size: 2,
                ironwood_commitment_tree_size: 3,
            },
        )?)
    }
}
