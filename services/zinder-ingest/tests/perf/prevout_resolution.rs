#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Producing-block reads that Explorer view replay spends on transparent
//! prevouts.
//!
//! The fixture is a transparent-heavy chain in which every block spends
//! outputs produced by earlier blocks, most of them recent and the rest
//! anywhere in history. Hydrating it page by page, as replay does, must
//! resolve every spend from the prevout cache instead of reading producing
//! blocks, and must derive exactly the spend facts the uncached path derives.
//! The assertions count reads rather than time them, so they hold on any CI
//! worker.

use std::collections::BTreeMap;

use metrics_exporter_prometheus::PrometheusBuilder;
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, Network, TransactionId, TransactionLocation,
    TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
    TransparentSpendFact,
};
use zinder_ingest::{CanonicalBlockContextReader, IngestError, PrevoutCache};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    sample_regtest_upgrade_activations, synthetic_transaction_public_facts,
};

const BLOCK_COUNT: u32 = 1_200;
const SPENDS_PER_BLOCK: u32 = 30;
const OUTPUTS_PER_TRANSACTION: u32 = 2;
/// Replay's default page size.
const PAGE_BLOCKS: u32 = 100;
/// Share of spends, in percent, that pick a coin from the last
/// `RECENT_WINDOW_BLOCKS` heights; the rest pick any earlier height.
const RECENT_SPEND_PERCENT: u64 = 60;
const RECENT_WINDOW_BLOCKS: u64 = 50;
const FIXTURE_REORG_WINDOW_BLOCKS: u32 = 100;

#[test]
fn replay_resolves_transparent_dense_history_without_producing_block_reads() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = transparent_dense_chain();
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let secondary_directory = TempDir::new()?;
    let secondary = RocksDbCanonicalSecondary::open_ready(
        writer.canonical_primary_path(),
        secondary_directory.path().join("canonical-secondary"),
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(FIXTURE_REORG_WINDOW_BLOCKS)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let (uncached_spends, uncached_reads) = with_prevout_metrics(|| {
        hydrate_in_pages(&mut CanonicalBlockContextReader::new(
            &secondary,
            &activations,
        ))
    })?;
    let mut prevouts = PrevoutCache::new(1 << 20);
    let (cached_spends, cached_reads) = with_prevout_metrics(|| {
        hydrate_in_pages(&mut CanonicalBlockContextReader::with_prevout_cache(
            &secondary,
            &activations,
            &mut prevouts,
        ))
    })?;

    let spend_count = cached_spends.values().map(Vec::len).sum::<usize>();
    assert!(spend_count > usize::try_from(BLOCK_COUNT * SPENDS_PER_BLOCK / 2)?);
    assert_eq!(cached_spends, uncached_spends);
    assert!(
        uncached_reads.producing_block_reads > u64::from(BLOCK_COUNT),
        "the uncached path reads producing blocks: {uncached_reads:?}"
    );
    assert_eq!(cached_reads.producing_block_reads, 0, "{cached_reads:?}");
    assert_eq!(cached_reads.from_canonical, 0, "{cached_reads:?}");
    assert_eq!(
        cached_reads.from_cache, uncached_reads.from_canonical,
        "every spend the uncached path read from canonical storage comes from the cache"
    );
    Ok(())
}

/// Spend facts per block, in input order.
type BlockSpends = BTreeMap<BlockHeight, Vec<TransparentSpendFact>>;

fn hydrate_in_pages(
    reader: &mut CanonicalBlockContextReader<'_>,
) -> Result<BlockSpends, IngestError> {
    let mut spends = BlockSpends::new();
    let mut start = 1_u32;
    while start <= BLOCK_COUNT {
        let end = start.saturating_add(PAGE_BLOCKS - 1).min(BLOCK_COUNT);
        let page = BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end));
        for context in reader.read_ordered_block_commit_contexts(page)? {
            let Some(resolved) = context.transparent_spends() else {
                continue;
            };
            let block_spends = context
                .transactions
                .iter()
                .flat_map(|transaction| &transaction.transparent_inputs)
                .filter_map(|input| resolved.get(&input.spent_outpoint).cloned())
                .collect();
            spends.insert(context.height, block_spends);
        }
        start = end.saturating_add(1);
    }
    Ok(spends)
}

#[derive(Debug)]
struct PrevoutReads {
    from_cache: u64,
    from_canonical: u64,
    producing_block_reads: u64,
}

fn with_prevout_metrics<T>(
    hydrate: impl FnOnce() -> Result<T, IngestError>,
) -> eyre::Result<(T, PrevoutReads)> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let hydrated = metrics::with_local_recorder(&recorder, hydrate)?;
    let rendered = handle.render();
    Ok((
        hydrated,
        PrevoutReads {
            from_cache: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_resolutions_total{source=\"cache\"}",
            ),
            from_canonical: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_resolutions_total{source=\"canonical\"}",
            ),
            producing_block_reads: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_block_reads_total",
            ),
        },
    ))
}

fn counter_value(rendered: &str, series: &str) -> u64 {
    rendered
        .lines()
        .filter_map(|line| line.strip_prefix(series)?.trim().parse::<u64>().ok())
        .sum()
}

/// Builds the fixture chain: each block's coinbase and spends create
/// [`OUTPUTS_PER_TRANSACTION`] outputs, and each spend consumes one coin an
/// earlier block produced.
fn transparent_dense_chain() -> ChainFixture {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(BLOCK_COUNT);
    let block_hashes = chain
        .blocks()
        .iter()
        .map(|block| block.hash)
        .collect::<Vec<_>>();
    let mut unspent_by_height: Vec<Vec<TransparentOutPoint>> = Vec::new();
    let mut picker = SpendPicker::new();
    for (height, block_hash) in (1..=BLOCK_COUNT).zip(block_hashes) {
        let mut produced = Vec::new();
        chain = chain.with_transaction_rows(transaction_rows(
            BlockPosition::new(height, block_hash, 0),
            Vec::new(),
            &mut produced,
        ));
        for spend_index in 1..=SPENDS_PER_BLOCK {
            let Some(spent) = picker.take(&mut unspent_by_height) else {
                break;
            };
            chain = chain.with_transaction_rows(transaction_rows(
                BlockPosition::new(height, block_hash, spend_index),
                vec![TransparentInputFact::new(0, spent)],
                &mut produced,
            ));
        }
        unspent_by_height.push(produced);
    }
    chain
}

#[derive(Clone, Copy)]
struct BlockPosition {
    height: u32,
    block_hash: BlockHash,
    tx_index_in_block: u32,
}

impl BlockPosition {
    const fn new(height: u32, block_hash: BlockHash, tx_index_in_block: u32) -> Self {
        Self {
            height,
            block_hash,
            tx_index_in_block,
        }
    }
}

fn transaction_rows(
    position: BlockPosition,
    transparent_inputs: Vec<TransparentInputFact>,
    produced: &mut Vec<TransparentOutPoint>,
) -> FixtureTransactionRows {
    let transaction_id = fixture_transaction_id(position.height, position.tx_index_in_block);
    let mut public_facts = synthetic_transaction_public_facts(transaction_id, 0);
    public_facts.is_coinbase = position.tx_index_in_block == 0;
    public_facts.counts.transparent_input_count =
        u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX);
    public_facts.counts.transparent_output_count = OUTPUTS_PER_TRANSACTION;
    let transparent_outputs = (0..OUTPUTS_PER_TRANSACTION)
        .map(|output_index| {
            produced.push(TransparentOutPoint::new(transaction_id, output_index));
            let script_pub_key = fixture_script_pub_key(position.height, output_index);
            let address_script_hash =
                TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
            TransparentOutputFact::new(
                output_index,
                u64::from(position.height) * 10_000 + u64::from(output_index),
                script_pub_key,
                address_script_hash,
            )
        })
        .collect();
    let mut rows = FixtureTransactionRows::from_public_facts(
        TransactionLocation::new(
            transaction_id,
            BlockHeight::new(position.height),
            position.block_hash,
            position.tx_index_in_block,
        ),
        public_facts,
    );
    rows.facts = rows
        .facts
        .with_transparent_facts(transparent_inputs, transparent_outputs);
    rows
}

fn fixture_transaction_id(height: u32, tx_index_in_block: u32) -> TransactionId {
    let mut bytes = [0x7c_u8; 32];
    bytes[..4].copy_from_slice(&height.to_be_bytes());
    bytes[4..8].copy_from_slice(&tx_index_in_block.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

/// A P2PKH script whose key hash names the producing height and output.
fn fixture_script_pub_key(height: u32, output_index: u32) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&height.to_be_bytes());
    script.extend_from_slice(&output_index.to_be_bytes());
    script.extend_from_slice(&[0x5a; 12]);
    script.extend_from_slice(&[0x88, 0xac]);
    script
}

/// Deterministic choice of which earlier coin each spend consumes.
struct SpendPicker {
    state: u64,
}

impl SpendPicker {
    const fn new() -> Self {
        Self {
            state: 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state >> 33
    }

    /// Takes one unspent coin from the chosen height, or else from the
    /// nearest earlier height, or else from the nearest later one.
    fn take(
        &mut self,
        unspent_by_height: &mut [Vec<TransparentOutPoint>],
    ) -> Option<TransparentOutPoint> {
        let produced_heights = u64::try_from(unspent_by_height.len()).ok()?;
        if produced_heights == 0 {
            return None;
        }
        let back = if self.next() % 100 < RECENT_SPEND_PERCENT {
            self.next() % RECENT_WINDOW_BLOCKS.min(produced_heights)
        } else {
            self.next() % produced_heights
        };
        let chosen = usize::try_from(produced_heights - 1 - back).ok()?;
        let (earlier, later) = unspent_by_height.split_at_mut(chosen.saturating_add(1));
        earlier
            .iter_mut()
            .rev()
            .find_map(Vec::pop)
            .or_else(|| later.iter_mut().find_map(Vec::pop))
    }
}
