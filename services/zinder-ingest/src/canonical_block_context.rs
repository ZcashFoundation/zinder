//! Materialized-view block contexts hydrated from admitted canonical facts.
//!
//! Canonical storage retains block-local facts only. Two inputs the
//! materialized-view consumers require are therefore derived here rather than
//! read from a dedicated family: final note-commitment roots come from the
//! persisted tree-state checkpoints plus compact-block commitments, and
//! transparent spend facts come from the output the spend names. That output
//! is found in the hydrated range, then in an optional [`PrevoutCache`] of
//! outputs earlier ranges produced, and last in the producing block's replay
//! row. Canonical reads for the last step, and for warming an empty cache,
//! run on several threads so their storage waits overlap.

mod parallel_reads;
mod prevout_cache;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    CanonicalBlockFacts, CanonicalTransactionFacts, CommitmentTreeAccumulator,
    CompactBlockArtifact, NetworkUpgradeActivations, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransactionLocation, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentOutputFact, TransparentSpendFact,
    ValidatedCanonicalBlockReplay,
};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, TransactionIntrinsicValueBalanceFacts,
    TransparentSpendFacts,
};
use zinder_store::{MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS, RocksDbCanonicalSecondary};

use crate::{CanonicalBlockConstructionError, IngestError};

use parallel_reads::{canonical_read_workers, read_in_parallel, read_slices_in_parallel};
use prevout_cache::BlockPrevoutDelta;
pub(crate) use prevout_cache::DEFAULT_PREVOUT_CACHE_MAX_OUTPUTS;
pub use prevout_cache::PrevoutCache;

/// Fewest heights a walk must have left before an empty prevout cache is
/// warmed from canonical history first.
///
/// Warming costs one sequential pass over the history below the walk, so it
/// pays off only when the walk would otherwise resolve many spends of older
/// outputs through random canonical reads.
pub(crate) const PREVOUT_CACHE_WARMUP_MIN_REMAINING_BLOCKS: u32 = 10_000;

/// Most heights one warm-up step decodes before it applies them to the cache.
///
/// The step's decoded deltas are held in memory until they are applied, so
/// the window bounds that transient memory in transparent-dense history. The
/// caller's window, normally its current replay page size, is capped here.
const PREVOUT_CACHE_WARMUP_WINDOW_BLOCKS: u32 = 512;

/// First height a canonical store retains when its history is complete.
const GENESIS_COMPLETE_FIRST_HEIGHT: BlockHeight = BlockHeight::new(1);

/// Refuses materialized-view work on a store without complete history.
///
/// Cumulative address views derive spent value from resolved prevouts. On a
/// store bootstrapped from a checkpoint the pre-floor outputs never exist, so
/// those views would silently under-report every spend of older coins.
pub fn require_genesis_complete_history(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), IngestError> {
    let first_available_height = secondary.history_bounds().first_available_height();
    if first_available_height != GENESIS_COMPLETE_FIRST_HEIGHT {
        return Err(IngestError::MaterializedViewHistoryIncomplete {
            first_available_height,
        });
    }
    Ok(())
}

/// Builds materialized-view block contexts from admitted canonical facts.
///
/// One reader serves a sequence of ranges. Sequential ranges reuse the
/// commitment-tree state left by the previous call; any other range reseeds
/// from the newest persisted tree-state checkpoint at or before its start.
pub struct CanonicalBlockContextReader<'canonical> {
    secondary: &'canonical RocksDbCanonicalSecondary,
    activations: &'canonical NetworkUpgradeActivations,
    roots: FinalRootsCursor,
    prevouts: Option<&'canonical mut PrevoutCache>,
}

impl<'canonical> CanonicalBlockContextReader<'canonical> {
    /// Binds one hydrator to an admitted canonical secondary.
    ///
    /// Every spent output produced before a hydrated range is read from its
    /// producing block.
    #[must_use]
    pub const fn new(
        secondary: &'canonical RocksDbCanonicalSecondary,
        activations: &'canonical NetworkUpgradeActivations,
    ) -> Self {
        Self {
            secondary,
            activations,
            roots: FinalRootsCursor { accumulator: None },
            prevouts: None,
        }
    }

    /// Binds one hydrator that resolves spent outputs through `prevouts`.
    ///
    /// The cache answers for outputs that earlier connected ranges produced,
    /// including ranges an earlier reader hydrated through the same cache, and
    /// learns the outputs each hydrated range produces and spends.
    #[must_use]
    pub const fn with_prevout_cache(
        secondary: &'canonical RocksDbCanonicalSecondary,
        activations: &'canonical NetworkUpgradeActivations,
        prevouts: &'canonical mut PrevoutCache,
    ) -> Self {
        Self {
            secondary,
            activations,
            roots: FinalRootsCursor { accumulator: None },
            prevouts: Some(prevouts),
        }
    }

    /// Returns the prevout cache this reader resolves through, if any.
    pub(crate) fn prevout_cache_mut(&mut self) -> Option<&mut PrevoutCache> {
        self.prevouts.as_deref_mut()
    }

    /// Warms a prevout cache with the unspent outputs canonical history holds
    /// at `through`.
    ///
    /// The cache is rebuilt by applying every retained block from the first
    /// retained height through `through` in order, as an uninterrupted walk
    /// from genesis would have left it, up to its bound. Rows are read and
    /// decoded on several threads one window at a time. `next_window` is
    /// sampled before each window and returns how many heights the window may
    /// span, capped at 512. When it returns `None` the warm-up stops and
    /// reports `false`, and the cache keeps the windows it applied, so a later
    /// call with the same `through` continues from there. A cache that
    /// ends above `through` or off its chain is emptied and warmed from the
    /// first retained height.
    ///
    /// A reader without a cache, a cache that already ends at `through`, a
    /// cache a pause released (see [`PrevoutCache::release_for_pause`]), or a
    /// height below retained history needs no walk.
    pub fn warm_prevout_cache(
        &mut self,
        through: BlockHeight,
        next_window: &mut dyn FnMut() -> Option<u32>,
    ) -> Result<bool, IngestError> {
        let first_height = self.secondary.history_bounds().first_available_height();
        let Some(prevouts) = self.prevouts.as_deref_mut() else {
            return Ok(true);
        };
        if !prevouts.retains_outputs()
            || !prevouts.warm_up_allowed()
            || prevouts.applied_height() == Some(through)
            || through < first_height
        {
            return Ok(true);
        }
        let mut next = match prevouts.applied_height().and_then(BlockHeight::next) {
            Some(resume) if resume > first_height && resume <= through => resume,
            _ => {
                prevouts.clear();
                first_height
            }
        };
        let started_at = Instant::now();
        while next <= through {
            let Some(window_blocks) = next_window() else {
                return Ok(false);
            };
            let window_blocks = window_blocks.clamp(1, PREVOUT_CACHE_WARMUP_WINDOW_BLOCKS);
            let end = BlockHeight::new(
                next.value()
                    .saturating_add(window_blocks - 1)
                    .min(through.value()),
            );
            let deltas =
                read_prevout_deltas(self.secondary, BlockHeightRange::inclusive(next, end))?;
            if deltas
                .first()
                .is_some_and(|first| !prevouts.extends_to(first))
            {
                prevouts.clear();
                next = first_height;
                continue;
            }
            prevouts.apply_deltas(&deltas);
            let Some(after) = end.next() else {
                break;
            };
            next = after;
        }
        tracing::info!(
            target: "zinder::ingest",
            event = "materialized_view_prevout_cache_warmed",
            cache = prevouts.metrics_label().unwrap_or("unlabeled"),
            from_height = first_height.value(),
            through_height = through.value(),
            cached_outputs = prevouts.len(),
            elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            "prevout cache warmed from canonical history before resuming a walk partway through it"
        );
        Ok(true)
    }

    /// Hydrates one connected height range into dispatch-ready contexts.
    ///
    /// The range must be connected and no wider than the canonical
    /// incremental replay bound; paging a broader event is the caller's
    /// responsibility.
    pub fn read_block_commit_contexts(
        &mut self,
        range: BlockHeightRange,
    ) -> Result<HashMap<BlockHeight, Arc<BlockCommitContext>>, IngestError> {
        Ok(self
            .read_ordered_block_commit_contexts(range)?
            .into_iter()
            .map(|context| (context.height, Arc::new(context)))
            .collect())
    }

    /// Hydrates one connected height range in ascending height order.
    pub fn read_ordered_block_commit_contexts(
        &mut self,
        range: BlockHeightRange,
    ) -> Result<Vec<BlockCommitContext>, IngestError> {
        let blocks = self.read_block_facts(range)?;
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let mut roots = self.read_final_note_commitment_roots(range)?;
        let transparent_spends = Arc::new(self.resolve_transparent_spends(&blocks)?);
        let intrinsic_value_balances = Arc::new(intrinsic_value_balances(&blocks));
        let mut contexts = Vec::with_capacity(blocks.len());
        for block in blocks {
            let height = block.block_header.height;
            contexts.push(block_commit_context(
                block,
                roots.remove(&height),
                &transparent_spends,
                &intrinsic_value_balances,
            )?);
        }
        Ok(contexts)
    }

    fn read_block_facts(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CanonicalBlockFacts>, IngestError> {
        self.secondary
            .scan_canonical_replay_range(range)?
            .map(|replay| {
                replay
                    .map(ValidatedCanonicalBlockReplay::into_facts)
                    .map_err(IngestError::from)
            })
            .collect()
    }

    fn read_final_note_commitment_roots(
        &mut self,
        range: BlockHeightRange,
    ) -> Result<HashMap<BlockHeight, BlockFinalNoteCommitmentRoots>, IngestError> {
        let compact_blocks = self.secondary.compact_blocks_in_range(range)?;
        let accumulator = self
            .roots
            .position_at(self.secondary, self.activations, range.start)?;
        let mut roots = HashMap::with_capacity(compact_blocks.len());
        for compact_block in &compact_blocks {
            append_compact_block(accumulator, compact_block)?;
            let block_roots = accumulator.final_note_commitment_roots(compact_block.block_hash());
            if has_any_pool_root(&block_roots) {
                roots.insert(compact_block.height(), block_roots);
            }
        }
        Ok(roots)
    }

    /// Resolves every non-coinbase input in `blocks`, then teaches the cache
    /// what the blocks produced and spent.
    ///
    /// The cache learns a range only after all of its inputs resolve, so a
    /// failed range leaves it at the last fully hydrated block.
    fn resolve_transparent_spends(
        &mut self,
        blocks: &[CanonicalBlockFacts],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendFact>, IngestError> {
        if let (Some(prevouts), Some(first_block)) = (self.prevouts.as_deref_mut(), blocks.first())
        {
            prevouts.connect(&first_block.block_header);
        }
        let spends = self.resolve_spend_requests(blocks)?;
        if let Some(prevouts) = self.prevouts.as_deref_mut() {
            prevouts.apply(blocks);
        }
        Ok(spends)
    }

    fn resolve_spend_requests(
        &self,
        blocks: &[CanonicalBlockFacts],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendFact>, IngestError> {
        let requests = transparent_spend_requests(blocks)?;
        if requests.is_empty() {
            return Ok(HashMap::new());
        }
        let requested = requests
            .iter()
            .map(|request| request.outpoint)
            .collect::<HashSet<_>>();
        let mut produced = produced_outputs_in_range(blocks, &requested);
        let resolved_in_range = produced.len();
        let resolved_from_cache = self.prevouts.as_deref().map_or(0, |prevouts| {
            prevouts.resolve_into(&requested, &mut produced)
        });
        let producing_block_reads =
            self.read_produced_outputs_below_range(&requested, &mut produced)?;
        record_prevout_resolutions(PrevoutResolutionCounts {
            walker: self
                .prevouts
                .as_deref()
                .and_then(PrevoutCache::metrics_label)
                .unwrap_or(UNCACHED_WALKER_LABEL),
            in_range: resolved_in_range,
            from_cache: resolved_from_cache,
            from_canonical: requested
                .len()
                .saturating_sub(resolved_in_range)
                .saturating_sub(resolved_from_cache),
            producing_block_reads,
        });
        requests
            .into_iter()
            .map(|request| {
                let output = produced.get(&request.outpoint).ok_or_else(|| {
                    prevout_unresolved(request.outpoint, "prevout resolution produced no output")
                })?;
                Ok((request.outpoint, request.into_spend_fact(output)))
            })
            .collect()
    }

    /// Reads the outputs `produced` still lacks from their producing blocks and
    /// returns how many producing blocks it read.
    ///
    /// Each distinct parent transaction costs one location read and each
    /// distinct producing height one replay-row read. Both sets of reads are
    /// independent, so they run on several threads. Each thread takes a
    /// contiguous run of sorted keys and reads it in batched lookups, so keys
    /// in the same or adjacent data blocks cost one device read.
    fn read_produced_outputs_below_range(
        &self,
        requested: &HashSet<TransparentOutPoint>,
        produced: &mut HashMap<TransparentOutPoint, ProducedOutput>,
    ) -> Result<usize, IngestError> {
        let mut missing_by_transaction = BTreeMap::<TransactionId, Vec<TransparentOutPoint>>::new();
        for outpoint in requested
            .iter()
            .filter(|outpoint| !produced.contains_key(*outpoint))
        {
            missing_by_transaction
                .entry(outpoint.transaction_id)
                .or_default()
                .push(*outpoint);
        }
        if missing_by_transaction.is_empty() {
            return Ok(0);
        }
        let missing_by_transaction = missing_by_transaction.into_iter().collect::<Vec<_>>();
        let locations = read_in_parallel(&missing_by_transaction, |chunk| {
            let transaction_ids = chunk
                .iter()
                .map(|(transaction_id, _)| *transaction_id)
                .collect::<Vec<_>>();
            self.secondary
                .transaction_locations(&transaction_ids)?
                .into_iter()
                .zip(chunk)
                .map(|(location, (_, outpoints))| {
                    location.ok_or_else(|| {
                        prevout_unresolved(
                            outpoints[0],
                            "producing transaction has no canonical location",
                        )
                    })
                })
                .collect()
        })?;
        let mut by_producing_height =
            BTreeMap::<BlockHeight, Vec<(TransparentOutPoint, u32)>>::new();
        for ((_, outpoints), location) in missing_by_transaction.iter().zip(&locations) {
            by_producing_height
                .entry(location.block_height)
                .or_default()
                .extend(
                    outpoints
                        .iter()
                        .map(|outpoint| (*outpoint, location.tx_index_in_block)),
                );
        }
        let producing_block_reads = by_producing_height.len();
        let by_producing_height = by_producing_height.into_iter().collect::<Vec<_>>();
        let outputs = read_in_parallel(&by_producing_height, |chunk| {
            let heights = chunk.iter().map(|(height, _)| *height).collect::<Vec<_>>();
            let mut outputs = Vec::new();
            self.secondary
                .visit_block_replay_facts_at_heights(&heights, |index, block| {
                    let Some((_, outpoints)) = chunk.get(index) else {
                        return Ok(());
                    };
                    for (outpoint, tx_index_in_block) in outpoints {
                        let block = block.as_ref().ok_or_else(|| {
                            prevout_unresolved(*outpoint, "producing block is not retained")
                        })?;
                        outputs.push((
                            *outpoint,
                            produced_output(block, *outpoint, *tx_index_in_block)?,
                        ));
                    }
                    Ok::<_, IngestError>(())
                })?;
            Ok(outputs)
        })?;
        produced.extend(outputs);
        Ok(producing_block_reads)
    }
}

/// Reads the prevout deltas of the connected range `range` in height order.
///
/// The range is split into one contiguous slice per read worker. Each slice is
/// read through the bounded replay-range scan, which proves that every block
/// extends the canonical row below it, so the concatenated deltas form one
/// connected run.
fn read_prevout_deltas(
    secondary: &RocksDbCanonicalSecondary,
    range: BlockHeightRange,
) -> Result<Vec<BlockPrevoutDelta>, IngestError> {
    let block_count = range
        .end
        .value()
        .saturating_sub(range.start.value())
        .saturating_add(1);
    let worker_count = u32::try_from(canonical_read_workers()).unwrap_or(1).max(1);
    let slice_blocks = block_count
        .div_ceil(worker_count)
        .clamp(1, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS);
    let mut slices = Vec::new();
    let mut start = range.start.value();
    while start <= range.end.value() {
        let end = start
            .saturating_add(slice_blocks - 1)
            .min(range.end.value());
        slices.push(BlockHeightRange::inclusive(
            BlockHeight::new(start),
            BlockHeight::new(end),
        ));
        start = end.saturating_add(1);
        if end == u32::MAX {
            break;
        }
    }
    let deltas = read_slices_in_parallel(&slices, |chunk| {
        let mut deltas = Vec::new();
        for slice in chunk {
            for replay in secondary.scan_canonical_replay_range(*slice)? {
                deltas.push(BlockPrevoutDelta::of_block(replay?.facts()));
            }
        }
        Ok(deltas)
    })?;
    if deltas.len() != usize::try_from(block_count).unwrap_or(usize::MAX)
        || deltas.first().map(BlockPrevoutDelta::height) != Some(range.start)
    {
        return Err(IngestError::MaterializedViewDispatch(format!(
            "canonical heights {}..={} returned {} blocks while warming the prevout cache",
            range.start.value(),
            range.end.value(),
            deltas.len(),
        )));
    }
    Ok(deltas)
}

/// Commitment-tree state carried across sequential hydration ranges.
struct FinalRootsCursor {
    accumulator: Option<CommitmentTreeAccumulator>,
}

impl FinalRootsCursor {
    fn position_at(
        &mut self,
        secondary: &RocksDbCanonicalSecondary,
        activations: &NetworkUpgradeActivations,
        start: BlockHeight,
    ) -> Result<&mut CommitmentTreeAccumulator, IngestError> {
        let anchor = BlockHeight::new(start.value().saturating_sub(1));
        let carried = self
            .accumulator
            .take()
            .filter(|accumulator| accumulator.tip_height() == anchor);
        let accumulator = match carried {
            Some(accumulator) => accumulator,
            None => seeded_accumulator(secondary, activations, anchor)?,
        };
        Ok(self.accumulator.insert(accumulator))
    }
}

fn seeded_accumulator(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    anchor: BlockHeight,
) -> Result<CommitmentTreeAccumulator, IngestError> {
    let checkpoint = secondary
        .tree_state_checkpoint_at(anchor, activations)?
        .ok_or(IngestError::CommitmentTreeCheckpointMissing { height: anchor })?;
    CommitmentTreeAccumulator::from_validated_frontiers(anchor, &checkpoint.frontiers, activations)
        .map_err(|source| IngestError::CommitmentTreeState {
            height: anchor,
            source,
        })
}

fn append_compact_block(
    accumulator: &mut CommitmentTreeAccumulator,
    compact_block: &CompactBlockArtifact,
) -> Result<(), IngestError> {
    let height = compact_block.height();
    let commitments = BlockCommitments::from_compact_block(compact_block);
    accumulator
        .append_block_commitments(
            height,
            &commitments.sapling,
            &commitments.orchard,
            &commitments.ironwood,
        )
        .map_err(|source| IngestError::CommitmentTreeState { height, source })
}

#[derive(Default)]
struct BlockCommitments {
    sapling: Vec<[u8; 32]>,
    orchard: Vec<[u8; 32]>,
    ironwood: Vec<[u8; 32]>,
}

impl BlockCommitments {
    fn from_compact_block(compact_block: &CompactBlockArtifact) -> Self {
        let mut commitments = Self::default();
        for transaction in compact_block.transactions() {
            commitments.sapling.extend(
                transaction
                    .data
                    .sapling_outputs
                    .iter()
                    .map(|output| output.commitment),
            );
            commitments.orchard.extend(
                transaction
                    .data
                    .orchard_actions
                    .iter()
                    .map(|action| action.commitment),
            );
            commitments.ironwood.extend(
                transaction
                    .data
                    .ironwood_actions
                    .iter()
                    .map(|action| action.commitment),
            );
        }
        commitments
    }
}

const fn has_any_pool_root(roots: &BlockFinalNoteCommitmentRoots) -> bool {
    roots.sapling.is_some() || roots.orchard.is_some() || roots.ironwood.is_some()
}

/// One transparent input awaiting its previous output.
struct SpendRequest {
    outpoint: TransparentOutPoint,
    input_index: u32,
    spending_transaction_id: TransactionId,
    tx_index_in_block: u32,
    block_height: BlockHeight,
    block_hash: BlockHash,
}

impl SpendRequest {
    fn into_spend_fact(self, output: &ProducedOutput) -> TransparentSpendFact {
        TransparentSpendFact::new(
            self.outpoint,
            self.input_index,
            self.spending_transaction_id,
            self.tx_index_in_block,
            self.block_height,
            self.block_hash,
            output.value_zat,
            output.address_script_hash,
            output.block_height,
            output.block_hash,
        )
    }
}

/// One previous output located at the block that mined it.
#[derive(Clone, Copy, Debug)]
struct ProducedOutput {
    value_zat: u64,
    address_script_hash: TransparentAddressScriptHash,
    block_height: BlockHeight,
    block_hash: BlockHash,
}

fn transparent_spend_requests(
    blocks: &[CanonicalBlockFacts],
) -> Result<Vec<SpendRequest>, IngestError> {
    let mut requests = Vec::new();
    for block in blocks {
        for (index, transaction) in block.transactions.iter().enumerate() {
            let tx_index_in_block = u32::try_from(index)
                .map_err(|_| CanonicalBlockConstructionError::TransactionIndexOverflow)?;
            append_spend_requests(
                &block.block_header,
                transaction,
                tx_index_in_block,
                &mut requests,
            );
        }
    }
    Ok(requests)
}

fn append_spend_requests(
    block_header: &BlockHeaderArtifact,
    transaction: &CanonicalTransactionFacts,
    tx_index_in_block: u32,
    requests: &mut Vec<SpendRequest>,
) {
    let spending_transaction_id = transaction.public_facts.transaction_id;
    requests.extend(
        transaction
            .transparent_inputs
            .iter()
            .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
            .map(|input| SpendRequest {
                outpoint: input.spent_outpoint,
                input_index: input.input_index,
                spending_transaction_id,
                tx_index_in_block,
                block_height: block_header.height,
                block_hash: block_header.block_hash,
            }),
    );
}

fn produced_outputs_in_range(
    blocks: &[CanonicalBlockFacts],
    requested: &HashSet<TransparentOutPoint>,
) -> HashMap<TransparentOutPoint, ProducedOutput> {
    blocks
        .iter()
        .flat_map(|block| {
            block.transactions.iter().flat_map(move |transaction| {
                transaction.transparent_outputs.iter().map(move |output| {
                    produced_output_entry(
                        &block.block_header,
                        transaction.public_facts.transaction_id,
                        output,
                    )
                })
            })
        })
        .filter(|(outpoint, _)| requested.contains(outpoint))
        .collect()
}

fn produced_output_entry(
    block_header: &BlockHeaderArtifact,
    transaction_id: TransactionId,
    output: &TransparentOutputFact,
) -> (TransparentOutPoint, ProducedOutput) {
    (
        TransparentOutPoint::new(transaction_id, output.output_index),
        ProducedOutput {
            value_zat: output.value_zat,
            address_script_hash: output.address_script_hash,
            block_height: block_header.height,
            block_hash: block_header.block_hash,
        },
    )
}

fn produced_output(
    block: &CanonicalBlockFacts,
    outpoint: TransparentOutPoint,
    tx_index_in_block: u32,
) -> Result<ProducedOutput, IngestError> {
    let index = usize::try_from(tx_index_in_block).unwrap_or(usize::MAX);
    let transaction = block
        .transactions
        .get(index)
        .filter(|transaction| transaction.public_facts.transaction_id == outpoint.transaction_id)
        .ok_or_else(|| {
            prevout_unresolved(
                outpoint,
                "producing block does not carry the located transaction",
            )
        })?;
    let output = transaction
        .transparent_outputs
        .iter()
        .find(|output| output.output_index == outpoint.output_index)
        .ok_or_else(|| {
            prevout_unresolved(
                outpoint,
                "producing transaction has no such transparent output",
            )
        })?;
    let (_, produced) = produced_output_entry(&block.block_header, outpoint.transaction_id, output);
    Ok(produced)
}

const fn prevout_unresolved(outpoint: TransparentOutPoint, reason: &'static str) -> IngestError {
    IngestError::TransparentPrevoutUnresolved {
        transaction_id: outpoint.transaction_id,
        output_index: outpoint.output_index,
        reason,
    }
}

/// `cache` label for spends resolved by a reader without a prevout cache.
const UNCACHED_WALKER_LABEL: &str = "none";

/// Where one hydrated range's distinct spent outputs were found.
#[derive(Clone, Copy)]
struct PrevoutResolutionCounts {
    /// The resolving reader's prevout-cache label, which names its walker.
    walker: &'static str,
    in_range: usize,
    from_cache: usize,
    from_canonical: usize,
    producing_block_reads: usize,
}

fn record_prevout_resolutions(counts: PrevoutResolutionCounts) {
    for (source, resolved) in [
        ("range", counts.in_range),
        ("cache", counts.from_cache),
        ("canonical", counts.from_canonical),
    ] {
        metrics::counter!(
            "zinder_materialized_view_prevout_resolutions_total",
            "source" => source,
            "cache" => counts.walker
        )
        .increment(u64::try_from(resolved).unwrap_or(u64::MAX));
    }
    metrics::counter!(
        "zinder_materialized_view_prevout_block_reads_total",
        "cache" => counts.walker
    )
    .increment(u64::try_from(counts.producing_block_reads).unwrap_or(u64::MAX));
}

fn intrinsic_value_balances(
    blocks: &[CanonicalBlockFacts],
) -> HashMap<TransactionId, TransactionIntrinsicValueBalances> {
    blocks
        .iter()
        .flat_map(|block| block.transactions.iter())
        .map(|transaction| {
            (
                transaction.public_facts.transaction_id,
                transaction.intrinsic_value_balances,
            )
        })
        .collect()
}

fn block_commit_context(
    block: CanonicalBlockFacts,
    final_note_commitment_roots: Option<BlockFinalNoteCommitmentRoots>,
    transparent_spends: &Arc<HashMap<TransparentOutPoint, TransparentSpendFact>>,
    intrinsic_value_balances: &Arc<HashMap<TransactionId, TransactionIntrinsicValueBalances>>,
) -> Result<BlockCommitContext, IngestError> {
    let block_header = block.block_header;
    let transactions = transaction_facts(&block_header, block.transactions)?;
    Ok(BlockCommitContext::new(
        BlockCommitInput {
            height: block_header.height,
            block_hash: block_header.block_hash,
            previous_block_hash: block_header.parent_hash,
            block_time_unix_seconds: block_header.block_time,
            block_size_bytes: block_header.block_size_bytes,
            transactions,
            final_note_commitment_roots,
        },
        TransparentSpendFacts::from_map(Arc::clone(transparent_spends)),
    )
    .with_transaction_intrinsic_value_balances(
        TransactionIntrinsicValueBalanceFacts::from_map(Arc::clone(intrinsic_value_balances)),
    ))
}

fn transaction_facts(
    block_header: &BlockHeaderArtifact,
    transactions: Vec<CanonicalTransactionFacts>,
) -> Result<Vec<TransactionFactsArtifact>, IngestError> {
    transactions
        .into_iter()
        .enumerate()
        .map(|(index, transaction)| {
            let tx_index_in_block = u32::try_from(index)
                .map_err(|_| CanonicalBlockConstructionError::TransactionIndexOverflow)?;
            let location = TransactionLocation::new(
                transaction.public_facts.transaction_id,
                block_header.height,
                block_header.block_hash,
                tx_index_in_block,
            );
            Ok(
                TransactionFactsArtifact::new(location, transaction.public_facts)
                    .with_transparent_facts(
                        transaction.transparent_inputs,
                        transaction.transparent_outputs,
                    ),
            )
        })
        .collect()
}
