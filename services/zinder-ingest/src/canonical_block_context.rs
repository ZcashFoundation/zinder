//! Materialized-view block contexts hydrated from admitted canonical facts.
//!
//! Canonical storage retains block-local facts only. Two inputs the
//! materialized-view consumers require are therefore derived here rather than
//! read from a dedicated family: final note-commitment roots come from the
//! persisted tree-state checkpoints plus compact-block commitments, and
//! transparent spend facts come from the output the spend names. That output
//! is found in the hydrated range, then in an optional [`PrevoutCache`] of
//! outputs earlier ranges produced, and last in the producing block's replay
//! row.

mod prevout_cache;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
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
use zinder_store::RocksDbCanonicalSecondary;

use crate::{CanonicalBlockConstructionError, IngestError};

pub(crate) use prevout_cache::DEFAULT_PREVOUT_CACHE_MAX_OUTPUTS;
pub use prevout_cache::PrevoutCache;

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
    fn read_produced_outputs_below_range(
        &self,
        requested: &HashSet<TransparentOutPoint>,
        produced: &mut HashMap<TransparentOutPoint, ProducedOutput>,
    ) -> Result<usize, IngestError> {
        let mut locations = HashMap::new();
        let mut by_producing_height =
            BTreeMap::<BlockHeight, Vec<(TransparentOutPoint, u32)>>::new();
        for outpoint in requested
            .iter()
            .filter(|outpoint| !produced.contains_key(*outpoint))
        {
            let location = self.transaction_location(&mut locations, *outpoint)?;
            by_producing_height
                .entry(location.block_height)
                .or_default()
                .push((*outpoint, location.tx_index_in_block));
        }
        let producing_block_reads = by_producing_height.len();
        for (height, outpoints) in by_producing_height {
            let block = self.secondary.block_replay_facts_at(height)?;
            for (outpoint, tx_index_in_block) in outpoints {
                let block = block.as_ref().ok_or_else(|| {
                    prevout_unresolved(outpoint, "producing block is not retained")
                })?;
                produced.insert(
                    outpoint,
                    produced_output(block, outpoint, tx_index_in_block)?,
                );
            }
        }
        Ok(producing_block_reads)
    }

    fn transaction_location(
        &self,
        locations: &mut HashMap<TransactionId, TransactionLocation>,
        outpoint: TransparentOutPoint,
    ) -> Result<TransactionLocation, IngestError> {
        if let Some(location) = locations.get(&outpoint.transaction_id) {
            return Ok(*location);
        }
        let location = self
            .secondary
            .transaction_location(outpoint.transaction_id)?
            .ok_or_else(|| {
                prevout_unresolved(outpoint, "producing transaction has no canonical location")
            })?;
        locations.insert(outpoint.transaction_id, location);
        Ok(location)
    }
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
        .tree_state_checkpoint_at_or_before(anchor)?
        .ok_or(IngestError::CommitmentTreeCheckpointMissing { height: anchor })?;
    let checkpoint_height = checkpoint.block_id.height;
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        checkpoint_height,
        &checkpoint.frontiers,
        activations,
    )
    .map_err(|source| IngestError::CommitmentTreeState {
        height: checkpoint_height,
        source,
    })?;
    let gap = checkpoint_height.next().map_or_else(
        || BlockHeightRange::empty_at(anchor),
        |start| BlockHeightRange::inclusive(start, anchor),
    );
    for compact_block in secondary.compact_blocks_in_range(gap)? {
        append_compact_block(&mut accumulator, &compact_block)?;
    }
    Ok(accumulator)
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

/// Where one hydrated range's distinct spent outputs were found.
#[derive(Clone, Copy)]
struct PrevoutResolutionCounts {
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
            "source" => source
        )
        .increment(u64::try_from(resolved).unwrap_or(u64::MAX));
    }
    metrics::counter!("zinder_materialized_view_prevout_block_reads_total")
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
