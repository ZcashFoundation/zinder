//! Shared paid-fee computation and bounded sub-batch hydration for canonical blocks.

use std::collections::{BTreeMap, HashMap, HashSet};

use tonic::Status;
use zinder_core::{
    BlockHash, BlockHeight, CanonicalTransactionFacts, ChainEpoch, TransactionId,
    TransparentOutPoint,
};
use zinder_proto::v1::explorer::{BlockSummary, CoinbaseTransactionSummary};
use zinder_store::RocksDbCanonicalSecondary;

use super::canonical_read::{
    CanonicalReplayVisitError, SharedCanonicalSecondary, encode_coinbase_summary,
    internal_canonical_error, resolve_locked_canonical_epoch,
};
use super::error::ExplorerError;

const DEFAULT_FEE_HYDRATION_SUBBATCH_BLOCKS: usize = 64;
const DEFAULT_FEE_HYDRATION_MAX_INPUTS: usize = 16_384;
const DEFAULT_FEE_HYDRATION_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_FEE_HYDRATION_MAX_PARENT_REPLAYS: usize = 256;
const RESOLVED_PREVOUT_ENTRY_BYTES: usize = 64;

/// Request-wide resource bounds for hydrating block paid fees across canonical sub-batches.
///
/// Bounding sub-batch size and cumulative input, byte, and parent-replay work keeps
/// read-lock windows short for concurrent secondary catch-up and bounds peak memory
/// when a 1,024-block production query spans many historical parent blocks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FeeHydrationBudget {
    /// Maximum number of requested blocks hydrated under a single read guard.
    pub(crate) subbatch_blocks: usize,
    /// Maximum transparent inputs across all requested blocks in one request.
    pub(crate) remaining_inputs: usize,
    /// Approximate memory budget for retained fee inputs and external prevout maps.
    pub(crate) remaining_bytes: usize,
    /// Maximum distinct historical parent block replays across the request.
    pub(crate) remaining_parent_replays: usize,
    /// Stops subsequent sub-batches once the request fee budget is exhausted.
    pub(crate) stop_on_exhaustion: bool,
    /// First `matched` index whose paid-fee hydration exhausted a request budget.
    pub(crate) first_exhausted_index: Option<usize>,
    inputs_exhausted: bool,
    parents_exhausted: bool,
}

impl Default for FeeHydrationBudget {
    fn default() -> Self {
        Self {
            subbatch_blocks: DEFAULT_FEE_HYDRATION_SUBBATCH_BLOCKS,
            remaining_inputs: DEFAULT_FEE_HYDRATION_MAX_INPUTS,
            remaining_bytes: DEFAULT_FEE_HYDRATION_MAX_BYTES,
            remaining_parent_replays: DEFAULT_FEE_HYDRATION_MAX_PARENT_REPLAYS,
            stop_on_exhaustion: false,
            first_exhausted_index: None,
            inputs_exhausted: false,
            parents_exhausted: false,
        }
    }
}

impl FeeHydrationBudget {
    pub(crate) fn for_time_range() -> Self {
        Self {
            stop_on_exhaustion: true,
            ..Self::default()
        }
    }

    fn record_exhaustion(&mut self, block_index: usize) {
        self.first_exhausted_index = Some(
            self.first_exhausted_index
                .map_or(block_index, |prev| prev.min(block_index)),
        );
    }

    fn reserve_block_inputs(&mut self, block_index: usize, inputs: &BlockFeeInputs) -> bool {
        if self.inputs_exhausted || self.parents_exhausted {
            self.record_exhaustion(block_index);
            return false;
        }
        let estimated = inputs.estimated_total_bytes();
        if inputs.transparent_input_count > self.remaining_inputs
            || estimated > self.remaining_bytes
        {
            self.inputs_exhausted = true;
            self.record_exhaustion(block_index);
            return false;
        }
        self.remaining_inputs -= inputs.transparent_input_count;
        self.remaining_bytes -= estimated;
        true
    }

    fn reserve_block_parents(&mut self, block_index: usize, additional_parents: usize) -> bool {
        if self.parents_exhausted {
            self.record_exhaustion(block_index);
            return false;
        }
        if additional_parents > self.remaining_parent_replays {
            self.parents_exhausted = true;
            self.record_exhaustion(block_index);
            return false;
        }
        self.remaining_parent_replays -= additional_parents;
        true
    }
}

/// Extracted per-block transparent inputs, intra-block outputs, and shielded net balances.
pub(crate) struct BlockFeeInputs {
    intra_block_outputs: HashMap<TransparentOutPoint, u64>,
    non_coinbase_txs: Vec<TxFeeInputs>,
    external_outpoints: Vec<TransparentOutPoint>,
    transparent_input_count: usize,
    estimated_bytes: usize,
}

struct TxFeeInputs {
    spent_outpoints: Vec<TransparentOutPoint>,
    transparent_output_sum_zat: u64,
    shielded_net_to_transparent_zat: i128,
}

impl BlockFeeInputs {
    /// Collects transparent input/output totals and shielded value balances from `transactions`.
    ///
    /// Returns `None` if any non-coinbase transaction uses an unsupported version, unsupported
    /// sections, a coinbase sentinel input, or overflowing arithmetic.
    pub(crate) fn from_transactions(transactions: &[CanonicalTransactionFacts]) -> Option<Self> {
        let mut intra_block_outputs = HashMap::new();
        let mut non_coinbase_txs = Vec::new();
        let mut external_outpoints = Vec::new();
        let mut transparent_input_count = 0_usize;

        for transaction in transactions {
            let txid = transaction.public_facts.transaction_id;
            if !transaction.public_facts.is_coinbase {
                let parsed = collect_non_coinbase_tx_fee_inputs(
                    transaction,
                    &intra_block_outputs,
                    &mut external_outpoints,
                    &mut transparent_input_count,
                )?;
                non_coinbase_txs.push(parsed);
            }
            for output in &transaction.transparent_outputs {
                intra_block_outputs.insert(
                    TransparentOutPoint::new(txid, output.output_index),
                    output.value_zat,
                );
            }
        }

        let estimated_bytes = estimate_block_fee_input_bytes(
            &intra_block_outputs,
            &non_coinbase_txs,
            transparent_input_count,
            &external_outpoints,
        );
        Some(Self {
            intra_block_outputs,
            non_coinbase_txs,
            external_outpoints,
            transparent_input_count,
            estimated_bytes,
        })
    }

    /// Computes total paid fees collected by the block in zatoshis.
    pub(crate) fn compute_paid_fees(
        &self,
        lookup_external_zat: impl Fn(&TransparentOutPoint) -> Option<u64>,
    ) -> Option<u64> {
        let mut total_paid_fee_zat = 0_u64;
        for transaction in &self.non_coinbase_txs {
            let mut input_sum_zat = 0_u64;
            for outpoint in &transaction.spent_outpoints {
                let prevout_zat = self
                    .intra_block_outputs
                    .get(outpoint)
                    .copied()
                    .or_else(|| lookup_external_zat(outpoint))?;
                input_sum_zat = input_sum_zat.checked_add(prevout_zat)?;
            }
            let tx_fee_i128 = i128::from(input_sum_zat)
                .checked_add(transaction.shielded_net_to_transparent_zat)?
                .checked_sub(i128::from(transaction.transparent_output_sum_zat))?;
            let tx_fee_zat = u64::try_from(tx_fee_i128).ok()?;
            total_paid_fee_zat = total_paid_fee_zat.checked_add(tx_fee_zat)?;
        }
        Some(total_paid_fee_zat)
    }

    fn estimated_total_bytes(&self) -> usize {
        self.estimated_bytes.saturating_add(
            self.external_outpoints
                .len()
                .saturating_mul(RESOLVED_PREVOUT_ENTRY_BYTES),
        )
    }
}

fn collect_non_coinbase_tx_fee_inputs(
    transaction: &CanonicalTransactionFacts,
    intra_block_outputs: &HashMap<TransparentOutPoint, u64>,
    external_outpoints: &mut Vec<TransparentOutPoint>,
    transparent_input_count: &mut usize,
) -> Option<TxFeeInputs> {
    if !transaction.public_facts.version.is_supported()
        || !transaction.public_facts.unsupported_sections.is_empty()
    {
        return None;
    }
    let mut output_sum_zat = 0_u64;
    for output in &transaction.transparent_outputs {
        output_sum_zat = output_sum_zat.checked_add(output.value_zat)?;
    }
    let mut spent_outpoints = Vec::with_capacity(transaction.transparent_inputs.len());
    for input in &transaction.transparent_inputs {
        if input.spent_outpoint == TransparentOutPoint::COINBASE_SENTINEL {
            return None;
        }
        *transparent_input_count = transparent_input_count.saturating_add(1);
        if !intra_block_outputs.contains_key(&input.spent_outpoint) {
            external_outpoints.push(input.spent_outpoint);
        }
        spent_outpoints.push(input.spent_outpoint);
    }
    let balances = transaction.intrinsic_value_balances;
    let shielded_net_to_transparent_zat = i128::from(balances.sprout_zat)
        .checked_add(i128::from(balances.sapling_zat))?
        .checked_add(i128::from(balances.orchard_zat))?
        .checked_add(i128::from(balances.ironwood_zat))?;
    Some(TxFeeInputs {
        spent_outpoints,
        transparent_output_sum_zat: output_sum_zat,
        shielded_net_to_transparent_zat,
    })
}

fn estimate_block_fee_input_bytes(
    intra_block_outputs: &HashMap<TransparentOutPoint, u64>,
    non_coinbase_txs: &[TxFeeInputs],
    transparent_input_count: usize,
    external_outpoints: &[TransparentOutPoint],
) -> usize {
    intra_block_outputs
        .capacity()
        .saturating_mul(std::mem::size_of::<(TransparentOutPoint, u64)>() + 16)
        .saturating_add(
            non_coinbase_txs
                .len()
                .saturating_mul(std::mem::size_of::<TxFeeInputs>()),
        )
        .saturating_add(
            transparent_input_count.saturating_mul(std::mem::size_of::<TransparentOutPoint>()),
        )
        .saturating_add(
            external_outpoints
                .len()
                .saturating_mul(std::mem::size_of::<TransparentOutPoint>()),
        )
}

/// Hydrates coinbase summaries and paid block fees in bounded sub-batches under pinned-epoch reads.
///
/// Releasing the canonical read guard and temporary input/prevout maps after each sub-batch keeps
/// read-lock hold times short and prevents unbounded memory growth over large ranges.
pub(crate) fn hydrate_coinbase_and_paid_fees_with_budget(
    canonical_store: &SharedCanonicalSecondary,
    epoch: ChainEpoch,
    matched: &mut [(BlockHeight, BlockHash, u32, BlockSummary)],
    budget: &mut FeeHydrationBudget,
) -> Result<Vec<Option<CoinbaseTransactionSummary>>, Status> {
    if matched.is_empty() {
        return Ok(Vec::new());
    }
    let subbatch_blocks = budget.subbatch_blocks.max(1);
    let mut coinbases = vec![None; matched.len()];
    let mut offset = 0_usize;
    while offset < matched.len() {
        let end = offset.saturating_add(subbatch_blocks).min(matched.len());
        hydrate_subbatch(
            canonical_store,
            epoch,
            HydrateSubbatchTarget {
                matched: &mut matched[offset..end],
                coinbases: &mut coinbases[offset..end],
                offset,
            },
            budget,
        )?;
        if budget.stop_on_exhaustion && budget.first_exhausted_index.is_some() {
            break;
        }
        offset = end;
    }
    Ok(coinbases)
}

struct HydrateSubbatchTarget<'a> {
    matched: &'a mut [(BlockHeight, BlockHash, u32, BlockSummary)],
    coinbases: &'a mut [Option<CoinbaseTransactionSummary>],
    offset: usize,
}

fn hydrate_subbatch(
    canonical_store: &SharedCanonicalSecondary,
    epoch: ChainEpoch,
    target: HydrateSubbatchTarget<'_>,
    budget: &mut FeeHydrationBudget,
) -> Result<(), Status> {
    let HydrateSubbatchTarget {
        matched: chunk_matched,
        coinbases: chunk_coinbases,
        offset,
    } = target;
    let guard = canonical_store.read();
    let verified_epoch = resolve_locked_canonical_epoch(&guard, Some(epoch.id.value()))?;
    let heights: Vec<BlockHeight> = chunk_matched
        .iter()
        .map(|(height, _, _, _)| *height)
        .collect();
    let mut block_fee_inputs: Vec<Option<BlockFeeInputs>> = Vec::with_capacity(chunk_matched.len());
    block_fee_inputs.resize_with(chunk_matched.len(), || None);

    guard
        .visit_block_replay_facts_at_heights(&heights, |idx, maybe_facts| {
            let Some(facts) = maybe_facts else {
                return Ok(());
            };
            let Some((_, expected_hash, _, _)) = chunk_matched.get(idx) else {
                return Err(Status::from(ExplorerError::internal(
                    "replay height index out of bounds",
                ))
                .into());
            };
            if facts.block_header.block_hash != *expected_hash {
                return Err(Status::from(ExplorerError::unsatisfied_precondition(
                    "canonical replay block hash disagrees with canonical header",
                ))
                .into());
            }
            chunk_coinbases[idx] = encode_coinbase_summary(&facts)?;
            if !budget.inputs_exhausted
                && !budget.parents_exhausted
                && let Some(fee_inputs) = BlockFeeInputs::from_transactions(&facts.transactions)
                && budget.reserve_block_inputs(offset + idx, &fee_inputs)
            {
                block_fee_inputs[idx] = Some(fee_inputs);
            }
            Ok::<(), CanonicalReplayVisitError>(())
        })
        .map_err(Status::from)?;

    let external_prevouts = resolve_subbatch_external_prevouts(
        &guard,
        verified_epoch.visible_tip_height,
        offset,
        &mut block_fee_inputs,
        budget,
    )?;
    drop(guard);

    for ((_, _, _, summary), fee_inputs) in chunk_matched.iter_mut().zip(block_fee_inputs) {
        summary.paid_fees_collected_zat = fee_inputs.as_ref().and_then(|inputs| {
            inputs.compute_paid_fees(|outpoint| external_prevouts.get(outpoint).copied())
        });
    }
    Ok(())
}

type TxLocationMap = HashMap<TransactionId, (BlockHeight, BlockHash)>;

fn resolve_subbatch_external_prevouts(
    secondary: &RocksDbCanonicalSecondary,
    visible_tip_height: BlockHeight,
    offset: usize,
    block_fee_inputs: &mut [Option<BlockFeeInputs>],
    budget: &mut FeeHydrationBudget,
) -> Result<HashMap<TransparentOutPoint, u64>, Status> {
    let tx_locations =
        lookup_subbatch_tx_locations(secondary, visible_tip_height, block_fee_inputs)?;
    let (selected_parents, selected_outpoints) =
        select_subbatch_parent_replays(offset, block_fee_inputs, &tx_locations, budget);
    if selected_parents.is_empty() {
        return Ok(HashMap::new());
    }
    replay_selected_parent_blocks(secondary, &selected_parents, &selected_outpoints)
}

fn lookup_subbatch_tx_locations(
    secondary: &RocksDbCanonicalSecondary,
    visible_tip_height: BlockHeight,
    block_fee_inputs: &[Option<BlockFeeInputs>],
) -> Result<TxLocationMap, Status> {
    let mut needed_txids = HashSet::new();
    for fee_inputs in block_fee_inputs.iter().flatten() {
        for outpoint in &fee_inputs.external_outpoints {
            needed_txids.insert(outpoint.transaction_id);
        }
    }
    if needed_txids.is_empty() {
        return Ok(HashMap::new());
    }
    let txids: Vec<TransactionId> = needed_txids.into_iter().collect();
    let locations = secondary
        .transaction_locations(&txids)
        .map_err(internal_canonical_error)?;
    let first_retained = secondary.history_bounds().first_available_height();
    let mut tx_locations = HashMap::with_capacity(txids.len());
    for location in locations.into_iter().flatten() {
        if location.block_height >= first_retained && location.block_height <= visible_tip_height {
            tx_locations.insert(
                location.transaction_id,
                (location.block_height, location.block_hash),
            );
        }
    }
    Ok(tx_locations)
}

fn select_subbatch_parent_replays(
    offset: usize,
    block_fee_inputs: &mut [Option<BlockFeeInputs>],
    tx_locations: &TxLocationMap,
    budget: &mut FeeHydrationBudget,
) -> (
    BTreeMap<BlockHeight, BlockHash>,
    HashSet<TransparentOutPoint>,
) {
    let mut selected_parents = BTreeMap::new();
    let mut selected_outpoints = HashSet::new();
    for (idx, maybe_inputs) in block_fee_inputs.iter_mut().enumerate() {
        let Some(fee_inputs) = maybe_inputs.as_ref() else {
            continue;
        };
        let mut newly_needed_parents = BTreeMap::new();
        for outpoint in &fee_inputs.external_outpoints {
            if let Some(&(parent_height, parent_hash)) = tx_locations.get(&outpoint.transaction_id)
                && !selected_parents.contains_key(&parent_height)
            {
                newly_needed_parents.insert(parent_height, parent_hash);
            }
        }
        if budget.reserve_block_parents(offset + idx, newly_needed_parents.len()) {
            selected_parents.extend(newly_needed_parents);
            selected_outpoints.extend(fee_inputs.external_outpoints.iter().copied());
        } else {
            *maybe_inputs = None;
        }
    }
    (selected_parents, selected_outpoints)
}

fn replay_selected_parent_blocks(
    secondary: &RocksDbCanonicalSecondary,
    parent_hashes_by_height: &BTreeMap<BlockHeight, BlockHash>,
    needed_outpoints: &HashSet<TransparentOutPoint>,
) -> Result<HashMap<TransparentOutPoint, u64>, Status> {
    let parent_heights: Vec<BlockHeight> = parent_hashes_by_height.keys().copied().collect();
    let mut resolved = HashMap::with_capacity(needed_outpoints.len().min(parent_heights.len() * 8));
    secondary
        .visit_block_replay_facts_at_heights(&parent_heights, |idx, maybe_facts| {
            let Some(facts) = maybe_facts else {
                return Ok(());
            };
            let Some(parent_height) = parent_heights.get(idx) else {
                return Err(Status::from(ExplorerError::internal(
                    "parent replay height index out of bounds",
                ))
                .into());
            };
            if parent_hashes_by_height.get(parent_height) != Some(&facts.block_header.block_hash) {
                return Err(Status::from(ExplorerError::unsatisfied_precondition(
                    "canonical parent replay block hash disagrees with transaction location",
                ))
                .into());
            }
            for transaction in facts.transactions {
                let txid = transaction.public_facts.transaction_id;
                for output in transaction.transparent_outputs {
                    let outpoint = TransparentOutPoint::new(txid, output.output_index);
                    if needed_outpoints.contains(&outpoint) {
                        resolved.insert(outpoint, output.value_zat);
                    }
                }
            }
            Ok::<(), CanonicalReplayVisitError>(())
        })
        .map_err(Status::from)?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::sync::Arc;

    use parking_lot::RwLock;
    use zinder_core::{
        LockTime, Network, PrivacyShape, TransactionComponentCounts, TransactionLocation,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentOutPoint, TransparentSpendFact, TransparentUnspentOutput,
    };
    use zinder_testkit::{
        ChainFixture, FixtureTransactionRows, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    use super::*;

    struct MultiBlockFeeFixture {
        _guard: WalletServingStoreFixture,
        canonical_store: SharedCanonicalSecondary,
        epoch: ChainEpoch,
        headers: Vec<(BlockHeight, BlockHash, u32)>,
    }

    #[derive(Clone, Copy)]
    struct SpendChainStep<'a> {
        prev_outpoint: TransparentOutPoint,
        prev_block: &'a zinder_testkit::FixtureBlock,
        spend_txid: TransactionId,
        spend_block: &'a zinder_testkit::FixtureBlock,
        input_zat: u64,
        output_zat: u64,
    }

    fn supported_public_facts(
        transaction_id: TransactionId,
        is_coinbase: bool,
    ) -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V4,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 120,
            counts: TransactionComponentCounts::EMPTY,
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: if is_coinbase {
                PrivacyShape::Coinbase
            } else {
                PrivacyShape::TransparentOnly
            },
            is_coinbase,
            unsupported_sections: Vec::new(),
        }
    }

    fn attach_spend_chain_step(mut chain: ChainFixture, step: SpendChainStep<'_>) -> ChainFixture {
        let SpendChainStep {
            prev_outpoint,
            prev_block,
            spend_txid,
            spend_block,
            input_zat,
            output_zat,
        } = step;
        let script = vec![0x76, 0xa9, 0x14];
        let script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        chain = chain
            .with_address_output_index(TransparentUnspentOutput::new(
                script_hash,
                script.clone(),
                prev_outpoint,
                input_zat,
                prev_block.height,
                prev_block.hash,
            ))
            .with_transaction_rows(FixtureTransactionRows::from_public_facts(
                TransactionLocation::new(spend_txid, spend_block.height, spend_block.hash, 1),
                supported_public_facts(spend_txid, false),
            ))
            .with_transparent_spend_fact(TransparentSpendFact::new(
                prev_outpoint,
                0,
                spend_txid,
                1,
                spend_block.height,
                spend_block.hash,
                input_zat,
                script_hash,
                prev_block.height,
                prev_block.hash,
            ))
            .with_address_output_index(TransparentUnspentOutput::new(
                script_hash,
                script,
                TransparentOutPoint::new(spend_txid, 0),
                output_zat,
                spend_block.height,
                spend_block.hash,
            ));
        chain
    }

    fn build_multi_block_fee_fixture() -> Result<MultiBlockFeeFixture, Box<dyn std::error::Error>> {
        let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
        let blocks = chain.blocks().to_vec();
        let cb1_txid = TransactionId::from_bytes([0x11; 32]);
        let cb2_txid = TransactionId::from_bytes([0x21; 32]);
        let spend2_txid = TransactionId::from_bytes([0x22; 32]);
        let spend3_txid = TransactionId::from_bytes([0x32; 32]);
        let spend4_txid = TransactionId::from_bytes([0x42; 32]);

        for (idx, block) in blocks.iter().enumerate() {
            let cb_byte = u8::try_from(idx + 1)? * 0x10 + 1;
            let coinbase_id = TransactionId::from_bytes([cb_byte; 32]);
            chain = chain.with_transaction_rows(FixtureTransactionRows::from_public_facts(
                TransactionLocation::new(coinbase_id, block.height, block.hash, 0),
                supported_public_facts(coinbase_id, true),
            ));
        }

        chain = attach_spend_chain_step(
            chain,
            SpendChainStep {
                prev_outpoint: TransparentOutPoint::new(cb1_txid, 0),
                prev_block: &blocks[0],
                spend_txid: spend2_txid,
                spend_block: &blocks[1],
                input_zat: 100_000,
                output_zat: 90_000,
            },
        );
        chain = attach_spend_chain_step(
            chain,
            SpendChainStep {
                prev_outpoint: TransparentOutPoint::new(cb2_txid, 0),
                prev_block: &blocks[1],
                spend_txid: spend3_txid,
                spend_block: &blocks[2],
                input_zat: 50_000,
                output_zat: 45_000,
            },
        );
        chain = attach_spend_chain_step(
            chain,
            SpendChainStep {
                prev_outpoint: TransparentOutPoint::new(spend3_txid, 0),
                prev_block: &blocks[2],
                spend_txid: spend4_txid,
                spend_block: &blocks[3],
                input_zat: 45_000,
                output_zat: 42_000,
            },
        );

        let mut store_fixture =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let (canonical_reader, _wallet_reader) = store_fixture.take_readers()?;
        let epoch = canonical_reader.chain_epoch()?;
        let headers = blocks
            .into_iter()
            .map(|blk| (blk.height, blk.hash, 0x1d00_ffff))
            .collect();
        Ok(MultiBlockFeeFixture {
            _guard: store_fixture,
            canonical_store: Arc::new(RwLock::new(canonical_reader)),
            epoch,
            headers,
        })
    }

    fn empty_summary(height: BlockHeight) -> BlockSummary {
        BlockSummary {
            block_height: height.value(),
            ..Default::default()
        }
    }

    fn hydrate_with_budget_for_test(
        fixture: &MultiBlockFeeFixture,
        mut budget: FeeHydrationBudget,
    ) -> Result<Vec<Option<u64>>, Box<dyn std::error::Error>> {
        let mut matched: Vec<_> = fixture
            .headers
            .iter()
            .map(|&(height, hash, bits)| (height, hash, bits, empty_summary(height)))
            .collect();
        let coinbases = hydrate_coinbase_and_paid_fees_with_budget(
            &fixture.canonical_store,
            fixture.epoch,
            &mut matched,
            &mut budget,
        )?;
        assert_eq!(coinbases.len(), matched.len());
        assert!(coinbases.iter().all(Option::is_some));
        Ok(matched
            .into_iter()
            .map(|(_, _, _, summary)| summary.paid_fees_collected_zat)
            .collect())
    }

    #[test]
    fn subbatch_hydration_computes_exact_paid_fees_and_coinbases()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = build_multi_block_fee_fixture()?;
        let paid_fees = hydrate_with_budget_for_test(
            &fixture,
            FeeHydrationBudget {
                subbatch_blocks: 2,
                ..FeeHydrationBudget::default()
            },
        )?;
        assert_eq!(
            paid_fees,
            vec![Some(0), Some(10_000), Some(5_000), Some(3_000)]
        );
        Ok(())
    }

    #[test]
    fn fee_budget_limits_leave_remaining_paid_fees_unavailable_with_coinbases()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = build_multi_block_fee_fixture()?;

        let input_fees = hydrate_with_budget_for_test(
            &fixture,
            FeeHydrationBudget {
                subbatch_blocks: 4,
                remaining_inputs: 1,
                ..FeeHydrationBudget::default()
            },
        )?;
        assert_eq!(input_fees, vec![Some(0), Some(10_000), None, None]);

        let byte_fees = hydrate_with_budget_for_test(
            &fixture,
            FeeHydrationBudget {
                subbatch_blocks: 1,
                remaining_bytes: 256,
                ..FeeHydrationBudget::default()
            },
        )?;
        assert_eq!(byte_fees[0], Some(0));
        assert_eq!(byte_fees[3], None);

        let parent_fees = hydrate_with_budget_for_test(
            &fixture,
            FeeHydrationBudget {
                subbatch_blocks: 2,
                remaining_parent_replays: 1,
                ..FeeHydrationBudget::default()
            },
        )?;
        assert_eq!(parent_fees, vec![Some(0), Some(10_000), None, None]);
        Ok(())
    }

    #[test]
    fn stop_on_exhaustion_halts_subbatches_and_prioritizes_matched_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = build_multi_block_fee_fixture()?;

        let mut matched: Vec<_> = fixture
            .headers
            .iter()
            .map(|&(height, hash, bits)| (height, hash, bits, empty_summary(height)))
            .collect();
        let mut stop_budget = FeeHydrationBudget {
            subbatch_blocks: 1,
            remaining_parent_replays: 1,
            stop_on_exhaustion: true,
            ..FeeHydrationBudget::default()
        };
        let coinbases = hydrate_coinbase_and_paid_fees_with_budget(
            &fixture.canonical_store,
            fixture.epoch,
            &mut matched,
            &mut stop_budget,
        )?;
        assert_eq!(stop_budget.first_exhausted_index, Some(2));
        assert!(coinbases[0].is_some() && coinbases[1].is_some() && coinbases[2].is_some());
        assert!(coinbases[3].is_none());

        let mut reordered: Vec<_> = [fixture.headers[0], fixture.headers[3], fixture.headers[1]]
            .into_iter()
            .map(|(height, hash, bits)| (height, hash, bits, empty_summary(height)))
            .collect();
        let mut order_budget = FeeHydrationBudget {
            subbatch_blocks: 4,
            remaining_parent_replays: 1,
            stop_on_exhaustion: true,
            ..FeeHydrationBudget::default()
        };
        hydrate_coinbase_and_paid_fees_with_budget(
            &fixture.canonical_store,
            fixture.epoch,
            &mut reordered,
            &mut order_budget,
        )?;
        let reordered_fees: Vec<_> = reordered
            .into_iter()
            .map(|(_, _, _, summary)| summary.paid_fees_collected_zat)
            .collect();
        assert_eq!(reordered_fees, vec![Some(0), Some(3_000), None]);
        assert_eq!(order_budget.first_exhausted_index, Some(2));
        Ok(())
    }
}
