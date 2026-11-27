//! `ExplorerQuery.BlockTransactions` handler.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockFinalNoteCommitmentRoots as CoreFinalRoots, BlockHash, BlockHeight, BlockHeightRange,
    CanonicalBlockFacts, CanonicalTransactionFacts, ChainEpoch, NetworkUpgradeActivations,
    TransactionId, TransactionLocation, TransparentOutPoint,
    wire::{decode_rpc_block_hash_hex, encode_height_key_ascending, encode_rpc_transaction_id_hex},
};
use zinder_materialized_views::{BLOCK_SUMMARY_COLUMN_FAMILY, MaterializedViewStore};
use zinder_proto::capabilities::EXPLORER_BLOCK_TRANSACTIONS_V2;
use zinder_proto::v1::{
    explorer::{
        BlockDetailRequest, BlockFinalNoteCommitmentRoots as WireFinalRoots, BlockSummary,
        BlockTransaction, BlockTransactionsResponse, TransparentInput, block_detail_request,
    },
    wallet,
};
use zinder_store::{
    BlockHashLookup, CanonicalStoreError, RocksDbCanonicalSecondary, chain_epoch_message,
};

use super::block_paid_fees::BlockFeeInputs;
use super::canonical_read::{
    CanonicalReplayVisitError, SharedCanonicalSecondary, internal_canonical_error,
    join_block_summary_with_header, maybe_catch_up_for_epoch, resolve_locked_canonical_epoch,
};
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use super::transaction_detail::encode_public_facts;

/// Bounded request-wide parent-block replay budget matching block-production fee hydration.
const DEFAULT_MAX_PARENT_REPLAYS: usize = 256;
/// Maximum distinct historical parent heights replayed under one canonical read guard.
const PARENT_REPLAY_SUBBATCH_HEIGHTS: usize = 64;

/// Resolved prevout amount and script for one non-coinbase transparent input.
type ResolvedPrevout = (u64, Vec<u8>);

/// Shared dependencies for `BlockTransactions`.
pub(crate) struct BlockTransactionsContext<'store> {
    pub(crate) canonical_store: &'store SharedCanonicalSecondary,
    pub(crate) materialized_view_store: &'store MaterializedViewStore,
    pub(crate) network_upgrade_activations: Option<&'store NetworkUpgradeActivations>,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

enum ParsedBlockSelector {
    Height(BlockHeight),
    Hash {
        block_hash: BlockHash,
        raw_hex: String,
    },
}

struct BlockTransactionsQuery<'scope> {
    at_epoch_id: Option<u64>,
    selector: &'scope ParsedBlockSelector,
    fallback_activations: Option<&'scope NetworkUpgradeActivations>,
    max_parent_replays: usize,
}

struct InitialBlockTransactionsRead {
    epoch: ChainEpoch,
    height: BlockHeight,
    facts: CanonicalBlockFacts,
    final_roots: Option<WireFinalRoots>,
    location_by_txid: HashMap<TransactionId, TransactionLocation>,
    first_retained: BlockHeight,
}

struct BlockTransactionsOutcome {
    epoch: ChainEpoch,
    summary: BlockSummary,
    transactions: Vec<BlockTransaction>,
    final_roots: Option<WireFinalRoots>,
}

/// Serves `ExplorerQuery.BlockTransactions`.
pub(crate) async fn query_block_transactions(
    context: BlockTransactionsContext<'_>,
    request: Request<BlockDetailRequest>,
) -> Result<Response<BlockTransactionsResponse>, Status> {
    let inner = request.into_inner();
    let selector = parse_block_selector(inner.selector)?;
    let fallback_activations = context.network_upgrade_activations.cloned();
    let mv_store = context.materialized_view_store.clone();
    let canonical_store = Arc::clone(context.canonical_store);
    let at_epoch_id = inner.at_epoch_id;

    let outcome = tokio::task::spawn_blocking(move || {
        let query = BlockTransactionsQuery {
            at_epoch_id,
            selector: &selector,
            fallback_activations: fallback_activations.as_ref(),
            max_parent_replays: DEFAULT_MAX_PARENT_REPLAYS,
        };
        read_block_transactions_with_budget(&canonical_store, &mv_store, &query)
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("canonical read task failed: {error}")))??;

    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            Some(context.materialized_view_store),
            EXPLORER_BLOCK_TRANSACTIONS_V2,
            Some(chain_epoch_message(outcome.epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(BlockTransactionsResponse {
        freshness: Some(freshness),
        summary: Some(outcome.summary),
        transactions: outcome.transactions,
        final_note_commitment_roots: outcome.final_roots,
    }))
}

fn parse_block_selector(
    selector: Option<block_detail_request::Selector>,
) -> Result<ParsedBlockSelector, Status> {
    match selector
        .ok_or_else(|| ExplorerError::invalid_request("BlockDetailRequest.selector is required"))?
    {
        block_detail_request::Selector::BlockHeight(height) => {
            Ok(ParsedBlockSelector::Height(BlockHeight::new(height)))
        }
        block_detail_request::Selector::BlockHash(raw_hex) => {
            let block_hash = decode_rpc_block_hash_hex(&raw_hex)
                .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;
            Ok(ParsedBlockSelector::Hash {
                block_hash,
                raw_hex,
            })
        }
    }
}

fn read_block_transactions_with_budget(
    canonical_store: &SharedCanonicalSecondary,
    mv_store: &MaterializedViewStore,
    query: &BlockTransactionsQuery<'_>,
) -> Result<BlockTransactionsOutcome, Status> {
    let initial = read_initial_block_transactions(canonical_store, query)?;
    let mut summary = read_validated_block_summary(
        mv_store,
        initial.height,
        &initial.facts,
        initial.epoch.visible_tip_height.value(),
    )?;
    let (intra_block_needed, external_by_height) = partition_needed_prevouts(
        &initial.facts.transactions,
        &initial.location_by_txid,
        initial.height,
        initial.epoch.visible_tip_height,
        initial.first_retained,
    );
    let (prevouts, parent_budget_exhausted) = resolve_block_prevouts_bounded(
        canonical_store,
        &initial,
        intra_block_needed,
        &external_by_height,
        query.max_parent_replays,
    )?;

    summary.paid_fees_collected_zat = (!parent_budget_exhausted)
        .then(|| {
            BlockFeeInputs::from_transactions(&initial.facts.transactions).and_then(|fee_inputs| {
                fee_inputs.compute_paid_fees(|outpoint| {
                    prevouts.get(outpoint).map(|(amount_zat, _)| *amount_zat)
                })
            })
        })
        .flatten();
    let transactions = encode_block_transactions(&initial.facts.transactions, &prevouts)?;

    Ok(BlockTransactionsOutcome {
        epoch: initial.epoch,
        summary,
        transactions,
        final_roots: initial.final_roots,
    })
}

fn read_initial_block_transactions(
    canonical_store: &SharedCanonicalSecondary,
    query: &BlockTransactionsQuery<'_>,
) -> Result<InitialBlockTransactionsRead, Status> {
    maybe_catch_up_for_epoch(canonical_store, query.at_epoch_id)?;
    let guard = canonical_store.read();
    let epoch = resolve_locked_canonical_epoch(&guard, query.at_epoch_id)?;
    let height = resolve_canonical_height(&guard, epoch, query.selector)?;
    let facts = guard
        .block_replay_facts_at(height)
        .map_err(internal_canonical_error)?
        .ok_or_else(|| {
            ExplorerError::not_materialized(format!(
                "canonical replay facts are unavailable at height {}",
                height.value(),
            ))
        })?;
    let final_roots = read_block_final_roots(&guard, height, query.fallback_activations)?;
    let parent_txids = collect_unique_parent_txids(&facts.transactions);
    let location_by_txid = if parent_txids.is_empty() {
        HashMap::new()
    } else {
        let locations = guard
            .transaction_locations(&parent_txids)
            .map_err(internal_canonical_error)?;
        parent_txids
            .into_iter()
            .zip(locations)
            .filter_map(|(txid, maybe_loc)| maybe_loc.map(|loc| (txid, loc)))
            .collect()
    };
    let first_retained = guard.history_bounds().first_available_height();
    drop(guard);

    Ok(InitialBlockTransactionsRead {
        epoch,
        height,
        facts,
        final_roots,
        location_by_txid,
        first_retained,
    })
}

fn read_validated_block_summary(
    mv_store: &MaterializedViewStore,
    height: BlockHeight,
    facts: &CanonicalBlockFacts,
    visible_tip_height: u32,
) -> Result<BlockSummary, Status> {
    let summary_payload = mv_store
        .get_consumer(
            BLOCK_SUMMARY_COLUMN_FAMILY,
            &encode_height_key_ascending(height),
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| {
            ExplorerError::not_materialized(format!(
                "BlockSummary is not materialized for height {}",
                height.value(),
            ))
        })?;
    join_block_summary_with_header(&summary_payload, &facts.block_header, visible_tip_height)?
        .ok_or_else(|| {
            ExplorerError::unsatisfied_precondition(format!(
                "materialized BlockSummary at height {} disagrees with canonical block header",
                height.value(),
            ))
            .into()
        })
}

fn resolve_canonical_height(
    secondary: &RocksDbCanonicalSecondary,
    epoch: ChainEpoch,
    selector: &ParsedBlockSelector,
) -> Result<BlockHeight, Status> {
    match selector {
        ParsedBlockSelector::Height(height) => {
            if *height > epoch.visible_tip_height
                || secondary.history_bounds().intentionally_excludes(*height)
            {
                return Err(ExplorerError::not_materialized(format!(
                    "canonical block {} is absent at visible tip {}",
                    height.value(),
                    epoch.visible_tip_height.value(),
                ))
                .into());
            }
            if secondary
                .block_header_at(*height)
                .map_err(internal_canonical_error)?
                .is_none()
            {
                return Err(ExplorerError::not_materialized(format!(
                    "canonical block {} is not present in the secondary store",
                    height.value(),
                ))
                .into());
            }
            Ok(*height)
        }
        ParsedBlockSelector::Hash {
            block_hash,
            raw_hex,
        } => match secondary
            .block_hash_lookup(*block_hash)
            .map_err(internal_canonical_error)?
        {
            BlockHashLookup::Resolved(block_id) if block_id.height <= epoch.visible_tip_height => {
                Ok(block_id.height)
            }
            BlockHashLookup::Resolved(_)
            | BlockHashLookup::NotInBestChain
            | BlockHashLookup::NotIndexed => Err(ExplorerError::not_materialized(format!(
                "block hash {raw_hex} is absent from the canonical chain",
            ))
            .into()),
        },
    }
}

fn resolve_block_prevouts_bounded(
    canonical_store: &SharedCanonicalSecondary,
    initial: &InitialBlockTransactionsRead,
    intra_block_needed: Vec<NeededOutpoint>,
    external_by_height: &ExternalOutpointsByHeight,
    max_parent_replays: usize,
) -> Result<(HashMap<TransparentOutPoint, ResolvedPrevout>, bool), Status> {
    let mut external_heights: Vec<BlockHeight> = external_by_height.keys().copied().collect();
    let parent_budget_exhausted = external_heights.len() > max_parent_replays;
    external_heights.truncate(max_parent_replays);
    let budgeted_external_outpoints: usize = external_heights
        .iter()
        .filter_map(|parent_height| external_by_height.get(parent_height).map(Vec::len))
        .sum();
    let mut resolved =
        HashMap::with_capacity(intra_block_needed.len() + budgeted_external_outpoints);

    for (block_hash, txid, tx_idx, out_idx) in intra_block_needed {
        if initial.facts.block_header.block_hash == block_hash {
            insert_prevout_from_facts(&initial.facts, txid, tx_idx, out_idx, &mut resolved);
        }
    }

    for height_chunk in external_heights.chunks(PARENT_REPLAY_SUBBATCH_HEIGHTS) {
        let guard = canonical_store.read();
        resolve_locked_canonical_epoch(&guard, Some(initial.epoch.id.value()))?;
        guard
            .visit_block_replay_facts_at_heights(height_chunk, |idx, maybe_parent_facts| {
                let Some(parent_facts) = maybe_parent_facts else {
                    return Ok(());
                };
                if let Some(height_outpoints) = external_by_height.get(&height_chunk[idx]) {
                    for &(block_hash, txid, tx_idx, out_idx) in height_outpoints {
                        if parent_facts.block_header.block_hash == block_hash {
                            insert_prevout_from_facts(
                                &parent_facts,
                                txid,
                                tx_idx,
                                out_idx,
                                &mut resolved,
                            );
                        }
                    }
                }
                Ok::<(), CanonicalReplayVisitError>(())
            })
            .map_err(Status::from)?;
        drop(guard);
    }

    Ok((resolved, parent_budget_exhausted))
}

type NeededOutpoint = (BlockHash, TransactionId, u32, u32);
type ExternalOutpointsByHeight = BTreeMap<BlockHeight, Vec<NeededOutpoint>>;

fn partition_needed_prevouts(
    transactions: &[CanonicalTransactionFacts],
    location_by_txid: &HashMap<TransactionId, zinder_core::TransactionLocation>,
    current_height: BlockHeight,
    visible_tip_height: BlockHeight,
    first_retained: BlockHeight,
) -> (Vec<NeededOutpoint>, ExternalOutpointsByHeight) {
    let mut intra_block_needed = Vec::new();
    let mut external_by_height: ExternalOutpointsByHeight = BTreeMap::new();
    for tx in transactions {
        for input in &tx.transparent_inputs {
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let Some(loc) = location_by_txid.get(&input.spent_outpoint.transaction_id) else {
                continue;
            };
            if loc.block_height > visible_tip_height || loc.block_height < first_retained {
                continue;
            }
            let entry = (
                loc.block_hash,
                loc.transaction_id,
                loc.tx_index_in_block,
                input.spent_outpoint.output_index,
            );
            if loc.block_height == current_height {
                intra_block_needed.push(entry);
            } else {
                external_by_height
                    .entry(loc.block_height)
                    .or_default()
                    .push(entry);
            }
        }
    }
    (intra_block_needed, external_by_height)
}

fn collect_unique_parent_txids(transactions: &[CanonicalTransactionFacts]) -> Vec<TransactionId> {
    let mut txids = Vec::new();
    for tx in transactions {
        for input in &tx.transparent_inputs {
            if !input.spent_outpoint.is_coinbase_sentinel() {
                txids.push(input.spent_outpoint.transaction_id);
            }
        }
    }
    txids.sort_unstable_by_key(|txid| encode_rpc_transaction_id_hex(*txid));
    txids.dedup();
    txids
}

fn insert_prevout_from_facts(
    parent_facts: &CanonicalBlockFacts,
    txid: TransactionId,
    tx_idx: u32,
    out_idx: u32,
    resolved: &mut HashMap<TransparentOutPoint, ResolvedPrevout>,
) {
    let tx_opt = usize::try_from(tx_idx)
        .ok()
        .and_then(|pos| parent_facts.transactions.get(pos))
        .filter(|tx| tx.public_facts.transaction_id == txid)
        .or_else(|| {
            parent_facts
                .transactions
                .iter()
                .find(|tx| tx.public_facts.transaction_id == txid)
        });
    let Some(parent_tx) = tx_opt else {
        return;
    };
    let out_opt = usize::try_from(out_idx)
        .ok()
        .and_then(|pos| parent_tx.transparent_outputs.get(pos))
        .filter(|out| out.output_index == out_idx)
        .or_else(|| {
            parent_tx
                .transparent_outputs
                .iter()
                .find(|out| out.output_index == out_idx)
        });
    if let Some(output) = out_opt {
        resolved.insert(
            TransparentOutPoint::new(txid, out_idx),
            (output.value_zat, output.script_pub_key.clone()),
        );
    }
}

fn encode_block_transactions(
    transactions: &[CanonicalTransactionFacts],
    prevouts: &HashMap<TransparentOutPoint, ResolvedPrevout>,
) -> Result<Vec<BlockTransaction>, Status> {
    let mut rows = Vec::with_capacity(transactions.len());
    for (idx, tx) in transactions.iter().enumerate() {
        let transaction_index = u32::try_from(idx)
            .map_err(|_| ExplorerError::internal("block transaction index exceeds u32"))?;
        let transparent_outputs = tx
            .transparent_outputs
            .iter()
            .map(|output| wallet::TransparentOutput {
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect();
        let transparent_inputs = tx
            .transparent_inputs
            .iter()
            .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
            .map(|input| {
                let spent = prevouts.get(&input.spent_outpoint);
                TransparentInput {
                    input_index: input.input_index,
                    spent_outpoint: Some(wallet::OutPoint {
                        transaction_id: encode_rpc_transaction_id_hex(
                            input.spent_outpoint.transaction_id,
                        ),
                        output_index: input.spent_outpoint.output_index,
                    }),
                    value_zat: spent.map(|(zat_amount, _)| *zat_amount),
                    script_pub_key: spent.map(|(_, script)| script.clone()),
                }
            })
            .collect();
        rows.push(BlockTransaction {
            transaction_index,
            transaction_id: encode_rpc_transaction_id_hex(tx.public_facts.transaction_id),
            public_facts: Some(encode_public_facts(&tx.public_facts)),
            transparent_outputs,
            transparent_inputs,
        });
    }
    Ok(rows)
}

fn read_block_final_roots(
    secondary: &RocksDbCanonicalSecondary,
    height: BlockHeight,
    fallback_activations: Option<&NetworkUpgradeActivations>,
) -> Result<Option<WireFinalRoots>, Status> {
    let Some(activations) = secondary
        .network_upgrade_activations()
        .or(fallback_activations)
    else {
        return Ok(None);
    };
    let range = BlockHeightRange::inclusive(height, height);
    let roots = match secondary.stored_final_note_commitment_roots_in_range(range, activations) {
        Ok(entries) => entries.into_iter().next().flatten(),
        Err(CanonicalStoreError::AdmissionRefused { .. }) => return Ok(None),
        Err(error) => return Err(internal_canonical_error(error)),
    };
    Ok(roots.map(encode_final_roots))
}

fn encode_final_roots(roots: CoreFinalRoots) -> WireFinalRoots {
    WireFinalRoots {
        sapling: roots.sapling.map(|root| root.as_bytes().to_vec()),
        orchard: roots.orchard.map(|root| root.as_bytes().to_vec()),
        ironwood: roots.ironwood.map(|root| root.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::sync::Arc;

    use parking_lot::RwLock;
    use prost::Message as _;
    use tempfile::TempDir;
    use zinder_core::{
        LockTime, Network, PrivacyShape, TransactionComponentCounts, TransactionLocation,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentOutPoint, TransparentSpendFact, TransparentUnspentOutput,
        wire::encode_rpc_block_hash_hex,
    };
    use zinder_materialized_views::{
        BlockSummaryConsumer, MaterializedViewStore, MaterializedViewStoreOptions,
    };
    use zinder_proto::v1::explorer::BlockSummaryRecord;
    use zinder_store::RocksDbResourceBudget;
    use zinder_testkit::{
        ChainFixture, FixtureTransactionRows, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    use super::*;

    struct ManyParentTransactionsFixture {
        _store_guard: WalletServingStoreFixture,
        _mv_dir: TempDir,
        canonical_store: SharedCanonicalSecondary,
        mv_store: MaterializedViewStore,
        target_height: BlockHeight,
    }

    fn supported_tx_facts(txid: TransactionId, is_coinbase: bool) -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id: txid,
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

    fn build_many_parent_fixture()
    -> Result<ManyParentTransactionsFixture, Box<dyn std::error::Error>> {
        let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
        let blocks = chain.blocks().to_vec();
        let script = vec![0x76, 0xa9, 0x14];
        let script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);

        for (idx, block) in blocks.iter().enumerate() {
            let cb_tag = u8::try_from(idx + 1)?;
            let coinbase_txid = TransactionId::from_bytes([cb_tag; 32]);
            chain = chain
                .with_transaction_rows(FixtureTransactionRows::from_public_facts(
                    TransactionLocation::new(coinbase_txid, block.height, block.hash, 0),
                    supported_tx_facts(coinbase_txid, true),
                ))
                .with_address_output_index(TransparentUnspentOutput::new(
                    script_hash,
                    script.clone(),
                    TransparentOutPoint::new(coinbase_txid, 0),
                    100_000,
                    block.height,
                    block.hash,
                ));
        }

        let target_block = &blocks[4];
        for (parent_idx, parent_block) in blocks.iter().take(4).enumerate() {
            chain = append_parent_spend(
                chain,
                parent_block,
                target_block,
                parent_idx,
                (&script, script_hash),
            )?;
        }

        let mut store_guard =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let (canonical_reader, _wallet_reader) = store_guard.take_readers()?;
        let (mv_dir, mv_store) = open_test_summary_mv_store(target_block)?;

        Ok(ManyParentTransactionsFixture {
            _store_guard: store_guard,
            _mv_dir: mv_dir,
            canonical_store: Arc::new(RwLock::new(canonical_reader)),
            mv_store,
            target_height: target_block.height,
        })
    }

    fn append_parent_spend(
        chain: ChainFixture,
        parent_block: &zinder_testkit::FixtureBlock,
        target_block: &zinder_testkit::FixtureBlock,
        parent_idx: usize,
        script_parts: (&[u8], TransparentAddressScriptHash),
    ) -> Result<ChainFixture, Box<dyn std::error::Error>> {
        let (script, script_hash) = script_parts;
        let prev_txid = TransactionId::from_bytes([u8::try_from(parent_idx + 1)?; 32]);
        let tx_index = u32::try_from(parent_idx + 1)?;
        let spend_txid = TransactionId::from_bytes([u8::try_from(0x50 + parent_idx + 1)?; 32]);
        Ok(chain
            .with_transaction_rows(FixtureTransactionRows::from_public_facts(
                TransactionLocation::new(
                    spend_txid,
                    target_block.height,
                    target_block.hash,
                    tx_index,
                ),
                supported_tx_facts(spend_txid, false),
            ))
            .with_transparent_spend_fact(TransparentSpendFact::new(
                TransparentOutPoint::new(prev_txid, 0),
                0,
                spend_txid,
                tx_index,
                target_block.height,
                target_block.hash,
                100_000,
                script_hash,
                parent_block.height,
                parent_block.hash,
            ))
            .with_address_output_index(TransparentUnspentOutput::new(
                script_hash,
                script.to_vec(),
                TransparentOutPoint::new(spend_txid, 0),
                95_000,
                target_block.height,
                target_block.hash,
            )))
    }

    fn open_test_summary_mv_store(
        target_block: &zinder_testkit::FixtureBlock,
    ) -> Result<(TempDir, MaterializedViewStore), Box<dyn std::error::Error>> {
        let mv_dir = TempDir::new()?;
        let mv_primary_path = mv_dir.path().join("mv-primary");
        let mv_secondary_path = mv_dir.path().join("mv-secondary");
        let mv_opts = || MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        };
        let mv_primary = MaterializedViewStore::open(&mv_primary_path, mv_opts())?;
        let record = BlockSummaryRecord {
            summary: Some(BlockSummary {
                block_height: target_block.height.value(),
                block_hash: encode_rpc_block_hash_hex(target_block.hash),
                previous_block_hash: encode_rpc_block_hash_hex(target_block.parent_hash),
                block_time_unix_seconds: i64::from(target_block.block_time_seconds),
                transaction_count: 5,
                total_size_bytes: u64::try_from(target_block.raw_block_bytes.len())?,
                confirmations: 1,
                is_canonical: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        mv_primary.put_consumer(
            BLOCK_SUMMARY_COLUMN_FAMILY,
            &BlockSummaryConsumer::key_for_height(target_block.height),
            &record.encode_to_vec(),
        )?;
        let mv_store =
            MaterializedViewStore::open_secondary(&mv_primary_path, &mv_secondary_path, mv_opts())?;
        mv_store.try_catch_up()?;
        Ok((mv_dir, mv_store))
    }

    #[test]
    fn many_parent_block_bounds_parent_replays_and_omits_unresolved_prevouts_and_paid_fees()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = build_many_parent_fixture()?;
        let selector = ParsedBlockSelector::Height(fixture.target_height);

        let bounded = read_block_transactions_with_budget(
            &fixture.canonical_store,
            &fixture.mv_store,
            &BlockTransactionsQuery {
                at_epoch_id: None,
                selector: &selector,
                fallback_activations: None,
                max_parent_replays: 2,
            },
        )?;
        assert_eq!(bounded.summary.paid_fees_collected_zat, None);
        assert_eq!(bounded.transactions.len(), 5);
        assert_eq!(
            bounded.transactions[1].transparent_inputs[0].value_zat,
            Some(100_000)
        );
        assert!(
            bounded.transactions[1].transparent_inputs[0]
                .script_pub_key
                .is_some()
        );
        assert_eq!(
            bounded.transactions[2].transparent_inputs[0].value_zat,
            Some(100_000)
        );
        assert!(
            bounded.transactions[2].transparent_inputs[0]
                .script_pub_key
                .is_some()
        );
        assert_eq!(
            bounded.transactions[3].transparent_inputs[0].value_zat,
            None
        );
        assert_eq!(
            bounded.transactions[3].transparent_inputs[0].script_pub_key,
            None
        );
        assert_eq!(
            bounded.transactions[4].transparent_inputs[0].value_zat,
            None
        );
        assert_eq!(
            bounded.transactions[4].transparent_inputs[0].script_pub_key,
            None
        );

        let unbounded = read_block_transactions_with_budget(
            &fixture.canonical_store,
            &fixture.mv_store,
            &BlockTransactionsQuery {
                at_epoch_id: None,
                selector: &selector,
                fallback_activations: None,
                max_parent_replays: 4,
            },
        )?;
        assert_eq!(unbounded.summary.paid_fees_collected_zat, Some(20_000));
        for tx_row in unbounded.transactions.iter().skip(1) {
            assert_eq!(tx_row.transparent_inputs[0].value_zat, Some(100_000));
            assert!(tx_row.transparent_inputs[0].script_pub_key.is_some());
        }
        Ok(())
    }
}
