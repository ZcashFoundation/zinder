//! `ExplorerQuery.BlockTransactions` handler.

use std::collections::HashMap;

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockFinalNoteCommitmentRoots as CoreFinalRoots, BlockHash, BlockHeight, BlockHeightRange,
    CanonicalBlockFacts, CanonicalTransactionFacts, ChainEpoch, NetworkUpgradeActivations,
    TransactionId, TransparentOutPoint,
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

use super::canonical_read::{
    CanonicalReplayVisitError, SharedCanonicalSecondary, internal_canonical_error,
    join_block_summary_with_header, read_with_pinned_canonical_epoch,
};
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use super::transaction_detail::encode_public_facts;

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

    let outcome = read_with_pinned_canonical_epoch(
        context.canonical_store,
        inner.at_epoch_id,
        move |secondary, epoch| {
            read_block_transactions_sync(
                secondary,
                &mv_store,
                epoch,
                &selector,
                fallback_activations.as_ref(),
            )
        },
    )
    .await?;

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

fn read_block_transactions_sync(
    secondary: &RocksDbCanonicalSecondary,
    mv_store: &MaterializedViewStore,
    epoch: ChainEpoch,
    selector: &ParsedBlockSelector,
    fallback_activations: Option<&NetworkUpgradeActivations>,
) -> Result<BlockTransactionsOutcome, Status> {
    let height = resolve_canonical_height(secondary, epoch, selector)?;
    let facts = secondary
        .block_replay_facts_at(height)
        .map_err(internal_canonical_error)?
        .ok_or_else(|| {
            ExplorerError::not_materialized(format!(
                "canonical replay facts are unavailable at height {}",
                height.value(),
            ))
        })?;
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
    let summary = join_block_summary_with_header(
        &summary_payload,
        &facts.block_header,
        epoch.visible_tip_height.value(),
    )?
    .ok_or_else(|| {
        ExplorerError::unsatisfied_precondition(format!(
            "materialized BlockSummary at height {} disagrees with canonical block header",
            height.value(),
        ))
    })?;

    let prevouts = resolve_block_prevouts(secondary, epoch, &facts)?;
    let transactions = encode_block_transactions(&facts.transactions, &prevouts)?;
    let final_roots = read_block_final_roots(secondary, height, fallback_activations)?;

    Ok(BlockTransactionsOutcome {
        epoch,
        summary,
        transactions,
        final_roots,
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

fn resolve_block_prevouts(
    secondary: &RocksDbCanonicalSecondary,
    epoch: ChainEpoch,
    block_facts: &CanonicalBlockFacts,
) -> Result<HashMap<TransparentOutPoint, ResolvedPrevout>, Status> {
    let parent_txids = collect_unique_parent_txids(&block_facts.transactions);
    if parent_txids.is_empty() {
        return Ok(HashMap::new());
    }
    let locations = secondary
        .transaction_locations(&parent_txids)
        .map_err(internal_canonical_error)?;

    let mut needed_outpoints: Vec<(BlockHeight, BlockHash, TransactionId, u32, u32)> = Vec::new();
    let current_height = block_facts.block_header.height;
    let first_retained = secondary.history_bounds().first_available_height();
    for tx in &block_facts.transactions {
        for input in &tx.transparent_inputs {
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let Some(pos) = parent_txids
                .iter()
                .position(|txid| *txid == input.spent_outpoint.transaction_id)
            else {
                continue;
            };
            let Some(Some(loc)) = locations.get(pos) else {
                continue;
            };
            if loc.block_height <= epoch.visible_tip_height && loc.block_height >= first_retained {
                needed_outpoints.push((
                    loc.block_height,
                    loc.block_hash,
                    loc.transaction_id,
                    loc.tx_index_in_block,
                    input.spent_outpoint.output_index,
                ));
            }
        }
    }

    let mut resolved = HashMap::with_capacity(needed_outpoints.len());
    let mut external_heights: Vec<BlockHeight> = needed_outpoints
        .iter()
        .filter_map(|(height, _, _, _, _)| (*height != current_height).then_some(*height))
        .collect();
    external_heights.sort_unstable();
    external_heights.dedup();

    for &(height, block_hash, txid, tx_idx, out_idx) in &needed_outpoints {
        if height == current_height && block_facts.block_header.block_hash == block_hash {
            insert_prevout_from_facts(block_facts, txid, tx_idx, out_idx, &mut resolved);
        }
    }

    if !external_heights.is_empty() {
        secondary
            .visit_block_replay_facts_at_heights(&external_heights, |idx, maybe_parent_facts| {
                let Some(parent_facts) = maybe_parent_facts else {
                    return Ok(());
                };
                let target_height = external_heights[idx];
                for &(height, block_hash, txid, tx_idx, out_idx) in &needed_outpoints {
                    if height == target_height && parent_facts.block_header.block_hash == block_hash
                    {
                        insert_prevout_from_facts(
                            &parent_facts,
                            txid,
                            tx_idx,
                            out_idx,
                            &mut resolved,
                        );
                    }
                }
                Ok::<(), CanonicalReplayVisitError>(())
            })
            .map_err(Status::from)?;
    }

    Ok(resolved)
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
