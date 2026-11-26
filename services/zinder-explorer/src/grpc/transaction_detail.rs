//! `ExplorerQuery.TransactionDetail` handler.
//!
//! Reads one transaction through `WalletQuery.Transaction` and surfaces the
//! status location alongside
//! the cross-cutting [`ExplorerFreshness`] envelope. The handler owns the
//! conversion between the `zinder_core::TransactionPublicFacts` shape and
//! its proto mirror; the source-side parser is the single source of truth
//! for everything else.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, ChainEpochId, ConsensusBranchId, LockTime as CoreLockTime,
    NetworkUpgradeActivations, TransactionId, TransactionPublicFacts as CoreFacts,
    TransactionVersion as CoreTransactionVersion, TransparentOutPoint, TransparentOutputFact,
    explorer_reasons::TRANSPARENT_OUTPUT_SPEND_STATUS_NOT_INDEXED,
    wire::{
        decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, encode_branch_id_hex,
        encode_rpc_auth_digest_hex, encode_rpc_transaction_id_hex, encode_rpc_wtxid_hex,
    },
};
use zinder_proto::capabilities::EXPLORER_TRANSACTION_DETAIL_V4;
use zinder_proto::wire::{TRANSPARENT_OUTPUT_SPENT_BY_FIELD_PATH, encode_privacy_shape};

use zinder_materialized_views::{
    MaterializedViewState, MaterializedViewStore, TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME,
    TransactionFeesConsumer, TransparentOutpointSpendConsumer,
};
use zinder_proto::v1::{
    explorer::{
        LockTime as WireLockTime, LockTimeUnlocked, TransactionComponentCounts,
        TransactionDetailRequest, TransactionDetailResponse, TransactionFeesRecord,
        TransactionIntrinsicValueBalances as WireIntrinsicValueBalances,
        TransactionPublicFacts as WireFacts, TransactionVersion as WireVersion,
        TransactionVersionKind, TransparentOutput as WireTransparentOutput, UnavailableField,
        UnavailableReason, lock_time as wire_lock_time,
    },
    wallet::{
        self, TransactionLocation as WireTransactionLocation,
        transaction_location as wire_location, wallet_query_client::WalletQueryClient,
    },
};
use zinder_runtime::AuthenticatedChannel;
use zinder_source::TransactionPublicFactSet;
use zinder_store::{CanonicalStoreError, chain_epoch_from_message, transparent_spend_message};

use super::canonical_read::{
    SharedCanonicalSecondary, encode_intrinsic_value_balances, internal_canonical_error,
};
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use super::transparent_input::encode_unresolved_transparent_inputs;

/// Read backends the `TransactionDetail` handler needs from the adapter.
///
/// Bundled into one struct so the handler signature stays under the
/// workspace's clippy `too-many-arguments` threshold and so adding a new
/// shared dependency does not ripple through every call site.
pub(crate) struct TransactionDetailContext<'context> {
    pub(crate) canonical_store: Option<&'context SharedCanonicalSecondary>,
    pub(crate) materialized_view_store: Option<&'context MaterializedViewStore>,
    pub(crate) network_upgrade_activations: &'context NetworkUpgradeActivations,
    pub(crate) upstream_observation_cache: &'context UpstreamObservationCache,
}

/// Executes one `ExplorerQuery.TransactionDetail` request.
pub(crate) async fn query_transaction_detail(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    context: TransactionDetailContext<'_>,
    request: Request<TransactionDetailRequest>,
) -> Result<Response<TransactionDetailResponse>, Status> {
    let TransactionDetailContext {
        canonical_store,
        materialized_view_store,
        network_upgrade_activations,
        upstream_observation_cache,
    } = context;
    let inner = request.into_inner();
    let transaction_id = decode_rpc_transaction_id_hex(&inner.transaction_id)
        .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;

    let status_response = wallet_client
        .transaction(Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            at_epoch_id: inner.at_epoch_id,
        }))
        .await?
        .into_inner();
    let chain_epoch = status_response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| ExplorerError::internal("WalletQuery.Transaction missing chain_epoch"))?;
    let expected_epoch = chain_epoch_from_message(chain_epoch.clone())
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let location = status_response
        .location
        .and_then(|location| location.location)
        .ok_or_else(|| {
            ExplorerError::internal("WalletQuery.Transaction response missing location")
        })?;
    let mined_block_id = mined_location_block_id(&location)?;

    let mut response = transaction_detail_response(
        materialized_view_store,
        network_upgrade_activations,
        transaction_id,
        chain_epoch,
        location,
    )?;
    if let (Some(canonical_store), Some(block_id)) = (canonical_store, mined_block_id) {
        response.intrinsic_value_balances = resolve_mined_intrinsic_value_balances(
            canonical_store,
            expected_epoch.id,
            transaction_id,
            block_id,
        )
        .await?;
    }
    let freshness = response.freshness.take().unwrap_or_default();
    response.freshness =
        Some(attach_upstream_observation(upstream_observation_cache, freshness).await);
    Ok(Response::new(response))
}

fn mined_location_block_id(location: &wire_location::Location) -> Result<Option<BlockId>, Status> {
    let wire_location::Location::Mined(mined) = location else {
        return Ok(None);
    };
    let Some(block_loc) = mined.location.as_ref() else {
        return Ok(None);
    };
    let block_hash = decode_rpc_block_hash_hex(&block_loc.block_hash)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    Ok(Some(BlockId::new(
        BlockHeight::new(block_loc.block_height),
        block_hash,
    )))
}

async fn resolve_mined_intrinsic_value_balances(
    canonical_store: &SharedCanonicalSecondary,
    epoch_id: ChainEpochId,
    transaction_id: TransactionId,
    block_id: BlockId,
) -> Result<Option<WireIntrinsicValueBalances>, Status> {
    let store = Arc::clone(canonical_store);
    tokio::task::spawn_blocking(move || {
        read_mined_intrinsic_value_balances(&store, epoch_id, transaction_id, block_id)
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("canonical read task failed: {error}")))?
}

fn read_mined_intrinsic_value_balances(
    store: &SharedCanonicalSecondary,
    epoch_id: ChainEpochId,
    transaction_id: TransactionId,
    block_id: BlockId,
) -> Result<Option<WireIntrinsicValueBalances>, Status> {
    if store
        .read()
        .chain_epoch()
        .map_err(internal_canonical_error)?
        .id
        < epoch_id
    {
        store
            .write()
            .try_catch_up()
            .map_err(internal_canonical_error)?;
    }
    let guard = store.read();
    let current_epoch = guard.chain_epoch().map_err(internal_canonical_error)?;
    if current_epoch.id < epoch_id {
        return Ok(None);
    }
    if current_epoch.id != epoch_id {
        let pinned = match guard.chain_epoch_at(epoch_id) {
            Ok(epoch) => epoch,
            Err(CanonicalStoreError::CanonicalEpochNotRetained { .. }) => return Ok(None),
            Err(error) => return Err(internal_canonical_error(error)),
        };
        if pinned.visible_tip_height > BlockHeight::new(0)
            && guard
                .block_header_at(pinned.visible_tip_height)
                .map_err(internal_canonical_error)?
                .is_none_or(|header| header.block_hash != pinned.visible_tip_hash)
        {
            return Ok(None);
        }
    }
    let Some(facts) = guard
        .block_replay_facts_at(block_id.height)
        .map_err(internal_canonical_error)?
    else {
        return Ok(None);
    };
    drop(guard);
    if facts.block_header.block_hash != block_id.hash {
        return Ok(None);
    }
    Ok(facts
        .transactions
        .iter()
        .find(|tx| tx.public_facts.transaction_id == transaction_id)
        .map(|tx| encode_intrinsic_value_balances(tx.intrinsic_value_balances)))
}

/// Builds the detail response for one wallet-plane transaction, before the
/// upstream observation is attached to its freshness envelope.
fn transaction_detail_response(
    materialized_view_store: Option<&MaterializedViewStore>,
    network_upgrade_activations: &NetworkUpgradeActivations,
    transaction_id: TransactionId,
    chain_epoch: wallet::ChainEpoch,
    location: wire_location::Location,
) -> Result<TransactionDetailResponse, Status> {
    let transaction =
        resolve_facts_and_location(network_upgrade_activations, transaction_id, location)?;

    let fees = resolve_fee_record(materialized_view_store, &transaction)?;
    let transparent_rows = resolve_transparent_rows(
        &transaction,
        &chain_epoch,
        fees.as_ref(),
        materialized_view_store,
    )?;
    let mut freshness = build_explorer_freshness(
        materialized_view_store,
        EXPLORER_TRANSACTION_DETAIL_V4,
        Some(chain_epoch),
        0,
    )?;
    if !transparent_rows.output_spends_known {
        freshness
            .unavailable
            .push(transparent_output_spend_status_unavailable());
    }
    let (paid_fee_zat, prevout_resolution_status) = fees.as_ref().map_or((None, 0), |record| {
        (record.paid_fee_zat, record.prevout_resolution_status)
    });
    Ok(TransactionDetailResponse {
        freshness: Some(freshness),
        facts: Some(encode_public_facts(&transaction.facts)),
        location: Some(transaction.location),
        paid_fee_zat,
        prevout_resolution_status,
        transparent_inputs: transparent_rows.inputs,
        transparent_outputs: transparent_rows.outputs,
        intrinsic_value_balances: None,
    })
}

struct ResolvedTransactionDetail {
    facts: CoreFacts,
    location: WireTransactionLocation,
    fact_set: TransactionPublicFactSet,
}

struct ResolvedTransparentRows {
    inputs: Vec<zinder_proto::v1::explorer::TransparentInput>,
    outputs: Vec<WireTransparentOutput>,
    /// `false` when the outputs' `spent_by` relations are unknown rather than
    /// absent because unspent.
    output_spends_known: bool,
}

/// Canonical spend relations of a transaction's transparent outputs, keyed by
/// outpoint, or `None` when the spend view does not cover the response epoch.
type TransparentOutputSpends = Option<HashMap<TransparentOutPoint, wallet::TransparentSpend>>;

fn resolve_transparent_rows(
    transaction: &ResolvedTransactionDetail,
    chain_epoch: &wallet::ChainEpoch,
    fees: Option<&TransactionFeesRecord>,
    materialized_view_store: Option<&MaterializedViewStore>,
) -> Result<ResolvedTransparentRows, Status> {
    let fact_set = &transaction.fact_set;
    let inputs = encode_unresolved_transparent_inputs(&fact_set.transparent_inputs, fees);
    let output_spends =
        resolve_transparent_output_spends(chain_epoch, transaction, materialized_view_store)?;
    let output_spends_known = output_spends.is_some();
    let outputs = encode_transparent_output_facts(
        fact_set.public_facts.transaction_id,
        &fact_set.transparent_outputs,
        &output_spends.unwrap_or_default(),
    );
    Ok(ResolvedTransparentRows {
        inputs,
        outputs,
        output_spends_known,
    })
}

fn transparent_output_spend_status_unavailable() -> UnavailableField {
    UnavailableField {
        field_path: TRANSPARENT_OUTPUT_SPENT_BY_FIELD_PATH.to_owned(),
        reason: UnavailableReason::UnavailableStale as i32,
        human_reason: TRANSPARENT_OUTPUT_SPEND_STATUS_NOT_INDEXED.to_owned(),
    }
}

/// Resolves the parsed public facts and the wire location for one transaction.
///
/// The `location` oneof is carried through verbatim so the explorer detail
/// returns the same `{ mined, in_mempool }` shape the wallet plane answered
/// with. Facts come from the raw transaction bytes the wallet plane returns.
fn resolve_facts_and_location(
    network_upgrade_activations: &NetworkUpgradeActivations,
    transaction_id: zinder_core::TransactionId,
    location: wire_location::Location,
) -> Result<ResolvedTransactionDetail, Status> {
    let (facts, inner, fact_set) = match location {
        wire_location::Location::Mined(mined) => {
            let branch_id = mined_consensus_branch_id(&mined)?;
            let raw_transaction_bytes =
                mined.raw_transaction_bytes.as_deref().ok_or_else(|| {
                    ExplorerError::not_materialized(format!(
                        "raw transaction bytes are not available for {transaction_id:?}"
                    ))
                })?;
            let fact_set = zinder_source::parse_transaction_public_fact_set(
                raw_transaction_bytes,
                Some(BlockHeight::new(
                    mined
                        .location
                        .as_ref()
                        .ok_or_else(|| {
                            ExplorerError::internal("MinedTransaction missing block location")
                        })?
                        .block_height,
                )),
                network_upgrade_activations,
            )
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
            if fact_set.public_facts.transaction_id != transaction_id {
                return Err(ExplorerError::internal(
                    "WalletQuery.Transaction mined payload transaction id mismatch",
                )
                .into());
            }
            let mut facts = fact_set.public_facts.clone();
            facts.consensus_branch_id = Some(branch_id);
            (facts, wire_location::Location::Mined(mined), fact_set)
        }
        wire_location::Location::InMempool(mempool) => {
            let fact_set = zinder_source::parse_transaction_public_fact_set(
                &mempool.raw_transaction_bytes,
                None,
                network_upgrade_activations,
            )
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
            if fact_set.public_facts.transaction_id != transaction_id {
                return Err(ExplorerError::internal(
                    "WalletQuery.Transaction mempool payload transaction id mismatch",
                )
                .into());
            }
            (
                fact_set.public_facts.clone(),
                wire_location::Location::InMempool(mempool),
                fact_set,
            )
        }
    };
    Ok(ResolvedTransactionDetail {
        facts,
        location: WireTransactionLocation {
            location: Some(inner),
        },
        fact_set,
    })
}

fn resolve_fee_record(
    materialized_view_store: Option<&MaterializedViewStore>,
    transaction: &ResolvedTransactionDetail,
) -> Result<Option<TransactionFeesRecord>, Status> {
    if transaction.facts.is_coinbase {
        return Ok(None);
    }
    let Some(materialized_view_store) = materialized_view_store else {
        return Ok(None);
    };

    TransactionFeesConsumer::read_fees_record(
        materialized_view_store,
        transaction.facts.transaction_id,
        transaction.facts.privacy_shape,
    )
    .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn resolve_transparent_output_spends(
    chain_epoch: &wallet::ChainEpoch,
    transaction: &ResolvedTransactionDetail,
    materialized_view_store: Option<&MaterializedViewStore>,
) -> Result<TransparentOutputSpends, Status> {
    if matches!(
        transaction.location.location,
        Some(wire_location::Location::InMempool(_))
    ) {
        return Ok(Some(HashMap::new()));
    }
    let requested_outpoints: Vec<TransparentOutPoint> = transaction
        .fact_set
        .transparent_outputs
        .iter()
        .map(|output| {
            TransparentOutPoint::new(transaction.facts.transaction_id, output.output_index)
        })
        .collect();
    if requested_outpoints.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let requested_outpoint_set: HashSet<TransparentOutPoint> =
        requested_outpoints.iter().copied().collect();
    if requested_outpoint_set.len() != requested_outpoints.len() {
        return Err(ExplorerError::internal(
            "TransactionDetail transaction artifact contains duplicate transparent output indexes",
        )
        .into());
    }

    let materialized_view_store = materialized_view_store.ok_or_else(|| {
        ExplorerError::dependency_not_configured(
            "TransactionDetail requires the explorer materialized-view store",
        )
    })?;
    resolve_materialized_transparent_output_spends(
        materialized_view_store,
        chain_epoch,
        transaction,
        &requested_outpoints,
    )
}

fn resolve_materialized_transparent_output_spends(
    materialized_view_store: &MaterializedViewStore,
    chain_epoch: &wallet::ChainEpoch,
    transaction: &ResolvedTransactionDetail,
    requested_outpoints: &[TransparentOutPoint],
) -> Result<TransparentOutputSpends, Status> {
    materialized_view_store
        .try_catch_up()
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let expected_epoch = chain_epoch_from_message(chain_epoch.clone())
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mined_height = transaction
        .location
        .location
        .as_ref()
        .and_then(|location| match location {
            wire_location::Location::Mined(mined) => mined
                .location
                .as_ref()
                .map(|location| location.block_height),
            wire_location::Location::InMempool(_) => None,
        })
        .map(BlockHeight::new)
        .ok_or_else(|| ExplorerError::internal("MinedTransaction missing block location"))?;
    let spends = {
        let snapshot = materialized_view_store.read_snapshot();
        let state = snapshot
            .consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        if !state.is_some_and(|state| spend_view_covers(&state, &expected_epoch, mined_height)) {
            return Ok(None);
        }
        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints_snapshot(
            &snapshot,
            requested_outpoints,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
        drop(snapshot);
        spends
    };
    Ok(Some(
        spends
            .into_iter()
            .map(|(outpoint, spend)| (outpoint, transparent_spend_message(&spend)))
            .collect(),
    ))
}

/// Returns whether the spend view has recorded every spend of an output mined
/// at `mined_height`, through exactly the epoch's visible tip.
///
/// Only then does an absent spend row mean unspent at that epoch. A view that
/// trails, leads, or sits on another branch than the epoch answers nothing.
fn spend_view_covers(
    state: &MaterializedViewState,
    epoch: &ChainEpoch,
    mined_height: BlockHeight,
) -> bool {
    let state_matches_epoch = state.chain_epoch_id == epoch.id
        && state.tip_height == epoch.visible_tip_height
        && state.tip_hash == epoch.visible_tip_hash;
    state_matches_epoch
        && state.coverage.is_some_and(|coverage| {
            coverage.complete_from_height <= mined_height
                && coverage.complete_through_height >= epoch.visible_tip_height
                && coverage.complete_through_hash == epoch.visible_tip_hash
        })
}

fn encode_transparent_output_facts(
    transaction_id: TransactionId,
    outputs: &[TransparentOutputFact],
    spends: &HashMap<TransparentOutPoint, wallet::TransparentSpend>,
) -> Vec<WireTransparentOutput> {
    outputs
        .iter()
        .map(|output| {
            let outpoint = TransparentOutPoint::new(transaction_id, output.output_index);
            WireTransparentOutput {
                output_index: output.output_index,
                output: Some(wallet::TransparentOutput {
                    value_zat: output.value_zat,
                    script_pub_key: output.script_pub_key.clone(),
                }),
                spent_by: spends.get(&outpoint).cloned(),
            }
        })
        .collect()
}

fn mined_consensus_branch_id(
    mined: &wallet::MinedTransaction,
) -> Result<ConsensusBranchId, Status> {
    let chain_context = mined
        .chain_context
        .as_ref()
        .ok_or_else(|| ExplorerError::internal("MinedTransaction missing chain context"))?;
    Ok(ConsensusBranchId::new(chain_context.consensus_branch_id))
}

pub(crate) fn encode_public_facts(facts: &CoreFacts) -> WireFacts {
    WireFacts {
        transaction_id: encode_rpc_transaction_id_hex(facts.transaction_id),
        auth_digest: facts.auth_digest.map_or_default(encode_rpc_auth_digest_hex),
        wtxid: facts.wtxid.map_or_default(encode_rpc_wtxid_hex),
        version: Some(encode_transaction_version(facts.version)),
        consensus_branch_id_hex: facts
            .consensus_branch_id
            .map_or_default(encode_branch_id_hex),
        lock_time: Some(encode_lock_time(facts.lock_time)),
        expiry_height: facts.expiry_height.map_or(0, BlockHeight::value),
        size_bytes: facts.size_bytes,
        counts: Some(encode_component_counts(facts.counts)),
        privacy_shape: encode_privacy_shape(facts.privacy_shape) as i32,
        is_coinbase: facts.is_coinbase,
    }
}

/// Encodes the public component counts shared by transaction and mempool rows.
pub(crate) fn encode_component_counts(
    counts: zinder_core::TransactionComponentCounts,
) -> TransactionComponentCounts {
    TransactionComponentCounts {
        transparent_input_count: counts.transparent_input_count,
        transparent_output_count: counts.transparent_output_count,
        sapling_spend_count: counts.sapling_spend_count,
        sapling_output_count: counts.sapling_output_count,
        orchard_action_count: counts.orchard_action_count,
        ironwood_action_count: counts.ironwood_action_count,
        sprout_joinsplit_count: counts.sprout_joinsplit_count,
    }
}

fn encode_transaction_version(version: CoreTransactionVersion) -> WireVersion {
    let (kind, version_group_id) = match version {
        CoreTransactionVersion::V1 => (TransactionVersionKind::V1, None),
        CoreTransactionVersion::V2 => (TransactionVersionKind::V2, None),
        CoreTransactionVersion::V3 => (TransactionVersionKind::V3, None),
        CoreTransactionVersion::V4 => (TransactionVersionKind::V4, None),
        CoreTransactionVersion::V5 => (TransactionVersionKind::V5, None),
        CoreTransactionVersion::V6 => (TransactionVersionKind::V6, None),
        CoreTransactionVersion::Unsupported {
            version_group_id, ..
        } => (TransactionVersionKind::Unsupported, version_group_id),
    };
    WireVersion {
        kind: kind as i32,
        effective_version: version.effective_version(),
        version_group_id,
    }
}

fn encode_lock_time(lock_time: CoreLockTime) -> WireLockTime {
    let kind = match lock_time {
        CoreLockTime::Unlocked => wire_lock_time::Kind::Unlocked(LockTimeUnlocked {}),
        CoreLockTime::Height(height) => wire_lock_time::Kind::Height(height.value()),
        CoreLockTime::UnixSeconds(seconds) => wire_lock_time::Kind::UnixSeconds(seconds),
    };
    WireLockTime { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, ChainEpochId, ChainTipMetadata, Network, TransactionId,
        TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
        TransparentSpendFact, UnixTimestampMillis,
    };
    use zinder_materialized_views::{MaterializedViewCoverage, MaterializedViewStoreOptions};
    use zinder_proto::v1::explorer::TransparentInputValueRecord;
    use zinder_store::{RocksDbResourceBudget, chain_epoch_message};
    use zinder_testkit::seed_transparent_outpoint_spends;

    use super::*;

    #[test]
    fn spend_view_covers_only_the_exact_epoch_from_the_mined_height() {
        let epoch = chain_epoch_at(100);
        let current = spend_view_state(100, 1, 100);
        assert!(spend_view_covers(&current, &epoch, BlockHeight::new(50)));
        assert!(
            !spend_view_covers(&current, &epoch, BlockHeight::new(0)),
            "an output mined below the verified coverage has unrecorded spends"
        );

        let trailing = spend_view_state(99, 1, 99);
        assert!(!spend_view_covers(&trailing, &epoch, BlockHeight::new(50)));
        let leading = spend_view_state(101, 1, 101);
        assert!(!spend_view_covers(&leading, &epoch, BlockHeight::new(50)));
        let replaying = spend_view_state(100, 1, 40);
        assert!(!spend_view_covers(&replaying, &epoch, BlockHeight::new(50)));
        let other_branch = MaterializedViewState {
            tip_hash: block_hash(7),
            ..current
        };
        assert!(!spend_view_covers(
            &other_branch,
            &epoch,
            BlockHeight::new(50)
        ));
        let unverified = MaterializedViewState {
            coverage: None,
            ..current
        };
        assert!(!spend_view_covers(
            &unverified,
            &epoch,
            BlockHeight::new(50)
        ));
    }

    #[test]
    fn mined_outputs_are_served_with_unknown_spends_before_the_spend_view_covers_the_epoch()
    -> eyre::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let materialized_view_store = open_spend_view_store(tempdir.path())?;
        let transaction = resolve_mined_transparent_transaction()?;
        let chain_epoch = chain_epoch_message(chain_epoch_at(1));

        let rows = resolve_transparent_rows(
            &transaction,
            &chain_epoch,
            None,
            Some(&materialized_view_store),
        )?;

        assert!(!rows.output_spends_known);
        assert_eq!(rows.inputs.len(), 1);
        assert_eq!(rows.outputs.len(), 2);
        assert!(rows.outputs.iter().all(|output| output.spent_by.is_none()));
        Ok(())
    }

    #[test]
    fn unknown_output_spend_status_names_the_spent_by_field_as_stale() {
        let unavailable = transparent_output_spend_status_unavailable();

        assert_eq!(unavailable.field_path, "transparent_outputs.spent_by");
        assert_eq!(
            unavailable.reason,
            UnavailableReason::UnavailableStale as i32
        );
        assert_eq!(
            unavailable.human_reason,
            TRANSPARENT_OUTPUT_SPEND_STATUS_NOT_INDEXED
        );
    }

    #[test]
    fn detail_withholds_recorded_spends_while_the_spend_view_misses_the_epoch() -> eyre::Result<()>
    {
        let not_started = None;
        let trailing = Some(spend_view_state(1, 1, 1));
        let leading = Some(spend_view_state(3, 1, 3));
        for spend_view in [not_started, trailing, leading] {
            let tempdir = tempfile::tempdir()?;
            let materialized_view_store = open_spend_view_store(tempdir.path())?;
            if let Some(state) = spend_view {
                materialized_view_store
                    .put_consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, state)?;
            }
            let (transaction_id, location) = mined_transparent_location()?;
            seed_transparent_outpoint_spends(
                &materialized_view_store,
                &[spend_of_first_output(transaction_id)],
            )?;

            let response = transaction_detail_response(
                Some(&materialized_view_store),
                &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
                transaction_id,
                chain_epoch_message(chain_epoch_at(2)),
                location,
            )?;

            assert!(response.facts.is_some());
            assert!(response.location.is_some());
            assert_eq!(response.transparent_inputs.len(), 1);
            assert_eq!(response.transparent_outputs.len(), 2);
            assert!(
                response
                    .transparent_outputs
                    .iter()
                    .all(|output| output.spent_by.is_none()),
                "{spend_view:?}: a spend view off the epoch must not report spend relations"
            );
            assert_eq!(
                spent_by_markers(&response),
                vec![transparent_output_spend_status_unavailable()],
                "{spend_view:?}: unknown spend status must be marked stale"
            );
        }
        Ok(())
    }

    #[test]
    fn detail_reports_spent_and_unspent_outputs_when_the_spend_view_covers_the_epoch()
    -> eyre::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let materialized_view_store = open_spend_view_store(tempdir.path())?;
        materialized_view_store.put_consumer_state(
            TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME,
            spend_view_state(2, 1, 2),
        )?;
        let (transaction_id, location) = mined_transparent_location()?;
        seed_transparent_outpoint_spends(
            &materialized_view_store,
            &[spend_of_first_output(transaction_id)],
        )?;

        let response = transaction_detail_response(
            Some(&materialized_view_store),
            &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
            transaction_id,
            chain_epoch_message(chain_epoch_at(2)),
            location,
        )?;

        assert_eq!(response.transparent_outputs.len(), 2);
        assert_eq!(
            response.transparent_outputs[0]
                .spent_by
                .as_ref()
                .map(|spend| spend.spending_transaction_id.clone()),
            Some(encode_rpc_transaction_id_hex(spending_transaction_id()))
        );
        assert!(response.transparent_outputs[1].spent_by.is_none());
        assert_eq!(
            spent_by_markers(&response),
            [] as [zinder_proto::v1::explorer::UnavailableField; 0]
        );
        Ok(())
    }

    fn spent_by_markers(response: &TransactionDetailResponse) -> Vec<UnavailableField> {
        response
            .freshness
            .iter()
            .flat_map(|freshness| freshness.unavailable.iter())
            .filter(|unavailable| unavailable.field_path == TRANSPARENT_OUTPUT_SPENT_BY_FIELD_PATH)
            .cloned()
            .collect()
    }

    fn open_spend_view_store(path: &Path) -> eyre::Result<MaterializedViewStore> {
        Ok(MaterializedViewStore::open_explorer_views(
            path,
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?)
    }

    fn spending_transaction_id() -> TransactionId {
        TransactionId::from_bytes([9; 32])
    }

    /// Spend of output 0 mined at height 2 by [`spending_transaction_id`].
    fn spend_of_first_output(transaction_id: TransactionId) -> TransparentSpendFact {
        TransparentSpendFact::new(
            TransparentOutPoint::new(transaction_id, 0),
            0,
            spending_transaction_id(),
            1,
            BlockHeight::new(2),
            block_hash(2),
            1_000,
            TransparentAddressScriptHash::of_script_pub_key(&[0x51]),
            BlockHeight::new(1),
            block_hash(1),
        )
    }

    fn chain_epoch_at(height: u32) -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(u64::from(height)),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(height),
            visible_tip_hash: block_hash(height),
            settled_tip_height: BlockHeight::new(0),
            settled_tip_hash: Network::ZcashRegtest.genesis_hash(),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(u64::from(height)),
        }
    }

    /// Spend-view state at `tip_height`, with verified coverage from
    /// `complete_from` through `complete_through`.
    fn spend_view_state(
        tip_height: u32,
        complete_from: u32,
        complete_through: u32,
    ) -> MaterializedViewState {
        MaterializedViewState {
            chain_epoch_id: ChainEpochId::new(u64::from(tip_height)),
            tip_height: BlockHeight::new(tip_height),
            tip_hash: block_hash(tip_height),
            revision: 1,
            coverage: Some(MaterializedViewCoverage {
                complete_from_height: BlockHeight::new(complete_from),
                complete_through_height: BlockHeight::new(complete_through),
                complete_through_hash: block_hash(complete_through),
            }),
        }
    }

    fn block_hash(height: u32) -> BlockHash {
        let mut bytes = [0xC3; 32];
        bytes[..4].copy_from_slice(&height.to_be_bytes());
        BlockHash::from_bytes(bytes)
    }

    #[test]
    fn mempool_detail_preserves_ordered_transparent_rows_from_payload() -> eyre::Result<()> {
        let payload_bytes = transparent_transaction_bytes();
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let parsed =
            zinder_source::parse_transaction_public_fact_set(&payload_bytes, None, &activations)?;
        let transaction_id = parsed.public_facts.transaction_id;

        let resolved = resolve_facts_and_location(
            &activations,
            transaction_id,
            wire_location::Location::InMempool(wallet::MempoolEntry {
                raw_transaction_bytes: payload_bytes,
                first_seen_unix_millis: 1_700_000_000_000,
                ..Default::default()
            }),
        )?;
        let fact_set = resolved.fact_set;
        let inputs = encode_unresolved_transparent_inputs(&fact_set.transparent_inputs, None);
        let outputs = encode_transparent_output_facts(
            transaction_id,
            &fact_set.transparent_outputs,
            &HashMap::new(),
        );

        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].input_index, 0);
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].output_index, 0);
        assert_eq!(
            outputs[0].output.as_ref().map(|output| output.value_zat),
            Some(1_000)
        );
        assert_eq!(outputs[1].output_index, 1);
        assert_eq!(
            outputs[1].output.as_ref().map(|output| output.value_zat),
            Some(2_500)
        );
        assert!(outputs.iter().all(|output| output.spent_by.is_none()));
        Ok(())
    }

    #[test]
    fn mined_detail_parses_wallet_transaction_bytes() -> eyre::Result<()> {
        let resolved = resolve_mined_transparent_transaction()?;

        assert_eq!(
            resolved.facts.transaction_id,
            resolved.fact_set.public_facts.transaction_id
        );
        assert_eq!(
            resolved.facts.consensus_branch_id,
            Some(ConsensusBranchId::new(0xAABB_CCDD))
        );
        Ok(())
    }

    #[test]
    fn fee_record_stays_behind_the_materialized_view_store() -> eyre::Result<()> {
        let resolved = resolve_mined_transparent_transaction()?;

        let outcome = resolve_fee_record(None, &resolved);

        assert!(matches!(outcome, Ok(None)));
        Ok(())
    }

    #[test]
    fn transparent_input_keeps_projected_value_without_a_parent_script() {
        let parent_transaction_id = TransactionId::from_bytes([7; 32]);
        let inputs = [TransparentInputFact::new(
            0,
            TransparentOutPoint::new(parent_transaction_id, 3),
        )];
        let fees = TransactionFeesRecord {
            transparent_inputs: vec![TransparentInputValueRecord {
                input_index: 0,
                value_zat: Some(42_000),
            }],
            ..Default::default()
        };

        let encoded = encode_unresolved_transparent_inputs(&inputs, Some(&fees));

        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].value_zat, Some(42_000));
        assert_eq!(encoded[0].script_pub_key, None);
    }

    #[test]
    fn malformed_non_coinbase_sentinel_is_not_exposed_as_a_transparent_input() {
        let inputs = [TransparentInputFact::new(
            0,
            TransparentOutPoint::COINBASE_SENTINEL,
        )];

        let encoded = encode_unresolved_transparent_inputs(&inputs, None);

        assert_eq!(
            encoded,
            [] as [zinder_proto::v1::explorer::TransparentInput; 0]
        );
    }

    fn resolve_mined_transparent_transaction() -> eyre::Result<ResolvedTransactionDetail> {
        let (transaction_id, location) = mined_transparent_location()?;
        Ok(resolve_facts_and_location(
            &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
            transaction_id,
            location,
        )?)
    }

    /// The two-output transparent transaction mined at height 1.
    fn mined_transparent_location() -> eyre::Result<(TransactionId, wire_location::Location)> {
        let payload_bytes = transparent_transaction_bytes();
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let parsed = zinder_source::parse_transaction_public_fact_set(
            &payload_bytes,
            Some(BlockHeight::new(1)),
            &activations,
        )?;
        let transaction_id = parsed.public_facts.transaction_id;

        Ok((
            transaction_id,
            wire_location::Location::Mined(wallet::MinedTransaction {
                location: Some(wallet::MinedBlockLocation {
                    transaction_id: encode_rpc_transaction_id_hex(transaction_id),
                    block_height: 1,
                    block_hash: "00".repeat(32),
                    tx_index_in_block: 0,
                }),
                chain_context: Some(wallet::MinedTransactionChainContext {
                    consensus_branch_id: 0xAABB_CCDD,
                    block_time: 1_700_000_000,
                    confirmations: 1,
                }),
                raw_transaction_bytes: Some(payload_bytes),
            }),
        ))
    }

    fn transparent_transaction_bytes() -> Vec<u8> {
        let mut bytes = vec![1, 0, 0, 0, 1];
        bytes.extend_from_slice(&[0xA5; 32]);
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.push(2);
        for (value_zat, script_pub_key) in [(1_000_u64, 0x51_u8), (2_500_u64, 0x52_u8)] {
            bytes.extend_from_slice(&value_zat.to_le_bytes());
            bytes.extend_from_slice(&[1, script_pub_key]);
        }
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes
    }
}
