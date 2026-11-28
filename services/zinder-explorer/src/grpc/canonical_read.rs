//! Shared canonical secondary access and projection helpers for `ExplorerQuery`.

use std::sync::Arc;

use parking_lot::RwLock;
use prost::Message as _;
use tonic::Status;
use zinder_core::{
    BlockHeaderArtifact, BlockHeight, CanonicalBlockFacts, ChainEpoch, ChainEpochId,
    TransactionIntrinsicValueBalances,
    wire::{decode_rpc_block_hash_hex, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex},
};
use zinder_proto::v1::{
    explorer::{
        BlockSummary, BlockSummaryRecord, CoinbaseTransactionSummary,
        DisplacedBlockCanonicalCounterpart, DisplacedBlockCoinbaseOutput,
        TransactionIntrinsicValueBalances as WireIntrinsicValueBalances,
    },
    wallet,
};
use zinder_store::{CanonicalStoreError, RocksDbCanonicalSecondary};

use super::error::ExplorerError;

/// Shared lock-protected canonical secondary store handle.
pub(crate) type SharedCanonicalSecondary = Arc<RwLock<RocksDbCanonicalSecondary>>;

/// Fails a handler when the explorer binary or test adapter was built without
/// a canonical secondary store.
pub(crate) fn canonical_store_not_configured(method: &'static str) -> Status {
    ExplorerError::dependency_not_configured(format!(
        "{method} requires canonical-store reads, which are not configured on this explorer",
    ))
    .into()
}

/// Converts a canonical storage failure into a boundary `Status`.
#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as a Result::map_err callback consuming CanonicalStoreError"
)]
pub(crate) fn internal_canonical_error(error: CanonicalStoreError) -> Status {
    ExplorerError::internal(format!("canonical store read failed: {error}")).into()
}

/// Error adapter for `visit_block_replay_facts_at_heights` callbacks.
pub(crate) enum CanonicalReplayVisitError {
    Canonical(CanonicalStoreError),
    Status(Status),
}

impl From<CanonicalStoreError> for CanonicalReplayVisitError {
    fn from(error: CanonicalStoreError) -> Self {
        Self::Canonical(error)
    }
}

impl From<Status> for CanonicalReplayVisitError {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl From<CanonicalReplayVisitError> for Status {
    fn from(error: CanonicalReplayVisitError) -> Self {
        match error {
            CanonicalReplayVisitError::Canonical(canonical) => internal_canonical_error(canonical),
            CanonicalReplayVisitError::Status(status) => status,
        }
    }
}

/// Executes `read_fn` under a canonical-secondary read guard pinned to either
/// the requested `at_epoch_id` or the current visible epoch.
pub(crate) async fn read_with_pinned_canonical_epoch<Output>(
    canonical_store: &SharedCanonicalSecondary,
    at_epoch_id: Option<u64>,
    read_fn: impl FnOnce(&RocksDbCanonicalSecondary, ChainEpoch) -> Result<Output, Status>
    + Send
    + 'static,
) -> Result<Output, Status>
where
    Output: Send + 'static,
{
    let store = Arc::clone(canonical_store);
    tokio::task::spawn_blocking(move || {
        maybe_catch_up_for_epoch(&store, at_epoch_id)?;
        let guard = store.read();
        let epoch = resolve_locked_canonical_epoch(&guard, at_epoch_id)?;
        let outcome = read_fn(&guard, epoch);
        drop(guard);
        outcome
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("canonical read task failed: {error}")))?
}

pub(crate) fn maybe_catch_up_for_epoch(
    store: &SharedCanonicalSecondary,
    at_epoch_id: Option<u64>,
) -> Result<(), Status> {
    let Some(requested_epoch_id) = at_epoch_id else {
        return Ok(());
    };
    let current_epoch = store
        .read()
        .chain_epoch()
        .map_err(internal_canonical_error)?;
    if requested_epoch_id <= current_epoch.id.value() {
        return Ok(());
    }
    store
        .write()
        .try_catch_up()
        .map_err(internal_canonical_error)?;
    Ok(())
}

pub(crate) fn resolve_locked_canonical_epoch(
    secondary: &RocksDbCanonicalSecondary,
    at_epoch_id: Option<u64>,
) -> Result<ChainEpoch, Status> {
    let current_epoch = secondary.chain_epoch().map_err(internal_canonical_error)?;
    let Some(requested_id) = at_epoch_id else {
        return Ok(current_epoch);
    };
    if requested_id == current_epoch.id.value() {
        return Ok(current_epoch);
    }
    if requested_id > current_epoch.id.value() {
        return Err(ExplorerError::unsatisfied_precondition(format!(
            "requested chain epoch {requested_id} is not yet visible (visible epoch {})",
            current_epoch.id.value(),
        ))
        .into());
    }
    let pinned_epoch = match secondary.chain_epoch_at(ChainEpochId::new(requested_id)) {
        Ok(epoch) => epoch,
        Err(CanonicalStoreError::CanonicalEpochNotRetained { .. }) => {
            return Err(ExplorerError::unsatisfied_precondition(format!(
                "requested chain epoch {requested_id} is no longer retained",
            ))
            .into());
        }
        Err(error) => return Err(internal_canonical_error(error)),
    };
    if pinned_epoch.visible_tip_height == BlockHeight::new(0) {
        return Ok(pinned_epoch);
    }
    let tip_matches = secondary
        .block_header_at(pinned_epoch.visible_tip_height)
        .map_err(internal_canonical_error)?
        .is_some_and(|header| header.block_hash == pinned_epoch.visible_tip_hash);
    if !tip_matches {
        return Err(ExplorerError::unsatisfied_precondition(format!(
            "requested chain epoch {requested_id} was superseded by a canonical reorg",
        ))
        .into());
    }
    Ok(pinned_epoch)
}

/// Validates that a materialized `BlockSummaryRecord` agrees on height, hash,
/// and block time with the canonical `BlockHeaderArtifact` and sets
/// epoch-bound request-time fields.
pub(crate) fn join_block_summary_with_header(
    payload: &[u8],
    header: &BlockHeaderArtifact,
    visible_tip_height: u32,
) -> Result<Option<BlockSummary>, Status> {
    let record = BlockSummaryRecord::decode(payload).map_err(|error| {
        ExplorerError::internal(format!("BlockSummaryRecord decode failed: {error}"))
    })?;
    let Some(mut summary) = record.summary else {
        return Err(ExplorerError::internal("BlockSummaryRecord.summary missing").into());
    };
    let summary_hash = decode_rpc_block_hash_hex(&summary.block_hash)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    if summary.block_height != header.height.value()
        || summary_hash != header.block_hash
        || summary.block_time_unix_seconds != header.block_time
    {
        return Ok(None);
    }
    summary.confirmations = visible_tip_height
        .saturating_sub(summary.block_height)
        .saturating_add(1);
    summary.is_canonical = summary.block_height <= visible_tip_height;
    Ok(Some(summary))
}

/// Builds a wire `CoinbaseTransactionSummary` from canonical replay facts.
pub(crate) fn encode_coinbase_summary(
    facts: &CanonicalBlockFacts,
) -> Result<Option<CoinbaseTransactionSummary>, Status> {
    let Some(coinbase) = facts.transactions.first() else {
        return Ok(None);
    };
    if !coinbase.public_facts.is_coinbase {
        return Err(ExplorerError::internal(
            "canonical block replay transaction[0] is not coinbase",
        )
        .into());
    }
    let transparent_outputs = coinbase
        .transparent_outputs
        .iter()
        .map(|output| wallet::TransparentOutput {
            value_zat: output.value_zat,
            script_pub_key: output.script_pub_key.clone(),
        })
        .collect();
    let has_shielded_outputs = (coinbase.public_facts.version.is_supported()
        && coinbase.public_facts.unsupported_sections.is_empty())
    .then_some(coinbase.public_facts.counts.has_shielded_output());
    Ok(Some(CoinbaseTransactionSummary {
        transaction_id: encode_rpc_transaction_id_hex(coinbase.public_facts.transaction_id),
        transparent_outputs,
        has_shielded_outputs,
    }))
}

/// Builds a `DisplacedBlockCanonicalCounterpart` from canonical replay facts.
pub(crate) fn encode_canonical_counterpart(
    facts: &CanonicalBlockFacts,
) -> Result<DisplacedBlockCanonicalCounterpart, Status> {
    let coinbase = facts.transactions.first().ok_or_else(|| {
        ExplorerError::internal("canonical block replay has no coinbase transaction")
    })?;
    if !coinbase.public_facts.is_coinbase {
        return Err(ExplorerError::internal(
            "canonical block replay transaction[0] is not coinbase",
        )
        .into());
    }
    let transaction_count = u32::try_from(facts.transactions.len())
        .map_err(|_| ExplorerError::internal("canonical block transaction count exceeds u32"))?;
    let coinbase_outputs = coinbase
        .transparent_outputs
        .iter()
        .map(|output| DisplacedBlockCoinbaseOutput {
            output_index: output.output_index,
            value_zat: output.value_zat,
            script_pub_key: output.script_pub_key.clone(),
        })
        .collect();
    Ok(DisplacedBlockCanonicalCounterpart {
        block_height: facts.block_header.height.value(),
        block_hash: encode_rpc_block_hash_hex(facts.block_header.block_hash),
        previous_block_hash: encode_rpc_block_hash_hex(facts.block_header.parent_hash),
        block_time_unix_seconds: facts.block_header.block_time,
        total_size_bytes: facts.block_header.block_size_bytes,
        difficulty_bits: facts.block_header.bits,
        transaction_count,
        coinbase_outputs,
    })
}

/// Encodes domain `TransactionIntrinsicValueBalances` into its protobuf shape.
pub(crate) const fn encode_intrinsic_value_balances(
    balances: TransactionIntrinsicValueBalances,
) -> WireIntrinsicValueBalances {
    WireIntrinsicValueBalances {
        sprout_zat: balances.sprout_zat,
        sapling_zat: balances.sapling_zat,
        orchard_zat: balances.orchard_zat,
        ironwood_zat: balances.ironwood_zat,
    }
}
