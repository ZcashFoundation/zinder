//! Canonical reads and node access for the light-wallet qualification build.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, RawTransactionBytes, SubtreeRootRange,
    TransactionBroadcastOutcome,
};
use zinder_source::{SourceError, TransactionBroadcaster, ZebraJsonRpcSource};
use zinder_store::{ArtifactFamily, BlockHashLookup, TreeStateLookup};

use crate::{
    ArtifactKey, CanonicalReader, QueryError, RocksDbCanonicalReader, SubtreeRoots, TreeState,
    broadcast_byte_budget::BroadcastByteBudget,
    tree_state_replay_limit::{
        TreeStateReplayLimit, record_stored_tree_state_read, replay_holding_slot,
    },
    wallet_serving_query::{canonical_subtree_roots, tree_state_response},
};

/// Reads for a qualification-only lightwalletd server with one canonical store and one node.
#[derive(Clone)]
pub struct QualificationWalletQuery {
    source: ZebraJsonRpcSource,
    capture: Arc<dyn Fn() -> Arc<RocksDbCanonicalReader> + Send + Sync>,
    replays: TreeStateReplayLimit,
}

impl QualificationWalletQuery {
    /// Uses the primary node for forwarding and the wallet replay bounds for canonical reads.
    #[must_use]
    pub fn new(
        capture: impl Fn() -> Arc<RocksDbCanonicalReader> + Send + Sync + 'static,
        source: ZebraJsonRpcSource,
        wait: Duration,
        waiters_per_slot: NonZeroU32,
    ) -> Self {
        Self {
            capture: Arc::new(capture),
            source,
            replays: TreeStateReplayLimit::for_available_parallelism(wait)
                .with_waiters_per_slot(waiters_per_slot),
        }
    }

    /// Verifies the configured node against the writer-admitted schedule and tip before forwarding.
    pub async fn verified_node(&self) -> Result<&ZebraJsonRpcSource, SourceError> {
        let target =
            crate::node_broadcast::VerificationTarget::from_canonical((self.capture)().as_ref())?;
        target.verify(&self.source).await?;
        Ok(&self.source)
    }

    /// Submits a transaction through the verified endpoint under the wallet broadcast byte budget.
    pub async fn broadcast(
        &self,
        bytes: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        if bytes.len() > zinder_core::MAX_RAW_TRANSACTION_BYTES {
            return Err(QueryError::BroadcastTransactionTooLarge {
                actual: bytes.len(),
                maximum: zinder_core::MAX_RAW_TRANSACTION_BYTES,
            });
        }
        let reservation = BroadcastByteBudget::process().try_reserve(bytes.len())?;
        let node = self.verified_node().await.map_err(QueryError::Node)?;
        reservation
            .hold_during(async {
                let outcome = node.broadcast_transaction(bytes).await;
                crate::node_broadcast::record_attempt(0, &outcome);
                outcome.map_err(QueryError::Node)
            })
            .await
    }

    /// Reads a stored or reconstructed frontier at a height, or the latest checkpoint when unset.
    pub async fn tree_state(
        &self,
        reader: Arc<RocksDbCanonicalReader>,
        height: Option<BlockHeight>,
    ) -> Result<TreeState, QueryError> {
        let epoch = reader.chain_epoch();
        let Some(height) = height else {
            let checkpoint = reader
                .tree_state_checkpoint_at_or_before(epoch.visible_tip_height)
                .await
                .map_err(QueryError::canonical_read)?
                .ok_or(QueryError::ArtifactUnavailable {
                    family: ArtifactFamily::TreeState,
                    key: ArtifactKey::BlockHeight(epoch.visible_tip_height),
                })?;
            return Ok(tree_state_response(epoch, checkpoint));
        };
        let lookup = reader
            .tree_state_lookup(height)
            .await
            .map_err(QueryError::canonical_read)?
            .ok_or(QueryError::BlockNotInBestChain)?;
        let replay = match lookup {
            TreeStateLookup::Stored(state) => {
                record_stored_tree_state_read(height, &state);
                return Ok(tree_state_response(epoch, state));
            }
            TreeStateLookup::Replay(replay) => replay,
        };
        let block = reader
            .block_header_at(height)
            .await
            .map_err(QueryError::canonical_read)?
            .ok_or(QueryError::BlockNotInBestChain)?;
        let expected = BlockId::new(height, block.block_hash);
        drop(reader);
        let replay_slot = Arc::new(self.replays.admit().await?);
        let reader = (self.capture)();
        let replay_epoch = reader.chain_epoch();
        if !retains_block(&reader, epoch, expected).await? {
            return Err(QueryError::BlockNotInBestChain);
        }
        replay_holding_slot(height, replay_slot, async move {
            reader.replay_tree_state(replay).await
        })
        .await
        .map(|state| tree_state_response(replay_epoch, state))
    }

    /// Returns the completed roots in one bounded page as of the captured epoch, while its tip remains canonical.
    pub async fn subtree_roots(
        &self,
        epoch: ChainEpoch,
        range: SubtreeRootRange,
    ) -> Result<SubtreeRoots, QueryError> {
        crate::validate_subtree_root_range(range)?;
        let reader = (self.capture)();
        let initial_tip = BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash);
        if !retains_block(&reader, epoch, initial_tip).await? {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: epoch.id,
            });
        }
        canonical_subtree_roots(reader.as_ref(), epoch, range).await
    }
}

async fn retains_block(
    reader: &RocksDbCanonicalReader,
    captured_epoch: ChainEpoch,
    block: BlockId,
) -> Result<bool, QueryError> {
    if reader.chain_epoch() == captured_epoch {
        return Ok(true);
    }
    let resolved = reader
        .block_hash_lookup(block.hash)
        .await
        .map_err(QueryError::canonical_read)?;
    Ok(resolved == BlockHashLookup::Resolved(block))
}

#[cfg(test)]
mod tests;
