//! Canonical reads and node access for the light-wallet qualification build.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use zinder_core::{
    BlockHeight, RawTransactionBytes, SubtreeRootRange, TransactionBroadcastOutcome,
};
use zinder_source::{SourceError, TransactionBroadcaster, ZebraJsonRpcSource};
use zinder_store::{ArtifactFamily, TreeStateLookup, TreeStateReplayOutcome};

use crate::{
    ArtifactKey, CanonicalReader, OrderedBroadcaster, QueryError, RocksDbCanonicalReader,
    SubtreeRoots, TreeState,
    broadcast_byte_budget::BroadcastByteBudget,
    tree_state_replay_limit::TreeStateReplayLimit,
    wallet_serving_query::{canonical_subtree_roots, tree_state_response},
};

/// Reads for a qualification-only lightwalletd server with one canonical store and one node.
#[derive(Clone)]
pub struct QualificationWalletQuery {
    source: ZebraJsonRpcSource,
    nodes: OrderedBroadcaster<ZebraJsonRpcSource>,
    replays: TreeStateReplayLimit,
}

impl QualificationWalletQuery {
    /// Uses the primary node for forwarding and the wallet replay bounds for canonical reads.
    #[must_use]
    pub fn new(source: ZebraJsonRpcSource, wait: Duration, waiters_per_slot: NonZeroU32) -> Self {
        Self {
            nodes: OrderedBroadcaster::new(source.clone(), []),
            source,
            replays: TreeStateReplayLimit::for_available_parallelism(wait)
                .with_waiters_per_slot(waiters_per_slot),
        }
    }

    /// Verifies the configured node against the writer-admitted schedule and tip before forwarding.
    pub async fn verified_node(
        &self,
        reader: &RocksDbCanonicalReader,
    ) -> Result<&ZebraJsonRpcSource, SourceError> {
        self.nodes.verify_primary_for_canonical(reader).await?;
        Ok(&self.source)
    }

    /// Submits a transaction through the verified endpoint under the wallet broadcast byte budget.
    pub async fn broadcast(
        &self,
        reader: &RocksDbCanonicalReader,
        bytes: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        if bytes.len() > zinder_core::MAX_RAW_TRANSACTION_BYTES {
            return Err(QueryError::BroadcastTransactionTooLarge {
                actual: bytes.len(),
                maximum: zinder_core::MAX_RAW_TRANSACTION_BYTES,
            });
        }
        let reservation = BroadcastByteBudget::process().try_reserve(bytes.len())?;
        self.verified_node(reader).await.map_err(QueryError::Node)?;
        reservation
            .hold_during(self.nodes.broadcast_transaction(bytes))
            .await
            .map_err(QueryError::Node)
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
        let state = match lookup {
            TreeStateLookup::Stored(state) => state,
            TreeStateLookup::Replay(replay) => {
                let permit = self.replays.admit().await?;
                let outcome = tokio::spawn(async move {
                    let _permit = permit;
                    reader.replay_tree_state(replay).await
                })
                .await
                .map_err(|error| QueryError::BlockingTaskFailed {
                    reason: error.to_string(),
                })?
                .map_err(QueryError::canonical_read)?
                .ok_or(QueryError::BlockNotInBestChain)?;
                match outcome {
                    TreeStateReplayOutcome::Stored(state)
                    | TreeStateReplayOutcome::Replayed {
                        tree_state: state, ..
                    } => state,
                }
            }
        };
        Ok(tree_state_response(epoch, state))
    }

    /// Returns the completed roots in one bounded page at the reader's epoch.
    pub async fn subtree_roots(
        reader: &RocksDbCanonicalReader,
        range: SubtreeRootRange,
    ) -> Result<SubtreeRoots, QueryError> {
        crate::validate_subtree_root_range(range)?;
        canonical_subtree_roots(reader, reader.chain_epoch(), range).await
    }
}
