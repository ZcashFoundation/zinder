//! Canonical reads and node access for the light-wallet qualification build.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, RawTransactionBytes, SubtreeRootRange,
    TransactionBroadcastOutcome,
};
use zinder_source::{SourceError, TransactionBroadcaster, ZebraJsonRpcSource};
use zinder_store::{ArtifactFamily, BlockHashLookup};

use crate::{
    ArtifactKey, CanonicalReader, QueryError, RocksDbCanonicalReader, SubtreeRoots, TreeState,
    broadcast_byte_budget::BroadcastByteBudget,
    tree_state_replay_limit::{
        ReaderGeneration, ReadyState, ReplayKey, Selection, TreeStateReplayLimit,
        read_missing_height,
    },
    wallet_serving_query::{canonical_subtree_roots, tree_state_response},
};

/// Reads for a qualification-only lightwalletd server with one canonical store and one node.
#[derive(Clone)]
pub struct QualificationWalletQuery {
    source: ZebraJsonRpcSource,
    capture: Arc<dyn Fn() -> Arc<RocksDbCanonicalReader> + Send + Sync>,
    replays: TreeStateReplayLimit,
    verification: Arc<crate::node_broadcast::QualificationNodeVerification>,
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
            verification: Arc::default(),
            replays: TreeStateReplayLimit::for_available_parallelism(wait)
                .with_waiters_per_slot(waiters_per_slot),
        }
    }

    /// Verifies the configured node against the writer-admitted schedule and tip before forwarding.
    pub async fn verified_node(&self) -> Result<&ZebraJsonRpcSource, SourceError> {
        let target =
            crate::node_broadcast::VerificationTarget::from_canonical((self.capture)().as_ref())?;
        self.verification.verify(&self.source, &target).await?;
        if !target.still_served_by((self.capture)().as_ref()) {
            return Err(SourceError::NodeUnavailable {
                reason:
                    "served canonical fence or upgrade schedule changed during node verification"
                        .to_owned(),
            });
        }
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

    fn live_generation(
        &self,
        reader: &Arc<RocksDbCanonicalReader>,
    ) -> (std::sync::Weak<RocksDbCanonicalReader>, ReaderGeneration) {
        let generation = Arc::downgrade(reader);
        let capture = Arc::clone(&self.capture);
        let weak_for_check = generation.clone();
        let cache_generation = ReaderGeneration::live(&generation, move || {
            std::sync::Weak::ptr_eq(&weak_for_check, &Arc::downgrade(&capture()))
        });
        (generation, cache_generation)
    }

    fn cached_on_current_reader(
        &self,
        key: &ReplayKey,
        reader: &Arc<RocksDbCanonicalReader>,
    ) -> Option<ReadyState> {
        self.replays
            .cached_for_reader(key, reader)
            .filter(|_| Arc::ptr_eq(reader, &(self.capture)()))
    }

    /// Reads a stored or reconstructed frontier at a height, or the latest checkpoint when unset.
    pub async fn tree_state(
        &self,
        mut reader: Arc<RocksDbCanonicalReader>,
        height: Option<BlockHeight>,
    ) -> Result<TreeState, QueryError> {
        let Some(height) = height else {
            return latest_checkpoint(reader.as_ref()).await;
        };
        let mut deadline = None;
        let mut carried = None;
        loop {
            let request_deadline = *deadline.get_or_insert_with(|| self.replays.deadline());
            let epoch = reader.chain_epoch();
            let key = ReplayKey::new(height, reader.as_ref());
            if let Some(ready) = self.cached_on_current_reader(&key, &reader) {
                return Ok(tree_state_response(epoch, ready.serve()));
            }
            let (generation, cache_generation) = self.live_generation(&reader);
            let ready = {
                let canonical_reader = Arc::clone(&reader) as Arc<dyn CanonicalReader>;
                self.replays
                    .ready_state(&key, &cache_generation, &canonical_reader, request_deadline)
                    .await?
            };
            let (selected, expected) = match ready {
                Some(ready @ ReadyState::Stored(_)) => {
                    return Ok(tree_state_response(epoch, ready.serve()));
                }
                Some(ReadyState::Cached(state)) => {
                    let block = state.block_id();
                    (Selection::Ready(ReadyState::Cached(state)), block)
                }
                None => {
                    let block = reader
                        .block_header_at(height)
                        .await
                        .map_err(QueryError::canonical_read)?
                        .ok_or(QueryError::BlockNotInBestChain)?;
                    let selected = self.replays.select(
                        &key,
                        &cache_generation,
                        request_deadline,
                        carried.take(),
                    )?;
                    (selected, BlockId::new(height, block.block_hash))
                }
            };
            let subscription = match selected {
                Selection::Ready(state) => {
                    let current = (self.capture)();
                    if !retains_block(&current, epoch, expected).await? {
                        return Err(QueryError::BlockNotInBestChain);
                    }
                    if ReplayKey::new(height, current.as_ref()) == key {
                        return Ok(tree_state_response(epoch, state.serve()));
                    }
                    reader = current;
                    continue;
                }
                Selection::Waiting(subscription) => subscription,
            };
            drop(reader);
            let slot = subscription.admit(&self.replays).await?;
            reader = (self.capture)();
            if !generation.ptr_eq(&Arc::downgrade(&reader)) {
                carried = subscription.carry(&slot);
                drop(subscription);
                drop(slot);
                if !retains_block(&reader, epoch, expected).await? {
                    return Err(QueryError::BlockNotInBestChain);
                }
                continue;
            }
            let captured = reader.clone();
            if let Some(state) = subscription
                .replay(slot, |cancelled| async move {
                    read_missing_height(captured.as_ref(), height, cancelled).await
                })
                .await?
            {
                return Ok(tree_state_response(epoch, state));
            }
        }
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

async fn latest_checkpoint(reader: &RocksDbCanonicalReader) -> Result<TreeState, QueryError> {
    let epoch = reader.chain_epoch();
    let checkpoint = reader
        .tree_state_checkpoint_at_or_before(epoch.visible_tip_height)
        .await
        .map_err(QueryError::canonical_read)?
        .ok_or(QueryError::ArtifactUnavailable {
            family: ArtifactFamily::TreeState,
            key: ArtifactKey::BlockHeight(epoch.visible_tip_height),
        })?;
    Ok(tree_state_response(epoch, checkpoint))
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
