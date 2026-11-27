//! Canonical reads and node access for the node-backed light-wallet serving composition.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, RawTransactionBytes, SubtreeRootRange,
    TransactionBroadcastOutcome,
};
use zinder_source::{SourceError, TransactionBroadcaster, ZebraJsonRpcSource};
use zinder_store::{ArtifactFamily, BlockHashLookup};

use crate::{
    ArtifactKey, CanonicalReader, QueryError, RocksDbCanonicalReader, ServedTreeStateHex,
    SubtreeRoots, TreeState,
    broadcast_byte_budget::BroadcastByteBudget,
    tree_state_replay_limit::{
        ReaderGeneration, ReadyState, ReplayKey, Selection, TreeStateReplayLimit,
        read_missing_height,
    },
    wallet_serving_query::{canonical_subtree_roots, tree_state_response},
};

/// The canonical fence and upgrade schedule of one captured reader, which a
/// forwarded read requires the node to serve.
///
/// A request captures it once, so node admission checks the chain view the
/// request answers for even after the served reader changes.
pub struct VerifiedNodeTarget(crate::node_broadcast::VerificationTarget);

impl VerifiedNodeTarget {
    /// Captures the tip and schedule `reader` serves.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::SourceProtocolMismatch`] when the reader has no
    /// writer-admitted upgrade schedule.
    pub fn of(reader: &RocksDbCanonicalReader) -> Result<Self, SourceError> {
        crate::node_broadcast::VerificationTarget::from_canonical(reader).map(Self)
    }
}

type ReaderCapture = Arc<dyn Fn() -> Arc<RocksDbCanonicalReader> + Send + Sync>;
type CurrentReaderCheck = Arc<dyn Fn(&Arc<RocksDbCanonicalReader>) -> bool + Send + Sync>;

/// Reads for a node-backed lightwalletd server with one canonical store and one node.
#[derive(Clone)]
pub struct LightWalletNodeQuery {
    source: ZebraJsonRpcSource,
    capture: ReaderCapture,
    is_current: Option<CurrentReaderCheck>,
    replays: TreeStateReplayLimit,
    verification: Arc<crate::node_broadcast::LightWalletNodeVerification>,
}

impl LightWalletNodeQuery {
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
            is_current: None,
            source,
            verification: Arc::default(),
            replays: TreeStateReplayLimit::for_available_parallelism(wait)
                .with_waiters_per_slot(waiters_per_slot),
        }
    }

    /// Uses a lightweight current-generation predicate (such as an `ArcSwap`
    /// guard pointer check) to avoid cloning and dropping the current reader on
    /// settled tree-state cache hits.
    #[must_use]
    pub fn with_current_reader_check(
        mut self,
        is_current: impl Fn(&Arc<RocksDbCanonicalReader>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.is_current = Some(Arc::new(is_current));
        self
    }

    /// Verifies the configured node against the schedule and tip of `target`,
    /// the reader generation a forwarded read answers for.
    ///
    /// The schedule check is shared and reused for up to a minute; the tip
    /// check is shared only with concurrent callers checking the same tip.
    pub async fn verified_node(
        &self,
        target: &VerifiedNodeTarget,
    ) -> Result<&ZebraJsonRpcSource, SourceError> {
        self.verification.verify(&self.source, &target.0).await?;
        Ok(&self.source)
    }

    /// Verifies the configured node's capabilities and upgrade schedule
    /// against `target`, without checking that its best chain holds the
    /// served tip.
    ///
    /// Use it only for a read whose node reply binds itself to the served
    /// chain view: mined transaction bytes the node reports in the served
    /// block, unspent outputs the node reports at the served tip, or address
    /// history pages bracketed by their own tip checks.
    pub async fn schedule_verified_node(
        &self,
        target: &VerifiedNodeTarget,
    ) -> Result<&ZebraJsonRpcSource, SourceError> {
        self.verification
            .verify_schedule_and_capabilities(&self.source, &target.0)
            .await?;
        Ok(&self.source)
    }

    /// Verifies the configured node against the schedule and tip of `target`,
    /// charging the tip check against `reads` as a `kind` call inside the
    /// shared check.
    pub async fn verify_admitted_node(
        &self,
        target: &VerifiedNodeTarget,
        reads: &crate::ForwardedReadLimit,
        kind: crate::ForwardedReadKind,
        deadline: tokio::time::Instant,
    ) -> Result<&ZebraJsonRpcSource, QueryError> {
        self.verification
            .verify_schedule_and_capabilities(&self.source, &target.0)
            .await
            .map_err(QueryError::Node)?;
        self.verification
            .verify_served_tip_admitted(
                &self.source,
                &target.0,
                crate::node_broadcast::AdmittedTipCheck {
                    limit: reads,
                    kind,
                    deadline,
                },
            )
            .await?;
        Ok(&self.source)
    }

    /// Returns whether the configured node's best chain holds `block`,
    /// sharing one call with concurrent callers asking about the same block.
    pub async fn node_holds_block(&self, block: BlockId) -> Result<bool, SourceError> {
        self.verification.holds_block(&self.source, block).await
    }

    /// Returns whether the configured node's best chain holds `block`,
    /// charging one `kind` slot of `reads` inside the shared check.
    pub async fn node_holds_block_admitted(
        &self,
        block: BlockId,
        reads: &crate::ForwardedReadLimit,
        kind: crate::ForwardedReadKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, QueryError> {
        self.verification
            .holds_block_admitted(
                &self.source,
                block,
                crate::node_broadcast::AdmittedTipCheck {
                    limit: reads,
                    kind,
                    deadline,
                },
            )
            .await
    }

    /// Verifies the configured node against the currently served reader, and
    /// refuses when that reader changes during verification.
    pub(crate) async fn verified_current_node(&self) -> Result<&ZebraJsonRpcSource, SourceError> {
        self.verify_current(&self.source).await
    }

    /// Returns the readiness-probe view of the configured node, whose calls
    /// never wait behind forwarded calls.
    pub(crate) fn readiness_probe_node(&self) -> ZebraJsonRpcSource {
        self.source.for_readiness_probes()
    }

    /// Verifies `node`, the configured node or a view of it, against the
    /// currently served reader, and refuses when that reader changes during
    /// verification. Every view shares one schedule verification.
    ///
    /// The configured node shares its tip check with concurrent forwarded
    /// requests. Any other view, such as the readiness-probe view, makes its
    /// own tip check through its own call slots, so it never waits on a call
    /// that holds a shared slot.
    pub(crate) async fn verify_current<'node>(
        &self,
        node: &'node ZebraJsonRpcSource,
    ) -> Result<&'node ZebraJsonRpcSource, SourceError> {
        let target = VerifiedNodeTarget::of((self.capture)().as_ref())?;
        if std::ptr::eq(node, &raw const self.source) {
            self.verification.verify(node, &target.0).await?;
        } else {
            self.verification
                .verify_with_own_tip_check(node, &target.0)
                .await?;
        }
        if !target.0.still_served_by((self.capture)().as_ref()) {
            return Err(SourceError::NodeUnavailable {
                reason:
                    "served canonical fence or upgrade schedule changed during node verification"
                        .to_owned(),
            });
        }
        Ok(node)
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
        let node = self
            .verified_current_node()
            .await
            .map_err(QueryError::Node)?;
        reservation
            .hold_during(async {
                let outcome =
                    zinder_runtime::trace_node_rpc(node.broadcast_transaction(bytes)).await;
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
        height: BlockHeight,
        visible_tip_height: BlockHeight,
        reader: &Arc<RocksDbCanonicalReader>,
    ) -> Option<ReadyState> {
        self.replays
            .cached_for_reader(height, visible_tip_height, reader)
            .filter(|_| {
                self.is_current.as_ref().map_or_else(
                    || Arc::ptr_eq(reader, &(self.capture)()),
                    |check| check(reader),
                )
            })
    }

    async fn ready_or_select(
        &self,
        reader: &Arc<RocksDbCanonicalReader>,
        (key, cache_generation): (&ReplayKey, &ReaderGeneration),
        request_deadline: tokio::time::Instant,
        carried: &mut Option<crate::tree_state_replay_limit::TakeReplayPermit>,
    ) -> Result<Result<ReadyState, (Selection, BlockId)>, QueryError> {
        let canonical = Arc::clone(reader) as Arc<dyn CanonicalReader>;
        let ready = self
            .replays
            .ready_state(key, cache_generation, &canonical, request_deadline)
            .await?;
        drop(canonical);
        match ready {
            Some(ready @ ReadyState::Stored(_)) => Ok(Ok(ready)),
            Some(ReadyState::Cached(state)) => {
                let block = state.state().block_id();
                Ok(Err((Selection::Ready(ReadyState::Cached(state)), block)))
            }
            None => {
                let height = key.height();
                let block = reader
                    .block_header_at(height)
                    .await
                    .map_err(QueryError::canonical_read)?
                    .ok_or(QueryError::BlockNotInBestChain)?;
                let selected =
                    self.replays
                        .select(key, cache_generation, request_deadline, carried.take())?;
                Ok(Err((selected, BlockId::new(height, block.block_hash))))
            }
        }
    }

    /// Reads a stored or reconstructed frontier formatted for lightwalletd
    /// `GetTreeState`, reusing pre-encoded hex strings on settled cache hits.
    pub async fn tree_state_hex(
        &self,
        reader: Arc<RocksDbCanonicalReader>,
        height: Option<BlockHeight>,
    ) -> Result<ServedTreeStateHex, QueryError> {
        if let Some(height) = height {
            let epoch = reader.chain_epoch();
            if let Some(ready) =
                self.cached_on_current_reader(height, epoch.visible_tip_height, &reader)
            {
                return Ok(ready.serve_hex(epoch.network));
            }
        }
        let state = self.tree_state(reader, height).await?;
        Ok(ServedTreeStateHex::from_tree_state(&state))
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
            let epoch = reader.chain_epoch();
            if let Some(ready) =
                self.cached_on_current_reader(height, epoch.visible_tip_height, &reader)
            {
                return Ok(tree_state_response(epoch, ready.serve()));
            }
            let request_deadline = *deadline.get_or_insert_with(|| self.replays.deadline());
            let key = ReplayKey::new(height, reader.as_ref());
            let (generation, cache_generation) = self.live_generation(&reader);
            let (selected, expected) = match self
                .ready_or_select(
                    &reader,
                    (&key, &cache_generation),
                    request_deadline,
                    &mut carried,
                )
                .await?
            {
                Ok(ready) => return Ok(tree_state_response(epoch, ready.serve())),
                Err(pending) => pending,
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
                drop((subscription, slot));
                if !retains_block(&reader, epoch, expected).await? {
                    return Err(QueryError::BlockNotInBestChain);
                }
                continue;
            }
            let captured = Arc::clone(&reader);
            let replayed = subscription
                .replay(slot, |cancelled| async move {
                    read_missing_height(captured.as_ref(), height, cancelled).await
                })
                .await?;
            if let Some(state) = replayed {
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
