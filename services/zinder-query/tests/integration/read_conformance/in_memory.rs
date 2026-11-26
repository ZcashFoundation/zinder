//! The canonical conformance suite run against a canonical reader that holds
//! its chain in memory.
//!
//! The engine stores nothing a storage library defines: it builds every
//! value the interface returns from the testkit's chain, using only the
//! engine-neutral constructors a second storage engine would use. Passing the
//! suite shows that [`CanonicalReader`] can be implemented outside `RocksDB`.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error as StdError,
    num::NonZeroU32,
    sync::Arc,
};

use async_trait::async_trait;
use eyre::{ensure, eyre};
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalHistoryBounds, ChainEpoch, ChainEpochId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeCheckpoint, CommitmentTreeFrontiers, CompactBlockArtifact, Network,
    NetworkUpgradeActivations, NetworkUpgradeActivationsFingerprintVersion, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootRange, TransactionBlobArtifact, TransactionId,
    TransactionLocation, UnixTimestampMillis,
};
use zinder_query::{ArtifactKey, CanonicalReader, IndexedTransaction, StorageReadError};
use zinder_store::{
    ArtifactFamily, BlockHashLookup, CURRENT_ARTIFACT_SCHEMA_VERSION, CanonicalActivationSchedule,
    CanonicalEventFence, ChainEpochCommitted, ChainEvent, ChainEventEnvelope,
    ChainEventStreamFamily, ChainEventStreamResume, ChainRangeReverted, EventStreamStartPosition,
    HeightOrder, RawBlobRetention, ServedTreeState, StreamCursorTokenV1, TreeStateLookup,
    TreeStateReplay, TreeStateReplayOutcome,
};
use zinder_testkit::{CanonicalBaseline, ChainFixture};

use super::{ReaderHarness, canonical_reader_conformance};

/// Bytes the engine counts for each stored compact block, plus one per
/// transaction, when it bounds a range read by bytes.
const BLOCK_BYTES: usize = 64;
/// Leading bytes of every chain-event cursor this engine issues.
const CURSOR_MAGIC: &[u8; 4] = b"imc1";
/// Length of a cursor: its magic, its family, and its event sequence.
const CURSOR_LEN: usize = 13;

/// A writer that keeps every canonical fence in memory, and the reader of the
/// fence that is current when one opens.
pub(crate) struct InMemoryReaderHarness {
    current: Arc<Snapshot>,
    /// Commitment trees after each visible block, which the writer needs to
    /// store a checkpoint for a block it appends.
    trees_after: BTreeMap<u32, CommitmentTreeCheckpoint>,
}

/// Everything one admitted fence serves.
#[derive(Clone)]
struct Snapshot {
    network: Network,
    activations: NetworkUpgradeActivations,
    schedule: CanonicalActivationSchedule,
    raw_blob_retention: RawBlobRetention,
    history_bounds: CanonicalHistoryBounds,
    fence: CanonicalEventFence,
    chain_epoch: ChainEpoch,
    blocks: BTreeMap<u32, StoredBlock>,
    displaced: HashSet<BlockHash>,
    transactions: HashMap<TransactionId, StoredTransaction>,
    checkpoints: BTreeMap<u32, CommitmentTreeCheckpoint>,
    subtree_roots: BTreeMap<(u8, u32), SubtreeRootArtifact>,
    events: Vec<RecordedEvent>,
    oldest_retained_sequence: u64,
}

#[derive(Clone)]
struct StoredBlock {
    header: BlockHeaderArtifact,
    compact_block: CompactBlockArtifact,
    blob: BlockBlobArtifact,
}

#[derive(Clone)]
struct StoredTransaction {
    location: TransactionLocation,
    blob: Option<TransactionBlobArtifact>,
}

#[derive(Clone)]
struct RecordedEvent {
    event_sequence: u64,
    chain_epoch: ChainEpoch,
    event: ChainEvent,
    newly_settled: Option<BlockHeightRange>,
}

impl ReaderHarness for InMemoryReaderHarness {
    type Canonical = InMemoryCanonicalReader;

    fn publish(
        baseline: &CanonicalBaseline<'_>,
        activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        let chain = baseline.chain();
        let tip = baseline
            .tip_height()
            .ok_or_else(|| eyre!("a baseline publishes at least one block"))?;
        let predecessor = baseline.history_floor().unwrap_or(BlockHeight::new(0));
        let first_retained = predecessor
            .next()
            .ok_or_else(|| eyre!("the history floor leaves no block to retain"))?;
        let trees_after = trees_after_each_block(chain, tip, activations)?;
        let history_bounds = match baseline.history_floor() {
            None => CanonicalHistoryBounds::complete(),
            Some(floor) => CanonicalHistoryBounds::checkpointed(block_id(chain, floor)?)?,
        };
        let mut blocks = BTreeMap::new();
        for height in first_retained.value()..=tip.value() {
            blocks.insert(height, stored_block(chain, BlockHeight::new(height))?);
        }
        let visible_block_count = u64::try_from(blocks.len())?;
        let mut checkpoints = BTreeMap::new();
        for height in [predecessor.value(), tip.value()] {
            checkpoints.insert(height, tree_checkpoint(&trees_after, height)?);
        }
        let mut subtree_roots = BTreeMap::new();
        for root in baseline.subtree_roots() {
            let completing = block_id(chain, root.completing_block_height)?;
            subtree_roots.insert(
                (protocol_key(root.protocol), root.subtree_index.value()),
                SubtreeRootArtifact::new(
                    root.protocol,
                    root.subtree_index,
                    root.root_hash,
                    completing.height,
                    completing.hash,
                ),
            );
        }
        let settled = baseline
            .settled_tip_height()
            .ok_or_else(|| eyre!("a baseline settles a block"))?;
        let mut snapshot = Snapshot {
            network: chain.network(),
            activations: activations.clone(),
            schedule: CanonicalActivationSchedule::first_revision(
                activations.clone(),
                activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
                block_id(chain, tip)?,
            )?,
            raw_blob_retention: chain.raw_blob_retention(),
            history_bounds,
            fence: fence_at(1, block_id(chain, tip)?, visible_block_count),
            chain_epoch: epoch_at(1, chain.network(), &blocks, block_id(chain, settled)?)?,
            blocks,
            displaced: HashSet::new(),
            transactions: HashMap::new(),
            checkpoints,
            subtree_roots,
            events: Vec::new(),
            oldest_retained_sequence: 1,
        };
        snapshot.index_transactions(chain);
        let committed = ChainEpochCommitted {
            chain_epoch: snapshot.chain_epoch,
            block_range: BlockHeightRange::inclusive(first_retained, tip),
        };
        snapshot.record(
            ChainEvent::ChainCommitted { committed },
            Some(BlockHeightRange::inclusive(first_retained, settled)),
        );
        Ok(Self {
            current: Arc::new(snapshot),
            trees_after,
        })
    }

    fn append_live_block(
        &mut self,
        chain: &ChainFixture,
        settled_tip: BlockId,
    ) -> eyre::Result<()> {
        let height = self
            .current
            .tip()?
            .height
            .next()
            .ok_or_else(|| eyre!("the tip height cannot grow"))?;
        let mut next = self.current.as_ref().clone();
        next.add_live_block(chain, height, &mut self.trees_after)?;
        next.index_transactions(chain);
        let committed = next.advance(BlockHeightRange::inclusive(height, height), settled_tip)?;
        let newly_settled = self
            .current
            .chain_epoch
            .settled_tip_height
            .next()
            .filter(|start| *start <= settled_tip.height)
            .map(|start| BlockHeightRange::inclusive(start, settled_tip.height));
        next.record(ChainEvent::ChainCommitted { committed }, newly_settled);
        self.current = Arc::new(next);
        Ok(())
    }

    fn replace_live_blocks(
        &mut self,
        replacement_chain: &ChainFixture,
        first_replaced_height: BlockHeight,
    ) -> eyre::Result<()> {
        let reverted = ChainRangeReverted {
            chain_epoch: self.current.chain_epoch,
            block_range: BlockHeightRange::inclusive(
                first_replaced_height,
                self.current.tip()?.height,
            ),
        };
        ensure!(
            self.current.chain_epoch.settled_tip_height < first_replaced_height,
            "a replacement cannot reach settled blocks"
        );
        let mut next = self.current.as_ref().clone();
        let displaced = next.blocks.split_off(&first_replaced_height.value());
        next.displaced
            .extend(displaced.values().map(|block| block.header.block_hash));
        next.transactions
            .retain(|_, transaction| transaction.location.block_height < first_replaced_height);
        next.checkpoints
            .retain(|height, _| *height < first_replaced_height.value());
        self.trees_after
            .retain(|height, _| *height < first_replaced_height.value());
        let replacement_tip = replacement_chain
            .tip_height()
            .ok_or_else(|| eyre!("a replacement chain has blocks"))?;
        for height in first_replaced_height.value()..=replacement_tip.value() {
            next.add_live_block(
                replacement_chain,
                BlockHeight::new(height),
                &mut self.trees_after,
            )?;
        }
        next.index_transactions(replacement_chain);
        let committed = next.advance(
            BlockHeightRange::inclusive(first_replaced_height, replacement_tip),
            BlockId::new(
                reverted.chain_epoch.settled_tip_height,
                reverted.chain_epoch.settled_tip_hash,
            ),
        )?;
        next.record(
            ChainEvent::ChainReorged {
                reverted,
                committed,
            },
            None,
        );
        self.current = Arc::new(next);
        Ok(())
    }

    fn prune_chain_events_before(&mut self, oldest_retained_sequence: u64) -> eyre::Result<()> {
        let mut next = self.current.as_ref().clone();
        next.events
            .retain(|event| event.event_sequence >= oldest_retained_sequence);
        next.oldest_retained_sequence = oldest_retained_sequence;
        self.current = Arc::new(next);
        Ok(())
    }

    fn canonical_reader(&mut self) -> eyre::Result<InMemoryCanonicalReader> {
        Ok(InMemoryCanonicalReader {
            snapshot: Arc::clone(&self.current),
        })
    }
}

impl Snapshot {
    fn tip(&self) -> eyre::Result<BlockId> {
        self.blocks
            .values()
            .next_back()
            .map(|block| BlockId::new(block.header.height, block.header.block_hash))
            .ok_or_else(|| eyre!("the snapshot holds no block"))
    }

    /// Stores the block of `chain` at `height` above the tip, with a
    /// checkpoint, as live following does.
    fn add_live_block(
        &mut self,
        chain: &ChainFixture,
        height: BlockHeight,
        trees_after: &mut BTreeMap<u32, CommitmentTreeCheckpoint>,
    ) -> eyre::Result<()> {
        let block = stored_block(chain, height)?;
        let below = height
            .value()
            .checked_sub(1)
            .ok_or_else(|| eyre!("a live block is above genesis"))?;
        let seed = tree_checkpoint(trees_after, below)?;
        let checkpoint = replay_tree(&seed, height, &self.activations, |height| {
            (height == block.header.height).then_some(&block.compact_block)
        })
        .map_err(|error| eyre!("live block trees: {error}"))?;
        trees_after.insert(height.value(), checkpoint.clone());
        self.checkpoints.insert(height.value(), checkpoint);
        self.blocks.insert(height.value(), block);
        Ok(())
    }

    /// Indexes the transactions `chain` mines in this snapshot's visible
    /// blocks.
    fn index_transactions(&mut self, chain: &ChainFixture) {
        for rows in chain.canonical_transaction_rows() {
            let visible = self
                .blocks
                .get(&rows.location.block_height.value())
                .is_some_and(|block| block.header.block_hash == rows.location.block_hash);
            if visible {
                self.transactions.insert(
                    rows.location.transaction_id,
                    StoredTransaction {
                        location: rows.location,
                        blob: rows.blob,
                    },
                );
            }
        }
    }

    /// Commits the next epoch at the current tip and returns its payload.
    fn advance(
        &mut self,
        block_range: BlockHeightRange,
        settled_tip: BlockId,
    ) -> eyre::Result<ChainEpochCommitted> {
        let next_epoch = self
            .chain_epoch
            .id
            .value()
            .checked_add(1)
            .ok_or_else(|| eyre!("the epoch id cannot grow"))?;
        let tip = self.tip()?;
        self.chain_epoch = epoch_at(next_epoch, self.network, &self.blocks, settled_tip)?;
        self.fence = fence_at(next_epoch, tip, self.blocks.len().try_into()?);
        Ok(ChainEpochCommitted {
            chain_epoch: self.chain_epoch,
            block_range,
        })
    }

    fn record(&mut self, event: ChainEvent, newly_settled: Option<BlockHeightRange>) {
        self.events.push(RecordedEvent {
            event_sequence: self.fence.chain_event_sequence(),
            chain_epoch: self.chain_epoch,
            event,
            newly_settled,
        });
    }

    fn block_by_hash(&self, block_hash: BlockHash) -> Option<&StoredBlock> {
        self.blocks
            .values()
            .find(|block| block.header.block_hash == block_hash)
    }

    fn checkpoint_below(&self, height: BlockHeight) -> Option<&CommitmentTreeCheckpoint> {
        self.checkpoints
            .range(..height.value())
            .next_back()
            .map(|(_, checkpoint)| checkpoint)
    }

    fn tree_state_lookup(&self, height: BlockHeight) -> Option<TreeStateLookup> {
        let predecessor = self
            .history_bounds
            .first_available_height()
            .value()
            .saturating_sub(1);
        if height > self.chain_epoch.visible_tip_height || height.value() < predecessor {
            return None;
        }
        if let Some(checkpoint) = self.checkpoints.get(&height.value()) {
            return Some(TreeStateLookup::Stored(ServedTreeState::from(checkpoint)));
        }
        let seed = self.checkpoint_below(height)?.clone();
        TreeStateReplay::new(seed, height).map(TreeStateLookup::Replay)
    }
}

/// A [`CanonicalReader`] over one in-memory fence.
#[derive(Clone)]
pub(crate) struct InMemoryCanonicalReader {
    snapshot: Arc<Snapshot>,
}

#[async_trait]
impl CanonicalReader for InMemoryCanonicalReader {
    fn network(&self) -> Network {
        self.snapshot.network
    }

    fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
        Some(&self.snapshot.schedule)
    }

    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.snapshot.raw_blob_retention
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.snapshot.fence
    }

    fn chain_epoch(&self) -> ChainEpoch {
        self.snapshot.chain_epoch
    }

    fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.snapshot.history_bounds
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        Ok(self
            .snapshot
            .blocks
            .get(&height.value())
            .map(|block| block.header.clone()))
    }

    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, StorageReadError> {
        Ok(match self.snapshot.block_by_hash(block_hash) {
            Some(block) => BlockHashLookup::Resolved(BlockId::new(
                block.header.height,
                block.header.block_hash,
            )),
            None if self.snapshot.displaced.contains(&block_hash) => {
                BlockHashLookup::NotInBestChain
            }
            None => BlockHashLookup::NotIndexed,
        })
    }

    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        Ok(self
            .snapshot
            .block_by_hash(block_hash)
            .map(|block| block.header.clone()))
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        Ok(self
            .snapshot
            .block_by_hash(block_hash)
            .map(|block| block.compact_block.clone()))
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        Ok(self
            .snapshot
            .blocks
            .get(&height.value())
            .map(|block| block.compact_block.clone()))
    }

    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        (range.start.value()..=range.end.value())
            .map(|height| {
                self.snapshot
                    .blocks
                    .get(&height)
                    .map(|block| block.compact_block.clone())
                    .ok_or(StorageReadError::ArtifactUnavailable {
                        family: ArtifactFamily::CompactBlock,
                        key: ArtifactKey::BlockHeight(BlockHeight::new(height)),
                    })
            })
            .collect()
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        let mut blocks = self.compact_blocks_in_range(range).await?;
        if matches!(order, HeightOrder::Descending) {
            blocks.reverse();
        }
        let mut kept_bytes = 0_usize;
        let mut kept = Vec::new();
        for block in blocks {
            let block_bytes = BLOCK_BYTES.saturating_add(block.transactions().len());
            if !kept.is_empty() && kept_bytes.saturating_add(block_bytes) > max_bytes {
                break;
            }
            kept_bytes = kept_bytes.saturating_add(block_bytes);
            kept.push(block);
        }
        kept.sort_by_key(CompactBlockArtifact::height);
        Ok(kept)
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError> {
        Ok(self
            .snapshot
            .raw_blob_retention
            .retains_block_blobs()
            .then(|| self.snapshot.blocks.get(&height.value()))
            .flatten()
            .map(|block| block.blob.clone()))
    }

    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError> {
        let mut blobs = Vec::new();
        for height in range.start.value()..=range.end.value() {
            blobs.push(self.block_blob_at(BlockHeight::new(height)).await?);
        }
        Ok(blobs)
    }

    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError> {
        let Some(transaction) = self.snapshot.transactions.get(&transaction_id) else {
            if self
                .snapshot
                .history_bounds
                .intentionally_excludes(BlockHeight::new(1))
            {
                return Err(StorageReadError::CanonicalHistoryUnavailable {
                    first_available_height: self.snapshot.history_bounds.first_available_height(),
                });
            }
            return Ok(None);
        };
        Ok(Some(IndexedTransaction {
            location: transaction.location,
            block_header: self
                .snapshot
                .blocks
                .get(&transaction.location.block_height.value())
                .map(|block| block.header.clone()),
            transaction_blob: self.retained_blob(transaction),
        }))
    }

    async fn mined_transactions(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<Vec<Option<IndexedTransaction>>, StorageReadError> {
        Ok(transaction_ids
            .iter()
            .map(|transaction_id| {
                let transaction = self.snapshot.transactions.get(transaction_id)?;
                Some(IndexedTransaction {
                    location: transaction.location,
                    block_header: self
                        .snapshot
                        .blocks
                        .get(&transaction.location.block_height.value())
                        .map(|block| block.header.clone()),
                    transaction_blob: self.retained_blob(transaction),
                })
            })
            .collect())
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError> {
        let unavailable = |family| StorageReadError::ArtifactUnavailable {
            family,
            key: ArtifactKey::TransactionId(transaction_id),
        };
        let transaction = self
            .snapshot
            .transactions
            .get(&transaction_id)
            .ok_or_else(|| unavailable(ArtifactFamily::TransactionLocation))?;
        self.retained_blob(transaction)
            .ok_or_else(|| unavailable(ArtifactFamily::TransactionBlob))
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        Ok(self
            .snapshot
            .checkpoints
            .range(..=height.value())
            .next_back()
            .map(|(_, checkpoint)| ServedTreeState::from(checkpoint)))
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        Ok(self.snapshot.tree_state_lookup(height))
    }

    async fn replay_tree_state(
        &self,
        replay: TreeStateReplay,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        // Nothing proves where the seed `replay` names came from, so the
        // engine replays from the seed it holds below the height.
        let replay = match self.snapshot.tree_state_lookup(replay.height()) {
            None => return Ok(None),
            Some(TreeStateLookup::Stored(tree_state)) => {
                return Ok(Some(TreeStateReplayOutcome::Stored(tree_state)));
            }
            Some(TreeStateLookup::Replay(own)) => own,
        };
        let checkpoint = replay_tree(
            replay.seed(),
            replay.height(),
            &self.snapshot.activations,
            |height| {
                self.snapshot
                    .blocks
                    .get(&height.value())
                    .map(|block| &block.compact_block)
            },
        )
        .map_err(|source| StorageReadError::Unavailable { source })?;
        Ok(Some(TreeStateReplayOutcome::Replayed {
            replay: Box::new(replay),
            tree_state: ServedTreeState::from(checkpoint),
        }))
    }

    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError> {
        range
            .into_iter()
            .map(|index| {
                self.snapshot
                    .subtree_roots
                    .get(&(protocol_key(range.protocol), index.value()))
                    .cloned()
                    .ok_or(StorageReadError::ArtifactUnavailable {
                        family: ArtifactFamily::SubtreeRoot,
                        key: ArtifactKey::SubtreeRootIndex {
                            protocol: range.protocol,
                            index,
                        },
                    })
            })
            .collect()
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError> {
        // A cursor's own family applies to the events after it.
        let (family, after) = match from_cursor {
            None => (family, 0),
            Some(cursor) => {
                let (cursor_family, event_sequence) = decode_cursor(&cursor)?;
                if event_sequence.saturating_add(1) < self.snapshot.oldest_retained_sequence {
                    return Err(StorageReadError::ChainEventCursorExpired {
                        event_sequence,
                        oldest_retained_sequence: self.snapshot.oldest_retained_sequence,
                    });
                }
                (cursor_family, event_sequence)
            }
        };
        Ok(self
            .snapshot
            .events
            .iter()
            .filter(|recorded| recorded.event_sequence > after)
            .filter_map(|recorded| {
                let event = match family {
                    ChainEventStreamFamily::Visible => recorded.event.clone(),
                    ChainEventStreamFamily::Settled => ChainEvent::ChainCommitted {
                        committed: ChainEpochCommitted {
                            chain_epoch: recorded.chain_epoch,
                            block_range: recorded.newly_settled?,
                        },
                    },
                };
                Some(ChainEventEnvelope {
                    cursor: encode_cursor(family, recorded.event_sequence),
                    event_sequence: recorded.event_sequence,
                    chain_epoch: recorded.chain_epoch,
                    settled_tip_height: recorded.chain_epoch.settled_tip_height,
                    event,
                })
            })
            .take(usize::try_from(max_events.get()).unwrap_or(usize::MAX))
            .collect())
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError> {
        Ok(match start {
            EventStreamStartPosition::EarliestRetained => ChainEventStreamResume {
                cursor: None,
                family: requested_family,
            },
            EventStreamStartPosition::LiveTail => ChainEventStreamResume {
                cursor: Some(encode_cursor(
                    requested_family,
                    self.snapshot.fence.chain_event_sequence(),
                )),
                family: requested_family,
            },
            EventStreamStartPosition::AfterCursor(cursor) => {
                let (family, _) = decode_cursor(&cursor)?;
                // The default family resumes a cursor of either family.
                if requested_family != ChainEventStreamFamily::Visible && family != requested_family
                {
                    return Err(StorageReadError::ChainEventCursorInvalid {
                        reason: "cursor names another stream family",
                    });
                }
                ChainEventStreamResume {
                    cursor: Some(cursor),
                    family,
                }
            }
        })
    }
}

impl InMemoryCanonicalReader {
    fn retained_blob(&self, transaction: &StoredTransaction) -> Option<TransactionBlobArtifact> {
        self.snapshot
            .raw_blob_retention
            .retains_transaction_blobs()
            .then(|| transaction.blob.clone())
            .flatten()
    }
}

/// The commitment trees after every block of `chain` through `tip`, from the
/// empty trees before height 1, taking the trees `chain` declares where it
/// declares them.
fn trees_after_each_block(
    chain: &ChainFixture,
    tip: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> eyre::Result<BTreeMap<u32, CommitmentTreeCheckpoint>> {
    let first = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("the chain starts at height 1"))?;
    let mut checkpoint = CommitmentTreeCheckpoint::new(
        BlockId::new(BlockHeight::new(0), first.parent_hash),
        0,
        CommitmentTreeFrontiers::default(),
    );
    let mut trees_after = BTreeMap::from([(0, checkpoint.clone())]);
    let compact_blocks = chain.compact_block_artifacts();
    for compact_block in compact_blocks.iter().filter(|block| block.height() <= tip) {
        let height = compact_block.height();
        checkpoint = match chain.commitment_tree_frontiers_at(height) {
            Some(frontiers) => CommitmentTreeCheckpoint::new(
                BlockId::new(height, compact_block.block_hash()),
                compact_block.time(),
                frontiers.clone(),
            ),
            None => replay_tree(&checkpoint, height, activations, |replayed| {
                (replayed == height).then_some(compact_block)
            })
            .map_err(|error| eyre!("fixture trees at height {}: {error}", height.value()))?,
        };
        trees_after.insert(height.value(), checkpoint.clone());
    }
    Ok(trees_after)
}

/// Appends the note commitments of each compact block `block_at` returns
/// above `seed` through `height`, and returns the trees after `height`.
fn replay_tree<'block>(
    seed: &CommitmentTreeCheckpoint,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
    block_at: impl Fn(BlockHeight) -> Option<&'block CompactBlockArtifact>,
) -> Result<CommitmentTreeCheckpoint, Box<dyn StdError + Send + Sync>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        seed.block_id.height,
        &seed.frontiers,
        activations,
    )?;
    let mut last = None;
    for replayed in seed.block_id.height.value().saturating_add(1)..=height.value() {
        let block = block_at(BlockHeight::new(replayed))
            .ok_or_else(|| format!("compact block {replayed} is not held"))?;
        let mut sapling = Vec::new();
        let mut orchard = Vec::new();
        let mut ironwood = Vec::new();
        for transaction in block.transactions() {
            sapling.extend(
                transaction
                    .data
                    .sapling_outputs
                    .iter()
                    .map(|output| output.commitment),
            );
            orchard.extend(
                transaction
                    .data
                    .orchard_actions
                    .iter()
                    .map(|action| action.commitment),
            );
            ironwood.extend(
                transaction
                    .data
                    .ironwood_actions
                    .iter()
                    .map(|action| action.commitment),
            );
        }
        accumulator.append_block_commitments(block.height(), &sapling, &orchard, &ironwood)?;
        last = Some(block);
    }
    let block = last.ok_or("a replay reconstructs at least one block")?;
    Ok(CommitmentTreeCheckpoint::new(
        BlockId::new(block.height(), block.block_hash()),
        block.time(),
        accumulator.cross_checked_frontiers()?,
    ))
}

fn tree_checkpoint(
    trees_after: &BTreeMap<u32, CommitmentTreeCheckpoint>,
    height: u32,
) -> eyre::Result<CommitmentTreeCheckpoint> {
    trees_after
        .get(&height)
        .cloned()
        .ok_or_else(|| eyre!("no commitment trees after height {height}"))
}

fn stored_block(chain: &ChainFixture, height: BlockHeight) -> eyre::Result<StoredBlock> {
    let block = chain
        .block_at(height)
        .ok_or_else(|| eyre!("the chain has no block at height {}", height.value()))?;
    let compact_block = chain
        .compact_block_artifacts()
        .into_iter()
        .find(|compact_block| compact_block.height() == height)
        .ok_or_else(|| {
            eyre!(
                "the chain has no compact block at height {}",
                height.value()
            )
        })?;
    Ok(StoredBlock {
        header: block.block_header_artifact(),
        compact_block,
        blob: block.block_blob_artifact(),
    })
}

fn block_id(chain: &ChainFixture, height: BlockHeight) -> eyre::Result<BlockId> {
    chain
        .block_at(height)
        .map(|block| BlockId::new(block.height, block.hash))
        .ok_or_else(|| eyre!("the chain has no block at height {}", height.value()))
}

/// The epoch `id` that shows the tip of `blocks` and settles at `settled`.
fn epoch_at(
    id: u64,
    network: Network,
    blocks: &BTreeMap<u32, StoredBlock>,
    settled: BlockId,
) -> eyre::Result<ChainEpoch> {
    let tip = blocks
        .values()
        .next_back()
        .ok_or_else(|| eyre!("an epoch shows at least one block"))?;
    let trees = tip.compact_block.chain_metadata();
    Ok(ChainEpoch {
        id: ChainEpochId::new(id),
        network,
        visible_tip_height: tip.header.height,
        visible_tip_hash: tip.header.block_hash,
        settled_tip_height: settled.height,
        settled_tip_hash: settled.hash,
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::new(
            trees.sapling_commitment_tree_size,
            trees.orchard_commitment_tree_size,
            trees.ironwood_commitment_tree_size,
        ),
        created_at: UnixTimestampMillis::new(id.saturating_mul(1_000)),
    })
}

/// The fence of the commit that made epoch `id` visible at `tip`. Each commit
/// records one chain event, so its sequence equals the epoch id.
fn fence_at(id: u64, tip: BlockId, visible_block_count: u64) -> CanonicalEventFence {
    CanonicalEventFence::new(
        ChainEpochId::new(id),
        id,
        tip,
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            visible_block_count,
            tip.hash.as_bytes(),
        ),
    )
}

fn protocol_key(protocol: ShieldedProtocol) -> u8 {
    match protocol {
        ShieldedProtocol::Sapling => 0,
        ShieldedProtocol::Orchard => 1,
        ShieldedProtocol::Ironwood => 2,
        _ => u8::MAX,
    }
}

fn encode_cursor(family: ChainEventStreamFamily, event_sequence: u64) -> StreamCursorTokenV1 {
    let mut encoded = Vec::with_capacity(CURSOR_LEN);
    encoded.extend_from_slice(CURSOR_MAGIC);
    encoded.push(match family {
        ChainEventStreamFamily::Visible => 0,
        ChainEventStreamFamily::Settled => 1,
    });
    encoded.extend_from_slice(&event_sequence.to_be_bytes());
    StreamCursorTokenV1::from_bytes(encoded)
}

fn decode_cursor(
    cursor: &StreamCursorTokenV1,
) -> Result<(ChainEventStreamFamily, u64), StorageReadError> {
    let malformed = || StorageReadError::ChainEventCursorInvalid {
        reason: "cursor was not issued by this reader",
    };
    let encoded = cursor.as_bytes();
    if encoded.len() != CURSOR_LEN || !encoded.starts_with(CURSOR_MAGIC) {
        return Err(malformed());
    }
    let family = match encoded.get(4) {
        Some(0) => ChainEventStreamFamily::Visible,
        Some(1) => ChainEventStreamFamily::Settled,
        _ => return Err(malformed()),
    };
    let sequence = encoded
        .get(5..CURSOR_LEN)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .ok_or_else(malformed)?;
    Ok((family, u64::from_be_bytes(sequence)))
}

mod canonical_cases {
    super::canonical_reader_conformance!(super::InMemoryReaderHarness);
}
