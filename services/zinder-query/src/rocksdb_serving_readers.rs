//! The `RocksDB` implementation of the serving readers.
//!
//! Each reader owns a shared handle to one admitted secondary that its owner
//! no longer catches up, and runs every storage read on the blocking pool
//! inside the caller's tracing span, or on the calling thread when
//! [`read_on_blocking_thread`](crate::read_on_blocking_thread) drives the
//! read there. A secondary uses direct I/O, so a cache miss is a synchronous
//! `pread`, which must never hold a reactor thread.
//!
//! A read that finds a table file of its secondary deleted by the writer's
//! compaction fails with [`StorageReadError::ReaderInvalidated`]. Every other
//! storage failure is [`StorageReadError::Unavailable`].

use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::{Arc, atomic::AtomicBool},
};

use async_trait::async_trait;
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    CanonicalHistoryBounds, ChainEpoch, CompactBlockArtifact, Network, NetworkUpgradeActivations,
    SubtreeRootArtifact, SubtreeRootRange, TransactionBlobArtifact, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentOutpointSpendStatus,
};
use zinder_store::{
    ArtifactFamily, BlockHashLookup, CanonicalActivationSchedule, CanonicalEventFence,
    CanonicalStoreError, ChainEventEnvelope, ChainEventHistoryRequest, ChainEventStreamFamily,
    ChainEventStreamResume, EncodedCompactBlockRange, EncodedCompactBlockRangeRequest,
    EventStreamStartPosition, HeightOrder, RawBlobRetention, RocksDbCanonicalSecondary,
    ServedTreeState, StreamCursorTokenV1, TreeStateLookup, TreeStateReplay, TreeStateReplayOutcome,
};
use zinder_wallet_projection::{
    WalletAddressTransactionHistoryPage, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletAddressUnspentOutputsPage, WalletProjectionReadyEvidence,
};
use zinder_wallet_rocksdb::{RocksDbWalletError, RocksDbWalletSecondary};

use crate::{
    ArtifactKey, CanonicalReader, IndexedTransaction, SpendStatusReadEnd, StorageReadError,
    WalletProjectionReader, serving_readers::drives_blocking_read,
};

/// A [`CanonicalReader`] over one admitted `RocksDB` canonical secondary.
///
/// The reader shares the secondary, so its owner takes the secondary back,
/// for example to catch it up for a later fence, only after every reader and
/// every read in flight has released it.
#[derive(Clone)]
pub struct RocksDbCanonicalReader {
    secondary: Arc<RocksDbCanonicalSecondary>,
    chain_epoch: ChainEpoch,
}

impl RocksDbCanonicalReader {
    /// Admits `secondary` at its current fence, reading the chain epoch it
    /// serves there on the calling thread.
    ///
    /// The caller must not catch the secondary up while the reader holds it.
    /// Taking ownership of the handle, or of the only other `Arc` to it,
    /// makes that structural.
    ///
    /// # Errors
    ///
    /// Returns the [`StorageReadError`] that kept the chain epoch from being
    /// read.
    pub fn new(
        secondary: impl Into<Arc<RocksDbCanonicalSecondary>>,
    ) -> Result<Self, StorageReadError> {
        let secondary = secondary.into();
        let chain_epoch = secondary.chain_epoch().map_err(canonical_read_error)?;
        Ok(Self {
            secondary,
            chain_epoch,
        })
    }

    /// Returns the secondary this reader reads, for its owner's lifecycle
    /// work such as memory accounting.
    #[must_use]
    pub fn secondary(&self) -> &RocksDbCanonicalSecondary {
        &self.secondary
    }

    /// Returns the secondary once nothing else holds it, such as another
    /// clone of this reader or a read still in flight, so its owner can
    /// catch it up or close it; otherwise returns the reader.
    ///
    /// # Errors
    ///
    /// Returns `self` while anything else shares the secondary.
    pub fn into_secondary(self) -> Result<RocksDbCanonicalSecondary, Self> {
        let chain_epoch = self.chain_epoch;
        Arc::try_unwrap(self.secondary).map_err(|secondary| Self {
            secondary,
            chain_epoch,
        })
    }

    /// Runs `read` against the secondary off the async runtime.
    async fn read<Output>(
        &self,
        read: impl FnOnce(&RocksDbCanonicalSecondary) -> Result<Output, CanonicalStoreError>
        + Send
        + 'static,
    ) -> Result<Output, StorageReadError>
    where
        Output: Send + 'static,
    {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || read(&secondary).map_err(canonical_read_error)).await
    }

    /// Reads where the served chain mines `transaction_id`, answering `None`
    /// only for a proven absence.
    ///
    /// # Errors
    ///
    /// Returns [`StorageReadError::CanonicalHistoryUnavailable`] for a miss
    /// below a checkpointed store's history floor.
    pub async fn mined_transaction_location(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<zinder_core::TransactionLocation>, StorageReadError> {
        self.read(move |secondary| secondary.transaction_location_or_proven_absence(transaction_id))
            .await
    }
}

/// Where the served chain mines a transaction and the digest of the bytes
/// ingest committed for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MinedTransactionDigest {
    /// The transaction's canonical location.
    pub location: zinder_core::TransactionLocation,
    /// The digest of the transaction's serialized bytes.
    pub serialized_bytes_digest: zinder_core::SerializedBytesDigest,
}

impl MinedTransactionDigest {
    /// Reads the exact byte commitment from the indexed transaction location.
    ///
    /// # Errors
    ///
    /// Returns an artifact corruption error when the location has no digest.
    pub fn from_location(
        location: zinder_core::TransactionLocation,
    ) -> Result<Self, crate::QueryError> {
        let serialized_bytes_digest = location
            .serialized_bytes_sha256
            .map(zinder_core::SerializedBytesDigest::from_bytes)
            .ok_or_else(|| crate::QueryError::ArtifactCorrupt {
                family: ArtifactFamily::TransactionLocation,
                reason: "canonical transaction location has no serialized byte digest".to_owned(),
            })?;
        Ok(Self {
            location,
            serialized_bytes_digest,
        })
    }

    /// Checks the node's containing block and the exact serialized bytes.
    ///
    /// # Errors
    ///
    /// Returns a node availability error for a different block and an artifact
    /// corruption error for a byte digest mismatch.
    pub fn verify_node_bytes(
        self,
        block: zinder_core::BlockId,
        bytes: &[u8],
    ) -> Result<(), crate::QueryError> {
        if block != zinder_core::BlockId::new(self.location.block_height, self.location.block_hash)
        {
            return Err(crate::QueryError::Node(
                zinder_source::SourceError::NodeUnavailable {
                    reason:
                        "node does not mine the transaction in the served block; retry with backoff"
                            .to_owned(),
                },
            ));
        }
        if zinder_core::SerializedBytesDigest::from_serialized_bytes(bytes)
            != self.serialized_bytes_digest
        {
            return Err(crate::QueryError::ArtifactCorrupt {
                family: ArtifactFamily::TransactionLocation,
                reason:
                    "node transaction bytes differ from the bytes committed to the served chain"
                        .to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl CanonicalReader for RocksDbCanonicalReader {
    fn network(&self) -> Network {
        self.secondary.network()
    }

    fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
        self.secondary.network_upgrade_schedule()
    }

    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.secondary.raw_blob_retention()
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.secondary.event_fence()
    }

    fn chain_epoch(&self) -> ChainEpoch {
        self.chain_epoch
    }

    fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.secondary.history_bounds()
    }

    fn history_predecessor_digest(&self) -> Option<[u8; 32]> {
        Some(self.secondary.history_predecessor_digest())
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.block_header_at(height))
            .await
    }

    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, StorageReadError> {
        self.read(move |secondary| secondary.block_hash_lookup(block_hash))
            .await
    }

    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.read(
            move |secondary| match secondary.block_hash_lookup(block_hash)? {
                BlockHashLookup::Resolved(block_id) => secondary.block_header_at(block_id.height),
                BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => Ok(None),
            },
        )
        .await
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.read(
            move |secondary| match secondary.block_hash_lookup(block_hash)? {
                BlockHashLookup::Resolved(block_id) => secondary
                    .compact_block_at(block_id.height)?
                    .map(Some)
                    .ok_or(CanonicalStoreError::ArtifactUnavailable {
                        family: ArtifactFamily::CompactBlock,
                        height: block_id.height,
                    }),
                BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => Ok(None),
            },
        )
        .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.compact_block_at(height))
            .await
    }

    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.compact_blocks_in_range(range))
            .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.compact_blocks_in_range_up_to(range, order, max_bytes))
            .await
    }

    async fn encoded_compact_block_range_up_to(
        &self,
        request: EncodedCompactBlockRangeRequest,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Option<EncodedCompactBlockRange>, StorageReadError> {
        self.read(move |secondary| {
            secondary.encoded_compact_block_range_up_to(request, cancel.as_deref())
        })
        .await
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.block_blob_at(height))
            .await
    }

    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError> {
        self.read(move |secondary| secondary.block_blobs_in_range(range))
            .await
    }

    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError> {
        self.read(move |secondary| {
            // READY admission requires transaction-location coverage to equal
            // the authenticated source count for every retention mode, and
            // live append and replacement update every retained family in
            // one batch, so a miss over complete history is a proven absence.
            let Some((location, transaction_blob)) =
                secondary.transaction_location_and_blob_or_proven_absence(transaction_id)?
            else {
                return Ok(None);
            };
            Ok(Some(IndexedTransaction {
                location,
                block_header: secondary.block_header_at(location.block_height)?,
                transaction_blob,
            }))
        })
        .await
    }

    async fn mined_transactions(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<Vec<Option<IndexedTransaction>>, StorageReadError> {
        if transaction_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = transaction_ids.to_vec();
        self.read(move |secondary| {
            let located = secondary.transaction_locations_and_blobs(&ids)?;
            let heights: Vec<BlockHeight> = located
                .iter()
                .filter_map(|entry| entry.as_ref().map(|(location, _)| location.block_height))
                .collect();
            let mut headers = secondary.block_headers_at(&heights)?.into_iter();
            Ok(located
                .into_iter()
                .map(|entry| {
                    entry.map(|(location, transaction_blob)| IndexedTransaction {
                        location,
                        block_header: headers.next().flatten(),
                        transaction_blob,
                    })
                })
                .collect())
        })
        .await
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError> {
        let unavailable = move |family| StorageReadError::ArtifactUnavailable {
            family,
            key: ArtifactKey::TransactionId(transaction_id),
        };
        let blob = self
            .read(move |secondary| {
                let Some((_, blob)) = secondary.transaction_location_and_blob(transaction_id)?
                else {
                    return Ok(Err(ArtifactFamily::TransactionLocation));
                };
                Ok(blob.ok_or(ArtifactFamily::TransactionBlob))
            })
            .await?;
        blob.map_err(unavailable)
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        self.read(move |secondary| secondary.tree_state_checkpoint_at_or_before(height))
            .await
    }

    async fn stored_tree_state_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        admitted_activations(&self.secondary)?;
        self.read(move |secondary| secondary.stored_tree_state_at(height))
            .await
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || {
            secondary
                .tree_state_lookup(height, admitted_activations(&secondary)?)
                .map_err(canonical_read_error)
        })
        .await
    }

    async fn tree_state_lookup_until_cancelled(
        &self,
        height: BlockHeight,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || {
            secondary
                .tree_state_lookup_until_cancelled(
                    height,
                    admitted_activations(&secondary)?,
                    || cancelled.load(std::sync::atomic::Ordering::Relaxed),
                )
                .map_err(canonical_read_error)
        })
        .await
    }

    async fn replay_tree_state(
        &self,
        replay: TreeStateReplay,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || {
            secondary
                .replay_tree_state(&replay, admitted_activations(&secondary)?)
                .map_err(canonical_read_error)
        })
        .await
    }

    async fn replay_tree_state_until_cancelled(
        &self,
        replay: TreeStateReplay,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || {
            secondary
                .replay_tree_state_until_cancelled(
                    &replay,
                    admitted_activations(&secondary)?,
                    || cancelled.load(std::sync::atomic::Ordering::Relaxed),
                )
                .map_err(canonical_read_error)
        })
        .await
    }

    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError> {
        self.read(move |secondary| secondary.subtree_roots(range))
            .await
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError> {
        self.read(move |secondary| {
            secondary.wallet_chain_event_history(ChainEventHistoryRequest::new_for_family(
                from_cursor.as_ref(),
                family,
                max_events,
            ))
        })
        .await
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError> {
        self.read(move |secondary| {
            secondary.resolve_wallet_chain_event_stream_start(&start, requested_family)
        })
        .await
    }
}

/// A [`WalletProjectionReader`] over one admitted `RocksDB` wallet secondary.
///
/// The reader shares the secondary, so its owner takes the secondary back,
/// for example to catch it up for a later source, only after every reader and
/// every read in flight has released it.
#[derive(Clone)]
pub struct RocksDbWalletReader {
    secondary: Arc<RocksDbWalletSecondary>,
}

impl RocksDbWalletReader {
    /// Admits `secondary` at its current READY source.
    ///
    /// The caller must not catch the secondary up while the reader holds it.
    #[must_use]
    pub fn new(secondary: impl Into<Arc<RocksDbWalletSecondary>>) -> Self {
        Self {
            secondary: secondary.into(),
        }
    }

    /// Runs `read` against the secondary off the async runtime.
    async fn read<Output>(
        &self,
        read: impl FnOnce(&RocksDbWalletSecondary) -> Result<Output, RocksDbWalletError>
        + Send
        + 'static,
    ) -> Result<Output, StorageReadError>
    where
        Output: Send + 'static,
    {
        let secondary = Arc::clone(&self.secondary);
        off_reactor(move || read(&secondary).map_err(wallet_read_error)).await
    }
}

#[async_trait]
impl WalletProjectionReader for RocksDbWalletReader {
    fn network(&self) -> Network {
        self.secondary.network()
    }

    fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.secondary.ready_evidence()
    }

    async fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, StorageReadError> {
        self.read(move |secondary| {
            secondary.address_unspent_outputs_page_from_height(
                address_script_hash,
                start_height,
                after,
                page_size,
            )
        })
        .await
    }

    async fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, StorageReadError> {
        self.read(move |secondary| {
            secondary.address_transaction_history_range_page(
                address_script_hash,
                height_range,
                after,
                page_size,
            )
        })
        .await
    }

    async fn total_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
    ) -> Result<u64, StorageReadError> {
        self.read(move |secondary| {
            addresses.into_iter().try_fold(0_u64, |total, address| {
                Ok(total.saturating_add(secondary.address_balance(address)?))
            })
        })
        .await
    }

    async fn outpoint_spend_statuses(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        end: SpendStatusReadEnd,
    ) -> Result<Vec<TransparentOutpointSpendStatus>, StorageReadError> {
        self.read(move |secondary| {
            let mut statuses = Vec::with_capacity(outpoints.len());
            for outpoint in outpoints {
                let status = outpoint_spend_status(secondary, outpoint)?;
                statuses.push(status);
                if end == SpendStatusReadEnd::FirstNotFound
                    && status == TransparentOutpointSpendStatus::NotFound
                {
                    break;
                }
            }
            Ok(statuses)
        })
        .await
    }
}

/// Reads how `secondary` holds `outpoint`: unspent, spent, or not at all.
fn outpoint_spend_status(
    secondary: &RocksDbWalletSecondary,
    outpoint: TransparentOutPoint,
) -> Result<TransparentOutpointSpendStatus, RocksDbWalletError> {
    if secondary.find_unspent_output(outpoint)?.is_some() {
        return Ok(TransparentOutpointSpendStatus::Unspent);
    }
    Ok(secondary.find_spent_output(outpoint)?.map_or(
        TransparentOutpointSpendStatus::NotFound,
        |spent| TransparentOutpointSpendStatus::SpentBy {
            transaction_id: spent.spent_at.transaction_id,
            input_index: spent.input_index,
            block: spent.spent_at.block,
        },
    ))
}

/// Runs `read` on the blocking pool inside the caller's tracing span, or on
/// this thread when [`read_on_blocking_thread`](crate::read_on_blocking_thread)
/// drives the read here.
async fn off_reactor<Output>(
    read: impl FnOnce() -> Result<Output, StorageReadError> + Send + 'static,
) -> Result<Output, StorageReadError>
where
    Output: Send + 'static,
{
    if drives_blocking_read() {
        return read();
    }
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(read))
        .await
        .unwrap_or_else(|join_error| {
            Err(StorageReadError::ReadTaskFailed {
                reason: join_error.to_string(),
            })
        })
}

/// Returns the network-upgrade activations the canonical writer admitted
/// with `secondary`'s fence, which tree states are derived under.
fn admitted_activations(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<&NetworkUpgradeActivations, StorageReadError> {
    secondary
        .network_upgrade_activations()
        .ok_or_else(|| StorageReadError::Unavailable {
            source: Box::new(ScheduleNotAdmitted),
        })
}

/// A tree-state read on a secondary that carries no writer-admitted
/// network-upgrade schedule.
#[derive(Debug, thiserror::Error)]
#[error("canonical reader carries no writer-admitted network-upgrade schedule")]
struct ScheduleNotAdmitted;

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "CanonicalStoreError is non-exhaustive; every failure a request cannot cause is a storage failure"
)]
fn canonical_read_error(error: CanonicalStoreError) -> StorageReadError {
    if error.is_missing_table_file() {
        return StorageReadError::ReaderInvalidated {
            source: Box::new(error),
        };
    }
    match error {
        CanonicalStoreError::ArtifactUnavailable { family, height } => {
            StorageReadError::ArtifactUnavailable {
                family,
                key: ArtifactKey::BlockHeight(height),
            }
        }
        CanonicalStoreError::SubtreeRootUnavailable {
            protocol,
            subtree_index,
        } => StorageReadError::ArtifactUnavailable {
            family: ArtifactFamily::SubtreeRoot,
            key: ArtifactKey::SubtreeRootIndex {
                protocol,
                index: subtree_index,
            },
        },
        CanonicalStoreError::CanonicalHistoryUnavailable {
            first_available_height,
        } => StorageReadError::CanonicalHistoryUnavailable {
            first_available_height,
        },
        CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        } => StorageReadError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        },
        CanonicalStoreError::CanonicalEventCursorMalformed { reason } => {
            StorageReadError::ChainEventCursorInvalid { reason }
        }
        CanonicalStoreError::CanonicalEventCursorUnknownVersion { .. } => {
            StorageReadError::ChainEventCursorInvalid {
                reason: "cursor version is unsupported",
            }
        }
        CanonicalStoreError::ArtifactCorrupt { family, reason } => {
            StorageReadError::ArtifactCorrupt { family, reason }
        }
        CanonicalStoreError::ReplayCancelled => StorageReadError::Cancelled,
        other => StorageReadError::Unavailable {
            source: Box::new(other),
        },
    }
}

fn wallet_read_error(error: RocksDbWalletError) -> StorageReadError {
    if error.is_missing_table_file() {
        StorageReadError::ReaderInvalidated {
            source: Box::new(error),
        }
    } else {
        StorageReadError::Unavailable {
            source: Box::new(error),
        }
    }
}
