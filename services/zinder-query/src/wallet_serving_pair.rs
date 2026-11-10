//! Immutable, exact-fence read pairs for wallet serving.

use std::{fmt, io, num::NonZeroU16, sync::Arc};

use thiserror::Error;
use zinder_core::{
    BlockBlobArtifact, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId, ChainEpoch,
    CommitmentTreeCheckpoint, CompactBlockArtifact, Network, SubtreeRootArtifact, SubtreeRootRange,
    TransactionBlobArtifact, TransactionId, TransactionLocation, TransparentAddressScriptHash,
};
use zinder_store::{
    BlockHashLookup, CanonicalActivationSchedule, CanonicalEventFence, CanonicalStoreError,
    ChainEventEnvelope, ChainEventHistoryRequest, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, RawBlobRetention, RocksDbCanonicalSecondary,
};
use zinder_wallet_projection::{
    WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletCanonicalSourceIdentity,
    WalletProjectionReadyEvidence, WalletProjectionSourcePosition,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletError, RocksDbWalletSecondary, WalletAddressTransactionHistoryPage,
    WalletAddressUnspentOutputsPage,
};

use crate::QueryError;

/// Read-only canonical data held at one immutable admitted fence.
///
/// Implementations must never catch up or otherwise mutate their observed
/// fence while a caller holds the same instance in a [`WalletServingReadPair`].
pub trait CanonicalReader: Send + Sync + 'static {
    /// Returns the writer-admitted network-upgrade schedule observed with
    /// this fence, or `None` before the writer's first admission.
    fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule>;

    /// Returns the persisted raw-blob retention authenticated at admission.
    fn raw_blob_retention(&self) -> RawBlobRetention;

    /// Returns the immutable network admitted by this canonical reader.
    fn network(&self) -> Network;

    /// Returns the exact admitted canonical event fence.
    fn event_fence(&self) -> CanonicalEventFence;

    /// Reads the visible chain epoch at the admitted fence.
    fn chain_epoch(&self) -> Result<ChainEpoch, CanonicalStoreError>;

    /// Reads one retained canonical epoch by its exact identifier.
    fn chain_epoch_at(
        &self,
        epoch_id: zinder_core::ChainEpochId,
    ) -> Result<ChainEpoch, CanonicalStoreError>;

    /// Reads one canonical header by height.
    fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError>;

    /// Resolves a block hash through the admitted canonical best-chain index.
    fn block_hash_lookup(
        &self,
        block_hash: zinder_core::BlockHash,
    ) -> Result<BlockHashLookup, CanonicalStoreError>;

    /// Reads one compact block by height.
    fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError>;

    /// Reads an inclusive compact-block range.
    fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError>;

    /// Reads one retained full block blob by height.
    fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError>;

    /// Reads retained full block blobs for an inclusive height range.
    fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError>;

    /// Reads the canonical location of a transaction.
    fn transaction_location(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionLocation>, CanonicalStoreError>;

    /// Reads a raw transaction at an authenticated canonical location.
    fn transaction_blob(
        &self,
        location: TransactionLocation,
    ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError>;

    /// Reads the newest commitment-tree checkpoint at or below a height.
    fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError>;

    /// Reads a contiguous range of subtree roots.
    fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError>;

    /// Projects retained canonical transitions into authenticated wallet events.
    fn wallet_chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError>;

    /// Resolves an authenticated wallet chain-event subscription start.
    fn resolve_wallet_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, CanonicalStoreError>;
}

/// Read-only wallet projection state held at one immutable READY source identity.
///
/// Implementations must never follow a primary or mutate their observed READY
/// evidence while a caller holds the instance in a [`WalletServingReadPair`].
pub trait WalletProjectionReader: Send + Sync + 'static {
    /// Returns the immutable wallet network admitted from store control.
    fn network(&self) -> Network;

    /// Returns the complete READY evidence bound to this reader.
    fn ready_evidence(&self) -> &WalletProjectionReadyEvidence;

    /// Reads one bounded page of unspent outputs for an address.
    fn address_unspent_outputs_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError>;

    /// Reads one bounded page of unspent outputs at or above a creation-height
    /// lower bound.
    fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError>;

    /// Reads one bounded page of address-touching transaction history.
    fn address_transaction_history_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError>;

    /// Reads one bounded page of address-touching transactions within an
    /// inclusive height range.
    fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError>;

    /// Reads one address balance, treating an absent row as zero.
    fn address_balance(
        &self,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError>;
}

/// Typed reason why canonical and wallet readers cannot form one serving pair.
///
/// This error is intentionally distinct from [`QueryError`]. Runtime pair
/// publishers use it to classify replica lag, wallet projection lag, and
/// malformed admitted evidence before deciding readiness. Query construction
/// maps it to a fail-closed request error.
#[derive(Debug, Error)]
pub enum WalletServingAdmissionError {
    /// The independently admitted readers committed different networks.
    #[error("canonical and wallet readers have different admitted networks")]
    NetworkMismatch {
        /// Network committed by the canonical reader.
        canonical: Network,
        /// Network committed by the wallet reader.
        wallet: Network,
    },
    /// The canonical reader could not decode its visible epoch.
    #[error("canonical reader failed while validating a wallet-serving read pair")]
    CanonicalRead {
        /// Exact canonical storage failure.
        #[source]
        source: CanonicalStoreError,
    },
    /// The visible canonical epoch disagreed with the canonical READY fence.
    #[error("canonical READY epoch does not match its admitted event fence")]
    CanonicalFenceMismatch,
    /// The wallet READY identity did not exactly match canonical state.
    #[error("wallet READY evidence does not match the canonical fence and settlement boundary")]
    WalletSourceMismatch {
        /// Identity reconstructed from canonical READY state.
        canonical: Box<WalletCanonicalSourceIdentity>,
        /// Identity committed by wallet READY evidence.
        wallet: Box<WalletCanonicalSourceIdentity>,
    },
}

macro_rules! impl_canonical_read {
    ($store:ty) => {
        impl CanonicalReader for $store {
            fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
                self.network_upgrade_schedule()
            }

            fn raw_blob_retention(&self) -> RawBlobRetention {
                self.raw_blob_retention()
            }

            fn network(&self) -> Network {
                self.network()
            }

            fn event_fence(&self) -> CanonicalEventFence {
                self.event_fence()
            }

            fn chain_epoch(&self) -> Result<ChainEpoch, CanonicalStoreError> {
                self.chain_epoch()
            }

            fn chain_epoch_at(
                &self,
                epoch_id: zinder_core::ChainEpochId,
            ) -> Result<ChainEpoch, CanonicalStoreError> {
                self.chain_epoch_at(epoch_id)
            }

            fn block_header_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
                self.block_header_at(height)
            }

            fn block_hash_lookup(
                &self,
                block_hash: zinder_core::BlockHash,
            ) -> Result<BlockHashLookup, CanonicalStoreError> {
                self.block_hash_lookup(block_hash)
            }

            fn compact_block_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
                self.compact_block_at(height)
            }

            fn compact_blocks_in_range(
                &self,
                range: BlockHeightRange,
            ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
                self.compact_blocks_in_range(range)
            }

            fn block_blob_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError> {
                self.block_blob_at(height)
            }

            fn block_blobs_in_range(
                &self,
                range: BlockHeightRange,
            ) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError> {
                self.block_blobs_in_range(range)
            }

            fn transaction_location(
                &self,
                transaction_id: TransactionId,
            ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
                self.transaction_location(transaction_id)
            }

            fn transaction_blob(
                &self,
                location: TransactionLocation,
            ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
                self.transaction_blob(location)
            }

            fn tree_state_checkpoint_at_or_before(
                &self,
                height: BlockHeight,
            ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
                self.tree_state_checkpoint_at_or_before(height)
            }

            fn subtree_roots(
                &self,
                range: SubtreeRootRange,
            ) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
                self.subtree_roots(range)
            }

            fn wallet_chain_event_history(
                &self,
                request: ChainEventHistoryRequest<'_>,
            ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
                self.wallet_chain_event_history(request)
            }

            fn resolve_wallet_chain_event_stream_start(
                &self,
                start: &EventStreamStartPosition,
                requested_family: ChainEventStreamFamily,
            ) -> Result<ChainEventStreamResume, CanonicalStoreError> {
                self.resolve_wallet_chain_event_stream_start(start, requested_family)
            }
        }
    };
}

impl_canonical_read!(RocksDbCanonicalSecondary);

impl WalletProjectionReader for RocksDbWalletSecondary {
    fn network(&self) -> Network {
        self.network()
    }

    fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.ready_evidence()
    }

    fn address_unspent_outputs_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.address_unspent_outputs_page(address_script_hash, after, page_size)
    }

    fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.address_unspent_outputs_page_from_height(
            address_script_hash,
            start_height,
            after,
            page_size,
        )
    }

    fn address_transaction_history_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.address_transaction_history_page(address_script_hash, after, page_size)
    }

    fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.address_transaction_history_range_page(
            address_script_hash,
            height_range,
            after,
            page_size,
        )
    }

    fn address_balance(
        &self,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError> {
        self.address_balance(address_script_hash)
    }
}

/// One immutable canonical and wallet reader pair at the exact same source.
///
/// Pair construction validates network, epoch, event cursor, visible tip,
/// sequence digest, and settled tip. Callers can safely hold an `Arc` to this
/// pair for an entire request while a different generation catches up.
pub struct WalletServingReadPair {
    canonical: Arc<dyn CanonicalReader>,
    wallet: Arc<dyn WalletProjectionReader>,
    canonical_fence: CanonicalEventFence,
    wallet_source: WalletCanonicalSourceIdentity,
}

impl WalletServingReadPair {
    /// Creates an immutable pair only when both readers prove one exact source.
    pub fn new(
        canonical: Arc<dyn CanonicalReader>,
        wallet: Arc<dyn WalletProjectionReader>,
    ) -> Result<Self, QueryError> {
        Self::validate_readers(canonical.as_ref(), wallet.as_ref())
            .map_err(|error| pair_admission_query_error(&error))?;
        Ok(Self {
            canonical_fence: canonical.event_fence(),
            wallet_source: WalletCanonicalSourceIdentity::from_ready_evidence(
                wallet.ready_evidence(),
            ),
            canonical,
            wallet,
        })
    }

    /// Validates whether two immutable readers represent one exact source.
    ///
    /// Candidate-pair publishers use this before publication. It compares the
    /// admitted network, epoch, event cursor, visible tip, sequence digest,
    /// and settlement boundary without retaining either reader.
    pub fn validate_readers(
        canonical: &(dyn CanonicalReader + 'static),
        wallet: &(dyn WalletProjectionReader + 'static),
    ) -> Result<(), WalletServingAdmissionError> {
        if canonical.network() != wallet.network() {
            return Err(WalletServingAdmissionError::NetworkMismatch {
                canonical: canonical.network(),
                wallet: wallet.network(),
            });
        }
        let canonical_fence = canonical.event_fence();
        let canonical_epoch = canonical
            .chain_epoch()
            .map_err(|source| WalletServingAdmissionError::CanonicalRead { source })?;
        let canonical_visible_tip = BlockId::new(
            canonical_epoch.visible_tip_height,
            canonical_epoch.visible_tip_hash,
        );
        if canonical_epoch.network != canonical.network()
            || canonical_epoch.id != canonical_fence.chain_epoch_id()
            || canonical_visible_tip != canonical_fence.visible_tip()
        {
            return Err(WalletServingAdmissionError::CanonicalFenceMismatch);
        }
        let canonical_source = WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(
                canonical_fence.chain_epoch_id(),
                canonical_fence.visible_tip(),
                canonical_fence.chain_event_sequence(),
            ),
            canonical_fence.sequence_digest(),
            BlockId::new(
                canonical_epoch.settled_tip_height,
                canonical_epoch.settled_tip_hash,
            ),
        );
        let wallet_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        if wallet_source != canonical_source {
            return Err(WalletServingAdmissionError::WalletSourceMismatch {
                canonical: Box::new(canonical_source),
                wallet: Box::new(wallet_source),
            });
        }
        Ok(())
    }

    /// Returns the canonical reader frozen for this pair's entire lifetime.
    #[must_use]
    pub fn canonical(&self) -> &(dyn CanonicalReader + 'static) {
        self.canonical.as_ref()
    }

    /// Returns the wallet reader frozen for this pair's entire lifetime.
    #[must_use]
    pub fn wallet(&self) -> &(dyn WalletProjectionReader + 'static) {
        self.wallet.as_ref()
    }

    /// Returns the exact canonical fence admitted for this pair.
    #[must_use]
    pub const fn canonical_fence(&self) -> CanonicalEventFence {
        self.canonical_fence
    }

    /// Returns the exact wallet source admitted for this pair.
    #[must_use]
    pub const fn wallet_source(&self) -> WalletCanonicalSourceIdentity {
        self.wallet_source
    }
}

impl fmt::Debug for WalletServingReadPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WalletServingReadPair")
            .field("canonical_fence", &self.canonical_fence)
            .field("wallet_source", &self.wallet_source)
            .finish_non_exhaustive()
    }
}

fn pair_admission_query_error(error: &WalletServingAdmissionError) -> QueryError {
    QueryError::WalletProjectionRead {
        source: Box::new(io::Error::other(error.to_string())),
    }
}
