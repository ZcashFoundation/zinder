//! Typed Rust client surface for Zinder chain-index consumers.
//!
//! `zinder-client` is the public Rust API that wallet daemons and application
//! code import. It keeps `RocksDB`, `tonic`, and generated protobuf types behind
//! typed domain methods so downstream consumers do not need to couple to
//! Zinder service internals.
//!
//! This crate is the external consumer SDK; Zinder's own services do not use
//! it for service-to-service calls, which go through each service's own
//! authenticated channel setup instead.

#[cfg(feature = "remote")]
mod capability;
mod chain_index;
mod error;
mod error_reason;
#[cfg(feature = "remote")]
mod remote;
#[cfg(feature = "remote")]
mod server_info;

#[cfg(feature = "remote")]
pub use capability::{Capability, CapabilityDescriptor};

#[cfg(feature = "remote")]
pub use chain_index::{
    ChainEpochCommitted, ChainEvent, ChainEventCursor, ChainEventEnvelope, ChainEventStream,
    ChainEventStreamFamily, ChainRangeReverted, EndpointBackedIndex, EventStreamStart,
    MempoolEvent, MempoolEventCursor, MempoolEventEnvelope, MempoolEventStream,
    MempoolSnapshotCursor, MempoolSnapshotRequest, MempoolSnapshotView,
};
pub use chain_index::{
    ChainIndex, ChainSnapshot, IndexStream, OwnedChainSnapshot, TransparentAddressTransactionChunk,
    TransparentAddressTxIdsQuery, TransparentAddressTxIdsStream,
    TransparentAddressUnspentOutputsQuery, TransparentAddressUnspentOutputsStream,
    TransparentHistoryCursor, TransparentOutpointSpendStatuses, TransparentUnspentOutputChunk,
};
pub use error::{ChainEventCursorRecovery, IndexerError, RetryPolicy};
pub use error_reason::ErrorReason;
#[cfg(feature = "remote")]
pub use remote::{MIN_SUPPORTED_CONTRACT_REVISION, RemoteChainIndex, RemoteOpenOptions};
#[cfg(feature = "remote")]
pub use server_info::{NodeServerInfo, ServerInfo};
pub use zinder_core::{
    ArtifactSchemaVersion, BlockBlobArtifact, BlockHash, BlockHeader, BlockHeight,
    BlockHeightRange, BlockId, BlockSelector, BroadcastAccepted, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastQueued, BroadcastRejected, BroadcastRejectionReason,
    BroadcastUnknown, ChainEpoch, ChainEpochId, ChainValuePool, ChainValuePools,
    ChainValuePoolsAtTip, CompactBlockArtifact, ConsensusBranchId,
    MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST, MAX_SUBTREE_ROOTS_PER_REQUEST, MempoolEntry,
    MempoolEvictionReason, MinedTransaction, MinedTransactionChainContext, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange, TransactionBroadcastOutcome,
    TransactionId, TransparentAddressScriptHash, TransparentAddressTxIndexArtifact,
    TransparentMempoolOutput, TransparentMempoolSpend, TransparentOutPoint,
    TransparentOutpointSpendStatus, TransparentUnspentOutput, TreeStateArtifact, TxStatus,
};
/// The server-side wallet recipe, compiled as a doctest so its worked skeleton
/// cannot drift from the real `connect` and stream API.
#[cfg(feature = "remote")]
#[allow(
    clippy::doc_markdown,
    reason = "Reader-facing recipe prose names product and crate identifiers without backticks by design."
)]
#[doc = include_str!("../README.md")]
mod server_side_wallet_recipe {}
