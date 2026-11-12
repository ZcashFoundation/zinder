//! Core Zinder domain values shared across storage and service boundaries.
//!
//! This crate intentionally owns chain vocabulary such as [`ChainEpoch`],
//! [`BlockHeaderArtifact`], and [`CompactBlockArtifact`] without depending on a
//! storage engine, node client, or wallet protocol crate.

#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("Zinder supports only 32-bit and 64-bit pointer-width targets.");

pub mod artifact_family;
mod block_artifact;
mod block_header;
mod block_id;
mod canonical_block_facts;
mod canonical_block_replay;
mod canonical_history;
mod chain_epoch;
mod chain_value_pools;
#[cfg(feature = "commitment-tree-codec")]
mod commitment_tree_codec;
mod displaced_block;
pub mod explorer_reasons;
pub mod explorer_search;
mod mempool;
mod network_upgrade_activations;
mod subtree_root;
mod transaction;
mod transaction_public_facts;
mod transparent_address_balance;
mod transparent_address_tx_index;
mod transparent_output;
mod tree_state;
mod utxo_set_commitment;
mod wallet_scan;
pub mod wire;

pub use utxo_set_commitment::{
    TransparentUtxoSetCommitment, UTXO_SET_COMMITMENT_DIGEST_LEN, UTXO_SET_COMMITMENT_LEN,
    UtxoSetCommitmentScheme,
};

pub use block_artifact::{
    BlockBlobArtifact, BlockHeaderArtifact, BlockTransactionIndexArtifact, CompactBlockArtifact,
    CompactBlockArtifactError, CompactBlockArtifactParts, CompactChainMetadata, CompactTransaction,
};
pub use block_header::BlockHeader;
pub use block_id::{BlockId, BlockSelector};
pub use canonical_block_facts::{
    CanonicalBlockFacts, CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsReferenceEncoding, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockFactsSequenceLengthOverflow, CanonicalTransactionFacts, SerializedBytesDigest,
    UnsupportedCanonicalBlockFactsDigestVersion,
    UnsupportedCanonicalBlockFactsSequenceDigestVersion,
};
pub use canonical_block_replay::{
    CanonicalBlockReplayDecodeError, CanonicalBlockReplayEnvelope,
    CanonicalBlockReplayFormatVersion, UnsupportedCanonicalBlockReplayFormatVersion,
    ValidatedCanonicalBlockReplay, decode_canonical_block_replay, encode_canonical_block_replay,
};
pub use canonical_history::{CanonicalHistoryBounds, CanonicalHistoryBoundsError};
pub use chain_epoch::{
    ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, BlockHeightRangeIter,
    ChainEpoch, ChainEpochId, ChainTipMetadata, Network, UnixTimestampMillis,
};
pub use chain_value_pools::{
    BlockValuePoolBalances, ChainValuePool, ChainValuePools, ChainValuePoolsAtTip, ValuePoolBalance,
};
#[cfg(feature = "commitment-tree-codec")]
pub use commitment_tree_codec::{
    CommitmentTreeAccumulator, CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontierValidationError, CommitmentTreeFrontiers,
    MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES,
};
pub use displaced_block::{
    DisplacedBlock, DisplacedBlockArchiveCoverage, DisplacedBlockCoinbaseOutput,
    DisplacedRootArchiveCoverage, DisplacedRootCandidate,
};
pub use mempool::{
    MempoolEntry, MempoolEntryBuildError, MempoolEntryParts, MempoolEvictionReason,
    MempoolObservation, TransparentMempoolOutput, TransparentMempoolSpend,
};
pub use network_upgrade_activations::{
    ConsensusBranchId, NetworkUpgradeActivation, NetworkUpgradeActivations,
    NetworkUpgradeActivationsError, NetworkUpgradeActivationsFingerprint,
    NetworkUpgradeActivationsFingerprintVersion,
    UnsupportedNetworkUpgradeActivationsFingerprintVersion,
};
pub use subtree_root::{
    MAX_SUBTREE_ROOTS_PER_REQUEST, SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootArtifact,
    SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange, SubtreeRootRangeIter,
};
pub use transaction::{
    AuthDigest, BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding, BroadcastQueued,
    BroadcastRejected, BroadcastRejectionReason, BroadcastUnknown, MAX_RAW_TRANSACTION_BYTES,
    MinedTransaction, MinedTransactionChainContext, RawTransactionBytes, TransactionBlobArtifact,
    TransactionBroadcastOutcome, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransactionIntrinsicValueBalancesArtifact,
    TransactionLocation, TxStatus,
};
pub use transaction_public_facts::{
    LockTime, PrivacyShape, TransactionComponentCounts, TransactionPublicFacts, TransactionVersion,
    UnsupportedSection, Wtxid, classify_privacy_shape,
};
pub use transparent_address_balance::TransparentAddressBalance;
pub use transparent_address_tx_index::TransparentAddressTxIndexArtifact;
pub use transparent_output::{
    TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
    TransparentOutputArtifact, TransparentOutputFact, TransparentSpendEntry, TransparentSpendFact,
    TransparentUnspentOutput,
};
pub use tree_state::{BlockFinalNoteCommitmentRoots, FinalNoteCommitmentRoot, TreeStateArtifact};
pub use wallet_scan::{
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput,
};
