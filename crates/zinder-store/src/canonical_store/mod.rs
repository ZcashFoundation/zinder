//! Canonical storage contract.
//!
//! This module owns the exact `RocksDB` layout for the production canonical
//! canonical data plane. It deliberately exposes no generic database adapter
//! and no compatibility decoder for earlier Zinder stores.

mod activation_schedule;
mod block_load;
mod block_replay;
mod builder;
mod construction_manifest;
mod control;
mod displaced_archive;
mod event_lifecycle;
mod live_commit;
mod live_replacement;
#[cfg(test)]
mod live_replacement_tests;
mod mempool_lifecycle;
mod publication;
mod reader;
mod rocksdb;
mod secondary;
mod subtree_load;
mod wallet_events;

use std::{io, num::NonZeroU32, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, CanonicalBlockReplayFormatVersion, ChainEpochId,
    CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
    MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol,
};

use crate::RawBlobRetention;

pub use activation_schedule::CanonicalActivationSchedule;
pub use block_load::{CanonicalBlockLoadEvidence, CanonicalBuildBlock};
pub use block_replay::{
    CanonicalReplayRangeScan, CanonicalReplayScan, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS,
};
pub use builder::RocksDbCanonicalBuilder;
pub use construction_manifest::{
    CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION, CanonicalConstructionManifestBinding,
};
pub use event_lifecycle::{
    CanonicalEventCursor, CanonicalEventHistoryRequest, CanonicalEventKind,
    CanonicalEventRetentionReport, CanonicalRetainedEvent, ProjectionBuildAnchor,
    ProjectionBuildLease, ProjectionBuildLeaseId,
};
pub use live_commit::{CanonicalAppendAnchor, CanonicalEventFence, CanonicalLiveAppend};
pub use live_replacement::{CanonicalLiveReplacement, CanonicalReplacementBlock};
pub use mempool_lifecycle::CanonicalMempoolSnapshotStart;
pub use publication::{
    CanonicalBaselinePublication, PreparedCanonicalBaselinePublication,
    ValidatedRocksDbCanonicalBuild,
};
pub use rocksdb::RocksDbCanonicalStore;
pub use rocksdb::{CanonicalOwnerCheckpointAdmission, CanonicalOwnerCheckpointEvidence};
pub use secondary::{CanonicalSecondaryCatchupOutcome, RocksDbCanonicalSecondary};
pub use subtree_load::{
    CanonicalBuildSubtreeRoot, CanonicalSubtreeRootLoadCoverage, CanonicalSubtreeRootLoadEvidence,
};

/// Exact persisted identity of the clean canonical store.
pub const CANONICAL_STORE_IDENTITY: &str = "canonical";
/// Exact physical schema accepted by this canonical store implementation.
///
/// Schema 2 adds the immutable reorg policy and authenticated settled-sequence
/// checkpoint to the control record. Schema 3 adds the retention-floor and
/// generation-bearing projection-build lease control records. Schema 4 makes
/// every retained event carry its exact authenticated resulting fence. Schema 5
/// binds a complete construction manifest into every READY control record.
/// Schema 7 makes raw-blob retention an immutable construction and admission
/// contract independent of the canonical workload.
/// Earlier stores are refused and rebuilt; there is no compatibility decoder
/// or migration path.
pub const CANONICAL_STORE_SCHEMA_VERSION: u16 = 7;
/// Global block-height cadence for typed commitment-tree checkpoints.
///
/// A checkpoint at least every 100 blocks keeps wallet rewind anchors within the
/// standard scan-recovery window regardless of canonical loader batch boundaries.
pub const TREE_STATE_CHECKPOINT_STRIDE: u32 = 100;

/// Default canonical reorg window in blocks.
///
/// Zebra and Zakura finalize a block only once their best non-finalized chain
/// grows past 1000 blocks (`MAX_BLOCK_REORG_HEIGHT`), so this window covers
/// every reorg either node can present.
pub const DEFAULT_REORG_WINDOW_BLOCKS: u32 = 1_000;

const REQUIRED_CANONICAL_NETWORK_UPGRADES: [&str; 5] =
    ["Overwinter", "Sapling", "Blossom", "Heartwood", "Canopy"];

/// Maximum canonical replacement depth for one canonical store.
///
/// The construction policy is immutable store identity. The canonical writer
/// may later admit a larger window in place; it never lowers one. Ordering
/// compares window depths.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanonicalReorgPolicy {
    reorg_window_blocks: NonZeroU32,
}

impl CanonicalReorgPolicy {
    /// Validates an exact nonzero canonical replacement depth.
    pub const fn new(reorg_window_blocks: u32) -> Result<Self, CanonicalStoreBuildPlanError> {
        let Some(reorg_window_blocks) = NonZeroU32::new(reorg_window_blocks) else {
            return Err(CanonicalStoreBuildPlanError::ZeroReorgWindowBlocks);
        };
        Ok(Self {
            reorg_window_blocks,
        })
    }

    /// Returns the maximum supported replacement depth in blocks.
    #[must_use]
    pub const fn reorg_window_blocks(self) -> u32 {
        self.reorg_window_blocks.get()
    }
}

/// Fixed source-chain range for one fresh canonical construction.
///
/// The predecessor anchors the first retained block even for complete history,
/// where it is the network's height-zero block. The fixed tip prevents an
/// exhausted or failed source stream from publishing a contiguous prefix as a
/// complete build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalStoreBuildPlan {
    network: zinder_core::Network,
    network_upgrade_activations_fingerprint: NetworkUpgradeActivationsFingerprint,
    raw_blob_retention: RawBlobRetention,
    reorg_policy: CanonicalReorgPolicy,
    history_bounds: zinder_core::CanonicalHistoryBounds,
    history_predecessor: CommitmentTreeCheckpoint,
    build_tip: BlockId,
}

impl CanonicalStoreBuildPlan {
    /// Builds a plan retaining every non-genesis block through `build_tip`.
    pub fn complete(
        network_upgrade_activations: &NetworkUpgradeActivations,
        genesis_block_time_seconds: u32,
        build_tip: BlockId,
        raw_blob_retention: RawBlobRetention,
        reorg_policy: CanonicalReorgPolicy,
    ) -> Result<Self, CanonicalStoreBuildPlanError> {
        validate_required_network_upgrades(network_upgrade_activations)?;
        let network = network_upgrade_activations.network();
        Self {
            network,
            network_upgrade_activations_fingerprint: network_upgrade_activations
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            raw_blob_retention,
            reorg_policy,
            history_bounds: zinder_core::CanonicalHistoryBounds::complete(),
            history_predecessor: CommitmentTreeCheckpoint::new(
                BlockId::new(BlockHeight::new(0), network.genesis_hash()),
                genesis_block_time_seconds,
                CommitmentTreeFrontiers::default(),
            ),
            build_tip,
        }
        .validate()
    }

    /// Builds a plan retaining blocks immediately after `checkpoint` through `build_tip`.
    pub fn checkpointed(
        network_upgrade_activations: &NetworkUpgradeActivations,
        checkpoint: CommitmentTreeCheckpoint,
        build_tip: BlockId,
        raw_blob_retention: RawBlobRetention,
        reorg_policy: CanonicalReorgPolicy,
    ) -> Result<Self, CanonicalStoreBuildPlanError> {
        validate_required_network_upgrades(network_upgrade_activations)?;
        if checkpoint.block_id.height.value() == 0 {
            return Err(CanonicalStoreBuildPlanError::CheckpointAtGenesis);
        }
        let history_bounds = zinder_core::CanonicalHistoryBounds::checkpointed(checkpoint.block_id)
            .map_err(|_| CanonicalStoreBuildPlanError::CheckpointHasNoSuccessor)?;
        validate_checkpoint_frontier_presence(
            network_upgrade_activations,
            checkpoint.block_id,
            &checkpoint.frontiers,
        )?;
        Self {
            network: network_upgrade_activations.network(),
            network_upgrade_activations_fingerprint: network_upgrade_activations
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            raw_blob_retention,
            reorg_policy,
            history_bounds,
            history_predecessor: checkpoint,
            build_tip,
        }
        .validate()
    }

    pub(super) fn validate(self) -> Result<Self, CanonicalStoreBuildPlanError> {
        match self.history_bounds.preceding_checkpoint() {
            None if self.history_predecessor.block_id
                != BlockId::new(BlockHeight::new(0), self.network.genesis_hash()) =>
            {
                return Err(CanonicalStoreBuildPlanError::InvalidHistoryPredecessor);
            }
            Some(checkpoint) if self.history_predecessor.block_id != checkpoint => {
                return Err(CanonicalStoreBuildPlanError::InvalidHistoryPredecessor);
            }
            Some(checkpoint) if checkpoint.height.value() == 0 => {
                return Err(CanonicalStoreBuildPlanError::CheckpointAtGenesis);
            }
            None | Some(_) => {}
        }
        if self.history_bounds.preceding_checkpoint().is_none()
            && self.history_predecessor.frontiers != CommitmentTreeFrontiers::default()
        {
            return Err(CanonicalStoreBuildPlanError::CompleteHistoryHasFrontiers);
        }
        for protocol in [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ] {
            if let Some(frontier) = self.history_predecessor.frontiers.get(protocol)
                && frontier.final_state_bytes().len()
                    > MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES
            {
                return Err(CanonicalStoreBuildPlanError::PredecessorFrontierTooLarge {
                    protocol,
                    encoded_bytes: frontier.final_state_bytes().len(),
                });
            }
        }
        let first_available_height = self.history_bounds.first_available_height();
        if self.build_tip.height.value() < first_available_height.value() {
            return Err(CanonicalStoreBuildPlanError::BuildTipPrecedesHistory {
                build_tip: self.build_tip.height.value(),
                first_available_height: first_available_height.value(),
            });
        }
        Ok(self)
    }

    /// Returns the immutable network for this build.
    #[must_use]
    pub const fn network(&self) -> zinder_core::Network {
        self.network
    }

    /// Returns the immutable node activation-table identity for this build.
    #[must_use]
    pub const fn network_upgrade_activations_fingerprint(
        &self,
    ) -> NetworkUpgradeActivationsFingerprint {
        self.network_upgrade_activations_fingerprint
    }

    /// Returns the immutable raw-blob retention contract.
    #[must_use]
    pub const fn raw_blob_retention(&self) -> RawBlobRetention {
        self.raw_blob_retention
    }

    /// Returns the immutable canonical replacement policy.
    #[must_use]
    pub const fn reorg_policy(&self) -> CanonicalReorgPolicy {
        self.reorg_policy
    }

    /// Returns the durable boundary of intentionally retained history.
    #[must_use]
    pub const fn history_bounds(&self) -> zinder_core::CanonicalHistoryBounds {
        self.history_bounds
    }

    /// Returns the typed checkpoint immediately preceding retained history.
    #[must_use]
    pub const fn history_predecessor(&self) -> &CommitmentTreeCheckpoint {
        &self.history_predecessor
    }

    /// Returns the exact source tip this build must reach.
    #[must_use]
    pub const fn build_tip(&self) -> BlockId {
        self.build_tip
    }
}

/// Invalid source-chain range for canonical construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CanonicalStoreBuildPlanError {
    /// Canonical replacement must retain at least one displaced block.
    #[error("canonical build reorg window must be greater than zero")]
    ZeroReorgWindowBlocks,
    /// A persisted reorg-policy revision must raise the construction window.
    #[error(
        "reorg policy revision {revised_window_blocks} does not raise the construction window {construction_window_blocks}"
    )]
    ReorgPolicyRevisionNotRaised {
        /// Immutable construction window.
        construction_window_blocks: u32,
        /// Rejected revised window.
        revised_window_blocks: u32,
    },
    /// The node activation table cannot interpret the universal v3-v4 history baseline.
    #[error("canonical build activation table is missing required network upgrade {name}")]
    MissingRequiredNetworkUpgrade {
        /// Exact node-advertised upgrade name required by v1.
        name: &'static str,
    },
    /// The checkpoint cannot have retained history after it.
    #[error("canonical build checkpoint has no successor height")]
    CheckpointHasNoSuccessor,
    /// Height zero has complete-history semantics and cannot be a checkpoint.
    #[error("canonical build checkpoint cannot be the height-zero genesis block")]
    CheckpointAtGenesis,
    /// The predecessor does not match the selected complete or checkpointed history.
    #[error("canonical build history predecessor does not match its history bounds")]
    InvalidHistoryPredecessor,
    /// A complete build must start before every shielded-pool frontier.
    #[error("complete history predecessor commitment-tree frontiers must all be absent")]
    CompleteHistoryHasFrontiers,
    /// One predecessor frontier exceeds the exact canonical store-control bound.
    #[error(
        "canonical build {protocol:?} predecessor frontier is {encoded_bytes} bytes; maximum is 1090"
    )]
    PredecessorFrontierTooLarge {
        /// Shielded pool whose frontier exceeded the bound.
        protocol: ShieldedProtocol,
        /// Observed canonical `finalState` byte length.
        encoded_bytes: usize,
    },
    /// Checkpoint frontier presence disagrees with the source activation table.
    #[error(
        "canonical build {protocol:?} predecessor frontier presence at height {checkpoint_height} does not match the network upgrade activations"
    )]
    PredecessorFrontierActivationMismatch {
        /// Shielded pool whose presence was inconsistent.
        protocol: ShieldedProtocol,
        /// Checkpoint height used for activation admission.
        checkpoint_height: u32,
    },
    /// The target tip is below the first retained height.
    #[error(
        "canonical build tip {build_tip} precedes first available height {first_available_height}"
    )]
    BuildTipPrecedesHistory {
        /// Invalid target height.
        build_tip: u32,
        /// First retained height required by the history bounds.
        first_available_height: u32,
    },
}

fn validate_required_network_upgrades(
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreBuildPlanError> {
    for name in REQUIRED_CANONICAL_NETWORK_UPGRADES {
        if network_upgrade_activations
            .activation_height_by_name(name)
            .is_none()
        {
            return Err(CanonicalStoreBuildPlanError::MissingRequiredNetworkUpgrade { name });
        }
    }
    Ok(())
}

fn validate_checkpoint_frontier_presence(
    network_upgrade_activations: &NetworkUpgradeActivations,
    checkpoint: BlockId,
    frontiers: &CommitmentTreeFrontiers,
) -> Result<(), CanonicalStoreBuildPlanError> {
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        let is_active = network_upgrade_activations
            .activation_height_by_name(protocol.activation_upgrade_name())
            .is_some_and(|activation_height| activation_height <= checkpoint.height);
        if frontiers.get(protocol).is_some() != is_active {
            return Err(
                CanonicalStoreBuildPlanError::PredecessorFrontierActivationMismatch {
                    protocol,
                    checkpoint_height: checkpoint.height.value(),
                },
            );
        }
    }
    Ok(())
}

/// Failure while streaming source facts into a fresh canonical build.
#[derive(Debug, Error)]
pub enum CanonicalStoreBuildError<SourceError> {
    /// Upstream fetch or canonical preparation failed before ingestion.
    #[error("canonical source stream failed")]
    Source {
        /// Concrete upstream failure preserved for diagnosis.
        #[source]
        source: SourceError,
    },
    /// Canonical storage construction failed.
    #[error(transparent)]
    Store(#[from] CanonicalStoreError),
}

/// Closed canonical data workload persisted before any data family is built.
///
/// Both workloads retain the semantic replay needed by projections. The
/// selected workload fixes which optional explorer source artifacts must be
/// complete. Raw block and transaction retention is an independent persisted
/// [`RawBlobRetention`] contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalStoreWorkload {
    /// Wallet APIs, including typed tree-state checkpoints and continuous
    /// subtree roots.
    ///
    /// This workload omits per-block final roots, daily value-pool balances,
    /// and other explorer-only source facts.
    Wallet,
    /// Wallet APIs plus per-block final roots, daily value-pool balances, and
    /// other explorer-only source facts.
    Explorer,
}

impl CanonicalStoreWorkload {
    /// Returns the persisted configuration spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wallet => "wallet",
            Self::Explorer => "explorer",
        }
    }
}

/// Durable construction state of a canonical store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "READY carries fixed-size admission evidence by value so store-control decoding remains allocation-free and the evidence can be copied atomically with its fence"
)]
pub enum CanonicalStoreBuildState {
    /// Data families are inactive and may still be under construction.
    Building,
    /// Every required family was validated and the baseline epoch is visible.
    Ready(CanonicalStoreReadyEvidence),
}

/// Authenticated canonical replay prefix through the durable settled tip.
///
/// This checkpoint is the only resumable prefix admitted by the version-1
/// store. Its count starts at the first retained block, and its logical byte
/// total counts replay-envelope values only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalSequenceCheckpoint {
    through: BlockId,
    retained_block_count: u64,
    sequence_digest: CanonicalBlockFactsSequenceDigest,
    logical_replay_bytes: u64,
}

impl CanonicalSequenceCheckpoint {
    pub(super) const fn from_admitted_parts(
        through: BlockId,
        retained_block_count: u64,
        sequence_digest: CanonicalBlockFactsSequenceDigest,
        logical_replay_bytes: u64,
    ) -> Self {
        Self {
            through,
            retained_block_count,
            sequence_digest,
            logical_replay_bytes,
        }
    }

    /// Returns the last retained block authenticated by this prefix.
    #[must_use]
    pub const fn through(self) -> BlockId {
        self.through
    }

    /// Returns the retained block count from the first stored block through `through`.
    #[must_use]
    pub const fn retained_block_count(self) -> u64 {
        self.retained_block_count
    }

    /// Returns the typed ordered digest through `through`.
    #[must_use]
    pub const fn sequence_digest(self) -> CanonicalBlockFactsSequenceDigest {
        self.sequence_digest
    }

    /// Returns cumulative replay-envelope value bytes through `through`.
    #[must_use]
    pub const fn logical_replay_bytes(self) -> u64 {
        self.logical_replay_bytes
    }
}

/// Validation evidence that makes a constructed canonical store visible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalStoreReadyEvidence {
    /// First retained canonical block.
    pub first_retained_block: BlockId,
    /// Current visible canonical tip.
    pub visible_tip: BlockId,
    /// Current visible epoch identifier.
    pub visible_epoch: ChainEpochId,
    /// Latest durable chain-event sequence that produced `visible_epoch`.
    pub visible_event_sequence: u64,
    /// Number of contiguous blocks authenticated at `visible_tip`.
    pub visible_block_count: u64,
    /// Canonical block-fact digest contract.
    pub block_digest_version: CanonicalBlockFactsDigestVersion,
    /// Canonical replay-envelope contract.
    pub replay_format_version: CanonicalBlockReplayFormatVersion,
    /// Ordered sequence-digest contract.
    pub sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion,
    /// Ordered digest of the complete visible fact sequence.
    pub visible_sequence_digest: [u8; 32],
    /// Total semantic replay-envelope bytes through `visible_tip`.
    pub visible_logical_replay_bytes: u64,
    /// Resumable authenticated replay prefix through the settled tip.
    pub sequence_checkpoint: CanonicalSequenceCheckpoint,
    /// Exact version of the immutable construction manifest that certified the
    /// original fresh build before its first READY transition.
    pub construction_manifest_version: u16,
    /// SHA-256 of the immutable construction manifest written before the first
    /// READY transition. This value is retained unchanged through following.
    pub construction_manifest_sha256: [u8; 32],
}

/// Failure to create or admit a clean canonical store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalStoreError {
    /// The supplied `RocksDB` resource budget violates a hard bound.
    #[error("invalid canonical store resource budget: {reason}")]
    InvalidResourceBudget {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A builder was pointed at a path that already exists.
    #[error("canonical store builder requires a fresh path: {path:?}")]
    PathNotFresh {
        /// Existing path refused by the builder.
        path: PathBuf,
    },

    /// An owner checkpoint was pointed at a path that already exists.
    #[error("canonical owner checkpoint requires an absent target path: {path:?}")]
    CheckpointTargetExists {
        /// Existing path preserved without mutation.
        path: PathBuf,
    },

    /// The concrete `RocksDB` checkpoint operation failed.
    #[error("canonical owner checkpoint at {path:?} failed")]
    CheckpointFailed {
        /// Requested checkpoint target.
        path: PathBuf,
        /// Underlying `RocksDB` checkpoint failure.
        #[source]
        source: rust_rocksdb::Error,
    },

    /// A prior process left the deterministic canonical block staging directory.
    #[error(
        "canonical block staging path already exists and requires full build cleanup: {path:?}"
    )]
    BlockLoadStagingExists {
        /// Existing staging path preserved without repair or deletion.
        path: PathBuf,
    },

    /// A filesystem operation failed.
    #[error("canonical store path {path:?} is unavailable: {source}")]
    PathUnavailable {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: io::Error,
    },

    /// Secure cursor-authentication material could not be generated.
    #[error("canonical store cursor authentication key generation failed")]
    EntropyUnavailable {
        /// Operating-system entropy failure.
        #[source]
        source: getrandom::Error,
    },

    /// An existing path does not exactly match the clean canonical contract.
    #[error("canonical store admission refused for {path:?}: {reason}")]
    AdmissionRefused {
        /// Existing path that was inspected without data-family creation.
        path: PathBuf,
        /// Exact incompatibility observed during admission.
        reason: String,
    },

    /// A serving store open encountered an unpublished BUILDING store.
    #[error("canonical store is not READY: {path:?}")]
    StoreNotReady {
        /// Exact store path that remains unpublished.
        path: PathBuf,
    },

    /// A `RocksDB` operation failed after identity admission.
    #[error("canonical store RocksDB {operation} failed")]
    RocksDbOperation {
        /// Concrete operation that failed.
        operation: &'static str,
        /// Underlying `RocksDB` failure.
        #[source]
        source: rust_rocksdb::Error,
    },

    /// Canonical block replay input is empty, discontinuous, or inconsistent.
    #[error("canonical block replay sequence is invalid: {reason}")]
    BlockReplaySequenceInvalid {
        /// Exact sequence invariant that failed.
        reason: String,
    },

    /// A persisted canonical block replay row is malformed or mis-keyed.
    #[error("canonical block replay at height {height} is invalid: {reason}")]
    BlockReplayInvalid {
        /// Height used to address the replay row.
        height: u32,
        /// Exact replay invariant that failed.
        reason: String,
    },

    /// A persisted replay key is not the exact version-1 ascending height key.
    #[error("canonical block replay key is invalid: {reason}")]
    BlockReplayKeyInvalid {
        /// Exact key-decoding failure.
        reason: String,
    },

    /// Canonical block-family input is empty, discontinuous, or inconsistent.
    #[error("canonical block sequence is invalid: {reason}")]
    BlockLoadSequenceInvalid {
        /// Exact sequence invariant that failed.
        reason: String,
    },

    /// Canonical block-family rows already exist in an unpublished store.
    #[error(
        "canonical block families are already populated in a BUILDING store; full build cleanup is required"
    )]
    BlockLoadAlreadyLoaded,

    /// Persisted replay rows differ from the prepared canonical block sequence.
    #[error("canonical block replay readback does not match the prepared version-1 block sequence")]
    BlockLoadReadbackMismatch,

    /// Subtree roots were requested before the canonical block families were complete.
    #[error("canonical subtree roots require completed canonical block families")]
    CanonicalBlocksNotLoaded,

    /// Subtree roots were already loaded into this fresh build.
    #[error("canonical subtree roots are already populated; full build cleanup is required")]
    SubtreeRootLoadAlreadyLoaded,

    /// Source subtree roots do not exactly cover the selected load ranges.
    #[error("canonical subtree-root sequence is invalid: {reason}")]
    SubtreeRootSequenceInvalid {
        /// Exact source or chain-identity mismatch.
        reason: String,
    },

    /// Persisted subtree-root rows differ from the authenticated source sequence.
    #[error("canonical subtree-root readback does not match the authenticated source sequence")]
    SubtreeRootReadbackMismatch,

    /// The final source checkpoint does not authenticate the locally accumulated fixed tip.
    #[error("canonical fixed-tip source checkpoint is invalid: {reason}")]
    SourceTipCheckpointMismatch {
        /// Exact identity, time, or frontier mismatch.
        reason: String,
    },

    /// A fresh canonical build is incomplete or changed during cold validation.
    #[error("canonical publication refused: {reason}")]
    PublicationRefused {
        /// Exact missing prerequisite or validation mismatch.
        reason: String,
    },

    /// The atomic write returned an error, so admission must determine whether it committed.
    #[error("canonical publication write outcome is unknown for {path:?}")]
    PublicationWriteOutcomeUnknown {
        /// Store path that must be reopened through normal admission.
        path: PathBuf,
        /// Underlying atomic write failure.
        #[source]
        source: rust_rocksdb::Error,
    },

    /// The atomic write committed but its immediate readback could not certify it.
    #[error(
        "canonical publication committed but immediate verification failed for {path:?}: {reason}"
    )]
    PublicationCommittedButUnverified {
        /// Store path that must be reopened through normal admission.
        path: PathBuf,
        /// Immediate verification failure.
        reason: String,
    },

    /// A READY store refused a live canonical transition before its atomic write.
    #[error("canonical live commit refused: {reason}")]
    LiveCommitRefused {
        /// Exact transition invariant that failed.
        reason: String,
    },

    /// A live atomic write returned an error, so admission must determine its outcome.
    #[error("canonical live commit write outcome is unknown for {path:?}")]
    LiveCommitWriteOutcomeUnknown {
        /// Store path that must be reopened through normal READY admission.
        path: PathBuf,
        /// Underlying atomic write failure.
        #[source]
        source: rust_rocksdb::Error,
    },

    /// A live atomic write committed but immediate exact readback could not certify it.
    #[error(
        "canonical live commit completed but immediate verification failed for {path:?}: {reason}"
    )]
    LiveCommitCompletedButUnverified {
        /// Store path that must be reopened through normal READY admission.
        path: PathBuf,
        /// Immediate verification failure.
        reason: String,
    },

    /// A canonical displaced-fact archive key, value, link, or event fence is invalid.
    #[error("canonical displaced archive refused: {reason}")]
    DisplacedArchiveInvalid {
        /// Exact archive invariant that failed.
        reason: String,
    },

    /// A persisted canonical event cursor names history older than the retained floor.
    #[error(
        "canonical event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    CanonicalEventCursorExpired {
        /// Persisted cursor sequence.
        event_sequence: u64,
        /// Inclusive oldest retained event sequence.
        oldest_retained_sequence: u64,
    },

    /// A persisted canonical event cursor has an unsupported encoding version.
    #[error("canonical event cursor version {version} is unsupported")]
    CanonicalEventCursorUnknownVersion {
        /// Encoded cursor version.
        version: u8,
    },

    /// A persisted canonical event cursor cannot identify an exact retained position.
    #[error("canonical event cursor is malformed: {reason}")]
    CanonicalEventCursorMalformed {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A retained canonical event record has an unsupported version.
    #[error("canonical event {event_sequence} has unsupported version {version}")]
    CanonicalEventVersionUnsupported {
        /// Event row sequence.
        event_sequence: u64,
        /// Encoded record version.
        version: u8,
    },

    /// A retained canonical event record has an invalid range or transition shape.
    #[error("canonical event {event_sequence} is malformed: {reason}")]
    CanonicalEventRecordMalformed {
        /// Event row sequence.
        event_sequence: u64,
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A retained canonical event refers to an epoch whose immutable row is absent.
    #[error("canonical epoch {epoch_id} is not retained")]
    CanonicalEpochNotRetained {
        /// Exact epoch identity requested by a retained transition.
        epoch_id: u64,
    },

    /// A persisted mempool-event cursor is older than the durable retention floor.
    #[error(
        "mempool event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    MempoolEventCursorExpired {
        /// Persisted cursor sequence.
        event_sequence: u64,
        /// Inclusive oldest retained event sequence.
        oldest_retained_sequence: u64,
    },

    /// A persisted mempool-event cursor does not authenticate an exact retained event.
    #[error("mempool event cursor is invalid: {reason}")]
    MempoolEventCursorInvalid {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A mempool-snapshot paging cursor does not authenticate its anchor.
    #[error("mempool snapshot page cursor is invalid: {reason}")]
    MempoolSnapshotCursorInvalid {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A mempool-snapshot paging cursor names an event sequence beyond the durable head.
    #[error(
        "mempool snapshot page cursor expired: cursor anchor sequence {anchor_event_sequence}, current {current_event_sequence}"
    )]
    MempoolSnapshotCursorExpired {
        /// Cursor anchor sequence.
        anchor_event_sequence: u64,
        /// Current durable mempool-event sequence.
        current_event_sequence: u64,
    },

    /// The durable mempool event sequence cannot advance further.
    #[error("mempool event sequence overflow")]
    MempoolEventSequenceOverflow,

    /// A durable mempool-event key, envelope, or head pointer is invalid.
    #[error("mempool event log is invalid: {reason}")]
    MempoolEventLogInvalid {
        /// Exact invariant that failed.
        reason: String,
    },

    /// A projection-build lease cannot preserve a valid retained-event anchor.
    #[error("projection build lease is invalid: {reason}")]
    ProjectionBuildLeaseInvalid {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// A projection-build lease was used after its durable expiry.
    #[error("projection build lease has expired")]
    ProjectionBuildLeaseExpired,
}

impl CanonicalStoreError {
    fn admission(path: &std::path::Path, reason: impl Into<String>) -> Self {
        Self::AdmissionRefused {
            path: path.to_path_buf(),
            reason: reason.into(),
        }
    }

    fn block_replay_sequence(reason: impl Into<String>) -> Self {
        Self::BlockReplaySequenceInvalid {
            reason: reason.into(),
        }
    }

    fn block_replay_invalid(height: BlockHeight, reason: impl Into<String>) -> Self {
        Self::BlockReplayInvalid {
            height: height.value(),
            reason: reason.into(),
        }
    }

    fn block_load_sequence(reason: impl Into<String>) -> Self {
        Self::BlockLoadSequenceInvalid {
            reason: reason.into(),
        }
    }

    fn subtree_root_sequence(reason: impl Into<String>) -> Self {
        Self::SubtreeRootSequenceInvalid {
            reason: reason.into(),
        }
    }

    fn live_commit(reason: impl Into<String>) -> Self {
        Self::LiveCommitRefused {
            reason: reason.into(),
        }
    }

    fn source_tip_checkpoint(reason: impl Into<String>) -> Self {
        Self::SourceTipCheckpointMismatch {
            reason: reason.into(),
        }
    }

    fn publication(reason: impl Into<String>) -> Self {
        Self::PublicationRefused {
            reason: reason.into(),
        }
    }

    fn displaced_archive(reason: impl Into<String>) -> Self {
        Self::DisplacedArchiveInvalid {
            reason: reason.into(),
        }
    }
}

impl From<zinder_rocksdb_bulk_load::BulkLoadError> for CanonicalStoreError {
    fn from(source: zinder_rocksdb_bulk_load::BulkLoadError) -> Self {
        match source {
            zinder_rocksdb_bulk_load::BulkLoadError::InvalidInput { reason } => {
                Self::block_load_sequence(reason)
            }
            zinder_rocksdb_bulk_load::BulkLoadError::AccountedMemoryLimit {
                limit_bytes,
                required_bytes,
            } => Self::block_load_sequence(format!(
                "bulk-load records require {required_bytes} accounted bytes, limit is {limit_bytes}"
            )),
            zinder_rocksdb_bulk_load::BulkLoadError::TemporaryFileLimit {
                limit_bytes,
                required_bytes,
            } => Self::block_load_sequence(format!(
                "bulk-load runs require {required_bytes} temporary bytes, limit is {limit_bytes}"
            )),
            zinder_rocksdb_bulk_load::BulkLoadError::MemoryAllocation { operation, source } => {
                Self::block_load_sequence(format!(
                    "bulk-load {operation} allocation failed: {source}"
                ))
            }
            zinder_rocksdb_bulk_load::BulkLoadError::PathUnavailable { path, source } => {
                Self::PathUnavailable { path, source }
            }
            zinder_rocksdb_bulk_load::BulkLoadError::RocksDbOperation { operation, source } => {
                Self::RocksDbOperation { operation, source }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn test_network_upgrade_activations(
    network: zinder_core::Network,
) -> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
    let activations = [
        "Overwinter",
        "Sapling",
        "Blossom",
        "Heartwood",
        "Canopy",
        "NU5",
        "NU6",
        "NU6.1",
        "NU6.2",
        "NU6.3",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| zinder_core::NetworkUpgradeActivation {
        branch_id: zinder_core::ConsensusBranchId::new(
            u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1),
        ),
        activation_height: BlockHeight::new(
            u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1),
        ),
        name: name.to_owned(),
    })
    .collect();
    NetworkUpgradeActivations::new(network, activations)
}

#[cfg(test)]
pub(crate) fn test_checkpoint_frontiers(
    network_upgrade_activations: &NetworkUpgradeActivations,
    checkpoint_height: BlockHeight,
) -> CommitmentTreeFrontiers {
    let active_frontier = |protocol: ShieldedProtocol| {
        network_upgrade_activations
            .activation_height_by_name(protocol.activation_upgrade_name())
            .is_some_and(|activation_height| activation_height <= checkpoint_height)
            .then(|| zinder_core::CommitmentTreeFrontier::empty(protocol))
    };
    CommitmentTreeFrontiers::from_validated_parts(
        active_frontier(ShieldedProtocol::Sapling),
        active_frontier(ShieldedProtocol::Orchard),
        active_frontier(ShieldedProtocol::Ironwood),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{BlockHash, CommitmentTreeFrontier, Network};

    #[test]
    fn complete_build_plan_preserves_genesis_anchor_and_tip()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = zinder_core::Network::ZcashTestnet;
        let activations = test_network_upgrade_activations(network)?;
        let build_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]));
        let plan = CanonicalStoreBuildPlan::complete(
            &activations,
            1_234,
            build_tip,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        assert_eq!(plan.network(), network);
        assert_eq!(
            plan.network_upgrade_activations_fingerprint(),
            activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        );
        assert_eq!(
            plan.history_predecessor().block_id,
            BlockId::new(BlockHeight::new(0), network.genesis_hash())
        );
        assert_eq!(plan.history_predecessor().block_time_seconds, 1_234);
        assert_eq!(
            plan.history_bounds().first_available_height(),
            BlockHeight::new(1)
        );
        assert_eq!(
            &plan.history_predecessor().frontiers,
            &CommitmentTreeFrontiers::default()
        );
        assert_eq!(plan.build_tip(), build_tip);
        assert_eq!(plan.reorg_policy().reorg_window_blocks(), 100);
        Ok(())
    }

    #[test]
    fn reorg_policy_rejects_zero_window() {
        let error = CanonicalReorgPolicy::new(0).err();

        assert_eq!(
            error,
            Some(CanonicalStoreBuildPlanError::ZeroReorgWindowBlocks)
        );
    }

    #[test]
    fn build_plan_rejects_incomplete_activation_table() -> Result<(), Box<dyn std::error::Error>> {
        let error = CanonicalStoreBuildPlan::complete(
            &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
            0,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )
        .err();
        assert_eq!(
            error,
            Some(
                CanonicalStoreBuildPlanError::MissingRequiredNetworkUpgrade { name: "Overwinter" }
            )
        );
        Ok(())
    }

    #[test]
    fn build_plan_accepts_regtest_with_post_canopy_upgrades_disabled()
    -> Result<(), Box<dyn std::error::Error>> {
        let full_activations = test_network_upgrade_activations(Network::ZcashRegtest)?;
        let canopy_only_activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            full_activations.activations()[..REQUIRED_CANONICAL_NETWORK_UPGRADES.len()].to_vec(),
        )?;
        let build_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]));

        let plan = CanonicalStoreBuildPlan::complete(
            &canopy_only_activations,
            0,
            build_tip,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;

        assert_eq!(plan.build_tip(), build_tip);
        Ok(())
    }

    #[test]
    fn build_plan_rejects_tip_before_retained_history() -> Result<(), Box<dyn std::error::Error>> {
        let activations = test_network_upgrade_activations(Network::ZcashRegtest)?;
        let error = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(0), BlockHash::from_bytes([0; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )
        .err();
        assert!(matches!(
            error,
            Some(CanonicalStoreBuildPlanError::BuildTipPrecedesHistory {
                build_tip: 0,
                first_available_height: 1,
            })
        ));
        Ok(())
    }

    #[test]
    fn checkpointed_build_plan_rejects_height_ceiling() -> Result<(), Box<dyn std::error::Error>> {
        let activations = test_network_upgrade_activations(Network::ZcashRegtest)?;
        let error = CanonicalStoreBuildPlan::checkpointed(
            &activations,
            CommitmentTreeCheckpoint::new(
                BlockId::new(BlockHeight::new(u32::MAX), BlockHash::from_bytes([9; 32])),
                99,
                CommitmentTreeFrontiers::default(),
            ),
            BlockId::new(BlockHeight::new(u32::MAX), BlockHash::from_bytes([9; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )
        .err();
        assert_eq!(
            error,
            Some(CanonicalStoreBuildPlanError::CheckpointHasNoSuccessor)
        );
        Ok(())
    }

    #[test]
    fn checkpointed_build_plan_preserves_tree_position() -> Result<(), Box<dyn std::error::Error>> {
        let checkpoint = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([9; 32]));
        let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
        let checkpoint_frontiers = active_empty_frontiers();
        let plan = CanonicalStoreBuildPlan::checkpointed(
            &activations,
            CommitmentTreeCheckpoint::new(checkpoint, 1_234, checkpoint_frontiers.clone()),
            BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([10; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;

        assert_eq!(plan.history_predecessor().block_id, checkpoint);
        assert_eq!(plan.history_predecessor().block_time_seconds, 1_234);
        assert_eq!(plan.history_predecessor().frontiers, checkpoint_frontiers);
        Ok(())
    }

    #[test]
    fn complete_build_plan_domain_rejects_predecessor_frontiers()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = zinder_core::Network::ZcashTestnet;
        let activations = test_network_upgrade_activations(network)?;
        let error = CanonicalStoreBuildPlan {
            network,
            network_upgrade_activations_fingerprint: activations
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            history_bounds: zinder_core::CanonicalHistoryBounds::complete(),
            history_predecessor: CommitmentTreeCheckpoint::new(
                BlockId::new(BlockHeight::new(0), network.genesis_hash()),
                0,
                CommitmentTreeFrontiers::from_validated_parts(
                    Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
                    None,
                    None,
                ),
            ),
            build_tip: BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
        }
        .validate()
        .err();

        assert_eq!(
            error,
            Some(CanonicalStoreBuildPlanError::CompleteHistoryHasFrontiers)
        );
        Ok(())
    }

    #[test]
    fn checkpointed_build_plan_rejects_genesis_and_activation_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = test_network_upgrade_activations(Network::ZcashRegtest)?;
        let genesis = BlockId::new(BlockHeight::new(0), Network::ZcashRegtest.genesis_hash());
        assert_eq!(
            CanonicalStoreBuildPlan::checkpointed(
                &activations,
                CommitmentTreeCheckpoint::new(genesis, 0, CommitmentTreeFrontiers::default(),),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
            )
            .err(),
            Some(CanonicalStoreBuildPlanError::CheckpointAtGenesis)
        );

        let checkpoint = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32]));
        assert!(matches!(
            CanonicalStoreBuildPlan::checkpointed(
                &activations,
                CommitmentTreeCheckpoint::new(checkpoint, 0, CommitmentTreeFrontiers::default(),),
                BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32])),
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
            )
            .err(),
            Some(
                CanonicalStoreBuildPlanError::PredecessorFrontierActivationMismatch {
                    protocol: ShieldedProtocol::Sapling,
                    checkpoint_height: 3,
                }
            )
        ));
        Ok(())
    }

    fn active_empty_frontiers() -> CommitmentTreeFrontiers {
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
        )
    }
}
