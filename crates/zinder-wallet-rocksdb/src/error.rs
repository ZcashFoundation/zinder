//! Fail-closed errors for the concrete wallet `RocksDB` lifecycle.

use std::{io, path::PathBuf};

use thiserror::Error;
use zinder_core::{CanonicalBlockFactsSequenceLengthOverflow, Network, UnixTimestampMillis};
use zinder_rocksdb_bulk_load::BulkLoadError;
use zinder_store::CanonicalStoreError;
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionBuildOwner, WalletProjectionContractError,
};

/// Failure to create, publish, admit, or query a wallet store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RocksDbWalletError {
    /// The authenticated canonical replay refused a row or its terminal fence.
    #[error("wallet projection canonical replay failed")]
    CanonicalReplay {
        /// Canonical storage failure preserved for diagnosis.
        #[source]
        source: CanonicalStoreError,
    },
    /// Complete-history construction observed no height-one block.
    #[error("complete-history wallet construction requires canonical block height 1")]
    EmptyCanonicalHistory,
    /// The canonical fact sequence exceeded the version-1 count domain.
    #[error(transparent)]
    SourceSequenceLength(#[from] CanonicalBlockFactsSequenceLengthOverflow),
    /// Retained reorg undo state would exceed its explicit accounted-memory limit.
    #[error(
        "wallet reorg undo requires at least {required_bytes} accounted bytes, limit is {limit_bytes}"
    )]
    AccountedReorgUndoMemoryLimit {
        /// Caller-supplied hard limit.
        limit_bytes: u64,
        /// Minimum accounted bytes needed by the refused operation.
        required_bytes: u64,
    },
    /// External sorting or ordered SST construction failed.
    #[error(transparent)]
    BulkLoad(#[from] BulkLoadError),
    /// A build counter exceeded the version-1 report domain.
    #[error("wallet projection build counter exceeds u64::MAX")]
    BuildCounterOverflow,
    /// The completed derivation did not match the canonical READY fence.
    #[error("wallet projection canonical source fence mismatch: {reason}")]
    CanonicalSourceFenceMismatch {
        /// Stable mismatch reason.
        reason: &'static str,
    },
    /// A fresh build target already exists, so construction refused to adopt it.
    #[error("wallet RocksDB build target is not fresh: {path}")]
    PathNotFresh {
        /// Rejected build path.
        path: PathBuf,
    },
    /// An owner checkpoint was pointed at a path that already exists.
    #[error("wallet owner checkpoint requires an absent target path: {path}")]
    CheckpointTargetExists {
        /// Existing path preserved without mutation.
        path: PathBuf,
    },
    /// The concrete `RocksDB` checkpoint operation failed.
    #[error("wallet owner checkpoint at {path} failed: {source}")]
    CheckpointFailed {
        /// Requested checkpoint target.
        path: PathBuf,
        /// Underlying `RocksDB` checkpoint failure.
        #[source]
        source: rust_rocksdb::Error,
    },
    /// A deterministic sibling projection-load staging path already exists.
    #[error(
        "wallet projection staging path already exists and requires full build cleanup: {path}"
    )]
    ProjectionStagingPathNotFresh {
        /// Existing staging path preserved without repair or adoption.
        path: PathBuf,
    },
    /// The wallet store path could not be created or resolved.
    #[error("wallet RocksDB path is unavailable at {path}: {source}")]
    PathUnavailable {
        /// Unavailable path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The bounded resource budget violated a locked storage invariant.
    #[error("invalid wallet RocksDB resource budget: {reason}")]
    InvalidResourceBudget {
        /// Rejected budget invariant.
        reason: &'static str,
    },
    /// Logical row, SST byte, or file accounting exceeded `u64::MAX`.
    #[error("wallet RocksDB projection load accounting exceeds u64::MAX")]
    WalletProjectionLoadAccountingOverflow,
    /// Equal address-history keys carried conflicting transaction identities.
    #[error("canonical facts produce conflicting values for one address-transaction key")]
    AddressTransactionConflict,
    /// A concrete `RocksDB` operation failed.
    #[error("wallet RocksDB {operation} failed: {source}")]
    RocksDbOperation {
        /// Stable operation label.
        operation: &'static str,
        /// Driver error.
        #[source]
        source: rust_rocksdb::Error,
    },
    /// The database does not contain exactly the wallet families.
    #[error(
        "wallet RocksDB column families do not match version 1; expected {expected:?}, observed {observed:?}"
    )]
    ColumnFamilyContractMismatch {
        /// Exact required family names.
        expected: Vec<String>,
        /// Observed family names.
        observed: Vec<String>,
    },
    /// The default column family did not contain exactly one control record.
    #[error("wallet RocksDB default column family must contain only store_control")]
    StoreControlCardinalityMismatch,
    /// The singleton wallet control record is absent.
    #[error("wallet RocksDB store_control record is missing")]
    StoreControlMissing,
    /// A query open encountered an unpublished BUILDING store.
    #[error("wallet RocksDB store is not READY: {path}")]
    StoreNotReady {
        /// Rejected store path.
        path: PathBuf,
    },
    /// The admitted store belongs to a different network.
    #[error("wallet RocksDB network mismatch; expected {expected:?}, observed {observed:?}")]
    NetworkMismatch {
        /// Operator-selected network.
        expected: Network,
        /// Network committed by the control record.
        observed: Network,
    },
    /// The READY projection does not represent the caller's authenticated canonical source.
    #[error(
        "wallet RocksDB canonical source mismatch; expected {expected:?}, observed {observed:?}"
    )]
    CanonicalSourceMismatch {
        /// Canonical identity required by the serving process.
        expected: Box<WalletCanonicalSourceIdentity>,
        /// Canonical identity committed by the READY wallet control record.
        observed: Box<WalletCanonicalSourceIdentity>,
    },
    /// A different owner holds an unexpired durable projection-build lease.
    #[error("wallet projection build lease is held by owner {holder} until {expires_at:?}")]
    WalletProjectionBuildLeaseHeld {
        /// Owner identity persisted by the live lease.
        holder: WalletProjectionBuildOwner,
        /// The first instant at which a different owner may take over.
        expires_at: UnixTimestampMillis,
    },
    /// The supplied lease is no longer live at the caller's explicit clock.
    #[error("wallet projection build lease expired at {expires_at:?}")]
    WalletProjectionBuildLeaseExpired {
        /// Durable expiry bound that was reached.
        expires_at: UnixTimestampMillis,
    },
    /// The caller supplied a non-future expiry for an acquire or renewal.
    #[error("wallet projection build lease expiry must be after the supplied clock")]
    WalletProjectionBuildLeaseExpiryNotFuture,
    /// A lease renewal did not extend the existing durable expiry.
    #[error("wallet projection build lease renewal must extend the durable expiry")]
    WalletProjectionBuildLeaseRenewalNotExtended,
    /// The supplied capability is owned by a different process identity.
    #[error("wallet projection build lease owner does not match the durable owner")]
    WalletProjectionBuildLeaseOwnerMismatch {
        /// Owner identity persisted by the active lease.
        expected: WalletProjectionBuildOwner,
        /// Owner identity supplied by the caller.
        observed: WalletProjectionBuildOwner,
    },
    /// The supplied capability belongs to an obsolete ownership generation.
    #[error("wallet projection build lease generation does not match the durable generation")]
    WalletProjectionBuildLeaseGenerationMismatch {
        /// Monotonic generation persisted by the active lease.
        expected: u64,
        /// Generation supplied by the caller.
        observed: u64,
    },
    /// A requested or promoted source differs from the durable lease anchor.
    #[error("wallet projection build lease canonical anchor mismatch: {reason}")]
    WalletProjectionBuildLeaseCanonicalAnchorMismatch {
        /// Stable source-anchor mismatch reason.
        reason: &'static str,
    },
    /// A control record has no active build lease to authorize the mutation.
    #[error("wallet projection build lease is absent")]
    WalletProjectionBuildLeaseMissing,
    /// The monotonic lease-generation domain is exhausted.
    #[error("wallet projection build lease generation exceeds u64::MAX")]
    WalletProjectionBuildLeaseGenerationOverflow,
    /// A caller stopped a projection build before READY promotion.
    #[error("wallet projection build was cancelled")]
    ProjectionBuildCancelled,
    /// A failed pre-promotion build could not clear its own exact durable lease.
    #[error(
        "wallet projection build failed ({build_error}) and exact lease cleanup failed ({cleanup_error})"
    )]
    BuildLeaseCleanup {
        /// The original build failure that triggered cleanup.
        build_error: Box<Self>,
        /// The failure while releasing only the acquired lease capability.
        cleanup_error: Box<Self>,
    },
    /// A caller stopped an incremental wallet transition before its atomic write.
    #[error("wallet projection transition was cancelled before its atomic write")]
    ProjectionTransitionCancelled,
    /// A transition budget exceeds the hard maximum safe in-process plan size.
    #[error(
        "wallet projection transition logical-byte limit {requested_bytes} exceeds the maximum {maximum_bytes}"
    )]
    InvalidTransitionLogicalByteLimit {
        /// Caller-requested logical-byte ceiling.
        requested_bytes: u64,
        /// Hard implementation maximum.
        maximum_bytes: u64,
    },
    /// A planned transition would exceed its accounted batch-and-overlay ceiling.
    #[error(
        "wallet projection transition requires at least {required_bytes} accounted logical bytes, limit is {limit_bytes}"
    )]
    TransitionLogicalByteLimit {
        /// Caller-supplied hard ceiling.
        limit_bytes: u64,
        /// Minimum logical bytes required by the refused planned mutation.
        required_bytes: u64,
    },
    /// Checked logical-byte accounting exceeded the `u64` domain.
    #[error("wallet projection transition logical-byte accounting exceeds u64::MAX")]
    TransitionLogicalByteAccountingOverflow,
    /// The current wallet undo suffix cannot safely represent a valid canonical rewind.
    #[error(
        "wallet projection rebuild is required before following this canonical transition: {reason}"
    )]
    WalletProjectionRebuildRequired {
        /// Stable rebuild trigger suitable for operator and projector policy.
        reason: &'static str,
    },
    /// A canonical event, fence, replay range, or durable wallet row was inconsistent.
    #[error("wallet projection transition was rejected: {reason}")]
    ProjectionTransitionRejected {
        /// Stable rejection reason.
        reason: &'static str,
    },
    /// The wallet primary's monotonic transition generation is exhausted.
    #[error("wallet projection transition generation exceeds u64::MAX")]
    ProjectionTransitionGenerationOverflow,
    /// A page continuation belongs to an address other than the requested address.
    #[error("wallet RocksDB {index} continuation belongs to a different address")]
    ContinuationAddressMismatch {
        /// Stable index name whose continuation was rejected.
        index: &'static str,
    },
    /// A required data family could not be resolved after exact admission.
    #[error("wallet RocksDB required column family is unavailable: {name}")]
    ColumnFamilyUnavailable {
        /// Required family name.
        name: &'static str,
    },
    /// The store changed between admission and its bounded serving open.
    #[error("wallet RocksDB admission failed: {reason}")]
    AdmissionChanged {
        /// Failed admission invariant.
        reason: &'static str,
    },
    /// Durable wallet bytes violate the clean version-1 contract.
    #[error(transparent)]
    Contract(#[from] WalletProjectionContractError),
}

impl RocksDbWalletError {
    pub(crate) fn rocksdb(operation: &'static str, source: rust_rocksdb::Error) -> Self {
        Self::RocksDbOperation { operation, source }
    }
}
