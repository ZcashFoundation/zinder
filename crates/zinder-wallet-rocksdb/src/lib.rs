//! Wallet projection construction and storage for `RocksDB`.

#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("Zinder supports only 32-bit and 64-bit pointer-width targets.");

mod build;
mod build_lock;
mod error;
mod projection_load;
mod secondary;
mod spike_combined;
mod store;
mod transition;

pub use build::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildOutcome, RocksDbWalletBuildReport,
    WalletBuildLeaseHeartbeat, WalletBuildLeasePhase, WalletBuildPhaseDurations,
    WalletColdValidationPhaseDurations, WalletProjectionBuildLeaseExecution,
    WalletProjectionReplaySource, build_wallet_from_canonical,
    build_wallet_from_canonical_with_lease, build_wallet_from_canonical_with_lease_and_heartbeat,
    validate_wallet_projection_pre_promotion_fence,
};
pub use error::RocksDbWalletError;
pub use secondary::{RocksDbWalletSecondary, WalletSecondaryCatchupOutcome};
pub use store::{
    RocksDbWalletBuildStore, RocksDbWalletFollowingStore, RocksDbWalletStore,
    WALLET_ROCKSDB_SCHEMA_VERSION, WalletAddressTransactionHistoryPage,
    WalletAddressUnspentOutputsPage, WalletOwnerCheckpointAdmission, WalletOwnerCheckpointEvidence,
    WalletRecoveryAdmissionConfig,
};
pub use spike_combined::{
    SPIKE_WALLET_CONTROL_COLUMN_FAMILY, SpikeColdReadback, SpikePendingWalletPublication,
    SpikePlannedWalletTransition, SpikeWalletPlanStats, spike_decode_wallet_control,
    spike_wallet_column_family_descriptors, spike_wallet_column_family_names,
    spike_wallet_data_column_family_names, spike_wallet_data_options,
};
pub use transition::{
    MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, SpikeWalletPlanOps, SpikeWalletRowOp,
};
