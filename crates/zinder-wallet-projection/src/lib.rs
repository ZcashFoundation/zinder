//! Wallet projection contracts independent of storage engines.
//!
//! This crate owns the exact durable keys and values needed by wallet-facing
//! reads. It does not depend on canonical-store readers or the materialized-view
//! plane.

#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("Zinder supports only 32-bit and 64-bit pointer-width targets.");

mod contract_error;
mod control;
mod digest;
mod rows;
mod serial_oracle;

pub use contract_error::WalletProjectionContractError;
pub use control::{
    WalletCanonicalSourceIdentity, WalletProjectionBuildLease, WalletProjectionBuildLeaseRequest,
    WalletProjectionBuildOwner, WalletProjectionBuildPlan, WalletProjectionBuildState,
    WalletProjectionDigest, WalletProjectionEventCursor, WalletProjectionFamilyRowCounts,
    WalletProjectionReadyEvidence, WalletProjectionRetainedEventAnchor,
    WalletProjectionSourcePosition, WalletStoreControlRecord, WalletUtxoSetSummary,
};
pub use digest::{
    WALLET_PROJECTION_ACCUMULATOR_LEN, WALLET_PROJECTION_ACCUMULATOR_VERSION,
    WalletProjectionAccumulator, WalletProjectionDigestBuilder, WalletProjectionRowFamily,
};
pub use rows::{
    WalletAddressBalance, WalletAddressTransaction, WalletAddressTransactionHistoryPage,
    WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletAddressUnspentOutputsPage,
    WalletOutpointKey, WalletReorgUndo, WalletSpentOutput, WalletTransactionPosition,
    WalletUnspentOutput,
};
pub use serial_oracle::WalletProjectionSerialOracle;

/// Physical wallet schema version under the clean `wallet` identity.
///
/// Version 2 holds accumulator version 2 and UTXO commitment scheme 2. A store
/// or state bundle of version 1 is refused and must be rebuilt.
pub const WALLET_PROJECTION_SCHEMA_VERSION: u16 = 2;

/// Wallet projection row-value encoding version with source-bound reorg undo.
pub const WALLET_PROJECTION_VALUE_ENCODING_VERSION: u16 = 2;

/// Version of the durable projection-build lease embedded in store control.
pub const WALLET_PROJECTION_BUILD_LEASE_VERSION: u16 = 1;

/// Canonical store schema admitted by this projection schema.
pub const REQUIRED_CANONICAL_STORE_SCHEMA_VERSION: u16 = 6;

/// Canonical replay format admitted by this projection schema.
pub const REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION: u32 = 1;

/// Canonical block-facts digest admitted by this projection schema.
pub const REQUIRED_CANONICAL_FACTS_DIGEST_VERSION: u16 = 1;

/// Canonical sequence digest admitted by this projection schema.
pub const REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION: u16 = 1;

/// Fixed key of the singleton wallet control record.
pub const WALLET_STORE_CONTROL_KEY: &[u8] = b"store_control";

/// Persisted identity admitted as a wallet projection store.
pub const WALLET_PROJECTION_STORE_IDENTITY: &[u8] = b"wallet";
