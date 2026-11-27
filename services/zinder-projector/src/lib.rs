//! Production ownership boundary for wallet projection construction.
//!
//! The release binary is the only component that opens the wallet store as a
//! primary. Canonical facts are consumed through a process-local `RocksDB`
//! secondary so projection construction cannot contend with canonical ingest.
//! After cold validation, the service keeps the wallet primary private and
//! continuously reconciles it from authenticated retained canonical events.
//! A wallet is considered ready for serving only when its persisted source
//! fence exactly matches the canonical writer fence.

#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("Zinder supports only 32-bit and 64-bit pointer-width targets.");

/// Stable service name used in logs and operational endpoint labels.
pub const PROJECTOR_SERVICE_NAME: &str = "zinder-projector";

/// Coherent canonical-and-wallet checkpoint capture and manifest admission.
pub mod state_bundle;

/// Sealed, fixed-layout recovery artifact packaging and byte admission.
pub mod recovery_archive;

/// Reusable runtime entrypoints, configuration, and backup CLI helpers.
pub mod runtime;
