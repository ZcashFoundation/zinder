//! Mempool concerns owned by `zinder-ingest`.
//!
//! Groups the in-memory live index and hydration of source observations
//! into canonical [`zinder_core::MempoolEntry`] records.

mod entry;
mod index;
mod live_owner;
mod ready_gate;

pub use entry::{MempoolEntryBuildError, build_mempool_entry};
pub(crate) use index::MempoolIndexPreflight;
pub use index::{MempoolApplyOutcome, MempoolIndex, MempoolSnapshotPage};
pub use live_owner::{
    DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES, LiveMempoolOwner,
    MempoolRetentionSettings, run_live_mempool_owner, run_mempool_retention,
};
pub use ready_gate::{MempoolReadyGate, MempoolReadySignal, mempool_ready_channel};
