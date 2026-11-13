//! lightwalletd-compatible wallet sync adapter.
//!
//! This crate owns compatibility translation for the vendored lightwalletd
//! `CompactTxStreamer` protocol. It consumes [`zinder_query::WalletQueryApi`]
//! and does not open storage, call upstream nodes, or build artifacts.

mod grpc;
mod mempool;

pub use grpc::{
    DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS, DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS,
    LightwalletdAdmissionError, LightwalletdGrpcAdapter, LightwalletdResponseLimits,
    compact_transaction_has_payload,
};
pub use mempool::{
    IngestControlMempoolSurface, MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface,
    MempoolSurfaceError, MempoolTransactionLookup, SharedMempoolSurface, SharedTipChangeWatcher,
    TipChangeWatcher, TipChangeWatcherError, WatchTipChangeWatcher,
    spawn_ingest_control_tip_change_publisher,
};
