//! Mempool surfaces consumed by the lightwalletd compatibility binary.
//!
//! Owns the trait boundary that the gRPC adapter sees ([`MempoolSurface`],
//! [`TipChangeWatcher`]) plus the production wirings backed by the writer's
//! private `IngestControl` gRPC endpoint.

mod ingest_control;
mod surface;

pub use ingest_control::{
    IngestControlMempoolSurface, WatchTipChangeWatcher, spawn_ingest_control_tip_change_publisher,
};
pub use surface::{
    MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface, MempoolSurfaceError,
    MempoolTransactionLookup, SharedMempoolSurface, SharedTipChangeWatcher, TipChangeWatcher,
    TipChangeWatcherError,
};
