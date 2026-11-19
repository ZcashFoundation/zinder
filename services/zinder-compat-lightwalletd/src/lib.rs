//! lightwalletd-compatible wallet sync adapter.
//!
//! This crate owns compatibility translation for the vendored lightwalletd
//! `CompactTxStreamer` protocol. It consumes [`zinder_query::WalletQueryApi`]
//! and does not open storage, call upstream nodes, or build artifacts.

mod block_range;
mod grpc;
mod mempool;
mod network_tip_estimate;
mod node_tip_witness;
mod ping;
mod request_limits;
mod stale_tip;

pub use block_range::{
    BLOCK_RANGE_CHUNK_MAX_BYTES, BlockRangeChunks, BlockRangeContinuity, BlockRangeResponse,
    BlockRangeSink, BlockRangeStage, BlockRangeStreamLimit, BlockRangeStreamSlot,
    DEFAULT_BLOCK_RANGE_ADMISSION_WAIT, DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
};
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
pub use network_tip_estimate::estimated_network_height;
pub use node_tip_witness::{
    NODE_TIP_OBSERVATION_MAX_AGE, NodeTipWitness, node_tip_from_writer_status,
    spawn_node_tip_witness,
};
pub use ping::LightwalletdPing;
pub use request_limits::COMPACT_TX_STREAMER_FIELD_LIMITS;
pub use stale_tip::{
    DEFAULT_STALE_TIP_LIMIT, StaleTipLimit, TipBlockTime, TipBlockTimeCache, unix_now_seconds,
};
