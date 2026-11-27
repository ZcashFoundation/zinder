//! lightwalletd-compatible wallet sync adapters.
//!
//! This crate owns compatibility translation for the vendored lightwalletd
//! `CompactTxStreamer` protocol. [`LightwalletdGrpcAdapter`] serves every
//! method from [`zinder_query::WalletQueryApi`].
//! [`CompactBlocksGrpcAdapter`] serves the block methods from the
//! [`zinder_query::RocksDbCanonicalReader`] of each compact-blocks
//! generation alone, and both answer those methods through one
//! implementation. Neither opens storage, calls upstream nodes, or builds
//! artifacts.

mod block_methods;
mod block_range;
mod compact_block_messages;
mod compact_blocks_grpc;
mod grpc;
mod mempool;
mod network_tip_estimate;
mod node_tip_witness;
mod ping;
mod published_range;
mod sealed_history;
pub use published_range::{
    PublicationConcurrency, PublishedShieldedHistory, ShieldedCatalogPublisher,
};
mod request_limits;
pub mod runtime;
mod stale_tip;
mod tip_wait;

pub use block_range::{
    BLOCK_RANGE_CHUNK_MAX_BYTES, BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES,
    DEFAULT_BLOCK_RANGE_ADMISSION_WAIT, DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
};
pub use compact_blocks_grpc::{CanonicalReaderSlot, CompactBlocksGrpcAdapter};
pub use grpc::light_wallet::NodeBackedLightWallet;
pub use grpc::{
    DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS, DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS,
    LightwalletdAdmissionError, LightwalletdGrpcAdapter, LightwalletdResponseLimits,
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
pub use stale_tip::{DEFAULT_STALE_TIP_LIMIT, StaleTipLimit};
pub use tip_wait::{
    DEFAULT_TIP_WAIT, DEFAULT_TIP_WAIT_MAX_BLOCKS, DEFAULT_TIP_WAIT_MAX_WAITERS, TipWait,
};

/// The fixed header length, in bytes, of a `ZSHRNG01` sealed segment file.
pub const SEALED_HEADER_LEN: usize = sealed_history::HEADER_LEN;
