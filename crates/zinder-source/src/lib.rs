//! Source values and adapters for Zinder ingestion.
//!
//! This crate normalizes upstream node observations into source-domain values. It
//! does not decide canonical chain state and does not build durable artifacts.

mod json_rpc_mempool;
mod mempool_source;
mod node_auth;
mod node_capabilities;
mod node_source;
mod node_target;
mod source_block;
mod source_chain_update;
mod source_error;
mod source_subtree_root;
mod source_transaction;
mod source_tree_state;
mod transparent_address;
mod transport;
mod upstream_health;
mod zebra_indexer_block;
mod zebra_indexer_chain_tip;
mod zebra_indexer_mempool;
mod zebra_json_rpc;
mod zebra_ready_endpoint;

pub use json_rpc_mempool::{
    DEFAULT_MEMPOOL_POLL_INTERVAL, JsonRpcMempoolSource, JsonRpcMempoolSourceOptions,
};
pub use mempool_source::{
    DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES, DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT,
    MempoolHydrationFailureReason, MempoolSource, MempoolSourceAdmissionLimits,
    MempoolSourceBackend, MempoolSourceCapabilities, MempoolSourceEntry, MempoolSourceEvent,
    MempoolSourceEventStream,
};
pub use node_auth::{CookieSource, CookieSourceError, NodeAuth};
pub use node_capabilities::{NodeCapabilities, NodeCapabilitiesError, NodeCapability};
pub use node_source::{NodeSource, TransactionBroadcaster, TreeStateUpstream};
pub use node_target::{
    DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS, DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS,
    DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR, DEFAULT_NODE_REQUEST_TIMEOUT_SECS,
    NodeAuthSection, NodeConfigError, NodeHealthConfig, NodeHealthSection,
    NodeIndexerClientIdentity, NodeIndexerTls, NodeIndexerTlsSection, NodeSection, NodeTarget,
};
pub use source_block::{
    SourceBlock, SourceBlockHeader, block_header_from_raw_block_bytes, decode_rpc_block_hash,
    encode_rpc_block_hash,
};
pub use source_chain_update::{
    SourceChainCursor, SourceChainSegment, SourceChainSegmentLimits, SourceChainSegmentStats,
    SourceChainUpdate,
};
pub use source_error::{SourceError, SourceFailureClass};
pub use source_subtree_root::{SourceSubtreeRoot, SourceSubtreeRoots};
pub use source_transaction::{
    TransactionPublicFactSet, parse_transaction_public_fact_set, parse_transaction_public_facts,
    transaction_component_counts, transaction_public_fact_set_from_parsed,
};
pub use source_tree_state::SourceTreeState;
pub use transparent_address::transparent_address_matches_network;
pub use transport::{
    ResilientClient, ZEBRA_REBUILD_THRESHOLD, ZebraIndexerChannelOptions, ZebraTransportError,
    build_zebra_json_rpc_client, connect_zebra_indexer_channel, is_transport_failure,
    validate_zebra_indexer_endpoint,
};
pub use upstream_health::{
    UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR, UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS,
    UPSTREAM_HEALTH_REASON_NO_TIP, UPSTREAM_HEALTH_REASON_OK, UPSTREAM_HEALTH_REASON_SYNCING,
    UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
    UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT, UpstreamHealthSnapshot,
};
pub use zebra_indexer_block::{ZebraIndexerBlockSource, ZebraIndexerBlockSourceOptions};
pub use zebra_indexer_chain_tip::{
    ChainTipNotification, ChainTipNotificationSource, ChainTipNotificationStream,
    ZebraIndexerChainTipSource, ZebraIndexerChainTipSourceOptions,
};
pub use zebra_indexer_mempool::{
    ZebraIndexerMempoolSource, ZebraIndexerMempoolSourceOptions, ZebraIndexerSourceTarget,
};
pub use zebra_json_rpc::{
    DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, UpstreamTransactionLookup, ZebraJsonRpcSource,
    ZebraJsonRpcSourceOptions,
};
