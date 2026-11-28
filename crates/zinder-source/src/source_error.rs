//! Source boundary error vocabulary.
//!
//! Variants describe *what the upstream node did* (or failed to do). They do
//! not encode loop-lifecycle decisions: the long-running writer loops in
//! `zinder-ingest` own that decision via
//! [`source_recovery::decide_recovery`](../../../services/zinder-ingest/src/source_recovery.rs).
//!
//! Each variant maps onto exactly one [`SourceFailureClass`]; the class is the
//! operator-facing label that flows into readiness payloads, metrics, and
//! recovery backoff selection.

use thiserror::Error;
use zinder_core::BlockHeight;

use crate::NodeCapability;

/// Error returned while normalizing upstream node source values.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SourceError {
    /// Display-order block hash hex could not be decoded.
    #[error("block hash is not valid hex: {reason}")]
    InvalidBlockHashHex {
        /// Hex decoding failure.
        reason: String,
    },

    /// Raw block hex returned by the node could not be decoded.
    ///
    /// Prefer this variant when the `getblock` payload is malformed at the hex
    /// layer (odd-length, non-hex digit). Once the bytes decode successfully
    /// but fail a Zcash invariant (coinbase height, parse, timestamp range),
    /// reach for the matching `RawBlock*` variant instead. Pure JSON-RPC
    /// envelope violations (missing fields, wrong types) belong in
    /// [`Self::SourceProtocolMismatch`].
    #[error("raw block is not valid hex")]
    InvalidRawBlockHex {
        /// Hex decoding failure.
        #[source]
        source: hex::FromHexError,
    },

    /// Raw transaction hex returned by the node could not be decoded.
    ///
    /// Distinct from [`Self::InvalidRawBlockHex`] because the metric class
    /// label aggregates by error variant; mempool hydration failures must
    /// not be charged to the block-decode counter.
    #[error("raw transaction is not valid hex")]
    InvalidRawTransactionHex {
        /// Hex decoding failure.
        #[source]
        source: hex::FromHexError,
    },

    /// Display-order block hash decoded to the wrong byte length.
    #[error("block hash must be 32 bytes, got {byte_count}")]
    InvalidBlockHashLength {
        /// Decoded byte length.
        byte_count: usize,
    },

    /// Display-order transaction id hex could not be decoded.
    #[error("transaction id is not valid hex: {reason}")]
    InvalidTransactionIdHex {
        /// Hex decoding failure.
        reason: String,
    },

    /// Display-order transaction id decoded to the wrong byte length.
    #[error("transaction id must be 32 bytes, got {byte_count}")]
    InvalidTransactionIdLength {
        /// Decoded byte length.
        byte_count: usize,
    },

    /// Subtree root hex could not be decoded.
    #[error("subtree root is not valid hex")]
    InvalidSubtreeRootHex {
        /// Hex decoding failure.
        #[source]
        source: hex::FromHexError,
    },

    /// Subtree root decoded to the wrong byte length.
    #[error("subtree root must be 32 bytes, got {byte_count}")]
    InvalidSubtreeRootLength {
        /// Decoded byte length.
        byte_count: usize,
    },

    /// A final note-commitment root was not valid hex.
    #[error("{protocol:?} final note-commitment root is not valid hex")]
    InvalidFinalNoteCommitmentRootHex {
        /// Shielded protocol whose root was invalid.
        protocol: zinder_core::ShieldedProtocol,
        /// Hex decoding failure.
        #[source]
        source: hex::FromHexError,
    },

    /// A final note-commitment root decoded to a length other than 32 bytes.
    #[error("{protocol:?} final note-commitment root must be 32 bytes, got {byte_count}")]
    InvalidFinalNoteCommitmentRootLength {
        /// Shielded protocol whose root had the wrong length.
        protocol: zinder_core::ShieldedProtocol,
        /// Decoded byte length.
        byte_count: usize,
    },

    /// A note-commitment frontier did not have the required response shape.
    #[error("{protocol:?} note-commitment frontier is malformed: {reason}")]
    MalformedCommitmentTreeFrontier {
        /// Shielded protocol whose frontier was malformed.
        protocol: zinder_core::ShieldedProtocol,
        /// Frontier invariant that was violated.
        reason: &'static str,
    },

    /// A note-commitment frontier was not valid hex.
    #[error("{protocol:?} note-commitment frontier is not valid hex")]
    InvalidCommitmentTreeFrontierHex {
        /// Shielded protocol whose frontier was invalid.
        protocol: zinder_core::ShieldedProtocol,
        /// Hex decoding failure.
        #[source]
        source: hex::FromHexError,
    },

    /// A note-commitment frontier exceeded the maximum canonical RPC length.
    #[error(
        "{protocol:?} note-commitment frontier exceeds {max_byte_count} bytes: got {byte_count}"
    )]
    CommitmentTreeFrontierTooLarge {
        /// Shielded protocol whose frontier was oversized.
        protocol: zinder_core::ShieldedProtocol,
        /// Actual decoded frontier length.
        byte_count: usize,
        /// Maximum canonical frontier length.
        max_byte_count: usize,
    },

    /// A note-commitment frontier was not a canonical legacy tree encoding.
    #[error("{protocol:?} note-commitment frontier encoding is invalid: {reason}")]
    InvalidCommitmentTreeFrontierEncoding {
        /// Shielded protocol whose frontier was invalid.
        protocol: zinder_core::ShieldedProtocol,
        /// Encoding invariant that was violated.
        reason: &'static str,
    },

    /// A decoded note-commitment frontier cannot be represented by Zinder.
    #[error("{protocol:?} note-commitment tree size {tree_size} exceeds u32")]
    CommitmentTreeSizeOutOfRange {
        /// Shielded protocol whose tree was too large.
        protocol: zinder_core::ShieldedProtocol,
        /// Decoded tree size.
        tree_size: u64,
    },

    /// A note-commitment frontier did not match its advertised final root.
    #[error("{protocol:?} note-commitment frontier does not match finalRoot")]
    CommitmentTreeFrontierRootMismatch {
        /// Shielded protocol whose root did not match.
        protocol: zinder_core::ShieldedProtocol,
    },

    /// A pool's frontier presence disagreed with its activation height.
    #[error("{protocol:?} note-commitment frontier activation mismatch at {height:?}: {reason}")]
    CommitmentTreeFrontierActivationMismatch {
        /// Shielded protocol whose presence was invalid.
        protocol: zinder_core::ShieldedProtocol,
        /// Checkpoint height being validated.
        height: BlockHeight,
        /// Presence invariant that was violated.
        reason: &'static str,
    },

    /// Raw block bytes did not contain a valid serialized Zcash block header.
    ///
    /// Prefer this variant when bytes decoded from hex successfully but the
    /// header prefix is truncated or malformed. Full-block and transaction
    /// validation belongs to canonical preparation after the source adapter has
    /// established the ordered header identity. For hex-layer failures use
    /// [`Self::InvalidRawBlockHex`]; for JSON-RPC envelope violations use
    /// [`Self::SourceProtocolMismatch`].
    #[error("raw block parse failed: {reason}")]
    RawBlockParseFailed {
        /// Parser failure reason.
        reason: String,
    },

    /// Raw transaction bytes could not be parsed as a Zcash transaction.
    #[error("raw transaction parse failed: {reason}")]
    RawTransactionParseFailed {
        /// Parser failure reason.
        reason: String,
    },

    /// A transaction component index cannot be represented on public wires.
    #[error("{component} index exceeds u32")]
    TransactionComponentIndexOverflow {
        /// Component whose zero-based index overflowed.
        component: &'static str,
    },

    /// Parsed raw block timestamp could not be represented as Unix seconds.
    #[error("raw block time is outside the supported Unix-seconds range")]
    RawBlockTimeOutOfRange,

    /// Configured upstream node could not answer a request.
    ///
    /// Construct this variant when the upstream node is unreachable, the
    /// transport timed out, or the JSON-RPC error code indicates the node
    /// itself is not in a state to respond (e.g. warming up). Use
    /// [`Self::BlockUnavailable`] instead when the node responded but the
    /// requested height is no longer addressable (best-chain race during a
    /// reorg).
    #[error("upstream node is unavailable: {reason}")]
    NodeUnavailable {
        /// Node or transport failure reason.
        reason: String,
    },

    /// The upstream node refused a JSON-RPC request with HTTP 429.
    ///
    /// Zebra answers HTTP 429 (`Too many connections`) when its JSON-RPC
    /// server already holds its configured connection limit, and a rate
    /// limiter in front of the node answers the same way. The node is up, so
    /// the request is retryable after a brief backoff, and the HTTP transport
    /// itself is healthy: callers must not rebuild their connection pool,
    /// because new connections add to the load the node refused.
    #[error("upstream node rate-limited {operation}: HTTP 429 Too Many Requests")]
    NodeRateLimited {
        /// Node method the node refused.
        operation: &'static str,
    },

    /// A JSON-RPC response exceeded the configured adapter response limit.
    ///
    /// Batched source adapters should split bounded segments before surfacing
    /// this to callers. If a single source item still exceeds the limit, the
    /// operator must raise `node.max_response_bytes` or switch to a source feed
    /// that does not require large JSON payloads.
    #[error(
        "source response exceeded node.max_response_bytes during {operation}: limit={max_response_bytes}"
    )]
    SourceResponseTooLarge {
        /// Source operation that exceeded the configured response limit.
        operation: &'static str,
        /// Configured response limit in bytes.
        max_response_bytes: u64,
    },

    /// The node refused to send an answer larger than its own response limit.
    ///
    /// Zebra and Zakura answer JSON-RPC error -32008 instead of a response
    /// that would exceed their `rpc.max_response_body_size`. That limit
    /// belongs to the node, not to `node.max_response_bytes`
    /// ([`Self::SourceResponseTooLarge`]). The node is healthy and gives the
    /// same answer to the same request, so the caller narrows the request or
    /// the operator raises the node's limit. The source reports it for
    /// transparent-address lookups, which Zakura answers without a row limit.
    #[error(
        "node refused to send its {operation} answer because it exceeds the node's response size limit: {reason}"
    )]
    NodeResponseTooLarge {
        /// Node method whose answer was too large.
        operation: &'static str,
        /// Node error message.
        reason: String,
    },

    /// The configured upstream node does not support a required capability.
    #[error("node capability is missing: {capability}")]
    NodeCapabilityMissing {
        /// Missing node capability.
        capability: NodeCapability,
    },

    /// The wired transaction broadcaster is a no-op.
    ///
    /// Returned by the unit `TransactionBroadcaster` impl to let query layers
    /// distinguish a deliberate read-only configuration from a real upstream
    /// node failure.
    #[error("transaction broadcast is disabled")]
    TransactionBroadcastDisabled,

    /// The selected node authentication mode is unsupported by this source.
    #[error("node source {source_name} does not support {auth_scheme} authentication")]
    UnsupportedNodeAuth {
        /// Node source name.
        source_name: &'static str,
        /// Unsupported authentication scheme.
        auth_scheme: &'static str,
    },

    /// The node's chain tip changed during every attempt of a request whose
    /// answer depends on that tip.
    ///
    /// The node bounds a `getaddresstxids` height range by the tip it sees
    /// when it answers. The source reads the tip before and after the
    /// request and accepts the answer, or a refusal of its size, only when
    /// both reads name the same block, because a tip that fell below the
    /// range in between makes the node answer for its new tip instead. It is
    /// classed [`SourceFailureClass::UpstreamViewChanged`], so the caller
    /// retries against the node's fresh tip.
    #[error("chain tip changed during each of {attempts} {operation} attempts")]
    ChainTipChangedDuringFetch {
        /// Node method whose answer depends on the tip.
        operation: &'static str,
        /// Number of attempts made.
        attempts: u32,
    },

    /// The node committed a block while it read its address index, and
    /// refused to answer from a mix of the old and new state.
    ///
    /// Zebra and Zakura refuse `getaddressbalance`, and some
    /// `getaddresstxids` and `getaddressutxos` lookups, with an error that
    /// says `state was committing a block`. The node is healthy, so it is
    /// classed [`SourceFailureClass::UpstreamViewChanged`] and the caller
    /// retries the same node.
    #[error("node committed a block while answering {operation}: {reason}")]
    BlockCommittedDuringRead {
        /// Node method the node refused.
        operation: &'static str,
        /// Node error message.
        reason: String,
    },

    /// The node responded but the requested block could not be returned.
    ///
    /// Most commonly produced by a best-chain race: the requested height was
    /// in the upstream's best chain when Zinder resolved it, but the chain
    /// changed before the follow-up block fetch. Long-running writer loops
    /// must treat this as a re-observation signal (refresh tip, re-fetch),
    /// not a fatal exit.
    #[error("block at height {height:?} is unavailable: {reason}")]
    BlockUnavailable {
        /// Requested block height.
        height: BlockHeight,
        /// Node error message.
        reason: String,
    },

    /// Concurrent observations of the same height disagreed.
    ///
    /// Produced when two reads of the same height observe different blocks
    /// because the upstream chain reorged between the requests, for example a
    /// height-keyed `z_gettreestate` answer that names another block than the
    /// `getblock` answer before it. Distinct from
    /// [`Self::SourceProtocolMismatch`] (a wire-contract violation: a broken
    /// node) and [`Self::BlockUnavailable`] (a height that left the best chain
    /// before any fetch landed). The loop treats this as a re-observation
    /// signal under the
    /// [`SourceFailureClass::UpstreamViewChanged`] class.
    #[error("upstream chain reorged during concurrent fetch at height {height:?}: {reason}")]
    BlockReorgDuringFetch {
        /// Requested block height.
        height: BlockHeight,
        /// Names which cross-call invariant failed (for example,
        /// tree-state-vs-block hash disagreement).
        reason: &'static str,
    },

    /// The node returned an error for required subtree roots.
    #[error("{protocol:?} subtree roots from {start_index:?} are unavailable: {reason}")]
    SubtreeRootsUnavailable {
        /// Shielded protocol requested.
        protocol: zinder_core::ShieldedProtocol,
        /// First requested subtree-root index.
        start_index: zinder_core::SubtreeRootIndex,
        /// Node error message.
        reason: String,
    },

    /// The node permanently lacks the commitment-tree state at `height`.
    ///
    /// A node built by a fast-sync path that skips per-height trees refuses
    /// tree-state reads below its last checkpoint. Retrying the same node
    /// cannot succeed, so this is classed as a missing capability rather than
    /// a changed upstream view.
    #[error("tree state at height {height:?} is permanently unavailable from this node: {reason}")]
    HistoricalTreeStateUnavailable {
        /// Requested block height.
        height: BlockHeight,
        /// Node error message.
        reason: String,
    },

    /// The node knows the block at `height` but cannot return its complete
    /// body.
    ///
    /// Zebra 7.1.0 and later refuse to serve a stored block whose
    /// transactions are incomplete or whose stored metadata is inconsistent,
    /// instead of returning a partial block. A node that keeps every block
    /// body raises this only when its store is damaged, so retrying the same
    /// node cannot succeed. It is classed as a missing capability rather than
    /// a changed upstream view.
    #[error("block body at height {height:?} is permanently unavailable from this node: {reason}")]
    HistoricalBlockBodyUnavailable {
        /// Requested block height.
        height: BlockHeight,
        /// Node error message.
        reason: String,
    },

    /// The node permanently lacks the requested shielded subtree roots.
    ///
    /// A node built by a fast-sync path that skips per-height trees never
    /// records subtrees completed below its last checkpoint, and continued
    /// synchronization does not restore them.
    #[error(
        "{protocol:?} subtree roots from {start_index:?} are permanently unavailable from this node: {reason}"
    )]
    HistoricalSubtreeRootsUnavailable {
        /// Shielded protocol requested.
        protocol: zinder_core::ShieldedProtocol,
        /// First requested subtree-root index.
        start_index: zinder_core::SubtreeRootIndex,
        /// Node error message.
        reason: String,
    },

    /// The node cannot answer transparent-address lookups.
    ///
    /// A Zakura node in pruned storage mode with checkpoint sync stops
    /// maintaining its transparent address indexes and refuses
    /// `getaddressbalance`, `getaddresstxids`, and `getaddressutxos` instead
    /// of answering from partial data. A node without these methods is
    /// reported the same way. Retrying the same node cannot succeed.
    #[error("transparent address lookups are unavailable from this node: {reason}")]
    TransparentAddressIndexesUnavailable {
        /// Node error message.
        reason: String,
    },

    /// A transparent-address lookup matched more rows than one answer holds.
    ///
    /// The caller narrows the lookup and retries: a shorter height range for
    /// transaction ids, or fewer addresses for unspent outputs.
    #[error("{operation} matched more than {max_rows} rows; narrow the lookup")]
    TransparentAddressRowLimitExceeded {
        /// Node method that matched too many rows.
        operation: &'static str,
        /// Most rows one answer holds.
        max_rows: u32,
    },

    /// A transparent-address lookup targets another network than the source.
    #[error(
        "transparent address lookup for {lookup_network:?} sent to a {source_network:?} node source"
    )]
    TransparentAddressLookupNetworkMismatch {
        /// Network the lookup was validated for.
        lookup_network: zinder_core::Network,
        /// Network the node source serves.
        source_network: zinder_core::Network,
    },

    /// The node response did not match the expected JSON-RPC contract.
    ///
    /// Prefer this variant when the JSON-RPC envelope or response shape is
    /// wrong: missing fields, wrong types, header height disagreeing with the
    /// requested height, tree-state hash disagreeing with the anchor hash. For
    /// hex-layer failures use [`Self::InvalidRawBlockHex`]; for byte-level
    /// parse failures use [`Self::RawBlockParseFailed`].
    #[error("source protocol mismatch: {reason}")]
    SourceProtocolMismatch {
        /// Protocol mismatch reason.
        reason: &'static str,
    },

    /// A source payload could not be encoded for downstream storage.
    #[error("source payload could not be encoded as JSON")]
    SourcePayloadEncodingFailed {
        /// JSON encoding failure.
        #[source]
        source: serde_json::Error,
    },

    /// The upstream mempool source stream closed or failed.
    ///
    /// Returned when the gRPC `MempoolChange` stream terminates with a
    /// transport error or with `RecvError::Lagged`. Adapters that wrap a
    /// [`crate::MempoolSource`] for durable consumption must reconnect and
    /// snapshot the mempool state because lagged events are not replayed.
    #[error("upstream mempool source stream is unavailable: {reason}")]
    MempoolStreamUnavailable {
        /// Stream or transport failure reason.
        reason: String,
    },

    /// Hydrating an `Added` mempool observation failed.
    ///
    /// Returned when fetching the raw transaction bytes for an
    /// `ADDED` source change fails (e.g. JSON-RPC `getrawtransaction`
    /// returns an error). The adapter increments
    /// `zinder_mempool_hydration_failures_total` before yielding this
    /// error.
    #[error("mempool transaction hydration failed: {reason}")]
    MempoolHydrationFailed {
        /// Identifier of the unhydrated transaction.
        transaction_id: zinder_core::TransactionId,
        /// Hydration failure reason.
        reason: String,
    },

    /// The upstream mempool contains more transactions than this adapter admits.
    #[error(
        "mempool transaction count exceeds ingest.mempool.max_transaction_count: count={transaction_count}, limit={max_transaction_count}"
    )]
    MempoolTransactionCountLimitExceeded {
        /// Distinct transaction count observed in the upstream mempool.
        transaction_count: u64,
        /// Configured transaction-count ceiling.
        max_transaction_count: u64,
    },

    /// The upstream mempool contains more raw transaction bytes than this adapter admits.
    #[error(
        "mempool raw transaction bytes exceed ingest.mempool.max_total_raw_transaction_bytes: bytes={total_raw_transaction_bytes}, limit={max_total_raw_transaction_bytes}"
    )]
    MempoolRawTransactionBytesLimitExceeded {
        /// Cumulative raw transaction bytes observed in the upstream mempool.
        total_raw_transaction_bytes: u64,
        /// Configured cumulative raw-byte ceiling.
        max_total_raw_transaction_bytes: u64,
    },

    /// Upstream chain-tip notification stream is unavailable.
    ///
    /// Returned when the gRPC `ChainTipChange` subscription cannot be
    /// established or terminates with a transport error. A consumer treats
    /// this as a signal to keep polling and re-subscribe in the background.
    #[error("upstream chain-tip notification stream is unavailable: {reason}")]
    ChainTipStreamUnavailable {
        /// Stream or transport failure reason.
        reason: String,
    },
}

/// Operator-facing classification of an upstream failure.
///
/// The classification describes *what the source observed*, not what the
/// caller should do. Writer-loop lifecycle decisions live in
/// `services/zinder-ingest/src/source_recovery.rs`; readiness payloads,
/// metrics labels, and recovery backoff selection consume this enum to
/// surface a stable signal to operators and dashboards.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SourceFailureClass {
    /// Transport, authentication, or upstream-node warming-up failure.
    /// The node itself cannot answer; retry the same call after a brief
    /// backoff.
    NodeUnreachable,
    /// The node is up but refused the request with HTTP 429 because it is
    /// at its connection or request limit. Retry after a brief backoff, or
    /// fail over to another node; do not rebuild the transport.
    NodeRateLimited,
    /// The upstream answered, but the request targeted a view that is no
    /// longer current (best-chain race, height not in best chain, txid
    /// dropped between observation and lookup). Re-observe the upstream's
    /// fresh state before retrying dependent requests.
    UpstreamViewChanged,
    /// A long-lived subscription (chain-tip or mempool notification stream)
    /// disconnected or failed to open. Reconnect with backoff; the writer
    /// loop continues to poll while the stream is down.
    StreamDisconnected,
    /// The upstream is missing a capability ingest needs (e.g. a probed RPC
    /// method is absent). Operator action: upgrade the node or change the
    /// configured source.
    CapabilityMissing,
    /// The upstream's response did not match the expected JSON-RPC or wire
    /// contract (missing fields, hash mismatch, height disagreement after
    /// parse). Operator action: investigate the upstream version.
    ProtocolMismatch,
    /// Bytes returned by the upstream could not be decoded or parsed as
    /// valid Zcash data. Operator action: investigate the upstream version.
    Malformed,
    /// Adapter configuration or a request bound refused the operation
    /// (unsupported auth scheme, broadcast disabled by deliberate read-only
    /// configuration, a response or row limit exceeded).
    Configuration,
}

impl SourceFailureClass {
    /// Every label this enum may render through [`Self::label`].
    ///
    /// Iterated by the readiness ops endpoint to zero out
    /// `zinder_readiness_node_failure_class` gauge cells for inactive
    /// classes on every scrape. The unit test
    /// `label_appears_in_all_labels` enforces parity with [`Self::label`],
    /// so adding a new variant without extending this slice fails CI.
    pub const ALL_LABELS: &'static [&'static str] = &[
        "node_unreachable",
        "node_rate_limited",
        "upstream_view_changed",
        "stream_disconnected",
        "capability_missing",
        "protocol_mismatch",
        "malformed",
        "configuration",
    ];

    /// Stable kebab-case label suitable for metrics, logs, and readiness
    /// payloads. The label is the operator-facing identifier; do not
    /// rename without coordinating dashboards and alert rules.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NodeUnreachable => "node_unreachable",
            Self::NodeRateLimited => "node_rate_limited",
            Self::UpstreamViewChanged => "upstream_view_changed",
            Self::StreamDisconnected => "stream_disconnected",
            Self::CapabilityMissing => "capability_missing",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::Malformed => "malformed",
            Self::Configuration => "configuration",
        }
    }
}

impl SourceError {
    /// Returns true when the upstream node lacks a capability the request
    /// requires, a condition operator reconfiguration must clear.
    ///
    /// This covers every error classed
    /// [`SourceFailureClass::CapabilityMissing`], including the permanent
    /// historical tree-state and subtree-root refusals of a fast-synced node
    /// and the refusal of a stored block body the node cannot reassemble.
    #[must_use]
    pub const fn is_node_capability_missing(&self) -> bool {
        matches!(
            self.upstream_classification(),
            SourceFailureClass::CapabilityMissing
        )
    }

    /// Returns the operator-facing class describing what the upstream did.
    ///
    /// Classification is descriptive: the class names a kind of failure, not
    /// a recommended action. Loop lifecycle decisions belong to the writer
    /// loop, which composes this class with its own policy.
    #[must_use]
    pub const fn upstream_classification(&self) -> SourceFailureClass {
        match self {
            Self::NodeUnavailable { .. } => SourceFailureClass::NodeUnreachable,
            Self::NodeRateLimited { .. } => SourceFailureClass::NodeRateLimited,
            Self::SourceResponseTooLarge { .. }
            | Self::NodeResponseTooLarge { .. }
            | Self::MempoolTransactionCountLimitExceeded { .. }
            | Self::MempoolRawTransactionBytesLimitExceeded { .. }
            | Self::TransactionBroadcastDisabled
            | Self::UnsupportedNodeAuth { .. }
            | Self::TransparentAddressRowLimitExceeded { .. }
            | Self::TransparentAddressLookupNetworkMismatch { .. } => {
                SourceFailureClass::Configuration
            }
            Self::BlockUnavailable { .. }
            | Self::BlockReorgDuringFetch { .. }
            | Self::ChainTipChangedDuringFetch { .. }
            | Self::BlockCommittedDuringRead { .. }
            | Self::SubtreeRootsUnavailable { .. }
            | Self::MempoolHydrationFailed { .. } => SourceFailureClass::UpstreamViewChanged,
            Self::MempoolStreamUnavailable { .. } | Self::ChainTipStreamUnavailable { .. } => {
                SourceFailureClass::StreamDisconnected
            }
            Self::NodeCapabilityMissing { .. }
            | Self::HistoricalTreeStateUnavailable { .. }
            | Self::HistoricalBlockBodyUnavailable { .. }
            | Self::HistoricalSubtreeRootsUnavailable { .. }
            | Self::TransparentAddressIndexesUnavailable { .. } => {
                SourceFailureClass::CapabilityMissing
            }
            Self::SourceProtocolMismatch { .. } => SourceFailureClass::ProtocolMismatch,
            Self::InvalidBlockHashHex { .. }
            | Self::InvalidRawBlockHex { .. }
            | Self::InvalidRawTransactionHex { .. }
            | Self::InvalidBlockHashLength { .. }
            | Self::InvalidTransactionIdHex { .. }
            | Self::InvalidTransactionIdLength { .. }
            | Self::InvalidSubtreeRootHex { .. }
            | Self::InvalidSubtreeRootLength { .. }
            | Self::InvalidFinalNoteCommitmentRootHex { .. }
            | Self::InvalidFinalNoteCommitmentRootLength { .. }
            | Self::MalformedCommitmentTreeFrontier { .. }
            | Self::InvalidCommitmentTreeFrontierHex { .. }
            | Self::CommitmentTreeFrontierTooLarge { .. }
            | Self::InvalidCommitmentTreeFrontierEncoding { .. }
            | Self::CommitmentTreeSizeOutOfRange { .. }
            | Self::CommitmentTreeFrontierRootMismatch { .. }
            | Self::CommitmentTreeFrontierActivationMismatch { .. }
            | Self::RawBlockParseFailed { .. }
            | Self::RawTransactionParseFailed { .. }
            | Self::TransactionComponentIndexOverflow { .. }
            | Self::RawBlockTimeOutOfRange
            | Self::SourcePayloadEncodingFailed { .. } => SourceFailureClass::Malformed,
        }
    }
}

#[cfg(test)]
mod source_failure_class_tests {
    use super::SourceFailureClass;

    #[test]
    fn label_appears_in_all_labels() {
        for class in [
            SourceFailureClass::NodeUnreachable,
            SourceFailureClass::NodeRateLimited,
            SourceFailureClass::UpstreamViewChanged,
            SourceFailureClass::StreamDisconnected,
            SourceFailureClass::CapabilityMissing,
            SourceFailureClass::ProtocolMismatch,
            SourceFailureClass::Malformed,
            SourceFailureClass::Configuration,
        ] {
            assert!(
                SourceFailureClass::ALL_LABELS.contains(&class.label()),
                "label {} for {class:?} missing from ALL_LABELS",
                class.label(),
            );
        }
        assert_eq!(SourceFailureClass::ALL_LABELS.len(), 8);
    }
}
