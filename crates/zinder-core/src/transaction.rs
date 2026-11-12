//! Transaction identity, submission, and durable artifact values.

use crate::{
    BlockHash, BlockHeight, ChainEpoch, ConsensusBranchId, MempoolEntry, TransactionPublicFacts,
    TransparentInputFact, TransparentOutputFact,
};

/// Zcash transaction identifier bytes.
///
/// Pre-v5 transactions use the `SHA256d` hash of the serialized transaction;
/// v5+ transactions use the `BLAKE2b-256` `txid_digest` defined by ZIP-244.
/// Both forms are 32 bytes and are addressed as the same canonical
/// identifier on the wire and in storage; Zinder treats the bytes as opaque
/// and does not recompute them.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionId([u8; 32]);

impl TransactionId {
    /// Creates a transaction identifier from canonical 32-byte id material.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the transaction identifier bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// ZIP-244 transaction authorization digest bytes.
///
/// Populated for v5+ transactions. The legacy v1-v4 transactions have no
/// distinct authorization digest because their txid already covers their
/// witness data; on those source observations the mempool entry's
/// authorization digest is `None`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AuthDigest([u8; 32]);

impl AuthDigest {
    /// Creates an authorization digest from canonical 32-byte material.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the authorization digest bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Maximum serialized length, in bytes, of a raw transaction accepted for
/// broadcast.
///
/// A post-Sapling Zcash transaction cannot exceed the maximum block size, since
/// a transaction larger than a block could never be mined. The bound matches
/// The bound comes from [`zcash_protocol::constants::MAX_BLOCK_BYTES`]. The
/// per-transaction limit is in practice a few bytes smaller (a block also
/// carries a header and a transaction count), so the block size is a safe
/// upper bound for the broadcast guard.
pub const MAX_RAW_TRANSACTION_BYTES: usize = zcash_protocol::constants::MAX_BLOCK_BYTES;

/// Raw serialized Zcash transaction bytes submitted by a wallet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawTransactionBytes(Vec<u8>);

impl RawTransactionBytes {
    /// Creates raw transaction bytes from serialized transaction material.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the raw serialized transaction bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the serialized transaction length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the serialized transaction carries no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Result of submitting a raw transaction to a node or network path.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransactionBroadcastOutcome {
    /// The node accepted the transaction for mempool admission or relay.
    Accepted(BroadcastAccepted),
    /// The node reported the transaction was already known.
    Duplicate(BroadcastDuplicate),
    /// The node could not decode the submitted transaction bytes.
    InvalidEncoding(BroadcastInvalidEncoding),
    /// The node has the transaction queued for download or verification.
    ///
    /// Distinct from [`TransactionBroadcastOutcome::Duplicate`]: queued means
    /// the upstream node has already accepted the broadcast into its
    /// download or verification queue but has not yet produced a final
    /// verdict. Callers that submit the same bytes again while a prior
    /// submission is still in flight observe this state instead of a
    /// hard rejection.
    Queued(BroadcastQueued),
    /// The node rejected the transaction with a typed reason.
    Rejected(BroadcastRejected),
    /// The node returned an unclassified broadcast response.
    Unknown(BroadcastUnknown),
}

/// Accepted transaction broadcast details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BroadcastAccepted {
    /// Transaction identifier reported by the node.
    pub transaction_id: TransactionId,
}

/// Duplicate transaction broadcast details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastDuplicate {
    /// Node error code when one was supplied.
    pub error_code: Option<i64>,
    /// Operator-facing node message.
    pub message: String,
}

/// Invalid transaction encoding details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastInvalidEncoding {
    /// Node error code when one was supplied.
    pub error_code: Option<i64>,
    /// Operator-facing node message.
    pub message: String,
}

/// Rejected transaction broadcast details.
///
/// Carries a [`BroadcastRejectionReason`] so callers can match the typed
/// reason without substring-checking the operator-facing message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastRejected {
    /// Typed rejection reason.
    pub kind: BroadcastRejectionReason,
    /// Node error code when one was supplied.
    pub error_code: Option<i64>,
    /// Operator-facing node message.
    pub message: String,
}

/// Typed broadcast rejection reason.
///
/// Upstream Zebra reports mempool rejections through a single JSON-RPC error
/// code (`-25 Verify`) with the original `MempoolError` variant collapsed
/// into the error message. The submitter normalizes that message into one
/// of these variants so downstream consumers (auto-shield retry loops,
/// metrics labels, lightwalletd compat) can dispatch on the typed reason.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum BroadcastRejectionReason {
    /// Node returned a rejection that did not match any known reason.
    #[default]
    Unknown,
    /// Verifier rejected one or more transaction signatures.
    InvalidSignature,
    /// Transaction's `nExpiryHeight` is at or below the visible tip.
    BadExpiryHeight,
    /// Transaction's consensus branch id does not match the network upgrade.
    BadConsensusBranch,
    /// Mempool is at capacity and refused the transaction.
    MempoolFull,
    /// Transaction version is not supported by the network upgrade that
    /// applies to the next block (for example v4 from NU7 under ZIP 2003, or
    /// v5 before NU5). The transaction must be rebuilt; re-signing cannot fix
    /// it.
    UnsupportedTransactionVersion,
    /// Transaction alone exceeds a ZIP 218 per-block shielded action limit,
    /// so no block can include it. The payment must be split across
    /// transactions.
    ExceedsBlockActionLimit,
}

/// Queued transaction broadcast details.
///
/// The node has accepted the broadcast into a download or verification
/// queue but has not produced a final verdict. Re-submitting the same
/// byte-identical transaction while the prior submission is in flight
/// produces this state on Zebra.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastQueued {
    /// Operator-facing node message describing the queued state.
    pub message: String,
}

/// Unclassified transaction broadcast details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastUnknown {
    /// Node error code when one was supplied.
    pub error_code: Option<i64>,
    /// Operator-facing node message.
    pub message: String,
}

/// Canonical location of a mined transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionLocation {
    /// Transaction identifier.
    pub transaction_id: TransactionId,
    /// Height of the containing block.
    pub block_height: BlockHeight,
    /// Hash of the containing block.
    pub block_hash: BlockHash,
    /// Block-local transaction index.
    pub tx_index_in_block: u32,
}

impl TransactionLocation {
    /// Creates a mined transaction location.
    #[must_use]
    pub const fn new(
        transaction_id: TransactionId,
        block_height: BlockHeight,
        block_hash: BlockHash,
        tx_index_in_block: u32,
    ) -> Self {
        Self {
            transaction_id,
            block_height,
            block_hash,
            tx_index_in_block,
        }
    }
}

/// Canonical public transaction facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionFactsArtifact {
    /// Location of the mined transaction.
    pub location: TransactionLocation,
    /// Public facts parsed from the transaction.
    pub public_facts: TransactionPublicFacts,
    /// Ordered transparent inputs observed in the transaction.
    pub transparent_inputs: Vec<TransparentInputFact>,
    /// Ordered transparent outputs observed in the transaction.
    pub transparent_outputs: Vec<TransparentOutputFact>,
}

impl TransactionFactsArtifact {
    /// Creates canonical transaction facts.
    #[must_use]
    pub const fn new(location: TransactionLocation, public_facts: TransactionPublicFacts) -> Self {
        Self {
            location,
            public_facts,
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        }
    }

    /// Attaches the transaction-local transparent facts parsed by ingest.
    #[must_use]
    pub fn with_transparent_facts(
        mut self,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> Self {
        self.transparent_inputs = transparent_inputs;
        self.transparent_outputs = transparent_outputs;
        self
    }
}

/// Signed transaction-intrinsic shielded value balances.
///
/// Values use Zebra's transaction-pool sign convention: positive value enters
/// the transaction from the named pool, while negative value leaves the
/// transaction for that pool. Transparent value is deliberately absent
/// because computing it requires resolving previous outputs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionIntrinsicValueBalances {
    /// Sprout `sum(vpub_new) - sum(vpub_old)` in zatoshi.
    pub sprout_zat: i64,
    /// Sapling `valueBalanceSapling` in zatoshi.
    pub sapling_zat: i64,
    /// Orchard `valueBalanceOrchard` in zatoshi.
    pub orchard_zat: i64,
    /// Ironwood `valueBalanceIronwood` in zatoshi.
    pub ironwood_zat: i64,
}

impl TransactionIntrinsicValueBalances {
    /// Creates transaction-intrinsic shielded value balances.
    #[must_use]
    pub const fn new(
        sprout_zat: i64,
        sapling_zat: i64,
        orchard_zat: i64,
        ironwood_zat: i64,
    ) -> Self {
        Self {
            sprout_zat,
            sapling_zat,
            orchard_zat,
            ironwood_zat,
        }
    }
}

/// Canonical transaction-intrinsic value-balance artifact.
///
/// The location binds source-derived intrinsic balances to a mined
/// transaction and lets historical enrichment reject stale branch data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionIntrinsicValueBalancesArtifact {
    /// Canonical location of the transaction carrying these balances.
    pub location: TransactionLocation,
    /// Signed balances parsed from the transaction's shielded bundles.
    pub value_balances: TransactionIntrinsicValueBalances,
}

impl TransactionIntrinsicValueBalancesArtifact {
    /// Creates a located transaction-intrinsic value-balance artifact.
    #[must_use]
    pub const fn new(
        location: TransactionLocation,
        value_balances: TransactionIntrinsicValueBalances,
    ) -> Self {
        Self {
            location,
            value_balances,
        }
    }
}

/// Optional cold-path raw transaction blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBlobArtifact {
    /// Location of the mined transaction.
    pub location: TransactionLocation,
    /// Serialized consensus transaction bytes.
    pub raw_transaction_bytes: Vec<u8>,
}

impl TransactionBlobArtifact {
    /// Creates a raw transaction blob.
    #[must_use]
    pub fn new(location: TransactionLocation, raw_transaction_bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            location,
            raw_transaction_bytes: raw_transaction_bytes.into(),
        }
    }
}

/// Mined-transaction enrichment fields bound to a response's [`ChainEpoch`].
///
/// `MinedTransactionChainContext` is a *response/read-model* value, not a
/// persisted field on
/// [`TransactionLocation`]. The only public constructor takes the response
/// epoch and the mined block's identity together, which prevents the racy
/// `tip_height - block_height` confirmations computation by construction:
/// the epoch is in scope when confirmations are computed, so callers cannot
/// accidentally re-read the tip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MinedTransactionChainContext {
    /// Consensus branch identifier in effect at the mined height.
    pub consensus_branch_id: ConsensusBranchId,
    /// Block-time as Unix seconds, taken from the mined block header.
    pub block_time: i64,
    /// Confirmations: `tip_height - mined_height + 1`, bound to the
    /// response's `ChainEpoch`.
    pub confirmations: u32,
}

impl MinedTransactionChainContext {
    /// Constructs the canonical mined-transaction chain context, binding
    /// confirmations to the response's chain epoch.
    ///
    /// Callers supply pre-computed `consensus_branch_id` and `block_time`;
    /// these are derived from the source-aware (zinder-source) layer that
    /// owns Zcash consensus parameters and block-header parsing. The
    /// constructor itself only computes `confirmations`, which is the
    /// quantity that depends on the response epoch.
    #[must_use]
    pub fn from_response_epoch(
        epoch: &ChainEpoch,
        mined_height: BlockHeight,
        consensus_branch_id: ConsensusBranchId,
        block_time: i64,
    ) -> Self {
        let tip = epoch.visible_tip_height.value();
        let mined = mined_height.value();
        let confirmations = tip.saturating_sub(mined).saturating_add(1);
        Self {
            consensus_branch_id,
            block_time,
            confirmations,
        }
    }
}

/// Mined-transaction read-model record carried in [`TxStatus::Mined`]
/// and the wire-side `MinedTransaction` message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MinedTransaction {
    /// Canonical mined transaction location.
    pub location: TransactionLocation,
    /// Chain context bound to the response's [`ChainEpoch`].
    pub chain_context: MinedTransactionChainContext,
    /// Serialized consensus transaction bytes.
    ///
    /// `None` when the deployment does not retain raw transaction blobs
    /// (ingest `raw_blob_policy` is `none`); `Some` when the policy is
    /// `transactions` or `all`. Symmetric with the mempool arm's hydrated
    /// bytes so a verbose mined-transaction read carries the serialized
    /// form alongside the location and confirmations.
    pub raw_transaction_bytes: Option<Vec<u8>>,
}

impl MinedTransaction {
    /// Creates a mined-transaction read-model record.
    #[must_use]
    pub fn new(
        location: TransactionLocation,
        chain_context: MinedTransactionChainContext,
        raw_transaction_bytes: Option<Vec<u8>>,
    ) -> Self {
        Self {
            location,
            chain_context,
            raw_transaction_bytes,
        }
    }
}

/// Typed transaction-status value returned by transaction lookups.
///
/// Mirrors the wire `TransactionStatusResponse.status` oneof at the
/// in-memory boundary, with one extra `NotFound` arm. `NotFound` is a
/// gRPC `NOT_FOUND` on the wire (typed errors don't waste oneof slots);
/// this Rust enum surfaces it as a value so callers can match
/// exhaustively without conflating it with an error.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "InMempool carries the full hydrated MempoolEntry by design so consumers can match typed transaction state directly. Boxing would push allocation cost into every consumer's pattern match."
)]
pub enum TxStatus {
    /// Transaction is mined in the canonical chain.
    ///
    /// Carries the durable [`TransactionLocation`] together with
    /// epoch-bound [`MinedTransactionChainContext`].
    Mined(MinedTransaction),
    /// Transaction is not indexed in the visible canonical chain.
    NotFound,
    /// Transaction is known to be in the mempool.
    InMempool(MempoolEntry),
}
