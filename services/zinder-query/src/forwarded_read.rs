//! Admission for wallet reads that a reader forwards to its node, and the
//! verified transaction bytes those reads share.
//!
//! Two layers bound forwarded work. The first is a budget of node calls:
//! each forwarded read holds a slot only while one call to the node runs, so
//! a slow client never keeps a node slot. Transaction lookups, address UTXO
//! lookups, and address history pages each have a reserved floor of that
//! budget and a cap, and share the rest. The second layer bounds what a
//! forwarded response holds while the client reads it: unspent-output
//! response bodies and open address-history streams. A request takes its
//! response slot before any node slot and never holds a node slot while it
//! waits for a response slot, so the layers cannot wait on each other.
//!
//! Each kind waits for a slot in arrival order up to the configured wait, and
//! is refused when the wait ends or when its waiting places are full.

mod slot_pool;
mod transaction_bytes;

#[cfg(test)]
mod tests;

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::time::Instant;
use zinder_source::{SourceError, SourceFailureClass, ZebraJsonRpcSource};

use crate::{MinedTransactionDigest, QueryError};
use slot_pool::{ClassShare, PoolSlot, SlotPool};
use transaction_bytes::TransactionBytes;

/// Default node calls that forwarded reads run at once, per process.
///
/// It stays well below Zebra's default limit of 100 JSON-RPC connections,
/// which ingest and other readers of the same node share.
pub const DEFAULT_FORWARDED_READ_MAX_CONCURRENT: NonZeroU32 = NonZeroU32::MIN.saturating_add(31);

/// Most node calls an operator may let forwarded reads run at once.
pub const MAX_FORWARDED_READ_MAX_CONCURRENT: NonZeroU32 = NonZeroU32::MIN.saturating_add(255);

/// Default time a forwarded read waits for a slot before it is refused.
pub const DEFAULT_FORWARDED_READ_WAIT: Duration = Duration::from_secs(10);

/// Longest wait for a slot an operator may configure.
pub const MAX_FORWARDED_READ_WAIT: Duration = Duration::from_secs(30);

/// Default requests that may wait for each slot of a kind's cap.
///
/// At 64 waiters per slot, the 8-slot UTXO cap holds up to 512 waiting calls,
/// enough for 256 concurrent wallets issuing paired UTXO lookups to queue and
/// drain well inside [`DEFAULT_FORWARDED_READ_WAIT`].
pub const DEFAULT_FORWARDED_READ_WAITERS_PER_SLOT: NonZeroU32 = NonZeroU32::MIN.saturating_add(63);

/// Most requests an operator may let wait for each slot of a kind's cap.
pub const MAX_FORWARDED_READ_WAITERS_PER_SLOT: NonZeroU32 = NonZeroU32::MIN.saturating_add(255);

/// Default size of the verified transaction byte cache: 64 MiB.
pub const DEFAULT_FORWARDED_TRANSACTION_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Largest verified transaction byte cache an operator may configure: 1 GiB.
pub const MAX_FORWARDED_TRANSACTION_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

/// Longest a forwarded address-history request runs, from its first node
/// call through the last row of its stream.
pub const FORWARDED_HISTORY_DEADLINE: Duration = Duration::from_mins(1);

/// Default unspent-output response bodies held at once, and requests that
/// may wait for one before contacting the node.
const UTXO_RESPONSE_LIMIT: ResponseLimit = ResponseLimit {
    active: NonZeroU32::MIN.saturating_add(15),
    waiters: 1_024,
};

/// Default address-history streams open at once, and requests that may wait
/// for one.
const HISTORY_STREAM_LIMIT: ResponseLimit = ResponseLimit {
    active: NonZeroU32::MIN.saturating_add(31),
    waiters: 128,
};

/// How many responses of one kind are held at once, and how many requests
/// may wait for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResponseLimit {
    active: NonZeroU32,
    waiters: u32,
}

const ACTIVE_GAUGE: &str = "zinder_forwarded_read_active";
const QUEUED_GAUGE: &str = "zinder_forwarded_read_queued";
const WAIT_HISTOGRAM: &str = "zinder_forwarded_read_wait_duration_seconds";
const REFUSALS_COUNTER: &str = "zinder_forwarded_read_refusals_total";
const CACHE_LOOKUPS_COUNTER: &str = "zinder_forwarded_transaction_cache_lookups_total";

/// Settings of a [`ForwardedReadLimit`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForwardedReadConfig {
    max_concurrent: NonZeroU32,
    wait: Duration,
    waiters_per_slot: NonZeroU32,
    transaction_cache_bytes: u64,
    utxo_responses: ResponseLimit,
    history_streams: ResponseLimit,
}

impl Default for ForwardedReadConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_FORWARDED_READ_MAX_CONCURRENT,
            wait: DEFAULT_FORWARDED_READ_WAIT,
            waiters_per_slot: DEFAULT_FORWARDED_READ_WAITERS_PER_SLOT,
            transaction_cache_bytes: DEFAULT_FORWARDED_TRANSACTION_CACHE_BYTES,
            utxo_responses: UTXO_RESPONSE_LIMIT,
            history_streams: HISTORY_STREAM_LIMIT,
        }
    }
}

impl ForwardedReadConfig {
    /// Validates operator settings, taking the default for each one unset.
    ///
    /// # Errors
    ///
    /// Returns the first setting outside its accepted range.
    pub fn resolve(
        max_concurrent: Option<u32>,
        wait_ms: Option<u64>,
        waiters_per_slot: Option<u32>,
        transaction_cache_bytes: Option<u64>,
    ) -> Result<Self, ForwardedReadConfigError> {
        let defaults = Self::default();
        let max_concurrent = match max_concurrent {
            None => defaults.max_concurrent,
            Some(configured) => NonZeroU32::new(configured)
                .filter(|slots| *slots <= MAX_FORWARDED_READ_MAX_CONCURRENT)
                .ok_or(ForwardedReadConfigError::MaxConcurrent)?,
        };
        let wait = match wait_ms {
            None => defaults.wait,
            Some(configured) => Some(Duration::from_millis(configured))
                .filter(|wait| *wait <= MAX_FORWARDED_READ_WAIT)
                .ok_or(ForwardedReadConfigError::Wait)?,
        };
        let waiters_per_slot = match waiters_per_slot {
            None => defaults.waiters_per_slot,
            Some(configured) => NonZeroU32::new(configured)
                .filter(|waiters| *waiters <= MAX_FORWARDED_READ_WAITERS_PER_SLOT)
                .ok_or(ForwardedReadConfigError::WaitersPerSlot)?,
        };
        let transaction_cache_bytes = match transaction_cache_bytes {
            None => defaults.transaction_cache_bytes,
            Some(configured) if configured <= MAX_FORWARDED_TRANSACTION_CACHE_BYTES => configured,
            Some(_) => return Err(ForwardedReadConfigError::TransactionCacheBytes),
        };
        Ok(Self {
            max_concurrent,
            wait,
            waiters_per_slot,
            transaction_cache_bytes,
            ..defaults
        })
    }

    /// Returns these settings with at most `active` responses of `kind` held
    /// at once and at most `waiters` requests waiting for one.
    ///
    /// By default 16 unspent-output response bodies (with 1,024 waiting
    /// places) and 32 address-history streams (with 128 waiting places) are
    /// held at once. Operators do not configure these limits; an embedding
    /// runtime may size them to its memory budget.
    #[must_use]
    pub const fn with_response_limit(
        self,
        kind: ForwardedResponseKind,
        active: NonZeroU32,
        waiters: u32,
    ) -> Self {
        let limit = ResponseLimit { active, waiters };
        match kind {
            ForwardedResponseKind::UtxoResponse => Self {
                utxo_responses: limit,
                ..self
            },
            ForwardedResponseKind::HistoryStream => Self {
                history_streams: limit,
                ..self
            },
        }
    }

    /// Node calls forwarded reads run at once.
    #[must_use]
    pub const fn max_concurrent(&self) -> NonZeroU32 {
        self.max_concurrent
    }

    /// Longest a forwarded read waits for a slot.
    #[must_use]
    pub const fn wait(&self) -> Duration {
        self.wait
    }

    /// Requests that may wait for each slot of a kind's cap.
    #[must_use]
    pub const fn waiters_per_slot(&self) -> NonZeroU32 {
        self.waiters_per_slot
    }

    /// Size of the verified transaction byte cache; zero disables it.
    #[must_use]
    pub const fn transaction_cache_bytes(&self) -> u64 {
        self.transaction_cache_bytes
    }
}

/// A forwarded-read setting outside its accepted range.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ForwardedReadConfigError {
    /// The node-call budget is zero or too large.
    #[error("forwarded_read_max_concurrent must be in 1..={MAX_FORWARDED_READ_MAX_CONCURRENT}")]
    MaxConcurrent,
    /// The slot wait is too long.
    #[error("forwarded_read_wait_ms must be in 0..={}", MAX_FORWARDED_READ_WAIT.as_millis())]
    Wait,
    /// The waiters per slot are zero or too many.
    #[error("forwarded_read_waiters_per_slot must be in 1..={MAX_FORWARDED_READ_WAITERS_PER_SLOT}")]
    WaitersPerSlot,
    /// The transaction cache is too large.
    #[error(
        "forwarded_transaction_cache_bytes must be in 0..={MAX_FORWARDED_TRANSACTION_CACHE_BYTES}"
    )]
    TransactionCacheBytes,
}

/// What a forwarded node call reads, which sets its share of the node-call
/// budget.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ForwardedReadKind {
    /// Mined transaction bytes, for `GetTransaction` and address history rows.
    Transaction,
    /// Unspent outputs of transparent addresses, including balance lookups.
    Utxo,
    /// Transaction IDs of transparent address history.
    History,
}

impl ForwardedReadKind {
    const ALL: [Self; 3] = [Self::Transaction, Self::Utxo, Self::History];

    const fn class(self) -> usize {
        match self {
            Self::Transaction => 0,
            Self::Utxo => 1,
            Self::History => 2,
        }
    }

    /// The `kind` metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Transaction => "transaction",
            Self::Utxo => "utxo",
            Self::History => "history",
        }
    }

    /// Parts of 32 that the kind reserves and may hold at most.
    const fn proportions(self) -> (u32, u32) {
        match self {
            Self::Transaction => (12, 24),
            Self::Utxo => (4, 8),
            Self::History => (4, 16),
        }
    }
}

/// A forwarded response that holds memory or a stream while its client reads
/// it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ForwardedResponseKind {
    /// An unspent-output response body.
    UtxoResponse,
    /// An open address-history stream.
    HistoryStream,
}

impl ForwardedResponseKind {
    /// The `kind` metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::UtxoResponse => "utxo_response",
            Self::HistoryStream => "history_stream",
        }
    }
}

/// Why a forwarded read was refused, as the refusal counter labels it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefusalReason {
    WaitersFull,
    WaitTimeout,
    NodeRateLimited,
}

impl RefusalReason {
    const ALL: [Self; 3] = [Self::WaitersFull, Self::WaitTimeout, Self::NodeRateLimited];

    const fn label(self) -> &'static str {
        match self {
            Self::WaitersFull => "waiters_full",
            Self::WaitTimeout => "wait_timeout",
            Self::NodeRateLimited => "node_rate_limited",
        }
    }
}

/// A forwarded read refused because its kind's slots stayed taken through
/// the wait or its waiting places were full.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(
    "forwarded {kind} reads are at their limit of {max_concurrent} at once; retry with backoff"
)]
pub struct ForwardedReadRefused {
    kind: &'static str,
    max_concurrent: u32,
}

impl ForwardedReadRefused {
    /// The `kind` metric label of the refused read.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    /// Most reads of the refused kind that run at once.
    #[must_use]
    pub const fn max_concurrent(&self) -> u32 {
        self.max_concurrent
    }
}

impl From<ForwardedReadRefused> for QueryError {
    fn from(refused: ForwardedReadRefused) -> Self {
        Self::ForwardedReadLimitReached {
            kind: refused.kind,
            max_concurrent: refused.max_concurrent,
        }
    }
}

/// Why verified mined transaction bytes could not be read from the node.
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ForwardedTransactionError {
    /// The node-call budget refused the read.
    #[error(transparent)]
    Refused(#[from] ForwardedReadRefused),
    /// The node does not report the transaction mined in the served block.
    #[error("node does not mine the transaction in the served block; retry with backoff")]
    OutsideServedView,
    /// The node returned bytes other than the ones ingest committed.
    #[error("node transaction bytes differ from the bytes committed to the served chain")]
    DigestMismatch,
    /// The node call failed.
    #[error("{0}")]
    Node(Arc<SourceError>),
}

impl From<ForwardedTransactionError> for QueryError {
    fn from(error: ForwardedTransactionError) -> Self {
        match error {
            ForwardedTransactionError::Refused(refused) => refused.into(),
            ForwardedTransactionError::OutsideServedView => {
                Self::Node(SourceError::NodeUnavailable {
                    reason: error.to_string(),
                })
            }
            ForwardedTransactionError::DigestMismatch => Self::ArtifactCorrupt {
                family: zinder_store::ArtifactFamily::TransactionLocation,
                reason: error.to_string(),
            },
            ForwardedTransactionError::Node(source) => Self::Node(owned_source_error(source)),
        }
    }
}

/// Takes `error` back from a result that coalesced callers shared, or rebuilds
/// an equivalent one when another caller still holds it.
///
/// The rebuilt error keeps the failure class and message of the original, so
/// every caller maps it to the same status.
#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "variants without their own failure class keep their message as an unavailable node"
)]
pub(crate) fn owned_source_error(error: Arc<SourceError>) -> SourceError {
    Arc::try_unwrap(error).unwrap_or_else(|shared| match &*shared {
        SourceError::NodeRateLimited { operation } => SourceError::NodeRateLimited { operation },
        SourceError::SourceProtocolMismatch { reason } => {
            SourceError::SourceProtocolMismatch { reason }
        }
        SourceError::InvalidRawTransactionHex { source } => {
            SourceError::InvalidRawTransactionHex { source: *source }
        }
        SourceError::SourceResponseTooLarge {
            operation,
            max_response_bytes,
        } => SourceError::SourceResponseTooLarge {
            operation,
            max_response_bytes: *max_response_bytes,
        },
        SourceError::NodeResponseTooLarge { operation, reason } => {
            SourceError::NodeResponseTooLarge {
                operation,
                reason: reason.clone(),
            }
        }
        SourceError::TransparentAddressRowLimitExceeded {
            operation,
            max_rows,
        } => SourceError::TransparentAddressRowLimitExceeded {
            operation,
            max_rows: *max_rows,
        },
        SourceError::TransparentAddressIndexesUnavailable { reason } => {
            SourceError::TransparentAddressIndexesUnavailable {
                reason: reason.clone(),
            }
        }
        SourceError::BlockCommittedDuringRead { operation, reason } => {
            SourceError::BlockCommittedDuringRead {
                operation,
                reason: reason.clone(),
            }
        }
        SourceError::ChainTipChangedDuringFetch {
            operation,
            attempts,
        } => SourceError::ChainTipChangedDuringFetch {
            operation,
            attempts: *attempts,
        },
        SourceError::NodeCapabilityMissing { capability } => SourceError::NodeCapabilityMissing {
            capability: *capability,
        },
        SourceError::HistoricalTreeStateUnavailable { height, reason } => {
            SourceError::HistoricalTreeStateUnavailable {
                height: *height,
                reason: reason.clone(),
            }
        }
        SourceError::HistoricalBlockBodyUnavailable { height, reason } => {
            SourceError::HistoricalBlockBodyUnavailable {
                height: *height,
                reason: reason.clone(),
            }
        }
        SourceError::HistoricalSubtreeRootsUnavailable {
            protocol,
            start_index,
            reason,
        } => SourceError::HistoricalSubtreeRootsUnavailable {
            protocol: *protocol,
            start_index: *start_index,
            reason: reason.clone(),
        },
        other => SourceError::NodeUnavailable {
            reason: other.to_string(),
        },
    })
}

/// Bounds the reads a process forwards to its node and holds the verified
/// transaction bytes they share.
///
/// Clones share one budget, one queue per kind, and one cache.
#[derive(Clone)]
pub struct ForwardedReadLimit {
    inner: Arc<LimitInner>,
}

struct LimitInner {
    config: ForwardedReadConfig,
    node_calls: Arc<SlotPool>,
    utxo_responses: Arc<SlotPool>,
    history_streams: Arc<SlotPool>,
    transactions: TransactionBytes,
}

impl std::fmt::Debug for ForwardedReadLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForwardedReadLimit")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl Default for ForwardedReadLimit {
    fn default() -> Self {
        Self::new(ForwardedReadConfig::default())
    }
}

impl ForwardedReadLimit {
    /// Builds the budget, queues, and cache `config` describes, and reports
    /// every kind's gauges and refusal counters at zero.
    #[must_use]
    pub fn new(config: ForwardedReadConfig) -> Self {
        let (shares, elastic) = node_call_shares(config);
        for outcome in ["hit", "coalesced", "miss"] {
            metrics::counter!(CACHE_LOOKUPS_COUNTER, "outcome" => outcome).increment(0);
        }
        let response_pool = |kind: ForwardedResponseKind, limit: ResponseLimit| {
            SlotPool::new(
                &[ClassShare {
                    label: kind.label(),
                    reserved: limit.active.get(),
                    cap: limit.active.get(),
                    max_waiters: usize::try_from(limit.waiters).unwrap_or(usize::MAX),
                }],
                0,
            )
        };
        Self {
            inner: Arc::new(LimitInner {
                config,
                node_calls: SlotPool::new(&shares, elastic),
                utxo_responses: response_pool(
                    ForwardedResponseKind::UtxoResponse,
                    config.utxo_responses,
                ),
                history_streams: response_pool(
                    ForwardedResponseKind::HistoryStream,
                    config.history_streams,
                ),
                transactions: TransactionBytes::new(
                    usize::try_from(config.transaction_cache_bytes).unwrap_or(usize::MAX),
                ),
            }),
        }
    }

    /// Returns the settings this limit was built from.
    #[must_use]
    pub fn config(&self) -> ForwardedReadConfig {
        self.inner.config
    }

    /// Returns when a read starting now stops waiting for a slot.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        Instant::now() + self.inner.config.wait
    }

    /// Runs `call` under a node-call slot of `kind`, waiting for one in
    /// arrival order until `deadline`, and returns the slot once the call
    /// returns.
    ///
    /// A call the node refuses with HTTP 429 is counted as a refusal with
    /// reason `node_rate_limited`.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardedReadRefused`] without calling the node when no slot
    /// frees before `deadline` or the kind's waiting places are full.
    pub async fn node_call<T>(
        &self,
        kind: ForwardedReadKind,
        deadline: Instant,
        call: impl Future<Output = Result<T, SourceError>>,
    ) -> Result<Result<T, SourceError>, ForwardedReadRefused> {
        let slot = self.node_call_slot(kind, deadline).await?;
        let outcome = call.await;
        drop(slot);
        if let Err(error) = &outcome
            && error.upstream_classification() == SourceFailureClass::NodeRateLimited
        {
            metrics::counter!(
                REFUSALS_COUNTER,
                "kind" => kind.label(),
                "reason" => RefusalReason::NodeRateLimited.label(),
            )
            .increment(1);
        }
        Ok(outcome)
    }

    async fn node_call_slot(
        &self,
        kind: ForwardedReadKind,
        deadline: Instant,
    ) -> Result<PoolSlot, ForwardedReadRefused> {
        let pool = &self.inner.node_calls;
        pool.acquire(kind.class(), deadline)
            .await
            .map_err(|_| ForwardedReadRefused {
                kind: kind.label(),
                max_concurrent: pool.share(kind.class()).map_or(0, |share| share.cap),
            })
    }

    /// Takes a response slot of `kind`, waiting for one in arrival order
    /// until `deadline`. The slot is held until the returned permit drops.
    ///
    /// Take it before any node call of the same request, so a request never
    /// holds a node-call slot while it waits for a response slot.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardedReadRefused`] when no slot frees before `deadline`
    /// or the kind's waiting places are full.
    pub async fn admit_response(
        &self,
        kind: ForwardedResponseKind,
        deadline: Instant,
    ) -> Result<ForwardedResponsePermit, ForwardedReadRefused> {
        let pool = match kind {
            ForwardedResponseKind::UtxoResponse => &self.inner.utxo_responses,
            ForwardedResponseKind::HistoryStream => &self.inner.history_streams,
        };
        pool.acquire(0, deadline)
            .await
            .map(|slot| ForwardedResponsePermit { _slot: slot })
            .map_err(|_| ForwardedReadRefused {
                kind: kind.label(),
                max_concurrent: pool.share(0).map_or(0, |share| share.cap),
            })
    }

    /// Returns the cached verified bytes of `mined`, without contacting the
    /// node, or `None` when they are not cached.
    #[must_use]
    pub fn cached_transaction(&self, mined: &MinedTransactionDigest) -> Option<Arc<[u8]>> {
        self.inner.transactions.cached(mined)
    }

    /// Returns the bytes of `mined` that the node reports mined in its served
    /// block, verified against the digest ingest committed.
    ///
    /// Cached bytes are returned without contacting the node or taking a
    /// slot. Otherwise concurrent callers for the same transaction and digest
    /// share one node call, run under a `kind` slot that the first caller
    /// waits for until `deadline`; a caller that joins it shares that wait,
    /// and a caller that leaves does not cancel the call for the others.
    /// `deadline` bounds only the wait for a slot, never the node call.
    /// Verified bytes are cached, failures are not.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardedTransactionError::Refused`] when no slot frees
    /// before `deadline`, [`ForwardedTransactionError::OutsideServedView`]
    /// when the node does not mine the transaction in the served block,
    /// [`ForwardedTransactionError::DigestMismatch`] when its bytes differ,
    /// and [`ForwardedTransactionError::Node`] when the call fails.
    pub async fn mined_transaction_bytes(
        &self,
        node: &ZebraJsonRpcSource,
        mined: MinedTransactionDigest,
        kind: ForwardedReadKind,
        deadline: Instant,
    ) -> Result<Arc<[u8]>, ForwardedTransactionError> {
        let fetch = match self.inner.transactions.lookup(mined, || {
            let limit = self.clone();
            let node = node.clone();
            async move { limit.fetch_verified(&node, mined, kind, deadline).await }
        }) {
            transaction_bytes::Lookup::Cached(bytes) => return Ok(bytes),
            transaction_bytes::Lookup::Fetch(fetch) => fetch,
        };
        fetch.await
    }

    async fn fetch_verified(
        &self,
        node: &ZebraJsonRpcSource,
        mined: MinedTransactionDigest,
        kind: ForwardedReadKind,
        deadline: Instant,
    ) -> Result<Arc<[u8]>, ForwardedTransactionError> {
        let location = mined.location;
        let (block, bytes) = self
            .node_call(
                kind,
                deadline,
                node.fetch_mined_transaction_bytes(location.transaction_id),
            )
            .await?
            .map_err(|error| ForwardedTransactionError::Node(Arc::new(error)))?
            .ok_or(ForwardedTransactionError::OutsideServedView)?;
        if block != zinder_core::BlockId::new(location.block_height, location.block_hash) {
            return Err(ForwardedTransactionError::OutsideServedView);
        }
        if zinder_core::SerializedBytesDigest::from_serialized_bytes(&bytes)
            != mined.serialized_bytes_digest
        {
            return Err(ForwardedTransactionError::DigestMismatch);
        }
        Ok(Arc::from(bytes))
    }
}

#[cfg(test)]
impl ForwardedReadLimit {
    /// Returns the node-call slots `kind` holds and the requests of `kind`
    /// waiting for one.
    pub(crate) fn node_call_counts(&self, kind: ForwardedReadKind) -> (u32, usize) {
        self.inner.node_calls.counts(kind.class())
    }
}

/// A taken response slot of a [`ForwardedReadLimit`], returned when dropped.
#[derive(Debug)]
pub struct ForwardedResponsePermit {
    _slot: PoolSlot,
}

/// Splits `config`'s node-call budget into each kind's reserved floor and
/// cap, and the shared remainder.
///
/// At the default 32 slots, transaction lookups reserve 12 and hold at most
/// 24, UTXO lookups reserve 4 and hold at most 8, history pages reserve 4 and
/// hold at most 16, and 12 slots are shared. Other budgets scale the same
/// parts of 32, and every kind keeps at least one reserved slot once the
/// budget has three.
fn node_call_shares(config: ForwardedReadConfig) -> ([ClassShare; 3], u32) {
    let budget = config.max_concurrent.get();
    let scaled = |parts: u32| u32::try_from(u64::from(budget) * u64::from(parts) / 32).unwrap_or(0);
    let reserved = ForwardedReadKind::ALL.map(|kind| {
        if budget < 3 {
            0
        } else {
            scaled(kind.proportions().0).max(1)
        }
    });
    let elastic = budget.saturating_sub(reserved.iter().sum());
    let shares = ForwardedReadKind::ALL.map(|kind| {
        let floor = reserved[kind.class()];
        let cap = scaled(kind.proportions().1).clamp(floor.max(1), floor + elastic);
        ClassShare {
            label: kind.label(),
            reserved: floor,
            cap,
            max_waiters: usize::try_from(cap.saturating_mul(config.waiters_per_slot.get()))
                .unwrap_or(usize::MAX),
        }
    });
    (shares, elastic)
}

/// Registers `# HELP` and `# TYPE` text for the forwarded-read metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_gauge!(
        ACTIVE_GAUGE,
        metrics::Unit::Count,
        "Forwarded reads holding a slot, by kind: node calls (`transaction`, `utxo`, \
         `history`) and response slots (`utxo_response`, `history_stream`)."
    );
    metrics::describe_gauge!(
        QUEUED_GAUGE,
        metrics::Unit::Count,
        "Forwarded reads waiting for a slot, by kind."
    );
    metrics::describe_histogram!(
        WAIT_HISTOGRAM,
        metrics::Unit::Seconds,
        "Time a forwarded read waited for a slot, by kind and outcome (`admitted`, \
         `refused`, or `cancelled`)."
    );
    metrics::describe_counter!(
        REFUSALS_COUNTER,
        metrics::Unit::Count,
        "Forwarded reads refused, by kind and reason: `waiters_full` when every waiting \
         place was taken, `wait_timeout` when no slot freed within the wait, and \
         `node_rate_limited` when the node refused the call with HTTP 429."
    );
    metrics::describe_counter!(
        CACHE_LOOKUPS_COUNTER,
        metrics::Unit::Count,
        "Forwarded transaction byte lookups, by outcome: `hit` from the verified cache, \
         `coalesced` onto a node call already running, or `miss` starting a node call."
    );
}
