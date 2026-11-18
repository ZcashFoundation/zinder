//! Mempool source boundary values.
//!
//! The source layer normalizes upstream node mempool observations into typed
//! [`MempoolSourceEvent`] envelopes. Adapters hydrate `ADDED` observations
//! into raw transaction bytes before yielding them; the spec forbids the
//! lightwalletd compatibility shim from compensating for missing hydration.
//!
//! `MempoolSourceEntry` is the partial record the source layer can produce.
//! Ingest finalizes it into the public [`zinder_core::MempoolEntry`] when it
//! stamps the chain epoch visible at observation time and computes the
//! compact-transaction bytes from the raw payload.

use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use tokio_stream::Stream;
use zinder_core::{
    AuthDigest, BlockHash, BlockHeight, BlockId, MempoolEvictionReason, RawTransactionBytes,
    TransactionId, UnixTimestampMillis,
};

use crate::SourceError;

/// Default maximum number of transactions admitted into one source generation.
///
/// Zebra's default 80,000,000 transaction-cost ceiling and 10,000 minimum
/// per-transaction cost bound the default upstream mempool to 8,000 entries.
pub const DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT: NonZeroU32 = NonZeroU32::MIN.saturating_add(7_999);

/// Default maximum total raw transaction bytes admitted into one source generation.
///
/// This matches Zebra's default 80,000,000 transaction-cost ceiling. Zebra's
/// cost is at least the serialized transaction size, so the default accepts
/// every mempool that fits the upstream's default policy.
pub const DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES: NonZeroU64 =
    NonZeroU64::MIN.saturating_add(79_999_999);

/// Resource limits applied to a mempool source generation.
///
/// Both limits describe the complete currently admitted set, not one hydration
/// batch. A source withdraws its generation when either bound is exceeded; it
/// never publishes a truncated snapshot as complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolSourceAdmissionLimits {
    /// Maximum number of distinct transactions in the admitted set.
    pub max_transaction_count: NonZeroU32,
    /// Maximum cumulative serialized bytes across the admitted set.
    pub max_total_raw_transaction_bytes: NonZeroU64,
}

impl Default for MempoolSourceAdmissionLimits {
    fn default() -> Self {
        Self {
            max_transaction_count: DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT,
            max_total_raw_transaction_bytes: DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES,
        }
    }
}

/// Admission accounting shared by polling snapshots and streaming deltas.
#[derive(Debug)]
pub(crate) struct MempoolSourceAdmission {
    limits: MempoolSourceAdmissionLimits,
    raw_transaction_bytes_by_transaction_id: HashMap<TransactionId, u64>,
    total_raw_transaction_bytes: u64,
}

impl MempoolSourceAdmission {
    pub(crate) fn new(limits: MempoolSourceAdmissionLimits) -> Self {
        Self {
            limits,
            raw_transaction_bytes_by_transaction_id: HashMap::new(),
            total_raw_transaction_bytes: 0,
        }
    }

    pub(crate) fn validate_snapshot_transaction_count(
        &self,
        transaction_count: usize,
    ) -> Result<(), SourceError> {
        let transaction_count = u64::try_from(transaction_count).unwrap_or(u64::MAX);
        let max_transaction_count = u64::from(self.limits.max_transaction_count.get());
        if transaction_count > max_transaction_count {
            return Err(SourceError::MempoolTransactionCountLimitExceeded {
                transaction_count,
                max_transaction_count,
            });
        }
        Ok(())
    }

    pub(crate) fn admit_added_entry(
        &mut self,
        entry: &MempoolSourceEntry,
    ) -> Result<(), SourceError> {
        let transaction_id = entry.transaction_id;
        let raw_transaction_bytes =
            u64::try_from(entry.raw_transaction_bytes.as_slice().len()).unwrap_or(u64::MAX);
        let previous_raw_transaction_bytes = self
            .raw_transaction_bytes_by_transaction_id
            .get(&transaction_id)
            .copied();
        if previous_raw_transaction_bytes.is_some() {
            return Ok(());
        }
        let transaction_count = self
            .raw_transaction_bytes_by_transaction_id
            .len()
            .saturating_add(1);
        self.validate_snapshot_transaction_count(transaction_count)?;

        let total_raw_transaction_bytes = self
            .total_raw_transaction_bytes
            .saturating_add(raw_transaction_bytes);
        let max_total_raw_transaction_bytes = self.limits.max_total_raw_transaction_bytes.get();
        if total_raw_transaction_bytes > max_total_raw_transaction_bytes {
            return Err(SourceError::MempoolRawTransactionBytesLimitExceeded {
                total_raw_transaction_bytes,
                max_total_raw_transaction_bytes,
            });
        }

        self.raw_transaction_bytes_by_transaction_id
            .insert(transaction_id, raw_transaction_bytes);
        self.total_raw_transaction_bytes = total_raw_transaction_bytes;
        Ok(())
    }

    pub(crate) fn remove_transaction(&mut self, transaction_id: TransactionId) {
        if let Some(raw_transaction_bytes) = self
            .raw_transaction_bytes_by_transaction_id
            .remove(&transaction_id)
        {
            self.total_raw_transaction_bytes = self
                .total_raw_transaction_bytes
                .saturating_sub(raw_transaction_bytes);
        }
    }

    pub(crate) fn transaction_ids(&self) -> impl Iterator<Item = &TransactionId> {
        self.raw_transaction_bytes_by_transaction_id.keys()
    }

    pub(crate) fn contains_transaction(&self, transaction_id: TransactionId) -> bool {
        self.raw_transaction_bytes_by_transaction_id
            .contains_key(&transaction_id)
    }
}

/// Source-observed mempool transition.
///
/// Lifecycle variants correspond directly to Zebra's
/// `MempoolChange::ChangeType`; control-plane variants delimit a coherent
/// stream generation. A polling backend that observes a txid disappear
/// without a chain commit emits [`MempoolSourceEvent::Invalidated`] with
/// [`MempoolEvictionReason::Unknown`]; the streaming backend always emits
/// `Unknown` because Zebra's `MempoolChange` does not carry a reason on the
/// wire.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolSourceEvent {
    /// Control-plane marker emitted once the source has completed its
    /// initial snapshot for this stream generation.
    ///
    /// This is not a mempool lifecycle transition and must never be
    /// persisted in the canonical mempool-event log. Consumers use it as the
    /// only proof that an empty or partially populated in-memory index may be
    /// exposed to callers. Streaming sources open the change stream before
    /// taking the snapshot and emit this marker only after replaying the
    /// snapshot and the buffered change-stream prefix. Polling sources emit
    /// it after their first fully successful poll.
    InitialSnapshotComplete {
        /// Exact upstream best-chain tip that remained stable while the
        /// source constructed this complete snapshot generation.
        source_tip: BlockId,
    },
    /// Control-plane marker that ends the current stream generation because
    /// the upstream best-chain tip moved normally.
    ///
    /// This is not a mempool lifecycle transition and must never be persisted
    /// in the canonical mempool-event log. Consumers discard provisional
    /// state or retire the certified generation, then reconnect to build a
    /// snapshot against the newly observed tip. Source monitor, transport,
    /// and hydration failures remain [`SourceError`] stream items rather than
    /// this marker.
    SourceTipChanged {
        /// Tip against which the ending generation was constructed.
        generation_source_tip: BlockId,
        /// New tip observed by the source.
        observed_source_tip: BlockId,
    },
    /// New transaction admitted to the upstream mempool, with hydrated raw
    /// bytes.
    Added(MempoolSourceEntry),
    /// Mempool transaction was removed without being mined.
    Invalidated {
        /// Identifier of the invalidated transaction.
        transaction_id: TransactionId,
        /// Source-classified eviction reason.
        reason: MempoolEvictionReason,
    },
    /// Mempool transaction was mined into the best chain at the upstream
    /// node.
    Mined {
        /// Identifier of the mined transaction.
        transaction_id: TransactionId,
        /// Height at which the source observed the mining.
        mined_height: BlockHeight,
        /// Hash of the block that mined the transaction, as observed by the
        /// source. Authoritative observation: avoids the canonical-chain
        /// catch-up race when consumers want to track lifecycle without a
        /// follow-up tip read.
        block_hash: BlockHash,
    },
}

/// Partial mempool entry produced by the source layer.
///
/// The source captures everything it can produce locally: the txid, the
/// authorization digest (when the source provides one), the hydrated raw
/// transaction bytes, and the wall-clock observation timestamp. Ingest
/// completes the record into a [`zinder_core::MempoolEntry`] by stamping
/// the visible chain epoch, parsing transparent overlays from the raw
/// bytes, and building compact-transaction bytes for the lightwalletd
/// compatibility adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MempoolSourceEntry {
    /// Transaction identifier reported by the source.
    pub transaction_id: TransactionId,
    /// ZIP-244 authorization digest. Set for v5+ transactions; `None` for
    /// v1-v4.
    pub auth_digest: Option<AuthDigest>,
    /// Raw serialized transaction bytes hydrated from the source.
    pub raw_transaction_bytes: RawTransactionBytes,
    /// Wall-clock time when the adapter observed the source change.
    pub observed_at_unix_millis: UnixTimestampMillis,
}

/// Backend powering a [`MempoolSource`] adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolSourceBackend {
    /// Source consumes the upstream node's gRPC mempool change stream.
    ///
    /// Emits typed change events with low latency. Hydration of `Added`
    /// observations still requires a JSON-RPC follow-up because Zebra's
    /// `MempoolChange` carries only the txid and authorization digest.
    Streaming,
    /// Source diffs the upstream node's mempool state against its previous
    /// snapshot.
    ///
    /// Yields the same change variants as the streaming backend, except
    /// that eviction reasons collapse into
    /// [`MempoolEvictionReason::Unknown`] when the source cannot prove a
    /// more specific cause.
    Polling,
}

/// Capabilities the wired [`MempoolSource`] supports at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolSourceCapabilities {
    /// Mempool source backend in use.
    pub backend: MempoolSourceBackend,
    /// Whether the backend can report eviction reasons more precise than
    /// [`MempoolEvictionReason::Unknown`].
    pub reports_eviction_reasons: bool,
    /// Whether the backend reports `Mined` events without correlating with
    /// chain commits separately.
    ///
    /// Streaming backends that mirror Zebra's `MempoolChange::MINED`
    /// directly report `true`; polling backends that derive `Mined` from
    /// chain-commit correlation report `false`.
    pub reports_mined_directly: bool,
}

impl MempoolSourceCapabilities {
    /// Returns the capability set for a Zebra streaming backend.
    #[must_use]
    pub const fn streaming() -> Self {
        Self {
            backend: MempoolSourceBackend::Streaming,
            reports_eviction_reasons: false,
            reports_mined_directly: true,
        }
    }

    /// Returns the capability set for a JSON-RPC polling backend.
    #[must_use]
    pub const fn polling() -> Self {
        Self {
            backend: MempoolSourceBackend::Polling,
            reports_eviction_reasons: false,
            reports_mined_directly: false,
        }
    }
}

/// Stream of source-observed mempool events.
pub type MempoolSourceEventStream =
    Pin<Box<dyn Stream<Item = Result<MempoolSourceEvent, SourceError>> + Send + 'static>>;

/// Why hydrating a mempool source observation failed.
///
/// Each variant maps to a canonical `reason` label of the
/// `zinder_mempool_hydration_failures_total` counter so dashboards stay in
/// sync with emitter sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolHydrationFailureReason {
    /// The upstream node did not return raw bytes for the txid.
    NotFound,
    /// The hydration RPC failed for a non-not-found reason.
    RpcError,
    /// No visible chain epoch was available to stamp the entry with.
    NoVisibleChainEpoch,
    /// Parsing the raw transaction bytes into a typed transaction failed.
    TransactionParseFailed,
    /// Source-reported transaction identity disagreed with the hydrated bytes.
    TransactionIdMismatch,
    /// Source-reported authorization digest disagreed with the hydrated bytes.
    AuthDigestMismatch,
    /// Building the compact-transaction projection from the parsed
    /// transaction failed.
    CompactTransactionBuildFailed,
    /// A transparent output index did not fit in `u32`.
    TransparentOutputIndexOverflow,
    /// The live mempool owner received a [`MempoolSourceEvent`] variant it does
    /// not yet know how to handle.
    UnknownSourceEventVariant,
}

impl MempoolHydrationFailureReason {
    /// Returns the canonical metric `reason` label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::RpcError => "rpc_error",
            Self::NoVisibleChainEpoch => "no_visible_chain_epoch",
            Self::TransactionParseFailed => "transaction_parse_failed",
            Self::TransactionIdMismatch => "transaction_id_mismatch",
            Self::AuthDigestMismatch => "auth_digest_mismatch",
            Self::CompactTransactionBuildFailed => "compact_transaction_build_failed",
            Self::TransparentOutputIndexOverflow => "transparent_output_index_overflow",
            Self::UnknownSourceEventVariant => "unknown_source_event_variant",
        }
    }
}

/// Configured upstream mempool source for ingestion.
///
/// Implementations encapsulate streaming or polling, raw-transaction
/// hydration, initial-snapshot completion, and reconnect handling. The trait
/// yields one event stream per call to [`MempoolSource::events`]; callers
/// expecting durable reconnect must wrap the trait themselves.
#[async_trait]
pub trait MempoolSource: Send + Sync + 'static {
    /// Returns the capabilities of this mempool source backend.
    fn capabilities(&self) -> MempoolSourceCapabilities;

    /// Opens a typed mempool source event stream.
    ///
    /// Returns a [`MempoolSourceEventStream`] that yields hydrated
    /// [`MempoolSourceEvent`] values until the underlying source closes or
    /// fails permanently. A generation that reaches certification emits
    /// exactly one [`MempoolSourceEvent::InitialSnapshotComplete`] marker
    /// after its initial snapshot is complete. Normal source-tip movement
    /// emits one [`MempoolSourceEvent::SourceTipChanged`] marker and ends the
    /// generation; transient monitor, transport, and hydration failures are
    /// signalled by a [`SourceError`] item instead. Callers should reconnect
    /// and keep the previous snapshot unavailable until the next generation's
    /// completion marker because the underlying broadcast channel may have
    /// lagged events.
    async fn events(&self) -> Result<MempoolSourceEventStream, SourceError>;
}

/// A node endpoint that can build the live mempool source ingest follows.
pub trait MempoolSourceFactory {
    /// Builds this endpoint's mempool source, admitting at most
    /// `admission_limits` per source generation.
    fn mempool_source(
        &self,
        admission_limits: MempoolSourceAdmissionLimits,
    ) -> Arc<dyn MempoolSource>;
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use zinder_core::{RawTransactionBytes, TransactionId, UnixTimestampMillis};

    use super::{
        MempoolSourceAdmission, MempoolSourceAdmissionLimits, MempoolSourceBackend,
        MempoolSourceCapabilities, MempoolSourceEntry,
    };
    use crate::{SourceError, SourceFailureClass};

    fn admission_limits(
        max_transaction_count: u32,
        max_total_raw_transaction_bytes: u64,
    ) -> Result<MempoolSourceAdmissionLimits, &'static str> {
        Ok(MempoolSourceAdmissionLimits {
            max_transaction_count: NonZeroU32::new(max_transaction_count)
                .ok_or("transaction limit must be nonzero")?,
            max_total_raw_transaction_bytes: NonZeroU64::new(max_total_raw_transaction_bytes)
                .ok_or("raw-byte limit must be nonzero")?,
        })
    }

    fn source_entry(transaction_tag: u8, raw_transaction_bytes: usize) -> MempoolSourceEntry {
        MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
            auth_digest: None,
            raw_transaction_bytes: RawTransactionBytes::new(vec![
                transaction_tag;
                raw_transaction_bytes
            ]),
            observed_at_unix_millis: UnixTimestampMillis::new(1),
        }
    }

    #[test]
    fn streaming_capabilities_report_streaming_backend() {
        let capabilities = MempoolSourceCapabilities::streaming();
        assert_eq!(capabilities.backend, MempoolSourceBackend::Streaming);
        assert!(capabilities.reports_mined_directly);
    }

    #[test]
    fn polling_capabilities_report_polling_backend() {
        let capabilities = MempoolSourceCapabilities::polling();
        assert_eq!(capabilities.backend, MempoolSourceBackend::Polling);
        assert!(!capabilities.reports_mined_directly);
    }

    #[test]
    fn admission_accepts_inclusive_limits_and_duplicate_observations() -> Result<(), &'static str> {
        let mut admission = MempoolSourceAdmission::new(admission_limits(2, 5)?);
        let first_entry = source_entry(1, 2);
        let duplicate_with_different_bytes = source_entry(1, 5);
        let second_entry = source_entry(2, 3);

        admission
            .admit_added_entry(&first_entry)
            .map_err(|_| "first entry should be admitted")?;
        admission
            .admit_added_entry(&duplicate_with_different_bytes)
            .map_err(|_| "duplicate entry should not consume capacity")?;
        admission
            .admit_added_entry(&second_entry)
            .map_err(|_| "inclusive limits should be admitted")?;

        assert_eq!(admission.transaction_ids().count(), 2);
        assert_eq!(admission.total_raw_transaction_bytes, 5);
        Ok(())
    }

    #[test]
    fn admission_releases_entry_and_raw_byte_capacity_on_removal() -> Result<(), &'static str> {
        let mut admission = MempoolSourceAdmission::new(admission_limits(1, 3)?);
        let first_entry = source_entry(1, 3);
        let replacement_entry = source_entry(2, 3);
        admission
            .admit_added_entry(&first_entry)
            .map_err(|_| "first entry should be admitted")?;

        admission.remove_transaction(first_entry.transaction_id);
        admission
            .admit_added_entry(&replacement_entry)
            .map_err(|_| "removed entry should release capacity")?;

        assert!(admission.contains_transaction(replacement_entry.transaction_id));
        assert_eq!(admission.total_raw_transaction_bytes, 3);
        Ok(())
    }

    #[test]
    fn admission_reports_transaction_count_and_raw_byte_overflow() -> Result<(), &'static str> {
        let mut count_admission = MempoolSourceAdmission::new(admission_limits(1, 10)?);
        count_admission
            .admit_added_entry(&source_entry(1, 1))
            .map_err(|_| "first count fixture should be admitted")?;
        assert!(matches!(
            count_admission.admit_added_entry(&source_entry(2, 1)),
            Err(SourceError::MempoolTransactionCountLimitExceeded {
                transaction_count: 2,
                max_transaction_count: 1,
            })
        ));

        let mut byte_admission = MempoolSourceAdmission::new(admission_limits(2, 3)?);
        byte_admission
            .admit_added_entry(&source_entry(1, 2))
            .map_err(|_| "first byte fixture should be admitted")?;
        assert!(matches!(
            byte_admission.admit_added_entry(&source_entry(2, 2)),
            Err(SourceError::MempoolRawTransactionBytesLimitExceeded {
                total_raw_transaction_bytes: 4,
                max_total_raw_transaction_bytes: 3,
            })
        ));
        let admission_error = SourceError::MempoolTransactionCountLimitExceeded {
            transaction_count: 2,
            max_transaction_count: 1,
        };
        assert_eq!(
            admission_error.upstream_classification(),
            SourceFailureClass::Configuration
        );
        Ok(())
    }
}
