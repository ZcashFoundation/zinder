//! Wallet-serving secondary-pair lifecycle shared by native and compatibility runtimes.
//!
//! Each reader runtime owns no canonical or wallet primary handle. It catches
//! up only an inactive generation, authenticates that generation against the
//! canonical writer control plane, and atomically publishes it as one
//! immutable canonical/wallet pair. A replaced pair that a request observed,
//! because a response could expose its chain epoch, keeps answering reads
//! pinned to that epoch for one retention window, however fast later blocks
//! arrive: while every spare generation still holds such a pair, the
//! publisher publishes nothing and then publishes the writer's latest fence,
//! so blocks in a burst share one publication. A replaced pair that no
//! request observed is released at once, so a burst of blocks that no request
//! observes does not wait on the retention window. Requests still holding
//! such a pair can delay reuse of its generation until they drop it. Each
//! generation stays frozen until every in-flight request has dropped its
//! captured pair `Arc`;
//! the publisher then catches that same open generation up for a later
//! publication, so a slot is opened cold only on its first use or after a
//! failed catch-up. A request that holds an old pair for a long time pins
//! only that generation: the publisher keeps refreshing into the other
//! bounded generation slots.
//!
//! Every generation reads through one block cache per store, which the
//! publisher owns, so the configured cache sizes bound the process rather
//! than each generation, and a generation opened or caught up later finds the
//! blocks its siblings already loaded.
//!
//! A secondary does not keep the primary's table files alive, so a frozen
//! generation can name a file the writer's compaction has since deleted. A
//! read that finds such a file gone marks its pair, which then leaves the
//! slot. The read that marks the pair wakes the publisher, which publishes a
//! fresh generation even when the writer has not moved. The read then runs
//! once more on the fresh pair when that pair answers the read's epoch.

use std::{
    borrow::Cow,
    num::{NonZeroU8, NonZeroU32},
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::{
    AdmittedIngestControl, OrderedBroadcaster, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletServingAdmissionError, WalletServingReadPair,
    ingest_control::wallet_ingest_control_request,
    node_broadcast::{STALE_VERIFICATION_RETRY, VerificationBasis},
    wallet_serving_pair::CanonicalAdmission,
};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use zinder_core::{ChainEpochId, Network, UnixTimestampMillis};
use zinder_proto::{
    v1::{
        ingest::{
            CanonicalWriterFence, CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
            canonical_control_client::CanonicalControlClient,
        },
        ops::ReorgWindowExceededDetail,
    },
    wire::canonical_writer_fence_message,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, CanonicalFenceConvergence,
    CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy, CanonicalFenceDivergence,
    CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation, ComposedReadiness,
    NodeUnavailableDetail, Readiness, ReadinessCause, ReadinessInput, ReadinessState,
    UpstreamHealth, UpstreamNotReadyDetail, WriterAttestation, classify_canonical_writer_fence,
    connect_zinder_grpc, converge_on_canonical_writer_fence, refresh_within_stale_deadline,
    writer_attests_network_upgrade_schedule,
};
use zinder_source::{
    BroadcastNodeProbe, NodeCapability, NodeSource, SourceError, SourceFailureClass,
    UpstreamHealthSnapshot,
};
use zinder_store::{
    CanonicalActivationSchedule, CanonicalReorgPolicy, CanonicalStoreError,
    CanonicalStoreRequirements, CanonicalStoredSet, ResourceGaugeThrottle,
    RocksDbCanonicalSecondary, RocksDbInstanceMemory, RocksDbOpenResources, RocksDbResourceBudget,
    SharedSecondaryResources, StoreRole, record_instance_memory_gauges,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{RocksDbWalletError, RocksDbWalletSecondary};

/// How long a replaced pair that a request observed keeps answering reads
/// pinned to its epoch.
///
/// The window is a guarantee as well as a bound. Publication never releases
/// an observed replaced pair before its window ends, so a pin on its epoch is
/// answered for exactly this long after the pair was replaced, however many
/// blocks arrive meanwhile. A pair no request observed has no pin to answer,
/// so publication releases it at once.
const RETAINED_PAIR_WINDOW: Duration = Duration::from_mins(1);
/// Longest a request may keep reading a pair after the publisher released it.
///
/// Past this bound the publisher revokes reads on the pair, so a slow or
/// abandoned read delays publication by at most this long instead of holding
/// a generation slot until it ends.
pub(crate) const RELEASED_PAIR_READ_LIMIT: Duration = Duration::from_secs(30);
/// Secondary generation slots.
///
/// The current pair takes one. Each other slot holds the candidate, a
/// replaced pair inside its retention window, or a released pair that a
/// request still holds, so at most three replaced pairs are retained. While
/// no slot is free, the publisher waits. A retained pair frees its slot when
/// its window ends. A released pair that a request holds frees it at most one
/// page read after [`RELEASED_PAIR_READ_LIMIT`]. A publication therefore
/// follows the previous one within about the window plus the read limit.
const SECONDARY_GENERATION_COUNT: usize = 4;
/// Longest a read waits for the publisher to replace a pair whose table file
/// the writer's compaction deleted, before it fails with its storage error.
///
/// A replacement recycled from a drained generation is caught up well within
/// this bound. One that needs a cold open can take longer on a large store;
/// the read then fails, retryably, and later reads use the replacement.
const MISSING_TABLE_FILE_REPLACEMENT_WAIT: Duration = Duration::from_secs(5);
const CONVERGENCE_RETRY_DELAY_CAP: Duration = Duration::from_millis(100);
const STORED_SET_RETRY_DELAY: Duration = Duration::from_secs(30);
const READINESS_REASON_MAX_BYTES: usize = 256;

/// Immutable configuration for the bounded secondary-pair lifecycle.
#[derive(Clone, Debug)]
pub struct WalletServingPairConfig {
    /// Canonical writer-owned primary path followed by the secondary.
    pub canonical_primary_path: PathBuf,
    /// Process-exclusive root for canonical secondary generations.
    pub canonical_secondary_root: PathBuf,
    /// Wallet projector-owned primary path followed by the secondary.
    pub wallet_primary_path: PathBuf,
    /// Process-exclusive root for wallet secondary generations.
    pub wallet_secondary_root: PathBuf,
    /// Network both stores and writer status must attest. Each canonical
    /// generation adopts the network-upgrade schedule the canonical writer
    /// admitted for this network.
    pub network: Network,
    /// Minimum persisted data and historical coverage for each canonical secondary.
    pub canonical_requirements: CanonicalStoreRequirements,
    /// Canonical replacement-depth identity expected from the writer.
    pub canonical_reorg_policy: CanonicalReorgPolicy,
    /// `RocksDB` budget for each canonical secondary generation. Its
    /// `block_cache_bytes` sizes the one block cache that every canonical
    /// generation of the process shares.
    pub canonical_resource_budget: RocksDbResourceBudget,
    /// `RocksDB` budget for each wallet secondary generation. Its
    /// `block_cache_bytes` sizes the one block cache that every wallet
    /// generation of the process shares.
    pub wallet_resource_budget: RocksDbResourceBudget,
    /// Delay between refresh attempts after initial publication.
    pub catchup_interval: Duration,
    /// Maximum wall time allowed for pair convergence.
    pub convergence_timeout: Duration,
    /// Maximum catch-up attempts allowed for pair convergence.
    pub convergence_attempts: NonZeroU8,
    /// Writer lag tolerated before readiness fails.
    pub replica_lag_threshold_chain_epochs: u64,
    /// Longest a published pair keeps admitting traffic after the last writer
    /// attestation. Past it the transient warning gives way to the fail-closed
    /// cause the failure would otherwise have raised.
    pub serving_pair_staleness_ceiling: Duration,
}

/// Read-only handle to the currently published wallet-serving pair.
///
/// Consumers can capture one immutable pair for a request, but only
/// [`WalletServingPairPublisher`] can replace the published pair. This keeps
/// the storage evidence used to derive endpoint capabilities stable for the
/// lifetime of a composed query.
///
/// Each replaced pair that a request may have exposed to a client keeps
/// answering reads pinned to its chain epoch for one minute after it was
/// replaced, however many pairs are published meanwhile. A replaced pair that
/// no request exposed is released at once, because no pin can name it.
#[derive(Clone)]
pub struct WalletServingPairSlot {
    published: Arc<ArcSwap<PublishedPairs>>,
    signals: Arc<SlotSignals>,
    /// Orders the first observation of a pair against its replacement.
    ///
    /// Held only for the once-per-pair observation and for a publication, so
    /// a request that finds its pair already observed never takes it.
    observation: Arc<Mutex<()>>,
}

/// Wake-ups exchanged between the requests reading a slot and its publisher.
struct SlotSignals {
    /// Wakes the publisher when a read first finds a pair missing a table
    /// file.
    missing_table_file: Notify,
    /// Changes on every publication, so a read can wait for a replacement.
    publication: watch::Sender<()>,
}

/// The current pair and the replaced pairs that still answer pinned reads.
struct PublishedPairs {
    current: Arc<WalletServingReadPair>,
    /// Newest first. No two entries share an epoch, and none shares the
    /// current pair's epoch.
    retained: Vec<RetainedPair>,
}

#[derive(Clone)]
struct RetainedPair {
    pair: Arc<WalletServingReadPair>,
    replaced_at: Instant,
}

impl RetainedPair {
    fn is_within_window(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.replaced_at) < RETAINED_PAIR_WINDOW
    }
}

impl PublishedPairs {
    /// Reports whether the current pair answers `chain_epoch_id`.
    fn serves(&self, chain_epoch_id: ChainEpochId) -> bool {
        self.current.canonical_fence().chain_epoch_id() == chain_epoch_id
    }

    /// Returns the pair that answers a pin on `chain_epoch_id` at `now`: the
    /// current pair, else a retained pair inside its window without a missing
    /// table file, else the current pair at another epoch.
    fn pinned_pair(
        &self,
        chain_epoch_id: ChainEpochId,
        now: Instant,
    ) -> Arc<WalletServingReadPair> {
        if self.serves(chain_epoch_id) {
            return Arc::clone(&self.current);
        }
        self.retained
            .iter()
            .find(|retained| {
                retained.is_within_window(now)
                    && !retained.pair.has_missing_table_file()
                    && retained.pair.canonical_fence().chain_epoch_id() == chain_epoch_id
            })
            .map_or_else(
                || Arc::clone(&self.current),
                |retained| Arc::clone(&retained.pair),
            )
    }
}

impl std::fmt::Debug for WalletServingPairSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let published = self.published.load();
        formatter
            .debug_struct("WalletServingPairSlot")
            .field("canonical_fence", &published.current.canonical_fence())
            .field("retained_pairs", &published.retained.len())
            .finish_non_exhaustive()
    }
}

impl WalletServingPairSlot {
    /// Creates a read-only slot from one admitted immutable pair.
    ///
    /// This constructor supports fixed-pair embeddings and contract tests.
    /// Production replacement is owned exclusively by
    /// [`WalletServingPairPublisher`].
    #[must_use]
    pub fn new(initial_pair: Arc<WalletServingReadPair>) -> Self {
        Self {
            published: Arc::new(ArcSwap::from_pointee(PublishedPairs {
                current: initial_pair,
                retained: Vec::new(),
            })),
            signals: Arc::new(SlotSignals {
                missing_table_file: Notify::new(),
                publication: watch::Sender::new(()),
            }),
            observation: Arc::new(Mutex::new(())),
        }
    }

    /// Captures the exact pair to retain for one request, and keeps that pair
    /// readable for pins after it is replaced.
    ///
    /// Use this when the response can expose the pair's chain epoch, or the
    /// request reads the pair again by that epoch. A request that only needs
    /// the current pair's data captures it with [`Self::capture_ephemeral`].
    #[must_use]
    pub fn capture(&self) -> Arc<WalletServingReadPair> {
        let published = self.published.load();
        if published.current.is_observed_for_epoch_pin() {
            return Arc::clone(&published.current);
        }
        drop(published);
        // Under the lock no publication runs, so the pair marked here is the
        // current pair at that moment and its replacement sees the mark.
        let _observation = self.observation.lock();
        let published = self.published.load();
        published.current.mark_observed_for_epoch_pin();
        Arc::clone(&published.current)
    }

    /// Captures the current pair for a read whose response exposes no chain
    /// epoch and which no later read continues by epoch.
    ///
    /// The pair is not kept readable for pins once it is replaced, unless
    /// another request captured it with [`Self::capture`], so such reads never
    /// hold a generation through the retention window. Taking it never waits
    /// for a publication.
    #[must_use]
    pub fn capture_ephemeral(&self) -> Arc<WalletServingReadPair> {
        Arc::clone(&self.published.load().current)
    }

    /// Subscribes to the slot's publications.
    ///
    /// The receiver reports a change for every pair published after this
    /// call, so a caller that subscribes before it reads the current pair
    /// cannot miss the publication that replaces it. Holding the receiver
    /// holds no pair.
    #[must_use]
    pub fn subscribe_publications(&self) -> watch::Receiver<()> {
        self.signals.publication.subscribe()
    }

    /// Reports whether `candidate` still names the currently published pair.
    #[must_use]
    pub(crate) fn is_current_weak(&self, candidate: &Weak<WalletServingReadPair>) -> bool {
        Weak::ptr_eq(candidate, &Arc::downgrade(&self.published.load().current))
    }

    /// Captures the pair that answers a read pinned to `chain_epoch_id`.
    ///
    /// A retained pair answers a pin that names its epoch until its retention
    /// window ends, even before the publisher releases it, unless a read
    /// found one of its table files missing. Every other pin captures the
    /// current pair, so the caller's epoch check still reports a pin that no
    /// pair serves.
    ///
    /// An absent pin captures as [`Self::capture`] does, because the response
    /// exposes the epoch of the pair it answers. A pin that the current pair
    /// answers observes that pair too, because the caller holds its epoch
    /// from some response, perhaps one of another replica, and may pin it
    /// again after the pair is replaced.
    #[must_use]
    pub(crate) fn capture_at(
        &self,
        chain_epoch_id: Option<ChainEpochId>,
    ) -> Arc<WalletServingReadPair> {
        let Some(chain_epoch_id) = chain_epoch_id else {
            return self.capture();
        };
        let published = self.published.load();
        if !published.serves(chain_epoch_id) || published.current.is_observed_for_epoch_pin() {
            return published.pinned_pair(chain_epoch_id, Instant::now());
        }
        drop(published);
        // A publication may have replaced the pair before the lock was taken,
        // so the pin is selected again from the pairs published under it.
        let _observation = self.observation.lock();
        let published = self.published.load();
        let pair = published.pinned_pair(chain_epoch_id, Instant::now());
        if Arc::ptr_eq(&pair, &published.current) && published.serves(chain_epoch_id) {
            pair.mark_observed_for_epoch_pin();
        }
        pair
    }

    /// Makes `pair` current as the publisher would, for tests outside this
    /// module that replace the pair under a running read.
    #[cfg(test)]
    pub(crate) fn publish_for_test(&self, pair: Arc<WalletServingReadPair>) {
        let _released = self.publish(pair, Instant::now());
    }

    fn retained_pair_count(&self) -> usize {
        self.published.load().retained.len()
    }

    /// Makes `pair` current and retains the pair it replaces if a request
    /// observed it.
    ///
    /// Returns the pairs that leave the slot: the replaced pair when no
    /// request observed it, retained pairs at `pair`'s epoch, which `pair` now
    /// answers, and pairs missing a table file. A schedule revision or a
    /// replacement for a pair missing a table file republishes the current
    /// epoch, so the pair it replaces leaves at once instead of keeping a
    /// generation that no pin can reach. The republished pair inherits the
    /// observation of the pairs it replaces, because a client that received
    /// the epoch may still pin it. Every other observed pair stays until its
    /// window ends. Only the publisher writes the slot, so the load and store
    /// below cannot race another publication.
    fn publish(
        &self,
        pair: Arc<WalletServingReadPair>,
        replaced_at: Instant,
    ) -> Vec<Arc<WalletServingReadPair>> {
        let observation = self.observation.lock();
        let previous = self.published.load_full();
        let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
        let (released, retained): (Vec<_>, Vec<_>) = std::iter::once(RetainedPair {
            pair: Arc::clone(&previous.current),
            replaced_at,
        })
        .chain(previous.retained.iter().cloned())
        .partition(|retained| {
            retained.pair.canonical_fence().chain_epoch_id() == chain_epoch_id
                || retained.pair.has_missing_table_file()
                || !retained.pair.is_observed_for_epoch_pin()
        });
        if released.iter().any(|released| {
            released.pair.canonical_fence().chain_epoch_id() == chain_epoch_id
                && released.pair.is_observed_for_epoch_pin()
        }) {
            pair.mark_observed_for_epoch_pin();
        }
        self.published.store(Arc::new(PublishedPairs {
            current: pair,
            retained,
        }));
        drop(observation);
        self.signals.publication.send_replace(());
        released.into_iter().map(|released| released.pair).collect()
    }

    /// Releases the retained pairs replaced at least [`RETAINED_PAIR_WINDOW`]
    /// before `now`, and those missing a table file, returning them.
    fn release_expired(&self, now: Instant) -> Vec<Arc<WalletServingReadPair>> {
        let previous = self.published.load_full();
        let (retained, expired): (Vec<_>, Vec<_>) =
            previous.retained.iter().cloned().partition(|retained| {
                retained.is_within_window(now) && !retained.pair.has_missing_table_file()
            });
        if expired.is_empty() {
            return Vec::new();
        }
        self.published.store(Arc::new(PublishedPairs {
            current: Arc::clone(&previous.current),
            retained,
        }));
        expired.into_iter().map(|expired| expired.pair).collect()
    }

    /// Marks `pair` after a read through it failed with `error` because one
    /// of its table files was deleted.
    ///
    /// The first report for a pair counts and logs it and wakes the publisher
    /// to replace it. Later reports change nothing: when no replacement can be
    /// published yet, such as while every inactive generation is taken, the
    /// publisher's periodic refresh retries, so sustained reads on the marked
    /// pair cannot drive back-to-back refreshes. The marked pair answers no
    /// new capture except as the current pair, which it stays until the
    /// publisher publishes its replacement.
    pub(crate) fn report_missing_table_file(
        &self,
        pair: &WalletServingReadPair,
        error: &crate::QueryError,
    ) {
        if !pair.mark_table_file_missing() {
            return;
        }
        metrics::counter!("zinder_wallet_serving_missing_table_file_pairs_total").increment(1);
        tracing::warn!(
            target: "zinder::wallet_serving",
            event = "wallet_serving_pair_table_file_missing",
            chain_epoch = pair.canonical_fence().chain_epoch_id().value(),
            error = %error_with_sources(error),
            "a read found a table file of the wallet-serving pair deleted by the writer's compaction; replacing the pair"
        );
        self.signals.missing_table_file.notify_one();
    }

    /// Reports `pair` as missing a table file and waits, for at most
    /// [`MISSING_TABLE_FILE_REPLACEMENT_WAIT`], for `capture` to return a pair
    /// without that mark.
    ///
    /// `capture` selects the replacement the way the read selected `pair`,
    /// with [`Self::capture_at`] for a read that observes its pair and
    /// [`Self::capture_ephemeral`] for one that does not, so recovery never
    /// changes whether the replacement is retained. Returns `None` when no
    /// unmarked pair is published in time. A pin whose epoch only the marked
    /// pair served gets the current pair, at another epoch, so the caller's
    /// epoch check refuses it.
    pub(crate) async fn replace_missing_table_file_pair(
        &self,
        pair: Arc<WalletServingReadPair>,
        error: &crate::QueryError,
        capture: impl Fn() -> Arc<WalletServingReadPair>,
    ) -> Option<Arc<WalletServingReadPair>> {
        let mut publications = self.signals.publication.subscribe();
        self.report_missing_table_file(&pair, error);
        drop(pair);
        let deadline = Instant::now() + MISSING_TABLE_FILE_REPLACEMENT_WAIT;
        loop {
            let replacement = capture();
            if !replacement.has_missing_table_file() {
                return Some(replacement);
            }
            drop(replacement);
            if !matches!(
                tokio::time::timeout_at(deadline, publications.changed()).await,
                Ok(Ok(()))
            ) {
                return None;
            }
        }
    }
}

/// Readiness owner for one wallet-serving runtime.
///
/// The serving pair alone decides whether the runtime admits traffic, because
/// every stored read answers from it. Node-source and ingest-control health
/// are dependencies published as separate inputs to one
/// [`ComposedReadiness`]: a failing dependency fails only the requests that
/// call it and appears in the report's degraded dependencies, except that a
/// canonical writer's halt reported through ingest control still refuses all
/// traffic. The runtime [`Readiness`] remains the single operations and gRPC
/// projection, and no input writer can erase another input's observation.
///
/// A halt the ingest-control probe observes stays published until the pair
/// publisher adopts it. The probe's later observations cannot clear it, so a
/// publisher that missed the park cannot reopen traffic on its pair before it
/// has validated that pair against a writer that follows again.
#[derive(Clone, Debug)]
pub struct WalletServingReadiness {
    composed: ComposedReadiness,
    ingest_control: Arc<Mutex<IngestControlInput>>,
}

/// The ingest-control probe's latest observation, and a writer halt it
/// observed that the pair publisher has not adopted yet.
#[derive(Debug)]
struct IngestControlInput {
    observed: ReadinessCause,
    unadopted_halt: Option<ReorgWindowExceededDetail>,
    awaiting_writer_attestation: bool,
}

impl IngestControlInput {
    fn published_cause(&self) -> ReadinessCause {
        self.unadopted_halt
            .map_or_else(|| self.observed.clone(), ReadinessCause::from)
    }
}

impl WalletServingReadiness {
    /// Starts the composition native query and lightwalletd compatibility
    /// wallet serving share: the serving pair unadmitted, and the node source
    /// and ingest control as dependencies not yet observed.
    ///
    /// Both runtimes call the node to broadcast, and compatibility also to
    /// read the standard fee. Both call ingest control for mempool reads and
    /// unmined transaction lookups, so each runs both dependency probes.
    #[must_use]
    pub fn with_node_and_ingest_control(runtime: Readiness) -> Self {
        Self::with_dependencies(
            runtime,
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        )
    }

    /// Starts a composition whose readiness is the serving pair alone.
    #[must_use]
    pub fn pair_only(runtime: Readiness) -> Self {
        Self::with_dependencies(runtime, &[])
    }

    fn with_dependencies(runtime: Readiness, dependency_inputs: &[ReadinessInput]) -> Self {
        Self {
            composed: ComposedReadiness::with_dependency_inputs(runtime, dependency_inputs),
            ingest_control: Arc::new(Mutex::new(IngestControlInput {
                observed: ReadinessCause::Starting,
                unadopted_halt: None,
                awaiting_writer_attestation: false,
            })),
        }
    }

    /// Returns the shared admission for the storage publisher and its stale deadline.
    #[must_use]
    pub const fn canonical_admission(&self) -> &ComposedReadiness {
        &self.composed
    }

    /// Hands a latched ingest halt to the canonical publisher before its writer-status read.
    pub fn begin_canonical_refresh(&self, writer: &mut WriterAttestation, height: u32) {
        let mut input = self.ingest_control.lock();
        if let Some(halt) = input.unadopted_halt {
            input.awaiting_writer_attestation = true;
            drop(input);
            writer.remember_halt(halt, Some(height));
        }
    }

    /// Releases the halt observed before this refresh once the writer attests the served fence.
    /// A new halt observed during the refresh must wait for the next attestation.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the latest observation and its publication must stay ordered"
    )]
    pub fn confirm_canonical_attestation(&self) {
        let mut input = self.ingest_control.lock();
        if input.awaiting_writer_attestation {
            input.unadopted_halt = None;
            input.awaiting_writer_attestation = false;
            self.composed
                .publish_input(ReadinessInput::IngestControl, input.observed.clone());
        }
    }

    /// Returns the projected readiness handle used by operations and gRPC traffic gates.
    #[must_use]
    pub fn runtime_readiness(&self) -> Readiness {
        self.composed.runtime_readiness()
    }

    /// Irreversibly drains readiness before graceful task and server shutdown.
    pub fn publish_shutting_down(&self) {
        self.composed.publish_shutting_down();
    }

    fn publish_pair_state(&self, pair_state: ReadinessState) {
        self.composed.publish_admission(pair_state);
    }

    fn publish_served_epoch(&self, committed_at: UnixTimestampMillis) {
        self.composed
            .runtime_readiness()
            .set_served_epoch_committed_at(committed_at);
    }

    fn publish_node_source_cause(&self, node_source_cause: ReadinessCause) {
        self.composed
            .publish_input(ReadinessInput::NodeSource, node_source_cause);
    }

    /// Publishes the ingest-control probe's latest observation.
    ///
    /// A writer halt in it stays the published cause, whatever the probe
    /// observes later, until the pair publisher adopts it.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the input lock must cover publication so a concurrent adoption cannot be overwritten by a stale cause"
    )]
    fn publish_ingest_control_cause(&self, ingest_control_cause: ReadinessCause) {
        let mut input = self.ingest_control.lock();
        if let Some(halt) = ingest_control_cause.reorg_window_exceeded_detail() {
            input.unadopted_halt = Some(halt);
            {
                input.awaiting_writer_attestation = false;
            }
        }
        input.observed = ingest_control_cause;
        self.composed
            .publish_input(ReadinessInput::IngestControl, input.published_cause());
    }

    /// Hands a writer halt that only the ingest-control probe observed to
    /// the pair admission, which `adopt` returns, and then republishes the
    /// probe's latest observation as the dependency.
    ///
    /// The admission takes the halt before the dependency releases it, so
    /// traffic stays refused in between.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the input lock must cover both publications so the probe cannot latch a newer halt between them"
    )]
    fn adopt_ingest_control_halt(
        &self,
        adopt: impl FnOnce(ReorgWindowExceededDetail) -> ReadinessState,
    ) {
        let mut input = self.ingest_control.lock();
        let Some(halt) = input.unadopted_halt.take() else {
            return;
        };
        self.composed.publish_admission(adopt(halt));
        self.composed
            .publish_input(ReadinessInput::IngestControl, input.published_cause());
    }
}

/// Starts the health probe for the exact admitted ingest-control channel.
///
/// Each observation reaches both `WriterStatus` and a bounded
/// `MempoolSnapshot` through the same authenticated channel used by pair
/// publication and live wallet operations. A failure degrades the
/// ingest-control dependency without closing the traffic gate, except a
/// parked writer's halt, which refuses traffic until the pair publisher has
/// adopted it and read a following writer with a confirmed tip that attests
/// the served pair. Structural capabilities stay immutable either way.
pub fn spawn_wallet_ingest_control_readiness_probe(
    ingest_control: AdmittedIngestControl,
    readiness: WalletServingReadiness,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut observer = IngestControlObserver::default();
        loop {
            let cause = tokio::select! {
                () = cancel.cancelled() => break,
                cause = observer.observe(&ingest_control) => cause,
            };
            readiness.publish_ingest_control_cause(cause);

            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {}
            }
        }
    })
}

/// State the ingest-control probe carries from one observation to the next.
#[derive(Debug, Default)]
struct IngestControlObserver {
    /// Whether the last observation found ingest control healthy, so that
    /// only transitions are logged.
    last_health_was_available: Option<bool>,
    /// Outage of the writer's live mempool, which waits on its node, in
    /// progress across observations.
    writer_mempool_outage: Option<WalletNodeOutage>,
}

impl IngestControlObserver {
    /// Probes ingest control once and returns the cause to publish as the
    /// ingest-control dependency.
    async fn observe(&mut self, ingest_control: &AdmittedIngestControl) -> ReadinessCause {
        let observed = match ingest_control.probe_health().await {
            Ok(()) => {
                if self.last_health_was_available == Some(false) {
                    tracing::info!(
                        target: "zinder::query",
                        event = "ingest_control_health_recovered",
                        "admitted ingest-control health recovered"
                    );
                }
                self.last_health_was_available = Some(true);
                ReadinessCause::Ready
            }
            Err(error) => {
                if self.last_health_was_available != Some(false) {
                    tracing::warn!(
                        target: "zinder::query",
                        event = "ingest_control_health_unavailable",
                        error_class = error.class(),
                        error = %error,
                        "admitted ingest-control health became unavailable"
                    );
                }
                self.last_health_was_available = Some(false);
                error.readiness_cause()
            }
        };
        self.continue_writer_mempool_outage(observed)
    }

    /// Carries the start and failure count of a writer mempool outage across
    /// observations, so the relayed `node_unavailable` detail grows like the
    /// node probe's own.
    fn continue_writer_mempool_outage(&mut self, observed: ReadinessCause) -> ReadinessCause {
        let ReadinessCause::NodeUnavailable(detail) = observed else {
            self.writer_mempool_outage = None;
            return observed;
        };
        let continued = WalletNodeOutage::continue_with(
            self.writer_mempool_outage.as_ref(),
            detail.failure_class,
            &detail.last_reason,
        );
        let cause = ReadinessCause::NodeUnavailable(continued.detail.clone());
        self.writer_mempool_outage = Some(continued);
        cause
    }
}

/// Starts the verification and liveness probe for the node endpoints a
/// wallet query broadcasts through.
///
/// Each endpoint is verified on its own schedule against the pair
/// `serving_pair_slot` serves, as [`OrderedBroadcaster::verify`] describes,
/// so an endpoint that is down or on another chain when the reader starts is
/// verified once it recovers, and an endpoint that answers only when its
/// request times out delays neither another endpoint's verification nor the
/// liveness observation. An endpoint is verified again every
/// `poll_interval`, every two seconds while it holds only a verification of
/// an earlier basis, and at once when a publication changes the pair's
/// schedule revision or the network upgrade at the height after its tip, or
/// brings that height to one at which the endpoint's table disagrees with the
/// writer-admitted schedule. The verified endpoints are observed every
/// `poll_interval`, and as soon as an endpoint's verification outcome
/// changes, in broadcast preference order through `tip_id` and upstream sync
/// health, moving past an unreachable one. When an endpoint after an
/// unreachable one answers, it becomes the preferred endpoint, as a
/// broadcast that failed over would make it. Every failure degrades the
/// node-source dependency; it never closes the traffic gate, and each
/// broadcast fails on its own. Without node endpoints, the dependency
/// reports ready and the task only waits for cancellation.
#[must_use]
pub fn spawn_wallet_node_readiness_probe<Source>(
    nodes: Option<OrderedBroadcaster<Source>>,
    serving_pair_slot: WalletServingPairSlot,
    readiness: WalletServingReadiness,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Source: NodeSource + BroadcastNodeProbe,
{
    let probe =
        nodes.map(|nodes| WalletNodeProbe::new(nodes, serving_pair_slot, poll_interval, false));
    spawn_wallet_node_probe(probe, readiness, cancel)
}

pub(crate) fn spawn_wallet_node_probe<Source>(
    probe: Option<WalletNodeProbe<Source>>,
    readiness: WalletServingReadiness,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Source: NodeSource + BroadcastNodeProbe,
{
    let Some(probe) = probe else {
        readiness.publish_node_source_cause(ReadinessCause::Ready);
        return tokio::spawn(async move {
            cancel.cancelled().await;
        });
    };

    tokio::spawn(async move {
        let verification = futures_util::future::join_all(
            (0..probe.nodes.endpoint_count()).map(|position| probe.keep_verified(position)),
        );
        tokio::select! {
            () = cancel.cancelled() => {}
            _ = futures_util::future::join(verification, probe.observe_liveness(&readiness)) => {}
        }
    })
}

/// The node endpoints one readiness probe verifies and observes.
pub(crate) struct WalletNodeProbe<Source> {
    primary_required: bool,
    nodes: OrderedBroadcaster<Source>,
    serving_pair_slot: WalletServingPairSlot,
    poll_interval: Duration,
    /// The latest verification outcome of each endpoint, in position order,
    /// or `None` before its first verification ends.
    outcomes: watch::Sender<Vec<Option<Result<(), NodeFailure>>>>,
}

impl<Source> WalletNodeProbe<Source>
where
    Source: NodeSource + BroadcastNodeProbe,
{
    pub(crate) fn new(
        nodes: OrderedBroadcaster<Source>,
        serving_pair_slot: WalletServingPairSlot,
        poll_interval: Duration,
        primary_required: bool,
    ) -> Self {
        Self {
            outcomes: watch::Sender::new(vec![None; nodes.endpoint_count()]),
            nodes,
            serving_pair_slot,
            poll_interval,
            primary_required,
        }
    }

    /// Verifies the endpoint at `position` whenever its verification is
    /// due, without waiting on any other endpoint.
    async fn keep_verified(&self, position: usize) {
        let mut publications = self.serving_pair_slot.signals.publication.subscribe();
        loop {
            let basis = VerificationBasis::of(&self.serving_pair_slot.capture_ephemeral());
            let outcome = self
                .nodes
                .verify_endpoint(position, &self.serving_pair_slot)
                .await
                .map_err(|error| NodeFailure::from(&error));
            self.outcomes.send_if_modified(|outcomes| {
                outcomes.get_mut(position).is_some_and(|latest| {
                    let changed = latest.as_ref() != Some(&outcome);
                    *latest = Some(outcome);
                    changed
                })
            });
            let next_round = if self.nodes.awaits_reverification(position) {
                self.poll_interval.min(STALE_VERIFICATION_RETRY)
            } else {
                self.poll_interval
            };
            tokio::select! {
                () = tokio::time::sleep(next_round) => {}
                () = self.verification_ended(position, basis, &mut publications) => {}
            }
        }
    }

    /// Returns once a publication ends what the last verification of the
    /// endpoint at `position`, made against `basis`, established, or never
    /// if the slot stops publishing.
    async fn verification_ended(
        &self,
        position: usize,
        basis: VerificationBasis,
        publications: &mut watch::Receiver<()>,
    ) {
        while publications.changed().await.is_ok() {
            let pair = self.serving_pair_slot.capture_ephemeral();
            if self.nodes.verification_ended_by(position, basis, &pair) {
                return;
            }
        }
        std::future::pending::<()>().await;
    }

    /// Publishes the node dependency every `poll_interval` and whenever an
    /// endpoint's verification outcome changes.
    async fn observe_liveness(&self, readiness: &WalletServingReadiness) {
        let mut outage = None;
        let mut outcomes = self.outcomes.subscribe();
        loop {
            if let Some(observation) = self.observe().await {
                publish_wallet_node_observation(readiness, observation, &mut outage);
            }
            tokio::select! {
                () = tokio::time::sleep(self.poll_interval) => {}
                Ok(()) = outcomes.changed() => {}
            }
        }
    }

    /// Observes the liveness of the first verified endpoint in broadcast
    /// preference order that is reachable. A query that also needs the
    /// primary for cold transaction reads observes that endpoint directly.
    ///
    /// Returns `None` while no endpoint is verified and no verification has
    /// ended yet.
    async fn observe(&self) -> Option<Result<UpstreamHealthSnapshot, NodeFailure>> {
        let mut unreachable = None;
        for (position, source) in self
            .nodes
            .verified_endpoints()
            .filter(|(position, _)| !self.primary_required || *position == 0)
        {
            match probe_wallet_node_readiness(source).await {
                Err(error)
                    if matches!(
                        error.upstream_classification(),
                        SourceFailureClass::NodeUnreachable | SourceFailureClass::NodeRateLimited
                    ) =>
                {
                    unreachable = Some(error);
                }
                observation => {
                    if unreachable.is_some() {
                        self.nodes.prefer_after_unreachable(position);
                    }
                    return Some(observation.map_err(|error| NodeFailure::from(&error)));
                }
            }
        }
        unreachable.map_or_else(
            || self.verification_failure().map(Err),
            |error| Some(Err(NodeFailure::from(&error))),
        )
    }

    /// Returns the failure of the first endpoint in broadcast preference
    /// order whose last verification failed, or `None` before any
    /// verification ends.
    fn verification_failure(&self) -> Option<NodeFailure> {
        let outcomes = self.outcomes.borrow();
        let observed = if self.primary_required {
            outcomes.first()?.is_some()
        } else {
            outcomes.iter().any(Option::is_some)
        };
        if !observed {
            return None;
        }
        let failure = self
            .nodes
            .preference_order()
            .filter(|position| !self.primary_required || *position == 0)
            .find_map(|position| outcomes.get(position)?.as_ref()?.as_ref().err().cloned());
        drop(outcomes);
        Some(failure.unwrap_or_else(|| {
            NodeFailure::from(&SourceError::NodeUnavailable {
                reason: "no node endpoint holds a first-contact verification".to_owned(),
            })
        }))
    }
}

/// Why the node dependency is degraded, as the readiness report shows it.
#[derive(Clone, Debug, PartialEq)]
enum NodeFailure {
    CapabilityMissing(NodeCapability),
    Unavailable {
        failure_class: &'static str,
        reason: String,
    },
}

impl From<&SourceError> for NodeFailure {
    fn from(error: &SourceError) -> Self {
        if let SourceError::NodeCapabilityMissing { capability } = error {
            return Self::CapabilityMissing(*capability);
        }
        Self::Unavailable {
            failure_class: error.upstream_classification().label(),
            reason: error.to_string(),
        }
    }
}

fn publish_wallet_node_observation(
    readiness: &WalletServingReadiness,
    observation: Result<UpstreamHealthSnapshot, NodeFailure>,
    outage: &mut Option<WalletNodeOutage>,
) {
    readiness.publish_node_source_cause(node_observation_cause(observation, outage));
}

fn node_observation_cause(
    observation: Result<UpstreamHealthSnapshot, NodeFailure>,
    outage: &mut Option<WalletNodeOutage>,
) -> ReadinessCause {
    match observation {
        Ok(snapshot) if snapshot.ready_for_queries => {
            *outage = None;
            ReadinessCause::Ready
        }
        Ok(snapshot) => {
            *outage = None;
            ReadinessCause::UpstreamNotReady(upstream_not_ready_detail(snapshot))
        }
        Err(NodeFailure::CapabilityMissing(capability)) => {
            *outage = None;
            ReadinessCause::NodeCapabilityMissing {
                capability: capability.name(),
            }
        }
        Err(NodeFailure::Unavailable {
            failure_class,
            reason,
        }) => {
            let continued =
                WalletNodeOutage::continue_with(outage.as_ref(), failure_class, &reason);
            let cause = ReadinessCause::NodeUnavailable(continued.detail.clone());
            *outage = Some(continued);
            cause
        }
    }
}

impl crate::LightWalletNodeQuery {
    /// Probes this endpoint's forwarding admission and health for the node-source dependency.
    #[must_use]
    pub fn spawn_node_readiness_probe(
        self,
        readiness: WalletServingReadiness,
        interval: Duration,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut outage = None;
            loop {
                let probe = async {
                    let node = self.verified_current_node().await?;
                    probe_wallet_node_readiness(node).await
                };
                let observation = tokio::select! {
                    () = cancel.cancelled() => break,
                    observation = probe => observation.map_err(|error| NodeFailure::from(&error)),
                };
                publish_wallet_node_observation(&readiness, observation, &mut outage);
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(interval) => {}
                }
            }
        })
    }
}

async fn probe_wallet_node_readiness(
    source: &impl NodeSource,
) -> Result<UpstreamHealthSnapshot, SourceError> {
    source.probe_tip_id().await?;
    source.poll_upstream_health().await
}

#[derive(Clone, Debug)]
struct WalletNodeOutage {
    started_at: Instant,
    detail: NodeUnavailableDetail,
}

impl WalletNodeOutage {
    /// Records one more failed observation of the node: it continues `prior`
    /// when an outage is already in progress and starts one otherwise.
    fn continue_with(prior: Option<&Self>, failure_class: &'static str, reason: &str) -> Self {
        let last_reason = Cow::Owned(truncate_readiness_reason(reason));
        let Some(prior) = prior else {
            return Self {
                started_at: Instant::now(),
                detail: NodeUnavailableDetail::first_iteration(failure_class, last_reason),
            };
        };
        let outage_seconds =
            u32::try_from(prior.started_at.elapsed().as_secs()).unwrap_or(u32::MAX);
        Self {
            started_at: prior.started_at,
            detail: NodeUnavailableDetail::extend_with(
                &prior.detail,
                failure_class,
                last_reason,
                outage_seconds,
            ),
        }
    }
}

fn truncate_readiness_reason(reason: &str) -> String {
    if reason.len() <= READINESS_REASON_MAX_BYTES {
        return reason.to_owned();
    }
    let mut truncated = reason
        .char_indices()
        .take_while(|(byte_index, _)| *byte_index < READINESS_REASON_MAX_BYTES)
        .map(|(_, character)| character)
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn upstream_not_ready_detail(snapshot: UpstreamHealthSnapshot) -> UpstreamNotReadyDetail {
    UpstreamNotReadyDetail {
        upstream_committed_height: snapshot.upstream_committed_height,
        upstream_estimated_height: snapshot.upstream_estimated_height,
        upstream_verification_progress: snapshot.upstream_verification_progress,
        upstream_health: UpstreamHealth {
            source: Cow::Borrowed(snapshot.source),
            reason: snapshot.reason,
        },
    }
}

/// The wallet-serving publisher's final teardown, which closes its
/// secondaries on a blocking thread, panicked or was cancelled.
#[derive(Debug, Error)]
#[error("wallet-serving publisher teardown failed: {0}")]
pub struct WalletServingPairTeardownError(#[source] tokio::task::JoinError);

/// Drops `publisher` on a blocking thread, because the last owner of a
/// secondary pays its close, and records how long the teardown took.
async fn tear_down_on_blocking_thread<T: Send + 'static>(
    publisher: T,
) -> Result<(), WalletServingPairTeardownError> {
    let started_at = Instant::now();
    let teardown = tokio::task::spawn_blocking(move || drop(publisher)).await;
    metrics::histogram!(
        "zinder_wallet_serving_pair_publisher_teardown_duration_seconds",
        "status" => if teardown.is_ok() { "ok" } else { "error" }
    )
    .record(started_at.elapsed());
    teardown.map_err(WalletServingPairTeardownError)
}

/// Errors that stop bootstrap or make a refresh generation ineligible.
#[derive(Debug, Error)]
pub enum WalletServingPairError {
    /// The canonical secondary failed admission or catch-up.
    #[error(transparent)]
    Canonical(#[from] CanonicalStoreError),
    /// The wallet secondary failed admission or catch-up.
    #[error(transparent)]
    Wallet(#[from] RocksDbWalletError),
    /// A bounded secondary generation directory could not be created before opening it.
    #[error("failed to create wallet-serving secondary generation directory {path}: {source}")]
    SecondaryGenerationDirectoryCreate {
        /// Per-generation directory that must exist before `RocksDB` can create its leaf.
        path: PathBuf,
        /// Filesystem failure returned while creating the generation directory.
        #[source]
        source: std::io::Error,
    },
    /// A bare writer-status endpoint could not be connected.
    #[error(transparent)]
    WriterStatusConnect(#[from] BearerTokenConnectError),
    /// The private canonical-control RPC failed.
    #[error("canonical writer-status RPC failed: {0}")]
    WriterStatusRpc(tonic::Status),
    /// Writer status did not contain an exact, usable canonical fence.
    #[error("canonical writer-status response did not contain a valid fence")]
    WriterStatusInvalid,
    /// Writer status disagreed with a same-epoch pair, indicating a fence or
    /// primary-path replacement inconsistency rather than normal lag.
    #[error("canonical writer-status fence disagrees with the serving pair")]
    WriterFenceMismatch,
    /// A blocking secondary operation stopped without a result.
    #[error("secondary candidate task failed: {0}")]
    CandidateTask(#[from] tokio::task::JoinError),
    /// A pair was unexpectedly absent after successful bootstrap.
    #[error("wallet-serving secondary pair slot is unavailable")]
    PairSlotUnavailable,
    /// A candidate was not present in the expected inactive generation.
    #[error("inactive secondary generation {generation} has no candidate")]
    CandidateUnavailable {
        /// Bounded generation slot that was expected to hold a candidate.
        generation: usize,
    },
    /// Convergence exhausted its time or attempt bound without a wallet-serving pair.
    #[error(
        "canonical and wallet secondaries did not converge within the configured bounds; last outcome={last_outcome:?}"
    )]
    ConvergenceTimedOut {
        /// Typed final mismatch observed before the bound expired.
        last_outcome: WalletServingConvergence,
    },
    /// Pair construction changed after pre-publication validation.
    #[error("wallet-serving read pair changed during publication: {0}")]
    PairPublication(crate::QueryError),
}

/// Typed candidate outcome used for readiness and metrics classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalletServingConvergence {
    /// Canonical and wallet secondaries are exact, but the writer advanced.
    ReplicaBehind,
    /// Canonical is current for the candidate but wallet projection trails it.
    ProjectionBehind,
    /// The admitted networks, canonical fence, or local schema evidence is invalid.
    SchemaOrFenceMismatch,
}

impl WalletServingConvergence {
    const fn label(self) -> &'static str {
        match self {
            Self::ReplicaBehind => "replica_behind",
            Self::ProjectionBehind => "projection_behind",
            Self::SchemaOrFenceMismatch => "schema_or_fence_mismatch",
        }
    }
}

struct GenerationWait {
    reason: &'static str,
    since: Instant,
}

/// Owns the bounded reader generations and their publication slot.
pub struct WalletServingPairPublisher {
    config: WalletServingPairConfig,
    writer_status: CanonicalWriterStatusClient,
    readiness: WalletServingReadiness,
    serving_pair_slot: Option<WalletServingPairSlot>,
    admitted_stored_set: Option<CanonicalStoredSet>,
    stored_set_retry_at: Option<Instant>,
    generations: [SecondaryGeneration; SECONDARY_GENERATION_COUNT],
    /// Closing a generation preserves both its cached blocks and its contribution
    /// to the store's cumulative tickers.
    store_resources: ServingStoreResources,
    /// Gates the memory sample of the open generations, whose table-cache
    /// lookups are too costly to repeat on every refresh.
    instance_memory_throttle: ResourceGaugeThrottle,
    published_generation: Option<usize>,
    generation_wait: Option<GenerationWait>,
    /// Trailing references to the pairs released from the slot.
    ///
    /// Holding them makes the publisher the last owner of every released
    /// pair, so a pair drains only when the publisher drops it, and the
    /// generation's secondaries then have no other owner. Reusing a
    /// generation requires its lease to have drained, which cannot happen
    /// while its reference is here.
    retired_pairs: Vec<RetiredPair>,
    /// Last attestation of the published pair by a following writer with a
    /// confirmed tip, and the halt the writer last reported, which refresh
    /// failures republish so a writer-status outage or a writer restart cannot
    /// reopen traffic on a frozen pair.
    writer_attestation: WriterAttestation,
    /// Writer lag observed at the last successful writer-status fetch.
    observed_lag_chain_epochs: u64,
    #[cfg(test)]
    candidate_preparations: CandidatePreparationCounts,
}

impl WalletServingPairPublisher {
    /// Bootstraps a pair from a bare writer-status endpoint, without
    /// ingest-control admission.
    ///
    /// The release runtimes bootstrap through
    /// [`Self::bootstrap_with_admitted_ingest_control`]; this entry point
    /// serves an embedding that reaches only `CanonicalControl`.
    pub async fn bootstrap_from_writer_status_endpoint(
        config: WalletServingPairConfig,
        readiness: WalletServingReadiness,
        writer_status_endpoint: &str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<(Self, WalletServingPairSlot), WalletServingPairError> {
        let writer_status =
            CanonicalWriterStatusClient::connect(writer_status_endpoint, bearer_token).await?;
        Self::bootstrap_with_writer_status(config, readiness, writer_status).await
    }

    /// Bootstraps the pair through the runtime's already-admitted ingest
    /// channel.
    pub async fn bootstrap_with_admitted_ingest_control(
        config: WalletServingPairConfig,
        readiness: WalletServingReadiness,
        ingest_control: &AdmittedIngestControl,
    ) -> Result<(Self, WalletServingPairSlot), WalletServingPairError> {
        let writer_status = CanonicalWriterStatusClient::from_admitted(ingest_control);
        Self::bootstrap_with_writer_status(config, readiness, writer_status).await
    }

    /// Opens, converges, and authenticates the first serving pair before the
    /// listener starts. Bootstrap fails closed instead of serving a primary
    /// handle or a mixed secondary view.
    async fn bootstrap_with_writer_status(
        config: WalletServingPairConfig,
        readiness: WalletServingReadiness,
        writer_status: CanonicalWriterStatusClient,
    ) -> Result<(Self, WalletServingPairSlot), WalletServingPairError> {
        let generations =
            std::array::from_fn(|generation| SecondaryGeneration::new(&config, generation));
        let store_resources = ServingStoreResources {
            canonical: SharedSecondaryResources::new(config.canonical_resource_budget),
            wallet: SharedSecondaryResources::new(config.wallet_resource_budget),
        };
        let writer_attestation =
            WriterAttestation::new(config.network, config.serving_pair_staleness_ceiling);
        let mut publisher = Self {
            config,
            writer_status,
            readiness,
            serving_pair_slot: None,
            admitted_stored_set: None,
            stored_set_retry_at: None,
            generations,
            store_resources,
            instance_memory_throttle: ResourceGaugeThrottle::for_instance_memory(),
            published_generation: None,
            generation_wait: None,
            retired_pairs: Vec::with_capacity(SECONDARY_GENERATION_COUNT),
            writer_attestation,
            observed_lag_chain_epochs: 0,
            #[cfg(test)]
            candidate_preparations: CandidatePreparationCounts::default(),
        };
        publisher.prepare_candidate(0).await?;
        publisher.converge_and_publish(0).await?;
        publisher.record_resource_gauges().await;
        let Some(serving_pair_slot) = publisher.serving_pair_slot.clone() else {
            return Err(WalletServingPairError::PairSlotUnavailable);
        };
        Ok((publisher, serving_pair_slot))
    }

    /// Runs bounded refreshes until shutdown, then retains reader ownership
    /// until the serving runtime confirms its request paths have drained.
    ///
    /// A refresh runs every catch-up interval, but at least every half
    /// staleness ceiling, at once when a read first finds a pair missing a
    /// table file, and when the staleness ceiling passes after the writer
    /// last attested the pair. A writer that answers therefore attests the
    /// pair again before the ceiling passes, whatever the catch-up interval.
    /// Admission on an unattested pair ends at the ceiling even while a
    /// refresh is still running, and at once when the loop starts after it,
    /// so a stalled writer-status read cannot keep admitting traffic past
    /// it. Every refresh failure leaves the current immutable pair untouched
    /// and updates readiness with its typed cause; it never falls back to a
    /// primary store.
    ///
    /// The task closes the publisher's secondaries on a blocking thread once
    /// `serving_runtime_drained` is cancelled, and fails when that teardown
    /// fails.
    #[must_use = "await the publisher on shutdown so secondary handles close before process exit"]
    pub fn spawn(
        mut self,
        cancel: CancellationToken,
        serving_runtime_drained: CancellationToken,
    ) -> JoinHandle<Result<(), WalletServingPairTeardownError>> {
        tokio::spawn(async move {
            let signals = self
                .serving_pair_slot
                .as_ref()
                .map(|slot| Arc::clone(&slot.signals));
            let refresh_interval = self
                .writer_attestation
                .refresh_interval(self.config.catchup_interval);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(refresh_interval) => {}
                    () = missing_table_file_reported(signals.as_deref()) => {}
                    () = self.writer_attestation.stale_deadline().passed() => {}
                }
                self.refresh_within_staleness_ceiling().await;
            }
            self.end_generation_wait("interrupted");
            serving_runtime_drained.cancelled().await;
            tear_down_on_blocking_thread(self).await
        })
    }

    /// Refreshes, ending admission on the published pair once the staleness
    /// ceiling of the last attestation passes before the refresh completes.
    async fn refresh_within_staleness_ceiling(&mut self) {
        let deadline = self.writer_attestation.stale_deadline();
        let admission = self.readiness.composed.clone();
        refresh_within_stale_deadline(deadline, &admission, self.refresh_and_record()).await;
        self.writer_attestation.end_lapsed_allowance();
    }

    async fn refresh_and_record(&mut self) {
        let refreshed = self.refresh_once().await;
        if refreshed.is_err() {
            self.end_generation_wait("interrupted");
        }
        self.record_resource_gauges().await;
        let Err(error) = refreshed else {
            metrics::counter!(
                "zinder_wallet_serving_pair_publisher_refresh_total",
                "status" => "ok",
                "error_class" => "none"
            )
            .increment(1);
            return;
        };
        let still_admitted = self.record_refresh_failure(&error);
        if refresh_failure_is_routine_lag(
            &error,
            self.observed_lag_chain_epochs,
            self.config.replica_lag_threshold_chain_epochs,
            still_admitted,
        ) {
            tracing::debug!(
                target: "zinder::wallet_serving",
                event = "wallet_serving_pair_publisher_refresh_failed",
                error = %error,
                lag_chain_epochs = self.observed_lag_chain_epochs,
                "inactive wallet-serving pair has not caught up yet; retaining the prior pair"
            );
        } else {
            tracing::warn!(
                target: "zinder::wallet_serving",
                event = "wallet_serving_pair_publisher_refresh_failed",
                error = %error_with_sources(&error),
                lag_chain_epochs = self.observed_lag_chain_epochs,
                "inactive wallet-serving pair refresh failed; retaining the prior pair"
            );
        }
    }

    async fn refresh_once(&mut self) -> Result<(), WalletServingPairError> {
        let now = Instant::now();
        self.release_expired_pairs(now);
        self.reap_retired_pairs(now).await;
        let serving_pair_slot = self
            .serving_pair_slot
            .as_ref()
            .ok_or(WalletServingPairError::PairSlotUnavailable)?;
        let active_pair = serving_pair_slot.capture_ephemeral();
        self.adopt_ingest_control_halt(&active_pair);
        let writer_status = self.writer_status.fetch().await?;
        let relation = self.update_active_readiness(&active_pair, &writer_status)?;
        // A pair missing a table file is replaced at the writer's fence even
        // when that fence has not moved.
        let replaces_active_pair = active_pair.has_missing_table_file();
        drop(active_pair);
        let needs_generation = match relation {
            ActiveWriterRelation::Exact => replaces_active_pair,
            ActiveWriterRelation::Refusing => false,
            ActiveWriterRelation::Behind => true,
        };
        if !needs_generation {
            self.end_generation_wait("interrupted");
            return Ok(());
        }
        if self
            .stored_set_retry_at
            .is_some_and(|retry_at| Instant::now() < retry_at)
        {
            return Ok(());
        }

        // Waiting keeps every retained pair pinnable for its whole window. A
        // later refresh publishes the writer's fence at that time, so the
        // blocks that arrive meanwhile share one publication.
        let fence = writer_status
            .fence
            .as_ref()
            .ok_or_else(|| writer_fence_divergence_error(CanonicalFenceDivergence::MissingFence))?;
        let Some(generation) = self.inactive_generation() else {
            self.record_generation_wait(fence);
            return Ok(());
        };
        self.end_generation_wait("available");
        tracing::info!(
            target: "zinder::wallet_serving",
            event = "wallet_serving_pair_publisher_generation_selected",
            process_id = std::process::id(),
            generation,
            chain_epoch = fence.chain_epoch_id,
            event_sequence = fence.event_sequence,
            "selected a secondary generation for the observed writer fence"
        );
        self.prepare_candidate(generation).await?;
        self.converge_and_publish(generation).await
    }

    /// Publishes shared cache usage, cumulative tickers, and the number of open
    /// generations, and once per throttle interval the memory the open
    /// generations hold outside the caches.
    async fn record_resource_gauges(&mut self) {
        self.store_resources
            .canonical
            .record_resource_gauges(StoreRole::CanonicalSecondary);
        self.store_resources
            .wallet
            .record_resource_gauges(StoreRole::WalletSecondary);
        let open_generations = self
            .generations
            .iter()
            .filter(|generation| !matches!(generation.state, SecondaryGenerationState::Vacant))
            .count();
        metrics::gauge!("zinder_wallet_serving_pair_publisher_open_generations").set(f64::from(
            u32::try_from(open_generations).unwrap_or(u32::MAX),
        ));
        if self.instance_memory_throttle.should_sample() {
            self.record_instance_memory().await;
        }
    }

    /// Publishes the memtable and table-reader memory of every open
    /// generation, summed for each store.
    ///
    /// The sample looks up every table file's reader, so it runs on the
    /// blocking pool. Published readers are shared with that task, and a
    /// candidate moves to it and back as it does for catch-up. A task that
    /// fails drops the candidate, and a later refresh opens its slot cold.
    async fn record_instance_memory(&mut self) {
        let published =
            self.generations
                .iter()
                .filter_map(|generation| match &generation.state {
                    SecondaryGenerationState::Published { readers, .. } => {
                        Some((Arc::clone(&readers.canonical), Arc::clone(&readers.wallet)))
                    }
                    SecondaryGenerationState::Vacant
                    | SecondaryGenerationState::Candidate { .. } => None,
                })
                .collect::<Vec<_>>();
        let candidates = self
            .generations
            .iter_mut()
            .enumerate()
            .filter_map(|(generation, slot)| {
                slot.take_candidate()
                    .map(|candidate| (generation, candidate))
            })
            .collect::<Vec<_>>();
        let sampled = tokio::task::spawn_blocking(move || {
            // Every published pair sees the same two caches, even when a retired pair is pinned.
            if let Some((canonical, wallet)) = published.first() {
                canonical.record_block_cache_entry_gauges();
                wallet.record_block_cache_entry_gauges();
            }
            let canonical = published
                .iter()
                .map(|(canonical, _)| canonical.instance_memory())
                .chain(
                    candidates
                        .iter()
                        .map(|(_, candidate)| candidate.canonical.instance_memory()),
                )
                .sum::<RocksDbInstanceMemory>();
            let wallet = published
                .iter()
                .map(|(_, wallet)| wallet.instance_memory())
                .chain(
                    candidates
                        .iter()
                        .map(|(_, candidate)| candidate.wallet.instance_memory()),
                )
                .sum::<RocksDbInstanceMemory>();
            (canonical, wallet, candidates)
        })
        .await;
        let Ok((canonical, wallet, candidates)) = sampled else {
            return;
        };
        for (generation, candidate) in candidates {
            self.generations[generation].state = SecondaryGenerationState::Candidate { candidate };
        }
        record_instance_memory_gauges(StoreRole::CanonicalSecondary, canonical);
        record_instance_memory_gauges(StoreRole::WalletSecondary, wallet);
    }

    /// Counts a refresh that found every unpublished slot taken and published
    /// nothing.
    ///
    /// `retention_window` means a slot holds a replaced pair whose window has
    /// not ended, so the wait ends by the time that window does at the latest.
    /// `in_flight_requests` means only released pairs that requests still
    /// hold take the slots.
    fn record_generation_wait(&mut self, fence: &CanonicalWriterFence) {
        let reason = if self
            .serving_pair_slot
            .as_ref()
            .is_some_and(|slot| slot.retained_pair_count() > 0)
        {
            "retention_window"
        } else {
            "in_flight_requests"
        };
        metrics::counter!(
            "zinder_wallet_serving_pair_publisher_generation_wait_total",
            "reason" => reason
        )
        .increment(1);
        if self
            .generation_wait
            .as_ref()
            .is_some_and(|wait| wait.reason == reason)
        {
            return;
        }
        self.end_generation_wait("reason_changed");
        self.generation_wait = Some(GenerationWait {
            reason,
            since: Instant::now(),
        });
        tracing::info!(
            target: "zinder::wallet_serving",
            event = "wallet_serving_pair_publisher_generation_wait_started",
            process_id = std::process::id(),
            reason,
            chain_epoch = fence.chain_epoch_id,
            event_sequence = fence.event_sequence,
            "observed every inactive generation occupied"
        );
    }

    fn end_generation_wait(&mut self, outcome: &'static str) {
        if let Some(wait) = self.generation_wait.take() {
            let elapsed = wait.since.elapsed();
            metrics::histogram!(
                "zinder_wallet_serving_pair_publisher_generation_wait_duration_seconds",
                "reason" => wait.reason,
                "outcome" => outcome
            )
            .record(elapsed);
            tracing::info!(
                target: "zinder::wallet_serving",
                event = "wallet_serving_pair_publisher_generation_wait_ended",
                process_id = std::process::id(),
                reason = wait.reason,
                outcome,
                elapsed_seconds = elapsed.as_secs_f64(),
                "finished observing a generation wait"
            );
        }
    }

    /// Stops serving pins on retained pairs replaced longer ago than the
    /// retention window allows.
    fn release_expired_pairs(&mut self, now: Instant) {
        let Some(slot) = &self.serving_pair_slot else {
            return;
        };
        let expired = slot.release_expired(now);
        if expired.is_empty() {
            return;
        }
        record_retained_pair_count(slot);
        self.retire_pairs(expired, now);
    }

    /// Takes ownership of pairs that just left the slot.
    fn retire_pairs(
        &mut self,
        pairs: impl IntoIterator<Item = Arc<WalletServingReadPair>>,
        retired_at: Instant,
    ) {
        self.retired_pairs.extend(
            pairs
                .into_iter()
                .map(|pair| RetiredPair { pair, retired_at }),
        );
    }

    /// Revokes reads on every retired pair that requests have held for at
    /// least [`RELEASED_PAIR_READ_LIMIT`] since it left the slot.
    ///
    /// A revoked read fails at its next bounded step and drops the pair, so a
    /// later refresh can close the pair and reuse its generation.
    fn revoke_overdue_reads(&self, now: Instant) {
        for retired in &self.retired_pairs {
            let overdue =
                now.saturating_duration_since(retired.retired_at) >= RELEASED_PAIR_READ_LIMIT;
            if overdue && Arc::strong_count(&retired.pair) > 1 && retired.pair.revoke_reads() {
                metrics::counter!("zinder_wallet_serving_pair_publisher_revoked_pairs_total")
                    .increment(1);
                tracing::warn!(
                    target: "zinder::wallet_serving",
                    event = "wallet_serving_pair_reads_revoked",
                    chain_epoch = retired.pair.canonical_fence().chain_epoch_id().value(),
                    read_limit_seconds = RELEASED_PAIR_READ_LIMIT.as_secs(),
                    "requests held a released wallet-serving pair past the read limit; stopping them"
                );
            }
        }
    }

    /// Drops every released pair that no request still holds, on the
    /// blocking pool.
    ///
    /// A pair stays alive until the last request that captured it has
    /// released it; that is the same condition [`Self::inactive_generation`]
    /// waits on before reusing its generation. The generation keeps its
    /// secondaries for the next candidate, and the drop stays off the reactor
    /// like every other release of a secondary handle, because the last owner
    /// of a direct-I/O instance pays its close. Reads that held a pair too
    /// long are revoked first.
    async fn reap_retired_pairs(&mut self, now: Instant) {
        self.revoke_overdue_reads(now);
        let (released, pinned): (Vec<_>, Vec<_>) = std::mem::take(&mut self.retired_pairs)
            .into_iter()
            .partition(|retired| Arc::strong_count(&retired.pair) == 1);
        self.retired_pairs = pinned;
        metrics::gauge!("zinder_wallet_serving_pair_publisher_pinned_retired_pairs").set(
            f64::from(u32::try_from(self.retired_pairs.len()).unwrap_or(u32::MAX)),
        );
        if released.is_empty() {
            return;
        }
        let started_at = Instant::now();
        let teardown = tokio::task::spawn_blocking(move || drop(released)).await;
        metrics::histogram!(
            "zinder_wallet_serving_pair_publisher_teardown_duration_seconds",
            "status" => if teardown.is_ok() { "ok" } else { "error" }
        )
        .record(started_at.elapsed());
    }

    /// Chooses the generation slot for the next candidate, or `None` while
    /// the slot retains, or requests still hold, the pair of every slot but
    /// the published one.
    ///
    /// An open candidate is reused first. A drained published slot is
    /// preferred over a vacant one because it is caught up in place instead
    /// of opened cold, so the extra slots open only while older pairs are
    /// retained or pinned. Each search starts after the published slot, so
    /// drained slots are reused in turn and none keeps the primary files of
    /// an old catch-up open while the others advance.
    fn inactive_generation(&self) -> Option<usize> {
        let first = self
            .published_generation
            .map_or(0, |published| published + 1);
        let unpublished = || {
            (first..first + SECONDARY_GENERATION_COUNT)
                .map(|position| position % SECONDARY_GENERATION_COUNT)
                .filter(|generation| Some(*generation) != self.published_generation)
        };
        let state = |generation: usize| &self.generations[generation].state;
        unpublished()
            .find(|generation| {
                matches!(
                    state(*generation),
                    SecondaryGenerationState::Candidate { .. }
                )
            })
            .or_else(|| {
                unpublished().find(|generation| {
                    matches!(
                        state(*generation),
                        SecondaryGenerationState::Published { lease, readers }
                            if lease.is_reusable() && !readers.is_shared()
                    )
                })
            })
            .or_else(|| {
                unpublished().find(|generation| {
                    matches!(state(*generation), SecondaryGenerationState::Vacant)
                })
            })
    }

    /// Makes `generation` hold a candidate unless it already does.
    ///
    /// The caller chooses a slot whose prior pair has no strong references.
    /// A drained published generation becomes the candidate with its
    /// secondaries still open, and convergence catches them up in place.
    /// Only a vacant slot is opened cold. A generation whose secondaries are
    /// still shared stays published and frozen, and the refresh fails instead
    /// of opening a second instance over the same metadata paths.
    async fn prepare_candidate(&mut self, generation: usize) -> Result<(), WalletServingPairError> {
        let state = std::mem::replace(
            &mut self.generations[generation].state,
            SecondaryGenerationState::Vacant,
        );
        let (candidate, preparation) = match state {
            SecondaryGenerationState::Candidate { candidate } => {
                self.generations[generation].state =
                    SecondaryGenerationState::Candidate { candidate };
                return Ok(());
            }
            SecondaryGenerationState::Published { lease, readers } => {
                match readers.into_candidate() {
                    Ok(candidate) => (Box::new(candidate), CandidatePreparation::Recycled),
                    Err(readers) => {
                        self.generations[generation].state =
                            SecondaryGenerationState::Published { lease, readers };
                        return Err(WalletServingPairError::CandidateUnavailable { generation });
                    }
                }
            }
            SecondaryGenerationState::Vacant => (
                self.open_cold_candidate(generation).await?,
                CandidatePreparation::Cold,
            ),
        };
        metrics::counter!(
            "zinder_wallet_serving_pair_publisher_candidate_prepared_total",
            "kind" => preparation.label()
        )
        .increment(1);
        #[cfg(test)]
        self.candidate_preparations.record(preparation);
        self.generations[generation].state = SecondaryGenerationState::Candidate { candidate };
        Ok(())
    }

    /// Opens both secondaries of `generation` and runs their full cold
    /// admission against the primaries.
    ///
    /// This deliberately does not delete the metadata path: opening an
    /// inactive path is safe and bounded, while deleting it would widen the
    /// failure surface without improving correctness.
    async fn open_cold_candidate(
        &self,
        generation: usize,
    ) -> Result<Box<SecondaryPairCandidate>, WalletServingPairError> {
        let secondary_generation = &self.generations[generation];
        let canonical_primary_path = self.config.canonical_primary_path.clone();
        let canonical_generation_path = secondary_generation.canonical_generation_path.clone();
        let canonical_secondary_path = secondary_generation.canonical_secondary_path.clone();
        let wallet_primary_path = self.config.wallet_primary_path.clone();
        let wallet_generation_path = secondary_generation.wallet_generation_path.clone();
        let wallet_secondary_path = secondary_generation.wallet_secondary_path.clone();
        let canonical_reorg_policy = self.config.canonical_reorg_policy;
        let requirements = self.config.canonical_requirements.clone();
        let canonical_resources = RocksDbOpenResources::from(&self.store_resources.canonical);
        let wallet_resources = RocksDbOpenResources::from(&self.store_resources.wallet);
        let network = self.config.network;
        let started_at = Instant::now();
        let opened = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&canonical_generation_path).map_err(|source| {
                WalletServingPairError::SecondaryGenerationDirectoryCreate {
                    path: canonical_generation_path.clone(),
                    source,
                }
            })?;
            std::fs::create_dir_all(&wallet_generation_path).map_err(|source| {
                WalletServingPairError::SecondaryGenerationDirectoryCreate {
                    path: wallet_generation_path.clone(),
                    source,
                }
            })?;
            let canonical = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
                canonical_primary_path,
                canonical_secondary_path,
                network,
                requirements,
                canonical_reorg_policy,
                canonical_resources,
            )?;
            let wallet = RocksDbWalletSecondary::open_ready(
                wallet_primary_path,
                wallet_secondary_path,
                network,
                wallet_resources,
            )?;
            Ok::<_, WalletServingPairError>(Box::new(SecondaryPairCandidate { canonical, wallet }))
        })
        .await
        .unwrap_or_else(|error| Err(WalletServingPairError::CandidateTask(error)));
        let elapsed = started_at.elapsed();
        metrics::histogram!(
            "zinder_wallet_serving_pair_publisher_cold_open_duration_seconds",
            "status" => if opened.is_ok() { "ok" } else { "error" }
        )
        .record(elapsed);
        if opened.is_ok() {
            tracing::info!(
                target: "zinder::wallet_serving",
                event = "wallet_serving_pair_publisher_generation_opened",
                generation,
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "opened and cold-admitted a wallet-serving secondary generation"
            );
        }
        opened
    }

    async fn converge_and_publish(
        &mut self,
        generation: usize,
    ) -> Result<(), WalletServingPairError> {
        let retry_delay = self
            .config
            .catchup_interval
            .min(CONVERGENCE_RETRY_DELAY_CAP);
        let policy = CanonicalFenceConvergencePolicy {
            timeout: self.config.convergence_timeout,
            max_attempts: Some(NonZeroU32::from(self.config.convergence_attempts)),
            initial_delay: retry_delay,
            max_delay: retry_delay,
        };
        let network = self.config.network;
        let mut probe = CandidatePairFenceProbe {
            publisher: self,
            generation,
            last_outcome: WalletServingConvergence::ReplicaBehind,
        };
        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            network,
            policy,
            &CancellationToken::new(),
        )
        .await;
        let last_outcome = probe.last_outcome;
        match outcome {
            Ok(CanonicalFenceConvergence::Converged(attesting_status)) => {
                self.publish_candidate(generation, &attesting_status).await
            }
            Ok(CanonicalFenceConvergence::Cancelled | CanonicalFenceConvergence::Trailing(_))
            | Err(CanonicalFenceConvergenceError::DidNotConverge) => {
                Err(WalletServingPairError::ConvergenceTimedOut { last_outcome })
            }
            Err(CanonicalFenceConvergenceError::Probe(error)) => Err(error),
            Err(CanonicalFenceConvergenceError::Diverged(divergence)) => {
                Err(writer_fence_divergence_error(divergence))
            }
        }
    }

    async fn catch_up_candidate(
        &mut self,
        generation: usize,
    ) -> Result<Result<(), WalletServingConvergence>, WalletServingPairError> {
        let state = std::mem::replace(
            &mut self.generations[generation].state,
            SecondaryGenerationState::Vacant,
        );
        let SecondaryGenerationState::Candidate { mut candidate } = state else {
            return Err(WalletServingPairError::CandidateUnavailable { generation });
        };
        let started_at = Instant::now();
        let candidate_task_outcome = tokio::task::spawn_blocking(move || {
            candidate.canonical.try_catch_up()?;
            candidate.wallet.try_catch_up()?;
            let convergence = candidate_convergence(&candidate)?;
            Ok::<_, WalletServingPairError>((candidate, convergence))
        })
        .await;
        match candidate_task_outcome {
            Ok(Ok((candidate, convergence))) => {
                metrics::histogram!(
                    "zinder_wallet_serving_pair_publisher_catchup_duration_seconds",
                    "status" => "ok"
                )
                .record(started_at.elapsed());
                self.generations[generation].state =
                    SecondaryGenerationState::Candidate { candidate };
                Ok(convergence)
            }
            Ok(Err(error)) => {
                metrics::histogram!(
                    "zinder_wallet_serving_pair_publisher_catchup_duration_seconds",
                    "status" => "error"
                )
                .record(started_at.elapsed());
                Err(error)
            }
            Err(error) => Err(WalletServingPairError::CandidateTask(error)),
        }
    }

    fn candidate_network_upgrade_schedule(
        &self,
        generation: usize,
    ) -> Result<Option<&CanonicalActivationSchedule>, WalletServingPairError> {
        let SecondaryGenerationState::Candidate { candidate } = &self.generations[generation].state
        else {
            return Err(WalletServingPairError::CandidateUnavailable { generation });
        };
        Ok(candidate.canonical.network_upgrade_schedule())
    }

    fn candidate_wallet_source(
        &self,
        generation: usize,
    ) -> Result<WalletCanonicalSourceIdentity, WalletServingPairError> {
        let SecondaryGenerationState::Candidate { candidate } = &self.generations[generation].state
        else {
            return Err(WalletServingPairError::CandidateUnavailable { generation });
        };
        Ok(WalletCanonicalSourceIdentity::from_ready_evidence(
            candidate.wallet.ready_evidence(),
        ))
    }

    async fn publish_candidate(
        &mut self,
        generation: usize,
        writer_status: &CanonicalWriterStatusResponse,
    ) -> Result<(), WalletServingPairError> {
        let state = std::mem::replace(
            &mut self.generations[generation].state,
            SecondaryGenerationState::Vacant,
        );
        let SecondaryGenerationState::Candidate { candidate } = state else {
            return Err(WalletServingPairError::CandidateUnavailable { generation });
        };
        let admitted_stored_set = self.admitted_stored_set;
        if admitted_stored_set.is_some_and(|expected| candidate.canonical.stored_set() != expected)
        {
            self.writer_attestation.withdraw_allowance();
            self.stored_set_retry_at = Some(Instant::now() + STORED_SET_RETRY_DELAY);
        }
        let (pair, readers) = tokio::task::spawn_blocking(move || {
            if let Some(expected) = admitted_stored_set {
                candidate.canonical.require_stored_set(expected)?;
            }
            let readers = PublishedReaders::new(*candidate);
            let canonical =
                RocksDbCanonicalReader::new(Arc::clone(&readers.canonical)).map_err(|error| {
                    WalletServingPairError::PairPublication(crate::QueryError::canonical_read(
                        error,
                    ))
                })?;
            let wallet = RocksDbWalletReader::new(Arc::clone(&readers.wallet));
            WalletServingReadPair::new(Arc::new(canonical), Arc::new(wallet))
                .map(|pair| (Arc::new(pair), readers))
                .map_err(WalletServingPairError::PairPublication)
        })
        .await
        .map_err(WalletServingPairError::CandidateTask)??;
        self.stored_set_retry_at = None;
        if let Some(slot) = &self.serving_pair_slot {
            let now = Instant::now();
            let released = slot.publish(Arc::clone(&pair), now);
            record_retained_pair_count(slot);
            self.retire_pairs(released, now);
        } else {
            self.admitted_stored_set = Some(readers.canonical.stored_set());
            self.serving_pair_slot = Some(WalletServingPairSlot::new(Arc::clone(&pair)));
        }
        self.generations[generation].state = SecondaryGenerationState::Published {
            lease: GenerationLease::new(&pair),
            readers,
        };
        self.published_generation = Some(generation);
        self.observed_lag_chain_epochs = 0;
        let visible_height = Some(pair.canonical_fence().visible_tip().height.value());
        let refusal = self
            .writer_attestation
            .observe(writer_status, visible_height);
        if refusal.is_none() {
            self.writer_attestation.attest();
        }
        self.readiness
            .publish_served_epoch(pair.chain_epoch().created_at);
        self.readiness
            .publish_pair_state(refusal.unwrap_or_else(|| ReadinessState::ready(visible_height)));
        metrics::counter!("zinder_wallet_serving_pair_publisher_publications_total").increment(1);
        tracing::info!(
            target: "zinder::wallet_serving",
            event = "wallet_serving_pair_publisher_published",
            generation,
            chain_epoch = pair.canonical_fence().chain_epoch_id().value(),
            event_sequence = pair.canonical_fence().chain_event_sequence(),
            visible_height = pair.canonical_fence().visible_tip().height.value(),
            visible_tip_hash = ?pair.canonical_fence().visible_tip().hash,
            sequence_digest = ?pair.canonical_fence().sequence_digest().as_bytes(),
            process_id = std::process::id(),
            "published exact immutable canonical and wallet secondary pair"
        );
        Ok(())
    }

    /// Takes over a writer halt that only the ingest-control probe observed,
    /// as if this publisher had read it from writer status.
    ///
    /// The halt then withdraws the staleness allowance and refuses traffic on
    /// `active_pair` until a writer status read after this call shows a
    /// following writer with a confirmed tip. The same refresh validates the
    /// pair's fence against that status before it publishes an admitting
    /// state, so a probe that sees the writer recover cannot reopen traffic
    /// on a pair the recovered writer no longer attests.
    fn adopt_ingest_control_halt(&mut self, active_pair: &WalletServingReadPair) {
        let visible_height = Some(active_pair.canonical_fence().visible_tip().height.value());
        self.readiness.adopt_ingest_control_halt(|halt| {
            self.writer_attestation.remember_halt(halt, visible_height)
        });
    }

    fn update_active_readiness(
        &mut self,
        active_pair: &WalletServingReadPair,
        writer_status: &CanonicalWriterStatusResponse,
    ) -> Result<ActiveWriterRelation, WalletServingPairError> {
        let active_source = active_pair.wallet_source();
        let visible_height = Some(active_source.source_position().tip.height.value());
        let relation = classify_canonical_writer_fence(
            writer_status,
            self.config.network,
            &wallet_source_writer_fence(active_source),
        );
        if let CanonicalFenceRelation::Diverged(
            divergence @ (CanonicalFenceDivergence::MissingFence
            | CanonicalFenceDivergence::NetworkMismatch),
        ) = relation
        {
            return Err(writer_fence_divergence_error(divergence));
        }
        // A parked writer, or one that has not confirmed its tip since it
        // reopened, still attests its fence; the pair stays published but
        // refuses traffic, and no replacement pair is prepared.
        if let Some(refusal) = self
            .writer_attestation
            .observe(writer_status, visible_height)
        {
            self.readiness.publish_pair_state(refusal);
            return Ok(ActiveWriterRelation::Refusing);
        }
        let behind = match relation {
            CanonicalFenceRelation::Diverged(divergence) => {
                return Err(writer_fence_divergence_error(divergence));
            }
            CanonicalFenceRelation::Matches
                if writer_attests_network_upgrade_schedule(
                    writer_status,
                    active_pair.canonical().network_upgrade_schedule(),
                ) =>
            {
                record_replica_lag(0);
                self.observed_lag_chain_epochs = 0;
                if self.stored_set_retry_at.is_some() && active_pair.has_missing_table_file() {
                    self.readiness.publish_pair_state(ReadinessState::not_ready(
                        ReadinessCause::StorageUnavailable,
                    ));
                } else {
                    self.stored_set_retry_at = None;
                    self.writer_attestation.attest();
                    self.readiness
                        .publish_pair_state(ReadinessState::ready(visible_height));
                }
                return Ok(ActiveWriterRelation::Exact);
            }
            // A writer restart can admit a new schedule revision without moving
            // the fence; the immutable active pair then refreshes like a
            // lagging one.
            CanonicalFenceRelation::Matches => CanonicalFenceLag {
                lag_chain_epochs: 0,
                secondary_visible_tip_height: active_source.source_position().tip.height.value(),
                writer_visible_tip_height: active_source.source_position().tip.height.value(),
            },
            CanonicalFenceRelation::SecondaryBehind(behind) => behind,
        };
        let lag = behind.lag_chain_epochs;
        record_replica_lag(lag);
        self.observed_lag_chain_epochs = lag;
        if self.stored_set_retry_at.is_some() {
            self.readiness.publish_pair_state(ReadinessState::not_ready(
                ReadinessCause::StorageUnavailable,
            ));
        } else if lag > self.config.replica_lag_threshold_chain_epochs {
            self.readiness.publish_pair_state(
                self.stale_serving_pair_readiness(visible_height)
                    .unwrap_or_else(|| ReadinessState::replica_lagging(lag, visible_height)),
            );
        } else {
            self.readiness
                .publish_pair_state(ReadinessState::ready_with_target(
                    visible_height,
                    Some(behind.writer_visible_tip_height),
                ));
        }
        Ok(ActiveWriterRelation::Behind)
    }

    /// Returns the traffic-permitting stale-pair state while the published
    /// pair is still within the staleness ceiling.
    ///
    /// `None` once the ceiling passes, so the caller falls back to the
    /// fail-closed cause and the service stops admitting traffic.
    fn stale_serving_pair_readiness(&self, visible_height: Option<u32>) -> Option<ReadinessState> {
        let staleness = self.writer_attestation.stale_allowance()?;
        Some(ReadinessState::serving_pair_stale(
            self.observed_lag_chain_epochs,
            staleness.as_secs(),
            visible_height,
        ))
    }

    /// Returns the visible tip height of the published pair, or `None` before
    /// the first publication.
    fn published_visible_height(&self) -> Option<u32> {
        self.serving_pair_slot.as_ref().map(|serving_pair_slot| {
            serving_pair_slot
                .capture_ephemeral()
                .canonical_fence()
                .visible_tip()
                .height
                .value()
        })
    }

    /// Publishes the readiness a failed refresh leaves behind and returns
    /// whether that readiness still admits traffic on the published pair.
    fn record_refresh_failure(&self, error: &WalletServingPairError) -> bool {
        let visible_height = self.published_visible_height();
        let admitted_while_stale = refresh_failure_retains_attested_pair(error)
            .then(|| self.stale_serving_pair_readiness(visible_height))
            .flatten();
        // A writer last seen parked keeps its halt through any later failure;
        // only a valid status without the halt clears it.
        let failure_state = self
            .writer_attestation
            .remembered_halt()
            .or(admitted_while_stale)
            .unwrap_or_else(|| {
                refresh_failure_not_ready_cause(error).map_or_else(
                    || {
                        ReadinessState::replica_lagging(
                            self.config
                                .replica_lag_threshold_chain_epochs
                                .saturating_add(1),
                            visible_height,
                        )
                    },
                    ReadinessState::not_ready,
                )
            });
        let still_admitted = failure_state.cause.permits_traffic();
        self.readiness.publish_pair_state(failure_state);
        metrics::counter!(
            "zinder_wallet_serving_pair_publisher_refresh_total",
            "status" => "error",
            "error_class" => wallet_serving_pair_error_class(error)
        )
        .increment(1);
        still_admitted
    }
}

/// Resolves once a read reports a pair missing a table file, and never when
/// the publisher has no slot yet.
async fn missing_table_file_reported(signals: Option<&SlotSignals>) {
    match signals {
        Some(signals) => signals.missing_table_file.notified().await,
        None => std::future::pending().await,
    }
}

/// Renders `error` followed by each source its message does not already end
/// with.
///
/// The canonical store's `RocksDB` failure leaves the driver status, which
/// names the missing `.sst` path, to its source.
fn error_with_sources(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let message = cause.to_string();
        if !rendered.ends_with(&message) {
            rendered.push_str(": ");
            rendered.push_str(&message);
        }
        source = cause.source();
    }
    rendered
}

fn record_retained_pair_count(slot: &WalletServingPairSlot) {
    let retained_pairs = u32::try_from(slot.retained_pair_count()).unwrap_or(u32::MAX);
    metrics::gauge!("zinder_wallet_serving_pair_publisher_retained_pairs")
        .set(f64::from(retained_pairs));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveWriterRelation {
    Exact,
    Behind,
    /// The writer is parked on a reorg deeper than its window, or has not
    /// confirmed its tip since it reopened. Its fence does not advance in
    /// either state, so no replacement pair is prepared.
    Refusing,
}

/// The cache and statistics of each store shared by all serving generations.
struct ServingStoreResources {
    canonical: SharedSecondaryResources,
    wallet: SharedSecondaryResources,
}

struct SecondaryGeneration {
    canonical_generation_path: PathBuf,
    canonical_secondary_path: PathBuf,
    wallet_generation_path: PathBuf,
    wallet_secondary_path: PathBuf,
    state: SecondaryGenerationState,
}

impl SecondaryGeneration {
    fn new(config: &WalletServingPairConfig, generation: usize) -> Self {
        let generation_name = format!("generation-{generation}");
        let canonical_generation_path = config.canonical_secondary_root.join(&generation_name);
        let wallet_generation_path = config.wallet_secondary_root.join(generation_name);
        Self {
            canonical_secondary_path: canonical_generation_path.join("canonical"),
            canonical_generation_path,
            wallet_secondary_path: wallet_generation_path.join("wallet"),
            wallet_generation_path,
            state: SecondaryGenerationState::Vacant,
        }
    }

    /// Takes the candidate this generation holds, leaving the slot vacant.
    fn take_candidate(&mut self) -> Option<Box<SecondaryPairCandidate>> {
        match std::mem::replace(&mut self.state, SecondaryGenerationState::Vacant) {
            SecondaryGenerationState::Candidate { candidate } => Some(candidate),
            state @ (SecondaryGenerationState::Vacant
            | SecondaryGenerationState::Published { .. }) => {
                self.state = state;
                None
            }
        }
    }
}

enum SecondaryGenerationState {
    Vacant,
    Candidate {
        candidate: Box<SecondaryPairCandidate>,
    },
    Published {
        lease: GenerationLease<WalletServingReadPair>,
        readers: PublishedReaders,
    },
}

struct SecondaryPairCandidate {
    canonical: RocksDbCanonicalSecondary,
    wallet: RocksDbWalletSecondary,
}

/// The secondaries behind a published pair.
///
/// The pair's readers share these handles, and so does every storage read
/// in flight through them. Keeping them here lets the publisher take back
/// sole ownership once the pair and its reads drain, and only then catch the
/// same open secondaries up for a later candidate.
struct PublishedReaders {
    canonical: Arc<RocksDbCanonicalSecondary>,
    wallet: Arc<RocksDbWalletSecondary>,
}

impl PublishedReaders {
    fn new(candidate: SecondaryPairCandidate) -> Self {
        Self {
            canonical: Arc::new(candidate.canonical),
            wallet: Arc::new(candidate.wallet),
        }
    }

    /// Reports whether anything else shares either secondary: a pair a
    /// request still holds, or a storage read still in flight, which can
    /// outlive the request that started it.
    fn is_shared(&self) -> bool {
        Arc::strong_count(&self.canonical) > 1 || Arc::strong_count(&self.wallet) > 1
    }

    /// Returns both secondaries as a candidate, or `self` while anything
    /// else, such as a pair a request still holds, shares either of them.
    fn into_candidate(self) -> Result<SecondaryPairCandidate, Self> {
        match (
            Arc::try_unwrap(self.canonical),
            Arc::try_unwrap(self.wallet),
        ) {
            (Ok(canonical), Ok(wallet)) => Ok(SecondaryPairCandidate { canonical, wallet }),
            (canonical, wallet) => Err(Self {
                canonical: canonical.map_or_else(std::convert::identity, Arc::new),
                wallet: wallet.map_or_else(std::convert::identity, Arc::new),
            }),
        }
    }
}

/// How a generation came to hold its candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidatePreparation {
    /// Opened and cold-admitted from the primaries.
    Cold,
    /// Taken back from a drained published generation, still open, for
    /// convergence to catch up in place.
    Recycled,
}

impl CandidatePreparation {
    const fn label(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Recycled => "recycled",
        }
    }
}

/// Candidate preparations by kind, which tests read to prove that publication
/// recycles drained generations instead of opening new ones.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CandidatePreparationCounts {
    cold: usize,
    recycled: usize,
}

#[cfg(test)]
impl CandidatePreparationCounts {
    fn record(&mut self, preparation: CandidatePreparation) {
        match preparation {
            CandidatePreparation::Cold => self.cold = self.cold.saturating_add(1),
            CandidatePreparation::Recycled => self.recycled = self.recycled.saturating_add(1),
        }
    }
}

/// A pair released from the slot, kept until no request holds it.
struct RetiredPair {
    pair: Arc<WalletServingReadPair>,
    retired_at: Instant,
}

/// Weak ownership token used to prove a path has no active request readers.
struct GenerationLease<T: ?Sized> {
    pair: Weak<T>,
}

impl<T: ?Sized> GenerationLease<T> {
    fn new(pair: &Arc<T>) -> Self {
        Self {
            pair: Arc::downgrade(pair),
        }
    }

    fn is_reusable(&self) -> bool {
        self.pair.strong_count() == 0
    }
}

/// Narrow authenticated reader used only to bind a secondary candidate to the
/// canonical writer at the service boundary immediately before publication.
struct CanonicalWriterStatusClient {
    client: CanonicalControlClient<AuthenticatedChannel>,
}

impl CanonicalWriterStatusClient {
    async fn connect(
        endpoint: &str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<Self, WalletServingPairError> {
        let channel = connect_zinder_grpc(endpoint, bearer_token).await?;
        Ok(Self {
            client: CanonicalControlClient::new(channel),
        })
    }

    fn from_admitted(ingest_control: &AdmittedIngestControl) -> Self {
        Self {
            client: CanonicalControlClient::new(ingest_control.channel()),
        }
    }

    async fn fetch(&mut self) -> Result<CanonicalWriterStatusResponse, WalletServingPairError> {
        let started_at = Instant::now();
        let outcome = self
            .client
            .writer_status(wallet_ingest_control_request(
                CanonicalWriterStatusRequest {},
            ))
            .await
            .map(tonic::Response::into_inner)
            .map_err(WalletServingPairError::WriterStatusRpc);
        metrics::histogram!(
            "zinder_wallet_serving_writer_status_duration_seconds",
            "status" => if outcome.is_ok() { "ok" } else { "error" }
        )
        .record(started_at.elapsed());
        metrics::counter!(
            "zinder_wallet_serving_writer_status_total",
            "status" => if outcome.is_ok() { "ok" } else { "error" }
        )
        .increment(1);
        metrics::gauge!("zinder_wallet_serving_writer_status_available").set(if outcome.is_ok() {
            1.0
        } else {
            0.0
        });
        outcome
    }
}

/// Returns how a caught-up candidate converges: `Ok(())` when its two
/// secondaries prove one exact source, and otherwise how far apart they are.
fn candidate_convergence(
    candidate: &SecondaryPairCandidate,
) -> Result<Result<(), WalletServingConvergence>, WalletServingPairError> {
    let chain_epoch = match candidate.canonical.chain_epoch() {
        Ok(chain_epoch) => chain_epoch,
        Err(error) => return candidate_epoch_read_failure(error).map(Err),
    };
    let admission = CanonicalAdmission {
        network: candidate.canonical.network(),
        network_upgrade_schedule: candidate.canonical.network_upgrade_schedule(),
        event_fence: candidate.canonical.event_fence(),
        chain_epoch,
    };
    Ok(WalletServingReadPair::validate_admission(
        admission,
        candidate.wallet.network(),
        candidate.wallet.ready_evidence(),
    )
    .map(drop)
    .map_err(|error| classify_pair_admission(&error)))
}

/// Returns how a caught-up candidate whose canonical epoch read failed with
/// `error` converges.
///
/// A read that failed only because the writer's compaction deleted a table
/// file after the catch-up is the candidate's own storage failure, as it is
/// during the catch-up, so it fails the refresh with its typed source instead
/// of reading as a schema or fence mismatch.
fn candidate_epoch_read_failure(
    error: CanonicalStoreError,
) -> Result<WalletServingConvergence, WalletServingPairError> {
    if error.is_missing_table_file() {
        Err(WalletServingPairError::Canonical(error))
    } else {
        Ok(WalletServingConvergence::SchemaOrFenceMismatch)
    }
}

fn classify_pair_admission(error: &WalletServingAdmissionError) -> WalletServingConvergence {
    match error {
        WalletServingAdmissionError::WalletSourceMismatch { canonical, wallet } => {
            let canonical_event_sequence = canonical.source_position().event_sequence;
            let wallet_event_sequence = wallet.source_position().event_sequence;
            match canonical_event_sequence.cmp(&wallet_event_sequence) {
                std::cmp::Ordering::Less => WalletServingConvergence::ReplicaBehind,
                std::cmp::Ordering::Greater => WalletServingConvergence::ProjectionBehind,
                std::cmp::Ordering::Equal => WalletServingConvergence::SchemaOrFenceMismatch,
            }
        }
        WalletServingAdmissionError::NetworkMismatch { .. }
        | WalletServingAdmissionError::CanonicalFenceMismatch
        | WalletServingAdmissionError::NetworkUpgradeScheduleNotAdmitted => {
            WalletServingConvergence::SchemaOrFenceMismatch
        }
    }
}

/// Candidate generation driven through writer-fence convergence.
struct CandidatePairFenceProbe<'a> {
    publisher: &'a mut WalletServingPairPublisher,
    generation: usize,
    last_outcome: WalletServingConvergence,
}

impl CanonicalFenceProbe for CandidatePairFenceProbe<'_> {
    type Error = WalletServingPairError;

    async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, WalletServingPairError> {
        self.publisher.prepare_candidate(self.generation).await?;
        match self.publisher.catch_up_candidate(self.generation).await? {
            Ok(()) => {
                self.last_outcome = WalletServingConvergence::ReplicaBehind;
                let source = self.publisher.candidate_wallet_source(self.generation)?;
                Ok(Some(wallet_source_writer_fence(source)))
            }
            Err(outcome) => {
                self.last_outcome = outcome;
                record_pair_convergence(outcome);
                Ok(None)
            }
        }
    }

    async fn writer_status(
        &mut self,
    ) -> Result<CanonicalWriterStatusResponse, WalletServingPairError> {
        self.publisher.writer_status.fetch().await
    }

    fn attests_writer_schedule(&self, status: &CanonicalWriterStatusResponse) -> bool {
        self.publisher
            .candidate_network_upgrade_schedule(self.generation)
            .is_ok_and(|schedule| writer_attests_network_upgrade_schedule(status, schedule))
    }

    /// A halt or store damage the writer reports while the candidate trails
    /// refuses traffic on the published pair at once, and stays remembered
    /// through a refresh failure that ends convergence.
    fn observe_writer_status(&mut self, status: &CanonicalWriterStatusResponse) {
        let visible_height = self.publisher.published_visible_height();
        if let Some(refusal) = self
            .publisher
            .writer_attestation
            .observe(status, visible_height)
        {
            self.publisher.readiness.publish_pair_state(refusal);
        }
    }

    fn secondary_behind(&mut self, _lag: CanonicalFenceLag, _elapsed: Duration) {
        record_pair_convergence(WalletServingConvergence::ReplicaBehind);
    }
}

fn wallet_source_writer_fence(source: WalletCanonicalSourceIdentity) -> CanonicalWriterFence {
    let position = source.source_position();
    canonical_writer_fence_message(
        position.chain_epoch_id,
        position.event_sequence,
        position.tip,
        source.source_sequence_digest(),
    )
}

const fn writer_fence_divergence_error(
    divergence: CanonicalFenceDivergence,
) -> WalletServingPairError {
    match divergence {
        CanonicalFenceDivergence::MissingFence | CanonicalFenceDivergence::NetworkMismatch => {
            WalletServingPairError::WriterStatusInvalid
        }
        CanonicalFenceDivergence::FenceMismatch { .. } => {
            WalletServingPairError::WriterFenceMismatch
        }
    }
}

fn record_pair_convergence(outcome: WalletServingConvergence) {
    metrics::counter!(
        "zinder_wallet_serving_pair_publisher_convergence_total",
        "outcome" => outcome.label()
    )
    .increment(1);
}

fn record_replica_lag(lag_chain_epochs: u64) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "The metric is a bounded operational lag signal; readiness compares the original integer."
    )]
    let lag = lag_chain_epochs as f64;
    metrics::gauge!("zinder_wallet_serving_pair_publisher_replica_lag_chain_epochs").set(lag);
}

/// Returns whether a refresh failure leaves the published pair exactly as the
/// writer last attested it.
///
/// Only lag, control-plane transport failures, and a candidate that opened,
/// caught up, or checked its admission evidence while the writer's or the
/// projector's compaction deleted a table file it had just listed qualify:
/// they describe the candidate generation or the status RPC, never the
/// published pair. Every failure that impugns the pair itself, its schema, or
/// the slot holding it is excluded, so those keep failing closed the moment
/// they occur.
fn refresh_failure_retains_attested_pair(error: &WalletServingPairError) -> bool {
    matches!(
        error,
        WalletServingPairError::WriterStatusConnect(_)
            | WalletServingPairError::WriterStatusRpc(_)
            | WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::ReplicaBehind
                    | WalletServingConvergence::ProjectionBehind,
            }
    ) || matches!(error, WalletServingPairError::Canonical(source) if source.is_missing_table_file())
        || matches!(error, WalletServingPairError::Wallet(source) if source.is_missing_table_file())
}

/// Returns whether a refresh failure is routine lag that a later refresh is
/// expected to clear.
///
/// The projector applies each block after the canonical writer commits it, so
/// a candidate regularly catches its two secondaries up to different epochs
/// while blocks arrive. Such a failure is routine while the writer lag stays
/// within `secondary_replica_lag_threshold_chain_epochs` and the published pair
/// still admits traffic.
fn refresh_failure_is_routine_lag(
    error: &WalletServingPairError,
    lag_chain_epochs: u64,
    lag_threshold_chain_epochs: u64,
    still_admitted: bool,
) -> bool {
    still_admitted
        && lag_chain_epochs <= lag_threshold_chain_epochs
        && matches!(
            error,
            WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::ReplicaBehind
                    | WalletServingConvergence::ProjectionBehind,
            }
        )
}

/// Returns the fail-closed readiness cause for a refresh error. `None` is
/// reserved for the convergence-lag outcomes, which the caller reports as
/// replica lag rather than as a storage failure.
fn refresh_failure_not_ready_cause(error: &WalletServingPairError) -> Option<ReadinessCause> {
    match error {
        WalletServingPairError::WriterStatusConnect(_)
        | WalletServingPairError::WriterStatusRpc(_) => {
            Some(ReadinessCause::WriterStatusUnavailable)
        }
        WalletServingPairError::WriterStatusInvalid
        | WalletServingPairError::WriterFenceMismatch
        | WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::SchemaOrFenceMismatch,
        } => Some(ReadinessCause::SchemaMismatch),
        WalletServingPairError::ConvergenceTimedOut {
            last_outcome:
                WalletServingConvergence::ReplicaBehind | WalletServingConvergence::ProjectionBehind,
        } => None,
        WalletServingPairError::Canonical(_)
        | WalletServingPairError::Wallet(_)
        | WalletServingPairError::SecondaryGenerationDirectoryCreate { .. }
        | WalletServingPairError::CandidateTask(_)
        | WalletServingPairError::PairPublication(_)
        | WalletServingPairError::PairSlotUnavailable
        | WalletServingPairError::CandidateUnavailable { .. } => {
            Some(ReadinessCause::StorageUnavailable)
        }
    }
}

fn wallet_serving_pair_error_class(error: &WalletServingPairError) -> &'static str {
    match error {
        WalletServingPairError::Canonical(_) => "canonical_secondary",
        WalletServingPairError::Wallet(_) => "wallet_secondary",
        WalletServingPairError::SecondaryGenerationDirectoryCreate { .. } => {
            "generation_directory_create"
        }
        WalletServingPairError::WriterStatusConnect(_) => "writer_status_connect",
        WalletServingPairError::WriterStatusRpc(_) => "writer_status_rpc",
        WalletServingPairError::WriterStatusInvalid => "writer_status_invalid",
        WalletServingPairError::WriterFenceMismatch => "writer_fence_mismatch",
        WalletServingPairError::CandidateTask(_) => "candidate_task",
        WalletServingPairError::PairSlotUnavailable => "pair_slot_unavailable",
        WalletServingPairError::CandidateUnavailable { .. } => "candidate_unavailable",
        WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::ReplicaBehind,
        } => "replica_behind",
        WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::ProjectionBehind,
        } => "projection_behind",
        WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::SchemaOrFenceMismatch,
        } => "schema_or_fence_mismatch",
        WalletServingPairError::PairPublication(_) => "pair_publication",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        net::TcpStream,
        num::{NonZeroU32, NonZeroU64},
        path::{Path, PathBuf},
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use parking_lot::Mutex;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
    use tokio_util::sync::CancellationToken;
    use tonic::service::Interceptor as _;
    use tonic::{Code, Request, Response, Status, transport::Server};
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    };
    use tonic_types::StatusExt as _;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
        CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockFactsSequenceDigestVersion, CanonicalBlockReplayFormatVersion, ChainEpochId,
        ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontiers, ConsensusBranchId,
        Network, NetworkUpgradeActivation, NetworkUpgradeActivations, SerializedBytesDigest,
        TransparentAddressScriptHash, UnixTimestampMillis, encode_canonical_block_replay,
        wire::encode_zinder_native_chain_name,
    };
    use zinder_proto::{
        capabilities::{
            WALLET_ADDRESS_TRANSPARENT_HISTORY_V1, WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
            WALLET_EVENTS_CHAIN_V1, WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1,
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1, WALLET_READ_COMPACT_BLOCK_AT_V2,
            WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2, WALLET_READ_COMPACT_BLOCK_RANGE_V2,
            WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2,
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1, WALLET_READ_SERVER_INFO_V2,
            WALLET_READ_SETTLED_TIP_BLOCK_V1, WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
            WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1, WALLET_READ_TRANSACTION_BY_ID_V2,
            WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            WALLET_READ_VISIBLE_TIP_BLOCK_V1,
        },
        v1::{
            ingest::{
                AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
                CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse,
                CanonicalWriterFence, CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
                CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
                ReadmitCanonicalOwnerCheckpointRequest,
                ReleaseCanonicalProjectionBuildLeaseRequest,
                ReleaseCanonicalProjectionBuildLeaseResponse,
                RenewCanonicalProjectionBuildLeaseRequest,
                canonical_control_server::{CanonicalControl, CanonicalControlServer},
                ingest_control_server::IngestControlServer,
            },
            ops::ErrorReason,
            wallet::{self, wallet_query_client::WalletQueryClient},
        },
    };
    use zinder_source::{
        NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError,
        UpstreamHealthSnapshot,
    };
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence,
        CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalReorgPolicy,
        CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreRequirements,
        CanonicalStoreWorkload, EventStreamStartPosition, RawBlobRetention, ResourceGaugeThrottle,
        RocksDbCanonicalBuilder, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
        RocksDbResourceBudget, StreamCursorTokenV1, event_stream_start_message,
    };
    use zinder_wallet_projection::{WalletCanonicalSourceIdentity, WalletProjectionSourcePosition};
    use zinder_wallet_rocksdb::{
        RocksDbWalletBuildOptions, RocksDbWalletStore, build_wallet_from_canonical,
    };

    use super::{
        CandidatePreparationCounts, CanonicalFenceRelation, IngestControlObserver,
        RETAINED_PAIR_WINDOW, SECONDARY_GENERATION_COUNT, SecondaryGenerationState,
        WalletServingConvergence, WalletServingPairConfig, WalletServingPairError,
        WalletServingPairPublisher, WalletServingPairSlot, WalletServingReadiness,
        candidate_epoch_read_failure, classify_canonical_writer_fence, classify_pair_admission,
        error_with_sources, missing_table_file_reported, refresh_failure_is_routine_lag,
        refresh_failure_not_ready_cause, refresh_failure_retains_attested_pair,
        spawn_wallet_ingest_control_readiness_probe, spawn_wallet_node_readiness_probe,
        wallet_ingest_control_request, wallet_source_writer_fence,
    };
    use crate::{
        AdmittedIngestControl, NativeWalletEndpointCapabilities, RocksDbCanonicalReader,
        RocksDbWalletReader, WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter,
        WalletServingQuery,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixture,
        IngestControlFixtureService, LogCapture, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    fn degraded_inputs(runtime: &zinder_runtime::Readiness) -> Vec<zinder_runtime::ReadinessInput> {
        runtime
            .report()
            .degraded
            .into_iter()
            .map(|dependency| dependency.input)
            .collect()
    }

    #[test]
    fn wallet_serving_readiness_follows_the_pair_and_reports_dependencies_independently() {
        use zinder_runtime::ReadinessInput::{IngestControl, NodeSource};

        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        let visible_height = Some(42);
        let node_failure = zinder_runtime::ReadinessCause::NodeUnavailable(
            zinder_runtime::NodeUnavailableDetail::first_iteration(
                "node_unreachable",
                "test node outage",
            ),
        );
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::Starting
        ));

        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert!(runtime.report().is_ready);
        assert_eq!(degraded_inputs(&runtime), [NodeSource, IngestControl]);

        readiness.publish_node_source_cause(node_failure.clone());
        readiness
            .publish_ingest_control_cause(zinder_runtime::ReadinessCause::IngestControlUnavailable);
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, visible_height);
        assert_eq!(report.degraded[0].cause, node_failure);

        readiness.publish_pair_state(zinder_runtime::ReadinessState::replica_lagging(
            2,
            visible_height,
        ));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 2
            }
        ));
        assert_eq!(degraded_inputs(&runtime), [IngestControl]);

        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert!(runtime.report().is_ready);
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(runtime.report().is_ready);
        assert_eq!(
            degraded_inputs(&runtime),
            [] as [zinder_runtime::ReadinessInput; 0]
        );

        readiness.publish_node_source_cause(node_failure);
        readiness.publish_pair_state(zinder_runtime::ReadinessState::not_ready(
            zinder_runtime::ReadinessCause::StorageUnavailable,
        ));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::StorageUnavailable
        ));
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert!(runtime.report().is_ready);
        assert_eq!(degraded_inputs(&runtime), [NodeSource]);

        readiness.publish_shutting_down();
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ShuttingDown
        ));
    }

    #[test]
    fn concurrent_recovery_cannot_erase_another_dependency_degradation() {
        const ITERATIONS: usize = 2_048;

        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(42)));
        let iteration_start = Arc::new(Barrier::new(3));
        let iteration_complete = Arc::new(Barrier::new(3));

        std::thread::scope(|scope| {
            let node_readiness = readiness.clone();
            let node_start = Arc::clone(&iteration_start);
            let node_complete = Arc::clone(&iteration_complete);
            scope.spawn(move || {
                for _ in 0..ITERATIONS {
                    node_start.wait();
                    node_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
                    node_complete.wait();
                }
            });

            let ingest_readiness = readiness.clone();
            let ingest_start = Arc::clone(&iteration_start);
            let ingest_complete = Arc::clone(&iteration_complete);
            scope.spawn(move || {
                for _ in 0..ITERATIONS {
                    ingest_start.wait();
                    ingest_readiness.publish_ingest_control_cause(
                        zinder_runtime::ReadinessCause::IngestControlUnavailable,
                    );
                    ingest_complete.wait();
                }
            });

            for iteration in 0..ITERATIONS {
                readiness.publish_node_source_cause(
                    zinder_runtime::ReadinessCause::NodeUnavailable(
                        zinder_runtime::NodeUnavailableDetail::first_iteration(
                            "node_unreachable",
                            "test node outage",
                        ),
                    ),
                );
                readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
                iteration_start.wait();
                iteration_complete.wait();
                assert_eq!(
                    degraded_inputs(&runtime),
                    [zinder_runtime::ReadinessInput::IngestControl],
                    "concurrent node recovery erased ingest degradation at iteration {iteration}"
                );
            }
        });
    }

    #[tokio::test]
    async fn wallet_node_probe_degrades_and_recovers_without_changing_capabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::new()?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let capabilities = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(crate::OrderedBroadcaster::new(source.clone(), [])))
            .native_endpoint_capabilities()
            .clone();
        let admitted_capabilities = capabilities.clone();
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(crate::OrderedBroadcaster::new(source.clone(), [])),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());
        source.available.store(false, Ordering::SeqCst);
        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::NodeUnavailable(_),
                }]
            )
        })
        .await?;
        assert!(runtime.report().is_ready);
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        source.available.store(true, Ordering::SeqCst);
        source.synchronized.store(false, Ordering::SeqCst);
        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::UpstreamNotReady(_),
                }]
            )
        })
        .await?;
        assert!(runtime.report().is_ready);
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        source.synchronized.store(true, Ordering::SeqCst);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A query without a configured node serves storage only, so its node
    /// dependency reports ready and never degrades.
    #[tokio::test]
    async fn primary_outage_degrades_cold_reads_while_the_fallback_broadcasts()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = MutableHealthSource::new()?;
        let fallback = MutableHealthSource::new()?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let nodes = crate::OrderedBroadcaster::new(primary.clone(), [fallback.clone()]);
        nodes.verify(&slot).await?;
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let probe = super::WalletNodeProbe::new(nodes, slot, Duration::from_millis(5), true);
        let handle = super::spawn_wallet_node_probe(Some(probe), readiness, cancel.clone());

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        primary.available.store(false, Ordering::SeqCst);
        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;
        assert_eq!(fallback.broadcast_attempts.load(Ordering::SeqCst), 1);
        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::NodeUnavailable(_),
                }]
            )
        })
        .await?;
        let mut gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(gate.call(Request::new(())).is_ok());

        primary.available.store(true, Ordering::SeqCst);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        cancel.cancel();
        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_node_probe_reports_ready_without_a_configured_node()
    -> Result<(), Box<dyn std::error::Error>> {
        let (slot, _store) = regtest_serving_pair_slot()?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe::<MutableHealthSource>(
            None,
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| {
            report.is_ready && report.degraded.is_empty()
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(
            report.degraded,
            [] as [zinder_runtime::ReadinessDependency; 0]
        );

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A node that is down when the reader starts is verified once it
    /// recovers. Until then the node dependency is degraded, and a broadcast
    /// fails with `NODE_UNAVAILABLE` without sending the transaction.
    #[tokio::test]
    async fn wallet_node_probe_verifies_a_node_that_recovers_after_startup()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::new()?;
        source.available.store(false, Ordering::SeqCst);
        let (slot, _store) = regtest_serving_pair_slot()?;
        let node = crate::OrderedBroadcaster::new(source.clone(), []);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(node.clone()));
        assert!(
            query
                .native_endpoint_capabilities()
                .contains(zinder_proto::capabilities::WALLET_BROADCAST_TRANSACTION_V1),
            "a configured node advertises broadcast before its first contact"
        );
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(node.clone()),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::NodeUnavailable(_),
                }]
            )
        })
        .await?;
        assert!(runtime.report().is_ready);
        source.available.store(true, Ordering::SeqCst);
        source.broadcast_attempts.store(0, Ordering::SeqCst);

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(node.verified_capabilities().is_some());
        assert!(query.upstream_node_capabilities().is_some());
        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;
        assert_eq!(source.broadcast_attempts.load(Ordering::SeqCst), 1);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A primary endpoint that never answers leaves broadcast to the
    /// fallback.
    ///
    /// The probe verifies and observes the fallback, the node dependency
    /// reports ready, and a broadcast reaches only the fallback. The primary
    /// stays preferred, so it is used again once it is verified.
    #[tokio::test]
    async fn wallet_node_probe_verifies_a_fallback_while_the_primary_is_down()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = MutableHealthSource::new()?;
        primary.available.store(false, Ordering::SeqCst);
        let fallback = MutableHealthSource::new()?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let nodes = crate::OrderedBroadcaster::new(primary.clone(), [fallback.clone()]);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(nodes.clone()),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(query.upstream_node_capabilities().is_some());
        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;
        assert_eq!(primary.broadcast_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(fallback.broadcast_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            nodes.preferred_position(),
            0,
            "skipping an unverified primary does not move the preference"
        );

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A verified primary that goes down while broadcast is idle leaves the
    /// node dependency ready while a verified fallback answers.
    ///
    /// The probe moves the preference to the fallback, so the next broadcast
    /// and `ServerInfo.node` use it without waiting for a broadcast timeout.
    #[tokio::test]
    async fn wallet_node_probe_moves_past_a_verified_primary_that_goes_down()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = MutableHealthSource::new()?;
        let fallback = MutableHealthSource::new()?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let nodes = crate::OrderedBroadcaster::new(primary.clone(), [fallback.clone()]);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(nodes.clone()),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert_eq!(nodes.preferred_position(), 0);
        primary.available.store(false, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            while nodes.preferred_position() != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(query.upstream_node_capabilities().is_some());
        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;
        assert_eq!(primary.broadcast_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(fallback.broadcast_attempts.load(Ordering::SeqCst), 1);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A standby that never answers its verification delays neither the
    /// liveness observation of the verified endpoints nor the move past a
    /// verified one that goes down.
    #[tokio::test(start_paused = true)]
    async fn wallet_node_probe_moves_past_a_dead_endpoint_while_a_standby_hangs()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = MutableHealthSource::new()?;
        let standby = MutableHealthSource::new()?;
        standby.stalled.store(true, Ordering::SeqCst);
        let fallback = MutableHealthSource::new()?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let nodes =
            crate::OrderedBroadcaster::new(primary.clone(), [standby.clone(), fallback.clone()]);
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(nodes.clone()),
            slot,
            readiness,
            Duration::from_secs(1),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        primary.available.store(false, Ordering::SeqCst);
        wait_until(|| nodes.preferred_position() == 2).await?;
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert_eq!(standby.capability_probes.load(Ordering::SeqCst), 1);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// An endpoint left verified against an earlier basis is checked again
    /// every two seconds while a standby never answers.
    ///
    /// Once the endpoint recovers from an outage that began as the tip
    /// crossed an activation, it is verified against the new basis and keeps
    /// receiving broadcasts after the earlier verification's grace would have
    /// ended.
    #[tokio::test(start_paused = true)]
    async fn wallet_node_probe_reverifies_a_recovering_endpoint_while_a_standby_hangs()
    -> Result<(), Box<dyn std::error::Error>> {
        let primary = MutableHealthSource::new()?;
        let standby = MutableHealthSource::new()?;
        standby.stalled.store(true, Ordering::SeqCst);
        // NU6.1 and NU6.2 activate at height 3, which a broadcast targets
        // once the tip reaches height 2.
        let (below, _below_store) = regtest_serving_pair(1)?;
        let (crossed, _crossed_store) = regtest_serving_pair(2)?;
        let slot = WalletServingPairSlot::new(below);
        let nodes = crate::OrderedBroadcaster::new(primary.clone(), [standby.clone()]);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        let readiness = WalletServingReadiness::with_node_and_ingest_control(
            zinder_runtime::Readiness::default(),
        );
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(nodes.clone()),
            slot.clone(),
            readiness,
            Duration::from_secs(30),
            cancel.clone(),
        );
        wait_until(|| nodes.verified_capabilities().is_some()).await?;

        primary.available.store(false, Ordering::SeqCst);
        let _released = slot.publish(crossed, tokio::time::Instant::now());
        wait_until(|| nodes.awaits_reverification(0)).await?;
        tokio::time::sleep(Duration::from_secs(20)).await;
        primary.available.store(true, Ordering::SeqCst);
        wait_until(|| !nodes.awaits_reverification(0)).await?;
        tokio::time::sleep(Duration::from_mins(1)).await;

        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;
        assert_eq!(primary.broadcast_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(standby.capability_probes.load(Ordering::SeqCst), 1);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A tip that reaches an activation only the endpoint's table schedules
    /// ends the endpoint's verification, though the writer's schedule and the
    /// upgrade it activates at the broadcast height stay the same.
    ///
    /// A broadcast skips the endpoint at once, and the probe verifies it
    /// again without waiting for its poll interval and excludes it.
    #[tokio::test(start_paused = true)]
    async fn wallet_node_probe_excludes_an_endpoint_at_an_activation_only_it_schedules()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = MutableHealthSource::new()?;
        source.node_activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            sample_regtest_upgrade_activations()
                .activations()
                .iter()
                .map(|activation| NetworkUpgradeActivation {
                    activation_height: if activation.name == "NU6.3" {
                        BlockHeight::new(4)
                    } else {
                        activation.activation_height
                    },
                    ..activation.clone()
                })
                .collect(),
        )?;
        let (below, _below_store) = regtest_serving_pair(2)?;
        let (crossed, _crossed_store) = regtest_serving_pair(3)?;
        assert_eq!(
            crate::node_broadcast::VerificationBasis::of(&below),
            crate::node_broadcast::VerificationBasis::of(&crossed)
        );
        let slot = WalletServingPairSlot::new(below);
        let nodes = crate::OrderedBroadcaster::new(source.clone(), []);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(nodes.clone()),
            slot.clone(),
            readiness,
            Duration::from_hours(1),
            cancel.clone(),
        );
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await?;

        let _released = slot.publish(crossed, tokio::time::Instant::now());
        let refused = query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await;
        assert!(
            matches!(
                refused,
                Err(crate::QueryError::Node(SourceError::NodeUnavailable { .. }))
            ),
            "unexpected broadcast outcome: {refused:?}"
        );
        assert_eq!(source.broadcast_attempts.load(Ordering::SeqCst), 1);
        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::NodeUnavailable(detail),
                }] if detail.failure_class == "protocol_mismatch"
            )
        })
        .await?;
        assert_eq!(source.capability_probes.load(Ordering::SeqCst), 2);

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    /// A schedule revision starts a probe round at once, so the endpoint is
    /// verified against the new schedule without waiting for the poll
    /// interval.
    #[tokio::test(flavor = "multi_thread")]
    async fn wallet_node_probe_verifies_again_as_soon_as_the_schedule_is_revised()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let mut source = MutableHealthSource::new()?;
        source.node_activations = primaries.activations.clone();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(
            zinder_runtime::Readiness::default(),
        );
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(crate::OrderedBroadcaster::new(source.clone(), [])),
            slot,
            readiness,
            Duration::from_hours(1),
            cancel.clone(),
        );
        let wait_for_probes = async |count: usize| {
            tokio::time::timeout(Duration::from_secs(5), async {
                while source.capability_probes.load(Ordering::SeqCst) < count {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
        };

        wait_for_probes(1).await?;
        primaries.admit_schedule_revision()?;
        publisher.refresh_once().await?;
        wait_for_probes(2).await?;

        cancel.cancel();
        handle.await?;
        primaries.shutdown().await
    }

    /// A node whose activation table disagrees with the writer-admitted
    /// schedule is on another network, so it never receives a
    /// transaction.
    #[tokio::test]
    async fn wallet_node_probe_refuses_a_node_on_another_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = MutableHealthSource::new()?;
        source.node_activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            sample_regtest_upgrade_activations()
                .activations()
                .iter()
                .map(|activation| NetworkUpgradeActivation {
                    activation_height: BlockHeight::new(activation.activation_height.value() + 1),
                    ..activation.clone()
                })
                .collect(),
        )?;
        let (slot, _store) = regtest_serving_pair_slot()?;
        let node = crate::OrderedBroadcaster::new(source.clone(), []);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(node.clone()));
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(node.clone()),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| {
            matches!(
                report.degraded.as_slice(),
                [zinder_runtime::ReadinessDependency {
                    input: zinder_runtime::ReadinessInput::NodeSource,
                    cause: zinder_runtime::ReadinessCause::NodeUnavailable(detail),
                }] if detail.failure_class == "protocol_mismatch"
            )
        })
        .await?;
        let refused = query
            .broadcast_transaction(zinder_core::RawTransactionBytes::new(vec![0x05; 32]))
            .await;
        assert!(matches!(
            refused,
            Err(crate::QueryError::Node(SourceError::NodeUnavailable { .. }))
        ));
        assert_eq!(source.broadcast_attempts.load(Ordering::SeqCst), 0);
        assert!(query.upstream_node_capabilities().is_none());

        cancel.cancel();
        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_node_probe_stops_while_a_node_request_is_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::new()?;
        source.stalled.store(true, Ordering::SeqCst);
        let (slot, _store) = regtest_serving_pair_slot()?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            Some(crate::OrderedBroadcaster::new(source.clone(), [])),
            slot,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        source.stalled_request_started.notified().await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle).await??;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_ingest_control_probe_preserves_stale_snapshot_as_ready()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let stale_status = zinder_proto::status_for_reason(
            ErrorReason::ChainEpochPinUnavailable,
            "requested chain epoch is no longer available",
        );
        ingest_fixture.set_mempool_snapshot_error(Some(stale_status.clone()));
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let direct_ingest_control = admitted_ingest_control.clone();
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| {
            report.is_ready && report.degraded.is_empty()
        })
        .await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        let direct_status = direct_ingest_control
            .client()
            .mempool_snapshot(wallet_ingest_control_request(
                zinder_proto::v1::wallet::MempoolSnapshotRequest {
                    max_entries: 1,
                    from_cursor: Vec::new(),
                },
            ))
            .await
            .err()
            .ok_or("stale snapshot status was not forwarded")?;
        assert_eq!(direct_status.code(), Code::FailedPrecondition);
        let direct_details = direct_status.get_error_details();
        let error_info = direct_details
            .error_info()
            .ok_or("stale snapshot omitted ErrorInfo")?;
        assert_eq!(error_info.domain, zinder_proto::ZINDER_ERROR_DOMAIN);
        assert_eq!(
            error_info.reason,
            ErrorReason::ChainEpochPinUnavailable.as_str_name()
        );

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_ingest_control_probe_projects_a_parked_writer_as_a_typed_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture = IngestControlFixtureService::new(Network::ZcashRegtest);
        let writer_control = WriterControlFixture::spawn(
            ingest_fixture.clone(),
            MutableCanonicalControl::new(following_writer_status()),
        )
        .await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(&writer_control.endpoint, None, Network::ZcashRegtest)
                .await?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness.clone(),
            Duration::from_millis(5),
            cancel.clone(),
        );
        wait_for_report(&runtime, |report| {
            report.is_ready && report.degraded.is_empty()
        })
        .await?;

        ingest_fixture.set_reorg_window_exceeded(Some(
            zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            },
        ));
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(
                cause,
                zinder_runtime::ReadinessCause::ReorgWindowExceeded {
                    depth: 1_001,
                    configured: 1_000,
                    covered: Some(1_000),
                }
            )
        })
        .await?;
        // The pair cannot refresh against a parked writer; the halt stays the
        // projected cause instead of the refresh failure it explains.
        readiness.publish_pair_state(zinder_runtime::ReadinessState::not_ready(
            zinder_runtime::ReadinessCause::WriterStatusUnavailable,
        ));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ReorgWindowExceeded { .. }
        ));
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        let halted = traffic_gate
            .call(Request::new(()))
            .err()
            .ok_or("a parked writer must close the wallet traffic gate")?;
        assert_eq!(halted.code(), Code::FailedPrecondition);

        // The probe alone cannot clear the halt: it waits for the publisher,
        // which takes it over and clears it on a writer status that also
        // attests the served pair.
        ingest_fixture.set_reorg_window_exceeded(None);
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        wait_for_probe_observation(&readiness, |observed| {
            matches!(observed, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ReorgWindowExceeded { .. }
        ));
        readiness.adopt_ingest_control_halt(|halt| {
            zinder_runtime::ReadinessState::not_ready(zinder_runtime::ReadinessCause::from(halt))
        });
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;

        cancel.cancel();
        handle.await?;
        writer_control.shutdown().await
    }

    /// A writer halt the ingest-control probe observed outlasts every later
    /// probe observation.
    ///
    /// It keeps refusing traffic through a control outage and through a
    /// writer that follows again, even while the pair publisher, which has
    /// not seen the halt, admits traffic on its stale pair. Only the
    /// publisher's adoption moves it, into the pair admission, after which
    /// the dependency reports the probe's latest observation again.
    #[tokio::test]
    async fn wallet_ingest_control_probe_halt_outlasts_later_observations_until_adopted()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture = IngestControlFixtureService::new(Network::ZcashRegtest);
        let writer_control = WriterControlFixture::spawn(
            ingest_fixture.clone(),
            MutableCanonicalControl::new(following_writer_status()),
        )
        .await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(&writer_control.endpoint, None, Network::ZcashRegtest)
                .await?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::serving_pair_stale(
            0,
            1,
            Some(1),
        ));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let mut observer = IngestControlObserver::default();
        let is_halted = |runtime: &zinder_runtime::Readiness| {
            let report = runtime.report();
            !report.is_ready
                && matches!(
                    report.cause,
                    zinder_runtime::ReadinessCause::ReorgWindowExceeded { depth: 1_001, .. }
                )
        };

        ingest_fixture.set_reorg_window_exceeded(Some(
            zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            },
        ));
        readiness.publish_ingest_control_cause(observer.observe(&admitted_ingest_control).await);
        assert!(is_halted(&runtime), "{:?}", runtime.report());

        ingest_fixture.set_health_available(false);
        readiness.publish_ingest_control_cause(observer.observe(&admitted_ingest_control).await);
        assert!(
            is_halted(&runtime),
            "a control outage must not reopen traffic on a parked writer's chain: {:?}",
            runtime.report()
        );

        ingest_fixture.set_reorg_window_exceeded(None);
        ingest_fixture.set_health_available(true);
        readiness.publish_ingest_control_cause(observer.observe(&admitted_ingest_control).await);
        assert!(
            is_halted(&runtime),
            "a following writer seen only by the probe must not reopen traffic: {:?}",
            runtime.report()
        );

        readiness.adopt_ingest_control_halt(|halt| {
            zinder_runtime::ReadinessState::not_ready(zinder_runtime::ReadinessCause::from(halt))
        });
        let adopted = runtime.report();
        assert!(is_halted(&runtime), "{adopted:?}");
        assert!(adopted.degraded.is_empty(), "{adopted:?}");

        writer_control.shutdown().await
    }

    /// Snapshot refusals degrade the ingest-control dependency by what they
    /// mean, and none drains the reader.
    #[tokio::test]
    async fn wallet_ingest_control_probe_classifies_snapshot_refusals_without_draining()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let stale_status = zinder_proto::status_for_reason(
            ErrorReason::ChainEpochPinUnavailable,
            "requested chain epoch is no longer available",
        );
        ingest_fixture.set_mempool_snapshot_error(Some(stale_status.clone()));
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        // A hydrating writer answers with a typed refusal: ingest control
        // answers, and the dependency reports the writer's node outage that
        // fails mempool calls, growing across observations.
        ingest_fixture.set_mempool_snapshot_error(Some(zinder_proto::status_for_reason(
            ErrorReason::NodeUnavailable,
            "live mempool index is hydrating from an upstream snapshot",
        )));
        wait_for_report(&runtime, |report| {
            report.degraded.iter().any(|dependency| {
                dependency.input == zinder_runtime::ReadinessInput::IngestControl
                    && matches!(
                        &dependency.cause,
                        zinder_runtime::ReadinessCause::NodeUnavailable(detail)
                            if detail.consecutive_failures > 1
                                && detail.failure_class == "stream_disconnected"
                    )
            })
        })
        .await?;
        assert!(runtime.report().is_ready);
        assert!(traffic_gate.call(Request::new(())).is_ok());

        // An untyped unavailable status is indistinguishable from a transport
        // failure.
        ingest_fixture.set_mempool_snapshot_error(Some(Status::unavailable(
            "fixture mempool hydration is unavailable",
        )));
        wait_for_report(&runtime, ingest_control_is_unavailable).await?;
        assert!(runtime.report().is_ready);
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_mempool_snapshot_error(None);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_mempool_snapshot_error(Some(stale_status));
        ingest_fixture.set_health_available(false);
        wait_for_report(&runtime, ingest_control_is_unavailable).await?;
        assert!(runtime.report().is_ready);
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_health_available(true);
        ingest_fixture.set_mempool_snapshot_error(None);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    /// A writer whose node is syncing reports `upstream_not_ready` in its
    /// `WriterStatus`. Native query relays that observation as its
    /// ingest-control dependency and keeps serving.
    #[tokio::test]
    async fn wallet_ingest_control_probe_relays_a_writer_upstream_not_ready_without_draining()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;

        ingest_fixture.set_upstream_not_ready(Some(
            zinder_proto::v1::ops::UpstreamNotReadyDetail {
                upstream_committed_height: Some(1),
                upstream_estimated_height: Some(40),
                upstream_verification_progress: Some(0.4),
                upstream_health_source: "zebra_ready_endpoint".to_owned(),
                upstream_health_reason: "syncing".to_owned(),
            },
        ));
        wait_for_report(&runtime, |report| !report.degraded.is_empty()).await?;
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(report.cause, zinder_runtime::ReadinessCause::Ready);
        let [
            zinder_runtime::ReadinessDependency {
                input: zinder_runtime::ReadinessInput::IngestControl,
                cause: zinder_runtime::ReadinessCause::UpstreamNotReady(detail),
            },
        ] = report.degraded.as_slice()
        else {
            return Err(format!("unexpected degraded dependencies {:?}", report.degraded).into());
        };
        assert_eq!(detail.upstream_estimated_height, Some(40));
        assert_eq!(detail.upstream_health.reason, "syncing");
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_upstream_not_ready(None);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_ingest_control_probe_degrades_and_recovers_without_changing_capabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let log_capture = LogCapture::install_for_target("zinder::query");
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            &admitted_ingest_control,
        );
        let admitted_capabilities = capabilities.clone();
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_health_available(false);
        wait_for_report(&runtime, ingest_control_is_unavailable).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        ingest_fixture.set_health_available(true);
        wait_for_report(&runtime, |report| report.degraded.is_empty()).await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        let health_events = log_capture.events();
        assert_eq!(
            health_events
                .iter()
                .filter(|event| {
                    event.field("event") == Some("ingest_control_health_unavailable")
                })
                .count(),
            1,
            "one outage transition must produce one warning"
        );
        assert!(health_events.iter().any(|event| {
            event.field("event") == Some("ingest_control_health_unavailable")
                && event.field("error_class") == Some("writer_status_rpc")
        }));
        assert_eq!(
            health_events
                .iter()
                .filter(|event| { event.field("event") == Some("ingest_control_health_recovered") })
                .count(),
            1,
            "one recovery transition must produce one info event"
        );
        Ok(())
    }

    fn ingest_control_is_unavailable(report: &zinder_runtime::ReadinessReport) -> bool {
        matches!(
            report.degraded.as_slice(),
            [zinder_runtime::ReadinessDependency {
                input: zinder_runtime::ReadinessInput::IngestControl,
                cause: zinder_runtime::ReadinessCause::IngestControlUnavailable,
            }]
        )
    }

    async fn wait_for_report(
        readiness: &zinder_runtime::Readiness,
        predicate: impl Fn(&zinder_runtime::ReadinessReport) -> bool + Send + Sync,
    ) -> Result<(), &'static str> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate(&readiness.report()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| "timed out waiting for readiness report")
    }

    async fn wait_for_probe_observation(
        readiness: &WalletServingReadiness,
        predicate: impl Fn(&zinder_runtime::ReadinessCause) -> bool + Send + Sync,
    ) -> Result<(), &'static str> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate(&readiness.ingest_control.lock().observed) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| "timed out waiting for the ingest-control probe observation")
    }

    async fn wait_for_readiness_cause(
        readiness: &zinder_runtime::Readiness,
        predicate: impl Fn(&zinder_runtime::ReadinessCause) -> bool + Send + Sync,
    ) -> Result<(), &'static str> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate(&readiness.report().cause) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| "timed out waiting for readiness cause")
    }

    /// A node outage leaves every stored read serving through the traffic gate.
    ///
    /// Only a call that needs the node fails, on its own with
    /// `NODE_UNAVAILABLE`, while `/readyz` lists the outage as a degraded
    /// dependency.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::too_many_lines,
        reason = "the outage and recovery read as one end-to-end sequence over one composition"
    )]
    async fn node_outage_fails_only_node_calls_and_keeps_stored_reads_serving()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::with_capabilities(NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TipId,
            NodeCapability::TreeState,
            NodeCapability::TransactionBroadcast,
        ])?);
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(1);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(crate::WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let visible_height = pair.canonical_fence().visible_tip().height.value();
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let slot = WalletServingPairSlot::new(pair);
        let node = crate::OrderedBroadcaster::new(source.clone(), []);
        let query = WalletServingQuery::from_admitted_native_serving_pair(
            slot.clone(),
            ingest_control.clone(),
        )
        .with_node_broadcast(Some(node.clone()));
        assert!(
            query
                .native_endpoint_capabilities()
                .contains(zinder_proto::capabilities::WALLET_BROADCAST_TRANSACTION_V1)
        );

        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(visible_height)));
        let cancel = CancellationToken::new();
        let node_probe = spawn_wallet_node_readiness_probe(
            Some(node),
            slot,
            readiness.clone(),
            Duration::from_millis(5),
            cancel.clone(),
        );
        let ingest_probe = spawn_wallet_ingest_control_readiness_probe(
            ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );
        let ops_port_reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let ops_address = ops_port_reservation.local_addr()?;
        drop(ops_port_reservation);
        let ops_handle = zinder_runtime::spawn_ops_endpoint(
            ops_address,
            zinder_runtime::OpsServer {
                service_name: "zinder-query-test",
                service_version: "0.0.0",
                network_name: "zcash-regtest",
                advertised_capabilities: query.native_endpoint_capabilities().shared_identifiers(),
            },
            runtime.clone(),
        )
        .await?;
        let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
        let wallet_endpoint = format!("http://{}", wallet_listener.local_addr()?);
        let grpc_adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
        let traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        let wallet_server = tokio::spawn({
            let shutdown = cancel.clone().cancelled_owned();
            async move {
                Server::builder()
                    .add_service(tonic::service::interceptor::InterceptedService::new(
                        grpc_adapter.into_server(),
                        traffic_gate,
                    ))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(wallet_listener), shutdown)
                    .await
            }
        });
        let mut wallet_client = WalletQueryClient::connect(wallet_endpoint).await?;
        wait_for_readyz(ops_address, |readyz| {
            readyz["cause"] == "ready" && readyz.get("degraded").is_none()
        })
        .await?;

        source.available.store(false, Ordering::SeqCst);
        let outage_readyz = wait_for_readyz(ops_address, |readyz| {
            readyz["degraded"][0]["input"] == "node_source"
        })
        .await?;
        assert_eq!(outage_readyz["status"], "ready");
        assert_eq!(outage_readyz["cause"], "ready");
        assert_eq!(
            outage_readyz["degraded"][0]["dependency_cause"]["node_unavailable"]["failure_class"],
            "node_unreachable"
        );
        assert_eq!(
            outage_readyz["degraded"].as_array().map(Vec::len),
            Some(1),
            "{outage_readyz}"
        );

        let outage_tip = wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner()
            .visible_tip_block
            .ok_or("the visible-tip response must contain a block during a node outage")?;
        assert_eq!(outage_tip.height, visible_height);
        let outage_block = wallet_client
            .compact_block(wallet::CompactBlockRequest {
                height: visible_height,
                at_epoch_id: None,
            })
            .await?
            .into_inner()
            .compact_block
            .ok_or("the compact-block response must contain a block during a node outage")?;
        assert_eq!(outage_block.height, visible_height);
        let broadcast_attempts = source.broadcast_attempts.load(Ordering::SeqCst);
        let refused_broadcast = wallet_client
            .broadcast_transaction(wallet::BroadcastTransactionRequest {
                raw_transaction: vec![0x05; 32],
            })
            .await
            .err()
            .ok_or("a broadcast must fail while the node is unavailable")?;
        assert_eq!(refused_broadcast.code(), Code::Unavailable);
        let refused_details = refused_broadcast.get_error_details();
        assert_eq!(
            refused_details
                .error_info()
                .map(|error_info| error_info.reason.as_str()),
            Some(ErrorReason::NodeUnavailable.as_str_name())
        );
        assert_eq!(
            source.broadcast_attempts.load(Ordering::SeqCst),
            broadcast_attempts + 1,
            "the broadcast must reach the node instead of failing on the probe's last observation"
        );

        source.available.store(true, Ordering::SeqCst);
        wait_for_readyz(ops_address, |readyz| readyz.get("degraded").is_none()).await?;
        wallet_client
            .broadcast_transaction(wallet::BroadcastTransactionRequest {
                raw_transaction: vec![0x05; 32],
            })
            .await?;

        cancel.cancel();
        node_probe.await?;
        ingest_probe.await?;
        wallet_server.await??;
        ops_handle.shutdown().await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[derive(Clone)]
    struct MutableHealthSource {
        available: Arc<AtomicBool>,
        synchronized: Arc<AtomicBool>,
        stalled: Arc<AtomicBool>,
        stalled_request_started: Arc<tokio::sync::Notify>,
        broadcast_attempts: Arc<std::sync::atomic::AtomicUsize>,
        capability_probes: Arc<std::sync::atomic::AtomicUsize>,
        capabilities: NodeCapabilities,
        node_activations: NetworkUpgradeActivations,
        holds_tip: bool,
    }

    impl MutableHealthSource {
        fn new() -> Result<Self, zinder_source::NodeCapabilitiesError> {
            Ok(Self::with_capabilities(NodeCapabilities::new([
                NodeCapability::TipId,
                NodeCapability::TransactionBroadcast,
                NodeCapability::OpenRpcDiscovery,
            ])?))
        }

        fn with_capabilities(capabilities: NodeCapabilities) -> Self {
            Self {
                available: Arc::new(AtomicBool::new(true)),
                synchronized: Arc::new(AtomicBool::new(true)),
                stalled: Arc::new(AtomicBool::new(false)),
                stalled_request_started: Arc::new(tokio::sync::Notify::new()),
                broadcast_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                capability_probes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                capabilities,
                node_activations: sample_regtest_upgrade_activations(),
                holds_tip: true,
            }
        }

        fn outage_error(&self) -> Option<SourceError> {
            (!self.available.load(Ordering::SeqCst)).then(|| SourceError::NodeUnavailable {
                reason: "synthetic transient node outage".to_owned(),
            })
        }
    }

    #[async_trait]
    impl zinder_source::BroadcastNodeProbe for MutableHealthSource {
        async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
            self.capability_probes.fetch_add(1, Ordering::SeqCst);
            if self.stalled.load(Ordering::SeqCst) {
                self.stalled_request_started.notify_one();
                std::future::pending::<()>().await;
            }
            self.outage_error().map_or(Ok(self.capabilities), Err)
        }

        async fn fetch_network_upgrade_activations(
            &self,
        ) -> Result<NetworkUpgradeActivations, SourceError> {
            self.outage_error()
                .map_or_else(|| Ok(self.node_activations.clone()), Err)
        }

        async fn holds_block(&self, _block: BlockId) -> Result<bool, SourceError> {
            self.outage_error().map_or(Ok(self.holds_tip), Err)
        }
    }

    #[async_trait]
    impl zinder_source::TransactionBroadcaster for MutableHealthSource {
        async fn broadcast_transaction(
            &self,
            _raw_transaction: zinder_core::RawTransactionBytes,
        ) -> Result<zinder_core::TransactionBroadcastOutcome, SourceError> {
            self.broadcast_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(outage) = self.outage_error() {
                return Err(outage);
            }
            Ok(zinder_core::TransactionBroadcastOutcome::Accepted(
                zinder_core::BroadcastAccepted {
                    transaction_id: zinder_core::TransactionId::from_bytes([0x05; 32]),
                },
            ))
        }
    }

    #[async_trait]
    impl NodeSource for MutableHealthSource {
        fn capabilities(&self) -> NodeCapabilities {
            self.capabilities
        }

        async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
            Err(SourceError::NodeUnavailable {
                reason: "health test does not fetch blocks".to_owned(),
            })
        }

        async fn tip_id(&self) -> Result<BlockId, SourceError> {
            if self.stalled.load(Ordering::SeqCst) {
                self.stalled_request_started.notify_one();
                std::future::pending::<()>().await;
            }
            if !self.available.load(Ordering::SeqCst) {
                return Err(SourceError::NodeUnavailable {
                    reason: "synthetic transient node outage".to_owned(),
                });
            }
            Ok(BlockId::new(
                BlockHeight::new(1),
                BlockHash::from_bytes([1; 32]),
            ))
        }

        async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
            if self.synchronized.load(Ordering::SeqCst) {
                Ok(UpstreamHealthSnapshot::ready(
                    "test_probe",
                    Some(1),
                    Some(1),
                    Some(1.0),
                ))
            } else {
                Ok(UpstreamHealthSnapshot::not_ready(
                    "test_probe",
                    "syncing",
                    Some(1),
                    Some(2),
                    Some(0.5),
                ))
            }
        }
    }

    #[test]
    fn exact_source_is_eligible_for_wallet_serving_pair_publisher_publication() {
        let source = source_identity(3, 0x33);

        assert_eq!(
            classify_canonical_writer_fence(
                &writer_status(source),
                Network::ZcashRegtest,
                &wallet_source_writer_fence(source),
            ),
            CanonicalFenceRelation::Matches
        );
    }

    #[test]
    fn mutated_writer_fence_is_rejected_before_wallet_serving_pair_publisher_publication() {
        let source = source_identity(3, 0x33);
        let mut status = writer_status(source);
        if let Some(fence) = status.fence.as_mut() {
            fence.canonical_sequence_digest[0] ^= 0xff;
        }

        assert!(matches!(
            classify_canonical_writer_fence(
                &status,
                Network::ZcashRegtest,
                &wallet_source_writer_fence(source),
            ),
            CanonicalFenceRelation::Diverged(_)
        ));
    }

    #[test]
    fn equal_cursor_but_mutated_pair_evidence_is_a_schema_or_fence_mismatch() {
        let canonical = source_identity(3, 0x33);
        let wallet = source_identity(3, 0x44);
        let error = crate::WalletServingAdmissionError::WalletSourceMismatch {
            canonical: Box::new(canonical),
            wallet: Box::new(wallet),
        };

        assert_eq!(
            classify_pair_admission(&error),
            WalletServingConvergence::SchemaOrFenceMismatch
        );
    }

    #[test]
    fn schema_or_fence_convergence_failure_is_not_ready() {
        assert_eq!(
            refresh_failure_not_ready_cause(&WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::SchemaOrFenceMismatch,
            }),
            Some(zinder_runtime::ReadinessCause::SchemaMismatch)
        );
    }

    #[test]
    fn only_bounded_lag_on_an_admitted_pair_is_routine() {
        let projection_behind = WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::ProjectionBehind,
        };
        let replica_behind = WalletServingPairError::ConvergenceTimedOut {
            last_outcome: WalletServingConvergence::ReplicaBehind,
        };
        assert!(refresh_failure_is_routine_lag(
            &projection_behind,
            1,
            4,
            true
        ));
        assert!(refresh_failure_is_routine_lag(&replica_behind, 4, 4, true));

        assert!(
            !refresh_failure_is_routine_lag(&projection_behind, 5, 4, true),
            "lag past the warning threshold is not routine"
        );
        assert!(
            !refresh_failure_is_routine_lag(&projection_behind, 1, 4, false),
            "a refresh failure that stops traffic is not routine"
        );
        for not_lag in [
            WalletServingPairError::WriterStatusRpc(Status::unavailable("writer status")),
            WalletServingPairError::WriterFenceMismatch,
            WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::SchemaOrFenceMismatch,
            },
        ] {
            assert!(
                !refresh_failure_is_routine_lag(&not_lag, 0, 4, true),
                "{not_lag} is not routine lag"
            );
        }
    }

    #[test]
    fn only_lag_and_control_plane_failures_keep_the_attested_pair_serving() {
        for transient in [
            WalletServingPairError::WriterStatusRpc(Status::unavailable("writer status")),
            WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::ReplicaBehind,
            },
            WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::ProjectionBehind,
            },
        ] {
            assert!(
                refresh_failure_retains_attested_pair(&transient),
                "{transient} must keep the attested pair serving"
            );
            assert!(
                !matches!(
                    refresh_failure_not_ready_cause(&transient),
                    Some(zinder_runtime::ReadinessCause::StorageUnavailable)
                ),
                "{transient} keeps serving, so it must not be classed as a storage failure"
            );
        }

        for fail_closed in [
            WalletServingPairError::WriterFenceMismatch,
            WalletServingPairError::WriterStatusInvalid,
            WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::SchemaOrFenceMismatch,
            },
            WalletServingPairError::PairSlotUnavailable,
            WalletServingPairError::CandidateUnavailable { generation: 0 },
            // An admission refusal fails closed even when it quotes a missing
            // table file's status.
            WalletServingPairError::Canonical(CanonicalStoreError::AdmissionRefused {
                path: PathBuf::from("/canonical"),
                reason: "store-control iteration failed: IO error: No such file or directory: \
                         While open a file for random read: /canonical/000199.sst: \
                         No such file or directory"
                    .to_owned(),
            }),
        ] {
            assert!(
                !refresh_failure_retains_attested_pair(&fail_closed),
                "{fail_closed} must never keep admitting traffic"
            );
            let cause = refresh_failure_not_ready_cause(&fail_closed);
            let Some(cause) = cause else {
                unreachable!("{fail_closed} must resolve to a fail-closed cause")
            };
            assert!(
                !cause.permits_traffic(),
                "{fail_closed} resolved to traffic-permitting {cause:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_adopts_the_schedule_from_ready_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary = build_lifecycle_canonical_primary_through(
            &canonical_primary_path,
            &activations,
            BlockHeight::new(1),
        )?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let (endpoint, cancel, server_task) = serve_canonical_control(control.clone()).await?;
        let config = WalletServingPairConfig {
            catchup_interval: Duration::from_millis(1),
            convergence_attempts: std::num::NonZeroU8::MIN,
            convergence_timeout: Duration::from_secs(10),
            ..wallet_serving_pair_config(
                canonical_primary_path.clone(),
                temporary.path().join("canonical-secondaries"),
                wallet_primary_path.clone(),
                temporary.path().join("wallet-secondaries"),
            )?
        };
        let bootstrap = tokio::spawn(async move {
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await
        });

        let (_publisher, slot) = bootstrap.await??;

        let pair = slot.capture();
        assert_eq!(
            pair.canonical()
                .network_upgrade_schedule()
                .map(super::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(pair.network_upgrade_activations(), &activations);
        drop(pair);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// Serves `control` on a loopback port until the returned token is
    /// cancelled.
    async fn serve_canonical_control(
        control: MutableCanonicalControl,
    ) -> Result<
        (
            String,
            CancellationToken,
            tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(CanonicalControlServer::new(control))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await
        });
        Ok((endpoint, cancel, server_task))
    }

    /// A writer restart that admits a new schedule revision leaves the fence
    /// unchanged, so the publisher must refresh the immutable active pair on
    /// schedule identity alone.
    #[tokio::test(flavor = "multi_thread")]
    async fn publisher_refreshes_the_pair_when_the_writer_admits_a_schedule_revision()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let mut canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;
        assert_eq!(
            slot.capture()
                .canonical()
                .network_upgrade_schedule()
                .map(super::CanonicalActivationSchedule::revision),
            Some(1)
        );
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        assert_eq!(query.network_upgrade_activations().await?, activations);

        let mut entries = activations.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(0x7719_0ad9),
            activation_height: BlockHeight::new(200),
            name: "NU7".to_owned(),
        });
        let extended = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
        let fence_before = canonical_primary.event_fence();
        canonical_primary.admit_network_upgrade_schedule(&extended)?;
        assert_eq!(canonical_primary.event_fence(), fence_before);
        control.set_writer_status(writer_status_for_store(&canonical_primary));

        publisher.refresh_once().await?;
        let pair = slot.capture();
        let schedule = pair
            .canonical()
            .network_upgrade_schedule()
            .ok_or("refreshed pair must carry the admitted schedule")?;
        assert_eq!(schedule.revision(), 2);
        assert_eq!(schedule.activations(), &extended);
        assert_eq!(pair.canonical_fence(), fence_before);
        assert_eq!(query.network_upgrade_activations().await?, extended);

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// A request that keeps a retired pair alive pins only that pair's
    /// generation. Publication continues through the spare slots and waits
    /// only once every unpublished slot is pinned.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the scenario pins every spare generation and then releases one"
    )]
    async fn a_pinned_retired_pair_does_not_stop_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let mut canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;
        // Each schedule revision makes the next refresh publish a new pair,
        // and every published pair stays captured like a long-running read.
        let mut admit_schedule_revision = |revision: u32| {
            let mut entries = activations.activations().to_vec();
            entries.push(NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: BlockHeight::new(200 + revision),
                name: "NU7".to_owned(),
            });
            NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)
                .map_err(Box::<dyn std::error::Error>::from)
                .and_then(|extended| {
                    canonical_primary
                        .admit_network_upgrade_schedule(&extended)
                        .map_err(Box::<dyn std::error::Error>::from)
                })
                .map(|()| control.set_writer_status(writer_status_for_store(&canonical_primary)))
        };
        let mut pinned_pairs = vec![slot.capture()];
        for revision in 1..u32::try_from(super::SECONDARY_GENERATION_COUNT)? {
            admit_schedule_revision(revision)?;
            publisher.refresh_once().await?;
            let refreshed_pair = slot.capture();
            assert!(
                !Arc::ptr_eq(
                    &refreshed_pair,
                    pinned_pairs.last().ok_or("no pinned pair")?
                ),
                "revision {revision} must publish while older pairs are pinned"
            );
            pinned_pairs.push(refreshed_pair);
        }

        let revision = u32::try_from(super::SECONDARY_GENERATION_COUNT)?;
        admit_schedule_revision(revision)?;
        publisher.refresh_once().await?;
        assert!(
            Arc::ptr_eq(
                &slot.capture(),
                pinned_pairs.last().ok_or("no pinned pair")?
            ),
            "with every unpublished slot pinned, the publisher waits"
        );

        drop(pinned_pairs.remove(0));
        publisher.refresh_once().await?;
        let released_slot_pair = slot.capture();
        assert!(!Arc::ptr_eq(
            &released_slot_pair,
            pinned_pairs.last().ok_or("no pinned pair")?
        ));
        assert_eq!(publisher.published_generation, Some(0));
        assert_eq!(
            released_slot_pair
                .canonical()
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            // The construction table is revision 1.
            Some(u64::from(revision) + 1)
        );

        drop(released_slot_pair);
        drop(pinned_pairs);
        drop(publisher);
        drop(slot);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// Unspent-output reads that clients abandon or stop consuming never stop
    /// publication.
    ///
    /// An abandoned read releases its pair at once. A read that is consumed
    /// while a newer pair is published finishes the complete set at the epoch
    /// it started on. A read whose client stops consuming keeps its pair only
    /// until the publisher revokes reads on it, and then ends with its epoch
    /// unavailable.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the scenario fills every spare generation twice, once with abandoned reads and once with stalled ones"
    )]
    async fn abandoned_and_stalled_unspent_output_reads_do_not_stop_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        // The writer starts without NU7, so each revision can append it at a
        // new height and still extend the table the store was built with.
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            sample_regtest_upgrade_activations()
                .activations()
                .iter()
                .filter(|activation| activation.name != "NU7")
                .cloned()
                .collect(),
        )?;
        let (chain, address) = address_with_unspent_outputs(
            3 * usize::from(crate::TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE.get()) + 1,
        )?;
        let mut writer =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
        let temporary = TempDir::new()?;
        let wallet_primary_path = temporary.path().join("wallet-primary");
        build_wallet_from_canonical(
            writer.canonical_primary()?,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control =
            MutableCanonicalControl::new(writer_status_for_store(writer.canonical_primary()?));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let config = WalletServingPairConfig {
            canonical_requirements: CanonicalStoreRequirements::for_blob_retention(
                writer.canonical_primary()?.raw_blob_retention(),
            ),
            ..wallet_serving_pair_config(
                writer.canonical_primary_path().to_path_buf(),
                temporary.path().join("canonical-secondaries"),
                wallet_primary_path,
                temporary.path().join("wallet-secondaries"),
            )?
        };
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let mut revision = 0_u32;
        let mut publish_next = async |publisher: &mut WalletServingPairPublisher| {
            revision += 1;
            let mut entries = activations.activations().to_vec();
            entries.push(NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: BlockHeight::new(700 + revision),
                name: "NU7".to_owned(),
            });
            writer.admit_network_upgrade_schedule(&NetworkUpgradeActivations::new(
                Network::ZcashRegtest,
                entries,
            )?)?;
            control.set_writer_status(writer_status_for_store(writer.canonical_primary()?));
            let before = slot.capture();
            publisher.refresh_once().await?;
            Ok::<_, Box<dyn std::error::Error>>(!Arc::ptr_eq(&before, &slot.capture()))
        };
        // Each read takes its first output and stops, so it holds the pair it
        // started on, and a publication follows each one.
        let mut abandoned = Vec::new();
        for _ in 1..super::SECONDARY_GENERATION_COUNT {
            let pair = Arc::downgrade(&slot.capture());
            let read = started_unspent_output_read(&query, address).await?;
            abandoned.push((pair, read));
            assert!(
                publish_next(&mut publisher).await?,
                "a read that holds an older pair must not stop publication"
            );
        }
        assert!(
            !publish_next(&mut publisher).await?,
            "with every spare generation held by a read, the publisher waits"
        );

        // Abandoning the reads mid-stream releases their pairs without any
        // further publisher action.
        let abandoned_pairs: Vec<_> = abandoned.into_iter().map(|(pair, _read)| pair).collect();
        wait_for_released_pairs(&abandoned_pairs).await?;
        assert!(
            publish_next(&mut publisher).await?,
            "publication resumes once abandoned reads release their pairs"
        );

        // A read that a newer publication overtakes mid-stream still delivers
        // the complete set at the epoch it started on.
        let replaced_pair = slot.capture();
        let replaced_epoch_id = replaced_pair.canonical_fence().chain_epoch_id();
        let replaced = Arc::downgrade(&replaced_pair);
        drop(replaced_pair);
        let mut overtaken = started_unspent_output_read(&query, address).await?;
        assert!(
            publish_next(&mut publisher).await?,
            "a read in progress must not stop publication"
        );
        assert!(!replaced.ptr_eq(&Arc::downgrade(&slot.capture())));
        let mut remaining_output_indexes = Vec::new();
        while let Some(output) = overtaken.next().await {
            remaining_output_indexes.push(output?.outpoint.output_index);
        }
        assert_eq!(overtaken.chain_epoch.id, replaced_epoch_id);
        assert_eq!(
            remaining_output_indexes,
            (1..=u32::try_from(
                3 * usize::from(crate::TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE.get())
            )?)
                .collect::<Vec<_>>()
        );
        drop(overtaken);
        wait_for_released_pairs(&[replaced]).await?;

        // Reads whose clients stop consuming fill the spare generations again.
        let mut stalled = Vec::new();
        for _ in 0..2 * super::SECONDARY_GENERATION_COUNT {
            let pair = slot.capture();
            let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
            stalled.push((
                Arc::downgrade(&pair),
                chain_epoch_id,
                started_unspent_output_read(&query, address).await?,
            ));
            drop(pair);
            if !publish_next(&mut publisher).await? {
                break;
            }
        }
        assert!(
            !publish_next(&mut publisher).await?,
            "stalled reads hold every spare generation"
        );

        // Once the retention window and the read limit pass, the publisher
        // revokes the stalled reads and publishes again.
        let now = tokio::time::Instant::now() + RETAINED_PAIR_WINDOW;
        publisher.release_expired_pairs(now);
        publisher
            .reap_retired_pairs(now + super::RELEASED_PAIR_READ_LIMIT)
            .await;
        let current_pair = Arc::downgrade(&slot.capture());
        let stalled_pairs: Vec<_> = stalled
            .iter()
            .map(|(pair, _, _)| pair.clone())
            .filter(|pair| !pair.ptr_eq(&current_pair))
            .collect();
        wait_for_released_pairs(&stalled_pairs).await?;
        assert!(
            publish_next(&mut publisher).await?,
            "publication resumes once revoked reads release their pairs"
        );
        for (pair, chain_epoch_id, mut read) in stalled {
            if pair.ptr_eq(&current_pair) {
                continue;
            }
            let failure = loop {
                match read.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => break error,
                    None => return Err("a revoked read ended as if complete".into()),
                }
            };
            assert!(
                matches!(
                    failure,
                    crate::QueryError::ChainEpochPinUnavailable { chain_epoch_id: failed }
                        if failed == chain_epoch_id
                ),
                "unexpected failure {failure:?}"
            );
        }

        drop(query);
        drop(publisher);
        drop(slot);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// Opens an unspent-output read for `address` and consumes its first
    /// output only, leaving the read mid-stream.
    async fn started_unspent_output_read(
        query: &WalletServingQuery<()>,
        address: TransparentAddressScriptHash,
    ) -> Result<crate::TransparentAddressUnspentOutputs, Box<dyn std::error::Error>> {
        let mut read = query
            .transparent_address_unspent_outputs(
                crate::TransparentAddressUnspentOutputsRequest {
                    address_script_hash: address,
                    start_height: BlockHeight::new(0),
                },
                None,
            )
            .await?;
        read.next()
            .await
            .ok_or("the read ended before its first output")??;
        Ok(read)
    }

    /// Waits until only the publisher or the slot still holds each pair.
    async fn wait_for_released_pairs(
        pairs: &[std::sync::Weak<crate::WalletServingReadPair>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while pairs.iter().any(|pair| pair.strong_count() > 1) {
            if tokio::time::Instant::now() > deadline {
                return Err("a read kept its pair".into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    /// A chain whose block 1 creates `output_count` outputs for one address.
    fn address_with_unspent_outputs(
        output_count: usize,
    ) -> Result<(ChainFixture, TransparentAddressScriptHash), Box<dyn std::error::Error>> {
        const SCRIPT_PUB_KEY: &[u8] = &[0x51];
        let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
        let block = chain
            .block_at(BlockHeight::new(1))
            .cloned()
            .ok_or("fixture must contain height 1")?;
        let address = TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY);
        let creator = zinder_core::TransactionId::from_bytes([0x10; 32]);
        for output_index in 0..u32::try_from(output_count)? {
            chain = chain.with_address_output_index(zinder_core::TransparentUnspentOutput::new(
                address,
                SCRIPT_PUB_KEY.to_vec(),
                zinder_core::TransparentOutPoint::new(creator, output_index),
                1_000,
                block.height,
                block.hash,
            ));
        }
        Ok((chain, address))
    }

    /// A publisher that started with the node's pre-upgrade table must keep
    /// refreshing after ingest admits the upgrade and the chain crosses its
    /// activation height.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the scenario keeps startup, writer admission, crossing the activation, and refresh in one causal sequence"
    )]
    async fn publisher_keeps_refreshing_after_the_chain_crosses_a_later_admitted_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let mut canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        let wallet_outcome = build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let initial_source = wallet_outcome.report.canonical_source_identity();
        let mut wallet_primary = wallet_outcome.store;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;

        // The node is upgraded and only the writer restarts: it admits the
        // new table, which activates NU7 at the next block, as revision 2.
        let mut entries = activations.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(0x7719_0ad9),
            activation_height: BlockHeight::new(2),
            name: "NU7".to_owned(),
        });
        let upgraded = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
        canonical_primary.admit_network_upgrade_schedule(&upgraded)?;
        let initial_fence = canonical_primary.event_fence();
        let initial_settled_tip = initial_fence.visible_tip();
        let (next_canonical_primary, crossing_fence) = canonical_primary.commit_live_append(
            CanonicalLiveAppend::new(
                initial_fence,
                lifecycle_build_block(
                    BlockHeight::new(2),
                    BlockHash::from_bytes([0x22; 32]),
                    initial_fence.visible_tip().hash,
                ),
                Vec::new(),
                initial_settled_tip,
                UnixTimestampMillis::new(1_750_000_000_001),
            ),
            &upgraded,
        )?;
        canonical_primary = next_canonical_primary;
        let wallet_reconcile_secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_primary_path,
            temporary
                .path()
                .join("wallet-reconcile-canonical-secondary"),
            &upgraded,
            zinder_store::CanonicalStoreRequirements::for_blob_retention(
                RawBlobRetention::Transactions,
            ),
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let crossing_source = reconcile_wallet_primary(
            &mut wallet_primary,
            &wallet_reconcile_secondary,
            initial_source,
            crossing_fence,
            initial_settled_tip,
        )?;
        drop(wallet_reconcile_secondary);
        control.set_writer_status(writer_status_for_store(&canonical_primary));

        publisher.refresh_once().await?;
        let pair = slot.capture();
        assert_eq!(pair.canonical_fence(), crossing_fence);
        assert_eq!(pair.wallet_source(), crossing_source);
        assert_eq!(
            pair.canonical()
                .network_upgrade_schedule()
                .map(super::CanonicalActivationSchedule::activations),
            Some(&upgraded)
        );

        cancel.cancel();
        server_task.await??;
        drop(wallet_primary);
        Ok(())
    }

    /// The configured ceiling, not the lag threshold alone, decides how long a
    /// reader may answer from a fence the writer has moved past.
    #[tokio::test(flavor = "multi_thread")]
    async fn over_threshold_lag_stops_admitting_traffic_at_the_configured_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_millis(200);
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });

        let readiness = zinder_runtime::Readiness::default();
        let mut config = wallet_serving_pair_config(
            canonical_primary_path.clone(),
            temporary.path().join("canonical-secondaries"),
            wallet_primary_path.clone(),
            temporary.path().join("wallet-secondaries"),
        )?;
        config.serving_pair_staleness_ceiling = staleness_ceiling;
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::pair_only(readiness.clone()),
                &endpoint,
                None,
            )
            .await?;

        let pair = slot.capture();
        let mut lagging_status = writer_status_for_store(&canonical_primary);
        let Some(lagging_fence) = lagging_status.fence.as_mut() else {
            return Err("writer status fixture must carry a fence".into());
        };
        lagging_fence.chain_epoch_id = lagging_fence.chain_epoch_id.saturating_add(64);
        lagging_fence.event_sequence = lagging_fence.event_sequence.saturating_add(64);

        publisher.update_active_readiness(&pair, &lagging_status)?;
        let stale = readiness.report();
        assert!(stale.is_ready);
        assert_eq!(stale.cause.metric_label(), "serving_pair_stale");

        tokio::time::sleep(staleness_ceiling).await;
        publisher.update_active_readiness(&pair, &lagging_status)?;
        let lagging = readiness.report();
        assert!(!lagging.is_ready);
        assert_eq!(lagging.cause.metric_label(), "replica_lagging");

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// The serving pair refuses a writer that has not confirmed its tip since
    /// it reopened.
    ///
    /// It stays unready with `canonical_tip_unconfirmed`, never
    /// `serving_pair_stale`, through a writer-status outage, keeps a
    /// remembered halt while the flag is set, and turns ready only once a
    /// status carries neither.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, outage, recovery, a park, and the restarted writer form one causal sequence"
    )]
    async fn pair_refuses_a_writer_with_an_unconfirmed_tip()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let unconfirmed_status = CanonicalWriterStatusResponse {
            tip_unconfirmed: true,
            ..writer_status_for_store(&canonical_primary)
        };
        let control = MutableCanonicalControl::new(unconfirmed_status.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let readiness = zinder_runtime::Readiness::default();
        let serving_readiness =
            WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                serving_readiness,
                &endpoint,
                None,
            )
            .await?;
        let unconfirmed = zinder_runtime::ReadinessCause::CanonicalTipUnconfirmed;
        assert_eq!(readiness.report().cause, unconfirmed);
        assert!(!readiness.report().is_ready);

        control.set_available(false);
        let outage = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("refresh must fail during a writer-status outage")?;
        publisher.record_refresh_failure(&outage);
        let during_outage = readiness.report();
        assert!(!during_outage.is_ready);
        assert_ne!(during_outage.cause.metric_label(), "serving_pair_stale");

        control.set_available(true);
        publisher.refresh_once().await?;
        assert_eq!(readiness.report().cause, unconfirmed);

        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        assert!(readiness.report().is_ready);

        // A park, then a restarted writer that has not yet rediscovered it,
        // keeps the remembered halt instead of the unconfirmed cause.
        let halt = zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        control.set_writer_status(CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(halt),
            tip_unconfirmed: true,
            ..writer_status_for_store(&canonical_primary)
        });
        publisher.refresh_once().await?;
        control.set_writer_status(unconfirmed_status);
        publisher.refresh_once().await?;
        assert!(matches!(
            readiness.report().cause,
            zinder_runtime::ReadinessCause::ReorgWindowExceeded { .. }
        ));
        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        assert!(readiness.report().is_ready);

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// The pair refuses at once while the writer reports store damage.
    ///
    /// It refuses on the next refresh, with no staleness allowance, and keeps
    /// refusing through a writer-status outage until a status reports no
    /// damage.
    #[tokio::test(flavor = "multi_thread")]
    async fn pair_refuses_at_once_while_the_writer_reports_store_damage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let (endpoint, cancel, server_task) = serve_canonical_control(control.clone()).await?;
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(readiness.clone()),
                &endpoint,
                None,
            )
            .await?;
        assert!(readiness.report().is_ready);

        control.set_writer_status(CanonicalWriterStatusResponse {
            canonical_store_damaged: Some(zinder_proto::wire::canonical_store_damaged_detail(
                Some(BlockHeight::new(1)),
                Some(zinder_core::ShieldedProtocol::Sapling),
            )),
            ..writer_status_for_store(&canonical_primary)
        });
        publisher.refresh_once().await?;
        let damaged = zinder_runtime::ReadinessCause::CanonicalStoreDamaged {
            height: Some(1),
            pool: Some("sapling"),
        };
        assert_eq!(readiness.report().cause, damaged);
        assert!(!readiness.report().is_ready);

        control.set_available(false);
        let outage = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("refresh must fail during a writer-status outage")?;
        publisher.record_refresh_failure(&outage);
        assert_eq!(readiness.report().cause, damaged);

        control.set_available(true);
        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        assert!(readiness.report().is_ready);

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// Store damage reported while a candidate trails the writer outlasts an
    /// outage.
    ///
    /// The damage refuses traffic at once, and a writer-status outage that
    /// then ends the refresh keeps refusing instead of admitting traffic
    /// under the staleness allowance.
    #[tokio::test(flavor = "multi_thread")]
    async fn damage_reported_while_a_candidate_trails_survives_a_later_outage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let (endpoint, cancel, server_task) = serve_canonical_control(control.clone()).await?;
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(readiness.clone()),
                &endpoint,
                None,
            )
            .await?;
        assert!(readiness.report().is_ready);

        // The first status shows the writer one epoch ahead, so the refresh
        // converges a candidate that trails it; the writer reports damage on
        // the next status and then stops answering.
        let ahead = writer_status_ahead_of(&canonical_primary)?;
        control.queue([
            Some(ahead.clone()),
            Some(CanonicalWriterStatusResponse {
                canonical_store_damaged: Some(zinder_proto::wire::canonical_store_damaged_detail(
                    Some(BlockHeight::new(1)),
                    None,
                )),
                ..ahead
            }),
            None,
        ]);
        let outage = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("the refresh must fail on the outage")?;
        publisher.record_refresh_failure(&outage);

        assert_eq!(
            readiness.report().cause,
            zinder_runtime::ReadinessCause::CanonicalStoreDamaged {
                height: Some(1),
                pool: None,
            }
        );
        assert!(!readiness.report().is_ready);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// Returns the status of a writer one chain epoch past `store`.
    fn writer_status_ahead_of(
        store: &RocksDbCanonicalStore,
    ) -> Result<CanonicalWriterStatusResponse, Box<dyn std::error::Error>> {
        let mut status = writer_status_for_store(store);
        let fence = status
            .fence
            .as_mut()
            .ok_or("a store's writer status carries its fence")?;
        fence.chain_epoch_id += 1;
        fence.event_sequence += 1;
        fence.visible_tip_height += 1;
        Ok(status)
    }

    /// The staleness ceiling holds when the catch-up interval is longer.
    ///
    /// The reader stops admitting traffic on an unattested pair once the
    /// ceiling passes, before its next scheduled refresh.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_staleness_ceiling_holds_with_a_longer_catch_up_interval()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_millis(200);
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let (endpoint, cancel, server_task) = serve_canonical_control(control.clone()).await?;
        let readiness = zinder_runtime::Readiness::default();
        let mut config = wallet_serving_pair_config(
            canonical_primary_path.clone(),
            temporary.path().join("canonical-secondaries"),
            wallet_primary_path.clone(),
            temporary.path().join("wallet-secondaries"),
        )?;
        config.catchup_interval = Duration::from_hours(1);
        config.serving_pair_staleness_ceiling = staleness_ceiling;
        let (publisher, _slot) = WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
            config,
            WalletServingReadiness::pair_only(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.set_available(false);
        let publisher_cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = publisher.spawn(publisher_cancel.clone(), drained.clone());
        tokio::time::sleep(staleness_ceiling * 4).await;
        let after_ceiling = readiness.report();
        publisher_cancel.cancel();
        drained.cancel();
        publisher_task.await??;

        assert!(
            !after_ceiling.is_ready,
            "an unattested pair must stop admitting traffic at the ceiling: {:?}",
            after_ceiling.cause
        );
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// A writer-status read that stalls cannot hold admission past the
    /// staleness ceiling.
    ///
    /// Each read waits until the wallet ingest-control bound ends it, so a
    /// read is always in flight when the ceiling passes; the pair stops
    /// admitting traffic at the ceiling instead of when that read ends.
    #[tokio::test]
    async fn a_stalled_writer_status_read_ends_admission_at_the_staleness_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_mins(1);
        let mut fixture =
            StalenessFixture::bootstrap(Duration::from_secs(1), staleness_ceiling).await?;
        fixture.control.stall();
        tokio::time::pause();
        let publisher_cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = fixture
            .publisher
            .take()
            .ok_or("the fixture holds its publisher")?
            .spawn(publisher_cancel.clone(), drained.clone());

        tokio::time::sleep(Duration::from_secs(59)).await;
        let before_ceiling = fixture.readiness.report();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_ceiling = fixture.readiness.report();
        publisher_cancel.cancel();
        drained.cancel();
        tokio::time::timeout(
            crate::WALLET_INGEST_CONTROL_REQUEST_TIMEOUT * 4,
            publisher_task,
        )
        .await???;
        tokio::time::resume();

        assert!(before_ceiling.is_ready, "{:?}", before_ceiling.cause);
        assert_eq!(
            after_ceiling.cause,
            zinder_runtime::ReadinessCause::WriterStatusUnavailable
        );
        fixture.shutdown().await
    }

    /// A publisher that starts its loop after the ceiling has passed ends
    /// admission at once instead of after its catch-up interval.
    #[tokio::test]
    async fn a_publisher_that_starts_after_the_staleness_ceiling_ends_admission_at_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_mins(1);
        let mut fixture =
            StalenessFixture::bootstrap(Duration::from_hours(1), staleness_ceiling).await?;
        fixture.control.stall();
        tokio::time::pause();
        tokio::time::advance(staleness_ceiling + Duration::from_secs(1)).await;
        assert!(fixture.readiness.report().is_ready);
        let publisher_cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = fixture
            .publisher
            .take()
            .ok_or("the fixture holds its publisher")?
            .spawn(publisher_cancel.clone(), drained.clone());

        tokio::time::sleep(Duration::from_millis(1)).await;
        let at_start = fixture.readiness.report();
        publisher_cancel.cancel();
        drained.cancel();
        tokio::time::timeout(
            crate::WALLET_INGEST_CONTROL_REQUEST_TIMEOUT * 4,
            publisher_task,
        )
        .await???;
        tokio::time::resume();

        assert_eq!(
            at_start.cause,
            zinder_runtime::ReadinessCause::WriterStatusUnavailable
        );
        fixture.shutdown().await
    }

    /// A writer that answers keeps a pair admitting traffic when the
    /// catch-up interval is longer than the staleness ceiling.
    ///
    /// Each answer takes 100 ms, so a refresh that started only once the
    /// ceiling passed would refuse traffic until the writer answered it. The
    /// test runs on the real clock because the paused clock can advance past
    /// a pending socket read.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_answering_writer_keeps_admission_past_the_staleness_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_secs(1);
        let mut fixture =
            StalenessFixture::bootstrap(Duration::from_hours(1), staleness_ceiling).await?;
        fixture.control.delay_answers(Duration::from_millis(100));
        let publisher_cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = fixture
            .publisher
            .take()
            .ok_or("the fixture holds its publisher")?
            .spawn(publisher_cancel.clone(), drained.clone());

        let mut refusals = Vec::new();
        let sampled_until = tokio::time::Instant::now() + staleness_ceiling * 3;
        while tokio::time::Instant::now() < sampled_until {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let report = fixture.readiness.report();
            if !report.is_ready {
                refusals.push(report.cause);
            }
        }
        publisher_cancel.cancel();
        drained.cancel();
        publisher_task.await??;

        assert!(
            refusals.is_empty(),
            "an answering writer must keep the pair admitted: {refusals:?}"
        );
        fixture.shutdown().await
    }

    /// A bootstrapped publisher over a writer-status server the test controls.
    struct StalenessFixture {
        _temporary: TempDir,
        _canonical_primary: RocksDbCanonicalStore,
        control: MutableCanonicalControl,
        readiness: zinder_runtime::Readiness,
        publisher: Option<WalletServingPairPublisher>,
        _slot: WalletServingPairSlot,
        server_cancel: CancellationToken,
        server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl StalenessFixture {
        async fn bootstrap(
            catchup_interval: Duration,
            staleness_ceiling: Duration,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let activations = lifecycle_upgrade_activations()?;
            let canonical_primary_path = temporary.path().join("canonical-primary");
            let wallet_primary_path = temporary.path().join("wallet-primary");
            let canonical_primary =
                build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
            build_wallet_from_canonical(
                &canonical_primary,
                &wallet_primary_path,
                RocksDbWalletBuildOptions {
                    supported_reorg_depth: 100,
                    ..RocksDbWalletBuildOptions::for_local_tests()
                },
            )?;
            let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
            let (endpoint, server_cancel, server_task) =
                serve_canonical_control(control.clone()).await?;
            let readiness = zinder_runtime::Readiness::default();
            let mut config = wallet_serving_pair_config(
                canonical_primary_path,
                temporary.path().join("canonical-secondaries"),
                wallet_primary_path,
                temporary.path().join("wallet-secondaries"),
            )?;
            config.catchup_interval = catchup_interval;
            config.serving_pair_staleness_ceiling = staleness_ceiling;
            let (publisher, slot) =
                WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                    config,
                    WalletServingReadiness::pair_only(readiness.clone()),
                    &endpoint,
                    None,
                )
                .await?;
            if !readiness.report().is_ready {
                return Err("bootstrap must admit traffic".into());
            }
            Ok(Self {
                _temporary: temporary,
                _canonical_primary: canonical_primary,
                control,
                readiness,
                publisher: Some(publisher),
                _slot: slot,
                server_cancel,
                server_task,
            })
        }

        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            self.server_cancel.cancel();
            self.server_task.abort();
            let _join_outcome = self.server_task.await;
            Ok(())
        }
    }

    /// The serving pair refuses a parked writer with its halt.
    ///
    /// A parked writer keeps attesting its frozen fence, so the publisher
    /// reads the halt from canonical writer status itself, with no
    /// ingest-control probe running: at bootstrap, on every refresh, and
    /// until the writer clears it.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, recovery, a second park, and the following outage form one causal halt sequence"
    )]
    async fn pair_refuses_a_parked_writer_with_the_typed_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let halt = zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let parked_status = CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(halt),
            ..writer_status_for_store(&canonical_primary)
        };
        let control = MutableCanonicalControl::new(parked_status.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });

        let readiness = zinder_runtime::Readiness::default();
        let serving_readiness =
            WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                )?,
                serving_readiness,
                &endpoint,
                None,
            )
            .await?;
        let expected_halt = zinder_runtime::ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let bootstrapped = readiness.report();
        assert_eq!(bootstrapped.cause, expected_halt);
        assert!(!bootstrapped.is_ready);
        assert_eq!(bootstrapped.cause, expected_halt);

        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        assert!(readiness.report().is_ready);

        control.set_writer_status(parked_status);
        publisher.refresh_once().await?;
        let parked = readiness.report();
        assert!(!parked.is_ready);
        assert_eq!(parked.cause, expected_halt);

        // A writer-status outage after the halt must keep refusing traffic,
        // never reopen it as a stale pair within the staleness ceiling.
        cancel.cancel();
        server_task.await??;
        let outage = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("refresh must fail once the writer status endpoint is gone")?;
        publisher.record_refresh_failure(&outage);
        let after_outage = readiness.report();
        assert!(!after_outage.is_ready);
        assert_eq!(after_outage.cause, expected_halt);
        Ok(())
    }

    /// A parked writer's halt outranks a fence mismatch on refresh: the halt is
    /// the cause an operator must resolve, and the pair keeps refusing traffic
    /// with it instead of failing the refresh as a divergence.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_reports_a_parked_writer_halt_before_a_fence_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let mut parked_mismatched_status = CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            ..writer_status_for_store(&canonical_primary)
        };
        let mismatched_fence = parked_mismatched_status
            .fence
            .as_mut()
            .ok_or("writer status fixture must carry a fence")?;
        mismatched_fence.visible_block_count =
            mismatched_fence.visible_block_count.saturating_add(1);
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path,
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path,
                    temporary.path().join("wallet-secondaries"),
                )?,
                WalletServingReadiness::pair_only(readiness.clone()),
                &endpoint,
                None,
            )
            .await?;
        assert!(readiness.report().is_ready);

        control.set_writer_status(parked_mismatched_status);
        publisher.refresh_once().await?;
        let parked = readiness.report();
        assert!(!parked.is_ready);
        assert!(matches!(
            parked.cause,
            zinder_runtime::ReadinessCause::ReorgWindowExceeded { depth: 1_001, .. }
        ));

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// A halt that only the ingest-control probe observed keeps refusing
    /// traffic until the publisher has validated its pair against a following
    /// writer.
    ///
    /// The publisher never read the halt from writer status, so its pair
    /// still admits traffic and its staleness allowance still runs. Once it
    /// adopts the probe's halt, a status without a fence keeps the halt, a
    /// writer far ahead refuses the pair as lagging instead of admitting it
    /// as stale, and only a status that attests the served pair reopens
    /// traffic.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the probe's halt, the probe's recovery, and the publisher's three validations form one causal sequence"
    )]
    async fn probe_observed_halt_refuses_traffic_until_the_publisher_validates_the_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let readiness = zinder_runtime::Readiness::default();
        let serving_readiness =
            WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path,
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path,
                    temporary.path().join("wallet-secondaries"),
                )?,
                serving_readiness.clone(),
                &endpoint,
                None,
            )
            .await?;
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        serving_readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(readiness.report().is_ready);
        let is_halted = |report: &zinder_runtime::ReadinessReport| {
            !report.is_ready
                && matches!(
                    report.cause,
                    zinder_runtime::ReadinessCause::ReorgWindowExceeded { depth: 1_001, .. }
                )
        };

        serving_readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::from(
            zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            },
        ));
        serving_readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(
            is_halted(&readiness.report()),
            "a probe that sees the writer recover must not reopen traffic on its own: {:?}",
            readiness.report()
        );

        control.set_writer_status(CanonicalWriterStatusResponse {
            fence: None,
            ..writer_status_for_store(&canonical_primary)
        });
        let missing_fence = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("a writer status without a fence must fail the refresh")?;
        publisher.record_refresh_failure(&missing_fence);
        let unvalidated = readiness.report();
        assert!(is_halted(&unvalidated), "{unvalidated:?}");
        assert!(unvalidated.degraded.is_empty(), "{unvalidated:?}");

        let pair = slot.capture();
        let mut far_ahead_status = writer_status_for_store(&canonical_primary);
        let far_ahead_fence = far_ahead_status
            .fence
            .as_mut()
            .ok_or("writer status fixture must carry a fence")?;
        far_ahead_fence.chain_epoch_id = far_ahead_fence.chain_epoch_id.saturating_add(64);
        far_ahead_fence.event_sequence = far_ahead_fence.event_sequence.saturating_add(64);
        publisher.update_active_readiness(&pair, &far_ahead_status)?;
        let lagging = readiness.report();
        assert!(!lagging.is_ready, "{lagging:?}");
        assert_eq!(lagging.cause.metric_label(), "replica_lagging");

        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        let recovered = readiness.report();
        assert!(recovered.is_ready, "{recovered:?}");
        assert!(recovered.degraded.is_empty(), "{recovered:?}");

        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_fails_closed_on_a_same_sequence_writer_fence_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let mut mismatched_status = writer_status_for_store(&canonical_primary);
        let mismatched_fence = mismatched_status
            .fence
            .as_mut()
            .ok_or("writer status fixture must carry a fence")?;
        mismatched_fence.visible_block_count =
            mismatched_fence.visible_block_count.saturating_add(1);
        let control = MutableCanonicalControl::new(mismatched_status);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(CanonicalControlServer::new(control))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await
        });

        let outcome = WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
            wallet_serving_pair_config(
                canonical_primary_path,
                temporary.path().join("canonical-secondaries"),
                wallet_primary_path,
                temporary.path().join("wallet-secondaries"),
            )?,
            WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
            &endpoint,
            None,
        )
        .await;

        assert!(matches!(
            outcome.as_ref().err(),
            Some(WalletServingPairError::WriterFenceMismatch)
        ));
        drop(outcome);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the cold process lifecycle test keeps primary advancement, secondary rotation, and fail-closed publication in one causal scenario"
    )]
    async fn cold_lifecycle_rotates_exact_secondaries_and_rejects_a_mutated_writer_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        let wallet_primary_path = temporary.path().join("wallet-primary");
        let canonical_secondary_root = temporary.path().join("canonical-secondaries");
        let wallet_secondary_root = temporary.path().join("wallet-secondaries");
        let mut canonical_primary =
            build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
        let wallet_outcome = build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 100,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        let initial_source = wallet_outcome.report.canonical_source_identity();
        let mut wallet_primary = wallet_outcome.store;
        let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let ingest_control_service = IngestControlFixtureService::new(Network::ZcashRegtest);
        let server_ingest_control_service = ingest_control_service.clone();
        let server_task = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .add_service(IngestControlServer::new(server_ingest_control_service))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            }
        });
        let admitted_ingest_control =
            AdmittedIngestControl::connect(&endpoint, None, Network::ZcashRegtest).await?;

        assert!(!canonical_secondary_root.exists());
        assert!(!wallet_secondary_root.exists());
        assert!(
            !canonical_secondary_root
                .join("generation-0")
                .join("canonical")
                .exists()
        );
        assert!(
            !wallet_secondary_root
                .join("generation-0")
                .join("wallet")
                .exists()
        );
        let readiness = zinder_runtime::Readiness::default();
        let serving_readiness =
            WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_with_admitted_ingest_control(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    canonical_secondary_root.clone(),
                    wallet_primary_path.clone(),
                    wallet_secondary_root.clone(),
                )?,
                serving_readiness.clone(),
                &admitted_ingest_control,
            )
            .await?;
        let old_pair = slot.capture();
        assert_eq!(old_pair.canonical_fence(), canonical_primary.event_fence());
        assert_eq!(old_pair.wallet_source(), initial_source);
        let query = WalletServingQuery::from_admitted_native_serving_pair(
            slot.clone(),
            admitted_ingest_control.clone(),
        );
        let visible_tip = query.visible_tip_block(None).await?;
        let settled_tip = query.settled_tip_block(None).await?;
        assert_eq!(
            BlockId::new(visible_tip.height, visible_tip.block_hash),
            old_pair.canonical_fence().visible_tip()
        );
        assert_eq!(
            BlockId::new(settled_tip.height, settled_tip.block_hash),
            old_pair.wallet_source().settled_tip()
        );
        let pin_outcome = query
            .visible_tip_block(Some(ChainEpochId::new(
                visible_tip.chain_epoch.id.value().saturating_add(1),
            )))
            .await;
        assert!(matches!(
            pin_outcome,
            Err(crate::QueryError::ChainEpochPinUnavailable { .. })
        ));
        let grpc_adapter =
            WalletQueryGrpcAdapter::new(query.clone(), WalletEndpointMetadata::default());
        let reflection_service =
            zinder_runtime::public_grpc_reflection(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)?;
        let advertised_capabilities = query.native_endpoint_capabilities().shared_identifiers();
        let ops_port_reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let ops_address = ops_port_reservation.local_addr()?;
        drop(ops_port_reservation);
        let ops_handle = zinder_runtime::spawn_ops_endpoint(
            ops_address,
            zinder_runtime::OpsServer {
                service_name: "zinder-query-test",
                service_version: "0.0.0",
                network_name: "zcash-regtest",
                advertised_capabilities,
            },
            readiness.clone(),
        )
        .await?;
        let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
        let wallet_endpoint = format!("http://{}", wallet_listener.local_addr()?);
        let wallet_cancel = CancellationToken::new();
        let wallet_server_cancel = wallet_cancel.clone();
        let query_readiness = zinder_runtime::TrafficReadinessInterceptor::new(readiness.clone());
        let reflection_readiness =
            zinder_runtime::TrafficReadinessInterceptor::new(readiness.clone());
        let wallet_server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(tonic::service::interceptor::InterceptedService::new(
                    grpc_adapter.into_server(),
                    query_readiness,
                ))
                .add_service(tonic::service::interceptor::InterceptedService::new(
                    reflection_service,
                    reflection_readiness,
                ))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(wallet_listener),
                    wallet_server_cancel.cancelled_owned(),
                )
                .await
        });
        let mut wallet_client = WalletQueryClient::connect(wallet_endpoint.clone()).await?;
        // The published pair alone admits traffic: the node and ingest-control
        // probes have not reported yet, so both dependencies are degraded.
        let (pending_status, pending_readyz) = fetch_operations_json(ops_address, "/readyz")?;
        assert_eq!(pending_status, 200);
        assert_eq!(pending_readyz["cause"], "ready");
        assert_eq!(
            pending_readyz["degraded"],
            serde_json::json!([
                {"input": "node_source", "dependency_cause": "starting"},
                {"input": "ingest_control", "dependency_cause": "starting"},
            ])
        );
        wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await?;
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let ingest_control_probe_cancel = CancellationToken::new();
        let ingest_control_probe = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            serving_readiness,
            Duration::from_millis(5),
            ingest_control_probe_cancel.clone(),
        );
        wait_for_readyz(ops_address, |readyz| {
            readyz["cause"] == "ready" && readyz.get("degraded").is_none()
        })
        .await?;
        let visible_response = wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner();
        let mut compact_blocks = wallet_client
            .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
                start_height: visible_tip.height.value(),
                end_height: visible_tip.height.value(),
                at_epoch_id: Some(visible_tip.chain_epoch.id.value()),
            })
            .await?
            .into_inner();
        let header = compact_blocks
            .next()
            .await
            .ok_or("native compact range must open with its chain-view header")??;
        let compact_block = compact_blocks
            .next()
            .await
            .ok_or("native compact range must return the visible block")??;
        assert!(compact_blocks.next().await.is_none());
        assert!(matches!(
            header.body,
            Some(wallet::compact_blocks_in_range_chunk::Body::Header(wallet::ChainView {
                chain_epoch: Some(ref epoch),
                ..
            })) if epoch.chain_epoch_id == visible_tip.chain_epoch.id.value()
        ));
        assert!(matches!(
            compact_block.body,
            Some(wallet::compact_blocks_in_range_chunk::Body::Item(ref block))
                if block.height == visible_tip.height.value()
        ));
        let settled_response = wallet_client
            .settled_tip_block(wallet::SettledTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner();
        let server_info = wallet_client
            .server_info(wallet::ServerInfoRequest {})
            .await?
            .into_inner()
            .info
            .ok_or("native server info response must contain a descriptor")?;
        let healthy_server_info = server_info.clone();
        assert_eq!(
            visible_response
                .visible_tip_block
                .ok_or("native visible-tip response must contain a block")?
                .height,
            visible_tip.height.value()
        );
        assert_eq!(
            settled_response
                .settled_tip_block
                .ok_or("native settled-tip response must contain a block")?
                .height,
            settled_tip.height.value()
        );
        let mut chain_events = wallet_client
            .chain_events(wallet::ChainEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            })
            .await?
            .into_inner();
        let first_chain_event = chain_events
            .next()
            .await
            .ok_or("native serving-pair event stream must return the baseline event")??;
        assert_eq!(first_chain_event.event_sequence, 1);
        assert_ne!(first_chain_event.cursor, [] as [u8; 0]);
        let advertised = server_info
            .common
            .ok_or("native server info must contain common metadata")?
            .capabilities;
        assert_eq!(
            advertised,
            query
                .native_endpoint_capabilities()
                .iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        );
        for required in [
            WALLET_READ_VISIBLE_TIP_BLOCK_V1,
            WALLET_READ_SETTLED_TIP_BLOCK_V1,
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
            WALLET_READ_COMPACT_BLOCK_AT_V2,
            WALLET_READ_COMPACT_BLOCK_RANGE_V2,
            WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2,
            WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
            WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
            WALLET_READ_SERVER_INFO_V2,
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
            WALLET_READ_TRANSACTION_BY_ID_V2,
            WALLET_EVENTS_CHAIN_V1,
            WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
            WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
            WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1,
        ] {
            assert!(
                advertised.iter().any(|capability| capability == required),
                "admitted serving-pair query omitted structural capability {required}"
            );
        }
        let partially_implemented = WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1;
        assert!(
            !advertised
                .iter()
                .any(|capability| capability == partially_implemented),
            "serving-pair query advertised an operation that is not production-admitted: \
             {partially_implemented}"
        );
        let (healthy_status, healthy_healthz) = fetch_operations_json(ops_address, "/healthz")?;
        assert_eq!(healthy_status, 200);
        let (ready_status, readyz) = fetch_operations_json(ops_address, "/readyz")?;
        assert_eq!(ready_status, 200);
        assert_eq!(readyz["cause"], "ready");

        assert!(readyz.get("degraded").is_none(), "{readyz}");
        // The fixture writer committed the served epoch at a fixed past time.
        let committed_at = old_pair.canonical().chain_epoch().created_at.value();
        let served_epoch_age = readyz["served_epoch_age_seconds"]
            .as_u64()
            .ok_or("a published pair must report its served epoch age")?;
        assert!(
            served_epoch_age.abs_diff(
                UnixTimestampMillis::now()
                    .value()
                    .saturating_sub(committed_at)
                    / 1_000
            ) <= 2,
            "{readyz}"
        );

        // An ingest-control outage degrades only the live-state methods that
        // call it; the published pair keeps serving every stored read.
        ingest_control_service.set_health_available(false);
        let outage_readyz = wait_for_readyz(ops_address, |readyz| {
            readyz["degraded"]
                == serde_json::json!([
                    {"input": "ingest_control", "dependency_cause": "ingest_control_unavailable"},
                ])
        })
        .await?;
        assert_eq!(outage_readyz["status"], "ready");
        assert_eq!(outage_readyz["cause"], "ready");
        let (outage_health_status, outage_healthz) =
            fetch_operations_json(ops_address, "/healthz")?;
        assert_eq!(outage_health_status, 200);
        assert_eq!(outage_healthz, healthy_healthz);

        let outage_tip = wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner()
            .visible_tip_block
            .ok_or("native visible-tip response must contain a block during the outage")?;
        assert_eq!(outage_tip.height, visible_tip.height.value());
        let mempool_outage = wallet_client
            .mempool_snapshot(wallet::MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            })
            .await
            .err()
            .ok_or("a mempool snapshot must fail while ingest control is unavailable")?;
        assert_eq!(mempool_outage.code(), Code::Unavailable);

        ingest_control_service.set_health_available(true);
        wait_for_readyz(ops_address, |readyz| readyz.get("degraded").is_none()).await?;
        let recovered_server_info = wallet_client
            .server_info(wallet::ServerInfoRequest {})
            .await?
            .into_inner()
            .info
            .ok_or("recovered native server info response must contain a descriptor")?;
        assert_eq!(recovered_server_info, healthy_server_info);
        let (recovered_health_status, recovered_healthz) =
            fetch_operations_json(ops_address, "/healthz")?;
        assert_eq!(recovered_health_status, 200);
        assert_eq!(recovered_healthz, healthy_healthz);

        let compact_at = wallet_client
            .compact_block(wallet::CompactBlockRequest {
                height: visible_tip.height.value(),
                at_epoch_id: Some(visible_tip.chain_epoch.id.value()),
            })
            .await?
            .into_inner()
            .compact_block
            .ok_or("native compact-at response must contain the requested block")?;
        assert!(
            compact_at.chain_metadata.is_some(),
            "Ironwood-capable compact encoding must carry required chain metadata"
        );
        let latest_checkpoint = wallet_client
            .latest_tree_state_checkpoint(wallet::LatestTreeStateCheckpointRequest {
                at_epoch_id: Some(visible_tip.chain_epoch.id.value()),
            })
            .await?
            .into_inner();
        assert!(latest_checkpoint.chain_view.is_some());
        assert!(latest_checkpoint.block_time_seconds.is_some());
        let ironwood_roots = wallet_client
            .subtree_roots(wallet::SubtreeRootsRequest {
                shielded_protocol: wallet::ShieldedProtocol::Ironwood as i32,
                start_index: 0,
                max_entries: 1,
                at_epoch_id: Some(visible_tip.chain_epoch.id.value()),
            })
            .await?
            .into_inner();
        assert!(ironwood_roots.chain_view.is_some());
        let address = wallet::AddressLookup {
            selector: Some(wallet::address_lookup::Selector::ScriptHash(vec![0x51; 32])),
        };
        let mut empty_outputs = wallet_client
            .transparent_address_unspent_outputs(wallet::TransparentAddressUnspentOutputsRequest {
                address: Some(address.clone()),
                start_height: 0,
                at_epoch_id: Some(visible_tip.chain_epoch.id.value()),
            })
            .await?
            .into_inner();
        let output_header = empty_outputs
            .message()
            .await?
            .ok_or("transparent-output stream omitted its epoch header")?;
        assert!(matches!(
            output_header.body,
            Some(wallet::transparent_unspent_outputs_chunk::Body::Header(_))
        ));
        assert!(empty_outputs.message().await?.is_none());
        let mut empty_history = wallet_client
            .transparent_address_tx_ids_in_range(wallet::TransparentAddressTxIdsInRangeRequest {
                address: Some(address),
                start_height: 0,
                end_height: visible_tip.height.value(),
                max_entries: 1,
                from_cursor: Vec::new(),
            })
            .await?
            .into_inner();
        let history_header = empty_history
            .message()
            .await?
            .ok_or("transparent-history stream omitted its epoch header")?;
        assert!(matches!(
            history_header.body,
            Some(wallet::transparent_address_tx_ids_chunk::Body::Header(_))
        ));
        assert!(empty_history.message().await?.is_none());
        let activation_response = wallet_client
            .network_upgrade_activations(wallet::NetworkUpgradeActivationsRequest {})
            .await?
            .into_inner();
        assert_ne!(
            activation_response.activations,
            [] as [zinder_proto::v1::wallet::NetworkUpgradeActivation; 0]
        );
        let reflection_channel = tonic::transport::Endpoint::new(wallet_endpoint)?
            .connect()
            .await?;
        let mut reflection_client = ServerReflectionClient::new(reflection_channel);
        let reflection_request = Request::new(tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        }));
        let mut reflection_stream = reflection_client
            .server_reflection_info(reflection_request)
            .await?
            .into_inner();
        let reflection_response = reflection_stream
            .next()
            .await
            .ok_or("reflection stream must return a service list")??
            .message_response
            .ok_or("reflection response must contain a service list")?;
        let MessageResponse::ListServicesResponse(services) = reflection_response else {
            return Err("reflection response must list services".into());
        };
        assert!(
            services
                .service
                .iter()
                .any(|service| service.name == "zinder.v1.wallet.WalletQuery")
        );
        drop(chain_events);
        let mut live_chain_events = wallet_client
            .chain_events(wallet::ChainEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::LiveTail,
                )),
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            })
            .await?
            .into_inner();
        assert!(
            canonical_secondary_root
                .join("generation-0")
                .join("canonical")
                .exists()
        );
        assert!(
            wallet_secondary_root
                .join("generation-0")
                .join("wallet")
                .exists()
        );

        let initial_fence = canonical_primary.event_fence();
        let initial_settled_tip = initial_fence.visible_tip();
        let (next_canonical_primary, append_fence) = canonical_primary.commit_live_append(
            CanonicalLiveAppend::new(
                initial_fence,
                lifecycle_build_block(
                    BlockHeight::new(2),
                    BlockHash::from_bytes([0x22; 32]),
                    initial_fence.visible_tip().hash,
                ),
                Vec::new(),
                initial_settled_tip,
                UnixTimestampMillis::new(1_750_000_000_001),
            ),
            &activations,
        )?;
        canonical_primary = next_canonical_primary;
        let wallet_reconcile_secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_primary_path,
            temporary
                .path()
                .join("wallet-reconcile-canonical-secondary"),
            &activations,
            zinder_store::CanonicalStoreRequirements::for_blob_retention(
                RawBlobRetention::Transactions,
            ),
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let updated_source = reconcile_wallet_primary(
            &mut wallet_primary,
            &wallet_reconcile_secondary,
            initial_source,
            append_fence,
            initial_settled_tip,
        )?;
        assert_eq!(
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet_primary.ready_evidence()),
            updated_source
        );
        drop(wallet_reconcile_secondary);

        control.set_writer_status(writer_status_for_store(&canonical_primary));
        publisher.refresh_once().await?;
        let refreshed_pair = slot.capture();
        assert!(!Arc::ptr_eq(&old_pair, &refreshed_pair));
        assert_eq!(refreshed_pair.canonical_fence(), append_fence);
        assert_eq!(refreshed_pair.wallet_source(), updated_source);
        let historical_block = query
            .block_id_by_selector(BlockSelector::Hash(initial_fence.visible_tip().hash), None)
            .await?;
        assert_eq!(historical_block.block_id, initial_fence.visible_tip());
        let unknown_block = query
            .block_id_by_selector(BlockSelector::Hash(BlockHash::from_bytes([0xff; 32])), None)
            .await;
        assert!(matches!(
            unknown_block,
            Err(crate::QueryError::BlockNotInBestChain)
        ));
        let retained_epoch_tip = query
            .visible_tip_block(Some(visible_tip.chain_epoch.id))
            .await?;
        assert_eq!(retained_epoch_tip.chain_epoch, visible_tip.chain_epoch);
        assert_eq!(retained_epoch_tip.height, visible_tip.height);
        let appended_event = tokio::time::timeout(Duration::from_secs(5), live_chain_events.next())
            .await?
            .ok_or("live-tail stream closed before the post-subscribe event")??;
        assert_eq!(appended_event.event_sequence, 2);
        let mut resumed_chain_events = wallet_client
            .chain_events(wallet::ChainEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::AfterCursor(StreamCursorTokenV1::from_bytes(
                        first_chain_event.cursor,
                    )),
                )),
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            })
            .await?
            .into_inner();
        let resumed_event =
            tokio::time::timeout(Duration::from_secs(5), resumed_chain_events.next())
                .await?
                .ok_or("after-cursor stream closed across the serving-pair swap")??;
        assert_eq!(resumed_event.event_sequence, 2);
        let family_result = query
            .resolve_chain_events_start(
                EventStreamStartPosition::AfterCursor(StreamCursorTokenV1::from_bytes(
                    appended_event.cursor.clone(),
                )),
                zinder_store::ChainEventStreamFamily::Settled,
            )
            .await;
        let Err(family_error) = family_result else {
            return Err("non-default family mismatch must fail closed".into());
        };
        assert!(matches!(
            family_error,
            crate::QueryError::ChainEventCursorInvalid { .. }
        ));
        assert_eq!(published_generation_is_reusable(&publisher, 0), Some(false));
        assert!(
            canonical_secondary_root
                .join("generation-1")
                .join("canonical")
                .exists()
        );
        assert!(
            wallet_secondary_root
                .join("generation-1")
                .join("wallet")
                .exists()
        );

        drop(old_pair);
        publisher
            .reap_retired_pairs(tokio::time::Instant::now())
            .await;
        assert_eq!(
            published_generation_is_reusable(&publisher, 0),
            Some(false),
            "a retained pair keeps its generation open for pinned reads"
        );
        publisher.release_expired_pairs(tokio::time::Instant::now() + RETAINED_PAIR_WINDOW);
        let expired_epoch_outcome = query
            .visible_tip_block(Some(visible_tip.chain_epoch.id))
            .await;
        assert!(matches!(
            expired_epoch_outcome,
            Err(crate::QueryError::ChainEpochPinUnavailable { .. })
        ));
        assert_eq!(published_generation_is_reusable(&publisher, 0), Some(false));
        publisher
            .reap_retired_pairs(tokio::time::Instant::now())
            .await;
        assert_eq!(published_generation_is_reusable(&publisher, 0), Some(true));
        drop(live_chain_events);
        drop(resumed_chain_events);
        wallet_cancel.cancel();
        wallet_server_task.await??;

        let mut mutated_status = writer_status_for_store(&canonical_primary);
        let writer_fence = mutated_status
            .fence
            .as_mut()
            .ok_or("writer status fixture must contain a fence")?;
        let digest_byte = writer_fence
            .canonical_sequence_digest
            .first_mut()
            .ok_or("writer status fixture must contain a digest")?;
        *digest_byte ^= 0xff;
        control.set_writer_status(mutated_status);
        let failure = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("mutated writer fence must fail refresh")?;
        assert!(matches!(
            failure,
            WalletServingPairError::WriterFenceMismatch
        ));
        publisher.record_refresh_failure(&failure);
        let pair_after_failure = slot.capture();
        assert!(Arc::ptr_eq(&refreshed_pair, &pair_after_failure));
        assert!(matches!(
            readiness.report().cause,
            zinder_runtime::ReadinessCause::SchemaMismatch
        ));

        ingest_control_probe_cancel.cancel();
        ingest_control_probe.await?;
        ops_handle.shutdown().await?;
        drop(pair_after_failure);
        drop(refreshed_pair);
        drop(publisher);
        drop(slot);
        drop(wallet_primary);
        drop(canonical_primary);
        cancel.cancel();
        server_task.await??;
        Ok(())
    }

    /// A block followed by its own refresh is published by its own pair.
    ///
    /// The blocks arrive at least a third of a retention window apart, so
    /// three publications span one window and the oldest retained pair ages
    /// out before the publication that needs its slot. Once each generation
    /// slot has been opened, the next block's candidate is a drained
    /// generation caught up in place, and the publisher opens no further
    /// secondaries.
    #[tokio::test(flavor = "multi_thread")]
    async fn steady_state_publication_recycles_drained_generations()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: 1,
                recycled: 0
            }
        );
        observe(&slot);
        // Each replaced pair keeps its generation for a minute, so the first
        // blocks open the remaining slots before any pair drains.
        let warm_up_blocks = SECONDARY_GENERATION_COUNT - 1;
        for block in 1..=3 * SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 2);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
            assert_eq!(
                slot.capture().canonical_fence(),
                fence,
                "block {block} must be published"
            );
            let expected = if block <= warm_up_blocks {
                CandidatePreparationCounts {
                    cold: 1 + block,
                    recycled: 0,
                }
            } else {
                CandidatePreparationCounts {
                    cold: SECONDARY_GENERATION_COUNT,
                    recycled: block - warm_up_blocks,
                }
            };
            assert_eq!(
                publisher.candidate_preparations, expected,
                "candidate preparations after block {block}"
            );
        }

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// A refresh publishes the writer's latest fence, so blocks committed
    /// before one refresh share one publication and the earlier height gets
    /// no pair of its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn blocks_committed_within_one_refresh_share_one_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let bootstrap_pair = slot.capture();
        let earlier = primaries.append_block()?;
        let latest = primaries.append_block()?;
        assert_ne!(earlier.chain_epoch_id(), latest.chain_epoch_id());

        publisher.refresh_once().await?;
        assert_eq!(slot.capture().canonical_fence(), latest);
        assert_eq!(
            slot.retained_pair_count(),
            1,
            "only the bootstrap pair was replaced"
        );
        assert!(Arc::ptr_eq(
            &slot.capture_at(Some(bootstrap_pair.canonical_fence().chain_epoch_id())),
            &bootstrap_pair
        ));
        assert_eq!(
            slot.capture_at(Some(earlier.chain_epoch_id()))
                .canonical_fence(),
            latest,
            "no pair serves the earlier block's epoch"
        );
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: 2,
                recycled: 0
            }
        );

        drop(bootstrap_pair);
        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    #[tokio::test]
    async fn cold_pair_replacement_refuses_a_different_admitted_stored_set()
    -> Result<(), Box<dyn std::error::Error>> {
        use zinder_proto::capabilities::WALLET_READ_FULL_BLOCK_AT_V1;
        use zinder_runtime::ReadinessCause;

        let activations = sample_regtest_upgrade_activations();
        let first_height = BlockHeight::new(1);
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
        let all = chain.clone().with_raw_blob_retention(RawBlobRetention::All);
        let transactions = chain.with_raw_blob_retention(RawBlobRetention::Transactions);
        let baseline = |chain| CanonicalBaseline::new(chain).through(first_height);
        let bootstrap = CanonicalWriterFixture::publish(&baseline(&all), &activations)?;
        let mut replacement =
            CanonicalWriterFixture::publish(&baseline(&transactions), &activations)?;
        let replacement_fence = replacement.append_live_block(&transactions)?;

        let paths = TempDir::new()?;
        let original_wallet = paths.path().join("original-wallet");
        let replacement_wallet = paths.path().join("replacement-wallet");
        let primary = bootstrap.canonical_primary()?;
        let replacement_primary = replacement.canonical_primary()?;
        let reorg = primary.reorg_policy();
        let options = RocksDbWalletBuildOptions {
            supported_reorg_depth: reorg.reorg_window_blocks(),
            ..RocksDbWalletBuildOptions::for_local_tests()
        };
        for (primary, path) in [
            (primary, &original_wallet),
            (replacement_primary, &replacement_wallet),
        ] {
            drop(build_wallet_from_canonical(primary, path, options)?.store);
        }

        let control = MutableCanonicalControl::new(writer_status_for_store(primary));
        let writer_control = WriterControlFixture::spawn(
            IngestControlFixtureService::new(Network::ZcashRegtest),
            control.clone(),
        )
        .await?;
        let mut config = wallet_serving_pair_config(
            bootstrap.canonical_primary_path().to_owned(),
            paths.path().join("canonical-secondaries"),
            original_wallet,
            paths.path().join("wallet-secondaries"),
        )?;
        config.canonical_reorg_policy = reorg;
        // A lag within the normal allowance must not reopen the refused pair.
        config.replica_lag_threshold_chain_epochs = u64::MAX;
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::pair_only(readiness.clone()),
                &writer_control.endpoint,
                None,
            )
            .await?;
        let original = slot.capture();
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        assert!(
            query
                .native_endpoint_capabilities()
                .contains(WALLET_READ_FULL_BLOCK_AT_V1)
        );
        query.full_block_at(first_height, None).await?;

        assert!(
            replacement_fence.chain_event_sequence()
                > original.canonical_fence().chain_event_sequence()
        );
        publisher.config.canonical_primary_path = replacement.canonical_primary_path().to_owned();
        publisher.config.wallet_primary_path = replacement_wallet;
        control.set_writer_status(writer_status_for_store(replacement_primary));
        publisher.refresh_and_record().await;
        assert_eq!(readiness.report().cause, ReadinessCause::StorageUnavailable);
        assert!(Arc::ptr_eq(&original, &slot.capture()));
        assert_eq!(publisher.candidate_preparations.cold, 2);

        assert_stored_set_backoff(&mut publisher, &readiness).await;
        assert!(Arc::ptr_eq(&original, &slot.capture()));
        query.full_block_at(first_height, None).await?;
        writer_control.shutdown().await
    }

    /// Refreshes inside the stored-set backoff open no candidate, and the
    /// first refresh after it opens one that is refused again.
    async fn assert_stored_set_backoff(
        publisher: &mut WalletServingPairPublisher,
        readiness: &zinder_runtime::Readiness,
    ) {
        use zinder_runtime::ReadinessCause;

        let cold = publisher.candidate_preparations.cold;
        for _ in 0..3 {
            publisher.refresh_and_record().await;
            assert_eq!(readiness.report().cause, ReadinessCause::StorageUnavailable);
            assert_eq!(publisher.candidate_preparations.cold, cold);
        }
        tokio::time::pause();
        tokio::time::advance(super::STORED_SET_RETRY_DELAY).await;
        tokio::time::resume();
        publisher.refresh_and_record().await;
        assert_eq!(readiness.report().cause, ReadinessCause::StorageUnavailable);
        assert_eq!(publisher.candidate_preparations.cold, cold + 1);
    }

    /// A burst of blocks keeps an epoch pinned before it answering until its
    /// retention window ends.
    ///
    /// Blocks are published until every spare slot holds a pair inside its
    /// window. Later blocks wait for a slot instead of releasing a pair early,
    /// while readiness keeps admitting traffic. Once the oldest window ends,
    /// one refresh publishes the writer's latest fence in the freed slot
    /// without opening a new one, and the blocks before it get no
    /// publication of their own.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the scenario keeps the burst, the coalesced wait, and the release of the pinned pair in one causal sequence"
    )]
    async fn a_block_burst_keeps_a_pinned_epoch_and_coalesces_later_blocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries = AdvancingPrimaries::start().await?;
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, slot) = primaries.bootstrap_publisher(readiness.clone()).await?;
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let pinned = query.visible_tip_block(None).await?.chain_epoch;

        for block in 1..SECONDARY_GENERATION_COUNT {
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(slot.capture().canonical_fence(), fence);
            assert_eq!(
                query.visible_tip_block(Some(pinned.id)).await?.chain_epoch,
                pinned,
                "burst block {block} must keep the pinned epoch"
            );
        }
        let last_published = slot.capture();
        let mut wait_start = None;
        let mut coalesced = Vec::new();
        for block in 1..=2 {
            coalesced.push(primaries.append_block()?);
            publisher.refresh_once().await?;
            assert!(
                Arc::ptr_eq(&slot.capture(), &last_published),
                "coalesced block {block} must wait for a slot"
            );
            let wait = publisher
                .generation_wait
                .as_ref()
                .ok_or("missing slot wait")?;
            assert_eq!(*wait_start.get_or_insert(wait.since), wait.since);
            assert!(readiness.report().is_ready);
            assert_eq!(
                query.visible_tip_block(Some(pinned.id)).await?.chain_epoch,
                pinned
            );
        }
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: SECONDARY_GENERATION_COUNT,
                recycled: 0
            }
        );
        assert_eq!(
            generation_waits(&rendered_metrics.render(), "retention_window"),
            2
        );

        primaries.control.set_available(false);
        publisher.refresh_and_record().await;
        assert!(publisher.generation_wait.is_none());
        primaries.control.set_available(true);
        publisher.refresh_once().await?;

        let replaced_at = slot
            .published
            .load()
            .retained
            .iter()
            .find(|retained| retained.pair.canonical_fence().chain_epoch_id() == pinned.id)
            .map(|retained| retained.replaced_at)
            .ok_or("the pinned pair must still be retained")?;
        publisher
            .release_expired_pairs(replaced_at + RETAINED_PAIR_WINDOW - Duration::from_millis(1));
        assert_eq!(slot.retained_pair_count(), SECONDARY_GENERATION_COUNT - 1);
        publisher.release_expired_pairs(replaced_at + RETAINED_PAIR_WINDOW);
        assert_eq!(slot.retained_pair_count(), SECONDARY_GENERATION_COUNT - 2);
        assert!(matches!(
            query.visible_tip_block(Some(pinned.id)).await,
            Err(crate::QueryError::ChainEpochPinUnavailable { .. })
        ));

        publisher.refresh_once().await?;
        assert!(publisher.generation_wait.is_none());
        let elapsed = rendered_metrics.render();
        for outcome in ["interrupted", "available"] {
            let sample = elapsed.lines().find(|line| {
                line.starts_with("zinder_wallet_serving_pair_publisher_generation_wait_duration_seconds_count{")
                    && line.contains("reason=\"retention_window\"")
                    && line.contains(&format!("outcome=\"{outcome}\""))
            }).ok_or("missing retention wait")?;
            assert_eq!(sample.split_whitespace().last(), Some("1"));
        }
        let latest = *coalesced.last().ok_or("the burst must coalesce blocks")?;
        assert_eq!(slot.capture().canonical_fence(), latest);
        assert_eq!(
            publisher.published_generation,
            Some(0),
            "the pinned pair's slot is caught up for the latest fence"
        );
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: SECONDARY_GENERATION_COUNT,
                recycled: 1
            }
        );
        for skipped in &coalesced[..coalesced.len() - 1] {
            assert_eq!(
                slot.capture_at(Some(skipped.chain_epoch_id()))
                    .canonical_fence(),
                latest,
                "a coalesced block has no pair of its own"
            );
        }

        drop(last_published);
        drop(query);
        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// Every generation reads through one block cache per store.
    ///
    /// A generation that reads the blocks an older one already loaded adds
    /// nothing to the cache. With every slot open, each cache reports the
    /// configured capacity once, while the memtable gauges sum the open
    /// generations whenever the memory-sample throttle admits. Once the
    /// publisher and every pair drop, both caches keep their blocks with
    /// nothing pinned, so no closed generation still holds an entry.
    #[tokio::test(flavor = "multi_thread")]
    async fn generations_read_through_one_block_cache_per_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries =
            AdvancingPrimaries::start_through(BlockHeight::new(SPLIT_TIP_HEIGHT)).await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let canonical_cache = publisher.store_resources.canonical.block_cache().clone();
        let wallet_cache = publisher.store_resources.wallet.block_cache().clone();
        assert_eq!(
            gauge_sample(
                &rendered_metrics.render(),
                "zinder_wallet_serving_pair_publisher_open_generations"
            ),
            Some(1.0)
        );

        let first = slot.capture();
        let before_first_read = canonical_cache.usage_bytes();
        let first_blocks = first
            .canonical()
            .compact_blocks_in_range(split_block_range())
            .await?;
        assert!(canonical_cache.usage_bytes() > before_first_read);

        primaries.append_block()?;
        publisher.refresh_and_record().await;
        let second = slot.capture();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(
            gauge_sample(
                &rendered_metrics.render(),
                "zinder_store_memtable_budget_bytes{store_role=\"canonical_secondary\"}"
            ),
            Some(f64::from(u32::try_from(
                RocksDbResourceBudget::for_local_tests().memtable_budget_bytes
            )?)),
            "the memory sample waits for its throttle interval after bootstrap"
        );
        publisher.instance_memory_throttle = ResourceGaugeThrottle::new(Duration::ZERO);
        let before_second_read = canonical_cache.usage_bytes();
        let second_blocks = second
            .canonical()
            .compact_blocks_in_range(split_block_range())
            .await?;
        assert_eq!(second_blocks, first_blocks);
        assert_eq!(
            canonical_cache.usage_bytes(),
            before_second_read,
            "a second generation reading the same blocks must not grow the cache"
        );

        for _ in 2..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_and_record().await;
        }
        assert_eq!(
            publisher.candidate_preparations.cold,
            SECONDARY_GENERATION_COUNT
        );
        let exposition = rendered_metrics.render();
        assert_eq!(
            gauge_sample(
                &exposition,
                "zinder_wallet_serving_pair_publisher_open_generations"
            ),
            Some(f64::from(u32::try_from(SECONDARY_GENERATION_COUNT)?))
        );
        for (store_role, block_cache) in [
            ("canonical_secondary", &canonical_cache),
            ("wallet_secondary", &wallet_cache),
        ] {
            assert_shared_cache_gauges(&exposition, store_role, block_cache)?;
        }

        drop((first, second, slot, publisher));
        for block_cache in [&canonical_cache, &wallet_cache] {
            assert_eq!(block_cache.pinned_usage_bytes(), 0);
            assert!(block_cache.usage_bytes() > 0);
        }
        primaries.shutdown().await
    }

    /// Asserts that `store_role`'s gauges report `block_cache` once, at the
    /// configured capacity, and sum the memtables of every open generation.
    fn assert_shared_cache_gauges(
        exposition: &str,
        store_role: &str,
        block_cache: &zinder_store::SharedBlockCache,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            exposition
                .lines()
                .any(|line| line.starts_with("zinder_store_rocksdb_ticker{")
                    && line.contains(&format!("store_role=\"{store_role}\""))
                    && line.contains("ticker=\"rocksdb.bytes.read\""))
        );
        let budget = RocksDbResourceBudget::for_local_tests();
        let open_generations = u32::try_from(SECONDARY_GENERATION_COUNT)?;
        let gauge = |name: &str| {
            gauge_sample(
                exposition,
                &format!("{name}{{store_role=\"{store_role}\"}}"),
            )
        };
        assert_eq!(
            gauge("zinder_store_block_cache_capacity_bytes"),
            Some(f64::from(u32::try_from(budget.block_cache_bytes)?)),
            "{store_role} reports one cache of the configured size"
        );
        assert!(
            block_cache.usage_bytes() > 0,
            "{store_role} cache must hold blocks"
        );
        assert_eq!(
            gauge("zinder_store_block_cache_usage_bytes"),
            Some(f64::from(u32::try_from(block_cache.usage_bytes())?))
        );
        assert!(gauge("zinder_store_block_cache_pinned_usage_bytes").is_some());
        assert_eq!(
            gauge("zinder_store_memtable_budget_bytes"),
            Some(f64::from(
                open_generations * u32::try_from(budget.memtable_budget_bytes)?
            )),
            "{store_role} memtable budget sums the open generations"
        );
        assert!(gauge("zinder_store_memtable_budget_usage_bytes").is_some());
        assert!(gauge("zinder_store_table_reader_memory_bytes").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn refresh_counter_includes_unchanged_fences_and_failed_attempts()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, _slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        publisher.refresh_and_record().await;
        primaries.control.set_available(false);
        publisher.refresh_and_record().await;
        let exposition = rendered_metrics.render();
        for (status, class) in [("ok", "none"), ("error", "writer_status_rpc")] {
            let series = format!(
                "zinder_wallet_serving_pair_publisher_refresh_total{{status=\"{status}\",error_class=\"{class}\"}}"
            );
            assert_eq!(gauge_sample(&exposition, &series), Some(1.0));
        }
        primaries.shutdown().await
    }

    /// Reads one unlabelled or fully labelled gauge series from a Prometheus
    /// exposition.
    fn gauge_sample(exposition: &str, series: &str) -> Option<f64> {
        exposition.lines().find_map(|line| {
            line.strip_prefix(series)?
                .strip_prefix(' ')?
                .trim()
                .parse::<f64>()
                .ok()
        })
    }

    /// Reads one `generation_wait_total` series from a Prometheus exposition.
    fn generation_waits(exposition: &str, reason: &str) -> u64 {
        let series = format!(
            "zinder_wallet_serving_pair_publisher_generation_wait_total{{reason=\"{reason}\"}} "
        );
        exposition
            .lines()
            .find_map(|line| line.strip_prefix(series.as_str()))
            .and_then(|count| count.trim().parse().ok())
            .unwrap_or(0)
    }

    /// A burst no request observes publishes every block at once.
    ///
    /// No pin can name a pair that no response exposed, so the publisher
    /// releases each replaced pair and reuses its generation instead of
    /// waiting out the retention window.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unobserved_burst_publishes_every_block_without_a_retention_wait()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;

        for block in 1..=3 * SECONDARY_GENERATION_COUNT {
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(
                slot.capture_ephemeral().canonical_fence(),
                fence,
                "burst block {block} must be published"
            );
            assert!(publisher.generation_wait.is_none());
            assert_eq!(slot.retained_pair_count(), 0);
        }
        assert_eq!(
            generation_waits(&rendered_metrics.render(), "retention_window"),
            0
        );

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// Tip polling between blocks, as `GetLatestBlock` and `GetLightdInfo`
    /// do, observes no pair: the burst still publishes every block at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_ephemeral_tip_poll_between_blocks_holds_no_retention_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        assert_eq!(
            query.served_tip_snapshot().await?.chain_epoch,
            slot.capture_ephemeral().chain_epoch()
        );

        for block in 1..=3 * SECONDARY_GENERATION_COUNT {
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            let snapshot = query.served_tip_snapshot().await?;
            assert_eq!(
                snapshot.chain_epoch.id,
                fence.chain_epoch_id(),
                "burst block {block} must be published"
            );
            query.network_upgrade_activations().await?;
            assert!(publisher.generation_wait.is_none());
            assert_eq!(slot.retained_pair_count(), 0);
        }
        assert_eq!(
            generation_waits(&rendered_metrics.render(), "retention_window"),
            0
        );
        let snapshot = query.served_tip_snapshot().await?;
        let header = query
            .block_header_by_selector(
                crate::BlockSelector::Height(snapshot.chain_epoch.visible_tip_height),
                Some(snapshot.chain_epoch.id),
            )
            .await?;
        assert_eq!(
            snapshot.block_time_unix_seconds,
            header.block_header.block_time
        );

        drop(query);
        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// A burst whose every pair a request observed keeps each replaced pair
    /// for the whole window, so the block after the spare slots fill waits.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_observed_burst_waits_for_the_retention_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        observe(&slot);
        for _ in 1..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
        }
        assert_eq!(slot.retained_pair_count(), SECONDARY_GENERATION_COUNT - 1);
        let last_published = slot.capture_ephemeral();

        primaries.append_block()?;
        publisher.refresh_once().await?;
        assert!(Arc::ptr_eq(&slot.capture_ephemeral(), &last_published));
        assert!(publisher.generation_wait.is_some());
        assert_eq!(
            generation_waits(&rendered_metrics.render(), "retention_window"),
            1
        );

        let oldest_replaced_at = slot
            .published
            .load()
            .retained
            .iter()
            .map(|retained| retained.replaced_at)
            .min()
            .ok_or("the burst must retain its replaced pairs")?;
        publisher.release_expired_pairs(oldest_replaced_at + RETAINED_PAIR_WINDOW);
        publisher.refresh_once().await?;
        assert!(!Arc::ptr_eq(&slot.capture_ephemeral(), &last_published));
        assert!(publisher.generation_wait.is_none());

        drop(last_published);
        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// The publisher opens a slot it has not used only when every opened slot
    /// still holds a retained pair, so the block spacing, not the start,
    /// decides when the last slots open.
    ///
    /// Blocks more than one retention window apart alternate between two
    /// slots. The first block within a window of the previous publication
    /// opens the third, and a third publication within one window opens the
    /// fourth. After that, each block a third of a window after the one before
    /// reuses the slot whose pair aged out.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_block_burst_opens_the_remaining_slots_after_spaced_blocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let counts = |cold, recycled| CandidatePreparationCounts { cold, recycled };

        let spaced_blocks = 2 * SECONDARY_GENERATION_COUNT;
        for block in 1..=spaced_blocks {
            publisher.release_expired_pairs(tokio::time::Instant::now() + RETAINED_PAIR_WINDOW);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(slot.capture().canonical_fence(), fence);
            assert_eq!(
                publisher.candidate_preparations,
                counts(2, block - 1),
                "spaced block {block} reuses one of the first two slots"
            );
        }
        let spaced_recycled = spaced_blocks - 1;

        for (block, opened) in [(1, 3), (2, SECONDARY_GENERATION_COUNT)] {
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(slot.capture().canonical_fence(), fence);
            assert_eq!(
                publisher.candidate_preparations,
                counts(opened, spaced_recycled),
                "burst block {block} opens slot {opened}"
            );
        }
        for block in 1..=SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 2);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(slot.capture().canonical_fence(), fence);
            assert_eq!(
                publisher.candidate_preparations,
                counts(SECONDARY_GENERATION_COUNT, spaced_recycled + block),
                "later block {block} reuses a drained slot"
            );
        }

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// Spaced blocks after a burst reuse every drained slot in turn.
    ///
    /// Once a burst has opened every slot, blocks spaced beyond the retention
    /// window keep every open generation within the last few publications
    /// instead of leaving some with the primary files of an old fence.
    #[tokio::test(flavor = "multi_thread")]
    async fn spaced_blocks_after_a_burst_catch_every_drained_generation_up()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        observe(&slot);
        for _ in 1..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
        }
        assert_eq!(
            publisher.candidate_preparations.cold,
            SECONDARY_GENERATION_COUNT
        );

        let mut spaced_fences = Vec::new();
        for block in 1..=2 * SECONDARY_GENERATION_COUNT {
            publisher.release_expired_pairs(tokio::time::Instant::now() + RETAINED_PAIR_WINDOW);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
            assert_eq!(
                slot.capture().canonical_fence(),
                fence,
                "spaced block {block} must be published"
            );
            spaced_fences.push(fence);
        }
        let recent_fences = spaced_fences
            .iter()
            .rev()
            .take(SECONDARY_GENERATION_COUNT)
            .collect::<Vec<_>>();
        for (generation, generation_entry) in publisher.generations.iter().enumerate() {
            let SecondaryGenerationState::Published { readers, .. } = &generation_entry.state
            else {
                return Err(format!("generation {generation} must stay open").into());
            };
            let canonical_fence = readers.canonical.event_fence();
            assert!(
                recent_fences.contains(&&canonical_fence),
                "generation {generation} must be caught up within the last \
                 {SECONDARY_GENERATION_COUNT} publications, but holds {canonical_fence:?}"
            );
            assert_eq!(
                WalletCanonicalSourceIdentity::from_ready_evidence(readers.wallet.ready_evidence())
                    .source_position()
                    .event_sequence,
                canonical_fence.chain_event_sequence(),
                "generation {generation} wallet secondary must match its canonical secondary"
            );
        }
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: SECONDARY_GENERATION_COUNT,
                recycled: 2 * SECONDARY_GENERATION_COUNT
            },
            "spaced blocks reuse opened slots without another cold open"
        );

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// A request that holds a pair freezes its generation: the publisher
    /// publishes every block from the other generations, never catches the
    /// held one up, and recycles it once the request releases it.
    ///
    /// The other three slots hold the current pair, the candidate, and one
    /// retained pair, so the blocks arrive at least half a retention window
    /// apart.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_held_pair_keeps_its_generation_frozen_while_publication_continues()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let held = slot.capture();
        let held_generation = publisher
            .published_generation
            .ok_or("bootstrap must publish a generation")?;
        let held_fence = held.canonical_fence();
        for block in 1..=2 * SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 1);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
            assert_eq!(
                slot.capture().canonical_fence(),
                fence,
                "block {block} must be published while a request holds an older pair"
            );
            assert_ne!(publisher.published_generation, Some(held_generation));
            assert_eq!(held.canonical().event_fence(), held_fence);
            assert_eq!(
                held.canonical().chain_epoch().id,
                held_fence.chain_epoch_id()
            );
            assert_eq!(
                WalletCanonicalSourceIdentity::from_ready_evidence(held.wallet().ready_evidence()),
                held.wallet_source()
            );
        }
        assert_eq!(
            publisher.candidate_preparations.cold,
            SECONDARY_GENERATION_COUNT
        );

        drop(held);
        let fence = primaries.append_block()?;
        publisher.refresh_once().await?;
        assert_eq!(slot.capture().canonical_fence(), fence);
        assert_eq!(
            publisher.published_generation,
            Some(held_generation),
            "the released generation is caught up and published again"
        );
        assert_eq!(
            publisher.candidate_preparations.cold,
            SECONDARY_GENERATION_COUNT
        );

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// A recycled generation follows a schedule revision the writer admits
    /// without moving its fence, and a writer fence it cannot reach fails the
    /// refresh closed exactly as it does for a cold candidate.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recycled_generation_follows_the_schedule_and_refuses_a_mutated_writer_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let readiness = zinder_runtime::Readiness::default();
        let (mut publisher, slot) = primaries.bootstrap_publisher(readiness.clone()).await?;
        observe(&slot);
        for _ in 0..SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 2);
            primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
        }
        let warmed = publisher.candidate_preparations;
        assert_eq!(
            warmed,
            CandidatePreparationCounts {
                cold: SECONDARY_GENERATION_COUNT,
                recycled: 1
            }
        );

        let fence_before_revision = slot.capture().canonical_fence();
        let extended = primaries.admit_schedule_revision()?;
        age_out_retained_pairs(&mut publisher, 2);
        publisher.refresh_once().await?;
        let revised_pair = slot.capture();
        assert_eq!(revised_pair.canonical_fence(), fence_before_revision);
        assert_eq!(
            revised_pair
                .canonical()
                .network_upgrade_schedule()
                .map(super::CanonicalActivationSchedule::activations),
            Some(&extended)
        );
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: warmed.cold,
                recycled: warmed.recycled + 1
            }
        );

        let appended_fence = primaries.append_block()?;
        let mut mutated_status = writer_status_for_store(primaries.canonical_primary()?);
        let digest_byte = mutated_status
            .fence
            .as_mut()
            .and_then(|fence| fence.canonical_sequence_digest.first_mut())
            .ok_or("writer status fixture must contain a digest")?;
        *digest_byte ^= 0xff;
        primaries.control.set_writer_status(mutated_status);
        let failure = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("a writer fence the recycled candidate cannot reach must fail refresh")?;
        assert!(matches!(
            failure,
            WalletServingPairError::WriterFenceMismatch
        ));
        publisher.record_refresh_failure(&failure);
        assert!(Arc::ptr_eq(&revised_pair, &slot.capture()));
        assert!(matches!(
            readiness.report().cause,
            zinder_runtime::ReadinessCause::SchemaMismatch
        ));

        primaries.publish_writer_status()?;
        publisher.refresh_once().await?;
        assert_eq!(slot.capture().canonical_fence(), appended_fence);
        assert!(readiness.report().is_ready);
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: warmed.cold,
                recycled: warmed.recycled + 2
            },
            "the refused candidate is kept and published without another open"
        );

        drop(revised_pair);
        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// A drained generation whose secondaries are still shared, as by a
    /// storage read that outlives the request that started it, stays frozen.
    ///
    /// The refresh waits for the generation, as it waits for one whose pair a
    /// request holds, instead of catching the shared secondaries up or
    /// opening a second instance over their metadata paths.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_generation_with_shared_secondaries_is_neither_caught_up_nor_reopened()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = AdvancingPrimaries::start().await?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let SecondaryGenerationState::Published { readers, .. } = &publisher.generations[0].state
        else {
            return Err("bootstrap must publish generation 0".into());
        };
        let shared_canonical = Arc::clone(&readers.canonical);
        let bootstrap_fence = shared_canonical.event_fence();
        observe(&slot);
        // These blocks fill the other slots. The bootstrap pair's window then
        // ends, which releases generation 0's pair.
        for _ in 1..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_once().await?;
            observe(&slot);
        }
        age_out_retained_pairs(&mut publisher, 2);

        let published_fence = slot.capture().canonical_fence();
        let fence = primaries.append_block()?;
        publisher.refresh_once().await?;
        assert_eq!(
            slot.capture().canonical_fence(),
            published_fence,
            "a shared generation must not become a candidate"
        );
        assert_eq!(shared_canonical.event_fence(), bootstrap_fence);
        assert_eq!(published_generation_is_reusable(&publisher, 0), Some(true));
        assert_eq!(
            publisher.candidate_preparations.cold,
            SECONDARY_GENERATION_COUNT
        );

        drop(shared_canonical);
        publisher.refresh_once().await?;
        assert_eq!(slot.capture().canonical_fence(), fence);
        assert_eq!(publisher.published_generation, Some(0));
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: SECONDARY_GENERATION_COUNT,
                recycled: 1
            }
        );

        drop(slot);
        drop(publisher);
        primaries.shutdown().await
    }

    /// Canonical blocks the table-file tests build before giving each
    /// canonical row a table file of its own.
    ///
    /// With the test budget's 64 open files, a secondary keeps at most 77
    /// table files open: a 54-entry table cache over 64 one-entry shards, plus
    /// up to 13 handles pinned at open. A range read over these blocks looks
    /// up only the compact-block row of each block, which sits in a level-0
    /// file of its own, so the read needs 96 files and must reopen one
    /// whatever the cache's hash seed.
    const SPLIT_TIP_HEIGHT: u32 = 96;

    /// A read that finds a table file of its pair deleted by the writer's
    /// compaction is answered by a replacement pair the publisher publishes
    /// at the writer's unchanged fence.
    ///
    /// The publisher's catch-up interval is an hour, so only the reads'
    /// report can wake it. Both concurrent reads capture the stale pair, and
    /// the pair is counted and logged once, with the missing file's path.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_missing_a_table_file_is_answered_by_a_replacement_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries =
            AdvancingPrimaries::start_through(BlockHeight::new(SPLIT_TIP_HEIGHT)).await?;
        let canonical_primary_path = primaries.close_canonical_primary()?;
        split_rows_into_table_files(&canonical_primary_path)?;
        let (publisher, slot) = primaries
            .bootstrap_publisher_every(
                zinder_runtime::Readiness::default(),
                Duration::from_hours(1),
            )
            .await?;
        let stale_pair = slot.capture();
        let stale_epoch = stale_pair.canonical_fence().chain_epoch_id();
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
        compact_away_table_files(&canonical_primary_path)?;

        let log_capture = LogCapture::install_for_target("zinder::wallet_serving");
        let range = split_block_range();
        let (first, second) = tokio::join!(
            query.compact_blocks_in_range(range, None),
            query.compact_blocks_in_range(range, None),
        );
        for blocks in [first?, second?] {
            assert_eq!(blocks.chain_epoch.id, stale_epoch);
            assert_eq!(
                blocks.compact_blocks.len(),
                usize::try_from(SPLIT_TIP_HEIGHT)?
            );
        }
        let replacement = slot.capture();
        assert!(
            !Arc::ptr_eq(&replacement, &stale_pair),
            "the pair missing a table file must be replaced"
        );
        assert_eq!(replacement.canonical_fence(), stale_pair.canonical_fence());
        assert_eq!(missing_table_file_pairs(&rendered_metrics.render()), 1);
        let warnings = log_capture
            .events()
            .into_iter()
            .filter(|event| event.field("event") == Some("wallet_serving_pair_table_file_missing"))
            .collect::<Vec<_>>();
        let [warning] = warnings.as_slice() else {
            return Err(format!("expected one missing-file warning, got {warnings:?}").into());
        };
        let logged_error = warning.field("error").unwrap_or_default();
        assert!(
            logged_error.contains(&canonical_primary_path.display().to_string())
                && logged_error.contains(".sst"),
            "the warning must name the missing canonical table file, got {logged_error:?}"
        );
        drop(log_capture);
        let pinned = query
            .compact_blocks_in_range(range, Some(stale_epoch))
            .await?;
        assert_eq!(pinned.chain_epoch.id, stale_epoch);

        drop((stale_pair, replacement, query, slot));
        cancel.cancel();
        serving_runtime_drained.cancel();
        publisher_task.await??;
        primaries.shutdown().await
    }

    /// A tip poll that finds a table file of its pair deleted recovers on an
    /// ephemeral replacement, so tip polling alone still holds no retention
    /// window when the next block arrives.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tip_snapshot_recovering_from_a_missing_table_file_retains_no_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let mut primaries =
            AdvancingPrimaries::start_through(BlockHeight::new(SPLIT_TIP_HEIGHT)).await?;
        let canonical_primary_path = primaries.close_canonical_primary()?;
        split_rows_into_table_files(&canonical_primary_path)?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher_every(
                zinder_runtime::Readiness::default(),
                Duration::from_hours(1),
            )
            .await?;
        let stale_pair = slot.capture_ephemeral();
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        compact_away_table_files(&canonical_primary_path)?;

        let replace_the_marked_pair = async {
            tokio::time::timeout(Duration::from_secs(20), async {
                while !slot.capture_ephemeral().has_missing_table_file() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|_| "the tip read never found a missing table file")?;
            publisher.refresh_once().await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (snapshot, replaced) =
            tokio::join!(query.served_tip_snapshot(), replace_the_marked_pair);
        replaced?;
        assert_eq!(snapshot?.chain_epoch, stale_pair.chain_epoch());
        let replacement = slot.capture_ephemeral();
        assert!(!Arc::ptr_eq(&replacement, &stale_pair));
        assert!(
            !replacement.is_observed_for_epoch_pin(),
            "recovery must not observe the replacement"
        );

        primaries.reopen_canonical_primary()?;
        let moved = primaries.append_block()?;
        publisher.refresh_once().await?;
        assert_eq!(slot.capture_ephemeral().canonical_fence(), moved);
        assert_eq!(slot.retained_pair_count(), 0);
        assert!(publisher.generation_wait.is_none());
        assert_eq!(
            generation_waits(&rendered_metrics.render(), "retention_window"),
            0
        );

        drop((stale_pair, replacement, query, slot));
        drop(publisher);
        primaries.shutdown().await
    }

    /// A read pinned to the epoch of a pair missing a table file is refused
    /// with the pin error once the replacement serves the writer's newer
    /// fence, and an unpinned read is answered there.
    ///
    /// A stream's next page, which carries no epoch check of its own, stops
    /// at its own epoch the same way instead of continuing at the newer one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pin_only_the_pair_missing_a_table_file_served_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries =
            AdvancingPrimaries::start_through(BlockHeight::new(SPLIT_TIP_HEIGHT)).await?;
        let canonical_primary_path = primaries.close_canonical_primary()?;
        split_rows_into_table_files(&canonical_primary_path)?;
        let (publisher, slot) = primaries
            .bootstrap_publisher_every(
                zinder_runtime::Readiness::default(),
                Duration::from_hours(1),
            )
            .await?;
        let stale_pair = slot.capture();
        let stale_epoch = stale_pair.canonical_fence().chain_epoch_id();
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
        compact_away_table_files(&canonical_primary_path)?;
        primaries.reopen_canonical_primary()?;
        let moved = primaries.append_block()?;

        match query
            .compact_blocks_in_range(split_block_range(), Some(stale_epoch))
            .await
        {
            Err(crate::QueryError::ChainEpochPinUnavailable { chain_epoch_id })
                if chain_epoch_id == stale_epoch => {}
            other => {
                return Err(format!("pin {stale_epoch:?} must be refused, got {other:?}").into());
            }
        }
        assert_eq!(slot.capture().canonical_fence(), moved);
        let current = query
            .compact_blocks_in_range(split_block_range(), None)
            .await?;
        assert_eq!(current.chain_epoch.id, moved.chain_epoch_id());
        let next_page = crate::serving_read::read_pair(
            &slot,
            Arc::clone(&stale_pair),
            Some(stale_epoch),
            |pair| async move {
                pair.canonical()
                    .compact_blocks_in_range(split_block_range())
                    .await
                    .map_err(crate::QueryError::canonical_read)
            },
        )
        .await;
        assert!(
            matches!(
                next_page,
                Err(crate::QueryError::ChainEpochPinUnavailable { chain_epoch_id })
                    if chain_epoch_id == stale_epoch
            ),
            "a stream page must stop at its own epoch, got {:?}",
            next_page.map(|blocks| blocks.len())
        );

        drop((stale_pair, query, slot));
        cancel.cancel();
        serving_runtime_drained.cancel();
        publisher_task.await??;
        primaries.shutdown().await
    }

    /// A read that fails on a corrupt table file fails closed as before: its
    /// pair is neither marked nor replaced.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_corrupt_table_file_fails_the_read_without_replacing_the_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries =
            AdvancingPrimaries::start_through(BlockHeight::new(SPLIT_TIP_HEIGHT)).await?;
        let canonical_primary_path = primaries.close_canonical_primary()?;
        let split_files = split_rows_into_table_files(&canonical_primary_path)?;
        let (mut publisher, slot) = primaries
            .bootstrap_publisher(zinder_runtime::Readiness::default())
            .await?;
        let pair = slot.capture();
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        corrupt_first_data_blocks(&split_files)?;

        let failure = query
            .compact_blocks_in_range(split_block_range(), None)
            .await
            .err()
            .ok_or("a corrupt table file must fail the read")?;
        assert!(!failure.invalidates_reader(), "{failure:?}");
        assert_eq!(
            zinder_proto::BoundaryError::error_reason(&failure),
            ErrorReason::StorageUnavailable
        );
        assert!(!pair.has_missing_table_file());
        publisher.refresh_once().await?;
        assert!(Arc::ptr_eq(&slot.capture(), &pair));
        assert_eq!(
            publisher.candidate_preparations,
            CandidatePreparationCounts {
                cold: 1,
                recycled: 0
            }
        );

        drop((pair, query, slot, publisher));
        primaries.shutdown().await
    }

    /// Only the report that marks a pair wakes the publisher.
    ///
    /// Every read on the marked pair that finds the file gone reports it
    /// again while the replacement is pending. A wake for each would run
    /// back-to-back refreshes whenever no generation is free, instead of
    /// leaving the retry to the periodic refresh.
    #[tokio::test]
    async fn reports_on_a_marked_pair_wake_the_publisher_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(crate::WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let slot = WalletServingPairSlot::new(Arc::clone(&pair));
        // The report only logs the read's failure.
        let failure = crate::QueryError::BlockingTaskFailed {
            reason: "a table file of the pair is missing".to_owned(),
        };

        let mut wakes = 0;
        for _ in 0..8 {
            slot.report_missing_table_file(&pair, &failure);
            if tokio::time::timeout(
                Duration::ZERO,
                missing_table_file_reported(Some(&slot.signals)),
            )
            .await
            .is_ok()
            {
                wakes += 1;
            }
        }

        assert_eq!(wakes, 1);
        assert!(pair.has_missing_table_file());
        Ok(())
    }

    /// A candidate that finds a table file it just listed deleted leaves the
    /// published pair serving.
    ///
    /// That holds whether the file goes missing during the candidate's open
    /// or catch-up or during its admission read after the catch-up, and the
    /// logged failure names the file.
    #[test]
    fn a_candidate_missing_a_table_file_keeps_the_attested_pair_serving()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = lifecycle_upgrade_activations()?;
        let canonical_primary_path = temporary.path().join("canonical-primary");
        drop(build_lifecycle_canonical_primary_through(
            &canonical_primary_path,
            &activations,
            BlockHeight::new(SPLIT_TIP_HEIGHT),
        )?);
        split_rows_into_table_files(&canonical_primary_path)?;
        let stale = RocksDbCanonicalSecondary::open_ready(
            &canonical_primary_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            zinder_store::CanonicalStoreRequirements::for_blob_retention(
                RawBlobRetention::Transactions,
            ),
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        compact_away_table_files(&canonical_primary_path)?;
        let missing = stale
            .compact_blocks_in_range(split_block_range())
            .err()
            .ok_or("a stale secondary must reopen a compacted-away table file")?;
        assert!(missing.is_missing_table_file(), "{missing:?}");

        let failure = match candidate_epoch_read_failure(missing) {
            Err(failure @ WalletServingPairError::Canonical(_)) => failure,
            other => {
                return Err(format!("the refresh must fail with its source, got {other:?}").into());
            }
        };
        assert_eq!(
            candidate_epoch_read_failure(CanonicalStoreError::StoreNotReady {
                path: canonical_primary_path.clone(),
            })
            .ok(),
            Some(WalletServingConvergence::SchemaOrFenceMismatch),
            "every other admission read failure stays a schema or fence mismatch"
        );
        let logged_error = error_with_sources(&failure);
        assert!(
            logged_error.contains(&canonical_primary_path.display().to_string())
                && logged_error.contains(".sst"),
            "the refresh failure must name the missing table file, got {logged_error:?}"
        );
        assert!(refresh_failure_retains_attested_pair(&failure));
        assert_eq!(
            refresh_failure_not_ready_cause(&failure),
            Some(zinder_runtime::ReadinessCause::StorageUnavailable),
            "past the staleness ceiling the failure still fails closed"
        );
        Ok(())
    }

    /// Outputs of the address the stream tests read, all created in block 1.
    ///
    /// With every address-index row in a table file of its own, the scan of a
    /// page read from the fourth page on opens at least the 88 index files at
    /// or after its start. That is more than the 77 a secondary keeps open
    /// (see [`SPLIT_TIP_HEIGHT`]), so such a read must reopen a file whatever
    /// the cache's hash seed. A stream whose caller has taken only its first
    /// output has read at most three pages, so it reads the fourth only after
    /// a test that then deletes the files resumes reading.
    const SPLIT_OUTPUT_COUNT: usize = 112;

    /// Outputs each page read of the stream tests returns.
    const SPLIT_PAGE_SIZE: std::num::NonZeroU16 = std::num::NonZeroU16::MIN.saturating_add(7);

    /// A stream continues at its epoch after its pair loses a wallet table
    /// file.
    ///
    /// The projector's compaction deletes the file after the stream delivered
    /// a page. The stream continues on the pair the publisher publishes in
    /// its place at the same epoch, and delivers every output exactly once.
    ///
    /// The publisher's catch-up interval is an hour, so only the page read's
    /// report can wake it. The warning names a missing wallet table file, so
    /// the wallet secondary reports the deletion with the same status as the
    /// canonical one.
    #[tokio::test]
    async fn a_stream_continues_at_its_epoch_after_its_pair_loses_a_wallet_table_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let primaries = UnspentOutputPrimaries::start().await?;
        split_wallet_address_index_into_table_files(&primaries.wallet_primary_path)?;
        let (publisher, slot) = primaries
            .bootstrap_publisher_every(Duration::from_hours(1))
            .await?;
        let stale_pair = slot.capture();
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
        let log_capture = LogCapture::install_for_target("zinder::wallet_serving");
        let mut stream = primaries.open_stream(&slot);
        let first = stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;

        compact_away_table_files(&primaries.wallet_primary_path)?;
        let mut output_indexes = vec![first.outpoint.output_index];
        while let Some(output) = stream.next().await {
            output_indexes.push(output?.outpoint.output_index);
        }

        assert_eq!(
            stream.chain_epoch.id,
            stale_pair.canonical_fence().chain_epoch_id()
        );
        assert_eq!(
            output_indexes,
            (0..u32::try_from(SPLIT_OUTPUT_COUNT)?).collect::<Vec<_>>()
        );
        assert!(
            stale_pair.has_missing_table_file(),
            "a page read must have found a wallet table file of the stream's pair deleted"
        );
        let replacement = slot.capture();
        assert!(!Arc::ptr_eq(&replacement, &stale_pair));
        assert_eq!(replacement.canonical_fence(), stale_pair.canonical_fence());
        let warnings = log_capture
            .events()
            .into_iter()
            .filter(|event| event.field("event") == Some("wallet_serving_pair_table_file_missing"))
            .collect::<Vec<_>>();
        let [warning] = warnings.as_slice() else {
            return Err(format!("expected one missing-file warning, got {warnings:?}").into());
        };
        let logged_error = warning.field("error").unwrap_or_default();
        assert!(
            logged_error.contains(&primaries.wallet_primary_path.display().to_string())
                && logged_error.contains(".sst"),
            "the warning must name the missing wallet table file, got {logged_error:?}"
        );

        drop((log_capture, stream, stale_pair, replacement, slot));
        cancel.cancel();
        serving_runtime_drained.cancel();
        publisher_task.await??;
        primaries.shutdown().await
    }

    /// A stream ends with its own epoch unavailable when the pair that
    /// replaces its pair serves a newer epoch.
    ///
    /// The pair loses a wallet table file after the stream delivered a page,
    /// and the stream never ends as if complete.
    ///
    /// The writer and the projector commit the next block before the
    /// projector's compaction deletes the file, so the replacement the read's
    /// report asks for is published at the newer epoch. The test runtime has
    /// one thread, so the publisher and the stream's producer act only while
    /// the test awaits the stream.
    #[tokio::test]
    async fn a_stream_ends_with_its_epoch_unavailable_when_its_replacement_serves_a_newer_epoch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut primaries = UnspentOutputPrimaries::start().await?;
        split_wallet_address_index_into_table_files(&primaries.wallet_primary_path)?;
        let (publisher, slot) = primaries
            .bootstrap_publisher_every(Duration::from_hours(1))
            .await?;
        let stale_pair = slot.capture();
        let stale_epoch = stale_pair.canonical_fence().chain_epoch_id();
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
        let mut stream = primaries.open_stream(&slot);
        stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;

        let moved = primaries.append_block()?;
        compact_away_table_files(&primaries.wallet_primary_path)?;
        let mut delivered = 1;
        let failure = loop {
            match stream.next().await {
                Some(Ok(_)) => delivered += 1,
                Some(Err(error)) => break error,
                None => {
                    return Err("a stream whose pair lost a table file ended as if complete".into());
                }
            }
        };

        assert!(
            matches!(
                failure,
                crate::QueryError::ChainEpochPinUnavailable { chain_epoch_id }
                    if chain_epoch_id == stale_epoch
            ),
            "the stream must end with its own epoch unavailable, got {failure:?}"
        );
        assert!(delivered < SPLIT_OUTPUT_COUNT);
        assert!(stream.next().await.is_none());
        assert!(stale_pair.has_missing_table_file());
        assert_eq!(slot.capture().canonical_fence(), moved);

        drop((stream, stale_pair, slot));
        cancel.cancel();
        serving_runtime_drained.cancel();
        publisher_task.await??;
        primaries.shutdown().await
    }

    fn split_block_range() -> BlockHeightRange {
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(SPLIT_TIP_HEIGHT))
    }

    /// Opens a closed canonical or wallet primary directly, with automatic
    /// compaction off in every family so only the test replaces its table
    /// files.
    fn open_raw_primary(
        path: &Path,
    ) -> Result<(rust_rocksdb::DB, Vec<String>), Box<dyn std::error::Error>> {
        let families = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), path)?;
        let db = rust_rocksdb::DB::open_cf(&rust_rocksdb::Options::default(), path, &families)?;
        for family in &families {
            let handle = db.cf_handle(family).ok_or("a listed family must open")?;
            db.set_options_cf(&handle, &[("disable_auto_compactions", "true")])?;
        }
        Ok((db, families))
    }

    /// Rewrites every row of a closed canonical or wallet primary unchanged,
    /// flushing after each, so each row's newest version sits in an L0 table
    /// file of its own. Returns those files.
    fn split_rows_into_table_files(
        path: &Path,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let (db, families) = open_raw_primary(path)?;
        for family in &families {
            split_family_rows(&db, family)?;
        }
        level_zero_table_files(&db)
    }

    /// Splits only the address index of a closed wallet primary as
    /// [`split_rows_into_table_files`] splits every family.
    ///
    /// The index is the family a page read of an unspent-output stream scans,
    /// and splitting nothing else keeps the flushes, and the files each
    /// secondary opens, to one per output.
    fn split_wallet_address_index_into_table_files(
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (db, _families) = open_raw_primary(path)?;
        split_family_rows(&db, "transparent_unspent_output_by_address")
    }

    fn split_family_rows(
        db: &rust_rocksdb::DB,
        family: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let handle = db.cf_handle(family).ok_or("the family must exist")?;
        let rows = db
            .iterator_cf(&handle, rust_rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        for (key, row) in rows {
            db.put_cf(&handle, key, row)?;
            db.flush_cf(&handle)?;
        }
        Ok(())
    }

    /// Compacts every family of a closed canonical or wallet primary, as the
    /// writer's or the projector's compaction does, and waits until its L0
    /// table files are deleted.
    ///
    /// Rewriting the bottommost level too keeps a file that a compaction
    /// would only move from surviving under its old number.
    fn compact_away_table_files(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let (db, families) = open_raw_primary(path)?;
        let replaced = level_zero_table_files(&db)?;
        let mut options = rust_rocksdb::CompactOptions::default();
        options.set_bottommost_level_compaction(rust_rocksdb::BottommostLevelCompaction::Force);
        for family in &families {
            let handle = db.cf_handle(family).ok_or("a listed family must open")?;
            db.compact_range_cf_opt(&handle, None::<&[u8]>, None::<&[u8]>, &options);
        }
        // The primary deletes obsolete files after each compaction job, which
        // can finish just after the call returns.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while replaced.iter().any(|file| file.exists()) {
            if std::time::Instant::now() >= deadline {
                return Err("the primary must delete the compacted-away table files".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn level_zero_table_files(
        db: &rust_rocksdb::DB,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        Ok(db
            .live_files()?
            .into_iter()
            .filter(|file| file.level == 0)
            .map(|file| Path::new(&file.directory).join(file.name.trim_start_matches('/')))
            .collect())
    }

    /// Flips the first byte of each table file, which starts its first data
    /// block, so reading a row it holds fails that block's checksum.
    fn corrupt_first_data_blocks(files: &[PathBuf]) -> Result<(), Box<dyn std::error::Error>> {
        for file in files {
            let mut table_bytes = std::fs::read(file)?;
            let first_byte = table_bytes
                .first_mut()
                .ok_or("a table file is never empty")?;
            *first_byte ^= 0xff;
            std::fs::write(file, table_bytes)?;
        }
        Ok(())
    }

    /// Reads the missing-table-file pair count from a Prometheus exposition.
    fn missing_table_file_pairs(exposition: &str) -> u64 {
        exposition
            .lines()
            .find_map(|line| {
                line.strip_prefix("zinder_wallet_serving_missing_table_file_pairs_total ")
            })
            .and_then(|count| count.trim().parse().ok())
            .unwrap_or(0)
    }

    fn fetch_operations_json(
        address: std::net::SocketAddr,
        path: &str,
    ) -> Result<(u16, serde_json::Value), Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(address)?;
        stream.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let body_offset = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|offset| offset + 4)
            .ok_or("operations response omitted HTTP body delimiter")?;
        let headers = std::str::from_utf8(&response[..body_offset])?;
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .ok_or("operations response omitted HTTP status")?
            .parse()?;
        let body = serde_json::from_slice(&response[body_offset..])?;
        Ok((status, body))
    }

    /// Polls `/readyz` until `predicate` accepts its body and returns that
    /// body.
    async fn wait_for_readyz(
        address: std::net::SocketAddr,
        predicate: impl Fn(&serde_json::Value) -> bool,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (_, readyz) = fetch_operations_json(address, "/readyz")?;
            if predicate(&readyz) {
                return Ok(readyz);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(
                    format!("timed out waiting for /readyz, which last served {readyz}").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Serves a one-block regtest chain whose writer admitted the sample
    /// regtest schedule. The fixture owns the store directory.
    fn regtest_serving_pair_slot()
    -> Result<(WalletServingPairSlot, WalletServingStoreFixture), Box<dyn std::error::Error>> {
        let (pair, store) = regtest_serving_pair(1)?;
        Ok((WalletServingPairSlot::new(pair), store))
    }

    /// A regtest pair whose visible tip is at height `block_count`.
    fn regtest_serving_pair(
        block_count: u32,
    ) -> Result<
        (Arc<crate::WalletServingReadPair>, WalletServingStoreFixture),
        Box<dyn std::error::Error>,
    > {
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(block_count);
        let mut store =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = crate::WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?;
        Ok((Arc::new(pair), store))
    }

    /// Waits up to three seconds for `condition`.
    async fn wait_until(
        condition: impl Fn() -> bool + Send + Sync,
    ) -> Result<(), tokio::time::error::Elapsed> {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
    }

    fn writer_status(source: WalletCanonicalSourceIdentity) -> CanonicalWriterStatusResponse {
        let position = source.source_position();
        CanonicalWriterStatusResponse {
            network_name: "zcash-regtest".to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: position.chain_epoch_id.value(),
                event_sequence: position.event_sequence,
                visible_tip_height: position.tip.height.value(),
                visible_tip_hash: position.tip.hash.as_bytes().to_vec(),
                visible_block_count: source.source_sequence_digest().block_count(),
                canonical_sequence_digest: source.source_sequence_digest().as_bytes().to_vec(),
            }),
            oldest_retained_event_sequence: 1,
            network_upgrade_schedule: None,
            reorg_window_exceeded: None,
            tip_unconfirmed: false,
            canonical_store_damaged: None,
        }
    }

    fn source_identity(
        event_sequence: u64,
        sequence_digest_byte: u8,
    ) -> WalletCanonicalSourceIdentity {
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0x33; 32]));
        WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(ChainEpochId::new(3), tip, event_sequence),
            zinder_core::CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                3,
                [sequence_digest_byte; 32],
            ),
            tip,
        )
    }

    fn wallet_serving_pair_config(
        canonical_primary_path: std::path::PathBuf,
        canonical_secondary_root: std::path::PathBuf,
        wallet_primary_path: std::path::PathBuf,
        wallet_secondary_root: std::path::PathBuf,
    ) -> Result<WalletServingPairConfig, Box<dyn std::error::Error>> {
        Ok(WalletServingPairConfig {
            canonical_primary_path,
            canonical_secondary_root,
            wallet_primary_path,
            wallet_secondary_root,
            network: Network::ZcashRegtest,
            canonical_requirements: CanonicalStoreRequirements::for_blob_retention(
                RawBlobRetention::Transactions,
            ),
            canonical_reorg_policy: CanonicalReorgPolicy::new(100)?,
            canonical_resource_budget: RocksDbResourceBudget::for_local_tests(),
            wallet_resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: NonZeroU32::new(4)
                .and_then(|attempts| u8::try_from(attempts.get()).ok())
                .and_then(std::num::NonZeroU8::new)
                .ok_or("wallet-serving pair convergence attempts must be non-zero")?,
            replica_lag_threshold_chain_epochs: 4,
            serving_pair_staleness_ceiling: Duration::from_mins(5),
        })
    }

    fn build_lifecycle_canonical_primary(
        canonical_primary_path: &Path,
        activations: &NetworkUpgradeActivations,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        build_lifecycle_canonical_primary_through(
            canonical_primary_path,
            activations,
            BlockHeight::new(1),
        )
    }

    /// Builds a canonical primary holding blocks 1 through `tip_height` and
    /// admits `activations` as schedule revision 1.
    ///
    /// Blocks carry the hashes [`AdvancingPrimaries::append_block`] gives
    /// later blocks, and the admission is the one the canonical writer makes
    /// when it first opens the store.
    fn build_lifecycle_canonical_primary_through(
        canonical_primary_path: &Path,
        activations: &NetworkUpgradeActivations,
        tip_height: BlockHeight,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let mut blocks = Vec::new();
        let mut parent_hash = Network::ZcashRegtest.genesis_hash();
        for height in (1..=tip_height.value()).map(BlockHeight::new) {
            let block_hash = if height == BlockHeight::new(1) {
                BlockHash::from_bytes([0x11; 32])
            } else {
                BlockHash::from_bytes([u8::try_from(height.value())?.wrapping_add(0x40); 32])
            };
            let mut block = lifecycle_build_block(height, block_hash, parent_hash);
            // A bulk load holds checkpoints only at the build tip and on the
            // global cadence.
            if height != tip_height
                && !height
                    .value()
                    .is_multiple_of(zinder_store::TREE_STATE_CHECKPOINT_STRIDE)
            {
                block.tree_state_checkpoint = None;
            }
            blocks.push(block);
            parent_hash = block_hash;
        }
        let tip = BlockId::new(tip_height, parent_hash);
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            canonical_primary_path,
            CanonicalStoreWorkload::Wallet,
            CanonicalStoreBuildPlan::complete(
                activations,
                0,
                tip,
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
            )?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(activations, blocks.into_iter().map(Ok::<_, std::io::Error>))?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            tip,
            tip_height.value(),
            CommitmentTreeFrontiers::default(),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            tip,
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn lifecycle_build_block(
        height: BlockHeight,
        block_hash: BlockHash,
        parent_hash: BlockHash,
    ) -> CanonicalBuildBlock {
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                height,
                block_hash,
                parent_hash,
                [0; 32],
                [0; 32],
                i64::from(height.value()),
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &block_hash.as_bytes(),
            ),
            transactions: Vec::new(),
        };
        CanonicalBuildBlock {
            replay_envelope: encode_canonical_block_replay(
                &facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            ),
            compact_block: zinder_core::CompactBlockArtifact::empty(
                BlockId::new(height, block_hash),
                parent_hash,
                height.value(),
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
                BlockId::new(height, block_hash),
                height.value(),
                CommitmentTreeFrontiers::default(),
            )),
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        }
    }

    fn reconcile_wallet_primary(
        wallet_primary: &mut RocksDbWalletStore,
        canonical_secondary: &RocksDbCanonicalSecondary,
        initial_source: WalletCanonicalSourceIdentity,
        target_fence: CanonicalEventFence,
        target_settled_tip: BlockId,
    ) -> Result<WalletCanonicalSourceIdentity, Box<dyn std::error::Error>> {
        let source_cursor = initial_source.source_position().event_cursor.as_bytes();
        let retained_events =
            canonical_secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
                Some(&source_cursor),
                NonZeroU32::new(16).ok_or("canonical event page limit must be non-zero")?,
            ))?;
        let first_replayed_height = initial_source
            .source_position()
            .tip
            .height
            .next()
            .ok_or("the wallet source tip cannot advance")?;
        let replay_range =
            BlockHeightRange::inclusive(first_replayed_height, target_fence.visible_tip().height);
        wallet_primary.reconcile_canonical_event_sequence(
            initial_source,
            &retained_events,
            target_fence,
            target_settled_tip,
            None,
            replay_range,
            NonZeroU64::new(512_u64 * 1024 * 1024)
                .ok_or("wallet transition byte limit must be non-zero")?,
            canonical_secondary.scan_canonical_replay_range(replay_range)?,
        )?;
        Ok(WalletCanonicalSourceIdentity::from_ready_evidence(
            wallet_primary.ready_evidence(),
        ))
    }

    fn writer_status_for_store(store: &RocksDbCanonicalStore) -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            network_upgrade_schedule: store.network_upgrade_schedule().map(|schedule| {
                zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                    schedule.activations(),
                    schedule.revision(),
                )
            }),
            ..writer_status_for_fence(store.event_fence())
        }
    }

    fn writer_status_for_fence(fence: CanonicalEventFence) -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: fence.chain_epoch_id().value(),
                event_sequence: fence.chain_event_sequence(),
                visible_tip_height: fence.visible_tip().height.value(),
                visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
                visible_block_count: fence.sequence_digest().block_count(),
                canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
            }),
            oldest_retained_event_sequence: 1,
            network_upgrade_schedule: None,
            reorg_window_exceeded: None,
            tip_unconfirmed: false,
            canonical_store_damaged: None,
        }
    }

    fn published_generation_is_reusable(
        publisher: &WalletServingPairPublisher,
        generation: usize,
    ) -> Option<bool> {
        match &publisher.generations[generation].state {
            SecondaryGenerationState::Published { lease, .. } => Some(lease.is_reusable()),
            SecondaryGenerationState::Vacant | SecondaryGenerationState::Candidate { .. } => None,
        }
    }

    /// Observes the published pair, as a request that exposes its chain epoch
    /// does, so the slot retains it once it is replaced.
    fn observe(slot: &WalletServingPairSlot) {
        drop(slot.capture());
    }

    /// Ends the retention window of every retained pair except the `kept`
    /// most recently replaced ones, as blocks spaced far enough apart would.
    ///
    /// Keeping two models blocks at least a third of a window apart, so three
    /// publications span the window. Keeping one models blocks at least half
    /// a window apart, and keeping none models blocks a whole window apart.
    fn age_out_retained_pairs(publisher: &mut WalletServingPairPublisher, kept: usize) {
        let newest_aged_out = publisher.serving_pair_slot.as_ref().and_then(|slot| {
            slot.published
                .load()
                .retained
                .get(kept)
                .map(|retained| retained.replaced_at)
        });
        if let Some(replaced_at) = newest_aged_out {
            publisher.release_expired_pairs(replaced_at + RETAINED_PAIR_WINDOW);
        }
    }

    fn lifecycle_upgrade_activations()
    -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
        let activations = [
            ("Overwinter", 1_u32),
            ("Sapling", 2),
            ("Blossom", 3),
            ("Heartwood", 4),
            ("Canopy", 5),
            ("NU5", 6),
            ("NU6", 7),
            ("NU6.1", 8),
            ("NU6.2", 9),
            ("NU6.3", 10),
        ]
        .into_iter()
        .map(|(name, branch_id)| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(branch_id),
            activation_height: BlockHeight::new(100),
            name: name.to_owned(),
        })
        .collect();
        Ok(NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            activations,
        )?)
    }

    /// A canonical writer whose wallet projection follows it one block at a
    /// time, behind the writer-status endpoint a publisher attests against.
    struct AdvancingPrimaries {
        activations: NetworkUpgradeActivations,
        canonical_primary: Option<RocksDbCanonicalStore>,
        wallet_primary: RocksDbWalletStore,
        projector_secondary: RocksDbCanonicalSecondary,
        wallet_source: WalletCanonicalSourceIdentity,
        settled_tip: BlockId,
        control: MutableCanonicalControl,
        endpoint: String,
        cancel: CancellationToken,
        server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        // Declared last so the stores close before their directory is removed.
        temporary: TempDir,
    }

    impl AdvancingPrimaries {
        async fn start() -> Result<Self, Box<dyn std::error::Error>> {
            Self::start_through(BlockHeight::new(1)).await
        }

        async fn start_through(
            tip_height: BlockHeight,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let activations = lifecycle_upgrade_activations()?;
            let canonical_primary_path = temporary.path().join("canonical-primary");
            let canonical_primary = build_lifecycle_canonical_primary_through(
                &canonical_primary_path,
                &activations,
                tip_height,
            )?;
            let wallet_outcome = build_wallet_from_canonical(
                &canonical_primary,
                temporary.path().join("wallet-primary"),
                RocksDbWalletBuildOptions {
                    supported_reorg_depth: 100,
                    ..RocksDbWalletBuildOptions::for_local_tests()
                },
            )?;
            let projector_secondary = RocksDbCanonicalSecondary::open_ready(
                &canonical_primary_path,
                temporary.path().join("projector-canonical-secondary"),
                &activations,
                zinder_store::CanonicalStoreRequirements::for_blob_retention(
                    RawBlobRetention::Transactions,
                ),
                CanonicalReorgPolicy::new(100)?,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            let control = MutableCanonicalControl::new(writer_status_for_store(&canonical_primary));
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let cancel = CancellationToken::new();
            let server_task = tokio::spawn({
                let control = control.clone();
                let shutdown = cancel.clone().cancelled_owned();
                async move {
                    Server::builder()
                        .add_service(CanonicalControlServer::new(control))
                        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                        .await
                }
            });
            Ok(Self {
                activations,
                settled_tip: canonical_primary.event_fence().visible_tip(),
                canonical_primary: Some(canonical_primary),
                wallet_source: wallet_outcome.report.canonical_source_identity(),
                wallet_primary: wallet_outcome.store,
                projector_secondary,
                control,
                endpoint,
                cancel,
                server_task,
                temporary,
            })
        }

        async fn bootstrap_publisher(
            &self,
            readiness: zinder_runtime::Readiness,
        ) -> Result<(WalletServingPairPublisher, WalletServingPairSlot), Box<dyn std::error::Error>>
        {
            self.bootstrap_publisher_every(readiness, Duration::from_millis(1))
                .await
        }

        async fn bootstrap_publisher_every(
            &self,
            readiness: zinder_runtime::Readiness,
            catchup_interval: Duration,
        ) -> Result<(WalletServingPairPublisher, WalletServingPairSlot), Box<dyn std::error::Error>>
        {
            let config = WalletServingPairConfig {
                catchup_interval,
                ..wallet_serving_pair_config(
                    self.temporary.path().join("canonical-primary"),
                    self.temporary.path().join("canonical-secondaries"),
                    self.temporary.path().join("wallet-primary"),
                    self.temporary.path().join("wallet-secondaries"),
                )?
            };
            Ok(
                WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                    config,
                    WalletServingReadiness::pair_only(readiness),
                    &self.endpoint,
                    None,
                )
                .await?,
            )
        }

        /// Closes the canonical primary so the test can act on its files as
        /// only the writer may, and returns its path.
        fn close_canonical_primary(&mut self) -> Result<PathBuf, Box<dyn std::error::Error>> {
            drop(
                self.canonical_primary
                    .take()
                    .ok_or("the canonical primary is already closed")?,
            );
            Ok(self.temporary.path().join("canonical-primary"))
        }

        /// Reopens the canonical primary [`Self::close_canonical_primary`]
        /// closed.
        fn reopen_canonical_primary(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.canonical_primary = Some(RocksDbCanonicalStore::open_ready(
                self.temporary.path().join("canonical-primary"),
                &self.activations,
                CanonicalStoreWorkload::Wallet,
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
                RocksDbResourceBudget::for_local_tests(),
            )?);
            Ok(())
        }

        fn canonical_primary(&self) -> Result<&RocksDbCanonicalStore, Box<dyn std::error::Error>> {
            Ok(self
                .canonical_primary
                .as_ref()
                .ok_or("an earlier append closed the canonical primary")?)
        }

        /// Commits the next block, projects it into the wallet primary, and
        /// serves the writer status that attests it.
        fn append_block(&mut self) -> Result<CanonicalEventFence, Box<dyn std::error::Error>> {
            let canonical_primary = self
                .canonical_primary
                .take()
                .ok_or("an earlier append closed the canonical primary")?;
            let parent = canonical_primary.event_fence();
            let height = parent
                .visible_tip()
                .height
                .next()
                .ok_or("the visible tip cannot advance")?;
            let hash_byte = u8::try_from(height.value())?.wrapping_add(0x40);
            let (canonical_primary, fence) = canonical_primary.commit_live_append(
                CanonicalLiveAppend::new(
                    parent,
                    lifecycle_build_block(
                        height,
                        BlockHash::from_bytes([hash_byte; 32]),
                        parent.visible_tip().hash,
                    ),
                    Vec::new(),
                    self.settled_tip,
                    UnixTimestampMillis::new(1_750_000_000_000 + u64::from(height.value())),
                ),
                &self.activations,
            )?;
            self.canonical_primary = Some(canonical_primary);
            self.projector_secondary.try_catch_up()?;
            self.wallet_source = reconcile_wallet_primary(
                &mut self.wallet_primary,
                &self.projector_secondary,
                self.wallet_source,
                fence,
                self.settled_tip,
            )?;
            self.publish_writer_status()?;
            Ok(fence)
        }

        /// Admits a schedule revision that adds NU7 without moving the fence.
        fn admit_schedule_revision(
            &mut self,
        ) -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
            let mut entries = self.activations.activations().to_vec();
            entries.push(NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: BlockHeight::new(200),
                name: "NU7".to_owned(),
            });
            let extended = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
            self.canonical_primary
                .as_mut()
                .ok_or("an earlier append closed the canonical primary")?
                .admit_network_upgrade_schedule(&extended)?;
            self.activations = extended.clone();
            self.publish_writer_status()?;
            Ok(extended)
        }

        fn publish_writer_status(&self) -> Result<(), Box<dyn std::error::Error>> {
            self.control
                .set_writer_status(writer_status_for_store(self.canonical_primary()?));
            Ok(())
        }

        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            self.cancel.cancel();
            self.server_task.await??;
            Ok(())
        }
    }

    /// A canonical writer whose block 1 creates [`SPLIT_OUTPUT_COUNT`]
    /// outputs for one address, its wallet projection, and the writer-status
    /// endpoint a publisher attests against.
    ///
    /// The wallet primary stays closed between steps, so a test can act on
    /// its table files as only the projector may.
    struct UnspentOutputPrimaries {
        activations: NetworkUpgradeActivations,
        chain: ChainFixture,
        address: TransparentAddressScriptHash,
        writer: CanonicalWriterFixture,
        wallet_primary_path: PathBuf,
        wallet_source: WalletCanonicalSourceIdentity,
        control: MutableCanonicalControl,
        endpoint: String,
        cancel: CancellationToken,
        server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        // Declared last so the stores close before their directory is removed.
        temporary: TempDir,
    }

    impl UnspentOutputPrimaries {
        /// Publishes blocks 1 and 2 and projects them, leaving block 3 for
        /// [`Self::append_block`].
        async fn start() -> Result<Self, Box<dyn std::error::Error>> {
            let activations = sample_regtest_upgrade_activations();
            let (chain, address) = address_with_unspent_outputs(SPLIT_OUTPUT_COUNT)?;
            let chain = chain.extend_blocks(1);
            let writer = CanonicalWriterFixture::publish(
                &CanonicalBaseline::new(&chain).through(BlockHeight::new(2)),
                &activations,
            )?;
            let temporary = TempDir::new()?;
            let wallet_primary_path = temporary.path().join("wallet-primary");
            let wallet_source = build_wallet_from_canonical(
                writer.canonical_primary()?,
                &wallet_primary_path,
                RocksDbWalletBuildOptions {
                    supported_reorg_depth: 100,
                    ..RocksDbWalletBuildOptions::for_local_tests()
                },
            )?
            .report
            .canonical_source_identity();
            let control =
                MutableCanonicalControl::new(writer_status_for_store(writer.canonical_primary()?));
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let cancel = CancellationToken::new();
            let server_task = tokio::spawn({
                let control = control.clone();
                let shutdown = cancel.clone().cancelled_owned();
                async move {
                    Server::builder()
                        .add_service(CanonicalControlServer::new(control))
                        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                        .await
                }
            });
            Ok(Self {
                activations,
                chain,
                address,
                writer,
                wallet_primary_path,
                wallet_source,
                control,
                endpoint,
                cancel,
                server_task,
                temporary,
            })
        }

        async fn bootstrap_publisher_every(
            &self,
            catchup_interval: Duration,
        ) -> Result<(WalletServingPairPublisher, WalletServingPairSlot), Box<dyn std::error::Error>>
        {
            let config = WalletServingPairConfig {
                catchup_interval,
                canonical_requirements: CanonicalStoreRequirements::for_blob_retention(
                    self.writer.canonical_primary()?.raw_blob_retention(),
                ),
                ..wallet_serving_pair_config(
                    self.writer.canonical_primary_path().to_path_buf(),
                    self.temporary.path().join("canonical-secondaries"),
                    self.wallet_primary_path.clone(),
                    self.temporary.path().join("wallet-secondaries"),
                )?
            };
            Ok(
                WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                    config,
                    WalletServingReadiness::pair_only(zinder_runtime::Readiness::default()),
                    &self.endpoint,
                    None,
                )
                .await?,
            )
        }

        /// Streams the address's outputs from the current pair of `slot`,
        /// [`SPLIT_PAGE_SIZE`] outputs per page read.
        fn open_stream(
            &self,
            slot: &WalletServingPairSlot,
        ) -> crate::TransparentAddressUnspentOutputs {
            let pair = slot.capture();
            let chain_epoch = pair.canonical().chain_epoch();
            crate::TransparentAddressUnspentOutputs::spawn_paged(
                slot.clone(),
                pair,
                &crate::TransparentAddressUnspentOutputsRequest {
                    address_script_hash: self.address,
                    start_height: BlockHeight::new(0),
                },
                chain_epoch,
                SPLIT_PAGE_SIZE,
            )
        }

        /// Commits block 3 and serves the writer status that attests it, then
        /// projects the block into the wallet primary, as the projector
        /// follows the writer.
        fn append_block(&mut self) -> Result<CanonicalEventFence, Box<dyn std::error::Error>> {
            let settled = self.writer.canonical_primary()?.chain_epoch()?;
            let settled_tip = BlockId::new(settled.settled_tip_height, settled.settled_tip_hash);
            let fence = self.writer.append_live_block(&self.chain)?;
            self.control
                .set_writer_status(writer_status_for_store(self.writer.canonical_primary()?));
            let projector_secondary = RocksDbCanonicalSecondary::open_ready(
                self.writer.canonical_primary_path(),
                self.temporary.path().join("projector-canonical-secondary"),
                &self.activations,
                zinder_store::CanonicalStoreRequirements::for_blob_retention(
                    self.writer.canonical_primary()?.raw_blob_retention(),
                ),
                CanonicalReorgPolicy::new(100)?,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            let mut wallet_primary = RocksDbWalletStore::open_ready(
                &self.wallet_primary_path,
                Network::ZcashRegtest,
                self.wallet_source,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            self.wallet_source = reconcile_wallet_primary(
                &mut wallet_primary,
                &projector_secondary,
                self.wallet_source,
                fence,
                settled_tip,
            )?;
            Ok(fence)
        }

        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            self.cancel.cancel();
            self.server_task.await??;
            Ok(())
        }
    }

    /// Ingest control and canonical control served on one endpoint, as the
    /// canonical writer serves them.
    struct WriterControlFixture {
        endpoint: String,
        cancel: CancellationToken,
        server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl WriterControlFixture {
        async fn spawn(
            ingest_control: IngestControlFixtureService,
            canonical_control: MutableCanonicalControl,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let cancel = CancellationToken::new();
            let server_cancel = cancel.clone();
            let server_task = tokio::spawn(async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(canonical_control))
                    .add_service(IngestControlServer::new(ingest_control))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        server_cancel.cancelled_owned(),
                    )
                    .await
            });
            Ok(Self {
                endpoint,
                cancel,
                server_task,
            })
        }

        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            self.cancel.cancel();
            self.server_task.await??;
            Ok(())
        }
    }

    /// Canonical status of a regtest writer that follows its node with a
    /// confirmed tip.
    fn following_writer_status() -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            network_name: zinder_core::wire::encode_zinder_native_chain_name(Network::ZcashRegtest)
                .to_owned(),
            ..CanonicalWriterStatusResponse::default()
        }
    }

    #[derive(Clone)]
    struct MutableCanonicalControl {
        writer_status: Arc<Mutex<CanonicalWriterStatusResponse>>,
        /// Answers served once each, in order, before `writer_status`; `None`
        /// answers with an outage.
        queued: Arc<Mutex<std::collections::VecDeque<Option<CanonicalWriterStatusResponse>>>>,
        available: Arc<std::sync::atomic::AtomicBool>,
        /// Accepts each writer-status call and never answers it.
        stalled: Arc<std::sync::atomic::AtomicBool>,
        /// How long each writer-status call waits before it answers.
        answer_delay: Arc<Mutex<Duration>>,
    }

    impl MutableCanonicalControl {
        fn new(writer_status: CanonicalWriterStatusResponse) -> Self {
            Self {
                writer_status: Arc::new(Mutex::new(writer_status)),
                queued: Arc::new(Mutex::new(std::collections::VecDeque::new())),
                available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                stalled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                answer_delay: Arc::new(Mutex::new(Duration::ZERO)),
            }
        }

        fn stall(&self) {
            self.stalled
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn delay_answers(&self, delay: Duration) {
            *self.answer_delay.lock() = delay;
        }

        fn set_writer_status(&self, writer_status: CanonicalWriterStatusResponse) {
            *self.writer_status.lock() = writer_status;
        }

        /// Serves `answers` once each, in order, before the current status.
        fn queue(&self, answers: impl IntoIterator<Item = Option<CanonicalWriterStatusResponse>>) {
            self.queued.lock().extend(answers);
        }

        fn set_available(&self, available: bool) {
            self.available
                .store(available, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[tonic::async_trait]
    impl CanonicalControl for MutableCanonicalControl {
        async fn writer_status(
            &self,
            _request: Request<CanonicalWriterStatusRequest>,
        ) -> Result<Response<CanonicalWriterStatusResponse>, Status> {
            if self.stalled.load(std::sync::atomic::Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            let answer_delay = *self.answer_delay.lock();
            if !answer_delay.is_zero() {
                tokio::time::sleep(answer_delay).await;
            }
            if !self.available.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Status::unavailable("fixture writer status outage"));
            }
            let queued = self.queued.lock().pop_front();
            match queued {
                Some(Some(writer_status)) => Ok(Response::new(writer_status)),
                Some(None) => Err(Status::unavailable("fixture queued writer status outage")),
                None => Ok(Response::new(self.writer_status.lock().clone())),
            }
        }

        async fn event_page(
            &self,
            _request: Request<CanonicalEventPageRequest>,
        ) -> Result<Response<CanonicalEventPageResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }

        async fn create_owner_checkpoint(
            &self,
            _request: Request<CreateCanonicalOwnerCheckpointRequest>,
        ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }

        async fn readmit_owner_checkpoint(
            &self,
            _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
        ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }

        async fn acquire_projection_build_lease(
            &self,
            _request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }

        async fn renew_projection_build_lease(
            &self,
            _request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }

        async fn release_projection_build_lease(
            &self,
            _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<Response<ReleaseCanonicalProjectionBuildLeaseResponse>, Status> {
            Err(Status::unimplemented("fixture only serves writer status"))
        }
    }
}

#[cfg(test)]
mod retention_tests {
    use std::{collections::HashSet, error::Error, sync::Arc, time::Duration};

    use tokio::time::Instant;
    use zinder_core::{
        BlockHeight, ChainEpochId, Network, TransactionId, TransparentAddressScriptHash,
        TransparentOutPoint, TransparentSpendFact, TransparentUnspentOutput,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
        sample_regtest_upgrade_activations,
    };

    use super::{RETAINED_PAIR_WINDOW, WalletServingPairSlot};
    use crate::{
        QueryError, RocksDbCanonicalReader, RocksDbWalletReader,
        TransparentAddressUnspentOutputsRequest, WalletQueryApi, WalletServingQuery,
        WalletServingReadPair,
    };

    const SCRIPT_PUB_KEY: &[u8] = &[0x51];

    /// Outputs of one address: block 1 creates `kept` and `spent`, and block
    /// 2 spends `spent` and creates `created`.
    struct AddressHistory {
        chain: ChainFixture,
        address: TransparentAddressScriptHash,
        kept: TransparentOutPoint,
        spent: TransparentOutPoint,
        created: TransparentOutPoint,
    }

    #[tokio::test]
    async fn replaced_pair_answers_pinned_reads_until_released() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 3)?;
        let epochs: Vec<ChainEpochId> = pairs
            .iter()
            .map(|pair| pair.canonical_fence().chain_epoch_id())
            .collect();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let first_replaced_at = Instant::now();

        drop(slot.capture());
        assert!(
            slot.publish(Arc::clone(&pairs[1]), first_replaced_at)
                .is_empty()
        );
        let pinned = unspent_outpoints(&query, &history, Some(epochs[0])).await?;
        assert_eq!(
            pinned,
            (epochs[0], HashSet::from([history.kept, history.spent]))
        );
        let current = unspent_outpoints(&query, &history, None).await?;
        assert_eq!(
            current,
            (epochs[1], HashSet::from([history.kept, history.created]))
        );

        // Later publications retain every replaced pair instead of releasing
        // the oldest one.
        for (offset_seconds, pair) in [(1, &pairs[2]), (2, &pairs[3])] {
            drop(slot.capture());
            assert!(
                slot.publish(
                    Arc::clone(pair),
                    first_replaced_at + Duration::from_secs(offset_seconds)
                )
                .is_empty()
            );
        }
        assert_eq!(slot.retained_pair_count(), 3);
        for epoch in &epochs {
            assert_eq!(
                unspent_outpoints(&query, &history, Some(*epoch)).await?.0,
                *epoch
            );
        }

        let expired = slot.release_expired(first_replaced_at + RETAINED_PAIR_WINDOW);
        assert_eq!(expired.len(), 1);
        assert!(expired.iter().all(|pair| Arc::ptr_eq(pair, &pairs[0])));
        assert_pin_unavailable(&query, &history, epochs[0]).await?;
        assert_eq!(
            unspent_outpoints(&query, &history, Some(epochs[1]))
                .await?
                .0,
            epochs[1]
        );

        let expired =
            slot.release_expired(first_replaced_at + Duration::from_secs(1) + RETAINED_PAIR_WINDOW);
        assert_eq!(expired.len(), 1);
        assert!(expired.iter().all(|pair| Arc::ptr_eq(pair, &pairs[1])));
        assert_pin_unavailable(&query, &history, epochs[1]).await?;
        assert_eq!(
            unspent_outpoints(&query, &history, Some(epochs[2]))
                .await?
                .0,
            epochs[2]
        );
        assert_eq!(
            unspent_outpoints(&query, &history, Some(epochs[3]))
                .await?
                .0,
            epochs[3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn transparent_history_continuation_resumes_on_the_retained_pair_that_issued_it()
    -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 2)?;
        // The pair at height 2 holds both history entries.
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[1]));
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let first_page = query
            .transparent_address_tx_ids_in_range(history_request(&history, None), None)
            .await?;
        let cursor = first_page
            .next_cursor
            .ok_or("a one-entry page over two entries must issue a continuation")?;

        assert!(
            slot.publish(Arc::clone(&pairs[2]), Instant::now())
                .is_empty()
        );
        assert_ne!(
            query.visible_tip_block(None).await?.chain_epoch,
            first_page.chain_epoch
        );
        let second_page = query
            .transparent_address_tx_ids_in_range(history_request(&history, Some(cursor)), None)
            .await?;

        assert_eq!(second_page.chain_epoch, first_page.chain_epoch);
        assert_eq!(second_page.artifacts.len(), 1);
        assert_ne!(second_page.artifacts, first_page.artifacts);
        Ok(())
    }

    #[tokio::test]
    async fn retained_pair_stops_answering_pins_when_its_window_ends() -> Result<(), Box<dyn Error>>
    {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 1)?;
        let replaced_epoch = pairs[0].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let replaced_at = Instant::now()
            .checked_sub(RETAINED_PAIR_WINDOW)
            .ok_or("the monotonic clock must reach back one retention window")?;

        drop(slot.capture());
        assert!(slot.publish(Arc::clone(&pairs[1]), replaced_at).is_empty());
        assert_eq!(slot.retained_pair_count(), 1);
        assert_pin_unavailable(&query, &history, replaced_epoch).await?;
        Ok(())
    }

    /// A burst of publications seconds apart keeps a replaced pair answering
    /// its epoch's pins for exactly one retention window after it was
    /// replaced.
    #[tokio::test(start_paused = true)]
    async fn a_burst_keeps_a_replaced_epoch_pinnable_for_its_whole_window()
    -> Result<(), Box<dyn Error>> {
        const BLOCK_INTERVAL: Duration = Duration::from_secs(2);
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 3)?;
        let epochs: Vec<ChainEpochId> = pairs
            .iter()
            .map(|pair| pair.canonical_fence().chain_epoch_id())
            .collect();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));
        let replaced_at = Instant::now();
        for pair in &pairs[1..] {
            drop(slot.capture());
            assert!(slot.publish(Arc::clone(pair), Instant::now()).is_empty());
            tokio::time::advance(BLOCK_INTERVAL).await;
        }
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epochs[0])), &pairs[0]));

        let just_inside_window = RETAINED_PAIR_WINDOW
            .saturating_sub(replaced_at.elapsed())
            .saturating_sub(Duration::from_millis(1));
        tokio::time::advance(just_inside_window).await;
        assert!(
            Arc::ptr_eq(&slot.capture_at(Some(epochs[0])), &pairs[0]),
            "the pin answers until its window ends"
        );
        assert!(slot.release_expired(Instant::now()).is_empty());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(
            Arc::ptr_eq(&slot.capture_at(Some(epochs[0])), &pairs[3]),
            "a pin past its window falls through to the current pair"
        );
        let expired = slot.release_expired(Instant::now());
        assert_eq!(expired.len(), 1);
        assert!(expired.iter().all(|pair| Arc::ptr_eq(pair, &pairs[0])));
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epochs[1])), &pairs[1]));
        Ok(())
    }

    /// A pair captured before its replacement is retained for the window.
    #[test]
    fn a_capture_before_a_publication_keeps_its_pair_pinnable() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 1)?;
        let epoch = pairs[0].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));

        assert!(Arc::ptr_eq(&slot.capture(), &pairs[0]));
        assert!(
            slot.publish(Arc::clone(&pairs[1]), Instant::now())
                .is_empty()
        );
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epoch)), &pairs[0]));
        Ok(())
    }

    /// A capture after the publication observes the replacement, and the
    /// next publication retains it.
    #[test]
    fn a_capture_after_a_publication_observes_the_replacement() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 2)?;
        let epoch = pairs[1].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));

        let released = slot.publish(Arc::clone(&pairs[1]), Instant::now());
        assert!(released.iter().all(|pair| Arc::ptr_eq(pair, &pairs[0])));
        assert_eq!(released.len(), 1);
        assert!(Arc::ptr_eq(&slot.capture(), &pairs[1]));
        assert!(
            slot.publish(Arc::clone(&pairs[2]), Instant::now())
                .is_empty()
        );
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epoch)), &pairs[1]));
        Ok(())
    }

    /// A pin that the current pair answers observes it, because the caller
    /// holds an epoch that came from some response.
    #[test]
    fn an_explicit_pin_on_the_current_pair_keeps_it_pinnable() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 1)?;
        let epoch = pairs[0].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));

        assert!(Arc::ptr_eq(&slot.capture_at(Some(epoch)), &pairs[0]));
        assert!(
            slot.publish(Arc::clone(&pairs[1]), Instant::now())
                .is_empty()
        );
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epoch)), &pairs[0]));
        assert_eq!(slot.retained_pair_count(), 1);
        Ok(())
    }

    /// A pin that no pair answers, and an ephemeral capture, observe nothing.
    #[test]
    fn a_pin_no_pair_answers_and_an_ephemeral_capture_observe_nothing() -> Result<(), Box<dyn Error>>
    {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 1)?;
        let unserved = pairs[1].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));

        assert!(Arc::ptr_eq(&slot.capture_at(Some(unserved)), &pairs[0]));
        assert!(Arc::ptr_eq(&slot.capture_ephemeral(), &pairs[0]));
        let released = slot.publish(Arc::clone(&pairs[1]), Instant::now());
        assert_eq!(released.len(), 1);
        assert!(released.iter().all(|pair| Arc::ptr_eq(pair, &pairs[0])));
        assert_eq!(slot.retained_pair_count(), 0);
        Ok(())
    }

    /// Captures and publications racing on shared threads never lose a pin.
    ///
    /// Each round replays one race on a fresh slot, with a capture that
    /// observes by absence of a pin on even rounds and by an explicit pin on
    /// odd rounds. Whichever order the scheduler picks, the pair a capture
    /// returned still answers its epoch inside the window. The test counts no
    /// outcomes, so no scheduling frequency can fail it, and it records
    /// failures instead of asserting inside a round, so a failure cannot leave
    /// a thread waiting at the barrier.
    #[test]
    fn concurrent_captures_and_publications_never_lose_a_pin() -> Result<(), Box<dyn Error>> {
        const ROUNDS: usize = 20_000;
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 1)?;
        let first_epoch = pairs[0].canonical_fence().chain_epoch_id();
        let start = std::sync::Barrier::new(3);
        let finish = std::sync::Barrier::new(3);
        let slot_of_round = parking_lot::Mutex::new(None::<WalletServingPairSlot>);
        let captured_in_round = parking_lot::Mutex::new(None::<Arc<WalletServingReadPair>>);
        let mut failures = Vec::new();

        std::thread::scope(|scope| {
            scope.spawn(|| {
                for round in 0..ROUNDS {
                    start.wait();
                    let slot = slot_of_round.lock().clone();
                    *captured_in_round.lock() = slot.map(|slot| {
                        if round % 2 == 0 {
                            slot.capture()
                        } else {
                            slot.capture_at(Some(first_epoch))
                        }
                    });
                    finish.wait();
                }
            });
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    start.wait();
                    let slot = slot_of_round.lock().clone();
                    if let Some(slot) = slot {
                        drop(slot.publish(Arc::clone(&pairs[1]), Instant::now()));
                    }
                    finish.wait();
                }
            });
            for round in 0..ROUNDS {
                for pair in &pairs {
                    pair.clear_observation_for_test();
                }
                let slot = WalletServingPairSlot::new(Arc::clone(&pairs[0]));
                *slot_of_round.lock() = Some(slot.clone());
                start.wait();
                finish.wait();
                let captured = captured_in_round.lock().take();
                if let Some(captured) = captured {
                    let epoch = captured.canonical_fence().chain_epoch_id();
                    if !Arc::ptr_eq(&slot.capture_at(Some(epoch)), &captured) {
                        failures.push(round);
                    }
                }
            }
        });

        assert!(
            failures.is_empty(),
            "rounds {failures:?} lost the pin of the pair they captured"
        );
        Ok(())
    }

    /// A pair republished at its own epoch leaves no retained duplicate, and
    /// keeps the observation of the pair it replaces, so a client that holds
    /// the epoch can still pin it when the next block arrives.
    #[test]
    fn a_pair_republished_at_its_own_epoch_leaves_the_slot_at_once() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&history.chain).through(BlockHeight::new(1)),
            &sample_regtest_upgrade_activations(),
        )?;
        let first = serving_pair(&mut writer)?;
        let republished = serving_pair(&mut writer)?;
        let epoch = first.canonical_fence().chain_epoch_id();
        assert_eq!(republished.canonical_fence().chain_epoch_id(), epoch);
        let slot = WalletServingPairSlot::new(Arc::clone(&first));
        drop(slot.capture());

        let released = slot.publish(Arc::clone(&republished), Instant::now());
        assert_eq!(released.len(), 1);
        assert!(released.iter().all(|pair| Arc::ptr_eq(pair, &first)));
        assert_eq!(slot.retained_pair_count(), 0);

        writer.append_live_block(&history.chain)?;
        assert!(
            slot.publish(serving_pair(&mut writer)?, Instant::now())
                .is_empty()
        );
        assert!(
            Arc::ptr_eq(&slot.capture_at(Some(epoch)), &republished),
            "the republished pair answers its epoch once it is replaced"
        );
        Ok(())
    }

    /// A republished pair whose predecessor no request observed leaves with
    /// the next publication, because no client holds its epoch.
    #[test]
    fn an_unobserved_republished_pair_is_released_when_replaced() -> Result<(), Box<dyn Error>> {
        let history = address_history()?;
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&history.chain).through(BlockHeight::new(1)),
            &sample_regtest_upgrade_activations(),
        )?;
        let first = serving_pair(&mut writer)?;
        let republished = serving_pair(&mut writer)?;
        let slot = WalletServingPairSlot::new(Arc::clone(&first));

        assert_eq!(
            slot.publish(Arc::clone(&republished), Instant::now()).len(),
            1
        );
        writer.append_live_block(&history.chain)?;
        let released = slot.publish(serving_pair(&mut writer)?, Instant::now());
        assert_eq!(released.len(), 1);
        assert!(released.iter().all(|pair| Arc::ptr_eq(pair, &republished)));
        assert_eq!(slot.retained_pair_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn pinned_first_history_page_answers_from_the_retained_pair() -> Result<(), Box<dyn Error>>
    {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 2)?;
        // The pair at height 2 holds both history entries.
        let pinned_epoch = pairs[1].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[1]));
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());

        drop(slot.capture());
        assert!(
            slot.publish(Arc::clone(&pairs[2]), Instant::now())
                .is_empty()
        );
        let pinned_page = query
            .transparent_address_tx_ids_in_range(
                history_request(&history, None),
                Some(pinned_epoch),
            )
            .await?;
        let current_page = query
            .transparent_address_tx_ids_in_range(history_request(&history, None), None)
            .await?;

        assert_eq!(pinned_page.chain_epoch.id, pinned_epoch);
        assert_ne!(current_page.chain_epoch.id, pinned_epoch);
        Ok(())
    }

    async fn unspent_outpoints(
        query: &WalletServingQuery<()>,
        history: &AddressHistory,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<(ChainEpochId, HashSet<TransparentOutPoint>), QueryError> {
        let unspent = query
            .transparent_address_unspent_outputs(
                TransparentAddressUnspentOutputsRequest {
                    address_script_hash: history.address,
                    start_height: BlockHeight::new(0),
                },
                at_epoch_id,
            )
            .await?;
        let chain_epoch_id = unspent.chain_epoch.id;
        let outputs: Vec<_> = tokio_stream::StreamExt::collect::<Result<_, _>>(unspent).await?;
        Ok((
            chain_epoch_id,
            outputs.iter().map(|output| output.outpoint).collect(),
        ))
    }

    async fn assert_pin_unavailable(
        query: &WalletServingQuery<()>,
        history: &AddressHistory,
        chain_epoch_id: ChainEpochId,
    ) -> Result<(), Box<dyn Error>> {
        match unspent_outpoints(query, history, Some(chain_epoch_id)).await {
            Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: refused,
            }) if refused == chain_epoch_id => Ok(()),
            other => Err(format!("pin {chain_epoch_id:?} must be refused, got {other:?}").into()),
        }
    }

    fn history_request(
        history: &AddressHistory,
        from_cursor: Option<zinder_store::StreamCursorTokenV1>,
    ) -> crate::TransparentAddressTxIdsInRangeRequest {
        crate::TransparentAddressTxIdsInRangeRequest {
            address_script_hash: history.address,
            start_height: BlockHeight::new(0),
            end_height: BlockHeight::new(3),
            max_entries: std::num::NonZeroU32::MIN,
            from_cursor,
        }
    }

    /// Returns the pair admitted at the baseline (height 1) and one more for
    /// each of `live_appends` blocks appended after it.
    fn serving_pairs_through_live_appends(
        chain: &ChainFixture,
        live_appends: usize,
    ) -> Result<Vec<Arc<WalletServingReadPair>>, Box<dyn Error>> {
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(chain).through(BlockHeight::new(1)),
            &sample_regtest_upgrade_activations(),
        )?;
        let mut pairs = Vec::with_capacity(live_appends + 1);
        pairs.push(serving_pair(&mut writer)?);
        for _ in 0..live_appends {
            writer.append_live_block(chain)?;
            pairs.push(serving_pair(&mut writer)?);
        }
        Ok(pairs)
    }

    fn serving_pair(
        writer: &mut CanonicalWriterFixture,
    ) -> Result<Arc<WalletServingReadPair>, Box<dyn Error>> {
        let (canonical, wallet) = writer.serving_store_fixture()?.take_readers()?;
        Ok(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?))
    }

    fn address_history() -> Result<AddressHistory, Box<dyn Error>> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
        let block_one = chain
            .block_at(BlockHeight::new(1))
            .cloned()
            .ok_or("fixture must contain height 1")?;
        let block_two = chain
            .block_at(BlockHeight::new(2))
            .cloned()
            .ok_or("fixture must contain height 2")?;
        let address = TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY);
        let creator = TransactionId::from_bytes([0x10; 32]);
        let spender = TransactionId::from_bytes([0x20; 32]);
        let kept = TransparentOutPoint::new(creator, 0);
        let spent = TransparentOutPoint::new(creator, 1);
        let created = TransparentOutPoint::new(spender, 0);
        let output = |outpoint: TransparentOutPoint, block: &zinder_testkit::FixtureBlock| {
            TransparentUnspentOutput::new(
                address,
                SCRIPT_PUB_KEY.to_vec(),
                outpoint,
                1_000,
                block.height,
                block.hash,
            )
        };
        let chain = chain
            .with_transaction_rows(FixtureTransactionRows::coinbase(
                TransactionId::from_bytes([0x2C; 32]),
                block_two.height,
                block_two.hash,
                b"block-two-coinbase".to_vec(),
            ))
            .with_address_output_index(output(kept, &block_one))
            .with_address_output_index(output(spent, &block_one))
            .with_address_output_index(output(created, &block_two))
            .with_transparent_spend_fact(TransparentSpendFact::new(
                spent,
                0,
                spender,
                1,
                block_two.height,
                block_two.hash,
                1_000,
                address,
                block_one.height,
                block_one.hash,
            ));
        Ok(AddressHistory {
            chain,
            address,
            kept,
            spent,
            created,
        })
    }
}

#[cfg(test)]
mod teardown_tests {
    use super::{WalletServingPairTeardownError, tear_down_on_blocking_thread};

    /// Stands in for a publisher whose secondaries panic while closing.
    struct PanicsWhenDropped;

    impl Drop for PanicsWhenDropped {
        #[allow(clippy::panic, reason = "the test needs a teardown that panics")]
        fn drop(&mut self) {
            panic!("closing a secondary panicked");
        }
    }

    #[tokio::test]
    async fn a_teardown_that_panics_is_an_error() {
        let teardown = tear_down_on_blocking_thread(PanicsWhenDropped).await;

        assert!(
            teardown.is_err(),
            "a panicking teardown must fail the publisher task"
        );
    }

    #[tokio::test]
    async fn a_clean_teardown_succeeds() -> Result<(), WalletServingPairTeardownError> {
        tear_down_on_blocking_thread(()).await
    }
}
