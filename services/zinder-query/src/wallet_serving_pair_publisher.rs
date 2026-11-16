//! Wallet-serving secondary-pair lifecycle shared by native and compatibility runtimes.
//!
//! Each reader runtime owns no canonical or wallet primary handle. It catches
//! up only an inactive generation, authenticates that generation against the
//! canonical writer control plane, and atomically publishes it as one
//! immutable canonical/wallet pair. A replaced pair keeps answering reads
//! pinned to its chain epoch for one retention window, however fast later
//! blocks arrive: while every spare generation still holds such a pair, the
//! publisher publishes nothing and then publishes the writer's latest fence,
//! so blocks in a burst share one publication. Each generation stays
//! frozen until every in-flight request has dropped its captured pair `Arc`;
//! the publisher then catches that same open generation up for a later
//! publication, so a slot is opened cold only on its first use or after a
//! failed catch-up. A request that holds an old pair for a long time pins
//! only that generation: the publisher keeps refreshing into the other
//! bounded generation slots.

use std::{
    borrow::Cow,
    num::{NonZeroU8, NonZeroU32},
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::{
    AdmittedIngestControl, CanonicalReader, WalletProjectionReader, WalletServingAdmissionError,
    WalletServingReadPair, ingest_control::wallet_ingest_control_request,
};
use arc_swap::ArcSwap;
use thiserror::Error;
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use zinder_core::{ChainEpochId, Network, NetworkUpgradeActivations};
use zinder_proto::{
    v1::ingest::{
        CanonicalWriterFence, CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
        canonical_control_client::CanonicalControlClient,
    },
    wire::{canonical_writer_fence_message, decode_canonical_network_upgrade_schedule},
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, CanonicalFenceConvergence,
    CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy, CanonicalFenceDivergence,
    CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation, ComposedReadiness,
    NodeUnavailableDetail, Readiness, ReadinessCause, ReadinessInput, ReadinessState,
    UpstreamHealth, UpstreamNotReadyDetail, WriterAttestation, classify_canonical_writer_fence,
    connect_zinder_grpc, converge_on_canonical_writer_fence,
    writer_attests_network_upgrade_schedule,
};
use zinder_source::{NodeCapability, NodeSource, SourceError, UpstreamHealthSnapshot};
use zinder_store::{
    CanonicalActivationSchedule, CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalSecondary, RocksDbResourceBudget,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{RocksDbWalletError, RocksDbWalletSecondary};

/// How long a replaced pair keeps answering reads pinned to its epoch.
///
/// The window is a guarantee as well as a bound. Publication never releases
/// a replaced pair before its window ends, so a pin on its epoch is answered
/// for exactly this long after the pair was replaced, however many blocks
/// arrive meanwhile.
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
const CONVERGENCE_RETRY_DELAY_CAP: Duration = Duration::from_millis(100);
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
    /// Network both stores and writer status must attest.
    pub network: Network,
    /// Network upgrade activation table used to open canonical storage.
    pub network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    /// Persisted raw-blob retention every published canonical pair must attest.
    pub expected_raw_blob_retention: RawBlobRetention,
    /// Canonical replacement-depth identity expected from the writer.
    pub canonical_reorg_policy: CanonicalReorgPolicy,
    /// `RocksDB` budget for each canonical secondary generation.
    pub canonical_resource_budget: RocksDbResourceBudget,
    /// `RocksDB` budget for each wallet secondary generation.
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
/// Each replaced pair keeps answering reads pinned to its chain epoch for one
/// minute after it was replaced, however many pairs are published meanwhile.
#[derive(Clone)]
pub struct WalletServingPairSlot {
    published: Arc<ArcSwap<PublishedPairs>>,
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
        }
    }

    /// Captures the exact pair to retain for one request.
    #[must_use]
    pub fn capture(&self) -> Arc<WalletServingReadPair> {
        Arc::clone(&self.published.load().current)
    }

    /// Captures the pair that answers a read pinned to `chain_epoch_id`.
    ///
    /// A retained pair answers a pin that names its epoch until its retention
    /// window ends, even before the publisher releases it. Every other pin,
    /// and an absent one, captures the current pair, so the caller's epoch
    /// check still reports a pin that no pair serves.
    #[must_use]
    pub(crate) fn capture_at(
        &self,
        chain_epoch_id: Option<ChainEpochId>,
    ) -> Arc<WalletServingReadPair> {
        let published = self.published.load();
        let serves = |pair: &WalletServingReadPair| {
            chain_epoch_id.is_some_and(|id| pair.canonical_fence().chain_epoch_id() == id)
        };
        if serves(&published.current) {
            return Arc::clone(&published.current);
        }
        let now = Instant::now();
        published
            .retained
            .iter()
            .find(|retained| retained.is_within_window(now) && serves(&retained.pair))
            .map_or_else(
                || Arc::clone(&published.current),
                |retained| Arc::clone(&retained.pair),
            )
    }

    fn retained_pair_count(&self) -> usize {
        self.published.load().retained.len()
    }

    /// Makes `pair` current and retains the pair it replaces.
    ///
    /// Returns the pairs that leave the slot: only retained pairs at `pair`'s
    /// epoch, which `pair` now answers. A schedule revision republishes the
    /// current epoch, so the pair it replaces leaves at once instead of
    /// keeping a generation that no pin can reach. Every other retained pair
    /// stays until its window ends. Only the publisher writes the slot, so
    /// the load and store below cannot race another publication.
    fn publish(
        &self,
        pair: Arc<WalletServingReadPair>,
        replaced_at: Instant,
    ) -> Vec<Arc<WalletServingReadPair>> {
        let previous = self.published.load_full();
        let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
        let (released, retained): (Vec<_>, Vec<_>) = std::iter::once(RetainedPair {
            pair: Arc::clone(&previous.current),
            replaced_at,
        })
        .chain(previous.retained.iter().cloned())
        .partition(|retained| retained.pair.canonical_fence().chain_epoch_id() == chain_epoch_id);
        self.published.store(Arc::new(PublishedPairs {
            current: pair,
            retained,
        }));
        released.into_iter().map(|released| released.pair).collect()
    }

    /// Releases the retained pairs replaced at least [`RETAINED_PAIR_WINDOW`]
    /// before `now`, returning them.
    fn release_expired(&self, now: Instant) -> Vec<Arc<WalletServingReadPair>> {
        let previous = self.published.load_full();
        let (retained, expired): (Vec<_>, Vec<_>) = previous
            .retained
            .iter()
            .cloned()
            .partition(|retained| retained.is_within_window(now));
        if expired.is_empty() {
            return Vec::new();
        }
        self.published.store(Arc::new(PublishedPairs {
            current: Arc::clone(&previous.current),
            retained,
        }));
        expired.into_iter().map(|expired| expired.pair).collect()
    }
}

/// Conjunctive readiness owner for one wallet-serving runtime.
///
/// Pair lifecycle, node-source health, and ingest-control health publish
/// independent inputs to one [`ComposedReadiness`]. The existing runtime
/// [`Readiness`] remains the single operations and gRPC projection, but no
/// input writer can erase another input's failure.
#[derive(Clone, Debug)]
pub struct WalletServingReadiness {
    composed: ComposedReadiness,
}

impl WalletServingReadiness {
    /// Starts with the serving pair, node source, and ingest control unadmitted.
    #[must_use]
    pub fn awaiting_node_and_ingest_control(runtime: Readiness) -> Self {
        Self::new(
            runtime,
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        )
    }

    /// Starts with both the serving pair and required node source unadmitted.
    ///
    /// Compatibility runtimes that own a separate live-state readiness
    /// contract use this constructor; native query uses
    /// [`Self::awaiting_node_and_ingest_control`].
    #[must_use]
    pub fn awaiting_node_source(runtime: Readiness) -> Self {
        Self::new(runtime, &[ReadinessInput::NodeSource])
    }

    /// Starts a storage-only composition whose node contribution is already satisfied.
    #[must_use]
    pub fn without_node_source(runtime: Readiness) -> Self {
        Self::new(runtime, &[])
    }

    fn new(runtime: Readiness, required_inputs: &[ReadinessInput]) -> Self {
        Self {
            composed: ComposedReadiness::new(runtime, required_inputs),
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

    fn publish_node_source_cause(&self, node_source_cause: ReadinessCause) {
        self.composed
            .publish_input(ReadinessInput::NodeSource, node_source_cause);
    }

    fn publish_ingest_control_cause(&self, ingest_control_cause: ReadinessCause) {
        self.composed
            .publish_input(ReadinessInput::IngestControl, ingest_control_cause);
    }
}

/// Starts the health probe for the exact admitted ingest-control channel.
///
/// Each observation reaches both `WriterStatus` and a bounded
/// `MempoolSnapshot` through the same authenticated channel used by pair
/// publication and live wallet operations. Structural capabilities remain
/// immutable while a transient failure drains readiness.
pub fn spawn_wallet_ingest_control_readiness_probe(
    ingest_control: AdmittedIngestControl,
    readiness: WalletServingReadiness,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_health_was_available = None;
        loop {
            let health_outcome = tokio::select! {
                () = cancel.cancelled() => break,
                outcome = ingest_control.probe_health() => outcome,
            };
            let cause = match health_outcome {
                Ok(()) => {
                    if last_health_was_available == Some(false) {
                        tracing::info!(
                            target: "zinder::query",
                            event = "ingest_control_health_recovered",
                            "admitted ingest-control health recovered"
                        );
                    }
                    last_health_was_available = Some(true);
                    ReadinessCause::Ready
                }
                Err(error) => {
                    if last_health_was_available != Some(false) {
                        tracing::warn!(
                            target: "zinder::query",
                            event = "ingest_control_health_unavailable",
                            error_class = error.class(),
                            error = %error,
                            "admitted ingest-control health became unavailable"
                        );
                    }
                    last_health_was_available = Some(false);
                    error.readiness_cause()
                }
            };
            readiness.publish_ingest_control_cause(cause);

            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {}
            }
        }
    })
}

/// Starts the liveness probe for the exact node source admitted by a wallet query.
///
/// Capability discovery remains a startup-only structural operation. This
/// task observes liveness through `tip_id` and upstream sync health without
/// mutating the source's cached capability set.
pub fn spawn_wallet_node_readiness_probe<Source>(
    source: Source,
    native_endpoint_capabilities: &crate::NativeWalletEndpointCapabilities,
    readiness: WalletServingReadiness,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, crate::QueryError>
where
    Source: NodeSource,
{
    if !native_endpoint_capabilities.has_node_backed_capabilities() {
        readiness.publish_node_source_cause(ReadinessCause::Ready);
        return Ok(tokio::spawn(async move {
            cancel.cancelled().await;
        }));
    }
    if !source.capabilities().supports(NodeCapability::TipId) {
        return Err(crate::QueryError::Node(
            SourceError::NodeCapabilityMissing {
                capability: NodeCapability::TipId,
            },
        ));
    }

    Ok(tokio::spawn(async move {
        let mut outage = None;
        loop {
            let observation = tokio::select! {
                () = cancel.cancelled() => break,
                observation = probe_wallet_node_readiness(&source) => observation,
            };
            match observation {
                Ok(snapshot) if snapshot.ready_for_queries => {
                    outage = None;
                    readiness.publish_node_source_cause(ReadinessCause::Ready);
                }
                Ok(snapshot) => {
                    outage = None;
                    readiness.publish_node_source_cause(ReadinessCause::UpstreamNotReady(
                        upstream_not_ready_detail(snapshot),
                    ));
                }
                Err(SourceError::NodeCapabilityMissing { capability }) => {
                    outage = None;
                    readiness.publish_node_source_cause(ReadinessCause::NodeCapabilityMissing {
                        capability: capability.name(),
                    });
                }
                Err(error) => {
                    let detail = node_unavailable_detail(&error, outage.as_ref());
                    outage = Some(WalletNodeOutage {
                        started_at: outage
                            .as_ref()
                            .map_or_else(Instant::now, |prior| prior.started_at),
                        detail: detail.clone(),
                    });
                    readiness.publish_node_source_cause(ReadinessCause::NodeUnavailable(detail));
                }
            }

            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {}
            }
        }
    }))
}

async fn probe_wallet_node_readiness(
    source: &impl NodeSource,
) -> Result<UpstreamHealthSnapshot, SourceError> {
    source.tip_id().await?;
    source.poll_upstream_health().await
}

#[derive(Clone, Debug)]
struct WalletNodeOutage {
    started_at: Instant,
    detail: NodeUnavailableDetail,
}

fn node_unavailable_detail(
    error: &SourceError,
    prior_outage: Option<&WalletNodeOutage>,
) -> NodeUnavailableDetail {
    let failure_class = error.upstream_classification().label();
    let last_reason = Cow::Owned(truncate_readiness_reason(&error.to_string()));
    if let Some(prior_outage) = prior_outage {
        let outage_seconds =
            u32::try_from(prior_outage.started_at.elapsed().as_secs()).unwrap_or(u32::MAX);
        NodeUnavailableDetail::extend_with(
            &prior_outage.detail,
            failure_class,
            last_reason,
            outage_seconds,
        )
    } else {
        NodeUnavailableDetail::first_iteration(failure_class, last_reason)
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
            source: snapshot.source,
            reason: snapshot.reason,
        },
    }
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
    /// The compatibility runtime could not connect to canonical writer status.
    ///
    /// P3 replaces this endpoint-only path with compatibility-specific
    /// ingest-control admission.
    #[error(transparent)]
    WriterStatusConnect(#[from] BearerTokenConnectError),
    /// The private canonical-control RPC failed.
    #[error("canonical writer-status RPC failed: {0}")]
    WriterStatusRpc(tonic::Status),
    /// Writer status did not contain an exact, usable canonical fence or
    /// network-upgrade schedule.
    #[error("canonical writer-status response did not contain a valid fence or schedule")]
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

/// Owns the bounded reader generations and their publication slot.
pub struct WalletServingPairPublisher {
    config: WalletServingPairConfig,
    writer_status: CanonicalWriterStatusClient,
    readiness: WalletServingReadiness,
    serving_pair_slot: Option<WalletServingPairSlot>,
    generations: [SecondaryGeneration; SECONDARY_GENERATION_COUNT],
    published_generation: Option<usize>,
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
    /// Activation table each new canonical secondary generation opens with.
    ///
    /// It starts as the node's table and then follows the table the writer
    /// reports, so a publisher started before a schedule admission keeps
    /// opening generations after the chain crosses the admitted activation.
    secondary_activations: Arc<NetworkUpgradeActivations>,
    #[cfg(test)]
    candidate_preparations: CandidatePreparationCounts,
}

impl WalletServingPairPublisher {
    /// Bootstraps the current compatibility pair from its writer-status endpoint.
    ///
    /// This preserves the pre-P3 compatibility composition without applying
    /// native wallet admission requirements to it.
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

    /// Bootstraps the native pair through its already-admitted ingest channel.
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
        let secondary_activations = Arc::clone(&config.network_upgrade_activations);
        let writer_attestation =
            WriterAttestation::new(config.network, config.serving_pair_staleness_ceiling);
        let mut publisher = Self {
            config,
            writer_status,
            readiness,
            serving_pair_slot: None,
            generations,
            published_generation: None,
            retired_pairs: Vec::with_capacity(SECONDARY_GENERATION_COUNT),
            writer_attestation,
            observed_lag_chain_epochs: 0,
            secondary_activations,
            #[cfg(test)]
            candidate_preparations: CandidatePreparationCounts::default(),
        };
        publisher.prepare_candidate(0).await?;
        publisher.converge_and_publish(0).await?;
        let Some(serving_pair_slot) = publisher.serving_pair_slot.clone() else {
            return Err(WalletServingPairError::PairSlotUnavailable);
        };
        Ok((publisher, serving_pair_slot))
    }

    /// Runs bounded refreshes until shutdown, then retains reader ownership
    /// until the serving runtime confirms its request paths have drained.
    ///
    /// Every refresh failure leaves the current immutable pair untouched and
    /// updates readiness with its typed cause; it never falls back to a
    /// primary store.
    #[must_use = "await the publisher on shutdown so secondary handles close before process exit"]
    pub fn spawn(
        mut self,
        cancel: CancellationToken,
        serving_runtime_drained: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(self.config.catchup_interval) => {
                        if let Err(error) = self.refresh_once().await {
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
                                    error = %error,
                                    lag_chain_epochs = self.observed_lag_chain_epochs,
                                    "inactive wallet-serving pair refresh failed; retaining the prior pair"
                                );
                            }
                        }
                    }
                }
            }
            serving_runtime_drained.cancelled().await;
            let started_at = Instant::now();
            let teardown = tokio::task::spawn_blocking(move || drop(self)).await;
            metrics::histogram!(
                "zinder_wallet_serving_pair_publisher_teardown_duration_seconds",
                "status" => if teardown.is_ok() { "ok" } else { "error" }
            )
            .record(started_at.elapsed());
            if let Err(error) = teardown {
                tracing::warn!(
                    target: "zinder::wallet_serving",
                    event = "wallet_serving_pair_publisher_teardown_failed",
                    error = %error,
                    "wallet-serving publisher teardown task failed"
                );
            }
        })
    }

    async fn refresh_once(&mut self) -> Result<(), WalletServingPairError> {
        let now = Instant::now();
        self.release_expired_pairs(now);
        self.reap_retired_pairs(now).await;
        let serving_pair_slot = self
            .serving_pair_slot
            .as_ref()
            .ok_or(WalletServingPairError::PairSlotUnavailable)?;
        let active_pair = serving_pair_slot.capture();
        let writer_status = self.writer_status.fetch().await?;
        let relation = self.update_active_readiness(&active_pair, &writer_status)?;
        if matches!(
            relation,
            ActiveWriterRelation::Exact | ActiveWriterRelation::Refusing
        ) {
            return Ok(());
        }
        self.follow_writer_schedule(&writer_status)?;

        // Waiting keeps every retained pair pinnable for its whole window. A
        // later refresh publishes the writer's fence at that time, so the
        // blocks that arrive meanwhile share one publication.
        let Some(generation) = self.inactive_generation() else {
            self.record_generation_wait();
            return Ok(());
        };
        self.prepare_candidate(generation).await?;
        self.converge_and_publish(generation).await
    }

    /// Counts a refresh that found every unpublished slot taken and published
    /// nothing.
    ///
    /// `retention_window` means a slot holds a replaced pair whose window has
    /// not ended, so the wait ends by the time that window does at the latest.
    /// `in_flight_requests` means only released pairs that requests still
    /// hold take the slots.
    fn record_generation_wait(&self) {
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

    /// Adopts the writer-reported schedule for later secondary generations.
    ///
    /// Secondary admission compares the opening table with the admitted
    /// schedule through the visible tip. The node table observed at startup
    /// stops agreeing once the chain crosses an activation the writer admitted
    /// afterwards, so later generations open with the writer's table instead.
    fn follow_writer_schedule(
        &mut self,
        writer_status: &CanonicalWriterStatusResponse,
    ) -> Result<(), WalletServingPairError> {
        let Some(schedule) = writer_status.network_upgrade_schedule.clone() else {
            return Ok(());
        };
        let (activations, _revision) =
            decode_canonical_network_upgrade_schedule(schedule, self.config.network)
                .map_err(|_| WalletServingPairError::WriterStatusInvalid)?;
        self.secondary_activations = Arc::new(activations);
        Ok(())
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
            .find(|generation| matches!(state(*generation), SecondaryGenerationState::Candidate { .. }))
            .or_else(|| {
                unpublished().find(|generation| {
                    matches!(state(*generation), SecondaryGenerationState::Published { lease, .. } if lease.is_reusable())
                })
            })
            .or_else(|| {
                unpublished()
                    .find(|generation| matches!(state(*generation), SecondaryGenerationState::Vacant))
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
        let network_upgrade_activations = Arc::clone(&self.secondary_activations);
        let canonical_reorg_policy = self.config.canonical_reorg_policy;
        let expected_raw_blob_retention = self.config.expected_raw_blob_retention;
        let canonical_resource_budget = self.config.canonical_resource_budget;
        let wallet_resource_budget = self.config.wallet_resource_budget;
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
            let canonical = RocksDbCanonicalSecondary::open_ready(
                canonical_primary_path,
                canonical_secondary_path,
                network_upgrade_activations.as_ref(),
                CanonicalStoreWorkload::Wallet,
                expected_raw_blob_retention,
                canonical_reorg_policy,
                canonical_resource_budget,
            )?;
            let wallet = RocksDbWalletSecondary::open_ready(
                wallet_primary_path,
                wallet_secondary_path,
                network,
                wallet_resource_budget,
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
            let convergence = match WalletServingReadPair::validate_readers(
                &candidate.canonical,
                &candidate.wallet,
            ) {
                Ok(()) => Ok(()),
                Err(error) => Err(classify_pair_admission(&error)),
            };
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
        let (pair, readers) = tokio::task::spawn_blocking(move || {
            let readers = PublishedReaders::new(*candidate);
            let canonical: Arc<dyn CanonicalReader> = readers.canonical.clone();
            let wallet: Arc<dyn WalletProjectionReader> = readers.wallet.clone();
            WalletServingReadPair::new(canonical, wallet)
                .map(|pair| (Arc::new(pair), readers))
                .map_err(WalletServingPairError::PairPublication)
        })
        .await
        .map_err(WalletServingPairError::CandidateTask)??;
        if let Some(slot) = &self.serving_pair_slot {
            let now = Instant::now();
            let released = slot.publish(Arc::clone(&pair), now);
            record_retained_pair_count(slot);
            self.retire_pairs(released, now);
        } else {
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
            .publish_pair_state(refusal.unwrap_or_else(|| ReadinessState::ready(visible_height)));
        metrics::counter!("zinder_wallet_serving_pair_publisher_publications_total").increment(1);
        tracing::info!(
            target: "zinder::wallet_serving",
            event = "wallet_serving_pair_publisher_published",
            generation,
            chain_epoch = pair.canonical_fence().chain_epoch_id().value(),
            event_sequence = pair.canonical_fence().chain_event_sequence(),
            visible_height = pair.canonical_fence().visible_tip().height.value(),
            "published exact immutable canonical and wallet secondary pair"
        );
        Ok(())
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
                self.writer_attestation.attest();
                self.observed_lag_chain_epochs = 0;
                self.readiness
                    .publish_pair_state(ReadinessState::ready(visible_height));
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
        if lag > self.config.replica_lag_threshold_chain_epochs {
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

    /// Publishes the readiness a failed refresh leaves behind and returns
    /// whether that readiness still admits traffic on the published pair.
    fn record_refresh_failure(&self, error: &WalletServingPairError) -> bool {
        let visible_height = self.serving_pair_slot.as_ref().map(|serving_pair_slot| {
            serving_pair_slot
                .capture()
                .canonical_fence()
                .visible_tip()
                .height
                .value()
        });
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
/// The pair reads through unsized clones of these handles. Keeping them here
/// lets the publisher take back sole ownership once the pair drains, and only
/// then catch the same open secondaries up for a later candidate.
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
        | WalletServingAdmissionError::CanonicalRead { .. }
        | WalletServingAdmissionError::CanonicalFenceMismatch => {
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
/// Only lag and control-plane transport failures qualify: they describe the
/// candidate generation or the status RPC, never the published pair. Every
/// failure that impugns the pair itself, its schema, or the slot holding it is
/// excluded, so those keep failing closed the moment they occur.
fn refresh_failure_retains_attested_pair(error: &WalletServingPairError) -> bool {
    matches!(
        error,
        WalletServingPairError::WriterStatusConnect(_)
            | WalletServingPairError::WriterStatusRpc(_)
            | WalletServingPairError::ConvergenceTimedOut {
                last_outcome: WalletServingConvergence::ReplicaBehind
                    | WalletServingConvergence::ProjectionBehind,
            }
    )
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
        path::Path,
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
        CanonicalStoreBuildPlan, CanonicalStoreWorkload, EventStreamStartPosition,
        RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
        RocksDbCanonicalStore, RocksDbResourceBudget, StreamCursorTokenV1,
        event_stream_start_message,
    };
    use zinder_wallet_projection::{WalletCanonicalSourceIdentity, WalletProjectionSourcePosition};
    use zinder_wallet_rocksdb::{
        RocksDbWalletBuildOptions, RocksDbWalletStore, build_wallet_from_canonical,
    };

    use super::{
        CandidatePreparationCounts, CanonicalFenceRelation, RETAINED_PAIR_WINDOW,
        SECONDARY_GENERATION_COUNT, SecondaryGenerationState, WalletServingConvergence,
        WalletServingPairConfig, WalletServingPairError, WalletServingPairPublisher,
        WalletServingPairSlot, WalletServingReadiness, classify_canonical_writer_fence,
        classify_pair_admission, refresh_failure_is_routine_lag, refresh_failure_not_ready_cause,
        refresh_failure_retains_attested_pair, spawn_wallet_ingest_control_readiness_probe,
        spawn_wallet_node_readiness_probe, wallet_ingest_control_request,
        wallet_source_writer_fence,
    };
    use crate::{
        AdmittedIngestControl, NativeWalletEndpointCapabilities, WalletEndpointMetadata,
        WalletQueryApi, WalletQueryGrpcAdapter, WalletServingQuery,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixture,
        IngestControlFixtureService, LogCapture, sample_regtest_upgrade_activations,
    };

    #[test]
    fn wallet_serving_readiness_retains_pair_node_and_ingest_failures_independently() {
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
        let visible_height = Some(42);
        let node_failure = zinder_runtime::ReadinessCause::NodeUnavailable(
            zinder_runtime::NodeUnavailableDetail::first_iteration(
                "node_unreachable",
                "test node outage",
            ),
        );

        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::Starting
        ));

        readiness.publish_node_source_cause(node_failure.clone());
        assert_eq!(runtime.report().cause, node_failure);
        readiness
            .publish_ingest_control_cause(zinder_runtime::ReadinessCause::IngestControlUnavailable);
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert_eq!(runtime.report().cause, node_failure);

        readiness.publish_pair_state(zinder_runtime::ReadinessState::replica_lagging(
            2,
            visible_height,
        ));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 2
            }
        ));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 2
            }
        ));

        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::IngestControlUnavailable
        ));
        readiness.publish_ingest_control_cause(zinder_runtime::ReadinessCause::Ready);
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::Ready
        ));
        readiness.publish_node_source_cause(node_failure.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::not_ready(
            zinder_runtime::ReadinessCause::StorageUnavailable,
        ));
        assert!(matches!(
            runtime.report().cause,
            zinder_runtime::ReadinessCause::StorageUnavailable
        ));
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(visible_height));
        assert_eq!(runtime.report().cause, node_failure);

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
    fn concurrent_recovery_cannot_erase_another_readiness_input_failure() {
        const ITERATIONS: usize = 2_048;

        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
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
                assert!(
                    matches!(
                        runtime.report().cause,
                        zinder_runtime::ReadinessCause::IngestControlUnavailable
                    ),
                    "concurrent node recovery erased ingest failure at iteration {iteration}"
                );
            }
        });
    }

    #[tokio::test]
    async fn wallet_node_probe_drains_and_recovers_without_changing_capabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::new()?;
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            source.capabilities(),
            &admitted_ingest_control,
        );
        let admitted_capabilities = capabilities.clone();
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_source(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            source.clone(),
            &capabilities,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        )?;

        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());
        source.available.store(false, Ordering::SeqCst);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::NodeUnavailable(_))
        })
        .await?;
        assert!(matches!(
            traffic_gate.call(Request::new(())),
            Err(status) if status.code() == Code::Unavailable
        ));
        assert_eq!(capabilities, admitted_capabilities);

        source.available.store(true, Ordering::SeqCst);
        source.synchronized.store(false, Ordering::SeqCst);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::UpstreamNotReady(_))
        })
        .await?;
        assert!(matches!(
            traffic_gate.call(Request::new(())),
            Err(status) if status.code() == Code::Unavailable
        ));
        assert_eq!(capabilities, admitted_capabilities);

        source.synchronized.store(true, Ordering::SeqCst);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());
        assert_eq!(capabilities, admitted_capabilities);

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_node_probe_stops_while_a_node_request_is_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = MutableHealthSource::new()?;
        source.stalled.store(true, Ordering::SeqCst);
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            source.capabilities(),
            &admitted_ingest_control,
        );
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_source(runtime);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_node_readiness_probe(
            source.clone(),
            &capabilities,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        )?;

        source.stalled_request_started.notified().await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle).await??;
        ingest_fixture.shutdown().await?;
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
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
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
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness.clone(),
            Duration::from_millis(5),
            cancel.clone(),
        );
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
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

        ingest_fixture.set_reorg_window_exceeded(None);
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_ingest_control_probe_drains_for_hydration_and_recovers()
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
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_mempool_snapshot_error(Some(Status::unavailable(
            "fixture mempool hydration is unavailable",
        )));
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(
                cause,
                zinder_runtime::ReadinessCause::IngestControlUnavailable
            )
        })
        .await?;
        assert!(matches!(
            traffic_gate.call(Request::new(())),
            Err(status) if status.code() == Code::Unavailable
        ));

        ingest_fixture.set_mempool_snapshot_error(None);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_mempool_snapshot_error(Some(stale_status));
        ingest_fixture.set_health_available(false);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(
                cause,
                zinder_runtime::ReadinessCause::IngestControlUnavailable
            )
        })
        .await?;
        assert!(matches!(
            traffic_gate.call(Request::new(())),
            Err(status) if status.code() == Code::Unavailable
        ));

        ingest_fixture.set_health_available(true);
        ingest_fixture.set_mempool_snapshot_error(None);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        assert!(traffic_gate.call(Request::new(())).is_ok());

        cancel.cancel();
        handle.await?;
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_ingest_control_probe_drains_and_recovers_without_changing_capabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let log_capture = LogCapture::install_for_target("zinder::query");
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted_ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            NodeCapabilities::default(),
            &admitted_ingest_control,
        );
        let admitted_capabilities = capabilities.clone();
        let runtime = zinder_runtime::Readiness::default();
        let readiness = WalletServingReadiness::awaiting_node_and_ingest_control(runtime.clone());
        readiness.publish_pair_state(zinder_runtime::ReadinessState::ready(Some(1)));
        readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let cancel = CancellationToken::new();
        let handle = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            readiness,
            Duration::from_millis(5),
            cancel.clone(),
        );

        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
        let mut traffic_gate = zinder_runtime::TrafficReadinessInterceptor::new(runtime.clone());
        assert!(traffic_gate.call(Request::new(())).is_ok());

        ingest_fixture.set_health_available(false);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(
                cause,
                zinder_runtime::ReadinessCause::IngestControlUnavailable
            )
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            traffic_gate.call(Request::new(())),
            Err(status) if status.code() == Code::Unavailable
        ));
        assert_eq!(capabilities, admitted_capabilities);

        ingest_fixture.set_health_available(true);
        wait_for_readiness_cause(&runtime, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
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

    #[derive(Clone)]
    struct MutableHealthSource {
        available: Arc<AtomicBool>,
        synchronized: Arc<AtomicBool>,
        stalled: Arc<AtomicBool>,
        stalled_request_started: Arc<tokio::sync::Notify>,
        capabilities: NodeCapabilities,
    }

    impl MutableHealthSource {
        fn new() -> Result<Self, zinder_source::NodeCapabilitiesError> {
            Ok(Self {
                available: Arc::new(AtomicBool::new(true)),
                synchronized: Arc::new(AtomicBool::new(true)),
                stalled: Arc::new(AtomicBool::new(false)),
                stalled_request_started: Arc::new(tokio::sync::Notify::new()),
                capabilities: NodeCapabilities::new([
                    NodeCapability::TipId,
                    NodeCapability::TreeState,
                    NodeCapability::OpenRpcDiscovery,
                ])?,
            })
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

    /// A writer restart that admits a new schedule revision leaves the fence
    /// unchanged, so the publisher must refresh the immutable active pair on
    /// schedule identity alone.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the refresh scenario keeps bootstrap, writer admission, and the served table in one causal sequence"
    )]
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
                    Arc::new(activations.clone()),
                )?,
                WalletServingReadiness::without_node_source(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;
        assert!(
            slot.capture()
                .canonical()
                .network_upgrade_schedule()
                .is_none()
        );
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(activations.clone()),
        );
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
        assert_eq!(schedule.revision(), 1);
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
                    Arc::new(activations.clone()),
                )?,
                WalletServingReadiness::without_node_source(zinder_runtime::Readiness::default()),
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
            Some(u64::from(revision))
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
            expected_raw_blob_retention: writer.canonical_primary()?.raw_blob_retention(),
            ..wallet_serving_pair_config(
                writer.canonical_primary_path().to_path_buf(),
                temporary.path().join("canonical-secondaries"),
                wallet_primary_path,
                temporary.path().join("wallet-secondaries"),
                Arc::new(activations.clone()),
            )?
        };
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::without_node_source(zinder_runtime::Readiness::default()),
                &endpoint,
                None,
            )
            .await?;
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(activations.clone()),
        );
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
        // The writer's first start admits the unchanged node table as
        // revision 1 before the query starts with that same table.
        canonical_primary.admit_network_upgrade_schedule(&activations)?;
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
                    Arc::new(activations.clone()),
                )?,
                WalletServingReadiness::without_node_source(zinder_runtime::Readiness::default()),
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
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
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
            Arc::new(activations.clone()),
        )?;
        config.serving_pair_staleness_ceiling = staleness_ceiling;
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                config,
                WalletServingReadiness::without_node_source(readiness.clone()),
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

    /// The compatibility composition refuses a writer that has not confirmed
    /// its tip since it reopened.
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
    async fn compatibility_pair_refuses_a_writer_with_an_unconfirmed_tip()
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
        let serving_readiness = WalletServingReadiness::awaiting_node_source(readiness.clone());
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                    Arc::new(activations.clone()),
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

    /// The compatibility composition refuses a parked writer with its halt.
    ///
    /// A parked writer keeps attesting its frozen fence, and compatibility has
    /// no ingest-control probe, so the halt must come from canonical writer
    /// status: at bootstrap, on every refresh, and until the writer clears it.
    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, recovery, a second park, and the following outage form one causal halt sequence"
    )]
    async fn compatibility_pair_refuses_a_parked_writer_with_the_typed_halt()
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
        let serving_readiness = WalletServingReadiness::awaiting_node_source(readiness.clone());
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let (mut publisher, _slot) =
            WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    temporary.path().join("canonical-secondaries"),
                    wallet_primary_path.clone(),
                    temporary.path().join("wallet-secondaries"),
                    Arc::new(activations.clone()),
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
                    Arc::new(activations),
                )?,
                WalletServingReadiness::without_node_source(readiness.clone()),
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
                Arc::new(activations),
            )?,
            WalletServingReadiness::without_node_source(zinder_runtime::Readiness::default()),
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
            WalletServingReadiness::awaiting_node_and_ingest_control(readiness.clone());
        let (mut publisher, slot) =
            WalletServingPairPublisher::bootstrap_with_admitted_ingest_control(
                wallet_serving_pair_config(
                    canonical_primary_path.clone(),
                    canonical_secondary_root.clone(),
                    wallet_primary_path.clone(),
                    wallet_secondary_root.clone(),
                    Arc::new(activations.clone()),
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
            (),
            admitted_ingest_control.clone(),
            Arc::new(activations.clone()),
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
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)
            .build_v1()?;
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
        let not_ready = wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await
            .err()
            .ok_or("native traffic must be refused before readiness")?;
        assert_eq!(not_ready.code(), Code::Unavailable);
        let reflection_channel = tonic::transport::Endpoint::new(wallet_endpoint.clone())?
            .connect()
            .await?;
        let mut reflection_client = ServerReflectionClient::new(reflection_channel);
        let reflection_request = Request::new(tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        }));
        let reflection_not_ready = reflection_client
            .server_reflection_info(reflection_request)
            .await
            .err()
            .ok_or("reflection traffic must be refused before readiness")?;
        assert_eq!(reflection_not_ready.code(), Code::Unavailable);
        serving_readiness.publish_node_source_cause(zinder_runtime::ReadinessCause::Ready);
        let ingest_control_probe_cancel = CancellationToken::new();
        let ingest_control_probe = spawn_wallet_ingest_control_readiness_probe(
            admitted_ingest_control,
            serving_readiness,
            Duration::from_millis(5),
            ingest_control_probe_cancel.clone(),
        );
        wait_for_readiness_cause(&readiness, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
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
        assert!(!first_chain_event.cursor.is_empty());
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
        assert!(
            !advertised
                .iter()
                .any(|capability| capability == WALLET_READ_TREE_STATE_AT_HEIGHT_V2),
            "tree-state fill must not be advertised without a probed upstream provider"
        );
        let (healthy_status, healthy_healthz) = fetch_operations_json(ops_address, "/healthz")?;
        assert_eq!(healthy_status, 200);
        let (ready_status, readyz) = fetch_operations_json(ops_address, "/readyz")?;
        assert_eq!(ready_status, 200);
        assert_eq!(readyz["cause"], "ready");

        ingest_control_service.set_health_available(false);
        wait_for_readiness_cause(&readiness, |cause| {
            matches!(
                cause,
                zinder_runtime::ReadinessCause::IngestControlUnavailable
            )
        })
        .await?;
        let (outage_ready_status, outage_readyz) = fetch_operations_json(ops_address, "/readyz")?;
        assert_eq!(outage_ready_status, 503);
        assert_eq!(outage_readyz["cause"], "ingest_control_unavailable");
        let (outage_health_status, outage_healthz) =
            fetch_operations_json(ops_address, "/healthz")?;
        assert_eq!(outage_health_status, 200);
        assert_eq!(outage_healthz, healthy_healthz);

        let blocked_tip = wallet_client
            .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
            .await
            .err()
            .ok_or("native traffic must be refused during ingest-control outage")?;
        assert_eq!(blocked_tip.code(), Code::Unavailable);
        let blocked_error_details = blocked_tip.get_error_details();
        let blocked_error_info = blocked_error_details
            .error_info()
            .ok_or("readiness-gated traffic must carry ErrorInfo")?;
        assert_eq!(
            blocked_error_info.reason,
            ErrorReason::ServiceNotReady.as_str_name()
        );
        assert_eq!(
            blocked_error_info.metadata.get("readiness_cause"),
            Some(&"ingest_control_unavailable".to_owned())
        );

        ingest_control_service.set_health_available(true);
        wait_for_readiness_cause(&readiness, |cause| {
            matches!(cause, zinder_runtime::ReadinessCause::Ready)
        })
        .await?;
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
        assert!(!activation_response.activations.is_empty());
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
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
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
        // Each replaced pair keeps its generation for a minute, so the first
        // blocks open the remaining slots before any pair drains.
        let warm_up_blocks = SECONDARY_GENERATION_COUNT - 1;
        for block in 1..=3 * SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 2);
            let fence = primaries.append_block()?;
            publisher.refresh_once().await?;
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
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(primaries.activations.clone()),
        );
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
        let mut coalesced = Vec::new();
        for block in 1..=2 {
            coalesced.push(primaries.append_block()?);
            publisher.refresh_once().await?;
            assert!(
                Arc::ptr_eq(&slot.capture(), &last_published),
                "coalesced block {block} must wait for a slot"
            );
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
        for _ in 1..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_once().await?;
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
        for generation in 0..SECONDARY_GENERATION_COUNT {
            let SecondaryGenerationState::Published { readers, .. } =
                &publisher.generations[generation].state
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
                held.canonical().chain_epoch()?.id,
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
        for _ in 0..SECONDARY_GENERATION_COUNT {
            age_out_retained_pairs(&mut publisher, 2);
            primaries.append_block()?;
            publisher.refresh_once().await?;
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

    /// A drained generation whose secondaries are still shared stays frozen.
    ///
    /// The refresh fails instead of catching the shared secondaries up or
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
        // These blocks fill the other slots. The bootstrap pair's window then
        // ends, which releases generation 0's pair.
        for _ in 1..SECONDARY_GENERATION_COUNT {
            primaries.append_block()?;
            publisher.refresh_once().await?;
        }
        age_out_retained_pairs(&mut publisher, 2);

        let fence = primaries.append_block()?;
        let failure = publisher
            .refresh_once()
            .await
            .err()
            .ok_or("a shared generation must not become a candidate")?;
        assert!(matches!(
            failure,
            WalletServingPairError::CandidateUnavailable { generation: 0 }
        ));
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
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Result<WalletServingPairConfig, Box<dyn std::error::Error>> {
        Ok(WalletServingPairConfig {
            canonical_primary_path,
            canonical_secondary_root,
            wallet_primary_path,
            wallet_secondary_root,
            network: Network::ZcashRegtest,
            network_upgrade_activations,
            expected_raw_blob_retention: RawBlobRetention::Transactions,
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
        let tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32]));
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
        builder.bulk_load_blocks(std::iter::once(Ok::<_, std::io::Error>(
            lifecycle_build_block(tip.height, tip.hash, Network::ZcashRegtest.genesis_hash()),
        )))?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            tip,
            1,
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
            let temporary = TempDir::new()?;
            let activations = lifecycle_upgrade_activations()?;
            let canonical_primary_path = temporary.path().join("canonical-primary");
            let canonical_primary =
                build_lifecycle_canonical_primary(&canonical_primary_path, &activations)?;
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
                CanonicalStoreWorkload::Wallet,
                RawBlobRetention::Transactions,
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
            let config = wallet_serving_pair_config(
                self.temporary.path().join("canonical-primary"),
                self.temporary.path().join("canonical-secondaries"),
                self.temporary.path().join("wallet-primary"),
                self.temporary.path().join("wallet-secondaries"),
                Arc::new(self.activations.clone()),
            )?;
            Ok(
                WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
                    config,
                    WalletServingReadiness::without_node_source(readiness),
                    &self.endpoint,
                    None,
                )
                .await?,
            )
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

    #[derive(Clone)]
    struct MutableCanonicalControl {
        writer_status: Arc<Mutex<CanonicalWriterStatusResponse>>,
        available: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MutableCanonicalControl {
        fn new(writer_status: CanonicalWriterStatusResponse) -> Self {
            Self {
                writer_status: Arc::new(Mutex::new(writer_status)),
                available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        fn set_writer_status(&self, writer_status: CanonicalWriterStatusResponse) {
            *self.writer_status.lock() = writer_status;
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
            if !self.available.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Status::unavailable("fixture writer status outage"));
            }
            Ok(Response::new(self.writer_status.lock().clone()))
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
        QueryError, TransparentAddressUnspentOutputsRequest, WalletQueryApi, WalletServingQuery,
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
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(sample_regtest_upgrade_activations()),
        );
        let first_replaced_at = Instant::now();

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
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(sample_regtest_upgrade_activations()),
        );
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
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(sample_regtest_upgrade_activations()),
        );
        let replaced_at = Instant::now()
            .checked_sub(RETAINED_PAIR_WINDOW)
            .ok_or("the monotonic clock must reach back one retention window")?;

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

        let released = slot.publish(Arc::clone(&republished), Instant::now());
        assert_eq!(released.len(), 1);
        assert!(released.iter().all(|pair| Arc::ptr_eq(pair, &first)));
        assert_eq!(slot.retained_pair_count(), 0);
        assert!(Arc::ptr_eq(&slot.capture_at(Some(epoch)), &republished));

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

    #[tokio::test]
    async fn pinned_first_history_page_answers_from_the_retained_pair() -> Result<(), Box<dyn Error>>
    {
        let history = address_history()?;
        let pairs = serving_pairs_through_live_appends(&history.chain, 2)?;
        // The pair at height 2 holds both history entries.
        let pinned_epoch = pairs[1].canonical_fence().chain_epoch_id();
        let slot = WalletServingPairSlot::new(Arc::clone(&pairs[1]));
        let query = WalletServingQuery::from_serving_pair_slot(
            slot.clone(),
            (),
            Arc::new(sample_regtest_upgrade_activations()),
        );

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
            Arc::new(canonical),
            Arc::new(wallet),
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
