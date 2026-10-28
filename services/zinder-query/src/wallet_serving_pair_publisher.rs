//! Wallet-serving secondary-pair lifecycle shared by native and compatibility runtimes.
//!
//! Each reader runtime owns no canonical or wallet primary handle. It catches
//! up only an inactive generation, authenticates that generation against the
//! canonical writer control plane, and atomically publishes it as one
//! immutable canonical/wallet pair. The prior generation stays open until
//! every in-flight request has dropped its captured pair `Arc`.

use std::{
    borrow::Cow,
    num::NonZeroU8,
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::{
    AdmittedIngestControl, CanonicalReader, WalletProjectionReader, WalletServingAdmissionError,
    WalletServingReadPair, ingest_control::wallet_ingest_control_request,
};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use zinder_core::{Network, NetworkUpgradeActivations, wire::encode_zinder_native_chain_name};
use zinder_proto::v1::ingest::{
    CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
    canonical_control_client::CanonicalControlClient,
};
use zinder_proto::wire::{
    decode_canonical_network_upgrade_schedule, encode_canonical_network_upgrade_schedule,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, NodeUnavailableDetail, Readiness,
    ReadinessCause, ReadinessState, UpstreamHealth, UpstreamNotReadyDetail, WriterAttestation,
    connect_zinder_grpc,
};
use zinder_source::{NodeCapability, NodeSource, SourceError, UpstreamHealthSnapshot};
use zinder_store::{
    CanonicalActivationSchedule, CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalSecondary, RocksDbResourceBudget,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{RocksDbWalletError, RocksDbWalletSecondary};

const SECONDARY_GENERATION_COUNT: usize = 2;
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
#[derive(Clone)]
pub struct WalletServingPairSlot {
    current: Arc<ArcSwap<WalletServingReadPair>>,
}

impl std::fmt::Debug for WalletServingPairSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalletServingPairSlot")
            .field("canonical_fence", &self.capture().canonical_fence())
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
            current: Arc::new(ArcSwap::from(initial_pair)),
        }
    }

    /// Captures the exact pair to retain for one request.
    #[must_use]
    pub fn capture(&self) -> Arc<WalletServingReadPair> {
        self.current.load_full()
    }

    fn publish(&self, pair: Arc<WalletServingReadPair>) -> Arc<WalletServingReadPair> {
        self.current.swap(pair)
    }
}

/// Conjunctive readiness owner for one wallet-serving runtime.
///
/// Pair lifecycle, node-source health, and ingest-control health publish
/// independent inputs here. The existing runtime [`Readiness`] remains the
/// single operations and gRPC projection, but no input writer can erase
/// another input's failure.
#[derive(Clone, Debug)]
pub struct WalletServingReadiness {
    runtime: Readiness,
    state: Arc<Mutex<WalletServingReadinessState>>,
}

#[derive(Clone, Debug)]
struct WalletServingReadinessState {
    pair_state: ReadinessState,
    node_source_cause: ReadinessCause,
    ingest_control_cause: ReadinessCause,
    is_shutting_down: bool,
}

impl WalletServingReadiness {
    /// Starts with the serving pair, node source, and ingest control unadmitted.
    #[must_use]
    pub fn awaiting_node_and_ingest_control(runtime: Readiness) -> Self {
        Self::new(runtime, ReadinessCause::Starting, ReadinessCause::Starting)
    }

    /// Starts with both the serving pair and required node source unadmitted.
    ///
    /// Compatibility runtimes that own a separate live-state readiness
    /// contract use this constructor; native query uses
    /// [`Self::awaiting_node_and_ingest_control`].
    #[must_use]
    pub fn awaiting_node_source(runtime: Readiness) -> Self {
        Self::new(runtime, ReadinessCause::Starting, ReadinessCause::Ready)
    }

    /// Starts a storage-only composition whose node contribution is already satisfied.
    #[must_use]
    pub fn without_node_source(runtime: Readiness) -> Self {
        Self::new(runtime, ReadinessCause::Ready, ReadinessCause::Ready)
    }

    fn new(
        runtime: Readiness,
        node_source_cause: ReadinessCause,
        ingest_control_cause: ReadinessCause,
    ) -> Self {
        let readiness = Self {
            runtime,
            state: Arc::new(Mutex::new(WalletServingReadinessState {
                pair_state: ReadinessState::starting(),
                node_source_cause,
                ingest_control_cause,
                is_shutting_down: false,
            })),
        };
        readiness.publish_projection();
        readiness
    }

    /// Returns the projected readiness handle used by operations and gRPC traffic gates.
    #[must_use]
    pub fn runtime_readiness(&self) -> Readiness {
        self.runtime.clone()
    }

    /// Irreversibly drains readiness before graceful task and server shutdown.
    pub fn publish_shutting_down(&self) {
        self.publish_state_update(|state| {
            state.is_shutting_down = true;
        });
    }

    fn publish_pair_state(&self, pair_state: ReadinessState) {
        self.publish_state_update(|state| {
            state.pair_state = pair_state;
        });
    }

    fn publish_node_source_cause(&self, node_source_cause: ReadinessCause) {
        self.publish_state_update(|state| {
            state.node_source_cause = node_source_cause;
        });
    }

    fn publish_ingest_control_cause(&self, ingest_control_cause: ReadinessCause) {
        self.publish_state_update(|state| {
            state.ingest_control_cause = ingest_control_cause;
        });
    }

    fn publish_projection(&self) {
        self.publish_state_update(|_| {});
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the readiness-state lock must cover runtime publication so a stale projection cannot overtake a concurrent failure"
    )]
    fn publish_state_update(&self, update: impl FnOnce(&mut WalletServingReadinessState)) {
        let mut state = self.state.lock();
        update(&mut state);
        let projected = Self::projected_readiness(&state);
        // Keep the projection write in the same critical section as its input
        // mutation. Otherwise a delayed recovery projection can overtake a
        // newer failure from another readiness input and reopen traffic.
        self.runtime.set(projected);
    }

    fn projected_readiness(state: &WalletServingReadinessState) -> ReadinessState {
        let mut projected = state.pair_state.clone();
        if state.is_shutting_down {
            projected.cause = ReadinessCause::ShuttingDown;
            projected.target_height = None;
        } else if matches!(
            state.ingest_control_cause,
            ReadinessCause::ReorgWindowExceeded { .. }
        ) {
            // A parked writer is the root cause of any pair refresh failure
            // that follows it, so its typed halt wins over them.
            projected.cause = state.ingest_control_cause.clone();
            projected.target_height = None;
        } else if !state.pair_state.cause.permits_traffic() {
            // The pair state is already the complete projection.
        } else if !state.node_source_cause.permits_traffic() {
            projected.cause = state.node_source_cause.clone();
            projected.target_height = None;
        } else if !state.ingest_control_cause.permits_traffic() {
            projected.cause = state.ingest_control_cause.clone();
            projected.target_height = None;
        }
        projected
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
            let observation = probe_wallet_node_readiness(&source).await;
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

/// Owns the two bounded reader generations and their publication slot.
pub struct WalletServingPairPublisher {
    config: WalletServingPairConfig,
    writer_status: CanonicalWriterStatusClient,
    readiness: WalletServingReadiness,
    serving_pair_slot: Option<WalletServingPairSlot>,
    generations: [SecondaryGeneration; SECONDARY_GENERATION_COUNT],
    published_generation: Option<usize>,
    /// Trailing reference to the pair swapped out of the slot.
    ///
    /// Holding it makes the publisher the last owner of every retired pair,
    /// so the direct-I/O `RocksDB` close never runs on a request task's
    /// reactor thread. Only one pair is ever retained: reusing a generation
    /// requires its lease to have drained, which cannot happen while this
    /// reference is alive.
    retired_pair: Option<Arc<WalletServingReadPair>>,
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
            retired_pair: None,
            writer_attestation,
            observed_lag_chain_epochs: 0,
            secondary_activations,
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
                            self.record_refresh_failure(&error);
                            tracing::warn!(
                                target: "zinder::wallet_serving",
                                event = "wallet_serving_pair_publisher_refresh_failed",
                                error = %error,
                                "inactive wallet-serving pair refresh failed; retaining the prior pair"
                            );
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
        self.reap_retired_pair().await;
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

        let generation = self.inactive_generation();
        if !self.prepare_candidate(generation).await? {
            return Ok(());
        }
        self.converge_and_publish(generation).await
    }

    /// Closes the retired pair's `RocksDB` handles on the blocking pool.
    ///
    /// Closing a direct-I/O instance is expensive, so the drop that actually
    /// runs the destructors is moved off the reactor. The pair stays open
    /// until the last request that captured it has released it; that is the
    /// same condition [`Self::prepare_candidate`] already waits on before
    /// reopening the generation.
    async fn reap_retired_pair(&mut self) {
        let Some(retired_pair) = self.retired_pair.take() else {
            return;
        };
        if Arc::strong_count(&retired_pair) > 1 {
            self.retired_pair = Some(retired_pair);
            return;
        }
        let started_at = Instant::now();
        let teardown = tokio::task::spawn_blocking(move || drop(retired_pair)).await;
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

    fn inactive_generation(&self) -> usize {
        self.published_generation.map_or(0, |generation| {
            (generation + 1) % SECONDARY_GENERATION_COUNT
        })
    }

    /// Opens a generation only after the prior pair in that exact path has no
    /// strong references. It deliberately does not delete the metadata path:
    /// opening an inactive path is safe and bounded, while deleting it would
    /// widen the failure surface without improving correctness.
    async fn prepare_candidate(
        &mut self,
        generation: usize,
    ) -> Result<bool, WalletServingPairError> {
        let secondary_generation = &mut self.generations[generation];
        match &secondary_generation.state {
            SecondaryGenerationState::Candidate { .. } => return Ok(true),
            SecondaryGenerationState::Published { lease } if !lease.is_reusable() => {
                metrics::counter!(
                    "zinder_wallet_serving_pair_publisher_generation_wait_total",
                    "reason" => "in_flight_requests"
                )
                .increment(1);
                return Ok(false);
            }
            SecondaryGenerationState::Vacant | SecondaryGenerationState::Published { .. } => {}
        }
        secondary_generation.state = SecondaryGenerationState::Vacant;
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
        let candidate = tokio::task::spawn_blocking(move || {
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
        .await??;
        self.generations[generation].state = SecondaryGenerationState::Candidate { candidate };
        Ok(true)
    }

    async fn converge_and_publish(
        &mut self,
        generation: usize,
    ) -> Result<(), WalletServingPairError> {
        let deadline = Instant::now() + self.config.convergence_timeout;
        let mut last_outcome = WalletServingConvergence::ReplicaBehind;
        for attempt in 0..u32::from(self.config.convergence_attempts.get()) {
            match self.catch_up_candidate(generation).await? {
                Ok(()) => {
                    let source = self.candidate_wallet_source(generation)?;
                    let writer_status = self.writer_status.fetch().await?;
                    if writer_status_matches_source(&writer_status, source, self.config.network)
                        && writer_schedule_matches(
                            &writer_status,
                            self.candidate_network_upgrade_schedule(generation)?,
                        )
                    {
                        self.publish_candidate(generation, &writer_status).await?;
                        return Ok(());
                    }
                    last_outcome = WalletServingConvergence::ReplicaBehind;
                    record_pair_convergence(last_outcome);
                }
                Err(outcome) => {
                    last_outcome = outcome;
                    record_pair_convergence(outcome);
                }
            }
            if Instant::now() >= deadline
                || attempt + 1 == u32::from(self.config.convergence_attempts.get())
            {
                break;
            }
            tokio::time::sleep(
                self.config
                    .catchup_interval
                    .min(CONVERGENCE_RETRY_DELAY_CAP),
            )
            .await;
        }
        Err(WalletServingPairError::ConvergenceTimedOut { last_outcome })
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
        let pair = tokio::task::spawn_blocking(move || {
            let canonical: Arc<dyn CanonicalReader> = Arc::new(candidate.canonical);
            let wallet: Arc<dyn WalletProjectionReader> = Arc::new(candidate.wallet);
            WalletServingReadPair::new(canonical, wallet)
                .map(Arc::new)
                .map_err(WalletServingPairError::PairPublication)
        })
        .await
        .map_err(WalletServingPairError::CandidateTask)??;
        self.retired_pair = if let Some(slot) = &self.serving_pair_slot {
            Some(slot.publish(Arc::clone(&pair)))
        } else {
            self.serving_pair_slot = Some(WalletServingPairSlot::new(Arc::clone(&pair)));
            None
        };
        self.generations[generation].state = SecondaryGenerationState::Published {
            lease: GenerationLease::new(&pair),
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
        let Some(writer_fence) = writer_status.fence.as_ref() else {
            return Err(WalletServingPairError::WriterStatusInvalid);
        };
        if writer_status.network_name != encode_zinder_native_chain_name(self.config.network) {
            return Err(WalletServingPairError::WriterStatusInvalid);
        }
        let active_source = active_pair.wallet_source();
        let active_epoch = active_source.source_position().chain_epoch_id.value();
        if writer_fence.chain_epoch_id < active_epoch {
            return Err(WalletServingPairError::WriterFenceMismatch);
        }
        let visible_height = Some(active_source.source_position().tip.height.value());
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
        // A writer restart can admit a new schedule revision without moving the
        // fence; the immutable active pair then refreshes like a lagging one.
        if writer_fence.chain_epoch_id == active_epoch
            && writer_schedule_matches(
                writer_status,
                active_pair.canonical().network_upgrade_schedule(),
            )
        {
            if !writer_status_matches_source(writer_status, active_source, self.config.network) {
                return Err(WalletServingPairError::WriterFenceMismatch);
            }
            record_replica_lag(0);
            self.writer_attestation.attest();
            self.observed_lag_chain_epochs = 0;
            self.readiness
                .publish_pair_state(ReadinessState::ready(visible_height));
            return Ok(ActiveWriterRelation::Exact);
        }
        let lag = writer_fence.chain_epoch_id.saturating_sub(active_epoch);
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
                    Some(writer_fence.visible_tip_height),
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

    fn record_refresh_failure(&self, error: &WalletServingPairError) {
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
            .or(admitted_while_stale);
        self.readiness
            .publish_pair_state(failure_state.unwrap_or_else(|| {
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
            }));
        metrics::counter!(
            "zinder_wallet_serving_pair_publisher_refresh_total",
            "status" => "error",
            "error_class" => wallet_serving_pair_error_class(error)
        )
        .increment(1);
    }
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
    },
}

struct SecondaryPairCandidate {
    canonical: RocksDbCanonicalSecondary,
    wallet: RocksDbWalletSecondary,
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

fn writer_status_matches_source(
    status: &CanonicalWriterStatusResponse,
    source: WalletCanonicalSourceIdentity,
    network: Network,
) -> bool {
    let Some(fence) = status.fence.as_ref() else {
        return false;
    };
    let source_position = source.source_position();
    status.network_name == encode_zinder_native_chain_name(network)
        && fence.chain_epoch_id == source_position.chain_epoch_id.value()
        && fence.event_sequence == source_position.event_sequence
        && fence.visible_tip_height == source_position.tip.height.value()
        && fence.visible_tip_hash == source_position.tip.hash.as_bytes()
        && fence.visible_block_count == source.source_sequence_digest().block_count()
        && fence.canonical_sequence_digest == source.source_sequence_digest().as_bytes()
}

/// Returns whether the writer reports exactly the schedule revision `schedule`
/// carries, including when neither side has admitted one yet.
fn writer_schedule_matches(
    status: &CanonicalWriterStatusResponse,
    schedule: Option<&CanonicalActivationSchedule>,
) -> bool {
    status.network_upgrade_schedule
        == schedule.map(|schedule| {
            encode_canonical_network_upgrade_schedule(schedule.activations(), schedule.revision())
        })
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
        UnixTimestampMillis, encode_canonical_block_replay, wire::encode_zinder_native_chain_name,
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
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2, WALLET_READ_VISIBLE_TIP_BLOCK_V1,
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
        SecondaryGenerationState, WalletServingConvergence, WalletServingPairConfig,
        WalletServingPairError, WalletServingPairPublisher, WalletServingReadiness,
        classify_pair_admission, refresh_failure_not_ready_cause,
        refresh_failure_retains_attested_pair, spawn_wallet_ingest_control_readiness_probe,
        spawn_wallet_node_readiness_probe, wallet_ingest_control_request,
        writer_status_matches_source,
    };
    use crate::{
        AdmittedIngestControl, NativeWalletEndpointCapabilities, WalletEndpointMetadata,
        WalletQueryApi, WalletQueryGrpcAdapter, WalletServingQuery,
    };
    use zinder_testkit::{IngestControlFixture, IngestControlFixtureService, LogCapture};

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
        capabilities: NodeCapabilities,
    }

    impl MutableHealthSource {
        fn new() -> Result<Self, zinder_source::NodeCapabilitiesError> {
            Ok(Self {
                available: Arc::new(AtomicBool::new(true)),
                synchronized: Arc::new(AtomicBool::new(true)),
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

        assert!(writer_status_matches_source(
            &writer_status(source),
            source,
            Network::ZcashRegtest,
        ));
    }

    #[test]
    fn mutated_writer_fence_is_rejected_before_wallet_serving_pair_publisher_publication() {
        let source = source_identity(3, 0x33);
        let mut status = writer_status(source);
        if let Some(fence) = status.fence.as_mut() {
            fence.canonical_sequence_digest[0] ^= 0xff;
        }

        assert!(!writer_status_matches_source(
            &status,
            source,
            Network::ZcashRegtest,
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
        let compact_block = compact_blocks
            .next()
            .await
            .ok_or("native compact range must return the visible block")??;
        assert!(compact_blocks.next().await.is_none());
        assert_eq!(
            compact_block
                .chain_view
                .as_ref()
                .and_then(|view| view.chain_epoch.as_ref())
                .map(|epoch| epoch.chain_epoch_id),
            Some(visible_tip.chain_epoch.id.value())
        );
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
                descending: false,
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
        let stale_epoch_outcome = query
            .visible_tip_block(Some(visible_tip.chain_epoch.id))
            .await;
        assert!(matches!(
            stale_epoch_outcome,
            Err(crate::QueryError::ChainEpochPinUnavailable { .. })
        ));
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
        assert_eq!(published_generation_is_reusable(&publisher, 0), Some(false));
        publisher.reap_retired_pair().await;
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
        let replay_range =
            BlockHeightRange::inclusive(BlockHeight::new(2), target_fence.visible_tip().height);
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
                super::encode_canonical_network_upgrade_schedule(
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
            SecondaryGenerationState::Published { lease } => Some(lease.is_reusable()),
            SecondaryGenerationState::Vacant | SecondaryGenerationState::Candidate { .. } => None,
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
