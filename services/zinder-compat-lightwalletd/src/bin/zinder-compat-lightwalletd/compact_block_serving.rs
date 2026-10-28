//! Private compact-block compatibility composition.
//!
//! This module deliberately owns both the canonical-secondary lifecycle and
//! the restricted protocol adapter. No compact-only topology type crosses the
//! binary boundary.

use std::{path::PathBuf, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, CompactBlockArtifact, CompactTransaction,
    CompactTransactionData, Network, NetworkUpgradeActivations,
    wire::{encode_branch_id_hex, encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::{
    compat::lightwalletd::{self, compact_tx_streamer_server},
    v1::ingest::{
        CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
        canonical_control_client::CanonicalControlClient,
    },
    wire::decode_canonical_network_upgrade_schedule,
};
use zinder_query::{DEFAULT_MAX_COMPACT_BLOCK_RANGE, QueryError, status_from_query_error};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, ConfigError, NodeUnavailableDetail, Readiness,
    ReadinessCause, ReadinessState, UpstreamHealth, UpstreamNotReadyDetail, connect_zinder_grpc,
};
use zinder_source::{NodeCapability, NodeSource};
use zinder_store::{
    BlockHashLookup, CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalSecondary, RocksDbResourceBudget,
};

type GrpcStream<T> = std::pin::Pin<
    Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>,
>;

const GENERATION_COUNT: usize = 2;
const RANGE_CHANNEL_CAPACITY: usize = 8;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactBlockServingError {
    #[error("compact-block storage failed: {0}")]
    Storage(#[from] CanonicalStoreError),
    #[error("compact-block writer status failed: {0}")]
    WriterStatus(String),
    #[error("compact-block runtime failed: {0}")]
    Runtime(String),
}

#[derive(Debug, thiserror::Error)]
enum ConvergenceFailure {
    #[error("writer status transport failed: {0}")]
    WriterStatusTransport(String),
    #[error("writer is ahead of the candidate secondary")]
    WriterAhead,
    #[error("writer status is behind or invalid for the candidate secondary")]
    WriterBehindOrInvalid,
    #[error("writer fence disagrees with a same-epoch candidate")]
    SameEpochMismatch,
    #[error("candidate storage failed: {0}")]
    Storage(CompactBlockServingError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriterFenceRelation {
    Exact,
    Ahead(u64),
    BehindOrInvalid,
    SameEpochMismatch,
}

fn classify_writer_fence(
    status: &CanonicalWriterStatusResponse,
    expected_network: Network,
    active_epoch: u64,
    same_epoch_exact: bool,
) -> WriterFenceRelation {
    if status.network_name != zinder_core::wire::encode_zinder_native_chain_name(expected_network) {
        return WriterFenceRelation::BehindOrInvalid;
    }
    let Some(fence) = status.fence.as_ref() else {
        return WriterFenceRelation::BehindOrInvalid;
    };
    match fence.chain_epoch_id.cmp(&active_epoch) {
        std::cmp::Ordering::Less => WriterFenceRelation::BehindOrInvalid,
        std::cmp::Ordering::Equal if same_epoch_exact => WriterFenceRelation::Exact,
        std::cmp::Ordering::Equal => WriterFenceRelation::SameEpochMismatch,
        std::cmp::Ordering::Greater => {
            WriterFenceRelation::Ahead(fence.chain_epoch_id - active_epoch)
        }
    }
}

#[derive(Clone)]
pub(crate) struct CompactServingReadiness {
    runtime: Readiness,
    state: Arc<Mutex<CompactServingReadinessState>>,
}

#[derive(Clone)]
struct CompactServingReadinessState {
    storage: ReadinessState,
    node: ReadinessCause,
    shutting_down: bool,
}

impl CompactServingReadiness {
    pub(crate) fn new(runtime: Readiness) -> Self {
        let readiness = Self {
            runtime,
            state: Arc::new(Mutex::new(CompactServingReadinessState {
                storage: ReadinessState::starting(),
                node: ReadinessCause::Starting,
                shutting_down: false,
            })),
        };
        readiness.project();
        readiness
    }

    pub(crate) fn runtime(&self) -> Readiness {
        self.runtime.clone()
    }

    pub(crate) fn publish_storage(&self, storage: ReadinessState) {
        let mut state = self.state.lock();
        state.storage = storage;
        self.project_locked(&state);
        drop(state);
    }

    pub(crate) fn publish_node(&self, node: ReadinessCause) {
        let mut state = self.state.lock();
        state.node = node;
        self.project_locked(&state);
        drop(state);
    }

    pub(crate) fn publish_shutting_down(&self) {
        let mut state = self.state.lock();
        state.shutting_down = true;
        self.project_locked(&state);
        drop(state);
    }

    fn project(&self) {
        let state = self.state.lock();
        self.project_locked(&state);
    }

    fn project_locked(&self, state: &CompactServingReadinessState) {
        let mut projected = state.storage.clone();
        if state.shutting_down {
            projected.cause = ReadinessCause::ShuttingDown;
            projected.target_height = None;
        } else if !state.storage.cause.permits_traffic() {
            // Storage admission is authoritative until a valid snapshot exists.
        } else if !state.node.permits_traffic() {
            projected.cause = state.node.clone();
            projected.target_height = None;
        }
        self.runtime.set(projected);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompactBlockServingConfig {
    pub(crate) canonical_primary_path: PathBuf,
    pub(crate) canonical_secondary_root: PathBuf,
    pub(crate) network: Network,
    pub(crate) activations: Arc<NetworkUpgradeActivations>,
    pub(crate) raw_blob_retention: RawBlobRetention,
    pub(crate) reorg_policy: CanonicalReorgPolicy,
    pub(crate) resource_budget: RocksDbResourceBudget,
    pub(crate) catchup_interval: Duration,
    pub(crate) convergence_timeout: Duration,
    pub(crate) convergence_attempts: u8,
    pub(crate) staleness_ceiling: Duration,
    pub(crate) lag_threshold: u64,
}

#[derive(Clone)]
pub(crate) struct CompactBlockServingSlot {
    current: Arc<ArcSwap<RocksDbCanonicalSecondary>>,
}

impl CompactBlockServingSlot {
    fn new(reader: Arc<RocksDbCanonicalSecondary>) -> Self {
        Self {
            current: Arc::new(ArcSwap::from(reader)),
        }
    }

    pub(crate) fn capture(&self) -> Arc<RocksDbCanonicalSecondary> {
        self.current.load_full()
    }

    fn publish(&self, reader: Arc<RocksDbCanonicalSecondary>) -> Arc<RocksDbCanonicalSecondary> {
        self.current.swap(reader)
    }
}

struct WriterStatus {
    client: CanonicalControlClient<AuthenticatedChannel>,
}

impl WriterStatus {
    async fn connect(endpoint: &str, token: Option<&BearerToken>) -> Result<Self, Status> {
        let channel = connect_zinder_grpc(endpoint, token)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Self {
            client: CanonicalControlClient::new(channel),
        })
    }

    async fn fetch(&mut self) -> Result<CanonicalWriterStatusResponse, Status> {
        self.client
            .writer_status(Request::new(CanonicalWriterStatusRequest {}))
            .await
            .map(Response::into_inner)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

pub(crate) struct CompactBlockPublisher {
    config: CompactBlockServingConfig,
    writer_status: WriterStatus,
    readiness: CompactServingReadiness,
    slot: CompactBlockServingSlot,
    active_generation: usize,
    retired: Option<Arc<RocksDbCanonicalSecondary>>,
    /// Last attestation of the active reader by a following writer. `None`
    /// while the writer is parked, so a frozen reader never earns a staleness
    /// allowance that would admit traffic.
    last_attested: Option<tokio::time::Instant>,
    /// Halt the writer last reported. Writer-status failures republish it
    /// until a valid status without the halt clears it.
    parked: Option<ReadinessState>,
    /// Activation table each new generation opens with: the node's table at
    /// startup, then the table the writer reports, so generations keep
    /// opening after the chain crosses an activation admitted later.
    generation_activations: Arc<NetworkUpgradeActivations>,
}

impl CompactBlockPublisher {
    pub(crate) async fn bootstrap(
        config: CompactBlockServingConfig,
        readiness: CompactServingReadiness,
        writer_status_endpoint: &str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<(Self, CompactBlockServingSlot), CompactBlockServingError> {
        let mut writer_status = WriterStatus::connect(writer_status_endpoint, bearer_token)
            .await
            .map_err(|error| CompactBlockServingError::WriterStatus(error.message().to_owned()))?;
        let generation_activations = Arc::clone(&config.activations);
        let candidate = open_generation(&config, &generation_activations, 0).await?;
        let candidate = catch_up_generation(candidate).await?;
        let (candidate, attesting_status) =
            converge_candidate(&mut writer_status, &config, candidate)
                .await
                .map_err(|error| CompactBlockServingError::Runtime(error.to_string()))?;
        let reader = Arc::new(candidate);
        let slot = CompactBlockServingSlot::new(Arc::clone(&reader));
        let visible_height = reader.event_fence().visible_tip().height.value();
        let parked = parked_writer_readiness(&attesting_status, config.network, visible_height);
        readiness.publish_storage(
            parked
                .clone()
                .unwrap_or_else(|| ReadinessState::ready(Some(visible_height))),
        );
        let publisher = Self {
            config,
            writer_status,
            readiness,
            slot: slot.clone(),
            active_generation: 0,
            retired: None,
            last_attested: parked.is_none().then(tokio::time::Instant::now),
            parked,
            generation_activations,
        };
        Ok((publisher, slot))
    }

    pub(crate) fn spawn(
        mut self,
        cancel: CancellationToken,
        serving_drained: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(self.config.catchup_interval) => {
                        self.refresh().await;
                    }
                }
            }
            serving_drained.cancelled().await;
            match tokio::task::spawn_blocking(move || drop(self)).await {
                Ok(()) => tracing::info!(
                    target: "zinder::compat_lightwalletd",
                    event = "compact_serving_publisher_teardown_complete",
                    "compact serving publisher teardown completed"
                ),
                Err(error) => tracing::error!(
                    target: "zinder::compat_lightwalletd",
                    event = "compact_serving_publisher_teardown_failed",
                    error = %error,
                    "compact serving publisher teardown task failed"
                ),
            }
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Replacement admission keeps writer attestation, generation leasing, and readiness transitions together."
    )]
    async fn refresh(&mut self) {
        let active = self.slot.capture();
        let status = match self.writer_status.fetch().await {
            Ok(status) => status,
            Err(_error) => {
                // A writer last seen parked keeps its halt through an outage;
                // only a valid status without the halt clears it.
                if let Some(parked) = self.parked.clone() {
                    self.readiness.publish_storage(parked);
                } else {
                    self.record_transport_failure(
                        active.event_fence().visible_tip().height.value(),
                    );
                }
                return;
            }
        };
        // A parked writer keeps attesting its frozen fence; the reader stays
        // published but refuses traffic with the writer's typed halt.
        self.parked = parked_writer_readiness(
            &status,
            self.config.network,
            active.event_fence().visible_tip().height.value(),
        );
        if let Some(parked) = self.parked.clone() {
            self.last_attested = None;
            self.readiness.publish_storage(parked);
            return;
        }
        let active_epoch = active.event_fence().chain_epoch_id().value();
        let relation = classify_writer_fence(
            &status,
            self.config.network,
            active_epoch,
            attest(&status, active.as_ref(), self.config.network).is_ok(),
        );
        if matches!(
            relation,
            WriterFenceRelation::BehindOrInvalid | WriterFenceRelation::SameEpochMismatch
        ) {
            self.record_failure(
                ReadinessCause::SchemaMismatch,
                active.event_fence().visible_tip().height.value(),
            );
            return;
        }
        if relation == WriterFenceRelation::Exact {
            self.last_attested = Some(tokio::time::Instant::now());
            self.readiness.publish_storage(ReadinessState::ready(Some(
                active.event_fence().visible_tip().height.value(),
            )));
            return;
        }
        let WriterFenceRelation::Ahead(lag) = relation else {
            return;
        };
        let Some(fence) = status.fence.as_ref() else {
            return;
        };
        if lag <= self.config.lag_threshold {
            self.readiness
                .publish_storage(ReadinessState::ready_with_target(
                    Some(active.event_fence().visible_tip().height.value()),
                    Some(fence.visible_tip_height),
                ));
        } else if let Some(staleness) = self.stale_allowance() {
            self.readiness
                .publish_storage(ReadinessState::serving_pair_stale(
                    lag,
                    staleness.as_secs(),
                    Some(active.event_fence().visible_tip().height.value()),
                ));
        } else {
            self.record_failure(
                ReadinessCause::ReplicaLagging {
                    lag_chain_epochs: lag,
                },
                active.event_fence().visible_tip().height.value(),
            );
        }
        if let Some(retired) = self.retired.take() {
            if Arc::strong_count(&retired) > 1 {
                self.retired = Some(retired);
                return;
            }
            if let Err(error) = tokio::task::spawn_blocking(move || drop(retired)).await {
                tracing::error!(
                    target: "zinder::compat_lightwalletd",
                    event = "compact_serving_retired_generation_teardown_failed",
                    error = %error,
                    "retired compact serving generation teardown failed"
                );
            }
        }
        if let Some(schedule) = status.network_upgrade_schedule.clone() {
            let Ok((activations, _revision)) =
                decode_canonical_network_upgrade_schedule(schedule, self.config.network)
            else {
                self.record_failure(
                    ReadinessCause::SchemaMismatch,
                    active.event_fence().visible_tip().height.value(),
                );
                return;
            };
            self.generation_activations = Arc::new(activations);
        }
        let candidate_generation = (self.active_generation + 1) % GENERATION_COUNT;
        let Ok(candidate) = open_generation(
            &self.config,
            &self.generation_activations,
            candidate_generation,
        )
        .await
        else {
            self.record_failure(
                ReadinessCause::StorageUnavailable,
                active.event_fence().visible_tip().height.value(),
            );
            return;
        };
        let Ok(candidate) = catch_up_generation(candidate).await else {
            self.record_failure(
                ReadinessCause::StorageUnavailable,
                active.event_fence().visible_tip().height.value(),
            );
            return;
        };
        let (candidate, attesting_status) =
            match converge_candidate(&mut self.writer_status, &self.config, candidate).await {
                Ok(converged) => converged,
                Err(ConvergenceFailure::WriterStatusTransport(_)) => {
                    let cause = self.stale_allowance().map_or(
                        ReadinessCause::WriterStatusUnavailable,
                        |staleness| ReadinessCause::ServingPairStale {
                            lag_chain_epochs: lag,
                            staleness_seconds: staleness.as_secs(),
                        },
                    );
                    self.record_failure(cause, active.event_fence().visible_tip().height.value());
                    return;
                }
                Err(ConvergenceFailure::WriterAhead) => {
                    let cause = self.stale_allowance().map_or(
                        ReadinessCause::ReplicaLagging {
                            lag_chain_epochs: lag,
                        },
                        |staleness| ReadinessCause::ServingPairStale {
                            lag_chain_epochs: lag,
                            staleness_seconds: staleness.as_secs(),
                        },
                    );
                    self.record_failure(cause, active.event_fence().visible_tip().height.value());
                    return;
                }
                Err(
                    ConvergenceFailure::WriterBehindOrInvalid
                    | ConvergenceFailure::SameEpochMismatch,
                ) => {
                    self.record_failure(
                        ReadinessCause::SchemaMismatch,
                        active.event_fence().visible_tip().height.value(),
                    );
                    return;
                }
                Err(ConvergenceFailure::Storage(_)) => {
                    self.record_failure(
                        ReadinessCause::StorageUnavailable,
                        active.event_fence().visible_tip().height.value(),
                    );
                    return;
                }
            };
        let reader = Arc::new(candidate);
        self.retired = Some(self.slot.publish(Arc::clone(&reader)));
        self.active_generation = candidate_generation;
        // The writer can park between the first fetch and the one that
        // attested the candidate; the attesting status decides, as at bootstrap.
        let visible_height = reader.event_fence().visible_tip().height.value();
        self.parked =
            parked_writer_readiness(&attesting_status, self.config.network, visible_height);
        self.last_attested = self.parked.is_none().then(tokio::time::Instant::now);
        self.readiness.publish_storage(
            self.parked
                .clone()
                .unwrap_or_else(|| ReadinessState::ready(Some(visible_height))),
        );
    }

    fn record_failure(&self, cause: ReadinessCause, height: u32) {
        metrics::counter!(
            "zinder_compat_compact_refresh_failures_total",
            "cause" => cause.metric_label()
        )
        .increment(1);
        tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "compact_serving_refresh_failed",
            cause = cause.metric_label(),
            height,
            "compact serving refresh failed"
        );
        self.readiness.publish_storage(ReadinessState {
            cause,
            current_height: Some(height),
            target_height: None,
            phase: None,
        });
    }

    fn record_transport_failure(&self, height: u32) {
        self.record_failure(transport_failure_cause(self.stale_allowance()), height);
    }

    /// Time since the last attestation by a following writer while it is still
    /// within the staleness ceiling; `None` once the ceiling passes or while
    /// no following writer has attested the active reader.
    fn stale_allowance(&self) -> Option<Duration> {
        self.last_attested
            .map(|attested| attested.elapsed())
            .filter(|staleness| *staleness < self.config.staleness_ceiling)
    }
}

fn transport_failure_cause(stale_allowance: Option<Duration>) -> ReadinessCause {
    stale_allowance.map_or(ReadinessCause::WriterStatusUnavailable, |staleness| {
        ReadinessCause::ServingPairStale {
            lag_chain_epochs: 0,
            staleness_seconds: staleness.as_secs(),
        }
    })
}

async fn open_generation(
    config: &CompactBlockServingConfig,
    activations: &Arc<NetworkUpgradeActivations>,
    generation: usize,
) -> Result<RocksDbCanonicalSecondary, CompactBlockServingError> {
    let primary = config.canonical_primary_path.clone();
    let secondary_root = config.canonical_secondary_root.clone();
    let activations = Arc::clone(activations);
    let retention = config.raw_blob_retention;
    let reorg = config.reorg_policy;
    let budget = config.resource_budget;
    tokio::task::spawn_blocking(move || {
        let secondary = secondary_root.join(format!("generation-{generation}"));
        std::fs::create_dir_all(&secondary).map_err(|error| {
            CompactBlockServingError::Runtime(format!(
                "cannot create compact-block secondary: {error}"
            ))
        })?;
        RocksDbCanonicalSecondary::open_ready(
            primary,
            secondary,
            activations.as_ref(),
            CanonicalStoreWorkload::Wallet,
            retention,
            reorg,
            budget,
        )
        .map_err(CompactBlockServingError::Storage)
    })
    .await
    .map_err(|error| {
        CompactBlockServingError::Runtime(format!("compact-block secondary task failed: {error}"))
    })?
}

async fn catch_up_generation(
    mut candidate: RocksDbCanonicalSecondary,
) -> Result<RocksDbCanonicalSecondary, CompactBlockServingError> {
    Ok(
        tokio::task::spawn_blocking(move || candidate.try_catch_up().map(|_| candidate))
            .await
            .map_err(|error| {
                CompactBlockServingError::Runtime(format!(
                    "compact-block catch-up task failed: {error}"
                ))
            })??,
    )
}

fn attest(
    status: &CanonicalWriterStatusResponse,
    reader: &RocksDbCanonicalSecondary,
    network: Network,
) -> Result<(), Status> {
    let Some(fence) = status.fence.as_ref() else {
        return Err(Status::failed_precondition("writer status has no fence"));
    };
    let local = reader.event_fence();
    if status.network_name != zinder_core::wire::encode_zinder_native_chain_name(network)
        || fence.chain_epoch_id != local.chain_epoch_id().value()
        || fence.event_sequence != local.chain_event_sequence()
        || fence.visible_tip_height != local.visible_tip().height.value()
        || fence.visible_tip_hash != local.visible_tip().hash.as_bytes()
        || fence.canonical_sequence_digest != local.sequence_digest().as_bytes()
    {
        return Err(Status::failed_precondition(
            "writer status does not attest the compact-block reader",
        ));
    }
    Ok(())
}

/// Returns the typed halt a parked writer reports on canonical writer status.
///
/// Only a halt from the configured network is honoured; any other status
/// falls through to the usual fence classification.
fn parked_writer_readiness(
    status: &CanonicalWriterStatusResponse,
    network: Network,
    visible_height: u32,
) -> Option<ReadinessState> {
    let halt = status.reorg_window_exceeded?;
    (status.network_name == zinder_core::wire::encode_zinder_native_chain_name(network)).then(
        || {
            ReadinessState::reorg_window_exceeded(
                halt.depth,
                halt.configured,
                halt.covered,
                Some(visible_height),
            )
        },
    )
}

/// Converges `candidate` on the writer fence and returns it with the writer
/// status that attested it.
async fn converge_candidate(
    writer_status: &mut WriterStatus,
    config: &CompactBlockServingConfig,
    mut candidate: RocksDbCanonicalSecondary,
) -> Result<(RocksDbCanonicalSecondary, CanonicalWriterStatusResponse), ConvergenceFailure> {
    let deadline = tokio::time::Instant::now() + config.convergence_timeout;
    let attempts = config.convergence_attempts.max(1);
    for attempt in 0..attempts {
        candidate = catch_up_generation(candidate)
            .await
            .map_err(ConvergenceFailure::Storage)?;
        let status = writer_status.fetch().await.map_err(|error| {
            ConvergenceFailure::WriterStatusTransport(error.message().to_owned())
        })?;
        if attest(&status, &candidate, config.network).is_ok() {
            return Ok((candidate, status));
        }
        let relation = classify_writer_fence(
            &status,
            config.network,
            candidate.event_fence().chain_epoch_id().value(),
            false,
        );
        if !matches!(relation, WriterFenceRelation::Ahead(_)) {
            return Err(match relation {
                WriterFenceRelation::SameEpochMismatch => ConvergenceFailure::SameEpochMismatch,
                WriterFenceRelation::BehindOrInvalid => ConvergenceFailure::WriterBehindOrInvalid,
                WriterFenceRelation::Exact | WriterFenceRelation::Ahead(_) => unreachable!(),
            });
        }
        if attempt + 1 == attempts || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(config.catchup_interval.min(Duration::from_millis(100))).await;
    }
    Err(ConvergenceFailure::WriterAhead)
}

#[derive(Clone)]
pub(crate) struct CompactBlockAdapter {
    slot: CompactBlockServingSlot,
    activations: Arc<NetworkUpgradeActivations>,
}

impl CompactBlockAdapter {
    pub(crate) fn new(
        slot: CompactBlockServingSlot,
        activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        Self { slot, activations }
    }

    pub(crate) fn into_server(self) -> compact_tx_streamer_server::CompactTxStreamerServer<Self> {
        compact_tx_streamer_server::CompactTxStreamerServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES)
    }

    fn reader(&self) -> Arc<RocksDbCanonicalSecondary> {
        self.slot.capture()
    }

    fn unsupported(method: &'static str) -> Status {
        Status::unimplemented(format!(
            "compact-blocks compatibility does not implement {method}"
        ))
    }

    fn resolve_height(
        reader: &RocksDbCanonicalSecondary,
        block: &lightwalletd::BlockId,
    ) -> Result<BlockHeight, Status> {
        if block.height != 0 {
            return u32::try_from(block.height)
                .map(BlockHeight::new)
                .map_err(|_| Status::invalid_argument("block height exceeds u32"));
        }
        if block.hash.len() != 32 {
            return Err(Status::invalid_argument("block identifier is unspecified"));
        }
        let hash = BlockHash::from_bytes(
            block
                .hash
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?,
        );
        match reader.block_hash_lookup(hash).map_err(store_status)? {
            BlockHashLookup::Resolved(block_id) => Ok(block_id.height),
            BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => Err(
                Status::not_found("block is not in the canonical best chain"),
            ),
        }
    }
}

pub(crate) fn spawn_node_readiness_probe<Source>(
    source: Source,
    readiness: CompactServingReadiness,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, ConfigError>
where
    Source: NodeSource,
{
    if !source.capabilities().supports(NodeCapability::TipId) {
        return Err(ConfigError::invalid(
            "compact-blocks serving requires the node TipId capability",
        ));
    }
    Ok(tokio::spawn(async move {
        loop {
            let observation = match source.tip_id().await {
                Ok(_) => source.poll_upstream_health().await,
                Err(error) => Err(error),
            };
            match observation {
                Ok(snapshot) if snapshot.ready_for_queries => {
                    readiness.publish_node(ReadinessCause::Ready);
                }
                Ok(snapshot) => {
                    readiness.publish_node(ReadinessCause::UpstreamNotReady(
                        UpstreamNotReadyDetail {
                            upstream_committed_height: snapshot.upstream_committed_height,
                            upstream_estimated_height: snapshot.upstream_estimated_height,
                            upstream_verification_progress: snapshot.upstream_verification_progress,
                            upstream_health: UpstreamHealth {
                                source: snapshot.source,
                                reason: snapshot.reason,
                            },
                        },
                    ));
                }
                Err(error) => {
                    readiness.publish_node(ReadinessCause::NodeUnavailable(
                        NodeUnavailableDetail::first_iteration(
                            "node_unavailable",
                            error.to_string(),
                        ),
                    ));
                }
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {}
            }
        }
    }))
}

#[tonic::async_trait]
impl compact_tx_streamer_server::CompactTxStreamer for CompactBlockAdapter {
    type GetBlockRangeStream = GrpcStream<lightwalletd::CompactBlock>;
    type GetBlockRangeNullifiersStream = GrpcStream<lightwalletd::CompactBlock>;
    type GetTaddressTxidsStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetMempoolTxStream = GrpcStream<lightwalletd::CompactTx>;
    type GetMempoolStreamStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;
    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_lightd_info(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::LightdInfo>, Status> {
        let reader = self.reader();
        let epoch = reader.chain_epoch().map_err(store_status)?;
        // Before the writer's first admission, reader admission requires the
        // startup table to equal the store's construction table.
        let activations = reader
            .network_upgrade_activations()
            .unwrap_or(&self.activations);
        if epoch.network != activations.network() {
            return Err(Status::failed_precondition(
                "admitted network does not match node activations",
            ));
        }
        Ok(Response::new(lightd_info(
            activations,
            epoch.visible_tip_height.value(),
            false,
        )))
    }

    async fn get_latest_block(
        &self,
        _request: Request<lightwalletd::ChainSpec>,
    ) -> Result<Response<lightwalletd::BlockId>, Status> {
        let reader = self.reader();
        let epoch = reader.chain_epoch().map_err(store_status)?;
        Ok(Response::new(lightwalletd::BlockId {
            height: u64::from(epoch.visible_tip_height.value()),
            hash: encode_internal_block_hash(epoch.visible_tip_hash).to_vec(),
        }))
    }

    async fn get_block(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let reader = self.reader();
        let request = request.into_inner();
        let height = Self::resolve_height(reader.as_ref(), &request)?;
        let epoch = reader.chain_epoch().map_err(store_status)?;
        if height > epoch.visible_tip_height {
            return Err(Status::out_of_range(
                "requested block is newer than latest block",
            ));
        }
        let block = reader
            .compact_block_at(height)
            .map_err(store_status)?
            .ok_or_else(|| Status::not_found("block is not indexed"))?;
        let response = compact_block_to_lightwalletd(&block)?;
        if !request.hash.is_empty() && request.hash != response.hash {
            return Err(Status::not_found(
                "requested block hash does not match indexed block",
            ));
        }
        Ok(Response::new(response))
    }

    async fn get_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let reader = self.reader();
        let request = request.into_inner();
        let start = request
            .start
            .ok_or_else(|| Status::invalid_argument("range.start is required"))?;
        let end = request
            .end
            .ok_or_else(|| Status::invalid_argument("range.end is required"))?;
        let start = BlockHeight::new(
            u32::try_from(start.height)
                .map_err(|_| Status::invalid_argument("range height exceeds u32"))?,
        );
        let end = BlockHeight::new(
            u32::try_from(end.height)
                .map_err(|_| Status::invalid_argument("range height exceeds u32"))?,
        );
        let descending = start > end;
        let range = if descending {
            BlockHeightRange::inclusive(end, start)
        } else {
            BlockHeightRange::inclusive(start, end)
        };
        let epoch = reader.chain_epoch().map_err(store_status)?;
        if range.end > epoch.visible_tip_height {
            return Err(Status::out_of_range(
                "requested range is newer than latest block",
            ));
        }
        let pool_selection = pool_selection_from_request(&request.pool_types)?;
        let chunks = range_chunks(range, descending);
        let Some((first, remaining)) = chunks.split_first() else {
            return Ok(Response::new(
                Box::pin(tokio_stream::iter(std::iter::empty())) as Self::GetBlockRangeStream,
            ));
        };
        let first_messages =
            encode_chunk(Arc::clone(&reader), *first, pool_selection, descending).await?;
        if remaining.is_empty() {
            return Ok(Response::new(
                Box::pin(tokio_stream::iter(first_messages)) as Self::GetBlockRangeStream
            ));
        }
        let (sender, receiver) = mpsc::channel(RANGE_CHANNEL_CAPACITY);
        let reader = Arc::clone(&reader);
        let remaining = remaining.to_vec();
        tokio::spawn(async move {
            for chunk in remaining {
                let chunk_result = tokio::task::spawn_blocking({
                    let reader = Arc::clone(&reader);
                    move || {
                        encode_chunk_blocking(reader.as_ref(), chunk, pool_selection, descending)
                    }
                })
                .await
                .map_err(|error| {
                    Status::unavailable(format!("compact range worker failed: {error}"))
                })
                .and_then(std::convert::identity);
                match chunk_result {
                    Ok(messages) => {
                        for message in messages {
                            if sender.send(message).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::iter(first_messages).chain(ReceiverStream::new(receiver)),
        ) as Self::GetBlockRangeStream))
    }

    async fn get_block_nullifiers(
        &self,
        _request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        Err(Self::unsupported("GetBlockNullifiers"))
    }
    async fn get_block_range_nullifiers(
        &self,
        _request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Err(Self::unsupported("GetBlockRangeNullifiers"))
    }
    async fn get_transaction(
        &self,
        _request: Request<lightwalletd::TxFilter>,
    ) -> Result<Response<lightwalletd::RawTransaction>, Status> {
        Err(Self::unsupported("GetTransaction"))
    }
    async fn send_transaction(
        &self,
        _request: Request<lightwalletd::RawTransaction>,
    ) -> Result<Response<lightwalletd::SendResponse>, Status> {
        Err(Self::unsupported("SendTransaction"))
    }
    async fn get_taddress_txids(
        &self,
        _request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Self::unsupported("GetTaddressTxids"))
    }
    async fn get_taddress_transactions(
        &self,
        _request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Self::unsupported("GetTaddressTransactions"))
    }
    async fn get_taddress_balance(
        &self,
        _request: Request<lightwalletd::AddressList>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        Err(Self::unsupported("GetTaddressBalance"))
    }
    async fn get_taddress_balance_stream(
        &self,
        _request: Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        Err(Self::unsupported("GetTaddressBalanceStream"))
    }
    async fn get_mempool_tx(
        &self,
        _request: Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Self::unsupported("GetMempoolTx"))
    }
    async fn get_mempool_stream(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Self::unsupported("GetMempoolStream"))
    }
    async fn get_tree_state(
        &self,
        _request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        Err(Self::unsupported("GetTreeState"))
    }
    async fn get_latest_tree_state(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        Err(Self::unsupported("GetLatestTreeState"))
    }
    async fn get_subtree_roots(
        &self,
        _request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Self::unsupported("GetSubtreeRoots"))
    }
    async fn get_address_utxos(
        &self,
        _request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<lightwalletd::GetAddressUtxosReplyList>, Status> {
        Err(Self::unsupported("GetAddressUtxos"))
    }
    async fn get_address_utxos_stream(
        &self,
        _request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Self::unsupported("GetAddressUtxosStream"))
    }
    async fn ping(
        &self,
        _request: Request<lightwalletd::Duration>,
    ) -> Result<Response<lightwalletd::PingResponse>, Status> {
        Err(Self::unsupported("Ping"))
    }
}

fn store_status(error: CanonicalStoreError) -> Status {
    status_from_query_error(&QueryError::CanonicalStore(error))
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "the four protocol pool toggles are independent operator selections"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PoolSelection {
    sapling: bool,
    orchard: bool,
    ironwood: bool,
    transparent: bool,
}

fn pool_selection_from_request(pool_types: &[i32]) -> Result<PoolSelection, Status> {
    if pool_types.is_empty() {
        return Ok(PoolSelection {
            sapling: true,
            orchard: true,
            ironwood: true,
            transparent: false,
        });
    }
    let mut selection = PoolSelection {
        sapling: false,
        orchard: false,
        ironwood: false,
        transparent: false,
    };
    for pool_type in pool_types {
        match lightwalletd::PoolType::try_from(*pool_type) {
            Ok(lightwalletd::PoolType::Sapling) => selection.sapling = true,
            Ok(lightwalletd::PoolType::Orchard) => selection.orchard = true,
            Ok(lightwalletd::PoolType::Ironwood) => selection.ironwood = true,
            Ok(lightwalletd::PoolType::Transparent) => selection.transparent = true,
            Ok(lightwalletd::PoolType::Invalid) | Err(_) => {
                return Err(Status::invalid_argument("invalid pool type requested"));
            }
        }
    }
    Ok(selection)
}

fn range_chunks(range: BlockHeightRange, descending: bool) -> Vec<BlockHeightRange> {
    let mut chunks = Vec::new();
    let mut start = range.start.value();
    let end = range.end.value();
    while start <= end {
        let chunk_end = start
            .saturating_add(DEFAULT_MAX_COMPACT_BLOCK_RANGE.get().saturating_sub(1))
            .min(end);
        chunks.push(BlockHeightRange::inclusive(
            BlockHeight::new(start),
            BlockHeight::new(chunk_end),
        ));
        if chunk_end == end {
            break;
        }
        start = chunk_end.saturating_add(1);
    }
    if descending {
        chunks.reverse();
    }
    chunks
}

async fn encode_chunk(
    reader: Arc<RocksDbCanonicalSecondary>,
    range: BlockHeightRange,
    selection: PoolSelection,
    descending: bool,
) -> Result<Vec<Result<lightwalletd::CompactBlock, Status>>, Status> {
    tokio::task::spawn_blocking(move || {
        encode_chunk_blocking(reader.as_ref(), range, selection, descending)
    })
    .await
    .map_err(|error| Status::unavailable(format!("compact range worker failed: {error}")))?
}

fn encode_chunk_blocking(
    reader: &RocksDbCanonicalSecondary,
    range: BlockHeightRange,
    selection: PoolSelection,
    descending: bool,
) -> Result<Vec<Result<lightwalletd::CompactBlock, Status>>, Status> {
    let mut blocks = reader
        .compact_blocks_in_range(range)
        .map_err(store_status)?
        .into_iter()
        .map(|block| {
            compact_block_to_lightwalletd(&block)
                .map(|message| prune_pool_types(message, selection))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if descending {
        blocks.reverse();
    }
    Ok(blocks.into_iter().map(Ok).collect())
}

fn prune_pool_types(
    mut block: lightwalletd::CompactBlock,
    selection: PoolSelection,
) -> lightwalletd::CompactBlock {
    for transaction in &mut block.vtx {
        if !selection.sapling {
            transaction.spends.clear();
            transaction.outputs.clear();
        }
        if !selection.orchard {
            transaction.actions.clear();
        }
        if !selection.ironwood {
            transaction.ironwood_actions.clear();
        }
        if !selection.transparent {
            transaction.vin.clear();
            transaction.vout.clear();
        }
    }
    block
}

fn compact_block_to_lightwalletd(
    block: &CompactBlockArtifact,
) -> Result<lightwalletd::CompactBlock, Status> {
    let metadata = block.chain_metadata();
    Ok(lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx: block
            .transactions()
            .iter()
            .map(compact_transaction_to_lightwalletd)
            .collect::<Result<Vec<_>, _>>()?,
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    })
}

fn compact_transaction_to_lightwalletd(
    transaction: &CompactTransaction,
) -> Result<lightwalletd::CompactTx, Status> {
    compact_transaction_data_to_lightwalletd(
        transaction.index,
        transaction.transaction_id,
        &transaction.data,
    )
}

fn compact_transaction_data_to_lightwalletd(
    index: u64,
    transaction_id: zinder_core::TransactionId,
    transaction_data: &CompactTransactionData,
) -> Result<lightwalletd::CompactTx, Status> {
    let fee = transaction_data
        .fee_zat
        .map(u32::try_from)
        .transpose()
        .map_err(|_| Status::data_loss("compact transaction fee exceeds u32"))?
        .unwrap_or_default();
    Ok(lightwalletd::CompactTx {
        index,
        txid: encode_internal_transaction_id(transaction_id).to_vec(),
        fee,
        spends: transaction_data
            .sapling_spends
            .iter()
            .map(|spend| lightwalletd::CompactSaplingSpend {
                nf: spend.nullifier.to_vec(),
            })
            .collect(),
        outputs: transaction_data
            .sapling_outputs
            .iter()
            .map(|output| lightwalletd::CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: transaction_data
            .orchard_actions
            .iter()
            .map(|action| lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                cmx: action.commitment.to_vec(),
                ephemeral_key: action.ephemeral_key.to_vec(),
                ciphertext: action.ciphertext.to_vec(),
            })
            .collect(),
        ironwood_actions: transaction_data
            .ironwood_actions
            .iter()
            .map(|action| lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                cmx: action.commitment.to_vec(),
                ephemeral_key: action.ephemeral_key.to_vec(),
                ciphertext: action.ciphertext.to_vec(),
            })
            .collect(),
        vin: transaction_data
            .transparent_inputs
            .iter()
            .map(|input| lightwalletd::CompactTxIn {
                prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                    .to_vec(),
                prevout_index: input.previous_output_index,
            })
            .collect(),
        vout: transaction_data
            .transparent_outputs
            .iter()
            .map(|output| lightwalletd::TxOut {
                script_pub_key: output.script_pub_key.clone(),
                value: output.value_zat,
            })
            .collect(),
    })
}

fn lightd_info(
    activations: &NetworkUpgradeActivations,
    height: u32,
    taddr_support: bool,
) -> lightwalletd::LightdInfo {
    let tip_height = BlockHeight::new(height);
    let consensus_branch_id = activations.active_at(tip_height).map_or_else(
        || "00000000".to_owned(),
        |activation| encode_branch_id_hex(activation.branch_id),
    );
    // The protocol defines `upgradeName` and `upgradeHeight` as the next
    // pending upgrade, not the active one.
    let next_pending = activations
        .activations()
        .iter()
        .find(|activation| activation.activation_height > tip_height);
    let upgrade_name = next_pending.map_or_else(String::new, |activation| activation.name.clone());
    let upgrade_height = next_pending.map_or(0, |activation| {
        u64::from(activation.activation_height.value())
    });
    let sapling_activation_height = activations
        .activation_height_by_name("Sapling")
        .map_or(0, |height| u64::from(height.value()));
    lightwalletd::LightdInfo {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        vendor: "Zinder".to_owned(),
        taddr_support,
        chain_name: zinder_core::wire::encode_bip70_chain_name(activations.network()).to_owned(),
        sapling_activation_height,
        consensus_branch_id,
        block_height: u64::from(height),
        git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
        branch: String::new(),
        build_date: String::new(),
        build_user: String::new(),
        estimated_height: u64::from(height),
        zcashd_build: String::new(),
        zcashd_subversion: String::new(),
        donation_address: String::new(),
        upgrade_name,
        upgrade_height,
        lightwallet_protocol_version:
            zinder_proto::compat::lightwalletd::LIGHTWALLETD_PROTOCOL_COMMIT.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{
        CompactBlockAdapter, CompactBlockPublisher, CompactBlockServingConfig,
        CompactBlockServingSlot, CompactServingReadiness, PoolSelection, WriterFenceRelation,
        classify_writer_fence, compact_block_to_lightwalletd, range_chunks,
        transport_failure_cause,
    };
    use compact_tx_streamer_server::CompactTxStreamer;
    use parking_lot::Mutex;
    use prost::Message;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request, transport::Server};
    use zinder_core::{
        BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
        CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, ChainTipMetadata,
        CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers,
        ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
        SerializedBytesDigest, ShieldedProtocol, UnixTimestampMillis,
        encode_canonical_block_replay,
    };
    use zinder_proto::compat::lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient;
    use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server};
    use zinder_proto::v1::ingest::{
        AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
        CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse, CanonicalWriterFence,
        CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
        CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
        ReadmitCanonicalOwnerCheckpointRequest, ReleaseCanonicalProjectionBuildLeaseRequest,
        ReleaseCanonicalProjectionBuildLeaseResponse, RenewCanonicalProjectionBuildLeaseRequest,
        canonical_control_server::{CanonicalControl, CanonicalControlServer},
    };
    use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
        CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention,
        RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbResourceBudget,
    };

    fn assert_unimplemented<T>(result: Result<T, tonic::Status>) {
        assert!(result.is_err());
        let Some(status) = result.err() else {
            return;
        };
        assert_eq!(status.code(), Code::Unimplemented);
    }

    #[derive(Clone)]
    struct MutableCanonicalControl {
        writer_status: Arc<Mutex<CanonicalWriterStatusResponse>>,
        /// Answers served once each, in order, before `writer_status`.
        queued_statuses: Arc<Mutex<std::collections::VecDeque<CanonicalWriterStatusResponse>>>,
        available: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MutableCanonicalControl {
        fn new(writer_status: CanonicalWriterStatusResponse) -> Self {
            Self {
                writer_status: Arc::new(Mutex::new(writer_status)),
                queued_statuses: Arc::new(Mutex::new(std::collections::VecDeque::new())),
                available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        fn queue_writer_status(&self, writer_status: CanonicalWriterStatusResponse) {
            self.queued_statuses.lock().push_back(writer_status);
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
        ) -> Result<tonic::Response<CanonicalWriterStatusResponse>, tonic::Status> {
            if !self.available.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(tonic::Status::unavailable("fixture writer status outage"));
            }
            let queued = self.queued_statuses.lock().pop_front();
            Ok(tonic::Response::new(
                queued.unwrap_or_else(|| self.writer_status.lock().clone()),
            ))
        }

        async fn event_page(
            &self,
            _request: Request<CanonicalEventPageRequest>,
        ) -> Result<tonic::Response<CanonicalEventPageResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn create_owner_checkpoint(
            &self,
            _request: Request<CreateCanonicalOwnerCheckpointRequest>,
        ) -> Result<tonic::Response<CreateCanonicalOwnerCheckpointResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn readmit_owner_checkpoint(
            &self,
            _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
        ) -> Result<tonic::Response<CreateCanonicalOwnerCheckpointResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn acquire_projection_build_lease(
            &self,
            _request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<CanonicalProjectionBuildLeaseResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn renew_projection_build_lease(
            &self,
            _request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<CanonicalProjectionBuildLeaseResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn release_projection_build_lease(
            &self,
            _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<ReleaseCanonicalProjectionBuildLeaseResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }
    }

    fn lifecycle_build_block(
        block: &zinder_testkit::FixtureBlock,
        activations: &NetworkUpgradeActivations,
    ) -> CanonicalBuildBlock {
        let facts = CanonicalBlockFacts {
            block_header: block.block_header_artifact(),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &block.raw_block_bytes,
            ),
            transactions: Vec::new(),
        };
        CanonicalBuildBlock {
            replay_envelope: encode_canonical_block_replay(
                &facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            ),
            compact_block: block.compact_block_artifact(),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
                BlockId::new(block.height, block.hash),
                block.block_time_seconds,
                lifecycle_frontiers(activations, block.height),
            )),
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        }
    }

    /// Live following derives each checkpoint's frontiers from its
    /// predecessor, so every shielded pool active at `height` carries an
    /// empty frontier.
    fn lifecycle_frontiers(
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> CommitmentTreeFrontiers {
        let active_frontier = |protocol: ShieldedProtocol| {
            activations
                .activation_height_by_name(protocol.activation_upgrade_name())
                .is_some_and(|activation_height| activation_height <= height)
                .then(|| CommitmentTreeFrontier::empty(protocol))
        };
        CommitmentTreeFrontiers::from_validated_parts(
            active_frontier(ShieldedProtocol::Sapling),
            active_frontier(ShieldedProtocol::Orchard),
            active_frontier(ShieldedProtocol::Ironwood),
        )
    }

    fn lifecycle_activations() -> NetworkUpgradeActivations {
        let upgrades = [
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
        NetworkUpgradeActivations::new(Network::ZcashRegtest, upgrades)
            .unwrap_or_else(|_| NetworkUpgradeActivations::empty(Network::ZcashRegtest))
    }

    fn lifecycle_writer_status(store: &RocksDbCanonicalStore) -> CanonicalWriterStatusResponse {
        let fence = store.event_fence();
        CanonicalWriterStatusResponse {
            network_name: zinder_core::wire::encode_zinder_native_chain_name(Network::ZcashRegtest)
                .to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: fence.chain_epoch_id().value(),
                event_sequence: fence.chain_event_sequence(),
                visible_tip_height: fence.visible_tip().height.value(),
                visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
                visible_block_count: fence.sequence_digest().block_count(),
                canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
            }),
            oldest_retained_event_sequence: 1,
            network_upgrade_schedule: store.network_upgrade_schedule().map(|schedule| {
                zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                    schedule.activations(),
                    schedule.revision(),
                )
            }),
            reorg_window_exceeded: None,
        }
    }

    fn lifecycle_store(
        root: &std::path::Path,
        activations: &zinder_core::NetworkUpgradeActivations,
    ) -> Result<
        (RocksDbCanonicalStore, Vec<zinder_testkit::FixtureBlock>),
        Box<dyn std::error::Error>,
    > {
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut blocks = chain.blocks().to_vec();
        let first = blocks
            .first_mut()
            .ok_or("lifecycle fixture has no baseline block")?;
        first.parent_hash = Network::ZcashRegtest.genesis_hash();
        let first = first.clone();
        let builder_plan = CanonicalStoreBuildPlan::complete(
            activations,
            first.block_time_seconds.saturating_sub(1),
            BlockId::new(first.height, first.hash),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            root,
            CanonicalStoreWorkload::Wallet,
            builder_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(std::iter::once(Ok::<_, std::io::Error>(
            lifecycle_build_block(&first, activations),
        )))?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            BlockId::new(first.height, first.hash),
            first.block_time_seconds,
            lifecycle_frontiers(activations, first.height),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BlockId::new(first.height, first.hash),
            UnixTimestampMillis::new(u64::from(first.block_time_seconds) * 1_000),
        ))?;
        Ok((validated.publish_baseline(publication)?, blocks))
    }

    #[test]
    fn range_chunks_are_bounded_and_preserve_direction() {
        let ascending = range_chunks(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2_005)),
            false,
        );
        assert_eq!(ascending.len(), 3);
        assert_eq!(ascending[0].start, BlockHeight::new(1));
        assert_eq!(ascending[2].end, BlockHeight::new(2_005));
        assert!(
            ascending
                .iter()
                .all(|chunk| { chunk.end.value().saturating_sub(chunk.start.value()) < 1_000 })
        );

        let descending = range_chunks(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2_005)),
            true,
        );
        assert_eq!(descending[0].start, BlockHeight::new(2_001));
        assert_eq!(descending[2].start, BlockHeight::new(1));
    }

    #[test]
    fn empty_pool_request_is_shielded_only() {
        let selection_result = super::pool_selection_from_request(&[]);
        assert!(selection_result.is_ok());
        let Some(selection) = selection_result.ok() else {
            return;
        };
        assert_eq!(
            selection,
            PoolSelection {
                sapling: true,
                orchard: true,
                ironwood: true,
                transparent: false,
            }
        );
    }

    #[test]
    fn invalid_pool_request_fails_before_storage_read() {
        let error_result = super::pool_selection_from_request(&[99]);
        assert!(error_result.is_err());
        let Some(error) = error_result.err() else {
            return;
        };
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn readiness_is_conjunctive_and_shutdown_is_irreversible() {
        let runtime = Readiness::default();
        let readiness = CompactServingReadiness::new(runtime.clone());
        readiness.publish_storage(ReadinessState::ready(Some(10)));
        assert!(!runtime.report().is_ready);
        readiness.publish_node(ReadinessCause::Ready);
        assert!(runtime.report().is_ready);
        readiness.publish_shutting_down();
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
        readiness.publish_node(ReadinessCause::Ready);
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The lifecycle test keeps admission, publication, lease retention, outage, and identity rejection in one deterministic causal path."
    )]
    async fn publisher_lifecycle_replaces_and_preserves_readiness_while_leased()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (mut primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });

        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        serving_readiness.publish_node(ReadinessCause::Ready);
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            activations: Arc::new(activations.clone()),
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_millis(50),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let initial = slot.capture();
        assert_eq!(
            initial.event_fence().visible_tip().height,
            BlockHeight::new(1)
        );

        let second = blocks
            .get(1)
            .cloned()
            .ok_or("lifecycle fixture has no append block")?;
        let expected_fence = primary.event_fence();
        let (next_primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&second, &activations),
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(2_000),
            ),
            &activations,
        )?;
        primary = next_primary;
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        let replaced = slot.capture();
        assert_eq!(
            replaced.event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        assert!(!Arc::ptr_eq(&initial, &replaced));

        let third = blocks
            .get(2)
            .cloned()
            .ok_or("lifecycle fixture has no second append block")?;
        let expected_fence = primary.event_fence();
        let (next_primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&third, &activations),
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(3_000),
            ),
            &activations,
        )?;
        primary = next_primary;
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "serving_pair_stale"
        );
        assert_eq!(
            slot.capture().event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        assert_eq!(
            initial.event_fence().visible_tip().height,
            BlockHeight::new(1)
        );

        control.set_available(false);
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "serving_pair_stale"
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        control.set_available(true);
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "replica_lagging");

        control.set_available(false);
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "writer_status_unavailable"
        );

        control.set_available(true);
        let mut invalid = lifecycle_writer_status(&primary);
        invalid.network_name = "wrong-network".to_owned();
        control.set_writer_status(invalid);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut missing_fence = lifecycle_writer_status(&primary);
        missing_fence.fence = None;
        control.set_writer_status(missing_fence);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut behind = lifecycle_writer_status(&primary);
        if let Some(fence) = behind.fence.as_mut() {
            fence.chain_epoch_id = 0;
        }
        control.set_writer_status(behind);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut same_epoch_mismatch = lifecycle_writer_status(&primary);
        if let Some(fence) = same_epoch_mismatch.fence.as_mut() {
            fence.chain_epoch_id = 2;
            fence.visible_tip_height = fence.visible_tip_height.saturating_add(1);
        }
        control.set_writer_status(same_epoch_mismatch);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        drop(initial);
        control.set_available(true);
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        let reused = slot.capture();
        assert_eq!(
            reused.event_fence().visible_tip().height,
            BlockHeight::new(3)
        );
        assert!(!Arc::ptr_eq(&replaced, &reused));

        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_handle = publisher.spawn(cancel.clone(), drained.clone());
        cancel.cancel();
        drained.cancel();
        publisher_handle.await?;
        drop(replaced);
        drop(reused);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// Compact serving refuses a parked writer with its halt.
    ///
    /// A parked writer keeps attesting its frozen fence, so the halt must come
    /// from canonical writer status: at bootstrap, on every refresh, and until
    /// the writer clears it.
    #[tokio::test]
    async fn publisher_refuses_a_parked_writer_with_the_typed_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let halt = zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let parked_status = CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(halt),
            ..lifecycle_writer_status(&primary)
        };
        let control = MutableCanonicalControl::new(parked_status.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });

        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        serving_readiness.publish_node(ReadinessCause::Ready);
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            activations: Arc::new(activations.clone()),
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let expected_halt = ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let bootstrapped = readiness.report();
        assert!(!bootstrapped.is_ready);
        assert_eq!(bootstrapped.cause, expected_halt);

        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert!(readiness.report().is_ready);

        control.set_writer_status(parked_status);
        publisher.refresh().await;
        let parked = readiness.report();
        assert!(!parked.is_ready);
        assert_eq!(parked.cause, expected_halt);

        // A writer-status outage after the halt must keep refusing traffic,
        // never reopen it as a stale reader within the staleness ceiling.
        control.set_available(false);
        publisher.refresh().await;
        let after_outage = readiness.report();
        assert!(!after_outage.is_ready);
        assert_eq!(after_outage.cause, expected_halt);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A writer that parks between a refresh's first status and the status
    /// that attests the rotated reader must leave the reader refusing with
    /// the halt, also through a later writer-status outage.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, the append, the park between two statuses, and the outage form one causal sequence"
    )]
    async fn rotation_attested_by_a_parked_writer_refuses_with_the_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        serving_readiness.publish_node(ReadinessCause::Ready);
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            activations: Arc::new(activations.clone()),
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        assert!(readiness.report().is_ready);

        let second = blocks
            .get(1)
            .cloned()
            .ok_or("lifecycle fixture has no append block")?;
        let expected_fence = primary.event_fence();
        let (primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&second, &activations),
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(2_000),
            ),
            &activations,
        )?;
        // The refresh's first status is still following; the writer parks
        // before the status that attests the rotated reader.
        control.queue_writer_status(lifecycle_writer_status(&primary));
        control.set_writer_status(CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            ..lifecycle_writer_status(&primary)
        });
        publisher.refresh().await;
        assert_eq!(
            slot.capture().event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        let expected_halt = ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let rotated = readiness.report();
        assert!(!rotated.is_ready);
        assert_eq!(rotated.cause, expected_halt);

        control.set_available(false);
        publisher.refresh().await;
        let after_outage = readiness.report();
        assert!(!after_outage.is_ready);
        assert_eq!(after_outage.cause, expected_halt);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A runtime started on the pre-upgrade table keeps refreshing after the
    /// chain crosses an activation the writer admitted later.
    ///
    /// `GetLightdInfo` reports the admitted upgrade as pending below its
    /// height and as the active branch from that height on.
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The scenario keeps startup, writer admission, the pending and crossing refreshes, and the served upgrade in one causal sequence."
    )]
    async fn publisher_keeps_refreshing_after_the_chain_crosses_a_later_admitted_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        // A pre-NU7 table whose upgrades are all active by height 2, so NU7 is
        // the only upgrade that can be pending.
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            lifecycle_activations()
                .activations()
                .iter()
                .cloned()
                .map(|activation| {
                    let activation_height = if matches!(
                        activation.name.as_str(),
                        "Overwinter" | "Sapling" | "Blossom" | "Heartwood" | "Canopy"
                    ) {
                        BlockHeight::new(1)
                    } else {
                        BlockHeight::new(2)
                    };
                    NetworkUpgradeActivation {
                        activation_height,
                        ..activation
                    }
                })
                .collect(),
        )?;
        let branch_active_at = |height: u32| {
            activations
                .active_at(BlockHeight::new(height))
                .map(|activation| zinder_core::wire::encode_branch_id_hex(activation.branch_id))
                .ok_or("the startup table must have an active upgrade")
        };
        let canopy_branch_id = branch_active_at(1)?;
        let nu6_3_branch_id = branch_active_at(2)?;
        let (mut primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        // The writer's first start admits the unchanged node table as
        // revision 1 before the compatibility runtime starts with that table.
        primary.admit_network_upgrade_schedule(&activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let serving_readiness = CompactServingReadiness::new(Readiness::default());
        serving_readiness.publish_node(ReadinessCause::Ready);
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            activations: Arc::new(activations.clone()),
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 4,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let adapter = CompactBlockAdapter::new(slot.clone(), Arc::new(activations.clone()));
        let startup = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?
            .into_inner();
        assert_eq!(startup.consensus_branch_id, canopy_branch_id);
        assert_eq!(startup.upgrade_name, "NU5");
        assert_eq!(startup.upgrade_height, 2);

        // The node is upgraded and only the writer restarts: it admits the
        // new table, which activates NU7 at height 3, as revision 2.
        let nu7_branch_id = ConsensusBranchId::new(0x7719_0ad9);
        let mut entries = activations.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: nu7_branch_id,
            activation_height: BlockHeight::new(3),
            name: "NU7".to_owned(),
        });
        let upgraded = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
        primary.admit_network_upgrade_schedule(&upgraded)?;
        for block in blocks.iter().skip(1) {
            let expected_fence = primary.event_fence();
            let (next_primary, _) = primary.commit_live_append(
                CanonicalLiveAppend::new(
                    expected_fence,
                    lifecycle_build_block(block, &upgraded),
                    Vec::new(),
                    expected_fence.visible_tip(),
                    UnixTimestampMillis::new(u64::from(block.height.value()) * 1_000),
                ),
                &upgraded,
            )?;
            primary = next_primary;
            control.set_writer_status(lifecycle_writer_status(&primary));
            publisher.refresh().await;
            let published_reader = slot.capture();
            assert_eq!(published_reader.event_fence(), primary.event_fence());
            assert_eq!(
                published_reader.network_upgrade_activations(),
                Some(&upgraded)
            );
            drop(published_reader);

            let info = adapter
                .get_lightd_info(Request::new(lightwalletd::Empty::default()))
                .await?
                .into_inner();
            assert_eq!(info.block_height, u64::from(block.height.value()));
            if block.height < BlockHeight::new(3) {
                assert_eq!(info.consensus_branch_id, nu6_3_branch_id);
                assert_eq!(info.upgrade_name, "NU7");
                assert_eq!(info.upgrade_height, 3);
            } else {
                assert_eq!(
                    info.consensus_branch_id,
                    zinder_core::wire::encode_branch_id_hex(nu7_branch_id)
                );
                assert_eq!(info.upgrade_name, "");
                assert_eq!(info.upgrade_height, 0);
            }
        }

        drop(adapter);
        drop(publisher);
        drop(primary);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn writer_fence_classification_distinguishes_exact_ahead_and_invalid() {
        let network = zinder_core::Network::ZcashRegtest;
        let status = |epoch| CanonicalWriterStatusResponse {
            network_name: zinder_core::wire::encode_zinder_native_chain_name(network).to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: epoch,
                ..CanonicalWriterFence::default()
            }),
            oldest_retained_event_sequence: 0,
            network_upgrade_schedule: None,
            reorg_window_exceeded: None,
        };
        assert_eq!(
            classify_writer_fence(&status(4), network, 4, true),
            WriterFenceRelation::Exact
        );
        assert_eq!(
            classify_writer_fence(&status(5), network, 4, false),
            WriterFenceRelation::Ahead(1)
        );
        assert_eq!(
            classify_writer_fence(&status(4), network, 4, false),
            WriterFenceRelation::SameEpochMismatch
        );
        assert_eq!(
            classify_writer_fence(&status(3), network, 4, false),
            WriterFenceRelation::BehindOrInvalid
        );
        let wrong_network = CanonicalWriterStatusResponse {
            network_name: "mainnet".to_owned(),
            ..status(4)
        };
        assert_eq!(
            classify_writer_fence(&wrong_network, network, 4, true),
            WriterFenceRelation::BehindOrInvalid
        );
        let missing_fence = CanonicalWriterStatusResponse {
            fence: None,
            ..status(4)
        };
        assert_eq!(
            classify_writer_fence(&missing_fence, network, 4, true),
            WriterFenceRelation::BehindOrInvalid
        );
    }

    #[test]
    fn transport_outage_uses_stale_grace_then_fails_closed() {
        assert_eq!(
            transport_failure_cause(Some(Duration::from_secs(2))),
            ReadinessCause::ServingPairStale {
                lag_chain_epochs: 0,
                staleness_seconds: 2,
            }
        );
        assert_eq!(
            transport_failure_cause(None),
            ReadinessCause::WriterStatusUnavailable
        );
    }

    #[tokio::test]
    async fn snapshot_capture_pins_old_generation_until_request_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut first =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let mut second =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (first_reader, first_wallet) = first.take_readers()?;
        let (second_reader, second_wallet) = second.take_readers()?;
        drop(first_wallet);
        drop(second_wallet);

        let slot = CompactBlockServingSlot::new(Arc::new(first_reader));
        let pinned = slot.capture();
        let retired = slot.publish(Arc::new(second_reader));
        assert_eq!(
            retired.event_fence().visible_tip(),
            pinned.event_fence().visible_tip()
        );
        assert!(Arc::strong_count(&retired) >= 2);
        drop(pinned);
        assert_eq!(Arc::strong_count(&retired), 1);
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The fixture contract test keeps the supported and explicitly rejected RPC surface auditable together."
    )]
    async fn fixture_adapter_serves_four_methods_and_rejects_the_rest()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let expected_tip = canonical.event_fence().visible_tip();
        let expected_block = canonical
            .compact_block_at(BlockHeight::new(2))?
            .ok_or("fixture block missing")?;
        let expected_wire = compact_block_to_lightwalletd(&expected_block)?;
        let adapter = CompactBlockAdapter::new(
            CompactBlockServingSlot::new(Arc::new(canonical)),
            activations,
        );

        let info = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?
            .into_inner();
        assert!(!info.taddr_support);
        assert_eq!(info.block_height, u64::from(expected_tip.height.value()));

        let latest = adapter
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await?
            .into_inner();
        assert_eq!(latest.height, u64::from(expected_tip.height.value()));

        let block = adapter
            .get_block(Request::new(lightwalletd::BlockId {
                height: 2,
                hash: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(block.encode_to_vec(), expected_wire.encode_to_vec());

        let transparent = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: 2,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: 2,
                    hash: Vec::new(),
                }),
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            }))
            .await?
            .into_inner()
            .next()
            .await
            .ok_or("transparent range returned no block")??;
        assert!(transparent.vtx.iter().all(|transaction| {
            transaction.spends.is_empty()
                && transaction.outputs.is_empty()
                && transaction.actions.is_empty()
                && transaction.ironwood_actions.is_empty()
        }));

        let mut stream = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: 4,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: 1,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            }))
            .await?
            .into_inner();
        let mut heights = Vec::new();
        while let Some(block) = stream.next().await {
            heights.push(block?.height);
        }
        assert_eq!(heights, vec![4, 3, 2, 1]);

        assert_unimplemented(
            adapter
                .get_block_nullifiers(Request::new(lightwalletd::BlockId::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_block_range_nullifiers(Request::new(lightwalletd::BlockRange::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_transaction(Request::new(lightwalletd::TxFilter::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .send_transaction(Request::new(lightwalletd::RawTransaction::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_balance(Request::new(lightwalletd::AddressList::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_txids(Request::new(
                    lightwalletd::TransparentAddressBlockFilter::default(),
                ))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_transactions(Request::new(
                    lightwalletd::TransparentAddressBlockFilter::default(),
                ))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_mempool_stream(Request::new(lightwalletd::Empty::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_latest_tree_state(Request::new(lightwalletd::Empty::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_tree_state(Request::new(lightwalletd::BlockId::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_address_utxos_stream(Request::new(lightwalletd::GetAddressUtxosArg::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .ping(Request::new(lightwalletd::Duration::default()))
                .await,
        );
        Ok(())
    }

    #[tokio::test]
    async fn generated_client_rejects_client_streaming_rpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let adapter = CompactBlockAdapter::new(
            CompactBlockServingSlot::new(Arc::new(canonical)),
            Arc::clone(&activations),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(compact_tx_streamer_server::CompactTxStreamerServer::new(
                    adapter,
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let mut client = CompactTxStreamerClient::connect(endpoint).await?;
        let error = client
            .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address::default()]))
            .await
            .err()
            .ok_or("client-streaming RPC unexpectedly succeeded")?;
        assert_eq!(error.code(), Code::Unimplemented);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }
}
