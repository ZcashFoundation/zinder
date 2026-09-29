//! Zinder wallet projector release binary.

use std::{net::SocketAddr, num::NonZeroU32, path::PathBuf, process::ExitCode, time::Duration};

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFactsSequenceDigest, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_runtime::{
    OpsServer, Readiness, ReadinessState, cancel_on_terminating_signal,
    host_cpu_meets_compiled_baseline, install_tracing_subscriber, spawn_ops_endpoint,
};
use zinder_source::{ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalEventCursor, CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalReorgPolicy,
    CanonicalRetainedEvent, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    RocksDbCanonicalSecondary,
};
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionBuildLeaseRequest, WalletProjectionBuildOwner,
    WalletProjectionRetainedEventAnchor, WalletProjectionSourcePosition,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildStore, RocksDbWalletFollowingStore,
    RocksDbWalletStore, WalletBuildLeaseHeartbeat, WalletBuildLeasePhase,
    WalletProjectionBuildLeaseExecution, build_wallet_from_canonical_with_lease_and_heartbeat,
};

mod canonical_writer_control;
mod config;
mod projector_control;

use canonical_writer_control::{CanonicalRetentionLease, CanonicalWriterControlClient};
use config::{ProjectorConfigOverrides, ProjectorError};
use projector_control::{
    ProjectorControlCommand, ProjectorControlGrpcAdapter, projector_control_channel,
};
use zinder_projector::PROJECTOR_SERVICE_NAME;
use zinder_projector::state_bundle::{
    CanonicalCheckpointAdmissionEvidence, complete_state_bundle_capture,
    prepare_state_bundle_capture,
};

const CANONICAL_FENCE_CONVERGENCE_INITIAL_DELAY: Duration = Duration::from_millis(50);
const CANONICAL_FENCE_CONVERGENCE_SYNCING_AFTER: Duration = Duration::from_secs(1);
const CANONICAL_FENCE_CONVERGENCE_MAX_DELAY: Duration = Duration::from_secs(1);
/// Renew before a transient control-plane delay can let a live cursor expire.
const FOLLOW_RETENTION_LEASE_RENEWAL_HEADROOM: Duration = Duration::from_mins(1);
/// The writer deliberately limits each control RPC; the projector composes at
/// one authenticated page at a time and commits durable progress before it
/// asks for the next page.
const CANONICAL_CONTROL_EVENT_PAGE_EVENTS: u32 = 1_024;
const CANONICAL_CONTROL_EVENT_PAGE_EVENTS_USIZE: usize =
    CANONICAL_CONTROL_EVENT_PAGE_EVENTS as usize;
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Parser)]
#[command(name = "zinder-projector")]
#[command(about = "Zinder wallet projection construction service")]
#[command(version)]
struct Cli {
    /// TOML configuration file loaded before environment and CLI overrides.
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,
    /// Print resolved configuration without opening stores or binding sockets.
    #[arg(long = "print-config", global = true)]
    print_config: bool,
    /// Zinder-native network name.
    #[arg(long)]
    network: Option<String>,
    /// Canonical primary path consumed only through a `RocksDB` secondary.
    #[arg(long = "canonical-path")]
    canonical_path: Option<PathBuf>,
    /// Process-owned canonical secondary metadata path.
    #[arg(long = "canonical-secondary-path")]
    canonical_secondary_path: Option<PathBuf>,
    /// Wallet projection primary path owned by this process.
    #[arg(long = "wallet-path")]
    wallet_path: Option<PathBuf>,
    /// Canonical and wallet replacement window.
    #[arg(long = "reorg-window-blocks")]
    reorg_window_blocks: Option<u32>,
    /// Stable 16-byte projection builder identity encoded as 32 hex characters.
    #[arg(long = "build-owner-hex")]
    build_owner_hex: Option<String>,
    /// Explicit duration of each durable projection-build lease (at least four hours).
    #[arg(long = "lease-duration-seconds")]
    lease_duration_seconds: Option<u64>,
    /// Zebra JSON-RPC endpoint used only to authenticate the activation table.
    #[arg(long = "node-json-rpc-addr")]
    node_json_rpc_addr: Option<String>,
    /// Private ingest-control endpoint used for retained-event lease ownership.
    #[arg(long = "ingest-control-addr")]
    ingest_control_addr: Option<String>,
    /// File containing the ingest-control bearer token.
    #[arg(long = "ingest-control-token-path")]
    ingest_control_bearer_token_path: Option<PathBuf>,
    /// Loopback-only owner control endpoint for coherent checkpoint capture.
    #[arg(long = "projector-control-listen-addr")]
    projector_control_listen_addr: Option<SocketAddr>,
    /// File containing the bearer token required by projector owner control.
    #[arg(long = "projector-control-token-path")]
    projector_control_bearer_token_path: Option<PathBuf>,
    /// Shared state-bundle candidate root, matching ingest checkpoint staging.
    #[arg(long = "projector-control-checkpoint-staging-root")]
    projector_control_checkpoint_staging_root: Option<PathBuf>,
    /// Operational HTTP endpoint for health, readiness, and metrics.
    #[arg(long = "ops-listen-addr")]
    ops_listen_addr: Option<SocketAddr>,
}

impl From<Cli> for ProjectorConfigOverrides {
    fn from(cli: Cli) -> Self {
        Self {
            network: cli.network,
            canonical_path: cli.canonical_path,
            canonical_secondary_path: cli.canonical_secondary_path,
            wallet_path: cli.wallet_path,
            reorg_window_blocks: cli.reorg_window_blocks,
            build_owner_hex: cli.build_owner_hex,
            lease_duration_seconds: cli.lease_duration_seconds,
            node_json_rpc_addr: cli.node_json_rpc_addr,
            ingest_control_addr: cli.ingest_control_addr,
            ingest_control_bearer_token_path: cli.ingest_control_bearer_token_path,
            projector_control_listen_addr: cli.projector_control_listen_addr,
            projector_control_bearer_token_path: cli.projector_control_bearer_token_path,
            projector_control_checkpoint_staging_root: cli
                .projector_control_checkpoint_staging_root,
            ops_listen_addr: cli.ops_listen_addr,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing_subscriber();
    if !host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }
    if cli.print_config {
        return print_config(cli);
    }
    match Box::pin(run_projector(cli)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_error(&error),
    }
}

#[allow(
    clippy::print_stdout,
    reason = "--print-config is a structured TOML data dump, not a log event"
)]
fn print_config(cli: Cli) -> ExitCode {
    let config_path = cli.config_path.clone();
    match config::load_projector_config(config_path, cli.into())
        .and_then(|resolved| config::projector_config_toml(&resolved))
    {
        Ok(rendered) => {
            println!("{rendered}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_error(&error),
    }
}

async fn run_projector(cli: Cli) -> Result<(), ProjectorError> {
    let config_path = cli.config_path.clone();
    let config = config::load_projector_config(config_path, cli.into())?;
    let readiness = Readiness::default();
    let ops_handle = config.ops_listen_addr.map(|listen_addr| {
        spawn_ops_endpoint(
            listen_addr,
            OpsServer {
                service_name: PROJECTOR_SERVICE_NAME,
                service_version: env!("CARGO_PKG_VERSION"),
                network_name: encode_zinder_native_chain_name(config.network),
                advertised_capabilities: Vec::new(),
            },
            readiness.clone(),
        )
    });
    let cancel = CancellationToken::new();
    let _signal = cancel_on_terminating_signal(cancel.clone());

    let projector_outcome =
        Box::pin(run_owned_projector(&config, &readiness, cancel.clone())).await;
    readiness.set(ReadinessState::not_ready(
        zinder_runtime::ReadinessCause::ShuttingDown,
    ));
    cancel.cancel();
    if let Some(ops_handle) = ops_handle {
        ops_handle.shutdown().await;
    }
    projector_outcome
}

#[allow(
    clippy::too_many_lines,
    reason = "the projector ownership sequence is intentionally visible in one fail-closed order"
)]
async fn run_owned_projector(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    cancel: CancellationToken,
) -> Result<(), ProjectorError> {
    let source = ZebraJsonRpcSource::with_options(
        config.network,
        &config.node.json_rpc_addr,
        config.node.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: config.node.request_timeout,
            max_response_bytes: config.node.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let activations = source
        .discover_network_upgrade_activations(PROJECTOR_SERVICE_NAME)
        .await?;
    let reorg_policy = CanonicalReorgPolicy::new(config.reorg_window_blocks).map_err(|error| {
        zinder_runtime::ConfigError::invalid(format!(
            "projector.reorg_window_blocks is invalid: {error}"
        ))
    })?;
    let mut canonical = RocksDbCanonicalSecondary::open_ready(
        &config.canonical_path,
        &config.canonical_secondary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        reorg_policy,
        config.canonical_rocksdb_budget,
    )?;
    let mut canonical_control = CanonicalWriterControlClient::connect(
        &config.ingest_control_addr,
        config.ingest_control_bearer_token.as_ref(),
        config.projector_control.bearer_token.as_ref(),
    )
    .await?;
    let Some(canonical_ready) = converge_on_writer_fence(
        &mut canonical,
        &mut canonical_control,
        readiness,
        &cancel,
        config.fence_convergence_timeout,
    )
    .await?
    else {
        return Ok(());
    };
    let expected_wallet_source = canonical_source_identity(&canonical_ready);

    if config.wallet_path.exists() {
        match RocksDbWalletStore::open_ready_for_following(
            &config.wallet_path,
            config.network,
            config.wallet_rocksdb_budget,
        ) {
            Ok(wallet) => {
                let wallet_source =
                    WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
                match admit_resumed_following(
                    &mut canonical_control,
                    wallet_source,
                    config.lease_duration,
                )
                .await?
                {
                    ResumedFollowingAdmission::Anchored(canonical_lease) => {
                        tracing::info!(
                            target: "zinder::projector",
                            event = "wallet_projection_following_resumed",
                            height = wallet_source.source_position().tip.height.value(),
                            event_sequence = wallet_source.source_position().event_sequence,
                            "resuming wallet projection from its persisted authenticated cursor"
                        );
                        return run_continuous_wallet_following(
                            config,
                            readiness,
                            cancel,
                            canonical,
                            canonical_control,
                            wallet,
                            canonical_lease,
                        )
                        .await;
                    }
                    ResumedFollowingAdmission::BootstrapFirstRetainedEvent {
                        first_retained_event_sequence,
                    } => {
                        tracing::info!(
                            target: "zinder::projector",
                            event = "wallet_projection_following_bootstrap",
                            height = wallet_source.source_position().tip.height.value(),
                            event_sequence = wallet_source.source_position().event_sequence,
                            first_retained_event_sequence,
                            "bootstrapping following from the one retained event after the persisted wallet cursor"
                        );
                        return bootstrap_resumed_wallet_following(
                            config,
                            readiness,
                            cancel,
                            canonical,
                            canonical_control,
                            wallet,
                            first_retained_event_sequence,
                        )
                        .await;
                    }
                }
            }
            Err(zinder_wallet_rocksdb::RocksDbWalletError::StoreNotReady { .. }) => {
                RocksDbWalletBuildStore::open(
                    &config.wallet_path,
                    config.network,
                    config.wallet_rocksdb_budget,
                )?
                .discard_unpublished(UnixTimestampMillis::now())?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    readiness.set(ReadinessState::syncing(
        Some(canonical_ready.visible_block_count),
        None,
        Some(canonical_ready.visible_tip.height.value()),
    ));
    let initial_now = UnixTimestampMillis::now();
    let initial_expiry = lease_expiry(initial_now, config.lease_duration);
    let canonical_cursor = CanonicalEventCursor::at(canonical_ready.visible_event_sequence)?;
    let canonical_lease = CanonicalRetentionLease::new(
        config.build_owner,
        canonical_ready.visible_epoch.value(),
        canonical_cursor.as_bytes().to_vec(),
        initial_expiry,
    );
    let canonical_lease = canonical_control.acquire(canonical_lease).await?;
    let canonical_lease_for_recovery = canonical_lease.clone();
    let wallet_lease_request = WalletProjectionBuildLeaseRequest::new(
        WalletProjectionBuildOwner::from_bytes(config.build_owner),
        expected_wallet_source,
        WalletProjectionRetainedEventAnchor::new(canonical_ready.visible_event_sequence),
        initial_expiry,
    );
    let wallet_lease_execution =
        WalletProjectionBuildLeaseExecution::new(wallet_lease_request, initial_now);
    let build_options = RocksDbWalletBuildOptions {
        resource_budget: config.wallet_rocksdb_budget,
        max_outpoint_sort_memory_bytes: config.build.max_outpoint_sort_memory_bytes,
        max_secondary_sort_memory_bytes_per_sorter: config
            .build
            .max_secondary_sort_memory_bytes_per_sorter,
        max_temporary_file_bytes_per_sorter: config.build.max_temporary_file_bytes_per_sorter,
        sst_target_logical_bytes: config.build.sst_target_logical_bytes,
        max_accounted_reorg_undo_bytes: config.build.max_accounted_reorg_undo_bytes,
        supported_reorg_depth: config.reorg_window_blocks,
    };
    let lease_duration = config.lease_duration;
    let build_cancel = cancel.clone();
    let runtime = tokio::runtime::Handle::current();
    let wallet_path = config.wallet_path.clone();
    let network = config.network;
    let joined_build = tokio::task::spawn_blocking(move || {
        let mut canonical_control = canonical_control;
        let mut canonical_lease = canonical_lease;
        let mut heartbeat =
            |phase, wallet_lease: zinder_wallet_projection::WalletProjectionBuildLease| {
                if build_cancel.is_cancelled() {
                    return Err(
                        zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled,
                    );
                }
                let now = UnixTimestampMillis::now();
                let renew_until = lease_expiry(now, lease_duration);
                // Before READY promotion, renew even when the freshly computed
                // expiry happens to equal the current one. The writer uses this
                // RPC to prove that the exact retained build anchor is still
                // live; later following can safely reconcile from that anchor
                // even when the writer advanced during the long build.
                if is_strict_lease_extension(renew_until, canonical_lease.expires_at())
                    || phase == WalletBuildLeasePhase::BeforePromotion
                {
                    let candidate = if is_strict_lease_extension(
                        renew_until,
                        canonical_lease.expires_at(),
                    ) {
                        canonical_lease.renewed(renew_until)
                    } else {
                        canonical_lease.clone()
                    };
                    canonical_lease = runtime
                        .block_on(canonical_control.renew(candidate))
                        .map_err(|error| {
                            tracing::error!(
                                target: "zinder::projector",
                                ?phase,
                                %error,
                                "canonical retention lease renewal failed"
                            );
                            zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled
                        })?;
                }
                if phase == WalletBuildLeasePhase::BeforePromotion {
                    let writer_status = runtime
                        .block_on(canonical_control.writer_status())
                        .map_err(|error| {
                            tracing::error!(
                                target: "zinder::projector",
                                %error,
                                "canonical writer fence query failed before wallet promotion"
                            );
                            zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled
                        })?;
                    require_pre_promotion_follower_admission(
                        writer_status,
                        &canonical_lease,
                        expected_wallet_source,
                        network,
                    )
                    .inspect_err(|_| {
                        tracing::error!(
                            target: "zinder::projector",
                            "canonical retention anchor was not live or writer did not remain at-or-after the pinned construction event before wallet promotion"
                        );
                    })?;
                }
                if is_strict_lease_extension(renew_until, wallet_lease.expires_at()) {
                    Ok(WalletBuildLeaseHeartbeat::renew(now, renew_until))
                } else {
                    Ok(WalletBuildLeaseHeartbeat::at(now))
                }
            };
        let build_outcome = build_wallet_from_canonical_with_lease_and_heartbeat(
            &canonical,
            wallet_path,
            build_options,
            wallet_lease_execution,
            &mut heartbeat,
        );
        (build_outcome, canonical, canonical_control, canonical_lease)
    })
    .await;

    let (build_result, canonical, mut canonical_control, canonical_lease) = match joined_build {
        Ok(build_completion) => build_completion,
        Err(error) => {
            best_effort_release_canonical_lease(config, &canonical_lease_for_recovery).await;
            return Err(error.into());
        }
    };

    let outcome = match build_result {
        Ok(outcome) => outcome,
        Err(build_error) => {
            if let Err(release_error) = canonical_control.release(&canonical_lease).await {
                tracing::error!(
                    target: "zinder::projector",
                    %release_error,
                    "best-effort canonical retention lease release failed after wallet construction failed"
                );
            }
            return Err(build_error.into());
        }
    };
    let report = outcome.report;
    drop(outcome.store);
    let wallet = match RocksDbWalletStore::open_ready_for_following(
        &config.wallet_path,
        config.network,
        config.wallet_rocksdb_budget,
    ) {
        Ok(wallet) => wallet,
        Err(error) => {
            if let Err(release_error) = canonical_control.release(&canonical_lease).await {
                tracing::error!(
                    target: "zinder::projector",
                    %release_error,
                    "best-effort canonical retention lease release failed after wallet following admission failed"
                );
            }
            return Err(error.into());
        }
    };
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    require_built_wallet_source(expected_wallet_source, wallet_source)?;
    let canonical_lease = advance_retention_lease_anchor(
        &mut canonical_control,
        canonical_lease,
        wallet_source,
        config.lease_duration,
    )
    .await?;
    let ready_height = report.source_position.tip.height.value();
    tracing::info!(
        target: "zinder::projector",
        event = "wallet_projection_construction_complete",
        height = ready_height,
        chain_epoch = report.source_position.chain_epoch_id.value(),
        event_sequence = report.source_position.event_sequence,
        row_count = wallet_row_count(report.row_counts),
        projection_digest = %display_digest(&report.projection_digest.as_bytes()),
        build_seconds = report.phase_durations.total.as_secs_f64(),
        "wallet projection constructed, cold-validated, and entering continuous following"
    );
    run_continuous_wallet_following(
        config,
        readiness,
        cancel,
        canonical,
        canonical_control,
        wallet,
        canonical_lease,
    )
    .await
}

/// Owns one wallet primary while it continuously reconciles to the canonical writer.
///
/// The handle remains following-only for its whole lifetime. A query process
/// must independently reopen the wallet at an exact admitted fence.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the live projector keeps its fence, retention, replay, and READY ordering visible in one ownership loop"
)]
async fn run_continuous_wallet_following(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    cancel: CancellationToken,
    mut canonical: RocksDbCanonicalSecondary,
    mut canonical_control: CanonicalWriterControlClient,
    mut wallet: RocksDbWalletFollowingStore,
    mut canonical_lease: CanonicalRetentionLease,
) -> Result<(), ProjectorError> {
    let mut control = ProjectorControlTasks::start(&config.projector_control, &cancel).await?;
    loop {
        control.require_running(&cancel)?;
        if cancel.is_cancelled() {
            tracing::info!(
                target: "zinder::projector",
                event = "wallet_projection_following_cancelled",
                "stopping wallet following without releasing the bounded retention lease"
            );
            return Ok(());
        }

        let Some(canonical_ready) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            readiness,
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        let target_source = canonical_source_identity(&canonical_ready);
        let target_fence = canonical.event_fence();
        let wallet_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        if wallet_source == target_source {
            let renewed = renew_following_retention_lease_if_due(
                &mut canonical_control,
                &mut canonical_lease,
                wallet_source,
                config.lease_duration,
            )
            .await?;
            if renewed {
                let Some(post_renewal_canonical) = converge_on_writer_fence(
                    &mut canonical,
                    &mut canonical_control,
                    readiness,
                    &cancel,
                    config.fence_convergence_timeout,
                )
                .await?
                else {
                    return Ok(());
                };
                if wallet_source != canonical_source_identity(&post_renewal_canonical) {
                    continue;
                }
            }
            readiness.set(ReadinessState::ready(Some(
                wallet_source.source_position().tip.height.value(),
            )));
            if let Some(command) = control.next_command_or_poll(&cancel).await? {
                apply_projector_control_command(
                    config,
                    readiness,
                    target_fence,
                    &mut canonical_control,
                    &mut wallet,
                    &mut canonical_lease,
                    command,
                )
                .await;
            } else if cancel.is_cancelled() {
                return Ok(());
            }
            continue;
        }
        if wallet_source.source_position().event_sequence
            >= target_source.source_position().event_sequence
        {
            return Err(ProjectorError::CanonicalEventPageInvalid {
                reason: "wallet and canonical event cursors diverged without a forward retained transition",
            });
        }

        set_following_syncing(readiness, wallet_source, target_source);
        let _renewed = renew_following_retention_lease_if_due(
            &mut canonical_control,
            &mut canonical_lease,
            wallet_source,
            config.lease_duration,
        )
        .await?;

        let retained_events = match fetch_retained_event_page_to_target(
            &mut canonical_control,
            &canonical,
            wallet_source,
            target_fence,
        )
        .await
        {
            Ok(RetainedEventPageValidation::RetryAfterWriterAdvance) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(ProjectorError::CanonicalEventCursorExpired {
                wallet_event_sequence,
                oldest_retained_event_sequence,
            }) => {
                return Err(ProjectorError::WalletRebuildRequired {
                    wallet_event_sequence,
                    oldest_retained_event_sequence,
                });
            }
            Err(error) => return Err(error),
        };
        let transition = plan_next_wallet_transition(&canonical, &retained_events)?;
        renew_following_retention_lease_for_transition(
            &mut canonical_control,
            &mut canonical_lease,
            wallet_source,
            config.lease_duration,
        )
        .await?;
        let Some(reconciled_source) = apply_next_wallet_transition(
            &mut wallet,
            &canonical,
            wallet_source,
            transition,
            config.reorg_window_blocks,
            config.follow.max_transition_logical_bytes,
            &cancel,
        )?
        else {
            return Ok(());
        };
        canonical_lease = advance_retention_lease_anchor(
            &mut canonical_control,
            canonical_lease,
            reconciled_source,
            config.lease_duration,
        )
        .await?;
    }
}

/// Private capture-control task state owned for the same lifetime as the
/// following wallet primary. The adapter cannot mutate storage directly; it
/// can only submit one command to this owner loop.
struct ProjectorControlTasks {
    commands: Option<mpsc::Receiver<ProjectorControlCommand>>,
    server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl ProjectorControlTasks {
    async fn start(
        config: &zinder_runtime::ResolvedProjectorControl,
        cancel: &CancellationToken,
    ) -> Result<Self, ProjectorError> {
        let Some(listen_addr) = config.listen_addr else {
            return Ok(Self {
                commands: None,
                server: None,
            });
        };
        let Some(bearer_token) = config.bearer_token.clone() else {
            return Err(ProjectorError::ProjectorControlTokenMissing);
        };
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .map_err(|source| ProjectorError::ProjectorControlBind {
                address: listen_addr,
                source,
            })?;
        let (handle, commands) = projector_control_channel();
        let adapter = ProjectorControlGrpcAdapter::new(handle, bearer_token);
        let server_cancel = cancel.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(adapter.into_server())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await
        });
        Ok(Self {
            commands: Some(commands),
            server: Some(server),
        })
    }

    fn require_running(&self, cancel: &CancellationToken) -> Result<(), ProjectorError> {
        if !cancel.is_cancelled()
            && self
                .server
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Err(ProjectorError::ProjectorControlStopped);
        }
        Ok(())
    }

    async fn next_command_or_poll(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<ProjectorControlCommand>, ProjectorError> {
        let (Some(commands), Some(server)) = (self.commands.as_mut(), self.server.as_mut()) else {
            let _cancelled = wait_for_follow_poll(cancel).await;
            return Ok(None);
        };
        tokio::select! {
            () = cancel.cancelled() => Ok(None),
            command = commands.recv() => Ok(command),
            server_outcome = server => {
                let server_outcome = server_outcome?;
                if cancel.is_cancelled() {
                    return Ok(None);
                }
                match server_outcome {
                    Ok(()) => Err(ProjectorError::ProjectorControlStopped),
                    Err(error) => Err(ProjectorError::ProjectorControlServer(error)),
                }
            },
            () = tokio::time::sleep(FOLLOW_POLL_INTERVAL) => Ok(None),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the wallet-owner command boundary keeps capture state and its sole mutable owners explicit"
)]
async fn apply_projector_control_command(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    expected_fence: CanonicalEventFence,
    canonical_control: &mut CanonicalWriterControlClient,
    wallet: &mut RocksDbWalletFollowingStore,
    canonical_lease: &mut CanonicalRetentionLease,
    command: ProjectorControlCommand,
) {
    match command {
        ProjectorControlCommand::CreateStateBundleCapture {
            candidate_id,
            reply,
        } => {
            readiness.set(ReadinessState::syncing(
                None,
                None,
                Some(wallet.ready_evidence().source_position.tip.height.value()),
            ));
            let capture_outcome = capture_state_bundle(
                config,
                canonical_control,
                wallet,
                canonical_lease,
                candidate_id,
                expected_fence,
            )
            .await;
            let _reply_sent = reply.send(capture_outcome);
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "coherent capture explicitly carries both storage owners, the protected lease, and the fixed fence"
)]
async fn capture_state_bundle(
    config: &config::ProjectorConfig,
    canonical_control: &mut CanonicalWriterControlClient,
    wallet: &mut RocksDbWalletFollowingStore,
    canonical_lease: &mut CanonicalRetentionLease,
    candidate_id: String,
    expected_fence: CanonicalEventFence,
) -> Result<zinder_proto::v1::ingest::CreateStateBundleCaptureResponse, tonic::Status> {
    let renewed_until = lease_expiry(UnixTimestampMillis::now(), config.lease_duration);
    let renewal = canonical_lease.renewed(renewed_until);
    *canonical_lease = canonical_control
        .renew(renewal)
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let paths = prepare_state_bundle_capture(
        &config.projector_control.checkpoint_staging_root,
        &candidate_id,
    )
    .map_err(|error| state_bundle_status(&error))?;
    let response = canonical_control
        .create_owner_checkpoint(
            candidate_id,
            paths.staging_root_binding().to_vec(),
            canonical_writer_fence_message(expected_fence),
        )
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let evidence = CanonicalCheckpointAdmissionEvidence::try_from(response)
        .map_err(|error| state_bundle_status(&error))?;
    let renewed_until = lease_expiry(UnixTimestampMillis::now(), config.lease_duration);
    *canonical_lease = canonical_control
        .renew(canonical_lease.renewed(renewed_until))
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let re_admitted_response = canonical_control
        .readmit_owner_checkpoint(
            evidence.candidate_id().to_owned(),
            paths.staging_root_binding().to_vec(),
            evidence.visible_fence(),
            evidence.database_identity().to_vec(),
        )
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let re_admitted = CanonicalCheckpointAdmissionEvidence::try_from(re_admitted_response)
        .map_err(|error| state_bundle_status(&error))?;
    evidence
        .verify_exact_readmission(&re_admitted)
        .map_err(|error| state_bundle_status(&error))?;
    let manifest =
        complete_state_bundle_capture(&paths, &evidence, wallet, config.wallet_rocksdb_budget)
            .map_err(|error| state_bundle_status(&error))?;
    let network = manifest
        .network()
        .map_err(|error| state_bundle_status(&error))?;
    Ok(zinder_proto::v1::ingest::CreateStateBundleCaptureResponse {
        candidate_id: manifest.candidate_id().to_owned(),
        state_bundle_identity: manifest.identity().to_owned(),
        state_bundle_format_version: u32::from(manifest.format_version()),
        topology: manifest.topology().to_owned(),
        network_name: encode_zinder_native_chain_name(network).to_owned(),
        chain_epoch_id: manifest.fence().chain_epoch_id(),
        chain_event_sequence: manifest.fence().chain_event_sequence(),
        visible_tip_height: manifest.fence().visible_tip_height(),
    })
}

fn canonical_writer_fence_message(
    fence: CanonicalEventFence,
) -> zinder_proto::v1::ingest::CanonicalWriterFence {
    zinder_proto::v1::ingest::CanonicalWriterFence {
        chain_epoch_id: fence.chain_epoch_id().value(),
        event_sequence: fence.chain_event_sequence(),
        visible_tip_height: fence.visible_tip().height.value(),
        visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
        canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
        visible_block_count: fence.sequence_digest().block_count(),
    }
}

fn state_bundle_status(error: &zinder_projector::state_bundle::StateBundleError) -> tonic::Status {
    tracing::warn!(
        target: "zinder::projector",
        %error,
        "state-bundle capture refused"
    );
    tonic::Status::failed_precondition(error.to_string())
}

/// Bridges a persisted wallet whose cursor event was pruned.
///
/// Its immediate successor remains retained. The successor lease is acquired
/// before any wallet mutation, then the page is fetched again under that lease
/// so a prune or writer-advance race cannot create an unprotected gap.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the bootstrap keeps the retained-page, lease, transition, and readiness ordering explicit"
)]
async fn bootstrap_resumed_wallet_following(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    cancel: CancellationToken,
    mut canonical: RocksDbCanonicalSecondary,
    mut canonical_control: CanonicalWriterControlClient,
    mut wallet: RocksDbWalletFollowingStore,
    first_retained_event_sequence: u64,
) -> Result<(), ProjectorError> {
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let mut bootstrap_lease = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let Some(canonical_ready) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            readiness,
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        let target_source = canonical_source_identity(&canonical_ready);
        set_following_syncing(readiness, wallet_source, target_source);
        let retained_events = match fetch_retained_event_page_to_target(
            &mut canonical_control,
            &canonical,
            wallet_source,
            canonical.event_fence(),
        )
        .await
        {
            Ok(RetainedEventPageValidation::RetryAfterWriterAdvance) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(ProjectorError::CanonicalEventCursorExpired {
                wallet_event_sequence,
                oldest_retained_event_sequence,
            }) => {
                return Err(ProjectorError::WalletRebuildRequired {
                    wallet_event_sequence,
                    oldest_retained_event_sequence,
                });
            }
            Err(error) => return Err(error),
        };
        let bootstrap_source = bootstrap_first_retained_event_source(
            &canonical,
            &retained_events,
            first_retained_event_sequence,
        )?;
        match acquire_following_retention_lease(
            &mut canonical_control,
            bootstrap_source,
            config.lease_duration,
        )
        .await
        {
            Ok(lease) => break lease,
            Err(error) => return Err(error),
        }
    };

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let Some(canonical_ready) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            readiness,
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        let target_source = canonical_source_identity(&canonical_ready);
        set_following_syncing(readiness, wallet_source, target_source);
        let retained_events = match fetch_retained_event_page_to_target(
            &mut canonical_control,
            &canonical,
            wallet_source,
            canonical.event_fence(),
        )
        .await
        {
            Ok(RetainedEventPageValidation::RetryAfterWriterAdvance) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(ProjectorError::CanonicalEventCursorExpired {
                wallet_event_sequence,
                oldest_retained_event_sequence,
            }) => {
                return Err(ProjectorError::WalletRebuildRequired {
                    wallet_event_sequence,
                    oldest_retained_event_sequence,
                });
            }
            Err(error) => return Err(error),
        };
        let bootstrap_source = bootstrap_first_retained_event_source(
            &canonical,
            &retained_events,
            first_retained_event_sequence,
        )?;
        require_retention_lease_anchor(&bootstrap_lease, bootstrap_source)?;

        let transition = plan_next_wallet_transition(&canonical, &retained_events)?;
        renew_following_retention_lease_for_transition(
            &mut canonical_control,
            &mut bootstrap_lease,
            bootstrap_source,
            config.lease_duration,
        )
        .await?;
        let Some(reconciled_source) = apply_next_wallet_transition(
            &mut wallet,
            &canonical,
            wallet_source,
            transition,
            config.reorg_window_blocks,
            config.follow.max_transition_logical_bytes,
            &cancel,
        )?
        else {
            return Ok(());
        };
        let canonical_lease = advance_retention_lease_anchor(
            &mut canonical_control,
            bootstrap_lease,
            reconciled_source,
            config.lease_duration,
        )
        .await?;
        return run_continuous_wallet_following(
            config,
            readiness,
            cancel,
            canonical,
            canonical_control,
            wallet,
            canonical_lease,
        )
        .await;
    }
}

/// Returns a source fence lease for the first retained event after a pruned cursor.
///
/// This gate reads the exact historical canonical epoch, so its retained
/// successor lease includes the same settlement boundary the transition must
/// persist before a wallet write.
fn bootstrap_first_retained_event_source(
    canonical: &RocksDbCanonicalSecondary,
    retained_events: &[CanonicalRetainedEvent],
    first_retained_event_sequence: u64,
) -> Result<WalletCanonicalSourceIdentity, ProjectorError> {
    let Some(first_event) = retained_events.first().copied() else {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "bootstrap retained-event page is empty",
        });
    };
    let resulting_fence = first_event.resulting_fence();
    if first_event.cursor().event_sequence() != first_retained_event_sequence
        || resulting_fence.chain_event_sequence() != first_retained_event_sequence
        || first_event.resulting_epoch_id() != resulting_fence.chain_epoch_id()
    {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "bootstrap retained-event page no longer starts at the leased successor",
        });
    }
    Ok(wallet_source_identity_from_fence(
        resulting_fence,
        settled_tip_for_event(canonical, first_event)?,
    ))
}

fn set_following_syncing(
    readiness: &Readiness,
    wallet_source: WalletCanonicalSourceIdentity,
    target_source: WalletCanonicalSourceIdentity,
) {
    let wallet_height = wallet_source.source_position().tip.height.value();
    let target_height = target_source.source_position().tip.height.value();
    readiness.set(ReadinessState::syncing(
        target_height.checked_sub(wallet_height).map(u64::from),
        Some(wallet_height),
        Some(target_height),
    ));
}

/// Executes one next retained-event transition.
///
/// Append-only lag progresses one event at a time. Only a run whose
/// intermediate results were overwritten by a later reorg becomes one direct
/// rollback-and-replay publication.
#[allow(
    clippy::too_many_arguments,
    reason = "the transition execution keeps its source, replay, byte budget, and cancellation fence explicit"
)]
fn apply_next_wallet_transition(
    wallet: &mut RocksDbWalletFollowingStore,
    canonical: &RocksDbCanonicalSecondary,
    wallet_source: WalletCanonicalSourceIdentity,
    transition: NextWalletTransition,
    supported_reorg_depth: u32,
    max_logical_bytes: std::num::NonZeroU64,
    cancel: &CancellationToken,
) -> Result<Option<WalletCanonicalSourceIdentity>, ProjectorError> {
    let transition_fence = transition.resulting_fence();
    let resulting_settled_tip = settled_tip_for_transition(canonical, &transition)?;
    let expected_result =
        wallet_source_identity_from_fence(transition_fence, resulting_settled_tip);
    let transition_result = match transition {
        NextWalletTransition::ApplyOne(event) => {
            let replay_range = event.committed_range();
            require_bounded_reconciliation_replay(replay_range)?;
            let replay_rows = canonical.scan_canonical_replay_range(replay_range)?;
            wallet.apply_canonical_event_range_cancellable(
                wallet_source,
                event,
                transition_fence,
                resulting_settled_tip,
                max_logical_bytes,
                replay_rows,
                || cancel.is_cancelled(),
            )
        }
        NextWalletTransition::ReconcileOverwritten { events, .. } => {
            let common_ancestor = discover_common_ancestor(
                wallet,
                canonical,
                transition_fence,
                supported_reorg_depth,
            )?;
            let (rollback_range, replay_range) = reconciliation_ranges(
                wallet_source.source_position().tip,
                common_ancestor,
                transition_fence.visible_tip(),
            )?;
            require_bounded_reconciliation_replay(replay_range)?;
            let replay_rows = canonical.scan_canonical_replay_range(replay_range)?;
            wallet.reconcile_canonical_event_sequence_cancellable(
                wallet_source,
                &events,
                transition_fence,
                resulting_settled_tip,
                rollback_range,
                replay_range,
                max_logical_bytes,
                replay_rows,
                || cancel.is_cancelled(),
            )
        }
    };
    match transition_result {
        Ok(()) => {}
        Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionTransitionCancelled)
            if cancel.is_cancelled() =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    }
    let observed = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    if observed != expected_result {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "wallet transition returned a source fence different from the authenticated retained-event result",
        });
    }
    Ok(Some(observed))
}

fn settled_tip_for_transition(
    canonical: &RocksDbCanonicalSecondary,
    transition: &NextWalletTransition,
) -> Result<BlockId, ProjectorError> {
    let event =
        match transition {
            NextWalletTransition::ApplyOne(event) => *event,
            NextWalletTransition::ReconcileOverwritten { events, .. } => events
                .last()
                .copied()
                .ok_or(ProjectorError::CanonicalEventPageInvalid {
                    reason: "collapsed reconciliation has no final retained event",
                })?,
        };
    settled_tip_for_event(canonical, event)
}

async fn wait_for_follow_poll(cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(FOLLOW_POLL_INTERVAL) => false,
    }
}

async fn acquire_following_retention_lease(
    control: &mut CanonicalWriterControlClient,
    source: WalletCanonicalSourceIdentity,
    lease_duration: Duration,
) -> Result<CanonicalRetentionLease, ProjectorError> {
    let now = UnixTimestampMillis::now();
    let position = source.source_position();
    let lease = CanonicalRetentionLease::new(
        fresh_retention_lease_id()?,
        position.chain_epoch_id.value(),
        position.event_cursor.as_bytes().to_vec(),
        lease_expiry(now, lease_duration),
    );
    control.acquire(lease).await.map_err(Into::into)
}

/// Resume path selected from the current writer retention floor.
enum ResumedFollowingAdmission {
    /// The persisted wallet event is still retained and can be leased directly.
    Anchored(CanonicalRetentionLease),
    /// Retention begins at the immediate successor; lease it before the first write.
    BootstrapFirstRetainedEvent { first_retained_event_sequence: u64 },
}

/// Classifies whether the persisted cursor still has a direct retention
/// anchor, can safely bootstrap through its one retained successor, or needs
/// a side-by-side rebuild.
fn classify_resumed_following_retention(
    wallet_source: WalletCanonicalSourceIdentity,
    oldest_retained_event_sequence: u64,
) -> Result<ResumedFollowingRetentionPlan, ProjectorError> {
    let wallet_event_sequence = wallet_source.source_position().event_sequence;
    if oldest_retained_event_sequence == 0 {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer omitted a nonzero retained-event floor while resuming following",
        });
    }
    let next_wallet_event_sequence =
        wallet_event_sequence
            .checked_add(1)
            .ok_or(ProjectorError::CanonicalEventPageInvalid {
                reason: "wallet event sequence cannot advance while resuming following",
            })?;
    if oldest_retained_event_sequence <= wallet_event_sequence {
        return Ok(ResumedFollowingRetentionPlan::AnchorPersistedCursor);
    }
    if oldest_retained_event_sequence == next_wallet_event_sequence {
        return Ok(ResumedFollowingRetentionPlan::BootstrapFirstRetainedEvent {
            first_retained_event_sequence: oldest_retained_event_sequence,
        });
    }
    Err(ProjectorError::WalletRebuildRequired {
        wallet_event_sequence,
        oldest_retained_event_sequence,
    })
}

/// Internal classification before the async lease acquisition makes it
/// generation-bearing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumedFollowingRetentionPlan {
    AnchorPersistedCursor,
    BootstrapFirstRetainedEvent { first_retained_event_sequence: u64 },
}

/// Admits a READY wallet without silently treating a lost cursor as restartable.
///
/// A lease-acquire race is classified from a fresh writer status so the
/// immediate-successor bootstrap remains available.
async fn admit_resumed_following(
    control: &mut CanonicalWriterControlClient,
    source: WalletCanonicalSourceIdentity,
    lease_duration: Duration,
) -> Result<ResumedFollowingAdmission, ProjectorError> {
    let initial_status = control.writer_status().await?;
    match classify_resumed_following_retention(
        source,
        initial_status.oldest_retained_event_sequence,
    )? {
        ResumedFollowingRetentionPlan::BootstrapFirstRetainedEvent {
            first_retained_event_sequence,
        } => Ok(ResumedFollowingAdmission::BootstrapFirstRetainedEvent {
            first_retained_event_sequence,
        }),
        ResumedFollowingRetentionPlan::AnchorPersistedCursor => {
            let acquisition_error =
                match acquire_following_retention_lease(control, source, lease_duration).await {
                    Ok(lease) => return Ok(ResumedFollowingAdmission::Anchored(lease)),
                    Err(error) => error,
                };
            let refreshed_status = control.writer_status().await?;
            match classify_resumed_following_retention(
                source,
                refreshed_status.oldest_retained_event_sequence,
            )? {
                ResumedFollowingRetentionPlan::BootstrapFirstRetainedEvent {
                    first_retained_event_sequence,
                } => Ok(ResumedFollowingAdmission::BootstrapFirstRetainedEvent {
                    first_retained_event_sequence,
                }),
                ResumedFollowingRetentionPlan::AnchorPersistedCursor => Err(acquisition_error),
            }
        }
    }
}

/// Moves pruning protection forward without leaving the persisted cursor unprotected.
///
/// If deletion of the old lease fails, the new lease is still retained and the
/// old bounded lease simply expires naturally.
async fn advance_retention_lease_anchor(
    control: &mut CanonicalWriterControlClient,
    previous: CanonicalRetentionLease,
    source: WalletCanonicalSourceIdentity,
    lease_duration: Duration,
) -> Result<CanonicalRetentionLease, ProjectorError> {
    let successor = acquire_following_retention_lease(control, source, lease_duration).await?;
    if let Err(error) = control.release(&previous).await {
        tracing::error!(
            target: "zinder::projector",
            %error,
            "successor canonical retention lease acquired but predecessor release failed; retaining both until the predecessor expires"
        );
    }
    Ok(successor)
}

async fn renew_following_retention_lease_if_due(
    control: &mut CanonicalWriterControlClient,
    lease: &mut CanonicalRetentionLease,
    source: WalletCanonicalSourceIdentity,
    lease_duration: Duration,
) -> Result<bool, ProjectorError> {
    require_retention_lease_anchor(lease, source)?;
    let now = UnixTimestampMillis::now();
    let Some(renewed_expiry) =
        following_retention_lease_renewal_expiry(now, lease.expires_at(), lease_duration)
    else {
        return Ok(false);
    };
    *lease = control.renew(lease.renewed(renewed_expiry)).await?;
    Ok(true)
}

/// Returns the configured full-duration extension when a following lease is
/// inside its renewal headroom. The caller performs the authenticated renewal.
fn following_retention_lease_renewal_expiry(
    now: UnixTimestampMillis,
    current_expiry: UnixTimestampMillis,
    lease_duration: Duration,
) -> Option<UnixTimestampMillis> {
    if lease_expiry(now, FOLLOW_RETENTION_LEASE_RENEWAL_HEADROOM) < current_expiry {
        return None;
    }
    let renewed_expiry = lease_expiry(now, lease_duration);
    is_strict_lease_extension(renewed_expiry, current_expiry).then_some(renewed_expiry)
}

/// Returns the full-duration extension required before a wallet mutation.
///
/// Unlike idle polling, this does not wait for renewal headroom: a transition
/// must begin with a complete lease window.
fn following_retention_lease_transition_expiry(
    now: UnixTimestampMillis,
    current_expiry: UnixTimestampMillis,
    lease_duration: Duration,
) -> Option<UnixTimestampMillis> {
    let renewed_expiry = lease_expiry(now, lease_duration);
    is_strict_lease_extension(renewed_expiry, current_expiry).then_some(renewed_expiry)
}

/// Renews the live cursor immediately before a wallet transition when it does
/// not already have at least one full configured lease duration remaining.
async fn renew_following_retention_lease_for_transition(
    control: &mut CanonicalWriterControlClient,
    lease: &mut CanonicalRetentionLease,
    source: WalletCanonicalSourceIdentity,
    lease_duration: Duration,
) -> Result<(), ProjectorError> {
    require_retention_lease_anchor(lease, source)?;
    let now = UnixTimestampMillis::now();
    let Some(renewed_expiry) =
        following_retention_lease_transition_expiry(now, lease.expires_at(), lease_duration)
    else {
        return Ok(());
    };
    *lease = control.renew(lease.renewed(renewed_expiry)).await?;
    Ok(())
}

fn require_retention_lease_anchor(
    lease: &CanonicalRetentionLease,
    source: WalletCanonicalSourceIdentity,
) -> Result<(), ProjectorError> {
    let position = source.source_position();
    if lease.anchor_chain_epoch_id() != position.chain_epoch_id.value()
        || lease.anchor_event_cursor() != position.event_cursor.as_bytes().as_slice()
    {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "active canonical retention lease is not anchored at the expected following cursor",
        });
    }
    Ok(())
}

fn fresh_retention_lease_id() -> Result<[u8; 16], getrandom::Error> {
    let mut lease_id = [0; 16];
    getrandom::fill(&mut lease_id)?;
    Ok(lease_id)
}

async fn best_effort_release_canonical_lease(
    config: &config::ProjectorConfig,
    lease: &CanonicalRetentionLease,
) {
    match CanonicalWriterControlClient::connect(
        &config.ingest_control_addr,
        config.ingest_control_bearer_token.as_ref(),
        config.projector_control.bearer_token.as_ref(),
    )
    .await
    {
        Ok(mut control) => {
            if let Err(error) = control.release(lease).await {
                tracing::error!(
                    target: "zinder::projector",
                    %error,
                    "best-effort canonical retention lease release failed after projector task failure"
                );
            }
        }
        Err(error) => tracing::error!(
            target: "zinder::projector",
            %error,
            "could not reconnect for best-effort canonical retention lease release after projector task failure"
        ),
    }
}

/// Confirms that a promoted wallet still represents its fixed secondary fence.
///
/// This is distinct from the current writer fence: the continuous follower
/// handles any retained events that arrived during construction.
fn require_built_wallet_source(
    expected: WalletCanonicalSourceIdentity,
    observed: WalletCanonicalSourceIdentity,
) -> Result<(), ProjectorError> {
    if expected != observed {
        return Err(ProjectorError::WalletConstructionFenceMismatch);
    }
    Ok(())
}

/// Admits a fixed-tip wallet promotion into continuous following.
///
/// The canonical writer need not still be at the fixed build fence: a build
/// may take hours while ingest continues. The just-renewed lease proves that
/// the exact initial event remains retained, and the writer must be on the
/// configured network at that event or a later event so the follower has a
/// contiguous authenticated route forward.
fn require_pre_promotion_follower_admission(
    status: zinder_proto::v1::ingest::CanonicalWriterStatusResponse,
    lease: &CanonicalRetentionLease,
    expected: WalletCanonicalSourceIdentity,
    network: zinder_core::Network,
) -> Result<(), zinder_wallet_rocksdb::RocksDbWalletError> {
    let expected_position = expected.source_position();
    let expected_cursor = expected_position.event_cursor.as_bytes();
    let Some(fence) = status.fence else {
        return Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled);
    };
    if lease.anchor_chain_epoch_id() != expected_position.chain_epoch_id.value()
        || lease.anchor_event_cursor() != expected_cursor.as_slice()
        || status.network_name != encode_zinder_native_chain_name(network)
        || fence.event_sequence < expected_position.event_sequence
        || status.oldest_retained_event_sequence > expected_position.event_sequence
    {
        return Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriterFenceRelation {
    Matches,
    SecondaryBehind,
    Diverged {
        secondary_event_sequence: u64,
        writer_event_sequence: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FenceConvergence {
    Converged,
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
struct FenceConvergencePolicy {
    timeout: Duration,
    syncing_after: Duration,
    initial_delay: Duration,
    max_delay: Duration,
}

impl FenceConvergencePolicy {
    const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            syncing_after: CANONICAL_FENCE_CONVERGENCE_SYNCING_AFTER,
            initial_delay: CANONICAL_FENCE_CONVERGENCE_INITIAL_DELAY,
            max_delay: CANONICAL_FENCE_CONVERGENCE_MAX_DELAY,
        }
    }
}

trait WriterFenceProbe {
    fn catch_up(&mut self) -> Result<WalletCanonicalSourceIdentity, ProjectorError>;

    async fn writer_status(
        &mut self,
    ) -> Result<zinder_proto::v1::ingest::CanonicalWriterStatusResponse, ProjectorError>;
}

struct LiveWriterFenceProbe<'a> {
    canonical: &'a mut RocksDbCanonicalSecondary,
    control: &'a mut CanonicalWriterControlClient,
}

impl WriterFenceProbe for LiveWriterFenceProbe<'_> {
    fn catch_up(&mut self) -> Result<WalletCanonicalSourceIdentity, ProjectorError> {
        self.canonical.try_catch_up()?;
        Ok(canonical_source_identity(&self.canonical.ready_evidence()))
    }

    async fn writer_status(
        &mut self,
    ) -> Result<zinder_proto::v1::ingest::CanonicalWriterStatusResponse, ProjectorError> {
        Ok(self.control.writer_status().await?)
    }
}

/// Returns `None` when `cancel` fires before the fence converges.
async fn converge_on_writer_fence(
    canonical: &mut RocksDbCanonicalSecondary,
    control: &mut CanonicalWriterControlClient,
    readiness: &Readiness,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<Option<CanonicalStoreReadyEvidence>, ProjectorError> {
    let network = canonical.network();
    let outcome = converge_probe_on_writer_fence(
        &mut LiveWriterFenceProbe { canonical, control },
        network,
        readiness,
        cancel,
        FenceConvergencePolicy::with_timeout(timeout),
    )
    .await?;
    Ok(match outcome {
        FenceConvergence::Converged => Some(canonical.ready_evidence()),
        FenceConvergence::Cancelled => None,
    })
}

/// Catches the secondary up until it authenticates the writer's exact fence.
///
/// The writer keeps committing while the projector converges, so a secondary
/// behind the writer's event sequence is ordinary lag and is retried with
/// capped exponential backoff until the policy deadline. Lag that outlasts
/// `syncing_after` is reported as syncing; shorter races leave readiness
/// untouched. Equal sequences must agree on every fence field, and a secondary
/// ahead of the writer cannot be caught up; both are divergence and fail
/// immediately.
async fn converge_probe_on_writer_fence(
    probe: &mut impl WriterFenceProbe,
    network: zinder_core::Network,
    readiness: &Readiness,
    cancel: &CancellationToken,
    policy: FenceConvergencePolicy,
) -> Result<FenceConvergence, ProjectorError> {
    let started = tokio::time::Instant::now();
    let deadline = started + policy.timeout;
    let mut delay = policy.initial_delay;
    loop {
        let source = probe.catch_up()?;
        let status = probe.writer_status().await?;
        let writer_tip_height = status.fence.as_ref().map(|fence| fence.visible_tip_height);
        match classify_writer_fence(&status, source, network) {
            WriterFenceRelation::Matches => return Ok(FenceConvergence::Converged),
            WriterFenceRelation::Diverged {
                secondary_event_sequence,
                writer_event_sequence,
            } => {
                return Err(ProjectorError::CanonicalFenceDiverged {
                    secondary_event_sequence,
                    writer_event_sequence,
                });
            }
            WriterFenceRelation::SecondaryBehind => {}
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(ProjectorError::CanonicalFenceDidNotConverge);
        }
        if now.duration_since(started) >= policy.syncing_after {
            let secondary_height = source.source_position().tip.height.value();
            readiness.set(ReadinessState::syncing(
                writer_tip_height
                    .and_then(|writer_height| writer_height.checked_sub(secondary_height))
                    .map(u64::from),
                Some(secondary_height),
                writer_tip_height,
            ));
        }
        tokio::select! {
            () = cancel.cancelled() => return Ok(FenceConvergence::Cancelled),
            () = tokio::time::sleep(delay.min(deadline.saturating_duration_since(now))) => {}
        }
        delay = delay.saturating_mul(2).min(policy.max_delay);
    }
}

fn classify_writer_fence(
    status: &zinder_proto::v1::ingest::CanonicalWriterStatusResponse,
    expected: WalletCanonicalSourceIdentity,
    network: zinder_core::Network,
) -> WriterFenceRelation {
    let source_position = expected.source_position();
    let secondary_event_sequence = source_position.event_sequence;
    let Some(fence) = status.fence.as_ref() else {
        return WriterFenceRelation::Diverged {
            secondary_event_sequence,
            writer_event_sequence: 0,
        };
    };
    let writer_event_sequence = fence.event_sequence;
    let diverged = WriterFenceRelation::Diverged {
        secondary_event_sequence,
        writer_event_sequence,
    };
    if status.network_name != encode_zinder_native_chain_name(network) {
        return diverged;
    }
    match secondary_event_sequence.cmp(&writer_event_sequence) {
        std::cmp::Ordering::Less => WriterFenceRelation::SecondaryBehind,
        std::cmp::Ordering::Greater => diverged,
        std::cmp::Ordering::Equal => {
            if fence.chain_epoch_id == source_position.chain_epoch_id.value()
                && fence.visible_tip_height == source_position.tip.height.value()
                && fence.visible_tip_hash == source_position.tip.hash.as_bytes()
                && fence.visible_block_count == expected.source_sequence_digest().block_count()
                && fence.canonical_sequence_digest == expected.source_sequence_digest().as_bytes()
            {
                WriterFenceRelation::Matches
            } else {
                diverged
            }
        }
    }
}

fn canonical_source_identity(ready: &CanonicalStoreReadyEvidence) -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            ready.visible_epoch,
            ready.visible_tip,
            ready.visible_event_sequence,
        ),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready.sequence_digest_version,
            ready.visible_block_count,
            ready.visible_sequence_digest,
        ),
        ready.sequence_checkpoint.through(),
    )
}

fn wallet_source_identity_from_fence(
    fence: CanonicalEventFence,
    settled_tip: BlockId,
) -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            fence.chain_epoch_id(),
            fence.visible_tip(),
            fence.chain_event_sequence(),
        ),
        fence.sequence_digest(),
        settled_tip,
    )
}

fn settled_tip_for_event(
    canonical: &RocksDbCanonicalSecondary,
    event: CanonicalRetainedEvent,
) -> Result<BlockId, ProjectorError> {
    let epoch = canonical.chain_epoch_at(event.resulting_epoch_id())?;
    Ok(BlockId::new(
        epoch.settled_tip_height,
        epoch.settled_tip_hash,
    ))
}

/// One authenticated `EventPage` covers the local secondary fence.
///
/// Otherwise, the caller must catch the secondary up after the writer advanced.
enum RetainedEventPageValidation {
    /// The writer moved between secondary convergence and the `EventPage` request.
    RetryAfterWriterAdvance,
    /// Local retained events verified against the authenticated remote page.
    Events(Vec<CanonicalRetainedEvent>),
}

/// Reads one writer-bounded `EventPage` from the persisted wallet cursor.
///
/// The projector never accumulates a multi-page catch-up in memory. After a
/// durable transition it starts a new page at the newly persisted cursor,
/// which naturally lets arbitrary retained lag drain through bounded commits.
async fn fetch_retained_event_page_to_target(
    control: &mut CanonicalWriterControlClient,
    canonical: &RocksDbCanonicalSecondary,
    wallet_source: WalletCanonicalSourceIdentity,
    target_fence: CanonicalEventFence,
) -> Result<RetainedEventPageValidation, ProjectorError> {
    let page_limit = NonZeroU32::new(CANONICAL_CONTROL_EVENT_PAGE_EVENTS).ok_or(
        ProjectorError::CanonicalEventPageInvalid {
            reason: "configured canonical-control page limit must be nonzero",
        },
    )?;
    let cursor = wallet_source.source_position().event_cursor.as_bytes();
    let page = control.event_page(&cursor, page_limit.get()).await?;
    let local_events = canonical
        .canonical_event_history(CanonicalEventHistoryRequest::new(Some(&cursor), page_limit))?;
    validate_one_retained_event_page(&page, local_events, wallet_source, target_fence)
}

/// Validates one remote `EventPage` against retained local-secondary rows.
///
/// This deliberately does not require the page to end at the current writer
/// fence because a bounded reconciliation may span several writer pages.
fn validate_one_retained_event_page(
    page: &zinder_proto::v1::ingest::CanonicalEventPageResponse,
    local_events: Vec<CanonicalRetainedEvent>,
    wallet_source: WalletCanonicalSourceIdentity,
    target_fence: CanonicalEventFence,
) -> Result<RetainedEventPageValidation, ProjectorError> {
    let wallet_position = wallet_source.source_position();
    let next_wallet_event_sequence = wallet_position.event_sequence.checked_add(1).ok_or(
        ProjectorError::CanonicalEventPageInvalid {
            reason: "wallet event sequence cannot advance",
        },
    )?;
    if page.oldest_retained_event_sequence == 0 {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer omitted a nonzero retained-event floor",
        });
    }
    if page.oldest_retained_event_sequence > next_wallet_event_sequence {
        return Err(ProjectorError::CanonicalEventCursorExpired {
            wallet_event_sequence: wallet_position.event_sequence,
            oldest_retained_event_sequence: page.oldest_retained_event_sequence,
        });
    }
    let Some(page_writer_fence) = page.writer_fence.as_ref() else {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer EventPage omitted its bounding fence",
        });
    };
    if !writer_fence_matches(page_writer_fence, target_fence) {
        return Ok(RetainedEventPageValidation::RetryAfterWriterAdvance);
    }
    if page.events.len() > CANONICAL_CONTROL_EVENT_PAGE_EVENTS_USIZE {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer returned more events than the canonical-control page ceiling",
        });
    }
    if page.events.len() != local_events.len() {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer and secondary retained-event page lengths differ",
        });
    }
    if local_events.is_empty() {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer fence advanced but retained-event page is empty",
        });
    }

    for (wire_event, local_event) in page.events.iter().zip(&local_events) {
        if !wire_event_matches_local(wire_event, *local_event) {
            return Err(ProjectorError::CanonicalEventPageInvalid {
                reason: "writer and secondary retained-event bytes differ",
            });
        }
    }
    Ok(RetainedEventPageValidation::Events(local_events))
}

/// The smallest durable unit that can advance a following wallet.
///
/// When the next event already ends on the current canonical branch it is
/// applied alone. A later reorg can overwrite one or more retained events;
/// only that overwritten run is collapsed, ending at the first event whose
/// resulting tip is again present on the current branch.
enum NextWalletTransition {
    ApplyOne(CanonicalRetainedEvent),
    ReconcileOverwritten {
        events: Vec<CanonicalRetainedEvent>,
        resulting_fence: CanonicalEventFence,
    },
}

impl NextWalletTransition {
    fn resulting_fence(&self) -> CanonicalEventFence {
        match self {
            Self::ApplyOne(event) => event.resulting_fence(),
            Self::ReconcileOverwritten {
                resulting_fence, ..
            } => *resulting_fence,
        }
    }
}

/// Selects the earliest current-canonical retained-event boundary.
///
/// A page without a return to the current branch cannot be safely collapsed:
/// accepting a larger in-memory sequence would turn the control-page limit
/// into an unbounded write batch. The writer's bounded reorg policy keeps a
/// valid overwritten run inside this page.
fn plan_next_wallet_transition(
    canonical: &RocksDbCanonicalSecondary,
    retained_events: &[CanonicalRetainedEvent],
) -> Result<NextWalletTransition, ProjectorError> {
    let Some(first_event) = retained_events.first().copied() else {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "retained-event page is empty while the wallet is behind",
        });
    };
    if retained_event_result_is_current_canonical(canonical, first_event)? {
        return Ok(NextWalletTransition::ApplyOne(first_event));
    }

    for (index, event) in retained_events.iter().copied().enumerate().skip(1) {
        if retained_event_result_is_current_canonical(canonical, event)? {
            return Ok(NextWalletTransition::ReconcileOverwritten {
                events: retained_events[..=index].to_vec(),
                resulting_fence: event.resulting_fence(),
            });
        }
    }

    Err(ProjectorError::CanonicalReconciliationEventPageTooLarge {
        maximum_events: CANONICAL_CONTROL_EVENT_PAGE_EVENTS,
    })
}

/// Returns whether an event's resulting tip is still a prefix of the current
/// canonical secondary. A matching block hash commits to the entire ancestor
/// chain, so no historical scan is needed.
fn retained_event_result_is_current_canonical(
    canonical: &RocksDbCanonicalSecondary,
    event: CanonicalRetainedEvent,
) -> Result<bool, ProjectorError> {
    let resulting_tip = event.resulting_fence().visible_tip();
    let ready = canonical.ready_evidence();
    if resulting_tip.height < ready.first_retained_block.height
        || resulting_tip.height > ready.visible_tip.height
    {
        return Ok(false);
    }
    Ok(canonical_block_at(canonical, resulting_tip.height)? == resulting_tip)
}

/// Compares a remote event envelope with the locally admitted event.
///
/// The remote control plane authenticates the page boundary; the secondary
/// authenticates every event against its durable event and epoch rows.
fn wire_event_matches_local(
    wire: &zinder_proto::v1::ingest::CanonicalRetainedEvent,
    local: CanonicalRetainedEvent,
) -> bool {
    let expected_kind = match local.kind() {
        zinder_store::CanonicalEventKind::Committed => {
            zinder_proto::v1::ingest::CanonicalRetainedEventKind::Committed as i32
        }
        zinder_store::CanonicalEventKind::Reorged => {
            zinder_proto::v1::ingest::CanonicalRetainedEventKind::Reorged as i32
        }
    };
    wire.cursor == local.cursor().as_bytes()
        && wire.resulting_epoch_id == local.resulting_epoch_id().value()
        && wire.previous_epoch_id
            == local
                .previous_epoch_id()
                .map(zinder_core::ChainEpochId::value)
        && wire.kind == expected_kind
        && wire_range_matches_local(wire.reverted_range.as_ref(), local.reverted_range())
        && wire_range_matches_local(wire.committed_range.as_ref(), Some(local.committed_range()))
        && wire
            .resulting_fence
            .as_ref()
            .is_some_and(|fence| writer_fence_matches(fence, local.resulting_fence()))
}

fn wire_range_matches_local(
    wire: Option<&zinder_proto::v1::ingest::CanonicalEventBlockRange>,
    local: Option<BlockHeightRange>,
) -> bool {
    match (wire, local) {
        (Some(wire), Some(local)) => {
            wire.start_height == local.start.value() && wire.end_height == local.end.value()
        }
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn writer_fence_matches(
    wire: &zinder_proto::v1::ingest::CanonicalWriterFence,
    local: CanonicalEventFence,
) -> bool {
    wire.chain_epoch_id == local.chain_epoch_id().value()
        && wire.event_sequence == local.chain_event_sequence()
        && wire.visible_tip_height == local.visible_tip().height.value()
        && wire.visible_tip_hash == local.visible_tip().hash.as_bytes()
        && wire.visible_block_count == local.sequence_digest().block_count()
        && wire.canonical_sequence_digest == local.sequence_digest().as_bytes()
}

/// Finds the current canonical block shared by the persisted wallet and the
/// current canonical branch without scanning either full history.
///
/// The wallet can establish old branch identities only from its durable undo
/// suffix, so failing to find an equal block within that bounded suffix is a
/// hard reconciliation failure rather than a request to replay arbitrary
/// historical state.
fn discover_common_ancestor(
    wallet: &RocksDbWalletFollowingStore,
    canonical: &RocksDbCanonicalSecondary,
    target_fence: CanonicalEventFence,
    supported_reorg_depth: u32,
) -> Result<BlockId, ProjectorError> {
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let wallet_tip = wallet_source.source_position().tip;
    let target_tip = target_fence.visible_tip();
    let upper_height = BlockHeight::new(wallet_tip.height.value().min(target_tip.height.value()));
    let canonical_floor = canonical.ready_evidence().first_retained_block.height;
    if upper_height < canonical_floor {
        return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
    }
    let minimum_height = BlockHeight::new(
        wallet_tip
            .height
            .value()
            .saturating_sub(supported_reorg_depth)
            .max(canonical_floor.value()),
    );
    if upper_height < minimum_height {
        return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
    }

    let mut height = upper_height.value();
    let mut expected_parent_hash = None;
    loop {
        let block = if height == wallet_tip.height.value() {
            wallet_tip
        } else {
            let Some(undo) = wallet.find_reorg_undo(BlockHeight::new(height))? else {
                return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
            };
            undo.block
        };
        if block.height.value() != height {
            return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
        }
        if let Some(expected_parent_hash) = expected_parent_hash
            && block.hash != expected_parent_hash
        {
            return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
        }
        let canonical_block = canonical_block_at(canonical, block.height)?;
        if canonical_block == block {
            return Ok(block);
        }
        let Some(undo) = wallet.find_reorg_undo(block.height)? else {
            return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
        };
        if undo.block != block {
            return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
        }
        expected_parent_hash = Some(undo.parent_hash);
        if height == minimum_height.value() {
            return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
        }
        height = height
            .checked_sub(1)
            .ok_or(ProjectorError::CanonicalCommonAncestorUnavailable)?;
    }
}

fn canonical_block_at(
    canonical: &RocksDbCanonicalSecondary,
    height: BlockHeight,
) -> Result<BlockId, ProjectorError> {
    let range = BlockHeightRange::inclusive(height, height);
    let mut replay = canonical.scan_canonical_replay_range(range)?;
    let Some(next_replay) = replay.next() else {
        return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
    };
    let replay_row = next_replay?;
    if replay.next().is_some() {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "single-height canonical replay scan returned multiple rows",
        });
    }
    let header = &replay_row.facts().block_header;
    Ok(BlockId::new(header.height, header.block_hash))
}

fn reconciliation_ranges(
    wallet_tip: BlockId,
    common_ancestor: BlockId,
    target_tip: BlockId,
) -> Result<(Option<BlockHeightRange>, BlockHeightRange), ProjectorError> {
    if common_ancestor.height > wallet_tip.height || common_ancestor.height > target_tip.height {
        return Err(ProjectorError::CanonicalCommonAncestorUnavailable);
    }
    let rollback_range = if common_ancestor.height < wallet_tip.height {
        let start = common_ancestor
            .height
            .next()
            .ok_or(ProjectorError::CanonicalCommonAncestorUnavailable)?;
        Some(BlockHeightRange::inclusive(start, wallet_tip.height))
    } else {
        None
    };
    let replay_range = if common_ancestor.height < target_tip.height {
        let start = common_ancestor
            .height
            .next()
            .ok_or(ProjectorError::CanonicalCommonAncestorUnavailable)?;
        BlockHeightRange::inclusive(start, target_tip.height)
    } else {
        BlockHeightRange::empty_at(target_tip.height)
    };
    Ok((rollback_range, replay_range))
}

fn require_bounded_reconciliation_replay(
    replay_range: BlockHeightRange,
) -> Result<(), ProjectorError> {
    let block_count = replay_range
        .end
        .value()
        .checked_sub(replay_range.start.value())
        .and_then(|distance| distance.checked_add(1))
        .unwrap_or(0);
    if block_count > zinder_store::MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS {
        return Err(ProjectorError::CanonicalReconciliationReplayTooLarge {
            requested_blocks: block_count,
            maximum_blocks: zinder_store::MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS,
        });
    }
    Ok(())
}

fn lease_expiry(now: UnixTimestampMillis, duration: Duration) -> UnixTimestampMillis {
    let duration_millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    UnixTimestampMillis::new(now.value().saturating_add(duration_millis))
}

fn is_strict_lease_extension(candidate: UnixTimestampMillis, current: UnixTimestampMillis) -> bool {
    candidate > current
}

fn display_digest(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn wallet_row_count(counts: zinder_wallet_projection::WalletProjectionFamilyRowCounts) -> u64 {
    counts
        .transparent_unspent_output_count
        .saturating_add(counts.transparent_unspent_output_by_address_count)
        .saturating_add(counts.transparent_spent_output_count)
        .saturating_add(counts.transparent_address_transaction_count)
        .saturating_add(counts.transparent_address_balance_count)
        .saturating_add(counts.reorg_undo_count)
}

fn emit_error(error: &ProjectorError) -> ExitCode {
    tracing::error!(target: "zinder::projector", %error, "projector failed");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, path::PathBuf, str::FromStr as _, time::Duration};

    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use zinder_runtime::{BearerToken, ResolvedProjectorControl};

    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
        CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockFactsSequenceDigestVersion, CanonicalBlockReplayFormatVersion,
        CanonicalTransactionFacts, ChainEpochId, ChainTipMetadata, CommitmentTreeCheckpoint,
        CommitmentTreeFrontier, CommitmentTreeFrontiers, CompactBlockArtifact,
        CompactChainMetadata, LockTime, PrivacyShape, SerializedBytesDigest, ShieldedProtocol,
        TransactionBlobArtifact, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, UnixTimestampMillis, UnsupportedSection, encode_canonical_block_replay,
    };

    use super::{
        CanonicalBlockFactsSequenceDigest, CanonicalRetentionLease, FenceConvergence,
        FenceConvergencePolicy, NextWalletTransition, ProjectorControlTasks, ProjectorError,
        ResumedFollowingRetentionPlan, WalletCanonicalSourceIdentity,
        WalletProjectionSourcePosition, WriterFenceProbe, WriterFenceRelation,
        classify_resumed_following_retention, classify_writer_fence,
        converge_probe_on_writer_fence, encode_zinder_native_chain_name,
        following_retention_lease_renewal_expiry, following_retention_lease_transition_expiry,
        lease_expiry, reconciliation_ranges, require_bounded_reconciliation_replay,
        require_built_wallet_source, require_pre_promotion_follower_admission,
        require_retention_lease_anchor, settled_tip_for_transition,
    };
    use zinder_proto::v1::ingest::{CanonicalWriterFence, CanonicalWriterStatusResponse};
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventHistoryRequest,
        CanonicalLiveAppend, CanonicalLiveReplacement, CanonicalReorgPolicy,
        CanonicalReplacementBlock, CanonicalStoreBuildPlan, CanonicalStoreWorkload,
        RocksDbCanonicalBuilder, RocksDbCanonicalSecondary, RocksDbResourceBudget,
    };

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end regression must construct a retained overwritten event, advance settlement beyond it, and compare both settlement sources"
    )]
    fn collapsed_transition_uses_target_event_settlement_not_current_checkpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let canonical_path = temporary.path().join("canonical");
        let reorg_policy = CanonicalReorgPolicy::new(3)?;
        let block_one = empty_canonical_block(
            1,
            zinder_core::Network::ZcashRegtest.genesis_hash().as_bytes(),
            [0x11; 32],
        );
        let block_one_id = BlockId::new(
            block_one.facts.block_header.height,
            block_one.facts.block_header.block_hash,
        );
        let build_plan =
            CanonicalStoreBuildPlan::complete(&activations, 0, block_one_id, reorg_policy)?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks([Ok::<_, std::convert::Infallible>(block_one)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            block_one_id,
            1,
            regtest_checkpoint_frontiers_at(BlockHeight::new(1)),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            block_one_id,
            UnixTimestampMillis::new(1),
        ))?;
        let canonical = validated.publish_baseline(publication)?;
        let baseline_fence = canonical.event_fence();

        let old_block_two = empty_canonical_block(2, [0x11; 32], [0x22; 32]);
        let (canonical, old_append_fence) = canonical.commit_live_append(
            CanonicalLiveAppend::new(
                baseline_fence,
                old_block_two,
                Vec::new(),
                block_one_id,
                UnixTimestampMillis::new(2),
            ),
            &activations,
        )?;
        let replacement_block_two = empty_canonical_block(2, [0x11; 32], [0x32; 32]);
        let replacement_block_two_id = BlockId::new(
            replacement_block_two.facts.block_header.height,
            replacement_block_two.facts.block_header.block_hash,
        );
        let (canonical, replacement_fence) = canonical.commit_live_replacement(
            CanonicalLiveReplacement::new(
                old_append_fence,
                vec![CanonicalReplacementBlock::new(
                    replacement_block_two,
                    Vec::new(),
                )],
                UnixTimestampMillis::new(3),
            ),
            &activations,
        )?;

        let mut current_fence = replacement_fence;
        let mut canonical = canonical;
        for (height, parent_hash, block_hash, settled_tip) in [
            (3, [0x32; 32], [0x33; 32], block_one_id),
            (4, [0x33; 32], [0x34; 32], block_one_id),
            (5, [0x34; 32], [0x35; 32], replacement_block_two_id),
        ] {
            let (next, fence) = canonical.commit_live_append(
                CanonicalLiveAppend::new(
                    current_fence,
                    empty_canonical_block(height, parent_hash, block_hash),
                    Vec::new(),
                    settled_tip,
                    UnixTimestampMillis::new(u64::from(height).saturating_add(3)),
                ),
                &activations,
            )?;
            canonical = next;
            current_fence = fence;
        }

        let secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            CanonicalStoreWorkload::Wallet,
            reorg_policy,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let events = secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
            None,
            NonZeroU32::new(16).ok_or("zero event page limit")?,
        ))?;
        let overwritten_append = events
            .iter()
            .copied()
            .find(|event| event.resulting_fence() == old_append_fence)
            .ok_or("overwritten append event missing")?;
        let replacement = events
            .iter()
            .copied()
            .find(|event| event.resulting_fence() == replacement_fence)
            .ok_or("replacement event missing")?;
        let transition = NextWalletTransition::ReconcileOverwritten {
            events: vec![overwritten_append, replacement],
            resulting_fence: replacement_fence,
        };

        assert_eq!(
            secondary.sequence_checkpoint().through(),
            replacement_block_two_id,
            "the current checkpoint must be ahead of the historical event settlement",
        );
        assert_eq!(
            settled_tip_for_transition(&secondary, &transition)?,
            block_one_id,
        );
        Ok(())
    }

    #[tokio::test]
    async fn enabled_projector_control_refuses_an_occupied_listener()
    -> Result<(), Box<dyn std::error::Error>> {
        let occupied = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                // Some sandboxed host runners deny every socket operation.
                // The unrestricted Linux CI profile exercises this boundary.
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let address = occupied.local_addr()?;
        let config = ResolvedProjectorControl {
            listen_addr: Some(address),
            bearer_token_path: Some(PathBuf::from("projector-control.token")),
            bearer_token: Some(BearerToken::from_str("projector-control-test-token")?),
            checkpoint_staging_root: PathBuf::from("checkpoint-staging"),
        };

        let error = ProjectorControlTasks::start(&config, &CancellationToken::new())
            .await
            .err()
            .ok_or("projector control unexpectedly shared an occupied listener")?;
        assert!(matches!(
            error,
            ProjectorError::ProjectorControlBind {
                address: observed,
                ..
            } if observed == address
        ));
        Ok(())
    }

    #[test]
    fn built_wallet_source_accepts_the_exact_fixed_construction_identity() {
        let identity = source_identity(1, 1);

        assert!(require_built_wallet_source(identity, identity).is_ok());
    }

    #[test]
    fn built_wallet_source_refuses_a_different_construction_identity() {
        assert!(matches!(
            require_built_wallet_source(source_identity(1, 1), source_identity(1, 2)),
            Err(ProjectorError::WalletConstructionFenceMismatch)
        ));
    }

    #[test]
    fn pre_promotion_admission_accepts_the_exact_writer_status() {
        let expected = source_identity(1, 1);

        assert!(
            require_pre_promotion_follower_admission(
                writer_status(expected),
                &retention_lease(expected),
                expected,
                test_network(),
            )
            .is_ok()
        );
    }

    #[test]
    fn pre_promotion_admission_accepts_a_writer_that_advanced_during_build() {
        let expected = source_identity(1, 1);
        let mut advanced = writer_status(source_identity(2, 2));
        if let Some(fence) = advanced.fence.as_mut() {
            fence.chain_epoch_id = 2;
            fence.visible_tip_height = 2;
            fence.visible_tip_hash = vec![0x44; 32];
        }

        assert!(
            require_pre_promotion_follower_admission(
                advanced,
                &retention_lease(expected),
                expected,
                test_network(),
            )
            .is_ok()
        );
    }

    #[test]
    fn pre_promotion_admission_refuses_a_writer_before_the_pinned_event() {
        let expected = source_identity(2, 2);
        assert!(matches!(
            require_pre_promotion_follower_admission(
                writer_status(source_identity(1, 1)),
                &retention_lease(expected),
                expected,
                test_network(),
            ),
            Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled)
        ));
    }

    #[test]
    fn pre_promotion_admission_refuses_a_lease_with_a_different_anchor() {
        let expected = source_identity(1, 1);

        assert!(matches!(
            require_pre_promotion_follower_admission(
                writer_status(expected),
                &retention_lease(source_identity(2, 2)),
                expected,
                test_network(),
            ),
            Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled)
        ));
    }

    #[test]
    fn exact_writer_matching_requires_the_visible_block_count() {
        let expected = source_identity(1, 1);
        let mut status = writer_status(expected);
        if let Some(fence) = status.fence.as_mut() {
            fence.visible_block_count = 2;
        }

        assert_eq!(
            classify_writer_fence(&status, expected, test_network()),
            WriterFenceRelation::Diverged {
                secondary_event_sequence: 1,
                writer_event_sequence: 1,
            }
        );
    }

    struct ScriptedProbe {
        secondary_sequence: u64,
        writer_sequence: u64,
        commits_remaining: Option<u32>,
        catch_ups: u32,
    }

    impl ScriptedProbe {
        fn new(commits_remaining: Option<u32>) -> Self {
            Self {
                secondary_sequence: 1,
                writer_sequence: 1,
                commits_remaining,
                catch_ups: 0,
            }
        }
    }

    impl WriterFenceProbe for ScriptedProbe {
        fn catch_up(&mut self) -> Result<WalletCanonicalSourceIdentity, ProjectorError> {
            self.catch_ups += 1;
            self.secondary_sequence = self.writer_sequence;
            Ok(source_identity(self.secondary_sequence, 7))
        }

        async fn writer_status(&mut self) -> Result<CanonicalWriterStatusResponse, ProjectorError> {
            match &mut self.commits_remaining {
                Some(0) => {}
                Some(remaining) => {
                    *remaining -= 1;
                    self.writer_sequence += 1;
                }
                None => self.writer_sequence += 1,
            }
            Ok(writer_status(source_identity(self.writer_sequence, 7)))
        }
    }

    fn fast_policy(timeout: Duration) -> FenceConvergencePolicy {
        FenceConvergencePolicy {
            timeout,
            syncing_after: Duration::from_hours(1),
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        }
    }

    #[tokio::test]
    async fn fence_convergence_survives_writer_commits_between_snapshot_and_status() {
        let mut probe = ScriptedProbe::new(Some(6));
        let readiness =
            zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(1)));

        let outcome = converge_probe_on_writer_fence(
            &mut probe,
            test_network(),
            &readiness,
            &CancellationToken::new(),
            fast_policy(Duration::from_secs(5)),
        )
        .await;

        assert!(matches!(outcome, Ok(FenceConvergence::Converged)));
        assert_eq!(probe.catch_ups, 7);
        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn fence_convergence_reports_syncing_once_lag_outlasts_the_grace_period() {
        let mut probe = ScriptedProbe::new(Some(6));
        let readiness =
            zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(1)));
        let policy = FenceConvergencePolicy {
            syncing_after: Duration::ZERO,
            ..fast_policy(Duration::from_secs(5))
        };

        let outcome = converge_probe_on_writer_fence(
            &mut probe,
            test_network(),
            &readiness,
            &CancellationToken::new(),
            policy,
        )
        .await;

        assert!(matches!(outcome, Ok(FenceConvergence::Converged)));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            zinder_runtime::ReadinessCause::Syncing {
                lag_blocks: Some(0)
            }
        ));
        assert_eq!(report.current_height, Some(1));
        assert_eq!(report.target_height, Some(1));
    }

    #[tokio::test]
    async fn fence_convergence_returns_promptly_when_cancelled_during_lag() {
        let mut probe = ScriptedProbe::new(None);
        let readiness = zinder_runtime::Readiness::default();
        let cancel = CancellationToken::new();
        let policy = FenceConvergencePolicy {
            initial_delay: Duration::from_mins(10),
            max_delay: Duration::from_mins(10),
            ..fast_policy(Duration::from_hours(1))
        };
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            converge_probe_on_writer_fence(&mut probe, test_network(), &readiness, &cancel, policy),
        )
        .await;

        assert!(matches!(outcome, Ok(Ok(FenceConvergence::Cancelled))));
    }

    #[tokio::test]
    async fn fence_convergence_fails_after_the_deadline_under_sustained_lag() {
        let mut probe = ScriptedProbe::new(None);
        let readiness = zinder_runtime::Readiness::default();

        let outcome = converge_probe_on_writer_fence(
            &mut probe,
            test_network(),
            &readiness,
            &CancellationToken::new(),
            fast_policy(Duration::from_millis(30)),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(ProjectorError::CanonicalFenceDidNotConverge)
        ));
        assert!(probe.catch_ups > 1);
    }

    #[tokio::test]
    async fn fence_convergence_fails_immediately_on_same_sequence_mismatch() {
        struct SameSequenceMismatch;
        impl WriterFenceProbe for SameSequenceMismatch {
            fn catch_up(&mut self) -> Result<WalletCanonicalSourceIdentity, ProjectorError> {
                Ok(source_identity(5, 1))
            }
            async fn writer_status(
                &mut self,
            ) -> Result<CanonicalWriterStatusResponse, ProjectorError> {
                Ok(writer_status(source_identity(5, 2)))
            }
        }

        let readiness = zinder_runtime::Readiness::default();
        let outcome = converge_probe_on_writer_fence(
            &mut SameSequenceMismatch,
            test_network(),
            &readiness,
            &CancellationToken::new(),
            fast_policy(Duration::from_mins(1)),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(ProjectorError::CanonicalFenceDiverged {
                secondary_event_sequence: 5,
                writer_event_sequence: 5,
            })
        ));
    }

    #[test]
    fn writer_fence_classification_separates_lag_from_divergence() {
        let secondary = source_identity(5, 1);

        assert_eq!(
            super::classify_writer_fence(
                &writer_status(source_identity(6, 2)),
                secondary,
                test_network()
            ),
            WriterFenceRelation::SecondaryBehind
        );
        assert_eq!(
            super::classify_writer_fence(
                &writer_status(source_identity(4, 1)),
                secondary,
                test_network()
            ),
            WriterFenceRelation::Diverged {
                secondary_event_sequence: 5,
                writer_event_sequence: 4,
            }
        );
        assert_eq!(
            super::classify_writer_fence(
                &writer_status(source_identity(5, 1)),
                secondary,
                test_network()
            ),
            WriterFenceRelation::Matches
        );
    }

    #[test]
    fn resumed_following_keeps_a_direct_anchor_when_the_wallet_cursor_is_retained()
    -> Result<(), ProjectorError> {
        let source = source_identity(4, 4);

        assert_eq!(
            classify_resumed_following_retention(source, 4)?,
            ResumedFollowingRetentionPlan::AnchorPersistedCursor
        );
        Ok(())
    }

    #[test]
    fn resumed_following_bootstraps_only_the_immediate_retained_successor()
    -> Result<(), ProjectorError> {
        let source = source_identity(4, 4);

        assert_eq!(
            classify_resumed_following_retention(source, 5)?,
            ResumedFollowingRetentionPlan::BootstrapFirstRetainedEvent {
                first_retained_event_sequence: 5,
            }
        );
        Ok(())
    }

    #[test]
    fn resumed_following_requires_a_side_by_side_rebuild_after_a_larger_gap() {
        assert!(matches!(
            classify_resumed_following_retention(source_identity(4, 4), 6),
            Err(ProjectorError::WalletRebuildRequired {
                wallet_event_sequence: 4,
                oldest_retained_event_sequence: 6,
            })
        ));
    }

    #[test]
    fn bootstrap_reauthentication_refuses_a_changed_successor_before_transition() {
        let lease = retention_lease(source_identity(5, 5));

        assert!(matches!(
            require_retention_lease_anchor(&lease, source_identity(6, 6)),
            Err(ProjectorError::CanonicalEventPageInvalid {
                reason: "active canonical retention lease is not anchored at the expected following cursor",
            })
        ));
    }

    #[test]
    fn reconciliation_ranges_replay_only_the_append_suffix() -> Result<(), ProjectorError> {
        let common_ancestor = block_id(100, 0x10);
        let target_tip = block_id(102, 0x12);

        let (rollback, replay) =
            reconciliation_ranges(common_ancestor, common_ancestor, target_tip)?;

        assert_eq!(rollback, None);
        assert_eq!(
            replay,
            BlockHeightRange::inclusive(BlockHeight::new(101), BlockHeight::new(102))
        );
        Ok(())
    }

    #[test]
    fn reconciliation_ranges_roll_back_only_the_divergent_wallet_suffix()
    -> Result<(), ProjectorError> {
        let common_ancestor = block_id(100, 0x10);
        let wallet_tip = block_id(105, 0x15);
        let target_tip = block_id(103, 0x23);

        let (rollback, replay) = reconciliation_ranges(wallet_tip, common_ancestor, target_tip)?;

        assert_eq!(
            rollback,
            Some(BlockHeightRange::inclusive(
                BlockHeight::new(101),
                BlockHeight::new(105),
            ))
        );
        assert_eq!(
            replay,
            BlockHeightRange::inclusive(BlockHeight::new(101), BlockHeight::new(103))
        );
        Ok(())
    }

    #[test]
    fn reconciliation_replay_cap_accepts_the_exact_limit_and_refuses_one_more()
    -> Result<(), ProjectorError> {
        let limit = zinder_store::MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS;
        require_bounded_reconciliation_replay(BlockHeightRange::inclusive(
            BlockHeight::new(1),
            BlockHeight::new(limit),
        ))?;

        assert!(matches!(
            require_bounded_reconciliation_replay(BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(limit.saturating_add(1)),
            )),
            Err(ProjectorError::CanonicalReconciliationReplayTooLarge {
                requested_blocks,
                maximum_blocks,
            }) if requested_blocks == limit.saturating_add(1) && maximum_blocks == limit
        ));
        Ok(())
    }

    #[test]
    fn lease_renewal_requires_a_strict_expiry_extension() {
        assert!(!super::is_strict_lease_extension(
            zinder_core::UnixTimestampMillis::new(10),
            zinder_core::UnixTimestampMillis::new(10),
        ));
        assert!(super::is_strict_lease_extension(
            zinder_core::UnixTimestampMillis::new(11),
            zinder_core::UnixTimestampMillis::new(10),
        ));
    }

    #[test]
    fn following_lease_renewal_uses_the_configured_full_duration() {
        let now = UnixTimestampMillis::new(1_000_000);
        let configured_duration = Duration::from_hours(4);
        let current_expiry = lease_expiry(now, super::FOLLOW_RETENTION_LEASE_RENEWAL_HEADROOM);

        assert_eq!(
            following_retention_lease_renewal_expiry(now, current_expiry, configured_duration,),
            Some(lease_expiry(now, configured_duration))
        );
    }

    #[test]
    fn following_transition_renews_to_the_full_configured_window_before_mutation() {
        let now = UnixTimestampMillis::new(1_000_000);
        let configured_duration = Duration::from_hours(4);
        let current_expiry = lease_expiry(now, Duration::from_mins(2));

        assert_eq!(
            following_retention_lease_transition_expiry(now, current_expiry, configured_duration,),
            Some(lease_expiry(now, configured_duration))
        );
    }

    fn empty_canonical_block(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
    ) -> CanonicalBuildBlock {
        let height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes(block_hash);
        let parent_hash = BlockHash::from_bytes(parent_hash);
        let (transaction, transaction_blob) = empty_coinbase(height, block_hash);
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
            transactions: vec![transaction],
        };
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block: CompactBlockArtifact::empty(
                BlockId::new(height, block_hash),
                parent_hash,
                height.value(),
                CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
            tip_metadata: ChainTipMetadata::empty(),
            tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
                BlockId::new(height, block_hash),
                height.value(),
                regtest_checkpoint_frontiers_at(height),
            )),
            block_final_note_commitment_roots: None,
            transaction_blobs: vec![transaction_blob],
            block_blob: None,
        }
    }

    fn empty_coinbase(
        height: BlockHeight,
        block_hash: BlockHash,
    ) -> (CanonicalTransactionFacts, TransactionBlobArtifact) {
        let transaction_tag = block_hash.as_bytes()[0];
        let raw_transaction_bytes = vec![transaction_tag];
        let transaction_id = TransactionId::from_bytes([transaction_tag; 32]);
        let facts = CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::Unsupported {
                    effective_version: 0,
                    version_group_id: None,
                },
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 1,
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: true,
                unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &raw_transaction_bytes,
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        };
        let blob = TransactionBlobArtifact::new(
            TransactionLocation::new(transaction_id, height, block_hash, 0),
            raw_transaction_bytes,
        );
        (facts, blob)
    }

    fn regtest_checkpoint_frontiers_at(height: BlockHeight) -> CommitmentTreeFrontiers {
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            (height.value() >= 2).then(|| CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
            (height.value() >= 603)
                .then(|| CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
        )
    }

    fn writer_status(identity: WalletCanonicalSourceIdentity) -> CanonicalWriterStatusResponse {
        let position = identity.source_position();
        CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(test_network()).to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: position.chain_epoch_id.value(),
                event_sequence: position.event_sequence,
                visible_tip_height: position.tip.height.value(),
                visible_tip_hash: position.tip.hash.as_bytes().to_vec(),
                visible_block_count: identity.source_sequence_digest().block_count(),
                canonical_sequence_digest: identity.source_sequence_digest().as_bytes().to_vec(),
            }),
            oldest_retained_event_sequence: 1,
        }
    }

    fn retention_lease(source: WalletCanonicalSourceIdentity) -> CanonicalRetentionLease {
        let position = source.source_position();
        CanonicalRetentionLease::new(
            [0x55; 16],
            position.chain_epoch_id.value(),
            position.event_cursor.as_bytes().to_vec(),
            zinder_core::UnixTimestampMillis::new(1_000),
        )
    }

    fn test_network() -> zinder_core::Network {
        zinder_core::Network::ZcashRegtest
    }

    fn source_identity(
        event_sequence: u64,
        sequence_digest_byte: u8,
    ) -> WalletCanonicalSourceIdentity {
        WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(
                ChainEpochId::new(1),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x33; 32])),
                event_sequence,
            ),
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                1,
                [sequence_digest_byte; 32],
            ),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x33; 32])),
        )
    }

    fn block_id(height: u32, hash_byte: u8) -> BlockId {
        BlockId::new(
            BlockHeight::new(height),
            BlockHash::from_bytes([hash_byte; 32]),
        )
    }
}
