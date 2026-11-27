//! Reusable runtime entrypoints and CLI parsing for `zinder-projector`.

mod canonical_writer_control;
pub mod config;
mod following_readiness;
mod parked_following;
mod projector_control;
pub mod recovery;

use std::{
    ffi::OsString,
    net::SocketAddr,
    num::NonZeroU32,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use clap::{Parser, Subcommand};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFactsSequenceDigest, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_proto::v1::ingest::{CreateStateBundleCaptureRequest, CreateStateBundleCaptureResponse};
use zinder_runtime::{
    CanonicalFenceConvergence, CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy,
    CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation, OpsHealthcheckArgs, OpsServer,
    OpsServerError, Readiness, ReadinessState, StartupPhase, StartupPhaseGuard,
    TracingSubscriberError, cancel_on_terminating_signal, compare_canonical_writer_fences,
    converge_on_canonical_writer_fence, expected_writer_fence, host_cpu_meets_compiled_baseline,
    host_cpu_meets_pasta_backend_baseline, install_metrics_recorder, install_tracing_subscriber,
    spawn_ops_endpoint, writer_attests_network_upgrade_schedule,
};
use zinder_store::{
    CanonicalEventCursor, CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalReorgPolicy,
    CanonicalRetainedEvent, CanonicalStoreReadyEvidence, RocksDbCanonicalSecondary,
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

use crate::PROJECTOR_SERVICE_NAME;
use crate::state_bundle::{
    CanonicalCheckpointAdmissionEvidence, StateBundleError, complete_state_bundle_capture,
    prepare_state_bundle_capture,
};

pub use canonical_writer_control::CanonicalWriterControlError;
use canonical_writer_control::{CanonicalRetentionLease, CanonicalWriterControlClient};
use following_readiness::FollowingReadiness;
use parked_following::{
    ParkedFollowing, ParkedLease, RefusedTransition, park_on_refused_transition,
};
use projector_control::{
    ProjectorControlCommand, ProjectorControlGrpcAdapter, projector_control_channel,
};

pub use config::{
    ProjectorBuildConfig, ProjectorConfig, ProjectorConfigOverrides, ProjectorError,
    ProjectorFollowConfig, load_projector_config, load_projector_config_with_env,
    projector_config_toml,
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

/// Command-line interface for the `zinder-projector` runtime.
#[derive(Debug, Parser)]
#[command(name = "zinder-projector")]
#[command(about = "Zinder wallet projection construction service")]
#[command(version)]
pub struct Cli {
    /// TOML configuration file loaded before environment and CLI overrides.
    #[arg(long = "config", global = true)]
    pub config_path: Option<PathBuf>,
    /// Print resolved configuration without opening stores or binding sockets.
    #[arg(long = "print-config", global = true)]
    pub print_config: bool,
    /// Zinder-native network name.
    #[arg(long)]
    pub network: Option<String>,
    /// Canonical primary path consumed only through a `RocksDB` secondary.
    #[arg(long = "canonical-path")]
    pub canonical_path: Option<PathBuf>,
    /// Process-owned canonical secondary metadata path.
    #[arg(long = "canonical-secondary-path")]
    pub canonical_secondary_path: Option<PathBuf>,
    /// Wallet projection primary path owned by this process.
    #[arg(long = "wallet-path")]
    pub wallet_path: Option<PathBuf>,
    /// Canonical and wallet replacement window.
    #[arg(long = "reorg-window-blocks")]
    pub reorg_window_blocks: Option<u32>,
    /// Stable 16-byte projection builder identity encoded as 32 hex characters.
    #[arg(long = "build-owner-hex")]
    pub build_owner_hex: Option<String>,
    /// Explicit duration of each durable projection-build lease (at least four hours).
    #[arg(long = "lease-duration-seconds")]
    pub lease_duration_seconds: Option<u64>,
    /// Private ingest-control endpoint used for retained-event lease ownership.
    #[arg(long = "ingest-control-addr")]
    pub ingest_control_addr: Option<String>,
    /// File containing the ingest-control bearer token.
    #[arg(long = "ingest-control-token-path")]
    pub ingest_control_bearer_token_path: Option<PathBuf>,
    /// Loopback-only owner control endpoint for coherent checkpoint capture.
    #[arg(long = "projector-control-listen-addr")]
    pub projector_control_listen_addr: Option<SocketAddr>,
    /// File containing the bearer token required by projector owner control.
    #[arg(long = "projector-control-token-path")]
    pub projector_control_bearer_token_path: Option<PathBuf>,
    /// Shared state-bundle candidate root, matching ingest checkpoint staging.
    #[arg(long = "projector-control-checkpoint-staging-root")]
    pub projector_control_checkpoint_staging_root: Option<PathBuf>,
    /// Operational HTTP endpoint for health, readiness, and metrics.
    #[arg(long = "ops-listen-addr")]
    pub ops_listen_addr: Option<SocketAddr>,
    /// Optional operational or backup subcommand.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands supported by `zinder-projector`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Probe the operational endpoint once and exit 0 only when it answers
    /// HTTP 200. Reads only `[ops] listen_addr` from the `--config` file.
    Healthcheck(OpsHealthcheckArgs),
    /// Coherent wallet backup, verification, and restore operations.
    #[command(flatten)]
    Recovery(recovery::RecoveryCommand),
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

/// Runs the standalone `zinder-projector` CLI from process arguments.
pub async fn main_from_env(binary_name: &str) -> ExitCode {
    let mut cli = Cli::parse();
    let recovery = match cli.command.take() {
        Some(Command::Healthcheck(healthcheck)) => {
            return run_healthcheck(binary_name, &healthcheck, cli.config_path.as_deref());
        }
        Some(Command::Recovery(command)) => Some(command),
        None => None,
    };
    if let Err(error) = install_tracing_subscriber() {
        return emit_tracing_subscriber_error(binary_name, &error);
    }
    run_parsed_cli(binary_name, cli, recovery).await
}

/// Runs `zinder-projector` from explicit CLI arguments when tracing is already
/// initialized by a parent process such as `zinder run`.
pub async fn run_from_args<I, T>(binary_name: &str, args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let mut cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => return print_clap_error(&error),
    };
    let recovery = match cli.command.take() {
        Some(Command::Healthcheck(healthcheck)) => {
            return run_healthcheck(binary_name, &healthcheck, cli.config_path.as_deref());
        }
        Some(Command::Recovery(command)) => Some(command),
        None => None,
    };
    run_parsed_cli(binary_name, cli, recovery).await
}

async fn run_parsed_cli(
    binary_name: &str,
    cli: Cli,
    recovery: Option<recovery::RecoveryCommand>,
) -> ExitCode {
    if !host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }
    if !host_cpu_meets_pasta_backend_baseline() {
        return ExitCode::FAILURE;
    }
    if cli.print_config {
        return print_config(cli);
    }
    if let Some(command) = recovery {
        return command.run(binary_name, cli).await;
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
    let load_config = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
    let mut config = match config::load_projector_config(config_path, cli.into()) {
        Ok(config) => config,
        Err(error) => {
            load_config.fail(&error);
            return Err(error);
        }
    };
    if let Err(error) = config::ensure_build_owner(&mut config) {
        load_config.fail(&error);
        return Err(error);
    }
    let ops_server = OpsServer {
        service_name: PROJECTOR_SERVICE_NAME,
        service_version: env!("CARGO_PKG_VERSION"),
        network_name: encode_zinder_native_chain_name(config.network),
        advertised_capabilities: Arc::from([]),
    };
    if config.ops_listen_addr.is_some()
        && let Err(error) = install_metrics_recorder(&ops_server).map_err(OpsServerError::from)
    {
        load_config.fail(&error);
        return Err(error.into());
    }
    load_config.complete();

    let readiness = Readiness::default();
    let ops_handle = if let Some(address) = config.ops_listen_addr {
        let start_api = StartupPhase::StartApi.start();
        let listener = spawn_ops_endpoint(address, ops_server, readiness.clone()).await;
        Some(finish_startup_phase(start_api, listener)?)
    } else {
        None
    };
    let cancel = CancellationToken::new();
    let _signal = cancel_on_terminating_signal(cancel.clone());

    let projector_outcome =
        Box::pin(run_owned_projector(&config, &readiness, cancel.clone())).await;
    readiness.set(ReadinessState::not_ready(
        zinder_runtime::ReadinessCause::ShuttingDown,
    ));
    cancel.cancel();
    let ops_outcome = match ops_handle {
        Some(ops_handle) => ops_handle.shutdown().await,
        None => Ok(()),
    };
    match projector_outcome {
        Err(error) => {
            if let Err(ops_error) = ops_outcome {
                tracing::warn!(
                    target: "zinder::projector",
                    event = "ops_endpoint_shutdown_failed",
                    error = %ops_error,
                    "operational endpoint shutdown also failed"
                );
            }
            Err(error)
        }
        Ok(()) => ops_outcome.map_err(ProjectorError::from),
    }
}

fn finish_startup_phase<T, E: std::fmt::Display>(
    phase: StartupPhaseGuard,
    outcome: Result<T, E>,
) -> Result<T, E> {
    match &outcome {
        Ok(_) => phase.complete(),
        Err(error) => phase.fail(error),
    }
    outcome
}

fn complete_pending_phase(phase: &mut Option<StartupPhaseGuard>) {
    if let Some(phase) = phase.take() {
        phase.complete();
    }
}

async fn run_owned_projector(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    cancel: CancellationToken,
) -> Result<(), ProjectorError> {
    let mut active_phase = None;
    let outcome = Box::pin(project_wallet(config, readiness, cancel, &mut active_phase)).await;
    if let (Err(error), Some(phase)) = (&outcome, active_phase) {
        phase.fail(error);
    }
    outcome
}

#[allow(
    clippy::too_many_lines,
    reason = "the projector ownership sequence is intentionally visible in one fail-closed order"
)]
async fn project_wallet(
    config: &config::ProjectorConfig,
    readiness: &Readiness,
    cancel: CancellationToken,
    active_phase: &mut Option<StartupPhaseGuard>,
) -> Result<(), ProjectorError> {
    let reorg_policy = CanonicalReorgPolicy::new(config.reorg_window_blocks).map_err(|error| {
        zinder_runtime::ConfigError::invalid(format!(
            "projector.reorg_window_blocks is invalid: {error}"
        ))
    })?;
    *active_phase = Some(StartupPhase::OpenStorage.start());
    let Some(mut canonical) = open_canonical_secondary(config, reorg_policy, &cancel)? else {
        return Ok(());
    };
    complete_pending_phase(active_phase);
    *active_phase = Some(StartupPhase::AdmitIngestControl.start());
    let Some(mut canonical_control) = CanonicalWriterControlClient::connect_at_startup(
        &config.ingest_control_addr,
        config.ingest_control_bearer_token.as_ref(),
        config.projector_control.bearer_token.as_ref(),
        &cancel,
    )
    .await?
    else {
        return Ok(());
    };
    // Admission needs the wallet cursor to choose a fence. Only the phase
    // performing the current operation owns its failure.
    let admission = active_phase.replace(StartupPhase::RecoverState.start());
    let resumable_wallet = open_resumable_wallet(config, canonical.reorg_policy())?;
    let recovery = std::mem::replace(active_phase, admission);
    let resumable_source = resumable_wallet
        .as_ref()
        .map(|wallet| WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence()));
    let mut startup_readiness = resumable_source.map_or_else(
        || FollowingReadiness::new(readiness),
        |wallet_source| {
            FollowingReadiness::for_wallet(
                readiness,
                wallet_source.source_position().tip.height.value(),
            )
        },
    );
    // Following authenticates every event past a resumed wallet against the
    // writer's page, so only a fresh build needs the writer's exact fence.
    let Some(writer_fence) = converge_on_writer_fence(
        &mut canonical,
        &mut canonical_control,
        &mut startup_readiness,
        resumable_source.map(|wallet_source| wallet_source.source_position().event_sequence),
        None,
        &cancel,
        config.fence_convergence_timeout,
    )
    .await?
    else {
        return Ok(());
    };

    complete_pending_phase(active_phase);
    *active_phase = recovery;
    if let Some(wallet) = resumable_wallet {
        let wallet_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        match admit_resumed_following(&mut canonical_control, wallet_source, config.lease_duration)
            .await?
        {
            ResumedFollowingAdmission::Anchored(canonical_lease) => {
                complete_pending_phase(active_phase);
                tracing::info!(
                    target: "zinder::projector",
                    event = "wallet_projection_following_resumed",
                    height = wallet_source.source_position().tip.height.value(),
                    event_sequence = wallet_source.source_position().event_sequence,
                    "resuming wallet projection from its persisted authenticated cursor"
                );
                let control =
                    ProjectorControlTasks::start(&config.projector_control, &cancel).await?;
                return run_continuous_wallet_following(
                    config,
                    readiness,
                    cancel,
                    canonical,
                    canonical_control,
                    wallet,
                    canonical_lease,
                    control,
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
                return Box::pin(bootstrap_resumed_wallet_following(
                    config,
                    readiness,
                    cancel,
                    canonical,
                    canonical_control,
                    wallet,
                    first_retained_event_sequence,
                    active_phase,
                ))
                .await;
            }
        }
    }
    let canonical_ready = *writer_fence.secondary();
    let expected_wallet_source = canonical_source_identity(&canonical_ready);

    readiness.set(ReadinessState::syncing(
        Some(canonical_ready.visible_block_count),
        None,
        Some(canonical_ready.visible_tip.height.value()),
    ));
    let initial_now = UnixTimestampMillis::now();
    let initial_expiry = lease_expiry(initial_now, config.lease_duration);
    let canonical_cursor = CanonicalEventCursor::at(canonical_ready.visible_event_sequence)?;
    let build_owner = config.build_owner.ok_or_else(|| {
        zinder_runtime::ConfigError::invalid("projector build owner must be resolved before build")
    })?;
    let canonical_lease = CanonicalRetentionLease::new(
        build_owner,
        canonical_ready.visible_epoch.value(),
        canonical_cursor.as_bytes().to_vec(),
        initial_expiry,
    );
    let canonical_lease = canonical_control.acquire(canonical_lease).await?;
    let canonical_lease_for_recovery = canonical_lease.clone();
    let wallet_lease_request = WalletProjectionBuildLeaseRequest::new(
        WalletProjectionBuildOwner::from_bytes(build_owner),
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
        accumulator_hashing_workers: config.build.accumulator_hashing_workers,
    };
    let lease_duration = config.lease_duration;
    let build_cancel = cancel.clone();
    let runtime = tokio::runtime::Handle::current();
    let wallet_path = config.wallet_path.clone();
    let network = config.network;
    let joined_build = tokio::task::spawn_blocking(move || {
        let mut canonical_control = canonical_control;
        let mut canonical_lease = canonical_lease;
        let mut shutdown_at_heartbeat = false;
        let mut heartbeat =
            |phase, wallet_lease: zinder_wallet_projection::WalletProjectionBuildLease| {
                if build_cancel.is_cancelled() {
                    shutdown_at_heartbeat = true;
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
        let build_outcome = match build_outcome {
            Err(zinder_wallet_rocksdb::RocksDbWalletError::ProjectionBuildCancelled)
                if shutdown_at_heartbeat => Ok(None),
            outcome => outcome.map(Some),
        };
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
        Ok(Some(outcome)) => outcome,
        Ok(None) => {
            match canonical_control.release(&canonical_lease).await {
                Err(CanonicalWriterControlError::Status(status))
                    if status.code() == tonic::Code::Unavailable =>
                {
                    tracing::warn!(
                        target: "zinder::projector",
                        code = ?status.code(),
                        expires_at_unix_millis = canonical_lease.expires_at().value(),
                        "writer unavailable during shutdown, leaving any remaining retention lease to expire"
                    );
                }
                release => release?,
            }
            return Ok(());
        }
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
    complete_pending_phase(active_phase);
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
    let control = ProjectorControlTasks::start(&config.projector_control, &cancel).await?;
    run_continuous_wallet_following(
        config,
        readiness,
        cancel,
        canonical,
        canonical_control,
        wallet,
        canonical_lease,
        control,
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
    mut control: ProjectorControlTasks,
) -> Result<(), ProjectorError> {
    wallet.record_resource_gauges();
    let mut following_readiness = FollowingReadiness::for_wallet(
        readiness,
        WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence())
            .source_position()
            .tip
            .height
            .value(),
    );
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

        let wallet_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let wallet_event_sequence = wallet_source.source_position().event_sequence;
        let Some(writer_fence) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            &mut following_readiness,
            Some(wallet_event_sequence),
            Some(ConvergenceLease {
                lease: &mut canonical_lease,
                duration: config.lease_duration,
            }),
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        let target_source = canonical_source_identity(writer_fence.secondary());
        let target_fence = canonical.event_fence();
        // A trailing secondary always holds events past the wallet, so only
        // an attested fence can equal the wallet's.
        if wallet_source == target_source {
            let renewed = renew_following_retention_lease_if_due(
                &mut canonical_control,
                &mut canonical_lease,
                wallet_source,
                config.lease_duration,
            )
            .await?;
            if renewed {
                let Some(post_renewal_fence) = converge_on_writer_fence(
                    &mut canonical,
                    &mut canonical_control,
                    &mut following_readiness,
                    Some(wallet_event_sequence),
                    Some(ConvergenceLease {
                        lease: &mut canonical_lease,
                        duration: config.lease_duration,
                    }),
                    &cancel,
                    config.fence_convergence_timeout,
                )
                .await?
                else {
                    return Ok(());
                };
                if wallet_source != canonical_source_identity(post_renewal_fence.secondary()) {
                    continue;
                }
            }
            wallet.record_resource_gauges();
            following_readiness.caught_up(wallet_source.source_position().tip.height.value());
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

        following_readiness.trailing(
            wallet_source.source_position().tip.height.value(),
            writer_fence.writer_tip_height(),
        );
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
            Ok(RetainedEventPageValidation::Reconverge) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(error) => return Err(rebuild_required_for_expired_cursor(error)),
        };
        let transition = plan_next_wallet_transition(&canonical, &retained_events)?;
        renew_following_retention_lease_for_transition(
            &mut canonical_control,
            &mut canonical_lease,
            wallet_source,
            config.lease_duration,
        )
        .await?;
        let resulting_fence = transition.resulting_fence();
        let reconciled_source = match apply_next_wallet_transition(
            &mut wallet,
            &canonical,
            wallet_source,
            transition,
            config.reorg_window_blocks,
            config.follow.max_transition_logical_bytes,
            &cancel,
        ) {
            Ok(Some(reconciled_source)) => reconciled_source,
            Ok(None) => return Ok(()),
            Err(ProjectorError::WalletTransitionLogicalByteLimit {
                limit_bytes,
                required_bytes,
            }) => {
                match Box::pin(park_on_refused_transition(
                    config,
                    readiness,
                    &cancel,
                    &mut canonical,
                    &mut canonical_control,
                    &mut control,
                    ParkedLease {
                        lease: &mut canonical_lease,
                        anchor: wallet_source,
                    },
                    wallet_source,
                    RefusedTransition {
                        resulting_fence,
                        required_bytes,
                        limit_bytes,
                    },
                    writer_fence.writer_tip_height(),
                ))
                .await?
                {
                    ParkedFollowing::Cancelled => return Ok(()),
                    ParkedFollowing::TransitionChanged => continue,
                }
            }
            Err(error) => return Err(error),
        };
        following_readiness.wallet_advanced(reconciled_source.source_position().tip.height.value());
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
            return Ok(Self::disabled());
        };
        let start_api = StartupPhase::StartApi.start();
        let bound = Self::bind(config, listen_addr, cancel).await;
        finish_startup_phase(start_api, bound)
    }

    async fn bind(
        config: &zinder_runtime::ResolvedProjectorControl,
        listen_addr: std::net::SocketAddr,
        cancel: &CancellationToken,
    ) -> Result<Self, ProjectorError> {
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

    /// Returns tasks without an owner-control listener, which only pace the
    /// follow loop's polling.
    const fn disabled() -> Self {
        Self {
            commands: None,
            server: None,
        }
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
        ProjectorControlCommand::CreateStateBundleCapture { request, reply } => {
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
                request,
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
    request: CreateStateBundleCaptureRequest,
    expected_fence: CanonicalEventFence,
) -> Result<CreateStateBundleCaptureResponse, tonic::Status> {
    let renewed_until = lease_expiry(UnixTimestampMillis::now(), config.lease_duration);
    let renewal = canonical_lease.renewed(renewed_until);
    *canonical_lease = canonical_control
        .renew(renewal)
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    let paths = match prepare_state_bundle_capture(
        &config.projector_control.checkpoint_staging_root,
        &request.candidate_id,
    ) {
        Ok(paths) => paths,
        Err(StateBundleError::TargetExists { path }) if request.reuse_completed => {
            return projector_control::read_capture_receipt(
                &path,
                &request.candidate_id,
                config.network,
                None,
            )
            .map(|(receipt, _)| receipt)
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()));
        }
        Err(error) => return Err(state_bundle_status(&error)),
    };
    let response = canonical_control
        .create_owner_checkpoint(
            request.candidate_id,
            paths.staging_root_binding().to_vec(),
            expected_writer_fence(expected_fence),
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
    projector_control::read_capture_receipt(
        paths.root(),
        paths.candidate_id(),
        config.network,
        Some(&manifest),
    )
    .map(|(receipt, _)| receipt)
    .map_err(|error| tonic::Status::failed_precondition(error.to_string()))
}

fn state_bundle_status(error: &StateBundleError) -> tonic::Status {
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
    recover_state: &mut Option<StartupPhaseGuard>,
) -> Result<(), ProjectorError> {
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let mut following_readiness = FollowingReadiness::for_wallet(
        readiness,
        wallet_source.source_position().tip.height.value(),
    );
    let mut bootstrap_lease = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let Some(writer_fence) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            &mut following_readiness,
            Some(wallet_source.source_position().event_sequence),
            None,
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        set_following_syncing(readiness, wallet_source, writer_fence.writer_tip_height());
        let retained_events = match fetch_retained_event_page_to_target(
            &mut canonical_control,
            &canonical,
            wallet_source,
            canonical.event_fence(),
        )
        .await
        {
            Ok(RetainedEventPageValidation::Reconverge) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(error) => return Err(rebuild_required_for_expired_cursor(error)),
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

    // Bound before the bootstrap can park, so a parked bootstrap refuses
    // captures; continuous following inherits this listener.
    let recovery = recover_state.take();
    let mut control = ProjectorControlTasks::start(&config.projector_control, &cancel).await?;
    *recover_state = recovery;
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let Some(writer_fence) = converge_on_writer_fence(
            &mut canonical,
            &mut canonical_control,
            &mut following_readiness,
            Some(wallet_source.source_position().event_sequence),
            Some(ConvergenceLease {
                lease: &mut bootstrap_lease,
                duration: config.lease_duration,
            }),
            &cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(());
        };
        set_following_syncing(readiness, wallet_source, writer_fence.writer_tip_height());
        let retained_events = match fetch_retained_event_page_to_target(
            &mut canonical_control,
            &canonical,
            wallet_source,
            canonical.event_fence(),
        )
        .await
        {
            Ok(RetainedEventPageValidation::Reconverge) => {
                if wait_for_follow_poll(&cancel).await {
                    return Ok(());
                }
                continue;
            }
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Err(error) => return Err(rebuild_required_for_expired_cursor(error)),
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
        let resulting_fence = transition.resulting_fence();
        let reconciled_source = match apply_next_wallet_transition(
            &mut wallet,
            &canonical,
            wallet_source,
            transition,
            config.reorg_window_blocks,
            config.follow.max_transition_logical_bytes,
            &cancel,
        ) {
            Ok(Some(reconciled_source)) => reconciled_source,
            Ok(None) => return Ok(()),
            Err(ProjectorError::WalletTransitionLogicalByteLimit {
                limit_bytes,
                required_bytes,
            }) => {
                match Box::pin(park_on_refused_transition(
                    config,
                    readiness,
                    &cancel,
                    &mut canonical,
                    &mut canonical_control,
                    &mut control,
                    ParkedLease {
                        lease: &mut bootstrap_lease,
                        anchor: bootstrap_source,
                    },
                    wallet_source,
                    RefusedTransition {
                        resulting_fence,
                        required_bytes,
                        limit_bytes,
                    },
                    writer_fence.writer_tip_height(),
                ))
                .await?
                {
                    ParkedFollowing::Cancelled => return Ok(()),
                    ParkedFollowing::TransitionChanged => continue,
                }
            }
            Err(error) => return Err(error),
        };
        let canonical_lease = advance_retention_lease_anchor(
            &mut canonical_control,
            bootstrap_lease,
            reconciled_source,
            config.lease_duration,
        )
        .await?;
        complete_pending_phase(recover_state);
        return run_continuous_wallet_following(
            config,
            readiness,
            cancel,
            canonical,
            canonical_control,
            wallet,
            canonical_lease,
            control,
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
    target_height: u32,
) {
    let wallet_height = wallet_source.source_position().tip.height.value();
    readiness.set(ReadinessState::syncing(
        target_height.checked_sub(wallet_height).map(u64::from),
        Some(wallet_height),
        Some(target_height),
    ));
}

/// Opens the published wallet the projector resumes following, or returns
/// `None` when the projector must construct one.
fn open_resumable_wallet(
    config: &config::ProjectorConfig,
    canonical_reorg_policy: CanonicalReorgPolicy,
) -> Result<Option<RocksDbWalletFollowingStore>, ProjectorError> {
    if !config.wallet_path.exists() {
        return Ok(None);
    }
    match RocksDbWalletStore::open_ready_for_following(
        &config.wallet_path,
        config.network,
        config.wallet_rocksdb_budget,
    ) {
        Ok(mut wallet) => {
            raise_wallet_reorg_depth(&mut wallet, canonical_reorg_policy)?;
            Ok(Some(wallet))
        }
        Err(zinder_wallet_rocksdb::RocksDbWalletError::StoreNotReady { .. }) => {
            let build_owner = config.build_owner.ok_or_else(|| {
                zinder_runtime::ConfigError::invalid(
                    "projector build owner must be resolved before discarding unpublished wallet build",
                )
            })?;
            // A crashed build still holds its lease under this owner, which
            // may discard it at once; another owner must wait for the lease
            // to expire.
            RocksDbWalletBuildStore::open(
                &config.wallet_path,
                config.network,
                config.wallet_rocksdb_budget,
            )?
            .discard_unpublished(
                WalletProjectionBuildOwner::from_bytes(build_owner),
                UnixTimestampMillis::now(),
            )?;
            tracing::info!(
                target: "zinder::projector",
                event = "wallet_projection_unpublished_build_discarded",
                "discarded an unpublished wallet build; constructing again from the current canonical fence"
            );
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Binds a resumed wallet's undo depth to the canonical effective window,
/// which canonical admission already matched against this projector's
/// configured window.
fn raise_wallet_reorg_depth(
    wallet: &mut RocksDbWalletFollowingStore,
    canonical_reorg_policy: CanonicalReorgPolicy,
) -> Result<(), ProjectorError> {
    let previous_depth = wallet.supported_reorg_depth();
    let reorg_window_blocks = canonical_reorg_policy.reorg_window_blocks();
    wallet.raise_supported_reorg_depth(reorg_window_blocks)?;
    if previous_depth != reorg_window_blocks {
        wallet.record_resource_gauges();
        tracing::info!(
            target: "zinder::projector",
            event = "wallet_projection_reorg_depth_raised",
            previous_supported_reorg_depth = previous_depth,
            supported_reorg_depth = reorg_window_blocks,
            "raised the wallet's supported reorg depth to the canonical window; retained undo rows grow with new blocks"
        );
    }
    Ok(())
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
        Ok(()) => wallet.record_resource_gauges(),
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
    let position = observed.source_position();
    tracing::info!(
        target: "zinder::projector",
        event = "wallet_projection_following_applied",
        process_id = std::process::id(),
        chain_epoch = position.chain_epoch_id.value(),
        event_sequence = position.event_sequence,
        visible_height = position.tip.height.value(),
        visible_tip_hash = ?position.tip.hash,
        sequence_digest = ?observed.source_sequence_digest().as_bytes(),
        "applied and verified the following wallet fence"
    );
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

/// Where the canonical secondary stands against the writer after
/// convergence.
#[derive(Clone, Copy, Debug)]
enum WriterFence {
    /// The writer attests the secondary's exact fence.
    Attested(CanonicalStoreReadyEvidence),
    /// The writer has moved past the secondary, which still holds retained
    /// events past the wallet. Each of them is authenticated against the
    /// writer's retained-event page before the wallet applies it.
    Trailing {
        secondary: CanonicalStoreReadyEvidence,
        writer_tip_height: u32,
    },
}

impl WriterFence {
    const fn secondary(&self) -> &CanonicalStoreReadyEvidence {
        match self {
            Self::Attested(secondary) | Self::Trailing { secondary, .. } => secondary,
        }
    }

    const fn writer_tip_height(&self) -> u32 {
        match self {
            Self::Attested(secondary) => secondary.visible_tip.height.value(),
            Self::Trailing {
                writer_tip_height, ..
            } => *writer_tip_height,
        }
    }
}

/// Opens a canonical secondary that adopts the schedule bound to READY.
fn open_canonical_secondary(
    config: &config::ProjectorConfig,
    reorg_policy: CanonicalReorgPolicy,
    cancel: &CancellationToken,
) -> Result<Option<RocksDbCanonicalSecondary>, ProjectorError> {
    if cancel.is_cancelled() {
        return Ok(None);
    }
    std::fs::create_dir_all(&config.canonical_secondary_path).map_err(|source| {
        ProjectorError::CanonicalSecondaryDirectoryCreate {
            path: config.canonical_secondary_path.clone(),
            source,
        }
    })?;
    let canonical = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &config.canonical_path,
        &config.canonical_secondary_path,
        config.network,
        config.canonical_requirements(),
        reorg_policy,
        config.canonical_rocksdb_budget,
    )
    .map_err(|source| ProjectorError::CanonicalSecondaryOpen {
        primary_path: config.canonical_path.clone(),
        secondary_path: config.canonical_secondary_path.clone(),
        source,
    })?;
    Ok(Some(canonical))
}

/// Converges the canonical secondary on the writer, returning `None` when
/// `cancel` fires first.
///
/// With `wallet_event_sequence`, a round whose caught-up secondary holds
/// events past the wallet ends convergence even while the writer is ahead,
/// so a burst of blocks cannot keep the wallet from advancing. Without it the
/// writer must attest the secondary's exact fence. A deadline that passes
/// first reports the secondary as stale, renews `lease`, and
/// starts another attempt.
#[allow(
    clippy::too_many_arguments,
    reason = "convergence keeps the secondary, writer client, readiness owner, wallet position, retention lease, cancellation, and deadline explicit"
)]
async fn converge_on_writer_fence(
    canonical: &mut RocksDbCanonicalSecondary,
    control: &mut CanonicalWriterControlClient,
    readiness: &mut FollowingReadiness<'_>,
    wallet_event_sequence: Option<u64>,
    lease: Option<ConvergenceLease<'_>>,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<Option<WriterFence>, ProjectorError> {
    let network = canonical.network();
    let mut probe = LiveWriterFenceProbe {
        canonical,
        control,
        readiness,
        wallet_event_sequence,
        lease,
        timeout,
        last_lag: None,
    };
    let outcome = converge_without_deadline_exit(
        &mut probe,
        network,
        CanonicalFenceConvergencePolicy {
            timeout,
            max_attempts: None,
            initial_delay: CANONICAL_FENCE_CONVERGENCE_INITIAL_DELAY,
            max_delay: CANONICAL_FENCE_CONVERGENCE_MAX_DELAY,
        },
        cancel,
    )
    .await?;
    let secondary = canonical.ready_evidence();
    Ok(outcome.map(|outcome| match outcome {
        FollowingConvergence::Attested => WriterFence::Attested(secondary),
        FollowingConvergence::Trailing(lag) => WriterFence::Trailing {
            secondary,
            writer_tip_height: lag.writer_visible_tip_height,
        },
    }))
}

/// How [`converge_without_deadline_exit`] ended a convergence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FollowingConvergence {
    /// The writer attested the secondary's exact fence.
    Attested,
    /// The probe accepted a round in which the secondary trailed the writer.
    Trailing(CanonicalFenceLag),
}

/// Runs [`converge_on_canonical_writer_fence`] until the writer attests the
/// secondary or the probe accepts a trailing round, or returns `None` when
/// `cancel` fires first.
///
/// A secondary that trails the writer is ordinary replication lag, never a
/// reason to stop following: each deadline that passes calls
/// [`FollowingFenceProbe::deadline_passed`] and starts a new attempt.
/// Divergence and probe failures still end it.
async fn converge_without_deadline_exit<Probe>(
    probe: &mut Probe,
    network: zinder_core::Network,
    policy: CanonicalFenceConvergencePolicy,
    cancel: &CancellationToken,
) -> Result<Option<FollowingConvergence>, ProjectorError>
where
    Probe: FollowingFenceProbe,
{
    loop {
        match converge_on_canonical_writer_fence(probe, network, policy, cancel).await {
            Ok(CanonicalFenceConvergence::Converged(_)) => {
                return Ok(Some(FollowingConvergence::Attested));
            }
            Ok(CanonicalFenceConvergence::Trailing(lag)) => {
                return Ok(Some(FollowingConvergence::Trailing(lag)));
            }
            Ok(CanonicalFenceConvergence::Cancelled) => return Ok(None),
            Err(CanonicalFenceConvergenceError::DidNotConverge) => probe.deadline_passed().await?,
            Err(CanonicalFenceConvergenceError::Probe(error)) => return Err(error),
            Err(CanonicalFenceConvergenceError::Diverged(divergence)) => {
                return Err(ProjectorError::CanonicalFenceDiverged(divergence));
            }
        }
    }
}

/// A probe that keeps converging after a convergence deadline passes.
trait FollowingFenceProbe: CanonicalFenceProbe<Error = ProjectorError> {
    /// Handles an attempt whose deadline passed before the secondary reached
    /// the writer, before the next attempt starts.
    fn deadline_passed(&mut self) -> impl Future<Output = Result<(), ProjectorError>> + Send;
}

/// The retention lease that protects a wallet's backlog while convergence
/// waits, renewed to a full `duration` whenever a deadline passes.
struct ConvergenceLease<'a> {
    lease: &'a mut CanonicalRetentionLease,
    duration: Duration,
}

struct LiveWriterFenceProbe<'a, 'readiness> {
    canonical: &'a mut RocksDbCanonicalSecondary,
    control: &'a mut CanonicalWriterControlClient,
    readiness: &'a mut FollowingReadiness<'readiness>,
    /// Event sequence of the wallet the secondary feeds, when convergence may
    /// end on a secondary that trails the writer.
    wallet_event_sequence: Option<u64>,
    lease: Option<ConvergenceLease<'a>>,
    timeout: Duration,
    /// Lag of the last round in this attempt whose secondary trailed the
    /// writer.
    last_lag: Option<CanonicalFenceLag>,
}

impl FollowingFenceProbe for LiveWriterFenceProbe<'_, '_> {
    async fn deadline_passed(&mut self) -> Result<(), ProjectorError> {
        self.report_unreached_writer();
        // A deadline may be longer than the idle renewal headroom, so each
        // passed attempt renews to a full lease duration.
        if let Some(lease) = self.lease.as_mut()
            && let Some(renewed_expiry) = following_retention_lease_transition_expiry(
                UnixTimestampMillis::now(),
                lease.lease.expires_at(),
                lease.duration,
            )
        {
            *lease.lease = self
                .control
                .renew(lease.lease.renewed(renewed_expiry))
                .await?;
        }
        Ok(())
    }
}

impl LiveWriterFenceProbe<'_, '_> {
    fn report_unreached_writer(&mut self) {
        tracing::warn!(
            target: "zinder::projector",
            event = "canonical_secondary_behind_writer",
            secondary_height = self.canonical.event_fence().visible_tip().height.value(),
            writer_height = ?self.last_lag.map(|lag| lag.writer_visible_tip_height),
            lag_chain_epochs = ?self.last_lag.map(|lag| lag.lag_chain_epochs),
            timeout_seconds = self.timeout.as_secs(),
            "canonical secondary did not reach the writer fence within the convergence timeout; retrying"
        );
        self.readiness.writer_unreached(self.last_lag.take());
    }
}

impl CanonicalFenceProbe for LiveWriterFenceProbe<'_, '_> {
    type Error = ProjectorError;

    fn catch_up(
        &mut self,
    ) -> impl std::future::Future<
        Output = Result<Option<zinder_proto::v1::ingest::CanonicalWriterFence>, ProjectorError>,
    > {
        std::future::ready(
            self.canonical
                .try_catch_up()
                .map(|_| Some(expected_writer_fence(self.canonical.event_fence())))
                .map_err(ProjectorError::from),
        )
    }

    async fn writer_status(
        &mut self,
    ) -> Result<zinder_proto::v1::ingest::CanonicalWriterStatusResponse, ProjectorError> {
        Ok(self.control.writer_status().await?)
    }

    /// The projector waits for the writer-admitted schedule revision, but
    /// converges on the fence of a parked or unconfirmed writer, or of one
    /// that reports damage in its store: only serving readers refuse that
    /// fence, so the projector keeps following instead of failing.
    fn attests_writer_schedule(
        &self,
        status: &zinder_proto::v1::ingest::CanonicalWriterStatusResponse,
    ) -> bool {
        writer_attests_network_upgrade_schedule(status, self.canonical.network_upgrade_schedule())
    }

    fn secondary_behind(&mut self, lag: CanonicalFenceLag, elapsed: Duration) {
        self.last_lag = Some(lag);
        report_writer_fence_lag(self.readiness, lag, elapsed);
    }

    fn accepts_trailing(&self, _lag: CanonicalFenceLag) -> bool {
        secondary_holds_unapplied_events(
            self.canonical.event_fence().chain_event_sequence(),
            self.wallet_event_sequence,
        )
    }
}

/// Whether a secondary at `secondary_event_sequence` holds events that the
/// wallet at `wallet_event_sequence`, if one is open, has not applied.
fn secondary_holds_unapplied_events(
    secondary_event_sequence: u64,
    wallet_event_sequence: Option<u64>,
) -> bool {
    wallet_event_sequence
        .is_some_and(|wallet_event_sequence| secondary_event_sequence > wallet_event_sequence)
}

/// Reports lag that outlasts `CANONICAL_FENCE_CONVERGENCE_SYNCING_AFTER` as
/// trailing the writer; shorter races leave readiness untouched.
fn report_writer_fence_lag(
    readiness: &mut FollowingReadiness<'_>,
    lag: CanonicalFenceLag,
    elapsed: Duration,
) {
    if elapsed < CANONICAL_FENCE_CONVERGENCE_SYNCING_AFTER {
        return;
    }
    readiness.writer_ahead(
        lag.secondary_visible_tip_height,
        lag.writer_visible_tip_height,
    );
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
/// Otherwise, the caller must classify the writer again.
enum RetainedEventPageValidation {
    /// The page's writer fence neither matches nor extends the local
    /// secondary fence.
    Reconverge,
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
    let page = control
        .event_page(cursor.as_slice(), page_limit.get())
        .await?;
    let local_events = canonical.canonical_event_history(CanonicalEventHistoryRequest::new(
        Some(cursor.as_slice()),
        page_limit,
    ))?;
    validate_one_retained_event_page(&page, local_events, wallet_source, target_fence)
}

/// Reports a wallet cursor the writer no longer retains as a side-by-side
/// rebuild requirement, and passes every other page failure through.
fn rebuild_required_for_expired_cursor(error: ProjectorError) -> ProjectorError {
    if let ProjectorError::CanonicalEventCursorExpired {
        wallet_event_sequence,
        oldest_retained_event_sequence,
    } = error
    {
        return ProjectorError::WalletRebuildRequired {
            wallet_event_sequence,
            oldest_retained_event_sequence,
        };
    }
    error
}

/// Validates one remote `EventPage` against retained local-secondary rows.
///
/// This deliberately does not require the page to end at the current writer
/// fence because a bounded reconciliation may span several writer pages. A
/// writer that has committed past the secondary also returns those later
/// events, so the secondary's events must then be an exact prefix of the
/// page; the wallet only ever applies events the writer's page attests.
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
    let writer_extends_secondary = match compare_canonical_writer_fences(
        page_writer_fence,
        &expected_writer_fence(target_fence),
    ) {
        CanonicalFenceRelation::Matches => false,
        CanonicalFenceRelation::SecondaryBehind(_) => true,
        CanonicalFenceRelation::Diverged(_) => {
            return Ok(RetainedEventPageValidation::Reconverge);
        }
    };
    if page.events.len() > CANONICAL_CONTROL_EVENT_PAGE_EVENTS_USIZE {
        return Err(ProjectorError::CanonicalEventPageInvalid {
            reason: "writer returned more events than the canonical-control page ceiling",
        });
    }
    let lengths_agree = if writer_extends_secondary {
        page.events.len() >= local_events.len()
    } else {
        page.events.len() == local_events.len()
    };
    if !lengths_agree {
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
    *wire == expected_writer_fence(local)
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
        .saturating_add(counts.block_hash_by_height_count)
}

#[allow(
    clippy::print_stderr,
    reason = "the container runtime records the healthcheck's stderr as its failure reason"
)]
fn run_healthcheck(
    binary_name: &str,
    healthcheck: &OpsHealthcheckArgs,
    config_path: Option<&Path>,
) -> ExitCode {
    let default_listen_addr = zinder_runtime::section_defaults::default_ops_listen_addr(
        zinder_runtime::RuntimeService::Projector,
    );
    match healthcheck.run(config_path, default_listen_addr) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{binary_name}: healthcheck failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[allow(
    clippy::print_stderr,
    reason = "no tracing subscriber is installed, so stderr is the only channel left"
)]
fn emit_tracing_subscriber_error(binary_name: &str, error: &TracingSubscriberError) -> ExitCode {
    eprintln!("{binary_name}: {error}");
    ExitCode::FAILURE
}

fn print_clap_error(error: &clap::Error) -> ExitCode {
    let exit_code = if error.exit_code() == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(2))
    };
    let _ = error.print();
    exit_code
}

fn emit_error(error: &ProjectorError) -> ExitCode {
    tracing::error!(target: "zinder::projector", %error, "projector failed");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU32,
        path::{Path, PathBuf},
        str::FromStr as _,
        time::Duration,
    };

    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use zinder_runtime::{BearerToken, ResolvedProjectorControl};

    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
        CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockFactsSequenceDigestVersion, CanonicalBlockReplayFormatVersion,
        CanonicalTransactionFacts, ChainEpochId, ChainTipMetadata, CommitmentTreeCheckpoint,
        CommitmentTreeFrontier, CommitmentTreeFrontiers, CompactBlockArtifact,
        CompactChainMetadata, LockTime, Network, PrivacyShape, SerializedBytesDigest,
        ShieldedProtocol, TransactionBlobArtifact, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, UnixTimestampMillis, UnsupportedSection, encode_canonical_block_replay,
    };

    use super::{
        CANONICAL_CONTROL_EVENT_PAGE_EVENTS, CanonicalBlockFactsSequenceDigest,
        CanonicalFenceConvergencePolicy, CanonicalFenceLag, CanonicalFenceProbe,
        CanonicalRetentionLease, FollowingConvergence, FollowingFenceProbe, FollowingReadiness,
        NextWalletTransition, ProjectorControlTasks, ProjectorError, ResumedFollowingRetentionPlan,
        RetainedEventPageValidation, WalletCanonicalSourceIdentity, WalletProjectionSourcePosition,
        classify_resumed_following_retention, converge_without_deadline_exit,
        encode_zinder_native_chain_name, expected_writer_fence,
        following_retention_lease_renewal_expiry, following_retention_lease_transition_expiry,
        lease_expiry, plan_next_wallet_transition, reconciliation_ranges, report_writer_fence_lag,
        require_bounded_reconciliation_replay, require_built_wallet_source,
        require_pre_promotion_follower_admission, require_retention_lease_anchor,
        secondary_holds_unapplied_events, settled_tip_for_transition,
        validate_one_retained_event_page, wallet_source_identity_from_fence,
    };
    use zinder_proto::v1::ingest::{
        CanonicalEventBlockRange, CanonicalEventPageResponse, CanonicalRetainedEventKind,
        CanonicalWriterFence, CanonicalWriterStatusResponse,
    };
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence,
        CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalLiveAppend,
        CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
        CanonicalRetainedEvent, CanonicalStoreBuildPlan, RawBlobRetention, RocksDbCanonicalBuilder,
        RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
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
        )?;
        let block_one_id = BlockId::new(
            block_one.facts.block_header.height,
            block_one.facts.block_header.block_hash,
        );
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            block_one_id,
            RawBlobRetention::None,
            reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_path,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(&activations, [Ok::<_, std::convert::Infallible>(block_one)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            block_one_id,
            1,
            regtest_checkpoint_frontiers_at(BlockHeight::new(1))?,
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            block_one_id,
            UnixTimestampMillis::new(1),
        ))?;
        let canonical = validated.publish_baseline(publication)?;
        let baseline_fence = canonical.event_fence();

        let old_block_two = empty_canonical_block(2, [0x11; 32], [0x22; 32])?;
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
        let replacement_block_two = empty_canonical_block(2, [0x11; 32], [0x32; 32])?;
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
                    empty_canonical_block(height, parent_hash, block_hash)?,
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
            zinder_store::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
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

        let capture = zinder_testkit::LogCapture::install_for_target("zinder::startup");
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
        let reason = error.to_string();
        assert!(
            capture.any(|event| event.field("phase") == Some("start_api")
                && event.field("phase_state") == Some("exit")
                && event.field("outcome") == Some("failed")
                && event.field("reason") == Some(reason.as_str()))
        );
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
    fn writer_fence_lag_reports_syncing_only_after_the_grace_period() {
        let readiness =
            zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(1)));
        let lag = CanonicalFenceLag {
            lag_chain_epochs: 2,
            secondary_visible_tip_height: 1,
            writer_visible_tip_height: 3,
        };

        let mut following_readiness = FollowingReadiness::new(&readiness);
        report_writer_fence_lag(&mut following_readiness, lag, Duration::from_millis(10));
        assert!(readiness.report().is_ready);

        report_writer_fence_lag(&mut following_readiness, lag, Duration::from_secs(1));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            zinder_runtime::ReadinessCause::Syncing {
                lag_blocks: Some(2),
                wait_reason: None,
            }
        ));
        assert_eq!(report.current_height, Some(1));
        assert_eq!(report.target_height, Some(3));
    }

    #[tokio::test]
    async fn writer_fence_lag_of_one_block_keeps_a_ready_projector_ready() {
        let readiness =
            zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(1)));
        let lag = CanonicalFenceLag {
            lag_chain_epochs: 1,
            secondary_visible_tip_height: 1,
            writer_visible_tip_height: 2,
        };

        let mut following_readiness = FollowingReadiness::for_wallet(&readiness, 1);
        report_writer_fence_lag(&mut following_readiness, lag, Duration::from_secs(1));
        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(1));
        assert_eq!(report.target_height, Some(2));
    }

    #[tokio::test]
    async fn writer_fence_lag_keeps_a_ready_wallet_behind_the_secondary_ready_at_its_height() {
        let readiness =
            zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(10)));
        let lag = CanonicalFenceLag {
            lag_chain_epochs: 1,
            secondary_visible_tip_height: 11,
            writer_visible_tip_height: 12,
        };

        let mut following_readiness = FollowingReadiness::for_wallet(&readiness, 10);
        report_writer_fence_lag(&mut following_readiness, lag, Duration::from_secs(1));
        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.target_height, Some(12));
    }

    #[tokio::test]
    async fn a_convergence_deadline_reports_the_lagging_secondary_and_keeps_converging()
    -> Result<(), Box<dyn std::error::Error>> {
        // The writer commits on each of its first 64 status reads, as during
        // a long burst, so several attempts pass their deadline first.
        let mut probe = BurstProbe::new(64, None);

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(outcome, Some(FollowingConvergence::Attested));
        assert!(
            probe.deadlines >= 1,
            "expected at least one passed deadline"
        );
        assert_eq!(probe.secondary_sequence, probe.writer_sequence);
        Ok(())
    }

    /// The projector converges on a writer that reports store damage.
    ///
    /// That writer keeps attesting its fence while it commits, so the
    /// projector treats it as it treats a parked writer's fence instead of
    /// failing, and the deployment does not restart into the same damage.
    #[tokio::test]
    async fn a_writer_reporting_store_damage_is_converged_on_like_a_parked_one()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut probe = BurstProbe::new(2, None);
        probe.store_damage = Some(zinder_proto::wire::canonical_store_damaged_detail(
            Some(BlockHeight::new(1)),
            None,
        ));

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(outcome, Some(FollowingConvergence::Attested));
        assert_eq!(probe.secondary_sequence, probe.writer_sequence);
        Ok(())
    }

    #[tokio::test]
    async fn a_secondary_past_the_wallet_ends_convergence_during_a_burst()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut probe = BurstProbe::new(u32::MAX, Some(4));

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await?;

        assert!(matches!(
            outcome,
            Some(FollowingConvergence::Trailing(CanonicalFenceLag {
                lag_chain_epochs: 1,
                ..
            }))
        ));
        assert_eq!(probe.deadlines, 0);
        assert_eq!(probe.catch_ups, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_resumed_wallet_at_the_secondary_follows_the_next_catch_up_during_a_burst()
    -> Result<(), Box<dyn std::error::Error>> {
        // A projector that restarts during a burst resumes a wallet that
        // already holds every event its secondary holds.
        let mut probe = BurstProbe::new(u32::MAX, Some(5));

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await?;

        assert!(matches!(outcome, Some(FollowingConvergence::Trailing(_))));
        assert_eq!(probe.deadlines, 0);
        assert_eq!(probe.catch_ups, 2);
        assert!(probe.secondary_sequence > 5);
        Ok(())
    }

    #[tokio::test]
    async fn a_fresh_build_waits_for_the_writer_to_attest_its_fence_after_a_burst()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut probe = BurstProbe::new(3, None);

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(outcome, Some(FollowingConvergence::Attested));
        assert_eq!(probe.secondary_sequence, 8);
        assert_eq!(probe.writer_sequence, 8);
        Ok(())
    }

    #[tokio::test]
    async fn a_diverged_writer_still_ends_following() {
        let mut probe = BurstProbe::new(0, Some(4));
        probe.writer_sequence_override = Some(0);

        let outcome = converge_without_deadline_exit(
            &mut probe,
            test_network(),
            short_convergence_policy(),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(ProjectorError::CanonicalFenceDiverged(
                zinder_runtime::CanonicalFenceDivergence::FenceMismatch { .. }
            ))
        ));
    }

    #[test]
    fn a_page_from_a_writer_past_the_secondary_authenticates_the_secondary_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let burst = BurstFixture::build(3, 3)?;
        let page = burst.writer_page(burst.writer.event_fence(), |_| {})?;

        let RetainedEventPageValidation::Events(events) = validate_one_retained_event_page(
            &page,
            burst.secondary_events()?,
            burst.wallet_source,
            burst.secondary.event_fence(),
        )?
        else {
            return Err("a page from an advanced writer asked to reconverge".into());
        };

        assert_eq!(page.events.len(), 6);
        assert_eq!(events.len(), 3);
        assert_eq!(
            events.last().map(|event| event.resulting_fence()),
            Some(burst.secondary.event_fence())
        );
        let NextWalletTransition::ApplyOne(next) =
            plan_next_wallet_transition(&burst.secondary, &events)?
        else {
            return Err("an append-only burst planned a reconciliation".into());
        };
        assert_eq!(next.committed_range().start, BlockHeight::new(2));
        Ok(())
    }

    #[test]
    fn a_page_from_a_writer_past_the_secondary_refuses_a_different_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let burst = BurstFixture::build(3, 3)?;
        let page = burst.writer_page(burst.writer.event_fence(), |events| {
            if let Some(fence) = events
                .first_mut()
                .and_then(|event| event.resulting_fence.as_mut())
            {
                fence.visible_tip_hash = vec![0xee; 32];
            }
        })?;

        assert!(matches!(
            validate_one_retained_event_page(
                &page,
                burst.secondary_events()?,
                burst.wallet_source,
                burst.secondary.event_fence(),
            ),
            Err(ProjectorError::CanonicalEventPageInvalid {
                reason: "writer and secondary retained-event bytes differ"
            })
        ));
        Ok(())
    }

    #[test]
    fn a_page_at_the_secondary_fence_must_hold_exactly_its_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let burst = BurstFixture::build(3, 3)?;
        let page = burst.writer_page(burst.secondary.event_fence(), |_| {})?;

        assert!(matches!(
            validate_one_retained_event_page(
                &page,
                burst.secondary_events()?,
                burst.wallet_source,
                burst.secondary.event_fence(),
            ),
            Err(ProjectorError::CanonicalEventPageInvalid {
                reason: "writer and secondary retained-event page lengths differ"
            })
        ));
        Ok(())
    }

    #[test]
    fn a_page_from_a_writer_behind_the_secondary_reconverges()
    -> Result<(), Box<dyn std::error::Error>> {
        let burst = BurstFixture::build(3, 3)?;
        let page = burst.writer_page(burst.baseline_fence, |_| {})?;

        assert!(matches!(
            validate_one_retained_event_page(
                &page,
                burst.secondary_events()?,
                burst.wallet_source,
                burst.secondary.event_fence(),
            )?,
            RetainedEventPageValidation::Reconverge
        ));
        Ok(())
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
            following_retention_lease_renewal_expiry(now, current_expiry, configured_duration),
            Some(lease_expiry(now, configured_duration))
        );
    }

    #[test]
    fn following_transition_renews_to_the_full_configured_window_before_mutation() {
        let now = UnixTimestampMillis::new(1_000_000);
        let configured_duration = Duration::from_hours(4);
        let current_expiry = lease_expiry(now, Duration::from_mins(2));

        assert_eq!(
            following_retention_lease_transition_expiry(now, current_expiry, configured_duration),
            Some(lease_expiry(now, configured_duration))
        );
    }

    fn short_convergence_policy() -> CanonicalFenceConvergencePolicy {
        CanonicalFenceConvergencePolicy {
            timeout: Duration::from_millis(30),
            max_attempts: None,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        }
    }

    fn scripted_writer_fence(event_sequence: u64) -> CanonicalWriterFence {
        let height = u32::try_from(event_sequence).unwrap_or(u32::MAX);
        zinder_proto::wire::canonical_writer_fence_message(
            ChainEpochId::new(event_sequence),
            event_sequence,
            block_id(height, 0x44),
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                event_sequence,
                [0x44; 32],
            ),
        )
    }

    /// A writer that commits once on each status read while its burst lasts,
    /// so the secondary it feeds trails it after every catch-up.
    struct BurstProbe {
        secondary_sequence: u64,
        writer_sequence: u64,
        burst_commits_remaining: u32,
        /// Event sequence of the open wallet; `None` for a fresh build.
        wallet_sequence: Option<u64>,
        writer_sequence_override: Option<u64>,
        /// Store damage every status reports.
        store_damage: Option<zinder_proto::v1::ops::CanonicalStoreDamagedDetail>,
        catch_ups: u32,
        deadlines: u32,
    }

    impl BurstProbe {
        fn new(burst_commits: u32, wallet_sequence: Option<u64>) -> Self {
            Self {
                secondary_sequence: 5,
                writer_sequence: 5,
                burst_commits_remaining: burst_commits,
                wallet_sequence,
                writer_sequence_override: None,
                store_damage: None,
                catch_ups: 0,
                deadlines: 0,
            }
        }
    }

    impl CanonicalFenceProbe for BurstProbe {
        type Error = ProjectorError;

        fn catch_up(
            &mut self,
        ) -> impl std::future::Future<Output = Result<Option<CanonicalWriterFence>, ProjectorError>> + Send
        {
            self.catch_ups += 1;
            self.secondary_sequence = self.writer_sequence;
            std::future::ready(Ok(Some(scripted_writer_fence(self.secondary_sequence))))
        }

        fn writer_status(
            &mut self,
        ) -> impl std::future::Future<
            Output = Result<CanonicalWriterStatusResponse, ProjectorError>,
        > + Send {
            if self.burst_commits_remaining > 0 {
                self.burst_commits_remaining -= 1;
                self.writer_sequence += 1;
            }
            let writer_sequence = self
                .writer_sequence_override
                .unwrap_or(self.writer_sequence);
            std::future::ready(Ok(CanonicalWriterStatusResponse {
                network_name: encode_zinder_native_chain_name(test_network()).to_owned(),
                fence: Some(scripted_writer_fence(writer_sequence)),
                oldest_retained_event_sequence: 1,
                canonical_store_damaged: self.store_damage.clone(),
                ..CanonicalWriterStatusResponse::default()
            }))
        }

        fn accepts_trailing(&self, _lag: CanonicalFenceLag) -> bool {
            secondary_holds_unapplied_events(self.secondary_sequence, self.wallet_sequence)
        }
    }

    impl FollowingFenceProbe for BurstProbe {
        fn deadline_passed(
            &mut self,
        ) -> impl std::future::Future<Output = Result<(), ProjectorError>> + Send {
            self.deadlines += 1;
            std::future::ready(Ok(()))
        }
    }

    /// A canonical writer that committed a burst of blocks after its
    /// secondary last caught up, with a wallet still at the baseline block.
    struct BurstFixture {
        _temporary: TempDir,
        writer: RocksDbCanonicalStore,
        secondary: RocksDbCanonicalSecondary,
        baseline_fence: CanonicalEventFence,
        wallet_source: WalletCanonicalSourceIdentity,
    }

    impl BurstFixture {
        /// Appends `caught_up_blocks` blocks before the secondary catches up
        /// and `burst_blocks` more after it.
        fn build(
            caught_up_blocks: u32,
            burst_blocks: u32,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let activations = zinder_testkit::sample_regtest_upgrade_activations();
            let canonical_path = temporary.path().join("canonical");
            // Wide enough to keep the baseline block settled across the burst.
            let reorg_policy = CanonicalReorgPolicy::new(16)?;
            let (mut writer, block_one_id) = baseline_writer(&canonical_path, reorg_policy)?;
            let baseline_fence = writer.event_fence();
            let mut next_height = 2;
            writer = append_empty_blocks(writer, &mut next_height, caught_up_blocks, block_one_id)?;
            let secondary = RocksDbCanonicalSecondary::open_ready(
                &canonical_path,
                temporary.path().join("canonical-secondary"),
                &activations,
                zinder_store::CanonicalStoreRequirements::for_blob_retention(
                    RawBlobRetention::None,
                ),
                reorg_policy,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            writer = append_empty_blocks(writer, &mut next_height, burst_blocks, block_one_id)?;
            Ok(Self {
                _temporary: temporary,
                writer,
                secondary,
                baseline_fence,
                wallet_source: wallet_source_identity_from_fence(baseline_fence, block_one_id),
            })
        }

        fn secondary_events(
            &self,
        ) -> Result<Vec<CanonicalRetainedEvent>, Box<dyn std::error::Error>> {
            let cursor = self.wallet_source.source_position().event_cursor.as_bytes();
            Ok(self
                .secondary
                .canonical_event_history(CanonicalEventHistoryRequest::new(
                    Some(cursor.as_slice()),
                    NonZeroU32::new(CANONICAL_CONTROL_EVENT_PAGE_EVENTS)
                        .ok_or("zero event page limit")?,
                ))?)
        }

        /// Returns the writer's page from the wallet cursor, bounded by
        /// `writer_fence`, after `tamper` edits its events.
        fn writer_page(
            &self,
            writer_fence: CanonicalEventFence,
            tamper: impl FnOnce(&mut Vec<zinder_proto::v1::ingest::CanonicalRetainedEvent>),
        ) -> Result<CanonicalEventPageResponse, Box<dyn std::error::Error>> {
            let cursor = self.wallet_source.source_position().event_cursor.as_bytes();
            let mut events = self
                .writer
                .canonical_event_history(CanonicalEventHistoryRequest::new(
                    Some(cursor.as_slice()),
                    NonZeroU32::new(CANONICAL_CONTROL_EVENT_PAGE_EVENTS)
                        .ok_or("zero event page limit")?,
                ))?
                .into_iter()
                .map(writer_retained_event)
                .collect();
            tamper(&mut events);
            Ok(CanonicalEventPageResponse {
                events,
                writer_fence: Some(expected_writer_fence(writer_fence)),
                oldest_retained_event_sequence: self.writer.canonical_event_retention_floor()?,
            })
        }
    }

    /// Publishes a Regtest canonical writer whose only block is an empty
    /// block one, and returns it with that block's identity.
    pub(super) fn baseline_writer(
        canonical_path: &Path,
        reorg_policy: CanonicalReorgPolicy,
    ) -> Result<(RocksDbCanonicalStore, BlockId), Box<dyn std::error::Error>> {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let block_one = empty_canonical_block(
            1,
            Network::ZcashRegtest.genesis_hash().as_bytes(),
            [0x11; 32],
        )?;
        let block_one_id = BlockId::new(
            block_one.facts.block_header.height,
            block_one.facts.block_header.block_hash,
        );
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            canonical_path,
            CanonicalStoreBuildPlan::complete(
                &activations,
                0,
                block_one_id,
                RawBlobRetention::None,
                reorg_policy,
            )?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(&activations, [Ok::<_, std::convert::Infallible>(block_one)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            block_one_id,
            1,
            regtest_checkpoint_frontiers_at(BlockHeight::new(1))?,
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            block_one_id,
            UnixTimestampMillis::new(1),
        ))?;
        Ok((validated.publish_baseline(publication)?, block_one_id))
    }

    fn append_empty_blocks(
        mut writer: RocksDbCanonicalStore,
        next_height: &mut u32,
        count: u32,
        settled_tip: BlockId,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        for _ in 0..count {
            let height = *next_height;
            let parent_byte = u8::try_from(0x10 + height - 1)?;
            let block_byte = u8::try_from(0x10 + height)?;
            let parent_hash = if height == 2 {
                [0x11; 32]
            } else {
                [parent_byte; 32]
            };
            let fence = writer.event_fence();
            let (next, _) = writer.commit_live_append(
                CanonicalLiveAppend::new(
                    fence,
                    empty_canonical_block(height, parent_hash, [block_byte; 32])?,
                    Vec::new(),
                    settled_tip,
                    UnixTimestampMillis::new(u64::from(height)),
                ),
                &activations,
            )?;
            writer = next;
            *next_height += 1;
        }
        Ok(writer)
    }

    /// Encodes a retained event the way the writer's `EventPage` does.
    pub(super) fn writer_retained_event(
        event: CanonicalRetainedEvent,
    ) -> zinder_proto::v1::ingest::CanonicalRetainedEvent {
        let range = |range: BlockHeightRange| CanonicalEventBlockRange {
            start_height: range.start.value(),
            end_height: range.end.value(),
        };
        zinder_proto::v1::ingest::CanonicalRetainedEvent {
            cursor: event.cursor().as_bytes().to_vec(),
            resulting_epoch_id: event.resulting_epoch_id().value(),
            previous_epoch_id: event.previous_epoch_id().map(ChainEpochId::value),
            kind: match event.kind() {
                CanonicalEventKind::Committed => CanonicalRetainedEventKind::Committed as i32,
                CanonicalEventKind::Reorged => CanonicalRetainedEventKind::Reorged as i32,
            },
            reverted_range: event.reverted_range().map(range),
            committed_range: Some(range(event.committed_range())),
            resulting_fence: Some(expected_writer_fence(event.resulting_fence())),
        }
    }

    pub(super) fn empty_canonical_block(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
    ) -> Result<CanonicalBuildBlock, zinder_core::CommitmentTreeFrontiersError> {
        let height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes(block_hash);
        let parent_hash = BlockHash::from_bytes(parent_hash);
        let (transaction, _transaction_blob) = empty_coinbase(height, block_hash);
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
        Ok(CanonicalBuildBlock {
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
                regtest_checkpoint_frontiers_at(height)?,
            )),
            transaction_blobs: Vec::new(),
            block_blob: None,
        })
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

    fn regtest_checkpoint_frontiers_at(
        height: BlockHeight,
    ) -> Result<CommitmentTreeFrontiers, zinder_core::CommitmentTreeFrontiersError> {
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
            network_upgrade_schedule: None,
            reorg_window_exceeded: None,
            tip_unconfirmed: false,
            canonical_store_damaged: None,
        }
    }

    fn retention_lease(source: WalletCanonicalSourceIdentity) -> CanonicalRetentionLease {
        let position = source.source_position();
        CanonicalRetentionLease::new(
            [0x55; 16],
            position.chain_epoch_id.value(),
            position.event_cursor.as_bytes().to_vec(),
            UnixTimestampMillis::new(1_000),
        )
    }

    fn test_network() -> Network {
        Network::ZcashRegtest
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

    fn publish_test_canonical_store(
        canonical_path: &Path,
        reorg_policy: CanonicalReorgPolicy,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let block_one =
            empty_canonical_block(1, test_network().genesis_hash().as_bytes(), [0x11; 32])?;
        let block_one_id = BlockId::new(
            block_one.facts.block_header.height,
            block_one.facts.block_header.block_hash,
        );
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            block_one_id,
            RawBlobRetention::None,
            reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            canonical_path,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(&activations, [Ok::<_, std::convert::Infallible>(block_one)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            block_one_id,
            1,
            regtest_checkpoint_frontiers_at(BlockHeight::new(1))?,
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            block_one_id,
            UnixTimestampMillis::new(1),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn test_projector_config(
        canonical_path: PathBuf,
        canonical_secondary_path: PathBuf,
        wallet_path: PathBuf,
    ) -> Result<super::config::ProjectorConfig, Box<dyn std::error::Error>> {
        Ok(super::config::load_projector_config_with_env(
            None,
            super::config::ProjectorConfigOverrides {
                network: Some("zcash-regtest".to_owned()),
                canonical_path: Some(canonical_path),
                canonical_secondary_path: Some(canonical_secondary_path),
                wallet_path: Some(wallet_path),
                reorg_window_blocks: Some(3),
                build_owner_hex: Some("00112233445566778899aabbccddeeff".to_owned()),
                lease_duration_seconds: Some(14_400),
                ..Default::default()
            },
            std::iter::empty(),
        )?)
    }

    #[test]
    fn open_canonical_secondary_creates_missing_nested_parent_directories()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let canonical_path = temporary.path().join("canonical");
        let reorg_policy = CanonicalReorgPolicy::new(3)?;
        let _canonical = publish_test_canonical_store(&canonical_path, reorg_policy)?;
        let secondary_path = temporary.path().join("missing/nested/canonical-secondary");
        let cfg = test_projector_config(
            canonical_path,
            secondary_path.clone(),
            temporary.path().join("wallet"),
        )?;

        let opened =
            super::open_canonical_secondary(&cfg, reorg_policy, &CancellationToken::new())?;
        assert!(opened.is_some());
        assert!(secondary_path.is_dir());
        Ok(())
    }

    #[test]
    fn open_canonical_secondary_reports_exact_paths_on_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let reorg_policy = CanonicalReorgPolicy::new(3)?;
        let blocker = temporary.path().join("blocker");
        std::fs::write(&blocker, b"not-a-directory")?;
        let blocked_secondary = blocker.join("canonical-secondary");
        let blocked_cfg = test_projector_config(
            temporary.path().join("canonical"),
            blocked_secondary.clone(),
            temporary.path().join("wallet"),
        )?;
        let Err(blocked_err) =
            super::open_canonical_secondary(&blocked_cfg, reorg_policy, &CancellationToken::new())
        else {
            return Err("blocked secondary path should fail".into());
        };
        assert!(matches!(
            blocked_err,
            ProjectorError::CanonicalSecondaryDirectoryCreate { .. }
        ));
        assert!(
            blocked_err
                .to_string()
                .contains(&blocked_secondary.display().to_string()),
            "{blocked_err}"
        );

        let missing_primary = temporary.path().join("missing/canonical");
        let created_secondary = temporary.path().join("created/nested/canonical-secondary");
        let missing_cfg = test_projector_config(
            missing_primary.clone(),
            created_secondary.clone(),
            temporary.path().join("wallet-missing"),
        )?;
        let Err(open_err) =
            super::open_canonical_secondary(&missing_cfg, reorg_policy, &CancellationToken::new())
        else {
            return Err("missing primary path should fail".into());
        };
        assert!(matches!(
            open_err,
            ProjectorError::CanonicalSecondaryOpen { .. }
        ));
        let message = open_err.to_string();
        assert!(
            message.contains(&created_secondary.display().to_string()),
            "{message}"
        );
        assert!(
            message.contains(&missing_primary.display().to_string()),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn full_blocks_profiles_reject_canonical_store_without_block_blobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let canonical_path = temporary.path().join("canonical");
        let reorg_policy = CanonicalReorgPolicy::new(3)?;
        let _canonical = publish_test_canonical_store(&canonical_path, reorg_policy)?;

        for profile in ["wallet", "explorer"] {
            let secondary_path = temporary.path().join(format!("secondary-{profile}"));
            let wallet_path = temporary.path().join(format!("wallet-{profile}"));
            let cfg = super::config::load_projector_config_with_env(
                None,
                super::config::ProjectorConfigOverrides {
                    network: Some("zcash-regtest".to_owned()),
                    canonical_path: Some(canonical_path.clone()),
                    canonical_secondary_path: Some(secondary_path),
                    wallet_path: Some(wallet_path),
                    reorg_window_blocks: Some(3),
                    build_owner_hex: Some("00112233445566778899aabbccddeeff".to_owned()),
                    lease_duration_seconds: Some(14_400),
                    ..Default::default()
                },
                [
                    ("ZINDER_DEPLOYMENT__PROFILE".to_owned(), profile.to_owned()),
                    ("ZINDER_STORAGE__FULL_BLOCKS".to_owned(), "true".to_owned()),
                ],
            )?;
            assert_eq!(
                cfg.expected_canonical_raw_blob_retention,
                zinder_store::RawBlobRetention::All
            );
            let Err(open_err) =
                super::open_canonical_secondary(&cfg, reorg_policy, &CancellationToken::new())
            else {
                return Err(format!(
                    "profile {profile} with full_blocks = true must reject store without block blobs"
                )
                .into());
            };
            assert!(matches!(
                open_err,
                ProjectorError::CanonicalSecondaryOpen { .. }
            ));
        }
        Ok(())
    }
}
