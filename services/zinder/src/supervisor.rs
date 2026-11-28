//! Rust-native subprocess supervision for `zinder serve` and `zinder index`.

use std::{
    collections::HashSet,
    fs::OpenOptions,
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, Instant},
};

use tempfile::TempDir;
use thiserror::Error;
use tokio::{net::TcpStream, process::Command};
use tokio_util::sync::CancellationToken;
use zinder_runtime::cancel_on_terminating_signal;

use crate::{
    cli::ServiceRole,
    config::{RoleLaunchSpec, SupervisionPlan},
    ops::{
        RoleOpsSnapshot, SharedOpsState, StartupStage, SupervisorOpsError, SupervisorOpsHandle,
        fetch_child_capabilities, fetch_child_readyz, fetch_child_served_chain,
        spawn_supervisor_ops,
    },
};

const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const STEADY_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(20);

/// Error returned when supervising preset roles fails.
#[derive(Debug, Error)]
pub enum SupervisorError {
    /// Failed to bind or serve the T=0 operational HTTP endpoint.
    #[error(transparent)]
    Ops(#[from] SupervisorOpsError),
    /// Failed to prepare private temporary runtime config files or reserve sockets.
    #[error("supervisor setup failed: {0}")]
    Setup(#[from] std::io::Error),
    /// Failed to resolve or spawn the `zinder` binary for a child role.
    #[error("failed to launch child role `{role}`: {source}")]
    Spawn {
        /// Role label that failed to launch.
        role: &'static str,
        /// Underlying OS spawn error.
        #[source]
        source: std::io::Error,
    },
    /// A supervised child process exited unexpectedly.
    #[error("child role `{role}` exited unexpectedly ({status})")]
    ChildExited {
        /// Role label that exited.
        role: &'static str,
        /// OS exit status reported by `wait`/`try_wait`.
        status: ExitStatus,
    },
    /// Waiting for a child process failed due to an OS wait error.
    #[error("child role `{role}` status check failed: {source}")]
    ChildWait {
        /// Role label whose wait returned an error.
        role: &'static str,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

struct ManagedChild {
    spec: RoleLaunchSpec,
    config_file: PathBuf,
    ops_addr: SocketAddr,
    process: Option<tokio::process::Child>,
    snapshot: RoleOpsSnapshot,
}

enum StartupGate {
    None,
    CanonicalPrimaryAndControl {
        canonical_path: PathBuf,
        control_addr: SocketAddr,
    },
    WalletPrimary {
        wallet_path: PathBuf,
    },
    TcpEndpoint {
        addr: SocketAddr,
    },
}

/// Runs the supervisor lifecycle: T=0 ops endpoint, phased child startup, readiness polling, and bounded shutdown.
pub async fn run_supervision(plan: SupervisionPlan) -> Result<(), SupervisorError> {
    let runtime_dir = TempDir::new()?;
    let ops_ports = reserve_loopback_ports(plan.roles.len())?;
    let initial_snaps: Vec<RoleOpsSnapshot> = plan
        .roles
        .iter()
        .zip(&ops_ports)
        .map(|(spec, port)| {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port);
            RoleOpsSnapshot::starting(spec.role, spec.required_for_readyz, Some(addr))
        })
        .collect();
    let shared_ops = SharedOpsState::new(
        plan.mode_label(),
        plan.profile.as_str(),
        zinder_core::wire::encode_zinder_native_chain_name(plan.network),
        initial_snaps,
    );
    let ops_handle = bind_t0_ops(plan.ops_listen_addr, shared_ops.clone()).await?;
    let cancel = CancellationToken::new();
    let _signal_guard = cancel_on_terminating_signal(cancel.clone());

    let mut managed = prepare_managed_children(&plan, runtime_dir.path(), &ops_ports)?;
    let exe_path = std::env::current_exe()?;
    let run_outcome =
        supervise_lifecycle(&exe_path, &plan, &mut managed, &shared_ops, &cancel).await;

    shared_ops.set_stage(if run_outcome.is_ok() {
        StartupStage::ShuttingDown
    } else {
        StartupStage::Failed
    });
    shutdown_children(&mut managed).await;
    if let Some(handle) = ops_handle {
        let _ = handle.shutdown().await;
    }
    run_outcome
}

impl SupervisionPlan {
    const fn mode_label(&self) -> &'static str {
        match self.mode {
            crate::config::SupervisorMode::Serve => "serve",
            crate::config::SupervisorMode::Index => "index",
        }
    }
}

async fn bind_t0_ops(
    listen_addr: Option<SocketAddr>,
    shared_ops: SharedOpsState,
) -> Result<Option<SupervisorOpsHandle>, SupervisorError> {
    match listen_addr {
        Some(addr) => Ok(Some(spawn_supervisor_ops(addr, shared_ops).await?)),
        None => Ok(None),
    }
}

fn prepare_managed_children(
    plan: &SupervisionPlan,
    dir: &Path,
    ops_ports: &[u16],
) -> Result<Vec<ManagedChild>, SupervisorError> {
    let mut children = Vec::with_capacity(plan.roles.len());
    for (spec, port) in plan.roles.iter().zip(ops_ports) {
        let config_file = dir.join(format!("{}.toml", spec.role.label()));
        write_private_config(&config_file, &spec.non_secret_toml)?;
        let ops_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port);
        let snapshot =
            RoleOpsSnapshot::starting(spec.role, spec.required_for_readyz, Some(ops_addr));
        children.push(ManagedChild {
            spec: spec.clone(),
            config_file,
            ops_addr,
            process: None,
            snapshot,
        });
    }
    Ok(children)
}

fn write_private_config(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents.as_bytes())
}

fn reserve_loopback_ports(count: usize) -> std::io::Result<Vec<u16>> {
    let mut ports = Vec::with_capacity(count);
    let mut used = HashSet::with_capacity(count);
    while ports.len() < count {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let port = listener.local_addr()?.port();
        if used.insert(port) {
            ports.push(port);
        }
    }
    Ok(ports)
}

async fn supervise_lifecycle(
    exe_path: &Path,
    plan: &SupervisionPlan,
    managed: &mut [ManagedChild],
    shared_ops: &SharedOpsState,
    cancel: &CancellationToken,
) -> Result<(), SupervisorError> {
    for idx in 0..managed.len() {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let role = managed[idx].spec.role;
        let child = spawn_role_process(exe_path, &managed[idx])?;
        managed[idx].process = Some(child);
        tracing::info!(
            target: "zinder::supervisor",
            role = role.label(),
            ops_addr = %managed[idx].ops_addr,
            "spawned supervised role"
        );
        let gate = startup_gate_after(role, plan);
        wait_for_gate(managed, &gate, shared_ops, cancel).await?;
    }

    loop {
        poll_children_and_publish(managed, shared_ops).await?;
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            () = tokio::time::sleep(STEADY_POLL_INTERVAL) => {}
        }
    }
}

fn startup_gate_after(role: ServiceRole, plan: &SupervisionPlan) -> StartupGate {
    match role {
        ServiceRole::Ingest => plan
            .ingest_control_addr
            .map_or(StartupGate::None, |control_addr| {
                StartupGate::CanonicalPrimaryAndControl {
                    canonical_path: plan.canonical_path.clone(),
                    control_addr: loopback_connect_addr(control_addr),
                }
            }),
        ServiceRole::Projector => {
            plan.wallet_path
                .as_ref()
                .map_or(StartupGate::None, |wallet_path| {
                    StartupGate::WalletPrimary {
                        wallet_path: wallet_path.clone(),
                    }
                })
        }
        ServiceRole::Query => {
            plan.query_listen_addr
                .map_or(StartupGate::None, |addr| StartupGate::TcpEndpoint {
                    addr: loopback_connect_addr(addr),
                })
        }
        ServiceRole::Explorer => {
            plan.explorer_listen_addr
                .map_or(StartupGate::None, |addr| StartupGate::TcpEndpoint {
                    addr: loopback_connect_addr(addr),
                })
        }
        ServiceRole::CompatLightwalletd | ServiceRole::CompatCipherscan => StartupGate::None,
    }
}

async fn wait_for_gate(
    managed: &mut [ManagedChild],
    gate: &StartupGate,
    shared_ops: &SharedOpsState,
    cancel: &CancellationToken,
) -> Result<(), SupervisorError> {
    while !cancel.is_cancelled() {
        poll_children_and_publish(managed, shared_ops).await?;
        if gate_satisfied(gate).await {
            return Ok(());
        }
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            () = tokio::time::sleep(STARTUP_POLL_INTERVAL) => {}
        }
    }
    Ok(())
}

async fn gate_satisfied(gate: &StartupGate) -> bool {
    match gate {
        StartupGate::None => true,
        StartupGate::CanonicalPrimaryAndControl {
            canonical_path,
            control_addr,
        } => {
            canonical_path.join("CURRENT").exists()
                && TcpStream::connect(*control_addr).await.is_ok()
        }
        StartupGate::WalletPrimary { wallet_path } => wallet_path.join("CURRENT").exists(),
        StartupGate::TcpEndpoint { addr } => TcpStream::connect(*addr).await.is_ok(),
    }
}

fn spawn_role_process(
    exe_path: &Path,
    entry: &ManagedChild,
) -> Result<tokio::process::Child, SupervisorError> {
    let mut cmd = Command::new(exe_path);
    cmd.arg("run")
        .arg(entry.spec.role.label())
        .arg("--config")
        .arg(&entry.config_file);
    for (env_name, _) in std::env::vars_os() {
        if let Some(as_str) = env_name.to_str()
            && as_str.starts_with("ZINDER_")
        {
            cmd.env_remove(&env_name);
        }
    }
    for (key, secret_val) in &entry.spec.secret_env {
        cmd.env(key, secrecy::ExposeSecret::expose_secret(secret_val));
    }
    cmd.env("ZINDER_OPS__LISTEN_ADDR", entry.ops_addr.to_string())
        .stdin(Stdio::null())
        .kill_on_drop(true);

    cmd.spawn().map_err(|source| SupervisorError::Spawn {
        role: entry.spec.role.label(),
        source,
    })
}

async fn poll_children_and_publish(
    managed: &mut [ManagedChild],
    shared_ops: &SharedOpsState,
) -> Result<(), SupervisorError> {
    let mut exited_role: Option<(&'static str, ExitStatus)> = None;
    for entry in managed.iter_mut() {
        let Some(child) = entry.process.as_mut() else {
            continue;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                "exited".clone_into(&mut entry.snapshot.state);
                entry.snapshot.ready = false;
                entry.snapshot.served_chain = None;
                entry.snapshot.detail = Some(format!("exited with {status}"));
                exited_role = Some((entry.spec.role.label(), status));
                break;
            }
            Ok(None) => {}
            Err(source) => {
                return Err(SupervisorError::ChildWait {
                    role: entry.spec.role.label(),
                    source,
                });
            }
        }
        refresh_child_snapshot(entry).await;
    }

    if let Some((role, status)) = exited_role {
        let snapshots = managed.iter().map(|entry| entry.snapshot.clone()).collect();
        shared_ops.update(StartupStage::Failed, snapshots);
        return Err(SupervisorError::ChildExited { role, status });
    }

    let stage = compute_startup_stage(managed);
    let snapshots = managed.iter().map(|entry| entry.snapshot.clone()).collect();
    shared_ops.update(stage, snapshots);
    Ok(())
}

async fn refresh_child_snapshot(entry: &mut ManagedChild) {
    let serves_wallet_chain = matches!(
        entry.spec.role,
        ServiceRole::Query | ServiceRole::CompatLightwalletd
    );
    let (readyz, served_chain) = tokio::join!(fetch_child_readyz(entry.ops_addr), async {
        if serves_wallet_chain {
            fetch_child_served_chain(entry.ops_addr).await
        } else {
            None
        }
    });
    entry.snapshot.served_chain = served_chain;
    if let Some((is_ready, payload)) = readyz {
        if !entry.snapshot.capabilities_loaded
            && let Some(capabilities) = fetch_child_capabilities(entry.ops_addr).await
        {
            entry.snapshot.capabilities = capabilities;
            entry.snapshot.capabilities_loaded = true;
        }
        entry.snapshot.ready = is_ready;
        entry.snapshot.state = if is_ready {
            "ready".to_owned()
        } else {
            "not_ready".to_owned()
        };
        entry.snapshot.cause = payload.cause;
        entry.snapshot.phase = payload.phase;
        entry.snapshot.current_height = payload.current_height;
        entry.snapshot.target_height = payload.target_height;
        entry.snapshot.detail = None;
    } else {
        entry.snapshot.ready = false;
        "starting".clone_into(&mut entry.snapshot.state);
    }
}

fn compute_startup_stage(managed: &[ManagedChild]) -> StartupStage {
    let role_ready = |kind: ServiceRole| -> bool {
        managed
            .iter()
            .find(|entry| entry.spec.role == kind)
            .is_none_or(|entry| entry.snapshot.ready)
    };
    if !role_ready(ServiceRole::Ingest) {
        return StartupStage::BootstrappingCanonical;
    }
    if !role_ready(ServiceRole::Projector) {
        return StartupStage::CatchingUpWalletProjection;
    }
    if !role_ready(ServiceRole::Explorer) {
        return StartupStage::WaitingForExplorerViews;
    }
    if managed
        .iter()
        .any(|entry| entry.spec.required_for_readyz && !entry.snapshot.ready)
    {
        return StartupStage::WaitingForServingListeners;
    }
    StartupStage::Ready
}

async fn shutdown_children(managed: &mut [ManagedChild]) {
    for entry in managed.iter_mut().rev() {
        if let Some(child) = entry.process.as_mut() {
            signal_terminate_child(child);
        }
    }
    let deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    for entry in managed.iter_mut().rev() {
        let Some(mut child) = entry.process.take() else {
            continue;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || tokio::time::timeout(remaining, child.wait()).await.is_err() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        "stopped".clone_into(&mut entry.snapshot.state);
        entry.snapshot.ready = false;
    }
}

fn signal_terminate_child(child: &tokio::process::Child) {
    #[cfg(unix)]
    if let Some(raw_pid) = child.id()
        && let Ok(raw_i32) = i32::try_from(raw_pid)
        && let Some(pid) = rustix::process::Pid::from_raw(raw_i32)
    {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        return;
    }
    let _ = child.id();
}

const fn loopback_connect_addr(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        IpAddr::V4(_) | IpAddr::V6(_) => addr,
    }
}
