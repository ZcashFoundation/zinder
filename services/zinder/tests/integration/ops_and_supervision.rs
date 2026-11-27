//! Integration tests for T=0 operational endpoints, `zinder healthcheck`, and process supervision.

use std::{
    net::{Ipv4Addr, SocketAddr},
    process::{Command, Stdio},
    time::Duration,
};

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zinder::{
    cli::ServiceRole,
    ops::{RoleOpsSnapshot, SharedOpsState, StartupStage, spawn_supervisor_ops},
};

fn allocate_free_port() -> eyre::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    Ok(listener.local_addr()?)
}

fn clean_zinder_command(args: &[&str]) -> Command {
    let mut cmd = Command::new(zinder_testkit::bin_exe!("zinder"));
    cmd.args(args);
    for (env_name, _) in std::env::vars_os() {
        if let Some(key) = env_name.to_str()
            && key.starts_with("ZINDER_")
        {
            cmd.env_remove(&env_name);
        }
    }
    cmd
}

async fn http_get(addr: SocketAddr, path: &str) -> eyre::Result<(u16, String)> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status_line = text.lines().next().unwrap_or_default();
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|digits| digits.parse::<u16>().ok())
        .ok_or_else(|| eyre::eyre!("malformed HTTP status line: {status_line}"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map_or("", |(_, b)| b)
        .to_owned();
    Ok((code, body))
}

async fn wait_for_healthz(addr: SocketAddr, child: &mut std::process::Child) -> eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait()? {
            eyre::bail!("supervised process exited early during startup ({status})");
        }
        if let Ok((200, _)) = http_get(addr, "/healthz").await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!("timed out waiting for /healthz at {addr}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t0_ops_and_healthcheck_track_readiness_transitions() -> eyre::Result<()> {
    let ops_addr = allocate_free_port()?;
    let ops_str = ops_addr.to_string();
    let shared = SharedOpsState::new(
        "serve",
        "wallet",
        "zcash-regtest",
        vec![
            RoleOpsSnapshot::starting(ServiceRole::Ingest, true, None),
            RoleOpsSnapshot::starting(ServiceRole::Projector, true, None),
        ],
    );
    let handle = spawn_supervisor_ops(ops_addr, shared.clone()).await?;

    let healthz_probe = clean_zinder_command(&["healthcheck", "--addr", &ops_str]).output()?;
    assert!(
        healthz_probe.status.success(),
        "healthcheck failed: {}",
        String::from_utf8_lossy(&healthz_probe.stderr)
    );

    let readyz_early =
        clean_zinder_command(&["healthcheck", "--ready", "--addr", &ops_str]).output()?;
    assert!(!readyz_early.status.success());

    let (early_code, early_body) = http_get(ops_addr, "/readyz").await?;
    assert_eq!(early_code, 503);
    assert!(
        early_body.contains("bootstrapping_canonical"),
        "{early_body}"
    );

    let mut ready_ingest = RoleOpsSnapshot::starting(ServiceRole::Ingest, true, None);
    ready_ingest.ready = true;
    ready_ingest.state = "ready".to_owned();
    let mut ready_projector = RoleOpsSnapshot::starting(ServiceRole::Projector, true, None);
    ready_projector.ready = true;
    ready_projector.state = "ready".to_owned();
    shared.update(StartupStage::Ready, vec![ready_ingest, ready_projector]);

    let readyz_ok =
        clean_zinder_command(&["healthcheck", "--ready", "--addr", &ops_str]).output()?;
    assert!(readyz_ok.status.success());

    let (metrics_code, metrics_body) = http_get(ops_addr, "/metrics").await?;
    assert_eq!(metrics_code, 200);
    assert!(metrics_body.contains("zinder_build_info"), "{metrics_body}");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn supervised_serve_exposes_t0_ops_and_drains_cleanly_on_sigterm() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let data_dir = sandbox.path().join("zinder-data");
    let ops_addr = allocate_free_port()?;
    let ingest_ctrl_addr = allocate_free_port()?;
    let compat_addr = allocate_free_port()?;
    let unreachable_node = allocate_free_port()?;
    let cfg_path = sandbox.path().join("serve.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "[network]\nname = \"zcash-regtest\"\n\n\
             [ops]\nlisten_addr = \"{ops_addr}\"\n\n\
             [node]\njson_rpc_addr = \"http://{unreachable_node}\"\n\n\
             [ingest_control]\nlisten_addr = \"{ingest_ctrl_addr}\"\n\
             addr = \"http://{ingest_ctrl_addr}\"\n\n\
             [compat]\nlisten_addr = \"{compat_addr}\"\n"
        ),
    )?;

    let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;
    let data_str = data_dir.to_str().ok_or_else(|| eyre::eyre!("path"))?;
    let mut child = clean_zinder_command(&[
        "serve",
        "--preset",
        "light-wallet",
        "--data-dir",
        data_str,
        "--config",
        cfg_str,
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()?;

    let outcome = super_verify_t0_and_sigterm(ops_addr, &mut child).await;
    if outcome.is_err() {
        let _ = child.kill();
    }
    outcome
}

async fn super_verify_t0_and_sigterm(
    ops_addr: SocketAddr,
    child: &mut std::process::Child,
) -> eyre::Result<()> {
    wait_for_healthz(ops_addr, child).await?;
    let (ready_code, ready_body) = http_get(ops_addr, "/readyz").await?;
    assert_eq!(
        ready_code, 503,
        "expected 503 before node reachable: {ready_body}"
    );
    assert!(
        ready_body.contains("bootstrapping_canonical"),
        "expected bootstrapping stage JSON: {ready_body}"
    );
    let (metrics_code, metrics_body) = http_get(ops_addr, "/metrics").await?;
    assert_eq!(metrics_code, 200);
    assert!(
        metrics_body.contains("zinder_supervisor_ready"),
        "{metrics_body}"
    );

    #[cfg(unix)]
    if let Ok(raw_pid) = i32::try_from(child.id())
        && let Some(pid) = rustix::process::Pid::from_raw(raw_pid)
    {
        rustix::process::kill_process(pid, rustix::process::Signal::TERM)?;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait()? {
            assert!(status.success(), "expected clean shutdown, got {status}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!("timed out waiting for supervised zinder serve to drain on SIGTERM");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn supervised_serve_fails_fast_when_child_role_exits_early() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let ops_addr = allocate_free_port()?;
    let missing_canon = sandbox.path().join("missing-canonical-primary");
    let sec_root = sandbox.path().join("secondary-root");
    let unreachable_ctrl = allocate_free_port()?;
    let compat_addr = allocate_free_port()?;
    let cfg_path = sandbox.path().join("readers-only.toml");
    let canon_display = missing_canon.display();
    let sec_display = sec_root.display();
    std::fs::write(
        &cfg_path,
        format!(
            "[network]\nname = \"zcash-regtest\"\n\n\
             [ops]\nlisten_addr = \"{ops_addr}\"\n\n\
             [storage]\npath = \"{canon_display}\"\nsecondary_path = \"{sec_display}\"\ninitial_catchup_timeout_ms = 200\n\n\
             [ingest_control]\naddr = \"http://{unreachable_ctrl}\"\n\n\
             [compat]\nlisten_addr = \"{compat_addr}\"\n"
        ),
    )?;

    let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;
    let mut child = clean_zinder_command(&[
        "serve",
        "--preset",
        "light-wallet",
        "--readers-only",
        "--config",
        cfg_str,
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait()? {
            assert!(
                !status.success(),
                "supervisor must fail non-zero when child fails admission"
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = child.kill();
            eyre::bail!("supervisor failed to propagate child failure before timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
