#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    fs,
    net::SocketAddr,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use tempfile::tempdir;
use tokio::process::Command as TokioCommand;
use tonic::transport::{Channel, Endpoint};
use zinder_proto::{
    capabilities::{
        EXPLORER_BLOCK_SUMMARY_V1, EXPLORER_OVERVIEW_SNAPSHOT_V1, EXPLORER_TRANSACTION_DETAIL_V4,
        EXPLORER_TRANSACTION_FEES_V1,
    },
    v1::explorer::{ServerInfoRequest, explorer_query_client::ExplorerQueryClient},
};

#[test]
fn print_config_accepts_explorer_bearer_token_path() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("materialized-view-print-config-store");
    let secondary_path = tempdir
        .path()
        .join("materialized-view-print-config-secondary");
    let token_path = tempdir.path().join("materialized-view-explorer.token");
    fs::write(&token_path, "expected-token\n")?;

    let output = zinder_explorer_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--storage-path",
            path_str(&storage_path)?,
            "--secondary-path",
            path_str(&secondary_path)?,
            "--listen-addr",
            "127.0.0.1:9068",
            "--bearer-token-path",
            path_str(&token_path)?,
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stdout.contains("[explorer]"), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "bearer_token_path = \"{}\"",
            path_str(&token_path)?
        )),
        "{stdout}"
    );
    assert!(!stdout.contains("expected-token"), "{stdout}");
    assert!(!stderr.contains("expected-token"), "{stderr}");

    Ok(())
}

#[test]
fn wallet_serving_explorer_image_config_resolves() -> eyre::Result<()> {
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/config/wallet-serving/explorer.toml");

    let output = zinder_explorer_command()
        .args(["--config", path_str(&config_path)?, "--print-config"])
        .env("ZINDER_NETWORK__NAME", "zcash-testnet")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    for expected in [
        "path = \"/var/lib/zinder/canonical\"",
        "secondary_path = \"/var/lib/zinder/explorer/secondary\"",
        "listen_addr = \"[::]:9068\"",
        "wallet_query_endpoint = \"http://127.0.0.1:9102\"",
        "listen_addr = \"[::]:9069\"",
        "allow_public_bind = true",
    ] {
        assert!(stdout.contains(expected), "missing {expected}: {stdout}");
    }

    Ok(())
}

#[test]
fn invalid_explorer_bearer_token_path_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("materialized-view-invalid-token-store");
    let secondary_path = tempdir
        .path()
        .join("materialized-view-invalid-token-secondary");
    let token_path = tempdir.path().join("materialized-view-empty.token");
    fs::write(&token_path, "\n")?;

    let output = zinder_explorer_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--storage-path",
            path_str(&storage_path)?,
            "--secondary-path",
            path_str(&secondary_path)?,
            "--listen-addr",
            "127.0.0.1:9068",
            "--bearer-token-path",
            path_str(&token_path)?,
        ])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("bearer token must not be empty"),
        "{stderr}"
    );

    Ok(())
}

#[tokio::test]
async fn runtime_starts_without_materialized_view_store_and_omits_materialized_view_capabilities()
-> eyre::Result<()> {
    // The explorer never opens a canonical primary; `storage.path` only locates
    // the materialized-view primary, which this empty directory lacks.
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("store");
    fs::create_dir(&storage_path)?;
    let secondary_path = tempdir.path().join("explorer-secondary");
    let config_path = tempdir.path().join("zinder-explorer.toml");
    let listen_addr = unused_loopback_addr()?;
    fs::write(
        &config_path,
        explorer_config_toml(&storage_path, &secondary_path, listen_addr)?,
    )?;

    let mut child = zinder_explorer_tokio_command()
        .args(["--config", path_str(&config_path)?])
        .spawn()?;

    let channel = await_explorer_channel(listen_addr).await;
    if channel.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    let channel = channel?;
    let mut client = ExplorerQueryClient::new(channel);
    let server_info = tokio::time::timeout(
        Duration::from_secs(5),
        client.server_info(ServerInfoRequest {}),
    )
    .await;

    let _ = child.kill().await;
    let _ = child.wait().await;

    let explorer_info = server_info
        .map_err(|_| eyre::eyre!("explorer ServerInfo request timed out"))??
        .into_inner()
        .info
        .ok_or_else(|| eyre::eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .ok_or_else(|| eyre::eyre!("server info missing common envelope"))?;

    assert!(
        !common
            .capabilities
            .iter()
            .any(|capability| capability == EXPLORER_TRANSACTION_DETAIL_V4),
        "{:?}",
        common.capabilities
    );
    assert!(
        !common
            .capabilities
            .iter()
            .any(|capability| capability == EXPLORER_BLOCK_SUMMARY_V1),
        "{:?}",
        common.capabilities
    );
    assert!(
        !common
            .capabilities
            .iter()
            .any(|capability| capability == EXPLORER_OVERVIEW_SNAPSHOT_V1),
        "{:?}",
        common.capabilities
    );
    assert!(
        !common
            .capabilities
            .iter()
            .any(|capability| capability == EXPLORER_TRANSACTION_FEES_V1),
        "{:?}",
        common.capabilities
    );

    Ok(())
}

fn zinder_explorer_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zinder-explorer"));
    command.env_clear();
    command
}

fn zinder_explorer_tokio_command() -> TokioCommand {
    let mut command = TokioCommand::new(env!("CARGO_BIN_EXE_zinder-explorer"));
    command.env_clear();
    command.stdout(Stdio::null()).stderr(Stdio::null());
    command
}

fn explorer_config_toml(
    storage_path: &Path,
    secondary_path: &Path,
    listen_addr: SocketAddr,
) -> eyre::Result<String> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[explorer]
listen_addr = "{}"
wallet_query_endpoint = "http://127.0.0.1:1"

[ops]
listen_addr = ""
"#,
        path_str(storage_path)?,
        path_str(secondary_path)?,
        listen_addr,
    ))
}

fn unused_loopback_addr() -> eyre::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

async fn await_explorer_channel(listen_addr: SocketAddr) -> eyre::Result<Channel> {
    let endpoint = format!("http://{listen_addr}");
    for _ in 0..100 {
        match Endpoint::from_shared(endpoint.clone())?.connect().await {
            Ok(channel) => return Ok(channel),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    Err(eyre::eyre!(
        "explorer did not accept gRPC connections at {listen_addr}"
    ))
}

fn path_str(path: &Path) -> eyre::Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre::eyre!("path is not valid UTF-8: {}", path.display()))
}
