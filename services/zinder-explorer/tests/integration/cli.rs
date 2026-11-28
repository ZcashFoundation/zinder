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

use prost::Message as _;
use tempfile::tempdir;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    process::{Child as TokioChild, Command as TokioCommand},
    time::timeout,
};
use tonic::transport::{Channel, Endpoint};
use tonic_health::pb::health_check_response::ServingStatus;
use zinder_core::{BlockHeight, Network, wire::encode_rpc_block_hash_hex};
use zinder_materialized_views::{BLOCK_SUMMARY_COLUMN_FAMILY, BlockSummaryConsumer};
use zinder_proto::{
    capabilities::{
        EXPLORER_BLOCK_SUMMARY_V1, EXPLORER_OVERVIEW_SNAPSHOT_V1, EXPLORER_TRANSACTION_DETAIL_V4,
        EXPLORER_TRANSACTION_FEES_V1,
    },
    v1::explorer::{
        BlockDetailRequest, BlockSummary, BlockSummaryRecord, ServerInfoRequest,
        block_detail_request, explorer_query_client::ExplorerQueryClient,
    },
};
use zinder_runtime::BearerTokenClientInterceptor;
use zinder_store::DEFAULT_SETTLED_SEGMENT_BLOCKS;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, PROBE_WAIT_LIMIT, PublicListenerProbe,
    open_test_materialized_view_store_for_canonical, sample_regtest_upgrade_activations,
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
fn browser_transport_rejects_wildcard_with_inline_or_file_bearer_auth() -> eyre::Result<()> {
    let storage = tempdir()?;
    let config_path = storage.path().join("explorer.toml");
    let token_path = storage.path().join("explorer.token");
    let token = "fixture-browser-token";
    fs::write(&token_path, token)?;
    for origin in ["*", "https://explorer.example"] {
        fs::write(
            &config_path,
            format!("[explorer]\nbrowser_origins = [\"{origin}\"]\n"),
        )?;
        for (name, configured_token) in [
            ("ZINDER_EXPLORER__BEARER_TOKEN", token),
            ("ZINDER_EXPLORER__BEARER_TOKEN_PATH", path_str(&token_path)?),
        ] {
            let output = zinder_explorer_command()
                .env_clear()
                .args(["--print-config", "--network", "zcash-regtest", "--config"])
                .arg(&config_path)
                .env(name, configured_token)
                .output()?;
            let stdout = String::from_utf8(output.stdout)?;
            let stderr = String::from_utf8(output.stderr)?;
            assert!(!stdout.contains(token), "{stdout}");
            assert!(!stderr.contains(token), "{stderr}");
            if origin == "*" {
                assert!(!output.status.success(), "{stdout}");
                assert!(
                    stderr.contains("explorer.browser_origins requires specific origins"),
                    "{stderr}"
                );
            } else {
                assert!(output.status.success(), "{stderr}");
                assert!(stdout.contains(origin), "{stdout}");
            }
        }
    }
    Ok(())
}

#[test]
fn wallet_serving_explorer_image_config_resolves() -> eyre::Result<()> {
    let output = zinder_explorer_command()
        .args(["--print-config"])
        .env("ZINDER_NETWORK__NAME", "zcash-testnet")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .env("ZINDER_STORAGE__PATH", "/var/lib/zinder/canonical")
        .env(
            "ZINDER_STORAGE__SECONDARY_PATH",
            "/var/lib/zinder/explorer/secondary",
        )
        .env("ZINDER_EXPLORER__LISTEN_ADDR", "[::]:9068")
        .env(
            "ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT",
            "http://127.0.0.1:9102",
        )
        .env("ZINDER_OPS__LISTEN_ADDR", "[::]:9069")
        .env("ZINDER_SECURITY__ALLOW_PUBLIC_BIND", "true")
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
fn default_secondary_path_matches_wallet_serving_explorer_layout() -> eyre::Result<()> {
    let output = zinder_explorer_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains("secondary_path = \"/var/lib/zinder/explorer/secondary\""),
        "{stdout}"
    );

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
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-explorer"));
    command.env_clear();
    command
}

fn zinder_explorer_tokio_command() -> TokioCommand {
    let mut command = TokioCommand::new(zinder_testkit::bin_exe!("zinder-explorer"));
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

#[test]
fn data_dir_and_inline_bearer_token_work_with_redaction() -> eyre::Result<()> {
    let scratch = tempfile::tempdir()?;
    let custom_dir = scratch.path().join("zinder-root");
    let output = zinder_explorer_command()
        .args(["--print-config"])
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_STORAGE__DATA_DIR", &custom_dir)
        .env("ZINDER_EXPLORER__BEARER_TOKEN", "explorer-inline-secret")
        .env("ZINDER_INGEST__SOURCE", "zebra-json-rpc")
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains(&format!(
            "path = \"{}\"",
            custom_dir.join("canonical").display()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "secondary_path = \"{}\"",
            custom_dir.join("explorer").display()
        )),
        "{stdout}"
    );
    assert!(stdout.contains("bearer_token = \"[REDACTED]\""), "{stdout}");
    assert!(!stdout.contains("explorer-inline-secret"), "{stdout}");

    let conflict = zinder_explorer_command()
        .args(["--print-config"])
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env(
            "ZINDER_EXPLORER__BEARER_TOKEN_PATH",
            scratch.path().join("tok"),
        )
        .env("ZINDER_EXPLORER__BEARER_TOKEN", "explorer-inline-secret")
        .output()?;
    assert!(!conflict.status.success(), "{conflict:?}");
    let stderr = String::from_utf8(conflict.stderr)?;
    assert!(!stderr.contains("explorer-inline-secret"), "{stderr}");
    Ok(())
}

struct ExplorerChildGuard(TokioChild);

impl Drop for ExplorerChildGuard {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

async fn scrape_ops_metrics(ops_addr: SocketAddr) -> eyre::Result<String> {
    let mut stream = TcpStream::connect(ops_addr).await?;
    stream
        .write_all(
            format!("GET /metrics HTTP/1.1\r\nHost: {ops_addr}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let text = String::from_utf8(response)?;
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| eyre::eyre!("malformed /metrics HTTP response"))?;
    eyre::ensure!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected /metrics status: {head}"
    );
    Ok(body.to_owned())
}

fn settled_segment_metric(exposition: &str, suffix: &str, kind: &str) -> eyre::Result<u64> {
    let prefix = format!("zinder_store_settled_segment_{suffix}{{kind=\"{kind}\"}} ");
    let raw_metric = exposition
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .ok_or_else(|| eyre::eyre!("missing settled-segment metric line `{prefix}`"))?;
    Ok(raw_metric.trim().parse()?)
}

fn seed_settled_explorer_fixture() -> eyre::Result<CanonicalWriterFixture> {
    let chain =
        ChainFixture::new(Network::ZcashRegtest).extend_blocks(DEFAULT_SETTLED_SEGMENT_BLOCKS);
    let mut canonical = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain)
            .through(BlockHeight::new(DEFAULT_SETTLED_SEGMENT_BLOCKS - 1)),
        &sample_regtest_upgrade_activations(),
    )?;
    canonical.append_settled_blocks(&chain)?;
    let target_block = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("missing fixture block 1"))?;
    let mv_primary =
        open_test_materialized_view_store_for_canonical(canonical.canonical_primary_path())?;
    let summary_record = BlockSummaryRecord {
        summary: Some(BlockSummary {
            block_height: 1,
            block_hash: encode_rpc_block_hash_hex(target_block.hash),
            previous_block_hash: encode_rpc_block_hash_hex(target_block.parent_hash),
            block_time_unix_seconds: i64::from(target_block.block_time_seconds),
            transaction_count: 0,
            total_size_bytes: u64::try_from(target_block.raw_block_bytes.len())?,
            confirmations: DEFAULT_SETTLED_SEGMENT_BLOCKS,
            is_canonical: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    mv_primary.put_consumer(
        BLOCK_SUMMARY_COLUMN_FAMILY,
        &BlockSummaryConsumer::key_for_height(BlockHeight::new(1)),
        &summary_record.encode_to_vec(),
    )?;
    Ok(canonical)
}

async fn await_explorer_subprocess_ready(
    grpc: SocketAddr,
    ops: SocketAddr,
    child: &mut ExplorerChildGuard,
) -> eyre::Result<PublicListenerProbe> {
    let mut last_probe_status = String::from("not yet connected");
    timeout(PROBE_WAIT_LIMIT, async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                eyre::bail!("zinder-explorer exited before readiness: {status}");
            }
            match PublicListenerProbe::connect(grpc, ops).await {
                Ok(probe) => match probe.agreed_status().await {
                    Ok(ServingStatus::Serving) => break Ok(probe),
                    Ok(status) => last_probe_status = format!("status={status:?}"),
                    Err(error) => last_probe_status = format!("agreed_status err={error}"),
                },
                Err(error) => last_probe_status = format!("connect err={error}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("timed out waiting for explorer readiness ({last_probe_status})"))?
}

async fn await_settled_histograms(ops: SocketAddr) -> eyre::Result<String> {
    // Histograms drain on zinder-runtime's 5s upkeep interval.
    timeout(Duration::from_secs(10), async {
        loop {
            let body = scrape_ops_metrics(ops).await?;
            let ready = ["shielded", "supplement"].into_iter().all(|kind| {
                settled_segment_metric(&body, "pread_duration_seconds_count", kind)
                    .is_ok_and(|count| count > 0)
            });
            if ready {
                break Ok::<String, eyre::Report>(body);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await?
}

fn assert_initial_settled_metrics(metrics: &str) -> eyre::Result<u64> {
    for kind in ["shielded", "supplement"] {
        assert_eq!(
            settled_segment_metric(metrics, "cache_open_readers", kind)?,
            1
        );
        assert!(settled_segment_metric(metrics, "cache_index_heap_bytes", kind)? > 0);
        assert_eq!(
            settled_segment_metric(metrics, "cache_misses_total", kind)?,
            1
        );
        assert!(settled_segment_metric(metrics, "pread_bytes_total", kind)? > 0);
    }
    settled_segment_metric(metrics, "pread_bytes_total", "supplement")
}

async fn assert_explorer_metrics_session(
    probe: &PublicListenerProbe,
    ops: SocketAddr,
    token_text: &str,
) -> eyre::Result<()> {
    let initial_metrics = scrape_ops_metrics(ops).await?;
    let initial_supplement_pread_bytes = assert_initial_settled_metrics(&initial_metrics)?;

    let token = token_text.parse()?;
    let mut explorer = ExplorerQueryClient::with_interceptor(
        probe.channel(),
        BearerTokenClientInterceptor::new(Some(&token))?,
    );
    for _ in 0..2 {
        let response = explorer
            .block_transactions(BlockDetailRequest {
                selector: Some(block_detail_request::Selector::BlockHeight(1)),
                at_epoch_id: None,
            })
            .await?
            .into_inner();
        assert_eq!(
            response
                .summary
                .as_ref()
                .map(|summary| summary.block_height),
            Some(1)
        );
    }

    let final_metrics = await_settled_histograms(ops).await?;
    assert!(settled_segment_metric(&final_metrics, "cache_hits_total", "supplement")? >= 2);
    assert_eq!(
        settled_segment_metric(&final_metrics, "cache_misses_total", "supplement")?,
        1
    );
    assert!(
        settled_segment_metric(&final_metrics, "pread_bytes_total", "supplement")?
            > initial_supplement_pread_bytes
    );
    Ok(())
}

#[tokio::test]
async fn explorer_startup_exports_settled_segment_metrics_from_opened_readers() -> eyre::Result<()>
{
    let seeded = seed_settled_explorer_fixture()?;
    let grpc = unused_loopback_addr()?;
    let ops = unused_loopback_addr()?;
    let dir = tempdir()?;
    let secondary_root = dir.path().join("secondary");
    fs::create_dir_all(&secondary_root)?;
    let token_path = dir.path().join("explorer.token");
    fs::write(&token_path, "test-token")?;

    let mut child = ExplorerChildGuard(
        zinder_explorer_tokio_command()
            .args([
                "--network",
                "zcash-regtest",
                "--storage-path",
                path_str(seeded.canonical_primary_path())?,
                "--secondary-path",
                path_str(&secondary_root)?,
                "--listen-addr",
                &grpc.to_string(),
                "--ops-listen-addr",
                &ops.to_string(),
                "--bearer-token-path",
                path_str(&token_path)?,
            ])
            .spawn()?,
    );

    let outcome = async {
        let probe = await_explorer_subprocess_ready(grpc, ops, &mut child).await?;
        assert_explorer_metrics_session(&probe, ops, "test-token").await
    }
    .await;

    let _ = child.0.kill().await;
    let _ = child.0.wait().await;
    drop(seeded);
    outcome
}
