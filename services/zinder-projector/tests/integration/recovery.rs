use std::{fs, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use eyre::{Result, bail, ensure};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    time::{sleep, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_core::Network;
use zinder_ingest::{
    CanonicalCheckpointStagingRoot, CanonicalControlGrpcAdapter, CanonicalFollowConfig,
    CanonicalFollower, CanonicalStoreDamageSignal, DEFAULT_MAX_REPLACEMENT_BYTES,
    canonical_control_channel, follow_canonical_tip_with_control,
};
use zinder_projector::recovery_archive::RECOVERY_ARCHIVE_MANIFEST_FILE_NAME;
use zinder_runtime::{BearerToken, ComposedReadiness, Readiness};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_store::{RawBlobRetention, RocksDbResourceBudget};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, JsonRpcTestServer, RpcReply, method,
    sample_regtest_upgrade_activations,
};

const NETWORK: Network = Network::ZcashRegtest;
const TOKEN: &str = "fixture-control-token";
const WAIT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_receipt_verifies_and_restores_the_owner_checkpoint_offline() -> Result<()> {
    let scratch = tempfile::tempdir()?;
    let root = scratch.path();
    let staging = root.join("staging");
    fs::create_dir(&staging)?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let mut writer =
        CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let store = writer.take_canonical_primary()?;
    let tip = store.event_fence().visible_tip();
    let node =
        JsonRpcTestServer::start([method("getbestblockheightandhash").reply_with(move |_| {
            RpcReply::result(json!({ "height": tip.height.value(), "hash": tip.hash.as_bytes() }))
        })])?;
    let source = ZebraJsonRpcSource::new(NETWORK, node.url(), NodeAuth::None, WAIT)?;
    let token: BearerToken = TOKEN.parse()?;
    let (handle, commands) = canonical_control_channel();
    let service = CanonicalControlGrpcAdapter::new(
        handle,
        CanonicalCheckpointStagingRoot::new(staging),
        RocksDbResourceBudget::for_local_tests(),
        CanonicalStoreDamageSignal::default(),
    )
    .with_bearer_token(Some(token.clone()))
    .with_checkpoint_bearer_token(Some(token))
    .into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let owner_port = TcpListener::bind("127.0.0.1:0").await?;
    write_projector_config(
        root,
        writer.canonical_primary_path(),
        store.reorg_policy().reorg_window_blocks(),
        listener.local_addr()?,
        owner_port.local_addr()?,
    )?;

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        activations,
        CanonicalFollowConfig {
            request_timeout: WAIT,
            poll_interval: Duration::from_millis(50),
            lag_threshold_blocks: 1,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: WAIT,
            max_replacement_bytes: DEFAULT_MAX_REPLACEMENT_BYTES,
            mempool_ready_gate: None,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let archive = root.join("wallet-backup");
    let pin = tokio::select! {
        result = capture_with_owner(root, owner_port, &archive) => result?,
        result = follow_canonical_tip_with_control(store, follower, commands) => {
            result?;
            bail!("canonical follower exited before the backup")
        },
        result = Server::builder().add_service(service).serve_with_incoming(TcpListenerStream::new(listener)) => {
            result?;
            bail!("canonical control exited before the backup")
        },
    };
    drop(node);
    verify_and_restore_offline(root, &archive, &pin).await
}

fn write_projector_config(
    root: &Path,
    primary: &Path,
    reorg_window: u32,
    ingest: SocketAddr,
    projector: SocketAddr,
) -> Result<()> {
    let token = root.join("token");
    fs::write(&token, TOKEN)?;
    let config = json!({
        "network": { "name": "zcash-regtest" },
        "storage": {
            "canonical_path": primary,
            "canonical_secondary_path": root.join("secondary"),
        },
        "wallet": { "path": root.join("wallet") },
        "projector": {
            "reorg_window_blocks": reorg_window,
            "build_owner_hex": "00112233445566778899aabbccddeeff",
            "lease_duration_seconds": 14400,
        },
        "ingest_control": { "addr": format!("http://{ingest}"), "bearer_token_path": token },
        "projector_control": {
            "listen_addr": projector,
            "bearer_token_path": token,
            "checkpoint_staging_root": root.join("staging"),
        },
        "ops": { "listen_addr": "" },
    });
    fs::write(root.join("projector.toml"), toml::to_string(&config)?)?;
    Ok(())
}

fn projector(root: &Path) -> Command {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-projector"));
    command
        .current_dir(root)
        .env_clear()
        .env("TOKIO_WORKER_THREADS", "2")
        .kill_on_drop(true);
    command
}

async fn capture_with_owner(
    root: &Path,
    owner_port: TcpListener,
    archive: &Path,
) -> Result<String> {
    let address = owner_port.local_addr()?;
    drop(owner_port);
    let mut owner = projector(root)
        .args(["--config", "projector.toml"])
        .spawn()?;
    timeout(WAIT, async {
        while TcpStream::connect(address).await.is_err() {
            if let Some(status) = owner.try_wait()? {
                bail!("projector exited before serving control: {status}");
            }
            sleep(Duration::from_millis(20)).await;
        }
        Ok::<(), eyre::Report>(())
    })
    .await??;
    let backup = || {
        let mut command = projector(root);
        command
            .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
            .env("ZINDER_WALLET_SERVING__LIGHTWALLETD", "true")
            .args(["--config", "projector.toml", "backup", "--archive"])
            .arg(archive);
        command
    };
    assert_insufficient_sort_memory(backup()).await?;
    assert!(!archive.exists());
    let pin = receipt(&mut backup()).await?;
    let manifest = fs::read(archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME))?;
    assert_eq!(pin, hex::encode(Sha256::digest(manifest)));
    assert_insufficient_sort_memory(backup()).await?;
    assert_eq!(receipt(&mut backup()).await?, pin);
    owner.kill().await?;
    fs::remove_dir_all(root.join("staging"))?;
    fs::remove_file(root.join("projector.toml"))?;
    Ok(pin)
}

async fn verify_and_restore_offline(root: &Path, archive: &Path, pin: &str) -> Result<()> {
    let destination = root.join("restored");
    for (operation, into) in [("verify", None), ("restore", Some(&destination))] {
        let command = |digest: &str| {
            let mut command = projector(root);
            command
                .args([
                    operation,
                    "--network",
                    "zcash-regtest",
                    "--sha256",
                    digest,
                    "--archive",
                ])
                .arg(archive);
            if let Some(path) = into {
                command.arg("--into").arg(path);
            }
            command
        };
        let bad = timeout(WAIT, command(&"00".repeat(32)).output()).await??;
        assert!(!bad.status.success(), "{bad:?}");
        assert_eq!(bad.stdout, b"");
        assert!(String::from_utf8(bad.stderr)?.contains("trusted digest"));
        assert_insufficient_sort_memory(command(pin)).await?;
        assert!(!destination.exists());
        assert_eq!(receipt(&mut command(pin)).await?, pin);
    }
    assert!(destination.join("canonical/CURRENT").is_file());
    assert!(destination.join("wallet/CURRENT").is_file());
    Ok(())
}

async fn assert_insufficient_sort_memory(mut command: Command) -> Result<()> {
    let output = timeout(
        WAIT,
        command
            .args(["--max-sort-memory-bytes-per-sorter", "1"])
            .output(),
    )
    .await??;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("wallet recovery checkpoint admission failed")
            && stderr.contains("accounted bytes, limit is 1"),
        "{stderr}"
    );
    Ok(())
}

async fn receipt(command: &mut Command) -> Result<String> {
    #[derive(serde::Deserialize)]
    struct Receipt {
        manifest_sha256: String,
    }
    let output = timeout(WAIT, command.output()).await??;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice::<Receipt>(&output.stdout)?.manifest_sha256)
}
