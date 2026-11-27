use super::CompatProcess;
use crate::common::shielded_catalog_keys as signing_keys;
use eyre::{OptionExt as _, Result, WrapErr as _, eyre};
use std::{fs, io::Write as _, num::NonZeroU32, path::Path, time::Duration};
use zinder_compat_lightwalletd::ShieldedCatalogPublisher;
use zinder_core::{BlockHeight, Network};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::RocksDbCanonicalReader;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

fn canonical_fixture(chain: &ChainFixture) -> Result<WalletServingStoreFixture> {
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain),
        &sample_regtest_upgrade_activations(),
    )?
    .into_serving_store_fixture()
}

async fn start_signed(
    serving: &str,
    directory: &Path,
    chain: &ChainFixture,
) -> Result<CompatProcess> {
    let (private, public) = signing_keys(directory)?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain),
        &sample_regtest_upgrade_activations(),
    )?;
    let config = directory.join("publisher.toml");
    fs::write(
        &config,
        format!(
            r#"[network]
name = "zcash-regtest"
[storage]
path = "{}"
secondary_path = "{}"
raw_blob_policy = "{}"
[compat]
serving = "compact-blocks"
reorg_window_blocks = 100
"#,
            writer.canonical_primary_path().display(),
            directory.join("secondary").display(),
            chain.raw_blob_retention()
        ),
    )?;
    let publication =
        tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"))
            .env_clear()
            .arg("--config")
            .arg(config)
            .arg("publish-shielded-history")
            .arg("--directory")
            .arg(directory)
            .arg("--signing-key")
            .arg(private)
            .args(["--first-height", "1", "--segment-blocks", "2"])
            .output()
            .await?;
    eyre::ensure!(
        publication.status.success(),
        "publisher failed: {}",
        String::from_utf8_lossy(&publication.stderr)
    );
    start_signed_listener(serving, &directory.join("current.json"), &public).await
}

async fn start_signed_listener(
    serving: &str,
    manifest: &Path,
    public: &Path,
) -> Result<CompatProcess> {
    let section = format!(
        r#"[compat.shielded_history]
manifest = "{}"
publisher_key = "{}"
"#,
        manifest.display(),
        public.display()
    );
    CompatProcess::start_with_history(serving, Some(super::ORIGIN), &section).await
}

#[tokio::test]
async fn configured_listener_serves_signed_history_and_live_tail_on_the_configured_endpoint()
-> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    for serving in ["wallet", "compact-blocks"] {
        let files = tempfile::tempdir()?;
        let runtime = start_signed(serving, files.path(), &chain).await?;
        let mut client =
            CompactTxStreamerClient::connect(format!("http://{}", runtime.addr)).await?;
        let id = |height| lightwalletd::BlockId {
            height,
            hash: Vec::new(),
        };
        let mut blocks = client
            .get_block_range(lightwalletd::BlockRange {
                start: Some(id(1)),
                end: Some(id(3)),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let mut previous = None;
        for height in 1..=3 {
            let block = blocks
                .message()
                .await?
                .ok_or_else(|| eyre!("missing block at height {height}"))?;
            assert_eq!(block.height, height);
            if let Some(parent) = previous {
                assert_eq!(block.prev_hash, parent);
            }
            previous = Some(block.hash);
        }
        assert!(blocks.message().await?.is_none());
        let native = collect_range(&mut client, 2, 1).await?;
        let request = lightwalletd::BlockRange {
            start: Some(id(2)),
            end: Some(id(1)),
            pool_types: Vec::new(),
        };
        let http = reqwest::Client::builder().http1_only().build()?;
        let expected: Vec<_> = native.iter().map(prost::Message::encode_to_vec).collect();
        for encoding in [super::CONTENT_TYPE, super::TEXT_CONTENT_TYPE] {
            let response = runtime
                .post_as(&http, "GetBlockRange", encoding, &request)
                .await?;
            assert_eq!(
                super::read_web_response(response, encoding, 0).await?,
                expected
            );
        }
        let tree = client.get_tree_state(id(3)).await;
        if serving == "wallet" {
            assert_eq!(tree?.into_inner().height, 3);
        } else {
            assert_eq!(
                tree.err().map(|status| status.code()),
                Some(tonic::Code::Unimplemented)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn configured_listener_rejects_a_signed_manifest_from_a_displaced_branch_at_startup()
-> Result<()> {
    let files = tempfile::tempdir()?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(3)
        .fork_at(BlockHeight::new(2))?
        .extend_blocks(2);
    let outcome = start_signed("wallet", files.path(), &chain).await;
    let err = outcome
        .err()
        .ok_or_else(|| eyre!("runtime served a displaced branch"))?;
    let message = format!("{err:#}");
    assert!(
        message.contains("shielded range terminal block"),
        "{message}"
    );
    Ok(())
}

#[tokio::test]
async fn wallet_adopts_first_publication_on_the_same_connection() -> Result<()> {
    let files = tempfile::tempdir()?;
    let directory = files.path();
    let trusted = tempfile::tempdir()?;
    let (private, public) = signing_keys(trusted.path())?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut fixture = canonical_fixture(&chain)?;
    let (canonical, _) = fixture.take_readers()?;
    let reader = RocksDbCanonicalReader::new(canonical)?;
    let section = format!(
        "[compat.shielded_history]\ndirectory = \"{}\"\npublisher_key = \"{}\"\n",
        directory.display(),
        public.display()
    );
    let storage = tempfile::tempdir()?;
    let log = storage.path().join("compat.log");
    let (runtime, publisher) = tokio::join!(
        CompatProcess::start_with_history_in("wallet", Some(super::ORIGIN), &section, storage),
        async {
            wait_for_log_file(&log, "shielded_publisher_starting").await?;
            Ok::<_, eyre::Report>(ShieldedCatalogPublisher::open(
                directory,
                &private,
                BlockHeight::new(1),
                NonZeroU32::MIN,
            )?)
        }
    );
    let runtime = runtime?;
    let mut publisher = publisher?;
    let mut wallet = runtime.native_client().await?;
    let expected = collect_range(&mut wallet, 1, 3).await?;
    let checkpoint = directory.join(".publisher-head.json");
    let empty_head = fs::read(&checkpoint)?;
    replace_manifest(&checkpoint, b"invalid signed publisher checkpoint")?;
    wait_for_log(&runtime, "shielded_catalog_refresh_failed").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    replace_manifest(&checkpoint, &empty_head)?;
    let manifest = directory.join("current.json");
    replace_manifest(&manifest, b"{}")?;
    wait_for_log(&runtime, "missing field `manifest`").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    assert!(publisher.publish_next(&reader)?);
    wait_for_log(&runtime, "shielded_catalog_installed").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);

    replace_manifest(&checkpoint, br#"{"manifest":"{}","signature":"invalid"}"#)?;
    wait_for_log(&runtime, "invalid shielded range signature encoding").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    fs::remove_file(&checkpoint)?;
    let missing = fs::metadata(&checkpoint)
        .err()
        .ok_or_eyre("removed publisher checkpoint still present")?;
    wait_for_log(&runtime, &missing.to_string()).await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    Ok(())
}

#[tokio::test]
async fn running_wallet_admits_an_extension_and_keeps_it_after_a_signed_rollback() -> Result<()> {
    let files = tempfile::tempdir()?;
    let directory = files.path();
    let (private, public) = signing_keys(directory)?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut fixture = canonical_fixture(&chain)?;
    let (canonical, _) = fixture.take_readers()?;
    let reader = RocksDbCanonicalReader::new(canonical)?;
    let mut publisher =
        ShieldedCatalogPublisher::open(directory, &private, BlockHeight::new(1), NonZeroU32::MIN)?;
    assert!(publisher.publish_next(&reader)?);
    let manifest = directory.join("current.json");
    let initial = fs::read(&manifest)?;
    #[cfg(unix)]
    let listener_manifest = {
        let link = directory.join("snapshot.json");
        std::os::unix::fs::symlink(&manifest, &link)?;
        link
    };
    #[cfg(not(unix))]
    let listener_manifest = manifest.clone();
    let runtime = start_signed_listener("wallet", &listener_manifest, &public).await?;
    let mut wallet = runtime.native_client().await?;
    let expected = collect_range(&mut wallet, 1, 3).await?;

    replace_manifest(&manifest, b"invalid signed catalog")?;
    wait_for_log(&runtime, "shielded_catalog_refresh_failed").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    replace_manifest(&manifest, &initial)?;
    assert!(publisher.publish_next(&reader)?);
    wait_for_log(&runtime, "shielded_catalog_installed").await?;
    let reversed = expected.iter().cloned().rev().collect::<Vec<_>>();
    assert_eq!(collect_range(&mut wallet, 3, 1).await?, reversed);

    replace_manifest(&manifest, &initial)?;
    wait_for_log(&runtime, "regressed its admitted prefix").await?;
    assert_eq!(collect_range(&mut wallet, 1, 3).await?, expected);
    let tree = wallet
        .get_tree_state(lightwalletd::BlockId {
            height: 3,
            hash: Vec::new(),
        })
        .await?
        .into_inner();
    assert_eq!(tree.height, 3);
    Ok(())
}

#[tokio::test]
async fn running_compact_listener_rejects_a_signed_extension_from_a_displaced_branch() -> Result<()>
{
    let files = tempfile::tempdir()?;
    let directory = files.path();
    let (private, public) = signing_keys(directory)?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut fixture = canonical_fixture(&chain)?;
    let (canonical, _) = fixture.take_readers()?;
    let first = BlockHeight::new(1);
    let mut publisher =
        ShieldedCatalogPublisher::open(directory, &private, first, NonZeroU32::MIN)?;
    assert!(publisher.publish_next(&RocksDbCanonicalReader::new(canonical)?)?);
    drop(publisher);
    let manifest = directory.join("current.json");
    let runtime = start_signed_listener("compact-blocks", &manifest, &public).await?;
    let mut client = runtime.native_client().await?;
    let expected = collect_range(&mut client, 1, 3).await?;
    let fork = chain.fork_at(BlockHeight::new(2))?.extend_blocks(2);
    let mut alternate = canonical_fixture(&fork)?;
    let (branch, _) = alternate.take_readers()?;
    let mut publisher =
        ShieldedCatalogPublisher::open(directory, &private, first, NonZeroU32::MIN)?;
    assert!(publisher.publish_next(&RocksDbCanonicalReader::new(branch)?)?);
    wait_for_log(&runtime, "shielded range terminal block").await?;
    assert_eq!(collect_range(&mut client, 1, 3).await?, expected);
    Ok(())
}

#[tokio::test]
async fn a_signed_extension_is_retried_when_the_serving_secondary_catches_up() -> Result<()> {
    let files = tempfile::tempdir()?;
    let directory = files.path();
    let (private, public) = signing_keys(directory)?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(4)
        .with_raw_blob_retention(zinder_store::RawBlobRetention::None);
    let mut fixture = canonical_fixture(&chain)?;
    let (canonical, _) = fixture.take_readers()?;
    let reader = RocksDbCanonicalReader::new(canonical)?;
    let mut publisher = ShieldedCatalogPublisher::open(
        directory,
        &private,
        BlockHeight::new(1),
        2_u32.try_into()?,
    )?;
    assert!(publisher.publish_next(&reader)?);
    let manifest = directory.join("current.json");
    let mut runtime = start_signed_listener("compact-blocks", &manifest, &public).await?;
    let mut client = runtime.native_client().await?;
    assert!(publisher.publish_next(&reader)?);
    wait_for_log(
        &runtime,
        "shielded range does not belong to this settled view",
    )
    .await?;
    collect_range(&mut client, 1, 3).await?;

    runtime.writer.append_settled_blocks(&chain)?;
    let primary = runtime.writer.canonical_primary()?;
    *runtime.writer_control.status.lock() = super::writer_status_for_fence(
        primary.event_fence(),
        chain.network(),
        primary.network_upgrade_schedule(),
    );
    wait_for_log(&runtime, "shielded_catalog_installed").await?;
    let blocks = collect_range(&mut client, 1, 4).await?;
    assert_eq!(blocks.last().map(|block| block.height), Some(4));
    Ok(())
}

async fn collect_range(
    wallet: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    first: u64,
    last: u64,
) -> Result<Vec<lightwalletd::CompactBlock>> {
    let id = |height| lightwalletd::BlockId {
        height,
        hash: Vec::new(),
    };
    let mut stream = wallet
        .get_block_range(lightwalletd::BlockRange {
            start: Some(id(first)),
            end: Some(id(last)),
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    let mut blocks = Vec::new();
    while let Some(block) = stream.message().await? {
        blocks.push(block);
    }
    assert_eq!(u64::try_from(blocks.len())?, first.abs_diff(last) + 1);
    Ok(blocks)
}

fn replace_manifest(manifest: &Path, contents: &[u8]) -> Result<()> {
    let mut replacement = tempfile::NamedTempFile::new_in(
        manifest.parent().ok_or_eyre("missing manifest directory")?,
    )?;
    replacement.write_all(contents)?;
    replacement.persist(manifest)?;
    Ok(())
}

async fn wait_for_log(runtime: &CompatProcess, message: &str) -> Result<()> {
    wait_for_log_file(&runtime.storage.path().join("compat.log"), message).await
}

async fn wait_for_log_file(log: &Path, message: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match fs::read_to_string(log) {
                Ok(contents) if contents.contains(message) => return Ok(()),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .wrap_err_with(|| {
        format!(
            "missing {message} in {}",
            fs::read_to_string(log).unwrap_or_default()
        )
    })?
}
