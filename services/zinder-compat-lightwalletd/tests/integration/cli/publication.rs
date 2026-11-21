use std::{fs, io, path::Path, time::Duration};

use eyre::Result;
use tokio::process::Command;
use zinder_compat_lightwalletd::PublishedShieldedHistory;
use zinder_core::{BlockHeight, Network};
use zinder_query::RocksDbCanonicalReader;
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
};

use crate::common::shielded_catalog_keys;

#[test]
fn history_configuration_requires_one_location_and_an_independent_key() -> Result<()> {
    let base =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/config/history-publisher.toml");
    let render = |location: &[(&str, &str)]| {
        super::zinder_compat_command()
            .args(["--print-config", "--config"])
            .arg(&base)
            .env("ZINDER_NETWORK__NAME", "zcash-regtest")
            .env(
                "ZINDER_COMPAT__SHIELDED_HISTORY__PUBLISHER_KEY",
                "/trusted/key",
            )
            .envs(location.iter().copied())
            .output()
    };
    let directory = ("ZINDER_COMPAT__SHIELDED_HISTORY__DIRECTORY", "/published");
    let manifest = (
        "ZINDER_COMPAT__SHIELDED_HISTORY__MANIFEST",
        "/published/current.json",
    );
    for (setting, expected) in [
        (directory, "directory = \"/published\""),
        (manifest, "manifest = \"/published/current.json\""),
    ] {
        let output = render(&[setting])?;
        assert!(output.status.success(), "{output:?}");
        assert!(String::from_utf8(output.stdout)?.contains(expected));
    }
    assert!(!render(&[directory, manifest])?.status.success());
    assert!(!render(&[])?.status.success());
    Ok(())
}

#[tokio::test]
async fn publisher_follows_settlement_and_resumes_a_checkpointed_store() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let catalog = files.path().join("catalog");
    let manifest = catalog.join("current.json");
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(3)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let baseline = CanonicalBaseline::new(&chain)
        .through(BlockHeight::new(2))
        .with_history_floor(BlockHeight::new(1));
    let mut writer =
        CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?;
    let mut command = Command::from(super::zinder_compat_command());
    command
        .kill_on_drop(true)
        .env("TMPDIR", files.path())
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .arg("--config")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../deploy/config/history-publisher.toml"),
        )
        .arg("--canonical-primary-path")
        .arg(writer.canonical_primary_path())
        .args([
            "--reorg-window-blocks",
            "100",
            "publish-shielded-history",
            "--directory",
        ])
        .arg(&catalog)
        .arg("--signing-key")
        .arg(&private)
        .args(["--segment-blocks", "2", "--follow-interval-secs", "1"]);

    let mut publisher = command.spawn()?;
    wait_for_change(&catalog.join(".publisher-head.json"), &[]).await?;
    assert!(!manifest.try_exists()?);
    writer.append_settled_blocks(&chain)?;
    let first = wait_for_change(&manifest, &[]).await?;
    publisher.kill().await?;
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let admitted = PublishedShieldedHistory::open(&manifest, &public, false, &reader).await?;
    let mut resumed = command.spawn()?;
    writer.append_settled_blocks(&chain.extend_blocks(2))?;
    wait_for_change(&manifest, &first).await?;
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    assert!(admitted.refresh(&reader).await?);
    resumed.kill().await?;
    Ok(())
}

async fn wait_for_change(path: &Path, previous: &[u8]) -> Result<Vec<u8>> {
    Ok(tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match fs::read(path) {
                Ok(bytes) if bytes != previous => return Ok::<_, io::Error>(bytes),
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??)
}
