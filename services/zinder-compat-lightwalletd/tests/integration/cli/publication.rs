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
    let scratch = files.path().join("publisher-secondary");
    let manifest = catalog.join("current.json");
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(3)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let baseline = CanonicalBaseline::new(&chain)
        .through(BlockHeight::new(2))
        .with_history_floor(BlockHeight::new(1));
    let mut writer =
        CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?;
    let mut command = follow_command(
        writer.canonical_primary_path(),
        &scratch,
        &catalog,
        &private,
    );
    let mut publisher = command.spawn()?;
    wait_for_change(&catalog.join(".publisher-head.json"), &[]).await?;
    assert_eq!(fs::read_dir(&scratch)?.count(), 1);
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

#[cfg(unix)]
#[tokio::test]
async fn publisher_recovers_a_missing_table_while_holding_the_catalog_lock() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, _) = shielded_catalog_keys(files.path())?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("secondary");
    let manifest = catalog.join("current.json");
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(128)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;
    drop(writer.take_canonical_primary()?);
    let primary = writer.canonical_primary_path();
    let db = split_compact_blocks_into_tables(primary)?;
    let mut process = follow_command(primary, &scratch, &catalog, &private)
        .env("ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES", "64")
        .spawn()?;
    wait_for_change(&manifest, &[]).await?;
    let pid = process
        .id()
        .ok_or_else(|| eyre::eyre!("publisher exited"))?
        .to_string();
    assert!(
        Command::new("kill")
            .args(["-STOP", &pid])
            .status()
            .await?
            .success()
    );
    let generation = fs::read_dir(&scratch)?
        .next()
        .transpose()?
        .ok_or_else(|| eyre::eyre!("missing publisher secondary"))?
        .path();
    compact_away_the_old_tables(&db).await?;
    assert!(
        Command::new("kill")
            .args(["-CONT", &pid])
            .status()
            .await?
            .success()
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while generation.try_exists()? {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, io::Error>(())
    })
    .await??;
    assert!(process.try_wait()?.is_none());
    assert!(
        fs::File::open(catalog.join(".publisher.lock"))?
            .try_lock()
            .is_err()
    );
    let last_good = fs::read(&manifest)?;
    wait_for_change(&manifest, &last_good).await?;
    assert!(process.try_wait()?.is_none());
    process.kill().await?;
    Ok(())
}

#[cfg(unix)]
async fn compact_away_the_old_tables(db: &rust_rocksdb::DB) -> Result<()> {
    let old = db
        .live_files()?
        .into_iter()
        .filter(|file| file.column_family_name == "compact_block" && file.level == 0)
        .map(|file| Path::new(&file.directory).join(file.name.trim_start_matches('/')))
        .collect::<Vec<_>>();
    eyre::ensure!(!old.is_empty(), "missing level-zero compact-block tables");
    let family = db
        .cf_handle("compact_block")
        .ok_or_else(|| eyre::eyre!("missing compact-block family"))?;
    db.compact_range_cf(&family, None::<&[u8]>, None::<&[u8]>);
    tokio::time::timeout(Duration::from_secs(15), async {
        while old.iter().any(|table| table.exists()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[cfg(unix)]
fn split_compact_blocks_into_tables(primary: &Path) -> Result<rust_rocksdb::DB> {
    let options = rust_rocksdb::Options::default();
    let db = rust_rocksdb::DB::open_cf(
        &options,
        primary,
        rust_rocksdb::DB::list_cf(&options, primary)?,
    )?;
    let family = db
        .cf_handle("compact_block")
        .ok_or_else(|| eyre::eyre!("missing compact-block family"))?;
    // Each old row needs a distinct table so the secondary's 64-file cache
    // cannot keep the entire unpublished suffix alive across compaction.
    db.set_options_cf(
        &family,
        &[
            ("disable_auto_compactions", "true"),
            ("level0_slowdown_writes_trigger", "1000"),
            ("level0_stop_writes_trigger", "1000"),
        ],
    )?;
    let blocks = db
        .iterator_cf(&family, rust_rocksdb::IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()?;
    for (key, row) in blocks {
        db.put_cf(&family, key, row)?;
        db.flush_cf(&family)?;
    }
    drop(family);
    Ok(db)
}

fn follow_command(primary: &Path, scratch: &Path, catalog: &Path, private: &Path) -> Command {
    let mut command = Command::from(super::zinder_compat_command());
    command
        .kill_on_drop(true)
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .arg("--config")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../deploy/config/history-publisher.toml"),
        )
        .arg("--canonical-primary-path")
        .arg(primary)
        .arg("--canonical-secondary-root")
        .arg(scratch)
        .args([
            "--reorg-window-blocks",
            "100",
            "publish-shielded-history",
            "--directory",
        ])
        .arg(catalog)
        .arg("--signing-key")
        .arg(private)
        .args(["--segment-blocks", "2", "--follow-interval-secs", "1"]);

    command
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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??)
}
