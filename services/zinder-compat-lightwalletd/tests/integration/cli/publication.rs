use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;

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
fn history_publisher_prints_its_effective_configuration() -> Result<()> {
    let base = zinder_testkit::manifest_dir!().join("../../deploy/config/history-publisher.toml");
    let shipped: toml::Table = toml::from_str(&fs::read_to_string(&base)?)?;
    assert_eq!(
        shipped["storage"]["raw_blob_policy"].as_str(),
        Some("transactions")
    );
    assert_eq!(
        shipped["compat"]["serving"].as_str(),
        Some("compact-blocks")
    );
    let render = |path: &Path, setup: &[(&str, &str)]| {
        super::zinder_compat_command()
            .args(["--print-config", "--config"])
            .arg(path)
            .envs(setup.iter().copied())
            .args([
                "publish-shielded-history",
                "--directory",
                "/unused/catalog",
                "--signing-key",
                "/unused/publisher.key",
            ])
            .output()
    };
    let defaults = render(&base, &[("ZINDER_NETWORK__NAME", "zcash-regtest")])?;
    assert!(defaults.status.success(), "{defaults:?}");
    assert!(String::from_utf8(defaults.stdout)?.contains("reorg_window_blocks = 1000"));

    let output = render(
        &base,
        &[
            ("ZINDER_NETWORK__NAME", "zcash-regtest"),
            ("ZINDER_DEPLOYMENT__PROFILE", "light-wallet"),
            ("ZINDER_COMPAT__REORG_WINDOW_BLOCKS", "200"),
            ("ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES", "64"),
        ],
    )?;
    assert!(output.status.success(), "{output:?}");
    let printed = String::from_utf8(output.stdout)?;
    let settings: toml::Table = toml::from_str(&printed)?;
    assert_eq!(settings.len(), 3);
    assert!(settings["storage"].get("raw_blob_policy").is_none());
    assert!(settings["compat"].get("serving").is_none());
    for field in [
        "name = \"zcash-regtest\"",
        "path = \"/var/lib/zinder/canonical\"",
        "secondary_path = \"/tmp/secondary\"",
        "[storage.canonical.rocksdb]",
        "max_open_files = 64",
        "reorg_window_blocks = 200",
    ] {
        assert!(printed.lines().any(|line| line == field), "{printed}");
    }
    let directory = tempfile::tempdir()?;
    let copy = directory.path().join("publisher.toml");
    fs::write(&copy, &printed)?;
    let output = render(&copy, &[])?;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8(output.stdout)?, printed);
    Ok(())
}

#[test]
fn listener_history_configuration_requires_one_location_and_an_independent_key() -> Result<()> {
    let files = tempfile::tempdir()?;
    let base = files.path().join("listener.toml");
    fs::write(
        &base,
        r#"[storage]
path = "/unused/canonical"
secondary_path = "/unused/secondary"
[compat]
serving = "compact-blocks"
"#,
    )?;
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
async fn publisher_with_light_wallet_profile_publishes_under_every_blob_policy() -> Result<()> {
    for retention in [
        RawBlobRetention::None,
        RawBlobRetention::Transactions,
        RawBlobRetention::All,
    ] {
        let files = tempfile::tempdir()?;
        let (private, public) = shielded_catalog_keys(files.path())?;
        let catalog = files.path().join("catalog");
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .extend_blocks(2)
            .with_raw_blob_retention(retention);
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain),
            &sample_regtest_upgrade_activations(),
        )?;
        let output = publication_command(
            writer.canonical_primary_path(),
            &files.path().join("secondary"),
            &catalog,
            &private,
        )
        .env("ZINDER_DEPLOYMENT__PROFILE", "light-wallet")
        .output()
        .await?;
        assert!(output.status.success(), "{retention:?}: {output:?}");
        let mut store = writer.serving_store_fixture()?;
        let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
        PublishedShieldedHistory::open(&catalog.join("current.json"), &public, false, &reader)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn publisher_follows_settlement_and_resumes_a_checkpointed_store() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("publisher-secondary");
    let manifest = catalog.join("current.json");
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
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
    let mut current = wait_for_change(&manifest, &[]).await?;
    let own = scratch_reservation(&scratch, None)?;
    let other_catalog = files.path().join("other-catalog");
    let mut other = follow_command(
        writer.canonical_primary_path(),
        &scratch,
        &other_catalog,
        &private,
    )
    .spawn()?;
    wait_for_change(&other_catalog.join("current.json"), &[]).await?;
    let other_scratch = scratch_reservation(&scratch, Some(&own))?;
    let other_file = other_scratch.join("canonical/other-publisher");
    fs::write(&other_file, b"active")?;
    let operator_file = scratch.join(".tmp-operator");
    fs::write(&operator_file, b"operator")?;
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let admitted = PublishedShieldedHistory::open(&manifest, &public, false, &reader).await?;
    for _ in 0..3 {
        publisher.kill().await?;
        let abandoned = own.join("canonical/interrupted");
        fs::write(&abandoned, b"abandoned")?;
        publisher = command.spawn()?;
        chain = chain.extend_blocks(2);
        writer.append_settled_blocks(&chain)?;
        current = wait_for_change(&manifest, &current).await?;
        assert!(!abandoned.try_exists()?);
        assert_eq!(fs::read_dir(&scratch)?.count(), 3);
        assert_eq!(fs::read_dir(&own)?.count(), 2);
        assert_eq!(fs::read(&other_file)?, b"active");
        assert_eq!(fs::read(&operator_file)?, b"operator");
    }
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    assert!(admitted.refresh(&reader).await?);
    tokio::time::timeout(Duration::from_secs(10), async {
        while fs::read(other_catalog.join("current.json"))? != current {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, io::Error>(())
    })
    .await??;
    assert!(other.try_wait()?.is_none());
    other.kill().await?;
    publisher.kill().await?;
    Ok(())
}

fn scratch_reservation(root: &Path, except: Option<&Path>) -> io::Result<PathBuf> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() && Some(path.as_path()) != except {
            return Ok(path);
        }
    }
    Err(io::Error::other("missing publisher scratch reservation"))
}

#[cfg(unix)]
#[tokio::test]
async fn publisher_recovers_a_missing_table_while_holding_the_catalog_lock() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let key = fs::read(public)?;
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
    let paused = signed_segments(&manifest, &key)?;
    // With more than 77 untouched table files, this generation must read a
    // retired file during publication, before it can finish and catch up.
    eyre::ensure!(
        paused.len() <= 24,
        "publisher advanced too far before suspension"
    );
    let generation = scratch_reservation(&scratch, None)?.join("canonical");
    let previous = fs::File::open(&generation)?;
    let previous_inode = previous.metadata()?.ino();
    compact_away_the_old_tables(&db).await?;
    assert!(
        Command::new("kill")
            .args(["-CONT", &pid])
            .status()
            .await?
            .success()
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match generation.metadata() {
                Ok(metadata) if metadata.ino() != previous_inode => break,
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
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
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = signed_segments(&manifest, &key)?;
            if observed.last().and_then(|entry| entry["last"].as_u64()) == Some(128) {
                return Ok::<_, eyre::Report>(observed);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert!(completed.starts_with(&paused));
    assert!(process.try_wait()?.is_none());
    process.kill().await?;
    Ok(())
}

#[cfg(unix)]
fn signed_segments(manifest: &Path, key: &[u8]) -> Result<Vec<serde_json::Value>> {
    #[derive(serde::Deserialize)]
    struct SignedCatalog {
        manifest: String,
        signature: String,
    }
    #[derive(serde::Deserialize)]
    struct Catalog {
        segments: Vec<serde_json::Value>,
    }
    let signed: SignedCatalog = serde_json::from_slice(&fs::read(manifest)?)?;
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, key)
        .verify(signed.manifest.as_bytes(), &hex::decode(signed.signature)?)
        .map_err(|_| eyre::eyre!("invalid publisher signature"))?;
    Ok(serde_json::from_str::<Catalog>(&signed.manifest)?.segments)
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
    let mut command = publication_command(primary, scratch, catalog, private);
    command.args(["--follow-interval-secs", "1"]);
    command
}

fn publication_command(primary: &Path, scratch: &Path, catalog: &Path, private: &Path) -> Command {
    let mut command = Command::from(super::zinder_compat_command());
    command
        .kill_on_drop(true)
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .arg("--config")
        .arg(zinder_testkit::manifest_dir!().join("../../deploy/config/history-publisher.toml"))
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
        .args(["--segment-blocks", "2"]);

    command
}

#[tokio::test]
async fn publisher_creates_nested_missing_directory_and_secondary_root() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;

    let nested_catalog = files.path().join("nested/catalog/dir");
    let nested_scratch = files.path().join("nested/secondary/root");
    let success = publication_command(
        writer.canonical_primary_path(),
        &nested_scratch,
        &nested_catalog,
        &private,
    )
    .output()
    .await?;
    assert!(success.status.success(), "{success:?}");
    assert!(nested_catalog.join("current.json").is_file());
    assert!(nested_scratch.is_dir());
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    PublishedShieldedHistory::open(
        &nested_catalog.join("current.json"),
        &public,
        false,
        &reader,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn publisher_reports_exact_paths_and_os_errors_for_missing_inputs() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, _) = shielded_catalog_keys(files.path())?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("secondary");
    let missing_key = files.path().join("missing/nested/publisher.key");
    let missing_primary = files.path().join("missing/canonical/primary");
    let blocker = files.path().join("blocker");
    fs::write(&blocker, b"not-a-directory")?;

    let cases = [
        (
            writer.canonical_primary_path().to_path_buf(),
            scratch.clone(),
            catalog.clone(),
            missing_key.clone(),
            missing_key.display().to_string(),
        ),
        (
            missing_primary.clone(),
            scratch.clone(),
            catalog.clone(),
            private.clone(),
            missing_primary.display().to_string(),
        ),
        (
            writer.canonical_primary_path().to_path_buf(),
            scratch,
            blocker.join("catalog"),
            private.clone(),
            blocker.display().to_string(),
        ),
        (
            writer.canonical_primary_path().to_path_buf(),
            blocker.join("secondary"),
            catalog,
            private,
            blocker.join("secondary").display().to_string(),
        ),
    ];

    for (primary_path, scratch_path, catalog_path, key_path, expected_path) in cases {
        let output = publication_command(&primary_path, &scratch_path, &catalog_path, &key_path)
            .output()
            .await?;
        assert!(!output.status.success(), "{expected_path}: {output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(&expected_path), "{stderr}");
    }
    Ok(())
}

#[tokio::test]
async fn publisher_rejects_oversized_and_non_regular_signing_keys() -> Result<()> {
    let files = tempfile::tempdir()?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("secondary");

    let oversized_key = files.path().join("oversized.key");
    fs::write(&oversized_key, vec![0xab; 512])?;
    let key_directory = files.path().join("key-directory");
    fs::create_dir(&key_directory)?;

    #[cfg(unix)]
    let invalid_keys = {
        let fifo_key = files.path().join("signing-key.fifo");
        let mkfifo_status = std::process::Command::new("mkfifo")
            .arg(&fifo_key)
            .status()?;
        assert!(mkfifo_status.success(), "mkfifo failed: {mkfifo_status}");
        vec![oversized_key, key_directory, fifo_key]
    };
    #[cfg(not(unix))]
    let invalid_keys = vec![oversized_key, key_directory];

    for invalid_key in invalid_keys {
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            publication_command(
                writer.canonical_primary_path(),
                &scratch,
                &catalog,
                &invalid_key,
            )
            .output(),
        )
        .await??;
        assert!(!output.status.success(), "{invalid_key:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains(&invalid_key.display().to_string()),
            "{stderr}"
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn publisher_accepts_symlinked_catalog_directory() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;

    let target_catalog = files.path().join("real-catalog");
    fs::create_dir(&target_catalog)?;
    let linked_catalog = files.path().join("linked-catalog");
    std::os::unix::fs::symlink(&target_catalog, &linked_catalog)?;
    let scratch = files.path().join("secondary");

    let output = publication_command(
        writer.canonical_primary_path(),
        &scratch,
        &linked_catalog,
        &private,
    )
    .output()
    .await?;
    assert!(output.status.success(), "{output:?}");
    assert!(target_catalog.join("current.json").is_file());
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    PublishedShieldedHistory::open(
        &linked_catalog.join("current.json"),
        &public,
        false,
        &reader,
    )
    .await?;

    let plain_file = files.path().join("plain-file");
    fs::write(&plain_file, b"not-a-directory")?;
    let linked_file = files.path().join("linked-file");
    std::os::unix::fs::symlink(&plain_file, &linked_file)?;
    let rejected = publication_command(
        writer.canonical_primary_path(),
        &scratch,
        &linked_file,
        &private,
    )
    .output()
    .await?;
    assert!(!rejected.status.success(), "{rejected:?}");
    let stderr = String::from_utf8(rejected.stderr)?;
    assert!(
        stderr.contains(&linked_file.display().to_string()),
        "{stderr}"
    );
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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??)
}

#[tokio::test]
async fn publisher_rejects_out_of_range_workers_argument() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, _) = shielded_catalog_keys(files.path())?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("secondary");
    for bad_workers in ["0", "17"] {
        let rejected =
            publication_command(&files.path().join("primary"), &scratch, &catalog, &private)
                .args(["--workers", bad_workers])
                .output()
                .await?;
        assert!(!rejected.status.success(), "{bad_workers}: {rejected:?}");
    }
    Ok(())
}

#[tokio::test]
async fn publisher_workers_4_produces_identical_catalog_to_workers_1() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(12)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;

    let seq_catalog = files.path().join("catalog-seq");
    let par_catalog = files.path().join("catalog-par");
    let seq_output = publication_command(
        writer.canonical_primary_path(),
        &files.path().join("secondary-seq"),
        &seq_catalog,
        &private,
    )
    .args(["--workers", "1"])
    .output()
    .await?;
    assert!(seq_output.status.success(), "{seq_output:?}");

    let par_output = publication_command(
        writer.canonical_primary_path(),
        &files.path().join("secondary-par"),
        &par_catalog,
        &private,
    )
    .args(["--workers", "4"])
    .output()
    .await?;
    assert!(par_output.status.success(), "{par_output:?}");

    assert_eq!(
        fs::read(seq_catalog.join("current.json"))?,
        fs::read(par_catalog.join("current.json"))?
    );
    assert_eq!(
        fs::read(seq_catalog.join(".publisher-head.json"))?,
        fs::read(par_catalog.join(".publisher-head.json"))?
    );
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    PublishedShieldedHistory::open(&par_catalog.join("current.json"), &public, false, &reader)
        .await?;
    let key = fs::read(&public)?;
    let entries = signed_segments(&par_catalog.join("current.json"), &key)?;
    assert_eq!(entries.len(), 6);
    assert_eq!(
        entries.last().and_then(|entry| entry["last"].as_u64()),
        Some(12)
    );
    let mut segment_count = 0_usize;
    for entry in fs::read_dir(&seq_catalog)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name.starts_with("segment-") && name.ends_with(".bin") {
            segment_count += 1;
            assert_eq!(
                fs::read(entry.path())?,
                fs::read(par_catalog.join(&file_name))?
            );
        }
    }
    assert_eq!(segment_count, 6);
    Ok(())
}

#[tokio::test]
async fn publisher_workers_follow_mode_publishes_initial_and_incremental() -> Result<()> {
    let files = tempfile::tempdir()?;
    let (private, public) = shielded_catalog_keys(files.path())?;
    let key = fs::read(&public)?;
    let catalog = files.path().join("catalog");
    let scratch = files.path().join("secondary");
    let manifest = catalog.join("current.json");
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(6)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;

    let mut command = follow_command(
        writer.canonical_primary_path(),
        &scratch,
        &catalog,
        &private,
    );
    command.args(["--workers", "4"]);
    let mut process = command.spawn()?;

    let initial_manifest = wait_for_change(&manifest, &[]).await?;
    let mut store = writer.serving_store_fixture()?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let admitted = PublishedShieldedHistory::open(&manifest, &public, false, &reader).await?;
    let initial_entries = signed_segments(&manifest, &key)?;
    assert_eq!(initial_entries.len(), 3);
    assert_eq!(
        initial_entries
            .last()
            .and_then(|entry| entry["last"].as_u64()),
        Some(6)
    );

    chain = chain.extend_blocks(2);
    writer.append_settled_blocks(&chain)?;
    wait_for_change(&manifest, &initial_manifest).await?;

    let mut refreshed_store = writer.serving_store_fixture()?;
    let refreshed_reader = RocksDbCanonicalReader::new(refreshed_store.take_readers()?.0)?;
    assert!(admitted.refresh(&refreshed_reader).await?);
    let updated_entries = signed_segments(&manifest, &key)?;
    assert_eq!(updated_entries.len(), 4);
    assert_eq!(
        updated_entries
            .last()
            .and_then(|entry| entry["last"].as_u64()),
        Some(8)
    );
    process.kill().await?;
    Ok(())
}
