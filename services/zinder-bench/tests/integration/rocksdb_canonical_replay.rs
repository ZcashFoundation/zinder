#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{num::NonZeroU32, process::Command};

use eyre::{Result, eyre};
use tempfile::tempdir;
use zinder_bench::canonical_replay_storage::rocksdb::{
    RocksDbCanonicalReplayConfig, run_rocksdb_canonical_replay_storage,
    validate_rocksdb_canonical_replay_storage_with_fresh_open,
};
use zinder_bench::fixture::FixtureManifest;
use zinder_core::{CanonicalBlockReplayFormatVersion, Network};
use zinder_store::{RawBlobRetention, RocksDbResourceBudget, TreeStateCheckpointSelection};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
};

use crate::common::write_regtest_fixture;

#[test]
fn rocksdb_store_properties_reports_the_resolved_construction_budget() -> Result<()> {
    let (fixture_directory, _) = write_regtest_fixture()?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(4);
    let budget = NonZeroU32::new(256).ok_or_else(|| eyre!("budget must be positive"))?;
    let baseline = CanonicalBaseline::new(&chain).with_tree_state_checkpoint_selection(
        TreeStateCheckpointSelection::CommitmentBudget(budget),
    );
    let store = CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?;
    let scratch = tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_zinder-bench"))
        .arg("rocksdb-store-properties")
        .arg("--fixture")
        .arg(fixture_directory.path())
        .arg("--canonical-store")
        .arg(store.canonical_primary_path())
        .arg("--secondary-root")
        .arg(scratch.path().join("secondary"))
        .args([
            "--software-revision",
            "test",
            "--canonical-io-mode",
            "buffered",
        ])
        .arg("--supported-reorg-depth")
        .arg(
            store
                .canonical_primary()?
                .reorg_policy()
                .reorg_window_blocks()
                .to_string(),
        )
        .output()?;
    eyre::ensure!(
        output.status.success(),
        "store properties failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        report["construction_tree_state_checkpoint_policy"],
        serde_json::json!({"selection": "commitment-budget", "commitment_budget": 256})
    );
    Ok(())
}

#[tokio::test]
async fn external_sst_round_trip_publishes_validated_facts_for_a_fresh_reader() -> Result<()> {
    let (fixture_directory, block) = write_regtest_fixture()?;
    let candidate_parent = tempdir()?;
    let candidate_path = candidate_parent
        .path()
        .join("rocksdb-canonical-replay-storage");
    let rocksdb_resource_budget = RocksDbResourceBudget::for_local_tests();
    let block_prepare_concurrency = NonZeroU32::new(2).ok_or_else(|| eyre!("2 is non-zero"))?;

    let result = run_rocksdb_canonical_replay_storage(RocksDbCanonicalReplayConfig {
        fixture_directory: fixture_directory.path().to_path_buf(),
        candidate_path: candidate_path.clone(),
        block_prepare_concurrency,
        rocksdb_resource_budget,
    })
    .await?;

    assert_eq!(result.validation.block_count, 1);
    assert_eq!(result.validation.first_height, block.height);
    assert_eq!(result.validation.tip_height, block.height);
    assert_eq!(result.validation.tip_hash, block.hash);
    assert_eq!(
        result.validation.replay_format_version,
        CanonicalBlockReplayFormatVersion::CURRENT.value()
    );
    assert!(result.logical_replay_bytes > 0);
    assert!(result.external_sst_bytes > 0);
    assert!(result.physical_storage_bytes >= result.external_sst_bytes);
    assert_eq!(result.rocksdb_resource_budget, rocksdb_resource_budget);
    assert_eq!(result.block_prepare_concurrency, block_prepare_concurrency);

    let fresh_reader = validate_rocksdb_canonical_replay_storage_with_fresh_open(
        &candidate_path,
        fixture_directory.path(),
        rocksdb_resource_budget,
    )?;
    assert_eq!(fresh_reader, result.validation);

    Ok(())
}

#[tokio::test]
async fn fixture_oracle_mismatch_leaves_the_candidate_unpublished() -> Result<()> {
    let (fixture_directory, _) = write_regtest_fixture()?;
    let mut manifest = FixtureManifest::read(fixture_directory.path())?;
    manifest
        .canonical_block_facts_digest_evidence
        .sequence_digest_sha256 = "00".repeat(32);
    manifest.write(fixture_directory.path())?;

    let candidate_parent = tempdir()?;
    let candidate_path = candidate_parent
        .path()
        .join("rocksdb-canonical-replay-storage");
    let rocksdb_resource_budget = RocksDbResourceBudget::for_local_tests();
    let block_prepare_concurrency = NonZeroU32::new(2).ok_or_else(|| eyre!("2 is non-zero"))?;
    let Err(round_trip_error) =
        run_rocksdb_canonical_replay_storage(RocksDbCanonicalReplayConfig {
            fixture_directory: fixture_directory.path().to_path_buf(),
            candidate_path: candidate_path.clone(),
            block_prepare_concurrency,
            rocksdb_resource_budget,
        })
        .await
    else {
        return Err(eyre!("mismatched fixture oracle must reject publication"));
    };
    assert!(round_trip_error.to_string().contains("sequence digest"));

    let Err(fresh_reader_error) = validate_rocksdb_canonical_replay_storage_with_fresh_open(
        &candidate_path,
        fixture_directory.path(),
        rocksdb_resource_budget,
    ) else {
        return Err(eyre!(
            "an unpublished candidate must not reopen as complete"
        ));
    };
    assert!(
        fresh_reader_error
            .to_string()
            .contains("completion marker is absent")
    );

    Ok(())
}

#[tokio::test]
async fn existing_candidate_directory_is_rejected_without_modification() -> Result<()> {
    let (fixture_directory, _) = write_regtest_fixture()?;
    let existing_candidate = tempdir()?;
    let sentinel_path = existing_candidate.path().join("operator-owned");
    std::fs::write(&sentinel_path, b"preserve me")?;
    let block_prepare_concurrency = NonZeroU32::new(2).ok_or_else(|| eyre!("2 is non-zero"))?;

    let Err(error) = run_rocksdb_canonical_replay_storage(RocksDbCanonicalReplayConfig {
        fixture_directory: fixture_directory.path().to_path_buf(),
        candidate_path: existing_candidate.path().to_path_buf(),
        block_prepare_concurrency,
        rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
    })
    .await
    else {
        return Err(eyre!("an existing candidate directory must be rejected"));
    };

    assert!(error.to_string().contains("require a fresh path"));
    assert_eq!(std::fs::read(&sentinel_path)?, b"preserve me");
    Ok(())
}
