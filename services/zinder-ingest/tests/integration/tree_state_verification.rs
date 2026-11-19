//! The tree-state verification pass over a published store whose blocks
//! carry note commitments.
//!
//! The baseline publishes 360 blocks with a checkpoint every 100 blocks and at
//! its tip, and settles at the tip, so one cycle verifies the rows at 0, 100,
//! 200, 300, and 360. The canonical primary is closed before the pass runs, so
//! a test can damage a stored row first.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    HistoricalWorkGate, TREE_STATE_VERIFICATION_WATERMARK_FILE, TreeStateVerificationConfig,
    TreeStateVerificationContext, spawn_tree_state_verification_task,
};
use zinder_runtime::{ReadinessCause, ReadinessState};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbResourceBudget,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations, with_note_commitments,
};

use super::tree_state_densification::following_tip_gate;

const BASELINE_TIP: u32 = 360;
const TEST_REORG_WINDOW: u32 = 100;
const PASS_DEADLINE: Duration = Duration::from_mins(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_over_an_intact_store_completes_its_cycle_and_keeps_ingest_ready() -> eyre::Result<()>
{
    let store = PublishedStore::new()?;
    let gate = following_tip_gate();

    store.run_until_cycle_completes(&gate).await?;

    eyre::ensure!(
        gate.readiness().admission_state().cause != ReadinessCause::StorageUnavailable,
        "an intact store must not withdraw readiness"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_whose_root_does_not_match_withdraws_readiness_and_ends_the_pass() -> eyre::Result<()>
{
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(200)?;
    let gate = following_tip_gate();

    let task = store.spawn_pass(&gate, CancellationToken::new())?;
    tokio::time::timeout(PASS_DEADLINE, task)
        .await
        .map_err(|_| eyre::eyre!("the pass must end on its own once it finds damage"))??;

    let readiness = gate.readiness();
    eyre::ensure!(
        readiness.runtime_readiness().report().cause == ReadinessCause::StorageUnavailable,
        "store damage must withdraw readiness with storage_unavailable"
    );
    readiness.publish_admission(ReadinessState::ready(Some(BASELINE_TIP)));
    eyre::ensure!(
        readiness.runtime_readiness().report().cause == ReadinessCause::StorageUnavailable,
        "a later admission write must not restore readiness"
    );
    eyre::ensure!(
        store.watermark()?["next_height"] == 200,
        "the pass must record the damaged row so a restart finds it again"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_resumes_its_cycle_at_the_recorded_height() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    // Damage below the recorded height is outside the resumed cycle.
    store.flip_first_root_byte(100)?;
    store.write_watermark(200)?;
    let gate = following_tip_gate();

    store.run_until_cycle_completes(&gate).await?;

    eyre::ensure!(
        gate.readiness().admission_state().cause != ReadinessCause::StorageUnavailable,
        "the resumed cycle must start above the damaged row"
    );
    Ok(())
}

/// A published store whose canonical primary is closed.
struct PublishedStore {
    _serving: WalletServingStoreFixture,
    primary_path: PathBuf,
    manifest_sha256: String,
    activations: NetworkUpgradeActivations,
    secondary_root: tempfile::TempDir,
}

impl PublishedStore {
    fn new() -> eyre::Result<Self> {
        let activations = sample_regtest_upgrade_activations();
        let chain = with_note_commitments(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(BASELINE_TIP),
            &activations,
        )?;
        let fixture =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
        let primary_path = fixture.canonical_primary_path().to_path_buf();
        let manifest_sha256 = hex::encode(
            fixture
                .canonical_primary()?
                .ready_evidence()
                .construction_manifest_sha256,
        );
        Ok(Self {
            _serving: fixture.into_serving_store_fixture()?,
            primary_path,
            manifest_sha256,
            activations,
            secondary_root: tempfile::tempdir()?,
        })
    }

    fn spawn_pass(
        &self,
        gate: &HistoricalWorkGate,
        cancel: CancellationToken,
    ) -> eyre::Result<tokio::task::JoinHandle<()>> {
        Ok(spawn_tree_state_verification_task(
            TreeStateVerificationContext {
                canonical_primary_path: self.primary_path.clone(),
                secondary_path: self.secondary_root.path().join("secondary"),
                network_upgrade_activations: Arc::new(self.activations.clone()),
                workload: CanonicalStoreWorkload::Wallet,
                raw_blob_retention: RawBlobRetention::None,
                reorg_policy: CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
                resource_budget: RocksDbResourceBudget::for_local_tests(),
                gate: gate.clone(),
                config: TreeStateVerificationConfig {
                    enabled: true,
                    max_rows_per_second: std::num::NonZeroU32::MAX,
                },
            },
            cancel,
        ))
    }

    /// Runs the pass until it completes a cycle, which records that the next
    /// cycle starts from height 0.
    async fn run_until_cycle_completes(&self, gate: &HistoricalWorkGate) -> eyre::Result<()> {
        let cancel = CancellationToken::new();
        let task = self.spawn_pass(gate, cancel.clone())?;
        let completed = tokio::time::timeout(PASS_DEADLINE, async {
            loop {
                if self
                    .watermark()
                    .is_ok_and(|watermark| watermark["next_height"] == 0)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        cancel.cancel();
        task.await?;
        completed.map_err(|_| eyre::eyre!("the pass did not complete a cycle in time"))
    }

    fn watermark_path(&self) -> PathBuf {
        self.primary_path
            .join(TREE_STATE_VERIFICATION_WATERMARK_FILE)
    }

    fn watermark(&self) -> eyre::Result<serde_json::Value> {
        Ok(serde_json::from_slice(&std::fs::read(
            self.watermark_path(),
        )?)?)
    }

    fn write_watermark(&self, next_height: u32) -> eyre::Result<()> {
        std::fs::write(
            self.watermark_path(),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "construction_manifest_sha256": self.manifest_sha256,
                "next_height": next_height,
            }))?,
        )?;
        Ok(())
    }

    /// Flips a byte of the first stored root of the row at `height`, which
    /// follows the block time and the presence bitmap.
    fn flip_first_root_byte(&self, height: u32) -> eyre::Result<()> {
        rewrite_checkpoint_row(&self.primary_path, height, |row| {
            let root_byte = row
                .get_mut(5)
                .ok_or_else(|| eyre::eyre!("checkpoint row holds no root"))?;
            *root_byte ^= 0xff;
            Ok(())
        })
    }
}

fn rewrite_checkpoint_row(
    primary_path: &Path,
    height: u32,
    rewrite: impl FnOnce(&mut Vec<u8>) -> eyre::Result<()>,
) -> eyre::Result<()> {
    let options = rust_rocksdb::Options::default();
    let column_families = rust_rocksdb::DB::list_cf(&options, primary_path)?;
    let db = rust_rocksdb::DB::open_cf(&options, primary_path, &column_families)?;
    let family = db
        .cf_handle("tree_state_checkpoint")
        .ok_or_else(|| eyre::eyre!("a canonical store owns the tree-state checkpoint family"))?;
    let key = zinder_core::wire::encode_height_key_ascending(BlockHeight::new(height));
    let mut row = db
        .get_cf(&family, key)?
        .ok_or_else(|| eyre::eyre!("height {height} holds no checkpoint"))?;
    rewrite(&mut row)?;
    db.put_cf(&family, key, &row)?;
    db.flush_cf(&family)?;
    Ok(())
}
