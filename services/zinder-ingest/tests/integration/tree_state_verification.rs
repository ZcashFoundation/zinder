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
use zinder_core::{BlockHeight, Network, NetworkUpgradeActivations, ShieldedProtocol};
use zinder_ingest::{
    CanonicalStoreDamage, CanonicalStoreDamageSignal, CanonicalStoreDamageSite, HistoricalWorkGate,
    MaterializedViewReplayConfig, MaterializedViewTailer, ReplayPrevoutCaches,
    TREE_STATE_VERIFICATION_WATERMARK_FILE, TreeStateVerificationConfig,
    TreeStateVerificationContext, spawn_materialized_view_tailer_task,
    spawn_tree_state_verification_task,
};
use zinder_runtime::{ComposedReadiness, IngestPhase, Readiness, ReadinessCause, ReadinessState};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations, with_note_commitments,
};

use super::tree_state_densification::following_tip_gate;

const BASELINE_TIP: u32 = 360;
const TEST_REORG_WINDOW: u32 = 100;
const PASS_DEADLINE: Duration = Duration::from_mins(2);
/// The row the damaging tests break, whose first stored root is Sapling's.
const DAMAGED_HEIGHT: u32 = 200;

fn damaged_row() -> CanonicalStoreDamage {
    CanonicalStoreDamage {
        site: CanonicalStoreDamageSite::Row(BlockHeight::new(DAMAGED_HEIGHT)),
        pool: Some(ShieldedProtocol::Sapling),
    }
}

/// The record a pass that found the damaged row leaves.
fn damaged_row_record(store: &PublishedStore) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "construction_manifest_sha256": store.manifest_sha256,
        "next_height": DAMAGED_HEIGHT,
        "damaged": {"site": "row", "height": DAMAGED_HEIGHT, "pool": "sapling"},
    })
}

fn damaged_cause() -> ReadinessCause {
    damaged_row().readiness_cause()
}

/// Damage at the damaged row that fails outside any pool, such as a block
/// time that differs from its block's.
fn damaged_without_a_pool() -> CanonicalStoreDamage {
    CanonicalStoreDamage {
        site: CanonicalStoreDamageSite::Row(BlockHeight::new(DAMAGED_HEIGHT)),
        pool: None,
    }
}

/// The record a pass that found [`damaged_without_a_pool`] leaves.
fn damaged_without_a_pool_record(store: &PublishedStore) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "construction_manifest_sha256": store.manifest_sha256,
        "next_height": DAMAGED_HEIGHT,
        "damaged": {"site": "row", "height": DAMAGED_HEIGHT},
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_over_an_intact_store_completes_its_cycle_and_keeps_ingest_ready() -> eyre::Result<()>
{
    let store = PublishedStore::new()?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();

    store
        .run_until_cycle_completes(&gate, &store_damage)
        .await?;

    eyre::ensure!(
        gate.readiness().admission_state().cause == ReadinessCause::Starting,
        "an intact store must leave readiness to the writer"
    );
    eyre::ensure!(
        store_damage.current().is_none(),
        "an intact store reports no damage"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_damaged_row_is_reported_to_readers_and_parks_readiness() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();

    store.run_until_the_pass_ends(&gate, &store_damage).await?;

    eyre::ensure!(
        store_damage.current() == Some(damaged_row()),
        "the pass must report the damaged row and its pool, got {:?}",
        store_damage.current()
    );
    let readiness = gate.readiness();
    eyre::ensure!(
        readiness.runtime_readiness().report().cause == damaged_cause(),
        "store damage must park readiness with canonical_store_damaged"
    );
    readiness.publish_admission(ReadinessState::ready(Some(BASELINE_TIP)));
    eyre::ensure!(
        readiness.runtime_readiness().report().cause == damaged_cause(),
        "a later admission write must not restore readiness"
    );
    let watermark = store.watermark()?;
    eyre::ensure!(
        watermark == damaged_row_record(&store),
        "the pass must record the damaged row so a restart reports it again: {watermark}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_resumes_its_cycle_at_the_recorded_height() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    // Damage below the recorded height is outside the resumed cycle.
    store.flip_first_root_byte(100)?;
    store.write_watermark(&serde_json::json!({
        "version": 1,
        "construction_manifest_sha256": store.manifest_sha256,
        "next_height": DAMAGED_HEIGHT,
    }))?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);

    store
        .run_until_cycle_completes(&gate, &store_damage)
        .await?;

    eyre::ensure!(
        store_damage.current().is_none(),
        "the resumed cycle must start above the damaged row"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_blocked_on_a_settled_checkpoint_reports_damage_before_views_catch_up()
-> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_block_time_bit(DAMAGED_HEIGHT)?;
    check_damage_during_replay(
        &store,
        damaged_without_a_pool(),
        damaged_without_a_pool_record(&store),
        "block time",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sparse_replay_with_a_damaged_seed_reports_the_seed_height() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    check_damage_during_replay(&store, damaged_row(), damaged_row_record(&store), "root").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_replay_reports_an_active_frontier_missing_from_its_predecessor() -> eyre::Result<()>
{
    let store = PublishedStore::new()?;
    let (_directory, views) =
        super::materialized_view_replay::view_store().map_err(|error| eyre::eyre!("{error}"))?;
    // Stop a real replay after it persists height 200, then repair its next page.
    store.flip_block_time_bit(300)?;
    let tailer = store.replay_tailer(views.clone())?;
    eyre::ensure!(matches!(
        tailer.catch_up(),
        Err(zinder_ingest::IngestError::CanonicalStore(_))
    ));
    drop(tailer);
    eyre::ensure!(
        views.last_materialized_height_ascending(
            zinder_materialized_views::TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
        )? == Some(BlockHeight::new(DAMAGED_HEIGHT)),
        "replay must resume above the damaged seed"
    );
    store.flip_block_time_bit(300)?;
    rewrite_row(
        &store.primary_path,
        "tree_state_checkpoint",
        DAMAGED_HEIGHT,
        |row| {
            // Keep the stored block time and replace all frontiers with an empty presence bitmap.
            row.truncate(4);
            row.push(0);
            Ok(())
        },
    )?;
    let resumed = store.replay_tailer(views)?;
    check_damage_with_tailer(
        &store,
        resumed,
        damaged_row(),
        damaged_row_record(&store),
        "pool activation/frontier mismatch",
    )
    .await
}

async fn check_damage_during_replay(
    store: &PublishedStore,
    expected: CanonicalStoreDamage,
    expected_record: serde_json::Value,
    reason: &str,
) -> eyre::Result<()> {
    let (_views_dir, views) =
        super::materialized_view_replay::view_store().map_err(|error| eyre::eyre!("{error}"))?;
    let tailer = store.replay_tailer(views)?;
    check_damage_with_tailer(store, tailer, expected, expected_record, reason).await
}

async fn check_damage_with_tailer(
    store: &PublishedStore,
    tailer: MaterializedViewTailer,
    expected: CanonicalStoreDamage,
    expected_record: serde_json::Value,
    reason: &str,
) -> eyre::Result<()> {
    let error = tailer
        .catch_up()
        .err()
        .ok_or_else(|| eyre::eyre!("the malformed checkpoint must block replay"))?;
    let error = eyre::Report::new(error);
    eyre::ensure!(
        error
            .chain()
            .any(|cause| cause.to_string().contains(reason)),
        "unexpected replay error: {error:#}"
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    readiness.update_admission(|admission| admission.phase = Some(IngestPhase::FollowingTip));
    let gate = HistoricalWorkGate::new(readiness);
    let store_damage = CanonicalStoreDamageSignal::default();
    let cancel = CancellationToken::new();
    let mut disabled = store.verification_context(&gate, &store_damage)?;
    disabled.config.enabled = false;
    spawn_tree_state_verification_task(disabled, cancel.clone()).await?;
    eyre::ensure!(store_damage.current().is_none() && !store.watermark_path().exists());
    let verifier = store.spawn_pass(&gate, &store_damage, cancel.clone())?;
    let replay = spawn_materialized_view_tailer_task(
        tailer,
        Duration::from_millis(50),
        gate.clone(),
        cancel.clone(),
    );

    let reported = tokio::time::timeout(Duration::from_secs(15), verifier).await;
    cancel.cancel();
    replay.await?;
    reported.map_err(|_| {
        eyre::eyre!("a stalled replay must trigger verification of its settled checkpoint")
    })??;
    eyre::ensure!(!gate.is_open());
    eyre::ensure!(store_damage.current() == Some(expected));
    eyre::ensure!(store.watermark()? == expected_record);
    Ok(())
}

/// A restart reports the recorded damage before any writer status attests
/// the store, and confirms it while historical work is still closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_reports_recorded_damage_at_once_and_confirms_it_without_historical_work()
-> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    store
        .run_until_the_pass_ends(
            &following_tip_gate(),
            &CanonicalStoreDamageSignal::default(),
        )
        .await?;

    let restarted_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    eyre::ensure!(
        restarted_damage.current() == Some(damaged_row()),
        "a restart must report the recorded damage before the pass runs"
    );
    let closed_gate = closed_gate();
    store
        .run_until_the_pass_ends(&closed_gate, &restarted_damage)
        .await?;

    eyre::ensure!(
        closed_gate.readiness().admission_state().cause == damaged_cause(),
        "the pass must confirm recorded damage before historical work opens"
    );
    eyre::ensure!(restarted_damage.current() == Some(damaged_row()));
    Ok(())
}

/// Recorded damage that the row no longer shows stops being reported once
/// the pass checks it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_damage_the_row_no_longer_shows_is_cleared() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.write_watermark(&damaged_row_record(&store))?;
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    eyre::ensure!(store_damage.current() == Some(damaged_row()));
    let closed_gate = closed_gate();
    let cancel = CancellationToken::new();
    let task = store.spawn_pass(&closed_gate, &store_damage, cancel.clone())?;

    let cleared = wait_until(|| store_damage.current().is_none()).await;
    cancel.cancel();
    task.await?;

    cleared.map_err(|_| eyre::eyre!("an intact row must clear the recorded damage"))?;
    eyre::ensure!(
        store.watermark()?.get("damaged").is_none(),
        "the cleared damage must leave the record"
    );
    eyre::ensure!(
        closed_gate.readiness().admission_state().cause == ReadinessCause::Starting,
        "cleared damage must not park readiness"
    );
    Ok(())
}

/// A row whose only change is its block time is damage outside any pool,
/// since a wallet would otherwise be served a time its block does not carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_whose_block_time_differs_from_its_block_is_reported_without_a_pool()
-> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_block_time_bit(DAMAGED_HEIGHT)?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();

    store.run_until_the_pass_ends(&gate, &store_damage).await?;

    eyre::ensure!(
        store_damage.current() == Some(damaged_without_a_pool()),
        "the pass must report the row with another block time, got {:?}",
        store_damage.current()
    );
    eyre::ensure!(
        gate.readiness().runtime_readiness().report().cause
            == damaged_without_a_pool().readiness_cause()
    );
    eyre::ensure!(store.watermark()? == damaged_without_a_pool_record(&store));
    Ok(())
}

/// Recorded damage at a row that still carries another block time stays
/// reported: checking the row again checks its block time too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_damage_at_a_row_whose_block_time_still_differs_stays_reported() -> eyre::Result<()>
{
    let store = PublishedStore::new()?;
    store.flip_block_time_bit(DAMAGED_HEIGHT)?;
    store.write_watermark(&damaged_without_a_pool_record(&store))?;
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    let closed_gate = closed_gate();

    store
        .run_until_the_pass_ends(&closed_gate, &store_damage)
        .await?;

    eyre::ensure!(
        store_damage.current() == Some(damaged_without_a_pool()),
        "a row that still carries another block time must stay reported, got {:?}",
        store_damage.current()
    );
    eyre::ensure!(
        closed_gate.readiness().admission_state().cause
            == damaged_without_a_pool().readiness_cause()
    );
    eyre::ensure!(store.watermark()? == damaged_without_a_pool_record(&store));
    Ok(())
}

/// A canonical header that does not decode is damage at the row whose block
/// time it checks, so the pass reports it instead of retrying that row
/// forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_whose_block_header_does_not_decode_is_reported_without_a_pool() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    rewrite_row(
        &store.primary_path,
        "block_header",
        DAMAGED_HEIGHT,
        |header| {
            *header = vec![0xff; 3];
            Ok(())
        },
    )?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();

    store.run_until_the_pass_ends(&gate, &store_damage).await?;

    eyre::ensure!(
        store_damage.current() == Some(damaged_without_a_pool()),
        "the pass must report the row whose header does not decode, got {:?}",
        store_damage.current()
    );
    eyre::ensure!(store.watermark()? == damaged_without_a_pool_record(&store));
    Ok(())
}

/// Damage found while the writer is parked on a deep reorg is still reported
/// to readers, but the reorg halt stays ingest's typed cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn damage_found_under_a_reorg_halt_keeps_the_halt() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    let gate = following_tip_gate();
    let halt = ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(1_000), Some(BASELINE_TIP));
    gate.readiness().park_admission(halt.clone());
    let store_damage = CanonicalStoreDamageSignal::default();

    store.run_until_the_pass_ends(&gate, &store_damage).await?;

    eyre::ensure!(
        gate.readiness().admission_state().cause == halt.cause,
        "store damage must not replace the reorg halt"
    );
    eyre::ensure!(store_damage.current() == Some(damaged_row()));
    Ok(())
}

/// Readers learn of the damage while its record still waits on the disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn damage_is_reported_before_its_record_is_written() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    let staged = store.block_record_on_a_fifo()?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();
    let task = store.spawn_pass(&gate, &store_damage, CancellationToken::new())?;

    let reported = wait_until(|| store_damage.current().is_some()).await;
    let parked = gate.readiness().admission_state().cause == damaged_cause();
    let unwritten = !store.watermark_path().exists();
    let staged_path = staged.clone();
    let drained = tokio::task::spawn_blocking(move || std::fs::read(&staged_path)).await??;
    // A FIFO cannot be synced, so that write fails; the next one writes a
    // file once the FIFO is gone.
    let _retired = retire_fifo(&staged)?;
    tokio::time::timeout(PASS_DEADLINE, task)
        .await
        .map_err(|_| eyre::eyre!("the pass must end once its record is written"))??;

    reported.map_err(|_| eyre::eyre!("damage must be reported while its record waits"))?;
    eyre::ensure!(parked, "readiness must be parked while the record waits");
    eyre::ensure!(unwritten, "the record must still be waiting on the disk");
    eyre::ensure!(!drained.is_empty(), "the pass then writes its record");
    eyre::ensure!(store.watermark()? == damaged_row_record(&store));
    Ok(())
}

/// A damage record that fails to reach the disk is written again until it
/// does, and the damage stays reported meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_damage_record_is_written_again_until_it_succeeds() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    let staged = store.block_record_on_a_fifo()?;
    let gate = following_tip_gate();
    let store_damage = CanonicalStoreDamageSignal::default();
    let task = store.spawn_pass(&gate, &store_damage, CancellationToken::new())?;

    // Opening the FIFO returns once the pass opens it to write the record;
    // closing it at once fails that write.
    let staged_path = staged.clone();
    tokio::time::timeout(
        PASS_DEADLINE,
        tokio::task::spawn_blocking(move || std::fs::File::open(&staged_path).map(drop)),
    )
    .await
    .map_err(|_| eyre::eyre!("the pass must try to write its record"))???;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let retrying = !task.is_finished();
    let reported_while_unwritten =
        store_damage.current() == Some(damaged_row()) && !store.watermark_path().exists();
    let _retired = retire_fifo(&staged)?;
    tokio::time::timeout(PASS_DEADLINE, task)
        .await
        .map_err(|_| eyre::eyre!("the pass must end once its record is written"))??;

    eyre::ensure!(retrying, "a failed damage record must be written again");
    eyre::ensure!(
        reported_while_unwritten,
        "the damage must stay reported while its record is not written"
    );
    eyre::ensure!(
        store.watermark()? == damaged_row_record(&store),
        "the retried write must record the damage"
    );
    eyre::ensure!(store_damage.current() == Some(damaged_row()));
    eyre::ensure!(gate.readiness().admission_state().cause == damaged_cause());
    Ok(())
}

/// Damage recorded at a row clears only once that row is stored and passes.
///
/// The row at 200 is the cadence checkpoint every reconstruction from 200 to
/// 299 seeds from, so a restart that finds it gone keeps reporting the
/// damage even though every row above it passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_damage_at_a_row_that_is_gone_stays_reported() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.write_watermark(&damaged_row_record(&store))?;
    delete_checkpoint_row(&store.primary_path, DAMAGED_HEIGHT)?;
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    let closed_gate = closed_gate();

    store
        .run_until_the_pass_ends(&closed_gate, &store_damage)
        .await?;

    eyre::ensure!(
        store_damage.current() == Some(damaged_row()),
        "a missing damaged row must stay reported, got {:?}",
        store_damage.current()
    );
    eyre::ensure!(
        closed_gate.readiness().admission_state().cause == damaged_cause(),
        "a missing damaged row must park readiness"
    );
    eyre::ensure!(store.watermark()? == damaged_row_record(&store));
    Ok(())
}

/// A record ingest cannot read is damage at an unknown height, reported from
/// the start, until a full cycle passes without historical work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_record_is_damage_until_a_full_cycle_passes() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    std::fs::write(store.watermark_path(), b"{\"version\": 1, \"next_he")?;
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    eyre::ensure!(
        store_damage.current()
            == Some(CanonicalStoreDamage {
                site: CanonicalStoreDamageSite::Unknown,
                pool: None,
            }),
        "an unreadable record must be reported as damage, got {:?}",
        store_damage.current()
    );
    let closed_gate = closed_gate();
    let cancel = CancellationToken::new();
    let task = store.spawn_pass(&closed_gate, &store_damage, cancel.clone())?;

    let cleared = wait_until(|| store_damage.current().is_none()).await;
    let recorded = store.watermark();
    cancel.cancel();
    task.await?;

    cleared.map_err(|_| eyre::eyre!("a full cycle must clear damage at an unknown height"))?;
    let recorded = recorded?;
    eyre::ensure!(
        recorded["next_height"] == 0 && recorded.get("damaged").is_none(),
        "the passed cycle must replace the unreadable record: {recorded}"
    );
    eyre::ensure!(
        closed_gate.readiness().admission_state().cause == ReadinessCause::Starting,
        "a passed cycle must not park readiness"
    );
    Ok(())
}

/// The full cycle behind an unreadable record finds damage the record may
/// have held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_record_over_a_damaged_row_reports_that_row() -> eyre::Result<()> {
    let store = PublishedStore::new()?;
    store.flip_first_root_byte(DAMAGED_HEIGHT)?;
    std::fs::write(store.watermark_path(), b"not json")?;
    let store_damage = CanonicalStoreDamageSignal::recorded_in(&store.primary_path);
    let closed_gate = closed_gate();

    store
        .run_until_the_pass_ends(&closed_gate, &store_damage)
        .await?;

    eyre::ensure!(store_damage.current() == Some(damaged_row()));
    eyre::ensure!(closed_gate.readiness().admission_state().cause == damaged_cause());
    eyre::ensure!(store.watermark()? == damaged_row_record(&store));
    Ok(())
}

/// Removes the FIFO at `staged`, so the next write of the record creates a
/// file, and returns a handle that keeps a write already waiting on the FIFO
/// from waiting forever.
///
/// A write that opened the FIFO before it was removed waits for a reader, so
/// the handle opens it for reading and writing, which never blocks, before
/// removing it. That write then fails to sync, and the retry after it writes
/// the file.
fn retire_fifo(staged: &Path) -> eyre::Result<std::fs::File> {
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(staged)?;
    std::fs::remove_file(staged)?;
    Ok(reader)
}

/// A gate whose historical work never opens.
fn closed_gate() -> HistoricalWorkGate {
    HistoricalWorkGate::new(ComposedReadiness::new(Readiness::default(), &[]))
}

/// Polls `condition` until it holds or the pass deadline passes.
async fn wait_until(
    condition: impl Fn() -> bool + Send + Sync,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(PASS_DEADLINE, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
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

    fn replay_tailer(
        &self,
        views: zinder_materialized_views::MaterializedViewStore,
    ) -> eyre::Result<MaterializedViewTailer> {
        let canonical = RocksDbCanonicalSecondary::open_ready(
            &self.primary_path,
            self.secondary_root.path().join("replay"),
            &self.activations,
            zinder_store::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
            CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(MaterializedViewTailer {
            canonical: Arc::new(parking_lot::RwLock::new(canonical)),
            materialized_view_store: views,
            config: MaterializedViewReplayConfig::DEFAULT,
            activations: Arc::new(self.activations.clone()),
            reorg_window_blocks: TEST_REORG_WINDOW,
            chain_event_retention_window: None,
            cursor_at_risk_warning: Duration::from_hours(24),
            prevout_caches: ReplayPrevoutCaches::default(),
            volume_space_guard: None,
        })
    }

    fn spawn_pass(
        &self,
        gate: &HistoricalWorkGate,
        store_damage: &CanonicalStoreDamageSignal,
        cancel: CancellationToken,
    ) -> eyre::Result<tokio::task::JoinHandle<()>> {
        Ok(spawn_tree_state_verification_task(
            self.verification_context(gate, store_damage)?,
            cancel,
        ))
    }

    fn verification_context(
        &self,
        gate: &HistoricalWorkGate,
        store_damage: &CanonicalStoreDamageSignal,
    ) -> eyre::Result<TreeStateVerificationContext> {
        Ok(TreeStateVerificationContext {
            canonical_primary_path: self.primary_path.clone(),
            secondary_path: self.secondary_root.path().join("secondary"),
            network_upgrade_activations: Arc::new(self.activations.clone()),
            workload: CanonicalStoreWorkload::Wallet,
            raw_blob_retention: RawBlobRetention::None,
            reorg_policy: CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            gate: gate.clone(),
            store_damage: store_damage.clone(),
            config: TreeStateVerificationConfig {
                enabled: true,
                max_rows_per_second: std::num::NonZeroU32::MAX,
            },
        })
    }

    /// Runs the pass until it ends on its own, as it does once it finds or
    /// confirms damage.
    async fn run_until_the_pass_ends(
        &self,
        gate: &HistoricalWorkGate,
        store_damage: &CanonicalStoreDamageSignal,
    ) -> eyre::Result<()> {
        let task = self.spawn_pass(gate, store_damage, CancellationToken::new())?;
        tokio::time::timeout(PASS_DEADLINE, task)
            .await
            .map_err(|_| eyre::eyre!("the pass must end on its own once it finds damage"))??;
        Ok(())
    }

    /// Runs the pass until it completes a cycle, which records that the next
    /// cycle starts from height 0.
    async fn run_until_cycle_completes(
        &self,
        gate: &HistoricalWorkGate,
        store_damage: &CanonicalStoreDamageSignal,
    ) -> eyre::Result<()> {
        let cancel = CancellationToken::new();
        let task = self.spawn_pass(gate, store_damage, cancel.clone())?;
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

    fn write_watermark(&self, record: &serde_json::Value) -> eyre::Result<()> {
        std::fs::write(self.watermark_path(), serde_json::to_vec(record)?)?;
        Ok(())
    }

    /// Puts a FIFO where the pass stages its record and returns its path.
    ///
    /// Opening a FIFO for writing blocks until a reader opens it, so the
    /// staged write cannot finish until the test reads it, and a FIFO cannot
    /// be synced, so that write then fails.
    fn block_record_on_a_fifo(&self) -> eyre::Result<PathBuf> {
        let staged = self.watermark_path().with_extension("json.tmp");
        let fifo = std::process::Command::new("mkfifo").arg(&staged).status()?;
        eyre::ensure!(fifo.success(), "mkfifo failed");
        Ok(staged)
    }

    /// Flips a byte of the first stored root of the row at `height`, which
    /// follows the block time and the presence bitmap.
    fn flip_first_root_byte(&self, height: u32) -> eyre::Result<()> {
        rewrite_row(&self.primary_path, "tree_state_checkpoint", height, |row| {
            let root_byte = row
                .get_mut(5)
                .ok_or_else(|| eyre::eyre!("checkpoint row holds no root"))?;
            *root_byte ^= 0xff;
            Ok(())
        })
    }

    /// Flips the low bit of the block time that leads the row at `height`,
    /// which keeps a row that decodes to the same trees.
    fn flip_block_time_bit(&self, height: u32) -> eyre::Result<()> {
        rewrite_row(&self.primary_path, "tree_state_checkpoint", height, |row| {
            let block_time_byte = row
                .first_mut()
                .ok_or_else(|| eyre::eyre!("checkpoint row is empty"))?;
            *block_time_byte ^= 0x01;
            Ok(())
        })
    }
}

fn delete_checkpoint_row(primary_path: &Path, height: u32) -> eyre::Result<()> {
    let options = rust_rocksdb::Options::default();
    let column_families = rust_rocksdb::DB::list_cf(&options, primary_path)?;
    let db = rust_rocksdb::DB::open_cf(&options, primary_path, &column_families)?;
    let family = db
        .cf_handle("tree_state_checkpoint")
        .ok_or_else(|| eyre::eyre!("a canonical store owns the tree-state checkpoint family"))?;
    db.delete_cf(
        &family,
        zinder_core::wire::encode_height_key_ascending(BlockHeight::new(height)),
    )?;
    db.flush_cf(&family)?;
    Ok(())
}

/// Rewrites the row the canonical family `family_name` stores at `height`.
fn rewrite_row(
    primary_path: &Path,
    family_name: &str,
    height: u32,
    rewrite: impl FnOnce(&mut Vec<u8>) -> eyre::Result<()>,
) -> eyre::Result<()> {
    let options = rust_rocksdb::Options::default();
    let column_families = rust_rocksdb::DB::list_cf(&options, primary_path)?;
    let db = rust_rocksdb::DB::open_cf(&options, primary_path, &column_families)?;
    let family = db
        .cf_handle(family_name)
        .ok_or_else(|| eyre::eyre!("a canonical store owns the {family_name} family"))?;
    let key = zinder_core::wire::encode_height_key_ascending(BlockHeight::new(height));
    let mut row = db
        .get_cf(&family, key)?
        .ok_or_else(|| eyre::eyre!("height {height} holds no row"))?;
    rewrite(&mut row)?;
    db.put_cf(&family, key, &row)?;
    db.flush_cf(&family)?;
    Ok(())
}
