//! The Vizor wallet's sync-engine request pattern.
//!
//! Vizor downloads scan batches sized by a byte budget. Each batch streams its
//! blocks with a pool filter naming Sapling, Orchard, and Ironwood, and asks
//! for the tree state below its first block at the same time. Once a batch
//! arrives, the engine starts downloads of up to four batches beyond it, then
//! one more each time it takes the next batch from that queue. Batches sized
//! by bytes end wherever the budget runs out, so after the first budget-cut
//! batch the tree-state heights leave every fixed grid.

use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};

use clap::ValueEnum;
use serde::Serialize;
use tokio::task::JoinHandle;
use zinder_proto::compat::lightwalletd::PoolType;

use super::{
    WalletPlan, WalletProgress, WorkloadSettings,
    calls::{self, BlockRangeCall, CallFailure, CallLimits, Connector, LwdClient},
};

/// Batches downloading ahead of the one being scanned (`PREFETCH_DEPTH`).
const PREFETCH_DEPTH: usize = 4;
/// Fewest blocks per batch (`MIN_BATCH_BLOCKS`).
const MIN_BATCH_BLOCKS: u64 = 10;
/// `MOBILE_BATCH_BYTE_BUDGET` and the mobile `BATCH_SIZE_FOREGROUND`.
const MOBILE_BATCH_BYTE_BUDGET: u64 = 11_300_000;
const MOBILE_BATCH_BLOCKS: u64 = 1_000;
/// `DESKTOP_BATCH_BYTE_BUDGET` and the desktop `BATCH_SIZE_FOREGROUND`.
const DESKTOP_BATCH_BYTE_BUDGET: u64 = 31_000_000;
const DESKTOP_BATCH_BLOCKS: u64 = 10_000;
/// A queued batch whose download fails within the shortest lightwalletd
/// timeout is retried after each pause (`PREFETCH_RETRY_PAUSES`).
const PREFETCH_RETRY_PAUSES: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(4)];
const QUICK_FAILURE: Duration = Duration::from_secs(10);
/// A failed inline download ends the sync attempt; `run_sync_inner` retries
/// the attempt this many times, after 2, 4, and 8 seconds.
const SYNC_RETRIES: u32 = 3;
/// Mainnet tree states compiled into Vizor (`tree_states/mainnet_data.rs`).
///
/// There is one every 10,000 blocks from 420,000, 316 entries at the read
/// revision. A batch starting just above one reads it locally.
const COMPILED_TREE_STATE_START: u64 = 420_000;
const COMPILED_TREE_STATE_STEP: u64 = 10_000;
const COMPILED_TREE_STATE_LAST: u64 = 3_570_000;

const LIMITS: CallLimits = CallLimits {
    unary: Duration::from_secs(20),
    stream_open: Duration::from_secs(20),
    stream_idle: Some(Duration::from_secs(30)),
    stream_total: None,
};

/// Vizor platform, which fixes the batch byte budget and block cap.
#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum VizorProfile {
    /// iOS and Android: 11.3 MB batches of at most 1,000 blocks.
    Mobile,
    /// macOS, Windows, and Linux: 31 MB batches of at most 10,000 blocks.
    Desktop,
}

/// Constants and settings a Vizor-shaped run used.
#[derive(Debug, Serialize)]
pub(super) struct Parameters {
    profile: VizorProfile,
    batch_byte_budget: u64,
    max_batch_blocks: u64,
    min_batch_blocks: u64,
    prefetch_depth: usize,
    sync_retries: u32,
    limits: CallLimits,
}

impl Parameters {
    pub(super) fn new(settings: &WorkloadSettings) -> Self {
        let sizing = BatchSizing::new(settings);
        Self {
            profile: settings.vizor_profile,
            batch_byte_budget: sizing.byte_budget,
            max_batch_blocks: sizing.max_blocks,
            min_batch_blocks: MIN_BATCH_BLOCKS,
            prefetch_depth: PREFETCH_DEPTH,
            sync_retries: SYNC_RETRIES,
            limits: LIMITS,
        }
    }
}

#[derive(Clone, Copy)]
struct BatchSizing {
    byte_budget: u64,
    max_blocks: u64,
}

impl BatchSizing {
    fn new(settings: &WorkloadSettings) -> Self {
        let (profile_budget, max_blocks) = match settings.vizor_profile {
            VizorProfile::Mobile => (MOBILE_BATCH_BYTE_BUDGET, MOBILE_BATCH_BLOCKS),
            VizorProfile::Desktop => (DESKTOP_BATCH_BYTE_BUDGET, DESKTOP_BATCH_BLOCKS),
        };
        Self {
            byte_budget: settings.batch_byte_budget.unwrap_or(profile_budget),
            max_blocks,
        }
    }

    /// Last block of the batch starting at `start`: as many blocks as fit the
    /// byte budget at `received` density, between the minimum and the cap.
    ///
    /// Vizor estimates density from compiled Mainnet tree sizes, so even its
    /// first batch fits the budget. This client uses the density it has
    /// received so far, and the minimum batch until it has received a block.
    fn batch_end(self, start: u64, range_end: u64, received: (u64, u64)) -> u64 {
        let (blocks, bytes) = received;
        let fitting = bytes
            .checked_div(blocks)
            .map_or(MIN_BATCH_BLOCKS, |per_block| {
                self.byte_budget / per_block.max(1)
            });
        let span = fitting.clamp(MIN_BATCH_BLOCKS.min(self.max_blocks), self.max_blocks);
        start.saturating_add(span - 1).min(range_end)
    }
}

/// A batch downloading ahead of the scan.
struct Prefetch {
    start: u64,
    end: u64,
    download: JoinHandle<Result<(), CallFailure>>,
}

/// Syncs `plan` with Vizor's inline batch, prefetch queue, and sync retries.
pub(super) async fn sync_wallet(
    connector: Connector,
    plan: &Arc<WalletPlan>,
    settings: &WorkloadSettings,
    progress: &Arc<WalletProgress>,
) -> Result<(), CallFailure> {
    let sizing = BatchSizing::new(settings);
    // A failed connect fails the sync attempt like a failed download.
    let mut client = None;
    let mut queue = VecDeque::<Prefetch>::new();
    let mut sync_attempt = 0_u32;
    let mut start = plan.from_height;
    while start <= plan.to_height {
        let (end, downloaded) = match connector.reuse_or_connect(&mut client).await {
            Ok(connected) => {
                take_or_download(
                    &connector,
                    connected,
                    plan,
                    progress,
                    (queue.pop_front(), start, sizing),
                )
                .await
            }
            Err(failure) => (start, Err(failure)),
        };
        if let Err(failure) = downloaded {
            for batch in std::mem::take(&mut queue) {
                batch.download.abort();
            }
            sync_attempt += 1;
            if sync_attempt > SYNC_RETRIES {
                return Err(failure);
            }
            progress.note_retry();
            tokio::time::sleep(Duration::from_secs(1 << sync_attempt)).await;
            client = None;
            continue;
        }
        start = end.saturating_add(1);
        let mut next = queue
            .back()
            .map_or(start, |batch| batch.end.saturating_add(1));
        let Some(connected) = &client else { continue };
        while queue.len() < PREFETCH_DEPTH && next <= plan.to_height {
            let end = sizing.batch_end(next, plan.to_height, progress.received());
            queue.push_back(Prefetch {
                start: next,
                end,
                download: tokio::spawn(prefetch_batch(
                    connector.clone(),
                    connected.clone(),
                    Arc::clone(plan),
                    Arc::clone(progress),
                    (next, end),
                )),
            });
            next = end.saturating_add(1);
        }
    }
    Ok(())
}

/// Takes the queued batch starting at `start`, or downloads one inline when
/// none is queued or the queued download failed.
async fn take_or_download(
    connector: &Connector,
    client: &LwdClient,
    plan: &WalletPlan,
    progress: &WalletProgress,
    (queued, start, sizing): (Option<Prefetch>, u64, BatchSizing),
) -> (u64, Result<(), CallFailure>) {
    let Some(batch) = queued.filter(|batch| batch.start == start) else {
        let end = sizing.batch_end(start, plan.to_height, progress.received());
        let downloaded =
            download_batch(connector, client.clone(), plan, progress, (start, end)).await;
        return (end, downloaded);
    };
    if matches!(batch.download.await, Ok(Ok(()))) {
        return (batch.end, Ok(()));
    }
    progress.note_retry();
    let downloaded = download_batch(
        connector,
        client.clone(),
        plan,
        progress,
        (start, batch.end),
    )
    .await;
    (batch.end, downloaded)
}

async fn prefetch_batch(
    connector: Connector,
    client: LwdClient,
    plan: Arc<WalletPlan>,
    progress: Arc<WalletProgress>,
    (start, end): (u64, u64),
) -> Result<(), CallFailure> {
    retry_quick_failures(&progress, || {
        download_batch(&connector, client.clone(), &plan, &progress, (start, end))
    })
    .await
}

/// Downloads one batch's blocks and the tree state below it together. Either
/// failure fails the batch and cancels the other request.
async fn download_batch(
    connector: &Connector,
    client: LwdClient,
    plan: &WalletPlan,
    progress: &WalletProgress,
    (start, end): (u64, u64),
) -> Result<(), CallFailure> {
    let recorder = connector.recorder();
    let mut block_client = client.clone();
    let mut tree_state_client = client;
    let blocks = calls::get_block_range(
        recorder,
        &mut block_client,
        &LIMITS,
        BlockRangeCall {
            start,
            end,
            pool_types: vec![
                PoolType::Sapling as i32,
                PoolType::Orchard as i32,
                PoolType::Ironwood as i32,
            ],
        },
        |block, encoded_bytes| {
            progress.note_block(block, encoded_bytes);
            std::future::ready(())
        },
    );
    let tree_state = async {
        if reads_tree_state_locally(plan, start) {
            return Ok(());
        }
        calls::get_tree_state(recorder, &mut tree_state_client, &LIMITS, start - 1).await
    };
    tokio::try_join!(blocks, tree_state).map(drop)
}

/// Whether Vizor builds the batch's starting tree state without a request:
/// an empty state at or below Sapling activation, or a compiled Mainnet entry.
fn reads_tree_state_locally(plan: &WalletPlan, start: u64) -> bool {
    let below = start - 1;
    start <= plan.sapling_activation_height
        || (plan.mainnet
            && (COMPILED_TREE_STATE_START..=COMPILED_TREE_STATE_LAST).contains(&below)
            && (below - COMPILED_TREE_STATE_START).is_multiple_of(COMPILED_TREE_STATE_STEP))
}

/// Runs `attempt` again after each pause while it fails sooner than the
/// shortest lightwalletd timeout. A slower failure is returned at once.
async fn retry_quick_failures<Attempt, AttemptFuture>(
    progress: &WalletProgress,
    mut attempt: Attempt,
) -> Result<(), CallFailure>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<(), CallFailure>>,
{
    let mut pauses = PREFETCH_RETRY_PAUSES.iter();
    loop {
        let started = tokio::time::Instant::now();
        let failure = match attempt().await {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        match pauses.next() {
            Some(pause) if started.elapsed() < QUICK_FAILURE => {
                progress.note_retry();
                tokio::time::sleep(*pause).await;
            }
            Some(_) | None => return Err(failure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchSizing, WalletPlan, reads_tree_state_locally};

    fn plan(mainnet: bool) -> WalletPlan {
        WalletPlan {
            from_height: 1,
            to_height: 4_000_000,
            mainnet,
            sapling_activation_height: 419_200,
        }
    }

    #[test]
    fn byte_budget_cuts_batches_off_the_block_grid() {
        let sizing = BatchSizing {
            byte_budget: 11_300_000,
            max_blocks: 1_000,
        };

        assert_eq!(sizing.batch_end(1_000, 9_000, (0, 0)), 1_009);
        assert_eq!(sizing.batch_end(1_000, 9_000, (10, 10)), 1_999);
        assert_eq!(sizing.batch_end(2_000, 9_000, (100, 4_000_000)), 2_281);
        assert_eq!(sizing.batch_end(2_282, 9_000, (100, 1_000_000_000)), 2_291);
        assert_eq!(sizing.batch_end(8_995, 9_000, (0, 0)), 9_000);
    }

    #[test]
    fn compiled_mainnet_tree_states_and_pre_sapling_starts_need_no_request() {
        assert!(reads_tree_state_locally(&plan(true), 1_000_001));
        assert!(!reads_tree_state_locally(&plan(false), 1_000_001));
        assert!(!reads_tree_state_locally(&plan(true), 1_000_101));
        assert!(!reads_tree_state_locally(&plan(true), 3_580_001));
        assert!(reads_tree_state_locally(&plan(false), 419_200));
    }
}
