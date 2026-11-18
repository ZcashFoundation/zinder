//! The Slipstream engine's request pattern, with its Android defaults, for a
//! wallet connected directly rather than through an OHTTP gateway.
//!
//! Slipstream asks for the tree state below the range, then runs four block
//! workers, each on its own connection. Workers take plan chunks from one
//! dispatcher: a 64-block probe first, then chunks sized to one worker's share
//! of the read-ahead budget at the density received so far, at most 10,000
//! blocks. Each worker hands its stream on in pieces of up to 8 MiB, and every
//! piece except the range's last asks for the tree state at its last block. A
//! failed chunk is resumed from its next height on a fresh connection after
//! 0.25, 0.5, and 1 seconds. When the blocks are in, the wallet fetches
//! transactions eight at a time.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{StreamExt as _, TryFutureExt as _, future::try_join_all, stream};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::{sync::mpsc, task::JoinSet};

use super::{
    WalletPlan, WalletProgress, WorkloadSettings,
    calls::{self, BlockRangeCall, CallFailure, CallLimits, Connector, LwdClient},
};

/// `EngineConfig::DEFAULT_FETCH_STREAMS`.
const FETCH_STREAMS: u64 = 4;
/// `EngineConfig::DEFAULT_CHUNK_BLOCKS`: the most blocks one request asks for.
const CHUNK_BLOCKS: u64 = 10_000;
/// `EngineConfig::DEFAULT_CHUNK_SPLIT_BYTES`.
pub(super) const DEFAULT_SPLIT_BYTES: u64 = 8 * 1024 * 1024;
/// The read-ahead budget is this many pieces (`AHEAD_BUDGET_FACTOR`).
const AHEAD_BUDGET_FACTOR: u64 = 8;
/// First chunk of a fetch (`PROBE_SPAN`), and the fewest blocks a sized chunk
/// takes (`BOOST_MIN_SPAN`).
const PROBE_SPAN: u64 = 64;
const MIN_SPAN: u64 = 10;
/// Workers other than the probe's wait up to `DENSITY_WAIT` for this many
/// blocks before sizing their first chunk (`DENSITY_MIN_BLOCKS`).
const DENSITY_MIN_BLOCKS: u64 = 8;
const DENSITY_WAIT: Duration = Duration::from_secs(3);
const DENSITY_POLL: Duration = Duration::from_millis(50);
/// `FetchPlan::retries_per_chunk`, with the backoff before each retry.
const CHUNK_RETRIES: u32 = 3;
const FIRST_CHUNK_RETRY_PAUSE: Duration = Duration::from_millis(250);
/// `retry_get_tree_state` makes three attempts, pausing 1 then 3 seconds.
const TREE_STATE_RETRY_PAUSES: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(3)];
/// `ENHANCE_FETCH_CONCURRENCY`.
const GET_TRANSACTION_CONCURRENCY: usize = 8;

const LIMITS: CallLimits = CallLimits {
    unary: Duration::from_secs(30),
    stream_open: Duration::from_secs(30),
    stream_idle: Some(Duration::from_secs(30)),
    stream_total: None,
};

/// Constants and settings a Slipstream-shaped run used.
#[derive(Debug, Serialize)]
pub(super) struct Parameters {
    fetch_streams: u64,
    chunk_blocks: u64,
    probe_blocks: u64,
    split_bytes: u64,
    chunk_target_bytes: u64,
    chunk_retries: u32,
    get_transaction_concurrency: usize,
    transactions_per_wallet: usize,
    limits: CallLimits,
}

impl Parameters {
    pub(super) fn new(settings: &WorkloadSettings) -> Self {
        Self {
            fetch_streams: FETCH_STREAMS,
            chunk_blocks: CHUNK_BLOCKS,
            probe_blocks: PROBE_SPAN,
            split_bytes: settings.split_bytes,
            chunk_target_bytes: chunk_target_bytes(settings.split_bytes),
            chunk_retries: CHUNK_RETRIES,
            get_transaction_concurrency: GET_TRANSACTION_CONCURRENCY,
            transactions_per_wallet: settings.transactions_per_wallet,
            limits: LIMITS,
        }
    }
}

/// One worker's share of the read-ahead budget (`chunk_target_bytes`).
fn chunk_target_bytes(split_bytes: u64) -> u64 {
    split_bytes.saturating_mul(AHEAD_BUDGET_FACTOR) / FETCH_STREAMS
}

/// State the wallet's workers share.
///
/// Tree-state tasks hold clones of what they use rather than the wallet, so
/// dropping the wallet aborts them.
struct Wallet {
    connector: Connector,
    plan: Arc<WalletPlan>,
    progress: Arc<WalletProgress>,
    /// The connection tree-state and transaction requests share.
    client: LwdClient,
    split_bytes: u64,
    dispatcher: Dispatcher,
    boundaries: Mutex<JoinSet<Result<(), CallFailure>>>,
    boundary_failures: mpsc::UnboundedSender<CallFailure>,
}

/// Syncs `plan` with Slipstream's block workers, then fetches transactions.
pub(super) async fn sync_wallet(
    connector: Connector,
    plan: Arc<WalletPlan>,
    settings: Arc<WorkloadSettings>,
    progress: Arc<WalletProgress>,
) -> Result<(), CallFailure> {
    let mut shared = None;
    tree_state_with_retries(&connector, &progress, &mut shared, plan.from_height - 1).await?;
    let client = connector.reuse_or_connect(&mut shared).await?.clone();
    let (boundary_failures, mut failed_boundaries) = mpsc::unbounded_channel();
    let wallet = Arc::new(Wallet {
        dispatcher: Dispatcher::new(&plan, chunk_target_bytes(settings.split_bytes)),
        connector,
        plan,
        progress,
        client,
        split_bytes: settings.split_bytes,
        boundaries: Mutex::new(JoinSet::new()),
        boundary_failures,
    });
    // A failed tree state fails the pass at once, as it does in Slipstream.
    let fetched = tokio::select! {
        fetched = try_join_all((0..FETCH_STREAMS).map(|_| fetch_worker(&wallet))) => fetched.map(drop),
        Some(failure) = failed_boundaries.recv() => Err(failure),
    };
    let mut boundaries = std::mem::take(&mut *wallet.boundaries.lock());
    let boundaries_done = match fetched {
        Ok(()) => join_boundaries(&mut boundaries).await,
        Err(failure) => Err(failure),
    };
    // Stop tree-state requests still running before the wallet reports.
    boundaries.shutdown().await;
    boundaries_done?;
    // Transaction fetches are not fatal to a Slipstream pass, so their
    // failures are only counted.
    let recorder = wallet.connector.recorder();
    stream::iter(wallet.progress.take_txids())
        .for_each_concurrent(GET_TRANSACTION_CONCURRENCY, |txid| {
            calls::get_transaction(recorder, wallet.client.clone(), LIMITS, txid)
                .unwrap_or_else(drop)
        })
        .await;
    Ok(())
}

async fn join_boundaries(
    boundaries: &mut JoinSet<Result<(), CallFailure>>,
) -> Result<(), CallFailure> {
    while let Some(joined) = boundaries.join_next().await {
        joined.map_err(|error| CallFailure {
            label: "client_task_failed",
            detail: error.to_string(),
        })??;
    }
    Ok(())
}

/// Takes chunks until the range is handed out. A worker connects when it
/// takes its first chunk.
async fn fetch_worker(wallet: &Arc<Wallet>) -> Result<(), CallFailure> {
    let mut client = None;
    while let Some((start, end)) = wallet.dispatcher.take(&wallet.progress).await {
        fetch_chunk(wallet, &mut client, start, end).await?;
    }
    Ok(())
}

/// Streams one plan chunk, resuming after failures, and hands it on in pieces.
///
/// A failed connect counts as a failed attempt, like a failed stream.
async fn fetch_chunk(
    wallet: &Arc<Wallet>,
    client: &mut Option<LwdClient>,
    start: u64,
    end: u64,
) -> Result<(), CallFailure> {
    let recorder = wallet.connector.recorder();
    let mut resume_from = start;
    let mut pieces = PieceSplitter::new(wallet.split_bytes);
    let mut failed_attempts = 0_u32;
    while resume_from <= end {
        let mut made_progress = false;
        let streamed = match wallet.connector.reuse_or_connect(client).await {
            Ok(connected) => {
                calls::get_block_range(
                    recorder,
                    connected,
                    &LIMITS,
                    BlockRangeCall {
                        start: resume_from,
                        end,
                        pool_types: Vec::new(),
                    },
                    |block, encoded_bytes| {
                        wallet.progress.note_block(block, encoded_bytes);
                        if let Some(last_height) = pieces.push(block.height, encoded_bytes) {
                            hand_on_piece(wallet, last_height);
                        }
                        resume_from = block.height.saturating_add(1);
                        made_progress = true;
                    },
                )
                .await
            }
            Err(failure) => Err(failure),
        };
        let Err(failure) = streamed else { break };
        if resume_from > end {
            break;
        }
        // An attempt that received blocks starts a fresh retry budget.
        failed_attempts = if made_progress {
            1
        } else {
            failed_attempts + 1
        };
        if failed_attempts > CHUNK_RETRIES {
            return Err(failure);
        }
        wallet.progress.note_retry();
        tokio::time::sleep(FIRST_CHUNK_RETRY_PAUSE.saturating_mul(1 << (failed_attempts - 1)))
            .await;
        *client = None;
    }
    if let Some(last_height) = pieces.finish() {
        hand_on_piece(wallet, last_height);
    }
    Ok(())
}

/// Cuts one chunk's stream into pieces of at most `split_bytes`, or of one
/// larger block, as Slipstream's sub-chunk splitter does.
struct PieceSplitter {
    split_bytes: u64,
    bytes: u64,
    last_height: Option<u64>,
}

impl PieceSplitter {
    const fn new(split_bytes: u64) -> Self {
        Self {
            split_bytes,
            bytes: 0,
            last_height: None,
        }
    }

    /// Adds one block, returning the last height of the piece it completes.
    fn push(&mut self, height: u64, encoded_bytes: u64) -> Option<u64> {
        let completed =
            if self.bytes > 0 && self.bytes.saturating_add(encoded_bytes) > self.split_bytes {
                self.bytes = 0;
                self.last_height
            } else {
                None
            };
        self.bytes = self.bytes.saturating_add(encoded_bytes);
        self.last_height = Some(height);
        completed
    }

    /// The last height of the piece still open, if any.
    fn finish(self) -> Option<u64> {
        self.last_height.filter(|_| self.bytes > 0)
    }
}

/// Asks for the tree state at a piece's last block, unless the piece ends the
/// range.
fn hand_on_piece(wallet: &Arc<Wallet>, last_height: u64) {
    if last_height >= wallet.plan.to_height {
        return;
    }
    let connector = wallet.connector.clone();
    let progress = Arc::clone(&wallet.progress);
    let failures = wallet.boundary_failures.clone();
    let mut client = Some(wallet.client.clone());
    wallet.boundaries.lock().spawn(async move {
        let outcome =
            tree_state_with_retries(&connector, &progress, &mut client, last_height).await;
        if let Err(failure) = &outcome {
            // The receiver is gone once the wallet has already ended.
            let _ = failures.send(failure.clone());
        }
        outcome
    });
}

/// Requests one tree state up to three times, the retries on a new
/// connection. A failed connect counts as a failed attempt.
async fn tree_state_with_retries(
    connector: &Connector,
    progress: &WalletProgress,
    client: &mut Option<LwdClient>,
    height: u64,
) -> Result<(), CallFailure> {
    let mut pauses = TREE_STATE_RETRY_PAUSES.iter();
    loop {
        let attempt = match connector.reuse_or_connect(client).await {
            Ok(connected) => {
                calls::get_tree_state(connector.recorder(), connected, &LIMITS, height).await
            }
            Err(failure) => Err(failure),
        };
        let Err(failure) = attempt else {
            return Ok(());
        };
        let Some(pause) = pauses.next() else {
            return Err(failure);
        };
        progress.note_retry();
        tokio::time::sleep(*pause).await;
        *client = None;
    }
}

/// Hands out plan chunks in height order, sized by the bytes they will carry.
struct Dispatcher {
    end: u64,
    target_bytes: u64,
    next: Mutex<DispatchState>,
}

struct DispatchState {
    index: u64,
    start: u64,
    /// When the probe was handed out. Later workers wait from then for the
    /// density to size their chunks.
    probe_taken: Option<Instant>,
}

impl DispatchState {
    /// Whether too few blocks have arrived to size a chunk by density, and
    /// the wait for them has not run out.
    fn density_unknown(&self, received_blocks: u64) -> bool {
        received_blocks < DENSITY_MIN_BLOCKS
            && self
                .probe_taken
                .is_none_or(|taken| taken.elapsed() < DENSITY_WAIT)
    }
}

impl Dispatcher {
    fn new(plan: &WalletPlan, target_bytes: u64) -> Self {
        Self {
            end: plan.to_height,
            target_bytes,
            next: Mutex::new(DispatchState {
                index: 0,
                start: plan.from_height,
                probe_taken: None,
            }),
        }
    }

    /// The next chunk `(start, end)`, or `None` once the range is handed out.
    /// After the probe, a worker waits briefly for density to size its chunk.
    async fn take(&self, progress: &WalletProgress) -> Option<(u64, u64)> {
        loop {
            let received = progress.received();
            let waiting = {
                let next = self.next.lock();
                if next.start > self.end {
                    return None;
                }
                next.index > 0 && next.density_unknown(received.0)
            };
            if !waiting {
                return self.take_now(received);
            }
            tokio::time::sleep(DENSITY_POLL).await;
        }
    }

    fn take_now(&self, (blocks, bytes): (u64, u64)) -> Option<(u64, u64)> {
        let mut next = self.next.lock();
        if next.start > self.end {
            return None;
        }
        let span = if next.index == 0 || next.density_unknown(blocks) {
            PROBE_SPAN
        } else {
            // With no block received by the end of the density wait,
            // Slipstream takes the full chunk span. The simulated wallet keeps
            // the probe span instead, so a stalled server is not sent the
            // largest request.
            bytes.checked_div(blocks).map_or(PROBE_SPAN, |per_block| {
                (self.target_bytes / per_block.max(1)).clamp(MIN_SPAN, CHUNK_BLOCKS)
            })
        };
        let start = next.start;
        let end = start.saturating_add(span - 1).min(self.end);
        next.start = end.saturating_add(1);
        next.index += 1;
        next.probe_taken.get_or_insert_with(Instant::now);
        drop(next);
        Some((start, end))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        CHUNK_BLOCKS, DEFAULT_SPLIT_BYTES, DENSITY_WAIT, Dispatcher, PieceSplitter, WalletPlan,
        chunk_target_bytes,
    };

    #[test]
    fn pieces_close_before_the_block_that_would_overflow_them() {
        let mut pieces = PieceSplitter::new(1_000);

        assert_eq!(pieces.push(10, 600), None);
        assert_eq!(pieces.push(11, 400), None);
        assert_eq!(pieces.push(12, 1), Some(11));
        // A block larger than a piece is a piece of its own.
        assert_eq!(pieces.push(13, 5_000), Some(12));
        assert_eq!(pieces.push(14, 10), Some(13));
        assert_eq!(pieces.finish(), Some(14));
        assert_eq!(PieceSplitter::new(1_000).finish(), None);
    }

    #[test]
    fn chunks_start_with_a_probe_then_follow_density_up_to_the_cap() {
        let dispatcher = Dispatcher::new(
            &WalletPlan {
                from_height: 3_424_811,
                to_height: 3_500_000,
                mainnet: true,
                sapling_activation_height: 419_200,
            },
            chunk_target_bytes(DEFAULT_SPLIT_BYTES),
        );

        assert_eq!(dispatcher.take_now((0, 0)), Some((3_424_811, 3_424_874)));
        // Until eight blocks arrive, another probe-sized chunk.
        assert_eq!(
            dispatcher.take_now((3, 3_000)),
            Some((3_424_875, 3_424_938))
        );
        assert_eq!(
            dispatcher.take_now((64, 64 * 1_000)),
            Some((3_424_939, 3_424_939 + CHUNK_BLOCKS - 1))
        );
        // 16 MiB at 100 KiB a block is 163 blocks.
        assert_eq!(
            dispatcher.take_now((64, 64 * 102_400)),
            Some((3_434_939, 3_435_101))
        );
    }

    #[test]
    fn a_server_that_sent_no_block_by_the_density_wait_gets_probe_sized_chunks()
    -> Result<(), Box<dyn std::error::Error>> {
        let dispatcher = Dispatcher::new(
            &WalletPlan {
                from_height: 1_000,
                to_height: 100_000,
                mainnet: false,
                sapling_activation_height: 1,
            },
            chunk_target_bytes(DEFAULT_SPLIT_BYTES),
        );
        assert_eq!(dispatcher.take_now((0, 0)), Some((1_000, 1_063)));
        dispatcher.next.lock().probe_taken = Some(
            Instant::now()
                .checked_sub(DENSITY_WAIT + Duration::from_secs(1))
                .ok_or("the clock is too close to its start")?,
        );

        assert_eq!(dispatcher.take_now((0, 0)), Some((1_064, 1_127)));
        Ok(())
    }
}
