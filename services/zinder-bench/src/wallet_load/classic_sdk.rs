//! The classic Zcash Android SDK's `CompactBlockProcessor` sync pattern.
//!
//! One connection downloads serial batches. Each batch asks for the tree state
//! below its first block, then streams its blocks with an empty pool filter.
//! The processor overlaps one download with scanning; a client that does not
//! scan therefore downloads strictly one batch after another. A restored
//! wallet starts one block above its bundled checkpoint, so every batch of the
//! birthday range starts as far off the 100-block cadence as that checkpoint.

use std::{future::Future, time::Duration};

use serde::Serialize;

use super::{
    WalletPlan, WalletProgress,
    calls::{self, BlockRangeCall, CallFailure, CallLimits, Connector},
};

/// `CompactBlockProcessor.SYNC_BATCH_SIZE`.
const BATCH_BLOCKS: u64 = 1_000;
/// `SYNC_BATCH_SMALL_SIZE`, used for batches that touch the sandblasting range.
const SANDBLASTING_BATCH_BLOCKS: u64 = 100;
/// `SANDBLASTING_RANGE`. The SDK applies it on every network.
const SANDBLASTING_HEIGHTS: std::ops::RangeInclusive<u64> = 1_710_000..=2_050_000;
/// `CompactBlockProcessor.RETRIES`: attempts per tree state and per batch.
const ATTEMPTS: u32 = 5;
/// `retryUpToAndContinue` waits this long after the first failure and doubles
/// it after each further one.
const FIRST_RETRY_PAUSE: Duration = Duration::from_millis(500);

/// `LightWalletClient` defaults: unary calls and whole streams have deadlines;
/// a stream has no idle limit of its own.
const LIMITS: CallLimits = CallLimits {
    unary: Duration::from_secs(10),
    stream_open: Duration::from_secs(90),
    stream_idle: None,
    stream_total: Some(Duration::from_secs(90)),
};

/// Constants a classic-SDK-shaped run used.
#[derive(Debug, Serialize)]
pub(super) struct Parameters {
    batch_blocks: u64,
    sandblasting_batch_blocks: u64,
    sandblasting_heights: [u64; 2],
    attempts_per_call: u32,
    limits: CallLimits,
}

impl Parameters {
    pub(super) const fn new() -> Self {
        Self {
            batch_blocks: BATCH_BLOCKS,
            sandblasting_batch_blocks: SANDBLASTING_BATCH_BLOCKS,
            sandblasting_heights: [*SANDBLASTING_HEIGHTS.start(), *SANDBLASTING_HEIGHTS.end()],
            attempts_per_call: ATTEMPTS,
            limits: LIMITS,
        }
    }
}

/// Syncs `plan` one batch at a time on one connection.
pub(super) async fn sync_wallet(
    connector: Connector,
    plan: &WalletPlan,
    progress: &WalletProgress,
) -> Result<(), CallFailure> {
    let recorder = connector.recorder();
    let client = retry_up_to(progress, || connector.connect()).await?;
    let mut start = plan.from_height;
    while start <= plan.to_height {
        let end = batch_end(start, plan.to_height);
        retry_up_to(progress, || {
            let mut client = client.clone();
            async move { calls::get_tree_state(recorder, &mut client, &LIMITS, start - 1).await }
        })
        .await?;
        // A failed batch is requested again in full: the SDK collects the
        // stream into a list and discards it on error.
        retry_up_to(progress, || {
            let mut client = client.clone();
            async move {
                calls::get_block_range(
                    recorder,
                    &mut client,
                    &LIMITS,
                    BlockRangeCall {
                        start,
                        end,
                        pool_types: Vec::new(),
                    },
                    |block, encoded_bytes| {
                        progress.note_block(block, encoded_bytes);
                        std::future::ready(())
                    },
                )
                .await
            }
        })
        .await?;
        start = end.saturating_add(1);
    }
    Ok(())
}

/// Last block of the batch starting at `start`, as `getBatchedBlockList` plans it.
fn batch_end(start: u64, range_end: u64) -> u64 {
    let end = start.saturating_add(BATCH_BLOCKS - 1).min(range_end);
    if start <= *SANDBLASTING_HEIGHTS.end() && end >= *SANDBLASTING_HEIGHTS.start() {
        return start
            .saturating_add(SANDBLASTING_BATCH_BLOCKS - 1)
            .min(range_end);
    }
    end
}

async fn retry_up_to<Success, Attempt, AttemptFuture>(
    progress: &WalletProgress,
    mut attempt: Attempt,
) -> Result<Success, CallFailure>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<Success, CallFailure>>,
{
    let mut pause = FIRST_RETRY_PAUSE;
    let mut failed_attempts = 0;
    loop {
        match attempt().await {
            Ok(success) => return Ok(success),
            Err(failure) => {
                failed_attempts += 1;
                if failed_attempts == ATTEMPTS {
                    return Err(failure);
                }
                progress.note_retry();
                tokio::time::sleep(pause).await;
                pause = pause.saturating_mul(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::batch_end;

    #[test]
    fn batches_shrink_to_one_hundred_blocks_where_they_touch_the_sandblasting_range() {
        assert_eq!(batch_end(3_424_811, 3_500_000), 3_425_810);
        assert_eq!(batch_end(1_709_500, 1_800_000), 1_709_599);
        assert_eq!(batch_end(2_050_000, 2_100_000), 2_050_099);
        assert_eq!(batch_end(2_050_001, 2_100_000), 2_051_000);
        assert_eq!(batch_end(10, 20), 20);
    }
}
