//! Streaming tip-following commit loop, mempool churn driver, pre-commit
//! atomic live-tail settlement, and nearest-rank latency statistics.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio_postgres::Client;
use zinder_core::{BlockHeight, Network};
use zinder_source::NodeSource;

use crate::error::BenchError;
use crate::fixture::FixtureNodeSource;

use super::database::{candidate_error, postgres_error, u32_to_i64, usize_to_i64};
use super::loader::{
    PendingSealTransition, StreamingPrepareState, TipCommitPayload, WriterFenceHandle,
    persist_tip_block_in_txn,
};
use super::segment_builder::{PreparedSegmentedBlock, seal_settled_segments};
use super::wallet_projection::ProvisionalWalletProjection;

/// Nearest-rank latency percentiles in milliseconds (`p50`, `p95`, `p99`, `max`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LatencyPercentilesMillis {
    /// Number of observations included.
    pub sample_count: u64,
    /// Median latency (`p50`).
    pub p50_ms: f64,
    /// 95th-percentile latency (`p95`).
    pub p95_ms: f64,
    /// 99th-percentile latency (`p99`).
    pub p99_ms: f64,
    /// Maximum observed latency (`max`).
    pub max_ms: f64,
}

impl LatencyPercentilesMillis {
    /// Computes deterministic nearest-rank percentiles over millisecond samples.
    #[must_use]
    pub fn from_samples(raw_samples: &[f64]) -> Self {
        if raw_samples.is_empty() {
            return Self::default();
        }
        let mut sorted = raw_samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let sample_count = u64::try_from(sorted.len()).unwrap_or(u64::MAX);
        Self {
            sample_count,
            p50_ms: nearest_rank(&sorted, 50),
            p95_ms: nearest_rank(&sorted, 95),
            p99_ms: nearest_rank(&sorted, 99),
            max_ms: sorted.last().copied().unwrap_or(0.0),
        }
    }
}

fn nearest_rank(sorted: &[f64], percentile: usize) -> f64 {
    let len = sorted.len();
    if len == 0 {
        return 0.0;
    }
    let idx = (len.saturating_mul(percentile))
        .div_ceil(100)
        .saturating_sub(1)
        .min(len - 1);
    sorted[idx]
}

/// Configuration constructing [`TipFollowState`].
pub struct TipFollowParameters {
    /// Active network.
    pub network: Network,
    /// Validated target `PostgreSQL` schema name.
    pub schema_name: String,
    /// Active fenced writer term verified on every tip commit.
    pub fence: WriterFenceHandle,
    /// First block height of the benchmark chain.
    pub first_height: BlockHeight,
    /// Whether `ZSBLKB01` / `raw_block_bytes` retention is enabled.
    pub full_blocks: bool,
    /// Settled segment byte upper bound.
    pub max_segment_bytes: u64,
    /// Maximum blocks grouped into each settled segment.
    pub settled_segment_blocks: u32,
    /// Upper bound on rows kept in `{schema}.canonical_live_tail`.
    pub live_tail_max_blocks: u32,
    /// Number of synthetic mempool transactions admitted+evicted per tip block.
    pub mempool_churn_txs_per_block: u32,
}

/// Shared source + preparation + wallet accumulator advanced across tip blocks.
pub struct TipAdvanceTarget<'a> {
    /// Random-access streaming fixture block source.
    pub source: &'a FixtureNodeSource,
    /// Shared canonical sequence & tree-size preparation accumulator.
    pub prepare_state: &'a mut StreamingPrepareState,
    /// Provisional wallet projection state updated upon commit.
    pub wallet_projection: &'a mut ProvisionalWalletProjection,
}

/// Accumulated tip-follow outcome returned at end of both tip-follow phases.
#[derive(Clone, Debug)]
pub struct TipFollowRunMetrics {
    /// Latency percentiles across uncontended tip commits.
    pub uncontended_commit_latencies_ms: LatencyPercentilesMillis,
    /// Latency percentiles across reader-contended tip commits.
    pub contended_commit_latencies_ms: LatencyPercentilesMillis,
    /// Count of runtime tail-to-settled segment transitions executed.
    pub tail_seal_transitions: u64,
    /// Count of mempool admit operations executed.
    pub mempool_admits: u64,
    /// Count of mempool eviction operations executed.
    pub mempool_evictions: u64,
    /// Largest single sealed segment byte length observed across build + runtime seals.
    pub max_segment_bytes_observed: u64,
}

/// Mutable state tracking mempool churn and live-tail blocks eligible for sealing.
pub struct TipFollowState {
    network: Network,
    schema_name: String,
    fence: WriterFenceHandle,
    first_height: BlockHeight,
    full_blocks: bool,
    max_segment_bytes: u64,
    settled_segment_blocks: u32,
    live_tail_max_blocks: u32,
    mempool_churn_txs_per_block: u32,
    tail_buffer: Vec<PreparedSegmentedBlock>,
    active_mempool_txids: VecDeque<[u8; 32]>,
    mempool_seq_counter: u64,
    max_segment_bytes_observed: u64,
    tail_seal_transitions: u64,
    mempool_admits: u64,
    mempool_evictions: u64,
    extra_prepare_duration: Duration,
}

impl TipFollowState {
    /// Initializes tip-follow tracking from already-loaded unsealed `tail_buffer` rows.
    #[must_use]
    pub fn new(
        params: TipFollowParameters,
        initial_tail_blocks: Vec<PreparedSegmentedBlock>,
        initial_max_segment_bytes: u64,
    ) -> Self {
        Self {
            network: params.network,
            schema_name: params.schema_name,
            fence: params.fence,
            first_height: params.first_height,
            full_blocks: params.full_blocks,
            max_segment_bytes: params.max_segment_bytes,
            settled_segment_blocks: params.settled_segment_blocks,
            live_tail_max_blocks: params.live_tail_max_blocks,
            mempool_churn_txs_per_block: params.mempool_churn_txs_per_block,
            tail_buffer: initial_tail_blocks,
            active_mempool_txids: VecDeque::new(),
            mempool_seq_counter: 0,
            max_segment_bytes_observed: initial_max_segment_bytes,
            tail_seal_transitions: 0,
            mempool_admits: 0,
            mempool_evictions: 0,
            extra_prepare_duration: Duration::ZERO,
        }
    }

    /// Streams `[start_height..=end_height]` one block at a time, admits mempool entries,
    /// pre-seals any excess tail batch, and atomically commits tip + settlement under fence.
    pub async fn advance_height_range(
        &mut self,
        client: &mut Client,
        target: &mut TipAdvanceTarget<'_>,
        height_range: Option<(u32, u32)>,
    ) -> Result<LatencyPercentilesMillis, BenchError> {
        let Some((start_h, end_h)) = height_range else {
            return Ok(LatencyPercentilesMillis::default());
        };
        if start_h > end_h {
            return Ok(LatencyPercentilesMillis::default());
        }
        let block_span = usize::try_from(end_h - start_h + 1).unwrap_or(0);
        let mut commit_samples_ms = Vec::with_capacity(block_span);
        for height_val in start_h..=end_h {
            let prep_started = Instant::now();
            let source_block = target
                .source
                .fetch_block_at(BlockHeight::new(height_val))
                .await
                .map_err(|err| {
                    candidate_error(format!("fetching tip block {height_val}: {err}"))
                })?;
            let block = target.prepare_state.prepare_source_block(&source_block)?;
            self.extra_prepare_duration += prep_started.elapsed();

            let commit_ms = self
                .commit_prepared_tip_block(client, &block, target.wallet_projection)
                .await?;
            self.tail_buffer.push(block);
            commit_samples_ms.push(commit_ms);
        }
        Ok(LatencyPercentilesMillis::from_samples(&commit_samples_ms))
    }

    async fn commit_prepared_tip_block(
        &mut self,
        client: &mut Client,
        block: &PreparedSegmentedBlock,
        wallet_projection: &mut ProvisionalWalletProjection,
    ) -> Result<f64, BenchError> {
        let started = Instant::now();
        self.admit_mempool_entries(client, block.height).await?;

        let staged_wallet = wallet_projection.stage_block(&block.facts)?;
        let evict_count = self.planned_mined_eviction_count();
        let evicted_txids: Vec<[u8; 32]> = self
            .active_mempool_txids
            .iter()
            .take(evict_count)
            .copied()
            .collect();
        let pending_seal = self.prepare_precommit_seal_transition().await?;

        let txn = client
            .transaction()
            .await
            .map_err(|err| postgres_error("starting tip-follow transaction", &err))?;
        let payload = TipCommitPayload {
            fence: self.fence,
            full_blocks: self.full_blocks,
            first_height: self.first_height,
            block,
            wallet_delta: &staged_wallet.delta,
            evicted_mempool_txids: &evicted_txids,
            pending_seal: pending_seal.as_ref(),
        };
        persist_tip_block_in_txn(&txn, &self.schema_name, &payload).await?;
        txn.commit()
            .await
            .map_err(|err| postgres_error("committing tip-follow transaction", &err))?;

        wallet_projection.commit_staged_persisted(staged_wallet);
        for _ in 0..evicted_txids.len() {
            let _ = self.active_mempool_txids.pop_front();
        }
        self.mempool_evictions = self
            .mempool_evictions
            .saturating_add(u64::try_from(evicted_txids.len()).unwrap_or(0));

        if let Some(seal) = pending_seal {
            self.tail_buffer.drain(..seal.seal_count);
            self.max_segment_bytes_observed = self
                .max_segment_bytes_observed
                .max(seal.max_segment_bytes_observed);
            self.tail_seal_transitions = self.tail_seal_transitions.saturating_add(1);
        }

        Ok(started.elapsed().as_secs_f64() * 1000.0)
    }

    async fn prepare_precommit_seal_transition(
        &self,
    ) -> Result<Option<PendingSealTransition>, BenchError> {
        let max_tail = usize::try_from(self.live_tail_max_blocks)
            .unwrap_or(128)
            .max(1);
        let projected_len = self.tail_buffer.len().saturating_add(1);
        if projected_len <= max_tail || self.tail_buffer.is_empty() {
            return Ok(None);
        }
        let excess = projected_len.saturating_sub(max_tail);
        let preferred_batch = (self.tail_buffer.len() / 2).max(excess).min(
            usize::try_from(self.settled_segment_blocks)
                .unwrap_or(1)
                .max(excess),
        );
        let seal_count = preferred_batch.min(self.tail_buffer.len());
        let candidate_slice = self.tail_buffer[..seal_count].to_vec();
        let last = candidate_slice
            .last()
            .ok_or_else(|| candidate_error("empty tail seal slice"))?;
        let settled_height = last.height;
        let settled_hash = last.block_hash;
        let settled_prev = last.previous_block_hash;

        let sealed = seal_settled_segments(
            self.network,
            candidate_slice,
            self.settled_segment_blocks,
            self.full_blocks,
            self.max_segment_bytes,
        )
        .await?;

        Ok(Some(PendingSealTransition {
            bundles: sealed.bundles,
            settled_height,
            settled_hash,
            settled_prev,
            seal_count,
            max_segment_bytes_observed: sealed.max_segment_bytes_observed,
        }))
    }

    /// Returns extra source-prepare duration accumulated while streaming tip blocks.
    #[must_use]
    pub const fn extra_prepare_duration(&self) -> Duration {
        self.extra_prepare_duration
    }

    /// Returns cumulative mempool/sealing totals alongside uncontended & contended percentiles.
    #[must_use]
    pub fn into_metrics(
        self,
        uncontended_commit_latencies_ms: LatencyPercentilesMillis,
        contended_commit_latencies_ms: LatencyPercentilesMillis,
    ) -> TipFollowRunMetrics {
        TipFollowRunMetrics {
            uncontended_commit_latencies_ms,
            contended_commit_latencies_ms,
            tail_seal_transitions: self.tail_seal_transitions,
            mempool_admits: self.mempool_admits,
            mempool_evictions: self.mempool_evictions,
            max_segment_bytes_observed: self.max_segment_bytes_observed,
        }
    }

    fn planned_mined_eviction_count(&self) -> usize {
        let target = usize::try_from(self.mempool_churn_txs_per_block).unwrap_or(0);
        if self.active_mempool_txids.len() > target {
            self.active_mempool_txids
                .len()
                .saturating_sub(target)
                .min(target)
        } else {
            0
        }
    }

    async fn admit_mempool_entries(
        &mut self,
        client: &mut Client,
        block_height: BlockHeight,
    ) -> Result<(), BenchError> {
        if self.mempool_churn_txs_per_block == 0 {
            return Ok(());
        }
        let schema = &self.schema_name;
        let height_pg = u32_to_i64(block_height.value());
        let mut staged_txids =
            Vec::with_capacity(usize::try_from(self.mempool_churn_txs_per_block).unwrap_or(0));
        let txn = client
            .transaction()
            .await
            .map_err(|err| postgres_error("starting mempool admit txn", &err))?;

        let mut bumped_seq = self.mempool_seq_counter;
        for _ in 0..self.mempool_churn_txs_per_block {
            bumped_seq = bumped_seq.saturating_add(1);
            let txid = synthetic_mempool_txid(block_height, bumped_seq);
            let payload = vec![0x05_u8; 256];
            let payload_len = usize_to_i64(payload.len(), "mempool tx len")?;
            txn.execute(
                &format!(
                    "INSERT INTO {schema}.mempool_active_entry (txid, raw_tx_bytes, tx_byte_len) VALUES ($1, $2, $3)"
                ),
                &[&txid.as_slice(), &payload.as_slice(), &payload_len],
            )
            .await
            .map_err(|err| postgres_error("inserting mempool_active_entry", &err))?;
            txn.execute(
                &format!(
                    "INSERT INTO {schema}.mempool_event (event_kind, txid, block_height) VALUES ('admit', $1, $2)"
                ),
                &[&txid.as_slice(), &height_pg],
            )
            .await
            .map_err(|err| postgres_error("inserting admit mempool_event", &err))?;
            staged_txids.push(txid);
        }
        txn.commit()
            .await
            .map_err(|err| postgres_error("committing mempool admit txn", &err))?;

        self.mempool_seq_counter = bumped_seq;
        self.mempool_admits = self
            .mempool_admits
            .saturating_add(u64::try_from(staged_txids.len()).unwrap_or(0));
        self.active_mempool_txids.extend(staged_txids);
        Ok(())
    }
}

fn synthetic_mempool_txid(height: BlockHeight, counter: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"zinder-bench-mempool-churn-v1");
    hasher.update(height.value().to_le_bytes());
    hasher.update(counter.to_le_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::LatencyPercentilesMillis;

    #[test]
    fn nearest_rank_percentiles_match_standard_boundaries() {
        let empty = LatencyPercentilesMillis::from_samples(&[]);
        assert_eq!(empty.sample_count, 0);
        assert_eq!(empty.max_ms, 0.0);

        let one = LatencyPercentilesMillis::from_samples(&[4.2]);
        assert_eq!(one.sample_count, 1);
        assert_eq!(one.p50_ms, 4.2);
        assert_eq!(one.p95_ms, 4.2);
        assert_eq!(one.p99_ms, 4.2);
        assert_eq!(one.max_ms, 4.2);

        let samples: Vec<f64> = (1..=100).rev().map(f64::from).collect();
        let stats = LatencyPercentilesMillis::from_samples(&samples);
        assert_eq!(stats.sample_count, 100);
        assert_eq!(stats.p50_ms, 50.0);
        assert_eq!(stats.p95_ms, 95.0);
        assert_eq!(stats.p99_ms, 99.0);
        assert_eq!(stats.max_ms, 100.0);
    }
}
