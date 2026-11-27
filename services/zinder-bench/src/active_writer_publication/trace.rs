//! Deterministic shared request trace replayed identically across every arm.
//!
//! Generating the full workload geometry and request shapes upfront guarantees
//! matched catalog span widths, descending tail probe depths, slow-reader
//! frame counts, tree-state heights, and wallet RPC targets across quiescent
//! and active arms.

use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::ActiveWriterPublicationConfig;

/// Deterministic trace shared across all arms of one benchmark run.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkloadTrace {
    pub(crate) base_tip_height: u32,
    pub(crate) final_tip_height: u32,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) segment_boundary_height: u32,
    pub(crate) catalog_span_blocks: u32,
    pub(crate) tail_probe_span_blocks: u32,
    pub(crate) slow_boundary_span_blocks: u32,
    pub(crate) slow_reader_frame_delay_ms: u64,
    pub(crate) mempool_churn_sizes: Vec<u32>,
    pub(crate) quiescent_rounds: u32,
    pub(crate) tree_state_settled_height: u32,
    pub(crate) wallet_addresses: Vec<String>,
    pub(crate) settled_txids: Vec<[u8; 32]>,
    pub(crate) live_txids: Vec<(u32, [u8; 32])>,
    pub(crate) digest_hex: String,
}

impl WorkloadTrace {
    /// Derives the shared workload trace and computes `digest_hex`.
    pub(crate) fn build(
        config: &ActiveWriterPublicationConfig,
        segment_boundary_height: u32,
        wallet_addresses: Vec<String>,
        settled_txids: Vec<[u8; 32]>,
        live_txids: Vec<(u32, [u8; 32])>,
    ) -> Self {
        let base_tip_height = config.base_blocks;
        let final_tip_height = config.base_blocks.saturating_add(config.live_blocks);
        let base_settled = base_tip_height
            .saturating_sub(config.reorg_window_blocks)
            .max(1);
        let catalog_span_blocks = 128_u32.min(base_settled.saturating_sub(1));
        let tail_probe_span_blocks = config
            .tail_probe_blocks
            .saturating_sub(1)
            .min(base_tip_height.saturating_sub(1));

        // Both quiescent controls and active modes must sleep across the exact same
        // frame count on slow boundary reads so comparisons isolate contention.
        let boundary_start = segment_boundary_height.saturating_sub(6).max(1);
        let slow_boundary_span_blocks = final_tip_height
            .saturating_sub(boundary_start)
            .min(base_tip_height.saturating_sub(1));
        let tree_state_settled_height = base_tip_height.saturating_sub(10).max(1);
        let slow_reader_frame_delay_ms =
            u64::try_from(config.slow_reader_frame_delay.as_millis()).unwrap_or(u64::MAX);

        let mut trace = Self {
            base_tip_height,
            final_tip_height,
            reorg_window_blocks: config.reorg_window_blocks,
            segment_boundary_height,
            catalog_span_blocks,
            tail_probe_span_blocks,
            slow_boundary_span_blocks,
            slow_reader_frame_delay_ms,
            mempool_churn_sizes: config.mempool_churn_sizes.clone(),
            quiescent_rounds: config.quiescent_rounds,
            tree_state_settled_height,
            wallet_addresses,
            settled_txids,
            live_txids,
            digest_hex: String::new(),
        };
        trace.digest_hex = compute_trace_digest(&trace);
        trace
    }

    /// Descending `(start, end)` bounds for a `GetBlockRange(tip, floor)` tail probe.
    pub(crate) fn descending_tail_range(&self, tip: u32) -> (u32, u32) {
        let floor = tip.saturating_sub(self.tail_probe_span_blocks).max(1);
        (tip, floor)
    }

    /// Ascending `(start, end)` bounds for the slow boundary-crossing range probe.
    pub(crate) fn slow_boundary_range(&self, tip: u32) -> (u32, u32) {
        let end = tip.max(self.base_tip_height).max(1);
        let start = end.saturating_sub(self.slow_boundary_span_blocks).max(1);
        (start, end)
    }

    /// Ascending `(start, end)` bounds for settled catalog reads at `served_tip`.
    pub(crate) fn catalog_range(&self, served_tip: u32) -> (u32, u32) {
        let settled_tip = served_tip.saturating_sub(self.reorg_window_blocks).max(1);
        let end = settled_tip.min(self.segment_boundary_height);
        let start = end.saturating_sub(self.catalog_span_blocks).max(1);
        (start, end)
    }

    /// Artificial per-frame delay applied by slow boundary readers.
    pub(crate) const fn slow_reader_frame_delay(&self) -> Duration {
        Duration::from_millis(self.slow_reader_frame_delay_ms)
    }

    /// Confirmed transaction id available at or below `served_tip`.
    pub(crate) fn txid_visible_at(&self, round: u32, served_tip: u32) -> Option<[u8; 32]> {
        let mut eligible: Vec<[u8; 32]> = self.settled_txids.clone();
        for &(height, txid) in &self.live_txids {
            if height <= served_tip {
                eligible.push(txid);
            }
        }
        if eligible.is_empty() {
            return None;
        }
        let idx = usize::try_from(round).unwrap_or(0) % eligible.len();
        Some(eligible[idx])
    }
}

fn compute_trace_digest(trace: &WorkloadTrace) -> String {
    let encoded = serde_json::to_vec(trace).unwrap_or_default();
    let digest = Sha256::digest(encoded);
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use super::{ActiveWriterPublicationConfig, WorkloadTrace};

    #[test]
    fn workload_trace_has_stable_digest_and_matched_slow_span_across_modes() {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            tail_probe_blocks: 24,
            slow_reader_frame_delay: Duration::from_millis(3),
            ..ActiveWriterPublicationConfig::default()
        };
        let trace_a = WorkloadTrace::build(
            &config,
            64,
            vec!["tmAddr".to_owned()],
            vec![[1_u8; 32]],
            vec![(65, [2_u8; 32])],
        );
        let trace_b = WorkloadTrace::build(
            &config,
            64,
            vec!["tmAddr".to_owned()],
            vec![[1_u8; 32]],
            vec![(65, [2_u8; 32])],
        );
        assert_ne!(trace_a.digest_hex, "");
        assert_eq!(trace_a.digest_hex, trace_b.digest_hex);

        let control_slow = trace_a.slow_boundary_range(72);
        let active_slow_start = trace_a.slow_boundary_range(60);
        let control_frames = control_slow.1 - control_slow.0 + 1;
        let active_frames = active_slow_start.1 - active_slow_start.0 + 1;
        assert_eq!(control_frames, active_frames);

        let (desc_start, desc_end) = trace_a.descending_tail_range(72);
        assert!(desc_start > desc_end);
        assert_eq!(desc_start - desc_end + 1, 24);
    }
}
