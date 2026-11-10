//! Per-stage byte-budget reservations that bound in-flight bytes and drive
//! construction-pipeline backpressure.

use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct ByteWatermark {
    stage: &'static str,
    limit_bytes: NonZeroU64,
    inner: Arc<Mutex<ByteWatermarkState>>,
}

#[derive(Debug, Default)]
struct ByteWatermarkState {
    reserved_bytes: u64,
    reservations: u32,
}

#[derive(Debug)]
pub(crate) struct ByteReservation {
    watermark: ByteWatermark,
    bytes: u64,
    is_released: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WatermarkSnapshot {
    pub(crate) reserved_bytes: u64,
    pub(crate) reservations: u32,
    pub(crate) limit_bytes: u64,
}

impl ByteWatermark {
    pub(crate) fn new(stage: &'static str, limit_bytes: NonZeroU64) -> Self {
        Self {
            stage,
            limit_bytes,
            inner: Arc::new(Mutex::new(ByteWatermarkState::default())),
        }
    }

    pub(crate) fn try_reserve(&self, bytes: u64) -> Option<ByteReservation> {
        let bytes = bytes.max(1);
        let mut state = self.inner.lock();
        let next_reserved_bytes = state.reserved_bytes.checked_add(bytes)?;
        let first_reservation = state.reservations == 0;
        let is_over_limit = next_reserved_bytes > self.limit_bytes.get();
        if is_over_limit && !first_reservation {
            metrics::counter!(
                "zinder_ingest_bulk_pipeline_watermark_blocked_total",
                "stage" => self.stage
            )
            .increment(1);
            return None;
        }

        state.reserved_bytes = next_reserved_bytes;
        state.reservations = state.reservations.saturating_add(1);
        let snapshot = self.snapshot_from_state(&state);
        drop(state);
        self.record(snapshot);
        if is_over_limit {
            self.record_oversized("single_reservation");
        }
        Some(ByteReservation {
            watermark: self.clone(),
            bytes,
            is_released: AtomicBool::new(false),
        })
    }

    pub(crate) fn snapshot(&self) -> WatermarkSnapshot {
        let state = self.inner.lock();
        self.snapshot_from_state(&state)
    }

    fn resize(&self, current_bytes: u64, next_bytes: u64) {
        let mut state = self.inner.lock();
        let was_over_limit = state.reserved_bytes > self.limit_bytes.get();
        state.reserved_bytes = state
            .reserved_bytes
            .saturating_sub(current_bytes)
            .saturating_add(next_bytes.max(1));
        let snapshot = self.snapshot_from_state(&state);
        drop(state);
        self.record(snapshot);
        if snapshot.is_over_limit() && !was_over_limit {
            self.record_oversized("reservation_resize");
        }
    }

    fn release(&self, bytes: u64) {
        let mut state = self.inner.lock();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
        state.reservations = state.reservations.saturating_sub(1);
        let snapshot = self.snapshot_from_state(&state);
        drop(state);
        self.record(snapshot);
    }

    fn snapshot_from_state(&self, state: &ByteWatermarkState) -> WatermarkSnapshot {
        WatermarkSnapshot {
            reserved_bytes: state.reserved_bytes,
            reservations: state.reservations,
            limit_bytes: self.limit_bytes.get(),
        }
    }

    fn record(&self, snapshot: WatermarkSnapshot) {
        metrics::gauge!(
            "zinder_ingest_bulk_pipeline_queue_bytes",
            "stage" => self.stage
        )
        .set(u64_to_f64(snapshot.reserved_bytes));
        metrics::gauge!(
            "zinder_ingest_bulk_pipeline_active",
            "stage" => self.stage
        )
        .set(f64::from(snapshot.reservations));
    }

    fn record_oversized(&self, reason: &'static str) {
        metrics::counter!(
            "zinder_ingest_bulk_pipeline_watermark_oversized_total",
            "stage" => self.stage,
            "reason" => reason
        )
        .increment(1);
    }
}

impl WatermarkSnapshot {
    /// Returns whether accepted or measured work currently exceeds the
    /// admission limit.
    ///
    /// The watermark permits one oversized reservation so a single large work
    /// item can make progress, and a measured resize can reveal that an
    /// estimate was low. Both states stop additional admission until existing
    /// reservations fall back under the configured limit.
    #[must_use]
    pub(crate) const fn is_over_limit(self) -> bool {
        self.reserved_bytes > self.limit_bytes
    }
}

impl ByteReservation {
    /// Replaces an admission estimate with the measured retained bytes.
    ///
    /// A measurement can exceed the configured limit because the allocation
    /// already exists. The watermark records that transition and blocks new
    /// reservations; it is admission control, not a hard allocator cap.
    pub(crate) fn resize(&mut self, next_bytes: u64) {
        if self.is_released.load(Ordering::Acquire) {
            return;
        }
        let next_bytes = next_bytes.max(1);
        self.watermark.resize(self.bytes, next_bytes);
        self.bytes = next_bytes;
    }

    pub(crate) fn release(&self) {
        if self.is_released.swap(true, Ordering::AcqRel) {
            return;
        }
        self.watermark.release(self.bytes);
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) fn record_queue_depth(stage: &'static str, depth: usize) {
    metrics::gauge!(
        "zinder_ingest_bulk_pipeline_queue_depth",
        "stage" => stage
    )
    .set(f64::from(usize_to_u32_saturating(depth)));
}

pub(crate) fn record_reorder_buffer(stage: &'static str, blocks: usize, bytes: u64) {
    metrics::gauge!(
        "zinder_ingest_bulk_pipeline_reorder_buffer_blocks",
        "stage" => stage
    )
    .set(f64::from(usize_to_u32_saturating(blocks)));
    metrics::gauge!(
        "zinder_ingest_bulk_pipeline_reorder_buffer_bytes",
        "stage" => stage
    )
    .set(u64_to_f64(bytes));
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; byte counts are diagnostic magnitudes"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn reservation_releases_on_drop() -> Result<(), Box<dyn Error>> {
        let watermark = ByteWatermark::new(
            "test_stage",
            NonZeroU64::new(100).ok_or("invalid watermark")?,
        );
        {
            let _reservation = watermark.try_reserve(40).ok_or("reservation should fit")?;
            assert_eq!(watermark.snapshot().reserved_bytes, 40);
        }
        assert_eq!(watermark.snapshot().reserved_bytes, 0);
        Ok(())
    }

    #[test]
    fn reservation_refuses_over_limit_after_first_reservation() -> Result<(), Box<dyn Error>> {
        let watermark = ByteWatermark::new(
            "test_stage",
            NonZeroU64::new(100).ok_or("invalid watermark")?,
        );
        let _first = watermark
            .try_reserve(80)
            .ok_or("first reservation should fit")?;

        assert!(watermark.try_reserve(21).is_none());
        assert_eq!(watermark.snapshot().reserved_bytes, 80);
        Ok(())
    }

    #[test]
    fn first_reservation_can_exceed_limit() -> Result<(), Box<dyn Error>> {
        let watermark = ByteWatermark::new(
            "test_stage",
            NonZeroU64::new(100).ok_or("invalid watermark")?,
        );
        let _reservation = watermark
            .try_reserve(120)
            .ok_or("first reservation should be allowed")?;

        assert_eq!(watermark.snapshot().reserved_bytes, 120);
        Ok(())
    }

    #[test]
    fn reservation_resize_updates_snapshot() -> Result<(), Box<dyn Error>> {
        let watermark = ByteWatermark::new(
            "test_stage",
            NonZeroU64::new(100).ok_or("invalid watermark")?,
        );
        let mut reservation = watermark.try_reserve(30).ok_or("reservation should fit")?;

        reservation.resize(70);

        assert_eq!(watermark.snapshot().reserved_bytes, 70);
        Ok(())
    }

    #[test]
    fn oversized_resize_blocks_new_admission_until_release() -> Result<(), Box<dyn Error>> {
        let watermark = ByteWatermark::new(
            "test_stage",
            NonZeroU64::new(100).ok_or("invalid watermark")?,
        );
        let mut reservation = watermark.try_reserve(40).ok_or("reservation should fit")?;

        reservation.resize(120);

        assert!(watermark.snapshot().is_over_limit());
        assert!(watermark.try_reserve(1).is_none());
        reservation.release();
        assert!(!watermark.snapshot().is_over_limit());
        assert!(watermark.try_reserve(1).is_some());
        Ok(())
    }
}
