//! Validated shielded wire frames retained by one canonical secondary generation.

use std::collections::HashMap;

use parking_lot::RwLock;
use zinder_core::{BlockHash, BlockHeight, BlockId};
use zinder_proto::compat::lightwalletd::EncodedBlock;

use crate::settled_shielded_segment::PoolPresenceMask;

use super::{
    CanonicalStoreError, encoded_compact_range::encoded_block_from_payload,
    live_tail::LiveTailShieldedWire,
};

const MAX_CACHE_BYTES: usize = 5 * 1024 * 1024;
// Allow for the shared allocation bookkeeping used by Bytes.
const FRAME_OVERHEAD: usize = 64;
const BUCKET_BYTES: usize = size_of::<(BlockHeight, CachedLiveTailBlock)>() + 1;
// Allow for hashbrown's trailing group of replicated control tags.
const CONTROL_TRAILER_BYTES: usize = 16;

pub(super) struct LiveTailShieldedCache {
    settled_height: BlockHeight,
    visible_height: BlockHeight,
    entries: RwLock<CacheEntries>,
}

#[derive(Default)]
struct CacheEntries {
    blocks: HashMap<BlockHeight, CachedLiveTailBlock>,
    frame_bytes: usize,
}

impl CacheEntries {
    fn total_bytes(&self) -> usize {
        self.frame_bytes + table_bytes(buckets(self.blocks.capacity()))
    }

    fn bytes_before_new_frame(&self) -> usize {
        let capacity = self.blocks.capacity();
        if self.blocks.len() == capacity {
            self.frame_bytes + table_bytes(buckets(capacity).saturating_mul(2).max(4))
        } else {
            self.total_bytes()
        }
    }
}

// The map only inserts, so capacity reflects the bucket count at hashbrown's
// load limit. Small tables keep one slot empty instead of applying the 7/8 limit.
fn buckets(capacity: usize) -> usize {
    match capacity {
        0 => 0,
        1..=3 => 4,
        _ => capacity.saturating_mul(8).div_ceil(7).next_power_of_two(),
    }
}

const fn table_bytes(bucket_count: usize) -> usize {
    if bucket_count == 0 {
        0
    } else {
        bucket_count * BUCKET_BYTES + CONTROL_TRAILER_BYTES
    }
}

#[derive(Clone)]
pub(super) struct CachedLiveTailBlock {
    block_id: BlockId,
    previous_block_hash: BlockHash,
    time: u32,
    pool_presence: PoolPresenceMask,
    has_unrepresentable_wire_fee: bool,
    pub(super) frame: EncodedBlock,
    pub(super) stored_row_bytes: usize,
}

impl CachedLiveTailBlock {
    pub(super) fn wire(&self) -> LiveTailShieldedWire<'_> {
        LiveTailShieldedWire {
            block_id: self.block_id,
            previous_block_hash: self.previous_block_hash,
            time: self.time,
            shielded_payload: &self.frame.as_length_prefixed()[EncodedBlock::PREFIX_LEN..],
            pool_presence: self.pool_presence,
            has_unrepresentable_wire_fee: self.has_unrepresentable_wire_fee,
        }
    }
}

impl LiveTailShieldedCache {
    pub(super) fn new(settled_height: BlockHeight, visible_height: BlockHeight) -> Self {
        Self {
            settled_height,
            visible_height,
            entries: RwLock::default(),
        }
    }

    pub(super) fn lookup(&self, heights: &[BlockHeight]) -> Vec<Option<CachedLiveTailBlock>> {
        let entries = self.entries.read();
        heights
            .iter()
            .map(|height| entries.blocks.get(height).cloned())
            .collect()
    }

    pub(super) fn admit(
        &self,
        wire: &LiveTailShieldedWire<'_>,
        stored_row_bytes: usize,
    ) -> Result<Option<CachedLiveTailBlock>, CanonicalStoreError> {
        let height = wire.block_id.height;
        if height <= self.settled_height || height > self.visible_height {
            return Ok(None);
        }
        let mut entries = self.entries.write();
        if let Some(entry) = entries.blocks.get(&height) {
            return Ok(Some(entry.clone()));
        }
        let charge = wire.shielded_payload.len() + EncodedBlock::PREFIX_LEN + FRAME_OVERHEAD;
        if entries.bytes_before_new_frame() + charge > MAX_CACHE_BYTES {
            return Ok(None);
        }
        let entry = CachedLiveTailBlock {
            block_id: wire.block_id,
            previous_block_hash: wire.previous_block_hash,
            time: wire.time,
            pool_presence: wire.pool_presence,
            has_unrepresentable_wire_fee: wire.has_unrepresentable_wire_fee,
            frame: encoded_block_from_payload(wire.shielded_payload)?,
            stored_row_bytes,
        };
        entries.blocks.insert(height, entry.clone());
        entries.frame_bytes += charge;
        debug_assert!(entries.total_bytes() <= MAX_CACHE_BYTES);
        drop(entries);
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(height: u32, payload: &[u8]) -> LiveTailShieldedWire<'_> {
        let hash = BlockHash::from_bytes([1; 32]);
        LiveTailShieldedWire {
            block_id: BlockId::new(BlockHeight::new(height), hash),
            previous_block_hash: hash,
            time: 1,
            shielded_payload: payload,
            pool_presence: PoolPresenceMask::EMPTY,
            has_unrepresentable_wire_fee: false,
        }
    }

    #[test]
    fn descending_tiny_frames_stay_within_cap_as_table_grows() -> Result<(), CanonicalStoreError> {
        const LAST_HEIGHT: u32 = 4_096;
        let cache = LiveTailShieldedCache::new(BlockHeight::new(0), BlockHeight::new(LAST_HEIGHT));
        let payload = [0];
        let charge = payload.len() + EncodedBlock::PREFIX_LEN + FRAME_OVERHEAD;
        let mut previous_buckets = 0;
        let mut growths = 0;
        for height in (1..=LAST_HEIGHT).rev() {
            assert!(
                cache
                    .admit(&wire(height, &payload), payload.len())?
                    .is_some()
            );
            let entries = cache.entries.read();
            let current_buckets = buckets(entries.blocks.capacity());
            if current_buckets != previous_buckets {
                assert_eq!(current_buckets, (previous_buckets * 2).max(4));
                previous_buckets = current_buckets;
                growths += 1;
            }
            assert_eq!(entries.frame_bytes, entries.blocks.len() * charge);
            assert!(entries.total_bytes() <= MAX_CACHE_BYTES);
            drop(entries);
        }
        assert!(growths > 1);
        let heights = [
            BlockHeight::new(1),
            BlockHeight::new(LAST_HEIGHT),
            BlockHeight::new(2),
        ];
        for (entry, height) in cache.lookup(&heights).into_iter().zip(heights) {
            assert_eq!(entry.map(|entry| entry.block_id.height), Some(height));
        }
        Ok(())
    }

    #[test]
    fn admission_rejects_table_growth_that_would_exceed_the_cap() -> Result<(), CanonicalStoreError>
    {
        let cache = LiveTailShieldedCache::new(BlockHeight::new(0), BlockHeight::new(4));
        let tiny = [0];
        let tiny_charge = tiny.len() + EncodedBlock::PREFIX_LEN + FRAME_OVERHEAD;
        // Leave space for three tiny frames in the initial table. Only two
        // can fit beside the larger frame before inserting requires growth.
        let large = vec![
            0;
            MAX_CACHE_BYTES
                - table_bytes(4)
                - 3 * tiny_charge
                - EncodedBlock::PREFIX_LEN
                - FRAME_OVERHEAD
        ];
        assert!(cache.admit(&wire(1, &large), large.len())?.is_some());
        for height in [2, 3] {
            assert!(cache.admit(&wire(height, &tiny), tiny.len())?.is_some());
        }
        let before = cache.entries.read().total_bytes();
        assert_eq!(before + tiny_charge, MAX_CACHE_BYTES);
        assert!(cache.admit(&wire(3, &tiny), tiny.len())?.is_some());
        assert!(cache.admit(&wire(4, &tiny), tiny.len())?.is_none());

        let entries = cache.entries.read();
        assert_eq!(entries.blocks.capacity(), 3);
        assert_eq!(entries.blocks.len(), 3);
        assert_eq!(entries.total_bytes(), before);
        Ok(())
    }

    #[test]
    fn admission_charges_large_frames_and_ignores_duplicate_and_out_of_range_heights()
    -> Result<(), CanonicalStoreError> {
        let cache = LiveTailShieldedCache::new(BlockHeight::new(1), BlockHeight::new(4));
        let payload =
            vec![
                0;
                (MAX_CACHE_BYTES - table_bytes(4)) / 2 - EncodedBlock::PREFIX_LEN - FRAME_OVERHEAD
            ];
        for height in [1, 5] {
            assert!(
                cache
                    .admit(&wire(height, &payload), payload.len())?
                    .is_none()
            );
        }
        assert_eq!(cache.entries.read().total_bytes(), 0);
        for height in [3, 2] {
            assert!(
                cache
                    .admit(&wire(height, &payload), payload.len())?
                    .is_some()
            );
        }
        assert_eq!(cache.entries.read().total_bytes(), MAX_CACHE_BYTES);
        assert!(cache.admit(&wire(2, &payload), payload.len())?.is_some());
        assert!(cache.admit(&wire(4, &[0]), 1)?.is_none());

        let entries = cache.entries.read();
        assert_eq!(entries.blocks.capacity(), 3);
        assert_eq!(entries.blocks.len(), 2);
        assert_eq!(entries.total_bytes(), MAX_CACHE_BYTES);
        Ok(())
    }
}
