//! Validated shielded wire frames retained by one canonical secondary generation.

use parking_lot::RwLock;
use zinder_core::{BlockHash, BlockHeight, BlockId};
use zinder_proto::compat::lightwalletd::EncodedBlock;

use crate::settled_shielded_segment::PoolPresenceMask;

use super::{
    CanonicalStoreError, encoded_compact_range::encoded_block_from_payload,
    live_tail::LiveTailShieldedWire,
};

const MAX_CACHE_BYTES: usize = 5 * 1024 * 1024;
// Charge the entry and reserve space for the Bytes shared allocation bookkeeping.
const ENTRY_OVERHEAD: usize = size_of::<CachedLiveTailBlock>() + 64;

pub(super) struct LiveTailShieldedCache {
    settled_height: BlockHeight,
    visible_height: BlockHeight,
    entries: RwLock<CacheEntries>,
}

#[derive(Default)]
struct CacheEntries {
    blocks: Vec<CachedLiveTailBlock>,
    bytes: usize,
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
            .map(|height| {
                let index = entries
                    .blocks
                    .binary_search_by_key(height, |entry| entry.block_id.height)
                    .ok()?;
                Some(entries.blocks[index].clone())
            })
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
        let index = match entries
            .blocks
            .binary_search_by_key(&height, |entry| entry.block_id.height)
        {
            Ok(index) => return Ok(Some(entries.blocks[index].clone())),
            Err(index) => index,
        };
        let charge = wire.shielded_payload.len() + EncodedBlock::PREFIX_LEN + ENTRY_OVERHEAD;
        if charge > MAX_CACHE_BYTES - entries.bytes {
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
        // Keep admitted entries until the generation changes. Exact reservation
        // prevents spare capacity from growing outside the per-entry charge.
        entries.blocks.reserve_exact(1);
        entries.blocks.insert(index, entry.clone());
        entries.bytes += charge;
        drop(entries);
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_charges_frames_and_entries_and_stops_at_cap() -> Result<(), CanonicalStoreError> {
        let cache = LiveTailShieldedCache::new(BlockHeight::new(1), BlockHeight::new(4));
        let payload = vec![0; MAX_CACHE_BYTES / 2 - ENTRY_OVERHEAD - EncodedBlock::PREFIX_LEN];
        let hash = BlockHash::from_bytes([1; 32]);
        let wire = |height| LiveTailShieldedWire {
            block_id: BlockId::new(BlockHeight::new(height), hash),
            previous_block_hash: hash,
            time: 1,
            shielded_payload: &payload,
            pool_presence: PoolPresenceMask::EMPTY,
            has_unrepresentable_wire_fee: false,
        };
        assert!(cache.admit(&wire(1), payload.len())?.is_none());
        assert!(cache.admit(&wire(5), payload.len())?.is_none());
        assert!(
            cache
                .admit(
                    &LiveTailShieldedWire {
                        shielded_payload: &vec![0; MAX_CACHE_BYTES],
                        ..wire(4)
                    },
                    MAX_CACHE_BYTES,
                )?
                .is_none()
        );
        assert!(cache.admit(&wire(3), payload.len())?.is_some());
        assert!(cache.admit(&wire(2), payload.len())?.is_some());
        assert_eq!(cache.entries.read().bytes, MAX_CACHE_BYTES);
        assert!(cache.admit(&wire(2), payload.len())?.is_some());
        assert!(cache.admit(&wire(4), payload.len())?.is_none());

        let hits = cache.lookup(&[BlockHeight::new(2), BlockHeight::new(4)]);
        assert!(hits[0].is_some());
        assert!(hits[1].is_none());
        assert_eq!(cache.entries.read().bytes, MAX_CACHE_BYTES);
        Ok(())
    }
}
