//! Bounded memory of transparent outputs that sequential hydration saw produced.
//!
//! Resolving a spent output from canonical storage costs one transaction
//! location read plus a decode of the producing block's whole replay row.
//! Replay hydrates heights in order, so it has already decoded the block that
//! produced almost every coin it later sees spent. The cache keeps those
//! outputs, keyed by outpoint, until a hydrated block spends them.
//!
//! An entry is only ever answered for a block that extends the chain the cache
//! applied: every range must start at the next height and name the last
//! applied block as its parent, otherwise the cache empties before the lookup.
//! A hit is therefore an output produced on the current block's own ancestry
//! and not spent since, with the producing height and hash of that ancestry.

use std::collections::{BTreeMap, HashMap, HashSet, btree_map::Entry};

use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, CanonicalBlockFacts,
    TransparentAddressScriptHash, TransparentOutPoint,
};

use super::ProducedOutput;

/// Outputs per cache that keep the hash table inside 2^22 buckets.
///
/// One entry is an 88-byte outpoint and output pair plus one control byte, so
/// a full cache holds about 375 MB.
pub(crate) const DEFAULT_PREVOUT_CACHE_MAX_OUTPUTS: usize = 3_500_000;

/// Unspent transparent outputs produced by the blocks a reader hydrated.
///
/// When the cache holds more than its bound, it drops the outputs produced at
/// the oldest heights first, down to three quarters of the bound. A dropped or
/// never-seen output is resolved from canonical storage instead, so the bound
/// trades memory for producing-block reads and never changes a result.
#[derive(Debug)]
pub struct PrevoutCache {
    max_outputs: usize,
    outputs: HashMap<TransparentOutPoint, CachedOutput>,
    heights: BTreeMap<BlockHeight, CachedHeight>,
    applied_through: Option<BlockId>,
}

/// One cached output; its block hash lives once per height in `heights`.
#[derive(Clone, Copy, Debug)]
struct CachedOutput {
    value_zat: u64,
    address_script_hash: TransparentAddressScriptHash,
    block_height: BlockHeight,
}

#[derive(Clone, Copy, Debug)]
struct CachedHeight {
    block_hash: BlockHash,
    cached_outputs: usize,
}

impl PrevoutCache {
    /// Creates an empty cache that holds at most `max_outputs` outputs.
    ///
    /// A bound of zero keeps nothing, so every lookup falls back to canonical
    /// storage.
    #[must_use]
    pub fn new(max_outputs: usize) -> Self {
        Self {
            max_outputs,
            outputs: HashMap::new(),
            heights: BTreeMap::new(),
            applied_through: None,
        }
    }

    /// Returns the number of outputs the cache currently holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.outputs.len()
    }

    /// Returns whether the cache holds no outputs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Drops every cached output and the applied chain position.
    pub fn clear(&mut self) {
        self.outputs = HashMap::new();
        self.heights.clear();
        self.applied_through = None;
    }

    /// Empties the cache unless `first_block` extends the last applied block.
    pub(super) fn connect(&mut self, first_block: &BlockHeaderArtifact) {
        let Some(applied) = self.applied_through else {
            return;
        };
        if applied.height.next() != Some(first_block.height)
            || applied.hash != first_block.parent_hash
        {
            self.clear();
        }
    }

    /// Moves the cached outputs named by `requested` into `produced`, skipping
    /// any outpoint `produced` already resolves, and returns how many it moved.
    pub(super) fn resolve_into(
        &self,
        requested: &HashSet<TransparentOutPoint>,
        produced: &mut HashMap<TransparentOutPoint, ProducedOutput>,
    ) -> usize {
        let mut resolved = 0_usize;
        for outpoint in requested {
            if produced.contains_key(outpoint) {
                continue;
            }
            let Some(output) = self.produced_output(outpoint) else {
                continue;
            };
            produced.insert(*outpoint, output);
            resolved = resolved.saturating_add(1);
        }
        resolved
    }

    /// Applies one connected, ascending run of hydrated blocks.
    ///
    /// Each block's outputs are added before its own spends are removed, so an
    /// output spent later in the same block never stays cached. Eviction runs
    /// after every block, so the bound is exceeded by at most one block.
    pub(super) fn apply(&mut self, blocks: &[CanonicalBlockFacts]) {
        for block in blocks {
            self.apply_block(block);
            self.evict_oldest_heights();
        }
    }

    fn apply_block(&mut self, block: &CanonicalBlockFacts) {
        let header = &block.block_header;
        if self.max_outputs > 0 {
            for transaction in &block.transactions {
                let transaction_id = transaction.public_facts.transaction_id;
                for output in &transaction.transparent_outputs {
                    self.insert(
                        TransparentOutPoint::new(transaction_id, output.output_index),
                        CachedOutput {
                            value_zat: output.value_zat,
                            address_script_hash: output.address_script_hash,
                            block_height: header.height,
                        },
                        header.block_hash,
                    );
                }
            }
        }
        for input in block
            .transactions
            .iter()
            .flat_map(|transaction| &transaction.transparent_inputs)
            .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
        {
            self.remove(&input.spent_outpoint);
        }
        self.applied_through = Some(BlockId::new(header.height, header.block_hash));
    }

    fn produced_output(&self, outpoint: &TransparentOutPoint) -> Option<ProducedOutput> {
        let output = self.outputs.get(outpoint)?;
        let height = self.heights.get(&output.block_height)?;
        Some(ProducedOutput {
            value_zat: output.value_zat,
            address_script_hash: output.address_script_hash,
            block_height: output.block_height,
            block_hash: height.block_hash,
        })
    }

    fn insert(
        &mut self,
        outpoint: TransparentOutPoint,
        output: CachedOutput,
        block_hash: BlockHash,
    ) {
        if self.outputs.insert(outpoint, output).is_some() {
            return;
        }
        self.heights
            .entry(output.block_height)
            .or_insert(CachedHeight {
                block_hash,
                cached_outputs: 0,
            })
            .cached_outputs += 1;
    }

    fn remove(&mut self, outpoint: &TransparentOutPoint) {
        let Some(output) = self.outputs.remove(outpoint) else {
            return;
        };
        if let Entry::Occupied(mut height) = self.heights.entry(output.block_height) {
            let cached_outputs = &mut height.get_mut().cached_outputs;
            *cached_outputs = cached_outputs.saturating_sub(1);
            if *cached_outputs == 0 {
                height.remove();
            }
        }
    }

    /// Drops whole oldest heights until the cache is back at three quarters of
    /// its bound, so the full-table pass runs once per quarter of new outputs.
    fn evict_oldest_heights(&mut self) {
        if self.outputs.len() <= self.max_outputs {
            return;
        }
        let target = self.max_outputs - self.max_outputs / 4;
        let mut remaining = self.outputs.len();
        let mut first_kept = None;
        for (height, cached) in &self.heights {
            if remaining <= target {
                first_kept = Some(*height);
                break;
            }
            remaining = remaining.saturating_sub(cached.cached_outputs);
        }
        let Some(first_kept) = first_kept else {
            self.outputs.clear();
            self.heights.clear();
            return;
        };
        self.heights = self.heights.split_off(&first_kept);
        self.outputs
            .retain(|_, output| output.block_height >= first_kept);
    }
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        CanonicalTransactionFacts, SerializedBytesDigest, TransactionId,
        TransactionIntrinsicValueBalances, TransparentInputFact, TransparentOutputFact,
    };
    use zinder_testkit::synthetic_transaction_public_facts;

    use super::*;

    fn hash(seed: u32) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&seed.to_be_bytes());
        BlockHash::from_bytes(bytes)
    }

    fn txid(seed: u32) -> TransactionId {
        let mut bytes = [0xa5_u8; 32];
        bytes[..4].copy_from_slice(&seed.to_be_bytes());
        TransactionId::from_bytes(bytes)
    }

    fn script_hash(seed: u32) -> TransparentAddressScriptHash {
        let mut bytes = [0x11_u8; 32];
        bytes[..4].copy_from_slice(&seed.to_be_bytes());
        TransparentAddressScriptHash::from_bytes(bytes)
    }

    /// One block at `height` whose single transaction spends `spends` and
    /// creates `output_count` outputs.
    fn block(
        height: u32,
        parent: BlockHash,
        spends: &[TransparentOutPoint],
        output_count: u32,
    ) -> CanonicalBlockFacts {
        let transaction_id = txid(height);
        let transparent_inputs = spends
            .iter()
            .zip(0_u32..)
            .map(|(outpoint, input_index)| TransparentInputFact::new(input_index, *outpoint))
            .collect();
        let transparent_outputs = (0..output_count)
            .map(|output_index| {
                TransparentOutputFact::new(
                    output_index,
                    u64::from(height) * 1_000 + u64::from(output_index),
                    vec![0x51],
                    script_hash(height),
                )
            })
            .collect();
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(height),
                hash(height),
                parent,
                [0; 32],
                [0; 32],
                0,
                0,
                [0; 32],
                4,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
            transactions: vec![CanonicalTransactionFacts {
                public_facts: synthetic_transaction_public_facts(transaction_id, 0),
                serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
                intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
                transparent_inputs,
                transparent_outputs,
            }],
        }
    }

    fn lookup(cache: &PrevoutCache, outpoint: TransparentOutPoint) -> Option<ProducedOutput> {
        let mut produced = HashMap::new();
        cache.resolve_into(&HashSet::from([outpoint]), &mut produced);
        produced.remove(&outpoint)
    }

    #[test]
    fn a_cached_output_carries_its_producing_block_until_it_is_spent() {
        let mut cache = PrevoutCache::new(16);
        let produced = TransparentOutPoint::new(txid(1), 1);
        cache.apply(&[block(1, hash(0), &[], 2)]);

        let output = lookup(&cache, produced);
        assert_eq!(output.map(|output| output.value_zat), Some(1_001));
        assert_eq!(output.map(|output| output.block_hash), Some(hash(1)));
        assert_eq!(
            output.map(|output| output.address_script_hash),
            Some(script_hash(1))
        );

        cache.connect(&block(2, hash(1), &[], 0).block_header);
        cache.apply(&[block(2, hash(1), &[produced], 0)]);
        assert!(lookup(&cache, produced).is_none());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_block_that_does_not_extend_the_applied_tip_empties_the_cache() {
        let mut cache = PrevoutCache::new(16);
        cache.apply(&[block(1, hash(0), &[], 1), block(2, hash(1), &[], 1)]);
        assert_eq!(cache.len(), 2);

        cache.connect(&block(3, hash(1), &[], 0).block_header);
        assert!(cache.is_empty(), "a replaced parent must not reuse outputs");

        cache.apply(&[block(1, hash(0), &[], 1), block(2, hash(1), &[], 1)]);
        cache.connect(&block(2, hash(1), &[], 0).block_header);
        assert!(cache.is_empty(), "a repeated height must not reuse outputs");

        cache.apply(&[block(1, hash(0), &[], 1), block(2, hash(1), &[], 1)]);
        cache.connect(&block(3, hash(2), &[], 0).block_header);
        assert_eq!(cache.len(), 2, "the next connected block keeps the cache");
    }

    #[test]
    fn an_output_spent_in_its_own_block_is_never_cached() {
        let mut cache = PrevoutCache::new(16);
        let mut producing = block(1, hash(0), &[], 1);
        let spent = TransparentOutPoint::new(txid(1), 0);
        producing.transactions.push(CanonicalTransactionFacts {
            transparent_inputs: vec![TransparentInputFact::new(0, spent)],
            transparent_outputs: Vec::new(),
            ..producing.transactions[0].clone()
        });
        cache.apply(&[producing]);
        assert!(lookup(&cache, spent).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn a_full_cache_drops_the_oldest_heights_first() {
        let mut cache = PrevoutCache::new(8);
        let mut parent = hash(0);
        let mut blocks = Vec::new();
        for height in 1..=5 {
            blocks.push(block(height, parent, &[], 2));
            parent = hash(height);
        }
        cache.apply(&blocks);

        assert_eq!(
            cache.len(),
            6,
            "eviction stops at three quarters of the bound"
        );
        assert!(lookup(&cache, TransparentOutPoint::new(txid(2), 0)).is_none());
        assert!(lookup(&cache, TransparentOutPoint::new(txid(3), 0)).is_some());
        assert!(lookup(&cache, TransparentOutPoint::new(txid(5), 1)).is_some());
    }

    #[test]
    fn a_zero_bound_keeps_nothing_but_still_tracks_the_tip() {
        let mut cache = PrevoutCache::new(0);
        cache.apply(&[block(1, hash(0), &[], 3)]);
        assert!(cache.is_empty());
        assert_eq!(
            cache.applied_through,
            Some(BlockId::new(BlockHeight::new(1), hash(1)))
        );
    }

    #[test]
    fn the_default_bound_fits_the_documented_table_size() {
        let entry_bytes = size_of::<(TransparentOutPoint, CachedOutput)>();
        assert_eq!(entry_bytes, 88);
        let buckets = (DEFAULT_PREVOUT_CACHE_MAX_OUTPUTS * 8 / 7).next_power_of_two();
        assert_eq!(buckets, 1 << 22);
    }
}
