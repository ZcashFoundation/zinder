//! Bounded LRU cache of settled replayed [`ServedTreeState`] entries.
//!
//! Only heights within retained settled history enter this cache, anchored to
//! one [`SettledChainAnchor`]. When `settled_tip` advances at the same immutable
//! chain identity, one single-flight canonical header lookup at the prior
//! settled height proves the extension before promoting the anchor so cached
//! entries survive tip advances without rebuilding or re-reading storage.

use std::{
    collections::{BTreeMap, HashMap},
    mem::size_of,
    sync::Weak,
};

use zinder_core::{
    ArtifactSchemaVersion, BlockHeight, BlockId, CanonicalHistoryBounds, Network,
    NetworkUpgradeActivationsFingerprint, ShieldedProtocol,
};
use zinder_store::ServedTreeState;

/// Default maximum entry count for one query's settled tree-state cache.
pub(super) const SETTLED_MAX_ENTRIES: usize = 1_024;

/// Default byte budget (8 MiB) for one query's settled tree-state cache.
pub(super) const SETTLED_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Settled chain identity to which cached [`ServedTreeState`] rows belong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct SettledChainAnchor {
    pub(super) network: Network,
    pub(super) artifact_schema: ArtifactSchemaVersion,
    pub(super) retained: CanonicalHistoryBounds,
    pub(super) schedule_revision: Option<u64>,
    pub(super) activations_fingerprint: Option<NetworkUpgradeActivationsFingerprint>,
    pub(super) settled_tip: BlockId,
}

impl SettledChainAnchor {
    /// Returns whether `height` lies within retained settled history under this anchor.
    pub(super) fn covers(&self, height: BlockHeight) -> bool {
        let predecessor = self
            .retained
            .preceding_checkpoint()
            .map_or(BlockHeight::new(0), |checkpoint| checkpoint.height);
        height >= predecessor && height <= self.settled_tip.height
    }

    /// Returns whether `other` preserves network, schema, retention, and upgrade schedule.
    pub(super) fn shares_immutable_identity(&self, other: &Self) -> bool {
        self.network == other.network
            && self.artifact_schema == other.artifact_schema
            && self.retained == other.retained
            && self.schedule_revision == other.schedule_revision
            && self.activations_fingerprint == other.activations_fingerprint
    }
}

/// Outcome of classifying a settled-cache lookup before async header verification.
#[derive(Debug)]
pub(super) enum LookupStep {
    Hit(ServedTreeState),
    Miss,
    VerifyExtension(SettledChainAnchor),
}

#[derive(Debug)]
struct CachedEntry {
    state: ServedTreeState,
    bytes: usize,
    tick: u64,
}

/// Bounded LRU cache of settled replayed tree states.
#[derive(Debug)]
pub(super) struct SettledStates {
    max_entries: usize,
    max_bytes: usize,
    total_bytes: usize,
    next_tick: u64,
    anchor: Option<SettledChainAnchor>,
    anchor_generation: Option<Weak<dyn Send + Sync>>,
    entries: HashMap<BlockHeight, CachedEntry>,
    lru: BTreeMap<u64, BlockHeight>,
}

impl Default for SettledStates {
    fn default() -> Self {
        Self::new(SETTLED_MAX_ENTRIES, SETTLED_MAX_BYTES)
    }
}

impl SettledStates {
    pub(super) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
            total_bytes: 0,
            next_tick: 0,
            anchor: None,
            anchor_generation: None,
            entries: HashMap::new(),
            lru: BTreeMap::new(),
        }
    }

    fn anchor_generation_alive(&self) -> bool {
        self.anchor_generation
            .as_ref()
            .is_some_and(|weak| weak.strong_count() > 0)
    }

    fn refresh_generation_if_dropped(&mut self, generation: &Weak<dyn Send + Sync>) {
        if !self.anchor_generation_alive() {
            self.anchor_generation = Some(generation.clone());
        }
    }

    fn allocate_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick = self.next_tick.wrapping_add(1);
        tick
    }

    fn reset_to(&mut self, anchor: SettledChainAnchor, generation: &Weak<dyn Send + Sync>) {
        self.entries.clear();
        self.lru.clear();
        self.total_bytes = 0;
        self.anchor = Some(anchor);
        self.anchor_generation = Some(generation.clone());
    }

    fn get_touching(&mut self, height: BlockHeight) -> Option<ServedTreeState> {
        let new_tick = self.allocate_tick();
        let entry = self.entries.get_mut(&height)?;
        self.lru.remove(&entry.tick);
        entry.tick = new_tick;
        self.lru.insert(new_tick, height);
        Some(entry.state.clone())
    }

    pub(super) fn classify_lookup(
        &mut self,
        target: SettledChainAnchor,
        generation: &Weak<dyn Send + Sync>,
        height: BlockHeight,
    ) -> LookupStep {
        if !target.covers(height) {
            return LookupStep::Miss;
        }
        match self.anchor {
            None => LookupStep::Miss,
            Some(current) if current == target => {
                self.refresh_generation_if_dropped(generation);
                self.get_touching(height)
                    .map_or(LookupStep::Miss, LookupStep::Hit)
            }
            Some(_) if self.entries.is_empty() => {
                self.reset_to(target, generation);
                LookupStep::Miss
            }
            Some(current)
                if current.shares_immutable_identity(&target)
                    && target.settled_tip.height > current.settled_tip.height =>
            {
                LookupStep::VerifyExtension(current)
            }
            Some(current)
                if current.shares_immutable_identity(&target)
                    && target.settled_tip.height < current.settled_tip.height =>
            {
                if !self.anchor_generation_alive() {
                    self.reset_to(target, generation);
                }
                LookupStep::Miss
            }
            Some(_) => {
                self.reset_to(target, generation);
                LookupStep::Miss
            }
        }
    }

    pub(super) fn complete_extension_lookup(
        &mut self,
        (prior, target): (SettledChainAnchor, SettledChainAnchor),
        generation: &Weak<dyn Send + Sync>,
        height: BlockHeight,
        extends_prior: bool,
    ) -> Option<ServedTreeState> {
        if self.anchor == Some(prior) {
            if extends_prior {
                self.anchor = Some(target);
                self.anchor_generation = Some(generation.clone());
                return self.get_touching(height);
            }
            self.reset_to(target, generation);
            return None;
        }
        if self.anchor == Some(target) {
            return self.get_touching(height);
        }
        None
    }

    pub(super) fn cached_at_anchor(
        &mut self,
        target: SettledChainAnchor,
        height: BlockHeight,
    ) -> Option<ServedTreeState> {
        if !target.covers(height) || self.anchor != Some(target) {
            return None;
        }
        self.get_touching(height)
    }

    pub(super) fn remember(
        &mut self,
        anchor: SettledChainAnchor,
        generation: &Weak<dyn Send + Sync>,
        state: &ServedTreeState,
    ) {
        let height = state.block_id().height;
        if !anchor.covers(height) {
            return;
        }
        match self.anchor {
            None => {
                self.anchor = Some(anchor);
                self.anchor_generation = Some(generation.clone());
            }
            Some(current) if current == anchor => {
                self.refresh_generation_if_dropped(generation);
            }
            Some(current) if self.entries.is_empty() => {
                if !current.shares_immutable_identity(&anchor)
                    || anchor.settled_tip.height >= current.settled_tip.height
                    || !self.anchor_generation_alive()
                {
                    self.reset_to(anchor, generation);
                } else {
                    return;
                }
            }
            Some(current) if !current.shares_immutable_identity(&anchor) => {
                self.reset_to(anchor, generation);
            }
            Some(_) => return,
        }
        self.put(height, state.clone());
    }

    fn put(&mut self, height: BlockHeight, state: ServedTreeState) {
        let bytes = tree_state_byte_footprint(&state);
        if self.max_entries == 0 || bytes > self.max_bytes {
            return;
        }
        if let Some(removed) = self.entries.remove(&height) {
            self.lru.remove(&removed.tick);
            self.total_bytes = self.total_bytes.saturating_sub(removed.bytes);
        }
        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries
                || self.total_bytes.saturating_add(bytes) > self.max_bytes)
        {
            let Some((_, oldest_height)) = self.lru.pop_first() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest_height) {
                self.total_bytes = self.total_bytes.saturating_sub(evicted.bytes);
            }
        }
        let tick = self.allocate_tick();
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.lru.insert(tick, height);
        self.entries
            .insert(height, CachedEntry { state, bytes, tick });
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn total_bytes(&self) -> usize {
        self.total_bytes
    }
}

pub(super) fn tree_state_byte_footprint(tree_state: &ServedTreeState) -> usize {
    let pool_bytes = |protocol| {
        tree_state
            .final_state_bytes(protocol)
            .map_or(0, <[u8]>::len)
    };
    size_of::<ServedTreeState>()
        .saturating_add(pool_bytes(ShieldedProtocol::Sapling))
        .saturating_add(pool_bytes(ShieldedProtocol::Orchard))
        .saturating_add(pool_bytes(ShieldedProtocol::Ironwood))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockId, CanonicalHistoryBounds,
        CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers, Network,
        NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
        ShieldedProtocol,
    };
    use zinder_store::ServedTreeState;

    use super::{LookupStep, SettledChainAnchor, SettledStates, tree_state_byte_footprint};

    fn anchor(settled_height: u32, hash_byte: u8) -> SettledChainAnchor {
        SettledChainAnchor {
            network: Network::ZcashRegtest,
            artifact_schema: ArtifactSchemaVersion::new(1),
            retained: CanonicalHistoryBounds::complete(),
            schedule_revision: Some(1),
            activations_fingerprint: Some(NetworkUpgradeActivationsFingerprint::from_bytes(
                NetworkUpgradeActivationsFingerprintVersion::V1,
                [0x11; 32],
            )),
            settled_tip: BlockId::new(
                BlockHeight::new(settled_height),
                BlockHash::from_bytes([hash_byte; 32]),
            ),
        }
    }

    fn served_at(height: u32) -> Result<ServedTreeState, Box<dyn Error>> {
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let checkpoint = CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([7; 32])),
            height,
            CommitmentTreeFrontiers::from_validated_parts(Some(sapling), None, None)?,
        );
        Ok(ServedTreeState::from(checkpoint))
    }

    #[test]
    fn lru_evicts_least_recently_used_by_entry_count_and_byte_budget() -> Result<(), Box<dyn Error>>
    {
        let settled = anchor(100, 0xaa);
        let holder: Arc<dyn Send + Sync> = Arc::new(());
        let generation = Arc::downgrade(&holder);
        let first = served_at(10)?;
        let second = served_at(20)?;
        let third = served_at(30)?;
        let entry_bytes = tree_state_byte_footprint(&first);

        let mut by_count = SettledStates::new(2, entry_bytes * 10);
        by_count.remember(settled, &generation, &first);
        by_count.remember(settled, &generation, &second);
        assert_eq!(
            by_count.cached_at_anchor(settled, BlockHeight::new(10)),
            Some(first.clone())
        );
        by_count.remember(settled, &generation, &third);
        assert_eq!(by_count.len(), 2);
        assert!(
            by_count
                .cached_at_anchor(settled, BlockHeight::new(20))
                .is_none()
        );
        assert_eq!(
            by_count.cached_at_anchor(settled, BlockHeight::new(10)),
            Some(first.clone())
        );
        assert_eq!(
            by_count.cached_at_anchor(settled, BlockHeight::new(30)),
            Some(third.clone())
        );

        let mut by_bytes = SettledStates::new(10, entry_bytes * 2);
        by_bytes.remember(settled, &generation, &first);
        by_bytes.remember(settled, &generation, &second);
        assert_eq!(by_bytes.total_bytes(), 2 * entry_bytes);
        by_bytes.remember(settled, &generation, &third);
        assert_eq!(by_bytes.len(), 2);
        assert!(by_bytes.total_bytes() <= entry_bytes * 2);
        Ok(())
    }

    #[test]
    fn oversized_entry_does_not_evict_existing_cache_entries() -> Result<(), Box<dyn Error>> {
        let settled = anchor(100, 0xaa);
        let holder: Arc<dyn Send + Sync> = Arc::new(());
        let generation = Arc::downgrade(&holder);
        let small = served_at(10)?;
        let small_bytes = tree_state_byte_footprint(&small);

        let oversized = ServedTreeState::from(CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(20), BlockHash::from_bytes([8; 32])),
            20,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
                Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
                Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
            )?,
        ));
        let large_bytes = tree_state_byte_footprint(&oversized);
        assert!(large_bytes > small_bytes);

        let mut cache = SettledStates::new(4, small_bytes);
        cache.remember(settled, &generation, &small);
        assert_eq!(cache.len(), 1);
        cache.remember(settled, &generation, &oversized);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.cached_at_anchor(settled, BlockHeight::new(10)),
            Some(small)
        );
        assert!(
            cache
                .cached_at_anchor(settled, BlockHeight::new(20))
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn reinserting_same_height_updates_bytes_without_duplicating_entries()
    -> Result<(), Box<dyn Error>> {
        let settled = anchor(100, 0xaa);
        let holder: Arc<dyn Send + Sync> = Arc::new(());
        let generation = Arc::downgrade(&holder);
        let state = served_at(10)?;
        let entry_bytes = tree_state_byte_footprint(&state);
        let mut cache = SettledStates::new(4, entry_bytes * 4);
        cache.remember(settled, &generation, &state);
        cache.remember(settled, &generation, &state);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_bytes(), entry_bytes);
        Ok(())
    }

    #[test]
    fn unsettled_and_below_floor_heights_never_enter_settled_cache() -> Result<(), Box<dyn Error>> {
        let mut settled = anchor(50, 0xbb);
        settled.retained = CanonicalHistoryBounds::checkpointed(BlockId::new(
            BlockHeight::new(10),
            BlockHash::from_bytes([1; 32]),
        ))?;
        let holder: Arc<dyn Send + Sync> = Arc::new(());
        let generation = Arc::downgrade(&holder);
        let mut cache = SettledStates::new(8, 64 * 1024);

        cache.remember(settled, &generation, &served_at(9)?);
        cache.remember(settled, &generation, &served_at(51)?);
        assert_eq!(cache.len(), 0);

        cache.remember(settled, &generation, &served_at(10)?);
        cache.remember(settled, &generation, &served_at(50)?);
        assert_eq!(cache.len(), 2);
        assert!(matches!(
            cache.classify_lookup(settled, &generation, BlockHeight::new(9)),
            LookupStep::Miss
        ));
        Ok(())
    }
}
