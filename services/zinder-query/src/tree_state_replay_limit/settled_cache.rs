//! Bounded LRU cache of settled replayed [`ServedTreeState`] entries.
//!
//! Only heights within retained settled history enter this cache, anchored to
//! one [`SettledChainAnchor`]. When `settled_tip` advances at the same immutable
//! chain identity, one single-flight canonical header lookup at the prior
//! settled height proves the extension before promoting the anchor so cached
//! entries survive tip advances without rebuilding or re-reading storage.

use std::{
    collections::HashMap,
    fmt,
    mem::size_of,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
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

type CurrentCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Lookup and ownership context for the caller's storage reader generation.
#[derive(Clone)]
pub(crate) struct ReaderGeneration {
    weak: Weak<dyn Send + Sync>,
    current: CurrentCheck,
}

impl fmt::Debug for ReaderGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderGeneration")
            .field("addr", &self.addr())
            .finish_non_exhaustive()
    }
}

impl ReaderGeneration {
    /// Captures a weak reader handle alongside an authoritative current-ownership check.
    pub(crate) fn live<T: Send + Sync + 'static>(
        weak: &Weak<T>,
        is_current: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            weak: weak.clone(),
            current: Arc::new(is_current),
        }
    }

    #[cfg(test)]
    pub(crate) fn static_for_test<T: Send + Sync + 'static>(
        weak: &Weak<T>,
        is_current: bool,
    ) -> Self {
        Self::live(weak, move || is_current)
    }

    /// Returns the allocation address used to identify an active flight's reader generation.
    pub(crate) fn addr(&self) -> usize {
        self.weak.as_ptr().addr()
    }

    pub(super) fn is_current(&self) -> bool {
        (self.current)()
    }
}

/// Settled chain identity to which cached [`ServedTreeState`] rows belong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct SettledChainAnchor {
    pub(super) network: Network,
    pub(super) artifact_schema: ArtifactSchemaVersion,
    pub(super) retained: CanonicalHistoryBounds,
    pub(super) predecessor_digest: Option<[u8; 32]>,
    pub(super) schedule_revision: Option<u64>,
    pub(super) activations_fingerprint: Option<NetworkUpgradeActivationsFingerprint>,
    pub(super) settled_tip: BlockId,
}

impl SettledChainAnchor {
    /// Returns whether `height` lies within retained settled history under this anchor.
    pub(super) fn covers(&self, height: BlockHeight) -> bool {
        if self.retained.preceding_checkpoint().is_some() && self.predecessor_digest.is_none() {
            return false;
        }
        let predecessor = self
            .retained
            .preceding_checkpoint()
            .map_or(BlockHeight::new(0), |checkpoint| checkpoint.height);
        height >= predecessor && height <= self.settled_tip.height
    }

    /// Returns whether `other` preserves network, schema, retention, seed checkpoint, and upgrade schedule.
    pub(super) fn shares_immutable_identity(&self, other: &Self) -> bool {
        self.network == other.network
            && self.artifact_schema == other.artifact_schema
            && self.retained == other.retained
            && self.predecessor_digest == other.predecessor_digest
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
    last_used: AtomicU64,
}

/// Bounded LRU cache of settled replayed tree states.
#[derive(Debug)]
pub(super) struct SettledStates {
    max_entries: usize,
    max_bytes: usize,
    total_bytes: usize,
    next_tick: AtomicU64,
    anchor: Option<SettledChainAnchor>,
    anchor_generations: Vec<Weak<dyn Send + Sync>>,
    entries: HashMap<BlockHeight, CachedEntry>,
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
            next_tick: AtomicU64::new(0),
            anchor: None,
            anchor_generations: Vec::new(),
            entries: HashMap::new(),
        }
    }

    fn prune_dead_generations(&mut self) {
        self.anchor_generations
            .retain(|weak| weak.strong_count() > 0);
    }

    fn any_anchor_generation_alive(&mut self) -> bool {
        self.prune_dead_generations();
        !self.anchor_generations.is_empty()
    }

    fn adopt_generation(&mut self, generation: &Weak<dyn Send + Sync>) {
        self.prune_dead_generations();
        if generation.strong_count() == 0 {
            return;
        }
        let addr = generation.as_ptr().addr();
        if !self.tracks_generation(addr) {
            self.anchor_generations.push(generation.clone());
        }
    }

    fn allocate_tick(&self) -> u64 {
        self.next_tick.fetch_add(1, Ordering::Relaxed)
    }

    fn reset_to(&mut self, anchor: SettledChainAnchor, generation: &Weak<dyn Send + Sync>) {
        self.entries.clear();
        self.total_bytes = 0;
        self.anchor = Some(anchor);
        self.anchor_generations.clear();
        self.adopt_generation(generation);
    }

    fn get_touching(&self, height: BlockHeight) -> Option<ServedTreeState> {
        let entry = self.entries.get(&height)?;
        entry
            .last_used
            .store(self.allocate_tick(), Ordering::Relaxed);
        Some(entry.state.clone())
    }

    pub(super) fn classify_lookup(
        &mut self,
        target: SettledChainAnchor,
        generation: &ReaderGeneration,
        height: BlockHeight,
    ) -> LookupStep {
        if !target.covers(height) {
            return LookupStep::Miss;
        }
        let is_current = generation.is_current();
        match self.anchor {
            None => {
                if is_current {
                    self.reset_to(target, &generation.weak);
                }
                LookupStep::Miss
            }
            Some(current) if current == target => {
                if is_current {
                    self.adopt_generation(&generation.weak);
                }
                self.get_touching(height)
                    .map_or(LookupStep::Miss, LookupStep::Hit)
            }
            Some(_) if !is_current => LookupStep::Miss,
            Some(_) if self.entries.is_empty() => {
                self.reset_to(target, &generation.weak);
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
                if !self.any_anchor_generation_alive() {
                    self.reset_to(target, &generation.weak);
                }
                LookupStep::Miss
            }
            Some(_) => {
                self.reset_to(target, &generation.weak);
                LookupStep::Miss
            }
        }
    }

    pub(super) fn apply_extension_result(
        &mut self,
        (prior, target): (SettledChainAnchor, SettledChainAnchor),
        generation: &ReaderGeneration,
        extends_prior: bool,
    ) {
        let is_current = generation.is_current();
        if self.anchor == Some(prior) {
            if !is_current {
                return;
            }
            if extends_prior {
                self.anchor = Some(target);
                self.anchor_generations.clear();
                self.adopt_generation(&generation.weak);
            } else {
                self.reset_to(target, &generation.weak);
            }
        } else if self.anchor == Some(target) && is_current {
            self.adopt_generation(&generation.weak);
        }
    }

    pub(super) fn cached_after_extension(
        &mut self,
        target: SettledChainAnchor,
        generation: &ReaderGeneration,
        height: BlockHeight,
    ) -> Option<ServedTreeState> {
        if !target.covers(height) || self.anchor != Some(target) {
            return None;
        }
        if generation.is_current() {
            self.adopt_generation(&generation.weak);
        }
        self.get_touching(height)
    }

    pub(super) fn tracks_generation(&self, addr: usize) -> bool {
        self.anchor_generations
            .iter()
            .any(|generation| generation.as_ptr().addr() == addr)
    }

    /// Fast read-only lookup for a live reader generation already adopted into
    /// the current anchor.
    ///
    /// Because every adopted reader generation is immutable and
    /// `anchor_generations` is cleared whenever `self.anchor` changes, a live
    /// reader whose address is tracked is already guaranteed to match
    /// `self.anchor`.
    pub(super) fn cached_for_tracked_generation(
        &self,
        addr: usize,
        height: BlockHeight,
    ) -> Option<ServedTreeState> {
        if !self.tracks_generation(addr) || !self.anchor.is_some_and(|anchor| anchor.covers(height))
        {
            return None;
        }
        self.get_touching(height)
    }

    pub(super) fn cached_at_anchor(
        &self,
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
        generation: &ReaderGeneration,
        state: &ServedTreeState,
    ) {
        let height = state.block_id().height;
        if !anchor.covers(height) {
            return;
        }
        let is_current = generation.is_current();
        match self.anchor {
            None => {
                if !is_current {
                    return;
                }
                self.reset_to(anchor, &generation.weak);
            }
            Some(current) if current == anchor => {
                if is_current {
                    self.adopt_generation(&generation.weak);
                }
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
            self.total_bytes = self.total_bytes.saturating_sub(removed.bytes);
        }
        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries
                || self.total_bytes.saturating_add(bytes) > self.max_bytes)
        {
            let Some((&oldest_height, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used.load(Ordering::Relaxed))
            else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest_height) {
                self.total_bytes = self.total_bytes.saturating_sub(evicted.bytes);
            }
        }
        let tick = self.allocate_tick();
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.entries.insert(
            height,
            CachedEntry {
                state,
                bytes,
                last_used: AtomicU64::new(tick),
            },
        );
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
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockId, CanonicalHistoryBounds,
        CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers, Network,
        NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
        ShieldedProtocol,
    };
    use zinder_store::ServedTreeState;

    use super::{
        LookupStep, ReaderGeneration, SettledChainAnchor, SettledStates, tree_state_byte_footprint,
    };

    fn current_gen(holder: &Arc<()>) -> ReaderGeneration {
        ReaderGeneration::static_for_test(&Arc::downgrade(holder), true)
    }

    fn obsolete_gen(holder: &Arc<()>) -> ReaderGeneration {
        ReaderGeneration::static_for_test(&Arc::downgrade(holder), false)
    }

    fn anchor(settled_height: u32, hash_byte: u8) -> SettledChainAnchor {
        SettledChainAnchor {
            network: Network::ZcashRegtest,
            artifact_schema: ArtifactSchemaVersion::new(1),
            retained: CanonicalHistoryBounds::complete(),
            predecessor_digest: None,
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
        let holder = Arc::new(());
        let generation = current_gen(&holder);
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
        let holder = Arc::new(());
        let generation = current_gen(&holder);
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
        let holder = Arc::new(());
        let generation = current_gen(&holder);
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
        settled.predecessor_digest = Some([0x22; 32]);
        let holder = Arc::new(());
        let generation = current_gen(&holder);
        let mut cache = SettledStates::new(8, 64 * 1024);

        cache.remember(settled, &generation, &served_at(9)?);
        cache.remember(settled, &generation, &served_at(51)?);
        assert_eq!(cache.len(), 0);

        // Checkpointed anchors without a predecessor digest must fail closed.
        let unverified = SettledChainAnchor {
            predecessor_digest: None,
            ..settled
        };
        cache.remember(unverified, &generation, &served_at(10)?);
        assert_eq!(cache.len(), 0);
        assert!(!unverified.covers(BlockHeight::new(10)));

        cache.remember(settled, &generation, &served_at(10)?);
        cache.remember(settled, &generation, &served_at(50)?);
        assert_eq!(cache.len(), 2);
        assert!(matches!(
            cache.classify_lookup(settled, &generation, BlockHeight::new(9)),
            LookupStep::Miss
        ));
        Ok(())
    }

    #[test]
    fn same_anchor_handoff_tracks_new_reader_after_original_reader_drops()
    -> Result<(), Box<dyn Error>> {
        let settled = anchor(100, 0xaa);
        let reader_a = Arc::new(());
        let reader_b = Arc::new(());
        let lagging_reader = Arc::new(());
        let gen_a = current_gen(&reader_a);
        let gen_b = current_gen(&reader_b);
        let lagging_anchor = anchor(90, 0x99);
        let mut cache = SettledStates::new(8, 64 * 1024);

        let state = served_at(10)?;
        cache.remember(settled, &gen_a, &state);
        assert!(matches!(
            cache.classify_lookup(settled, &gen_b, BlockHeight::new(10)),
            LookupStep::Hit(_)
        ));
        drop(reader_a);

        // Even a lagging reader with is_current=true sees B is still live and must miss without
        // clearing B's active cache.
        assert!(matches!(
            cache.classify_lookup(
                lagging_anchor,
                &obsolete_gen(&lagging_reader),
                BlockHeight::new(10)
            ),
            LookupStep::Miss
        ));
        assert!(matches!(
            cache.classify_lookup(
                lagging_anchor,
                &current_gen(&lagging_reader),
                BlockHeight::new(10)
            ),
            LookupStep::Miss
        ));
        assert_eq!(
            cache.cached_at_anchor(settled, BlockHeight::new(10)),
            Some(state)
        );
        Ok(())
    }

    #[test]
    fn obsolete_pinned_readers_do_not_clear_current_cache_on_retention_or_fork_mismatch()
    -> Result<(), Box<dyn Error>> {
        let current = anchor(100, 0xaa);
        let different_retention = SettledChainAnchor {
            retained: CanonicalHistoryBounds::checkpointed(BlockId::new(
                BlockHeight::new(5),
                BlockHash::from_bytes([2; 32]),
            ))?,
            predecessor_digest: Some([0x22; 32]),
            ..current
        };
        let same_height_fork = anchor(100, 0xbb);
        let current_holder = Arc::new(());
        let pinned_holder = Arc::new(());
        let gen_current = current_gen(&current_holder);
        let gen_pinned = obsolete_gen(&pinned_holder);
        let mut cache = SettledStates::new(8, 64 * 1024);
        let expected = served_at(10)?;
        let pinned_state = served_at(11)?;
        cache.remember(current, &gen_current, &expected);

        for mismatch in [different_retention, same_height_fork] {
            assert!(matches!(
                cache.classify_lookup(mismatch, &gen_pinned, BlockHeight::new(10)),
                LookupStep::Miss
            ));
            cache.remember(mismatch, &gen_pinned, &pinned_state);
            assert!(matches!(
                cache.classify_lookup(current, &gen_current, BlockHeight::new(10)),
                LookupStep::Hit(ref state) if state == &expected
            ));
        }
        Ok(())
    }

    #[test]
    fn stale_replay_completion_after_replacement_does_not_overwrite_newer_anchor()
    -> Result<(), Box<dyn Error>> {
        let prior = anchor(100, 0xaa);
        let replacement = SettledChainAnchor {
            retained: CanonicalHistoryBounds::checkpointed(BlockId::new(
                BlockHeight::new(5),
                BlockHash::from_bytes([3; 32]),
            ))?,
            predecessor_digest: Some([0x33; 32]),
            ..prior
        };
        let old_holder = Arc::new(());
        let new_holder = Arc::new(());
        let old_captured_current = current_gen(&old_holder);
        let new_current = current_gen(&new_holder);

        // Replacement anchor established while cache is still empty: late old replay must be ignored.
        let mut empty_replacement = SettledStates::new(8, 64 * 1024);
        assert!(matches!(
            empty_replacement.classify_lookup(replacement, &new_current, BlockHeight::new(10)),
            LookupStep::Miss
        ));
        empty_replacement.remember(prior, &old_captured_current, &served_at(10)?);
        assert_eq!(empty_replacement.len(), 0);
        let replacement_state = served_at(12)?;
        empty_replacement.remember(replacement, &new_current, &replacement_state);
        assert_eq!(
            empty_replacement.cached_at_anchor(replacement, BlockHeight::new(12)),
            Some(replacement_state.clone())
        );

        // Replacement anchor already populated: late old replay must not discard entries.
        let mut populated_replacement = SettledStates::new(8, 64 * 1024);
        populated_replacement.remember(replacement, &new_current, &replacement_state);
        populated_replacement.remember(prior, &old_captured_current, &served_at(10)?);
        assert_eq!(populated_replacement.len(), 1);
        assert_eq!(
            populated_replacement.cached_at_anchor(replacement, BlockHeight::new(12)),
            Some(replacement_state)
        );
        Ok(())
    }

    #[test]
    fn delayed_lookup_from_superseded_request_does_not_clear_replacement_cache()
    -> Result<(), Box<dyn Error>> {
        let active_id = Arc::new(AtomicUsize::new(1));
        let holder_a = Arc::new(());
        let gen_a = ReaderGeneration::live(&Arc::downgrade(&holder_a), {
            let active = Arc::clone(&active_id);
            move || active.load(Ordering::Acquire) == 1
        });
        let fork = anchor(100, 0xcc);
        let retention_change = SettledChainAnchor {
            retained: CanonicalHistoryBounds::checkpointed(BlockId::new(
                BlockHeight::new(5),
                BlockHash::from_bytes([4; 32]),
            ))?,
            predecessor_digest: Some([0x44; 32]),
            ..anchor(100, 0xaa)
        };
        let schedule_change = SettledChainAnchor {
            schedule_revision: Some(2),
            ..anchor(100, 0xaa)
        };

        for (slot_id, replacement_anchor) in
            [(2, fork), (3, retention_change), (4, schedule_change)]
        {
            let holder_b = Arc::new(());
            let gen_b = ReaderGeneration::live(&Arc::downgrade(&holder_b), {
                let active = Arc::clone(&active_id);
                move || active.load(Ordering::Acquire) == slot_id
            });
            active_id.store(slot_id, Ordering::Release);

            let mut cache = SettledStates::new(8, 64 * 1024);
            let replacement_state = served_at(10)?;
            assert!(matches!(
                cache.classify_lookup(replacement_anchor, &gen_b, BlockHeight::new(10)),
                LookupStep::Miss
            ));
            cache.remember(replacement_anchor, &gen_b, &replacement_state);

            // Delayed Request A still uses gen_a created before replacement.
            assert!(matches!(
                cache.classify_lookup(anchor(100, 0xaa), &gen_a, BlockHeight::new(10)),
                LookupStep::Miss
            ));
            cache.apply_extension_result((anchor(90, 0x99), anchor(100, 0xaa)), &gen_a, true);
            assert!(matches!(
                cache.classify_lookup(replacement_anchor, &gen_b, BlockHeight::new(10)),
                LookupStep::Hit(ref state) if state == &replacement_state
            ));
        }

        // Also ensure delayed apply_extension_result does not promote prior anchor once superseded.
        let prior = anchor(90, 0x99);
        let stale_ext = anchor(100, 0xaa);
        active_id.store(1, Ordering::Release);
        let mut cache = SettledStates::new(8, 64 * 1024);
        let prior_state = served_at(10)?;
        cache.remember(prior, &gen_a, &prior_state);
        active_id.store(2, Ordering::Release);
        cache.apply_extension_result((prior, stale_ext), &gen_a, true);
        assert!(
            cache
                .cached_at_anchor(prior, BlockHeight::new(10))
                .is_some()
        );
        assert!(
            cache
                .cached_at_anchor(stale_ext, BlockHeight::new(10))
                .is_none()
        );
        Ok(())
    }
}
