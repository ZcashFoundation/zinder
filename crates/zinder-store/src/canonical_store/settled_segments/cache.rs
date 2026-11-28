//! Thread-safe bounded-LRU lazy cache of authenticated settled segment readers
//! keyed by `(start_height, SettledSegmentKind)` with miss singleflight deduplication.

use std::{
    collections::{HashMap, VecDeque},
    fmt, io,
    num::NonZeroU64,
    path::Path,
    sync::Arc,
};

use parking_lot::{Condvar, Mutex};
use zinder_core::{BlockHeight, BlockId};

use crate::{
    StoreRole, container_memory_budget_bytes,
    settled_raw_block_segment::{
        RawBlockSegmentIdentity, SettledRawBlockSegmentError, SettledRawBlockSegmentOpenOptions,
        SettledRawBlockSegmentReader,
    },
    settled_shielded_segment::{
        SettledShieldedSegmentError, SettledShieldedSegmentOpenOptions,
        SettledShieldedSegmentReader, ShieldedSegmentIdentity,
    },
    settled_supplement_segment::{
        SettledSupplementSegmentError, SettledSupplementSegmentOpenOptions,
        SettledSupplementSegmentReader, SupplementSegmentIdentity,
    },
};

use super::super::CanonicalSequenceCheckpoint;
use super::{
    SettledSegmentsError, VerifiedDir,
    layout::{
        SEGMENTS_DIR_NAME, SettledSegmentIndexEntry, SettledSegmentKind, SettledSegmentModules,
        format_segment_filename,
    },
    page_cache::SettledSegmentPageCache,
};

/// Default shared bounded-LRU segment reader capacity (`1,024` segments).
pub(crate) const DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY: usize = 1_024;

/// Bundle of shared authenticated readers for one settled segment stride.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct CachedSettledSegmentReaders {
    /// Authenticated `ZSHRNG02` shielded segment reader.
    pub(crate) shielded: Arc<SettledShieldedSegmentReader>,
    /// Authenticated `ZSTSUP01` supplement reader when enabled for this segment.
    pub(crate) supplement: Option<Arc<SettledSupplementSegmentReader>>,
    /// Authenticated `ZSBLKB01` raw-block reader when enabled for this segment.
    pub(crate) raw_block: Option<Arc<SettledRawBlockSegmentReader>>,
}

#[cfg(test)]
impl fmt::Debug for CachedSettledSegmentReaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedSettledSegmentReaders")
            .field("shielded_identity", &self.shielded.identity())
            .field("has_supplement", &self.supplement.is_some())
            .field("has_raw_block", &self.raw_block.is_some())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct CacheIdentityPin {
    start_height: BlockHeight,
    end_block: BlockId,
    modules: SettledSegmentModules,
    shielded_identity: ShieldedSegmentIdentity,
    supplement_identity: Option<SupplementSegmentIdentity>,
    raw_block_identity: Option<RawBlockSegmentIdentity>,
    end_sequence_checkpoint: CanonicalSequenceCheckpoint,
}

impl CacheIdentityPin {
    fn from_entry(entry: &SettledSegmentIndexEntry) -> Self {
        Self {
            start_height: entry.start_height,
            end_block: entry.end_block,
            modules: entry.modules,
            shielded_identity: entry.shielded_identity,
            supplement_identity: entry.supplement_identity,
            raw_block_identity: entry.raw_block_identity,
            end_sequence_checkpoint: entry.end_sequence_checkpoint,
        }
    }
}

#[derive(Clone)]
enum LoadedModuleReader {
    Shielded(Arc<SettledShieldedSegmentReader>),
    Supplement(Arc<SettledSupplementSegmentReader>),
    RawBlock(Arc<SettledRawBlockSegmentReader>),
}

impl LoadedModuleReader {
    const fn kind(&self) -> SettledSegmentKind {
        match self {
            Self::Shielded(_) => SettledSegmentKind::Shielded,
            Self::Supplement(_) => SettledSegmentKind::Supplement,
            Self::RawBlock(_) => SettledSegmentKind::RawBlock,
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "Prometheus gauges use f64 and segment indexes fit within its exact integer range"
    )]
    fn index_heap_bytes(&self) -> f64 {
        (match self {
            Self::Shielded(reader) => reader.index_heap_bytes(),
            Self::Supplement(reader) => reader.index_heap_bytes(),
            Self::RawBlock(reader) => reader.index_heap_bytes(),
        }) as f64
    }
}

const SEGMENT_KINDS: [SettledSegmentKind; 3] = [
    SettledSegmentKind::Shielded,
    SettledSegmentKind::Supplement,
    SettledSegmentKind::RawBlock,
];

struct CacheKindMetrics {
    open_readers: metrics::Gauge,
    index_heap_bytes: metrics::Gauge,
    hits: metrics::Counter,
    misses: metrics::Counter,
    evictions: metrics::Counter,
}

impl CacheKindMetrics {
    fn new(kind: SettledSegmentKind) -> Self {
        let kind = kind.metric_label();
        Self {
            open_readers: metrics::gauge!("zinder_store_settled_segment_cache_open_readers", "kind" => kind),
            index_heap_bytes: metrics::gauge!("zinder_store_settled_segment_cache_index_heap_bytes", "kind" => kind),
            hits: metrics::counter!("zinder_store_settled_segment_cache_hits_total", "kind" => kind),
            misses: metrics::counter!("zinder_store_settled_segment_cache_misses_total", "kind" => kind),
            evictions: metrics::counter!("zinder_store_settled_segment_cache_evictions_total", "kind" => kind),
        }
    }
}

struct CacheMetrics([CacheKindMetrics; 3]);

impl CacheMetrics {
    fn for_kind(&self, kind: SettledSegmentKind) -> &CacheKindMetrics {
        &self.0[match kind {
            SettledSegmentKind::Shielded => 0,
            SettledSegmentKind::Supplement => 1,
            SettledSegmentKind::RawBlock => 2,
        }]
    }

    fn remove_slot(&self, slot: &CachedSlot, evicted: bool) {
        for kind in SEGMENT_KINDS {
            if let Some(reader) = slot.module(kind) {
                let metrics = self.for_kind(kind);
                metrics.open_readers.decrement(1.0);
                metrics
                    .index_heap_bytes
                    .decrement(reader.index_heap_bytes());
                if evicted {
                    metrics.evictions.increment(1);
                }
            }
        }
    }
}

#[derive(Clone)]
enum CachedOpenFailure {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    ShieldedFsVerityRequired,
    SupplementFsVerityRequired,
    RawBlockFsVerityRequired,
    InvalidLayout(String),
    InvalidFormat(String),
    Integrity(String),
    ChainLinkage(String),
}

impl CachedOpenFailure {
    fn from_error(err: &SettledSegmentsError) -> Self {
        match err {
            SettledSegmentsError::Io(source) => Self::Io {
                kind: source.kind(),
                message: source.to_string(),
            },
            SettledSegmentsError::Shielded(other) => match other {
                SettledShieldedSegmentError::Io(source) => Self::Io {
                    kind: source.kind(),
                    message: source.to_string(),
                },
                SettledShieldedSegmentError::FsVerityRequired => Self::ShieldedFsVerityRequired,
                SettledShieldedSegmentError::InvalidFormat(reason) => {
                    Self::InvalidFormat(reason.clone())
                }
                SettledShieldedSegmentError::ChainLinkage(reason) => {
                    Self::ChainLinkage(reason.clone())
                }
                SettledShieldedSegmentError::PayloadIntegrity(_)
                | SettledShieldedSegmentError::PayloadLimitExceeded { .. }
                | SettledShieldedSegmentError::HeightOutOfRange { .. }
                | SettledShieldedSegmentError::Cancelled => Self::Integrity(other.to_string()),
            },
            SettledSegmentsError::Supplement(other) => match other {
                SettledSupplementSegmentError::Io(source) => Self::Io {
                    kind: source.kind(),
                    message: source.to_string(),
                },
                SettledSupplementSegmentError::FsVerityRequired => Self::SupplementFsVerityRequired,
                SettledSupplementSegmentError::InvalidFormat(reason) => {
                    Self::InvalidFormat(reason.clone())
                }
                SettledSupplementSegmentError::ChainLinkage(reason) => {
                    Self::ChainLinkage(reason.clone())
                }
                SettledSupplementSegmentError::InvalidCodecId(_)
                | SettledSupplementSegmentError::PayloadIntegrity(_)
                | SettledSupplementSegmentError::PayloadLimitExceeded { .. }
                | SettledSupplementSegmentError::HeightOutOfRange { .. } => {
                    Self::Integrity(other.to_string())
                }
            },
            SettledSegmentsError::RawBlock(other) => match other {
                SettledRawBlockSegmentError::Io(source) => Self::Io {
                    kind: source.kind(),
                    message: source.to_string(),
                },
                SettledRawBlockSegmentError::FsVerityRequired => Self::RawBlockFsVerityRequired,
                SettledRawBlockSegmentError::InvalidFormat(reason) => {
                    Self::InvalidFormat(reason.clone())
                }
                SettledRawBlockSegmentError::ChainLinkage(reason) => {
                    Self::ChainLinkage(reason.clone())
                }
                SettledRawBlockSegmentError::PayloadIntegrity(_)
                | SettledRawBlockSegmentError::PayloadLimitExceeded { .. }
                | SettledRawBlockSegmentError::HeightOutOfRange { .. }
                | SettledRawBlockSegmentError::TransactionIndexOutOfRange { .. } => {
                    Self::Integrity(other.to_string())
                }
            },
            SettledSegmentsError::Json(source) => Self::InvalidFormat(source.to_string()),
            SettledSegmentsError::InvalidLayout(reason) => Self::InvalidLayout(reason.clone()),
            SettledSegmentsError::InvalidFormat(reason) => Self::InvalidFormat(reason.clone()),
            SettledSegmentsError::Integrity(reason) => Self::Integrity(reason.clone()),
            SettledSegmentsError::ChainLinkage(reason) => Self::ChainLinkage(reason.clone()),
        }
    }

    fn to_error(&self) -> SettledSegmentsError {
        match self {
            Self::Io { kind, message } => {
                SettledSegmentsError::Io(io::Error::new(*kind, message.clone()))
            }
            Self::ShieldedFsVerityRequired => {
                SettledSegmentsError::Shielded(SettledShieldedSegmentError::FsVerityRequired)
            }
            Self::SupplementFsVerityRequired => {
                SettledSegmentsError::Supplement(SettledSupplementSegmentError::FsVerityRequired)
            }
            Self::RawBlockFsVerityRequired => {
                SettledSegmentsError::RawBlock(SettledRawBlockSegmentError::FsVerityRequired)
            }
            Self::InvalidLayout(reason) => SettledSegmentsError::InvalidLayout(reason.clone()),
            Self::InvalidFormat(reason) => SettledSegmentsError::InvalidFormat(reason.clone()),
            Self::Integrity(reason) => SettledSegmentsError::Integrity(reason.clone()),
            Self::ChainLinkage(reason) => SettledSegmentsError::ChainLinkage(reason.clone()),
        }
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct InflightKey {
    start_height: BlockHeight,
    kind: SettledSegmentKind,
}

struct InflightLoad {
    pin: CacheIdentityPin,
    generation: u64,
    state: Mutex<Option<Result<LoadedModuleReader, CachedOpenFailure>>>,
    ready: Condvar,
}

impl InflightLoad {
    const fn new(pin: CacheIdentityPin, generation: u64) -> Self {
        Self {
            pin,
            generation,
            state: Mutex::new(None),
            ready: Condvar::new(),
        }
    }
}

struct CachedSlot {
    pin: CacheIdentityPin,
    shielded: Option<Arc<SettledShieldedSegmentReader>>,
    supplement: Option<Arc<SettledSupplementSegmentReader>>,
    raw_block: Option<Arc<SettledRawBlockSegmentReader>>,
}

impl CachedSlot {
    const fn empty(pin: CacheIdentityPin) -> Self {
        Self {
            pin,
            shielded: None,
            supplement: None,
            raw_block: None,
        }
    }

    fn module(&self, kind: SettledSegmentKind) -> Option<LoadedModuleReader> {
        match kind {
            SettledSegmentKind::Shielded => self.shielded.clone().map(LoadedModuleReader::Shielded),
            SettledSegmentKind::Supplement => {
                self.supplement.clone().map(LoadedModuleReader::Supplement)
            }
            SettledSegmentKind::RawBlock => {
                self.raw_block.clone().map(LoadedModuleReader::RawBlock)
            }
        }
    }

    fn set_module(&mut self, reader: LoadedModuleReader, metrics: &CacheMetrics) {
        let kind = reader.kind();
        let metrics = metrics.for_kind(kind);
        let prior = self.module(kind);
        let prior_bytes = prior
            .as_ref()
            .map_or(0.0, LoadedModuleReader::index_heap_bytes);
        metrics
            .index_heap_bytes
            .increment(reader.index_heap_bytes() - prior_bytes);
        if prior.is_none() {
            metrics.open_readers.increment(1.0);
        }
        match reader {
            LoadedModuleReader::Shielded(inner) => self.shielded = Some(inner),
            LoadedModuleReader::Supplement(inner) => self.supplement = Some(inner),
            LoadedModuleReader::RawBlock(inner) => self.raw_block = Some(inner),
        }
    }
}

struct LruReaderState {
    slots: HashMap<BlockHeight, CachedSlot>,
    lru_order: VecDeque<BlockHeight>,
    inflight: HashMap<InflightKey, Arc<InflightLoad>>,
    generation: u64,
    page_cache: SettledSegmentPageCache,
}

impl LruReaderState {
    fn new(capacity: usize) -> Self {
        Self {
            slots: HashMap::with_capacity(capacity),
            lru_order: VecDeque::with_capacity(capacity),
            inflight: HashMap::new(),
            generation: 0,
            page_cache: SettledSegmentPageCache::default(),
        }
    }

    fn touch(&mut self, height: BlockHeight) {
        if let Some(pos) = self.lru_order.iter().position(|queued| *queued == height) {
            self.lru_order.remove(pos);
        }
        self.lru_order.push_back(height);
    }

    fn clear_resident_readers(&mut self, metrics: &CacheMetrics) {
        for (_, slot) in self.slots.drain() {
            metrics.remove_slot(&slot, false);
        }
        self.lru_order.clear();
    }
}

#[cfg(test)]
type OpenHook = Arc<dyn Fn(SettledSegmentKind) + Send + Sync>;

#[cfg(test)]
#[derive(Default)]
struct CacheTestHooks {
    shielded_opens: std::sync::atomic::AtomicUsize,
    supplement_opens: std::sync::atomic::AtomicUsize,
    raw_block_opens: std::sync::atomic::AtomicUsize,
    on_disk_open: Mutex<Option<OpenHook>>,
}

/// Bounded-LRU lazy reader cache over canonical settled segments with per-module
/// singleflight deduplication on cold cache misses.
pub(crate) struct SettledSegmentReaderCache {
    root_dir: VerifiedDir,
    max_open_segments: usize,
    require_fs_verity: bool,
    inner: Mutex<LruReaderState>,
    metrics: CacheMetrics,
    #[cfg(test)]
    test_hooks: CacheTestHooks,
}

impl Drop for SettledSegmentReaderCache {
    fn drop(&mut self) {
        for slot in self.inner.get_mut().slots.values() {
            self.metrics.remove_slot(slot, false);
        }
    }
}

impl fmt::Debug for SettledSegmentReaderCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let open_count = self.inner.lock().slots.len();
        formatter
            .debug_struct("SettledSegmentReaderCache")
            .field("store_root", &self.root_dir.display_path())
            .field("max_open_segments", &self.max_open_segments)
            .field("require_fs_verity", &self.require_fs_verity)
            .field("open_segments", &open_count)
            .finish_non_exhaustive()
    }
}

enum MissAcquisition {
    Hit(LoadedModuleReader),
    Wait(Arc<InflightLoad>),
    Lead(Arc<InflightLoad>),
}

struct InflightLeaderGuard<'a> {
    cache: &'a SettledSegmentReaderCache,
    key: InflightKey,
    inflight: Arc<InflightLoad>,
    active: bool,
}

impl Drop for InflightLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut guard = self.cache.inner.lock();
        if guard
            .inflight
            .get(&self.key)
            .is_some_and(|existing| Arc::ptr_eq(existing, &self.inflight))
        {
            guard.inflight.remove(&self.key);
        }
        drop(guard);

        let mut state_guard = self.inflight.state.lock();
        if state_guard.is_none() {
            *state_guard = Some(Err(CachedOpenFailure::ChainLinkage(
                "concurrent segment open cancelled before completion".to_owned(),
            )));
            drop(state_guard);
            self.inflight.ready.notify_all();
        } else {
            drop(state_guard);
        }
    }
}

impl SettledSegmentReaderCache {
    /// Creates a bounded reader cache opening segments under `store_root`.
    pub(crate) fn new(
        store_root: impl AsRef<Path>,
        max_open_segments: usize,
        require_fs_verity: bool,
    ) -> Result<Self, SettledSegmentsError> {
        if max_open_segments == 0 {
            return Err(SettledSegmentsError::InvalidLayout(
                "SettledSegmentReaderCache max_open_segments must be positive".to_owned(),
            ));
        }
        let root_dir = VerifiedDir::open_existing(store_root.as_ref(), &[])?;
        Ok(Self {
            root_dir,
            max_open_segments,
            require_fs_verity,
            inner: Mutex::new(LruReaderState::new(max_open_segments)),
            metrics: CacheMetrics(SEGMENT_KINDS.map(CacheKindMetrics::new)),
            #[cfg(test)]
            test_hooks: CacheTestHooks::default(),
        })
    }

    /// Creates a shared reader cache with [`DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY`] (`1,024`).
    #[cfg(test)]
    pub(crate) fn with_default_capacity(
        store_root: impl AsRef<Path>,
        require_fs_verity: bool,
    ) -> Result<Arc<Self>, SettledSegmentsError> {
        Ok(Arc::new(Self::new(
            store_root,
            DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY,
            require_fs_verity,
        )?))
    }

    pub(in crate::canonical_store) fn with_admitted_corpus(
        store_root: &Path,
        entries: &[SettledSegmentIndexEntry],
    ) -> Result<Arc<Self>, SettledSegmentsError> {
        Self::with_corpus_budget(
            store_root,
            entries,
            container_memory_budget_bytes().and_then(NonZeroU64::new),
        )
    }

    pub(in crate::canonical_store) fn with_corpus_budget(
        store_root: &Path,
        entries: &[SettledSegmentIndexEntry],
        memory_budget: Option<NonZeroU64>,
    ) -> Result<Arc<Self>, SettledSegmentsError> {
        let mut cache = Self::new(
            store_root,
            DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY,
            false,
        )?;
        cache.inner.get_mut().page_cache =
            SettledSegmentPageCache::measure(&cache.root_dir, entries, memory_budget)?;
        Ok(Arc::new(cache))
    }

    pub(in crate::canonical_store) fn record_corpus_admission(&self, role: StoreRole) {
        self.record_corpus_admission_transition(role, true);
    }

    pub(in crate::canonical_store) fn record_corpus_admission_transition(
        &self,
        role: StoreRole,
        warn_if_constrained: bool,
    ) {
        let page_cache = self.inner.lock().page_cache;
        page_cache.record_admission(role, warn_if_constrained);
    }

    pub(in crate::canonical_store) fn prepare_corpus_growth(
        &self,
        entries: &[SettledSegmentIndexEntry],
    ) -> Result<SettledSegmentPageCache, SettledSegmentsError> {
        let page_cache = self.inner.lock().page_cache;
        page_cache.with_new_entries(&self.root_dir, entries)
    }

    pub(in crate::canonical_store) fn admit_corpus_growth(
        &self,
        page_cache: SettledSegmentPageCache,
        role: StoreRole,
    ) {
        let mut inner = self.inner.lock();
        let changed = inner.page_cache.constrained() != page_cache.constrained();
        inner.page_cache = page_cache;
        if changed {
            inner.generation = inner.generation.wrapping_add(1);
            inner.clear_resident_readers(&self.metrics);
            inner.inflight.clear();
        }
        drop(inner);
        page_cache.record_admission(role, changed);
    }

    pub(in crate::canonical_store) fn constrained(&self) -> bool {
        self.inner.lock().page_cache.constrained()
    }

    /// Returns the configured maximum number of open segments held in the cache.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn capacity(&self) -> usize {
        self.max_open_segments
    }

    /// Returns the number of segments currently cached open.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().slots.len()
    }

    /// Returns `true` when zero segments are cached open.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock().slots.is_empty()
    }

    /// Returns `true` if any module at `start_height` is currently cached.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn contains_start_height(&self, start_height: BlockHeight) -> bool {
        self.inner.lock().slots.contains_key(&start_height)
    }

    /// Returns `true` when `kind` for `start_height` is currently cached open.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn contains_module(
        &self,
        start_height: BlockHeight,
        kind: SettledSegmentKind,
    ) -> bool {
        self.inner
            .lock()
            .slots
            .get(&start_height)
            .is_some_and(|slot| slot.module(kind).is_some())
    }

    /// Returns the cumulative number of disk opens completed or started for `kind`.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn open_count(&self, kind: SettledSegmentKind) -> usize {
        use std::sync::atomic::Ordering;
        match kind {
            SettledSegmentKind::Shielded => self.test_hooks.shielded_opens.load(Ordering::Acquire),
            SettledSegmentKind::Supplement => {
                self.test_hooks.supplement_opens.load(Ordering::Acquire)
            }
            SettledSegmentKind::RawBlock => self.test_hooks.raw_block_opens.load(Ordering::Acquire),
        }
    }

    /// Installs a deterministic callback invoked when a singleflight leader opens a module from disk.
    #[cfg(test)]
    pub(crate) fn set_on_disk_open_hook(&self, hook: Option<OpenHook>) {
        *self.test_hooks.on_disk_open.lock() = hook;
    }

    /// Evicts any cached readers for `start_height`, returning `true` if present.
    #[cfg(test)]
    pub(crate) fn evict(&self, start_height: BlockHeight) -> bool {
        let mut guard = self.inner.lock();
        guard.generation = guard.generation.wrapping_add(1);
        if let Some(pos) = guard
            .lru_order
            .iter()
            .position(|queued| *queued == start_height)
        {
            guard.lru_order.remove(pos);
        }
        let removed = guard.slots.remove(&start_height);
        if let Some(slot) = &removed {
            self.metrics.remove_slot(slot, false);
        }
        drop(guard);
        removed.is_some()
    }

    /// Drops all cached open readers.
    #[cfg(test)]
    pub(crate) fn clear(&self) {
        let mut guard = self.inner.lock();
        guard.generation = guard.generation.wrapping_add(1);
        guard.clear_resident_readers(&self.metrics);
    }

    /// Lazily opens (or returns cached) ONLY the `ZSHRNG02` shielded segment reader.
    ///
    /// Never opens or hashes `ZSTSUP01` supplement or `ZSBLKB01` raw-block files.
    pub(crate) fn get_or_open_shielded(
        &self,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<Arc<SettledShieldedSegmentReader>, SettledSegmentsError> {
        match self.acquire_module(entry, SettledSegmentKind::Shielded)? {
            LoadedModuleReader::Shielded(reader) => Ok(reader),
            LoadedModuleReader::Supplement(_) | LoadedModuleReader::RawBlock(_) => {
                Err(SettledSegmentsError::ChainLinkage(
                    "unexpected reader kind returned for shielded module".to_owned(),
                ))
            }
        }
    }

    /// Lazily opens (or returns cached) ONLY the `ZSTSUP01` supplement reader
    /// when `entry.modules.supplement` is set, returning `Ok(None)` otherwise.
    pub(crate) fn get_or_open_supplement(
        &self,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<Option<Arc<SettledSupplementSegmentReader>>, SettledSegmentsError> {
        entry.validate()?;
        if !entry.modules.supplement {
            self.verify_existing_slot_pin(entry)?;
            return Ok(None);
        }
        match self.acquire_module(entry, SettledSegmentKind::Supplement)? {
            LoadedModuleReader::Supplement(reader) => Ok(Some(reader)),
            LoadedModuleReader::Shielded(_) | LoadedModuleReader::RawBlock(_) => {
                Err(SettledSegmentsError::ChainLinkage(
                    "unexpected reader kind returned for supplement module".to_owned(),
                ))
            }
        }
    }

    /// Lazily opens (or returns cached) ONLY the `ZSBLKB01` raw-block reader
    /// when `entry.modules.raw_bodies` is set, returning `Ok(None)` otherwise.
    pub(crate) fn get_or_open_raw_block(
        &self,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<Option<Arc<SettledRawBlockSegmentReader>>, SettledSegmentsError> {
        entry.validate()?;
        if !entry.modules.raw_bodies {
            self.verify_existing_slot_pin(entry)?;
            return Ok(None);
        }
        match self.acquire_module(entry, SettledSegmentKind::RawBlock)? {
            LoadedModuleReader::RawBlock(reader) => Ok(Some(reader)),
            LoadedModuleReader::Shielded(_) | LoadedModuleReader::Supplement(_) => {
                Err(SettledSegmentsError::ChainLinkage(
                    "unexpected reader kind returned for raw_block module".to_owned(),
                ))
            }
        }
    }

    /// Opens or returns all enabled authenticated module readers for `entry`
    /// using the lazy singleflight path per enabled module.
    #[cfg(test)]
    pub(crate) fn get_or_open(
        &self,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<CachedSettledSegmentReaders, SettledSegmentsError> {
        let shielded = self.get_or_open_shielded(entry)?;
        let supplement = self.get_or_open_supplement(entry)?;
        let raw_block = self.get_or_open_raw_block(entry)?;
        Ok(CachedSettledSegmentReaders {
            shielded,
            supplement,
            raw_block,
        })
    }

    /// Inserts already-verified readers opened during cold admission so
    /// subsequent replay validation reuses them without reopening segment files.
    pub(crate) fn insert_verified_readers(
        &self,
        entry: &SettledSegmentIndexEntry,
        shielded: Arc<SettledShieldedSegmentReader>,
        supplement: Option<Arc<SettledSupplementSegmentReader>>,
        raw_block: Option<Arc<SettledRawBlockSegmentReader>>,
    ) -> Result<(), SettledSegmentsError> {
        entry.validate()?;
        let expected_pin = CacheIdentityPin::from_entry(entry);
        let mut guard = self.inner.lock();
        self.insert_module_into_slot(
            &mut guard,
            entry.start_height,
            &expected_pin,
            LoadedModuleReader::Shielded(shielded),
        )
        .map_err(|failure| failure.to_error())?;
        if let Some(sup_reader) = supplement {
            self.insert_module_into_slot(
                &mut guard,
                entry.start_height,
                &expected_pin,
                LoadedModuleReader::Supplement(sup_reader),
            )
            .map_err(|failure| failure.to_error())?;
        }
        if let Some(raw_reader) = raw_block {
            self.insert_module_into_slot(
                &mut guard,
                entry.start_height,
                &expected_pin,
                LoadedModuleReader::RawBlock(raw_reader),
            )
            .map_err(|failure| failure.to_error())?;
        }
        Ok(())
    }

    fn verify_existing_slot_pin(
        &self,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<(), SettledSegmentsError> {
        let expected_pin = CacheIdentityPin::from_entry(entry);
        let guard = self.inner.lock();
        let mismatch = guard
            .slots
            .get(&entry.start_height)
            .is_some_and(|slot| slot.pin != expected_pin);
        drop(guard);
        if mismatch {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "cached segment at start_height {} does not match requested identity",
                entry.start_height.value()
            )));
        }
        Ok(())
    }

    fn acquire_module(
        &self,
        entry: &SettledSegmentIndexEntry,
        kind: SettledSegmentKind,
    ) -> Result<LoadedModuleReader, SettledSegmentsError> {
        entry.validate()?;
        let expected_pin = CacheIdentityPin::from_entry(entry);
        let acquisition = self.lookup_or_claim_miss(entry.start_height, kind, &expected_pin)?;
        let metrics = self.metrics.for_kind(kind);
        if matches!(acquisition, MissAcquisition::Hit(_)) {
            metrics.hits.increment(1);
        } else {
            metrics.misses.increment(1);
        }
        match acquisition {
            MissAcquisition::Hit(reader) => Ok(reader),
            MissAcquisition::Wait(inflight) => {
                let mut state_guard = inflight.state.lock();
                while state_guard.is_none() {
                    inflight.ready.wait(&mut state_guard);
                }
                let outcome = match state_guard.as_ref() {
                    Some(Ok(reader)) => Ok(reader.clone()),
                    Some(Err(failure)) => Err(failure.to_error()),
                    None => Err(SettledSegmentsError::ChainLinkage(
                        "concurrent segment load exited without status".to_owned(),
                    )),
                };
                drop(state_guard);
                outcome
            }
            MissAcquisition::Lead(inflight) => {
                self.execute_leader_open(entry, kind, &expected_pin, &inflight)
            }
        }
    }

    fn lookup_or_claim_miss(
        &self,
        start_height: BlockHeight,
        kind: SettledSegmentKind,
        expected_pin: &CacheIdentityPin,
    ) -> Result<MissAcquisition, SettledSegmentsError> {
        let key = InflightKey { start_height, kind };
        let mut guard = self.inner.lock();
        if let Some(slot) = guard.slots.get(&start_height) {
            if slot.pin != *expected_pin {
                drop(guard);
                return Err(SettledSegmentsError::ChainLinkage(format!(
                    "cached segment at start_height {} does not match requested identity",
                    start_height.value()
                )));
            }
            if let Some(reader) = slot.module(kind) {
                guard.touch(start_height);
                return Ok(MissAcquisition::Hit(reader));
            }
        }
        if let Some(existing) = guard.inflight.get(&key) {
            if existing.pin != *expected_pin {
                drop(guard);
                return Err(SettledSegmentsError::ChainLinkage(format!(
                    "inflight segment at start_height {} does not match requested identity",
                    start_height.value()
                )));
            }
            return Ok(MissAcquisition::Wait(Arc::clone(existing)));
        }
        let inflight = Arc::new(InflightLoad::new(expected_pin.clone(), guard.generation));
        guard.inflight.insert(key, Arc::clone(&inflight));
        drop(guard);
        Ok(MissAcquisition::Lead(inflight))
    }

    fn execute_leader_open(
        &self,
        entry: &SettledSegmentIndexEntry,
        kind: SettledSegmentKind,
        expected_pin: &CacheIdentityPin,
        inflight: &Arc<InflightLoad>,
    ) -> Result<LoadedModuleReader, SettledSegmentsError> {
        let key = InflightKey {
            start_height: entry.start_height,
            kind,
        };
        let mut leader_guard = InflightLeaderGuard {
            cache: self,
            key,
            inflight: Arc::clone(inflight),
            active: true,
        };
        #[cfg(test)]
        {
            self.record_test_open(kind);
            let open_hook = self.test_hooks.on_disk_open.lock().clone();
            if let Some(hook) = open_hook {
                hook(kind);
            }
        }
        let loaded = open_single_module(self, entry, kind)
            .map_err(|err| CachedOpenFailure::from_error(&err));
        let final_result = self.commit_leader_outcome(key, expected_pin, inflight, loaded);

        let mut state_guard = inflight.state.lock();
        *state_guard = Some(final_result.clone());
        drop(state_guard);
        inflight.ready.notify_all();
        leader_guard.active = false;

        final_result.map_err(|failure| failure.to_error())
    }

    fn commit_leader_outcome(
        &self,
        key: InflightKey,
        expected_pin: &CacheIdentityPin,
        inflight: &Arc<InflightLoad>,
        loaded: Result<LoadedModuleReader, CachedOpenFailure>,
    ) -> Result<LoadedModuleReader, CachedOpenFailure> {
        let mut guard = self.inner.lock();
        if guard
            .inflight
            .get(&key)
            .is_some_and(|existing| Arc::ptr_eq(existing, inflight))
        {
            guard.inflight.remove(&key);
        }
        let reader = loaded?;
        if guard.generation == inflight.generation {
            self.insert_module_into_slot(
                &mut guard,
                key.start_height,
                expected_pin,
                reader.clone(),
            )?;
        }
        drop(guard);
        Ok(reader)
    }

    fn insert_module_into_slot(
        &self,
        guard: &mut LruReaderState,
        start_height: BlockHeight,
        expected_pin: &CacheIdentityPin,
        reader: LoadedModuleReader,
    ) -> Result<(), CachedOpenFailure> {
        if let Some(existing) = guard.slots.get_mut(&start_height) {
            if existing.pin != *expected_pin {
                return Err(CachedOpenFailure::ChainLinkage(format!(
                    "cached segment at start_height {} changed identity concurrently",
                    start_height.value()
                )));
            }
            existing.set_module(reader, &self.metrics);
            guard.touch(start_height);
            return Ok(());
        }
        while guard.slots.len() >= self.max_open_segments {
            let Some(evict_height) = guard.lru_order.pop_front() else {
                break;
            };
            if let Some(slot) = guard.slots.remove(&evict_height) {
                self.metrics.remove_slot(&slot, true);
            }
        }
        let mut new_slot = CachedSlot::empty(expected_pin.clone());
        new_slot.set_module(reader, &self.metrics);
        guard.slots.insert(start_height, new_slot);
        guard.lru_order.push_back(start_height);
        Ok(())
    }

    #[cfg(test)]
    fn record_test_open(&self, kind: SettledSegmentKind) {
        use std::sync::atomic::Ordering;
        match kind {
            SettledSegmentKind::Shielded => {
                self.test_hooks
                    .shielded_opens
                    .fetch_add(1, Ordering::AcqRel);
            }
            SettledSegmentKind::Supplement => {
                self.test_hooks
                    .supplement_opens
                    .fetch_add(1, Ordering::AcqRel);
            }
            SettledSegmentKind::RawBlock => {
                self.test_hooks
                    .raw_block_opens
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

fn open_single_module(
    cache: &SettledSegmentReaderCache,
    entry: &SettledSegmentIndexEntry,
    kind: SettledSegmentKind,
) -> Result<LoadedModuleReader, SettledSegmentsError> {
    let segments_dir = cache.root_dir.open_child_dir_existing(SEGMENTS_DIR_NAME)?;
    let require_fs_verity = cache.require_fs_verity;
    let start = entry.start_height;
    let end = entry.end_block.height;
    let file = segments_dir
        .open_child_dir_existing(kind.subdir_name())?
        .open_regular_file_nofollow(&format_segment_filename(kind, start, end)?)?;
    let constrained = cache.constrained();

    match kind {
        SettledSegmentKind::Shielded => Ok(LoadedModuleReader::Shielded(Arc::new(
            SettledShieldedSegmentReader::open_file_with_scan_policy(
                file,
                &entry.shielded_identity,
                SettledShieldedSegmentOpenOptions {
                    require_fs_verity,
                    verify_full_payload: false,
                },
                constrained,
            )?,
        ))),
        SettledSegmentKind::Supplement => {
            let sup_id = entry.supplement_identity.as_ref().ok_or_else(|| {
                SettledSegmentsError::InvalidFormat(
                    "supplement_identity missing for active supplement module".to_owned(),
                )
            })?;
            Ok(LoadedModuleReader::Supplement(Arc::new(
                SettledSupplementSegmentReader::open_file_with_scan_policy(
                    file,
                    sup_id,
                    SettledSupplementSegmentOpenOptions {
                        require_fs_verity,
                        verify_full_payload: false,
                    },
                    constrained,
                )?,
            )))
        }
        SettledSegmentKind::RawBlock => {
            let raw_id = entry.raw_block_identity.as_ref().ok_or_else(|| {
                SettledSegmentsError::InvalidFormat(
                    "raw_block_identity missing for active raw_bodies module".to_owned(),
                )
            })?;
            Ok(LoadedModuleReader::RawBlock(Arc::new(
                SettledRawBlockSegmentReader::open_file_with_scan_policy(
                    file,
                    raw_id,
                    SettledRawBlockSegmentOpenOptions {
                        require_fs_verity,
                        verify_full_payload: false,
                    },
                    constrained,
                )?,
            )))
        }
    }
}
