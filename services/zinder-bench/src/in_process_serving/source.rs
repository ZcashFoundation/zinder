//! A node source whose tip is revealed one block at a time by the writer.
//!
//! Blocks and tree-state checkpoints come from the real Testnet node over
//! JSON-RPC. A background task prefetches a bounded window ahead of the
//! writer so commit pacing does not depend on node round trips.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeCheckpoint, NetworkUpgradeActivations, SubtreeRootRange,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceError, SourceSubtreeRoots, ZebraJsonRpcSource,
};

#[derive(Default)]
struct SourceCache {
    blocks: BTreeMap<u32, SourceBlock>,
    checkpoints: BTreeMap<u32, CommitmentTreeCheckpoint>,
}

/// Paced replay source over the real node.
pub(crate) struct PacedReplaySource {
    inner: ZebraJsonRpcSource,
    activations: Arc<NetworkUpgradeActivations>,
    revealed: AtomicU32,
    end_height: u32,
    writer_tip: AtomicU32,
    cache: Mutex<SourceCache>,
    node_fetches: AtomicU64,
    cache_hits: AtomicU64,
    stopped: AtomicBool,
}

impl PacedReplaySource {
    pub(crate) fn new(
        inner: ZebraJsonRpcSource,
        activations: Arc<NetworkUpgradeActivations>,
        store_tip: u32,
        end_height: u32,
    ) -> Self {
        Self {
            inner,
            activations,
            revealed: AtomicU32::new(store_tip),
            end_height,
            writer_tip: AtomicU32::new(store_tip),
            cache: Mutex::new(SourceCache::default()),
            node_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    pub(crate) fn end_height(&self) -> u32 {
        self.end_height
    }

    pub(crate) fn revealed_height(&self) -> u32 {
        self.revealed.load(Ordering::SeqCst)
    }

    /// Reveals the next source height, or returns `None` at the replay end.
    pub(crate) fn reveal_next(&self) -> Option<u32> {
        let current = self.revealed.load(Ordering::SeqCst);
        if current >= self.end_height {
            return None;
        }
        self.revealed.store(current + 1, Ordering::SeqCst);
        Some(current + 1)
    }

    pub(crate) fn set_writer_tip(&self, height: u32) {
        self.writer_tip.store(height, Ordering::SeqCst);
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub(crate) fn counters(&self) -> (u64, u64) {
        (
            self.node_fetches.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn cached_blocks(&self) -> usize {
        self.cache.lock().blocks.len()
    }

    async fn cached_block(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if let Some(block) = self.cache.lock().blocks.get(&height.value()).cloned() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(block);
        }
        self.node_fetches.fetch_add(1, Ordering::Relaxed);
        let block = self.inner.fetch_block_at(height).await?;
        self.cache
            .lock()
            .blocks
            .insert(height.value(), block.clone());
        Ok(block)
    }

    async fn cached_checkpoint(
        &self,
        height: BlockHeight,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        if let Some(checkpoint) = self.cache.lock().checkpoints.get(&height.value()).cloned() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(checkpoint);
        }
        self.node_fetches.fetch_add(1, Ordering::Relaxed);
        let checkpoint = self
            .inner
            .fetch_chain_checkpoint(height, self.activations.as_ref())
            .await?;
        self.cache
            .lock()
            .checkpoints
            .insert(height.value(), checkpoint.clone());
        Ok(checkpoint)
    }

    /// Keeps `window` blocks and checkpoints prefetched above the writer tip.
    pub(crate) async fn run_prefetcher(
        self: Arc<Self>,
        window: u32,
        concurrency: usize,
        retain_below_tip: u32,
    ) {
        let mut next = self.writer_tip.load(Ordering::SeqCst) + 1;
        while !self.stopped.load(Ordering::SeqCst) {
            let tip = self.writer_tip.load(Ordering::SeqCst);
            let want_through = self.end_height.min(tip.saturating_add(window));
            next = next.max(tip.saturating_add(1));
            if next > want_through {
                self.evict(tip, retain_below_tip);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let batch_end = want_through.min(next + 255);
            let heights = (next..=batch_end).collect::<Vec<_>>();
            let source = Arc::clone(&self);
            let outcomes = stream::iter(heights)
                .map(|height| {
                    let source = Arc::clone(&source);
                    async move {
                        let height = BlockHeight::new(height);
                        let block = source.cached_block(height).await;
                        let checkpoint = source.cached_checkpoint(height).await;
                        (height, block.err().or(checkpoint.err()))
                    }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            for (height, error) in outcomes {
                if let Some(error) = error {
                    tracing::warn!(
                        target: "zinder::bench::spike",
                        height = height.value(),
                        %error,
                        "prefetch failed; the writer will fetch on demand"
                    );
                }
            }
            next = batch_end + 1;
            self.evict(tip, retain_below_tip);
        }
    }

    fn evict(&self, tip: u32, retain_below_tip: u32) {
        let floor = tip.saturating_sub(retain_below_tip);
        let mut cache = self.cache.lock();
        cache.blocks = cache.blocks.split_off(&floor);
        cache.checkpoints = cache.checkpoints.split_off(&floor);
    }
}

#[async_trait]
impl NodeSource for PacedReplaySource {
    fn capabilities(&self) -> NodeCapabilities {
        self.inner.capabilities()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height.value() > self.revealed.load(Ordering::SeqCst) {
            return Err(SourceError::NodeUnavailable {
                reason: format!("spike: height {} is not revealed yet", height.value()),
            });
        }
        self.cached_block(height).await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let height = BlockHeight::new(self.revealed.load(Ordering::SeqCst));
        let block = self.cached_block(height).await?;
        Ok(BlockId::new(height, block.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.cached_checkpoint(height).await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.node_fetches.fetch_add(1, Ordering::Relaxed);
        self.inner.fetch_subtree_root_range(range).await
    }
}
