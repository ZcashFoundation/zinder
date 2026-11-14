//! Block source: fixture blocks derived exactly as ingest following does.

use std::{sync::Arc, time::{Duration, Instant}};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zinder_bench::{BenchError, fixture::FixtureNodeSource};
use zinder_core::{BlockHeight, CommitmentTreeCheckpoint, NetworkUpgradeActivations};
use zinder_ingest::{spike_derive_live_block, spike_fetch_live_subtree_roots, spike_prepared_replacement_bytes};
use zinder_source::NodeSource;
use zinder_store::{CanonicalBuildBlock, CanonicalBuildSubtreeRoot, RawBlobRetention};

use super::spike_error;

const SOURCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One block ready for a canonical live append or replacement.
pub(crate) struct DerivedBlock {
    pub(crate) block: CanonicalBuildBlock,
    pub(crate) subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    pub(crate) fetch_seconds: f64,
    pub(crate) derive_seconds: f64,
    pub(crate) source_bytes: u64,
    pub(crate) prepared_bytes: u64,
    pub(crate) transaction_count: u64,
    pub(crate) transparent_input_count: u64,
    pub(crate) transparent_output_count: u64,
}

/// Fetches and derives one block on top of `predecessor`.
pub(crate) async fn derive_block(
    source: &FixtureNodeSource,
    height: BlockHeight,
    predecessor: &CommitmentTreeCheckpoint,
    activations: &NetworkUpgradeActivations,
    retention: RawBlobRetention,
    cancel: &CancellationToken,
) -> Result<(DerivedBlock, CommitmentTreeCheckpoint), BenchError> {
    let fetch_started = Instant::now();
    let source_block = source.fetch_block_at(height).await?;
    let fetch_seconds = fetch_started.elapsed().as_secs_f64();
    let derive_started = Instant::now();
    let (block, tip_metadata, checkpoint) =
        spike_derive_live_block(&source_block, predecessor, activations, retention)
            .map_err(|source| spike_error("derive live block", source))?;
    let derive_seconds = derive_started.elapsed().as_secs_f64();
    let subtree_roots = spike_fetch_live_subtree_roots(
        source,
        SOURCE_REQUEST_TIMEOUT,
        cancel,
        predecessor.tip_metadata(),
        tip_metadata,
    )
    .await
    .map_err(|source| spike_error("fetch live subtree roots", source))?;
    let transaction_count = block.facts.transactions.len() as u64;
    let transparent_input_count = block
        .facts
        .transactions
        .iter()
        .map(|transaction| transaction.transparent_inputs.len() as u64)
        .sum();
    let transparent_output_count = block
        .facts
        .transactions
        .iter()
        .map(|transaction| transaction.transparent_outputs.len() as u64)
        .sum();
    let prepared_bytes = spike_prepared_replacement_bytes(&block);
    Ok((
        DerivedBlock {
            block,
            subtree_roots,
            fetch_seconds,
            derive_seconds,
            source_bytes: source_block.raw_block_bytes.len() as u64,
            prepared_bytes,
            transaction_count,
            transparent_input_count,
            transparent_output_count,
        },
        checkpoint,
    ))
}

/// Derives `from..=through` ahead of the committer through a bounded channel.
pub(crate) fn spawn_pipeline(
    runtime: &tokio::runtime::Handle,
    source: FixtureNodeSource,
    start: CommitmentTreeCheckpoint,
    from: BlockHeight,
    through: BlockHeight,
    activations: Arc<NetworkUpgradeActivations>,
    retention: RawBlobRetention,
    depth: usize,
) -> mpsc::Receiver<Result<DerivedBlock, BenchError>> {
    let (sender, receiver) = mpsc::channel(depth.max(1));
    runtime.spawn(async move {
        let cancel = CancellationToken::new();
        let mut predecessor = start;
        let mut height = from;
        while height <= through {
            match derive_block(
                &source,
                height,
                &predecessor,
                &activations,
                retention,
                &cancel,
            )
            .await
            {
                Ok((derived, checkpoint)) => {
                    predecessor = checkpoint;
                    if sender.send(Ok(derived)).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            }
            let Some(next) = height.next() else {
                return;
            };
            height = next;
        }
    });
    receiver
}
