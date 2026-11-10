//! Shared builders for the cross-cutting [`ExplorerFreshness`] envelope.
//!
//! Every explorer handler folds the same upstream tip into the
//! `chain_view.upstream_tip` axis of its freshness envelope before
//! responding. The adapter owns one cached [`UpstreamHealthSnapshot`]
//! (refreshed by a background probe) and shares it with every handler through
//! an [`UpstreamObservationCache`] handle.
//!
//! Per ADR-0011 the axis is optional: a response that fires before the first
//! probe completes carries `chain_view.upstream_tip = None`. Consumers treat
//! absence as "unknown", not zero.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, BLOCK_SUMMARY_CONSUMER_NAME, MaterializedViewStore,
};

use super::error::ExplorerError;
use zinder_proto::v1::explorer::{BlockSummaryRecord, ExplorerFreshness};
use zinder_proto::v1::wallet::{self, ChainView, IndexedTip, MaterializedViewStatus, UpstreamTip};
use zinder_source::{NodeSource, UpstreamHealthSnapshot};

/// Shared, lock-protected handle to the most recent
/// [`UpstreamHealthSnapshot`] the adapter has observed.
///
/// Cloned cheaply (it is `Arc<RwLock<_>>` inside) and passed into every
/// handler so the response builder can read the cached snapshot without
/// hitting the upstream node on the request path. Updated only by the
/// background probe task spawned by
/// [`spawn_upstream_observation_probe_task`].
#[derive(Clone, Debug, Default)]
pub(crate) struct UpstreamObservationCache {
    inner: Arc<RwLock<Option<UpstreamHealthSnapshot>>>,
}

impl UpstreamObservationCache {
    /// Returns a fresh empty cache.
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Reads the latest cached snapshot, if the probe has fired at least
    /// once. Cloned out so handlers do not hold the read guard across
    /// freshness construction.
    pub(crate) async fn observe(&self) -> Option<UpstreamHealthSnapshot> {
        self.inner.read().await.clone()
    }

    async fn store(&self, snapshot: UpstreamHealthSnapshot) {
        *self.inner.write().await = Some(snapshot);
    }
}

/// Builds an [`UpstreamTip`] proto from a cached snapshot.
///
/// Carries heights only; the upstream probe has no single block hash.
pub(crate) fn upstream_tip_from_snapshot(snapshot: &UpstreamHealthSnapshot) -> UpstreamTip {
    UpstreamTip {
        committed_height: snapshot.upstream_committed_height,
        estimated_height: snapshot.upstream_estimated_height,
    }
}

/// Folds the cached upstream tip into the `chain_view.upstream_tip` axis of an
/// already-built [`ExplorerFreshness`] body.
///
/// Every handler builds its own freshness (the chain epoch, snapshot age,
/// capability version, and per-field unavailability vary per RPC). The shared
/// upstream tip is overlaid here so no handler reaches into the cache directly.
/// Responses such as `ServerInfo` do not resolve a chain epoch, but still need
/// the upstream tip as the sync-progress denominator during cold starts and
/// source-node catch-up. In that case this function creates a minimal
/// `chain_view` that carries only `upstream_tip`.
pub(crate) async fn attach_upstream_observation(
    cache: &UpstreamObservationCache,
    mut freshness: ExplorerFreshness,
) -> ExplorerFreshness {
    if let Some(snapshot) = cache.observe().await {
        let upstream_tip = upstream_tip_from_snapshot(&snapshot);
        match freshness.chain_view.as_mut() {
            Some(chain_view) => {
                chain_view.upstream_tip = Some(upstream_tip);
            }
            None => {
                freshness.chain_view = Some(ChainView {
                    chain_epoch: None,
                    indexed_tip: None,
                    upstream_tip: Some(upstream_tip),
                    materialized_views: None,
                });
            }
        }
    }
    freshness
}

/// Reads the explorer's indexed tip: the highest block the materialized views
/// have fully materialized, decoded from the newest `BlockSummaryRecord`.
/// Returns `None` when no block is materialized yet.
///
/// All chain-event materialized-view consumers advance under one shared cursor, so the
/// block-summary head is an accurate indexed tip for every capability.
pub(crate) fn read_indexed_tip(
    materialized_view_store: &MaterializedViewStore,
) -> Result<Option<IndexedTip>, Status> {
    if !materialized_view_store.has_consumer(BLOCK_SUMMARY_CONSUMER_NAME) {
        return Ok(None);
    }
    let Some((_, payload)) = materialized_view_store
        .last_consumer_entry(BLOCK_SUMMARY_COLUMN_FAMILY)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    decode_indexed_tip(&payload).map(Some)
}

fn decode_indexed_tip(payload: &[u8]) -> Result<IndexedTip, Status> {
    let summary = BlockSummaryRecord::decode(payload)
        .map_err(|error| {
            ExplorerError::internal(format!("BlockSummaryRecord decode failed: {error}"))
        })?
        .summary
        .ok_or_else(|| ExplorerError::internal("BlockSummaryRecord.summary missing"))?;
    Ok(IndexedTip {
        tip: Some(wallet::BlockTip {
            height: summary.block_height,
            hash: summary.block_hash,
        }),
        block_time_unix_seconds: summary.block_time_unix_seconds,
    })
}

/// Builds the per-response [`ExplorerFreshness`] body shared by every read
/// handler.
///
/// Assembles the cross-plane `chain_view` from the canonical follower tip
/// (`chain_epoch`), the materialized-view plane's indexed tip (the block the response
/// actually reflects), and the persisted materialized-view status. Consumers read index
/// lag as `chain_view.chain_epoch.visible_tip.height -
/// chain_view.indexed_tip.tip.height`. The materialized-view identity (indexed tip
/// and materialized-view status) is carried whenever `materialized_view_store` is wired, so the
/// bootstrap `ServerInfo` call reports how far the materialized views have
/// materialized even though its `chain_epoch` is absent because it makes no
/// snapshot-consistency claim. `chain_view` stays unset only when the response
/// resolves no chain epoch and no materialized-view store is wired. The upstream tip is
/// overlaid separately by [`attach_upstream_observation`].
pub(crate) fn build_explorer_freshness(
    materialized_view_store: Option<&MaterializedViewStore>,
    capability_version: &str,
    chain_epoch: Option<wallet::ChainEpoch>,
    snapshot_age_millis: u64,
) -> Result<ExplorerFreshness, Status> {
    let (indexed_tip, materialized_views) = match materialized_view_store {
        Some(store) => (
            read_indexed_tip(store)?,
            read_materialized_view_status(Some(store))?,
        ),
        None => (None, None),
    };
    Ok(explorer_freshness(
        indexed_tip,
        materialized_views,
        capability_version,
        chain_epoch,
        snapshot_age_millis,
    ))
}

fn explorer_freshness(
    indexed_tip: Option<IndexedTip>,
    materialized_views: Option<MaterializedViewStatus>,
    capability_version: &str,
    chain_epoch: Option<wallet::ChainEpoch>,
    snapshot_age_millis: u64,
) -> ExplorerFreshness {
    let chain_view =
        if chain_epoch.is_some() || indexed_tip.is_some() || materialized_views.is_some() {
            Some(ChainView {
                chain_epoch,
                indexed_tip,
                upstream_tip: None,
                materialized_views,
            })
        } else {
            None
        };
    ExplorerFreshness {
        chain_view,
        snapshot_age_millis,
        capability_version: capability_version.to_owned(),
        unavailable: Vec::new(),
    }
}

/// Returns true when the materialized-view indexed tip is the visible tip of the
/// supplied canonical chain epoch.
pub(crate) fn indexed_tip_matches_chain_epoch(
    freshness: &ExplorerFreshness,
    chain_epoch: &wallet::ChainEpoch,
) -> bool {
    freshness
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.indexed_tip.as_ref())
        .and_then(|indexed_tip| indexed_tip.tip.as_ref())
        .zip(chain_epoch.visible_tip.as_ref())
        .is_some_and(|(indexed_tip, visible_tip)| indexed_tip == visible_tip)
}

/// Reads the persisted materialized-view status.
///
/// Decodes the record into the wire [`MaterializedViewStatus`]. Returns `None`
/// when no materialized-view store is wired or the ingest plane has not written
/// a record yet.
pub(crate) fn read_materialized_view_status(
    materialized_view_store: Option<&MaterializedViewStore>,
) -> Result<Option<MaterializedViewStatus>, Status> {
    let Some(store) = materialized_view_store else {
        return Ok(None);
    };
    let Some(bytes) = store
        .get_materialized_view_status()
        .map_err(|error| ExplorerError::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    decode_materialized_view_status(&bytes).map(Some)
}

fn decode_materialized_view_status(bytes: &[u8]) -> Result<MaterializedViewStatus, Status> {
    MaterializedViewStatus::decode(bytes).map_err(|error| {
        ExplorerError::internal(format!("MaterializedViewStatus decode failed: {error}")).into()
    })
}

/// Spawns the background task that refreshes the
/// [`UpstreamObservationCache`] on a fixed cadence.
///
/// The task ticks every `poll_interval`, calls
/// [`NodeSource::poll_upstream_health`] on `source`, and writes the
/// returned [`UpstreamHealthSnapshot`] into `cache`. Errors are logged at
/// warn and never propagate; the cache keeps serving its prior value
/// (or stays empty if the probe never succeeded) so a transient upstream
/// outage does not poison the freshness envelope.
#[must_use = "drop the handle to detach the task or await it for symmetric shutdown"]
pub(crate) fn spawn_upstream_observation_probe_task<Source>(
    source: Arc<Source>,
    cache: UpstreamObservationCache,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Source: NodeSource + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {
                    run_upstream_observation_probe_once(source.as_ref(), &cache).await;
                }
            }
        }
    })
}

async fn run_upstream_observation_probe_once<Source>(
    source: &Source,
    cache: &UpstreamObservationCache,
) where
    Source: NodeSource,
{
    match source.poll_upstream_health().await {
        Ok(snapshot) => cache.store(snapshot).await,
        Err(error) => {
            tracing::warn!(
                target: "zinder::explorer",
                event = "upstream_observation_probe_failed",
                error = %error,
                "upstream observation probe failed; freshness envelope keeps the prior snapshot",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;
    use zinder_source::UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT;

    fn synthetic_freshness(chain_view: Option<ChainView>) -> ExplorerFreshness {
        ExplorerFreshness {
            chain_view,
            snapshot_age_millis: 0,
            capability_version: zinder_proto::capabilities::EXPLORER_OVERVIEW_SNAPSHOT_V1
                .to_owned(),
            unavailable: Vec::new(),
        }
    }

    fn synthetic_chain_view() -> ChainView {
        ChainView {
            chain_epoch: Some(wallet::ChainEpoch::default()),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }
    }

    #[tokio::test]
    async fn attach_leaves_upstream_unset_when_probe_never_fired() {
        let cache = UpstreamObservationCache::empty();
        let freshness =
            attach_upstream_observation(&cache, synthetic_freshness(Some(synthetic_chain_view())))
                .await;
        let upstream_tip = freshness
            .chain_view
            .and_then(|chain_view| chain_view.upstream_tip);
        assert!(upstream_tip.is_none());
    }

    #[tokio::test]
    async fn attach_copies_cached_snapshot_into_freshness() -> Result<(), &'static str> {
        let cache = UpstreamObservationCache::empty();
        cache
            .store(UpstreamHealthSnapshot::ready(
                UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
                Some(2_530_000),
                Some(2_544_375),
                Some(0.9943),
            ))
            .await;
        let freshness =
            attach_upstream_observation(&cache, synthetic_freshness(Some(synthetic_chain_view())))
                .await;
        let Some(upstream) = freshness
            .chain_view
            .and_then(|chain_view| chain_view.upstream_tip)
        else {
            return Err("expected upstream tip");
        };
        assert_eq!(upstream.committed_height, Some(2_530_000));
        assert_eq!(upstream.estimated_height, Some(2_544_375));
        Ok(())
    }
}
