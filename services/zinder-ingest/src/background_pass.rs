//! What the background passes over the canonical store share: opening their
//! own canonical secondary once a store is published, and recording their
//! progress in a file beside the store.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use zinder_core::NetworkUpgradeActivations;
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};

use crate::runtime_config::{
    HistoricalWorkGate, sleep_or_cancel, wait_until_historical_work_or_cancelled,
};

/// How often a pass reopens its canonical secondary when no store is
/// published yet.
const OPEN_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// The canonical secondary one background pass reads through.
pub(crate) struct BackgroundSecondary<'pass> {
    pub(crate) canonical_primary_path: &'pass Path,
    pub(crate) secondary_path: &'pass Path,
    pub(crate) network_upgrade_activations: &'pass Arc<NetworkUpgradeActivations>,
    pub(crate) workload: CanonicalStoreWorkload,
    pub(crate) raw_blob_retention: RawBlobRetention,
    pub(crate) reorg_policy: CanonicalReorgPolicy,
    pub(crate) resource_budget: RocksDbResourceBudget,
}

impl BackgroundSecondary<'_> {
    /// Opens the secondary once a READY store is published, retrying while
    /// none is, or returns `None` once `cancel` fires.
    ///
    /// Opening catches the secondary up and admits the store, which reads
    /// retained history, so each attempt runs off the async workers live
    /// ingest uses and, when `gate` is set, waits for it first. The first
    /// failed attempt logs `unavailable_event`.
    pub(crate) async fn open_when_published(
        &self,
        gate: Option<&HistoricalWorkGate>,
        cancel: &CancellationToken,
        unavailable_event: &'static str,
    ) -> Option<RocksDbCanonicalSecondary> {
        let mut waiting_logged = false;
        loop {
            if let Some(gate) = gate
                && wait_until_historical_work_or_cancelled(gate, cancel).await
            {
                return None;
            }
            if cancel.is_cancelled() {
                return None;
            }
            match self.open().await {
                Ok(secondary) => return Some(secondary),
                Err(error) if !waiting_logged => {
                    waiting_logged = true;
                    tracing::info!(
                        target: "zinder::ingest",
                        event = unavailable_event,
                        error = %error,
                        retry_interval_seconds = OPEN_RETRY_INTERVAL.as_secs(),
                        "waiting for a published canonical store before starting a background pass"
                    );
                }
                Err(_) => {}
            }
            if sleep_or_cancel(OPEN_RETRY_INTERVAL, cancel).await {
                return None;
            }
        }
    }

    async fn open(&self) -> Result<RocksDbCanonicalSecondary, String> {
        let primary_path = self.canonical_primary_path.to_path_buf();
        let secondary_path = self.secondary_path.to_path_buf();
        let activations = Arc::clone(self.network_upgrade_activations);
        let (workload, raw_blob_retention, reorg_policy, resource_budget) = (
            self.workload,
            self.raw_blob_retention,
            self.reorg_policy,
            self.resource_budget,
        );
        tokio::task::spawn_blocking(move || {
            RocksDbCanonicalSecondary::open_ready(
                primary_path,
                secondary_path,
                &activations,
                workload,
                raw_blob_retention,
                reorg_policy,
                resource_budget,
            )
            .map_err(|error| error.to_string())
        })
        .await
        .unwrap_or_else(|error| Err(error.to_string()))
    }
}

/// Replaces the progress file at `path` atomically with `record`.
///
/// The record is synced before it replaces the file, and the directory after,
/// so a crash leaves either the previous record or this one.
pub(crate) fn write_progress_record(
    path: &Path,
    record: &impl serde::Serialize,
) -> std::io::Result<()> {
    use std::io::Write as _;
    let encoded = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    let staged: PathBuf = path.with_extension("json.tmp");
    let mut file = std::fs::File::create(&staged)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    std::fs::rename(&staged, path)?;
    if let Some(directory) = path.parent() {
        std::fs::File::open(directory)?.sync_all()?;
    }
    Ok(())
}
