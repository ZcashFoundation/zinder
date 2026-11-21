//! Admission and refresh of the signed history used by the wallet listener.

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::{
    io,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::{CanonicalReaderSlot, PublishedShieldedHistory};
use zinder_query::WalletServingPairSlot;
use zinder_runtime::{ConfigError, SupervisedTasks};

use super::config::{HistoryLocation, LightwalletdConfigError, ShieldedHistorySection};

pub(super) enum CatalogSource {
    Wallet(WalletServingPairSlot),
    Compact(CanonicalReaderSlot),
}

impl CatalogSource {
    async fn open(
        &self,
        config: &ShieldedHistorySection,
    ) -> Result<PublishedShieldedHistory, tonic::Status> {
        match self {
            Self::Wallet(slot) => config.open(slot.capture().canonical()).await,
            Self::Compact(slot) => config.open(slot.capture().as_ref()).await,
        }
    }

    async fn refresh_tail(&self, history: &PublishedShieldedHistory) -> Result<(), tonic::Status> {
        match self {
            Self::Wallet(slot) => history.refresh_tail(slot.capture().canonical()).await,
            Self::Compact(slot) => history.refresh_tail(slot.capture().as_ref()).await,
        }
    }

    async fn refresh(&self, history: &PublishedShieldedHistory) -> Result<bool, tonic::Status> {
        match self {
            Self::Wallet(slot) => history.refresh(slot.capture().canonical()).await,
            Self::Compact(slot) => history.refresh(slot.capture().as_ref()).await,
        }
    }
}

impl ShieldedHistorySection {
    async fn open(
        &self,
        source: &dyn zinder_query::CanonicalReader,
    ) -> Result<PublishedShieldedHistory, tonic::Status> {
        match &self.location {
            HistoryLocation::Manifest(path) => {
                PublishedShieldedHistory::open(
                    path,
                    &self.publisher_key,
                    self.require_fs_verity,
                    source,
                )
                .await
            }
            HistoryLocation::Directory(path) => {
                PublishedShieldedHistory::follow_publisher(
                    path,
                    &self.publisher_key,
                    self.require_fs_verity,
                    source,
                )
                .await
            }
        }
    }
}

#[derive(Eq, PartialEq)]
enum ManifestStamp {
    Unpublished,
    Present {
        length: u64,
        modified: SystemTime,
        #[cfg(unix)]
        identity: (u64, u64, i64, i64),
    },
}

impl ManifestStamp {
    async fn read(path: &Path) -> io::Result<Self> {
        let path = path.to_owned();
        let metadata = match tokio::task::spawn_blocking(move || std::fs::metadata(path))
            .await
            .map_err(io::Error::other)?
        {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::Unpublished),
            Err(error) => return Err(error),
        };
        Ok(Self::Present {
            length: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            identity: (
                metadata.dev(),
                metadata.ino(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            ),
        })
    }
}

pub(super) struct CatalogRefresh {
    pub(super) history: Arc<PublishedShieldedHistory>,
    source: CatalogSource,
    manifest: std::path::PathBuf,
    applied: Option<ManifestStamp>,
}

impl CatalogRefresh {
    pub(super) async fn admit(
        paths: &ShieldedHistorySection,
        source: CatalogSource,
    ) -> Result<Self, LightwalletdConfigError> {
        let manifest = paths.location.manifest();
        let before = ManifestStamp::read(&manifest)
            .await
            .map_err(admission_error)?;
        let history = source.open(paths).await.map_err(admission_error)?;
        let after = ManifestStamp::read(&manifest)
            .await
            .map_err(admission_error)?;
        Ok(Self {
            history: Arc::new(history),
            manifest,
            source,
            applied: (before == after).then_some(after),
        })
    }

    pub(super) fn supervise(
        refresh: Option<Self>,
        interval: Duration,
        stopping: CancellationToken,
        tasks: SupervisedTasks,
    ) -> SupervisedTasks {
        match refresh {
            Some(watcher) => tasks.with(
                "shielded history catalog refresh",
                tokio::spawn(watcher.watch(interval, stopping)),
            ),
            None => tasks,
        }
    }

    async fn watch(mut self, interval: Duration, stopping: CancellationToken) {
        let mut reported_error = None;
        loop {
            tokio::select! {
                biased;
                () = stopping.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            let admission = tokio::select! {
                biased;
                () = stopping.cancelled() => return,
                admission = self.refresh() => admission,
            };
            match admission {
                Ok(()) => reported_error = None,
                Err(reason) if reported_error.as_ref() != Some(&reason) => {
                    tracing::warn!(target: "zinder::compat_lightwalletd", event = "shielded_catalog_refresh_failed", %reason, "retaining the admitted shielded history");
                    reported_error = Some(reason);
                }
                Err(_) => {}
            }
        }
    }

    async fn refresh(&mut self) -> Result<(), String> {
        let stamp = ManifestStamp::read(&self.manifest)
            .await
            .map_err(|error| error.to_string())?;
        if self.applied.as_ref() == Some(&stamp) {
            return self
                .source
                .refresh_tail(&self.history)
                .await
                .map_err(|status| status.message().to_owned());
        }
        let changed = self
            .source
            .refresh(&self.history)
            .await
            .map_err(|status| status.message().to_owned())?;
        if changed {
            tracing::info!(target: "zinder::compat_lightwalletd", event = "shielded_catalog_installed", "serving the new signed shielded history");
        }
        self.applied = ManifestStamp::read(&self.manifest)
            .await
            .ok()
            .filter(|after| *after == stamp);
        Ok(())
    }
}

fn admission_error(error: impl std::fmt::Display) -> ConfigError {
    ConfigError::invalid(format!("shielded range admission failed: {error}"))
}
