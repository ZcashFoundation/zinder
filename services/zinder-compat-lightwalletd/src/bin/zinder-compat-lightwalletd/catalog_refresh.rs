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

use super::config::{LightwalletdConfigError, ShieldedHistorySection};

pub(super) enum CatalogSource {
    Wallet(WalletServingPairSlot),
    Compact(CanonicalReaderSlot),
}

impl CatalogSource {
    async fn open(
        &self,
        config: &ShieldedHistorySection,
    ) -> Result<PublishedShieldedHistory, tonic::Status> {
        let open = |source| {
            PublishedShieldedHistory::open(
                &config.manifest,
                &config.publisher_key,
                config.require_fs_verity,
                source,
            )
        };
        match self {
            Self::Wallet(slot) => open(slot.capture().canonical()).await,
            Self::Compact(slot) => open(slot.capture().as_ref()).await,
        }
    }

    async fn refresh(&self, history: &PublishedShieldedHistory) -> Result<bool, tonic::Status> {
        match self {
            Self::Wallet(slot) => history.refresh(slot.capture().canonical()).await,
            Self::Compact(slot) => history.refresh(slot.capture().as_ref()).await,
        }
    }
}

#[derive(Eq, PartialEq)]
struct FileStamp {
    length: u64,
    modified: SystemTime,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

impl FileStamp {
    async fn read(path: &Path) -> io::Result<Self> {
        let path = path.to_owned();
        let metadata = tokio::task::spawn_blocking(move || std::fs::metadata(path))
            .await
            .map_err(io::Error::other)??;
        Ok(Self {
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
    paths: ShieldedHistorySection,
    applied: Option<FileStamp>,
}

impl CatalogRefresh {
    pub(super) async fn admit(
        paths: &ShieldedHistorySection,
        source: CatalogSource,
    ) -> Result<Self, LightwalletdConfigError> {
        let before = FileStamp::read(&paths.manifest)
            .await
            .map_err(admission_error)?;
        let history = source.open(paths).await.map_err(admission_error)?;
        let after = FileStamp::read(&paths.manifest)
            .await
            .map_err(admission_error)?;
        Ok(Self {
            history: Arc::new(history),
            paths: paths.clone(),
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
        let stamp = FileStamp::read(&self.paths.manifest)
            .await
            .map_err(|error| error.to_string())?;
        if self.applied.as_ref() == Some(&stamp) {
            return Ok(());
        }
        let changed = self
            .source
            .refresh(&self.history)
            .await
            .map_err(|status| status.message().to_owned())?;
        if changed {
            tracing::info!(target: "zinder::compat_lightwalletd", event = "shielded_catalog_installed", "serving the new signed shielded history");
        }
        self.applied = FileStamp::read(&self.paths.manifest)
            .await
            .ok()
            .filter(|after| *after == stamp);
        Ok(())
    }
}

fn admission_error(error: impl std::fmt::Display) -> ConfigError {
    ConfigError::invalid(format!("shielded range admission failed: {error}"))
}
