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
            Self::Wallet(slot) => config.open(slot.capture_ephemeral().canonical()).await,
            Self::Compact(slot) => config.open(slot.capture().as_ref()).await,
        }
    }

    async fn verify_source(&self, history: &PublishedShieldedHistory) -> Result<(), tonic::Status> {
        match self {
            Self::Wallet(slot) => {
                history
                    .verify_source(slot.capture_ephemeral().canonical())
                    .await
            }
            Self::Compact(slot) => history.verify_source(slot.capture().as_ref()).await,
        }
    }

    async fn refresh(&self, history: &PublishedShieldedHistory) -> Result<bool, tonic::Status> {
        match self {
            Self::Wallet(slot) => history.refresh(slot.capture_ephemeral().canonical()).await,
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
struct FileStamp {
    length: u64,
    modified: SystemTime,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

impl FileStamp {
    fn read(path: &Path) -> io::Result<Option<Self>> {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(path) {
                    Ok(link) if !link.is_symlink() => link,
                    Ok(_) => return Err(error),
                    Err(link_error) if link_error.kind() == io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(link_error) => return Err(link_error),
                }
            }
            Err(error) => return Err(error),
        };
        Ok(Some(Self {
            length: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            identity: (
                metadata.dev(),
                metadata.ino(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            ),
        }))
    }
}

#[derive(Default, Eq, PartialEq)]
struct CatalogStamp {
    checkpoint: Option<FileStamp>,
    manifest: Option<FileStamp>,
}

impl HistoryLocation {
    async fn stamp(&self) -> io::Result<CatalogStamp> {
        let location = self.clone();
        tokio::task::spawn_blocking(move || {
            let checkpoint = match &location {
                Self::Directory(directory) => {
                    FileStamp::read(&directory.join(".publisher-head.json"))?
                }
                Self::Manifest(_) => None,
            };
            Ok(CatalogStamp {
                checkpoint,
                manifest: FileStamp::read(&location.manifest())?,
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn initial_stamp(&self, timeout: Duration) -> io::Result<CatalogStamp> {
        let stamp = self.stamp().await?;
        if matches!(self, Self::Manifest(_)) || stamp != CatalogStamp::default() {
            return Ok(stamp);
        }
        tracing::info!(target: "zinder::compat_lightwalletd",
            event = "shielded_publisher_starting",
            "waiting for the first signed publisher checkpoint"
        );
        tokio::time::timeout(timeout, async {
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let stamp = self.stamp().await?;
                if stamp != CatalogStamp::default() {
                    return Ok(stamp);
                }
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for the first signed publisher checkpoint",
            )
        })?
    }
}

pub(super) struct CatalogRefresh {
    pub(super) history: Arc<PublishedShieldedHistory>,
    source: CatalogSource,
    location: HistoryLocation,
    applied: Option<CatalogStamp>,
}

impl CatalogRefresh {
    pub(super) async fn admit(
        paths: &ShieldedHistorySection,
        source: CatalogSource,
    ) -> Result<Self, LightwalletdConfigError> {
        let location = &paths.location;
        let before = location
            .initial_stamp(Duration::from_secs(30))
            .await
            .map_err(admission_error)?;
        let history = source.open(paths).await.map_err(admission_error)?;
        let after = location.stamp().await.map_err(admission_error)?;
        Ok(Self {
            history: Arc::new(history),
            location: location.clone(),
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
                    tracing::warn!(target: "zinder::compat_lightwalletd", event = "shielded_catalog_refresh_failed", %reason, "continuing to serve the last verified wallet view");
                    reported_error = Some(reason);
                }
                Err(_) => {}
            }
        }
    }

    async fn refresh(&mut self) -> Result<(), String> {
        let stamp = self
            .location
            .stamp()
            .await
            .map_err(|error| error.to_string())?;
        if self.applied.as_ref() == Some(&stamp) {
            return self
                .source
                .verify_source(&self.history)
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
        self.applied = self
            .location
            .stamp()
            .await
            .ok()
            .filter(|after| *after == stamp);
        Ok(())
    }
}

fn admission_error(error: impl std::fmt::Display) -> ConfigError {
    ConfigError::invalid(format!("shielded range admission failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_a_fresh_managed_directory_waits_for_publisher_initialization() -> eyre::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let files = HistoryLocation::Directory(dir.path().into());
        let timeout = Duration::from_millis(20);
        let error = files
            .initial_stamp(timeout)
            .await
            .err()
            .ok_or_else(|| eyre::eyre!("missing timeout"))?;
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let snapshot = HistoryLocation::Manifest(files.manifest());
        assert!(snapshot.initial_stamp(timeout).await? == CatalogStamp::default());
        std::fs::write(
            files.manifest(),
            b"requires immediate signature verification",
        )?;
        let managed = files.initial_stamp(timeout).await?;
        assert!(managed.manifest.is_some());
        let standalone = snapshot.stamp().await?;
        std::fs::write(
            dir.path().join(".publisher-head.json"),
            b"updated checkpoint",
        )?;
        assert!(files.stamp().await? != managed);
        assert!(snapshot.stamp().await? == standalone);
        Ok(())
    }
}
