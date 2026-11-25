use std::{
    fmt, fs, io,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Args;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::ShieldedCatalogPublisher;
use zinder_core::BlockHeight;
use zinder_query::{RocksDbCanonicalReader, StorageReadError};
use zinder_store::{
    CanonicalStoreError, CanonicalStoreRequirements, CanonicalStoredSet, RocksDbCanonicalSecondary,
};

use super::config::{HistoryPublisherConfig, LightwalletdConfigError};

mod scratch;
use scratch::{PublicationScratch, path_io_error};

#[derive(Args)]
pub(super) struct PublishHistory {
    /// Directory for the signed current.json, segment files, and publisher state.
    #[arg(long)]
    directory: PathBuf,
    /// File containing the publisher's DER-encoded Ed25519 PKCS#8 private key.
    #[arg(long)]
    signing_key: PathBuf,
    /// First covered height. Defaults to the first retained height.
    #[arg(long)]
    first_height: Option<u32>,
    /// Number of settled heights per segment.
    #[arg(long, default_value = "4096")]
    segment_blocks: NonZeroU32,
    /// Keep publishing and check for newly settled blocks at this interval.
    #[arg(long, value_name = "SECONDS")]
    follow_interval_secs: Option<NonZeroU32>,
}

impl PublishHistory {
    pub(super) async fn publish(
        &self,
        config: &HistoryPublisherConfig,
    ) -> Result<(), LightwalletdConfigError> {
        validate_signing_key_readable(&self.signing_key)?;
        validate_catalog_destination(&self.directory)?;
        let stopping = CancellationToken::new();
        let _signal = self
            .follow_interval_secs
            .map(|_| zinder_runtime::cancel_on_terminating_signal(stopping.clone()));
        let scratch = PublicationScratch::reserve(
            &config.secondary_path,
            &self.directory,
            &config.primary_path,
        )?;
        let mut generation: Option<PublicationReader> = None;
        let mut admitted_stored_set = None;
        let mut publisher = None;
        loop {
            let candidate = generation.take().map_or_else(
                || PublicationReader::open(config, &scratch, admitted_stored_set),
                PublicationReader::catch_up,
            );
            let publication = candidate.and_then(|current| {
                admitted_stored_set.get_or_insert_with(|| current.reader.secondary().stored_set());
                let publisher = match &mut publisher {
                    Some(resumed) => resumed,
                    None => publisher.insert(self.open_publisher(&current.reader)?),
                };
                let mut segments = 0_u32;
                while !stopping.is_cancelled() && publisher.publish_next(&current.reader)? {
                    segments += 1;
                }
                if segments > 0 || self.follow_interval_secs.is_none() {
                    tracing::info!(segments, "settled shielded history publication complete");
                }
                Ok(current)
            });
            match publication {
                Ok(current) => generation = Some(current),
                Err(error) if self.follow_interval_secs.is_some() && reader_invalidated(&error) => {
                    tracing::warn!(%error, "reopening the history publisher's compacted canonical secondary");
                }
                Err(error) => return Err(error.into()),
            }
            let Some(interval) = self.follow_interval_secs else {
                return Ok(());
            };
            tokio::select! {
                biased;
                () = stopping.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_secs(u64::from(interval.get()))) => {}
            }
        }
    }

    fn open_publisher(
        &self,
        reader: &RocksDbCanonicalReader,
    ) -> io::Result<ShieldedCatalogPublisher> {
        let first = self.first_height.map_or_else(
            || reader.secondary().history_bounds().first_available_height(),
            BlockHeight::new,
        );
        validate_signing_key_readable(&self.signing_key)?;
        ShieldedCatalogPublisher::open(
            &self.directory,
            &self.signing_key,
            first,
            self.segment_blocks,
        )
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "could not open shielded history directory {} with signing key {}: {error}",
                    self.directory.display(),
                    self.signing_key.display()
                ),
            )
        })
    }
}

fn validate_signing_key_readable(signing_key: &Path) -> io::Result<()> {
    fs::read(signing_key).map_err(|error| {
        path_io_error("could not read publisher signing key", signing_key, &error)
    })?;
    Ok(())
}

fn validate_catalog_destination(directory: &Path) -> io::Result<()> {
    match directory.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::other(format!(
            "shielded history directory {} is not a directory",
            directory.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(path_io_error(
            "could not inspect shielded history directory",
            directory,
            &error,
        )),
    }
}

#[derive(Debug)]
struct SecondaryOpenError {
    primary_path: PathBuf,
    secondary_path: PathBuf,
    source: CanonicalStoreError,
}

impl fmt::Display for SecondaryOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "could not open canonical secondary {} against canonical primary {}: {}",
            self.secondary_path.display(),
            self.primary_path.display(),
            self.source
        )
    }
}

impl std::error::Error for SecondaryOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct PublicationReader {
    reader: RocksDbCanonicalReader,
    scratch: TempDir,
}

impl PublicationReader {
    fn open(
        config: &HistoryPublisherConfig,
        reservation: &PublicationScratch,
        admitted_stored_set: Option<CanonicalStoredSet>,
    ) -> io::Result<Self> {
        let scratch = reservation.generation()?;
        let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &config.primary_path,
            scratch.path(),
            config.network,
            CanonicalStoreRequirements::default(),
            config.canonical_reorg_policy,
            config.canonical_rocksdb_budget,
        )
        .map_err(|source| {
            io::Error::other(SecondaryOpenError {
                primary_path: config.primary_path.clone(),
                secondary_path: scratch.path().to_path_buf(),
                source,
            })
        })?;
        if let Some(expected) = admitted_stored_set {
            secondary
                .require_stored_set(expected)
                .map_err(io::Error::other)?;
        }
        Ok(Self {
            reader: RocksDbCanonicalReader::new(secondary).map_err(io::Error::other)?,
            scratch,
        })
    }

    fn catch_up(self) -> io::Result<Self> {
        let mut secondary = self
            .reader
            .into_secondary()
            .map_err(|_| io::Error::other("publisher canonical reader is still shared"))?;
        secondary.try_catch_up().map_err(io::Error::other)?;
        Ok(Self {
            reader: RocksDbCanonicalReader::new(secondary).map_err(io::Error::other)?,
            scratch: self.scratch,
        })
    }
}

fn reader_invalidated(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|source| {
        source
            .downcast_ref::<CanonicalStoreError>()
            .is_some_and(CanonicalStoreError::is_missing_table_file)
            || source
                .downcast_ref::<SecondaryOpenError>()
                .is_some_and(|open| open.source.is_missing_table_file())
            || matches!(
                source.downcast_ref::<StorageReadError>(),
                Some(StorageReadError::ReaderInvalidated { .. })
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_catalog_file_or_corrupt_canonical_store_does_not_replace_the_secondary() {
        let absent = io::Error::from(io::ErrorKind::NotFound);
        assert!(!reader_invalidated(&absent));
        let corruption = io::Error::other(StorageReadError::Unavailable {
            source: "corrupt table checksum".into(),
        });
        assert!(!reader_invalidated(&corruption));
        let retired = io::Error::other(StorageReadError::ReaderInvalidated {
            source: "retired canonical table".into(),
        });
        assert!(reader_invalidated(&retired));
    }

    #[test]
    fn wrapped_secondary_open_error_preserves_missing_table_classification() {
        let wrapped = io::Error::other(SecondaryOpenError {
            primary_path: PathBuf::from("/canonical"),
            secondary_path: PathBuf::from("/secondary"),
            source: CanonicalStoreError::PathUnavailable {
                path: PathBuf::from("/canonical"),
                source: io::Error::from(io::ErrorKind::NotFound),
            },
        });
        assert!(!reader_invalidated(&wrapped));
        let message = wrapped.to_string();
        assert!(message.contains("/secondary"), "{message}");
        assert!(message.contains("/canonical"), "{message}");
    }

    #[test]
    fn missing_key_and_blocked_parents_report_exact_paths_and_errors() -> io::Result<()> {
        let files = tempfile::tempdir()?;
        let missing_key = files.path().join("missing/nested/publisher.key");
        let key_error = validate_signing_key_readable(&missing_key)
            .err()
            .ok_or_else(|| io::Error::other("missing key should fail"))?;
        assert_eq!(key_error.kind(), io::ErrorKind::NotFound);
        assert!(
            key_error
                .to_string()
                .contains(&missing_key.display().to_string()),
            "{key_error}"
        );

        let blocker = files.path().join("blocker");
        fs::write(&blocker, b"not-a-directory")?;
        let blocked_catalog = blocker.join("nested/catalog");
        let missing_primary = files.path().join("missing/canonical");
        let secondary_root = files.path().join("nested/secondary");

        let catalog_error =
            PublicationScratch::reserve(&secondary_root, &blocked_catalog, &missing_primary)
                .err()
                .ok_or_else(|| io::Error::other("blocked catalog parent should fail"))?;
        assert!(
            catalog_error
                .to_string()
                .contains(&blocker.display().to_string()),
            "{catalog_error}"
        );

        let valid_catalog = files.path().join("nested/catalog");
        let primary_error =
            PublicationScratch::reserve(&secondary_root, &valid_catalog, &missing_primary)
                .err()
                .ok_or_else(|| io::Error::other("missing primary should fail"))?;
        assert_eq!(primary_error.kind(), io::ErrorKind::NotFound);
        assert!(
            primary_error
                .to_string()
                .contains(&missing_primary.display().to_string()),
            "{primary_error}"
        );

        let primary_dir = files.path().join("primary");
        fs::create_dir(&primary_dir)?;
        let blocked_secondary = blocker.join("nested/secondary");
        let secondary_error =
            PublicationScratch::reserve(&blocked_secondary, &valid_catalog, &primary_dir)
                .err()
                .ok_or_else(|| io::Error::other("blocked secondary root should fail"))?;
        assert!(
            secondary_error
                .to_string()
                .contains(&blocked_secondary.display().to_string()),
            "{secondary_error}"
        );

        PublicationScratch::reserve(&secondary_root, &valid_catalog, &primary_dir)?;
        assert!(secondary_root.is_dir());
        Ok(())
    }
}
