use std::{io, num::NonZeroU32, path::PathBuf, time::Duration};

use clap::Args;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::ShieldedCatalogPublisher;
use zinder_core::BlockHeight;
use zinder_query::{RocksDbCanonicalReader, StorageReadError};
use zinder_store::{CanonicalStoreError, CanonicalStoreWorkload, RocksDbCanonicalSecondary};

use super::config::{LightwalletdConfig, LightwalletdConfigError};

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
        config: &LightwalletdConfig,
    ) -> Result<(), LightwalletdConfigError> {
        let stopping = CancellationToken::new();
        let _signal = self
            .follow_interval_secs
            .map(|_| zinder_runtime::cancel_on_terminating_signal(stopping.clone()));
        std::fs::create_dir_all(&config.storage.secondary_path)?;
        let mut generation: Option<PublicationReader> = None;
        let mut publisher = None;
        loop {
            let candidate = generation.take().map_or_else(
                || PublicationReader::open(config),
                PublicationReader::catch_up,
            );
            let publication = candidate.and_then(|current| {
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
        ShieldedCatalogPublisher::open(
            &self.directory,
            &self.signing_key,
            first,
            self.segment_blocks,
        )
    }
}

struct PublicationReader {
    reader: RocksDbCanonicalReader,
    scratch: TempDir,
}

impl PublicationReader {
    fn open(config: &LightwalletdConfig) -> io::Result<Self> {
        let scratch = tempfile::tempdir_in(&config.storage.secondary_path)?;
        let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &config.storage.path,
            scratch.path(),
            config.network,
            CanonicalStoreWorkload::Wallet,
            config.storage.expected_raw_blob_retention,
            config.canonical_reorg_policy,
            config.storage.canonical_rocksdb_budget,
        )
        .map_err(io::Error::other)?;
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
}
