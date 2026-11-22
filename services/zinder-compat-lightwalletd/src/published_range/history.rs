use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::BlockHeightRange;
use zinder_query::CanonicalReader;
use zinder_store::HeightOrder;

use super::PublishedShieldedCatalog;
use crate::sealed_history::SealedBlock;

/// The admitted shielded history shared by a listener and its catalog refresher.
pub struct PublishedShieldedHistory {
    current: ArcSwap<PublishedShieldedCatalog>,
    manifest: PathBuf,
    key: [u8; 32],
    require_fs_verity: bool,
}

impl PublishedShieldedHistory {
    /// Opens signed settled history against the listener's canonical reader.
    /// The verification key is a separate, trusted file containing 32 raw Ed25519 bytes.
    /// When required, every segment must have active Linux fs-verity at admission.
    ///
    /// # Errors
    ///
    /// Returns an error if a signature, segment, or canonical anchor fails verification.
    pub async fn open(
        manifest: &Path,
        publisher_key: &Path,
        require_fs_verity: bool,
        source: &dyn CanonicalReader,
    ) -> Result<Self, Status> {
        let path = manifest.to_owned();
        let publisher_key = publisher_key.to_owned();
        let (key, catalog) = tokio::task::spawn_blocking(move || {
            let key = super::manifest::verification_key_from(&publisher_key)?;
            let catalog =
                PublishedShieldedCatalog::open_signed(&path, &key, require_fs_verity, None)?;
            Ok::<_, io::Error>((key, catalog))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        catalog.admit_source(source).await?;
        Ok(Self {
            current: ArcSwap::from_pointee(catalog),
            manifest: manifest.to_owned(),
            key,
            require_fs_verity,
        })
    }

    /// Refreshes the default shielded live tail against a captured canonical view.
    /// An incomplete or displaced refresh keeps the previous fence available.
    pub async fn refresh_tail(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        self.catalog().refresh_tail(source).await
    }

    pub(crate) fn catalog(&self) -> Arc<PublishedShieldedCatalog> {
        self.current.load_full()
    }

    /// Admits an extension of the active signed catalog against the current canonical source.
    /// Returns true when this call installs the extension.
    ///
    /// # Errors
    ///
    /// A signature, storage, or chain error leaves the active catalog serving existing requests.
    pub async fn refresh(&self, source: &dyn CanonicalReader) -> Result<bool, Status> {
        let current = self.catalog();
        let previous = current.clone();
        let manifest = self.manifest.clone();
        let key = self.key;
        let require_fs_verity = self.require_fs_verity;
        let candidate = tokio::task::spawn_blocking(move || {
            PublishedShieldedCatalog::open_signed(
                &manifest,
                &key,
                require_fs_verity,
                Some(&previous),
            )
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if candidate.segments().len() == current.segments().len() {
            current.refresh_tail(source).await?;
            return Ok(false);
        }
        candidate.admit_source(source).await?;
        let displaced = self.current.compare_and_swap(&current, Arc::new(candidate));
        Ok(Arc::ptr_eq(&current, &displaced))
    }
}

impl PublishedShieldedCatalog {
    async fn admit_source(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        self.verify_source(source).await?;
        if let Err(status) = self.refresh_tail(source).await {
            tracing::warn!(%status, "serving the uncovered tail from canonical storage");
        }
        Ok(())
    }

    pub(super) fn read_chunk(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        (allow_oversized, cancel): (bool, &CancellationToken),
    ) -> io::Result<Vec<SealedBlock>> {
        if cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "block-range stream cancelled",
            ));
        }
        let segments = self.segments();
        let start =
            segments.partition_point(|part| part.segment.identity().end.height < range.start);
        let end = segments.partition_point(|part| part.segment.identity().start <= range.end);
        let selected = segments
            .get(start..end)
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "range outside shielded catalog",
                )
            })?;
        if let [part] = selected {
            return part
                .segment
                .bounded_rows(range, order, max_bytes, allow_oversized);
        }
        let mut output = Vec::new();
        let mut remaining = max_bytes;
        let mut parts = selected.iter();
        while let Some(part) = match order {
            HeightOrder::Ascending => parts.next(),
            HeightOrder::Descending => parts.next_back(),
        } {
            if cancel.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "block-range stream cancelled",
                ));
            }
            let identity = part.segment.identity();
            let span = BlockHeightRange::inclusive(
                range.start.max(identity.start),
                range.end.min(identity.end.height),
            );
            let allow_oversized = allow_oversized && output.is_empty();
            let mut rows = part
                .segment
                .bounded_rows(span, order, remaining, allow_oversized)?;
            let whole_span = u64::try_from(rows.len()).ok()
                == Some(u64::from(span.end.value() - span.start.value()) + 1);
            remaining = rows
                .iter()
                .fold(remaining, |room, row| room.saturating_sub(row.native_bytes));
            if order == HeightOrder::Descending {
                rows.reverse();
            }
            output.append(&mut rows);
            if !whole_span || remaining == 0 {
                break;
            }
        }
        if order == HeightOrder::Descending {
            output.reverse();
        }
        Ok(output)
    }
}
