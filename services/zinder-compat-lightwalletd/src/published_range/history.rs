use std::{
    borrow::Borrow,
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

use super::{PublishedShieldedCatalog, ShieldedTailBuffer};
use crate::sealed_history::SealedBlock;

/// The admitted shielded history shared by a listener and its catalog refresher.
pub struct PublishedShieldedHistory {
    current: ArcSwap<HistorySnapshot>,
    manifest: PathBuf,
    publisher_directory: Option<PathBuf>,
    key: [u8; 32],
    require_fs_verity: bool,
}

pub(super) enum HistorySnapshot {
    Bootstrap(ShieldedTailBuffer),
    Published(Arc<PublishedShieldedCatalog>),
}

impl HistorySnapshot {
    fn catalog(&self) -> Option<&Arc<PublishedShieldedCatalog>> {
        match self {
            Self::Bootstrap(_) => None,
            Self::Published(catalog) => Some(catalog),
        }
    }

    pub(super) fn encoded_range(
        &self,
    ) -> (Option<Arc<PublishedShieldedCatalog>>, ShieldedTailBuffer) {
        match self {
            Self::Bootstrap(tail) => (None, tail.clone()),
            Self::Published(catalog) => (Some(catalog.clone()), catalog.tail()),
        }
    }
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
        Self::open_captured(manifest, publisher_key, require_fs_verity, || source).await
    }

    /// Opens signed settled history like [`Self::open`], capturing the
    /// canonical source only once the catalog is read, so a caller that
    /// captures a replaceable serving pair holds it for the verification
    /// alone.
    ///
    /// # Errors
    ///
    /// Returns an error if a signature, segment, or canonical anchor fails verification.
    pub async fn open_captured<Source, Capture>(
        manifest: &Path,
        publisher_key: &Path,
        require_fs_verity: bool,
        capture: Capture,
    ) -> Result<Self, Status>
    where
        Capture: FnOnce() -> Source,
        Source: Borrow<dyn CanonicalReader>,
    {
        Self::admit(manifest, None, publisher_key, require_fs_verity, capture).await
    }

    /// Follows an active publisher and serves canonical ranges until its first segment.
    /// The publisher's signed checkpoint must authenticate the empty or populated catalog.
    ///
    /// # Errors
    ///
    /// Returns an error for missing publisher state, rollback, or catalog admission failure.
    pub async fn follow_publisher(
        directory: &Path,
        publisher_key: &Path,
        require_fs_verity: bool,
        source: &dyn CanonicalReader,
    ) -> Result<Self, Status> {
        Self::follow_publisher_captured(directory, publisher_key, require_fs_verity, || source)
            .await
    }

    /// Follows an active publisher like [`Self::follow_publisher`], capturing
    /// the canonical source only once the catalog is read.
    ///
    /// # Errors
    ///
    /// Returns an error for missing publisher state, rollback, or catalog admission failure.
    pub async fn follow_publisher_captured<Source, Capture>(
        directory: &Path,
        publisher_key: &Path,
        require_fs_verity: bool,
        capture: Capture,
    ) -> Result<Self, Status>
    where
        Capture: FnOnce() -> Source,
        Source: Borrow<dyn CanonicalReader>,
    {
        Self::admit(
            &directory.join("current.json"),
            Some(directory),
            publisher_key,
            require_fs_verity,
            capture,
        )
        .await
    }

    async fn admit<Source, Capture>(
        manifest: &Path,
        publisher_directory: Option<&Path>,
        publisher_key: &Path,
        require_fs_verity: bool,
        capture: Capture,
    ) -> Result<Self, Status>
    where
        Capture: FnOnce() -> Source,
        Source: Borrow<dyn CanonicalReader>,
    {
        let path = manifest.to_owned();
        let directory = publisher_directory.map(Path::to_owned);
        let publisher_key = publisher_key.to_owned();
        let (key, catalog) = tokio::task::spawn_blocking(move || {
            let publisher_key = match &directory {
                Some(directory) => independent_key(directory, &publisher_key)?,
                None => publisher_key,
            };
            let key = super::manifest::verification_key_from(&publisher_key)?;
            let catalog = read_catalog(&path, directory.as_deref(), &key, require_fs_verity, None)?;
            Ok::<_, io::Error>((key, catalog))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let Some(catalog) = &catalog {
            let source = capture();
            catalog.verify_source(source.borrow()).await?;
        }
        Ok(Self {
            current: ArcSwap::from_pointee(catalog.map_or_else(
                || HistorySnapshot::Bootstrap(ShieldedTailBuffer::default()),
                |catalog| HistorySnapshot::Published(Arc::new(catalog)),
            )),
            manifest: manifest.to_owned(),
            publisher_directory: publisher_directory.map(Path::to_owned),
            key,
            require_fs_verity,
        })
    }

    /// Checks the active catalog's terminal against a captured canonical view.
    /// The encoded live tail fills on demand for the view each request reads.
    ///
    /// # Errors
    ///
    /// Returns an error when the view no longer holds the catalog's terminal.
    pub async fn verify_source(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        if let Some(catalog) = self.snapshot().catalog() {
            catalog.verify_source(source).await?;
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Arc<HistorySnapshot> {
        self.current.load_full()
    }

    /// Serves the admitted catalog with its tail, or a standalone live tail
    /// until the first catalog is adopted. Adoption releases the standalone
    /// rows once streams that started before it finish.
    pub(crate) fn encoded_range(
        &self,
    ) -> (Option<Arc<PublishedShieldedCatalog>>, ShieldedTailBuffer) {
        self.current.load().encoded_range()
    }

    /// Admits an extension of the active signed catalog against the current canonical source.
    /// Returns true when this call installs the extension.
    ///
    /// # Errors
    ///
    /// A signature, storage, or chain error leaves the active catalog serving existing requests.
    pub async fn refresh(&self, source: &dyn CanonicalReader) -> Result<bool, Status> {
        self.refresh_captured(|| source).await
    }

    /// Admits an extension like [`Self::refresh`], capturing the canonical
    /// source only once the catalog is read and only when a catalog needs
    /// verifying, so a caller that captures a replaceable serving pair holds
    /// it for the verification alone.
    ///
    /// # Errors
    ///
    /// A signature, storage, or chain error leaves the active catalog serving existing requests.
    pub async fn refresh_captured<Source, Capture>(&self, capture: Capture) -> Result<bool, Status>
    where
        Capture: FnOnce() -> Source,
        Source: Borrow<dyn CanonicalReader>,
    {
        let current = self.snapshot();
        let previous = current.catalog().cloned();
        let manifest = self.manifest.clone();
        let directory = self.publisher_directory.clone();
        let key = self.key;
        let require_fs_verity = self.require_fs_verity;
        let candidate = tokio::task::spawn_blocking(move || {
            read_catalog(
                &manifest,
                directory.as_deref(),
                &key,
                require_fs_verity,
                previous.as_deref(),
            )
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let Some(candidate) = candidate else {
            return Ok(false);
        };
        let source = capture();
        if let Some(active) = current.catalog()
            && candidate.segments().len() == active.segments().len()
        {
            active.verify_source(source.borrow()).await?;
            return Ok(false);
        }
        candidate.verify_source(source.borrow()).await?;
        drop(source);
        let next = Arc::new(HistorySnapshot::Published(Arc::new(candidate)));
        let displaced = self.current.compare_and_swap(&current, next);
        Ok(Arc::ptr_eq(&current, &displaced))
    }
}

fn independent_key(directory: &Path, key: &Path) -> io::Result<PathBuf> {
    let directory = directory.canonicalize()?;
    let parent = key
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let resolved = key.canonicalize()?;
    if resolved.starts_with(&directory) || parent.canonicalize()?.starts_with(&directory) {
        return Err(super::manifest::invalid(
            "the trusted key must be outside the publisher directory",
        ));
    }
    Ok(resolved)
}

fn read_catalog(
    manifest: &Path,
    publisher_directory: Option<&Path>,
    key: &[u8; 32],
    require_fs_verity: bool,
    previous: Option<&PublishedShieldedCatalog>,
) -> io::Result<Option<PublishedShieldedCatalog>> {
    // The publisher installs the manifest before its checkpoint. Read the checkpoint
    // first so a concurrent append cannot make an authentic prefix look rolled back.
    let head = publisher_directory
        .map(|directory| super::catalog::PublisherHead::read(directory, key))
        .transpose()?;
    if previous.is_none()
        && let Some(head) = &head
        && std::fs::symlink_metadata(manifest)
            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
    {
        head.verify(&[])?;
        return Ok(None);
    }
    let catalog = PublishedShieldedCatalog::open_signed(
        manifest,
        key,
        require_fs_verity,
        previous,
        head.as_ref(),
    )?;
    Ok(Some(catalog))
}

impl PublishedShieldedCatalog {
    #[expect(
        clippy::too_many_arguments,
        reason = "the slab, range, order, byte budget, and oversized-row exception describe one bounded catalog read"
    )]
    pub(super) fn read_chunk(
        &self,
        slab: crate::sealed_history::SealedSlab,
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
                .bounded_rows(slab, range, order, max_bytes, allow_oversized);
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
            let mut rows =
                part.segment
                    .bounded_rows(slab, span, order, remaining, allow_oversized)?;
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
