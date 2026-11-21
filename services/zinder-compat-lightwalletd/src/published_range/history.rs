use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwapOption;
use tonic::Status;
use zinder_core::BlockHeightRange;
use zinder_query::CanonicalReader;
use zinder_store::HeightOrder;

use super::PublishedShieldedCatalog;
use crate::sealed_history::SealedBlock;

/// The admitted shielded history shared by a listener and its catalog refresher.
pub struct PublishedShieldedHistory {
    current: ArcSwapOption<PublishedShieldedCatalog>,
    manifest: PathBuf,
    publisher_directory: Option<PathBuf>,
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
        Self::admit(manifest, None, publisher_key, require_fs_verity, source).await
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
        Self::admit(
            &directory.join("current.json"),
            Some(directory),
            publisher_key,
            require_fs_verity,
            source,
        )
        .await
    }

    async fn admit(
        manifest: &Path,
        publisher_directory: Option<&Path>,
        publisher_key: &Path,
        require_fs_verity: bool,
        source: &dyn CanonicalReader,
    ) -> Result<Self, Status> {
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
            catalog.admit_source(source).await?;
        }
        Ok(Self {
            current: ArcSwapOption::from(catalog.map(Arc::new)),
            manifest: manifest.to_owned(),
            publisher_directory: publisher_directory.map(Path::to_owned),
            key,
            require_fs_verity,
        })
    }

    /// Refreshes the default shielded live tail against a captured canonical view.
    /// An incomplete or displaced refresh keeps the previous fence available.
    pub async fn refresh_tail(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        if let Some(catalog) = self.catalog() {
            catalog.refresh_tail(source).await?;
        }
        Ok(())
    }

    pub(crate) fn has_catalog(&self) -> bool {
        self.current.load().is_some()
    }

    pub(crate) fn catalog(&self) -> Option<Arc<PublishedShieldedCatalog>> {
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
        if let Some(active) = &current
            && candidate.segments().len() == active.segments().len()
        {
            active.refresh_tail(source).await?;
            return Ok(false);
        }
        candidate.admit_source(source).await?;
        let displaced = self
            .current
            .compare_and_swap(&current, Some(Arc::new(candidate)));
        Ok(current.as_ref().map(Arc::as_ptr) == displaced.as_ref().map(Arc::as_ptr))
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
    ) -> io::Result<Vec<SealedBlock>> {
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
            return part.segment.bounded_rows(range, order, max_bytes, true);
        }
        let mut output = Vec::new();
        let mut remaining = max_bytes;
        let mut parts = selected.iter();
        while let Some(part) = match order {
            HeightOrder::Ascending => parts.next(),
            HeightOrder::Descending => parts.next_back(),
        } {
            let identity = part.segment.identity();
            let span = BlockHeightRange::inclusive(
                range.start.max(identity.start),
                range.end.min(identity.end.height),
            );
            let allow_oversized = output.is_empty();
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
