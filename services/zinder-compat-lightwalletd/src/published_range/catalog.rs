//! Append-only signed coverage for settled shielded blocks.
mod publisher;

pub use publisher::ShieldedCatalogPublisher;

use std::{io, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};
use zinder_core::{BlockHeight, BlockHeightRange};
use zinder_query::CanonicalReader;

use super::{
    PublishedShieldedRange, ShieldedRangeIdentity,
    manifest::{invalid, segment_path, signed_contents},
};

pub(super) const CATALOG_LIMIT: u64 = 32 * 1024 * 1024;
const MAX_SEGMENTS: usize = 32_768;
const FORMAT: &str = "zinder.shielded-range.catalog.v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogManifest<Segments = Vec<ShieldedRangeIdentity>> {
    format: String,
    segments: Segments,
}

fn load_entries(path: &Path, key: &[u8]) -> io::Result<Vec<ShieldedRangeIdentity>> {
    let manifest: CatalogManifest =
        serde_json::from_str(&signed_contents(path, key, CATALOG_LIMIT)?)?;
    if manifest.format != FORMAT {
        return Err(invalid("unsupported signed shielded catalog format"));
    }
    let entries = manifest.segments;
    if entries.is_empty() || entries.len() > MAX_SEGMENTS {
        return Err(invalid("invalid number of shielded catalog segments"));
    }
    for pair in entries.windows(2) {
        let a = pair[0];
        let b = pair[1];
        if a.last.checked_add(1) != Some(b.first)
            || a.network_id != b.network_id
            || a.artifact_schema != b.artifact_schema
        {
            return Err(invalid(
                "shielded catalog segments are not contiguous on one network and schema",
            ));
        }
    }
    Ok(entries)
}

fn open_segment(
    directory: &Path,
    entry: &ShieldedRangeIdentity,
    previous: Option<ShieldedRangeIdentity>,
) -> io::Result<PublishedShieldedRange> {
    let range = PublishedShieldedRange::open(&segment_path(directory, entry.sha256), *entry)?;
    check_parent(previous, &range)?;
    Ok(range)
}

fn check_parent(
    previous: Option<ShieldedRangeIdentity>,
    range: &PublishedShieldedRange,
) -> io::Result<()> {
    if previous.is_some_and(|prior| prior.last_hash != range.segment.first_parent().as_bytes()) {
        return Err(invalid(
            "shielded catalog segment does not link to its predecessor",
        ));
    }
    Ok(())
}

/// Signed, contiguous historical coverage admitted from one publisher.
#[derive(Clone)]
pub(crate) struct PublishedShieldedCatalog {
    segments: Vec<Arc<PublishedShieldedRange>>,
}

impl PublishedShieldedCatalog {
    pub(super) fn open_signed(
        path: &Path,
        key: &[u8; 32],
        require_fs_verity: bool,
        previous: Option<&Self>,
    ) -> io::Result<Self> {
        let entries = load_entries(path, key)?;
        let mut segments = Vec::with_capacity(entries.len());
        if let Some(previous) = previous {
            if previous.segments.len() > entries.len()
                || !previous
                    .segments
                    .iter()
                    .zip(&entries)
                    .all(|(old, new)| old.identity() == *new)
            {
                return Err(invalid(
                    "shielded catalog changed publisher or regressed its admitted prefix",
                ));
            }
            segments.extend(previous.segments.iter().cloned());
        }
        let directory = path.parent().unwrap_or_else(|| Path::new("."));
        for entry in &entries[segments.len()..] {
            let prior = segments.last().map(|range| range.identity());
            let segment = open_segment(directory, entry, prior)?;
            if require_fs_verity && !segment.segment.kernel_verified {
                return Err(invalid(
                    "shielded history requires Linux fs-verity on every segment",
                ));
            }
            segments.push(Arc::new(segment));
        }
        Ok(Self { segments })
    }

    /// All admitted segments in ascending height order.
    #[must_use]
    pub(super) fn segments(&self) -> &[Arc<PublishedShieldedRange>] {
        &self.segments
    }

    pub(super) fn terminal(&self) -> &PublishedShieldedRange {
        &self.segments[self.segments.len() - 1]
    }

    pub(super) fn bounds(&self) -> BlockHeightRange {
        BlockHeightRange::inclusive(
            BlockHeight::new(self.segments[0].identity.first),
            BlockHeight::new(self.terminal().identity.last),
        )
    }

    /// Verifies the linked catalog's terminal against a captured canonical reader.
    ///
    /// # Errors
    ///
    /// Fails when the source is unavailable or the catalog is outside its settled chain.
    pub(super) async fn verify_source(
        &self,
        reader: &dyn CanonicalReader,
    ) -> Result<(), tonic::Status> {
        if reader
            .block_header_at(self.bounds().start)
            .await
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?
            .is_none()
        {
            return Err(tonic::Status::failed_precondition(
                "shielded history starts below retained history",
            ));
        }
        self.terminal().verify_source(reader).await
    }
}

#[cfg(test)]
mod tests;
