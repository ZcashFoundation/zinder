//! Portable publication and serving of a settled shielded range for lightwalletd readers.

mod catalog;
mod history;
mod manifest;
mod read;
mod tail;

pub(crate) use catalog::PublishedShieldedCatalog;
pub use catalog::{PublicationConcurrency, ShieldedCatalogPublisher};
pub use history::PublishedShieldedHistory;
pub(crate) use read::{PublishedBatchedRangeRead, PublishedRangeRead};
pub use tail::EncodedTailTelemetry;
pub(crate) use tail::ShieldedTailBuffer;

use std::{collections::VecDeque, io, path::Path};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::{BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch, Network};
use zinder_query::{CanonicalReader, RocksDbCanonicalReader};

use crate::{
    block_methods::CompactBlockReads,
    sealed_history::{SealedHistory, SegmentIdentity, SegmentWriter},
};

/// Publisher's identity for one settled default shielded range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShieldedRangeIdentity {
    network_id: u32,
    artifact_schema: u16,
    first: u32,
    last: u32,
    last_hash: [u8; 32],
    sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fs_verity_sha256: Option<[u8; 32]>,
}

const MAX_VERIFIED_SETTLED_TIPS: usize = 16;

/// A settled, read-only projection of the default shielded range format.
struct PublishedShieldedRange {
    identity: ShieldedRangeIdentity,
    segment: SealedHistory,
    verified_settled_tips: Mutex<VecDeque<BlockId>>,
    anchor_check_lock: tokio::sync::Mutex<()>,
}

impl PublishedShieldedRange {
    /// Publishes the requested contiguous settled history using a captured canonical reader.
    /// This performs blocking storage I/O and encoding on the calling thread.
    ///
    /// # Errors
    ///
    /// Fails if the view, range, or canonical data cannot establish the requested
    /// settled prefix, or if the segment cannot be persisted and reopened.
    fn build(
        reader: &RocksDbCanonicalReader,
        path: &Path,
        staged: (tempfile::NamedTempFile, tempfile::NamedTempFile),
        range: BlockHeightRange,
    ) -> io::Result<Self> {
        Self::build_with_sync(reader, path, staged, range, true)
    }

    /// Builds a staged shielded segment without syncing the staging directory entry.
    /// The caller must sync the final catalog directory before committing manifests.
    fn build_staged(
        reader: &RocksDbCanonicalReader,
        path: &Path,
        staged: (tempfile::NamedTempFile, tempfile::NamedTempFile),
        range: BlockHeightRange,
    ) -> io::Result<Self> {
        Self::build_with_sync(reader, path, staged, range, false)
    }

    fn build_with_sync(
        reader: &RocksDbCanonicalReader,
        path: &Path,
        staged: (tempfile::NamedTempFile, tempfile::NamedTempFile),
        range: BlockHeightRange,
        sync_directory: bool,
    ) -> io::Result<Self> {
        let view = reader.chain_epoch();
        let network = view.network;
        if !(range.start..=view.settled_tip_height).contains(&range.end) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shielded range is outside the source's settled history",
            ));
        }
        let canonical = reader.secondary();
        let end = canonical
            .block_header_at(range.end)
            .map_err(io::Error::other)?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing terminal block header")
            })?;
        #[cfg(target_os = "linux")]
        let fresh_file = crate::sealed_history::verity::reopen_staged(&staged.0)?;
        let mut writer = SegmentWriter::create(
            path,
            staged,
            SegmentIdentity::span(
                network,
                view.artifact_schema_version.value(),
                range.start,
                BlockId::new(range.end, end.block_hash),
            ),
        )?;
        write_range_blocks(&mut writer, reader, range)?;
        let segment = if sync_directory {
            writer.finish()?
        } else {
            writer.finish_staged()?
        };
        #[cfg(target_os = "linux")]
        crate::sealed_history::verity::protect_new_segment(&fresh_file, segment.verity)?;
        Self::open(
            path,
            ShieldedRangeIdentity {
                network_id: network.id(),
                artifact_schema: view.artifact_schema_version.value(),
                first: segment.start.value(),
                last: segment.end.height.value(),
                last_hash: segment.end.hash.as_bytes(),
                sha256: segment.sha256,
                fs_verity_sha256: segment.verity,
            },
        )
    }

    /// Admits an independent copy against the identity received from its trusted publisher.
    /// The file is opened and verified on the calling thread.
    ///
    /// # Errors
    ///
    /// Fails on invalid identity, damaged or incomplete data, or a storage error.
    fn open(path: &Path, identity: ShieldedRangeIdentity) -> io::Result<Self> {
        let descriptor = SegmentIdentity {
            network: Network::from_id(identity.network_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "unknown segment network")
            })?,
            artifact_schema: identity.artifact_schema,
            start: BlockHeight::new(identity.first),
            end: BlockId::new(
                BlockHeight::new(identity.last),
                BlockHash::from_bytes(identity.last_hash),
            ),
            sha256: identity.sha256,
            verity: identity.fs_verity_sha256,
        };
        let segment = SealedHistory::open(path, descriptor)?;
        Ok(Self {
            identity,
            segment,
            verified_settled_tips: Mutex::new(VecDeque::with_capacity(MAX_VERIFIED_SETTLED_TIPS)),
            anchor_check_lock: tokio::sync::Mutex::new(()),
        })
    }

    fn is_settled_tip_verified(&self, settled: BlockId) -> bool {
        let mut tips = self.verified_settled_tips.lock();
        let Some(pos) = tips.iter().position(|cached| *cached == settled) else {
            return false;
        };
        if pos + 1 != tips.len()
            && let Some(hit) = tips.remove(pos)
        {
            tips.push_back(hit);
        }
        true
    }

    fn record_verified_settled_tip(&self, settled: BlockId) {
        let mut tips = self.verified_settled_tips.lock();
        if let Some(pos) = tips.iter().position(|cached| *cached == settled) {
            if pos + 1 != tips.len()
                && let Some(hit) = tips.remove(pos)
            {
                tips.push_back(hit);
            }
            return;
        }
        if tips.len() == MAX_VERIFIED_SETTLED_TIPS {
            tips.pop_front();
        }
        tips.push_back(settled);
    }

    /// The authenticated value a replacement reader needs to admit the same projection.
    #[must_use]
    const fn identity(&self) -> ShieldedRangeIdentity {
        self.identity
    }

    async fn check_view<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        view: &Reads::View,
        cancel: &CancellationToken,
    ) -> Result<(), Status> {
        self.check_anchor(
            Reads::view_chain_epoch(view),
            cancel,
            reads.block_hash_at(view, self.segment.identity().end.height),
        )
        .await
    }

    /// Verifies this projection against the canonical reader before its listener becomes ready.
    ///
    /// # Errors
    ///
    /// Rejects a differently settled chain, an incompatible artifact schema, or unavailable canonical data.
    async fn verify_source(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        let end = self.segment.identity().end.height;
        self.check_anchor(source.chain_epoch(), &CancellationToken::new(), async {
            source
                .block_header_at(end)
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .map(|row| row.block_hash)
                .ok_or_else(|| Status::failed_precondition("missing settled shielded range anchor"))
        })
        .await
    }

    async fn check_anchor(
        &self,
        chain: ChainEpoch,
        cancel: &CancellationToken,
        terminal: impl Future<Output = Result<BlockHash, Status>> + Send,
    ) -> Result<(), Status> {
        let segment = self.segment.identity();
        if segment.network != chain.network
            || self.identity.artifact_schema != chain.artifact_schema_version.value()
            || segment.end.height > chain.settled_tip_height
        {
            return Err(Status::failed_precondition(
                "shielded range does not belong to this settled view",
            ));
        }
        let settled = BlockId::new(chain.settled_tip_height, chain.settled_tip_hash);
        // A settled tip hash commits to its ancestry, including the authenticated segment end.
        if self.is_settled_tip_verified(settled) {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err(Status::cancelled("block-range stream cancelled"));
        }
        let _anchor_guard = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(Status::cancelled("block-range stream cancelled")),
            guard = self.anchor_check_lock.lock() => guard,
        };
        if cancel.is_cancelled() {
            return Err(Status::cancelled("block-range stream cancelled"));
        }
        if self.is_settled_tip_verified(settled) {
            return Ok(());
        }
        let anchor = if segment.end.height == chain.settled_tip_height {
            chain.settled_tip_hash
        } else {
            terminal.await?
        };
        if segment.end.hash != anchor {
            return Err(Status::failed_precondition(
                "shielded range terminal block differs from the settled chain",
            ));
        }
        self.record_verified_settled_tip(settled);
        Ok(())
    }
}

fn write_range_blocks(
    writer: &mut SegmentWriter,
    reader: &RocksDbCanonicalReader,
    range: BlockHeightRange,
) -> io::Result<()> {
    let canonical = reader.secondary();
    let mut first = range.start;
    loop {
        let last = BlockHeight::new(first.value().saturating_add(999)).min(range.end);
        let rows = canonical
            .compact_block_rows_with_lengths(
                BlockHeightRange::inclusive(first, last),
                crate::BLOCK_RANGE_CHUNK_MAX_BYTES,
            )
            .map_err(io::Error::other)?;
        let kept = rows
            .last()
            .map(|(block, _)| block.height())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty canonical range"))?;
        for (row, native_bytes) in rows {
            writer.push(row, native_bytes)?;
        }
        if kept == range.end {
            break;
        }
        first = kept
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "height overflow"))?;
    }
    Ok(())
}
