use std::{
    fs::File,
    io,
    num::NonZeroU32,
    path::{Path, PathBuf},
};

use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zinder_core::{BlockHeight, BlockHeightRange};
use zinder_query::{CanonicalReader, RocksDbCanonicalReader};

use super::{
    super::manifest::{
        install_signed, invalid, lock_directory, publish_segment, signed_contents, signing_key_from,
    },
    CatalogManifest, FORMAT, MAX_SEGMENTS, ShieldedRangeIdentity, check_parent, load_entries,
    open_segment,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublisherHead {
    count: usize,
    digest: [u8; 32],
}

impl PublisherHead {
    fn of(entries: &[ShieldedRangeIdentity]) -> io::Result<Self> {
        Ok(Self {
            count: entries.len(),
            digest: Sha256::digest(serde_json::to_vec(entries)?).into(),
        })
    }
}

/// Publishes complete newly settled segments from successive captured canonical readers.
pub struct ShieldedCatalogPublisher {
    _lock: File,
    directory: PathBuf,
    key: Ed25519KeyPair,
    first: BlockHeight,
    segment_blocks: NonZeroU32,
    entries: Vec<ShieldedRangeIdentity>,
    needs_reopen: bool,
}

impl ShieldedCatalogPublisher {
    /// Resumes the signed catalog in `directory`, or starts at `first` when empty.
    /// Both operations hold one exclusive publisher lease for this directory.
    /// `segment_blocks` sets the number of newly settled heights per publication.
    ///
    /// # Errors
    ///
    /// Rejects another active publisher, invalid keys, rollback below the last durable
    /// publisher checkpoint, missing segments, or gaps in the resumed publication.
    pub fn open(
        directory: &Path,
        signing_key: &Path,
        first: BlockHeight,
        segment_blocks: NonZeroU32,
    ) -> io::Result<Self> {
        let key = signing_key_from(signing_key)?;
        let lock = lock_directory(directory)?;
        let current = directory.join("current.json");
        let entries = if current.try_exists()? {
            load_entries(&current, key.public_key().as_ref())?
        } else {
            Vec::new()
        };
        if entries
            .first()
            .is_some_and(|entry| entry.first != first.value())
        {
            return Err(invalid(
                "configured first height differs from resumed shielded catalog",
            ));
        }
        let head_path = directory.join(".publisher-head.json");
        if head_path.try_exists()? {
            let head: PublisherHead = serde_json::from_str(&signed_contents(
                &head_path,
                key.public_key().as_ref(),
                2048,
            )?)?;
            let prefix = entries.get(..head.count).ok_or_else(|| {
                invalid("signed shielded catalog rolled back below publisher checkpoint")
            })?;
            if PublisherHead::of(prefix)?.digest != head.digest {
                return Err(invalid(
                    "signed shielded catalog changed the publisher checkpoint",
                ));
            }
        } else if !entries.is_empty() {
            return Err(invalid(
                "missing durable checkpoint for existing shielded catalog",
            ));
        }
        let mut previous = None;
        for entry in &entries {
            let segment = open_segment(directory, entry, previous)?;
            previous = Some(segment.identity());
        }
        let resumed = Self {
            _lock: lock,
            directory: directory.to_owned(),
            key,
            first,
            segment_blocks,
            entries,
            needs_reopen: false,
        };
        resumed.checkpoint()?;
        Ok(resumed)
    }

    fn checkpoint(&self) -> io::Result<()> {
        let head = PublisherHead::of(&self.entries)?;
        install_signed(
            &self.directory.join(".publisher-head.json"),
            serde_json::to_string(&head)?,
            &self.key,
        )
    }

    /// Publishes the next full settled segment, returning false when the remaining suffix is shorter.
    /// Reads only previously unpublished canonical block payloads. This performs blocking file I/O.
    ///
    /// # Errors
    ///
    /// Returns an error on a displaced publication, missing canonical data, or a storage failure.
    pub fn publish_next(&mut self, reader: &RocksDbCanonicalReader) -> io::Result<bool> {
        if self.needs_reopen {
            return Err(invalid(
                "reopen the publisher after a failed catalog installation",
            ));
        }
        let chain = reader.chain_epoch();
        let prior = self.entries.last().copied();
        let start = if let Some(previous) = prior {
            let end = BlockHeight::new(previous.last);
            if previous.network_id != chain.network.id()
                || previous.artifact_schema != chain.artifact_schema_version.value()
                || end > chain.settled_tip_height
                || reader
                    .secondary()
                    .block_header_at(end)
                    .map_err(io::Error::other)?
                    .is_none_or(|header| header.block_hash.as_bytes() != previous.last_hash)
            {
                return Err(invalid(
                    "previous shielded catalog does not belong to the writer's settled chain",
                ));
            }
            let Some(first) = end.next() else {
                return Ok(false);
            };
            first
        } else {
            self.first
        };
        let Some(end) = start
            .value()
            .checked_add(self.segment_blocks.get() - 1)
            .map(BlockHeight::new)
        else {
            return Ok(false);
        };
        if end > chain.settled_tip_height {
            return Ok(false);
        }
        if self.entries.len() >= MAX_SEGMENTS {
            return Err(invalid("shielded catalog segment limit exceeded"));
        }
        let range = BlockHeightRange::inclusive(start, end);
        let segment = publish_segment(reader, &self.directory, range)?;
        check_parent(prior, &segment)?;
        self.entries.push(segment.identity());
        let catalog = CatalogManifest {
            format: FORMAT.to_owned(),
            segments: &self.entries,
        };
        let current = self.directory.join("current.json");
        // A rename may be visible even if the subsequent directory fsync fails.
        // Resume from the signed files before attempting another publication.
        self.needs_reopen = true;
        install_signed(&current, serde_json::to_string(&catalog)?, &self.key)?;
        self.checkpoint()?;
        self.needs_reopen = false;
        Ok(true)
    }
}
