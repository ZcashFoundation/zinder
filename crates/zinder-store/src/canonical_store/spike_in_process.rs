//! Spike-only hooks for in-process serving from primary snapshots.
//!
//! Throwaway code for the in-process serving spike. It exposes the shared
//! primary handle, snapshot-bound reads over the real canonical row formats,
//! and one process-wide batch observer so a benchmark can time synced writes
//! and add synthetic density rows to the same batch. Nothing here is part of
//! the supported storage contract.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use rust_rocksdb::{
    DB, DBRawIteratorWithThreadMode, IteratorMode, Options, ReadOptions, SnapshotWithThreadMode,
    WriteBatch, WriteOptions,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, CompactBlockArtifact, Network, TransactionId,
    TransactionLocation, ValidatedCanonicalBlockReplay,
    wire::{encode_height_key_ascending, encode_internal_transaction_id},
};

use super::{
    CanonicalReplayRangeScan, CanonicalReplayScan, CanonicalStoreError,
    CanonicalStoreReadyEvidence, RocksDbCanonicalStore,
    block_load::encode_block_position,
    block_replay::{block_replay_column_family, decode_persisted_replay},
    publication::column_family,
    reader::{decode_block_header, decode_chain_epoch, decode_transaction_location},
    rocksdb::{
        BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY,
        COMPACT_BLOCK_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, STORE_CONTROL_KEY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY, canonical_data_options,
    },
};

/// One primary snapshot of a spike-shared database.
pub type SpikeSnapshot<'db> = SnapshotWithThreadMode<'db, DB>;

/// Which synced batch the observer sees.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SpikeBatchKind {
    /// One canonical live append.
    CanonicalAppend,
    /// One canonical suffix replacement.
    CanonicalReplacement,
    /// One wallet projection transition.
    WalletTransition,
}

/// Process-wide observer of synced store batches.
pub trait SpikeBatchObserver: Send + Sync {
    /// Runs immediately before the synced write and may add rows to it.
    fn before_write(&self, kind: SpikeBatchKind, db: &DB, batch: &mut WriteBatch);
    /// Runs after a successful synced write with the final batch size.
    fn after_write(&self, kind: SpikeBatchKind, batch_bytes: usize, write_elapsed: Duration);
}

static SPIKE_BATCH_OBSERVER: RwLock<Option<Arc<dyn SpikeBatchObserver>>> = RwLock::new(None);

/// Installs or clears the process-wide synced-batch observer.
pub fn spike_install_batch_observer(observer: Option<Arc<dyn SpikeBatchObserver>>) {
    *SPIKE_BATCH_OBSERVER.write() = observer;
}

/// Writes one batch through the installed observer, if any.
pub fn spike_observed_write(
    kind: SpikeBatchKind,
    db: &DB,
    batch: &mut WriteBatch,
    write_options: &WriteOptions,
) -> Result<(), rust_rocksdb::Error> {
    let observer = SPIKE_BATCH_OBSERVER.read().clone();
    if let Some(observer) = &observer {
        observer.before_write(kind, db, batch);
    }
    let started = Instant::now();
    db.write_opt(&*batch, write_options)?;
    let elapsed = started.elapsed();
    if let Some(observer) = &observer {
        observer.after_write(kind, batch.size_in_bytes(), elapsed);
    }
    Ok(())
}

impl RocksDbCanonicalStore {
    /// Spike: returns the shared primary handle for in-process snapshot readers.
    #[must_use]
    pub fn spike_shared_db(&self) -> Arc<DB> {
        Arc::clone(&self.bounded_open.db)
    }

    /// Spike: returns the retained open options that carry the statistics tickers.
    #[must_use]
    pub fn spike_statistics(&self) -> Arc<Options> {
        Arc::clone(&self.bounded_open.statistics)
    }

    /// Spike: production canonical data-family options on this store's block cache.
    #[must_use]
    pub fn spike_data_cf_options(&self, budget: crate::RocksDbResourceBudget) -> Options {
        canonical_data_options(&self.bounded_open.block_cache, budget)
    }

    /// Spike: bounded connected replay rows from the primary's admitted fence.
    pub fn spike_scan_canonical_replay_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<CanonicalReplayRangeScan<'_>, CanonicalStoreError> {
        CanonicalReplayRangeScan::new(&self.bounded_open.db, &self.ready_evidence, range)
    }
}

/// Read options bound to an optional snapshot.
#[must_use]
pub fn spike_read_options(snapshot: Option<&SpikeSnapshot<'_>>, fill_cache: bool) -> ReadOptions {
    let mut options = ReadOptions::default();
    options.fill_cache(fill_cache);
    if let Some(snapshot) = snapshot {
        options.set_snapshot(snapshot);
    }
    options
}

/// Everything a reader must see unchanged through one consistent view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpikeCanonicalViewFingerprint {
    /// Highest retained epoch id.
    pub epoch_id: u64,
    /// Visible tip height named by that epoch.
    pub visible_tip_height: u32,
    /// Visible tip hash named by that epoch.
    pub visible_tip_hash: [u8; 32],
    /// Settled tip height named by that epoch.
    pub settled_tip_height: u32,
    /// Highest durable chain-event sequence.
    pub latest_event_sequence: u64,
    /// Hash of the READY control record bytes.
    pub control_hash: u64,
    /// Number of displaced-archive rows.
    pub displaced_archive_rows: u64,
}

/// Reads only the latest epoch's visible tip through one optional snapshot.
pub fn spike_canonical_epoch_tip(
    db: &DB,
    network: Network,
    snapshot: Option<&SpikeSnapshot<'_>>,
) -> Result<(u32, [u8; 32]), CanonicalStoreError> {
    let epoch_family = column_family(db, CHAIN_EPOCH_COLUMN_FAMILY)?;
    let mut epochs = db.iterator_cf_opt(
        &epoch_family,
        spike_read_options(snapshot, true),
        IteratorMode::End,
    );
    let (epoch_key, epoch_value) = epochs
        .next()
        .ok_or_else(|| CanonicalStoreError::publication("spike: no chain epoch row"))?
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike latest epoch read",
            source,
        })?;
    let epoch_id = u64::from_be_bytes(
        epoch_key
            .as_ref()
            .try_into()
            .map_err(|_| CanonicalStoreError::publication("spike: epoch key is not 8 bytes"))?,
    );
    let epoch = decode_chain_epoch(
        network,
        zinder_core::ChainEpochId::new(epoch_id),
        &epoch_value,
    )?;
    Ok((
        epoch.visible_tip_height.value(),
        epoch.visible_tip_hash.as_bytes(),
    ))
}

/// Reads the view fingerprint through one optional snapshot.
pub fn spike_canonical_view_fingerprint(
    db: &DB,
    network: Network,
    snapshot: Option<&SpikeSnapshot<'_>>,
) -> Result<SpikeCanonicalViewFingerprint, CanonicalStoreError> {
    let epoch_family = column_family(db, CHAIN_EPOCH_COLUMN_FAMILY)?;
    let mut epochs = db.iterator_cf_opt(
        &epoch_family,
        spike_read_options(snapshot, true),
        IteratorMode::End,
    );
    let (epoch_key, epoch_value) = epochs
        .next()
        .ok_or_else(|| CanonicalStoreError::publication("spike: no chain epoch row"))?
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike latest epoch read",
            source,
        })?;
    let epoch_id = u64::from_be_bytes(
        epoch_key
            .as_ref()
            .try_into()
            .map_err(|_| CanonicalStoreError::publication("spike: epoch key is not 8 bytes"))?,
    );
    let epoch = decode_chain_epoch(
        network,
        zinder_core::ChainEpochId::new(epoch_id),
        &epoch_value,
    )?;

    let event_family = column_family(db, CHAIN_EVENT_COLUMN_FAMILY)?;
    let mut events = db.iterator_cf_opt(
        &event_family,
        spike_read_options(snapshot, true),
        IteratorMode::End,
    );
    let latest_event_sequence =
        match events.next() {
            Some(row) => {
                let (key, _) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "spike latest event read",
                    source,
                })?;
                u64::from_be_bytes(key.as_ref().try_into().map_err(|_| {
                    CanonicalStoreError::publication("spike: event key is not 8 bytes")
                })?)
            }
            None => 0,
        };

    let control = db
        .get_opt(STORE_CONTROL_KEY, &spike_read_options(snapshot, true))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike control read",
            source,
        })?
        .ok_or_else(|| CanonicalStoreError::publication("spike: control record is absent"))?;
    let mut hasher = DefaultHasher::new();
    control.hash(&mut hasher);

    let displaced_family = column_family(db, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)?;
    let mut displaced_archive_rows = 0_u64;
    for row in db.iterator_cf_opt(
        &displaced_family,
        spike_read_options(snapshot, false),
        IteratorMode::Start,
    ) {
        row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike displaced archive count",
            source,
        })?;
        displaced_archive_rows = displaced_archive_rows.saturating_add(1);
    }

    Ok(SpikeCanonicalViewFingerprint {
        epoch_id,
        visible_tip_height: epoch.visible_tip_height.value(),
        visible_tip_hash: epoch.visible_tip_hash.as_bytes(),
        settled_tip_height: epoch.settled_tip_height.value(),
        latest_event_sequence,
        control_hash: hasher.finish(),
        displaced_archive_rows,
    })
}

/// One compact-block row read with its canonical header.
#[derive(Clone, Debug)]
pub struct SpikeCompactBlockRow {
    /// Row height.
    pub height: u32,
    /// Header block hash.
    pub block_hash: [u8; 32],
    /// Header parent hash.
    pub parent_hash: [u8; 32],
    /// Encoded compact-block value length.
    pub encoded_bytes: usize,
    /// Hash of the encoded compact-block value.
    pub value_hash: u64,
    /// Decoded compact block.
    pub compact: CompactBlockArtifact,
}

/// Outcome of one height read: present, or the family whose row is absent.
#[derive(Clone, Debug)]
pub enum SpikeCompactBlockRead {
    /// Both rows exist and agree.
    Present(Box<SpikeCompactBlockRow>),
    /// The compact-block row is absent.
    CompactAbsent(u32),
    /// The header row is absent while the compact row exists.
    HeaderAbsent(u32),
    /// The header names a different block than the compact row.
    HeaderMismatch(u32),
}

/// Reads compact blocks and headers through one optional snapshot.
pub fn spike_compact_blocks(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    range: BlockHeightRange,
) -> Result<Vec<SpikeCompactBlockRead>, CanonicalStoreError> {
    let compact_family = column_family(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let header_family = column_family(db, BLOCK_HEADER_COLUMN_FAMILY)?;
    let heights = range.into_iter().collect::<Vec<_>>();
    let keys = heights
        .iter()
        .copied()
        .map(encode_block_position)
        .collect::<Vec<_>>();
    let read_options = spike_read_options(snapshot, true);
    let compact_rows = db.batched_multi_get_cf_opt(
        &compact_family,
        keys.iter().map(<[u8; 4]>::as_slice),
        true,
        &read_options,
    );
    let header_rows = db.batched_multi_get_cf_opt(
        &header_family,
        keys.iter().map(<[u8; 4]>::as_slice),
        true,
        &read_options,
    );
    let mut reads = Vec::with_capacity(heights.len());
    for ((height, compact_row), header_row) in
        heights.into_iter().zip(compact_rows).zip(header_rows)
    {
        let compact_row = compact_row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike compact block read",
            source,
        })?;
        let header_row = header_row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike block header read",
            source,
        })?;
        let Some(compact_row) = compact_row else {
            reads.push(SpikeCompactBlockRead::CompactAbsent(height.value()));
            continue;
        };
        let Some(header_row) = header_row else {
            reads.push(SpikeCompactBlockRead::HeaderAbsent(height.value()));
            continue;
        };
        reads.push(decode_compact_read(height, &compact_row, &header_row)?);
    }
    Ok(reads)
}

fn decode_compact_read(
    height: BlockHeight,
    compact_row: &[u8],
    header_row: &[u8],
) -> Result<SpikeCompactBlockRead, CanonicalStoreError> {
    let header = decode_block_header(height, header_row)?;
    let compact = crate::decode_compact_block_artifact(compact_row).map_err(|_| {
        CanonicalStoreError::publication(format!(
            "spike: compact block at height {} is invalid",
            height.value()
        ))
    })?;
    if compact.height() != height || compact.block_hash() != header.block_hash {
        return Ok(SpikeCompactBlockRead::HeaderMismatch(height.value()));
    }
    let mut hasher = DefaultHasher::new();
    compact_row.hash(&mut hasher);
    Ok(SpikeCompactBlockRead::Present(Box::new(
        SpikeCompactBlockRow {
            height: height.value(),
            block_hash: header.block_hash.as_bytes(),
            parent_hash: header.parent_hash.as_bytes(),
            encoded_bytes: compact_row.len(),
            value_hash: hasher.finish(),
            compact,
        },
    )))
}

/// Returns whether a compact-block row exists at `height` in the view.
pub fn spike_compact_block_exists(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    height: u32,
) -> Result<bool, CanonicalStoreError> {
    let compact_family = column_family(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    db.get_pinned_cf_opt(
        &compact_family,
        encode_block_position(BlockHeight::new(height)),
        &spike_read_options(snapshot, true),
    )
    .map(|row| row.is_some())
    .map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "spike compact block probe",
        source,
    })
}

/// A compact-block iterator that stays open, pinning its version, for one hold.
pub struct SpikeCompactBlockCursor<'db> {
    db: &'db DB,
    snapshot: Option<&'db SpikeSnapshot<'db>>,
    iterator: DBRawIteratorWithThreadMode<'db, DB>,
}

impl<'db> SpikeCompactBlockCursor<'db> {
    /// Opens one held iterator over the compact-block family.
    pub fn open(
        db: &'db DB,
        snapshot: Option<&'db SpikeSnapshot<'db>>,
    ) -> Result<Self, CanonicalStoreError> {
        let compact_family = column_family(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
        let mut read_options = spike_read_options(snapshot, true);
        read_options.set_readahead_size(2 * 1024 * 1024);
        let iterator = db.raw_iterator_cf_opt(&compact_family, read_options);
        Ok(Self {
            db,
            snapshot,
            iterator,
        })
    }

    /// Reads one inclusive range through the held iterator and verifies headers.
    pub fn read_range(
        &mut self,
        range: BlockHeightRange,
    ) -> Result<Vec<SpikeCompactBlockRead>, CanonicalStoreError> {
        let mut raw = Vec::new();
        self.iterator.seek(encode_block_position(range.start));
        let mut expected = range.start.value();
        while expected <= range.end.value() {
            let Some((key, value)) = self.iterator.item() else {
                break;
            };
            let height = u32::from_be_bytes(key.try_into().map_err(|_| {
                CanonicalStoreError::publication("spike: compact key is not 4 bytes")
            })?);
            if height > range.end.value() {
                break;
            }
            while expected < height {
                raw.push((expected, None));
                expected = expected.saturating_add(1);
            }
            raw.push((height, Some(value.to_vec())));
            expected = height.saturating_add(1);
            self.iterator.next();
        }
        self.iterator
            .status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "spike held compact iterator",
                source,
            })?;
        while expected <= range.end.value() {
            raw.push((expected, None));
            expected = expected.saturating_add(1);
        }
        let header_family = column_family(self.db, BLOCK_HEADER_COLUMN_FAMILY)?;
        let read_options = spike_read_options(self.snapshot, true);
        let keys = raw
            .iter()
            .map(|(height, _)| encode_block_position(BlockHeight::new(*height)))
            .collect::<Vec<_>>();
        let headers = self.db.batched_multi_get_cf_opt(
            &header_family,
            keys.iter().map(<[u8; 4]>::as_slice),
            true,
            &read_options,
        );
        let mut reads = Vec::with_capacity(raw.len());
        for ((height, compact_row), header_row) in raw.into_iter().zip(headers) {
            let header_row =
                header_row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "spike held header read",
                    source,
                })?;
            let Some(compact_row) = compact_row else {
                reads.push(SpikeCompactBlockRead::CompactAbsent(height));
                continue;
            };
            let Some(header_row) = header_row else {
                reads.push(SpikeCompactBlockRead::HeaderAbsent(height));
                continue;
            };
            reads.push(decode_compact_read(
                BlockHeight::new(height),
                &compact_row,
                &header_row,
            )?);
        }
        Ok(reads)
    }
}

/// Resolves one transaction location and its raw blob through one view.
pub fn spike_transaction_lookup(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    transaction_id: TransactionId,
) -> Result<Option<(TransactionLocation, Option<usize>)>, CanonicalStoreError> {
    let read_options = spike_read_options(snapshot, true);
    let location_family = column_family(db, TRANSACTION_LOCATION_COLUMN_FAMILY)?;
    let Some(encoded) = db
        .get_pinned_cf_opt(
            &location_family,
            encode_internal_transaction_id(transaction_id),
            &read_options,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike transaction location read",
            source,
        })?
    else {
        return Ok(None);
    };
    let location = decode_transaction_location(transaction_id, &encoded)?;
    let blob_family = column_family(db, TRANSACTION_BLOB_COLUMN_FAMILY)?;
    let blob = db
        .get_pinned_cf_opt(
            &blob_family,
            super::block_load::encode_transaction_position(
                location.block_height,
                location.tx_index_in_block,
            ),
            &read_options,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike transaction blob read",
            source,
        })?
        .map(|blob| blob.len());
    Ok(Some((location, blob)))
}

/// Reads one validated replay row through a snapshot.
pub fn spike_block_replay_at(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    height: BlockHeight,
) -> Result<Option<ValidatedCanonicalBlockReplay>, CanonicalStoreError> {
    let key = encode_height_key_ascending(height);
    let Some(encoded) = db
        .get_cf_opt(
            &block_replay_column_family(db)?,
            key,
            &spike_read_options(snapshot, false),
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "spike replay read",
            source,
        })?
    else {
        return Ok(None);
    };
    let (_, replay) = decode_persisted_replay(&key, &encoded)?;
    Ok(Some(replay))
}

/// Opens the full authenticated replay scan through one snapshot.
pub fn spike_replay_scan_at_snapshot<'db>(
    db: &'db DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    snapshot: &'db SpikeSnapshot<'db>,
) -> Result<CanonicalReplayScan<'db>, CanonicalStoreError> {
    CanonicalReplayScan::spike_new_at_snapshot(db, ready_evidence, snapshot)
}
