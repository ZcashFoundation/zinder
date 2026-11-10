//! Capture and read boundary for blocks displaced by canonical replacements.

use std::{mem::size_of, num::NonZeroU32};

use zinder_core::{
    BlockHash, BlockHeightRange, BlockId, ChainEpoch, DisplacedBlock,
    DisplacedBlockArchiveCoverage, DisplacedBlockCoinbaseOutput, DisplacedRootArchiveCoverage,
    DisplacedRootCandidate, FinalNoteCommitmentRoot, Network, ShieldedProtocol,
    TransparentOutPoint,
};

use crate::{
    ArtifactFamily, DisplacedBlockCursor, DisplacedBlockPage, ReorgWindowChange, StoreError,
    block_artifact::{
        read_block_blob_artifact, read_block_header_artifact,
        read_block_transaction_index_artifacts_at_height,
    },
    chain_store::read_visible_transparent_output_block_outpoints,
    final_note_commitment_roots::read_final_note_commitment_roots,
    format::{
        StoreKey, decode_displaced_block, decode_displaced_block_archive_coverage,
        encode_displaced_block, encode_displaced_block_archive_coverage,
    },
    kv::{PrefixScanControl, RocksChainStoreRead, StoragePut, StorageTable},
    transparent_output::read_current_transparent_outputs_by_outpoints,
};

/// Read boundary for the product-neutral displaced-block archive.
pub trait DisplacedBlockStore {
    /// Reads a bounded newest-first page strictly older than `after`.
    fn displaced_block_page(
        &self,
        after: Option<&DisplacedBlockCursor>,
        limit: NonZeroU32,
    ) -> Result<DisplacedBlockPage, StoreError>;

    /// Reads up to `limit` archived blocks in newest event/height order.
    fn newest_displaced_blocks(&self, limit: NonZeroU32)
    -> Result<Vec<DisplacedBlock>, StoreError>;

    /// Reads one archived block by its stable block hash.
    fn displaced_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<DisplacedBlock>, StoreError>;

    /// Reads up to `limit` blocks linked to one displacement event, newest height first.
    fn displaced_blocks_for_event(
        &self,
        event_sequence: u64,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, StoreError>;

    /// Returns the total number of archived displaced blocks.
    fn displaced_block_count(&self) -> Result<u64, StoreError>;

    /// Returns the event from which replacement archive coverage is guaranteed.
    fn displaced_block_archive_coverage(
        &self,
    ) -> Result<Option<DisplacedBlockArchiveCoverage>, StoreError>;
}

pub(crate) fn build_displaced_block_archive_puts_for_change(
    inner: &impl RocksChainStoreRead,
    previous_chain_epoch: Option<ChainEpoch>,
    displacement_epoch: ChainEpoch,
    displacement_event_sequence: u64,
    reorg_window_change: &ReorgWindowChange,
) -> Result<Vec<StoragePut>, StoreError> {
    let ReorgWindowChange::Replace { from_height } = reorg_window_change else {
        return Ok(Vec::new());
    };
    let previous_chain_epoch =
        previous_chain_epoch.ok_or(StoreError::InvalidChainEpochArtifacts {
            reason: "replacement requires an existing chain epoch",
        })?;
    build_displaced_block_archive_puts(
        inner,
        previous_chain_epoch,
        displacement_epoch,
        displacement_event_sequence,
        *from_height,
    )
}

fn build_displaced_block_archive_puts(
    inner: &impl RocksChainStoreRead,
    previous_chain_epoch: ChainEpoch,
    displacement_epoch: ChainEpoch,
    displacement_event_sequence: u64,
    from_height: zinder_core::BlockHeight,
) -> Result<Vec<StoragePut>, StoreError> {
    let range = BlockHeightRange::inclusive(from_height, previous_chain_epoch.visible_tip_height);
    let existing_count = read_displaced_block_count(inner)?;
    let mut root_coverage =
        read_displaced_root_archive_coverage(inner)?.unwrap_or(DisplacedRootArchiveCoverage {
            activation_event_sequence: displacement_event_sequence,
            activation_epoch: displacement_epoch.id,
            activated_at: displacement_epoch.created_at,
            captured_block_count: 0,
            root_artifact_unavailable_count: 0,
        });
    let mut puts = Vec::new();
    let mut captured_count = 0u64;

    for height in range {
        let block = capture_displaced_block(
            inner,
            previous_chain_epoch,
            displacement_epoch,
            displacement_event_sequence,
            height,
        )?;
        push_displaced_block_archive_rows(&mut puts, displacement_epoch.network, &block)?;
        if block.final_note_commitment_roots.is_none() {
            root_coverage.root_artifact_unavailable_count = root_coverage
                .root_artifact_unavailable_count
                .checked_add(1)
                .ok_or(StoreError::InvalidChainEpochArtifacts {
                    reason: "displaced root artifact unavailable count overflowed",
                })?;
        }
        root_coverage.captured_block_count = root_coverage
            .captured_block_count
            .checked_add(1)
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced root captured block count overflowed",
            })?;
        captured_count =
            captured_count
                .checked_add(1)
                .ok_or(StoreError::InvalidChainEpochArtifacts {
                    reason: "displaced block archive count overflowed",
                })?;
    }

    let total_count = existing_count.checked_add(captured_count).ok_or(
        StoreError::InvalidChainEpochArtifacts {
            reason: "displaced block archive count overflowed",
        },
    )?;
    puts.push(StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::displaced_block_count(),
        value: total_count.to_be_bytes().to_vec(),
    });
    if read_displaced_block_archive_coverage(inner)?.is_none() {
        puts.push(StoragePut {
            table: StorageTable::StorageControl,
            key: StoreKey::displaced_block_archive_coverage(),
            value: encode_displaced_block_archive_coverage(DisplacedBlockArchiveCoverage {
                activation_event_sequence: displacement_event_sequence,
                activation_epoch: displacement_epoch.id,
                activated_at: displacement_epoch.created_at,
            }),
        });
    }
    puts.push(StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::displaced_root_archive_coverage(),
        value: encode_displaced_root_archive_coverage(root_coverage),
    });

    Ok(puts)
}

fn push_displaced_block_archive_rows(
    puts: &mut Vec<StoragePut>,
    network: Network,
    block: &DisplacedBlock,
) -> Result<(), StoreError> {
    let encoded = encode_displaced_block(block)?;
    let block_id = BlockId::new(block.header.height, block.block_hash);
    puts.push(StoragePut {
        table: StorageTable::DisplacedBlock,
        key: StoreKey::displaced_block_by_order(
            network,
            block.displacement_event_sequence,
            block_id.height,
        ),
        value: encoded.clone(),
    });
    puts.push(StoragePut {
        table: StorageTable::DisplacedBlock,
        key: StoreKey::displaced_block_by_hash(network, block_id.hash),
        value: encoded,
    });
    if let Some(roots) = block.final_note_commitment_roots {
        for (protocol, root) in present_roots(roots) {
            puts.push(StoragePut {
                table: StorageTable::DisplacedBlock,
                key: StoreKey::displaced_root_index(
                    network,
                    root,
                    protocol,
                    (block.displacement_event_sequence, block_id),
                ),
                value: Vec::new(),
            });
        }
    }
    Ok(())
}

fn capture_displaced_block(
    inner: &impl RocksChainStoreRead,
    previous_chain_epoch: ChainEpoch,
    displacement_epoch: ChainEpoch,
    displacement_event_sequence: u64,
    height: zinder_core::BlockHeight,
) -> Result<DisplacedBlock, StoreError> {
    let header = read_block_header_artifact(inner, previous_chain_epoch, height)?.ok_or(
        StoreError::ArtifactMissing {
            family: ArtifactFamily::BlockHeader,
            key: StoreKey::displaced_block_by_order(
                previous_chain_epoch.network,
                displacement_event_sequence,
                height,
            )
            .into(),
        },
    )?;
    let transaction_ids =
        read_block_transaction_index_artifacts_at_height(inner, previous_chain_epoch, height)?
            .into_iter()
            .map(|row| row.transaction_id)
            .collect::<Vec<_>>();
    let coinbase_outputs = match transaction_ids.first().copied() {
        Some(coinbase_transaction_id) => {
            read_coinbase_outputs(inner, previous_chain_epoch, height, coinbase_transaction_id)?
        }
        None => Vec::new(),
    };
    let raw_block_bytes = read_block_blob_artifact(inner, previous_chain_epoch, height)?
        .map(|blob| blob.raw_block_bytes);
    let final_note_commitment_roots =
        read_final_note_commitment_roots(inner, previous_chain_epoch, height)?;
    Ok(DisplacedBlock {
        block_hash: header.block_hash,
        header,
        transaction_ids,
        coinbase_outputs,
        raw_block_bytes,
        final_note_commitment_roots,
        displacement_event_sequence,
        displacement_epoch: displacement_epoch.id,
        displaced_at: displacement_epoch.created_at,
    })
}

fn read_coinbase_outputs(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    height: zinder_core::BlockHeight,
    coinbase_transaction_id: zinder_core::TransactionId,
) -> Result<Vec<DisplacedBlockCoinbaseOutput>, StoreError> {
    let outpoints = read_visible_transparent_output_block_outpoints(inner, chain_epoch, height)?
        .into_iter()
        .filter(|outpoint| outpoint.transaction_id == coinbase_transaction_id)
        .collect::<Vec<TransparentOutPoint>>();
    let outputs = read_current_transparent_outputs_by_outpoints(inner, chain_epoch, &outpoints)?;
    let mut payouts = outpoints
        .into_iter()
        .filter_map(|outpoint| {
            outputs.get(&outpoint).map(|output| {
                DisplacedBlockCoinbaseOutput::new(
                    outpoint.output_index,
                    output.value_zat,
                    output.script_pub_key.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    payouts.sort_by_key(|output| output.output_index);
    Ok(payouts)
}

pub(crate) fn read_newest_displaced_blocks(
    inner: &impl RocksChainStoreRead,
    network: Network,
    limit: NonZeroU32,
) -> Result<Vec<DisplacedBlock>, StoreError> {
    read_displaced_blocks_by_prefix(
        inner,
        &StoreKey::displaced_block_order_prefix(network),
        limit,
    )
}

pub(crate) fn read_displaced_block_page(
    inner: &impl RocksChainStoreRead,
    network: Network,
    after: Option<&DisplacedBlockCursor>,
    limit: NonZeroU32,
) -> Result<DisplacedBlockPage, StoreError> {
    if let Some(cursor) = after {
        let cursor_key =
            StoreKey::displaced_block_by_order(network, cursor.event_sequence(), cursor.height());
        let linked = inner
            .get(StorageTable::DisplacedBlock, &cursor_key)?
            .map(|record_bytes| decode_displaced_block(&cursor_key, &record_bytes))
            .transpose()?;
        if linked
            .as_ref()
            .is_none_or(|block| block.block_hash != cursor.block_hash())
        {
            return Err(StoreError::DisplacedBlockCursorInvalid);
        }
    }
    let prefix = StoreKey::displaced_block_order_prefix(network);
    let max_blocks = usize::try_from(limit.get()).unwrap_or(usize::MAX);
    let mut blocks = Vec::with_capacity(max_blocks.saturating_add(1));
    let mut visit = |key_bytes: &[u8], record_bytes: &[u8]| {
        let key = StoreKey::from_raw_bytes(key_bytes);
        blocks.push(decode_displaced_block(&key, record_bytes)?);
        Ok(if blocks.len() > max_blocks {
            PrefixScanControl::Stop
        } else {
            PrefixScanControl::Continue
        })
    };
    match after {
        Some(cursor) => inner.scan_prefix_reverse_before(
            StorageTable::DisplacedBlock,
            &prefix,
            &StoreKey::displaced_block_by_order(network, cursor.event_sequence(), cursor.height()),
            &mut visit,
        )?,
        None => inner.scan_prefix_reverse(StorageTable::DisplacedBlock, &prefix, &mut visit)?,
    }
    let has_more = blocks.len() > max_blocks;
    blocks.truncate(max_blocks);
    let next_cursor = has_more
        .then(|| {
            blocks.last().map(|block| {
                DisplacedBlockCursor::from_position(
                    block.displacement_event_sequence,
                    block.header.height,
                    block.block_hash,
                )
            })
        })
        .flatten();
    Ok(DisplacedBlockPage {
        blocks,
        has_more,
        next_cursor,
    })
}

pub(crate) fn read_displaced_blocks_for_event(
    inner: &impl RocksChainStoreRead,
    network: Network,
    event_sequence: u64,
    limit: NonZeroU32,
) -> Result<Vec<DisplacedBlock>, StoreError> {
    read_displaced_blocks_by_prefix(
        inner,
        &StoreKey::displaced_block_event_prefix(network, event_sequence),
        limit,
    )
}

fn read_displaced_blocks_by_prefix(
    inner: &impl RocksChainStoreRead,
    prefix: &StoreKey,
    limit: NonZeroU32,
) -> Result<Vec<DisplacedBlock>, StoreError> {
    let max_blocks = usize::try_from(limit.get()).unwrap_or(usize::MAX);
    let mut blocks = Vec::with_capacity(max_blocks);
    inner.scan_prefix_reverse(
        StorageTable::DisplacedBlock,
        prefix,
        &mut |key_bytes, record_bytes| {
            let key = StoreKey::from_raw_bytes(key_bytes);
            blocks.push(decode_displaced_block(&key, record_bytes)?);
            Ok(if blocks.len() >= max_blocks {
                PrefixScanControl::Stop
            } else {
                PrefixScanControl::Continue
            })
        },
    )?;
    Ok(blocks)
}

pub(crate) fn read_displaced_block_by_hash(
    inner: &impl RocksChainStoreRead,
    network: Network,
    block_hash: BlockHash,
) -> Result<Option<DisplacedBlock>, StoreError> {
    let key = StoreKey::displaced_block_by_hash(network, block_hash);
    inner
        .get(StorageTable::DisplacedBlock, &key)?
        .map(|record_bytes| decode_displaced_block(&key, &record_bytes))
        .transpose()
}

pub(crate) fn read_displaced_block_count(
    inner: &impl RocksChainStoreRead,
) -> Result<u64, StoreError> {
    let key = StoreKey::displaced_block_count();
    let Some(count_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(0);
    };
    let bytes =
        <[u8; 8]>::try_from(count_bytes.as_slice()).map_err(|_| StoreError::ArtifactCorrupt {
            family: ArtifactFamily::DisplacedBlock,
            key: key.clone().into(),
            reason: "displaced block count must be 8 bytes",
        })?;
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn read_displaced_block_archive_coverage(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<DisplacedBlockArchiveCoverage>, StoreError> {
    let key = StoreKey::displaced_block_archive_coverage();
    inner
        .get(StorageTable::StorageControl, &key)?
        .map(|record_bytes| decode_displaced_block_archive_coverage(&key, &record_bytes))
        .transpose()
}

pub(crate) fn read_displaced_root_candidates(
    inner: &impl RocksChainStoreRead,
    network: Network,
    protocol: ShieldedProtocol,
    root: FinalNoteCommitmentRoot,
    limit: NonZeroU32,
) -> Result<Vec<DisplacedRootCandidate>, StoreError> {
    let prefix = StoreKey::displaced_root_index_prefix(network, root, protocol);
    let max_candidates = usize::try_from(limit.get()).unwrap_or(usize::MAX);
    let mut candidates = Vec::with_capacity(max_candidates);
    inner.scan_prefix_reverse(
        StorageTable::DisplacedBlock,
        &prefix,
        &mut |key_bytes, _| {
            let index_key = StoreKey::from_raw_bytes(key_bytes);
            let Some((event_sequence, block_id)) =
                StoreKey::displaced_root_index_position(key_bytes)
            else {
                return Err(StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::DisplacedBlock,
                    key: index_key.into(),
                    reason: "displaced root index key is malformed",
                });
            };
            let order_key =
                StoreKey::displaced_block_by_order(network, event_sequence, block_id.height);
            let Some(record_bytes) = inner.get(StorageTable::DisplacedBlock, &order_key)? else {
                return Err(StoreError::ArtifactMissing {
                    family: ArtifactFamily::DisplacedBlock,
                    key: order_key.into(),
                });
            };
            let block = decode_displaced_block(&order_key, &record_bytes)?;
            let indexed_root = block
                .final_note_commitment_roots
                .and_then(|roots| root_for_protocol(roots, protocol));
            if block.block_hash != block_id.hash
                || block.header.height != block_id.height
                || block.displacement_event_sequence != event_sequence
                || indexed_root != Some(root)
            {
                return Err(StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::DisplacedBlock,
                    key: index_key.into(),
                    reason: "displaced root index does not match its archive row",
                });
            }
            candidates.push(DisplacedRootCandidate {
                block_id,
                protocol,
                root,
                displacement_event_sequence: event_sequence,
                displacement_epoch: block.displacement_epoch,
                block_time_unix_seconds: block.header.block_time,
            });
            Ok(if candidates.len() >= max_candidates {
                PrefixScanControl::Stop
            } else {
                PrefixScanControl::Continue
            })
        },
    )?;
    Ok(candidates)
}

pub(crate) fn read_displaced_root_archive_coverage(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<DisplacedRootArchiveCoverage>, StoreError> {
    let key = StoreKey::displaced_root_archive_coverage();
    inner
        .get(StorageTable::StorageControl, &key)?
        .map(|record_bytes| decode_displaced_root_archive_coverage(&key, &record_bytes))
        .transpose()
}

fn present_roots(
    roots: zinder_core::BlockFinalNoteCommitmentRoots,
) -> impl Iterator<Item = (ShieldedProtocol, FinalNoteCommitmentRoot)> {
    [
        (ShieldedProtocol::Sapling, roots.sapling),
        (ShieldedProtocol::Orchard, roots.orchard),
        (ShieldedProtocol::Ironwood, roots.ironwood),
    ]
    .into_iter()
    .filter_map(|(protocol, root)| root.map(|root| (protocol, root)))
}

fn root_for_protocol(
    roots: zinder_core::BlockFinalNoteCommitmentRoots,
    protocol: ShieldedProtocol,
) -> Option<FinalNoteCommitmentRoot> {
    match protocol {
        ShieldedProtocol::Sapling => roots.sapling,
        ShieldedProtocol::Orchard => roots.orchard,
        ShieldedProtocol::Ironwood => roots.ironwood,
        _ => None,
    }
}

const DISPLACED_ROOT_COVERAGE_FORMAT_VERSION: u8 = 1;
const DISPLACED_ROOT_COVERAGE_FIELD_LEN: usize = size_of::<u64>();
const DISPLACED_ROOT_COVERAGE_LEN: usize = 1 + (5 * DISPLACED_ROOT_COVERAGE_FIELD_LEN);

fn encode_displaced_root_archive_coverage(coverage: DisplacedRootArchiveCoverage) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(DISPLACED_ROOT_COVERAGE_LEN);
    encoded.push(DISPLACED_ROOT_COVERAGE_FORMAT_VERSION);
    encoded.extend_from_slice(&coverage.activation_event_sequence.to_be_bytes());
    encoded.extend_from_slice(&coverage.activation_epoch.value().to_be_bytes());
    encoded.extend_from_slice(&coverage.activated_at.value().to_be_bytes());
    encoded.extend_from_slice(&coverage.captured_block_count.to_be_bytes());
    encoded.extend_from_slice(&coverage.root_artifact_unavailable_count.to_be_bytes());
    encoded
}

fn decode_displaced_root_archive_coverage(
    key: &StoreKey,
    encoded: &[u8],
) -> Result<DisplacedRootArchiveCoverage, StoreError> {
    if encoded.len() != DISPLACED_ROOT_COVERAGE_LEN
        || encoded.first().copied() != Some(DISPLACED_ROOT_COVERAGE_FORMAT_VERSION)
    {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::DisplacedBlock,
            key: key.clone().into(),
            reason: "displaced root archive coverage record is malformed",
        });
    }
    let mut fields = encoded[1..].chunks_exact(DISPLACED_ROOT_COVERAGE_FIELD_LEN);
    let mut next_u64 = || {
        fields
            .next()
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or(StoreError::ArtifactCorrupt {
                family: ArtifactFamily::DisplacedBlock,
                key: key.clone().into(),
                reason: "displaced root archive coverage record is malformed",
            })
    };
    Ok(DisplacedRootArchiveCoverage {
        activation_event_sequence: next_u64()?,
        activation_epoch: zinder_core::ChainEpochId::new(next_u64()?),
        activated_at: zinder_core::UnixTimestampMillis::new(next_u64()?),
        captured_block_count: next_u64()?,
        root_artifact_unavailable_count: next_u64()?,
    })
}
