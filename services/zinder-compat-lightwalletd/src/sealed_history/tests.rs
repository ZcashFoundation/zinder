use super::*;
use std::{
    fs,
    io::{Seek, SeekFrom},
};
use zinder_core::CompactChainMetadata;

const NETWORK: Network = Network::ZcashRegtest;
const ARTIFACT_SCHEMA: u16 = 1;

fn hash(height: u32) -> BlockHash {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_bytes(bytes)
}

fn id(height: u32) -> BlockId {
    BlockId::new(BlockHeight::new(height), hash(height))
}

fn block(height: u32) -> CompactBlockArtifact {
    CompactBlockArtifact::empty(
        id(height),
        hash(height - 1),
        height,
        CompactChainMetadata {
            sapling_commitment_tree_size: height,
            orchard_commitment_tree_size: height,
            ironwood_commitment_tree_size: height,
        },
    )
}

fn staged_files(directory: &Path) -> eyre::Result<(NamedTempFile, NamedTempFile)> {
    Ok((
        NamedTempFile::new_in(directory)?,
        NamedTempFile::new_in(directory)?,
    ))
}

fn range(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}

fn expected_message(slab: SealedSlab, height: u32) -> eyre::Result<Vec<u8>> {
    expected_message_for(slab, &block(height))
}

fn expected_message_for(slab: SealedSlab, block: &CompactBlockArtifact) -> eyre::Result<Vec<u8>> {
    let shape = match slab {
        SealedSlab::Shielded => CompactBlockShape::DEFAULT_RANGE,
        SealedSlab::Nullifier => CompactBlockShape::DEFAULT_NULLIFIER_RANGE,
    };
    Ok(shape.message(block.clone())?.encode_to_vec())
}

fn publish(path: &Path, start: u32, end: u32) -> eyre::Result<SegmentIdentity> {
    let mut writer = SegmentWriter::create(
        path,
        staged_files(parent_directory(path))?,
        SegmentIdentity::span(NETWORK, ARTIFACT_SCHEMA, id(start).height, id(end)),
    )?;
    for height in start..=end {
        let block = block(height);
        let native_bytes = zinder_store::encode_compact_block_artifact(&block).len();
        writer.push(block, native_bytes)?;
    }
    Ok(writer.finish()?)
}

/// The file's trailer bytes: the whole-body digest the identity carries.
fn trailer_of(path: &Path) -> eyre::Result<([u8; 32], Vec<u8>)> {
    let bytes = fs::read(path)?;
    let split = bytes.len() - TRAILER_LEN;
    let trailer: [u8; 32] = bytes[split..]
        .try_into()
        .map_err(|_| invalid("short test segment"))?;
    Ok((trailer, bytes[..split].to_vec()))
}

fn index_offset_of(bytes: &[u8]) -> eyre::Result<usize> {
    let raw = u64::from_be_bytes(
        bytes
            .get(104..112)
            .and_then(|raw| raw.try_into().ok())
            .ok_or_else(|| invalid("short test header"))?,
    );
    Ok(usize::try_from(raw).map_err(|_| invalid("test index offset exceeds address space"))?)
}

#[test]
fn both_slabs_round_trip_each_projection_in_each_direction() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 4)?;
    let (trailer, body) = trailer_of(&path)?;
    assert_eq!(<[u8; 32]>::from(Sha256::digest(&body)), trailer);
    assert_eq!(identity.sha256, trailer);
    let history = SealedHistory::open(&path, identity)?;
    let native_bytes = zinder_store::encode_compact_block_artifact(&block(1)).len();
    for slab in [SealedSlab::Shielded, SealedSlab::Nullifier] {
        for (order, expected) in [
            (HeightOrder::Ascending, [1, 2]),
            (HeightOrder::Descending, [3, 4]),
        ] {
            let read = history.read_chunk(slab, range(1, 4), order, native_bytes * 3 - 1)?;
            assert_eq!(read.len(), expected.len());
            for (row, height) in read.iter().zip(expected) {
                assert_eq!(row.id, id(height));
                assert_eq!(row.parent, hash(height - 1));
                assert_eq!(row.native_bytes, native_bytes);
                let expected_block = expected_message(slab, height)?;
                assert_eq!(row.message.payload().as_ref(), expected_block.as_slice());
                assert_eq!(
                    lightwalletd::CompactBlock::decode(row.message.payload())?,
                    lightwalletd::CompactBlock::decode(expected_block.as_slice())?
                );
            }
        }
        let oversized = history.read_chunk(slab, range(3, 4), HeightOrder::Descending, 0)?;
        assert_eq!(oversized.len(), 1);
        assert_eq!(oversized[0].id, id(4));
        assert!(
            history
                .read_chunk(slab, range(4, 5), HeightOrder::Descending, 1000)
                .is_err()
        );
        assert!(
            history
                .read_chunk(slab, range(4, 1), HeightOrder::Descending, 1000)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn unfinished_or_competing_writers_never_replace_published_bytes() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let mut writer = SegmentWriter::create(
        &path,
        staged_files(dir.path())?,
        SegmentIdentity::span(NETWORK, ARTIFACT_SCHEMA, id(1).height, id(2)),
    )?;
    writer.push(block(1), 300)?;
    assert!(!path.exists());
    assert!(writer.finish().is_err());
    assert!(!path.exists());
    let first = publish(&path, 1, 3)?;
    assert_eq!(publish(&path, 1, 3)?, first);
    assert!(publish(&path, 2, 3).is_err());
    assert_eq!(SealedHistory::open(&path, first)?.identity(), first);
    Ok(())
}

#[test]
fn wrong_identity_truncation_and_trailing_bytes_fail_admission() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    for wrong in [
        SegmentIdentity {
            network: Network::ZcashTestnet,
            ..identity
        },
        SegmentIdentity {
            artifact_schema: ARTIFACT_SCHEMA + 1,
            ..identity
        },
        SegmentIdentity {
            end: BlockId::new(id(3).height, hash(4)),
            ..identity
        },
        SegmentIdentity {
            start: id(2).height,
            ..identity
        },
        SegmentIdentity {
            sha256: [0; 32],
            ..identity
        },
    ] {
        assert!(SealedHistory::open(&path, wrong).is_err());
    }
    let original = fs::read(&path)?;
    let rehash = |bytes: &[u8]| SegmentIdentity {
        sha256: Sha256::digest(bytes).into(),
        ..identity
    };
    for end in [HEADER_LEN - 1, HEADER_LEN + 3, original.len() - 1] {
        fs::write(&path, &original[..end])?;
        assert!(SealedHistory::open(&path, rehash(&original[..end])).is_err());
    }
    let mut damaged = original;
    damaged.push(0);
    fs::write(&path, &damaged)?;
    assert!(SealedHistory::open(&path, rehash(&damaged)).is_err());
    damaged.pop();
    let index = index_offset_of(&damaged)?;
    damaged[index + 4..index + LENGTHS_LEN].copy_from_slice(&u32::MAX.to_be_bytes());
    fs::write(&path, &damaged)?;
    assert!(SealedHistory::open(&path, rehash(&damaged)).is_err());
    Ok(())
}

#[test]
fn admitted_reader_checks_copied_bytes_per_slab_and_keeps_its_original_file() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let opened = SealedHistory::open(&path, identity)?;
    let original = dir.path().join("old-segment");
    fs::rename(&path, &original)?;
    fs::write(&path, b"different file")?;
    assert!(SealedHistory::open(&path, identity).is_err());
    let heights = range(1, 3);
    let rows = opened.read_chunk(
        SealedSlab::Shielded,
        heights,
        HeightOrder::Ascending,
        usize::MAX,
    )?;
    assert_eq!(rows.len(), 3);
    let shielded_rel = &opened.shielded_rel;
    let last_frame_len = frame_len(shielded_rel, 2)?;
    let payload_end = HEADER_LEN as u64 + u64::from(shielded_rel[2]) + last_frame_len as u64 - 1;
    let mut file = fs::OpenOptions::new().write(true).open(&original)?;
    file.seek(SeekFrom::Start(payload_end - 1))?;
    let last_byte = rows[2]
        .message
        .as_length_prefixed()
        .last()
        .ok_or_else(|| invalid("empty test payload"))?;
    file.write_all(&[last_byte ^ 0xff])?;
    // The shielded slab's damaged row fails its read; the nullifier slab,
    // whose bytes are untouched, still serves it.
    assert!(
        opened
            .read_chunk(
                SealedSlab::Shielded,
                heights,
                HeightOrder::Ascending,
                usize::MAX
            )
            .is_err()
    );
    let intact = opened.read_chunk(
        SealedSlab::Nullifier,
        heights,
        HeightOrder::Ascending,
        usize::MAX,
    )?;
    assert_eq!(intact.len(), 3);
    assert!(
        opened
            .bounded_rows(
                SealedSlab::Shielded,
                range(3, 3),
                HeightOrder::Descending,
                1,
                false
            )?
            .is_empty()
    );
    assert!(SealedHistory::open(&original, identity).is_err());
    assert!(
        opened
            .read_chunk(
                SealedSlab::Nullifier,
                range(1, 2),
                HeightOrder::Descending,
                usize::MAX
            )
            .is_ok()
    );
    Ok(())
}

/// Rewrites the last row's shielded payload with `rewrite`, repairs its
/// metadata digest, and re-derives the trailer, so only the payload's own
/// consistency can fail.
fn rewrite_last_frame(
    path: &Path,
    rewrite: impl FnOnce(&mut [u8], &mut [u8]) -> eyre::Result<()>,
) -> eyre::Result<SegmentIdentity> {
    const COUNT: usize = 2;
    let identity = publish(path, 1, 2)?;
    let mut bytes = fs::read(path)?;
    let index = index_offset_of(&bytes)?;
    let tables = 2 * (COUNT + 1) * 4;
    // The shielded table's entries for the last row: its frame start and end.
    let rel_at = |entry: usize| -> eyre::Result<usize> {
        Ok(usize::try_from(u32::from_be_bytes(
            bytes
                .get(index + entry * 4..index + entry * 4 + 4)
                .and_then(|raw| raw.try_into().ok())
                .ok_or_else(|| invalid("short test index"))?,
        ))
        .map_err(|_| invalid("offset exceeds address space"))?)
    };
    let offset = HEADER_LEN + rel_at(COUNT - 1)?;
    let frame_end = HEADER_LEN + rel_at(COUNT)?;
    let row_meta = index + tables + ROW_META_LEN;
    let (before_meta, after_meta) = bytes.split_at_mut(row_meta);
    let payload = &mut before_meta[offset + LENGTHS_LEN..frame_end];
    let metadata = &mut after_meta[..ROW_META_LEN];
    rewrite(payload, metadata)?;
    metadata[72..104].copy_from_slice(&Sha256::digest(payload));
    let trailer: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - TRAILER_LEN]).into();
    let split = bytes.len() - TRAILER_LEN;
    bytes[split..].copy_from_slice(&trailer);
    fs::write(path, &bytes)?;
    let mut measurement = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    measurement.write_all(&bytes)?;
    Ok(SegmentIdentity {
        sha256: trailer,
        verity: Some(
            measurement
                .finalize()
                .as_ref()
                .try_into()
                .map_err(|_| invalid("invalid fs-verity digest length"))?,
        ),
        ..identity
    })
}

fn assert_second_payload_fails(
    history: &SealedHistory,
    slab: SealedSlab,
    reason: &str,
) -> eyre::Result<()> {
    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        let unaffected = history.read_chunk(other(slab), range(1, 1), order, usize::MAX)?;
        assert_eq!(unaffected[0].id, id(1));
        let error = history
            .read_chunk(slab, range(1, 2), order, usize::MAX)
            .err()
            .ok_or_else(|| invalid("served an invalid signed payload"))?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), reason);
    }
    Ok(())
}

fn other(slab: SealedSlab) -> SealedSlab {
    match slab {
        SealedSlab::Shielded => SealedSlab::Nullifier,
        SealedSlab::Nullifier => SealedSlab::Shielded,
    }
}

#[test]
fn authenticated_payload_identity_must_match_its_index() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    for (field, height, block_hash, parent) in [
        ("height", 1, hash(2), hash(1)),
        ("hash", 2, hash(3), hash(1)),
        ("parent", 2, hash(2), hash(0)),
    ] {
        let path = dir.path().join(field);
        let identity = rewrite_last_frame(&path, |payload, _metadata| {
            let mut message = lightwalletd::CompactBlock::decode(&*payload)?;
            message.height = height;
            message.hash = encode_internal_block_hash(block_hash).to_vec();
            message.prev_hash = encode_internal_block_hash(parent).to_vec();
            payload.copy_from_slice(&message.encode_to_vec());
            Ok(())
        })?;
        assert_second_payload_fails(
            &SealedHistory::open(&path, identity)?,
            SealedSlab::Shielded,
            &PayloadError::Identity.to_string(),
        )?;
    }
    Ok(())
}

fn malformed_metadata(payload: &mut [u8], metadata: &mut [u8]) -> eyre::Result<()> {
    let message = lightwalletd::CompactBlock::decode(&*payload)?;
    let chain = message
        .chain_metadata
        .ok_or_else(|| invalid("missing fixture metadata"))?;
    assert!(payload.ends_with(&chain.encode_to_vec()));
    // The final varint in the nested metadata must terminate.
    *payload.last_mut().ok_or_else(|| invalid("empty segment"))? |= 0x80;
    metadata[72..104].copy_from_slice(&Sha256::digest(payload));
    Ok(())
}

#[test]
fn malformed_nested_protobuf_with_matching_digests_fails_on_read() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = rewrite_last_frame(&path, malformed_metadata)?;
    assert_second_payload_fails(
        &SealedHistory::open(&path, identity)?,
        SealedSlab::Shielded,
        &PayloadError::Protobuf.to_string(),
    )
}

#[test]
fn admission_reads_each_portable_byte_once_and_skips_protected_payloads() -> eyre::Result<()> {
    struct Meter {
        file: io::Cursor<Vec<u8>>,
        bytes: usize,
        reads: usize,
        seeks: Vec<u64>,
    }
    impl Read for Meter {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            let count = self.file.read(buffer)?;
            self.bytes += count;
            Ok(count)
        }
    }
    impl Seek for Meter {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            let offset = self.file.seek(position)?;
            self.seeks.push(offset);
            Ok(offset)
        }
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = publish(&path, 1, 7)?;
    let bytes = fs::read(&path)?;
    let len = bytes.len();
    let index = index_offset_of(&bytes)?;
    let tables = 2 * (7 + 1) * 4;
    for protected in [false, true] {
        let mut meter = Meter {
            file: io::Cursor::new(bytes.clone()),
            bytes: 0,
            reads: 0,
            seeks: Vec::new(),
        };
        let rows = index::admit(&mut meter, identity, len as u64, protected)?;
        assert_eq!(rows.rows.len(), 7);
        if protected {
            assert_eq!(meter.bytes, HEADER_LEN + tables + 7 * ROW_META_LEN);
            assert_eq!(meter.seeks, [index as u64]);
        } else {
            assert_eq!(meter.bytes, len);
            assert_eq!(meter.seeks, [] as [u64; 0]);
        }
    }
    assert!(index::pinned_verity(identity.verity, || Ok(
        identity.verity
    ))?);
    assert!(index::pinned_verity(identity.verity, || Ok(Some([0; 32]))).is_err());
    assert!(!index::pinned_verity(None, || Err(invalid(
        "unnecessary kernel measurement"
    )))?);
    assert!(!index::pinned_verity(identity.verity, || Ok(None))?);
    Ok(())
}

#[test]
fn signed_index_and_header_must_describe_the_exact_frames_and_links() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = publish(&path, 1, 3)?;
    let original = fs::read(&path)?;
    let index = index_offset_of(&original)?;
    let tables = 2 * (3 + 1) * 4;
    // Header anchors, offset tables, and metadata rows all fail admission
    // with a digest that matches the changed bytes, so only their own
    // cross-checks can catch the damage.
    for byte in [
        0,
        8,
        12,
        14,
        16,
        24,
        56,
        88,
        96,
        104,
        112,
        index + 4,
        index + 8,
        index + tables,
        index + tables + 32,
        index + tables + 64,
        index + tables + 72,
        index + tables + 104,
    ] {
        let mut changed = original.clone();
        changed[byte] ^= 1;
        let expected = reseal_body(&path, identity, changed)?;
        assert!(
            SealedHistory::open(&path, expected).is_err(),
            "byte {byte} must fail admission"
        );
    }
    Ok(())
}

/// Admits `bytes` as one portable and one kernel-protected segment.
fn admission_errors(identity: SegmentIdentity, bytes: &[u8]) -> [Option<String>; 2] {
    let mut errors = [None, None];
    for (slot, protected) in errors.iter_mut().zip([false, true]) {
        let mut input = io::Cursor::new(bytes);
        *slot = index::admit(&mut input, identity, bytes.len() as u64, protected)
            .err()
            .map(|error| error.to_string());
    }
    errors
}

#[test]
fn resealed_offset_tables_must_start_ascend_and_end_at_their_slabs() -> eyre::Result<()> {
    const COUNT: usize = 3;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = publish(&path, 1, u32::try_from(COUNT)?)?;
    let original = fs::read(&path)?;
    let index = index_offset_of(&original)?;
    let entry = |table: usize, at: usize| index + (table * (COUNT + 1) + at) * 4;
    let read_entry = |bytes: &[u8], table, at| -> eyre::Result<u32> {
        Ok(u32::from_be_bytes(
            bytes[entry(table, at)..entry(table, at) + 4].try_into()?,
        ))
    };

    // The unchanged file admits under both modes.
    assert_eq!(admission_errors(identity, &original), [None, None]);

    for (table, name) in [(0, "shielded"), (1, "nullifier")] {
        let last = read_entry(&original, table, COUNT)?;
        for (label, at, value, reason) in [
            (
                "nonzero origin",
                0,
                1,
                "shielded segment offset table does not start at its slab",
            ),
            (
                "descending pair",
                1,
                read_entry(&original, table, 2)? + 1,
                "shielded segment offset table spans are not ascending",
            ),
            (
                "repeated offset",
                2,
                read_entry(&original, table, 1)?,
                "shielded segment offset table spans are not ascending",
            ),
            (
                "terminal beyond the slab",
                COUNT,
                last + 1,
                "shielded segment offset table ends away from its slab",
            ),
            (
                "terminal short of the slab",
                COUNT,
                last - 1,
                "shielded segment offset table ends away from its slab",
            ),
        ] {
            let mut changed = original.clone();
            changed[entry(table, at)..entry(table, at) + 4].copy_from_slice(&value.to_be_bytes());
            let resealed = reseal_body(&path, identity, changed.clone())?;
            // Resealing changes the trailer, so read back the pinned file.
            let changed = fs::read(&path)?;
            assert_eq!(
                admission_errors(resealed, &changed),
                [Some(reason.to_owned()), Some(reason.to_owned())],
                "{name} table, {label}"
            );
        }
    }
    Ok(())
}

#[test]
fn a_resealed_metadata_row_that_breaks_the_chain_fails_before_the_trailer() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = publish(&path, 1, 3)?;
    let original = fs::read(&path)?;
    let index = index_offset_of(&original)?;
    let tables = 2 * (3 + 1) * 4;
    // Row 1's parent hash no longer names row 0, and the trailer, pin, and
    // fs-verity measurement are all re-derived over the changed body.
    let resealed = reseal(
        &path,
        identity,
        original,
        index + tables + ROW_META_LEN + 32,
    )?;
    let changed = fs::read(&path)?;
    let errors = admission_errors(resealed, &changed);
    let [Some(portable), Some(protected)] = errors else {
        eyre::bail!("a broken chain link was admitted");
    };
    assert_eq!(portable, protected);
    assert!(
        !portable.contains("SHA-256 mismatch"),
        "the structural check must run, not the stale digest: {portable}"
    );
    Ok(())
}

#[test]
fn a_single_row_at_the_highest_height_round_trips() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, u32::MAX, u32::MAX)?;
    let history = SealedHistory::open(&path, identity)?;
    for slab in [SealedSlab::Shielded, SealedSlab::Nullifier] {
        let rows =
            history.read_chunk(slab, range(u32::MAX, u32::MAX), HeightOrder::Descending, 0)?;
        assert_eq!(rows[0].id, id(u32::MAX));
        assert_eq!(
            rows[0].message.payload().as_ref(),
            expected_message(slab, u32::MAX)?.as_slice()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn enable_verity(path: &Path) -> eyre::Result<()> {
    let protection = std::process::Command::new("fsverity")
        .arg("enable")
        .arg(path)
        .output()?;
    eyre::ensure!(
        protection.status.success(),
        "fsverity enable failed: {}",
        String::from_utf8_lossy(&protection.stderr)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_file_is_bound_before_the_digest_scan() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    assert!(!SealedHistory::open(&path, identity)?.kernel_verified);
    enable_verity(&path)?;
    assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
    let sealed = SealedHistory::open(&path, identity)?;
    assert!(sealed.kernel_verified);
    assert!(
        SealedHistory::open(
            &path,
            SegmentIdentity {
                verity: Some([0; 32]),
                ..identity
            }
        )
        .is_err()
    );
    let first = sealed.read_chunk(
        SealedSlab::Shielded,
        range(1, 3),
        HeightOrder::Descending,
        usize::MAX,
    )?;
    let copy = dir.path().join("copy");
    fs::copy(&path, &copy)?;
    let mut permissions = fs::metadata(&copy)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&copy, permissions)?;
    let portable = SealedHistory::open(&copy, identity)?;
    assert!(!portable.kernel_verified);
    let second = portable.read_chunk(
        SealedSlab::Shielded,
        range(1, 3),
        HeightOrder::Descending,
        usize::MAX,
    )?;
    assert!(
        first
            .iter()
            .map(|row| &row.message)
            .eq(second.iter().map(|row| &row.message))
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_segment_with_sha512_rejects_a_signed_sha256_pin() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let status = std::process::Command::new("fsverity")
        .args(["enable", "--hash-alg", "sha512", "--block-size", "4096"])
        .arg(&path)
        .status()?;
    eyre::ensure!(status.success(), "fsverity enable failed: {status}");
    assert_eq!(
        SealedHistory::open(&path, identity)
            .err()
            .map(|error| error.kind()),
        Some(io::ErrorKind::InvalidData)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem"]
fn kernel_protected_publication_requires_a_closed_writer_and_matching_pin() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let reader = File::open(&path)?;
    let writer = fs::OpenOptions::new().write(true).open(&path)?;
    assert_eq!(
        verity::protect_new_segment(&reader, identity.verity)
            .err()
            .map(|error| error.kind()),
        Some(io::ErrorKind::ExecutableFileBusy)
    );
    drop(writer);
    assert_eq!(
        verity::protect_new_segment(&reader, Some([0; 32]))
            .err()
            .map(|error| error.kind()),
        Some(io::ErrorKind::InvalidData)
    );
    assert_eq!(verity::measure(&reader)?, identity.verity);
    Ok(())
}

/// Flips one byte of `bytes`, re-derives the trailer, and returns the
/// identity that pins the changed file.
fn reseal(
    path: &Path,
    identity: SegmentIdentity,
    mut bytes: Vec<u8>,
    at: usize,
) -> eyre::Result<SegmentIdentity> {
    bytes[at] ^= 1;
    reseal_body(path, identity, bytes)
}

/// Re-derives the trailer over the changed body, writes the file, and returns
/// the identity that pins it.
fn reseal_body(
    path: &Path,
    identity: SegmentIdentity,
    mut bytes: Vec<u8>,
) -> eyre::Result<SegmentIdentity> {
    let trailer: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - TRAILER_LEN]).into();
    let split = bytes.len() - TRAILER_LEN;
    bytes[split..].copy_from_slice(&trailer);
    fs::write(path, &bytes)?;
    let mut measurement = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    measurement.write_all(&bytes)?;
    Ok(SegmentIdentity {
        sha256: trailer,
        verity: Some(
            measurement
                .finalize()
                .as_ref()
                .try_into()
                .map_err(|_| invalid("invalid fs-verity digest length"))?,
        ),
        ..identity
    })
}

/// Corrupts one byte of the last row's shielded metadata digest.
#[cfg(target_os = "linux")]
fn tampered_digest(path: &Path) -> eyre::Result<SegmentIdentity> {
    let identity = publish(path, 1, 2)?;
    let bytes = fs::read(path)?;
    let index = index_offset_of(&bytes)?;
    let row_meta = index + 2 * 3 * 4 + ROW_META_LEN;
    reseal(path, identity, bytes, row_meta + 72)
}

/// Corrupts one byte of the last row's shielded lengths prefix.
#[cfg(target_os = "linux")]
fn tampered_lengths(path: &Path) -> eyre::Result<SegmentIdentity> {
    let identity = publish(path, 1, 2)?;
    let bytes = fs::read(path)?;
    let index = index_offset_of(&bytes)?;
    let rel = usize::try_from(u32::from_be_bytes(
        bytes
            .get(index + 4..index + 8)
            .and_then(|raw| raw.try_into().ok())
            .ok_or_else(|| invalid("short test index"))?,
    ))
    .map_err(|_| invalid("offset exceeds address space"))?;
    let frame = HEADER_LEN + rel;
    reseal(path, identity, bytes, frame + 3)
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_payload_must_match_its_index_before_serving() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    for (field, tamper, reason) in [
        ("protobuf", 0, PayloadError::Protobuf.to_string()),
        (
            "length",
            1,
            "shielded segment frame lengths differ from its index".to_owned(),
        ),
        ("digest", 2, PayloadError::Digest.to_string()),
    ] {
        let path = dir.path().join(field);
        let identity = match tamper {
            0 => rewrite_last_frame(&path, |payload, metadata| {
                malformed_metadata(payload, metadata)
            })?,
            1 => tampered_lengths(&path)?,
            _ => tampered_digest(&path)?,
        };
        enable_verity(&path)?;
        let history = SealedHistory::open(&path, identity)?;
        assert!(history.kernel_verified);
        assert_second_payload_fails(&history, SealedSlab::Shielded, &reason)?;
    }
    Ok(())
}

#[test]
fn payload_length_bounds_include_the_exact_limit_and_native_length() {
    assert!(validate_lengths(1, 0).is_err());
    assert!(validate_lengths(1, 2).is_err());
    assert!(validate_lengths(2, 1).is_ok());
    assert!(validate_lengths(MAX_PAYLOAD_BYTES, MAX_PAYLOAD_BYTES).is_ok());
    assert!(validate_lengths(MAX_PAYLOAD_BYTES + 1, MAX_PAYLOAD_BYTES + 1).is_err());
}

#[test]
fn the_terminal_height_and_hash_end_the_segment() {
    let segment = SegmentIdentity {
        network: NETWORK,
        artifact_schema: ARTIFACT_SCHEMA,
        start: id(1).height,
        end: id(2),
        sha256: [0; 32],
        verity: None,
    };
    assert!(segment.check_block(id(1), hash(0), None).is_ok());
    assert!(segment.check_block(id(2), hash(1), Some(id(1))).is_ok());
    assert!(segment.check_block(id(2), hash(1), None).is_err());
    assert!(segment.check_block(id(2), hash(0), Some(id(1))).is_err());
    assert!(
        segment
            .check_block(BlockId::new(id(2).height, hash(3)), hash(1), Some(id(1)))
            .is_err()
    );
    assert!(segment.check_block(id(3), hash(2), Some(id(2))).is_err());
}

fn dense_block(height: u32) -> eyre::Result<CompactBlockArtifact> {
    use zinder_core::{
        CompactSaplingOutput, CompactTransaction, CompactTransactionData, TransactionId,
    };
    let tag = u8::try_from(height)?;
    Ok(CompactBlockArtifact::new(
        id(height),
        hash(height - 1),
        height,
        vec![CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes([tag; 32]),
            data: CompactTransactionData {
                fee_zat: None,
                sapling_spends: Vec::new(),
                sapling_outputs: vec![
                    CompactSaplingOutput {
                        commitment: [1; 32],
                        ephemeral_key: [2; 32],
                        ciphertext: [3; 52],
                    };
                    12
                ],
                orchard_actions: Vec::new(),
                ironwood_actions: Vec::new(),
                transparent_inputs: Vec::new(),
                transparent_outputs: Vec::new(),
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: height * 12,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?)
}

#[test]
fn dense_chunks_split_into_independent_64kib_slabs() -> eyre::Result<()> {
    const TOTAL_BLOCKS: u32 = 100;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded-dense");
    let mut writer = SegmentWriter::create(
        &path,
        staged_files(parent_directory(&path))?,
        SegmentIdentity::span(NETWORK, ARTIFACT_SCHEMA, id(1).height, id(TOTAL_BLOCKS)),
    )?;
    for height in 1..=TOTAL_BLOCKS {
        let dense = dense_block(height)?;
        let native_bytes = zinder_store::encode_compact_block_artifact(&dense).len();
        writer.push(dense, native_bytes)?;
    }
    let identity = writer.finish()?;
    let history = SealedHistory::open(&path, identity)?;
    let mut rows = history.read_chunk(
        SealedSlab::Shielded,
        range(1, TOTAL_BLOCKS),
        HeightOrder::Ascending,
        usize::MAX,
    )?;
    assert_eq!(rows.len(), TOTAL_BLOCKS as usize);
    let total_payload_bytes: usize = rows.iter().map(|row| row.message.payload_len()).sum();
    assert!(
        total_payload_bytes > SLAB_TARGET_BYTES,
        "test fixture must exceed 64 KiB slab target (got {total_payload_bytes} bytes)"
    );

    let mut slab_one_count = 0_usize;
    let mut slab_bytes = 0_usize;
    for row in &rows {
        if slab_one_count > 0
            && slab_bytes + LENGTHS_LEN + row.message.payload_len() > SLAB_TARGET_BYTES
        {
            break;
        }
        assert_eq!(row.id, id(u32::try_from(slab_one_count)? + 1));
        slab_bytes += LENGTHS_LEN + row.message.payload_len();
        slab_one_count += 1;
    }
    assert!(slab_one_count > 0 && slab_one_count < rows.len());

    let later_slabs = rows.split_off(slab_one_count);
    let probe = rows
        .pop()
        .ok_or_else(|| eyre::eyre!("expected non-empty first slab"))?;
    assert!(!probe.message.as_length_prefixed().is_unique());
    drop(rows);
    assert!(
        probe.message.as_length_prefixed().is_unique(),
        "first slab must release independently of later slabs"
    );
    assert_eq!(later_slabs[0].id, id(u32::try_from(slab_one_count)? + 1));
    Ok(())
}

#[test]
fn the_nullifier_slab_stages_on_disk_and_streams_into_the_segment() -> eyre::Result<()> {
    const TOTAL_BLOCKS: u32 = 100;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded-staged");
    let mut writer = SegmentWriter::create(
        &path,
        staged_files(dir.path())?,
        SegmentIdentity::span(NETWORK, ARTIFACT_SCHEMA, id(1).height, id(TOTAL_BLOCKS)),
    )?;
    let spill = writer.nullifiers.get_ref().path().to_owned();
    for height in 1..=TOTAL_BLOCKS {
        let dense = dense_block(height)?;
        let native_bytes = zinder_store::encode_compact_block_artifact(&dense).len();
        writer.push(dense, native_bytes)?;
    }
    // The frames wait in the staging file, not in writer memory.
    writer.nullifiers.flush()?;
    assert_eq!(
        fs::metadata(&spill)?.len(),
        u64::from(
            *writer
                .nullifier_rel
                .last()
                .ok_or_else(|| invalid("missing nullifier offsets"))?
        )
    );
    let identity = writer.finish()?;
    assert!(!spill.exists(), "finish must release the staging file");

    let history = SealedHistory::open(&path, identity)?;
    let rows = history.read_chunk(
        SealedSlab::Nullifier,
        range(1, TOTAL_BLOCKS),
        HeightOrder::Ascending,
        usize::MAX,
    )?;
    assert_eq!(rows.len(), TOTAL_BLOCKS as usize);
    for (row, height) in rows.iter().zip(1..) {
        assert_eq!(
            row.message.payload().to_vec(),
            expected_message_for(SealedSlab::Nullifier, &dense_block(height)?)?
        );
    }
    Ok(())
}

#[test]
fn positional_reads_fill_a_slab_exactly_and_report_a_short_final_frame() -> eyre::Result<()> {
    let mut file = tempfile::tempfile()?;
    file.write_all(b"prefix")?;
    let expected: Vec<_> = (0_u16..1025).flat_map(u16::to_be_bytes).collect();
    file.write_all(&expected)?;
    file.write_all(b"suffix")?;

    assert_eq!(read_slab_at(&file, expected.len(), 6)?, expected);

    file.set_len(6 + u64::try_from(expected.len())? - 1)?;
    let error = read_slab_at(&file, expected.len(), 6)
        .err()
        .ok_or_else(|| invalid("served a truncated frame"))?;
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    Ok(())
}
