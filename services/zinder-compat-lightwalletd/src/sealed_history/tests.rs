use super::*;
use std::{
    fs,
    io::{Seek, SeekFrom},
};
use zinder_core::CompactChainMetadata;

const NETWORK: Network = Network::ZcashRegtest;

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

fn range(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}

fn publish(path: &Path, start: u32, end: u32) -> eyre::Result<SegmentIdentity> {
    let staged = NamedTempFile::new_in(parent_directory(path))?;
    let mut writer = SegmentWriter::create(path, staged, NETWORK, id(start).height, id(end))?;
    for height in start..=end {
        let block = block(height);
        let native_bytes = zinder_store::encode_compact_block_artifact(&block).len();
        writer.push(block, native_bytes)?;
    }
    Ok(writer.finish()?)
}

#[test]
fn reopened_segment_uses_native_row_budgets_in_each_direction() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 4)?;
    assert_eq!(
        identity.sha256,
        <[u8; 32]>::from(Sha256::digest(fs::read(&path)?))
    );
    let history = SealedHistory::open(&path, identity)?;
    let native_bytes = zinder_store::encode_compact_block_artifact(&block(1)).len();
    for (order, expected) in [
        (HeightOrder::Ascending, [1, 2]),
        (HeightOrder::Descending, [3, 4]),
    ] {
        let read = history.read_chunk(range(1, 4), order, native_bytes * 3 - 1)?;
        assert_eq!(read.len(), expected.len());
        for (row, height) in read.iter().zip(expected) {
            assert_eq!(row.id, id(height));
            assert_eq!(row.parent, hash(height - 1));
            assert_eq!(row.native_bytes, native_bytes);
            let expected_block = CompactBlockShape::for_range(&[])?.message(block(height))?;
            assert_eq!(
                row.payload.as_ref(),
                expected_block.encode_to_vec().as_slice()
            );
            assert_eq!(
                lightwalletd::CompactBlock::decode(row.payload.clone())?,
                expected_block
            );
        }
    }
    let oversized = history.read_chunk(range(3, 4), HeightOrder::Descending, 0)?;
    assert_eq!(oversized.len(), 1);
    assert_eq!(oversized[0].id, id(4));
    assert!(
        history
            .read_chunk(range(4, 5), HeightOrder::Descending, 1000)
            .is_err()
    );
    assert!(
        history
            .read_chunk(range(4, 1), HeightOrder::Descending, 1000)
            .is_err()
    );
    Ok(())
}

#[test]
fn unfinished_or_competing_writers_never_replace_published_bytes() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SegmentWriter::create(&path, staged, NETWORK, id(1).height, id(2))?;
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
    damaged[HEADER_LEN + 4..HEADER_LEN + LENGTHS_LEN].copy_from_slice(&u32::MAX.to_be_bytes());
    fs::write(&path, &damaged)?;
    assert!(SealedHistory::open(&path, rehash(&damaged)).is_err());
    Ok(())
}

#[test]
fn admitted_reader_checks_copied_bytes_and_keeps_its_original_file() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let opened = SealedHistory::open(&path, identity)?;
    let original = dir.path().join("old-segment");
    fs::rename(&path, &original)?;
    fs::write(&path, b"different file")?;
    assert!(SealedHistory::open(&path, identity).is_err());
    let heights = range(1, 3);
    let rows = opened.read_chunk(heights, HeightOrder::Ascending, usize::MAX)?;
    assert_eq!(rows.len(), 3);
    let mut file = fs::OpenOptions::new().write(true).open(&original)?;
    file.seek(SeekFrom::Start(
        opened.rows[2].offset + opened.rows[2].len as u64 - 1,
    ))?;
    let last_byte = rows[2]
        .payload
        .last()
        .ok_or_else(|| invalid("empty test payload"))?;
    file.write_all(&[last_byte ^ 0xff])?;
    assert!(
        opened
            .read_chunk(heights, HeightOrder::Ascending, usize::MAX)
            .is_err()
    );
    assert!(
        opened
            .bounded_rows(range(3, 3), HeightOrder::Descending, 1, false)?
            .is_empty()
    );
    assert!(SealedHistory::open(&original, identity).is_err());
    assert!(
        opened
            .read_chunk(range(1, 2), HeightOrder::Descending, usize::MAX)
            .is_ok()
    );
    Ok(())
}

fn rewrite_last_frame(
    path: &Path,
    rewrite: impl FnOnce(&mut [u8], &mut [u8]) -> eyre::Result<()>,
) -> eyre::Result<SegmentIdentity> {
    let identity = publish(path, 1, 2)?;
    let mut bytes = fs::read(path)?;
    let trailer = bytes.len() - 2 * INDEX_LEN;
    let (frames, index) = bytes.split_at_mut(trailer);
    let entry = &mut index[INDEX_LEN..];
    let offset = usize::try_from(u64::from_be_bytes(entry[..8].try_into()?))?;
    rewrite(&mut frames[offset - LENGTHS_LEN..], entry)?;
    fs::write(path, &bytes)?;
    let mut measurement = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    measurement.write_all(&bytes)?;
    Ok(SegmentIdentity {
        sha256: Sha256::digest(&bytes).into(),
        verity: Some(measurement.finalize().as_ref().try_into()?),
        ..identity
    })
}

fn assert_second_payload_fails(history: &SealedHistory, reason: &str) -> eyre::Result<()> {
    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        let unaffected = history.read_chunk(range(1, 1), order, usize::MAX)?;
        assert_eq!(unaffected[0].id, id(1));
        let error = history
            .read_chunk(range(1, 2), order, usize::MAX)
            .err()
            .ok_or_else(|| invalid("served an invalid signed payload"))?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), reason);
    }
    Ok(())
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
        let identity = rewrite_last_frame(&path, |frame, index| {
            let payload = &mut frame[LENGTHS_LEN..];
            let mut message = lightwalletd::CompactBlock::decode(&*payload)?;
            message.height = height;
            message.hash = encode_internal_block_hash(block_hash).to_vec();
            message.prev_hash = encode_internal_block_hash(parent).to_vec();
            payload.copy_from_slice(&message.encode_to_vec());
            index[80..].copy_from_slice(&Sha256::digest(payload));
            Ok(())
        })?;
        assert_second_payload_fails(
            &SealedHistory::open(&path, identity)?,
            &PayloadError::Identity.to_string(),
        )?;
    }
    Ok(())
}

fn malformed_metadata(frame: &mut [u8], index: &mut [u8]) -> eyre::Result<()> {
    let payload = &mut frame[LENGTHS_LEN..];
    let message = lightwalletd::CompactBlock::decode(&*payload)?;
    let metadata = message
        .chain_metadata
        .ok_or_else(|| invalid("missing fixture metadata"))?;
    assert!(payload.ends_with(&metadata.encode_to_vec()));
    // The final varint in the nested metadata must terminate.
    *payload.last_mut().ok_or_else(|| invalid("empty segment"))? |= 0x80;
    index[80..].copy_from_slice(&Sha256::digest(payload));
    Ok(())
}

#[test]
fn malformed_nested_protobuf_with_matching_digests_fails_on_read() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = rewrite_last_frame(&path, malformed_metadata)?;
    assert_second_payload_fails(
        &SealedHistory::open(&path, identity)?,
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
    for protected in [false, true] {
        let mut meter = Meter {
            file: io::Cursor::new(bytes.clone()),
            bytes: 0,
            reads: 0,
            seeks: Vec::new(),
        };
        let rows = index::admit(&mut meter, identity, len as u64, protected)?;
        assert_eq!(rows.len(), 7);
        if protected {
            assert_eq!(meter.reads, 2);
            assert_eq!(meter.bytes, HEADER_LEN + 7 * INDEX_LEN);
            assert_eq!(meter.seeks, [(len - 7 * INDEX_LEN) as u64]);
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
fn signed_index_must_describe_the_exact_frames_and_links() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("segment");
    let identity = publish(&path, 1, 3)?;
    let original = fs::read(&path)?;
    let trailer = original.len() - 3 * INDEX_LEN;
    for byte in [0, 8, 15, 16, INDEX_LEN + 48, 80] {
        let mut changed = original.clone();
        changed[trailer + byte] ^= 1;
        let expected = SegmentIdentity {
            sha256: Sha256::digest(&changed).into(),
            ..identity
        };
        fs::write(&path, &changed)?;
        assert!(SealedHistory::open(&path, expected).is_err());
    }
    Ok(())
}

#[test]
fn a_single_row_at_the_highest_height_round_trips() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, u32::MAX, u32::MAX)?;
    let history = SealedHistory::open(&path, identity)?;
    let rows = history.read_chunk(range(u32::MAX, u32::MAX), HeightOrder::Descending, 0)?;
    assert_eq!(rows[0].id, id(u32::MAX));
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
    let first = sealed.read_chunk(range(1, 3), HeightOrder::Descending, usize::MAX)?;
    let copy = dir.path().join("copy");
    fs::copy(&path, &copy)?;
    let mut permissions = fs::metadata(&copy)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&copy, permissions)?;
    let portable = SealedHistory::open(&copy, identity)?;
    assert!(!portable.kernel_verified);
    let second = portable.read_chunk(range(1, 3), HeightOrder::Descending, usize::MAX)?;
    assert!(
        first
            .iter()
            .map(|row| &row.payload)
            .eq(second.iter().map(|row| &row.payload))
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

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_payload_must_match_its_index_before_serving() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    for (field, reason) in [
        ("protobuf", PayloadError::Protobuf.to_string()),
        (
            "length",
            "shielded segment frame lengths differ from its index".into(),
        ),
        ("digest", PayloadError::Digest.to_string()),
    ] {
        let path = dir.path().join(field);
        let identity = rewrite_last_frame(&path, |frame, index| match field {
            "protobuf" => malformed_metadata(frame, index),
            "length" => {
                frame[3] ^= 1;
                Ok(())
            }
            _ => {
                index[80] ^= 1;
                Ok(())
            }
        })?;
        enable_verity(&path)?;
        let history = SealedHistory::open(&path, identity)?;
        assert!(history.kernel_verified);
        assert_second_payload_fails(&history, &reason)?;
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
    let staged = NamedTempFile::new_in(parent_directory(&path))?;
    let mut writer = SegmentWriter::create(&path, staged, NETWORK, id(1).height, id(TOTAL_BLOCKS))?;
    for height in 1..=TOTAL_BLOCKS {
        let dense = dense_block(height)?;
        let native_bytes = zinder_store::encode_compact_block_artifact(&dense).len();
        writer.push(dense, native_bytes)?;
    }
    let identity = writer.finish()?;
    let history = SealedHistory::open(&path, identity)?;
    let mut rows =
        history.read_chunk(range(1, TOTAL_BLOCKS), HeightOrder::Ascending, usize::MAX)?;
    assert_eq!(rows.len(), TOTAL_BLOCKS as usize);
    let total_payload_bytes: usize = rows.iter().map(|row| row.payload.len()).sum();
    assert!(
        total_payload_bytes > SLAB_TARGET_BYTES,
        "test fixture must exceed 64 KiB slab target (got {total_payload_bytes} bytes)"
    );

    let mut slab_one_count = 0_usize;
    let mut slab_bytes = 0_usize;
    for row in &rows {
        if slab_one_count > 0 && slab_bytes + row.payload.len() > SLAB_TARGET_BYTES {
            break;
        }
        assert_eq!(row.id, id(u32::try_from(slab_one_count)? + 1));
        slab_bytes += row.payload.len();
        slab_one_count += 1;
    }
    assert!(slab_one_count > 0 && slab_one_count < rows.len());

    let later_slabs = rows.split_off(slab_one_count);
    let probe = rows
        .pop()
        .ok_or_else(|| eyre::eyre!("expected non-empty first slab"))?;
    assert!(!probe.payload.is_unique());
    drop(rows);
    assert!(
        probe.payload.is_unique(),
        "first slab must release independently of later slabs"
    );
    assert_eq!(later_slabs[0].id, id(u32::try_from(slab_one_count)? + 1));
    Ok(())
}
