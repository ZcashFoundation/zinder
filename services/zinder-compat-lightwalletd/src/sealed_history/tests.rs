use super::*;
use std::{
    fs,
    io::{Seek, SeekFrom},
};
use zinder_core::CompactChainMetadata;
use zinder_proto::compat::lightwalletd;

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
    let mut writer = SegmentWriter::create(path, NETWORK, id(start).height, id(end))?;
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
    let mut writer = SegmentWriter::create(&path, NETWORK, id(1).height, id(2))?;
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
    file.seek(SeekFrom::End(-1))?;
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

#[test]
fn broken_payload_link_is_rejected_even_with_matching_digest() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let history = SealedHistory::open(&path, identity)?;
    let row = &history.rows[1];
    let mut bytes = fs::read(&path)?;
    let start = usize::try_from(row.offset)?;
    let payload = &mut bytes[start..start + row.len];
    let mut message = lightwalletd::CompactBlock::decode(&*payload)?;
    message.prev_hash = encode_internal_block_hash(hash(0)).to_vec();
    payload.copy_from_slice(&message.encode_to_vec());
    fs::write(&path, &bytes)?;
    let altered = SegmentIdentity {
        sha256: Sha256::digest(&bytes).into(),
        ..identity
    };
    assert!(SealedHistory::open(&path, altered).is_err());
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
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_file_is_bound_before_the_digest_scan() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shielded");
    let identity = publish(&path, 1, 3)?;
    let ordinary = SealedHistory::open(&path, identity)?;
    assert!(ordinary.rows.iter().all(|row| row.sha256.is_some()));
    let protection = std::process::Command::new("fsverity")
        .arg("enable")
        .arg(&path)
        .output()?;
    eyre::ensure!(
        protection.status.success(),
        "fsverity enable failed: {}",
        String::from_utf8_lossy(&protection.stderr)
    );
    assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
    let sealed = SealedHistory::open(&path, identity)?;
    assert!(sealed.rows.iter().all(|row| row.sha256.is_none()));
    assert!(
        SealedHistory::open(
            &path,
            SegmentIdentity {
                sha256: [0; 32],
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
    assert!(portable.rows.iter().all(|row| row.sha256.is_some()));
    let second = portable.read_chunk(range(1, 3), HeightOrder::Descending, usize::MAX)?;
    assert!(
        first
            .iter()
            .map(|row| &row.payload)
            .eq(second.iter().map(|row| &row.payload))
    );
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
