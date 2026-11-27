//! Pinned external segment snapshotting (hard-link with `EXDEV` copy fallback)
//! and parallel cold-admission segment verification against [`SettledSegmentIndexEntry`].

use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, Network, SerializedBytesDigest,
    wire::decode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd;

use crate::{
    HeightOrder, segment_verity,
    settled_raw_block_segment::{SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader},
    settled_shielded_segment::{SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader},
    settled_supplement_segment::{
        SettledSupplementSegmentOpenOptions, SettledSupplementSegmentReader,
    },
};

use super::{
    EntryKind, SettledSegmentsError, VerifiedDir,
    cache::SettledSegmentReaderCache,
    layout::{
        CanonicalSegmentsManifest, SEGMENTS_DIR_NAME, SEGMENTS_STAGING_DIR_NAME,
        SettledSegmentIndexEntry, SettledSegmentKind, format_segment_filename,
        read_segments_manifest, segment_file_path, validate_contiguous_index_entries,
        write_segments_manifest_atomic_in_dir,
    },
};

const COPY_CHUNK_BYTES: usize = 64 * 1024;
const TRAILER_DIGEST_LEN: usize = 32;
const MAX_VERIFY_WORKERS: usize = 8;

/// Hard-links (or copies with SHA-256 and fs-verity protection on `EXDEV`) pinned segments.
///
/// Only the pinned segment files enumerated by `entries` are linked or copied (ignoring newer
/// unpinned segments in `source_root`), then `segments-manifest.json` is written atomically.
pub(crate) fn snapshot_pinned_segments(
    source_root: &Path,
    target_root: &Path,
    entries: &[SettledSegmentIndexEntry],
) -> Result<CanonicalSegmentsManifest, SettledSegmentsError> {
    snapshot_pinned_segments_internal::<fn(&Path, &Path) -> io::Result<()>>(
        source_root,
        target_root,
        entries,
        None,
    )
}

/// Testable snapshot entrypoint parameterizing the hard-link primitive for `EXDEV` simulation.
#[cfg(test)]
pub(crate) fn snapshot_pinned_segments_with_linker<F>(
    source_root: &Path,
    target_root: &Path,
    entries: &[SettledSegmentIndexEntry],
    mut link_fn: F,
) -> Result<CanonicalSegmentsManifest, SettledSegmentsError>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    snapshot_pinned_segments_internal(source_root, target_root, entries, Some(&mut link_fn))
}

fn snapshot_pinned_segments_internal<F>(
    source_root: &Path,
    target_root: &Path,
    entries: &[SettledSegmentIndexEntry],
    link_override: Option<&mut F>,
) -> Result<CanonicalSegmentsManifest, SettledSegmentsError>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    let source_root_dir = VerifiedDir::open_existing(source_root, &[])?;
    let network = resolve_snapshot_network(source_root, entries)?;
    let target_root_dir = VerifiedDir::create_or_open(target_root, &[])?;

    if !entries.is_empty() {
        let mut ctx = build_snapshot_context(
            SnapshotRoots {
                source_path: source_root,
                target_path: target_root,
                source_dir: &source_root_dir,
                target_dir: &target_root_dir,
            },
            entries,
            link_override,
        )?;
        for entry in entries {
            snapshot_single_entry(&mut ctx, entry)?;
        }
        sync_snapshot_target_dirs(&mut ctx, entries)?;
    }

    let manifest = CanonicalSegmentsManifest::from_entries(network, entries.to_vec())?;
    write_segments_manifest_atomic_in_dir(&target_root_dir, &manifest)?;
    Ok(manifest)
}

/// Opens and authenticates every pinned segment file referenced by `entries`
/// in parallel across workers against its committed hashes, consecutive-stride
/// parent linkage, and optional `fs-verity`.
#[cfg(test)]
pub(crate) fn verify_pinned_segments(
    store_root: &Path,
    entries: &[SettledSegmentIndexEntry],
    require_fs_verity: bool,
) -> Result<(), SettledSegmentsError> {
    verify_pinned_segments_internal(store_root, entries, require_fs_verity, None)
}

/// Opens, authenticates, and populates `cache` with every verified reader opened
/// for `entries` so subsequent cold replay avoids reopening segment files.
pub(crate) fn verify_pinned_segments_into_cache(
    store_root: &Path,
    entries: &[SettledSegmentIndexEntry],
    require_fs_verity: bool,
    cache: &SettledSegmentReaderCache,
) -> Result<(), SettledSegmentsError> {
    verify_pinned_segments_internal(store_root, entries, require_fs_verity, Some(cache))
}

fn verify_pinned_segments_internal(
    store_root: &Path,
    entries: &[SettledSegmentIndexEntry],
    require_fs_verity: bool,
    cache: Option<&SettledSegmentReaderCache>,
) -> Result<(), SettledSegmentsError> {
    validate_contiguous_index_entries(entries)?;
    let root_dir = VerifiedDir::open_existing(store_root, &[])?;
    if entries.is_empty() {
        return Ok(());
    }
    let segments_dir = root_dir.open_child_dir_existing(SEGMENTS_DIR_NAME)?;
    let dirs = open_verification_dirs(&segments_dir, entries)?;
    let worker_count = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(entries.len())
        .min(MAX_VERIFY_WORKERS);
    if worker_count <= 1 {
        let mut expected_prev_hash: Option<BlockHash> = None;
        for entry in entries {
            verify_single_entry_files(&dirs, entry, expected_prev_hash, require_fs_verity, cache)?;
            expected_prev_hash = Some(entry.end_block.hash);
        }
        return Ok(());
    }
    verify_entries_parallel(&dirs, entries, require_fs_verity, worker_count, cache)
}

fn verify_entries_parallel(
    dirs: &VerificationDirs,
    entries: &[SettledSegmentIndexEntry],
    require_fs_verity: bool,
    worker_count: usize,
    cache: Option<&SettledSegmentReaderCache>,
) -> Result<(), SettledSegmentsError> {
    let next_idx = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let earliest_error: Mutex<Option<(usize, SettledSegmentsError)>> = Mutex::new(None);

    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                while !failed.load(Ordering::Acquire) {
                    let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                    let Some(entry) = entries.get(idx) else {
                        break;
                    };
                    let expected_prev_hash =
                        idx.checked_sub(1).map(|prev| entries[prev].end_block.hash);
                    if let Err(err) = verify_single_entry_files(
                        dirs,
                        entry,
                        expected_prev_hash,
                        require_fs_verity,
                        cache,
                    ) {
                        failed.store(true, Ordering::Release);
                        record_earliest_error(&earliest_error, idx, err);
                        break;
                    }
                }
            });
        }
    });

    if let Some((_, err)) = earliest_error.into_inner() {
        return Err(err);
    }
    Ok(())
}

fn record_earliest_error(
    slot: &Mutex<Option<(usize, SettledSegmentsError)>>,
    idx: usize,
    err: SettledSegmentsError,
) {
    let mut guard = slot.lock();
    let replace = guard.as_ref().is_none_or(|(prev_idx, _)| idx < *prev_idx);
    if replace {
        *guard = Some((idx, err));
    }
}

/// Loads `segments-manifest.json` from `store_root` and verifies every pinned
/// segment listed in it against its committed metadata.
#[cfg(test)]
pub(crate) fn verify_pinned_manifest_and_segments(
    store_root: &Path,
    require_fs_verity: bool,
) -> Result<CanonicalSegmentsManifest, SettledSegmentsError> {
    let manifest = read_segments_manifest(store_root)?;
    verify_pinned_segments(store_root, &manifest.entries, require_fs_verity)?;
    Ok(manifest)
}

struct SnapshotContext<'a, F> {
    source_root: &'a Path,
    target_root: &'a Path,
    source_shielded: VerifiedDir,
    source_supplement: Option<VerifiedDir>,
    source_raw_block: Option<VerifiedDir>,
    target_segments: VerifiedDir,
    target_shielded: VerifiedDir,
    target_supplement: Option<VerifiedDir>,
    target_raw_block: Option<VerifiedDir>,
    target_staging: Option<VerifiedDir>,
    link_override: Option<&'a mut F>,
}

struct VerificationDirs {
    shielded: VerifiedDir,
    supplement: Option<VerifiedDir>,
    raw_block: Option<VerifiedDir>,
}

#[derive(Clone, Copy)]
struct PinnedFileSpec<'a> {
    kind: SettledSegmentKind,
    filename: &'a str,
    src_dir: &'a VerifiedDir,
    dst_dir: &'a VerifiedDir,
    expected_sha256: [u8; 32],
    expected_fs_verity: Option<[u8; 32]>,
    start_height: BlockHeight,
    end_height: BlockHeight,
}

fn resolve_snapshot_network(
    source_root: &Path,
    entries: &[SettledSegmentIndexEntry],
) -> Result<Network, SettledSegmentsError> {
    if let Some(network) = validate_contiguous_index_entries(entries)? {
        return Ok(network);
    }
    let manifest = read_segments_manifest(source_root)?;
    Ok(manifest.network)
}

#[derive(Clone, Copy)]
struct SnapshotRoots<'a> {
    source_path: &'a Path,
    target_path: &'a Path,
    source_dir: &'a VerifiedDir,
    target_dir: &'a VerifiedDir,
}

fn build_snapshot_context<'a, F>(
    roots: SnapshotRoots<'a>,
    entries: &[SettledSegmentIndexEntry],
    link_override: Option<&'a mut F>,
) -> Result<SnapshotContext<'a, F>, SettledSegmentsError> {
    let source_root = roots.source_path;
    let target_root = roots.target_path;
    let source_root_dir = roots.source_dir;
    let target_root_dir = roots.target_dir;
    let need_sup = entries.iter().any(|entry| entry.modules.supplement);
    let need_raw = entries.iter().any(|entry| entry.modules.raw_bodies);

    let source_segments = source_root_dir.open_child_dir_existing(SEGMENTS_DIR_NAME)?;
    let source_shielded =
        source_segments.open_child_dir_existing(SettledSegmentKind::Shielded.subdir_name())?;
    let source_supplement = need_sup
        .then(|| {
            source_segments.open_child_dir_existing(SettledSegmentKind::Supplement.subdir_name())
        })
        .transpose()?;
    let source_raw_block = need_raw
        .then(|| {
            source_segments.open_child_dir_existing(SettledSegmentKind::RawBlock.subdir_name())
        })
        .transpose()?;

    let target_segments = target_root_dir.create_or_open_child_dir(SEGMENTS_DIR_NAME)?;
    let target_shielded =
        target_segments.create_or_open_child_dir(SettledSegmentKind::Shielded.subdir_name())?;
    let target_supplement = need_sup
        .then(|| {
            target_segments.create_or_open_child_dir(SettledSegmentKind::Supplement.subdir_name())
        })
        .transpose()?;
    let target_raw_block = need_raw
        .then(|| {
            target_segments.create_or_open_child_dir(SettledSegmentKind::RawBlock.subdir_name())
        })
        .transpose()?;

    Ok(SnapshotContext {
        source_root,
        target_root,
        source_shielded,
        source_supplement,
        source_raw_block,
        target_segments,
        target_shielded,
        target_supplement,
        target_raw_block,
        target_staging: None,
        link_override,
    })
}

fn open_verification_dirs(
    segments_dir: &VerifiedDir,
    entries: &[SettledSegmentIndexEntry],
) -> Result<VerificationDirs, SettledSegmentsError> {
    let need_sup = entries.iter().any(|entry| entry.modules.supplement);
    let need_raw = entries.iter().any(|entry| entry.modules.raw_bodies);
    let shielded =
        segments_dir.open_child_dir_existing(SettledSegmentKind::Shielded.subdir_name())?;
    let supplement = need_sup
        .then(|| segments_dir.open_child_dir_existing(SettledSegmentKind::Supplement.subdir_name()))
        .transpose()?;
    let raw_block = need_raw
        .then(|| segments_dir.open_child_dir_existing(SettledSegmentKind::RawBlock.subdir_name()))
        .transpose()?;
    Ok(VerificationDirs {
        shielded,
        supplement,
        raw_block,
    })
}

fn sync_snapshot_target_dirs<F>(
    ctx: &mut SnapshotContext<'_, F>,
    entries: &[SettledSegmentIndexEntry],
) -> Result<(), SettledSegmentsError> {
    if entries.is_empty() {
        return Ok(());
    }
    ctx.target_shielded.sync()?;
    if let Some(sup_dir) = &ctx.target_supplement {
        sup_dir.sync()?;
    }
    if let Some(raw_dir) = &ctx.target_raw_block {
        raw_dir.sync()?;
    }
    if ctx.target_staging.take().is_some() {
        let _ = ctx
            .target_segments
            .remove_empty_dir(SEGMENTS_STAGING_DIR_NAME);
    }
    ctx.target_segments.sync()
}

fn snapshot_single_entry<F>(
    ctx: &mut SnapshotContext<'_, F>,
    entry: &SettledSegmentIndexEntry,
) -> Result<(), SettledSegmentsError>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    entry.validate()?;
    let start = entry.start_height;
    let end = entry.end_block.height;

    let shielded_name = format_segment_filename(SettledSegmentKind::Shielded, start, end)?;
    let shielded_id = entry.shielded_identity;
    let shielded_src = ctx.source_shielded.try_clone()?;
    let shielded_dst = ctx.target_shielded.try_clone()?;
    verify_source_shielded(&shielded_src, &shielded_name, &shielded_id, false)?;
    place_segment_file(
        ctx,
        PinnedFileSpec {
            kind: SettledSegmentKind::Shielded,
            filename: &shielded_name,
            src_dir: &shielded_src,
            dst_dir: &shielded_dst,
            expected_sha256: shielded_id.sha256,
            expected_fs_verity: shielded_id.fs_verity_sha256,
            start_height: start,
            end_height: end,
        },
        |dir, name| verify_source_shielded(dir, name, &shielded_id, false),
    )?;

    if let (Some(src_sup_dir), Some(dst_sup_dir), Some(sup_id)) = (
        ctx.source_supplement
            .as_ref()
            .map(VerifiedDir::try_clone)
            .transpose()?,
        ctx.target_supplement
            .as_ref()
            .map(VerifiedDir::try_clone)
            .transpose()?,
        entry.supplement_identity,
    ) {
        let sup_name = format_segment_filename(SettledSegmentKind::Supplement, start, end)?;
        verify_source_supplement(&src_sup_dir, &sup_name, &sup_id, false)?;
        place_segment_file(
            ctx,
            PinnedFileSpec {
                kind: SettledSegmentKind::Supplement,
                filename: &sup_name,
                src_dir: &src_sup_dir,
                dst_dir: &dst_sup_dir,
                expected_sha256: sup_id.sha256,
                expected_fs_verity: sup_id.fs_verity_sha256,
                start_height: start,
                end_height: end,
            },
            |dir, name| verify_source_supplement(dir, name, &sup_id, false),
        )?;
    }

    if let (Some(src_raw_dir), Some(dst_raw_dir), Some(raw_id)) = (
        ctx.source_raw_block
            .as_ref()
            .map(VerifiedDir::try_clone)
            .transpose()?,
        ctx.target_raw_block
            .as_ref()
            .map(VerifiedDir::try_clone)
            .transpose()?,
        entry.raw_block_identity,
    ) {
        let raw_name = format_segment_filename(SettledSegmentKind::RawBlock, start, end)?;
        verify_source_raw_block(&src_raw_dir, &raw_name, &raw_id, false)?;
        place_segment_file(
            ctx,
            PinnedFileSpec {
                kind: SettledSegmentKind::RawBlock,
                filename: &raw_name,
                src_dir: &src_raw_dir,
                dst_dir: &dst_raw_dir,
                expected_sha256: raw_id.sha256,
                expected_fs_verity: raw_id.fs_verity_sha256,
                start_height: start,
                end_height: end,
            },
            |dir, name| verify_source_raw_block(dir, name, &raw_id, false),
        )?;
    }
    Ok(())
}

fn place_segment_file<F, V>(
    ctx: &mut SnapshotContext<'_, F>,
    spec: PinnedFileSpec<'_>,
    verify_target: V,
) -> Result<(), SettledSegmentsError>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
    V: Fn(&VerifiedDir, &str) -> Result<(), SettledSegmentsError>,
{
    if spec.src_dir.stat_entry_nofollow(spec.filename)? != Some(EntryKind::RegularFile) {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "refusing non-regular source segment {}",
            spec.filename
        )));
    }
    if spec.dst_dir.stat_entry_nofollow(spec.filename)?.is_some() {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "snapshot destination already exists: {}",
            spec.filename
        )));
    }

    let link_outcome = match ctx.link_override.as_deref_mut() {
        Some(link_fn) => {
            let src_path = segment_file_path(
                ctx.source_root,
                spec.kind,
                spec.start_height,
                spec.end_height,
            )?;
            let dst_path = segment_file_path(
                ctx.target_root,
                spec.kind,
                spec.start_height,
                spec.end_height,
            )?;
            link_fn(&src_path, &dst_path)
        }
        None => spec
            .src_dir
            .link_from_dir(spec.filename, spec.dst_dir, spec.filename),
    };

    match link_outcome {
        Ok(()) => {}
        Err(err) if is_cross_device_link_error(&err) => {
            copy_with_integrity_and_verity(ctx, &spec)?;
        }
        Err(err) => return Err(SettledSegmentsError::Io(err)),
    }
    verify_target(spec.dst_dir, spec.filename)
}

fn copy_with_integrity_and_verity<F>(
    ctx: &mut SnapshotContext<'_, F>,
    spec: &PinnedFileSpec<'_>,
) -> Result<(), SettledSegmentsError> {
    if ctx.target_staging.is_none() {
        ctx.target_staging = Some(
            ctx.target_segments
                .create_or_open_child_dir(SEGMENTS_STAGING_DIR_NAME)?,
        );
    }
    let Some(staging_dir) = ctx.target_staging.as_ref() else {
        return Err(SettledSegmentsError::InvalidLayout(
            "snapshot staging directory missing".to_owned(),
        ));
    };

    let mut source = spec.src_dir.open_regular_file_nofollow(spec.filename)?;
    let total_len = source.metadata()?.len();
    let min_len = u64::try_from(TRAILER_DIGEST_LEN).unwrap_or(32);
    if total_len < min_len {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "segment file {} truncated before SHA-256 trailer",
            spec.filename
        )));
    }

    let (temp_name, staged) = staging_dir.create_temp_file("snap")?;
    let copy_outcome = (|| -> Result<(), SettledSegmentsError> {
        let computed_verity = stream_and_verify_copy(&mut source, &staged, total_len, spec)?;
        staged.sync_all()?;
        let dev_ino = file_dev_ino(&staged)?;
        drop(staged);

        let readonly = staging_dir.open_regular_matching_inode(&temp_name, dev_ino)?;
        segment_verity::protect_new_segment(&readonly, Some(computed_verity), "settled segment")?;
        drop(readonly);

        staging_dir
            .rename_noclobber_to_dir(&temp_name, spec.dst_dir, spec.filename)
            .map_err(SettledSegmentsError::Io)?;
        Ok(())
    })();
    if let Err(err) = copy_outcome {
        let _ = staging_dir.unlink_file(&temp_name);
        return Err(err);
    }
    Ok(())
}

fn file_dev_ino(file: &File) -> io::Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let meta = file.metadata()?;
        Ok((meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok((0, 0))
    }
}

fn stream_and_verify_copy(
    source: &mut File,
    mut dest: &File,
    total_len: u64,
    spec: &PinnedFileSpec<'_>,
) -> Result<[u8; 32], SettledSegmentsError> {
    let trailer_len_u64 = u64::try_from(TRAILER_DIGEST_LEN).unwrap_or(32);
    let trailer_offset = total_len - trailer_len_u64;
    let mut body_hasher = Sha256::new();
    let mut verity_hasher = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    let mut chunk = vec![0_u8; COPY_CHUNK_BYTES];
    let chunk_cap_u64 = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
    let mut remaining = trailer_offset;

    while remaining > 0 {
        let to_read = usize::try_from(remaining.min(chunk_cap_u64)).map_err(|_| {
            SettledSegmentsError::InvalidFormat("copy chunk size exceeds address space".to_owned())
        })?;
        source.read_exact(&mut chunk[..to_read])?;
        dest.write_all(&chunk[..to_read])?;
        body_hasher.update(&chunk[..to_read]);
        verity_hasher.write_all(&chunk[..to_read])?;
        remaining -= u64::try_from(to_read).unwrap_or(0);
    }

    let mut trailer = [0_u8; TRAILER_DIGEST_LEN];
    source.read_exact(&mut trailer)?;
    let computed_body: [u8; 32] = body_hasher.finalize().into();
    if computed_body != spec.expected_sha256 || trailer != spec.expected_sha256 {
        return Err(SettledSegmentsError::Integrity(format!(
            "copied segment {} SHA-256 differs from pinned entry",
            spec.filename
        )));
    }
    dest.write_all(&trailer)?;
    verity_hasher.write_all(&trailer)?;
    let computed_verity: [u8; 32] = verity_hasher.finalize().as_ref().try_into().map_err(|_| {
        SettledSegmentsError::Integrity("invalid computed fs-verity digest length".to_owned())
    })?;
    if let Some(expected_verity) = spec.expected_fs_verity
        && computed_verity != expected_verity
    {
        return Err(SettledSegmentsError::Integrity(format!(
            "copied segment {} computed fs-verity digest mismatch",
            spec.filename
        )));
    }
    Ok(computed_verity)
}

struct AuthenticatedShieldedBoundary {
    hash: BlockHash,
    prev_hash: BlockHash,
    payload: Vec<u8>,
}

struct ShieldedEntryContext<'a> {
    entry: &'a SettledSegmentIndexEntry,
    shielded_hashes: &'a [BlockHash],
    first_prev_hash: BlockHash,
    first_shielded: &'a AuthenticatedShieldedBoundary,
    last_shielded: Option<&'a AuthenticatedShieldedBoundary>,
    require_fs_verity: bool,
}

fn verify_single_entry_files(
    dirs: &VerificationDirs,
    entry: &SettledSegmentIndexEntry,
    expected_prev_hash: Option<BlockHash>,
    require_fs_verity: bool,
    cache: Option<&SettledSegmentReaderCache>,
) -> Result<(), SettledSegmentsError> {
    let start = entry.start_height;
    let end = entry.end_block.height;
    let shielded_name = format_segment_filename(SettledSegmentKind::Shielded, start, end)?;
    let shielded_file = dirs.shielded.open_regular_file_nofollow(&shielded_name)?;
    let shielded_reader = SettledShieldedSegmentReader::open_file(
        shielded_file,
        &entry.shielded_identity,
        SettledShieldedSegmentOpenOptions {
            require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !shielded_reader.kernel_verified() {
        shielded_reader.verify_full_payload()?;
    }
    let first_prev_hash = shielded_reader.first_previous_block_hash()?;
    let shielded_hashes = shielded_reader.read_all_block_hashes()?;
    let first_shielded = read_authenticated_shielded_block(
        &shielded_reader,
        entry.start_height,
        entry.supplement_identity.is_some(),
    )?;
    check_stride_first_linkage(entry, first_prev_hash, &first_shielded, expected_prev_hash)?;
    let last_shielded = read_trailing_shielded_block(&shielded_reader, entry)?;

    let ctx = ShieldedEntryContext {
        entry,
        shielded_hashes: &shielded_hashes,
        first_prev_hash,
        first_shielded: &first_shielded,
        last_shielded: last_shielded.as_ref(),
        require_fs_verity,
    };
    let sup_reader = if let (Some(sup_dir), Some(sup_id)) =
        (dirs.supplement.as_ref(), entry.supplement_identity.as_ref())
    {
        Some(verify_supplement_linkage(&ctx, sup_dir, sup_id)?)
    } else {
        None
    };

    let raw_reader = if let (Some(raw_dir), Some(raw_id)) =
        (dirs.raw_block.as_ref(), entry.raw_block_identity.as_ref())
    {
        Some(verify_raw_block_linkage(
            &ctx,
            raw_dir,
            raw_id,
            sup_reader.as_ref(),
        )?)
    } else {
        None
    };

    if let Some(target_cache) = cache {
        target_cache.insert_verified_readers(
            entry,
            Arc::new(shielded_reader),
            sup_reader.map(Arc::new),
            raw_reader.map(Arc::new),
        )?;
    }
    Ok(())
}

fn check_stride_first_linkage(
    entry: &SettledSegmentIndexEntry,
    first_prev_hash: BlockHash,
    first_shielded: &AuthenticatedShieldedBoundary,
    expected_prev_hash: Option<BlockHash>,
) -> Result<(), SettledSegmentsError> {
    if first_shielded.prev_hash != first_prev_hash {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "shielded first block {} prev_hash disagrees with header",
            entry.start_height.value()
        )));
    }
    if let Some(prev_hash) = expected_prev_hash
        && first_shielded.prev_hash != prev_hash
    {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "stride starting at {} links parent {:?} instead of prior stride tip {:?}",
            entry.start_height.value(),
            first_shielded.prev_hash,
            prev_hash
        )));
    }
    if entry.start_height == entry.end_block.height && first_shielded.hash != entry.end_block.hash {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "single-block stride {} hash disagrees with end_block",
            entry.start_height.value()
        )));
    }
    Ok(())
}

fn read_trailing_shielded_block(
    shielded_reader: &SettledShieldedSegmentReader,
    entry: &SettledSegmentIndexEntry,
) -> Result<Option<AuthenticatedShieldedBoundary>, SettledSegmentsError> {
    if entry.end_block.height == entry.start_height || entry.supplement_identity.is_none() {
        return Ok(None);
    }
    let trailing =
        read_authenticated_shielded_block(shielded_reader, entry.end_block.height, true)?;
    if trailing.hash != entry.end_block.hash {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "shielded trailing block {} hash disagrees with end_block",
            entry.end_block.height.value()
        )));
    }
    Ok(Some(trailing))
}

fn read_authenticated_shielded_block(
    reader: &SettledShieldedSegmentReader,
    height: BlockHeight,
    use_reconstruction_read: bool,
) -> Result<AuthenticatedShieldedBoundary, SettledSegmentsError> {
    let range = BlockHeightRange::inclusive(height, height);
    let encoded = if use_reconstruction_read {
        reader.read_default_range_for_reconstruction(range, HeightOrder::Ascending)?
    } else {
        reader.read_default_range(range, HeightOrder::Ascending)?
    };
    let [frame] = encoded.as_slice() else {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "expected exactly 1 shielded block at height {}, got {}",
            height.value(),
            encoded.len()
        )));
    };
    let payload = frame.payload().to_vec();
    let proto_block = lightwalletd::CompactBlock::decode(payload.as_slice()).map_err(|err| {
        SettledSegmentsError::Integrity(format!(
            "invalid shielded CompactBlock protobuf at height {}: {err}",
            height.value()
        ))
    })?;
    if proto_block.height != u64::from(height.value()) {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "shielded block height {} disagrees with requested {}",
            proto_block.height,
            height.value()
        )));
    }
    let hash_bytes: [u8; 32] = proto_block.hash.as_slice().try_into().map_err(|_| {
        SettledSegmentsError::Integrity("invalid shielded block hash byte length".to_owned())
    })?;
    let prev_bytes: [u8; 32] = proto_block.prev_hash.as_slice().try_into().map_err(|_| {
        SettledSegmentsError::Integrity("invalid shielded prev_hash byte length".to_owned())
    })?;
    let hash = decode_internal_block_hash(&hash_bytes).map_err(|err| {
        SettledSegmentsError::Integrity(format!("invalid shielded block hash: {err}"))
    })?;
    let prev_hash = decode_internal_block_hash(&prev_bytes).map_err(|err| {
        SettledSegmentsError::Integrity(format!("invalid shielded prev_hash: {err}"))
    })?;
    Ok(AuthenticatedShieldedBoundary {
        hash,
        prev_hash,
        payload,
    })
}

fn verify_supplement_linkage(
    ctx: &ShieldedEntryContext<'_>,
    sup_dir: &VerifiedDir,
    sup_id: &crate::settled_supplement_segment::SupplementSegmentIdentity,
) -> Result<SettledSupplementSegmentReader, SettledSegmentsError> {
    let filename = format_segment_filename(
        SettledSegmentKind::Supplement,
        ctx.entry.start_height,
        ctx.entry.end_block.height,
    )?;
    let sup_file = sup_dir.open_regular_file_nofollow(&filename)?;
    let reader = SettledSupplementSegmentReader::open_file(
        sup_file,
        sup_id,
        SettledSupplementSegmentOpenOptions {
            require_fs_verity: ctx.require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !reader.kernel_verified() {
        reader.verify_full_payload()?;
    }
    if reader.first_previous_block_hash()? != ctx.first_prev_hash {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "ZSTSUP01 first_previous_block_hash at {}..={} disagrees with ZSHRNG02",
            sup_id.start_height.value(),
            sup_id.end_block.height.value()
        )));
    }
    let sup_hashes = reader.read_all_block_hashes()?;
    if sup_hashes.len() != ctx.shielded_hashes.len() {
        return Err(SettledSegmentsError::ChainLinkage(
            "ZSTSUP01 block count disagrees with ZSHRNG02".to_owned(),
        ));
    }
    for (idx, (&sup_hash, &shielded_hash)) in sup_hashes.iter().zip(ctx.shielded_hashes).enumerate()
    {
        if sup_hash != shielded_hash {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "ZSTSUP01 block hash at height {} disagrees with ZSHRNG02",
                ctx.entry.start_height.value() + u32::try_from(idx).unwrap_or(0)
            )));
        }
    }
    reader.reconstruct_block(sup_id.start_height, &ctx.first_shielded.payload)?;
    if let Some(trailing) = ctx.last_shielded {
        reader.reconstruct_block(sup_id.end_block.height, &trailing.payload)?;
    }
    Ok(reader)
}

fn verify_raw_block_linkage(
    ctx: &ShieldedEntryContext<'_>,
    raw_dir: &VerifiedDir,
    raw_id: &crate::settled_raw_block_segment::RawBlockSegmentIdentity,
    sup_reader: Option<&SettledSupplementSegmentReader>,
) -> Result<SettledRawBlockSegmentReader, SettledSegmentsError> {
    let filename = format_segment_filename(
        SettledSegmentKind::RawBlock,
        ctx.entry.start_height,
        ctx.entry.end_block.height,
    )?;
    let raw_file = raw_dir.open_regular_file_nofollow(&filename)?;
    let raw_reader = SettledRawBlockSegmentReader::open_file(
        raw_file,
        raw_id,
        SettledRawBlockSegmentOpenOptions {
            require_fs_verity: ctx.require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !raw_reader.kernel_verified() {
        raw_reader.verify_full_payload()?;
    }
    if raw_reader.first_previous_block_hash()? != ctx.first_prev_hash {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "ZSBLKB01 first_previous_block_hash at {}..={} disagrees with ZSHRNG02",
            raw_id.start_height.value(),
            raw_id.end_block.height.value()
        )));
    }
    let raw_rows = raw_reader.read_all_row_hashes_and_digests()?;
    if raw_rows.len() != ctx.shielded_hashes.len() {
        return Err(SettledSegmentsError::ChainLinkage(
            "ZSBLKB01 block count disagrees with ZSHRNG02".to_owned(),
        ));
    }
    for (idx, ((raw_hash, _), &shielded_hash)) in
        raw_rows.iter().zip(ctx.shielded_hashes).enumerate()
    {
        if *raw_hash != shielded_hash {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "ZSBLKB01 block hash at height {} disagrees with ZSHRNG02",
                ctx.entry.start_height.value() + u32::try_from(idx).unwrap_or(0)
            )));
        }
    }

    verify_raw_boundary_digests(&raw_reader, raw_id, &raw_rows)?;
    if let Some(sup) = sup_reader {
        verify_raw_against_supplement_range(
            sup,
            &raw_reader,
            ctx.entry.start_height,
            ctx.entry.end_block.height,
            &raw_rows,
        )?;
    }
    Ok(raw_reader)
}

fn verify_raw_boundary_digests(
    raw_reader: &SettledRawBlockSegmentReader,
    raw_id: &crate::settled_raw_block_segment::RawBlockSegmentIdentity,
    raw_rows: &[(BlockHash, SerializedBytesDigest)],
) -> Result<(), SettledSegmentsError> {
    let first_raw = raw_reader.read_raw_block(raw_id.start_height)?;
    if SerializedBytesDigest::from_serialized_bytes(&first_raw) != raw_rows[0].1 {
        return Err(SettledSegmentsError::Integrity(format!(
            "ZSBLKB01 boundary digest mismatch at height {}",
            raw_id.start_height.value()
        )));
    }
    if raw_id.end_block.height != raw_id.start_height {
        let last_raw = raw_reader.read_raw_block(raw_id.end_block.height)?;
        let last_digest = raw_rows.last().ok_or_else(|| {
            SettledSegmentsError::ChainLinkage("empty ZSBLKB01 row metadata".to_owned())
        })?;
        if SerializedBytesDigest::from_serialized_bytes(&last_raw) != last_digest.1 {
            return Err(SettledSegmentsError::Integrity(format!(
                "ZSBLKB01 boundary digest mismatch at height {}",
                raw_id.end_block.height.value()
            )));
        }
    }
    Ok(())
}

fn verify_raw_against_supplement_range(
    sup: &SettledSupplementSegmentReader,
    raw_reader: &SettledRawBlockSegmentReader,
    start_height: BlockHeight,
    end_height: BlockHeight,
    raw_rows: &[(BlockHash, SerializedBytesDigest)],
) -> Result<(), SettledSegmentsError> {
    sup.visit_canonical_metadata_range::<SettledSegmentsError, _>(
        start_height,
        end_height,
        |height, maybe_meta| {
            let Some(canonical_meta) = maybe_meta else {
                return Ok(());
            };
            let row_idx = usize::try_from(height.value() - start_height.value()).map_err(|_| {
                SettledSegmentsError::InvalidFormat("height offset exceeds usize".to_owned())
            })?;
            let (_, raw_digest) = raw_rows.get(row_idx).ok_or_else(|| {
                SettledSegmentsError::ChainLinkage("ZSBLKB01 row missing for height".to_owned())
            })?;
            if *raw_digest != canonical_meta.serialized_bytes_digest {
                return Err(SettledSegmentsError::Integrity(format!(
                    "ZSBLKB01 raw block digest at height {} disagrees with ZSTSUP01",
                    height.value()
                )));
            }
            let expected_count =
                u32::try_from(canonical_meta.transactions.len()).map_err(|_| {
                    SettledSegmentsError::InvalidFormat(
                        "ZSTSUP01 transaction count exceeds u32".to_owned(),
                    )
                })?;
            let indexed_count = raw_reader
                .visit_indexed_transaction_digests::<SettledSegmentsError, _>(
                    height,
                    |tx_idx, tx_digest| {
                        let idx = usize::try_from(tx_idx).map_err(|_| {
                            SettledSegmentsError::InvalidFormat(
                                "ZSBLKB01 transaction index exceeds usize".to_owned(),
                            )
                        })?;
                        let Some(meta_tx) = canonical_meta.transactions.get(idx) else {
                            return Err(SettledSegmentsError::Integrity(format!(
                                "ZSBLKB01 raw block tx {tx_idx} at height {} missing from ZSTSUP01",
                                height.value()
                            )));
                        };
                        if meta_tx.serialized_bytes_digest != tx_digest {
                            return Err(SettledSegmentsError::Integrity(format!(
                                "ZSBLKB01 raw tx {tx_idx} digest at height {} disagrees with ZSTSUP01",
                                height.value()
                            )));
                        }
                        Ok(())
                    },
                )?;
            if indexed_count != expected_count {
                return Err(SettledSegmentsError::Integrity(format!(
                    "ZSBLKB01 raw block tx count ({indexed_count}) at height {} disagrees with ZSTSUP01 ({expected_count})",
                    height.value()
                )));
            }
            Ok(())
        },
    )
}

fn verify_source_shielded(
    dir: &VerifiedDir,
    filename: &str,
    expected: &crate::settled_shielded_segment::ShieldedSegmentIdentity,
    require_fs_verity: bool,
) -> Result<(), SettledSegmentsError> {
    let file = dir.open_regular_file_nofollow(filename)?;
    let reader = SettledShieldedSegmentReader::open_file(
        file,
        expected,
        SettledShieldedSegmentOpenOptions {
            require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !reader.kernel_verified() {
        reader.verify_full_payload()?;
    }
    Ok(())
}

fn verify_source_supplement(
    dir: &VerifiedDir,
    filename: &str,
    expected: &crate::settled_supplement_segment::SupplementSegmentIdentity,
    require_fs_verity: bool,
) -> Result<(), SettledSegmentsError> {
    let file = dir.open_regular_file_nofollow(filename)?;
    let reader = SettledSupplementSegmentReader::open_file(
        file,
        expected,
        SettledSupplementSegmentOpenOptions {
            require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !reader.kernel_verified() {
        reader.verify_full_payload()?;
    }
    Ok(())
}

fn verify_source_raw_block(
    dir: &VerifiedDir,
    filename: &str,
    expected: &crate::settled_raw_block_segment::RawBlockSegmentIdentity,
    require_fs_verity: bool,
) -> Result<(), SettledSegmentsError> {
    let file = dir.open_regular_file_nofollow(filename)?;
    let reader = SettledRawBlockSegmentReader::open_file(
        file,
        expected,
        SettledRawBlockSegmentOpenOptions {
            require_fs_verity,
            verify_full_payload: false,
        },
    )?;
    if !reader.kernel_verified() {
        reader.verify_full_payload()?;
    }
    Ok(())
}

fn is_cross_device_link_error(err: &io::Error) -> bool {
    #[cfg(unix)]
    if rustix::io::Errno::from_io_error(err) == Some(rustix::io::Errno::XDEV) {
        return true;
    }
    err.kind() == io::ErrorKind::CrossesDevices
}
