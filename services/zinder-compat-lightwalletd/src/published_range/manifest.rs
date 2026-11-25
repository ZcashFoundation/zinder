use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use ring::signature::{ED25519, Ed25519KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use tempfile::{Builder, NamedTempFile, TempPath};
use zinder_core::{BlockHash, BlockHeightRange};
use zinder_query::RocksDbCanonicalReader;

use super::{PublishedShieldedRange, ShieldedRangeIdentity};

#[derive(Clone, Copy)]
pub(super) struct BuiltSegment {
    pub(super) identity: ShieldedRangeIdentity,
    pub(super) first_parent: BlockHash,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedManifest {
    manifest: String,
    signature: String,
}

const STAGED_WRITE: &str = ".publisher-write.tmp";
const STAGED_SEGMENT: &str = ".publisher-segment.tmp";
const STAGED_NULLIFIERS: &str = ".publisher-nullifiers.tmp";

fn staging_name(prefix: &str, slot: Option<usize>) -> String {
    slot.map_or_else(|| prefix.to_owned(), |worker| format!("{prefix}.{worker}"))
}

fn is_publisher_staging_name(name: &str) -> bool {
    [STAGED_WRITE, STAGED_SEGMENT, STAGED_NULLIFIERS]
        .iter()
        .any(|prefix| {
            name == *prefix
                || name
                    .strip_prefix(prefix)
                    .and_then(|suffix| suffix.strip_prefix('.'))
                    .is_some_and(|slot| {
                        !slot.is_empty() && slot.bytes().all(|byte| byte.is_ascii_digit())
                    })
        })
}

fn staged_temp_file(
    directory: &Path,
    prefix: &str,
    slot: Option<usize>,
) -> io::Result<NamedTempFile> {
    let label = staging_name(prefix, slot);
    Builder::new()
        .prefix(&label)
        .rand_bytes(0)
        .tempfile_in(directory)
}

fn staged_write(directory: &Path) -> io::Result<NamedTempFile> {
    staged_temp_file(directory, STAGED_WRITE, None)
}

pub(super) fn lock_directory(directory: &Path) -> io::Result<File> {
    let mut parents = Vec::new();
    for path in directory.ancestors() {
        if path.as_os_str().is_empty() || path.try_exists()? {
            break;
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        parents.push(parent.unwrap_or_else(|| Path::new(".")));
    }
    fs::create_dir_all(directory)?;
    for parent in parents.into_iter().rev() {
        File::open(parent)?.sync_all()?;
    }
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(".publisher.lock"))?;
    file.try_lock()?;
    // A killed publisher can leave legacy or worker-slot staging files before its next run.
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        if file_name.to_str().is_some_and(is_publisher_staging_name)
            && let Err(error) = fs::remove_file(entry.path())
            && error.kind() != io::ErrorKind::NotFound
        {
            return Err(error);
        }
    }
    Ok(file)
}

pub(super) fn publish_segment(
    reader: &RocksDbCanonicalReader,
    directory: &Path,
    range: BlockHeightRange,
) -> io::Result<PublishedShieldedRange> {
    publish_segment_in_slot(reader, directory, range, None, true)
}

pub(super) fn stage_segment(
    reader: &RocksDbCanonicalReader,
    directory: &Path,
    range: BlockHeightRange,
    slot: usize,
) -> io::Result<BuiltSegment> {
    let published = publish_segment_in_slot(reader, directory, range, Some(slot), false)?;
    Ok(BuiltSegment {
        identity: published.identity(),
        first_parent: published.segment.first_parent(),
    })
}

pub(super) fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

fn publish_segment_in_slot(
    reader: &RocksDbCanonicalReader,
    directory: &Path,
    range: BlockHeightRange,
    slot: Option<usize>,
    sync_dir: bool,
) -> io::Result<PublishedShieldedRange> {
    let segment = TempPath::try_from_path(directory.join(staging_name(STAGED_SEGMENT, slot)))?;
    let staged = (
        staged_temp_file(directory, STAGED_WRITE, slot)?,
        staged_temp_file(directory, STAGED_NULLIFIERS, slot)?,
    );
    let history = if sync_dir {
        PublishedShieldedRange::build(reader, &segment, staged, range)?
    } else {
        PublishedShieldedRange::build_staged(reader, &segment, staged, range)?
    };
    let destination = segment_path(directory, history.identity.sha256);
    let published = match fs::hard_link(&segment, &destination) {
        Ok(()) => history,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing = PublishedShieldedRange::open(&destination, history.identity())?;
            if history.segment.kernel_verified && !existing.segment.kernel_verified {
                // Reuse the verified bytes without changing other hard links to a portable orphan.
                fs::rename(&segment, &destination)?;
                history
            } else {
                existing
            }
        }
        Err(error) => return Err(error),
    };
    if sync_dir {
        sync_directory(directory)?;
    }
    Ok(published)
}

pub(super) fn install_signed(
    current: &Path,
    manifest: String,
    key: &Ed25519KeyPair,
) -> io::Result<()> {
    let directory = current.parent().unwrap_or_else(|| Path::new("."));
    let signed = SignedManifest {
        signature: hex::encode(key.sign(manifest.as_bytes()).as_ref()),
        manifest,
    };
    let mut output = staged_write(directory)?;
    serde_json::to_writer(&mut output, &signed)?;
    output.flush()?;
    if output.as_file().metadata()?.len() > super::catalog::CATALOG_LIMIT {
        return Err(invalid("signed shielded catalog exceeds its size limit"));
    }
    output.as_file().sync_all()?;
    output.persist(current)?;
    File::open(directory)?.sync_all()
}

pub(super) fn signing_key_from(path: &Path) -> io::Result<Ed25519KeyPair> {
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&read_bounded(path, 256)?)
        .map_err(|_| invalid("invalid Ed25519 PKCS#8 signing key"))
}

pub(super) fn verification_key_from(path: &Path) -> io::Result<[u8; 32]> {
    read_bounded(path, 32)?
        .try_into()
        .map_err(|_| invalid("expected a 32-byte raw Ed25519 verification key"))
}

pub(super) fn signed_contents(path: &Path, key: &[u8], limit: u64) -> io::Result<String> {
    let signed: SignedManifest = serde_json::from_slice(&read_bounded(path, limit)?)?;
    let mut signature = [0; 64];
    hex::decode_to_slice(signed.signature, &mut signature)
        .map_err(|_| invalid("invalid shielded range signature encoding"))?;
    UnparsedPublicKey::new(&ED25519, key)
        .verify(signed.manifest.as_bytes(), &signature)
        .map_err(|_| invalid("shielded range manifest signature verification failed"))?;
    Ok(signed.manifest)
}

pub(super) fn segment_path(directory: &Path, digest: [u8; 32]) -> PathBuf {
    directory.join(format!("segment-{}.bin", hex::encode(digest)))
}

fn read_bounded(path: &Path, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    crate::sealed_history::open_regular_file(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid(
            "shielded range manifest or key exceeds its size limit",
        ));
    }
    Ok(bytes)
}

pub(super) fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
