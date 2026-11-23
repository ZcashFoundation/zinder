use std::{fs, num::NonZeroU32, path::PathBuf};

use eyre::{Result, eyre};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use tempfile::{TempDir, tempdir};
use zinder_core::Network;
use zinder_query::RocksDbCanonicalReader;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

use super::*;
use crate::published_range::manifest::{self, verification_key_from};

struct Fixture {
    directory: TempDir,
    private: PathBuf,
    public: PathBuf,
}

impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempdir()?;
        let private = directory.path().join("publisher.pk8");
        let public = directory.path().join("publisher.pub");
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| eyre!("generate fixture key"))?;
        let key =
            Ed25519KeyPair::from_pkcs8(document.as_ref()).map_err(|_| eyre!("open fixture key"))?;
        fs::write(&private, document.as_ref())?;
        fs::write(&public, key.public_key())?;
        Ok(Self {
            directory,
            private,
            public,
        })
    }

    fn current(&self) -> PathBuf {
        self.directory.path().join("current.json")
    }

    fn publisher(&self) -> io::Result<ShieldedCatalogPublisher> {
        self.publisher_from(BlockHeight::new(1))
    }

    fn publisher_from(&self, first: BlockHeight) -> io::Result<ShieldedCatalogPublisher> {
        ShieldedCatalogPublisher::open(
            self.directory.path(),
            &self.private,
            first,
            NonZeroU32::new(2).ok_or_else(|| invalid("invalid fixture width"))?,
        )
    }

    fn catalog(
        &self,
        previous: Option<&PublishedShieldedCatalog>,
    ) -> io::Result<PublishedShieldedCatalog> {
        PublishedShieldedCatalog::open_signed(
            &self.current(),
            &verification_key_from(&self.public)?,
            false,
            previous,
            None,
        )
    }

    fn entries(&self) -> io::Result<Vec<ShieldedRangeIdentity>> {
        load_entries(&self.current(), &verification_key_from(&self.public)?)
    }

    fn force_catalog(&self, entries: Vec<ShieldedRangeIdentity>) -> io::Result<()> {
        let contents = serde_json::to_string(&CatalogManifest {
            format: FORMAT.to_owned(),
            segments: entries,
        })?;
        manifest::install_signed(
            &self.current(),
            contents,
            &manifest::signing_key_from(&self.private)?,
        )
    }
}

fn reader(chain: &ChainFixture) -> Result<(WalletServingStoreFixture, RocksDbCanonicalReader)> {
    let mut store = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain),
        &sample_regtest_upgrade_activations(),
    )?
    .into_serving_store_fixture()?;
    let canonical = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    Ok((store, canonical))
}

/// Fills `catalog`'s live tail from `reader` as a request for `requested` does.
async fn fill_tail(
    catalog: &PublishedShieldedCatalog,
    reader: &Arc<RocksDbCanonicalReader>,
    requested: BlockHeightRange,
) -> Result<Arc<super::super::tail::EncodedTail>> {
    let slot = crate::CanonicalReaderSlot::new(reader.clone());
    let terminal = (
        catalog.bounds().end,
        catalog.terminal().segment.identity().end,
    );
    catalog
        .tail
        .load_or_refresh(
            (&slot, reader, &tokio_util::sync::CancellationToken::new()),
            Some(terminal),
            requested,
            super::super::tail::TailReadLimits {
                ascending_bytes: None,
                refresh_wait: crate::DEFAULT_BLOCK_RANGE_ADMISSION_WAIT,
            },
        )
        .await?
        .ok_or_else(|| eyre!("missing tail"))
}

#[tokio::test]
async fn catching_up_the_secondary_extends_and_reuses_the_admitted_prefix() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(7);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(2)),
        &sample_regtest_upgrade_activations(),
    )?;
    let mut store = writer.serving_store_fixture()?;
    let initial = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let f = Fixture::new()?;
    let mut publisher = f.publisher()?;
    assert!(f.publisher().is_err());
    assert!(publisher.publish_next(&initial)?);
    assert!(!publisher.publish_next(&initial)?);
    let first = f.catalog(None)?;
    let preceding = fs::read(f.current())?;
    writer.append_settled_blocks(&chain)?;
    let mut follower = initial
        .into_secondary()
        .map_err(|_| eyre!("fixture owns one reader"))?;
    follower.try_catch_up()?;
    let latest = RocksDbCanonicalReader::new(follower)?;
    assert!(publisher.publish_next(&latest)?);
    assert!(publisher.publish_next(&latest)?);
    assert!(!publisher.publish_next(&latest)?);
    drop(publisher);
    let mut restarted = f.publisher()?;
    assert!(!restarted.publish_next(&latest)?);

    let path = segment_path(f.directory.path(), f.entries()?[0].sha256);
    let retired = path.with_extension("retired");
    fs::rename(&path, &retired)?;
    let extended = f.catalog(Some(&first))?;
    assert_eq!(extended.bounds().end, BlockHeight::new(6));
    assert!(Arc::ptr_eq(&first.segments()[0], &extended.segments()[0]));
    assert!(f.catalog(None).is_err());
    extended.verify_source(&latest).await?;
    fs::rename(retired, path)?;
    drop(restarted);
    fs::write(f.current(), preceding)?;
    assert!(f.catalog(Some(&extended)).is_err());
    assert!(f.publisher().is_err());
    Ok(())
}

#[tokio::test]
async fn managed_history_requires_the_trusted_checkpoint_on_every_restart() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let (_files, canonical) = reader(&chain)?;
    let f = Fixture::new()?;
    let other = Fixture::new()?;
    let trusted = tempdir()?;
    let public = trusted.path().join("publisher.pub");
    fs::copy(&f.public, &public)?;
    let follow = || {
        super::super::PublishedShieldedHistory::follow_publisher(
            f.directory.path(),
            &public,
            false,
            &canonical,
        )
    };
    assert!(follow().await.is_err());
    let mut publisher = f.publisher()?;
    assert!(
        super::super::PublishedShieldedHistory::follow_publisher(
            f.directory.path(),
            &f.public,
            false,
            &canonical
        )
        .await
        .is_err()
    );
    let waiting = follow().await?;
    assert!(waiting.encoded_range().0.is_none());
    assert!(
        super::super::PublishedShieldedHistory::follow_publisher(
            f.directory.path(),
            &other.public,
            false,
            &canonical
        )
        .await
        .is_err()
    );
    assert!(
        super::super::PublishedShieldedHistory::open(&f.current(), &f.public, false, &canonical)
            .await
            .is_err()
    );
    assert!(publisher.publish_next(&canonical)?);
    assert!(waiting.refresh(&canonical).await?);
    let (adopted, _) = waiting.encoded_range();
    assert!(adopted.is_some_and(|catalog| catalog.bounds().end == BlockHeight::new(2)));
    let first = fs::read(f.current())?;
    assert!(publisher.publish_next(&canonical)?);
    fs::write(f.current(), &first)?;
    assert!(follow().await.is_err());
    assert!(waiting.refresh(&canonical).await.is_err());
    fs::remove_file(f.current())?;
    assert!(follow().await.is_err());
    assert!(waiting.refresh(&canonical).await.is_err());
    assert!(waiting.encoded_range().0.is_some());
    Ok(())
}

#[tokio::test]
async fn captures_on_either_side_of_first_adoption_keep_their_shared_tail() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let (_files, canonical) = reader(&chain)?;
    let canonical = Arc::new(canonical);
    let fixture = Fixture::new()?;
    let keys = tempdir()?;
    let trusted = keys.path().join("publisher.pub");
    fs::copy(&fixture.public, &trusted)?;
    let mut publisher = fixture.publisher()?;
    let history = super::super::PublishedShieldedHistory::follow_publisher(
        fixture.directory.path(),
        &trusted,
        false,
        canonical.as_ref(),
    )
    .await?;

    let first = history.snapshot();
    let second = history.snapshot();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(publisher.publish_next(&canonical)?);
    assert!(history.refresh(canonical.as_ref()).await?);

    let (old_catalog, old_tail) = first.encoded_range();
    let (other_catalog, other_tail) = second.encoded_range();
    assert!(old_catalog.is_none() && other_catalog.is_none());
    let (new_catalog, new_tail) = history.encoded_range();
    assert!(new_catalog.is_some());
    let slot = crate::CanonicalReaderSlot::new(canonical.clone());
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
    let filled = old_tail
        .load_or_refresh(
            (
                &slot,
                &canonical,
                &tokio_util::sync::CancellationToken::new(),
            ),
            None,
            range,
            super::super::tail::TailReadLimits {
                ascending_bytes: None,
                refresh_wait: crate::DEFAULT_BLOCK_RANGE_ADMISSION_WAIT,
            },
        )
        .await?
        .ok_or_else(|| eyre!("missing bootstrap rows"))?;
    assert!(
        other_tail
            .load_full()
            .is_some_and(|tail| Arc::ptr_eq(&tail, &filled))
    );
    assert!(new_tail.load_full().is_none());
    Ok(())
}

#[test]
fn publisher_recovers_the_signed_extension_after_interrupted_checkpoint_installation() -> Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let (_reader_files, reader) = reader(&chain)?;
    let f = Fixture::new()?;
    let mut publisher = f.publisher()?;
    assert!(publisher.publish_next(&reader)?);
    let old = f.catalog(None)?;
    let checkpoint = f.directory.path().join(".publisher-head.json");
    let earlier = fs::read(&checkpoint)?;
    fs::remove_file(&checkpoint)?;
    fs::create_dir(&checkpoint)?;
    assert!(publisher.publish_next(&reader).is_err());
    let latest = f.catalog(Some(&old))?;
    assert_eq!(latest.bounds().end, BlockHeight::new(4));
    drop(publisher);
    fs::remove_dir(&checkpoint)?;
    fs::write(checkpoint, earlier)?;
    let mut recovered = f.publisher()?;
    assert!(recovered.publish_next(&reader)?);
    assert!(!recovered.publish_next(&reader)?);
    drop(recovered);
    let final_catalog = f.catalog(Some(&latest))?;
    assert_eq!(final_catalog.bounds().end, BlockHeight::new(6));
    let last = f
        .entries()?
        .pop()
        .ok_or_else(|| eyre!("expected segment"))?;
    fs::remove_file(segment_path(f.directory.path(), last.sha256))?;
    assert!(f.publisher().is_err());
    assert!(f.catalog(Some(&latest)).is_err());
    Ok(())
}

#[test]
fn publisher_recovers_its_staging_files_after_an_interrupted_write() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let (_files, canonical) = reader(&chain)?;
    let fixture = Fixture::new()?;
    let mut first = fixture.publisher()?;
    assert!(first.publish_next(&canonical)?);
    let dir = fixture.directory.path();
    let head = dir.join(".publisher-head.json");
    let checkpoint = fs::read(&head)?;
    let current = fs::read(fixture.current())?;
    let installed = segment_path(dir, fixture.entries()?[0].sha256);
    let retained = fs::read(&installed)?;
    let partial = dir.join(".publisher-write.tmp");
    let staged_segment = dir.join(".publisher-segment.tmp");
    fs::write(&partial, b"interrupted write")?;
    fs::hard_link(&installed, &staged_segment)?;
    let operator_file = dir.join(".operator-backup");
    fs::write(&operator_file, b"private backup")?;

    assert!(fixture.publisher().is_err());
    assert!(partial.exists());
    assert!(staged_segment.exists());
    drop(first);

    let mut resumed = fixture.publisher()?;
    assert!(!partial.exists());
    assert!(!staged_segment.exists());
    assert_eq!(fs::read(operator_file)?, b"private backup");
    assert_eq!(fs::read(installed)?, retained);
    assert_eq!(fs::read(head)?, checkpoint);
    assert_eq!(fs::read(fixture.current())?, current);
    assert!(resumed.publish_next(&canonical)?);
    assert_eq!(fixture.catalog(None)?.bounds().end, BlockHeight::new(4));
    Ok(())
}

#[test]
fn initial_publication_creates_and_reopens_nested_directories() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let (_canonical_files, canonical) = reader(&chain)?;
    let fixture = Fixture::new()?;
    let nested = fixture.directory.path().join("artifacts/ranges");
    let first = BlockHeight::new(1);
    let width = NonZeroU32::MIN;
    let mut publisher = ShieldedCatalogPublisher::open(&nested, &fixture.private, first, width)?;
    assert!(publisher.publish_next(&canonical)?);
    drop(publisher);
    let catalog = PublishedShieldedCatalog::open_signed(
        &nested.join("current.json"),
        &verification_key_from(&fixture.public)?,
        false,
        None,
        None,
    )?;
    assert_eq!(catalog.bounds().end, first);
    Ok(())
}

#[test]
fn a_damaged_orphan_cannot_replace_the_last_good_pointer() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let (_canonical_files, canonical) = reader(&chain)?;
    let fixture = Fixture::new()?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    let previous_pointer = fs::read(fixture.current())?;
    let dir = fixture.directory.path();
    let next = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(4));
    let description = manifest::publish_segment(&canonical, dir, next)?;
    let orphan = segment_path(dir, description.identity.sha256);
    fs::remove_file(&orphan)?;
    fs::write(&orphan, b"partial segment")?;
    assert!(publisher.publish_next(&canonical).is_err());
    assert_eq!(fs::read(fixture.current())?, previous_pointer);
    assert_eq!(fixture.catalog(None)?.bounds().end, BlockHeight::new(2));
    fs::remove_file(orphan)?;
    assert!(publisher.publish_next(&canonical)?);
    assert_eq!(fixture.catalog(None)?.bounds().end, BlockHeight::new(4));
    Ok(())
}

#[tokio::test]
async fn adjacent_signed_files_from_disconnected_chains_and_wrong_keys_are_rejected() -> Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let fork = chain.fork_at(BlockHeight::new(2))?.extend_blocks(3);
    let (_canonical_files, canonical) = reader(&chain)?;
    let (_other_files, other) = reader(&fork)?;
    let f = Fixture::new()?;
    let mut publisher = f.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    let old = f.catalog(None)?;
    assert!(old.verify_source(&other).await.is_err());
    assert!(publisher.publish_next(&other).is_err());
    let first = f.entries()?;
    let bad = manifest::publish_segment(
        &other,
        f.directory.path(),
        BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(4)),
    )?;
    let bad = bad.identity();
    let mut entries = first.clone();
    entries.push(bad);
    f.force_catalog(entries)?;
    assert!(f.catalog(Some(&old)).is_err());
    let wrong = Fixture::new()?;
    assert!(
        PublishedShieldedCatalog::open_signed(
            &f.current(),
            &verification_key_from(&wrong.public)?,
            false,
            None,
            None
        )
        .is_err()
    );
    let mut gapped = first;
    let mut out_of_order = bad;
    out_of_order.first = 4;
    gapped.push(out_of_order);
    f.force_catalog(gapped)?;
    assert!(f.catalog(None).is_err());
    Ok(())
}

#[test]
fn missing_publisher_head_rejects_an_older_signed_pointer() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let (_canonical_files, canonical) = reader(&chain)?;
    let fixture = Fixture::new()?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    let old = fs::read(fixture.current())?;
    assert!(publisher.publish_next(&canonical)?);
    drop(publisher);
    fs::write(fixture.current(), &old)?;
    fs::remove_file(fixture.directory.path().join(".publisher-head.json"))?;
    assert!(fixture.publisher().is_err());
    assert_eq!(fs::read(fixture.current())?, old);
    Ok(())
}

#[test]
fn required_verity_rejects_an_ordinary_signed_segment() -> Result<()> {
    let fixture = Fixture::new()?;
    let (_canonical_files, canonical) =
        reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(2))?;
    assert!(fixture.publisher()?.publish_next(&canonical)?);
    let key = verification_key_from(&fixture.public)?;
    let segment = segment_path(fixture.directory.path(), fixture.entries()?[0].sha256);
    let ordinary = segment.with_extension("copy");
    fs::copy(&segment, &ordinary)?;
    fs::rename(ordinary, segment)?;
    assert!(fixture.catalog(None).is_ok());
    assert!(
        PublishedShieldedCatalog::open_signed(&fixture.current(), &key, true, None, None).is_err()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem"]
fn kernel_protected_catalog_requires_every_segment_before_admission() -> Result<()> {
    let fixture = Fixture::new()?;
    let (_canonical_files, canonical) =
        reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(4))?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    assert!(publisher.publish_next(&canonical)?);
    let key = verification_key_from(&fixture.public)?;
    let open = || PublishedShieldedCatalog::open_signed(&fixture.current(), &key, true, None, None);
    let catalog = open()?;
    assert_eq!(catalog.bounds().end, BlockHeight::new(4));
    let mut entries = fixture.entries()?;
    let path = segment_path(fixture.directory.path(), entries[1].sha256);
    assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
    entries[1].fs_verity_sha256 = Some([0; 32]);
    fixture.force_catalog(entries)?;
    assert!(open().is_err());
    assert!(fixture.catalog(None).is_err());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem"]
fn kernel_protected_publication_recovers_portable_orphans() -> Result<()> {
    let fixture = Fixture::new()?;
    let (_canonical_files, canonical) =
        reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(4))?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    let dir = fixture.directory.path();
    let next = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(4));
    let description = manifest::publish_segment(&canonical, dir, next)?;
    let destination = segment_path(dir, description.identity.sha256);
    let ordinary_copy = dir.join("portable-backup");
    fs::copy(&destination, &ordinary_copy)?;
    fs::remove_file(&destination)?;
    fs::hard_link(&ordinary_copy, &destination)?;
    fs::hard_link(&ordinary_copy, dir.join(".publisher-segment.tmp"))?;

    assert!(publisher.publish_next(&canonical)?);
    let key = verification_key_from(&fixture.public)?;
    let catalog =
        PublishedShieldedCatalog::open_signed(&fixture.current(), &key, true, None, None)?;
    assert_eq!(catalog.bounds().end, BlockHeight::new(4));
    assert!(
        fs::OpenOptions::new()
            .write(true)
            .open(&destination)
            .is_err()
    );
    fs::OpenOptions::new().write(true).open(ordinary_copy)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn catalog_keys_manifests_and_segments_reject_a_pipe_without_a_writer() -> Result<()> {
    let files = Fixture::new()?;
    let pipe = files.directory.path().join("pipe");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&pipe)
            .status()?
            .success()
    );
    assert_eq!(
        verification_key_from(&pipe).err().map(|err| err.kind()),
        Some(io::ErrorKind::InvalidData)
    );
    assert_eq!(
        manifest::signed_contents(&pipe, &[0; 32], CATALOG_LIMIT)
            .err()
            .map(|err| err.kind()),
        Some(io::ErrorKind::InvalidData)
    );
    let (_paths, canonical) = reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(2))?;
    assert!(files.publisher()?.publish_next(&canonical)?);
    let entry = files
        .entries()?
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("missing segment"))?;
    fs::rename(&pipe, segment_path(files.directory.path(), entry.sha256))?;
    assert_eq!(
        files.catalog(None).err().map(|err| err.kind()),
        Some(io::ErrorKind::InvalidData)
    );
    Ok(())
}

#[tokio::test]
async fn encoded_tail_reads_each_generation_from_its_own_store() -> Result<()> {
    use zinder_store::HeightOrder;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let extension = chain.clone().extend_blocks(2);
    let fork = extension.fork_at(BlockHeight::new(6))?.extend_blocks(3);
    let fixture = Fixture::new()?;
    let (_old, initial) = reader(&chain)?;
    let (_new, extended) = reader(&extension)?;
    let (_fork, replacement) = reader(&fork)?;
    assert!(fixture.publisher()?.publish_next(&initial)?);
    let catalog = fixture.catalog(None)?;
    let mut retained = None;
    for source in [initial, extended, replacement].map(Arc::new) {
        let requested = BlockHeightRange::inclusive(
            BlockHeight::new(1),
            source.chain_epoch().visible_tip_height,
        );
        let tail = fill_tail(&catalog, &source, requested).await?;
        let span = tail.bounds().ok_or_else(|| eyre!("empty tail"))?;
        assert_eq!(span.start, BlockHeight::new(3));
        assert_eq!(span.end, source.chain_epoch().visible_tip_height);
        let rows = tail.select(span, HeightOrder::Ascending, usize::MAX, true);
        for row in &rows {
            let block = source
                .compact_blocks_in_range(BlockHeightRange::inclusive(row.id.height, row.id.height))
                .await?
                .pop()
                .ok_or_else(|| eyre!("missing canonical block"))?;
            let canonical =
                crate::compact_block_messages::CompactBlockShape::for_range(&[])?.message(block)?;
            assert_eq!(
                row.message.payload().as_ref(),
                prost::Message::encode_to_vec(&canonical)
            );
        }
        if let Some(prior) = &retained {
            let prior: &prost::bytes::Bytes = prior;
            assert_ne!(
                prior.as_ptr(),
                rows[0].message.as_length_prefixed().as_ptr()
            );
        }
        retained = Some(rows[0].message.as_length_prefixed().clone());
        for order in [HeightOrder::Ascending, HeightOrder::Descending] {
            assert_eq!(tail.select(span, order, 0, true).len(), 1);
            assert!(tail.select(span, order, 0, false).is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_signed_extension_prunes_covered_tail_rows_at_the_same_fence() -> Result<()> {
    use zinder_store::HeightOrder;
    let fixture = Fixture::new()?;
    let (_store, source) = reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(6))?;
    let source = Arc::new(source);
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&source)?);
    let first = fixture.catalog(None)?;
    let original = fill_tail(
        &first,
        &source,
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6)),
    )
    .await?;

    assert!(publisher.publish_next(&source)?);
    let extension = fixture.catalog(Some(&first))?;
    let retained = extension
        .tail
        .load_full()
        .ok_or_else(|| eyre!("missing tail"))?;
    let suffix = BlockHeightRange::inclusive(BlockHeight::new(5), BlockHeight::new(6));
    assert_eq!(retained.bounds(), Some(suffix));
    assert_eq!(
        original.bounds().map(|span| span.start),
        Some(BlockHeight::new(3))
    );
    let reused = retained.select(suffix, HeightOrder::Ascending, usize::MAX, true);
    let previous = original.select(suffix, HeightOrder::Ascending, usize::MAX, true);
    assert_eq!(reused.len(), 2);
    assert!(
        previous
            .iter()
            .zip(reused)
            .all(|(old, row)| old.message.as_length_prefixed().as_ptr()
                == row.message.as_length_prefixed().as_ptr())
    );

    assert!(publisher.publish_next(&source)?);
    let complete = fixture.catalog(Some(&extension))?;
    assert_eq!(
        complete.tail.load_full().and_then(|tail| tail.bounds()),
        None
    );
    Ok(())
}

#[tokio::test]
async fn descending_tail_chunk_defers_an_oversized_catalog_row_until_its_own_read() -> Result<()> {
    use crate::{
        CanonicalReaderSlot,
        block_methods::{ChunkRead, ChunkReadFailure, ChunkView, RangeSource as _},
        block_range::RangeBlock as _,
        compact_block_messages::CompactBlockShape,
        published_range::PublishedRangeRead,
    };
    use zinder_store::HeightOrder;
    let fixture = Fixture::new()?;
    let (_store, source) = reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(6))?;
    let source = Arc::new(source);
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&source)?);
    assert!(publisher.publish_next(&source)?);
    let segment = segment_path(fixture.directory.path(), fixture.entries()?[1].sha256);
    let ordinary = segment.with_extension("copy");
    fs::copy(&segment, &ordinary)?;
    fs::rename(ordinary, &segment)?;
    let catalog = Arc::new(fixture.catalog(None)?);
    let fifth = BlockHeight::new(5);
    let tail = fill_tail(
        &catalog,
        &source,
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6)),
    )
    .await?;
    let row = tail
        .select(
            BlockHeightRange::inclusive(fifth, fifth),
            HeightOrder::Descending,
            0,
            true,
        )
        .pop()
        .ok_or_else(|| eyre!("missing tail row at height 5"))?;
    fs::OpenOptions::new()
        .write(true)
        .open(segment)?
        .set_len(0)?;

    let range = PublishedRangeRead::new(
        Some(catalog.clone()),
        catalog.tail(),
        CompactBlockShape::for_range(&[])?,
        true,
        crate::block_range::BlockRangeStreamLimit::new(NonZeroU32::MIN),
    );
    let slot = CanonicalReaderSlot::new(source.clone());
    let read = |first, last, max_bytes| {
        range.read(
            &slot,
            ChunkRead {
                chunk: BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last)),
                order: HeightOrder::Descending,
                max_bytes,
                view: ChunkView::Current,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
            |_, rows| rows.into_iter().map(|row| row.height()).collect::<Vec<_>>(),
        )
    };
    for first in [1, 4] {
        let answer = read(first, 5, row.native_bytes + 1).await;
        assert!(matches!(answer.converted, Ok(heights) if heights == [fifth]));
    }
    let damaged = read(4, 4, 1).await;
    assert!(
        matches!(damaged.converted, Err(ChunkReadFailure::Failed(status)) if status.code() == tonic::Code::DataLoss)
    );
    Ok(())
}

#[derive(Clone)]
struct HashGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct ObservedRangeReads {
    slot: crate::compact_blocks_grpc::CanonicalReaderSlot,
    epoch_override: Arc<parking_lot::Mutex<Option<zinder_core::ChainEpoch>>>,
    capture_calls: Arc<std::sync::atomic::AtomicUsize>,
    hash_calls: Arc<std::sync::atomic::AtomicUsize>,
    native_calls: Arc<std::sync::atomic::AtomicUsize>,
    hash_gate: Option<HashGate>,
}

impl ObservedRangeReads {
    fn new(reader: Arc<RocksDbCanonicalReader>) -> Self {
        Self {
            slot: crate::compact_blocks_grpc::CanonicalReaderSlot::new(reader),
            epoch_override: Arc::new(parking_lot::Mutex::new(None)),
            capture_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            native_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_gate: None,
        }
    }

    fn set_settled_tip(&self, height: u32, hash: zinder_core::BlockHash) {
        let mut epoch = self.slot.capture().chain_epoch();
        epoch.settled_tip_height = BlockHeight::new(height);
        epoch.settled_tip_hash = hash;
        *self.epoch_override.lock() = Some(epoch);
    }
}

impl crate::block_methods::CompactBlockReads for ObservedRangeReads {
    type View = (Arc<RocksDbCanonicalReader>, zinder_core::ChainEpoch);

    fn capture_view(&self) -> impl Future<Output = std::result::Result<Self::View, tonic::Status>> {
        self.capture_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let reader = self.slot.capture();
        let epoch = (*self.epoch_override.lock()).unwrap_or_else(|| reader.chain_epoch());
        std::future::ready(Ok((reader, epoch)))
    }

    fn view_chain_epoch(view: &Self::View) -> zinder_core::ChainEpoch {
        view.1
    }

    fn range_fence(
        &self,
        view: &Self::View,
    ) -> Option<(
        zinder_store::CanonicalEventFence,
        zinder_core::CanonicalHistoryBounds,
    )> {
        self.slot.range_fence(&view.0)
    }

    fn view_activations(
        &self,
        view: &Self::View,
    ) -> impl Future<
        Output = std::result::Result<zinder_core::NetworkUpgradeActivations, tonic::Status>,
    > + Send {
        self.slot.view_activations(&view.0)
    }

    fn tip_block_time(
        &self,
        view: &Self::View,
    ) -> impl Future<Output = std::result::Result<i64, tonic::Status>> + Send {
        self.slot.tip_block_time(&view.0)
    }

    fn read_compact_block(
        &self,
        view: &Self::View,
        block: crate::block_methods::RequestedBlock,
    ) -> impl Future<Output = std::result::Result<zinder_core::CompactBlockArtifact, tonic::Status>> + Send
    {
        self.slot.read_compact_block(&view.0, block)
    }

    async fn block_hash_at(
        &self,
        view: &Self::View,
        height: BlockHeight,
    ) -> std::result::Result<zinder_core::BlockHash, tonic::Status> {
        let call = self
            .hash_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if call == 0
            && let Some(gate) = &self.hash_gate
        {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.slot.block_hash_at(&view.0, height).await
    }

    async fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: crate::block_methods::ChunkRead<Self::View>,
        convert: Convert,
    ) -> crate::block_methods::ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(zinder_core::ChainEpoch, Vec<zinder_core::CompactBlockArtifact>) -> Output
            + Send
            + 'static,
    {
        self.native_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mapped_view = match read.view {
            crate::block_methods::ChunkView::Captured((reader, _)) => {
                crate::block_methods::ChunkView::Captured(reader)
            }
            crate::block_methods::ChunkView::Current => crate::block_methods::ChunkView::Current,
        };
        let forwarded = crate::block_methods::ChunkRead {
            chunk: read.chunk,
            order: read.order,
            max_bytes: read.max_bytes,
            view: mapped_view,
            cancel: read.cancel,
        };
        self.slot.read_compact_block_chunk(forwarded, convert).await
    }
}

fn synthetic_tip_hash(tag: u32) -> zinder_core::BlockHash {
    let mut raw = [9_u8; 32];
    raw[..4].copy_from_slice(&tag.to_le_bytes());
    zinder_core::BlockHash::from_bytes(raw)
}

async fn setup_gated_anchor_fixture() -> Result<(
    Arc<PublishedShieldedCatalog>,
    ObservedRangeReads,
    HashGate,
    (Arc<RocksDbCanonicalReader>, zinder_core::ChainEpoch),
)> {
    use crate::block_methods::CompactBlockReads as _;

    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let (_store, source) = reader(&chain)?;
    let fixture = Fixture::new()?;
    assert!(fixture.publisher()?.publish_next(&source)?);
    let catalog = Arc::new(fixture.catalog(None)?);
    let gate = HashGate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let mut reads = ObservedRangeReads::new(Arc::new(source));
    reads.hash_gate = Some(gate.clone());
    reads.set_settled_tip(3, synthetic_tip_hash(3));
    let view = reads.capture_view().await?;
    Ok((catalog, reads, gate, view))
}

#[tokio::test]
async fn anchor_cache_is_single_flight_and_keeps_lru_of_16_tips() -> Result<()> {
    use crate::block_methods::CompactBlockReads as _;

    let (catalog, reads, gate, view) = setup_gated_anchor_fixture().await?;
    let no_cancel = tokio_util::sync::CancellationToken::new();
    let leader_catalog = catalog.clone();
    let leader_reads = reads.clone();
    let leader_view = view.clone();
    let leader_cancel = no_cancel.clone();
    let leader = tokio::spawn(async move {
        leader_catalog
            .terminal()
            .check_view(&leader_reads, &leader_view, &leader_cancel)
            .await
    });
    gate.entered.notified().await;

    let mut followers = tokio::task::JoinSet::new();
    for _ in 0..15 {
        let (fc, fr, fv, ft) = (
            catalog.clone(),
            reads.clone(),
            view.clone(),
            no_cancel.clone(),
        );
        followers.spawn(async move { fc.terminal().check_view(&fr, &fv, &ft).await });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    gate.release.notify_waiters();
    leader.await??;
    while let Some(joined) = followers.join_next().await {
        joined??;
    }
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    reads.set_settled_tip(4, synthetic_tip_hash(4));
    let view_four = reads.capture_view().await?;
    catalog
        .terminal()
        .check_view(&reads, &view_four, &no_cancel)
        .await?;
    for cycle in [view.clone(), view_four.clone(), view.clone()] {
        catalog
            .terminal()
            .check_view(&reads, &cycle, &no_cancel)
            .await?;
    }
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        2
    );

    // Fill 15 newer tips while `view` (tip 3) was touched after `view_four` (tip 4).
    for tip in 5_u32..20 {
        reads.set_settled_tip(tip, synthetic_tip_hash(tip));
        let newer = reads.capture_view().await?;
        catalog
            .terminal()
            .check_view(&reads, &newer, &no_cancel)
            .await?;
    }
    let after_fill = reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed);
    catalog
        .terminal()
        .check_view(&reads, &view, &no_cancel)
        .await?;
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        after_fill
    );
    catalog
        .terminal()
        .check_view(&reads, &view_four, &no_cancel)
        .await?;
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        after_fill + 1
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_anchor_waiter_exits_mutex_queue_without_extra_hash_read() -> Result<()> {
    use crate::block_methods::CompactBlockReads as _;

    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let (_store, source) = reader(&chain)?;
    let fixture = Fixture::new()?;
    assert!(fixture.publisher()?.publish_next(&source)?);
    let catalog = Arc::new(fixture.catalog(None)?);
    let gate = HashGate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let mut reads = ObservedRangeReads::new(Arc::new(source));
    reads.hash_gate = Some(gate.clone());
    reads.set_settled_tip(3, synthetic_tip_hash(3));
    let view = reads.capture_view().await?;

    let leader_catalog = catalog.clone();
    let leader_reads = reads.clone();
    let leader_view = view.clone();
    let leader = tokio::spawn(async move {
        leader_catalog
            .terminal()
            .check_view(
                &leader_reads,
                &leader_view,
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
    });
    gate.entered.notified().await;

    let follower_cancel = tokio_util::sync::CancellationToken::new();
    let follower_catalog = catalog.clone();
    let follower_reads = reads.clone();
    let follower_token = follower_cancel.clone();
    let follower = tokio::spawn(async move {
        follower_catalog
            .terminal()
            .check_view(&follower_reads, &view, &follower_token)
            .await
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    follower_cancel.cancel();
    let follower_outcome = follower.await?;
    assert!(
        matches!(follower_outcome, Err(status) if status.code() == tonic::Code::Cancelled),
        "cancelled waiter must leave anchor queue immediately"
    );

    gate.release.notify_waiters();
    leader.await??;
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    Ok(())
}

#[tokio::test]
async fn catalog_chunk_uses_range_fence_without_hash_probe_and_falls_back_below_floor() -> Result<()>
{
    use zinder_store::HeightOrder;

    use crate::block_methods::RangeSource as _;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(6);
    let baseline = CanonicalBaseline::new(&chain);
    let activations = sample_regtest_upgrade_activations();
    let (_full_store, full_source) = reader(&chain)?;
    let fixture = Fixture::new()?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&full_source)?);
    assert!(publisher.publish_next(&full_source)?);
    let catalog = Arc::new(fixture.catalog(None)?);
    assert_eq!(catalog.bounds().end, BlockHeight::new(4));

    let mut floored_store = CanonicalWriterFixture::publish(
        &baseline.with_history_floor(BlockHeight::new(1)),
        &activations,
    )?
    .into_serving_store_fixture()?;
    let floored_reader = Arc::new(RocksDbCanonicalReader::new(
        floored_store.take_readers()?.0,
    )?);
    let reads = ObservedRangeReads::new(floored_reader);
    let range = crate::published_range::PublishedRangeRead::new(
        Some(catalog.clone()),
        catalog.tail(),
        crate::compact_block_messages::CompactBlockShape::for_range(&[])?,
        false,
        crate::block_range::BlockRangeStreamLimit::new(NonZeroU32::MIN),
    );

    let read_chunk = |first, last| {
        range.read(
            &reads,
            crate::block_methods::ChunkRead {
                chunk: BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last)),
                order: HeightOrder::Ascending,
                max_bytes: usize::MAX,
                view: crate::block_methods::ChunkView::Current,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
            |_, rows| rows.len(),
        )
    };
    let covered = read_chunk(2, 4).await;
    assert!(matches!(covered.converted, Ok(3)));
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        reads
            .native_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    let repeat = read_chunk(2, 4).await;
    assert!(matches!(repeat.converted, Ok(3)));
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        reads
            .native_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    let below_floor = read_chunk(1, 2).await;
    assert!(below_floor.converted.is_err());
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        reads
            .native_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    Ok(())
}

#[tokio::test]
async fn precancelled_published_chunk_read_returns_cancelled_without_storage_io() -> Result<()> {
    use crate::block_methods::{ChunkReadFailure, RangeSource as _};
    use zinder_store::HeightOrder;

    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let (_store, source) = reader(&chain)?;
    let fixture = Fixture::new()?;
    assert!(fixture.publisher()?.publish_next(&source)?);
    let catalog = Arc::new(fixture.catalog(None)?);
    let reads = ObservedRangeReads::new(Arc::new(source));
    let range = crate::published_range::PublishedRangeRead::new(
        Some(catalog.clone()),
        catalog.tail(),
        crate::compact_block_messages::CompactBlockShape::for_range(&[])?,
        false,
        crate::block_range::BlockRangeStreamLimit::new(NonZeroU32::MIN),
    );
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();

    let cancelled = range
        .read(
            &reads,
            crate::block_methods::ChunkRead {
                chunk: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
                order: HeightOrder::Ascending,
                max_bytes: usize::MAX,
                view: crate::block_methods::ChunkView::Current,
                cancel,
            },
            |_, rows| rows.len(),
        )
        .await;
    assert!(matches!(
        cancelled.converted,
        Err(ChunkReadFailure::Failed(status)) if status.code() == tonic::Code::Cancelled
    ));
    assert_eq!(
        reads
            .capture_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        reads.hash_calls.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        reads
            .native_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    Ok(())
}

/// The blocks-only adapter's canonical reads, recording the heights each
/// live-tail row read and each canonical chunk read asks for.
#[derive(Clone)]
struct RecordedSlot {
    slot: crate::CanonicalReaderSlot,
    rows: Arc<parking_lot::Mutex<Vec<BlockHeightRange>>>,
    native: Arc<parking_lot::Mutex<Vec<BlockHeightRange>>>,
}

impl crate::block_methods::CompactBlockReads for RecordedSlot {
    type View = Arc<RocksDbCanonicalReader>;

    fn capture_view(
        &self,
    ) -> impl Future<Output = std::result::Result<Self::View, tonic::Status>> + Send {
        self.slot.capture_view()
    }

    fn view_chain_epoch(view: &Self::View) -> zinder_core::ChainEpoch {
        crate::CanonicalReaderSlot::view_chain_epoch(view)
    }

    fn range_fence(
        &self,
        view: &Self::View,
    ) -> Option<(
        zinder_store::CanonicalEventFence,
        zinder_core::CanonicalHistoryBounds,
    )> {
        self.slot.range_fence(view)
    }

    fn tail_source(&self, view: &Self::View) -> Option<zinder_query::CompactBlockFence> {
        self.slot.tail_source(view)
    }

    fn view_activations(
        &self,
        view: &Self::View,
    ) -> impl Future<
        Output = std::result::Result<zinder_core::NetworkUpgradeActivations, tonic::Status>,
    > + Send {
        self.slot.view_activations(view)
    }

    fn tip_block_time(
        &self,
        view: &Self::View,
    ) -> impl Future<Output = std::result::Result<i64, tonic::Status>> + Send {
        self.slot.tip_block_time(view)
    }

    fn read_compact_block(
        &self,
        view: &Self::View,
        block: crate::block_methods::RequestedBlock,
    ) -> impl Future<Output = std::result::Result<zinder_core::CompactBlockArtifact, tonic::Status>> + Send
    {
        self.slot.read_compact_block(view, block)
    }

    async fn read_compact_block_rows_with_lengths(
        &self,
        view: &Self::View,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> std::result::Result<Option<Vec<(zinder_core::CompactBlockArtifact, usize)>>, tonic::Status>
    {
        self.rows.lock().push(range);
        self.slot
            .read_compact_block_rows_with_lengths(view, range, max_bytes)
            .await
    }

    async fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: crate::block_methods::ChunkRead<Self::View>,
        convert: Convert,
    ) -> crate::block_methods::ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(zinder_core::ChainEpoch, Vec<zinder_core::CompactBlockArtifact>) -> Output
            + Send
            + 'static,
    {
        self.native.lock().push(read.chunk);
        self.slot.read_compact_block_chunk(read, convert).await
    }
}

/// Streams `first..=last` as the blocks-only adapter does, over `tail`, and
/// returns how many frames it sent.
async fn stream_blocks_only(
    reads: &RecordedSlot,
    tail: &super::super::ShieldedTailBuffer,
    (first, last): (u64, u64),
) -> Result<usize> {
    use tokio_stream::StreamExt as _;
    let block = |height| {
        Some(zinder_proto::compat::lightwalletd::BlockId {
            height,
            hash: Vec::new(),
        })
    };
    let request = zinder_proto::compat::lightwalletd::BlockRange {
        start: block(first),
        end: block(last),
        pool_types: Vec::new(),
    };
    let mut stream = crate::block_methods::BlockMethods::default()
        .published_block_range(reads, &request, (None, tail.clone()))
        .await?;
    let mut frames = 0;
    while let Some(frame) = stream.next().await {
        frame?;
        frames += 1;
    }
    Ok(frames)
}

#[tokio::test]
async fn blocks_only_reads_only_the_requested_rows_around_a_cached_frontier() -> Result<()> {
    const TIP: u32 = 2_100;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(TIP);
    let (_store, reader) = reader(&chain)?;
    assert_eq!(
        reader.chain_epoch().visible_tip_height,
        BlockHeight::new(TIP)
    );
    let reads = RecordedSlot {
        slot: crate::CanonicalReaderSlot::new(Arc::new(reader)),
        rows: Arc::default(),
        native: Arc::default(),
    };
    let span =
        |first, last| BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last));
    let cold = super::super::ShieldedTailBuffer::default();
    for range in [(85, 100), (100, 85)] {
        assert_eq!(stream_blocks_only(&reads, &cold, range).await?, 16);
    }
    assert!(cold.load_full().is_none());
    assert_eq!(*reads.rows.lock(), Vec::<BlockHeightRange>::new());
    assert_eq!(*reads.native.lock(), vec![span(85, 100), span(85, 100)]);

    let tail = super::super::ShieldedTailBuffer::default();
    reads.native.lock().clear();
    assert_eq!(stream_blocks_only(&reads, &tail, (2_085, 2_100)).await?, 16);
    for range in [(85, 100), (100, 85), (2_085, 2_100), (2_100, 2_085)] {
        assert_eq!(stream_blocks_only(&reads, &tail, range).await?, 16);
    }
    assert_eq!(*reads.rows.lock(), vec![span(2_085, 2_100)]);
    assert_eq!(*reads.native.lock(), vec![span(85, 100), span(85, 100)]);

    for range in [(2_069, 2_084), (2_068, 2_053)] {
        assert_eq!(stream_blocks_only(&reads, &tail, range).await?, 16);
    }
    assert_eq!(
        *reads.rows.lock(),
        vec![span(2_085, 2_100), span(2_069, 2_084), span(2_053, 2_068)]
    );
    assert_eq!(reads.native.lock().len(), 2);
    Ok(())
}

/// Streams `first..=last` as an adapter with `catalog` attached does, through
/// `methods`, and returns how many frames it sent.
async fn stream_published(
    methods: &crate::block_methods::BlockMethods,
    reads: &RecordedSlot,
    catalog: &Arc<PublishedShieldedCatalog>,
    (first, last, pool_types): (u64, u64, Vec<i32>),
) -> Result<usize> {
    use tokio_stream::StreamExt as _;
    let block = |height| {
        Some(zinder_proto::compat::lightwalletd::BlockId {
            height,
            hash: Vec::new(),
        })
    };
    let request = zinder_proto::compat::lightwalletd::BlockRange {
        start: block(first),
        end: block(last),
        pool_types,
    };
    let mut stream = methods
        .published_block_range(reads, &request, (Some(catalog.clone()), catalog.tail()))
        .await?;
    let mut frames = 0;
    while let Some(frame) = stream.next().await {
        frame?;
        frames += 1;
    }
    Ok(frames)
}

#[tokio::test]
async fn ranges_that_cannot_use_signed_files_stream_while_every_published_read_permit_is_held()
-> Result<()> {
    use std::time::Duration;
    const TIP: u32 = 2_100;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(TIP);
    let (_store, reader) = reader(&chain)?;
    let reader = Arc::new(reader);
    let fixture = Fixture::new()?;
    let mut publisher = fixture.publisher()?;
    for _ in 0..3 {
        assert!(publisher.publish_next(&reader)?);
    }
    let catalog = Arc::new(fixture.catalog(None)?);
    assert_eq!(catalog.bounds().end, BlockHeight::new(6));
    let reads = RecordedSlot {
        slot: crate::CanonicalReaderSlot::new(reader),
        rows: Arc::default(),
        native: Arc::default(),
    };
    let methods = crate::block_methods::BlockMethods::default();
    let span =
        |first, last| BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last));
    let default_shape = |first, last| (first, last, Vec::new());

    assert_eq!(
        stream_published(&methods, &reads, &catalog, default_shape(2_100, 1_000)).await?,
        1_101
    );
    assert_eq!(
        *reads.rows.lock(),
        vec![span(1_101, 2_100), span(1_000, 1_100)]
    );

    let limit = methods.block_range_streams();
    let held: Vec<_> = std::iter::from_fn(|| limit.try_published_read_permit()).collect();
    assert_eq!(held.len(), 16);
    let prompt = Duration::from_secs(10);
    for range in [default_shape(1_000, 2_100), default_shape(2_100, 1_000)] {
        let frames =
            tokio::time::timeout(prompt, stream_published(&methods, &reads, &catalog, range))
                .await??;
        assert_eq!(frames, 1_101);
    }
    for (first, last) in [(1, 6), (1_000, 1_015)] {
        let transparent = vec![zinder_proto::compat::lightwalletd::PoolType::Transparent as i32];
        let frames = tokio::time::timeout(
            prompt,
            stream_published(&methods, &reads, &catalog, (first, last, transparent)),
        )
        .await??;
        assert_eq!(frames as u64, last - first + 1);
    }
    assert_eq!(
        *reads.rows.lock(),
        vec![span(1_101, 2_100), span(1_000, 1_100)]
    );
    assert_eq!(*reads.native.lock(), vec![span(1, 6), span(1_000, 1_015)]);
    below_catalog_streams_without_permits(&methods, &reads, prompt).await?;

    let queued = Duration::from_millis(200);
    for range in [
        default_shape(1, 6),
        default_shape(5, 20),
        default_shape(20, 5),
    ] {
        assert!(
            tokio::time::timeout(queued, stream_published(&methods, &reads, &catalog, range))
                .await
                .is_err()
        );
    }
    drop(held);
    for (range, expected) in [
        (default_shape(1, 6), 6),
        (default_shape(5, 20), 16),
        (default_shape(20, 5), 16),
    ] {
        let frames =
            tokio::time::timeout(prompt, stream_published(&methods, &reads, &catalog, range))
                .await??;
        assert_eq!(frames, expected);
    }
    Ok(())
}

async fn below_catalog_streams_without_permits(
    methods: &crate::block_methods::BlockMethods,
    reads: &RecordedSlot,
    prompt: std::time::Duration,
) -> Result<()> {
    let fixture = Fixture::new()?;
    let first = BlockHeight::new(1_000);
    assert!(
        fixture
            .publisher_from(first)?
            .publish_next(reads.slot.capture().as_ref())?
    );
    let catalog = Arc::new(fixture.catalog(None)?);
    assert_eq!(catalog.bounds().start, first);
    let before = reads.native.lock().len();
    for (start, end, count) in [(1, 16, 16), (16, 1, 16), (1_001, 999, 3)] {
        let frames = tokio::time::timeout(
            prompt,
            stream_published(methods, reads, &catalog, (start, end, Vec::new())),
        )
        .await??;
        assert_eq!(frames, count);
    }
    let expected = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(16));
    assert_eq!(
        reads.native.lock().get(before..),
        Some([expected, expected, test_span(999, 1_001)].as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn a_chunk_holds_a_published_read_permit_exactly_when_its_plan_can_open_signed_files()
-> Result<()> {
    use crate::{
        CanonicalReaderSlot,
        block_range::{BlockRangeChunks, BlockRangeStreamLimit},
        compact_block_messages::CompactBlockShape,
        published_range::PublishedRangeRead,
    };
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2_010);
    let (_store, reader) = reader(&chain)?;
    let reader = Arc::new(reader);
    let reads = CanonicalReaderSlot::new(Arc::clone(&reader));
    // One- and two-segment catalogs, at the chain start and above it.
    for (first, segments, heights) in [
        (1, 1, 1..=7),
        (1_000, 1, 995..=1_006),
        (1_000, 2, 995..=1_008),
    ] {
        let fixture = Fixture::new()?;
        let mut publisher = fixture.publisher_from(BlockHeight::new(first))?;
        for _ in 0..segments {
            assert!(publisher.publish_next(&reader)?);
        }
        let catalog = Arc::new(fixture.catalog(None)?);
        assert_eq!(catalog.bounds(), test_span(first, first + 2 * segments - 1));
        // Test every read against a contiguous live tail as well as the
        // signed catalog. An unfilled tail can only reduce what can use files.
        fill_tail(&catalog, &reader, test_span(first, 2_010)).await?;
        for is_descending in [false, true] {
            let range = PublishedRangeRead::new(
                Some(catalog.clone()),
                catalog.tail(),
                CompactBlockShape::for_range(&[])?,
                is_descending,
                BlockRangeStreamLimit::new(NonZeroU32::MIN),
            );
            if first == 1_000 && segments == 1 && is_descending {
                let mut chunks = BlockRangeChunks::new(test_span(999, 1_999), true);
                chunks.advance(998);
                assert_eq!(chunks.next_chunk(), Some(test_span(999, 1_001)));
                assert_eq!(
                    chunks.checked_heights().map(|(retry, _)| retry),
                    Some(test_span(1_000, 1_001))
                );
                assert!(assert_published_read_admission(&range, &reads, chunks).await?);
            }
            for start in heights.clone() {
                for end in start..=*heights.end() {
                    let chunk = test_span(start, end);
                    let first = BlockRangeChunks::new(chunk, is_descending);
                    assert_eq!(first.checked_heights(), None);
                    assert_published_read_admission(&range, &reads, first).await?;
                    // Short earlier reads leave up to five checked heights,
                    // enough for a retry that spans the whole catalog.
                    for previously_checked in 0..=5 {
                        let kept = zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE.get()
                            - previously_checked;
                        let previous_range = if is_descending {
                            test_span(start, end + kept)
                        } else if start > kept {
                            test_span(start - kept, end)
                        } else {
                            continue;
                        };
                        let mut later = BlockRangeChunks::new(previous_range, is_descending);
                        later.advance(usize::try_from(kept)?);
                        assert_eq!(later.next_chunk(), Some(chunk));
                        assert_published_read_admission(&range, &reads, later).await?;
                        if let Some((_, only_checked)) = later.checked_heights() {
                            assert_eq!(only_checked.checked_heights(), None);
                            assert_published_read_admission(&range, &reads, only_checked).await?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn assert_published_read_admission(
    range: &crate::published_range::PublishedRangeRead,
    reads: &crate::CanonicalReaderSlot,
    chunks: crate::block_range::BlockRangeChunks,
) -> Result<bool> {
    use crate::block_methods::RangeSource;
    let chunk = chunks.next_chunk().ok_or_else(|| eyre!("missing chunk"))?;
    let retry = chunks.checked_heights().map(|(checked, _)| checked);
    let eligible =
        RangeSource::<crate::CanonicalReaderSlot>::published_read_limit(range, chunk, retry)
            .is_some();
    let whole_reads_files = range.plan_can_read_signed_files(reads, chunk).await?;
    let retry_reads_files = match retry {
        Some(checked) => range.plan_can_read_signed_files(reads, checked).await?,
        None => false,
    };
    assert_eq!(
        eligible,
        whole_reads_files || retry_reads_files,
        "chunk {chunk:?} retry={retry:?} order={:?}",
        chunks.order()
    );
    Ok(retry_reads_files && !whole_reads_files)
}

fn test_span(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}
