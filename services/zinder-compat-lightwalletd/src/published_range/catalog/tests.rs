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
        ShieldedCatalogPublisher::open(
            self.directory.path(),
            &self.private,
            BlockHeight::new(1),
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
    assert!(PublishedShieldedCatalog::open_signed(&fixture.current(), &key, true, None).is_err());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires TMPDIR on a disposable fs-verity filesystem and fsverity on PATH"]
fn kernel_protected_catalog_requires_every_segment_before_admission() -> Result<()> {
    let fixture = Fixture::new()?;
    let (_canonical_files, canonical) =
        reader(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(4))?;
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&canonical)?);
    assert!(publisher.publish_next(&canonical)?);
    let key = verification_key_from(&fixture.public)?;
    let open = || PublishedShieldedCatalog::open_signed(&fixture.current(), &key, true, None);
    for entry in fixture.entries()? {
        assert!(open().is_err());
        let path = segment_path(fixture.directory.path(), entry.sha256);
        assert!(
            std::process::Command::new("fsverity")
                .arg("enable")
                .arg(path)
                .status()?
                .success()
        );
    }
    let catalog = open()?;
    assert_eq!(catalog.bounds().end, BlockHeight::new(4));
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
async fn encoded_tail_reuses_linked_rows_and_replaces_only_the_fork_suffix() -> Result<()> {
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
    for source in [&initial, &extended, &replacement] {
        catalog.refresh_tail(source).await?;
        let tail = catalog
            .tail
            .load_full()
            .ok_or_else(|| eyre!("missing tail"))?;
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
                row.payload.as_ref(),
                prost::Message::encode_to_vec(&canonical)
            );
        }
        if let Some(prior) = &retained {
            let prior: &prost::bytes::Bytes = prior;
            assert_eq!(prior.as_ptr(), rows[0].payload.as_ptr());
        }
        retained = Some(rows[0].payload.clone());
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
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&source)?);
    let first = fixture.catalog(None)?;
    first.refresh_tail(&source).await?;
    let original = first
        .tail
        .load_full()
        .ok_or_else(|| eyre!("missing tail"))?;

    assert!(publisher.publish_next(&source)?);
    let extension = fixture.catalog(Some(&first))?;
    extension.refresh_tail(&source).await?;
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
            .all(|(old, row)| old.payload.as_ptr() == row.payload.as_ptr())
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
    let mut publisher = fixture.publisher()?;
    assert!(publisher.publish_next(&source)?);
    assert!(publisher.publish_next(&source)?);
    let catalog = Arc::new(fixture.catalog(None)?);
    catalog.refresh_tail(&source).await?;
    let fifth = BlockHeight::new(5);
    let tail = catalog
        .tail
        .load_full()
        .ok_or_else(|| eyre!("missing tail"))?;
    let row = tail
        .select(
            BlockHeightRange::inclusive(fifth, fifth),
            HeightOrder::Descending,
            0,
            true,
        )
        .pop()
        .ok_or_else(|| eyre!("missing tail row at height 5"))?;
    let segment = segment_path(
        fixture.directory.path(),
        catalog.terminal().identity().sha256,
    );
    fs::OpenOptions::new()
        .write(true)
        .open(segment)?
        .set_len(0)?;

    let range = PublishedRangeRead::new(catalog, CompactBlockShape::for_range(&[])?, true);
    let slot = CanonicalReaderSlot::new(Arc::new(source));
    let read = |first, last, max_bytes| {
        range.read(
            &slot,
            ChunkRead {
                chunk: BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last)),
                order: HeightOrder::Descending,
                max_bytes,
                view: ChunkView::Current,
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
