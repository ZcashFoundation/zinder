use ring::{rand::SystemRandom, signature::KeyPair};
use zinder_core::{MempoolEvictionReason, Network, UnixTimestampMillis};

use super::*;
use crate::{MempoolEvent, MempoolEventHistoryRequest};
use zinder_testkit::{CanonicalBaseline, CanonicalWriterFixture, ChainFixture};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn publishes_only_after_attestation_and_rotates_cursor_key() -> TestResult {
    let root = tempfile::tempdir()?;
    let (_source_directory, mut source) = admitted_source()?;
    let old_key = source.cursor_auth_key;
    let event = MempoolEvent::Invalidated {
        transaction_id: TransactionId::from_bytes([17; 32]),
        reason: MempoolEvictionReason::Conflict,
    };
    source.append_mempool_event(event, UnixTimestampMillis::new(1))?;
    let signing_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let publisher =
        Ed25519KeyPair::from_pkcs8(signing_key.as_ref()).map_err(|_| "key parsing failed")?;
    let keys = [publisher.public_key().as_ref().try_into()?];
    let bundle = root.path().join("bundle");
    let sealed = root.path().join("opaque");
    fs::create_dir(&sealed)?;
    fs::write(sealed.join("segment"), b"opaque history")?;
    export_edge_snapshot(&mut source, &bundle, signing_key.as_ref(), Some(&sealed))?;
    assert_eq!(old_key, source.cursor_auth_key);
    let history = MempoolEventHistoryRequest::with_default_limit(None);
    assert_eq!(source.mempool_event_history(history)?.len(), 1);
    let activations = source
        .network_upgrade_activations()
        .ok_or("missing schedule")?
        .clone();
    let config = || EdgeSnapshotStoreConfig {
        activations: &activations,
        workload: source.workload(),
        raw_blob_retention: source.raw_blob_retention(),
        reorg_policy: source.reorg_policy(),
        resource_budget: RocksDbResourceBudget::for_local_tests(),
    };
    let target = root.path().join("target");
    let sealed_output = root.path().join("sealed");
    let run = || EdgeSnapshotBootstrap {
        source: &bundle,
        target: &target,
        trusted_keys: &keys,
        spot_check_blocks: DEFAULT_SPOT_CHECK_BLOCKS,
        sealed_history_output: Some(&sealed_output),
    };
    assert!(
        prepare_bootstrapped_store(run(), config(), |_| async {
            Err(refused("validator mismatch"))
        })
        .await
        .is_err()
    );
    assert!(!target.exists());
    assert!(!root.path().join("target.bootstrap.staging").exists());

    check_corrupt_files(&bundle, || {
        let stage = root.path().join("probe");
        create_private_dir(&stage)?;
        let staged = stage_files(&run(), &stage);
        fs::remove_dir_all(stage)?;
        assert!(staged.is_err());
        Ok(())
    })?;

    prepare_bootstrapped_store(run(), config(), |evidence| async move {
        assert_eq!(evidence.checkpoint.block_id.height, BlockHeight::new(4096));
        assert!((32..=33).contains(&evidence.blocks.len()));
        assert!(
            evidence
                .blocks
                .iter()
                .any(|block| block.block == evidence.checkpoint.block_id)
        );
        Ok::<_, EdgeSnapshotError>(())
    })
    .await?;
    let restored = config().open(&target)?;
    assert_ne!(restored.cursor_auth_key, old_key);
    assert_eq!(restored.mempool_event_history(history)?, []);
    assert_eq!(restored.ready_evidence(), source.ready_evidence());
    assert_eq!(fs::read(sealed_output.join("segment"))?, b"opaque history");
    assert!(Workspace::new(&target, ".bootstrap.staging").is_err());
    Ok(())
}

fn admitted_source() -> TestResult<(CanonicalWriterFixture, RocksDbCanonicalStore)> {
    let activations =
        crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let chain = ChainFixture::new(Network::ZcashTestnet).extend_blocks(4096);
    let baseline = CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(4059));
    let mut fixture = CanonicalWriterFixture::publish(&baseline, &activations)
        .map_err(|error| error.to_string())?;
    let external = fixture
        .take_canonical_primary()
        .map_err(|error| error.to_string())?;
    let reorg_policy = CanonicalReorgPolicy::new(external.reorg_policy().reorg_window_blocks())?;
    drop(external);
    let store = RocksDbCanonicalStore::open_ready(
        fixture.canonical_primary_path(),
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        reorg_policy,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    Ok((fixture, store))
}

fn check_corrupt_files(bundle: &Path, mut check: impl FnMut() -> TestResult) -> TestResult {
    let sst = inventory(bundle)?
        .into_iter()
        .find(|(name, _)| Path::new(name).extension() == Some(std::ffi::OsStr::new("sst")))
        .ok_or("no sst")?
        .1;
    let mut modified = fs::read(&sst)?;
    *modified.last_mut().ok_or("empty sst")? ^= 1;
    // Replacing the link preserves the source and the immutable checkpoint.
    let moved = sst.with_extension("backup");
    fs::rename(&sst, &moved)?;
    fs::write(&sst, modified)?;
    let backup = bundle.parent().ok_or("missing parent")?.join("backup");
    fs::rename(moved, &backup)?;
    check()?;
    fs::remove_file(&sst)?;
    check()?;
    fs::rename(backup, &sst)?;
    fs::write(bundle.join("unexpected"), b"extra")?;
    check()?;
    fs::remove_file(bundle.join("unexpected"))?;
    std::os::unix::fs::symlink(&sst, bundle.join("symlink"))?;
    check()?;
    fs::remove_file(bundle.join("symlink"))?;
    Ok(())
}

#[test]
fn signature_and_paths_are_strict() -> TestResult {
    let signing_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let publisher =
        Ed25519KeyPair::from_pkcs8(signing_key.as_ref()).map_err(|_| "key parsing failed")?;
    let key = publisher.public_key().as_ref().try_into()?;
    let block = || BlockId::new(BlockHeight::new(0), Network::ZcashRegtest.genesis_hash()).into();
    let mut manifest = EdgeSnapshotManifest {
        format: FORMAT.into(),
        anchors: ManifestAnchors {
            canonical_schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            network_id: Network::ZcashRegtest.id(),
            genesis_hash: Network::ZcashRegtest.genesis_hash().as_bytes(),
            activation_fingerprint: [0; 32],
            reorg_window_blocks: 2,
            history_floor: block(),
            snapshot: block(),
            trees: [None, None, None],
        },
        files: [
            "CURRENT",
            "MANIFEST-00001",
            CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME,
        ]
        .into_iter()
        .map(|file| SnapshotFile {
            path: format!("{CANONICAL}/{file}"),
            bytes: 0,
            sha256: [0; 32],
        })
        .collect(),
    };
    let bytes = serde_json::to_vec(&manifest)?;
    let signature = publisher.sign(&signature_message(&bytes));
    EdgeSnapshotManifest::verify(&bytes, signature.as_ref(), &[key])?;
    assert!(EdgeSnapshotManifest::verify(&bytes, signature.as_ref(), &[[0; 32]]).is_err());
    assert!(EdgeSnapshotManifest::verify(&bytes, &signature.as_ref()[..63], &[key]).is_err());
    let mut modified = bytes;
    modified.push(b' ');
    assert!(EdgeSnapshotManifest::verify(&modified, signature.as_ref(), &[key]).is_err());
    for path in [
        "../evil",
        "/evil",
        "sealed-history/../evil",
        "canonical-rocksdb/CURRENT",
    ] {
        manifest.files.push(SnapshotFile {
            path: path.into(),
            bytes: 0,
            sha256: [0; 32],
        });
        let bytes = serde_json::to_vec(&manifest)?;
        let signature = publisher.sign(&signature_message(&bytes));
        assert!(EdgeSnapshotManifest::verify(&bytes, signature.as_ref(), &[key]).is_err());
        manifest.files.pop();
    }
    Ok(())
}
