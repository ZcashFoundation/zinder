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
    seed_mempool(&source)?;
    let signing_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let publisher =
        Ed25519KeyPair::from_pkcs8(signing_key.as_ref()).map_err(|_| "key parsing failed")?;
    let keys = [publisher.public_key().as_ref().try_into()?];
    let bundle = root.path().join("bundle");
    let key_file = root.path().join("signer.der");
    fs::write(&key_file, signing_key.as_ref())?;
    export_edge_snapshot(&mut source, &bundle, &key_file)?;
    assert_eq!(old_key, source.cursor_auth_key);
    let history = MempoolEventHistoryRequest::with_default_limit(None);
    assert_eq!(source.mempool_event_history(history)?.len(), 3);
    let activations = source
        .network_upgrade_activations()
        .ok_or("missing schedule")?
        .clone();
    let config = || EdgeSnapshotStoreConfig {
        activations: &activations,
        raw_blob_retention: source.raw_blob_retention(),
        reorg_policy: source.reorg_policy(),
        required_first_available_height: BlockHeight::new(u32::MAX),
        resource_budget: RocksDbResourceBudget::for_local_tests(),
    };
    let target = root.path().join("target");
    let run = || EdgeSnapshotBootstrap {
        source: &bundle,
        trusted_keys: &keys,
        spot_check_blocks: DEFAULT_SPOT_CHECK_BLOCKS,
    };
    let reserve = || EdgeSnapshotDestination::reserve(&target, false);
    assert!(
        prepare_bootstrapped_store(reserve()?, run(), config(), |_| async {
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
        let staged = stage_files(&run(), &stage, &config());
        fs::remove_dir_all(stage)?;
        assert!(staged.is_err());
        Ok(())
    })?;

    prepare_bootstrapped_store(reserve()?, run(), config(), |evidence| async move {
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
    assert!(reserve().is_err());
    Ok(())
}

fn seed_mempool(source: &RocksDbCanonicalStore) -> TestResult {
    for (sequence, seed) in [(1, 17), (2, 18), (3, 19)] {
        let event = MempoolEvent::Invalidated {
            transaction_id: TransactionId::from_bytes([seed; 32]),
            reason: MempoolEvictionReason::Conflict,
        };
        source.append_mempool_event(event, UnixTimestampMillis::new(sequence))?;
    }
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
    fs::create_dir(bundle.join("unsigned-directory"))?;
    fs::write(bundle.join("unsigned-directory").join("unsigned"), b"extra")?;
    check()?;
    fs::remove_dir_all(bundle.join("unsigned-directory"))?;
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
            raw_blob_retention: RawBlobRetention::None,
            reorg_window_blocks: 2,
            history_floor: block(),
            snapshot: block(),
            trees: [None, None, None],
        },
        files: [
            "CURRENT",
            "MANIFEST-00001",
            CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME,
            crate::SEGMENTS_MANIFEST_FILE_NAME,
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
        "canonical-rocksdb/../evil",
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

#[test]
fn reservation_refuses_occupied_target_or_staging_workspace() -> TestResult {
    let root = tempfile::tempdir()?;
    let target = root.path().join("canonical");
    let staging = root.path().join("canonical.bootstrap.staging");
    let download = root.path().join("canonical.bootstrap.download");
    let occupied = root.path().join("occupied");
    fs::create_dir(&occupied)?;
    fs::write(occupied.join("segment"), b"preserved")?;
    let failure = EdgeSnapshotDestination::reserve(&occupied, true);
    assert!(
        matches!(failure, Err(EdgeSnapshotError::Refused(reason)) if reason.contains("occupied"))
    );
    assert_eq!(fs::read(occupied.join("segment"))?, b"preserved");

    fs::create_dir(&staging)?;
    let failure = EdgeSnapshotDestination::reserve(&target, true);
    assert!(failure.is_err());
    assert!(!target.exists());
    assert!(!download.exists());
    assert_eq!(fs::read_dir(&staging)?.count(), 0);
    Ok(())
}

#[test]
fn incompatible_local_manifest_refuses_before_any_payload_is_copied() -> TestResult {
    let root = tempfile::tempdir()?;
    let (_source_directory, mut source) = admitted_source()?;
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let publisher = Ed25519KeyPair::from_pkcs8(key.as_ref()).map_err(|_| "key parsing failed")?;
    let keys = [publisher.public_key().as_ref().try_into()?];
    let key_path = root.path().join("signer.der");
    fs::write(&key_path, key.as_ref())?;
    let bundle = root.path().join("bundle");
    export_edge_snapshot(&mut source, &bundle, &key_path)?;
    let target = root.path().join("restored");
    let destination = EdgeSnapshotDestination::reserve(&target, false)?;
    let activations = NetworkUpgradeActivations::new(Network::ZcashRegtest, Vec::new())?;
    let config = EdgeSnapshotStoreConfig {
        activations: &activations,
        raw_blob_retention: source.raw_blob_retention(),
        reorg_policy: source.reorg_policy(),
        required_first_available_height: BlockHeight::new(u32::MAX),
        resource_budget: RocksDbResourceBudget::for_local_tests(),
    };
    let input = EdgeSnapshotBootstrap {
        source: &bundle,
        trusted_keys: &keys,
        spot_check_blocks: DEFAULT_SPOT_CHECK_BLOCKS,
    };
    let stage = &destination.stage.path;
    let failure = stage_files(&input, stage, &config);
    assert!(
        matches!(failure, Err(EdgeSnapshotError::Refused(reason)) if reason.contains("store identity"))
    );
    let entries = fs::read_dir(stage)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(entries, [WORKSPACE_MARKER]);
    assert!(!target.exists());
    Ok(())
}

#[test]
fn aliased_directory_identities_detect_destination_and_workspace_overlap() {
    let shared = (7, 11);
    let entry = |path, tail| PathIdentity {
        path: Path::new(path),
        deepest: shared,
        ancestors: vec![shared, (7, 1)],
        tail: Path::new(tail),
    };
    let target = entry("/first/canonical", "canonical");
    assert!(target.overlaps(&entry("/second/canonical", "canonical")));
    assert!(target.overlaps(&entry("/second/canonical/sidecar", "canonical/sidecar")));
    let workspace = entry(
        "/second/history.bootstrap.staging",
        "history.bootstrap.staging",
    );
    assert!(
        entry(
            "/first/history.bootstrap.staging",
            "history.bootstrap.staging"
        )
        .overlaps(&workspace)
    );
    assert!(!target.overlaps(&workspace));

    let existing = entry("/first", "");
    let descendant = PathIdentity {
        path: Path::new("/second/child/sidecar"),
        deepest: (7, 12),
        ancestors: vec![(7, 12), shared, (7, 1)],
        tail: Path::new("sidecar"),
    };
    assert!(existing.overlaps(&descendant));
    assert!(descendant.overlaps(&existing));
}

#[test]
fn workspace_cleanup_preserves_a_replacement_directory() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("workspace");
    let work = Workspace::create(path.clone())?;
    assert!(path.join(WORKSPACE_MARKER).is_file());
    assert!(require_empty_or_absent(&path).is_err());
    let outside = root.path().join("outside");
    fs::create_dir(&outside)?;
    fs::write(outside.join("keep"), b"outside")?;
    let subtree = path.join("nested");
    fs::create_dir(&subtree)?;
    fs::write(subtree.join("payload"), b"owned")?;
    std::os::unix::fs::symlink(&outside, subtree.join("link"))?;
    let moved = root.path().join("moved");
    fs::rename(&path, &moved)?;
    fs::create_dir(&path)?;
    fs::write(path.join("keep"), b"replacement")?;
    drop(work);
    assert_eq!(fs::read(path.join("keep"))?, b"replacement");
    assert_eq!(fs::read(outside.join("keep"))?, b"outside");
    assert_eq!(fs::read_dir(&moved)?.count(), 0);
    Ok(())
}

#[test]
fn exported_checkpoint_removes_publisher_keys_leases_and_mempool() -> TestResult {
    use crate::{CanonicalEventHistoryRequest, ProjectionBuildLease, ProjectionBuildLeaseId};

    let root = tempfile::tempdir()?;
    let (_source_directory, mut source) = admitted_source()?;
    let publisher_keys = [[0x91; 32], [0xa7; 32]];
    set_publisher_cursor_keys(&mut source, publisher_keys)?;
    seed_mempool(&source)?;
    let now = UnixTimestampMillis::new(10);
    let anchor = source
        .canonical_event_history(CanonicalEventHistoryRequest::new(None, NonZeroU32::MIN))?
        .first()
        .ok_or("missing baseline event")?
        .projection_build_anchor();
    let lease = source.acquire_projection_build_lease(
        ProjectionBuildLease::new(
            ProjectionBuildLeaseId::from_bytes([9; 16]),
            anchor,
            UnixTimestampMillis::new(20),
        ),
        now,
    )?;
    let history = MempoolEventHistoryRequest::with_default_limit(None);
    let source_events = source.mempool_event_history(history)?;
    let source_control = source.bounded_open.db.get(STORE_CONTROL_KEY)?;
    let db = &source.bounded_open.db;
    db.flush()?;
    db.flush_cf(&column_family(db, MEMPOOL_EVENT_COLUMN_FAMILY)?)?;
    let inherited = sensitive_tables(db)?;
    let signing_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let signer =
        Ed25519KeyPair::from_pkcs8(signing_key.as_ref()).map_err(|_| "key parsing failed")?;
    let key_file = root.path().join("signer.der");
    fs::write(&key_file, signing_key.as_ref())?;
    let bundle = root.path().join("bundle");
    export_edge_snapshot(&mut source, &bundle, &key_file)?;

    let trusted = [signer.public_key().as_ref().try_into()?];
    let manifest = EdgeSnapshotManifest::verify(
        &fs::read(bundle.join(MANIFEST_FILE))?,
        &fs::read(bundle.join(SIGNATURE_FILE))?,
        &trusted,
    )?;
    validate_local_inventory(&bundle, &manifest)?;
    for (path, bytes, digest) in manifest.files() {
        assert_eq!(
            transfer(&bundle.join(path), None, Some(bytes))?,
            (bytes, digest)
        );
    }
    inspect_exported_checkpoint(&bundle.join(CANONICAL), &inherited, &publisher_keys)?;
    assert_eq!(source.cursor_auth_key, publisher_keys[1]);
    assert_eq!(
        source.bounded_open.db.get(STORE_CONTROL_KEY)?,
        source_control
    );
    assert_eq!(source.mempool_event_history(history)?, source_events);
    assert_eq!(
        source.active_projection_build_lease(lease.id(), now)?,
        Some(lease)
    );
    Ok(())
}

fn set_publisher_cursor_keys(
    source: &mut RocksDbCanonicalStore,
    publisher_keys: [[u8; 32]; 2],
) -> TestResult {
    let db = &source.bounded_open.db;
    let original = db
        .get(STORE_CONTROL_KEY)?
        .ok_or("missing source store control")?;
    let control = decode_store_control(db.path(), &original)?;
    for key in publisher_keys {
        db.put(
            STORE_CONTROL_KEY,
            encode_ready_store_control_with_revisions(
                &control.build_plan,
                key,
                &source.ready_evidence(),
                control.revisions(),
            )?,
        )?;
        db.flush()?;
        source.cursor_auth_key = key;
    }
    Ok(())
}

fn sensitive_tables(db: &rust_rocksdb::DB) -> TestResult<BTreeSet<String>> {
    Ok(db
        .live_files()?
        .into_iter()
        .filter(|sst| {
            matches!(
                sst.column_family_name.as_str(),
                "default" | MEMPOOL_EVENT_COLUMN_FAMILY
            )
        })
        .map(|sst| sst.name)
        .collect())
}

fn inspect_exported_checkpoint(
    path: &Path,
    inherited: &BTreeSet<String>,
    publisher_keys: &[[u8; 32]; 2],
) -> TestResult {
    use crate::canonical_store::event_lifecycle::PROJECTION_BUILD_LEASE_PREFIX;
    use rust_rocksdb::{DB, Options};

    let options = Options::default();
    let families = DB::list_cf(&options, path)?;
    let checkpoint = DB::open_cf_for_read_only(&options, path, &families, false)?;
    let encoded = checkpoint
        .get(STORE_CONTROL_KEY)?
        .ok_or("missing exported store control")?;
    let control = decode_store_control(path, &encoded)?;
    assert!(!publisher_keys.contains(&control.cursor_auth_key));
    assert!(checkpoint.get(MEMPOOL_EVENT_SEQUENCE_KEY)?.is_none());
    assert!(checkpoint.get(MEMPOOL_EVENT_RETENTION_FLOOR_KEY)?.is_none());
    assert!(sensitive_tables(&checkpoint)?.is_disjoint(inherited));
    let live = checkpoint.live_files()?;
    for family in families {
        let cf = checkpoint
            .cf_handle(&family)
            .ok_or("missing exported column family")?;
        let mut visible = 0_u64;
        for row in checkpoint.iterator_cf(&cf, IteratorMode::Start) {
            let (key, encoded) = row?;
            assert!(!key.starts_with(PROJECTION_BUILD_LEASE_PREFIX) || family != "default");
            assert_no_publisher_key(&key, publisher_keys);
            assert_no_publisher_key(&encoded, publisher_keys);
            visible += 1;
        }
        if family == MEMPOOL_EVENT_COLUMN_FAMILY {
            assert_eq!(visible, 0);
        }
        if family == "default" {
            // The decoded row scan accounts for compression. The table entry
            // count also rules out hidden superseded rows in this family.
            let entries: u64 = live
                .iter()
                .filter(|sst| sst.column_family_name == family)
                .map(|sst| sst.num_entries)
                .sum();
            assert_eq!(entries, visible);
        }
    }
    for entry in fs::read_dir(path)? {
        let file = entry?.path();
        match file.extension().and_then(std::ffi::OsStr::to_str) {
            Some("sst") => assert_no_publisher_key(&fs::read(file)?, publisher_keys),
            Some("log") => assert_eq!(fs::metadata(file)?.len(), 0),
            _ => {}
        }
    }
    Ok(())
}

fn assert_no_publisher_key(bytes: &[u8], keys: &[[u8; 32]; 2]) {
    for key in keys {
        assert!(!bytes.windows(key.len()).any(|window| window == key));
    }
}

#[test]
fn marker_collision_preserves_unowned_entries() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("workspace");
    let marker = path.join(WORKSPACE_MARKER);
    let payload = path.join("keep");
    let result = Workspace::create_with_marker(path, |directory| {
        fs::write(&marker, b"unowned marker")?;
        fs::write(&payload, b"unowned payload")?;
        create_workspace_marker(directory)
    });
    assert!(
        matches!(result, Err(EdgeSnapshotError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    assert_eq!(fs::read(marker)?, b"unowned marker");
    assert_eq!(fs::read(payload)?, b"unowned payload");
    Ok(())
}

#[test]
fn marker_error_preserves_a_populated_reservation() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("workspace");
    let payload = path.join("keep");
    let result = Workspace::create_with_marker(path, |_| {
        fs::write(&payload, b"unowned payload")?;
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    });
    assert!(
        matches!(result, Err(EdgeSnapshotError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied)
    );
    assert_eq!(fs::read(payload)?, b"unowned payload");
    Ok(())
}

#[test]
fn empty_reservation_failure_allows_immediate_retry() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("workspace");
    let result = Workspace::create_with_marker(path.clone(), |_| {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    });
    assert!(
        matches!(result, Err(EdgeSnapshotError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied)
    );
    assert!(!path.exists());

    let workspace = Workspace::create(path.clone())?;
    assert!(path.join(WORKSPACE_MARKER).is_file());
    drop(workspace);
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn substituted_directories_are_refused_after_attestation() -> TestResult {
    let root = tempfile::tempdir()?;
    let (_source_directory, mut source) = admitted_source()?;
    let signing = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| "key generation failed")?;
    let publisher =
        Ed25519KeyPair::from_pkcs8(signing.as_ref()).map_err(|_| "key parsing failed")?;
    let keys = [publisher.public_key().as_ref().try_into()?];
    let key_file = root.path().join("signer.der");
    fs::write(&key_file, signing.as_ref())?;
    let bundle = root.path().join("bundle");
    export_edge_snapshot(&mut source, &bundle, &key_file)?;
    let activations = source
        .network_upgrade_activations()
        .ok_or("missing schedule")?;
    for label in ["stage", CANONICAL] {
        let target = root.path().join(format!("{label}-target"));
        let destination = EdgeSnapshotDestination::reserve(&target, false)?;
        let stage = destination.stage.path.clone();
        let replaced = match label {
            "stage" => stage.clone(),
            child => stage.join(child),
        };
        let moved = root.path().join(format!("{label}-moved"));
        let replacement = if label == "stage" {
            replaced.join(CANONICAL)
        } else {
            replaced.clone()
        };
        let unadmitted = replacement.join("unadmitted");
        let config = EdgeSnapshotStoreConfig {
            activations,
            raw_blob_retention: source.raw_blob_retention(),
            reorg_policy: source.reorg_policy(),
            required_first_available_height: BlockHeight::new(u32::MAX),
            resource_budget: RocksDbResourceBudget::for_local_tests(),
        };
        let failure = prepare_bootstrapped_store(
            destination,
            EdgeSnapshotBootstrap {
                source: &bundle,
                trusted_keys: &keys,
                spot_check_blocks: DEFAULT_SPOT_CHECK_BLOCKS,
            },
            config,
            |_| async {
                tokio::task::yield_now().await;
                fs::rename(&replaced, &moved)?;
                fs::create_dir_all(&replacement)?;
                fs::write(&unadmitted, b"unadmitted")?;
                Ok::<_, EdgeSnapshotError>(())
            },
        )
        .await;
        assert!(
            matches!(failure, Err(EdgeSnapshotError::Refused(reason)) if reason == "snapshot workspace was substituted during attestation")
        );
        assert!(!target.exists());
        if label == "stage" {
            assert_eq!(fs::read(&unadmitted)?, b"unadmitted");
        }
    }
    Ok(())
}
