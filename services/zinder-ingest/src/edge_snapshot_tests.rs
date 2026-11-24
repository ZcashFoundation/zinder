use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use ring::signature::{Ed25519KeyPair, KeyPair};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use zebra_chain::{
    block::{Height, genesis::regtest_genesis_block, merkle::Root},
    serialization::ZcashSerialize,
    transparent::Input,
};
use zinder_core::{
    BlockHash, CommitmentTreeCheckpoint, CommitmentTreeFrontiers, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootHash, SubtreeRootIndex, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalWriterConfig,
    CanonicalWriterError, CommitmentTreeSizes, RawBlobPolicy, admit_canonical_store,
    position_canonical_block, prepare_canonical_block,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{NodeCapabilities, SourceBlock, SourceSubtreeRoot, SourceSubtreeRoots};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
    CanonicalStoreBuildPlan, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbResourceBudget,
    edge_snapshot::EdgeSnapshotBlock,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const SNAPSHOT: BlockHeight = BlockHeight::new(4096);
const HISTORY_FLOOR: BlockHeight = SNAPSHOT;
const POOLS: [ShieldedProtocol; 3] = [
    ShieldedProtocol::Sapling,
    ShieldedProtocol::Orchard,
    ShieldedProtocol::Ironwood,
];

#[derive(Clone)]
struct Validator {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    checkpoint: CommitmentTreeCheckpoint,
    activations: NetworkUpgradeActivations,
    tip: BlockId,
    subtrees: Vec<SubtreeRootArtifact>,
}

#[async_trait::async_trait]
impl NodeSource for Validator {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        Ok(self.activations.clone())
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "missing fixture block".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }

    async fn fetch_chain_checkpoint(
        &self,
        _: BlockHeight,
        _: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        Ok(self.checkpoint.clone())
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        _: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        let roots = self
            .subtrees
            .iter()
            .filter(|root| root.protocol == protocol)
            .map(|root| {
                SourceSubtreeRoot::new(
                    root.subtree_index,
                    root.root_hash,
                    root.completing_block_height,
                )
            })
            .collect::<Vec<_>>();
        Ok(SourceSubtreeRoots::new(protocol, start_index, roots))
    }
}

impl Validator {
    fn fixture() -> TestResult<Self> {
        let network = Network::ZcashRegtest;
        let genesis = regtest_genesis_block();
        let genesis = SourceBlock::from_raw_block_bytes(
            network,
            BlockHeight::new(0),
            genesis.zcash_serialize_to_vec()?,
        )?;
        let early = block_after(&genesis, BlockHeight::new(4093))?;
        let before_floor = block_after(&early, BlockHeight::new(4094))?;
        let floor = block_after(&before_floor, BlockHeight::new(4095))?;
        let snapshot = block_after(&floor, SNAPSHOT)?;
        let next = block_after(&snapshot, BlockHeight::new(4097))?;
        let tip = block_id(&next);
        let checkpoint = checkpoint(&snapshot);
        let blocks = [genesis, early, before_floor, floor, snapshot, next]
            .into_iter()
            .map(|block| (block.height, block))
            .collect();
        let mut scheduled = zinder_testkit::sample_regtest_upgrade_activations()
            .activations()
            .to_vec();
        for upgrade in &mut scheduled {
            upgrade.activation_height = BlockHeight::new(5000);
        }
        Ok(Self {
            blocks,
            checkpoint,
            activations: NetworkUpgradeActivations::new(network, scheduled)?,
            tip,
            subtrees: Vec::new(),
        })
    }

    fn with_completed_subtrees() -> TestResult<Self> {
        let mut validator = Self::fixture()?;
        let mut schedule = validator.activations.activations().to_vec();
        for upgrade in &mut schedule {
            upgrade.activation_height = BlockHeight::new(1);
        }
        validator.activations = NetworkUpgradeActivations::new(Network::ZcashRegtest, schedule)?;
        let frontier = |pool| {
            zinder_testkit::completed_subtree_frontier(pool).map_err(|error| error.to_string())
        };
        validator.checkpoint.frontiers = CommitmentTreeFrontiers::from_validated_parts(
            Some(frontier(ShieldedProtocol::Sapling)?),
            Some(frontier(ShieldedProtocol::Orchard)?),
            Some(frontier(ShieldedProtocol::Ironwood)?),
        )?;
        validator.subtrees = POOLS
            .into_iter()
            .zip([(1, 4093), (2, 4094), (3, 4095)])
            .map(|(pool, (seed, height))| {
                let completion = block_id(validator.block(height)?);
                Ok(SubtreeRootArtifact::new(
                    pool,
                    SubtreeRootIndex::new(0),
                    SubtreeRootHash::from_bytes([seed; 32]),
                    completion.height,
                    completion.hash,
                ))
            })
            .collect::<TestResult<Vec<_>>>()?;
        Ok(validator)
    }

    fn checkpoint_at(&self, block: &SourceBlock) -> CommitmentTreeCheckpoint {
        CommitmentTreeCheckpoint::new(
            block_id(block),
            block.block_time_seconds,
            self.checkpoint.frontiers.clone(),
        )
    }

    fn block(&self, height: u32) -> TestResult<&SourceBlock> {
        Ok(self
            .blocks
            .get(&BlockHeight::new(height))
            .ok_or("missing fixture block")?)
    }

    fn config(&self) -> TestResult<EdgeSnapshotStoreConfig<'_>> {
        Ok(EdgeSnapshotStoreConfig {
            activations: &self.activations,
            workload: CanonicalStoreWorkload::Wallet,
            raw_blob_retention: RawBlobRetention::None,
            reorg_policy: CanonicalReorgPolicy::new(1)?,
            required_first_available_height: HISTORY_FLOOR,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
        })
    }

    fn build_block(&self, height: u32) -> TestResult<CanonicalBuildBlock> {
        let block = self.block(height)?;
        let prepared = prepare_canonical_block(block, &self.activations, RawBlobPolicy::None)?;
        let mut sizes = CommitmentTreeSizes::from_tip_metadata(self.checkpoint.tip_metadata());
        let positioned = position_canonical_block(prepared, &mut sizes)?;
        Ok(CanonicalBuildBlock {
            facts: positioned.facts,
            replay_envelope: positioned.replay_envelope,
            compact_block: positioned.compact_block,
            tip_metadata: positioned.tip_metadata,
            tree_state_checkpoint: Some(self.checkpoint_at(block)),
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
        })
    }

    fn publish(&self, path: &Path) -> TestResult<RocksDbCanonicalStore> {
        let config = self.config()?;
        let plan = CanonicalStoreBuildPlan::checkpointed(
            config.activations,
            self.checkpoint_at(self.block(4095)?),
            self.checkpoint.block_id,
            config.raw_blob_retention,
            config.reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            config.workload,
            plan,
            config.resource_budget,
        )?;
        builder.bulk_load_blocks(
            config.activations,
            [Ok::<_, io::Error>(self.build_block(4096)?)],
        )?;
        if self.subtrees.is_empty() {
            builder.load_subtree_roots(std::iter::empty())?;
        } else {
            builder.load_complete_subtree_root_prefix(self.subtrees.iter().cloned())?;
        }
        builder.confirm_source_tip_checkpoint(&self.checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            self.checkpoint.block_id,
            UnixTimestampMillis::new(1),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }
}

fn block_id(block: &SourceBlock) -> BlockId {
    BlockId::new(block.height, block.hash)
}

fn checkpoint(block: &SourceBlock) -> CommitmentTreeCheckpoint {
    CommitmentTreeCheckpoint::new(
        block_id(block),
        block.block_time_seconds,
        CommitmentTreeFrontiers::default(),
    )
}

fn block_after(parent: &SourceBlock, height: BlockHeight) -> TestResult<SourceBlock> {
    let mut block: Block = parent.raw_block_bytes.as_slice().zcash_deserialize_into()?;
    let coinbase = block.transactions.first().ok_or("missing coinbase")?;
    let input = coinbase
        .inputs()
        .into_iter()
        .next()
        .ok_or("missing coinbase input")?;
    let original = input.zcash_serialize_to_vec()?;
    let replacement = Input::Coinbase {
        height: Height(height.value()),
        data: vec![0],
        sequence: u32::MAX,
    };
    let mut tx = coinbase.zcash_serialize_to_vec()?;
    assert_eq!(tx.get(5..5 + original.len()), Some(original.as_slice()));
    tx.splice(5..5 + original.len(), replacement.zcash_serialize_to_vec()?);
    block.transactions = vec![Arc::new(tx.as_slice().zcash_deserialize_into()?)];
    let header = Arc::make_mut(&mut block.header);
    header.previous_block_hash = zebra_chain::block::Hash(parent.hash.as_bytes());
    header.merkle_root = block.transactions.iter().collect::<Root>();
    Ok(SourceBlock::from_raw_block_bytes(
        parent.network,
        height,
        block.zcash_serialize_to_vec()?,
    )?)
}

#[tokio::test]
async fn local_and_http_snapshot_bootstrap_can_append() -> TestResult {
    let root = tempfile::tempdir()?;
    let validator = Validator::fixture()?;
    let (bundle, keys) = signed_bundle(root.path(), &validator)?;
    let (url, _, server) = serve_bundle(bundle.clone()).await?;
    for (name, source) in [
        ("local", bundle.to_string_lossy().into_owned()),
        ("http", url),
    ] {
        let target = root.path().join(name).join("");
        assert!(target.as_os_str().as_encoded_bytes().ends_with(b"/"));
        let config = validator.config()?;
        let args = bootstrap_args(source, keys);
        let (destination, source) = prepare_source(&args, &target, &config).await?;
        if name == "http" {
            assert_eq!(
                destination.download_path(),
                Some(root.path().join("http.bootstrap.download").as_path())
            );
        }
        let policy = config.reorg_policy;
        let required = config.required_first_available_height;
        prepare_bootstrapped_store(
            destination,
            EdgeSnapshotBootstrap {
                source: &source,
                trusted_keys: &keys,
                spot_check_blocks: NonZeroU32::MIN,
            },
            config,
            |evidence| {
                attest(
                    &validator,
                    &validator.activations,
                    policy,
                    required,
                    evidence,
                )
            },
        )
        .await?;
        assert_append_restored(&validator, &target)?;
        assert!(!root.path().join("http.bootstrap.download").exists());
    }
    server.abort();
    Ok(())
}

fn signed_bundle(root: &Path, validator: &Validator) -> TestResult<(PathBuf, [[u8; 32]; 1])> {
    let mut canonical = validator.publish(&root.join("publisher"))?;
    // PKCS#8 v1 seed-only DER, as exported by OpenSSL.
    let mut signing_key = vec![
        0x30, 0x2e, 0x02, 0x01, 0, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
    ];
    signing_key.extend([42; 32]);
    let signer = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&signing_key)
        .map_err(|_| "key parsing failed")?;
    let keys = [signer.public_key().as_ref().try_into()?];
    let bundle = root.join("bundle");
    let key_file = root.join("signing-key.der");
    fs::write(&key_file, signing_key)?;
    export_edge_snapshot(&mut canonical, &bundle, &key_file, None)?;
    Ok((bundle, keys))
}

fn assert_append_restored(validator: &Validator, target: &Path) -> TestResult {
    let config = validator.config()?;
    let store = RocksDbCanonicalStore::open_ready(
        target,
        config.activations,
        config.workload,
        config.raw_blob_retention,
        config.reorg_policy,
        config.resource_budget,
    )?;
    let fence = store.event_fence();
    let (_, appended) = store.commit_live_append(
        CanonicalLiveAppend::new(
            fence,
            validator.build_block(4097)?,
            Vec::new(),
            validator.checkpoint.block_id,
            UnixTimestampMillis::new(2),
        ),
        config.activations,
    )?;
    assert_eq!(appended.visible_tip(), validator.tip);
    Ok(())
}

type BundleServer = (
    String,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<io::Result<()>>,
);

async fn serve_bundle(bundle: PathBuf) -> TestResult<BundleServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/bundle", listener.local_addr()?);
    let payload_requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&payload_requests);
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await?;
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).await?;
            let path = line
                .split_whitespace()
                .nth(1)
                .and_then(|path| path.strip_prefix("/bundle/"))
                .ok_or_else(|| io::Error::other("unexpected fixture request"))?;
            if !matches!(path, MANIFEST_FILE | SIGNATURE_FILE) {
                observed.fetch_add(1, Ordering::Relaxed);
            }
            let bytes = fs::read(bundle.join(path))?;
            loop {
                let mut header = String::new();
                if socket.read_line(&mut header).await? == 0 || header == "\r\n" {
                    break;
                }
            }
            let socket = socket.get_mut();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        bytes.len(),
                    )
                    .as_bytes(),
                )
                .await?;
            socket.write_all(&bytes).await?;
            socket.shutdown().await?;
        }
    });
    Ok((url, payload_requests, server))
}

fn bootstrap_args(source: String, keys: [[u8; 32]; 1]) -> BootstrapArgs {
    BootstrapArgs {
        source,
        public_key: keys.to_vec(),
        spot_check_blocks: NonZeroU32::MIN,
        sealed_history_output: None,
    }
}

#[tokio::test]
async fn http_preflight_refusals_fetch_no_payload() -> TestResult {
    let root = tempfile::tempdir()?;
    let validator = Validator::fixture()?;
    let config = validator.config()?;
    let (bundle, keys) = signed_bundle(root.path(), &validator)?;
    let (url, requests, server) = serve_bundle(bundle).await?;
    let mut args = bootstrap_args(url, keys);
    let occupied = root.path().join("occupied");
    fs::create_dir(&occupied)?;
    fs::write(occupied.join("marker"), b"preserve")?;
    let failure = prepare_source(&args, &occupied, &config).await;
    assert!(
        matches!(failure, Err(SnapshotCommandError::Snapshot(EdgeSnapshotError::Refused(reason))) if reason.contains("occupied"))
    );

    let target = root.path().join("restored");
    args.sealed_history_output = Some(occupied);
    let failure = prepare_source(&args, &target, &config).await;
    assert!(
        matches!(failure, Err(SnapshotCommandError::Snapshot(EdgeSnapshotError::Refused(reason))) if reason.contains("occupied"))
    );

    args.sealed_history_output = None;
    let other_network = NetworkUpgradeActivations::new(Network::ZcashTestnet, Vec::new())?;
    let other_schedule = NetworkUpgradeActivations::new(config.activations.network(), Vec::new())?;
    for incompatible in [
        EdgeSnapshotStoreConfig {
            activations: &other_network,
            ..config
        },
        EdgeSnapshotStoreConfig {
            activations: &other_schedule,
            ..config
        },
        EdgeSnapshotStoreConfig {
            workload: CanonicalStoreWorkload::Explorer,
            ..config
        },
        EdgeSnapshotStoreConfig {
            raw_blob_retention: RawBlobRetention::Transactions,
            ..config
        },
        EdgeSnapshotStoreConfig {
            reorg_policy: CanonicalReorgPolicy::new(2)?,
            ..config
        },
    ] {
        let failure = prepare_source(&args, &target, &incompatible).await;
        assert!(
            matches!(failure, Err(SnapshotCommandError::Snapshot(EdgeSnapshotError::Refused(reason))) if reason.contains("store identity"))
        );
    }
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    assert!(!target.exists());
    assert!(!root.path().join("restored.bootstrap.download").exists());
    assert!(!root.path().join("restored.bootstrap.staging").exists());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn validator_refuses_each_mismatched_anchor_before_publication() -> TestResult {
    let root = tempfile::tempdir()?;
    let publisher = Validator::fixture()?;
    let (bundle, keys) = signed_bundle(root.path(), &publisher)?;
    let stage = root.path().join("restored.bootstrap.staging");
    let target = root.path().join("restored");
    for mismatch in [
        "genesis", "schedule", "floor", "snapshot", "margin", "tree", "subtree", "header", "txid",
    ] {
        let validator = Validator::fixture()?;
        let config = publisher.config()?;
        let policy = config.reorg_policy;
        let required = config.required_first_available_height;
        let activations = &publisher.activations;
        let failure = prepare_bootstrapped_store(
            EdgeSnapshotDestination::reserve(&target, None, false)?,
            EdgeSnapshotBootstrap {
                source: &bundle,
                trusted_keys: &keys,
                spot_check_blocks: NonZeroU32::MIN,
            },
            config,
            |mut evidence| async move {
                let mut validator = validator;
                introduce_mismatch(mismatch, &mut validator, &mut evidence)?;
                attest(&validator, activations, policy, required, evidence).await
            },
        )
        .await;
        assert!(
            matches!(failure, Err(SnapshotCommandError::Refused(_))),
            "{mismatch}: {failure:?}"
        );
        assert!(!target.exists(), "{mismatch}");
        assert!(!stage.exists(), "{mismatch}");
    }
    Ok(())
}

fn introduce_mismatch(
    mismatch: &str,
    validator: &mut Validator,
    evidence: &mut EdgeSnapshotAttestation,
) -> Result<(), SnapshotCommandError> {
    let wrong_hash = BlockHash::from_bytes([1; 32]);
    let missing = || SnapshotCommandError::Refused("missing mismatch fixture");
    match mismatch {
        "genesis" => {
            validator
                .blocks
                .get_mut(&BlockHeight::new(0))
                .ok_or_else(missing)?
                .hash = wrong_hash;
        }
        "schedule" => {
            validator.activations =
                NetworkUpgradeActivations::new(Network::ZcashTestnet, Vec::new())
                    .map_err(|_| missing())?;
        }
        "floor" => evidence.history_floor.hash = wrong_hash,
        "snapshot" => evidence.checkpoint.block_id.hash = wrong_hash,
        "margin" => validator.tip = validator.checkpoint.block_id,
        "tree" => {
            let tree =
                zinder_testkit::frontier_from_note_commitments(ShieldedProtocol::Sapling, &[])
                    .map_err(|_| missing())?;
            evidence.checkpoint.frontiers =
                CommitmentTreeFrontiers::from_validated_parts(Some(tree), None, None)
                    .map_err(|_| missing())?;
        }
        "subtree" => {
            evidence.terminal_subtrees[0] = Some(SubtreeRootArtifact::new(
                ShieldedProtocol::Sapling,
                SubtreeRootIndex::new(0),
                SubtreeRootHash::from_bytes([1; 32]),
                SNAPSHOT,
                evidence.checkpoint.block_id.hash,
            ));
        }
        "header" => {
            let observed = validator
                .blocks
                .get_mut(&BlockHeight::new(4095))
                .ok_or_else(missing)?;
            evidence.blocks.push(EdgeSnapshotBlock {
                block: block_id(observed),
                transaction_ids: Vec::new(),
            });
            *observed.raw_block_bytes.get_mut(108).ok_or_else(missing)? ^= 1;
        }
        "txid" => {
            evidence
                .blocks
                .first_mut()
                .ok_or_else(missing)?
                .transaction_ids = vec![TransactionId::from_bytes([1; 32])];
        }
        _ => return Err(missing()),
    }
    Ok(())
}

#[tokio::test]
async fn terminal_subtree_attestation_checks_all_pools_before_publication() -> TestResult {
    let root = tempfile::tempdir()?;
    let publisher = Validator::with_completed_subtrees()?;
    let (bundle, keys) = signed_bundle(root.path(), &publisher)?;
    let target = root.path().join("restored");
    for pool in 0..POOLS.len() {
        for mismatch in [
            "index",
            "root",
            "completion height",
            "completing block hash",
        ] {
            let mut validator = Validator::with_completed_subtrees()?;
            let subtree = validator
                .subtrees
                .get_mut(pool)
                .ok_or("missing terminal fixture root")?;
            let expected = match mismatch {
                "index" => {
                    subtree.subtree_index = SubtreeRootIndex::new(1);
                    "validator terminal subtree root differs"
                }
                "root" => {
                    subtree.root_hash = SubtreeRootHash::from_bytes([7; 32]);
                    "validator terminal subtree root differs"
                }
                "completion height" => {
                    subtree.completing_block_height = SNAPSHOT;
                    "validator terminal subtree root differs"
                }
                "completing block hash" => {
                    validator
                        .blocks
                        .get_mut(&subtree.completing_block_height)
                        .ok_or("missing completion block")?
                        .hash = BlockHash::from_bytes([7; 32]);
                    "validator best-chain block identity differs"
                }
                _ => return Err("unknown subtree mismatch".into()),
            };
            let failure =
                bootstrap_terminal_fixture(&publisher, &validator, &bundle, &keys, &target).await;
            assert!(
                matches!(failure, Err(SnapshotCommandError::Refused(reason)) if reason == expected),
                "{mismatch}: {failure:?}"
            );
            assert!(!target.exists());
            assert!(!root.path().join("restored.bootstrap.staging").exists());
        }
    }
    bootstrap_terminal_fixture(&publisher, &publisher, &bundle, &keys, &target).await?;
    assert_append_restored(&publisher, &target)?;
    Ok(())
}

async fn bootstrap_terminal_fixture(
    publisher: &Validator,
    validator: &Validator,
    bundle: &Path,
    keys: &[[u8; 32]],
    target: &Path,
) -> Result<(), SnapshotCommandError> {
    let config = publisher
        .config()
        .map_err(|_| SnapshotCommandError::Refused("invalid terminal fixture"))?;
    let policy = config.reorg_policy;
    let required = config.required_first_available_height;
    prepare_bootstrapped_store(
        EdgeSnapshotDestination::reserve(target, None, false)?,
        EdgeSnapshotBootstrap {
            source: bundle,
            trusted_keys: keys,
            spot_check_blocks: NonZeroU32::MIN,
        },
        config,
        |evidence| {
            attest(
                validator,
                &publisher.activations,
                policy,
                required,
                evidence,
            )
        },
    )
    .await
}

#[tokio::test]
async fn snapshot_below_the_configured_history_floor_is_refused_before_payloads() -> TestResult {
    let root = tempfile::tempdir()?;
    let validator = Validator::fixture()?;
    let (bundle, keys) = signed_bundle(root.path(), &validator)?;
    let (url, requests, server) = serve_bundle(bundle.clone()).await?;
    let target = root.path().join("restored");
    let uncovered = EdgeSnapshotStoreConfig {
        required_first_available_height: configured_first_available_height(None),
        ..validator.config()?
    };
    let args = bootstrap_args(url, keys);
    let failure = prepare_source(&args, &target, &uncovered).await;
    assert!(
        matches!(failure, Err(SnapshotCommandError::Snapshot(EdgeSnapshotError::Refused(reason))) if reason.contains("history starts above")),
        "unexpected HTTP preflight outcome"
    );
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    assert!(!target.exists());
    server.abort();

    let args = bootstrap_args(bundle.to_string_lossy().into_owned(), keys);
    let (destination, source) = prepare_source(&args, &target, &uncovered).await?;
    let policy = uncovered.reorg_policy;
    let required = uncovered.required_first_available_height;
    let failure = prepare_bootstrapped_store(
        destination,
        EdgeSnapshotBootstrap {
            source: &source,
            trusted_keys: &keys,
            spot_check_blocks: NonZeroU32::MIN,
        },
        uncovered,
        |evidence| {
            attest(
                &validator,
                &validator.activations,
                policy,
                required,
                evidence,
            )
        },
    )
    .await;
    assert!(
        matches!(failure, Err(SnapshotCommandError::Snapshot(EdgeSnapshotError::Refused(reason))) if reason.contains("history starts above")),
        "unexpected local preflight outcome"
    );
    assert!(!target.exists());

    let args = bootstrap_args(bundle.to_string_lossy().into_owned(), keys);
    let (destination, source) = prepare_source(&args, &target, &validator.config()?).await?;
    let config = validator.config()?;
    let policy = config.reorg_policy;
    let below_floor = BlockHeight::new(HISTORY_FLOOR.value() - 1);
    let failure = prepare_bootstrapped_store(
        destination,
        EdgeSnapshotBootstrap {
            source: &source,
            trusted_keys: &keys,
            spot_check_blocks: NonZeroU32::MIN,
        },
        config,
        |evidence| {
            attest(
                &validator,
                &validator.activations,
                policy,
                below_floor,
                evidence,
            )
        },
    )
    .await;
    assert!(
        matches!(failure, Err(SnapshotCommandError::Refused(reason)) if reason.contains("history starts above"))
    );
    assert!(!target.exists());
    Ok(())
}

#[tokio::test]
async fn bootstrapped_store_passes_normal_writer_admission() -> TestResult {
    let root = tempfile::tempdir()?;
    let validator = Validator::fixture()?;
    let (bundle, keys) = signed_bundle(root.path(), &validator)?;
    let target = root.path().join("restored");
    let checkpoint_height = BlockHeight::new(HISTORY_FLOOR.value() - 1);
    let config = EdgeSnapshotStoreConfig {
        required_first_available_height: configured_first_available_height(Some(checkpoint_height)),
        ..validator.config()?
    };
    let args = bootstrap_args(bundle.to_string_lossy().into_owned(), keys);
    let (destination, source) = prepare_source(&args, &target, &config).await?;
    let policy = config.reorg_policy;
    let required = config.required_first_available_height;
    prepare_bootstrapped_store(
        destination,
        EdgeSnapshotBootstrap {
            source: &source,
            trusted_keys: &keys,
            spot_check_blocks: NonZeroU32::MIN,
        },
        config,
        |evidence| {
            attest(
                &validator,
                &validator.activations,
                policy,
                required,
                evidence,
            )
        },
    )
    .await?;

    let admit = |checkpoint_height| {
        let validator = validator.clone();
        let target = target.clone();
        async move {
            let writer = CanonicalWriterConfig {
                storage_path: target,
                resource_budget: RocksDbResourceBudget::for_local_tests(),
                construction: CanonicalConstructionConfig::for_local_tests(
                    Duration::from_secs(5),
                    Arc::new(validator.activations.clone()),
                ),
                checkpoint_height,
                raw_blob_retention: RawBlobRetention::None,
                reorg_window_blocks: 1,
                tree_state_checkpoint_selection: None,
                follow: CanonicalFollowConfig {
                    request_timeout: Duration::from_secs(5),
                    poll_interval: Duration::from_millis(10),
                    lag_threshold_blocks: 0,
                    target_height: None,
                    event_retention_window: None,
                    event_retention_check_interval: Duration::from_secs(1),
                    mempool_ready_gate: None,
                    max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
                    node_failover: None,
                },
            };
            admit_canonical_store(
                &validator,
                Arc::new(validator.activations.clone()),
                &writer,
                &ComposedReadiness::new(Readiness::default(), &[]),
                &CancellationToken::new(),
            )
            .await
        }
    };
    admit(Some(checkpoint_height)).await?;
    assert!(matches!(
        admit(None).await,
        Err(CanonicalWriterError::InsufficientRetainedHistory { .. })
    ));
    Ok(())
}
