use std::{collections::BTreeMap, sync::Arc};

use ring::signature::{Ed25519KeyPair, KeyPair};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
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
    CommitmentTreeSizes, RawBlobPolicy, position_canonical_block, prepare_canonical_block,
};
use zinder_source::{NodeCapabilities, SourceBlock, SourceSubtreeRoots};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
    CanonicalStoreBuildPlan, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbResourceBudget,
    edge_snapshot::EdgeSnapshotBlock,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const SNAPSHOT: BlockHeight = BlockHeight::new(4096);

struct Validator {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    checkpoint: CommitmentTreeCheckpoint,
    activations: NetworkUpgradeActivations,
    tip: BlockId,
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
        Ok(SourceSubtreeRoots::new(protocol, start_index, Vec::new()))
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
        let floor = block_after(&genesis, BlockHeight::new(4095))?;
        let snapshot = block_after(&floor, SNAPSHOT)?;
        let next = block_after(&snapshot, BlockHeight::new(4097))?;
        let tip = block_id(&next);
        let checkpoint = checkpoint(&snapshot);
        let blocks = [genesis, floor, snapshot, next]
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
        })
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
            resource_budget: RocksDbResourceBudget::for_local_tests(),
        })
    }

    fn build_block(&self, height: u32) -> TestResult<CanonicalBuildBlock> {
        let block = self.block(height)?;
        let prepared = prepare_canonical_block(block, &self.activations, RawBlobPolicy::None)?;
        let positioned = position_canonical_block(prepared, &mut CommitmentTreeSizes::default())?;
        Ok(CanonicalBuildBlock {
            facts: positioned.facts,
            replay_envelope: positioned.replay_envelope,
            compact_block: positioned.compact_block,
            tip_metadata: positioned.tip_metadata,
            tree_state_checkpoint: Some(checkpoint(block)),
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
        })
    }

    fn publish(&self, path: &Path) -> TestResult<RocksDbCanonicalStore> {
        let config = self.config()?;
        let plan = CanonicalStoreBuildPlan::checkpointed(
            config.activations,
            checkpoint(self.block(4095)?),
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
        builder.load_subtree_roots(std::iter::empty())?;
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
    let (url, server) = serve_bundle(bundle.clone()).await?;
    let http = download_bundle(&url, &root.path().join("http"), &keys).await?;
    server.abort();
    for (name, source) in [("local", &bundle), ("http", &http.0)] {
        let target = root.path().join(name);
        let config = validator.config()?;
        let policy = config.reorg_policy;
        prepare_bootstrapped_store(
            EdgeSnapshotBootstrap {
                source,
                target: &target,
                trusted_keys: &keys,
                spot_check_blocks: NonZeroU32::MIN,
                sealed_history_output: None,
            },
            config,
            |evidence| attest(&validator, &validator.activations, policy, evidence),
        )
        .await?;
        assert_append_restored(&validator, &target)?;
    }
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
    export_edge_snapshot(&mut canonical, &bundle, &signing_key, None)?;
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

async fn serve_bundle(
    bundle: PathBuf,
) -> TestResult<(String, tokio::task::JoinHandle<io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/bundle", listener.local_addr()?);
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
    Ok((url, server))
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
        let activations = &publisher.activations;
        let failure = prepare_bootstrapped_store(
            EdgeSnapshotBootstrap {
                source: &bundle,
                target: &target,
                trusted_keys: &keys,
                spot_check_blocks: NonZeroU32::MIN,
                sealed_history_output: None,
            },
            config,
            |mut evidence| async move {
                let mut validator = validator;
                introduce_mismatch(mismatch, &mut validator, &mut evidence)?;
                attest(&validator, activations, policy, evidence).await
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
