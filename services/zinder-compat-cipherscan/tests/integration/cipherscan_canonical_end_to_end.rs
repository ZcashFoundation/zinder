//! End-to-end integration test exercising `zinder-compat-cipherscan` over
//! real canonical + materialized-view + wallet + explorer stores and WebSocket.

use std::{
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{body::Body, http::Request as HttpRequest, http::StatusCode};
use eyre::{Result, eyre};
use parking_lot::RwLock;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use tower::ServiceExt as _;
use zebra_chain::serialization::ZcashDeserialize as _;
use zebra_chain::transaction::Transaction as ZebraTransaction;
use zinder_compat_cipherscan::{CipherscanMarketPriceEndpoints, CipherscanRestAdapter};
use zinder_core::wire::encode_rpc_block_hash_hex;
use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainEpoch, ChainTipMetadata,
    CommitmentTreeAccumulator, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
    CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, LockTime, Network,
    NetworkUpgradeActivations, PrivacyShape, SerializedBytesDigest, ShieldedProtocol,
    TransactionBlobArtifact, TransactionComponentCounts, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
    TransactionVersion, TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
    TransparentOutputFact, UnixTimestampMillis, encode_canonical_block_replay,
};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_ingest::{MaterializedViewReplayConfig, MaterializedViewTailer, ReplayPrevoutCaches};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockProductionTimeBackfillCoverage,
    BlockProductionTimeConsumer, BlockProductionTimeRow, CommitmentRootBackfillCoverage,
    CommitmentRootSearchConsumer, ConventionalFeeDistributionBackfillCoverage,
    ConventionalFeeDistributionConsumer, MaterializedViewStore, MaterializedViewStoreOptions,
    TransparentSpendFacts,
};
use zinder_proto::capabilities::{
    WALLET_EVENTS_CHAIN_V1, WALLET_EVENTS_MEMPOOL_V2, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_SNAPSHOT_MEMPOOL_V3,
};
use zinder_proto::v1::wallet::{
    self, wallet_query_server::WalletQuery, wallet_query_server::WalletQueryServer,
};
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{HttpListenerLimits, connect_zinder_grpc, serve_http};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreRequirements, CanonicalStoreWorkload, RawBlobRetention,
    RocksDbCanonicalBuilder, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    RocksDbResourceBudget, chain_epoch_message,
};
use zinder_testkit::{
    IngestControlFixture, WalletServingStoreFixture,
    open_test_materialized_view_store_for_canonical, sample_regtest_upgrade_activations,
};

const TEST_REORG_WINDOW: u32 = 4;
const TEST_BITS: u32 = 0x200f_0f0f;
const VALUE_POOL_IDS: [&str; 6] = [
    "transparent",
    "sprout",
    "sapling",
    "orchard",
    "ironwood",
    "lockbox",
];

type InnerWalletAdapter = WalletQueryGrpcAdapter<WalletServingQuery<(), AdmittedIngestControl>>;
type MempoolSender = mpsc::Sender<std::result::Result<wallet::MempoolEventEnvelope, Status>>;

#[derive(Clone)]
struct TestWalletQuery {
    inner: Arc<InnerWalletAdapter>,
    chain_events_tx: broadcast::Sender<wallet::ChainEventEnvelope>,
    mempool_senders: Arc<RwLock<Vec<MempoolSender>>>,
}

impl TestWalletQuery {
    fn new(inner: InnerWalletAdapter) -> (Self, broadcast::Sender<wallet::ChainEventEnvelope>) {
        let (chain_events_tx, _) = broadcast::channel(32);
        let service = Self {
            inner: Arc::new(inner),
            chain_events_tx: chain_events_tx.clone(),
            mempool_senders: Arc::new(RwLock::new(Vec::new())),
        };
        (service, chain_events_tx)
    }
}

#[tonic::async_trait]
impl WalletQuery for TestWalletQuery {
    type CompactBlocksInRangeStream =
        <InnerWalletAdapter as WalletQuery>::CompactBlocksInRangeStream;
    type FullBlocksInRangeStream = <InnerWalletAdapter as WalletQuery>::FullBlocksInRangeStream;
    type ChainEventsStream =
        ReceiverStream<std::result::Result<wallet::ChainEventEnvelope, Status>>;
    type MempoolEventsStream =
        ReceiverStream<std::result::Result<wallet::MempoolEventEnvelope, Status>>;
    type TransparentAddressUnspentOutputsStream =
        <InnerWalletAdapter as WalletQuery>::TransparentAddressUnspentOutputsStream;
    type TransparentAddressTxIdsInRangeStream =
        <InnerWalletAdapter as WalletQuery>::TransparentAddressTxIdsInRangeStream;

    async fn visible_tip_block(
        &self,
        request: Request<wallet::VisibleTipBlockRequest>,
    ) -> std::result::Result<Response<wallet::VisibleTipBlockResponse>, Status> {
        self.inner.visible_tip_block(request).await
    }

    async fn settled_tip_block(
        &self,
        request: Request<wallet::SettledTipBlockRequest>,
    ) -> std::result::Result<Response<wallet::SettledTipBlockResponse>, Status> {
        self.inner.settled_tip_block(request).await
    }

    async fn block_id_by_selector(
        &self,
        request: Request<wallet::BlockSelectorRequest>,
    ) -> std::result::Result<Response<wallet::BlockIdResponse>, Status> {
        self.inner.block_id_by_selector(request).await
    }

    async fn block_header_by_selector(
        &self,
        request: Request<wallet::BlockSelectorRequest>,
    ) -> std::result::Result<Response<wallet::BlockHeaderResponse>, Status> {
        self.inner.block_header_by_selector(request).await
    }

    async fn compact_block(
        &self,
        request: Request<wallet::CompactBlockRequest>,
    ) -> std::result::Result<Response<wallet::CompactBlockResponse>, Status> {
        self.inner.compact_block(request).await
    }

    async fn compact_blocks_in_range(
        &self,
        request: Request<wallet::CompactBlocksInRangeRequest>,
    ) -> std::result::Result<Response<Self::CompactBlocksInRangeStream>, Status> {
        self.inner.compact_blocks_in_range(request).await
    }

    async fn full_block(
        &self,
        request: Request<wallet::FullBlockRequest>,
    ) -> std::result::Result<Response<wallet::FullBlockResponse>, Status> {
        self.inner.full_block(request).await
    }

    async fn full_blocks_in_range(
        &self,
        request: Request<wallet::FullBlocksInRangeRequest>,
    ) -> std::result::Result<Response<Self::FullBlocksInRangeStream>, Status> {
        self.inner.full_blocks_in_range(request).await
    }

    async fn transaction(
        &self,
        request: Request<wallet::TransactionRequest>,
    ) -> std::result::Result<Response<wallet::TransactionStatusResponse>, Status> {
        self.inner.transaction(request).await
    }

    async fn tree_state_at_height(
        &self,
        request: Request<wallet::TreeStateAtHeightRequest>,
    ) -> std::result::Result<Response<wallet::TreeStateResponse>, Status> {
        self.inner.tree_state_at_height(request).await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        request: Request<wallet::LatestTreeStateCheckpointRequest>,
    ) -> std::result::Result<Response<wallet::TreeStateResponse>, Status> {
        self.inner.latest_tree_state_checkpoint(request).await
    }

    async fn subtree_roots(
        &self,
        request: Request<wallet::SubtreeRootsRequest>,
    ) -> std::result::Result<Response<wallet::SubtreeRootsResponse>, Status> {
        self.inner.subtree_roots(request).await
    }

    async fn broadcast_transaction(
        &self,
        request: Request<wallet::BroadcastTransactionRequest>,
    ) -> std::result::Result<Response<wallet::BroadcastTransactionResponse>, Status> {
        self.inner.broadcast_transaction(request).await
    }

    async fn chain_events(
        &self,
        _request: Request<wallet::ChainEventsRequest>,
    ) -> std::result::Result<Response<Self::ChainEventsStream>, Status> {
        let mut rx = self.chain_events_tx.subscribe();
        let (tx, out_rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Ok(envelope) = rx.recv().await {
                if tx.send(Ok(envelope)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    async fn mempool_snapshot(
        &self,
        _request: Request<wallet::MempoolSnapshotRequest>,
    ) -> std::result::Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        let tip = self
            .inner
            .visible_tip_block(Request::new(wallet::VisibleTipBlockRequest {
                at_epoch_id: None,
            }))
            .await?
            .into_inner();
        Ok(Response::new(wallet::MempoolSnapshotResponse {
            chain_view: tip.chain_view,
            entries: Vec::new(),
            next_cursor: Vec::new(),
            snapshot_age_millis: 0,
            events_resume_cursor: Vec::new(),
            source_tip: None,
        }))
    }

    async fn mempool_events(
        &self,
        _request: Request<wallet::MempoolEventsRequest>,
    ) -> std::result::Result<Response<Self::MempoolEventsStream>, Status> {
        let (tx, out_rx) = mpsc::channel(1);
        self.mempool_senders.write().push(tx);
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: Request<wallet::TransparentAddressUnspentOutputsRequest>,
    ) -> std::result::Result<Response<Self::TransparentAddressUnspentOutputsStream>, Status> {
        self.inner
            .transparent_address_unspent_outputs(request)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: Request<wallet::TransparentAddressTxIdsInRangeRequest>,
    ) -> std::result::Result<Response<Self::TransparentAddressTxIdsInRangeStream>, Status> {
        self.inner
            .transparent_address_tx_ids_in_range(request)
            .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        request: Request<wallet::TransparentOutpointSpendStatusRequest>,
    ) -> std::result::Result<Response<wallet::TransparentOutpointSpendStatusResponse>, Status> {
        self.inner.transparent_outpoint_spend_status(request).await
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> std::result::Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        let tip = self
            .inner
            .visible_tip_block(Request::new(wallet::VisibleTipBlockRequest {
                at_epoch_id: None,
            }))
            .await?
            .into_inner();
        let source_tip = tip.visible_tip_block.map(|block_id| wallet::BlockTip {
            height: block_id.height,
            hash: block_id.block_hash,
        });
        let pools = VALUE_POOL_IDS
            .iter()
            .map(|pool_id| wallet::ChainValuePool {
                id: (*pool_id).to_owned(),
                monitored: true,
                chain_value_zat: Some(1_000_000_000),
            })
            .collect();
        Ok(Response::new(wallet::ChainValuePoolsAtTipResponse {
            chain_view: tip.chain_view,
            pools,
            source_tip,
        }))
    }

    async fn server_info(
        &self,
        request: Request<wallet::ServerInfoRequest>,
    ) -> std::result::Result<Response<wallet::ServerInfoResponse>, Status> {
        let mut response = self.inner.server_info(request).await?.into_inner();
        if let Some(descriptor) = response.info.as_mut().and_then(|srv| srv.common.as_mut()) {
            for extra in [
                WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
                WALLET_SNAPSHOT_MEMPOOL_V3,
                WALLET_EVENTS_CHAIN_V1,
                WALLET_EVENTS_MEMPOOL_V2,
            ] {
                if !descriptor.capabilities.iter().any(|cap| cap == extra) {
                    descriptor.capabilities.push(extra.to_owned());
                }
            }
        }
        Ok(Response::new(response))
    }

    async fn network_upgrade_activations(
        &self,
        request: Request<wallet::NetworkUpgradeActivationsRequest>,
    ) -> std::result::Result<Response<wallet::NetworkUpgradeActivationsResponse>, Status> {
        self.inner.network_upgrade_activations(request).await
    }
}

struct BuiltTxSpec {
    raw_bytes: Vec<u8>,
    facts: CanonicalTransactionFacts,
}

struct BlockBlueprint {
    height: u32,
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    block_time: i64,
    raw_block_bytes: Vec<u8>,
    txs: Vec<BuiltTxSpec>,
}

struct ConstructedBlock {
    build_block: CanonicalBuildBlock,
    final_roots: BlockFinalNoteCommitmentRoots,
    commit_context: BlockCommitContext,
}

struct SplitTxArtifacts {
    blobs: Vec<TransactionBlobArtifact>,
    facts: Vec<CanonicalTransactionFacts>,
    commit_artifacts: Vec<TransactionFactsArtifact>,
}

struct AccumulatedCommitments {
    frontiers: CommitmentTreeFrontiers,
    tip_metadata: ChainTipMetadata,
    compact_txs: Vec<CompactTransaction>,
}

fn p2pkh_script(hash160: [u8; 20]) -> Vec<u8> {
    let mut script = Vec::with_capacity(25);
    script.extend_from_slice(&[0x76, 0xa9, 0x14]);
    script.extend_from_slice(&hash160);
    script.extend_from_slice(&[0x88, 0xac]);
    script
}

fn zebra_txid(raw_bytes: &[u8]) -> Result<TransactionId> {
    let parsed = ZebraTransaction::zcash_deserialize(raw_bytes)?;
    Ok(TransactionId::from_bytes(parsed.hash().0))
}

fn build_v1_coinbase_bytes(height: u32, reward_zat: u64, pubkey_hash: [u8; 20]) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&[0_u8; 32]);
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    let mut script_sig = match height {
        0 => vec![0x00_u8],
        1..=16 => vec![0x50_u8.saturating_add(u8::try_from(height)?)],
        _ => {
            let mut raw = Vec::with_capacity(5);
            let mut remaining = height;
            while remaining > 0 {
                raw.push(u8::try_from(remaining & 0xff)?);
                remaining >>= 8;
            }
            if raw.last().is_some_and(|msb| msb & 0x80 != 0) {
                raw.push(0x00);
            }
            let mut push = Vec::with_capacity(raw.len() + 1);
            push.push(u8::try_from(raw.len())?);
            push.extend_from_slice(&raw);
            push
        }
    };
    script_sig.extend_from_slice(&[0x06, b'z', b'i', b'n', b'd', b'e', b'r']);
    bytes.push(u8::try_from(script_sig.len())?);
    bytes.extend_from_slice(&script_sig);
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&reward_zat.to_le_bytes());
    let script_pub_key = p2pkh_script(pubkey_hash);
    bytes.push(u8::try_from(script_pub_key.len())?);
    bytes.extend_from_slice(&script_pub_key);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    Ok(bytes)
}

fn build_v1_spend_bytes(
    prev_txid: TransactionId,
    prev_vout: u32,
    output_zat: u64,
    recipient_hash: [u8; 20],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&prev_txid.as_bytes());
    bytes.extend_from_slice(&prev_vout.to_le_bytes());
    bytes.push(1);
    bytes.push(0x51);
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&output_zat.to_le_bytes());
    let script_pub_key = p2pkh_script(recipient_hash);
    bytes.push(u8::try_from(script_pub_key.len())?);
    bytes.extend_from_slice(&script_pub_key);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    Ok(bytes)
}

fn p2pkh_output_fact(
    output_index: u32,
    zatoshis: u64,
    pubkey_hash: [u8; 20],
) -> TransparentOutputFact {
    let script_pub_key = p2pkh_script(pubkey_hash);
    let script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    TransparentOutputFact::new(output_index, zatoshis, script_pub_key, script_hash)
}

fn make_coinbase_spec(height: u32, reward_zat: u64, pubkey_hash: [u8; 20]) -> Result<BuiltTxSpec> {
    let raw_bytes = build_v1_coinbase_bytes(height, reward_zat, pubkey_hash)?;
    let txid = zebra_txid(&raw_bytes)?;
    let facts = CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id: txid,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V1,
            consensus_branch_id: None,
            lock_time: LockTime::Height(BlockHeight::new(0)),
            expiry_height: None,
            size_bytes: u32::try_from(raw_bytes.len())?,
            counts: TransactionComponentCounts {
                transparent_input_count: 1,
                transparent_output_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Coinbase,
            is_coinbase: true,
            unsupported_sections: Vec::new(),
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&raw_bytes),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
        transparent_inputs: vec![TransparentInputFact::new(
            0,
            TransparentOutPoint::COINBASE_SENTINEL,
        )],
        transparent_outputs: vec![p2pkh_output_fact(0, reward_zat, pubkey_hash)],
    };
    Ok(BuiltTxSpec { raw_bytes, facts })
}

fn make_spend_spec(
    prev_txid: TransactionId,
    prev_vout: u32,
    output_zat: u64,
    recipient_hash: [u8; 20],
) -> Result<BuiltTxSpec> {
    let raw_bytes = build_v1_spend_bytes(prev_txid, prev_vout, output_zat, recipient_hash)?;
    let txid = zebra_txid(&raw_bytes)?;
    let facts = CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id: txid,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V1,
            consensus_branch_id: None,
            lock_time: LockTime::Height(BlockHeight::new(0)),
            expiry_height: None,
            size_bytes: u32::try_from(raw_bytes.len())?,
            counts: TransactionComponentCounts {
                transparent_input_count: 1,
                transparent_output_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::TransparentOnly,
            is_coinbase: false,
            unsupported_sections: Vec::new(),
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&raw_bytes),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
        transparent_inputs: vec![TransparentInputFact::new(
            0,
            TransparentOutPoint::new(prev_txid, prev_vout),
        )],
        transparent_outputs: vec![p2pkh_output_fact(0, output_zat, recipient_hash)],
    };
    Ok(BuiltTxSpec { raw_bytes, facts })
}

const fn derived_commitment(pool_tag: u8, block_hash: [u8; 32]) -> [u8; 32] {
    let mut commitment = [0_u8; 32];
    commitment[0] = block_hash[0];
    commitment[1] = block_hash[1];
    commitment[2] = block_hash[2];
    commitment[3] = block_hash[3];
    commitment[4] = pool_tag;
    commitment
}

fn pool_active_at(
    activations: &NetworkUpgradeActivations,
    protocol: ShieldedProtocol,
    height: BlockHeight,
) -> bool {
    activations
        .activation_height_by_name(protocol.activation_upgrade_name())
        .is_some_and(|activation_height| activation_height <= height)
}

fn build_compact_txs(
    txs: &[BuiltTxSpec],
    sapling_c: &[[u8; 32]],
    orchard_c: &[[u8; 32]],
    ironwood_c: &[[u8; 32]],
) -> Vec<CompactTransaction> {
    let Some(coinbase) = txs.first() else {
        return Vec::new();
    };
    if sapling_c.is_empty() && orchard_c.is_empty() && ironwood_c.is_empty() {
        return Vec::new();
    }
    let compact_scan = CompactTransactionData {
        sapling_outputs: sapling_c
            .iter()
            .map(|&commitment| CompactSaplingOutput {
                commitment,
                ephemeral_key: [0; 32],
                ciphertext: [0; 52],
            })
            .collect(),
        orchard_actions: orchard_c
            .iter()
            .map(|&commitment| CompactShieldedAction {
                nullifier: [0; 32],
                commitment,
                ephemeral_key: [0; 32],
                ciphertext: [0; 52],
            })
            .collect(),
        ironwood_actions: ironwood_c
            .iter()
            .map(|&commitment| CompactShieldedAction {
                nullifier: [0; 32],
                commitment,
                ephemeral_key: [0; 32],
                ciphertext: [0; 52],
            })
            .collect(),
        ..CompactTransactionData::default()
    };
    vec![CompactTransaction {
        index: 0,
        transaction_id: coinbase.facts.public_facts.transaction_id,
        data: compact_scan,
    }]
}

fn accumulate_commitments(
    blueprint: &BlockBlueprint,
    activations: &NetworkUpgradeActivations,
    accumulator: &mut CommitmentTreeAccumulator,
) -> Result<AccumulatedCommitments> {
    let height = BlockHeight::new(blueprint.height);
    let sapling_c = if pool_active_at(activations, ShieldedProtocol::Sapling, height) {
        vec![derived_commitment(1, blueprint.block_hash)]
    } else {
        Vec::new()
    };
    let orchard_c = if pool_active_at(activations, ShieldedProtocol::Orchard, height) {
        vec![derived_commitment(2, blueprint.block_hash)]
    } else {
        Vec::new()
    };
    let ironwood_c = if pool_active_at(activations, ShieldedProtocol::Ironwood, height) {
        vec![derived_commitment(3, blueprint.block_hash)]
    } else {
        Vec::new()
    };
    accumulator.append_block_commitments(height, &sapling_c, &orchard_c, &ironwood_c)?;
    Ok(AccumulatedCommitments {
        frontiers: accumulator.validated_frontiers()?,
        tip_metadata: accumulator.tip_metadata(),
        compact_txs: build_compact_txs(&blueprint.txs, &sapling_c, &orchard_c, &ironwood_c),
    })
}

fn split_tx_artifacts(
    height: BlockHeight,
    block_hash: BlockHash,
    txs: Vec<BuiltTxSpec>,
) -> Result<SplitTxArtifacts> {
    let mut blobs = Vec::with_capacity(txs.len());
    let mut facts = Vec::with_capacity(txs.len());
    let mut commit_artifacts = Vec::with_capacity(txs.len());
    for (idx, spec) in txs.into_iter().enumerate() {
        let location = TransactionLocation::new(
            spec.facts.public_facts.transaction_id,
            height,
            block_hash,
            u32::try_from(idx)?,
        );
        blobs.push(TransactionBlobArtifact::new(location, spec.raw_bytes));
        commit_artifacts.push(
            TransactionFactsArtifact::new(location, spec.facts.public_facts.clone())
                .with_transparent_facts(
                    spec.facts.transparent_inputs.clone(),
                    spec.facts.transparent_outputs.clone(),
                ),
        );
        facts.push(spec.facts);
    }
    Ok(SplitTxArtifacts {
        blobs,
        facts,
        commit_artifacts,
    })
}

fn build_final_note_roots(
    height: BlockHeight,
    block_hash: BlockHash,
    frontiers: &CommitmentTreeFrontiers,
) -> BlockFinalNoteCommitmentRoots {
    BlockFinalNoteCommitmentRoots::new(
        height,
        block_hash,
        frontiers.sapling().map(CommitmentTreeFrontier::final_root),
        frontiers.orchard().map(CommitmentTreeFrontier::final_root),
        frontiers.ironwood().map(CommitmentTreeFrontier::final_root),
    )
}

fn assemble_block(
    mut blueprint: BlockBlueprint,
    activations: &NetworkUpgradeActivations,
    accumulator: &mut CommitmentTreeAccumulator,
) -> Result<ConstructedBlock> {
    let height = BlockHeight::new(blueprint.height);
    let block_hash = BlockHash::from_bytes(blueprint.block_hash);
    let parent_hash = BlockHash::from_bytes(blueprint.parent_hash);
    let block_id = BlockId::new(height, block_hash);
    let block_time_u32 = u32::try_from(blueprint.block_time)?;
    let committed = accumulate_commitments(&blueprint, activations, accumulator)?;
    let compact_block = CompactBlockArtifact::new(
        block_id,
        parent_hash,
        block_time_u32,
        committed.compact_txs,
        CompactChainMetadata {
            sapling_commitment_tree_size: committed.tip_metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: committed.tip_metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: committed.tip_metadata.ironwood_commitment_tree_size,
        },
    )?;
    let block_len = u64::try_from(blueprint.raw_block_bytes.len())?;
    let txs = std::mem::take(&mut blueprint.txs);
    let split = split_tx_artifacts(height, block_hash, txs)?;
    let facts = CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            height,
            block_hash,
            parent_hash,
            [0x03; 32],
            [0x04; 32],
            blueprint.block_time,
            TEST_BITS,
            [0x05; 32],
            4,
            block_len,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &blueprint.raw_block_bytes,
        ),
        transactions: split.facts,
    };
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    let final_roots = build_final_note_roots(height, block_hash, &committed.frontiers);
    let build_block = CanonicalBuildBlock {
        facts,
        replay_envelope,
        compact_block,
        tip_metadata: committed.tip_metadata,
        tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
            block_id,
            block_time_u32,
            committed.frontiers,
        )),
        block_final_note_commitment_roots: None,
        transaction_blobs: split.blobs,
        block_blob: Some(BlockBlobArtifact::new(
            height,
            block_hash,
            parent_hash,
            blueprint.raw_block_bytes,
        )),
    };
    let commit_context = BlockCommitContext::new(
        BlockCommitInput {
            height,
            block_hash,
            previous_block_hash: parent_hash,
            block_time_unix_seconds: blueprint.block_time,
            block_size_bytes: block_len,
            transactions: split.commit_artifacts,
            final_note_commitment_roots: Some(final_roots),
        },
        TransparentSpendFacts::Offline,
    );
    Ok(ConstructedBlock {
        build_block,
        final_roots,
        commit_context,
    })
}

fn clone_commit_context(ctx: &BlockCommitContext) -> BlockCommitContext {
    BlockCommitContext::new(
        BlockCommitInput {
            height: ctx.height,
            block_hash: ctx.block_hash,
            previous_block_hash: ctx.previous_block_hash,
            block_time_unix_seconds: ctx.block_time_unix_seconds,
            block_size_bytes: ctx.block_size_bytes,
            transactions: ctx.transactions.clone(),
            final_note_commitment_roots: ctx.final_note_commitment_roots,
        },
        TransparentSpendFacts::Offline,
    )
}

fn build_tailer(
    canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    materialized_view_store: MaterializedViewStore,
    activations: Arc<NetworkUpgradeActivations>,
) -> MaterializedViewTailer {
    MaterializedViewTailer {
        canonical,
        materialized_view_store,
        config: MaterializedViewReplayConfig::DEFAULT,
        activations,
        reorg_window_blocks: TEST_REORG_WINDOW,
        chain_event_retention_window: Some(Duration::from_hours(168)),
        cursor_at_risk_warning: Duration::from_hours(24),
        prevout_caches: ReplayPrevoutCaches::default(),
        volume_space_guard: None,
    }
}

fn seed_baseline_views(
    mv_primary: &MaterializedViewStore,
    b1: BlockCommitContext,
    b2: BlockCommitContext,
) -> Result<()> {
    seed_backfill_views(mv_primary, &[b1, b2])
}

fn seed_backfill_views(
    mv_primary: &MaterializedViewStore,
    contexts: &[BlockCommitContext],
) -> Result<()> {
    let first = contexts
        .first()
        .ok_or_else(|| eyre!("cannot seed empty backfill contexts"))?;
    let last = contexts
        .last()
        .ok_or_else(|| eyre!("cannot seed empty backfill contexts"))?;
    let tail_boundary = BlockHeight::new(last.height.value().saturating_add(1));
    if BlockProductionTimeConsumer::tail_coverage(mv_primary)?.is_none() {
        BlockProductionTimeConsumer::initialize_tail_boundary(mv_primary, tail_boundary)
            .map_err(|error| eyre!("{error}"))?;
    }
    if BlockProductionTimeConsumer::coverage(mv_primary)?.is_none() {
        let rows: Vec<BlockProductionTimeRow> = contexts
            .iter()
            .map(|ctx| BlockProductionTimeRow {
                block_time_unix_seconds: ctx.block_time_unix_seconds,
                block_height: ctx.height,
                block_hash: ctx.block_hash,
            })
            .collect();
        BlockProductionTimeConsumer::write_backfill_rows(
            mv_primary,
            &rows,
            BlockProductionTimeBackfillCoverage::new(
                first.height,
                last.height,
                first.block_time_unix_seconds,
                last.block_time_unix_seconds,
            ),
        )
        .map_err(|error| eyre!("{error}"))?;
    }
    if CommitmentRootSearchConsumer::backfill_coverage(mv_primary)?.is_none() {
        CommitmentRootSearchConsumer::new()
            .write_backfill_batch(
                mv_primary,
                contexts,
                CommitmentRootBackfillCoverage::new(first.height, last.height),
            )
            .map_err(|error| eyre!("{error}"))?;
    }
    if ConventionalFeeDistributionConsumer::coverage(mv_primary)?.is_none() {
        ConventionalFeeDistributionConsumer::new()
            .write_backfill_batch(
                mv_primary,
                contexts,
                ConventionalFeeDistributionBackfillCoverage::new(
                    first.height,
                    last.height,
                    first.block_time_unix_seconds,
                    last.block_time_unix_seconds,
                ),
            )
            .map_err(|error| eyre!("{error}"))?;
    }
    Ok(())
}

struct BaselineBlocks {
    block1: ConstructedBlock,
    block2_old: ConstructedBlock,
    acc: CommitmentTreeAccumulator,
    tx_b1_cb_id: TransactionId,
}

fn build_baseline_blocks(
    activations: &NetworkUpgradeActivations,
    base_time: i64,
) -> Result<BaselineBlocks> {
    let mut acc = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let b1_cb = make_coinbase_spec(1, 625_000_000, [0x11; 20])?;
    let tx_b1_cb_id = b1_cb.facts.public_facts.transaction_id;
    let block1 = assemble_block(
        BlockBlueprint {
            height: 1,
            block_hash: [0x11; 32],
            parent_hash: Network::ZcashRegtest.genesis_hash().as_bytes(),
            block_time: base_time + 10,
            raw_block_bytes: vec![0xB1; 96],
            txs: vec![b1_cb],
        },
        activations,
        &mut acc,
    )?;
    let mut fork_acc = acc.clone();
    let b2_old_cb = make_coinbase_spec(2, 625_000_000, [0x20; 20])?;
    let block2_old = assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [0x20; 32],
            parent_hash: [0x11; 32],
            block_time: base_time + 20,
            raw_block_bytes: vec![0xD2; 96],
            txs: vec![b2_old_cb],
        },
        activations,
        &mut fork_acc,
    )?;
    Ok(BaselineBlocks {
        block1,
        block2_old,
        acc,
        tx_b1_cb_id,
    })
}

struct BaselineOutput {
    canonical_primary: RocksDbCanonicalStore,
    mv_primary: MaterializedViewStore,
    tail_canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    acc: CommitmentTreeAccumulator,
    tx_b1_cb_id: TransactionId,
    b2_old_roots: BlockFinalNoteCommitmentRoots,
}

fn publish_baseline(
    canonical_path: &Path,
    secondary_root: &Path,
    activations: &NetworkUpgradeActivations,
    base_time: i64,
) -> Result<BaselineOutput> {
    let built = build_baseline_blocks(activations, base_time)?;
    let b2_old_roots = built.block2_old.final_roots;
    let b1_mv = clone_commit_context(&built.block1.commit_context);
    let b2_old_mv = clone_commit_context(&built.block2_old.commit_context);
    let tip_cp = built
        .block2_old
        .build_block
        .tree_state_checkpoint
        .clone()
        .ok_or_else(|| eyre!("missing tip checkpoint"))?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        u32::try_from(base_time)?,
        BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0x20; 32])),
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        canonical_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .with_every_block_tree_state_checkpoints()?;
    builder.bulk_load_blocks(
        activations,
        [built.block1.build_block, built.block2_old.build_block]
            .into_iter()
            .map(Ok::<_, std::convert::Infallible>),
    )?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&tip_cp)?;
    let validated = builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
        UnixTimestampMillis::new(u64::try_from(base_time)? * 1_000),
    ))?;
    let primary = validated.publish_baseline(publication)?;
    let mv_primary = open_test_materialized_view_store_for_canonical(canonical_path)?;
    let tail_canonical = Arc::new(RwLock::new(RocksDbCanonicalSecondary::open_ready(
        canonical_path,
        secondary_root.join("tail-canon"),
        activations,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?));
    build_tailer(
        Arc::clone(&tail_canonical),
        mv_primary.clone(),
        Arc::new(activations.clone()),
    )
    .catch_up()?;
    seed_baseline_views(&mv_primary, b1_mv, b2_old_mv)?;
    Ok(BaselineOutput {
        canonical_primary: primary,
        mv_primary,
        tail_canonical,
        acc: built.acc,
        tx_b1_cb_id: built.tx_b1_cb_id,
        b2_old_roots,
    })
}

struct BuiltStoreState {
    canonical_primary: RocksDbCanonicalStore,
    b2_old_roots: BlockFinalNoteCommitmentRoots,
    b2_new_roots: BlockFinalNoteCommitmentRoots,
}

fn apply_reorg_and_append(
    mut base: BaselineOutput,
    activations: &NetworkUpgradeActivations,
    base_time: i64,
) -> Result<BuiltStoreState> {
    let b2_new_cb = make_coinbase_spec(2, 625_015_000, [0x22; 20])?;
    let b2_spend = make_spend_spec(base.tx_b1_cb_id, 0, 624_985_000, [0x29; 20])?;
    let tx_b2_spend_id = b2_spend.facts.public_facts.transaction_id;
    let block2_new = assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [0x22; 32],
            parent_hash: [0x11; 32],
            block_time: base_time + 30,
            raw_block_bytes: vec![0x22; 128],
            txs: vec![b2_new_cb, b2_spend],
        },
        activations,
        &mut base.acc,
    )?;
    let b2_new_roots = block2_new.final_roots;
    let reorg_fence = base.canonical_primary.event_fence();
    let (mid_primary, _) = base.canonical_primary.commit_live_replacement(
        CanonicalLiveReplacement::new(
            reorg_fence,
            vec![CanonicalReplacementBlock::new(
                block2_new.build_block,
                Vec::new(),
            )],
            UnixTimestampMillis::new(u64::try_from(base_time + 30)? * 1_000),
        ),
        activations,
    )?;

    let b3_cb = make_coinbase_spec(3, 625_005_000, [0x33; 20])?;
    let b3_spend = make_spend_spec(tx_b2_spend_id, 0, 624_980_000, [0x39; 20])?;
    let block3 = assemble_block(
        BlockBlueprint {
            height: 3,
            block_hash: [0x33; 32],
            parent_hash: [0x22; 32],
            block_time: base_time + 40,
            raw_block_bytes: vec![0x33; 128],
            txs: vec![b3_cb, b3_spend],
        },
        activations,
        &mut base.acc,
    )?;
    let append_fence = mid_primary.event_fence();
    let (final_primary, _) = mid_primary.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            block3.build_block,
            Vec::new(),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
            UnixTimestampMillis::new(u64::try_from(base_time + 40)? * 1_000),
        ),
        activations,
    )?;

    base.tail_canonical.write().try_catch_up()?;
    build_tailer(
        Arc::clone(&base.tail_canonical),
        base.mv_primary.clone(),
        Arc::new(activations.clone()),
    )
    .catch_up()?;
    Ok(BuiltStoreState {
        canonical_primary: final_primary,
        b2_old_roots: base.b2_old_roots,
        b2_new_roots,
    })
}

async fn spawn_wallet_grpc(
    mut wallet_fixture: WalletServingStoreFixture,
    cancel: &CancellationToken,
) -> Result<(
    SocketAddr,
    broadcast::Sender<wallet::ChainEventEnvelope>,
    JoinHandle<()>,
)> {
    let (wallet_canon, wallet_store) = wallet_fixture.take_readers()?;
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(wallet_canon)?),
        Arc::new(RocksDbWalletReader::new(wallet_store)),
    )?);
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        ingest_control,
    );
    let (wallet_service, wallet_events_tx) = TestWalletQuery::new(WalletQueryGrpcAdapter::new(
        wallet_query,
        WalletEndpointMetadata::default(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        let _keep = (wallet_fixture, ingest_control_fixture);
        let _ = Server::builder()
            .add_service(WalletQueryServer::new(wallet_service))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await;
    });
    Ok((addr, wallet_events_tx, task))
}

async fn spawn_explorer_grpc(
    explorer_canonical: RocksDbCanonicalSecondary,
    mv_secondary: MaterializedViewStore,
    wallet_addr: SocketAddr,
    cancel: &CancellationToken,
) -> Result<(SocketAddr, JoinHandle<()>)> {
    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_canonical_store(Arc::new(RwLock::new(explorer_canonical)))
    .with_materialized_view_store(mv_secondary)
    .with_wallet_query_endpoint(format!("http://{wallet_addr}"))
    .with_prevout_resolution_online(true);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(explorer_adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await;
    });
    Ok((addr, task))
}

async fn spawn_cipherscan_http(
    explorer_addr: SocketAddr,
    wallet_addr: SocketAddr,
    cancel: &CancellationToken,
) -> Result<(SocketAddr, axum::Router, JoinHandle<()>)> {
    let explorer_channel = connect_zinder_grpc(&format!("http://{explorer_addr}"), None).await?;
    let wallet_channel = connect_zinder_grpc(&format!("http://{wallet_addr}"), None).await?;
    let market_price_endpoints = CipherscanMarketPriceEndpoints::new(
        reqwest::Url::parse("http://127.0.0.1:9/current")?,
        "http://127.0.0.1:9/history/{date}".to_owned(),
    )?;
    let rest_adapter = CipherscanRestAdapter::new(
        Network::ZcashRegtest,
        explorer_channel,
        wallet_channel,
        market_price_endpoints,
        cancel.clone(),
    )?;
    let router = rest_adapter.router();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let http_router = router.clone();
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        let _ = serve_http(
            listener,
            http_router,
            HttpListenerLimits::default(),
            shutdown.cancelled_owned(),
        )
        .await;
    });
    Ok((addr, router, task))
}

struct E2eEnv {
    _canonical_dir: TempDir,
    _secondary_dir: TempDir,
    b2_old_hash: BlockHash,
    b2_new_hash: BlockHash,
    b3_hash: BlockHash,
    b2_old_roots: BlockFinalNoteCommitmentRoots,
    b2_new_roots: BlockFinalNoteCommitmentRoots,
    wallet_server_addr: SocketAddr,
    explorer_server_addr: SocketAddr,
    http_addr: SocketAddr,
    router: axum::Router,
    wallet_events_tx: broadcast::Sender<wallet::ChainEventEnvelope>,
    tip_epoch: ChainEpoch,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

fn open_e2e_secondaries(
    canonical_path: &Path,
    secondary_dir: &Path,
) -> Result<(RocksDbCanonicalSecondary, MaterializedViewStore, ChainEpoch)> {
    let explorer_canonical = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        canonical_path,
        secondary_dir.join("canonical"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let tip_epoch = explorer_canonical.chain_epoch()?;
    let mv_secondary = MaterializedViewStore::open_explorer_views_secondary(
        MaterializedViewStore::path_for_canonical(canonical_path),
        secondary_dir.join("materialized-views"),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    mv_secondary.try_catch_up()?;
    Ok((explorer_canonical, mv_secondary, tip_epoch))
}

async fn setup_cipherscan_e2e_env() -> Result<E2eEnv> {
    let now_secs = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
    let base_time = now_secs - 600;
    let activations = sample_regtest_upgrade_activations();
    let canonical_dir = TempDir::new()?;
    let canonical_path = canonical_dir.path().join("canonical");
    let secondary_dir = TempDir::new()?;

    let base = publish_baseline(
        &canonical_path,
        secondary_dir.path(),
        &activations,
        base_time,
    )?;
    let built = apply_reorg_and_append(base, &activations, base_time)?;

    let wallet_fixture = WalletServingStoreFixture::from_published_canonical(
        TempDir::new()?,
        &canonical_path,
        built.canonical_primary,
        &activations,
    )?;
    let (explorer_canonical, mv_secondary, tip_epoch) =
        open_e2e_secondaries(&canonical_path, secondary_dir.path())?;

    let cancel = CancellationToken::new();
    let (wallet_server_addr, wallet_events_tx, wallet_task) =
        spawn_wallet_grpc(wallet_fixture, &cancel).await?;
    let (explorer_server_addr, explorer_task) = spawn_explorer_grpc(
        explorer_canonical,
        mv_secondary,
        wallet_server_addr,
        &cancel,
    )
    .await?;
    let (http_addr, router, http_task) =
        spawn_cipherscan_http(explorer_server_addr, wallet_server_addr, &cancel).await?;

    Ok(E2eEnv {
        _canonical_dir: canonical_dir,
        _secondary_dir: secondary_dir,
        b2_old_hash: BlockHash::from_bytes([0x20; 32]),
        b2_new_hash: BlockHash::from_bytes([0x22; 32]),
        b3_hash: BlockHash::from_bytes([0x33; 32]),
        b2_old_roots: built.b2_old_roots,
        b2_new_roots: built.b2_new_roots,
        wallet_server_addr,
        explorer_server_addr,
        http_addr,
        router,
        wallet_events_tx,
        tip_epoch,
        cancel,
        tasks: vec![wallet_task, explorer_task, http_task],
    })
}

async fn get_json(client: &reqwest::Client, url: &str) -> Result<Value> {
    let response = client.get(url).send().await?;
    let status = response.status();
    let body_text = response.text().await?;
    if !status.is_success() {
        return Err(eyre!("GET {url} returned HTTP {status}: {body_text}"));
    }
    Ok(serde_json::from_str(&body_text)?)
}

fn first_root_hex(roots: BlockFinalNoteCommitmentRoots) -> Option<String> {
    roots
        .sapling
        .or(roots.orchard)
        .or(roots.ironwood)
        .map(|root| hex::encode(root.as_bytes()))
}

async fn assert_cipherscan_block_routes(
    env: &E2eEnv,
    client: &reqwest::Client,
    base: &str,
) -> Result<()> {
    let oneshot_response = env
        .router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/api/blocks")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(oneshot_response.status(), StatusCode::OK);

    let blocks_json = get_json(client, &format!("{base}/api/blocks")).await?;
    assert_eq!(blocks_json["pagination"]["total"], json!("3"));
    assert_eq!(blocks_json["blocks"].as_array().map(Vec::len), Some(3));

    let blocks_list_json = get_json(client, &format!("{base}/api/blocks/list")).await?;
    assert_eq!(blocks_list_json["success"], json!(true));

    let block_by_height = get_json(client, &format!("{base}/api/block/2")).await?;
    assert_eq!(block_by_height["height"], json!("2"));
    assert!(block_by_height["miner_address"].is_string());
    let listed_block2 = blocks_json["blocks"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["height"] == json!("2")))
        .ok_or_else(|| eyre!("block 2 missing from /api/blocks"))?;
    assert_eq!(block_by_height["total_fees"], listed_block2["total_fees"]);
    assert_eq!(block_by_height["total_fees"], json!("15000"));

    let canonical_hash = encode_rpc_block_hash_hex(env.b2_new_hash);
    let block_by_hash = get_json(client, &format!("{base}/api/block/{canonical_hash}")).await?;
    assert_eq!(block_by_hash["height"], json!("2"));

    let displaced_hash = encode_rpc_block_hash_hex(env.b2_old_hash);
    let displaced_block = get_json(client, &format!("{base}/api/block/{displaced_hash}")).await?;
    assert_eq!(displaced_block["isOrphaned"], json!(true));
    Ok(())
}

async fn assert_cipherscan_analytics_and_uncles(
    env: &E2eEnv,
    client: &reqwest::Client,
    base: &str,
) -> Result<()> {
    let network_stats = get_json(client, &format!("{base}/api/network/stats")).await?;
    assert_eq!(network_stats["success"], json!(true));

    let fee_dist = get_json(
        client,
        &format!("{base}/api/network/fee-distribution?period=24h"),
    )
    .await?;
    assert_eq!(fee_dist["feeBasis"], json!("zip317_conventional_fallback"));

    if let Some(root_hex) = first_root_hex(env.b2_new_roots) {
        let anchor_ok = get_json(client, &format!("{base}/api/search/anchor/{root_hex}")).await?;
        assert_eq!(anchor_ok["found"], json!(true));
        assert_eq!(anchor_ok["canonical"].as_array().map(Vec::len), Some(1));
    }
    if let Some(displaced_root_hex) = first_root_hex(env.b2_old_roots) {
        let anchor_displaced = get_json(
            client,
            &format!("{base}/api/search/anchor/{displaced_root_hex}"),
        )
        .await?;
        assert_eq!(anchor_displaced["found"], json!(false));
        assert_eq!(
            anchor_displaced["displacedCoverage"]["available"],
            json!(true)
        );
        assert_eq!(
            anchor_displaced["displacedCoverage"]["capturedBlockCount"],
            json!(1)
        );
        assert_eq!(
            anchor_displaced["displacedCoverage"]["rootArtifactUnavailableCount"],
            json!(1)
        );
    }

    let uncles = get_json(client, &format!("{base}/api/uncles")).await?;
    assert_eq!(uncles["success"], json!(true));

    let displaced_hash = encode_rpc_block_hash_hex(env.b2_old_hash);
    let uncle_detail = get_json(client, &format!("{base}/api/uncle/{displaced_hash}")).await?;
    assert_eq!(uncle_detail["success"], json!(true));
    Ok(())
}

async fn assert_cipherscan_mining_routes(client: &reqwest::Client, base: &str) -> Result<()> {
    let pool_dist = get_json(
        client,
        &format!("{base}/api/mining/pool-distribution?period=24h"),
    )
    .await?;
    assert_eq!(pool_dist["period"], json!("24h"));
    assert_eq!(pool_dist["totalBlocks"], json!(3));

    let mining_rewards = get_json(client, &format!("{base}/api/mining/rewards?period=24h")).await?;
    assert_eq!(mining_rewards["period"], json!("24h"));
    assert_eq!(mining_rewards["feeBasis"], json!("paid"));

    let mining_metrics = get_json(
        client,
        &format!("{base}/api/network/mining-metrics?period=24h"),
    )
    .await?;
    assert_eq!(mining_metrics["success"], json!(true));
    Ok(())
}

async fn assert_cipherscan_rest_routes(env: &E2eEnv) -> Result<()> {
    let client = reqwest::Client::new();
    let base = format!("http://{}", env.http_addr);
    assert_cipherscan_block_routes(env, &client, &base).await?;
    assert_cipherscan_analytics_and_uncles(env, &client, &base).await?;
    assert_cipherscan_mining_routes(&client, &base).await?;
    Ok(())
}

async fn write_all(stream: &TcpStream, mut payload: &[u8]) -> Result<()> {
    while !payload.is_empty() {
        stream.writable().await?;
        match stream.try_write(payload) {
            Ok(written) => payload = &payload[written..],
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn read_more(stream: &TcpStream, buffer: &mut Vec<u8>) -> Result<usize> {
    let mut chunk = [0_u8; 4096];
    loop {
        stream.readable().await?;
        match stream.try_read(&mut chunk) {
            Ok(count) => {
                buffer.extend_from_slice(&chunk[..count]);
                return Ok(count);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
    }
}

async fn ensure_bytes(stream: &TcpStream, buffer: &mut Vec<u8>, needed: usize) -> Result<()> {
    while buffer.len() < needed {
        if read_more(stream, buffer).await? == 0 {
            return Err(eyre!("unexpected EOF waiting for WebSocket bytes"));
        }
    }
    Ok(())
}

async fn next_ws_frame(stream: &TcpStream, buffer: &mut Vec<u8>) -> Result<(u8, Vec<u8>)> {
    ensure_bytes(stream, buffer, 2).await?;
    let opcode = buffer[0] & 0x0f;
    let len_byte = buffer[1] & 0x7f;
    let mut header_len = 2_usize;
    let payload_len = match len_byte {
        126 => {
            header_len = 4;
            ensure_bytes(stream, buffer, header_len).await?;
            usize::from(u16::from_be_bytes([buffer[2], buffer[3]]))
        }
        127 => {
            header_len = 10;
            ensure_bytes(stream, buffer, header_len).await?;
            let raw_len = u64::from_be_bytes([
                buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7], buffer[8],
                buffer[9],
            ]);
            usize::try_from(raw_len)?
        }
        short => usize::from(short),
    };
    ensure_bytes(stream, buffer, header_len + payload_len).await?;
    let payload = buffer[header_len..header_len + payload_len].to_vec();
    buffer.drain(..header_len + payload_len);
    Ok((opcode, payload))
}

async fn open_ws_stream(http_addr: SocketAddr) -> Result<(TcpStream, Vec<u8>)> {
    let stream = TcpStream::connect(http_addr).await?;
    let upgrade_req = format!(
        "GET / HTTP/1.1\r\nHost: {http_addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    write_all(&stream, upgrade_req.as_bytes()).await?;
    let mut buffer = Vec::new();
    let header_end = loop {
        if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        if read_more(&stream, &mut buffer).await? == 0 {
            return Err(eyre!("EOF during WebSocket HTTP upgrade"));
        }
    };
    let head = std::str::from_utf8(&buffer[..header_end])?;
    assert!(
        head.contains("101"),
        "expected HTTP 101 upgrade, got: {head}"
    );
    buffer.drain(..header_end);
    Ok((stream, buffer))
}

async fn emit_block3_chain_event(env: &E2eEnv) -> Result<()> {
    for _ in 0..200 {
        if env.wallet_events_tx.receiver_count() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        env.wallet_events_tx.receiver_count() > 0,
        "relay_chain_events never subscribed"
    );
    let epoch_msg = chain_epoch_message(env.tip_epoch);
    let chain_view = zinder_store::chain_view_message(env.tip_epoch);
    let committed = wallet::ChainEpochCommitted {
        chain_epoch: Some(epoch_msg),
        start_height: 3,
        end_height: 3,
    };
    let _ = env.wallet_events_tx.send(wallet::ChainEventEnvelope {
        cursor: vec![3, 0, 0, 0],
        event_sequence: env.tip_epoch.id.value(),
        chain_view: Some(chain_view),
        event: Some(wallet::chain_event_envelope::Event::ChainCommitted(
            wallet::ChainCommitted {
                committed: Some(committed),
            },
        )),
    });
    Ok(())
}

async fn assert_realtime_new_block_ws(env: &E2eEnv) -> Result<()> {
    let (stream, mut buffer) = open_ws_stream(env.http_addr).await?;
    emit_block3_chain_event(env).await?;

    let matched_frame: Value = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (opcode, payload) = next_ws_frame(&stream, &mut buffer).await?;
            if opcode == 0x9 {
                continue;
            }
            if opcode != 0x1 {
                return Err(eyre!("unexpected WebSocket opcode {opcode}"));
            }
            let parsed: Value = serde_json::from_slice(&payload)?;
            if parsed["type"] == json!("new_block") {
                return Ok(parsed);
            }
        }
    })
    .await??;

    let expected_hash = encode_rpc_block_hash_hex(env.b3_hash);
    assert_eq!(matched_frame["data"]["hash"], json!(expected_hash));
    Ok(())
}

#[tokio::test]
async fn cipherscan_canonical_routes_and_realtime_websocket_succeed() -> Result<()> {
    let env = setup_cipherscan_e2e_env().await?;
    assert!(env.wallet_server_addr.port() > 0);
    assert!(env.explorer_server_addr.port() > 0);

    assert_cipherscan_rest_routes(&env).await?;
    assert_realtime_new_block_ws(&env).await?;

    env.cancel.cancel();
    for task in env.tasks {
        let _ = task.await;
    }
    Ok(())
}

fn synthetic_block_hash(height: u32) -> [u8; 32] {
    let mut hash = [0_u8; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash[31] = 1;
    hash
}

fn build_long_fee_chain(
    block_count: u32,
    activations: &NetworkUpgradeActivations,
    base_time: i64,
) -> Result<(Vec<ConstructedBlock>, CommitmentTreeAccumulator)> {
    let mut acc = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let mut blocks = Vec::with_capacity(usize::try_from(block_count)?);
    let mut prev_txid = None;
    let mut prev_reward = 0_u64;
    for height in 1..=block_count {
        let miner_tag = if height.is_multiple_of(2) { 0xAA } else { 0xBB };
        let reward_zat = if height == 1 {
            625_000_000
        } else {
            625_001_000
        };
        let coinbase = make_coinbase_spec(height, reward_zat, [miner_tag; 20])?;
        let cb_txid = coinbase.facts.public_facts.transaction_id;
        let mut txs = vec![coinbase];
        if let Some(parent_txid) = prev_txid {
            txs.push(make_spend_spec(
                parent_txid,
                0,
                prev_reward - 1_000,
                [0xCC; 20],
            )?);
        }
        let parent_hash = if height == 1 {
            Network::ZcashRegtest.genesis_hash().as_bytes()
        } else {
            synthetic_block_hash(height - 1)
        };
        let constructed = assemble_block(
            BlockBlueprint {
                height,
                block_hash: synthetic_block_hash(height),
                parent_hash,
                block_time: base_time + i64::from(height) * 2,
                raw_block_bytes: vec![miner_tag; 96],
                txs,
            },
            activations,
            &mut acc,
        )?;
        prev_txid = Some(cb_txid);
        prev_reward = reward_zat;
        blocks.push(constructed);
    }
    Ok((blocks, acc))
}

fn publish_long_fee_chain_stores(
    canonical_path: &Path,
    secondary_dir: &Path,
    block_count: u32,
    activations: &NetworkUpgradeActivations,
    base_time: i64,
) -> Result<RocksDbCanonicalStore> {
    let (blocks, _) = build_long_fee_chain(block_count, activations, base_time)?;
    let tip_cp = blocks
        .last()
        .and_then(|blk| blk.build_block.tree_state_checkpoint.clone())
        .ok_or_else(|| eyre!("missing tip checkpoint"))?;
    let mv_contexts: Vec<_> = blocks
        .iter()
        .map(|blk| clone_commit_context(&blk.commit_context))
        .collect();
    let settled_height = block_count - TEST_REORG_WINDOW;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        u32::try_from(base_time)?,
        BlockId::new(
            BlockHeight::new(block_count),
            BlockHash::from_bytes(synthetic_block_hash(block_count)),
        ),
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        canonical_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .with_every_block_tree_state_checkpoints()?;
    builder.bulk_load_blocks(
        activations,
        blocks
            .into_iter()
            .map(|blk| Ok::<_, std::convert::Infallible>(blk.build_block)),
    )?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&tip_cp)?;
    let validated = builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        BlockId::new(
            BlockHeight::new(settled_height),
            BlockHash::from_bytes(synthetic_block_hash(settled_height)),
        ),
        UnixTimestampMillis::new(u64::try_from(base_time)? * 1_000),
    ))?;
    let primary = validated.publish_baseline(publication)?;
    let mv_primary = open_test_materialized_view_store_for_canonical(canonical_path)?;
    let tail_canonical = Arc::new(RwLock::new(RocksDbCanonicalSecondary::open_ready(
        canonical_path,
        secondary_dir.join("tail-canon"),
        activations,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?));
    build_tailer(
        tail_canonical,
        mv_primary.clone(),
        Arc::new(activations.clone()),
    )
    .catch_up()?;
    seed_backfill_views(&mv_primary, &mv_contexts)?;
    Ok(primary)
}

#[tokio::test]
async fn pool_distribution_succeeds_across_more_than_256_distinct_parent_blocks() -> Result<()> {
    let block_count = 270_u32;
    let now_secs = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
    let base_time = now_secs - 1_200;
    let activations = sample_regtest_upgrade_activations();
    let canonical_dir = TempDir::new()?;
    let canonical_path = canonical_dir.path().join("canonical");
    let secondary_dir = TempDir::new()?;
    let canonical_primary = publish_long_fee_chain_stores(
        &canonical_path,
        secondary_dir.path(),
        block_count,
        &activations,
        base_time,
    )?;

    let wallet_fixture = WalletServingStoreFixture::from_published_canonical(
        TempDir::new()?,
        &canonical_path,
        canonical_primary,
        &activations,
    )?;
    let (explorer_canonical, mv_secondary, _) =
        open_e2e_secondaries(&canonical_path, secondary_dir.path())?;

    let cancel = CancellationToken::new();
    let (wallet_addr, _, wallet_task) = spawn_wallet_grpc(wallet_fixture, &cancel).await?;
    let (explorer_addr, explorer_task) =
        spawn_explorer_grpc(explorer_canonical, mv_secondary, wallet_addr, &cancel).await?;
    let (http_addr, _, http_task) =
        spawn_cipherscan_http(explorer_addr, wallet_addr, &cancel).await?;

    let client = reqwest::Client::new();
    let pool_dist = get_json(
        &client,
        &format!("http://{http_addr}/api/mining/pool-distribution?period=24h"),
    )
    .await?;
    assert_eq!(pool_dist["period"], json!("24h"));
    assert_eq!(pool_dist["totalBlocks"], json!(270));
    let pools = pool_dist["pools"]
        .as_array()
        .ok_or_else(|| eyre!("missing pools array"))?;
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0]["blocks"], json!(270));
    assert_eq!(pools[0]["totalFeesZat"], json!("269000"));

    cancel.cancel();
    for task in [wallet_task, explorer_task, http_task] {
        let _ = task.await;
    }
    Ok(())
}
