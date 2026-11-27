//! End-to-end integration tests for canonical-backed `ExplorerQuery` RPCs.
#![allow(
    missing_docs,
    reason = "Integration tests exercise ExplorerQuery canonical RPCs end-to-end."
)]

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use eyre::{Result, eyre};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Server};
use zebra_chain::serialization::{ZcashDeserialize as _, ZcashSerialize as _};
use zebra_chain::transaction::Transaction as ZebraTransaction;
use zinder_core::wire::{encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex};
use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainTipMetadata,
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
    EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1, EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
    EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1, EXPLORER_BLOCK_TRANSACTIONS_V2,
    EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1, EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
    EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1, EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
    EXPLORER_PAID_FEE_DISTRIBUTION_V1, EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
};
use zinder_proto::v1::explorer::{
    BlockDetailRequest, BlockProductionInTimeRangeRequest, BlockProductionSeriesRequest,
    CommitmentRootSearchRequest, DisplacedBlockDetailRequest, DisplacedBlockHistoryRequest,
    PaidFeeDistributionRequest, ServerInfoRequest, TransactionDetailRequest,
    TransactionHistoryRequest, block_detail_request, explorer_query_client::ExplorerQueryClient,
};
use zinder_proto::v1::wallet::wallet_query_server::WalletQueryServer;
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{AuthenticatedChannel, connect_zinder_grpc};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreRequirements, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::{
    IngestControlFixture, WalletServingStoreFixture,
    open_test_materialized_view_store_for_canonical, sample_regtest_upgrade_activations,
};

const TEST_REORG_WINDOW: u32 = 4;
const TEST_BITS: u32 = 0x200f_0f0f;
const BASE_TIME: i64 = 1_750_000_100;
const BLOCK1_TIME: i64 = BASE_TIME;
const BLOCK2_OLD_TIME: i64 = BASE_TIME + 20;
const BLOCK2_NEW_TIME: i64 = BASE_TIME + 40;
const BLOCK3_TIME: i64 = BASE_TIME + 60;

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

struct ChainFixtureBundle {
    _canonical_dir: TempDir,
    _secondary_dir: TempDir,
    wallet_fixture: Option<WalletServingStoreFixture>,
    canonical_secondary: Option<RocksDbCanonicalSecondary>,
    mv_secondary: MaterializedViewStore,
    b2_old_hash: BlockHash,
    b2_old_raw_block_bytes: Vec<u8>,
    b2_new_hash: BlockHash,
    b2_old_roots: BlockFinalNoteCommitmentRoots,
    b2_new_roots: BlockFinalNoteCommitmentRoots,
    tx_b2_spend_id: TransactionId,
    tx_b2_new_cb_id: TransactionId,
    tx_b3_intra_child_id: TransactionId,
    tx_b1_script: Vec<u8>,
}

struct ServerHandle {
    addr: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl ServerHandle {
    async fn shutdown(self) -> Result<()> {
        self.cancel.cancel();
        self.task.await??;
        Ok(())
    }
}

fn p2pkh_script(hash160: [u8; 20]) -> Vec<u8> {
    let mut script = Vec::with_capacity(25);
    script.extend_from_slice(&[0x76, 0xa9, 0x14]);
    script.extend_from_slice(&hash160);
    script.extend_from_slice(&[0x88, 0xac]);
    script
}

fn double_sha256(bytes: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(bytes);
    Sha256::digest(first).into()
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
    script_sig.extend_from_slice(&[0x04, b'z', b'n', b'd', b'r']);
    bytes.push(u8::try_from(script_sig.len())?);
    bytes.extend_from_slice(&script_sig);
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&reward_zat.to_le_bytes());
    let script_pub_key = p2pkh_script(pubkey_hash);
    bytes.push(u8::try_from(script_pub_key.len())?);
    bytes.extend_from_slice(&script_pub_key);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    let _parsed = ZebraTransaction::zcash_deserialize(bytes.as_slice())?;
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
    let _parsed = ZebraTransaction::zcash_deserialize(bytes.as_slice())?;
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
    let txid = TransactionId::from_bytes(double_sha256(&raw_bytes));
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
    let txid = TransactionId::from_bytes(double_sha256(&raw_bytes));
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

fn make_sprout_spend_spec(
    outpoint: (TransactionId, u32),
    output: (u64, [u8; 20]),
    vpub_balances: (u64, u64),
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<BuiltTxSpec> {
    let (prev_txid, prev_vout) = outpoint;
    let (output_zat, recipient_hash) = output;
    let (vpub_old, vpub_new) = vpub_balances;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../zinder-ingest/tests/fixtures/zcash-mainnet-sprout-block-396.json"
    ))?;
    let raw_block = hex::decode(
        fixture
            .get("raw_block_hex")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| eyre!("Mainnet block 396 fixture is missing raw_block_hex"))?,
    )?;
    let block = zebra_chain::block::Block::zcash_deserialize(&mut std::io::Cursor::new(raw_block))?;
    let joinsplit_transaction = block
        .transactions
        .get(1)
        .ok_or_else(|| eyre!("Mainnet block 396 has no JoinSplit transaction"))?
        .zcash_serialize_to_vec()?;
    let joinsplit_tail_start = joinsplit_transaction
        .len()
        .checked_sub(1 + 1_802 + 32 + 64)
        .ok_or_else(|| eyre!("JoinSplit transaction is shorter than one description"))?;
    let mut prefix = build_v1_spend_bytes(prev_txid, prev_vout, output_zat, recipient_hash)?;
    prefix[..4].copy_from_slice(&2_u32.to_le_bytes());
    let mut tail = joinsplit_transaction[joinsplit_tail_start..].to_vec();
    tail[1..9].copy_from_slice(&vpub_old.to_le_bytes());
    tail[9..17].copy_from_slice(&vpub_new.to_le_bytes());
    let mut raw_bytes = prefix;
    raw_bytes.extend_from_slice(&tail);

    let parsed =
        zinder_source::parse_transaction_public_fact_set(&raw_bytes, Some(height), activations)?;
    let facts = CanonicalTransactionFacts {
        public_facts: parsed.public_facts,
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&raw_bytes),
        intrinsic_value_balances: parsed.intrinsic_value_balances,
        transparent_inputs: parsed.transparent_inputs,
        transparent_outputs: parsed.transparent_outputs,
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

fn build_canonical_block_facts(
    blueprint: &BlockBlueprint,
    tx_facts: Vec<CanonicalTransactionFacts>,
) -> Result<CanonicalBlockFacts> {
    Ok(CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            BlockHeight::new(blueprint.height),
            BlockHash::from_bytes(blueprint.block_hash),
            BlockHash::from_bytes(blueprint.parent_hash),
            [0x03; 32],
            [0x04; 32],
            blueprint.block_time,
            TEST_BITS,
            [0x05; 32],
            4,
            u64::try_from(blueprint.raw_block_bytes.len())?,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &blueprint.raw_block_bytes,
        ),
        transactions: tx_facts,
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
    let txs = std::mem::take(&mut blueprint.txs);
    let split = split_tx_artifacts(height, block_hash, txs)?;
    for blob in &split.blobs {
        blueprint
            .raw_block_bytes
            .extend_from_slice(&blob.raw_transaction_bytes);
    }
    let facts = build_canonical_block_facts(&blueprint, split.facts)?;
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    let final_roots = build_final_note_roots(height, block_hash, &committed.frontiers);
    let block_len = u64::try_from(blueprint.raw_block_bytes.len())?;
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

fn seed_two_baseline_blocks(
    mv_primary: &MaterializedViewStore,
    b1: BlockCommitContext,
    b2: BlockCommitContext,
) -> Result<()> {
    let contexts = vec![b1, b2];
    if BlockProductionTimeConsumer::tail_coverage(mv_primary)?.is_none() {
        BlockProductionTimeConsumer::initialize_tail_boundary(mv_primary, BlockHeight::new(3))
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
                BlockHeight::new(1),
                BlockHeight::new(2),
                BLOCK1_TIME,
                BLOCK2_OLD_TIME,
            ),
        )
        .map_err(|error| eyre!("{error}"))?;
    }
    if CommitmentRootSearchConsumer::backfill_coverage(mv_primary)?.is_none() {
        CommitmentRootSearchConsumer::new()
            .write_backfill_batch(
                mv_primary,
                &contexts,
                CommitmentRootBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(2)),
            )
            .map_err(|error| eyre!("{error}"))?;
    }
    if ConventionalFeeDistributionConsumer::coverage(mv_primary)?.is_none() {
        ConventionalFeeDistributionConsumer::new()
            .write_backfill_batch(
                mv_primary,
                &contexts,
                ConventionalFeeDistributionBackfillCoverage::new(
                    BlockHeight::new(1),
                    BlockHeight::new(2),
                    BLOCK1_TIME,
                    BLOCK2_OLD_TIME,
                ),
            )
            .map_err(|error| eyre!("{error}"))?;
    }
    Ok(())
}

struct BaselineBlocks {
    block1: ConstructedBlock,
    block2_old: ConstructedBlock,
    base_acc: CommitmentTreeAccumulator,
    tx_b1_cb_id: TransactionId,
    tx_b1_script: Vec<u8>,
}

fn build_baseline_blocks(activations: &NetworkUpgradeActivations) -> Result<BaselineBlocks> {
    let mut base_acc = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let b1_cb = make_coinbase_spec(1, 625_000_000, [0x11; 20])?;
    let tx_b1_cb_id = b1_cb.facts.public_facts.transaction_id;
    let tx_b1_script = p2pkh_script([0x11; 20]);
    let block1 = assemble_block(
        BlockBlueprint {
            height: 1,
            block_hash: [0x11; 32],
            parent_hash: Network::ZcashRegtest.genesis_hash().as_bytes(),
            block_time: BLOCK1_TIME,
            raw_block_bytes: vec![0xB1; 96],
            txs: vec![b1_cb],
        },
        activations,
        &mut base_acc,
    )?;
    let mut fork_acc = base_acc.clone();
    let b2_old_cb = make_coinbase_spec(2, 625_000_000, [0x20; 20])?;
    let block2_old = assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [0x20; 32],
            parent_hash: [0x11; 32],
            block_time: BLOCK2_OLD_TIME,
            raw_block_bytes: vec![0xD2; 96],
            txs: vec![b2_old_cb],
        },
        activations,
        &mut fork_acc,
    )?;
    Ok(BaselineBlocks {
        block1,
        block2_old,
        base_acc,
        tx_b1_cb_id,
        tx_b1_script,
    })
}

struct BaselineOutput {
    mv_primary: MaterializedViewStore,
    tail_canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    base_acc: CommitmentTreeAccumulator,
    tx_b1_cb_id: TransactionId,
    tx_b1_script: Vec<u8>,
    b2_old_roots: BlockFinalNoteCommitmentRoots,
    b2_old_raw_block_bytes: Vec<u8>,
}

fn publish_baseline_store(
    canonical_path: &Path,
    secondary_root: &Path,
    activations: &NetworkUpgradeActivations,
) -> Result<(RocksDbCanonicalStore, BaselineOutput)> {
    let built = build_baseline_blocks(activations)?;
    let b2_old_roots = built.block2_old.final_roots;
    let b2_old_raw_block_bytes = built
        .block2_old
        .build_block
        .block_blob
        .as_ref()
        .ok_or_else(|| eyre!("missing b2_old block blob"))?
        .raw_block_bytes
        .clone();
    let b1_mv = clone_commit_context(&built.block1.commit_context);
    let b2_old_mv = clone_commit_context(&built.block2_old.commit_context);
    let tip_cp = built
        .block2_old
        .build_block
        .tree_state_checkpoint
        .clone()
        .ok_or_else(|| eyre!("missing baseline tip checkpoint"))?;

    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        u32::try_from(BLOCK1_TIME - 10)?,
        BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0x20; 32])),
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        canonical_path,
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
        UnixTimestampMillis::new(1_750_000_000_000),
    ))?;
    let canonical_primary = validated.publish_baseline(publication)?;
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
    seed_two_baseline_blocks(&mv_primary, b1_mv, b2_old_mv)?;

    Ok((
        canonical_primary,
        BaselineOutput {
            mv_primary,
            tail_canonical,
            base_acc: built.base_acc,
            tx_b1_cb_id: built.tx_b1_cb_id,
            tx_b1_script: built.tx_b1_script,
            b2_old_roots,
            b2_old_raw_block_bytes,
        },
    ))
}

struct ReorgOutput {
    canonical_primary: RocksDbCanonicalStore,
    b2_new_roots: BlockFinalNoteCommitmentRoots,
    tx_b2_new_cb_id: TransactionId,
    tx_b2_spend_id: TransactionId,
    tx_b3_intra_child_id: TransactionId,
}

fn commit_reorg_block2(
    baseline: &mut BaselineOutput,
    activations: &NetworkUpgradeActivations,
    primary: RocksDbCanonicalStore,
) -> Result<(
    RocksDbCanonicalStore,
    BlockFinalNoteCommitmentRoots,
    TransactionId,
    TransactionId,
)> {
    let b2_new_cb = make_coinbase_spec(2, 625_015_000, [0x22; 20])?;
    let tx_b2_new_cb_id = b2_new_cb.facts.public_facts.transaction_id;
    let b2_spend = make_sprout_spend_spec(
        (baseline.tx_b1_cb_id, 0),
        (624_900_000, [0x29; 20]),
        (95_000, 10_000),
        BlockHeight::new(2),
        activations,
    )?;
    let tx_b2_spend_id = b2_spend.facts.public_facts.transaction_id;
    let block2_new = assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [0x22; 32],
            parent_hash: [0x11; 32],
            block_time: BLOCK2_NEW_TIME,
            raw_block_bytes: vec![0x22; 128],
            txs: vec![b2_new_cb, b2_spend],
        },
        activations,
        &mut baseline.base_acc,
    )?;
    let b2_new_roots = block2_new.final_roots;
    let reorg_fence = primary.event_fence();
    let (next_primary, _) = primary.commit_live_replacement(
        CanonicalLiveReplacement::new(
            reorg_fence,
            vec![CanonicalReplacementBlock::new(
                block2_new.build_block,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_100),
        ),
        activations,
    )?;
    Ok((next_primary, b2_new_roots, tx_b2_new_cb_id, tx_b2_spend_id))
}

fn commit_append_block3(
    accumulator: &mut CommitmentTreeAccumulator,
    activations: &NetworkUpgradeActivations,
    primary: RocksDbCanonicalStore,
    tx_b2_spend_id: TransactionId,
) -> Result<(RocksDbCanonicalStore, TransactionId)> {
    let b3_cb = make_coinbase_spec(3, 625_007_000, [0x33; 20])?;
    let b3_intra = make_spend_spec(tx_b2_spend_id, 0, 624_895_000, [0x3A; 20])?;
    let b3_intra_child = make_spend_spec(
        b3_intra.facts.public_facts.transaction_id,
        0,
        624_893_000,
        [0x3B; 20],
    )?;
    let tx_b3_intra_child_id = b3_intra_child.facts.public_facts.transaction_id;
    let block3 = assemble_block(
        BlockBlueprint {
            height: 3,
            block_hash: [0x33; 32],
            parent_hash: [0x22; 32],
            block_time: BLOCK3_TIME,
            raw_block_bytes: vec![0x33; 128],
            txs: vec![b3_cb, b3_intra, b3_intra_child],
        },
        activations,
        accumulator,
    )?;
    let append_fence = primary.event_fence();
    let (next_primary, _) = primary.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            block3.build_block,
            Vec::new(),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
            UnixTimestampMillis::new(1_750_000_000_200),
        ),
        activations,
    )?;
    Ok((next_primary, tx_b3_intra_child_id))
}

fn apply_live_reorg_and_append(
    baseline: &mut BaselineOutput,
    canonical_primary: RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
) -> Result<ReorgOutput> {
    let (mid_primary, b2_new_roots, tx_b2_new_cb_id, tx_b2_spend_id) =
        commit_reorg_block2(baseline, activations, canonical_primary)?;
    let (next_primary, tx_b3_intra_child_id) = commit_append_block3(
        &mut baseline.base_acc,
        activations,
        mid_primary,
        tx_b2_spend_id,
    )?;
    baseline.tail_canonical.write().try_catch_up()?;
    build_tailer(
        Arc::clone(&baseline.tail_canonical),
        baseline.mv_primary.clone(),
        Arc::new(activations.clone()),
    )
    .catch_up()?;
    Ok(ReorgOutput {
        canonical_primary: next_primary,
        b2_new_roots,
        tx_b2_new_cb_id,
        tx_b2_spend_id,
        tx_b3_intra_child_id,
    })
}

fn open_explorer_secondaries(
    canonical_path: &Path,
    secondary_root: &Path,
) -> Result<(RocksDbCanonicalSecondary, MaterializedViewStore)> {
    let explorer_canonical = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        canonical_path,
        secondary_root.join("canonical"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let mv_primary_path = MaterializedViewStore::path_for_canonical(canonical_path);
    let mv_secondary = MaterializedViewStore::open_explorer_views_secondary(
        &mv_primary_path,
        secondary_root.join("materialized-views"),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    mv_secondary.try_catch_up()?;
    Ok((explorer_canonical, mv_secondary))
}

fn setup_canonical_chain_fixture() -> Result<ChainFixtureBundle> {
    let activations = sample_regtest_upgrade_activations();
    let canonical_dir = TempDir::new()?;
    let canonical_path = canonical_dir.path().join("canonical");
    let secondary_dir = TempDir::new()?;

    let (canonical_primary, mut baseline) =
        publish_baseline_store(&canonical_path, secondary_dir.path(), &activations)?;
    let tx_b1_script = baseline.tx_b1_script.clone();
    let b2_old_roots = baseline.b2_old_roots;
    let b2_old_raw_block_bytes = baseline.b2_old_raw_block_bytes.clone();
    let reorg = apply_live_reorg_and_append(&mut baseline, canonical_primary, &activations)?;
    drop(baseline);

    let wallet_dir = TempDir::new()?;
    let wallet_fixture = WalletServingStoreFixture::from_published_canonical(
        wallet_dir,
        &canonical_path,
        reorg.canonical_primary,
        &activations,
    )?;
    let (canonical_secondary, mv_secondary) =
        open_explorer_secondaries(&canonical_path, secondary_dir.path())?;

    Ok(ChainFixtureBundle {
        _canonical_dir: canonical_dir,
        _secondary_dir: secondary_dir,
        wallet_fixture: Some(wallet_fixture),
        canonical_secondary: Some(canonical_secondary),
        mv_secondary,
        b2_old_hash: BlockHash::from_bytes([0x20; 32]),
        b2_old_raw_block_bytes,
        b2_new_hash: BlockHash::from_bytes([0x22; 32]),
        b2_old_roots,
        b2_new_roots: reorg.b2_new_roots,
        tx_b2_spend_id: reorg.tx_b2_spend_id,
        tx_b2_new_cb_id: reorg.tx_b2_new_cb_id,
        tx_b3_intra_child_id: reorg.tx_b3_intra_child_id,
        tx_b1_script,
    })
}

struct WalletServerFixtures {
    _wallet_fixture: WalletServingStoreFixture,
    _ingest_control: IngestControlFixture,
}

async fn spawn_wallet_server(
    mut wallet_fixture: WalletServingStoreFixture,
) -> Result<(ServerHandle, WalletServerFixtures)> {
    let (canonical, wallet) = wallet_fixture.take_readers()?;
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        ingest_control,
    );
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(WalletQueryServer::new(adapter))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await
    });
    Ok((
        ServerHandle { addr, cancel, task },
        WalletServerFixtures {
            _wallet_fixture: wallet_fixture,
            _ingest_control: ingest_control_fixture,
        },
    ))
}

async fn spawn_explorer_server(
    adapter: ExplorerQueryGrpcAdapter,
) -> Result<(ServerHandle, ExplorerQueryClient<AuthenticatedChannel>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await
    });
    let channel = connect_zinder_grpc(&format!("http://{addr}"), None).await?;
    Ok((
        ServerHandle { addr, cancel, task },
        ExplorerQueryClient::new(channel),
    ))
}

fn first_present_root(roots: BlockFinalNoteCommitmentRoots) -> Option<Vec<u8>> {
    roots
        .sapling
        .or(roots.orchard)
        .or(roots.ironwood)
        .map(|root| root.as_bytes().to_vec())
}

async fn assert_server_info_and_unserved_paid_fees(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
) -> Result<()> {
    let descriptor = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .and_then(|server_info| server_info.common)
        .ok_or_else(|| eyre!("Explorer ServerInfo missing common"))?;
    for expected in [
        EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
        EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
        EXPLORER_BLOCK_TRANSACTIONS_V2,
        EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1,
        EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
        EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1,
        EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
        EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
        EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
    ] {
        assert!(
            descriptor.capabilities.iter().any(|cap| cap == expected),
            "missing capability {expected}: {:?}",
            descriptor.capabilities
        );
    }
    assert!(
        !descriptor
            .capabilities
            .iter()
            .any(|cap| cap == EXPLORER_PAID_FEE_DISTRIBUTION_V1),
        "PaidFeeDistribution must stay unadvertised"
    );

    let paid_fee_err = client
        .paid_fee_distribution(PaidFeeDistributionRequest {
            start_time_unix_seconds: BLOCK1_TIME,
            end_time_unix_seconds: BLOCK3_TIME + 10,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("PaidFeeDistribution must return FailedPrecondition"))?;
    assert_eq!(paid_fee_err.code(), Code::FailedPrecondition);
    Ok(())
}

async fn assert_block_production_series_rpc(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
    tx_b2_new_cb_id: TransactionId,
) -> Result<()> {
    let series = client
        .block_production_series(BlockProductionSeriesRequest {
            start_height: 1,
            end_height: 5,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(series.covered_block_count, 3);
    assert_eq!(series.missing_block_count, 2);
    assert_eq!(series.points.len(), 3);

    let p2 = &series.points[1];
    let p2_summary = p2
        .summary
        .as_ref()
        .ok_or_else(|| eyre!("series point 2 missing summary"))?;
    assert_eq!(p2_summary.block_height, 2);
    assert_eq!(p2_summary.paid_fees_collected_zat, Some(15_000));
    assert_eq!(p2.bits, TEST_BITS);
    let coinbase2 = p2
        .coinbase
        .as_ref()
        .ok_or_else(|| eyre!("series point 2 missing coinbase"))?;
    assert_eq!(
        coinbase2.transaction_id,
        encode_rpc_transaction_id_hex(tx_b2_new_cb_id)
    );
    assert_eq!(coinbase2.transparent_outputs[0].value_zat, 625_015_000);

    let p3_summary = series.points[2]
        .summary
        .as_ref()
        .ok_or_else(|| eyre!("series point 3 missing summary"))?;
    assert_eq!(p3_summary.paid_fees_collected_zat, Some(7_000));
    Ok(())
}

async fn assert_block_production_time_range_rpc(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
) -> Result<()> {
    let page1 = client
        .block_production_in_time_range(BlockProductionInTimeRangeRequest {
            start_time_unix_seconds: BLOCK1_TIME,
            end_time_unix_seconds: BLOCK3_TIME + 100,
            max_entries: 2,
            from_cursor: Vec::new(),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(page1.points.len(), 2);
    assert_eq!(page1.covered_block_count, 2);
    assert_eq!(page1.missing_paid_fee_count, 0);
    assert_eq!(page1.missing_coinbase_count, 0);
    assert_ne!(page1.next_cursor, [] as [u8; 0]);

    let page2 = client
        .block_production_in_time_range(BlockProductionInTimeRangeRequest {
            start_time_unix_seconds: BLOCK1_TIME,
            end_time_unix_seconds: BLOCK3_TIME + 100,
            max_entries: 2,
            from_cursor: page1.next_cursor,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(page2.points.len(), 1);
    assert_eq!(page2.next_cursor, [] as [u8; 0]);
    Ok(())
}

async fn assert_block_transactions_rpc(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
    fixture: &ChainFixtureBundle,
) -> Result<()> {
    let by_height = client
        .block_transactions(BlockDetailRequest {
            selector: Some(block_detail_request::Selector::BlockHeight(2)),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let by_hash = client
        .block_transactions(BlockDetailRequest {
            selector: Some(block_detail_request::Selector::BlockHash(
                encode_rpc_block_hash_hex(fixture.b2_new_hash),
            )),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(by_height.transactions, by_hash.transactions);
    assert_eq!(by_height.summary, by_hash.summary);
    assert_eq!(
        by_height
            .summary
            .as_ref()
            .and_then(|summary| summary.paid_fees_collected_zat),
        Some(15_000)
    );
    assert_eq!(by_height.transactions.len(), 2);
    assert_eq!(by_height.transactions[0].transparent_inputs, []);
    assert_eq!(
        by_height.transactions[1].transparent_inputs[0].value_zat,
        Some(625_000_000)
    );
    assert_eq!(
        by_height.transactions[1].transparent_inputs[0].script_pub_key,
        Some(fixture.tx_b1_script.clone())
    );
    assert!(by_height.final_note_commitment_roots.is_some());

    let block3_txs = client
        .block_transactions(BlockDetailRequest {
            selector: Some(block_detail_request::Selector::BlockHeight(3)),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(
        block3_txs
            .summary
            .as_ref()
            .and_then(|summary| summary.paid_fees_collected_zat),
        Some(7_000)
    );
    assert_eq!(block3_txs.transactions.len(), 3);
    assert_eq!(
        block3_txs.transactions[2].transparent_inputs[0].value_zat,
        Some(624_895_000)
    );

    let displaced_err = client
        .block_transactions(BlockDetailRequest {
            selector: Some(block_detail_request::Selector::BlockHash(
                encode_rpc_block_hash_hex(fixture.b2_old_hash),
            )),
            at_epoch_id: None,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("displaced hash must return NotFound from BlockTransactions"))?;
    assert_eq!(displaced_err.code(), Code::NotFound);
    Ok(())
}

async fn assert_commitment_root_search_rpc(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
    fixture: &ChainFixtureBundle,
) -> Result<()> {
    if let Some(canonical_root) = first_present_root(fixture.b2_new_roots) {
        let canonical_search = client
            .commitment_root_search(CommitmentRootSearchRequest {
                root: canonical_root,
                max_matches: 10,
            })
            .await?
            .into_inner();
        assert_eq!(canonical_search.matches.len(), 1);
        assert_eq!(canonical_search.matches[0].block_height, 2);
        assert_eq!(canonical_search.displaced_matches, []);
    }
    if let Some(displaced_root) = first_present_root(fixture.b2_old_roots) {
        let displaced_search = client
            .commitment_root_search(CommitmentRootSearchRequest {
                root: displaced_root,
                max_matches: 10,
            })
            .await?
            .into_inner();
        assert_eq!(displaced_search.matches, []);
        assert_eq!(displaced_search.displaced_matches.len(), 1);
        assert_eq!(displaced_search.displaced_matches[0].block_height, 2);
        assert_eq!(
            displaced_search.displaced_matches[0].block_hash,
            encode_rpc_block_hash_hex(fixture.b2_old_hash)
        );
        let displaced_cov = displaced_search
            .displaced_coverage
            .ok_or_else(|| eyre!("missing displaced_coverage"))?;
        assert_eq!(displaced_cov.captured_block_count, 1);
        assert_eq!(displaced_cov.root_artifact_unavailable_count, 0);
        assert!(displaced_cov.captured_range_complete);
    }
    Ok(())
}

async fn assert_displaced_block_rpcs(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
    fixture: &ChainFixtureBundle,
) -> Result<()> {
    let history = client
        .displaced_block_history(DisplacedBlockHistoryRequest {
            page_size: 10,
            cursor: Vec::new(),
        })
        .await?
        .into_inner();
    assert_eq!(history.total_count, 1);
    assert_eq!(history.entries.len(), 1);
    assert!(!history.has_more);
    let displaced_summary = history.entries[0]
        .block
        .as_ref()
        .ok_or_else(|| eyre!("missing displaced block summary"))?;
    assert_eq!(
        displaced_summary.block_hash,
        encode_rpc_block_hash_hex(fixture.b2_old_hash)
    );
    let counterpart = history.entries[0]
        .current_canonical_block
        .as_ref()
        .ok_or_else(|| eyre!("missing canonical counterpart"))?;
    assert_eq!(
        counterpart.block_hash,
        encode_rpc_block_hash_hex(fixture.b2_new_hash)
    );
    assert_eq!(counterpart.transaction_count, 2);

    let detail = client
        .displaced_block_detail(DisplacedBlockDetailRequest {
            block_hash: encode_rpc_block_hash_hex(fixture.b2_old_hash),
        })
        .await?
        .into_inner();
    assert_eq!(
        detail.raw_block_bytes,
        Some(fixture.b2_old_raw_block_bytes.clone())
    );

    let canonical_err = client
        .displaced_block_detail(DisplacedBlockDetailRequest {
            block_hash: encode_rpc_block_hash_hex(fixture.b2_new_hash),
        })
        .await
        .err()
        .ok_or_else(|| eyre!("canonical block hash must not appear in DisplacedBlockDetail"))?;
    assert_eq!(canonical_err.code(), Code::NotFound);
    Ok(())
}

async fn assert_intrinsic_value_balances_rpcs(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
    fixture: &ChainFixtureBundle,
) -> Result<()> {
    let tx_detail = client
        .transaction_detail(TransactionDetailRequest {
            transaction_id: encode_rpc_transaction_id_hex(fixture.tx_b2_spend_id),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let balances = tx_detail
        .intrinsic_value_balances
        .ok_or_else(|| eyre!("TransactionDetail missing intrinsic_value_balances"))?;
    assert_eq!(balances.sprout_zat, -85_000);
    assert_eq!(balances.sapling_zat, 0);
    assert_eq!(balances.orchard_zat, 0);
    assert_eq!(balances.ironwood_zat, 0);

    let tx_history = client
        .transaction_history(TransactionHistoryRequest {
            page_size: 20,
            start: None,
            direction: 0,
            filter: None,
            include_total_count: false,
            read_fence: None,
        })
        .await?
        .into_inner();
    let target_hex = encode_rpc_transaction_id_hex(fixture.tx_b2_spend_id);
    let matched_entry = tx_history
        .entries
        .iter()
        .find(|entry| entry.transaction_id == target_hex)
        .ok_or_else(|| eyre!("tx_b2_spend absent from TransactionHistory"))?;
    let history_balances = matched_entry
        .intrinsic_value_balances
        .ok_or_else(|| eyre!("TransactionHistory missing intrinsic_value_balances"))?;
    assert_eq!(history_balances.sprout_zat, -85_000);
    assert_eq!(
        encode_rpc_transaction_id_hex(fixture.tx_b3_intra_child_id).len(),
        64
    );
    Ok(())
}

#[tokio::test]
async fn explorer_canonical_rpcs_and_paid_fee_unserved_end_to_end() -> Result<()> {
    let mut fixture = setup_canonical_chain_fixture()?;
    let wallet_fixture = fixture
        .wallet_fixture
        .take()
        .ok_or_else(|| eyre!("missing wallet fixture"))?;
    let canonical_secondary = fixture
        .canonical_secondary
        .take()
        .ok_or_else(|| eyre!("missing canonical secondary"))?;
    let (wallet_server, _wallet_fixtures) = spawn_wallet_server(wallet_fixture).await?;
    let shared_canonical = Arc::new(RwLock::new(canonical_secondary));
    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_canonical_store(shared_canonical)
    .with_materialized_view_store(fixture.mv_secondary.clone())
    .with_wallet_query_endpoint(format!("http://{}", wallet_server.addr))
    .with_prevout_resolution_online(true);

    let (explorer_server, mut client) = spawn_explorer_server(explorer_adapter).await?;

    assert_server_info_and_unserved_paid_fees(&mut client).await?;
    assert_block_production_series_rpc(&mut client, fixture.tx_b2_new_cb_id).await?;
    assert_block_production_time_range_rpc(&mut client).await?;
    assert_block_transactions_rpc(&mut client, &fixture).await?;
    assert_commitment_root_search_rpc(&mut client, &fixture).await?;
    assert_displaced_block_rpcs(&mut client, &fixture).await?;
    assert_intrinsic_value_balances_rpcs(&mut client, &fixture).await?;

    explorer_server.shutdown().await?;
    wallet_server.shutdown().await?;
    Ok(())
}

async fn assert_canonical_rpcs_fail_precondition(
    client: &mut ExplorerQueryClient<AuthenticatedChannel>,
) -> Result<()> {
    let series_status = client
        .block_production_series(BlockProductionSeriesRequest {
            start_height: 1,
            end_height: 1,
            at_epoch_id: None,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("expected FailedPrecondition"))?;
    assert_eq!(series_status.code(), Code::FailedPrecondition);

    let txs_status = client
        .block_transactions(BlockDetailRequest {
            selector: Some(block_detail_request::Selector::BlockHeight(1)),
            at_epoch_id: None,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("expected FailedPrecondition"))?;
    assert_eq!(txs_status.code(), Code::FailedPrecondition);

    let displaced_status = client
        .displaced_block_history(DisplacedBlockHistoryRequest {
            page_size: 10,
            cursor: Vec::new(),
        })
        .await
        .err()
        .ok_or_else(|| eyre!("expected FailedPrecondition"))?;
    assert_eq!(displaced_status.code(), Code::FailedPrecondition);
    Ok(())
}

#[tokio::test]
async fn explorer_without_canonical_secondary_degrades_gracefully() -> Result<()> {
    let mut fixture = setup_canonical_chain_fixture()?;
    let wallet_fixture = fixture
        .wallet_fixture
        .take()
        .ok_or_else(|| eyre!("missing wallet fixture"))?;
    let (wallet_server, _wallet_fixtures) = spawn_wallet_server(wallet_fixture).await?;
    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_materialized_view_store(fixture.mv_secondary.clone())
    .with_wallet_query_endpoint(format!("http://{}", wallet_server.addr));

    let (explorer_server, mut client) = spawn_explorer_server(explorer_adapter).await?;
    let descriptor = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .and_then(|server_info| server_info.common)
        .ok_or_else(|| eyre!("Explorer ServerInfo missing common"))?;
    for canonical_cap in [
        EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
        EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
        EXPLORER_BLOCK_TRANSACTIONS_V2,
        EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
        EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
        EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
        EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
    ] {
        assert!(
            !descriptor
                .capabilities
                .iter()
                .any(|cap| cap == canonical_cap),
            "canonical capability {canonical_cap} must be omitted without canonical secondary"
        );
    }

    assert_canonical_rpcs_fail_precondition(&mut client).await?;
    explorer_server.shutdown().await?;
    wallet_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn lagging_mv_after_same_height_reorg_reports_incomplete_coverage() -> Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let canonical_dir = TempDir::new()?;
    let canonical_path = canonical_dir.path().join("canonical");
    let secondary_dir = TempDir::new()?;

    let (canonical_primary, mut baseline) =
        publish_baseline_store(&canonical_path, secondary_dir.path(), &activations)?;
    let (canonical_primary, b2_new_roots, _, _) =
        commit_reorg_block2(&mut baseline, &activations, canonical_primary)?;
    // Leave mv_primary at baseline epoch 1 / height 2 without running MaterializedViewTailer catch-up.
    let (canonical_secondary, mv_secondary) =
        open_explorer_secondaries(&canonical_path, secondary_dir.path())?;
    let wallet_dir = TempDir::new()?;
    let wallet_fixture = WalletServingStoreFixture::from_published_canonical(
        wallet_dir,
        &canonical_path,
        canonical_primary,
        &activations,
    )?;
    let (wallet_server, _wallet_fixtures) = spawn_wallet_server(wallet_fixture).await?;
    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_canonical_store(Arc::new(RwLock::new(canonical_secondary)))
    .with_materialized_view_store(mv_secondary)
    .with_wallet_query_endpoint(format!("http://{}", wallet_server.addr));
    let (explorer_server, mut client) = spawn_explorer_server(explorer_adapter).await?;

    let time_range = client
        .block_production_in_time_range(BlockProductionInTimeRangeRequest {
            start_time_unix_seconds: BLOCK2_NEW_TIME,
            end_time_unix_seconds: BLOCK2_NEW_TIME + 10,
            max_entries: 10,
            from_cursor: Vec::new(),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert!(
        !time_range
            .coverage
            .ok_or_else(|| eyre!("missing time range coverage"))?
            .requested_range_complete
    );

    let root = first_present_root(b2_new_roots).ok_or_else(|| eyre!("missing b2_new root"))?;
    let root_search = client
        .commitment_root_search(CommitmentRootSearchRequest {
            root,
            max_matches: 10,
        })
        .await?
        .into_inner();
    assert!(
        !root_search
            .coverage
            .ok_or_else(|| eyre!("missing root search coverage"))?
            .canonical_history_complete
    );

    explorer_server.shutdown().await?;
    wallet_server.shutdown().await?;
    Ok(())
}

struct SharedRootDisplacementBundle {
    canonical_dir: TempDir,
    secondary_dir: TempDir,
    primary_store: RocksDbCanonicalStore,
    stale_h2_canonical: RocksDbCanonicalSecondary,
    mv_secondary: MaterializedViewStore,
    shared_root: Vec<u8>,
    b2_alt2_hash: BlockHash,
}

fn build_block2_with_hash(
    activations: &NetworkUpgradeActivations,
    base_acc: &CommitmentTreeAccumulator,
    hash_fill: u8,
    block_time: i64,
) -> Result<ConstructedBlock> {
    let mut acc = base_acc.clone();
    let coinbase = make_coinbase_spec(2, 625_000_000, [hash_fill; 20])?;
    assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [hash_fill; 32],
            parent_hash: [0x11; 32],
            block_time,
            raw_block_bytes: vec![hash_fill; 96],
            txs: vec![coinbase],
        },
        activations,
        &mut acc,
    )
}

fn replace_block2_sequence(
    mut primary: RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
    steps: Vec<(CanonicalBuildBlock, u64)>,
) -> Result<RocksDbCanonicalStore> {
    for (block, ts) in steps {
        let fence = primary.event_fence();
        let (next, _) = primary.commit_live_replacement(
            CanonicalLiveReplacement::new(
                fence,
                vec![CanonicalReplacementBlock::new(block, Vec::new())],
                UnixTimestampMillis::new(ts),
            ),
            activations,
        )?;
        primary = next;
    }
    Ok(primary)
}

fn append_block3_with_shared_root(
    primary: RocksDbCanonicalStore,
    baseline: &BaselineOutput,
    activations: &NetworkUpgradeActivations,
    shared_sapling: [u8; 32],
) -> Result<RocksDbCanonicalStore> {
    let mut b3_acc = baseline.base_acc.clone();
    let _ = assemble_block(
        BlockBlueprint {
            height: 2,
            block_hash: [0x23; 32],
            parent_hash: [0x11; 32],
            block_time: BLOCK2_NEW_TIME + 5,
            raw_block_bytes: vec![0x23; 96],
            txs: vec![make_coinbase_spec(2, 625_000_000, [0x23; 20])?],
        },
        activations,
        &mut b3_acc,
    )?;
    let mut b3 = assemble_block(
        BlockBlueprint {
            height: 3,
            block_hash: [0x33; 32],
            parent_hash: [0x23; 32],
            block_time: BLOCK3_TIME,
            raw_block_bytes: vec![0x33; 96],
            txs: vec![make_coinbase_spec(3, 625_000_000, [0x33; 20])?],
        },
        activations,
        &mut b3_acc,
    )?;
    let forced_b3 = BlockFinalNoteCommitmentRoots::new(
        BlockHeight::new(3),
        BlockHash::from_bytes([0x33; 32]),
        Some(zinder_core::FinalNoteCommitmentRoot::from_bytes(
            shared_sapling,
        )),
        b3.final_roots.orchard,
        b3.final_roots.ironwood,
    );
    b3.commit_context.final_note_commitment_roots = Some(forced_b3);
    let append_fence = primary.event_fence();
    let (next_primary, _) = primary.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            b3.build_block,
            Vec::new(),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
            UnixTimestampMillis::new(1_750_000_000_500),
        ),
        activations,
    )?;
    baseline.tail_canonical.write().try_catch_up()?;
    build_tailer(
        Arc::clone(&baseline.tail_canonical),
        baseline.mv_primary.clone(),
        Arc::new(activations.clone()),
    )
    .catch_up()?;
    CommitmentRootSearchConsumer::new()
        .write_backfill_batch(
            &baseline.mv_primary,
            &[b3.commit_context],
            CommitmentRootBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(3)),
        )
        .map_err(|error| eyre!("{error}"))?;
    Ok(next_primary)
}

fn setup_shared_root_displacement_bundle() -> Result<SharedRootDisplacementBundle> {
    let activations = sample_regtest_upgrade_activations();
    let canonical_dir = TempDir::new()?;
    let canonical_path = canonical_dir.path().join("canonical");
    let secondary_dir = TempDir::new()?;
    let (primary, baseline) =
        publish_baseline_store(&canonical_path, secondary_dir.path(), &activations)?;
    let alt1 = build_block2_with_hash(&activations, &baseline.base_acc, 0x21, BLOCK2_NEW_TIME)?;
    let alt2_first =
        build_block2_with_hash(&activations, &baseline.base_acc, 0x23, BLOCK2_NEW_TIME + 5)?;
    let alt2_second =
        build_block2_with_hash(&activations, &baseline.base_acc, 0x23, BLOCK2_NEW_TIME + 5)?;
    let alt3 = build_block2_with_hash(&activations, &baseline.base_acc, 0x25, BLOCK2_NEW_TIME + 8)?;
    let shared_sapling = alt2_second
        .final_roots
        .sapling
        .ok_or_else(|| eyre!("missing alt2 sapling final root"))?
        .as_bytes();

    // Event 2 displaces b2_old, Event 3 displaces alt1, Event 4 displaces alt2,
    // Event 5 displaces alt3 and re-adopts alt2 as canonical at height 2.
    let primary = replace_block2_sequence(
        primary,
        &activations,
        vec![
            (alt1.build_block, 1_750_000_000_100),
            (alt2_first.build_block, 1_750_000_000_200),
            (alt3.build_block, 1_750_000_000_300),
            (alt2_second.build_block, 1_750_000_000_400),
        ],
    )?;

    // Freeze Explorer canonical secondary at height 2 (alt2) before appending block 3 for MV.
    let stale_h2_canonical = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &canonical_path,
        secondary_dir.path().join("stale-h2"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let primary = append_block3_with_shared_root(primary, &baseline, &activations, shared_sapling)?;
    let (_latest_canonical, mv_secondary) =
        open_explorer_secondaries(&canonical_path, secondary_dir.path())?;
    Ok(SharedRootDisplacementBundle {
        canonical_dir,
        secondary_dir,
        primary_store: primary,
        stale_h2_canonical,
        mv_secondary,
        shared_root: shared_sapling.to_vec(),
        b2_alt2_hash: BlockHash::from_bytes([0x23; 32]),
    })
}

#[tokio::test]
async fn commitment_root_search_pages_past_ahead_mv_candidates_and_reports_displaced_coverage()
-> Result<()> {
    let bundle = setup_shared_root_displacement_bundle()?;
    let _keep_alive = (
        bundle.canonical_dir,
        bundle.secondary_dir,
        bundle.primary_store,
    );
    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_canonical_store(Arc::new(RwLock::new(bundle.stale_h2_canonical)))
    .with_materialized_view_store(bundle.mv_secondary);
    let (explorer_server, mut client) = spawn_explorer_server(explorer_adapter).await?;

    let response = client
        .commitment_root_search(CommitmentRootSearchRequest {
            root: bundle.shared_root,
            max_matches: 1,
        })
        .await?
        .into_inner();
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].block_height, 2);
    assert_eq!(
        response.matches[0].block_hash,
        encode_rpc_block_hash_hex(bundle.b2_alt2_hash)
    );
    let canonical_coverage = response
        .coverage
        .as_ref()
        .ok_or_else(|| eyre!("missing canonical coverage"))?;
    assert!(!canonical_coverage.canonical_history_complete);
    let displaced_coverage = response
        .displaced_coverage
        .as_ref()
        .ok_or_else(|| eyre!("missing displaced coverage"))?;
    assert_eq!(displaced_coverage.captured_block_count, 4);
    assert_eq!(displaced_coverage.root_artifact_unavailable_count, 0);
    assert!(displaced_coverage.captured_range_complete);
    explorer_server.shutdown().await?;
    Ok(())
}
