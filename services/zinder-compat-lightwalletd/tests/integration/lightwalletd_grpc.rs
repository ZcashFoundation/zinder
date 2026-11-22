#![allow(
    missing_docs,
    reason = "Integration test names describe the compatibility behavior under test."
)]

mod browser_streams;
mod published_range;

use async_trait::async_trait;
use eyre::eyre;
use parking_lot::Mutex;
use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status, transport::Server};
use tonic_types::StatusExt as _;
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, serialization::ZcashDeserializeInto as _,
    transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::{
    BLOCK_RANGE_CHUNK_MAX_BYTES, BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES, CanonicalReaderSlot,
    CompactBlocksGrpcAdapter, DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS, LightwalletdGrpcAdapter,
    LightwalletdPing, NodeTipWitness, StaleTipLimit,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, BlockSelector, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastRejected, BroadcastRejectionReason, BroadcastUnknown,
    ChainEpochId, CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, Network, NetworkUpgradeActivations, RawTransactionBytes,
    SUBTREE_LEAF_COUNT, ShieldedProtocol, StandardFee, SubtreeRootHash, SubtreeRootIndex,
    SubtreeRootRange, TransactionBroadcastOutcome, TransactionComponentCounts, TransactionId,
    TransactionLocation, TransparentAddressBalance, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentSpendFact, TransparentUnspentOutput,
    wire::{
        encode_height_key_ascending, encode_internal_block_hash, encode_rpc_block_hash_bytes,
        encode_rpc_block_hash_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, ChainMetadata, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer,
};

use zinder_query::{
    ArtifactKey, BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents, CompactBlock, CompactBlockRange,
    DEFAULT_MAX_COMPACT_BLOCK_RANGE, FullBlock, FullBlockStream,
    MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES, OrderedBroadcaster, QueryError, RawTransaction,
    RocksDbCanonicalReader, RocksDbWalletReader, SettledTipBlock, StorageReadError, SubtreeRoots,
    Transaction, TransactionStatus, TransparentAddressTxIds, TransparentAddressTxIdsInRangeRequest,
    TransparentAddressUnspentOutputs, TransparentAddressUnspentOutputsRequest,
    TransparentOutpointSpendStatuses, TreeState, VisibleTipBlock, WalletQueryApi,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{
    ArtifactFamily, CanonicalBuildSubtreeRoot, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, HeightOrder, RawBlobRetention, StreamCursorTokenV1,
    encode_compact_block_artifact,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FIXTURE_NOTE_COMMITMENT, FixtureBlock,
    FixtureTransactionRows, LogCapture, MockTransactionBroadcaster, WalletServingStoreFixture,
    commitment_tree_checkpoints, completed_subtree_frontier, frontier_of_size, one_leaf_frontier,
    sample_regtest_upgrade_activations, synthetic_transaction_public_facts, with_note_commitments,
};

const ACCEPTANCE_BLOCK_HEIGHT: BlockHeight = BlockHeight::new(1);

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Acceptance test keeps the read-sync RPC matrix together."
)]
async fn lightwalletd_adapter_serves_read_sync_methods() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let visible_tip_block = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();
    let block = adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let block_range = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged_blocks = collect_stream(block_range).await?;
    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let latest_tree_state_checkpoint = adapter
        .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let subtree_roots = adapter
        .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
            max_entries: 1,
        }))
        .await?
        .into_inner();
    let subtree_roots = collect_stream(subtree_roots).await?;
    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(visible_tip_block.height, 1);
    assert_eq!(block.height, 1);
    assert_eq!(block.vtx.len(), 1);
    assert!(
        block.chain_metadata.is_some(),
        "GetBlock must retain commitment-tree sizes"
    );
    assert_eq!(ranged_blocks.len(), 1);
    assert_eq!(ranged_blocks[0].height, 1);
    assert_eq!(ranged_blocks[0].vtx[0].vin.len(), 0);
    assert_eq!(ranged_blocks[0].vtx[0].vout.len(), 0);
    assert!(
        ranged_blocks[0].chain_metadata.is_some(),
        "GetBlockRange must retain commitment-tree sizes"
    );
    assert_eq!(tree_state.height, 1);
    assert_eq!(latest_tree_state_checkpoint.height, tree_state.height);
    assert_eq!(latest_tree_state_checkpoint.hash, tree_state.hash);
    assert_eq!(
        latest_tree_state_checkpoint.sapling_tree,
        tree_state.sapling_tree
    );
    assert_eq!(
        subtree_roots,
        [] as [zinder_proto::compat::lightwalletd::SubtreeRoot; 0]
    );
    assert_eq!(lightd_info.vendor, "Zinder");
    // Regtest collapses to "test" per BIP70 (Zebra's NetworkKind::bip70_network_name).
    // Wallet SDKs match `chainName` against ID_TESTNET, which is "test".
    assert_eq!(lightd_info.chain_name, "test");
    assert_eq!(lightd_info.block_height, 1);
    // The fixture tip is old, so the network estimate runs ahead of it.
    assert!(lightd_info.estimated_height >= lightd_info.block_height);
    assert_eq!(
        lightd_info.lightwallet_protocol_version,
        lightwalletd::LIGHTWALLETD_PROTOCOL_VERSION
    );
    assert!(
        !lightd_info.taddr_support,
        "the generic adapter must not advertise transparent-address support"
    );
    // upgradeName and upgradeHeight name the next pending upgrade: in the
    // sample regtest table, NU5 activates at height 2.
    assert_eq!(lightd_info.upgrade_name, "NU5");
    assert_eq!(lightd_info.upgrade_height, 2);

    Ok(())
}

fn unix_now() -> eyre::Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}

/// `GetLightdInfo` estimates the network height from the served tip.
///
/// The estimate is the tip while its block is fresh, and the tip plus the
/// blocks the target spacing would have produced since, once it is old, as
/// Zebra estimates it. `blockHeight` stays the served tip.
#[tokio::test]
async fn lightd_info_estimates_the_network_height_from_the_served_tip() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let activations = recorder.network_upgrade_activations().await?;

    // A tip's block time never changes, so the adapter reads it once per
    // tip; each tip age below uses its own adapter.
    recorder.set_block_time(unix_now()?);
    let fresh = LightwalletdGrpcAdapter::new(recorder.clone())
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    assert_eq!(
        fresh.block_height,
        u64::from(ACCEPTANCE_BLOCK_HEIGHT.value())
    );
    assert_eq!(fresh.estimated_height, fresh.block_height);

    let tip_time = unix_now()? - 3_000;
    recorder.set_block_time(tip_time);
    let stale = LightwalletdGrpcAdapter::new(recorder.clone())
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let expected = zinder_compat_lightwalletd::estimated_network_height(
        &activations,
        ACCEPTANCE_BLOCK_HEIGHT,
        tip_time,
        unix_now()?,
    );
    assert_eq!(
        stale.block_height,
        u64::from(ACCEPTANCE_BLOCK_HEIGHT.value())
    );
    assert!(
        stale.estimated_height.abs_diff(u64::from(expected.value())) <= 1,
        "estimated {} where {} was expected",
        stale.estimated_height,
        expected.value()
    );
    assert!(stale.estimated_height > stale.block_height + 10);
    Ok(())
}

/// Tip calls refuse a stale served tip and answer again for a fresh one.
///
/// With no report from the writer's node, once the served tip is older than
/// the stale-tip limit, `GetLatestBlock` and `GetLightdInfo` fail with
/// `UNAVAILABLE`, while the stored chain keeps serving. The tip calls answer
/// again as soon as a fresh tip is served.
#[tokio::test]
async fn tip_calls_refuse_a_stale_tip_and_recover_with_a_fresh_one() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    // The node returns and the served tip advances to height 2 once a block
    // range has been read.
    let advanced_store =
        acceptance_store_from_chain(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(2))?;
    let recorder =
        EpochPinRecorder::new(store_fixture.query()).with_later_views(vec![advanced_store.query()]);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)));

    recorder.set_block_time(unix_now()? - 30 * 60);
    let latest = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await
        .err()
        .map(|status| status.code());
    let info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .map(|status| status.code());
    assert_eq!(latest, Some(Code::Unavailable));
    assert_eq!(info, Some(Code::Unavailable));

    // Stored data keeps serving past the limit.
    let block = adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: u64::from(ACCEPTANCE_BLOCK_HEIGHT.value()),
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    assert_eq!(block.height, u64::from(ACCEPTANCE_BLOCK_HEIGHT.value()));
    let range = collect_block_range(&adapter, block_range_request(1, 1)).await?;
    assert_eq!(range.len(), 1);
    adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: u64::from(ACCEPTANCE_BLOCK_HEIGHT.value()),
            hash: Vec::new(),
        }))
        .await?;

    // A fresh tip clears the refusal.
    recorder.set_block_time(unix_now()?);
    let latest = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();
    assert_eq!(latest.height, 2);
    adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?;
    Ok(())
}

/// Runs the two tip calls and returns how each ended.
async fn tip_call_codes<QueryApi>(
    adapter: &LightwalletdGrpcAdapter<QueryApi>,
) -> (Result<(), Code>, Result<(), Code>)
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let latest = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await
        .map(|_| ())
        .map_err(|status| status.code());
    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await
        .map(|_| ())
        .map_err(|status| status.code());
    (latest, lightd_info)
}

const REFUSED: (Result<(), Code>, Result<(), Code>) =
    (Err(Code::Unavailable), Err(Code::Unavailable));
const ANSWERED: (Result<(), Code>, Result<(), Code>) = (Ok(()), Ok(()));

/// A quiet chain is not a stale one: while the writer's node is reachable
/// and has no block above the served tip, the tip calls answer however old
/// that tip is.
#[tokio::test]
async fn tip_calls_answer_an_old_tip_the_node_still_reports() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let node_tip_witness = NodeTipWitness::default();
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)))
        .with_node_tip_witness(node_tip_witness.clone());

    recorder.set_block_time(unix_now()? - 60 * 60);
    node_tip_witness.observe(Some(ACCEPTANCE_BLOCK_HEIGHT));
    assert_eq!(tip_call_codes(&adapter).await, ANSWERED);
    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    assert!(
        lightd_info.estimated_height > lightd_info.block_height,
        "estimatedHeight still tells the wallet the chain is quiet"
    );
    Ok(())
}

/// While the writer's node is unreachable, an old tip is refused, and the
/// tip calls answer again once the node reports the served tip.
#[tokio::test]
async fn tip_calls_refuse_an_old_tip_while_the_node_is_unreachable() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let node_tip_witness = NodeTipWitness::default();
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)))
        .with_node_tip_witness(node_tip_witness.clone());

    // Within the limit the tip calls answer whatever the node does.
    recorder.set_block_time(unix_now()? - 10 * 60);
    node_tip_witness.observe(None);
    assert_eq!(tip_call_codes(&adapter).await, ANSWERED);

    let old_adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)))
        .with_node_tip_witness(node_tip_witness.clone());
    recorder.set_block_time(unix_now()? - 30 * 60);
    assert_eq!(tip_call_codes(&old_adapter).await, REFUSED);

    // The node answers again at the served tip: the chain was only quiet.
    node_tip_witness.observe(Some(ACCEPTANCE_BLOCK_HEIGHT));
    assert_eq!(tip_call_codes(&old_adapter).await, ANSWERED);
    Ok(())
}

/// While the writer is behind its node, an old tip is refused, and the tip
/// calls answer again once the writer has caught up to the node.
#[tokio::test]
async fn tip_calls_refuse_an_old_tip_while_the_writer_is_behind_its_node() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let caught_up_store =
        acceptance_store_from_chain(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(2))?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_later_views(vec![caught_up_store.query()]);
    let node_tip_witness = NodeTipWitness::default();
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)))
        .with_node_tip_witness(node_tip_witness.clone());

    recorder.set_block_time(unix_now()? - 30 * 60);
    node_tip_witness.observe(Some(BlockHeight::new(2)));
    assert_eq!(tip_call_codes(&adapter).await, REFUSED);

    // A block-range read moves the served view to height 2, the node's tip.
    collect_block_range(&adapter, block_range_request(1, 1)).await?;
    assert_eq!(tip_call_codes(&adapter).await, ANSWERED);
    let latest = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();
    assert_eq!(latest.height, 2);
    Ok(())
}

/// A node that answers but reports itself behind its peers cannot vouch for
/// its tip, so an old tip is refused until the node has caught up.
///
/// A not-ready report derived only from the tip's age, which a quiet chain
/// also raises, still vouches.
#[tokio::test]
async fn tip_calls_refuse_an_old_tip_while_the_node_reports_it_is_behind() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let node_tip_witness = NodeTipWitness::default();
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::after(std::time::Duration::from_mins(25)))
        .with_node_tip_witness(node_tip_witness.clone());
    recorder.set_block_time(unix_now()? - 30 * 60);
    let writer_status = |node_health_reason: Option<&str>| {
        let tip = ACCEPTANCE_BLOCK_HEIGHT.value();
        zinder_proto::v1::ingest::WriterStatusResponse {
            network_name: zinder_core::wire::encode_zinder_native_chain_name(Network::ZcashRegtest)
                .to_owned(),
            chain_view: Some(zinder_proto::v1::wallet::ChainView {
                upstream_tip: Some(zinder_proto::v1::wallet::UpstreamTip {
                    committed_height: Some(tip),
                    estimated_height: None,
                }),
                ..zinder_proto::v1::wallet::ChainView::default()
            }),
            upstream_not_ready: node_health_reason.map(|reason| {
                // Zebra's `/ready` reports carry no heights; the writer
                // reports the node tip its follower observed beside them.
                zinder_proto::v1::ops::UpstreamNotReadyDetail {
                    upstream_health_source: "zebra_ready_endpoint".to_owned(),
                    upstream_health_reason: reason.to_owned(),
                    ..zinder_proto::v1::ops::UpstreamNotReadyDetail::default()
                }
            }),
            ..zinder_proto::v1::ingest::WriterStatusResponse::default()
        }
    };

    let observe = |node_health_reason| {
        node_tip_witness.observe(zinder_compat_lightwalletd::node_tip_from_writer_status(
            &writer_status(node_health_reason),
            Network::ZcashRegtest,
        ));
    };

    observe(Some("syncing"));
    assert_eq!(tip_call_codes(&adapter).await, REFUSED);
    // The node catches up and reports the served tip as the chain tip.
    observe(None);
    assert_eq!(tip_call_codes(&adapter).await, ANSWERED);
    // A quiet chain makes Zebra report the tip's age; that still vouches.
    observe(Some("tip_age=1800s"));
    assert_eq!(tip_call_codes(&adapter).await, ANSWERED);
    Ok(())
}

/// With the stale-tip limit disabled, the tip calls keep answering however
/// old the served tip is.
#[tokio::test]
async fn a_disabled_stale_tip_limit_serves_an_old_tip() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_stale_tip_limit(StaleTipLimit::DISABLED);

    recorder.set_block_time(unix_now()? - 7 * 24 * 60 * 60);
    adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?;
    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    assert!(lightd_info.estimated_height > lightd_info.block_height);
    Ok(())
}

/// Pins every `LightdInfo` field and the `GetLatestBlock` hash order that the
/// identity contract in `docs/reference/lightwalletd-compatibility.md`
/// classifies as comparable, live-tip, or Zinder-specific.
#[tokio::test]
async fn lightd_info_and_latest_block_follow_the_identity_contract() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip = chain
        .block_at(ACCEPTANCE_BLOCK_HEIGHT)
        .ok_or_else(|| eyre!("identity fixture must include the height 1 block"))?;
    let store_fixture = acceptance_store_from_chain(&chain)?;
    let adapter = acceptance_adapter(&store_fixture);

    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let latest_block = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();

    // Regtest collapses to "test" per BIP70. Canopy is the branch active at
    // height 1 in the sample table, and NU5 at height 2 is the next pending
    // upgrade.
    let expected = lightwalletd::LightdInfo {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        vendor: "Zinder".to_owned(),
        taddr_support: false,
        chain_name: "test".to_owned(),
        sapling_activation_height: 1,
        consensus_branch_id: "e9ff75a6".to_owned(),
        block_height: 1,
        git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
        branch: String::new(),
        build_date: String::new(),
        build_user: String::new(),
        // The network estimate from the tip's block time, which
        // `lightd_info_estimates_the_network_height_from_the_served_tip`
        // checks; the fixture tip is old, so it runs ahead of the tip.
        estimated_height: lightd_info.estimated_height,
        zcashd_build: String::new(),
        zcashd_subversion: String::new(),
        donation_address: String::new(),
        upgrade_name: "NU5".to_owned(),
        upgrade_height: 2,
        lightwallet_protocol_version: lightwalletd::LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
    };
    assert_eq!(lightd_info, expected);
    assert!(lightd_info.estimated_height >= lightd_info.block_height);

    assert_eq!(latest_block.height, lightd_info.block_height);
    assert_eq!(
        latest_block.hash,
        encode_internal_block_hash(tip.hash).to_vec()
    );
    assert_eq!(latest_block.hash.len(), 32);

    Ok(())
}

/// zingolib accepts the reported `lightwalletProtocolVersion`.
///
/// zingolib refuses to sync unless the field parses as
/// `vMAJOR.MINOR.PATCH` and is at least 0.5.0 (zingolabs/zingolib `f930d80`,
/// `pepper-sync/src/client.rs:104-143`, checked at `sync.rs:464`). The pinned
/// reference lightwalletd reports the vendored `lightwallet-protocol` tag here,
/// so Zinder reports `LIGHTWALLETD_PROTOCOL_VERSION` (`v0.6.0`).
#[tokio::test]
async fn lightd_info_protocol_version_satisfies_zingolib_minimum() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(
        lightd_info.lightwallet_protocol_version,
        lightwalletd::LIGHTWALLETD_PROTOCOL_VERSION
    );
    let version =
        zingolib_protocol_version(&lightd_info.lightwallet_protocol_version).ok_or_else(|| {
            eyre!(
                "zingolib cannot parse lightwalletProtocolVersion {:?}",
                lightd_info.lightwallet_protocol_version
            )
        })?;
    assert!(
        version >= (0, 5, 0),
        "zingolib requires protocol 0.5.0 or later, got {version:?}"
    );
    Ok(())
}

#[tokio::test]
async fn get_standard_fee_returns_the_node_fee_unchanged() -> eyre::Result<()> {
    let node = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x11; 32]))
        .with_standard_fee(
            StandardFee::new(7_000, 3).ok_or_else(|| eyre!("the node fee must be non-zero"))?,
        );
    let (adapter, _store) = broadcasting_acceptance_adapter(node).await?;

    let fee = adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(fee.fee_zat, 7_000);
    assert_eq!(fee.estimator_version, 3);
    Ok(())
}

#[tokio::test]
async fn get_standard_fee_is_unimplemented_when_the_node_lacks_the_method() -> eyre::Result<()> {
    let node = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x11; 32]));
    let (adapter, _store) = broadcasting_acceptance_adapter(node).await?;

    let status = adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .ok_or_else(|| eyre!("expected GetStandardFee to return UNIMPLEMENTED"))?;

    assert_eq!(status.code(), Code::Unimplemented);
    Ok(())
}

#[tokio::test]
async fn get_standard_fee_is_unimplemented_without_a_configured_node() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .ok_or_else(|| eyre!("expected GetStandardFee to return UNIMPLEMENTED"))?;

    assert_eq!(status.code(), Code::Unimplemented);
    Ok(())
}

#[tokio::test]
async fn get_standard_fee_is_unavailable_when_the_node_is_unreachable() -> eyre::Result<()> {
    let node = MockTransactionBroadcaster::node_unavailable("node refuses connections");
    let (adapter, _store) = broadcasting_acceptance_adapter(node).await?;

    let status = adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .ok_or_else(|| eyre!("expected GetStandardFee to return UNAVAILABLE"))?;

    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn get_standard_fee_is_unavailable_while_a_fee_capable_fallback_is_unreachable()
-> eyre::Result<()> {
    let fallback = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x12; 32]))
        .with_standard_fee(
            StandardFee::new(7_000, 3).ok_or_else(|| eyre!("the node fee must be non-zero"))?,
        );
    fallback.report_unreachable();
    let (adapter, _store) = ordered_broadcasting_acceptance_adapter(vec![
        MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x11; 32])),
        fallback,
    ])
    .await?;

    let status = adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .ok_or_else(|| eyre!("expected GetStandardFee to return UNAVAILABLE"))?;

    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

/// Port of zingolib's `parse_protocol_version`: an optional `v` prefix, then
/// `major.minor.patch`, ignoring any non-digit suffix on the patch number.
fn zingolib_protocol_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.trim().trim_start_matches('v').splitn(3, '.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts
        .next()?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((major, minor, patch))
}

#[tokio::test]
async fn get_block_returns_lightwalletd_error_codes_for_invalid_selectors() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected unknown-height error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::OutOfRange);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected height-zero error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: vec![0xff; 32],
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected hash-mismatch error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::NotFound);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: vec![0xff; 31],
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected malformed-hash error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

/// A best-chain hash whose compact block the store lacks reports the missing
/// block, not a hash outside the best chain, and both serving modes report
/// both outcomes alike.
#[tokio::test]
async fn get_block_by_hash_reports_a_missing_compact_block_alike_in_both_modes() -> eyre::Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let damaged = chain
        .block_at(BlockHeight::new(2))
        .cloned()
        .ok_or_else(|| eyre!("the fixture must contain height 2"))?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;
    let canonical_primary_path = writer.canonical_primary_path().to_owned();
    let mut store_fixture = writer.into_serving_store_fixture()?;
    damage_compact_block_row(&canonical_primary_path, damaged.height, None)?;
    let (mut canonical, wallet) = store_fixture.take_readers()?;
    canonical.try_catch_up()?;
    let canonical = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    let wallet_serving = LightwalletdGrpcAdapter::new(WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::<RocksDbCanonicalReader>::clone(&canonical),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?)),
    ));
    let compact_blocks_serving = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(canonical));

    let cases = [
        (
            "missing compact block",
            damaged.hash,
            "ARTIFACT_UNAVAILABLE",
        ),
        (
            "hash outside the best chain",
            BlockHash::from_bytes([0xee; 32]),
            "BLOCK_NOT_IN_BEST_CHAIN",
        ),
    ];
    for (case, block_hash, expected_reason) in cases {
        let request = || {
            Request::new(lightwalletd::BlockId {
                height: 0,
                hash: encode_internal_block_hash(block_hash).to_vec(),
            })
        };
        let outcomes = [
            ("wallet", wallet_serving.get_block(request()).await),
            (
                "compact-blocks",
                compact_blocks_serving.get_block(request()).await,
            ),
        ];
        for (mode, outcome) in outcomes {
            let status = match outcome {
                Ok(response) => {
                    return Err(eyre!(
                        "{mode} serving answered GetBlock for a {case}: {response:?}"
                    ));
                }
                Err(status) => status,
            };
            let reason = status
                .get_details_error_info()
                .map(|error_info| error_info.reason);
            assert_eq!(status.code(), Code::NotFound, "{mode} serving, {case}");
            assert_eq!(
                reason.as_deref(),
                Some(expected_reason),
                "{mode} serving, {case}"
            );
        }
    }
    Ok(())
}

/// Removes or overwrites a compact-block row in a closed fixture primary.
fn damage_compact_block_row(
    canonical_primary_path: &std::path::Path,
    height: BlockHeight,
    replacement: Option<&[u8]>,
) -> eyre::Result<()> {
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, canonical_primary_path)?;
    let db = rust_rocksdb::DB::open_cf(&options, canonical_primary_path, &families)?;
    let family = db
        .cf_handle("compact_block")
        .ok_or_else(|| eyre!("the canonical primary has no compact-block family"))?;
    let key = encode_height_key_ascending(height);
    match replacement {
        Some(bytes) => db.put_cf(&family, key, bytes)?,
        None => db.delete_cf(&family, key)?,
    }
    db.flush_cf(&family)?;
    Ok(())
}

#[tokio::test]
async fn get_block_nullifiers_reports_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_block_nullifiers(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected future-height error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::OutOfRange);

    Ok(())
}

#[tokio::test]
async fn get_block_range_methods_report_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let request = || lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };

    let Err(status) = adapter.get_block_range(Request::new(request())).await else {
        return Err(eyre!("expected future-height error, got a block stream"));
    };
    assert_eq!(status.code(), Code::OutOfRange);

    let Err(status) = adapter
        .get_block_range_nullifiers(Request::new(request()))
        .await
    else {
        return Err(eyre!("expected future-height error, got a block stream"));
    };
    assert_eq!(status.code(), Code::OutOfRange);

    Ok(())
}

/// lightwalletd puts no width bound on `BlockRange`, and the Zcash SDK clients
/// pointed at this surface rely on that. A window wider than one store read
/// must stream in full instead of failing the call.
#[tokio::test]
async fn get_block_range_streams_windows_wider_than_one_store_read() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let visible_tip_height = BlockHeight::new(2500);
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(visible_tip_height);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let request = |start: u64, end: u64| lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: start,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: end,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };

    let ascending = collect_stream(
        adapter
            .get_block_range(Request::new(request(1, 2500)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(
        ascending
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        (1..=2500).collect::<Vec<_>>()
    );
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        vec![
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1000)),
            BlockHeightRange::inclusive(BlockHeight::new(1001), BlockHeight::new(2000)),
            BlockHeightRange::inclusive(BlockHeight::new(2001), BlockHeight::new(2500)),
        ],
        "each store read must stay within the compact-block range cap"
    );

    let descending = collect_stream(
        adapter
            .get_block_range(Request::new(request(2500, 1)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(
        descending
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        (1..=2500).rev().collect::<Vec<_>>(),
        "descending windows must stay monotonic across chunk edges"
    );

    Ok(())
}

/// Returns how many slot-wait samples `rendered` holds for `outcome`.
fn slot_wait_samples(rendered: &str, outcome: &str) -> Option<u64> {
    let series = format!(
        "zinder_compat_block_range_slot_wait_duration_seconds_count{{outcome=\"{outcome}\"}} "
    );
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str())?.parse::<u64>().ok())
}

/// Returns how many samples `rendered` holds for the block-range `stage`.
fn block_range_stage_samples(rendered: &str, stage: &str) -> Option<u64> {
    let series =
        format!("zinder_compat_block_range_stage_duration_seconds_count{{stage=\"{stage}\"}} ");
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str())?.parse::<u64>().ok())
}

/// Each chunk of a block-range stream records how long it took to read and
/// to convert.
///
/// Each chunk after the first also records how long its reader waited to
/// queue it for the client; the first is sent from the response.
#[tokio::test]
async fn get_block_range_times_each_chunk_stage() -> eyre::Result<()> {
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_handle = metrics_recorder.handle();
    // The test runtime runs every task on this thread, so the local recorder
    // sees the reader task of the later chunks too.
    let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(2_500));
    let adapter = LightwalletdGrpcAdapter::new(recorder);

    let blocks = collect_block_range(&adapter, block_range_request(1, 2_500)).await?;
    assert_one_linked_chain(&blocks, 1, 2_500);

    let rendered = metrics_handle.render();
    assert_eq!(block_range_stage_samples(&rendered, "read"), Some(3));
    assert_eq!(block_range_stage_samples(&rendered, "convert"), Some(3));
    assert_eq!(block_range_stage_samples(&rendered, "send"), Some(2));
    Ok(())
}

/// A block-range chunk becomes lightwalletd messages on the blocking pool,
/// not on the task that read it.
///
/// The runtime has one blocking thread and the test holds it, so a chunk the
/// query has already read waits to be converted until the test releases the
/// thread.
#[test]
fn get_block_range_converts_each_chunk_on_the_blocking_pool() -> eyre::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?
        .block_on(convert_a_chunk_while_the_blocking_pool_is_held())
}

async fn convert_a_chunk_while_the_blocking_pool_is_held() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(10));
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let (gate_held, gate_held_receiver) = tokio::sync::oneshot::channel();
    let (release_gate, gate) = tokio::sync::oneshot::channel::<()>();
    let gate_thread = tokio::task::spawn_blocking(move || {
        let _ = gate_held.send(());
        let _ = gate.blocking_recv();
    });
    gate_held_receiver.await?;

    let mut call = Box::pin(adapter.get_block_range(Request::new(block_range_request(1, 10))));
    let chunk_read = async {
        while recorder.recorded_compact_block_ranges().is_empty() {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        outcome = &mut call => {
            return Err(eyre!(
                "the call answered before its chunk was read: {:?}",
                outcome.map(|_| ())
            ));
        }
        () = chunk_read => {}
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut call)
            .await
            .is_err(),
        "a read chunk must wait for the blocking pool to be converted"
    );

    let _ = release_gate.send(());
    gate_thread.await?;
    let blocks = collect_stream(call.await?.into_inner()).await?;
    assert_one_linked_chain(&blocks, 1, 10);
    Ok(())
}

/// Returns the seconds `rendered` records for the block-range `stage`.
fn block_range_stage_seconds(rendered: &str, stage: &str) -> Option<f64> {
    let series =
        format!("zinder_compat_block_range_stage_duration_seconds_sum{{stage=\"{stage}\"}} ");
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str())?.parse::<f64>().ok())
}

/// A chunk's read stage ends when the wallet query returns, so it leaves out
/// the time the chunk then waits for a blocking thread to be converted.
#[test]
fn get_block_range_read_stage_leaves_out_the_wait_to_convert() -> eyre::Result<()> {
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_handle = metrics_recorder.handle();
    let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
    let hold = std::time::Duration::from_secs(1);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?
        .block_on(convert_a_chunk_after_the_blocking_pool_is_held_for(hold))?;

    let rendered = metrics_handle.render();
    assert_eq!(block_range_stage_samples(&rendered, "read"), Some(1));
    let read_seconds =
        block_range_stage_seconds(&rendered, "read").ok_or_else(|| eyre!("no read stage"))?;
    assert!(
        read_seconds < hold.as_secs_f64(),
        "the read stage counted the {hold:?} its chunk waited to be converted: {read_seconds}s"
    );
    Ok(())
}

/// Streams heights 1 through 10, one chunk, while the test holds the only
/// blocking thread from before the chunk is read until `hold` after.
async fn convert_a_chunk_after_the_blocking_pool_is_held_for(
    hold: std::time::Duration,
) -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(10));
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let (gate_held, gate_held_receiver) = tokio::sync::oneshot::channel();
    let (release_gate, gate) = tokio::sync::oneshot::channel::<()>();
    let gate_thread = tokio::task::spawn_blocking(move || {
        let _ = gate_held.send(());
        let _ = gate.blocking_recv();
    });
    gate_held_receiver.await?;

    let mut call = Box::pin(adapter.get_block_range(Request::new(block_range_request(1, 10))));
    let chunk_read = async {
        while recorder.recorded_compact_block_ranges().is_empty() {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        outcome = &mut call => {
            return Err(eyre!(
                "the call answered before its chunk was read: {:?}",
                outcome.map(|_| ())
            ));
        }
        () = chunk_read => {}
    }
    assert!(
        tokio::time::timeout(hold, &mut call).await.is_err(),
        "a read chunk must wait for the blocking pool to be converted"
    );

    let _ = release_gate.send(());
    gate_thread.await?;
    let blocks = collect_stream(call.await?.into_inner()).await?;
    assert_one_linked_chain(&blocks, 1, 10);
    Ok(())
}

fn block_range_request(start: u64, end: u64) -> lightwalletd::BlockRange {
    lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: start,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: end,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    }
}

/// Blocks a block-range stream sent, and the status that ended it early.
struct DrainedBlockRange {
    blocks: Vec<lightwalletd::CompactBlock>,
    ending: Option<Status>,
}

/// Drains a block-range stream one block at a time and yields between
/// blocks, like a client that reads slower than the server.
async fn drain_slowly<Stream>(mut stream: Stream) -> DrainedBlockRange
where
    Stream: tokio_stream::Stream<Item = Result<lightwalletd::CompactBlock, Status>> + Unpin,
{
    use tokio_stream::StreamExt as _;

    let mut blocks = Vec::new();
    while let Some(message) = stream.next().await {
        match message {
            Ok(block) => blocks.push(block),
            Err(status) => {
                return DrainedBlockRange {
                    blocks,
                    ending: Some(status),
                };
            }
        }
        tokio::task::yield_now().await;
    }
    DrainedBlockRange {
        blocks,
        ending: None,
    }
}

/// Asserts that `blocks` are the consecutive heights from `first_height` to
/// `last_height` in order, and that each block links by hash to the one sent
/// before it.
fn assert_one_linked_chain(
    blocks: &[lightwalletd::CompactBlock],
    first_height: u64,
    last_height: u64,
) {
    let is_descending = first_height > last_height;
    let expected_count = first_height.abs_diff(last_height).saturating_add(1);
    assert_eq!(
        u64::try_from(blocks.len()).unwrap_or(u64::MAX),
        expected_count,
        "the stream must send heights {first_height} through {last_height}"
    );
    for (offset, block) in (0_u64..).zip(blocks) {
        let expected_height = if is_descending {
            first_height.saturating_sub(offset)
        } else {
            first_height.saturating_add(offset)
        };
        assert_eq!(block.height, expected_height);
    }
    for pair in blocks.windows(2) {
        let [earlier, later] = pair else {
            continue;
        };
        let (parent, child) = if is_descending {
            (later, earlier)
        } else {
            (earlier, later)
        };
        assert_eq!(
            child.prev_hash, parent.hash,
            "block {} must link to block {}",
            later.height, earlier.height
        );
    }
}

/// Asserts that a stream ended with `code`, after the blocks it sent.
fn assert_stream_ended_with(ending: Option<&Status>, code: Code) {
    assert_eq!(
        ending.map(Status::code),
        Some(code),
        "the stream must end with {code:?}: {ending:?}"
    );
}

/// Returns the `cause` and `sent_chain_epoch` of every
/// `compat_block_range_ended_by_reorg` event.
fn reorg_endings(log_capture: &LogCapture) -> Vec<(String, String)> {
    log_capture
        .events()
        .iter()
        .filter(|event| event.field("event") == Some("compat_block_range_ended_by_reorg"))
        .map(|event| {
            (
                event.field("cause").unwrap_or_default().to_owned(),
                event
                    .field("sent_chain_epoch")
                    .unwrap_or_default()
                    .to_owned(),
            )
        })
        .collect()
}

/// Returns the `current_chain_epoch` of every
/// `compat_block_range_continued_at_newer_epoch` event.
fn continued_at_newer_epochs(log_capture: &LogCapture) -> Vec<String> {
    log_capture
        .events()
        .iter()
        .filter(|event| event.field("event") == Some("compat_block_range_continued_at_newer_epoch"))
        .map(|event| {
            event
                .field("current_chain_epoch")
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

/// Wire hash of the synthetic block at `height` on `branch`.
fn synthetic_wire_hash(height: u64, branch: u8) -> Vec<u8> {
    let height = u32::try_from(height).unwrap_or(u32::MAX);
    encode_internal_block_hash(synthetic_block_hash(height, branch)).to_vec()
}

/// Streams `start..=end` from a recorder that serves `views` in turn and
/// drains it slowly.
async fn stream_across_views(
    store_fixture: &AcceptanceStore,
    request: lightwalletd::BlockRange,
    views: Vec<SyntheticView>,
) -> eyre::Result<DrainedBlockRange> {
    let recorder = EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(views);
    let adapter = LightwalletdGrpcAdapter::new(recorder);
    let stream = adapter.get_block_range(Request::new(request)).await?;
    Ok(drain_slowly(stream.into_inner()).await)
}

/// A wallet that reads slower than blocks arrive keeps its stream. Each chunk
/// after the first reads the view current when it is read, so no chunk waits
/// on a pin that retention has already released.
#[tokio::test]
async fn get_block_range_completes_across_publications_while_the_client_reads_slowly()
-> eyre::Result<()> {
    let log_capture = LogCapture::install_for_target("zinder::compat_lightwalletd");
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(vec![
        SyntheticView::entry(ENTRY_EPOCH, 3_500),
        SyntheticView::entry(ENTRY_EPOCH + 1, 3_501),
        SyntheticView::entry(ENTRY_EPOCH + 2, 3_502),
        SyntheticView::entry(ENTRY_EPOCH + 3, 3_503),
    ]);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let drained = drain_slowly(
        adapter
            .get_block_range(Request::new(block_range_request(1, 3_500)))
            .await?
            .into_inner(),
    )
    .await;

    if let Some(status) = drained.ending {
        return Err(eyre!(
            "a slow stream across publications must complete, but it ended with {:?}: {}",
            status.code(),
            status.message()
        ));
    }
    assert_one_linked_chain(&drained.blocks, 1, 3_500);
    assert_eq!(
        recorder.recorded_epoch_ids(),
        vec![Some(ChainEpochId::new(ENTRY_EPOCH)), None, None, None],
        "only the first chunk is pinned to the entry epoch"
    );
    assert_eq!(
        recorder.served_compact_block_epochs(),
        (ENTRY_EPOCH..=ENTRY_EPOCH + 3)
            .map(ChainEpochId::new)
            .collect::<Vec<_>>(),
        "each later chunk reads the view current when it is read"
    );
    assert_eq!(
        continued_at_newer_epochs(&log_capture),
        (ENTRY_EPOCH + 1..=ENTRY_EPOCH + 3)
            .map(|epoch| epoch.to_string())
            .collect::<Vec<_>>(),
        "each chunk that links at a newer view is logged at debug"
    );
    Ok(())
}

/// A reorg that replaces a block the stream already sent ends the stream with
/// `ABORTED` after the blocks it sent, as lightwalletd does, and no block of
/// the replacing branch is sent.
#[tokio::test]
async fn get_block_range_ends_aborted_when_a_sent_block_is_replaced() -> eyre::Result<()> {
    let log_capture = LogCapture::install_for_target("zinder::compat_lightwalletd");
    let store_fixture = acceptance_store_fixture()?;

    let drained = stream_across_views(
        &store_fixture,
        block_range_request(1, 3_000),
        vec![
            SyntheticView::entry(ENTRY_EPOCH, 3_000),
            SyntheticView::forked(ENTRY_EPOCH + 1, 500, 1, 3_000),
        ],
    )
    .await?;

    assert_one_linked_chain(&drained.blocks, 1, 1_000);
    assert!(
        drained
            .blocks
            .iter()
            .all(|block| block.hash == synthetic_wire_hash(block.height, 0)),
        "no block of the replacing branch may be sent"
    );
    assert_stream_ended_with(drained.ending.as_ref(), Code::Aborted);
    assert_eq!(
        reorg_endings(&log_capture),
        vec![("chain_replaced".to_owned(), ENTRY_EPOCH.to_string())]
    );
    Ok(())
}

/// A reorg above the blocks already sent keeps the chain the stream sent, so
/// the stream continues on the replacing branch.
#[tokio::test]
async fn get_block_range_follows_a_reorg_above_the_blocks_it_sent() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;

    let drained = stream_across_views(
        &store_fixture,
        block_range_request(1, 3_000),
        vec![
            SyntheticView::entry(ENTRY_EPOCH, 3_000),
            SyntheticView::forked(ENTRY_EPOCH + 1, 1_500, 1, 3_000),
        ],
    )
    .await?;

    assert!(drained.ending.is_none(), "{:?}", drained.ending);
    assert_one_linked_chain(&drained.blocks, 1, 3_000);
    assert!(drained.blocks.iter().all(|block| {
        let branch = u8::from(block.height >= 1_500);
        block.hash == synthetic_wire_hash(block.height, branch)
    }));
    Ok(())
}

/// The link is checked at the last block an ascending stream sent: a fork at
/// that height ends the stream, and a fork one height above it does not.
#[tokio::test]
async fn ascending_get_block_range_continues_only_when_its_last_sent_block_survives()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let views_forked_at = |fork_height| {
        vec![
            SyntheticView::entry(ENTRY_EPOCH, 2_000),
            SyntheticView::forked(ENTRY_EPOCH + 1, fork_height, 1, 2_000),
        ]
    };

    let fork_at_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(1, 2_000),
        views_forked_at(1_000),
    )
    .await?;
    assert_one_linked_chain(&fork_at_last_sent.blocks, 1, 1_000);
    assert_stream_ended_with(fork_at_last_sent.ending.as_ref(), Code::Aborted);

    let fork_above_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(1, 2_000),
        views_forked_at(1_001),
    )
    .await?;
    assert!(
        fork_above_last_sent.ending.is_none(),
        "{:?}",
        fork_above_last_sent.ending
    );
    assert_one_linked_chain(&fork_above_last_sent.blocks, 1, 2_000);
    assert_eq!(
        fork_above_last_sent
            .blocks
            .get(1_000)
            .map(|block| block.hash.clone()),
        Some(synthetic_wire_hash(1_001, 1))
    );
    Ok(())
}

/// A descending stream needs only the parent of the last block it sent: a
/// fork at that block's height keeps the stream on the chain it was reading,
/// and a fork one height below it ends the stream.
#[tokio::test]
async fn descending_get_block_range_continues_only_into_the_parent_of_its_last_sent_block()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let views_forked_at = |fork_height| {
        vec![
            SyntheticView::entry(ENTRY_EPOCH, 3_000),
            SyntheticView::forked(ENTRY_EPOCH + 1, fork_height, 1, 3_000),
        ]
    };

    // The first chunk sends 3,000 down to 2,001.
    let fork_at_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(3_000, 1),
        views_forked_at(2_001),
    )
    .await?;
    assert!(
        fork_at_last_sent.ending.is_none(),
        "{:?}",
        fork_at_last_sent.ending
    );
    assert_one_linked_chain(&fork_at_last_sent.blocks, 3_000, 1);
    assert!(
        fork_at_last_sent
            .blocks
            .iter()
            .all(|block| block.hash == synthetic_wire_hash(block.height, 0)),
        "the stream returns exactly the entry view's chain"
    );

    let fork_below_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(3_000, 1),
        views_forked_at(2_000),
    )
    .await?;
    assert_one_linked_chain(&fork_below_last_sent.blocks, 3_000, 2_001);
    assert_stream_ended_with(fork_below_last_sent.ending.as_ref(), Code::Aborted);
    Ok(())
}

/// A reorg that leaves the visible tip below a height still to send ends the
/// stream with `OUT_OF_RANGE`.
///
/// The ending is not the `NOT_FOUND` of a read above the tip, and the log
/// names the epoch of the last chunk sent.
#[tokio::test]
async fn get_block_range_ends_out_of_range_when_the_tip_falls_below_the_range() -> eyre::Result<()>
{
    let log_capture = LogCapture::install_for_target("zinder::compat_lightwalletd");
    let store_fixture = acceptance_store_fixture()?;

    let drained = stream_across_views(
        &store_fixture,
        block_range_request(1, 3_000),
        vec![
            SyntheticView::entry(ENTRY_EPOCH, 3_000),
            SyntheticView::forked(ENTRY_EPOCH + 1, 2_900, 1, 2_950),
        ],
    )
    .await?;

    assert_one_linked_chain(&drained.blocks, 1, 2_000);
    assert_stream_ended_with(drained.ending.as_ref(), Code::OutOfRange);
    assert_eq!(
        reorg_endings(&log_capture),
        vec![("tip_lowered".to_owned(), (ENTRY_EPOCH + 1).to_string())]
    );
    Ok(())
}

/// `GetBlockRangeNullifiers` streams through the same chunk path: it
/// completes across publications and ends at a reorg into its sent blocks.
#[tokio::test]
async fn get_block_range_nullifiers_follows_the_same_chunk_contract() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let nullifiers_across_views = |views| {
        let adapter = LightwalletdGrpcAdapter::new(
            EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(views),
        );
        async move {
            let stream = adapter
                .get_block_range_nullifiers(Request::new(block_range_request(1, 2_500)))
                .await?;
            Ok::<_, eyre::Report>(drain_slowly(stream.into_inner()).await)
        }
    };

    let across_publications = nullifiers_across_views(vec![
        SyntheticView::entry(ENTRY_EPOCH, 2_500),
        SyntheticView::entry(ENTRY_EPOCH + 1, 2_501),
        SyntheticView::entry(ENTRY_EPOCH + 2, 2_502),
    ])
    .await?;
    assert!(
        across_publications.ending.is_none(),
        "{:?}",
        across_publications.ending
    );
    assert_one_linked_chain(&across_publications.blocks, 1, 2_500);

    let across_reorg = nullifiers_across_views(vec![
        SyntheticView::entry(ENTRY_EPOCH, 2_500),
        SyntheticView::forked(ENTRY_EPOCH + 1, 900, 1, 2_500),
    ])
    .await?;
    assert_one_linked_chain(&across_reorg.blocks, 1, 1_000);
    assert_stream_ended_with(across_reorg.ending.as_ref(), Code::Aborted);
    Ok(())
}

/// Store-backed artifacts carry real parent hashes. A stream whose sent
/// blocks a live replacement displaced ends with `ABORTED` instead of sending
/// the replacing branch.
#[tokio::test]
async fn get_block_range_ends_when_a_live_replacement_displaces_sent_blocks() -> eyre::Result<()> {
    const TIP_HEIGHT: u32 = 1_020;
    const FORK_HEIGHT: u32 = 995;
    let log_capture = LogCapture::install_for_target("zinder::compat_lightwalletd");
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(TIP_HEIGHT);
    for height in FORK_HEIGHT..=TIP_HEIGHT {
        let displaced = fixture_block(&chain, BlockHeight::new(height))?;
        let mut transaction_id = [0x5a; 32];
        transaction_id[..4].copy_from_slice(&height.to_le_bytes());
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes(transaction_id),
            displaced.height,
            displaced.hash,
            format!("raw-displaced-coinbase-{height}").into_bytes(),
        ));
    }
    let replacement_chain = chain
        .fork_at(BlockHeight::new(FORK_HEIGHT))?
        .extend_blocks(TIP_HEIGHT - FORK_HEIGHT + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(FORK_HEIGHT - 5)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before_reorg =
        AcceptanceStore::from_store_fixture(writer.serving_store_fixture()?, Vec::new())?;
    let sent_epoch = before_reorg
        .query()
        .visible_tip_block(None)
        .await?
        .chain_epoch
        .id;
    writer.replace_live_blocks(&replacement_chain, BlockHeight::new(FORK_HEIGHT))?;
    let after_reorg = acceptance_store_from_writer(writer)?;
    let recorder =
        EpochPinRecorder::new(before_reorg.query()).with_later_views(vec![after_reorg.query()]);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let drained = drain_slowly(
        adapter
            .get_block_range(Request::new(block_range_request(1, u64::from(TIP_HEIGHT))))
            .await?
            .into_inner(),
    )
    .await;

    assert_one_linked_chain(&drained.blocks, 1, 1_000);
    let last_sent = fixture_block(&chain, BlockHeight::new(1_000))?;
    assert_eq!(
        drained.blocks.last().map(|block| block.hash.clone()),
        Some(encode_internal_block_hash(last_sent.hash).to_vec())
    );
    assert_stream_ended_with(drained.ending.as_ref(), Code::Aborted);
    assert_eq!(
        reorg_endings(&log_capture),
        vec![("chain_replaced".to_owned(), sent_epoch.value().to_string())]
    );
    assert_eq!(
        recorder.served_compact_block_epochs().len(),
        2,
        "the second chunk is read at the replacing view"
    );
    Ok(())
}

/// Sapling outputs in each synthetic block of the large-block tests, about
/// 20 KB per stored block, so one chunk keeps about 100 of them.
const LARGE_BLOCK_SAPLING_OUTPUTS: u16 = 170;

/// Highest height of the large-block tests: about 12 MB of stored blocks,
/// spread over several byte-capped chunks.
const LARGE_BLOCK_TIP: u32 = 600;

/// Drains one `GetBlockRange` request from `adapter` and returns its blocks.
async fn collect_block_range<QueryApi>(
    adapter: &LightwalletdGrpcAdapter<QueryApi>,
    request: lightwalletd::BlockRange,
) -> eyre::Result<Vec<lightwalletd::CompactBlock>>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let stream = adapter.get_block_range(Request::new(request)).await?;
    Ok(collect_stream(stream.into_inner()).await?)
}

/// Stored bytes of the block at `height` in `view`, the size a chunk's byte
/// budget counts.
fn stored_block_bytes(view: &SyntheticView, height: u32) -> eyre::Result<usize> {
    Ok(encode_compact_block_artifact(&view.compact_block_at(BlockHeight::new(height))?).len())
}

/// Asserts that chunk reads were capped by bytes, were offered the heights
/// the chunk plan gives, and covered a range.
///
/// `reads` are the heights each chunk read kept and `offered` the heights it
/// was offered, which must cover `first_height..=last_height` in wire order.
/// Each read keeps as many blocks of `view` as fit its cap, which is
/// [`BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES`] for the first read and
/// [`BLOCK_RANGE_CHUNK_MAX_BYTES`] for the others, or every height it was
/// offered. Each read is offered up to 1,000 heights from the one it starts
/// at, however many the read before it was offered.
fn assert_byte_capped_chunks(
    view: &SyntheticView,
    reads: &[BlockHeightRange],
    offered: &[BlockHeightRange],
    first_height: u32,
    last_height: u32,
) -> eyre::Result<()> {
    assert_eq!(
        reads.len(),
        offered.len(),
        "each read must be recorded once"
    );
    let is_descending = first_height > last_height;
    let in_wire_order = |range: &BlockHeightRange| {
        if is_descending {
            (range.end.value(), range.start.value())
        } else {
            (range.start.value(), range.end.value())
        }
    };
    let new_heights_end = |from: u32| {
        if is_descending {
            from.saturating_sub(999).max(last_height)
        } else {
            from.saturating_add(999).min(last_height)
        }
    };
    let mut expected_next = Some(first_height);
    for (read_index, (read, offered)) in reads.iter().zip(offered).enumerate() {
        let (read_first, read_last) = in_wire_order(read);
        let (offered_first, offered_last) = in_wire_order(offered);
        assert_eq!(
            Some(read_first),
            expected_next,
            "chunk {read_index} must start where the one before it stopped"
        );
        assert_eq!(
            (offered_first, offered_last),
            (read_first, new_heights_end(read_first)),
            "chunk {read_index} must be offered up to 1,000 heights from the one it starts at"
        );
        let cap = if read_index == 0 {
            BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES
        } else {
            BLOCK_RANGE_CHUNK_MAX_BYTES
        };
        let read_bytes = read
            .into_iter()
            .map(|height| stored_block_bytes(view, height.value()))
            .sum::<eyre::Result<usize>>()?;
        assert!(
            read_bytes <= cap,
            "chunk {read_index} kept {read_bytes} bytes, above its {cap}-byte cap"
        );
        expected_next = if read_last == last_height {
            None
        } else if is_descending {
            Some(read_last - 1)
        } else {
            Some(read_last + 1)
        };
        if let Some(next_height) = expected_next.filter(|_| read_last != offered_last) {
            assert!(
                read_bytes + stored_block_bytes(view, next_height)? > cap,
                "chunk {read_index} stopped before its next block would pass its cap"
            );
        }
    }
    assert_eq!(expected_next, None, "the chunks must cover the range");
    Ok(())
}

/// Every height read alone, which no byte cap can split, in wire order.
async fn one_block_reads<QueryApi>(
    adapter: &LightwalletdGrpcAdapter<QueryApi>,
    heights: impl IntoIterator<Item = u32>,
) -> eyre::Result<Vec<lightwalletd::CompactBlock>>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let mut blocks = Vec::new();
    for height in heights {
        let height = u64::from(height);
        blocks.extend(collect_block_range(adapter, block_range_request(height, height)).await?);
    }
    Ok(blocks)
}

/// A range of large blocks is read and sent in chunks capped by bytes, the
/// first chunk included, and the client receives exactly the blocks that
/// one-block requests return.
#[tokio::test]
async fn get_block_range_sends_large_blocks_in_byte_capped_chunks() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let view = SyntheticView::entry(ENTRY_EPOCH, LARGE_BLOCK_TIP)
        .with_sapling_outputs(LARGE_BLOCK_SAPLING_OUTPUTS);
    let large_block_adapter = || {
        let recorder =
            EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(vec![view]);
        (LightwalletdGrpcAdapter::new(recorder.clone()), recorder)
    };
    let tip = u64::from(LARGE_BLOCK_TIP);

    let (adapter, recorder) = large_block_adapter();
    let ascending = collect_block_range(&adapter, block_range_request(1, tip)).await?;
    assert_one_linked_chain(&ascending, 1, tip);
    let ascending_reads = recorder.recorded_compact_block_ranges();
    assert!(
        ascending_reads.len() > 2,
        "the range must span several chunks: {ascending_reads:?}"
    );
    assert!(
        ascending_reads.first().map(|read| read.into_iter().len())
            < ascending_reads.get(1).map(|read| read.into_iter().len()),
        "the first chunk must keep fewer blocks than a later one: {ascending_reads:?}"
    );
    assert_byte_capped_chunks(
        &view,
        &ascending_reads,
        &recorder.offered_compact_block_ranges(),
        1,
        LARGE_BLOCK_TIP,
    )?;
    assert!(
        ascending == one_block_reads(&adapter, 1..=LARGE_BLOCK_TIP).await?,
        "byte-capped chunks must send the blocks that one-block reads return"
    );

    let (adapter, recorder) = large_block_adapter();
    let descending = collect_block_range(&adapter, block_range_request(tip, 1)).await?;
    assert_one_linked_chain(&descending, tip, 1);
    assert_byte_capped_chunks(
        &view,
        &recorder.recorded_compact_block_ranges(),
        &recorder.offered_compact_block_ranges(),
        LARGE_BLOCK_TIP,
        1,
    )?;
    assert!(
        descending == one_block_reads(&adapter, (1..=LARGE_BLOCK_TIP).rev()).await?,
        "a descending stream must send the one-block reads in reverse"
    );

    // Nullifier-only messages are small, but the read materializes whole
    // blocks, so the nullifier stream reads the same byte-capped chunks.
    let (adapter, nullifier_recorder) = large_block_adapter();
    let nullifiers = collect_stream(
        adapter
            .get_block_range_nullifiers(Request::new(block_range_request(1, tip)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_one_linked_chain(&nullifiers, 1, tip);
    assert_eq!(
        nullifier_recorder.recorded_compact_block_ranges(),
        ascending_reads
    );
    Ok(())
}

/// Every byte-capped chunk edge is checked like a 1,000-block chunk edge.
///
/// A reorg that replaces the last block a chunk sent ends the stream with
/// `ABORTED`, and a fork one height above it continues on the new branch.
#[tokio::test]
async fn byte_capped_chunks_keep_the_hash_link_at_every_chunk_edge() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let large = |view: SyntheticView| view.with_sapling_outputs(LARGE_BLOCK_SAPLING_OUTPUTS);
    let tip = u64::from(LARGE_BLOCK_TIP);

    // Find where the second chunk ends when no reorg happens.
    let plain = EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(vec![large(
        SyntheticView::entry(ENTRY_EPOCH, LARGE_BLOCK_TIP),
    )]);
    collect_block_range(
        &LightwalletdGrpcAdapter::new(plain.clone()),
        block_range_request(1, tip),
    )
    .await?;
    let second_chunk_end = plain
        .recorded_compact_block_ranges()
        .get(1)
        .map(|read| read.end.value())
        .ok_or_else(|| eyre!("the stream must read a second chunk"))?;
    assert!(
        second_chunk_end < LARGE_BLOCK_TIP / 2,
        "the first two chunks must each hold far fewer blocks than one store read"
    );

    // The views answer the first chunk, the second, and every later one.
    let views_forked_at = |fork_height| {
        vec![
            large(SyntheticView::entry(ENTRY_EPOCH, LARGE_BLOCK_TIP)),
            large(SyntheticView::entry(ENTRY_EPOCH + 1, LARGE_BLOCK_TIP)),
            large(SyntheticView::forked(
                ENTRY_EPOCH + 2,
                fork_height,
                1,
                LARGE_BLOCK_TIP,
            )),
        ]
    };
    let fork_at_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(1, tip),
        views_forked_at(second_chunk_end),
    )
    .await?;
    assert_one_linked_chain(&fork_at_last_sent.blocks, 1, u64::from(second_chunk_end));
    assert!(
        fork_at_last_sent
            .blocks
            .iter()
            .all(|block| block.hash == synthetic_wire_hash(block.height, 0)),
        "no block of the replacing branch may be sent"
    );
    assert_stream_ended_with(fork_at_last_sent.ending.as_ref(), Code::Aborted);

    let fork_above_last_sent = stream_across_views(
        &store_fixture,
        block_range_request(1, tip),
        views_forked_at(second_chunk_end + 1),
    )
    .await?;
    assert!(
        fork_above_last_sent.ending.is_none(),
        "{:?}",
        fork_above_last_sent.ending
    );
    assert_one_linked_chain(&fork_above_last_sent.blocks, 1, tip);
    assert!(fork_above_last_sent.blocks.iter().all(|block| {
        let branch = u8::from(block.height > u64::from(second_chunk_end));
        block.hash == synthetic_wire_hash(block.height, branch)
    }));
    Ok(())
}

/// Admission wait of the adapters that test refusals: long enough for a
/// request to queue, short enough to keep the tests fast.
const REFUSAL_WAIT: std::time::Duration = std::time::Duration::from_millis(100);

/// Opens `GetBlockRange` streams until one is admitted, for up to ten
/// seconds, and returns how the last attempt ended.
async fn wait_for_block_range_slot<QueryApi>(
    adapter: &LightwalletdGrpcAdapter<QueryApi>,
) -> Result<(), Status>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match adapter
            .get_block_range(Request::new(block_range_request(1, 10)))
            .await
        {
            Ok(_) => return Ok(()),
            Err(status)
                if status.code() == Code::Unavailable && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(status) => return Err(status),
        }
    }
}

/// The process-wide limit refuses a block-range stream that waited past its
/// admission wait with `UNAVAILABLE`.
///
/// A slot returns when its stream ends, when its request fails, and when the
/// client drops a stream partway through.
#[tokio::test]
async fn block_range_streams_past_the_limit_are_refused_until_a_slot_returns() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(2_500));
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let assert_refused = |outcome: Result<_, Status>| {
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable),
            "a stream past the limit must be refused"
        );
    };

    // A stream of one chunk holds its slot until the client has drained it.
    let narrow = adapter
        .get_block_range(Request::new(block_range_request(1, 10)))
        .await?
        .into_inner();
    assert_refused(
        adapter
            .get_block_range_nullifiers(Request::new(block_range_request(11, 20)))
            .await
            .map(|_| ()),
    );
    assert!(
        !recorder
            .recorded_compact_block_ranges()
            .contains(&BlockHeightRange::inclusive(
                BlockHeight::new(11),
                BlockHeight::new(20)
            )),
        "a refused stream reads nothing"
    );
    collect_stream(narrow).await?;

    // A request that fails returns its slot at once.
    let future_range = adapter
        .get_block_range(Request::new(block_range_request(1, 9_999)))
        .await;
    assert_eq!(
        future_range.err().map(|status| status.code()),
        Some(Code::OutOfRange)
    );

    // A stream wider than one chunk keeps its slot while a background reader
    // still serves it, and returns it once the client drops the stream.
    let mut wide = adapter
        .get_block_range(Request::new(block_range_request(1, 2_500)))
        .await?
        .into_inner();
    let first_block = tokio_stream::StreamExt::next(&mut wide).await;
    assert!(matches!(first_block, Some(Ok(_))));
    assert_refused(
        adapter
            .get_block_range(Request::new(block_range_request(1, 10)))
            .await
            .map(|_| ()),
    );
    drop(wide);
    wait_for_block_range_slot(&adapter).await?;
    Ok(())
}

#[tokio::test]
async fn block_range_checks_bounds_before_admission_and_pool_sentinel_afterward() -> eyre::Result<()>
{
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(2_500));
    let adapter = LightwalletdGrpcAdapter::new(recorder)
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let open_stream = adapter
        .get_block_range(Request::new(block_range_request(1, 10)))
        .await?;
    assert_eq!(
        adapter
            .get_block_range(Request::new(block_range_request(11, 20)))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable),
        "the open stream takes the only slot"
    );

    let invalid_pool = lightwalletd::BlockRange {
        pool_types: vec![lightwalletd::PoolType::Invalid as i32],
        ..block_range_request(1, 10)
    };
    assert_eq!(
        adapter
            .get_block_range(Request::new(invalid_pool.clone()))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable),
    );
    assert_eq!(
        adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: None,
                ..invalid_pool
            }))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::InvalidArgument),
    );
    assert_eq!(
        adapter
            .get_block_range_nullifiers(Request::new(lightwalletd::BlockRange {
                end: None,
                ..block_range_request(1, 10)
            }))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::InvalidArgument),
        "a malformed nullifier request at the limit must still be INVALID_ARGUMENT"
    );
    drop(open_stream);
    Ok(())
}

/// A dropped stream keeps its slot only while its read in flight runs.
///
/// The reader of the stream's later chunks finishes that read after the
/// client went away and starts no other, so the next stream is admitted once
/// that read has finished.
#[tokio::test]
async fn a_dropped_block_range_stream_keeps_its_slot_only_for_the_read_in_flight()
-> eyre::Result<()> {
    use tokio_stream::StreamExt as _;

    let store_fixture = acceptance_store_fixture()?;
    let later_chunk_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(3_500))
        .with_later_chunk_gate(Arc::clone(&later_chunk_gate));
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);

    let mut wide = adapter
        .get_block_range(Request::new(block_range_request(1, 3_500)))
        .await?
        .into_inner();
    // The client takes the first chunk while the read of the second is held.
    for _ in 0..1_000 {
        wide.next()
            .await
            .ok_or_else(|| eyre!("the stream ended inside its first chunk"))??;
    }
    // Give the reader time to reach the held read of its second chunk.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(wide);
    assert_eq!(
        adapter
            .get_block_range(Request::new(block_range_request(1, 10)))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable),
        "the reader of a dropped stream must keep its slot while its read runs"
    );

    // Each read returns the permit it took, so a read after the held one
    // would run too.
    later_chunk_gate.add_permits(1);
    wait_for_block_range_slot(&adapter).await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let range =
        |start, end| BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end));
    assert_eq!(
        recorder.offered_compact_block_ranges(),
        vec![range(1, 1_000), range(1_001, 2_000), range(1, 10)],
        "a reader whose client went away must start no read after the one in flight"
    );
    Ok(())
}

/// A later read that finds a height outside retained history after the
/// client went away is the stream's last read.
///
/// While the client holds the stream, the server then reads the heights an
/// earlier read checked and sends them before the stream ends with
/// `NOT_FOUND`. Once the client has dropped the stream, it reads none of
/// them, so the stream's slot returns as soon as the failed read finishes.
#[tokio::test]
async fn a_dropped_stream_reads_no_checked_heights_after_a_read_outside_retained_history()
-> eyre::Result<()> {
    // Heights 1,000 down to 0: the first read checks 1,000 down to 1 and
    // keeps a few large blocks, so the second is offered the heights it did
    // not keep together with unretained height 0.
    const TIP: u32 = 1_000;
    let store_fixture = acceptance_store_fixture()?;
    let view =
        SyntheticView::entry(ENTRY_EPOCH, TIP).with_sapling_outputs(LARGE_BLOCK_SAPLING_OUTPUTS);
    let request = || block_range_request(u64::from(TIP), 0);

    let held = stream_across_views(&store_fixture, request(), vec![view]).await?;
    assert_one_linked_chain(&held.blocks, u64::from(TIP), 1);
    assert_stream_ended_with(held.ending.as_ref(), Code::NotFound);

    let later_chunk_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_views(vec![view])
        .with_later_chunk_gate(Arc::clone(&later_chunk_gate));
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let dropped = adapter
        .get_block_range(Request::new(request()))
        .await?
        .into_inner();
    later_chunk_read_at_gate(&recorder).await?;
    drop(dropped);
    later_chunk_gate.add_permits(1);
    wait_for_block_range_slot(&adapter).await?;

    let first_chunk_start = recorder
        .recorded_compact_block_ranges()
        .first()
        .map(|read| read.start.value())
        .ok_or_else(|| eyre!("the stream must read its first chunk"))?;
    let range =
        |start, end| BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end));
    assert_eq!(
        recorder.offered_compact_block_ranges(),
        vec![
            range(1, TIP),
            range(0, first_chunk_start.saturating_sub(1)),
            range(1, 10)
        ],
        "a reader whose client went away must not read the checked heights after a failed read"
    );
    Ok(())
}

/// Waits until `recorder` has recorded at least `reads` compact-block range
/// reads, lets the stream's reader run on, and returns how many it recorded.
async fn settled_compact_block_reads<Inner: Sync>(
    recorder: &EpochPinRecorder<Inner>,
    reads: usize,
) -> eyre::Result<usize> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while recorder.recorded_compact_block_ranges().len() < reads {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(recorder.recorded_compact_block_ranges().len())
}

/// Waits until a later block-range chunk read of `recorder` waits on the gate
/// of [`EpochPinRecorder::with_later_chunk_gate`], for up to ten seconds.
///
/// Returns at once when a read has reached the gate since the last wait.
async fn later_chunk_read_at_gate<Inner: Sync>(
    recorder: &EpochPinRecorder<Inner>,
) -> eyre::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        recorder.later_chunk_gate_reached.notified(),
    )
    .await?;
    Ok(())
}

/// A stream holds at most two chunks however long its client stalls: the
/// one the client takes and the one the server reads next.
#[tokio::test]
async fn a_stalled_block_range_client_leaves_the_stream_two_chunks() -> eyre::Result<()> {
    use tokio_stream::StreamExt as _;

    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(vec![
        SyntheticView::entry(ENTRY_EPOCH, LARGE_BLOCK_TIP)
            .with_sapling_outputs(LARGE_BLOCK_SAPLING_OUTPUTS),
    ]);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());
    let tip = u64::from(LARGE_BLOCK_TIP);
    let mut stream = adapter
        .get_block_range(Request::new(block_range_request(1, tip)))
        .await?
        .into_inner();
    assert_eq!(
        settled_compact_block_reads(&recorder, 2).await?,
        2,
        "a stream whose client takes nothing reads one chunk past its first"
    );

    // Asking past the first chunk takes the second, which lets the server
    // read the third and no more.
    let first_chunk_blocks = recorder
        .recorded_compact_block_ranges()
        .first()
        .map(|read| read.into_iter().len())
        .ok_or_else(|| eyre!("the stream must read its first chunk"))?;
    let mut blocks = Vec::new();
    while blocks.len() <= first_chunk_blocks {
        blocks.push(
            stream
                .next()
                .await
                .ok_or_else(|| eyre!("the stream ended early"))??,
        );
    }
    assert_eq!(
        settled_compact_block_reads(&recorder, 3).await?,
        3,
        "a client that stalls inside the second chunk leaves the stream the second and third"
    );

    while let Some(block) = stream.next().await {
        blocks.push(block?);
    }
    assert_one_linked_chain(&blocks, 1, tip);
    Ok(())
}

/// A missing block fails the call only when the first chunk's read reaches it.
///
/// A block missing inside retained history that the read of the first chunk
/// reaches fails the call with `UNAVAILABLE`. That read reaches the block
/// just past the first chunk too, because it looks at that block to learn
/// that it does not fit. Past that read, the stream opens and ends with
/// `UNAVAILABLE` after the chunks before the one whose read reaches it.
#[tokio::test]
async fn a_block_missing_past_the_first_chunk_ends_the_stream_unavailable() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let large = SyntheticView::entry(ENTRY_EPOCH, LARGE_BLOCK_TIP)
        .with_sapling_outputs(LARGE_BLOCK_SAPLING_OUTPUTS);
    let tip = u64::from(LARGE_BLOCK_TIP);
    let plain = EpochPinRecorder::new(store_fixture.query()).with_synthetic_views(vec![large]);
    collect_block_range(
        &LightwalletdGrpcAdapter::new(plain.clone()),
        block_range_request(1, tip),
    )
    .await?;
    let first_chunk_end = plain
        .recorded_compact_block_ranges()
        .first()
        .map(|read| read.end.value())
        .ok_or_else(|| eyre!("the stream must read its first chunk"))?;

    for (missing_height, place) in [
        (first_chunk_end, "the last block of the first chunk"),
        (first_chunk_end + 1, "the block just past the first chunk"),
    ] {
        let reached_by_the_first_read = LightwalletdGrpcAdapter::new(
            EpochPinRecorder::new(store_fixture.query())
                .with_synthetic_views(vec![large.with_missing_block(missing_height)]),
        );
        assert_eq!(
            reached_by_the_first_read
                .get_block_range(Request::new(block_range_request(1, tip)))
                .await
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable),
            "a missing block at {place}, height {missing_height}, must fail the call"
        );
    }

    let past_first_chunk = stream_across_views(
        &store_fixture,
        block_range_request(1, tip),
        vec![large.with_missing_block(first_chunk_end + 20)],
    )
    .await?;
    assert_one_linked_chain(&past_first_chunk.blocks, 1, u64::from(first_chunk_end));
    assert_stream_ended_with(past_first_chunk.ending.as_ref(), Code::Unavailable);
    Ok(())
}

/// A request cancelled during its first chunk read keeps its slot until the
/// read has finished.
///
/// A store read finishes on the blocking pool after its caller is gone, so
/// the slot must cover it. The slot returns once the read has finished.
#[tokio::test]
async fn a_request_cancelled_during_its_first_read_keeps_its_slot_until_the_read_ends()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let first_chunk_gate = FirstChunkGate::closed();
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(2_500))
        .with_first_chunk_gate(first_chunk_gate.clone());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);

    let mut cancelled = adapter.get_block_range(Request::new(block_range_request(1, 2_500)));
    tokio::select! {
        outcome = &mut cancelled => {
            return Err(eyre!(
                "the request ended before its gated first read: {:?}",
                outcome.map(|_| ())
            ));
        }
        () = first_chunk_gate.reached.notified() => {}
    }
    drop(cancelled);

    assert_eq!(
        adapter
            .get_block_range(Request::new(block_range_request(1, 10)))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable),
        "a request cancelled during its first read must keep its slot while the read runs"
    );
    assert!(
        recorder.recorded_compact_block_ranges().is_empty(),
        "the cancelled request's first read is still waiting"
    );

    first_chunk_gate.release.add_permits(1);
    wait_for_block_range_slot(&adapter).await?;
    assert_eq!(
        recorder.recorded_compact_block_ranges().first(),
        Some(&BlockHeightRange::inclusive(
            BlockHeight::new(1),
            BlockHeight::new(1_000)
        )),
        "the slot returns only after the cancelled request's first read has finished"
    );
    Ok(())
}

/// A client that disconnects in the middle of a stream returns its slot, and
/// the refusal reaches a generated client as `UNAVAILABLE`.
#[tokio::test]
async fn a_disconnected_client_returns_its_block_range_stream_slot() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    // The gate holds the stream's later chunks, so the stream keeps its slot
    // until the test opens the gate.
    let later_chunk_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(5_000))
        .with_later_chunk_gate(Arc::clone(&later_chunk_gate));
    let adapter = LightwalletdGrpcAdapter::new(recorder)
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let server = adapter.clone().into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut syncing_wallet =
        CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
    let mut wide = syncing_wallet
        .get_block_range(block_range_request(1, 5_000))
        .await?
        .into_inner();
    assert!(wide.message().await?.is_some());

    let mut other_wallet =
        CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
    let refused = other_wallet
        .get_block_range(block_range_request(1, 10))
        .await;
    assert_eq!(
        refused.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );

    drop(wide);
    drop(syncing_wallet);
    later_chunk_gate.add_permits(1);
    wait_for_block_range_slot(&adapter).await?;
    let admitted = other_wallet
        .get_block_range(block_range_request(1, 10))
        .await?
        .into_inner();
    drop(admitted);

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

/// A request past the limit waits for a slot instead of failing, and is
/// admitted as soon as the stream holding the slot ends.
#[tokio::test]
async fn a_block_range_request_past_the_limit_waits_for_the_next_slot() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(2_500));
    let adapter = LightwalletdGrpcAdapter::new(recorder)
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN);
    let open_stream = adapter
        .get_block_range(Request::new(block_range_request(1, 10)))
        .await?
        .into_inner();

    let waiting = tokio::spawn({
        let adapter = adapter.clone();
        async move {
            let stream = adapter
                .get_block_range(Request::new(block_range_request(11, 20)))
                .await?
                .into_inner();
            collect_stream(stream).await
        }
    });
    tokio::time::sleep(REFUSAL_WAIT).await;
    assert!(
        !waiting.is_finished(),
        "a request past the limit must wait for a slot"
    );

    collect_stream(open_stream).await?;
    let blocks = waiting.await??;
    assert_eq!(
        blocks.iter().map(|block| block.height).collect::<Vec<_>>(),
        (11..=20).collect::<Vec<_>>()
    );
    Ok(())
}

/// A client that disconnects while its block-range request waits for a slot
/// gives back its place in the queue.
///
/// One slot admits one waiter. If the disconnected request still waited, the
/// next request would find the queue full and fail at once.
#[tokio::test]
async fn a_disconnected_waiting_client_gives_back_its_place_in_the_queue() -> eyre::Result<()> {
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_handle = metrics_recorder.handle();
    // The test runtime runs the server on this thread too, so the local
    // recorder sees the server's admission metrics.
    let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
    let store_fixture = acceptance_store_fixture()?;
    // The gate holds the stream's later chunks, so the stream keeps its slot
    // until the test opens the gate.
    let later_chunk_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(5_000))
        .with_later_chunk_gate(Arc::clone(&later_chunk_gate));
    let adapter = LightwalletdGrpcAdapter::new(recorder)
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let server = adapter.clone().into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let endpoint = format!("http://{server_addr}");

    let mut syncing_wallet = CompactTxStreamerClient::connect(endpoint.clone()).await?;
    let mut open_stream = syncing_wallet
        .get_block_range(block_range_request(1, 5_000))
        .await?
        .into_inner();
    assert!(open_stream.message().await?.is_some());
    let waiting_wallet = tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            let mut client = CompactTxStreamerClient::connect(endpoint).await?;
            client.get_block_range(block_range_request(1, 10)).await?;
            Ok::<_, eyre::Report>(())
        }
    });
    tokio::time::sleep(REFUSAL_WAIT).await;
    assert!(!waiting_wallet.is_finished(), "the second wallet must wait");
    waiting_wallet.abort();
    assert!(
        waiting_wallet
            .await
            .is_err_and(|error| error.is_cancelled())
    );
    // The server records the cancelled wait when it drops the waiting
    // request, in the same step that gives back its place.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while slot_wait_samples(&metrics_handle.render(), "cancelled") != Some(1) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;

    let mut next_wallet = CompactTxStreamerClient::connect(endpoint).await?;
    let next_request = tokio::spawn(async move {
        next_wallet
            .get_block_range(block_range_request(11, 20))
            .await
            .map(tonic::Response::into_inner)
    });
    tokio::time::sleep(REFUSAL_WAIT).await;
    assert!(
        !next_request.is_finished(),
        "the next request must wait in the place the disconnected one gave back"
    );
    drop(open_stream);
    drop(syncing_wallet);
    later_chunk_gate.add_permits(1);
    let admitted = next_request.await??;
    drop(admitted);

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

/// The default limit serves a wallet that syncs in 1,000-block batches with a
/// nullifier pass beside each, and admits that many streams at once.
#[tokio::test]
async fn the_default_block_range_stream_limit_serves_wallet_syncs() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(5_000));
    let adapter =
        LightwalletdGrpcAdapter::new(recorder).with_block_range_admission_wait(REFUSAL_WAIT);

    for start in (1_u64..=5_000).step_by(1_000) {
        let request = block_range_request(start, start + 999);
        let (blocks, nullifiers) =
            tokio::try_join!(collect_block_range(&adapter, request.clone()), async {
                let stream = adapter
                    .get_block_range_nullifiers(Request::new(request.clone()))
                    .await?;
                Ok::<_, eyre::Report>(collect_stream(stream.into_inner()).await?)
            },)?;
        assert_one_linked_chain(&blocks, start, start + 999);
        assert_one_linked_chain(&nullifiers, start, start + 999);
    }

    let mut open_streams = Vec::new();
    for _ in 0..DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS.get() {
        open_streams.push(
            adapter
                .get_block_range(Request::new(block_range_request(1, 10)))
                .await?,
        );
    }
    assert_eq!(
        adapter
            .get_block_range(Request::new(block_range_request(1, 10)))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable)
    );
    drop(open_streams);
    wait_for_block_range_slot(&adapter).await?;
    Ok(())
}

#[tokio::test]
async fn get_block_range_rejects_malformed_requests_before_streaming() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: None,
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await
    else {
        return Err(eyre!("expected missing-start error, got a block stream"));
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: None,
            pool_types: Vec::new(),
        }))
        .await
    else {
        return Err(eyre!("expected missing-end error, got a block stream"));
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: vec![99, 0],
        }))
        .await
    else {
        return Err(eyre!("expected invalid-pool error, got a block stream"));
    };
    assert_eq!(status.code(), Code::Unknown);
    assert_eq!(
        status.message(),
        "GetBlockRange: invalid pool type requested"
    );

    Ok(())
}

/// Unary `GetBlockNullifiers` retains every stored transaction as a shell.
///
/// Transparent-only coinbase transactions remain as `{index, txid, fee}`
/// shells, whereas `GetBlockRangeNullifiers` drops transactions outside the
/// requested shielded pools.
#[tokio::test]
async fn get_block_nullifiers_retains_transparent_only_shells_unlike_range_nullifiers()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let block = fixture_block(&chain, ACCEPTANCE_BLOCK_HEIGHT)?;
    let mut coinbase = FixtureTransactionRows::coinbase(
        TransactionId::from_bytes([0x10; 32]),
        block.height,
        block.hash,
        b"transparent-coinbase".to_vec(),
    );
    coinbase.facts.public_facts.counts.transparent_output_count = 1;
    let shielded = mixed_pool_transaction_rows(&block, 1);
    let rows = [coinbase, shielded];
    let artifact = acceptance_compact_block_artifact_at(&block, &rows)?;
    let mut transactions = artifact.into_parts().transactions;
    let [coinbase_tx, shielded_tx] = transactions.as_mut_slice() else {
        return Err(eyre!("expected coinbase and shielded transaction"));
    };
    coinbase_tx
        .data
        .transparent_outputs
        .push(zinder_core::CompactTransparentOutput {
            value_zat: 50_000,
            script_pub_key: vec![0x51],
        });
    shielded_tx.data.fee_zat = Some(10_000);
    let compact = CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        canonical_parent_hash(&block),
        block.block_time_seconds,
        transactions,
        block.compact_block_artifact().chain_metadata(),
    )?;
    let chain = rows
        .into_iter()
        .fold(chain, ChainFixture::with_transaction_rows)
        .with_compact_block_artifact(compact);
    let store_fixture = acceptance_store_from_chain(&chain)?;
    let adapter = acceptance_adapter(&store_fixture);

    let unary = adapter
        .get_block_nullifiers(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged = collect_stream(
        adapter
            .get_block_range_nullifiers(Request::new(block_range_request(1, 1)))
            .await?
            .into_inner(),
    )
    .await?;
    let [ranged_block] = ranged.as_slice() else {
        return Err(eyre!("expected one block from GetBlockRangeNullifiers"));
    };

    assert_eq!(unary.chain_metadata, Some(ChainMetadata::default()));
    assert_eq!(
        unary.vtx.iter().map(|tx| tx.index).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        unary.vtx[0],
        lightwalletd::CompactTx {
            index: 0,
            txid: vec![0x10; 32],
            fee: 0,
            ..lightwalletd::CompactTx::default()
        }
    );
    assert_eq!(
        ranged_block
            .vtx
            .iter()
            .map(|tx| tx.index)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(unary.vtx[1], ranged_block.vtx[0]);

    Ok(())
}

/// Both nullifiers-only RPCs must omit everything the deprecated lightwalletd
/// contract excludes.
///
/// The redacted fields are block-level commitment-tree sizes
/// (`chain_metadata`), transparent inputs and outputs, Sapling outputs, and the
/// non-nullifier Orchard and Ironwood action fields. Only the shielded
/// nullifiers survive.
#[tokio::test]
async fn block_nullifiers_omit_commitment_tree_sizes_and_redact_non_nullifier_fields()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let single = adapter
        .get_block_nullifiers(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged = adapter
        .get_block_range_nullifiers(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged = collect_stream(ranged).await?;

    let ranged_block = ranged
        .first()
        .ok_or_else(|| eyre!("GetBlockRangeNullifiers must return the indexed block"))?;
    assert_nullifiers_only_redaction(&single)?;
    assert_nullifiers_only_redaction(ranged_block)?;

    Ok(())
}

fn nullifier_parity_store() -> eyre::Result<AcceptanceStore> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let block = fixture_block(&chain, ACCEPTANCE_BLOCK_HEIGHT)?;
    let mixed = mixed_pool_transaction_rows(&block, 0);
    let transaction_id = TransactionId::from_bytes([17; 32]);
    let mut deposit = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        block.height,
        block.hash,
        1,
        b"sapling-deposit",
    );
    deposit.facts.public_facts.counts.sapling_output_count = 1;
    let rows = [mixed, deposit];
    let artifact = acceptance_compact_block_artifact_at(&block, &rows)?;
    let mut transactions = artifact.into_parts().transactions;
    let [_, deposit] = transactions.as_mut_slice() else {
        return Err(eyre!("expected a mixed transaction and a Sapling deposit"));
    };
    deposit.data.fee_zat = Some(10_000);
    let compact = CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        canonical_parent_hash(&block),
        block.block_time_seconds,
        transactions,
        block.compact_block_artifact().chain_metadata(),
    )?;
    let chain = rows
        .into_iter()
        .fold(chain, ChainFixture::with_transaction_rows)
        .with_compact_block_artifact(compact);
    acceptance_store_from_chain(&chain)
}

#[tokio::test]
async fn nullifier_range_pool_selection_matches_reference_transactions() -> eyre::Result<()> {
    #[derive(serde::Deserialize)]
    struct Case {
        pool_types: Vec<i32>,
        transactions_hex: Vec<String>,
    }

    // Go lightwalletd v0.6.2 served the mixed transaction and Sapling output-only
    // deposit from nullifier_parity_store through its GetBlockRangeNullifiers handler.
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "fixtures/lightwalletd-v0.6.2-nullifier-pools.json"
    ))?;
    let store_fixture = nullifier_parity_store()?;
    let adapter = acceptance_adapter(&store_fixture);
    for case in cases {
        let request = lightwalletd::BlockRange {
            pool_types: case.pool_types.clone(),
            ..block_range_request(1, 1)
        };
        let stream = adapter
            .get_block_range_nullifiers(Request::new(request))
            .await?
            .into_inner();
        let blocks = collect_stream(stream).await?;
        let [block] = blocks.as_slice() else {
            return Err(eyre!("expected one block for {:?}", case.pool_types));
        };
        assert_eq!(block.chain_metadata, Some(ChainMetadata::default()));
        let transactions: Vec<_> = block
            .vtx
            .iter()
            .map(|tx| hex::encode(prost::Message::encode_to_vec(tx)))
            .collect();
        assert_eq!(
            transactions, case.transactions_hex,
            "requested pools {:?}",
            case.pool_types
        );

        if case.pool_types.is_empty() {
            let single = adapter
                .get_block_nullifiers(Request::new(lightwalletd::BlockId {
                    height: 1,
                    hash: Vec::new(),
                }))
                .await?
                .into_inner();
            assert_eq!(&single, block);
        }
    }
    Ok(())
}

/// Asserts one nullifiers-only compact block carries only shielded nullifiers
/// and has every excluded field cleared.
fn assert_nullifiers_only_redaction(block: &lightwalletd::CompactBlock) -> eyre::Result<()> {
    assert!(
        block.chain_metadata == Some(ChainMetadata::default()),
        "nullifiers-only responses must omit commitment-tree sizes"
    );
    let transaction = block
        .vtx
        .first()
        .ok_or_else(|| eyre!("nullifiers-only block must retain the nullifier-bearing tx"))?;
    assert_eq!(
        transaction.vin.len(),
        0,
        "transparent inputs must be cleared"
    );
    assert_eq!(
        transaction.vout.len(),
        0,
        "transparent outputs must be cleared"
    );
    assert_eq!(
        transaction.outputs.len(),
        0,
        "Sapling outputs must be cleared"
    );
    assert_eq!(
        transaction.spends.len(),
        1,
        "Sapling spend nullifiers must survive"
    );
    assert_eq!(transaction.spends[0].nf, vec![3; 32]);
    let action = transaction
        .actions
        .first()
        .ok_or_else(|| eyre!("Orchard action must survive in nullifier-only form"))?;
    assert_eq!(
        action.nullifier,
        vec![9; 32],
        "Orchard nullifier must survive"
    );
    assert!(action.cmx.is_empty(), "Orchard cmx must be cleared");
    assert!(
        action.ephemeral_key.is_empty(),
        "Orchard ephemeralKey must be cleared"
    );
    assert!(
        action.ciphertext.is_empty(),
        "Orchard ciphertext must be cleared"
    );
    let ironwood_action = transaction
        .ironwood_actions
        .first()
        .ok_or_else(|| eyre!("Ironwood action must survive in nullifier-only form"))?;
    assert_eq!(
        ironwood_action.nullifier,
        vec![13; 32],
        "Ironwood nullifier must survive"
    );
    assert!(
        ironwood_action.cmx.is_empty(),
        "Ironwood cmx must be cleared"
    );
    assert!(
        ironwood_action.ephemeral_key.is_empty(),
        "Ironwood ephemeralKey must be cleared"
    );
    assert!(
        ironwood_action.ciphertext.is_empty(),
        "Ironwood ciphertext must be cleared"
    );
    Ok(())
}

/// Ironwood subtree-root requests are served like the other pools; an empty
/// store yields an empty stream rather than an error.
#[tokio::test]
async fn get_subtree_roots_serves_ironwood() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let stream = adapter
        .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: lightwalletd::ShieldedProtocol::Ironwood as i32,
            max_entries: 1,
        }))
        .await?
        .into_inner();
    let subtree_roots = collect_stream(stream).await?;
    assert!(
        subtree_roots.is_empty(),
        "an empty store must yield an empty ironwood subtree-root stream"
    );

    Ok(())
}

/// Records the `at_epoch_id` argument of every canonical read so a test can
/// assert one handler pins all of its reads to a single chain epoch.
///
/// The recorder can also change the chain view it serves while a handler
/// runs. `visible_tip_block` and `compact_blocks_in_range` answer from view
/// `n` after `n` completed compact-block reads or explicit advances, so view 0 answers
/// a block-range request and its first chunk, and view `n` answers the
/// chunk after the `n`th. A pin on any other view's epoch is refused, which
/// is stricter than production retention and needs no clock.
#[derive(Clone)]
struct EpochPinRecorder<Inner> {
    expose_range_fence: bool,
    gated_chunk_reads: Option<tokio::sync::mpsc::UnboundedSender<GatedChunkRead>>,
    inner: Inner,
    recorded_epoch_ids: Arc<Mutex<Vec<Option<ChainEpochId>>>>,
    recorded_compact_block_ranges: Arc<Mutex<Vec<BlockHeightRange>>>,
    recorded_rows_with_lengths: Arc<Mutex<Vec<BlockHeightRange>>>,
    /// Heights each compact-block range read was offered, which it checks in
    /// full however few blocks its byte budget keeps.
    offered_compact_block_ranges: Arc<Mutex<Vec<BlockHeightRange>>>,
    transparent_address_balance: Option<TransparentAddressBalance>,
    /// Synthetic chain views served in place of `inner`'s blocks.
    synthetic_views: Option<Arc<[SyntheticView]>>,
    /// Real queries that replace `inner` as the served view, in order.
    later_views: Arc<[Inner]>,
    current_view_index: Arc<AtomicUsize>,
    replace_before_header: Option<(usize, BlockHeight)>,
    completed_subtree_reads: Arc<AtomicUsize>,
    /// Epoch of the view that answered each successful compact-block range read.
    served_compact_block_epochs: Arc<Mutex<Vec<ChainEpochId>>>,
    /// Holds every unpinned compact-block range read, the read of a later
    /// block-range chunk, until it has a permit.
    later_chunk_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Notified each time a read waits on `later_chunk_gate`.
    later_chunk_gate_reached: Arc<tokio::sync::Notify>,
    /// Holds the next pinned compact-block range read, the read of a
    /// block-range stream's first chunk, until the gate opens. Only one read
    /// waits on it.
    first_chunk_gate: Arc<Mutex<Option<FirstChunkGate>>>,
    /// Refuses every tree-state rebuild as the query's full rebuild limit
    /// does.
    refuses_tree_state_rebuilds: bool,
    /// Block time every block header reports in place of its stored one, so
    /// a test sets how old the served tip looks.
    block_time_override: Arc<Mutex<Option<i64>>>,
}

struct GatedChunkRead {
    range: BlockHeightRange,
    max_bytes: usize,
    release: tokio::sync::oneshot::Sender<()>,
}

/// A gate on one read that tells the test when the read has reached it.
#[derive(Clone)]
struct FirstChunkGate {
    /// Notified once the read waits on the gate.
    reached: Arc<tokio::sync::Notify>,
    /// The read continues once this has a permit.
    release: Arc<tokio::sync::Semaphore>,
}

impl FirstChunkGate {
    fn closed() -> Self {
        Self {
            reached: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl<Inner> EpochPinRecorder<Inner> {
    fn new(inner: Inner) -> Self {
        Self {
            inner,
            expose_range_fence: false,
            gated_chunk_reads: None,
            recorded_epoch_ids: Arc::new(Mutex::new(Vec::new())),
            recorded_compact_block_ranges: Arc::new(Mutex::new(Vec::new())),
            recorded_rows_with_lengths: Arc::new(Mutex::new(Vec::new())),
            offered_compact_block_ranges: Arc::new(Mutex::new(Vec::new())),
            transparent_address_balance: None,
            synthetic_views: None,
            later_views: Arc::from(Vec::new()),
            current_view_index: Arc::new(AtomicUsize::new(0)),
            replace_before_header: None,
            completed_subtree_reads: Arc::new(AtomicUsize::new(0)),
            served_compact_block_epochs: Arc::new(Mutex::new(Vec::new())),
            later_chunk_gate: None,
            later_chunk_gate_reached: Arc::new(tokio::sync::Notify::new()),
            first_chunk_gate: Arc::new(Mutex::new(None)),
            refuses_tree_state_rebuilds: false,
            block_time_override: Arc::new(Mutex::new(None)),
        }
    }

    /// Holds the read of every later block-range chunk until `gate` has a
    /// permit. Each read returns the permit it took.
    fn with_later_chunk_gate(mut self, gate: Arc<tokio::sync::Semaphore>) -> Self {
        self.later_chunk_gate = Some(gate);
        self
    }

    /// Refuses every tree-state rebuild as a query whose rebuild slots stay
    /// taken past its wait does.
    const fn refusing_tree_state_rebuilds(mut self) -> Self {
        self.refuses_tree_state_rebuilds = true;
        self
    }

    /// Makes every block header report `block_time` from now on.
    fn set_block_time(&self, block_time: i64) {
        *self.block_time_override.lock() = Some(block_time);
    }

    /// Holds the read of the next block-range stream's first chunk on
    /// `gate`. Later first-chunk reads do not wait.
    fn with_first_chunk_gate(mut self, gate: FirstChunkGate) -> Self {
        self.first_chunk_gate = Arc::new(Mutex::new(Some(gate)));
        self
    }

    fn with_transparent_address_balance(
        mut self,
        transparent_address_balance: TransparentAddressBalance,
    ) -> Self {
        self.transparent_address_balance = Some(transparent_address_balance);
        self
    }

    /// Reports `visible_tip_height` as the visible tip and answers every
    /// compact-block range from synthetic blocks, so a test can span more
    /// heights than a store fixture holds.
    fn with_synthetic_compact_blocks_to(self, visible_tip_height: BlockHeight) -> Self {
        self.with_synthetic_views(vec![SyntheticView::entry(
            ENTRY_EPOCH,
            visible_tip_height.value(),
        )])
    }

    /// Serves `views` in turn, as the type documentation describes, with
    /// synthetic blocks.
    fn with_synthetic_views(mut self, views: Vec<SyntheticView>) -> Self {
        self.synthetic_views = Some(Arc::from(views));
        self
    }

    /// Serves `later_views` in turn after `inner`, as the type documentation
    /// describes.
    fn with_later_views(mut self, later_views: Vec<Inner>) -> Self {
        self.later_views = Arc::from(later_views);
        self
    }

    fn record(&self, at_epoch_id: Option<ChainEpochId>) {
        self.recorded_epoch_ids.lock().push(at_epoch_id);
    }

    fn recorded_epoch_ids(&self) -> Vec<Option<ChainEpochId>> {
        self.recorded_epoch_ids.lock().clone()
    }

    fn recorded_compact_block_ranges(&self) -> Vec<BlockHeightRange> {
        self.recorded_compact_block_ranges.lock().clone()
    }

    fn offered_compact_block_ranges(&self) -> Vec<BlockHeightRange> {
        self.offered_compact_block_ranges.lock().clone()
    }

    fn served_compact_block_epochs(&self) -> Vec<ChainEpochId> {
        self.served_compact_block_epochs.lock().clone()
    }

    fn view_index(&self) -> usize {
        self.current_view_index.load(Ordering::SeqCst)
    }

    fn advance_view(&self) {
        self.current_view_index.fetch_add(1, Ordering::SeqCst);
    }

    fn synthetic_view(&self, view_index: usize) -> Option<SyntheticView> {
        let views = self.synthetic_views.as_deref()?;
        views.get(view_index).or_else(|| views.last()).copied()
    }

    fn served_inner(&self, view_index: usize) -> &Inner {
        view_index
            .checked_sub(1)
            .and_then(|later_index| {
                self.later_views
                    .get(later_index)
                    .or_else(|| self.later_views.last())
            })
            .unwrap_or(&self.inner)
    }
}

/// Epoch of the first synthetic view a block-range test serves.
const ENTRY_EPOCH: u64 = 7;

/// Hash of the synthetic block at `height` on `branch`; branch 0 is the chain
/// a block-range request starts on.
fn synthetic_block_hash(height: u32, branch: u8) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    bytes[4] = branch;
    BlockHash::from_bytes(bytes)
}

/// One chain view a synthetic recorder serves: heights `1..=visible_tip_height`
/// of branch 0 below `fork_height`, and of `branch` from `fork_height` on.
/// Height 0 lies outside retained history, as in a store.
#[derive(Clone, Copy, Debug)]
struct SyntheticView {
    epoch_id: ChainEpochId,
    visible_tip_height: BlockHeight,
    branch: u8,
    fork_height: u32,
    /// Sapling outputs in each block's one transaction; zero leaves blocks
    /// empty.
    sapling_outputs_per_block: u16,
    /// A retained height whose block is missing, as store damage leaves it,
    /// so a read that reaches it fails as unavailable storage.
    missing_block_height: Option<u32>,
}

impl SyntheticView {
    /// Branch 0 through `visible_tip_height`.
    const fn entry(epoch_id: u64, visible_tip_height: u32) -> Self {
        Self {
            epoch_id: ChainEpochId::new(epoch_id),
            visible_tip_height: BlockHeight::new(visible_tip_height),
            branch: 0,
            fork_height: u32::MAX,
            sapling_outputs_per_block: 0,
            missing_block_height: None,
        }
    }

    /// Branch 0 below `fork_height` and `branch` from it, through
    /// `visible_tip_height`.
    const fn forked(epoch_id: u64, fork_height: u32, branch: u8, visible_tip_height: u32) -> Self {
        Self {
            epoch_id: ChainEpochId::new(epoch_id),
            visible_tip_height: BlockHeight::new(visible_tip_height),
            branch,
            fork_height,
            sapling_outputs_per_block: 0,
            missing_block_height: None,
        }
    }

    /// The same view with `sapling_outputs` Sapling outputs in every block,
    /// about 120 encoded bytes each.
    const fn with_sapling_outputs(mut self, sapling_outputs: u16) -> Self {
        self.sapling_outputs_per_block = sapling_outputs;
        self
    }

    /// The same view with the block at `height` missing.
    const fn with_missing_block(mut self, height: u32) -> Self {
        self.missing_block_height = Some(height);
        self
    }

    const fn branch_at(&self, height: u32) -> u8 {
        if height >= self.fork_height {
            self.branch
        } else {
            0
        }
    }

    fn block_hash_at(&self, height: u32) -> BlockHash {
        synthetic_block_hash(height, self.branch_at(height))
    }

    fn compact_block_at(&self, height: BlockHeight) -> Result<CompactBlockArtifact, QueryError> {
        let parent_height = height.value().saturating_sub(1);
        let block_id = BlockId::new(height, self.block_hash_at(height.value()));
        let chain_metadata = CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        };
        if self.sapling_outputs_per_block == 0 {
            return Ok(CompactBlockArtifact::empty(
                block_id,
                self.block_hash_at(parent_height),
                0,
                chain_metadata,
            ));
        }
        let height_byte = height.value().to_le_bytes()[0];
        let transaction = CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes(block_id.hash.as_bytes()),
            data: CompactTransactionData {
                sapling_outputs: vec![
                    CompactSaplingOutput {
                        commitment: [height_byte; 32],
                        ephemeral_key: [height_byte; 32],
                        ciphertext: [height_byte; 52],
                    };
                    usize::from(self.sapling_outputs_per_block)
                ],
                ..CompactTransactionData::default()
            },
        };
        CompactBlockArtifact::new(
            block_id,
            self.block_hash_at(parent_height),
            0,
            vec![transaction],
            chain_metadata,
        )
        .map_err(|error| QueryError::CompactBlockPayloadMalformed {
            height,
            reason: error.to_string(),
        })
    }

    /// Reads `block_range` from this view with the budget rule of a store
    /// read.
    ///
    /// The read walks the range in `order` and keeps blocks while their stored
    /// bytes fit `max_bytes`, always keeping the first. Once the kept blocks
    /// take at least `max_bytes`, it stops without looking at the next
    /// height. It returns the heights kept and their blocks in ascending
    /// order.
    fn compact_blocks_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<(BlockHeightRange, Vec<CompactBlockArtifact>), QueryError> {
        let requested = block_range.into_iter().len();
        let maximum = usize::try_from(DEFAULT_MAX_COMPACT_BLOCK_RANGE.get()).unwrap_or(usize::MAX);
        if requested > maximum {
            return Err(QueryError::BlockRangeTooLarge { requested, maximum });
        }
        let unretained_height = if block_range.end > self.visible_tip_height {
            Some(block_range.end)
        } else {
            (block_range.start.value() == 0).then_some(block_range.start)
        };
        if let Some(height) = unretained_height {
            return Err(QueryError::ArtifactUnavailable {
                family: ArtifactFamily::CompactBlock,
                key: ArtifactKey::BlockHeight(height),
            });
        }
        let mut heights = block_range.into_iter().collect::<Vec<_>>();
        if order == HeightOrder::Descending {
            heights.reverse();
        }
        let mut compact_blocks = Vec::new();
        let mut kept_bytes = 0_usize;
        for height in heights {
            if !compact_blocks.is_empty() && kept_bytes >= max_bytes {
                break;
            }
            if self.missing_block_height == Some(height.value()) {
                return Err(QueryError::canonical_read(StorageReadError::Unavailable {
                    source: format!("compact block at height {} is absent", height.value()).into(),
                }));
            }
            let compact_block = self.compact_block_at(height)?;
            let block_bytes = encode_compact_block_artifact(&compact_block).len();
            if !compact_blocks.is_empty() && kept_bytes.saturating_add(block_bytes) > max_bytes {
                break;
            }
            kept_bytes = kept_bytes.saturating_add(block_bytes);
            compact_blocks.push(compact_block);
        }
        if order == HeightOrder::Descending {
            compact_blocks.reverse();
        }
        let kept_range = match (compact_blocks.first(), compact_blocks.last()) {
            (Some(lowest), Some(highest)) => {
                BlockHeightRange::inclusive(lowest.height(), highest.height())
            }
            _ => block_range,
        };
        Ok((kept_range, compact_blocks))
    }

    /// Answers a read at `at_epoch_id`, refusing a pin on any other epoch.
    fn admit_pin(&self, at_epoch_id: Option<ChainEpochId>) -> Result<(), QueryError> {
        match at_epoch_id {
            Some(pinned) if pinned != self.epoch_id => Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: pinned,
            }),
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl<Inner: WalletQueryApi + Clone> WalletQueryApi for EpochPinRecorder<Inner> {
    fn compact_block_fence(
        &self,
        epoch: zinder_core::ChainEpoch,
    ) -> Option<zinder_query::CompactBlockFence> {
        if self.expose_range_fence {
            self.served_inner(self.view_index())
                .compact_block_fence(epoch)
        } else {
            None
        }
    }

    fn native_endpoint_capabilities(&self) -> &zinder_query::NativeWalletEndpointCapabilities {
        self.inner.native_endpoint_capabilities()
    }

    fn upstream_node_capabilities(&self) -> Option<zinder_query::UpstreamNodeCapabilities> {
        self.inner.upstream_node_capabilities()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        self.inner.network_upgrade_activations().await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        let view_index = self.view_index();
        let Some(view) = self.synthetic_view(view_index) else {
            return self
                .served_inner(view_index)
                .visible_tip_block(at_epoch_id)
                .await;
        };
        view.admit_pin(at_epoch_id)?;
        let mut visible_tip_block = self.inner.visible_tip_block(None).await?;
        visible_tip_block.height = view.visible_tip_height;
        visible_tip_block.block_hash = view.block_hash_at(view.visible_tip_height.value());
        visible_tip_block.chain_epoch.id = view.epoch_id;
        visible_tip_block.chain_epoch.visible_tip_height = visible_tip_block.height;
        visible_tip_block.chain_epoch.visible_tip_hash = visible_tip_block.block_hash;
        Ok(visible_tip_block)
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.settled_tip_block(at_epoch_id).await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        self.record(at_epoch_id);
        self.inner.block_id_by_selector(selector, at_epoch_id).await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        self.record(at_epoch_id);
        if let BlockSelector::Height(height) = selector
            && self.replace_before_header == Some((self.view_index(), height))
        {
            self.advance_view();
        }
        let mut header = self
            .served_inner(self.view_index())
            .block_header_by_selector(selector, at_epoch_id)
            .await?;
        let block_time_override = *self.block_time_override.lock();
        if let Some(block_time) = block_time_override {
            header.block_header.block_time = block_time;
        }
        Ok(header)
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.compact_block_at(height, at_epoch_id).await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.compact_blocks_in_range_up_to(
            block_range,
            HeightOrder::Ascending,
            usize::MAX,
            at_epoch_id,
        )
        .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        if let Some(gates) = self.gated_chunk_reads.clone() {
            tokio::task::spawn_blocking(move || {
                let (release, gate) = tokio::sync::oneshot::channel();
                if gates
                    .send(GatedChunkRead {
                        range: block_range,
                        max_bytes,
                        release,
                    })
                    .is_ok()
                {
                    let _ = gate.blocking_recv();
                }
            })
            .await
            .map_err(|error| QueryError::BlockingTaskFailed {
                reason: error.to_string(),
            })?;
        }
        let first_chunk_gate = if at_epoch_id.is_some() {
            self.first_chunk_gate.lock().take()
        } else {
            None
        };
        if let Some(gate) = first_chunk_gate {
            gate.reached.notify_one();
            let _permit = gate.release.acquire().await;
        }
        if at_epoch_id.is_none()
            && let Some(gate) = &self.later_chunk_gate
        {
            self.later_chunk_gate_reached.notify_one();
            let _permit = gate.acquire().await;
        }
        self.record(at_epoch_id);
        self.offered_compact_block_ranges.lock().push(block_range);
        let view_index = self.view_index();
        let compact_block_range = if let Some(view) = self.synthetic_view(view_index) {
            let chain_epoch = self.visible_tip_block(at_epoch_id).await;
            self.advance_view();
            match chain_epoch {
                Ok(visible_tip_block) => view
                    .compact_blocks_up_to(block_range, order, max_bytes)
                    .map(|(block_range, compact_blocks)| CompactBlockRange {
                        chain_epoch: visible_tip_block.chain_epoch,
                        block_range,
                        compact_blocks,
                    }),
                Err(error) => Err(error),
            }
        } else {
            let served = self
                .served_inner(view_index)
                .compact_blocks_in_range_up_to(block_range, order, max_bytes, at_epoch_id)
                .await;
            self.advance_view();
            served
        };
        // Record the heights a read kept, or the requested ones when it failed.
        let recorded_range = compact_block_range
            .as_ref()
            .map_or(block_range, |served| served.block_range);
        self.recorded_compact_block_ranges
            .lock()
            .push(recorded_range);
        if let Ok(served) = &compact_block_range {
            self.served_compact_block_epochs
                .lock()
                .push(served.chain_epoch.id);
        }
        compact_block_range
    }

    async fn compact_block_rows_with_lengths(
        &self,
        block_range: BlockHeightRange,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, QueryError> {
        if !self.expose_range_fence {
            return Ok(None);
        }
        self.recorded_rows_with_lengths.lock().push(block_range);
        self.served_inner(self.view_index())
            .compact_block_rows_with_lengths(block_range, max_bytes, at_epoch_id)
            .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.full_block_at(height, at_epoch_id).await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .full_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        self.record(at_epoch_id);
        self.inner.transaction(transaction_id, at_epoch_id).await
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transaction_at_block_index(height, tx_index, at_epoch_id)
            .await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .raw_transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_address_unspent_outputs(request, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs_on_demand(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        first_page_size: NonZeroU16,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_address_unspent_outputs_on_demand(request, first_page_size, at_epoch_id)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_address_tx_ids_in_range(request, at_epoch_id)
            .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_outpoint_spend_status(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        self.record(at_epoch_id);
        if let Some(transparent_address_balance) = self.transparent_address_balance {
            return Ok(transparent_address_balance);
        }
        self.inner
            .transparent_address_balance(addresses, at_epoch_id)
            .await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.record(at_epoch_id);
        if self.refuses_tree_state_rebuilds {
            return Err(QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays: NonZeroU32::MIN,
            });
        }
        self.inner.tree_state_at(height, at_epoch_id).await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.record(at_epoch_id);
        self.inner.latest_tree_state_checkpoint(at_epoch_id).await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        self.record(at_epoch_id);
        let read_index = self.completed_subtree_reads.fetch_add(1, Ordering::SeqCst);
        self.served_inner(read_index)
            .subtree_roots(subtree_root_range, at_epoch_id)
            .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        self.inner.chain_events(from_cursor, family).await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        self.inner
            .resolve_chain_events_start(start, requested_family)
            .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        self.inner.broadcast_transaction(raw_transaction).await
    }

    async fn standard_fee(&self) -> Result<StandardFee, QueryError> {
        self.inner.standard_fee().await
    }
}

/// The by-hash `GetTransaction` path reads a mined transaction once.
///
/// One canonical read returns its location and bytes, pinned to the epoch
/// the handler resolved, so the response cannot mix two chain states across
/// a tip move.
#[tokio::test]
async fn get_transaction_by_hash_pins_every_read_to_one_epoch() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x77; 32]);
    let transaction_payload = b"single-epoch-transaction-bytes".to_vec();
    let store_fixture =
        acceptance_store_fixture_with_transaction_rows(RawBlobRetention::Transactions, |block| {
            vec![FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block.height,
                block.hash,
                0,
                transaction_payload.clone(),
            )]
        })?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let transaction = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: transaction_id.as_bytes().to_vec(),
        }))
        .await?
        .into_inner();

    assert_eq!(transaction.data, transaction_payload);
    let recorded_pins = recorder.recorded_epoch_ids();
    assert_eq!(
        recorded_pins.len(),
        1,
        "the by-hash path reads the transaction and its bytes once"
    );
    let entry_epoch_id = recorded_pins
        .first()
        .copied()
        .flatten()
        .ok_or_else(|| eyre!("the first canonical read must be pinned to an epoch"))?;
    assert!(
        recorded_pins.iter().all(|pin| *pin == Some(entry_epoch_id)),
        "every read in one handler must pin the same epoch; recorded {recorded_pins:?}"
    );

    Ok(())
}

/// A refused tree-state rebuild is `UNAVAILABLE` on the lightwalletd surface.
///
/// A full block-range limit returns the same code, so both admission limits
/// tell a wallet to retry the same way.
#[tokio::test]
async fn a_refused_tree_state_rebuild_is_unavailable() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = LightwalletdGrpcAdapter::new(
        EpochPinRecorder::new(store_fixture.query()).refusing_tree_state_rebuilds(),
    );

    let status = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: u64::from(ACCEPTANCE_BLOCK_HEIGHT.value()),
            hash: Vec::new(),
        }))
        .await
        .err()
        .ok_or_else(|| eyre!("a refused rebuild must fail the call"))?;
    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn tree_state_reports_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected future-height error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn tree_state_matches_go_for_height_zero_and_the_genesis_hash() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);
    let missing = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId::default()))
        .await
        .err()
        .ok_or_else(|| eyre!("unspecified block identifier was accepted"))?;
    assert_eq!(missing.code(), Code::InvalidArgument);
    let genesis = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: encode_rpc_block_hash_bytes(Network::ZcashRegtest.genesis_hash()).to_vec(),
        }))
        .await
        .err()
        .ok_or_else(|| eyre!("genesis has no Sapling frontier"))?;
    assert_eq!(genesis.code(), Code::InvalidArgument);
    assert_eq!(
        genesis.message(),
        "GetTreeState: z_gettreestate did not return treestate"
    );
    Ok(())
}

pub(super) fn checkpointed_tree_state_store() -> eyre::Result<WalletServingStoreFixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(2)),
        &sample_regtest_upgrade_activations(),
    )?
    .into_serving_store_fixture()
}

pub(super) async fn assert_checkpointed_tree_state_selectors(
    adapter: &impl CompactTxStreamer,
) -> eyre::Result<()> {
    let genesis = encode_rpc_block_hash_bytes(Network::ZcashRegtest.genesis_hash()).to_vec();
    let request = |height, hash| Request::new(lightwalletd::BlockId { height, hash });
    let pruned = adapter
        .get_tree_state(request(1, Vec::new()))
        .await
        .err()
        .ok_or_else(|| eyre!("served a pruned height"))?;
    assert_eq!(pruned.code(), Code::NotFound);
    let retained = adapter
        .get_tree_state(request(3, genesis.clone()))
        .await?
        .into_inner();
    assert_eq!(retained.height, 3);
    let status = adapter
        .get_tree_state(request(0, genesis))
        .await
        .err()
        .ok_or_else(|| eyre!("served a pre-Sapling tree state"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "GetTreeState: z_gettreestate did not return treestate"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_tree_state_rejects_genesis_but_preserves_height_lookups() -> eyre::Result<()>
{
    let store = AcceptanceStore::from_store_fixture(checkpointed_tree_state_store()?, Vec::new())?;
    assert_checkpointed_tree_state_selectors(&acceptance_adapter(&store)).await
}

#[tokio::test]
async fn tree_state_matches_go_at_the_sapling_activation_boundary() -> eyre::Result<()> {
    let mut upgrades = sample_regtest_upgrade_activations().activations().to_vec();
    for upgrade in &mut upgrades {
        upgrade.activation_height = BlockHeight::new(upgrade.activation_height.value().max(2));
    }
    let activations = NetworkUpgradeActivations::new(Network::ZcashRegtest, upgrades)?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let store_fixture = AcceptanceStore::from_store_fixture(
        WalletServingStoreFixture::from_chain(&chain, &activations)?,
        Vec::new(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);
    for height in [1, 2] {
        let native = store_fixture
            .query()
            .tree_state_at(BlockHeight::new(height), None)
            .await?;
        assert_eq!(native.frontiers.sapling_final_state.is_some(), height == 2);
        for request in [
            lightwalletd::BlockId {
                height: u64::from(height),
                hash: Vec::new(),
            },
            lightwalletd::BlockId {
                height: 0,
                hash: encode_rpc_block_hash_bytes(native.block_hash).to_vec(),
            },
        ] {
            let outcome = adapter.get_tree_state(Request::new(request)).await;
            if height == 1 {
                let status = outcome
                    .err()
                    .ok_or_else(|| eyre!("pre-Sapling request succeeded"))?;
                assert_eq!(status.code(), Code::InvalidArgument);
                assert_eq!(
                    status.message(),
                    "GetTreeState: z_gettreestate did not return treestate"
                );
            } else {
                let response = outcome?.into_inner();
                assert_eq!(response.height, 2);
                assert_ne!(response.sapling_tree, "");
            }
        }
    }
    let before_activation = AcceptanceStore::from_store_fixture(
        WalletServingStoreFixture::from_chain(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
            &activations,
        )?,
        Vec::new(),
    )?;
    let native = before_activation
        .query()
        .latest_tree_state_checkpoint(None)
        .await?;
    assert!(native.frontiers.sapling_final_state.is_none());
    let status = acceptance_adapter(&before_activation)
        .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
        .await
        .err()
        .ok_or_else(|| eyre!("pre-Sapling latest tree state succeeded"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    Ok(())
}

/// `GetTreeState(BlockID { height: 0, hash })` accepts RPC byte order.
///
/// It matches `hex::decode(TreeState.hash)` and Go `lightwalletd`, rejects
/// unknown or reversed hashes with `INVALID_ARGUMENT`, and ignores `hash`
/// when `height` is non-zero.
#[tokio::test]
async fn tree_state_by_hash_uses_rpc_byte_order_like_lightwalletd() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let block = fixture_block(&chain, ACCEPTANCE_BLOCK_HEIGHT)?;
    let store_fixture = acceptance_store_from_chain(&chain)?;
    let adapter = acceptance_adapter(&store_fixture);

    let by_height = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let rpc_hash_bytes = hex::decode(&by_height.hash)?;
    assert_eq!(rpc_hash_bytes, encode_rpc_block_hash_bytes(block.hash));

    let by_rpc_hash = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: rpc_hash_bytes,
        }))
        .await?
        .into_inner();
    assert_eq!(by_rpc_hash, by_height);

    // Non-zero height ignores hash, matching reference lightwalletd.
    let by_height_with_bad_hash = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: vec![0xab],
        }))
        .await?
        .into_inner();
    assert_eq!(by_height_with_bad_hash, by_height);

    // Internal-byte-order hash (non-palindromic in ChainFixture) and unknown
    // or malformed hashes fail with INVALID_ARGUMENT.
    let internal_hash_bytes = encode_internal_block_hash(block.hash).to_vec();
    assert_ne!(
        internal_hash_bytes,
        encode_rpc_block_hash_bytes(block.hash),
        "fixture block hash must be non-palindromic to distinguish byte orders"
    );
    for invalid_hash in [
        internal_hash_bytes,
        vec![0xee; 32],
        vec![0xee; 31],
        vec![0xee; 33],
    ] {
        let outcome = adapter
            .get_tree_state(Request::new(lightwalletd::BlockId {
                height: 0,
                hash: invalid_hash,
            }))
            .await;
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::InvalidArgument)
        );
    }

    Ok(())
}

/// `GetTreeState` answers heights between construction checkpoints from the
/// store. The node holds no tree state, and the expected trees come from
/// appending every block's note commitments from genesis.
#[tokio::test]
async fn tree_state_between_checkpoints_is_served_from_storage_without_the_node() -> eyre::Result<()>
{
    let activations = sample_regtest_upgrade_activations();
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(150),
        &activations,
    )?;
    let probed_heights = [1, 2, 57, 99, 100, 101, 149, 150].map(BlockHeight::new);
    let expected = commitment_tree_checkpoints(&chain, &activations, &probed_heights)?;
    let store_fixture = AcceptanceStore::from_store_fixture(
        CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?
            .into_serving_store_fixture()?,
        Vec::new(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    for checkpoint in &expected {
        let height = checkpoint.block_id.height.value();
        let tree_state = adapter
            .get_tree_state(Request::new(lightwalletd::BlockId {
                height: u64::from(height),
                hash: Vec::new(),
            }))
            .await
            .map_err(|status| eyre!("height {height} was refused: {status:?}"))?
            .into_inner();
        let final_state = |protocol: ShieldedProtocol| {
            checkpoint
                .frontiers
                .get(protocol)
                .map_or_default(|frontier| hex::encode(frontier.final_state_bytes()))
        };

        assert_eq!(tree_state.height, u64::from(height));
        assert_eq!(
            tree_state.hash,
            encode_rpc_block_hash_hex(checkpoint.block_id.hash)
        );
        assert_eq!(tree_state.time, checkpoint.block_time_seconds);
        assert_eq!(
            tree_state.sapling_tree,
            final_state(ShieldedProtocol::Sapling),
            "sapling tree at height {height}"
        );
        assert_eq!(
            tree_state.orchard_tree,
            final_state(ShieldedProtocol::Orchard),
            "orchard tree at height {height}"
        );
        assert_eq!(tree_state.ironwood_tree, "");
    }
    Ok(())
}

/// Block-order index of the spending transaction in the UTXO stream fixture.
/// The spend must follow both creating transactions, which the fixture places
/// at indices 0 and 1.
const SPENDING_TRANSACTION_INDEX: u32 = 2;

#[tokio::test]
async fn get_address_utxos_stream_returns_indexed_unspent_transparent_outputs() -> eyre::Result<()>
{
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x11; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let transaction_id = TransactionId::from_bytes([0x55; 32]);
    let spent_transaction_id = TransactionId::from_bytes([0x66; 32]);
    let spending_transaction_id = TransactionId::from_bytes([0x77; 32]);
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::None,
        |_| Vec::new(),
        |block| {
            let unspent_outpoint = TransparentOutPoint::new(transaction_id, 0);
            let spent_outpoint = TransparentOutPoint::new(spent_transaction_id, 0);
            (
                vec![
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                        script_pub_key.clone(),
                        unspent_outpoint,
                        12,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                        script_pub_key.clone(),
                        spent_outpoint,
                        13,
                        block.height,
                        block.hash,
                    ),
                ],
                vec![TransparentSpendFact::new(
                    spent_outpoint,
                    0,
                    spending_transaction_id,
                    SPENDING_TRANSACTION_INDEX,
                    block.height,
                    block.hash,
                    13,
                    TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                    block.height,
                    block.hash,
                )],
            )
        },
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address.clone()],
        start_height: 1,
        max_entries: 10,
    };
    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 1);
    assert_eq!(streamed_utxos[0].address, address);
    assert_eq!(streamed_utxos[0].txid, transaction_id.as_bytes().to_vec());
    assert_eq!(streamed_utxos[0].index, 0);
    assert_eq!(streamed_utxos[0].script, script_pub_key);
    assert_eq!(streamed_utxos[0].value_zat, 12);
    assert_eq!(streamed_utxos[0].height, 1);

    Ok(())
}

#[tokio::test]
async fn get_address_utxos_honors_start_height_floor() -> eyre::Result<()> {
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x12; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    let before_floor_transaction_id = TransactionId::from_bytes([0x12; 32]);
    let at_floor_transaction_id = TransactionId::from_bytes([0x13; 32]);
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let before_floor_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("start-height fixture must include height 1"))?
        .clone();
    let at_floor_block = chain_fixture
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("start-height fixture must include height 2"))?
        .clone();
    let chain_fixture = chain_fixture
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script_pub_key.clone(),
            TransparentOutPoint::new(before_floor_transaction_id, 0),
            12,
            before_floor_block.height,
            before_floor_block.hash,
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script_pub_key,
            TransparentOutPoint::new(at_floor_transaction_id, 0),
            13,
            at_floor_block.height,
            at_floor_block.hash,
        ));
    let store_fixture = acceptance_store_from_chain(&chain_fixture)?;
    let adapter = acceptance_adapter(&store_fixture);
    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address],
        start_height: 2,
        max_entries: 10,
    };

    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 1);
    assert_eq!(streamed_utxos[0].height, 2);
    assert_eq!(streamed_utxos[0].txid, at_floor_transaction_id.as_bytes());

    Ok(())
}

/// Regression: txid bytes emitted by `GetAddressUtxos` must be accepted verbatim
/// by `GetTransaction(TxFilter { hash, ... })`.
///
/// The output and input use the same little-endian txid bytes; reversing the
/// input breaks this round trip. Lightwalletd-go documents the wire contract at
/// `frontend/service.go:792`: txid `bytes` fields are Zcash internal
/// little-endian, the same byte order [`TransactionId::as_bytes`] returns.
#[tokio::test]
async fn get_address_utxos_txid_round_trips_through_get_transaction_by_hash() -> eyre::Result<()> {
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x44; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let transaction_id = TransactionId::from_bytes([0x77; 32]);
    let transaction_payload = b"round-trip-transaction-bytes".to_vec();
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::Transactions,
        |block| {
            vec![FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block.height,
                block.hash,
                0,
                transaction_payload.clone(),
            )]
        },
        |block| {
            (
                vec![TransparentUnspentOutput::new(
                    TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                    script_pub_key.clone(),
                    TransparentOutPoint::new(transaction_id, 0),
                    21,
                    block.height,
                    block.hash,
                )],
                Vec::new(),
            )
        },
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let utxos = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![address],
            start_height: 1,
            max_entries: 10,
        }))
        .await?
        .into_inner();
    let utxo = utxos
        .address_utxos
        .first()
        .ok_or_else(|| eyre!("expected one UTXO in the round-trip fixture"))?;

    let transaction = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: utxo.txid.clone(),
        }))
        .await?
        .into_inner();

    assert_eq!(transaction.height, 1);
    assert_eq!(transaction.data, transaction_payload);

    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the acceptance scenario keeps multi-address ordering and truncation assertions together"
)]
async fn get_address_utxos_applies_max_entries_across_address_set() -> eyre::Result<()> {
    let transparent_address_a =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x21; 20]);
    let transparent_address_b =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x22; 20]);
    let address_a = transparent_address_a.to_string();
    let address_b = transparent_address_b.to_string();
    let script_pub_key_a = transparent_address_a.script().as_raw_bytes().to_vec();
    let script_pub_key_b = transparent_address_b.script().as_raw_bytes().to_vec();
    let first_transaction_id = TransactionId::from_bytes([0x10; 32]);
    let second_transaction_id = TransactionId::from_bytes([0x20; 32]);
    let truncated_transaction_id = TransactionId::from_bytes([0x30; 32]);
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::None,
        |_| Vec::new(),
        |block| {
            (
                vec![
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_a),
                        script_pub_key_a.clone(),
                        TransparentOutPoint::new(truncated_transaction_id, 0),
                        30,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_b),
                        script_pub_key_b.clone(),
                        TransparentOutPoint::new(first_transaction_id, 0),
                        10,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_b),
                        script_pub_key_b.clone(),
                        TransparentOutPoint::new(second_transaction_id, 0),
                        20,
                        block.height,
                        block.hash,
                    ),
                ],
                Vec::new(),
            )
        },
    )?;
    let adapter = LightwalletdGrpcAdapter::new(store_fixture.query());

    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_a, address_b.clone()],
        start_height: 1,
        max_entries: 2,
    };
    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;
    let returned_txids: Vec<_> = streamed_utxos
        .iter()
        .map(|utxo| utxo.txid.clone())
        .collect();

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 2);
    assert_eq!(
        returned_txids,
        vec![
            first_transaction_id.as_bytes().to_vec(),
            second_transaction_id.as_bytes().to_vec(),
        ]
    );
    assert!(streamed_utxos.iter().all(|utxo| utxo.address == address_b));

    Ok(())
}

#[tokio::test]
async fn get_transaction_returns_not_found_when_blob_is_unretained() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x32; 32]);
    let store_fixture =
        acceptance_store_fixture_with_transaction_rows(RawBlobRetention::None, |block| {
            vec![transaction_rows_without_blob(transaction_id, block)]
        })?;
    let adapter = acceptance_adapter(&store_fixture);

    let by_hash_status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: transaction_id.as_bytes().to_vec(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected missing raw blob, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(by_hash_status.code(), Code::NotFound);

    Ok(())
}

#[tokio::test]
async fn send_transaction_forwards_accepted_to_zero_error_code() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x42; 32]);
    let broadcaster = MockTransactionBroadcaster::accepted(transaction_id);
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster).await?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0xde, 0xad, 0xbe, 0xef],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, 0);
    let mut expected_id = transaction_id.as_bytes();
    expected_id.reverse();
    assert_eq!(response.error_message, hex::encode(expected_id));

    Ok(())
}

#[tokio::test]
async fn send_transaction_maps_invalid_encoding_to_minus_22() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::returning(
        TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
            error_code: None,
            message: "TX decode failed".to_owned(),
        }),
    );
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster).await?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0xff],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, -22);
    assert_eq!(response.error_message, "TX decode failed");

    Ok(())
}

#[tokio::test]
async fn send_transaction_maps_duplicate_and_rejected_codes() -> eyre::Result<()> {
    let cases = [
        (
            TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
                error_code: None,
                message: "transaction already in mempool".to_owned(),
            }),
            -27,
            "transaction already in mempool",
        ),
        (
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::Unknown,
                error_code: None,
                message: "bad-txns-invalid".to_owned(),
            }),
            -26,
            "bad-txns-invalid",
        ),
        (
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::UnsupportedTransactionVersion,
                error_code: Some(-25),
                message: "transaction version 4 not supported by the network upgrade Nu7"
                    .to_owned(),
            }),
            -25,
            "transaction version 4 not supported by the network upgrade Nu7",
        ),
        (
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::ExceedsBlockActionLimit,
                error_code: None,
                message: "mempool transaction has 331 Orchard actions, exceeding the next block's ZIP 218 limit of 330"
                    .to_owned(),
            }),
            -26,
            "mempool transaction has 331 Orchard actions, exceeding the next block's ZIP 218 limit of 330",
        ),
        (
            TransactionBroadcastOutcome::Unknown(BroadcastUnknown {
                error_code: None,
                message: "node returned unclassified".to_owned(),
            }),
            -1,
            "node returned unclassified",
        ),
    ];

    for (broadcast_outcome, expected_code, expected_message) in cases {
        let broadcaster = MockTransactionBroadcaster::returning(broadcast_outcome);
        let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster).await?;

        let response = adapter
            .send_transaction(Request::new(lightwalletd::RawTransaction {
                data: vec![0x00],
                height: 0,
            }))
            .await?
            .into_inner();

        assert_eq!(response.error_code, expected_code);
        assert_eq!(response.error_message, expected_message);
    }

    Ok(())
}

#[tokio::test]
async fn send_transaction_forwards_node_error_code_when_present() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::returning(TransactionBroadcastOutcome::Rejected(
        BroadcastRejected {
            kind: BroadcastRejectionReason::Unknown,
            error_code: Some(-25),
            message: "missing-input".to_owned(),
        },
    ));
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster).await?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0x00],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, -25);
    assert_eq!(response.error_message, "missing-input");

    Ok(())
}

/// The first node may have accepted the transaction before its broadcast
/// timeout, so the fallback's duplicate answer is a success.
///
/// Wallets treat a nonzero `errorCode` as a failed send, so the response
/// carries code 0 and the transaction id computed from the submitted bytes.
#[tokio::test]
async fn send_transaction_reports_a_duplicate_after_a_node_timeout_as_sent() -> eyre::Result<()> {
    let raw_transaction = sapling_transaction_without_effects();
    let parsed: zebra_chain::transaction::Transaction =
        raw_transaction.as_slice().zcash_deserialize_into()?;
    let (adapter, _store_fixture) = ordered_broadcasting_acceptance_adapter(vec![
        MockTransactionBroadcaster::node_unavailable("sendrawtransaction timed out after 5000ms"),
        MockTransactionBroadcaster::returning(TransactionBroadcastOutcome::Duplicate(
            BroadcastDuplicate {
                error_code: Some(-27),
                message: "transaction already exists in mempool".to_owned(),
            },
        )),
    ])
    .await?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: raw_transaction,
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, 0);
    assert_eq!(
        response.error_message,
        zinder_core::wire::encode_rpc_transaction_id_hex(TransactionId::from_bytes(
            parsed.hash().0
        ))
    );
    Ok(())
}

/// A version 4 (Sapling) transaction with no inputs, outputs, or shielded
/// data, which the consensus parser decodes.
fn sapling_transaction_without_effects() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0x8000_0004_u32.to_le_bytes());
    bytes.extend_from_slice(&0x892f_2085_u32.to_le_bytes());
    // No transparent inputs or outputs, lock time and expiry height zero,
    // a zero Sapling value balance, and no spends, outputs, or JoinSplits.
    bytes.extend_from_slice(&[0; 2 + 8 + 8 + 3]);
    bytes
}

#[tokio::test]
async fn send_transaction_reports_broadcast_disabled() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0x00],
            height: 0,
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!("expected disabled error, got {response:?}"));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::FailedPrecondition);

    Ok(())
}

#[tokio::test]
async fn taddress_balance_reports_the_confirmed_total_to_generated_lightwalletd_clients()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let wallet_query = store_fixture.query();
    let visible_tip_block = wallet_query.visible_tip_block(None).await?;
    let query_api = EpochPinRecorder::new(wallet_query).with_transparent_address_balance(
        TransparentAddressBalance {
            confirmed_zat: 1_000,
            address_count: 1,
            chain_epoch: visible_tip_block.chain_epoch,
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = LightwalletdGrpcAdapter::new(query_api).into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    {
        let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
        let transparent_address =
            ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x41; 20]);
        let address = transparent_address.to_string();
        let unary_balance = client
            .get_taddress_balance(lightwalletd::AddressList {
                addresses: vec![address.clone()],
            })
            .await?
            .into_inner();
        let stream_balance = client
            .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
                address,
            }]))
            .await?
            .into_inner();

        assert_eq!(unary_balance.value_zat, 1_000);
        assert_eq!(unary_balance, stream_balance);
    }

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

/// A repeated address counts once against the per-request address cap, so a
/// request that names one address more times than the cap is still served.
#[tokio::test]
async fn taddress_balance_counts_a_repeated_address_once() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);
    let address = ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x41; 20])
        .to_string();
    let repeated_address_count =
        usize::try_from(MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES)?.saturating_add(44);

    let balance = adapter
        .get_taddress_balance(Request::new(lightwalletd::AddressList {
            addresses: vec![address; repeated_address_count],
        }))
        .await?
        .into_inner();

    assert_eq!(balance.value_zat, 0);
    Ok(())
}

/// `GetTaddressBalanceStream` rejects the first distinct address over the cap
/// while the client is still streaming, instead of buffering every address
/// until the client half-closes.
#[tokio::test]
async fn taddress_balance_stream_rejects_an_over_cap_address_before_the_stream_ends()
-> eyre::Result<()> {
    let over_cap_count = MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES.saturating_add(1);
    let distinct_addresses = (0..over_cap_count)
        .map(|index| {
            let mut pub_key_hash = [0x41; 20];
            pub_key_hash[..4].copy_from_slice(&index.to_be_bytes());
            ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, pub_key_hash)
                .to_string()
        })
        .collect();

    let status = balance_stream_status_before_half_close(distinct_addresses).await?;

    assert_eq!(status.code(), Code::InvalidArgument);
    Ok(())
}

/// An invalid address is rejected as it arrives, so a client cannot make the
/// server hold large unparsed strings while it keeps the stream open.
#[tokio::test]
async fn taddress_balance_stream_rejects_an_invalid_address_before_the_stream_ends()
-> eyre::Result<()> {
    let oversized_invalid_address = "t".repeat(1024 * 1024);

    let status = balance_stream_status_before_half_close(vec![oversized_invalid_address]).await?;

    assert_eq!(status.code(), Code::InvalidArgument);
    Ok(())
}

/// Address history and UTXO lookups refuse an oversized address before
/// decoding it, so the request cannot spend its time in Base58 decoding.
#[tokio::test]
async fn transparent_address_lookups_refuse_an_oversized_address() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);
    let oversized_address = "t".repeat(1024 * 1024);

    let txids_code = adapter
        .get_taddress_txids(Request::new(lightwalletd::TransparentAddressBlockFilter {
            address: oversized_address.clone(),
            range: Some(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: 0,
                    hash: Vec::new(),
                }),
                end: None,
                pool_types: Vec::new(),
            }),
        }))
        .await
        .err()
        .map(|status| status.code());
    let utxos_code = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![oversized_address],
            start_height: 0,
            max_entries: 0,
        }))
        .await
        .err()
        .map(|status| status.code());

    assert_eq!(txids_code, Some(Code::InvalidArgument));
    assert_eq!(utxos_code, Some(Code::InvalidArgument));
    Ok(())
}

/// Returns the status a never-ending `GetTaddressBalanceStream` gets back.
///
/// Streams `addresses` over a real transport without half-closing, so only a
/// rejection while receiving can answer before the timeout.
async fn balance_stream_status_before_half_close(addresses: Vec<String>) -> eyre::Result<Status> {
    let store_fixture = acceptance_store_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = acceptance_adapter(&store_fixture).into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let outcome = {
        let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
        let never_ending_addresses = tokio_stream::StreamExt::chain(
            tokio_stream::iter(
                addresses
                    .into_iter()
                    .map(|address| lightwalletd::Address { address }),
            ),
            tokio_stream::pending(),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.get_taddress_balance_stream(never_ending_addresses),
        )
        .await
        .map_err(|_| eyre!("stream was not rejected before the client half-closed"))?
    };

    let _ = shutdown_tx.send(());
    server_handle.await??;
    match outcome {
        Ok(response) => Err(eyre!("expected rejection, got {response:?}")),
        Err(status) => Ok(status),
    }
}

#[tokio::test]
async fn generated_lightwalletd_client_streams_over_grpc_transport() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = acceptance_adapter(&store_fixture).into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    {
        let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
        let visible_tip_block = client
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?
            .into_inner();
        let mut compact_blocks = client
            .get_block_range(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: visible_tip_block.height,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: visible_tip_block.height,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let tree_state = client
            .get_latest_tree_state(lightwalletd::Empty {})
            .await?
            .into_inner();

        let compact_block = compact_blocks
            .message()
            .await?
            .ok_or_else(|| eyre!("missing compact block from compatibility stream"))?;

        assert!(compact_blocks.message().await?.is_none());
        assert_eq!(visible_tip_block.height, 1);
        assert_eq!(compact_block.height, visible_tip_block.height);
        assert_eq!(tree_state.height, visible_tip_block.height);
    }

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn tree_state_rejects_zero_empty_block_id_without_reader_fallback() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected block 0 error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn ping_is_refused_like_lightwalletd_without_its_testing_flag() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let outcome = adapter
        .ping(Request::new(lightwalletd::Duration { interval_us: 0 }))
        .await;

    let status = match outcome {
        Ok(response) => return Err(eyre!("expected Ping to be refused, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::FailedPrecondition);

    Ok(())
}

#[tokio::test]
async fn ping_answers_like_lightwalletd_when_enabled() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture).with_ping(LightwalletdPing::enabled(
        tokio_util::sync::CancellationToken::new(),
    ));

    let response = adapter
        .ping(Request::new(lightwalletd::Duration { interval_us: 0 }))
        .await?
        .into_inner();

    assert_eq!((response.entry, response.exit), (1, 0));
    Ok(())
}

#[tokio::test]
async fn get_transaction_rejects_block_index_without_txid_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            index: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected invalid argument, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "GetTransaction: specify a txid");
    Ok(())
}

#[tokio::test]
async fn get_transaction_rejects_unknown_block_index_without_txid_like_lightwalletd()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            index: 99,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!("expected not-found error, got {response:?}"));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "GetTransaction: specify a txid");

    Ok(())
}

async fn collect_stream<T, Stream>(mut stream: Stream) -> Result<Vec<T>, tonic::Status>
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    use tonic::codegen::tokio_stream::StreamExt;

    let mut values = Vec::new();
    while let Some(stream_item) = stream.next().await {
        values.push(stream_item?);
    }
    Ok(values)
}

fn transaction_rows_without_blob(
    transaction_id: TransactionId,
    block: &zinder_testkit::FixtureBlock,
) -> FixtureTransactionRows {
    let location = TransactionLocation::new(transaction_id, block.height, block.hash, 0);
    FixtureTransactionRows::from_public_facts(
        location,
        synthetic_transaction_public_facts(transaction_id, 128),
    )
}

// Release history that one complete baseline cannot express: completed
// subtrees, checkpointed history floors, and blocks a live replacement
// displaced. Each store is built through the production canonical writer.

const FLOOR_HEIGHT: BlockHeight = BlockHeight::new(1);
const FIRST_RETAINED_HEIGHT: BlockHeight = BlockHeight::new(2);

#[tokio::test]
async fn get_subtree_roots_serves_non_empty_orchard_and_ironwood() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let completing_height = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or_else(|| eyre!("regtest activations must activate Ironwood"))?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(completing_height.value());
    let completing_block = fixture_block(&chain, completing_height)?;
    let chain = chain
        .with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(completing_block.height, completing_block.hash),
            completing_block.parent_hash,
            completing_block.block_time_seconds,
            CompactChainMetadata {
                sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
                orchard_commitment_tree_size: SUBTREE_LEAF_COUNT,
                ironwood_commitment_tree_size: SUBTREE_LEAF_COUNT,
            },
        ))
        .with_commitment_tree_frontiers_at(
            completing_height,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Orchard)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Ironwood)?),
            )?,
        );
    let mut baseline = CanonicalBaseline::new(&chain);
    for (protocol, root_byte) in [
        (ShieldedProtocol::Sapling, 0x17),
        (ShieldedProtocol::Orchard, 0x18),
        (ShieldedProtocol::Ironwood, 0x19),
    ] {
        baseline = baseline.with_subtree_root(CanonicalBuildSubtreeRoot {
            protocol,
            subtree_index: SubtreeRootIndex::new(0),
            root_hash: SubtreeRootHash::from_bytes([root_byte; 32]),
            completing_block_height: completing_height,
        });
    }
    let store =
        acceptance_store_from_writer(CanonicalWriterFixture::publish(&baseline, &activations)?)?;
    let adapter = acceptance_adapter(&store);

    for (protocol, root_byte) in [
        (lightwalletd::ShieldedProtocol::Orchard, 0x18),
        (lightwalletd::ShieldedProtocol::Ironwood, 0x19),
    ] {
        let roots = collect_stream(
            adapter
                .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
                    start_index: 0,
                    shielded_protocol: protocol as i32,
                    max_entries: 1,
                }))
                .await?
                .into_inner(),
        )
        .await?;

        assert_eq!(roots.len(), 1, "{protocol:?} must serve its completed root");
        let root = roots
            .first()
            .ok_or_else(|| eyre!("{protocol:?} root missing"))?;
        assert_eq!(root.root_hash, vec![root_byte; 32]);
        // Reference lightwalletd reverses the hash into RPC byte order here,
        // unlike every other compact `bytes` hash field.
        let mut rpc_order_hash = completing_block.hash.as_bytes();
        rpc_order_hash.reverse();
        assert_eq!(root.completing_block_hash, rpc_order_hash.to_vec());
        assert_eq!(
            root.completing_block_height,
            u64::from(completing_height.value())
        );
    }

    Ok(())
}

fn fixture_subtree_root_hash(index: u32) -> SubtreeRootHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&index.to_be_bytes());
    SubtreeRootHash::from_bytes(bytes)
}

/// A store whose completing block closes `sapling_root_count` Sapling subtrees.
fn store_with_sapling_subtree_roots(
    sapling_root_count: u32,
) -> eyre::Result<(AcceptanceStore, BlockHeight)> {
    let (writer, chain) = writer_with_sapling_subtree_roots(sapling_root_count)?;
    let completing_height = chain
        .tip_height()
        .ok_or_else(|| eyre!("missing completing block"))?;
    Ok((acceptance_store_from_writer(writer)?, completing_height))
}

pub(super) fn writer_with_sapling_subtree_roots(
    sapling_root_count: u32,
) -> eyre::Result<(CanonicalWriterFixture, ChainFixture)> {
    let activations = sample_regtest_upgrade_activations();
    let completing_height = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or_else(|| eyre!("regtest activations must activate Ironwood"))?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(completing_height.value());
    let completing_block = fixture_block(&chain, completing_height)?;
    let sapling_tree_size = sapling_root_count.saturating_mul(SUBTREE_LEAF_COUNT);
    let chain = chain
        .with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(completing_block.height, completing_block.hash),
            completing_block.parent_hash,
            completing_block.block_time_seconds,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_tree_size,
                orchard_commitment_tree_size: SUBTREE_LEAF_COUNT,
                ironwood_commitment_tree_size: SUBTREE_LEAF_COUNT,
            },
        ))
        .with_commitment_tree_frontiers_at(
            completing_height,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(frontier_of_size(
                    ShieldedProtocol::Sapling,
                    sapling_tree_size,
                )?),
                Some(completed_subtree_frontier(ShieldedProtocol::Orchard)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Ironwood)?),
            )?,
        );
    let mut baseline = CanonicalBaseline::new(&chain);
    for (protocol, root_count) in [
        (ShieldedProtocol::Sapling, sapling_root_count),
        (ShieldedProtocol::Orchard, 1),
        (ShieldedProtocol::Ironwood, 1),
    ] {
        for index in 0..root_count {
            baseline = baseline.with_subtree_root(CanonicalBuildSubtreeRoot {
                protocol,
                subtree_index: SubtreeRootIndex::new(index),
                root_hash: fixture_subtree_root_hash(index),
                completing_block_height: completing_height,
            });
        }
    }
    Ok((
        CanonicalWriterFixture::publish(&baseline, &activations)?,
        chain,
    ))
}

fn sapling_subtree_roots_request(
    start_index: u32,
    max_entries: u32,
) -> lightwalletd::GetSubtreeRootsArg {
    lightwalletd::GetSubtreeRootsArg {
        start_index,
        shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
        max_entries,
    }
}

/// Reads a `GetSubtreeRoots` stream to its end, keeping the roots that arrived
/// before any error.
async fn drain_subtree_roots<Stream>(
    mut stream: Stream,
) -> (Vec<lightwalletd::SubtreeRoot>, Option<Status>)
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<lightwalletd::SubtreeRoot, Status>>
        + Unpin,
{
    use tonic::codegen::tokio_stream::StreamExt;

    let mut roots = Vec::new();
    while let Some(stream_item) = stream.next().await {
        match stream_item {
            Ok(root) => roots.push(root),
            Err(status) => return (roots, Some(status)),
        }
    }
    (roots, None)
}

/// `maxEntries = 0` means every remaining root, so a store with more roots
/// than one native page (1,024) streams them all at one chain epoch, and a
/// positive `maxEntries` above the native cap is honored.
#[tokio::test]
async fn get_subtree_roots_streams_every_root_past_the_native_page_limit() -> eyre::Result<()> {
    let (store, completing_height) = store_with_sapling_subtree_roots(1_030)?;
    let recorder = EpochPinRecorder::new(store.query());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let (roots, status) = drain_subtree_roots(
        adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(0, 0)))
            .await?
            .into_inner(),
    )
    .await;
    assert!(status.is_none(), "stream ended with {status:?}");
    assert_eq!(roots.len(), 1_030);
    for (index, root) in (0_u32..).zip(&roots) {
        assert_eq!(root.root_hash, fixture_subtree_root_hash(index).as_bytes());
        assert_eq!(
            root.completing_block_height,
            u64::from(completing_height.value())
        );
    }
    let pinned_epochs = recorder.recorded_epoch_ids();
    assert_eq!(pinned_epochs.len(), 2, "1,030 roots take two native pages");
    assert!(pinned_epochs[0].is_some());
    assert_eq!(pinned_epochs[0], pinned_epochs[1]);

    for (start_index, max_entries, expected_count) in [
        (500, 0, 530),
        (0, 1_025, 1_025),
        (1_000, 100, 30),
        (1_030, 0, 0),
        (9_999, 0, 0),
    ] {
        let stream = adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(
                start_index,
                max_entries,
            )))
            .await?
            .into_inner();
        let (roots, status) = drain_subtree_roots(stream).await;
        assert!(
            status.is_none(),
            "start {start_index}, max {max_entries}: {status:?}"
        );
        assert_eq!(
            roots.len(),
            expected_count,
            "start {start_index}, max {max_entries}"
        );
    }
    Ok(())
}

/// A stream that loses its entry epoch after the first page keeps the roots
/// already sent and ends with a retryable `UNAVAILABLE`, not an `OK`
/// truncation.
#[tokio::test]
async fn get_subtree_roots_ends_with_unavailable_when_the_entry_epoch_is_lost() -> eyre::Result<()>
{
    let (store, _) = store_with_sapling_subtree_roots(1_030)?;
    // A live append publishes a second chain epoch and retires the first.
    let advanced_store = AcceptanceStore::from_store_fixture(
        WalletServingStoreFixture::from_chain_after_live_append(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
            &sample_regtest_upgrade_activations(),
        )?,
        Vec::new(),
    )?;
    let recorder =
        EpochPinRecorder::new(store.query()).with_later_views(vec![advanced_store.query()]);
    let adapter = LightwalletdGrpcAdapter::new(recorder);

    let (roots, status) = drain_subtree_roots(
        adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(0, 0)))
            .await?
            .into_inner(),
    )
    .await;

    assert_eq!(roots.len(), 1_000);
    let status = status.ok_or_else(|| eyre!("a stream cut off by epoch loss ended with OK"))?;
    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

/// A full page that reaches the last completed root ends the stream without a
/// further read, so a client that drains the page slowly never sees the entry
/// epoch's retirement as `UNAVAILABLE`.
#[tokio::test]
async fn get_subtree_roots_ends_without_another_read_after_the_last_full_page() -> eyre::Result<()>
{
    let (store, _) = store_with_sapling_subtree_roots(1_030)?;
    let advanced_store = AcceptanceStore::from_store_fixture(
        WalletServingStoreFixture::from_chain_after_live_append(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
            &sample_regtest_upgrade_activations(),
        )?,
        Vec::new(),
    )?;
    let adapter_after_epoch_loss = || {
        let recorder =
            EpochPinRecorder::new(store.query()).with_later_views(vec![advanced_store.query()]);
        (LightwalletdGrpcAdapter::new(recorder.clone()), recorder)
    };
    let (adapter, recorder) = adapter_after_epoch_loss();

    // Roots 30 through 1,029 fill exactly one 1,000-root page.
    let (roots, status) = drain_subtree_roots(
        adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(30, 0)))
            .await?
            .into_inner(),
    )
    .await;
    assert!(status.is_none(), "complete stream ended with {status:?}");
    assert_eq!(roots.len(), 1_000);
    assert_eq!(recorder.recorded_epoch_ids().len(), 1, "one native read");

    // Roots 29 through 1,029 need a second read for the last root, which the
    // lost epoch refuses.
    let (adapter, _) = adapter_after_epoch_loss();
    let (roots, status) = drain_subtree_roots(
        adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(29, 0)))
            .await?
            .into_inner(),
    )
    .await;
    assert_eq!(roots.len(), 1_000);
    let status = status.ok_or_else(|| eyre!("a stream cut off by epoch loss ended with OK"))?;
    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

/// `maxEntries` honors the full `uint32` range. Go lightwalletd with Zebra
/// rejects values above 65,535 with `INVALID_ARGUMENT`; Zinder serves them.
#[tokio::test]
async fn get_subtree_roots_honors_max_entries_above_the_reference_limit() -> eyre::Result<()> {
    let (store, _) = store_with_sapling_subtree_roots(1_030)?;
    let adapter = LightwalletdGrpcAdapter::new(store.query());

    for max_entries in [65_535, 65_536, u32::MAX] {
        let stream = adapter
            .get_subtree_roots(Request::new(sapling_subtree_roots_request(0, max_entries)))
            .await?
            .into_inner();
        let (roots, status) = drain_subtree_roots(stream).await;
        assert!(status.is_none(), "max {max_entries}: {status:?}");
        assert_eq!(roots.len(), 1_030, "max {max_entries}");
    }
    Ok(())
}

/// A checkpointed store holds nothing at or below its history floor except
/// the floor's authenticated tree state.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the retained-history floor contract is a cross-RPC lightwalletd compatibility claim"
)]
async fn retained_history_floor_unavailable_artifacts_return_not_found() -> eyre::Result<()> {
    let store = acceptance_store_from_writer(floor_with_completed_subtree_writer()?)?;
    let adapter = acceptance_adapter(&store);
    let floor = || lightwalletd::BlockId {
        height: u64::from(FLOOR_HEIGHT.value()),
        hash: Vec::new(),
    };
    let floor_range = || lightwalletd::BlockRange {
        start: Some(floor()),
        end: Some(floor()),
        pool_types: Vec::new(),
    };

    let mut observed = Vec::new();
    observed.push((
        "GetBlock",
        adapter.get_block(Request::new(floor())).await.err(),
    ));
    observed.push((
        "GetBlockNullifiers",
        adapter
            .get_block_nullifiers(Request::new(floor()))
            .await
            .err(),
    ));
    observed.push((
        "GetBlockRange",
        first_stream_status(adapter.get_block_range(Request::new(floor_range())).await).await,
    ));
    observed.push((
        "GetBlockRangeNullifiers",
        first_stream_status(
            adapter
                .get_block_range_nullifiers(Request::new(floor_range()))
                .await,
        )
        .await,
    ));
    observed.push((
        "GetSubtreeRoots",
        adapter
            .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
                start_index: 0,
                shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
                max_entries: 1,
            }))
            .await
            .err(),
    ));
    let mismatches = observed
        .into_iter()
        .filter_map(|(method, status)| match status {
            Some(status) if status.code() == Code::NotFound => None,
            Some(status) => Some(format!(
                "{method}: expected NotFound, observed {:?} \"{}\"",
                status.code(),
                status.message()
            )),
            None => Some(format!("{method}: expected NotFound, observed a response")),
        })
        .collect::<Vec<_>>();
    if !mismatches.is_empty() {
        return Err(eyre!(mismatches.join("\n")));
    }

    let floor_tree_state = adapter
        .get_tree_state(Request::new(floor()))
        .await?
        .into_inner();
    assert_eq!(floor_tree_state.height, u64::from(FLOOR_HEIGHT.value()));
    assert_eq!(
        floor_tree_state.sapling_tree,
        hex::encode(completed_subtree_frontier(ShieldedProtocol::Sapling)?.final_state_bytes())
    );

    Ok(())
}

#[tokio::test]
async fn compact_block_methods_serve_first_block_above_retained_history_floor() -> eyre::Result<()>
{
    let (writer, chain) = first_block_above_floor_writer()?;
    let store = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&store);
    let retained_block = fixture_block(&chain, FIRST_RETAINED_HEIGHT)?;
    let retained_height = u64::from(FIRST_RETAINED_HEIGHT.value());
    let retained_id = || lightwalletd::BlockId {
        height: retained_height,
        hash: Vec::new(),
    };

    let block = adapter
        .get_block(Request::new(retained_id()))
        .await?
        .into_inner();
    let ranged_blocks = collect_stream(
        adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(retained_id()),
                end: Some(retained_id()),
                pool_types: Vec::new(),
            }))
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(block.height, retained_height);
    assert_eq!(
        block.hash,
        encode_internal_block_hash(retained_block.hash).to_vec()
    );
    assert_eq!(
        block.prev_hash,
        encode_internal_block_hash(retained_block.parent_hash).to_vec()
    );
    assert_eq!(
        block.chain_metadata,
        Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: 1,
            orchard_commitment_tree_size: 1,
            ironwood_commitment_tree_size: 0,
        })
    );
    let transaction = block
        .vtx
        .first()
        .ok_or_else(|| eyre!("retained block must carry its shielded transaction"))?;
    assert_eq!(
        transaction
            .outputs
            .iter()
            .map(|output| output.cmu.clone())
            .collect::<Vec<_>>(),
        vec![FIXTURE_NOTE_COMMITMENT.to_vec()]
    );
    assert_eq!(
        transaction
            .actions
            .iter()
            .map(|action| action.cmx.clone())
            .collect::<Vec<_>>(),
        vec![FIXTURE_NOTE_COMMITMENT.to_vec()]
    );
    assert_eq!(ranged_blocks, vec![block]);

    Ok(())
}

/// Tree states are derived from the typed frontiers the writer accumulated
/// from the retained block's note commitments.
#[tokio::test]
async fn tree_state_methods_serve_first_checkpoint_above_retained_history_floor() -> eyre::Result<()>
{
    let (writer, _chain) = first_block_above_floor_writer()?;
    let store = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&store);
    let retained_height = u64::from(FIRST_RETAINED_HEIGHT.value());

    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: retained_height,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let latest_tree_state = adapter
        .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(tree_state.height, retained_height);
    assert_eq!(
        tree_state.sapling_tree,
        hex::encode(one_leaf_frontier(ShieldedProtocol::Sapling)?.final_state_bytes())
    );
    assert_eq!(
        tree_state.orchard_tree,
        hex::encode(one_leaf_frontier(ShieldedProtocol::Orchard)?.final_state_bytes())
    );
    assert_eq!(tree_state.ironwood_tree, "");
    assert_eq!(latest_tree_state, tree_state);

    Ok(())
}

#[tokio::test]
async fn get_transaction_returns_not_found_after_reorg_invalidates_transaction() -> eyre::Result<()>
{
    let transaction_id = TransactionId::from_bytes([0x33; 32]);
    let reorged_height = BlockHeight::new(2);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let reorged_block = fixture_block(&chain, reorged_height)?;
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes([0x32; 32]),
            reorged_block.height,
            reorged_block.hash,
            b"raw-reorged-coinbase".to_vec(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            reorged_block.height,
            reorged_block.hash,
            1,
            b"raw-reorged-transaction".to_vec(),
        ));
    let replacement_chain = chain.fork_at(reorged_height)?.extend_blocks(1);
    let replacement_block = fixture_block(&replacement_chain, reorged_height)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let transaction_filter = || lightwalletd::TxFilter {
        block: None,
        index: 0,
        hash: transaction_id.as_bytes().to_vec(),
    };

    let before_reorg =
        AcceptanceStore::from_store_fixture(writer.serving_store_fixture()?, Vec::new())?;
    let served = acceptance_adapter(&before_reorg)
        .get_transaction(Request::new(transaction_filter()))
        .await?
        .into_inner();
    assert_eq!(served.height, u64::from(reorged_height.value()));
    assert_eq!(served.data, b"raw-reorged-transaction".to_vec());

    writer.replace_live_blocks(&replacement_chain, reorged_height)?;
    let after_reorg = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&after_reorg);
    let status = match adapter
        .get_transaction(Request::new(transaction_filter()))
        .await
    {
        Ok(response) => {
            return Err(eyre!(
                "expected reorged transaction to disappear, got {response:?}"
            ));
        }
        Err(status) => status,
    };
    let replacement = adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: u64::from(reorged_height.value()),
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(
        replacement.hash,
        encode_internal_block_hash(replacement_block.hash).to_vec()
    );

    Ok(())
}

/// Checkpointed history whose floor already completed one Sapling subtree,
/// with one retained block that adds no note commitments.
fn floor_with_completed_subtree_writer() -> eyre::Result<CanonicalWriterFixture> {
    let completed_tree = CompactChainMetadata {
        sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    for block in chain.blocks().to_vec() {
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(block.height, block.hash),
            canonical_parent_hash(&block),
            block.block_time_seconds,
            completed_tree,
        ));
    }
    let chain = chain.with_commitment_tree_frontiers_at(
        FLOOR_HEIGHT,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
            None,
            None,
        )?,
    );
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(FLOOR_HEIGHT),
        &sample_regtest_upgrade_activations(),
    )
}

/// Checkpointed history whose first retained block appends one Sapling and
/// one Orchard note commitment to empty trees.
fn first_block_above_floor_writer() -> eyre::Result<(CanonicalWriterFixture, ChainFixture)> {
    let transaction_id = TransactionId::from_bytes([0x52; 32]);
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let retained_block = fixture_block(&chain, FIRST_RETAINED_HEIGHT)?;
    let mut transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        retained_block.height,
        retained_block.hash,
        0,
        b"retained-shielded-transaction".to_vec(),
    );
    transaction_rows.facts.public_facts.counts = TransactionComponentCounts {
        sapling_output_count: 1,
        orchard_action_count: 1,
        ..TransactionComponentCounts::EMPTY
    };
    let compact_block = CompactBlockArtifact::new(
        BlockId::new(retained_block.height, retained_block.hash),
        retained_block.parent_hash,
        retained_block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id,
            data: CompactTransactionData {
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: FIXTURE_NOTE_COMMITMENT,
                    ephemeral_key: [0x22; 32],
                    ciphertext: [0x33; 52],
                }],
                orchard_actions: vec![CompactShieldedAction {
                    nullifier: [0x44; 32],
                    commitment: FIXTURE_NOTE_COMMITMENT,
                    ephemeral_key: [0x55; 32],
                    ciphertext: [0x66; 52],
                }],
                ..CompactTransactionData::default()
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: 1,
            orchard_commitment_tree_size: 1,
            ironwood_commitment_tree_size: 0,
        },
    )?;
    let chain = chain
        .with_transaction_rows(transaction_rows)
        .with_compact_block_artifact(compact_block);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(FLOOR_HEIGHT),
        &sample_regtest_upgrade_activations(),
    )?;
    Ok((writer, chain))
}

fn fixture_block(chain: &ChainFixture, height: BlockHeight) -> eyre::Result<FixtureBlock> {
    chain
        .block_at(height)
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height {}", height.value()))
}

/// The parent the canonical store records for `block`: the network's genesis
/// block below height 1.
fn canonical_parent_hash(block: &FixtureBlock) -> zinder_core::BlockHash {
    if block.height == BlockHeight::new(1) {
        Network::ZcashRegtest.genesis_hash()
    } else {
        block.parent_hash
    }
}

/// Returns the status a streaming call reports, at the call or in its stream.
async fn first_stream_status<T, Stream>(
    outcome: Result<tonic::Response<Stream>, Status>,
) -> Option<Status>
where
    Stream: tokio_stream::Stream<Item = Result<T, Status>> + Unpin,
{
    match outcome {
        Err(status) => Some(status),
        Ok(response) => collect_stream(response.into_inner()).await.err(),
    }
}
/// Serves the writer's final fence through the compatibility composition.
fn acceptance_store_from_writer(writer: CanonicalWriterFixture) -> eyre::Result<AcceptanceStore> {
    AcceptanceStore::from_store_fixture(writer.into_serving_store_fixture()?, Vec::new())
}

/// Compatibility query that broadcasts through a node double, when one is
/// configured.
type AcceptanceQuery = WalletServingQuery<Option<OrderedBroadcaster<MockTransactionBroadcaster>>>;

/// Exact production serving pair queried through the compatibility query.
struct AcceptanceStore {
    query: AcceptanceQuery,
    slot: WalletServingPairSlot,
    node: Option<OrderedBroadcaster<MockTransactionBroadcaster>>,
    _store_fixture: WalletServingStoreFixture,
}

impl AcceptanceStore {
    /// Serves `chain`, broadcasting through `nodes` in order, or storage only
    /// when `nodes` is empty.
    fn from_chain(
        chain: &ChainFixture,
        nodes: Vec<MockTransactionBroadcaster>,
    ) -> eyre::Result<Self> {
        Self::from_store_fixture(
            WalletServingStoreFixture::from_chain(chain, &sample_regtest_upgrade_activations())?,
            nodes,
        )
    }

    fn from_store_fixture(
        mut store_fixture: WalletServingStoreFixture,
        nodes: Vec<MockTransactionBroadcaster>,
    ) -> eyre::Result<Self> {
        let (canonical, wallet) = store_fixture.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        let mut nodes = nodes.into_iter().map(|broadcaster| {
            broadcaster.with_node_activations(sample_regtest_upgrade_activations())
        });
        let node = nodes
            .next()
            .map(|primary| OrderedBroadcaster::new(primary, nodes));
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(node.clone());
        Ok(Self {
            query,
            slot,
            node,
            _store_fixture: store_fixture,
        })
    }

    /// Passes the node's first-contact verification, as the node probe does
    /// once the node answers.
    async fn verify_node(&self) -> eyre::Result<()> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| eyre!("the acceptance store has no node to verify"))?;
        node.verify(&self.slot).await?;
        Ok(())
    }

    fn query(&self) -> AcceptanceQuery {
        self.query.clone()
    }
}

fn acceptance_adapter(store_fixture: &AcceptanceStore) -> LightwalletdGrpcAdapter<AcceptanceQuery> {
    LightwalletdGrpcAdapter::new(store_fixture.query())
}

fn acceptance_store_from_chain(chain: &ChainFixture) -> eyre::Result<AcceptanceStore> {
    AcceptanceStore::from_chain(chain, Vec::new())
}

/// Builds a compatibility adapter whose verified node forwards broadcasts
/// to `broadcaster`.
async fn broadcasting_acceptance_adapter(
    broadcaster: MockTransactionBroadcaster,
) -> eyre::Result<(LightwalletdGrpcAdapter<AcceptanceQuery>, AcceptanceStore)> {
    ordered_broadcasting_acceptance_adapter(vec![broadcaster]).await
}

/// Builds a compatibility adapter that broadcasts through `nodes` in order,
/// every one verified.
async fn ordered_broadcasting_acceptance_adapter(
    nodes: Vec<MockTransactionBroadcaster>,
) -> eyre::Result<(LightwalletdGrpcAdapter<AcceptanceQuery>, AcceptanceStore)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let store_fixture = AcceptanceStore::from_chain(&chain, nodes)?;
    store_fixture.verify_node().await?;
    Ok((acceptance_adapter(&store_fixture), store_fixture))
}

fn acceptance_store_fixture() -> eyre::Result<AcceptanceStore> {
    acceptance_store_fixture_with_transaction_rows(RawBlobRetention::None, |block| {
        vec![mixed_pool_transaction_rows(block, 0)]
    })
}

fn mixed_pool_transaction_rows(
    block: &FixtureBlock,
    tx_index_in_block: u32,
) -> FixtureTransactionRows {
    let transaction_id = TransactionId::from_bytes([2; 32]);
    let mut rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        block.height,
        block.hash,
        tx_index_in_block,
        b"compact-wallet-transaction",
    );
    rows.facts.public_facts.counts = TransactionComponentCounts {
        transparent_input_count: 0,
        transparent_output_count: 0,
        sapling_spend_count: 1,
        sapling_output_count: 1,
        orchard_action_count: 1,
        ironwood_action_count: 1,
        sprout_joinsplit_count: 0,
    };
    rows
}

fn acceptance_store_fixture_with_transaction_rows<TransactionsFn>(
    raw_blob_retention: RawBlobRetention,
    build_transaction_rows: TransactionsFn,
) -> eyre::Result<AcceptanceStore>
where
    TransactionsFn: FnOnce(&zinder_testkit::FixtureBlock) -> Vec<FixtureTransactionRows>,
{
    acceptance_store_fixture_with_transaction_rows_and_transparent(
        raw_blob_retention,
        build_transaction_rows,
        |_| (Vec::new(), Vec::new()),
    )
}

/// Builds the one-block acceptance pair.
///
/// The production fixture publishes empty commitment-tree frontiers, so the
/// compact block keeps its shielded payload but reports zero tree sizes.
fn acceptance_store_fixture_with_transaction_rows_and_transparent<TransactionsFn, TransparentFn>(
    raw_blob_retention: RawBlobRetention,
    build_transaction_rows: TransactionsFn,
    build_transparent_artifacts: TransparentFn,
) -> eyre::Result<AcceptanceStore>
where
    TransactionsFn: FnOnce(&zinder_testkit::FixtureBlock) -> Vec<FixtureTransactionRows>,
    TransparentFn: FnOnce(
        &zinder_testkit::FixtureBlock,
    ) -> (Vec<TransparentUnspentOutput>, Vec<TransparentSpendFact>),
{
    let base_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(raw_blob_retention)
        .extend_blocks(1);
    let acceptance_block = base_fixture
        .block_at(ACCEPTANCE_BLOCK_HEIGHT)
        .ok_or_else(|| eyre!("acceptance fixture must include the height 1 block"))?
        .clone();
    let transaction_rows = build_transaction_rows(&acceptance_block);
    let compact_block = acceptance_compact_block_artifact_at(&acceptance_block, &transaction_rows)?;
    let has_explicit_compact_transactions = !compact_block.transactions().is_empty();
    let (address_output_index, transparent_spend_facts) =
        build_transparent_artifacts(&acceptance_block);

    let mut chain_fixture = base_fixture;
    if has_explicit_compact_transactions {
        chain_fixture = chain_fixture.with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(acceptance_block.height, acceptance_block.hash),
            Network::ZcashRegtest.genesis_hash(),
            acceptance_block.block_time_seconds,
            compact_block.transactions().to_vec(),
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?);
    }
    for transaction_rows in transaction_rows {
        chain_fixture = chain_fixture.with_transaction_rows(transaction_rows);
    }
    for address_output_index in address_output_index {
        chain_fixture = chain_fixture.with_address_output_index(address_output_index);
    }
    for transparent_spend_fact in transparent_spend_facts {
        chain_fixture = chain_fixture.with_transparent_spend_fact(transparent_spend_fact);
    }

    acceptance_store_from_chain(&chain_fixture)
}

fn acceptance_compact_block_artifact_at(
    block: &zinder_testkit::FixtureBlock,
    rows: &[FixtureTransactionRows],
) -> eyre::Result<CompactBlockArtifact> {
    let mut sapling_outputs = 0_u32;
    let mut orchard_actions = 0_u32;
    let mut ironwood_actions = 0_u32;
    let mut transactions = Vec::new();
    for rows in rows {
        let counts = rows.facts.public_facts.counts;
        if counts.sapling_spend_count == 0
            && counts.sapling_output_count == 0
            && counts.orchard_action_count == 0
            && counts.ironwood_action_count == 0
            && counts.transparent_input_count == 0
            && counts.transparent_output_count == 0
        {
            continue;
        }
        sapling_outputs = sapling_outputs.saturating_add(counts.sapling_output_count);
        orchard_actions = orchard_actions.saturating_add(counts.orchard_action_count);
        ironwood_actions = ironwood_actions.saturating_add(counts.ironwood_action_count);
        transactions.push(CompactTransaction {
            index: u64::from(rows.location.tx_index_in_block),
            transaction_id: rows.location.transaction_id,
            data: CompactTransactionData {
                fee_zat: None,
                sapling_spends: (0..counts.sapling_spend_count)
                    .map(|_| CompactSaplingSpend { nullifier: [3; 32] })
                    .collect(),
                sapling_outputs: (0..counts.sapling_output_count)
                    .map(|_| CompactSaplingOutput {
                        commitment: [4; 32],
                        ephemeral_key: [5; 32],
                        ciphertext: [6; 52],
                    })
                    .collect(),
                orchard_actions: (0..counts.orchard_action_count)
                    .map(|_| CompactShieldedAction {
                        nullifier: [9; 32],
                        commitment: [10; 32],
                        ephemeral_key: [11; 32],
                        ciphertext: [12; 52],
                    })
                    .collect(),
                ironwood_actions: (0..counts.ironwood_action_count)
                    .map(|_| CompactShieldedAction {
                        nullifier: [13; 32],
                        commitment: [14; 32],
                        ephemeral_key: [15; 32],
                        ciphertext: [16; 52],
                    })
                    .collect(),
                transparent_inputs: Vec::new(),
                transparent_outputs: Vec::new(),
            },
        });
    }
    Ok(CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: sapling_outputs,
            orchard_commitment_tree_size: orchard_actions,
            ironwood_commitment_tree_size: ironwood_actions,
        },
    )?)
}
