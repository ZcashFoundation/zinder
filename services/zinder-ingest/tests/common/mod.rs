//! Shared helpers for `zinder-ingest`'s live tests under `tests/live/`.
//!
//! These helpers turn a [`LiveTestEnv`] resolved from the unified env-var
//! schema into the ingest-specific config types (`BulkCatchupRunConfig`,
//! `TipFollowConfig`) and run the cross-cutting wallet-API assertions every
//! live bulk catchup needs.

#![allow(
    dead_code,
    reason = "Each live test file consumes only a subset of the common helpers."
)]

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    process::Command,
    sync::Arc,
    time::Duration,
};

use eyre::{Result, eyre};
use prost::Message;
use tonic::Request;
use tonic::codegen::tokio_stream::StreamExt;
use zinder_core::NetworkUpgradeActivations;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_core::{
    BlockHeight, BlockHeightRange, Network, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionBroadcastOutcome,
};
use zinder_core::{BlockId, UnixTimestampMillis};
use zinder_ingest::{
    BulkCatchupRunConfig, CanonicalPipelineLimits, DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS,
    NodeSourceKind, TipFollowConfig,
};
use zinder_ingest::{CanonicalConstructionConfig, load_fresh_canonical};
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
use zinder_query::{
    WalletEndpointMetadata, WalletQuery, WalletQueryApi, WalletQueryGrpcAdapter,
    latest_tree_state_checkpoint_response, subtree_roots_response, tree_state_at_response,
    visible_tip_block_response,
};
use zinder_query::{WalletServingPairSlot, WalletServingReadPair};
use zinder_source::{NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, PrimaryChainStore, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbResourceBudget,
};
use zinder_testkit::{WalletServingStoreFixture, live::LiveTestEnv};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SubtreeRootStartIndices {
    pub(crate) sapling: u32,
    pub(crate) orchard: u32,
}

impl SubtreeRootStartIndices {
    pub(crate) const ZERO: Self = Self {
        sapling: 0,
        orchard: 0,
    };
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalletReadTestRange {
    pub(crate) network: Network,
    pub(crate) start_height: u32,
    pub(crate) end_height: u32,
    pub(crate) subtree_root_start_indices: SubtreeRootStartIndices,
}

/// Opens a materialized-view store with no consumer column families for tests that
/// only need to satisfy writer API wiring.
pub(crate) fn test_materialized_view_store(
    storage_path: &Path,
) -> Result<zinder_materialized_views::MaterializedViewStore> {
    Ok(zinder_materialized_views::MaterializedViewStore::open(
        zinder_materialized_views::MaterializedViewStore::path_for_canonical(storage_path),
        zinder_materialized_views::MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: &[],
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?)
}

/// Builds a `BulkCatchupRunConfig` from a resolved live-test env plus per-test
/// runtime knobs.
#[allow(
    clippy::too_many_arguments,
    reason = "Test helper mirrors the resolved BulkCatchupRunConfig field set."
)]
pub(crate) fn live_bulk_catchup_run_config(
    env: &LiveTestEnv,
    storage_path: &Path,
    from_height: BlockHeight,
    to_height: BlockHeight,
    canonical_batch_max_blocks: NonZeroU32,
    allow_reorg_window_settlement: bool,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
) -> BulkCatchupRunConfig {
    const SOURCE_SEGMENT_MAX_BLOCKS: NonZeroU32 = NonZeroU32::MIN.saturating_add(7);
    BulkCatchupRunConfig {
        node: env.target.clone(),
        node_source: NodeSourceKind::ZebraJsonRpc,
        storage_path: storage_path.to_owned(),
        canonical_rocksdb_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        reorg_window_blocks: 100,
        raw_blob_policy: zinder_ingest::RawBlobPolicy::All,
        network_upgrade_activations,
        from_height,
        to_height,
        canonical_batch_max_blocks,
        canonical_batch_max_artifact_bytes: NonZeroU64::new(512 * 1024 * 1024)
            .unwrap_or(NonZeroU64::MIN),
        canonical_batch_max_estimated_write_bytes: NonZeroU64::new(
            zinder_ingest::DEFAULT_CANONICAL_BATCH_MAX_ESTIMATED_WRITE_BYTES,
        )
        .unwrap_or(NonZeroU64::MIN),
        canonical_batch_min_blocks_before_estimated_write_close: NonZeroU32::new(
            zinder_ingest::DEFAULT_CANONICAL_BATCH_MIN_BLOCKS_BEFORE_ESTIMATED_WRITE_CLOSE,
        )
        .unwrap_or(NonZeroU32::MIN),
        pipeline_limits: CanonicalPipelineLimits {
            max_response_bytes: env.target.max_response_bytes,
            source_segment_max_blocks: SOURCE_SEGMENT_MAX_BLOCKS,
            source_segment_target_response_bytes: NonZeroU64::new(12 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
            source_fetch_max_in_flight_requests: NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
            source_fetch_max_in_flight_bytes: NonZeroU64::new(64 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
            block_prepare_concurrency: SOURCE_SEGMENT_MAX_BLOCKS,
            block_prepare_memory_watermark_bytes: NonZeroU64::new(128 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
        },
        commit_reassembly_max_queued_artifact_bytes: NonZeroU64::new(128 * 1024 * 1024)
            .unwrap_or(NonZeroU64::MIN),
        flush_interval_epochs: NonZeroU32::MIN.saturating_add(4),
        upstream_tip_hint: None,
        allow_reorg_window_settlement,
        checkpoint: None,
    }
}

/// Builds a `TipFollowConfig` from a resolved live-test env.
pub(crate) fn live_tip_follow_config(
    env: &LiveTestEnv,
    storage_path: &Path,
    reorg_window_blocks: u32,
    poll_interval: Duration,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
) -> TipFollowConfig {
    TipFollowConfig {
        node: env.target.clone(),
        storage_path: storage_path.to_owned(),
        canonical_rocksdb_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        raw_blob_policy: zinder_ingest::RawBlobPolicy::All,
        network_upgrade_activations,
        reorg_window_blocks,
        poll_interval,
        lag_threshold_blocks: DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS,
        phase_exit_lag_blocks: None,
        target_height: None,
    }
}

/// Builds a `ZebraJsonRpcSource` from a resolved bulk catchup config.
pub(crate) fn zebra_source_from_bulk_catchup(
    bulk_catchup_config: &BulkCatchupRunConfig,
) -> Result<ZebraJsonRpcSource> {
    match bulk_catchup_config.node_source {
        NodeSourceKind::ZebraJsonRpc => Ok(ZebraJsonRpcSource::with_options(
            bulk_catchup_config.node.network,
            &bulk_catchup_config.node.json_rpc_addr,
            bulk_catchup_config.node.node_auth.clone(),
            ZebraJsonRpcSourceOptions {
                request_timeout: bulk_catchup_config.node.request_timeout,
                max_response_bytes: bulk_catchup_config.node.max_response_bytes,
                broadcast_timeout: None,
            },
        )?),
    }
}

/// Builds a `ZebraJsonRpcSource` from a resolved tip-follow config.
pub(crate) fn zebra_source_from_tip_follow(
    tip_follow_config: &TipFollowConfig,
) -> Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::with_options(
        tip_follow_config.node.network,
        &tip_follow_config.node.json_rpc_addr,
        tip_follow_config.node.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: tip_follow_config.node.request_timeout,
            max_response_bytes: tip_follow_config.node.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

/// Probes the upstream node tip via a fresh source.
pub(crate) async fn fetch_live_tip_height(env: &LiveTestEnv) -> Result<BlockHeight> {
    let probe_source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    Ok(NodeSource::tip_id(&probe_source).await?.height)
}

/// Calls a raw Zebra JSON-RPC method by name.
///
/// Returns the `result` field as a `serde_json::Value`. Live tests use this
/// to drive RPCs the production `NodeSource` trait does not expose (regtest
/// `generate`, `invalidateblock`, `reconsiderblock`, `getblockhash`).
///
/// The Zebra sidecar is expected to be reachable at
/// `env.target.json_rpc_addr` with whatever auth is configured for the
/// node target; the basic-auth credentials carried by `env.target.node_auth`
/// are passed through as the `-u` argument when present so the same helper
/// works against a sidecar configured with `ZEBRA_RPC__ENABLE_COOKIE_AUTH=false`
/// and a `username:password` pair (the default ZFND `z3` setup).
pub(crate) async fn regtest_json_rpc_call(
    env: &LiveTestEnv,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    use secrecy::ExposeSecret;
    use zinder_source::NodeAuth;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    })
    .to_string();
    let mut command = tokio::process::Command::new("curl");
    command
        .arg("-s")
        .args(["-X", "POST"])
        .args(["-H", "content-type: application/json"])
        .arg("-d")
        .arg(&body);
    if let NodeAuth::Basic { username, password } = &env.target.node_auth {
        command
            .arg("-u")
            .arg(format!("{username}:{}", password.expose_secret()));
    }
    command.arg(env.target.json_rpc_addr.as_str());
    let output = command.output().await?;
    if !output.status.success() {
        return Err(eyre!(
            "{method} curl exited with status {:?}: stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let response_body = String::from_utf8(output.stdout)?;
    let parsed: serde_json::Value = serde_json::from_str(&response_body)
        .map_err(|error| eyre!("{method} response is not JSON: {error}; body={response_body}"))?;
    if let Some(error_field) = parsed.get("error")
        && !error_field.is_null()
    {
        return Err(eyre!("{method} RPC returned error: {error_field}"));
    }
    parsed
        .get("result")
        .cloned()
        .ok_or_else(|| eyre!("{method} response missing result field; body={response_body}"))
}

/// Calls Zebra's regtest-only `generate` JSON-RPC to mine `block_count` empty
/// blocks. Returns the list of newly mined block hashes.
///
/// Live tests use this to drive deterministic chain-tip changes against the
/// regtest sidecar without depending on a wallet-side broadcast cycle. Only
/// useful on networks that accept the `generate` RPC; on testnet/mainnet
/// Zebra the call returns an error.
pub(crate) async fn regtest_generate_blocks(
    env: &LiveTestEnv,
    block_count: u32,
) -> Result<Vec<String>> {
    let rpc_result =
        regtest_json_rpc_call(env, "generate", serde_json::json!([block_count])).await?;
    let block_hashes: Vec<String> = serde_json::from_value(rpc_result)
        .map_err(|error| eyre!("regtest generate result is not a list of block hashes: {error}"))?;
    Ok(block_hashes)
}

/// Calls Zebra's regtest-only `invalidateblock` JSON-RPC.
///
/// Drops the named block from the canonical chain along with every
/// descendant, so the tip rolls back to the block's parent. Used by
/// reorg-sweep live tests to force `ChainReorged` chain events.
pub(crate) async fn rpc_invalidate_block(env: &LiveTestEnv, block_hash_hex: &str) -> Result<()> {
    regtest_json_rpc_call(env, "invalidateblock", serde_json::json!([block_hash_hex])).await?;
    Ok(())
}

/// Calls Zebra's regtest-only `reconsiderblock` JSON-RPC.
///
/// Restores a block previously marked invalid via `invalidateblock`.
/// Best-effort cleanup helper for reorg-sweep tests that want to leave the
/// sidecar in a clean state for subsequent runs.
pub(crate) async fn rpc_reconsider_block(env: &LiveTestEnv, block_hash_hex: &str) -> Result<()> {
    regtest_json_rpc_call(env, "reconsiderblock", serde_json::json!([block_hash_hex])).await?;
    Ok(())
}

/// Fetches the network-upgrade activation table from the live node behind `env`.
///
/// Returns the discovered table wrapped in an `Arc` so live tests can thread
/// it through wallet/compat adapter constructors. The compat shim's
/// `GetLightdInfo` rejects requests when the activation table's `network`
/// disagrees with the chain epoch's `network`, so regtest tests may use the
/// hand-built `sample_regtest_upgrade_activations` fixture only when running
/// against a regtest node; tests that opt into testnet or mainnet must
/// discover the table at runtime.
pub(crate) async fn fetch_live_network_upgrade_activations(
    env: &LiveTestEnv,
) -> Result<Arc<NetworkUpgradeActivations>> {
    let source = zebra_source_for_live_env(env)?;
    let activations = source.fetch_network_upgrade_activations().await?;
    Ok(Arc::new(activations))
}

pub(crate) fn zebra_source_for_live_env(env: &LiveTestEnv) -> Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

/// Reorg window for canonical stores built from a live node in tests.
const LIVE_CANONICAL_REORG_WINDOW_BLOCKS: u32 = 100;

/// Production serving pair built from a live node, with the probed source
/// that built it.
pub(crate) struct LiveServingPair {
    /// Exact admitted canonical and wallet secondaries.
    pub(crate) slot: WalletServingPairSlot,
    /// Probed node source for node-backed query methods.
    pub(crate) source: ZebraJsonRpcSource,
    /// Owner of the store directories; dropping it removes them.
    pub(crate) store_fixture: WalletServingStoreFixture,
}

/// Builds a READY production canonical store holding the live chain from
/// genesis through `build_tip_height`, projects its wallet store, and admits
/// one serving pair.
///
/// Construction runs the same fresh-load and baseline publication path the
/// canonical writer uses, retaining transaction bytes and settling the build
/// tip. History is genesis-complete because the wallet projection must
/// resolve every transparent input's predecessor output; a checkpoint-bounded
/// window fails that check as soon as it spends an older output.
pub(crate) async fn live_serving_pair(
    env: &LiveTestEnv,
    activations: &Arc<NetworkUpgradeActivations>,
    build_tip_height: BlockHeight,
) -> Result<LiveServingPair> {
    let source = zebra_source_for_live_env(env)?;
    source.probe_capabilities().await?;
    let build_tip_block = source.fetch_block_at(build_tip_height).await?;
    let build_tip = BlockId::new(build_tip_block.height, build_tip_block.hash);
    let reorg_policy = CanonicalReorgPolicy::new(LIVE_CANONICAL_REORG_WINDOW_BLOCKS)?;
    let genesis_block = source.fetch_block_at(BlockHeight::new(0)).await?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        genesis_block.block_time_seconds,
        build_tip,
        RawBlobRetention::Transactions,
        reorg_policy,
    )?;
    let temporary_directory = tempfile::tempdir()?;
    let canonical_primary_path = temporary_directory.path().join("canonical-primary");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &canonical_primary_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        env.target.request_timeout,
        Arc::clone(activations),
    );
    let loaded = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = loaded.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        build_tip,
        UnixTimestampMillis::now(),
    ))?;
    let canonical_primary = validated.publish_baseline(publication)?;
    let mut store_fixture = WalletServingStoreFixture::from_published_canonical(
        temporary_directory,
        &canonical_primary_path,
        canonical_primary,
        activations,
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?));
    Ok(LiveServingPair {
        slot,
        source,
        store_fixture,
    })
}

/// Returns the coinbase transaction id at `height`.
///
/// The coinbase is the block's first transaction, and its compact form always
/// carries the coinbase's outputs, so the compact block's first entry at
/// index 0 identifies it without a block-index transaction lookup.
pub(crate) async fn coinbase_transaction_id(
    query: &impl WalletQueryApi,
    height: BlockHeight,
) -> Result<zinder_core::TransactionId> {
    let block = query.compact_block_at(height, None).await?;
    block
        .compact_block
        .transactions()
        .first()
        .filter(|transaction| transaction.index == 0)
        .map(|transaction| transaction.transaction_id)
        .ok_or_else(|| eyre!("compact block at height {} has no coinbase", height.value()))
}

/// Calls Zebra's `getblockhash` JSON-RPC and returns the canonical block
/// hash at `height` in Zebra's display (big-endian) encoding.
pub(crate) async fn rpc_block_hash_at_height(env: &LiveTestEnv, height: u32) -> Result<String> {
    let rpc_result =
        regtest_json_rpc_call(env, "getblockhash", serde_json::json!([height])).await?;
    let block_hash: String = serde_json::from_value(rpc_result)
        .map_err(|error| eyre!("getblockhash result is not a hex string: {error}"))?;
    Ok(block_hash)
}

/// Asserts that the bulk-caught-up store answers every wallet read RPC consistently
/// for `[start_height..=end_height]` against the visible chain epoch.
///
/// Live tests pass `activations` discovered from the running node via
/// [`fetch_live_network_upgrade_activations`] so the wallet adapters see
/// a table whose `network` matches the chain epoch's network.
pub(crate) async fn assert_native_wallet_read_responses(
    store: &PrimaryChainStore,
    network: Network,
    start_height: u32,
    end_height: u32,
    activations: Arc<NetworkUpgradeActivations>,
) -> Result<()> {
    assert_wallet_read_responses(
        store,
        WalletReadTestRange {
            network,
            start_height,
            end_height,
            subtree_root_start_indices: SubtreeRootStartIndices::ZERO,
        },
        activations,
    )
    .await
}

pub(crate) async fn assert_wallet_read_responses(
    store: &PrimaryChainStore,
    read_range: WalletReadTestRange,
    activations: Arc<NetworkUpgradeActivations>,
) -> Result<()> {
    let wallet_query = WalletQuery::new(store.clone(), (), Arc::clone(&activations));
    assert_native_compact_block_range_chunks(
        &wallet_query,
        read_range.network,
        read_range.start_height,
        read_range.end_height,
    )
    .await?;
    assert_native_visible_tip_block_response(
        &wallet_query,
        read_range.network,
        read_range.end_height,
    )
    .await?;
    assert_native_tree_state_checkpoint_response(
        &wallet_query,
        read_range.network,
        read_range.end_height,
    )
    .await?;
    assert_native_latest_tree_state_checkpoint_response(
        &wallet_query,
        read_range.network,
        read_range.end_height,
    )
    .await?;
    assert_native_subtree_roots_response(
        &wallet_query,
        read_range.network,
        read_range.subtree_root_start_indices,
    )
    .await?;
    assert_native_wallet_grpc_responses(store, read_range, &activations).await?;
    Ok(())
}

/// Asserts that the native wallet boundary rejects malformed transaction bytes.
pub(crate) async fn assert_native_broadcast_classifies_invalid(
    store: &PrimaryChainStore,
    bulk_catchup_config: &BulkCatchupRunConfig,
    activations: Arc<NetworkUpgradeActivations>,
) -> Result<()> {
    let source = zebra_source_from_bulk_catchup(bulk_catchup_config)?;
    let wallet_query = WalletQuery::new(store.clone(), source, activations);
    let outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0xff, 0xff, 0xff, 0xff]))
        .await?;

    assert!(matches!(
        outcome,
        TransactionBroadcastOutcome::InvalidEncoding(_)
            | TransactionBroadcastOutcome::Rejected(_)
            | TransactionBroadcastOutcome::Unknown(_)
    ));
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Live native gRPC acceptance keeps the read-sync RPC matrix together."
)]
async fn assert_native_wallet_grpc_responses(
    store: &PrimaryChainStore,
    read_range: WalletReadTestRange,
    activations: &Arc<NetworkUpgradeActivations>,
) -> Result<()> {
    let wallet_query = WalletQuery::new(store.clone(), (), Arc::clone(activations));
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());

    let visible_tip_block = WalletQueryService::visible_tip_block(
        &grpc_adapter,
        Request::new(wallet::VisibleTipBlockRequest { at_epoch_id: None }),
    )
    .await?
    .into_inner();
    let mut compact_block_stream = WalletQueryService::compact_blocks_in_range(
        &grpc_adapter,
        Request::new(wallet::CompactBlocksInRangeRequest {
            start_height: read_range.start_height,
            end_height: read_range.end_height,
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let mut compact_block_range = Vec::new();
    while let Some(compact_block_chunk) = compact_block_stream.next().await {
        compact_block_range.push(compact_block_chunk?);
    }
    let tree_state = WalletQueryService::tree_state_at_height(
        &grpc_adapter,
        Request::new(wallet::TreeStateAtHeightRequest {
            height: read_range.end_height,
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let latest_tree_state_checkpoint = WalletQueryService::latest_tree_state_checkpoint(
        &grpc_adapter,
        Request::new(wallet::LatestTreeStateCheckpointRequest { at_epoch_id: None }),
    )
    .await?
    .into_inner();

    assert_native_grpc_response_epoch(
        &visible_tip_block,
        read_range.network,
        read_range.end_height,
    )?;
    for compact_block_chunk in &compact_block_range {
        assert_native_grpc_response_epoch(
            compact_block_chunk,
            read_range.network,
            read_range.end_height,
        )?;
    }
    assert_native_grpc_response_epoch(&tree_state, read_range.network, read_range.end_height)?;
    assert_native_grpc_response_epoch(
        &latest_tree_state_checkpoint,
        read_range.network,
        read_range.end_height,
    )?;

    assert_eq!(
        visible_tip_block
            .visible_tip_block
            .ok_or_else(|| eyre!("native gRPC latest block missing metadata"))?
            .height,
        read_range.end_height
    );
    assert_eq!(
        compact_block_range.len(),
        usize::try_from(read_range.end_height - read_range.start_height + 1)?
    );
    for (offset, compact_block_chunk) in compact_block_range.iter().enumerate() {
        let offset = u32::try_from(offset)?;
        let compact_block = compact_block_chunk
            .compact_block
            .as_ref()
            .ok_or_else(|| eyre!("native gRPC compact-block chunk missing compact block"))?;
        assert_eq!(compact_block.height, read_range.start_height + offset);
        assert_eq!(compact_block.block_hash.len(), 64);
        assert_eq!(compact_block.previous_block_hash.len(), 64);
        assert!(compact_block.chain_metadata.is_some());
    }
    assert_eq!(tree_state.height, read_range.end_height);
    assert!(!tree_state.payload_bytes.is_empty());
    assert_eq!(latest_tree_state_checkpoint.height, read_range.end_height);
    assert!(!latest_tree_state_checkpoint.payload_bytes.is_empty());

    for (protocol, start_index) in [
        (
            wallet::ShieldedProtocol::Sapling,
            read_range.subtree_root_start_indices.sapling,
        ),
        (
            wallet::ShieldedProtocol::Orchard,
            read_range.subtree_root_start_indices.orchard,
        ),
    ] {
        let subtree_roots = WalletQueryService::subtree_roots(
            &grpc_adapter,
            Request::new(wallet::SubtreeRootsRequest {
                shielded_protocol: protocol as i32,
                start_index,
                max_entries: 8,
                at_epoch_id: None,
            }),
        )
        .await?
        .into_inner();
        assert_native_grpc_response_epoch(
            &subtree_roots,
            read_range.network,
            read_range.end_height,
        )?;
        assert_eq!(subtree_roots.start_index, start_index);
        assert!(subtree_roots.subtree_roots.len() <= 8);
    }

    Ok(())
}

trait HasNativeGrpcChainEpoch {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch>;
}

impl HasNativeGrpcChainEpoch for wallet::VisibleTipBlockResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

impl HasNativeGrpcChainEpoch for wallet::CompactBlocksInRangeChunk {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

impl HasNativeGrpcChainEpoch for wallet::TreeStateResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

impl HasNativeGrpcChainEpoch for wallet::SubtreeRootsResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

fn assert_native_grpc_response_epoch(
    response: &impl HasNativeGrpcChainEpoch,
    network: Network,
    end_height: u32,
) -> Result<()> {
    let chain_epoch = response
        .chain_epoch()
        .ok_or_else(|| eyre!("native gRPC response missing chain epoch"))?;

    assert_eq!(
        chain_epoch.network_name,
        encode_zinder_native_chain_name(network)
    );
    assert_eq!(
        chain_epoch
            .visible_tip
            .as_ref()
            .ok_or_else(|| eyre!("native gRPC response missing visible tip"))?
            .height,
        end_height
    );
    Ok(())
}

async fn assert_native_compact_block_range_chunks<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    network: Network,
    start_height: u32,
    end_height: u32,
) -> Result<()> {
    let compact_block_range = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(
                BlockHeight::new(start_height),
                BlockHeight::new(end_height),
            ),
            None,
        )
        .await?;
    let range_chain_epoch = compact_block_range.chain_epoch;
    assert_eq!(range_chain_epoch.network, network);
    assert_eq!(
        range_chain_epoch.visible_tip_height,
        BlockHeight::new(end_height)
    );
    assert_eq!(
        compact_block_range.compact_blocks.len(),
        usize::try_from(end_height - start_height + 1)?
    );

    for (height, compact_block) in
        (start_height..=end_height).zip(compact_block_range.compact_blocks)
    {
        let chunk = wallet::CompactBlocksInRangeChunk {
            chain_view: Some(zinder_store::chain_view_message(range_chain_epoch)),
            compact_block: Some(zinder_proto::wire::compact_block_message(&compact_block)),
        };
        let encoded_chunk = chunk.encode_to_vec();
        let decoded_chunk = wallet::CompactBlocksInRangeChunk::decode(encoded_chunk.as_slice())?;
        let chunk_chain_epoch = decoded_chunk
            .chain_view
            .and_then(|chain_view| chain_view.chain_epoch)
            .ok_or_else(|| eyre!("native compact-block chunk missing chain epoch"))?;
        let compact_block = decoded_chunk
            .compact_block
            .ok_or_else(|| eyre!("native compact-block chunk missing compact block"))?;

        assert_eq!(
            chunk_chain_epoch.network_name,
            encode_zinder_native_chain_name(network)
        );
        assert_eq!(
            chunk_chain_epoch
                .visible_tip
                .ok_or_else(|| eyre!("native compact-block chunk missing visible tip"))?
                .height,
            end_height
        );
        assert_eq!(compact_block.height, height);
        assert_eq!(compact_block.block_hash.len(), 64);
        assert_eq!(compact_block.previous_block_hash.len(), 64);
        assert!(compact_block.chain_metadata.is_some());
    }

    Ok(())
}

async fn assert_native_visible_tip_block_response<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    network: Network,
    end_height: u32,
) -> Result<()> {
    let response = visible_tip_block_response(wallet_query, None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::VisibleTipBlockResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| eyre!("native response missing chain epoch"))?;
    let visible_tip_block = decoded_response
        .visible_tip_block
        .ok_or_else(|| eyre!("native response missing visible-tip block"))?;

    assert_eq!(
        response_chain_epoch.network_name,
        encode_zinder_native_chain_name(network)
    );
    assert_eq!(
        response_chain_epoch
            .visible_tip
            .ok_or_else(|| eyre!("native response missing visible tip"))?
            .height,
        end_height
    );
    assert_eq!(visible_tip_block.height, end_height);
    assert!(!visible_tip_block.block_hash.is_empty());
    Ok(())
}

async fn assert_native_tree_state_checkpoint_response<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    network: Network,
    end_height: u32,
) -> Result<()> {
    let response = tree_state_at_response(wallet_query, BlockHeight::new(end_height), None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::TreeStateResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| eyre!("native response missing chain epoch"))?;

    assert_eq!(
        response_chain_epoch.network_name,
        encode_zinder_native_chain_name(network)
    );
    assert_eq!(
        response_chain_epoch
            .visible_tip
            .ok_or_else(|| eyre!("native response missing visible tip"))?
            .height,
        end_height
    );
    assert_eq!(decoded_response.height, end_height);
    assert!(!decoded_response.block_hash.is_empty());
    assert!(!decoded_response.payload_bytes.is_empty());
    Ok(())
}

async fn assert_native_latest_tree_state_checkpoint_response<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    network: Network,
    end_height: u32,
) -> Result<()> {
    let response = latest_tree_state_checkpoint_response(wallet_query, None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::TreeStateResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| eyre!("native latest tree-state response missing chain epoch"))?;

    assert_eq!(
        response_chain_epoch.network_name,
        encode_zinder_native_chain_name(network)
    );
    assert_eq!(
        response_chain_epoch
            .visible_tip
            .ok_or_else(|| eyre!("native response missing visible tip"))?
            .height,
        end_height
    );
    assert_eq!(decoded_response.height, end_height);
    assert!(!decoded_response.block_hash.is_empty());
    assert!(!decoded_response.payload_bytes.is_empty());
    Ok(())
}

async fn assert_native_subtree_roots_response<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    network: Network,
    subtree_root_start_indices: SubtreeRootStartIndices,
) -> Result<()> {
    for (protocol, start_index) in [
        (
            ShieldedProtocol::Sapling,
            subtree_root_start_indices.sapling,
        ),
        (
            ShieldedProtocol::Orchard,
            subtree_root_start_indices.orchard,
        ),
    ] {
        let response = subtree_roots_response(
            wallet_query,
            SubtreeRootRange::new(
                protocol,
                SubtreeRootIndex::new(start_index),
                NonZeroU32::new(8).ok_or_else(|| eyre!("invalid max entries"))?,
            ),
            None,
        )
        .await?;
        let encoded_response = response.encode_to_vec();
        let decoded_response = wallet::SubtreeRootsResponse::decode(encoded_response.as_slice())?;
        let response_chain_epoch = decoded_response
            .chain_view
            .and_then(|chain_view| chain_view.chain_epoch)
            .ok_or_else(|| eyre!("native subtree-roots response missing chain epoch"))?;

        assert_eq!(
            response_chain_epoch.network_name,
            encode_zinder_native_chain_name(network)
        );
        assert_eq!(decoded_response.start_index, start_index);
        assert!(decoded_response.subtree_roots.len() <= 8);
        for (offset, subtree_root) in decoded_response.subtree_roots.iter().enumerate() {
            assert_eq!(
                subtree_root.subtree_index,
                start_index.saturating_add(u32::try_from(offset)?)
            );
            assert_eq!(subtree_root.root_hash.len(), 32);
            assert_eq!(subtree_root.completing_block_hash.len(), 64);
            assert!(subtree_root.completing_block_height > 0);
        }
    }
    Ok(())
}

/// Returns a `Command` that runs the in-tree `zinder-ingest` binary with a
/// fully cleared environment, ready for env-var injection.
#[must_use]
pub(crate) fn zinder_ingest_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zinder-ingest"));
    command.env_clear();
    command
}

/// Builder for a bounded phase-driven ingest TOML config used by the CLI live tests.
pub(crate) struct BoundedIngestConfigToml<'fields> {
    pub(crate) network_name: &'fields str,
    pub(crate) json_rpc_addr: &'fields str,
    pub(crate) node_auth_username: &'fields str,
    pub(crate) node_auth_password: &'fields str,
    pub(crate) storage_path: &'fields Path,
    pub(crate) target_height: u32,
    pub(crate) request_timeout_secs: u64,
    pub(crate) allow_reorg_window_settlement: bool,
}

/// Builder for a wallet-serving bounded phase-driven ingest TOML config used by the
/// CLI live tests.
pub(crate) struct WalletServingIngestConfigToml<'fields> {
    pub(crate) network_name: &'fields str,
    pub(crate) json_rpc_addr: &'fields str,
    pub(crate) node_auth_username: &'fields str,
    pub(crate) node_auth_password: &'fields str,
    pub(crate) storage_path: &'fields Path,
    pub(crate) target_height: u32,
    pub(crate) request_timeout_secs: u64,
}

/// Renders a `BoundedIngestConfigToml` into the TOML shape `zinder-ingest` accepts.
///
/// The `[ingest.run_overrides].target_height` field makes the ingest loop exit
/// with status 0 once the canonical store reaches that height.
pub(crate) fn bounded_ingest_config_toml(
    config_toml: &BoundedIngestConfigToml<'_>,
) -> Result<String> {
    Ok(format!(
        r#"[network]
name = "{}"

[ops]
listen_addr = "127.0.0.1:0"

[node]
json_rpc_addr = "{}"
request_timeout_secs = {}

[node.auth]
method = "basic"
username = "{}"
password = "{}"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.construction]
canonical_batch_max_blocks = 1000

[ingest.run_overrides]
target_height = {}
allow_reorg_window_settlement = {}

[ingest_control]
listen_addr = "127.0.0.1:0"
"#,
        config_toml.network_name,
        config_toml.json_rpc_addr,
        config_toml.request_timeout_secs,
        config_toml.node_auth_username,
        config_toml.node_auth_password,
        path_str(config_toml.storage_path)?,
        config_toml.target_height,
        config_toml.allow_reorg_window_settlement
    ))
}

/// Renders a wallet-serving bounded phase-driven ingest TOML config.
///
/// The `coverage = "wallet-serving"` modifier instructs the loop to retain
/// complete non-genesis history before committing.
pub(crate) fn wallet_serving_ingest_config_toml(
    config_toml: &WalletServingIngestConfigToml<'_>,
) -> Result<String> {
    Ok(format!(
        r#"[network]
name = "{}"

[ops]
listen_addr = "127.0.0.1:0"

[node]
json_rpc_addr = "{}"
request_timeout_secs = {}

[node.auth]
method = "basic"
username = "{}"
password = "{}"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.construction]
canonical_batch_max_blocks = 100

[ingest.run_overrides]
coverage = "wallet-serving"
target_height = {}

[ingest_control]
listen_addr = "127.0.0.1:0"
"#,
        config_toml.network_name,
        config_toml.json_rpc_addr,
        config_toml.request_timeout_secs,
        config_toml.node_auth_username,
        config_toml.node_auth_password,
        path_str(config_toml.storage_path)?,
        config_toml.target_height,
    ))
}

/// Returns the node's basic-auth `(username, password)` from a resolved
/// live-test env, or an error if the env did not select Basic.
pub(crate) fn basic_auth_credentials(env: &LiveTestEnv) -> Result<(&str, &str)> {
    use secrecy::ExposeSecret;
    use zinder_source::NodeAuth;
    match &env.target.node_auth {
        NodeAuth::Basic { username, password } => Ok((username, password.expose_secret())),
        NodeAuth::None | NodeAuth::Cookie(_) => Err(eyre!(
            "live CLI test requires basic auth; set ZINDER_NODE__AUTH__METHOD=basic"
        )),
    }
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre!("path is not valid UTF-8: {}", path.display()))
}
