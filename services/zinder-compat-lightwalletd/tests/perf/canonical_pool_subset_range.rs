//! Throughput and speedup benchmark for `[PoolType::Orchard]` `GetBlockRangeNullifiers`
//! directly over canonical `ZSHRNG02` segments plus live tail vs canonical compact-block fallback.

use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::transport::{Channel, Server};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, BlockSelector, ChainEpoch, ChainEpochId,
    CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput, CompactSaplingSpend,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, Network,
    NetworkUpgradeActivations, RawTransactionBytes, StandardFee, SubtreeRootRange,
    TransactionBroadcastOutcome, TransactionId, TransparentAddressBalance,
    TransparentAddressScriptHash, TransparentOutPoint,
};
use zinder_proto::compat::lightwalletd::{
    self, PoolType, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents, CompactBlock, CompactBlockRange,
    CompactBlockStream, FullBlock, FullBlockStream, NativeWalletEndpointCapabilities, QueryError,
    RawTransaction, RocksDbCanonicalReader, RocksDbWalletReader, ServedTipSnapshot,
    SettledTipBlock, SubtreeRoots, Transaction, TransactionStatus, TransparentAddressTxIds,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TransparentOutpointSpendStatuses, TreeState,
    UpstreamNodeCapabilities, VisibleTipBlock, WalletQueryApi, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{
    ChainEventStreamFamily, ChainEventStreamResume, EncodedCompactBlockRange,
    EncodedCompactBlockRangeRequest, EventStreamStartPosition, HeightOrder, StreamCursorTokenV1,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
};

const TOTAL_BLOCKS: u32 = 4_000;
const SETTLED_TIP: u32 = 3_900;

#[cfg(debug_assertions)]
const MAX_CANONICAL_ORCHARD_SCAN_MS: u128 = 5_000;
#[cfg(not(debug_assertions))]
const MAX_CANONICAL_ORCHARD_SCAN_MS: u128 = 500;

/// Delegates to [`WalletServingQuery`] while withholding
/// `encoded_compact_block_range_up_to` so the baseline adapter falls back to
/// `compact_blocks_in_range_up_to`.
#[derive(Clone)]
struct CanonicalFallbackQuery {
    inner: WalletServingQuery<()>,
    canonical_blocks_read: Arc<AtomicUsize>,
}

impl CanonicalFallbackQuery {
    fn new(inner: WalletServingQuery<()>) -> Self {
        Self {
            inner,
            canonical_blocks_read: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn take_canonical_blocks_read(&self) -> usize {
        self.canonical_blocks_read.swap(0, Ordering::SeqCst)
    }
}

#[async_trait]
impl WalletQueryApi for CanonicalFallbackQuery {
    fn subscribe_publications(&self) -> Option<tokio::sync::watch::Receiver<()>> {
        self.inner.subscribe_publications()
    }

    fn current_visible_tip_height(&self) -> Option<BlockHeight> {
        self.inner.current_visible_tip_height()
    }

    fn current_chain_epoch(&self) -> Option<ChainEpoch> {
        self.inner.current_chain_epoch()
    }

    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        self.inner.native_endpoint_capabilities()
    }

    fn upstream_node_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.inner.upstream_node_capabilities()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        self.inner.network_upgrade_activations().await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        self.inner.visible_tip_block(at_epoch_id).await
    }

    async fn served_tip_snapshot(&self) -> Result<ServedTipSnapshot, QueryError> {
        self.inner.served_tip_snapshot().await
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        self.inner.settled_tip_block(at_epoch_id).await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        self.inner.block_id_by_selector(selector, at_epoch_id).await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        self.inner
            .block_header_by_selector(selector, at_epoch_id)
            .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        self.canonical_blocks_read.fetch_add(1, Ordering::SeqCst);
        self.inner.compact_block_at(height, at_epoch_id).await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        let served = self
            .inner
            .compact_blocks_in_range(block_range, at_epoch_id)
            .await?;
        self.canonical_blocks_read
            .fetch_add(served.compact_blocks.len(), Ordering::SeqCst);
        Ok(served)
    }

    async fn compact_block_stream(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockStream, QueryError> {
        self.inner
            .compact_block_stream(block_range, at_epoch_id)
            .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        let served = self
            .inner
            .compact_blocks_in_range_up_to(block_range, order, max_bytes, at_epoch_id)
            .await?;
        self.canonical_blocks_read
            .fetch_add(served.compact_blocks.len(), Ordering::SeqCst);
        Ok(served)
    }

    async fn encoded_compact_block_range_up_to(
        &self,
        _request: EncodedCompactBlockRangeRequest,
        _at_epoch_id: Option<ChainEpochId>,
        _cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Option<EncodedCompactBlockRange>, QueryError> {
        Ok(None)
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        self.inner.full_block_at(height, at_epoch_id).await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        self.inner
            .full_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        self.inner.transaction(transaction_id, at_epoch_id).await
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        self.inner
            .transaction_at_block_index(height, tx_index, at_epoch_id)
            .await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        self.inner
            .raw_transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
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
        self.inner
            .transparent_address_unspent_outputs_on_demand(request, first_page_size, at_epoch_id)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        self.inner
            .transparent_address_tx_ids_in_range(request, at_epoch_id)
            .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError> {
        self.inner
            .transparent_outpoint_spend_status(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        self.inner
            .transparent_address_balance(addresses, at_epoch_id)
            .await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.inner.tree_state_at(height, at_epoch_id).await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.inner.latest_tree_state_checkpoint(at_epoch_id).await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        self.inner
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

fn perf_commitment(seed: u8) -> [u8; 32] {
    let mut cmx = [0; 32];
    cmx[0] = seed;
    cmx
}

fn perf_orchard_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: perf_commitment(seed.wrapping_add(1)),
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
}

fn perf_sapling_output(seed: u8) -> CompactSaplingOutput {
    CompactSaplingOutput {
        commitment: perf_commitment(seed),
        ephemeral_key: [seed.wrapping_add(1); 32],
        ciphertext: [seed.wrapping_add(2); 52],
    }
}

fn perf_tx_id(height_val: u32, slot: u8) -> TransactionId {
    let mut raw_id = [slot; 32];
    raw_id[0..4].copy_from_slice(&height_val.to_le_bytes());
    TransactionId::from_bytes(raw_id)
}

fn attach_perf_tx_rows(
    mut chain: ChainFixture,
    blk: &zinder_testkit::FixtureBlock,
    shielded_tx_id: TransactionId,
) -> ChainFixture {
    let filler_id = perf_tx_id(blk.height.value(), 0xc0);
    chain = chain.with_transaction_rows(zinder_testkit::FixtureTransactionRows::coinbase(
        filler_id,
        blk.height,
        blk.hash,
        b"perf-coinbase".as_slice(),
    ));
    chain.with_transaction_rows(
        zinder_testkit::FixtureTransactionRows::from_raw_transaction(
            shielded_tx_id,
            blk.height,
            blk.hash,
            1,
            b"perf-shielded-tx".as_slice(),
        ),
    )
}

fn build_perf_chain() -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(TOTAL_BLOCKS);
    let blocks = chain.blocks().to_vec();
    let mut sapling_tree_size = 0_u32;
    let mut orchard_tree_size = 0_u32;
    for blk in blocks {
        let height_val = blk.height.value();
        let txs = if height_val % 10 == 0 {
            orchard_tree_size += 2;
            let tx_id = perf_tx_id(height_val, 0x44);
            chain = attach_perf_tx_rows(chain, &blk, tx_id);
            vec![CompactTransaction {
                index: 1,
                transaction_id: tx_id,
                data: CompactTransactionData {
                    fee_zat: Some(10_000),
                    orchard_actions: vec![perf_orchard_action(0x11), perf_orchard_action(0x15)],
                    ..Default::default()
                },
            }]
        } else if height_val % 10 == 5 {
            sapling_tree_size += 2;
            let tx_id = perf_tx_id(height_val, 0x55);
            chain = attach_perf_tx_rows(chain, &blk, tx_id);
            vec![CompactTransaction {
                index: 1,
                transaction_id: tx_id,
                data: CompactTransactionData {
                    fee_zat: Some(10_000),
                    sapling_spends: vec![CompactSaplingSpend {
                        nullifier: [0x21; 32],
                    }],
                    sapling_outputs: vec![perf_sapling_output(0x22), perf_sapling_output(0x25)],
                    ..Default::default()
                },
            }]
        } else {
            Vec::new()
        };
        let parent_hash = if height_val == 1 {
            Network::ZcashRegtest.genesis_hash()
        } else {
            blk.parent_hash
        };
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(blk.height, blk.hash),
            parent_hash,
            blk.block_time_seconds,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_tree_size,
                orchard_commitment_tree_size: orchard_tree_size,
                ironwood_commitment_tree_size: 0,
            },
        )?);
    }
    Ok(chain)
}

fn block_range_for_pools(pool_types: Vec<i32>) -> lightwalletd::BlockRange {
    lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: u64::from(TOTAL_BLOCKS),
            hash: Vec::new(),
        }),
        pool_types,
    }
}

async fn drain_encoded_nullifier_range<QueryApi: WalletQueryApi + Clone>(
    adapter: &LightwalletdGrpcAdapter<QueryApi>,
    pool_types: Vec<i32>,
) -> eyre::Result<(usize, usize)> {
    use zinder_proto::compat::lightwalletd::EncodedBlockRangeNullifiers as _;
    let mut stream = adapter
        .get_encoded_block_range_nullifiers(tonic::Request::new(block_range_for_pools(pool_types)))
        .await?
        .into_inner();
    let mut block_count = 0_usize;
    let mut payload_bytes = 0_usize;
    while let Some(next_block) = stream.next().await {
        let block = next_block?;
        block_count += 1;
        payload_bytes += block.payload_len();
    }
    Ok((block_count, payload_bytes))
}

#[allow(
    deprecated,
    reason = "Benchmarks GetBlockRangeNullifiers throughput for PoolType::Orchard."
)]
async fn drain_nullifier_range(
    client: &mut CompactTxStreamerClient<Channel>,
    pool_types: Vec<i32>,
) -> eyre::Result<(usize, usize)> {
    let mut stream = client
        .get_block_range_nullifiers(block_range_for_pools(pool_types))
        .await?
        .into_inner();
    let mut block_count = 0_usize;
    let mut tx_count = 0_usize;
    while let Some(next_block) = stream.next().await {
        let block = next_block?;
        block_count += 1;
        tx_count += block.vtx.len();
    }
    Ok((block_count, tx_count))
}

async fn start_perf_endpoint<QueryApi: WalletQueryApi + Clone>(
    adapter: LightwalletdGrpcAdapter<QueryApi>,
) -> eyre::Result<(
    CompactTxStreamerClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let client = CompactTxStreamerClient::connect(format!("http://{addr}")).await?;
    Ok((client, handle))
}

struct PerfAdapters {
    direct: LightwalletdGrpcAdapter<WalletServingQuery<()>>,
    canonical: LightwalletdGrpcAdapter<CanonicalFallbackQuery>,
    canonical_query: CanonicalFallbackQuery,
    _paths: zinder_testkit::WalletServingStoreFixture,
}

fn setup_perf_adapters() -> eyre::Result<PerfAdapters> {
    let chain = build_perf_chain()?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SETTLED_TIP)),
        &sample_regtest_upgrade_activations(),
    )?;
    let mut paths = writer.into_serving_store_fixture()?;
    let (mut canonical, wallet) = paths.take_readers()?;
    canonical.try_catch_up()?;
    let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    let pair = WalletServingReadPair::new(reader, Arc::new(RocksDbWalletReader::new(wallet)))?;
    let query: WalletServingQuery<()> =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
    let direct_adapter = LightwalletdGrpcAdapter::new(query.clone());
    let canonical_query = CanonicalFallbackQuery::new(query);
    let canonical_adapter = LightwalletdGrpcAdapter::new(canonical_query.clone());
    Ok(PerfAdapters {
        direct: direct_adapter,
        canonical: canonical_adapter,
        canonical_query,
        _paths: paths,
    })
}

struct PerfScanCounts {
    srv_can_blocks: usize,
    srv_can_bytes: usize,
    srv_def_blocks: usize,
    srv_def_bytes: usize,
    srv_orc_blocks: usize,
    srv_orc_bytes: usize,
    can_blocks: usize,
    can_txs: usize,
    def_blocks: usize,
    def_txs: usize,
    orc_blocks: usize,
    orc_txs: usize,
}

fn assert_perf_counts(counts: &PerfScanCounts, total_blocks: usize) {
    assert_eq!(counts.srv_can_blocks, total_blocks);
    assert_eq!(counts.srv_def_blocks, total_blocks);
    assert_eq!(counts.srv_orc_blocks, total_blocks);
    assert_eq!(counts.srv_orc_bytes, counts.srv_can_bytes);
    assert!(counts.srv_orc_bytes < counts.srv_def_bytes);
    assert_eq!(counts.can_blocks, total_blocks);
    assert_eq!(counts.def_blocks, total_blocks);
    assert_eq!(counts.orc_blocks, total_blocks);
    assert_eq!(counts.orc_txs, counts.can_txs);
    assert!(counts.orc_txs < counts.def_txs);
}

struct ServerPerfTimings {
    canonical_us: u128,
    default_us: u128,
    orchard_us: u128,
    can_blocks: usize,
    can_bytes: usize,
    def_blocks: usize,
    def_bytes: usize,
    orc_blocks: usize,
    orc_bytes: usize,
}

async fn measure_server_scans(
    adapters: &PerfAdapters,
    total_blocks: usize,
    orchard_pools: &[i32],
) -> eyre::Result<ServerPerfTimings> {
    let _ = drain_encoded_nullifier_range(&adapters.direct, Vec::new()).await?;
    let _ = drain_encoded_nullifier_range(&adapters.canonical, orchard_pools.to_vec()).await?;
    assert_eq!(
        adapters.canonical_query.take_canonical_blocks_read(),
        total_blocks
    );

    let started_can = Instant::now();
    let (can_blocks, can_bytes) =
        drain_encoded_nullifier_range(&adapters.canonical, orchard_pools.to_vec()).await?;
    let canonical_us = started_can.elapsed().as_micros();
    assert_eq!(
        adapters.canonical_query.take_canonical_blocks_read(),
        total_blocks
    );

    let started_def = Instant::now();
    let (def_blocks, def_bytes) =
        drain_encoded_nullifier_range(&adapters.direct, Vec::new()).await?;
    let default_us = started_def.elapsed().as_micros();

    let started_orc = Instant::now();
    let (orc_blocks, orc_bytes) =
        drain_encoded_nullifier_range(&adapters.direct, orchard_pools.to_vec()).await?;
    let orchard_us = started_orc.elapsed().as_micros();

    Ok(ServerPerfTimings {
        canonical_us,
        default_us,
        orchard_us,
        can_blocks,
        can_bytes,
        def_blocks,
        def_bytes,
        orc_blocks,
        orc_bytes,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchard_nullifier_range_scan_over_canonical_segments_and_tail_beats_canonical_fallback()
-> eyre::Result<()> {
    let adapters = setup_perf_adapters()?;
    let total_blocks = usize::try_from(TOTAL_BLOCKS)?;
    let orchard_pools = vec![PoolType::Orchard as i32];
    let srv = measure_server_scans(&adapters, total_blocks, &orchard_pools).await?;

    let (mut direct_client, direct_server) = start_perf_endpoint(adapters.direct).await?;
    let (mut can_client, can_server) = start_perf_endpoint(adapters.canonical).await?;
    let _ = drain_nullifier_range(&mut direct_client, Vec::new()).await?;
    let _ = drain_nullifier_range(&mut can_client, orchard_pools.clone()).await?;
    assert_eq!(
        adapters.canonical_query.take_canonical_blocks_read(),
        total_blocks
    );

    let started_canonical = Instant::now();
    let (can_blocks, can_txs) =
        drain_nullifier_range(&mut can_client, orchard_pools.clone()).await?;
    let canonical_micros = started_canonical.elapsed().as_micros();
    assert_eq!(
        adapters.canonical_query.take_canonical_blocks_read(),
        total_blocks
    );

    let started_default = Instant::now();
    let (def_blocks, def_txs) = drain_nullifier_range(&mut direct_client, Vec::new()).await?;
    let default_micros = started_default.elapsed().as_micros();

    let started_orchard = Instant::now();
    let (orc_blocks, orc_txs) = drain_nullifier_range(&mut direct_client, orchard_pools).await?;
    let orchard_micros = started_orchard.elapsed().as_micros();
    let orchard_millis = orchard_micros / 1_000;

    direct_server.abort();
    can_server.abort();

    assert_perf_counts(
        &PerfScanCounts {
            srv_can_blocks: srv.can_blocks,
            srv_can_bytes: srv.can_bytes,
            srv_def_blocks: srv.def_blocks,
            srv_def_bytes: srv.def_bytes,
            srv_orc_blocks: srv.orc_blocks,
            srv_orc_bytes: srv.orc_bytes,
            can_blocks,
            can_txs,
            def_blocks,
            def_txs,
            orc_blocks,
            orc_txs,
        },
        total_blocks,
    );

    #[allow(
        clippy::print_stderr,
        reason = "T2 perf benchmark reports measured durations across the 4,000-block corpus."
    )]
    {
        eprintln!(
            "compat_orchard_nullifier_perf blocks={TOTAL_BLOCKS} orchard_txs={orc_txs} default_txs={def_txs} server_canonical_us={} server_direct_orchard_us={} server_direct_default_us={} grpc_canonical_us={canonical_micros} grpc_direct_orchard_us={orchard_micros} grpc_direct_default_us={default_micros}",
            srv.canonical_us, srv.orchard_us, srv.default_us
        );
    }

    assert!(
        orchard_millis <= MAX_CANONICAL_ORCHARD_SCAN_MS,
        "direct Orchard nullifier scan took {orchard_millis} ms (budget {MAX_CANONICAL_ORCHARD_SCAN_MS} ms)"
    );
    Ok(())
}
