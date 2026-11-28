//! A `CompactTxStreamer` test server for block ranges and tree states.
//!
//! It ends its streams the way a test asks, counts the tree-state requests it
//! holds at once, and logs the requests a test inspects. Methods a test has
//! no use for are unimplemented.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::stream::{self, BoxStream, StreamExt as _};
use parking_lot::Mutex;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, Streaming, transport::Server};
use zinder_core::CommitmentTreeFrontier;
use zinder_proto::compat::lightwalletd::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainMetadata, ChainSpec, CompactBlock,
    CompactOrchardAction, CompactSaplingOutput, CompactTx, Duration as PingDuration, Empty,
    GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList, GetMempoolTxRequest,
    GetSubtreeRootsArg, LIGHTWALLETD_PROTOCOL_VERSION, LightdInfo, PingResponse, PoolType,
    RawTransaction, SendResponse, StandardFee, SubtreeRoot, TransparentAddressBlockFilter,
    TreeState, TxFilter, TxOut,
    compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer},
};

type Replies<Reply> = BoxStream<'static, Result<Reply, Status>>;

/// What a block-range stream sends after the requested blocks.
#[derive(Clone, Copy)]
pub(super) enum RangeEnding {
    /// Nothing: the stream ends with an OK status.
    Clean,
    /// Return OK without sending a block.
    Empty,
    /// One more block, the height after the requested end.
    ExtraBlock,
    /// A failed status.
    Status(Code),
}

/// Tree-state requests the server holds, now and at most.
#[derive(Default)]
pub(super) struct HeldRequests {
    counts: Mutex<(u64, u64)>,
}

impl HeldRequests {
    /// The most tree-state requests the server held at once.
    pub(super) fn peak(&self) -> u64 {
        self.counts.lock().1
    }

    fn enter(&self) {
        let mut counts = self.counts.lock();
        counts.0 += 1;
        counts.1 = counts.1.max(counts.0);
    }

    fn leave(&self) {
        let mut counts = self.counts.lock();
        counts.0 = counts.0.saturating_sub(1);
    }
}

/// Requests the server received, in arrival order.
#[derive(Default)]
pub(super) struct RequestLog {
    /// `GetTreeState` heights.
    pub(super) tree_state_heights: Mutex<Vec<u64>>,
    /// `GetTransaction` txids.
    pub(super) transaction_txids: Mutex<Vec<Vec<u8>>>,
    /// `GetBlockRange` `poolTypes`.
    pub(super) block_range_pool_types: Mutex<Vec<Vec<i32>>>,
    pub(super) block_ranges: Mutex<Vec<(u64, u64)>>,
    pub(super) transparent_history_ranges: Mutex<Vec<(u64, u64)>>,
    /// `GetMempoolTx` `poolTypes`.
    pub(super) mempool_pool_types: Mutex<Vec<Vec<i32>>>,
    /// `GetSubtreeRoots` `shieldedProtocol` values.
    pub(super) subtree_root_protocols: Mutex<Vec<i32>>,
    pub(super) subtree_root_requests: Mutex<Vec<(i32, u32, u32)>>,
    pub(super) mempool_streams: AtomicU64,
    pub(super) connections: Mutex<BTreeSet<SocketAddr>>,
}

impl RequestLog {
    fn record_connection<T>(&self, request: &Request<T>) {
        if let Some(address) = request.remote_addr() {
            self.connections.lock().insert(address);
        }
    }
}

/// The `chainMetadata` `GetBlock` sends with each block.
#[derive(Clone, Copy)]
pub(super) enum BlockChainMetadata {
    /// Commitment tree sizes whose completed subtrees match the stored
    /// subtree roots exactly.
    FromStoredRoots,
    /// These sizes, whatever the stored roots.
    Sizes(ChainMetadata),
    /// No `chainMetadata`.
    Absent,
}

/// The server's behaviour.
#[derive(Clone)]
pub(super) struct MockStreamer {
    pub(super) range_ending: RangeEnding,
    /// Return `DEADLINE_EXCEEDED` after this many blocks of the first range.
    pub(super) first_range_deadline_after: Option<u64>,
    /// Enable the fixed-tip Mainnet fixture with this Sapling frontier at every requested height.
    pub(super) mainnet_sapling_frontier: Option<CommitmentTreeFrontier>,
    /// How long each tree-state request is held before it is answered.
    pub(super) tree_state_delay: Duration,
    pub(super) held_tree_states: Arc<HeldRequests>,
    /// Reported chain tip for `GetLightdInfo` / `GetLatestBlock`.
    pub(super) tip_height: u64,
    /// `GetLatestBlock` calls, from the first, whose answer is one block
    /// above the previous call's, as if a block arrived between them.
    pub(super) tip_advancing_reads: u64,
    /// Override the block hash or status after the first `GetLatestBlock` call.
    pub(super) subsequent_tip: Option<Result<Vec<u8>, Code>>,
    /// `GetLatestBlock` calls answered so far.
    pub(super) latest_block_reads: Arc<AtomicU64>,
    /// Heights whose blocks include a compact Sapling output and advance the
    /// Sapling frontier; other heights repeat the frontier below them.
    pub(super) shielded_heights: BTreeSet<u64>,
    /// Heights whose blocks include a compact Ironwood action and advance the
    /// Ironwood frontier.
    pub(super) ironwood_heights: BTreeSet<u64>,
    /// Heights whose blocks include a transparent-only transaction, sent
    /// only when the request selects the transparent pool. Its txid is
    /// [`transparent_txid`].
    pub(super) transparent_heights: BTreeSet<u64>,
    /// Stored subtree roots keyed by `ShieldedProtocol` (`0` = Sapling,
    /// `1` = Orchard, `2` = Ironwood).
    pub(super) subtree_roots: BTreeMap<i32, Vec<SubtreeRoot>>,
    /// Simulated silent cap when `GetSubtreeRoots.max_entries == 0`.
    pub(super) subtree_root_zero_cap: Option<u32>,
    /// Roots a `GetSubtreeRoots` stream sends before it fails with
    /// `INTERNAL`, or `None` to end every stream cleanly.
    pub(super) subtree_root_failure_after: Option<usize>,
    /// Whether a `GetSubtreeRoots` stream with a nonzero `max_entries` fails
    /// with `INTERNAL` after its page.
    pub(super) subtree_root_page_failure: bool,
    /// Whether a `GetSubtreeRoots` stream sends distinct roots without end,
    /// pausing briefly between them.
    pub(super) subtree_root_endless: bool,
    /// Whether a `GetMempoolTx` stream sends distinct transactions without
    /// end, pausing briefly between them.
    pub(super) mempool_endless: bool,
    /// The `chainMetadata` of every block `GetBlock` returns.
    pub(super) block_chain_metadata: BlockChainMetadata,
    /// `GetBlock` calls, from the first, answered with `NOT_FOUND`, as if
    /// the block was rolled back.
    pub(super) block_not_found_reads: u64,
    /// `GetBlock` calls answered so far.
    pub(super) block_reads: Arc<AtomicU64>,
    /// The hash every `GetBlock` block carries, or `None` for the hash
    /// `GetLatestBlock` reports at that height.
    pub(super) block_hash: Option<Vec<u8>>,
    /// `LightdInfo.lightwalletProtocolVersion`, empty for a server that
    /// predates pool filtering. Such a server ignores `poolTypes`.
    pub(super) lightwallet_protocol_version: String,
    pub(super) requests: Arc<RequestLog>,
    /// Transactions served by `GetTransaction`, keyed by txid bytes.
    pub(super) transactions: BTreeMap<Vec<u8>, RawTransaction>,
    /// Mempool txids streamed by `GetMempoolTx`.
    pub(super) mempool_txids: Vec<Vec<u8>>,
    /// Responses served by M3 transparent RPCs.
    pub(super) transparent_txs: Vec<RawTransaction>,
    pub(super) transparent_balance: i64,
    pub(super) transparent_utxos: Vec<GetAddressUtxosReply>,
}

impl MockStreamer {
    pub(super) fn new(range_ending: RangeEnding) -> Self {
        Self {
            range_ending,
            first_range_deadline_after: None,
            mainnet_sapling_frontier: None,
            tree_state_delay: Duration::ZERO,
            held_tree_states: Arc::default(),
            tip_height: 250,
            tip_advancing_reads: 0,
            latest_block_reads: Arc::default(),
            subsequent_tip: None,
            shielded_heights: BTreeSet::new(),
            ironwood_heights: BTreeSet::new(),
            transparent_heights: BTreeSet::new(),
            subtree_roots: BTreeMap::new(),
            subtree_root_zero_cap: None,
            subtree_root_failure_after: None,
            subtree_root_page_failure: false,
            subtree_root_endless: false,
            mempool_endless: false,
            block_chain_metadata: BlockChainMetadata::FromStoredRoots,
            block_not_found_reads: 0,
            block_reads: Arc::default(),
            block_hash: None,
            lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
            requests: Arc::default(),
            transactions: BTreeMap::new(),
            mempool_txids: Vec::new(),
            transparent_txs: Vec::new(),
            transparent_balance: 0,
            transparent_utxos: Vec::new(),
        }
    }

    fn range_block(&self, height: u64) -> CompactBlock {
        if let Some(frontier) = &self.mainnet_sapling_frontier {
            return CompactBlock {
                height,
                hash: tip_hash(height),
                prev_hash: tip_hash(height.saturating_sub(1)),
                time: 1_760_000_000,
                chain_metadata: Some(ChainMetadata {
                    sapling_commitment_tree_size: frontier.tree_size(),
                    ..ChainMetadata::default()
                }),
                ..CompactBlock::default()
            };
        }
        block(height)
    }
}

/// A running mock server, stopped when dropped.
pub(super) struct RunningMock {
    pub(super) url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for RunningMock {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// Serves `streamer` on a local port.
pub(super) async fn serve(streamer: MockStreamer) -> Result<RunningMock, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let (shutdown, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(
        Server::builder()
            .add_service(CompactTxStreamerServer::new(streamer))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            }),
    );
    Ok(RunningMock {
        url,
        shutdown: Some(shutdown),
    })
}

/// A block whose encoded size does not depend on its height below 128.
pub(super) fn block(height: u64) -> CompactBlock {
    CompactBlock {
        height,
        hash: vec![0; 32],
        ..CompactBlock::default()
    }
}

/// The hash `GetLatestBlock` reports for a tip at `height`.
pub(super) fn tip_hash(height: u64) -> Vec<u8> {
    height.to_le_bytes().repeat(4)
}

/// The txid of the transparent-only transaction at `height`.
pub(super) fn transparent_txid(height: u64) -> Vec<u8> {
    let mut txid = vec![0x7A; 32];
    txid[..8].copy_from_slice(&height.to_le_bytes());
    txid
}

/// A stream that sends `replies` and ends cleanly.
fn replay<Reply: Clone + Send + 'static>(replies: &[Reply]) -> Response<Replies<Reply>> {
    let replies = replies.iter().cloned().map(Ok).collect::<Vec<_>>();
    Response::new(stream::iter(replies).boxed())
}

/// A 32-byte value that differs for each `index`.
fn distinct_hash(index: u32) -> Vec<u8> {
    let mut hash = vec![0; 32];
    hash[..4].copy_from_slice(&index.to_be_bytes());
    hash
}

fn unimplemented<Reply>() -> Result<Response<Reply>, Status> {
    Err(Status::unimplemented("not served by the mock"))
}

#[tonic::async_trait]
impl CompactTxStreamer for MockStreamer {
    type GetBlockRangeStream = Replies<CompactBlock>;
    type GetBlockRangeNullifiersStream = Replies<CompactBlock>;
    type GetTaddressTxidsStream = Replies<RawTransaction>;
    type GetTaddressTransactionsStream = Replies<RawTransaction>;
    type GetMempoolTxStream = Replies<CompactTx>;
    type GetMempoolStreamStream = Replies<RawTransaction>;
    type GetSubtreeRootsStream = Replies<SubtreeRoot>;
    type GetAddressUtxosStreamStream = Replies<GetAddressUtxosReply>;

    async fn get_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        self.requests.record_connection(&request);
        let range = request.into_inner();
        self.requests
            .block_range_pool_types
            .lock()
            .push(range.pool_types.clone());
        let transparent = !self.lightwallet_protocol_version.is_empty()
            && range.pool_types.contains(&(PoolType::Transparent as i32));
        let height = |id: Option<BlockId>| id.map_or(0, |id| id.height);
        let (start, end) = (height(range.start), height(range.end));
        let first_range = {
            let mut ranges = self.requests.block_ranges.lock();
            ranges.push((start, end));
            ranges.len() == 1
        };
        let mut last_reply = end;
        let ending = match self.range_ending {
            RangeEnding::Clean
                if let Some(delivered) =
                    self.first_range_deadline_after.filter(|_| first_range) =>
            {
                last_reply = start.saturating_add(delivered.saturating_sub(1)).min(end);
                vec![Err(Status::deadline_exceeded("fixture stream interrupted"))]
            }
            RangeEnding::Clean => Vec::new(),
            RangeEnding::Empty => return Ok(Response::new(stream::empty().boxed())),
            RangeEnding::ExtraBlock => vec![Ok(block(end + 1))],
            RangeEnding::Status(code) => vec![Err(Status::new(code, "stream failed at its end"))],
        };
        // A start above the end streams descending, as lightwalletd does.
        let heights = if start <= end {
            (start..=last_reply).collect::<Vec<_>>()
        } else {
            (end..=start).rev().collect()
        };
        let replies = heights
            .into_iter()
            .map(|height| {
                let mut cb = self.range_block(height);
                if self.shielded_heights.contains(&height) {
                    cb.vtx.push(CompactTx {
                        txid: height.to_le_bytes().to_vec(),
                        outputs: vec![CompactSaplingOutput::default()],
                        ..CompactTx::default()
                    });
                }
                if self.ironwood_heights.contains(&height) {
                    cb.vtx.push(CompactTx {
                        txid: (height | 1 << 63).to_le_bytes().to_vec(),
                        ironwood_actions: vec![CompactOrchardAction::default()],
                        ..CompactTx::default()
                    });
                }
                if transparent && self.transparent_heights.contains(&height) {
                    cb.vtx.push(CompactTx {
                        txid: transparent_txid(height),
                        vout: vec![TxOut::default()],
                        ..CompactTx::default()
                    });
                }
                Ok(cb)
            })
            .chain(ending);
        Ok(Response::new(
            stream::iter(replies.collect::<Vec<_>>()).boxed(),
        ))
    }

    async fn get_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        self.requests.record_connection(&request);
        self.held_tree_states.enter();
        tokio::time::sleep(self.tree_state_delay).await;
        self.held_tree_states.leave();
        let height = request.into_inner().height;
        self.requests.tree_state_heights.lock().push(height);
        if let Some(frontier) = &self.mainnet_sapling_frontier {
            return Ok(Response::new(TreeState {
                network: "main".to_owned(),
                height,
                hash: hex::encode(tip_hash(height).into_iter().rev().collect::<Vec<_>>()),
                time: 1_760_000_000,
                sapling_tree: hex::encode(frontier.final_state_bytes()),
                orchard_tree: "000000".to_owned(),
                ..TreeState::default()
            }));
        }
        let frontier = |pool: &str, heights: &BTreeSet<u64>| match heights.range(..=height).count()
        {
            0 => String::new(),
            commitments => format!("{pool}-frontier-{commitments}"),
        };
        Ok(Response::new(TreeState {
            height,
            sapling_tree: frontier("sapling", &self.shielded_heights),
            ironwood_tree: frontier("ironwood", &self.ironwood_heights),
            ..TreeState::default()
        }))
    }

    async fn get_latest_block(
        &self,
        request: Request<ChainSpec>,
    ) -> Result<Response<BlockId>, Status> {
        self.requests.record_connection(&request);
        let reads = self.latest_block_reads.fetch_add(1, Ordering::Relaxed) + 1;
        let height = self.tip_height + reads.min(self.tip_advancing_reads);
        let hash = match (reads, &self.subsequent_tip) {
            (1, _) | (_, None) => tip_hash(height),
            (_, Some(Ok(hash))) => hash.clone(),
            (_, Some(Err(code))) => return Err(Status::new(*code, "tip probe failed")),
        };
        Ok(Response::new(BlockId { height, hash }))
    }

    async fn get_block(&self, request: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        let reads = self.block_reads.fetch_add(1, Ordering::Relaxed) + 1;
        if reads <= self.block_not_found_reads {
            return Err(Status::not_found("block rolled back"));
        }
        let height = request.into_inner().height;
        let stored_size = |protocol: i32| {
            let roots = self.subtree_roots.get(&protocol).map_or(0, Vec::len);
            u32::try_from(roots)
                .unwrap_or(u32::MAX)
                .saturating_mul(1 << 16)
        };
        let chain_metadata = match self.block_chain_metadata {
            BlockChainMetadata::FromStoredRoots => Some(ChainMetadata {
                sapling_commitment_tree_size: stored_size(0),
                orchard_commitment_tree_size: stored_size(1),
                ironwood_commitment_tree_size: stored_size(2),
            }),
            BlockChainMetadata::Sizes(sizes) => Some(sizes),
            BlockChainMetadata::Absent => None,
        };
        Ok(Response::new(CompactBlock {
            chain_metadata,
            hash: self.block_hash.clone().unwrap_or_else(|| tip_hash(height)),
            ..self.range_block(height)
        }))
    }

    async fn get_block_nullifiers(
        &self,
        _: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        unimplemented()
    }

    async fn get_block_range_nullifiers(
        &self,
        _: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        unimplemented()
    }

    async fn get_transaction(
        &self,
        request: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        let txid = request.into_inner().hash;
        self.requests.transaction_txids.lock().push(txid.clone());
        self.transactions
            .get(&txid)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| Status::not_found("transaction not found"))
    }

    async fn send_transaction(
        &self,
        _: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        unimplemented()
    }

    async fn get_taddress_txids(
        &self,
        request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        self.requests.record_connection(&request);
        if let Some(range) = request.into_inner().range {
            let start = range.start.map_or(0, |start| start.height);
            let end = range.end.map_or(0, |end| end.height);
            self.requests
                .transparent_history_ranges
                .lock()
                .push((start, end));
        }
        Ok(replay(&self.transparent_txs))
    }

    async fn get_taddress_transactions(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Ok(replay(&self.transparent_txs))
    }

    async fn get_taddress_balance(
        &self,
        _: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        Ok(Response::new(Balance {
            value_zat: self.transparent_balance,
        }))
    }

    async fn get_taddress_balance_stream(
        &self,
        _: Request<Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        unimplemented()
    }

    async fn get_mempool_tx(
        &self,
        request: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        self.requests
            .mempool_pool_types
            .lock()
            .push(request.into_inner().pool_types);
        if self.mempool_endless {
            let txs = stream::unfold(0_u32, |index| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                let tx = CompactTx {
                    txid: distinct_hash(index),
                    ..CompactTx::default()
                };
                Some((Ok(tx), index.wrapping_add(1)))
            });
            return Ok(Response::new(txs.boxed()));
        }
        let mempool = self
            .mempool_txids
            .iter()
            .map(|txid| CompactTx {
                txid: txid.clone(),
                ..CompactTx::default()
            })
            .collect::<Vec<_>>();
        Ok(replay(&mempool))
    }

    async fn get_mempool_stream(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        self.requests.record_connection(&request);
        self.requests
            .mempool_streams
            .fetch_add(1, Ordering::Relaxed);
        if self.mainnet_sapling_frontier.is_some() {
            return Ok(Response::new(stream::pending().boxed()));
        }
        unimplemented()
    }

    async fn get_latest_tree_state(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        unimplemented()
    }

    async fn get_subtree_roots(
        &self,
        request: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        self.requests.record_connection(&request);
        let arg = request.into_inner();
        self.requests
            .subtree_root_protocols
            .lock()
            .push(arg.shielded_protocol);
        self.requests.subtree_root_requests.lock().push((
            arg.shielded_protocol,
            arg.start_index,
            arg.max_entries,
        ));
        if self.subtree_root_endless {
            let roots = stream::unfold(0_u32, |index| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                let root = SubtreeRoot {
                    root_hash: distinct_hash(index),
                    ..SubtreeRoot::default()
                };
                Some((Ok(root), index.wrapping_add(1)))
            });
            return Ok(Response::new(roots.boxed()));
        }
        let all = self
            .subtree_roots
            .get(&arg.shielded_protocol)
            .map_or(&[][..], Vec::as_slice);
        let start = usize::try_from(arg.start_index).unwrap_or(usize::MAX);
        let limit = if arg.max_entries == 0 {
            self.subtree_root_zero_cap
                .map_or(usize::MAX, |cap| usize::try_from(cap).unwrap_or(usize::MAX))
        } else {
            usize::try_from(arg.max_entries).unwrap_or(usize::MAX)
        };
        let page = all.iter().skip(start).take(limit).cloned().map(Ok);
        let failure = || Err(Status::internal("subtree roots failed mid-stream"));
        let replies = match self.subtree_root_failure_after {
            Some(sent) => page
                .take(sent)
                .chain(std::iter::once(failure()))
                .collect::<Vec<_>>(),
            None if self.subtree_root_page_failure && arg.max_entries > 0 => {
                page.chain(std::iter::once(failure())).collect()
            }
            None => page.collect(),
        };
        Ok(Response::new(stream::iter(replies).boxed()))
    }

    async fn get_address_utxos(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        Ok(Response::new(GetAddressUtxosReplyList {
            address_utxos: self.transparent_utxos.clone(),
        }))
    }

    async fn get_address_utxos_stream(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Ok(replay(&self.transparent_utxos))
    }

    async fn get_lightd_info(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<LightdInfo>, Status> {
        self.requests.record_connection(&request);
        let (chain_name, sapling_activation_height) = if self.mainnet_sapling_frontier.is_some() {
            ("main", 419_200)
        } else {
            ("regtest", 1)
        };
        Ok(Response::new(LightdInfo {
            vendor: "zinder-mock".to_owned(),
            version: "0.0.0-test".to_owned(),
            chain_name: chain_name.to_owned(),
            block_height: self.tip_height,
            sapling_activation_height,
            lightwallet_protocol_version: self.lightwallet_protocol_version.clone(),
            ..LightdInfo::default()
        }))
    }

    async fn get_standard_fee(&self, _: Request<Empty>) -> Result<Response<StandardFee>, Status> {
        unimplemented()
    }

    async fn ping(&self, _: Request<PingDuration>) -> Result<Response<PingResponse>, Status> {
        unimplemented()
    }
}
