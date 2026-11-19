//! A `CompactTxStreamer` test server for block ranges and tree states.
//!
//! It ends its streams the way a test asks and counts the tree-state
//! requests it holds at once. Every other method is unimplemented.

use std::{error::Error, sync::Arc, time::Duration};

use futures_util::stream::{self, BoxStream, StreamExt as _};
use parking_lot::Mutex;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, Streaming, transport::Server};
use zinder_proto::compat::lightwalletd::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx,
    Duration as PingDuration, Empty, GetAddressUtxosArg, GetAddressUtxosReply,
    GetAddressUtxosReplyList, GetMempoolTxRequest, GetSubtreeRootsArg, LightdInfo, PingResponse,
    RawTransaction, SendResponse, StandardFee, SubtreeRoot, TransparentAddressBlockFilter,
    TreeState, TxFilter,
    compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer},
};

type Replies<Reply> = BoxStream<'static, Result<Reply, Status>>;

/// What a block-range stream sends after the requested blocks.
#[derive(Clone, Copy)]
pub(super) enum RangeEnding {
    /// Nothing: the stream ends with an OK status.
    Clean,
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

/// The server's behaviour.
#[derive(Clone)]
pub(super) struct MockStreamer {
    pub(super) range_ending: RangeEnding,
    /// How long each tree-state request is held before it is answered.
    pub(super) tree_state_delay: Duration,
    pub(super) held_tree_states: Arc<HeldRequests>,
}

impl MockStreamer {
    pub(super) fn new(range_ending: RangeEnding) -> Self {
        Self {
            range_ending,
            tree_state_delay: Duration::ZERO,
            held_tree_states: Arc::default(),
        }
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
        let range = request.into_inner();
        let height = |id: Option<BlockId>| id.map_or(0, |id| id.height);
        let (start, end) = (height(range.start), height(range.end));
        let ending = match self.range_ending {
            RangeEnding::Clean => Vec::new(),
            RangeEnding::ExtraBlock => vec![Ok(block(end + 1))],
            RangeEnding::Status(code) => vec![Err(Status::new(code, "stream failed at its end"))],
        };
        let replies = (start..=end).map(|height| Ok(block(height))).chain(ending);
        Ok(Response::new(
            stream::iter(replies.collect::<Vec<_>>()).boxed(),
        ))
    }

    async fn get_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        self.held_tree_states.enter();
        tokio::time::sleep(self.tree_state_delay).await;
        self.held_tree_states.leave();
        Ok(Response::new(TreeState {
            height: request.into_inner().height,
            ..TreeState::default()
        }))
    }

    async fn get_latest_block(&self, _: Request<ChainSpec>) -> Result<Response<BlockId>, Status> {
        unimplemented()
    }

    async fn get_block(&self, _: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        unimplemented()
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
        _: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        unimplemented()
    }

    async fn send_transaction(
        &self,
        _: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        unimplemented()
    }

    async fn get_taddress_txids(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        unimplemented()
    }

    async fn get_taddress_transactions(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        unimplemented()
    }

    async fn get_taddress_balance(
        &self,
        _: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        unimplemented()
    }

    async fn get_taddress_balance_stream(
        &self,
        _: Request<Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        unimplemented()
    }

    async fn get_mempool_tx(
        &self,
        _: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        unimplemented()
    }

    async fn get_mempool_stream(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
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
        _: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        unimplemented()
    }

    async fn get_address_utxos(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        unimplemented()
    }

    async fn get_address_utxos_stream(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        unimplemented()
    }

    async fn get_lightd_info(&self, _: Request<Empty>) -> Result<Response<LightdInfo>, Status> {
        unimplemented()
    }

    async fn get_standard_fee(&self, _: Request<Empty>) -> Result<Response<StandardFee>, Status> {
        unimplemented()
    }

    async fn ping(&self, _: Request<PingDuration>) -> Result<Response<PingResponse>, Status> {
        unimplemented()
    }
}
