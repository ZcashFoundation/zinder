//! A reader one block behind another waits briefly for the block a wallet
//! learned from the other reader.
//!
//! Each test builds two readers from the same chain: the leading reader
//! serves block 9 and the lagging reader serves block 8. A wallet takes the
//! tip from the leading reader and asks the lagging reader for it, as it does
//! when a load balancer routes each request on its own.

pub(super) mod published_query;

use std::{
    future::Future,
    net::SocketAddr,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::eyre;
use futures_util::future::join_all;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt as _};
use tonic::{Code, Request, Status, codegen::Bytes};
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter, DEFAULT_TIP_WAIT_MAX_BLOCKS,
    DEFAULT_TIP_WAIT_MAX_WAITERS, LightwalletdGrpcAdapter, NodeBackedLightWallet, TipWait,
};
use zinder_core::{BlockHeight, Network};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server::CompactTxStreamer};
use zinder_query::{
    DEFAULT_TREE_STATE_REPLAY_WAIT, DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

use self::published_query::PublishedQuery;

const LAGGING_TIP: u32 = 8;
const SERVED_TIP: u32 = 9;
/// When the lagging reader publishes the block its waiting requests named.
const PUBLISH_DELAY: Duration = Duration::from_millis(300);
/// The wait the publication tests allow, long enough that a request still
/// waiting at its end shows up as a failed bound.
const PUBLICATION_WAIT: Duration = Duration::from_secs(5);
/// Every request the publication wakes must answer before this.
const ANSWERED_WITHIN: Duration = Duration::from_secs(4);
/// The wait a reader that never advances runs out.
const EXPIRING_WAIT: Duration = Duration::from_secs(1);
const HASH_MISMATCH: &str = "requested block hash does not match indexed block";

/// One request a wallet sends for the tip it learned, or for the range
/// ending at it.
#[derive(Clone, Copy, Debug)]
enum Call {
    Block,
    BlockWithHash,
    BlockNullifiers,
    BlockNullifiersWithHash,
    Range,
    DescendingRange,
    RangeNullifiers,
    EncodedRange,
    EncodedRangeNullifiers,
    TreeState,
}

impl Call {
    const fn carries_hash(self) -> bool {
        matches!(self, Self::BlockWithHash | Self::BlockNullifiersWithHash)
    }
}

/// The encoded messages a call answered with, or its status code and message.
type Outcome = Result<Vec<Vec<u8>>, (Code, String)>;

/// One block generation of a reader: its canonical and wallet readers.
struct Generation {
    canonical: Arc<RocksDbCanonicalReader>,
    wallet: Arc<RocksDbWalletReader>,
    _paths: WalletServingStoreFixture,
}

impl Generation {
    fn admit(writer: &mut CanonicalWriterFixture) -> eyre::Result<Self> {
        let mut paths = writer.serving_store_fixture()?;
        let (mut canonical, wallet) = paths.take_readers()?;
        canonical.try_catch_up()?;
        Ok(Self {
            canonical: Arc::new(RocksDbCanonicalReader::new(canonical)?),
            wallet: Arc::new(RocksDbWalletReader::new(wallet)),
            _paths: paths,
        })
    }

    fn wallet_query(&self) -> eyre::Result<WalletServingQuery<()>> {
        let pair = WalletServingReadPair::new(self.canonical.clone(), self.wallet.clone())?;
        Ok(WalletServingQuery::from_serving_pair_slot(
            WalletServingPairSlot::new(Arc::new(pair)),
        ))
    }
}

/// The block the lagging reader commits at the served tip height.
enum NextBlock {
    /// The block the leading reader serves.
    Served,
    /// A different block on the same parent.
    Sibling,
}

/// Two readers built from one chain, the lagging one a block behind.
struct Readers {
    leading: Generation,
    lagging: Generation,
    /// The lagging reader's generation at the served tip height, admitted but
    /// not yet published.
    lagging_next: Generation,
}

impl Readers {
    fn new(next_block: &NextBlock) -> eyre::Result<Self> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(SERVED_TIP);
        let mut leading_writer = lagging_writer(&chain)?;
        leading_writer.append_live_block(&chain)?;
        let leading = Generation::admit(&mut leading_writer)?;

        let mut writer = lagging_writer(&chain)?;
        let lagging = Generation::admit(&mut writer)?;
        match next_block {
            NextBlock::Served => writer.append_live_block(&chain)?,
            NextBlock::Sibling => writer.append_live_block(
                &chain
                    .fork_at(BlockHeight::new(SERVED_TIP))?
                    .extend_blocks(1),
            )?,
        };
        let lagging_next = Generation::admit(&mut writer)?;
        Ok(Self {
            leading,
            lagging,
            lagging_next,
        })
    }
}

/// A writer that published the chain through the lagging tip; its next
/// live append commits the served tip height.
fn lagging_writer(chain: &ChainFixture) -> eyre::Result<CanonicalWriterFixture> {
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain).through(BlockHeight::new(LAGGING_TIP)),
        &sample_regtest_upgrade_activations(),
    )
}

fn tip_wait(max_wait: Duration) -> TipWait {
    TipWait::new(
        max_wait,
        DEFAULT_TIP_WAIT_MAX_BLOCKS,
        DEFAULT_TIP_WAIT_MAX_WAITERS,
    )
}

/// One serving mode: how it serves a generation and publishes the next one.
trait Mode {
    type Server: CompactTxStreamer;
    type Slot: Clone + Send + Sync;

    /// The calls this mode serves.
    const CALLS: &'static [Call];

    fn slot(generation: &Generation) -> eyre::Result<Self::Slot>;

    fn server(slot: &Self::Slot, tip_wait: TipWait) -> eyre::Result<Self::Server>;

    fn publish(slot: &Self::Slot, generation: &Generation) -> eyre::Result<()>;
}

const EVERY_CALL: &[Call] = &[
    Call::Block,
    Call::BlockWithHash,
    Call::BlockNullifiers,
    Call::BlockNullifiersWithHash,
    Call::Range,
    Call::DescendingRange,
    Call::RangeNullifiers,
    Call::EncodedRange,
    Call::EncodedRangeNullifiers,
    Call::TreeState,
];

/// Compact-blocks serving, over a canonical reader slot.
struct CompactBlocks;

impl Mode for CompactBlocks {
    type Server = CompactBlocksGrpcAdapter;
    type Slot = CanonicalReaderSlot;

    const CALLS: &'static [Call] = &[
        Call::Block,
        Call::BlockWithHash,
        Call::Range,
        Call::DescendingRange,
        Call::EncodedRange,
    ];

    fn slot(generation: &Generation) -> eyre::Result<Self::Slot> {
        Ok(CanonicalReaderSlot::new(Arc::clone(&generation.canonical)))
    }

    fn server(slot: &Self::Slot, tip_wait: TipWait) -> eyre::Result<Self::Server> {
        Ok(CompactBlocksGrpcAdapter::new(slot.clone()).with_tip_wait(tip_wait))
    }

    fn publish(slot: &Self::Slot, generation: &Generation) -> eyre::Result<()> {
        let _replaced = slot.publish(Arc::clone(&generation.canonical));
        Ok(())
    }
}

/// Light-wallet serving, over a canonical reader slot. No call here
/// reaches the node.
struct LightWallet;

impl Mode for LightWallet {
    type Server = LightwalletdGrpcAdapter<NodeBackedLightWallet>;
    type Slot = CanonicalReaderSlot;

    const CALLS: &'static [Call] = EVERY_CALL;

    fn slot(generation: &Generation) -> eyre::Result<Self::Slot> {
        CompactBlocks::slot(generation)
    }

    fn server(slot: &Self::Slot, tip_wait: TipWait) -> eyre::Result<Self::Server> {
        let node = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:9",
            NodeAuth::None,
            Duration::from_secs(1),
        )?;
        Ok(LightwalletdGrpcAdapter::for_light_wallet(
            slot.clone(),
            node,
            DEFAULT_TREE_STATE_REPLAY_WAIT,
            DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
            zinder_query::ForwardedReadLimit::default(),
        )
        .with_tip_wait(tip_wait))
    }

    fn publish(slot: &Self::Slot, generation: &Generation) -> eyre::Result<()> {
        CompactBlocks::publish(slot, generation)
    }
}

/// Wallet serving, over a query whose publications the test drives.
struct Wallet;

impl Mode for Wallet {
    type Server = LightwalletdGrpcAdapter<PublishedQuery>;
    type Slot = PublishedQuery;

    const CALLS: &'static [Call] = EVERY_CALL;

    fn slot(generation: &Generation) -> eyre::Result<Self::Slot> {
        Ok(PublishedQuery::new(generation.wallet_query()?))
    }

    fn server(slot: &Self::Slot, tip_wait: TipWait) -> eyre::Result<Self::Server> {
        Ok(LightwalletdGrpcAdapter::new(slot.clone()).with_tip_wait(tip_wait))
    }

    fn publish(slot: &Self::Slot, generation: &Generation) -> eyre::Result<()> {
        slot.publish(generation.wallet_query()?);
        Ok(())
    }
}

/// The tip a wallet learns from `server`.
async fn latest_block<S: CompactTxStreamer>(server: &S) -> eyre::Result<lightwalletd::BlockId> {
    Ok(server
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner())
}

fn block_id(height: u64, hash: &[u8]) -> lightwalletd::BlockId {
    lightwalletd::BlockId {
        height,
        hash: hash.to_vec(),
    }
}

fn range(start: u64, end: u64) -> lightwalletd::BlockRange {
    lightwalletd::BlockRange {
        start: Some(block_id(start, &[])),
        end: Some(block_id(end, &[])),
        pool_types: Vec::new(),
    }
}

/// Sends `call` for `tip` to `server` and records how it answered.
async fn send<S: CompactTxStreamer>(
    server: &S,
    call: Call,
    tip: &lightwalletd::BlockId,
) -> eyre::Result<Outcome> {
    let height = tip.height;
    let below = height.saturating_sub(1);
    let outcome = match call {
        Call::Block => one(server.get_block(Request::new(block_id(height, &[]))).await),
        Call::BlockWithHash => one(server
            .get_block(Request::new(block_id(height, &tip.hash)))
            .await),
        Call::BlockNullifiers => one(server
            .get_block_nullifiers(Request::new(block_id(height, &[])))
            .await),
        Call::BlockNullifiersWithHash => one(server
            .get_block_nullifiers(Request::new(block_id(height, &tip.hash)))
            .await),
        Call::Range => {
            drain(
                server
                    .get_block_range(Request::new(range(below, height)))
                    .await,
            )
            .await
        }
        Call::DescendingRange => {
            drain(
                server
                    .get_block_range(Request::new(range(height, below)))
                    .await,
            )
            .await
        }
        Call::RangeNullifiers => {
            drain(
                server
                    .get_block_range_nullifiers(Request::new(range(below, height)))
                    .await,
            )
            .await
        }
        Call::EncodedRange => {
            let encoded = server
                .encoded_block_range()
                .ok_or_else(|| eyre!("the mode does not serve encoded block ranges"))?;
            drain_encoded(
                encoded
                    .get_encoded_block_range(Request::new(range(below, height)))
                    .await,
            )
            .await
        }
        Call::EncodedRangeNullifiers => {
            let encoded = server
                .encoded_block_range_nullifiers()
                .ok_or_else(|| eyre!("the mode does not serve encoded nullifier ranges"))?;
            drain_encoded(
                encoded
                    .get_encoded_block_range_nullifiers(Request::new(range(below, height)))
                    .await,
            )
            .await
        }
        Call::TreeState => one(server
            .get_tree_state(Request::new(block_id(height, &[])))
            .await),
    };
    Ok(outcome)
}

fn rejected(status: &Status) -> (Code, String) {
    (status.code(), status.message().to_owned())
}

fn one<M: Message>(answer: Result<tonic::Response<M>, Status>) -> Outcome {
    answer
        .map(|response| vec![response.into_inner().encode_to_vec()])
        .map_err(|status| rejected(&status))
}

async fn drain<M, St>(answer: Result<tonic::Response<St>, Status>) -> Outcome
where
    M: Message,
    St: Stream<Item = Result<M, Status>>,
{
    let stream = answer.map_err(|status| rejected(&status))?.into_inner();
    let mut stream = std::pin::pin!(stream);
    let mut messages = Vec::new();
    while let Some(message) = stream.next().await {
        messages.push(message.map_err(|status| rejected(&status))?.encode_to_vec());
    }
    Ok(messages)
}

async fn drain_encoded(
    answer: Result<tonic::Response<lightwalletd::EncodedBlockRangeStream>, Status>,
) -> Outcome {
    let mut stream = answer.map_err(|status| rejected(&status))?.into_inner();
    let mut messages = Vec::new();
    while let Some(message) = stream.next().await {
        messages.push(
            message
                .map_err(|status| rejected(&status))?
                .payload()
                .to_vec(),
        );
    }
    Ok(messages)
}

/// Runs `call` and returns how long it took to answer.
async fn timed<F: Future>(call: F) -> (F::Output, Duration) {
    let started = Instant::now();
    let answer = call.await;
    (answer, started.elapsed())
}

/// Sends every call of `M` to `server` at once and publishes `next` into
/// `slot` after `PUBLISH_DELAY`, while the calls wait.
async fn send_while_publishing<M: Mode>(
    server: &M::Server,
    tip: &lightwalletd::BlockId,
    slot: &M::Slot,
    next: &Generation,
) -> eyre::Result<Vec<(Call, Outcome, Duration)>> {
    let calls = join_all(M::CALLS.iter().map(|call| timed(send(server, *call, tip))));
    let publication = async {
        tokio::time::sleep(PUBLISH_DELAY).await;
        M::publish(slot, next)
    };
    let (answers, published) = tokio::join!(calls, publication);
    published?;
    M::CALLS
        .iter()
        .zip(answers)
        .map(|(call, (outcome, waited))| Ok((*call, outcome?, waited)))
        .collect()
}

fn assert_woken_by_the_publication(call: Call, waited: Duration) {
    assert!(
        waited >= PUBLISH_DELAY,
        "{call:?} answered after {waited:?}, before the publication at {PUBLISH_DELAY:?}"
    );
    assert!(
        waited < ANSWERED_WITHIN,
        "{call:?} answered after {waited:?}, not when the publication woke it"
    );
}

/// The lagging reader answers every call for the leading reader's tip with
/// the leading reader's bytes once it publishes that block.
async fn check_lagging_reader_serves_the_published_tip<M: Mode>() -> eyre::Result<()> {
    let readers = Readers::new(&NextBlock::Served)?;
    let leading = M::server(&M::slot(&readers.leading)?, TipWait::disabled())?;
    let lagging_slot = M::slot(&readers.lagging)?;
    let lagging = M::server(&lagging_slot, tip_wait(PUBLICATION_WAIT))?;
    let tip = latest_block(&leading).await?;
    assert_eq!(tip.height, u64::from(SERVED_TIP));

    let mut expected = Vec::new();
    for call in M::CALLS {
        let outcome = send(&leading, *call, &tip).await?;
        assert!(
            outcome.is_ok(),
            "the leading reader refused {call:?}: {outcome:?}"
        );
        expected.push(outcome);
    }
    let answers =
        send_while_publishing::<M>(&lagging, &tip, &lagging_slot, &readers.lagging_next).await?;
    for ((call, outcome, waited), expected) in answers.into_iter().zip(expected) {
        assert_eq!(outcome, expected, "{call:?}");
        assert_woken_by_the_publication(call, waited);
    }
    Ok(())
}

#[tokio::test]
async fn a_lagging_compact_blocks_reader_serves_the_tip_once_it_publishes() -> eyre::Result<()> {
    check_lagging_reader_serves_the_published_tip::<CompactBlocks>().await
}

#[tokio::test]
async fn a_lagging_light_wallet_reader_serves_the_tip_once_it_publishes() -> eyre::Result<()> {
    check_lagging_reader_serves_the_published_tip::<LightWallet>().await
}

#[tokio::test]
async fn a_lagging_wallet_reader_serves_the_tip_once_it_publishes() -> eyre::Result<()> {
    check_lagging_reader_serves_the_published_tip::<Wallet>().await
}

/// A lagging reader that adopts a sibling block at the requested height
/// serves its own block to height-only calls and refuses the leading
/// reader's hash, both as soon as it publishes.
async fn check_lagging_reader_serves_its_sibling<M: Mode>() -> eyre::Result<()> {
    let readers = Readers::new(&NextBlock::Sibling)?;
    let leading = M::server(&M::slot(&readers.leading)?, TipWait::disabled())?;
    let lagging_slot = M::slot(&readers.lagging)?;
    let lagging = M::server(&lagging_slot, tip_wait(PUBLICATION_WAIT))?;
    let lagging_now = M::server(&lagging_slot, TipWait::disabled())?;
    let tip = latest_block(&leading).await?;

    let answers =
        send_while_publishing::<M>(&lagging, &tip, &lagging_slot, &readers.lagging_next).await?;
    let sibling = latest_block(&lagging_now).await?;
    assert_eq!(sibling.height, tip.height);
    assert_ne!(
        sibling.hash, tip.hash,
        "the sibling must be a different block"
    );
    for (call, outcome, waited) in answers {
        assert_woken_by_the_publication(call, waited);
        if call.carries_hash() {
            assert_eq!(
                outcome,
                Err((Code::NotFound, HASH_MISMATCH.to_owned())),
                "{call:?}"
            );
            continue;
        }
        assert!(outcome.is_ok(), "{call:?} was refused: {outcome:?}");
        assert_eq!(outcome, send(&lagging_now, call, &tip).await?, "{call:?}");
        assert_ne!(
            outcome,
            send(&leading, call, &tip).await?,
            "{call:?} must serve the sibling, not the leading reader's block"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_lagging_compact_blocks_reader_serves_the_sibling_it_adopts() -> eyre::Result<()> {
    check_lagging_reader_serves_its_sibling::<CompactBlocks>().await
}

#[tokio::test]
async fn a_lagging_light_wallet_reader_serves_the_sibling_it_adopts() -> eyre::Result<()> {
    check_lagging_reader_serves_its_sibling::<LightWallet>().await
}

#[tokio::test]
async fn a_lagging_wallet_reader_serves_the_sibling_it_adopts() -> eyre::Result<()> {
    check_lagging_reader_serves_its_sibling::<Wallet>().await
}

/// A reader that never advances answers every call, once its wait expires,
/// exactly as it answers with the wait disabled.
async fn check_expired_wait_answers_as_without_it<M: Mode>() -> eyre::Result<()> {
    let readers = Readers::new(&NextBlock::Served)?;
    let leading = M::server(&M::slot(&readers.leading)?, TipWait::disabled())?;
    let lagging_slot = M::slot(&readers.lagging)?;
    let lagging = M::server(&lagging_slot, tip_wait(EXPIRING_WAIT))?;
    let lagging_now = M::server(&lagging_slot, TipWait::disabled())?;
    let tip = latest_block(&leading).await?;

    let answers = join_all(
        M::CALLS
            .iter()
            .map(|call| timed(send(&lagging, *call, &tip))),
    )
    .await;
    for (call, (outcome, waited)) in M::CALLS.iter().zip(answers) {
        let outcome = outcome?;
        let without_wait = send(&lagging_now, *call, &tip).await?;
        assert!(
            without_wait.is_err(),
            "{call:?} must fail without a wait: {without_wait:?}"
        );
        assert_eq!(outcome, without_wait, "{call:?}");
        assert!(
            waited >= EXPIRING_WAIT,
            "{call:?} answered after {waited:?}, before its wait expired"
        );
        if matches!(call, Call::TreeState) {
            assert_eq!(
                outcome,
                Err((
                    Code::InvalidArgument,
                    format!(
                        "tree state height {SERVED_TIP} is newer than latest block {LAGGING_TIP}"
                    )
                ))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_expired_compact_blocks_wait_answers_as_without_the_wait() -> eyre::Result<()> {
    check_expired_wait_answers_as_without_it::<CompactBlocks>().await
}

#[tokio::test]
async fn an_expired_light_wallet_wait_answers_as_without_the_wait() -> eyre::Result<()> {
    check_expired_wait_answers_as_without_it::<LightWallet>().await
}

#[tokio::test]
async fn an_expired_wallet_wait_answers_as_without_the_wait() -> eyre::Result<()> {
    check_expired_wait_answers_as_without_it::<Wallet>().await
}

/// A height further past the tip than the block window fails at once.
#[tokio::test]
async fn a_height_past_the_block_window_fails_at_once() -> eyre::Result<()> {
    let readers = Readers::new(&NextBlock::Served)?;
    let lagging = CompactBlocks::server(
        &CompactBlocks::slot(&readers.lagging)?,
        TipWait::new(
            PUBLICATION_WAIT,
            NonZeroU32::new(2).ok_or_else(|| eyre!("two is not zero"))?,
            DEFAULT_TIP_WAIT_MAX_WAITERS,
        ),
    )?;
    let three_ahead = u64::from(LAGGING_TIP + 3);
    let (outcome, waited) =
        timed(lagging.get_block(Request::new(block_id(three_ahead, &[])))).await;
    let Err(status) = outcome else {
        return Err(eyre!("a block three past the tip was served"));
    };
    assert_eq!(status.code(), Code::OutOfRange);
    assert!(
        waited < Duration::from_secs(2),
        "the refusal took {waited:?}"
    );
    Ok(())
}

/// A client deadline shorter than the wait ends the call with
/// `DEADLINE_EXCEEDED` through the public server stack, not with the
/// `OUT_OF_RANGE` the method answers after the wait.
#[tokio::test]
async fn a_client_deadline_inside_the_wait_ends_deadline_exceeded() -> eyre::Result<()> {
    let readers = Readers::new(&NextBlock::Served)?;
    let adapter = CompactBlocks::server(
        &CompactBlocks::slot(&readers.lagging)?,
        tip_wait(Duration::from_secs(3)),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        zinder_runtime::public_grpc_server(zinder_runtime::ClientAddressSource::Connection)
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(listener, async move {
                let _stopped = stopped.await;
            })
            .await
    });

    let (status, waited) = timed(get_block_with_deadline(
        address,
        &block_id(u64::from(SERVED_TIP), &[]),
        "200m",
    ))
    .await;
    assert_eq!(
        status?.map(|status| status.code()),
        Some(Code::DeadlineExceeded)
    );
    assert!(
        waited >= Duration::from_millis(200) && waited < Duration::from_secs(2),
        "the deadline ended the call after {waited:?}"
    );
    let _stopped = stop.send(());
    server.await??;
    Ok(())
}

/// Sends `GetBlock` with `grpc_timeout` on a raw HTTP/2 connection and
/// returns the status the server ends the call with.
///
/// tonic's client enforces a request timeout itself and reports `CANCELLED`
/// when it fires first, so it cannot show the server's own status.
async fn get_block_with_deadline(
    address: SocketAddr,
    block: &lightwalletd::BlockId,
    grpc_timeout: &'static str,
) -> eyre::Result<Option<Status>> {
    let io = TokioIo::new(tokio::net::TcpStream::connect(address).await?);
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(io)
        .await?;
    tokio::spawn(connection);
    let message = block.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(message.len())?.to_be_bytes());
    frame.extend_from_slice(&message);
    let request = http::Request::post(format!(
        "http://{address}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock"
    ))
    .header(http::header::CONTENT_TYPE, "application/grpc")
    .header(http::header::TE, "trailers")
    .header("grpc-timeout", grpc_timeout)
    .body(Full::new(Bytes::from(frame)))?;
    let response = sender.send_request(request).await?;
    if let Some(status) = Status::from_header_map(response.headers()) {
        return Ok(Some(status));
    }
    let trailers = response.into_body().collect().await?.trailers().cloned();
    Ok(trailers.as_ref().and_then(Status::from_header_map))
}
