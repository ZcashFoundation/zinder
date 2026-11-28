//! Engine-neutral conformance tests for stateless multi-instance wallet readers.

use std::{
    collections::BTreeMap, net::SocketAddr, num::NonZeroU32, ops::Bound, pin::Pin, sync::Arc,
    time::Duration,
};

use eyre::{ensure, eyre};
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, Request, Response, Status, service::interceptor::InterceptedService, transport::Server,
};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, CompactTransactionData, MempoolEntry, MempoolObservation,
    Network, RawTransactionBytes, TransactionId, TransparentAddressScriptHash, TransparentOutPoint,
    TransparentUnspentOutput, UnixTimestampMillis,
    wire::{
        encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex, encode_zinder_native_chain_name,
    },
};
use zinder_proto::{
    CONTRACT_REVISION,
    capabilities::{
        INGEST_CONTROL_MEMPOOL_EVENTS_V2, INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
        INGEST_CONTROL_MEMPOOL_TRANSACTION_V2, INGEST_CONTROL_SERVER_INFO_V1,
        INGEST_CONTROL_WRITER_STATUS_V1,
    },
    v1::{
        ingest::{
            self,
            ingest_control_server::{IngestControl, IngestControlServer},
        },
        ops,
        wallet::{
            self, wallet_query_client::WalletQueryClient,
            wallet_query_server::WalletQuery as WalletQueryService,
        },
    },
    wire::{chain_epoch_message, mempool_entry_message},
};
use zinder_query::{
    AdmittedIngestControl, CanonicalReader, QueryError, TransparentAddressTxIdsInRangeRequest,
    WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{
    ComposedReadiness, Readiness, ReadinessState, RuntimeService, TrafficReadinessInterceptor,
    WriterAttestation, refresh_within_stale_deadline,
};
use zinder_store::{
    ChainEvent, ChainEventStreamFamily, EventStreamStartPosition, MempoolEvent,
    MempoolEventEnvelope, MempoolEventPosition, SnapshotPageCursorAnchor, StreamCursorTokenV1,
    event_stream_start_from_message, event_stream_start_message, mempool_event_envelope_message,
};
use zinder_testkit::{
    CanonicalBaseline, ChainFixture, FixtureBlock, FixtureTransactionRows, IngestControlFixture,
    sample_regtest_upgrade_activations,
};

use super::WalletReaderHarness;

type StandaloneWalletQuery = WalletServingQuery<(), ()>;
type IngestConnectedWalletQuery = WalletServingQuery<(), AdmittedIngestControl>;

const SHARED_CURSOR_AUTH_KEY: [u8; 32] = [0x42; 32];
const FORGED_CURSOR_AUTH_KEY: [u8; 32] = [0xEE; 32];
const SCRIPT_PUB_KEY: [u8; 4] = [0x51, 0x01, 0x02, 0x03];
const MAX_REORG_BLOCKS: u32 = 100;

fn target_script_hash() -> TransparentAddressScriptHash {
    TransparentAddressScriptHash::from_bytes([0xA7; 32])
}

fn synthetic_tx_id(tag: u8, seq: u32) -> TransactionId {
    let mut bytes = [tag; 32];
    bytes[..4].copy_from_slice(&seq.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn block_at(chain: &ChainFixture, height: u32) -> eyre::Result<FixtureBlock> {
    chain
        .block_at(BlockHeight::new(height))
        .cloned()
        .ok_or_else(|| eyre!("missing fixture block at height {height}"))
}

fn block_id_at(chain: &ChainFixture, height: u32) -> eyre::Result<BlockId> {
    let block = block_at(chain, height)?;
    Ok(BlockId::new(block.height, block.hash))
}

fn attach_coinbases(
    mut chain: ChainFixture,
    from_height: u32,
    through_height: u32,
    tag: u8,
) -> eyre::Result<ChainFixture> {
    for height in from_height..=through_height {
        let block = block_at(&chain, height)?;
        let coinbase_id = synthetic_tx_id(tag, height);
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            coinbase_id,
            block.height,
            block.hash,
            format!("cb-{tag}-{height}").into_bytes(),
        ));
    }
    Ok(chain)
}

fn attach_transparent_output(
    chain: ChainFixture,
    block: &FixtureBlock,
    tx_index: u32,
    tx_id: TransactionId,
    zatoshis: u64,
) -> ChainFixture {
    let rows = FixtureTransactionRows::from_raw_transaction(
        tx_id,
        block.height,
        block.hash,
        tx_index,
        vec![0x01; 64],
    );
    let output = TransparentUnspentOutput {
        address_script_hash: target_script_hash(),
        script_pub_key: SCRIPT_PUB_KEY.to_vec(),
        outpoint: TransparentOutPoint::new(tx_id, 0),
        value_zat: zatoshis,
        block_height: block.height,
        block_hash: block.hash,
    };
    chain
        .with_transaction_rows(rows)
        .with_address_output_index(output)
}

fn open_standalone_pair<H: WalletReaderHarness>(
    harness: &mut H,
) -> eyre::Result<StandaloneWalletQuery> {
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(harness.canonical_reader()?),
        Arc::new(harness.wallet_reader()?),
    )?);
    Ok(WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(pair),
    ))
}

fn build_history_and_events_chain() -> eyre::Result<ChainFixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let mut populated = attach_coinbases(chain, 1, 4, 0x10)?;
    for height in 1_u32..=3_u32 {
        let block = block_at(&populated, height)?;
        populated = attach_transparent_output(
            populated,
            &block,
            1,
            synthetic_tx_id(0x21, height),
            10_000 + u64::from(height),
        );
    }
    Ok(populated)
}

pub(crate) async fn cross_instance_transparent_history_and_chain_event_cursors_continue_across_readers<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = build_history_and_events_chain()?;
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain)
            .through(BlockHeight::new(2))
            .with_settled_tip(BlockHeight::new(1)),
        &activations,
    )?;
    harness.append_live_block(&chain, block_id_at(&chain, 1)?)?;
    harness.append_live_block(&chain, block_id_at(&chain, 2)?)?;

    let query_a = open_standalone_pair(&mut harness)?;
    let query_b = open_standalone_pair(&mut harness)?;

    verify_transparent_history_alternating(&query_a, &query_b).await?;
    verify_chain_event_continuity(&query_a, &query_b).await?;
    Ok(())
}

async fn verify_transparent_history_alternating(
    query_a: &StandaloneWalletQuery,
    query_b: &StandaloneWalletQuery,
) -> eyre::Result<()> {
    let mut collected = Vec::new();
    let mut page_cursor = None;
    let readers = [query_a, query_b];
    for step in 0..8_usize {
        let active_reader = readers[step % readers.len()];
        let page = active_reader
            .transparent_address_tx_ids_in_range(
                TransparentAddressTxIdsInRangeRequest {
                    address_script_hash: target_script_hash(),
                    start_height: BlockHeight::new(1),
                    end_height: BlockHeight::new(4),
                    max_entries: NonZeroU32::MIN,
                    from_cursor: page_cursor.clone(),
                },
                None,
            )
            .await?;
        for artifact in page.artifacts {
            collected.push((artifact.block_height.value(), artifact.transaction_id));
        }
        let Some(resume_cursor) = page.next_cursor else {
            break;
        };
        page_cursor = Some(resume_cursor);
    }

    ensure!(
        collected.len() == 3,
        "expected 3 transparent history entries across reader instances, got {}",
        collected.len()
    );
    ensure!(
        collected.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "history across alternating readers must remain strictly ascending"
    );
    Ok(())
}

async fn verify_chain_event_continuity(
    query_a: &StandaloneWalletQuery,
    query_b: &StandaloneWalletQuery,
) -> eyre::Result<()> {
    let initial = query_a
        .chain_events(None, ChainEventStreamFamily::Visible)
        .await?;
    ensure!(
        initial.event_envelopes.len() >= 3,
        "expected baseline plus two append events, got {}",
        initial.event_envelopes.len()
    );
    let first = &initial.event_envelopes[0];
    let resolved = query_b
        .resolve_chain_events_start(
            EventStreamStartPosition::AfterCursor(first.cursor.clone()),
            ChainEventStreamFamily::Visible,
        )
        .await?;
    ensure!(
        resolved.cursor.as_ref() == Some(&first.cursor),
        "reader B must authenticate reader A's chain-event cursor"
    );
    let suffix = query_b
        .chain_events(resolved.cursor, resolved.family)
        .await?;
    ensure!(
        suffix.event_envelopes == initial.event_envelopes[1..],
        "chain-event suffix on reader B must match reader A verbatim"
    );

    let tampered = StreamCursorTokenV1::from_bytes(vec![0xFF; 12]);
    let invalid = query_b
        .chain_events(Some(tampered), ChainEventStreamFamily::Visible)
        .await;
    ensure!(
        matches!(invalid, Err(QueryError::ChainEventCursorInvalid { .. })),
        "reader B must reject tampered chain-event cursor, got {invalid:?}"
    );
    Ok(())
}

struct SharedKeyMempoolState {
    entries: BTreeMap<TransactionId, MempoolEntry>,
    envelopes: Vec<MempoolEventEnvelope>,
}

#[derive(Clone)]
struct SharedKeyMempoolHandle {
    cursor_auth_key: [u8; 32],
    chain_epoch: ChainEpoch,
    state: Arc<RwLock<SharedKeyMempoolState>>,
}

struct SharedKeyMempoolIngest {
    handle: SharedKeyMempoolHandle,
}

fn build_mempool_added_entry(
    seq: u64,
    tx_id: TransactionId,
    chain_epoch: ChainEpoch,
    cursor_auth_key: [u8; 32],
) -> eyre::Result<(MempoolEntry, MempoolEventEnvelope)> {
    let fill_byte = u8::try_from(seq & 0xFF).unwrap_or(1);
    let observed_ms = 1_700_000_000_000 + seq;
    let entry = MempoolEntry::new(
        tx_id,
        None,
        RawTransactionBytes::new(vec![fill_byte; 96]),
        CompactTransactionData::default(),
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(observed_ms),
            first_seen_chain_epoch: chain_epoch,
        },
    )?;
    let cursor =
        StreamCursorTokenV1::mempool_event(Network::ZcashRegtest, seq, tx_id, cursor_auth_key)?;
    let envelope = MempoolEventEnvelope {
        cursor,
        event_sequence: seq,
        source_observed_unix_millis: observed_ms,
        event: MempoolEvent::Added {
            entry: entry.clone(),
        },
    };
    Ok((entry, envelope))
}

impl SharedKeyMempoolHandle {
    fn create(cursor_auth_key: [u8; 32], chain_epoch: ChainEpoch) -> eyre::Result<Self> {
        let mut entries = BTreeMap::new();
        let mut envelopes = Vec::with_capacity(3);
        for index in 1_u32..=3_u32 {
            let tx_id = synthetic_tx_id(0x55, index);
            let (entry, envelope) =
                build_mempool_added_entry(u64::from(index), tx_id, chain_epoch, cursor_auth_key)?;
            entries.insert(tx_id, entry);
            envelopes.push(envelope);
        }
        Ok(Self {
            cursor_auth_key,
            chain_epoch,
            state: Arc::new(RwLock::new(SharedKeyMempoolState { entries, envelopes })),
        })
    }

    fn push_added_event(&self, tx_id: TransactionId) -> eyre::Result<u64> {
        let mut guard = self.state.write();
        let next_seq = u64::try_from(guard.envelopes.len() + 1)?;
        let (entry, envelope) =
            build_mempool_added_entry(next_seq, tx_id, self.chain_epoch, self.cursor_auth_key)?;
        guard.entries.insert(tx_id, entry);
        guard.envelopes.push(envelope);
        drop(guard);
        Ok(next_seq)
    }

    fn resolve_snapshot_start(
        &self,
        guard: &SharedKeyMempoolState,
        cursor_bytes: Vec<u8>,
    ) -> Result<(Option<TransactionId>, Option<MempoolEventPosition>), Status> {
        let current_head = guard.envelopes.last().map(MempoolEventEnvelope::position);
        let current_seq = current_head.map_or(0, |head| head.event_sequence);
        if cursor_bytes.is_empty() {
            return Ok((None, current_head));
        }
        let decoded = StreamCursorTokenV1::from_bytes(cursor_bytes)
            .decode_snapshot_page(Network::ZcashRegtest, self.cursor_auth_key)
            .map_err(|_| Status::invalid_argument("snapshot cursor rejected by shared key"))?;
        if decoded
            .events_resume_anchor
            .is_some_and(|anchor| anchor.event_sequence > current_seq)
        {
            return Err(Status::failed_precondition("snapshot cursor expired"));
        }
        if !guard.entries.contains_key(&decoded.after_transaction_id) {
            return Err(Status::invalid_argument("snapshot cursor anchor missing"));
        }
        Ok((
            Some(decoded.after_transaction_id),
            decoded.events_resume_anchor,
        ))
    }

    fn collect_snapshot_page<'a>(
        guard: &'a SharedKeyMempoolState,
        after_tx_id: Option<TransactionId>,
        limit: usize,
    ) -> (Vec<&'a MempoolEntry>, bool) {
        let iter: Box<dyn Iterator<Item = &'a MempoolEntry> + 'a> = match after_tx_id {
            None => Box::new(guard.entries.values()),
            Some(tx_id) => Box::new(
                guard
                    .entries
                    .range((Bound::Excluded(tx_id), Bound::Unbounded))
                    .map(|(_id, entry)| entry),
            ),
        };
        let mut selected: Vec<&'a MempoolEntry> = iter.take(limit.saturating_add(1)).collect();
        let has_more = selected.len() > limit;
        if has_more {
            selected.truncate(limit);
        }
        (selected, has_more)
    }

    fn build_snapshot_response(
        &self,
        req: wallet::MempoolSnapshotRequest,
    ) -> Result<wallet::MempoolSnapshotResponse, Status> {
        let guard = self.state.read();
        let (after_tx_id, events_resume_anchor) =
            self.resolve_snapshot_start(&guard, req.from_cursor)?;
        let limit = usize::try_from(req.max_entries.max(1)).unwrap_or(1);
        let (page_entries, has_more) = Self::collect_snapshot_page(&guard, after_tx_id, limit);
        let resume_cursor_bytes = events_resume_anchor
            .map(|anchor| {
                StreamCursorTokenV1::mempool_event(
                    Network::ZcashRegtest,
                    anchor.event_sequence,
                    anchor.transaction_id,
                    self.cursor_auth_key,
                )
                .map(|token| token.as_bytes().to_vec())
                .map_err(|error| Status::internal(error.to_string()))
            })
            .transpose()?
            .unwrap_or_default();
        let trailing_cursor = match (has_more, page_entries.last()) {
            (true, Some(last_entry)) => StreamCursorTokenV1::snapshot_page(
                SnapshotPageCursorAnchor {
                    network: Network::ZcashRegtest,
                    events_resume_anchor,
                    after_transaction_id: last_entry.transaction_id(),
                },
                self.cursor_auth_key,
            )
            .map_err(|error| Status::internal(error.to_string()))?
            .as_bytes()
            .to_vec(),
            _ => Vec::new(),
        };
        Ok(wallet::MempoolSnapshotResponse {
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(chain_epoch_message(self.chain_epoch)),
                ..wallet::ChainView::default()
            }),
            events_resume_cursor: resume_cursor_bytes,
            snapshot_age_millis: 1,
            entries: page_entries
                .into_iter()
                .map(mempool_entry_message)
                .collect(),
            next_cursor: trailing_cursor,
            source_tip: Some(wallet::BlockTip {
                height: self.chain_epoch.visible_tip_height.value(),
                hash: encode_rpc_block_hash_hex(self.chain_epoch.visible_tip_hash),
            }),
        })
    }
}

#[tonic::async_trait]
impl IngestControl for SharedKeyMempoolIngest {
    type VisibleChainEventsStream =
        Pin<Box<dyn Stream<Item = Result<wallet::ChainEventEnvelope, Status>> + Send>>;
    type MempoolEventsStream =
        Pin<Box<dyn Stream<Item = Result<wallet::MempoolEventEnvelope, Status>> + Send>>;

    async fn server_info(
        &self,
        _request: Request<ingest::ServerInfoRequest>,
    ) -> Result<Response<ingest::ServerInfoResponse>, Status> {
        let server_info = ops::ServerInfo {
            network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
            service_name: RuntimeService::Ingest.binary_name().to_owned(),
            service_version: "test".to_owned(),
            build_git_commit: "test".to_owned(),
            contract_revision: CONTRACT_REVISION,
            capabilities: vec![
                INGEST_CONTROL_SERVER_INFO_V1.to_owned(),
                INGEST_CONTROL_WRITER_STATUS_V1.to_owned(),
                INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3.to_owned(),
                INGEST_CONTROL_MEMPOOL_TRANSACTION_V2.to_owned(),
                INGEST_CONTROL_MEMPOOL_EVENTS_V2.to_owned(),
            ],
        };
        Ok(Response::new(ingest::ServerInfoResponse {
            server_info: Some(server_info),
        }))
    }

    async fn writer_status(
        &self,
        _request: Request<ingest::WriterStatusRequest>,
    ) -> Result<Response<ingest::WriterStatusResponse>, Status> {
        Ok(Response::new(ingest::WriterStatusResponse {
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(chain_epoch_message(self.handle.chain_epoch)),
                ..wallet::ChainView::default()
            }),
            network_name: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
            ..ingest::WriterStatusResponse::default()
        }))
    }

    async fn visible_chain_events(
        &self,
        _request: Request<wallet::EventStreamStart>,
    ) -> Result<Response<Self::VisibleChainEventsStream>, Status> {
        Err(Status::unimplemented(
            "unused in stateless reader conformance",
        ))
    }

    async fn mempool_snapshot(
        &self,
        request: Request<wallet::MempoolSnapshotRequest>,
    ) -> Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        self.handle
            .build_snapshot_response(request.into_inner())
            .map(Response::new)
    }

    async fn mempool_transaction(
        &self,
        _request: Request<ingest::MempoolTransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        Err(Status::unimplemented(
            "unused in mempool cursor conformance",
        ))
    }

    async fn mempool_events(
        &self,
        request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        let start = event_stream_start_from_message(request.into_inner().start)
            .ok_or_else(|| Status::invalid_argument("mempool events start required"))?;
        let guard = self.handle.state.read();
        let min_sequence = match start {
            EventStreamStartPosition::EarliestRetained => 0,
            EventStreamStartPosition::LiveTail => {
                guard.envelopes.last().map_or(0, |env| env.event_sequence)
            }
            EventStreamStartPosition::AfterCursor(cursor) => {
                cursor
                    .decode_mempool_event(Network::ZcashRegtest, self.handle.cursor_auth_key)
                    .map_err(|_| Status::invalid_argument("mempool event cursor rejected"))?
                    .event_sequence
            }
        };
        let encoded = guard
            .envelopes
            .iter()
            .filter(|env| env.event_sequence > min_sequence)
            .map(|env| {
                mempool_event_envelope_message(env)
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .collect::<Vec<_>>();
        drop(guard);
        Ok(Response::new(Box::pin(tokio_stream::iter(encoded))))
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        Err(Status::unimplemented(
            "unused in mempool cursor conformance",
        ))
    }
}

async fn spawn_shared_key_ingest(
    chain_epoch: ChainEpoch,
    cancel: &CancellationToken,
) -> eyre::Result<(
    String,
    SharedKeyMempoolHandle,
    tokio::task::JoinHandle<eyre::Result<()>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let handle = SharedKeyMempoolHandle::create(SHARED_CURSOR_AUTH_KEY, chain_epoch)?;
    let ingest = SharedKeyMempoolIngest {
        handle: handle.clone(),
    };
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(IngestControlServer::new(ingest))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await?;
        Ok(())
    });
    Ok((endpoint, handle, task))
}

async fn connect_adapter_replica<H: WalletReaderHarness>(
    harness: &mut H,
    ingest_endpoint: &str,
) -> eyre::Result<WalletQueryGrpcAdapter<IngestConnectedWalletQuery>> {
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(harness.canonical_reader()?),
        Arc::new(harness.wallet_reader()?),
    )?);
    let admitted =
        AdmittedIngestControl::connect(ingest_endpoint, None, Network::ZcashRegtest).await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        admitted,
    );
    Ok(WalletQueryGrpcAdapter::new(
        query,
        WalletEndpointMetadata::default(),
    ))
}

pub(crate) async fn cross_instance_mempool_snapshot_and_events_continue_with_shared_cursor_key<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = attach_coinbases(
        ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
        1,
        2,
        0x30,
    )?;
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(1)),
        &activations,
    )?;
    let published_epoch = harness.canonical_reader()?.chain_epoch();
    let cancel = CancellationToken::new();
    let (ingest_endpoint, ingest_handle, ingest_task) =
        spawn_shared_key_ingest(published_epoch, &cancel).await?;

    let adapter_a = connect_adapter_replica(&mut harness, &ingest_endpoint).await?;
    let adapter_b = connect_adapter_replica(&mut harness, &ingest_endpoint).await?;

    verify_cross_instance_mempool_paging(&adapter_a, &adapter_b, &ingest_handle).await?;
    verify_forged_mempool_cursors_rejected(&adapter_a, &adapter_b).await?;

    cancel.cancel();
    ingest_task.await??;
    Ok(())
}

async fn verify_cross_instance_mempool_paging(
    adapter_a: &WalletQueryGrpcAdapter<IngestConnectedWalletQuery>,
    adapter_b: &WalletQueryGrpcAdapter<IngestConnectedWalletQuery>,
    ingest_handle: &SharedKeyMempoolHandle,
) -> eyre::Result<()> {
    let injected_tx_id = synthetic_tx_id(0x10, 0);
    ensure!(
        injected_tx_id < synthetic_tx_id(0x55, 1),
        "injected transaction id must sort before seeded snapshot keys"
    );
    let mut snapshot_txids = Vec::new();
    let mut page_cursor = Vec::new();
    let mut resume_event_cursor = Vec::new();
    let adapters = [adapter_a, adapter_b];
    for step in 0..4_usize {
        let page = WalletQueryService::mempool_snapshot(
            adapters[step % adapters.len()],
            Request::new(wallet::MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: page_cursor,
            }),
        )
        .await?
        .into_inner();
        if step == 0 {
            resume_event_cursor = page.events_resume_cursor.clone();
            let injected_seq = ingest_handle.push_added_event(injected_tx_id)?;
            ensure!(
                injected_seq == 4,
                "expected injected sequence 4, got {injected_seq}"
            );
        } else {
            ensure!(
                page.events_resume_cursor == resume_event_cursor,
                "snapshot page {step} must preserve initial event-head anchor cursor"
            );
        }
        for entry in page.entries {
            snapshot_txids.push(entry.transaction_id);
        }
        if page.next_cursor.is_empty() {
            break;
        }
        page_cursor = page.next_cursor;
    }
    ensure!(
        snapshot_txids.len() == 3,
        "expected 3 seeded mempool snapshot entries, got {}",
        snapshot_txids.len()
    );

    let start_token = StreamCursorTokenV1::from_bytes(resume_event_cursor);
    let mut stream = WalletQueryService::mempool_events(
        adapter_b,
        Request::new(wallet::MempoolEventsRequest {
            start: Some(event_stream_start_message(
                &EventStreamStartPosition::AfterCursor(start_token),
            )),
        }),
    )
    .await?
    .into_inner();
    let mut streamed = Vec::new();
    while let Some(envelope_res) = stream.next().await {
        let envelope = envelope_res?;
        let txid = match envelope.event {
            Some(wallet::mempool_event_envelope::Event::Added(added)) => {
                added.entry.map_or_default(|entry| entry.transaction_id)
            }
            other => return Err(eyre!("expected Added mempool event, got {other:?}")),
        };
        streamed.push((envelope.event_sequence, txid));
    }
    let expected_injected_hex = encode_rpc_transaction_id_hex(injected_tx_id);
    ensure!(
        streamed == vec![(4_u64, expected_injected_hex)],
        "expected concurrent arrival [4] after initial snapshot anchor on adapter B, got {streamed:?}"
    );
    Ok(())
}

async fn verify_forged_mempool_cursors_rejected(
    adapter_a: &WalletQueryGrpcAdapter<IngestConnectedWalletQuery>,
    adapter_b: &WalletQueryGrpcAdapter<IngestConnectedWalletQuery>,
) -> eyre::Result<()> {
    let forged_snapshot = StreamCursorTokenV1::snapshot_page(
        SnapshotPageCursorAnchor {
            network: Network::ZcashRegtest,
            events_resume_anchor: None,
            after_transaction_id: synthetic_tx_id(0x55, 1),
        },
        FORGED_CURSOR_AUTH_KEY,
    )?;
    for adapter in [adapter_a, adapter_b] {
        let snapshot_err = WalletQueryService::mempool_snapshot(
            adapter,
            Request::new(wallet::MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: forged_snapshot.as_bytes().to_vec(),
            }),
        )
        .await
        .err()
        .ok_or_else(|| eyre!("forged snapshot cursor must be rejected"))?;
        ensure!(
            snapshot_err.code() == Code::InvalidArgument,
            "expected INVALID_ARGUMENT for forged snapshot cursor, got {:?}",
            snapshot_err.code()
        );
    }

    let forged_event = StreamCursorTokenV1::mempool_event(
        Network::ZcashRegtest,
        1,
        synthetic_tx_id(0x55, 1),
        FORGED_CURSOR_AUTH_KEY,
    )?;
    let events_err = WalletQueryService::mempool_events(
        adapter_b,
        Request::new(wallet::MempoolEventsRequest {
            start: Some(event_stream_start_message(
                &EventStreamStartPosition::AfterCursor(forged_event),
            )),
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("forged mempool event cursor must be rejected"))?;
    ensure!(
        events_err.code() == Code::InvalidArgument,
        "expected INVALID_ARGUMENT for forged event cursor, got {:?}",
        events_err.code()
    );
    Ok(())
}

fn build_max_depth_reorg_chains() -> eyre::Result<(ChainFixture, ChainFixture)> {
    let initial = ChainFixture::new(Network::ZcashRegtest).extend_blocks(103);
    let mut base_chain = attach_coinbases(initial, 1, 103, 0x41)?;
    let block4 = block_at(&base_chain, 4)?;
    let block103 = block_at(&base_chain, 103)?;
    base_chain = attach_transparent_output(base_chain, &block4, 1, synthetic_tx_id(0x61, 4), 400);
    base_chain =
        attach_transparent_output(base_chain, &block103, 1, synthetic_tx_id(0x61, 103), 10_300);

    let fork_base = base_chain
        .fork_at(BlockHeight::new(4))?
        .extend_blocks(MAX_REORG_BLOCKS);
    let replacement = attach_coinbases(fork_base, 4, 103, 0x71)?;
    let rep103 = block_at(&replacement, 103)?;
    let replacement =
        attach_transparent_output(replacement, &rep103, 1, synthetic_tx_id(0x72, 103), 99_000);
    Ok((base_chain, replacement))
}

pub(crate) async fn append_and_max_depth_reorg_invalidate_pinned_walks_without_mixing_forks<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let (base_chain, replacement) = build_max_depth_reorg_chains()?;
    let mut harness = H::publish(
        &CanonicalBaseline::new(&base_chain)
            .through(BlockHeight::new(102))
            .with_settled_tip(BlockHeight::new(2)),
        &activations,
    )?;
    harness.append_live_block(&base_chain, block_id_at(&base_chain, 3)?)?;

    let pre_reorg_a = open_standalone_pair(&mut harness)?;
    let pre_reorg_b = open_standalone_pair(&mut harness)?;
    let pre_tip_a = pre_reorg_a.visible_tip_block(None).await?;
    let pre_tip_b = pre_reorg_b.visible_tip_block(None).await?;
    ensure!(pre_tip_a.chain_epoch == pre_tip_b.chain_epoch);

    let first_page = pre_reorg_a
        .transparent_address_tx_ids_in_range(
            TransparentAddressTxIdsInRangeRequest {
                address_script_hash: target_script_hash(),
                start_height: BlockHeight::new(1),
                end_height: BlockHeight::new(103),
                max_entries: NonZeroU32::MIN,
                from_cursor: None,
            },
            Some(pre_tip_a.chain_epoch.id),
        )
        .await?;
    let pre_cursor = first_page
        .next_cursor
        .ok_or_else(|| eyre!("expected second transparent tx page on pre-reorg fork"))?;
    let pre_events = pre_reorg_b
        .chain_events(None, ChainEventStreamFamily::Visible)
        .await?;
    let last_pre_event = pre_events
        .event_envelopes
        .last()
        .ok_or_else(|| eyre!("expected visible chain event before reorg"))?;

    harness.replace_live_blocks(&replacement, BlockHeight::new(4))?;

    let post_reorg_a = open_standalone_pair(&mut harness)?;
    let post_reorg_b = open_standalone_pair(&mut harness)?;

    verify_pinned_walk_rejected(
        &post_reorg_a,
        &post_reorg_b,
        &pre_tip_a.chain_epoch,
        &pre_cursor,
    )
    .await?;
    verify_post_reorg_forks_consistent(
        &post_reorg_a,
        &post_reorg_b,
        &replacement,
        &pre_tip_a.chain_epoch,
        &last_pre_event.cursor,
    )
    .await?;
    Ok(())
}

async fn verify_pinned_walk_rejected(
    post_a: &StandaloneWalletQuery,
    post_b: &StandaloneWalletQuery,
    pre_epoch: &ChainEpoch,
    pre_cursor: &StreamCursorTokenV1,
) -> eyre::Result<()> {
    for reader in [post_a, post_b] {
        let pinned = reader.visible_tip_block(Some(pre_epoch.id)).await;
        ensure!(
            matches!(pinned, Err(QueryError::ChainEpochPinUnavailable { .. })),
            "pinned canonical read must fail after reorg, got {pinned:?}"
        );
        let resumed = reader
            .transparent_address_tx_ids_in_range(
                TransparentAddressTxIdsInRangeRequest {
                    address_script_hash: target_script_hash(),
                    start_height: BlockHeight::new(1),
                    end_height: BlockHeight::new(103),
                    max_entries: NonZeroU32::MIN,
                    from_cursor: Some(pre_cursor.clone()),
                },
                None,
            )
            .await;
        ensure!(
            matches!(resumed, Err(QueryError::ChainEpochPinUnavailable { .. })),
            "resuming pre-reorg cursor on post-reorg reader must fail explicitly, got {resumed:?}"
        );
    }
    Ok(())
}

async fn verify_post_reorg_forks_consistent(
    post_a: &StandaloneWalletQuery,
    post_b: &StandaloneWalletQuery,
    replacement: &ChainFixture,
    pre_epoch: &ChainEpoch,
    last_pre_cursor: &StreamCursorTokenV1,
) -> eyre::Result<()> {
    let tip_a = post_a.visible_tip_block(None).await?;
    let tip_b = post_b.visible_tip_block(None).await?;
    ensure!(tip_a.chain_epoch == tip_b.chain_epoch);
    ensure!(tip_a.chain_epoch.id > pre_epoch.id);
    let expected_tip = block_id_at(replacement, 103)?;
    ensure!(tip_a.height == expected_tip.height);
    ensure!(tip_a.block_hash == expected_tip.hash);

    let reorg_events = post_b
        .chain_events(
            Some(last_pre_cursor.clone()),
            ChainEventStreamFamily::Visible,
        )
        .await?;
    let has_full_depth_reorg = reorg_events.event_envelopes.iter().any(|envelope| {
        matches!(
            envelope.event,
            ChainEvent::ChainReorged { reverted, .. }
                if reverted.block_range.start.value() == 4
                    && reverted.block_range.end.value() == 103
                    && (reverted.block_range.end.value() - reverted.block_range.start.value() + 1)
                        == MAX_REORG_BLOCKS
        )
    });
    ensure!(
        has_full_depth_reorg,
        "expected max-depth {MAX_REORG_BLOCKS}-block ChainReorged event on post_b"
    );
    Ok(())
}

struct ReadyReplicaHandle {
    addr: SocketAddr,
    readiness: Readiness,
    composed: ComposedReadiness,
    attestation: WriterAttestation,
    task: tokio::task::JoinHandle<eyre::Result<()>>,
}

async fn spawn_gated_replica<H: WalletReaderHarness>(
    harness: &mut H,
    ingest_endpoint: &str,
    tip_height: u32,
    staleness_ceiling: Duration,
    cancel: &CancellationToken,
) -> eyre::Result<ReadyReplicaHandle> {
    let adapter = connect_adapter_replica(harness, ingest_endpoint).await?;
    let readiness = Readiness::new(ReadinessState::ready(Some(tip_height)));
    let composed = ComposedReadiness::new(readiness.clone(), &[]);
    let mut attestation = WriterAttestation::new(Network::ZcashRegtest, staleness_ceiling);
    attestation.attest();
    let gated = InterceptedService::new(
        adapter
            .with_traffic_readiness(readiness.clone())
            .into_server(),
        TrafficReadinessInterceptor::new(readiness.clone()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(gated)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await?;
        Ok(())
    });
    Ok(ReadyReplicaHandle {
        addr,
        readiness,
        composed,
        attestation,
        task,
    })
}

pub(crate) async fn stale_writer_heartbeat_fails_mempool_immediately_then_closes_gate_with_service_not_ready<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = attach_coinbases(
        ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
        1,
        2,
        0x81,
    )?;
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(1)),
        &activations,
    )?;
    let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let cancel = CancellationToken::new();
    let staleness_ceiling = Duration::from_millis(120);
    let rep_a = spawn_gated_replica(
        &mut harness,
        ingest_fixture.endpoint(),
        2,
        staleness_ceiling,
        &cancel,
    )
    .await?;
    let rep_b = spawn_gated_replica(
        &mut harness,
        ingest_fixture.endpoint(),
        2,
        staleness_ceiling,
        &cancel,
    )
    .await?;

    let mut client_a = WalletQueryClient::connect(format!("http://{}", rep_a.addr)).await?;
    let mut client_b = WalletQueryClient::connect(format!("http://{}", rep_b.addr)).await?;

    ingest_fixture.set_mempool_snapshot_error(Some(zinder_proto::status_for_reason(
        ops::ErrorReason::NodeUnavailable,
        "writer heartbeat stale",
    )));
    for rep in [&rep_a, &rep_b] {
        rep.composed
            .publish_admission(ReadinessState::serving_pair_stale(1, 0, Some(2)));
        ensure!(rep.readiness.report().is_ready);
    }

    assert_mempool_rejected_canonical_ok(&mut client_a).await?;
    assert_mempool_rejected_canonical_ok(&mut client_b).await?;

    tokio::join!(
        refresh_within_stale_deadline(
            rep_a.attestation.stale_deadline(),
            &rep_a.composed,
            tokio::time::sleep(Duration::from_millis(200)),
        ),
        refresh_within_stale_deadline(
            rep_b.attestation.stale_deadline(),
            &rep_b.composed,
            tokio::time::sleep(Duration::from_millis(200)),
        ),
    );

    assert_service_not_ready(&rep_a, &mut client_a).await?;
    assert_service_not_ready(&rep_b, &mut client_b).await?;

    cancel.cancel();
    rep_a.task.await??;
    rep_b.task.await??;
    ingest_fixture.shutdown().await?;
    Ok(())
}

async fn assert_mempool_rejected_canonical_ok(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
) -> eyre::Result<()> {
    let mempool_err = client
        .mempool_snapshot(wallet::MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: Vec::new(),
        })
        .await
        .err()
        .ok_or_else(|| eyre!("mempool_snapshot must fail immediately when writer is stale"))?;
    ensure!(
        mempool_err.code() == Code::Unavailable,
        "expected Unavailable on stale writer mempool_snapshot, got {:?}",
        mempool_err.code()
    );
    let canonical = client
        .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
        .await?
        .into_inner();
    ensure!(
        canonical
            .visible_tip_block
            .as_ref()
            .is_some_and(|block_id| block_id.height == 2)
    );
    Ok(())
}

async fn assert_service_not_ready(
    replica: &ReadyReplicaHandle,
    client: &mut WalletQueryClient<tonic::transport::Channel>,
) -> eyre::Result<()> {
    ensure!(
        !replica.readiness.report().is_ready,
        "readiness gate must close once staleness ceiling lapses"
    );
    let gate_err = client
        .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
        .await
        .err()
        .ok_or_else(|| eyre!("traffic gate must reject reads after staleness ceiling"))?;
    ensure!(gate_err.code() == Code::Unavailable);
    let details = gate_err.get_error_details();
    let error_details = details
        .error_info()
        .ok_or_else(|| eyre!("expected ErrorInfo metadata on ServiceNotReady rejection"))?;
    ensure!(
        error_details.reason == ops::ErrorReason::ServiceNotReady.as_str_name(),
        "expected SERVICE_NOT_READY, got {}",
        error_details.reason
    );
    Ok(())
}
