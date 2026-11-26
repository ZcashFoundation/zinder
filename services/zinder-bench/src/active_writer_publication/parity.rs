//! Wire-level final-tip reply byte parity probes across canonical, shielded,
//! tree-state, and transparent wallet-projection gRPC endpoints.

use std::marker::PhantomData;

use prost::bytes::{Buf as _, Bytes};
use sha2::{Digest, Sha256};
use tonic::{
    Status,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::http::uri::PathAndQuery,
};
use zinder_proto::compat::lightwalletd::{
    AddressList, BlockId, BlockRange, GetAddressUtxosArg, TxFilter,
};
use zinder_runtime::AuthenticatedChannel;

use super::report::{ModeParityDigests, WriterMode, update_length_prefixed_digest};
use crate::BenchError;

const CLIENT_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const GET_BLOCK_RANGE_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange";
const GET_TREE_STATE_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTreeState";
const GET_TADDRESS_BALANCE_PATH: &str =
    "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTaddressBalance";
const GET_ADDRESS_UTXOS_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxos";
const GET_TRANSACTION_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTransaction";

/// Parameters identifying the parity probe target arm at final tip.
pub(crate) struct ParityProbeRequest<'a> {
    pub(crate) mode: WriterMode,
    pub(crate) label: &'a str,
    pub(crate) wallet_serving: bool,
    pub(crate) base_tip_height: u32,
    pub(crate) final_tip_height: u32,
    pub(crate) segment_boundary_height: u32,
    pub(crate) wallet_addresses: &'a [String],
    pub(crate) probe_txids: &'a [[u8; 32]],
}

struct RawReplyCodec<Req>(PhantomData<fn(Req)>);

impl<Req> Default for RawReplyCodec<Req> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

struct ProtoRequestEncoder<Req>(PhantomData<fn(Req)>);
struct RawBytesDecoder;

impl<Req: prost::Message> Encoder for ProtoRequestEncoder<Req> {
    type Item = Req;
    type Error = Status;

    fn encode(&mut self, request: Req, buffer: &mut EncodeBuf<'_>) -> Result<(), Status> {
        request
            .encode(buffer)
            .map_err(|err| Status::internal(err.to_string()))
    }
}

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, frame: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(frame.copy_to_bytes(frame.remaining())))
    }
}

impl<Req: prost::Message + Default + Send + Sync + 'static> Codec for RawReplyCodec<Req> {
    type Encode = Req;
    type Decode = Bytes;
    type Encoder = ProtoRequestEncoder<Req>;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProtoRequestEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

/// Probes settled catalog, boundary-crossing, tail, tree-state, and wallet
/// replies at final tip using raw protobuf frame bytes for wire-byte parity.
pub(crate) async fn collect_final_tip_parity(
    channel: AuthenticatedChannel,
    req: &ParityProbeRequest<'_>,
) -> Result<ModeParityDigests, BenchError> {
    let mut grpc = Grpc::new(channel)
        .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES);
    let settled_start = req.base_tip_height.saturating_sub(128).max(1);
    let settled_end = req.segment_boundary_height.min(req.final_tip_height);
    let boundary_start = req.segment_boundary_height.saturating_sub(6).max(1);
    let tail_start = req
        .segment_boundary_height
        .saturating_add(1)
        .min(req.final_tip_height);

    let settled_catalog_range_sha256 =
        hash_block_range(&mut grpc, settled_start, settled_end).await?;
    let boundary_crossing_range_sha256 =
        hash_block_range(&mut grpc, boundary_start, req.final_tip_height).await?;
    let uncovered_tail_range_sha256 =
        hash_block_range(&mut grpc, tail_start, req.final_tip_height).await?;
    let tree_states_sha256 = hash_tree_states(
        &mut grpc,
        &[
            settled_start,
            req.segment_boundary_height,
            req.final_tip_height,
        ],
    )
    .await?;

    let (wallet_taddress_balance_sha256, wallet_address_utxos_sha256, wallet_transactions_sha256) =
        if req.wallet_serving {
            let balance_sha = hash_taddress_balances(&mut grpc, req.wallet_addresses).await?;
            let utxos_sha = hash_address_utxos(&mut grpc, req.wallet_addresses).await?;
            let txs_sha = hash_transactions(&mut grpc, req.probe_txids).await?;
            (Some(balance_sha), Some(utxos_sha), Some(txs_sha))
        } else {
            (None, None, None)
        };

    Ok(ModeParityDigests {
        mode: req.mode,
        label: req.label.to_owned(),
        settled_catalog_range_sha256,
        boundary_crossing_range_sha256,
        uncovered_tail_range_sha256,
        tree_states_sha256,
        wallet_taddress_balance_sha256,
        wallet_address_utxos_sha256,
        wallet_transactions_sha256,
    })
}

async fn hash_block_range(
    grpc: &mut Grpc<AuthenticatedChannel>,
    start: u32,
    end: u32,
) -> Result<String, BenchError> {
    let expected_frames = if start <= end {
        u64::from(end.saturating_sub(start)).saturating_add(1)
    } else {
        u64::from(start.saturating_sub(end)).saturating_add(1)
    };
    grpc.ready()
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    let mut stream = grpc
        .server_streaming(
            tonic::Request::new(BlockRange {
                start: Some(BlockId {
                    height: u64::from(start),
                    hash: Vec::new(),
                }),
                end: Some(BlockId {
                    height: u64::from(end),
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            }),
            PathAndQuery::from_static(GET_BLOCK_RANGE_PATH),
            RawReplyCodec::<BlockRange>::default(),
        )
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
        .into_inner();

    let mut hasher = Sha256::new();
    let mut observed_frames = 0_u64;
    while let Some(raw_frame) = stream
        .message()
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
    {
        observed_frames = observed_frames.saturating_add(1);
        update_length_prefixed_digest(&mut hasher, &raw_frame);
    }
    if observed_frames != expected_frames {
        return Err(BenchError::canonical_read(format!(
            "parity GetBlockRange({start}..={end}) returned {observed_frames} frames, expected {expected_frames}"
        )));
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_tree_states(
    grpc: &mut Grpc<AuthenticatedChannel>,
    heights: &[u32],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for &height in heights {
        let raw_payload = unary_raw_bytes(
            grpc,
            BlockId {
                height: u64::from(height),
                hash: Vec::new(),
            },
            GET_TREE_STATE_PATH,
        )
        .await?;
        update_length_prefixed_digest(&mut hasher, &raw_payload);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_taddress_balances(
    grpc: &mut Grpc<AuthenticatedChannel>,
    addresses: &[String],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for addr in addresses {
        let raw = unary_raw_bytes(
            grpc,
            AddressList {
                addresses: vec![addr.clone()],
            },
            GET_TADDRESS_BALANCE_PATH,
        )
        .await?;
        update_length_prefixed_digest(&mut hasher, &raw);
    }
    let combined = unary_raw_bytes(
        grpc,
        AddressList {
            addresses: addresses.to_vec(),
        },
        GET_TADDRESS_BALANCE_PATH,
    )
    .await?;
    update_length_prefixed_digest(&mut hasher, &combined);
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_address_utxos(
    grpc: &mut Grpc<AuthenticatedChannel>,
    addresses: &[String],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for addr in addresses {
        let raw = unary_raw_bytes(
            grpc,
            GetAddressUtxosArg {
                addresses: vec![addr.clone()],
                start_height: 1,
                max_entries: 128,
            },
            GET_ADDRESS_UTXOS_PATH,
        )
        .await?;
        update_length_prefixed_digest(&mut hasher, &raw);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_transactions(
    grpc: &mut Grpc<AuthenticatedChannel>,
    txids: &[[u8; 32]],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for &txid in txids {
        let raw = unary_raw_bytes(
            grpc,
            TxFilter {
                block: None,
                index: 0,
                hash: txid.to_vec(),
            },
            GET_TRANSACTION_PATH,
        )
        .await?;
        update_length_prefixed_digest(&mut hasher, &raw);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn unary_raw_bytes<Req>(
    grpc: &mut Grpc<AuthenticatedChannel>,
    request: Req,
    path: &'static str,
) -> Result<Bytes, BenchError>
where
    Req: prost::Message + Default + Send + Sync + 'static,
{
    grpc.ready()
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    grpc.unary(
        tonic::Request::new(request),
        PathAndQuery::from_static(path),
        RawReplyCodec::<Req>::default(),
    )
    .await
    .map(tonic::Response::into_inner)
    .map_err(|status| BenchError::canonical_read(status.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use parking_lot::Mutex;
    use prost::{
        Message as _,
        bytes::{Buf as _, BufMut as _, Bytes},
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{
        Request, Response, Status,
        body::Body,
        client::Grpc,
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        server::{NamedService, ServerStreamingService, UnaryService},
        transport::Server,
    };
    use zinder_proto::compat::lightwalletd::{BlockId, BlockRange, CompactBlock, TreeState};
    use zinder_runtime::connect_zinder_grpc_with_timeout;

    use super::{CLIENT_MAX_MESSAGE_BYTES, hash_block_range, hash_tree_states};

    struct RawSendCodec<Req>(PhantomData<fn(Req)>);

    impl<Req> Default for RawSendCodec<Req> {
        fn default() -> Self {
            Self(PhantomData)
        }
    }

    struct BytesEncoder;
    struct ProtoRequestDecoder<Req>(PhantomData<fn(Req)>);

    impl Encoder for BytesEncoder {
        type Item = Bytes;
        type Error = Status;

        fn encode(&mut self, bytes: Bytes, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
            buf.put_slice(&bytes);
            Ok(())
        }
    }

    impl<Req: prost::Message + Default> Decoder for ProtoRequestDecoder<Req> {
        type Item = Req;
        type Error = Status;

        fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Req>, Status> {
            let chunk = buf.copy_to_bytes(buf.remaining());
            Req::decode(chunk)
                .map(Some)
                .map_err(|err| Status::invalid_argument(err.to_string()))
        }
    }

    impl<Req: prost::Message + Default + Send + Sync + 'static> Codec for RawSendCodec<Req> {
        type Encode = Bytes;
        type Decode = Req;
        type Encoder = BytesEncoder;
        type Decoder = ProtoRequestDecoder<Req>;

        fn encoder(&mut self) -> Self::Encoder {
            BytesEncoder
        }

        fn decoder(&mut self) -> Self::Decoder {
            ProtoRequestDecoder(PhantomData)
        }
    }

    #[derive(Clone)]
    struct FixedRawStreamer {
        block_frames: Arc<Mutex<VecDeque<Vec<Bytes>>>>,
        tree_replies: Arc<Mutex<VecDeque<Bytes>>>,
    }

    impl NamedService for FixedRawStreamer {
        const NAME: &'static str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";
    }

    struct RangeSvc(Vec<Bytes>);
    impl ServerStreamingService<BlockRange> for RangeSvc {
        type Response = Bytes;
        type ResponseStream =
            Pin<Box<dyn tokio_stream::Stream<Item = Result<Bytes, Status>> + Send>>;
        type Future = std::future::Ready<Result<Response<Self::ResponseStream>, Status>>;

        fn call(&mut self, _request: Request<BlockRange>) -> Self::Future {
            let items: Vec<Result<Bytes, Status>> = self.0.iter().cloned().map(Ok).collect();
            let stream: Self::ResponseStream = Box::pin(tokio_stream::iter(items));
            std::future::ready(Ok(Response::new(stream)))
        }
    }

    struct TreeSvc(Bytes);
    impl UnaryService<BlockId> for TreeSvc {
        type Response = Bytes;
        type Future = std::future::Ready<Result<Response<Bytes>, Status>>;

        fn call(&mut self, _request: Request<BlockId>) -> Self::Future {
            std::future::ready(Ok(Response::new(self.0.clone())))
        }
    }

    impl tower::Service<http::Request<Body>> for FixedRawStreamer {
        type Response = http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<Body>) -> Self::Future {
            let path = req.uri().path().to_owned();
            if path.ends_with("GetBlockRange") {
                let frames = self.block_frames.lock().pop_front().unwrap_or_default();
                return Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(RawSendCodec::<BlockRange>::default());
                    Ok(grpc.server_streaming(RangeSvc(frames), req).await)
                });
            }
            let tree = self.tree_replies.lock().pop_front().unwrap_or_default();
            Box::pin(async move {
                let mut tree_grpc = tonic::server::Grpc::new(RawSendCodec::<BlockId>::default());
                Ok(tree_grpc.unary(TreeSvc(tree), req).await)
            })
        }
    }

    #[tokio::test]
    async fn parity_probe_detects_wire_encoding_difference_and_truncated_stream()
    -> Result<(), Box<dyn Error>> {
        let mut canonical_order = Vec::new();
        canonical_order.push(0x08);
        canonical_order.push(5);
        canonical_order.extend_from_slice(&[0x1a, 2, 0xaa, 0xbb]);

        let mut reversed_order = Vec::new();
        reversed_order.extend_from_slice(&[0x1a, 2, 0xaa, 0xbb]);
        reversed_order.push(0x08);
        reversed_order.push(5);

        assert_eq!(
            CompactBlock::decode(canonical_order.as_slice())?,
            CompactBlock::decode(reversed_order.as_slice())?
        );

        let svc = FixedRawStreamer {
            block_frames: Arc::new(Mutex::new(VecDeque::from([
                vec![Bytes::from(canonical_order)],
                vec![Bytes::from(reversed_order)],
                Vec::new(),
            ]))),
            tree_replies: Arc::new(Mutex::new(VecDeque::from([
                Bytes::from_static(b"\x0a\x02ab\x10\x05"),
                Bytes::from_static(b"\x10\x05\x0a\x02ab"),
            ]))),
        };

        assert_eq!(
            TreeState::decode(&b"\x0a\x02ab\x10\x05"[..])?,
            TreeState::decode(&b"\x10\x05\x0a\x02ab"[..])?
        );

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });

        let channel = connect_zinder_grpc_with_timeout(
            &format!("http://{addr}"),
            None,
            Duration::from_secs(2),
        )
        .await?;
        let mut grpc = Grpc::new(channel)
            .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES);

        let digest_a = hash_block_range(&mut grpc, 5, 5).await?;
        let digest_b = hash_block_range(&mut grpc, 5, 5).await?;
        assert_ne!(digest_a, digest_b);

        let tree_a = hash_tree_states(&mut grpc, &[5]).await?;
        let tree_b = hash_tree_states(&mut grpc, &[5]).await?;
        assert_ne!(tree_a, tree_b);

        assert!(hash_block_range(&mut grpc, 5, 6).await.is_err());

        server.abort();
        Ok(())
    }
}
