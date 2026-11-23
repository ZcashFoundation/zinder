use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use http_body::Frame;
use prost::{
    Message,
    bytes::{BufMut, Bytes, BytesMut},
};
use tonic::{
    Request, Response, Status,
    body::Body,
    codec::{Codec, EnabledCompressionEncodings, EncodeBuf, Encoder},
    codegen::{BoxStream, StdError, http, tokio_stream},
    server::{Grpc, ServerStreamingService},
};
use tonic_prost::ProstDecoder;

use crate::compat::lightwalletd::{BlockRange, CompactBlock};

/// A stream of complete lightwalletd `CompactBlock` messages, each already
/// framed for gRPC.
pub type EncodedBlockRangeStream = BoxStream<EncodedBlock>;

/// Serves lightwalletd `GetBlockRange` from previously encoded protobuf messages.
///
/// Return `Some(self)` from [`CompactTxStreamer::encoded_block_range`] to
/// use this implementation for range requests on the generated server.
///
/// [`CompactTxStreamer::encoded_block_range`]: crate::compat::lightwalletd::compact_tx_streamer_server::CompactTxStreamer::encoded_block_range
#[tonic::async_trait]
pub trait EncodedBlockRange: Send + Sync {
    /// Opens a stream of encoded compact blocks for the requested heights and pools.
    async fn get_encoded_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<EncodedBlockRangeStream>, Status>;
}

impl<'a> ServerStreamingService<BlockRange> for &'a (dyn EncodedBlockRange + '_) {
    type Response = EncodedBlock;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        (*self).get_encoded_block_range(request)
    }
}

/// One lightwalletd `CompactBlock` protobuf message in gRPC's
/// length-prefixed form: an uncompressed flag byte, the big-endian payload
/// length, and the payload.
///
/// Unless the server is configured to compress responses, a message of at
/// least 32 KiB leaves the generated server as this buffer, without the copy
/// into tonic's encode buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedBlock {
    message: Bytes,
}

/// A buffer that is not an uncompressed gRPC length-prefixed message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid gRPC length-prefixed CompactBlock message")]
pub struct InvalidEncodedBlock;

impl EncodedBlock {
    /// Bytes before the payload: the compressed flag and the payload length.
    pub const PREFIX_LEN: usize = 5;

    /// Encodes `block` with its gRPC length prefix.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEncodedBlock`] when the encoded block exceeds the
    /// 4 GiB gRPC message length.
    pub fn encode(block: &CompactBlock) -> Result<Self, InvalidEncodedBlock> {
        let payload_len = block.encoded_len();
        let prefix = message_prefix(payload_len)?;
        let mut message = Vec::with_capacity(Self::PREFIX_LEN + payload_len);
        message.extend_from_slice(&prefix);
        // `encode` would measure the block again; this is how `encode_to_vec`
        // writes into a vector sized from one `encoded_len`.
        block.encode_raw(&mut message);
        Ok(Self {
            message: message.into(),
        })
    }

    /// Accepts a buffer that already holds one uncompressed gRPC
    /// length-prefixed message.
    ///
    /// This only checks the prefix against the buffer length; the caller
    /// vouches that the payload is a `CompactBlock`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEncodedBlock`] when the buffer is shorter than the
    /// prefix, is marked compressed, or has a length other than its prefix states.
    pub fn from_length_prefixed(message: Bytes) -> Result<Self, InvalidEncodedBlock> {
        let prefix = message
            .len()
            .checked_sub(Self::PREFIX_LEN)
            .map(message_prefix)
            .ok_or(InvalidEncodedBlock)??;
        if !message.starts_with(&prefix) {
            return Err(InvalidEncodedBlock);
        }
        Ok(Self { message })
    }

    /// Returns the whole message, gRPC prefix included.
    #[must_use]
    pub const fn as_length_prefixed(&self) -> &Bytes {
        &self.message
    }

    /// Returns the protobuf payload without the gRPC prefix.
    #[must_use]
    pub fn payload(&self) -> Bytes {
        self.message.slice(Self::PREFIX_LEN..)
    }

    /// Returns the length of the protobuf payload.
    #[must_use]
    pub fn payload_len(&self) -> usize {
        self.message.len() - Self::PREFIX_LEN
    }
}

fn message_prefix(
    payload_len: usize,
) -> Result<[u8; EncodedBlock::PREFIX_LEN], InvalidEncodedBlock> {
    let len = u32::try_from(payload_len).map_err(|_| InvalidEncodedBlock)?;
    let mut prefix = [0; EncodedBlock::PREFIX_LEN];
    prefix[1..].copy_from_slice(&len.to_be_bytes());
    Ok(prefix)
}

pub(crate) struct EncodedBlockCodec;

impl Codec for EncodedBlockCodec {
    type Encode = EncodedBlock;
    type Decode = BlockRange;
    type Encoder = Self;
    type Decoder = ProstDecoder<BlockRange>;

    fn encoder(&mut self) -> Self::Encoder {
        Self
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder::default()
    }
}

impl Encoder for EncodedBlockCodec {
    type Item = EncodedBlock;
    type Error = Status;

    fn encode(
        &mut self,
        block: EncodedBlock,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Status> {
        destination.put_slice(&block.message[EncodedBlock::PREFIX_LEN..]);
        Ok(())
    }
}

/// Handles one `GetBlockRange` call for the generated server.
///
/// tonic decodes the request and builds the response head. Unless the server
/// may compress responses, the messages are then sent by [`EncodedBlockBody`]
/// instead of tonic's encoder, which would copy each one into its buffer.
pub(crate) async fn server_streaming<B>(
    grpc: &mut Grpc<EncodedBlockCodec>,
    service: &dyn EncodedBlockRange,
    request: http::Request<B>,
    send_compression: EnabledCompressionEncodings,
    max_message_size: Option<usize>,
) -> http::Response<Body>
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<StdError> + Send,
{
    if !send_compression.is_empty() {
        return grpc.server_streaming(service, request).await;
    }
    let mut messages = None;
    let response = grpc
        .server_streaming(
            TakeMessages {
                service,
                messages: Some(&mut messages),
            },
            request,
        )
        .await;
    match messages {
        Some(messages) => {
            let (parts, _) = response.into_parts();
            let body = EncodedBlockBody::new(messages, max_message_size);
            http::Response::from_parts(parts, Body::new(body))
        }
        None => response,
    }
}

/// Opens the range for tonic and keeps its messages for [`EncodedBlockBody`].
struct TakeMessages<'a> {
    service: &'a dyn EncodedBlockRange,
    messages: Option<&'a mut Option<EncodedBlockRangeStream>>,
}

impl<'a> ServerStreamingService<BlockRange> for TakeMessages<'a> {
    type Response = EncodedBlock;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        let service = self.service;
        let taken = self.messages.take();
        Box::pin(async move {
            let (metadata, messages, extensions) =
                service.get_encoded_block_range(request).await?.into_parts();
            let messages = match taken {
                Some(taken) => {
                    *taken = Some(messages);
                    Box::pin(tokio_stream::empty())
                }
                None => messages,
            };
            Ok(Response::from_parts(metadata, messages, extensions))
        })
    }
}

/// Messages of at least this many bytes leave the response body as their own
/// buffer.
///
/// Shorter ones are copied together up to this size, as tonic batches them,
/// because HTTP/2 writes each large DATA frame with its own system call.
/// This matches tonic's default yield threshold, so the body returns a range
/// of only short messages in the same chunks as tonic's encoder. A batch stays
/// under twice this size, because each message added to it is shorter than this.
const BATCH_BYTES: usize = 32 * 1024;

/// An uncompressed gRPC server response body over encoded messages.
///
/// The stream sends the bytes and trailers tonic's server encoder would
/// send: `OK` after the last message, the stream's error status after the
/// messages before it, or `OUT_OF_RANGE` for a message over the size limit
/// after the messages already sent.
struct EncodedBlockBody {
    messages: EncodedBlockRangeStream,
    max_message_size: Option<usize>,
    batch: BytesMut,
    after_batch: Option<Bytes>,
    status: Option<Status>,
    is_end_stream: bool,
}

impl EncodedBlockBody {
    fn new(messages: EncodedBlockRangeStream, max_message_size: Option<usize>) -> Self {
        Self {
            messages,
            max_message_size,
            batch: BytesMut::new(),
            after_batch: None,
            status: None,
            is_end_stream: false,
        }
    }

    fn take_batch(&mut self) -> Frame<Bytes> {
        Frame::data(self.batch.split().freeze())
    }

    fn check_size(&self, block: &EncodedBlock) -> Result<(), Status> {
        let len = block.payload_len();
        match self.max_message_size {
            Some(limit) if len > limit => Err(Status::out_of_range(format!(
                "Error, encoded message length too large: found {len} bytes, the limit is: {limit} bytes"
            ))),
            _ => Ok(()),
        }
    }

    fn trailers(&mut self, status: &Status) -> Result<Frame<Bytes>, Status> {
        self.is_end_stream = true;
        let mut trailers = http::HeaderMap::new();
        status.add_header(&mut trailers)?;
        Ok(Frame::trailers(trailers))
    }
}

impl http_body::Body for EncodedBlockBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        if this.is_end_stream {
            return Poll::Ready(None);
        }
        if let Some(message) = this.after_batch.take() {
            return Poll::Ready(Some(Ok(Frame::data(message))));
        }
        while this.status.is_none() {
            let block = match this.messages.as_mut().poll_next(cx) {
                Poll::Pending if this.batch.is_empty() => return Poll::Pending,
                Poll::Pending => return Poll::Ready(Some(Ok(this.take_batch()))),
                Poll::Ready(None) => {
                    this.status = Some(Status::ok(""));
                    break;
                }
                Poll::Ready(Some(Err(status))) => {
                    this.status = Some(status);
                    break;
                }
                Poll::Ready(Some(Ok(block))) => block,
            };
            if let Err(status) = this.check_size(&block) {
                // tonic drops the messages it has batched but not yet sent
                // when one fails to encode.
                this.batch.clear();
                this.status = Some(status);
                break;
            }
            if block.message.len() >= BATCH_BYTES {
                if this.batch.is_empty() {
                    return Poll::Ready(Some(Ok(Frame::data(block.message))));
                }
                this.after_batch = Some(block.message);
                return Poll::Ready(Some(Ok(this.take_batch())));
            }
            this.batch.extend_from_slice(&block.message);
            if this.batch.len() >= BATCH_BYTES {
                return Poll::Ready(Some(Ok(this.take_batch())));
            }
        }
        if !this.batch.is_empty() {
            return Poll::Ready(Some(Ok(this.take_batch())));
        }
        let status = this.status.take().unwrap_or_else(|| Status::ok(""));
        Poll::Ready(Some(this.trailers(&status)))
    }

    fn is_end_stream(&self) -> bool {
        self.is_end_stream
    }
}

#[cfg(test)]
mod tests;
