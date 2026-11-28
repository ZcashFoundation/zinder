use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use http_body::Frame;
use prost::{
    Message,
    bytes::{Buf as _, BufMut, Bytes},
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

/// A stream of pre-batched lightwalletd `CompactBlock` wire pieces for an
/// uncompressed `GetBlockRange` or `GetBlockRangeNullifiers` response.
pub type EncodedBlockFrameStream = BoxStream<EncodedBlockFrame>;

/// Streamed payload returned by [`TakeMessages`] or [`TakeNullifierMessages`].
enum TakenRangeStream {
    Blocks(EncodedBlockRangeStream),
    Frames(EncodedBlockFrameStream),
}

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

    /// Returns whether [`Self::get_encoded_block_range_frames`] serves
    /// pre-batched frames on the uncompressed path.
    fn supports_encoded_frames(&self) -> bool {
        false
    }

    /// Opens a stream of pre-batched wire frames for uncompressed responses.
    async fn get_encoded_block_range_frames(
        &self,
        _request: Request<BlockRange>,
    ) -> Result<Response<EncodedBlockFrameStream>, Status> {
        Err(Status::unimplemented(
            "batched encoded block range is not implemented",
        ))
    }
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

/// Serves lightwalletd `GetBlockRangeNullifiers` from previously encoded
/// protobuf messages.
///
/// Return `Some(self)` from [`CompactTxStreamer::encoded_block_range_nullifiers`]
/// to use this implementation for nullifier range requests on the generated
/// server.
///
/// [`CompactTxStreamer::encoded_block_range_nullifiers`]: crate::compat::lightwalletd::compact_tx_streamer_server::CompactTxStreamer::encoded_block_range_nullifiers
#[tonic::async_trait]
pub trait EncodedBlockRangeNullifiers: Send + Sync {
    /// Opens a stream of encoded nullifier-only compact blocks for the
    /// requested heights and pools.
    async fn get_encoded_block_range_nullifiers(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<EncodedBlockRangeStream>, Status>;

    /// Returns whether [`Self::get_encoded_block_range_nullifier_frames`]
    /// serves pre-batched frames on the uncompressed path.
    fn supports_encoded_frames(&self) -> bool {
        false
    }

    /// Opens a stream of pre-batched nullifier wire frames for uncompressed
    /// responses.
    async fn get_encoded_block_range_nullifier_frames(
        &self,
        _request: Request<BlockRange>,
    ) -> Result<Response<EncodedBlockFrameStream>, Status> {
        Err(Status::unimplemented(
            "batched encoded nullifier block range is not implemented",
        ))
    }
}

impl<'a> ServerStreamingService<BlockRange> for &'a (dyn EncodedBlockRangeNullifiers + '_) {
    type Response = EncodedBlock;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        (*self).get_encoded_block_range_nullifiers(request)
    }
}

/// One lightwalletd `CompactBlock` protobuf message in gRPC's
/// length-prefixed form: an uncompressed flag byte, the big-endian payload
/// length, and the payload.
///
/// Unless the server is configured to compress responses, a message of at
/// least 32 KiB leaves the generated server as this buffer, without the copy
/// into tonic's encode buffer, when the allocation behind it is known, at most
/// [`BoundedChunkRetention::MAX_ALLOCATION_BYTES`], and at most twice the
/// message. Otherwise it leaves as copied pieces.
#[derive(Clone, Debug)]
pub struct EncodedBlock {
    message: Bytes,
    /// Bytes of the allocation `message` keeps alive, when known.
    allocation_len: Option<usize>,
}

impl PartialEq for EncodedBlock {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl Eq for EncodedBlock {}

/// Response extension on a `GetBlockRange` response whose body keeps, behind
/// every data chunk, an allocation of at most [`Self::MAX_ALLOCATION_BYTES`]
/// and at most twice the chunk.
///
/// A response layer may then hand slices of the chunks to the transport
/// instead of copies, because a slice held by a stalled stream pins at most
/// that much. Only this crate creates the marker, and only on the uncompressed
/// path of the generated server, where [`EncodedBlock`] messages with a
/// larger or unknown allocation are copied into pieces of their own.
#[derive(Clone, Copy, Debug)]
pub struct BoundedChunkRetention(());

impl BoundedChunkRetention {
    /// The largest allocation one data chunk of a marked response keeps alive.
    ///
    /// The largest stored `CompactBlock` in the signed Mainnet catalog through
    /// height 3,592,192 is 265,677 bytes (height 1,723,203), and no row exceeds
    /// 512 KiB. A row stored alone fills a slab of its own, so this bound lets
    /// every observed large row go out without a copy, with about twice the
    /// largest as margin. A larger row is copied in 64 KiB pieces instead.
    pub const MAX_ALLOCATION_BYTES: usize = 512 * 1024;
}

/// A buffer that is not an uncompressed gRPC length-prefixed message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid gRPC length-prefixed CompactBlock message")]
pub struct InvalidEncodedBlock;

impl EncodedBlock {
    /// Bytes before the payload: the compressed flag and the payload length.
    pub const PREFIX_LEN: usize = 5;

    /// Framed message length at or above which an uncompressed response sends
    /// the message as its own buffer rather than coalescing it into a batch.
    pub const LARGE_MESSAGE_MIN_BYTES: usize = BATCH_BYTES;

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
            allocation_len: Some(message.capacity()),
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
        Ok(Self {
            message,
            allocation_len: None,
        })
    }

    /// Accepts a length-prefixed message, as [`Self::from_length_prefixed`]
    /// does, that keeps an allocation of `allocation_len` bytes alive.
    ///
    /// The server can then hand the message to the transport without a copy
    /// when that allocation is at most
    /// [`BoundedChunkRetention::MAX_ALLOCATION_BYTES`] and at most twice the
    /// message. The caller vouches for `allocation_len`, because it bounds
    /// what a stalled client can pin.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEncodedBlock`] for an invalid message or an
    /// `allocation_len` shorter than the message.
    pub fn from_length_prefixed_in(
        message: Bytes,
        allocation_len: usize,
    ) -> Result<Self, InvalidEncodedBlock> {
        if allocation_len < message.len() {
            return Err(InvalidEncodedBlock);
        }
        let block = Self::from_length_prefixed(message)?;
        Ok(Self {
            allocation_len: Some(allocation_len),
            ..block
        })
    }

    /// Returns whether the message's known allocation fits the bound a
    /// transport may retain without copying: at most
    /// [`BoundedChunkRetention::MAX_ALLOCATION_BYTES`], and at most twice the
    /// message, so a slice never pins much more than it carries.
    fn may_be_sliced(&self) -> bool {
        self.allocation_len.is_some_and(|len| {
            len <= BoundedChunkRetention::MAX_ALLOCATION_BYTES
                && len <= self.message.len().saturating_mul(2)
        })
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
/// tonic decodes the request and builds the response head. Unless the client
/// negotiates compression, the messages are then sent by [`EncodedBlockBody`]
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
    if negotiates_compression(request.headers(), send_compression) {
        return grpc.server_streaming(service, request).await;
    }
    let mut taken = None;
    let response = grpc
        .server_streaming(
            TakeMessages {
                service,
                taken: Some(&mut taken),
            },
            request,
        )
        .await;
    finish_encoded_body(response, taken, max_message_size)
}

/// Handles one `GetBlockRangeNullifiers` call for the generated server, as
/// [`server_streaming`] handles `GetBlockRange`.
pub(crate) async fn server_streaming_nullifiers<B>(
    grpc: &mut Grpc<EncodedBlockCodec>,
    service: &dyn EncodedBlockRangeNullifiers,
    request: http::Request<B>,
    send_compression: EnabledCompressionEncodings,
    max_message_size: Option<usize>,
) -> http::Response<Body>
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<StdError> + Send,
{
    if negotiates_compression(request.headers(), send_compression) {
        return grpc.server_streaming(service, request).await;
    }
    let mut taken = None;
    let response = grpc
        .server_streaming(
            TakeNullifierMessages {
                service,
                taken: Some(&mut taken),
            },
            request,
        )
        .await;
    finish_encoded_body(response, taken, max_message_size)
}

// Keep the fast-path check aligned with tonic's grpc-accept-encoding parser.
fn negotiates_compression(
    headers: &http::HeaderMap,
    mut enabled: EnabledCompressionEncodings,
) -> bool {
    let Some(accepts) = headers
        .get("grpc-accept-encoding")
        .and_then(|header| header.to_str().ok())
    else {
        return false;
    };
    while let Some(encoding) = enabled.pop() {
        let encoding = encoding.to_string();
        if accepts
            .split(',')
            .any(|accepted| accepted.trim() == encoding)
        {
            return true;
        }
    }
    false
}

/// Marks a response that keeps bounded allocations behind its data chunks and
/// hands its messages to [`EncodedBlockBody`].
fn finish_encoded_body(
    response: http::Response<Body>,
    taken: Option<TakenRangeStream>,
    max_message_size: Option<usize>,
) -> http::Response<Body> {
    let (mut parts, _) = match taken {
        Some(_) => response.into_parts(),
        None => return response,
    };
    parts.extensions.insert(BoundedChunkRetention(()));
    let body = match taken {
        Some(TakenRangeStream::Blocks(messages)) => {
            Body::new(EncodedBlockBody::new(messages, max_message_size))
        }
        Some(TakenRangeStream::Frames(frames)) => {
            Body::new(EncodedFrameBody::new(frames, max_message_size))
        }
        None => return http::Response::from_parts(parts, Body::empty()),
    };
    http::Response::from_parts(parts, body)
}

/// Moves `response`'s stream into `taken` and returns an empty stream for tonic.
fn hand_off_stream<S>(
    slot: Option<&mut Option<TakenRangeStream>>,
    response: Response<S>,
    wrap: fn(S) -> TakenRangeStream,
) -> Response<EncodedBlockRangeStream> {
    let (metadata, stream, extensions) = response.into_parts();
    if let Some(slot) = slot {
        *slot = Some(wrap(stream));
    }
    Response::from_parts(metadata, Box::pin(tokio_stream::empty()), extensions)
}

/// Opens the range for tonic and keeps its messages for [`EncodedBlockBody`].
struct TakeMessages<'a> {
    service: &'a dyn EncodedBlockRange,
    taken: Option<&'a mut Option<TakenRangeStream>>,
}

impl<'a> ServerStreamingService<BlockRange> for TakeMessages<'a> {
    type Response = EncodedBlock;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        let service = self.service;
        let taken = self.taken.take();
        Box::pin(async move {
            if service.supports_encoded_frames() {
                let response = service.get_encoded_block_range_frames(request).await?;
                Ok(hand_off_stream(taken, response, TakenRangeStream::Frames))
            } else {
                let response = service.get_encoded_block_range(request).await?;
                Ok(hand_off_stream(taken, response, TakenRangeStream::Blocks))
            }
        })
    }
}

/// Opens the nullifier range for tonic, as [`TakeMessages`] opens the range.
struct TakeNullifierMessages<'a> {
    service: &'a dyn EncodedBlockRangeNullifiers,
    taken: Option<&'a mut Option<TakenRangeStream>>,
}

impl<'a> ServerStreamingService<BlockRange> for TakeNullifierMessages<'a> {
    type Response = EncodedBlock;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        let service = self.service;
        let taken = self.taken.take();
        Box::pin(async move {
            if service.supports_encoded_frames() {
                let response = service
                    .get_encoded_block_range_nullifier_frames(request)
                    .await?;
                Ok(hand_off_stream(taken, response, TakenRangeStream::Frames))
            } else {
                let response = service.get_encoded_block_range_nullifiers(request).await?;
                Ok(hand_off_stream(taken, response, TakenRangeStream::Blocks))
            }
        })
    }
}

/// Messages of at least this many bytes leave the response body as their own
/// buffer.
///
/// Shorter ones are copied together up to this size, as tonic batches them,
/// because HTTP/2 writes each large DATA frame with its own system call.
/// This matches tonic's default yield threshold, so after the first frame the
/// body returns a range of only short messages in the same chunks as tonic's
/// encoder. A batch stays under twice this size, because each message added to
/// it is shorter than this.
const BATCH_BYTES: usize = 32 * 1024;

/// The largest piece a message over the retention bound is copied into.
const COPIED_PIECE_BYTES: usize = 64 * 1024;

/// The first batch is sent once it holds at least this many bytes.
///
/// A client behind a gateway that forwards whole HTTP/2 frames can decode the
/// first block only after the first DATA frame arrives, and after an idle
/// second a connection sends about 10 segments before waiting a round trip.
/// With segments of at least 1,340 bytes, as on tunnelled paths, that flight
/// carries about 13.4 KB. It also carries the response HEADERS frame, the
/// DATA frame header, any `SETTINGS` or `WINDOW_UPDATE` frames, and the gateway's
/// TLS records of about 1.2 KB with 22 bytes of overhead each, about 0.6 KB in
/// all. The first frame holds this cap plus at most one message, so 8 KiB
/// leaves room for a first-crossing message of about 4.6 KB.
const FIRST_BATCH_BYTES: usize = 8 * 1024;

/// An uncompressed gRPC server response body over encoded messages.
///
/// The stream sends the bytes and trailers tonic's server encoder would
/// send: `OK` after the last message, the stream's error status after the
/// messages before it, or `OUT_OF_RANGE` for a message over the size limit
/// after the messages already sent.
///
/// Every data chunk keeps at most [`BoundedChunkRetention::MAX_ALLOCATION_BYTES`]
/// and at most twice its own bytes alive: a batch is copied into an allocation
/// of exactly its length, a large message within both bounds is sent as its
/// own buffer, and any other large message is copied into pieces of at most
/// [`COPIED_PIECE_BYTES`].
///
/// Its first data frame differs from tonic's by design: the first batch is
/// sent at [`FIRST_BATCH_BYTES`], and the next ends where tonic's first would.
/// So when a message over the size limit follows in tonic's first batch, the
/// messages of the capped first frame have already been sent, where tonic
/// would drop them with the rest of its unsent batch.
struct EncodedBlockBody {
    messages: EncodedBlockRangeStream,
    max_message_size: Option<usize>,
    /// Short messages waiting to be copied into one batch.
    batch: Vec<Bytes>,
    batch_len: usize,
    /// The unsent part of a large message, sent after the batch.
    large: Bytes,
    /// Whether `large` is sent as copied pieces instead of as itself.
    copy_large: bool,
    /// Whether any data frame has been sent.
    sent_data: bool,
    /// Bytes of tonic's current batch already sent as a capped first frame.
    sent_of_batch: usize,
    status: Option<Status>,
    is_end_stream: bool,
}

impl EncodedBlockBody {
    fn new(messages: EncodedBlockRangeStream, max_message_size: Option<usize>) -> Self {
        Self {
            messages,
            max_message_size,
            batch: Vec::new(),
            batch_len: 0,
            large: Bytes::new(),
            copy_large: false,
            sent_data: false,
            sent_of_batch: 0,
            status: None,
            is_end_stream: false,
        }
    }

    fn take_batch(&mut self) -> Frame<Bytes> {
        // The source can yield early, so a batch's length is known only now.
        // Copying the queued messages once into an exact allocation avoids
        // both a reservation per batch and a shrink of an early one.
        self.sent_data = true;
        self.sent_of_batch = 0;
        let mut batch = Vec::with_capacity(self.batch_len);
        for message in self.batch.drain(..) {
            batch.extend_from_slice(&message);
        }
        self.batch_len = 0;
        Frame::data(Bytes::from(batch))
    }

    fn add_to_batch(&mut self, message: Bytes) {
        self.batch_len += message.len();
        self.batch.push(message);
    }

    fn clear_batch(&mut self) {
        self.batch.clear();
        self.batch_len = 0;
    }

    fn take_large(&mut self) -> Frame<Bytes> {
        self.sent_data = true;
        self.sent_of_batch = 0;
        if !self.copy_large {
            return Frame::data(mem::take(&mut self.large));
        }
        let piece_len = self.large.len().min(COPIED_PIECE_BYTES);
        let piece = Bytes::copy_from_slice(&self.large[..piece_len]);
        if piece_len == self.large.len() {
            self.large = Bytes::new();
        } else {
            self.large.advance(piece_len);
        }
        Frame::data(piece)
    }

    /// Returns the batch once it is due, as tonic would send it, except that
    /// the first is sent early at [`FIRST_BATCH_BYTES`].
    fn take_due_batch(&mut self) -> Option<Frame<Bytes>> {
        let len = self.batch_len;
        if !self.sent_data && (FIRST_BATCH_BYTES..BATCH_BYTES).contains(&len) {
            let frame = self.take_batch();
            self.sent_of_batch = len;
            return Some(frame);
        }
        (len + self.sent_of_batch >= BATCH_BYTES).then(|| self.take_batch())
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
        if !this.large.is_empty() {
            return Poll::Ready(Some(Ok(this.take_large())));
        }
        while this.status.is_none() {
            let block = match this.messages.as_mut().poll_next(cx) {
                Poll::Pending if this.batch.is_empty() => {
                    // tonic would end its batch here, so the next starts afresh.
                    this.sent_of_batch = 0;
                    return Poll::Pending;
                }
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
                this.clear_batch();
                this.status = Some(status);
                break;
            }
            if block.message.len() >= BATCH_BYTES {
                this.copy_large = !block.may_be_sliced();
                this.large = block.message;
                if this.batch.is_empty() {
                    return Poll::Ready(Some(Ok(this.take_large())));
                }
                return Poll::Ready(Some(Ok(this.take_batch())));
            }
            this.add_to_batch(block.message);
            if let Some(frame) = this.take_due_batch() {
                return Poll::Ready(Some(Ok(frame)));
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

/// One pre-batched piece of an uncompressed `GetBlockRange` response: either a
/// coalesced batch of short gRPC messages within [`BoundedChunkRetention`] or
/// one large message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedBlockFrame {
    kind: FrameKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FrameKind {
    Batch {
        bytes: Bytes,
        max_payload_len: usize,
        open_batch: bool,
    },
    Large(EncodedBlock),
}

/// Groups `CompactBlock` payloads or [`EncodedBlock`] messages into
/// [`EncodedBlockFrame`] pieces before handing a chunk to the HTTP/2 response
/// task.
///
/// The first frame of a stream's first chunk flushes at `8 KiB` and later
/// frames flush at `32 KiB`, while messages of at least
/// [`EncodedBlock::LARGE_MESSAGE_MIN_BYTES`] (`32 KiB`) leave as their own
/// buffer. Every emitted buffer satisfies [`BoundedChunkRetention`].
#[derive(Debug)]
pub struct EncodedBlockBatcher {
    frames: Vec<EncodedBlockFrame>,
    staged: Vec<u8>,
    max_payload_len: usize,
    sent_data: bool,
    sent_of_batch: usize,
}

impl EncodedBlockBatcher {
    /// Starts batching one chunk of a range stream.
    ///
    /// Pass `is_first_chunk = true` for the stream's opening chunk so its
    /// initial frame flushes at `8 KiB`.
    #[must_use]
    pub const fn new(is_first_chunk: bool) -> Self {
        Self {
            frames: Vec::new(),
            staged: Vec::new(),
            max_payload_len: 0,
            sent_data: !is_first_chunk,
            sent_of_batch: 0,
        }
    }

    /// Appends one raw `CompactBlock` protobuf payload (without the 5-byte
    /// gRPC prefix) in wire order.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEncodedBlock`] when `payload` exceeds the 4 GiB gRPC
    /// length field.
    pub fn push_payload(&mut self, payload: &[u8]) -> Result<(), InvalidEncodedBlock> {
        let prefix = message_prefix(payload.len())?;
        let framed_len = EncodedBlock::PREFIX_LEN + payload.len();
        if framed_len >= BATCH_BYTES {
            self.flush_staged(true);
            let mut buf = Vec::with_capacity(framed_len);
            buf.extend_from_slice(&prefix);
            buf.extend_from_slice(payload);
            let allocation_len = buf.capacity();
            let block = EncodedBlock {
                message: Bytes::from(buf),
                allocation_len: Some(allocation_len),
            };
            self.sent_data = true;
            self.sent_of_batch = 0;
            self.frames.push(EncodedBlockFrame {
                kind: FrameKind::Large(block),
            });
            return Ok(());
        }
        self.reserve_staged(framed_len);
        self.staged.extend_from_slice(&prefix);
        self.staged.extend_from_slice(payload);
        self.max_payload_len = self.max_payload_len.max(payload.len());
        self.flush_if_due();
        Ok(())
    }

    /// Appends one already length-prefixed [`EncodedBlock`] in wire order,
    /// keeping large messages zero-copy when their allocation fits
    /// [`BoundedChunkRetention`].
    pub fn push_block(&mut self, block: EncodedBlock) {
        if block.message.len() >= BATCH_BYTES {
            self.flush_staged(true);
            self.sent_data = true;
            self.sent_of_batch = 0;
            self.frames.push(EncodedBlockFrame {
                kind: FrameKind::Large(block),
            });
            return;
        }
        let payload_len = block.payload_len();
        self.reserve_staged(block.message.len());
        self.staged.extend_from_slice(&block.message);
        self.max_payload_len = self.max_payload_len.max(payload_len);
        self.flush_if_due();
    }

    /// Finishes the chunk and returns its coalesced frames.
    #[must_use]
    pub fn finish(mut self) -> Vec<EncodedBlockFrame> {
        self.flush_staged(false);
        self.frames
    }

    fn reserve_staged(&mut self, incoming_len: usize) {
        if self.staged.capacity() == 0 {
            let target = if self.sent_data {
                BATCH_BYTES + 4 * 1024
            } else {
                FIRST_BATCH_BYTES + 1024
            };
            self.staged.reserve_exact(target.max(incoming_len));
        }
    }

    fn flush_if_due(&mut self) {
        let len = self.staged.len();
        if !self.sent_data && (FIRST_BATCH_BYTES..BATCH_BYTES).contains(&len) {
            self.flush_staged(false);
            self.sent_of_batch = len;
            return;
        }
        if len + self.sent_of_batch >= BATCH_BYTES {
            self.flush_staged(false);
        }
    }

    fn flush_staged(&mut self, open_batch: bool) {
        if self.staged.is_empty() {
            return;
        }
        self.sent_data = true;
        self.sent_of_batch = 0;
        let max_payload_len = mem::replace(&mut self.max_payload_len, 0);
        let bytes = if self.staged.capacity() <= BoundedChunkRetention::MAX_ALLOCATION_BYTES
            && self.staged.capacity() <= self.staged.len().saturating_mul(2)
        {
            Bytes::from(mem::take(&mut self.staged))
        } else {
            let copied = Bytes::copy_from_slice(&self.staged);
            self.staged.clear();
            copied
        };
        self.frames.push(EncodedBlockFrame {
            kind: FrameKind::Batch {
                bytes,
                max_payload_len,
                open_batch,
            },
        });
    }
}

fn oversized_message_status(found: usize, limit: usize) -> Status {
    Status::out_of_range(format!(
        "Error, encoded message length too large: found {found} bytes, the limit is: {limit} bytes"
    ))
}

fn first_oversized_payload(mut batch: &[u8], limit: usize) -> usize {
    while let Some((prefix, rest)) = batch.split_at_checked(EncodedBlock::PREFIX_LEN) {
        let len_bytes: [u8; 4] = prefix[1..].try_into().unwrap_or([0; 4]);
        let payload_len = usize::try_from(u32::from_be_bytes(len_bytes)).unwrap_or(usize::MAX);
        if payload_len > limit {
            return payload_len;
        }
        let Some((_, after)) = rest.split_at_checked(payload_len) else {
            break;
        };
        batch = after;
    }
    limit.saturating_add(1)
}

/// Uncompressed gRPC response body over pre-batched [`EncodedBlockFrame`] items.
struct EncodedFrameBody {
    frames: EncodedBlockFrameStream,
    max_message_size: Option<usize>,
    peeked: Option<Result<EncodedBlockFrame, Status>>,
    large: Bytes,
    copy_large: bool,
    status: Option<Status>,
    is_end_stream: bool,
}

impl EncodedFrameBody {
    fn new(frames: EncodedBlockFrameStream, max_message_size: Option<usize>) -> Self {
        Self {
            frames,
            max_message_size,
            peeked: None,
            large: Bytes::new(),
            copy_large: false,
            status: None,
            is_end_stream: false,
        }
    }

    fn poll_next_item(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<EncodedBlockFrame, Status>>> {
        if let Some(buffered) = self.peeked.take() {
            return Poll::Ready(Some(buffered));
        }
        self.frames.as_mut().poll_next(cx)
    }

    fn take_large(&mut self) -> Frame<Bytes> {
        if !self.copy_large {
            return Frame::data(mem::take(&mut self.large));
        }
        let piece_len = self.large.len().min(COPIED_PIECE_BYTES);
        let piece = Bytes::copy_from_slice(&self.large[..piece_len]);
        if piece_len == self.large.len() {
            self.large = Bytes::new();
        } else {
            self.large.advance(piece_len);
        }
        Frame::data(piece)
    }

    fn trailers(&mut self, status: &Status) -> Result<Frame<Bytes>, Status> {
        self.is_end_stream = true;
        let mut trailers = http::HeaderMap::new();
        status.add_header(&mut trailers)?;
        Ok(Frame::trailers(trailers))
    }

    fn accept_batch(
        &mut self,
        cx: &mut Context<'_>,
        bytes: Bytes,
        max_payload_len: usize,
        open_batch: bool,
    ) -> Option<Frame<Bytes>> {
        if let Some(limit) = self.max_message_size {
            if max_payload_len > limit {
                let found = first_oversized_payload(&bytes, limit);
                self.status = Some(oversized_message_status(found, limit));
                return None;
            }
            if open_batch && let Poll::Ready(Some(next_item)) = self.poll_next_item(cx) {
                if let Ok(EncodedBlockFrame {
                    kind: FrameKind::Large(ref block),
                }) = next_item
                    && block.payload_len() > limit
                {
                    self.status = Some(oversized_message_status(block.payload_len(), limit));
                    return None;
                }
                self.peeked = Some(next_item);
            }
        }
        Some(Frame::data(bytes))
    }
}

impl http_body::Body for EncodedFrameBody {
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
        if !this.large.is_empty() {
            return Poll::Ready(Some(Ok(this.take_large())));
        }
        if let Some(status) = this.status.take() {
            return Poll::Ready(Some(this.trailers(&status)));
        }
        match this.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Some(this.trailers(&Status::ok("")))),
            Poll::Ready(Some(Err(status))) => Poll::Ready(Some(this.trailers(&status))),
            Poll::Ready(Some(Ok(EncodedBlockFrame { kind }))) => match kind {
                FrameKind::Batch {
                    bytes,
                    max_payload_len,
                    open_batch,
                } => {
                    if let Some(frame) = this.accept_batch(cx, bytes, max_payload_len, open_batch) {
                        Poll::Ready(Some(Ok(frame)))
                    } else {
                        let status = this.status.take().unwrap_or_else(|| Status::ok(""));
                        Poll::Ready(Some(this.trailers(&status)))
                    }
                }
                FrameKind::Large(block) => {
                    if let Some(limit) = this.max_message_size
                        && block.payload_len() > limit
                    {
                        let status = oversized_message_status(block.payload_len(), limit);
                        return Poll::Ready(Some(this.trailers(&status)));
                    }
                    this.copy_large = !block.may_be_sliced();
                    this.large = block.message;
                    Poll::Ready(Some(Ok(this.take_large())))
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.is_end_stream
    }
}

#[cfg(test)]
mod tests;
