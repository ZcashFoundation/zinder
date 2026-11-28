//! Element-count bounds on repeated request fields, enforced before decoding.
//!
//! tonic decodes a whole request message before the handler can check how many
//! elements a repeated field holds. An empty string or message element takes
//! 2 bytes on the wire and at least 24 bytes once decoded, and a packed enum
//! takes 1 byte on the wire and 4 once decoded, so one request at the
//! [`MAX_DECODING_MESSAGE_BYTES`](crate::MAX_DECODING_MESSAGE_BYTES) frame cap
//! can cost tens of MiB before a handler cap refuses it.
//!
//! [`RepeatedFieldGuard`] wraps a generated tonic server. For each method it
//! names, it scans every request frame with an allocation-free protobuf wire
//! walk and refuses the request with `INVALID_ARGUMENT` when a bounded field
//! holds more elements than allowed. Accepted compressed messages are
//! reframed as identity before tonic decodes the protobuf.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::{
    Status,
    body::Body as TonicBody,
    codegen::{Service, http},
    server::NamedService,
};

use crate::MAX_DECODING_MESSAGE_BYTES;

/// Length of the gRPC message prefix: one compression flag byte and a
/// big-endian `u32` message length.
const GRPC_FRAME_HEADER_BYTES: usize = 5;

/// How the elements of a bounded repeated field are encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepeatedElementEncoding {
    /// `string`, `bytes`, or message elements: one length-delimited record
    /// per element.
    LengthDelimited,
    /// Enum, integer, or `bool` elements: one varint per element, either as
    /// separate records or packed into one length-delimited record.
    Varint,
}

/// The most elements a request may carry in one repeated field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepeatedFieldLimit {
    field_name: &'static str,
    field_path: &'static [u32],
    encoding: RepeatedElementEncoding,
    max_elements: usize,
}

impl RepeatedFieldLimit {
    /// Bounds a repeated `string`, `bytes`, or message field.
    ///
    /// `field_path` holds the field numbers from the request message down to
    /// the repeated field. Every number but the last names a singular message
    /// field. `field_name` is the dotted protobuf field name used in the
    /// refusal message.
    #[must_use]
    pub const fn length_delimited(
        field_name: &'static str,
        field_path: &'static [u32],
        max_elements: usize,
    ) -> Self {
        Self {
            field_name,
            field_path,
            encoding: RepeatedElementEncoding::LengthDelimited,
            max_elements,
        }
    }

    /// Bounds a repeated enum, integer, or `bool` field, counting packed and
    /// unpacked elements alike.
    ///
    /// `field_path` and `field_name` follow
    /// [`RepeatedFieldLimit::length_delimited`].
    #[must_use]
    pub const fn varint(
        field_name: &'static str,
        field_path: &'static [u32],
        max_elements: usize,
    ) -> Self {
        Self {
            field_name,
            field_path,
            encoding: RepeatedElementEncoding::Varint,
            max_elements,
        }
    }

    /// Dotted protobuf field name, relative to the request message.
    #[must_use]
    pub const fn field_name(&self) -> &'static str {
        self.field_name
    }

    /// Field numbers from the request message down to the repeated field.
    #[must_use]
    pub const fn field_path(&self) -> &'static [u32] {
        self.field_path
    }

    /// Wire encoding of one element.
    #[must_use]
    pub const fn encoding(&self) -> RepeatedElementEncoding {
        self.encoding
    }

    /// Most elements one request may carry in this field.
    #[must_use]
    pub const fn max_elements(&self) -> usize {
        self.max_elements
    }

    fn check(&self, method: &str, message: &[u8]) -> Result<(), Status> {
        let element_count = count_repeated_elements(message, self.field_path, self.encoding);
        if element_count > self.max_elements {
            return Err(Status::invalid_argument(format!(
                "{method} request field {} carries {element_count} elements; at most {} are accepted",
                self.field_name, self.max_elements,
            )));
        }
        Ok(())
    }
}

/// Repeated-field bounds for the request message of one gRPC method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MethodFieldLimits {
    method: &'static str,
    fields: &'static [RepeatedFieldLimit],
}

impl MethodFieldLimits {
    /// Bounds the request message of `method`, the method name as it appears
    /// in the protobuf service (for example `GetTaddressBalance`).
    #[must_use]
    pub const fn new(method: &'static str, fields: &'static [RepeatedFieldLimit]) -> Self {
        Self { method, fields }
    }

    /// Method name within the protobuf service.
    #[must_use]
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// Bounded repeated fields of the method's request message.
    #[must_use]
    pub const fn fields(&self) -> &'static [RepeatedFieldLimit] {
        self.fields
    }

    fn check(&self, message: &[u8]) -> Result<(), Status> {
        self.fields
            .iter()
            .try_for_each(|field| field.check(self.method, message))
    }
}

/// A generated tonic server whose bounded requests are scanned before tonic
/// decodes them.
///
/// Identity requests to methods without [`MethodFieldLimits`] pass straight
/// through. For a bounded method, every gRPC frame is buffered up
/// to [`MAX_DECODING_MESSAGE_BYTES`](crate::MAX_DECODING_MESSAGE_BYTES), the
/// same cap tonic applies, and scanned without allocating decoded protobuf
/// values. Uncompressed frames that pass the scan are released unchanged.
/// When a repeated field exceeds its limit, the body ends with
/// an `INVALID_ARGUMENT` status, which tonic returns before the handler runs.
#[derive(Clone, Debug)]
pub struct RepeatedFieldGuard<S> {
    inner: S,
    method_limits: &'static [MethodFieldLimits],
    request_compression: bool,
}

impl<S> RepeatedFieldGuard<S> {
    /// Applies `method_limits` to the requests `inner` receives.
    #[must_use]
    pub const fn new(inner: S, method_limits: &'static [MethodFieldLimits]) -> Self {
        Self {
            inner,
            method_limits,
            request_compression: false,
        }
    }

    /// Decompress gzip and zstd requests under the message cap before scanning.
    ///
    /// Enable this when the wrapped server accepts both request encodings.
    /// The guard also bounds the zstd decoder window. It applies these limits
    /// to all methods, including those without repeated-field limits.
    #[must_use]
    pub const fn with_request_compression(mut self) -> Self {
        self.request_compression = true;
        self
    }
}

impl<S: NamedService> RepeatedFieldGuard<S> {
    fn limits_for(&self, request_path: &str) -> Option<&'static MethodFieldLimits> {
        let method = request_path
            .strip_prefix('/')?
            .strip_prefix(S::NAME)?
            .strip_prefix('/')?;
        self.method_limits
            .iter()
            .find(|limits| limits.method == method)
    }
}

impl<S: NamedService> NamedService for RepeatedFieldGuard<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, RequestBody> Service<http::Request<RequestBody>> for RepeatedFieldGuard<S>
where
    S: Service<http::Request<TonicBody>> + NamedService,
    RequestBody: HttpBody<Data = Bytes> + Send + 'static,
    RequestBody::Error: Into<tonic::codegen::StdError>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: http::Request<RequestBody>) -> Self::Future {
        let limits = self.limits_for(request.uri().path());
        let compression = self
            .request_compression
            .then(|| RequestCompression::from_headers(request.headers()))
            .flatten();
        if compression.is_some() {
            request.headers_mut().remove(http::header::CONTENT_LENGTH);
        }
        self.inner.call(request.map(|body| {
            let body = TonicBody::new(body);
            if limits.is_some() || compression.is_some() {
                TonicBody::new(BoundedRequestBody::new(body, limits, compression))
            } else {
                body
            }
        }))
    }
}

#[derive(Clone, Copy)]
enum RequestCompression {
    Gzip,
    Zstd,
}

impl RequestCompression {
    fn from_headers(headers: &http::HeaderMap) -> Option<Self> {
        match headers.get("grpc-encoding")?.as_bytes() {
            b"gzip" => Some(Self::Gzip),
            b"zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    fn decode(self, payload: &[u8]) -> Result<Bytes, Status> {
        match self {
            Self::Gzip => decode_frame(flate2::bufread::MultiGzDecoder::new(payload)),
            Self::Zstd => {
                validate_zstd_frames(payload)?;
                let mut decoder = zstd::stream::read::Decoder::with_buffer(payload)
                    .map_err(|error| compression_status(error.kind()))?;
                // A small frame can declare a much larger decoder window.
                decoder
                    .window_log_max(MAX_DECODING_MESSAGE_BYTES.ilog2())
                    .map_err(|error| compression_status(error.kind()))?;
                decode_frame(decoder)
            }
        }
    }
}

fn validate_zstd_frames(mut payload: &[u8]) -> Result<(), Status> {
    use zstd::zstd_safe::{FrameHeaderStatus, find_frame_compressed_size, get_frame_header};

    while !payload.is_empty() {
        // The legacy zstd decoder bypasses the configured window limit.
        let Ok(FrameHeaderStatus::Complete(header)) = get_frame_header(payload) else {
            return Err(invalid_compressed_message());
        };
        if header.windowSize > MAX_DECODING_MESSAGE_BYTES as u64 {
            return Err(Status::resource_exhausted(
                "zstd request exceeds the gRPC decoder window limit",
            ));
        }
        let frame_bytes =
            find_frame_compressed_size(payload).map_err(|_| invalid_compressed_message())?;
        payload = payload
            .get(frame_bytes..)
            .ok_or_else(invalid_compressed_message)?;
    }
    Ok(())
}

fn decode_frame(mut decoder: impl io::Read) -> Result<Bytes, Status> {
    let mut frame = BytesMut::with_capacity(GRPC_FRAME_HEADER_BYTES + 8 * 1024);
    frame.put_bytes(0, GRPC_FRAME_HEADER_BYTES);
    io::copy(
        &mut decoder,
        &mut (&mut frame).limit(MAX_DECODING_MESSAGE_BYTES).writer(),
    )
    .map_err(|error| compression_status(error.kind()))?;
    let length = u32::try_from(frame.len() - GRPC_FRAME_HEADER_BYTES).map_err(|_| {
        Status::resource_exhausted("decompressed request exceeds the gRPC frame size")
    })?;
    frame[1..GRPC_FRAME_HEADER_BYTES].copy_from_slice(&length.to_be_bytes());
    Ok(frame.freeze())
}

fn compression_status(kind: io::ErrorKind) -> Status {
    if kind == io::ErrorKind::WriteZero {
        Status::resource_exhausted("decompressed request exceeds the gRPC message size limit")
    } else {
        invalid_compressed_message()
    }
}

fn invalid_compressed_message() -> Status {
    Status::invalid_argument("invalid or unsupported compressed gRPC message")
}

/// Request body that releases each gRPC frame only after its bounded repeated
/// fields pass their limits.
struct BoundedRequestBody {
    inner: TonicBody,
    limits: Option<&'static MethodFieldLimits>,
    compression: Option<RequestCompression>,
    pending: BytesMut,
    state: BodyState,
}

enum BodyState {
    /// Each complete frame is scanned before it is released.
    Scanning,
    /// `pending` and then the rest of the body are released unscanned. tonic
    /// refuses the frame that caused this, so it decodes nothing after it.
    PassThrough(Tail),
    /// A frame was refused and the body has ended.
    Refused,
}

/// What follows `pending` once the body passes through.
enum Tail {
    /// The rest of the inner body.
    Inner,
    /// An item the inner body yielded before `pending` was released.
    Held(Result<Frame<Bytes>, Status>),
    /// The inner body has ended.
    Ended,
}

/// What the buffered bytes say about the next gRPC frame.
enum PendingFrame {
    /// The frame header or message is not complete yet.
    Incomplete,
    /// A complete frame of this many bytes, header included.
    Complete(usize),
    /// A frame over the decoding cap or with an unsupported compression flag.
    /// Tonic refuses it before decoding.
    Unscannable,
}

/// Outcome of one scanning step.
enum ScanStep {
    /// Yield this from `poll_frame`.
    Release(Result<Frame<Bytes>, Status>),
    /// More bytes arrived or the state changed; take another turn.
    Continue,
}

impl BoundedRequestBody {
    fn new(
        inner: TonicBody,
        limits: Option<&'static MethodFieldLimits>,
        compression: Option<RequestCompression>,
    ) -> Self {
        Self {
            inner,
            limits,
            compression,
            pending: BytesMut::new(),
            state: BodyState::Scanning,
        }
    }

    fn pending_frame(&self) -> PendingFrame {
        let Some(mut header) = self.pending.get(..GRPC_FRAME_HEADER_BYTES) else {
            return PendingFrame::Incomplete;
        };
        let flag = header.get_u8();
        let Ok(message_bytes) = usize::try_from(header.get_u32()) else {
            return PendingFrame::Unscannable;
        };
        if !scannable_flag(flag, self.compression) || message_bytes > MAX_DECODING_MESSAGE_BYTES {
            return PendingFrame::Unscannable;
        }
        let frame_bytes = GRPC_FRAME_HEADER_BYTES + message_bytes;
        if self.pending.len() < frame_bytes {
            PendingFrame::Incomplete
        } else {
            PendingFrame::Complete(frame_bytes)
        }
    }

    /// Appends `chunk` to the pending bytes.
    ///
    /// `BytesMut` grows by doubling, which would let a frame just over half
    /// the decoding limit hold twice its size. A buffer that must grow is
    /// instead replaced by one sized for the frame its header declares, or
    /// for the bytes at hand when they are more.
    fn buffer(&mut self, chunk: &[u8]) {
        let needed = self.pending.len().saturating_add(chunk.len());
        if needed > self.pending.capacity() {
            let declared = declared_frame_bytes(&self.pending, chunk, self.compression);
            let mut grown = BytesMut::with_capacity(needed.max(declared));
            grown.extend_from_slice(&self.pending);
            self.pending = grown;
        }
        self.pending.extend_from_slice(chunk);
    }

    fn scan_frame(&self, frame: Bytes) -> Result<Bytes, Status> {
        let frame = match (frame.first(), self.compression) {
            (Some(1), Some(compression)) => {
                compression.decode(&frame[GRPC_FRAME_HEADER_BYTES..])?
            }
            _ => frame,
        };
        if let Some(limits) = self.limits {
            limits.check(&frame[GRPC_FRAME_HEADER_BYTES..])?;
        }
        Ok(frame)
    }

    /// Releases the next complete frame, or reads more of the inner body.
    fn poll_scanning(&mut self, context: &mut Context<'_>) -> Poll<ScanStep> {
        match self.pending_frame() {
            PendingFrame::Complete(frame_bytes) => {
                let frame = self.pending.split_to(frame_bytes).freeze();
                let checked = self.scan_frame(frame).map(Frame::data);
                if checked.is_err() {
                    self.pending = BytesMut::new();
                    self.state = BodyState::Refused;
                }
                return Poll::Ready(ScanStep::Release(checked));
            }
            PendingFrame::Unscannable => {
                self.state = BodyState::PassThrough(Tail::Inner);
                return Poll::Ready(ScanStep::Continue);
            }
            PendingFrame::Incomplete => {}
        }
        // An incomplete frame released ahead of trailers or the end of the
        // body makes tonic report an unexpected end of stream, as it would
        // without the guard.
        let tail = match ready!(Pin::new(&mut self.inner).poll_frame(context)) {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(chunk) => {
                    self.buffer(&chunk);
                    return Poll::Ready(ScanStep::Continue);
                }
                Err(trailers) => Tail::Held(Ok(trailers)),
            },
            Some(Err(status)) => Tail::Held(Err(status)),
            None => Tail::Ended,
        };
        self.state = BodyState::PassThrough(tail);
        Poll::Ready(ScanStep::Continue)
    }
}

/// The length of the scannable frame whose header starts `buffered` and
/// continues into `chunk`, or zero while that header is incomplete or
/// declares a frame the guard passes through unscanned.
fn declared_frame_bytes(
    buffered: &[u8],
    chunk: &[u8],
    compression: Option<RequestCompression>,
) -> usize {
    let mut header = [0_u8; GRPC_FRAME_HEADER_BYTES];
    let mut header_bytes = buffered.iter().chain(chunk);
    for byte in &mut header {
        let Some(&next) = header_bytes.next() else {
            return 0;
        };
        *byte = next;
    }
    let mut header = &header[..];
    let flag = header.get_u8();
    match usize::try_from(header.get_u32()) {
        Ok(message_bytes)
            if scannable_flag(flag, compression) && message_bytes <= MAX_DECODING_MESSAGE_BYTES =>
        {
            GRPC_FRAME_HEADER_BYTES + message_bytes
        }
        Ok(_) | Err(_) => 0,
    }
}

const fn scannable_flag(flag: u8, compression: Option<RequestCompression>) -> bool {
    flag == 0 || (flag == 1 && compression.is_some())
}

impl HttpBody for BoundedRequestBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                BodyState::Refused => return Poll::Ready(None),
                BodyState::Scanning => match ready!(this.poll_scanning(context)) {
                    ScanStep::Release(released) => return Poll::Ready(Some(released)),
                    ScanStep::Continue => {}
                },
                BodyState::PassThrough(tail) => {
                    if !this.pending.is_empty() {
                        return Poll::Ready(Some(Ok(Frame::data(this.pending.split().freeze()))));
                    }
                    return match std::mem::replace(tail, Tail::Inner) {
                        Tail::Inner => Pin::new(&mut this.inner).poll_frame(context),
                        Tail::Held(held) => Poll::Ready(Some(held)),
                        Tail::Ended => {
                            *tail = Tail::Ended;
                            Poll::Ready(None)
                        }
                    };
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.state {
            BodyState::Refused => true,
            BodyState::Scanning | BodyState::PassThrough(Tail::Inner) => {
                self.pending.is_empty() && self.inner.is_end_stream()
            }
            BodyState::PassThrough(Tail::Held(_)) => false,
            BodyState::PassThrough(Tail::Ended) => self.pending.is_empty(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Wire type of a protobuf record, from the low three bits of its key.
mod wire {
    pub(super) const VARINT: u64 = 0;
    pub(super) const FIXED64: u64 = 1;
    pub(super) const LENGTH_DELIMITED: u64 = 2;
    pub(super) const START_GROUP: u64 = 3;
    pub(super) const END_GROUP: u64 = 4;
    pub(super) const FIXED32: u64 = 5;
}

/// One record of an encoded protobuf message.
struct WireRecord<'message> {
    field_number: u64,
    wire_type: u64,
    /// Payload of a length-delimited record; empty for every other wire type.
    payload: &'message [u8],
}

/// Counts the elements of the repeated field at `field_path` in `message`.
///
/// Every occurrence of an intermediate message field is scanned, because
/// protobuf merges repeated occurrences of a singular message field. Counting
/// stops at the first malformed record: the decoder refuses the message there,
/// so it never decodes anything after it.
fn count_repeated_elements(
    mut message: &[u8],
    field_path: &[u32],
    encoding: RepeatedElementEncoding,
) -> usize {
    let Some((&field_number, nested_path)) = field_path.split_first() else {
        return 0;
    };
    let mut element_count = 0_usize;
    while let Some(record) = next_record(&mut message) {
        if record.field_number != u64::from(field_number) {
            continue;
        }
        let record_elements = match (nested_path.is_empty(), record.wire_type, encoding) {
            (false, wire::LENGTH_DELIMITED, _) => {
                count_repeated_elements(record.payload, nested_path, encoding)
            }
            (true, wire::LENGTH_DELIMITED, RepeatedElementEncoding::LengthDelimited)
            | (true, wire::VARINT, RepeatedElementEncoding::Varint) => 1,
            // A packed run holds one varint per byte without a continuation bit.
            (true, wire::LENGTH_DELIMITED, RepeatedElementEncoding::Varint) => record
                .payload
                .iter()
                .filter(|byte| **byte & 0x80 == 0)
                .count(),
            _ => 0,
        };
        element_count = element_count.saturating_add(record_elements);
    }
    element_count
}

/// Reads the next record from `message`, or `None` at the end of the message
/// or at the first malformed record. A group is skipped whole and reported
/// with an empty payload.
fn next_record<'message>(message: &mut &'message [u8]) -> Option<WireRecord<'message>> {
    let key = read_varint(message)?;
    let field_number = key >> 3;
    let wire_type = key & 0x7;
    if field_number == 0 || field_number > u64::from(u32::MAX) {
        return None;
    }
    let payload = match wire_type {
        wire::LENGTH_DELIMITED => read_length_delimited(message)?,
        wire::START_GROUP => {
            skip_group(message)?;
            &[]
        }
        _ => {
            skip_scalar(message, wire_type)?;
            &[]
        }
    };
    Some(WireRecord {
        field_number,
        wire_type,
        payload,
    })
}

/// Skips the records of a group whose start key was just read, through its
/// matching end key.
fn skip_group(message: &mut &[u8]) -> Option<()> {
    let mut depth = 1_usize;
    while depth > 0 {
        let key = read_varint(message)?;
        match key & 0x7 {
            wire::START_GROUP => depth += 1,
            wire::END_GROUP => depth -= 1,
            wire::LENGTH_DELIMITED => {
                read_length_delimited(message)?;
            }
            other => skip_scalar(message, other)?,
        }
    }
    Some(())
}

fn skip_scalar(message: &mut &[u8], wire_type: u64) -> Option<()> {
    let width = match wire_type {
        wire::VARINT => {
            read_varint(message)?;
            return Some(());
        }
        wire::FIXED64 => 8,
        wire::FIXED32 => 4,
        _ => return None,
    };
    *message = message.get(width..)?;
    Some(())
}

fn read_length_delimited<'message>(message: &mut &'message [u8]) -> Option<&'message [u8]> {
    let length = usize::try_from(read_varint(message)?).ok()?;
    let payload = message.get(..length)?;
    *message = message.get(length..)?;
    Some(payload)
}

fn read_varint(message: &mut &[u8]) -> Option<u64> {
    let mut decoded = 0_u64;
    for (index, byte) in message.iter().take(10).enumerate() {
        decoded |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            *message = message.get(index + 1..)?;
            return Some(decoded);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Write as _};

    use http_body_util::BodyExt as _;

    use super::*;

    const ADDRESSES: RepeatedFieldLimit =
        RepeatedFieldLimit::length_delimited("addresses", &[1], 2);
    const POOL_TYPES: RepeatedFieldLimit = RepeatedFieldLimit::varint("poolTypes", &[3], 2);
    const FILTER_POOLS: RepeatedFieldLimit = RepeatedFieldLimit::varint("filter.pools", &[4, 2], 2);

    fn key(field_number: u8, wire_type: u8) -> u8 {
        (field_number << 3) | wire_type
    }

    fn count(message: &[u8], limit: RepeatedFieldLimit) -> usize {
        count_repeated_elements(message, limit.field_path(), limit.encoding())
    }

    #[test]
    fn oversized_zstd_frame_with_known_content_size_is_resource_exhausted() -> io::Result<()> {
        let compressed = zstd::bulk::compress(&vec![0; MAX_DECODING_MESSAGE_BYTES + 1], 3)?;
        assert_eq!(
            RequestCompression::Zstd
                .decode(&compressed)
                .err()
                .as_ref()
                .map(Status::code),
            Some(tonic::Code::ResourceExhausted)
        );
        Ok(())
    }

    const SMALL_ZSTD_REQUEST: &[u8] = &[0x0a, 1, b'a'];

    fn small_zstd_frame(window_log: u32) -> eyre::Result<Vec<u8>> {
        use zstd::zstd_safe::{FrameHeaderStatus, get_frame_content_size, get_frame_header};

        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1)?;
        encoder.window_log(window_log)?;
        encoder.include_contentsize(false)?;
        encoder.write_all(SMALL_ZSTD_REQUEST)?;
        // Flush before finishing so the frame keeps the streaming window.
        encoder.flush()?;
        let compressed = encoder.finish()?;
        assert!(matches!(get_frame_content_size(&compressed), Ok(None)));
        assert!(matches!(
            get_frame_header(&compressed),
            Ok(FrameHeaderStatus::Complete(header)) if header.windowSize == 1_u64 << window_log
        ));
        assert_eq!(
            zstd::bulk::decompress(&compressed, SMALL_ZSTD_REQUEST.len())?,
            SMALL_ZSTD_REQUEST
        );
        Ok(compressed)
    }

    fn scan_zstd_request(compressed: &[u8]) -> Result<Bytes, Status> {
        let mut frame = grpc_frame(compressed);
        frame[0] = 1;
        BoundedRequestBody::new(
            TonicBody::empty(),
            Some(&BALANCE_LIMITS),
            Some(RequestCompression::Zstd),
        )
        .scan_frame(frame.into())
    }

    fn assert_zstd_window_refused(compressed: &[u8]) -> eyre::Result<()> {
        let refusal = scan_zstd_request(compressed).err().ok_or_else(|| {
            eyre::eyre!("the guard accepted a zstd request with an oversized window")
        })?;
        assert_eq!(refusal.code(), tonic::Code::ResourceExhausted);
        assert!(refusal.message().contains("decoder window limit"));
        Ok(())
    }

    #[test]
    fn zstd_rejects_an_oversized_window_on_a_small_request() -> eyre::Result<()> {
        let compressed = small_zstd_frame(MAX_DECODING_MESSAGE_BYTES.ilog2() + 1)?;
        assert_zstd_window_refused(&compressed)
    }

    #[test]
    fn zstd_rejects_an_oversized_window_on_a_second_frame() -> eyre::Result<()> {
        let window_log = MAX_DECODING_MESSAGE_BYTES.ilog2();
        let mut compressed = small_zstd_frame(window_log)?;
        compressed.extend(small_zstd_frame(window_log + 1)?);
        assert_zstd_window_refused(&compressed)
    }

    #[test]
    fn zstd_accepts_a_window_at_the_limit() -> eyre::Result<()> {
        let compressed = small_zstd_frame(MAX_DECODING_MESSAGE_BYTES.ilog2())?;
        assert_eq!(
            scan_zstd_request(&compressed)?,
            grpc_frame(SMALL_ZSTD_REQUEST)
        );
        Ok(())
    }

    #[test]
    fn zstd_rejects_legacy_frames() -> eyre::Result<()> {
        // A v0.7 single-segment frame with one raw block and an end block.
        let legacy = [
            0x27, 0xb5, 0x2f, 0xfd, 0x20, 3, 0x40, 0, 3, 0x0a, 1, b'a', 0xc0, 0, 0,
        ];
        assert_eq!(
            zstd::stream::decode_all(legacy.as_slice())?,
            SMALL_ZSTD_REQUEST
        );
        let refusal = scan_zstd_request(&legacy)
            .err()
            .ok_or_else(|| eyre::eyre!("the guard accepted a legacy zstd frame"))?;
        assert_eq!(refusal.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            refusal.message(),
            "invalid or unsupported compressed gRPC message"
        );
        Ok(())
    }

    #[test]
    fn counts_length_delimited_elements() {
        let message = [key(1, 2), 0, key(1, 2), 1, b'a', key(2, 0), 7, key(1, 2), 0];
        assert_eq!(count(&message, ADDRESSES), 3);
    }

    #[test]
    fn counts_packed_and_unpacked_varints_together() {
        // Two unpacked elements, then a packed run of three (one is two bytes).
        let message = [key(3, 0), 1, key(3, 0), 2, key(3, 2), 4, 1, 0x80, 0x01, 3];
        assert_eq!(count(&message, POOL_TYPES), 5);
    }

    #[test]
    fn sums_elements_across_occurrences_of_a_nested_message() {
        let message = [
            key(4, 2),
            4,
            key(2, 2),
            2,
            1,
            2,
            key(1, 0),
            9,
            key(4, 2),
            2,
            key(2, 0),
            3,
        ];
        assert_eq!(count(&message, FILTER_POOLS), 3);
    }

    #[test]
    fn skips_groups_and_fixed_width_records() {
        let message = [
            key(5, 3),
            key(1, 2),
            0,
            key(6, 5),
            0,
            0,
            0,
            0,
            key(5, 4),
            key(7, 1),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            key(1, 2),
            0,
        ];
        // The field-1 record inside the group belongs to the group, not the request.
        assert_eq!(count(&message, ADDRESSES), 1);
    }

    #[test]
    fn stops_counting_at_a_malformed_record() {
        let message = [key(1, 2), 0, key(1, 2), 5, b'a', key(1, 2), 0];
        assert_eq!(count(&message, ADDRESSES), 1);
        let zero_field = [key(1, 2), 0, 0x00, key(1, 2), 0];
        assert_eq!(count(&zero_field, ADDRESSES), 1);
    }

    #[test]
    fn refuses_a_field_over_its_limit_with_invalid_argument() {
        let limits = MethodFieldLimits::new("GetTaddressBalance", &[ADDRESSES]);
        let message = [key(1, 2), 0, key(1, 2), 0, key(1, 2), 0];
        let refusal = limits.check(&message).err();
        assert_eq!(
            refusal.as_ref().map(Status::code),
            Some(tonic::Code::InvalidArgument),
        );
        assert_eq!(
            refusal.as_ref().map(Status::message),
            Some(
                "GetTaddressBalance request field addresses carries 3 elements; at most 2 are accepted"
            ),
        );
        assert!(limits.check(&message[..4]).is_ok());
    }

    const BALANCE_LIMITS: MethodFieldLimits =
        MethodFieldLimits::new("GetTaddressBalance", &[ADDRESSES]);

    /// Body that yields a fixed sequence of frames.
    struct ScriptedBody(VecDeque<Result<Frame<Bytes>, Status>>);

    impl HttpBody for ScriptedBody {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
            Poll::Ready(self.get_mut().0.pop_front())
        }
    }

    fn grpc_frame(message: &[u8]) -> Vec<u8> {
        let mut frame = vec![0];
        frame.extend_from_slice(
            &u32::try_from(message.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        frame.extend_from_slice(message);
        frame
    }

    fn compressed_grpc_frame(
        message: &[u8],
        compression: RequestCompression,
    ) -> io::Result<Vec<u8>> {
        let compressed = match compression {
            RequestCompression::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(message)?;
                encoder.finish()?
            }
            RequestCompression::Zstd => zstd::bulk::compress(message, 1)?,
        };
        let mut frame = grpc_frame(&compressed);
        frame[0] = 1;
        Ok(frame)
    }

    fn trailers() -> http::HeaderMap {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-trailer", http::HeaderValue::from_static("kept"));
        trailers
    }

    /// What the guard released for one scripted request body.
    struct Drained {
        bytes: Vec<u8>,
        trailers: Option<http::HeaderMap>,
        refusal: Option<Status>,
    }

    /// Feeds `chunks` and then trailers through the guard.
    async fn drain(chunks: &[&[u8]], compression: Option<RequestCompression>) -> Drained {
        let mut script: VecDeque<_> = chunks
            .iter()
            .map(|chunk| Ok(Frame::data(Bytes::copy_from_slice(chunk))))
            .collect();
        script.push_back(Ok(Frame::trailers(trailers())));
        let mut body = BoundedRequestBody::new(
            TonicBody::new(ScriptedBody(script)),
            Some(&BALANCE_LIMITS),
            compression,
        );
        let mut drained = Drained {
            bytes: Vec::new(),
            trailers: None,
            refusal: None,
        };
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(chunk) => drained.bytes.extend_from_slice(&chunk),
                    Err(frame) => drained.trailers = frame.into_trailers().ok(),
                },
                Err(status) => {
                    drained.refusal = Some(status);
                    break;
                }
            }
        }
        drained
    }

    #[test]
    fn buffering_a_frame_never_reserves_past_its_declared_size() {
        let frame = grpc_frame(&vec![0; 600_000]);
        let mut body = BoundedRequestBody::new(TonicBody::empty(), Some(&BALANCE_LIMITS), None);
        for chunk in frame.chunks(16 * 1024) {
            body.buffer(chunk);
            assert!(
                body.pending.capacity() <= frame.len(),
                "{} bytes reserved for a {}-byte frame",
                body.pending.capacity(),
                frame.len()
            );
        }
        assert_eq!(body.pending.as_ref(), frame.as_slice());
    }

    #[tokio::test]
    async fn releases_accepted_frames_unchanged_across_chunk_boundaries() {
        let mut encoded = grpc_frame(&[key(1, 2), 1, b'a', key(1, 2), 0]);
        encoded.extend(grpc_frame(&[key(2, 0), 1]));
        let chunks: Vec<&[u8]> = encoded.chunks(3).collect();

        let drained = drain(&chunks, None).await;

        assert_eq!(drained.bytes, encoded);
        assert_eq!(drained.trailers, Some(trailers()));
        assert!(
            drained.refusal.is_none(),
            "unexpected refusal: {:?}",
            drained.refusal
        );
    }

    #[tokio::test]
    async fn refuses_a_later_frame_over_its_limit() {
        let accepted = grpc_frame(&[key(1, 2), 0]);
        let mut encoded = accepted.clone();
        encoded.extend(grpc_frame(&[key(1, 2), 0, key(1, 2), 0, key(1, 2), 0]));

        let drained = drain(&[&encoded], None).await;

        assert_eq!(drained.bytes, accepted);
        assert_eq!(drained.trailers, None);
        assert_eq!(
            drained.refusal.as_ref().map(Status::code),
            Some(tonic::Code::InvalidArgument)
        );
    }

    #[tokio::test]
    async fn releases_fragmented_identity_and_compressed_messages_in_order() -> io::Result<()> {
        let first = grpc_frame(&[key(1, 2), 1, b'a']);
        let middle = [key(1, 2), 1, b'b'];
        let last = grpc_frame(&[key(1, 2), 1, b'c']);
        let expected = [first.as_slice(), &grpc_frame(&middle), last.as_slice()].concat();

        for compression in [RequestCompression::Gzip, RequestCompression::Zstd] {
            let encoded = [
                first.as_slice(),
                &compressed_grpc_frame(&middle, compression)?,
                last.as_slice(),
            ]
            .concat();
            let chunks: Vec<&[u8]> = encoded.chunks(2).collect();
            let drained = drain(&chunks, Some(compression)).await;

            assert_eq!(drained.bytes, expected);
            assert_eq!(drained.trailers, Some(trailers()));
            assert!(drained.refusal.is_none(), "{:?}", drained.refusal);
        }
        Ok(())
    }

    #[tokio::test]
    async fn releases_valid_messages_before_refusing_a_later_compressed_message() -> io::Result<()>
    {
        let first = grpc_frame(&[key(1, 2), 0]);
        let second = [key(1, 2), 1, b'a'];
        let over_limit = [key(1, 2), 0, key(1, 2), 0, key(1, 2), 0];
        let expected = [first.as_slice(), &grpc_frame(&second)].concat();

        for compression in [RequestCompression::Gzip, RequestCompression::Zstd] {
            let encoded = [
                first.as_slice(),
                &compressed_grpc_frame(&second, compression)?,
                &compressed_grpc_frame(&over_limit, compression)?,
            ]
            .concat();
            let drained = drain(&[&encoded], Some(compression)).await;

            assert_eq!(drained.bytes, expected);
            assert_eq!(drained.trailers, None);
            assert_eq!(
                drained.refusal.as_ref().map(Status::code),
                Some(tonic::Code::InvalidArgument)
            );
            assert_eq!(
                drained.refusal.as_ref().map(Status::message),
                Some(
                    "GetTaddressBalance request field addresses carries 3 elements; at most 2 are accepted"
                )
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn passes_through_a_frame_tonic_refuses_by_its_header() {
        let mut oversized = vec![0];
        let oversized_length = u32::try_from(MAX_DECODING_MESSAGE_BYTES + 1).unwrap_or(u32::MAX);
        oversized.extend_from_slice(&oversized_length.to_be_bytes());
        oversized.extend_from_slice(&[key(1, 2), 0, key(1, 2), 0, key(1, 2), 0]);
        let mut compressed = grpc_frame(&[key(1, 2), 0, key(1, 2), 0, key(1, 2), 0]);
        if let Some(flag) = compressed.first_mut() {
            *flag = 1;
        }

        for encoded in [oversized, compressed] {
            let drained = drain(&[&encoded], None).await;

            assert_eq!(drained.bytes, encoded);
            assert_eq!(drained.trailers, Some(trailers()));
            assert!(
                drained.refusal.is_none(),
                "unexpected refusal: {:?}",
                drained.refusal
            );
        }
    }

    #[tokio::test]
    async fn releases_an_incomplete_frame_before_the_trailers() {
        let encoded = grpc_frame(&[key(1, 2), 0]);
        let truncated = encoded.get(..encoded.len() - 1).unwrap_or_default();

        let drained = drain(&[truncated], None).await;

        assert_eq!(drained.bytes, truncated);
        assert_eq!(drained.trailers, Some(trailers()));
        assert!(
            drained.refusal.is_none(),
            "unexpected refusal: {:?}",
            drained.refusal
        );
    }
}
