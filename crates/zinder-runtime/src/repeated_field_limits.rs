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
//! holds more elements than allowed. Accepted frames reach tonic unchanged.

use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::{Buf as _, Bytes, BytesMut};
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
/// Requests to methods without [`MethodFieldLimits`] pass straight through.
/// For a bounded method, every gRPC frame in the request body is buffered up
/// to [`MAX_DECODING_MESSAGE_BYTES`](crate::MAX_DECODING_MESSAGE_BYTES), the
/// same cap tonic applies, scanned without allocating, and released unchanged
/// when every bounded field is within its limit. Otherwise the body ends with
/// an `INVALID_ARGUMENT` status, which tonic returns before the handler runs.
#[derive(Clone, Debug)]
pub struct RepeatedFieldGuard<S> {
    inner: S,
    method_limits: &'static [MethodFieldLimits],
}

impl<S> RepeatedFieldGuard<S> {
    /// Applies `method_limits` to the requests `inner` receives.
    #[must_use]
    pub const fn new(inner: S, method_limits: &'static [MethodFieldLimits]) -> Self {
        Self {
            inner,
            method_limits,
        }
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

    fn call(&mut self, request: http::Request<RequestBody>) -> Self::Future {
        let limits = self.limits_for(request.uri().path());
        let request = request.map(|body| {
            let body = TonicBody::new(body);
            match limits {
                Some(limits) => TonicBody::new(BoundedRequestBody::new(body, limits)),
                None => body,
            }
        });
        self.inner.call(request)
    }
}

/// Request body that releases each gRPC frame only after its bounded repeated
/// fields pass their limits.
struct BoundedRequestBody {
    inner: TonicBody,
    limits: &'static MethodFieldLimits,
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
    /// A complete uncompressed frame of this many bytes, header included.
    Complete(usize),
    /// A compressed frame, or one over the decoding cap. tonic refuses both
    /// before decoding: no compression encoding is enabled on Zinder servers,
    /// and the length check precedes the read. If a server ever accepts a
    /// compression encoding, this guard must decompress before scanning.
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
    fn new(inner: TonicBody, limits: &'static MethodFieldLimits) -> Self {
        Self {
            inner,
            limits,
            pending: BytesMut::new(),
            state: BodyState::Scanning,
        }
    }

    fn pending_frame(&self) -> PendingFrame {
        let Some(mut header) = self.pending.get(..GRPC_FRAME_HEADER_BYTES) else {
            return PendingFrame::Incomplete;
        };
        let compressed = header.get_u8() != 0;
        let Ok(message_bytes) = usize::try_from(header.get_u32()) else {
            return PendingFrame::Unscannable;
        };
        if compressed || message_bytes > MAX_DECODING_MESSAGE_BYTES {
            return PendingFrame::Unscannable;
        }
        let frame_bytes = GRPC_FRAME_HEADER_BYTES + message_bytes;
        if self.pending.len() < frame_bytes {
            PendingFrame::Incomplete
        } else {
            PendingFrame::Complete(frame_bytes)
        }
    }

    /// Releases the next complete frame, or reads more of the inner body.
    fn poll_scanning(&mut self, context: &mut Context<'_>) -> Poll<ScanStep> {
        match self.pending_frame() {
            PendingFrame::Complete(frame_bytes) => {
                let frame = self.pending.split_to(frame_bytes).freeze();
                let message = frame.get(GRPC_FRAME_HEADER_BYTES..).unwrap_or_default();
                if let Err(status) = self.limits.check(message) {
                    self.pending = BytesMut::new();
                    self.state = BodyState::Refused;
                    return Poll::Ready(ScanStep::Release(Err(status)));
                }
                return Poll::Ready(ScanStep::Release(Ok(Frame::data(frame))));
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
                    self.pending.extend_from_slice(&chunk);
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
    use std::collections::VecDeque;

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
    async fn drain(chunks: &[&[u8]]) -> Drained {
        let mut script: VecDeque<_> = chunks
            .iter()
            .map(|chunk| Ok(Frame::data(Bytes::copy_from_slice(chunk))))
            .collect();
        script.push_back(Ok(Frame::trailers(trailers())));
        let mut body =
            BoundedRequestBody::new(TonicBody::new(ScriptedBody(script)), &BALANCE_LIMITS);
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

    #[tokio::test]
    async fn releases_accepted_frames_unchanged_across_chunk_boundaries() {
        let mut encoded = grpc_frame(&[key(1, 2), 1, b'a', key(1, 2), 0]);
        encoded.extend(grpc_frame(&[key(2, 0), 1]));
        let chunks: Vec<&[u8]> = encoded.chunks(3).collect();

        let drained = drain(&chunks).await;

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

        let drained = drain(&[&encoded]).await;

        assert_eq!(drained.bytes, accepted);
        assert_eq!(drained.trailers, None);
        assert_eq!(
            drained.refusal.as_ref().map(Status::code),
            Some(tonic::Code::InvalidArgument)
        );
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
            let drained = drain(&[&encoded]).await;

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

        let drained = drain(&[truncated]).await;

        assert_eq!(drained.bytes, truncated);
        assert_eq!(drained.trailers, Some(trailers()));
        assert!(
            drained.refusal.is_none(),
            "unexpected refusal: {:?}",
            drained.refusal
        );
    }
}
