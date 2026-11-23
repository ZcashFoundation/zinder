use std::{
    collections::VecDeque,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

use eyre::eyre;
use http_body::Body as _;
use prost::Message;
use tonic::codec::{EncodeBody, SingleMessageCompressionOverride};
use tonic::codegen::tokio_stream::Stream;

use super::{
    BATCH_BYTES, BlockRange, BoundedChunkRetention, Bytes, COPIED_PIECE_BYTES, CompactBlock,
    EnabledCompressionEncodings, EncodedBlock, EncodedBlockBody, EncodedBlockCodec,
    EncodedBlockRange, EncodedBlockRangeStream, Grpc, InvalidEncodedBlock, Request, Response,
    Status, http, server_streaming,
};

/// A message stream that reports `Pending` once wherever the script has `None`.
struct Scripted(VecDeque<Option<Result<EncodedBlock, Status>>>);

impl Stream for Scripted {
    type Item = Result<EncodedBlock, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().0.pop_front() {
            Some(Some(message)) => Poll::Ready(Some(message)),
            Some(None) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct Sent {
    frames: Vec<Bytes>,
    trailers: Option<http::HeaderMap>,
}

impl Sent {
    fn bytes(&self) -> Vec<u8> {
        self.frames.concat()
    }
}

/// Polls `body` as hyper does: until it ends or sends its trailers.
fn drain<B>(body: B) -> eyre::Result<Sent>
where
    B: http_body::Body<Data = Bytes, Error = Status>,
{
    let mut body = pin!(body);
    let mut sent = Sent::default();
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        let frame = match body.as_mut().poll_frame(&mut cx) {
            Poll::Pending => continue,
            Poll::Ready(None) => return Ok(sent),
            Poll::Ready(Some(frame)) => frame?,
        };
        match frame.into_data() {
            Ok(frame) => sent.frames.push(frame),
            Err(frame) => {
                sent.trailers = Some(
                    frame
                        .into_trailers()
                        .map_err(|_| eyre!("unexpected body frame"))?,
                );
                return Ok(sent);
            }
        }
    }
}

fn framed(payload: &[u8]) -> eyre::Result<EncodedBlock> {
    let mut message = vec![0];
    message.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
    message.extend_from_slice(payload);
    let allocation = message.capacity();
    Ok(EncodedBlock::from_length_prefixed_in(
        message.into(),
        allocation,
    )?)
}

fn payload(len: usize, tag: u8) -> Vec<u8> {
    (0..len)
        .map(|offset| tag.wrapping_add(offset.to_le_bytes()[0]))
        .collect()
}

/// Sends `script` through the encoded body and through tonic's encoder.
fn both(
    script: &[Option<Result<EncodedBlock, Status>>],
    max_message_size: Option<usize>,
) -> eyre::Result<(Sent, Sent)> {
    let source = || Box::pin(Scripted(script.iter().cloned().collect()));
    let encoded = drain(EncodedBlockBody::new(source(), max_message_size))?;
    let tonic = drain(EncodeBody::new_server(
        EncodedBlockCodec,
        source(),
        None,
        SingleMessageCompressionOverride::default(),
        max_message_size,
    ))?;
    Ok((encoded, tonic))
}

fn grpc_status(sent: &Sent) -> Option<Status> {
    sent.trailers.as_ref().and_then(Status::from_header_map)
}

#[test]
fn mixed_payload_sizes_send_the_same_bytes_and_trailers_as_tonic() -> eyre::Result<()> {
    let at_batch = BATCH_BYTES - EncodedBlock::PREFIX_LEN;
    let sizes = [
        0,
        1,
        300,
        at_batch - 1,
        at_batch,
        2,
        97_000,
        20_000,
        20_000,
        at_batch + 1,
        0,
    ];
    let mut script = Vec::new();
    for (tag, len) in sizes.into_iter().enumerate() {
        script.push(Some(Ok(framed(&payload(len, u8::try_from(tag)?))?)));
        if tag % 3 == 2 {
            script.push(None);
        }
    }
    let (encoded, tonic) = both(&script, None)?;
    assert_eq!(encoded.bytes(), tonic.bytes());
    assert_eq!(encoded.trailers, tonic.trailers);
    assert_eq!(
        grpc_status(&encoded).map(|status| status.code()),
        Some(tonic::Code::Ok)
    );
    Ok(())
}

#[test]
fn the_body_returns_messages_of_a_batch_or_more_without_a_copy() -> eyre::Result<()> {
    let large = framed(&payload(97_000, 7))?;
    let small = framed(&payload(10, 1))?;
    let script = [Some(Ok(small)), Some(Ok(large.clone()))];
    let (encoded, _) = both(&script, None)?;
    assert_eq!(encoded.frames.len(), 2);
    assert_eq!(encoded.frames[1].as_ptr(), large.message.as_ptr());
    assert_eq!(encoded.frames[1], large.message);
    Ok(())
}

#[test]
fn a_mid_stream_error_ends_with_its_status_after_earlier_messages() -> eyre::Result<()> {
    let script = [
        Some(Ok(framed(&payload(97_000, 1))?)),
        Some(Ok(framed(&payload(300, 2))?)),
        Some(Err(Status::data_loss("damaged segment"))),
        Some(Ok(framed(&payload(300, 3))?)),
    ];
    let (encoded, tonic) = both(&script, None)?;
    assert_eq!(encoded.bytes(), tonic.bytes());
    assert_eq!(encoded.trailers, tonic.trailers);
    let status = grpc_status(&encoded).ok_or_else(|| eyre!("missing status"))?;
    assert_eq!(status.code(), tonic::Code::DataLoss);
    assert_eq!(status.message(), "damaged segment");
    assert_eq!(
        encoded.bytes().len(),
        2 * EncodedBlock::PREFIX_LEN + 97_000 + 300
    );
    Ok(())
}

#[test]
fn the_message_size_limit_admits_its_exact_size_and_refuses_one_more_byte() -> eyre::Result<()> {
    let limit = 40_000;
    let script = [
        Some(Ok(framed(&payload(limit, 1))?)),
        Some(Ok(framed(&payload(limit + 1, 2))?)),
        Some(Ok(framed(&payload(10, 3))?)),
    ];
    let (encoded, tonic) = both(&script, Some(limit))?;
    assert_eq!(encoded.bytes(), tonic.bytes());
    assert_eq!(encoded.trailers, tonic.trailers);
    let status = grpc_status(&encoded).ok_or_else(|| eyre!("missing status"))?;
    assert_eq!(status.code(), tonic::Code::OutOfRange);
    assert_eq!(encoded.bytes().len(), EncodedBlock::PREFIX_LEN + limit);
    Ok(())
}

#[test]
fn a_refused_message_drops_the_unsent_batch_as_tonic_does() -> eyre::Result<()> {
    let script = [
        Some(Ok(framed(&payload(300, 1))?)),
        Some(Ok(framed(&payload(500, 2))?)),
    ];
    let (encoded, tonic) = both(&script, Some(400))?;
    assert_eq!(encoded, tonic);
    assert_eq!(encoded.frames, Vec::<Bytes>::new());
    assert_eq!(
        grpc_status(&encoded).map(|status| status.code()),
        Some(tonic::Code::OutOfRange)
    );
    Ok(())
}

#[test]
fn encoding_keeps_compact_block_bytes_including_unknown_fields() -> eyre::Result<()> {
    // Height 42 followed by an unknown field, which decoding and re-encoding would discard.
    let stored: &'static [u8] = &[0x10, 42, 0x78, 1];
    let block = framed(stored)?;
    assert_eq!(block.payload(), Bytes::from_static(stored));
    let (encoded, _) = both(&[Some(Ok(block))], None)?;
    assert_eq!(encoded.bytes(), [0, 0, 0, 0, 4, 0x10, 42, 0x78, 1]);
    assert_eq!(CompactBlock::decode(&encoded.bytes()[5..])?.height, 42);

    let block = CompactBlock {
        height: 7,
        ..CompactBlock::default()
    };
    let encoded = EncodedBlock::encode(&block)?;
    assert_eq!(encoded.payload(), block.encode_to_vec());
    assert_eq!(
        EncodedBlock::from_length_prefixed(encoded.message.clone()),
        Ok(encoded)
    );
    Ok(())
}

#[test]
fn length_prefixed_messages_must_be_uncompressed_and_exactly_as_long_as_stated() {
    let refused = |message: &'static [u8]| {
        EncodedBlock::from_length_prefixed(Bytes::from_static(message)) == Err(InvalidEncodedBlock)
    };
    assert!(refused(&[]));
    assert!(refused(&[0, 0, 0, 0]));
    assert!(refused(&[1, 0, 0, 0, 1, 9]));
    assert!(refused(&[0, 0, 0, 0, 2, 9]));
    assert!(refused(&[0, 0, 0, 0, 0, 9]));
    assert!(!refused(&[0, 0, 0, 0, 0]));
    assert!(!refused(&[0, 0, 0, 0, 1, 9]));
}

#[test]
fn a_slow_reader_pins_only_the_message_it_was_handed() -> eyre::Result<()> {
    let first = framed(&payload(97_000, 1))?;
    let sent = first.as_length_prefixed().clone();
    let script = [Some(Ok(first)), Some(Ok(framed(&payload(97_000, 2))?))];
    let mut body = pin!(EncodedBlockBody::new(
        Box::pin(Scripted(script.into_iter().collect())),
        None
    ));
    let Poll::Ready(Some(frame)) = body
        .as_mut()
        .poll_frame(&mut Context::from_waker(Waker::noop()))
    else {
        return Err(eyre!("expected the first message"));
    };
    let frame = frame?
        .into_data()
        .map_err(|_| eyre!("expected message bytes"))?;
    assert_eq!(frame.as_ptr(), sent.as_ptr());
    assert!(!sent.is_unique());
    drop(frame);
    assert!(sent.is_unique(), "the body must not keep a sent message");
    Ok(())
}

#[test]
fn the_body_returns_small_messages_in_the_same_chunks_as_tonic() -> eyre::Result<()> {
    let mut script = Vec::new();
    for tag in 0..200_u8 {
        script.push(Some(Ok(framed(&payload(
            1_500 + usize::from(tag) * 7,
            tag,
        ))?)));
        if tag % 37 == 0 {
            script.push(None);
        }
    }
    let (encoded, tonic) = both(&script, None)?;
    assert_eq!(encoded, tonic);
    assert!(
        encoded
            .frames
            .iter()
            .all(|frame| frame.len() < 2 * BATCH_BYTES)
    );
    Ok(())
}

#[test]
fn only_messages_of_at_least_the_batch_size_are_sent_alone() -> eyre::Result<()> {
    let below = framed(&payload(BATCH_BYTES - EncodedBlock::PREFIX_LEN - 1, 1))?;
    let at = framed(&payload(BATCH_BYTES - EncodedBlock::PREFIX_LEN, 2))?;
    let small = framed(&payload(10, 3))?;
    let script = [
        Some(Ok(small.clone())),
        Some(Ok(below)),
        Some(Ok(small.clone())),
        Some(Ok(at.clone())),
    ];
    let (encoded, _) = both(&script, None)?;
    let lengths: Vec<_> = encoded.frames.iter().map(Bytes::len).collect();
    let small_len = small.as_length_prefixed().len();
    assert_eq!(
        lengths,
        [small_len + (BATCH_BYTES - 1), small_len, BATCH_BYTES]
    );
    assert_eq!(encoded.frames[2].as_ptr(), at.as_length_prefixed().as_ptr());
    Ok(())
}

const CAP: usize = BoundedChunkRetention::MAX_ALLOCATION_BYTES;

/// The frames the body sends for one large message.
fn large_message_frames(block: EncodedBlock) -> eyre::Result<Vec<Bytes>> {
    let (encoded, tonic) = both(&[Some(Ok(block))], None)?;
    assert_eq!(encoded.bytes(), tonic.bytes());
    assert_eq!(encoded.trailers, tonic.trailers);
    Ok(encoded.frames)
}

fn in_allocation(payload: &[u8], allocation: usize) -> eyre::Result<EncodedBlock> {
    let block = framed(payload)?;
    Ok(EncodedBlock::from_length_prefixed_in(
        block.as_length_prefixed().clone(),
        allocation,
    )?)
}

#[test]
fn a_large_message_in_an_allocation_at_the_bounds_is_sent_as_itself() -> eyre::Result<()> {
    let at_cap = in_allocation(&payload(CAP / 2, 1), CAP)?;
    let at_twice = in_allocation(&payload(40_000, 2), 2 * (40_000 + EncodedBlock::PREFIX_LEN))?;
    for block in [at_cap, at_twice] {
        let frames = large_message_frames(block.clone())?;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].as_ptr(), block.as_length_prefixed().as_ptr());
    }
    Ok(())
}

#[test]
fn a_large_message_over_the_bound_or_of_unknown_allocation_is_sent_as_copied_pieces()
-> eyre::Result<()> {
    let over = in_allocation(&payload(97_000, 2), CAP + 1)?;
    let unknown = EncodedBlock::from_length_prefixed(over.as_length_prefixed().clone())?;
    let oversized = framed(&payload(CAP + 1, 3))?;
    let sparse_len = 40_000;
    let sparse = in_allocation(
        &payload(sparse_len, 4),
        2 * (sparse_len + EncodedBlock::PREFIX_LEN) + 1,
    )?;
    for block in [over, unknown, oversized, sparse] {
        let frames = large_message_frames(block.clone())?;
        let message = block.as_length_prefixed();
        assert_eq!(frames.concat(), message.as_ref());
        assert_eq!(
            frames.len(),
            message.len().div_ceil(COPIED_PIECE_BYTES),
            "pieces of at most 64 KiB"
        );
        assert!(frames.iter().all(|frame| {
            let range = message.as_ptr_range();
            frame.len() <= COPIED_PIECE_BYTES && !range.contains(&frame.as_ptr())
        }));
    }
    Ok(())
}

#[test]
fn a_sent_batch_owns_an_allocation_of_its_length() -> eyre::Result<()> {
    // A full batch, then one sent early while the source waits.
    let script = [
        Some(Ok(framed(&payload(20_000, 1))?)),
        Some(Ok(framed(&payload(20_000, 2))?)),
        Some(Ok(framed(&payload(300, 3))?)),
        None,
    ];
    let mut body = pin!(EncodedBlockBody::new(
        Box::pin(Scripted(script.into_iter().collect())),
        None
    ));
    for expected_len in [
        2 * (20_000 + EncodedBlock::PREFIX_LEN),
        300 + EncodedBlock::PREFIX_LEN,
    ] {
        let Poll::Ready(Some(frame)) = body
            .as_mut()
            .poll_frame(&mut Context::from_waker(Waker::noop()))
        else {
            return Err(eyre!("expected a batch"));
        };
        let batch = frame?
            .into_data()
            .map_err(|_| eyre!("expected message bytes"))?;
        assert_eq!(batch.len(), expected_len);
        let owned = batch
            .try_into_mut()
            .map_err(|_| eyre!("the body kept a handle on the batch"))?;
        assert_eq!(owned.capacity(), expected_len);
    }
    Ok(())
}

/// A request body holding one gRPC message.
struct OneMessage(Option<Bytes>);

impl http_body::Body for OneMessage {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(
            self.get_mut()
                .0
                .take()
                .map(|message| Ok(http_body::Frame::data(message))),
        )
    }
}

/// Serves one empty block, or refuses the call.
struct OneBlock(bool);

#[tonic::async_trait]
impl EncodedBlockRange for OneBlock {
    async fn get_encoded_block_range(
        &self,
        _request: Request<BlockRange>,
    ) -> Result<Response<EncodedBlockRangeStream>, Status> {
        if !self.0 {
            return Err(Status::out_of_range("above the tip"));
        }
        let block = EncodedBlock::encode(&CompactBlock::default())
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(Box::pin(tonic::codegen::tokio_stream::once(
            Ok(block),
        ))))
    }
}

fn served(range: &dyn EncodedBlockRange) -> eyre::Result<http::Response<tonic::body::Body>> {
    let request = BlockRange::default().encode_to_vec();
    let mut message = vec![0];
    message.extend_from_slice(&u32::try_from(request.len())?.to_be_bytes());
    message.extend_from_slice(&request);
    let request = http::Request::builder()
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(OneMessage(Some(message.into())))?;
    let mut grpc = Grpc::new(EncodedBlockCodec);
    let mut call = pin!(server_streaming(
        &mut grpc,
        range,
        request,
        EnabledCompressionEncodings::default(),
        None,
    ));
    match call.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(response) => Ok(response),
        Poll::Pending => Err(eyre!("the call did not complete")),
    }
}

#[test]
fn only_the_uncompressed_range_body_carries_the_retention_marker() -> eyre::Result<()> {
    let marked = |response: &http::Response<tonic::body::Body>| {
        response
            .extensions()
            .get::<BoundedChunkRetention>()
            .is_some()
    };
    assert!(marked(&served(&OneBlock(true))?));
    assert!(!marked(&served(&OneBlock(false))?));
    Ok(())
}
