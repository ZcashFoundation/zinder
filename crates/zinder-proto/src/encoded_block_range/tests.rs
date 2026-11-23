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
    BATCH_BYTES, Bytes, CompactBlock, EncodedBlock, EncodedBlockBody, EncodedBlockCodec,
    InvalidEncodedBlock, Status, http,
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
    Ok(EncodedBlock::from_length_prefixed(message.into())?)
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
fn messages_of_a_batch_or_more_reach_the_transport_without_a_copy() -> eyre::Result<()> {
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
fn small_messages_reach_the_transport_in_the_same_chunks_as_tonic() -> eyre::Result<()> {
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
