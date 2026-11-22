use std::{future::Future, pin::Pin};

use prost::bytes::{BufMut, Bytes};
use tonic::{
    Request, Response, Status,
    codec::{Codec, EncodeBuf, Encoder},
    codegen::BoxStream,
    server::ServerStreamingService,
};
use tonic_prost::ProstDecoder;

use crate::compat::lightwalletd::BlockRange;

/// A stream of complete lightwalletd `CompactBlock` protobuf messages.
/// Each buffer contains one message payload. Tonic supplies the gRPC frame.
pub type EncodedBlockRangeStream = BoxStream<Bytes>;

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
    type Response = Bytes;
    type ResponseStream = EncodedBlockRangeStream;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<BlockRange>) -> Self::Future {
        (*self).get_encoded_block_range(request)
    }
}

pub(crate) struct EncodedBlockCodec;

impl Codec for EncodedBlockCodec {
    type Encode = Bytes;
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
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, block: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&block);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use eyre::eyre;
    use prost::Message;
    use tonic::{
        codec::{EncodeBody, SingleMessageCompressionOverride},
        codegen::{Body, tokio_stream},
    };

    use super::{Bytes, EncodedBlockCodec};
    use crate::compat::lightwalletd::CompactBlock;

    #[test]
    fn grpc_frame_preserves_compact_block_bytes_including_unknown_fields() -> eyre::Result<()> {
        // Height 42 followed by an unknown field, which decoding and re-encoding would discard.
        let block = Bytes::from_static(&[0x10, 42, 0x78, 1]);
        let mut body = pin!(EncodeBody::new_server(
            EncodedBlockCodec,
            tokio_stream::once(Ok(block.clone())),
            None,
            SingleMessageCompressionOverride::default(),
            None,
        ));
        let Poll::Ready(Some(frame)) = body
            .as_mut()
            .poll_frame(&mut Context::from_waker(Waker::noop()))
        else {
            return Err(eyre!("expected compact block frame"));
        };
        let frame = frame?
            .into_data()
            .map_err(|_| eyre!("expected message payload"))?;
        assert_eq!(&frame[..5], &[0, 0, 0, 0, 4]);
        assert_eq!(&frame[5..], &block[..]);
        assert_eq!(CompactBlock::decode(&frame[5..])?.height, 42);
        Ok(())
    }
}
