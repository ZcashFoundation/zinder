#![allow(
    missing_docs,
    reason = "Integration test names describe the canonical writer fence wire contract under test."
)]

use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, ChainEpochId,
};
use zinder_proto::{v1::ingest::CanonicalWriterFence, wire::canonical_writer_fence_message};

#[test]
fn canonical_writer_fence_message_carries_every_fence_field() {
    let mut tip_hash = [0x11; 32];
    tip_hash[0] = 0x01;
    let fence = canonical_writer_fence_message(
        ChainEpochId::new(7),
        7,
        BlockId::new(BlockHeight::new(42), BlockHash::from_bytes(tip_hash)),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            43,
            [0x22; 32],
        ),
    );

    assert_eq!(
        fence,
        CanonicalWriterFence {
            chain_epoch_id: 7,
            event_sequence: 7,
            visible_tip_height: 42,
            visible_tip_hash: tip_hash.to_vec(),
            canonical_sequence_digest: vec![0x22; 32],
            visible_block_count: 43,
        }
    );
}
