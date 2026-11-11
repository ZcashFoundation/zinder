//! Canonical writer fence wire mapping.
//!
//! `CanonicalControl.WriterStatus` publishes the writer's visible canonical
//! fence as one [`CanonicalWriterFence`] message. Every party that encodes or
//! attests a fence builds the message here, so the writer's emission and each
//! reader's expected fence agree field for field.

use zinder_core::{
    BlockId, CanonicalBlockFactsSequenceDigest, ChainEpochId, wire::encode_internal_block_hash,
};

use crate::v1::ingest::CanonicalWriterFence;

/// Encodes one canonical fence as its private control-plane message.
///
/// The visible tip hash uses internal byte order. The sequence digest
/// contributes both its committed block count and its digest bytes.
#[must_use]
pub fn canonical_writer_fence_message(
    chain_epoch_id: ChainEpochId,
    event_sequence: u64,
    visible_tip: BlockId,
    sequence_digest: CanonicalBlockFactsSequenceDigest,
) -> CanonicalWriterFence {
    CanonicalWriterFence {
        chain_epoch_id: chain_epoch_id.value(),
        event_sequence,
        visible_tip_height: visible_tip.height.value(),
        visible_tip_hash: encode_internal_block_hash(visible_tip.hash).to_vec(),
        canonical_sequence_digest: sequence_digest.as_bytes().to_vec(),
        visible_block_count: sequence_digest.block_count(),
    }
}
