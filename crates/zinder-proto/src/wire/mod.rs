//! Proto-enum mappings between Zinder native types and generated wire types.
//!
//! Every native-to-proto enum translation lives in this module. Each
//! submodule owns one concept (currently privacy-shape) and exposes
//! `encode_*` / `decode_*` functions named after the convention they
//! implement. Inline `match shape { PrivacyShape::TransparentOnly => ... }`
//! tables outside this module are a forbidden pattern; call the encoder.
//!
//! The pure-domain side (proto-agnostic key codecs, byte-order helpers)
//! lives in `crates/zinder-core/src/wire/`. This module is its proto-aware
//! twin: it owns the translations whose return type is one of the
//! generated `zinder_proto::v1::*` enums.

mod privacy_shape;
mod transaction_lookup;
mod transparent_delta_kind;
mod utxo_set_commitment;
mod wallet;

pub use privacy_shape::{decode_privacy_shape, encode_privacy_shape};
pub use transaction_lookup::{
    CANONICAL_HISTORY_FIRST_AVAILABLE_HEIGHT_KEY, TRANSACTION_ABSENCE_CHAIN_EPOCH_ID_KEY,
    TRANSACTION_ABSENCE_COVERS_MEMPOOL_KEY, TRANSACTION_ABSENCE_FIRST_COVERED_HEIGHT_KEY,
    TRANSACTION_ABSENCE_VISIBLE_TIP_HEIGHT_KEY, TransactionAbsence, TransactionLookupMetadataError,
    canonical_history_first_available_height, canonical_history_unavailable_status,
};
pub use transparent_delta_kind::{
    TRANSPARENT_DELTA_KIND_RECEIVED_BYTE, TRANSPARENT_DELTA_KIND_SPENT_BYTE,
    UnknownTransparentDeltaKindByte, decode_transparent_delta_kind,
};
pub use utxo_set_commitment::{
    TransparentUtxoSetCommitmentDecodeError, TransparentUtxoSetCommitmentEncodeError,
    decode_transparent_utxo_set_commitment, encode_transparent_utxo_set_commitment,
};
pub use wallet::{
    WalletWireDecodeError, chain_epoch_from_message, chain_epoch_message,
    compact_block_from_message, compact_block_message, compact_transaction_data_from_message,
    compact_transaction_data_message, decode_compact_block, encode_compact_block,
    mempool_entry_from_message, mempool_entry_message, outpoint_message,
    transparent_mempool_output_from_message, transparent_mempool_spend_from_message,
};
