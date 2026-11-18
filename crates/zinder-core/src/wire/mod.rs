//! Native to wire dialect conversions for Zinder domain types.
//!
//! Every native-to-wire translation lives in this module. Each submodule owns
//! one concept (`chain_name`, `transaction_id`, `block_hash`, `auth_digest`,
//! `wtxid`, `branch_id`) across all dialects that use it. Function names
//! disclose the convention: `encode_internal_*` and `decode_internal_*` for
//! the storage form and the lightwalletd-compat `bytes` boundary;
//! `encode_rpc_*_hex` and `decode_rpc_*_hex` for the RPC byte order hex form
//! every public wallet UI, block explorer, log record, and Zcash JSON-RPC
//! reply uses; `encode_rpc_*_bytes` and `decode_rpc_*_bytes` for RPC byte
//! order carried as raw bytes (Zebra's indexer gRPC fills hash `bytes` fields
//! with `bytes_in_display_order`, and lightwalletd fills
//! `SubtreeRoot.completingBlockHash` the same way).
//!
//! RPC byte order is defined normatively in the Zcash protocol specification
//! at protocol.tex:1127 (`\rpcByteOrder`) and used at protocol.tex:4036. It
//! is the byte-reversal of the internal SHA-256d output form Zinder stores.
//!
//! [`encode_branch_id_hex`] drops the `_internal_`/`_rpc_` qualifier because
//! consensus branch ids have a single canonical wire form (the `{:08x}`
//! lowercase hex string used by lightwalletd and Zebra `getblockchaininfo`);
//! there is no byte-order companion to disambiguate against. The single-form
//! chain-name encoders ([`encode_bip70_chain_name`],
//! [`encode_zinder_native_chain_name`]) follow the same pattern.
//!
//! Adding a new wire field or a new ingress dialect MUST route through a
//! function here. Inline `transaction_id.as_bytes()`, inline
//! `format!("{:08x}", ...)`, and inline byte reversal at wire boundaries are
//! forbidden patterns; the structural test at
//! `crates/zinder-core/tests/wire_invariants.rs` enforces this.

mod address_script_hash;
mod auth_digest;
mod block_hash;
mod branch_id;
mod chain_name;
mod height_key;
mod in_block_position;
mod merkle_root;
mod outpoint_key;
mod transaction_id;
mod unix_seconds;
mod utxo_set_commitment;
mod wtxid;

pub use address_script_hash::{
    ADDRESS_SCRIPT_HASH_LEN, decode_address_script_hash, encode_address_script_hash,
};
pub use auth_digest::{
    decode_internal_auth_digest, decode_rpc_auth_digest_bytes, decode_rpc_auth_digest_hex,
    encode_internal_auth_digest, encode_rpc_auth_digest_hex,
};
pub use block_hash::{
    decode_internal_block_hash, decode_rpc_block_hash_bytes, decode_rpc_block_hash_hex,
    encode_internal_block_hash, encode_rpc_block_hash_bytes, encode_rpc_block_hash_hex,
};
pub use branch_id::{decode_branch_id_hex, encode_branch_id_hex};
pub use chain_name::{
    decode_zinder_native_chain_name, encode_bip70_chain_name, encode_zinder_native_chain_name,
};
pub use height_key::{
    HEIGHT_KEY_LEN, decode_height_key_ascending, decode_height_key_descending,
    encode_height_key_ascending, encode_height_key_descending,
};
pub use in_block_position::{
    IN_BLOCK_POSITION_KEY_LEN, decode_in_block_position, encode_in_block_position,
};
pub use merkle_root::{decode_rpc_merkle_root_hex, encode_rpc_merkle_root_hex};
pub use outpoint_key::{OUTPOINT_KEY_LEN, decode_outpoint_key, encode_outpoint_key};
pub use transaction_id::{
    decode_internal_transaction_id, decode_rpc_transaction_id_bytes, decode_rpc_transaction_id_hex,
    encode_internal_transaction_id, encode_rpc_transaction_id_hex,
};
pub use unix_seconds::{UNIX_SECONDS_KEY_LEN, decode_unix_seconds, encode_unix_seconds};
pub use utxo_set_commitment::{
    UTXO_SET_COMMITMENT_ENCODING_VERSION, UTXO_SET_COMMITMENT_PERSONAL, UtxoSetCommitmentElement,
    encode_utxo_set_commitment_element,
};
pub use wtxid::{
    decode_internal_wtxid, decode_rpc_wtxid_hex, encode_internal_wtxid, encode_rpc_wtxid_hex,
};

/// Errors returned by `decode_*` functions in [`crate::wire`].
///
/// Encode operations are infallible by construction (the input is a typed
/// domain value with a fixed byte length). Decode operations may receive
/// arbitrary bytes or strings and must report the failure mode the caller can
/// recover from.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WireDecodeError {
    /// Input byte slice was not the expected length for the target type.
    #[error("wire decode expected {expected} bytes, received {actual}")]
    InvalidLength {
        /// Number of bytes the target type requires.
        expected: usize,
        /// Number of bytes the caller supplied.
        actual: usize,
    },

    /// Input hex string failed to decode.
    #[error("wire decode invalid hex: {reason}")]
    InvalidHex {
        /// Human-readable description of the hex decode failure.
        reason: String,
    },

    /// Input did not match a known enum discriminant on the named dialect.
    #[error("wire decode unrecognized {dialect} enum discriminant: {discriminant}")]
    UnrecognizedEnumDiscriminant {
        /// Wire dialect that produced the input (for example "lightwalletd", "zinder-native").
        dialect: &'static str,
        /// Discriminant value that did not match any known variant.
        discriminant: i32,
    },

    /// Input string did not match any known value for the named dialect.
    #[error("wire decode unrecognized {dialect} string: {input}")]
    UnrecognizedString {
        /// Wire dialect that produced the input.
        dialect: &'static str,
        /// String the caller supplied that did not match any known value.
        input: String,
    },
}
