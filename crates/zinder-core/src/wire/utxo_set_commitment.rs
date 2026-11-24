//! Transparent UTXO-set commitment element encoder.
//!
//! Owns the snapshot-immutable per-UTXO preimage and the 16-byte `BLAKE2b`
//! personalization tag that domain-separates the commitment's element
//! expansion ([`crate::expand_lthash16_element`]). The
//! accumulator type in [`crate::utxo_set_commitment`] feeds every element it
//! folds through [`encode_utxo_set_commitment_element`]; inline element
//! serialization elsewhere is a forbidden pattern enforced by
//! `crates/zinder-core/tests/integration/wire_invariants.rs`.
//!
//! The preimage is fixed-width little-endian:
//!
//! ```text
//! network_id(u32 LE) ‖ encoding_version(u8) ‖ txid(32, internal order)
//!   ‖ output_index(u32 LE) ‖ value_zat(u64 LE)
//!   ‖ script_len(u32 LE) ‖ raw_scriptPubKey ‖ block_height(u32 LE)
//! ```
//!
//! `txid` is the internal [`TransactionId`] byte order (the bytes
//! [`TransactionId::as_bytes`] returns), not the RPC byte-reversed form. The
//! network identifier and encoding version live in the preimage rather than the
//! BLAKE2 personalization: `BLAKE2b`'s `personal` field is 16 bytes and the fixed
//! tag already fills all 16, and keeping network/version in the bytes lets a
//! third party reproduce the preimage from a plain UTXO dump without BLAKE2
//! salt plumbing.

use crate::{BlockHeight, TransactionId, TransparentOutPoint};

/// `BLAKE2b` root personalization tag for the transparent UTXO-set commitment.
///
/// Exactly 16 bytes (the full `BLAKE2b` `personal` field). Domain-separates the
/// commitment's element expansion from every other BLAKE2 use in the codebase.
/// Its trailing `2` matches [`crate::UtxoSetCommitmentScheme::LtHash16ChaCha12`].
pub const UTXO_SET_COMMITMENT_PERSONAL: [u8; 16] = *b"ZinderUtxoRoot_2";

/// Encoding version embedded in every commitment element preimage.
///
/// A new value is a new snapshot scheme, not a reinterpretation of this one.
pub const UTXO_SET_COMMITMENT_ENCODING_VERSION: u8 = 1;

/// One transparent output to fold into the commitment accumulator.
///
/// The fields are exactly what the current-UTXO projection decode yields per
/// surviving settled-tip row, so the store fold constructs this without any
/// extra read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UtxoSetCommitmentElement<'a> {
    /// Stable numeric network identifier ([`crate::Network::id`]).
    pub network_id: u32,
    /// Output identity (transaction id in internal byte order plus index).
    pub outpoint: TransparentOutPoint,
    /// Output value in zatoshi.
    pub value_zat: u64,
    /// Raw `scriptPubKey` bytes, committed length-prefixed and verbatim.
    pub script_pub_key: &'a [u8],
    /// Height of the block that mined the output.
    pub block_height: BlockHeight,
}

/// Serializes one transparent output into its canonical commitment preimage.
///
/// The byte layout is the snapshot-immutable contract documented at the module
/// level. The returned bytes are expanded directly; no field is hashed or
/// projected first.
#[must_use]
pub fn encode_utxo_set_commitment_element(element: &UtxoSetCommitmentElement<'_>) -> Vec<u8> {
    let txid: [u8; 32] = encode_internal_commitment_transaction_id(element.outpoint.transaction_id);
    let script_len = u32::try_from(element.script_pub_key.len()).unwrap_or(u32::MAX);

    let mut preimage = Vec::with_capacity(57 + element.script_pub_key.len());
    preimage.extend_from_slice(&element.network_id.to_le_bytes());
    preimage.push(UTXO_SET_COMMITMENT_ENCODING_VERSION);
    preimage.extend_from_slice(&txid);
    preimage.extend_from_slice(&element.outpoint.output_index.to_le_bytes());
    preimage.extend_from_slice(&element.value_zat.to_le_bytes());
    preimage.extend_from_slice(&script_len.to_le_bytes());
    preimage.extend_from_slice(element.script_pub_key);
    preimage.extend_from_slice(&element.block_height.value().to_le_bytes());
    preimage
}

/// Returns the internal-order transaction-id bytes the commitment preimage
/// commits to.
///
/// The commitment binds the bytes [`TransactionId::as_bytes`] returns directly;
/// this wrapper keeps the conversion reachable from one named function.
#[must_use]
const fn encode_internal_commitment_transaction_id(transaction_id: TransactionId) -> [u8; 32] {
    transaction_id.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outpoint() -> TransparentOutPoint {
        let mut txid_bytes = [0u8; 32];
        for (index, byte) in txid_bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index).unwrap_or(0);
        }
        TransparentOutPoint::new(TransactionId::from_bytes(txid_bytes), 2)
    }

    #[test]
    fn personal_tag_is_sixteen_bytes() {
        assert_eq!(UTXO_SET_COMMITMENT_PERSONAL.len(), 16);
    }

    #[test]
    fn encodes_sample_utxo_to_exact_preimage() {
        let script_pub_key: Vec<u8> = {
            let mut script = vec![0x76, 0xa9, 0x14];
            script.extend(std::iter::repeat_n(0xab, 20));
            script.extend_from_slice(&[0x88, 0xac]);
            script
        };
        let element = UtxoSetCommitmentElement {
            network_id: 1,
            outpoint: sample_outpoint(),
            value_zat: 50_000,
            script_pub_key: &script_pub_key,
            block_height: BlockHeight::new(419_200),
        };

        let preimage = encode_utxo_set_commitment_element(&element);
        let expected = hex::decode(concat!(
            "01000000",
            "01",
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "02000000",
            "50c3000000000000",
            "19000000",
            "76a914abababababababababababababababababababab88ac",
            "80650600",
        ))
        .unwrap_or_default();
        assert_eq!(preimage, expected);
    }
}
