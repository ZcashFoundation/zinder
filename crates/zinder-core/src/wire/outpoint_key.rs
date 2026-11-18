//! Transparent outpoint encoder for materialized-view primary keys.
//!
//! The transparent-outpoint-spend projection keys its rows on the spent
//! outpoint: the creating transaction id followed by the big-endian output
//! index. Lexicographic order groups every output of one transaction together,
//! which is irrelevant to point lookups but keeps the layout self-describing.
//!
//! Materialized-view store boundaries build outpoint keys through this encoder rather than
//! inlining `outpoint.transaction_id.as_bytes()` with
//! `outpoint.output_index.to_be_bytes()`, keeping the 36-byte layout in one
//! place per the key-codec convention in ADR-0020.

use crate::TransparentOutPoint;
use crate::wire::{
    WireDecodeError, decode_internal_transaction_id, encode_internal_transaction_id,
};

/// Number of bytes a transparent outpoint occupies in a materialized-view key.
pub const OUTPOINT_KEY_LEN: usize = 36;

const TRANSACTION_ID_LEN: usize = 32;

/// Encodes a transparent outpoint into its materialized-view key bytes.
#[must_use]
pub fn encode_outpoint_key(outpoint: TransparentOutPoint) -> [u8; OUTPOINT_KEY_LEN] {
    let mut key = [0u8; OUTPOINT_KEY_LEN];
    key[..TRANSACTION_ID_LEN]
        .copy_from_slice(&encode_internal_transaction_id(outpoint.transaction_id));
    key[TRANSACTION_ID_LEN..].copy_from_slice(&outpoint.output_index.to_be_bytes());
    key
}

/// Decodes materialized-view key bytes back into a transparent outpoint.
///
/// Returns [`WireDecodeError::InvalidLength`] when `bytes` is not exactly
/// [`OUTPOINT_KEY_LEN`] bytes long.
pub fn decode_outpoint_key(bytes: &[u8]) -> Result<TransparentOutPoint, WireDecodeError> {
    let array: [u8; OUTPOINT_KEY_LEN] =
        bytes
            .try_into()
            .map_err(|_| WireDecodeError::InvalidLength {
                expected: OUTPOINT_KEY_LEN,
                actual: bytes.len(),
            })?;
    let transaction_id = decode_internal_transaction_id(&array[..TRANSACTION_ID_LEN])?;
    let index_bytes: [u8; 4] =
        array[TRANSACTION_ID_LEN..]
            .try_into()
            .map_err(|_| WireDecodeError::InvalidLength {
                expected: 4,
                actual: OUTPOINT_KEY_LEN - TRANSACTION_ID_LEN,
            })?;
    Ok(TransparentOutPoint::new(
        transaction_id,
        u32::from_be_bytes(index_bytes),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionId;

    fn sample_outpoint() -> TransparentOutPoint {
        TransparentOutPoint::new(TransactionId::from_bytes([9u8; 32]), 7)
    }

    #[test]
    fn round_trip_preserves_outpoint() {
        let encoded = encode_outpoint_key(sample_outpoint());
        let decoded = decode_outpoint_key(&encoded);
        assert!(matches!(decoded, Ok(outpoint) if outpoint == sample_outpoint()));
    }

    #[test]
    fn outputs_of_one_transaction_sort_together() {
        let first = encode_outpoint_key(TransparentOutPoint::new(
            TransactionId::from_bytes([1u8; 32]),
            0,
        ));
        let second = encode_outpoint_key(TransparentOutPoint::new(
            TransactionId::from_bytes([1u8; 32]),
            1,
        ));
        let other = encode_outpoint_key(TransparentOutPoint::new(
            TransactionId::from_bytes([2u8; 32]),
            0,
        ));
        assert!(first < second);
        assert!(second < other);
    }

    #[test]
    fn decode_rejects_wrong_length() {
        let outcome = decode_outpoint_key(&[0u8; 10]);
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength {
                expected: OUTPOINT_KEY_LEN,
                actual: 10
            })
        ));
    }
}
