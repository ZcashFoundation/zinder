//! Block-height key encoders for materialized-view column families.
//!
//! Materialized-view consumers persist per-height rows in `RocksDB` column families. The
//! ascending encoder lays heights down in lexicographic order so a forward
//! range scan returns oldest-first; the descending encoder returns
//! `u32::MAX - height` so the same scan returns newest-first. Both encoders
//! produce four-byte big-endian payloads suitable as full keys or as the
//! leading discriminator in a composite key.
//!
//! Inline `height.to_be_bytes()` at materialized-view boundaries is a forbidden
//! pattern enforced by
//! `crates/zinder-core/tests/integration/wire_invariants.rs`.

use crate::BlockHeight;
use crate::wire::WireDecodeError;

/// Number of bytes a height occupies in a materialized-view key.
pub const HEIGHT_KEY_LEN: usize = 4;

/// Encodes a block height into its ascending materialized-view key bytes.
///
/// The output is the height's big-endian byte representation. `RocksDB` scans
/// keys in lexicographic order, so the encoded payload sorts oldest-first.
#[must_use]
pub const fn encode_height_key_ascending(height: BlockHeight) -> [u8; HEIGHT_KEY_LEN] {
    height.value().to_be_bytes()
}

/// Decodes the ascending materialized-view key bytes back into a block height.
///
/// Returns [`WireDecodeError::InvalidLength`] when `bytes` is not exactly
/// [`HEIGHT_KEY_LEN`] bytes long.
pub fn decode_height_key_ascending(bytes: &[u8]) -> Result<BlockHeight, WireDecodeError> {
    let array = take_height_bytes(bytes)?;
    Ok(BlockHeight::new(u32::from_be_bytes(array)))
}

/// Returns where a forward scan of ascending height keys starts so that it
/// reaches every key sorting after the key of the height below `from`.
///
/// A key that extends a height key with more bytes, which no writer stores
/// but damage can leave, sorts after that height's key and before the next
/// height's. A scan that starts at the key of `from` skips such a key between
/// `from - 1` and `from`, so a scan that resumes at `from` starts at the
/// immediate successor of the key of `from - 1` instead. For height zero it
/// is empty, the position before every key.
#[must_use]
pub fn ascending_height_key_scan_start(from: BlockHeight) -> Vec<u8> {
    from.value()
        .checked_sub(1)
        .map_or_else(Vec::new, |previous| {
            let mut start = Vec::with_capacity(HEIGHT_KEY_LEN + 1);
            start.extend_from_slice(&encode_height_key_ascending(BlockHeight::new(previous)));
            start.push(0);
            start
        })
}

/// Encodes a block height into its descending materialized-view key bytes.
///
/// The output is `u32::MAX - height` in big-endian, so `RocksDB`
/// lexicographic order sorts the resulting payloads newest-first. Use
/// this as the leading discriminator in column families that serve
/// time-descending range scans (for example a "recent transactions"
/// projection).
#[must_use]
pub const fn encode_height_key_descending(height: BlockHeight) -> [u8; HEIGHT_KEY_LEN] {
    (u32::MAX - height.value()).to_be_bytes()
}

/// Decodes the descending materialized-view key bytes back into a block height.
///
/// Inverts [`encode_height_key_descending`]: returns the original height that
/// was encoded as `u32::MAX - height`. Returns [`WireDecodeError::InvalidLength`]
/// when `bytes` is not exactly [`HEIGHT_KEY_LEN`] bytes long.
pub fn decode_height_key_descending(bytes: &[u8]) -> Result<BlockHeight, WireDecodeError> {
    let array = take_height_bytes(bytes)?;
    Ok(BlockHeight::new(u32::MAX - u32::from_be_bytes(array)))
}

fn take_height_bytes(bytes: &[u8]) -> Result<[u8; HEIGHT_KEY_LEN], WireDecodeError> {
    bytes
        .try_into()
        .map_err(|_| WireDecodeError::InvalidLength {
            expected: HEIGHT_KEY_LEN,
            actual: bytes.len(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascending_keys_sort_lexicographically_by_height() {
        let lo = encode_height_key_ascending(BlockHeight::new(100));
        let hi = encode_height_key_ascending(BlockHeight::new(200));
        assert!(lo < hi);
    }

    #[test]
    fn descending_keys_sort_lexicographically_newest_first() {
        let newer = encode_height_key_descending(BlockHeight::new(200));
        let older = encode_height_key_descending(BlockHeight::new(100));
        assert!(newer < older);
    }

    #[test]
    fn ascending_round_trip_matches_height() {
        let height = BlockHeight::new(2_700_000);
        let bytes = encode_height_key_ascending(height);
        let outcome = decode_height_key_ascending(&bytes);
        assert!(matches!(outcome, Ok(decoded) if decoded == height));
    }

    #[test]
    fn descending_round_trip_matches_height() {
        let height = BlockHeight::new(2_700_000);
        let bytes = encode_height_key_descending(height);
        let outcome = decode_height_key_descending(&bytes);
        assert!(matches!(outcome, Ok(decoded) if decoded == height));
    }

    #[test]
    fn ascending_decode_rejects_wrong_length() {
        let too_short = [0u8; 3];
        let outcome = decode_height_key_ascending(&too_short);
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength {
                expected: HEIGHT_KEY_LEN,
                actual: 3
            })
        ));
    }

    #[test]
    fn descending_decode_rejects_wrong_length() {
        let too_long = [0u8; 8];
        let outcome = decode_height_key_descending(&too_long);
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength {
                expected: HEIGHT_KEY_LEN,
                actual: 8
            })
        ));
    }

    #[test]
    fn a_scan_start_sorts_after_the_height_below_and_before_every_key_extending_it() {
        let previous = encode_height_key_ascending(BlockHeight::new(100));
        let mut extended = previous.to_vec();
        extended.push(0);
        let start = ascending_height_key_scan_start(BlockHeight::new(101));

        assert!(previous.as_slice() < start.as_slice());
        assert!(start.as_slice() <= extended.as_slice());
        assert!(start.as_slice() < encode_height_key_ascending(BlockHeight::new(101)).as_slice());
        assert_eq!(
            ascending_height_key_scan_start(BlockHeight::new(0)),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn ascending_encoding_for_zero_height_is_all_zero_bytes() {
        assert_eq!(encode_height_key_ascending(BlockHeight::new(0)), [0u8; 4]);
    }

    #[test]
    fn descending_encoding_for_max_height_is_all_zero_bytes() {
        assert_eq!(
            encode_height_key_descending(BlockHeight::new(u32::MAX)),
            [0u8; 4]
        );
    }
}
