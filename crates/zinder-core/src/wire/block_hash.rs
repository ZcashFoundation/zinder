//! Block hash conversions across Zcash wire dialects.
//!
//! Block hashes follow the same two-form convention as transaction ids:
//!
//! - **Internal byte order**: the consensus byte order [`crate::BlockHash`]
//!   stores and Zinder's `RocksDB` keys use. Reference: Zcash protocol spec,
//!   protocol.tex:13560-13564.
//! - **RPC byte order**: the byte-reversed display form every Zcash
//!   JSON-RPC reply (`getbestblockhash`, `getblock`, etc.) emits, every
//!   block explorer shows, and the Zcash protocol spec's example block
//!   hashes use. Defined normatively at protocol.tex:1127 (`\rpcByteOrder`)
//!   and used at protocol.tex:4036 ("All block hashes given in this
//!   section are in RPC byte order").
//!
//! Pick the function whose name matches the wire surface:
//! - Lightwalletd-compat `bytes` fields carry internal byte order. Use
//!   [`encode_internal_block_hash`] and [`decode_internal_block_hash`].
//!   Two exceptions follow reference lightwalletd's RPC byte order on `bytes`
//!   fields: `SubtreeRoot.completingBlockHash` in responses
//!   ([`encode_rpc_block_hash_bytes`]) and `GetTreeState(BlockID { height: 0, hash })`
//!   in requests ([`decode_rpc_block_hash_bytes`]).
//! - Native zinder protobuf `string` hash fields, JSON, log records, and
//!   any human-facing surface carry RPC byte order hex. Use
//!   [`encode_rpc_block_hash_hex`] and [`decode_rpc_block_hash_hex`].

use crate::BlockHash;
use crate::wire::WireDecodeError;

const BLOCK_HASH_BYTE_COUNT: usize = 32;
const BLOCK_HASH_HEX_LEN: usize = BLOCK_HASH_BYTE_COUNT * 2;

/// Encode a [`BlockHash`] as internal byte order bytes.
///
/// The output is the byte form the lightwalletd-compat plane carries on
/// every `bytes`-typed hash field and the byte form the canonical storage
/// layer keys by. Exists alongside [`BlockHash::as_bytes`] so reviewers
/// can grep one name when auditing wire emissions.
///
/// Reference: Zcash protocol spec, protocol.tex:13560-13564.
#[must_use]
pub fn encode_internal_block_hash(block_hash: BlockHash) -> [u8; 32] {
    block_hash.as_bytes()
}

/// Decode internal byte order bytes into a [`BlockHash`].
///
/// # Errors
///
/// Returns [`WireDecodeError::InvalidLength`] if the input is not exactly 32
/// bytes.
pub fn decode_internal_block_hash(bytes: &[u8]) -> Result<BlockHash, WireDecodeError> {
    let buffer: [u8; 32] = bytes
        .try_into()
        .map_err(|_| WireDecodeError::InvalidLength {
            expected: BLOCK_HASH_BYTE_COUNT,
            actual: bytes.len(),
        })?;
    Ok(BlockHash::from_bytes(buffer))
}

/// Encode a [`BlockHash`] as a lowercase RPC byte order hex string.
///
/// Produces the canonical 64-character lowercase hex form every Zcash
/// JSON-RPC reply emits, every block explorer renders, and the protocol
/// spec's example block hashes use. The bytes are reversed before hex
/// encoding so the leftmost hex character corresponds to the block hash's
/// high byte in the form readers recognize (the spec describes this as
/// "byte-reversed relative to the normal order for a SHA-256d hash").
///
/// Reference: Zcash protocol spec, term `\rpcByteOrder` (protocol.tex:1127, :4036).
#[must_use]
pub fn encode_rpc_block_hash_hex(block_hash: BlockHash) -> String {
    let mut bytes = block_hash.as_bytes();
    bytes.reverse();
    hex::encode(bytes)
}

/// Decode an RPC byte order hex string into a [`BlockHash`].
///
/// Inverse of [`encode_rpc_block_hash_hex`]. Accepts canonical
/// 64-character lowercase or uppercase hex.
///
/// Reference: Zcash protocol spec, term `\rpcByteOrder` (protocol.tex:1127, :4036).
///
/// # Errors
///
/// Returns [`WireDecodeError::InvalidLength`] if the string is not 64
/// characters, and [`WireDecodeError::InvalidHex`] if it contains non-hex
/// characters.
pub fn decode_rpc_block_hash_hex(input: &str) -> Result<BlockHash, WireDecodeError> {
    if input.len() != BLOCK_HASH_HEX_LEN {
        return Err(WireDecodeError::InvalidLength {
            expected: BLOCK_HASH_HEX_LEN,
            actual: input.len(),
        });
    }
    let mut buffer = [0u8; BLOCK_HASH_BYTE_COUNT];
    hex::decode_to_slice(input, &mut buffer).map_err(|hex_error| WireDecodeError::InvalidHex {
        reason: hex_error.to_string(),
    })?;
    buffer.reverse();
    Ok(BlockHash::from_bytes(buffer))
}

/// Encode a [`BlockHash`] as RPC byte order bytes.
///
/// The raw-bytes analogue of [`encode_rpc_block_hash_hex`]. Reference
/// lightwalletd reverses the block hash into this form for
/// `SubtreeRoot.completingBlockHash`, unlike the other lightwalletd `bytes`
/// hash fields.
///
/// Reference: Zcash protocol spec, term `\rpcByteOrder` (protocol.tex:1127, :4036).
#[must_use]
pub fn encode_rpc_block_hash_bytes(block_hash: BlockHash) -> [u8; 32] {
    let mut bytes = block_hash.as_bytes();
    bytes.reverse();
    bytes
}

/// Decode RPC byte order bytes into a [`BlockHash`].
///
/// The raw-bytes analogue of [`decode_rpc_block_hash_hex`]. Zebra's
/// indexer gRPC surface (`zebra_indexer_rpc`) fills hash `bytes` fields
/// with `bytes_in_display_order`, and reference lightwalletd reads
/// `GetTreeState(BlockID { height: 0, hash })` in RPC byte order, so the
/// input is reversed into internal byte order before constructing the domain
/// value.
///
/// Reference: Zcash protocol spec, term `\rpcByteOrder` (protocol.tex:1127, :4036).
///
/// # Errors
///
/// Returns [`WireDecodeError::InvalidLength`] if the input is not exactly 32
/// bytes.
pub fn decode_rpc_block_hash_bytes(bytes: &[u8]) -> Result<BlockHash, WireDecodeError> {
    let mut buffer: [u8; 32] = bytes
        .try_into()
        .map_err(|_| WireDecodeError::InvalidLength {
            expected: BLOCK_HASH_BYTE_COUNT,
            actual: bytes.len(),
        })?;
    buffer.reverse();
    Ok(BlockHash::from_bytes(buffer))
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Internal byte order form of testnet block 4031230's block hash.
    /// Paired with [`TESTNET_BLOCK_HASH_RPC_HEX`] below; the two are
    /// byte-reversed.
    const TESTNET_BLOCK_HASH_INTERNAL_BYTES: [u8; 32] = [
        0xee, 0xce, 0xfc, 0x22, 0xf4, 0xa0, 0x9f, 0xe4, 0x30, 0x6f, 0x40, 0xaf, 0xa3, 0xa6, 0xf3,
        0xdb, 0x17, 0x3f, 0x1a, 0x5e, 0x3a, 0x0c, 0xcc, 0x3d, 0x8f, 0xeb, 0x22, 0xc6, 0xba, 0xf1,
        0x33, 0x00,
    ];

    /// RPC byte order hex form of testnet block 4031230's block hash.
    /// Matches `zcash-cli getblockhash 4031230` on testnet.
    const TESTNET_BLOCK_HASH_RPC_HEX: &str =
        "0033f1bac622eb8f3dcc0c3a5e1a3f17dbf3a6a3af406f30e49fa0f422fcceee";

    fn sample_block_hash() -> BlockHash {
        let mut bytes = [0u8; 32];
        for (index, slot) in bytes.iter_mut().enumerate() {
            let index_byte = u8::try_from(index).unwrap_or_default();
            *slot = index_byte.wrapping_mul(11).wrapping_add(31);
        }
        BlockHash::from_bytes(bytes)
    }

    #[test]
    fn internal_encode_is_identity() {
        let block_hash = sample_block_hash();
        assert_eq!(
            encode_internal_block_hash(block_hash),
            block_hash.as_bytes()
        );
    }

    #[test]
    fn internal_round_trip() -> TestResult {
        let original = sample_block_hash();
        let bytes = encode_internal_block_hash(original);
        let decoded = decode_internal_block_hash(&bytes)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn internal_decode_rejects_wrong_length() {
        let outcome = decode_internal_block_hash(&[0u8; 8]);
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength {
                expected: 32,
                actual: 8,
            })
        ));
    }

    #[test]
    fn rpc_hex_matches_zcash_cli_for_testnet_block() {
        let block_hash = BlockHash::from_bytes(TESTNET_BLOCK_HASH_INTERNAL_BYTES);
        assert_eq!(
            encode_rpc_block_hash_hex(block_hash),
            TESTNET_BLOCK_HASH_RPC_HEX
        );
    }

    #[test]
    fn rpc_hex_decode_matches_storage_form_for_testnet_block() -> TestResult {
        let decoded = decode_rpc_block_hash_hex(TESTNET_BLOCK_HASH_RPC_HEX)?;
        assert_eq!(decoded.as_bytes(), TESTNET_BLOCK_HASH_INTERNAL_BYTES);
        Ok(())
    }

    #[test]
    fn rpc_bytes_decode_matches_storage_form_for_testnet_block() -> TestResult {
        let mut display_order_bytes = TESTNET_BLOCK_HASH_INTERNAL_BYTES;
        display_order_bytes.reverse();
        let decoded = decode_rpc_block_hash_bytes(&display_order_bytes)?;
        assert_eq!(decoded.as_bytes(), TESTNET_BLOCK_HASH_INTERNAL_BYTES);
        Ok(())
    }

    #[test]
    fn rpc_bytes_encode_matches_zcash_cli_for_testnet_block() -> TestResult {
        let block_hash = BlockHash::from_bytes(TESTNET_BLOCK_HASH_INTERNAL_BYTES);
        let encoded = encode_rpc_block_hash_bytes(block_hash);
        assert_eq!(hex::encode(encoded), TESTNET_BLOCK_HASH_RPC_HEX);
        assert_eq!(decode_rpc_block_hash_bytes(&encoded)?, block_hash);
        Ok(())
    }

    #[test]
    fn rpc_bytes_decode_rejects_wrong_length() {
        let outcome = decode_rpc_block_hash_bytes(&[0u8; 8]);
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength {
                expected: 32,
                actual: 8,
            })
        ));
    }

    #[test]
    fn rpc_hex_reverses_internal_bytes() {
        let block_hash = BlockHash::from_bytes([
            0xaf, 0x7c, 0x89, 0xb6, 0x9b, 0x53, 0x8f, 0xdf, 0xd3, 0xb1, 0x2e, 0x84, 0x5f, 0x08,
            0xf2, 0x37, 0xd4, 0xeb, 0x3a, 0x93, 0x24, 0x1d, 0x27, 0x88, 0x67, 0x44, 0x4b, 0x2e,
            0x50, 0x15, 0x69, 0xee,
        ]);
        let rpc_form = encode_rpc_block_hash_hex(block_hash);
        assert_eq!(
            rpc_form,
            "ee6915502e4b446788271d24933aebd437f2085f842eb1d3df8f539bb6897caf"
        );
    }

    #[test]
    fn rpc_hex_round_trip() -> TestResult {
        let original = sample_block_hash();
        let rpc_form = encode_rpc_block_hash_hex(original);
        assert_eq!(rpc_form.len(), BLOCK_HASH_HEX_LEN);
        let decoded = decode_rpc_block_hash_hex(&rpc_form)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn rpc_hex_emits_lowercase() {
        let block_hash = BlockHash::from_bytes([0xCD; 32]);
        let rpc_form = encode_rpc_block_hash_hex(block_hash);
        assert_eq!(rpc_form, "cd".repeat(32));
    }

    #[test]
    fn rpc_hex_rejects_wrong_length() {
        let outcome = decode_rpc_block_hash_hex("cd");
        assert!(matches!(
            outcome,
            Err(WireDecodeError::InvalidLength { expected: 64, .. })
        ));
    }

    #[test]
    fn rpc_hex_rejects_non_hex_characters() {
        let invalid = "z".repeat(BLOCK_HASH_HEX_LEN);
        assert!(matches!(
            decode_rpc_block_hash_hex(&invalid),
            Err(WireDecodeError::InvalidHex { .. })
        ));
    }
}
