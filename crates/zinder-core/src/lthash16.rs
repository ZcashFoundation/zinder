//! `LtHash16` element expansion shared by Zinder's order-independent
//! accumulators.
//!
//! An `LtHash16` accumulator is 1,024 little-endian `u16` lanes. Adding an
//! element adds its expansion to the lanes modulo `2^16`, and removing the
//! element subtracts the same expansion, so the sum does not depend on order.
//! [`expand_lthash16_element`] derives an element's 2,048 expansion bytes:
//!
//! 1. The root is an unkeyed `BLAKE2b` hash of the element with a 44-byte
//!    output and the accumulator's 16-byte personalization.
//! 2. The root's first 32 bytes are the key and its last 12 bytes the 96-bit
//!    nonce of `ChaCha12` in the RFC 8439 state layout. Block counters 0 through
//!    31 produce 32 keystream blocks of 64 bytes, concatenated in counter
//!    order.
//!
//! The root and the stream are public. The construction relies on the
//! heuristic that a public `ChaCha12` stream keyed by a `BLAKE2b` output behaves
//! like a random function of the element; ADR-0017 records the security limits.

use blake2b_simd::Params;
use chacha20::ChaChaCore;
use chacha20::cipher::array::Array;
use chacha20::cipher::consts::U64;
use chacha20::cipher::{KeyIvInit, StreamCipherCore};
use chacha20::variants::Ietf;

/// Length in bytes of one element's expansion: 1,024 little-endian `u16` lanes.
pub const LTHASH16_EXPANSION_LEN: usize = 2048;

/// `ChaCha` rounds of the expansion. Every accumulator byte depends on it, so a
/// change is a new accumulator version and a new UTXO commitment scheme.
type ExpansionRounds = chacha20::R12;

/// Length in bytes of the `ChaCha` key, the first part of the root.
const KEY_LEN: usize = 32;

/// Length in bytes of the `ChaCha` nonce, the rest of the root.
const NONCE_LEN: usize = 12;

/// Length in bytes of the `BLAKE2b` root: the key, then the nonce.
///
/// Taking the nonce from the root as well as the key costs no extra
/// compression. It raises the generic bound for two elements with equal
/// `ChaCha` inputs from 2^128 to 2^176 trials.
const ROOT_LEN: usize = KEY_LEN + NONCE_LEN;

/// Expands `element` into the 2,048 bytes that an `LtHash16` accumulator adds
/// or subtracts, under the accumulator's `personal` tag.
///
/// Equal elements under equal tags always produce equal expansions, which is
/// what lets an accumulator remove an element it added.
#[must_use]
pub fn expand_lthash16_element(
    personal: &[u8; 16],
    element: &[u8],
) -> [u8; LTHASH16_EXPANSION_LEN] {
    let root = Params::new()
        .hash_length(ROOT_LEN)
        .personal(personal)
        .hash(element);
    let (key_bytes, nonce_bytes) = root.as_bytes().split_at(KEY_LEN);
    let mut key = [0; KEY_LEN];
    key.copy_from_slice(key_bytes);
    let mut nonce = [0; NONCE_LEN];
    nonce.copy_from_slice(nonce_bytes);
    let mut keystream = ChaChaCore::<ExpansionRounds, Ietf>::new(&key.into(), &nonce.into());
    let mut expansion = [0; LTHASH16_EXPANSION_LEN];
    let (blocks, _) = Array::<u8, U64>::slice_as_chunks_mut(&mut expansion);
    keystream.write_keystream_blocks(blocks);
    expansion
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ChaCha` block function written from RFC 8439 section 2.3, independent of
    /// the `chacha20` crate.
    fn reference_chacha_block(
        key: &[u8; 32],
        counter: u32,
        nonce: &[u8; 12],
        rounds: usize,
    ) -> [u8; 64] {
        fn quarter_round(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
            state[a] = state[a].wrapping_add(state[b]);
            state[d] = (state[d] ^ state[a]).rotate_left(16);
            state[c] = state[c].wrapping_add(state[d]);
            state[b] = (state[b] ^ state[c]).rotate_left(12);
            state[a] = state[a].wrapping_add(state[b]);
            state[d] = (state[d] ^ state[a]).rotate_left(8);
            state[c] = state[c].wrapping_add(state[d]);
            state[b] = (state[b] ^ state[c]).rotate_left(7);
        }
        let word = |bytes: &[u8]| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let mut initial = [0u32; 16];
        initial[..4].copy_from_slice(&[0x6170_7865, 0x3320_646e, 0x7962_2d32, 0x6b20_6574]);
        for (index, chunk) in key.chunks(4).enumerate() {
            initial[4 + index] = word(chunk);
        }
        initial[12] = counter;
        for (index, chunk) in nonce.chunks(4).enumerate() {
            initial[13 + index] = word(chunk);
        }
        let mut working = initial;
        for _ in 0..rounds / 2 {
            quarter_round(&mut working, 0, 4, 8, 12);
            quarter_round(&mut working, 1, 5, 9, 13);
            quarter_round(&mut working, 2, 6, 10, 14);
            quarter_round(&mut working, 3, 7, 11, 15);
            quarter_round(&mut working, 0, 5, 10, 15);
            quarter_round(&mut working, 1, 6, 11, 12);
            quarter_round(&mut working, 2, 7, 8, 13);
            quarter_round(&mut working, 3, 4, 9, 14);
        }
        let mut block = [0u8; 64];
        for (index, bytes) in block.chunks_mut(4).enumerate() {
            bytes.copy_from_slice(&working[index].wrapping_add(initial[index]).to_le_bytes());
        }
        block
    }

    /// The expansion rebuilt from the definition: a 44-byte `BLAKE2b` root
    /// split into key and nonce, and the reference `ChaCha12` block function at
    /// counters 0 through 31.
    fn reference_expansion(personal: &[u8; 16], element: &[u8]) -> [u8; LTHASH16_EXPANSION_LEN] {
        let root = Params::new()
            .hash_length(44)
            .personal(personal)
            .hash(element);
        let mut key = [0u8; 32];
        key.copy_from_slice(&root.as_bytes()[..32]);
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&root.as_bytes()[32..]);
        let mut expansion = [0u8; LTHASH16_EXPANSION_LEN];
        for (counter, block) in (0u32..).zip(expansion.chunks_mut(64)) {
            block.copy_from_slice(&reference_chacha_block(&key, counter, &nonce, 12));
        }
        expansion
    }

    fn block(expansion: &[u8; LTHASH16_EXPANSION_LEN], counter: usize) -> String {
        hex::encode(&expansion[counter * 64..(counter + 1) * 64])
    }

    fn patterned_bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..=u8::MAX)
            .cycle()
            .take(len)
            .map(|byte| byte ^ seed)
            .collect()
    }

    #[test]
    fn reference_block_function_matches_rfc_8439() {
        // RFC 8439 section 2.3.2: ChaCha20 block function test vector.
        let key: [u8; 32] = core::array::from_fn(|index| u8::try_from(index).unwrap_or(0));
        let nonce = [0, 0, 0, 0x09, 0, 0, 0, 0x4a, 0, 0, 0, 0];
        assert_eq!(
            hex::encode(reference_chacha_block(&key, 1, &nonce, 20)),
            concat!(
                "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4e",
                "d2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e",
            )
        );
    }

    /// Checks `ChaCha12` blocks alone, without a root, for the RFC 8439
    /// section 2.3.2 key and nonce.
    ///
    /// The vectors come from `reference_expansion.py` in the accumulator scheme
    /// evidence. Both the product stream and the reference block function in
    /// this module must reproduce them at counters 0, 1, and 31.
    #[test]
    fn chacha12_blocks_match_independent_vectors() {
        let key: [u8; 32] = core::array::from_fn(|index| u8::try_from(index).unwrap_or(0));
        let nonce = [0, 0, 0, 0x09, 0, 0, 0, 0x4a, 0, 0, 0, 0];
        let mut keystream = ChaChaCore::<ExpansionRounds, Ietf>::new(&key.into(), &nonce.into());
        let mut stream = [0u8; LTHASH16_EXPANSION_LEN];
        let (blocks, _) = Array::<u8, U64>::slice_as_chunks_mut(&mut stream);
        keystream.write_keystream_blocks(blocks);
        for (counter, expected) in [
            (
                0u32,
                concat!(
                    "15a783e5ad5e601c5a1d3e3db3e8d2405509bcf1c986967825888dc7fd4b991e",
                    "e5c8dfb889e3b1ecbe1e474122f60742a28322c85f8390fa8418df439fd50d66",
                ),
            ),
            (
                1,
                concat!(
                    "7f8b136677c73799e3e7777d16e6d8ccc787ce39694990c628e087029ce9190b",
                    "da4be31ac3fe2102a9ad737cf82fa3b06e68b63371c65c827299040ade1ba8a0",
                ),
            ),
            (
                31,
                concat!(
                    "2011bf5609dfc6d8c65fb4d9840971f36e8f9851910f29821aaa6269cacd89aa",
                    "cc3d006436a0ecbc05eb6f1edd8acf89a918861c3e963010bbb06e10e3e09e0b",
                ),
            ),
        ] {
            let index = usize::try_from(counter).unwrap_or(usize::MAX);
            assert_eq!(block(&stream, index), expected, "product block {counter}");
            assert_eq!(
                hex::encode(reference_chacha_block(&key, counter, &nonce, 12)),
                expected,
                "reference block {counter}"
            );
        }
    }

    #[test]
    fn expansion_matches_the_reference_in_every_block() {
        let personalizations = [
            *PROJECTION_ROW_PERSONAL,
            crate::wire::UTXO_SET_COMMITMENT_PERSONAL,
            [0; 16],
        ];
        let elements = [
            Vec::new(),
            vec![1, 2, 3],
            patterned_bytes(128, 0x01),
            patterned_bytes(129, 0x02),
            patterned_bytes(300, 0x5a),
        ];
        for personal in &personalizations {
            for element in &elements {
                let expansion = expand_lthash16_element(personal, element);
                let reference = reference_expansion(personal, element);
                for counter in 0..32 {
                    assert_eq!(
                        block(&expansion, counter),
                        block(&reference, counter),
                        "block {counter} of a {}-byte element, personalization {}",
                        element.len(),
                        hex::encode(personal)
                    );
                }
            }
        }
    }

    /// Checks vectors from an independent implementation.
    ///
    /// `reference_expansion.py` in the accumulator scheme evidence computed
    /// them with Python's standard `hashlib.blake2b` and its own `ChaCha` block
    /// function, so they share no code with either crate used here.
    #[test]
    fn expansion_matches_independent_vectors_in_blocks_0_and_31() {
        for (personal, element_len, first, last) in PINNED_VECTORS {
            let element: Vec<u8> = (0..=u8::MAX).take(*element_len).collect();
            let expansion = expand_lthash16_element(personal, &element);
            assert_eq!(block(&expansion, 0), *first, "{}", hex::encode(personal));
            assert_eq!(block(&expansion, 31), *last, "{}", hex::encode(personal));
        }
    }

    /// The wallet projection row personalization, owned by
    /// `zinder-wallet-projection`.
    const PROJECTION_ROW_PERSONAL: &[u8; 16] = b"ZinderWProjRoot2";

    /// Personalization, element length, block 0, and block 31 for both
    /// domains. The element is the first `len` bytes of `00, 01, ..., ff`.
    const PINNED_VECTORS: &[(&[u8; 16], usize, &str, &str)] = &[
        (
            PROJECTION_ROW_PERSONAL,
            0,
            concat!(
                "2ed437eb6b166bc13d77418fb1a4f8905e933ec3983026ea7336cb7d4261d682",
                "a71802f9b8d34291d5b642a9c92206511b556e449d8058d5ea54c1dc1d47e2b8",
            ),
            concat!(
                "a5988be115edb7182fe5329020c46e103fe0cfe92f319d203e161cf7b18600b7",
                "94cfeece04f18e3a235351c61e634ed5762aae1157472a1272314bf3e0a05508",
            ),
        ),
        (
            PROJECTION_ROW_PERSONAL,
            256,
            concat!(
                "a5dc3a6e8133e583dd919820ba2268b81f3e3a7cdf5f5bf1d6b7fb78407c1a50",
                "6e3e4c4b474050be32f47b7a3c01753343ccc6f8557ba1762d07b349fbe8620a",
            ),
            concat!(
                "70b3cb53363ff37b8e615b1594a254e64b6d677f7a2c92bd19912862daa14291",
                "12b1b85951b97bb219d40073e8af2a4e7a681d043572d41c8e16474501c7e57e",
            ),
        ),
        (
            &crate::wire::UTXO_SET_COMMITMENT_PERSONAL,
            0,
            concat!(
                "13827a75d088466acae443556339ba22ec26b790e222fc81a35d4893cf4d3c59",
                "35025b1a2e2c1475a77835525ae4051b41ca2e045933143e4750cf199f5a6fd6",
            ),
            concat!(
                "c6bcd0c4a2c70f96a06f727c7d90cd1a6e7f34b770f5fc984d5e1384eb572b13",
                "2f69a4d4ed290beb6a068d4158b5be8d6fd40d6668e56f1c73bbd579bdc01302",
            ),
        ),
        (
            &crate::wire::UTXO_SET_COMMITMENT_PERSONAL,
            256,
            concat!(
                "e77ef3f5d9fb416eed72e27f177e98a17e43615c077c7cf8cc8df1cbb55f1348",
                "e49b7c215f1ddcc2fc6471e3cace0d88df9c78d6d134c45062b91e8b653d2583",
            ),
            concat!(
                "068648fe48844c0333acfbd6734048ba5d4fb383d0a22a0d99c1de98652e87cf",
                "70f50f907f5ded7c47cc601c06667dbd3e437eb79bc959bcfef9ac49c1f4b9a0",
            ),
        ),
    ];
}
