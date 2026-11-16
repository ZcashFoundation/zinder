//! Homomorphic commitment to the transparent UTXO set (`LtHash16`).
//!
//! Each canonical UTXO element is expanded through a BLAKE2X XOF to 2048 bytes,
//! read as 1024 little-endian `u16` lanes. The accumulator sums lanes
//! componentwise modulo `2^16`. The sum is order-independent and invertible, so
//! folding a set yields the same accumulator regardless of insertion order, and
//! removing an element subtracts its lanes back out. The 2048-byte accumulator
//! is the commitment; a 32-byte display digest is `BLAKE2b-256` of it.
//!
//! BLAKE2X is the official construction from <https://www.blake2.net/blake2x.pdf>:
//! a root `BLAKE2b` hash over the element with the XOF output length encoded in
//! the parameter block, followed by per-block expansion hashes whose parameter
//! block sets `node_offset` to the block index, `xof_length` to the total
//! output length, `inner_length` to 64, and `fanout`/`depth` to 0. It is not an
//! ad-hoc counter feeding repeated `BLAKE2b` calls. The personalization is
//! [`UTXO_SET_COMMITMENT_PERSONAL`]; the element bytes come from
//! [`encode_utxo_set_commitment_element`].

use blake2b_simd::Params;
use blake2b_simd::many::{HashManyJob, hash_many};

use crate::wire::{
    UTXO_SET_COMMITMENT_PERSONAL, UtxoSetCommitmentElement, encode_utxo_set_commitment_element,
};

/// Number of `u16` lanes in the accumulator.
const COMMITMENT_LANE_COUNT: usize = 1024;

/// Length in bytes of the accumulator and the BLAKE2X XOF output.
pub const UTXO_SET_COMMITMENT_LEN: usize = COMMITMENT_LANE_COUNT * 2;

/// Length in bytes of the display digest.
pub const UTXO_SET_COMMITMENT_DIGEST_LEN: usize = 32;

/// Length in bytes of a full `BLAKE2b` output block.
const BLAKE2B_OUTPUT_LEN: usize = 64;

/// Number of BLAKE2X expansion blocks hashed in one batch, enough for a full
/// commitment expansion.
const EXPANSION_BATCH_BLOCKS: usize = UTXO_SET_COMMITMENT_LEN / BLAKE2B_OUTPUT_LEN;

/// Snapshot scheme identifying how a [`TransparentUtxoSetCommitment`] was built.
///
/// The scheme id is self-describing: a comparison across two commitments is
/// only meaningful when the schemes match. A future zcashd-comparable
/// membership rule is a new variant, never a reinterpretation of an existing
/// one.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum UtxoSetCommitmentScheme {
    /// Scheme not stated. Never produced by this crate; the absence sentinel a
    /// wire decoder maps an unset enum onto.
    Unspecified,
    /// Componentwise `u16`-lane homomorphic hash over the full transparent
    /// unspent set, BLAKE2X-expanded element encoding.
    LtHash16,
}

impl UtxoSetCommitmentScheme {
    /// Returns the stable numeric scheme identifier.
    #[must_use]
    pub const fn id(self) -> u32 {
        match self {
            Self::Unspecified => 0,
            Self::LtHash16 => 1,
        }
    }

    /// Resolves a numeric scheme identifier into a known scheme.
    #[must_use]
    pub const fn from_id(scheme_id: u32) -> Option<Self> {
        match scheme_id {
            0 => Some(Self::Unspecified),
            1 => Some(Self::LtHash16),
            _ => None,
        }
    }
}

/// A homomorphic commitment to the full transparent unspent set at one epoch.
///
/// Holds the scheme and the raw 2048-byte accumulator. The display digest is
/// derived on demand by [`Self::display_digest`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentUtxoSetCommitment {
    scheme: UtxoSetCommitmentScheme,
    accumulator: Box<[u8; UTXO_SET_COMMITMENT_LEN]>,
}

impl Default for TransparentUtxoSetCommitment {
    fn default() -> Self {
        Self::empty()
    }
}

impl TransparentUtxoSetCommitment {
    /// Returns the empty commitment: the all-zero accumulator under
    /// [`UtxoSetCommitmentScheme::LtHash16`].
    ///
    /// Folding zero elements leaves the accumulator at this value, so an empty
    /// unspent set and a freshly constructed commitment compare equal.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            scheme: UtxoSetCommitmentScheme::LtHash16,
            accumulator: Box::new([0u8; UTXO_SET_COMMITMENT_LEN]),
        }
    }

    /// Reconstructs a commitment from a scheme and raw accumulator bytes.
    ///
    /// Returns `None` when `accumulator` is not exactly
    /// [`UTXO_SET_COMMITMENT_LEN`] bytes. The bytes are taken verbatim; this is
    /// the inverse of reading [`Self::accumulator`] and is how a wire decoder
    /// rebuilds a received commitment.
    #[must_use]
    pub fn from_parts(scheme: UtxoSetCommitmentScheme, accumulator: &[u8]) -> Option<Self> {
        let accumulator: [u8; UTXO_SET_COMMITMENT_LEN] = accumulator.try_into().ok()?;
        Some(Self {
            scheme,
            accumulator: Box::new(accumulator),
        })
    }

    /// Returns the scheme this commitment was built under.
    #[must_use]
    pub const fn scheme(&self) -> UtxoSetCommitmentScheme {
        self.scheme
    }

    /// Returns the raw 2048-byte accumulator.
    #[must_use]
    pub const fn accumulator(&self) -> &[u8; UTXO_SET_COMMITMENT_LEN] {
        &self.accumulator
    }

    /// Folds one transparent output into the accumulator.
    ///
    /// Expands the element through the BLAKE2X XOF and adds its 1024 lanes to
    /// the accumulator componentwise modulo `2^16`.
    pub fn insert(&mut self, element: &UtxoSetCommitmentElement<'_>) {
        self.combine_lanes(element, u16::wrapping_add);
    }

    /// Removes one transparent output from the accumulator.
    ///
    /// The exact inverse of [`Self::insert`]: subtracts the element's lanes
    /// componentwise modulo `2^16`. Subtracting an element that was never
    /// inserted yields a well-defined accumulator (the additive inverse of its
    /// lanes) rather than an error.
    pub fn subtract(&mut self, element: &UtxoSetCommitmentElement<'_>) {
        self.combine_lanes(element, u16::wrapping_sub);
    }

    /// Returns the 32-byte display digest, `BLAKE2b-256` of the accumulator.
    #[must_use]
    pub fn display_digest(&self) -> [u8; UTXO_SET_COMMITMENT_DIGEST_LEN] {
        let hash = Params::new()
            .hash_length(UTXO_SET_COMMITMENT_DIGEST_LEN)
            .hash(self.accumulator.as_slice());
        let mut digest = [0u8; UTXO_SET_COMMITMENT_DIGEST_LEN];
        digest.copy_from_slice(hash.as_bytes());
        digest
    }

    fn combine_lanes(
        &mut self,
        element: &UtxoSetCommitmentElement<'_>,
        combine: impl Fn(u16, u16) -> u16,
    ) {
        let preimage = encode_utxo_set_commitment_element(element);
        let expansion = blake2x_expand(&preimage);
        let (accumulator_lanes, _) = self.accumulator.as_chunks_mut::<2>();
        let (element_lanes, _) = expansion.as_chunks::<2>();
        for (accumulator_lane, element_lane) in accumulator_lanes.iter_mut().zip(element_lanes) {
            *accumulator_lane = combine(
                u16::from_le_bytes(*accumulator_lane),
                u16::from_le_bytes(*element_lane),
            )
            .to_le_bytes();
        }
    }
}

/// Expands `message` to [`UTXO_SET_COMMITMENT_LEN`] bytes through the canonical
/// BLAKE2X XOF under the commitment personalization.
fn blake2x_expand(message: &[u8]) -> [u8; UTXO_SET_COMMITMENT_LEN] {
    let mut output = [0u8; UTXO_SET_COMMITMENT_LEN];
    blake2x_expand_into(message, &UTXO_SET_COMMITMENT_PERSONAL, &mut output);
    output
}

/// Expands `message` into `output` through the canonical BLAKE2X XOF under
/// `personal`.
///
/// The XOF length is `output.len()`, matching the official BLAKE2X construction
/// (<https://www.blake2.net/blake2x.pdf>): a root `BLAKE2b` hash carrying the
/// total length in the parameter block, then per-block expansion hashes. The
/// production fold reaches this through [`blake2x_expand`] with the commitment
/// personalization; the known-answer test reaches the same code with the empty
/// personalization.
fn blake2x_expand_into(message: &[u8], personal: &[u8; 16], output: &mut [u8]) {
    let xof_length = u32::try_from(output.len()).unwrap_or(u32::MAX);
    let root = blake2x_root_hash(message, personal, xof_length);

    // Each expansion block hashes the same root under its own node offset, so
    // the blocks are independent and hash together in SIMD batches.
    let batch_output_len = EXPANSION_BATCH_BLOCKS * BLAKE2B_OUTPUT_LEN;
    for (batch_index, batch) in output.chunks_mut(batch_output_len).enumerate() {
        let batch_len = batch.len();
        let mut blocks: [Option<HashManyJob<'_>>; EXPANSION_BATCH_BLOCKS] =
            core::array::from_fn(|slot| {
                let block_start = slot * BLAKE2B_OUTPUT_LEN;
                (block_start < batch_len).then(|| {
                    let block_index = u32::try_from(batch_index * EXPANSION_BATCH_BLOCKS + slot)
                        .unwrap_or(u32::MAX);
                    let block_length = (batch_len - block_start).min(BLAKE2B_OUTPUT_LEN);
                    HashManyJob::new(
                        &blake2x_expansion_params(personal, block_index, block_length, xof_length),
                        root.as_bytes(),
                    )
                })
            });
        hash_many(blocks.iter_mut().flatten());
        for (output_block, block) in batch
            .chunks_mut(BLAKE2B_OUTPUT_LEN)
            .zip(blocks.iter().flatten())
        {
            output_block.copy_from_slice(block.to_hash().as_bytes());
        }
    }
}

/// Computes the BLAKE2X root hash `H0` over `message`.
///
/// A full 64-byte `BLAKE2b` hash whose parameter block additionally carries the
/// total XOF output length in the high 32 bits of `node_offset`
/// (`cfg[12..16]`), matching the official construction.
fn blake2x_root_hash(message: &[u8], personal: &[u8; 16], xof_length: u32) -> blake2b_simd::Hash {
    Params::new()
        .hash_length(BLAKE2B_OUTPUT_LEN)
        .node_offset(u64::from(xof_length) << 32)
        .personal(personal)
        .hash(message)
}

/// Returns the parameters of one BLAKE2X expansion block `B_i` over the root
/// hash.
///
/// The parameter block sets `digest_length` to this block's length,
/// `leaf_length` to 64, `node_offset` to the block index with the XOF length in
/// the high 32 bits, `inner_length` to 64, and `fanout`/`depth` to 0.
fn blake2x_expansion_params(
    personal: &[u8; 16],
    block_index: u32,
    block_length: usize,
    xof_length: u32,
) -> Params {
    let mut params = Params::new();
    params
        .hash_length(block_length)
        .fanout(0)
        .max_depth(0)
        .max_leaf_length(u32::try_from(BLAKE2B_OUTPUT_LEN).unwrap_or(u32::MAX))
        .node_offset(u64::from(block_index) | (u64::from(xof_length) << 32))
        .inner_hash_length(BLAKE2B_OUTPUT_LEN)
        .personal(personal);
    params
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{BlockHeight, TransactionId, TransparentOutPoint};

    /// Official BLAKE2 test vectors for `BLAKE2Xb`, no key, no personalization.
    ///
    /// Source: <https://github.com/BLAKE2/BLAKE2/blob/master/testvectors/blake2-kat.json>
    /// (`hash == "blake2xb"`). Input is the 256-byte sequence `00..ff`.
    const BLAKE2XB_KAT_INPUT_LEN: usize = 256;

    /// Empty personalization: the official `BLAKE2Xb` vectors use no salt or
    /// personalization, so the KAT exercises [`blake2x_expand_into`] (the
    /// production XOF) at the standard parameters.
    const NO_PERSONALIZATION: [u8; 16] = [0u8; 16];

    fn kat_input() -> Vec<u8> {
        (0..BLAKE2XB_KAT_INPUT_LEN)
            .map(|index| u8::try_from(index).unwrap_or(0))
            .collect()
    }

    #[test]
    fn blake2xb_matches_official_kat_at_64_bytes() {
        let mut out = [0u8; 64];
        blake2x_expand_into(&kat_input(), &NO_PERSONALIZATION, &mut out);
        let expected = hex::decode(concat!(
            "571be91037c15145e2ab4894a7bb8d8a3cab75e6e64ef296e760c15cf8f3f3ac",
            "fa5c894ee56cb6ac2db9b32c39a1cc39f96c50dd333f1059230482f3ed2d9246",
        ))
        .unwrap_or_default();
        assert_eq!(out.as_slice(), expected.as_slice());
    }

    #[test]
    fn blake2xb_matches_official_kat_at_256_bytes() {
        let mut out = [0u8; 256];
        blake2x_expand_into(&kat_input(), &NO_PERSONALIZATION, &mut out);
        let expected = hex::decode(concat!(
            "59f8eea01a07a2670f2fe464bd755d8cde620cb4bac6006556a8663d2d9625c6",
            "2fe63b6b68adba279ab287c04d3de6c4c17e6428dff30e9b2524fea1e869e424",
            "85c03a9f48af40d12d5cba0d13abac272ee36efeb8bd098ce0e1da8233ef6e6b",
            "3e96c9e05a7fedb79ae44e698640e6b8f26c43674e2c32ef17b4d7b005554ec4",
            "fd8aa1dac0f975fc888bec5bd7a06fbf29ae09f2d37c5eb7d0f67c9c77d5caf7",
            "afe681ae336fb3fccd97ecdec0348cdea4787a4e9de4df4bbfb209eeb642ce8f",
            "92730d598a71c94259e648d0a4dd89079a06c4b463ba1d175476337d553b0401",
            "d2b6f0c32639e3edcdd8c225c61e0afa5cd103b5d26a56afe3ac9462df794dc0",
        ))
        .unwrap_or_default();
        assert_eq!(out.as_slice(), expected.as_slice());
    }

    #[test]
    fn lthash16_reference_lanes_match_go_xof() {
        // lukechampine/folly reference: blake2b.NewXOF(2048, nil) over [1,2,3],
        // little-endian u16 lanes. Cross-checked against golang.org/x/crypto.
        let mut out = [0u8; UTXO_SET_COMMITMENT_LEN];
        blake2x_expand_into(&[1, 2, 3], &NO_PERSONALIZATION, &mut out);
        let first_lanes: Vec<u16> = (0..8)
            .map(|index| u16::from_le_bytes([out[index * 2], out[index * 2 + 1]]))
            .collect();
        assert_eq!(
            first_lanes,
            vec![13199, 11388, 37027, 25013, 17230, 55544, 38087, 61763]
        );
        assert_eq!(
            out[..16].to_vec(),
            hex::decode("8f337c2ca390b5614e43f8d8c79443f1").unwrap_or_default()
        );
    }

    struct SampleUtxo {
        txid: [u8; 32],
        output_index: u32,
        value_zat: u64,
        script: Vec<u8>,
        height: u32,
    }

    impl SampleUtxo {
        fn new(index_seed: u8, output_index: u32, value_zat: u64, height: u32) -> Self {
            Self {
                txid: [index_seed; 32],
                output_index,
                value_zat,
                script: vec![0x6a, index_seed, 0xac],
                height,
            }
        }

        fn fold_into(&self, commitment: &mut TransparentUtxoSetCommitment) {
            commitment.insert(&UtxoSetCommitmentElement {
                network_id: 1,
                outpoint: TransparentOutPoint::new(
                    TransactionId::from_bytes(self.txid),
                    self.output_index,
                ),
                value_zat: self.value_zat,
                script_pub_key: &self.script,
                block_height: BlockHeight::new(self.height),
            });
        }
    }

    #[test]
    fn empty_commitment_is_all_zero() {
        let commitment = TransparentUtxoSetCommitment::empty();
        assert_eq!(
            commitment.accumulator().as_slice(),
            &[0u8; UTXO_SET_COMMITMENT_LEN]
        );
        assert_eq!(commitment.scheme(), UtxoSetCommitmentScheme::LtHash16);
    }

    #[test]
    fn insert_then_subtract_is_identity() {
        let mut commitment = TransparentUtxoSetCommitment::empty();
        let script = vec![0x76, 0xa9, 0x14, 0xff, 0x88, 0xac];
        let outpoint = TransparentOutPoint::new(TransactionId::from_bytes([7u8; 32]), 3);
        let lthash_element = UtxoSetCommitmentElement {
            network_id: 1,
            outpoint,
            value_zat: 12_345,
            script_pub_key: &script,
            block_height: BlockHeight::new(900),
        };
        commitment.insert(&lthash_element);
        assert_ne!(commitment, TransparentUtxoSetCommitment::empty());
        commitment.subtract(&lthash_element);
        assert_eq!(commitment, TransparentUtxoSetCommitment::empty());
    }

    #[test]
    fn fold_is_order_independent() {
        let elements = [
            SampleUtxo::new(1, 0, 100, 10),
            SampleUtxo::new(2, 1, 200, 20),
            SampleUtxo::new(3, 2, 300, 30),
            SampleUtxo::new(4, 3, 400, 40),
        ];
        let mut forward = TransparentUtxoSetCommitment::empty();
        for element in &elements {
            element.fold_into(&mut forward);
        }
        let mut reverse = TransparentUtxoSetCommitment::empty();
        for element in elements.iter().rev() {
            element.fold_into(&mut reverse);
        }
        assert_eq!(forward, reverse);
    }

    #[test]
    fn union_equals_sum_of_subcommitments() {
        let left = SampleUtxo::new(11, 0, 1, 1);
        let right = SampleUtxo::new(22, 1, 2, 2);

        let mut union = TransparentUtxoSetCommitment::empty();
        left.fold_into(&mut union);
        right.fold_into(&mut union);

        let mut summed = TransparentUtxoSetCommitment::empty();
        left.fold_into(&mut summed);
        let mut second = TransparentUtxoSetCommitment::empty();
        right.fold_into(&mut second);
        for lane_index in 0..COMMITMENT_LANE_COUNT {
            let byte_index = lane_index * 2;
            let summed_lane = u16::from_le_bytes([
                summed.accumulator[byte_index],
                summed.accumulator[byte_index + 1],
            ]);
            let second_lane = u16::from_le_bytes([
                second.accumulator[byte_index],
                second.accumulator[byte_index + 1],
            ]);
            let combined = summed_lane.wrapping_add(second_lane).to_le_bytes();
            summed.accumulator[byte_index] = combined[0];
            summed.accumulator[byte_index + 1] = combined[1];
        }
        assert_eq!(union, summed);
    }

    #[test]
    fn display_digest_matches_reference_for_known_set() {
        let mut commitment = TransparentUtxoSetCommitment::empty();
        let script = {
            let mut script = vec![0x76, 0xa9, 0x14];
            script.extend(std::iter::repeat_n(0xab, 20));
            script.extend_from_slice(&[0x88, 0xac]);
            script
        };
        let mut txid = [0u8; 32];
        for (index, byte) in txid.iter_mut().enumerate() {
            *byte = u8::try_from(index).unwrap_or(0);
        }
        commitment.insert(&UtxoSetCommitmentElement {
            network_id: 1,
            outpoint: TransparentOutPoint::new(TransactionId::from_bytes(txid), 2),
            value_zat: 50_000,
            script_pub_key: &script,
            block_height: BlockHeight::new(419_200),
        });
        let expected =
            hex::decode("3d0af8cb33b27778f2c14fc34b4bbecf4e968018a7424e60dc0d6b64d384f9e6")
                .unwrap_or_default();
        assert_eq!(commitment.display_digest().to_vec(), expected);
    }

    /// Serial BLAKE2X expansion that every faster path must reproduce byte
    /// for byte: one `Params::hash` call per 64-byte block, in block order.
    fn serial_reference_expand_into(message: &[u8], personal: &[u8; 16], output: &mut [u8]) {
        let xof_length = u32::try_from(output.len()).unwrap_or(u32::MAX);
        let root = Params::new()
            .hash_length(BLAKE2B_OUTPUT_LEN)
            .node_offset(u64::from(xof_length) << 32)
            .personal(personal)
            .hash(message);

        let mut produced = 0usize;
        let mut block_index = 0u32;
        while produced < output.len() {
            let block_length = (output.len() - produced).min(BLAKE2B_OUTPUT_LEN);
            let block = Params::new()
                .hash_length(block_length)
                .fanout(0)
                .max_depth(0)
                .max_leaf_length(u32::try_from(BLAKE2B_OUTPUT_LEN).unwrap_or(u32::MAX))
                .node_offset(u64::from(block_index) | (u64::from(xof_length) << 32))
                .inner_hash_length(BLAKE2B_OUTPUT_LEN)
                .personal(personal)
                .hash(root.as_bytes());
            output[produced..produced + block_length].copy_from_slice(block.as_bytes());
            produced += block_length;
            block_index = block_index.wrapping_add(1);
        }
    }

    #[test]
    fn blake2xb_matches_official_kat_at_100_bytes() {
        // The only official vector here whose final block is partial.
        let mut out = [0u8; 100];
        blake2x_expand_into(&kat_input(), &NO_PERSONALIZATION, &mut out);
        let expected = hex::decode(concat!(
            "726ead16f67729ba596654a551eb126e99457962286fc54bb6baf50d93c28340",
            "9694db0142264b697e6d9be81bd7f63e4965c784ef0af12529294ef7795e9d64",
            "c371b15c1a5701c48dae9e2a3d908602c4a82bbeddb9a20eca30b591140f76fb",
            "c11a3df2",
        ))
        .unwrap_or_default();
        assert_eq!(out.as_slice(), expected.as_slice());
    }

    #[test]
    fn commitment_expansion_keeps_its_pinned_bytes() {
        let expansion = blake2x_expand(&kat_input());
        assert_eq!(
            hex::encode(Sha256::digest(expansion)),
            "47c77b84583269e36c758c8853b7403f0ba9f6947b9bd894280d10e6acf717ba"
        );
    }

    #[test]
    fn expansion_matches_the_serial_reference_in_every_block() {
        let messages = [Vec::new(), vec![1, 2, 3], kat_input()];
        let personalizations = [
            NO_PERSONALIZATION,
            UTXO_SET_COMMITMENT_PERSONAL,
            *b"ZinderWalletProj",
        ];
        // One block, a partial final block, the official 256-byte vector, the
        // production length, and a length past it with a partial final block.
        let output_lengths = [64, 100, 256, UTXO_SET_COMMITMENT_LEN, 2148];
        for message in &messages {
            for personal in &personalizations {
                for output_length in output_lengths {
                    let mut expanded = vec![0u8; output_length];
                    blake2x_expand_into(message, personal, &mut expanded);
                    let mut reference = vec![0u8; output_length];
                    serial_reference_expand_into(message, personal, &mut reference);
                    for (block_index, (block, reference_block)) in expanded
                        .chunks(BLAKE2B_OUTPUT_LEN)
                        .zip(reference.chunks(BLAKE2B_OUTPUT_LEN))
                        .enumerate()
                    {
                        assert_eq!(
                            block,
                            reference_block,
                            "block {block_index} of {output_length} bytes over a {}-byte \
                             message, personalization {}",
                            message.len(),
                            hex::encode(personal)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn scheme_id_round_trips() {
        assert_eq!(
            UtxoSetCommitmentScheme::from_id(0),
            Some(UtxoSetCommitmentScheme::Unspecified)
        );
        assert_eq!(
            UtxoSetCommitmentScheme::from_id(1),
            Some(UtxoSetCommitmentScheme::LtHash16)
        );
        assert_eq!(UtxoSetCommitmentScheme::from_id(2), None);
        assert_eq!(UtxoSetCommitmentScheme::LtHash16.id(), 1);
    }
}
