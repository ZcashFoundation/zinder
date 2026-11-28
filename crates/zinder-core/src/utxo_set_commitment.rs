//! Homomorphic commitment to the transparent UTXO set (`LtHash16`).
//!
//! Each canonical UTXO element is expanded to 2,048 bytes by
//! [`expand_lthash16_element`] under [`UTXO_SET_COMMITMENT_PERSONAL`], read as
//! 1,024 little-endian `u16` lanes. The accumulator sums lanes componentwise
//! modulo `2^16`. The sum is order-independent and invertible, so folding a set
//! yields the same accumulator regardless of insertion order, and removing an
//! element subtracts its lanes back out. The 2,048-byte accumulator is the
//! commitment; a 32-byte display digest is `BLAKE2b-256` of it. The element
//! bytes come from [`encode_utxo_set_commitment_element`].

use blake2b_simd::Params;
use thiserror::Error;

use crate::lthash16::{LTHASH16_EXPANSION_LEN, expand_lthash16_element};
use crate::wire::{
    UTXO_SET_COMMITMENT_PERSONAL, UtxoSetCommitmentElement, encode_utxo_set_commitment_element,
};

/// Length in bytes of the accumulator: 1,024 `u16` lanes.
pub const UTXO_SET_COMMITMENT_LEN: usize = LTHASH16_EXPANSION_LEN;

/// Length in bytes of the display digest.
pub const UTXO_SET_COMMITMENT_DIGEST_LEN: usize = 32;

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
    /// unspent set. Each element encoding is expanded by a `BLAKE2b` root that
    /// keys `ChaCha12`.
    LtHash16ChaCha12,
}

impl UtxoSetCommitmentScheme {
    /// Returns the stable numeric scheme identifier.
    #[must_use]
    pub const fn id(self) -> u32 {
        match self {
            Self::Unspecified => 0,
            Self::LtHash16ChaCha12 => 1,
        }
    }

    /// Resolves a numeric scheme identifier into a known scheme.
    #[must_use]
    pub const fn from_id(scheme_id: u32) -> Option<Self> {
        match scheme_id {
            0 => Some(Self::Unspecified),
            1 => Some(Self::LtHash16ChaCha12),
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
    /// [`UtxoSetCommitmentScheme::LtHash16ChaCha12`].
    ///
    /// Folding zero elements leaves the accumulator at this value, so an empty
    /// unspent set and a freshly constructed commitment compare equal.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            scheme: UtxoSetCommitmentScheme::LtHash16ChaCha12,
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
    /// Expands the element and adds its 1,024 lanes to the accumulator
    /// componentwise modulo `2^16`.
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

    /// Adds every element folded into `other` to this commitment.
    ///
    /// Lanes are summed componentwise modulo `2^16`, so the result commits to
    /// the elements of both commitments, whatever order each was folded in.
    /// Commitments folded on separate threads over disjoint parts of a set
    /// combine into the commitment of the whole set.
    ///
    /// Both commitments must be [`UtxoSetCommitmentScheme::LtHash16ChaCha12`], the
    /// scheme this lane sum belongs to. Otherwise this returns an error and
    /// leaves `self` unchanged.
    pub fn combine(&mut self, other: &Self) -> Result<(), UtxoSetCommitmentSchemeMismatch> {
        if self.scheme != UtxoSetCommitmentScheme::LtHash16ChaCha12
            || other.scheme != UtxoSetCommitmentScheme::LtHash16ChaCha12
        {
            return Err(UtxoSetCommitmentSchemeMismatch {
                target: self.scheme,
                added: other.scheme,
            });
        }
        fold_lanes(&mut self.accumulator, &other.accumulator, u16::wrapping_add);
        Ok(())
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
        fold_lanes(
            &mut self.accumulator,
            &expand_lthash16_element(&UTXO_SET_COMMITMENT_PERSONAL, &preimage),
            combine,
        );
    }
}

/// Two UTXO set commitments that cannot be combined, because at least one of
/// them is not an [`UtxoSetCommitmentScheme::LtHash16ChaCha12`] commitment.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(
    "cannot combine a {added:?} UTXO set commitment into a {target:?} one; both must be LtHash16ChaCha12"
)]
pub struct UtxoSetCommitmentSchemeMismatch {
    /// Scheme of the commitment being added to.
    pub target: UtxoSetCommitmentScheme,
    /// Scheme of the commitment being added.
    pub added: UtxoSetCommitmentScheme,
}

/// Combines each little-endian `u16` lane of `lanes` into `accumulator`.
fn fold_lanes(
    accumulator: &mut [u8; UTXO_SET_COMMITMENT_LEN],
    lanes: &[u8; UTXO_SET_COMMITMENT_LEN],
    combine: impl Fn(u16, u16) -> u16,
) {
    let (accumulator_lanes, _) = accumulator.as_chunks_mut::<2>();
    let (other_lanes, _) = lanes.as_chunks::<2>();
    for (accumulator_lane, other_lane) in accumulator_lanes.iter_mut().zip(other_lanes) {
        *accumulator_lane = combine(
            u16::from_le_bytes(*accumulator_lane),
            u16::from_le_bytes(*other_lane),
        )
        .to_le_bytes();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlockHeight, TransactionId, TransparentOutPoint};

    /// Number of `u16` lanes in the accumulator.
    const COMMITMENT_LANE_COUNT: usize = UTXO_SET_COMMITMENT_LEN / 2;

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
        assert_eq!(
            commitment.scheme(),
            UtxoSetCommitmentScheme::LtHash16ChaCha12
        );
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
    fn combined_partial_commitments_equal_the_serial_fold_for_every_split()
    -> Result<(), UtxoSetCommitmentSchemeMismatch> {
        const PARTS: usize = 3;
        let elements: Vec<SampleUtxo> = (0..5u8)
            .map(|seed| {
                SampleUtxo::new(
                    seed.wrapping_mul(37).wrapping_add(1),
                    u32::from(seed),
                    u64::from(seed) * 1_000 + 7,
                    u32::from(seed) * 3 + 1,
                )
            })
            .collect();
        let mut serial = TransparentUtxoSetCommitment::empty();
        for element in &elements {
            element.fold_into(&mut serial);
        }
        let split_count = PARTS.pow(u32::try_from(elements.len()).unwrap_or(u32::MAX));
        for split in 0..split_count {
            let mut parts: [TransparentUtxoSetCommitment; PARTS] =
                core::array::from_fn(|_| TransparentUtxoSetCommitment::empty());
            let mut assignment = split;
            for element in &elements {
                element.fold_into(&mut parts[assignment % PARTS]);
                assignment /= PARTS;
            }
            let [mut combined, middle, last] = parts;
            combined.combine(&last)?;
            combined.combine(&middle)?;
            assert_eq!(combined, serial, "split {split}");
        }
        Ok(())
    }

    #[test]
    fn combining_an_empty_commitment_changes_nothing() -> Result<(), UtxoSetCommitmentSchemeMismatch>
    {
        let mut commitment = TransparentUtxoSetCommitment::empty();
        SampleUtxo::new(9, 1, 5, 2).fold_into(&mut commitment);
        let before = commitment.clone();
        commitment.combine(&TransparentUtxoSetCommitment::empty())?;
        assert_eq!(commitment, before);
        Ok(())
    }

    #[test]
    fn combine_refuses_a_commitment_of_another_scheme_without_changing_the_target() {
        let mut lthash = TransparentUtxoSetCommitment::empty();
        SampleUtxo::new(9, 1, 5, 2).fold_into(&mut lthash);
        let unspecified = TransparentUtxoSetCommitment::from_parts(
            UtxoSetCommitmentScheme::Unspecified,
            lthash.accumulator(),
        )
        .unwrap_or_default();
        let before = lthash.clone();
        assert_eq!(
            lthash.combine(&unspecified),
            Err(UtxoSetCommitmentSchemeMismatch {
                target: UtxoSetCommitmentScheme::LtHash16ChaCha12,
                added: UtxoSetCommitmentScheme::Unspecified,
            })
        );
        assert_eq!(lthash, before);
        let mut target = unspecified.clone();
        assert!(target.combine(&before).is_err());
        assert_eq!(target, unspecified);
    }

    /// Checks a digest from an independent implementation.
    ///
    /// `reference_expansion.py` in the accumulator scheme evidence computed it
    /// with Python's standard `hashlib.blake2b` and its own `ChaCha12`.
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
            hex::decode("2aba63e2c2490a0a06c5cc94295ff8d096c32c7b0dcb575d55721f0d88756579")
                .unwrap_or_default();
        assert_eq!(commitment.display_digest().to_vec(), expected);
    }

    #[test]
    fn scheme_id_round_trips() {
        assert_eq!(
            UtxoSetCommitmentScheme::from_id(0),
            Some(UtxoSetCommitmentScheme::Unspecified)
        );
        assert_eq!(UtxoSetCommitmentScheme::from_id(2), None);
        assert_eq!(
            UtxoSetCommitmentScheme::from_id(1),
            Some(UtxoSetCommitmentScheme::LtHash16ChaCha12)
        );
        assert_eq!(UtxoSetCommitmentScheme::from_id(3), None);
        assert_eq!(UtxoSetCommitmentScheme::LtHash16ChaCha12.id(), 1);
    }
}
