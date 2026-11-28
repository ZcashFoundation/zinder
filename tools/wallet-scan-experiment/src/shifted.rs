//! Variable-time key agreement for public benchmark viewing keys.
//!
//! Each signed GLV half contributes its low 64 digits on the original
//! base and its high 64 digits on the server-supplied shifted base.

use num_bigint::{BigInt, Sign};
use orchard::keys::IncomingViewingKey;
use pasta_curves::{
    arithmetic::CurveExt as _,
    glv::GlvParams,
    group::{
        Curve as _, CurveAffine as _, Group as _, GroupEncoding as _,
        ff::{Field as _, PrimeField as _},
    },
    pallas,
};

fn decode_nonidentity(bytes: &[u8; 32]) -> Option<pallas::Point> {
    let point = Option::<pallas::Affine>::from(pallas::Affine::from_bytes(bytes))?;
    (!bool::from(point.is_identity())).then(|| pallas::Point::from(point))
}

pub(crate) struct HintedKey {
    ordinary: [[i8; 2]; 128],
    hinted: [[i8; 4]; 64],
}

impl HintedKey {
    pub(crate) fn new(key: &IncomingViewingKey) -> Option<Self> {
        let bytes = key.to_bytes()[32..].try_into().ok()?;
        Self::from_scalar(Option::from(pallas::Scalar::from_repr(bytes))?)
    }

    fn from_scalar(scalar: pallas::Scalar) -> Option<Self> {
        let k = BigInt::from_bytes_le(Sign::Plus, &scalar.to_repr());
        let modulus = BigInt::from_bytes_le(Sign::Plus, &(-pallas::Scalar::ONE).to_repr()) + 1u8;
        // Round in the public Pallas GLV lattice basis. The upstream decomposed
        // scalar hides its digits, which this ladder needs on either side of bit 64.
        let nearest = |coefficient: u128| (&k * coefficient + &modulus / 2u8) / &modulus;
        let c1 = nearest(pallas::Point::V2B);
        let c2 = nearest(pallas::Point::V1B_NEG);
        let a = k - &c1 * pallas::Point::V1A - &c2 * pallas::Point::V2A;
        let b = c1 * pallas::Point::V1B_NEG - c2 * pallas::Point::V2B;
        let mut ordinary = [[0; 2]; 128];
        for (half, coefficient) in [a, b].iter().enumerate() {
            let mut magnitude = u128::try_from(coefficient.magnitude()).ok()?;
            for column in &mut ordinary {
                if magnitude == 0 {
                    break;
                }
                if magnitude & 1 != 0 {
                    let low = i8::try_from(magnitude & 15).ok()?;
                    let digit = if low > 7 { low - 16 } else { low };
                    magnitude = magnitude.checked_add_signed(-i128::from(digit))?;
                    column[half] = if coefficient.sign() == Sign::Minus {
                        -digit
                    } else {
                        digit
                    };
                }
                magnitude >>= 1;
            }
            if magnitude != 0 {
                return None;
            }
        }
        let hinted = std::array::from_fn(|i| {
            let [a_lo, b_lo] = ordinary[i];
            let [a_hi, b_hi] = ordinary[i + 64];
            [a_lo, b_lo, a_hi, b_hi]
        });
        Some(Self { ordinary, hinted })
    }
}

fn multiply<const LANES: usize>(
    columns: &[[i8; LANES]],
    table: &[pallas::Affine],
) -> pallas::Point {
    let mut acc = pallas::Point::identity();
    for digits in columns.iter().rev() {
        acc = acc.double();
        for (lane, &digit) in digits.iter().enumerate() {
            if digit != 0 {
                let odd = table[lane * 4 + usize::from(digit.unsigned_abs() / 2)];
                acc += if digit > 0 { odd } else { -odd };
            }
        }
    }
    acc
}

enum EphemeralKey {
    Invalid,
    Ordinary { offset: usize },
    Hinted { offset: usize },
}

pub(crate) struct HintedBatch {
    keys: Vec<EphemeralKey>,
    windows: Vec<pallas::Affine>,
}

fn append_window(windows: &mut Vec<pallas::Point>, base: pallas::Point) {
    let twice = base.double();
    let three = base + twice;
    let five = three + twice;
    let odd = [base, three, five, five + twice];
    windows.extend(odd);
    windows.extend(odd.map(|point| point.endo()));
}

impl HintedBatch {
    pub(crate) fn ordinary(keys: impl IntoIterator<Item = [u8; 32]>) -> Self {
        Self::new(keys.into_iter().map(|epk| (epk, None)))
    }

    pub(crate) fn new(keys: impl IntoIterator<Item = ([u8; 32], Option<[u8; 32]>)>) -> Self {
        let mut projective = Vec::new();
        let keys = keys
            .into_iter()
            .map(|(epk, hint)| {
                let Some(base) = decode_nonidentity(&epk) else {
                    return EphemeralKey::Invalid;
                };
                let offset = projective.len();
                append_window(&mut projective, base);
                let Some(shifted) = hint.as_ref().and_then(decode_nonidentity) else {
                    return EphemeralKey::Ordinary { offset };
                };
                append_window(&mut projective, shifted);
                EphemeralKey::Hinted { offset }
            })
            .collect();
        let mut windows = vec![pallas::Affine::identity(); projective.len()];
        pallas::Point::batch_normalize(&projective, &mut windows);
        Self { keys, windows }
    }

    pub(crate) fn shared_secrets(&self, key: &HintedKey) -> Vec<Option<[u8; 32]>> {
        let projective: Vec<_> = self
            .keys
            .iter()
            .map(|epk| match epk {
                EphemeralKey::Invalid => pallas::Point::identity(),
                EphemeralKey::Ordinary { offset } => {
                    multiply(&key.ordinary, &self.windows[*offset..*offset + 8])
                }
                EphemeralKey::Hinted { offset } => {
                    multiply(&key.hinted, &self.windows[*offset..*offset + 16])
                }
            })
            .collect();
        let mut affine = vec![pallas::Affine::identity(); projective.len()];
        pallas::Point::batch_normalize(&projective, &mut affine);
        affine
            .into_iter()
            .map(|point| (!bool::from(point.is_identity())).then(|| point.to_bytes()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::corpus::shifted_ephemeral_key;

    use super::*;

    #[test]
    fn split_products_match_stock_key_agreement() -> Result<(), &'static str> {
        let radix = pallas::Scalar::from_u128(1u128 << 64);
        let generator = pallas::Point::generator();
        let bases = [generator, generator.double(), generator * radix, -generator];
        let ordinary = HintedBatch::ordinary(bases.map(|base| base.to_bytes()));
        let batch = HintedBatch::new(bases.iter().map(|base| {
            let bytes = base.to_bytes();
            (bytes, shifted_ephemeral_key(&bytes))
        }));
        for base in bases {
            assert_eq!(
                shifted_ephemeral_key(&base.to_bytes()),
                Some((base * radix).to_bytes())
            );
        }
        let mut scalars = vec![
            pallas::Scalar::ZERO,
            pallas::Scalar::ONE,
            -pallas::Scalar::ONE,
            radix - pallas::Scalar::ONE,
            radix,
            radix + pallas::Scalar::ONE,
            radix.square(),
        ];
        let mut scalar = radix + pallas::Scalar::from(19);
        for _ in 0..32 {
            scalar = scalar.square() + pallas::Scalar::from(7);
            scalars.push(scalar);
        }
        for scalar in scalars {
            let key = HintedKey::from_scalar(scalar).ok_or("invalid GLV split")?;
            let expected = bases
                .map(|base| (!bool::from(scalar.is_zero())).then(|| (base * scalar).to_bytes()));
            assert_eq!(batch.shared_secrets(&key), expected);
            assert_eq!(ordinary.shared_secrets(&key), expected);
        }
        Ok(())
    }

    #[test]
    fn malformed_hints_fall_back_but_unrelated_points_change_the_product()
    -> Result<(), &'static str> {
        let base = pallas::Point::generator();
        let scalar = -pallas::Scalar::from(1_234_567)
            .invert()
            .into_option()
            .ok_or("nonzero denominator")?;
        let bytes = base.to_bytes();
        let identity = pallas::Point::identity().to_bytes();
        let key = HintedKey::from_scalar(scalar).ok_or("invalid GLV split")?;
        assert_eq!(shifted_ephemeral_key(&identity), None);
        assert_eq!(shifted_ephemeral_key(&[0xff; 32]), None);
        let answers = HintedBatch::new([
            (bytes, None),
            (bytes, Some([0xff; 32])),
            (bytes, Some(identity)),
            (bytes, shifted_ephemeral_key(&bytes)),
            (bytes, Some(bytes)),
            (identity, Some(bytes)),
            ([0xff; 32], None),
        ])
        .shared_secrets(&key);
        let expected = Some((base * scalar).to_bytes());
        assert_eq!(answers[..4], [expected; 4]);
        assert_ne!(answers[4], expected);
        assert_eq!(answers[5..], [None; 2]);
        Ok(())
    }
}
