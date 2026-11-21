use pasta_curves::{
    group::{Curve as _, Group as _, GroupEncoding as _},
    pallas,
};

pub(crate) const MAGIC: &[u8] = b"ZSCAN\x01";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Pool {
    Orchard,
    Ironwood,
}

#[derive(Clone, Debug)]
pub(crate) struct ScanAction {
    pub(crate) pool: Pool,
    pub(crate) nullifier: [u8; 32],
    pub(crate) cmx: [u8; 32],
    pub(crate) epk: [u8; 32],
    pub(crate) ciphertext: [u8; 52],
    pub(crate) hint: Option<[u8; 32]>,
}

pub(crate) fn shifted_ephemeral_key(epk: &[u8; 32]) -> Option<[u8; 32]> {
    let affine = Option::<pallas::Affine>::from(pallas::Affine::from_bytes(epk))?;
    let point = pallas::Point::from(affine);
    if bool::from(point.is_identity()) {
        return None;
    }
    let shifted = (0..64).fold(point, |acc, _| acc.double());
    Some(shifted.to_affine().to_bytes())
}
