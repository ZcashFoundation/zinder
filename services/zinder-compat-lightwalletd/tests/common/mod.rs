pub(crate) mod nullifier_parity;
pub(crate) mod verified_node;

pub(crate) mod canonical_control;
pub(crate) use canonical_control::{MutableCanonicalControl, writer_status_for_fence};

pub(crate) fn shielded_catalog_keys(
    directory: &std::path::Path,
) -> eyre::Result<(std::path::PathBuf, std::path::PathBuf)> {
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair as _},
    };
    let private = directory.join("signing.pk8");
    let public = directory.join("verification.pub");
    let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| eyre::eyre!("cannot generate fixture key"))?;
    let key = Ed25519KeyPair::from_pkcs8(document.as_ref())
        .map_err(|_| eyre::eyre!("cannot open fixture key"))?;
    std::fs::write(&private, document.as_ref())?;
    std::fs::write(&public, key.public_key().as_ref())?;
    Ok((private, public))
}
