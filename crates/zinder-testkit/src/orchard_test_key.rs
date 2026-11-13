//! Test-only Orchard receiver and Regtest unified address.
//!
//! [`OrchardTestKey`] derives a deterministic Orchard spending key from a test
//! seed. Its address receives the notes of the shielding spends that
//! [`crate::TransparentTestKey::build_p2pkh_shielding_spend`] builds, in either
//! the Orchard or the Ironwood pool, which share one address format.
//!
//! [`regtest_unified_address`] combines that receiver with a transparent test
//! key's P2PKH receiver. Zakura 1.8.6, mining to this address, pays the
//! transparent receiver before NU6.3 and an Ironwood note to the Orchard
//! receiver from NU6.3 onward, so the test seed controls every coinbase
//! output.

use orchard::keys::{FullViewingKey, Scope, SpendingKey};
use zcash_address::unified::{self, Encoding as _};
use zcash_protocol::consensus::NetworkType;
use zcash_transparent::address::TransparentAddress;
use zip32::AccountId;

use crate::transparent_signer::{TransparentSignerError, TransparentTestKey};

pub use orchard::Address as OrchardAddress;

/// Deterministic Orchard test key for Regtest, derived with ZIP 32 at
/// `m/32'/1'/0'` from a fixed seed.
pub struct OrchardTestKey {
    full_viewing_key: FullViewingKey,
}

impl OrchardTestKey {
    /// Derives the Orchard test key for account 0 from `seed`, using the
    /// Regtest coin type.
    pub fn from_seed(seed: &[u8]) -> Result<Self, TransparentSignerError> {
        let spending_key = SpendingKey::from_zip32_seed(
            seed,
            zcash_protocol::constants::regtest::COIN_TYPE,
            AccountId::ZERO,
        )
        .map_err(|error| TransparentSignerError::ShieldedKey(error.to_string()))?;
        Ok(Self {
            full_viewing_key: FullViewingKey::from(&spending_key),
        })
    }

    /// Returns the external address at diversifier index 0.
    #[must_use]
    pub fn address(&self) -> OrchardAddress {
        self.full_viewing_key.address_at(0_u32, Scope::External)
    }
}

impl std::fmt::Debug for OrchardTestKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrchardTestKey")
            .field("full_viewing_key", &"[REDACTED]")
            .finish()
    }
}

/// Encodes a Regtest unified address with the P2PKH receiver of
/// `transparent_key` and the Orchard receiver of `orchard_key`.
pub fn regtest_unified_address(
    transparent_key: &TransparentTestKey,
    orchard_key: &OrchardTestKey,
) -> Result<String, TransparentSignerError> {
    let pubkey_hash = match transparent_key.address() {
        TransparentAddress::PublicKeyHash(hash) | TransparentAddress::ScriptHash(hash) => *hash,
    };
    let address = unified::Address::try_from_items(
        unified::Revision::R0,
        vec![
            unified::Uitem::Data(unified::Receiver::Orchard(
                orchard_key.address().to_raw_address_bytes(),
            )),
            unified::Uitem::Data(unified::Receiver::P2pkh(pubkey_hash)),
        ],
    )
    .map_err(|error| TransparentSignerError::ShieldedKey(error.to_string()))?;
    Ok(address.encode(&NetworkType::Regtest))
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::{OrchardTestKey, regtest_unified_address};
    use crate::transparent_signer::{
        TRANSPARENT_BROADCAST_TEST_SEED, TransparentSignerError, TransparentTestKey,
    };

    #[test]
    fn unified_address_carries_both_test_receivers() -> Result<(), TransparentSignerError> {
        let transparent_key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
        let orchard_key = OrchardTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
        let encoded = regtest_unified_address(&transparent_key, &orchard_key)?;

        let address: zcash_address::ZcashAddress =
            encoded
                .parse()
                .map_err(|error: zcash_address::ParseError| {
                    TransparentSignerError::ShieldedKey(error.to_string())
                })?;
        assert!(encoded.starts_with("uregtest1"), "{encoded}");
        assert!(address.can_receive_as(zcash_protocol::PoolType::ORCHARD));
        assert!(address.can_receive_as(zcash_protocol::PoolType::TRANSPARENT));
        assert!(!address.can_receive_as(zcash_protocol::PoolType::SAPLING));
        Ok(())
    }

    /// The fixture-capture node configuration mines to the address this
    /// module derives, so the captured coinbase notes belong to the test keys.
    #[test]
    fn fixture_capture_configuration_mines_to_the_test_unified_address()
    -> Result<(), TransparentSignerError> {
        let transparent_key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
        let orchard_key = OrchardTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
        let expected = regtest_unified_address(&transparent_key, &orchard_key)?;
        let capture_config = include_str!("../../../deploy/zakura/regtest-fixture-capture.toml");
        assert!(
            capture_config.contains(&format!("miner_address = \"{expected}\"")),
            "deploy/zakura/regtest-fixture-capture.toml must mine to {expected}"
        );
        Ok(())
    }
}
