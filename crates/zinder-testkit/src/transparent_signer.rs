//! Test-only transparent v5 transaction builder.
//!
//! Produces signed v5 transparent transaction bytes that a Zebra regtest
//! sidecar will accept through `sendrawtransaction`. The signer wraps
//! `zcash_primitives::transaction::builder::Builder::mock_build`, which is
//! the test-only path that skips Sapling/Orchard provers; transparent-only
//! transactions never invoke them.
//!
//! The signer exists for the regtest broadcast cycle without depending on
//! Zallet (Zallet v0.1.0-alpha.3 is shielded-first, Zebra refuses
//! non-transparent miner addresses). It is unsuitable for any production
//! signing path: every secret derivation flows through `mock_build`'s
//! deterministic RNG and the test-vector code paths.
//!
//! # Activation heights
//!
//! [`regtest_local_network`] returns the [`LocalNetwork`] shape ZFND's `z3`
//! regtest sidecar is configured with by default (`overwinter..canopy = 1`,
//! `nu5 = 2`, `nu6 = 2`, `nu6_2 = 3`, later upgrades unset). It is derived from
//! [`crate::network_upgrade_fixtures::sample_regtest_upgrade_activations`]
//! so the two fixtures cannot drift apart. Intended for in-process unit-test
//! fixtures that do not broadcast to a live node.
//!
//! Live tests that broadcast through Zebra must derive the [`LocalNetwork`]
//! from the running node instead, via
//! [`crate::network_upgrade_fixtures::local_network_from_activations`]
//! applied to a
//! `ZebraJsonRpcSource::fetch_network_upgrade_activations()` result.
//! Mismatched activation heights produce an `incorrect consensus branch id`
//! rejection from Zebra's mempool.

use rand::rng;
use secp256k1::PublicKey;
use thiserror::Error;
use zcash_address::{ToAddress, ZcashAddress};
use zcash_primitives::transaction::builder::{BuildConfig, Builder, BundlePadding};
use zcash_protocol::{
    consensus::{BlockHeight, NetworkType},
    value::{BalanceError, Zatoshis},
};
use zcash_transparent::address::Script as TransparentScript;
use zcash_transparent::{
    builder::TransparentSigningSet,
    bundle::{OutPoint, TxOut},
    keys::{AccountPrivKey, NonHardenedChildIndex},
};

pub use zcash_protocol::local_consensus::LocalNetwork;
pub use zcash_transparent::address::TransparentAddress;
use zip32::AccountId;

/// Test seed for live regtest transparent-broadcast gates.
///
/// Operators configure Zebra's regtest miner address to the address derived
/// from this seed, so live tests can find a mature transparent coinbase output
/// they are allowed to spend.
pub const TRANSPARENT_BROADCAST_TEST_SEED: [u8; 32] = [0x42_u8; 32];

/// Exact fee charged for a v5 transaction with one transparent input and
/// one transparent output under ZIP-317's `FeeRule::standard()`.
///
/// `Builder::mock_build` always enforces ZIP-317; the fee is not a free
/// parameter. The standard rule charges `5_000` zatoshis per logical
/// action with a grace floor of 2 actions, so a `1-in / 1-out` transaction
/// pays exactly `5_000 * 2 = 10_000` zatoshis.
pub const ZIP317_FEE_ONE_IN_ONE_OUT_ZATS: u64 = 10_000;

/// Errors raised while constructing or signing a transparent v5 transaction.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransparentSignerError {
    /// BIP44 account-key derivation from the seed failed.
    #[error("BIP44 account-key derivation failed: {0}")]
    AccountKey(String),
    /// External-secret-key derivation failed.
    #[error("external secret-key derivation failed: {0}")]
    ExternalKey(String),
    /// Adding the transparent input to the builder failed.
    #[error("could not add transparent input: {0}")]
    AddInput(String),
    /// Adding the transparent output to the builder failed.
    #[error("could not add transparent output: {0}")]
    AddOutput(String),
    /// The configured value did not fit a valid `Zatoshis` amount.
    #[error("amount {amount_zats} zats is not a valid Zatoshis amount: {source}")]
    InvalidValue {
        /// The rejected amount, in zatoshis.
        amount_zats: u64,
        /// The underlying balance error.
        #[source]
        source: BalanceError,
    },
    /// Fee equals or exceeds the spent value, leaving no room for an output.
    #[error("fee {fee_zats} zats does not leave a positive output (input was {value_zats} zats)")]
    FeeExceedsValue {
        /// The configured input value.
        value_zats: u64,
        /// The configured fee.
        fee_zats: u64,
    },
    /// `Builder::mock_build` rejected the transaction.
    #[error("transaction build failed: {0}")]
    Build(String),
    /// Serializing the built transaction to bytes failed.
    #[error("transaction serialization failed: {source}")]
    Serialize {
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Deterministic transparent test key derived from a fixed seed.
///
/// One `TransparentTestKey` represents a single BIP44 external receiver at
/// `m/44'/<regtest-coin>'/0'/0/0`. The address is the P2PKH encoding of the
/// derived `secp256k1` public key.
pub struct TransparentTestKey {
    account_key: AccountPrivKey,
    pubkey: PublicKey,
    address: TransparentAddress,
    params: LocalNetwork,
}

/// Inputs for [`TransparentTestKey::build_p2pkh_spend`].
#[derive(Clone, Debug)]
pub struct P2pkhSpendArgs<'a> {
    /// 32-byte coinbase txid in **wire byte order** (big-endian; the order
    /// Zebra writes to the network and to the `OutPoint`). JSON-RPC
    /// responses display the txid as reversed hex; callers must reverse the
    /// displayed bytes before passing them in.
    pub coinbase_txid_be: [u8; 32],
    /// Output index of the coinbase UTXO being spent.
    pub coinbase_vout: u32,
    /// Coinbase output value in zatoshis.
    pub coinbase_value_zats: u64,
    /// Recipient address.
    pub recipient: &'a TransparentAddress,
    /// Block height the transaction targets. Must satisfy regtest coinbase
    /// maturity (`coinbase_height + 100 <= target_height`).
    pub target_height: u32,
}

impl TransparentTestKey {
    /// Derives a deterministic test key from the supplied seed using the
    /// local regtest activation heights.
    ///
    /// The seed must be at least 32 bytes (BIP32 minimum). Shorter seeds are
    /// rejected by the underlying derivation.
    pub fn from_seed(seed: &[u8]) -> Result<Self, TransparentSignerError> {
        Self::from_seed_with_local_network(seed, regtest_local_network())
    }

    /// Same as [`TransparentTestKey::from_seed`], but uses the supplied
    /// network parameters. Use this when the regtest Zebra has custom NU
    /// activation heights.
    pub fn from_seed_with_local_network(
        seed: &[u8],
        params: LocalNetwork,
    ) -> Result<Self, TransparentSignerError> {
        let account_key = AccountPrivKey::from_seed(&params, seed, AccountId::ZERO)
            .map_err(|error| TransparentSignerError::AccountKey(error.to_string()))?;
        let secret_key = account_key
            .derive_external_secret_key(NonHardenedChildIndex::ZERO)
            .map_err(|error| TransparentSignerError::ExternalKey(error.to_string()))?;
        let pubkey = PublicKey::from_secret_key(&secret_key);
        let address = TransparentAddress::from_pubkey(&pubkey);
        Ok(Self {
            account_key,
            pubkey,
            address,
            params,
        })
    }

    /// Returns a copy of this key bound to a different `LocalNetwork`. The
    /// derived address does not change because address derivation depends on
    /// the network's `coin_type()`, which is the same across regtest
    /// configurations.
    #[must_use]
    pub fn with_local_network(self, params: LocalNetwork) -> Self {
        Self { params, ..self }
    }

    /// Returns the derived P2PKH address.
    #[must_use]
    pub const fn address(&self) -> &TransparentAddress {
        &self.address
    }

    /// Returns the derived `secp256k1` public key.
    #[must_use]
    pub const fn pubkey(&self) -> &PublicKey {
        &self.pubkey
    }

    /// Returns the address's `script_pubkey` bytes as they would appear in a
    /// coinbase output paying to this key. Useful for matching against
    /// `zebra_chain::transparent::Output::lock_script::as_raw_bytes()`.
    #[must_use]
    pub fn address_script_bytes(&self) -> Vec<u8> {
        TransparentScript::from(self.address.script()).0.0
    }

    /// Returns the base58check-encoded address string suitable for
    /// `getaddressutxos` and similar JSON-RPC calls. Regtest p2pkh addresses
    /// share the testnet prefix per `zcash_address::NetworkType::Regtest`.
    #[must_use]
    pub fn address_base58(&self) -> String {
        let pubkey_hash = match self.address {
            TransparentAddress::PublicKeyHash(hash) | TransparentAddress::ScriptHash(hash) => hash,
        };
        ZcashAddress::from_transparent_p2pkh(NetworkType::Regtest, pubkey_hash).encode()
    }

    /// Returns a deterministic recipient address distinct from this key's
    /// funded address.
    ///
    /// Live tests use this to spend a miner-address UTXO without sending the
    /// change back to the same script, making the test transaction's output
    /// shape easy to recognize.
    #[must_use]
    pub fn scratch_recipient_address(&self, salt: u64) -> TransparentAddress {
        let funded_hash = match self.address {
            TransparentAddress::PublicKeyHash(hash) | TransparentAddress::ScriptHash(hash) => hash,
        };
        let salt_bytes = salt.to_le_bytes();
        let mut scratch = [0_u8; 20];
        for (index, byte) in funded_hash.iter().enumerate() {
            scratch[index] = byte ^ 0xFF ^ salt_bytes[index % salt_bytes.len()];
        }
        TransparentAddress::PublicKeyHash(scratch)
    }

    /// Builds and signs a v5 transparent transaction that spends one P2PKH
    /// UTXO and forwards `value - ZIP317_FEE_ONE_IN_ONE_OUT_ZATS` to the
    /// recipient.
    ///
    /// The fee is fixed by ZIP-317 and not a free parameter; see
    /// [`ZIP317_FEE_ONE_IN_ONE_OUT_ZATS`].
    pub fn build_p2pkh_spend(
        &self,
        args: &P2pkhSpendArgs<'_>,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        let fee_zats = ZIP317_FEE_ONE_IN_ONE_OUT_ZATS;
        if args.coinbase_value_zats <= fee_zats {
            return Err(TransparentSignerError::FeeExceedsValue {
                value_zats: args.coinbase_value_zats,
                fee_zats,
            });
        }
        let coinbase_amount = zatoshis_from_u64(args.coinbase_value_zats)?;
        let send_amount = zatoshis_from_u64(args.coinbase_value_zats - fee_zats)?;

        let mut signing_set = TransparentSigningSet::new();
        let secret_key = self
            .account_key
            .derive_external_secret_key(NonHardenedChildIndex::ZERO)
            .map_err(|error| TransparentSignerError::ExternalKey(error.to_string()))?;
        let pubkey = signing_set.add_key(secret_key);

        let coin = TxOut::new(
            coinbase_amount,
            TransparentAddress::from_pubkey(&pubkey).script().into(),
        );
        let outpoint = OutPoint::new(args.coinbase_txid_be, args.coinbase_vout);

        let mut builder = Builder::new(
            self.params,
            BlockHeight::from_u32(args.target_height),
            BuildConfig::Standard {
                sapling_anchor: None,
                orchard_anchor: None,
                ironwood_anchor: None,
                orchard_padding: BundlePadding::DEFAULT,
                ironwood_padding: BundlePadding::DEFAULT,
            },
        );
        builder
            .add_transparent_p2pkh_input(pubkey, outpoint, coin)
            .map_err(|error| TransparentSignerError::AddInput(error.to_string()))?;
        builder
            .add_transparent_output(args.recipient, send_amount)
            .map_err(|error| TransparentSignerError::AddOutput(error.to_string()))?;

        let build_outcome = builder
            .mock_build(&signing_set, &[], &[], rng())
            .map_err(|error| TransparentSignerError::Build(error.to_string()))?;

        let mut bytes = Vec::new();
        build_outcome
            .transaction()
            .write(&mut bytes)
            .map_err(|source| TransparentSignerError::Serialize { source })?;
        Ok(bytes)
    }
}

impl std::fmt::Debug for TransparentTestKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransparentTestKey")
            .field("address", &self.address_base58())
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

/// Activation heights for the `z3` regtest sidecar:
/// `overwinter..canopy = 1`, `nu5 = 2`, `nu6 = 2`, `nu6_2 = 3`,
/// later upgrades unset.
///
/// Derived from
/// [`crate::network_upgrade_fixtures::sample_regtest_upgrade_activations`]
/// via
/// [`crate::network_upgrade_fixtures::local_network_from_activations`] so
/// the two fixtures cannot drift apart. Intended for unit-test fixtures
/// that do not broadcast. Live tests that broadcast through Zebra must
/// derive the [`LocalNetwork`] from the running node, via
/// [`crate::network_upgrade_fixtures::local_network_from_activations`].
#[must_use]
pub fn regtest_local_network() -> LocalNetwork {
    crate::network_upgrade_fixtures::local_network_from_activations(
        &crate::network_upgrade_fixtures::sample_regtest_upgrade_activations(),
    )
}

fn zatoshis_from_u64(zats: u64) -> Result<Zatoshis, TransparentSignerError> {
    Zatoshis::from_u64(zats).map_err(|source| TransparentSignerError::InvalidValue {
        amount_zats: zats,
        source,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::{P2pkhSpendArgs, TransparentSignerError, TransparentTestKey};
    use zcash_transparent::address::TransparentAddress;

    fn fixed_seed() -> [u8; 32] {
        [0x42_u8; 32]
    }

    #[test]
    fn from_seed_is_deterministic() -> Result<(), TransparentSignerError> {
        let one = TransparentTestKey::from_seed(&fixed_seed())?;
        let two = TransparentTestKey::from_seed(&fixed_seed())?;
        assert_eq!(one.address(), two.address());
        assert_eq!(one.pubkey(), two.pubkey());
        Ok(())
    }

    #[test]
    fn from_seed_rejects_short_seed() {
        assert!(TransparentTestKey::from_seed(&[0_u8; 8]).is_err());
    }

    #[test]
    fn address_is_p2pkh() -> Result<(), TransparentSignerError> {
        let key = TransparentTestKey::from_seed(&fixed_seed())?;
        assert!(matches!(
            key.address(),
            TransparentAddress::PublicKeyHash(_)
        ));
        Ok(())
    }

    #[test]
    fn address_base58_starts_with_regtest_prefix() -> Result<(), TransparentSignerError> {
        let key = TransparentTestKey::from_seed(&fixed_seed())?;
        let encoded = key.address_base58();
        // Regtest p2pkh addresses share the testnet prefix `tm`.
        assert!(
            encoded.starts_with("tm"),
            "expected regtest/testnet `tm` prefix, got {encoded}"
        );
        Ok(())
    }

    #[test]
    fn debug_redacts_secret() -> Result<(), TransparentSignerError> {
        let key = TransparentTestKey::from_seed(&fixed_seed())?;
        let formatted = format!("{key:?}");
        assert!(formatted.contains("REDACTED"));
        assert!(!formatted.contains("secret_key: SecretKey"));
        Ok(())
    }

    #[test]
    fn build_p2pkh_spend_produces_v5_bytes() -> Result<(), TransparentSignerError> {
        let key = TransparentTestKey::from_seed(&fixed_seed())?;
        let recipient = TransparentAddress::PublicKeyHash([0x11_u8; 20]);
        let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
            coinbase_txid_be: [0xaa_u8; 32],
            coinbase_vout: 0,
            coinbase_value_zats: 10_000_000,
            recipient: &recipient,
            target_height: 120,
        })?;
        // v5 transactions use header bytes 0x05 0x00 0x00 0x80 (version 5,
        // overwintered) per ZIP-225 §Transaction Format.
        assert_eq!(&bytes[0..4], &[0x05, 0x00, 0x00, 0x80]);
        assert!(
            bytes.len() > 64,
            "serialized tx is too short: {}",
            bytes.len()
        );
        Ok(())
    }

    #[test]
    fn build_p2pkh_spend_rejects_fee_exceeding_value() -> Result<(), TransparentSignerError> {
        let key = TransparentTestKey::from_seed(&fixed_seed())?;
        let recipient = TransparentAddress::PublicKeyHash([0x22_u8; 20]);
        let outcome = key.build_p2pkh_spend(&P2pkhSpendArgs {
            coinbase_txid_be: [0_u8; 32],
            coinbase_vout: 0,
            coinbase_value_zats: 500,
            recipient: &recipient,
            target_height: 1,
        });
        assert!(matches!(
            outcome,
            Err(TransparentSignerError::FeeExceedsValue { .. })
        ));
        Ok(())
    }
}
