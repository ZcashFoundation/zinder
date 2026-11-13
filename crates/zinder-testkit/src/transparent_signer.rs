//! Test-only transaction builder that spends transparent outputs.
//!
//! Produces signed transaction bytes that a Regtest node accepts through
//! `sendrawtransaction`: transparent spends in the version the builder
//! suggests for the target height, or version 4 on request, and shielding
//! spends that move one transparent output into the Orchard or Ironwood pool.
//! The signer wraps `zcash_primitives::transaction::builder::Builder::mock_build`,
//! the test-only build path. It mocks only the Sapling provers; Orchard and
//! Ironwood bundles still carry real Halo2 proofs, so a shielding spend
//! verifies on a node.
//!
//! The signer exists for the Regtest broadcast cycle and fixture capture
//! without an external wallet. It is unsuitable for any production signing
//! path: its keys come from a fixed, public seed, and `mock_build` accepts any
//! random number generator as a cryptographic one.
//!
//! # Activation heights
//!
//! [`regtest_local_network`] returns the [`LocalNetwork`] shape of the captured
//! ZFND `z3` regtest fixtures (`overwinter..canopy = 1`, `nu5 = 2`, `nu6 = 2`,
//! `nu6_1 = 3`, `nu6_2 = 3`, `nu6_3 = 603`, `nu7 = 703`). It is derived from
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

use secp256k1::PublicKey;
use thiserror::Error;
use zcash_address::{ToAddress, ZcashAddress};
use zcash_primitives::transaction::{
    TxVersion,
    builder::{BuildConfig, Builder, BundlePadding},
    fees::zip317,
};
use zcash_protocol::{
    consensus::{BlockHeight, NetworkType},
    memo::MemoBytes,
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

use crate::orchard_test_key::OrchardAddress;

/// Test seed for live regtest transparent-broadcast gates.
///
/// Operators configure Zebra's regtest miner address to the address derived
/// from this seed, so live tests can find a mature transparent coinbase output
/// they are allowed to spend.
pub const TRANSPARENT_BROADCAST_TEST_SEED: [u8; 32] = [0x42_u8; 32];

/// Exact fee charged for a transaction with one transparent input and
/// one transparent output under ZIP-317's `FeeRule::standard()`.
///
/// `Builder::mock_build` always enforces ZIP-317; the fee is not a free
/// parameter. The standard rule charges `5_000` zatoshis per logical
/// action with a grace floor of 2 actions, so a `1-in / 1-out` transaction
/// pays exactly `5_000 * 2 = 10_000` zatoshis.
pub const ZIP317_FEE_ONE_IN_ONE_OUT_ZATS: u64 = 10_000;

/// Errors raised while constructing or signing a transparent transaction.
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
    /// Adding the shielded output to the builder failed, for example because
    /// the pool does not accept new value at the target height.
    #[error("could not add {pool:?} output: {reason}")]
    AddShieldedOutput {
        /// The pool that rejected the output.
        pool: ShieldedOutputPool,
        /// The builder's reason.
        reason: String,
    },
    /// Shielded test-key derivation or address encoding failed.
    #[error("shielded test key derivation failed: {0}")]
    ShieldedKey(String),
    /// The builder could not compute the ZIP-317 fee.
    #[error("ZIP-317 fee computation failed: {0}")]
    Fee(String),
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

/// Shielded pool that receives the output of a shielding spend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShieldedOutputPool {
    /// The Orchard pool, which accepts new value from NU5 until NU6.3.
    Orchard,
    /// The Ironwood pool, from NU6.3 onward.
    Ironwood,
}

/// Inputs for [`TransparentTestKey::build_p2pkh_shielding_spend`].
#[derive(Clone, Debug)]
pub struct P2pkhShieldingSpendArgs<'a> {
    /// 32-byte txid of the spent output in wire byte order; see
    /// [`P2pkhSpendArgs::coinbase_txid_be`].
    pub coinbase_txid_be: [u8; 32],
    /// Output index of the spent output.
    pub coinbase_vout: u32,
    /// Value of the spent output in zatoshis.
    pub coinbase_value_zats: u64,
    /// Pool that receives the shielded note.
    pub pool: ShieldedOutputPool,
    /// Recipient of the shielded note, an address of
    /// [`crate::OrchardTestKey`] or any other Orchard-format address.
    pub recipient: &'a OrchardAddress,
    /// Block height the transaction targets. Must satisfy regtest coinbase
    /// maturity when the spent output is a coinbase output.
    pub target_height: u32,
}

/// One transparent P2PKH output of the test key, ready to add to a builder.
struct SpentTestOutput {
    signing_set: TransparentSigningSet,
    pubkey: PublicKey,
    outpoint: OutPoint,
    coin: TxOut,
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

    /// Builds and signs a transparent transaction that spends one P2PKH
    /// UTXO and forwards `value - ZIP317_FEE_ONE_IN_ONE_OUT_ZATS` to the
    /// recipient.
    ///
    /// The version is the one the builder suggests for `args.target_height`:
    /// version 5 from NU5 and version 6 from NU6.3.
    ///
    /// The fee is fixed by ZIP-317 and not a free parameter; see
    /// [`ZIP317_FEE_ONE_IN_ONE_OUT_ZATS`].
    pub fn build_p2pkh_spend(
        &self,
        args: &P2pkhSpendArgs<'_>,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        self.build_p2pkh_spend_as(args, None)
    }

    /// Builds and signs a version-4 transparent transaction with the same
    /// shape as [`TransparentTestKey::build_p2pkh_spend`].
    ///
    /// `args.target_height` must fall in a branch that admits version 4
    /// (Sapling through NU6.3); the signature commits to that branch. The
    /// transaction expires at `expiry_height`, so a caller can submit it after
    /// a later activation, such as NU7, which rejects version 4 under ZIP 2003.
    pub fn build_p2pkh_spend_v4(
        &self,
        args: &P2pkhSpendArgs<'_>,
        expiry_height: u32,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        self.build_p2pkh_spend_as(args, Some(BlockHeight::from_u32(expiry_height)))
    }

    /// Builds the one-input, one-output spend. `v4_expiry_height` selects a
    /// version-4 transaction with that expiry; `None` keeps the version and
    /// expiry the builder suggests for the target height.
    fn build_p2pkh_spend_as(
        &self,
        args: &P2pkhSpendArgs<'_>,
        v4_expiry_height: Option<BlockHeight>,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        let fee_zats = ZIP317_FEE_ONE_IN_ONE_OUT_ZATS;
        if args.coinbase_value_zats <= fee_zats {
            return Err(TransparentSignerError::FeeExceedsValue {
                value_zats: args.coinbase_value_zats,
                fee_zats,
            });
        }
        let send_amount = zatoshis_from_u64(args.coinbase_value_zats - fee_zats)?;
        let SpentTestOutput {
            signing_set,
            pubkey,
            outpoint,
            coin,
        } = self.spent_output(
            args.coinbase_txid_be,
            args.coinbase_vout,
            args.coinbase_value_zats,
        )?;

        let mut builder = Builder::new(
            self.params,
            BlockHeight::from_u32(args.target_height),
            standard_build_config(None),
        );
        if let Some(expiry_height) = v4_expiry_height {
            builder = builder.with_expiry_height(expiry_height);
            builder
                .propose_version::<std::convert::Infallible>(TxVersion::V4)
                .map_err(|error| TransparentSignerError::Build(error.to_string()))?;
        }
        builder
            .add_transparent_p2pkh_input(pubkey, outpoint, coin)
            .map_err(|error| TransparentSignerError::AddInput(error.to_string()))?;
        builder
            .add_transparent_output(args.recipient, send_amount)
            .map_err(|error| TransparentSignerError::AddOutput(error.to_string()))?;

        mock_build_bytes(builder, &signing_set)
    }

    /// Builds and signs a transaction that moves one P2PKH output into a
    /// single shielded note in `args.pool`.
    ///
    /// The version is the one the builder suggests for `args.target_height`.
    /// The note's value is the spent value less the ZIP-317 fee, which counts
    /// the transparent input and the padded shielded actions: 15,000 zatoshis
    /// for one input and one note. The bundle's anchor is the empty tree,
    /// which Zebra 7.0.0 and Zakura 1.8.6 accept for a bundle without real
    /// spends.
    ///
    /// Orchard accepts new value only before NU6.3 and Ironwood only from
    /// NU6.3 onward; any other combination returns
    /// [`TransparentSignerError::AddShieldedOutput`]. Building the bundle's
    /// Halo2 proof takes seconds even in an optimized build.
    pub fn build_p2pkh_shielding_spend(
        &self,
        args: &P2pkhShieldingSpendArgs<'_>,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        // The fee depends on the action count, not on the note value.
        let (fee_probe, _) = self.shielding_builder(args, Zatoshis::ZERO)?;
        let fee = fee_probe
            .get_fee(&zip317::FeeRule::standard())
            .map_err(|error| TransparentSignerError::Fee(error.to_string()))?;
        let fee_zats = u64::from(fee);
        let note_value_zats = args
            .coinbase_value_zats
            .checked_sub(fee_zats)
            .filter(|note_value_zats| *note_value_zats > 0)
            .ok_or(TransparentSignerError::FeeExceedsValue {
                value_zats: args.coinbase_value_zats,
                fee_zats,
            })?;
        let (builder, signing_set) =
            self.shielding_builder(args, zatoshis_from_u64(note_value_zats)?)?;
        mock_build_bytes(builder, &signing_set)
    }

    /// Returns a builder that spends the output in `args` into one note of
    /// `note_value` in `args.pool`, and the signing set for its input.
    fn shielding_builder(
        &self,
        args: &P2pkhShieldingSpendArgs<'_>,
        note_value: Zatoshis,
    ) -> Result<(Builder<LocalNetwork, ()>, TransparentSigningSet), TransparentSignerError> {
        let SpentTestOutput {
            signing_set,
            pubkey,
            outpoint,
            coin,
        } = self.spent_output(
            args.coinbase_txid_be,
            args.coinbase_vout,
            args.coinbase_value_zats,
        )?;
        let mut builder = Builder::new(
            self.params,
            BlockHeight::from_u32(args.target_height),
            standard_build_config(Some(args.pool)),
        );
        builder
            .add_transparent_p2pkh_input(pubkey, outpoint, coin)
            .map_err(|error| TransparentSignerError::AddInput(error.to_string()))?;
        let added = match args.pool {
            ShieldedOutputPool::Orchard => builder.add_orchard_output::<std::convert::Infallible>(
                None,
                *args.recipient,
                note_value,
                MemoBytes::empty(),
            ),
            ShieldedOutputPool::Ironwood => builder
                .add_ironwood_output::<std::convert::Infallible>(
                    None,
                    *args.recipient,
                    note_value,
                    MemoBytes::empty(),
                ),
        };
        added.map_err(|error| TransparentSignerError::AddShieldedOutput {
            pool: args.pool,
            reason: error.to_string(),
        })?;
        Ok((builder, signing_set))
    }

    /// Returns this key's P2PKH output at `txid_be:vout` with `value_zats`,
    /// and a signing set that can spend it.
    fn spent_output(
        &self,
        txid_be: [u8; 32],
        vout: u32,
        value_zats: u64,
    ) -> Result<SpentTestOutput, TransparentSignerError> {
        let mut signing_set = TransparentSigningSet::new();
        let secret_key = self
            .account_key
            .derive_external_secret_key(NonHardenedChildIndex::ZERO)
            .map_err(|error| TransparentSignerError::ExternalKey(error.to_string()))?;
        let pubkey = signing_set.add_key(secret_key);
        let coin = TxOut::new(
            zatoshis_from_u64(value_zats)?,
            TransparentAddress::from_pubkey(&pubkey).script().into(),
        );
        Ok(SpentTestOutput {
            signing_set,
            pubkey,
            outpoint: OutPoint::new(txid_be, vout),
            coin,
        })
    }
}

/// Returns a non-coinbase build configuration whose only shielded bundle,
/// when `pool` names one, is anchored at that pool's empty tree.
fn standard_build_config(pool: Option<ShieldedOutputPool>) -> BuildConfig {
    let empty_anchor_for = |candidate| (pool == Some(candidate)).then(orchard::Anchor::empty_tree);
    BuildConfig::Standard {
        sapling_anchor: None,
        orchard_anchor: empty_anchor_for(ShieldedOutputPool::Orchard),
        ironwood_anchor: empty_anchor_for(ShieldedOutputPool::Ironwood),
        orchard_padding: BundlePadding::DEFAULT,
        ironwood_padding: BundlePadding::DEFAULT,
    }
}

/// Builds `builder` with `mock_build` and serializes the transaction.
fn mock_build_bytes(
    builder: Builder<LocalNetwork, ()>,
    signing_set: &TransparentSigningSet,
) -> Result<Vec<u8>, TransparentSignerError> {
    let build_outcome = builder
        .mock_build(signing_set, &[], &[], rand::rng())
        .map_err(|error| TransparentSignerError::Build(error.to_string()))?;
    let mut bytes = Vec::new();
    build_outcome
        .transaction()
        .write(&mut bytes)
        .map_err(|source| TransparentSignerError::Serialize { source })?;
    Ok(bytes)
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

/// Activation heights of the captured `z3` regtest fixtures:
/// `overwinter..canopy = 1`, `nu5 = 2`, `nu6 = 2`, `nu6_1 = 3`, `nu6_2 = 3`,
/// `nu6_3 = 603`, and `nu7 = 703`.
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

    use super::{
        LocalNetwork, P2pkhShieldingSpendArgs, P2pkhSpendArgs, ShieldedOutputPool,
        TransparentSignerError, TransparentTestKey,
    };
    use crate::OrchardTestKey;
    use zcash_primitives::transaction::Transaction;
    use zcash_protocol::consensus::{BlockHeight, BranchId};
    use zcash_transparent::address::TransparentAddress;

    fn fixed_seed() -> [u8; 32] {
        [0x42_u8; 32]
    }

    /// Activation heights of `deploy/zebra/regtest.toml` and
    /// `deploy/zakura/regtest.toml`, which place NU6.3 at 6 and NU7 at 140.
    fn nu7_regtest_network() -> LocalNetwork {
        let at = |height| Some(BlockHeight::from_u32(height));
        LocalNetwork {
            overwinter: at(1),
            sapling: at(1),
            blossom: at(1),
            heartwood: at(1),
            canopy: at(1),
            nu5: at(2),
            nu6: at(3),
            nu6_1: at(4),
            nu6_2: at(5),
            nu6_3: at(6),
            nu7: at(140),
        }
    }

    /// Activation heights of `deploy/zakura/regtest-fixture-capture.toml`,
    /// which place NU6.3 at 130 and NU7 at 160 so a coinbase matures while
    /// Orchard still accepts new value.
    fn fixture_capture_network() -> LocalNetwork {
        LocalNetwork {
            nu6_3: Some(BlockHeight::from_u32(130)),
            nu7: Some(BlockHeight::from_u32(160)),
            ..nu7_regtest_network()
        }
    }

    const SHIELDED_SPEND_VALUE_ZATS: u64 = 10_000_000;
    /// The spent value less the ZIP-317 fee for one transparent input and two
    /// padded shielded actions, 15,000 zatoshis, leaving the shielded pool.
    const SHIELDED_VALUE_BALANCE_ZATS: i64 = -9_985_000;

    fn shielding_spend(
        params: LocalNetwork,
        pool: ShieldedOutputPool,
        target_height: u32,
    ) -> Result<Vec<u8>, TransparentSignerError> {
        let key = TransparentTestKey::from_seed_with_local_network(&fixed_seed(), params)?;
        let recipient = OrchardTestKey::from_seed(&fixed_seed())?.address();
        key.build_p2pkh_shielding_spend(&P2pkhShieldingSpendArgs {
            coinbase_txid_be: [0xdd_u8; 32],
            coinbase_vout: 0,
            coinbase_value_zats: SHIELDED_SPEND_VALUE_ZATS,
            pool,
            recipient: &recipient,
            target_height,
        })
    }

    fn read_transaction(
        bytes: &[u8],
        branch_id: BranchId,
    ) -> Result<Transaction, TransparentSignerError> {
        Transaction::read(bytes, branch_id)
            .map_err(|source| TransparentSignerError::Serialize { source })
    }

    #[test]
    fn shielding_spend_before_nu6_3_pays_one_orchard_note() -> Result<(), TransparentSignerError> {
        let bytes = shielding_spend(fixture_capture_network(), ShieldedOutputPool::Orchard, 111)?;
        // v5 header: version 5 with fOverwintered.
        assert_eq!(&bytes[0..4], &[0x05, 0x00, 0x00, 0x80]);
        let transaction = read_transaction(&bytes, BranchId::Nu6_2)?;
        let orchard = transaction
            .orchard_bundle()
            .ok_or_else(|| TransparentSignerError::Build("no Orchard bundle".to_owned()))?;
        assert_eq!(
            orchard.actions().len(),
            2,
            "one output padded to two actions"
        );
        assert_eq!(
            i64::from(*orchard.value_balance()),
            SHIELDED_VALUE_BALANCE_ZATS
        );
        assert!(transaction.ironwood_bundle().is_none());
        assert!(transaction.sapling_bundle().is_none());
        let transparent = transaction
            .transparent_bundle()
            .ok_or_else(|| TransparentSignerError::Build("no transparent bundle".to_owned()))?;
        assert_eq!((transparent.vin.len(), transparent.vout.len()), (1, 0));
        Ok(())
    }

    #[test]
    fn shielding_spend_from_nu6_3_pays_one_ironwood_note() -> Result<(), TransparentSignerError> {
        let bytes = shielding_spend(nu7_regtest_network(), ShieldedOutputPool::Ironwood, 100)?;
        assert_eq!(&bytes[0..4], &[0x06, 0x00, 0x00, 0x80]);
        let transaction = read_transaction(&bytes, BranchId::Nu6_3)?;
        let ironwood = transaction
            .ironwood_bundle()
            .ok_or_else(|| TransparentSignerError::Build("no Ironwood bundle".to_owned()))?;
        assert_eq!(
            ironwood.actions().len(),
            2,
            "one output padded to two actions"
        );
        assert_eq!(
            i64::from(*ironwood.value_balance()),
            SHIELDED_VALUE_BALANCE_ZATS
        );
        assert!(transaction.orchard_bundle().is_none());
        assert!(transaction.sapling_bundle().is_none());
        Ok(())
    }

    #[test]
    fn shielding_spend_refuses_orchard_from_nu6_3() {
        let outcome = shielding_spend(nu7_regtest_network(), ShieldedOutputPool::Orchard, 100);
        assert!(
            matches!(
                outcome,
                Err(TransparentSignerError::AddShieldedOutput {
                    pool: ShieldedOutputPool::Orchard,
                    ..
                })
            ),
            "{outcome:?}"
        );
    }

    #[test]
    fn shielding_spend_refuses_ironwood_before_nu6_3() {
        let outcome = shielding_spend(fixture_capture_network(), ShieldedOutputPool::Ironwood, 111);
        assert!(
            matches!(
                outcome,
                Err(TransparentSignerError::AddShieldedOutput {
                    pool: ShieldedOutputPool::Ironwood,
                    ..
                })
            ),
            "{outcome:?}"
        );
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
    fn build_p2pkh_spend_v4_produces_v4_bytes_with_the_requested_expiry()
    -> Result<(), TransparentSignerError> {
        let key =
            TransparentTestKey::from_seed_with_local_network(&fixed_seed(), nu7_regtest_network())?;
        let recipient = TransparentAddress::PublicKeyHash([0x33_u8; 20]);
        let expiry_height: u32 = 240;
        let bytes = key.build_p2pkh_spend_v4(
            &P2pkhSpendArgs {
                coinbase_txid_be: [0xbb_u8; 32],
                coinbase_vout: 0,
                coinbase_value_zats: 10_000_000,
                recipient: &recipient,
                target_height: 100,
            },
            expiry_height,
        )?;
        // v4 header: version 4 with fOverwintered, then the Sapling version group ID.
        assert_eq!(&bytes[0..4], &[0x04, 0x00, 0x00, 0x80]);
        assert_eq!(&bytes[4..8], &0x892f_2085_u32.to_le_bytes());
        let transaction = zcash_primitives::transaction::Transaction::read(
            bytes.as_slice(),
            zcash_protocol::consensus::BranchId::Nu6_3,
        )
        .map_err(|source| TransparentSignerError::Serialize { source })?;
        assert_eq!(u32::from(transaction.expiry_height()), expiry_height);
        Ok(())
    }

    #[test]
    fn build_p2pkh_spend_v4_rejects_a_target_height_after_nu7() -> Result<(), TransparentSignerError>
    {
        let key =
            TransparentTestKey::from_seed_with_local_network(&fixed_seed(), nu7_regtest_network())?;
        let recipient = TransparentAddress::PublicKeyHash([0x44_u8; 20]);
        let outcome = key.build_p2pkh_spend_v4(
            &P2pkhSpendArgs {
                coinbase_txid_be: [0xcc_u8; 32],
                coinbase_vout: 0,
                coinbase_value_zats: 10_000_000,
                recipient: &recipient,
                target_height: 141,
            },
            240,
        );
        assert!(matches!(outcome, Err(TransparentSignerError::Build(_))));
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
