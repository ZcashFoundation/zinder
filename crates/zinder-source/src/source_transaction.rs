//! Node-sourced transaction values.
//!
//! Helpers that translate raw serialized Zcash transaction bytes into
//! Zinder vocabulary. Mirrors the [`crate::source_block`] pattern: the
//! upstream Zebra type is parsed once at the boundary and the typed
//! Zinder shape is the public return.

use std::io::Cursor;

use zcash_primitives::transaction::{
    Transaction as LibrustzcashTransaction, TxVersion as LibrustzcashTransactionVersion,
};
use zcash_protocol::consensus::BranchId as LibrustzcashBranchId;
use zebra_chain::{
    parameters::NetworkUpgrade as ZebraNetworkUpgrade,
    serialization::ZcashDeserializeInto,
    transaction::{Transaction as ZebraTransaction, TxVersion as ZebraTxVersion},
    transparent::Input as ZebraTransparentInput,
};
use zinder_core::{
    AuthDigest, BlockHeight, ConsensusBranchId, LockTime, NetworkUpgradeActivations,
    TransactionComponentCounts, TransactionId, TransactionIntrinsicValueBalances,
    TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
    TransparentOutPoint, TransparentOutputFact, UnsupportedSection, Wtxid, classify_privacy_shape,
};

use crate::SourceError;

/// Complete public transaction facts parsed from one serialized transaction.
///
/// The scalar facts and ordered transparent rows share one consensus parse so
/// ingest, mined explorer reads, and transient mempool reads cannot drift.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionPublicFactSet {
    /// Scalar transaction metadata and component counts.
    pub public_facts: TransactionPublicFacts,
    /// Signed shielded-pool balances intrinsic to the transaction bytes.
    pub intrinsic_value_balances: TransactionIntrinsicValueBalances,
    /// Ordered transparent inputs, excluding the coinbase sentinel.
    pub transparent_inputs: Vec<TransparentInputFact>,
    /// Ordered transparent outputs with intrinsic values and scripts.
    pub transparent_outputs: Vec<TransparentOutputFact>,
}

/// Parses every public transaction fact the explorer plane renders.
///
/// Single source of truth for transaction parsing across ingest, mempool
/// hydration, and the explorer read path per
/// [ADR-0010](../../../docs/adrs/0010-transaction-public-facts.md). The
/// `activations` table resolves the consensus branch ID for v3/v4
/// transactions whose header omits it; v5+ transactions carry the branch ID
/// directly through `Transaction::network_upgrade()`.
///
/// The parser delegates byte-level decoding to `zebra-chain` and never
/// re-derives the txid: it trusts `Transaction::hash()` which routes
/// `SHA256d` for pre-v5 and `BLAKE2b` ZIP-244 `txid_digest` for v5+.
pub fn parse_transaction_public_facts(
    raw_transaction_bytes: &[u8],
    mined_height: Option<BlockHeight>,
    activations: &NetworkUpgradeActivations,
) -> Result<TransactionPublicFacts, SourceError> {
    let transaction: ZebraTransaction =
        raw_transaction_bytes
            .zcash_deserialize_into()
            .map_err(|source| SourceError::RawTransactionParseFailed {
                reason: source.to_string(),
            })?;
    transaction_public_facts(
        &transaction,
        raw_transaction_bytes,
        mined_height,
        activations,
    )
}

/// Parses scalar and ordered transparent facts from one serialized transaction.
pub fn parse_transaction_public_fact_set(
    raw_transaction_bytes: &[u8],
    mined_height: Option<BlockHeight>,
    activations: &NetworkUpgradeActivations,
) -> Result<TransactionPublicFactSet, SourceError> {
    let transaction: ZebraTransaction =
        raw_transaction_bytes
            .zcash_deserialize_into()
            .map_err(|source| SourceError::RawTransactionParseFailed {
                reason: source.to_string(),
            })?;
    transaction_public_fact_set_from_parsed(
        &transaction,
        raw_transaction_bytes,
        mined_height,
        activations,
    )
}

/// Builds scalar and ordered transparent facts from an already parsed
/// transaction.
///
/// Canonical block preparation uses this entry point so it can share the
/// block parser's transaction values instead of serializing and deserializing
/// every transaction again. `raw_transaction_bytes` must be the exact
/// serialization of `transaction`; witnessed identifiers fail closed if the
/// byte stream cannot be parsed or is not fully consumed. Byte-oriented callers should continue to use
/// [`parse_transaction_public_fact_set`].
pub fn transaction_public_fact_set_from_parsed(
    transaction: &ZebraTransaction,
    raw_transaction_bytes: &[u8],
    mined_height: Option<BlockHeight>,
    activations: &NetworkUpgradeActivations,
) -> Result<TransactionPublicFactSet, SourceError> {
    let public_facts = transaction_public_facts(
        transaction,
        raw_transaction_bytes,
        mined_height,
        activations,
    )?;
    let intrinsic_value_balances = transaction_intrinsic_value_balances(transaction)?;
    let (transparent_inputs, transparent_outputs) = transaction_transparent_facts(transaction)?;
    Ok(TransactionPublicFactSet {
        public_facts,
        intrinsic_value_balances,
        transparent_inputs,
        transparent_outputs,
    })
}

fn transaction_intrinsic_value_balances(
    transaction: &ZebraTransaction,
) -> Result<TransactionIntrinsicValueBalances, SourceError> {
    let sprout_zat = sum_sprout_joinsplit_balances_zat(
        transaction.output_values_to_sprout().into_iter(),
        transaction.input_values_from_sprout().into_iter(),
    )?;
    let sapling_zat = transaction
        .sapling_value_balance()
        .sapling_amount()
        .zatoshis();
    let orchard_zat = transaction
        .orchard_value_balance()
        .orchard_amount()
        .zatoshis();
    let ironwood_zat = transaction
        .ironwood_value_balance()
        .ironwood_amount()
        .zatoshis();

    Ok(TransactionIntrinsicValueBalances::new(
        sprout_zat,
        sapling_zat,
        orchard_zat,
        ironwood_zat,
    ))
}

fn sum_sprout_joinsplit_balances_zat(
    values_to_sprout_zat: impl Iterator<Item = i64>,
    values_from_sprout_zat: impl Iterator<Item = i64>,
) -> Result<i64, SourceError> {
    let aggregate_zat = values_to_sprout_zat.map(i128::from).sum::<i128>()
        - values_from_sprout_zat.map(i128::from).sum::<i128>();
    i64::try_from(aggregate_zat).map_err(|_| SourceError::RawTransactionParseFailed {
        reason: "aggregate Sprout value balance exceeds i64".to_owned(),
    })
}

fn transaction_public_facts(
    transaction: &ZebraTransaction,
    raw_transaction_bytes: &[u8],
    mined_height: Option<BlockHeight>,
    activations: &NetworkUpgradeActivations,
) -> Result<TransactionPublicFacts, SourceError> {
    let version = classify_transaction_version(transaction);
    let counts = transaction_component_counts(transaction);
    let orchard_value_balance_zat = transaction.has_orchard_shielded_data().then(|| {
        transaction
            .orchard_value_balance()
            .orchard_amount()
            .zatoshis()
    });
    let orchard_anchor = transaction.orchard_anchor().map(<[u8; 32]>::from);
    let ironwood_value_balance_zat = transaction.has_ironwood_shielded_data().then(|| {
        transaction
            .ironwood_value_balance()
            .ironwood_amount()
            .zatoshis()
    });
    let is_coinbase = transaction.is_coinbase();
    let unsupported_sections = if version.is_supported() {
        Vec::new()
    } else {
        vec![UnsupportedSection::FutureVersionHeader]
    };
    let consensus_branch_id = resolve_consensus_branch_id(transaction, mined_height, activations);
    let (transaction_id, auth_digest, wtxid) =
        transaction_identifiers(transaction, raw_transaction_bytes)?;
    let lock_time = extract_lock_time(transaction);
    let expiry_height = extract_expiry_height(transaction);
    let size_bytes = u32::try_from(raw_transaction_bytes.len()).unwrap_or(u32::MAX);
    let privacy_shape = classify_privacy_shape(counts, is_coinbase, version);

    Ok(TransactionPublicFacts {
        transaction_id,
        auth_digest,
        wtxid,
        version,
        consensus_branch_id,
        lock_time,
        expiry_height,
        size_bytes,
        counts,
        orchard_value_balance_zat,
        orchard_anchor,
        ironwood_value_balance_zat,
        privacy_shape,
        is_coinbase,
        unsupported_sections,
    })
}

fn transaction_transparent_facts(
    transaction: &ZebraTransaction,
) -> Result<(Vec<TransparentInputFact>, Vec<TransparentOutputFact>), SourceError> {
    let mut inputs = Vec::new();
    for (input_index, input) in transaction.inputs().iter().enumerate() {
        let input_index = u32::try_from(input_index).map_err(|_| {
            SourceError::TransactionComponentIndexOverflow {
                component: "transparent input",
            }
        })?;
        let ZebraTransparentInput::PrevOut { outpoint, .. } = input else {
            continue;
        };
        inputs.push(TransparentInputFact::new(
            input_index,
            TransparentOutPoint::new(TransactionId::from_bytes(outpoint.hash.0), outpoint.index),
        ));
    }

    let mut outputs = Vec::new();
    for (output_index, output) in transaction.outputs().iter().enumerate() {
        let output_index = u32::try_from(output_index).map_err(|_| {
            SourceError::TransactionComponentIndexOverflow {
                component: "transparent output",
            }
        })?;
        let script_pub_key = output.lock_script.as_raw_bytes().to_vec();
        outputs.push(TransparentOutputFact::new(
            output_index,
            u64::from(output.value()),
            script_pub_key.clone(),
            TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
        ));
    }
    Ok((inputs, outputs))
}

fn classify_transaction_version(transaction: &ZebraTransaction) -> TransactionVersion {
    match transaction.tx_version() {
        ZebraTxVersion::Sprout(1) => TransactionVersion::V1,
        ZebraTxVersion::Sprout(2) => TransactionVersion::V2,
        ZebraTxVersion::V3 => TransactionVersion::V3,
        ZebraTxVersion::V4 => TransactionVersion::V4,
        ZebraTxVersion::V5 => TransactionVersion::V5,
        ZebraTxVersion::V6 => TransactionVersion::V6,
        ZebraTxVersion::Sprout(effective_version) => TransactionVersion::Unsupported {
            effective_version,
            version_group_id: None,
        },
    }
}

/// Counts the transparent / shielded components of a parsed Zcash transaction.
///
/// This is the canonical way to populate [`TransactionComponentCounts`] from
/// a `zebra-chain` transaction; every explorer-plane and source-plane caller
/// goes through this function so the counting rules stay in one place.
///
/// Each count is saturated at `u32::MAX` rather than overflowing; a
/// transaction with more than four billion of any component would not
/// validate, so the cap is defensive rather than load-bearing.
#[must_use]
pub fn transaction_component_counts(transaction: &ZebraTransaction) -> TransactionComponentCounts {
    let transparent_input_count = u32::try_from(transaction.inputs().len()).unwrap_or(u32::MAX);
    let transparent_output_count = u32::try_from(transaction.outputs().len()).unwrap_or(u32::MAX);
    let sapling_spend_count =
        u32::try_from(transaction.sapling_spends().count()).unwrap_or(u32::MAX);
    let sapling_output_count =
        u32::try_from(transaction.sapling_outputs().count()).unwrap_or(u32::MAX);
    let orchard_action_count =
        u32::try_from(transaction.orchard_actions().count()).unwrap_or(u32::MAX);
    let ironwood_action_count =
        u32::try_from(transaction.ironwood_actions().count()).unwrap_or(u32::MAX);
    let sprout_joinsplit_count = u32::try_from(transaction.joinsplit_count()).unwrap_or(u32::MAX);
    TransactionComponentCounts {
        transparent_input_count,
        transparent_output_count,
        sapling_spend_count,
        sapling_output_count,
        orchard_action_count,
        ironwood_action_count,
        sprout_joinsplit_count,
    }
}

fn transaction_identifiers(
    transaction: &ZebraTransaction,
    raw_transaction_bytes: &[u8],
) -> Result<(TransactionId, Option<AuthDigest>, Option<Wtxid>), SourceError> {
    match transaction.tx_version() {
        ZebraTxVersion::Sprout(_) | ZebraTxVersion::V3 | ZebraTxVersion::V4 => {
            Ok((TransactionId::from_bytes(transaction.hash().0), None, None))
        }
        ZebraTxVersion::V5 | ZebraTxVersion::V6 => {
            witnessed_transaction_identifiers(transaction, raw_transaction_bytes)
        }
    }
}

/// Derives both witnessed digests from one librustzcash transaction parse.
///
/// Zebra's current `WtxId::from(&Transaction)` path converts the transaction
/// separately for its transaction ID and authorizing digest. This source hot
/// path keeps the same librustzcash release line as Zebra but performs that
/// conversion once. Prefer a one-pass Zebra API when upstream exposes one.
fn witnessed_transaction_identifiers(
    transaction: &ZebraTransaction,
    raw_transaction_bytes: &[u8],
) -> Result<(TransactionId, Option<AuthDigest>, Option<Wtxid>), SourceError> {
    let branch_id = transaction
        .network_upgrade()
        .and_then(|network_upgrade| network_upgrade.branch_id())
        .and_then(|branch_id| LibrustzcashBranchId::try_from(u32::from(branch_id)).ok())
        .ok_or_else(|| SourceError::RawTransactionParseFailed {
            reason: "witnessed transaction has no supported consensus branch ID".to_owned(),
        })?;
    let mut raw_transaction_cursor = Cursor::new(raw_transaction_bytes);
    let librustzcash_transaction =
        LibrustzcashTransaction::read(&mut raw_transaction_cursor, branch_id).map_err(
            |source| SourceError::RawTransactionParseFailed {
                reason: source.to_string(),
            },
        )?;
    let expected_version = match transaction.tx_version() {
        ZebraTxVersion::V5 => LibrustzcashTransactionVersion::V5,
        ZebraTxVersion::V6 => LibrustzcashTransactionVersion::V6,
        ZebraTxVersion::Sprout(_) | ZebraTxVersion::V3 | ZebraTxVersion::V4 => {
            return Err(SourceError::RawTransactionParseFailed {
                reason: "witnessed identifier extraction requires a V5 or V6 transaction"
                    .to_owned(),
            });
        }
    };
    if librustzcash_transaction.version() != expected_version {
        return Err(SourceError::RawTransactionParseFailed {
            reason: "parsed transaction version does not match the Zebra transaction".to_owned(),
        });
    }
    if librustzcash_transaction.consensus_branch_id() != branch_id {
        return Err(SourceError::RawTransactionParseFailed {
            reason: "parsed transaction branch ID does not match the Zebra transaction".to_owned(),
        });
    }
    let consumed_byte_count =
        usize::try_from(raw_transaction_cursor.position()).unwrap_or(usize::MAX);
    if consumed_byte_count != raw_transaction_bytes.len() {
        return Err(SourceError::RawTransactionParseFailed {
            reason: format!(
                "witnessed transaction parser consumed {consumed_byte_count} of {} bytes",
                raw_transaction_bytes.len()
            ),
        });
    }

    let transaction_id_bytes = *librustzcash_transaction.txid().as_ref();
    let auth_digest = librustzcash_transaction.auth_commitment();
    let auth_digest_bytes: [u8; 32] =
        auth_digest
            .as_bytes()
            .try_into()
            .map_err(|_| SourceError::RawTransactionParseFailed {
                reason: "witnessed transaction auth digest is not 32 bytes".to_owned(),
            })?;
    let mut wtxid_bytes = [0u8; 64];
    wtxid_bytes[..32].copy_from_slice(&transaction_id_bytes);
    wtxid_bytes[32..].copy_from_slice(&auth_digest_bytes);

    Ok((
        TransactionId::from_bytes(transaction_id_bytes),
        Some(AuthDigest::from_bytes(auth_digest_bytes)),
        Some(Wtxid::from_bytes(wtxid_bytes)),
    ))
}

fn extract_lock_time(transaction: &ZebraTransaction) -> LockTime {
    use zebra_chain::transaction::LockTime as ZebraLockTime;
    match transaction.lock_time() {
        Some(ZebraLockTime::Height(height)) => LockTime::Height(BlockHeight::new(height.0)),
        Some(ZebraLockTime::Time(timestamp)) => {
            LockTime::UnixSeconds(u64::try_from(timestamp.timestamp()).unwrap_or(0))
        }
        None => LockTime::Unlocked,
    }
}

fn extract_expiry_height(transaction: &ZebraTransaction) -> Option<BlockHeight> {
    transaction
        .expiry_height()
        .map(|height| BlockHeight::new(height.0))
}

fn resolve_consensus_branch_id(
    transaction: &ZebraTransaction,
    mined_height: Option<BlockHeight>,
    activations: &NetworkUpgradeActivations,
) -> Option<ConsensusBranchId> {
    if let Some(network_upgrade) = transaction.network_upgrade() {
        return zebra_network_upgrade_branch_id(network_upgrade);
    }
    match transaction.tx_version() {
        ZebraTxVersion::Sprout(1 | 2) => None,
        ZebraTxVersion::Sprout(_)
        | ZebraTxVersion::V3
        | ZebraTxVersion::V4
        | ZebraTxVersion::V5
        | ZebraTxVersion::V6 => {
            mined_height.map(|height| activations.consensus_branch_id_at(height))
        }
    }
}

fn zebra_network_upgrade_branch_id(
    network_upgrade: ZebraNetworkUpgrade,
) -> Option<ConsensusBranchId> {
    network_upgrade
        .branch_id()
        .map(|branch_id| ConsensusBranchId::new(branch_id.into()))
}

#[cfg(test)]
mod tests {
    use zebra_chain::{
        serialization::ZcashDeserializeInto,
        transaction::{Transaction as ZebraTransaction, WtxId as ZebraWtxId},
    };
    use zinder_core::{
        AuthDigest, Network, NetworkUpgradeActivations, TransactionId,
        TransactionIntrinsicValueBalances, Wtxid,
    };

    use super::{
        SourceError, TransactionPublicFactSet, parse_transaction_public_fact_set,
        sum_sprout_joinsplit_balances_zat, transaction_public_fact_set_from_parsed,
    };

    #[test]
    fn parsed_v1_through_v6_transactions_preserve_balances_and_identifiers()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_transactions: [&[u8]; 6] = [
            &[0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[
                0x03, 0, 0, 0x80, 0x70, 0x82, 0xc4, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            &[
                0x04, 0, 0, 0x80, 0x85, 0x20, 0x2f, 0x89, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
            ],
            &[
                0x05, 0, 0, 0x80, 0x0a, 0x27, 0xa7, 0x26, 0xd9, 0x0a, 0x19, 0x77, 0, 0, 0, 0, 1, 0,
                0, 0, 0, 0, 0, 0, 0,
            ],
            &[
                0x06, 0, 0, 0x80, 0x98, 0xb6, 0x84, 0xd8, 0xd9, 0x0a, 0x19, 0x77, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
            ],
        ];
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

        for (transaction_index, raw_transaction) in raw_transactions.into_iter().enumerate() {
            let transaction: ZebraTransaction = raw_transaction
                .zcash_deserialize_into()
                .map_err(|source| format!("fixture {transaction_index}: {source}"))?;
            let fact_set = parse_transaction_public_fact_set(raw_transaction, None, &activations)?;
            let parsed_fact_set = transaction_public_fact_set_from_parsed(
                &transaction,
                raw_transaction,
                None,
                &activations,
            )?;
            assert_eq!(
                fact_set.intrinsic_value_balances,
                TransactionIntrinsicValueBalances::default()
            );
            assert_eq!(parsed_fact_set, fact_set);
            assert_transaction_identifiers_match_zebra(&transaction, &fact_set);
        }

        Ok(())
    }

    #[test]
    fn nu7_branch_id_is_preserved_for_version_five_transactions()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_transaction = [
            0x05, 0, 0, 0x80, 0x0a, 0x27, 0xa7, 0x26, 0xd9, 0x0a, 0x19, 0x77, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];
        let transaction: ZebraTransaction = raw_transaction.zcash_deserialize_into()?;
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

        assert_eq!(
            transaction.network_upgrade(),
            Some(zebra_chain::parameters::NetworkUpgrade::Nu7)
        );
        let facts = parse_transaction_public_fact_set(&raw_transaction, None, &activations)?;
        assert_eq!(
            facts.public_facts.consensus_branch_id,
            Some(zinder_core::ConsensusBranchId::new(0x7719_0ad9))
        );

        Ok(())
    }

    #[test]
    fn witnessed_identifiers_reject_mismatched_serialized_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_v5_transaction = [
            0x05, 0, 0, 0x80, 0x0a, 0x27, 0xa7, 0x26, 0xd9, 0x0a, 0x19, 0x77, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];
        let v5_transaction: ZebraTransaction = raw_v5_transaction.zcash_deserialize_into()?;
        let raw_v6_transaction = [
            0x06, 0, 0, 0x80, 0x98, 0xb6, 0x84, 0xd8, 0xd9, 0x0a, 0x19, 0x77, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let v6_transaction: ZebraTransaction = raw_v6_transaction.zcash_deserialize_into()?;
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

        let mut trailing_bytes = raw_v5_transaction.to_vec();
        trailing_bytes.push(0);
        let mut truncated_bytes = raw_v5_transaction.to_vec();
        truncated_bytes.truncate(truncated_bytes.len().saturating_sub(1));

        for mismatched_bytes in [&trailing_bytes, &truncated_bytes] {
            assert!(
                transaction_public_fact_set_from_parsed(
                    &v5_transaction,
                    mismatched_bytes,
                    None,
                    &activations,
                )
                .is_err()
            );
        }
        assert!(
            transaction_public_fact_set_from_parsed(
                &v6_transaction,
                &raw_v5_transaction,
                None,
                &activations,
            )
            .is_err()
        );

        Ok(())
    }

    fn assert_transaction_identifiers_match_zebra(
        transaction: &ZebraTransaction,
        fact_set: &TransactionPublicFactSet,
    ) {
        if transaction.network_upgrade().is_some() {
            let zebra_wtxid = ZebraWtxId::from(transaction);
            assert_eq!(
                fact_set.public_facts.transaction_id,
                TransactionId::from_bytes(zebra_wtxid.id.0)
            );
            assert_eq!(
                fact_set.public_facts.auth_digest,
                Some(AuthDigest::from_bytes(zebra_wtxid.auth_digest.0))
            );
            assert_eq!(
                fact_set.public_facts.wtxid,
                Some(Wtxid::from_bytes(zebra_wtxid.as_bytes()))
            );
        } else {
            assert_eq!(
                fact_set.public_facts.transaction_id,
                TransactionId::from_bytes(transaction.hash().0)
            );
            assert_eq!(fact_set.public_facts.auth_digest, None);
            assert_eq!(fact_set.public_facts.wtxid, None);
        }
    }

    #[test]
    fn sprout_joinsplit_balances_preserve_zebra_sign() -> Result<(), SourceError> {
        let balance = sum_sprout_joinsplit_balances_zat([5_i64].into_iter(), [12_i64].into_iter())?;

        assert_eq!(balance, -7);
        Ok(())
    }
}
