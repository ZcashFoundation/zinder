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
    parameters::NetworkUpgrade as ZebraNetworkUpgrade, serialization::ZcashDeserialize,
    transaction::Transaction as ZebraTransaction, transparent::Input as ZebraTransparentInput,
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
    let transaction = parse_zebra_transaction(raw_transaction_bytes)?;
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
    let transaction = parse_zebra_transaction(raw_transaction_bytes)?;
    transaction_public_fact_set_from_parsed(
        &transaction,
        raw_transaction_bytes,
        mined_height,
        activations,
    )
}

fn parse_zebra_transaction(raw_transaction_bytes: &[u8]) -> Result<ZebraTransaction, SourceError> {
    let mut cursor = Cursor::new(raw_transaction_bytes);
    let transaction = ZebraTransaction::zcash_deserialize(&mut cursor).map_err(|source| {
        SourceError::RawTransactionParseFailed {
            reason: source.to_string(),
        }
    })?;
    let consumed_byte_count = usize::try_from(cursor.position()).unwrap_or(usize::MAX);
    if consumed_byte_count != raw_transaction_bytes.len() {
        return Err(SourceError::RawTransactionParseFailed {
            reason: format!(
                "transaction parser consumed {consumed_byte_count} of {} bytes",
                raw_transaction_bytes.len()
            ),
        });
    }
    Ok(transaction)
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
    let sprout_zat = transaction
        .sprout_value_balance()
        .map_err(|source| SourceError::RawTransactionParseFailed {
            reason: source.to_string(),
        })?
        .sprout_amount()
        .zatoshis();
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
        LibrustzcashTransactionVersion::Sprout(1) => TransactionVersion::V1,
        LibrustzcashTransactionVersion::Sprout(2) => TransactionVersion::V2,
        LibrustzcashTransactionVersion::Sprout(effective_version) => {
            TransactionVersion::Unsupported {
                effective_version,
                version_group_id: None,
            }
        }
        LibrustzcashTransactionVersion::V3 => TransactionVersion::V3,
        LibrustzcashTransactionVersion::V4 => TransactionVersion::V4,
        LibrustzcashTransactionVersion::V5 => TransactionVersion::V5,
        LibrustzcashTransactionVersion::V6 => TransactionVersion::V6,
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
    let sapling_spend_count = u32::try_from(transaction.sapling_spends_count()).unwrap_or(u32::MAX);
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
    match transaction {
        _ if transaction.auth_digest().is_none() => {
            Ok((TransactionId::from_bytes(transaction.hash().0), None, None))
        }
        _ => witnessed_transaction_identifiers(transaction, raw_transaction_bytes),
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
        LibrustzcashTransactionVersion::V5 => LibrustzcashTransactionVersion::V5,
        LibrustzcashTransactionVersion::V6 => LibrustzcashTransactionVersion::V6,
        LibrustzcashTransactionVersion::Sprout(_)
        | LibrustzcashTransactionVersion::V3
        | LibrustzcashTransactionVersion::V4 => {
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
        LibrustzcashTransactionVersion::Sprout(_) => None,
        LibrustzcashTransactionVersion::V3
        | LibrustzcashTransactionVersion::V4
        | LibrustzcashTransactionVersion::V5
        | LibrustzcashTransactionVersion::V6 => {
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
    use std::io::Cursor;
    use std::sync::Arc;

    use zcash_primitives::transaction::{
        Authorized, TransactionData as LibrustzcashTransactionData,
        TxVersion as LibrustzcashTransactionVersion,
    };
    use zcash_protocol::consensus::{BlockHeight as LibrustzcashBlockHeight, BranchId};
    use zcash_protocol::value::Zatoshis;
    use zcash_transparent::{
        address::Script,
        bundle::{
            Authorized as TransparentAuthorized, Bundle as TransparentBundle, OutPoint, TxIn, TxOut,
        },
    };
    use zebra_chain::{
        block::Block as ZebraBlock,
        serialization::{ZcashDeserialize, ZcashSerialize},
        transaction::{Transaction as ZebraTransaction, WtxId as ZebraWtxId},
    };
    use zinder_core::{
        AuthDigest, BlockHeight, Network, NetworkUpgradeActivations, TransactionId,
        TransactionIntrinsicValueBalances, TransactionVersion, Wtxid,
        wire::{encode_rpc_auth_digest_hex, encode_rpc_transaction_id_hex},
    };

    use crate::SourceBlock;

    use super::{
        SourceError, TransactionPublicFactSet, parse_transaction_public_fact_set,
        parse_transaction_public_facts, parse_zebra_transaction,
        transaction_public_fact_set_from_parsed,
    };

    #[test]
    fn parsed_v1_through_v6_transactions_preserve_balances_and_identifiers()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_transactions = minimal_transaction_encodings()?;
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

        for raw_transaction in raw_transactions {
            let transaction = parse_zebra_transaction(&raw_transaction)?;
            let fact_set = parse_transaction_public_fact_set(&raw_transaction, None, &activations)?;
            let parsed_fact_set = transaction_public_fact_set_from_parsed(
                &transaction,
                &raw_transaction,
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
    fn minimal_v1_through_v6_transactions_keep_known_identifiers()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let mut observed = Vec::new();
        for raw_transaction in minimal_transaction_encodings()? {
            let facts = parse_transaction_public_facts(&raw_transaction, None, &activations)?;
            observed.push((
                facts.version,
                hex::encode(&raw_transaction),
                encode_rpc_transaction_id_hex(facts.transaction_id),
                facts.auth_digest.map(encode_rpc_auth_digest_hex),
            ));
        }

        assert_eq!(
            observed,
            [
                (
                    TransactionVersion::V1,
                    "01000000000000000000".to_owned(),
                    "d21633ba23f70118185227be58a63527675641ad37967e2aa461559f577aec43".to_owned(),
                    None,
                ),
                (
                    TransactionVersion::V2,
                    "0200000000000000000000".to_owned(),
                    "c7e8a6e4ebd4981c43ff919703d54e91b4b3cb2325caf102dfc384bcad455c6f".to_owned(),
                    None,
                ),
                (
                    TransactionVersion::V3,
                    "030000807082c4030000000000000000000000".to_owned(),
                    "dfc33e3e2fafbf9fc65284ce19e4f930b21da6d59a014d62a706e5530597aca6".to_owned(),
                    None,
                ),
                (
                    TransactionVersion::V4,
                    "0400008085202f89000000000000000000000000000000000000000000".to_owned(),
                    "e22daacccd32e2335184e5604d86742609cf4ee106884a0603dbedf736ddd782".to_owned(),
                    None,
                ),
                (
                    TransactionVersion::V5,
                    "050000800a27a726b4d0d6c200000000000000000000000000".to_owned(),
                    "8e6b6d721fc653ef162daa85b32bff85144b9245add517cf710d5155cf5876df".to_owned(),
                    Some(
                        "0f33c0dfa1558e86304f29c0fbe0d982eeb0e5f2435a7cb1da6c2fc7395c5a0a"
                            .to_owned()
                    ),
                ),
                (
                    TransactionVersion::V6,
                    "0600008098b684d85b16a5370000000000000000000000000000".to_owned(),
                    "07920cf6e0b4e8fac9d15d33c2352a321172a9fa0293907d006b82d5d1d9c000".to_owned(),
                    Some(
                        "cbaf0aab342237179af081612fa1d4a079677000854a6ea8be6ac9481d383038"
                            .to_owned()
                    ),
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn witnessed_identifiers_reject_mismatched_serialized_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_transactions = minimal_transaction_encodings()?;
        let raw_v5_transaction = raw_transactions.get(4).ok_or("missing V5 fixture")?;
        let raw_v6_transaction = raw_transactions.get(5).ok_or("missing V6 fixture")?;
        let v5_transaction = parse_zebra_transaction(raw_v5_transaction)?;
        let v6_transaction = parse_zebra_transaction(raw_v6_transaction)?;
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

        let mut trailing_bytes = (*raw_v5_transaction).clone();
        trailing_bytes.push(0);
        let mut truncated_bytes = (*raw_v5_transaction).clone();
        truncated_bytes.truncate(truncated_bytes.len().saturating_sub(1));
        let mut trailing_v6_bytes = (*raw_v6_transaction).clone();
        trailing_v6_bytes.push(0);
        let mut truncated_v6_bytes = (*raw_v6_transaction).clone();
        truncated_v6_bytes.truncate(truncated_v6_bytes.len().saturating_sub(1));

        for malformed_bytes in [
            &trailing_bytes,
            &truncated_bytes,
            &trailing_v6_bytes,
            &truncated_v6_bytes,
        ] {
            assert!(matches!(
                parse_transaction_public_fact_set(malformed_bytes, None, &activations),
                Err(SourceError::RawTransactionParseFailed { .. })
            ));
        }
        assert!(matches!(
            transaction_public_fact_set_from_parsed(
                &v6_transaction,
                raw_v5_transaction,
                None,
                &activations,
            ),
            Err(SourceError::RawTransactionParseFailed { .. })
        ));
        assert_eq!(
            v5_transaction.tx_version(),
            LibrustzcashTransactionVersion::V5
        );

        Ok(())
    }

    #[test]
    fn near_max_serialized_block_respects_zebra_byte_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let max_block_bytes = usize::try_from(zebra_chain::block::MAX_BLOCK_BYTES)?;
        assert_eq!(
            max_block_bytes,
            zcash_protocol::constants::MAX_BLOCK_BYTES,
            "transaction and block parsing must share the protocol byte bound",
        );

        let base_transaction_bytes = large_transparent_v4_transaction(0)?;
        let base_block_bytes = large_transparent_v4_block(0)?;
        let one_output_transaction_bytes = large_transparent_v4_transaction(1)?;
        let block_overhead = base_block_bytes
            .len()
            .checked_sub(base_transaction_bytes.len())
            .ok_or("serialized block was smaller than its transaction")?;
        let bytes_per_output = one_output_transaction_bytes
            .len()
            .checked_sub(base_transaction_bytes.len())
            .ok_or("one output reduced serialized transaction size")?;
        let mut output_count = max_block_bytes
            .checked_sub(block_overhead)
            .and_then(|available| available.checked_sub(base_transaction_bytes.len()))
            .ok_or("block framing exceeded the block byte limit")?
            / bytes_per_output;
        let (raw_block_bytes, output_count) = loop {
            let bytes = large_transparent_v4_block(output_count)?;
            if bytes.len() <= max_block_bytes {
                break (bytes, output_count);
            }
            output_count = output_count
                .checked_sub(1)
                .ok_or("block overhead exceeded the size limit")?;
        };
        let next_output_count = output_count
            .checked_add(1)
            .ok_or("transparent output count overflowed")?;
        let oversized_block_bytes = large_transparent_v4_block(next_output_count)?;
        assert!(oversized_block_bytes.len() > max_block_bytes);
        assert!(max_block_bytes - raw_block_bytes.len() < bytes_per_output);

        // The fixture header and transaction are valid serialized structures.
        // This checks byte-bound enforcement, not block consensus validity:
        // the synthetic transaction is not claimed to match the header's
        // Merkle root or to satisfy contextual transaction rules.
        let mut bounded_cursor = Cursor::new(raw_block_bytes.clone());
        let parsed_block = ZebraBlock::zcash_deserialize(&mut bounded_cursor)?;
        assert_eq!(
            bounded_cursor.position(),
            u64::try_from(raw_block_bytes.len())?
        );
        assert_eq!(parsed_block.transactions.len(), 1);

        let mut oversized_cursor = Cursor::new(oversized_block_bytes.clone());
        assert!(ZebraBlock::zcash_deserialize(&mut oversized_cursor).is_err());

        // SourceBlock intentionally validates only the header prefix. Full
        // block bounds are enforced by the Zebra block decoder in preparation.
        let source_block = SourceBlock::from_raw_block_bytes(
            Network::ZcashRegtest,
            BlockHeight::new(1),
            raw_block_bytes,
        )?;
        assert_eq!(source_block.height, BlockHeight::new(1));
        assert!(
            SourceBlock::from_raw_block_bytes(
                Network::ZcashRegtest,
                BlockHeight::new(1),
                oversized_block_bytes,
            )
            .is_ok()
        );

        // Keep transaction facts covered at the same near-limit boundary.
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let large_transaction = large_transparent_v4_transaction(output_count)?;
        let facts = parse_transaction_public_facts(&large_transaction, None, &activations)?;

        assert_eq!(facts.version, TransactionVersion::V4);
        assert_eq!(usize::try_from(facts.size_bytes)?, large_transaction.len(),);
        assert_eq!(
            facts.counts.transparent_input_count, 1,
            "the synthetic V4 transaction retains its structurally decoded input",
        );
        assert_eq!(
            facts.counts.transparent_output_count,
            u32::try_from(output_count)?,
        );

        Ok(())
    }

    fn minimal_transaction_encodings() -> Result<[Vec<u8>; 6], Box<dyn std::error::Error>> {
        let versions = [
            (LibrustzcashTransactionVersion::Sprout(1), BranchId::Sprout),
            (LibrustzcashTransactionVersion::Sprout(2), BranchId::Sprout),
            (LibrustzcashTransactionVersion::V3, BranchId::Overwinter),
            (LibrustzcashTransactionVersion::V4, BranchId::Canopy),
            (LibrustzcashTransactionVersion::V5, BranchId::Nu5),
        ];
        let mut encodings = versions
            .into_iter()
            .map(|(version, branch_id)| {
                let transaction = LibrustzcashTransactionData::<Authorized>::from_parts(
                    version,
                    branch_id,
                    0,
                    LibrustzcashBlockHeight::from_u32(0),
                    None,
                    None,
                    None,
                    None,
                )
                .freeze()?;
                let mut bytes = Vec::new();
                transaction.write(&mut bytes)?;
                Ok::<_, Box<dyn std::error::Error>>(bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let transaction = LibrustzcashTransactionData::<Authorized>::from_parts_v6(
            BranchId::Nu6_3,
            0,
            LibrustzcashBlockHeight::from_u32(0),
            None,
            None,
            None,
            None,
        )
        .freeze()?;
        let mut bytes = Vec::new();
        transaction.write(&mut bytes)?;
        encodings.push(bytes);
        encodings
            .try_into()
            .map_err(|_| std::io::Error::other("expected six transaction encodings").into())
    }

    fn large_transparent_v4_transaction(
        output_count: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let transparent_bundle = TransparentBundle {
            vin: vec![TxIn::from_parts(
                OutPoint::new([0x42; 32], 0),
                Script::default(),
                u32::MAX,
            )],
            vout: std::iter::repeat_with(|| TxOut::new(Zatoshis::ZERO, Script::default()))
                .take(output_count)
                .collect(),
            authorization: TransparentAuthorized,
        };
        let transaction = LibrustzcashTransactionData::<Authorized>::from_parts(
            LibrustzcashTransactionVersion::V4,
            BranchId::Canopy,
            0,
            LibrustzcashBlockHeight::from_u32(0),
            Some(transparent_bundle),
            None,
            None,
            None,
        )
        .freeze()?;
        let mut bytes = Vec::new();
        transaction.write(&mut bytes)?;
        Ok(bytes)
    }

    fn large_transparent_v4_block(
        output_count: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../services/zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
        ))?;
        let raw_fixture = hex::decode(
            fixture
                .get("raw_block_hex")
                .and_then(serde_json::Value::as_str)
                .ok_or("regtest block fixture is missing raw_block_hex")?,
        )?;
        let mut fixture_cursor = Cursor::new(raw_fixture);
        let fixture_block = ZebraBlock::zcash_deserialize(&mut fixture_cursor)?;
        let transaction =
            parse_zebra_transaction(&large_transparent_v4_transaction(output_count)?)?;
        let block = ZebraBlock {
            header: Arc::clone(&fixture_block.header),
            transactions: vec![Arc::new(transaction)],
        };
        let mut bytes = Vec::new();
        block.zcash_serialize(&mut bytes)?;
        Ok(bytes)
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
}
