//! Mempool entry hydration from source observations.
//!
//! Translates a source-observed [`MempoolSourceEntry`] into the canonical
//! public [`MempoolEntry`] consumed by the live `MempoolIndex` and by the
//! mempool surface RPCs. The translation:
//!
//! 1. Parses the raw transaction bytes via `zebra-chain` so the same
//!    consensus parser used by chain ingestion drives mempool hydration.
//! 2. Builds native structured wallet scan data on the ingest write side.
//! 3. Extracts the transparent outputs and outpoint spends carried by every
//!    snapshot entry and checked by the index's live-outpoint invariants.
//! 4. Stamps the visible [`ChainEpoch`] supplied by ingest at observation
//!    time. The chain epoch is the one stored alongside the source entry's
//!    wall-clock observation time, not the chain epoch at the moment a
//!    consumer reads the entry.

use thiserror::Error;
use zebra_chain::serialization::ZcashDeserializeInto;
use zebra_chain::transaction::Transaction as ZebraTransaction;
use zinder_core::{AuthDigest, ChainEpoch, MempoolEntry, MempoolObservation, TransactionId};
use zinder_source::MempoolSourceEntry;

use crate::artifact_builder::compact_transaction_data;

/// Error returned while finalizing a [`MempoolEntry`] from a source entry.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MempoolEntryBuildError {
    /// Raw transaction bytes did not parse as a Zcash transaction.
    #[error("mempool transaction parse failed: {source}")]
    TransactionParseFailed {
        /// Underlying parse error.
        #[source]
        source: zebra_chain::serialization::SerializationError,
    },

    /// The source-reported identifier disagrees with the parsed transaction.
    #[error("mempool transaction id mismatch: source {source_id:?}, parsed {parsed_id:?}")]
    TransactionIdMismatch {
        /// Identifier reported by the source observation.
        source_id: TransactionId,
        /// Identifier derived from the raw consensus transaction bytes.
        parsed_id: TransactionId,
    },

    /// The source-reported authorization digest disagrees with the parsed transaction.
    #[error(
        "mempool authorization digest mismatch: source {source_digest:?}, parsed {parsed_digest:?}"
    )]
    AuthDigestMismatch {
        /// Authorization digest reported by the source observation.
        source_digest: Option<AuthDigest>,
        /// Authorization digest derived from the raw consensus transaction bytes.
        parsed_digest: Option<AuthDigest>,
    },

    /// Building native compact transaction data failed.
    #[error("mempool compact-transaction build failed: {source}")]
    CompactTransactionBuildFailed {
        /// Underlying compact-transaction construction error.
        #[source]
        source: crate::CanonicalBlockConstructionError,
    },

    /// The transaction reports more transparent outputs than `u32::MAX`.
    #[error("mempool transparent output index overflowed u32")]
    TransparentOutputIndexOverflow,
}

/// Finalizes a source-observed mempool entry into the canonical public
/// [`MempoolEntry`].
pub fn build_mempool_entry(
    source_entry: MempoolSourceEntry,
    visible_chain_epoch: ChainEpoch,
) -> Result<MempoolEntry, MempoolEntryBuildError> {
    let parsed_transaction: ZebraTransaction = source_entry
        .raw_transaction_bytes
        .as_slice()
        .zcash_deserialize_into()
        .map_err(|source| MempoolEntryBuildError::TransactionParseFailed { source })?;
    let parsed_transaction_id = TransactionId::from_bytes(parsed_transaction.hash().0);
    if parsed_transaction_id != source_entry.transaction_id {
        return Err(MempoolEntryBuildError::TransactionIdMismatch {
            source_id: source_entry.transaction_id,
            parsed_id: parsed_transaction_id,
        });
    }
    let parsed_auth_digest = parsed_transaction
        .auth_digest()
        .map(|digest| AuthDigest::from_bytes(digest.0));
    if let Some(source_digest) = source_entry.auth_digest
        && parsed_auth_digest != Some(source_digest)
    {
        return Err(MempoolEntryBuildError::AuthDigestMismatch {
            source_digest: Some(source_digest),
            parsed_digest: parsed_auth_digest,
        });
    }
    let auth_digest = source_entry.auth_digest.or(parsed_auth_digest);

    let compact_transaction_data = compact_transaction_data(&parsed_transaction)
        .map_err(|source| MempoolEntryBuildError::CompactTransactionBuildFailed { source })?;
    MempoolEntry::new(
        source_entry.transaction_id,
        auth_digest,
        source_entry.raw_transaction_bytes,
        compact_transaction_data,
        MempoolObservation {
            first_seen_unix_millis: source_entry.observed_at_unix_millis,
            first_seen_chain_epoch: visible_chain_epoch,
        },
    )
    .map_err(|_| MempoolEntryBuildError::TransparentOutputIndexOverflow)
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use serde_json::Value;
    use zebra_chain::{
        block::Block as ZebraBlock,
        serialization::{ZcashDeserializeInto as _, ZcashSerialize as _},
    };

    use super::{MempoolEntryBuildError, build_mempool_entry};
    use zinder_core::{
        AuthDigest, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata, Network,
        RawTransactionBytes, TransactionId, UnixTimestampMillis,
    };
    use zinder_source::MempoolSourceEntry;
    use zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION;

    fn synthetic_chain_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(100),
            visible_tip_hash: BlockHash::from_bytes([0x42; 32]),
            settled_tip_height: BlockHeight::new(100),
            settled_tip_hash: BlockHash::from_bytes([0x42; 32]),
            artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1_700_000_000_000),
        }
    }

    #[test]
    fn build_mempool_entry_rejects_unparseable_bytes() {
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes([0xFF; 32]),
            auth_digest: Some(AuthDigest::from_bytes([0xAB; 32])),
            raw_transaction_bytes: RawTransactionBytes::new(vec![0u8; 4]),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let outcome = build_mempool_entry(source_entry, synthetic_chain_epoch());

        assert!(matches!(
            outcome,
            Err(MempoolEntryBuildError::TransactionParseFailed { .. })
        ));
    }

    #[test]
    fn build_mempool_entry_rejects_short_payload() {
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes([0xCD; 32]),
            auth_digest: None,
            raw_transaction_bytes: RawTransactionBytes::new(vec![0u8; 1]),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let outcome = build_mempool_entry(source_entry, synthetic_chain_epoch());

        assert!(matches!(
            outcome,
            Err(MempoolEntryBuildError::TransactionParseFailed { .. })
        ));
    }

    #[test]
    fn build_mempool_entry_rejects_source_transaction_id_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/z3-regtest-block-1.json"))?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex is missing")?;
        let raw_block_bytes = hex::decode(raw_block_hex)?;
        let parsed_block: ZebraBlock = raw_block_bytes.zcash_deserialize_into()?;
        let transaction = parsed_block
            .transactions
            .first()
            .ok_or("fixture block has no transaction")?;
        let raw_transaction_bytes = transaction.zcash_serialize_to_vec()?;
        let parsed_transaction_id = TransactionId::from_bytes(transaction.hash().0);
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes([0xFF; 32]),
            auth_digest: None,
            raw_transaction_bytes: RawTransactionBytes::new(raw_transaction_bytes),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let outcome = build_mempool_entry(source_entry, synthetic_chain_epoch());

        assert!(matches!(
            outcome,
            Err(MempoolEntryBuildError::TransactionIdMismatch {
                source_id,
                parsed_id,
            }) if source_id == TransactionId::from_bytes([0xFF; 32])
                && parsed_id == parsed_transaction_id
        ));
        Ok(())
    }

    #[test]
    fn build_mempool_entry_rejects_source_auth_digest_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/z3-regtest-block-1.json"))?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex is missing")?;
        let raw_block_bytes = hex::decode(raw_block_hex)?;
        let parsed_block: ZebraBlock = raw_block_bytes.zcash_deserialize_into()?;
        let transaction = parsed_block
            .transactions
            .first()
            .ok_or("fixture block has no transaction")?;
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes(transaction.hash().0),
            auth_digest: Some(AuthDigest::from_bytes([0xAB; 32])),
            raw_transaction_bytes: RawTransactionBytes::new(transaction.zcash_serialize_to_vec()?),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let outcome = build_mempool_entry(source_entry, synthetic_chain_epoch());

        assert!(matches!(
            outcome,
            Err(MempoolEntryBuildError::AuthDigestMismatch {
                source_digest: Some(_),
                parsed_digest: None,
            })
        ));
        Ok(())
    }

    #[test]
    fn build_mempool_entry_derives_auth_digest_when_source_omits_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/z3-regtest-ironwood-block-603.json"
        ))?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex is missing")?;
        let raw_block_bytes = hex::decode(raw_block_hex)?;
        let parsed_block: ZebraBlock = raw_block_bytes.zcash_deserialize_into()?;
        let transaction = parsed_block
            .transactions
            .iter()
            .find(|transaction| transaction.auth_digest().is_some())
            .ok_or("fixture block has no witnessed transaction")?;
        let parsed_auth_digest = transaction
            .auth_digest()
            .map(|digest| AuthDigest::from_bytes(digest.0));
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes(transaction.hash().0),
            auth_digest: None,
            raw_transaction_bytes: RawTransactionBytes::new(transaction.zcash_serialize_to_vec()?),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let entry = build_mempool_entry(source_entry, synthetic_chain_epoch())?;

        assert_eq!(entry.auth_digest(), parsed_auth_digest);
        Ok(())
    }

    #[test]
    fn build_mempool_entry_strictly_rejects_supplied_witness_digest_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/z3-regtest-ironwood-block-603.json"
        ))?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex is missing")?;
        let raw_block_bytes = hex::decode(raw_block_hex)?;
        let parsed_block: ZebraBlock = raw_block_bytes.zcash_deserialize_into()?;
        let transaction = parsed_block
            .transactions
            .iter()
            .find(|transaction| transaction.auth_digest().is_some())
            .ok_or("fixture block has no witnessed transaction")?;
        let source_entry = MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes(transaction.hash().0),
            auth_digest: Some(AuthDigest::from_bytes([0xAB; 32])),
            raw_transaction_bytes: RawTransactionBytes::new(transaction.zcash_serialize_to_vec()?),
            observed_at_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
        };

        let outcome = build_mempool_entry(source_entry, synthetic_chain_epoch());

        assert!(matches!(
            outcome,
            Err(MempoolEntryBuildError::AuthDigestMismatch {
                source_digest: Some(_),
                parsed_digest: Some(_),
            })
        ));
        Ok(())
    }
}
