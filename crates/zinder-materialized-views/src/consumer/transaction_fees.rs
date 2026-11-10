//! `TransactionFees` materialized-view consumer.
//!
//! Materializes one [`TransactionFeesRecord`] per non-coinbase canonical
//! transaction into the consumer-owned `transaction_fees` column family
//! keyed by the canonical transaction id. The handler-side
//! `TransactionDetail` reads the record at request time to populate
//! `paid_fee_zat`, `prevout_resolution_status`, and the per-input
//! `value_zat` list when the wallet plane's transparent-output
//! resolution capability is online.
//!
//! When prevout resolution is offline the consumer still writes a row
//! per transaction with status `UNAVAILABLE` so the read path stays
//! uniform: the wire surface either has the field populated or carries
//! a typed `UNAVAILABLE` chip; it never silently zeroes the value.
//!
//! `paid_fee_zat` is materialized only for fully resolved transparent-only
//! transactions. Canonical transaction facts identify shielded components but
//! do not retain their value balances, so a transparent input/output delta in a
//! shielding or mixed transaction is a transfer amount, not a provable fee.
//!
//! ## Rewind correctness
//!
//! The primary records are keyed by txid (32 bytes), so the consumer
//! maintains a per-height txid index in a second column family
//! [`TRANSACTION_FEES_INDEX_COLUMN_FAMILY`] keyed by ascending height.
//! On revert the consumer reads the persisted txid list for the reverted
//! height, deletes each primary record, and deletes the index entry. This
//! is what makes rewind correct under reorg: a reorg-then-rewrite at
//! height H replaces the canonical block, so re-fetching the block at H
//! and using *its* txids to drive deletes would delete the WRONG txids.
//! The persisted index captures what was actually written at apply time.

use std::collections::HashMap;

use prost::Message as _;
use zinder_core::wire::{encode_height_key_ascending, encode_internal_transaction_id};
use zinder_core::{
    BlockHeight, PrivacyShape, TransactionFactsArtifact, TransactionId, TransparentOutPoint,
    TransparentSpendFact,
};
use zinder_proto::v1::explorer::{
    PrevoutResolutionStatus, TransactionFeesRecord, TransparentInputValueRecord,
};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};

/// Column family holding per-transaction fee records keyed by 32-byte txid.
pub const TRANSACTION_FEES_COLUMN_FAMILY: &str = "transaction_fees";

/// Column family holding the per-height txid index used by rewind.
///
/// Key: ascending big-endian block height (4 bytes).
/// Value: concatenation of 32-byte non-coinbase txids in canonical order.
pub const TRANSACTION_FEES_INDEX_COLUMN_FAMILY: &str = "transaction_fees_index";

/// Column families the consumer needs registered before its first write.
pub const TRANSACTION_FEES_COLUMN_FAMILIES: &[&str] = &[
    TRANSACTION_FEES_COLUMN_FAMILY,
    TRANSACTION_FEES_INDEX_COLUMN_FAMILY,
];

/// Stable consumer name persisted in the SDK cursor table.
pub const TRANSACTION_FEES_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transaction_fees");

/// On-disk schema declaration for the transaction-fees materialized-view consumer.
pub const TRANSACTION_FEES_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSACTION_FEES_CONSUMER_NAME,
        2,
        TRANSACTION_FEES_COLUMN_FAMILIES,
    );

const TXID_LEN: usize = 32;

/// Materializes per-tx paid-fee records.
#[derive(Default)]
pub struct TransactionFeesConsumer;

impl TransactionFeesConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns the servable [`TransactionFeesRecord`] for `transaction_id`,
    /// when one was materialized.
    ///
    /// `paid_fee_zat` is provable only for an independently classified
    /// [`PrivacyShape::TransparentOnly`] transaction. Requiring `privacy_shape`
    /// lets this reader suppress an unprovable fee before it crosses the
    /// materialized-view boundary. Handlers never receive an unservable stored
    /// record.
    pub fn read_fees_record(
        store: &crate::store::MaterializedViewStore,
        transaction_id: TransactionId,
        privacy_shape: PrivacyShape,
    ) -> Result<Option<TransactionFeesRecord>, crate::error::MaterializedViewStoreError> {
        let key = encode_internal_transaction_id(transaction_id);
        let Some(bytes) = store.get_consumer(TRANSACTION_FEES_COLUMN_FAMILY, &key)? else {
            return Ok(None);
        };
        Ok(TransactionFeesRecord::decode(bytes.as_slice())
            .ok()
            .map(|record| record_with_provable_paid_fee(record, privacy_shape)))
    }

    /// Batch-reads servable fee records for transaction ids and their independently
    /// classified privacy shapes. Issues one `multi_get_cf` so the read path
    /// avoids N seeks for an N-transaction page.
    pub fn read_fees_records_many(
        store: &crate::store::MaterializedViewStore,
        transactions: &[(TransactionId, PrivacyShape)],
    ) -> Result<
        HashMap<TransactionId, TransactionFeesRecord>,
        crate::error::MaterializedViewStoreError,
    > {
        if transactions.is_empty() {
            return Ok(HashMap::new());
        }
        let keys: Vec<[u8; TXID_LEN]> = transactions
            .iter()
            .map(|(transaction_id, _privacy_shape)| encode_internal_transaction_id(*transaction_id))
            .collect();
        let values = store.multi_get_consumer(TRANSACTION_FEES_COLUMN_FAMILY, &keys)?;
        let mut out = HashMap::with_capacity(values.len());
        for ((transaction_id, privacy_shape), maybe_bytes) in
            transactions.iter().copied().zip(values)
        {
            if let Some(bytes) = maybe_bytes
                && let Ok(record) = TransactionFeesRecord::decode(bytes.as_slice())
            {
                out.insert(
                    transaction_id,
                    record_with_provable_paid_fee(record, privacy_shape),
                );
            }
        }
        Ok(out)
    }
}

fn record_with_provable_paid_fee(
    mut record: TransactionFeesRecord,
    privacy_shape: PrivacyShape,
) -> TransactionFeesRecord {
    if privacy_shape != PrivacyShape::TransparentOnly {
        record.paid_fee_zat = None;
    }
    record
}

impl BlockKeyedConsumer for TransactionFeesConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSACTION_FEES_CONSUMER_NAME
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let transparent_spends = block.transparent_spends();
        let fees_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_FEES_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_FEES_INDEX_COLUMN_FAMILY)?;

        let non_coinbase = block
            .transactions
            .iter()
            .filter(|transaction| !transaction.public_facts.is_coinbase);
        let non_coinbase_count = block
            .transactions
            .iter()
            .filter(|transaction| !transaction.public_facts.is_coinbase)
            .count();
        let mut index_payload: Vec<u8> = Vec::with_capacity(non_coinbase_count * TXID_LEN);

        for transaction in non_coinbase {
            let transaction_id_bytes = transaction.location.transaction_id.as_bytes();
            let record = build_fee_record(transaction, transparent_spends.as_deref());
            let mut payload = Vec::with_capacity(record.encoded_len());
            record
                .encode(&mut payload)
                .map_err(|error| TransactionFeesConsumerError::Encode(error.to_string()))?;
            ctx.batch.put_cf(&fees_cf, transaction_id_bytes, payload);
            index_payload.extend_from_slice(&transaction_id_bytes);
        }

        ctx.batch.put_cf(
            &index_cf,
            encode_height_key_ascending(block.height),
            index_payload,
        );
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_FEES_INDEX_COLUMN_FAMILY)?;
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx
            .store
            .get_consumer(TRANSACTION_FEES_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        if index_payload.len() % TXID_LEN != 0 {
            return Err(Box::new(
                TransactionFeesConsumerError::IndexLengthMismatch {
                    height: height.value(),
                    bytes: index_payload.len(),
                },
            ));
        }
        let fees_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_FEES_COLUMN_FAMILY)?;
        for chunk in index_payload.chunks_exact(TXID_LEN) {
            ctx.batch.delete_cf(&fees_cf, chunk);
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }
}

fn build_fee_record(
    transaction: &TransactionFactsArtifact,
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
) -> TransactionFeesRecord {
    let counts = transaction.public_facts.counts;
    let logical_actions = counts.logical_actions();

    let Some(spends_by_outpoint) = transparent_spends else {
        return TransactionFeesRecord {
            paid_fee_zat: None,
            prevout_resolution_status: PrevoutResolutionStatus::Unavailable as i32,
            transparent_inputs: Vec::new(),
            logical_actions,
        };
    };

    let mut transparent_inputs: Vec<TransparentInputValueRecord> = Vec::new();
    let mut has_unresolved = false;
    for input in &transaction.transparent_inputs {
        let value_zat = spends_by_outpoint
            .get(&input.spent_outpoint)
            .map(|spend| spend.spent_value_zat);
        if value_zat.is_none() {
            has_unresolved = true;
        }
        transparent_inputs.push(TransparentInputValueRecord {
            input_index: input.input_index,
            value_zat,
        });
    }
    let is_transparent_only =
        transaction.public_facts.privacy_shape == PrivacyShape::TransparentOnly;
    let status = if has_unresolved {
        PrevoutResolutionStatus::Partial
    } else {
        PrevoutResolutionStatus::Resolved
    };
    let paid_fee_zat = if has_unresolved || !is_transparent_only {
        None
    } else {
        paid_fee_from_input_details(transaction, &transparent_inputs)
    };
    TransactionFeesRecord {
        paid_fee_zat,
        prevout_resolution_status: status as i32,
        transparent_inputs,
        logical_actions,
    }
}

fn paid_fee_from_input_details(
    transaction: &TransactionFactsArtifact,
    transparent_inputs: &[TransparentInputValueRecord],
) -> Option<u64> {
    let total_transparent_input_zat =
        transparent_inputs.iter().try_fold(0_i128, |sum, input| {
            input
                .value_zat
                .map(|value_zat| sum.saturating_add(i128::from(value_zat)))
        })?;
    let total_transparent_output_zat = transaction
        .transparent_outputs
        .iter()
        .fold(0_i128, |sum, output| {
            sum.saturating_add(i128::from(output.value_zat))
        });
    total_transparent_input_zat
        .checked_sub(total_transparent_output_zat)
        .filter(|net| *net >= 0)
        .and_then(|net| u64::try_from(net).ok())
}

/// Consumer-specific failure modes [`TransactionFeesConsumer`] can surface.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransactionFeesConsumerError {
    /// Storage encoding of the materialized record failed.
    #[error("TransactionFeesRecord prost encode failed: {0}")]
    Encode(String),
    /// Per-height txid index was not a clean multiple of 32 bytes.
    #[error(
        "transaction_fees_index entry for height {height} has {bytes} bytes, not a multiple of 32"
    )]
    IndexLengthMismatch {
        /// Height whose persisted index was malformed.
        height: u32,
        /// Byte length actually persisted.
        bytes: usize,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use zinder_core::{
        BlockHash, BlockHeight, LockTime, PrivacyShape, TransactionComponentCounts,
        TransactionFactsArtifact, TransactionId, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
        TransparentOutPoint, TransparentOutputFact, TransparentSpendFact, classify_privacy_shape,
    };
    use zinder_proto::v1::explorer::{
        PrevoutResolutionStatus, TransactionFeesRecord, TransparentInputValueRecord,
    };

    use super::{build_fee_record, record_with_provable_paid_fee};

    const TRANSPARENT_INPUT_VALUE_ZAT: u64 = 50_000;
    const TRANSPARENT_OUTPUT_VALUE_ZAT: u64 = 40_000;

    fn transaction_with_resolved_transparent_input(
        counts: TransactionComponentCounts,
        transparent_output_value_zat: Option<u64>,
    ) -> (
        TransactionFactsArtifact,
        HashMap<TransparentOutPoint, TransparentSpendFact>,
    ) {
        let transaction_id = TransactionId::from_bytes([2; 32]);
        let block_height = BlockHeight::new(100);
        let block_hash = BlockHash::from_bytes([3; 32]);
        let spent_outpoint = TransparentOutPoint::new(TransactionId::from_bytes([1; 32]), 0);
        let transparent_outputs = transparent_output_value_zat.map_or_else(Vec::new, |value_zat| {
            vec![TransparentOutputFact::new(
                0,
                value_zat,
                b"script".to_vec(),
                TransparentAddressScriptHash::from_bytes([4; 32]),
            )]
        });
        let transaction = TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, block_height, block_hash, 1),
            TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 0,
                counts,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: classify_privacy_shape(counts, false, TransactionVersion::V5),
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
        )
        .with_transparent_facts(
            vec![TransparentInputFact::new(0, spent_outpoint)],
            transparent_outputs,
        );
        let transparent_spends = HashMap::from([(
            spent_outpoint,
            TransparentSpendFact::new(
                spent_outpoint,
                0,
                transaction_id,
                1,
                block_height,
                block_hash,
                TRANSPARENT_INPUT_VALUE_ZAT,
                TransparentAddressScriptHash::from_bytes([5; 32]),
                BlockHeight::new(50),
                BlockHash::from_bytes([6; 32]),
            ),
        )]);
        (transaction, transparent_spends)
    }

    #[test]
    fn transparent_only_transaction_materializes_resolved_paid_fee() {
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let (transaction, transparent_spends) =
            transaction_with_resolved_transparent_input(counts, Some(TRANSPARENT_OUTPUT_VALUE_ZAT));

        let record = build_fee_record(&transaction, Some(&transparent_spends));

        assert_eq!(record.paid_fee_zat, Some(10_000));
        assert_eq!(
            record.prevout_resolution_status,
            PrevoutResolutionStatus::Resolved as i32
        );
    }

    #[test]
    fn shielding_transaction_does_not_treat_transparent_input_as_paid_fee() {
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            sapling_output_count: 2,
            ..TransactionComponentCounts::EMPTY
        };
        let (transaction, transparent_spends) =
            transaction_with_resolved_transparent_input(counts, None);

        let record = build_fee_record(&transaction, Some(&transparent_spends));

        assert_eq!(record.paid_fee_zat, None);
        assert_eq!(
            record.prevout_resolution_status,
            PrevoutResolutionStatus::Resolved as i32
        );
    }

    #[test]
    fn mixed_transaction_does_not_treat_transparent_delta_as_paid_fee() {
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let (transaction, transparent_spends) =
            transaction_with_resolved_transparent_input(counts, Some(TRANSPARENT_OUTPUT_VALUE_ZAT));

        let record = build_fee_record(&transaction, Some(&transparent_spends));

        assert_eq!(record.paid_fee_zat, None);
        assert_eq!(
            record.prevout_resolution_status,
            PrevoutResolutionStatus::Resolved as i32
        );
    }

    #[test]
    fn unclassified_transaction_keeps_paid_fee_unavailable() {
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let (mut transaction, transparent_spends) =
            transaction_with_resolved_transparent_input(counts, Some(TRANSPARENT_OUTPUT_VALUE_ZAT));
        transaction.public_facts.privacy_shape = PrivacyShape::Unclassified;

        let record = build_fee_record(&transaction, Some(&transparent_spends));

        assert_eq!(record.paid_fee_zat, None);
        assert_eq!(
            record.prevout_resolution_status,
            PrevoutResolutionStatus::Resolved as i32
        );
    }

    #[test]
    fn non_transparent_paid_fee_is_suppressed_at_read_boundary() {
        let stored_record = TransactionFeesRecord {
            paid_fee_zat: Some(TRANSPARENT_INPUT_VALUE_ZAT),
            prevout_resolution_status: PrevoutResolutionStatus::Resolved as i32,
            transparent_inputs: vec![TransparentInputValueRecord {
                input_index: 0,
                value_zat: Some(TRANSPARENT_INPUT_VALUE_ZAT),
            }],
            logical_actions: 2,
        };

        let record = record_with_provable_paid_fee(stored_record, PrivacyShape::Shielding);

        assert_eq!(record.paid_fee_zat, None);
        assert_eq!(record.transparent_inputs[0].value_zat, Some(50_000));
        assert_eq!(
            record.prevout_resolution_status,
            PrevoutResolutionStatus::Resolved as i32
        );
    }

    #[test]
    fn transparent_only_paid_fee_remains_available_at_read_boundary() {
        let stored_record = TransactionFeesRecord {
            paid_fee_zat: Some(10_000),
            prevout_resolution_status: PrevoutResolutionStatus::Resolved as i32,
            transparent_inputs: Vec::new(),
            logical_actions: 1,
        };

        let record = record_with_provable_paid_fee(stored_record, PrivacyShape::TransparentOnly);

        assert_eq!(record.paid_fee_zat, Some(10_000));
    }
}
