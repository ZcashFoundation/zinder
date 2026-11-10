//! `TransparentOutpointSpend` materialized-view consumer.
//!
//! Materializes durable spender identity keyed by spent outpoint. Canonical
//! storage keeps block-local facts only and has no spend-keyed family, so this
//! consumer is the reverse-spend index that resolves an outpoint to the
//! transaction that spent it, however long ago.
//!
//! Authority split: spentness is decided by the wallet projection's
//! unspent-output rows (`LtHash16`-committed). This projection answers only
//! *who* spent an outpoint, never *whether* it is spent. A missing row means
//! "no spender recorded", never "unspent".
//!
//! The consumer derives spender identity from the child transaction's intrinsic
//! input and mined location, not from the parent output.

use std::collections::HashMap;

use zinder_core::wire::{
    OUTPOINT_KEY_LEN, decode_height_key_ascending, decode_internal_block_hash,
    decode_internal_transaction_id, encode_height_key_ascending, encode_internal_block_hash,
    encode_internal_transaction_id, encode_outpoint_key,
};
use zinder_core::{BlockHeight, TransparentOutPoint, TransparentSpendEntry, TransparentSpendFact};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewBlockCheckpoint,
    MaterializedViewConsumerCtx, MaterializedViewConsumerError, MaterializedViewConsumerName,
    MaterializedViewConsumerSchema, advance_verified_materialized_view_coverage,
};
use crate::error::{MaterializedViewStoreColumnFamily, MaterializedViewStoreError};
use crate::store::{
    MaterializedViewState, MaterializedViewStore, MaterializedViewStoreReadSnapshot,
};
use zinder_store::ChainEvent;

/// Primary rows keyed by spent outpoint, valued with the spender identity.
pub const TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY: &str = "transparent_outpoint_spend";

/// Per-height index used to delete the primary rows during reorg rewind.
pub const TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY: &str = "transparent_outpoint_spend_index";

/// Column families the consumer needs registered before its first write.
pub const TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILIES: &[&str] = &[
    TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY,
    TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY,
];

/// Stable consumer name persisted in the materialized-view cursor table.
pub const TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transparent_outpoint_spend");

/// On-disk schema declaration for the transparent-outpoint-spend consumer.
///
/// Moving this version requires a fresh materialized-view store replayed from
/// genesis-complete canonical history.
pub const TRANSPARENT_OUTPOINT_SPEND_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME,
        1,
        TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILIES,
    );

const TRANSACTION_ID_LEN: usize = 32;
const BLOCK_HASH_LEN: usize = 32;
const HEIGHT_LEN: usize = 4;
const INPUT_INDEX_LEN: usize = 4;
const SPEND_VALUE_LEN: usize = TRANSACTION_ID_LEN + BLOCK_HASH_LEN + HEIGHT_LEN + INPUT_INDEX_LEN;
const SPENDING_TRANSACTION_ID_RANGE: std::ops::Range<usize> = 0..TRANSACTION_ID_LEN;
const SPENDING_BLOCK_HASH_RANGE: std::ops::Range<usize> =
    TRANSACTION_ID_LEN..TRANSACTION_ID_LEN + BLOCK_HASH_LEN;
const SPENDING_HEIGHT_RANGE: std::ops::Range<usize> =
    TRANSACTION_ID_LEN + BLOCK_HASH_LEN..TRANSACTION_ID_LEN + BLOCK_HASH_LEN + HEIGHT_LEN;
const INPUT_INDEX_RANGE: std::ops::Range<usize> =
    TRANSACTION_ID_LEN + BLOCK_HASH_LEN + HEIGHT_LEN..SPEND_VALUE_LEN;

/// Materializes durable transparent-outpoint spender identity.
#[derive(Default)]
pub struct TransparentOutpointSpendConsumer;

#[derive(Clone, Copy)]
enum TransparentOutpointSpendRead<'store> {
    Store(&'store MaterializedViewStore),
    Snapshot(&'store MaterializedViewStoreReadSnapshot<'store>),
}

impl TransparentOutpointSpendRead<'_> {
    fn multi_get_consumer<K>(
        self,
        column_family: &'static str,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, MaterializedViewStoreError>
    where
        K: AsRef<[u8]>,
    {
        match self {
            Self::Store(store) => store.multi_get_consumer(column_family, keys),
            Self::Snapshot(snapshot) => snapshot.multi_get_consumer(column_family, keys),
        }
    }
}

impl TransparentOutpointSpendConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Resolves the recorded spender for each requested outpoint.
    ///
    /// Outpoints with no recorded spender are absent from the returned map;
    /// absence means "no spender recorded here", never "unspent".
    pub fn read_spends_by_outpoints(
        store: &MaterializedViewStore,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendEntry>, MaterializedViewStoreError>
    {
        Self::read_spends_by_outpoints_from(TransparentOutpointSpendRead::Store(store), outpoints)
    }

    /// Resolves recorded spenders from one materialized-view snapshot.
    pub fn read_spends_by_outpoints_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendEntry>, MaterializedViewStoreError>
    {
        Self::read_spends_by_outpoints_from(
            TransparentOutpointSpendRead::Snapshot(snapshot),
            outpoints,
        )
    }

    fn read_spends_by_outpoints_from(
        store: TransparentOutpointSpendRead<'_>,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendEntry>, MaterializedViewStoreError>
    {
        if outpoints.is_empty() {
            return Ok(HashMap::new());
        }
        let keys = outpoints
            .iter()
            .map(|outpoint| encode_outpoint_key(*outpoint))
            .collect::<Vec<_>>();
        let stored_rows =
            store.multi_get_consumer(TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY, &keys)?;
        let mut spends = HashMap::with_capacity(outpoints.len());
        for (outpoint, stored_row) in outpoints.iter().zip(stored_rows) {
            let Some(stored_row) = stored_row else {
                continue;
            };
            spends.insert(*outpoint, decode_spend_entry(*outpoint, &stored_row)?);
        }
        Ok(spends)
    }
}

impl BlockKeyedConsumer for TransparentOutpointSpendConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        // Spender identity is intrinsic to the child transaction input and its
        // mined location. Parent-output hydration supplies value and script
        // facts to other consumers and is not an input to these rows.
        let rows = collect_spend_rows(block);
        let spend_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY)?;

        let mut index_payload = Vec::with_capacity(rows.len() * OUTPOINT_KEY_LEN);
        for spend in rows {
            let key = encode_outpoint_key(spend.spent_outpoint);
            ctx.batch.put_cf(
                &spend_cf,
                key,
                encode_transparent_spend_entry_row_value(&spend).as_slice(),
            );
            index_payload.extend_from_slice(&key);
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
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx
            .store
            .get_consumer(TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        if index_payload.len() % OUTPOINT_KEY_LEN != 0 {
            return Err(Box::new(
                TransparentOutpointSpendConsumerError::IndexLengthMismatch {
                    height: height.value(),
                    bytes: index_payload.len(),
                },
            ));
        }
        let spend_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY)?;
        for chunk in index_payload.chunks_exact(OUTPOINT_KEY_LEN) {
            ctx.batch.delete_cf(&spend_cf, chunk);
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }

    fn stage_chain_event_checkpoint(
        &mut self,
        checkpoint: MaterializedViewBlockCheckpoint<'_>,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let tip_height = checkpoint
            .tip_height
            .ok_or(TransparentOutpointSpendConsumerError::IncompleteMaterializedViewCheckpoint)?;
        let tip_hash = checkpoint
            .tip_hash
            .ok_or(TransparentOutpointSpendConsumerError::IncompleteMaterializedViewCheckpoint)?;
        let current = ctx
            .store
            .consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?;
        // Re-applying an earlier committed chunk must not regress coverage a
        // later chunk has already made durable.
        if let Some(state) = current
            && matches!(checkpoint.chain_event, ChainEvent::ChainCommitted { .. })
            && tip_height < state.tip_height
        {
            return Ok(());
        }
        let revision = current
            .map_or(Some(1), |state| state.revision.checked_add(1))
            .ok_or(TransparentOutpointSpendConsumerError::MaterializedViewRevisionOverflow)?;
        let initial_complete_from = match checkpoint.chain_event {
            ChainEvent::ChainCommitted { committed }
            | ChainEvent::ChainReorged { committed, .. }
                if committed.block_range.start <= committed.block_range.end =>
            {
                Some(committed.block_range.start)
            }
            ChainEvent::ChainCommitted { .. } | ChainEvent::ChainReorged { .. } | _ => None,
        };
        let coverage = advance_verified_materialized_view_coverage(
            current.and_then(|state| state.coverage),
            checkpoint,
            tip_height,
            tip_hash,
            initial_complete_from,
        );
        ctx.store.stage_consumer_state(
            ctx.batch,
            TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME,
            MaterializedViewState {
                chain_epoch_id: checkpoint.chain_epoch.id,
                tip_height,
                tip_hash,
                revision,
                coverage,
            },
        )?;
        Ok(())
    }
}

fn collect_spend_rows(block: &BlockCommitContext) -> Vec<TransparentSpendEntry> {
    let mut rows = Vec::new();
    for transaction in &block.transactions {
        for input in &transaction.transparent_inputs {
            if input.spent_outpoint != TransparentOutPoint::COINBASE_SENTINEL {
                rows.push(TransparentSpendEntry {
                    spent_outpoint: input.spent_outpoint,
                    spending_transaction_id: transaction.location.transaction_id,
                    input_index: input.input_index,
                    spending_block_height: transaction.location.block_height,
                    spending_block_hash: transaction.location.block_hash,
                });
            }
        }
    }
    rows
}

/// Encodes a transparent spend fact into its durable projection row value.
///
/// Layout: spending transaction id, spending block hash, spending height, then
/// transparent input index. This is the single owner of the row's byte layout;
/// test seeders reuse it instead of hand-writing the same offsets.
#[must_use]
pub fn encode_transparent_spend_row_value(spend: &TransparentSpendFact) -> [u8; SPEND_VALUE_LEN] {
    encode_transparent_spend_entry_row_value(&TransparentSpendEntry {
        spent_outpoint: spend.spent_outpoint,
        spending_transaction_id: spend.spending_transaction_id,
        input_index: spend.input_index,
        spending_block_height: spend.block_height,
        spending_block_hash: spend.block_hash,
    })
}

fn encode_transparent_spend_entry_row_value(
    spend: &TransparentSpendEntry,
) -> [u8; SPEND_VALUE_LEN] {
    let mut encoded = [0u8; SPEND_VALUE_LEN];
    encoded[SPENDING_TRANSACTION_ID_RANGE].copy_from_slice(&encode_internal_transaction_id(
        spend.spending_transaction_id,
    ));
    encoded[SPENDING_BLOCK_HASH_RANGE]
        .copy_from_slice(&encode_internal_block_hash(spend.spending_block_hash));
    encoded[SPENDING_HEIGHT_RANGE]
        .copy_from_slice(&encode_height_key_ascending(spend.spending_block_height));
    encoded[INPUT_INDEX_RANGE].copy_from_slice(&spend.input_index.to_be_bytes());
    encoded
}

fn decode_spend_entry(
    spent_outpoint: TransparentOutPoint,
    stored_row: &[u8],
) -> Result<TransparentSpendEntry, MaterializedViewStoreError> {
    if stored_row.len() != SPEND_VALUE_LEN {
        return Err(decode_error("spend value length is invalid"));
    }
    let spending_transaction_id =
        decode_internal_transaction_id(&stored_row[SPENDING_TRANSACTION_ID_RANGE])
            .map_err(|error| decode_error(error.to_string()))?;
    let spending_block_hash = decode_internal_block_hash(&stored_row[SPENDING_BLOCK_HASH_RANGE])
        .map_err(|error| decode_error(error.to_string()))?;
    let spending_block_height = decode_height_key_ascending(&stored_row[SPENDING_HEIGHT_RANGE])
        .map_err(|error| decode_error(error.to_string()))?;
    let input_index_bytes: [u8; INPUT_INDEX_LEN] = stored_row[INPUT_INDEX_RANGE]
        .try_into()
        .map_err(|_| decode_error("input index length is invalid"))?;
    Ok(TransparentSpendEntry {
        spent_outpoint,
        spending_transaction_id,
        input_index: u32::from_be_bytes(input_index_bytes),
        spending_block_height,
        spending_block_hash,
    })
}

fn decode_error(reason: impl Into<String>) -> MaterializedViewStoreError {
    MaterializedViewStoreError::Decode {
        column_family: MaterializedViewStoreColumnFamily::ConsumerMetadata,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "tests assert on a known-present row; absence is a test-code bug, not a runtime condition."
    )]

    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId,
        ChainTipMetadata, LockTime, Network, PrivacyShape, TransactionComponentCounts,
        TransactionFactsArtifact, TransactionId, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
        TransparentOutPoint, TransparentSpendFact, UnixTimestampMillis,
    };
    use zinder_store::{ChainEpochCommitted, ChainEvent, RocksDbResourceBudget};
    use zinder_testkit::process_crash;

    use super::{
        TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY,
        TRANSPARENT_OUTPOINT_SPEND_SCHEMA, TransparentOutpointSpendConsumer,
    };
    use crate::consumer::block_commit_context::{
        BlockCommitContext, BlockCommitInput, TransparentSpendFacts,
    };
    use crate::consumer::{
        BlockKeyedConsumer, MaterializedViewBlockCheckpoint, MaterializedViewConsumerCtx,
    };
    use crate::store::{
        ChainEventDispatchInputs, MaterializedViewStore, MaterializedViewStoreOptions,
    };

    const SPENT_ADDRESS: TransparentAddressScriptHash =
        TransparentAddressScriptHash::from_bytes([7; 32]);
    const RECEIVE_HEIGHT: BlockHeight = BlockHeight::new(100);
    const SPEND_HEIGHT: BlockHeight = BlockHeight::new(105);

    fn transaction_id(seed: u8) -> TransactionId {
        TransactionId::from_bytes([seed; 32])
    }

    fn block_hash(seed: u8) -> BlockHash {
        BlockHash::from_bytes([seed; 32])
    }

    fn chain_epoch(id: u64, tip: BlockHeight, tip_hash: BlockHash) -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(id),
            network: Network::ZcashRegtest,
            visible_tip_height: tip,
            visible_tip_hash: tip_hash,
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: block_hash(1),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(id),
        }
    }

    fn public_facts(seed: u8) -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id: transaction_id(seed),
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V5,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 0,
            counts: TransactionComponentCounts::EMPTY,
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase: false,
            unsupported_sections: Vec::new(),
        }
    }

    fn received_outpoint() -> TransparentOutPoint {
        TransparentOutPoint::new(transaction_id(10), 0)
    }

    fn spend_fact() -> TransparentSpendFact {
        TransparentSpendFact::new(
            received_outpoint(),
            2,
            transaction_id(20),
            0,
            SPEND_HEIGHT,
            block_hash(5),
            5_000,
            SPENT_ADDRESS,
            RECEIVE_HEIGHT,
            block_hash(1),
        )
    }

    fn spend_block() -> BlockCommitContext {
        let mut spends = HashMap::new();
        spends.insert(received_outpoint(), spend_fact());
        spend_block_with_facts(TransparentSpendFacts::Static(Arc::new(spends)))
    }

    fn spend_block_with_facts(spend_facts: TransparentSpendFacts) -> BlockCommitContext {
        let location = TransactionLocation::new(transaction_id(20), SPEND_HEIGHT, block_hash(5), 0);
        let transaction = TransactionFactsArtifact::new(location, public_facts(20))
            .with_transparent_facts(
                vec![TransparentInputFact::new(2, received_outpoint())],
                Vec::new(),
            );
        BlockCommitContext::new(
            BlockCommitInput {
                height: SPEND_HEIGHT,
                block_hash: block_hash(5),
                previous_block_hash: block_hash(4),
                block_time_unix_seconds: 1_700_000_500,
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            spend_facts,
        )
    }

    fn coinbase_block() -> BlockCommitContext {
        let location =
            TransactionLocation::new(transaction_id(30), BlockHeight::new(106), block_hash(6), 0);
        let transaction = TransactionFactsArtifact::new(location, public_facts(30))
            .with_transparent_facts(
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                Vec::new(),
            );
        BlockCommitContext::new(
            BlockCommitInput {
                height: BlockHeight::new(106),
                block_hash: block_hash(6),
                previous_block_hash: block_hash(5),
                block_time_unix_seconds: 1_700_001_000,
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Static(Arc::new(HashMap::new())),
        )
    }

    fn open_store()
    -> Result<(tempfile::TempDir, MaterializedViewStore), Box<dyn std::error::Error + Send + Sync>>
    {
        let tempdir = tempdir()?;
        let store = open_store_at(tempdir.path())?;
        Ok((tempdir, store))
    }

    fn open_store_at(
        path: &Path,
    ) -> Result<MaterializedViewStore, Box<dyn std::error::Error + Send + Sync>> {
        Ok(MaterializedViewStore::open(
            path,
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_OUTPOINT_SPEND_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?)
    }

    fn apply_block(
        store: &MaterializedViewStore,
        consumer: &mut TransparentOutpointSpendConsumer,
        block: &BlockCommitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.apply_block(block, &mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    #[test]
    fn committed_checkpoint_persists_contiguous_retention_coverage()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let tip = BlockHeight::new(105);
        let tip_hash = block_hash(5);
        let epoch = chain_epoch(1, tip, tip_hash);
        let event = ChainEvent::ChainCommitted {
            committed: ChainEpochCommitted {
                chain_epoch: epoch,
                block_range: BlockHeightRange::inclusive(BlockHeight::new(100), tip),
            },
        };
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        TransparentOutpointSpendConsumer::new().stage_chain_event_checkpoint(
            MaterializedViewBlockCheckpoint {
                chain_epoch: epoch,
                chain_event: &event,
                tip_height: Some(tip),
                tip_hash: Some(tip_hash),
            },
            &mut ctx,
        )?;
        store.write_batch(&batch)?;

        let state = store
            .consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?
            .ok_or("projection state missing")?;
        let coverage = state.coverage.ok_or("projection coverage missing")?;
        assert_eq!(coverage.complete_from_height, BlockHeight::new(100));
        assert_eq!(coverage.complete_through_height, tip);
        assert_eq!(coverage.complete_through_hash, tip_hash);
        Ok(())
    }

    fn revert_block(
        store: &MaterializedViewStore,
        consumer: &mut TransparentOutpointSpendConsumer,
        height: BlockHeight,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.revert_block(height, &mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    #[test]
    fn spend_row_records_the_spender_identity()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();

        apply_block(&store, &mut consumer, &spend_block())?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[received_outpoint()],
        )?;
        let entry = spends
            .get(&received_outpoint())
            .expect("the spent outpoint must resolve to its spender");
        assert_eq!(entry.spent_outpoint, received_outpoint());
        assert_eq!(entry.spending_transaction_id, transaction_id(20));
        assert_eq!(entry.spending_block_hash, block_hash(5));
        assert_eq!(entry.spending_block_height, SPEND_HEIGHT);
        assert_eq!(entry.input_index, 2);
        assert_eq!(
            store.last_materialized_height_ascending(
                TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
            )?,
            Some(SPEND_HEIGHT)
        );
        Ok(())
    }

    #[test]
    fn unresolved_parent_still_records_spender_from_child_transaction()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();
        let block = spend_block_with_facts(TransparentSpendFacts::Static(Arc::new(HashMap::new())));

        apply_block(&store, &mut consumer, &block)?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[received_outpoint()],
        )?;
        let entry = spends
            .get(&received_outpoint())
            .expect("child transaction input must identify its spender without parent facts");
        assert_eq!(entry.spending_transaction_id, transaction_id(20));
        assert_eq!(entry.spending_block_hash, block_hash(5));
        assert_eq!(entry.spending_block_height, SPEND_HEIGHT);
        assert_eq!(entry.input_index, 2);
        Ok(())
    }

    #[test]
    fn coinbase_input_records_no_spend_row() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();

        apply_block(&store, &mut consumer, &coinbase_block())?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[TransparentOutPoint::COINBASE_SENTINEL],
        )?;
        assert!(spends.is_empty());
        // The empty per-height index row still advances the durable height.
        assert_eq!(
            store.last_materialized_height_ascending(
                TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
            )?,
            Some(BlockHeight::new(106))
        );
        Ok(())
    }

    #[test]
    fn offline_parent_facts_still_record_spender()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();
        let block = spend_block_with_facts(TransparentSpendFacts::Offline);

        apply_block(&store, &mut consumer, &block)?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[received_outpoint()],
        )?;
        assert!(spends.contains_key(&received_outpoint()));
        Ok(())
    }

    #[test]
    fn apply_then_rewind_matches_never_applied()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();

        apply_block(&store, &mut consumer, &spend_block())?;
        revert_block(&store, &mut consumer, SPEND_HEIGHT)?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[received_outpoint()],
        )?;
        assert!(spends.is_empty());
        assert_eq!(
            store.last_materialized_height_ascending(
                TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn rewind_of_top_block_keeps_lower_blocks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransparentOutpointSpendConsumer::new();

        apply_block(&store, &mut consumer, &spend_block())?;
        apply_block(&store, &mut consumer, &coinbase_block())?;
        revert_block(&store, &mut consumer, BlockHeight::new(106))?;

        let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
            &store,
            &[received_outpoint()],
        )?;
        assert!(spends.contains_key(&received_outpoint()));
        assert_eq!(
            store.last_materialized_height_ascending(
                TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
            )?,
            Some(SPEND_HEIGHT)
        );
        Ok(())
    }

    const FINAL_CHUNK_CRASH_CHILD: &str =
        "consumer::transparent_outpoint_spend::tests::final_chain_event_chunk_crash_child";
    const INTERMEDIATE_CHUNK_CRASH_CHILD: &str =
        "consumer::transparent_outpoint_spend::tests::intermediate_chain_event_chunk_crash_child";
    const CRASH_EVENT_CURSOR: &[u8] = b"canonical-event-cursor-2";

    fn write_spend_event_chunk(
        store: &MaterializedViewStore,
        advance_cursor: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tip = BlockHeight::new(106);
        let epoch = chain_epoch(2, tip, block_hash(6));
        let event = ChainEvent::ChainCommitted {
            committed: ChainEpochCommitted {
                chain_epoch: epoch,
                block_range: BlockHeightRange::inclusive(SPEND_HEIGHT, tip),
            },
        };
        let blocks = HashMap::from([
            (SPEND_HEIGHT, Arc::new(spend_block())),
            (tip, Arc::new(coinbase_block())),
        ]);
        let mut consumer = TransparentOutpointSpendConsumer::new();
        store.write_chain_event_chunk(
            &mut [&mut consumer],
            ChainEventDispatchInputs {
                chain_epoch: epoch,
                chain_event: &event,
                chain_cursor: CRASH_EVENT_CURSOR,
                event_sequence: 2,
                settled_tip_height: BlockHeight::new(1),
            },
            &blocks,
            advance_cursor,
        )?;
        Ok(())
    }

    #[test]
    fn final_chain_event_chunk_crash_child() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let Some(child) = process_crash::crash_child() else {
            return Ok(());
        };
        write_spend_event_chunk(&open_store_at(child.store_path())?, true)?;
        Err("final chain-event chunk returned without reaching its crash point".into())
    }

    #[test]
    fn intermediate_chain_event_chunk_crash_child()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(child) = process_crash::crash_child() else {
            return Ok(());
        };
        write_spend_event_chunk(&open_store_at(child.store_path())?, false)?;
        Err("intermediate chain-event chunk returned without reaching its crash point".into())
    }

    #[test]
    fn chain_event_chunk_reopens_at_one_complete_batch_across_process_abort()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let temporary = tempdir()?;
        for (child_test, crash_point, rows_durable, cursor_durable) in [
            (FINAL_CHUNK_CRASH_CHILD, "before_write", false, false),
            (FINAL_CHUNK_CRASH_CHILD, "after_write", true, true),
            (INTERMEDIATE_CHUNK_CRASH_CHILD, "after_write", true, false),
        ] {
            let store_path = temporary
                .path()
                .join(format!("{crash_point}-{cursor_durable}"));
            process_crash::run_crash_child(
                child_test,
                &store_path,
                &format!("materialized_view.chain_event_chunk.{crash_point}"),
            )?;

            let reopened = open_store_at(&store_path)?;
            let spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints(
                &reopened,
                &[received_outpoint()],
            )?;
            let state = reopened.consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?;
            assert_eq!(spends.contains_key(&received_outpoint()), rows_durable);
            assert_eq!(
                reopened.last_materialized_height_ascending(
                    TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY
                )?,
                rows_durable.then_some(BlockHeight::new(106))
            );
            assert_eq!(
                state.map(|state| state.tip_height),
                rows_durable.then_some(BlockHeight::new(106))
            );
            assert_eq!(
                reopened.get_chain_event_cursor(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?,
                cursor_durable.then(|| CRASH_EVENT_CURSOR.to_vec())
            );
        }
        Ok(())
    }
}

/// Consumer-specific failure modes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentOutpointSpendConsumerError {
    /// Per-height delete index payload had a malformed byte length.
    #[error(
        "transparent_outpoint_spend_index entry for height {height} has {bytes} bytes, not a multiple of {OUTPOINT_KEY_LEN}"
    )]
    IndexLengthMismatch {
        /// Height whose persisted index was malformed.
        height: u32,
        /// Byte length actually persisted.
        bytes: usize,
    },
    /// A dispatch omitted one or more block contexts required by the consumer.
    #[error("transparent-outpoint-spend materialized-view checkpoint is missing its indexed tip")]
    IncompleteMaterializedViewCheckpoint,
    /// Materialized-view revision exhausted its integer domain.
    #[error("transparent-outpoint-spend materialized-view revision overflowed")]
    MaterializedViewRevisionOverflow,
}
