//! `TransparentAddressActivity` materialized-view consumer.
//!
//! Materializes per-address activity rows keyed by
//! `(address_script_hash, reverse_height, in_block_position)`. The
//! storage layout sorts the newest rows first under each address prefix
//! so the read path serves a single-address timeline as a forward range
//! scan with no post-fetch reversal.
//!
//! ## Field-presence rules (ADR-0018)
//!
//! `net_value_zat` is set only when the consumer resolved every
//! previous transparent output the transaction reads from this address. If any
//! prevout is unresolved (or the prevout-resolution capability is off
//! entirely), `net_value_zat` is `None` and `prevout_resolution_status`
//! disambiguates which case applies. The handler renders a chip rather
//! than guessing.
//!
//! ## Rewind correctness
//!
//! The primary key starts with the 32-byte address script hash, not the
//! height, so a height-prefixed range-delete on the primary CF would
//! either sweep too many heights (across address ranges) or none at all.
//! The consumer therefore maintains a per-height index in
//! [`TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY`] keyed by
//! ascending height; on revert it reads the persisted
//! `(address, in_block_position)` list and deletes each primary key it
//! actually wrote. The index entry is then deleted in the same batch.

use std::collections::HashMap;

use prost::Message as _;
use zinder_core::wire::{
    encode_address_script_hash, encode_height_key_ascending, encode_height_key_descending,
    encode_in_block_position,
};
use zinder_core::{
    BlockHeight, TransparentAddressScriptHash, TransparentOutPoint, TransparentSpendFact,
};
use zinder_proto::v1::explorer::{PrevoutResolutionStatus, TransparentAddressActivityRecord};

use crate::consumer::address_value_event::{
    AddressValueEventKind, address_value_events, transaction_ids_by_position,
};
use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};

/// Primary column family holding per-address activity rows.
pub const TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY: &str = "transparent_address_activity";

/// Per-height index column family. Key: 4-byte ascending block height.
/// Value: concatenated `(address_script_hash_32 | in_block_position_4)`
/// for every row the consumer wrote at that height.
pub const TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY: &str =
    "transparent_address_activity_index";

/// Column families the consumer needs registered before its first write.
pub const TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILIES: &[&str] = &[
    TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY,
];

/// Stable consumer name persisted in the SDK cursor table.
pub const TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transparent_address_activity");

/// On-disk schema declaration for the transparent-address-activity consumer.
pub const TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME,
        1,
        TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILIES,
    );

/// Length of one primary storage key: 32 address + 4 reverse-height + 4 position.
pub const TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN: usize = 40;

const ADDRESS_HASH_LEN: usize = 32;
const POSITION_LEN: usize = 4;
const INDEX_ENTRY_LEN: usize = ADDRESS_HASH_LEN + POSITION_LEN;

/// Materializes confirmed per-address activity rows.
#[derive(Default)]
pub struct TransparentAddressActivityConsumer;

impl TransparentAddressActivityConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns the primary storage key for `(address, height, position)`.
    #[must_use]
    pub fn key_for_row(
        address: TransparentAddressScriptHash,
        height: BlockHeight,
        in_block_position: u32,
    ) -> [u8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN] {
        let mut key = [0u8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN];
        key[0..32].copy_from_slice(&encode_address_script_hash(address));
        key[32..36].copy_from_slice(&encode_height_key_descending(height));
        key[36..40].copy_from_slice(&encode_in_block_position(in_block_position));
        key
    }

    /// Returns the descending key prefix for one address (32 bytes).
    #[must_use]
    pub const fn key_prefix_for_address(
        address: TransparentAddressScriptHash,
    ) -> [u8; ADDRESS_HASH_LEN] {
        encode_address_script_hash(address)
    }
}

impl BlockKeyedConsumer for TransparentAddressActivityConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let transparent_spends = block.transparent_spends();
        let mut rows = aggregate_address_rows(block, transparent_spends.as_deref())
            .into_iter()
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|((address, position), _record)| (address.as_bytes(), *position));
        let primary_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY)?;

        let mut index_payload: Vec<u8> = Vec::with_capacity(rows.len() * INDEX_ENTRY_LEN);
        for ((address, position), record) in rows {
            let key = Self::key_for_row(address, block.height, position);
            let mut payload = Vec::with_capacity(record.encoded_len());
            record.encode(&mut payload).map_err(|error| {
                TransparentAddressActivityConsumerError::Encode(error.to_string())
            })?;
            ctx.batch.put_cf(&primary_cf, key, payload);
            index_payload.extend_from_slice(&encode_address_script_hash(address));
            index_payload.extend_from_slice(&encode_in_block_position(position));
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
            .get_consumer(TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        if index_payload.len() % INDEX_ENTRY_LEN != 0 {
            return Err(Box::new(
                TransparentAddressActivityConsumerError::IndexLengthMismatch {
                    height: height.value(),
                    bytes: index_payload.len(),
                },
            ));
        }
        let primary_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY)?;
        for chunk in index_payload.chunks_exact(INDEX_ENTRY_LEN) {
            let address_bytes: [u8; ADDRESS_HASH_LEN] =
                chunk[0..ADDRESS_HASH_LEN].try_into().unwrap_or([0u8; 32]);
            let position_bytes: [u8; POSITION_LEN] = chunk[ADDRESS_HASH_LEN..]
                .try_into()
                .unwrap_or([0u8; POSITION_LEN]);
            let address = TransparentAddressScriptHash::from_bytes(address_bytes);
            let position = u32::from_be_bytes(position_bytes);
            ctx.batch
                .delete_cf(&primary_cf, Self::key_for_row(address, height, position));
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }
}

/// Per-row accumulator the apply path builds before encoding.
struct RowAccumulator {
    transaction_id: String,
    input_count: u32,
    output_count: u32,
    output_value_zat: i128,
    input_value_zat: i128,
    /// `true` when every prevout the transaction reads from this address
    /// was resolved through `prevouts.get(...)`. Stays `true` for
    /// output-only rows (no inputs to resolve).
    every_input_resolved: bool,
}

impl RowAccumulator {
    fn new(transaction_id: String) -> Self {
        Self {
            transaction_id,
            input_count: 0,
            output_count: 0,
            output_value_zat: 0,
            input_value_zat: 0,
            every_input_resolved: true,
        }
    }

    fn into_record(
        self,
        block_time_unix_seconds: i64,
        prevout_resolution_status: PrevoutResolutionStatus,
    ) -> TransparentAddressActivityRecord {
        let net_value_zat = match prevout_resolution_status {
            PrevoutResolutionStatus::Resolved if self.every_input_resolved => self
                .output_value_zat
                .checked_sub(self.input_value_zat)
                .and_then(|net| i64::try_from(net).ok()),
            PrevoutResolutionStatus::Resolved
            | PrevoutResolutionStatus::Partial
            | PrevoutResolutionStatus::Unavailable
            | PrevoutResolutionStatus::Unspecified => None,
        };
        TransparentAddressActivityRecord {
            transaction_id: self.transaction_id,
            block_time_unix_seconds,
            net_value_zat,
            input_count: self.input_count,
            output_count: self.output_count,
            prevout_resolution_status: prevout_resolution_status as i32,
        }
    }
}

fn aggregate_address_rows(
    block: &BlockCommitContext,
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
) -> HashMap<(TransparentAddressScriptHash, u32), TransparentAddressActivityRecord> {
    let mut accumulators: HashMap<(TransparentAddressScriptHash, u32), RowAccumulator> =
        HashMap::new();
    let block_time_unix_seconds = block.block_time_unix_seconds;
    let transaction_ids = transaction_ids_by_position(block);

    let value_events = address_value_events(block, transparent_spends);
    for event in &value_events {
        let key = (event.address_script_hash, event.in_block_position);
        let entry = accumulators.entry(key).or_insert_with(|| {
            let transaction_id = transaction_ids
                .get(event.in_block_position as usize)
                .cloned()
                .unwrap_or_default();
            RowAccumulator::new(transaction_id)
        });
        match event.kind {
            AddressValueEventKind::Received => {
                entry.output_count = entry.output_count.saturating_add(1);
                entry.output_value_zat = entry
                    .output_value_zat
                    .saturating_add(i128::from(event.value_zat));
            }
            AddressValueEventKind::Spent => {
                entry.input_count = entry.input_count.saturating_add(1);
                entry.input_value_zat = entry
                    .input_value_zat
                    .saturating_add(i128::from(event.value_zat));
            }
        }
    }

    // Determine per-transaction resolution status and downgrade affected rows.
    let resolution_status = determine_resolution_status(transparent_spends);
    flag_partial_rows(&mut accumulators, block, transparent_spends);

    accumulators
        .into_iter()
        .map(|(key, accumulator)| {
            let row_status = if accumulator.every_input_resolved {
                resolution_status
            } else {
                PrevoutResolutionStatus::Partial
            };
            let record = accumulator.into_record(block_time_unix_seconds, row_status);
            (key, record)
        })
        .collect()
}

/// Top-level status for the block: Resolved when prevout resolution was
/// online for the consumer, Unavailable when offline.
fn determine_resolution_status(
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
) -> PrevoutResolutionStatus {
    if transparent_spends.is_some() {
        PrevoutResolutionStatus::Resolved
    } else {
        PrevoutResolutionStatus::Unavailable
    }
}

/// Marks per-transaction rows as partial when an input is unresolved.
///
/// A transaction that touches address A on the output side and address B on
/// the input side, but where B's prevout is unresolved, leaves A's row
/// marked partial too (the consumer can't tell that A's output was the
/// "real" change without seeing the full picture).
fn flag_partial_rows(
    accumulators: &mut HashMap<(TransparentAddressScriptHash, u32), RowAccumulator>,
    block: &BlockCommitContext,
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
) {
    let Some(spends_by_outpoint) = transparent_spends else {
        for entry in accumulators.values_mut() {
            entry.every_input_resolved = false;
        }
        return;
    };
    for transaction in &block.transactions {
        if transaction.public_facts.is_coinbase {
            continue;
        }
        let in_block_position = transaction.location.tx_index_in_block;
        let mut transaction_complete = true;
        for input in &transaction.transparent_inputs {
            if !spends_by_outpoint.contains_key(&input.spent_outpoint) {
                transaction_complete = false;
                break;
            }
        }
        if transaction_complete {
            continue;
        }
        accumulators
            .iter_mut()
            .filter(|((_, row_position), _)| *row_position == in_block_position)
            .for_each(|(_, entry)| entry.every_input_resolved = false);
    }
}

/// Consumer-specific failure modes
/// [`TransparentAddressActivityConsumer`] can surface.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentAddressActivityConsumerError {
    /// Storage encoding of the materialized record failed.
    #[error("TransparentAddressActivityRecord prost encode failed: {0}")]
    Encode(String),
    /// Per-height index entry was not a clean multiple of 36 bytes.
    #[error(
        "transparent_address_activity_index entry for height {height} has {bytes} bytes, not a multiple of {INDEX_ENTRY_LEN}"
    )]
    IndexLengthMismatch {
        /// Height whose persisted index was malformed.
        height: u32,
        /// Byte length actually persisted.
        bytes: usize,
    },
}
