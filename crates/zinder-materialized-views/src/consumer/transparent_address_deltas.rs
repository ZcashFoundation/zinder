//! `TransparentAddressDeltas` materialized-view consumer.
//!
//! Materializes one row per transparent-address value event keyed by
//! `(address_script_hash, ascending_height, in_block_position, kind, event_index)`.
//! The height segment is ascending, so a forward range scan serves a
//! single-address series oldest-first with no post-fetch reversal, matching
//! the zcashd `getaddressdeltas` order.
//!
//! ## Shared attribution
//!
//! The events come from
//! [`address_value_events`](crate::consumer::address_value_event::address_value_events),
//! the same per-event attribution
//! [`TransparentAddressActivityConsumer`](crate::consumer::transparent_address_activity)
//! folds into one net row per transaction. The delta surface persists the
//! events; the activity surface aggregates them, so net equals the sum of the
//! deltas over the same range.
//!
//! ## Resolution semantics
//!
//! Received-output events are always exact. Spend events carry
//! `spent_value_zat` from the canonical spend fact, so they need no prevout
//! re-resolution. A spend whose prevout is unresolved (or hydration is off)
//! produces no event rather than a wrong number; the per-page resolution
//! status is surfaced by the activity sibling for the same range.
//!
//! ## Rewind correctness
//!
//! The primary key starts with the address script hash, so a height-prefixed
//! range-delete cannot target one height across address ranges. The consumer
//! maintains a per-height index keyed by ascending height whose value lists
//! the `(address, in_block_position, kind, event_index)` tuples written at
//! that height; on revert it deletes each primary key it wrote, then the
//! index entry. The index is the schema's height undo index, so a store opened
//! with a reorg window keeps it only for that window's newest heights.

use prost::Message as _;
use zinder_core::wire::{
    encode_address_script_hash, encode_height_key_ascending, encode_in_block_position,
};
use zinder_core::{BlockHeight, TransparentAddressScriptHash};
use zinder_proto::v1::explorer::TransparentAddressDeltasRecord;

use crate::consumer::address_value_event::{address_value_events, transaction_ids_by_position};
use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};

/// Primary column family holding per-address delta rows.
pub const TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY: &str = "transparent_address_deltas";

/// Per-height index column family.
///
/// Key: 4-byte ascending block height. Value: concatenated
/// `(address_script_hash_32 | in_block_position_4 | kind_1 | event_index_4)`
/// for every row the consumer wrote at that height.
pub const TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY: &str = "transparent_address_deltas_index";

/// Column families the consumer needs registered before its first write.
pub const TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILIES: &[&str] = &[
    TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY,
];

/// Stable consumer name persisted in the SDK cursor table.
pub const TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transparent_address_deltas");

/// On-disk schema declaration for the transparent-address-deltas consumer.
pub const TRANSPARENT_ADDRESS_DELTAS_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME,
        1,
        TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILIES,
    )
    .with_height_undo_index(TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY);

const ADDRESS_HASH_LEN: usize = 32;
const HEIGHT_LEN: usize = 4;
const POSITION_LEN: usize = 4;
const KIND_LEN: usize = 1;
const EVENT_INDEX_LEN: usize = 4;

/// Length of one primary storage key:
/// 32 address + 4 ascending-height + 4 position + 1 kind + 4 event-index.
pub const TRANSPARENT_ADDRESS_DELTAS_KEY_LEN: usize =
    ADDRESS_HASH_LEN + HEIGHT_LEN + POSITION_LEN + KIND_LEN + EVENT_INDEX_LEN;

const INDEX_ENTRY_LEN: usize = ADDRESS_HASH_LEN + POSITION_LEN + KIND_LEN + EVENT_INDEX_LEN;

/// Materializes confirmed per-address value events.
#[derive(Default)]
pub struct TransparentAddressDeltasConsumer;

impl TransparentAddressDeltasConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns the primary storage key for one value event.
    #[must_use]
    pub fn key_for_event(
        address: TransparentAddressScriptHash,
        height: BlockHeight,
        in_block_position: u32,
        kind_byte: u8,
        event_index: u32,
    ) -> [u8; TRANSPARENT_ADDRESS_DELTAS_KEY_LEN] {
        let mut key = [0u8; TRANSPARENT_ADDRESS_DELTAS_KEY_LEN];
        let position_end = ADDRESS_HASH_LEN + HEIGHT_LEN + POSITION_LEN;
        let kind_end = position_end + KIND_LEN;
        key[0..ADDRESS_HASH_LEN].copy_from_slice(&encode_address_script_hash(address));
        key[ADDRESS_HASH_LEN..ADDRESS_HASH_LEN + HEIGHT_LEN]
            .copy_from_slice(&encode_height_key_ascending(height));
        key[ADDRESS_HASH_LEN + HEIGHT_LEN..position_end]
            .copy_from_slice(&encode_in_block_position(in_block_position));
        key[position_end] = kind_byte;
        key[kind_end..].copy_from_slice(&encode_in_block_position(event_index));
        key
    }

    /// Returns the address prefix (32 bytes) shared by every row for one
    /// address.
    #[must_use]
    pub const fn key_prefix_for_address(
        address: TransparentAddressScriptHash,
    ) -> [u8; ADDRESS_HASH_LEN] {
        encode_address_script_hash(address)
    }
}

impl BlockKeyedConsumer for TransparentAddressDeltasConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let transparent_spends = block.transparent_spends();
        let value_events = address_value_events(block, transparent_spends.as_deref());
        let primary_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY)?;

        let transaction_ids = transaction_ids_by_position(block);
        let mut index_payload: Vec<u8> = Vec::with_capacity(value_events.len() * INDEX_ENTRY_LEN);
        for event in &value_events {
            let Some(value_zat) = event.signed_value_zat() else {
                return Err(Box::new(
                    TransparentAddressDeltasConsumerError::ValueWidth {
                        height: block.height.value(),
                        value_zat: event.value_zat,
                    },
                ));
            };
            let kind_byte = event.kind.storage_byte();
            let key = Self::key_for_event(
                event.address_script_hash,
                block.height,
                event.in_block_position,
                kind_byte,
                event.event_index,
            );
            let record = TransparentAddressDeltasRecord {
                transaction_id: transaction_ids
                    .get(event.in_block_position as usize)
                    .cloned()
                    .unwrap_or_default(),
                block_time_unix_seconds: block.block_time_unix_seconds,
                value_zat,
            };
            let mut payload = Vec::with_capacity(record.encoded_len());
            record.encode(&mut payload).map_err(|error| {
                TransparentAddressDeltasConsumerError::Encode(error.to_string())
            })?;
            ctx.batch.put_cf(&primary_cf, key, payload);
            index_payload.extend_from_slice(&encode_address_script_hash(event.address_script_hash));
            index_payload.extend_from_slice(&encode_in_block_position(event.in_block_position));
            index_payload.push(kind_byte);
            index_payload.extend_from_slice(&encode_in_block_position(event.event_index));
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
            .get_consumer(TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        if index_payload.len() % INDEX_ENTRY_LEN != 0 {
            return Err(Box::new(
                TransparentAddressDeltasConsumerError::IndexLengthMismatch {
                    height: height.value(),
                    bytes: index_payload.len(),
                },
            ));
        }
        let primary_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY)?;
        for chunk in index_payload.chunks_exact(INDEX_ENTRY_LEN) {
            let address_bytes: [u8; ADDRESS_HASH_LEN] =
                chunk[0..ADDRESS_HASH_LEN].try_into().unwrap_or([0u8; 32]);
            let position_bytes: [u8; POSITION_LEN] = chunk
                [ADDRESS_HASH_LEN..ADDRESS_HASH_LEN + POSITION_LEN]
                .try_into()
                .unwrap_or([0u8; POSITION_LEN]);
            let kind_byte = chunk[ADDRESS_HASH_LEN + POSITION_LEN];
            let event_index_bytes: [u8; EVENT_INDEX_LEN] = chunk
                [ADDRESS_HASH_LEN + POSITION_LEN + KIND_LEN..]
                .try_into()
                .unwrap_or([0u8; EVENT_INDEX_LEN]);
            let address = TransparentAddressScriptHash::from_bytes(address_bytes);
            let position = u32::from_be_bytes(position_bytes);
            let event_index = u32::from_be_bytes(event_index_bytes);
            ctx.batch.delete_cf(
                &primary_cf,
                Self::key_for_event(address, height, position, kind_byte, event_index),
            );
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }
}

/// Consumer-specific failure modes [`TransparentAddressDeltasConsumer`] can
/// surface.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentAddressDeltasConsumerError {
    /// Storage encoding of the materialized record failed.
    #[error("TransparentAddressDeltasRecord prost encode failed: {0}")]
    Encode(String),
    /// A value magnitude did not fit the signed 64-bit wire width.
    #[error(
        "transparent delta value {value_zat} at height {height} exceeds the signed 64-bit range"
    )]
    ValueWidth {
        /// Height of the offending event.
        height: u32,
        /// The magnitude that overflowed.
        value_zat: u64,
    },
    /// Per-height index entry was not a clean multiple of the entry length.
    #[error(
        "transparent_address_deltas_index entry for height {height} has {bytes} bytes, not a multiple of {INDEX_ENTRY_LEN}"
    )]
    IndexLengthMismatch {
        /// Height whose persisted index was malformed.
        height: u32,
        /// Byte length actually persisted.
        bytes: usize,
    },
}
