//! Per-event transparent-address value attribution shared across consumers.
//!
//! A transparent address's signed value movements decompose into two event
//! kinds the canonical store already records:
//!
//! - A [`AddressValueEventKind::Received`] event for every transparent output
//!   paying the address: `+value_zat` at `(txid, output_index, height)`.
//! - A [`AddressValueEventKind::Spent`] event for every resolved transparent
//!   input spending one of the address's outputs: `-spent_value_zat` at
//!   `(txid, input_index, height)`.
//!
//! [`address_value_events`] is the one place this attribution is computed.
//! [`TransparentAddressDeltasConsumer`](super::transparent_address_deltas)
//! persists each event; [`TransparentAddressActivityConsumer`](super::transparent_address_activity)
//! folds the events for one `(address, transaction)` into a single net row, so
//! the net activity is an aggregation over the same events the delta surface
//! emits.

use std::collections::HashMap;

use zinder_core::wire::{
    WireDecodeError, decode_internal_transaction_id, decode_rpc_transaction_id_hex,
    encode_internal_transaction_id,
};
use zinder_core::{
    TransactionId, TransparentAddressScriptHash, TransparentOutPoint, TransparentSpendFact,
};
use zinder_proto::wire::{TRANSPARENT_DELTA_KIND_RECEIVED_BYTE, TRANSPARENT_DELTA_KIND_SPENT_BYTE};

use crate::consumer::BlockCommitContext;

/// Whether a value event credits the address (a received output) or debits it
/// (a spent prevout).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddressValueEventKind {
    /// A transparent output paid the address.
    Received,
    /// A transparent input spent one of the address's prior outputs.
    Spent,
}

impl AddressValueEventKind {
    /// Returns the one-byte storage discriminant ordering this kind in the
    /// per-event key. `Received` sorts before `Spent` at the same position.
    #[must_use]
    pub(crate) const fn storage_byte(self) -> u8 {
        match self {
            Self::Received => TRANSPARENT_DELTA_KIND_RECEIVED_BYTE,
            Self::Spent => TRANSPARENT_DELTA_KIND_SPENT_BYTE,
        }
    }
}

/// One signed value movement attributed to a transparent address.
///
/// `value_zat` is always the unsigned magnitude; the sign is carried by
/// [`AddressValueEvent::kind`]. Folding a set of events for one address
/// yields `sum(Received) - sum(Spent)`, which is exactly the net the activity
/// consumer materializes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AddressValueEvent {
    /// Address the event credits or debits.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Block-local index of the transaction producing the event.
    pub in_block_position: u32,
    /// Output index for a receive, input index for a spend.
    pub event_index: u32,
    /// Unsigned value magnitude in zatoshis.
    pub value_zat: u64,
    /// Whether the event credits or debits the address.
    pub kind: AddressValueEventKind,
}

impl AddressValueEvent {
    /// Returns the signed value the event moved for the address: positive for
    /// a received output, negative for a spent prevout. `None` when the
    /// magnitude does not fit a signed 64-bit width.
    #[must_use]
    pub(crate) fn signed_value_zat(&self) -> Option<i64> {
        let magnitude = i64::try_from(self.value_zat).ok()?;
        match self.kind {
            AddressValueEventKind::Received => Some(magnitude),
            AddressValueEventKind::Spent => magnitude.checked_neg(),
        }
    }
}

/// Decomposes one block's transparent activity into per-address value events.
///
/// Returns the events in canonical `(in_block_position, kind, event_index)`
/// traversal order. Output events are always exact. Spend events are emitted
/// only for inputs whose prevout resolved through `transparent_spends`; an
/// unresolved input (or hydration disabled entirely) produces no event rather
/// than a wrong number. Coinbase inputs are never resolvable and are skipped.
#[must_use]
pub(crate) fn address_value_events(
    block: &BlockCommitContext,
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
) -> Vec<AddressValueEvent> {
    let mut events = Vec::new();

    for transaction in &block.transactions {
        let in_block_position = transaction.location.tx_index_in_block;

        for output in &transaction.transparent_outputs {
            events.push(AddressValueEvent {
                address_script_hash: output.address_script_hash,
                in_block_position,
                event_index: output.output_index,
                value_zat: output.value_zat,
                kind: AddressValueEventKind::Received,
            });
        }

        if transaction.public_facts.is_coinbase {
            continue;
        }

        let Some(spends_by_outpoint) = transparent_spends else {
            continue;
        };
        for input in &transaction.transparent_inputs {
            let Some(spend) = spends_by_outpoint.get(&input.spent_outpoint) else {
                continue;
            };
            events.push(AddressValueEvent {
                address_script_hash: spend.spent_address_script_hash,
                in_block_position,
                event_index: input.input_index,
                value_zat: spend.spent_value_zat,
                kind: AddressValueEventKind::Spent,
            });
        }
    }

    events
}

/// Returns each transaction's id as 32 internal-order bytes, indexed by the
/// transaction's position in the block.
///
/// Delta and activity rows store this form rather than the 64-character RPC
/// hex form, which takes 32 more bytes per row and compresses poorly.
pub(crate) fn internal_transaction_ids_by_position(block: &BlockCommitContext) -> Vec<Vec<u8>> {
    let mut transaction_ids = vec![Vec::new(); block.transactions.len()];
    for transaction in &block.transactions {
        if let Some(slot) = transaction_ids.get_mut(transaction.location.tx_index_in_block as usize)
        {
            *slot = encode_internal_transaction_id(transaction.location.transaction_id).to_vec();
        }
    }
    transaction_ids
}

/// Decodes the transaction id a delta or activity row stores.
///
/// A row written by this release carries 32 internal-order bytes. A row an
/// earlier release wrote carries only the RPC hex form, which still decodes.
pub(crate) fn decode_stored_transaction_id(
    internal_transaction_id: &[u8],
    rpc_transaction_id_hex: &str,
) -> Result<TransactionId, WireDecodeError> {
    if internal_transaction_id.is_empty() {
        decode_rpc_transaction_id_hex(rpc_transaction_id_hex)
    } else {
        decode_internal_transaction_id(internal_transaction_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use zinder_core::{
        BlockHash, BlockHeight, LockTime, PrivacyShape, TransactionComponentCounts,
        TransactionFactsArtifact, TransactionId, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
        TransparentOutPoint, TransparentOutputFact, TransparentSpendFact,
    };

    use super::{AddressValueEvent, AddressValueEventKind, address_value_events};
    use crate::consumer::block_commit_context::{
        BlockCommitContext, BlockCommitInput, TransparentSpendFacts,
    };

    const ADDRESS_A: TransparentAddressScriptHash =
        TransparentAddressScriptHash::from_bytes([7; 32]);
    const ADDRESS_B: TransparentAddressScriptHash =
        TransparentAddressScriptHash::from_bytes([9; 32]);

    fn transaction_id(seed: u8) -> TransactionId {
        TransactionId::from_bytes([seed; 32])
    }

    fn public_facts(seed: u8, is_coinbase: bool) -> TransactionPublicFacts {
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
            is_coinbase,
            unsupported_sections: Vec::new(),
        }
    }

    fn block_with(transactions: Vec<TransactionFactsArtifact>) -> BlockCommitContext {
        let payload = BlockCommitInput {
            height: BlockHeight::new(100),
            block_hash: BlockHash::from_bytes([1; 32]),
            previous_block_hash: BlockHash::from_bytes([0; 32]),
            block_time_unix_seconds: 1_700_000_000,
            block_size_bytes: 0,
            transactions,
            final_note_commitment_roots: None,
        };
        BlockCommitContext::new(payload, TransparentSpendFacts::Offline)
    }

    #[test]
    fn received_output_yields_positive_event_at_output_index() {
        let location = TransactionLocation::new(
            transaction_id(1),
            BlockHeight::new(100),
            BlockHash::from_bytes([1; 32]),
            1,
        );
        let transaction = TransactionFactsArtifact::new(location, public_facts(1, false))
            .with_transparent_facts(
                Vec::new(),
                vec![TransparentOutputFact::new(
                    2,
                    5_000,
                    b"script".to_vec(),
                    ADDRESS_A,
                )],
            );
        let block = block_with(vec![transaction]);

        let events = address_value_events(&block, None);

        assert_eq!(events.len(), 1);
        let event = events[0];
        assert_eq!(event.address_script_hash, ADDRESS_A);
        assert_eq!(event.event_index, 2);
        assert_eq!(event.kind, AddressValueEventKind::Received);
        assert_eq!(event.signed_value_zat(), Some(5_000));
    }

    #[test]
    fn resolved_spend_yields_negative_event_at_input_index() {
        let spent_outpoint = TransparentOutPoint::new(transaction_id(8), 0);
        let location = TransactionLocation::new(
            transaction_id(2),
            BlockHeight::new(100),
            BlockHash::from_bytes([1; 32]),
            1,
        );
        let transaction = TransactionFactsArtifact::new(location, public_facts(2, false))
            .with_transparent_facts(
                vec![TransparentInputFact::new(3, spent_outpoint)],
                Vec::new(),
            );
        let block = block_with(vec![transaction]);

        let mut spends = HashMap::new();
        spends.insert(
            spent_outpoint,
            TransparentSpendFact::new(
                spent_outpoint,
                3,
                transaction_id(2),
                1,
                BlockHeight::new(100),
                BlockHash::from_bytes([1; 32]),
                4_000,
                ADDRESS_A,
                BlockHeight::new(50),
                BlockHash::from_bytes([2; 32]),
            ),
        );

        let events = address_value_events(&block, Some(&spends));

        assert_eq!(events.len(), 1);
        let event = events[0];
        assert_eq!(event.address_script_hash, ADDRESS_A);
        assert_eq!(event.event_index, 3);
        assert_eq!(event.kind, AddressValueEventKind::Spent);
        assert_eq!(event.signed_value_zat(), Some(-4_000));
    }

    #[test]
    fn unresolved_spend_is_omitted_not_misvalued() {
        let spent_outpoint = TransparentOutPoint::new(transaction_id(8), 0);
        let location = TransactionLocation::new(
            transaction_id(2),
            BlockHeight::new(100),
            BlockHash::from_bytes([1; 32]),
            1,
        );
        let transaction = TransactionFactsArtifact::new(location, public_facts(2, false))
            .with_transparent_facts(
                vec![TransparentInputFact::new(0, spent_outpoint)],
                Vec::new(),
            );
        let block = block_with(vec![transaction]);

        let events = address_value_events(&block, Some(&HashMap::new()));

        assert!(events.is_empty());
    }

    #[test]
    fn net_per_address_equals_sum_of_signed_deltas() {
        let received_outpoint = TransparentOutPoint::new(transaction_id(20), 0);
        let location = TransactionLocation::new(
            transaction_id(3),
            BlockHeight::new(100),
            BlockHash::from_bytes([1; 32]),
            2,
        );
        let transaction = TransactionFactsArtifact::new(location, public_facts(3, false))
            .with_transparent_facts(
                vec![TransparentInputFact::new(0, received_outpoint)],
                vec![
                    TransparentOutputFact::new(0, 9_000, b"a".to_vec(), ADDRESS_A),
                    TransparentOutputFact::new(1, 1_000, b"b".to_vec(), ADDRESS_B),
                ],
            );
        let block = block_with(vec![transaction]);

        let mut spends = HashMap::new();
        spends.insert(
            received_outpoint,
            TransparentSpendFact::new(
                received_outpoint,
                0,
                transaction_id(3),
                2,
                BlockHeight::new(100),
                BlockHash::from_bytes([1; 32]),
                6_000,
                ADDRESS_A,
                BlockHeight::new(40),
                BlockHash::from_bytes([3; 32]),
            ),
        );

        let events = address_value_events(&block, Some(&spends));
        let net_a: i64 = events
            .iter()
            .filter(|event| event.address_script_hash == ADDRESS_A)
            .filter_map(AddressValueEvent::signed_value_zat)
            .sum();
        let net_b: i64 = events
            .iter()
            .filter(|event| event.address_script_hash == ADDRESS_B)
            .filter_map(AddressValueEvent::signed_value_zat)
            .sum();

        assert_eq!(net_a, 9_000 - 6_000);
        assert_eq!(net_b, 1_000);
    }
}
