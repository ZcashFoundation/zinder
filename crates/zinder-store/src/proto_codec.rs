//! Protobuf encoders and decoders for store-owned chain-event values.

use prost::Message;
use thiserror::Error;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, decode_zinder_native_chain_name,
    encode_internal_transaction_id, encode_rpc_auth_digest_hex, encode_rpc_block_hash_hex,
    encode_rpc_transaction_id_hex, encode_zinder_native_chain_name,
};
use zinder_core::{
    ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactBlockArtifact, CompactChainMetadata, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, MempoolEntry, MempoolEvictionReason, TransactionId,
    TransparentMempoolOutput, TransparentMempoolSpend, TransparentOutPoint, TransparentSpendEntry,
    UnixTimestampMillis,
};
use zinder_proto::v1::wallet;

use crate::{
    ChainEpochCommitted, ChainEvent, ChainEventEnvelope, ChainEventStreamFamily,
    ChainRangeReverted, EventStreamStartPosition, MempoolEvent, MempoolEventEnvelope,
    StreamCursorTokenV1,
};

/// Error returned when a store chain event cannot be represented on the v1 wallet proto.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChainEventEncodeError {
    /// Store returned a chain-event variant unsupported by the wallet protocol.
    #[error("unsupported chain event: {event}")]
    UnsupportedChainEvent {
        /// Unsupported event description.
        event: &'static str,
    },
    /// A mempool eviction reason has no representation in the wallet protocol.
    #[error("unsupported mempool eviction reason")]
    UnsupportedMempoolEvictionReason,
}

/// Encodes a store chain-event envelope into the wallet protocol message.
#[allow(
    unreachable_patterns,
    reason = "This encoder fails closed when it receives a chain-event variant the current wire contract cannot represent."
)]
pub fn chain_event_envelope_message(
    event_envelope: &ChainEventEnvelope,
) -> Result<wallet::ChainEventEnvelope, ChainEventEncodeError> {
    let event = match &event_envelope.event {
        ChainEvent::ChainCommitted { committed } => {
            wallet::chain_event_envelope::Event::ChainCommitted(wallet::ChainCommitted {
                committed: Some(chain_epoch_committed_message(*committed)),
            })
        }
        ChainEvent::ChainReorged {
            reverted,
            committed,
        } => wallet::chain_event_envelope::Event::ChainReorged(wallet::ChainReorged {
            reverted: Some(chain_range_reverted_message(*reverted)),
            committed: Some(chain_epoch_committed_message(*committed)),
        }),
        _ => {
            return Err(ChainEventEncodeError::UnsupportedChainEvent {
                event: "unknown chain event variant",
            });
        }
    };

    Ok(wallet::ChainEventEnvelope {
        cursor: event_envelope.cursor.as_bytes().into(),
        event_sequence: event_envelope.event_sequence,
        chain_view: Some(chain_view_message(event_envelope.chain_epoch)),
        event: Some(event),
    })
}

fn chain_epoch_committed_message(committed: ChainEpochCommitted) -> wallet::ChainEpochCommitted {
    wallet::ChainEpochCommitted {
        chain_epoch: Some(chain_epoch_message(committed.chain_epoch)),
        start_height: committed.block_range.start.value(),
        end_height: committed.block_range.end.value(),
    }
}

fn chain_range_reverted_message(reverted: ChainRangeReverted) -> wallet::ChainRangeReverted {
    wallet::ChainRangeReverted {
        chain_epoch: Some(chain_epoch_message(reverted.chain_epoch)),
        start_height: reverted.block_range.start.value(),
        end_height: reverted.block_range.end.value(),
    }
}

/// Encodes a mempool entry into the wallet protocol message.
#[must_use]
pub fn mempool_entry_message(entry: &MempoolEntry) -> wallet::MempoolEntry {
    wallet::MempoolEntry {
        transaction_id: encode_rpc_transaction_id_hex(entry.transaction_id()),
        auth_digest: entry
            .auth_digest()
            .map(encode_rpc_auth_digest_hex)
            .unwrap_or_default(),
        raw_transaction_bytes: entry.raw_transaction_bytes().as_slice().into(),
        compact_transaction_data: Some(compact_transaction_data_message(
            entry.compact_transaction_data(),
        )),
        first_seen_unix_millis: entry.first_seen_unix_millis().value(),
        first_seen_chain_epoch: Some(chain_epoch_message(entry.first_seen_chain_epoch())),
        transparent_outputs: entry
            .transparent_outputs()
            .iter()
            .map(transparent_mempool_output_message)
            .collect(),
        transparent_spends: entry
            .transparent_spends()
            .iter()
            .map(transparent_mempool_spend_message)
            .collect(),
    }
}

/// Encodes one structured compact block for the native wallet protocol.
#[must_use]
pub fn compact_block_message(block: &CompactBlockArtifact) -> wallet::CompactBlock {
    wallet::CompactBlock {
        height: block.height().value(),
        block_hash: encode_rpc_block_hash_hex(block.block_hash()),
        previous_block_hash: encode_rpc_block_hash_hex(block.previous_block_hash()),
        time: block.time(),
        transactions: block
            .transactions()
            .iter()
            .map(compact_transaction_message)
            .collect(),
        chain_metadata: Some(compact_chain_metadata_message(block.chain_metadata())),
    }
}

/// Encodes one compact block for canonical storage.
#[must_use]
pub fn encode_compact_block_artifact(block: &CompactBlockArtifact) -> Vec<u8> {
    compact_block_message(block).encode_to_vec()
}

/// Decodes and validates one native compact block from canonical storage or the wire.
pub fn compact_block_from_message(
    message: wallet::CompactBlock,
) -> Result<CompactBlockArtifact, MempoolDecodeError> {
    zinder_proto::wire::compact_block_from_message(message).map_err(|error| {
        MempoolDecodeError::MalformedMessage {
            field: error.field(),
        }
    })
}

/// Decodes one canonical-storage compact block value.
pub fn decode_compact_block_artifact(
    encoded: &[u8],
) -> Result<CompactBlockArtifact, MempoolDecodeError> {
    let message = wallet::CompactBlock::decode(encoded).map_err(|_| {
        MempoolDecodeError::MalformedMessage {
            field: "compact_block",
        }
    })?;
    compact_block_from_message(message)
}

fn compact_chain_metadata_message(metadata: CompactChainMetadata) -> wallet::CompactChainMetadata {
    wallet::CompactChainMetadata {
        sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
        orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
        ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
    }
}

fn compact_transaction_message(transaction: &CompactTransaction) -> wallet::CompactTransaction {
    wallet::CompactTransaction {
        index: transaction.index,
        transaction_id: encode_internal_transaction_id(transaction.transaction_id).to_vec(),
        data: Some(compact_transaction_data_message(&transaction.data)),
    }
}

/// Encodes shared structured wallet scan data.
#[must_use]
pub(crate) fn compact_transaction_data_message(
    scan_data: &CompactTransactionData,
) -> wallet::CompactTransactionData {
    wallet::CompactTransactionData {
        fee_zat: scan_data.fee_zat,
        sapling_spends: scan_data
            .sapling_spends
            .iter()
            .map(|spend| wallet::CompactSaplingSpend {
                nullifier: spend.nullifier.to_vec(),
            })
            .collect(),
        sapling_outputs: scan_data
            .sapling_outputs
            .iter()
            .map(|output| wallet::CompactSaplingOutput {
                commitment: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        orchard_actions: scan_data
            .orchard_actions
            .iter()
            .map(compact_shielded_action_message)
            .collect(),
        ironwood_actions: scan_data
            .ironwood_actions
            .iter()
            .map(compact_shielded_action_message)
            .collect(),
        transparent_inputs: scan_data
            .transparent_inputs
            .iter()
            .map(|input| wallet::CompactTransparentInput {
                previous_transaction_id: encode_internal_transaction_id(
                    input.previous_transaction_id,
                )
                .to_vec(),
                previous_output_index: input.previous_output_index,
            })
            .collect(),
        transparent_outputs: scan_data
            .transparent_outputs
            .iter()
            .map(|output| wallet::CompactTransparentOutput {
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
    }
}

fn compact_shielded_action_message(
    action: &CompactShieldedAction,
) -> wallet::CompactShieldedAction {
    wallet::CompactShieldedAction {
        nullifier: action.nullifier.to_vec(),
        commitment: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

/// Encodes a mempool-event envelope into the wallet protocol message.
#[allow(
    unreachable_patterns,
    reason = "This encoder fails closed when it receives a mempool-event variant the current wire contract cannot represent."
)]
pub fn mempool_event_envelope_message(
    event_envelope: &MempoolEventEnvelope,
) -> Result<wallet::MempoolEventEnvelope, ChainEventEncodeError> {
    let event = match &event_envelope.event {
        MempoolEvent::Added { entry } => {
            wallet::mempool_event_envelope::Event::Added(wallet::MempoolAddedEvent {
                entry: Some(mempool_entry_message(entry)),
            })
        }
        MempoolEvent::Invalidated {
            transaction_id,
            reason,
        } => wallet::mempool_event_envelope::Event::Invalidated(wallet::MempoolInvalidatedEvent {
            transaction_id: encode_rpc_transaction_id_hex(*transaction_id),
            reason: mempool_eviction_reason_message(*reason)?.into(),
        }),
        MempoolEvent::Mined {
            transaction_id,
            mined_height,
            block_hash,
        } => wallet::mempool_event_envelope::Event::Mined(wallet::MempoolMinedEvent {
            transaction_id: encode_rpc_transaction_id_hex(*transaction_id),
            mined_height: mined_height.value(),
            block_hash: encode_rpc_block_hash_hex(*block_hash),
        }),
        _ => {
            return Err(ChainEventEncodeError::UnsupportedChainEvent {
                event: "unknown mempool event variant",
            });
        }
    };

    Ok(wallet::MempoolEventEnvelope {
        cursor: event_envelope.cursor.as_bytes().into(),
        event_sequence: event_envelope.event_sequence,
        source_observed_unix_millis: event_envelope.source_observed_unix_millis,
        event: Some(event),
    })
}

/// Encodes a [`TransparentOutPoint`] into the wallet protocol message.
///
/// Public so every wallet-plane outpoint-keyed RPC encodes through one
/// canonical helper instead of re-deriving `(transaction_id, output_index)`.
#[must_use]
pub fn outpoint_message(outpoint: &TransparentOutPoint) -> wallet::OutPoint {
    wallet::OutPoint {
        transaction_id: encode_rpc_transaction_id_hex(outpoint.transaction_id),
        output_index: outpoint.output_index,
    }
}

/// Encodes a [`TransparentSpendEntry`] into the wallet protocol message.
///
/// `ExplorerQuery.TransactionDetail` projects each spent output through this
/// one encoder so the outpoint, spending transaction id, input index, and
/// spending-block hash all route through the shared wire helpers.
#[must_use]
pub fn transparent_spend_message(entry: &TransparentSpendEntry) -> wallet::TransparentSpend {
    wallet::TransparentSpend {
        spent_outpoint: Some(outpoint_message(&entry.spent_outpoint)),
        spending_transaction_id: encode_rpc_transaction_id_hex(entry.spending_transaction_id),
        input_index: entry.input_index,
        spending_block: Some(block_tip_message(
            entry.spending_block_height,
            entry.spending_block_hash,
        )),
    }
}

/// Decodes a stream cursor from the bytes carried by a request message.
///
/// Empty bytes encode "no cursor" (start at the beginning of the stream);
/// every other value materializes a [`StreamCursorTokenV1`] without
/// validating its envelope. Validation happens at the seek site.
#[must_use]
pub fn stream_cursor_from_message_bytes(cursor_bytes: Vec<u8>) -> Option<StreamCursorTokenV1> {
    if cursor_bytes.is_empty() {
        None
    } else {
        Some(StreamCursorTokenV1::from_bytes(cursor_bytes))
    }
}

/// Decodes the chain-event stream family carried by a request message.
///
/// The wire field is the integer encoding of `wallet::ChainEventStreamFamily`.
/// Returns `None` for any unknown integer; callers map that to an
/// `INVALID_ARGUMENT` diagnostic at the transport boundary.
#[must_use]
pub fn chain_event_stream_family_from_message(family: i32) -> Option<ChainEventStreamFamily> {
    match wallet::ChainEventStreamFamily::try_from(family) {
        Ok(wallet::ChainEventStreamFamily::Visible) => Some(ChainEventStreamFamily::Visible),
        Ok(wallet::ChainEventStreamFamily::Settled) => Some(ChainEventStreamFamily::Settled),
        Err(_) => None,
    }
}

/// Decodes the wire `EventStreamStart` into the typed start position.
///
/// Returns `None` when the message or its `position` oneof is unset, and
/// when `after_cursor` carries empty bytes; callers map `None` to
/// `INVALID_ARGUMENT` at the transport boundary.
#[must_use]
pub fn event_stream_start_from_message(
    start: Option<wallet::EventStreamStart>,
) -> Option<EventStreamStartPosition> {
    match start?.position? {
        wallet::event_stream_start::Position::AfterCursor(cursor_bytes) => {
            stream_cursor_from_message_bytes(cursor_bytes)
                .map(EventStreamStartPosition::AfterCursor)
        }
        wallet::event_stream_start::Position::EarliestRetained(_) => {
            Some(EventStreamStartPosition::EarliestRetained)
        }
        wallet::event_stream_start::Position::LiveTail(_) => {
            Some(EventStreamStartPosition::LiveTail)
        }
    }
}

/// Encodes a typed start position into the wire `EventStreamStart` message.
#[must_use]
pub fn event_stream_start_message(start: &EventStreamStartPosition) -> wallet::EventStreamStart {
    let position = match start {
        EventStreamStartPosition::AfterCursor(cursor) => {
            wallet::event_stream_start::Position::AfterCursor(cursor.as_bytes().to_vec())
        }
        EventStreamStartPosition::EarliestRetained => {
            wallet::event_stream_start::Position::EarliestRetained(wallet::EarliestRetained {})
        }
        EventStreamStartPosition::LiveTail => {
            wallet::event_stream_start::Position::LiveTail(wallet::LiveTail {})
        }
    };
    wallet::EventStreamStart {
        position: Some(position),
    }
}

/// Encodes a [`TransparentMempoolOutput`] into the wallet protocol message.
fn transparent_mempool_output_message(
    transparent_output: &TransparentMempoolOutput,
) -> wallet::TransparentMempoolOutput {
    wallet::TransparentMempoolOutput {
        address_script_hash: transparent_output.address_script_hash.as_bytes().into(),
        script_pub_key: transparent_output.script_pub_key.clone(),
        outpoint: Some(outpoint_message(&transparent_output.outpoint)),
        value_zat: transparent_output.value_zat,
    }
}

/// Encodes a [`TransparentMempoolSpend`] into the wallet protocol message.
fn transparent_mempool_spend_message(
    transparent_spend: &TransparentMempoolSpend,
) -> wallet::TransparentMempoolSpend {
    wallet::TransparentMempoolSpend {
        spent_outpoint: Some(outpoint_message(&transparent_spend.spent_outpoint)),
        spending_transaction_id: encode_rpc_transaction_id_hex(
            transparent_spend.spending_transaction_id,
        ),
    }
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvictionReason is non-exhaustive; the encoder rejects future variants until the wallet protocol represents them."
)]
fn mempool_eviction_reason_message(
    reason: MempoolEvictionReason,
) -> Result<wallet::MempoolEvictionReason, ChainEventEncodeError> {
    match reason {
        MempoolEvictionReason::Conflict => Ok(wallet::MempoolEvictionReason::Conflict),
        MempoolEvictionReason::Expired => Ok(wallet::MempoolEvictionReason::Expired),
        MempoolEvictionReason::LowFee => Ok(wallet::MempoolEvictionReason::LowFee),
        MempoolEvictionReason::NodeRejected => Ok(wallet::MempoolEvictionReason::NodeRejected),
        MempoolEvictionReason::Unknown => Ok(wallet::MempoolEvictionReason::Unknown),
        _ => Err(ChainEventEncodeError::UnsupportedMempoolEvictionReason),
    }
}

/// Encodes one named chain tip (height plus RPC-form block hash) into the
/// wallet protocol message.
#[must_use]
pub fn block_tip_message(height: BlockHeight, hash: BlockHash) -> wallet::BlockTip {
    wallet::BlockTip {
        height: height.value(),
        hash: encode_rpc_block_hash_hex(hash),
    }
}

/// Encodes chain-epoch metadata into the wallet protocol message.
#[must_use]
pub fn chain_epoch_message(chain_epoch: ChainEpoch) -> wallet::ChainEpoch {
    wallet::ChainEpoch {
        chain_epoch_id: chain_epoch.id.value(),
        network_name: encode_zinder_native_chain_name(chain_epoch.network).to_owned(),
        artifact_schema_version: u32::from(chain_epoch.artifact_schema_version.value()),
        created_at_millis: chain_epoch.created_at.value(),
        visible_tip: Some(block_tip_message(
            chain_epoch.visible_tip_height,
            chain_epoch.visible_tip_hash,
        )),
        settled_tip: Some(block_tip_message(
            chain_epoch.settled_tip_height,
            chain_epoch.settled_tip_hash,
        )),
        sapling_commitment_tree_size: chain_epoch.tip_metadata.sapling_commitment_tree_size,
        orchard_commitment_tree_size: chain_epoch.tip_metadata.orchard_commitment_tree_size,
        ironwood_commitment_tree_size: chain_epoch.tip_metadata.ironwood_commitment_tree_size,
    }
}

/// Wraps chain-epoch metadata in the cross-plane [`wallet::ChainView`] envelope.
///
/// Leaves the materialized-view axes (`indexed_tip`, `upstream_tip`, `materialized_views`)
/// absent. The wallet plane owns the epoch; the explorer and ingest planes fill
/// the remaining axes from their own state.
#[must_use]
pub fn chain_view_message(chain_epoch: ChainEpoch) -> wallet::ChainView {
    wallet::ChainView {
        chain_epoch: Some(chain_epoch_message(chain_epoch)),
        indexed_tip: None,
        upstream_tip: None,
        materialized_views: None,
    }
}

/// Failure decoding a wallet-protocol message into canonical types.
///
/// Each variant carries a static `field` path so consumers can map the error
/// to their own domain (e.g. `IndexerError::MalformedResponse`,
/// `MempoolSurfaceError::Unavailable`, `tonic::Status::invalid_argument`)
/// without losing the diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum MempoolDecodeError {
    /// A protobuf message could not be decoded.
    #[error("{field} is not a valid wallet protocol message")]
    MalformedMessage {
        /// Static message path.
        field: &'static str,
    },
    /// A required protobuf field was absent.
    #[error("{field} is missing from the wallet protocol message")]
    MissingField {
        /// Static field path that was missing.
        field: &'static str,
    },
    /// A fixed-width byte field carried the wrong length.
    #[error("{field} expected {expected} bytes, got {actual}")]
    WrongLength {
        /// Static field path that carried the wrong length.
        field: &'static str,
        /// Required byte length.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// An RPC-form hex hash field failed to decode.
    #[error("{field}: invalid RPC-form hex hash: {reason}")]
    InvalidRpcHashHex {
        /// Static field path that carried the bad hex.
        field: &'static str,
        /// Human-readable description of the decode failure.
        reason: String,
    },
    /// An integer field overflowed the canonical type.
    #[error("{field} does not fit a {target}")]
    Overflow {
        /// Static field path that overflowed.
        field: &'static str,
        /// Target canonical type the value would not fit into.
        target: &'static str,
    },
    /// A network name was unknown.
    #[error("{field}: unknown network name {network_name}")]
    UnknownNetwork {
        /// Static field path that carried the unknown network name.
        field: &'static str,
        /// Observed network name.
        network_name: String,
    },
    /// A mempool eviction reason was unknown.
    #[error("{field}: unknown mempool eviction reason {encoded}")]
    UnknownEvictionReason {
        /// Static field path that carried the unknown encoded reason.
        field: &'static str,
        /// Encoded reason value.
        encoded: i32,
    },
}

impl MempoolDecodeError {
    /// Returns the static field path associated with this decode failure.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::MalformedMessage { field }
            | Self::MissingField { field }
            | Self::WrongLength { field, .. }
            | Self::InvalidRpcHashHex { field, .. }
            | Self::Overflow { field, .. }
            | Self::UnknownNetwork { field, .. }
            | Self::UnknownEvictionReason { field, .. } => field,
        }
    }
}

/// Decodes a wallet-protocol [`wallet::ChainEpoch`] into the canonical type.
#[allow(
    clippy::needless_pass_by_value,
    reason = "ChainEpoch fields are all small POD; taking by value keeps caller ergonomics symmetric with the message constructor and avoids a borrow-vs-clone choice at every callsite."
)]
pub fn chain_epoch_from_message(
    message: wallet::ChainEpoch,
) -> Result<ChainEpoch, MempoolDecodeError> {
    let network = decode_zinder_native_chain_name(&message.network_name)
        .ok()
        .ok_or_else(|| MempoolDecodeError::UnknownNetwork {
            field: "chain_epoch.network_name",
            network_name: message.network_name.clone(),
        })?;
    let artifact_schema_version = u16::try_from(message.artifact_schema_version).map_err(|_| {
        MempoolDecodeError::Overflow {
            field: "chain_epoch.artifact_schema_version",
            target: "u16",
        }
    })?;
    let visible_tip = message
        .visible_tip
        .ok_or(MempoolDecodeError::MissingField {
            field: "chain_epoch.visible_tip",
        })?;
    let settled_tip = message
        .settled_tip
        .ok_or(MempoolDecodeError::MissingField {
            field: "chain_epoch.settled_tip",
        })?;
    Ok(ChainEpoch {
        id: ChainEpochId::new(message.chain_epoch_id),
        network,
        visible_tip_height: BlockHeight::new(visible_tip.height),
        visible_tip_hash: block_hash_from_rpc_hex(
            "chain_epoch.visible_tip.hash",
            &visible_tip.hash,
        )?,
        settled_tip_height: BlockHeight::new(settled_tip.height),
        settled_tip_hash: block_hash_from_rpc_hex(
            "chain_epoch.settled_tip.hash",
            &settled_tip.hash,
        )?,
        artifact_schema_version: ArtifactSchemaVersion::new(artifact_schema_version),
        tip_metadata: ChainTipMetadata::new(
            message.sapling_commitment_tree_size,
            message.orchard_commitment_tree_size,
            message.ironwood_commitment_tree_size,
        ),
        created_at: UnixTimestampMillis::new(message.created_at_millis),
    })
}

/// Decodes a wallet-protocol [`wallet::MempoolEntry`] into the canonical type.
pub fn mempool_entry_from_message(
    message: wallet::MempoolEntry,
) -> Result<MempoolEntry, MempoolDecodeError> {
    zinder_proto::wire::mempool_entry_from_message(message).map_err(|error| {
        MempoolDecodeError::MalformedMessage {
            field: error.field(),
        }
    })
}

/// Decodes a wallet-protocol [`wallet::MempoolEventEnvelope`] into the canonical envelope.
pub fn mempool_event_envelope_from_message(
    message: wallet::MempoolEventEnvelope,
) -> Result<MempoolEventEnvelope, MempoolDecodeError> {
    let event_message = message.event.ok_or(MempoolDecodeError::MissingField {
        field: "mempool_event_envelope.event",
    })?;
    let event = match event_message {
        wallet::mempool_event_envelope::Event::Added(added) => {
            let entry_message = added.entry.ok_or(MempoolDecodeError::MissingField {
                field: "mempool_event_envelope.added.entry",
            })?;
            MempoolEvent::Added {
                entry: mempool_entry_from_message(entry_message)?,
            }
        }
        wallet::mempool_event_envelope::Event::Invalidated(invalidated) => {
            MempoolEvent::Invalidated {
                transaction_id: transaction_id_from_rpc_hex(
                    "mempool_event_envelope.invalidated.transaction_id",
                    &invalidated.transaction_id,
                )?,
                reason: mempool_eviction_reason_from_message(
                    "mempool_event_envelope.invalidated.reason",
                    invalidated.reason,
                )?,
            }
        }
        wallet::mempool_event_envelope::Event::Mined(mined) => MempoolEvent::Mined {
            transaction_id: transaction_id_from_rpc_hex(
                "mempool_event_envelope.mined.transaction_id",
                &mined.transaction_id,
            )?,
            mined_height: BlockHeight::new(mined.mined_height),
            block_hash: block_hash_from_rpc_hex(
                "mempool_event_envelope.mined.block_hash",
                &mined.block_hash,
            )?,
        },
    };
    Ok(MempoolEventEnvelope {
        cursor: StreamCursorTokenV1::from_bytes(message.cursor),
        event_sequence: message.event_sequence,
        source_observed_unix_millis: message.source_observed_unix_millis,
        event,
    })
}

fn mempool_eviction_reason_from_message(
    field: &'static str,
    encoded: i32,
) -> Result<MempoolEvictionReason, MempoolDecodeError> {
    match wallet::MempoolEvictionReason::try_from(encoded) {
        Ok(wallet::MempoolEvictionReason::Conflict) => Ok(MempoolEvictionReason::Conflict),
        Ok(wallet::MempoolEvictionReason::Expired) => Ok(MempoolEvictionReason::Expired),
        Ok(wallet::MempoolEvictionReason::LowFee) => Ok(MempoolEvictionReason::LowFee),
        Ok(wallet::MempoolEvictionReason::NodeRejected) => Ok(MempoolEvictionReason::NodeRejected),
        Ok(wallet::MempoolEvictionReason::Unknown) => Ok(MempoolEvictionReason::Unknown),
        Ok(wallet::MempoolEvictionReason::Unspecified) | Err(_) => {
            Err(MempoolDecodeError::UnknownEvictionReason { field, encoded })
        }
    }
}

fn transaction_id_from_rpc_hex(
    field: &'static str,
    rpc_hex: &str,
) -> Result<TransactionId, MempoolDecodeError> {
    decode_rpc_transaction_id_hex(rpc_hex).map_err(|error| MempoolDecodeError::InvalidRpcHashHex {
        field,
        reason: error.to_string(),
    })
}

fn block_hash_from_rpc_hex(
    field: &'static str,
    rpc_hex: &str,
) -> Result<BlockHash, MempoolDecodeError> {
    decode_rpc_block_hash_hex(rpc_hex).map_err(|error| MempoolDecodeError::InvalidRpcHashHex {
        field,
        reason: error.to_string(),
    })
}
