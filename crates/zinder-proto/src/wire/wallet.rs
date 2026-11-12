//! Native wallet protocol mappings for core domain values.

use prost::Message;
use thiserror::Error;
use zinder_core::wire::{
    decode_rpc_auth_digest_hex, decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex,
    decode_zinder_native_chain_name, encode_internal_transaction_id, encode_rpc_auth_digest_hex,
    encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex, encode_zinder_native_chain_name,
};
use zinder_core::{
    ArtifactSchemaVersion, AuthDigest, BlockHash, BlockHeight, BlockId, ChainEpoch, ChainEpochId,
    ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactSaplingSpend, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolObservation,
    RawTransactionBytes, TransactionId, TransparentMempoolOutput, TransparentMempoolSpend,
    TransparentOutPoint, TransparentOutpointSpendStatus, UnixTimestampMillis,
};

use crate::v1::wallet;

/// Failure decoding a native wallet-protocol message.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum WalletWireDecodeError {
    /// A protobuf message could not be decoded.
    #[error("{field} is not a valid wallet protocol message")]
    MalformedMessage {
        /// Static message path.
        field: &'static str,
    },
    /// A required protobuf field was absent.
    #[error("{field} is missing from the wallet protocol message")]
    MissingField {
        /// Static field path.
        field: &'static str,
    },
    /// A fixed-width byte field carried the wrong length.
    #[error("{field} expected {expected} bytes, got {actual}")]
    WrongLength {
        /// Static field path.
        field: &'static str,
        /// Required byte length.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// An RPC-form hex field failed to decode.
    #[error("{field}: invalid RPC-form hex value: {reason}")]
    InvalidRpcHex {
        /// Static field path.
        field: &'static str,
        /// Decode failure description.
        reason: String,
    },
    /// An integer field overflowed the canonical type.
    #[error("{field} does not fit a {target}")]
    Overflow {
        /// Static field path.
        field: &'static str,
        /// Canonical target type.
        target: &'static str,
    },
    /// A network name was unknown.
    #[error("{field}: unknown network name {network_name}")]
    UnknownNetwork {
        /// Static field path.
        field: &'static str,
        /// Unsupported wire value.
        network_name: String,
    },
    /// Derived transparent indexes contradict the structured scan data.
    #[error("mempool entry transparent indexes contradict compact transaction data")]
    InconsistentMempoolIndexes,
    /// Chain epoch tips violate canonical ordering or identity invariants.
    #[error("chain epoch has invalid visible and settled tips")]
    InvalidChainEpoch,
    /// Compact transaction indexes are not strictly increasing.
    #[error("compact block transaction indexes are not strictly increasing")]
    InvalidCompactTransactionOrder,
}

impl WalletWireDecodeError {
    /// Returns the static field path associated with this failure.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::MalformedMessage { field }
            | Self::MissingField { field }
            | Self::WrongLength { field, .. }
            | Self::InvalidRpcHex { field, .. }
            | Self::Overflow { field, .. }
            | Self::UnknownNetwork { field, .. } => field,
            Self::InconsistentMempoolIndexes => "mempool_entry.transparent_indexes",
            Self::InvalidChainEpoch => "chain_epoch",
            Self::InvalidCompactTransactionOrder => "compact_block.transactions.index",
        }
    }
}

/// Encodes one structured compact block.
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
            .map(|transaction| wallet::CompactTransaction {
                index: transaction.index,
                transaction_id: encode_internal_transaction_id(transaction.transaction_id).to_vec(),
                data: Some(compact_transaction_data_message(&transaction.data)),
            })
            .collect(),
        chain_metadata: Some(wallet::CompactChainMetadata {
            sapling_commitment_tree_size: block.chain_metadata().sapling_commitment_tree_size,
            orchard_commitment_tree_size: block.chain_metadata().orchard_commitment_tree_size,
            ironwood_commitment_tree_size: block.chain_metadata().ironwood_commitment_tree_size,
        }),
    }
}

/// Encodes one compact block as its public wire value.
#[must_use]
pub fn encode_compact_block(block: &CompactBlockArtifact) -> Vec<u8> {
    compact_block_message(block).encode_to_vec()
}

/// Decodes and validates one structured compact block.
pub fn compact_block_from_message(
    message: wallet::CompactBlock,
) -> Result<CompactBlockArtifact, WalletWireDecodeError> {
    let metadata = message
        .chain_metadata
        .ok_or(WalletWireDecodeError::MissingField {
            field: "compact_block.chain_metadata",
        })?;
    let transactions = message
        .transactions
        .into_iter()
        .map(|transaction| {
            Ok(CompactTransaction {
                index: transaction.index,
                transaction_id: TransactionId::from_bytes(fixed_bytes(
                    "compact_transaction.transaction_id",
                    transaction.transaction_id,
                )?),
                data: compact_transaction_data_from_message(transaction.data.ok_or(
                    WalletWireDecodeError::MissingField {
                        field: "compact_transaction.data",
                    },
                )?)?,
            })
        })
        .collect::<Result<Vec<_>, WalletWireDecodeError>>()?;
    let block_id = zinder_core::BlockId::new(
        BlockHeight::new(message.height),
        decode_block_hash("compact_block.block_hash", &message.block_hash)?,
    );
    CompactBlockArtifact::new(
        block_id,
        decode_block_hash(
            "compact_block.previous_block_hash",
            &message.previous_block_hash,
        )?,
        message.time,
        transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        },
    )
    .map_err(|_| WalletWireDecodeError::InvalidCompactTransactionOrder)
}

/// Decodes one encoded structured compact block.
pub fn decode_compact_block(encoded: &[u8]) -> Result<CompactBlockArtifact, WalletWireDecodeError> {
    compact_block_from_message(wallet::CompactBlock::decode(encoded).map_err(|_| {
        WalletWireDecodeError::MalformedMessage {
            field: "compact_block",
        }
    })?)
}

/// Encodes structured scan data shared by mined and mempool transactions.
#[must_use]
pub fn compact_transaction_data_message(
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
            .map(compact_action_message)
            .collect(),
        ironwood_actions: scan_data
            .ironwood_actions
            .iter()
            .map(compact_action_message)
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

/// Decodes and validates structured scan data.
pub fn compact_transaction_data_from_message(
    message: wallet::CompactTransactionData,
) -> Result<CompactTransactionData, WalletWireDecodeError> {
    Ok(CompactTransactionData {
        fee_zat: message.fee_zat,
        sapling_spends: message
            .sapling_spends
            .into_iter()
            .map(|spend| {
                Ok(CompactSaplingSpend {
                    nullifier: fixed_bytes("compact_sapling_spend.nullifier", spend.nullifier)?,
                })
            })
            .collect::<Result<Vec<_>, WalletWireDecodeError>>()?,
        sapling_outputs: message
            .sapling_outputs
            .into_iter()
            .map(|output| {
                Ok(CompactSaplingOutput {
                    commitment: fixed_bytes(
                        "compact_sapling_output.commitment",
                        output.commitment,
                    )?,
                    ephemeral_key: fixed_bytes(
                        "compact_sapling_output.ephemeral_key",
                        output.ephemeral_key,
                    )?,
                    ciphertext: fixed_bytes(
                        "compact_sapling_output.ciphertext",
                        output.ciphertext,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, WalletWireDecodeError>>()?,
        orchard_actions: compact_actions_from_messages(
            ActionFieldPaths {
                nullifier: "compact_transaction_data.orchard_actions.nullifier",
                commitment: "compact_transaction_data.orchard_actions.commitment",
                ephemeral_key: "compact_transaction_data.orchard_actions.ephemeral_key",
                ciphertext: "compact_transaction_data.orchard_actions.ciphertext",
            },
            message.orchard_actions,
        )?,
        ironwood_actions: compact_actions_from_messages(
            ActionFieldPaths {
                nullifier: "compact_transaction_data.ironwood_actions.nullifier",
                commitment: "compact_transaction_data.ironwood_actions.commitment",
                ephemeral_key: "compact_transaction_data.ironwood_actions.ephemeral_key",
                ciphertext: "compact_transaction_data.ironwood_actions.ciphertext",
            },
            message.ironwood_actions,
        )?,
        transparent_inputs: message
            .transparent_inputs
            .into_iter()
            .map(|input| {
                Ok(CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes(fixed_bytes(
                        "compact_transparent_input.previous_transaction_id",
                        input.previous_transaction_id,
                    )?),
                    previous_output_index: input.previous_output_index,
                })
            })
            .collect::<Result<Vec<_>, WalletWireDecodeError>>()?,
        transparent_outputs: message
            .transparent_outputs
            .into_iter()
            .map(|output| CompactTransparentOutput {
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key,
            })
            .collect(),
    })
}

/// Encodes a canonical mempool entry.
#[must_use]
pub fn mempool_entry_message(entry: &MempoolEntry) -> wallet::MempoolEntry {
    wallet::MempoolEntry {
        transaction_id: encode_rpc_transaction_id_hex(entry.transaction_id()),
        auth_digest: entry
            .auth_digest()
            .map(encode_rpc_auth_digest_hex)
            .unwrap_or_default(),
        raw_transaction_bytes: entry.raw_transaction_bytes().as_slice().to_vec(),
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

/// Decodes a canonical mempool entry and rejects contradictory derived indexes.
pub fn mempool_entry_from_message(
    message: wallet::MempoolEntry,
) -> Result<MempoolEntry, WalletWireDecodeError> {
    let transaction_id =
        decode_transaction_id("mempool_entry.transaction_id", &message.transaction_id)?;
    let auth_digest = if message.auth_digest.is_empty() {
        None
    } else {
        Some(decode_auth_digest(
            "mempool_entry.auth_digest",
            &message.auth_digest,
        )?)
    };
    let advertised_outputs = message.transparent_outputs.clone();
    let advertised_spends = message.transparent_spends.clone();
    let entry = MempoolEntry::new(
        transaction_id,
        auth_digest,
        RawTransactionBytes::new(message.raw_transaction_bytes),
        compact_transaction_data_from_message(message.compact_transaction_data.ok_or(
            WalletWireDecodeError::MissingField {
                field: "mempool_entry.compact_transaction_data",
            },
        )?)?,
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(message.first_seen_unix_millis),
            first_seen_chain_epoch: chain_epoch_from_message(
                message
                    .first_seen_chain_epoch
                    .ok_or(WalletWireDecodeError::MissingField {
                        field: "mempool_entry.first_seen_chain_epoch",
                    })?,
            )?,
        },
    )
    .map_err(|_| WalletWireDecodeError::Overflow {
        field: "mempool_entry.compact_transaction_data.transparent_outputs",
        target: "u32 output index",
    })?;
    if mempool_entry_message(&entry).transparent_outputs != advertised_outputs
        || mempool_entry_message(&entry).transparent_spends != advertised_spends
    {
        return Err(WalletWireDecodeError::InconsistentMempoolIndexes);
    }
    Ok(entry)
}

/// Encodes a chain epoch for wallet messages.
#[must_use]
pub fn chain_epoch_message(epoch: ChainEpoch) -> wallet::ChainEpoch {
    wallet::ChainEpoch {
        chain_epoch_id: epoch.id.value(),
        network_name: encode_zinder_native_chain_name(epoch.network).to_owned(),
        visible_tip: Some(wallet::BlockTip {
            height: epoch.visible_tip_height.value(),
            hash: encode_rpc_block_hash_hex(epoch.visible_tip_hash),
        }),
        settled_tip: Some(wallet::BlockTip {
            height: epoch.settled_tip_height.value(),
            hash: encode_rpc_block_hash_hex(epoch.settled_tip_hash),
        }),
        artifact_schema_version: u32::from(epoch.artifact_schema_version.value()),
        sapling_commitment_tree_size: epoch.tip_metadata.sapling_commitment_tree_size,
        orchard_commitment_tree_size: epoch.tip_metadata.orchard_commitment_tree_size,
        ironwood_commitment_tree_size: epoch.tip_metadata.ironwood_commitment_tree_size,
        created_at_millis: epoch.created_at.value(),
    }
}

/// Decodes a chain epoch from a wallet message.
pub fn chain_epoch_from_message(
    message: wallet::ChainEpoch,
) -> Result<ChainEpoch, WalletWireDecodeError> {
    let visible = message
        .visible_tip
        .ok_or(WalletWireDecodeError::MissingField {
            field: "chain_epoch.visible_tip",
        })?;
    let settled = message
        .settled_tip
        .ok_or(WalletWireDecodeError::MissingField {
            field: "chain_epoch.settled_tip",
        })?;
    let visible_tip_hash = decode_block_hash("chain_epoch.visible_tip.hash", &visible.hash)?;
    let settled_tip_hash = decode_block_hash("chain_epoch.settled_tip.hash", &settled.hash)?;
    if settled.height > visible.height
        || (settled.height == visible.height && settled_tip_hash != visible_tip_hash)
    {
        return Err(WalletWireDecodeError::InvalidChainEpoch);
    }
    Ok(ChainEpoch {
        id: ChainEpochId::new(message.chain_epoch_id),
        network: decode_zinder_native_chain_name(&message.network_name).map_err(|_| {
            WalletWireDecodeError::UnknownNetwork {
                field: "chain_epoch.network_name",
                network_name: message.network_name,
            }
        })?,
        visible_tip_height: BlockHeight::new(visible.height),
        visible_tip_hash,
        settled_tip_height: BlockHeight::new(settled.height),
        settled_tip_hash,
        artifact_schema_version: ArtifactSchemaVersion::new(
            u16::try_from(message.artifact_schema_version).map_err(|_| {
                WalletWireDecodeError::Overflow {
                    field: "chain_epoch.artifact_schema_version",
                    target: "u16",
                }
            })?,
        ),
        tip_metadata: ChainTipMetadata::new(
            message.sapling_commitment_tree_size,
            message.orchard_commitment_tree_size,
            message.ironwood_commitment_tree_size,
        ),
        created_at: UnixTimestampMillis::new(message.created_at_millis),
    })
}

fn compact_action_message(action: &CompactShieldedAction) -> wallet::CompactShieldedAction {
    wallet::CompactShieldedAction {
        nullifier: action.nullifier.to_vec(),
        commitment: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

/// Encodes a transparent outpoint for the native wallet protocol.
#[must_use]
pub fn outpoint_message(outpoint: &TransparentOutPoint) -> wallet::OutPoint {
    wallet::OutPoint {
        transaction_id: encode_rpc_transaction_id_hex(outpoint.transaction_id),
        output_index: outpoint.output_index,
    }
}

/// Decodes a transparent outpoint from the native wallet protocol.
///
/// `field` names the message path reported when the transaction id is not
/// RPC-form hex.
pub fn outpoint_from_message(
    field: &'static str,
    message: &wallet::OutPoint,
) -> Result<TransparentOutPoint, WalletWireDecodeError> {
    Ok(TransparentOutPoint::new(
        decode_transaction_id(field, &message.transaction_id)?,
        message.output_index,
    ))
}

/// Encodes one outpoint and its spend status as a native
/// `TransparentOutpointSpendStatus` response entry.
#[must_use]
pub fn transparent_outpoint_spend_status_entry_message(
    outpoint: &TransparentOutPoint,
    status: TransparentOutpointSpendStatus,
) -> wallet::TransparentOutpointSpendStatusEntry {
    use wallet::transparent_outpoint_spend_status_entry::Status;

    let status = match status {
        TransparentOutpointSpendStatus::Unspent => {
            Status::Unspent(wallet::TransparentOutpointUnspent {})
        }
        TransparentOutpointSpendStatus::SpentBy {
            transaction_id,
            input_index,
            block,
        } => Status::SpentBy(wallet::TransparentOutpointSpentBy {
            spending_transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            input_index,
            block: Some(wallet::BlockId {
                height: block.height.value(),
                block_hash: encode_rpc_block_hash_hex(block.hash),
            }),
        }),
        TransparentOutpointSpendStatus::NotFound => {
            Status::NotFound(wallet::TransparentOutpointNotFound {})
        }
    };
    wallet::TransparentOutpointSpendStatusEntry {
        outpoint: Some(outpoint_message(outpoint)),
        status: Some(status),
    }
}

/// Decodes one native `TransparentOutpointSpendStatus` response entry into
/// its outpoint and spend status.
pub fn transparent_outpoint_spend_status_entry_from_message(
    entry: &wallet::TransparentOutpointSpendStatusEntry,
) -> Result<(TransparentOutPoint, TransparentOutpointSpendStatus), WalletWireDecodeError> {
    use wallet::transparent_outpoint_spend_status_entry::Status;

    let outpoint = entry
        .outpoint
        .as_ref()
        .ok_or(WalletWireDecodeError::MissingField {
            field: "entries.outpoint",
        })?;
    let outpoint = outpoint_from_message("entries.outpoint.transaction_id", outpoint)?;
    let status = match entry
        .status
        .as_ref()
        .ok_or(WalletWireDecodeError::MissingField {
            field: "entries.status",
        })? {
        Status::Unspent(_) => TransparentOutpointSpendStatus::Unspent,
        Status::SpentBy(spent_by) => {
            let block = spent_by
                .block
                .as_ref()
                .ok_or(WalletWireDecodeError::MissingField {
                    field: "entries.spent_by.block",
                })?;
            TransparentOutpointSpendStatus::SpentBy {
                transaction_id: decode_transaction_id(
                    "entries.spent_by.spending_transaction_id",
                    &spent_by.spending_transaction_id,
                )?,
                input_index: spent_by.input_index,
                block: BlockId::new(
                    BlockHeight::new(block.height),
                    decode_block_hash("entries.spent_by.block.block_hash", &block.block_hash)?,
                ),
            }
        }
        Status::NotFound(_) => TransparentOutpointSpendStatus::NotFound,
    };
    Ok((outpoint, status))
}

fn transparent_mempool_output_message(
    output: &TransparentMempoolOutput,
) -> wallet::TransparentMempoolOutput {
    wallet::TransparentMempoolOutput {
        address_script_hash: output.address_script_hash.as_bytes().to_vec(),
        script_pub_key: output.script_pub_key.clone(),
        outpoint: Some(outpoint_message(&output.outpoint)),
        value_zat: output.value_zat,
    }
}

fn transparent_mempool_spend_message(
    spend: &TransparentMempoolSpend,
) -> wallet::TransparentMempoolSpend {
    wallet::TransparentMempoolSpend {
        spent_outpoint: Some(outpoint_message(&spend.spent_outpoint)),
        spending_transaction_id: encode_rpc_transaction_id_hex(spend.spending_transaction_id),
    }
}

fn compact_actions_from_messages(
    fields: ActionFieldPaths,
    actions: Vec<wallet::CompactShieldedAction>,
) -> Result<Vec<CompactShieldedAction>, WalletWireDecodeError> {
    actions
        .into_iter()
        .map(|action| {
            Ok(CompactShieldedAction {
                nullifier: fixed_bytes(fields.nullifier, action.nullifier)?,
                commitment: fixed_bytes(fields.commitment, action.commitment)?,
                ephemeral_key: fixed_bytes(fields.ephemeral_key, action.ephemeral_key)?,
                ciphertext: fixed_bytes(fields.ciphertext, action.ciphertext)?,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
struct ActionFieldPaths {
    nullifier: &'static str,
    commitment: &'static str,
    ephemeral_key: &'static str,
    ciphertext: &'static str,
}

fn fixed_bytes<const N: usize>(
    field: &'static str,
    bytes: Vec<u8>,
) -> Result<[u8; N], WalletWireDecodeError> {
    let actual = bytes.len();
    bytes
        .try_into()
        .map_err(|_| WalletWireDecodeError::WrongLength {
            field,
            expected: N,
            actual,
        })
}

fn decode_block_hash(
    field: &'static str,
    encoded: &str,
) -> Result<BlockHash, WalletWireDecodeError> {
    decode_rpc_block_hash_hex(encoded).map_err(|error| WalletWireDecodeError::InvalidRpcHex {
        field,
        reason: error.to_string(),
    })
}

fn decode_transaction_id(
    field: &'static str,
    encoded: &str,
) -> Result<TransactionId, WalletWireDecodeError> {
    decode_rpc_transaction_id_hex(encoded).map_err(|error| WalletWireDecodeError::InvalidRpcHex {
        field,
        reason: error.to_string(),
    })
}

fn decode_auth_digest(
    field: &'static str,
    encoded: &str,
) -> Result<AuthDigest, WalletWireDecodeError> {
    decode_rpc_auth_digest_hex(encoded).map_err(|error| WalletWireDecodeError::InvalidRpcHex {
        field,
        reason: error.to_string(),
    })
}
