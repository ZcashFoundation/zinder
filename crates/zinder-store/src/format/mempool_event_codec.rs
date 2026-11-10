//! Protobuf codec for persisted mempool event envelopes.
//!
//! A mempool entry record embeds its first-seen chain epoch and its structured
//! compact transaction data, so this module also owns those sub-record codecs.

use prost::Message;
use zinder_core::{
    ArtifactSchemaVersion, AuthDigest, BlockHash, BlockHeight, ChainEpoch, ChainEpochId,
    ChainTipMetadata, CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, MempoolEntry,
    MempoolEvictionReason, Network, RawTransactionBytes, TransactionId, UnixTimestampMillis,
};

use crate::{ArtifactFamily, MempoolEvent, MempoolEventEnvelope, StoreError, StreamCursorTokenV1};

use super::StoreKey;

pub(crate) fn encode_mempool_event_envelope(
    event_envelope: &MempoolEventEnvelope,
) -> Result<Vec<u8>, StoreError> {
    Ok(MempoolEventEnvelopeRecord {
        event_sequence: event_envelope.event_sequence,
        source_observed_unix_millis: event_envelope.source_observed_unix_millis,
        event: Some(mempool_event_record(&event_envelope.event)?),
    }
    .encode_to_vec())
}

pub(crate) fn decode_mempool_event_envelope(
    key: &StoreKey,
    record_bytes: &[u8],
    network: Network,
    cursor_auth_key: [u8; 32],
) -> Result<MempoolEventEnvelope, StoreError> {
    let record = MempoolEventEnvelopeRecord::decode(record_bytes).map_err(|_| {
        StoreError::ArtifactCorrupt {
            family: ArtifactFamily::MempoolEvent,
            key: key.clone().into(),
            reason: "mempool event envelope record is not valid protobuf",
        }
    })?;
    let event_record = record.event.ok_or(StoreError::ArtifactCorrupt {
        family: ArtifactFamily::MempoolEvent,
        key: key.clone().into(),
        reason: "mempool event envelope is missing event",
    })?;
    let event = decode_mempool_event_record(key, event_record)?;
    let cursor = StreamCursorTokenV1::mempool_event(
        network,
        record.event_sequence,
        event.transaction_id(),
        cursor_auth_key,
    )
    .map_err(|_| StoreError::ArtifactCorrupt {
        family: ArtifactFamily::MempoolEvent,
        key: key.clone().into(),
        reason: "mempool event cursor could not be reconstructed",
    })?;

    Ok(MempoolEventEnvelope {
        cursor,
        event_sequence: record.event_sequence,
        source_observed_unix_millis: record.source_observed_unix_millis,
        event,
    })
}

/// Kind discriminator of a persisted mempool event record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MempoolEventKind {
    Added,
    Invalidated,
    Mined,
}

impl MempoolEventKind {
    const fn from_kind(kind: u32) -> Option<Self> {
        match kind {
            MEMPOOL_EVENT_KIND_ADDED => Some(Self::Added),
            MEMPOOL_EVENT_KIND_INVALIDATED => Some(Self::Invalidated),
            MEMPOOL_EVENT_KIND_MINED => Some(Self::Mined),
            _ => None,
        }
    }
}

fn mempool_event_record(event: &MempoolEvent) -> Result<MempoolEventRecord, StoreError> {
    Ok(match event {
        MempoolEvent::Added { entry } => MempoolEventRecord {
            event_kind: MEMPOOL_EVENT_KIND_ADDED,
            added: Some(mempool_entry_record(entry)),
            invalidated: None,
            mined: None,
        },
        MempoolEvent::Invalidated {
            transaction_id,
            reason,
        } => MempoolEventRecord {
            event_kind: MEMPOOL_EVENT_KIND_INVALIDATED,
            added: None,
            invalidated: Some(MempoolInvalidatedRecord {
                transaction_id: transaction_id.as_bytes().to_vec(),
                reason_id: mempool_eviction_reason_id(*reason)?,
            }),
            mined: None,
        },
        MempoolEvent::Mined {
            transaction_id,
            mined_height,
            block_hash,
        } => MempoolEventRecord {
            event_kind: MEMPOOL_EVENT_KIND_MINED,
            added: None,
            invalidated: None,
            mined: Some(MempoolMinedRecord {
                transaction_id: transaction_id.as_bytes().to_vec(),
                mined_height: mined_height.value(),
                block_hash: block_hash.as_bytes().to_vec(),
            }),
        },
        // The Rust enum is `#[non_exhaustive]`; future variants force a
        // build error in this match because the encoder is not behind
        // `unreachable_patterns`.
    })
}

fn decode_mempool_event_record(
    key: &StoreKey,
    record: MempoolEventRecord,
) -> Result<MempoolEvent, StoreError> {
    match MempoolEventKind::from_kind(record.event_kind).ok_or(StoreError::ArtifactCorrupt {
        family: ArtifactFamily::MempoolEvent,
        key: key.clone().into(),
        reason: "mempool event kind is unknown",
    })? {
        MempoolEventKind::Added => {
            let entry_record = record.added.ok_or(StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "added mempool event is missing entry",
            })?;
            let entry = decode_mempool_entry_record(key, entry_record)?;
            Ok(MempoolEvent::Added { entry })
        }
        MempoolEventKind::Invalidated => {
            let invalidated = record.invalidated.ok_or(StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "invalidated mempool event is missing payload",
            })?;
            Ok(MempoolEvent::Invalidated {
                transaction_id: decode_transaction_id_for_family(
                    ArtifactFamily::MempoolEvent,
                    key,
                    &invalidated.transaction_id,
                )?,
                reason: mempool_eviction_reason_from_id(invalidated.reason_id).ok_or(
                    StoreError::ArtifactCorrupt {
                        family: ArtifactFamily::MempoolEvent,
                        key: key.clone().into(),
                        reason: "mempool eviction reason id is unknown",
                    },
                )?,
            })
        }
        MempoolEventKind::Mined => {
            let mined = record.mined.ok_or(StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "mined mempool event is missing payload",
            })?;
            Ok(MempoolEvent::Mined {
                transaction_id: decode_transaction_id_for_family(
                    ArtifactFamily::MempoolEvent,
                    key,
                    &mined.transaction_id,
                )?,
                mined_height: BlockHeight::new(mined.mined_height),
                block_hash: decode_block_hash(
                    ArtifactFamily::MempoolEvent,
                    key,
                    &mined.block_hash,
                )?,
            })
        }
    }
}

fn mempool_entry_record(entry: &MempoolEntry) -> MempoolEntryRecord {
    MempoolEntryRecord {
        transaction_id: entry.transaction_id().as_bytes().to_vec(),
        auth_digest: entry
            .auth_digest()
            .map_or_else(Vec::new, |digest| digest.as_bytes().to_vec()),
        raw_transaction_bytes: entry.raw_transaction_bytes().as_slice().to_vec(),
        compact_transaction_data: Some(compact_transaction_data_record(
            entry.compact_transaction_data(),
        )),
        first_seen_unix_millis: entry.first_seen_unix_millis().value(),
        first_seen_chain_epoch: Some(chain_epoch_record(&entry.first_seen_chain_epoch())),
    }
}

fn decode_mempool_entry_record(
    key: &StoreKey,
    record: MempoolEntryRecord,
) -> Result<MempoolEntry, StoreError> {
    let chain_epoch_record = record
        .first_seen_chain_epoch
        .ok_or(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::MempoolEvent,
            key: key.clone().into(),
            reason: "mempool entry record is missing first-seen chain epoch",
        })?;
    let auth_digest = if record.auth_digest.is_empty() {
        None
    } else {
        let auth_digest_bytes =
            <[u8; 32]>::try_from(record.auth_digest.as_slice()).map_err(|_| {
                StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::MempoolEvent,
                    key: key.clone().into(),
                    reason: "auth digest must be 32 bytes",
                }
            })?;
        Some(AuthDigest::from_bytes(auth_digest_bytes))
    };

    let transaction_id = decode_transaction_id_for_family(
        ArtifactFamily::MempoolEvent,
        key,
        &record.transaction_id,
    )?;
    let compact_transaction_data = decode_compact_transaction_data_record(
        ArtifactFamily::MempoolEvent,
        key,
        record
            .compact_transaction_data
            .ok_or(StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "mempool entry record is missing compact transaction data",
            })?,
    )?;
    MempoolEntry::new(
        transaction_id,
        auth_digest,
        RawTransactionBytes::new(record.raw_transaction_bytes),
        compact_transaction_data,
        zinder_core::MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(record.first_seen_unix_millis),
            first_seen_chain_epoch: decode_chain_epoch_record(
                ArtifactFamily::MempoolEvent,
                key,
                &chain_epoch_record,
            )?,
        },
    )
    .map_err(|_| StoreError::ArtifactCorrupt {
        family: ArtifactFamily::MempoolEvent,
        key: key.clone().into(),
        reason: "mempool entry transparent output count exceeds u32::MAX",
    })
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvictionReason is non-exhaustive; persistence must reject future variants until the durable contract assigns them an id."
)]
fn mempool_eviction_reason_id(reason: MempoolEvictionReason) -> Result<u32, StoreError> {
    match reason {
        MempoolEvictionReason::Conflict => Ok(1),
        MempoolEvictionReason::Expired => Ok(2),
        MempoolEvictionReason::LowFee => Ok(3),
        MempoolEvictionReason::NodeRejected => Ok(4),
        MempoolEvictionReason::Unknown => Ok(5),
        _ => Err(StoreError::InvalidChainEpochArtifacts {
            reason: "mempool eviction reason is unsupported by the durable event contract",
        }),
    }
}

const fn mempool_eviction_reason_from_id(reason_id: u32) -> Option<MempoolEvictionReason> {
    match reason_id {
        1 => Some(MempoolEvictionReason::Conflict),
        2 => Some(MempoolEvictionReason::Expired),
        3 => Some(MempoolEvictionReason::LowFee),
        4 => Some(MempoolEvictionReason::NodeRejected),
        5 => Some(MempoolEvictionReason::Unknown),
        _ => None,
    }
}

pub(super) fn compact_transaction_data_record(
    scan_data: &CompactTransactionData,
) -> CompactTransactionDataRecord {
    CompactTransactionDataRecord {
        fee_zat: scan_data.fee_zat,
        sapling_spends: scan_data
            .sapling_spends
            .iter()
            .map(|spend| CompactSaplingSpendRecord {
                nullifier: spend.nullifier.to_vec(),
            })
            .collect(),
        sapling_outputs: scan_data
            .sapling_outputs
            .iter()
            .map(|output| CompactSaplingOutputRecord {
                commitment: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        orchard_actions: scan_data
            .orchard_actions
            .iter()
            .map(compact_shielded_action_record)
            .collect(),
        ironwood_actions: scan_data
            .ironwood_actions
            .iter()
            .map(compact_shielded_action_record)
            .collect(),
        transparent_inputs: scan_data
            .transparent_inputs
            .iter()
            .map(|input| CompactTransparentInputRecord {
                previous_transaction_id: input.previous_transaction_id.as_bytes().to_vec(),
                previous_output_index: input.previous_output_index,
            })
            .collect(),
        transparent_outputs: scan_data
            .transparent_outputs
            .iter()
            .map(|output| CompactTransparentOutputRecord {
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
    }
}

fn compact_shielded_action_record(action: &CompactShieldedAction) -> CompactShieldedActionRecord {
    CompactShieldedActionRecord {
        nullifier: action.nullifier.to_vec(),
        commitment: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

pub(super) fn decode_compact_transaction_data_record(
    family: ArtifactFamily,
    key: &StoreKey,
    record: CompactTransactionDataRecord,
) -> Result<CompactTransactionData, StoreError> {
    Ok(CompactTransactionData {
        fee_zat: record.fee_zat,
        sapling_spends: record
            .sapling_spends
            .into_iter()
            .map(|spend| {
                Ok(CompactSaplingSpend {
                    nullifier: decode_compact_fixed(
                        family,
                        key,
                        spend.nullifier,
                        "compact Sapling nullifier must be 32 bytes",
                    )?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?,
        sapling_outputs: record
            .sapling_outputs
            .into_iter()
            .map(|output| {
                Ok(CompactSaplingOutput {
                    commitment: decode_compact_fixed(
                        family,
                        key,
                        output.commitment,
                        "compact Sapling commitment must be 32 bytes",
                    )?,
                    ephemeral_key: decode_compact_fixed(
                        family,
                        key,
                        output.ephemeral_key,
                        "compact Sapling ephemeral key must be 32 bytes",
                    )?,
                    ciphertext: decode_compact_fixed(
                        family,
                        key,
                        output.ciphertext,
                        "compact Sapling ciphertext must be 52 bytes",
                    )?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?,
        orchard_actions: decode_compact_actions(family, key, record.orchard_actions)?,
        ironwood_actions: decode_compact_actions(family, key, record.ironwood_actions)?,
        transparent_inputs: record
            .transparent_inputs
            .into_iter()
            .map(|input| {
                Ok(CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes(decode_compact_fixed(
                        family,
                        key,
                        input.previous_transaction_id,
                        "compact transparent previous transaction id must be 32 bytes",
                    )?),
                    previous_output_index: input.previous_output_index,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?,
        transparent_outputs: record
            .transparent_outputs
            .into_iter()
            .map(|output| CompactTransparentOutput {
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key,
            })
            .collect(),
    })
}

fn decode_compact_actions(
    family: ArtifactFamily,
    key: &StoreKey,
    records: Vec<CompactShieldedActionRecord>,
) -> Result<Vec<CompactShieldedAction>, StoreError> {
    records
        .into_iter()
        .map(|action| {
            Ok(CompactShieldedAction {
                nullifier: decode_compact_fixed(
                    family,
                    key,
                    action.nullifier,
                    "compact action nullifier must be 32 bytes",
                )?,
                commitment: decode_compact_fixed(
                    family,
                    key,
                    action.commitment,
                    "compact action commitment must be 32 bytes",
                )?,
                ephemeral_key: decode_compact_fixed(
                    family,
                    key,
                    action.ephemeral_key,
                    "compact action ephemeral key must be 32 bytes",
                )?,
                ciphertext: decode_compact_fixed(
                    family,
                    key,
                    action.ciphertext,
                    "compact action ciphertext must be 52 bytes",
                )?,
            })
        })
        .collect()
}

pub(super) fn decode_compact_fixed<const N: usize>(
    family: ArtifactFamily,
    key: &StoreKey,
    bytes: Vec<u8>,
    reason: &'static str,
) -> Result<[u8; N], StoreError> {
    bytes.try_into().map_err(|_| StoreError::ArtifactCorrupt {
        family,
        key: key.clone().into(),
        reason,
    })
}

pub(super) fn chain_epoch_record(chain_epoch: &ChainEpoch) -> ChainEpochRecord {
    ChainEpochRecord {
        chain_epoch: chain_epoch.id.value(),
        network_id: chain_epoch.network.id(),
        tip_height: chain_epoch.visible_tip_height.value(),
        tip_hash: chain_epoch.visible_tip_hash.as_bytes().to_vec(),
        settled_tip_height: chain_epoch.settled_tip_height.value(),
        settled_tip_hash: chain_epoch.settled_tip_hash.as_bytes().to_vec(),
        artifact_schema_version: u32::from(chain_epoch.artifact_schema_version.value()),
        sapling_commitment_tree_size: chain_epoch.tip_metadata.sapling_commitment_tree_size,
        orchard_commitment_tree_size: chain_epoch.tip_metadata.orchard_commitment_tree_size,
        ironwood_commitment_tree_size: chain_epoch.tip_metadata.ironwood_commitment_tree_size,
        created_at_millis: chain_epoch.created_at.value(),
    }
}

pub(super) fn decode_chain_epoch_record(
    family: ArtifactFamily,
    key: &StoreKey,
    record: &ChainEpochRecord,
) -> Result<ChainEpoch, StoreError> {
    let network = Network::from_id(record.network_id).ok_or(StoreError::ArtifactCorrupt {
        family,
        key: key.clone().into(),
        reason: "chain epoch record has an unknown network id",
    })?;

    Ok(ChainEpoch {
        id: ChainEpochId::new(record.chain_epoch),
        network,
        visible_tip_height: BlockHeight::new(record.tip_height),
        visible_tip_hash: decode_block_hash(family, key, &record.tip_hash)?,
        settled_tip_height: BlockHeight::new(record.settled_tip_height),
        settled_tip_hash: decode_block_hash(family, key, &record.settled_tip_hash)?,
        artifact_schema_version: ArtifactSchemaVersion::new(
            u16::try_from(record.artifact_schema_version).map_err(|_| {
                StoreError::ArtifactCorrupt {
                    family,
                    key: key.clone().into(),
                    reason: "artifact schema version does not fit u16",
                }
            })?,
        ),
        tip_metadata: ChainTipMetadata::new(
            record.sapling_commitment_tree_size,
            record.orchard_commitment_tree_size,
            record.ironwood_commitment_tree_size,
        ),
        created_at: UnixTimestampMillis::new(record.created_at_millis),
    })
}

pub(super) fn decode_block_hash(
    family: ArtifactFamily,
    key: &StoreKey,
    hash_bytes: &[u8],
) -> Result<BlockHash, StoreError> {
    let hash_bytes = <[u8; 32]>::try_from(hash_bytes).map_err(|_| StoreError::ArtifactCorrupt {
        family,
        key: key.clone().into(),
        reason: "block hash must be 32 bytes",
    })?;

    Ok(BlockHash::from_bytes(hash_bytes))
}

pub(super) fn decode_transaction_id_for_family(
    family: ArtifactFamily,
    key: &StoreKey,
    transaction_id_bytes: &[u8],
) -> Result<TransactionId, StoreError> {
    let transaction_id_bytes =
        <[u8; 32]>::try_from(transaction_id_bytes).map_err(|_| StoreError::ArtifactCorrupt {
            family,
            key: key.clone().into(),
            reason: "transaction id must be 32 bytes",
        })?;

    Ok(TransactionId::from_bytes(transaction_id_bytes))
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ChainEpochRecord {
    #[prost(uint64, tag = "1")]
    chain_epoch: u64,
    #[prost(uint32, tag = "2")]
    network_id: u32,
    #[prost(uint32, tag = "3")]
    tip_height: u32,
    #[prost(bytes, tag = "4")]
    tip_hash: Vec<u8>,
    #[prost(uint32, tag = "5")]
    settled_tip_height: u32,
    #[prost(bytes, tag = "6")]
    settled_tip_hash: Vec<u8>,
    #[prost(uint32, tag = "7")]
    artifact_schema_version: u32,
    #[prost(uint64, tag = "8")]
    created_at_millis: u64,
    #[prost(uint32, tag = "9")]
    sapling_commitment_tree_size: u32,
    #[prost(uint32, tag = "10")]
    orchard_commitment_tree_size: u32,
    #[prost(uint32, tag = "11")]
    ironwood_commitment_tree_size: u32,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct CompactTransactionDataRecord {
    #[prost(uint64, optional, tag = "1")]
    fee_zat: Option<u64>,
    #[prost(message, repeated, tag = "2")]
    sapling_spends: Vec<CompactSaplingSpendRecord>,
    #[prost(message, repeated, tag = "3")]
    sapling_outputs: Vec<CompactSaplingOutputRecord>,
    #[prost(message, repeated, tag = "4")]
    orchard_actions: Vec<CompactShieldedActionRecord>,
    #[prost(message, repeated, tag = "5")]
    ironwood_actions: Vec<CompactShieldedActionRecord>,
    #[prost(message, repeated, tag = "6")]
    transparent_inputs: Vec<CompactTransparentInputRecord>,
    #[prost(message, repeated, tag = "7")]
    transparent_outputs: Vec<CompactTransparentOutputRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct CompactSaplingSpendRecord {
    #[prost(bytes, tag = "1")]
    nullifier: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CompactSaplingOutputRecord {
    #[prost(bytes, tag = "1")]
    commitment: Vec<u8>,
    #[prost(bytes, tag = "2")]
    ephemeral_key: Vec<u8>,
    #[prost(bytes, tag = "3")]
    ciphertext: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CompactShieldedActionRecord {
    #[prost(bytes, tag = "1")]
    nullifier: Vec<u8>,
    #[prost(bytes, tag = "2")]
    commitment: Vec<u8>,
    #[prost(bytes, tag = "3")]
    ephemeral_key: Vec<u8>,
    #[prost(bytes, tag = "4")]
    ciphertext: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CompactTransparentInputRecord {
    #[prost(bytes, tag = "1")]
    previous_transaction_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    previous_output_index: u32,
}

#[derive(Clone, PartialEq, Message)]
struct CompactTransparentOutputRecord {
    #[prost(uint64, tag = "1")]
    value_zat: u64,
    #[prost(bytes, tag = "2")]
    script_pub_key: Vec<u8>,
}

const MEMPOOL_EVENT_KIND_ADDED: u32 = 1;
const MEMPOOL_EVENT_KIND_INVALIDATED: u32 = 2;
const MEMPOOL_EVENT_KIND_MINED: u32 = 3;

#[derive(Clone, PartialEq, Message)]
struct MempoolEventEnvelopeRecord {
    #[prost(uint64, tag = "1")]
    event_sequence: u64,
    #[prost(uint64, tag = "2")]
    source_observed_unix_millis: u64,
    #[prost(message, optional, tag = "3")]
    event: Option<MempoolEventRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct MempoolEventRecord {
    #[prost(uint32, tag = "1")]
    event_kind: u32,
    #[prost(message, optional, tag = "2")]
    added: Option<MempoolEntryRecord>,
    #[prost(message, optional, tag = "3")]
    invalidated: Option<MempoolInvalidatedRecord>,
    #[prost(message, optional, tag = "4")]
    mined: Option<MempoolMinedRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct MempoolEntryRecord {
    #[prost(bytes, tag = "1")]
    transaction_id: Vec<u8>,
    #[prost(bytes, tag = "2")]
    auth_digest: Vec<u8>,
    #[prost(bytes, tag = "3")]
    raw_transaction_bytes: Vec<u8>,
    #[prost(message, optional, tag = "4")]
    compact_transaction_data: Option<CompactTransactionDataRecord>,
    #[prost(uint64, tag = "5")]
    first_seen_unix_millis: u64,
    #[prost(message, optional, tag = "6")]
    first_seen_chain_epoch: Option<ChainEpochRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct MempoolInvalidatedRecord {
    #[prost(bytes, tag = "1")]
    transaction_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    reason_id: u32,
}

#[derive(Clone, PartialEq, Message)]
struct MempoolMinedRecord {
    #[prost(bytes, tag = "1")]
    transaction_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    mined_height: u32,
    #[prost(bytes, tag = "3")]
    block_hash: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    const GOLDEN_NETWORK: Network = Network::ZcashRegtest;
    const GOLDEN_CURSOR_AUTH_KEY: [u8; 32] = [0x5a; 32];

    // Exact bytes persisted in the canonical mempool event column family.
    // Existing stores must keep decoding them, so any difference here is a
    // durable format change.
    const GOLDEN_ADDED_ENVELOPE_HEX: &str = "\
        080310d8d495ffbc311ac705080112c2050a20212121212121212121212121212121212121212121\
        21212121212121212121211220222222222222222222222222222222222222222222222222222222\
        22222222221a040500008022910408e80712220a2031313131313131313131313131313131313131\
        313131313131313131313131311a7a0a203232323232323232323232323232323232323232323232\
        32323232323232323212203333333333333333333333333333333333333333333333333333333333\
        3333331a343434343434343434343434343434343434343434343434343434343434343434343434\
        3434343434343434343434343434343434229c010a20414141414141414141414141414141414141\
        41414141414141414141414141411220424242424242424242424242424242424242424242424242\
        42424242424242421a20434343434343434343434343434343434343434343434343434343434343\
        43432234444444444444444444444444444444444444444444444444444444444444444444444444\
        444444444444444444444444444444442a9c010a2051515151515151515151515151515151515151\
        51515151515151515151515151122052525252525252525252525252525252525252525252525252\
        525252525252521a2053535353535353535353535353535353535353535353535353535353535353\
        53223454545454545454545454545454545454545454545454545454545454545454545454545454\
        54545454545454545454545454545432240a20616161616161616161616161616161616161616161\
        616161616161616161616110023a0808d08603120276a928f4d395ffbc31325b0807100318782220\
        11111111111111111111111111111111111111111111111111111111111111112814322012121212\
        1212121212121212121212121212121212121212121212121212121238074080d095ffbc31480150\
        025803";
    const GOLDEN_INVALIDATED_ENVELOPE_HEX: &str = "\
        080410bcd595ffbc311a2808021a240a207171717171717171717171717171717171717171717171\
        7171717171717171711003";
    const GOLDEN_MINED_ENVELOPE_HEX: &str = "\
        080510a0d695ffbc311a4a080322460a208181818181818181818181818181818181818181818181\
        81818181818181818110791a20828282828282828282828282828282828282828282828282828282\
        8282828282";

    #[test]
    fn mempool_event_envelope_bytes_match_persisted_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            (
                golden_envelope(
                    3,
                    1_700_000_000_600,
                    MempoolEvent::Added {
                        entry: golden_added_entry()?,
                    },
                )?,
                GOLDEN_ADDED_ENVELOPE_HEX,
            ),
            (
                golden_envelope(
                    4,
                    1_700_000_000_700,
                    MempoolEvent::Invalidated {
                        transaction_id: TransactionId::from_bytes([0x71; 32]),
                        reason: MempoolEvictionReason::LowFee,
                    },
                )?,
                GOLDEN_INVALIDATED_ENVELOPE_HEX,
            ),
            (
                golden_envelope(
                    5,
                    1_700_000_000_800,
                    MempoolEvent::Mined {
                        transaction_id: TransactionId::from_bytes([0x81; 32]),
                        mined_height: BlockHeight::new(121),
                        block_hash: BlockHash::from_bytes([0x82; 32]),
                    },
                )?,
                GOLDEN_MINED_ENVELOPE_HEX,
            ),
        ];

        for (envelope, expected_hex) in cases {
            let encoded = encode_mempool_event_envelope(&envelope)?;
            assert_eq!(
                lower_hex(&encoded)?,
                expected_hex,
                "mempool event {} bytes changed",
                envelope.event_sequence
            );
            let decoded = decode_mempool_event_envelope(
                &StoreKey::mempool_event(envelope.event_sequence),
                &encoded,
                GOLDEN_NETWORK,
                GOLDEN_CURSOR_AUTH_KEY,
            )?;
            assert_eq!(decoded, envelope);
        }
        Ok(())
    }

    fn golden_added_entry() -> Result<MempoolEntry, Box<dyn std::error::Error>> {
        let first_seen_chain_epoch = ChainEpoch {
            id: ChainEpochId::new(7),
            network: GOLDEN_NETWORK,
            visible_tip_height: BlockHeight::new(120),
            visible_tip_hash: BlockHash::from_bytes([0x11; 32]),
            settled_tip_height: BlockHeight::new(20),
            settled_tip_hash: BlockHash::from_bytes([0x12; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(7),
            tip_metadata: ChainTipMetadata::new(1, 2, 3),
            created_at: UnixTimestampMillis::new(1_700_000_000_000),
        };
        let shielded_action = |seed: u8| CompactShieldedAction {
            nullifier: [seed; 32],
            commitment: [seed.wrapping_add(1); 32],
            ephemeral_key: [seed.wrapping_add(2); 32],
            ciphertext: [seed.wrapping_add(3); 52],
        };
        Ok(MempoolEntry::new(
            TransactionId::from_bytes([0x21; 32]),
            Some(AuthDigest::from_bytes([0x22; 32])),
            RawTransactionBytes::new(vec![0x05, 0x00, 0x00, 0x80]),
            CompactTransactionData {
                fee_zat: Some(1_000),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [0x31; 32],
                }],
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: [0x32; 32],
                    ephemeral_key: [0x33; 32],
                    ciphertext: [0x34; 52],
                }],
                orchard_actions: vec![shielded_action(0x41)],
                ironwood_actions: vec![shielded_action(0x51)],
                transparent_inputs: vec![CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes([0x61; 32]),
                    previous_output_index: 2,
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 50_000,
                    script_pub_key: vec![0x76, 0xa9],
                }],
            },
            zinder_core::MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_500),
                first_seen_chain_epoch,
            },
        )?)
    }

    fn golden_envelope(
        event_sequence: u64,
        source_observed_unix_millis: u64,
        event: MempoolEvent,
    ) -> Result<MempoolEventEnvelope, Box<dyn std::error::Error>> {
        Ok(MempoolEventEnvelope {
            cursor: StreamCursorTokenV1::mempool_event(
                GOLDEN_NETWORK,
                event_sequence,
                event.transaction_id(),
                GOLDEN_CURSOR_AUTH_KEY,
            )?,
            event_sequence,
            source_observed_unix_millis,
            event,
        })
    }

    fn lower_hex(bytes: &[u8]) -> Result<String, std::fmt::Error> {
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(encoded, "{byte:02x}")?;
        }
        Ok(encoded)
    }
}
