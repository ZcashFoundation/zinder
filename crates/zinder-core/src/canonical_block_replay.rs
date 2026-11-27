//! Durable semantic replay encoding for canonical block facts.
//!
//! The replay format is intentionally independent of both the
//! [`CanonicalBlockFactsDigestVersion`] contract and any storage engine's
//! physical schema. The envelope carries both version numbers explicitly so
//! either contract can evolve without overloading the other.

use prost::Message;
use thiserror::Error;

use crate::wire::{encode_internal_block_hash, encode_internal_transaction_id};
use crate::{
    AuthDigest, BlockHash, BlockHeaderArtifact, BlockHeight, CanonicalBlockFacts,
    CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion, CanonicalTransactionFacts,
    ConsensusBranchId, LockTime, PrivacyShape, SerializedBytesDigest, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
    UnsupportedSection, Wtxid,
};

/// Version of the durable semantic replay format for [`CanonicalBlockFacts`].
///
/// This version governs the protobuf envelope and payload records only. It is
/// not a storage-backend schema version, and it is not a
/// [`CanonicalBlockFactsDigestVersion`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CanonicalBlockReplayFormatVersion {
    /// Initial fact-only protobuf payload.
    V1,
}

impl CanonicalBlockReplayFormatVersion {
    /// Format emitted by new replay encodings.
    pub const CURRENT: Self = Self::V1;

    /// Returns the stable numeric version carried by the replay envelope.
    #[must_use]
    pub const fn value(self) -> u32 {
        match self {
            Self::V1 => 1,
        }
    }
}

/// An encoded canonical-block replay format this binary does not support.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("unsupported canonical block replay format version {encoded_version}")]
pub struct UnsupportedCanonicalBlockReplayFormatVersion {
    encoded_version: u32,
}

impl TryFrom<u32> for CanonicalBlockReplayFormatVersion {
    type Error = UnsupportedCanonicalBlockReplayFormatVersion;

    fn try_from(encoded_version: u32) -> Result<Self, Self::Error> {
        match encoded_version {
            1 => Ok(Self::V1),
            _ => Err(UnsupportedCanonicalBlockReplayFormatVersion { encoded_version }),
        }
    }
}

/// Canonical bytes and metadata produced by the semantic replay encoder.
///
/// The encoder returns this wrapper so callers can persist the bytes and reuse
/// the already-computed block identity and reference digest without decoding or
/// serializing the facts again. The block identity is in-memory metadata; it is
/// not appended to the durable replay bytes or included in the reference digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBlockReplayEnvelope {
    format_version: CanonicalBlockReplayFormatVersion,
    block_height: BlockHeight,
    block_hash: BlockHash,
    parent_hash: BlockHash,
    reference_digest: CanonicalBlockFactsDigest,
    bytes: Vec<u8>,
}

impl CanonicalBlockReplayEnvelope {
    /// Returns the replay-envelope format used by these bytes.
    #[must_use]
    pub const fn format_version(&self) -> CanonicalBlockReplayFormatVersion {
        self.format_version
    }

    /// Returns the source block height without decoding the replay bytes.
    #[must_use]
    pub const fn block_height(&self) -> BlockHeight {
        self.block_height
    }

    /// Returns the source block hash without decoding the replay bytes.
    #[must_use]
    pub const fn block_hash(&self) -> BlockHash {
        self.block_hash
    }

    /// Returns the source block's parent hash without decoding the replay bytes.
    #[must_use]
    pub const fn parent_hash(&self) -> BlockHash {
        self.parent_hash
    }

    /// Returns the backend-neutral digest already committed by the envelope.
    #[must_use]
    pub const fn reference_digest(&self) -> CanonicalBlockFactsDigest {
        self.reference_digest
    }

    /// Borrows the complete canonical replay envelope bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the wrapper and returns the complete replay envelope bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Complete canonical facts recovered from a validated replay envelope.
///
/// Construction is restricted to [`decode_canonical_block_replay`],
/// which validates the protobuf representation, canonical re-encoding, and
/// backend-neutral reference digest before returning this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedCanonicalBlockReplay {
    format_version: CanonicalBlockReplayFormatVersion,
    reference_digest: CanonicalBlockFactsDigest,
    facts: CanonicalBlockFacts,
}

impl ValidatedCanonicalBlockReplay {
    /// Returns the replay-envelope format that carried these facts.
    #[must_use]
    pub const fn format_version(&self) -> CanonicalBlockReplayFormatVersion {
        self.format_version
    }

    /// Returns the validated backend-neutral digest committed by the envelope.
    #[must_use]
    pub const fn reference_digest(&self) -> CanonicalBlockFactsDigest {
        self.reference_digest
    }

    /// Borrows the complete recovered canonical facts.
    #[must_use]
    pub const fn facts(&self) -> &CanonicalBlockFacts {
        &self.facts
    }

    /// Consumes the validated replay value and returns its canonical facts.
    #[must_use]
    pub fn into_facts(self) -> CanonicalBlockFacts {
        self.facts
    }

    /// Constructs a [`ValidatedCanonicalBlockReplay`] from already-authenticated block facts.
    #[must_use]
    pub fn from_authenticated_facts(
        facts: CanonicalBlockFacts,
        format_version: CanonicalBlockReplayFormatVersion,
        digest_version: CanonicalBlockFactsDigestVersion,
    ) -> Self {
        let reference_digest = facts.digest(digest_version);
        Self {
            format_version,
            reference_digest,
            facts,
        }
    }
}

/// Failure to decode and validate a canonical-block replay envelope.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalBlockReplayDecodeError {
    /// The outer bytes are not a valid protobuf envelope.
    #[error("canonical block replay envelope is not valid protobuf")]
    InvalidEnvelope {
        /// Underlying protobuf decoding failure.
        #[source]
        source: prost::DecodeError,
    },
    /// The envelope declares a replay format this binary cannot decode.
    #[error("unsupported canonical block replay format version {encoded_version}")]
    UnsupportedFormatVersion {
        /// Numeric version read from the envelope.
        encoded_version: u32,
    },
    /// The envelope declares a reference-digest contract this binary cannot validate.
    #[error("unsupported canonical block facts reference digest version {encoded_version}")]
    UnsupportedReferenceDigestVersion {
        /// Numeric digest version read from the envelope.
        encoded_version: u32,
    },
    /// The versioned payload bytes are absent.
    #[error("canonical block replay envelope is missing its payload")]
    MissingPayload,
    /// The versioned payload is not valid protobuf.
    #[error("canonical block replay payload is not valid protobuf")]
    InvalidPayload {
        /// Underlying protobuf decoding failure.
        #[source]
        source: prost::DecodeError,
    },
    /// A required nested message is absent.
    #[error("canonical block replay is missing required field {field}")]
    MissingField {
        /// Stable field path identifying the missing value.
        field: &'static str,
    },
    /// A fixed-width byte field has an invalid length.
    #[error(
        "canonical block replay field {field} must contain {expected} bytes, received {actual}"
    )]
    InvalidFieldLength {
        /// Stable field path identifying the invalid value.
        field: &'static str,
        /// Required byte length.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// An enum-like record contains an unknown numeric discriminator.
    #[error("canonical block replay field {field} has unknown discriminant {discriminant}")]
    UnknownDiscriminant {
        /// Stable field path identifying the invalid value.
        field: &'static str,
        /// Unknown numeric discriminator.
        discriminant: u32,
    },
    /// Related fields describe an impossible or noncanonical combination.
    #[error("canonical block replay field {field} is inconsistent: {reason}")]
    InconsistentField {
        /// Stable field path identifying the invalid value.
        field: &'static str,
        /// Stable explanation of the violated relationship.
        reason: &'static str,
    },
    /// A numeric value cannot fit the corresponding domain type.
    #[error("canonical block replay field {field} value {encoded_value} does not fit {target}")]
    NumericOutOfRange {
        /// Stable field path identifying the invalid value.
        field: &'static str,
        /// Encoded numeric value.
        encoded_value: u64,
        /// Domain target type.
        target: &'static str,
    },
    /// Decoding and re-encoding the payload did not reproduce the stored bytes.
    #[error("canonical block replay payload is not canonically encoded")]
    NonCanonicalPayload,
    /// The stored reference digest does not match the recovered semantic facts.
    #[error("canonical block replay reference digest does not match its payload")]
    ReferenceDigestMismatch,
    /// Decoding and re-encoding the whole envelope did not reproduce the input bytes.
    #[error("canonical block replay envelope is not canonically encoded")]
    NonCanonicalEnvelope,
}

/// Encodes complete canonical block facts into a versioned semantic replay envelope.
///
/// `format_version` controls only the protobuf representation. The independent
/// `reference_digest_version` controls the backend-neutral semantic digest
/// recorded beside the payload.
#[must_use]
pub fn encode_canonical_block_replay(
    facts: &CanonicalBlockFacts,
    format_version: CanonicalBlockReplayFormatVersion,
    reference_digest_version: CanonicalBlockFactsDigestVersion,
) -> CanonicalBlockReplayEnvelope {
    let payload = encode_payload(facts, format_version);
    let reference_digest = facts.digest(reference_digest_version);
    let bytes = replay_envelope_record(format_version, payload, reference_digest).encode_to_vec();
    CanonicalBlockReplayEnvelope {
        format_version,
        block_height: facts.block_header.height,
        block_hash: facts.block_header.block_hash,
        parent_hash: facts.block_header.parent_hash,
        reference_digest,
        bytes,
    }
}

/// Decodes a replay envelope into complete, validated canonical block facts.
///
/// Validation fails closed on unknown versions, malformed or incomplete
/// records, noncanonical protobuf encodings, and a reference-digest mismatch.
/// Canonical protobuf validation prevents multiple byte representations of the
/// same typed facts from entering snapshots or cross-backend comparisons; the
/// reference digest remains the independent semantic equality contract.
pub fn decode_canonical_block_replay(
    encoded: &[u8],
) -> Result<ValidatedCanonicalBlockReplay, CanonicalBlockReplayDecodeError> {
    let envelope = CanonicalBlockReplayEnvelopeRecord::decode(encoded)
        .map_err(|source| CanonicalBlockReplayDecodeError::InvalidEnvelope { source })?;
    let format_version = CanonicalBlockReplayFormatVersion::try_from(envelope.format_version)
        .map_err(
            |_| CanonicalBlockReplayDecodeError::UnsupportedFormatVersion {
                encoded_version: envelope.format_version,
            },
        )?;
    let reference_digest_version = decode_reference_digest_version(envelope.digest_version)?;

    if envelope.payload.is_empty() {
        return Err(CanonicalBlockReplayDecodeError::MissingPayload);
    }
    let facts = decode_payload(&envelope.payload, format_version)?;
    let canonical_payload = encode_payload(&facts, format_version);
    if canonical_payload != envelope.payload {
        return Err(CanonicalBlockReplayDecodeError::NonCanonicalPayload);
    }

    let stored_digest_bytes =
        fixed_bytes::<32>(&envelope.reference_digest, "envelope.reference_digest")?;
    let expected_digest = facts.digest(reference_digest_version);
    if expected_digest.as_bytes() != stored_digest_bytes {
        return Err(CanonicalBlockReplayDecodeError::ReferenceDigestMismatch);
    }

    let canonical_envelope =
        replay_envelope_record(format_version, canonical_payload, expected_digest).encode_to_vec();
    if canonical_envelope != encoded {
        return Err(CanonicalBlockReplayDecodeError::NonCanonicalEnvelope);
    }

    Ok(ValidatedCanonicalBlockReplay {
        format_version,
        reference_digest: expected_digest,
        facts,
    })
}

fn decode_reference_digest_version(
    encoded_version: u32,
) -> Result<CanonicalBlockFactsDigestVersion, CanonicalBlockReplayDecodeError> {
    let encoded_u16 = u16::try_from(encoded_version).map_err(|_| {
        CanonicalBlockReplayDecodeError::UnsupportedReferenceDigestVersion { encoded_version }
    })?;
    CanonicalBlockFactsDigestVersion::try_from(encoded_u16).map_err(|_| {
        CanonicalBlockReplayDecodeError::UnsupportedReferenceDigestVersion { encoded_version }
    })
}

fn replay_envelope_record(
    format_version: CanonicalBlockReplayFormatVersion,
    payload: Vec<u8>,
    reference_digest: CanonicalBlockFactsDigest,
) -> CanonicalBlockReplayEnvelopeRecord {
    CanonicalBlockReplayEnvelopeRecord {
        format_version: format_version.value(),
        payload,
        digest_version: u32::from(reference_digest.version().value()),
        reference_digest: reference_digest.as_bytes().to_vec(),
    }
}

fn encode_payload(
    facts: &CanonicalBlockFacts,
    format_version: CanonicalBlockReplayFormatVersion,
) -> Vec<u8> {
    match format_version {
        CanonicalBlockReplayFormatVersion::V1 => replay_v1_record(facts).encode_to_vec(),
    }
}

fn decode_payload(
    payload: &[u8],
    format_version: CanonicalBlockReplayFormatVersion,
) -> Result<CanonicalBlockFacts, CanonicalBlockReplayDecodeError> {
    match format_version {
        CanonicalBlockReplayFormatVersion::V1 => {
            let record = CanonicalBlockReplayV1Record::decode(payload)
                .map_err(|source| CanonicalBlockReplayDecodeError::InvalidPayload { source })?;
            canonical_block_facts_from_record(record)
        }
    }
}

fn replay_v1_record(facts: &CanonicalBlockFacts) -> CanonicalBlockReplayV1Record {
    CanonicalBlockReplayV1Record {
        block_header: Some(block_header_record(&facts.block_header)),
        transactions: facts
            .transactions
            .iter()
            .map(canonical_transaction_record)
            .collect(),
        serialized_bytes_digest: facts.serialized_bytes_digest.as_bytes().to_vec(),
    }
}

fn canonical_block_facts_from_record(
    record: CanonicalBlockReplayV1Record,
) -> Result<CanonicalBlockFacts, CanonicalBlockReplayDecodeError> {
    let block_header = required(record.block_header, "payload.block_header")?;
    Ok(CanonicalBlockFacts {
        block_header: block_header_from_record(&block_header)?,
        serialized_bytes_digest: SerializedBytesDigest::from_bytes(fixed_bytes(
            &record.serialized_bytes_digest,
            "payload.serialized_bytes_digest",
        )?),
        transactions: record
            .transactions
            .into_iter()
            .map(canonical_transaction_from_record)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn block_header_record(header: &BlockHeaderArtifact) -> BlockHeaderRecord {
    BlockHeaderRecord {
        height: header.height.value(),
        block_hash: encode_internal_block_hash(header.block_hash).to_vec(),
        parent_hash: encode_internal_block_hash(header.parent_hash).to_vec(),
        merkle_root_hash: header.merkle_root_hash.to_vec(),
        commitment_bytes: header.commitment_bytes.to_vec(),
        block_time: header.block_time,
        bits: header.bits,
        nonce: header.nonce.to_vec(),
        version: header.version,
        block_size_bytes: header.block_size_bytes,
    }
}

fn block_header_from_record(
    record: &BlockHeaderRecord,
) -> Result<BlockHeaderArtifact, CanonicalBlockReplayDecodeError> {
    Ok(BlockHeaderArtifact::new(
        BlockHeight::new(record.height),
        BlockHash::from_bytes(fixed_bytes(
            &record.block_hash,
            "payload.block_header.block_hash",
        )?),
        BlockHash::from_bytes(fixed_bytes(
            &record.parent_hash,
            "payload.block_header.parent_hash",
        )?),
        fixed_bytes(
            &record.merkle_root_hash,
            "payload.block_header.merkle_root_hash",
        )?,
        fixed_bytes(
            &record.commitment_bytes,
            "payload.block_header.commitment_bytes",
        )?,
        record.block_time,
        record.bits,
        fixed_bytes(&record.nonce, "payload.block_header.nonce")?,
        record.version,
        record.block_size_bytes,
    ))
}

fn canonical_transaction_record(facts: &CanonicalTransactionFacts) -> CanonicalTransactionRecord {
    CanonicalTransactionRecord {
        public_facts: Some(transaction_public_facts_record(&facts.public_facts)),
        intrinsic_value_balances: Some(intrinsic_value_balances_record(
            facts.intrinsic_value_balances,
        )),
        transparent_inputs: facts
            .transparent_inputs
            .iter()
            .copied()
            .map(transparent_input_record)
            .collect(),
        transparent_outputs: facts
            .transparent_outputs
            .iter()
            .map(transparent_output_record)
            .collect(),
        serialized_bytes_digest: facts.serialized_bytes_digest.as_bytes().to_vec(),
    }
}

fn canonical_transaction_from_record(
    record: CanonicalTransactionRecord,
) -> Result<CanonicalTransactionFacts, CanonicalBlockReplayDecodeError> {
    Ok(CanonicalTransactionFacts {
        public_facts: transaction_public_facts_from_record(required(
            record.public_facts,
            "payload.transactions.public_facts",
        )?)?,
        serialized_bytes_digest: SerializedBytesDigest::from_bytes(fixed_bytes(
            &record.serialized_bytes_digest,
            "payload.transactions.serialized_bytes_digest",
        )?),
        intrinsic_value_balances: intrinsic_value_balances_from_record(required(
            record.intrinsic_value_balances,
            "payload.transactions.intrinsic_value_balances",
        )?),
        transparent_inputs: record
            .transparent_inputs
            .into_iter()
            .map(transparent_input_from_record)
            .collect::<Result<Vec<_>, _>>()?,
        transparent_outputs: record
            .transparent_outputs
            .into_iter()
            .map(transparent_output_from_record)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn transaction_public_facts_record(facts: &TransactionPublicFacts) -> TransactionPublicFactsRecord {
    TransactionPublicFactsRecord {
        transaction_id: encode_internal_transaction_id(facts.transaction_id).to_vec(),
        auth_digest: facts.auth_digest.map(|digest| digest.as_bytes().to_vec()),
        wtxid: facts.wtxid.map(|wtxid| wtxid.as_bytes().to_vec()),
        transaction_version: Some(transaction_version_record(facts.version)),
        consensus_branch_id: facts.consensus_branch_id.map(ConsensusBranchId::value),
        lock_time: Some(lock_time_record(facts.lock_time)),
        expiry_height: facts.expiry_height.map(BlockHeight::value),
        size_bytes: facts.size_bytes,
        counts: Some(transaction_component_counts_record(facts.counts)),
        orchard_value_balance_zat: facts.orchard_value_balance_zat,
        orchard_anchor: facts.orchard_anchor.map(|anchor| anchor.to_vec()),
        ironwood_value_balance_zat: facts.ironwood_value_balance_zat,
        privacy_shape: privacy_shape_id(facts.privacy_shape),
        is_coinbase: facts.is_coinbase,
        unsupported_sections: facts
            .unsupported_sections
            .iter()
            .copied()
            .map(unsupported_section_id)
            .collect(),
    }
}

fn transaction_public_facts_from_record(
    record: TransactionPublicFactsRecord,
) -> Result<TransactionPublicFacts, CanonicalBlockReplayDecodeError> {
    Ok(TransactionPublicFacts {
        transaction_id: TransactionId::from_bytes(fixed_bytes(
            &record.transaction_id,
            "payload.transactions.public_facts.transaction_id",
        )?),
        auth_digest: record
            .auth_digest
            .map(|bytes| {
                fixed_bytes(&bytes, "payload.transactions.public_facts.auth_digest")
                    .map(AuthDigest::from_bytes)
            })
            .transpose()?,
        wtxid: record
            .wtxid
            .map(|bytes| {
                fixed_bytes(&bytes, "payload.transactions.public_facts.wtxid")
                    .map(Wtxid::from_bytes)
            })
            .transpose()?,
        version: transaction_version_from_record(required(
            record.transaction_version,
            "payload.transactions.public_facts.transaction_version",
        )?)?,
        consensus_branch_id: record.consensus_branch_id.map(ConsensusBranchId::new),
        lock_time: lock_time_from_record(required(
            record.lock_time,
            "payload.transactions.public_facts.lock_time",
        )?)?,
        expiry_height: record.expiry_height.map(BlockHeight::new),
        size_bytes: record.size_bytes,
        counts: transaction_component_counts_from_record(required(
            record.counts,
            "payload.transactions.public_facts.counts",
        )?),
        orchard_value_balance_zat: record.orchard_value_balance_zat,
        orchard_anchor: record
            .orchard_anchor
            .map(|bytes| fixed_bytes(&bytes, "payload.transactions.public_facts.orchard_anchor"))
            .transpose()?,
        ironwood_value_balance_zat: record.ironwood_value_balance_zat,
        privacy_shape: privacy_shape_from_id(record.privacy_shape)?,
        is_coinbase: record.is_coinbase,
        unsupported_sections: record
            .unsupported_sections
            .into_iter()
            .map(unsupported_section_from_id)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

const TRANSACTION_VERSION_V1: u32 = 1;
const TRANSACTION_VERSION_V2: u32 = 2;
const TRANSACTION_VERSION_V3: u32 = 3;
const TRANSACTION_VERSION_V4: u32 = 4;
const TRANSACTION_VERSION_V5: u32 = 5;
const TRANSACTION_VERSION_V6: u32 = 6;
const TRANSACTION_VERSION_UNSUPPORTED: u32 = 7;

const fn transaction_version_record(version: TransactionVersion) -> TransactionVersionRecord {
    match version {
        TransactionVersion::V1 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V1,
            effective_version: 1,
            version_group_id: None,
        },
        TransactionVersion::V2 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V2,
            effective_version: 2,
            version_group_id: None,
        },
        TransactionVersion::V3 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V3,
            effective_version: 3,
            version_group_id: None,
        },
        TransactionVersion::V4 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V4,
            effective_version: 4,
            version_group_id: None,
        },
        TransactionVersion::V5 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V5,
            effective_version: 5,
            version_group_id: None,
        },
        TransactionVersion::V6 => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_V6,
            effective_version: 6,
            version_group_id: None,
        },
        TransactionVersion::Unsupported {
            effective_version,
            version_group_id,
        } => TransactionVersionRecord {
            kind: TRANSACTION_VERSION_UNSUPPORTED,
            effective_version,
            version_group_id,
        },
    }
}

fn transaction_version_from_record(
    record: TransactionVersionRecord,
) -> Result<TransactionVersion, CanonicalBlockReplayDecodeError> {
    match record.kind {
        TRANSACTION_VERSION_V1 => supported_transaction_version(record, 1, TransactionVersion::V1),
        TRANSACTION_VERSION_V2 => supported_transaction_version(record, 2, TransactionVersion::V2),
        TRANSACTION_VERSION_V3 => supported_transaction_version(record, 3, TransactionVersion::V3),
        TRANSACTION_VERSION_V4 => supported_transaction_version(record, 4, TransactionVersion::V4),
        TRANSACTION_VERSION_V5 => supported_transaction_version(record, 5, TransactionVersion::V5),
        TRANSACTION_VERSION_V6 => supported_transaction_version(record, 6, TransactionVersion::V6),
        TRANSACTION_VERSION_UNSUPPORTED => Ok(TransactionVersion::Unsupported {
            effective_version: record.effective_version,
            version_group_id: record.version_group_id,
        }),
        discriminant => Err(CanonicalBlockReplayDecodeError::UnknownDiscriminant {
            field: "payload.transactions.public_facts.transaction_version.kind",
            discriminant,
        }),
    }
}

fn supported_transaction_version(
    record: TransactionVersionRecord,
    expected_effective_version: u32,
    version: TransactionVersion,
) -> Result<TransactionVersion, CanonicalBlockReplayDecodeError> {
    if record.effective_version != expected_effective_version {
        return Err(CanonicalBlockReplayDecodeError::InconsistentField {
            field: "payload.transactions.public_facts.transaction_version.effective_version",
            reason: "supported version kind must carry its matching effective version",
        });
    }
    if record.version_group_id.is_some() {
        return Err(CanonicalBlockReplayDecodeError::InconsistentField {
            field: "payload.transactions.public_facts.transaction_version.version_group_id",
            reason: "supported version kind must not carry an unsupported-version group id",
        });
    }
    Ok(version)
}

const LOCK_TIME_UNLOCKED: u32 = 1;
const LOCK_TIME_HEIGHT: u32 = 2;
const LOCK_TIME_UNIX_SECONDS: u32 = 3;

const fn lock_time_record(lock_time: LockTime) -> LockTimeRecord {
    match lock_time {
        LockTime::Unlocked => LockTimeRecord {
            kind: LOCK_TIME_UNLOCKED,
            value: 0,
        },
        LockTime::Height(height) => LockTimeRecord {
            kind: LOCK_TIME_HEIGHT,
            value: height.value() as u64,
        },
        LockTime::UnixSeconds(seconds) => LockTimeRecord {
            kind: LOCK_TIME_UNIX_SECONDS,
            value: seconds,
        },
    }
}

fn lock_time_from_record(
    record: LockTimeRecord,
) -> Result<LockTime, CanonicalBlockReplayDecodeError> {
    match record.kind {
        LOCK_TIME_UNLOCKED if record.value == 0 => Ok(LockTime::Unlocked),
        LOCK_TIME_UNLOCKED => Err(CanonicalBlockReplayDecodeError::InconsistentField {
            field: "payload.transactions.public_facts.lock_time.value",
            reason: "unlocked lock time must carry value zero",
        }),
        LOCK_TIME_HEIGHT => u32::try_from(record.value)
            .map(BlockHeight::new)
            .map(LockTime::Height)
            .map_err(|_| CanonicalBlockReplayDecodeError::NumericOutOfRange {
                field: "payload.transactions.public_facts.lock_time.value",
                encoded_value: record.value,
                target: "u32 block height",
            }),
        LOCK_TIME_UNIX_SECONDS => Ok(LockTime::UnixSeconds(record.value)),
        discriminant => Err(CanonicalBlockReplayDecodeError::UnknownDiscriminant {
            field: "payload.transactions.public_facts.lock_time.kind",
            discriminant,
        }),
    }
}

const fn transaction_component_counts_record(
    counts: TransactionComponentCounts,
) -> TransactionComponentCountsRecord {
    TransactionComponentCountsRecord {
        transparent_input_count: counts.transparent_input_count,
        transparent_output_count: counts.transparent_output_count,
        sapling_spend_count: counts.sapling_spend_count,
        sapling_output_count: counts.sapling_output_count,
        orchard_action_count: counts.orchard_action_count,
        ironwood_action_count: counts.ironwood_action_count,
        sprout_joinsplit_count: counts.sprout_joinsplit_count,
    }
}

const fn transaction_component_counts_from_record(
    record: TransactionComponentCountsRecord,
) -> TransactionComponentCounts {
    TransactionComponentCounts {
        transparent_input_count: record.transparent_input_count,
        transparent_output_count: record.transparent_output_count,
        sapling_spend_count: record.sapling_spend_count,
        sapling_output_count: record.sapling_output_count,
        orchard_action_count: record.orchard_action_count,
        ironwood_action_count: record.ironwood_action_count,
        sprout_joinsplit_count: record.sprout_joinsplit_count,
    }
}

const fn privacy_shape_id(privacy_shape: PrivacyShape) -> u32 {
    match privacy_shape {
        PrivacyShape::TransparentOnly => 1,
        PrivacyShape::Shielding => 2,
        PrivacyShape::Deshielding => 3,
        PrivacyShape::ShieldedOnly => 4,
        PrivacyShape::Mixed => 5,
        PrivacyShape::Coinbase => 6,
        PrivacyShape::ShieldedCoinbase => 7,
        PrivacyShape::Unclassified => 8,
    }
}

fn privacy_shape_from_id(
    discriminant: u32,
) -> Result<PrivacyShape, CanonicalBlockReplayDecodeError> {
    match discriminant {
        1 => Ok(PrivacyShape::TransparentOnly),
        2 => Ok(PrivacyShape::Shielding),
        3 => Ok(PrivacyShape::Deshielding),
        4 => Ok(PrivacyShape::ShieldedOnly),
        5 => Ok(PrivacyShape::Mixed),
        6 => Ok(PrivacyShape::Coinbase),
        7 => Ok(PrivacyShape::ShieldedCoinbase),
        8 => Ok(PrivacyShape::Unclassified),
        discriminant => Err(CanonicalBlockReplayDecodeError::UnknownDiscriminant {
            field: "payload.transactions.public_facts.privacy_shape",
            discriminant,
        }),
    }
}

const fn unsupported_section_id(section: UnsupportedSection) -> u32 {
    match section {
        UnsupportedSection::FutureVersionHeader => 1,
        UnsupportedSection::FutureShieldedProtocol => 2,
    }
}

fn unsupported_section_from_id(
    discriminant: u32,
) -> Result<UnsupportedSection, CanonicalBlockReplayDecodeError> {
    match discriminant {
        1 => Ok(UnsupportedSection::FutureVersionHeader),
        2 => Ok(UnsupportedSection::FutureShieldedProtocol),
        discriminant => Err(CanonicalBlockReplayDecodeError::UnknownDiscriminant {
            field: "payload.transactions.public_facts.unsupported_sections",
            discriminant,
        }),
    }
}

const fn intrinsic_value_balances_record(
    balances: TransactionIntrinsicValueBalances,
) -> TransactionIntrinsicValueBalancesRecord {
    TransactionIntrinsicValueBalancesRecord {
        sprout_zat: balances.sprout_zat,
        sapling_zat: balances.sapling_zat,
        orchard_zat: balances.orchard_zat,
        ironwood_zat: balances.ironwood_zat,
    }
}

const fn intrinsic_value_balances_from_record(
    record: TransactionIntrinsicValueBalancesRecord,
) -> TransactionIntrinsicValueBalances {
    TransactionIntrinsicValueBalances::new(
        record.sprout_zat,
        record.sapling_zat,
        record.orchard_zat,
        record.ironwood_zat,
    )
}

fn transparent_input_record(input: TransparentInputFact) -> TransparentInputRecord {
    TransparentInputRecord {
        input_index: input.input_index,
        spent_outpoint: Some(TransparentOutPointRecord {
            transaction_id: encode_internal_transaction_id(input.spent_outpoint.transaction_id)
                .to_vec(),
            output_index: input.spent_outpoint.output_index,
        }),
    }
}

fn transparent_input_from_record(
    record: TransparentInputRecord,
) -> Result<TransparentInputFact, CanonicalBlockReplayDecodeError> {
    let outpoint = required(
        record.spent_outpoint,
        "payload.transactions.transparent_inputs.spent_outpoint",
    )?;
    Ok(TransparentInputFact::new(
        record.input_index,
        TransparentOutPoint::new(
            TransactionId::from_bytes(fixed_bytes(
                &outpoint.transaction_id,
                "payload.transactions.transparent_inputs.spent_outpoint.transaction_id",
            )?),
            outpoint.output_index,
        ),
    ))
}

fn transparent_output_record(output: &TransparentOutputFact) -> TransparentOutputRecord {
    TransparentOutputRecord {
        output_index: output.output_index,
        value_zat: output.value_zat,
        script_pub_key: output.script_pub_key.clone(),
        address_script_hash: output.address_script_hash.as_bytes().to_vec(),
    }
}

fn transparent_output_from_record(
    record: TransparentOutputRecord,
) -> Result<TransparentOutputFact, CanonicalBlockReplayDecodeError> {
    Ok(TransparentOutputFact::new(
        record.output_index,
        record.value_zat,
        record.script_pub_key,
        TransparentAddressScriptHash::from_bytes(fixed_bytes(
            &record.address_script_hash,
            "payload.transactions.transparent_outputs.address_script_hash",
        )?),
    ))
}

fn required<T>(
    required_value: Option<T>,
    field: &'static str,
) -> Result<T, CanonicalBlockReplayDecodeError> {
    required_value.ok_or(CanonicalBlockReplayDecodeError::MissingField { field })
}

fn fixed_bytes<const N: usize>(
    bytes: &[u8],
    field: &'static str,
) -> Result<[u8; N], CanonicalBlockReplayDecodeError> {
    <[u8; N]>::try_from(bytes).map_err(|_| CanonicalBlockReplayDecodeError::InvalidFieldLength {
        field,
        expected: N,
        actual: bytes.len(),
    })
}

#[derive(Clone, PartialEq, Message)]
struct CanonicalBlockReplayEnvelopeRecord {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
    #[prost(uint32, tag = "3")]
    digest_version: u32,
    #[prost(bytes = "vec", tag = "4")]
    reference_digest: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CanonicalBlockReplayV1Record {
    #[prost(message, optional, tag = "1")]
    block_header: Option<BlockHeaderRecord>,
    #[prost(message, repeated, tag = "2")]
    transactions: Vec<CanonicalTransactionRecord>,
    #[prost(bytes = "vec", tag = "3")]
    serialized_bytes_digest: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct BlockHeaderRecord {
    #[prost(uint32, tag = "1")]
    height: u32,
    #[prost(bytes = "vec", tag = "2")]
    block_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    parent_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    merkle_root_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    commitment_bytes: Vec<u8>,
    #[prost(sint64, tag = "6")]
    block_time: i64,
    #[prost(uint32, tag = "7")]
    bits: u32,
    #[prost(bytes = "vec", tag = "8")]
    nonce: Vec<u8>,
    #[prost(uint32, tag = "9")]
    version: u32,
    #[prost(uint64, tag = "10")]
    block_size_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CanonicalTransactionRecord {
    #[prost(message, optional, tag = "1")]
    public_facts: Option<TransactionPublicFactsRecord>,
    #[prost(message, optional, tag = "2")]
    intrinsic_value_balances: Option<TransactionIntrinsicValueBalancesRecord>,
    #[prost(message, repeated, tag = "3")]
    transparent_inputs: Vec<TransparentInputRecord>,
    #[prost(message, repeated, tag = "4")]
    transparent_outputs: Vec<TransparentOutputRecord>,
    #[prost(bytes = "vec", tag = "5")]
    serialized_bytes_digest: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct TransactionPublicFactsRecord {
    #[prost(bytes = "vec", tag = "1")]
    transaction_id: Vec<u8>,
    #[prost(bytes = "vec", optional, tag = "2")]
    auth_digest: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "3")]
    wtxid: Option<Vec<u8>>,
    #[prost(message, optional, tag = "4")]
    transaction_version: Option<TransactionVersionRecord>,
    #[prost(uint32, optional, tag = "5")]
    consensus_branch_id: Option<u32>,
    #[prost(message, optional, tag = "6")]
    lock_time: Option<LockTimeRecord>,
    #[prost(uint32, optional, tag = "7")]
    expiry_height: Option<u32>,
    #[prost(uint32, tag = "8")]
    size_bytes: u32,
    #[prost(message, optional, tag = "9")]
    counts: Option<TransactionComponentCountsRecord>,
    #[prost(sint64, optional, tag = "10")]
    orchard_value_balance_zat: Option<i64>,
    #[prost(bytes = "vec", optional, tag = "11")]
    orchard_anchor: Option<Vec<u8>>,
    #[prost(sint64, optional, tag = "12")]
    ironwood_value_balance_zat: Option<i64>,
    #[prost(uint32, tag = "13")]
    privacy_shape: u32,
    #[prost(bool, tag = "14")]
    is_coinbase: bool,
    #[prost(uint32, repeated, tag = "15")]
    unsupported_sections: Vec<u32>,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct TransactionVersionRecord {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(uint32, tag = "2")]
    effective_version: u32,
    #[prost(uint32, optional, tag = "3")]
    version_group_id: Option<u32>,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct LockTimeRecord {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(uint64, tag = "2")]
    value: u64,
}

#[allow(
    clippy::struct_field_names,
    reason = "protobuf field names mirror the complete canonical transaction vocabulary"
)]
#[derive(Clone, Copy, PartialEq, Message)]
struct TransactionComponentCountsRecord {
    #[prost(uint32, tag = "1")]
    transparent_input_count: u32,
    #[prost(uint32, tag = "2")]
    transparent_output_count: u32,
    #[prost(uint32, tag = "3")]
    sapling_spend_count: u32,
    #[prost(uint32, tag = "4")]
    sapling_output_count: u32,
    #[prost(uint32, tag = "5")]
    orchard_action_count: u32,
    #[prost(uint32, tag = "6")]
    ironwood_action_count: u32,
    #[prost(uint32, tag = "7")]
    sprout_joinsplit_count: u32,
}

#[allow(
    clippy::struct_field_names,
    reason = "protobuf field names mirror the transaction-intrinsic value-balance vocabulary"
)]
#[derive(Clone, Copy, PartialEq, Message)]
struct TransactionIntrinsicValueBalancesRecord {
    #[prost(sint64, tag = "1")]
    sprout_zat: i64,
    #[prost(sint64, tag = "2")]
    sapling_zat: i64,
    #[prost(sint64, tag = "3")]
    orchard_zat: i64,
    #[prost(sint64, tag = "4")]
    ironwood_zat: i64,
}

#[derive(Clone, PartialEq, Message)]
struct TransparentInputRecord {
    #[prost(uint32, tag = "1")]
    input_index: u32,
    #[prost(message, optional, tag = "2")]
    spent_outpoint: Option<TransparentOutPointRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct TransparentOutPointRecord {
    #[prost(bytes = "vec", tag = "1")]
    transaction_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    output_index: u32,
}

#[derive(Clone, PartialEq, Message)]
struct TransparentOutputRecord {
    #[prost(uint32, tag = "1")]
    output_index: u32,
    #[prost(uint64, tag = "2")]
    value_zat: u64,
    #[prost(bytes = "vec", tag = "3")]
    script_pub_key: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    address_script_hash: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use eyre::{Result, eyre};
    use prost::Message as _;
    use sha2::{Digest as _, Sha256};

    use super::{
        CanonicalBlockReplayDecodeError, CanonicalBlockReplayEnvelopeRecord,
        CanonicalBlockReplayFormatVersion, CanonicalBlockReplayV1Record,
        ValidatedCanonicalBlockReplay, decode_canonical_block_replay,
        encode_canonical_block_replay,
    };
    use crate::{
        AuthDigest, BlockHash, BlockHeaderArtifact, BlockHeight, CanonicalBlockFacts,
        CanonicalBlockFactsDigestVersion, CanonicalTransactionFacts, ConsensusBranchId, LockTime,
        PrivacyShape, SerializedBytesDigest, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
        TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
        TransparentOutputFact, UnsupportedSection, Wtxid,
    };

    #[test]
    fn replay_round_trip_preserves_complete_canonical_facts() -> Result<()> {
        let facts = rich_block_facts();
        let encoding = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::CURRENT,
            CanonicalBlockFactsDigestVersion::CURRENT,
        );
        let replay = decode_canonical_block_replay(encoding.as_bytes())?;

        assert_eq!(
            encoding.format_version(),
            CanonicalBlockReplayFormatVersion::V1
        );
        assert_eq!(replay.format_version(), encoding.format_version());
        assert_eq!(replay.reference_digest(), encoding.reference_digest());
        assert_eq!(replay.facts(), &facts);

        let reencoded = encode_canonical_block_replay(
            replay.facts(),
            replay.format_version(),
            replay.reference_digest().version(),
        );
        assert_eq!(reencoded, encoding);
        assert_eq!(replay.into_facts(), facts);
        Ok(())
    }

    #[test]
    fn encoder_wrapper_exposes_block_identity_without_changing_durable_bytes() -> Result<()> {
        let facts = rich_block_facts();
        let encoding = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );

        assert_eq!(encoding.block_height(), facts.block_header.height);
        assert_eq!(encoding.block_hash(), facts.block_header.block_hash);
        assert_eq!(encoding.parent_hash(), facts.block_header.parent_hash);

        // The added identity remains wrapper-only metadata: the existing
        // durable bytes still decode to the complete canonical facts.
        let replay = decode_canonical_block_replay(encoding.as_bytes())?;
        assert_eq!(replay.facts(), &facts);
        Ok(())
    }

    #[test]
    fn replay_v1_encoding_matches_its_golden_digest() {
        let encoding = encoded_replay();
        let actual_digest: [u8; 32] = Sha256::digest(encoding.as_bytes()).into();

        // This digest is the durable V1 byte contract. A semantic or wire
        // change requires a new replay format version, not an updated V1 hash.
        assert_eq!(
            actual_digest,
            [
                251, 106, 111, 219, 199, 45, 93, 29, 17, 92, 252, 135, 80, 43, 109, 39, 118, 117,
                118, 158, 235, 74, 30, 229, 78, 251, 238, 132, 172, 7, 41, 163,
            ]
        );
    }

    #[test]
    fn from_authenticated_facts_matches_decoded_replay() {
        let facts = rich_block_facts();
        let encoding = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        let direct = ValidatedCanonicalBlockReplay::from_authenticated_facts(
            facts.clone(),
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        assert_eq!(
            direct.format_version(),
            CanonicalBlockReplayFormatVersion::V1
        );
        assert_eq!(direct.reference_digest(), encoding.reference_digest());
        assert_eq!(direct.facts(), &facts);
    }

    #[test]
    fn replay_round_trip_preserves_every_version_privacy_and_lock_time_variant() -> Result<()> {
        let mut facts = rich_block_facts();
        let variants = [
            (
                TransactionVersion::V1,
                PrivacyShape::TransparentOnly,
                LockTime::Unlocked,
            ),
            (
                TransactionVersion::V2,
                PrivacyShape::Shielding,
                LockTime::Height(BlockHeight::new(0)),
            ),
            (
                TransactionVersion::V3,
                PrivacyShape::Deshielding,
                LockTime::UnixSeconds(0),
            ),
            (
                TransactionVersion::V4,
                PrivacyShape::ShieldedOnly,
                LockTime::Unlocked,
            ),
            (
                TransactionVersion::V5,
                PrivacyShape::Mixed,
                LockTime::Height(BlockHeight::new(u32::MAX)),
            ),
            (
                TransactionVersion::V6,
                PrivacyShape::Coinbase,
                LockTime::UnixSeconds(u64::MAX),
            ),
            (
                TransactionVersion::Unsupported {
                    effective_version: 7,
                    version_group_id: None,
                },
                PrivacyShape::ShieldedCoinbase,
                LockTime::Unlocked,
            ),
            (
                TransactionVersion::Unsupported {
                    effective_version: u32::MAX,
                    version_group_id: Some(0),
                },
                PrivacyShape::Unclassified,
                LockTime::Height(BlockHeight::new(1)),
            ),
        ];
        facts.transactions = variants
            .into_iter()
            .enumerate()
            .map(|(index, (version, privacy_shape, lock_time))| {
                variant_transaction(index, version, privacy_shape, lock_time)
            })
            .collect();

        let encoding = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        let replay = decode_canonical_block_replay(encoding.as_bytes())?;

        assert_eq!(replay.into_facts(), facts);
        Ok(())
    }

    #[test]
    fn replay_format_versions_fail_closed_when_unknown() {
        assert_eq!(
            CanonicalBlockReplayFormatVersion::try_from(1),
            Ok(CanonicalBlockReplayFormatVersion::V1)
        );
        assert!(CanonicalBlockReplayFormatVersion::try_from(0).is_err());
        assert!(CanonicalBlockReplayFormatVersion::try_from(2).is_err());
        assert!(CanonicalBlockReplayFormatVersion::try_from(3).is_err());
    }

    #[test]
    fn decode_rejects_invalid_protobuf_envelope() {
        assert!(matches!(
            decode_canonical_block_replay(&[0x0f]),
            Err(CanonicalBlockReplayDecodeError::InvalidEnvelope { .. })
        ));
    }

    #[test]
    fn decode_rejects_unknown_replay_format_version() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        envelope.format_version = 99;

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::UnsupportedFormatVersion {
                encoded_version: 99
            })
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_unknown_reference_digest_version() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        envelope.digest_version = u32::MAX;

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(
                CanonicalBlockReplayDecodeError::UnsupportedReferenceDigestVersion {
                    encoded_version: u32::MAX
                }
            )
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_invalid_protobuf_payload() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        envelope.payload = vec![0x0f];

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::InvalidPayload { .. })
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_noncanonical_payload_with_unknown_fields() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        // Unknown field 100, wire type varint, value 1. Prost ignores the
        // field semantically, while canonical re-encoding deliberately drops it.
        envelope.payload.extend_from_slice(&[0xa0, 0x06, 0x01]);

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::NonCanonicalPayload)
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_noncanonical_envelope_with_unknown_fields() {
        let encoding = encoded_replay();
        let mut bytes = encoding.into_bytes();
        // Unknown field 100, wire type varint, value 1.
        bytes.extend_from_slice(&[0xa0, 0x06, 0x01]);

        assert!(matches!(
            decode_canonical_block_replay(&bytes),
            Err(CanonicalBlockReplayDecodeError::NonCanonicalEnvelope)
        ));
    }

    #[test]
    fn decode_rejects_tampered_reference_digest() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        let first_digest_byte = envelope
            .reference_digest
            .first_mut()
            .ok_or_else(|| eyre!("test replay envelope unexpectedly omitted its digest"))?;
        *first_digest_byte ^= 0x80;

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::ReferenceDigestMismatch)
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_tampered_canonical_fact_payload() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        let mut payload = CanonicalBlockReplayV1Record::decode(envelope.payload.as_slice())?;
        let header = payload
            .block_header
            .as_mut()
            .ok_or_else(|| eyre!("test replay payload unexpectedly omitted its block header"))?;
        header.height = header.height.saturating_add(1);
        envelope.payload = payload.encode_to_vec();

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::ReferenceDigestMismatch)
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_invalid_fixed_width_fact() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        let mut payload = CanonicalBlockReplayV1Record::decode(envelope.payload.as_slice())?;
        let header = payload
            .block_header
            .as_mut()
            .ok_or_else(|| eyre!("test replay payload unexpectedly omitted its block header"))?;
        header.block_hash = vec![0x42; 31];
        envelope.payload = payload.encode_to_vec();

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::InvalidFieldLength {
                field: "payload.block_header.block_hash",
                expected: 32,
                actual: 31,
            })
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_unknown_nested_discriminant() -> Result<()> {
        let mut envelope = encoded_envelope()?;
        let mut payload = CanonicalBlockReplayV1Record::decode(envelope.payload.as_slice())?;
        let first_transaction = payload
            .transactions
            .first_mut()
            .ok_or_else(|| eyre!("test replay payload unexpectedly omitted transactions"))?;
        let public_facts = first_transaction
            .public_facts
            .as_mut()
            .ok_or_else(|| eyre!("test replay transaction unexpectedly omitted public facts"))?;
        public_facts.privacy_shape = 99;
        envelope.payload = payload.encode_to_vec();

        assert!(matches!(
            decode_canonical_block_replay(&envelope.encode_to_vec()),
            Err(CanonicalBlockReplayDecodeError::UnknownDiscriminant {
                field: "payload.transactions.public_facts.privacy_shape",
                discriminant: 99,
            })
        ));
        Ok(())
    }

    fn encoded_replay() -> super::CanonicalBlockReplayEnvelope {
        encode_canonical_block_replay(
            &rich_block_facts(),
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        )
    }

    fn encoded_envelope() -> Result<CanonicalBlockReplayEnvelopeRecord> {
        Ok(CanonicalBlockReplayEnvelopeRecord::decode(
            encoded_replay().as_bytes(),
        )?)
    }

    fn rich_block_facts() -> CanonicalBlockFacts {
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(987_654),
                BlockHash::from_bytes([0x11; 32]),
                BlockHash::from_bytes([0x22; 32]),
                [0x33; 32],
                [0x44; 32],
                -1_700_000_000,
                0x1f07_ffff,
                [0x55; 32],
                u32::MAX,
                2_000_000,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(b"rich block"),
            transactions: vec![rich_transaction(), sparse_transaction()],
        }
    }

    fn rich_transaction() -> CanonicalTransactionFacts {
        CanonicalTransactionFacts {
            public_facts: rich_public_facts(),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                b"rich transaction",
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::new(
                i64::MIN,
                -1,
                0,
                i64::MAX,
            ),
            transparent_inputs: vec![
                TransparentInputFact::new(
                    0,
                    TransparentOutPoint::new(TransactionId::from_bytes([0x71; 32]), u32::MAX),
                ),
                TransparentInputFact::new(9, TransparentOutPoint::COINBASE_SENTINEL),
            ],
            transparent_outputs: vec![
                TransparentOutputFact::new(
                    0,
                    0,
                    Vec::new(),
                    TransparentAddressScriptHash::from_bytes([0x72; 32]),
                ),
                TransparentOutputFact::new(
                    u32::MAX,
                    u64::MAX,
                    [0x51, 0x21, 0x02],
                    TransparentAddressScriptHash::from_bytes([0x73; 32]),
                ),
            ],
        }
    }

    fn rich_public_facts() -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id: TransactionId::from_bytes([0x61; 32]),
            auth_digest: Some(AuthDigest::from_bytes([0x62; 32])),
            wtxid: Some(Wtxid::from_bytes([0x63; 64])),
            version: TransactionVersion::V6,
            consensus_branch_id: Some(ConsensusBranchId::new(0)),
            lock_time: LockTime::Height(BlockHeight::new(0)),
            expiry_height: Some(BlockHeight::new(0)),
            size_bytes: 2_000_000,
            counts: TransactionComponentCounts {
                transparent_input_count: 2,
                transparent_output_count: 2,
                sapling_spend_count: 3,
                sapling_output_count: 4,
                orchard_action_count: 5,
                ironwood_action_count: 6,
                sprout_joinsplit_count: 7,
            },
            orchard_value_balance_zat: Some(-42),
            orchard_anchor: Some([0x64; 32]),
            ironwood_value_balance_zat: Some(0),
            privacy_shape: PrivacyShape::Mixed,
            is_coinbase: false,
            unsupported_sections: vec![
                UnsupportedSection::FutureVersionHeader,
                UnsupportedSection::FutureShieldedProtocol,
            ],
        }
    }

    fn sparse_transaction() -> CanonicalTransactionFacts {
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: TransactionId::from_bytes([0x81; 32]),
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::Unsupported {
                    effective_version: u32::MAX,
                    version_group_id: Some(0),
                },
                consensus_branch_id: None,
                lock_time: LockTime::UnixSeconds(u64::MAX),
                expiry_height: None,
                size_bytes: 0,
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: true,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                b"sparse transaction",
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        }
    }

    fn variant_transaction(
        index: usize,
        version: TransactionVersion,
        privacy_shape: PrivacyShape,
        lock_time: LockTime,
    ) -> CanonicalTransactionFacts {
        let marker = u8::try_from(index).unwrap_or(u8::MAX).saturating_add(1);
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: TransactionId::from_bytes([marker; 32]),
                auth_digest: None,
                wtxid: None,
                version,
                consensus_branch_id: None,
                lock_time,
                expiry_height: None,
                size_bytes: marker.into(),
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[marker]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        }
    }
}
