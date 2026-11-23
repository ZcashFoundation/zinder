//! Deterministic source-block artifact builders.

use std::{
    fmt,
    io::{Cursor, Read as _},
    sync::Arc,
    time::Instant,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zebra_chain::{
    block::{
        Block as ZebraBlock, Header as ZebraBlockHeader, MAX_BLOCK_BYTES,
        merkle::Root as ZebraMerkleRoot,
    },
    serialization::{
        CompactSizeMessage, SerializationError, TrustedPreallocate, ZcashDeserializeInto,
        ZcashSerialize as _,
    },
    transaction::{MIN_TRANSPARENT_TX_SIZE, Transaction as ZebraTransaction},
    transparent::Input as ZebraTransparentInput,
};
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockReplayEnvelope,
    CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainTipMetadata,
    CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput, CompactSaplingSpend,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, CompactTransparentInput,
    CompactTransparentOutput, NetworkUpgradeActivations, SerializedBytesDigest, ShieldedProtocol,
    TransactionBlobArtifact, TransactionId, TransactionLocation, encode_canonical_block_replay,
    wire::encode_rpc_block_hash_hex,
};
use zinder_source::{SourceBlock, transaction_public_fact_set_from_parsed};
use zinder_store::RawBlobRetention;

const COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN: usize = 52;

/// Configured retention policy for raw consensus block and transaction blobs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RawBlobPolicy {
    /// Write no raw block or transaction blobs.
    None,
    /// Write raw transaction blobs only.
    Transactions,
    /// Write both raw block blobs and raw transaction blobs.
    All,
}

impl RawBlobPolicy {
    /// Returns the config spelling for this policy.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
        self.to_retention().as_kebab_case()
    }

    /// Maps the configured policy to the immutable store retention contract
    /// used for capability discovery and commit validation.
    #[must_use]
    pub const fn to_retention(self) -> RawBlobRetention {
        match self {
            Self::None => RawBlobRetention::None,
            Self::Transactions => RawBlobRetention::Transactions,
            Self::All => RawBlobRetention::All,
        }
    }
}

impl From<RawBlobRetention> for RawBlobPolicy {
    fn from(retention: RawBlobRetention) -> Self {
        match retention {
            RawBlobRetention::None => Self::None,
            RawBlobRetention::Transactions => Self::Transactions,
            RawBlobRetention::All => Self::All,
        }
    }
}

/// Error returned while constructing canonical blocks from source blocks.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalBlockConstructionError {
    /// Source block payload is empty.
    #[error("source block payload is empty")]
    EmptySourcePayload,

    /// Zebra-chain consensus block parse failed.
    #[error("zebra-chain block parse failed: {source}")]
    BlockParseFailed {
        /// Underlying parse error.
        #[source]
        source: zebra_chain::serialization::SerializationError,
    },

    /// The block parser left bytes after the one complete consensus block.
    #[error(
        "parsed block consumed {consumed_byte_count} of {source_byte_count} source payload bytes"
    )]
    SourcePayloadNotFullyConsumed {
        /// Bytes consumed by the consensus block parser.
        consumed_byte_count: usize,
        /// Bytes supplied by the source adapter.
        source_byte_count: usize,
    },

    /// Parsed block is missing the coinbase height.
    #[error("parsed block is missing coinbase height")]
    ParsedBlockMissingCoinbaseHeight,

    /// Parsed block field disagrees with the source-supplied value.
    #[error("parsed {field} {actual} does not match source {field} {expected}")]
    SourceBlockMismatch {
        /// Block identity field that disagrees.
        field: BlockMismatchField,
        /// Source-supplied value.
        expected: String,
        /// Parsed value.
        actual: String,
    },

    /// Commitment-tree size advanced past `u32::MAX`.
    #[error("{protocol:?} commitment tree size overflowed u32")]
    CommitmentTreeOverflow {
        /// Protocol whose tree overflowed.
        protocol: ShieldedProtocol,
    },

    /// A counted field cannot be encoded as u32.
    #[error("{field} does not fit u32")]
    CountOverflow {
        /// Counted field name.
        field: &'static str,
    },

    /// Compact note ciphertext is shorter than the native wallet-scan prefix.
    #[error("compact note ciphertext is shorter than {COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN} bytes")]
    CompactCiphertextTooShort,

    /// Transparent input previous transaction id has the wrong length.
    #[error("transparent input prevout transaction id is {byte_count} bytes, expected 32")]
    TransparentOutputTransactionIdMalformed {
        /// Observed byte count.
        byte_count: usize,
    },

    /// Transaction index cannot be represented as `u32`.
    #[error("transaction index does not fit u32")]
    TransactionIndexOverflow,

    /// Parsed transactions and prepared canonical facts lost their one-to-one order.
    #[error(
        "parsed transaction count {parsed_transaction_count} does not match canonical fact count {canonical_fact_count}"
    )]
    TransactionFactCountMismatch {
        /// Transactions owned by the parsed consensus block.
        parsed_transaction_count: usize,
        /// Canonical fact rows prepared from that same block.
        canonical_fact_count: usize,
    },

    /// Structured compact-block invariants failed during construction.
    #[error("compact block construction failed: {source}")]
    CompactBlockInvalid {
        /// Underlying compact-block invariant error.
        #[from]
        source: zinder_core::CompactBlockArtifactError,
    },

    /// Parsing typed transaction facts failed.
    #[error("transaction fact parse failed: {reason}")]
    TransactionFactsParseFailed {
        /// Parser failure reason.
        reason: String,
    },

    /// Parsed transaction sizes cannot be mapped back to the source block bytes.
    #[error("parsed transaction byte ranges exceed the {block_byte_count}-byte source block")]
    TransactionByteRangeMismatch {
        /// Source block byte length.
        block_byte_count: usize,
    },

    /// Serializing the parsed block header back to canonical bytes failed.
    #[error("block header serialization failed: {source}")]
    BlockHeaderSerializationFailed {
        /// Underlying serialization error.
        #[source]
        source: std::io::Error,
    },
}

/// Block identity field whose source/parsed values disagree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BlockMismatchField {
    /// Block height.
    Height,
    /// Block hash.
    Hash,
    /// Parent block hash.
    ParentHash,
    /// Block time.
    Time,
}

impl fmt::Display for BlockMismatchField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Height => formatter.write_str("block height"),
            Self::Hash => formatter.write_str("block hash"),
            Self::ParentHash => formatter.write_str("parent hash"),
            Self::Time => formatter.write_str("block time"),
        }
    }
}

/// Shielded commitment-tree positions at one block boundary.
///
/// `prepare_canonical_block` returns a per-block delta; `position_canonical_block` folds
/// those deltas into the running position. The same type expresses both
/// shapes, so callers can carry one running offset through a serial loop
/// or seed it from a `ChainTipMetadata` recovered from the store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitmentTreeSizes {
    /// Sapling output count contributed by (or accumulated up to) this point.
    pub sapling: u32,
    /// Orchard action count contributed by (or accumulated up to) this point.
    pub orchard: u32,
    /// Ironwood action count contributed by (or accumulated up to) this point.
    pub ironwood: u32,
}

impl CommitmentTreeSizes {
    /// Seeds a running offset from chain-tip metadata recovered from the
    /// store.
    #[must_use]
    pub const fn from_tip_metadata(tip_metadata: ChainTipMetadata) -> Self {
        Self {
            sapling: tip_metadata.sapling_commitment_tree_size,
            orchard: tip_metadata.orchard_commitment_tree_size,
            ironwood: tip_metadata.ironwood_commitment_tree_size,
        }
    }

    /// Sums two positions; errors if any pool overflows `u32`.
    pub fn checked_add(self, additions: Self) -> Result<Self, CanonicalBlockConstructionError> {
        let sapling = self.sapling.checked_add(additions.sapling).ok_or(
            CanonicalBlockConstructionError::CommitmentTreeOverflow {
                protocol: ShieldedProtocol::Sapling,
            },
        )?;
        let orchard = self.orchard.checked_add(additions.orchard).ok_or(
            CanonicalBlockConstructionError::CommitmentTreeOverflow {
                protocol: ShieldedProtocol::Orchard,
            },
        )?;
        let ironwood = self.ironwood.checked_add(additions.ironwood).ok_or(
            CanonicalBlockConstructionError::CommitmentTreeOverflow {
                protocol: ShieldedProtocol::Ironwood,
            },
        )?;

        Ok(Self {
            sapling,
            orchard,
            ironwood,
        })
    }

    /// Lowers to native compact-block chain metadata.
    #[must_use]
    pub const fn chain_metadata(self) -> CompactChainMetadata {
        CompactChainMetadata {
            sapling_commitment_tree_size: self.sapling,
            orchard_commitment_tree_size: self.orchard,
            ironwood_commitment_tree_size: self.ironwood,
        }
    }

    /// Lowers to the canonical `ChainTipMetadata` stored alongside each
    /// chain epoch.
    #[must_use]
    pub const fn tip_metadata(self) -> ChainTipMetadata {
        ChainTipMetadata::new(self.sapling, self.orchard, self.ironwood)
    }
}

/// Parallel-safe preparation output for one source block.
///
/// [`prepare_canonical_block`] populates every field that depends only on the
/// block content. [`position_canonical_block`] stamps the position-dependent fields
/// (the `chain_metadata` inside `partial_compact_block` and the
/// `tip_metadata` in the returned [`PositionedCanonicalBlock`]) after folding
/// `tree_size_additions` into the running commitment-tree position.
#[derive(Debug)]
pub struct PreparedCanonicalBlock {
    /// Immutable facts computed from this source block alone.
    pub facts: CanonicalBlockFacts,
    /// Versioned materialized-view replay bytes encoded on the parallel prepare lane.
    pub replay_envelope: CanonicalBlockReplayEnvelope,
    /// Retained raw blobs kept outside semantic fact identity.
    pub retained_raw_blobs: RetainedRawBlobs,
    /// Structured compact block whose zero metadata is replaced by the
    /// serial folder's final tree-size position.
    pub partial_compact_block: CompactBlockArtifact,
    /// Commitment-tree-size delta this block contributes.
    pub tree_size_additions: CommitmentTreeSizes,
}

/// Retained consensus raw blobs for one canonical block.
///
/// These blobs are physically and semantically separate from the replay
/// envelope, so changing deployment retention does not change canonical digests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetainedRawBlobs {
    /// Optional serialized consensus block artifact.
    pub block_blob: Option<BlockBlobArtifact>,
    /// Optional serialized consensus transaction artifacts.
    pub transaction_blobs: Vec<TransactionBlobArtifact>,
}

/// Prepared canonical facts placed at an ordered commitment-tree position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionedCanonicalBlock {
    /// Immutable block-local materialized-view facts.
    pub facts: CanonicalBlockFacts,
    /// Versioned replay envelope for `facts`.
    pub replay_envelope: CanonicalBlockReplayEnvelope,
    /// Retained raw blobs kept outside the replay envelope.
    pub retained_raw_blobs: RetainedRawBlobs,
    /// Native compact block with final chain metadata stamped.
    pub compact_block: CompactBlockArtifact,
    /// Running commitment-tree position after this block.
    pub tip_metadata: ChainTipMetadata,
}

/// Parses one source block and prepares its block-local canonical facts.
///
/// Preparation is parallel-safe because the returned value does not depend on
/// the block's position in the running chain state.
///
/// The output's compact block carries zero placeholder tree sizes;
/// [`position_canonical_block`] stamps the final commitment-tree position
/// before encoding the proto and validating against any source-supplied
/// observation.
pub fn prepare_canonical_block(
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    raw_blob_policy: RawBlobPolicy,
) -> Result<PreparedCanonicalBlock, CanonicalBlockConstructionError> {
    prepare_canonical_block_for_retention(source_block, activations, raw_blob_policy.to_retention())
}

#[allow(
    clippy::too_many_lines,
    reason = "one parse-and-construct pipeline keeps stage timing and field ownership auditable"
)]
pub(crate) fn prepare_canonical_block_for_retention(
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    raw_blob_retention: RawBlobRetention,
) -> Result<PreparedCanonicalBlock, CanonicalBlockConstructionError> {
    validate_source_block_payload(source_block)?;
    let ParsedSourceBlock {
        block: parsed_block,
        transaction_layout,
    } = measure_canonical_construction_stage("block_parse", || parse_source_block(source_block))?;
    measure_canonical_construction_stage("identity_validation", || {
        validate_parsed_block_identity(&parsed_block, source_block)
    })?;
    let prepared_transactions = measure_canonical_construction_stage("transaction_facts", || {
        prepare_canonical_transactions_from_parsed(
            &parsed_block,
            &transaction_layout,
            source_block,
            activations,
            raw_blob_retention.retains_transaction_blobs(),
        )
    })?;
    let (compact_transactions, tree_size_additions) =
        measure_canonical_construction_stage("compact_artifacts", || {
            compact_transactions(&parsed_block, &prepared_transactions.facts)
        })?;
    let block_header = measure_canonical_construction_stage("block_header_artifact", || {
        Ok(block_header_artifact_from_parsed_block(
            &parsed_block,
            source_block.height,
            usize_to_u64_saturating(source_block.raw_block_bytes.len()),
        ))
    })?;
    let block_blob = measure_canonical_construction_stage("block_blob", || {
        Ok(if raw_blob_retention.retains_block_blobs() {
            Some(BlockBlobArtifact::new(
                block_header.height,
                block_header.block_hash,
                block_header.parent_hash,
                source_block.raw_block_bytes.clone(),
            ))
        } else {
            record_raw_blob_disabled("block_blob", 1);
            None
        })
    })?;
    let partial_compact_block = CompactBlockArtifact::new(
        BlockId::new(source_block.height, source_block.hash),
        source_block.parent_hash,
        source_block.block_time_seconds,
        compact_transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?;

    let facts = CanonicalBlockFacts {
        block_header,
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &source_block.raw_block_bytes,
        ),
        transactions: prepared_transactions.facts,
    };
    let replay_envelope = measure_canonical_construction_stage("block_replay", || {
        Ok(encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::CURRENT,
            CanonicalBlockFactsDigestVersion::CURRENT,
        ))
    })?;

    Ok(PreparedCanonicalBlock {
        facts,
        replay_envelope,
        retained_raw_blobs: RetainedRawBlobs {
            block_blob,
            transaction_blobs: prepared_transactions.transaction_blobs,
        },
        partial_compact_block,
        tree_size_additions,
    })
}

fn block_header_artifact_from_parsed_block(
    parsed_block: &ZebraBlock,
    height: zinder_core::BlockHeight,
    block_size_bytes: u64,
) -> BlockHeaderArtifact {
    let header = &parsed_block.header;
    let ZebraMerkleRoot(merkle_root_hash) = header.merkle_root;
    BlockHeaderArtifact::new(
        height,
        BlockHash::from_bytes(parsed_block.hash().0),
        BlockHash::from_bytes(header.previous_block_hash.0),
        merkle_root_hash,
        *header.commitment_bytes,
        header.time.timestamp(),
        u32::from_be_bytes(header.difficulty_threshold.bytes_in_display_order()),
        *header.nonce,
        header.version,
        block_size_bytes,
    )
}

/// Positions one prepared block at its ordered commitment-tree position.
///
/// Applies `prepared.tree_size_additions` to `running_tree_sizes`, stamps
/// the final `chain_metadata` into the compact block, and returns the
/// positioned [`PositionedCanonicalBlock`].
/// Mutates `running_tree_sizes` in place so a sequential loop can carry
/// it forward across blocks.
pub fn position_canonical_block(
    prepared: PreparedCanonicalBlock,
    running_tree_sizes: &mut CommitmentTreeSizes,
) -> Result<PositionedCanonicalBlock, CanonicalBlockConstructionError> {
    let PreparedCanonicalBlock {
        facts,
        replay_envelope,
        retained_raw_blobs,
        partial_compact_block,
        tree_size_additions,
    } = prepared;

    let final_tree_sizes = running_tree_sizes.checked_add(tree_size_additions)?;

    let parts = partial_compact_block.into_parts();
    let compact_block = CompactBlockArtifact::new(
        parts.block_id,
        parts.previous_block_hash,
        parts.time,
        parts.transactions,
        final_tree_sizes.chain_metadata(),
    )?;
    *running_tree_sizes = final_tree_sizes;

    Ok(PositionedCanonicalBlock {
        facts,
        replay_envelope,
        retained_raw_blobs,
        compact_block,
        tip_metadata: final_tree_sizes.tip_metadata(),
    })
}

struct PreparedCanonicalTransactions {
    facts: Vec<CanonicalTransactionFacts>,
    transaction_blobs: Vec<TransactionBlobArtifact>,
}

fn prepare_canonical_transactions_from_parsed(
    parsed_block: &ZebraBlock,
    transaction_layout: &TransactionByteLayout,
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    retain_transaction_blobs: bool,
) -> Result<PreparedCanonicalTransactions, CanonicalBlockConstructionError> {
    let mut transactions = Vec::with_capacity(parsed_block.transactions.len());
    let mut transaction_blobs = Vec::new();
    validate_transaction_byte_layout(
        transaction_layout,
        parsed_block.transactions.len(),
        source_block.raw_block_bytes.len(),
    )?;
    let mut transaction_byte_offset = transaction_layout.first_transaction_offset;

    for ((tx_index_in_block, transaction), &serialized_size) in parsed_block
        .transactions
        .iter()
        .enumerate()
        .zip(&transaction_layout.transaction_sizes)
    {
        let tx_index_in_block = u32::try_from(tx_index_in_block)
            .map_err(|_| CanonicalBlockConstructionError::TransactionIndexOverflow)?;
        let raw_transaction_bytes = take_transaction_bytes(
            &source_block.raw_block_bytes,
            &mut transaction_byte_offset,
            serialized_size,
        )?;
        let fact_set = transaction_public_fact_set_from_parsed(
            transaction,
            raw_transaction_bytes.len(),
            Some(source_block.height),
            activations,
        )
        .map_err(|source| {
            CanonicalBlockConstructionError::TransactionFactsParseFailed {
                reason: source.to_string(),
            }
        })?;
        if retain_transaction_blobs {
            transaction_blobs.push(TransactionBlobArtifact::new(
                TransactionLocation::new(
                    fact_set.public_facts.transaction_id,
                    source_block.height,
                    source_block.hash,
                    tx_index_in_block,
                ),
                raw_transaction_bytes.to_vec(),
            ));
        } else {
            record_raw_blob_disabled("transaction_blob", 1);
        }
        transactions.push(CanonicalTransactionFacts {
            public_facts: fact_set.public_facts,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                raw_transaction_bytes,
            ),
            intrinsic_value_balances: fact_set.intrinsic_value_balances,
            transparent_inputs: fact_set.transparent_inputs,
            transparent_outputs: fact_set.transparent_outputs,
        });
    }

    if transaction_byte_offset != source_block.raw_block_bytes.len() {
        return Err(
            CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                block_byte_count: source_block.raw_block_bytes.len(),
            },
        );
    }

    Ok(PreparedCanonicalTransactions {
        facts: transactions,
        transaction_blobs,
    })
}

/// Where each transaction's bytes sit in the source block.
///
/// The block parse records these as it consumes the bytes, so a slice is
/// exactly the bytes the parser read for its transaction.
struct TransactionByteLayout {
    /// Bytes before the first transaction: the header and transaction count.
    first_transaction_offset: usize,
    /// Bytes the parser consumed for each transaction, in block order.
    transaction_sizes: Vec<usize>,
}

/// Checks that the recorded transaction byte ranges tile the block's
/// transaction region: one range per parsed transaction, starting right
/// after the transaction count and ending at the last block byte.
fn validate_transaction_byte_layout(
    transaction_layout: &TransactionByteLayout,
    parsed_transaction_count: usize,
    block_byte_count: usize,
) -> Result<(), CanonicalBlockConstructionError> {
    let layout_mismatch =
        CanonicalBlockConstructionError::TransactionByteRangeMismatch { block_byte_count };
    if transaction_layout.transaction_sizes.len() != parsed_transaction_count {
        return Err(layout_mismatch);
    }
    let transaction_region_end = transaction_layout
        .transaction_sizes
        .iter()
        .try_fold(transaction_layout.first_transaction_offset, |end, size| {
            end.checked_add(*size)
        });
    if transaction_region_end != Some(block_byte_count) {
        return Err(layout_mismatch);
    }
    Ok(())
}

fn take_transaction_bytes<'a>(
    raw_block_bytes: &'a [u8],
    byte_offset: &mut usize,
    transaction_byte_count: usize,
) -> Result<&'a [u8], CanonicalBlockConstructionError> {
    let transaction_byte_end = byte_offset.checked_add(transaction_byte_count).ok_or(
        CanonicalBlockConstructionError::TransactionByteRangeMismatch {
            block_byte_count: raw_block_bytes.len(),
        },
    )?;
    let transaction_bytes = raw_block_bytes
        .get(*byte_offset..transaction_byte_end)
        .ok_or(
            CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                block_byte_count: raw_block_bytes.len(),
            },
        )?;
    *byte_offset = transaction_byte_end;
    Ok(transaction_bytes)
}

fn record_raw_blob_disabled(table: &'static str, row_count: u64) {
    metrics::counter!("zinder_ingest_raw_blob_disabled_total", "table" => table)
        .increment(row_count);
}

fn record_canonical_construction_stage<T>(
    stage: &'static str,
    started_at: Instant,
    outcome: &Result<T, CanonicalBlockConstructionError>,
) {
    metrics::histogram!(
        "zinder_ingest_canonical_block_construction_stage_duration_seconds",
        "stage" => stage,
        "status" => if outcome.is_ok() { "ok" } else { "error" }
    )
    .record(started_at.elapsed());
}

fn measure_canonical_construction_stage<T>(
    stage: &'static str,
    operation: impl FnOnce() -> Result<T, CanonicalBlockConstructionError>,
) -> Result<T, CanonicalBlockConstructionError> {
    let started_at = Instant::now();
    let outcome = operation();
    record_canonical_construction_stage(stage, started_at, &outcome);
    outcome
}

fn usize_to_u64_saturating(amount: usize) -> u64 {
    u64::try_from(amount).unwrap_or(u64::MAX)
}

fn validate_source_block_payload(
    source_block: &SourceBlock,
) -> Result<(), CanonicalBlockConstructionError> {
    if source_block.raw_block_bytes.is_empty() {
        return Err(CanonicalBlockConstructionError::EmptySourcePayload);
    }

    Ok(())
}

/// A parsed source block with the byte range of each of its transactions.
struct ParsedSourceBlock {
    block: ZebraBlock,
    transaction_layout: TransactionByteLayout,
}

/// Parses the block as `zebra_chain`'s `Block` deserializer does, recording
/// how many bytes each transaction consumed.
///
/// The limits match `Block::zcash_deserialize` in zebra-chain 16: at most
/// `MAX_BLOCK_BYTES`, a transaction count within `max_allocation`, and
/// transactions that average at least `MIN_TRANSPARENT_TX_SIZE` bytes. Each
/// transaction goes through zebra-chain's own deserializer and its checks.
fn parse_source_block(
    source_block: &SourceBlock,
) -> Result<ParsedSourceBlock, CanonicalBlockConstructionError> {
    let parse_failed = |source| CanonicalBlockConstructionError::BlockParseFailed { source };
    let source_byte_count = source_block.raw_block_bytes.len();
    let mut reader = Cursor::new(source_block.raw_block_bytes.as_slice()).take(MAX_BLOCK_BYTES);
    let header: Arc<ZebraBlockHeader> = (&mut reader)
        .zcash_deserialize_into()
        .map_err(parse_failed)?;
    let transaction_count: CompactSizeMessage = (&mut reader)
        .zcash_deserialize_into()
        .map_err(parse_failed)?;
    let transaction_count = u64::try_from(usize::from(transaction_count)).map_err(|_| {
        parse_failed(SerializationError::Parse(
            "transaction count doesn't fit in a u64",
        ))
    })?;
    if transaction_count > ZebraTransaction::max_allocation() {
        return Err(parse_failed(SerializationError::Parse(
            "Vector longer than max_allocation",
        )));
    }

    let remaining_at_first_transaction = reader.limit();
    let first_transaction_offset = bytes_consumed(&reader);
    let mut transactions = Vec::new();
    let mut transaction_sizes = Vec::new();
    for parsed_count in 1..=transaction_count {
        let remaining_before_transaction = reader.limit();
        let transaction: Arc<ZebraTransaction> = (&mut reader)
            .zcash_deserialize_into()
            .map_err(parse_failed)?;
        let transaction_size =
            usize::try_from(remaining_before_transaction - reader.limit()).unwrap_or(usize::MAX);
        debug_assert_eq!(
            transaction.zcash_serialized_size(),
            transaction_size,
            "a transaction's parsed byte count must equal its serialized size",
        );
        transactions.push(transaction);
        transaction_sizes.push(transaction_size);
        if remaining_at_first_transaction - reader.limit() < parsed_count * MIN_TRANSPARENT_TX_SIZE
        {
            return Err(parse_failed(SerializationError::Parse(
                "block transactions are smaller than the smallest valid transaction",
            )));
        }
    }

    let consumed_byte_count = bytes_consumed(&reader);
    if consumed_byte_count != source_byte_count {
        return Err(
            CanonicalBlockConstructionError::SourcePayloadNotFullyConsumed {
                consumed_byte_count,
                source_byte_count,
            },
        );
    }
    Ok(ParsedSourceBlock {
        block: ZebraBlock {
            header,
            transactions,
        },
        transaction_layout: TransactionByteLayout {
            first_transaction_offset,
            transaction_sizes,
        },
    })
}

/// Bytes a `MAX_BLOCK_BYTES`-limited reader has consumed so far.
fn bytes_consumed<R>(reader: &std::io::Take<R>) -> usize {
    usize::try_from(MAX_BLOCK_BYTES - reader.limit()).unwrap_or(usize::MAX)
}

fn validate_parsed_block_identity(
    parsed_block: &ZebraBlock,
    source_block: &SourceBlock,
) -> Result<(), CanonicalBlockConstructionError> {
    let parsed_height = parsed_block
        .coinbase_height()
        .ok_or(CanonicalBlockConstructionError::ParsedBlockMissingCoinbaseHeight)?
        .0;
    if parsed_height != source_block.height.value() {
        return Err(CanonicalBlockConstructionError::SourceBlockMismatch {
            field: BlockMismatchField::Height,
            expected: source_block.height.value().to_string(),
            actual: parsed_height.to_string(),
        });
    }

    let parsed_hash = parsed_block.hash().0;
    if parsed_hash != source_block.hash.as_bytes() {
        return Err(CanonicalBlockConstructionError::SourceBlockMismatch {
            field: BlockMismatchField::Hash,
            expected: format_block_hash(source_block.hash.as_bytes()),
            actual: format_block_hash(parsed_hash),
        });
    }

    let parsed_parent_hash = parsed_block.header.previous_block_hash.0;
    if parsed_parent_hash != source_block.parent_hash.as_bytes() {
        return Err(CanonicalBlockConstructionError::SourceBlockMismatch {
            field: BlockMismatchField::ParentHash,
            expected: format_block_hash(source_block.parent_hash.as_bytes()),
            actual: format_block_hash(parsed_parent_hash),
        });
    }

    let parsed_time = u32::try_from(parsed_block.header.time.timestamp()).map_err(|_| {
        CanonicalBlockConstructionError::CountOverflow {
            field: "parsed block time",
        }
    })?;
    if parsed_time != source_block.block_time_seconds {
        return Err(CanonicalBlockConstructionError::SourceBlockMismatch {
            field: BlockMismatchField::Time,
            expected: source_block.block_time_seconds.to_string(),
            actual: parsed_time.to_string(),
        });
    }

    Ok(())
}

fn compact_transactions(
    parsed_block: &ZebraBlock,
    canonical_transactions: &[CanonicalTransactionFacts],
) -> Result<(Vec<CompactTransaction>, CommitmentTreeSizes), CanonicalBlockConstructionError> {
    if parsed_block.transactions.len() != canonical_transactions.len() {
        return Err(
            CanonicalBlockConstructionError::TransactionFactCountMismatch {
                parsed_transaction_count: parsed_block.transactions.len(),
                canonical_fact_count: canonical_transactions.len(),
            },
        );
    }
    let mut compact_transactions = Vec::new();
    let mut tree_size_additions = CommitmentTreeSizes::default();

    for (transaction_index, (transaction, canonical_transaction)) in parsed_block
        .transactions
        .iter()
        .zip(canonical_transactions)
        .enumerate()
    {
        let compact_transaction = compact_transaction_with_transaction_id(
            u64::try_from(transaction_index).map_err(|_| {
                CanonicalBlockConstructionError::CountOverflow {
                    field: "transaction index",
                }
            })?,
            canonical_transaction.public_facts.transaction_id,
            transaction.as_ref(),
        )?;
        tree_size_additions = tree_size_additions.checked_add(CommitmentTreeSizes {
            sapling: count_to_u32(
                compact_transaction.data.sapling_outputs.len(),
                "Sapling output count",
            )?,
            orchard: count_to_u32(
                compact_transaction.data.orchard_actions.len(),
                "Orchard action count",
            )?,
            ironwood: count_to_u32(
                compact_transaction.data.ironwood_actions.len(),
                "Ironwood action count",
            )?,
        })?;

        if compact_transaction_has_payload(&compact_transaction) {
            compact_transactions.push(compact_transaction);
        }
    }

    Ok((compact_transactions, tree_size_additions))
}

/// Builds Zinder-native wallet scan data from a parsed Zebra transaction.
pub(crate) fn compact_transaction_data(
    transaction: &ZebraTransaction,
) -> Result<CompactTransactionData, CanonicalBlockConstructionError> {
    Ok(CompactTransactionData {
        fee_zat: None,
        sapling_spends: compact_sapling_spends(transaction),
        sapling_outputs: compact_sapling_outputs(transaction)?,
        orchard_actions: compact_orchard_actions(transaction)?,
        ironwood_actions: compact_ironwood_actions(transaction)?,
        transparent_inputs: compact_transparent_inputs(transaction),
        transparent_outputs: compact_transparent_outputs(transaction),
    })
}

fn compact_transaction_with_transaction_id(
    index: u64,
    transaction_id: TransactionId,
    transaction: &ZebraTransaction,
) -> Result<CompactTransaction, CanonicalBlockConstructionError> {
    Ok(CompactTransaction {
        index,
        transaction_id,
        data: compact_transaction_data(transaction)?,
    })
}

fn compact_sapling_spends(transaction: &ZebraTransaction) -> Vec<CompactSaplingSpend> {
    transaction
        .sapling_spends()
        .map(|spend| CompactSaplingSpend {
            nullifier: spend.nullifier().0,
        })
        .collect()
}

fn compact_sapling_outputs(
    transaction: &ZebraTransaction,
) -> Result<Vec<CompactSaplingOutput>, CanonicalBlockConstructionError> {
    transaction
        .sapling_outputs()
        .map(|output| {
            Ok(CompactSaplingOutput {
                commitment: output.cmu().to_bytes(),
                ephemeral_key: output.ephemeral_key().0,
                ciphertext: compact_note_ciphertext_prefix(output.enc_ciphertext())?,
            })
        })
        .collect()
}

fn compact_orchard_actions(
    transaction: &ZebraTransaction,
) -> Result<Vec<CompactShieldedAction>, CanonicalBlockConstructionError> {
    transaction
        .orchard_actions()
        .map(|action| {
            let encrypted_note = action.encrypted_note();
            Ok(CompactShieldedAction {
                nullifier: action.nullifier().to_bytes(),
                commitment: action.cmx().to_bytes(),
                ephemeral_key: encrypted_note.epk_bytes,
                ciphertext: compact_note_ciphertext_prefix(encrypted_note.enc_ciphertext.as_ref())?,
            })
        })
        .collect()
}

fn compact_ironwood_actions(
    transaction: &ZebraTransaction,
) -> Result<Vec<CompactShieldedAction>, CanonicalBlockConstructionError> {
    transaction
        .ironwood_actions()
        .map(|action| {
            let encrypted_note = action.encrypted_note();
            Ok(CompactShieldedAction {
                nullifier: action.nullifier().to_bytes(),
                commitment: action.cmx().to_bytes(),
                ephemeral_key: encrypted_note.epk_bytes,
                ciphertext: compact_note_ciphertext_prefix(encrypted_note.enc_ciphertext.as_ref())?,
            })
        })
        .collect()
}

fn compact_note_ciphertext_prefix(
    ciphertext: &[u8],
) -> Result<[u8; COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN], CanonicalBlockConstructionError> {
    ciphertext
        .get(..COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN)
        .and_then(|prefix| prefix.try_into().ok())
        .ok_or(CanonicalBlockConstructionError::CompactCiphertextTooShort)
}

fn compact_transparent_inputs(transaction: &ZebraTransaction) -> Vec<CompactTransparentInput> {
    transaction
        .inputs()
        .iter()
        .filter_map(|input| match input {
            ZebraTransparentInput::PrevOut { outpoint, .. } => Some(CompactTransparentInput {
                previous_transaction_id: TransactionId::from_bytes(outpoint.hash.0),
                previous_output_index: outpoint.index,
            }),
            ZebraTransparentInput::Coinbase { .. } => None,
        })
        .collect()
}

fn compact_transparent_outputs(transaction: &ZebraTransaction) -> Vec<CompactTransparentOutput> {
    transaction
        .outputs()
        .iter()
        .map(|output| CompactTransparentOutput {
            value_zat: u64::from(output.value()),
            script_pub_key: output.lock_script.as_raw_bytes().to_vec(),
        })
        .collect()
}

fn compact_transaction_has_payload(transaction: &CompactTransaction) -> bool {
    !transaction.data.sapling_spends.is_empty()
        || !transaction.data.sapling_outputs.is_empty()
        || !transaction.data.orchard_actions.is_empty()
        || !transaction.data.ironwood_actions.is_empty()
        || !transaction.data.transparent_inputs.is_empty()
        || !transaction.data.transparent_outputs.is_empty()
}

fn count_to_u32(count: usize, field: &'static str) -> Result<u32, CanonicalBlockConstructionError> {
    u32::try_from(count).map_err(|_| CanonicalBlockConstructionError::CountOverflow { field })
}

fn format_block_hash(bytes: [u8; 32]) -> String {
    encode_rpc_block_hash_hex(BlockHash::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor};

    use proptest::{
        arbitrary::any,
        prop_oneof,
        strategy::{Just, Strategy},
        test_runner::{Config, TestCaseError, TestRng, TestRunner},
    };
    use serde_json::Value;
    use zebra_chain::serialization::{
        CompactSizeMessage, SerializationError, TrustedPreallocate, ZcashDeserialize,
        ZcashSerialize,
    };
    use zinder_core::{
        BlockHeight, CompactShieldedAction, CompactTransactionData, Network, TransactionId,
    };
    use zinder_source::SourceBlock;
    use zinder_testkit::sample_regtest_upgrade_activations;

    use super::{
        COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN, CanonicalBlockConstructionError, CommitmentTreeSizes,
        CompactTransaction, MAX_BLOCK_BYTES, MIN_TRANSPARENT_TX_SIZE, ParsedSourceBlock,
        RawBlobPolicy, ShieldedProtocol, TransactionByteLayout, ZebraBlock, ZebraTransaction,
        compact_note_ciphertext_prefix, compact_transaction_has_payload, parse_source_block,
        prepare_canonical_block, prepare_canonical_transactions_from_parsed,
        validate_transaction_byte_layout,
    };

    #[test]
    fn canonical_replay_is_invariant_across_raw_blob_policies() -> Result<(), Box<dyn Error>> {
        let source_block = regtest_fixture_block()?;
        let activations = sample_regtest_upgrade_activations();
        let no_blobs = prepare_canonical_block(&source_block, &activations, RawBlobPolicy::None)?;
        let transaction_blobs =
            prepare_canonical_block(&source_block, &activations, RawBlobPolicy::Transactions)?;
        let all_blobs = prepare_canonical_block(&source_block, &activations, RawBlobPolicy::All)?;

        assert_eq!(no_blobs.facts, transaction_blobs.facts);
        assert_eq!(no_blobs.facts, all_blobs.facts);
        assert_eq!(no_blobs.replay_envelope, transaction_blobs.replay_envelope);
        assert_eq!(no_blobs.replay_envelope, all_blobs.replay_envelope);

        assert!(no_blobs.retained_raw_blobs.block_blob.is_none());
        assert_eq!(
            no_blobs.retained_raw_blobs.transaction_blobs,
            [] as [zinder_core::TransactionBlobArtifact; 0]
        );
        assert!(transaction_blobs.retained_raw_blobs.block_blob.is_none());
        assert_eq!(
            transaction_blobs.retained_raw_blobs.transaction_blobs.len(),
            transaction_blobs.facts.transactions.len()
        );
        assert!(all_blobs.retained_raw_blobs.block_blob.is_some());
        assert_eq!(
            all_blobs.retained_raw_blobs.transaction_blobs.len(),
            all_blobs.facts.transactions.len()
        );

        Ok(())
    }

    #[test]
    fn canonical_preparation_rejects_source_bytes_after_the_consensus_block()
    -> Result<(), Box<dyn Error>> {
        let mut source_block = regtest_fixture_block()?;
        let consensus_block_byte_count = source_block.raw_block_bytes.len();
        source_block.raw_block_bytes.push(0);

        let error = prepare_canonical_block(
            &source_block,
            &sample_regtest_upgrade_activations(),
            RawBlobPolicy::None,
        )
        .err()
        .ok_or("source block with a trailing byte was accepted")?;

        assert!(matches!(
            error,
            CanonicalBlockConstructionError::SourcePayloadNotFullyConsumed {
                consumed_byte_count,
                source_byte_count,
            } if consumed_byte_count == consensus_block_byte_count
                && source_byte_count == consensus_block_byte_count.saturating_add(1)
        ));

        Ok(())
    }

    /// Every block fixture the repository holds: Sprout, Sapling (v4), NU5
    /// and NU6.2 (v5), and Ironwood and NU7 (v6) transactions.
    const BLOCK_FIXTURES: [&str; 10] = [
        include_str!("../tests/fixtures/zcash-mainnet-sprout-block-396.json"),
        include_str!("../tests/fixtures/zcash-mainnet-sapling-spend-block-419202.json"),
        include_str!("../tests/fixtures/zcash-testnet-sapling-block-1842432.json"),
        include_str!("../tests/fixtures/zcash-testnet-orchard-block-1842462.json"),
        include_str!("../tests/fixtures/z3-regtest-block-1.json"),
        include_str!("../tests/fixtures/z3-regtest-ironwood-block-603.json"),
        include_str!("../tests/fixtures/zakura-1.8.6-regtest-orchard-block-111.json"),
        include_str!("../tests/fixtures/zakura-1.8.6-regtest-ironwood-block-132.json"),
        include_str!("../tests/fixtures/zakura-1.8.6-regtest-nu7-block-162.json"),
        include_str!("../tests/fixtures/zebra-7.0.0-regtest-nu7-block-159.json"),
    ];

    fn fixture_block_bytes(fixture_json: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let fixture: Value = serde_json::from_str(fixture_json)?;
        Ok(hex::decode(
            fixture
                .get("raw_block_hex")
                .and_then(Value::as_str)
                .ok_or("fixture raw_block_hex is missing")?,
        )?)
    }

    /// Wraps bytes without parsing their header, so malformed bytes reach
    /// the block parser.
    fn unparsed_source_block(raw_block_bytes: Vec<u8>) -> SourceBlock {
        SourceBlock {
            network: Network::ZcashRegtest,
            height: BlockHeight::new(1),
            hash: zinder_core::BlockHash::from_bytes([0; 32]),
            parent_hash: zinder_core::BlockHash::from_bytes([0; 32]),
            block_time_seconds: 0,
            raw_block_bytes,
        }
    }

    /// How zebra-chain's own `Block` deserializer handles some bytes.
    #[derive(Debug)]
    enum ZebraBlockParse {
        Rejected(String),
        Trailing { consumed_byte_count: usize },
        Complete(Vec<u8>),
    }

    fn zebra_block_parse(raw_block_bytes: &[u8]) -> Result<ZebraBlockParse, Box<dyn Error>> {
        let mut cursor = Cursor::new(raw_block_bytes);
        match ZebraBlock::zcash_deserialize(&mut cursor) {
            Err(error) => Ok(ZebraBlockParse::Rejected(error.to_string())),
            Ok(block) => {
                let consumed_byte_count = usize::try_from(cursor.position())?;
                if consumed_byte_count == raw_block_bytes.len() {
                    Ok(ZebraBlockParse::Complete(block.zcash_serialize_to_vec()?))
                } else {
                    Ok(ZebraBlockParse::Trailing {
                        consumed_byte_count,
                    })
                }
            }
        }
    }

    /// Checks that the layout slices the block into exactly the bytes of
    /// each transaction, and returns the block's serialization.
    fn assert_layout_slices_each_transaction(
        parsed: &ParsedSourceBlock,
        raw_block_bytes: &[u8],
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let layout = &parsed.transaction_layout;
        validate_transaction_byte_layout(
            layout,
            parsed.block.transactions.len(),
            raw_block_bytes.len(),
        )?;
        let mut header_and_count = parsed.block.header.zcash_serialize_to_vec()?;
        CompactSizeMessage::try_from(parsed.block.transactions.len())?
            .zcash_serialize(&mut header_and_count)?;
        assert_eq!(layout.first_transaction_offset, header_and_count.len());
        let mut offset = layout.first_transaction_offset;
        for (transaction, &size) in parsed
            .block
            .transactions
            .iter()
            .zip(&layout.transaction_sizes)
        {
            let serialized = transaction.zcash_serialize_to_vec()?;
            assert_eq!(size, serialized.len());
            assert_eq!(
                raw_block_bytes.get(offset..offset + size),
                Some(&serialized[..])
            );
            offset += size;
        }
        assert_eq!(offset, raw_block_bytes.len());
        Ok(parsed.block.zcash_serialize_to_vec()?)
    }

    /// Checks that `parse_source_block` accepts, rejects, and reports
    /// trailing bytes exactly as zebra-chain's `Block` deserializer does.
    fn assert_parse_matches_zebra(raw_block_bytes: &[u8]) -> Result<bool, Box<dyn Error>> {
        let expected = zebra_block_parse(raw_block_bytes)?;
        let parsed = parse_source_block(&unparsed_source_block(raw_block_bytes.to_vec()));
        match (expected, parsed) {
            (ZebraBlockParse::Complete(expected_bytes), Ok(parsed)) => {
                let parsed_bytes = assert_layout_slices_each_transaction(&parsed, raw_block_bytes)?;
                assert_eq!(parsed_bytes, expected_bytes);
                assert_eq!(parsed_bytes, raw_block_bytes);
                Ok(true)
            }
            (
                ZebraBlockParse::Rejected(expected_reason),
                Err(CanonicalBlockConstructionError::BlockParseFailed { source }),
            ) => {
                assert_eq!(source.to_string(), expected_reason);
                Ok(false)
            }
            (
                ZebraBlockParse::Trailing {
                    consumed_byte_count: expected,
                },
                Err(CanonicalBlockConstructionError::SourcePayloadNotFullyConsumed {
                    consumed_byte_count,
                    source_byte_count,
                }),
            ) => {
                assert_eq!(
                    (consumed_byte_count, source_byte_count),
                    (expected, raw_block_bytes.len())
                );
                Ok(false)
            }
            (expected, parsed) => Err(format!(
                "zebra-chain parse {expected:?} but parse_source_block returned {:?}",
                parsed.map(|parsed| parsed.transaction_layout.transaction_sizes)
            )
            .into()),
        }
    }

    #[test]
    fn every_fixture_block_parses_to_zebra_blocks_and_exact_transaction_ranges()
    -> Result<(), Box<dyn Error>> {
        for fixture_json in BLOCK_FIXTURES {
            assert!(assert_parse_matches_zebra(&fixture_block_bytes(
                fixture_json
            )?)?);
        }
        Ok(())
    }

    #[derive(Clone, Debug)]
    enum BlockMutation {
        Truncate(usize),
        Flip(usize, u8),
        Insert(usize, u8),
        Append(Vec<u8>),
    }

    fn block_mutations() -> impl Strategy<Value = (usize, BlockMutation)> {
        let mutation = prop_oneof![
            any::<usize>().prop_map(BlockMutation::Truncate),
            (any::<usize>(), 1_u8..=u8::MAX).prop_map(|(at, flip)| BlockMutation::Flip(at, flip)),
            (any::<usize>(), any::<u8>()).prop_map(|(at, byte)| BlockMutation::Insert(at, byte)),
            proptest::collection::vec(any::<u8>(), 1..64).prop_map(BlockMutation::Append),
            Just(BlockMutation::Truncate(usize::MAX)),
        ];
        (0..BLOCK_FIXTURES.len(), mutation)
    }

    /// The recording parser decides every mutated block as zebra-chain does.
    ///
    /// Blocks are truncated, have one byte flipped or inserted, or have bytes
    /// appended. Whatever zebra-chain's `Block` deserializer decides, the
    /// recording parser decides the same, with the same reason, and every
    /// accepted block's ranges slice out each transaction exactly.
    #[test]
    fn recording_parser_matches_zebra_block_deserialization_on_mutated_blocks()
    -> Result<(), Box<dyn Error>> {
        let fixtures = BLOCK_FIXTURES
            .into_iter()
            .map(fixture_block_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        let accepted = std::sync::atomic::AtomicUsize::new(0);
        let config = Config {
            cases: 512,
            failure_persistence: None,
            ..Config::default()
        };
        let rng = TestRng::deterministic_rng(config.rng_algorithm);
        let mut runner = TestRunner::new_with_rng(config, rng);
        runner
            .run(&block_mutations(), |(fixture_index, mutation)| {
                let mut bytes = fixtures[fixture_index].clone();
                match mutation {
                    BlockMutation::Truncate(at) => bytes.truncate(at % bytes.len()),
                    BlockMutation::Flip(at, flip) => {
                        let at = at % bytes.len();
                        bytes[at] ^= flip;
                    }
                    BlockMutation::Insert(at, byte) => bytes.insert(at % bytes.len(), byte),
                    BlockMutation::Append(tail) => bytes.extend(tail),
                }
                let parsed = assert_parse_matches_zebra(&bytes)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                if parsed {
                    accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(())
            })
            .map_err(|error| error.to_string())?;
        assert!(
            accepted.load(std::sync::atomic::Ordering::Relaxed) >= 32,
            "too few mutated blocks parsed to exercise the transaction ranges"
        );
        Ok(())
    }

    /// A block of `transactions` after the regtest fixture's header.
    fn block_with_transactions(transactions: &[Vec<u8>]) -> Result<Vec<u8>, Box<dyn Error>> {
        let fixture = fixture_block_bytes(BLOCK_FIXTURES[4])?;
        let header = ZebraBlock::zcash_deserialize(&mut Cursor::new(fixture))?.header;
        let mut bytes = header.zcash_serialize_to_vec()?;
        CompactSizeMessage::try_from(transactions.len())?.zcash_serialize(&mut bytes)?;
        for transaction in transactions {
            bytes.extend_from_slice(transaction);
        }
        Ok(bytes)
    }

    /// A version-1 transaction with one non-coinbase input and one output
    /// whose script is `output_script_len` bytes, or no inputs or outputs.
    fn v1_transaction(output_script_len: Option<u8>) -> Vec<u8> {
        let mut bytes = 1_u32.to_le_bytes().to_vec();
        match output_script_len {
            None => bytes.extend([0, 0]),
            Some(script_len) => {
                bytes.push(1);
                bytes.extend([0x42; 32]);
                bytes.extend(0_u32.to_le_bytes());
                bytes.push(0);
                bytes.extend(u32::MAX.to_le_bytes());
                bytes.push(1);
                bytes.extend(0_u64.to_le_bytes());
                bytes.push(script_len);
                bytes.extend(std::iter::repeat_n(0x51, usize::from(script_len)));
            }
        }
        bytes.extend(0_u32.to_le_bytes());
        bytes
    }

    #[test]
    fn transactions_must_average_the_smallest_transparent_transaction_size()
    -> Result<(), Box<dyn Error>> {
        let minimum = usize::try_from(MIN_TRANSPARENT_TX_SIZE)?;
        let empty = v1_transaction(None);
        assert_eq!(empty.len(), 10);
        // A transparent transaction is 60 bytes plus its output script, so a
        // 38-byte script makes two transactions average exactly the minimum.
        let at_minimum = v1_transaction(Some(38));
        assert_eq!(at_minimum.len() + empty.len(), 2 * minimum);
        assert!(assert_parse_matches_zebra(&block_with_transactions(&[
            at_minimum,
            empty.clone()
        ])?)?);

        let below_minimum = block_with_transactions(&[v1_transaction(Some(37)), empty])?;
        assert!(!assert_parse_matches_zebra(&below_minimum)?);
        assert!(matches!(
            parse_source_block(&unparsed_source_block(below_minimum)),
            Err(CanonicalBlockConstructionError::BlockParseFailed {
                source: SerializationError::Parse(
                    "block transactions are smaller than the smallest valid transaction"
                ),
            })
        ));
        Ok(())
    }

    #[test]
    fn transaction_counts_above_the_allocation_limit_fail_before_parsing()
    -> Result<(), Box<dyn Error>> {
        let fixture = fixture_block_bytes(BLOCK_FIXTURES[4])?;
        let header = ZebraBlock::zcash_deserialize(&mut Cursor::new(fixture))?.header;
        let limit = usize::try_from(ZebraTransaction::max_allocation())?;
        for (count, rejected) in [(limit, false), (limit + 1, true)] {
            let mut bytes = header.zcash_serialize_to_vec()?;
            CompactSizeMessage::try_from(count)?.zcash_serialize(&mut bytes)?;
            assert!(!assert_parse_matches_zebra(&bytes)?);
            let error = parse_source_block(&unparsed_source_block(bytes))
                .err()
                .ok_or("a block without its transactions parsed")?;
            assert_eq!(
                matches!(
                    error,
                    CanonicalBlockConstructionError::BlockParseFailed {
                        source: SerializationError::Parse("Vector longer than max_allocation"),
                    }
                ),
                rejected
            );
        }
        Ok(())
    }

    #[test]
    fn blocks_stop_at_the_consensus_block_size_limit() -> Result<(), Box<dyn Error>> {
        let limit = usize::try_from(MAX_BLOCK_BYTES)?;
        let transaction = v1_transaction(Some(38));
        let mut transaction_count = limit / transaction.len();
        let within_limit = loop {
            let bytes = block_with_transactions(&vec![transaction.clone(); transaction_count])?;
            if bytes.len() <= limit {
                break bytes;
            }
            transaction_count -= 1;
        };
        let oversized = block_with_transactions(&vec![transaction; transaction_count + 1])?;
        assert!(oversized.len() > limit);

        assert!(assert_parse_matches_zebra(&within_limit)?);
        assert!(!assert_parse_matches_zebra(&oversized)?);
        Ok(())
    }

    /// A block of 98-byte transactions whose last transaction's output script
    /// grows by `extra_script_bytes`.
    fn block_of_transparent_transactions(
        transaction_count: usize,
        extra_script_bytes: usize,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut transactions = vec![v1_transaction(Some(38)); transaction_count - 1];
        transactions.push(v1_transaction(Some(u8::try_from(38 + extra_script_bytes)?)));
        block_with_transactions(&transactions)
    }

    #[test]
    fn a_block_ending_at_the_size_limit_parses_and_one_more_byte_fails()
    -> Result<(), Box<dyn Error>> {
        let limit = usize::try_from(MAX_BLOCK_BYTES)?;
        let mut transaction_count = limit / v1_transaction(Some(38)).len();
        while block_of_transparent_transactions(transaction_count, 0)?.len() > limit {
            transaction_count -= 1;
        }
        // One output script under 253 bytes keeps its one-byte length prefix,
        // so lengthening it adds exactly that many block bytes.
        let missing = limit - block_of_transparent_transactions(transaction_count, 0)?.len();
        let at_limit = block_of_transparent_transactions(transaction_count, missing)?;
        let over_limit = block_of_transparent_transactions(transaction_count, missing + 1)?;
        assert_eq!((at_limit.len(), over_limit.len()), (limit, limit + 1));

        assert!(assert_parse_matches_zebra(&at_limit)?);
        assert!(!assert_parse_matches_zebra(&over_limit)?);
        let error = parse_source_block(&unparsed_source_block(over_limit))
            .err()
            .ok_or("a block one byte over the limit parsed")?;
        assert!(matches!(
            error,
            CanonicalBlockConstructionError::BlockParseFailed {
                source: SerializationError::Io(ref io_error),
            } if io_error.kind() == std::io::ErrorKind::UnexpectedEof
        ));
        Ok(())
    }

    #[test]
    fn zero_and_non_minimal_transaction_counts_match_zebra() -> Result<(), Box<dyn Error>> {
        let fixture = fixture_block_bytes(BLOCK_FIXTURES[4])?;
        let header = ZebraBlock::zcash_deserialize(&mut Cursor::new(fixture))?
            .header
            .zcash_serialize_to_vec()?;

        // Zebra decodes a block with no transactions; preparation then fails
        // because there is no coinbase to give the height.
        let mut no_transactions = header.clone();
        no_transactions.push(0);
        assert!(assert_parse_matches_zebra(&no_transactions)?);
        assert!(matches!(
            prepare_canonical_block(
                &unparsed_source_block(no_transactions),
                &sample_regtest_upgrade_activations(),
                RawBlobPolicy::None,
            ),
            Err(CanonicalBlockConstructionError::ParsedBlockMissingCoinbaseHeight)
        ));

        // A count of one written in three, five, or nine bytes is rejected
        // before any transaction is read.
        for count_encoding in [
            &[0xfd, 0x01, 0x00][..],
            &[0xfe, 0x01, 0x00, 0x00, 0x00][..],
            &[0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..],
        ] {
            let mut bytes = header.clone();
            bytes.extend_from_slice(count_encoding);
            bytes.extend(v1_transaction(Some(38)));
            assert!(!assert_parse_matches_zebra(&bytes)?);
            assert!(matches!(
                parse_source_block(&unparsed_source_block(bytes)),
                Err(CanonicalBlockConstructionError::BlockParseFailed {
                    source: SerializationError::Parse("non-canonical CompactSize"),
                })
            ));
        }
        Ok(())
    }

    #[test]
    fn preparation_rejects_ranges_that_skip_a_parsed_transaction() -> Result<(), Box<dyn Error>> {
        // Three transactions: an Ironwood coinbase and two version-6 spends.
        let raw_block_bytes = fixture_block_bytes(BLOCK_FIXTURES[7])?;
        let source_block = unparsed_source_block(raw_block_bytes.clone());
        let activations = sample_regtest_upgrade_activations();
        let ParsedSourceBlock {
            block,
            transaction_layout,
        } = parse_source_block(&source_block)?;
        assert_eq!(transaction_layout.transaction_sizes.len(), 3);
        prepare_canonical_transactions_from_parsed(
            &block,
            &transaction_layout,
            &source_block,
            &activations,
            true,
        )?;

        // Two ranges that still reach the last block byte, the second
        // covering two transactions.
        let sizes = &transaction_layout.transaction_sizes;
        let merged_layout = TransactionByteLayout {
            first_transaction_offset: transaction_layout.first_transaction_offset,
            transaction_sizes: vec![sizes[0], sizes[1] + sizes[2]],
        };
        assert!(matches!(
            prepare_canonical_transactions_from_parsed(
                &block,
                &merged_layout,
                &source_block,
                &activations,
                true,
            ),
            Err(CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                block_byte_count,
            }) if block_byte_count == raw_block_bytes.len()
        ));
        Ok(())
    }

    #[test]
    fn transaction_ranges_must_tile_the_transaction_region() {
        let layout =
            |first_transaction_offset, transaction_sizes: &[usize]| TransactionByteLayout {
                first_transaction_offset,
                transaction_sizes: transaction_sizes.to_vec(),
            };
        let mismatch = |result| {
            matches!(
                result,
                Err(
                    CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                        block_byte_count: 100
                    }
                )
            )
        };
        assert!(validate_transaction_byte_layout(&layout(40, &[10, 50]), 2, 100).is_ok());
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(40, &[10, 49]),
            2,
            100
        )));
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(40, &[10, 51]),
            2,
            100
        )));
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(39, &[10, 50]),
            2,
            100
        )));
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(40, &[60]),
            2,
            100
        )));
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(40, &[10, 50]),
            3,
            100
        )));
        assert!(mismatch(validate_transaction_byte_layout(
            &layout(40, &[usize::MAX, 61]),
            2,
            100
        )));
    }

    fn regtest_fixture_block() -> Result<SourceBlock, Box<dyn Error>> {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/z3-regtest-block-1.json"))?;
        let raw_block_hex = fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or("fixture raw_block_hex is missing")?;
        let raw_block_bytes = hex::decode(raw_block_hex)?;
        let height = fixture
            .get("height")
            .and_then(Value::as_u64)
            .and_then(|height| u32::try_from(height).ok())
            .ok_or("fixture height is missing")?;
        Ok(SourceBlock::from_raw_block_bytes(
            Network::ZcashRegtest,
            BlockHeight::new(height),
            raw_block_bytes,
        )?)
    }

    #[test]
    fn commitment_tree_size_overflow_identifies_ironwood() -> Result<(), Box<dyn Error>> {
        let running_sizes = CommitmentTreeSizes {
            ironwood: u32::MAX,
            ..CommitmentTreeSizes::default()
        };
        let additions = CommitmentTreeSizes {
            ironwood: 1,
            ..CommitmentTreeSizes::default()
        };
        let error = match running_sizes.checked_add(additions) {
            Ok(sizes) => {
                return Err(format!("expected Ironwood overflow, got {sizes:?}").into());
            }
            Err(error) => error,
        };

        assert!(matches!(
            error,
            CanonicalBlockConstructionError::CommitmentTreeOverflow {
                protocol: ShieldedProtocol::Ironwood
            }
        ));
        Ok(())
    }

    #[test]
    fn compact_note_ciphertext_prefix_rejects_short_buffers() -> Result<(), Box<dyn Error>> {
        let short_ciphertext = [0u8; COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN - 1];
        let error = match compact_note_ciphertext_prefix(&short_ciphertext) {
            Ok(prefix) => {
                return Err(format!(
                    "expected short ciphertext failure, got {} bytes",
                    prefix.len()
                )
                .into());
            }
            Err(error) => error,
        };

        assert!(matches!(
            error,
            CanonicalBlockConstructionError::CompactCiphertextTooShort
        ));
        Ok(())
    }

    #[test]
    fn compact_note_ciphertext_prefix_returns_first_wallet_scan_prefix_bytes()
    -> Result<(), Box<dyn Error>> {
        let ciphertext = [1u8; 580];
        let prefix = compact_note_ciphertext_prefix(&ciphertext)?;

        assert_eq!(prefix, [1u8; COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN]);
        Ok(())
    }

    #[test]
    fn has_payload_keeps_an_ironwood_only_transaction() {
        let ironwood_only_transaction = CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes([0; 32]),
            data: CompactTransactionData {
                ironwood_actions: vec![CompactShieldedAction {
                    nullifier: [0; 32],
                    commitment: [0; 32],
                    ephemeral_key: [0; 32],
                    ciphertext: [0; 52],
                }],
                ..CompactTransactionData::default()
            },
        };

        assert!(
            compact_transaction_has_payload(&ironwood_only_transaction),
            "a transaction with only Ironwood actions and no transparent, Sapling, or Orchard \
             components must not be dropped from the compact block's transaction list"
        );
    }
}
