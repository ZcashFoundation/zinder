//! Deterministic source-block artifact builders.

use std::{fmt, io::Cursor, time::Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zebra_chain::{
    block::{Block as ZebraBlock, merkle::Root as ZebraMerkleRoot},
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::Transaction as ZebraTransaction,
    transparent::Input as ZebraTransparentInput,
};
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockId, BlockTransactionIndexArtifact,
    CanonicalBlockFacts, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayEnvelope,
    CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainTipMetadata,
    CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput, CompactSaplingSpend,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, CompactTransparentInput,
    CompactTransparentOutput, NetworkUpgradeActivations, SerializedBytesDigest, ShieldedProtocol,
    TransactionBlobArtifact, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalancesArtifact, TransactionLocation, TransparentOutPoint,
    TransparentOutputArtifact, encode_canonical_block_replay, wire::encode_rpc_block_hash_hex,
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

    const fn writes_block_blobs(self) -> bool {
        matches!(self, Self::All)
    }

    const fn writes_transaction_blobs(self) -> bool {
        matches!(self, Self::Transactions | Self::All)
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
#[allow(
    clippy::too_many_lines,
    reason = "one parse-and-construct pipeline keeps stage timing and field ownership auditable"
)]
pub fn prepare_canonical_block(
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    raw_blob_policy: RawBlobPolicy,
) -> Result<PreparedCanonicalBlock, CanonicalBlockConstructionError> {
    validate_source_block_payload(source_block)?;
    let parsed_block =
        measure_canonical_construction_stage("block_parse", || parse_source_block(source_block))?;
    measure_canonical_construction_stage("identity_validation", || {
        validate_parsed_block_identity(&parsed_block, source_block)
    })?;
    let prepared_transactions = measure_canonical_construction_stage("transaction_facts", || {
        prepare_canonical_transactions_from_parsed(
            &parsed_block,
            source_block,
            activations,
            raw_blob_policy,
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
        Ok(if raw_blob_policy.writes_block_blobs() {
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

/// Physical rows required by the current `RocksDB` writer.
///
/// These redundant index shapes are deliberately private to the canonical
/// writer boundary and are not part of the backend-neutral fact contract.
pub(crate) struct CanonicalStoreBlockArtifacts {
    pub(crate) block_header: BlockHeaderArtifact,
    pub(crate) block_blob: Option<BlockBlobArtifact>,
    pub(crate) block_transaction_index: Vec<BlockTransactionIndexArtifact>,
    pub(crate) transaction_locations: Vec<TransactionLocation>,
    pub(crate) transaction_facts: Vec<TransactionFactsArtifact>,
    pub(crate) transaction_intrinsic_value_balances: Vec<TransactionIntrinsicValueBalancesArtifact>,
    pub(crate) transaction_blobs: Vec<TransactionBlobArtifact>,
    pub(crate) transparent_outputs_by_outpoint: Vec<TransparentOutputArtifact>,
}

/// Expands the minimal fact envelope into the redundant rows the current
/// `RocksDB` schema still commits.
pub(crate) fn expand_canonical_store_block_artifacts(
    facts: CanonicalBlockFacts,
    retained_raw_blobs: RetainedRawBlobs,
) -> Result<CanonicalStoreBlockArtifacts, CanonicalBlockConstructionError> {
    let transparent_outputs_by_outpoint = canonical_transparent_outputs_by_outpoint(&facts);
    let CanonicalBlockFacts {
        block_header,
        transactions,
        ..
    } = facts;
    let block_height = block_header.height;
    let block_hash = block_header.block_hash;
    let RetainedRawBlobs {
        block_blob,
        transaction_blobs,
    } = retained_raw_blobs;
    let mut block_transaction_index = Vec::with_capacity(transactions.len());
    let mut transaction_locations = Vec::with_capacity(transactions.len());
    let mut transaction_facts = Vec::with_capacity(transactions.len());
    let mut transaction_intrinsic_value_balances = Vec::with_capacity(transactions.len());

    for (tx_index_in_block, transaction) in transactions.into_iter().enumerate() {
        let tx_index_in_block = u32::try_from(tx_index_in_block)
            .map_err(|_| CanonicalBlockConstructionError::TransactionIndexOverflow)?;
        let CanonicalTransactionFacts {
            public_facts,
            intrinsic_value_balances,
            transparent_inputs,
            transparent_outputs,
            ..
        } = transaction;
        let transaction_id = public_facts.transaction_id;
        let location =
            TransactionLocation::new(transaction_id, block_height, block_hash, tx_index_in_block);
        block_transaction_index.push(BlockTransactionIndexArtifact::new(
            block_height,
            tx_index_in_block,
            transaction_id,
            block_hash,
        ));
        transaction_locations.push(location);
        transaction_facts.push(
            TransactionFactsArtifact::new(location, public_facts)
                .with_transparent_facts(transparent_inputs, transparent_outputs),
        );
        transaction_intrinsic_value_balances.push(TransactionIntrinsicValueBalancesArtifact::new(
            location,
            intrinsic_value_balances,
        ));
    }

    Ok(CanonicalStoreBlockArtifacts {
        block_header,
        block_blob,
        block_transaction_index,
        transaction_locations,
        transaction_facts,
        transaction_intrinsic_value_balances,
        transaction_blobs,
        transparent_outputs_by_outpoint,
    })
}

struct PreparedCanonicalTransactions {
    facts: Vec<CanonicalTransactionFacts>,
    transaction_blobs: Vec<TransactionBlobArtifact>,
}

/// Expands block-local transparent outputs into canonical outpoint rows.
pub(crate) fn canonical_transparent_outputs_by_outpoint(
    facts: &CanonicalBlockFacts,
) -> Vec<TransparentOutputArtifact> {
    let block_height = facts.block_header.height;
    let block_hash = facts.block_header.block_hash;
    facts
        .transactions
        .iter()
        .flat_map(|transaction| {
            let transaction_id = transaction.public_facts.transaction_id;
            transaction.transparent_outputs.iter().map(move |output| {
                TransparentOutputArtifact::new(
                    TransparentOutPoint::new(transaction_id, output.output_index),
                    output.value_zat,
                    output.script_pub_key.clone(),
                    output.address_script_hash,
                    block_height,
                    block_hash,
                )
            })
        })
        .collect()
}

fn prepare_canonical_transactions_from_parsed(
    parsed_block: &ZebraBlock,
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    raw_blob_policy: RawBlobPolicy,
) -> Result<PreparedCanonicalTransactions, CanonicalBlockConstructionError> {
    let mut transactions = Vec::with_capacity(parsed_block.transactions.len());
    let mut transaction_blobs = Vec::new();
    let (mut transaction_byte_offset, serialized_transaction_sizes_bytes) =
        transaction_byte_layout(parsed_block, source_block)?;

    for ((tx_index_in_block, transaction), serialized_size) in parsed_block
        .transactions
        .iter()
        .enumerate()
        .zip(serialized_transaction_sizes_bytes)
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
            raw_transaction_bytes,
            Some(source_block.height),
            activations,
        )
        .map_err(|source| {
            CanonicalBlockConstructionError::TransactionFactsParseFailed {
                reason: source.to_string(),
            }
        })?;
        if raw_blob_policy.writes_transaction_blobs() {
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

fn transaction_byte_layout(
    parsed_block: &ZebraBlock,
    source_block: &SourceBlock,
) -> Result<(usize, Vec<usize>), CanonicalBlockConstructionError> {
    let mut serialized_transaction_sizes_bytes =
        Vec::with_capacity(parsed_block.transactions.len());
    let mut total_transaction_bytes = 0usize;
    for transaction in &parsed_block.transactions {
        let serialized_size_bytes = transaction.zcash_serialized_size();
        total_transaction_bytes = total_transaction_bytes
            .checked_add(serialized_size_bytes)
            .ok_or(
                CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                    block_byte_count: source_block.raw_block_bytes.len(),
                },
            )?;
        serialized_transaction_sizes_bytes.push(serialized_size_bytes);
    }
    let first_transaction_byte_offset = source_block
        .raw_block_bytes
        .len()
        .checked_sub(total_transaction_bytes)
        .ok_or(
            CanonicalBlockConstructionError::TransactionByteRangeMismatch {
                block_byte_count: source_block.raw_block_bytes.len(),
            },
        )?;
    Ok((
        first_transaction_byte_offset,
        serialized_transaction_sizes_bytes,
    ))
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

fn parse_source_block(
    source_block: &SourceBlock,
) -> Result<ZebraBlock, CanonicalBlockConstructionError> {
    let mut source_bytes = Cursor::new(source_block.raw_block_bytes.as_slice());
    let parsed_block = ZebraBlock::zcash_deserialize(&mut source_bytes)
        .map_err(|source| CanonicalBlockConstructionError::BlockParseFailed { source })?;
    let consumed_byte_count = usize::try_from(source_bytes.position()).unwrap_or(usize::MAX);
    let source_byte_count = source_block.raw_block_bytes.len();
    if consumed_byte_count != source_byte_count {
        return Err(
            CanonicalBlockConstructionError::SourcePayloadNotFullyConsumed {
                consumed_byte_count,
                source_byte_count,
            },
        );
    }
    Ok(parsed_block)
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
                nullifier: (*action.nullifier()).to_bytes(),
                commitment: action.cmx().to_bytes(),
                ephemeral_key: encrypted_note.epk_bytes,
                ciphertext: compact_note_ciphertext_prefix(&encrypted_note.enc_ciphertext.0)?,
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
                nullifier: (*action.nullifier()).to_bytes(),
                commitment: action.cmx().to_bytes(),
                ephemeral_key: encrypted_note.epk_bytes,
                ciphertext: compact_note_ciphertext_prefix(&encrypted_note.enc_ciphertext.0)?,
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
    use std::error::Error;

    use serde_json::Value;
    use zinder_core::{
        BlockHeight, ChainEpoch, ChainEpochId, CompactShieldedAction, CompactTransactionData,
        Network, TransactionId, UnixTimestampMillis,
    };
    use zinder_source::SourceBlock;
    use zinder_store::{
        CURRENT_ARTIFACT_SCHEMA_VERSION, ChainEpochArtifacts, ChainStoreOptions, RawBlobRetention,
    };
    use zinder_testkit::{StoreFixture, sample_regtest_upgrade_activations};

    use super::{
        COMPACT_NOTE_CIPHERTEXT_PREFIX_LEN, CanonicalBlockConstructionError, CommitmentTreeSizes,
        CompactTransaction, RawBlobPolicy, ShieldedProtocol, compact_note_ciphertext_prefix,
        compact_transaction_has_payload, expand_canonical_store_block_artifacts,
        position_canonical_block, prepare_canonical_block,
    };

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the boundary test keeps real-fixture expansion, commit, and row reads together"
    )]
    fn canonical_store_expansion_round_trips_real_fixture_rows() -> Result<(), Box<dyn Error>> {
        let source_block = regtest_fixture_block()?;
        let prepared = prepare_canonical_block(
            &source_block,
            &sample_regtest_upgrade_activations(),
            RawBlobPolicy::All,
        )?;
        let mut tree_sizes = CommitmentTreeSizes::default();
        let positioned = position_canonical_block(prepared, &mut tree_sizes)?;
        let compact_block = positioned.compact_block;
        let replay_envelope = positioned.replay_envelope;
        let tip_metadata = positioned.tip_metadata;
        let canonical_store_artifacts = expand_canonical_store_block_artifacts(
            positioned.facts,
            positioned.retained_raw_blobs,
        )?;
        let location = *canonical_store_artifacts
            .transaction_locations
            .first()
            .ok_or("fixture transaction location is missing")?;
        let transaction_facts = canonical_store_artifacts
            .transaction_facts
            .first()
            .cloned()
            .ok_or("fixture transaction facts are missing")?;
        let transaction_blob = canonical_store_artifacts
            .transaction_blobs
            .first()
            .cloned()
            .ok_or("fixture transaction blob is missing")?;
        let transparent_output = canonical_store_artifacts
            .transparent_outputs_by_outpoint
            .first()
            .cloned()
            .ok_or("fixture transparent output is missing")?;
        let fixture = StoreFixture::open_with_options(ChainStoreOptions {
            raw_blob_retention: RawBlobRetention::All,
            ..ChainStoreOptions::for_local_tests()
        })?;
        let chain_epoch = ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: source_block.height,
            visible_tip_hash: source_block.hash,
            settled_tip_height: source_block.height,
            settled_tip_hash: source_block.hash,
            artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata,
            created_at: UnixTimestampMillis::new(1_774_669_000_000),
        };
        let block_blobs = canonical_store_artifacts.block_blob.into_iter().collect();
        fixture.chain_store().commit_chain_epoch(
            ChainEpochArtifacts::new(
                chain_epoch,
                vec![canonical_store_artifacts.block_header],
                vec![replay_envelope],
                vec![compact_block],
            )
            .with_block_blobs(block_blobs)
            .with_block_transaction_index(canonical_store_artifacts.block_transaction_index)
            .with_transaction_locations(canonical_store_artifacts.transaction_locations)
            .with_transaction_facts(canonical_store_artifacts.transaction_facts)
            .with_transaction_intrinsic_value_balances(
                canonical_store_artifacts.transaction_intrinsic_value_balances,
            )
            .with_transaction_blobs(canonical_store_artifacts.transaction_blobs)
            .with_transparent_outputs_by_outpoint(
                canonical_store_artifacts.transparent_outputs_by_outpoint,
            ),
        )?;

        let reader = fixture.chain_store().current_chain_epoch_reader()?;
        assert_eq!(
            reader.transaction_id_at_block_index(source_block.height, 0)?,
            Some(location.transaction_id)
        );
        assert_eq!(
            reader.transaction_location_by_id(location.transaction_id)?,
            Some(location)
        );
        assert_eq!(
            reader.transaction_facts_by_id(location.transaction_id)?,
            Some(transaction_facts)
        );
        assert_eq!(
            reader.transaction_blob_by_id(location.transaction_id)?,
            Some(transaction_blob)
        );
        assert_eq!(
            reader
                .transparent_outputs_by_outpoints(&[transparent_output.outpoint])?
                .get(&transparent_output.outpoint),
            Some(&transparent_output)
        );
        Ok(())
    }

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
        assert!(no_blobs.retained_raw_blobs.transaction_blobs.is_empty());
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
