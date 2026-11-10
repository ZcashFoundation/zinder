//! Deterministic in-memory chain shapes for Zinder tests.
//!
//! A [`ChainFixture`] is a sequence of synthetic blocks at heights 1..=N. The
//! fixture's hashes, timestamps, and payloads are derived from the height and
//! a per-fork salt so that two fixtures with the same constructor calls
//! produce byte-identical artifacts. Fork helpers branch the chain at a given
//! height with a salt-perturbed hash space so reorg tests can compare two
//! shapes that share a common prefix.
//!
//! Fixture artifacts are *not* parseable by `zebra_chain`; they exist for
//! tests that bypass the artifact builder (validating storage, query, or
//! protocol surfaces). Tests that exercise `derive_*_artifact` should pull
//! real Zcash bytes from `services/zinder-ingest/tests/fixtures/`.

use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockId,
    BlockTransactionIndexArtifact, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockReplayEnvelope, CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts,
    ChainEpoch, ChainEpochId, ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata,
    CompactTransaction, CompactTransactionData, CompactTransparentInput, CompactTransparentOutput,
    LockTime, Network, PrivacyShape, SerializedBytesDigest, TransactionBlobArtifact,
    TransactionComponentCounts, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
    TransactionVersion, TransparentInputFact, TransparentOutputArtifact, TransparentOutputFact,
    TransparentSpendFact, TransparentUnspentOutput, TreeStateArtifact, UnixTimestampMillis,
    UnsupportedSection, encode_canonical_block_replay,
};
use zinder_source::{SourceBlock, SourceBlockHeader};
use zinder_store::{CURRENT_ARTIFACT_SCHEMA_VERSION, RawBlobRetention};

const FIXTURE_GENESIS_TIMESTAMP_SECONDS: u32 = 1_774_668_400;
const FIXTURE_HASH_HEIGHT_MIX: u32 = 0x9e37_79b9;
const FIXTURE_TREE_STATE_PAYLOAD: &[u8] =
    br#"{"sapling":{"commitments":{"size":0}},"orchard":{"commitments":{"size":0}}}"#;
const FIXTURE_CHAIN_EPOCH_CREATED_AT_MILLIS: u64 = 1_774_669_000_000;

/// One synthetic block in a [`ChainFixture`].
///
/// Field values are derived from the block height and the parent fixture's
/// fork salt; tests can read them directly to assert against the canonical
/// artifacts produced by the fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureBlock {
    /// Block height. Heights start at 1.
    pub height: BlockHeight,
    /// Synthetic block hash derived from height and fork salt.
    pub hash: BlockHash,
    /// Hash of the previous block in the same fixture branch.
    pub parent_hash: BlockHash,
    /// Block timestamp in Unix seconds. Monotonic with height.
    pub block_time_seconds: u32,
    /// Placeholder raw block bytes. Not parseable by `zebra_chain`.
    pub raw_block_bytes: Vec<u8>,
    /// Tree-state checkpoint JSON payload bytes for this block.
    pub tree_state_checkpoint_payload_bytes: Vec<u8>,
}

impl FixtureBlock {
    /// Returns canonical block-header facts for this fixture block.
    #[must_use]
    pub fn block_header_artifact(&self) -> BlockHeaderArtifact {
        BlockHeaderArtifact::new(
            self.height,
            self.hash,
            self.parent_hash,
            [0; 32],
            [0; 32],
            i64::from(self.block_time_seconds),
            0,
            [0; 32],
            0,
            u64::try_from(self.raw_block_bytes.len()).unwrap_or(u64::MAX),
        )
    }

    /// Returns the raw block blob for this fixture block.
    #[must_use]
    pub fn block_blob_artifact(&self) -> BlockBlobArtifact {
        BlockBlobArtifact::new(
            self.height,
            self.hash,
            self.parent_hash,
            self.raw_block_bytes.clone(),
        )
    }

    /// Returns the compact-block artifact for this fixture block.
    ///
    /// The compact block carries native structured scan data with an empty
    /// transaction list and zero commitment-tree sizes.
    #[must_use]
    pub fn compact_block_artifact(&self) -> CompactBlockArtifact {
        CompactBlockArtifact::empty(
            BlockId::new(self.height, self.hash),
            self.parent_hash,
            self.block_time_seconds,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    /// Returns the node-shaped [`SourceBlock`] for this fixture block.
    #[must_use]
    pub fn source_block(&self, network: Network) -> SourceBlock {
        SourceBlock::new(
            SourceBlockHeader {
                network,
                height: self.height,
                hash: self.hash,
                parent_hash: self.parent_hash,
                block_time_seconds: self.block_time_seconds,
            },
            self.raw_block_bytes.clone(),
        )
    }

    /// Returns the tree-state checkpoint artifact for this fixture block.
    #[must_use]
    pub fn tree_state_checkpoint_artifact(&self) -> TreeStateArtifact {
        TreeStateArtifact::new(
            self.height,
            self.hash,
            self.block_time_seconds,
            self.tree_state_checkpoint_payload_bytes.clone(),
        )
    }
}

/// Canonical transaction rows attached to a [`ChainFixture`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureTransactionRows {
    /// Block-local transaction id row.
    pub block_transaction_index: BlockTransactionIndexArtifact,
    /// Canonical mined transaction location.
    pub location: TransactionLocation,
    /// Parsed public transaction facts.
    pub facts: TransactionFactsArtifact,
    /// Optional raw transaction blob row.
    pub blob: Option<TransactionBlobArtifact>,
    /// Optional transaction-intrinsic shielded value balances.
    ///
    /// Constructors populate all-zero balances so schema-19 replay and
    /// canonical schema rows remain exact by default. `None` is reserved for
    /// tests that deliberately construct an invalid or incomplete artifact set.
    pub intrinsic_value_balances: Option<TransactionIntrinsicValueBalances>,
}

impl FixtureTransactionRows {
    /// Builds canonical rows for a synthetic raw transaction.
    #[must_use]
    pub fn from_raw_transaction(
        transaction_id: TransactionId,
        block_height: BlockHeight,
        block_hash: BlockHash,
        tx_index_in_block: u32,
        raw_transaction_bytes: impl Into<Vec<u8>>,
    ) -> Self {
        let raw_transaction_bytes = raw_transaction_bytes.into();
        let location =
            TransactionLocation::new(transaction_id, block_height, block_hash, tx_index_in_block);
        let public_facts =
            synthetic_transaction_public_facts(transaction_id, raw_transaction_bytes.len());
        Self {
            block_transaction_index: BlockTransactionIndexArtifact::new(
                block_height,
                tx_index_in_block,
                transaction_id,
                block_hash,
            ),
            location,
            facts: TransactionFactsArtifact::new(location, public_facts),
            blob: Some(TransactionBlobArtifact::new(
                location,
                raw_transaction_bytes,
            )),
            intrinsic_value_balances: Some(TransactionIntrinsicValueBalances::default()),
        }
    }

    /// Builds canonical rows for a transaction whose raw bytes are unavailable.
    #[must_use]
    pub fn from_public_facts(
        location: TransactionLocation,
        public_facts: TransactionPublicFacts,
    ) -> Self {
        Self {
            block_transaction_index: BlockTransactionIndexArtifact::new(
                location.block_height,
                location.tx_index_in_block,
                location.transaction_id,
                location.block_hash,
            ),
            location,
            facts: TransactionFactsArtifact::new(location, public_facts),
            blob: None,
            intrinsic_value_balances: Some(TransactionIntrinsicValueBalances::default()),
        }
    }

    /// Attaches transaction-intrinsic shielded value balances to these rows.
    #[must_use]
    pub const fn with_intrinsic_value_balances(
        mut self,
        intrinsic_value_balances: TransactionIntrinsicValueBalances,
    ) -> Self {
        self.intrinsic_value_balances = Some(intrinsic_value_balances);
        self
    }

    fn attach_transparent_input(&mut self, input: TransparentInputFact) {
        self.facts
            .transparent_inputs
            .retain(|existing| existing.input_index != input.input_index);
        self.facts.transparent_inputs.push(input);
        self.facts
            .transparent_inputs
            .sort_by_key(|existing| existing.input_index);
        self.facts.public_facts.counts.transparent_input_count =
            u32::try_from(self.facts.transparent_inputs.len()).unwrap_or(u32::MAX);
    }

    fn attach_transparent_output(&mut self, output: TransparentOutputFact) {
        self.facts
            .transparent_outputs
            .retain(|existing| existing.output_index != output.output_index);
        self.facts.transparent_outputs.push(output);
        self.facts
            .transparent_outputs
            .sort_by_key(|existing| existing.output_index);
        self.facts.public_facts.counts.transparent_output_count =
            u32::try_from(self.facts.transparent_outputs.len()).unwrap_or(u32::MAX);
    }
}

/// Builds one exact set of canonical fixture transaction rows from semantic
/// transaction rows plus transparent output and spend facts.
///
/// Existing transaction rows remain authoritative for transaction order and
/// raw bytes. Missing creator or spender transactions are synthesized with a
/// deterministic block-local index. The returned rows are the single source
/// for replay envelopes and outpoint-keyed transparent output artifacts.
#[must_use]
pub fn build_fixture_transaction_rows(
    transaction_rows: &[FixtureTransactionRows],
    transparent_outputs: &[TransparentOutputArtifact],
    transparent_spends: &[TransparentSpendFact],
) -> Vec<FixtureTransactionRows> {
    let mut canonical_rows = transaction_rows.to_vec();

    for spend in transparent_spends {
        let row_index = canonical_rows
            .iter()
            .position(|rows| {
                rows.location.transaction_id == spend.spending_transaction_id
                    && rows.location.block_height == spend.block_height
                    && rows.location.block_hash == spend.block_hash
            })
            .unwrap_or_else(|| {
                canonical_rows.push(FixtureTransactionRows::from_public_facts(
                    TransactionLocation::new(
                        spend.spending_transaction_id,
                        spend.block_height,
                        spend.block_hash,
                        spend.tx_index_in_block,
                    ),
                    synthetic_transaction_public_facts(spend.spending_transaction_id, 0),
                ));
                canonical_rows.len().saturating_sub(1)
            });
        canonical_rows[row_index].attach_transparent_input(TransparentInputFact::new(
            spend.input_index,
            spend.spent_outpoint,
        ));
    }

    for output in transparent_outputs {
        let row_index = canonical_rows
            .iter()
            .position(|rows| {
                rows.location.transaction_id == output.outpoint.transaction_id
                    && rows.location.block_height == output.block_height
                    && rows.location.block_hash == output.block_hash
            })
            .unwrap_or_else(|| {
                let tx_index_in_block = next_fixture_transaction_index(
                    &canonical_rows,
                    output.block_height,
                    output.block_hash,
                );
                canonical_rows.push(FixtureTransactionRows::from_public_facts(
                    TransactionLocation::new(
                        output.outpoint.transaction_id,
                        output.block_height,
                        output.block_hash,
                        tx_index_in_block,
                    ),
                    synthetic_transaction_public_facts(output.outpoint.transaction_id, 0),
                ));
                canonical_rows.len().saturating_sub(1)
            });
        canonical_rows[row_index].attach_transparent_output(TransparentOutputFact::new(
            output.outpoint.output_index,
            output.value_zat,
            output.script_pub_key.clone(),
            output.address_script_hash,
        ));
    }

    canonical_rows
        .sort_by_key(|rows| (rows.location.block_height, rows.location.tx_index_in_block));
    canonical_rows
}

fn next_fixture_transaction_index(
    transaction_rows: &[FixtureTransactionRows],
    block_height: BlockHeight,
    block_hash: BlockHash,
) -> u32 {
    let mut candidate = 0_u32;
    while transaction_rows.iter().any(|rows| {
        rows.location.block_height == block_height
            && rows.location.block_hash == block_hash
            && rows.location.tx_index_in_block == candidate
    }) {
        candidate = candidate.saturating_add(1);
    }
    candidate
}

/// Encodes one fixture block's complete semantic replay envelopes.
///
/// `ordered_transaction_rows` must contain only transactions mined in
/// `block_header`, in canonical block order. Raw block and transaction blobs
/// remain separate fixture artifacts. Missing intrinsic balances are
/// represented by the all-zero transaction-intrinsic value so
/// malformed-fixture tests can still reach store validation deliberately.
#[must_use]
pub fn encode_fixture_block_replay(
    block_header: &BlockHeaderArtifact,
    ordered_transaction_rows: &[FixtureTransactionRows],
) -> CanonicalBlockReplayEnvelope {
    encode_fixture_block_replay_with_raw_block(block_header, &[], ordered_transaction_rows)
}

/// Encodes semantic replay facts bound to the supplied synthetic raw block.
///
/// Use this variant whenever the fixture commit also retains a block blob.
#[must_use]
pub fn encode_fixture_block_replay_with_raw_block(
    block_header: &BlockHeaderArtifact,
    raw_block_bytes: &[u8],
    ordered_transaction_rows: &[FixtureTransactionRows],
) -> CanonicalBlockReplayEnvelope {
    let facts = CanonicalBlockFacts {
        block_header: block_header.clone(),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(raw_block_bytes),
        transactions: ordered_transaction_rows
            .iter()
            .map(|transaction_rows| CanonicalTransactionFacts {
                public_facts: transaction_rows.facts.public_facts.clone(),
                serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                    transaction_rows
                        .blob
                        .as_ref()
                        .map_or(&[], |blob| blob.raw_transaction_bytes.as_slice()),
                ),
                intrinsic_value_balances: transaction_rows
                    .intrinsic_value_balances
                    .unwrap_or_default(),
                transparent_inputs: transaction_rows.facts.transparent_inputs.clone(),
                transparent_outputs: transaction_rows.facts.transparent_outputs.clone(),
            })
            .collect(),
    };
    encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::CURRENT,
        CanonicalBlockFactsDigestVersion::CURRENT,
    )
}

/// Builds public facts for synthetic transaction bytes used by fixture tests.
#[must_use]
pub fn synthetic_transaction_public_facts(
    transaction_id: TransactionId,
    raw_transaction_size_bytes: usize,
) -> TransactionPublicFacts {
    TransactionPublicFacts {
        transaction_id,
        auth_digest: None,
        wtxid: None,
        version: TransactionVersion::Unsupported {
            effective_version: 0,
            version_group_id: None,
        },
        consensus_branch_id: None,
        lock_time: LockTime::Unlocked,
        expiry_height: None,
        size_bytes: u32::try_from(raw_transaction_size_bytes).unwrap_or(u32::MAX),
        counts: TransactionComponentCounts::EMPTY,
        orchard_value_balance_zat: None,
        orchard_anchor: None,
        ironwood_value_balance_zat: None,
        privacy_shape: PrivacyShape::Unclassified,
        is_coinbase: false,
        unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
    }
}

/// Deterministic in-memory chain of synthetic blocks.
///
/// Use [`ChainFixture::new`] to start an empty chain and
/// [`ChainFixture::extend_blocks`] to append height-by-height. Use
/// [`ChainFixture::fork_at`] to branch a divergent variant that shares an
/// ancestor prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainFixture {
    network: Network,
    raw_blob_retention: RawBlobRetention,
    branch_salt: u32,
    blocks: Vec<FixtureBlock>,
    compact_block_overrides: Vec<CompactBlockArtifact>,
    tip_metadata_override: Option<ChainTipMetadata>,
    transaction_rows: Vec<FixtureTransactionRows>,
    transparent_outputs_by_outpoint: Vec<TransparentOutputArtifact>,
    transparent_spend_facts: Vec<TransparentSpendFact>,
}

impl ChainFixture {
    /// Creates an empty fixture for the given network.
    #[must_use]
    pub const fn new(network: Network) -> Self {
        Self {
            network,
            raw_blob_retention: RawBlobRetention::None,
            branch_salt: 0,
            blocks: Vec::new(),
            compact_block_overrides: Vec::new(),
            tip_metadata_override: None,
            transaction_rows: Vec::new(),
            transparent_outputs_by_outpoint: Vec::new(),
            transparent_spend_facts: Vec::new(),
        }
    }

    /// Appends `count` synthetic blocks to the fixture, in increasing height
    /// order, starting from the current tip + 1.
    #[must_use]
    pub fn extend_blocks(mut self, count: u32) -> Self {
        let next_starting_height = self
            .blocks
            .last()
            .map_or(1, |last| last.height.value().saturating_add(1));

        for offset in 0..count {
            let height_value = next_starting_height.saturating_add(offset);
            let height = BlockHeight::new(height_value);
            let parent_hash = self
                .blocks
                .last()
                .map_or_else(|| synthetic_block_hash(0, 0), |last| last.hash);
            let hash = synthetic_block_hash(height_value, self.branch_salt);
            let block_time_seconds = FIXTURE_GENESIS_TIMESTAMP_SECONDS.saturating_add(height_value);
            let raw_block_bytes = format!("zinder-testkit-block-{height_value}").into_bytes();

            self.blocks.push(FixtureBlock {
                height,
                hash,
                parent_hash,
                block_time_seconds,
                raw_block_bytes,
                tree_state_checkpoint_payload_bytes: FIXTURE_TREE_STATE_PAYLOAD.to_vec(),
            });
        }

        self
    }

    pub(crate) fn with_canonical_genesis_parent(mut self) -> Self {
        if let Some(first_block) = self.blocks.first_mut() {
            first_block.parent_hash = self.network.genesis_hash();
        }
        self
    }

    /// Returns a new fixture that shares blocks `< divergence_height` with
    /// `self` and is empty from `divergence_height` onwards.
    ///
    /// Subsequent calls to [`ChainFixture::extend_blocks`] on the returned
    /// fixture build a divergent branch with hashes that differ from the
    /// parent fixture for every height at or above `divergence_height`.
    ///
    /// # Errors
    ///
    /// Returns an error if `divergence_height` is greater than the parent
    /// fixture's tip height + 1, or if the parent fixture is empty.
    pub fn fork_at(&self, divergence_height: BlockHeight) -> Result<Self, ChainFixtureError> {
        let Some(tip_height) = self.tip_height() else {
            return Err(ChainFixtureError::ForkBeforeGenesis);
        };
        let max_divergence_height = tip_height.value().saturating_add(1);
        if divergence_height.value() > max_divergence_height {
            return Err(ChainFixtureError::ForkAboveTip {
                requested: divergence_height,
                tip_plus_one: BlockHeight::new(max_divergence_height),
            });
        }
        if divergence_height.value() == 0 {
            return Err(ChainFixtureError::ForkAtGenesis);
        }

        let prefix_blocks = self
            .blocks
            .iter()
            .take_while(|fixture_block| fixture_block.height.value() < divergence_height.value())
            .cloned()
            .collect::<Vec<_>>();

        Ok(Self {
            network: self.network,
            raw_blob_retention: self.raw_blob_retention,
            branch_salt: self.branch_salt.wrapping_add(1).max(1),
            blocks: prefix_blocks,
            compact_block_overrides: self
                .compact_block_overrides
                .iter()
                .filter(|block| block.height() < divergence_height)
                .cloned()
                .collect(),
            tip_metadata_override: None,
            transaction_rows: Vec::new(),
            transparent_outputs_by_outpoint: Vec::new(),
            transparent_spend_facts: Vec::new(),
        })
    }

    /// Sets the raw consensus-blob contract for artifacts built by this fixture.
    ///
    /// `None` omits every blob, `Transactions` includes each available
    /// transaction blob, and `All` also includes every block blob. Store
    /// admission rejects a transaction-retaining fixture when any attached
    /// transaction row lacks its raw bytes.
    #[must_use]
    pub const fn with_raw_blob_retention(mut self, retention: RawBlobRetention) -> Self {
        self.raw_blob_retention = retention;
        self
    }

    /// Returns the raw consensus-blob contract selected for this fixture.
    #[must_use]
    pub const fn raw_blob_retention(&self) -> RawBlobRetention {
        self.raw_blob_retention
    }

    /// Overrides the [`ChainTipMetadata`] reported on the tip [`ChainEpoch`].
    ///
    /// Useful for tests that exercise subtree-root behavior at large tip
    /// commitment-tree sizes without committing thousands of synthetic
    /// outputs.
    #[must_use]
    pub const fn with_tip_metadata_override(mut self, tip_metadata: ChainTipMetadata) -> Self {
        self.tip_metadata_override = Some(tip_metadata);
        self
    }

    /// Overrides the structured compact artifact at its matching fixture height.
    #[must_use]
    pub fn with_compact_block_artifact(mut self, compact_block: CompactBlockArtifact) -> Self {
        self.compact_block_overrides
            .retain(|existing| existing.height() != compact_block.height());
        self.compact_block_overrides.push(compact_block);
        self
    }

    /// Replaces the tree-state checkpoint payload bytes attached to the block at `height`.
    ///
    /// Returns the fixture unchanged if no block exists at `height`.
    #[must_use]
    pub fn with_tree_state_checkpoint_payload_at(
        mut self,
        height: BlockHeight,
        payload_bytes: impl Into<Vec<u8>>,
    ) -> Self {
        let payload_bytes = payload_bytes.into();
        for block in &mut self.blocks {
            if block.height == height {
                block.tree_state_checkpoint_payload_bytes = payload_bytes;
                break;
            }
        }
        self
    }

    /// Attaches canonical transaction rows to this fixture's commit set.
    #[must_use]
    pub fn with_transaction_rows(mut self, transaction_rows: FixtureTransactionRows) -> Self {
        self.transaction_rows.push(transaction_rows);
        self
    }

    /// Attaches an [`TransparentUnspentOutput`]-shaped transparent output
    /// to this fixture's commit set.
    ///
    /// The store derives address-output projection rows from the canonical
    /// transparent-output artifacts at commit, so the fixture stores only
    /// the [`TransparentOutputArtifact`] form.
    #[must_use]
    pub fn with_address_output_index(
        mut self,
        address_output_index: TransparentUnspentOutput,
    ) -> Self {
        self.transparent_outputs_by_outpoint
            .push(TransparentOutputArtifact::new(
                address_output_index.outpoint,
                address_output_index.value_zat,
                address_output_index.script_pub_key,
                address_output_index.address_script_hash,
                address_output_index.block_height,
                address_output_index.block_hash,
            ));
        self
    }

    /// Attaches a [`TransparentSpendFact`] to this fixture's commit set.
    #[must_use]
    pub fn with_transparent_spend_fact(
        mut self,
        transparent_spend_fact: TransparentSpendFact,
    ) -> Self {
        self.transparent_spend_facts.push(transparent_spend_fact);
        self
    }

    /// Returns the network this fixture was built for.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// Returns the fixture's tip height, or `None` for an empty fixture.
    #[must_use]
    pub fn tip_height(&self) -> Option<BlockHeight> {
        self.blocks.last().map(|fixture_block| fixture_block.height)
    }

    /// Returns the fixture's tip hash, or `None` for an empty fixture.
    #[must_use]
    pub fn tip_hash(&self) -> Option<BlockHash> {
        self.blocks.last().map(|fixture_block| fixture_block.hash)
    }

    /// Returns the count of blocks in this fixture.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Returns a borrowed view of every block in this fixture, in ascending
    /// height order.
    #[must_use]
    pub fn blocks(&self) -> &[FixtureBlock] {
        &self.blocks
    }

    /// Returns the fixture block at `height`, or `None` if absent.
    #[must_use]
    pub fn block_at(&self, height: BlockHeight) -> Option<&FixtureBlock> {
        self.blocks
            .iter()
            .find(|fixture_block| fixture_block.height == height)
    }

    /// Returns the [`SourceBlock`] for `height`, or `None` if absent.
    #[must_use]
    pub fn source_block_at(&self, height: BlockHeight) -> Option<SourceBlock> {
        self.block_at(height)
            .map(|fixture_block| fixture_block.source_block(self.network))
    }

    /// Returns every block as a [`BlockHeaderArtifact`] in ascending height order.
    #[must_use]
    pub fn block_header_artifacts(&self) -> Vec<BlockHeaderArtifact> {
        self.blocks
            .iter()
            .map(FixtureBlock::block_header_artifact)
            .collect()
    }

    /// Returns complete semantic replay envelopes for every block in ascending
    /// height order.
    #[must_use]
    pub fn block_replay_envelopes(&self) -> Vec<CanonicalBlockReplayEnvelope> {
        let transaction_rows = self.canonical_transaction_rows();
        self.block_replay_envelopes_for(&transaction_rows)
    }

    fn block_replay_envelopes_for(
        &self,
        canonical_transaction_rows: &[FixtureTransactionRows],
    ) -> Vec<CanonicalBlockReplayEnvelope> {
        self.blocks
            .iter()
            .map(|block| {
                let mut transaction_rows = canonical_transaction_rows
                    .iter()
                    .filter(|transaction_rows| {
                        transaction_rows.location.block_height == block.height
                            && transaction_rows.location.block_hash == block.hash
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                transaction_rows
                    .sort_by_key(|transaction_rows| transaction_rows.location.tx_index_in_block);
                encode_fixture_block_replay_with_raw_block(
                    &block.block_header_artifact(),
                    &block.raw_block_bytes,
                    &transaction_rows,
                )
            })
            .collect()
    }

    /// Returns the complete ordered transaction rows used by canonical replay.
    ///
    /// Explicit transaction rows are merged with transparent output and spend
    /// facts so replay envelopes and compact blocks share one semantic source.
    #[must_use]
    pub(crate) fn canonical_transaction_rows(&self) -> Vec<FixtureTransactionRows> {
        build_fixture_transaction_rows(
            &self.transaction_rows,
            &self.transparent_outputs_by_outpoint,
            &self.transparent_spend_facts,
        )
    }

    /// Returns every block as a [`CompactBlockArtifact`] in ascending height order.
    #[must_use]
    pub fn compact_block_artifacts(&self) -> Vec<CompactBlockArtifact> {
        let canonical_transaction_rows = self.canonical_transaction_rows();
        self.blocks
            .iter()
            .map(|block| {
                self.compact_block_overrides
                    .iter()
                    .find(|compact| compact.height() == block.height)
                    .cloned()
                    .unwrap_or_else(|| {
                        let transactions = canonical_transaction_rows
                            .iter()
                            .filter(|rows| rows.location.block_height == block.height)
                            .filter_map(|rows| {
                                let transparent_inputs = rows
                                    .facts
                                    .transparent_inputs
                                    .iter()
                                    .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
                                    .map(|input| CompactTransparentInput {
                                        previous_transaction_id: input
                                            .spent_outpoint
                                            .transaction_id,
                                        previous_output_index: input.spent_outpoint.output_index,
                                    })
                                    .collect::<Vec<_>>();
                                let transparent_outputs = rows
                                    .facts
                                    .transparent_outputs
                                    .iter()
                                    .map(|output| CompactTransparentOutput {
                                        value_zat: output.value_zat,
                                        script_pub_key: output.script_pub_key.clone(),
                                    })
                                    .collect::<Vec<_>>();
                                if transparent_inputs.is_empty() && transparent_outputs.is_empty() {
                                    return None;
                                }
                                Some(CompactTransaction {
                                    index: u64::from(rows.location.tx_index_in_block),
                                    transaction_id: rows.location.transaction_id,
                                    data: CompactTransactionData {
                                        transparent_inputs,
                                        transparent_outputs,
                                        ..CompactTransactionData::default()
                                    },
                                })
                            })
                            .collect();
                        CompactBlockArtifact::new(
                            BlockId::new(block.height, block.hash),
                            block.parent_hash,
                            block.block_time_seconds,
                            transactions,
                            CompactChainMetadata {
                                sapling_commitment_tree_size: 0,
                                orchard_commitment_tree_size: 0,
                                ironwood_commitment_tree_size: 0,
                            },
                        )
                        .unwrap_or_else(|_| std::process::abort())
                    })
            })
            .collect()
    }

    /// Builds the [`ChainEpoch`] descriptor that names this fixture as a
    /// canonical commit input.
    ///
    /// Returns `None` for an empty fixture; canonical chain epochs require at
    /// least one block.
    #[must_use]
    pub fn chain_epoch(&self, epoch_id: ChainEpochId) -> Option<ChainEpoch> {
        let tip_block = self.blocks.last()?;
        Some(ChainEpoch {
            id: epoch_id,
            network: self.network,
            visible_tip_height: tip_block.height,
            visible_tip_hash: tip_block.hash,
            settled_tip_height: tip_block.height,
            settled_tip_hash: tip_block.hash,
            artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: self
                .tip_metadata_override
                .unwrap_or_else(ChainTipMetadata::empty),
            created_at: UnixTimestampMillis::new(FIXTURE_CHAIN_EPOCH_CREATED_AT_MILLIS),
        })
    }
}

/// Errors raised when building a [`ChainFixture`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ChainFixtureError {
    /// Cannot fork an empty fixture; build the parent prefix first.
    #[error("cannot fork an empty chain fixture")]
    ForkBeforeGenesis,
    /// Fork height must be at least 1; the genesis block is not represented.
    #[error("cannot fork a chain fixture at the genesis (height 0)")]
    ForkAtGenesis,
    /// Fork height exceeds the parent fixture's tip + 1.
    #[error("fork height {requested:?} exceeds parent tip + 1 ({tip_plus_one:?})")]
    ForkAboveTip {
        /// Requested divergence height.
        requested: BlockHeight,
        /// Parent fixture's tip height + 1.
        tip_plus_one: BlockHeight,
    },
}

fn synthetic_block_hash(height: u32, branch_salt: u32) -> BlockHash {
    let mixed_word = height.wrapping_mul(FIXTURE_HASH_HEIGHT_MIX) ^ branch_salt;
    let mixed_bytes = mixed_word.to_be_bytes();
    let mut hash_bytes = [0u8; 32];
    for hash_chunk in hash_bytes.chunks_exact_mut(4) {
        hash_chunk.copy_from_slice(&mixed_bytes);
    }
    BlockHash::from_bytes(hash_bytes)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{ChainFixture, ChainFixtureError, FixtureTransactionRows, synthetic_block_hash};
    use zinder_core::{
        BlockHeight, ChainEpochId, Network, TransactionId, TransactionIntrinsicValueBalances,
        decode_canonical_block_replay,
    };
    use zinder_store::RawBlobRetention;

    #[test]
    fn extend_blocks_links_parent_hashes() -> Result<(), Box<dyn Error>> {
        let fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);

        assert_eq!(fixture.block_count(), 5);
        for height_value in 1..=5_u32 {
            let block = fixture
                .block_at(BlockHeight::new(height_value))
                .ok_or("block must exist at every extended height")?;
            let expected_parent_hash = if height_value == 1 {
                synthetic_block_hash(0, 0)
            } else {
                synthetic_block_hash(height_value - 1, 0)
            };
            assert_eq!(block.parent_hash, expected_parent_hash);
        }
        Ok(())
    }

    #[test]
    fn fork_at_shares_prefix_and_diverges_above() -> Result<(), Box<dyn Error>> {
        let main_branch = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
        let alternate_branch = main_branch.fork_at(BlockHeight::new(3))?.extend_blocks(2);

        let main_block_2 = main_branch
            .block_at(BlockHeight::new(2))
            .ok_or("main fixture must contain block 2")?;
        let alternate_block_2 = alternate_branch
            .block_at(BlockHeight::new(2))
            .ok_or("alternate fixture must contain block 2")?;
        assert_eq!(
            main_block_2.hash, alternate_block_2.hash,
            "shared prefix must produce identical hashes below the divergence height"
        );

        let main_block_4 = main_branch
            .block_at(BlockHeight::new(4))
            .ok_or("main fixture must contain block 4")?;
        let alternate_block_4 = alternate_branch
            .block_at(BlockHeight::new(4))
            .ok_or("alternate fixture must contain block 4")?;
        assert_ne!(
            main_block_4.hash, alternate_block_4.hash,
            "fork branch must produce a distinct hash at every divergent height"
        );

        Ok(())
    }

    #[test]
    fn fork_at_zero_returns_genesis_error() {
        let fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        assert_eq!(
            fixture.fork_at(BlockHeight::new(0)),
            Err(ChainFixtureError::ForkAtGenesis)
        );
    }

    #[test]
    fn fork_at_above_tip_plus_one_returns_error() {
        let fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        assert_eq!(
            fixture.fork_at(BlockHeight::new(5)),
            Err(ChainFixtureError::ForkAboveTip {
                requested: BlockHeight::new(5),
                tip_plus_one: BlockHeight::new(4),
            })
        );
    }

    #[test]
    fn fork_on_empty_fixture_is_rejected() {
        let fixture = ChainFixture::new(Network::ZcashRegtest);
        assert_eq!(
            fixture.fork_at(BlockHeight::new(1)),
            Err(ChainFixtureError::ForkBeforeGenesis)
        );
    }

    #[test]
    fn fixture_artifacts_cover_every_block_and_tip_epoch() -> Result<(), Box<dyn Error>> {
        let fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
        let chain_epoch = fixture
            .chain_epoch(ChainEpochId::new(7))
            .ok_or("chain epoch should be available for a 4-block fixture")?;

        assert_eq!(fixture.block_header_artifacts().len(), 4);
        assert_eq!(fixture.block_replay_envelopes().len(), 4);
        assert_eq!(fixture.compact_block_artifacts().len(), 4);
        assert_eq!(chain_epoch.visible_tip_height.value(), 4);
        assert_eq!(chain_epoch.network, Network::ZcashRegtest);

        Ok(())
    }

    #[test]
    fn replay_envelopes_preserve_transaction_order_and_intrinsic_balances_while_blobs_stay_separate()
    -> Result<(), Box<dyn Error>> {
        let base_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let block = base_fixture
            .block_at(BlockHeight::new(1))
            .ok_or("fixture must contain block 1")?
            .clone();
        let first_transaction_id = TransactionId::from_bytes([0x11; 32]);
        let second_transaction_id = TransactionId::from_bytes([0x22; 32]);
        let intrinsic_value_balances = TransactionIntrinsicValueBalances::new(-1, 2, -3, 4);
        let first_transaction = FixtureTransactionRows::from_raw_transaction(
            first_transaction_id,
            block.height,
            block.hash,
            0,
            b"first-retained-transaction".to_vec(),
        );
        let second_transaction = FixtureTransactionRows::from_raw_transaction(
            second_transaction_id,
            block.height,
            block.hash,
            1,
            b"retained-transaction".to_vec(),
        )
        .with_intrinsic_value_balances(intrinsic_value_balances);
        let fixture = base_fixture
            .with_raw_blob_retention(RawBlobRetention::All)
            .with_transaction_rows(second_transaction)
            .with_transaction_rows(first_transaction);

        let replay_envelopes = fixture.block_replay_envelopes();
        let replay = decode_canonical_block_replay(
            replay_envelopes
                .first()
                .ok_or("replay envelopes must contain block 1")?
                .as_bytes(),
        )?;

        assert_eq!(
            replay
                .facts()
                .transactions
                .iter()
                .map(|transaction| transaction.public_facts.transaction_id)
                .collect::<Vec<_>>(),
            vec![first_transaction_id, second_transaction_id]
        );
        assert_eq!(
            replay.facts().transactions[1].intrinsic_value_balances,
            intrinsic_value_balances
        );
        let transaction_blobs = fixture
            .canonical_transaction_rows()
            .into_iter()
            .filter_map(|transaction_rows| transaction_rows.blob)
            .collect::<Vec<_>>();
        assert_eq!(transaction_blobs.len(), 2);
        assert_eq!(
            transaction_blobs[1].raw_transaction_bytes,
            b"retained-transaction"
        );

        Ok(())
    }
}
