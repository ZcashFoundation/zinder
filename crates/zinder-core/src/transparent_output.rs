//! Transparent output facts and address indexes.

use sha2::{Digest, Sha256};

use crate::{BlockHash, BlockHeight, BlockId, TransactionId};

/// Hash of a transparent address `scriptPubKey`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransparentAddressScriptHash([u8; 32]);

impl TransparentAddressScriptHash {
    /// Creates a script hash from fixed 32-byte material.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the script hash bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Hashes a transparent `scriptPubKey` into its canonical script hash.
    #[must_use]
    pub fn of_script_pub_key(script_pub_key: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(script_pub_key);
        Self::from_bytes(hasher.finalize().into())
    }
}

/// Transparent transaction output identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransparentOutPoint {
    /// Transaction that created the transparent output.
    pub transaction_id: TransactionId,
    /// Output index within the transaction.
    pub output_index: u32,
}

impl TransparentOutPoint {
    /// Sentinel outpoint used by Zcash transaction inputs to mark coinbase.
    pub const COINBASE_SENTINEL: Self = Self::new(TransactionId::from_bytes([0u8; 32]), u32::MAX);

    /// Creates a transparent outpoint.
    #[must_use]
    pub const fn new(transaction_id: TransactionId, output_index: u32) -> Self {
        Self {
            transaction_id,
            output_index,
        }
    }

    /// Returns true when this outpoint is the Zcash coinbase input sentinel.
    #[must_use]
    pub fn is_coinbase_sentinel(self) -> bool {
        self == Self::COINBASE_SENTINEL
    }
}

/// Transparent input fact local to a mined transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentInputFact {
    /// Transparent input index within the transaction.
    pub input_index: u32,
    /// Output consumed by this transparent input.
    pub spent_outpoint: TransparentOutPoint,
}

impl TransparentInputFact {
    /// Creates an ordered transparent input fact.
    #[must_use]
    pub const fn new(input_index: u32, spent_outpoint: TransparentOutPoint) -> Self {
        Self {
            input_index,
            spent_outpoint,
        }
    }
}

/// Transparent output fact local to a mined transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentOutputFact {
    /// Transparent output index within the transaction.
    pub output_index: u32,
    /// Output value in zatoshis.
    pub value_zat: u64,
    /// Raw scriptPubKey bytes.
    pub script_pub_key: Vec<u8>,
    /// Hash of the transparent output script.
    pub address_script_hash: TransparentAddressScriptHash,
}

impl TransparentOutputFact {
    /// Creates an ordered transparent output fact.
    #[must_use]
    pub fn new(
        output_index: u32,
        value_zat: u64,
        script_pub_key: impl Into<Vec<u8>>,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Self {
        Self {
            output_index,
            value_zat,
            script_pub_key: script_pub_key.into(),
            address_script_hash,
        }
    }
}

/// Canonical transparent output fact keyed by outpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentOutputArtifact {
    /// Output identity.
    pub outpoint: TransparentOutPoint,
    /// Output value in zatoshis.
    pub value_zat: u64,
    /// Raw scriptPubKey bytes.
    pub script_pub_key: Vec<u8>,
    /// Hash of the transparent output script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Height of the block that mined the output.
    pub block_height: BlockHeight,
    /// Hash of the block that mined the output.
    pub block_hash: BlockHash,
}

impl TransparentOutputArtifact {
    /// Creates a transparent output artifact.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "transparent output artifacts are immutable persisted facts"
    )]
    pub fn new(
        outpoint: TransparentOutPoint,
        value_zat: u64,
        script_pub_key: impl Into<Vec<u8>>,
        address_script_hash: TransparentAddressScriptHash,
        block_height: BlockHeight,
        block_hash: BlockHash,
    ) -> Self {
        Self {
            outpoint,
            value_zat,
            script_pub_key: script_pub_key.into(),
            address_script_hash,
            block_height,
            block_hash,
        }
    }
}

/// Unspent transparent output projected for one transparent address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentUnspentOutput {
    /// Hash of the transparent output script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Raw `scriptPubKey` bytes for the output.
    pub script_pub_key: Vec<u8>,
    /// Output identity.
    pub outpoint: TransparentOutPoint,
    /// Output value in zatoshis.
    pub value_zat: u64,
    /// Height of the block that mined the output.
    pub block_height: BlockHeight,
    /// Hash of the block that mined the output.
    pub block_hash: BlockHash,
}

impl TransparentUnspentOutput {
    /// Creates an unspent transparent output row.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "unspent transparent output rows are immutable persisted facts"
    )]
    pub fn new(
        address_script_hash: TransparentAddressScriptHash,
        script_pub_key: impl Into<Vec<u8>>,
        outpoint: TransparentOutPoint,
        value_zat: u64,
        block_height: BlockHeight,
        block_hash: BlockHash,
    ) -> Self {
        Self {
            address_script_hash,
            script_pub_key: script_pub_key.into(),
            outpoint,
            value_zat,
            block_height,
            block_hash,
        }
    }
}

/// Maximum number of outpoints one transparent outpoint spend-status request
/// may carry.
///
/// The bound keeps one request to a fixed number of point reads. Typed clients
/// split larger batches into requests of at most this size at one pinned epoch.
pub const MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST: usize = 1_024;

/// Spend status of one transparent outpoint on the canonical chain of one
/// chain epoch.
///
/// The status covers mined transactions only, from height 1 through the
/// epoch's visible tip. A spend that is still in the mempool leaves the output
/// [`Self::Unspent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransparentOutpointSpendStatus {
    /// A mined transaction created the output, and no mined transaction
    /// spends it.
    Unspent,
    /// A mined transaction spends the output.
    SpentBy {
        /// Transaction that spends the output.
        transaction_id: TransactionId,
        /// Transparent input index of the spend within that transaction.
        input_index: u32,
        /// Block that mined the spending transaction.
        block: BlockId,
    },
    /// No mined transaction created an output at this outpoint.
    ///
    /// This includes the coinbase input sentinel and the genesis coinbase
    /// output, which is unspendable and never indexed.
    NotFound,
}

/// Canonical transparent spend fact resolved from a spend-index row and the
/// transparent output it consumes.
///
/// The ingest writer persists this fact once with the chain epoch that mined
/// the spending transaction. Materialized-view consumers read it directly instead of
/// rehydrating spend context from transparent-output rows during replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentSpendFact {
    /// Output consumed by this transparent input.
    pub spent_outpoint: TransparentOutPoint,
    /// Transparent input index within the spending transaction.
    pub input_index: u32,
    /// Spending transaction identifier.
    pub spending_transaction_id: TransactionId,
    /// Spending transaction's block-local index.
    pub tx_index_in_block: u32,
    /// Height of the block that mined the spending transaction.
    pub block_height: BlockHeight,
    /// Hash of the block that mined the spending transaction.
    pub block_hash: BlockHash,
    /// Value of the spent output in zatoshis.
    pub spent_value_zat: u64,
    /// Hash of the spent output script.
    pub spent_address_script_hash: TransparentAddressScriptHash,
    /// Height of the block that mined the spent output.
    pub spent_block_height: BlockHeight,
    /// Hash of the block that mined the spent output.
    pub spent_block_hash: BlockHash,
}

impl TransparentSpendFact {
    /// Creates a resolved transparent spend fact.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "transparent spend facts are immutable persisted facts"
    )]
    pub fn new(
        spent_outpoint: TransparentOutPoint,
        input_index: u32,
        spending_transaction_id: TransactionId,
        tx_index_in_block: u32,
        block_height: BlockHeight,
        block_hash: BlockHash,
        spent_value_zat: u64,
        spent_address_script_hash: TransparentAddressScriptHash,
        spent_block_height: BlockHeight,
        spent_block_hash: BlockHash,
    ) -> Self {
        Self {
            spent_outpoint,
            input_index,
            spending_transaction_id,
            tx_index_in_block,
            block_height,
            block_hash,
            spent_value_zat,
            spent_address_script_hash,
            spent_block_height,
            spent_block_hash,
        }
    }

    /// Builds a resolved spend fact from a transparent input and its consumed
    /// canonical output.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "the spending input identity and consumed output identity are both persisted"
    )]
    pub fn from_input_and_output(
        spent_outpoint: TransparentOutPoint,
        input_index: u32,
        spending_transaction_id: TransactionId,
        tx_index_in_block: u32,
        block_height: BlockHeight,
        block_hash: BlockHash,
        output: &TransparentOutputArtifact,
    ) -> Self {
        Self::new(
            spent_outpoint,
            input_index,
            spending_transaction_id,
            tx_index_in_block,
            block_height,
            block_hash,
            output.value_zat,
            output.address_script_hash,
            output.block_height,
            output.block_hash,
        )
    }
}

/// Canonical-chain spend of one transparent outpoint: the spending
/// transaction, its input index, and the block that mined it.
///
/// Derived from a [`TransparentSpendFact`] by keeping the spending-side
/// identity and dropping the spent-output value and script. The durable
/// `transparent_outpoint_spend` Explorer view stores these entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentSpendEntry {
    /// Outpoint that was spent.
    pub spent_outpoint: TransparentOutPoint,
    /// Transaction that spends the outpoint.
    pub spending_transaction_id: TransactionId,
    /// Transparent input index of the spend within the spending transaction.
    pub input_index: u32,
    /// Height of the block that mined the spending transaction.
    pub spending_block_height: BlockHeight,
    /// Hash of the block that mined the spending transaction.
    pub spending_block_hash: BlockHash,
}

impl TransparentSpendEntry {
    /// Projects a [`TransparentSpendFact`] onto the reverse-spend entry.
    #[must_use]
    pub fn from_spend_fact(fact: &TransparentSpendFact) -> Self {
        Self {
            spent_outpoint: fact.spent_outpoint,
            spending_transaction_id: fact.spending_transaction_id,
            input_index: fact.input_index,
            spending_block_height: fact.block_height,
            spending_block_hash: fact.block_hash,
        }
    }
}
