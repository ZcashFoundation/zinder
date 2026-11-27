//! Pool selection and nullifier-only compact scans.

use crate::{BlockHash, BlockHeight, TransactionId};

/// A pool that contributes fields to a compact block scan.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompactPool {
    /// Sapling spends and outputs.
    Sapling,
    /// Orchard actions.
    Orchard,
    /// Ironwood actions.
    Ironwood,
    /// Transparent inputs and outputs.
    Transparent,
}

/// Shielded nullifiers from a transaction in a canonical block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTransactionNullifiers {
    /// Zero-based consensus transaction position.
    pub index: u64,
    /// Consensus transaction identifier.
    pub transaction_id: TransactionId,
    /// Sapling spend nullifiers in consensus order.
    pub sapling_nullifiers: Vec<[u8; 32]>,
    /// Orchard action nullifiers in consensus order.
    pub orchard_nullifiers: Vec<[u8; 32]>,
    /// Ironwood action nullifiers in consensus order.
    pub ironwood_nullifiers: Vec<[u8; 32]>,
}

/// Canonical block identity and shielded nullifiers for spend detection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBlockNullifiers {
    /// Canonical block height.
    pub height: BlockHeight,
    /// Canonical block hash.
    pub block_hash: BlockHash,
    /// Canonical parent block hash.
    pub previous_block_hash: BlockHash,
    /// Block time in Unix seconds.
    pub time: u32,
    /// Transactions with nullifiers in the selected pools, in consensus order.
    pub transactions: Vec<CompactTransactionNullifiers>,
}
