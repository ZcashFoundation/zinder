//! A competing branch of fixture blocks for a deep canonical reorg.
//!
//! [`ForkedFixtureSource`] presents the node view after a fork: the fixture
//! below the fork, then a rewritten copy of every block from the fork height
//! through one block past the replaced tip. Each rewritten block links to the
//! rewritten block before it, so its hash differs from the fixture block at
//! the same height, and the canonical follower sees a longer branch it must
//! switch to. Shielded data is never touched, so every rewritten block keeps
//! the node's tree state and only its block identity changes.

use std::{collections::BTreeMap, collections::HashMap, num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use clap::ValueEnum;
use serde::Serialize;
use zebra_chain::{
    block::{Block, merkle::Root},
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{self, Transaction},
};
use zinder_bench::{BenchError, tip_replay_checkpoints::TipReplayNodeSource};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, NetworkUpgradeActivations,
    ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError, SourceSubtreeRoots,
};

/// Byte offset of `lock_time` in a version 5 or later transaction.
const V5_LOCK_TIME_OFFSET: usize = 12;
/// Bytes of a serialized transparent outpoint: a txid and an output index.
const OUTPOINT_BYTES: usize = 36;

/// New txid of every transaction a branch renamed, keyed by its fixture txid.
type Renames = HashMap<transaction::Hash, transaction::Hash>;

/// How the competing branch differs from the fixture blocks it replaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ForkKind {
    /// Every transaction stays; only block identities change.
    SameTransactions,
    /// Every coinbase gets a new txid, and so does every transaction that
    /// spends an output the branch renamed.
    DistinctCoinbases,
}

/// What the rewrite changed across the branch.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct ForkRewrite {
    /// Rewritten blocks, including the one past the replaced tip.
    pub(crate) blocks: u32,
    /// Coinbase transactions given a new txid.
    pub(crate) coinbases: u32,
    /// Non-coinbase transactions given a new txid because they spend a
    /// renamed output.
    pub(crate) spending_transactions: u32,
    /// Transparent inputs relinked to a renamed output.
    pub(crate) relinked_inputs: u32,
    /// Serialized bytes of every rewritten block.
    pub(crate) block_bytes: u64,
}

/// Node view of the fixture after a fork at `first`.
pub(crate) struct ForkedFixtureSource {
    fixture: TipReplayNodeSource,
    kind: ForkKind,
    first: BlockHeight,
    tip: BlockHeight,
    /// Rewritten block hash at each height of the branch.
    hashes: BTreeMap<BlockHeight, BlockHash>,
    renames: Renames,
    rewrite: ForkRewrite,
}

impl ForkedFixtureSource {
    /// Rewrites fixture blocks `first..=tip` into one connected branch.
    pub(crate) async fn build(
        fixture: TipReplayNodeSource,
        kind: ForkKind,
        first: BlockHeight,
        tip: BlockHeight,
    ) -> Result<Self, BenchError> {
        let mut branch_renames = HashMap::new();
        let mut hashes = BTreeMap::new();
        let mut rewrite = ForkRewrite::default();
        let mut parent = None;
        let mut height = first;
        while height <= tip {
            let original = fixture.fetch_block_at(height).await?;
            let (rewritten, block_renames) = rewrite_block(
                &original,
                parent.unwrap_or(original.parent_hash),
                kind,
                &branch_renames,
                &mut rewrite,
            )?;
            branch_renames.extend(block_renames);
            rewrite.blocks += 1;
            rewrite.block_bytes += u64::try_from(rewritten.raw_block_bytes.len()).unwrap_or(0);
            hashes.insert(height, rewritten.hash);
            parent = Some(rewritten.hash);
            height = height.next().ok_or_else(|| {
                BenchError::invalid_argument("fork tip exceeds the height domain")
            })?;
        }
        Ok(Self {
            fixture,
            kind,
            first,
            tip,
            hashes,
            renames: branch_renames,
            rewrite,
        })
    }

    /// What the rewrite changed.
    pub(crate) const fn rewrite(&self) -> ForkRewrite {
        self.rewrite
    }

    /// The rewritten branch tip.
    pub(crate) fn tip(&self) -> Result<BlockId, BenchError> {
        self.forked_id(self.tip)
            .ok_or_else(|| BenchError::tip_replay("the fork lost its tip"))
    }

    fn forked_id(&self, height: BlockHeight) -> Option<BlockId> {
        self.hashes
            .get(&height)
            .map(|hash| BlockId::new(height, *hash))
    }

    fn forked_block(&self, original: &SourceBlock) -> Result<SourceBlock, SourceError> {
        let parent = if original.height == self.first {
            original.parent_hash
        } else {
            self.hashes
                .range(..original.height)
                .next_back()
                .map(|(_, hash)| *hash)
                .ok_or_else(|| fork_source_error(original.height, "the fork has no parent"))?
        };
        let mut rewrite = ForkRewrite::default();
        let (rewritten, _) =
            rewrite_block(original, parent, self.kind, &self.renames, &mut rewrite)
                .map_err(|error| fork_source_error(original.height, &error.to_string()))?;
        if Some(rewritten.hash) != self.hashes.get(&original.height).copied() {
            return Err(fork_source_error(
                original.height,
                "the rewritten block differs from the one the fork was built from",
            ));
        }
        Ok(rewritten)
    }

    fn in_branch(&self, height: BlockHeight) -> bool {
        self.first <= height && height <= self.tip
    }
}

#[async_trait]
impl NodeSource for ForkedFixtureSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.fixture.capabilities()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height > self.tip {
            return Err(SourceError::BlockUnavailable {
                height,
                reason: "the forked fixture ends at its branch tip".to_owned(),
            });
        }
        let original = self.fixture.fetch_block_at(height).await?;
        if self.in_branch(height) {
            self.forked_block(&original)
        } else {
            Ok(original)
        }
    }

    async fn fetch_chain_segment(
        &self,
        _limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        Err(SourceError::SourceProtocolMismatch {
            reason: "the forked fixture serves blocks one at a time",
        })
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        let checkpoint = self
            .fixture
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await?;
        match self.forked_id(height) {
            Some(block_id) => Ok(CommitmentTreeCheckpoint::new(
                block_id,
                checkpoint.block_time_seconds,
                checkpoint.frontiers,
            )),
            None => Ok(checkpoint),
        }
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.forked_id(self.tip)
            .ok_or_else(|| fork_source_error(self.tip, "the fork lost its tip"))
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fixture
            .fetch_subtree_roots(protocol, start_index, max_entries)
            .await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fixture.fetch_subtree_root_range(range).await
    }
}

fn fork_source_error(height: BlockHeight, reason: &str) -> SourceError {
    SourceError::BlockUnavailable {
        height,
        reason: format!("forked fixture block: {reason}"),
    }
}

/// Rewrites one fixture block onto `parent`, returning it and each txid it
/// renames. `earlier` holds the renames of earlier branch blocks.
fn rewrite_block(
    original: &SourceBlock,
    parent: BlockHash,
    kind: ForkKind,
    earlier: &Renames,
    rewrite: &mut ForkRewrite,
) -> Result<(SourceBlock, Renames), BenchError> {
    let block = Block::zcash_deserialize(original.raw_block_bytes.as_slice())
        .map_err(|source| fork_error(original.height, &source.to_string()))?;
    if block.zcash_serialize_to_vec().ok().as_deref() != Some(original.raw_block_bytes.as_slice()) {
        return Err(fork_error(
            original.height,
            "the fixture block does not reserialize to its own bytes",
        ));
    }
    let mut transactions = Vec::with_capacity(block.transactions.len());
    let mut block_renames = Renames::new();
    for (index, transaction) in block.transactions.iter().enumerate() {
        let rewritten = match kind {
            ForkKind::SameTransactions => None,
            ForkKind::DistinctCoinbases if index == 0 => {
                rewrite.coinbases += 1;
                Some(rename_coinbase(original.height, transaction)?)
            }
            ForkKind::DistinctCoinbases => relink_inputs(
                original.height,
                transaction,
                |txid| {
                    block_renames
                        .get(txid)
                        .or_else(|| earlier.get(txid))
                        .copied()
                },
                &mut rewrite.relinked_inputs,
            )?,
        };
        match rewritten {
            Some(rewritten) => {
                if index != 0 {
                    rewrite.spending_transactions += 1;
                }
                block_renames.insert(transaction.hash(), rewritten.hash());
                transactions.push(Arc::new(rewritten));
            }
            None => transactions.push(Arc::clone(transaction)),
        }
    }
    let mut header = *block.header;
    header.previous_block_hash = zebra_chain::block::Hash(parent.as_bytes());
    header.merkle_root = transactions
        .iter()
        .map(|transaction| transaction.hash())
        .collect::<Root>();
    if kind == ForkKind::SameTransactions {
        // The first block keeps its parent and every transaction, so only a
        // different nonce gives it a new identity.
        header.nonce.0[0] ^= 0xff;
    }
    let rewritten = Block {
        header: Arc::new(header),
        transactions,
    };
    let raw_block_bytes = rewritten
        .zcash_serialize_to_vec()
        .map_err(|source| fork_error(original.height, &source.to_string()))?;
    let block =
        SourceBlock::from_raw_block_bytes(original.network, original.height, raw_block_bytes)?;
    if block.parent_hash != parent || block.hash == original.hash {
        return Err(fork_error(
            original.height,
            "the rewritten block does not form a new branch",
        ));
    }
    Ok((block, block_renames))
}

/// Gives a coinbase a new txid by changing its lock time.
fn rename_coinbase(height: BlockHeight, coinbase: &Transaction) -> Result<Transaction, BenchError> {
    let mut bytes = coinbase
        .zcash_serialize_to_vec()
        .map_err(|source| fork_error(height, &source.to_string()))?;
    let offset = lock_time_offset(&bytes)
        .ok_or_else(|| fork_error(height, "the coinbase has no parseable lock time"))?;
    let lock_time = coinbase.raw_lock_time() ^ 1;
    bytes
        .get_mut(offset..offset + 4)
        .ok_or_else(|| fork_error(height, "the coinbase is shorter than its lock time"))?
        .copy_from_slice(&lock_time.to_le_bytes());
    let renamed = Transaction::zcash_deserialize(bytes.as_slice())
        .map_err(|source| fork_error(height, &source.to_string()))?;
    if renamed.raw_lock_time() != lock_time || renamed.hash() == coinbase.hash() {
        return Err(fork_error(height, "the coinbase lock time did not move"));
    }
    Ok(renamed)
}

/// Byte offset of `lock_time` in a serialized transaction.
///
/// Version 5 and later put it after the header, version group, and consensus
/// branch fields. Earlier versions put it after the transparent inputs and
/// outputs, behind a version group field when the transaction is overwintered.
fn lock_time_offset(transaction: &[u8]) -> Option<usize> {
    let header = u32::from_le_bytes(transaction.get(..4)?.try_into().ok()?);
    let overwintered = header >> 31 == 1;
    if overwintered && header & 0x7fff_ffff >= 5 {
        return Some(V5_LOCK_TIME_OFFSET);
    }
    let mut cursor = if overwintered { 8 } else { 4 };
    for _ in 0..read_compact_size(transaction, &mut cursor)? {
        cursor = cursor.checked_add(OUTPOINT_BYTES)?;
        let script = usize::try_from(read_compact_size(transaction, &mut cursor)?).ok()?;
        cursor = cursor.checked_add(script)?.checked_add(4)?;
    }
    for _ in 0..read_compact_size(transaction, &mut cursor)? {
        cursor = cursor.checked_add(8)?;
        let script = usize::try_from(read_compact_size(transaction, &mut cursor)?).ok()?;
        cursor = cursor.checked_add(script)?;
    }
    (cursor.checked_add(4)? <= transaction.len()).then_some(cursor)
}

/// Reads one Bitcoin `CompactSize` at `cursor` and advances past it.
fn read_compact_size(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let first = *bytes.get(*cursor)?;
    let width = match first {
        0xfd => 2,
        0xfe => 4,
        0xff => 8,
        _ => {
            *cursor += 1;
            return Some(u64::from(first));
        }
    };
    let start = cursor.checked_add(1)?;
    let encoded = bytes.get(start..start.checked_add(width)?)?;
    let mut little_endian = [0_u8; 8];
    little_endian[..width].copy_from_slice(encoded);
    *cursor = start + width;
    Some(u64::from_le_bytes(little_endian))
}

/// Points every input that spends a renamed output at its new txid.
///
/// Returns `None` when the transaction spends no renamed output.
fn relink_inputs(
    height: BlockHeight,
    spender: &Transaction,
    renamed: impl Fn(&transaction::Hash) -> Option<transaction::Hash>,
    relinked_inputs: &mut u32,
) -> Result<Option<Transaction>, BenchError> {
    let relinks: Vec<_> = spender
        .spent_outpoints()
        .filter_map(|outpoint| renamed(&outpoint.hash).map(|new_hash| (outpoint, new_hash)))
        .collect();
    if relinks.is_empty() {
        return Ok(None);
    }
    let mut bytes = spender
        .zcash_serialize_to_vec()
        .map_err(|source| fork_error(height, &source.to_string()))?;
    // Inputs serialize in spend order, so each search resumes after the
    // previous match and the transaction is scanned once. A match outside an
    // input leaves the real input unpatched, which the check below rejects.
    let mut cursor = 0;
    for (outpoint, new_hash) in &relinks {
        let mut serialized = [0_u8; OUTPOINT_BYTES];
        serialized[..32].copy_from_slice(&outpoint.hash.0);
        serialized[32..].copy_from_slice(&outpoint.index.to_le_bytes());
        let position = bytes
            .get(cursor..)
            .and_then(|rest| {
                rest.windows(OUTPOINT_BYTES)
                    .position(|window| window == serialized)
            })
            .map(|offset| cursor + offset)
            .ok_or_else(|| {
                fork_error(height, "a spent outpoint is missing from its transaction")
            })?;
        bytes[position..position + 32].copy_from_slice(&new_hash.0);
        cursor = position + OUTPOINT_BYTES;
        *relinked_inputs += 1;
    }
    let relinked = Transaction::zcash_deserialize(bytes.as_slice())
        .map_err(|source| fork_error(height, &source.to_string()))?;
    let expected: Vec<_> = spender
        .spent_outpoints()
        .map(|outpoint| renamed(&outpoint.hash).unwrap_or(outpoint.hash))
        .collect();
    if relinked
        .spent_outpoints()
        .map(|outpoint| outpoint.hash)
        .ne(expected)
    {
        return Err(fork_error(height, "relinked inputs spend other outputs"));
    }
    Ok(Some(relinked))
}

fn fork_error(height: BlockHeight, reason: &str) -> BenchError {
    BenchError::tip_replay(format!(
        "cannot fork fixture block {}: {reason}",
        height.value()
    ))
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use zebra_chain::{
        serialization::ZcashDeserialize,
        transaction::{self, Transaction},
    };
    use zinder_core::BlockHeight;

    use super::{Renames, relink_inputs};

    /// Enough inputs that rescanning the whole transaction once per input
    /// outlasts the test runner's kill window.
    const DENSE_SPEND_INPUTS: u16 = 10_000;

    #[test]
    fn relinks_every_input_of_a_dense_spend() -> Result<(), Box<dyn std::error::Error>> {
        let spent: Vec<transaction::Hash> = (0..DENSE_SPEND_INPUTS)
            .map(|index| transaction::Hash(Sha256::digest(index.to_le_bytes()).into()))
            .collect();
        let renames: Renames = spent
            .iter()
            .map(|hash| {
                let mut renamed = hash.0;
                renamed[0] ^= 0xff;
                (*hash, transaction::Hash(renamed))
            })
            .collect();
        let spender = Transaction::zcash_deserialize(version_1_spend(&spent).as_slice())?;

        let mut relinked_inputs = 0;
        let relinked = relink_inputs(
            BlockHeight::new(1),
            &spender,
            |txid| renames.get(txid).copied(),
            &mut relinked_inputs,
        )?
        .ok_or("every input spends a renamed output")?;

        assert_eq!(relinked_inputs, u32::from(DENSE_SPEND_INPUTS));
        assert!(
            relinked
                .spent_outpoints()
                .map(|outpoint| (outpoint.hash, outpoint.index))
                .eq(spent
                    .iter()
                    .map(|hash| (renames.get(hash).copied().unwrap_or(*hash), 0)))
        );
        Ok(())
    }

    /// A version 1 transaction that spends output 0 of each of `spent`.
    fn version_1_spend(spent: &[transaction::Hash]) -> Vec<u8> {
        let mut bytes = 1_u32.to_le_bytes().to_vec();
        bytes.push(0xfd);
        bytes.extend_from_slice(&u16::try_from(spent.len()).unwrap_or(u16::MAX).to_le_bytes());
        for hash in spent {
            bytes.extend_from_slice(&hash.0);
            bytes.extend_from_slice(&0_u32.to_le_bytes());
            bytes.push(0);
            bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        bytes.push(1);
        bytes.extend_from_slice(&1_u64.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes
    }
}
