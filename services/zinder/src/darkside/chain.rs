use std::sync::Arc;

use prost::Message;
use sha2::{Digest as _, Sha256};
use tonic::Status;
use zebra_chain::{
    block::{Block, Hash, Header},
    serialization::{ZcashDeserializeInto as _, ZcashSerialize as _},
    transaction::{Hash as TxHash, Transaction},
};
use zinder_core::{
    BlockHash, CompactShieldedAction, CompactTransactionData, TransactionId,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_ingest::compact_transaction_data;
use zinder_proto::compat::lightwalletd::{
    BlockId, ChainMetadata, CompactBlock, CompactOrchardAction, CompactSaplingOutput,
    CompactSaplingSpend, CompactTx, CompactTxIn, DarksideMetaState, RawTransaction, TxOut,
};

pub(super) const MAX_RAW_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_CHAIN_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_BLOCKS: usize = 4_096;

pub(super) struct FixtureTx {
    bytes: Vec<u8>,
    compact: CompactTx,
    pub(super) id: TransactionId,
    pub(super) coinbase: bool,
    pub(super) retained_bytes: usize,
}

impl FixtureTx {
    pub(super) fn parse(bytes: Vec<u8>) -> Result<Self, Status> {
        if bytes.len() > MAX_RAW_BYTES {
            return Err(Status::resource_exhausted(
                "transaction exceeds the fixture size limit",
            ));
        }
        let mut unread = bytes.as_slice();
        let tx: Transaction = (&mut unread)
            .zcash_deserialize_into()
            .map_err(|error| Status::invalid_argument(format!("invalid transaction: {error}")))?;
        if !unread.is_empty() {
            return Err(Status::invalid_argument("trailing bytes after transaction"));
        }
        Self::from_transaction(&tx, bytes)
    }

    fn from_transaction(tx: &Transaction, bytes: Vec<u8>) -> Result<Self, Status> {
        let id = TransactionId::from_bytes(tx.hash().0);
        let scan = compact_transaction_data(tx).map_err(|error| {
            Status::invalid_argument(format!("invalid compact transaction: {error}"))
        })?;
        let compact = compact_message(id, scan);
        // Include message allocation overhead even for transactions with tiny wire fields.
        let retained_bytes = bytes.len() + compact.encoded_len() * 3 + 512;
        Ok(Self {
            bytes,
            compact,
            id,
            coinbase: tx.is_coinbase(),
            retained_bytes,
        })
    }

    pub(super) fn raw(&self, height: u64) -> RawTransaction {
        RawTransaction {
            data: self.bytes.clone(),
            height,
        }
    }

    /// Returns the serialized transaction as staged in the fixture chain.
    pub(super) fn raw_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn txid_bytes(&self) -> &[u8] {
        &self.compact.txid
    }

    pub(super) fn compact(&self) -> CompactTx {
        self.compact.clone()
    }
}

fn compact_message(id: TransactionId, scan: CompactTransactionData) -> CompactTx {
    CompactTx {
        txid: encode_internal_transaction_id(id).to_vec(),
        spends: scan
            .sapling_spends
            .into_iter()
            .map(|spend| CompactSaplingSpend {
                nf: spend.nullifier.to_vec(),
            })
            .collect(),
        outputs: scan
            .sapling_outputs
            .into_iter()
            .map(|output| CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: scan.orchard_actions.iter().map(action_message).collect(),
        ironwood_actions: scan.ironwood_actions.iter().map(action_message).collect(),
        vin: scan
            .transparent_inputs
            .into_iter()
            .map(|input| CompactTxIn {
                prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                    .to_vec(),
                prevout_index: input.previous_output_index,
            })
            .collect(),
        vout: scan
            .transparent_outputs
            .into_iter()
            .map(|output| TxOut {
                value: output.value_zat,
                script_pub_key: output.script_pub_key,
            })
            .collect(),
        ..CompactTx::default()
    }
}

fn action_message(action: &CompactShieldedAction) -> CompactOrchardAction {
    CompactOrchardAction {
        nullifier: action.nullifier.to_vec(),
        cmx: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

#[derive(Clone)]
enum BlockTemplate {
    Serialized(Box<Header>),
    Synthetic(i32),
}

#[derive(Clone)]
pub(super) struct FixtureBlock {
    pub(super) height: u64,
    pub(super) hash: BlockHash,
    parent: BlockHash,
    template: BlockTemplate,
    time: u32,
    pub(super) transactions: Vec<Arc<FixtureTx>>,
    tree_sizes: CommitmentTreeSizes,
    retained_bytes: usize,
    compact_bytes: usize,
    appended_transactions: bool,
}

impl FixtureBlock {
    pub(super) fn synthetic(height: u64, nonce: i32) -> Self {
        Self::new(height, BlockTemplate::Synthetic(nonce), 1)
    }

    fn new(height: u64, template: BlockTemplate, time: u32) -> Self {
        Self {
            height,
            hash: BlockHash::from_bytes([0; 32]),
            parent: BlockHash::from_bytes([0; 32]),
            template,
            time,
            transactions: Vec::new(),
            tree_sizes: CommitmentTreeSizes::default(),
            retained_bytes: 2_048,
            compact_bytes: 256,
            appended_transactions: false,
        }
    }

    pub(super) fn parse(hex_block: &str) -> Result<Self, Status> {
        let bytes = decode_fixture(hex_block)?;
        let mut unread = bytes.as_slice();
        let block: Block = (&mut unread)
            .zcash_deserialize_into()
            .map_err(|error| Status::invalid_argument(format!("invalid block: {error}")))?;
        if !unread.is_empty() {
            return Err(Status::invalid_argument("trailing bytes after block"));
        }
        let height = block
            .coinbase_height()
            .ok_or_else(|| Status::invalid_argument("block lacks a coinbase height"))?;
        let time = u32::try_from(block.header.time.timestamp())
            .map_err(|_| Status::invalid_argument("block timestamp is outside the wire range"))?;
        let mut fixture = Self::new(
            u64::from(height.0),
            BlockTemplate::Serialized(Box::new(*block.header)),
            time,
        );
        for tx in &block.transactions {
            let mut raw_tx = Vec::new();
            tx.zcash_serialize(&mut raw_tx).map_err(|error| {
                Status::invalid_argument(format!("cannot encode transaction: {error}"))
            })?;
            fixture.push_tx(Arc::new(FixtureTx::from_transaction(tx, raw_tx)?))?;
        }
        Ok(fixture)
    }

    fn push_tx(&mut self, tx: Arc<FixtureTx>) -> Result<(), Status> {
        let compact_bytes = self.compact_bytes + tx.compact.encoded_len() + 24;
        if compact_bytes > super::MAX_MESSAGE_BYTES {
            return Err(Status::resource_exhausted(
                "compact block exceeds the response size limit",
            ));
        }
        self.retained_bytes += tx.retained_bytes;
        self.compact_bytes = compact_bytes;
        self.transactions.push(tx);
        Ok(())
    }

    fn original_parent(&self) -> BlockHash {
        match &self.template {
            BlockTemplate::Serialized(header) => {
                BlockHash::from_bytes(header.previous_block_hash.0)
            }
            BlockTemplate::Synthetic(_) => BlockHash::from_bytes([0; 32]),
        }
    }

    fn position(
        &mut self,
        parent: BlockHash,
        mut tree_sizes: CommitmentTreeSizes,
    ) -> Result<(), Status> {
        self.parent = parent;
        self.hash = match &self.template {
            BlockTemplate::Serialized(header) => {
                let mut header = **header;
                header.previous_block_hash = Hash(encode_internal_block_hash(parent));
                if self.appended_transactions {
                    header.merkle_root = self
                        .transactions
                        .iter()
                        .map(|tx| TxHash(encode_internal_transaction_id(tx.id)))
                        .collect();
                }
                BlockHash::from_bytes(header.hash().0)
            }
            BlockTemplate::Synthetic(nonce) => {
                let mut hash = Sha256::new();
                hash.update(b"zinder-darkside-block");
                hash.update(nonce.to_le_bytes());
                hash.update(self.height.to_le_bytes());
                hash.update(encode_internal_block_hash(parent));
                for tx in &self.transactions {
                    hash.update(encode_internal_transaction_id(tx.id));
                }
                BlockHash::from_bytes(hash.finalize().into())
            }
        };
        for tx in &self.transactions {
            let compact = &tx.compact;
            tree_sizes.sapling = add_commitments(tree_sizes.sapling, compact.outputs.len())?;
            tree_sizes.orchard = add_commitments(tree_sizes.orchard, compact.actions.len())?;
            tree_sizes.ironwood =
                add_commitments(tree_sizes.ironwood, compact.ironwood_actions.len())?;
        }
        self.tree_sizes = tree_sizes;
        Ok(())
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(super) fn id(&self) -> BlockId {
        BlockId {
            height: self.height,
            hash: encode_internal_block_hash(self.hash).to_vec(),
        }
    }

    pub(super) fn compact(&self) -> CompactBlock {
        let first_index = u64::from(matches!(self.template, BlockTemplate::Synthetic(_)));
        let id = self.id();
        CompactBlock {
            height: id.height,
            hash: id.hash,
            prev_hash: encode_internal_block_hash(self.parent).to_vec(),
            time: self.time,
            vtx: self
                .transactions
                .iter()
                .zip(first_index..)
                .map(|(tx, index)| {
                    let mut compact = tx.compact();
                    compact.index = index;
                    compact
                })
                .collect(),
            chain_metadata: Some(ChainMetadata {
                sapling_commitment_tree_size: self.tree_sizes.sapling,
                orchard_commitment_tree_size: self.tree_sizes.orchard,
                ironwood_commitment_tree_size: self.tree_sizes.ironwood,
            }),
            ..CompactBlock::default()
        }
    }
}

fn add_commitments(size: u32, additions: usize) -> Result<u32, Status> {
    u32::try_from(additions)
        .ok()
        .and_then(|additions| size.checked_add(additions))
        .ok_or_else(|| Status::out_of_range("commitment tree size overflow"))
}

pub(super) fn decode_fixture(line: &str) -> Result<Vec<u8>, Status> {
    if line.len() > 2 * MAX_RAW_BYTES {
        return Err(Status::resource_exhausted(
            "hex fixture exceeds the size limit",
        ));
    }
    hex::decode(line).map_err(|_| Status::invalid_argument("fixture must be hexadecimal"))
}

#[derive(Clone, Copy, Default)]
struct CommitmentTreeSizes {
    sapling: u32,
    orchard: u32,
    ironwood: u32,
}

#[derive(Default)]
pub(super) struct Chain {
    pub(super) metadata: Option<DarksideMetaState>,
    pub(super) active: Vec<Arc<FixtureBlock>>,
    pub(super) tip: Option<u64>,
    staged_blocks: Vec<Arc<FixtureBlock>>,
    staged_transactions: Vec<(u64, Arc<FixtureTx>)>,
}

impl Chain {
    pub(super) fn stage_blocks(&mut self, blocks: Vec<Arc<FixtureBlock>>) -> Result<(), Status> {
        if self.staged_blocks.len() + blocks.len() > MAX_BLOCKS {
            return Err(Status::resource_exhausted("too many staged blocks"));
        }
        self.check_budget(blocks.iter().map(|block| block.retained_bytes).sum())?;
        self.staged_blocks.extend(blocks);
        Ok(())
    }

    pub(super) fn stage_transactions(
        &mut self,
        transactions: Vec<(u64, Arc<FixtureTx>)>,
    ) -> Result<(), Status> {
        self.check_budget(transactions.iter().map(|(_, tx)| tx.retained_bytes).sum())?;
        self.staged_transactions.extend(transactions);
        Ok(())
    }

    fn check_budget(&self, added_bytes: usize) -> Result<(), Status> {
        let block_bytes: usize = self
            .active
            .iter()
            .chain(&self.staged_blocks)
            .map(|block| block.retained_bytes)
            .sum();
        let transaction_bytes: usize = self
            .staged_transactions
            .iter()
            .map(|(_, tx)| tx.retained_bytes)
            .sum();
        if block_bytes + transaction_bytes + added_bytes > MAX_CHAIN_BYTES {
            return Err(Status::resource_exhausted(
                "chain fixture memory budget exhausted",
            ));
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, presented_height: u64) -> Result<(), Status> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("call Reset first"))?;
        let start = u64::try_from(metadata.sapling_activation)
            .map_err(|_| Status::internal("invalid activation height"))?;
        let mut active = self.active.clone();
        let mut changed = active.len();
        for block in &self.staged_blocks {
            let index = block_index(start, block.height)?;
            if index > active.len() {
                return Err(Status::failed_precondition(
                    "staged blocks leave a gap in the chain",
                ));
            }
            changed = changed.min(index);
            active.truncate(index);
            active.push(Arc::clone(block));
        }
        let highest = active
            .last()
            .ok_or_else(|| Status::failed_precondition("no active blocks"))?
            .height;
        if presented_height < start {
            return Err(Status::out_of_range(
                "presented height precedes Sapling activation",
            ));
        }
        for (height, tx) in &self.staged_transactions {
            let index = block_index(start, *height)?;
            let block = active.get_mut(index).ok_or_else(|| {
                Status::failed_precondition("staged transaction has no active block")
            })?;
            changed = changed.min(index);
            let block = Arc::make_mut(block);
            block.push_tx(Arc::clone(tx))?;
            block.appended_transactions = true;
        }
        let (mut parent, mut tree_sizes) = changed
            .checked_sub(1)
            .and_then(|index| active.get(index))
            .map_or_else(
                || {
                    let parent = active
                        .first()
                        .map_or(BlockHash::from_bytes([0; 32]), |block| {
                            block.original_parent()
                        });
                    (
                        parent,
                        CommitmentTreeSizes {
                            sapling: metadata.start_sapling_commitment_tree_size,
                            orchard: metadata.start_orchard_commitment_tree_size,
                            ironwood: metadata.start_ironwood_commitment_tree_size,
                        },
                    )
                },
                |block| (block.hash, block.tree_sizes),
            );
        for block in &mut active[changed..] {
            let block = Arc::make_mut(block);
            block.position(parent, tree_sizes)?;
            parent = block.hash;
            tree_sizes = block.tree_sizes;
        }
        self.active = active;
        self.tip = Some(presented_height.min(highest));
        self.staged_blocks.clear();
        self.staged_transactions.clear();
        Ok(())
    }

    pub(super) fn visible(&self) -> &[Arc<FixtureBlock>] {
        let count = self
            .active
            .partition_point(|block| self.tip.is_some_and(|tip| block.height <= tip));
        &self.active[..count]
    }
}

fn block_index(start: u64, height: u64) -> Result<usize, Status> {
    height
        .checked_sub(start)
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < MAX_BLOCKS)
        .ok_or_else(|| Status::out_of_range("height is outside the fixture chain window"))
}
