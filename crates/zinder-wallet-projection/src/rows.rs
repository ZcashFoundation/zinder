//! Exact version-2 wallet projection query rows and durable byte layouts.

use zinder_core::wire::UtxoSetCommitmentElement;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, Network, TransactionId, TransparentAddressScriptHash,
    TransparentOutPoint,
};

use crate::WalletProjectionContractError;
use crate::contract_error::encoded_len;

const OUTPOINT_KEY_LEN: usize = 36;
const ADDRESS_UNSPENT_OUTPUT_KEY_LEN: usize = 72;
const ADDRESS_TRANSACTION_KEY_LEN: usize = 40;
const UNSPENT_OUTPUT_FIXED_VALUE_LEN: usize = 84;
const SPENT_OUTPUT_TRAILER_LEN: usize = 76;
const SOURCE_SEQUENCE_DIGEST_ENCODED_LEN: usize = 2 + 8 + 32;
const REORG_UNDO_FIXED_VALUE_LEN: usize = 32 + 32 + (SOURCE_SEQUENCE_DIGEST_ENCODED_LEN * 2) + 12;

/// Position of one transaction in the canonical chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletTransactionPosition {
    /// Transaction identifier.
    pub transaction_id: TransactionId,
    /// Block-local transaction index.
    pub tx_index_in_block: u32,
    /// Canonical block containing the transaction.
    pub block: BlockId,
}

impl WalletTransactionPosition {
    /// Creates a canonical transaction position.
    #[must_use]
    pub const fn new(
        transaction_id: TransactionId,
        tx_index_in_block: u32,
        block: BlockId,
    ) -> Self {
        Self {
            transaction_id,
            tx_index_in_block,
            block,
        }
    }
}

/// Full transparent output retained while it remains unspent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletUnspentOutput {
    /// Output identity.
    pub outpoint: TransparentOutPoint,
    /// Hash of the raw output script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Output value in zatoshi.
    pub value_zat: u64,
    /// Raw `scriptPubKey` bytes.
    pub script_pub_key: Vec<u8>,
    /// Transaction and block that created the output.
    pub created_at: WalletTransactionPosition,
}

impl WalletUnspentOutput {
    /// Creates an unspent wallet output.
    pub fn new(
        outpoint: TransparentOutPoint,
        address_script_hash: TransparentAddressScriptHash,
        value_zat: u64,
        script_pub_key: impl Into<Vec<u8>>,
        created_at: WalletTransactionPosition,
    ) -> Result<Self, WalletProjectionContractError> {
        if outpoint.transaction_id != created_at.transaction_id {
            return Err(WalletProjectionContractError::OutputCreatorMismatch);
        }
        Ok(Self {
            outpoint,
            address_script_hash,
            value_zat,
            script_pub_key: script_pub_key.into(),
            created_at,
        })
    }

    /// Encodes the exact version-1 `unspent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        let script_len = encoded_len(self.script_pub_key.len(), "unspent output script")?;
        let mut bytes =
            Vec::with_capacity(UNSPENT_OUTPUT_FIXED_VALUE_LEN + self.script_pub_key.len());
        bytes.extend_from_slice(&self.address_script_hash.as_bytes());
        bytes.extend_from_slice(&self.value_zat.to_be_bytes());
        bytes.extend_from_slice(&self.created_at.block.height.value().to_be_bytes());
        bytes.extend_from_slice(&self.created_at.block.hash.as_bytes());
        bytes.extend_from_slice(&self.created_at.tx_index_in_block.to_be_bytes());
        bytes.extend_from_slice(&script_len.to_be_bytes());
        bytes.extend_from_slice(&self.script_pub_key);
        Ok(bytes)
    }

    /// Decodes one exact version-1 `transparent_unspent_output` row.
    pub fn decode_value(
        key: WalletOutpointKey,
        encoded: &[u8],
    ) -> Result<Self, WalletProjectionContractError> {
        if encoded.len() < UNSPENT_OUTPUT_FIXED_VALUE_LEN {
            return Err(WalletProjectionContractError::DurableValueTooShort {
                field: "transparent_unspent_output value",
                minimum: UNSPENT_OUTPUT_FIXED_VALUE_LEN,
                actual: encoded.len(),
            });
        }
        let script_len = usize::try_from(u32::from_be_bytes(array_at::<4>(
            encoded,
            80,
            "transparent_unspent_output script length",
        )?))
        .unwrap_or(usize::MAX);
        let expected_len = UNSPENT_OUTPUT_FIXED_VALUE_LEN
            .checked_add(script_len)
            .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "transparent_unspent_output value",
            })?;
        if encoded.len() != expected_len {
            return Err(WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "transparent_unspent_output value",
            });
        }
        let outpoint = key.outpoint();
        Self::new(
            outpoint,
            TransparentAddressScriptHash::from_bytes(array_at::<32>(
                encoded,
                0,
                "transparent_unspent_output address",
            )?),
            u64::from_be_bytes(array_at::<8>(
                encoded,
                32,
                "transparent_unspent_output value",
            )?),
            encoded[UNSPENT_OUTPUT_FIXED_VALUE_LEN..].to_vec(),
            WalletTransactionPosition::new(
                outpoint.transaction_id,
                u32::from_be_bytes(array_at::<4>(
                    encoded,
                    76,
                    "transparent_unspent_output transaction index",
                )?),
                BlockId::new(
                    BlockHeight::new(u32::from_be_bytes(array_at::<4>(
                        encoded,
                        40,
                        "transparent_unspent_output creation height",
                    )?)),
                    BlockHash::from_bytes(array_at::<32>(
                        encoded,
                        44,
                        "transparent_unspent_output creation block hash",
                    )?),
                ),
            ),
        )
    }

    pub(crate) fn commitment_element(&self, network: Network) -> UtxoSetCommitmentElement<'_> {
        UtxoSetCommitmentElement {
            network_id: network.id(),
            outpoint: self.outpoint,
            value_zat: self.value_zat,
            script_pub_key: &self.script_pub_key,
            block_height: self.created_at.block.height,
        }
    }
}

/// Full historical transparent output paired with its consuming input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletSpentOutput {
    /// Original output and creator location.
    pub output: WalletUnspentOutput,
    /// Transaction and block that spent the output.
    pub spent_at: WalletTransactionPosition,
    /// Transparent input index inside the spending transaction.
    pub input_index: u32,
}

impl WalletSpentOutput {
    /// Creates a historical spent output.
    #[must_use]
    pub const fn new(
        output: WalletUnspentOutput,
        spent_at: WalletTransactionPosition,
        input_index: u32,
    ) -> Self {
        Self {
            output,
            spent_at,
            input_index,
        }
    }

    /// Encodes the exact version-1 `spent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        let mut bytes = self.output.encode_value()?;
        bytes.reserve(SPENT_OUTPUT_TRAILER_LEN);
        bytes.extend_from_slice(&self.spent_at.transaction_id.as_bytes());
        bytes.extend_from_slice(&self.input_index.to_be_bytes());
        bytes.extend_from_slice(&self.spent_at.tx_index_in_block.to_be_bytes());
        bytes.extend_from_slice(&self.spent_at.block.height.value().to_be_bytes());
        bytes.extend_from_slice(&self.spent_at.block.hash.as_bytes());
        Ok(bytes)
    }

    /// Decodes one exact version-1 `transparent_spent_output` row.
    pub fn decode_value(
        key: WalletOutpointKey,
        encoded: &[u8],
    ) -> Result<Self, WalletProjectionContractError> {
        let minimum_len = UNSPENT_OUTPUT_FIXED_VALUE_LEN + SPENT_OUTPUT_TRAILER_LEN;
        if encoded.len() < minimum_len {
            return Err(WalletProjectionContractError::DurableValueTooShort {
                field: "transparent_spent_output value",
                minimum: minimum_len,
                actual: encoded.len(),
            });
        }
        let script_len = usize::try_from(u32::from_be_bytes(array_at::<4>(
            encoded,
            80,
            "transparent_spent_output script length",
        )?))
        .unwrap_or(usize::MAX);
        let unspent_len = UNSPENT_OUTPUT_FIXED_VALUE_LEN
            .checked_add(script_len)
            .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "transparent_spent_output value",
            })?;
        let expected_len = unspent_len.checked_add(SPENT_OUTPUT_TRAILER_LEN).ok_or(
            WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "transparent_spent_output value",
            },
        )?;
        if encoded.len() != expected_len {
            return Err(WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "transparent_spent_output value",
            });
        }
        let output = WalletUnspentOutput::decode_value(key, &encoded[..unspent_len])?;
        let spent_transaction_id = TransactionId::from_bytes(array_at::<32>(
            encoded,
            unspent_len,
            "transparent_spent_output transaction id",
        )?);
        Ok(Self::new(
            output,
            WalletTransactionPosition::new(
                spent_transaction_id,
                u32::from_be_bytes(array_at::<4>(
                    encoded,
                    unspent_len + 36,
                    "transparent_spent_output transaction index",
                )?),
                BlockId::new(
                    BlockHeight::new(u32::from_be_bytes(array_at::<4>(
                        encoded,
                        unspent_len + 40,
                        "transparent_spent_output height",
                    )?)),
                    BlockHash::from_bytes(array_at::<32>(
                        encoded,
                        unspent_len + 44,
                        "transparent_spent_output block hash",
                    )?),
                ),
            ),
            u32::from_be_bytes(array_at::<4>(
                encoded,
                unspent_len + 32,
                "transparent_spent_output input index",
            )?),
        ))
    }
}

/// Lexicographically ordered version-1 `unspent_output` and `spent_output` key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WalletOutpointKey([u8; OUTPOINT_KEY_LEN]);

impl WalletOutpointKey {
    /// Encodes an outpoint as `txid || output_index_be`.
    #[must_use]
    pub const fn new(outpoint: TransparentOutPoint) -> Self {
        let mut bytes = [0u8; OUTPOINT_KEY_LEN];
        let transaction_id = outpoint.transaction_id.as_bytes();
        let output_index = outpoint.output_index.to_be_bytes();
        let mut index = 0;
        while index < transaction_id.len() {
            bytes[index] = transaction_id[index];
            index += 1;
        }
        bytes[32] = output_index[0];
        bytes[33] = output_index[1];
        bytes[34] = output_index[2];
        bytes[35] = output_index[3];
        Self(bytes)
    }

    /// Returns the exact durable key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; OUTPOINT_KEY_LEN] {
        &self.0
    }

    /// Decodes an exact version-1 outpoint key.
    pub fn decode(encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        Ok(Self(fixed_array::<OUTPOINT_KEY_LEN>(
            encoded,
            "wallet outpoint key",
        )?))
    }

    /// Returns the transparent outpoint represented by this key.
    #[must_use]
    pub fn outpoint(self) -> TransparentOutPoint {
        let mut transaction_id = [0; 32];
        transaction_id.copy_from_slice(&self.0[..32]);
        let mut output_index = [0; 4];
        output_index.copy_from_slice(&self.0[32..]);
        TransparentOutPoint::new(
            TransactionId::from_bytes(transaction_id),
            u32::from_be_bytes(output_index),
        )
    }
}

/// Ordered version-1 secondary key for unspent outputs belonging to an address.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WalletAddressUnspentOutputKey([u8; ADDRESS_UNSPENT_OUTPUT_KEY_LEN]);

impl WalletAddressUnspentOutputKey {
    /// Returns the first key at or after `height` for an address.
    ///
    /// The all-zero outpoint suffix makes this an inclusive lower bound for
    /// the address's creation-height ordering; it is not required to name an
    /// existing output.
    #[must_use]
    pub fn first_at_or_after(
        address_script_hash: TransparentAddressScriptHash,
        height: BlockHeight,
    ) -> Self {
        let mut bytes = [0u8; ADDRESS_UNSPENT_OUTPUT_KEY_LEN];
        bytes[..32].copy_from_slice(&address_script_hash.as_bytes());
        bytes[32..36].copy_from_slice(&height.value().to_be_bytes());
        Self(bytes)
    }

    /// Encodes `address_hash || creation_height_be || txid || output_index_be`.
    #[must_use]
    pub fn new(output: &WalletUnspentOutput) -> Self {
        let mut bytes = [0u8; ADDRESS_UNSPENT_OUTPUT_KEY_LEN];
        bytes[..32].copy_from_slice(&output.address_script_hash.as_bytes());
        bytes[32..36].copy_from_slice(&output.created_at.block.height.value().to_be_bytes());
        bytes[36..68].copy_from_slice(&output.outpoint.transaction_id.as_bytes());
        bytes[68..].copy_from_slice(&output.outpoint.output_index.to_be_bytes());
        Self(bytes)
    }

    /// Returns the exact durable key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; ADDRESS_UNSPENT_OUTPUT_KEY_LEN] {
        &self.0
    }

    /// Decodes an exact version-1 address-unspent-output key.
    pub fn decode(encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        Ok(Self(fixed_array::<ADDRESS_UNSPENT_OUTPUT_KEY_LEN>(
            encoded,
            "transparent_unspent_output_by_address key",
        )?))
    }

    /// Returns the address script hash encoded by this key.
    #[must_use]
    pub fn address_script_hash(self) -> TransparentAddressScriptHash {
        let mut address_script_hash = [0; 32];
        address_script_hash.copy_from_slice(&self.0[..32]);
        TransparentAddressScriptHash::from_bytes(address_script_hash)
    }

    /// Returns the creation height encoded by this key.
    #[must_use]
    pub fn creation_height(self) -> BlockHeight {
        let mut height = [0; 4];
        height.copy_from_slice(&self.0[32..36]);
        BlockHeight::new(u32::from_be_bytes(height))
    }

    /// Returns the outpoint encoded by this key.
    #[must_use]
    pub fn outpoint(self) -> TransparentOutPoint {
        let mut transaction_id = [0; 32];
        transaction_id.copy_from_slice(&self.0[36..68]);
        let mut output_index = [0; 4];
        output_index.copy_from_slice(&self.0[68..]);
        TransparentOutPoint::new(
            TransactionId::from_bytes(transaction_id),
            u32::from_be_bytes(output_index),
        )
    }
}

/// Ordered version-1 key for one address-touching transaction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WalletAddressTransactionKey([u8; ADDRESS_TRANSACTION_KEY_LEN]);

impl WalletAddressTransactionKey {
    /// Returns the first key at or after `height` for an address.
    ///
    /// The zero transaction index makes this an inclusive lower bound for the
    /// address's block-height ordering; it is not required to name an existing
    /// transaction.
    #[must_use]
    pub fn first_at_or_after(
        address_script_hash: TransparentAddressScriptHash,
        height: BlockHeight,
    ) -> Self {
        Self::new(address_script_hash, height, 0)
    }

    /// Encodes `address_hash || height_be || transaction_index_be`.
    #[must_use]
    pub fn new(
        address_script_hash: TransparentAddressScriptHash,
        block_height: BlockHeight,
        tx_index_in_block: u32,
    ) -> Self {
        let mut bytes = [0u8; ADDRESS_TRANSACTION_KEY_LEN];
        bytes[..32].copy_from_slice(&address_script_hash.as_bytes());
        bytes[32..36].copy_from_slice(&block_height.value().to_be_bytes());
        bytes[36..].copy_from_slice(&tx_index_in_block.to_be_bytes());
        Self(bytes)
    }

    /// Returns the exact durable key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; ADDRESS_TRANSACTION_KEY_LEN] {
        &self.0
    }

    /// Decodes an exact version-1 address-transaction key.
    pub fn decode(encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        Ok(Self(fixed_array::<ADDRESS_TRANSACTION_KEY_LEN>(
            encoded,
            "transparent_address_transaction key",
        )?))
    }

    /// Returns the address script hash encoded by this key.
    #[must_use]
    pub fn address_script_hash(self) -> TransparentAddressScriptHash {
        let mut address_script_hash = [0; 32];
        address_script_hash.copy_from_slice(&self.0[..32]);
        TransparentAddressScriptHash::from_bytes(address_script_hash)
    }

    /// Returns the block height encoded by this key.
    #[must_use]
    pub fn block_height(self) -> BlockHeight {
        let mut height = [0; 4];
        height.copy_from_slice(&self.0[32..36]);
        BlockHeight::new(u32::from_be_bytes(height))
    }

    /// Returns the block-local transaction index encoded by this key.
    #[must_use]
    pub fn tx_index_in_block(self) -> u32 {
        let mut tx_index_in_block = [0; 4];
        tx_index_in_block.copy_from_slice(&self.0[36..]);
        u32::from_be_bytes(tx_index_in_block)
    }
}

/// One address-transaction row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletAddressTransaction {
    /// Ordered address-transaction key.
    pub key: WalletAddressTransactionKey,
    /// Transaction touching the address.
    pub transaction_id: TransactionId,
    /// Canonical block containing the transaction.
    pub block_hash: BlockHash,
}

impl WalletAddressTransaction {
    /// Creates one address-transaction row.
    #[must_use]
    pub const fn new(
        key: WalletAddressTransactionKey,
        transaction_id: TransactionId,
        block_hash: BlockHash,
    ) -> Self {
        Self {
            key,
            transaction_id,
            block_hash,
        }
    }

    /// Encodes `txid || block_hash`.
    #[must_use]
    pub fn encode_value(self) -> [u8; 64] {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(&self.transaction_id.as_bytes());
        bytes[32..].copy_from_slice(&self.block_hash.as_bytes());
        bytes
    }

    /// Decodes one exact version-1 address-transaction row.
    pub fn decode_value(
        key: WalletAddressTransactionKey,
        encoded: &[u8],
    ) -> Result<Self, WalletProjectionContractError> {
        let encoded_transaction =
            fixed_array::<64>(encoded, "transparent_address_transaction value")?;
        let mut transaction_id = [0; 32];
        transaction_id.copy_from_slice(&encoded_transaction[..32]);
        let mut block_hash = [0; 32];
        block_hash.copy_from_slice(&encoded_transaction[32..]);
        Ok(Self::new(
            key,
            TransactionId::from_bytes(transaction_id),
            BlockHash::from_bytes(block_hash),
        ))
    }
}

/// One bounded page of current outputs ordered by creation position and outpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletAddressUnspentOutputsPage {
    /// Current outputs in durable version-1 address-index order.
    pub outputs: Vec<WalletUnspentOutput>,
    /// Exclusive continuation key to pass as `after` for the next page.
    pub next_page_after: Option<WalletAddressUnspentOutputKey>,
}

/// One bounded page of address history ordered by block height and transaction index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletAddressTransactionHistoryPage {
    /// Address-touching transactions in durable version-1 history order.
    pub transactions: Vec<WalletAddressTransaction>,
    /// Exclusive continuation key to pass as `after` for the next page.
    pub next_page_after: Option<WalletAddressTransactionKey>,
}

/// Current transparent balance for one address script hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletAddressBalance {
    /// Address script hash.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Sum of currently unspent outputs in zatoshi.
    pub balance_zat: u64,
}

impl WalletAddressBalance {
    /// Encodes the exact `address_balance` key.
    #[must_use]
    pub const fn encode_key(self) -> [u8; 32] {
        self.address_script_hash.as_bytes()
    }

    /// Encodes the exact version-1 balance value.
    #[must_use]
    pub const fn encode_value(self) -> [u8; 8] {
        self.balance_zat.to_be_bytes()
    }

    /// Decodes one exact version-1 address-balance row.
    pub fn decode(key: &[u8], encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        Ok(Self {
            address_script_hash: TransparentAddressScriptHash::from_bytes(fixed_array::<32>(
                key,
                "transparent_address_balance key",
            )?),
            balance_zat: u64::from_be_bytes(fixed_array::<8>(
                encoded,
                "transparent_address_balance value",
            )?),
        })
    }
}

/// Reversible changes made while projecting one canonical block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletReorgUndo {
    /// Canonical block whose wallet changes this record reverses.
    pub block: BlockId,
    /// Parent hash the reverted block was validated against.
    pub parent_hash: BlockHash,
    /// Exact source digest immediately before this block was applied.
    pub source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
    /// Exact source digest immediately after this block was applied.
    pub source_sequence_digest_after: CanonicalBlockFactsSequenceDigest,
    /// Outputs created by this block.
    pub created_outpoints: Vec<WalletOutpointKey>,
    /// Prior outputs spent by this block.
    pub spent_outpoints: Vec<WalletOutpointKey>,
    /// Exact address-transaction rows inserted by this block.
    pub address_transaction_keys: Vec<WalletAddressTransactionKey>,
}

impl WalletReorgUndo {
    /// Encodes the exact version-1 height key.
    #[must_use]
    pub const fn encode_key(&self) -> [u8; 4] {
        self.block.height.value().to_be_bytes()
    }

    /// Encodes the exact version-2 undo value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        self.validate_source_sequence_bounds()?;
        validate_strict_key_order(&self.created_outpoints, "reorg_undo created outpoint list")?;
        validate_strict_key_order(&self.spent_outpoints, "reorg_undo spent outpoint list")?;
        validate_strict_key_order(
            &self.address_transaction_keys,
            "reorg_undo address transaction list",
        )?;
        let created_count = encoded_len(self.created_outpoints.len(), "created outpoint list")?;
        let spent_count = encoded_len(self.spent_outpoints.len(), "spent outpoint list")?;
        let address_transaction_count = encoded_len(
            self.address_transaction_keys.len(),
            "address transaction key list",
        )?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.block.hash.as_bytes());
        bytes.extend_from_slice(&self.parent_hash.as_bytes());
        encode_source_sequence_digest(self.source_sequence_digest_before, &mut bytes);
        encode_source_sequence_digest(self.source_sequence_digest_after, &mut bytes);
        bytes.extend_from_slice(&created_count.to_be_bytes());
        for key in &self.created_outpoints {
            bytes.extend_from_slice(key.as_bytes());
        }
        bytes.extend_from_slice(&spent_count.to_be_bytes());
        for key in &self.spent_outpoints {
            bytes.extend_from_slice(key.as_bytes());
        }
        bytes.extend_from_slice(&address_transaction_count.to_be_bytes());
        for key in &self.address_transaction_keys {
            bytes.extend_from_slice(key.as_bytes());
        }
        Ok(bytes)
    }

    /// Decodes one exact version-2 reorg-undo row.
    pub fn decode(key: &[u8], encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        let height = BlockHeight::new(u32::from_be_bytes(fixed_array::<4>(key, "reorg_undo key")?));
        if encoded.len() < REORG_UNDO_FIXED_VALUE_LEN {
            return Err(WalletProjectionContractError::DurableValueTooShort {
                field: "reorg_undo value",
                minimum: REORG_UNDO_FIXED_VALUE_LEN,
                actual: encoded.len(),
            });
        }
        let block_hash =
            BlockHash::from_bytes(array_at::<32>(encoded, 0, "reorg_undo block hash")?);
        let parent_hash =
            BlockHash::from_bytes(array_at::<32>(encoded, 32, "reorg_undo parent hash")?);
        let mut offset = 64;
        let source_sequence_digest_before =
            decode_source_sequence_digest(encoded, &mut offset, "reorg_undo source digest before")?;
        let source_sequence_digest_after =
            decode_source_sequence_digest(encoded, &mut offset, "reorg_undo source digest after")?;
        let created_outpoints =
            decode_outpoint_list(encoded, &mut offset, "reorg_undo created outpoint list")?;
        let spent_outpoints =
            decode_outpoint_list(encoded, &mut offset, "reorg_undo spent outpoint list")?;
        let address_transaction_keys = decode_address_transaction_list(
            encoded,
            &mut offset,
            "reorg_undo address transaction list",
        )?;
        if offset != encoded.len() {
            return Err(WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "reorg_undo value",
            });
        }
        let undo = Self {
            block: BlockId::new(height, block_hash),
            parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints,
            spent_outpoints,
            address_transaction_keys,
        };
        undo.validate_source_sequence_bounds()?;
        Ok(undo)
    }

    fn validate_source_sequence_bounds(&self) -> Result<(), WalletProjectionContractError> {
        if self.source_sequence_digest_before.version()
            != CanonicalBlockFactsSequenceDigestVersion::V1
            || self.source_sequence_digest_after.version()
                != CanonicalBlockFactsSequenceDigestVersion::V1
        {
            return Err(WalletProjectionContractError::ReorgUndoSourceSequenceVersionMismatch);
        }
        if self
            .source_sequence_digest_before
            .block_count()
            .checked_add(1)
            != Some(self.source_sequence_digest_after.block_count())
        {
            return Err(WalletProjectionContractError::ReorgUndoSourceSequenceLengthMismatch);
        }
        Ok(())
    }
}

fn encode_source_sequence_digest(digest: CanonicalBlockFactsSequenceDigest, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&digest.version().value().to_be_bytes());
    bytes.extend_from_slice(&digest.block_count().to_be_bytes());
    bytes.extend_from_slice(&digest.as_bytes());
}

fn decode_source_sequence_digest(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<CanonicalBlockFactsSequenceDigest, WalletProjectionContractError> {
    let version = u16::from_be_bytes(array_at::<2>(encoded, *offset, field)?);
    *offset = offset
        .checked_add(2)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    if version != CanonicalBlockFactsSequenceDigestVersion::V1.value() {
        return Err(WalletProjectionContractError::ReorgUndoSourceSequenceVersionMismatch);
    }
    let block_count = u64::from_be_bytes(array_at::<8>(encoded, *offset, field)?);
    *offset = offset
        .checked_add(8)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let digest = array_at::<32>(encoded, *offset, field)?;
    *offset = offset
        .checked_add(32)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    Ok(
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            block_count,
            digest,
        ),
    )
}

fn fixed_array<const LEN: usize>(
    encoded: &[u8],
    field: &'static str,
) -> Result<[u8; LEN], WalletProjectionContractError> {
    encoded.try_into().map_err(
        |_| WalletProjectionContractError::DurableFieldLengthMismatch {
            field,
            expected: LEN,
            actual: encoded.len(),
        },
    )
}

fn validate_strict_key_order<Key: Ord>(
    keys: &[Key],
    field: &'static str,
) -> Result<(), WalletProjectionContractError> {
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(WalletProjectionContractError::DurableKeyOrder { field });
    }
    Ok(())
}

fn array_at<const LEN: usize>(
    encoded: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<[u8; LEN], WalletProjectionContractError> {
    let end = offset
        .checked_add(LEN)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let Some(bytes) = encoded.get(offset..end) else {
        return Err(WalletProjectionContractError::DurableValueTooShort {
            field,
            minimum: end,
            actual: encoded.len(),
        });
    };
    fixed_array(bytes, field)
}

fn decode_outpoint_list(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<Vec<WalletOutpointKey>, WalletProjectionContractError> {
    let count = decode_count(encoded, offset, field)?;
    let byte_len = count
        .checked_mul(OUTPOINT_KEY_LEN)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let end = offset
        .checked_add(byte_len)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let Some(list_bytes) = encoded.get(*offset..end) else {
        return Err(WalletProjectionContractError::DurableLengthPrefixMismatch { field });
    };
    let mut keys = Vec::with_capacity(count);
    for encoded_outpoint in list_bytes.as_chunks::<OUTPOINT_KEY_LEN>().0 {
        let key = WalletOutpointKey::decode(encoded_outpoint)?;
        keys.push(key);
    }
    validate_strict_key_order(&keys, field)?;
    *offset = end;
    Ok(keys)
}

fn decode_address_transaction_list(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<Vec<WalletAddressTransactionKey>, WalletProjectionContractError> {
    let count = decode_count(encoded, offset, field)?;
    let byte_len = count
        .checked_mul(ADDRESS_TRANSACTION_KEY_LEN)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let end = offset
        .checked_add(byte_len)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    let Some(list_bytes) = encoded.get(*offset..end) else {
        return Err(WalletProjectionContractError::DurableLengthPrefixMismatch { field });
    };
    let mut keys = Vec::with_capacity(count);
    for encoded_address_transaction in list_bytes.as_chunks::<ADDRESS_TRANSACTION_KEY_LEN>().0 {
        let key = WalletAddressTransactionKey::decode(encoded_address_transaction)?;
        keys.push(key);
    }
    validate_strict_key_order(&keys, field)?;
    *offset = end;
    Ok(keys)
}

fn decode_count(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<usize, WalletProjectionContractError> {
    let count = u32::from_be_bytes(array_at::<4>(encoded, *offset, field)?);
    *offset = offset
        .checked_add(4)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outpoint() -> TransparentOutPoint {
        TransparentOutPoint::new(TransactionId::from_bytes([0x11; 32]), 0x0102_0304)
    }

    fn sample_unspent_output() -> WalletUnspentOutput {
        WalletUnspentOutput::new(
            sample_outpoint(),
            TransparentAddressScriptHash::from_bytes([0x22; 32]),
            0x0102_0304_0506_0708,
            [0x51, 0xac],
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x11; 32]),
                0x1112_1314,
                BlockId::new(
                    BlockHeight::new(0x0a0b_0c0d),
                    BlockHash::from_bytes([0x33; 32]),
                ),
            ),
        )
        .unwrap_or_else(|error| unreachable!("valid sample output: {error}"))
    }

    fn undo_for(
        block: BlockId,
        created_outpoints: Vec<WalletOutpointKey>,
        spent_outpoints: Vec<WalletOutpointKey>,
        address_transaction_keys: Vec<WalletAddressTransactionKey>,
    ) -> WalletReorgUndo {
        let before_count = u64::from(block.height.value().saturating_sub(1));
        WalletReorgUndo {
            block,
            parent_hash: BlockHash::from_bytes([0x44; 32]),
            source_sequence_digest_before:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    before_count,
                    [0x55; 32],
                ),
            source_sequence_digest_after:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    before_count.saturating_add(1),
                    [0x66; 32],
                ),
            created_outpoints,
            spent_outpoints,
            address_transaction_keys,
        }
    }

    #[test]
    fn outpoint_and_address_keys_have_exact_version_one_bytes() {
        let output = sample_unspent_output();
        assert_eq!(
            hex::encode(WalletOutpointKey::new(output.outpoint).as_bytes()),
            concat!(
                "1111111111111111111111111111111111111111111111111111111111111111",
                "01020304"
            )
        );
        assert_eq!(
            hex::encode(WalletAddressUnspentOutputKey::new(&output).as_bytes()),
            concat!(
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0a0b0c0d",
                "1111111111111111111111111111111111111111111111111111111111111111",
                "01020304"
            )
        );
    }

    #[test]
    fn unspent_and_spent_values_have_exact_version_one_bytes() {
        let output = sample_unspent_output();
        assert_eq!(
            hex::encode(
                output
                    .encode_value()
                    .unwrap_or_else(|error| unreachable!("valid sample output: {error}"))
            ),
            concat!(
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0102030405060708",
                "0a0b0c0d",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "11121314",
                "00000002",
                "51ac"
            )
        );

        let spent = WalletSpentOutput::new(
            output,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                0x3132_3334,
                BlockId::new(
                    BlockHeight::new(0x4142_4344),
                    BlockHash::from_bytes([0x55; 32]),
                ),
            ),
            0x2122_2324,
        );
        assert_eq!(
            hex::encode(
                spent
                    .encode_value()
                    .unwrap_or_else(|error| unreachable!("valid sample spend: {error}"))
            ),
            concat!(
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0102030405060708",
                "0a0b0c0d",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "11121314",
                "00000002",
                "51ac",
                "4444444444444444444444444444444444444444444444444444444444444444",
                "21222324",
                "31323334",
                "41424344",
                "5555555555555555555555555555555555555555555555555555555555555555"
            )
        );
    }

    #[test]
    fn address_transaction_balance_and_undo_have_exact_version_two_bytes() {
        let address = TransparentAddressScriptHash::from_bytes([0x22; 32]);
        let address_transaction_key =
            WalletAddressTransactionKey::new(address, BlockHeight::new(0x0a0b_0c0d), 0x1112_1314);
        let address_transaction = WalletAddressTransaction::new(
            address_transaction_key,
            TransactionId::from_bytes([0x11; 32]),
            BlockHash::from_bytes([0x33; 32]),
        );
        assert_eq!(
            hex::encode(address_transaction_key.as_bytes()),
            concat!(
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0a0b0c0d",
                "11121314"
            )
        );
        assert_eq!(
            hex::encode(address_transaction.encode_value()),
            concat!(
                "1111111111111111111111111111111111111111111111111111111111111111",
                "3333333333333333333333333333333333333333333333333333333333333333"
            )
        );
        let balance = WalletAddressBalance {
            address_script_hash: address,
            balance_zat: 0x0102_0304_0506_0708,
        };
        assert_eq!(hex::encode(balance.encode_key()), "22".repeat(32));
        assert_eq!(hex::encode(balance.encode_value()), "0102030405060708");

        let undo = undo_for(
            BlockId::new(
                BlockHeight::new(0x0a0b_0c0d),
                BlockHash::from_bytes([0x33; 32]),
            ),
            vec![WalletOutpointKey::new(sample_outpoint())],
            vec![WalletOutpointKey::new(sample_outpoint())],
            vec![address_transaction_key],
        );
        assert_eq!(hex::encode(undo.encode_key()), "0a0b0c0d");
        let encoded = undo
            .encode_value()
            .unwrap_or_else(|error| unreachable!("valid sample undo: {error}"));
        assert_eq!(&encoded[..32], &undo.block.hash.as_bytes());
        assert_eq!(&encoded[32..64], &undo.parent_hash.as_bytes());
        assert_eq!(
            WalletReorgUndo::decode(&undo.encode_key(), &encoded),
            Ok(undo)
        );
    }

    #[test]
    fn reorg_undo_accepts_checkpoint_relative_source_counts()
    -> Result<(), WalletProjectionContractError> {
        let undo = WalletReorgUndo {
            block: BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([0x77; 32])),
            parent_hash: BlockHash::from_bytes([0x76; 32]),
            source_sequence_digest_before:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    0,
                    [0x75; 32],
                ),
            source_sequence_digest_after:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    1,
                    [0x74; 32],
                ),
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_transaction_keys: Vec::new(),
        };

        assert_eq!(
            WalletReorgUndo::decode(&undo.encode_key(), &undo.encode_value()?)?,
            undo
        );
        Ok(())
    }

    #[test]
    fn every_wallet_row_round_trips_exact_version_one_bytes()
    -> Result<(), WalletProjectionContractError> {
        let unspent_output = sample_unspent_output();
        let outpoint_key = WalletOutpointKey::new(unspent_output.outpoint);
        assert_eq!(
            WalletOutpointKey::decode(outpoint_key.as_bytes())?,
            outpoint_key
        );
        assert_eq!(outpoint_key.outpoint(), unspent_output.outpoint);
        assert_eq!(
            WalletUnspentOutput::decode_value(outpoint_key, &unspent_output.encode_value()?)?,
            unspent_output
        );

        let spent_output = WalletSpentOutput::new(
            unspent_output.clone(),
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                3,
                BlockId::new(BlockHeight::new(20), BlockHash::from_bytes([0x55; 32])),
            ),
            2,
        );
        assert_eq!(
            WalletSpentOutput::decode_value(outpoint_key, &spent_output.encode_value()?)?,
            spent_output
        );

        let address_output_key = WalletAddressUnspentOutputKey::new(&unspent_output);
        assert_eq!(
            WalletAddressUnspentOutputKey::decode(address_output_key.as_bytes())?,
            address_output_key
        );
        assert_eq!(
            address_output_key.address_script_hash(),
            unspent_output.address_script_hash
        );
        assert_eq!(
            address_output_key.creation_height(),
            unspent_output.created_at.block.height
        );
        assert_eq!(address_output_key.outpoint(), unspent_output.outpoint);

        let address_transaction_key = WalletAddressTransactionKey::new(
            unspent_output.address_script_hash,
            unspent_output.created_at.block.height,
            unspent_output.created_at.tx_index_in_block,
        );
        let address_transaction = WalletAddressTransaction::new(
            address_transaction_key,
            unspent_output.created_at.transaction_id,
            unspent_output.created_at.block.hash,
        );
        assert_eq!(
            WalletAddressTransactionKey::decode(address_transaction_key.as_bytes())?,
            address_transaction_key
        );
        assert_eq!(
            WalletAddressTransaction::decode_value(
                address_transaction_key,
                &address_transaction.encode_value()
            )?,
            address_transaction
        );

        let balance = WalletAddressBalance {
            address_script_hash: unspent_output.address_script_hash,
            balance_zat: unspent_output.value_zat,
        };
        assert_eq!(
            WalletAddressBalance::decode(&balance.encode_key(), &balance.encode_value())?,
            balance
        );

        let undo = undo_for(
            unspent_output.created_at.block,
            vec![outpoint_key],
            Vec::new(),
            vec![address_transaction_key],
        );
        assert_eq!(
            WalletReorgUndo::decode(&undo.encode_key(), &undo.encode_value()?)?,
            undo
        );
        Ok(())
    }

    #[test]
    fn wallet_row_decoders_reject_truncation_trailing_bytes_and_noncanonical_undo_keys()
    -> Result<(), WalletProjectionContractError> {
        let output = sample_unspent_output();
        let outpoint_key = WalletOutpointKey::new(output.outpoint);
        let mut encoded_output = output.encode_value()?;
        encoded_output.push(0);
        assert!(matches!(
            WalletUnspentOutput::decode_value(outpoint_key, &encoded_output),
            Err(WalletProjectionContractError::DurableLengthPrefixMismatch { .. })
        ));
        assert!(matches!(
            WalletOutpointKey::decode(&outpoint_key.as_bytes()[..35]),
            Err(WalletProjectionContractError::DurableFieldLengthMismatch { .. })
        ));

        let address_transaction_key = WalletAddressTransactionKey::new(
            output.address_script_hash,
            output.created_at.block.height,
            output.created_at.tx_index_in_block,
        );
        let undo = undo_for(
            output.created_at.block,
            vec![outpoint_key, outpoint_key],
            Vec::new(),
            vec![address_transaction_key],
        );
        assert!(matches!(
            undo.encode_value(),
            Err(WalletProjectionContractError::DurableKeyOrder { .. })
        ));
        Ok(())
    }

    #[test]
    fn reorg_undo_requires_strict_key_order() {
        let output = sample_unspent_output();
        let lower_key = WalletOutpointKey::new(output.outpoint);
        let higher_key = WalletOutpointKey::new(TransparentOutPoint::new(
            TransactionId::from_bytes([0x22; 32]),
            0,
        ));
        let undo = undo_for(
            output.created_at.block,
            vec![higher_key, lower_key],
            Vec::new(),
            Vec::new(),
        );

        assert!(matches!(
            undo.encode_value(),
            Err(WalletProjectionContractError::DurableKeyOrder { .. })
        ));
    }
}
