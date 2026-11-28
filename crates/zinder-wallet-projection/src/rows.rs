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
const BLOCK_HASH_KEY_LEN: usize = 4;
const BLOCK_HASH_VALUE_LEN: usize = 32;
const ADDRESS_TRANSACTION_VALUE_LEN: usize = 32;
const SCRIPT_KIND_MASK: u8 = 0x03;
const SCRIPT_KIND_RAW: u8 = 0;
const SCRIPT_KIND_P2PKH: u8 = 1;
const SCRIPT_KIND_P2SH: u8 = 2;
const EXPLICIT_ADDRESS_HASH_FLAG: u8 = 0x04;
const VALID_OUTPUT_FLAGS_MASK: u8 = 0x07;
const P2PKH_SCRIPT_LEN: usize = 25;
const P2SH_SCRIPT_LEN: usize = 23;
const SCRIPT_HASH160_LEN: usize = 20;
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

/// Height-only position persisted inside compact version-2 output rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletStoredTransactionPosition {
    /// Transaction identifier.
    pub transaction_id: TransactionId,
    /// Block-local transaction index.
    pub tx_index_in_block: u32,
    /// Height of the canonical block containing the transaction.
    pub block_height: BlockHeight,
}

impl WalletStoredTransactionPosition {
    /// Creates a compact stored transaction position.
    #[must_use]
    pub const fn new(
        transaction_id: TransactionId,
        tx_index_in_block: u32,
        block_height: BlockHeight,
    ) -> Self {
        Self {
            transaction_id,
            tx_index_in_block,
            block_height,
        }
    }

    /// Strips the redundant block hash from a full transaction position.
    #[must_use]
    pub const fn from_position(position: WalletTransactionPosition) -> Self {
        Self {
            transaction_id: position.transaction_id,
            tx_index_in_block: position.tx_index_in_block,
            block_height: position.block.height,
        }
    }

    /// Reconstructs the full canonical transaction position using `block_hash`.
    #[must_use]
    pub const fn with_block_hash(self, block_hash: BlockHash) -> WalletTransactionPosition {
        WalletTransactionPosition::new(
            self.transaction_id,
            self.tx_index_in_block,
            BlockId::new(self.block_height, block_hash),
        )
    }
}

/// Canonical block hash indexed by block height in `block_hash_by_height`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletBlockHash {
    /// Canonical block identifier.
    pub block: BlockId,
}

impl WalletBlockHash {
    /// Creates one height-to-block-hash row.
    #[must_use]
    pub const fn new(block: BlockId) -> Self {
        Self { block }
    }

    /// Encodes the big-endian `u32` height key.
    #[must_use]
    pub const fn encode_key(&self) -> [u8; BLOCK_HASH_KEY_LEN] {
        self.block.height.value().to_be_bytes()
    }

    /// Encodes the exact 32-byte block hash value.
    #[must_use]
    pub const fn encode_value(&self) -> [u8; BLOCK_HASH_VALUE_LEN] {
        self.block.hash.as_bytes()
    }

    /// Decodes one `block_hash_by_height` row.
    pub fn decode(key: &[u8], encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        let height = BlockHeight::new(u32::from_be_bytes(fixed_array::<BLOCK_HASH_KEY_LEN>(
            key,
            "block_hash_by_height key",
        )?));
        let block_hash = BlockHash::from_bytes(fixed_array::<BLOCK_HASH_VALUE_LEN>(
            encoded,
            "block_hash_by_height value",
        )?);
        Ok(Self::new(BlockId::new(height, block_hash)))
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

    /// Encodes the exact version-2 `transparent_unspent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        encode_unspent_parts(&UnspentOutputParts {
            address_script_hash: self.address_script_hash,
            value_zat: self.value_zat,
            block_height: self.created_at.block.height,
            tx_index_in_block: self.created_at.tx_index_in_block,
            script_pub_key: &self.script_pub_key,
        })
    }

    /// Decodes one version-2 `transparent_unspent_output` row and hydrates its
    /// creation block hash.
    pub fn decode_value_with_block_hash(
        key: WalletOutpointKey,
        encoded: &[u8],
        block_hash: BlockHash,
    ) -> Result<Self, WalletProjectionContractError> {
        Ok(StoredWalletUnspentOutput::decode_value(key, encoded)?.into_unspent(block_hash))
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

/// Compact version-2 stored form of an unspent transparent output without its
/// creation block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWalletUnspentOutput {
    /// Output identity.
    pub outpoint: TransparentOutPoint,
    /// Hash of the raw output script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Output value in zatoshi.
    pub value_zat: u64,
    /// Raw `scriptPubKey` bytes.
    pub script_pub_key: Vec<u8>,
    /// Transaction and creation height for the output.
    pub created_at: WalletStoredTransactionPosition,
}

impl StoredWalletUnspentOutput {
    /// Creates a compact stored unspent output.
    pub fn new(
        outpoint: TransparentOutPoint,
        address_script_hash: TransparentAddressScriptHash,
        value_zat: u64,
        script_pub_key: impl Into<Vec<u8>>,
        created_at: WalletStoredTransactionPosition,
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

    /// Converts a full unspent output into its compact stored representation.
    #[must_use]
    pub fn from_unspent(output: &WalletUnspentOutput) -> Self {
        Self {
            outpoint: output.outpoint,
            address_script_hash: output.address_script_hash,
            value_zat: output.value_zat,
            script_pub_key: output.script_pub_key.clone(),
            created_at: WalletStoredTransactionPosition::from_position(output.created_at),
        }
    }

    /// Hydrates a compact stored output with its canonical creation block hash.
    #[must_use]
    pub fn into_unspent(self, created_block_hash: BlockHash) -> WalletUnspentOutput {
        WalletUnspentOutput {
            outpoint: self.outpoint,
            address_script_hash: self.address_script_hash,
            value_zat: self.value_zat,
            script_pub_key: self.script_pub_key,
            created_at: self.created_at.with_block_hash(created_block_hash),
        }
    }

    /// Returns the secondary address-index key corresponding to this output.
    #[must_use]
    pub fn address_unspent_output_key(&self) -> WalletAddressUnspentOutputKey {
        WalletAddressUnspentOutputKey::from_parts(
            self.address_script_hash,
            self.created_at.block_height,
            self.outpoint,
        )
    }

    /// Encodes the exact version-2 `transparent_unspent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        encode_unspent_parts(&UnspentOutputParts {
            address_script_hash: self.address_script_hash,
            value_zat: self.value_zat,
            block_height: self.created_at.block_height,
            tx_index_in_block: self.created_at.tx_index_in_block,
            script_pub_key: &self.script_pub_key,
        })
    }

    /// Decodes one exact version-2 `transparent_unspent_output` row.
    pub fn decode_value(
        key: WalletOutpointKey,
        encoded: &[u8],
    ) -> Result<Self, WalletProjectionContractError> {
        let mut offset = 0;
        let decoded = decode_stored_unspent_prefix(
            key,
            encoded,
            &mut offset,
            "transparent_unspent_output value",
        )?;
        if offset != encoded.len() {
            return Err(WalletProjectionContractError::DurableTrailingBytes {
                field: "transparent_unspent_output value",
            });
        }
        Ok(decoded)
    }

    /// Returns the UTXO commitment element for this stored output.
    #[must_use]
    pub fn commitment_element(&self, network: Network) -> UtxoSetCommitmentElement<'_> {
        UtxoSetCommitmentElement {
            network_id: network.id(),
            outpoint: self.outpoint,
            value_zat: self.value_zat,
            script_pub_key: &self.script_pub_key,
            block_height: self.created_at.block_height,
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

    /// Encodes the exact version-2 `transparent_spent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        encode_spent_parts(
            &UnspentOutputParts {
                address_script_hash: self.output.address_script_hash,
                value_zat: self.output.value_zat,
                block_height: self.output.created_at.block.height,
                tx_index_in_block: self.output.created_at.tx_index_in_block,
                script_pub_key: &self.output.script_pub_key,
            },
            WalletStoredTransactionPosition::from_position(self.spent_at),
            self.input_index,
        )
    }

    /// Decodes one version-2 `transparent_spent_output` row and hydrates its
    /// creation and spend block hashes.
    pub fn decode_value_with_block_hash(
        key: WalletOutpointKey,
        encoded: &[u8],
        created_block_hash: BlockHash,
        spent_block_hash: BlockHash,
    ) -> Result<Self, WalletProjectionContractError> {
        Ok(StoredWalletSpentOutput::decode_value(key, encoded)?
            .into_spent(created_block_hash, spent_block_hash))
    }
}

/// Compact version-2 historical spent output without block hashes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWalletSpentOutput {
    /// Original output and creation height.
    pub output: StoredWalletUnspentOutput,
    /// Transaction and height that spent the output.
    pub spent_at: WalletStoredTransactionPosition,
    /// Transparent input index inside the spending transaction.
    pub input_index: u32,
}

impl StoredWalletSpentOutput {
    /// Creates a compact stored spent output.
    #[must_use]
    pub const fn new(
        output: StoredWalletUnspentOutput,
        spent_at: WalletStoredTransactionPosition,
        input_index: u32,
    ) -> Self {
        Self {
            output,
            spent_at,
            input_index,
        }
    }

    /// Converts a full spent output into its compact stored representation.
    #[must_use]
    pub fn from_spent(spent: &WalletSpentOutput) -> Self {
        Self {
            output: StoredWalletUnspentOutput::from_unspent(&spent.output),
            spent_at: WalletStoredTransactionPosition::from_position(spent.spent_at),
            input_index: spent.input_index,
        }
    }

    /// Hydrates a compact spent output with its canonical creation and spend block hashes.
    #[must_use]
    pub fn into_spent(
        self,
        created_block_hash: BlockHash,
        spent_block_hash: BlockHash,
    ) -> WalletSpentOutput {
        WalletSpentOutput {
            output: self.output.into_unspent(created_block_hash),
            spent_at: self.spent_at.with_block_hash(spent_block_hash),
            input_index: self.input_index,
        }
    }

    /// Encodes the exact version-2 `transparent_spent_output` value.
    pub fn encode_value(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        encode_spent_parts(
            &UnspentOutputParts {
                address_script_hash: self.output.address_script_hash,
                value_zat: self.output.value_zat,
                block_height: self.output.created_at.block_height,
                tx_index_in_block: self.output.created_at.tx_index_in_block,
                script_pub_key: &self.output.script_pub_key,
            },
            self.spent_at,
            self.input_index,
        )
    }

    /// Decodes one exact version-2 `transparent_spent_output` row.
    pub fn decode_value(
        key: WalletOutpointKey,
        encoded: &[u8],
    ) -> Result<Self, WalletProjectionContractError> {
        let field = "transparent_spent_output value";
        let mut offset = 0;
        let output = decode_stored_unspent_prefix(key, encoded, &mut offset, field)?;
        let spent_txid_bytes = array_at::<32>(encoded, offset, "transparent_spent_output txid")?;
        offset = offset
            .checked_add(32)
            .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
        let spent_height = BlockHeight::new(decode_varint_u32(
            encoded,
            &mut offset,
            "transparent_spent_output height",
        )?);
        let spent_tx_index = decode_varint_u32(
            encoded,
            &mut offset,
            "transparent_spent_output transaction index",
        )?;
        let input_index =
            decode_varint_u32(encoded, &mut offset, "transparent_spent_output input index")?;
        if offset != encoded.len() {
            return Err(WalletProjectionContractError::DurableTrailingBytes { field });
        }
        Ok(Self::new(
            output,
            WalletStoredTransactionPosition::new(
                TransactionId::from_bytes(spent_txid_bytes),
                spent_tx_index,
                spent_height,
            ),
            input_index,
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
        Self::from_parts(
            output.address_script_hash,
            output.created_at.block.height,
            output.outpoint,
        )
    }

    /// Encodes `address_hash || creation_height_be || txid || output_index_be`
    /// directly from stored key components.
    #[must_use]
    pub fn from_parts(
        address_script_hash: TransparentAddressScriptHash,
        creation_height: BlockHeight,
        outpoint: TransparentOutPoint,
    ) -> Self {
        let mut bytes = [0u8; ADDRESS_UNSPENT_OUTPUT_KEY_LEN];
        bytes[..32].copy_from_slice(&address_script_hash.as_bytes());
        bytes[32..36].copy_from_slice(&creation_height.value().to_be_bytes());
        bytes[36..68].copy_from_slice(&outpoint.transaction_id.as_bytes());
        bytes[68..].copy_from_slice(&outpoint.output_index.to_be_bytes());
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

    /// Encodes the version-2 `transparent_address_transaction` value (`txid`).
    #[must_use]
    pub fn encode_value(self) -> [u8; ADDRESS_TRANSACTION_VALUE_LEN] {
        self.transaction_id.as_bytes()
    }

    /// Decodes one stored version-2 `transparent_address_transaction` value (`txid`).
    pub fn decode_stored_value(
        _key: WalletAddressTransactionKey,
        encoded: &[u8],
    ) -> Result<TransactionId, WalletProjectionContractError> {
        let transaction_id = fixed_array::<ADDRESS_TRANSACTION_VALUE_LEN>(
            encoded,
            "transparent_address_transaction value",
        )?;
        Ok(TransactionId::from_bytes(transaction_id))
    }

    /// Decodes one stored address-transaction row and hydrates its block hash.
    pub fn decode_value_with_block_hash(
        key: WalletAddressTransactionKey,
        encoded: &[u8],
        block_hash: BlockHash,
    ) -> Result<Self, WalletProjectionContractError> {
        let transaction_id = Self::decode_stored_value(key, encoded)?;
        Ok(Self::new(key, transaction_id, block_hash))
    }
}

/// One bounded page of current outputs ordered by creation position and outpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletAddressUnspentOutputsPage {
    /// Current outputs in durable version-2 address-index order.
    pub outputs: Vec<WalletUnspentOutput>,
    /// Exclusive continuation key to pass as `after` for the next page.
    pub next_page_after: Option<WalletAddressUnspentOutputKey>,
}

/// One bounded page of address history ordered by block height and transaction index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletAddressTransactionHistoryPage {
    /// Address-touching transactions in durable version-2 history order.
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

struct UnspentOutputParts<'a> {
    address_script_hash: TransparentAddressScriptHash,
    value_zat: u64,
    block_height: BlockHeight,
    tx_index_in_block: u32,
    script_pub_key: &'a [u8],
}

fn encode_unspent_parts(
    parts: &UnspentOutputParts<'_>,
) -> Result<Vec<u8>, WalletProjectionContractError> {
    let mut bytes = Vec::with_capacity(64);
    append_unspent_parts(parts, &mut bytes)?;
    Ok(bytes)
}

fn encode_spent_parts(
    unspent: &UnspentOutputParts<'_>,
    spent_at: WalletStoredTransactionPosition,
    input_index: u32,
) -> Result<Vec<u8>, WalletProjectionContractError> {
    let mut bytes = Vec::with_capacity(112);
    append_unspent_parts(unspent, &mut bytes)?;
    bytes.extend_from_slice(&spent_at.transaction_id.as_bytes());
    encode_varint_u32(spent_at.block_height.value(), &mut bytes);
    encode_varint_u32(spent_at.tx_index_in_block, &mut bytes);
    encode_varint_u32(input_index, &mut bytes);
    Ok(bytes)
}

fn append_unspent_parts(
    parts: &UnspentOutputParts<'_>,
    bytes: &mut Vec<u8>,
) -> Result<(), WalletProjectionContractError> {
    let derived_hash = TransparentAddressScriptHash::of_script_pub_key(parts.script_pub_key);
    let explicit_hash = parts.address_script_hash != derived_hash;
    let p2pkh_hash160 = standard_p2pkh_hash160(parts.script_pub_key);
    let p2sh_hash160 = standard_p2sh_hash160(parts.script_pub_key);
    let kind_bits = if p2pkh_hash160.is_some() {
        SCRIPT_KIND_P2PKH
    } else if p2sh_hash160.is_some() {
        SCRIPT_KIND_P2SH
    } else {
        SCRIPT_KIND_RAW
    };
    let flags = kind_bits
        | if explicit_hash {
            EXPLICIT_ADDRESS_HASH_FLAG
        } else {
            0
        };
    bytes.push(flags);
    if explicit_hash {
        bytes.extend_from_slice(&parts.address_script_hash.as_bytes());
    }
    encode_varint_u64(parts.value_zat, bytes);
    encode_varint_u32(parts.block_height.value(), bytes);
    encode_varint_u32(parts.tx_index_in_block, bytes);
    if let Some(hash160) = p2pkh_hash160.or(p2sh_hash160) {
        bytes.extend_from_slice(hash160);
    } else {
        let script_len = encoded_len(parts.script_pub_key.len(), "unspent output script")?;
        encode_varint_u32(script_len, bytes);
        bytes.extend_from_slice(parts.script_pub_key);
    }
    Ok(())
}

fn decode_stored_unspent_prefix(
    key: WalletOutpointKey,
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<StoredWalletUnspentOutput, WalletProjectionContractError> {
    let flags = array_at::<1>(encoded, *offset, field)?[0];
    *offset = offset
        .checked_add(1)
        .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
    if (flags & !VALID_OUTPUT_FLAGS_MASK) != 0 {
        return Err(WalletProjectionContractError::UnsupportedEncodedValue {
            field: "wallet output flags",
            encoded: u64::from(flags),
        });
    }
    let script_kind = flags & SCRIPT_KIND_MASK;
    if script_kind > SCRIPT_KIND_P2SH {
        return Err(WalletProjectionContractError::UnsupportedEncodedValue {
            field: "wallet output script kind",
            encoded: u64::from(script_kind),
        });
    }
    let explicit_hash = if (flags & EXPLICIT_ADDRESS_HASH_FLAG) != 0 {
        let hash_bytes = array_at::<32>(encoded, *offset, "transparent output address hash")?;
        *offset = offset
            .checked_add(32)
            .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
        Some(TransparentAddressScriptHash::from_bytes(hash_bytes))
    } else {
        None
    };
    let value_zat = decode_varint_u64(encoded, offset, "transparent output amount")?;
    let creation_height = BlockHeight::new(decode_varint_u32(
        encoded,
        offset,
        "transparent output creation height",
    )?);
    let tx_index_in_block =
        decode_varint_u32(encoded, offset, "transparent output transaction index")?;
    let script_pub_key = decode_script_pub_key(script_kind, encoded, offset, field)?;
    let derived_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    let address_script_hash = match explicit_hash {
        Some(explicit) if explicit == derived_hash => {
            return Err(WalletProjectionContractError::DurableNonCanonicalEncoding {
                field: "wallet output address script hash",
            });
        }
        Some(explicit) => explicit,
        None => derived_hash,
    };
    let outpoint = key.outpoint();
    StoredWalletUnspentOutput::new(
        outpoint,
        address_script_hash,
        value_zat,
        script_pub_key,
        WalletStoredTransactionPosition::new(
            outpoint.transaction_id,
            tx_index_in_block,
            creation_height,
        ),
    )
}

fn decode_script_pub_key(
    script_kind: u8,
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<Vec<u8>, WalletProjectionContractError> {
    match script_kind {
        SCRIPT_KIND_P2PKH => {
            let hash160 =
                array_at::<SCRIPT_HASH160_LEN>(encoded, *offset, "transparent output p2pkh hash")?;
            *offset = offset
                .checked_add(SCRIPT_HASH160_LEN)
                .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
            Ok(expand_p2pkh(&hash160))
        }
        SCRIPT_KIND_P2SH => {
            let hash160 =
                array_at::<SCRIPT_HASH160_LEN>(encoded, *offset, "transparent output p2sh hash")?;
            *offset = offset
                .checked_add(SCRIPT_HASH160_LEN)
                .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
            Ok(expand_p2sh(&hash160))
        }
        SCRIPT_KIND_RAW => {
            let script_len = usize::try_from(decode_varint_u32(
                encoded,
                offset,
                "transparent output script length",
            )?)
            .unwrap_or(usize::MAX);
            let end = offset
                .checked_add(script_len)
                .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
            let Some(raw_bytes) = encoded.get(*offset..end) else {
                return Err(WalletProjectionContractError::DurableValueTooShort {
                    field: "transparent output raw script",
                    minimum: end,
                    actual: encoded.len(),
                });
            };
            *offset = end;
            if standard_p2pkh_hash160(raw_bytes).is_some()
                || standard_p2sh_hash160(raw_bytes).is_some()
            {
                return Err(WalletProjectionContractError::DurableNonCanonicalEncoding {
                    field: "wallet output raw script",
                });
            }
            Ok(raw_bytes.to_vec())
        }
        other => Err(WalletProjectionContractError::UnsupportedEncodedValue {
            field: "wallet output script kind",
            encoded: u64::from(other),
        }),
    }
}

fn standard_p2pkh_hash160(script: &[u8]) -> Option<&[u8; SCRIPT_HASH160_LEN]> {
    if script.len() == P2PKH_SCRIPT_LEN
        && script[0] == 0x76
        && script[1] == 0xa9
        && script[2] == 0x14
        && script[23] == 0x88
        && script[24] == 0xac
    {
        script[3..23].try_into().ok()
    } else {
        None
    }
}

fn standard_p2sh_hash160(script: &[u8]) -> Option<&[u8; SCRIPT_HASH160_LEN]> {
    if script.len() == P2SH_SCRIPT_LEN
        && script[0] == 0xa9
        && script[1] == 0x14
        && script[22] == 0x87
    {
        script[2..22].try_into().ok()
    } else {
        None
    }
}

fn expand_p2pkh(hash160: &[u8; SCRIPT_HASH160_LEN]) -> Vec<u8> {
    let mut script = Vec::with_capacity(P2PKH_SCRIPT_LEN);
    script.extend_from_slice(&[0x76, 0xa9, 0x14]);
    script.extend_from_slice(hash160);
    script.extend_from_slice(&[0x88, 0xac]);
    script
}

fn expand_p2sh(hash160: &[u8; SCRIPT_HASH160_LEN]) -> Vec<u8> {
    let mut script = Vec::with_capacity(P2SH_SCRIPT_LEN);
    script.extend_from_slice(&[0xa9, 0x14]);
    script.extend_from_slice(hash160);
    script.push(0x87);
    script
}

fn encode_varint_u32(number: u32, bytes: &mut Vec<u8>) {
    encode_varint_u64(u64::from(number), bytes);
}

fn encode_varint_u64(mut number: u64, bytes: &mut Vec<u8>) {
    loop {
        let low_bits = u8::try_from(number & 0x7f).unwrap_or(0);
        number >>= 7;
        if number == 0 {
            bytes.push(low_bits);
            return;
        }
        bytes.push(low_bits | 0x80);
    }
}

fn decode_varint_u32(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<u32, WalletProjectionContractError> {
    let wide = decode_varint_u64(encoded, offset, field)?;
    u32::try_from(wide)
        .map_err(|_| WalletProjectionContractError::DurableNonCanonicalEncoding { field })
}

fn decode_varint_u64(
    encoded: &[u8],
    offset: &mut usize,
    field: &'static str,
) -> Result<u64, WalletProjectionContractError> {
    let mut decoded = 0u64;
    let mut shift = 0u32;
    loop {
        let Some(&byte) = encoded.get(*offset) else {
            return Err(WalletProjectionContractError::DurableValueTooShort {
                field,
                minimum: offset.saturating_add(1),
                actual: encoded.len(),
            });
        };
        *offset = offset
            .checked_add(1)
            .ok_or(WalletProjectionContractError::DurableLengthPrefixMismatch { field })?;
        if shift == 63 && byte > 1 {
            return Err(WalletProjectionContractError::DurableNonCanonicalEncoding { field });
        }
        if (byte & 0x80) == 0 {
            if byte == 0 && shift > 0 {
                return Err(WalletProjectionContractError::DurableNonCanonicalEncoding { field });
            }
            decoded |= u64::from(byte) << shift;
            return Ok(decoded);
        }
        decoded |= u64::from(byte & 0x7f) << shift;
        shift = shift
            .checked_add(7)
            .ok_or(WalletProjectionContractError::DurableNonCanonicalEncoding { field })?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outpoint() -> TransparentOutPoint {
        TransparentOutPoint::new(TransactionId::from_bytes([0x11; 32]), 0x0102_0304)
    }

    fn p2pkh_script(fill: u8) -> Vec<u8> {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&[fill; SCRIPT_HASH160_LEN]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    }

    fn p2sh_script(fill: u8) -> Vec<u8> {
        let mut script = vec![0xa9, 0x14];
        script.extend_from_slice(&[fill; SCRIPT_HASH160_LEN]);
        script.push(0x87);
        script
    }

    fn sample_unspent_output() -> WalletUnspentOutput {
        let script = p2pkh_script(0x5a);
        WalletUnspentOutput::new(
            sample_outpoint(),
            TransparentAddressScriptHash::of_script_pub_key(&script),
            100_000_000,
            script,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x11; 32]),
                7,
                BlockId::new(
                    BlockHeight::new(2_500_000),
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
    fn outpoint_and_address_keys_have_exact_version_two_bytes() {
        let output = sample_unspent_output();
        let expected_address_hex = hex::encode(output.address_script_hash.as_bytes());
        assert_eq!(
            hex::encode(WalletOutpointKey::new(output.outpoint).as_bytes()),
            concat!(
                "1111111111111111111111111111111111111111111111111111111111111111",
                "01020304"
            )
        );
        assert_eq!(
            hex::encode(WalletAddressUnspentOutputKey::new(&output).as_bytes()),
            format!(
                "{expected_address_hex}002625a0\
                 1111111111111111111111111111111111111111111111111111111111111111\
                 01020304"
            )
        );
    }

    #[test]
    fn unspent_and_spent_values_have_exact_version_two_bytes() {
        let output = sample_unspent_output();
        // P2PKH (0x01) + derived hash omitted + value(100_000_000 = 80c2d72f)
        // + height(2_500_000 = a0cb9801) + tx_index(7 = 07) + 20-byte hash160.
        assert_eq!(
            hex::encode(
                output
                    .encode_value()
                    .unwrap_or_else(|error| unreachable!("valid sample output: {error}"))
            ),
            concat!(
                "01",
                "80c2d72f",
                "a0cb9801",
                "07",
                "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"
            )
        );

        let spent = WalletSpentOutput::new(
            output,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                3,
                BlockId::new(
                    BlockHeight::new(2_500_010),
                    BlockHash::from_bytes([0x55; 32]),
                ),
            ),
            1,
        );
        assert_eq!(
            hex::encode(
                spent
                    .encode_value()
                    .unwrap_or_else(|error| unreachable!("valid sample spend: {error}"))
            ),
            concat!(
                "01",
                "80c2d72f",
                "a0cb9801",
                "07",
                "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
                "4444444444444444444444444444444444444444444444444444444444444444",
                "aacb9801",
                "03",
                "01"
            )
        );
    }

    #[test]
    fn address_transaction_balance_block_hash_and_undo_have_exact_version_two_bytes() {
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
            "1111111111111111111111111111111111111111111111111111111111111111"
        );

        let block_hash_row = WalletBlockHash::new(BlockId::new(
            BlockHeight::new(0x0a0b_0c0d),
            BlockHash::from_bytes([0x33; 32]),
        ));
        assert_eq!(hex::encode(block_hash_row.encode_key()), "0a0b0c0d");
        assert_eq!(hex::encode(block_hash_row.encode_value()), "33".repeat(32));
        assert_eq!(
            WalletBlockHash::decode(&block_hash_row.encode_key(), &block_hash_row.encode_value()),
            Ok(block_hash_row)
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
    fn every_wallet_row_round_trips_exact_version_two_bytes()
    -> Result<(), WalletProjectionContractError> {
        let unspent_output = sample_unspent_output();
        let outpoint_key = WalletOutpointKey::new(unspent_output.outpoint);
        assert_eq!(
            WalletOutpointKey::decode(outpoint_key.as_bytes())?,
            outpoint_key
        );
        assert_eq!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &unspent_output.encode_value()?)?,
            StoredWalletUnspentOutput::from_unspent(&unspent_output)
        );
        assert_eq!(
            WalletUnspentOutput::decode_value_with_block_hash(
                outpoint_key,
                &unspent_output.encode_value()?,
                unspent_output.created_at.block.hash,
            )?,
            unspent_output
        );

        let spent_output = WalletSpentOutput::new(
            unspent_output,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                3,
                BlockId::new(
                    BlockHeight::new(2_500_020),
                    BlockHash::from_bytes([0x55; 32]),
                ),
            ),
            2,
        );
        assert_eq!(
            StoredWalletSpentOutput::decode_value(outpoint_key, &spent_output.encode_value()?)?,
            StoredWalletSpentOutput::from_spent(&spent_output)
        );
        assert_eq!(
            WalletSpentOutput::decode_value_with_block_hash(
                outpoint_key,
                &spent_output.encode_value()?,
                spent_output.output.created_at.block.hash,
                spent_output.spent_at.block.hash,
            )?,
            spent_output
        );
        Ok(())
    }

    #[test]
    fn address_and_undo_rows_round_trip_exact_version_two_bytes()
    -> Result<(), WalletProjectionContractError> {
        let unspent_output = sample_unspent_output();
        let outpoint_key = WalletOutpointKey::new(unspent_output.outpoint);
        let addr_out_key = WalletAddressUnspentOutputKey::new(&unspent_output);
        assert_eq!(
            WalletAddressUnspentOutputKey::decode(addr_out_key.as_bytes())?,
            addr_out_key
        );
        assert_eq!(addr_out_key.outpoint(), unspent_output.outpoint);

        let addr_tx_key = WalletAddressTransactionKey::new(
            unspent_output.address_script_hash,
            unspent_output.created_at.block.height,
            unspent_output.created_at.tx_index_in_block,
        );
        let addr_tx = WalletAddressTransaction::new(
            addr_tx_key,
            unspent_output.created_at.transaction_id,
            unspent_output.created_at.block.hash,
        );
        assert_eq!(
            WalletAddressTransaction::decode_value_with_block_hash(
                addr_tx_key,
                &addr_tx.encode_value(),
                unspent_output.created_at.block.hash,
            )?,
            addr_tx
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
            vec![addr_tx_key],
        );
        assert_eq!(
            WalletReorgUndo::decode(&undo.encode_key(), &undo.encode_value()?)?,
            undo
        );
        Ok(())
    }

    #[test]
    fn v2_output_round_trips_p2sh_raw_empty_and_explicit_address_variants()
    -> Result<(), WalletProjectionContractError> {
        let p2sh = p2sh_script(0x7c);
        let variants = [
            (
                p2sh.clone(),
                TransparentAddressScriptHash::of_script_pub_key(&p2sh),
                u64::MAX,
                u32::MAX,
                u32::MAX,
            ),
            (
                vec![0x51, 0xac],
                TransparentAddressScriptHash::of_script_pub_key(&[0x51, 0xac]),
                0,
                0,
                0,
            ),
            (
                Vec::new(),
                TransparentAddressScriptHash::of_script_pub_key(&[]),
                42,
                1,
                2,
            ),
            (
                p2pkh_script(0x19),
                TransparentAddressScriptHash::from_bytes([0x99; 32]),
                12_345,
                500,
                19,
            ),
        ];
        for (script, address_hash, amount_zat, height, tx_index) in variants {
            let output = WalletUnspentOutput::new(
                sample_outpoint(),
                address_hash,
                amount_zat,
                script,
                WalletTransactionPosition::new(
                    sample_outpoint().transaction_id,
                    tx_index,
                    BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([0x88; 32])),
                ),
            )?;
            let decoded = WalletUnspentOutput::decode_value_with_block_hash(
                WalletOutpointKey::new(output.outpoint),
                &output.encode_value()?,
                output.created_at.block.hash,
            )?;
            assert_eq!(decoded, output);
        }
        Ok(())
    }

    #[test]
    fn v2_decoders_reject_noncanonical_encodings_and_trailing_bytes()
    -> Result<(), WalletProjectionContractError> {
        let output = sample_unspent_output();
        let outpoint_key = WalletOutpointKey::new(output.outpoint);
        let mut trailing = output.encode_value()?;
        trailing.push(0);
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &trailing),
            Err(WalletProjectionContractError::DurableTrailingBytes { .. })
        ));
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &trailing[..3]),
            Err(WalletProjectionContractError::DurableValueTooShort { .. })
        ));

        // Raw-encoded standard P2PKH must be rejected.
        let mut raw_p2pkh = vec![SCRIPT_KIND_RAW];
        encode_varint_u64(output.value_zat, &mut raw_p2pkh);
        encode_varint_u32(output.created_at.block.height.value(), &mut raw_p2pkh);
        encode_varint_u32(output.created_at.tx_index_in_block, &mut raw_p2pkh);
        encode_varint_u32(25, &mut raw_p2pkh);
        raw_p2pkh.extend_from_slice(&output.script_pub_key);
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &raw_p2pkh),
            Err(WalletProjectionContractError::DurableNonCanonicalEncoding { .. })
        ));

        // Explicit address hash matching SHA256(script) must be rejected.
        let mut redundant_hash = vec![SCRIPT_KIND_P2PKH | EXPLICIT_ADDRESS_HASH_FLAG];
        redundant_hash.extend_from_slice(&output.address_script_hash.as_bytes());
        encode_varint_u64(output.value_zat, &mut redundant_hash);
        encode_varint_u32(output.created_at.block.height.value(), &mut redundant_hash);
        encode_varint_u32(output.created_at.tx_index_in_block, &mut redundant_hash);
        redundant_hash.extend_from_slice(&[0x5a; SCRIPT_HASH160_LEN]);
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &redundant_hash),
            Err(WalletProjectionContractError::DurableNonCanonicalEncoding { .. })
        ));

        // Overlong varint (0x80 0x00 for value = 0) must be rejected.
        let mut overlong_varint = vec![SCRIPT_KIND_P2PKH, 0x80, 0x00, 0x01, 0x00];
        overlong_varint.extend_from_slice(&[0x5a; SCRIPT_HASH160_LEN]);
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &overlong_varint),
            Err(WalletProjectionContractError::DurableNonCanonicalEncoding { .. })
        ));

        // Reserved script kind 3 and reserved upper flag bits must be rejected.
        let mut reserved_kind = output.encode_value()?;
        reserved_kind[0] = 0x03;
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &reserved_kind),
            Err(WalletProjectionContractError::UnsupportedEncodedValue { .. })
        ));
        reserved_kind[0] = 0x08;
        assert!(matches!(
            StoredWalletUnspentOutput::decode_value(outpoint_key, &reserved_kind),
            Err(WalletProjectionContractError::UnsupportedEncodedValue { .. })
        ));

        assert!(matches!(
            WalletOutpointKey::decode(&outpoint_key.as_bytes()[..35]),
            Err(WalletProjectionContractError::DurableFieldLengthMismatch { .. })
        ));
        assert!(matches!(
            WalletBlockHash::decode(&[0; 3], &[0; 32]),
            Err(WalletProjectionContractError::DurableFieldLengthMismatch { .. })
        ));
        assert!(matches!(
            WalletBlockHash::decode(&[0; 4], &[0; 64]),
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
