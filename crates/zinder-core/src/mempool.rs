//! Mempool entry vocabulary.
//!
//! These values describe a single hydrated mempool transaction as the
//! indexer observed it. They are produced by ingest after the source layer
//! delivers raw observations and the ingest layer stamps them with the chain
//! epoch visible at first observation.

use crate::{
    AuthDigest, ChainEpoch, CompactTransactionData, RawTransactionBytes, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, UnixTimestampMillis,
};
use thiserror::Error;

/// Hydrated record describing a mempool transaction observed by the indexer.
///
/// `MempoolEntry` is the canonical server-observed type. It is never used to
/// describe wallet-local pending state. Wallet-local "pending" can include
/// transactions the network never accepted; `MempoolEntry` only describes
/// transactions that an upstream node observed in its mempool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MempoolEntry {
    /// Transaction identifier reported by the source.
    transaction_id: TransactionId,
    /// ZIP-244 authorization digest, when the source provides one.
    ///
    /// Set for v5+ transactions; `None` for v1-v4 transactions where the
    /// txid alone authenticates witness data.
    auth_digest: Option<AuthDigest>,
    /// Raw serialized transaction bytes hydrated from the source.
    raw_transaction_bytes: RawTransactionBytes,
    /// Structured wallet scan data derived from the raw transaction.
    compact_transaction_data: CompactTransactionData,
    /// Wall-clock time when the indexer first observed this mempool entry.
    first_seen_unix_millis: UnixTimestampMillis,
    /// Chain epoch visible to ingest when this mempool entry was first
    /// observed.
    first_seen_chain_epoch: ChainEpoch,
    /// Transparent outputs created by this mempool transaction, indexed for
    /// address lookups.
    transparent_outputs: Vec<TransparentMempoolOutput>,
    /// Transparent inputs that spend previously-known outpoints.
    transparent_spends: Vec<TransparentMempoolSpend>,
}

impl MempoolEntry {
    /// Creates an entry and derives its transparent lookup indexes from the
    /// structured scan data and enclosing transaction identifier.
    pub fn new(
        transaction_id: TransactionId,
        auth_digest: Option<AuthDigest>,
        raw_transaction_bytes: RawTransactionBytes,
        compact_transaction_data: CompactTransactionData,
        observation: MempoolObservation,
    ) -> Result<Self, MempoolEntryBuildError> {
        let transparent_outputs = compact_transaction_data
            .transparent_outputs
            .iter()
            .enumerate()
            .map(|(output_index, output)| {
                let output_index = u32::try_from(output_index)
                    .map_err(|_| MempoolEntryBuildError::TransparentOutputIndexOverflow)?;
                Ok(TransparentMempoolOutput {
                    address_script_hash: TransparentAddressScriptHash::of_script_pub_key(
                        &output.script_pub_key,
                    ),
                    script_pub_key: output.script_pub_key.clone(),
                    outpoint: TransparentOutPoint::new(transaction_id, output_index),
                    value_zat: output.value_zat,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let transparent_spends = compact_transaction_data
            .transparent_inputs
            .iter()
            .map(|input| TransparentMempoolSpend {
                spent_outpoint: TransparentOutPoint::new(
                    input.previous_transaction_id,
                    input.previous_output_index,
                ),
                spending_transaction_id: transaction_id,
            })
            .collect();

        Ok(Self {
            transaction_id,
            auth_digest,
            raw_transaction_bytes,
            compact_transaction_data,
            first_seen_unix_millis: observation.first_seen_unix_millis,
            first_seen_chain_epoch: observation.first_seen_chain_epoch,
            transparent_outputs,
            transparent_spends,
        })
    }

    /// Returns the transaction identifier.
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the authorization digest when available.
    #[must_use]
    pub const fn auth_digest(&self) -> Option<AuthDigest> {
        self.auth_digest
    }

    /// Returns the raw serialized transaction bytes.
    #[must_use]
    pub const fn raw_transaction_bytes(&self) -> &RawTransactionBytes {
        &self.raw_transaction_bytes
    }

    /// Returns structured wallet scan data.
    #[must_use]
    pub const fn compact_transaction_data(&self) -> &CompactTransactionData {
        &self.compact_transaction_data
    }

    /// Returns the first-observed timestamp.
    #[must_use]
    pub const fn first_seen_unix_millis(&self) -> UnixTimestampMillis {
        self.first_seen_unix_millis
    }

    /// Returns the chain epoch visible at first observation.
    #[must_use]
    pub const fn first_seen_chain_epoch(&self) -> ChainEpoch {
        self.first_seen_chain_epoch
    }

    /// Returns derived transparent output indexes.
    #[must_use]
    pub fn transparent_outputs(&self) -> &[TransparentMempoolOutput] {
        &self.transparent_outputs
    }

    /// Returns derived transparent spend indexes.
    #[must_use]
    pub fn transparent_spends(&self) -> &[TransparentMempoolSpend] {
        &self.transparent_spends
    }

    /// Consumes the entry into source-observed fields; derived indexes are
    /// intentionally omitted because they are reconstructed by [`Self::new`].
    #[must_use]
    pub fn into_parts(self) -> MempoolEntryParts {
        MempoolEntryParts {
            transaction_id: self.transaction_id,
            auth_digest: self.auth_digest,
            raw_transaction_bytes: self.raw_transaction_bytes,
            compact_transaction_data: self.compact_transaction_data,
            observation: MempoolObservation {
                first_seen_unix_millis: self.first_seen_unix_millis,
                first_seen_chain_epoch: self.first_seen_chain_epoch,
            },
        }
    }
}

/// Source-observed fields that uniquely determine a mempool entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MempoolEntryParts {
    /// Transaction identifier.
    pub transaction_id: TransactionId,
    /// Authorization digest when available.
    pub auth_digest: Option<AuthDigest>,
    /// Raw serialized transaction bytes.
    pub raw_transaction_bytes: RawTransactionBytes,
    /// Structured wallet scan data.
    pub compact_transaction_data: CompactTransactionData,
    /// First-observation chain context and time.
    pub observation: MempoolObservation,
}

/// Chain context and time attached to the first source observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolObservation {
    /// Wall-clock time when the indexer first observed the transaction.
    pub first_seen_unix_millis: UnixTimestampMillis,
    /// Chain epoch visible at the first observation.
    pub first_seen_chain_epoch: ChainEpoch,
}

/// Error returned when structured mempool data cannot form lookup indexes.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolEntryBuildError {
    /// The transaction has more transparent outputs than an outpoint can index.
    #[error("transparent output count exceeds u32::MAX")]
    TransparentOutputIndexOverflow,
}

/// Reason a mempool transaction was removed from the source's mempool view
/// without being mined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolEvictionReason {
    /// A transaction spending the same inputs was admitted instead.
    Conflict,
    /// Source mempool expiry policy removed the transaction.
    Expired,
    /// Fee policy rejected the transaction at the source.
    LowFee,
    /// Source consensus or policy verifier rejected the transaction.
    NodeRejected,
    /// Source removed the transaction without a classifiable reason.
    ///
    /// Polling backends that observe a txid disappear without a chain
    /// commit emit this reason. The streaming `MempoolChange` wire
    /// envelope from Zebra carries no eviction reason, so streaming
    /// backends always emit `Unknown`.
    Unknown,
}

/// Transparent output visible in the live mempool index.
///
/// Mempool outputs do not carry a block height because the transaction has
/// not been mined yet. Consumers correlating mempool outputs with mined
/// outputs must compare on [`TransparentOutPoint`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentMempoolOutput {
    /// Hash of the transparent output script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Raw `scriptPubKey` bytes for the output.
    pub script_pub_key: Vec<u8>,
    /// Output identity within the mempool transaction.
    pub outpoint: TransparentOutPoint,
    /// Output value in zatoshis.
    pub value_zat: u64,
}

/// Transparent input visible in the live mempool index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentMempoolSpend {
    /// Outpoint consumed by this mempool input.
    pub spent_outpoint: TransparentOutPoint,
    /// Transaction in the mempool that consumes the outpoint.
    pub spending_transaction_id: TransactionId,
}
