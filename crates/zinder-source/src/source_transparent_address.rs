//! Transparent-address lookups answered by the node's address index.
//!
//! [`crate::ZebraJsonRpcSource`] forwards these lookups to `getaddressbalance`,
//! `getaddresstxids`, and `getaddressutxos`. The types here hold the
//! validated request and the decoded rows, so the request bounds and the
//! row order are the same whichever certified node answers.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use serde::Deserialize;
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::{BlockHeight, Network, TransparentOutPoint};

use crate::source_block::wire_error_to_transaction_id_error;
use crate::{SourceError, parse_transparent_address, transparent_address_matches_network};

/// Most distinct addresses one lookup carries.
///
/// Zebra refuses a request with more addresses. Zakura has no limit, so the
/// lookup enforces Zebra's for both nodes.
pub const MAX_TRANSPARENT_ADDRESS_LOOKUP_ADDRESSES: usize = 10_000;

/// Validated set of transparent addresses for one node lookup.
///
/// The addresses are distinct, belong to one [`Network`], and are not ZIP 320
/// TEX addresses. A node indexes the P2PKH address a TEX address is derived
/// from, so Zebra refuses a TEX address and Zakura matches nothing for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressLookup {
    network: Network,
    encoded_addresses: Vec<String>,
}

impl TransparentAddressLookup {
    /// Validates `addresses` for a lookup on `network`.
    ///
    /// Duplicate addresses are merged.
    ///
    /// # Errors
    ///
    /// Returns [`TransparentAddressLookupError`] when the set is empty, has
    /// more than [`MAX_TRANSPARENT_ADDRESS_LOOKUP_ADDRESSES`] distinct
    /// addresses, holds a TEX address, or holds an address of another network.
    pub fn new(
        network: Network,
        addresses: impl IntoIterator<Item = ZebraTransparentAddress>,
    ) -> Result<Self, TransparentAddressLookupError> {
        let mut encoded_addresses = BTreeSet::new();
        for address in addresses {
            if matches!(address, ZebraTransparentAddress::Tex { .. }) {
                return Err(TransparentAddressLookupError::TexAddress);
            }
            if !transparent_address_matches_network(address.network_kind(), network) {
                return Err(TransparentAddressLookupError::WrongNetwork { network });
            }
            encoded_addresses.insert(address.to_string());
            if encoded_addresses.len() > MAX_TRANSPARENT_ADDRESS_LOOKUP_ADDRESSES {
                return Err(TransparentAddressLookupError::TooManyAddresses);
            }
        }
        if encoded_addresses.is_empty() {
            return Err(TransparentAddressLookupError::NoAddresses);
        }
        Ok(Self {
            network,
            encoded_addresses: encoded_addresses.into_iter().collect(),
        })
    }

    /// Returns the network the addresses belong to.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// Returns the number of distinct addresses.
    #[must_use]
    pub const fn address_count(&self) -> usize {
        self.encoded_addresses.len()
    }

    /// Returns the request's `addresses` member.
    pub(crate) fn rpc_addresses(&self) -> &[String] {
        &self.encoded_addresses
    }
}

/// Why [`TransparentAddressLookup::new`] refused its addresses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentAddressLookupError {
    /// The lookup holds no address.
    #[error("transparent address lookup holds no address")]
    NoAddresses,
    /// The lookup holds more distinct addresses than a node accepts.
    #[error(
        "transparent address lookup holds more than \
         {MAX_TRANSPARENT_ADDRESS_LOOKUP_ADDRESSES} distinct addresses"
    )]
    TooManyAddresses,
    /// The lookup holds a ZIP 320 TEX address.
    #[error(
        "transparent address lookups do not accept TEX addresses; use the P2PKH address it encodes"
    )]
    TexAddress,
    /// The lookup holds an address of another network.
    #[error("transparent address does not belong to {network:?}")]
    WrongNetwork {
        /// Network the lookup targets.
        network: Network,
    },
}

/// Most rows one `getaddresstxids` or `getaddressutxos` answer may hold.
///
/// Zebra refuses a lookup that matches more rows, and a `maxEntries` above
/// this bound. Zakura has no limit, so the source enforces Zebra's for both
/// nodes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransparentAddressRowLimit(NonZeroU32);

impl TransparentAddressRowLimit {
    /// The largest row limit a node accepts.
    pub const MAX: Self = match NonZeroU32::new(100_000) {
        Some(rows) => Self(rows),
        None => Self(NonZeroU32::MIN),
    };

    /// Creates a row limit, or `None` when `rows` is zero or above
    /// [`Self::MAX`].
    #[must_use]
    pub const fn new(rows: u32) -> Option<Self> {
        match NonZeroU32::new(rows) {
            Some(rows) if rows.get() <= Self::MAX.0.get() => Some(Self(rows)),
            Some(_) | None => None,
        }
    }

    /// Returns the limit as a row count.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    pub(crate) fn row_count(self) -> usize {
        usize::try_from(self.get()).unwrap_or(usize::MAX)
    }
}

/// Confirmed transparent balance of a [`TransparentAddressLookup`], as the
/// node reports it at its current tip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceTransparentAddressBalance {
    /// Sum of the addresses' unspent outputs, in zatoshis.
    pub balance_zat: u64,
    /// Sum of every output the addresses ever received, change included, in
    /// zatoshis.
    pub received_zat: u64,
}

/// Unspent transparent output the node's address index returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTransparentUnspentOutput {
    /// Address the output pays.
    pub address: ZebraTransparentAddress,
    /// Output identity.
    pub outpoint: TransparentOutPoint,
    /// Raw `scriptPubKey` bytes.
    pub script_pub_key: Vec<u8>,
    /// Output value in zatoshis.
    pub value_zat: u64,
    /// Height of the block that mined the output.
    pub block_height: BlockHeight,
}

/// `getaddressbalance` answer.
#[derive(Deserialize)]
pub(crate) struct ZebraAddressBalance {
    balance: u64,
    received: u64,
}

impl From<ZebraAddressBalance> for SourceTransparentAddressBalance {
    fn from(balance: ZebraAddressBalance) -> Self {
        Self {
            balance_zat: balance.balance,
            received_zat: balance.received,
        }
    }
}

/// One `getaddressutxos` row.
#[derive(Deserialize)]
pub(crate) struct ZebraAddressUtxo {
    address: String,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: u32,
    script: String,
    satoshis: u64,
    height: u32,
}

/// Decodes `getaddressutxos` rows into the page the caller asked for.
///
/// Zebra applies `startHeight` and `maxEntries` itself. Zakura ignores both
/// and returns every unspent output in chain order, so the rows below
/// `start_height` are dropped and the rest truncated to `max_rows` here. Both
/// nodes return rows in chain order, which keeps the two results identical.
pub(crate) fn decode_address_utxo_page(
    rows: Vec<ZebraAddressUtxo>,
    start_height: BlockHeight,
    max_rows: TransparentAddressRowLimit,
) -> Result<Vec<SourceTransparentUnspentOutput>, SourceError> {
    let page_capacity = rows.len().min(max_rows.row_count());
    let mut page = Vec::with_capacity(page_capacity);
    let mut previous_height = BlockHeight::new(0);
    for row in rows {
        let block_height = BlockHeight::new(row.height);
        if block_height < previous_height {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "getaddressutxos rows are not in chain order",
            });
        }
        previous_height = block_height;
        if block_height < start_height || page.len() == page_capacity {
            continue;
        }
        page.push(decode_address_utxo(&row, block_height)?);
    }
    Ok(page)
}

fn decode_address_utxo(
    row: &ZebraAddressUtxo,
    block_height: BlockHeight,
) -> Result<SourceTransparentUnspentOutput, SourceError> {
    let address = parse_transparent_address(&row.address).map_err(|_| {
        SourceError::SourceProtocolMismatch {
            reason: "getaddressutxos returned an invalid transparent address",
        }
    })?;
    let transaction_id = zinder_core::wire::decode_rpc_transaction_id_hex(&row.txid)
        .map_err(wire_error_to_transaction_id_error)?;
    let script_pub_key =
        hex::decode(&row.script).map_err(|_| SourceError::SourceProtocolMismatch {
            reason: "getaddressutxos returned a script that is not hex",
        })?;
    Ok(SourceTransparentUnspentOutput {
        address,
        outpoint: TransparentOutPoint::new(transaction_id, row.output_index),
        script_pub_key,
        value_zat: row.satoshis,
        block_height,
    })
}

#[cfg(test)]
mod tests {
    use zebra_chain::parameters::NetworkKind as ZebraNetworkKind;

    use super::*;

    fn testnet_address(hash_byte: u8) -> ZebraTransparentAddress {
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Testnet, [hash_byte; 20])
    }

    fn utxo_row(height: u32, output_index: u32) -> ZebraAddressUtxo {
        ZebraAddressUtxo {
            address: testnet_address(1).to_string(),
            txid: "11".repeat(32),
            output_index,
            script: "76a914".to_owned(),
            satoshis: 5,
            height,
        }
    }

    #[test]
    fn lookup_merges_duplicates_and_keeps_network() -> Result<(), TransparentAddressLookupError> {
        let lookup = TransparentAddressLookup::new(
            Network::ZcashRegtest,
            [testnet_address(1), testnet_address(2), testnet_address(1)],
        )?;
        assert_eq!(lookup.address_count(), 2);
        assert_eq!(lookup.network(), Network::ZcashRegtest);
        Ok(())
    }

    #[test]
    fn lookup_refuses_empty_tex_foreign_and_oversized_sets() {
        assert_eq!(
            TransparentAddressLookup::new(Network::ZcashTestnet, []),
            Err(TransparentAddressLookupError::NoAddresses)
        );
        let tex = ZebraTransparentAddress::Tex {
            network_kind: ZebraNetworkKind::Testnet,
            validating_key_hash: [3; 20],
        };
        assert_eq!(
            TransparentAddressLookup::new(Network::ZcashTestnet, [tex]),
            Err(TransparentAddressLookupError::TexAddress)
        );
        let mainnet =
            ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Mainnet, [4; 20]);
        assert_eq!(
            TransparentAddressLookup::new(Network::ZcashTestnet, [mainnet]),
            Err(TransparentAddressLookupError::WrongNetwork {
                network: Network::ZcashTestnet
            })
        );
        let too_many = (0_u32..)
            .take(MAX_TRANSPARENT_ADDRESS_LOOKUP_ADDRESSES + 1)
            .map(|index| {
                let mut hash = [0_u8; 20];
                hash[..4].copy_from_slice(&index.to_le_bytes());
                ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Testnet, hash)
            });
        assert_eq!(
            TransparentAddressLookup::new(Network::ZcashTestnet, too_many),
            Err(TransparentAddressLookupError::TooManyAddresses)
        );
    }

    #[test]
    fn row_limit_accepts_one_through_the_node_cap() {
        assert_eq!(TransparentAddressRowLimit::MAX.get(), 100_000);
        assert_eq!(
            TransparentAddressRowLimit::new(1).map(TransparentAddressRowLimit::get),
            Some(1)
        );
        assert_eq!(
            TransparentAddressRowLimit::new(100_000),
            Some(TransparentAddressRowLimit::MAX)
        );
        assert_eq!(TransparentAddressRowLimit::new(0), None);
        assert_eq!(TransparentAddressRowLimit::new(100_001), None);
    }

    #[test]
    fn utxo_page_applies_start_height_and_row_limit_a_node_ignored()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = vec![
            utxo_row(3, 0),
            utxo_row(5, 0),
            utxo_row(5, 1),
            utxo_row(9, 0),
        ];
        let max_rows = TransparentAddressRowLimit::new(2).ok_or("row limit")?;

        let page = decode_address_utxo_page(rows, BlockHeight::new(4), max_rows)?;

        let positions: Vec<(u32, u32)> = page
            .iter()
            .map(|output| (output.block_height.value(), output.outpoint.output_index))
            .collect();
        assert_eq!(positions, [(5, 0), (5, 1)]);
        Ok(())
    }

    #[test]
    fn utxo_page_refuses_rows_out_of_chain_order() {
        let rows = vec![utxo_row(5, 0), utxo_row(4, 0)];
        assert!(matches!(
            decode_address_utxo_page(rows, BlockHeight::new(0), TransparentAddressRowLimit::MAX),
            Err(SourceError::SourceProtocolMismatch { .. })
        ));
    }
}
