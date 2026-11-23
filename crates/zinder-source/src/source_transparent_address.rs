//! Transparent-address lookups answered by the node's address index.
//!
//! [`crate::ZebraJsonRpcSource`] forwards these lookups to `getaddressbalance`,
//! `getaddresstxids`, and `getaddressutxos`. The types here hold the
//! validated request and the decoded rows, so the request bounds and the
//! row order are the same whichever certified node answers.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;

use serde::Deserialize;
use zcash_transparent::{address::TransparentAddress, bundle::TxOut};
use zebra_chain::amount::Amount;
use zebra_chain::transparent::{
    Address as ZebraTransparentAddress, Output as ZebraTransparentOutput, Script as ZebraScript,
};
use zinder_core::{BlockHeight, BlockId, Network, TransactionId, TransparentOutPoint};

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

    /// Returns whether `address` is one of the lookup's addresses.
    pub(crate) fn holds(&self, address: &ZebraTransparentAddress) -> bool {
        self.encoded_addresses
            .binary_search(&address.to_string())
            .is_ok()
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
/// Zebra refuses a `getaddresstxids` lookup that matches more rows, a
/// `getaddressutxos` lookup that matches more rows when `maxEntries` is zero,
/// and a `maxEntries` above this bound. Zakura has no limit, so the source
/// enforces Zebra's for both nodes.
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
    /// Raw `scriptPubKey` bytes, a P2PKH or P2SH script that pays
    /// [`Self::address`].
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

impl ZebraAddressUtxo {
    /// Returns the height of the block that mined the output.
    pub(crate) const fn height(&self) -> u32 {
        self.height
    }
}

/// How many of the matching unspent outputs a `getaddressutxos` lookup keeps.
#[derive(Clone, Copy, Debug)]
pub(crate) enum UtxoRowBound {
    /// Every matching output. More than [`TransparentAddressRowLimit::MAX`]
    /// is refused instead of truncated.
    Complete,
    /// The first matching outputs in chain order, up to the limit.
    Prefix(TransparentAddressRowLimit),
}

impl UtxoRowBound {
    /// Returns the request's `maxEntries` member.
    ///
    /// Zebra returns the first `maxEntries` matching outputs without saying
    /// whether more match. Only zero makes it refuse a lookup that matches
    /// more than [`TransparentAddressRowLimit::MAX`] outputs.
    pub(crate) const fn rpc_max_entries(self) -> u32 {
        match self {
            Self::Complete => 0,
            Self::Prefix(max_rows) => max_rows.get(),
        }
    }
}

/// A chain-ordered response to a bounded unspent-output lookup.
#[derive(Debug)]
pub struct SourceTransparentUtxoPage {
    /// Matching outputs the node supplied, up to [`TransparentAddressRowLimit::MAX`].
    pub outputs: Vec<SourceTransparentUnspentOutput>,
    /// True when the reply proves these are all matches at or above the requested height.
    /// A prefix that fills the node's requested limit can leave completeness unknown.
    pub complete: bool,
    /// The best-chain tip the node reports answering at.
    pub tip: BlockId,
}

/// Decoded `getaddressutxos` rows, before the reply's tip is attached.
pub(crate) struct DecodedUtxos {
    pub(crate) outputs: Vec<SourceTransparentUnspentOutput>,
    pub(crate) complete: bool,
}

/// Decodes the `getaddressutxos` rows of `lookup` at or above `start_height`.
///
/// Zebra applies `startHeight` and `maxEntries` itself. Zakura ignores both
/// and returns every unspent output in chain order, so the rows below
/// `start_height` are dropped and the rest bounded here. Both nodes return
/// rows in chain order, which keeps the two results identical. Every row is
/// validated before any is dropped, so a row the answer leaves out cannot
/// hide a node that breaks the contract.
///
/// # Errors
///
/// A complete lookup with more than [`TransparentAddressRowLimit::MAX`]
/// matching rows returns [`SourceError::TransparentAddressRowLimitExceeded`].
/// A row for an address outside `lookup`, a script that does not pay the
/// row's address, rows out of chain order, or a repeated outpoint return
/// [`SourceError::SourceProtocolMismatch`].
pub(crate) fn decode_address_utxos(
    rows: &[ZebraAddressUtxo],
    lookup: &TransparentAddressLookup,
    start_height: BlockHeight,
    bound: UtxoRowBound,
) -> Result<DecodedUtxos, SourceError> {
    // The rows of one address repeat its text and script, so each distinct
    // pair is checked once.
    let mut checked_payees: HashMap<(&str, &str), ZebraTransparentAddress> = HashMap::new();
    let max_rows = TransparentAddressRowLimit::MAX;
    let mut outputs = Vec::with_capacity(rows.len().min(max_rows.row_count()));
    let mut matching = 0;
    let mut order = UtxoChainOrder::default();
    for row in rows {
        let script_pub_key =
            hex::decode(&row.script).map_err(|_| SourceError::SourceProtocolMismatch {
                reason: "getaddressutxos returned a script that is not hex",
            })?;
        let address = match checked_payees.entry((row.address.as_str(), row.script.as_str())) {
            Entry::Occupied(checked) => *checked.get(),
            Entry::Vacant(unchecked) => {
                *unchecked.insert(checked_payee(&row.address, &script_pub_key, lookup)?)
            }
        };
        let output = decode_address_utxo(row, address, script_pub_key)?;
        order.check(&output)?;
        if output.block_height >= start_height {
            matching += 1;
            if matching <= max_rows.row_count() {
                outputs.push(output);
            }
        }
    }
    // Zebra stops at maxEntries. Zakura returns the full set even when the
    // requested limit is smaller. Only matching rows count toward our cap.
    let complete = matching <= max_rows.row_count()
        && match bound {
            UtxoRowBound::Complete => true,
            UtxoRowBound::Prefix(limit) => rows.len() != limit.row_count() || matching < rows.len(),
        };
    if matches!(bound, UtxoRowBound::Complete) && !complete {
        return Err(SourceError::TransparentAddressRowLimitExceeded {
            operation: "getaddressutxos",
            max_rows: max_rows.get(),
        });
    }
    outputs.shrink_to_fit();
    Ok(DecodedUtxos { outputs, complete })
}

/// Returns the address a row names after checking that `lookup` holds it
/// and that the row's script pays it.
fn checked_payee(
    address_text: &str,
    script_pub_key: &[u8],
    lookup: &TransparentAddressLookup,
) -> Result<ZebraTransparentAddress, SourceError> {
    let address = parse_transparent_address(address_text).map_err(|_| {
        SourceError::SourceProtocolMismatch {
            reason: "getaddressutxos returned an invalid transparent address",
        }
    })?;
    if !lookup.holds(&address) {
        return Err(SourceError::SourceProtocolMismatch {
            reason: "getaddressutxos returned an output of an address the lookup does not hold",
        });
    }
    let pays_address = script_recipient(script_pub_key)
        .is_some_and(|recipient| TransparentAddress::try_from(address) == Ok(recipient));
    if !pays_address {
        return Err(SourceError::SourceProtocolMismatch {
            reason: "getaddressutxos returned a script that does not pay the row's address",
        });
    }
    Ok(address)
}

fn decode_address_utxo(
    row: &ZebraAddressUtxo,
    address: ZebraTransparentAddress,
    script_pub_key: Vec<u8>,
) -> Result<SourceTransparentUnspentOutput, SourceError> {
    let transaction_id = zinder_core::wire::decode_rpc_transaction_id_hex(&row.txid)
        .map_err(wire_error_to_transaction_id_error)?;
    Ok(SourceTransparentUnspentOutput {
        address,
        outpoint: TransparentOutPoint::new(transaction_id, row.output_index),
        script_pub_key,
        value_zat: row.satoshis,
        block_height: BlockHeight::new(row.height),
    })
}

/// Returns the P2PKH or P2SH address `script_pub_key` pays, or `None` for any
/// other script.
///
/// Both nodes index an output under the address `zcash_transparent` derives
/// from its script, so a row whose script pays no address, or another one,
/// cannot come from their indexes.
fn script_recipient(script_pub_key: &[u8]) -> Option<TransparentAddress> {
    let output = ZebraTransparentOutput::new(Amount::zero(), ZebraScript::new(script_pub_key));
    TxOut::try_from(&output).ok()?.recipient_address()
}

#[derive(Default)]
struct UtxoChainOrder {
    previous: Option<(BlockHeight, TransparentOutPoint)>,
    finished_transactions: HashSet<TransactionId>,
}

impl UtxoChainOrder {
    fn check(&mut self, output: &SourceTransparentUnspentOutput) -> Result<(), SourceError> {
        let Some((height, outpoint)) = self
            .previous
            .replace((output.block_height, output.outpoint))
        else {
            return Ok(());
        };
        let in_order = if output.outpoint.transaction_id == outpoint.transaction_id {
            output.block_height == height && output.outpoint.output_index > outpoint.output_index
        } else {
            self.finished_transactions.insert(outpoint.transaction_id);
            output.block_height >= height
                && !self
                    .finished_transactions
                    .contains(&output.outpoint.transaction_id)
        };
        if !in_order {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "getaddressutxos rows are not in chain order or repeat an outpoint",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zebra_chain::parameters::NetworkKind as ZebraNetworkKind;

    use super::*;

    fn testnet_address(hash_byte: u8) -> ZebraTransparentAddress {
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Testnet, [hash_byte; 20])
    }

    /// Txid hex of the transaction a test block at `height` holds.
    fn block_transaction_hex(height: u32) -> String {
        let mut transaction_id = [0_u8; 32];
        transaction_id[..4].copy_from_slice(&height.to_le_bytes());
        hex::encode(transaction_id)
    }

    fn script_hex(address: ZebraTransparentAddress) -> String {
        hex::encode(address.script().as_raw_bytes())
    }

    /// Row for output `output_index` of the transaction at `height`, paying
    /// address 1.
    fn utxo_row(height: u32, output_index: u32) -> ZebraAddressUtxo {
        ZebraAddressUtxo {
            address: testnet_address(1).to_string(),
            txid: block_transaction_hex(height),
            output_index,
            script: script_hex(testnet_address(1)),
            satoshis: 5,
            height,
        }
    }

    fn decode_complete(
        rows: &[ZebraAddressUtxo],
        lookup: &TransparentAddressLookup,
    ) -> Result<Vec<SourceTransparentUnspentOutput>, SourceError> {
        decode_address_utxos(rows, lookup, BlockHeight::new(0), UtxoRowBound::Complete)
            .map(|page| page.outputs)
    }

    fn mismatch_reason<T>(outcome: &Result<T, SourceError>) -> Option<&'static str> {
        match outcome {
            Err(SourceError::SourceProtocolMismatch { reason }) => Some(*reason),
            Ok(_) | Err(_) => None,
        }
    }

    const SCRIPT_MISMATCH: Option<&str> =
        Some("getaddressutxos returned a script that does not pay the row's address");
    const ORDER_MISMATCH: Option<&str> =
        Some("getaddressutxos rows are not in chain order or repeat an outpoint");

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

    fn lookup_of_address_one() -> Result<TransparentAddressLookup, TransparentAddressLookupError> {
        TransparentAddressLookup::new(Network::ZcashTestnet, [testnet_address(1)])
    }

    fn heights_and_indexes(outputs: &[SourceTransparentUnspentOutput]) -> Vec<(u32, u32)> {
        outputs
            .iter()
            .map(|output| (output.block_height.value(), output.outpoint.output_index))
            .collect()
    }

    /// Rows at height 1, then `matching_rows` rows from height 5 up.
    fn rows_with_matching_count(matching_rows: usize) -> Vec<ZebraAddressUtxo> {
        let below_start = std::iter::once(utxo_row(1, 0));
        let matching = (0_u32..)
            .take(matching_rows)
            .map(|index| utxo_row(5 + index / 4, index % 4));
        below_start.chain(matching).collect()
    }

    #[test]
    fn utxo_page_keeps_matching_rows_when_the_node_ignores_its_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = [
            utxo_row(3, 0),
            utxo_row(5, 0),
            utxo_row(5, 1),
            utxo_row(9, 0),
        ];
        let max_rows = TransparentAddressRowLimit::new(2).ok_or("row limit")?;

        let prefix = decode_address_utxos(
            &rows,
            &lookup_of_address_one()?,
            BlockHeight::new(4),
            UtxoRowBound::Prefix(max_rows),
        )?;

        assert!(prefix.complete);
        assert_eq!(
            heights_and_indexes(&prefix.outputs),
            [(5, 0), (5, 1), (9, 0)]
        );
        Ok(())
    }

    #[test]
    fn complete_utxos_keep_every_row_at_or_above_the_start_height()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = [
            utxo_row(3, 0),
            utxo_row(5, 0),
            utxo_row(5, 1),
            utxo_row(9, 0),
        ];

        let outputs = decode_address_utxos(
            &rows,
            &lookup_of_address_one()?,
            BlockHeight::new(4),
            UtxoRowBound::Complete,
        )?;

        assert_eq!(
            heights_and_indexes(&outputs.outputs),
            [(5, 0), (5, 1), (9, 0)]
        );
        Ok(())
    }

    #[test]
    fn complete_utxos_refuse_more_matching_rows_than_the_cap_instead_of_truncating()
    -> Result<(), Box<dyn std::error::Error>> {
        // A node that ignores maxEntries returns every unspent output.
        let rows = rows_with_matching_count(TransparentAddressRowLimit::MAX.row_count() + 1);
        let lookup = lookup_of_address_one()?;

        let refusal =
            decode_address_utxos(&rows, &lookup, BlockHeight::new(5), UtxoRowBound::Complete);
        let prefix = decode_address_utxos(
            &rows,
            &lookup,
            BlockHeight::new(5),
            UtxoRowBound::Prefix(TransparentAddressRowLimit::MAX),
        )?;

        assert!(matches!(
            refusal,
            Err(SourceError::TransparentAddressRowLimitExceeded {
                operation: "getaddressutxos",
                max_rows: 100_000,
            })
        ));
        assert!(!prefix.complete);
        assert_eq!(
            prefix.outputs.len(),
            TransparentAddressRowLimit::MAX.row_count()
        );
        Ok(())
    }

    #[test]
    fn complete_utxos_count_only_rows_at_or_above_the_start_height()
    -> Result<(), Box<dyn std::error::Error>> {
        // The row below the start height takes the answer over the cap, but
        // only the matching rows count toward it.
        let rows = rows_with_matching_count(TransparentAddressRowLimit::MAX.row_count());

        let outputs = decode_address_utxos(
            &rows,
            &lookup_of_address_one()?,
            BlockHeight::new(5),
            UtxoRowBound::Complete,
        )?;

        assert!(outputs.complete);
        assert_eq!(
            outputs.outputs.len(),
            TransparentAddressRowLimit::MAX.row_count()
        );
        Ok(())
    }

    #[test]
    fn utxos_refuse_a_row_for_an_address_the_lookup_does_not_hold()
    -> Result<(), TransparentAddressLookupError> {
        let mut foreign_row = utxo_row(5, 1);
        foreign_row.address = testnet_address(9).to_string();
        let rows = [utxo_row(5, 0), foreign_row];

        for bound in [
            UtxoRowBound::Complete,
            UtxoRowBound::Prefix(TransparentAddressRowLimit::MAX),
        ] {
            assert!(matches!(
                decode_address_utxos(&rows, &lookup_of_address_one()?, BlockHeight::new(0), bound),
                Err(SourceError::SourceProtocolMismatch { .. })
            ));
        }
        Ok(())
    }

    #[test]
    fn utxos_refuse_rows_out_of_chain_order() -> Result<(), TransparentAddressLookupError> {
        let rows = [utxo_row(5, 0), utxo_row(4, 0)];
        assert!(matches!(
            decode_address_utxos(
                &rows,
                &lookup_of_address_one()?,
                BlockHeight::new(0),
                UtxoRowBound::Complete,
            ),
            Err(SourceError::SourceProtocolMismatch { .. })
        ));
        Ok(())
    }

    #[test]
    fn utxos_refuse_a_script_that_does_not_pay_the_row_address()
    -> Result<(), TransparentAddressLookupError> {
        // Address 2 is in the lookup, so only the script check refuses a row
        // that names address 1 but pays address 2.
        let lookup = TransparentAddressLookup::new(
            Network::ZcashTestnet,
            [testnet_address(1), testnet_address(2)],
        )?;
        let script_hash_address =
            ZebraTransparentAddress::from_script_hash(ZebraNetworkKind::Testnet, [1; 20]);
        for script in [
            "6a".to_owned(),
            script_hex(testnet_address(2)),
            script_hex(script_hash_address),
        ] {
            let mut row = utxo_row(5, 0);
            row.script = script;
            assert_eq!(
                mismatch_reason(&decode_complete(&[row], &lookup)),
                SCRIPT_MISMATCH
            );
        }
        Ok(())
    }

    #[test]
    fn utxos_accept_a_script_hash_row_whose_script_pays_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let script_hash_address =
            ZebraTransparentAddress::from_script_hash(ZebraNetworkKind::Testnet, [7; 20]);
        let lookup = TransparentAddressLookup::new(Network::ZcashTestnet, [script_hash_address])?;
        let mut row = utxo_row(5, 0);
        row.address = script_hash_address.to_string();
        row.script = script_hex(script_hash_address);

        let outputs = decode_complete(&[row], &lookup)?;

        assert_eq!(
            outputs
                .iter()
                .map(|output| output.address)
                .collect::<Vec<_>>(),
            [script_hash_address]
        );
        Ok(())
    }

    #[test]
    fn utxos_validate_rows_below_the_start_height_and_past_the_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let lookup = lookup_of_address_one()?;
        let mut unpaid_row_below_start = utxo_row(3, 0);
        unpaid_row_below_start.script = "6a".to_owned();
        let mut foreign_row_past_prefix = utxo_row(9, 0);
        foreign_row_past_prefix.address = testnet_address(9).to_string();
        let max_rows = TransparentAddressRowLimit::new(1).ok_or("row limit")?;

        let below_start = decode_address_utxos(
            &[unpaid_row_below_start, utxo_row(5, 0)],
            &lookup,
            BlockHeight::new(5),
            UtxoRowBound::Complete,
        );
        let past_prefix = decode_address_utxos(
            &[utxo_row(5, 0), foreign_row_past_prefix],
            &lookup,
            BlockHeight::new(0),
            UtxoRowBound::Prefix(max_rows),
        );

        assert_eq!(mismatch_reason(&below_start), SCRIPT_MISMATCH);
        assert_eq!(
            mismatch_reason(&past_prefix),
            Some("getaddressutxos returned an output of an address the lookup does not hold")
        );
        Ok(())
    }

    #[test]
    fn utxos_refuse_repeated_outpoints_and_reversed_output_indexes()
    -> Result<(), TransparentAddressLookupError> {
        let lookup = lookup_of_address_one()?;
        let mut other_transaction = utxo_row(5, 0);
        other_transaction.txid = block_transaction_hex(1_000);
        let mut transaction_at_two_heights = utxo_row(6, 1);
        transaction_at_two_heights.txid = block_transaction_hex(5);
        let invalid_answers = [
            vec![utxo_row(5, 0), utxo_row(5, 0)],
            vec![utxo_row(5, 1), utxo_row(5, 0)],
            vec![utxo_row(5, 0), other_transaction, utxo_row(5, 1)],
            vec![utxo_row(5, 0), transaction_at_two_heights],
        ];

        for rows in invalid_answers {
            assert_eq!(
                mismatch_reason(&decode_complete(&rows, &lookup)),
                ORDER_MISMATCH
            );
        }
        Ok(())
    }
}
