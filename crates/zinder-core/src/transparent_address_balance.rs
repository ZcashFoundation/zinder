//! Confirmed transparent-address balance read-model.
//!
//! The balance is the canonical confirmed total across the requested
//! addresses, bound to one [`ChainEpoch`]. Live mempool state is not part of
//! this read-model.
//!
//! `address_count` records how many distinct transparent addresses
//! participated in the response so consumers can distinguish "zero balance
//! because none of these addresses received anything" from "this exact
//! address has zero outputs".

use crate::ChainEpoch;

/// Confirmed transparent-address balance bound to one chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentAddressBalance {
    /// Sum of unspent canonical outputs at the requested addresses.
    pub confirmed_zat: u64,
    /// Number of distinct transparent addresses included in this response.
    pub address_count: u32,
    /// Chain epoch the confirmed total was read at.
    pub chain_epoch: ChainEpoch,
}
