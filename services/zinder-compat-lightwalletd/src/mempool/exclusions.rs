//! Wallet transaction ID suffixes applied to a single mempool snapshot.

use thiserror::Error;
use zinder_core::{TransactionId, wire::encode_internal_transaction_id};

/// Maximum number of transaction-id suffixes accepted by one `GetMempoolTx` request.
pub(crate) const MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST: usize = 20_000;

/// An invalid `GetMempoolTx` exclusion filter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MempoolExclusionsError {
    /// The request supplies too many suffixes.
    #[error(
        "exclude_txid_suffixes contains {count} entries; at most {MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST} are allowed"
    )]
    TooManySuffixes {
        /// Number of suffixes in the request before normalization.
        count: usize,
    },
    /// A suffix is longer than a transaction ID.
    #[error("exclude txid {index} is larger than 32 bytes")]
    SuffixTooLong {
        /// Zero-based position of the invalid suffix.
        index: usize,
    },
}

/// Transaction ID exclusions for a single mempool snapshot.
///
/// Observe every transaction in the snapshot before testing for unique
/// matches, including transactions outside the requested pools.
pub struct MempoolExclusions {
    suffixes: Vec<Vec<u8>>,
    matches: Vec<u8>,
}

impl MempoolExclusions {
    /// Validates and normalizes the internal-byte-order suffixes in a
    /// `GetMempoolTx` request. An empty suffix matches every transaction ID.
    pub fn from_suffixes(mut suffixes: Vec<Vec<u8>>) -> Result<Self, MempoolExclusionsError> {
        if suffixes.len() > MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST {
            return Err(MempoolExclusionsError::TooManySuffixes {
                count: suffixes.len(),
            });
        }
        if let Some(index) = suffixes.iter().position(|suffix| suffix.len() > 32) {
            return Err(MempoolExclusionsError::SuffixTooLong { index });
        }
        suffixes.sort_unstable_by(|left, right| left.iter().rev().cmp(right.iter().rev()));
        // Go's sorted filter uses the shortest matching suffix. Removing
        // covered suffixes makes the remaining match ranges disjoint.
        suffixes.dedup_by(|next, previous| next.ends_with(previous));
        let matches = vec![0; suffixes.len()];
        Ok(Self { suffixes, matches })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.suffixes.is_empty()
    }

    /// Records one transaction in the mempool snapshot.
    pub fn observe(&mut self, transaction_id: TransactionId) {
        if let Some(index) = self.matching_suffix(&encode_internal_transaction_id(transaction_id)) {
            self.matches[index] = self.matches[index].saturating_add(1);
        }
    }

    /// Reports whether the internal-byte-order transaction ID matches a
    /// suffix that matched exactly one observed transaction.
    #[must_use]
    pub fn uniquely_matches(&self, txid: &[u8]) -> bool {
        self.matching_suffix(txid)
            .is_some_and(|index| self.matches[index] == 1)
    }

    fn matching_suffix(&self, txid: &[u8]) -> Option<usize> {
        self.suffixes
            .binary_search_by(|suffix| {
                suffix
                    .iter()
                    .rev()
                    .cmp(txid.iter().rev().take(suffix.len()))
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_suffix_matches_the_whole_snapshot() -> eyre::Result<()> {
        let first = TransactionId::from_bytes([1; 32]);
        let mut exclusions = MempoolExclusions::from_suffixes(vec![vec![1], Vec::new()])?;
        exclusions.observe(first);
        assert!(exclusions.uniquely_matches(&[1; 32]));
        exclusions.observe(TransactionId::from_bytes([2; 32]));
        assert!(!exclusions.uniquely_matches(&[1; 32]));
        assert!(!exclusions.uniquely_matches(&[2; 32]));
        Ok(())
    }
}
