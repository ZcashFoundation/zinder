//! Wallet transaction ID suffixes applied to a single mempool snapshot.

use zinder_core::{TransactionId, wire::encode_internal_transaction_id};

pub(crate) struct MempoolExclusions {
    suffixes: Vec<Vec<u8>>,
    matches: Vec<u8>,
}

impl MempoolExclusions {
    pub(crate) fn new(mut suffixes: Vec<Vec<u8>>) -> Self {
        suffixes.sort_unstable_by(|left, right| left.iter().rev().cmp(right.iter().rev()));
        // Go's sorted filter uses the shortest matching suffix. Removing
        // covered suffixes makes the remaining match ranges disjoint.
        suffixes.dedup_by(|next, previous| next.ends_with(previous));
        let matches = vec![0; suffixes.len()];
        Self { suffixes, matches }
    }

    pub(crate) fn observe(&mut self, transaction_id: TransactionId) {
        if let Some(index) = self.matching_suffix(&encode_internal_transaction_id(transaction_id)) {
            self.matches[index] = self.matches[index].saturating_add(1);
        }
    }

    pub(crate) fn uniquely_matches(&self, txid: &[u8]) -> bool {
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
    fn empty_suffix_matches_the_whole_snapshot() {
        let first = TransactionId::from_bytes([1; 32]);
        let mut exclusions = MempoolExclusions::new(vec![vec![1], Vec::new()]);
        exclusions.observe(first);
        assert!(exclusions.uniquely_matches(&[1; 32]));
        exclusions.observe(TransactionId::from_bytes([2; 32]));
        assert!(!exclusions.uniquely_matches(&[1; 32]));
        assert!(!exclusions.uniquely_matches(&[2; 32]));
    }
}
