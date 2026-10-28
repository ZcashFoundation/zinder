//! `REORG_WINDOW_EXCEEDED` error-metadata codec.
//!
//! A server refusing traffic because its canonical writer parked on a reorg
//! deeper than its window carries the halt in `ErrorInfo.metadata` as
//! decimal block counts. A server that predates a key omits it, so every
//! decoded count is optional.

use std::{collections::HashMap, hash::BuildHasher};

use crate::v1::ops::ReorgWindowExceededDetail;

const REPLACEMENT_DEPTH_BLOCKS_KEY: &str = "replacement_depth_blocks";
const REORG_WINDOW_BLOCKS_KEY: &str = "reorg_window_blocks";
const COVERED_DEPTH_BLOCKS_KEY: &str = "covered_depth_blocks";

/// Halt block counts decoded from a `REORG_WINDOW_EXCEEDED` status.
///
/// Each count is `None` when the server omitted it or sent a value that is
/// not a decimal `u64`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReorgWindowExceededMetadata {
    /// Blocks the refused replacement would have replaced.
    pub replacement_depth_blocks: Option<u64>,
    /// The reorg window in blocks.
    pub reorg_window_blocks: Option<u64>,
    /// Replaceable depth the window covered when the replacement was refused.
    pub covered_depth_blocks: Option<u64>,
}

/// Returns the `ErrorInfo.metadata` entries that carry `halt` on a
/// `REORG_WINDOW_EXCEEDED` status.
///
/// The covered depth is present only when `halt` reports it.
pub fn encode_reorg_window_exceeded_metadata(
    halt: ReorgWindowExceededDetail,
) -> impl Iterator<Item = (String, String)> {
    [
        Some((REPLACEMENT_DEPTH_BLOCKS_KEY, halt.depth)),
        Some((REORG_WINDOW_BLOCKS_KEY, halt.configured)),
        halt.covered
            .map(|covered| (COVERED_DEPTH_BLOCKS_KEY, covered)),
    ]
    .into_iter()
    .flatten()
    .map(|(key, blocks)| (key.to_owned(), blocks.to_string()))
}

/// Decodes the halt block counts from a `REORG_WINDOW_EXCEEDED` status's
/// `ErrorInfo.metadata`.
#[must_use]
pub fn decode_reorg_window_exceeded_metadata<Hasher: BuildHasher>(
    metadata: &HashMap<String, String, Hasher>,
) -> ReorgWindowExceededMetadata {
    let blocks = |key: &str| metadata.get(key).and_then(|blocks| blocks.parse().ok());
    ReorgWindowExceededMetadata {
        replacement_depth_blocks: blocks(REPLACEMENT_DEPTH_BLOCKS_KEY),
        reorg_window_blocks: blocks(REORG_WINDOW_BLOCKS_KEY),
        covered_depth_blocks: blocks(COVERED_DEPTH_BLOCKS_KEY),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn halt_metadata_round_trips_through_error_info() {
        let metadata: HashMap<String, String> =
            encode_reorg_window_exceeded_metadata(ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(100),
            })
            .collect();
        assert_eq!(
            metadata.get("replacement_depth_blocks").map(String::as_str),
            Some("1001")
        );
        assert_eq!(
            metadata.get("reorg_window_blocks").map(String::as_str),
            Some("1000")
        );
        assert_eq!(
            metadata.get("covered_depth_blocks").map(String::as_str),
            Some("100")
        );
        assert_eq!(
            decode_reorg_window_exceeded_metadata(&metadata),
            ReorgWindowExceededMetadata {
                replacement_depth_blocks: Some(1_001),
                reorg_window_blocks: Some(1_000),
                covered_depth_blocks: Some(100),
            }
        );
    }

    #[test]
    fn halt_metadata_omits_an_unreported_covered_depth() {
        let metadata: HashMap<String, String> =
            encode_reorg_window_exceeded_metadata(ReorgWindowExceededDetail {
                depth: 12,
                configured: 10,
                covered: None,
            })
            .collect();
        assert_eq!(metadata.len(), 2);
        assert_eq!(
            decode_reorg_window_exceeded_metadata(&metadata).covered_depth_blocks,
            None
        );
    }

    #[test]
    fn missing_or_malformed_counts_decode_as_absent() {
        let metadata = HashMap::from([("reorg_window_blocks".to_owned(), "many".to_owned())]);
        assert_eq!(
            decode_reorg_window_exceeded_metadata(&metadata),
            ReorgWindowExceededMetadata::default()
        );
    }
}
