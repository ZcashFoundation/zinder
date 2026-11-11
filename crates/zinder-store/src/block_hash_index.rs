//! Best-chain block-hash lookup outcome.

use zinder_core::BlockId;

/// Outcome of a hash-to-height lookup against the canonical best chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockHashLookup {
    /// The hash addresses a block currently visible in the best chain.
    Resolved(BlockId),
    /// The indexed height does not contain this hash in the visible best chain.
    NotInBestChain,
    /// No entry is present for this hash in the reader's canonical index.
    NotIndexed,
}
