//! Conformance suite for the engine-neutral serving readers.
//!
//! The cases fix how every [`CanonicalReader`] and [`WalletProjectionReader`]
//! answers, so each storage engine behind them serves the same wallet
//! responses. They are written only against those interfaces and against a
//! [`ReaderHarness`], which builds the stored chain a case describes and
//! opens readers at its current fence. A storage engine runs the suite by
//! implementing the harness and instantiating the cases with
//! [`canonical_reader_conformance`] and [`wallet_reader_conformance`]; the
//! canonical cases need no wallet reader.
//!
//! Chains come from the testkit's [`ChainFixture`] and [`CanonicalBaseline`],
//! which describe blocks, transactions, and transparent outputs without
//! naming an engine.
//!
//! The suite runs against the `RocksDB` readers and, for the canonical cases,
//! against a reader that keeps its chain in memory and builds every value
//! from that description, which shows a second engine can implement the
//! canonical interface.

use zinder_core::{BlockHeight, BlockId, NetworkUpgradeActivations};
use zinder_query::{CanonicalReader, WalletProjectionReader};
use zinder_testkit::{CanonicalBaseline, ChainFixture};

pub(crate) mod canonical;
mod in_memory;
mod rocksdb;
pub(crate) mod wallet;

/// Stores chains for one storage engine and opens canonical readers over
/// them.
///
/// The harness plays the canonical writer: it publishes a baseline, then
/// appends and replaces blocks. Each reader it opens holds the fence that was
/// current when it opened, however the harness moves on afterward.
pub(crate) trait ReaderHarness: Sized {
    /// Canonical reader of this engine.
    type Canonical: CanonicalReader;

    /// Publishes `baseline` as the first canonical fence, with
    /// `activations` as the writer-admitted network-upgrade schedule.
    fn publish(
        baseline: &CanonicalBaseline<'_>,
        activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self>;

    /// Appends the block of `chain` above the visible tip and publishes `settled_tip`.
    fn append_live_block(&mut self, chain: &ChainFixture, settled_tip: BlockId)
    -> eyre::Result<()>;

    /// Replaces the canonical blocks from `first_replaced_height` with the
    /// blocks of `replacement_chain` from that height through its tip.
    fn replace_live_blocks(
        &mut self,
        replacement_chain: &ChainFixture,
        first_replaced_height: BlockHeight,
    ) -> eyre::Result<()>;

    /// Drops retained chain events below `oldest_retained_sequence`.
    fn prune_chain_events_before(&mut self, oldest_retained_sequence: u64) -> eyre::Result<()>;

    /// Opens a canonical reader at the current fence.
    fn canonical_reader(&mut self) -> eyre::Result<Self::Canonical>;
}

/// A [`ReaderHarness`] that also projects the wallet store.
pub(crate) trait WalletReaderHarness: ReaderHarness {
    /// Wallet projection reader of this engine.
    type Wallet: WalletProjectionReader;

    /// Projects the wallet store at the current fence and opens a wallet
    /// reader over it.
    fn wallet_reader(&mut self) -> eyre::Result<Self::Wallet>;
}

/// Instantiates every canonical conformance case as a test against
/// `$harness`.
macro_rules! canonical_reader_conformance {
    ($harness:ty) => {
        $crate::integration::read_conformance::conformance_cases!(
            $harness,
            canonical,
            admitted_identity_matches_the_published_fence,
            single_block_reads_answer_retained_heights_and_absence,
            compact_block_ranges_answer_their_ends,
            byte_bounded_ranges_keep_whole_blocks_from_the_first_end,
            block_blob_reads_follow_raw_blob_retention,
            mined_transactions_are_found_or_proven_absent,
            transaction_bytes_name_the_missing_artifact,
            a_checkpointed_store_cannot_prove_an_absence,
            tree_states_are_stored_or_replayed,
            a_replay_reads_its_own_seed_not_one_built_outside_it,
            subtree_roots_answer_the_completed_range,
            a_subtree_root_below_retained_history_is_unavailable,
            chain_event_pages_are_bounded_and_resume_after_their_cursor,
            chain_event_starts_resolve_to_a_resume_position,
            the_settled_stream_commits_each_newly_settled_range,
            a_settled_cursor_resumes_with_the_default_family,
            pruned_and_malformed_chain_event_cursors_are_refused,
            a_reader_keeps_its_fence_while_the_writer_appends,
            a_reorg_replaces_blocks_only_for_readers_opened_after_it,
            a_reorg_is_served_as_a_visible_chain_event,
        );
    };
}

/// Instantiates every wallet conformance case as a test against
/// `$harness`.
macro_rules! wallet_reader_conformance {
    ($harness:ty) => {
        $crate::integration::read_conformance::conformance_cases!(
            $harness,
            wallet,
            wallet_source_matches_the_canonical_fence,
            unspent_output_pages_are_bounded_and_resume,
            unspent_output_pages_start_at_their_height,
            transaction_history_pages_stay_in_their_range,
            address_balances_sum_and_count_an_absent_address_as_zero,
            outpoint_spend_statuses_follow_the_request_order,
            a_wallet_reader_keeps_its_source_while_the_projection_advances,
            a_reorg_reverts_a_spend_only_for_readers_opened_after_it,
        );
    };
}

/// Declares one test per case of `$suite`, each calling the case with
/// `$harness`.
macro_rules! conformance_cases {
    ($harness:ty, $suite:ident, $($case:ident),+ $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $case() -> eyre::Result<()> {
                $crate::integration::read_conformance::$suite::$case::<$harness>().await
            }
        )+
    };
}

pub(crate) use {canonical_reader_conformance, conformance_cases, wallet_reader_conformance};
