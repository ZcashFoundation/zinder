//! Correctness: combined wallet digest equals a projector-path reference.

use std::path::PathBuf;

use clap::Args;
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_store::RocksDbResourceBudget;
use zinder_wallet_rocksdb::RocksDbWalletStore;

use super::{
    CommonArgs, NETWORK_LABEL_TESTNET,
    layout::{Layout, absolute, open_canonical, wallet_family_budget},
    replay::{FenceView, WalletView},
};

const COLD_READBACK_UNDO_BYTES: u64 = 512 * 1024 * 1024;

/// Verify inputs.
#[derive(Args)]
pub(crate) struct VerifyArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Combined database after the replay (and optional reorg) arms.
    #[arg(long)]
    combined: PathBuf,
    /// Reference wallet store built by `rocksdb-wallet-rebuild` (the
    /// projector's `build_wallet_from_canonical`) over the same range.
    #[arg(long)]
    reference_wallet: PathBuf,
    /// Optional canonical-only store at the same tip, to compare canonical fences.
    #[arg(long)]
    baseline: Option<PathBuf>,
}

/// Verify report.
#[derive(Serialize)]
pub(crate) struct VerifyReport {
    pub(crate) spike: &'static str,
    pub(crate) action: &'static str,
    pub(crate) network_label: &'static str,
    pub(crate) label: Option<String>,
    pub(crate) combined: String,
    pub(crate) reference_wallet: String,
    pub(crate) combined_fence: FenceView,
    pub(crate) combined_wallet: WalletView,
    pub(crate) reference: WalletView,
    /// Rows of the combined wallet re-read cold; digest, counts, and UTXO
    /// summary equal the combined READY evidence.
    pub(crate) combined_rows_match_evidence: bool,
    pub(crate) combined_cold_readback_seconds: f64,
    pub(crate) digest_equal: bool,
    pub(crate) row_counts_equal: bool,
    pub(crate) utxo_summary_equal: bool,
    pub(crate) source_tip_equal: bool,
    pub(crate) settled_tip_equal: bool,
    pub(crate) source_sequence_digest_equal: bool,
    pub(crate) baseline_fence: Option<FenceView>,
    /// Canonical tip and block-facts sequence digest equal in both databases.
    pub(crate) baseline_canonical_sequence_equal: Option<bool>,
}

/// Runs the digest comparison.
pub(crate) fn run_verify(args: VerifyArgs) -> Result<VerifyReport, BenchError> {
    let network = args.common.network()?;
    let (_manifest, activations) = args.common.manifest_and_activations()?;

    let reference = RocksDbWalletStore::open_ready_for_following(
        &args.reference_wallet,
        network,
        RocksDbResourceBudget::wallet_projection_reader_defaults(),
    )?;
    let reference_evidence = reference.ready_evidence().clone();
    drop(reference);

    let mut baseline_fence = None;
    if let Some(baseline) = args.baseline.as_ref() {
        Layout {
            wallet: false,
            mirrors: false,
        }
        .register(wallet_family_budget());
        let store = open_canonical(
            baseline,
            &activations,
            args.common.reorg_window_blocks,
            RocksDbResourceBudget::canonical_reader_defaults(),
        )?;
        baseline_fence = Some(store.event_fence());
    }

    let existing = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), &args.combined)
        .map_err(|source| super::spike_error("list column families", source))?;
    Layout {
        wallet: true,
        mirrors: existing.iter().any(|name| name.starts_with("mirror_")),
    }
    .register(wallet_family_budget());
    let store = open_canonical(
        &args.combined,
        &activations,
        args.common.reorg_window_blocks,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?;
    let combined_fence = store.event_fence();
    let wallet = RocksDbWalletStore::spike_open_shared(store.spike_bounded_open().clone(), network)?;
    let (combined_rows_match_evidence, seconds) =
        match wallet.spike_cold_readback(COLD_READBACK_UNDO_BYTES) {
            Ok(readback) => (true, readback.elapsed.as_secs_f64()),
            Err(error) => {
                tracing::error!(
                    target: "zinder::bench::spike",
                    event = "spike_cold_readback_failed",
                    error = %error,
                    "combined wallet rows differ from their READY evidence"
                );
                (false, 0.0)
            }
        };
    let combined = wallet.ready_evidence().clone();
    let baseline_equal = baseline_fence.map(|fence| {
        fence.visible_tip() == combined_fence.visible_tip()
            && fence.sequence_digest() == combined_fence.sequence_digest()
    });
    Ok(VerifyReport {
        spike: "A-combined-commit",
        action: "verify",
        network_label: NETWORK_LABEL_TESTNET,
        label: args.common.label.clone(),
        combined: absolute(&args.combined).display().to_string(),
        reference_wallet: absolute(&args.reference_wallet).display().to_string(),
        combined_fence: FenceView::from(combined_fence),
        combined_wallet: WalletView::from(&combined),
        reference: WalletView::from(&reference_evidence),
        combined_rows_match_evidence,
        combined_cold_readback_seconds: seconds,
        digest_equal: combined.projection_digest == reference_evidence.projection_digest,
        row_counts_equal: combined.row_counts == reference_evidence.row_counts,
        utxo_summary_equal: combined.utxo_summary == reference_evidence.utxo_summary,
        source_tip_equal: combined.source_position.tip == reference_evidence.source_position.tip,
        settled_tip_equal: combined.settled_tip == reference_evidence.settled_tip,
        source_sequence_digest_equal: combined.source_sequence_digest
            == reference_evidence.source_sequence_digest,
        baseline_fence: baseline_fence.map(FenceView::from),
        baseline_canonical_sequence_equal: baseline_equal,
    })
}
