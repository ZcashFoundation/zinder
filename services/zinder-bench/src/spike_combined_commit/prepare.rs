//! Golden H0 stores: canonical-only, and canonical plus wallet in one database.
//!
//! Inputs are a READY wallet-serving canonical store at H0 and a READY wallet
//! store built from it by the projector path (`build_wallet_from_canonical`,
//! for example through `rocksdb-wallet-rebuild`). Neither input is opened or
//! written: both are copied first (SST files hard-linked). The combined
//! golden store receives the six wallet families by external SST ingestion
//! and the wallet control record in its own column family, then is admitted
//! through the canonical READY admission and a cold wallet row readback.

use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use clap::Args;
use rust_rocksdb::{
    DB, FlushOptions, IngestExternalFileOptions, IteratorMode, Options, ReadOptions,
    SstFileWriter, WriteOptions,
};
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_wallet_projection::WALLET_STORE_CONTROL_KEY;
use zinder_wallet_rocksdb::{
    RocksDbWalletStore, SPIKE_WALLET_CONTROL_COLUMN_FAMILY, spike_wallet_column_family_names,
    spike_wallet_data_column_family_names,
};

use super::{
    CommonArgs, NETWORK_LABEL_TESTNET,
    layout::{
        BudgetView, CopyStats, Layout, absolute, copy_closed_store, directory_bytes,
        ensure_column_families, open_canonical, wallet_family_budget, writer_budget,
    },
    replay::{FenceView, WalletView},
    spike_error,
};

const SST_TARGET_BYTES: u64 = 256 * 1024 * 1024;
const COLD_READBACK_UNDO_BYTES: u64 = 512 * 1024 * 1024;

/// Prepare inputs.
#[derive(Args)]
pub(crate) struct PrepareArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// READY wallet-serving canonical store at H0 (read only through a copy).
    #[arg(long)]
    canonical_h0: PathBuf,
    /// READY wallet store at H0 built from `--canonical-h0` (read only through a copy).
    #[arg(long)]
    wallet_h0: PathBuf,
    /// Output directory; receives `canonical-only.golden` and `combined.golden`.
    #[arg(long)]
    out_dir: PathBuf,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct FamilyImport {
    pub(crate) family: String,
    pub(crate) rows: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) sst_files: u64,
}

/// Prepare report.
#[derive(Serialize)]
pub(crate) struct PrepareReport {
    pub(crate) spike: &'static str,
    pub(crate) action: &'static str,
    pub(crate) network_label: &'static str,
    pub(crate) label: Option<String>,
    pub(crate) canonical_h0_source: String,
    pub(crate) wallet_h0_source: String,
    pub(crate) canonical_only_golden: String,
    pub(crate) combined_golden: String,
    pub(crate) canonical_copy: CopyStats,
    pub(crate) combined_copy: CopyStats,
    pub(crate) wallet_copy: CopyStats,
    pub(crate) wallet_import: Vec<FamilyImport>,
    pub(crate) wallet_import_seconds: f64,
    pub(crate) h0_fence: FenceView,
    pub(crate) combined_h0_fence: FenceView,
    pub(crate) wallet_h0: WalletView,
    pub(crate) wallet_source_matches_canonical_fence: bool,
    pub(crate) combined_rows_match_evidence: bool,
    pub(crate) cold_readback_seconds: f64,
    pub(crate) canonical_only_bytes: u64,
    pub(crate) combined_bytes: u64,
    pub(crate) combined_writer_budget: BudgetView,
}

/// Runs prepare.
pub(crate) async fn run_prepare(args: PrepareArgs) -> Result<PrepareReport, BenchError> {
    let network = args.common.network()?;
    let (_manifest, activations) = args.common.manifest_and_activations()?;
    std::fs::create_dir_all(&args.out_dir).map_err(|source| BenchError::io(&args.out_dir, source))?;
    let canonical_golden = args.out_dir.join("canonical-only.golden");
    let combined_golden = args.out_dir.join("combined.golden");
    let wallet_copy_path = args.out_dir.join("wallet-h0.import-source");
    let sst_dir = args.out_dir.join("wallet-import-sst");

    // Canonical-only golden: an exact copy, admitted with the plain layout.
    let canonical_copy = copy_closed_store(&args.canonical_h0, &canonical_golden)?;
    Layout {
        wallet: false,
        mirrors: false,
    }
    .register(wallet_family_budget());
    let canonical_only = open_canonical(
        &canonical_golden,
        &activations,
        args.common.reorg_window_blocks,
        zinder_store::RocksDbResourceBudget::canonical_writer_defaults(),
    )?;
    let h0_fence = canonical_only.event_fence();
    drop(canonical_only);

    // Combined golden: canonical copy plus imported wallet families.
    let combined_copy = copy_closed_store(&args.canonical_h0, &combined_golden)?;
    let wallet_copy = copy_closed_store(&args.wallet_h0, &wallet_copy_path)?;
    ensure_column_families(&combined_golden, &spike_wallet_column_family_names())?;
    let import_started = Instant::now();
    let wallet_import = import_wallet_families(&wallet_copy_path, &combined_golden, &sst_dir)?;
    let wallet_import_seconds = import_started.elapsed().as_secs_f64();
    std::fs::remove_dir_all(&wallet_copy_path)
        .map_err(|source| BenchError::io(&wallet_copy_path, source))?;
    let _ = std::fs::remove_dir_all(&sst_dir);

    let layout = Layout {
        wallet: true,
        mirrors: false,
    };
    layout.register(wallet_family_budget());
    let budget = writer_budget(
        true,
        args.common.budget_profile,
        args.common.max_background_jobs,
        args.common.statistics_level,
    );
    let combined = open_canonical(
        &combined_golden,
        &activations,
        args.common.reorg_window_blocks,
        budget,
    )?;
    let combined_h0_fence = combined.event_fence();
    let wallet = RocksDbWalletStore::spike_open_shared(combined.spike_bounded_open().clone(), network)?;
    let evidence = wallet.ready_evidence().clone();
    let wallet_source_matches_canonical_fence = evidence.source_position.tip
        == combined_h0_fence.visible_tip()
        && evidence.source_position.chain_epoch_id == combined_h0_fence.chain_epoch_id()
        && evidence.source_position.event_sequence == combined_h0_fence.chain_event_sequence()
        && evidence.source_sequence_digest == combined_h0_fence.sequence_digest();
    let readback = wallet.spike_cold_readback(COLD_READBACK_UNDO_BYTES)?;
    drop(wallet);
    drop(combined);
    if !wallet_source_matches_canonical_fence {
        return Err(BenchError::spike(
            "imported wallet source does not equal the canonical H0 fence",
        ));
    }

    Ok(PrepareReport {
        spike: "A-combined-commit",
        action: "prepare",
        network_label: NETWORK_LABEL_TESTNET,
        label: args.common.label.clone(),
        canonical_h0_source: absolute(&args.canonical_h0).display().to_string(),
        wallet_h0_source: absolute(&args.wallet_h0).display().to_string(),
        canonical_only_golden: absolute(&canonical_golden).display().to_string(),
        combined_golden: absolute(&combined_golden).display().to_string(),
        canonical_copy,
        combined_copy,
        wallet_copy,
        wallet_import,
        wallet_import_seconds,
        h0_fence: FenceView::from(h0_fence),
        combined_h0_fence: FenceView::from(combined_h0_fence),
        wallet_h0: WalletView::from(&evidence),
        wallet_source_matches_canonical_fence,
        combined_rows_match_evidence: true,
        cold_readback_seconds: readback.elapsed.as_secs_f64(),
        canonical_only_bytes: directory_bytes(&canonical_golden),
        combined_bytes: directory_bytes(&combined_golden),
        combined_writer_budget: BudgetView::from(budget),
    })
}

fn import_wallet_families(
    wallet_source: &Path,
    combined: &Path,
    sst_dir: &Path,
) -> Result<Vec<FamilyImport>, BenchError> {
    std::fs::create_dir_all(sst_dir).map_err(|source| BenchError::io(sst_dir, source))?;
    let source_families = DB::list_cf(&Options::default(), wallet_source)
        .map_err(|source| spike_error("list wallet families", source))?;
    let source_db = DB::open_cf_for_read_only(&Options::default(), wallet_source, &source_families, false)
        .map_err(|source| spike_error("open wallet copy read-only", source))?;
    let combined_families = DB::list_cf(&Options::default(), combined)
        .map_err(|source| spike_error("list combined families", source))?;
    let combined_db = DB::open_cf(&Options::default(), combined, &combined_families)
        .map_err(|source| spike_error("open combined for import", source))?;

    let mut imports = Vec::new();
    for family in spike_wallet_data_column_family_names() {
        let source_handle = source_db
            .cf_handle(family)
            .ok_or_else(|| BenchError::spike(format!("wallet copy lacks {family}")))?;
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(false);
        read_options.set_readahead_size(4 * 1024 * 1024);
        let mut import = FamilyImport {
            family: family.to_owned(),
            ..FamilyImport::default()
        };
        let sst_options = Options::default();
        let mut files = Vec::new();
        let mut writer: Option<(SstFileWriter<'_>, PathBuf, u64)> = None;
        for row in source_db.iterator_cf_opt(&source_handle, read_options, IteratorMode::Start) {
            let (key, value) = row.map_err(|source| spike_error("wallet copy scan", source))?;
            if writer.is_none() {
                let path = sst_dir.join(format!("{family}-{:06}.sst", files.len()));
                let sst = SstFileWriter::create(&sst_options);
                sst.open(&path)
                    .map_err(|source| spike_error("open import sst", source))?;
                writer = Some((sst, path, 0));
            }
            if let Some((sst, _, bytes)) = writer.as_mut() {
                sst.put(&key, &value)
                    .map_err(|source| spike_error("write import sst", source))?;
                *bytes += (key.len() + value.len()) as u64;
                import.rows += 1;
                import.logical_bytes += (key.len() + value.len()) as u64;
                if *bytes >= SST_TARGET_BYTES {
                    let (mut sst, path, _) = writer.take().ok_or_else(|| BenchError::spike("sst lost"))?;
                    sst.finish()
                        .map_err(|source| spike_error("finish import sst", source))?;
                    files.push(path);
                }
            }
        }
        if let Some((mut sst, path, _)) = writer.take() {
            sst.finish()
                .map_err(|source| spike_error("finish import sst", source))?;
            files.push(path);
        }
        import.sst_files = files.len() as u64;
        if !files.is_empty() {
            let target = combined_db
                .cf_handle(family)
                .ok_or_else(|| BenchError::spike(format!("combined lacks {family}")))?;
            let mut ingest = IngestExternalFileOptions::default();
            ingest.set_move_files(true);
            combined_db
                .ingest_external_file_cf_opts(&target, &ingest, files)
                .map_err(|source| spike_error("ingest wallet sst", source))?;
        }
        imports.push(import);
    }

    let control = source_db
        .get(WALLET_STORE_CONTROL_KEY)
        .map_err(|source| spike_error("read wallet control", source))?
        .ok_or_else(|| BenchError::spike("wallet copy has no control record"))?;
    let control_family = combined_db
        .cf_handle(SPIKE_WALLET_CONTROL_COLUMN_FAMILY)
        .ok_or_else(|| BenchError::spike("combined lacks the wallet control family"))?;
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
    combined_db
        .put_cf_opt(&control_family, WALLET_STORE_CONTROL_KEY, &control, &write_options)
        .map_err(|source| spike_error("write wallet control", source))?;
    let mut flush = FlushOptions::default();
    flush.set_wait(true);
    combined_db
        .flush_cf_opt(&control_family, &flush)
        .map_err(|source| spike_error("flush wallet control", source))?;
    drop(control_family);
    drop(combined_db);
    drop(source_db);
    Ok(imports)
}
