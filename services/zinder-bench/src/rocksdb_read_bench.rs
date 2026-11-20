//! Common admission, report provenance, and per-read attribution for READY secondaries.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use clap::Args;
use rust_rocksdb::{
    PerfMetric, PerfStatsLevel,
    perf::{set_perf_stats, with_thread_local},
    statistics::{Histogram, Ticker},
};
use serde::Serialize;
use zinder_bench::{BenchError, fixture::FixtureManifest};
use zinder_core::NetworkUpgradeActivations;
use zinder_query::RocksDbCanonicalReader;
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreReadyEvidence, CanonicalStoreWorkload, RawBlobRetention,
    RocksDbCanonicalSecondary, RocksDbResourceBudget, RocksDbStatisticsLevel,
};

use crate::build_provenance::BuildProvenance;

#[derive(Args)]
pub(crate) struct ReadStoreArgs {
    /// Directory containing the fixture manifest used to construct the store.
    #[arg(long)]
    fixture: PathBuf,
    /// Existing READY canonical primary store.
    #[arg(long)]
    canonical_store: PathBuf,
    /// Absent directory for disposable secondaries, outside the primary.
    #[arg(long)]
    secondary_root: PathBuf,
    /// Exact source revision of the measured binary.
    #[arg(long)]
    software_revision: String,
    /// Persisted canonical replacement depth.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    supported_reorg_depth: u32,
    /// Workload persisted in the canonical store.
    #[arg(long, default_value = "wallet", value_parser = parse_workload)]
    workload: CanonicalStoreWorkload,
    /// Raw-blob retention persisted in the canonical store.
    #[arg(long, default_value = "transactions", value_parser = crate::rocksdb_wallet_rebuild::parse_raw_blob_retention)]
    raw_blob_retention: RawBlobRetention,
    /// Collect engine statistics at off, tickers, or full (including histograms).
    #[arg(long, default_value = "tickers", value_parser = parse_statistics_level)]
    rocksdb_statistics_level: RocksDbStatisticsLevel,
    /// Write JSON to a new file outside the primary instead of stdout.
    #[arg(long)]
    pub(crate) report: Option<PathBuf>,
}

fn parse_workload(text: &str) -> Result<CanonicalStoreWorkload, String> {
    match text {
        "wallet" => Ok(CanonicalStoreWorkload::Wallet),
        "explorer" => Ok(CanonicalStoreWorkload::Explorer),
        _ => Err("expected wallet or explorer".to_owned()),
    }
}

fn parse_statistics_level(text: &str) -> Result<RocksDbStatisticsLevel, String> {
    RocksDbStatisticsLevel::parse(text).ok_or_else(|| "expected off, tickers, or full".to_owned())
}

pub(crate) struct ReadStore {
    pub(crate) primary: PathBuf,
    pub(crate) secondary_root: PathBuf,
    pub(crate) activations: NetworkUpgradeActivations,
    pub(crate) workload: CanonicalStoreWorkload,
    pub(crate) raw_blob_retention: RawBlobRetention,
    pub(crate) reorg_policy: CanonicalReorgPolicy,
    pub(crate) budget: RocksDbResourceBudget,
}

#[derive(Serialize)]
pub(crate) struct ReadEnvironment {
    software_revision: String,
    fixture_manifest_digest_sha256: String,
    canonical_store: PathBuf,
    build: BuildProvenance,
    host_cpu_model: Option<String>,
    statistics_level: &'static str,
    block_cache_bytes: u64,
}

impl ReadStoreArgs {
    pub(crate) fn prepare(&self) -> Result<(ReadStore, ReadEnvironment), BenchError> {
        if self.software_revision.trim().is_empty() {
            return Err(BenchError::invalid_argument(
                "--software-revision must not be empty",
            ));
        }
        let manifest = FixtureManifest::read(&self.fixture)?;
        let primary = fs::canonicalize(&self.canonical_store)
            .map_err(|source| BenchError::io(&self.canonical_store, source))?;
        let secondary_root = output_path(&self.secondary_root, &primary)?;
        if let Some(report) = &self.report {
            output_path(report, &primary)?;
        }
        let store = ReadStore {
            primary: primary.clone(),
            secondary_root,
            activations: manifest.activations_typed()?,
            workload: self.workload,
            raw_blob_retention: self.raw_blob_retention,
            reorg_policy: CanonicalReorgPolicy::new(self.supported_reorg_depth)?,
            budget: RocksDbResourceBudget {
                statistics_level: self.rocksdb_statistics_level,
                ..RocksDbResourceBudget::canonical_reader_defaults()
            },
        };
        let environment = ReadEnvironment {
            software_revision: self.software_revision.clone(),
            fixture_manifest_digest_sha256: manifest.digest_sha256()?,
            canonical_store: primary,
            build: BuildProvenance::of_this_binary(),
            host_cpu_model: fs::read_to_string("/proc/cpuinfo")
                .ok()
                .and_then(|cpuinfo| {
                    cpuinfo
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(key, _)| key.trim() == "model name")
                        .map(|(_, model)| model.trim().to_owned())
                }),
            statistics_level: self.rocksdb_statistics_level.as_str(),
            block_cache_bytes: store.budget.block_cache_bytes,
        };
        fs::create_dir(&store.secondary_root)
            .map_err(|source| BenchError::io(&store.secondary_root, source))?;
        Ok((store, environment))
    }
}

fn output_path(path: &Path, primary: &Path) -> Result<PathBuf, BenchError> {
    let name = path
        .file_name()
        .ok_or_else(|| BenchError::invalid_argument("output path lacks a file name"))?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let resolved = fs::canonicalize(parent)
        .map_err(|source| BenchError::io(parent, source))?
        .join(name);
    if resolved.starts_with(primary) || primary.starts_with(&resolved) {
        return Err(BenchError::invalid_argument(
            "secondary and report paths must be disjoint from --canonical-store",
        ));
    }
    Ok(resolved)
}

impl ReadStore {
    pub(crate) fn open(
        &self,
        name: &str,
        fence: Option<&CanonicalStoreReadyEvidence>,
    ) -> Result<(RocksDbCanonicalSecondary, f64), BenchError> {
        let start = Instant::now();
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &self.primary,
            self.secondary_root.join(name),
            &self.activations,
            self.workload,
            self.raw_blob_retention,
            self.reorg_policy,
            self.budget,
        )?;
        let seconds = start.elapsed().as_secs_f64();
        if fence.is_some_and(|expected| *expected != secondary.ready_evidence()) {
            return Err(BenchError::report_format(
                "canonical READY fence changed between measurements",
            ));
        }
        Ok((secondary, seconds))
    }
}

const TICKERS: [Ticker; 17] = [
    Ticker::NumberKeysRead,
    Ticker::NumberMultigetCalls,
    Ticker::NumberMultigetKeysRead,
    Ticker::NumberMultigetBytesRead,
    Ticker::NumberDbSeek,
    Ticker::NumberDbNext,
    Ticker::IterBytesRead,
    Ticker::BytesRead,
    Ticker::BlockCacheDataHit,
    Ticker::BlockCacheDataMiss,
    Ticker::BlockCacheIndexHit,
    Ticker::BlockCacheIndexMiss,
    Ticker::NoFileOpens,
    Ticker::NoFileErrors,
    Ticker::PrefetchBytes,
    Ticker::PrefetchBytesUseful,
    Ticker::ReadaheadTrimmed,
];

const HISTOGRAMS: [Histogram; 5] = [
    Histogram::DbMultiget,
    Histogram::DbGet,
    Histogram::ReadBlockGetMicros,
    Histogram::SstReadMicros,
    Histogram::TableOpenIoMicros,
];

const PERF_METRICS: [PerfMetric; 10] = [
    PerfMetric::BlockReadCount,
    PerfMetric::BlockReadByte,
    PerfMetric::BlockReadTime,
    PerfMetric::BlockCacheHitCount,
    PerfMetric::BlockDecompressTime,
    PerfMetric::MultigetReadBytes,
    PerfMetric::DataBlockReadByte,
    PerfMetric::IndexBlockReadByte,
    PerfMetric::FilterBlockReadByte,
    PerfMetric::GetReadBytes,
];

#[derive(Debug, Serialize)]
struct HistogramTotals {
    count: u64,
    sum_micros: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadCounters {
    ticker_deltas: Option<BTreeMap<&'static str, u64>>,
    histogram_deltas: Option<BTreeMap<&'static str, HistogramTotals>>,
    perf_context: BTreeMap<&'static str, u64>,
}

pub(crate) struct TimedRead<T> {
    pub(crate) rows: T,
    pub(crate) seconds: f64,
    pub(crate) counters: ReadCounters,
}

pub(crate) fn measure<T>(
    secondary: &RocksDbCanonicalSecondary,
    level: RocksDbStatisticsLevel,
    read: impl FnOnce() -> Result<T, BenchError>,
) -> Result<TimedRead<T>, BenchError> {
    let before_tickers = (level != RocksDbStatisticsLevel::Off)
        .then(|| TICKERS.map(|ticker| secondary.rocksdb_ticker_count(ticker)));
    let histogram_totals = |histogram| {
        let snapshot = secondary.rocksdb_histogram_data(histogram);
        HistogramTotals {
            count: snapshot.count(),
            sum_micros: snapshot.sum(),
        }
    };
    let before_histograms =
        (level == RocksDbStatisticsLevel::Full).then(|| HISTOGRAMS.map(histogram_totals));
    let (rows, seconds, perf_context) = with_thread_local(|context| {
        set_perf_stats(PerfStatsLevel::EnableTimeExceptForMutex);
        let start = Instant::now();
        let rows = read();
        let seconds = start.elapsed().as_secs_f64();
        set_perf_stats(PerfStatsLevel::Disable);
        let counters = PERF_METRICS
            .into_iter()
            .map(|metric| (metric.name(), context.metric(metric)))
            .collect();
        (rows, seconds, counters)
    });
    let ticker_deltas = before_tickers.map(|before| {
        TICKERS
            .into_iter()
            .zip(before)
            .map(|(ticker, count)| {
                (
                    ticker.name(),
                    secondary.rocksdb_ticker_count(ticker) - count,
                )
            })
            .collect()
    });
    let histogram_deltas = before_histograms.map(|before| {
        HISTOGRAMS
            .into_iter()
            .zip(before)
            .map(|(histogram, before)| {
                let after = histogram_totals(histogram);
                (
                    histogram.name(),
                    HistogramTotals {
                        count: after.count - before.count,
                        sum_micros: after.sum_micros - before.sum_micros,
                    },
                )
            })
            .collect()
    });
    Ok(TimedRead {
        rows: rows?,
        seconds,
        counters: ReadCounters {
            ticker_deltas,
            histogram_deltas,
            perf_context,
        },
    })
}

pub(crate) async fn measure_reader<T: Send + 'static>(
    reader: Arc<RocksDbCanonicalReader>,
    level: RocksDbStatisticsLevel,
    read: impl FnOnce(&RocksDbCanonicalReader) -> Result<T, zinder_query::StorageReadError>
    + Send
    + 'static,
) -> Result<TimedRead<T>, BenchError> {
    let submitted = Instant::now();
    let (sample, dispatch, finished) = tokio::task::spawn_blocking(move || {
        let dispatch = submitted.elapsed();
        let sample = measure(reader.secondary(), level, || {
            read(&reader).map_err(|error| BenchError::canonical_read(error.to_string()))
        });
        (sample, dispatch, Instant::now())
    })
    .await
    .map_err(|error| BenchError::canonical_read(error.to_string()))?;
    let mut sample = sample?;
    sample.seconds += (dispatch + finished.elapsed()).as_secs_f64();
    Ok(sample)
}

#[derive(Serialize)]
pub(crate) struct ReadyFenceSummary {
    first_retained_height: u32,
    visible_tip_height: u32,
    visible_tip_hash_hex: String,
    visible_epoch_id: u64,
    visible_event_sequence: u64,
    settled_tip_height: u32,
    settled_tip_hash_hex: String,
    visible_sequence_digest_sha256: String,
}

impl From<CanonicalStoreReadyEvidence> for ReadyFenceSummary {
    fn from(ready: CanonicalStoreReadyEvidence) -> Self {
        let settled = ready.sequence_checkpoint.through();
        Self {
            first_retained_height: ready.first_retained_block.height.value(),
            visible_tip_height: ready.visible_tip.height.value(),
            visible_tip_hash_hex: hex::encode(ready.visible_tip.hash.as_bytes()),
            visible_epoch_id: ready.visible_epoch.value(),
            visible_event_sequence: ready.visible_event_sequence,
            settled_tip_height: settled.height.value(),
            settled_tip_hash_hex: hex::encode(settled.hash.as_bytes()),
            visible_sequence_digest_sha256: hex::encode(ready.visible_sequence_digest),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use rust_rocksdb::{
        PerfMetric,
        statistics::{Histogram, Ticker},
    };
    use zinder_core::{BlockHeight, BlockHeightRange, Network};
    use zinder_query::{CanonicalReader, RocksDbCanonicalReader, read_on_blocking_thread};
    use zinder_store::{
        CanonicalStoreWorkload, RawBlobRetention, RocksDbResourceBudget, RocksDbStatisticsLevel,
    };
    use zinder_testkit::{CanonicalBaseline, CanonicalWriterFixture, ChainFixture};

    use super::{ReadCounters, ReadStore, measure, measure_reader, output_path};

    #[test]
    fn outputs_cannot_be_created_beneath_a_primary() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let primary = root.path().join("primary");
        fs::create_dir(&primary)?;
        let primary = fs::canonicalize(primary)?;
        assert!(output_path(&primary.join("secondary"), &primary).is_err());
        assert!(output_path(&primary.join("report.json"), &primary).is_err());
        assert!(output_path(&root.path().join("secondary"), &primary).is_ok());
        #[cfg(unix)]
        {
            let alias = root.path().join("alias");
            std::os::unix::fs::symlink(&primary, &alias)?;
            assert!(output_path(&alias.join("report.json"), &primary).is_err());
        }
        Ok(())
    }

    #[test]
    fn read_measurements_do_not_carry_counters_into_the_next_sample() -> eyre::Result<()> {
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(4);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let writer =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
        let scratch = tempfile::tempdir()?;
        let level = RocksDbStatisticsLevel::Full;
        let store = ReadStore {
            primary: writer.canonical_primary_path().to_path_buf(),
            secondary_root: scratch.path().to_owned(),
            activations,
            workload: CanonicalStoreWorkload::Wallet,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: writer.canonical_primary()?.reorg_policy(),
            budget: RocksDbResourceBudget {
                statistics_level: level,
                ..RocksDbResourceBudget::canonical_reader_defaults()
            },
        };
        let range = BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(3));
        let (direct, _) = store.open("direct", None)?;
        let direct_read = measure(
            &direct,
            level,
            || Ok(direct.compact_blocks_in_range(range)?),
        )?;
        assert_eq!(direct_read.rows.len(), 2);
        assert_multiget_activity(&direct_read.counters);
        assert_no_read_activity(&measure(&direct, level, || Ok(()))?.counters)?;

        let (secondary, _) = store.open("reader", Some(&direct.ready_evidence()))?;
        let reader = Arc::new(RocksDbCanonicalReader::new(secondary)?);
        tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()?
            .block_on(async {
                let reader_read = measure_reader(Arc::clone(&reader), level, move |reader| {
                    let rows = read_on_blocking_thread(reader.compact_blocks_in_range(range))?;
                    Ok((rows, std::thread::current().id()))
                })
                .await?;
                let idle =
                    measure_reader(reader, level, |_| Ok(std::thread::current().id())).await?;
                assert_eq!(reader_read.rows, (direct_read.rows, idle.rows));
                assert_multiget_activity(&reader_read.counters);
                assert_no_read_activity(&idle.counters)
            })
    }

    fn assert_multiget_activity(counters: &ReadCounters) {
        assert_eq!(
            counters
                .ticker_deltas
                .as_ref()
                .and_then(|deltas| deltas.get(Ticker::NumberMultigetCalls.name())),
            Some(&1)
        );
        assert_eq!(
            counters
                .histogram_deltas
                .as_ref()
                .and_then(|deltas| deltas.get(Histogram::DbMultiget.name()))
                .map(|totals| totals.count),
            Some(1)
        );
        assert!(counters.perf_context[PerfMetric::MultigetReadBytes.name()] > 0);
    }

    fn assert_no_read_activity(counters: &ReadCounters) -> eyre::Result<()> {
        let tickers = counters
            .ticker_deltas
            .as_ref()
            .ok_or_else(|| eyre::eyre!("full statistics must report ticker deltas"))?;
        let histograms = counters
            .histogram_deltas
            .as_ref()
            .ok_or_else(|| eyre::eyre!("full statistics must report histogram deltas"))?;
        assert_eq!(tickers.len(), super::TICKERS.len());
        assert_eq!(histograms.len(), super::HISTOGRAMS.len());
        assert_eq!(counters.perf_context.len(), super::PERF_METRICS.len());
        assert!(tickers.values().all(|delta| *delta == 0), "{tickers:?}");
        assert!(
            histograms
                .values()
                .all(|totals| totals.count == 0 && totals.sum_micros == 0),
            "{histograms:?}"
        );
        assert!(
            counters.perf_context.values().all(|delta| *delta == 0),
            "{:?}",
            counters.perf_context
        );
        Ok(())
    }
}
