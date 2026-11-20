//! Tree-state and admission counters read from a Zinder server's metrics.
//!
//! The client sees which heights it asked for, but only the server knows
//! whether each tree state came from a stored checkpoint or a replay: a store
//! that a checkpoint pass densified answers every settled height directly. A
//! run given the server's metrics endpoint reports the server's own counts
//! over the run instead of inferring them from the 100-block stride.
//!
//! The same reads show how full the public listener's receive pools got,
//! which requests they refused or evicted, and whether a proxy in front of
//! the server named each client. The slots each pool holds are also read
//! every second during the run, so their mean covers the whole run.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tokio_util::task::AbortOnDropHandle;
use zinder_bench::metrics_scrape::{MetricSample, parse_prometheus_samples, sum_by_name};

const TREE_STATE_REPLAYED_BLOCKS_COUNT: &str = "zinder_query_tree_state_replayed_blocks_count";
const TREE_STATE_REPLAYED_BLOCKS: &str = "zinder_query_tree_state_replayed_blocks_sum";
const TREE_STATE_READS: &str = "zinder_query_tree_state_reads_total";
const TREE_STATE_REPLAY_ADMISSIONS: &str = "zinder_query_tree_state_replay_admissions_total";
const TREE_STATE_REPLAY_REFUSALS: &str = "zinder_query_tree_state_replay_refusals_total";
const TREE_STATE_REPLAY_WAIT_SUM: &str = "zinder_query_tree_state_replay_wait_duration_seconds_sum";
const TREE_STATE_REPLAY_WAIT_COUNT: &str =
    "zinder_query_tree_state_replay_wait_duration_seconds_count";
const BLOCK_RANGE_ADMISSIONS: &str = "zinder_compat_block_range_streams_admitted_total";
const BLOCK_RANGE_REFUSALS: &str = "zinder_compat_block_range_stream_refusals_total";
const BLOCK_RANGE_SLOT_WAIT_SUM: &str = "zinder_compat_block_range_slot_wait_duration_seconds_sum";
const BLOCK_RANGE_SLOT_WAIT_COUNT: &str =
    "zinder_compat_block_range_slot_wait_duration_seconds_count";
const BLOCK_RANGE_STAGE_SUM: &str = "zinder_compat_block_range_stage_duration_seconds_sum";
const BLOCK_RANGE_STAGE_COUNT: &str = "zinder_compat_block_range_stage_duration_seconds_count";
const RECEIVE_REFUSALS: &str = "zinder_grpc_request_receive_refusals_total";
const RECEIVE_EVICTIONS: &str = "zinder_grpc_request_receive_evictions_total";
const RECEIVE_SLOTS_HELD: &str = "zinder_grpc_request_receive_slots_held";
const RECEIVE_SLOTS_HELD_COUNT: &str = "zinder_grpc_request_receive_slots_held_at_admission_count";
const RECEIVE_SLOTS_HELD_SUM: &str = "zinder_grpc_request_receive_slots_held_at_admission_sum";
const RECEIVE_SLOTS_HELD_BUCKET: &str =
    "zinder_grpc_request_receive_slots_held_at_admission_bucket";
const RECEIVE_SLOT_HOLD_SUM: &str = "zinder_grpc_request_receive_slot_hold_duration_seconds_sum";
const RECEIVE_SLOT_HOLD_COUNT: &str =
    "zinder_grpc_request_receive_slot_hold_duration_seconds_count";
const PROXY_CLIENT_ADDRESS_FALLBACKS: &str = "zinder_grpc_proxy_client_address_fallbacks_total";
const STALE_TIP_REFUSALS: &str = "zinder_compat_stale_tip_refusals_total";

/// Every counter the observation subtracts, checked for a reset.
const OBSERVED_COUNTERS: [&str; 22] = [
    TREE_STATE_REPLAYED_BLOCKS_COUNT,
    TREE_STATE_REPLAYED_BLOCKS,
    TREE_STATE_READS,
    TREE_STATE_REPLAY_ADMISSIONS,
    TREE_STATE_REPLAY_REFUSALS,
    TREE_STATE_REPLAY_WAIT_SUM,
    TREE_STATE_REPLAY_WAIT_COUNT,
    BLOCK_RANGE_ADMISSIONS,
    BLOCK_RANGE_REFUSALS,
    BLOCK_RANGE_SLOT_WAIT_SUM,
    BLOCK_RANGE_SLOT_WAIT_COUNT,
    BLOCK_RANGE_STAGE_SUM,
    BLOCK_RANGE_STAGE_COUNT,
    RECEIVE_REFUSALS,
    RECEIVE_EVICTIONS,
    RECEIVE_SLOTS_HELD_COUNT,
    RECEIVE_SLOTS_HELD_SUM,
    RECEIVE_SLOTS_HELD_BUCKET,
    RECEIVE_SLOT_HOLD_SUM,
    RECEIVE_SLOT_HOLD_COUNT,
    PROXY_CLIENT_ADDRESS_FALLBACKS,
    STALE_TIP_REFUSALS,
];

/// Longest one scrape may take, from connecting to the end of the response.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a run waits between reads of the slots each receive pool holds.
const HELD_SLOTS_READ_INTERVAL: Duration = Duration::from_secs(1);

/// Most bytes a scrape reads. A Zinder `/metrics` page is far smaller; an
/// endpoint that sends more fails the scrape instead of filling memory.
const MAX_SCRAPE_BYTES: u64 = 64 * 1024 * 1024;

/// What the server counted between the start and the end of a run.
///
/// A server that does not export a metric reports zero for it.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub(crate) struct ServerObserved {
    /// Tree states the server rebuilt by replaying compact blocks. Every
    /// other answered tree state came from a stored checkpoint.
    tree_state_reconstructions: u64,
    tree_state_replayed_blocks: u64,
    tree_state_stored_reads: u64,
    tree_state_replayed_reads: u64,
    /// Tree states read per `height_band`, the first height of the 250,000
    /// block band that holds the requested height.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tree_state_stored_reads_by_band: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tree_state_replayed_reads_by_band: BTreeMap<String, u64>,
    tree_state_replay_admissions: u64,
    tree_state_replay_refusals: u64,
    /// Total time tree-state rebuilds waited for a rebuild slot.
    tree_state_replay_wait_seconds: f64,
    tree_state_replay_waits: u64,
    block_range_admissions: u64,
    block_range_refusals: u64,
    /// Total time block-range requests waited for a stream slot.
    block_range_slot_wait_seconds: f64,
    block_range_slot_waits: u64,
    /// Time block-range chunks spent in each stage, by stage name.
    block_range_stages: BTreeMap<String, StageTime>,
    receive_refusals: u64,
    /// Requests that lost their receive slot to a client holding fewer.
    receive_evictions: u64,
    /// How each receive pool of the public gRPC listener was used, by `pool`
    /// label.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    receive_pools: BTreeMap<String, ReceivePoolUse>,
    /// Requests on a listener that trusts its proxy's client address that
    /// counted against the proxy's own connection, by `reason` label.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    proxy_client_address_fallbacks: BTreeMap<String, u64>,
    stale_tip_refusals: u64,
}

/// Total time and chunk count of one block-range stage over a run.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub(crate) struct StageTime {
    seconds: f64,
    chunks: u64,
}

/// How one receive pool of the server's public gRPC listener was used over
/// a run.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub(crate) struct ReceivePoolUse {
    /// Requests the pool admitted.
    admissions: u64,
    refusals: u64,
    evictions: u64,
    /// Slots the pool held once it admitted a request, the request's own
    /// included, averaged over admissions.
    mean_slots_held_at_admission: f64,
    /// The smallest histogram bucket that holds every admission: no request
    /// found more slots held than this. Absent when nothing was admitted, or
    /// when an admission exceeded every finite bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    peak_slots_held_at_most: Option<f64>,
    /// Mean time a request held its slot, over the slots freed or taken
    /// during the run.
    mean_slot_hold_seconds: f64,
    /// The mean number of slots held at once over the run, from the slots
    /// the pool held at each read. It counts the slots held across the
    /// start or end of the run, and never exceeds the pool's size.
    mean_slots_held: f64,
}

/// The server's metrics read when a run starts, and the reads of its held
/// receive slots that continue until the run ends.
#[derive(Debug)]
pub(super) struct RunObservation {
    url: String,
    before: Vec<MetricSample>,
    held_slots: HeldSlotsSampler,
}

impl RunObservation {
    /// Reads the server's metrics at `url` and starts reading its held
    /// receive slots.
    pub(super) async fn start(url: &str) -> Result<Self, String> {
        let before = scrape(url).await?;
        let held_slots = HeldSlotsSampler::start(url, &before);
        Ok(Self {
            url: url.to_owned(),
            before,
            held_slots,
        })
    }

    /// Reads the server's metrics after the run and returns what the server
    /// counted, or why its counts are missing.
    ///
    /// A failed read keeps the run's results, because the wallets' work is
    /// already done and would otherwise be lost.
    pub(super) async fn finish(self) -> (Option<ServerObserved>, Option<String>) {
        match self.held_slots.finish(&self.url).await {
            Ok((after, held_slots)) => {
                match ServerObserved::between(&self.before, &after, &held_slots) {
                    Ok(observed) => (Some(observed), None),
                    Err(reason) => (None, Some(reason)),
                }
            }
            Err(error) => (None, Some(error)),
        }
    }
}

/// The slots each receive pool held, read from the server at moments of a
/// run, in the order they were read.
#[derive(Clone, Debug, Default)]
struct HeldSlotsTimeline {
    readings: Vec<HeldSlotsReading>,
}

#[derive(Clone, Debug)]
struct HeldSlotsReading {
    /// Time since the run's first read.
    at: Duration,
    by_pool: BTreeMap<String, f64>,
}

impl HeldSlotsTimeline {
    /// Adds the slots each pool held in `samples`, read `at` into the run.
    fn record(&mut self, at: Duration, samples: &[MetricSample]) {
        let mut by_pool = BTreeMap::<String, f64>::new();
        for sample in samples
            .iter()
            .filter(|sample| sample.name == RECEIVE_SLOTS_HELD)
        {
            if let Some(pool) = sample.label("pool") {
                *by_pool.entry(pool.to_owned()).or_default() += sample.reading;
            }
        }
        self.readings.push(HeldSlotsReading { at, by_pool });
    }

    /// Ends the timeline at `ended`, the moment the run ended, with the
    /// slots each pool held in `samples`, the scrape taken after it.
    ///
    /// Reads that completed after `ended` are left out, and `samples` counts
    /// as read at `ended` however long its scrape took, so the mean covers
    /// the run and none of the time spent reading after it.
    fn close(&mut self, ended: Duration, samples: &[MetricSample]) {
        self.readings.retain(|reading| reading.at <= ended);
        self.record(ended, samples);
    }

    /// The slots `pool` held on average from the first read to the last.
    ///
    /// Between two reads, the pool counts as holding the mean of both. The
    /// result weighs each span by the time it covers, so it never exceeds
    /// the most slots any read found.
    fn mean(&self, pool: &str) -> f64 {
        let held = |reading: &HeldSlotsReading| reading.by_pool.get(pool).copied().unwrap_or(0.0);
        let (slot_seconds, observed) = self
            .readings
            .iter()
            .zip(self.readings.iter().skip(1))
            .map(|(earlier, later)| {
                let span = later.at.saturating_sub(earlier.at).as_secs_f64();
                (span * f64::midpoint(held(earlier), held(later)), span)
            })
            .fold((0.0, 0.0), |(slot_seconds, observed), (held_for, span)| {
                (slot_seconds + held_for, observed + span)
            });
        if observed > 0.0 {
            slot_seconds / observed
        } else {
            0.0
        }
    }
}

/// Reads the slots each receive pool of the server holds, every
/// [`HELD_SLOTS_READ_INTERVAL`] until the run ends.
#[derive(Debug)]
struct HeldSlotsSampler {
    started: Instant,
    timeline: Arc<Mutex<HeldSlotsTimeline>>,
    reader: AbortOnDropHandle<()>,
}

impl HeldSlotsSampler {
    /// Starts reading `url`, with `before`, the scrape just taken, as the
    /// run's first read.
    fn start(url: &str, before: &[MetricSample]) -> Self {
        let started = Instant::now();
        let mut timeline = HeldSlotsTimeline::default();
        timeline.record(Duration::ZERO, before);
        let timeline = Arc::new(Mutex::new(timeline));
        let reader = AbortOnDropHandle::new(tokio::spawn(read_held_slots(
            url.to_owned(),
            started,
            Arc::clone(&timeline),
        )));
        Self {
            started,
            timeline,
            reader,
        }
    }

    /// Ends the run now: stops reading, then scrapes `url` once more and
    /// returns that scrape with the reads, the scrape as the last.
    ///
    /// The reader stops before the closing scrape starts, so no read shows
    /// the server after that scrape. The timeline ends when this is called,
    /// before either step, as [`HeldSlotsTimeline::close`] describes.
    async fn finish(self, url: &str) -> Result<(Vec<MetricSample>, HeldSlotsTimeline), String> {
        let ended = self.started.elapsed();
        self.reader.abort();
        let _ = self.reader.await;
        let after = scrape(url).await?;
        let mut timeline = std::mem::take(&mut *self.timeline.lock());
        timeline.close(ended, &after);
        Ok((after, timeline))
    }
}

async fn read_held_slots(url: String, started: Instant, timeline: Arc<Mutex<HeldSlotsTimeline>>) {
    loop {
        tokio::time::sleep(HELD_SLOTS_READ_INTERVAL).await;
        // A failed read only widens the span between the reads around it.
        if let Ok(samples) = scrape(&url).await {
            timeline.lock().record(started.elapsed(), &samples);
        }
    }
}

impl ServerObserved {
    /// The counter increases from `before` to `after`.
    ///
    /// Fails when a counter series went down or disappeared, as it does when
    /// the server restarts during the run: the increases would then mix two
    /// processes' counts, and a per-band count could exceed its total.
    fn between(
        before: &[MetricSample],
        after: &[MetricSample],
        held_slots: &HeldSlotsTimeline,
    ) -> Result<Self, String> {
        if let Some(series) = reset_series(before, after) {
            return Err(format!(
                "server counter {series} went down during the run, so the server likely \
                 restarted; its counts over the run are unavailable"
            ));
        }
        let delta = |name| after_minus_before(before, after, name, None);
        let count = |name| counter_value(delta(name));
        let labeled =
            |name, label| counter_value(after_minus_before(before, after, name, Some(label)));
        let block_range_stages = after
            .iter()
            .filter(|sample| sample.name == BLOCK_RANGE_STAGE_SUM)
            .filter_map(|sample| sample.label("stage").map(str::to_owned))
            .map(|stage| {
                let time = StageTime {
                    seconds: after_minus_before(
                        before,
                        after,
                        BLOCK_RANGE_STAGE_SUM,
                        Some(("stage", &stage)),
                    )
                    .max(0.0),
                    chunks: counter_value(after_minus_before(
                        before,
                        after,
                        BLOCK_RANGE_STAGE_COUNT,
                        Some(("stage", &stage)),
                    )),
                };
                (stage, time)
            })
            .collect();
        let tree_state_replayed_reads = labeled(TREE_STATE_READS, ("source", "replayed"));
        let counts_reads_by_source = after.iter().any(|sample| sample.name == TREE_STATE_READS);
        Ok(Self {
            tree_state_reconstructions: if counts_reads_by_source {
                tree_state_replayed_reads
            } else {
                count(TREE_STATE_REPLAYED_BLOCKS_COUNT)
            },
            tree_state_replayed_blocks: count(TREE_STATE_REPLAYED_BLOCKS),
            tree_state_stored_reads: labeled(TREE_STATE_READS, ("source", "stored")),
            tree_state_replayed_reads,
            tree_state_stored_reads_by_band: reads_by_band(before, after, "stored"),
            tree_state_replayed_reads_by_band: reads_by_band(before, after, "replayed"),
            tree_state_replay_admissions: count(TREE_STATE_REPLAY_ADMISSIONS),
            tree_state_replay_refusals: count(TREE_STATE_REPLAY_REFUSALS),
            tree_state_replay_wait_seconds: delta(TREE_STATE_REPLAY_WAIT_SUM).max(0.0),
            tree_state_replay_waits: count(TREE_STATE_REPLAY_WAIT_COUNT),
            block_range_admissions: count(BLOCK_RANGE_ADMISSIONS),
            block_range_refusals: count(BLOCK_RANGE_REFUSALS),
            block_range_slot_wait_seconds: delta(BLOCK_RANGE_SLOT_WAIT_SUM).max(0.0),
            block_range_slot_waits: count(BLOCK_RANGE_SLOT_WAIT_COUNT),
            block_range_stages,
            receive_refusals: count(RECEIVE_REFUSALS),
            receive_evictions: count(RECEIVE_EVICTIONS),
            receive_pools: receive_pools(before, after, held_slots),
            proxy_client_address_fallbacks: increases_by_label(
                before,
                after,
                PROXY_CLIENT_ADDRESS_FALLBACKS,
                "reason",
            ),
            stale_tip_refusals: count(STALE_TIP_REFUSALS),
        })
    }
}

/// Each receive pool's use during the run, by `pool` label, for every pool
/// that held a slot, or admitted, refused, or evicted a request.
fn receive_pools(
    before: &[MetricSample],
    after: &[MetricSample],
    held_slots: &HeldSlotsTimeline,
) -> BTreeMap<String, ReceivePoolUse> {
    let pool_labels = after
        .iter()
        .filter(|sample| {
            [
                RECEIVE_SLOTS_HELD,
                RECEIVE_SLOTS_HELD_COUNT,
                RECEIVE_REFUSALS,
                RECEIVE_EVICTIONS,
            ]
            .contains(&sample.name.as_str())
        })
        .filter_map(|sample| sample.label("pool").map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    pool_labels
        .into_iter()
        .filter_map(|pool| {
            let in_pool = Some(("pool", pool.as_str()));
            let increase = |name| after_minus_before(before, after, name, in_pool).max(0.0);
            let admissions = increase(RECEIVE_SLOTS_HELD_COUNT);
            let holds = increase(RECEIVE_SLOT_HOLD_COUNT);
            let hold_seconds = increase(RECEIVE_SLOT_HOLD_SUM);
            let ratio = |numerator: f64, denominator: f64| {
                if denominator > 0.0 {
                    numerator / denominator
                } else {
                    0.0
                }
            };
            let used = ReceivePoolUse {
                admissions: counter_value(admissions),
                refusals: counter_value(increase(RECEIVE_REFUSALS)),
                evictions: counter_value(increase(RECEIVE_EVICTIONS)),
                mean_slots_held_at_admission: ratio(increase(RECEIVE_SLOTS_HELD_SUM), admissions),
                peak_slots_held_at_most: peak_bucket(before, after, &pool, admissions),
                mean_slot_hold_seconds: ratio(hold_seconds, holds),
                mean_slots_held: held_slots.mean(&pool),
            };
            (used != ReceivePoolUse::default()).then_some((pool, used))
        })
        .collect()
}

/// The smallest finite `le` bucket of the occupancy histogram whose increase
/// over the run covers all `admissions` in `pool`.
fn peak_bucket(
    before: &[MetricSample],
    after: &[MetricSample],
    pool: &str,
    admissions: f64,
) -> Option<f64> {
    if admissions <= 0.0 {
        return None;
    }
    after
        .iter()
        .filter(|sample| {
            sample.name == RECEIVE_SLOTS_HELD_BUCKET && sample.label("pool") == Some(pool)
        })
        .filter_map(|sample| {
            let bound = sample.label("le")?;
            let upper = bound
                .parse::<f64>()
                .ok()
                .filter(|upper| upper.is_finite())?;
            let covered = sample.reading
                - before
                    .iter()
                    .find(|earlier| {
                        earlier.name == RECEIVE_SLOTS_HELD_BUCKET
                            && earlier.label("pool") == Some(pool)
                            && earlier.label("le") == Some(bound)
                    })
                    .map_or(0.0, |earlier| earlier.reading);
            (covered >= admissions).then_some(upper)
        })
        .min_by(f64::total_cmp)
}

/// The increase of every series of `name`, by its `label`, leaving out the
/// labels that did not increase.
fn increases_by_label(
    before: &[MetricSample],
    after: &[MetricSample],
    name: &str,
    label: &str,
) -> BTreeMap<String, u64> {
    after
        .iter()
        .filter(|sample| sample.name == name)
        .filter_map(|sample| sample.label(label).map(str::to_owned))
        .filter_map(|label_value| {
            let increase = counter_value(after_minus_before(
                before,
                after,
                name,
                Some((label, &label_value)),
            ));
            (increase > 0).then_some((label_value, increase))
        })
        .collect()
}

/// The first observed counter series, rendered with its labels, whose
/// reading in `after` is below its reading in `before` or missing.
fn reset_series(before: &[MetricSample], after: &[MetricSample]) -> Option<String> {
    let series_key = |sample: &MetricSample| {
        let mut labels = sample
            .labels
            .iter()
            .map(|(key, label)| format!("{key}=\"{label}\""))
            .collect::<Vec<_>>();
        labels.sort();
        format!("{}{{{}}}", sample.name, labels.join(","))
    };
    let observed = |sample: &&MetricSample| OBSERVED_COUNTERS.contains(&sample.name.as_str());
    let after_readings = after
        .iter()
        .filter(observed)
        .map(|sample| (series_key(sample), sample.reading))
        .collect::<BTreeMap<_, _>>();
    before.iter().filter(observed).find_map(|sample| {
        let key = series_key(sample);
        let went_down = after_readings
            .get(&key)
            .is_none_or(|reading| *reading < sample.reading);
        went_down.then_some(key)
    })
}

/// Tree-state reads from `source` during the run, by `height_band` label.
fn reads_by_band(
    before: &[MetricSample],
    after: &[MetricSample],
    source: &str,
) -> BTreeMap<String, u64> {
    let totals = |samples: &[MetricSample]| {
        let mut totals = BTreeMap::<String, f64>::new();
        for sample in samples.iter().filter(|sample| {
            sample.name == TREE_STATE_READS && sample.label("source") == Some(source)
        }) {
            if let Some(band) = sample.label("height_band") {
                *totals.entry(band.to_owned()).or_default() += sample.reading;
            }
        }
        totals
    };
    let before = totals(before);
    totals(after)
        .into_iter()
        .filter_map(|(band, total)| {
            let reads = counter_value(total - before.get(&band).copied().unwrap_or(0.0));
            (reads > 0).then_some((band, reads))
        })
        .collect()
}

/// The increase of `name`, summed over every series or only those whose
/// label matches.
fn after_minus_before(
    before: &[MetricSample],
    after: &[MetricSample],
    name: &str,
    label: Option<(&str, &str)>,
) -> f64 {
    let total = |samples: &[MetricSample]| match label {
        None => sum_by_name(samples, name),
        Some((key, wanted)) => samples
            .iter()
            .filter(|sample| sample.name == name && sample.label(key) == Some(wanted))
            .map(|sample| sample.reading)
            .sum(),
    };
    total(after) - total(before)
}

/// Rounds a counter increase to a whole count.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is a non-negative, rounded counter increase far below u64::MAX"
)]
fn counter_value(increase: f64) -> u64 {
    increase.max(0.0).round() as u64
}

/// Reads the Prometheus exposition at an `http://host:port/path` URL.
async fn scrape(url: &str) -> Result<Vec<MetricSample>, String> {
    scrape_within(url, SCRAPE_TIMEOUT, MAX_SCRAPE_BYTES).await
}

/// Reads the exposition at `url`, failing when the whole exchange takes
/// longer than `deadline`.
async fn scrape_within(
    url: &str,
    deadline: Duration,
    max_bytes: u64,
) -> Result<Vec<MetricSample>, String> {
    tokio::time::timeout(deadline, read_exposition(url, max_bytes))
        .await
        .map_err(|_| {
            format!(
                "{url}: no complete response within {}s",
                deadline.as_secs_f64()
            )
        })?
}

async fn read_exposition(url: &str, max_bytes: u64) -> Result<Vec<MetricSample>, String> {
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url}: the metrics URL must start with http://"))?;
    let (authority, path) = without_scheme.split_once('/').map_or_else(
        || (without_scheme, "/".to_owned()),
        |(authority, path)| (authority, format!("/{path}")),
    );
    let mut stream = TcpStream::connect(authority)
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    // An HTTP/1.0 request rules out a chunked response, so the body is
    // every byte after the headers until the server closes the connection.
    stream
        .write_all(
            format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    let mut response = String::new();
    (&mut stream)
        .take(max_bytes.saturating_add(1))
        .read_to_string(&mut response)
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    if u64::try_from(response.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(format!("{url}: the response exceeds {max_bytes} bytes"));
    }
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{url}: response has no header end"))?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "{url}: {}",
            head.lines().next().unwrap_or_default()
        ));
    }
    let transfer_coded = head.lines().skip(1).any(|line| {
        line.split_once(':')
            .is_some_and(|(name, _)| name.trim().eq_ignore_ascii_case("transfer-encoding"))
    });
    if transfer_coded {
        return Err(format!(
            "{url}: the response has a Transfer-Encoding, which an HTTP/1.0 request does not allow"
        ));
    }
    Ok(parse_prometheus_samples(declared_body(url, head, body)?))
}

/// The body a response declared with `Content-Length`, or every byte after
/// the headers when it declares none and ends by closing the connection.
///
/// A connection that closes before the declared length is a failed scrape:
/// parsing the prefix would read the missing counters as zero.
fn declared_body<'response>(
    url: &str,
    head: &str,
    body: &'response str,
) -> Result<&'response str, String> {
    let declared = head.lines().skip(1).find_map(|line| {
        line.split_once(':')
            .filter(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
            .map(|(_, length)| length.trim().parse::<usize>())
    });
    match declared {
        None => Ok(body),
        Some(Err(_)) => Err(format!(
            "{url}: the response has an unreadable Content-Length"
        )),
        Some(Ok(length)) => body.get(..length).ok_or_else(|| {
            format!(
                "{url}: the response ended after {} of its {length} body bytes",
                body.len()
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use zinder_bench::metrics_scrape::{MetricSample, parse_prometheus_samples, sum_by_name};

    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::{
        HeldSlotsSampler, HeldSlotsTimeline, RunObservation, ServerObserved, scrape, scrape_within,
    };

    const RUN: Duration = Duration::from_secs(10);

    /// A run whose only reads are the scrapes before and after it.
    fn read_at_both_ends(before: &[MetricSample], after: &[MetricSample]) -> HeldSlotsTimeline {
        let mut held_slots = HeldSlotsTimeline::default();
        held_slots.record(Duration::ZERO, before);
        held_slots.record(RUN, after);
        held_slots
    }

    fn held_in_request_pool(held: u32) -> Vec<MetricSample> {
        parse_prometheus_samples(&format!(
            "zinder_grpc_request_receive_slots_held{{pool=\"request\"}} {held}\n"
        ))
    }

    #[test]
    fn counts_are_increases_over_the_run() -> Result<(), String> {
        let before = parse_prometheus_samples(
            "zinder_query_tree_state_replayed_blocks_count 4\n\
             zinder_query_tree_state_replayed_blocks_sum 300\n",
        );
        let after = parse_prometheus_samples(
            "zinder_query_tree_state_replayed_blocks_count 10\n\
             zinder_query_tree_state_replayed_blocks_sum 720\n\
             zinder_query_tree_state_replay_refusals_total 3\n\
             zinder_grpc_request_receive_refusals_total{pool=\"request\"} 1\n\
             zinder_grpc_request_receive_refusals_total{pool=\"client_stream\"} 1\n",
        );

        let observed =
            ServerObserved::between(&before, &after, &read_at_both_ends(&before, &after))?;

        assert_eq!(observed.tree_state_reconstructions, 6);
        assert_eq!(observed.tree_state_replayed_blocks, 420);
        assert_eq!(observed.tree_state_replay_refusals, 3);
        assert_eq!(observed.receive_refusals, 2);
        assert_eq!(observed.block_range_refusals, 0);
        Ok(())
    }

    #[test]
    fn a_counter_that_went_down_makes_the_observation_unavailable() {
        // The server restarted: one band's reads restarted from zero while
        // another band's new process count already exceeds its old one.
        let before = parse_prometheus_samples(
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"a\"} 50\n\
             zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"b\"} 1\n",
        );
        let restarted = parse_prometheus_samples(
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"a\"} 3\n\
             zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"b\"} 9\n",
        );
        let missing = parse_prometheus_samples(
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"b\"} 9\n",
        );

        for after in [restarted, missing] {
            let refused =
                ServerObserved::between(&before, &after, &read_at_both_ends(&before, &after)).err();

            assert!(
                refused.is_some_and(|reason| reason.contains("height_band=\"a\"")),
                "the reason names the series that went down"
            );
        }
        // A metric the observation does not read may go down.
        let unrelated_before = parse_prometheus_samples("process_resident_memory_bytes 9\n");
        let unrelated_after = parse_prometheus_samples("process_resident_memory_bytes 1\n");
        assert!(
            ServerObserved::between(
                &unrelated_before,
                &unrelated_after,
                &HeldSlotsTimeline::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn labeled_reads_waits_and_stages_are_split_by_label() -> Result<(), String> {
        let before = parse_prometheus_samples(
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"a\"} 2\n\
             zinder_compat_block_range_stage_duration_seconds_sum{stage=\"read\"} 1.5\n\
             zinder_compat_block_range_stage_duration_seconds_count{stage=\"read\"} 3\n",
        );
        let after = parse_prometheus_samples(
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"a\"} 7\n\
             zinder_query_tree_state_reads_total{source=\"replayed\",height_band=\"a\"} 4\n\
             zinder_query_tree_state_replayed_blocks_count{height_band=\"a\"} 9\n\
             zinder_query_tree_state_replay_wait_duration_seconds_sum 0.25\n\
             zinder_query_tree_state_replay_wait_duration_seconds_count 4\n\
             zinder_compat_block_range_stage_duration_seconds_sum{stage=\"read\"} 4.0\n\
             zinder_compat_block_range_stage_duration_seconds_count{stage=\"read\"} 13\n\
             zinder_compat_block_range_stage_duration_seconds_sum{stage=\"send\"} 0.5\n\
             zinder_compat_block_range_stage_duration_seconds_count{stage=\"send\"} 9\n",
        );

        let observed =
            ServerObserved::between(&before, &after, &read_at_both_ends(&before, &after))?;

        assert_eq!(observed.tree_state_stored_reads, 5);
        assert_eq!(observed.tree_state_replayed_reads, 4);
        assert_eq!(observed.tree_state_reconstructions, 4);
        assert_eq!(observed.tree_state_stored_reads_by_band["a"], 5);
        assert_eq!(observed.tree_state_replayed_reads_by_band["a"], 4);
        assert_eq!(observed.tree_state_replay_waits, 4);
        assert!((observed.tree_state_replay_wait_seconds - 0.25).abs() < 1e-9);
        let read = &observed.block_range_stages["read"];
        assert!((read.seconds - 2.5).abs() < 1e-9);
        assert_eq!(read.chunks, 10);
        assert_eq!(observed.block_range_stages["send"].chunks, 9);
        Ok(())
    }

    #[test]
    fn receive_pools_report_occupancy_evictions_refusals_and_proxy_fallbacks() -> Result<(), String>
    {
        let before = parse_prometheus_samples(
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"1\"} 10\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"2\"} 10\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"4\"} 10\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"+Inf\"} 10\n\
             zinder_grpc_request_receive_slots_held_at_admission_sum{pool=\"request\"} 10\n\
             zinder_grpc_request_receive_slots_held_at_admission_count{pool=\"request\"} 10\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_sum{pool=\"request\"} 0.5\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 10\n\
             zinder_grpc_request_receive_slots_held{pool=\"request\"} 0\n",
        );
        let after = parse_prometheus_samples(
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"1\"} 70\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"2\"} 100\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"4\"} 110\n\
             zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"+Inf\"} 110\n\
             zinder_grpc_request_receive_slots_held_at_admission_sum{pool=\"request\"} 160\n\
             zinder_grpc_request_receive_slots_held_at_admission_count{pool=\"request\"} 110\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_sum{pool=\"request\"} 2.5\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 110\n\
             zinder_grpc_request_receive_slots_held{pool=\"request\"} 1\n\
             zinder_grpc_request_receive_evictions_total{pool=\"request\"} 3\n\
             zinder_grpc_request_receive_refusals_total{pool=\"client_stream\"} 2\n\
             zinder_grpc_proxy_client_address_fallbacks_total{reason=\"missing\"} 4\n\
             zinder_grpc_proxy_client_address_fallbacks_total{reason=\"repeated\"} 0\n",
        );

        let observed =
            ServerObserved::between(&before, &after, &read_at_both_ends(&before, &after))?;

        assert_eq!(observed.receive_evictions, 3);
        assert_eq!(observed.receive_refusals, 2);
        let request = &observed.receive_pools["request"];
        assert_eq!(request.admissions, 100);
        assert_eq!(request.evictions, 3);
        assert_eq!(request.refusals, 0);
        assert!((request.mean_slots_held_at_admission - 1.5).abs() < 1e-9);
        // Every admission in the run fell at or below 4 slots, not all at 2.
        assert_eq!(request.peak_slots_held_at_most, Some(4.0));
        assert!((request.mean_slot_hold_seconds - 0.02).abs() < 1e-9);
        // No read between the scrapes, so the pool counts as holding the
        // mean of 0 and 1 slots.
        assert!((request.mean_slots_held - 0.5).abs() < 1e-9);
        let client_stream = &observed.receive_pools["client_stream"];
        assert_eq!(client_stream.refusals, 2);
        assert_eq!(client_stream.admissions, 0);
        assert_eq!(client_stream.peak_slots_held_at_most, None);
        assert_eq!(
            observed.proxy_client_address_fallbacks,
            BTreeMap::from([("missing".to_owned(), 4)])
        );
        Ok(())
    }

    #[test]
    fn mean_slots_held_counts_only_slot_time_inside_the_run() -> Result<(), String> {
        // A full 32-slot pool. Requests that took their slots long before the
        // run free them during it, for 2,519 slot-seconds of holds, and the
        // requests that replace them still hold their slots when it ends.
        let before = parse_prometheus_samples(
            "zinder_grpc_request_receive_slots_held{pool=\"request\"} 32\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_sum{pool=\"request\"} 0\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 0\n",
        );
        let after = parse_prometheus_samples(
            "zinder_grpc_request_receive_slots_held{pool=\"request\"} 32\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_sum{pool=\"request\"} 2519\n\
             zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 32\n",
        );
        let mut held_slots = HeldSlotsTimeline::default();
        held_slots.record(Duration::ZERO, &before);
        for second in 1..10 {
            // One slot stands free for the read at 5 seconds.
            let held = if second == 5 { 31 } else { 32 };
            held_slots.record(Duration::from_secs(second), &held_in_request_pool(held));
        }
        held_slots.record(RUN, &after);

        let observed = ServerObserved::between(&before, &after, &held_slots)?;

        let request = &observed.receive_pools["request"];
        assert!(
            request.mean_slots_held <= 32.0,
            "{}",
            request.mean_slots_held
        );
        // 320 slot-seconds, less half a slot over each of the two seconds
        // around the read at 5 seconds.
        assert!(
            (request.mean_slots_held - 31.9).abs() < 1e-9,
            "{}",
            request.mean_slots_held
        );
        Ok(())
    }

    #[test]
    fn mean_slots_held_counts_slots_held_across_both_ends_of_the_run() -> Result<(), String> {
        // Every slot is held from before the run until after it, so no hold
        // ends during the run.
        let before = held_in_request_pool(32);
        let after = held_in_request_pool(32);

        let observed =
            ServerObserved::between(&before, &after, &read_at_both_ends(&before, &after))?;

        let request = &observed.receive_pools["request"];
        assert!((request.mean_slots_held - 32.0).abs() < 1e-9);
        assert_eq!(request.admissions, 0);
        Ok(())
    }

    #[test]
    fn mean_slots_held_never_exceeds_the_most_slots_any_read_found() {
        // A read that lands just after the closing one leaves the timeline
        // out of order. The pool is full at every read.
        let mut held_slots = HeldSlotsTimeline::default();
        held_slots.record(Duration::ZERO, &held_in_request_pool(32));
        held_slots.record(RUN + Duration::from_millis(1), &held_in_request_pool(32));
        held_slots.record(RUN, &held_in_request_pool(32));

        let mean = held_slots.mean("request");

        assert!((mean - 32.0).abs() < 1e-9, "{mean}");
    }

    #[test]
    fn mean_slots_held_leaves_out_the_time_after_the_run_ends() {
        // The pool holds 8 slots for the run's last second and frees them
        // once the run ends at 2 seconds, so both a background read that
        // completes after the end and the closing scrape find none held.
        let ended = Duration::from_secs(2);
        let mut held_slots = HeldSlotsTimeline::default();
        held_slots.record(Duration::ZERO, &held_in_request_pool(0));
        held_slots.record(Duration::from_secs(1), &held_in_request_pool(8));
        held_slots.record(ended, &held_in_request_pool(8));
        held_slots.record(Duration::from_millis(2_400), &held_in_request_pool(0));

        held_slots.close(ended, &held_in_request_pool(0));

        assert!(
            held_slots
                .readings
                .iter()
                .all(|reading| reading.at <= ended),
            "{held_slots:?}"
        );
        assert_eq!(
            held_slots.readings.last().map(|reading| reading.at),
            Some(ended)
        );
        // 4 slot-seconds over the first second and 8 over the second.
        let mean = held_slots.mean("request");
        assert!((mean - 6.0).abs() < 1e-9, "{mean}");
    }

    #[tokio::test]
    async fn the_sampler_stops_reading_before_its_closing_scrape()
    -> Result<(), Box<dyn std::error::Error>> {
        const RESPONSE: &[u8] = b"HTTP/1.0 200 OK\r\n\r\n\
            zinder_grpc_request_receive_slots_held{pool=\"request\"} 8\n";
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let (first_read_answered, answered) = tokio::sync::oneshot::channel();
        let later_requests = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn({
            let later_requests = Arc::clone(&later_requests);
            async move {
                let (mut socket, _) = listener.accept().await?;
                let mut request = [0_u8; 512];
                let _ = socket.read(&mut request).await?;
                socket.write_all(RESPONSE).await?;
                drop(socket);
                let _ = first_read_answered.send(());
                // Every later answer takes longer than a read interval, so
                // a reader still running during the closing scrape reads too.
                while let Ok((mut socket, _)) = listener.accept().await {
                    later_requests.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let mut request = [0_u8; 512];
                        let _ = socket.read(&mut request).await;
                        tokio::time::sleep(super::HELD_SLOTS_READ_INTERVAL * 2).await;
                        let _ = socket.write_all(RESPONSE).await;
                    });
                }
                Ok::<_, std::io::Error>(())
            }
        });

        let sampler = HeldSlotsSampler::start(&url, &held_in_request_pool(0));
        answered.await?;
        let run_ended = sampler.started.elapsed();
        let (after, held_slots) = sampler.finish(&url).await?;
        server.abort();

        assert_eq!(
            later_requests.load(Ordering::SeqCst),
            1,
            "only the closing scrape reaches the server once it starts"
        );
        assert!((sum_by_name(&after, super::RECEIVE_SLOTS_HELD) - 8.0).abs() < 1e-9);
        assert!(
            held_slots
                .readings
                .windows(2)
                .all(|pair| pair[0].at <= pair[1].at),
            "{held_slots:?}"
        );
        // The closing scrape took two read intervals, but counts as read
        // when the run ended.
        assert!(
            held_slots.readings.last().is_some_and(|closing| {
                closing.at >= run_ended && closing.at < run_ended + super::HELD_SLOTS_READ_INTERVAL
            }),
            "{run_ended:?} {held_slots:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_sampler_reads_held_slots_until_the_run_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0_u8; 512];
                let _ = socket.read(&mut request).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.0 200 OK\r\n\r\n\
                          zinder_grpc_request_receive_slots_held{pool=\"request\"} 8\n",
                    )
                    .await;
            }
        });

        let sampler = HeldSlotsSampler::start(&url, &held_in_request_pool(0));
        tokio::time::sleep(super::HELD_SLOTS_READ_INTERVAL * 2 + Duration::from_millis(500)).await;
        let (_, held_slots) = sampler.finish(&url).await?;
        server.abort();

        // The reads at both ends, and at least one between them.
        assert!(held_slots.readings.len() >= 3, "{held_slots:?}");
        assert!(
            held_slots
                .readings
                .windows(2)
                .all(|pair| pair[0].at <= pair[1].at)
        );
        let mean = held_slots.mean("request");
        assert!(mean > 0.0 && mean < 8.0, "{mean}");
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_scrape_after_the_run_keeps_the_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            // Answer the read before the run, then stop listening.
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await?;
            socket.write_all(b"HTTP/1.0 200 OK\r\n\r\n").await?;
            Ok::<_, std::io::Error>(())
        });
        let observation = RunObservation::start(&url).await?;
        server.await??;

        let (observed, error) = observation.finish().await;

        assert!(observed.is_none());
        assert!(
            error.is_some_and(|error| error.contains(&url)),
            "the error names the metrics URL"
        );
        Ok(())
    }

    #[tokio::test]
    async fn scrape_reads_an_ops_endpoint() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await?;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\r\n\
                      zinder_query_tree_state_replay_refusals_total 5\n",
                )
                .await?;
            Ok::<_, std::io::Error>(())
        });

        let samples = scrape(&url).await?;
        server.await??;

        assert_eq!(
            ServerObserved::between(&[], &samples, &HeldSlotsTimeline::default())?
                .tree_state_replay_refusals,
            5
        );
        assert!(scrape("https://127.0.0.1:1/metrics").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn scrape_gives_up_on_an_endpoint_that_never_finishes_its_response()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await?;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\r\nzinder_")
                .await?;
            // Hold the connection open without finishing the response.
            std::future::pending::<()>().await;
            Ok::<_, std::io::Error>(())
        });

        let scraped =
            scrape_within(&url, Duration::from_millis(200), super::MAX_SCRAPE_BYTES).await;
        server.abort();

        let error = scraped
            .err()
            .ok_or("a stalled endpoint must fail the scrape")?;
        assert!(error.contains("no complete response"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn scrape_refuses_a_body_shorter_than_its_content_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await?;
            let body = "zinder_query_tree_state_replay_refusals_total 5\n";
            let response = format!(
                "HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                body.len() + 40,
                body
            );
            socket.write_all(response.as_bytes()).await?;
            Ok::<_, std::io::Error>(())
        });

        let scraped = scrape(&url).await;
        server.await??;

        let error = scraped
            .err()
            .ok_or("a body shorter than its Content-Length must fail the scrape")?;
        assert!(error.contains("ended after"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn scrape_refuses_a_response_larger_than_its_byte_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await?;
            let body = "zinder_query_tree_state_replay_refusals_total 5\n".repeat(64);
            let response = format!("HTTP/1.0 200 OK\r\n\r\n{body}");
            // The client stops reading at its limit and closes, so a write
            // past it may fail.
            let _ = socket.write_all(response.as_bytes()).await;
            Ok::<_, std::io::Error>(())
        });

        let scraped = scrape_within(&url, Duration::from_secs(5), 1024).await;
        server.await??;

        let error = scraped
            .err()
            .ok_or("a response past the byte limit must fail the scrape")?;
        assert!(error.contains("exceeds 1024 bytes"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn scrape_reads_a_close_delimited_body_and_a_declared_one()
    -> Result<(), Box<dyn std::error::Error>> {
        for declare_length in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("http://{}/metrics", listener.local_addr()?);
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await?;
                let mut request = [0_u8; 512];
                let _ = socket.read(&mut request).await?;
                let body = "zinder_query_tree_state_replay_refusals_total 5\n";
                let length = if declare_length {
                    format!("Content-Length: {}\r\n", body.len())
                } else {
                    String::new()
                };
                let response = format!("HTTP/1.0 200 OK\r\n{length}\r\n{body}");
                socket.write_all(response.as_bytes()).await?;
                Ok::<_, std::io::Error>(())
            });

            let samples = scrape(&url).await?;
            server.await??;

            assert_eq!(samples.len(), 1, "declared length: {declare_length}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn scrape_asks_for_http_1_0_and_refuses_a_chunked_response()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 512];
            let read = socket.read(&mut request).await?;
            // The chunks split a metric name, which chunk framing passed to
            // the parser would break.
            let (first, second) = ("zinder_query_tree_", "state_replay_refusals_total 5\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                 {:x}\r\n{first}\r\n{:x}\r\n{second}\r\n0\r\n\r\n",
                first.len(),
                second.len()
            );
            socket.write_all(response.as_bytes()).await?;
            Ok::<_, std::io::Error>(String::from_utf8_lossy(&request[..read]).into_owned())
        });

        let scraped = scrape(&url).await;
        let request = server.await??;

        assert!(
            request.starts_with("GET /metrics HTTP/1.0\r\n"),
            "{request}"
        );
        let error = scraped
            .err()
            .ok_or("a chunked response must fail the scrape")?;
        assert!(error.contains("Transfer-Encoding"), "{error}");
        Ok(())
    }
}
