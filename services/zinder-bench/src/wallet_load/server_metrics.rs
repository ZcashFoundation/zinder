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
//! the server named each client.

use std::{collections::BTreeMap, time::Duration};

use serde::Serialize;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use zinder_bench::metrics_scrape::{MetricSample, parse_prometheus_samples, sum_by_name};

const TREE_STATE_RECONSTRUCTIONS: &str = "zinder_query_tree_state_replayed_blocks_count";
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
    TREE_STATE_RECONSTRUCTIONS,
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
    /// Slot time held during the run over the run's wall time: the mean
    /// number of slots held at once.
    mean_slots_held: f64,
}

impl ServerObserved {
    /// The counter increases from `before` to `after`.
    ///
    /// Fails when a counter series went down or disappeared, as it does when
    /// the server restarts during the run: the increases would then mix two
    /// processes' counts, and a per-band count could exceed its total.
    pub(super) fn between(
        before: &[MetricSample],
        after: &[MetricSample],
        run: Duration,
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
        Ok(Self {
            tree_state_reconstructions: count(TREE_STATE_RECONSTRUCTIONS),
            tree_state_replayed_blocks: count(TREE_STATE_REPLAYED_BLOCKS),
            tree_state_stored_reads: labeled(TREE_STATE_READS, ("source", "stored")),
            tree_state_replayed_reads: labeled(TREE_STATE_READS, ("source", "replayed")),
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
            receive_pools: receive_pools(before, after, run),
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
/// that admitted, refused, or evicted a request.
fn receive_pools(
    before: &[MetricSample],
    after: &[MetricSample],
    run: Duration,
) -> BTreeMap<String, ReceivePoolUse> {
    let pool_labels = after
        .iter()
        .filter(|sample| {
            [
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
                mean_slots_held: ratio(hold_seconds, run.as_secs_f64()),
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
pub(super) async fn scrape(url: &str) -> Result<Vec<MetricSample>, String> {
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
    use zinder_bench::metrics_scrape::parse_prometheus_samples;

    use std::{collections::BTreeMap, time::Duration};

    use super::{ServerObserved, scrape, scrape_within};

    const RUN: Duration = Duration::from_secs(10);

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

        let observed = ServerObserved::between(&before, &after, RUN)?;

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
            let refused = ServerObserved::between(&before, &after, RUN).err();

            assert!(
                refused.is_some_and(|reason| reason.contains("height_band=\"a\"")),
                "the reason names the series that went down"
            );
        }
        // A metric the observation does not read may go down.
        let unrelated_before = parse_prometheus_samples("process_resident_memory_bytes 9\n");
        let unrelated_after = parse_prometheus_samples("process_resident_memory_bytes 1\n");
        assert!(ServerObserved::between(&unrelated_before, &unrelated_after, RUN).is_ok());
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
             zinder_query_tree_state_replay_wait_duration_seconds_sum 0.25\n\
             zinder_query_tree_state_replay_wait_duration_seconds_count 4\n\
             zinder_compat_block_range_stage_duration_seconds_sum{stage=\"read\"} 4.0\n\
             zinder_compat_block_range_stage_duration_seconds_count{stage=\"read\"} 13\n\
             zinder_compat_block_range_stage_duration_seconds_sum{stage=\"send\"} 0.5\n\
             zinder_compat_block_range_stage_duration_seconds_count{stage=\"send\"} 9\n",
        );

        let observed = ServerObserved::between(&before, &after, RUN)?;

        assert_eq!(observed.tree_state_stored_reads, 5);
        assert_eq!(observed.tree_state_replayed_reads, 4);
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
             zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 10\n",
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
             zinder_grpc_request_receive_evictions_total{pool=\"request\"} 3\n\
             zinder_grpc_request_receive_refusals_total{pool=\"client_stream\"} 2\n\
             zinder_grpc_proxy_client_address_fallbacks_total{reason=\"missing\"} 4\n\
             zinder_grpc_proxy_client_address_fallbacks_total{reason=\"repeated\"} 0\n",
        );

        let observed = ServerObserved::between(&before, &after, RUN)?;

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
        assert!((request.mean_slots_held - 0.2).abs() < 1e-9);
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
            ServerObserved::between(&[], &samples, RUN)?.tree_state_replay_refusals,
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
