//! Tree-state and admission counters read from a Zinder server's metrics.
//!
//! The client sees which heights it asked for, but only the server knows
//! whether each tree state came from a stored checkpoint or a replay: a store
//! that a checkpoint pass densified answers every settled height directly. A
//! run given the server's metrics endpoint reports the server's own counts
//! over the run instead of inferring them from the 100-block stride.

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
const STALE_TIP_REFUSALS: &str = "zinder_compat_stale_tip_refusals_total";

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
    stale_tip_refusals: u64,
}

/// Total time and chunk count of one block-range stage over a run.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub(crate) struct StageTime {
    seconds: f64,
    chunks: u64,
}

impl ServerObserved {
    /// The counter increases from `before` to `after`.
    pub(super) fn between(before: &[MetricSample], after: &[MetricSample]) -> Self {
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
        Self {
            tree_state_reconstructions: count(TREE_STATE_RECONSTRUCTIONS),
            tree_state_replayed_blocks: count(TREE_STATE_REPLAYED_BLOCKS),
            tree_state_stored_reads: labeled(TREE_STATE_READS, ("source", "stored")),
            tree_state_replayed_reads: labeled(TREE_STATE_READS, ("source", "replayed")),
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
            stale_tip_refusals: count(STALE_TIP_REFUSALS),
        }
    }
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

/// Rounds a counter increase to a whole count; a reset reads as zero.
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

    use std::time::Duration;

    use super::{ServerObserved, scrape, scrape_within};

    #[test]
    fn counts_are_increases_over_the_run() {
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

        let observed = ServerObserved::between(&before, &after);

        assert_eq!(observed.tree_state_reconstructions, 6);
        assert_eq!(observed.tree_state_replayed_blocks, 420);
        assert_eq!(observed.tree_state_replay_refusals, 3);
        assert_eq!(observed.receive_refusals, 2);
        assert_eq!(observed.block_range_refusals, 0);
    }

    #[test]
    fn labeled_reads_waits_and_stages_are_split_by_label() {
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

        let observed = ServerObserved::between(&before, &after);

        assert_eq!(observed.tree_state_stored_reads, 5);
        assert_eq!(observed.tree_state_replayed_reads, 4);
        assert_eq!(observed.tree_state_replay_waits, 4);
        assert!((observed.tree_state_replay_wait_seconds - 0.25).abs() < 1e-9);
        let read = &observed.block_range_stages["read"];
        assert!((read.seconds - 2.5).abs() < 1e-9);
        assert_eq!(read.chunks, 10);
        assert_eq!(observed.block_range_stages["send"].chunks, 9);
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
            ServerObserved::between(&[], &samples).tree_state_replay_refusals,
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
