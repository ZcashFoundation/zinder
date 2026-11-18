//! Tree-state and admission counters read from a Zinder server's metrics.
//!
//! The client sees which heights it asked for, but only the server knows
//! whether each tree state came from a stored checkpoint or a replay: a store
//! that a checkpoint pass densified answers every settled height directly. A
//! run given the server's metrics endpoint reports the server's own counts
//! over the run instead of inferring them from the 100-block stride.

use serde::Serialize;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use zinder_bench::metrics_scrape::{MetricSample, parse_prometheus_samples, sum_by_name};

const TREE_STATE_RECONSTRUCTIONS: &str = "zinder_query_tree_state_replayed_blocks_count";
const TREE_STATE_REPLAYED_BLOCKS: &str = "zinder_query_tree_state_replayed_blocks_sum";
const TREE_STATE_REPLAY_REFUSALS: &str = "zinder_query_tree_state_replay_refusals_total";
const BLOCK_RANGE_REFUSALS: &str = "zinder_compat_block_range_stream_refusals_total";
const RECEIVE_REFUSALS: &str = "zinder_grpc_request_receive_refusals_total";

/// What the server counted between the start and the end of a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ServerObserved {
    /// Tree states the server rebuilt by replaying compact blocks. Every
    /// other answered tree state came from a stored checkpoint.
    tree_state_reconstructions: u64,
    tree_state_replayed_blocks: u64,
    tree_state_replay_refusals: u64,
    block_range_refusals: u64,
    receive_refusals: u64,
}

impl ServerObserved {
    /// The counter increases from `before` to `after`.
    pub(super) fn between(before: &[MetricSample], after: &[MetricSample]) -> Self {
        let delta = |name| counter_value(sum_by_name(after, name) - sum_by_name(before, name));
        Self {
            tree_state_reconstructions: delta(TREE_STATE_RECONSTRUCTIONS),
            tree_state_replayed_blocks: delta(TREE_STATE_REPLAYED_BLOCKS),
            tree_state_replay_refusals: delta(TREE_STATE_REPLAY_REFUSALS),
            block_range_refusals: delta(BLOCK_RANGE_REFUSALS),
            receive_refusals: delta(RECEIVE_REFUSALS),
        }
    }
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
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{url}: response has no header end"))?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "{url}: {}",
            head.lines().next().unwrap_or_default()
        ));
    }
    Ok(parse_prometheus_samples(body))
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use zinder_bench::metrics_scrape::parse_prometheus_samples;

    use super::{ServerObserved, scrape};

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
}
