//! Whether the writer's node vouches for the tip this process serves.
//!
//! A served tip can be old for two very different reasons. The network may
//! simply not have produced a block, in which case the tip is still the chain
//! tip and wallets should keep using it. Or the served chain may have stopped
//! while the network moved on: the node is unreachable, or the writer is
//! behind its node, or the node itself is behind the network. The stale-tip
//! limit refuses only the second kind, so the process reads the writer's
//! `WriterStatus` from ingest control, which reports the node's committed tip
//! while the writer reaches its node, omits it while the node is unreachable,
//! and carries `upstream_not_ready` while the node reports itself behind.

use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, Network, wire::encode_zinder_native_chain_name};
use zinder_proto::v1::{
    ingest::{
        WriterStatusRequest, WriterStatusResponse, ingest_control_client::IngestControlClient,
    },
    ops::UpstreamNotReadyDetail,
};
use zinder_query::wallet_ingest_control_request;
use zinder_runtime::{AuthenticatedChannel, BearerToken, connect_zinder_grpc};
use zinder_source::{
    UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS, UPSTREAM_HEALTH_REASON_NO_TIP,
    UPSTREAM_HEALTH_REASON_SYNCING, UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
};

/// Shortest time one `WriterStatus` reading vouches for a served tip.
///
/// The witness polls every storage catch-up interval, one second by default,
/// and a reading lasts at least three intervals, so only a stopped ingest
/// control lets the readings expire.
pub const NODE_TIP_OBSERVATION_MAX_AGE: Duration = Duration::from_secs(30);

/// How long a node tip keeps vouching after the node reports a newer one.
///
/// A reader serves a new block up to one catch-up interval after the writer
/// commits it, so without this grace the first block after a long gap would
/// briefly refuse the tip it replaces.
const SUPERSEDED_NODE_TIP_GRACE: Duration = Duration::from_secs(10);

/// The writer's latest report of its node's committed tip, shared by every
/// clone.
///
/// A witness that has observed nothing, or only an unreachable node, vouches
/// for no tip.
#[derive(Clone, Debug)]
pub struct NodeTipWitness {
    latest: Arc<ArcSwapOption<NodeTipObservation>>,
    max_age: Duration,
}

impl Default for NodeTipWitness {
    fn default() -> Self {
        Self {
            latest: Arc::default(),
            max_age: NODE_TIP_OBSERVATION_MAX_AGE,
        }
    }
}

/// One `WriterStatus` reading.
#[derive(Clone, Copy, Debug)]
struct NodeTipObservation {
    /// The node's committed tip, or `None` when the writer could not report
    /// one.
    node_tip_height: Option<BlockHeight>,
    observed_at: Instant,
    /// The node tip this reading replaced and when, while the node stays
    /// reachable.
    superseded: Option<(BlockHeight, Instant)>,
}

impl NodeTipWitness {
    /// Returns a witness whose readings, polled every `poll_interval`,
    /// vouch for three intervals or [`NODE_TIP_OBSERVATION_MAX_AGE`],
    /// whichever is longer.
    #[must_use]
    pub fn for_poll_interval(poll_interval: Duration) -> Self {
        Self {
            latest: Arc::default(),
            max_age: poll_interval
                .saturating_mul(3)
                .max(NODE_TIP_OBSERVATION_MAX_AGE),
        }
    }

    /// Records that the writer reported its node at `node_tip_height`, or
    /// could not report a node tip when it is `None`.
    pub fn observe(&self, node_tip_height: Option<BlockHeight>) {
        let now = Instant::now();
        let previous = self.latest.load_full();
        let superseded = node_tip_height.and_then(|height| {
            let previous = previous.as_deref()?;
            match previous.node_tip_height {
                Some(previous_height) if previous_height < height => Some((previous_height, now)),
                Some(_) => previous.superseded,
                None => None,
            }
        });
        self.latest.store(Some(Arc::new(NodeTipObservation {
            node_tip_height,
            observed_at: now,
            superseded,
        })));
    }

    /// Returns whether the latest reading, within its maximum age, shows a
    /// reachable node with no block above `served_tip_height`, or one that
    /// passed that height within the last few seconds, so the served tip is
    /// the node's tip.
    #[must_use]
    pub fn vouches_for(&self, served_tip_height: BlockHeight) -> bool {
        self.latest.load().as_deref().is_some_and(|observation| {
            if observation.observed_at.elapsed() > self.max_age {
                return false;
            }
            let Some(node_tip_height) = observation.node_tip_height else {
                return false;
            };
            node_tip_height <= served_tip_height
                || observation
                    .superseded
                    .is_some_and(|(superseded_height, superseded_at)| {
                        superseded_height <= served_tip_height
                            && superseded_at.elapsed() <= SUPERSEDED_NODE_TIP_GRACE
                    })
        })
    }
}

/// Returns the node tip a `WriterStatus` of `network` reports, or `None` when
/// the writer cannot vouch for one.
///
/// A writer that reaches its node reports the node's committed tip in
/// `chain_view.upstream_tip`. A writer whose node is unreachable reports no
/// upstream tip, a node that its peers show to be behind the network cannot
/// vouch that its tip is the network's, a parked writer no longer follows its
/// node, and a writer of another network says nothing about this one.
#[must_use]
pub fn node_tip_from_writer_status(
    writer_status: &WriterStatusResponse,
    network: Network,
) -> Option<BlockHeight> {
    if writer_status.reorg_window_exceeded.is_some()
        || writer_status
            .upstream_not_ready
            .as_ref()
            .is_some_and(node_reports_it_is_behind_its_peers)
        || writer_status.network_name != encode_zinder_native_chain_name(network)
    {
        return None;
    }
    writer_status
        .chain_view
        .as_ref()?
        .upstream_tip
        .as_ref()?
        .committed_height
        .map(BlockHeight::new)
}

/// Returns whether a node's not-ready report shows it behind its peers.
///
/// Zebra's `/ready` reports `syncing` while its syncer still downloads blocks
/// from peers, `insufficient peers` without enough of them, and `no tip`
/// before it has a chain. Its other reasons, `tip_age=…` and `lag=…`, and both
/// reasons of the JSON-RPC fallback derive the lag from how old the node's tip
/// is, so a quiet chain raises them too, and they cannot tell a stopped chain
/// from one that has produced no block.
fn node_reports_it_is_behind_its_peers(detail: &UpstreamNotReadyDetail) -> bool {
    detail.upstream_health_source == UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT
        && [
            UPSTREAM_HEALTH_REASON_SYNCING,
            UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS,
            UPSTREAM_HEALTH_REASON_NO_TIP,
        ]
        .contains(&detail.upstream_health_reason.as_str())
}

/// Starts polling the `WriterStatus` of the `network` writer every
/// `poll_interval` and returns the witness it updates.
///
/// A failed call records that no node tip could be read, so the witness stops
/// vouching at once rather than when the last reading ages out.
#[must_use]
pub fn spawn_node_tip_witness(
    ingest_control_endpoint: String,
    bearer_token: Option<BearerToken>,
    network: Network,
    poll_interval: Duration,
    cancel: CancellationToken,
) -> (NodeTipWitness, JoinHandle<()>) {
    let witness = NodeTipWitness::for_poll_interval(poll_interval);
    let handle = tokio::spawn({
        let witness = witness.clone();
        async move {
            let mut client = None;
            loop {
                let node_tip_height = tokio::select! {
                    () = cancel.cancelled() => break,
                    node_tip_height = read_node_tip(
                        &ingest_control_endpoint,
                        bearer_token.as_ref(),
                        network,
                        &mut client,
                    ) => node_tip_height,
                };
                witness.observe(node_tip_height);
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    });
    (witness, handle)
}

/// Reads one `WriterStatus`, dialing ingest control first when no channel is
/// open, and returns the node tip it reports.
async fn read_node_tip(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
    network: Network,
    client: &mut Option<IngestControlClient<AuthenticatedChannel>>,
) -> Option<BlockHeight> {
    if client.is_none() {
        *client = connect_zinder_grpc(endpoint, bearer_token)
            .await
            .ok()
            .map(IngestControlClient::new);
    }
    let writer_status = client
        .as_mut()?
        .writer_status(wallet_ingest_control_request(WriterStatusRequest {}))
        .await
        .ok()?
        .into_inner();
    node_tip_from_writer_status(&writer_status, network)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zinder_core::{BlockHeight, Network, wire::encode_zinder_native_chain_name};
    use zinder_proto::v1::{
        ingest::WriterStatusResponse,
        ops::{ReorgWindowExceededDetail, UpstreamNotReadyDetail},
        wallet::{ChainView, UpstreamTip},
    };

    use super::{
        NODE_TIP_OBSERVATION_MAX_AGE, NodeTipWitness, SUPERSEDED_NODE_TIP_GRACE,
        node_tip_from_writer_status,
    };

    const NETWORK: Network = Network::ZcashTestnet;

    fn writer_status(upstream_committed_height: Option<u32>) -> WriterStatusResponse {
        WriterStatusResponse {
            chain_view: Some(ChainView {
                upstream_tip: upstream_committed_height.map(|committed_height| UpstreamTip {
                    committed_height: Some(committed_height),
                    estimated_height: None,
                }),
                ..ChainView::default()
            }),
            network_name: encode_zinder_native_chain_name(NETWORK).to_owned(),
            ..WriterStatusResponse::default()
        }
    }

    #[test]
    fn writer_status_reports_the_node_tip_only_while_the_writer_follows_its_node() {
        assert_eq!(
            node_tip_from_writer_status(&writer_status(Some(120)), NETWORK),
            Some(BlockHeight::new(120))
        );
        // A node outage clears the writer's upstream tip.
        assert_eq!(
            node_tip_from_writer_status(&writer_status(None), NETWORK),
            None
        );
        assert_eq!(
            node_tip_from_writer_status(&WriterStatusResponse::default(), NETWORK),
            None
        );
        let parked = WriterStatusResponse {
            reorg_window_exceeded: Some(ReorgWindowExceededDetail::default()),
            ..writer_status(Some(120))
        };
        assert_eq!(node_tip_from_writer_status(&parked, NETWORK), None);
        let not_ready = |source: &str, reason: &str| WriterStatusResponse {
            upstream_not_ready: Some(UpstreamNotReadyDetail {
                upstream_health_source: source.to_owned(),
                upstream_health_reason: reason.to_owned(),
                ..UpstreamNotReadyDetail::default()
            }),
            ..writer_status(Some(120))
        };
        for reason in ["syncing", "insufficient peers", "no tip"] {
            assert_eq!(
                node_tip_from_writer_status(&not_ready("zebra_ready_endpoint", reason), NETWORK),
                None,
                "a node its peers show behind cannot vouch: {reason}"
            );
        }
        // Reasons derived from the tip's age also fire on a quiet chain.
        for (source, reason) in [
            ("zebra_ready_endpoint", "tip_age=1800s"),
            ("zebra_ready_endpoint", "lag=24 blocks"),
            (
                "verification_progress_fallback",
                "estimated_gap_above_floor",
            ),
            (
                "verification_progress_fallback",
                "verification_progress_below_floor",
            ),
        ] {
            assert_eq!(
                node_tip_from_writer_status(&not_ready(source, reason), NETWORK),
                Some(BlockHeight::new(120)),
                "a quiet chain's {reason} still vouches"
            );
        }
        assert_eq!(
            node_tip_from_writer_status(&writer_status(Some(120)), Network::ZcashMainnet),
            None,
            "a writer of another network vouches for nothing here"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_reachable_node_at_or_below_the_served_tip_vouches_for_it() {
        let witness = NodeTipWitness::default();
        let served = BlockHeight::new(120);
        assert!(!witness.vouches_for(served), "no observation yet");

        witness.observe(Some(BlockHeight::new(120)));
        assert!(witness.vouches_for(served));
        // The node is unreachable.
        witness.observe(None);
        assert!(!witness.vouches_for(served));
        // The writer is behind a node that has had a newer block for a while.
        witness.observe(Some(BlockHeight::new(125)));
        assert!(!witness.vouches_for(served));

        witness.observe(Some(BlockHeight::new(120)));
        tokio::time::sleep(NODE_TIP_OBSERVATION_MAX_AGE).await;
        assert!(witness.vouches_for(served));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !witness.vouches_for(served),
            "a reading older than the bound vouches for nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_tip_keeps_vouching_briefly_after_the_node_moves_past_it() {
        let witness = NodeTipWitness::default();
        let served = BlockHeight::new(120);
        witness.observe(Some(BlockHeight::new(120)));
        // A new block: the writer commits it before the reader serves it.
        witness.observe(Some(BlockHeight::new(121)));
        assert!(witness.vouches_for(served));
        tokio::time::sleep(SUPERSEDED_NODE_TIP_GRACE).await;
        witness.observe(Some(BlockHeight::new(121)));
        assert!(witness.vouches_for(served));
        tokio::time::sleep(Duration::from_secs(1)).await;
        witness.observe(Some(BlockHeight::new(121)));
        assert!(
            !witness.vouches_for(served),
            "a reader still behind after the grace is behind its node"
        );

        // An outage drops the grace at once.
        witness.observe(Some(BlockHeight::new(122)));
        witness.observe(None);
        witness.observe(Some(BlockHeight::new(122)));
        assert!(!witness.vouches_for(BlockHeight::new(121)));
    }

    #[test]
    fn a_long_poll_interval_lengthens_how_long_a_reading_vouches() {
        assert_eq!(
            NodeTipWitness::for_poll_interval(Duration::from_secs(1)).max_age,
            NODE_TIP_OBSERVATION_MAX_AGE
        );
        assert_eq!(
            NodeTipWitness::for_poll_interval(Duration::from_mins(1)).max_age,
            Duration::from_mins(3)
        );
    }
}
