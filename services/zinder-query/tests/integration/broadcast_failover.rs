//! Transaction broadcast through the ordered node list: failover within a
//! request, the preferred endpoint, and per-endpoint verification.

use std::time::Duration;

use eyre::eyre;
use metrics_exporter_prometheus::PrometheusBuilder;
use zebra_chain::{serialization::ZcashDeserializeInto as _, transaction::Transaction};
use zinder_core::{
    BlockHeight, BroadcastAccepted, BroadcastDuplicate, BroadcastQueued, BroadcastRejected,
    BroadcastRejectionReason, ConsensusBranchId, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations, RawTransactionBytes, TransactionBroadcastOutcome, TransactionId,
};
use zinder_query::{
    OrderedBroadcaster, QueryError, WalletQueryApi, WalletServingPairSlot, WalletServingQuery,
};
use zinder_source::{SourceError, TransactionBroadcaster as _};
use zinder_testkit::{
    ChainFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

use crate::common::serving_pair_slot;

type BroadcastQuery = WalletServingQuery<Option<OrderedBroadcaster<MockTransactionBroadcaster>>>;

/// A query broadcasting through `endpoints` in order, over a one-block
/// regtest pair.
struct Endpoints {
    query: BroadcastQuery,
    nodes: OrderedBroadcaster<MockTransactionBroadcaster>,
    slot: WalletServingPairSlot,
    _store: WalletServingStoreFixture,
}

impl Endpoints {
    /// Builds the query without verifying any endpoint.
    fn unverified(endpoints: Vec<MockTransactionBroadcaster>) -> eyre::Result<Self> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let mut store =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let slot = serving_pair_slot(&mut store)?;
        let mut endpoints = endpoints.into_iter();
        let primary = endpoints
            .next()
            .ok_or_else(|| eyre!("an ordered broadcaster needs a primary endpoint"))?;
        let nodes = OrderedBroadcaster::new(primary, endpoints);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone())
            .with_node_broadcast(Some(nodes.clone()));
        Ok(Self {
            query,
            nodes,
            slot,
            _store: store,
        })
    }

    /// Builds the query and verifies every endpoint once.
    async fn verified(endpoints: Vec<MockTransactionBroadcaster>) -> eyre::Result<Self> {
        let endpoints = Self::unverified(endpoints)?;
        endpoints.nodes.verify(&endpoints.slot).await?;
        Ok(endpoints)
    }

    async fn broadcast(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        self.query.broadcast_transaction(raw_transaction).await
    }
}

/// A node endpoint whose table matches the regtest store.
fn regtest_node(broadcaster: MockTransactionBroadcaster) -> MockTransactionBroadcaster {
    broadcaster.with_node_activations(sample_regtest_upgrade_activations())
}

fn accepting_node(byte: u8) -> MockTransactionBroadcaster {
    regtest_node(MockTransactionBroadcaster::accepted(
        TransactionId::from_bytes([byte; 32]),
    ))
}

fn unreachable_node() -> MockTransactionBroadcaster {
    regtest_node(MockTransactionBroadcaster::node_unavailable(
        "sendrawtransaction timed out after 5000ms",
    ))
}

fn node_answering(outcome: TransactionBroadcastOutcome) -> MockTransactionBroadcaster {
    regtest_node(MockTransactionBroadcaster::returning(outcome))
}

fn consensus_branch_rejection() -> TransactionBroadcastOutcome {
    TransactionBroadcastOutcome::Rejected(BroadcastRejected {
        kind: BroadcastRejectionReason::BadConsensusBranch,
        error_code: Some(-26),
        message: "transaction has an incorrect consensus branch id".to_owned(),
    })
}

fn duplicate_answer() -> TransactionBroadcastOutcome {
    TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
        error_code: Some(-27),
        message: "transaction already exists in mempool".to_owned(),
    })
}

/// A regtest table whose every activation is one block later than the
/// store's, as a node on another network would report.
fn another_schedule() -> eyre::Result<NetworkUpgradeActivations> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .map(|activation| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(activation.activation_height.value() + 1),
                ..activation.clone()
            })
            .collect(),
    )?)
}

/// The store's regtest table with one more upgrade, activating at 1,000, as
/// a writer admits after its node learns a future upgrade.
fn schedule_with_a_future_upgrade() -> eyre::Result<NetworkUpgradeActivations> {
    let mut entries = sample_regtest_upgrade_activations().activations().to_vec();
    entries.push(NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(0x5a1d_e008),
        activation_height: BlockHeight::new(1_000),
        name: "NU8".to_owned(),
    });
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        entries,
    )?)
}

/// Two regtest serving pairs whose broadcast heights straddle an activation.
///
/// NU6.1 and NU6.2 activate at height 3, which a broadcast targets once the
/// served tip reaches height 2, so the pairs differ in verification basis.
struct ActivationCrossing {
    /// Serves a tip at height 1.
    before_slot: WalletServingPairSlot,
    /// Serves a tip at height 2.
    after_slot: WalletServingPairSlot,
    _stores: [WalletServingStoreFixture; 2],
}

impl ActivationCrossing {
    fn new() -> eyre::Result<Self> {
        let schedule = sample_regtest_upgrade_activations();
        let mut before_store = WalletServingStoreFixture::from_chain(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
            &schedule,
        )?;
        let mut after_store = WalletServingStoreFixture::from_chain(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
            &schedule,
        )?;
        Ok(Self {
            before_slot: serving_pair_slot(&mut before_store)?,
            after_slot: serving_pair_slot(&mut after_store)?,
            _stores: [before_store, after_store],
        })
    }
}

/// Serialized bytes of a transaction this build decodes, with the id the
/// consensus parser computes for them.
///
/// The bytes are a version 4 (Sapling) transaction with no inputs, outputs,
/// or shielded data.
fn decodable_transaction() -> eyre::Result<(RawTransactionBytes, TransactionId)> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0x8000_0004_u32.to_le_bytes());
    bytes.extend_from_slice(&0x892f_2085_u32.to_le_bytes());
    // No transparent inputs or outputs.
    bytes.extend_from_slice(&[0, 0]);
    // Lock time and expiry height.
    bytes.extend_from_slice(&[0; 8]);
    // Sapling value balance.
    bytes.extend_from_slice(&[0; 8]);
    // No Sapling spends, Sapling outputs, or JoinSplits.
    bytes.extend_from_slice(&[0, 0, 0]);
    let transaction: Transaction = bytes.as_slice().zcash_deserialize_into()?;
    Ok((
        RawTransactionBytes::new(bytes),
        TransactionId::from_bytes(transaction.hash().0),
    ))
}

#[tokio::test]
async fn broadcast_fails_over_to_the_next_endpoint_when_the_first_is_unreachable()
-> eyre::Result<()> {
    let primary = unreachable_node();
    let fallback = accepting_node(0x11);
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;

    let outcome = endpoints
        .broadcast(RawTransactionBytes::new([0x01]))
        .await?;

    assert_eq!(
        outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: TransactionId::from_bytes([0x11; 32]),
        })
    );
    assert_eq!(primary.call_count(), 1);
    assert_eq!(fallback.call_count(), 1);
    Ok(())
}

#[tokio::test]
async fn broadcast_returns_a_node_answer_without_trying_the_next_endpoint() -> eyre::Result<()> {
    let answers = [
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::InvalidSignature,
            error_code: Some(-25),
            message: "transaction signature is invalid".to_owned(),
        }),
        duplicate_answer(),
    ];
    for answer in answers {
        let fallback = accepting_node(0x22);
        let endpoints =
            Endpoints::verified(vec![node_answering(answer.clone()), fallback.clone()]).await?;

        assert_eq!(
            endpoints
                .broadcast(RawTransactionBytes::new([0x02]))
                .await?,
            answer
        );
        assert_eq!(fallback.call_count(), 0, "{answer:?} must not fail over");
    }
    Ok(())
}

/// The endpoint that answered stays preferred, so a hung primary costs a
/// timeout only for the requests that started before the first failover.
#[tokio::test]
async fn broadcast_prefers_the_endpoint_that_last_answered() -> eyre::Result<()> {
    let primary = unreachable_node();
    let fallback = accepting_node(0x33);
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;
    assert_eq!(endpoints.nodes.preferred_position(), 0);

    endpoints
        .broadcast(RawTransactionBytes::new([0x03]))
        .await?;
    endpoints
        .broadcast(RawTransactionBytes::new([0x04]))
        .await?;

    assert_eq!(endpoints.nodes.preferred_position(), 1);
    assert_eq!(primary.call_count(), 1);
    assert_eq!(fallback.call_count(), 2);
    Ok(())
}

#[tokio::test]
async fn broadcast_reports_node_unavailable_after_every_endpoint_fails() -> eyre::Result<()> {
    let primary = unreachable_node();
    let fallback = regtest_node(MockTransactionBroadcaster::node_unavailable(
        "fallback connection refused",
    ));
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;

    let error = endpoints
        .broadcast(RawTransactionBytes::new([0x05]))
        .await
        .err()
        .ok_or_else(|| eyre!("a broadcast no endpoint answered must fail"))?;

    assert!(
        matches!(
            error,
            QueryError::Node(SourceError::NodeUnavailable { ref reason })
                if reason == "fallback connection refused"
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(primary.call_count(), 1);
    assert_eq!(fallback.call_count(), 1);
    assert_eq!(endpoints.nodes.preferred_position(), 0);
    Ok(())
}

/// A node left on an older release after an upgrade activates refuses every
/// transaction for its consensus branch, so the broadcast moves on.
#[tokio::test]
async fn broadcast_fails_over_on_a_consensus_branch_rejection() -> eyre::Result<()> {
    let primary = node_answering(consensus_branch_rejection());
    let fallback = accepting_node(0x44);
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;

    let outcome = endpoints
        .broadcast(RawTransactionBytes::new([0x06]))
        .await?;

    assert_eq!(
        outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: TransactionId::from_bytes([0x44; 32]),
        })
    );
    assert_eq!(primary.call_count(), 1);
    assert_eq!(endpoints.nodes.preferred_position(), 1);
    Ok(())
}

/// A transaction built for the wrong branch is refused by every endpoint, so
/// the refusal reaches the wallet and the preferred endpoint stays put.
#[tokio::test]
async fn consensus_branch_rejection_from_every_endpoint_is_returned() -> eyre::Result<()> {
    let primary = node_answering(consensus_branch_rejection());
    let fallback = node_answering(consensus_branch_rejection());
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;

    let outcome = endpoints
        .broadcast(RawTransactionBytes::new([0x07]))
        .await?;

    assert_eq!(outcome, consensus_branch_rejection());
    assert_eq!(primary.call_count(), 1);
    assert_eq!(fallback.call_count(), 1);
    assert_eq!(endpoints.nodes.preferred_position(), 0);
    Ok(())
}

/// An unreachable endpoint may have accepted the transaction before it
/// failed, so a later endpoint's duplicate or queued answer means success.
///
/// Lightwalletd clients would otherwise read error codes -27 and -25 as a
/// failed send.
#[tokio::test]
async fn duplicate_after_an_unreachable_endpoint_is_reported_accepted() -> eyre::Result<()> {
    let (raw_transaction, transaction_id) = decodable_transaction()?;
    let later_answers = [
        duplicate_answer(),
        TransactionBroadcastOutcome::Queued(BroadcastQueued {
            message: "transaction was already queued for download".to_owned(),
        }),
    ];
    for later_answer in later_answers {
        let endpoints = Endpoints::verified(vec![
            unreachable_node(),
            node_answering(later_answer.clone()),
        ])
        .await?;

        assert_eq!(
            endpoints.broadcast(raw_transaction.clone()).await?,
            TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id }),
            "{later_answer:?} after an unreachable endpoint must report acceptance"
        );
    }
    Ok(())
}

/// Without an id to report, the node's own answer is returned unchanged.
#[tokio::test]
async fn duplicate_of_undecodable_bytes_after_an_unreachable_endpoint_stays_duplicate()
-> eyre::Result<()> {
    let endpoints =
        Endpoints::verified(vec![unreachable_node(), node_answering(duplicate_answer())]).await?;

    assert_eq!(
        endpoints
            .broadcast(RawTransactionBytes::new([0x08]))
            .await?,
        duplicate_answer()
    );
    Ok(())
}

#[tokio::test]
async fn endpoint_with_another_schedule_is_excluded_and_receives_nothing() -> eyre::Result<()> {
    let primary = accepting_node(0x55).with_node_activations(another_schedule()?);
    let fallback = accepting_node(0x66);
    let endpoints = Endpoints::verified(vec![primary.clone(), fallback.clone()]).await?;

    let outcome = endpoints
        .broadcast(RawTransactionBytes::new([0x09]))
        .await?;
    endpoints.nodes.verify(&endpoints.slot).await?;

    assert_eq!(
        outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: TransactionId::from_bytes([0x66; 32]),
        })
    );
    assert_eq!(primary.call_count(), 0);
    assert_eq!(
        primary.capability_probe_count(),
        1,
        "an excluded endpoint is not verified again before its backoff passes"
    );
    assert_eq!(
        endpoints.nodes.preferred_position(),
        0,
        "skipping an excluded endpoint does not move the preference"
    );
    Ok(())
}

/// An exclusion ends after a backoff that doubles while the endpoint keeps
/// answering with another schedule, so a node fixed by its operator
/// receives transactions again without a reader restart.
#[tokio::test(start_paused = true)]
async fn excluded_endpoint_is_verified_again_after_its_backoff() -> eyre::Result<()> {
    let node = accepting_node(0x77).with_node_activations(another_schedule()?);
    let endpoints = Endpoints::unverified(vec![node.clone()])?;
    let refused = |verification: Result<(), SourceError>| {
        matches!(
            verification,
            Err(SourceError::SourceProtocolMismatch { .. })
        )
    };

    assert!(refused(endpoints.nodes.verify(&endpoints.slot).await));
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(refused(endpoints.nodes.verify(&endpoints.slot).await));
    assert_eq!(node.capability_probe_count(), 2);

    node.report_node_activations(sample_regtest_upgrade_activations());
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(refused(endpoints.nodes.verify(&endpoints.slot).await));
    assert_eq!(
        node.capability_probe_count(),
        2,
        "the second exclusion lasts twice as long as the first"
    );
    assert!(
        endpoints
            .broadcast(RawTransactionBytes::new([0x0a]))
            .await
            .is_err()
    );
    assert_eq!(node.call_count(), 0);

    tokio::time::advance(Duration::from_secs(30)).await;
    endpoints.nodes.verify(&endpoints.slot).await?;
    assert_eq!(node.capability_probe_count(), 3);
    endpoints
        .broadcast(RawTransactionBytes::new([0x0b]))
        .await?;
    assert_eq!(node.call_count(), 1);
    Ok(())
}

/// A best chain that holds another block at the served tip can be a reorg the
/// pair has not published yet, so the endpoint is checked again on the next
/// round instead of being excluded.
#[tokio::test]
async fn endpoint_on_another_chain_is_verified_again_on_the_next_round() -> eyre::Result<()> {
    let node = accepting_node(0x88).on_another_chain();
    let endpoints = Endpoints::unverified(vec![node.clone()])?;

    for round in 1..=2 {
        assert!(endpoints.nodes.verify(&endpoints.slot).await.is_err());
        assert_eq!(node.capability_probe_count(), round);
    }
    assert_eq!(node.call_count(), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_verifications_of_an_endpoint_share_one_probe() -> eyre::Result<()> {
    let node = accepting_node(0x99);
    let endpoints = Endpoints::unverified(vec![node.clone()])?;

    let (first, second, third) = tokio::join!(
        endpoints.nodes.verify(&endpoints.slot),
        endpoints.nodes.verify(&endpoints.slot),
        endpoints.nodes.verify(&endpoints.slot),
    );

    first?;
    second?;
    third?;
    assert_eq!(node.capability_probe_count(), 1);
    Ok(())
}

/// A verification holds against the writer-admitted schedule revision it
/// checked, so the endpoint is verified again once the writer admits a new
/// revision.
#[tokio::test]
async fn endpoint_is_verified_again_after_a_schedule_revision() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let revised = schedule_with_a_future_upgrade()?;
    let mut first_store = WalletServingStoreFixture::from_chain_after_live_append(
        &chain,
        &sample_regtest_upgrade_activations(),
    )?;
    let mut revised_store = WalletServingStoreFixture::from_chain_after_admitted_live_append(
        &chain,
        &sample_regtest_upgrade_activations(),
        &revised,
    )?;
    let first_slot = serving_pair_slot(&mut first_store)?;
    let revised_slot = serving_pair_slot(&mut revised_store)?;
    let node = accepting_node(0xaa).with_node_activations(revised);
    let nodes = OrderedBroadcaster::new(node.clone(), []);

    nodes.verify(&first_slot).await?;
    nodes.verify(&first_slot).await?;
    assert_eq!(node.capability_probe_count(), 1);
    nodes.verify(&revised_slot).await?;
    assert_eq!(node.capability_probe_count(), 2);
    Ok(())
}

/// An endpoint is verified again once the block a broadcast targets crosses
/// an activation.
///
/// A verification checked the node's table only through the height after the
/// tip it saw.
#[tokio::test]
async fn endpoint_is_verified_again_when_the_tip_crosses_an_activation() -> eyre::Result<()> {
    let crossing = ActivationCrossing::new()?;
    let node = accepting_node(0xbb);
    let nodes = OrderedBroadcaster::new(node.clone(), []);

    nodes.verify(&crossing.before_slot).await?;
    assert_eq!(node.capability_probe_count(), 1);
    nodes.verify(&crossing.after_slot).await?;
    assert_eq!(node.capability_probe_count(), 2);
    Ok(())
}

/// An endpoint that timed out may have accepted the transaction, so a
/// consensus-branch rejection from another endpoint is not reported as the
/// outcome, in either order.
#[tokio::test]
async fn unreachable_endpoint_outweighs_a_consensus_branch_rejection() -> eyre::Result<()> {
    let orders = [
        vec![
            unreachable_node(),
            node_answering(consensus_branch_rejection()),
        ],
        vec![
            node_answering(consensus_branch_rejection()),
            unreachable_node(),
        ],
    ];
    for order in orders {
        let endpoints = Endpoints::verified(order).await?;

        let outcome = endpoints.broadcast(RawTransactionBytes::new([0x0c])).await;

        assert!(
            matches!(
                outcome,
                Err(QueryError::Node(SourceError::NodeUnavailable { .. }))
            ),
            "unexpected outcome: {outcome:?}"
        );
        assert_eq!(endpoints.nodes.preferred_position(), 0);
    }
    Ok(())
}

/// An endpoint skipped only because it is not verified yet, as `[node]` is
/// when the fallback's verification finishes first at startup, keeps its
/// place as the preferred endpoint.
#[tokio::test]
async fn skipping_an_unverified_endpoint_keeps_the_preference() -> eyre::Result<()> {
    let primary = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0xcc; 32]));
    let fallback = accepting_node(0xdd);
    let endpoints = Endpoints::unverified(vec![primary.clone(), fallback.clone()])?;
    endpoints.nodes.verify(&endpoints.slot).await?;

    endpoints
        .broadcast(RawTransactionBytes::new([0x0d]))
        .await?;
    assert_eq!(fallback.call_count(), 1);
    assert_eq!(endpoints.nodes.preferred_position(), 0);

    primary.report_node_activations(sample_regtest_upgrade_activations());
    endpoints.nodes.verify(&endpoints.slot).await?;
    endpoints
        .broadcast(RawTransactionBytes::new([0x0e]))
        .await?;
    assert_eq!(primary.call_count(), 1);
    assert_eq!(fallback.call_count(), 1);
    Ok(())
}

/// A node that cannot be verified against a new basis yet keeps its earlier
/// verification for a grace period.
///
/// A node that has not received the block that crossed an activation would
/// otherwise lose broadcasts at the upgrade boundary.
#[tokio::test(start_paused = true)]
async fn inconclusive_verification_after_a_basis_change_keeps_the_earlier_one() -> eyre::Result<()>
{
    let crossing = ActivationCrossing::new()?;
    let node = accepting_node(0xee);
    let nodes = OrderedBroadcaster::new(node.clone(), []);
    nodes.verify(&crossing.before_slot).await?;

    node.report_another_chain();
    nodes.verify(&crossing.after_slot).await?;
    nodes
        .broadcast_transaction(RawTransactionBytes::new([0x0f]))
        .await?;
    assert_eq!(node.call_count(), 1);
    assert!(nodes.verified_capabilities().is_some());

    tokio::time::advance(Duration::from_secs(61)).await;
    assert!(nodes.verify(&crossing.after_slot).await.is_err());
    assert!(
        nodes
            .broadcast_transaction(RawTransactionBytes::new([0x10]))
            .await
            .is_err()
    );
    assert_eq!(node.call_count(), 1);
    assert!(nodes.verified_capabilities().is_none());
    Ok(())
}

/// The grace of an earlier verification ends on time even when no later
/// verification round completes, as when the round waits on a slow node.
#[tokio::test(start_paused = true)]
async fn earlier_verification_ends_with_its_grace_without_another_round() -> eyre::Result<()> {
    let crossing = ActivationCrossing::new()?;
    let node = accepting_node(0x13);
    let nodes = OrderedBroadcaster::new(node.clone(), []);
    nodes.verify(&crossing.before_slot).await?;
    node.report_another_chain();
    nodes.verify(&crossing.after_slot).await?;

    tokio::time::advance(Duration::from_secs(61)).await;

    assert!(
        nodes
            .broadcast_transaction(RawTransactionBytes::new([0x13]))
            .await
            .is_err()
    );
    assert_eq!(node.call_count(), 0);
    assert!(nodes.verified_capabilities().is_none());
    Ok(())
}

/// An earlier verification kept after an inconclusive repeat is checked
/// again when the pair returns to its basis, as after a reorg back below the
/// activation, so the return does not extend its grace.
#[tokio::test(start_paused = true)]
async fn returning_to_the_earlier_basis_does_not_extend_its_grace() -> eyre::Result<()> {
    let crossing = ActivationCrossing::new()?;
    let node = accepting_node(0x14);
    let nodes = OrderedBroadcaster::new(node.clone(), []);
    nodes.verify(&crossing.before_slot).await?;
    node.report_another_chain();
    nodes.verify(&crossing.after_slot).await?;

    tokio::time::advance(Duration::from_secs(61)).await;

    assert!(nodes.verify(&crossing.before_slot).await.is_err());
    assert_eq!(node.capability_probe_count(), 3);
    assert!(nodes.verified_capabilities().is_none());
    Ok(())
}

/// A broadcast transaction targets the block after the served tip, so a node
/// whose table disagrees only at that height is excluded before the tip
/// reaches it.
#[tokio::test]
async fn endpoint_whose_table_disagrees_at_the_next_height_is_excluded() -> eyre::Result<()> {
    let delayed_nu5 = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .map(|activation| NetworkUpgradeActivation {
                activation_height: if activation.activation_height.value() == 2 {
                    BlockHeight::new(3)
                } else {
                    activation.activation_height
                },
                ..activation.clone()
            })
            .collect(),
    )?;
    let node = accepting_node(0xff).with_node_activations(delayed_nu5);
    let endpoints = Endpoints::unverified(vec![node.clone()])?;

    let verification = endpoints.nodes.verify(&endpoints.slot).await;

    assert!(
        matches!(
            verification,
            Err(SourceError::SourceProtocolMismatch { .. })
        ),
        "unexpected verification: {verification:?}"
    );
    Ok(())
}

#[tokio::test]
async fn broadcast_failover_records_endpoint_metrics() -> eyre::Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let endpoints = Endpoints::verified(vec![unreachable_node(), accepting_node(0x12)]).await?;

    endpoints
        .broadcast(RawTransactionBytes::new([0x11]))
        .await?;

    let rendered = recorded_metrics.render();
    for expected in [
        "zinder_wallet_broadcast_endpoint_attempts_total{position=\"0\",outcome=\"node_unreachable\"} 1",
        "zinder_wallet_broadcast_endpoint_attempts_total{position=\"1\",outcome=\"accepted\"} 1",
        "zinder_wallet_broadcast_endpoint_switches_total{from_position=\"0\",to_position=\"1\",cause=\"node_unreachable\"} 1",
        "zinder_wallet_broadcast_endpoint_state{position=\"1\",state=\"verified\"} 1",
        "zinder_wallet_broadcast_endpoint_state{position=\"1\",state=\"unverified\"} 0",
    ] {
        assert!(
            rendered.lines().any(|line| line == expected),
            "missing {expected}:\n{rendered}"
        );
    }
    Ok(())
}
