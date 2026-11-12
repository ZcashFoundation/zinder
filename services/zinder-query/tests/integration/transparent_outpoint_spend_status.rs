#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Wallet-projection spend status for transparent outpoints.
//!
//! These reads back Zallet's `spend-index` contract
//! (`ChainView::outpoint_spend_status`) and librustzcash
//! `TransactionDataRequest::GetSpendingTx`.

use eyre::eyre;
use tonic::{Code, Request};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHeight, BlockId, MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST, Network, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentOutpointSpendStatus,
    TransparentSpendFact, TransparentUnspentOutput,
};
use zinder_proto::capabilities::WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1;
use zinder_proto::v1::{
    ops,
    wallet::{self, wallet_query_server::WalletQuery as WalletQueryService},
};
use zinder_proto::wire::{outpoint_message, transparent_outpoint_spend_status_entry_from_message};
use zinder_query::{QueryError, WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    sample_regtest_upgrade_activations,
};

use crate::common::{native_serving_query_over, serving_query_from, serving_query_over};

const SCRIPT_PUB_KEY: &[u8] = &[0x51];

/// Outputs created in block 1 and the block-2 transaction that spends two of
/// them, at input indexes 0 and 1.
struct SpendChain {
    chain: ChainFixture,
    unspent: TransparentOutPoint,
    spent_by_input_zero: TransparentOutPoint,
    spent_by_input_one: TransparentOutPoint,
    created_in_block_two: TransparentOutPoint,
    spender: TransactionId,
    spend_block: BlockId,
}

fn spend_chain() -> eyre::Result<SpendChain> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let block_one = chain
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let block_two = chain
        .block_at(BlockHeight::new(2))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 2"))?;
    let address = TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY);
    let creator = TransactionId::from_bytes([0x10; 32]);
    let spender = TransactionId::from_bytes([0x20; 32]);
    let unspent = TransparentOutPoint::new(creator, 0);
    let spent_by_input_zero = TransparentOutPoint::new(creator, 1);
    let spent_by_input_one = TransparentOutPoint::new(creator, 2);
    let created_in_block_two = TransparentOutPoint::new(spender, 0);
    let output = |outpoint: TransparentOutPoint, block: &zinder_testkit::FixtureBlock| {
        TransparentUnspentOutput::new(
            address,
            SCRIPT_PUB_KEY.to_vec(),
            outpoint,
            1_000,
            block.height,
            block.hash,
        )
    };
    let spend = |spent_outpoint: TransparentOutPoint, input_index: u32| {
        TransparentSpendFact::new(
            spent_outpoint,
            input_index,
            spender,
            1,
            block_two.height,
            block_two.hash,
            1_000,
            address,
            block_one.height,
            block_one.hash,
        )
    };
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes([0x2C; 32]),
            block_two.height,
            block_two.hash,
            b"block-two-coinbase".to_vec(),
        ))
        .with_address_output_index(output(unspent, &block_one))
        .with_address_output_index(output(spent_by_input_zero, &block_one))
        .with_address_output_index(output(spent_by_input_one, &block_one))
        .with_address_output_index(output(created_in_block_two, &block_two))
        .with_transparent_spend_fact(spend(spent_by_input_zero, 0))
        .with_transparent_spend_fact(spend(spent_by_input_one, 1));
    Ok(SpendChain {
        chain,
        unspent,
        spent_by_input_zero,
        spent_by_input_one,
        created_in_block_two,
        spender,
        spend_block: BlockId::new(block_two.height, block_two.hash),
    })
}

fn spent_by(spend_chain: &SpendChain, input_index: u32) -> TransparentOutpointSpendStatus {
    TransparentOutpointSpendStatus::SpentBy {
        transaction_id: spend_chain.spender,
        input_index,
        block: spend_chain.spend_block,
    }
}

#[tokio::test]
async fn mixed_batch_reports_each_status_in_request_order() -> eyre::Result<()> {
    let spend_chain = spend_chain()?;
    let (wallet_query, _store_fixture) = serving_query_over(&spend_chain.chain)?;
    let visible_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    let never_created = TransparentOutPoint::new(TransactionId::from_bytes([0x99; 32]), 0);

    let response = wallet_query
        .transparent_outpoint_spend_status(
            vec![
                never_created,
                spend_chain.spent_by_input_one,
                spend_chain.unspent,
                spend_chain.spent_by_input_zero,
                spend_chain.created_in_block_two,
                spend_chain.spent_by_input_one,
                TransparentOutPoint::COINBASE_SENTINEL,
            ],
            None,
        )
        .await?;

    assert_eq!(response.chain_epoch, visible_epoch);
    assert_eq!(
        response.statuses,
        vec![
            TransparentOutpointSpendStatus::NotFound,
            spent_by(&spend_chain, 1),
            TransparentOutpointSpendStatus::Unspent,
            spent_by(&spend_chain, 0),
            TransparentOutpointSpendStatus::Unspent,
            spent_by(&spend_chain, 1),
            TransparentOutpointSpendStatus::NotFound,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn outpoint_count_must_be_between_one_and_the_request_cap() -> eyre::Result<()> {
    let spend_chain = spend_chain()?;
    let (wallet_query, _store_fixture) = serving_query_over(&spend_chain.chain)?;

    let empty = wallet_query
        .transparent_outpoint_spend_status(Vec::new(), None)
        .await;
    assert!(matches!(
        empty,
        Err(QueryError::TransparentOutpointCountOutOfRange {
            requested: 0,
            maximum: MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST,
        })
    ));

    let over_cap = MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST + 1;
    let oversized = wallet_query
        .transparent_outpoint_spend_status(vec![spend_chain.unspent; over_cap], None)
        .await;
    assert!(matches!(
        oversized,
        Err(QueryError::TransparentOutpointCountOutOfRange { requested, .. }) if requested == over_cap
    ));

    let at_cap = wallet_query
        .transparent_outpoint_spend_status(
            vec![spend_chain.unspent; MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST],
            None,
        )
        .await?;
    assert_eq!(
        at_cap.statuses.len(),
        MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST
    );
    assert!(
        at_cap
            .statuses
            .iter()
            .all(|status| *status == TransparentOutpointSpendStatus::Unspent)
    );
    Ok(())
}

#[tokio::test]
async fn pin_to_an_epoch_the_pair_does_not_serve_is_refused() -> eyre::Result<()> {
    let spend_chain = spend_chain()?;
    let (wallet_query, _store_fixture) = serving_query_over(&spend_chain.chain)?;
    let visible_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let pinned = wallet_query
        .transparent_outpoint_spend_status(vec![spend_chain.unspent], Some(visible_epoch.id))
        .await?;
    assert_eq!(pinned.chain_epoch, visible_epoch);
    assert_eq!(
        pinned.statuses,
        vec![TransparentOutpointSpendStatus::Unspent]
    );

    let other_epoch = zinder_core::ChainEpochId::new(visible_epoch.id.value() + 1);
    let refused = wallet_query
        .transparent_outpoint_spend_status(vec![spend_chain.unspent], Some(other_epoch))
        .await;
    assert!(matches!(
        refused,
        Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == other_epoch
    ));
    Ok(())
}

#[tokio::test]
async fn reorg_that_drops_the_spending_block_reports_the_outputs_again_unspent() -> eyre::Result<()>
{
    let spend_chain = spend_chain()?;
    let fork_height = BlockHeight::new(2);
    let replacement_chain = spend_chain.chain.fork_at(fork_height)?.extend_blocks(1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&spend_chain.chain).with_settled_tip(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let outpoints = vec![
        spend_chain.spent_by_input_zero,
        spend_chain.spent_by_input_one,
        spend_chain.created_in_block_two,
        spend_chain.unspent,
    ];

    let (before_query, _before_store) = serving_query_from(writer.serving_store_fixture()?)?;
    let before = before_query
        .transparent_outpoint_spend_status(outpoints.clone(), None)
        .await?;
    assert_eq!(
        before.statuses,
        vec![
            spent_by(&spend_chain, 0),
            spent_by(&spend_chain, 1),
            TransparentOutpointSpendStatus::Unspent,
            TransparentOutpointSpendStatus::Unspent,
        ]
    );

    writer.replace_live_blocks(&replacement_chain, fork_height)?;
    let (after_query, _after_store) = serving_query_from(writer.serving_store_fixture()?)?;
    let after = after_query
        .transparent_outpoint_spend_status(outpoints.clone(), None)
        .await?;

    assert_ne!(after.chain_epoch.id, before.chain_epoch.id);
    assert_eq!(
        after.statuses,
        vec![
            TransparentOutpointSpendStatus::Unspent,
            TransparentOutpointSpendStatus::Unspent,
            TransparentOutpointSpendStatus::NotFound,
            TransparentOutpointSpendStatus::Unspent,
        ]
    );
    let stale_pin = after_query
        .transparent_outpoint_spend_status(outpoints, Some(before.chain_epoch.id))
        .await;
    assert!(matches!(
        stale_pin,
        Err(QueryError::ChainEpochPinUnavailable { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn native_endpoint_advertises_and_serves_spend_status_over_grpc() -> eyre::Result<()> {
    let spend_chain = spend_chain()?;
    let (query, _store_fixture, _ingest_control_fixture) =
        native_serving_query_over(&spend_chain.chain).await?;
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let server_info =
        WalletQueryService::server_info(&adapter, Request::new(wallet::ServerInfoRequest {}))
            .await?
            .into_inner()
            .info
            .and_then(|info| info.common)
            .ok_or_else(|| eyre!("server info omitted its common metadata"))?;
    assert!(
        server_info
            .capabilities
            .iter()
            .any(|capability| capability == WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1)
    );
    let outpoints = [spend_chain.spent_by_input_one, spend_chain.unspent];

    let response = WalletQueryService::transparent_outpoint_spend_status(
        &adapter,
        Request::new(wallet::TransparentOutpointSpendStatusRequest {
            outpoints: outpoints.iter().map(outpoint_message).collect(),
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();

    assert!(
        response
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .is_some()
    );
    let entries = response
        .entries
        .iter()
        .map(transparent_outpoint_spend_status_entry_from_message)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        entries,
        vec![
            (spend_chain.spent_by_input_one, spent_by(&spend_chain, 1)),
            (spend_chain.unspent, TransparentOutpointSpendStatus::Unspent),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn native_endpoint_rejects_invalid_outpoint_requests_as_invalid_argument() -> eyre::Result<()>
{
    let spend_chain = spend_chain()?;
    let (query, _store_fixture, _ingest_control_fixture) =
        native_serving_query_over(&spend_chain.chain).await?;
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());

    let Err(oversized) = WalletQueryService::transparent_outpoint_spend_status(
        &adapter,
        Request::new(wallet::TransparentOutpointSpendStatusRequest {
            outpoints: vec![
                outpoint_message(&spend_chain.unspent);
                MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST + 1
            ],
            at_epoch_id: None,
        }),
    )
    .await
    else {
        return Err(eyre!("an over-cap request must be rejected"));
    };
    assert_eq!(oversized.code(), Code::InvalidArgument);
    let details = oversized.get_error_details();
    assert_eq!(
        details
            .error_info()
            .and_then(|error_info| ops::ErrorReason::from_str_name(&error_info.reason)),
        Some(ops::ErrorReason::TransparentOutpointCountOutOfRange)
    );
    assert_eq!(
        details
            .bad_request()
            .and_then(|bad_request| bad_request.field_violations.first())
            .map(|violation| violation.field.as_str()),
        Some("outpoints")
    );

    let Err(malformed) = WalletQueryService::transparent_outpoint_spend_status(
        &adapter,
        Request::new(wallet::TransparentOutpointSpendStatusRequest {
            outpoints: vec![wallet::OutPoint {
                transaction_id: "not-hex".to_owned(),
                output_index: 0,
            }],
            at_epoch_id: None,
        }),
    )
    .await
    else {
        return Err(eyre!("a malformed transaction id must be rejected"));
    };
    assert_eq!(malformed.code(), Code::InvalidArgument);
    Ok(())
}
