//! Live contract smoke for the standalone native `WalletQuery` runtime.

#![allow(
    missing_docs,
    reason = "Live test names and assertions describe the external contract under test."
)]

use std::time::Duration;

use eyre::{Result, WrapErr, eyre};
use tokio::time::timeout;
use tokio_stream::{StreamExt as _, once};
use tonic::{Request, transport::Endpoint};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};
use zinder_proto::{
    capabilities::{
        WALLET_EVENTS_CHAIN_V1, WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
        WALLET_READ_COMPACT_BLOCK_RANGE_V2, WALLET_READ_SERVER_INFO_V2,
        WALLET_READ_SETTLED_TIP_BLOCK_V1, WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
        WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
        WALLET_READ_VISIBLE_TIP_BLOCK_V1,
    },
    v1::wallet::{self, wallet_query_client::WalletQueryClient},
    wire::{chain_epoch_from_message, compact_block_from_message},
};
use zinder_testkit::live::{init, optional_env, require_live};

const QUERY_ENDPOINT_ENV: &str = "ZINDER_TEST_QUERY_GRPC_ADDR";
const EXPECTED_SERVICE: &str = "zinder.v1.wallet.WalletQuery";
const CHAIN_STREAM_OPEN_DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn standalone_wallet_query_serves_native_contract() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let Some(configured_endpoint) = optional_env(QUERY_ENDPOINT_ENV)? else {
        return Ok(());
    };
    let endpoint = normalize_endpoint(&configured_endpoint);
    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect()
        .await
        .wrap_err_with(|| format!("connecting to standalone WalletQuery at {endpoint}"))?;

    verify_reflection(channel.clone()).await?;

    let mut client = WalletQueryClient::new(channel);
    let expected_network = expected_network_name(env.network())?;
    verify_server_info(&mut client, expected_network).await?;
    let (current_epoch_id, settled_tip) =
        resolve_pinned_settled_tip(&mut client, expected_network).await?;
    let chain_metadata = verify_compact_tip(
        &mut client,
        current_epoch_id,
        expected_network,
        &settled_tip,
    )
    .await?;
    verify_tree_state(
        &mut client,
        current_epoch_id,
        expected_network,
        &settled_tip,
        &chain_metadata,
    )
    .await?;
    verify_chain_event_stream(&mut client).await?;

    Ok(())
}

async fn verify_server_info(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
    expected_network: &str,
) -> Result<()> {
    let server_info = client
        .server_info(wallet::ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .ok_or_else(|| eyre!("WalletQuery ServerInfo response omitted info"))?;
    let common = server_info
        .common
        .ok_or_else(|| eyre!("WalletQuery ServerInfo response omitted common identity"))?;
    assert_eq!(common.service_name, "zinder-query");
    assert_eq!(common.network, expected_network);
    assert_eq!(
        common.contract_revision,
        zinder_proto::CONTRACT_REVISION,
        "live standalone query must serve the exact native contract under test"
    );
    for required_capability in [
        WALLET_READ_SERVER_INFO_V2,
        WALLET_READ_VISIBLE_TIP_BLOCK_V1,
        WALLET_READ_SETTLED_TIP_BLOCK_V1,
        WALLET_READ_COMPACT_BLOCK_RANGE_V2,
        WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
        WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
        WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
        WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
        WALLET_EVENTS_CHAIN_V1,
    ] {
        assert!(
            common
                .capabilities
                .iter()
                .any(|capability| capability == required_capability),
            "standalone WalletQuery omitted a capability required by the P1 Zallet tracer: {required_capability}"
        );
    }
    Ok(())
}

async fn resolve_pinned_settled_tip(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
    expected_network: &str,
) -> Result<(u64, wallet::BlockId)> {
    let visible_response = client
        .visible_tip_block(wallet::VisibleTipBlockRequest { at_epoch_id: None })
        .await?
        .into_inner();
    let current_epoch = visible_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("visible-tip response omitted its chain epoch"))?;
    assert_eq!(current_epoch.network_name, expected_network);
    assert!(current_epoch.chain_epoch_id > 0);
    assert!(current_epoch.artifact_schema_version > 0);
    let _decoded_epoch = chain_epoch_from_message(current_epoch.clone())?;
    let current_epoch_id = current_epoch.chain_epoch_id;
    let visible_tip = visible_response
        .visible_tip_block
        .as_ref()
        .ok_or_else(|| eyre!("visible-tip response omitted block identity"))?;
    assert!(visible_tip.height > 0, "live visible tip must be nonzero");
    let epoch_visible_tip = current_epoch
        .visible_tip
        .as_ref()
        .ok_or_else(|| eyre!("current chain epoch omitted its visible tip"))?;
    assert_eq!(epoch_visible_tip.height, visible_tip.height);
    assert_eq!(epoch_visible_tip.hash, visible_tip.block_hash);
    let epoch_settled_tip = current_epoch
        .settled_tip
        .as_ref()
        .ok_or_else(|| eyre!("current chain epoch omitted its settled tip"))?;
    assert!(epoch_settled_tip.height > 0);
    assert!(epoch_settled_tip.height <= epoch_visible_tip.height);

    let settled_response = client
        .settled_tip_block(wallet::SettledTipBlockRequest {
            at_epoch_id: Some(current_epoch_id),
        })
        .await?
        .into_inner();
    assert_chain_view_epoch(
        settled_response.chain_view.as_ref(),
        current_epoch_id,
        expected_network,
    )?;
    let settled_tip = settled_response
        .settled_tip_block
        .ok_or_else(|| eyre!("settled-tip response omitted block identity"))?;
    assert!(settled_tip.height > 0, "live settled tip must be nonzero");
    assert!(settled_tip.height <= visible_tip.height);
    assert_eq!(settled_tip.height, epoch_settled_tip.height);
    assert_eq!(settled_tip.block_hash, epoch_settled_tip.hash);
    Ok((current_epoch_id, settled_tip))
}

async fn verify_compact_tip(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
    current_epoch_id: u64,
    expected_network: &str,
    settled_tip: &wallet::BlockId,
) -> Result<wallet::CompactChainMetadata> {
    let mut compact_tip_stream = client
        .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
            start_height: settled_tip.height,
            end_height: settled_tip.height,
            at_epoch_id: Some(current_epoch_id),
        })
        .await?
        .into_inner();
    let header_chunk = compact_tip_stream
        .next()
        .await
        .ok_or_else(|| eyre!("one-block compact range closed without a header"))??;
    let Some(wallet::compact_blocks_in_range_chunk::Body::Header(header)) = header_chunk.body
    else {
        return Err(eyre!(
            "one-block compact range must open with its chain-view header"
        ));
    };
    assert_chain_view_epoch(Some(&header), current_epoch_id, expected_network)?;
    let block_chunk = compact_tip_stream
        .next()
        .await
        .ok_or_else(|| eyre!("one-block compact range closed without a block"))??;
    let Some(wallet::compact_blocks_in_range_chunk::Body::Item(compact_tip)) = block_chunk.body
    else {
        return Err(eyre!(
            "one-block compact range must follow its header with a block"
        ));
    };
    assert_eq!(compact_tip.height, settled_tip.height);
    assert_eq!(compact_tip.block_hash, settled_tip.block_hash);
    assert_eq!(compact_tip.block_hash.len(), 64);
    assert_eq!(compact_tip.previous_block_hash.len(), 64);
    assert!(compact_tip.time > 0, "compact tip must carry block time");
    let chain_metadata = compact_tip
        .chain_metadata
        .ok_or_else(|| eyre!("compact tip omitted commitment-tree metadata"))?;
    let _decoded_compact_tip = compact_block_from_message(compact_tip)?;
    assert!(compact_tip_stream.next().await.is_none());
    Ok(chain_metadata)
}

async fn verify_tree_state(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
    current_epoch_id: u64,
    expected_network: &str,
    settled_tip: &wallet::BlockId,
    chain_metadata: &wallet::CompactChainMetadata,
) -> Result<()> {
    let tree_state = client
        .tree_state_at_height(wallet::TreeStateAtHeightRequest {
            height: settled_tip.height,
            at_epoch_id: Some(current_epoch_id),
        })
        .await?
        .into_inner();
    assert_chain_view_epoch(
        tree_state.chain_view.as_ref(),
        current_epoch_id,
        expected_network,
    )?;
    assert_eq!(tree_state.height, settled_tip.height);
    assert_eq!(tree_state.block_hash, settled_tip.block_hash);
    assert!(!tree_state.payload_bytes.is_empty());
    assert!(tree_state.block_time_seconds.is_some());
    let tree_state_json: serde_json::Value = serde_json::from_slice(&tree_state.payload_bytes)
        .wrap_err("pinned tree-state payload is not JSON")?;
    for (pool, tree_size) in [
        ("sapling", chain_metadata.sapling_commitment_tree_size),
        ("orchard", chain_metadata.orchard_commitment_tree_size),
        ("ironwood", chain_metadata.ironwood_commitment_tree_size),
    ] {
        let final_state = tree_state_json
            .pointer(&format!("/{pool}/commitments/finalState"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if tree_size > 0 {
            assert!(
                !final_state.is_empty(),
                "nonempty {pool} tree must carry finalState"
            );
        }
        if !final_state.is_empty() {
            let _frontier_bytes = hex::decode(final_state)
                .wrap_err_with(|| format!("pinned {pool} finalState is not hexadecimal"))?;
        }
    }
    Ok(())
}

async fn verify_chain_event_stream(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
) -> Result<()> {
    let request = wallet::ChainEventsRequest {
        start: Some(wallet::EventStreamStart {
            position: Some(wallet::event_stream_start::Position::LiveTail(
                wallet::LiveTail {},
            )),
        }),
        family: wallet::ChainEventStreamFamily::Visible as i32,
        address_filter: Vec::new(),
    };
    let chain_events = timeout(CHAIN_STREAM_OPEN_DEADLINE, client.chain_events(request))
        .await
        .wrap_err("standalone WalletQuery did not open the chain-event stream in time")??;
    drop(chain_events.into_inner());
    Ok(())
}

async fn verify_reflection(channel: tonic::transport::Channel) -> Result<()> {
    let mut reflection = ServerReflectionClient::new(channel);
    let request = Request::new(once(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }));
    let response = reflection
        .server_reflection_info(request)
        .await?
        .into_inner()
        .next()
        .await
        .ok_or_else(|| eyre!("reflection stream closed without a response"))??;
    let response = response
        .message_response
        .ok_or_else(|| eyre!("reflection response omitted its message"))?;
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "Reflection responses are non-exhaustive and unexpected variants fail the contract smoke."
    )]
    let services = match response {
        MessageResponse::ListServicesResponse(services) => services.service,
        other => return Err(eyre!("unexpected reflection response: {other:?}")),
    };
    assert!(
        services
            .iter()
            .any(|service| service.name == EXPECTED_SERVICE),
        "reflection did not advertise {EXPECTED_SERVICE}: {services:?}"
    );
    Ok(())
}

fn normalize_endpoint(configured: &str) -> String {
    if configured.starts_with("http://") || configured.starts_with("https://") {
        configured.to_owned()
    } else {
        format!("http://{configured}")
    }
}

fn assert_chain_view_epoch(
    chain_view: Option<&wallet::ChainView>,
    expected_epoch_id: u64,
    expected_network: &str,
) -> Result<()> {
    let chain_epoch = chain_view
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("pinned response omitted its chain epoch"))?;
    let _decoded_epoch = chain_epoch_from_message(chain_epoch.clone())?;
    assert_eq!(chain_epoch.chain_epoch_id, expected_epoch_id);
    assert_eq!(chain_epoch.network_name, expected_network);
    Ok(())
}

fn expected_network_name(network: zinder_core::Network) -> Result<&'static str> {
    match network {
        zinder_core::Network::ZcashRegtest => Ok("zcash-regtest"),
        zinder_core::Network::ZcashTestnet => Ok("zcash-testnet"),
        zinder_core::Network::ZcashMainnet => {
            Err(eyre!("standalone native smoke does not allow mainnet"))
        }
        #[allow(
            clippy::wildcard_enum_match_arm,
            reason = "Network is non-exhaustive; future variants need an explicit live-test policy."
        )]
        other => Err(eyre!("standalone native smoke does not allow {other:?}")),
    }
}
