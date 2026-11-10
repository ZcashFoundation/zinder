//! Live parity diff between Zinder's `zinder-compat-lightwalletd` and the
//! upstream `electriccoinco/lightwalletd` Go reference implementation, both
//! pointed at the same Zebra node.
//!
//! Double-gated by the Testing Runbook: `#[ignore]` keeps the test off the
//! default filter, and `require_live()` reads `ZINDER_TEST_LIVE=1`. Two additional
//! env vars name the gRPC endpoints; the test skips when either is absent so
//! the harness is invocable without infrastructure-level coordination:
//!
//! - `ZINDER_TEST_PARITY_ZINDER_ADDR` — Zinder compat-shim endpoint, e.g.
//!   `http://127.0.0.1:9087`.
//! - `ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR` — reference lightwalletd-go
//!   endpoint pointed at the same Zebra, e.g. `http://127.0.0.1:9088`.
//!
//! Operator-divergent fields (build metadata, version strings, donation
//! address) are explicitly allow-listed so the parity assertion focuses on
//! the wire shape both shims must agree on: `chainName`, `consensusBranchId`,
//! `saplingActivationHeight`, the next pending `upgradeName` and
//! `upgradeHeight`, latest block height and hash, single-block identity,
//! compact-block-range membership for a fixed span, nullifier redaction,
//! transaction round-trip by hash,
//! block/index-without-txid error parity, future-height status-code parity for
//! single-block compact block methods, invalid transaction broadcast rejection,
//! transparent address history, UTXOs, multi-address reads, and empty-address
//! behavior for regtest transparent addresses, tree-state identity,
//! Sapling/Orchard subtree-root availability, mempool compact-transaction
//! snapshot shape, non-empty mempool stream snapshot shape,
//! mempool request-validation status-code parity, and test-only Ping
//! availability.

#![allow(
    missing_docs,
    reason = "Live parity test names describe the wire-shape assertion under test."
)]

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use tonic::{Code, Status, transport::Endpoint};
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_core::{
    Network, TransactionId, UnixTimestampMillis, wire::decode_rpc_transaction_id_hex,
};
use zinder_proto::compat::lightwalletd::{
    self, BlockId, BlockRange, ChainSpec, Empty, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_source::{ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::live::{LiveTestEnv, init, optional_env, require_live, require_live_for};
use zinder_testkit::{
    P2pkhSpendArgs, TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey,
    ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, local_network_from_activations,
};

const PARITY_BLOCK_RANGE_END: u64 = 5;
const DEFAULT_PARITY_TRANSPARENT_ADDRESS: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";
const MEMPOOL_OBSERVE_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn lightd_info_advertises_matching_chain_metadata() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let zinder_info = zinder.get_lightd_info(Empty {}).await?.into_inner();
    let reference_info = reference.get_lightd_info(Empty {}).await?.into_inner();

    assert_eq!(
        zinder_info.chain_name, reference_info.chain_name,
        "chainName must agree across shims",
    );
    assert_eq!(
        zinder_info.sapling_activation_height, reference_info.sapling_activation_height,
        "saplingActivationHeight must agree",
    );
    assert_eq!(
        zinder_info.consensus_branch_id, reference_info.consensus_branch_id,
        "consensusBranchId must agree",
    );
    assert_eq!(
        zinder_info.upgrade_name, reference_info.upgrade_name,
        "upgradeName must name the same next pending upgrade",
    );
    assert_eq!(
        zinder_info.upgrade_height, reference_info.upgrade_height,
        "upgradeHeight must name the same next pending activation height",
    );
    assert!(
        zinder_info.taddr_support,
        "Zinder must advertise transparent-address support",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn latest_block_matches_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let zinder_block = zinder.get_latest_block(ChainSpec {}).await?.into_inner();
    let reference_block = reference.get_latest_block(ChainSpec {}).await?.into_inner();

    // Heights may differ by one when ingest is mid-commit; require within a
    // tight tolerance and assert hashes only when heights agree exactly.
    let height_difference = zinder_block.height.abs_diff(reference_block.height);
    assert!(
        height_difference <= 1,
        "latest block heights drifted: zinder={} reference={}",
        zinder_block.height,
        reference_block.height,
    );
    if zinder_block.height == reference_block.height {
        assert_eq!(
            zinder_block.hash, reference_block.hash,
            "latest block hashes diverge at the same height",
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn block_lookup_matches_reference_for_known_height() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || BlockId {
        height: 1,
        hash: Vec::new(),
    };
    let zinder_block = zinder.get_block(request()).await?.into_inner();
    let reference_block = reference.get_block(request()).await?.into_inner();

    assert_compact_block_identity_matches(&zinder_block, &reference_block);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn compact_block_range_matches_reference_for_first_blocks() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || BlockRange {
        start: Some(BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: PARITY_BLOCK_RANGE_END,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    let zinder_blocks = drain_block_range(zinder.get_block_range(request()).await?.into_inner())
        .await
        .map_err(|error| eyre!("zinder block-range stream failed: {error}"))?;
    let reference_blocks =
        drain_block_range(reference.get_block_range(request()).await?.into_inner())
            .await
            .map_err(|error| eyre!("reference block-range stream failed: {error}"))?;

    assert_eq!(
        zinder_blocks.len(),
        reference_blocks.len(),
        "compact block count diverges across shims",
    );
    for (zinder_block, reference_block) in zinder_blocks.iter().zip(reference_blocks.iter()) {
        assert_compact_block_identity_matches(zinder_block, reference_block);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn block_lookup_matches_reference_near_common_tip() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let height = common_latest_height(&mut zinder, &mut reference).await?;
    let request = || BlockId {
        height,
        hash: Vec::new(),
    };
    let zinder_block = zinder.get_block(request()).await?.into_inner();
    let reference_block = reference.get_block(request()).await?.into_inner();

    assert_compact_block_identity_matches(&zinder_block, &reference_block);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn compact_block_range_matches_reference_near_common_tip() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let end_height = common_latest_height(&mut zinder, &mut reference).await?;
    let start_height = end_height.saturating_sub(2).max(1);
    let request = || BlockRange {
        start: Some(BlockId {
            height: start_height,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: end_height,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    let zinder_blocks = drain_block_range(zinder.get_block_range(request()).await?.into_inner())
        .await
        .map_err(|error| eyre!("zinder tail block-range stream failed: {error}"))?;
    let reference_blocks =
        drain_block_range(reference.get_block_range(request()).await?.into_inner())
            .await
            .map_err(|error| eyre!("reference tail block-range stream failed: {error}"))?;

    assert_eq!(
        zinder_blocks.len(),
        reference_blocks.len(),
        "tail compact block count diverges across shims",
    );
    for (zinder_block, reference_block) in zinder_blocks.iter().zip(reference_blocks.iter()) {
        assert_compact_block_identity_matches(zinder_block, reference_block);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn transaction_round_trips_through_get_transaction_by_hash() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let block = zinder
        .get_block(BlockId {
            height: 1,
            hash: Vec::new(),
        })
        .await?
        .into_inner();
    let first_tx = block
        .vtx
        .first()
        .ok_or_else(|| eyre!("regtest block 1 should carry at least one transaction"))?;

    let zinder_transaction = zinder
        .get_transaction(TxFilter {
            block: None,
            index: 0,
            hash: first_tx.txid.clone(),
        })
        .await?
        .into_inner();
    let reference_transaction = reference
        .get_transaction(TxFilter {
            block: None,
            index: 0,
            hash: first_tx.txid.clone(),
        })
        .await?
        .into_inner();

    assert_eq!(zinder_transaction.height, reference_transaction.height);
    assert_eq!(zinder_transaction.data, reference_transaction.data);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn transaction_by_block_index_without_txid_errors_like_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || TxFilter {
        block: Some(BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        index: 0,
        hash: Vec::new(),
    };
    let zinder_status = error_status(
        zinder.get_transaction(request()).await,
        "Zinder GetTransaction",
        "block index without txid",
    )?;
    let reference_status = error_status(
        reference.get_transaction(request()).await,
        "reference GetTransaction",
        "block index without txid",
    )?;

    assert_matching_status_code(
        &zinder_status,
        &reference_status,
        Code::InvalidArgument,
        "GetTransaction",
        "block index without txid",
    );
    assert_eq!(zinder_status.message(), reference_status.message());
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn send_transaction_rejects_invalid_encoding_like_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || lightwalletd::RawTransaction {
        data: vec![0x00],
        height: 0,
    };
    let zinder_response = zinder.send_transaction(request()).await?.into_inner();
    let reference_response = reference.send_transaction(request()).await?.into_inner();

    assert_eq!(
        zinder_response.error_code, reference_response.error_code,
        "invalid transaction encoding error codes diverge",
    );
    assert_ne!(
        zinder_response.error_code, 0,
        "invalid transaction encoding must not be accepted",
    );
    assert!(
        !zinder_response.error_message.is_empty(),
        "Zinder invalid transaction rejection must include an error message",
    );
    assert!(
        !reference_response.error_message.is_empty(),
        "reference invalid transaction rejection must include an error message",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn accepted_send_transaction_surfaces_in_non_empty_mempool_methods() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let test_key = transparent_broadcast_test_key(&env).await?;
    let funded_address = parity_transparent_address()?;
    let expected_funded_address = test_key.address_base58();
    if funded_address != expected_funded_address {
        return Err(eyre!(
            "ZINDER_TEST_PARITY_TRANSPARENT_ADDRESS must be the address derived from \
             TRANSPARENT_BROADCAST_TEST_SEED: configured={funded_address} expected={expected_funded_address}"
        ));
    }

    let target_height = common_latest_height(&mut zinder, &mut reference)
        .await?
        .saturating_add(1);
    let pending_spends = pending_transparent_spend_outpoints(&mut zinder).await?;
    let spendable_utxo = select_spendable_lightwalletd_utxo(
        &mut zinder,
        &funded_address,
        target_height,
        &pending_spends,
    )
    .await?;
    let recipient_address = test_key.scratch_recipient_address(UnixTimestampMillis::now().value());
    let raw_transaction = test_key
        .build_p2pkh_spend(&P2pkhSpendArgs {
            coinbase_txid_be: spendable_utxo.txid,
            coinbase_vout: spendable_utxo.vout,
            coinbase_value_zats: spendable_utxo.value_zats,
            recipient: &recipient_address,
            target_height: spendable_utxo.target_height,
        })
        .map_err(|error| eyre!("transparent signer rejected the spend: {error}"))?;

    let send_response = zinder
        .send_transaction(lightwalletd::RawTransaction {
            data: raw_transaction.clone(),
            height: 0,
        })
        .await?
        .into_inner();
    assert_eq!(
        send_response.error_code, 0,
        "accepted Zinder SendTransaction must return lightwalletd success code; response={send_response:?}",
    );
    let broadcast_txid = decode_rpc_transaction_id_hex(&send_response.error_message)
        .map_err(|error| eyre!("SendTransaction success message was not a txid: {error}"))?;

    let zinder_raw =
        wait_for_mempool_stream_snapshot_transaction(&mut zinder, &raw_transaction, "Zinder")
            .await?;
    let reference_raw =
        wait_for_mempool_stream_snapshot_transaction(&mut reference, &raw_transaction, "reference")
            .await?;
    assert_eq!(zinder_raw.height, 0, "Zinder mempool RawTransaction height");
    assert_eq!(
        reference_raw.height, 0,
        "reference mempool RawTransaction height"
    );

    let zinder_compact =
        wait_for_compact_mempool_transaction(&mut zinder, broadcast_txid, "Zinder").await?;
    let reference_compact =
        wait_for_compact_mempool_transaction(&mut reference, broadcast_txid, "reference").await?;
    assert_eq!(
        zinder_compact, reference_compact,
        "transparent mempool CompactTx diverged after accepted send",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
#[allow(
    deprecated,
    reason = "GetBlockNullifiers is deprecated in the lightwalletd contract but the single-block redaction parity must still hold for the wallets that call it."
)]
async fn block_nullifiers_omit_commitment_tree_sizes_like_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || BlockId {
        height: 1,
        hash: Vec::new(),
    };
    let zinder_block = zinder.get_block_nullifiers(request()).await?.into_inner();
    let reference_block = reference
        .get_block_nullifiers(request())
        .await?
        .into_inner();

    assert_nullifier_response_redacts_non_nullifier_payloads(&zinder_block);
    assert_nullifier_response_redacts_non_nullifier_payloads(&reference_block);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
#[allow(
    deprecated,
    reason = "GetBlockRangeNullifiers is deprecated in the lightwalletd contract but the redaction parity must still hold for the wallets that call it."
)]
async fn block_range_nullifiers_omit_commitment_tree_sizes_like_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || BlockRange {
        start: Some(BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: PARITY_BLOCK_RANGE_END,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    let zinder_blocks = drain_block_range(
        zinder
            .get_block_range_nullifiers(request())
            .await?
            .into_inner(),
    )
    .await
    .map_err(|error| eyre!("zinder nullifier-range stream failed: {error}"))?;
    let reference_blocks = drain_block_range(
        reference
            .get_block_range_nullifiers(request())
            .await?
            .into_inner(),
    )
    .await
    .map_err(|error| eyre!("reference nullifier-range stream failed: {error}"))?;

    for blocks in [&zinder_blocks, &reference_blocks] {
        for block in blocks {
            assert_nullifier_response_redacts_non_nullifier_payloads(block);
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
#[allow(
    deprecated,
    reason = "GetBlockNullifiers is deprecated in the lightwalletd contract but future-height error parity must still hold for wallets that call it."
)]
async fn future_block_errors_match_reference_for_single_block_methods() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let future_height = future_height_above_both_shims(&mut zinder, &mut reference).await?;
    let future_height_case = format!("future height {future_height}");
    let request = || BlockId {
        height: future_height,
        hash: Vec::new(),
    };

    let zinder_block_status = error_status(
        zinder.get_block(request()).await,
        "Zinder GetBlock",
        &future_height_case,
    )?;
    let reference_block_status = error_status(
        reference.get_block(request()).await,
        "reference GetBlock",
        &future_height_case,
    )?;
    assert_matching_status_code(
        &zinder_block_status,
        &reference_block_status,
        Code::OutOfRange,
        "GetBlock",
        &future_height_case,
    );

    let zinder_nullifiers_status = error_status(
        zinder.get_block_nullifiers(request()).await,
        "Zinder GetBlockNullifiers",
        &future_height_case,
    )?;
    let reference_nullifiers_status = error_status(
        reference.get_block_nullifiers(request()).await,
        "reference GetBlockNullifiers",
        &future_height_case,
    )?;
    assert_matching_status_code(
        &zinder_nullifiers_status,
        &reference_nullifiers_status,
        Code::OutOfRange,
        "GetBlockNullifiers",
        &future_height_case,
    );

    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
#[allow(
    deprecated,
    reason = "GetBlockRangeNullifiers is deprecated in the lightwalletd contract but future-height error parity must still hold for wallets that call it."
)]
async fn future_block_errors_match_reference_for_range_methods() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let future_height = future_height_above_both_shims(&mut zinder, &mut reference).await?;
    let future_height_case = format!("future height {future_height}");
    let request = || BlockRange {
        start: Some(BlockId {
            height: future_height,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: future_height,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };

    let zinder_range_status = stream_error_status(
        zinder.get_block_range(request()).await,
        "Zinder GetBlockRange",
        &future_height_case,
    )
    .await?;
    let reference_range_status = stream_error_status(
        reference.get_block_range(request()).await,
        "reference GetBlockRange",
        &future_height_case,
    )
    .await?;
    assert_matching_status_code(
        &zinder_range_status,
        &reference_range_status,
        Code::OutOfRange,
        "GetBlockRange",
        &future_height_case,
    );

    let zinder_nullifiers_status = stream_error_status(
        zinder.get_block_range_nullifiers(request()).await,
        "Zinder GetBlockRangeNullifiers",
        &future_height_case,
    )
    .await?;
    let reference_nullifiers_status = stream_error_status(
        reference.get_block_range_nullifiers(request()).await,
        "reference GetBlockRangeNullifiers",
        &future_height_case,
    )
    .await?;
    assert_matching_status_code(
        &zinder_nullifiers_status,
        &reference_nullifiers_status,
        Code::OutOfRange,
        "GetBlockRangeNullifiers",
        &future_height_case,
    );

    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn tree_state_matches_reference_for_known_height() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || BlockId {
        height: 1,
        hash: Vec::new(),
    };
    let zinder_tree_state = zinder.get_tree_state(request()).await?.into_inner();
    let reference_tree_state = reference.get_tree_state(request()).await?.into_inner();

    assert_tree_state_identity_matches(&zinder_tree_state, &reference_tree_state);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn latest_tree_state_matches_reference_when_tip_matches() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let zinder_tree_state = zinder.get_latest_tree_state(Empty {}).await?.into_inner();
    let reference_tree_state = reference
        .get_latest_tree_state(Empty {})
        .await?
        .into_inner();

    assert_eq!(
        zinder_tree_state.network, reference_tree_state.network,
        "latest tree-state network labels diverge",
    );
    let height_difference = zinder_tree_state
        .height
        .abs_diff(reference_tree_state.height);
    assert!(
        height_difference <= 1,
        "latest tree-state heights drifted: zinder={} reference={}",
        zinder_tree_state.height,
        reference_tree_state.height,
    );
    if zinder_tree_state.height == reference_tree_state.height {
        assert_tree_state_identity_matches(&zinder_tree_state, &reference_tree_state);
    }
    Ok(())
}

/// Asserts Sapling/Orchard subtree roots agree across shims, byte for byte.
///
/// On the default regtest premine (tens of blocks), both sides return an
/// empty list: a shielded subtree completes only after `2^16` notes, far
/// beyond what a parity CI run mines. An empty-vs-empty pass here proves the
/// two shims agree there is nothing to report, not that a non-empty root
/// round-trips correctly. Non-empty coverage is tracked as remaining work in
/// `docs/reference/lightwalletd-compatibility.md` (`GetSubtreeRoots`
/// row) and needs a testnet/mainnet-scale fixture, not a regtest one.
#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn subtree_roots_match_reference_for_supported_pools() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    for protocol in [
        lightwalletd::ShieldedProtocol::Sapling,
        lightwalletd::ShieldedProtocol::Orchard,
    ] {
        let request = || lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: protocol as i32,
            max_entries: 100,
        };
        let zinder_roots =
            drain_subtree_roots(zinder.get_subtree_roots(request()).await?.into_inner()).await?;
        let reference_roots =
            drain_subtree_roots(reference.get_subtree_roots(request()).await?.into_inner()).await?;

        assert_eq!(
            zinder_roots, reference_roots,
            "{protocol:?} subtree roots diverge across shims",
        );
    }

    Ok(())
}

/// Asserts the mempool compact-transaction snapshot agrees across shims
/// after ordering normalization.
///
/// Neither shim seeds a mempool transaction, so on a quiescent regtest
/// fixture both sides are typically empty: this proves the two shims agree
/// there is nothing pending, not that a non-empty snapshot round-trips
/// correctly. Non-empty coverage needs a broadcast helper (see
/// `zinder-testkit::TransparentTestKey`, already used by `zinder-ingest`'s
/// live broadcast-cycle test) and is tracked as remaining work in
/// `docs/reference/lightwalletd-compatibility.md` (`GetMempoolTx`
/// row).
///
/// If a mempool transaction does exist at call time, this issues two
/// separate unary calls (Zinder, then reference) against the same live
/// Zebra mempool; a transaction entering or leaving between the two calls
/// would read as a false parity failure. Low risk against the pinned,
/// otherwise-idle regtest fixture; do not point this test at a node with
/// organic traffic.
#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn mempool_tx_snapshot_matches_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let request = || lightwalletd::GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    };
    let zinder_mempool =
        drain_compact_transactions(zinder.get_mempool_tx(request()).await?.into_inner()).await?;
    let reference_mempool =
        drain_compact_transactions(reference.get_mempool_tx(request()).await?.into_inner()).await?;

    assert_eq!(
        normalize_compact_transactions(&zinder_mempool),
        normalize_compact_transactions(&reference_mempool),
        "mempool compact transaction snapshots diverge",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn mempool_tx_invalid_requests_match_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let oversized_suffix_request = || lightwalletd::GetMempoolTxRequest {
        exclude_txid_suffixes: vec![vec![0; 33]],
        pool_types: Vec::new(),
    };
    let zinder_suffix_status = error_status(
        zinder.get_mempool_tx(oversized_suffix_request()).await,
        "Zinder GetMempoolTx",
        "oversized excluded txid suffix",
    )?;
    let reference_suffix_status = error_status(
        reference.get_mempool_tx(oversized_suffix_request()).await,
        "reference GetMempoolTx",
        "oversized excluded txid suffix",
    )?;
    assert_matching_status_code(
        &zinder_suffix_status,
        &reference_suffix_status,
        Code::InvalidArgument,
        "GetMempoolTx",
        "oversized excluded txid suffix",
    );

    let invalid_pool_request = || lightwalletd::GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: vec![lightwalletd::PoolType::Invalid as i32],
    };
    let zinder_pool_status = error_status(
        zinder.get_mempool_tx(invalid_pool_request()).await,
        "Zinder GetMempoolTx",
        "invalid pool type",
    )?;
    let reference_pool_status = error_status(
        reference.get_mempool_tx(invalid_pool_request()).await,
        "reference GetMempoolTx",
        "invalid pool type",
    )?;
    assert_matching_status_code(
        &zinder_pool_status,
        &reference_pool_status,
        Code::InvalidArgument,
        "GetMempoolTx",
        "invalid pool type",
    );

    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn ping_responds_on_both_shims() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };

    let zinder_ping = zinder
        .ping(lightwalletd::Duration { interval_us: 0 })
        .await?
        .into_inner();
    let reference_ping = reference
        .ping(lightwalletd::Duration { interval_us: 0 })
        .await?
        .into_inner();

    assert!(
        zinder_ping.entry >= 0 && zinder_ping.exit >= 0,
        "Zinder Ping counts must be non-negative",
    );
    assert!(
        reference_ping.entry >= 0 && reference_ping.exit >= 0,
        "reference Ping counts must be non-negative",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn address_utxos_match_reference_for_miner_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = parity_transparent_address()?;

    let request = || lightwalletd::GetAddressUtxosArg {
        addresses: vec![address.clone()],
        start_height: 1,
        max_entries: 100,
    };
    let zinder_list = zinder.get_address_utxos(request()).await?.into_inner();
    let reference_list = reference.get_address_utxos(request()).await?.into_inner();
    let zinder_stream = drain_address_utxos(
        zinder
            .get_address_utxos_stream(request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_stream = drain_address_utxos(
        reference
            .get_address_utxos_stream(request())
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(
        zinder_list.address_utxos, zinder_stream,
        "Zinder GetAddressUtxos list and stream diverge",
    );
    assert_eq!(
        reference_list.address_utxos, reference_stream,
        "reference GetAddressUtxos list and stream diverge",
    );
    assert_eq!(
        normalize_address_utxos(&zinder_stream),
        normalize_address_utxos(&reference_stream),
        "transparent UTXOs diverge for miner address {address}",
    );
    assert!(
        !zinder_stream.is_empty(),
        "miner address {address} should have transparent UTXOs after regtest mining",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn address_utxos_respect_max_entries_for_miner_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = parity_transparent_address()?;

    let request = || lightwalletd::GetAddressUtxosArg {
        addresses: vec![address.clone()],
        start_height: 1,
        max_entries: 1,
    };
    let zinder_list = zinder.get_address_utxos(request()).await?.into_inner();
    let reference_list = reference.get_address_utxos(request()).await?.into_inner();
    let zinder_stream = drain_address_utxos(
        zinder
            .get_address_utxos_stream(request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_stream = drain_address_utxos(
        reference
            .get_address_utxos_stream(request())
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(
        zinder_list.address_utxos, zinder_stream,
        "Zinder max-entry GetAddressUtxos list and stream diverge",
    );
    assert_eq!(
        reference_list.address_utxos, reference_stream,
        "reference max-entry GetAddressUtxos list and stream diverge",
    );
    assert_eq!(
        normalize_address_utxos(&zinder_stream),
        normalize_address_utxos(&reference_stream),
        "max-entry transparent UTXOs diverge for miner address {address}",
    );
    assert!(
        zinder_stream.len() <= 1,
        "Zinder returned more UTXOs than max_entries for miner address {address}",
    );
    assert!(
        reference_stream.len() <= 1,
        "reference returned more UTXOs than max_entries for miner address {address}",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn address_utxos_match_reference_for_unused_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = unused_parity_transparent_address();

    let utxo_request = || lightwalletd::GetAddressUtxosArg {
        addresses: vec![address.clone()],
        start_height: 1,
        max_entries: 100,
    };
    let zinder_utxo_list = zinder.get_address_utxos(utxo_request()).await?.into_inner();
    let reference_utxo_list = reference
        .get_address_utxos(utxo_request())
        .await?
        .into_inner();
    let zinder_utxo_stream = drain_address_utxos(
        zinder
            .get_address_utxos_stream(utxo_request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_utxo_stream = drain_address_utxos(
        reference
            .get_address_utxos_stream(utxo_request())
            .await?
            .into_inner(),
    )
    .await?;

    assert!(zinder_utxo_list.address_utxos.is_empty());
    assert!(reference_utxo_list.address_utxos.is_empty());
    assert!(zinder_utxo_stream.is_empty());
    assert!(reference_utxo_stream.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn taddress_history_matches_reference_for_unused_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = unused_parity_transparent_address();
    let visible_tip_block = zinder.get_latest_block(ChainSpec {}).await?.into_inner();

    let history_request = || lightwalletd::TransparentAddressBlockFilter {
        address: address.clone(),
        range: Some(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(BlockId {
                height: visible_tip_block.height,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }),
    };
    let zinder_txids = drain_raw_transactions(
        zinder
            .get_taddress_txids(history_request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_txids = drain_raw_transactions(
        reference
            .get_taddress_txids(history_request())
            .await?
            .into_inner(),
    )
    .await?;
    let zinder_transactions = drain_raw_transactions(
        zinder
            .get_taddress_transactions(history_request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_transactions = drain_raw_transactions(
        reference
            .get_taddress_transactions(history_request())
            .await?
            .into_inner(),
    )
    .await?;

    assert!(zinder_txids.is_empty());
    assert!(reference_txids.is_empty());
    assert!(zinder_transactions.is_empty());
    assert!(reference_transactions.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn taddress_balance_matches_reference_for_unused_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = unused_parity_transparent_address();

    let zinder_balance = zinder
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![address.clone()],
        })
        .await?
        .into_inner();
    let reference_balance = reference
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![address.clone()],
        })
        .await?
        .into_inner();
    let zinder_stream_balance = zinder
        .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
            address: address.clone(),
        }]))
        .await?
        .into_inner();
    let reference_stream_balance = reference
        .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
            address: address.clone(),
        }]))
        .await?
        .into_inner();

    assert_eq!(zinder_balance, reference_balance);
    assert_eq!(zinder_balance, zinder_stream_balance);
    assert_eq!(reference_balance, reference_stream_balance);
    assert_eq!(zinder_balance.value_zat, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn taddress_history_matches_reference_for_miner_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = parity_transparent_address()?;

    let visible_tip_block = zinder.get_latest_block(ChainSpec {}).await?.into_inner();
    let request = || lightwalletd::TransparentAddressBlockFilter {
        address: address.clone(),
        range: Some(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(BlockId {
                height: visible_tip_block.height,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }),
    };
    let zinder_txids =
        drain_raw_transactions(zinder.get_taddress_txids(request()).await?.into_inner()).await?;
    let reference_txids =
        drain_raw_transactions(reference.get_taddress_txids(request()).await?.into_inner()).await?;
    let zinder_transactions = drain_raw_transactions(
        zinder
            .get_taddress_transactions(request())
            .await?
            .into_inner(),
    )
    .await?;
    let reference_transactions = drain_raw_transactions(
        reference
            .get_taddress_transactions(request())
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(
        normalize_raw_transactions(&zinder_txids),
        normalize_raw_transactions(&reference_txids),
        "GetTaddressTxids diverges for miner address {address}",
    );
    assert_eq!(
        normalize_raw_transactions(&zinder_transactions),
        normalize_raw_transactions(&reference_transactions),
        "GetTaddressTransactions diverges for miner address {address}",
    );
    assert!(
        !zinder_txids.is_empty(),
        "miner address {address} should have transparent history after regtest mining",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn taddress_balance_matches_reference_for_miner_address() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let address = parity_transparent_address()?;

    let zinder_balance = zinder
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![address.clone()],
        })
        .await?
        .into_inner();
    let reference_balance = reference
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![address.clone()],
        })
        .await?
        .into_inner();
    let zinder_stream_balance = zinder
        .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
            address: address.clone(),
        }]))
        .await?
        .into_inner();
    let reference_stream_balance = reference
        .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
            address: address.clone(),
        }]))
        .await?
        .into_inner();

    assert_eq!(
        zinder_balance, zinder_stream_balance,
        "Zinder t-address balance unary and stream methods diverge",
    );
    assert_eq!(
        reference_balance, reference_stream_balance,
        "reference t-address balance unary and stream methods diverge",
    );
    assert_eq!(
        zinder_balance, reference_balance,
        "t-address balance diverges for miner address {address}",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live parity test; see CLAUDE.md §Live Node Tests"]
async fn multi_address_transparent_queries_match_reference() -> Result<()> {
    let _guard = init();
    let Some(_env) = require_live()? else {
        return Ok(());
    };
    let Some((mut zinder, mut reference)) = open_parity_clients().await? else {
        return Ok(());
    };
    let miner_address = parity_transparent_address()?;
    let unused_address = unused_parity_transparent_address();
    let addresses = || vec![miner_address.clone(), unused_address.clone()];

    let utxo_request = || lightwalletd::GetAddressUtxosArg {
        addresses: addresses(),
        start_height: 1,
        max_entries: 100,
    };
    let zinder_utxos = zinder.get_address_utxos(utxo_request()).await?.into_inner();
    let reference_utxos = reference
        .get_address_utxos(utxo_request())
        .await?
        .into_inner();
    assert_eq!(
        normalize_address_utxos(&zinder_utxos.address_utxos),
        normalize_address_utxos(&reference_utxos.address_utxos),
        "multi-address transparent UTXOs diverge",
    );

    let zinder_balance = zinder
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: addresses(),
        })
        .await?
        .into_inner();
    let reference_balance = reference
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: addresses(),
        })
        .await?
        .into_inner();
    let zinder_stream_balance = zinder
        .get_taddress_balance_stream(tokio_stream::iter(
            addresses()
                .into_iter()
                .map(|address| lightwalletd::Address { address }),
        ))
        .await?
        .into_inner();
    let reference_stream_balance = reference
        .get_taddress_balance_stream(tokio_stream::iter(
            addresses()
                .into_iter()
                .map(|address| lightwalletd::Address { address }),
        ))
        .await?
        .into_inner();

    assert_eq!(
        zinder_balance, reference_balance,
        "multi-address transparent balances diverge",
    );
    assert_eq!(
        zinder_balance, zinder_stream_balance,
        "Zinder multi-address balance unary and stream methods diverge",
    );
    assert_eq!(
        reference_balance, reference_stream_balance,
        "reference multi-address balance unary and stream methods diverge",
    );
    Ok(())
}

fn assert_compact_block_identity_matches(
    zinder_block: &lightwalletd::CompactBlock,
    reference_block: &lightwalletd::CompactBlock,
) {
    assert_eq!(
        zinder_block.height, reference_block.height,
        "compact block heights diverge",
    );
    assert_eq!(
        zinder_block.hash, reference_block.hash,
        "compact block hashes diverge at height {}",
        zinder_block.height,
    );
    assert_eq!(
        zinder_block.prev_hash, reference_block.prev_hash,
        "compact block prev_hash diverges at height {}",
        zinder_block.height,
    );
    assert_eq!(
        zinder_block.vtx.len(),
        reference_block.vtx.len(),
        "compact transaction count diverges at height {}",
        zinder_block.height,
    );
    for (zinder_tx, reference_tx) in zinder_block.vtx.iter().zip(reference_block.vtx.iter()) {
        assert_eq!(
            zinder_tx.txid, reference_tx.txid,
            "compact transaction ids diverge at height {}",
            zinder_block.height,
        );
    }
}

fn assert_tree_state_identity_matches(
    zinder_tree_state: &lightwalletd::TreeState,
    reference_tree_state: &lightwalletd::TreeState,
) {
    assert_eq!(
        zinder_tree_state.network, reference_tree_state.network,
        "tree-state network labels diverge",
    );
    assert_eq!(
        zinder_tree_state.height, reference_tree_state.height,
        "tree-state heights diverge",
    );
    assert_eq!(
        zinder_tree_state.hash, reference_tree_state.hash,
        "tree-state hashes diverge at height {}",
        zinder_tree_state.height,
    );
    assert_eq!(
        zinder_tree_state.time, reference_tree_state.time,
        "tree-state times diverge at height {}",
        zinder_tree_state.height,
    );
    assert_tree_field_matches_when_reference_has_value(
        "saplingTree",
        &zinder_tree_state.sapling_tree,
        &reference_tree_state.sapling_tree,
        zinder_tree_state.height,
    );
    assert_tree_field_matches_when_reference_has_value(
        "orchardTree",
        &zinder_tree_state.orchard_tree,
        &reference_tree_state.orchard_tree,
        zinder_tree_state.height,
    );
    assert_tree_field_matches_when_reference_has_value(
        "ironwoodTree",
        &zinder_tree_state.ironwood_tree,
        &reference_tree_state.ironwood_tree,
        zinder_tree_state.height,
    );
}

fn assert_tree_field_matches_when_reference_has_value(
    field_name: &'static str,
    zinder_value: &str,
    reference_value: &str,
    height: u64,
) {
    if reference_value.is_empty() {
        return;
    }
    assert_eq!(
        zinder_value, reference_value,
        "{field_name} diverges at height {height}",
    );
}

fn assert_nullifier_response_redacts_non_nullifier_payloads(block: &lightwalletd::CompactBlock) {
    // The reference emits an all-zero `chainMetadata` message here while
    // Zinder omits the field entirely; both withhold the witness-construction
    // tree sizes, so the parity contract is "no non-zero tree size leaks", not
    // strict field absence.
    let leaks_tree_sizes = block.chain_metadata.as_ref().is_some_and(|metadata| {
        metadata.sapling_commitment_tree_size != 0
            || metadata.orchard_commitment_tree_size != 0
            || metadata.ironwood_commitment_tree_size != 0
    });
    assert!(
        !leaks_tree_sizes,
        "nullifiers-only responses must not leak commitment-tree sizes at height {}",
        block.height,
    );
    for transaction in &block.vtx {
        assert!(
            transaction.vin.is_empty() && transaction.vout.is_empty(),
            "nullifiers-only transparent data must be cleared at height {}",
            block.height,
        );
        assert!(
            transaction.outputs.is_empty(),
            "nullifiers-only Sapling outputs must be cleared at height {}",
            block.height,
        );
    }
}

fn normalize_address_utxos(
    utxos: &[lightwalletd::GetAddressUtxosReply],
) -> Vec<lightwalletd::GetAddressUtxosReply> {
    let mut normalized = utxos.to_vec();
    normalized.sort_by(|left, right| {
        (
            left.height,
            left.txid.as_slice(),
            left.index,
            left.value_zat,
            left.script.as_slice(),
            left.address.as_str(),
        )
            .cmp(&(
                right.height,
                right.txid.as_slice(),
                right.index,
                right.value_zat,
                right.script.as_slice(),
                right.address.as_str(),
            ))
    });
    normalized
}

fn normalize_raw_transactions(
    transactions: &[lightwalletd::RawTransaction],
) -> Vec<lightwalletd::RawTransaction> {
    let mut normalized = transactions.to_vec();
    normalized.sort_by(|left, right| {
        (left.height, left.data.as_slice()).cmp(&(right.height, right.data.as_slice()))
    });
    normalized
}

fn normalize_compact_transactions(
    transactions: &[lightwalletd::CompactTx],
) -> Vec<lightwalletd::CompactTx> {
    let mut normalized = transactions.to_vec();
    normalized.sort_by(|left, right| left.txid.cmp(&right.txid));
    normalized
}

fn parity_transparent_address() -> Result<String> {
    Ok(optional_env("ZINDER_TEST_PARITY_TRANSPARENT_ADDRESS")?
        .unwrap_or_else(|| DEFAULT_PARITY_TRANSPARENT_ADDRESS.to_owned()))
}

type TransparentOutpoint = ([u8; 32], u32);

struct SpendableLightwalletdUtxo {
    txid: [u8; 32],
    vout: u32,
    value_zats: u64,
    target_height: u32,
}

async fn transparent_broadcast_test_key(env: &LiveTestEnv) -> Result<TransparentTestKey> {
    let source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let schedule = source
        .fetch_network_upgrade_activations()
        .await
        .map_err(|error| eyre!("could not fetch node-advertised upgrade schedule: {error}"))?;
    TransparentTestKey::from_seed_with_local_network(
        &TRANSPARENT_BROADCAST_TEST_SEED,
        local_network_from_activations(&schedule),
    )
    .map_err(|error| eyre!("could not derive transparent broadcast test key: {error}"))
}

async fn pending_transparent_spend_outpoints(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
) -> Result<HashSet<TransparentOutpoint>> {
    let mempool_transactions = drain_compact_transactions(
        client
            .get_mempool_tx(transparent_mempool_request())
            .await?
            .into_inner(),
    )
    .await?;
    let mut outpoints = HashSet::new();
    for transaction in mempool_transactions {
        for input in transaction.vin {
            let Ok(txid) = lightwalletd_txid_bytes(&input.prevout_txid, "mempool prevout txid")
            else {
                continue;
            };
            outpoints.insert((txid, input.prevout_index));
        }
    }
    Ok(outpoints)
}

async fn select_spendable_lightwalletd_utxo(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    address: &str,
    target_height: u64,
    pending_spends: &HashSet<TransparentOutpoint>,
) -> Result<SpendableLightwalletdUtxo> {
    let response = client
        .get_address_utxos(lightwalletd::GetAddressUtxosArg {
            addresses: vec![address.to_owned()],
            start_height: 1,
            max_entries: 500,
        })
        .await?
        .into_inner();
    let mut utxos = response.address_utxos;
    utxos.sort_by_key(|utxo| utxo.value_zat);
    utxos.reverse();
    let maturity_cutoff = target_height.saturating_sub(100);

    for utxo in utxos {
        if utxo.height > maturity_cutoff || utxo.value_zat <= 0 {
            continue;
        }
        let txid = lightwalletd_txid_bytes(&utxo.txid, "address UTXO txid")?;
        let vout = u32::try_from(utxo.index)
            .map_err(|_| eyre!("address UTXO index is negative: {}", utxo.index))?;
        if pending_spends.contains(&(txid, vout)) {
            continue;
        }
        let value_zats = u64::try_from(utxo.value_zat)
            .map_err(|_| eyre!("address UTXO value is negative: {}", utxo.value_zat))?;
        if value_zats <= ZIP317_FEE_ONE_IN_ONE_OUT_ZATS {
            continue;
        }
        let target_height = u32::try_from(target_height)
            .map_err(|_| eyre!("target height {target_height} exceeds u32"))?;
        return Ok(SpendableLightwalletdUtxo {
            txid,
            vout,
            value_zats,
            target_height,
        });
    }

    Err(eyre!(
        "no mature spendable UTXO for parity address {address}; the parity fixture must premine at least 125 blocks to the address printed by `cargo run -p zinder-testkit --example print_broadcast_test_address`"
    ))
}

async fn wait_for_mempool_stream_snapshot_transaction(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    expected_raw_transaction: &[u8],
    source_name: &str,
) -> Result<lightwalletd::RawTransaction> {
    let started = Instant::now();
    while started.elapsed() < MEMPOOL_OBSERVE_TIMEOUT {
        let mut stream = client
            .get_mempool_stream(Empty {})
            .await
            .map_err(|status| eyre!("{source_name} GetMempoolStream failed: {status}"))?
            .into_inner();
        if let Some(transaction) = find_mempool_stream_snapshot_transaction(
            &mut stream,
            expected_raw_transaction,
            source_name,
        )
        .await?
        {
            return Ok(transaction);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(eyre!(
        "{source_name} GetMempoolStream snapshot did not include the accepted transaction within {:?}",
        MEMPOOL_OBSERVE_TIMEOUT
    ))
}

async fn find_mempool_stream_snapshot_transaction(
    stream: &mut tonic::Streaming<lightwalletd::RawTransaction>,
    expected_raw_transaction: &[u8],
    source_name: &str,
) -> Result<Option<lightwalletd::RawTransaction>> {
    loop {
        match tokio::time::timeout(Duration::from_millis(500), stream.message()).await {
            Ok(Ok(Some(transaction))) if transaction.data == expected_raw_transaction => {
                return Ok(Some(transaction));
            }
            Ok(Ok(Some(_other_transaction))) => {}
            Ok(Ok(None)) => return Ok(None),
            Ok(Err(status)) => {
                return Err(eyre!(
                    "{source_name} GetMempoolStream errored before emitting the accepted transaction: {status}"
                ));
            }
            Err(_elapsed) => return Ok(None),
        }
    }
}

async fn wait_for_compact_mempool_transaction(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    transaction_id: TransactionId,
    source_name: &str,
) -> Result<lightwalletd::CompactTx> {
    let started = Instant::now();
    let expected_txid = transaction_id.as_bytes();
    while started.elapsed() < MEMPOOL_OBSERVE_TIMEOUT {
        let mempool_transactions = drain_compact_transactions(
            client
                .get_mempool_tx(transparent_mempool_request())
                .await?
                .into_inner(),
        )
        .await?;
        if let Some(transaction) = mempool_transactions
            .into_iter()
            .find(|transaction| transaction.txid.as_slice() == expected_txid)
        {
            return Ok(transaction);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(eyre!(
        "{source_name} GetMempoolTx did not include txid {} within {:?}",
        hex::encode(expected_txid),
        MEMPOOL_OBSERVE_TIMEOUT
    ))
}

fn transparent_mempool_request() -> lightwalletd::GetMempoolTxRequest {
    lightwalletd::GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: vec![lightwalletd::PoolType::Transparent as i32],
    }
}

fn lightwalletd_txid_bytes(txid: &[u8], field_name: &str) -> Result<[u8; 32]> {
    txid.try_into()
        .map_err(|_| eyre!("{field_name} decoded to {} bytes, expected 32", txid.len()))
}

fn unused_parity_transparent_address() -> String {
    ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x99; 20]).to_string()
}

async fn common_latest_height(
    zinder: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    reference: &mut CompactTxStreamerClient<tonic::transport::Channel>,
) -> Result<u64> {
    let zinder_block = zinder.get_latest_block(ChainSpec {}).await?.into_inner();
    let reference_block = reference.get_latest_block(ChainSpec {}).await?.into_inner();
    let height_difference = zinder_block.height.abs_diff(reference_block.height);
    if height_difference > 1 {
        return Err(eyre!(
            "latest block heights drifted too far for common-tip parity: zinder={} reference={}",
            zinder_block.height,
            reference_block.height,
        ));
    }
    Ok(zinder_block.height.min(reference_block.height))
}

async fn future_height_above_both_shims(
    zinder: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    reference: &mut CompactTxStreamerClient<tonic::transport::Channel>,
) -> Result<u64> {
    let zinder_block = zinder.get_latest_block(ChainSpec {}).await?.into_inner();
    let reference_block = reference.get_latest_block(ChainSpec {}).await?.into_inner();
    Ok(zinder_block.height.max(reference_block.height) + 1)
}

fn error_status<T>(
    outcome: Result<tonic::Response<T>, Status>,
    method_name: &str,
    case_name: &str,
) -> Result<Status> {
    match outcome {
        Ok(_) => Err(eyre!(
            "{method_name} unexpectedly succeeded for {case_name}"
        )),
        Err(status) => Ok(status),
    }
}

async fn stream_error_status<T>(
    outcome: Result<tonic::Response<tonic::Streaming<T>>, Status>,
    method_name: &str,
    case_name: &str,
) -> Result<Status> {
    match outcome {
        Err(status) => Ok(status),
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(_message)) => {}
                    Ok(None) => {
                        return Err(eyre!(
                            "{method_name} unexpectedly completed for {case_name}"
                        ));
                    }
                    Err(status) => return Ok(status),
                }
            }
        }
    }
}

fn assert_matching_status_code(
    zinder_status: &Status,
    reference_status: &Status,
    expected_code: Code,
    method_name: &str,
    case_name: &str,
) {
    assert_eq!(
        zinder_status.code(),
        reference_status.code(),
        "{method_name} {case_name} status codes diverge: zinder={zinder_status:?} reference={reference_status:?}",
    );
    assert_eq!(
        zinder_status.code(),
        expected_code,
        "{method_name} {case_name} status code changed: {zinder_status:?}",
    );
}

async fn open_parity_clients() -> Result<
    Option<(
        CompactTxStreamerClient<tonic::transport::Channel>,
        CompactTxStreamerClient<tonic::transport::Channel>,
    )>,
> {
    let Some(zinder_addr) = optional_env("ZINDER_TEST_PARITY_ZINDER_ADDR")? else {
        return Ok(None);
    };
    let Some(reference_addr) = optional_env("ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR")? else {
        return Ok(None);
    };
    let zinder = CompactTxStreamerClient::new(Endpoint::new(zinder_addr)?.connect().await?);
    let reference = CompactTxStreamerClient::new(Endpoint::new(reference_addr)?.connect().await?);
    Ok(Some((zinder, reference)))
}

async fn drain_address_utxos(
    mut stream: tonic::Streaming<lightwalletd::GetAddressUtxosReply>,
) -> Result<Vec<lightwalletd::GetAddressUtxosReply>> {
    let mut replies = Vec::new();
    while let Some(message) = stream.message().await? {
        replies.push(message);
    }
    Ok(replies)
}

async fn drain_raw_transactions(
    mut stream: tonic::Streaming<lightwalletd::RawTransaction>,
) -> Result<Vec<lightwalletd::RawTransaction>> {
    let mut transactions = Vec::new();
    while let Some(message) = stream.message().await? {
        transactions.push(message);
    }
    Ok(transactions)
}

async fn drain_subtree_roots(
    mut stream: tonic::Streaming<lightwalletd::SubtreeRoot>,
) -> Result<Vec<lightwalletd::SubtreeRoot>> {
    let mut roots = Vec::new();
    while let Some(message) = stream.message().await? {
        roots.push(message);
    }
    Ok(roots)
}

async fn drain_compact_transactions(
    mut stream: tonic::Streaming<lightwalletd::CompactTx>,
) -> Result<Vec<lightwalletd::CompactTx>> {
    let mut transactions = Vec::new();
    while let Some(message) = stream.message().await? {
        transactions.push(message);
    }
    Ok(transactions)
}

async fn drain_block_range(
    mut stream: tonic::Streaming<lightwalletd::CompactBlock>,
) -> Result<Vec<lightwalletd::CompactBlock>> {
    let mut blocks = Vec::new();
    while let Some(message) = stream.message().await? {
        blocks.push(message);
    }
    Ok(blocks)
}
