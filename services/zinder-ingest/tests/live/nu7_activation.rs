//! Live NU7 activation crossing.
//!
//! The node must advertise an NU7 activation height above its tip, mine to
//! the transparent broadcast test key, and hold a matured coinbase below NU7.
//! `deploy/zebra/regtest.toml` with `scripts/mine-live-regtest.sh` produces
//! that chain. The test then:
//!
//! 1. broadcasts a funded transparent spend below NU7 and mines it;
//! 2. signs a version-4 spend for the last pre-NU7 branch, then mines across
//!    the activation;
//! 3. broadcasts a funded spend after NU7 and mines it;
//! 4. submits the version-4 spend and expects `UnsupportedTransactionVersion`
//!    (ZIP 2003);
//! 5. constructs a production canonical store and wallet projection from
//!    genesis through the tip, and checks the mined spends, their consensus
//!    branches, the per-block Sapling, Orchard, and Ironwood tree sizes, the
//!    tree states, and the subtree roots against the node.
//!
//! When `ZINDER_TEST_QUERY_GRPC_ADDR` names a running `zinder-query`, every
//! broadcast goes through the public `zinder-client` remote chain index, so
//! the typed rejection is checked across the native wire boundary. Otherwise
//! the test broadcasts through the same Zebra JSON-RPC broadcaster that
//! `zinder-query` composes.
//!
//! A chain that already reached NU7 cannot exercise the crossing, so the test
//! logs and returns. CI runs it alone, before the rest of the live suite.

#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::{num::NonZeroU32, sync::Arc};

use eyre::{Result, eyre};
use serde::Deserialize;
use zinder_client::{EndpointBackedIndex as _, RemoteChainIndex, RemoteOpenOptions};
use zinder_core::{
    BlockHeight, BroadcastAccepted, BroadcastRejected, BroadcastRejectionReason, ConsensusBranchId,
    MinedTransaction, Network, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionBroadcastOutcome, TransactionId, TxStatus,
    UnixTimestampMillis,
    wire::{
        decode_rpc_transaction_id_hex, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_query::{TransactionStatus, WalletQueryApi, WalletServingQuery};
use zinder_source::{TransactionBroadcaster as _, ZebraJsonRpcSource};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;
use zinder_testkit::live::{LiveTestEnv, init, optional_env, require_live_for};
use zinder_testkit::{
    P2pkhSpendArgs, TRANSPARENT_BROADCAST_TEST_SEED, TransparentAddress, TransparentTestKey,
    WalletServingStoreFixture, ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, local_network_from_activations,
};

use crate::common::{
    LiveServingPair, coinbase_transaction_id, fetch_live_network_upgrade_activations,
    fetch_live_tip_height, live_serving_pair, regtest_generate_blocks, regtest_json_rpc_call,
    zebra_source_for_live_env,
};

const NU6_3_BRANCH_ID: ConsensusBranchId = ConsensusBranchId::new(0x37a5_165b);
const NU7_BRANCH_ID: ConsensusBranchId = ConsensusBranchId::new(0x7719_0ad9);
/// Blocks a transparent coinbase output waits before it can be spent.
const COINBASE_MATURITY_BLOCKS: u32 = 100;
/// Expiry headroom for the version-4 spend, so the node rejects it for its
/// version rather than for expiry.
const VERSION_4_EXPIRY_HEADROOM_BLOCKS: u32 = 100;
const QUERY_ENDPOINT_ENV: &str = "ZINDER_TEST_QUERY_GRPC_ADDR";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn funded_spends_cross_nu7_and_version_4_is_rejected_after_it() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let Some(nu7_height) = activations.activation_height_by_name("NU7") else {
        tracing::warn!(
            target: "zinder::live",
            event = "nu7_activation_unconfigured",
            "node advertises no NU7 activation height; start it with deploy/zebra/regtest.toml"
        );
        return Ok(());
    };
    let tip_before = fetch_live_tip_height(&env).await?;
    if tip_before.value().saturating_add(1) >= nu7_height.value() {
        tracing::warn!(
            target: "zinder::live",
            event = "nu7_activation_already_reached",
            tip = tip_before.value(),
            nu7_height = nu7_height.value(),
            "chain already reached NU7; restart from a fresh chain below the activation"
        );
        return Ok(());
    }

    let source = zebra_source_for_live_env(&env)?;
    let test_key = TransparentTestKey::from_seed_with_local_network(
        &TRANSPARENT_BROADCAST_TEST_SEED,
        local_network_from_activations(&activations),
    )
    .map_err(|error| eyre!("could not derive the broadcast test key: {error}"))?;
    let mut wallet = FundedTestKey {
        env: &env,
        broadcaster: Broadcaster::connect(source, env.network())?,
        key: test_key,
        reserved: Vec::new(),
    };

    let pre_nu7 = wallet.broadcast_funded_spend().await?;
    regtest_generate_blocks(&env, 1).await?;

    // Sign the version-4 spend while its target height is still below NU7, so
    // the signature commits to the NU6.3 branch that admits version 4.
    let version_4_spend = wallet.sign_version_4_spend(nu7_height).await?;

    let tip = fetch_live_tip_height(&env).await?;
    regtest_generate_blocks(&env, nu7_height.value().saturating_sub(tip.value())).await?;
    let post_nu7 = wallet.broadcast_funded_spend().await?;
    let rejection = wallet.broadcaster.broadcast(version_4_spend).await?;
    assert_unsupported_transaction_version(&rejection)?;
    regtest_generate_blocks(&env, 1).await?;

    let tip = fetch_live_tip_height(&env).await?;
    let served = ServedChain::construct(&env, &activations, tip).await?;
    served
        .assert_mined_below(pre_nu7, nu7_height, NU6_3_BRANCH_ID)
        .await?;
    served
        .assert_mined_at_or_above(post_nu7, nu7_height, NU7_BRANCH_ID)
        .await?;
    served
        .assert_coinbase_branches_straddle(nu7_height, &activations)
        .await?;
    served.assert_tree_sizes_match_node(&env, tip).await?;
    served
        .assert_tree_states_match_node(&env, &activations, nu7_height, tip)
        .await?;
    served.assert_subtree_roots_match_node(&env).await?;
    Ok(())
}

/// Where the test submits transactions.
enum Broadcaster {
    /// The Zebra JSON-RPC broadcaster that `zinder-query` composes.
    Node(Box<ZebraJsonRpcSource>),
    /// A running `zinder-query`, reached through the public client.
    Query(Box<RemoteChainIndex>),
}

impl Broadcaster {
    fn connect(source: ZebraJsonRpcSource, network: Network) -> Result<Self> {
        let Some(endpoint) = optional_env(QUERY_ENDPOINT_ENV)? else {
            return Ok(Self::Node(Box::new(source)));
        };
        tracing::info!(
            target: "zinder::live",
            event = "nu7_broadcast_through_query",
            %endpoint,
            "broadcasting through the running zinder-query"
        );
        Ok(Self::Query(Box::new(RemoteChainIndex::connect(
            RemoteOpenOptions { endpoint, network },
        )?)))
    }

    async fn broadcast(&self, raw_transaction: Vec<u8>) -> Result<TransactionBroadcastOutcome> {
        let raw_transaction = RawTransactionBytes::new(raw_transaction);
        match self {
            Self::Node(source) => Ok(source.broadcast_transaction(raw_transaction).await?),
            Self::Query(index) => Ok(index.broadcast_transaction(raw_transaction).await?),
        }
    }
}

/// The broadcast test key with the outpoints this test already spent.
struct FundedTestKey<'env> {
    env: &'env LiveTestEnv,
    broadcaster: Broadcaster,
    key: TransparentTestKey,
    reserved: Vec<([u8; 32], u32)>,
}

impl FundedTestKey<'_> {
    /// Spends one matured coinbase output and returns the accepted txid.
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "TransactionBroadcastOutcome is non_exhaustive; every outcome other than Accepted fails the test"
    )]
    async fn broadcast_funded_spend(&mut self) -> Result<TransactionId> {
        let spendable = self.reserve_matured_output().await?;
        let target_height = fetch_live_tip_height(self.env)
            .await?
            .value()
            .saturating_add(1);
        let recipient = self
            .key
            .scratch_recipient_address(UnixTimestampMillis::now().value());
        let raw_transaction = self
            .key
            .build_p2pkh_spend(&spend_args(&spendable, &recipient, target_height))
            .map_err(|error| eyre!("transparent signer rejected the spend: {error}"))?;
        match self.broadcaster.broadcast(raw_transaction).await? {
            TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id }) => {
                Ok(transaction_id)
            }
            outcome => Err(eyre!(
                "node rejected the funded spend at height {target_height}: {outcome:?}"
            )),
        }
    }

    /// Signs a version-4 spend for the next block, which must be below NU7.
    async fn sign_version_4_spend(&mut self, nu7_height: BlockHeight) -> Result<Vec<u8>> {
        let spendable = self.reserve_matured_output().await?;
        let target_height = fetch_live_tip_height(self.env)
            .await?
            .value()
            .saturating_add(1);
        if target_height >= nu7_height.value() {
            return Err(eyre!(
                "version-4 spend must target a pre-NU7 height; target {target_height} reaches NU7 at {}",
                nu7_height.value()
            ));
        }
        let recipient = self
            .key
            .scratch_recipient_address(UnixTimestampMillis::now().value());
        self.key
            .build_p2pkh_spend_v4(
                &spend_args(&spendable, &recipient, target_height),
                nu7_height
                    .value()
                    .saturating_add(VERSION_4_EXPIRY_HEADROOM_BLOCKS),
            )
            .map_err(|error| eyre!("transparent signer rejected the version-4 spend: {error}"))
    }

    async fn reserve_matured_output(&mut self) -> Result<SpendableOutput> {
        let target_height = fetch_live_tip_height(self.env)
            .await?
            .value()
            .saturating_add(1);
        let maturity_cutoff = target_height.saturating_sub(COINBASE_MATURITY_BLOCKS);
        let address = self.key.address_base58();
        let utxos: Vec<NodeAddressUtxo> = serde_json::from_value(
            regtest_json_rpc_call(
                self.env,
                "getaddressutxos",
                serde_json::json!([{ "addresses": [address] }]),
            )
            .await?,
        )?;
        for utxo in utxos {
            let txid_be = decode_rpc_transaction_id_hex(&utxo.txid)?.as_bytes();
            if utxo.height > maturity_cutoff
                || utxo.satoshis <= ZIP317_FEE_ONE_IN_ONE_OUT_ZATS
                || self.reserved.contains(&(txid_be, utxo.output_index))
                || !self.output_is_unspent(&utxo).await?
            {
                continue;
            }
            self.reserved.push((txid_be, utxo.output_index));
            return Ok(SpendableOutput {
                txid_be,
                vout: utxo.output_index,
                value_zats: utxo.satoshis,
            });
        }
        Err(eyre!(
            "no matured, unspent output pays {address} at target height {target_height}; \
             mine to that address with scripts/mine-live-regtest.sh"
        ))
    }

    async fn output_is_unspent(&self, utxo: &NodeAddressUtxo) -> Result<bool> {
        let output = regtest_json_rpc_call(
            self.env,
            "gettxout",
            serde_json::json!([utxo.txid, utxo.output_index]),
        )
        .await?;
        Ok(!output.is_null())
    }
}

fn spend_args<'recipient>(
    spendable: &SpendableOutput,
    recipient: &'recipient TransparentAddress,
    target_height: u32,
) -> P2pkhSpendArgs<'recipient> {
    P2pkhSpendArgs {
        coinbase_txid_be: spendable.txid_be,
        coinbase_vout: spendable.vout,
        coinbase_value_zats: spendable.value_zats,
        recipient,
        target_height,
    }
}

struct SpendableOutput {
    txid_be: [u8; 32],
    vout: u32,
    value_zats: u64,
}

#[derive(Debug, Deserialize)]
struct NodeAddressUtxo {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: u32,
    satoshis: u64,
    height: u32,
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "TransactionBroadcastOutcome is non_exhaustive; every outcome other than this rejection fails the test"
)]
fn assert_unsupported_transaction_version(outcome: &TransactionBroadcastOutcome) -> Result<()> {
    match outcome {
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::UnsupportedTransactionVersion,
            message,
            ..
        }) => {
            tracing::info!(
                target: "zinder::live",
                event = "nu7_version_4_rejected",
                %message,
                "node rejected the version-4 spend after NU7"
            );
            Ok(())
        }
        other => Err(eyre!(
            "version-4 spend after NU7 must be rejected as UnsupportedTransactionVersion; got {other:?}"
        )),
    }
}

/// A production canonical store and wallet projection constructed from
/// genesis through the node tip, read through the serving query.
struct ServedChain {
    wallet_query: WalletServingQuery<ZebraJsonRpcSource>,
    _store_fixture: WalletServingStoreFixture,
}

impl ServedChain {
    async fn construct(
        env: &LiveTestEnv,
        activations: &Arc<NetworkUpgradeActivations>,
        tip: BlockHeight,
    ) -> Result<Self> {
        let LiveServingPair {
            slot,
            source,
            store_fixture,
        } = live_serving_pair(env, activations, tip).await?;
        let wallet_query =
            WalletServingQuery::from_probed_node_source(slot, source, Arc::clone(activations))?;
        Ok(Self {
            wallet_query,
            _store_fixture: store_fixture,
        })
    }

    async fn mined(&self, transaction_id: TransactionId) -> Result<MinedTransaction> {
        let status = self.wallet_query.transaction(transaction_id, None).await?;
        let TransactionStatus {
            status: TxStatus::Mined(mined),
            ..
        } = status
        else {
            return Err(eyre!(
                "transaction {} must be mined; got {:?}",
                encode_rpc_transaction_id_hex(transaction_id),
                status.status
            ));
        };
        Ok(mined)
    }

    async fn assert_mined_below(
        &self,
        transaction_id: TransactionId,
        nu7_height: BlockHeight,
        expected_branch_id: ConsensusBranchId,
    ) -> Result<()> {
        let mined = self.mined(transaction_id).await?;
        assert!(
            mined.location.block_height < nu7_height,
            "pre-NU7 spend mined at {} at or above NU7 {}",
            mined.location.block_height.value(),
            nu7_height.value()
        );
        assert_eq!(mined.chain_context.consensus_branch_id, expected_branch_id);
        assert_eq!(transaction_version(&mined)?, 6);
        Ok(())
    }

    async fn assert_mined_at_or_above(
        &self,
        transaction_id: TransactionId,
        nu7_height: BlockHeight,
        expected_branch_id: ConsensusBranchId,
    ) -> Result<()> {
        let mined = self.mined(transaction_id).await?;
        assert!(
            mined.location.block_height >= nu7_height,
            "post-NU7 spend mined at {} below NU7 {}",
            mined.location.block_height.value(),
            nu7_height.value()
        );
        assert_eq!(mined.chain_context.consensus_branch_id, expected_branch_id);
        assert_eq!(transaction_version(&mined)?, 6);
        Ok(())
    }

    async fn assert_coinbase_branches_straddle(
        &self,
        nu7_height: BlockHeight,
        activations: &NetworkUpgradeActivations,
    ) -> Result<()> {
        for (height, expected) in [
            (
                BlockHeight::new(nu7_height.value().saturating_sub(1)),
                NU6_3_BRANCH_ID,
            ),
            (nu7_height, NU7_BRANCH_ID),
        ] {
            assert_eq!(activations.consensus_branch_id_at(height), expected);
            let coinbase = coinbase_transaction_id(&self.wallet_query, height).await?;
            let mined = self.mined(coinbase).await?;
            assert_eq!(
                mined.chain_context.consensus_branch_id,
                expected,
                "coinbase branch at height {}",
                height.value()
            );
        }
        Ok(())
    }

    /// Compares the tree sizes Zinder derives for each block with the sizes
    /// the node reports in `getblock`.
    async fn assert_tree_sizes_match_node(
        &self,
        env: &LiveTestEnv,
        tip: BlockHeight,
    ) -> Result<()> {
        let mut final_sizes = (0, 0, 0);
        for height in 1..=tip.value() {
            let compact = self
                .wallet_query
                .compact_block_at(BlockHeight::new(height), None)
                .await?;
            let metadata = compact.compact_block.chain_metadata();
            let node_block =
                regtest_json_rpc_call(env, "getblock", serde_json::json!([height.to_string(), 1]))
                    .await?;
            let node_size = |pool: &str| {
                node_block["trees"][pool]["size"]
                    .as_u64()
                    .map_or(Ok(0), u32::try_from)
            };
            let zinder_sizes = (
                metadata.sapling_commitment_tree_size,
                metadata.orchard_commitment_tree_size,
                metadata.ironwood_commitment_tree_size,
            );
            let node_sizes = (
                node_size("sapling")?,
                node_size("orchard")?,
                node_size("ironwood")?,
            );
            assert_eq!(
                zinder_sizes, node_sizes,
                "(Sapling, Orchard, Ironwood) tree sizes at height {height}"
            );
            final_sizes = zinder_sizes;
        }
        tracing::info!(
            target: "zinder::live",
            event = "nu7_tree_sizes_matched",
            tip = tip.value(),
            sapling = final_sizes.0,
            orchard = final_sizes.1,
            ironwood = final_sizes.2,
            "per-block tree sizes match the node"
        );
        assert!(
            final_sizes.0 > 0 && final_sizes.1 > 0 && final_sizes.2 > 0,
            "every shielded tree must hold notes; mine the shielded prefix with scripts/mine-live-regtest.sh"
        );
        Ok(())
    }

    /// Compares the served tree state with `z_gettreestate` at the
    /// activation heights, across NU7, and at every height that holds a
    /// stored checkpoint. Construction stores a checkpoint, built from
    /// Zinder's own commitment-tree frontiers, at each multiple of
    /// [`TREE_STATE_CHECKPOINT_STRIDE`] and at the tip; other heights are
    /// filled from the node for Zinder's canonical block.
    async fn assert_tree_states_match_node(
        &self,
        env: &LiveTestEnv,
        activations: &NetworkUpgradeActivations,
        nu7_height: BlockHeight,
        tip: BlockHeight,
    ) -> Result<()> {
        let mut heights = vec![1, nu7_height.value().saturating_sub(1), nu7_height.value()];
        heights.extend(
            ["NU5", "NU6.3"]
                .into_iter()
                .filter_map(|name| activations.activation_height_by_name(name))
                .map(BlockHeight::value),
        );
        heights.extend(
            (1..=tip.value() / TREE_STATE_CHECKPOINT_STRIDE)
                .map(|multiple| multiple * TREE_STATE_CHECKPOINT_STRIDE),
        );
        heights.push(tip.value());
        heights.sort_unstable();
        heights.dedup();
        for height in heights {
            let tree_state = self
                .wallet_query
                .tree_state_at(BlockHeight::new(height), None)
                .await?;
            let zinder_payload: serde_json::Value =
                serde_json::from_slice(&tree_state.payload_bytes)?;
            let node_payload = regtest_json_rpc_call(
                env,
                "z_gettreestate",
                serde_json::json!([height.to_string()]),
            )
            .await?;
            assert_eq!(
                encode_rpc_block_hash_hex(tree_state.block_hash),
                node_payload["hash"].as_str().unwrap_or_default(),
                "tree-state block hash at height {height}"
            );
            for pool in ["sapling", "orchard", "ironwood"] {
                assert_eq!(
                    zinder_payload[pool]["commitments"]["finalState"],
                    node_payload[pool]["commitments"]["finalState"],
                    "{pool} final state at height {height}"
                );
            }
        }
        Ok(())
    }

    async fn assert_subtree_roots_match_node(&self, env: &LiveTestEnv) -> Result<()> {
        for (protocol, pool) in [
            (ShieldedProtocol::Sapling, "sapling"),
            (ShieldedProtocol::Orchard, "orchard"),
            (ShieldedProtocol::Ironwood, "ironwood"),
        ] {
            let zinder_roots = self
                .wallet_query
                .subtree_roots(
                    SubtreeRootRange::new(
                        protocol,
                        SubtreeRootIndex::new(0),
                        NonZeroU32::new(16).ok_or_else(|| eyre!("invalid max entries"))?,
                    ),
                    None,
                )
                .await?;
            let node_roots =
                regtest_json_rpc_call(env, "z_getsubtreesbyindex", serde_json::json!([pool, 0]))
                    .await?;
            let node_subtrees = node_roots["subtrees"]
                .as_array()
                .ok_or_else(|| eyre!("z_getsubtreesbyindex {pool} returned no subtree list"))?;
            assert_eq!(
                zinder_roots.subtree_roots.len(),
                node_subtrees.len(),
                "{pool} subtree-root count"
            );
            for (zinder_root, node_root) in zinder_roots.subtree_roots.iter().zip(node_subtrees) {
                assert_eq!(
                    hex::encode(zinder_root.root_hash.as_bytes()),
                    node_root["root"].as_str().unwrap_or_default(),
                    "{pool} subtree root"
                );
            }
        }
        Ok(())
    }
}

fn transaction_version(mined: &MinedTransaction) -> Result<u32> {
    let raw = mined
        .raw_transaction_bytes
        .as_deref()
        .ok_or_else(|| eyre!("mined transaction has no retained raw bytes"))?;
    let header: [u8; 4] = raw
        .get(0..4)
        .ok_or_else(|| eyre!("raw transaction is shorter than its header"))?
        .try_into()?;
    Ok(u32::from_le_bytes(header) & 0x7fff_ffff)
}
