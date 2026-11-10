//! Shared helpers for `zinder-ingest`'s integration and live tests.
//!
//! These helpers turn a [`LiveTestEnv`] resolved from the unified env-var
//! schema into node sources, drive regtest RPCs the production source does
//! not expose, build a production serving pair from a live node, and render
//! the ingest TOML shapes the CLI tests run.

#![allow(
    dead_code,
    reason = "Each live test file consumes only a subset of the common helpers."
)]

use std::{path::Path, process::Command, sync::Arc};

use eyre::{Result, eyre};
use zinder_core::NetworkUpgradeActivations;
use zinder_core::{BlockHeight, BlockId, UnixTimestampMillis};
use zinder_ingest::{CanonicalConstructionConfig, load_fresh_canonical};
use zinder_query::{WalletQueryApi, WalletServingPairSlot, WalletServingReadPair};
use zinder_source::{NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbResourceBudget,
};
use zinder_testkit::{WalletServingStoreFixture, live::LiveTestEnv};

/// Probes the upstream node tip via a fresh source.
pub(crate) async fn fetch_live_tip_height(env: &LiveTestEnv) -> Result<BlockHeight> {
    let probe_source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    Ok(NodeSource::tip_id(&probe_source).await?.height)
}

/// Calls a raw Zebra JSON-RPC method by name.
///
/// Returns the `result` field as a `serde_json::Value`. Live tests use this
/// to drive RPCs the production `NodeSource` trait does not expose (regtest
/// `generate`, `invalidateblock`, `reconsiderblock`, `getblockhash`).
///
/// The Zebra sidecar is expected to be reachable at
/// `env.target.json_rpc_addr` with whatever auth is configured for the
/// node target; the basic-auth credentials carried by `env.target.node_auth`
/// are passed through as the `-u` argument when present so the same helper
/// works against a sidecar configured with `ZEBRA_RPC__ENABLE_COOKIE_AUTH=false`
/// and a `username:password` pair (the default ZFND `z3` setup).
pub(crate) async fn regtest_json_rpc_call(
    env: &LiveTestEnv,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    use secrecy::ExposeSecret;
    use zinder_source::NodeAuth;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    })
    .to_string();
    let mut command = tokio::process::Command::new("curl");
    command
        .arg("-s")
        .args(["-X", "POST"])
        .args(["-H", "content-type: application/json"])
        .arg("-d")
        .arg(&body);
    if let NodeAuth::Basic { username, password } = &env.target.node_auth {
        command
            .arg("-u")
            .arg(format!("{username}:{}", password.expose_secret()));
    }
    command.arg(env.target.json_rpc_addr.as_str());
    let output = command.output().await?;
    if !output.status.success() {
        return Err(eyre!(
            "{method} curl exited with status {:?}: stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let response_body = String::from_utf8(output.stdout)?;
    let parsed: serde_json::Value = serde_json::from_str(&response_body)
        .map_err(|error| eyre!("{method} response is not JSON: {error}; body={response_body}"))?;
    if let Some(error_field) = parsed.get("error")
        && !error_field.is_null()
    {
        return Err(eyre!("{method} RPC returned error: {error_field}"));
    }
    parsed
        .get("result")
        .cloned()
        .ok_or_else(|| eyre!("{method} response missing result field; body={response_body}"))
}

/// Calls Zebra's regtest-only `generate` JSON-RPC to mine `block_count` empty
/// blocks. Returns the list of newly mined block hashes.
///
/// Live tests use this to drive deterministic chain-tip changes against the
/// regtest sidecar without depending on a wallet-side broadcast cycle. Only
/// useful on networks that accept the `generate` RPC; on testnet/mainnet
/// Zebra the call returns an error.
pub(crate) async fn regtest_generate_blocks(
    env: &LiveTestEnv,
    block_count: u32,
) -> Result<Vec<String>> {
    let rpc_result =
        regtest_json_rpc_call(env, "generate", serde_json::json!([block_count])).await?;
    let block_hashes: Vec<String> = serde_json::from_value(rpc_result)
        .map_err(|error| eyre!("regtest generate result is not a list of block hashes: {error}"))?;
    Ok(block_hashes)
}

/// Calls Zebra's regtest-only `invalidateblock` JSON-RPC.
///
/// Drops the named block from the canonical chain along with every
/// descendant, so the tip rolls back to the block's parent. Used by
/// reorg-sweep live tests to force `ChainReorged` chain events.
pub(crate) async fn rpc_invalidate_block(env: &LiveTestEnv, block_hash_hex: &str) -> Result<()> {
    regtest_json_rpc_call(env, "invalidateblock", serde_json::json!([block_hash_hex])).await?;
    Ok(())
}

/// Calls Zebra's regtest-only `reconsiderblock` JSON-RPC.
///
/// Restores a block previously marked invalid via `invalidateblock`.
/// Best-effort cleanup helper for reorg-sweep tests that want to leave the
/// sidecar in a clean state for subsequent runs.
pub(crate) async fn rpc_reconsider_block(env: &LiveTestEnv, block_hash_hex: &str) -> Result<()> {
    regtest_json_rpc_call(env, "reconsiderblock", serde_json::json!([block_hash_hex])).await?;
    Ok(())
}

/// Fetches the network-upgrade activation table from the live node behind `env`.
///
/// Returns the discovered table wrapped in an `Arc` so live tests can thread
/// it through wallet/compat adapter constructors. The compat shim's
/// `GetLightdInfo` rejects requests when the activation table's `network`
/// disagrees with the chain epoch's `network`, so regtest tests may use the
/// hand-built `sample_regtest_upgrade_activations` fixture only when running
/// against a regtest node; tests that opt into testnet or mainnet must
/// discover the table at runtime.
pub(crate) async fn fetch_live_network_upgrade_activations(
    env: &LiveTestEnv,
) -> Result<Arc<NetworkUpgradeActivations>> {
    let source = zebra_source_for_live_env(env)?;
    let activations = source.fetch_network_upgrade_activations().await?;
    Ok(Arc::new(activations))
}

pub(crate) fn zebra_source_for_live_env(env: &LiveTestEnv) -> Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

/// Reorg window for canonical stores built from a live node in tests.
const LIVE_CANONICAL_REORG_WINDOW_BLOCKS: u32 = 100;

/// Production serving pair built from a live node, with the probed source
/// that built it.
pub(crate) struct LiveServingPair {
    /// Exact admitted canonical and wallet secondaries.
    pub(crate) slot: WalletServingPairSlot,
    /// Probed node source for node-backed query methods.
    pub(crate) source: ZebraJsonRpcSource,
    /// Owner of the store directories; dropping it removes them.
    pub(crate) store_fixture: WalletServingStoreFixture,
}

/// Builds a READY production canonical store holding the live chain from
/// genesis through `build_tip_height`, projects its wallet store, and admits
/// one serving pair.
///
/// Construction runs the same fresh-load and baseline publication path the
/// canonical writer uses, retaining transaction bytes and settling the build
/// tip. History is genesis-complete because the wallet projection must
/// resolve every transparent input's predecessor output; a checkpoint-bounded
/// window fails that check as soon as it spends an older output.
pub(crate) async fn live_serving_pair(
    env: &LiveTestEnv,
    activations: &Arc<NetworkUpgradeActivations>,
    build_tip_height: BlockHeight,
) -> Result<LiveServingPair> {
    let source = zebra_source_for_live_env(env)?;
    source.probe_capabilities().await?;
    let build_tip_block = source.fetch_block_at(build_tip_height).await?;
    let build_tip = BlockId::new(build_tip_block.height, build_tip_block.hash);
    let reorg_policy = CanonicalReorgPolicy::new(LIVE_CANONICAL_REORG_WINDOW_BLOCKS)?;
    let genesis_block = source.fetch_block_at(BlockHeight::new(0)).await?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        genesis_block.block_time_seconds,
        build_tip,
        RawBlobRetention::Transactions,
        reorg_policy,
    )?;
    let temporary_directory = tempfile::tempdir()?;
    let canonical_primary_path = temporary_directory.path().join("canonical-primary");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &canonical_primary_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        env.target.request_timeout,
        Arc::clone(activations),
    );
    let loaded = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = loaded.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        build_tip,
        UnixTimestampMillis::now(),
    ))?;
    let canonical_primary = validated.publish_baseline(publication)?;
    let mut store_fixture = WalletServingStoreFixture::from_published_canonical(
        temporary_directory,
        &canonical_primary_path,
        canonical_primary,
        activations,
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?));
    Ok(LiveServingPair {
        slot,
        source,
        store_fixture,
    })
}

/// Returns the coinbase transaction id at `height`.
///
/// The coinbase is the block's first transaction, and its compact form always
/// carries the coinbase's outputs, so the compact block's first entry at
/// index 0 identifies it without a block-index transaction lookup.
pub(crate) async fn coinbase_transaction_id(
    query: &impl WalletQueryApi,
    height: BlockHeight,
) -> Result<zinder_core::TransactionId> {
    let block = query.compact_block_at(height, None).await?;
    block
        .compact_block
        .transactions()
        .first()
        .filter(|transaction| transaction.index == 0)
        .map(|transaction| transaction.transaction_id)
        .ok_or_else(|| eyre!("compact block at height {} has no coinbase", height.value()))
}

/// Calls Zebra's `getblockhash` JSON-RPC and returns the canonical block
/// hash at `height` in Zebra's display (big-endian) encoding.
pub(crate) async fn rpc_block_hash_at_height(env: &LiveTestEnv, height: u32) -> Result<String> {
    let rpc_result =
        regtest_json_rpc_call(env, "getblockhash", serde_json::json!([height])).await?;
    let block_hash: String = serde_json::from_value(rpc_result)
        .map_err(|error| eyre!("getblockhash result is not a hex string: {error}"))?;
    Ok(block_hash)
}

/// Returns a `Command` that runs the in-tree `zinder-ingest` binary with a
/// fully cleared environment, ready for env-var injection.
#[must_use]
pub(crate) fn zinder_ingest_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zinder-ingest"));
    command.env_clear();
    command
}

/// Builder for a bounded phase-driven ingest TOML config used by the CLI live tests.
pub(crate) struct BoundedIngestConfigToml<'fields> {
    pub(crate) network_name: &'fields str,
    pub(crate) json_rpc_addr: &'fields str,
    pub(crate) node_auth_username: &'fields str,
    pub(crate) node_auth_password: &'fields str,
    pub(crate) storage_path: &'fields Path,
    pub(crate) target_height: u32,
    pub(crate) request_timeout_secs: u64,
}

/// Builder for a wallet-serving bounded phase-driven ingest TOML config used by the
/// CLI live tests.
pub(crate) struct WalletServingIngestConfigToml<'fields> {
    pub(crate) network_name: &'fields str,
    pub(crate) json_rpc_addr: &'fields str,
    pub(crate) node_auth_username: &'fields str,
    pub(crate) node_auth_password: &'fields str,
    pub(crate) storage_path: &'fields Path,
    pub(crate) target_height: u32,
    pub(crate) request_timeout_secs: u64,
}

/// Renders a `BoundedIngestConfigToml` into the TOML shape `zinder-ingest` accepts.
///
/// The `[ingest.run_overrides].target_height` field makes the ingest loop exit
/// with status 0 once the canonical store reaches that height.
pub(crate) fn bounded_ingest_config_toml(
    config_toml: &BoundedIngestConfigToml<'_>,
) -> Result<String> {
    Ok(format!(
        r#"[network]
name = "{}"

[ops]
listen_addr = "127.0.0.1:0"

[node]
json_rpc_addr = "{}"
request_timeout_secs = {}

[node.auth]
method = "basic"
username = "{}"
password = "{}"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.run_overrides]
target_height = {}

[ingest_control]
listen_addr = "127.0.0.1:0"
"#,
        config_toml.network_name,
        config_toml.json_rpc_addr,
        config_toml.request_timeout_secs,
        config_toml.node_auth_username,
        config_toml.node_auth_password,
        path_str(config_toml.storage_path)?,
        config_toml.target_height,
    ))
}

/// Renders a wallet-serving bounded phase-driven ingest TOML config.
///
/// The `coverage = "wallet-serving"` modifier instructs the loop to retain
/// complete non-genesis history before committing.
pub(crate) fn wallet_serving_ingest_config_toml(
    config_toml: &WalletServingIngestConfigToml<'_>,
) -> Result<String> {
    Ok(format!(
        r#"[network]
name = "{}"

[ops]
listen_addr = "127.0.0.1:0"

[node]
json_rpc_addr = "{}"
request_timeout_secs = {}

[node.auth]
method = "basic"
username = "{}"
password = "{}"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.run_overrides]
coverage = "wallet-serving"
target_height = {}

[ingest_control]
listen_addr = "127.0.0.1:0"
"#,
        config_toml.network_name,
        config_toml.json_rpc_addr,
        config_toml.request_timeout_secs,
        config_toml.node_auth_username,
        config_toml.node_auth_password,
        path_str(config_toml.storage_path)?,
        config_toml.target_height,
    ))
}

/// Returns the node's basic-auth `(username, password)` from a resolved
/// live-test env, or an error if the env did not select Basic.
pub(crate) fn basic_auth_credentials(env: &LiveTestEnv) -> Result<(&str, &str)> {
    use secrecy::ExposeSecret;
    use zinder_source::NodeAuth;
    match &env.target.node_auth {
        NodeAuth::Basic { username, password } => Ok((username, password.expose_secret())),
        NodeAuth::None | NodeAuth::Cookie(_) => Err(eyre!(
            "live CLI test requires basic auth; set ZINDER_NODE__AUTH__METHOD=basic"
        )),
    }
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre!("path is not valid UTF-8: {}", path.display()))
}
