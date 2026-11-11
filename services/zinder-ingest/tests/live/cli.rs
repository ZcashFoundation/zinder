#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::{fs, path::Path};

use eyre::{Result, eyre};
use tempfile::tempdir;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_core::{BlockHeight, BlockHeightRange, Network, NetworkUpgradeActivations};
use zinder_source::NodeSource;
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::live::{init, require_live, require_live_for};

use crate::common::{
    BoundedIngestConfigToml, WalletServingIngestConfigToml, basic_auth_credentials,
    bounded_ingest_config_toml, fetch_live_network_upgrade_activations,
    wallet_serving_ingest_config_toml, zebra_source_for_live_env, zinder_ingest_command,
};

const WALLET_SERVING_BOUNDED_DEPTH_BLOCKS: u32 = 150;

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn cli_constructs_bounded_canonical_store_from_config() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let (username, password) = basic_auth_credentials(&env)?;

    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("zinder-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let target_height = match env.network() {
        Network::ZcashRegtest => BlockHeight::new(1),
        Network::ZcashTestnet | Network::ZcashMainnet => BlockHeight::new(2),
        other => return Err(eyre!("unsupported network for CLI test: {other:?}")),
    };
    fs::write(
        &config_path,
        bounded_ingest_config_toml(&BoundedIngestConfigToml {
            network_name: encode_zinder_native_chain_name(env.network()),
            json_rpc_addr: &env.target.json_rpc_addr,
            node_auth_username: username,
            node_auth_password: password,
            storage_path: &storage_path,
            target_height: target_height.value(),
            request_timeout_secs: env.target.request_timeout.as_secs(),
        })?,
    )?;

    let output = zinder_ingest_command()
        .args([
            "--config",
            config_path
                .to_str()
                .ok_or_else(|| eyre!("config path not utf-8"))?,
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("event=\"canonical_fresh_construction_ready\""),
        "{stderr}"
    );
    assert!(stderr.contains("chain_epoch=1"), "{stderr}");
    assert!(
        stderr.contains(&format!("visible_tip_height={}", target_height.value())),
        "{stderr}"
    );

    // The bounded config leaves `storage.raw_blob_policy` at its default.
    let store = open_wallet_canonical_store(
        &storage_path,
        &activations,
        zinder_store::RawBlobRetention::None,
    )?;
    let compact_block = store
        .compact_block_at(target_height)?
        .ok_or_else(|| eyre!("missing tip compact block"))?;

    assert_eq!(store.network(), env.network());
    assert_eq!(store.workload(), CanonicalStoreWorkload::Wallet);
    assert_eq!(store.event_fence().visible_tip().height, target_height);
    assert_eq!(compact_block.height(), target_height);
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "the live CLI scenario keeps node-derived range selection, process execution, and cold READY-store admission in one auditable flow"
)]
async fn cli_constructs_complete_wallet_serving_history_from_config() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashTestnet, Network::ZcashMainnet])? else {
        return Ok(());
    };
    let (username, password) = basic_auth_credentials(&env)?;

    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("zinder-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let source = zebra_source_for_live_env(&env)?;
    let first_retained_height = BlockHeight::new(1);
    let checkpoint_height = BlockHeight::new(0);
    let to_height = BlockHeight::new(WALLET_SERVING_BOUNDED_DEPTH_BLOCKS);
    let node_tip = NodeSource::tip_id(&source).await?.height;
    if node_tip.value() <= to_height.value() {
        return Err(eyre!(
            "complete wallet-serving bounded test needs tip above {}; got {}",
            to_height.value(),
            node_tip.value()
        ));
    }

    fs::write(
        &config_path,
        wallet_serving_ingest_config_toml(&WalletServingIngestConfigToml {
            network_name: encode_zinder_native_chain_name(env.network()),
            json_rpc_addr: &env.target.json_rpc_addr,
            node_auth_username: username,
            node_auth_password: password,
            storage_path: &storage_path,
            target_height: to_height.value(),
            request_timeout_secs: env.target.request_timeout.as_secs(),
        })?,
    )?;

    let output = zinder_ingest_command()
        .args([
            "--config",
            config_path
                .to_str()
                .ok_or_else(|| eyre!("config path not utf-8"))?,
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("event=\"wallet_serving_modifiers_resolved\""),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!("from_height={}", first_retained_height.value())),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!("checkpoint_height={}", checkpoint_height.value())),
        "{stderr}"
    );
    assert!(
        stderr.contains("event=\"canonical_fresh_construction_ready\""),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!("visible_tip_height={}", to_height.value())),
        "{stderr}"
    );

    let store = open_wallet_canonical_store(
        &storage_path,
        &activations,
        zinder_store::RawBlobRetention::Transactions,
    )?;
    assert_eq!(store.network(), env.network());
    assert_eq!(store.workload(), CanonicalStoreWorkload::Wallet);
    assert_eq!(store.event_fence().visible_tip().height, to_height);
    assert_eq!(
        store.history_bounds().first_available_height(),
        first_retained_height
    );
    assert_eq!(
        store
            .history_bounds()
            .preceding_checkpoint()
            .map(|checkpoint| checkpoint.height),
        None
    );
    assert!(store.compact_block_at(checkpoint_height)?.is_none());
    assert!(store.compact_block_at(first_retained_height)?.is_some());
    assert!(
        store
            .tree_state_checkpoint_at_or_before(to_height)?
            .is_some()
    );
    let compact_blocks = store.compact_blocks_in_range(BlockHeightRange::inclusive(
        first_retained_height,
        to_height,
    ))?;
    assert_eq!(
        compact_blocks.len(),
        usize::try_from(WALLET_SERVING_BOUNDED_DEPTH_BLOCKS)?
    );
    Ok(())
}

fn open_wallet_canonical_store(
    storage_path: &Path,
    activations: &NetworkUpgradeActivations,
    raw_blob_retention: zinder_store::RawBlobRetention,
) -> Result<RocksDbCanonicalStore> {
    Ok(RocksDbCanonicalStore::open_ready(
        storage_path,
        activations,
        CanonicalStoreWorkload::Wallet,
        raw_blob_retention,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::canonical_writer_defaults(),
    )?)
}
