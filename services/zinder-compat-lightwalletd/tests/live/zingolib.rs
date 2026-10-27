//! Zingolib real-client certification against Zinder's lightwalletd adapter.
//!
//! The gate is deliberately client-specific. It calls the published Zingolib
//! CLI rather than reproducing wallet behavior through Zinder's generated
//! client. That proves a real independent scanner can bootstrap, restore,
//! discover transparent funds, shield them, observe the pending transaction,
//! and discover the mined Orchard note.

#![allow(
    missing_docs,
    reason = "Live test names describe the independently versioned client flow."
)]

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use secrecy::ExposeSecret as _;
use serde_json::Value;
use tokio::process::Command;
use tonic::transport::Endpoint;
use zcash_address::{ConversionError, TryFromAddress, ZcashAddress};
use zcash_protocol::consensus::NetworkType;
use zinder_core::{
    Network, TransactionId, UnixTimestampMillis,
    wire::{decode_rpc_transaction_id_hex, encode_rpc_transaction_id_hex},
};
use zinder_proto::compat::lightwalletd::{
    self, BlockId, BlockRange, ChainSpec, Empty,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_source::{NodeAuth, NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::live::{LiveTestEnv, init, optional_env, require_live_for};
use zinder_testkit::{
    P2pkhSpendArgs, TRANSPARENT_BROADCAST_TEST_SEED, TransparentAddress, TransparentTestKey,
    ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, local_network_from_activations,
};

const ZINGOLIB_BINARY_ENV: &str = "ZINDER_TEST_ZINGOLIB_BIN";
const ZINGOLIB_SERVER_ENV: &str = "ZINDER_TEST_ZINGOLIB_SERVER";
const ZINGOLIB_VERSION: &str = "5.0.0";
const ZINGOLIB_UPGRADE_CEILING: &str = "NU6.2";
const ZINGOLIB_BIRTHDAY: u64 = 1;
const ZINGOLIB_CONFIRMATION_BLOCKS: u32 = 3;
const MEMPOOL_OBSERVE_TIMEOUT: Duration = Duration::from_secs(20);
const WALLET_SERVING_READY_TIMEOUT: Duration = Duration::from_secs(20);
const PUBLIC_REGTEST_TEST_VECTOR_SEED: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "The ordered client workflow is the certification contract; splitting it would obscure its externally observable stages."
)]
async fn zingolib_bootstrap_restore_shield_and_mempool_flow_certifies_zinder() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let Some(config) = ZingolibCertificationConfig::from_environment()? else {
        return Ok(());
    };

    let source = zebra_source(&env)?;
    let schedule = source
        .fetch_network_upgrade_activations()
        .await
        .map_err(|error| eyre!("could not fetch node upgrade schedule: {error}"))?;
    let node_tip = source
        .tip_id()
        .await
        .map_err(|error| eyre!("could not fetch node tip: {error}"))?
        .height
        .value();
    assert_zingolib_supported_upgrade_range(&schedule, u64::from(node_tip))?;

    let mut zinder =
        CompactTxStreamerClient::new(Endpoint::new(config.server.clone())?.connect().await?);
    let lightd_info = zinder.get_lightd_info(Empty {}).await?.into_inner();
    if !lightd_info.taddr_support {
        return Err(eyre!(
            "Zinder must advertise taddrSupport=true for Zingolib certification"
        ));
    }

    let temporary_wallets = tempfile::tempdir()?;
    let created_wallet_dir = temporary_wallets.path().join("created");
    let restored_wallet_dir = temporary_wallets.path().join("restored");
    let create_output = run_zingolib(
        &config,
        &created_wallet_dir,
        Some(PUBLIC_REGTEST_TEST_VECTOR_SEED),
        false,
        "t_addresses",
        &[],
    )
    .await?;
    let recipient_address = zingolib_transparent_address(&create_output)?;
    let recipient = transparent_address_from_base58(&recipient_address)?;

    let test_key = transparent_broadcast_test_key(&source).await?;
    let test_address = test_key.address_base58();
    let target_height = zinder
        .get_latest_block(ChainSpec {})
        .await?
        .into_inner()
        .height
        .saturating_add(1);
    let pending_spends = pending_transparent_spend_outpoints(&mut zinder).await?;
    let spendable_utxo =
        select_spendable_utxo(&mut zinder, &test_address, target_height, &pending_spends).await?;
    let funding_transaction = test_key
        .build_p2pkh_spend(&P2pkhSpendArgs {
            coinbase_txid_be: spendable_utxo.txid,
            coinbase_vout: spendable_utxo.vout,
            coinbase_value_zats: spendable_utxo.value_zats,
            recipient: &recipient,
            target_height: spendable_utxo.target_height,
        })
        .map_err(|error| eyre!("could not construct Zingolib funding transaction: {error}"))?;
    let funding = zinder
        .send_transaction(lightwalletd::RawTransaction {
            data: funding_transaction.clone(),
            height: 0,
        })
        .await?
        .into_inner();
    if funding.error_code != 0 {
        let encoded_branch_id = transaction_branch_id(&funding_transaction)?;
        return Err(eyre!(
            "Zinder rejected the Zingolib funding transaction: code={} branch_id={encoded_branch_id:#010x} message={}",
            funding.error_code,
            funding.error_message
        ));
    }
    let funding_txid = decode_rpc_transaction_id_hex(&funding.error_message)
        .map_err(|error| eyre!("Zinder SendTransaction success did not contain a txid: {error}"))?;
    wait_for_mempool_transaction(&mut zinder, funding_txid.as_bytes()).await?;
    regtest_generate_blocks(&env, ZINGOLIB_CONFIRMATION_BLOCKS).await?;
    wait_for_wallet_serving_at_node_tip(&mut zinder, &source, &test_address).await?;

    let transparent_discovery_output =
        run_zingolib(&config, &created_wallet_dir, None, true, "coins", &[]).await?;
    assert_output_contains_txid(
        &transparent_discovery_output,
        funding_txid.as_bytes(),
        "created-wallet coins",
    )?;

    let restore_output = run_zingolib(
        &config,
        &restored_wallet_dir,
        Some(PUBLIC_REGTEST_TEST_VECTOR_SEED),
        true,
        "coins",
        &[],
    )
    .await?;
    assert_output_contains_txid(
        &restore_output,
        funding_txid.as_bytes(),
        "restored-wallet coins",
    )?;

    let send_output =
        run_zingolib(&config, &created_wallet_dir, None, true, "quickshield", &[]).await?;
    let shield_txid = zingolib_transaction_id(&send_output, "quickshield")?;
    wait_for_zingolib_mempool_transaction(&config, &created_wallet_dir, shield_txid).await?;

    regtest_generate_blocks(&env, ZINGOLIB_CONFIRMATION_BLOCKS).await?;
    wait_for_wallet_serving_at_node_tip(&mut zinder, &source, &test_address).await?;
    let shielded_note_output =
        run_zingolib(&config, &created_wallet_dir, None, true, "notes", &[]).await?;
    assert_output_contains_txid(
        &shielded_note_output,
        shield_txid.as_bytes(),
        "Zingolib notes",
    )?;
    assert_nonempty_orchard_notes(&shielded_note_output)?;

    Ok(())
}

struct ZingolibCertificationConfig {
    binary: PathBuf,
    server: String,
}

impl ZingolibCertificationConfig {
    fn from_environment() -> Result<Option<Self>> {
        let binary = optional_env(ZINGOLIB_BINARY_ENV)?;
        let server = optional_env(ZINGOLIB_SERVER_ENV)?;
        if binary.is_none() && server.is_none() {
            return Ok(None);
        }
        let (Some(binary), Some(server)) = (binary, server) else {
            return Err(eyre!(
                "{ZINGOLIB_BINARY_ENV} and {ZINGOLIB_SERVER_ENV} must be set together"
            ));
        };
        let binary = PathBuf::from(binary);
        if !binary.is_file() {
            return Err(eyre!(
                "{ZINGOLIB_BINARY_ENV} is not a file: {}",
                binary.display()
            ));
        }
        Ok(Some(Self { binary, server }))
    }
}

struct SpendableUtxo {
    txid: [u8; 32],
    vout: u32,
    value_zats: u64,
    target_height: u32,
}

fn zebra_source(env: &LiveTestEnv) -> Result<ZebraJsonRpcSource> {
    ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )
    .map_err(Into::into)
}

async fn transparent_broadcast_test_key(source: &ZebraJsonRpcSource) -> Result<TransparentTestKey> {
    let schedule = source
        .fetch_network_upgrade_activations()
        .await
        .map_err(|error| eyre!("could not fetch node upgrade schedule: {error}"))?;
    TransparentTestKey::from_seed_with_local_network(
        &TRANSPARENT_BROADCAST_TEST_SEED,
        local_network_from_activations(&schedule),
    )
    .map_err(|error| eyre!("could not derive transparent funding key: {error}"))
}

fn assert_zingolib_supported_upgrade_range(
    schedule: &zinder_core::NetworkUpgradeActivations,
    node_tip: u64,
) -> Result<()> {
    if let Some(activation_height) = schedule.activation_height_by_name("NU6.3")
        && u64::from(activation_height.value()) <= node_tip
    {
        return Err(eyre!(
            "Zingolib {ZINGOLIB_VERSION} supports this certification gate only through {ZINGOLIB_UPGRADE_CEILING}; node tip {node_tip} is at or after NU6.3 activation height {}",
            activation_height.value()
        ));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "Each argument corresponds directly to a Zingolib CLI invocation field used by the certification workflow."
)]
async fn run_zingolib(
    config: &ZingolibCertificationConfig,
    wallet_dir: &Path,
    seed: Option<&str>,
    wait_for_sync: bool,
    command: &str,
    args: &[&str],
) -> Result<String> {
    let mut process = Command::new(&config.binary);
    process
        .arg("--chain")
        .arg("regtest")
        .arg("--server")
        .arg(&config.server)
        .arg("--data-dir")
        .arg(wallet_dir);
    if let Some(seed) = seed {
        process
            .arg("--seed")
            .arg(seed)
            .arg("--birthday")
            .arg(ZINGOLIB_BIRTHDAY.to_string());
    }
    if wait_for_sync {
        process.arg("--waitsync");
    } else {
        process.arg("--nosync");
    }
    process.arg(command).args(args);
    let output = process.output().await?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    if !output.status.success() {
        return Err(eyre!(
            "Zingolib {command} failed with {:?}: {stderr}",
            output.status.code()
        ));
    }
    if stdout.to_ascii_lowercase().contains("error:")
        || stderr.to_ascii_lowercase().contains("error:")
    {
        return Err(eyre!(
            "Zingolib {command} reported an error: {stdout}\n{stderr}"
        ));
    }
    if let Ok(json) = first_json_value(&stdout, command)
        && let Some(error) = json.get("error").and_then(Value::as_str)
    {
        return Err(eyre!("Zingolib {command} reported an error: {error}"));
    }
    Ok(stdout)
}

fn zingolib_transparent_address(output: &str) -> Result<String> {
    let json = first_json_value(output, "t_addresses")?;
    json.as_array()
        .and_then(|addresses| addresses.first())
        .and_then(|address| address.get("encoded_address"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| eyre!("Zingolib t_addresses response did not contain encoded_address"))
}

struct DecodedTransparentAddress(TransparentAddress);

impl TryFromAddress for DecodedTransparentAddress {
    type Error = std::convert::Infallible;

    fn try_from_transparent_p2pkh(
        _net: NetworkType,
        pubkey_hash: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self(TransparentAddress::PublicKeyHash(pubkey_hash)))
    }

    fn try_from_transparent_p2sh(
        _net: NetworkType,
        script_hash: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self(TransparentAddress::ScriptHash(script_hash)))
    }
}

fn transparent_address_from_base58(encoded: &str) -> Result<TransparentAddress> {
    let DecodedTransparentAddress(address) = ZcashAddress::try_from_encoded(encoded)
        .map_err(|error| eyre!("Zingolib transparent address did not decode: {error}"))?
        .convert::<DecodedTransparentAddress>()
        .map_err(|error| eyre!("Zingolib address is not a transparent receiver: {error:?}"))?;
    Ok(address)
}

fn zingolib_transaction_id(output: &str, command: &str) -> Result<zinder_core::TransactionId> {
    let json = first_json_value(output, command)?;
    let txid = json
        .get("txids")
        .and_then(Value::as_array)
        .and_then(|txids| txids.first())
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("Zingolib {command} response did not contain a transaction id"))?;
    decode_rpc_transaction_id_hex(txid)
        .map_err(|error| eyre!("Zingolib {command} txid was invalid: {error}"))
}

fn first_json_value(output: &str, command: &str) -> Result<Value> {
    let start = output
        .char_indices()
        .find_map(|(index, character)| matches!(character, '{' | '[').then_some(index))
        .ok_or_else(|| eyre!("Zingolib {command} did not produce JSON: {output}"))?;
    serde_json::Deserializer::from_str(&output[start..])
        .into_iter::<Value>()
        .next()
        .transpose()?
        .ok_or_else(|| eyre!("Zingolib {command} produced empty JSON output"))
}

async fn pending_transparent_spend_outpoints(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
) -> Result<HashSet<([u8; 32], u32)>> {
    let transactions = drain_compact_transactions(
        client
            .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: Vec::new(),
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            })
            .await?
            .into_inner(),
    )
    .await?;
    let mut outpoints = HashSet::new();
    for transaction in transactions {
        for input in transaction.vin {
            if let Ok(txid) = input.prevout_txid.as_slice().try_into() {
                outpoints.insert((txid, input.prevout_index));
            }
        }
    }
    Ok(outpoints)
}

async fn select_spendable_utxo(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    address: &str,
    target_height: u64,
    pending_spends: &HashSet<([u8; 32], u32)>,
) -> Result<SpendableUtxo> {
    let response = client
        .get_address_utxos(lightwalletd::GetAddressUtxosArg {
            addresses: vec![address.to_owned()],
            start_height: 1,
            max_entries: 500,
        })
        .await?
        .into_inner();
    let maturity_cutoff = target_height.saturating_sub(100);
    for utxo in response.address_utxos {
        if utxo.height > maturity_cutoff || utxo.value_zat <= 0 {
            continue;
        }
        // lightwalletd `bytes` txids use the Zcash internal byte order, which
        // is also the outpoint wire order the transparent signer expects.
        let txid: [u8; 32] = utxo
            .txid
            .as_slice()
            .try_into()
            .map_err(|_| eyre!("Zinder UTXO txid must be 32 bytes"))?;
        let vout = u32::try_from(utxo.index)
            .map_err(|_| eyre!("Zinder UTXO index must be non-negative"))?;
        let value_zats = u64::try_from(utxo.value_zat)
            .map_err(|_| eyre!("Zinder UTXO value must be non-negative"))?;
        if pending_spends.contains(&(txid, vout)) || value_zats <= ZIP317_FEE_ONE_IN_ONE_OUT_ZATS {
            continue;
        }
        return Ok(SpendableUtxo {
            txid,
            vout,
            value_zats,
            target_height: u32::try_from(target_height)
                .map_err(|_| eyre!("target height {target_height} exceeds u32"))?,
        });
    }
    Err(eyre!(
        "no mature transparent UTXO for Zingolib funding; mine at least 125 blocks to {}",
        address
    ))
}

async fn wait_for_mempool_transaction(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    expected_txid: [u8; 32],
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < MEMPOOL_OBSERVE_TIMEOUT {
        let transactions = drain_compact_transactions(
            client
                .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
                    exclude_txid_suffixes: Vec::new(),
                    pool_types: vec![lightwalletd::PoolType::Transparent as i32],
                })
                .await?
                .into_inner(),
        )
        .await?;
        if transactions
            .iter()
            .any(|transaction| transaction.txid.as_slice() == expected_txid)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(eyre!(
        "Zinder GetMempoolTx did not expose funding transaction {}",
        hex::encode(expected_txid)
    ))
}

async fn drain_compact_transactions(
    mut stream: tonic::Streaming<lightwalletd::CompactTx>,
) -> Result<Vec<lightwalletd::CompactTx>> {
    let mut transactions = Vec::new();
    while let Some(transaction) = stream.message().await? {
        transactions.push(transaction);
    }
    Ok(transactions)
}

async fn regtest_generate_blocks(env: &LiveTestEnv, block_count: u32) -> Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": UnixTimestampMillis::now().value(),
        "method": "generate",
        "params": [block_count],
    })
    .to_string();
    let mut command = Command::new("curl");
    command
        .arg("-s")
        .args(["-X", "POST"])
        .args(["-H", "content-type: application/json"])
        .arg("-d")
        .arg(body);
    match &env.target.node_auth {
        NodeAuth::Basic { username, password } => {
            command
                .arg("-u")
                .arg(format!("{username}:{}", password.expose_secret()));
        }
        NodeAuth::Cookie(source) => {
            let credentials = source
                .read_credentials()
                .map_err(|error| eyre!("could not read Zebra RPC cookie: {error}"))?;
            command.arg("-u").arg(credentials.expose_secret());
        }
        NodeAuth::None => {}
    }
    let output = command.arg(&env.target.json_rpc_addr).output().await?;
    if !output.status.success() {
        return Err(eyre!(
            "Zebra generate({block_count}) failed with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let response: Value = serde_json::from_slice(&output.stdout)?;
    if !response["error"].is_null() {
        return Err(eyre!(
            "Zebra generate({block_count}) returned {}",
            response["error"]
        ));
    }
    let hashes = response["result"]
        .as_array()
        .ok_or_else(|| eyre!("Zebra generate({block_count}) did not return block hashes"))?;
    if hashes.len() != usize::try_from(block_count)? {
        return Err(eyre!(
            "Zebra generate({block_count}) returned {} hashes",
            hashes.len()
        ));
    }
    Ok(())
}

fn assert_output_contains_txid(output: &str, txid: [u8; 32], surface: &str) -> Result<()> {
    let txid = encode_rpc_transaction_id_hex(TransactionId::from_bytes(txid));
    if output.to_ascii_lowercase().contains(&txid) {
        Ok(())
    } else {
        Err(eyre!("{surface} did not contain transaction {txid}"))
    }
}

async fn wait_for_zingolib_mempool_transaction(
    config: &ZingolibCertificationConfig,
    wallet_dir: &Path,
    transaction_id: TransactionId,
) -> Result<String> {
    let expected_txid = encode_rpc_transaction_id_hex(transaction_id);
    let deadline = Instant::now() + MEMPOOL_OBSERVE_TIMEOUT;
    let mut last_output = String::new();
    while Instant::now() < deadline {
        let output = run_zingolib(config, wallet_dir, None, true, "transactions", &[]).await?;
        if zingolib_reports_mempool_transaction(&output, &expected_txid) {
            return Ok(output);
        }
        last_output = output;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(eyre!(
        "Zingolib did not report shield transaction {expected_txid} as mempool within {MEMPOOL_OBSERVE_TIMEOUT:?}: {last_output}"
    ))
}

fn zingolib_reports_mempool_transaction(output: &str, expected_txid: &str) -> bool {
    let Some(transaction_start) = output.find(&format!("txid: {expected_txid}")) else {
        return false;
    };
    let transaction_output = &output[transaction_start..];
    let transaction_end = transaction_output
        .find("\n{\n    txid:")
        .unwrap_or(transaction_output.len());
    transaction_output[..transaction_end]
        .to_ascii_lowercase()
        .contains("status: mempool")
}

async fn wait_for_wallet_serving_at_node_tip(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    source: &ZebraJsonRpcSource,
    address: &str,
) -> Result<()> {
    let node_tip = source
        .tip_id()
        .await
        .map_err(|error| eyre!("could not resolve Zebra tip after mining: {error}"))?
        .height
        .value();
    let deadline = Instant::now() + WALLET_SERVING_READY_TIMEOUT;
    loop {
        let zinder_tip = client
            .get_latest_block(ChainSpec {})
            .await?
            .into_inner()
            .height;
        if zinder_tip < u64::from(node_tip) {
            if Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return Err(eyre!(
                "Zinder did not reach Zebra height {node_tip} after mining; its latest height is {zinder_tip}"
            ));
        }
        match client
            .get_taddress_txids(lightwalletd::TransparentAddressBlockFilter {
                address: address.to_owned(),
                range: Some(BlockRange {
                    start: Some(BlockId {
                        height: 1,
                        hash: Vec::new(),
                    }),
                    end: Some(BlockId {
                        height: zinder_tip,
                        hash: Vec::new(),
                    }),
                    pool_types: Vec::new(),
                }),
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(status)
                if status.code() == tonic::Code::Unavailable && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => {
                return Err(eyre!(
                    "Zinder transparent history was not ready at Zebra height {node_tip} for Zingolib after mining: {status}"
                ));
            }
        }
    }
}

fn assert_nonempty_orchard_notes(output: &str) -> Result<()> {
    let notes = first_json_value(output, "notes")?;
    let orchard_notes = notes
        .get("orchard_notes")
        .and_then(|orchard_notes| orchard_notes.get("note_summaries"))
        .and_then(Value::as_array)
        .ok_or_else(|| eyre!("Zingolib notes did not contain orchard_notes.note_summaries"))?;
    if orchard_notes.is_empty() {
        return Err(eyre!("Zingolib did not discover a confirmed Orchard note"));
    }
    Ok(())
}

fn transaction_branch_id(raw_transaction: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = raw_transaction
        .get(8..12)
        .ok_or_else(|| {
            eyre!("transparent funding transaction was shorter than its branch-id field")
        })?
        .try_into()
        .map_err(|_| eyre!("transparent funding branch-id field was not four bytes"))?;
    Ok(u32::from_le_bytes(bytes))
}
