//! Counterparty payment harness for the Zally typed headless wallet engine.
//!
//! Build outside Zally's own workspace against its pinned source, then drive one
//! command per process. Every command writes one JSON object on stdout.
use std::env;
use std::io;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio_stream::StreamExt;
use zally_chain::{ZinderChainSource, ZinderRemoteOptions};
use zally_core::{AccountId, BlockHeight, IdempotencyKey, Memo, Network, PaymentRecipient, Zatoshis};
use zally_keys::{AgeFileSealing, AgeFileSealingOptions};
use zally_storage::{Sqlite, SqliteOptions};
use zally_wallet::{SendPaymentPlan, SyncDriver, SyncDriverOptions, Wallet, WalletOptions};
use zcash_protocol::consensus::BlockHeight as ConsensusBlockHeight;
use zcash_protocol::local_consensus::LocalNetwork as ZallyLocalNetwork;

#[derive(Debug)]
enum HarnessError {
    Message(String),
    Io(io::Error),
}

impl From<io::Error> for HarnessError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl std::fmt::Display for HarnessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => formatter.write_str(message),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for HarnessError {}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn fail<T>(message: impl Into<String>) -> std::result::Result<T, Box<dyn std::error::Error>> {
    Err(Box::new(HarnessError::Message(message.into())))
}

fn node_call(node: &str, method: &str, params: &Value) -> Result<Value> {
    let request = json!({"jsonrpc": "2.0", "id": "zally-payment", "method": method, "params": params});
    let output = Command::new("curl")
        .args(["-sS", "--fail", "--max-time", "30", "--data-binary"])
        .arg(request.to_string())
        .args(["-H", "content-type: application/json", node])
        .output()?;
    if !output.status.success() {
        return fail(format!("node RPC {method} failed"));
    }
    let response: Value = serde_json::from_slice(&output.stdout)?;
    if !response.get("error").is_none_or(Value::is_null) {
        return fail(format!("node RPC {method} returned {}", response["error"]));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| Box::new(HarnessError::Message("node RPC response missing result".into())) as Box<dyn std::error::Error>)
}

fn network_from_node(node: &str) -> Result<(Network, u32)> {
    let info = node_call(node, "getblockchaininfo", &json!([]))?;
    let upgrades = info["upgrades"]
        .as_object()
        .ok_or_else(|| Box::new(HarnessError::Message("missing upgrades".into())) as Box<dyn std::error::Error>)?;
    let mut heights = Vec::new();
    for (branch, value) in upgrades {
        let name = value["name"]
            .as_str()
            .ok_or_else(|| Box::new(HarnessError::Message("upgrade missing name".into())) as Box<dyn std::error::Error>)?;
        let height = value["activationheight"]
            .as_u64()
            .ok_or_else(|| Box::new(HarnessError::Message(format!("upgrade {branch} missing height"))) as Box<dyn std::error::Error>)?;
        let height = u32::try_from(height)?;
        heights.push((name.to_owned(), height));
    }
    let activation = |wanted: &str| {
        heights
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
            .map(|(_, height)| ConsensusBlockHeight::from_u32(*height))
    };
    let local = ZallyLocalNetwork {
        overwinter: activation("Overwinter"),
        sapling: activation("Sapling"),
        blossom: activation("Blossom"),
        heartwood: activation("Heartwood"),
        canopy: activation("Canopy"),
        nu5: activation("NU5"),
        nu6: activation("NU6"),
        nu6_1: activation("NU6.1"),
        nu6_2: activation("NU6.2"),
        nu6_3: activation("NU6.3"),
        nu7: activation("NU7"),
    };
    let tip = info["blocks"]
        .as_u64()
        .ok_or_else(|| Box::new(HarnessError::Message("missing tip".into())) as Box<dyn std::error::Error>)?;
    Ok((Network::Regtest(local), u32::try_from(tip)?))
}

fn connect(network: Network, endpoint: &str) -> Result<ZinderChainSource> {
    Ok(ZinderChainSource::connect_remote(ZinderRemoteOptions {
        endpoint: endpoint.to_owned(),
        network,
    })?)
}

async fn open_wallet(root: &Path, network: Network) -> Result<(Wallet, AccountId)> {
    let sealing = AgeFileSealing::new(AgeFileSealingOptions::at_path(root.join("wallet.age")));
    let storage = Sqlite::new(SqliteOptions::for_network(network, root.join("wallet.db")));
    let (wallet, account) = Wallet::builder(network, sealing, storage)
        .with_options(WalletOptions::default().with_minimum_source_reorg_window_blocks(None))
        .open()
        .await?;
    Ok((wallet, account))
}

async fn create_wallet(
    root: &Path,
    network: Network,
    chain: &ZinderChainSource,
    birthday: BlockHeight,
) -> Result<(Wallet, AccountId)> {
    let sealing = AgeFileSealing::new(AgeFileSealingOptions::at_path(root.join("wallet.age")));
    let storage = Sqlite::new(SqliteOptions::for_network(network, root.join("wallet.db")));
    let (wallet, account, _mnemonic) = Wallet::builder(network, sealing, storage)
        .with_options(WalletOptions::default().with_minimum_source_reorg_window_blocks(None))
        .create(chain, birthday)
        .await?;
    Ok((wallet, account))
}

async fn sync_to(wallet: &Wallet, chain: &ZinderChainSource, target: u32) -> Result<()> {
    let driver = SyncDriver::new(
        wallet.clone(),
        Arc::new(chain.clone()),
        SyncDriverOptions::default().with_poll_interval_ms(250),
    )?;
    let handle = driver.sync_continuously();
    let mut snapshots = handle.observe_status();
    let reached = tokio::time::timeout(Duration::from_secs(180), async {
        while let Some(snapshot) = snapshots.next().await {
            if let zally_wallet::SyncStatus::AtTip { visible_tip_height } = snapshot.sync_status
                && visible_tip_height.as_u32() >= target
            {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    handle.close().await?;
    if !reached {
        return fail("wallet sync did not reach the requested tip");
    }
    Ok(())
}

fn zat(value: zally_core::Zatoshis) -> u64 {
    value.as_u64()
}

async fn snapshot(wallet: &Wallet, account: AccountId, endpoint: &str) -> Result<Value> {
    let balance = wallet.get_account_balance(account).await?;
    let receives = wallet.list_shielded_receives(account).await?;
    let unspent = wallet.list_unspent_shielded_notes(account).await?;
    let spendable = wallet.spendable_for_next_dispense(account).await?;
    let mut rows = Vec::new();
    for note in &receives {
        let memo = wallet
            .read_text_memo(note.tx_id, u16::try_from(note.output_index).unwrap_or(u16::MAX))
            .await?;
        rows.push(json!({
            "txid": note.tx_id.to_string(), "pool": format!("{:?}", note.pool).to_lowercase(),
            "value": zat(note.value), "mined_height": note.mined_height.as_u32(),
            "output_index": note.output_index, "is_change": note.is_change,
            "spent_our_inputs": note.spent_our_inputs, "memo": memo,
        }));
    }
    let unspent: Vec<_> = unspent
        .iter()
        .map(|note| json!({"txid": note.tx_id.to_string(), "value": zat(note.value),
            "pool": format!("{:?}", note.pool).to_lowercase(), "confirmations": note.confirmations}))
        .collect();
    Ok(json!({
        "endpoint": endpoint,
        "account": account.as_uuid().to_string(),
        "total_zat": zat(balance.total_zat()),
        "sapling_zat": zat(balance.sapling_zat),
        "orchard_zat": zat(balance.orchard_zat),
        "ironwood_zat": zat(balance.ironwood_zat),
        "transparent_mature_zat": zat(balance.transparent_mature_zat),
        "transparent_immature_zat": zat(balance.transparent_immature_zat),
        "height": balance.as_of_height.map(BlockHeight::as_u32),
        "spendable_zat": zat(spendable),
        "receives": rows,
        "unspent": unspent,
        "pid": std::process::id(),
    }))
}

async fn run() -> Result<Value> {
    let args: Vec<String> = env::args().skip(1).collect();
    let usage = "usage: zally-payment COMMAND PRIVATE-DIRECTORY ENDPOINT NODE-RPC [ARGS]";
    let (command, root, endpoint, node, rest) = match args.as_slice() {
        [command, root, endpoint, node, rest @ ..] => {
            (command.as_str(), Path::new(root), endpoint.as_str(), node.as_str(), rest)
        }
        _ => return fail(usage),
    };
    if !endpoint.starts_with("http://127.0.0.1:") || !node.starts_with("http://127.0.0.1:") {
        return fail("this harness requires explicit loopback endpoints");
    }
    let (network, tip) = network_from_node(node)?;
    let chain = connect(network, endpoint)?;
    let mut result = match command {
        "create" => {
            let birthday = BlockHeight::from(tip.saturating_sub(2).max(1));
            let (wallet, account) = create_wallet(root, network, &chain, birthday).await?;
            let encoded = wallet
                .derive_next_address_with_transparent(account)
                .await?
                .encode(&network.to_parameters());
            let main = wallet.derive_next_address(account).await?;
            json!({"account": account.as_uuid().to_string(), "birthday": birthday.as_u32(),
                   "unified": main.encode(&network.to_parameters()),
                   "unified_with_transparent": encoded})
        }
        "address" => {
            let (wallet, account) = open_wallet(root, network).await?;
            let address = wallet.derive_next_address(account).await?;
            json!({"address": address.encode(&network.to_parameters()),
                   "account": account.as_uuid().to_string()})
        }
        "sync" => {
            let (wallet, account) = open_wallet(root, network).await?;
            sync_to(&wallet, &chain, tip).await?;
            snapshot(&wallet, account, endpoint).await?
        }
        "read" => {
            let (wallet, account) = open_wallet(root, network).await?;
            snapshot(&wallet, account, endpoint).await?
        }
        "send" => {
            let [address, amount, memo] = rest else {
                return fail("send requires ADDRESS ATOMS MEMO-OR-DASH");
            };
            let (wallet, account) = open_wallet(root, network).await?;
            let zatoshis = Zatoshis::try_from(amount.parse::<u64>()?)?;
            let recipient = PaymentRecipient::UnifiedAddress {
                encoded: address.clone(),
                network,
            };
            let idempotency = IdempotencyKey::try_from(format!("zinder-payment-{amount}-{tip}"))?;
            let submitter = chain.submitter();
            let mut plan =
                SendPaymentPlan::conventional(account, idempotency, recipient, zatoshis, &submitter);
            if memo.as_str() != "-" {
                plan = plan.with_memo(Memo::from_str(memo)?);
            }
            let outcome = wallet.send_payment(plan).await?;
            json!({"txid": outcome.tx_id().to_string(), "amount_zat": zat(zatoshis)})
        }
        _ => return fail("unknown command"),
    };
    result["server_tip"] = json!(tip);
    Ok(result)
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(result) => println!("{result}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
