//! Build as an example in Vizor v0.0.65's rust crate. The release harness owns the node.
//!
//! Counterparty payment roles: an external recipient address is supplied on the
//! command line, so the same engine pays an independently keyed wallet and is
//! paid by one. Every command prints one JSON object on stdout.
use rust_lib_zcash_wallet::api::{simple, sync, wallet};
use serde_json::{Value, json};
use std::{env, fs, path::Path, time::Duration};
use tonic::transport::Endpoint;
use zcash_client_backend::proto::{
    compact_formats::CompactBlock,
    service::{BlockId, Empty, compact_tx_streamer_client::CompactTxStreamerClient},
};
use zcash_primitives::block::BlockHash;

const NETWORK: &str = "regtest";

fn verify_anchor(block: &CompactBlock, height: u64, display_hash: &str) -> Result<(), &'static str> {
    let hash = BlockHash::try_from_slice(&block.hash).ok_or("invalid wallet endpoint block hash")?;
    if block.height != height || hash.to_string() != display_hash {
        return Err("wallet endpoint block differs from the immutable fixture anchor");
    }
    Ok(())
}

async fn verify_endpoint(
    url: &str,
    revision: &str,
    height: u64,
    display_hash: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(5);
    let channel = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = CompactTxStreamerClient::new(channel);
    let info = client.get_lightd_info(Empty {}).await?.into_inner();
    // Zinder uses the BIP70 chain name "test" for Regtest on the lightwalletd interface.
    if info.chain_name != "test" || info.git_commit != revision {
        return Err("live wallet endpoint identity differs from the selected fixture".into());
    }
    let block = client
        .get_block(BlockId {
            height,
            ..Default::default()
        })
        .await?
        .into_inner();
    verify_anchor(&block, height, display_hash)?;
    Ok(())
}

fn account(db: &str) -> Result<String, String> {
    let accounts = wallet::list_accounts(db.into(), NETWORK.into())?;
    match accounts.as_slice() {
        [account] => Ok(account.uuid.clone()),
        _ => Err("expected one wallet account".into()),
    }
}

fn fee_state(value: &sync::TransactionFeeState) -> &'static str {
    match value {
        sync::TransactionFeeState::Known => "known",
        sync::TransactionFeeState::Unknown => "unknown",
        sync::TransactionFeeState::NotApplicable => "not_applicable",
    }
}

fn snapshot(db: &str) -> Result<Value, String> {
    let account = account(db)?;
    let funds = sync::get_balance(db.into(), NETWORK.into(), account.clone())?;
    let progress = sync::get_sync_status(db.into(), NETWORK.into())?;
    let history = sync::get_transaction_history(db.into(), NETWORK.into(), Some(200), account.clone())?;
    let history: Vec<_> = history
        .into_iter()
        .map(|tx| {
            json!({
                "txid":tx.txid_hex, "mined_height":tx.mined_height, "expired":tx.expired_unmined,
                "balance_delta":tx.account_balance_delta, "fee_zat":tx.fee, "fee_state":fee_state(&tx.fee_state),
                "tx_kind":tx.tx_kind, "display_amount":tx.display_amount, "pool":tx.activity_pool,
                "details_complete":tx.details_complete,
            })
        })
        .collect();
    Ok(json!({
        "account":account, "transparent":funds.transparent, "sapling":funds.sapling,
        "orchard":funds.orchard, "ironwood":funds.ironwood,
        "scanned":progress.scanned_height, "tip":progress.chain_tip_height, "history":history,
    }))
}

fn run() -> Result<Value, String> {
    let args: Vec<_> = env::args().skip(1).collect();
    let [command, scratch, endpoint, activation, revision, anchor_height, anchor_hash, rest @ ..] =
        args.as_slice()
    else {
        return Err(
            "usage: vizor-payment COMMAND PRIVATE-DIRECTORY LOOPBACK-URL NU6.3-HEIGHT REVISION ANCHOR-HEIGHT ANCHOR-HASH [ARGS]"
                .into(),
        );
    };
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|s| s.parse::<u16>().ok());
    if port.is_none() || port == Some(0) {
        return Err("expected IPv4 loopback endpoint with port".into());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?
        .block_on(verify_endpoint(
            endpoint,
            revision,
            anchor_height.parse::<u64>().map_err(|e| e.to_string())?,
            anchor_hash,
        ))
        .map_err(|e| e.to_string())?;
    simple::configure_regtest_ironwood_activation_height(
        activation.parse::<u32>().map_err(|e| e.to_string())?,
    )?;
    let height = wallet::get_latest_block_height(endpoint.clone(), NETWORK.into())?;
    let root = Path::new(scratch);
    let seed = root.join("seed");
    let birthday = root.join("birthday");
    let path = root.join("restored.sqlite");
    let db = path.to_str().ok_or("invalid path")?;
    let net = NETWORK.to_string();
    let mut result = match command.as_str() {
        "create" => {
            if seed.exists() || root.join("original.sqlite").exists() {
                return Err("wallet exists".into());
            }
            let original = root.join("original.sqlite");
            let source = original.to_str().ok_or("invalid wallet path")?;
            let wallet = wallet::create_wallet(net.clone(), source.into(), Some(height), None)?;
            let address = wallet::get_transparent_receive_address(
                source.into(),
                net,
                Some(wallet.account_uuid.clone()),
            )?;
            fs::write(seed, wallet.mnemonic).map_err(|e| e.to_string())?;
            fs::write(birthday, height.to_string()).map_err(|e| e.to_string())?;
            json!({"unified":wallet.unified_address, "transparent_address":address,
                   "birthday":height, "account":wallet.account_uuid})
        }
        "restore" => {
            if path.exists() {
                return Err("restore database exists".into());
            }
            let height = fs::read_to_string(birthday)
                .map_err(|e| e.to_string())?
                .parse::<u64>()
                .map_err(|e| e.to_string())?;
            let seed = fs::read_to_string(seed).map_err(|e| e.to_string())?;
            let wallet = wallet::import_software_wallet_with_account_discovery(
                seed,
                String::new(),
                Some(height),
                net,
                db.into(),
                None,
                true,
                1,
                Vec::new(),
            )?;
            let [restored] = wallet.accounts.as_slice() else {
                return Err("expected one restored account".into());
            };
            json!({"unified":restored.unified_address,"account":restored.account_uuid,"birthday":height})
        }
        "sync" | "sync-original" => {
            let target = if command == "sync-original" {
                root.join("original.sqlite")
            } else {
                path.clone()
            };
            let value = target.to_str().ok_or("invalid path")?;
            sync::run_full_sync_blocking(value.into(), endpoint.clone(), net, 1)?;
            snapshot(value)?
        }
        "read" => snapshot(db)?,
        "read-original" => {
            let original = root.join("original.sqlite");
            snapshot(original.to_str().ok_or("invalid path")?)?
        }
        "send" => {
            let [address, amount, memo] = rest else {
                return Err("send requires ADDRESS ATOMS MEMO-OR-DASH".into());
            };
            let memo = (memo.as_str() != "-").then(|| memo.clone());
            let db_path = root.join("original.sqlite").to_str().ok_or("invalid path")?.to_string();
            let uuid = account(&db_path)?;
            let flow = format!("zinder-payment-{amount}");
            let proposal = sync::propose_send(
                db_path.clone(),
                net.clone(),
                uuid,
                flow.clone(),
                address.clone(),
                amount.parse::<u64>().map_err(|e| e.to_string())?,
                memo,
            )?;
            let seed = fs::read(seed).map_err(|e| e.to_string())?;
            let sent = sync::execute_proposal(
                db_path.clone(),
                endpoint.clone(),
                proposal.proposal_id,
                flow,
                seed,
                None,
                None,
            )?;
            if sent.status != "broadcasted" || sent.total_count != 1 || sent.broadcasted_count != 1 {
                return Err("expected one signed and broadcast transaction".into());
            }
            json!({"txids":sent.txids, "fee_zatoshi":proposal.fee_zatoshi,
                   "needs_sapling_params":proposal.needs_sapling_params, "proposal_id":proposal.proposal_id})
        }
        "detail" => {
            let [txid] = rest else {
                return Err("detail requires TXID".into());
            };
            // Wallet rows use database byte order; the caller supplies the node's display order.
            let mut wire = (0..txid.len() / 2)
                .map(|index| u8::from_str_radix(&txid[index * 2..index * 2 + 2], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|e| e.to_string())?;
            wire.reverse();
            let wallet_txid: String = wire.iter().map(|byte| format!("{byte:02x}")).collect();
            let db_path = root.join("original.sqlite").to_str().ok_or("invalid path")?.to_string();
            let uuid = account(&db_path)?;
            let mut found = None;
            let mut ordering = "";
            for candidate in [wallet_txid.clone(), txid.to_string()] {
                let rows = sync::get_transaction_rows(
                    db_path.clone(), net.clone(), uuid.clone(), candidate.clone())?;
                let Some(row) = rows.first() else { continue };
                if let Ok(detail) = sync::get_transaction_detail(
                    db_path.clone(), net.clone(), uuid.clone(), candidate.clone(),
                    row.tx_kind.clone()) {
                    ordering = if candidate == wallet_txid { "wallet-bytes" } else { "display-bytes" };
                    found = Some(detail);
                    break;
                }
            }
            let detail = found.ok_or("transaction absent from wallet history")?;
            json!({"txid":txid, "tx_kind":detail.tx_kind, "memo":detail.memo,
                   "ordering":ordering,
                   "network_fee":detail.network_fee, "details_complete":detail.details_complete,
                   "primary_address":detail.primary_address,
                   "outputs":detail.outputs.iter().map(|o| json!({
                       "address":o.address, "amount":o.amount_zatoshi, "pool":o.pool})).collect::<Vec<_>>()})
        }
        _ => return Err("unknown command".into()),
    };
    result["pid"] = json!(std::process::id());
    result["server_tip"] = json!(height);
    Ok(result)
}

fn main() {
    match run() {
        Ok(result) => println!("{result}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
