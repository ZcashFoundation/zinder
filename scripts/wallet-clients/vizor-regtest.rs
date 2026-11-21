//! Build as an example in Vizor v0.0.65's rust crate. The release harness owns the node.
use rust_lib_zcash_wallet::api::{simple, sync, wallet};
use serde_json::{Value, json};
use std::{env, fs, path::Path};

const NETWORK: &str = "regtest";

fn account(db: &str) -> Result<String, String> {
    let accounts = wallet::list_accounts(db.into(), NETWORK.into())?;
    match accounts.as_slice() {
        [account] => Ok(account.uuid.clone()),
        _ => Err("expected one wallet account".into()),
    }
}

fn snapshot(db: &str) -> Result<Value, String> {
    let account = account(db)?;
    let funds = sync::get_balance(db.into(), NETWORK.into(), account.clone())?;
    let progress = sync::get_sync_status(db.into(), NETWORK.into())?;
    let history =
        sync::get_transaction_history(db.into(), NETWORK.into(), Some(100), account.clone())?;
    let history: Vec<_> = history
        .into_iter()
        .map(|tx| {
            json!({
                "txid":tx.txid_hex, "mined_height":tx.mined_height, "expired":tx.expired_unmined,
                "balance_delta":tx.account_balance_delta, "pool":tx.activity_pool,
            })
        })
        .collect();
    Ok(
        json!({"account":account, "transparent":funds.transparent, "ironwood":funds.ironwood,
        "scanned":progress.scanned_height,"tip":progress.chain_tip_height, "history":history}),
    )
}

fn run() -> Result<Value, String> {
    let args: Vec<_> = env::args().skip(1).collect();
    let [command, scratch, endpoint, activation] = args.as_slice() else {
        return Err(
            "usage: vizor-regtest COMMAND PRIVATE-DIRECTORY LOOPBACK-URL NU6.3-HEIGHT".into(),
        );
    };
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|s| s.parse::<u16>().ok());
    if port.is_none() || port == Some(0) {
        return Err("expected IPv4 loopback endpoint with port".into());
    }
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
                Some(wallet.account_uuid),
            )?;
            fs::write(seed, wallet.mnemonic).map_err(|e| e.to_string())?;
            fs::write(birthday, height.to_string()).map_err(|e| e.to_string())?;
            json!({"unified":wallet.unified_address, "transparent_address":address})
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
            json!({"unified":restored.unified_address,"account":restored.account_uuid})
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
        "send" => {
            let recipient_path = root.join("recipient.sqlite");
            if recipient_path.exists() {
                return Err("recipient wallet exists".into());
            }
            let recipient = wallet::create_wallet(
                net.clone(),
                recipient_path.to_str().ok_or("invalid path")?.into(),
                Some(height),
                None,
            )?;
            let flow = "vizor-regtest";
            let proposal = sync::propose_send(
                db.into(),
                net,
                account(db)?,
                flow.into(),
                recipient.unified_address,
                100_000,
                None,
            )?;
            if proposal.needs_sapling_params {
                return Err("expected an Ironwood-only send".into());
            }
            let seed = fs::read(seed).map_err(|e| e.to_string())?;
            let sent = sync::execute_proposal(
                db.into(),
                endpoint.clone(),
                proposal.proposal_id,
                flow.into(),
                seed,
                None,
                None,
            )?;
            if sent.status != "broadcasted" || sent.total_count != 1 || sent.broadcasted_count != 1
            {
                return Err("expected one signed and broadcast transaction".into());
            }
            json!({"txid":sent.txids})
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
