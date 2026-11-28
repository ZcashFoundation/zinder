//! Two-party Zkool payment driver.
//!
//! Built inside Zkool's own `rust/` crate so it uses the pinned engine's
//! account, sync, payment-preparation, signing, and broadcast functions.
//!
//!   zkool_payment create <lightwalletd> <birthday> <mnemonic> <dir>
//!   zkool_payment sync <lightwalletd> <dir>
//!   zkool_payment addresses <dir>
//!   zkool_payment report <dir>
//!   zkool_payment send <lightwalletd> <dir> <address> <zats> <src_pools> <memo>

use std::path::Path;

use anyhow::{bail, Context as _, Result};
use rlz::api::account::{get_addresses, new_account, NewAccount};
use rlz::api::coin::Coin;
use rlz::api::sync::SyncOutcome;
use rlz::pay::pool::{POOL_IRONWOOD, POOL_ORCHARD, POOL_SAPLING, POOL_TRANSPARENT};
use serde_json::json;
use sqlx::{Connection as _, Row as _, SqliteConnection};

const REGTEST: u8 = 2;
/// What Zkool's server passes to a sync.
const ACTIONS_PER_SYNC: u32 = 100_000;

fn usage() -> ! {
    eprintln!(
        "usage: zkool_payment create <lightwalletd> <birthday> <mnemonic> <dir>\n       \
         zkool_payment sync <lightwalletd> <dir>\n       \
         zkool_payment addresses <dir>\n       \
         zkool_payment report <dir>\n       \
         zkool_payment send <lightwalletd> <dir> <address> <zats> <src_pools> <memo>"
    );
    std::process::exit(2);
}

fn db_path(dir: &Path) -> String {
    dir.join("zkool.db").to_string_lossy().into_owned()
}

async fn open(network: u8, dir: &Path) -> Result<Coin> {
    Coin::new(Some(network)).open_database(db_path(dir), None).await
}

async fn account(dir: &Path) -> Result<u32> {
    Ok(std::fs::read_to_string(dir.join("account"))?.parse()?)
}

async fn create(server: &str, birthday: u32, mnemonic: &str, dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context("wallet dir")?;
    rlz::api::db::create_db(&db_path(dir), None, REGTEST).await?;
    let coin = open(REGTEST, dir).await?.set_lwd(0, server.into())?;
    let account = new_account(
        &NewAccount {
            icon: None,
            name: "payment".into(),
            restore: false,
            key: mnemonic.into(),
            passphrase: None,
            fingerprint: None,
            aindex: 0,
            birth: Some(birthday),
            folder: String::new(),
            pools: Some(POOL_TRANSPARENT | POOL_SAPLING | POOL_ORCHARD | POOL_IRONWOOD),
            use_internal: Some(std::env::var("ZINDER_ZKOOL_USE_INTERNAL").as_deref() != Ok("false")),
            internal: false,
            hw: 0,
        },
        &coin,
    )
    .await?;
    std::fs::write(dir.join("account"), account.to_string()).context("write account")?;
    resume(server, dir).await
}

async fn resume(server: &str, dir: &Path) -> Result<()> {
    let coin = open(REGTEST, dir).await?.set_lwd(0, server.into())?;
    let account = account(dir).await?;
    let tip = rlz::api::network::get_current_height(&coin).await?;
    let driver = rlz::sync::driver::SyncDriver::start();
    let (progress, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    let outcome = driver
        .sync(
            &coin,
            rlz::sync::driver::SyncRequest {
                accounts: vec![account],
                current_height: tip,
                actions_per_sync: ACTIONS_PER_SYNC,
                checkpoint_age: 10_000,
                skip_details: false,
            },
            progress,
        )
        .await?;
    match outcome {
        SyncOutcome::Synced { height } => println!("synced to {height}"),
        other => bail!("the sync did not complete: {other:?}"),
    }
    Ok(())
}

async fn addresses(dir: &Path) -> Result<()> {
    let coin = open(REGTEST, dir).await?;
    let a = get_addresses(15, account(dir).await?, &coin).await?;
    println!(
        "{}",
        json!({"transparent": a.taddr, "sapling": a.saddr, "unified": a.ua})
    );
    Ok(())
}

async fn report(dir: &Path) -> Result<()> {
    let coin = open(REGTEST, dir).await?;
    let account = account(dir).await?;
    let synced_height = rlz::api::sync::get_db_height(account, &coin).await?.height;
    let balance = rlz::api::sync::balance(account, &coin).await?.0;
    let mut connection = SqliteConnection::connect(&format!("sqlite://{}", db_path(dir))).await?;
    let rows = sqlx::query(
        "SELECT txid, height, value, fee, tpe FROM transactions WHERE account = ?1 \
         ORDER BY height, id_tx",
    )
    .bind(account)
    .fetch_all(&mut connection)
    .await?;
    let memo_rows = sqlx::query(
        "SELECT t.txid, m.memo_text FROM memos m \
         JOIN transactions t ON t.id_tx = m.tx AND t.account = m.account \
         WHERE m.account = ?1 ORDER BY m.height, m.id_memo",
    )
    .bind(account)
    .fetch_all(&mut connection)
    .await?;
    let mut transactions = Vec::new();
    for row in rows {
        let txid: Vec<u8> = row.try_get("txid")?;
        let mut reversed = txid.clone();
        reversed.reverse();
        let value: i64 = row.try_get("value")?;
        let fee: i64 = row.try_get("fee")?;
        let height: i64 = row.try_get("height")?;
        let tpe: Option<i64> = row.try_get("tpe")?;
        transactions.push(json!({
            "txid": hex::encode(&reversed),
            "txid_stored": hex::encode(&txid),
            "height": height,
            "value_zat": value,
            "fee_zat": fee,
            "kind": tpe,
        }));
    }
    let mut memos = Vec::new();
    for row in memo_rows {
        let txid: Vec<u8> = row.try_get("txid")?;
        let mut reversed = txid.clone();
        reversed.reverse();
        let text: Option<String> = row.try_get("memo_text")?;
        memos.push(json!({"txid": hex::encode(&reversed), "text": text}));
    }
    println!(
        "{}",
        json!({
            "wallet": "zkool",
            "synced_height": synced_height,
            "balance": {
                "transparent": balance[0],
                "sapling": balance[1],
                "orchard": balance[2],
                "ironwood": balance[3],
                "total": balance.iter().sum::<u64>(),
            },
            "transactions": transactions,
            "memos": memos,
        })
    );
    Ok(())
}

async fn send(
    server: &str,
    dir: &Path,
    address: &str,
    amount: u64,
    pools: u8,
    memo: &str,
) -> Result<()> {
    let coin = open(REGTEST, dir).await?.set_lwd(0, server.into())?;
    let account = account(dir).await?;
    let recipient = rlz::pay::Recipient {
        address: address.into(),
        amount,
        pools: None,
        user_memo: if memo.is_empty() { None } else { Some(memo.into()) },
        ..Default::default()
    };
    let package = rlz::api::pay::prepare(
        &[recipient],
        rlz::api::pay::PaymentOptions {
            src_pools: pools,
            recipient_pays_fee: false,
            smart_transparent: false,
            category: None,
        },
        account,
        &coin,
    )
    .await?;
    let signed = rlz::api::pay::sign_transaction(&package, account, &coin).await?;
    let txid = rlz::api::pay::broadcast(&signed, account, &coin).await?;
    println!("{}", json!({"txid": txid}));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let argv: Vec<_> = args.iter().map(String::as_str).collect();
    match argv.as_slice() {
        ["create", server, birthday, mnemonic, dir] => {
            create(server, birthday.parse().context("birthday")?, mnemonic, Path::new(dir)).await
        }
        ["sync", server, dir] => resume(server, Path::new(dir)).await,
        ["addresses", dir] => addresses(Path::new(dir)).await,
        ["report", dir] => report(Path::new(dir)).await,
        ["send", server, dir, address, amount, pools, memo] => {
            send(
                server,
                Path::new(dir),
                address,
                amount.parse()?,
                pools.parse()?,
                memo,
            )
            .await
        }
        _ => usage(),
    }
}
