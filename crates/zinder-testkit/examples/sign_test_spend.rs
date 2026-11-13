//! Signs a spend of one output that pays the broadcast test key, for the next
//! block of a Regtest node, and prints the raw transaction as hex.
//!
//! ```text
//! sign_test_spend <node-json-rpc-url> <transparent|orchard|ironwood> <txid> <vout> <value-zats>
//! ```
//!
//! `<txid>` is the display-order hex that `getaddressutxos` reports. The node
//! supplies its activation heights and tip, so the signature commits to the
//! consensus branch of the next block. `transparent` pays a scratch P2PKH
//! address; `orchard` and `ironwood` pay one note to the test seed's Orchard
//! receiver. `scripts/capture-zakura-shielded-fixtures.sh` uses this binary,
//! which never broadcasts.

use std::{fmt::Write as _, time::Duration};

use eyre::{Result, eyre};
use zinder_core::{Network, wire::decode_rpc_transaction_id_hex};
use zinder_source::{NodeAuth, NodeSource as _, ZebraJsonRpcSource};
use zinder_testkit::{
    OrchardTestKey, P2pkhShieldingSpendArgs, P2pkhSpendArgs, ShieldedOutputPool,
    TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey, local_network_from_activations,
};

const USAGE: &str = "usage: sign_test_spend <node-json-rpc-url> <transparent|orchard|ironwood> <txid> <vout> <value-zats>";

#[allow(
    clippy::print_stdout,
    reason = "operator-facing binary that emits one raw transaction as hex"
)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let [json_rpc_addr, pool, txid, vout, value_zats] = arguments.as_slice() else {
        return Err(eyre!("{USAGE}"));
    };
    let pool = match pool.as_str() {
        "transparent" => None,
        "orchard" => Some(ShieldedOutputPool::Orchard),
        "ironwood" => Some(ShieldedOutputPool::Ironwood),
        other => return Err(eyre!("unknown pool {other}; {USAGE}")),
    };
    let coinbase_txid_be = decode_rpc_transaction_id_hex(txid)?.as_bytes();
    let coinbase_vout: u32 = vout.parse()?;
    let coinbase_value_zats: u64 = value_zats.parse()?;

    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        json_rpc_addr.as_str(),
        NodeAuth::None,
        Duration::from_secs(30),
    )?;
    let activations = source.fetch_network_upgrade_activations().await?;
    let target_height = source.tip_id().await?.height.value().saturating_add(1);
    let key = TransparentTestKey::from_seed_with_local_network(
        &TRANSPARENT_BROADCAST_TEST_SEED,
        local_network_from_activations(&activations),
    )?;

    let raw_transaction = if let Some(pool) = pool {
        let recipient = OrchardTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?.address();
        key.build_p2pkh_shielding_spend(&P2pkhShieldingSpendArgs {
            coinbase_txid_be,
            coinbase_vout,
            coinbase_value_zats,
            pool,
            recipient: &recipient,
            target_height,
        })?
    } else {
        let recipient = key.scratch_recipient_address(u64::from(target_height));
        key.build_p2pkh_spend(&P2pkhSpendArgs {
            coinbase_txid_be,
            coinbase_vout,
            coinbase_value_zats,
            recipient: &recipient,
            target_height,
        })?
    };
    let raw_hex = raw_transaction.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    });
    println!("{raw_hex}");
    Ok(())
}
