//! Prints the address derived from the regtest broadcast cycle's test seed.
//!
//! Operators copy the address into Zebra's `ZEBRA_MINING__MINER_ADDRESS`
//! environment variable (or `[mining] miner_address` in `zebrad.toml`) so
//! the live broadcast and reorg tests find a coinbase output they can
//! spend.
//!
//! With `--unified`, it prints the Regtest unified address that combines the
//! same P2PKH receiver with the test seed's Orchard receiver instead. The
//! fixture-capture node configuration mines to it, so shielded coinbase notes
//! also belong to the test keys.

use zinder_testkit::{
    OrchardTestKey, TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey, regtest_unified_address,
};

#[allow(
    clippy::print_stdout,
    reason = "operator-facing binary that emits one line of structured config data"
)]
fn main() -> eyre::Result<()> {
    let key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)
        .map_err(|error| eyre::eyre!("could not derive test key: {error}"))?;
    match std::env::args().nth(1).as_deref() {
        None => println!("{}", key.address_base58()),
        Some("--unified") => {
            let orchard_key = OrchardTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)
                .map_err(|error| eyre::eyre!("could not derive Orchard test key: {error}"))?;
            let address = regtest_unified_address(&key, &orchard_key)
                .map_err(|error| eyre::eyre!("could not encode unified address: {error}"))?;
            println!("{address}");
        }
        Some(argument) => {
            return Err(eyre::eyre!(
                "unexpected argument {argument}; usage: print_broadcast_test_address [--unified]"
            ));
        }
    }
    Ok(())
}
