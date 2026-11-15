//! Hashing cost a wallet transition pays for each transparent output.
//!
//! Creating or spending one output folds an unspent row, its by-address row,
//! and a balance row into the wallet projection accumulator, and the output
//! into the UTXO-set commitment. This benchmark reports nanoseconds per call
//! for each of those folds on representative rows, together with the
//! `blake2b_simd` batch degree the host detected. Timings depend on the host
//! and the build profile, so it asserts nothing about them. Run it in release
//! on an idle host:
//!
//! ```text
//! cargo nextest run -p zinder-wallet-projection --cargo-profile release \
//!   --run-ignored=all --no-capture -E 'test(/^perf::digest_hashing::/)'
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use zinder_core::wire::UtxoSetCommitmentElement;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, TransactionId, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentUtxoSetCommitment,
};
use zinder_wallet_projection::{
    WalletAddressBalance, WalletAddressUnspentOutputKey, WalletOutpointKey,
    WalletProjectionContractError, WalletProjectionDigestBuilder, WalletProjectionRowFamily,
    WalletTransactionPosition, WalletUnspentOutput,
};

const CALLS_PER_ROUND: u32 = 4_096;
const ROUNDS: usize = 9;

/// A pay-to-public-key-hash `scriptPubKey`, the most common transparent output.
const P2PKH_SCRIPT: [u8; 25] = [
    0x76, 0xa9, 0x14, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x88, 0xac,
];

#[test]
#[ignore = "manual report-only benchmark; run in release on an idle host"]
fn row_hashing_reports_nanoseconds_per_call() -> Result<(), WalletProjectionContractError> {
    let output = representative_output()?;
    let unspent_key = WalletOutpointKey::new(output.outpoint);
    let unspent_value = output.encode_value()?;
    let by_address_key = WalletAddressUnspentOutputKey::new(&output);
    let balance = WalletAddressBalance {
        address_script_hash: output.address_script_hash,
        balance_zat: output.value_zat,
    };

    let (unspent_append, unspent_remove) = row_costs(
        WalletProjectionRowFamily::TransparentUnspentOutput,
        unspent_key.as_bytes(),
        &unspent_value,
    )?;
    let (by_address_append, by_address_remove) = row_costs(
        WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
        by_address_key.as_bytes(),
        &[],
    )?;
    let (balance_append, balance_remove) = row_costs(
        WalletProjectionRowFamily::TransparentAddressBalance,
        &balance.encode_key(),
        &balance.encode_value(),
    )?;
    let utxo_insert = utxo_commitment_insert_cost(&UtxoSetCommitmentElement {
        network_id: Network::ZcashTestnet.id(),
        outpoint: output.outpoint,
        value_zat: output.value_zat,
        script_pub_key: &output.script_pub_key,
        block_height: output.created_at.block.height,
    });

    #[allow(
        clippy::print_stderr,
        reason = "report-only benchmark prints its measurements for operator review"
    )]
    {
        eprintln!(
            "wallet_digest_hashing many_degree={} unspent_append_ns={unspent_append} \
             unspent_remove_ns={unspent_remove} by_address_append_ns={by_address_append} \
             by_address_remove_ns={by_address_remove} balance_append_ns={balance_append} \
             balance_remove_ns={balance_remove} utxo_commitment_insert_ns={utxo_insert} \
             (calls_per_round={CALLS_PER_ROUND} rounds={ROUNDS} median)",
            blake2b_simd::many::degree(),
        );
    }
    Ok(())
}

fn representative_output() -> Result<WalletUnspentOutput, WalletProjectionContractError> {
    let transaction_id = TransactionId::from_bytes([0x11; 32]);
    WalletUnspentOutput::new(
        TransparentOutPoint::new(transaction_id, 1),
        TransparentAddressScriptHash::from_bytes([0x22; 32]),
        150_000,
        P2PKH_SCRIPT,
        WalletTransactionPosition::new(
            transaction_id,
            7,
            BlockId::new(
                BlockHeight::new(4_600_001),
                BlockHash::from_bytes([0x33; 32]),
            ),
        ),
    )
}

/// Returns the median nanoseconds per `append_row` and per `remove_row` call.
fn row_costs(
    family: WalletProjectionRowFamily,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<(u128, u128), WalletProjectionContractError> {
    let mut append_rounds = Vec::with_capacity(ROUNDS);
    let mut remove_rounds = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let mut builder = WalletProjectionDigestBuilder::new();
        let started = Instant::now();
        for _ in 0..CALLS_PER_ROUND {
            builder.append_row(family, black_box(key), black_box(encoded_value))?;
        }
        append_rounds.push(started.elapsed());
        let started = Instant::now();
        for _ in 0..CALLS_PER_ROUND {
            builder.remove_row(family, black_box(key), black_box(encoded_value))?;
        }
        remove_rounds.push(started.elapsed());
        black_box(builder.finish());
    }
    Ok((
        median_nanos_per_call(append_rounds),
        median_nanos_per_call(remove_rounds),
    ))
}

/// Returns the median nanoseconds per `TransparentUtxoSetCommitment::insert`.
fn utxo_commitment_insert_cost(element: &UtxoSetCommitmentElement<'_>) -> u128 {
    let mut rounds = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let mut commitment = TransparentUtxoSetCommitment::empty();
        let started = Instant::now();
        for _ in 0..CALLS_PER_ROUND {
            commitment.insert(black_box(element));
        }
        rounds.push(started.elapsed());
        black_box(commitment.display_digest());
    }
    median_nanos_per_call(rounds)
}

fn median_nanos_per_call(mut rounds: Vec<Duration>) -> u128 {
    rounds.sort_unstable();
    rounds[rounds.len() / 2].as_nanos() / u128::from(CALLS_PER_ROUND)
}
