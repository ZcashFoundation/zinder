//! Worked example: observe a transparent address.
//!
//! Demonstrates the canonical `snapshot once, subscribe forever, re-derive
//! on hint` pattern from `docs/architecture/chain-events.md`.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "Worked example demonstrates console output; its target is operator-facing terminal use."
)]
//!
//! The example:
//!
//! 1. Connects to a remote Zinder `WalletQuery` endpoint.
//! 2. Reads the server's identity through `ServerInfo` so the operator can
//!    confirm the deployment.
//! 3. Snapshots current UTXOs for the watched address.
//! 4. Subscribes to chain events with `address_filter = [watched]`. Each
//!    received envelope is an *invalidation hint*: the watched address may
//!    have new activity at the committed range; the consumer re-derives
//!    per-address state from the committed compact block.
//!
//! Run it after starting Zinder:
//!
//! ```bash
//! cargo run -p zinder-client --example observe_transparent_address -- \
//!     http://127.0.0.1:9102 zcash-regtest tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx
//! ```
//!
//! The third argument is the transparent t-address you want to watch. The
//! example handles reconnect with exponential backoff.

use std::env;
use std::process::ExitCode;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;
use zinder_client::{
    BlockHeight, ChainEvent, ChainEventStreamFamily, ChainIndex, ChainSnapshot,
    EndpointBackedIndex, EventStreamStart, IndexerError, Network, RemoteChainIndex,
    RemoteClientOptions, RemoteOpenOptions, RetryPolicy, TransparentAddressScriptHash,
};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().collect::<Vec<_>>();
    let [_, endpoint, network_name, address] =
        if let [program, endpoint, network_name, address] = args.as_slice() {
            [
                program.clone(),
                endpoint.clone(),
                network_name.clone(),
                address.clone(),
            ]
        } else {
            eprintln!(
                "usage: observe_transparent_address <endpoint> <network> <t-address>\n\
             example: observe_transparent_address http://127.0.0.1:9102 zcash-regtest tm..."
            );
            return ExitCode::from(2);
        };

    let network = match network_name.as_str() {
        "zcash-mainnet" => Network::ZcashMainnet,
        "zcash-testnet" => Network::ZcashTestnet,
        "zcash-regtest" => Network::ZcashRegtest,
        other => {
            eprintln!(
                "unknown network {other:?}; expected one of zcash-mainnet, zcash-testnet, zcash-regtest"
            );
            return ExitCode::from(2);
        }
    };

    if let Err(error) = run(endpoint, network, address).await {
        eprintln!("observe_transparent_address: fatal: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run(endpoint: String, network: Network, address: String) -> Result<(), IndexerError> {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match observe_once(&endpoint, network, &address).await {
            Ok(()) => return Ok(()),
            Err(error) => match error.retry_policy() {
                RetryPolicy::RetryWithBackoff => {
                    eprintln!(
                        "retryable failure ({error}); backing off for {} ms",
                        backoff.as_millis()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                RetryPolicy::RefreshChainEpoch | RetryPolicy::RestartFromEarliestRetained => {}
                RetryPolicy::OperatorActionRequired
                | RetryPolicy::ClientError
                | RetryPolicy::RefreshMempoolSnapshot => {
                    return Err(error);
                }
                // RetryPolicy is non-exhaustive; fail closed on unknown
                // policies so a new variant cannot silently be retried.
                _ => return Err(error),
            },
        }
    }
}

async fn observe_once(endpoint: &str, network: Network, address: &str) -> Result<(), IndexerError> {
    let chain_index = RemoteChainIndex::connect_with_options(
        RemoteOpenOptions {
            endpoint: endpoint.to_owned(),
            network,
        },
        RemoteClientOptions::default().with_read_retries(),
    )?;

    let wallet_info = chain_index.server_info().await?;
    println!(
        "connected: network={:?} service_version={} schema_version={}",
        wallet_info.network,
        wallet_info.service_version,
        wallet_info.schema_version.value()
    );

    let script_hash = transparent_address_script_hash(address)?;

    let snapshot = chain_index.snapshot().await?;
    snapshot_utxos(&snapshot, script_hash, address).await?;
    subscribe_chain_events(chain_index, address).await
}

async fn snapshot_utxos(
    snapshot: &ChainSnapshot<'_, RemoteChainIndex>,
    script_hash: TransparentAddressScriptHash,
    address: &str,
) -> Result<(), IndexerError> {
    let mut total_zat: u64 = 0;
    let mut utxo_count: u32 = 0;
    let mut unspent_outputs = snapshot
        .transparent_address_unspent_outputs(script_hash, BlockHeight::new(0))
        .await?;
    while let Some(unspent_item) = unspent_outputs.next().await {
        let utxo = unspent_item?.output;
        utxo_count += 1;
        total_zat = total_zat.saturating_add(utxo.value_zat);
        println!(
            "snapshot utxo: address={} height={} value_zat={} outpoint={:?}",
            address,
            utxo.block_height.value(),
            utxo.value_zat,
            utxo.outpoint
        );
    }
    println!("snapshot complete: utxos={utxo_count} total_zat={total_zat}");
    Ok(())
}

async fn subscribe_chain_events(
    chain_index: RemoteChainIndex,
    address: &str,
) -> Result<(), IndexerError> {
    let mut events = chain_index
        .chain_events_with_filter(
            EventStreamStart::EarliestRetained,
            ChainEventStreamFamily::Visible,
            vec![address.to_owned()],
        )
        .await?;
    println!("subscribed to chain events with address_filter=[{address}]");
    while let Some(envelope) = events.next().await {
        let envelope = envelope?;
        match envelope.event {
            ChainEvent::ChainCommitted { committed } => {
                println!(
                    "invalidation: kind=commit chain_epoch={} range={}..={}",
                    committed.chain_epoch.id.value(),
                    committed.block_range.start.value(),
                    committed.block_range.end.value(),
                );
            }
            ChainEvent::ChainReorged {
                reverted,
                committed,
            } => {
                println!(
                    "invalidation: kind=reorg reverted={}..={} committed={}..={}",
                    reverted.block_range.start.value(),
                    reverted.block_range.end.value(),
                    committed.block_range.start.value(),
                    committed.block_range.end.value(),
                );
            }
            // ChainEvent is non-exhaustive; future variants land as ignored
            // hints (the consumer should re-derive on any envelope).
            _ => {
                println!(
                    "invalidation: kind=unknown event_sequence={}",
                    envelope.event_sequence
                );
            }
        }
        // Production consumers re-derive per-address UTXO state here by
        // calling `compact_block_at` for each height in the committed
        // range and merging the result into their wallet store.
    }
    Ok(())
}

fn transparent_address_script_hash(
    address: &str,
) -> Result<TransparentAddressScriptHash, IndexerError> {
    use zebra_chain::transparent::Address as ZebraTransparentAddress;
    let zebra_address =
        address
            .parse::<ZebraTransparentAddress>()
            .map_err(|_| IndexerError::InvalidRequest {
                reason: format!("transparent address {address:?} could not be parsed"),
            })?;
    let script_pub_key = zebra_address.script().as_raw_bytes().to_vec();
    if script_pub_key.is_empty() {
        return Err(IndexerError::InvalidRequest {
            reason: format!("transparent address {address:?} has no receivable script"),
        });
    }
    let mut hasher = Sha256::new();
    hasher.update(&script_pub_key);
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(TransparentAddressScriptHash::from_bytes(digest))
}
