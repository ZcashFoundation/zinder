//! Worked example: observe a transparent address across the mempool boundary.
//!
//! Pairs with [`observe_transparent_address`] to demonstrate the
//! mempool-to-confirmed handoff:
//!
//! 1. Page the live mempool through [`EndpointBackedIndex::mempool_snapshot`]
//!    and keep the entries whose transparent outputs pay the watched address.
//! 2. Subscribe to the mempool event log via
//!    [`EndpointBackedIndex::mempool_events`] from the snapshot's resume
//!    cursor to receive `Added`/`Mined`/`Invalidated` transitions.
//! 3. Track each unconfirmed transaction id observed for the address until
//!    one of those transitions resolves it; emit one line per state change.
//!
//! Deduplication rule at the boundary: when an entry first appears via the
//! snapshot, the example records `(transaction_id, address)`. When the same
//! transaction id arrives via `MempoolEvent::Mined`, the example treats the
//! confirmation as the canonical state change and discards the unconfirmed
//! record. This mirrors the contract a wallet would implement when handing
//! the zero-conf to one-conf transition off to its persistent store.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "Worked example demonstrates console output; its target is operator-facing terminal use."
)]
//!
//! Run it after starting Zinder:
//!
//! ```bash
//! cargo run -p zinder-client --example observe_mempool_to_confirmed -- \
//!     http://127.0.0.1:9102 zcash-regtest tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx
//! ```

use std::collections::HashSet;
use std::env;
use std::process::ExitCode;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;
use zinder_client::{
    EndpointBackedIndex, EventStreamStart, IndexerError, MempoolEntry, MempoolEvent,
    MempoolEventCursor, MempoolSnapshotRequest, Network, RemoteChainIndex, RemoteOpenOptions,
    RetryPolicy, TransactionId, TransparentAddressScriptHash,
};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 256;

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
                "usage: observe_mempool_to_confirmed <endpoint> <network> <t-address>\n\
                 example: observe_mempool_to_confirmed http://127.0.0.1:9102 zcash-regtest tm..."
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
        eprintln!("observe_mempool_to_confirmed: fatal: {error}");
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
                RetryPolicy::OperatorActionRequired | RetryPolicy::ClientError => {
                    return Err(error);
                }
                _ => return Err(error),
            },
        }
    }
}

async fn observe_once(endpoint: &str, network: Network, address: &str) -> Result<(), IndexerError> {
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint: endpoint.to_owned(),
        network,
    })?;

    let script_hash = transparent_address_script_hash(address)?;
    let (watched, resume_cursor) = snapshot_mempool(&chain_index, script_hash, address).await?;
    subscribe_mempool_events(chain_index, watched, resume_cursor, script_hash).await
}

async fn snapshot_mempool(
    chain_index: &RemoteChainIndex,
    script_hash: TransparentAddressScriptHash,
    address: &str,
) -> Result<(HashSet<TransactionId>, Option<MempoolEventCursor>), IndexerError> {
    let mut watched = HashSet::new();
    let mut request = MempoolSnapshotRequest {
        max_entries: MEMPOOL_SNAPSHOT_PAGE_SIZE,
        from_cursor: None,
    };
    loop {
        let page = chain_index.mempool_snapshot(request).await?;
        for entry in &page.entries {
            for output in entry
                .transparent_outputs()
                .iter()
                .filter(|output| output.address_script_hash == script_hash)
            {
                watched.insert(entry.transaction_id());
                println!(
                    "mempool snapshot: address={} outpoint={:?} value_zat={}",
                    address, output.outpoint, output.value_zat
                );
            }
        }
        let Some(next_cursor) = page.next_cursor else {
            println!("snapshot complete: tracked={}", watched.len());
            return Ok((watched, page.events_resume_cursor));
        };
        request = MempoolSnapshotRequest {
            max_entries: MEMPOOL_SNAPSHOT_PAGE_SIZE,
            from_cursor: Some(next_cursor),
        };
    }
}

async fn subscribe_mempool_events(
    chain_index: RemoteChainIndex,
    initial_watched: HashSet<TransactionId>,
    resume_cursor: Option<MempoolEventCursor>,
    script_hash: TransparentAddressScriptHash,
) -> Result<(), IndexerError> {
    let start = resume_cursor.map_or(
        EventStreamStart::EarliestRetained,
        EventStreamStart::AfterCursor,
    );
    let mut events = chain_index.mempool_events(start).await?;
    let mut watched = initial_watched;
    println!(
        "subscribed to mempool events; tracking {} transactions",
        watched.len()
    );
    while let Some(envelope) = events.next().await {
        let envelope = envelope?;
        match envelope.event {
            MempoolEvent::Added { entry } => {
                if entry_pays_address(&entry, script_hash) && watched.insert(entry.transaction_id())
                {
                    println!(
                        "mempool added: tx={:?} first_seen_unix_millis={}",
                        entry.transaction_id(),
                        entry.first_seen_unix_millis().value()
                    );
                }
            }
            MempoolEvent::Mined {
                transaction_id,
                mined_height,
                block_hash,
            } => {
                if watched.remove(&transaction_id) {
                    println!(
                        "mempool mined: tx={:?} height={} block_hash={:?}",
                        transaction_id,
                        mined_height.value(),
                        block_hash
                    );
                }
            }
            MempoolEvent::Invalidated {
                transaction_id,
                reason,
            } => {
                if watched.remove(&transaction_id) {
                    println!("mempool invalidated: tx={transaction_id:?} reason={reason:?}");
                }
            }
            _ => {
                println!(
                    "mempool event: sequence={} (unhandled variant)",
                    envelope.event_sequence
                );
            }
        }
    }
    Ok(())
}

fn entry_pays_address(entry: &MempoolEntry, script_hash: TransparentAddressScriptHash) -> bool {
    entry
        .transparent_outputs()
        .iter()
        .any(|output| output.address_script_hash == script_hash)
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
