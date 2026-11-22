//! Builds a synthetic canonical `RocksDB` fixture for wallet-reader A/B runs.

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use eyre::{Context as _, ensure, eyre};
use parking_lot::Mutex;
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingSpend,
    CompactTransaction, CompactTransactionData, Network, TransactionComponentCounts, TransactionId,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::{
    compat::lightwalletd as lwd, v1::ingest::canonical_control_server::CanonicalControlServer,
};
use zinder_store::CanonicalEventFence;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
};

#[path = "../tests/common/canonical_control.rs"]
mod canonical_control;

use canonical_control::{MutableCanonicalControl, writer_status_for_fence};

fn write_json(path: &Path, document: &Value) -> eyre::Result<()> {
    let temporary = path.with_extension("incoming");
    fs::write(&temporary, serde_json::to_vec_pretty(document)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn encode_wire_block(
    height: u32,
    block: &zinder_testkit::FixtureBlock,
    previous_hash: zinder_core::BlockHash,
    txid: TransactionId,
    spends: &[CompactSaplingSpend],
) -> Vec<u8> {
    lwd::CompactBlock {
        height: height.into(),
        hash: encode_internal_block_hash(block.hash).to_vec(),
        prev_hash: encode_internal_block_hash(previous_hash).to_vec(),
        time: block.block_time_seconds,
        header: Vec::new(),
        chain_metadata: Some(lwd::ChainMetadata::default()),
        vtx: vec![lwd::CompactTx {
            txid: encode_internal_transaction_id(txid).to_vec(),
            spends: spends
                .iter()
                .map(|spend| lwd::CompactSaplingSpend {
                    nf: spend.nullifier.to_vec(),
                })
                .collect(),
            ..lwd::CompactTx::default()
        }],
    }
    .encode_to_vec()
}

fn build_compact_artifact(
    height: BlockHeight,
    block: &zinder_testkit::FixtureBlock,
    previous_hash: zinder_core::BlockHash,
    txid: TransactionId,
    spends: Vec<CompactSaplingSpend>,
) -> eyre::Result<CompactBlockArtifact> {
    let compact_transaction = CompactTransaction {
        index: 0,
        transaction_id: txid,
        data: CompactTransactionData {
            sapling_spends: spends,
            ..CompactTransactionData::default()
        },
    };
    CompactBlockArtifact::new(
        BlockId::new(height, block.hash),
        previous_hash,
        block.block_time_seconds,
        vec![compact_transaction],
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )
    .map_err(Into::into)
}

fn build_chain(output: &Path, blocks: u32, spends_per_block: u32) -> eyre::Result<ChainFixture> {
    let network = Network::ZcashRegtest;
    let total_blocks = blocks
        .checked_add(1)
        .ok_or_else(|| eyre!("block count overflow"))?;
    let mut chain = ChainFixture::new(network).extend_blocks(total_blocks);
    let rows_dir = output.join("rows");
    fs::create_dir(&rows_dir).wrap_err("create fresh rows directory")?;
    let mut summary = Vec::with_capacity(total_blocks as usize);
    for height_value in 1..=total_blocks {
        let height = BlockHeight::new(height_value);
        let block = chain
            .block_at(height)
            .ok_or_else(|| eyre!("missing fixture block {height_value}"))?
            .clone();
        let previous_hash = if height_value == 1 {
            network.genesis_hash()
        } else {
            block.parent_hash
        };
        let mut txid_bytes = [0_u8; 32];
        txid_bytes[..4].copy_from_slice(&height_value.to_le_bytes());
        let txid = TransactionId::from_bytes(txid_bytes);
        let spends: Vec<_> = (0..spends_per_block)
            .map(|index| {
                let mut nullifier = [0_u8; 32];
                nullifier[..4].copy_from_slice(&height_value.to_le_bytes());
                nullifier[4..8].copy_from_slice(&index.to_le_bytes());
                CompactSaplingSpend { nullifier }
            })
            .collect();
        let encoded = encode_wire_block(height_value, &block, previous_hash, txid, &spends);
        fs::write(rows_dir.join(format!("{height_value}.bin")), &encoded)?;
        summary.push(json!({
            "height": height_value,
            "bytes": encoded.len(),
            "sha256": hex::encode(Sha256::digest(&encoded)),
            "hash": hex::encode(block.hash.as_bytes()),
        }));
        let mut transaction_rows = FixtureTransactionRows::from_raw_transaction(
            txid,
            height,
            block.hash,
            0,
            format!("transaction-{height_value}").into_bytes(),
        );
        transaction_rows.facts.public_facts.counts = TransactionComponentCounts {
            sapling_spend_count: spends_per_block,
            ..TransactionComponentCounts::EMPTY
        };
        let artifact = build_compact_artifact(height, &block, previous_hash, txid, spends)?;
        chain = chain
            .with_transaction_rows(transaction_rows)
            .with_compact_block_artifact(artifact);
    }
    write_json(&output.join("rows.json"), &json!(summary))?;
    Ok(chain)
}

#[derive(Debug, Parser)]
#[command(about = "Build a synthetic canonical store for wallet-reader A/B runs")]
struct Arguments {
    /// New output directory for ready.json, append controls, and row oracles.
    #[arg(long)]
    directory: PathBuf,
    /// Number of blocks in the initial canonical store.
    #[arg(long, default_value_t = 16)]
    blocks: u32,
    /// Sapling spends encoded in each synthetic compact block.
    #[arg(long = "spends-per-block", default_value_t = 64)]
    spends_per_block: u32,
}

fn write_ready(
    arguments: &Arguments,
    writer: &CanonicalWriterFixture,
    chain: &ChainFixture,
    address: SocketAddr,
    fence: CanonicalEventFence,
) -> eyre::Result<()> {
    let next_height = arguments.blocks + 1;
    let next = chain
        .block_at(BlockHeight::new(next_height))
        .ok_or_else(|| eyre!("append block missing"))?;
    write_json(
        &arguments.directory.join("ready.json"),
        &json!({
            "canonical_path": writer.canonical_primary_path(),
            "control_endpoint": format!("http://{address}"),
            "initial_height": arguments.blocks,
            "initial_hash": hex::encode(fence.visible_tip().hash.as_bytes()),
            "next_height": next_height,
            "next_hash": hex::encode(next.hash.as_bytes()),
            "hash_order": "internal",
            "reorg_window": 100,
            "network": "Regtest",
            "spends_per_height": arguments.spends_per_block,
            "raw_blob_retention": chain.raw_blob_retention().as_kebab_case(),
        }),
    )
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let arguments = Arguments::parse();
    let blocks = arguments.blocks;
    let spends_per_block = arguments.spends_per_block;
    ensure!(blocks > 0, "--blocks must be greater than zero");
    let next_height = blocks
        .checked_add(1)
        .ok_or_else(|| eyre!("block count overflow"))?;
    fs::create_dir(&arguments.directory).wrap_err("--directory must be a new directory")?;
    let chain = build_chain(&arguments.directory, blocks, spends_per_block)?;
    let network = chain.network();
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(blocks)),
        &zinder_testkit::sample_regtest_upgrade_activations(),
    )?;
    let fence = writer.canonical_primary()?.event_fence();
    let status = Arc::new(Mutex::new(writer_status_for_fence(
        fence,
        network,
        writer.canonical_primary()?.network_upgrade_schedule(),
    )));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();
    let service = MutableCanonicalControl {
        status: Arc::clone(&status),
    };
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(CanonicalControlServer::new(service))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await
    });
    write_ready(&arguments, &writer, &chain, address, fence)?;
    let expected_next_hash = chain
        .block_at(BlockHeight::new(next_height))
        .ok_or_else(|| eyre!("append block missing"))?
        .hash;
    let mut appended = false;
    while !arguments.directory.join("stop").exists() {
        if !appended && arguments.directory.join("append.go").exists() {
            let fence = writer.append_live_block(&chain)?;
            ensure!(
                fence.visible_tip().height == BlockHeight::new(next_height)
                    && fence.visible_tip().hash == expected_next_hash,
                "writer append fence mismatch"
            );
            *status.lock() = writer_status_for_fence(
                fence,
                network,
                writer.canonical_primary()?.network_upgrade_schedule(),
            );
            write_json(
                &arguments.directory.join("appended.json"),
                &json!({
                    "height": next_height,
                    "hash": hex::encode(expected_next_hash.as_bytes())
                }),
            )?;
            appended = true;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    cancellation.cancel();
    server.await??;
    Ok(())
}
