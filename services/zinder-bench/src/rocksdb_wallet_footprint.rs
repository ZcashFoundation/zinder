//! Per-column-family footprint, cold validation, and warm transparent-query
//! latency harness for `zinder-wallet-rocksdb` on Mainnet-shaped workloads.

use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU16,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Args;
use rust_rocksdb::{DB, Options};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zinder_bench::BenchError;
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts,
    ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontiers, CompactBlockArtifact,
    CompactChainMetadata, ConsensusBranchId, LockTime, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations, PrivacyShape, SerializedBytesDigest, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
    UnixTimestampMillis, encode_canonical_block_replay,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
    CanonicalStoreBuildPlan, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::WALLET_PROJECTION_SCHEMA_VERSION;
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildReport, RocksDbWalletSecondary,
    WALLET_ROCKSDB_SCHEMA_VERSION, build_wallet_from_canonical,
};

const FIXTURE_VERSION: u16 = 1;
const REPORT_FORMAT_VERSION: u16 = 1;
const BASE_TIMESTAMP_MILLIS: u64 = 1_780_000_000_000;
const COINBASE_VALUE_ZAT: u64 = 312_500_000;
const QUERY_PAGE_SIZE: u16 = 256;
const HEAVY_ADDRESS_COUNT: usize = 16;
const MEDIUM_ADDRESS_COUNT: usize = 128;
const SAMPLE_HEAVY_COUNT: usize = 8;
const SAMPLE_MEDIUM_COUNT: usize = 16;
const SAMPLE_LOW_COUNT: usize = 16;
const SAMPLE_ABSENT_COUNT: usize = 4;

/// CLI arguments for measuring wallet projection `RocksDB` footprint and latency.
#[derive(Args)]
pub(crate) struct RocksDbWalletFootprintArgs {
    /// Total blocks generated for the synthetic transparent chain (>= 2).
    #[arg(long, default_value_t = 2_000)]
    pub(crate) blocks: u32,
    /// Transparent outputs created per block (>= 2).
    #[arg(long = "outputs-per-block", default_value_t = 80)]
    pub(crate) outputs_per_block: u32,
    /// Measured warm-cache query rounds across sampled addresses (>= 1).
    #[arg(long = "warm-iterations", default_value_t = 10)]
    pub(crate) warm_iterations: u32,
    /// Target logical bytes per generated SST file during bulk build.
    #[arg(long = "sst-target-bytes", default_value_t = 2 * 1024 * 1024)]
    pub(crate) sst_target_bytes: u64,
    /// Write the JSON report to this absent path instead of stdout.
    #[arg(long = "output-json")]
    pub(crate) output_json: Option<PathBuf>,
    /// Optional non-existent directory to keep generated stores for inspection.
    #[arg(long = "keep-dir")]
    pub(crate) keep_dir: Option<PathBuf>,
}

/// Complete footprint, validation, latency, and parity report.
#[derive(Debug, Serialize)]
pub(crate) struct WalletFootprintReport {
    pub(crate) contract_identity: &'static str,
    pub(crate) report_format_version: u16,
    pub(crate) fixture_version: u16,
    pub(crate) wallet_rocksdb_schema_version: u16,
    pub(crate) wallet_projection_schema_version: u16,
    pub(crate) blocks: u32,
    pub(crate) outputs_per_block: u32,
    pub(crate) total_created_outputs: u64,
    pub(crate) total_spent_outputs: u64,
    pub(crate) total_unspent_outputs: u64,
    pub(crate) sampled_address_count: usize,
    pub(crate) warm_iterations: u32,
    pub(crate) wallet_build: WalletBuildSummary,
    pub(crate) total_sst_bytes: u64,
    pub(crate) total_logical_bytes: u64,
    pub(crate) total_data_compressed_bytes: u64,
    pub(crate) total_data_uncompressed_bytes: u64,
    pub(crate) total_index_bytes: u64,
    pub(crate) total_filter_bytes: u64,
    pub(crate) overall_data_compression_ratio: f64,
    pub(crate) overall_sst_compression_ratio: f64,
    pub(crate) physical_wallet_dir_bytes: u64,
    pub(crate) column_families: BTreeMap<String, ColumnFamilyFootprint>,
    pub(crate) warm_queries: WarmQueryBenchmarks,
    pub(crate) parity_digests: ParityDigests,
}

/// Build and cold-readback timings reported by `build_wallet_from_canonical`.
#[derive(Debug, Serialize)]
pub(crate) struct WalletBuildSummary {
    pub(crate) tip_height: u32,
    pub(crate) settled_height: u32,
    pub(crate) logical_row_bytes: u64,
    pub(crate) sst_file_bytes: u64,
    pub(crate) sst_file_count: u64,
    pub(crate) total_build_seconds: f64,
    pub(crate) canonical_scan_seconds: f64,
    pub(crate) outpoint_sort_seconds: f64,
    pub(crate) outpoint_merge_seconds: f64,
    pub(crate) secondary_row_derivation_seconds: f64,
    pub(crate) row_load_seconds: f64,
    pub(crate) flush_and_cold_reopen_seconds: f64,
    pub(crate) cold_validation_seconds: f64,
    pub(crate) cold_validation_random_read_count: u64,
}

/// Per-column-family logical bytes and physical SST component sizes.
#[derive(Debug, Serialize)]
pub(crate) struct ColumnFamilyFootprint {
    pub(crate) num_entries: u64,
    pub(crate) raw_key_bytes: u64,
    pub(crate) raw_value_bytes: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) data_compressed_bytes: u64,
    pub(crate) data_uncompressed_bytes: u64,
    pub(crate) index_bytes: u64,
    pub(crate) top_level_index_bytes: u64,
    pub(crate) filter_bytes: u64,
    pub(crate) num_data_blocks: u64,
    pub(crate) num_filter_entries: u64,
    pub(crate) index_partitions: u64,
    pub(crate) total_sst_files_bytes: u64,
    pub(crate) estimate_live_data_bytes: u64,
    pub(crate) data_compression_ratio: f64,
    pub(crate) sst_compression_ratio: f64,
}

/// Warm-cache query latency summaries for the three transparent serving paths.
#[derive(Debug, Serialize)]
pub(crate) struct WarmQueryBenchmarks {
    pub(crate) taddress_txids: LatencyStats,
    pub(crate) taddress_balance: LatencyStats,
    pub(crate) address_utxos: LatencyStats,
}

/// Percentile latencies (microseconds) across all measured query calls.
#[derive(Debug, Serialize)]
pub(crate) struct LatencyStats {
    pub(crate) calls: usize,
    pub(crate) total_rows_returned: u64,
    pub(crate) mean_us: f64,
    pub(crate) p50_us: f64,
    pub(crate) p95_us: f64,
    pub(crate) p99_us: f64,
    pub(crate) max_us: f64,
}

/// Content hashes (SHA-256 hex) proving byte-identical serving answers across layouts.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ParityDigests {
    pub(crate) taddress_txids: String,
    pub(crate) taddress_balance: String,
    pub(crate) address_utxos: String,
}

#[derive(Clone)]
struct AddressEntry {
    address_script_hash: TransparentAddressScriptHash,
    script_pub_key: Vec<u8>,
}

struct LiveOutpoint {
    outpoint: TransparentOutPoint,
}

struct GeneratedChain {
    blocks: Vec<CanonicalBlockFacts>,
    sampled_addresses: Vec<TransparentAddressScriptHash>,
    total_created_outputs: u64,
    total_spent_outputs: u64,
    total_unspent_outputs: u64,
    supported_reorg_depth: u32,
}

struct RunWorkspace {
    _temp: Option<TempDir>,
    root: PathBuf,
}

struct ExecutionBudgets {
    canonical_writer: RocksDbResourceBudget,
    wallet_writer: RocksDbResourceBudget,
    wallet_reader: RocksDbResourceBudget,
    sst_target_logical_bytes: u64,
    local_workers_only: bool,
}

/// Executes the wallet footprint and warm latency benchmark.
pub(crate) fn run(args: &RocksDbWalletFootprintArgs) -> Result<WalletFootprintReport, BenchError> {
    let budgets = ExecutionBudgets {
        canonical_writer: RocksDbResourceBudget::for_local_tests(),
        wallet_writer: RocksDbResourceBudget::wallet_projection_writer_defaults(),
        wallet_reader: RocksDbResourceBudget::wallet_projection_reader_defaults(),
        sst_target_logical_bytes: args.sst_target_bytes,
        local_workers_only: false,
    };
    measure_footprint(args, &budgets)
}

fn measure_footprint(
    args: &RocksDbWalletFootprintArgs,
    budgets: &ExecutionBudgets,
) -> Result<WalletFootprintReport, BenchError> {
    validate_args(args)?;
    let workspace = prepare_workspace(args.keep_dir.as_deref())?;
    let chain = generate_mainnet_shaped_chain(args.blocks, args.outputs_per_block)?;
    let activations = test_activations()?;

    let canonical_path = workspace.root.join("canonical");
    let wallet_path = workspace.root.join("wallet");
    let secondary_path = workspace.root.join("wallet-secondary");

    let canonical_store = build_canonical_store(
        &canonical_path,
        &activations,
        &chain,
        budgets.canonical_writer,
    )?;
    let build_options = RocksDbWalletBuildOptions {
        resource_budget: budgets.wallet_writer,
        sst_target_logical_bytes: budgets.sst_target_logical_bytes,
        supported_reorg_depth: chain.supported_reorg_depth,
        accumulator_hashing_workers: if budgets.local_workers_only {
            RocksDbWalletBuildOptions::for_local_tests().accumulator_hashing_workers
        } else {
            RocksDbWalletBuildOptions::automatic_accumulator_hashing_workers()
        },
        ..RocksDbWalletBuildOptions::for_local_tests()
    };
    let build_outcome = build_wallet_from_canonical(&canonical_store, &wallet_path, build_options)?;
    drop(canonical_store);
    drop(build_outcome.store);

    let secondary = RocksDbWalletSecondary::open_ready(
        &wallet_path,
        &secondary_path,
        Network::ZcashRegtest,
        budgets.wallet_reader,
    )?;
    let column_families = collect_column_family_footprints(&wallet_path, &secondary)?;
    let (warm_queries, parity_digests) = benchmark_warm_queries(
        &secondary,
        &chain.sampled_addresses,
        chain.blocks.len(),
        args.warm_iterations,
    )?;
    drop(secondary);

    let physical_wallet_dir_bytes = directory_size_bytes(&wallet_path)?;
    Ok(assemble_report(ReportInputs {
        args,
        chain: &chain,
        build: &build_outcome.report,
        column_families,
        warm_queries,
        parity_digests,
        physical_wallet_dir_bytes,
    }))
}

fn validate_args(args: &RocksDbWalletFootprintArgs) -> Result<(), BenchError> {
    if args.blocks < 2 {
        return Err(BenchError::invalid_argument("--blocks must be at least 2"));
    }
    if args.outputs_per_block < 2 {
        return Err(BenchError::invalid_argument(
            "--outputs-per-block must be at least 2",
        ));
    }
    if args.warm_iterations == 0 {
        return Err(BenchError::invalid_argument(
            "--warm-iterations must be at least 1",
        ));
    }
    if args.sst_target_bytes == 0 {
        return Err(BenchError::invalid_argument(
            "--sst-target-bytes must be greater than zero",
        ));
    }
    Ok(())
}

fn prepare_workspace(keep_dir: Option<&Path>) -> Result<RunWorkspace, BenchError> {
    if let Some(root) = keep_dir {
        fs::create_dir(root).map_err(|source| BenchError::io(root, source))?;
        return Ok(RunWorkspace {
            _temp: None,
            root: root.to_path_buf(),
        });
    }
    let temporary = tempfile::tempdir()
        .map_err(|source| BenchError::io(Path::new("wallet-footprint-tempdir"), source))?;
    let root = temporary.path().to_path_buf();
    Ok(RunWorkspace {
        _temp: Some(temporary),
        root,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "fixture generator keeps address tiers, output spends, and sampling deterministic in one place"
)]
fn generate_mainnet_shaped_chain(
    blocks_count: u32,
    outputs_per_block: u32,
) -> Result<GeneratedChain, BenchError> {
    let total_outputs = u64::from(blocks_count) * u64::from(outputs_per_block);
    let total_target_spends = (total_outputs * 88) / 100;
    let tier2_count = usize::try_from((total_outputs / 3).max(32)).unwrap_or(10_000);
    let address_count = HEAVY_ADDRESS_COUNT + MEDIUM_ADDRESS_COUNT + tier2_count;
    let addresses = (0..address_count)
        .map(deterministic_address)
        .collect::<Vec<_>>();

    let supported_reorg_depth = if blocks_count > 100 {
        100
    } else {
        blocks_count / 2
    };

    let mut generated_blocks = Vec::with_capacity(usize::try_from(blocks_count).unwrap_or(0));
    let mut live_utxos = Vec::<LiveOutpoint>::new();
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut created_so_far = 0_u64;
    let mut spent_so_far = 0_u64;
    let denom_blocks = u64::from(blocks_count.saturating_sub(1).max(1));

    for block_offset in 0..blocks_count {
        let height = block_offset + 1;
        let block_hash = BlockHash::from_bytes(hash_tag(b"block", height, 0));
        let desired_spends = if block_offset == 0 {
            0
        } else {
            let cumulative_target = (total_target_spends * u64::from(block_offset)) / denom_blocks;
            cumulative_target
                .saturating_sub(spent_so_far)
                .min(u64::try_from(live_utxos.len()).unwrap_or(u64::MAX))
        };
        let mut block_spends_left = usize::try_from(desired_spends).unwrap_or(0);
        let mut transactions = Vec::new();
        let mut tx_local_index = 0_u32;

        // Coinbases create 1 output on normal blocks, or all block outputs when no spendable inputs exist.
        let coinbase_txid = TransactionId::from_bytes(hash_tag(b"tx", height, tx_local_index));
        let coinbase_outputs_count = if block_spends_left == 0 {
            outputs_per_block
        } else {
            1
        };
        let mut coinbase_outputs =
            Vec::with_capacity(usize::try_from(coinbase_outputs_count).unwrap_or(1));
        for out_idx in 0..coinbase_outputs_count {
            let coinbase_addr = pick_address(&addresses, created_so_far, height);
            let output_value_zat = if out_idx == 0 {
                COINBASE_VALUE_ZAT
            } else {
                50_000 + ((created_so_far * 7_919) % 250_000_000)
            };
            coinbase_outputs.push(TransparentOutputFact::new(
                out_idx,
                output_value_zat,
                coinbase_addr.script_pub_key.clone(),
                coinbase_addr.address_script_hash,
            ));
            live_utxos.push(LiveOutpoint {
                outpoint: TransparentOutPoint::new(coinbase_txid, out_idx),
            });
            created_so_far += 1;
        }
        transactions.push(build_tx(
            coinbase_txid,
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            coinbase_outputs,
        ));
        tx_local_index += 1;

        let mut remaining_outputs = outputs_per_block.saturating_sub(coinbase_outputs_count);
        while remaining_outputs > 0 {
            let batch_outputs = if block_spends_left <= 1 {
                remaining_outputs
            } else {
                remaining_outputs.min(2)
            };
            let remaining_tx_slots = usize::try_from(remaining_outputs.div_ceil(2)).unwrap_or(1);
            let inputs_target = if block_spends_left <= 1 || remaining_tx_slots <= 1 {
                block_spends_left
            } else {
                block_spends_left
                    .div_ceil(remaining_tx_slots)
                    .min(block_spends_left)
            };
            let mut tx_inputs = Vec::with_capacity(inputs_target.max(1));
            for input_idx in 0..inputs_target {
                let utxo = take_live_utxo(
                    &mut live_utxos,
                    height,
                    tx_local_index,
                    u32::try_from(input_idx).unwrap_or(0),
                )?;
                let idx_u32 = u32::try_from(input_idx)
                    .map_err(|_| BenchError::report_format("tx input index overflow"))?;
                tx_inputs.push(TransparentInputFact::new(idx_u32, utxo.outpoint));
                block_spends_left -= 1;
                spent_so_far += 1;
            }

            let txid = TransactionId::from_bytes(hash_tag(b"tx", height, tx_local_index));
            let mut tx_outputs = Vec::with_capacity(usize::try_from(batch_outputs).unwrap_or(2));
            for out_idx in 0..batch_outputs {
                let addr = pick_address(&addresses, created_so_far, height);
                let out_val = 50_000 + ((created_so_far * 7_919) % 250_000_000);
                tx_outputs.push(TransparentOutputFact::new(
                    out_idx,
                    out_val,
                    addr.script_pub_key.clone(),
                    addr.address_script_hash,
                ));
                live_utxos.push(LiveOutpoint {
                    outpoint: TransparentOutPoint::new(txid, out_idx),
                });
                created_so_far += 1;
            }
            transactions.push(build_tx(txid, false, tx_inputs, tx_outputs));
            remaining_outputs -= batch_outputs;
            tx_local_index += 1;
        }

        let header = BlockHeaderArtifact::new(
            BlockHeight::new(height),
            block_hash,
            parent_hash,
            [0; 32],
            [0; 32],
            i64::from(height),
            0,
            [0; 32],
            0,
            0,
        );
        let serialized_bytes_digest =
            SerializedBytesDigest::from_serialized_bytes(&block_hash.as_bytes());
        generated_blocks.push(CanonicalBlockFacts {
            block_header: header,
            serialized_bytes_digest,
            transactions,
        });
        parent_hash = block_hash;
    }

    let sampled_addresses = collect_sampled_addresses(&addresses);
    let total_unspent_outputs = u64::try_from(live_utxos.len()).unwrap_or(0);
    Ok(GeneratedChain {
        blocks: generated_blocks,
        sampled_addresses,
        total_created_outputs: created_so_far,
        total_spent_outputs: spent_so_far,
        total_unspent_outputs,
        supported_reorg_depth,
    })
}

fn deterministic_address(index: usize) -> AddressEntry {
    let index_u32 = u32::try_from(index).unwrap_or(u32::MAX);
    let payload = hash_tag(b"addr-script", index_u32, 0);
    let script_pub_key = match index % 100 {
        0..80 => {
            let mut script = Vec::with_capacity(25);
            script.extend_from_slice(&[0x76, 0xa9, 0x14]);
            script.extend_from_slice(&payload[..20]);
            script.extend_from_slice(&[0x88, 0xac]);
            script
        }
        80..98 => {
            let mut script = Vec::with_capacity(23);
            script.extend_from_slice(&[0xa9, 0x14]);
            script.extend_from_slice(&payload[..20]);
            script.push(0x87);
            script
        }
        _ => {
            let mut script = Vec::with_capacity(34);
            script.push(0x51);
            script.push(0x20);
            script.extend_from_slice(&payload);
            script
        }
    };
    AddressEntry {
        address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
        script_pub_key,
    }
}

fn pick_address(addresses: &[AddressEntry], sequence: u64, height: u32) -> &AddressEntry {
    let mix = hash_tag(
        b"pick-addr",
        u32::try_from(sequence & 0xffff_ffff).unwrap_or(0),
        height ^ u32::try_from(sequence >> 32).unwrap_or(0),
    );
    let bucket = u32::from_be_bytes([mix[0], mix[1], mix[2], mix[3]]) % 100;
    let selector = u64::from_be_bytes([
        mix[4], mix[5], mix[6], mix[7], mix[8], mix[9], mix[10], mix[11],
    ]);
    let index = if bucket < 12 {
        usize::try_from(selector % u64::try_from(HEAVY_ADDRESS_COUNT).unwrap_or(1)).unwrap_or(0)
    } else if bucket < 42 {
        HEAVY_ADDRESS_COUNT
            + usize::try_from(selector % u64::try_from(MEDIUM_ADDRESS_COUNT).unwrap_or(1))
                .unwrap_or(0)
    } else {
        let tier2_start = HEAVY_ADDRESS_COUNT + MEDIUM_ADDRESS_COUNT;
        let tier2_len = addresses.len().saturating_sub(tier2_start).max(1);
        tier2_start + usize::try_from(selector % u64::try_from(tier2_len).unwrap_or(1)).unwrap_or(0)
    };
    &addresses[index]
}

fn take_live_utxo(
    live_utxos: &mut Vec<LiveOutpoint>,
    height: u32,
    tx_index: u32,
    input_index: u32,
) -> Result<LiveOutpoint, BenchError> {
    if live_utxos.is_empty() {
        return Err(BenchError::report_format(
            "synthetic fixture ran out of spendable UTXOs",
        ));
    }
    let selector = hash_tag(b"spend-pick", height ^ tx_index, input_index);
    let slot = if selector[0].is_multiple_of(4) {
        0
    } else {
        let raw = u64::from_be_bytes([
            selector[1],
            selector[2],
            selector[3],
            selector[4],
            selector[5],
            selector[6],
            selector[7],
            selector[8],
        ]);
        usize::try_from(raw % u64::try_from(live_utxos.len()).unwrap_or(1)).unwrap_or(0)
    };
    Ok(live_utxos.swap_remove(slot))
}

fn collect_sampled_addresses(addresses: &[AddressEntry]) -> Vec<TransparentAddressScriptHash> {
    let mut sampled = Vec::with_capacity(
        SAMPLE_HEAVY_COUNT + SAMPLE_MEDIUM_COUNT + SAMPLE_LOW_COUNT + SAMPLE_ABSENT_COUNT,
    );
    for addr_entry in addresses
        .iter()
        .take(SAMPLE_HEAVY_COUNT.min(HEAVY_ADDRESS_COUNT))
    {
        sampled.push(addr_entry.address_script_hash);
    }
    let medium_stride = (MEDIUM_ADDRESS_COUNT / SAMPLE_MEDIUM_COUNT).max(1);
    for step in 0..SAMPLE_MEDIUM_COUNT {
        let idx = HEAVY_ADDRESS_COUNT + (step * medium_stride) % MEDIUM_ADDRESS_COUNT;
        sampled.push(addresses[idx].address_script_hash);
    }
    let tier2_start = HEAVY_ADDRESS_COUNT + MEDIUM_ADDRESS_COUNT;
    let tier2_len = addresses.len().saturating_sub(tier2_start).max(1);
    let low_stride = (tier2_len / SAMPLE_LOW_COUNT).max(1);
    for step in 0..SAMPLE_LOW_COUNT {
        let idx = tier2_start + (step * low_stride) % tier2_len;
        if idx < addresses.len() {
            sampled.push(addresses[idx].address_script_hash);
        }
    }
    for absent_idx in 0..u32::try_from(SAMPLE_ABSENT_COUNT).unwrap_or(0) {
        sampled.push(TransparentAddressScriptHash::from_bytes(hash_tag(
            b"absent-address",
            absent_idx,
            0,
        )));
    }
    sampled
}

fn build_tx(
    transaction_id: TransactionId,
    is_coinbase: bool,
    transparent_inputs: Vec<TransparentInputFact>,
    transparent_outputs: Vec<TransparentOutputFact>,
) -> CanonicalTransactionFacts {
    CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V4,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 32,
            counts: TransactionComponentCounts {
                transparent_input_count: u32::try_from(transparent_inputs.len())
                    .unwrap_or(u32::MAX),
                transparent_output_count: u32::try_from(transparent_outputs.len())
                    .unwrap_or(u32::MAX),
                ..TransactionComponentCounts::EMPTY
            },
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase,
            unsupported_sections: Vec::new(),
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &transaction_id.as_bytes(),
        ),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
        transparent_inputs,
        transparent_outputs,
    }
}

fn hash_tag(domain: &[u8], first: u32, second: u32) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"zinder:wallet-footprint:v1\0");
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(first.to_be_bytes());
    hasher.update(second.to_be_bytes());
    hasher.finalize().into()
}

fn test_activations() -> Result<NetworkUpgradeActivations, BenchError> {
    let names = [
        "Overwinter",
        "Sapling",
        "Blossom",
        "Heartwood",
        "Canopy",
        "NU5",
        "NU6",
        "NU6.1",
        "NU6.2",
        "NU6.3",
    ];
    let activations = names
        .into_iter()
        .enumerate()
        .map(|(idx, name)| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(u32::try_from(idx).unwrap_or(0) + 1),
            activation_height: BlockHeight::new(1_000_000 + u32::try_from(idx).unwrap_or(0)),
            name: name.to_owned(),
        })
        .collect();
    NetworkUpgradeActivations::new(Network::ZcashRegtest, activations)
        .map_err(|source| BenchError::invalid_argument(source.to_string()))
}

fn build_canonical_store(
    canonical_path: &Path,
    activations: &NetworkUpgradeActivations,
    chain: &GeneratedChain,
    budget: RocksDbResourceBudget,
) -> Result<RocksDbCanonicalStore, BenchError> {
    let last_facts = chain
        .blocks
        .last()
        .ok_or_else(|| BenchError::report_format("generated chain has no tip block"))?;
    let tip = BlockId::new(
        last_facts.block_header.height,
        last_facts.block_header.block_hash,
    );
    let settled_height_raw = tip
        .height
        .value()
        .saturating_sub(chain.supported_reorg_depth)
        .max(1);
    let settled_facts = &chain.blocks[usize::try_from(settled_height_raw - 1).unwrap_or(0)];
    let settled_tip = BlockId::new(
        settled_facts.block_header.height,
        settled_facts.block_header.block_hash,
    );

    let plan = CanonicalStoreBuildPlan::complete(
        activations,
        0,
        tip,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(chain.supported_reorg_depth)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(canonical_path, plan, budget)?
        .with_every_block_tree_state_checkpoints()?;
    let build_blocks = chain
        .blocks
        .iter()
        .cloned()
        .map(|facts| Ok::<_, std::io::Error>(to_canonical_build_block(facts)));
    builder
        .bulk_load_blocks(activations, build_blocks)
        .map_err(|error| BenchError::report_format(format!("bulk load blocks failed: {error}")))?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
        tip,
        tip.height.value(),
        CommitmentTreeFrontiers::default(),
    ))?;
    let validated = builder.prepare_trusted_fresh_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        settled_tip,
        UnixTimestampMillis::new(BASE_TIMESTAMP_MILLIS),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

fn to_canonical_build_block(facts: CanonicalBlockFacts) -> CanonicalBuildBlock {
    let height = facts.block_header.height;
    let block_hash = facts.block_header.block_hash;
    let parent_hash = facts.block_header.parent_hash;
    let block_id = BlockId::new(height, block_hash);
    let compact_block = CompactBlockArtifact::empty(
        block_id,
        parent_hash,
        height.value(),
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    );
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    CanonicalBuildBlock {
        facts,
        replay_envelope,
        compact_block,
        tip_metadata: ChainTipMetadata::new(0, 0, 0),
        tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
            block_id,
            height.value(),
            CommitmentTreeFrontiers::default(),
        )),
        transaction_blobs: Vec::new(),
        block_blob: None,
    }
}

fn collect_column_family_footprints(
    wallet_path: &Path,
    secondary: &RocksDbWalletSecondary,
) -> Result<BTreeMap<String, ColumnFamilyFootprint>, BenchError> {
    let cf_names = DB::list_cf(&Options::default(), wallet_path).map_err(|source| {
        BenchError::report_format(format!("failed to list wallet CFs: {source}"))
    })?;
    let mut metrics = BTreeMap::new();
    for cf_name in cf_names {
        let cf_static = known_wallet_cf_static(&cf_name);
        let footprint = match cf_static {
            Some(static_name) => read_cf_footprint(secondary, static_name)?,
            None => continue,
        };
        metrics.insert(cf_name, footprint);
    }
    Ok(metrics)
}

fn known_wallet_cf_static(name: &str) -> Option<&'static str> {
    match name {
        "default" => Some("default"),
        "transparent_unspent_output" => Some("transparent_unspent_output"),
        "transparent_unspent_output_by_address" => Some("transparent_unspent_output_by_address"),
        "transparent_spent_output" => Some("transparent_spent_output"),
        "transparent_address_transaction" => Some("transparent_address_transaction"),
        "transparent_address_balance" => Some("transparent_address_balance"),
        "reorg_undo" => Some("reorg_undo"),
        "block_hash_by_height" => Some("block_hash_by_height"),
        _ => None,
    }
}

fn read_cf_footprint(
    secondary: &RocksDbWalletSecondary,
    family: &'static str,
) -> Result<ColumnFamilyFootprint, BenchError> {
    let total_sst_files_bytes =
        read_int_cf_property(secondary, family, "rocksdb.total-sst-files-size")?;
    let estimate_live_data_bytes =
        read_int_cf_property(secondary, family, "rocksdb.estimate-live-data-size")?;
    let agg_text = secondary
        .rocksdb_property_value_cf(family, "rocksdb.aggregated-table-properties")?
        .unwrap_or_default();
    let parsed = parse_aggregated_table_properties(&agg_text)?;

    let raw_key_bytes = parsed.get("raw_key_size").copied().unwrap_or(0);
    let raw_value_bytes = parsed.get("raw_value_size").copied().unwrap_or(0);
    let logical_bytes = raw_key_bytes.saturating_add(raw_value_bytes);
    let data_compressed_bytes = parsed.get("data_size").copied().unwrap_or(0);
    let data_uncompressed_bytes = parsed.get("uncompressed_data_size").copied().unwrap_or(0);

    Ok(ColumnFamilyFootprint {
        num_entries: parsed.get("num_entries").copied().unwrap_or(0),
        raw_key_bytes,
        raw_value_bytes,
        logical_bytes,
        data_compressed_bytes,
        data_uncompressed_bytes,
        index_bytes: parsed.get("index_size").copied().unwrap_or(0),
        top_level_index_bytes: parsed.get("top_level_index_size").copied().unwrap_or(0),
        filter_bytes: parsed.get("filter_size").copied().unwrap_or(0),
        num_data_blocks: parsed.get("num_data_blocks").copied().unwrap_or(0),
        num_filter_entries: parsed.get("num_filter_entries").copied().unwrap_or(0),
        index_partitions: parsed.get("index_partitions").copied().unwrap_or(0),
        total_sst_files_bytes,
        estimate_live_data_bytes,
        data_compression_ratio: safe_ratio(data_uncompressed_bytes, data_compressed_bytes),
        sst_compression_ratio: safe_ratio(logical_bytes, total_sst_files_bytes),
    })
}

fn read_int_cf_property(
    secondary: &RocksDbWalletSecondary,
    family: &'static str,
    property: &str,
) -> Result<u64, BenchError> {
    let text = secondary
        .rocksdb_property_value_cf(family, property)?
        .unwrap_or_else(|| "0".to_owned());
    parse_u64(property, &text)
}

fn parse_aggregated_table_properties(
    properties: &str,
) -> Result<BTreeMap<&'static str, u64>, BenchError> {
    let mut parsed = BTreeMap::new();
    for segment in properties.split(';') {
        let Some((label, digits)) = segment.trim().split_once('=') else {
            continue;
        };
        let key = match label {
            "# entries" => "num_entries",
            "# data blocks" => "num_data_blocks",
            "# entries for filter" => "num_filter_entries",
            "# index partitions" => "index_partitions",
            "top-level index size" => "top_level_index_size",
            "raw key size" => "raw_key_size",
            "raw value size" => "raw_value_size",
            "data block size" => "data_size",
            "data uncompressed size" => "uncompressed_data_size",
            prefix if prefix.starts_with("index block size (") => "index_size",
            "filter block size" => "filter_size",
            _ => continue,
        };
        parsed.insert(key, parse_u64(key, digits)?);
    }
    Ok(parsed)
}

fn parse_u64(label: &str, text: &str) -> Result<u64, BenchError> {
    u64::from_str(text.trim())
        .map_err(|error| BenchError::report_format(format!("invalid {label}: {error}")))
}

fn benchmark_warm_queries(
    secondary: &RocksDbWalletSecondary,
    sampled_addresses: &[TransparentAddressScriptHash],
    block_count: usize,
    warm_iterations: u32,
) -> Result<(WarmQueryBenchmarks, ParityDigests), BenchError> {
    let tip_height = BlockHeight::new(u32::try_from(block_count).unwrap_or(u32::MAX));
    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), tip_height);
    let page_size = NonZeroU16::new(QUERY_PAGE_SIZE).unwrap_or(NonZeroU16::MIN);

    // Warm the block cache and record deterministic parity digests on one full pass.
    let mut txids_hasher = Sha256::new();
    let mut balance_hasher = Sha256::new();
    let mut utxos_hasher = Sha256::new();

    for &address in sampled_addresses {
        let _tx_count =
            hash_txids_pages(secondary, address, full_range, page_size, &mut txids_hasher)?;
        let balance_zat = secondary.address_balance(address)?;
        balance_hasher.update(address.as_bytes());
        balance_hasher.update(balance_zat.to_be_bytes());
        let _utxo_count = hash_utxos_pages(secondary, address, page_size, &mut utxos_hasher)?;
    }

    let parity_digests = ParityDigests {
        taddress_txids: hex::encode(txids_hasher.finalize()),
        taddress_balance: hex::encode(balance_hasher.finalize()),
        address_utxos: hex::encode(utxos_hasher.finalize()),
    };

    let iterations = usize::try_from(warm_iterations).unwrap_or(1);
    let call_cap = iterations.saturating_mul(sampled_addresses.len());
    let mut txid_samples = Vec::with_capacity(call_cap);
    let mut balance_samples = Vec::with_capacity(call_cap);
    let mut utxo_samples = Vec::with_capacity(call_cap);
    let mut total_txid_rows = 0_u64;
    let mut total_balance_rows = 0_u64;
    let mut total_utxo_rows = 0_u64;

    for _ in 0..iterations {
        for &address in sampled_addresses {
            let started = Instant::now();
            let rows = measure_txids_pages(secondary, address, full_range, page_size)?;
            txid_samples.push(started.elapsed());
            total_txid_rows = total_txid_rows.saturating_add(rows);

            let started = Instant::now();
            let balance_zat = secondary.address_balance(address)?;
            std::hint::black_box(balance_zat);
            balance_samples.push(started.elapsed());
            total_balance_rows = total_balance_rows.saturating_add(1);

            let started = Instant::now();
            let utxos = measure_utxos_pages(secondary, address, page_size)?;
            utxo_samples.push(started.elapsed());
            total_utxo_rows = total_utxo_rows.saturating_add(utxos);
        }
    }

    Ok((
        WarmQueryBenchmarks {
            taddress_txids: summarize_latencies(txid_samples, total_txid_rows),
            taddress_balance: summarize_latencies(balance_samples, total_balance_rows),
            address_utxos: summarize_latencies(utxo_samples, total_utxo_rows),
        },
        parity_digests,
    ))
}

fn hash_txids_pages(
    secondary: &RocksDbWalletSecondary,
    address: TransparentAddressScriptHash,
    range: BlockHeightRange,
    page_size: NonZeroU16,
    hasher: &mut Sha256,
) -> Result<u64, BenchError> {
    let mut after = None;
    let mut count = 0_u64;
    hasher.update(address.as_bytes());
    loop {
        let page =
            secondary.address_transaction_history_range_page(address, range, after, page_size)?;
        for tx_row in &page.transactions {
            hasher.update(tx_row.key.block_height().value().to_be_bytes());
            hasher.update(tx_row.key.tx_index_in_block().to_be_bytes());
            hasher.update(tx_row.transaction_id.as_bytes());
            hasher.update(tx_row.block_hash.as_bytes());
            count += 1;
        }
        match page.next_page_after {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    hasher.update(count.to_be_bytes());
    Ok(count)
}

fn measure_txids_pages(
    secondary: &RocksDbWalletSecondary,
    address: TransparentAddressScriptHash,
    range: BlockHeightRange,
    page_size: NonZeroU16,
) -> Result<u64, BenchError> {
    let mut after = None;
    let mut count = 0_u64;
    loop {
        let page =
            secondary.address_transaction_history_range_page(address, range, after, page_size)?;
        count = count.saturating_add(u64::try_from(page.transactions.len()).unwrap_or(0));
        std::hint::black_box(&page.transactions);
        match page.next_page_after {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    Ok(count)
}

fn hash_utxos_pages(
    secondary: &RocksDbWalletSecondary,
    address: TransparentAddressScriptHash,
    page_size: NonZeroU16,
    hasher: &mut Sha256,
) -> Result<u64, BenchError> {
    let mut after = None;
    let mut count = 0_u64;
    hasher.update(address.as_bytes());
    loop {
        let page = secondary.address_unspent_outputs_page_from_height(
            address,
            BlockHeight::new(1),
            after,
            page_size,
        )?;
        for utxo in &page.outputs {
            hasher.update(utxo.outpoint.transaction_id.as_bytes());
            hasher.update(utxo.outpoint.output_index.to_be_bytes());
            hasher.update(utxo.address_script_hash.as_bytes());
            hasher.update(utxo.value_zat.to_be_bytes());
            hasher.update(
                u32::try_from(utxo.script_pub_key.len())
                    .unwrap_or(0)
                    .to_be_bytes(),
            );
            hasher.update(&utxo.script_pub_key);
            hasher.update(utxo.created_at.transaction_id.as_bytes());
            hasher.update(utxo.created_at.tx_index_in_block.to_be_bytes());
            hasher.update(utxo.created_at.block.height.value().to_be_bytes());
            hasher.update(utxo.created_at.block.hash.as_bytes());
            count += 1;
        }
        match page.next_page_after {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    hasher.update(count.to_be_bytes());
    Ok(count)
}

fn measure_utxos_pages(
    secondary: &RocksDbWalletSecondary,
    address: TransparentAddressScriptHash,
    page_size: NonZeroU16,
) -> Result<u64, BenchError> {
    let mut after = None;
    let mut count = 0_u64;
    loop {
        let page = secondary.address_unspent_outputs_page_from_height(
            address,
            BlockHeight::new(1),
            after,
            page_size,
        )?;
        count = count.saturating_add(u64::try_from(page.outputs.len()).unwrap_or(0));
        std::hint::black_box(&page.outputs);
        match page.next_page_after {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    Ok(count)
}

fn summarize_latencies(mut samples: Vec<Duration>, total_rows_returned: u64) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            calls: 0,
            total_rows_returned,
            mean_us: 0.0,
            p50_us: 0.0,
            p95_us: 0.0,
            p99_us: 0.0,
            max_us: 0.0,
        };
    }
    samples.sort_unstable();
    let calls = samples.len();
    let total_nanos: u128 = samples.iter().map(Duration::as_nanos).sum();
    let mean_us = duration_nanos_to_us(total_nanos) / usize_to_f64(calls);
    let p50_us = percentile_us(&samples, 50, 100);
    let p95_us = percentile_us(&samples, 95, 100);
    let p99_us = percentile_us(&samples, 99, 100);
    let max_us = duration_nanos_to_us(samples[calls - 1].as_nanos());
    LatencyStats {
        calls,
        total_rows_returned,
        mean_us,
        p50_us,
        p95_us,
        p99_us,
        max_us,
    }
}

fn percentile_us(sorted: &[Duration], num: usize, den: usize) -> f64 {
    let last = sorted.len().saturating_sub(1);
    let idx = (last * num) / den;
    duration_nanos_to_us(sorted[idx].as_nanos())
}

#[allow(
    clippy::cast_precision_loss,
    reason = "microsecond latency summary requires floating-point division"
)]
fn duration_nanos_to_us(nanos: u128) -> f64 {
    (nanos as f64) / 1_000.0
}

#[allow(
    clippy::cast_precision_loss,
    reason = "sample count to float for mean latency calculation"
)]
fn usize_to_f64(count: usize) -> f64 {
    count as f64
}

#[allow(
    clippy::cast_precision_loss,
    reason = "compression ratio is a diagnostic scalar"
)]
fn safe_ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64) / (denominator as f64)
    }
}

fn directory_size_bytes(path: &Path) -> Result<u64, BenchError> {
    let mut total = 0_u64;
    let entries = fs::read_dir(path).map_err(|source| BenchError::io(path, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| BenchError::io(path, source))?;
        let entry_path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|source| BenchError::io(&entry_path, source))?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_size_bytes(&entry_path)?);
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

struct ReportInputs<'a> {
    args: &'a RocksDbWalletFootprintArgs,
    chain: &'a GeneratedChain,
    build: &'a RocksDbWalletBuildReport,
    column_families: BTreeMap<String, ColumnFamilyFootprint>,
    warm_queries: WarmQueryBenchmarks,
    parity_digests: ParityDigests,
    physical_wallet_dir_bytes: u64,
}

fn assemble_report(inputs: ReportInputs<'_>) -> WalletFootprintReport {
    let ReportInputs {
        args,
        chain,
        build,
        column_families,
        warm_queries,
        parity_digests,
        physical_wallet_dir_bytes,
    } = inputs;
    let total_sst_bytes: u64 = column_families
        .values()
        .map(|family| family.total_sst_files_bytes)
        .sum();
    let total_logical_bytes: u64 = column_families
        .values()
        .map(|family| family.logical_bytes)
        .sum();
    let total_data_compressed_bytes: u64 = column_families
        .values()
        .map(|family| family.data_compressed_bytes)
        .sum();
    let total_data_uncompressed_bytes: u64 = column_families
        .values()
        .map(|family| family.data_uncompressed_bytes)
        .sum();
    let total_index_bytes: u64 = column_families
        .values()
        .map(|family| family.index_bytes)
        .sum();
    let total_filter_bytes: u64 = column_families
        .values()
        .map(|family| family.filter_bytes)
        .sum();

    let durations = build.phase_durations;
    let wallet_build = WalletBuildSummary {
        tip_height: build.source_position.tip.height.value(),
        settled_height: build.settled_tip.height.value(),
        logical_row_bytes: build.logical_row_bytes,
        sst_file_bytes: build.sst_file_bytes,
        sst_file_count: build.sst_file_count,
        total_build_seconds: durations.total.as_secs_f64(),
        canonical_scan_seconds: durations.canonical_scan.as_secs_f64(),
        outpoint_sort_seconds: durations.outpoint_sort.as_secs_f64(),
        outpoint_merge_seconds: durations.outpoint_merge.as_secs_f64(),
        secondary_row_derivation_seconds: durations.secondary_row_derivation.as_secs_f64(),
        row_load_seconds: durations.row_load.as_secs_f64(),
        flush_and_cold_reopen_seconds: durations.flush_and_cold_reopen.as_secs_f64(),
        cold_validation_seconds: durations.cold_validation.as_secs_f64(),
        cold_validation_random_read_count: build.cold_validation_random_read_count,
    };

    WalletFootprintReport {
        contract_identity: "rocksdb-wallet-footprint",
        report_format_version: REPORT_FORMAT_VERSION,
        fixture_version: FIXTURE_VERSION,
        wallet_rocksdb_schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
        wallet_projection_schema_version: WALLET_PROJECTION_SCHEMA_VERSION,
        blocks: args.blocks,
        outputs_per_block: args.outputs_per_block,
        total_created_outputs: chain.total_created_outputs,
        total_spent_outputs: chain.total_spent_outputs,
        total_unspent_outputs: chain.total_unspent_outputs,
        sampled_address_count: chain.sampled_addresses.len(),
        warm_iterations: args.warm_iterations,
        wallet_build,
        total_sst_bytes,
        total_logical_bytes,
        total_data_compressed_bytes,
        total_data_uncompressed_bytes,
        total_index_bytes,
        total_filter_bytes,
        overall_data_compression_ratio: safe_ratio(
            total_data_uncompressed_bytes,
            total_data_compressed_bytes,
        ),
        overall_sst_compression_ratio: safe_ratio(total_logical_bytes, total_sst_bytes),
        physical_wallet_dir_bytes,
        column_families,
        warm_queries,
        parity_digests,
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionBudgets, RocksDbWalletFootprintArgs, measure_footprint};
    use zinder_store::RocksDbResourceBudget;

    #[test]
    fn small_mainnet_shaped_fixture_measures_per_cf_footprint_and_deterministic_parity()
    -> Result<(), Box<dyn std::error::Error>> {
        let args = RocksDbWalletFootprintArgs {
            blocks: 20,
            outputs_per_block: 10,
            warm_iterations: 2,
            sst_target_bytes: 64 * 1024,
            output_json: None,
            keep_dir: None,
        };
        let budgets = ExecutionBudgets {
            canonical_writer: RocksDbResourceBudget::for_local_tests(),
            wallet_writer: RocksDbResourceBudget::for_local_tests(),
            wallet_reader: RocksDbResourceBudget::for_local_tests(),
            sst_target_logical_bytes: args.sst_target_bytes,
            local_workers_only: true,
        };
        let first = measure_footprint(&args, &budgets)?;
        let second = measure_footprint(&args, &budgets)?;

        assert!(first.total_sst_bytes > 0);
        assert!(first.total_logical_bytes > 0);
        assert!(
            first
                .column_families
                .contains_key("transparent_spent_output")
        );
        assert!(
            first
                .column_families
                .contains_key("transparent_address_transaction")
        );
        assert!(first.column_families.contains_key("reorg_undo"));
        assert!(first.column_families.contains_key("block_hash_by_height"));
        assert_eq!(first.parity_digests, second.parity_digests);
        assert_eq!(
            first.warm_queries.taddress_txids.calls,
            first.sampled_address_count * 2
        );
        Ok(())
    }
}
