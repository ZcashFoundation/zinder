//! Point reads a wallet makes besides block downloads.
//!
//! Each simulated wallet repeats one round: `GetLightdInfo` and
//! `GetLatestBlock`, as wallets do to pick and follow a server; a tree state
//! at a stored construction checkpoint and one a block below it, which
//! Zinder rebuilds by replaying the 99 blocks above the checkpoint below; and,
//! for each `--transparent-address`, `GetTaddressTxids` over the range and
//! one `GetAddressUtxosStream` for all of them. Opt-in settings add tree
//! states at distinct frontiers, `GetTransaction` for mined, pending, and
//! missing transaction IDs, and further transparent-address reads. Every call
//! is recorded once, without retries, so its client-observed status is the
//! server's answer.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde::Serialize;
use zinder_proto::compat::lightwalletd::{
    AddressList, BlockId, BlockRange, ChainSpec, Empty, GetAddressUtxosArg,
    TransparentAddressBlockFilter,
};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

use super::{
    WalletPlan, WorkloadSettings,
    calls::{self, CallFailure, CallLimits, CallRecorder, Connector, LwdClient, TransactionRead},
};

const LIMITS: CallLimits = CallLimits {
    unary: Duration::from_secs(30),
    stream_open: Duration::from_secs(30),
    stream_idle: Some(Duration::from_secs(30)),
    stream_total: None,
};

/// Settings a point-reads run used.
#[derive(Debug, Serialize)]
pub(super) struct Parameters {
    rounds: u32,
    stored_checkpoint_height: Option<u64>,
    replay_height: Option<u64>,
    transparent_addresses: usize,
    distinct_frontier_heights: Vec<u64>,
    mined_txids: usize,
    pending_txids: usize,
    missing_tx_checks: u32,
    include_taddress_transactions: bool,
    include_taddress_balance: bool,
    include_address_utxos_unary: bool,
    limits: CallLimits,
}

impl Parameters {
    pub(super) fn new(settings: &WorkloadSettings, plan: &WalletPlan) -> Self {
        let heights = TreeStateHeights::within(plan);
        Self {
            rounds: settings.rounds,
            stored_checkpoint_height: heights.map(|heights| heights.stored_checkpoint),
            replay_height: heights.map(|heights| heights.replay),
            transparent_addresses: settings.transparent_addresses.len(),
            distinct_frontier_heights: settings.distinct_frontier_heights.clone(),
            mined_txids: settings.mined_txids.len(),
            pending_txids: settings.pending_txids.len(),
            missing_tx_checks: settings.missing_tx_checks,
            include_taddress_transactions: settings.include_taddress_transactions,
            include_taddress_balance: settings.include_taddress_balance,
            include_address_utxos_unary: settings.include_address_utxos_unary,
            limits: LIMITS,
        }
    }
}

/// A construction checkpoint inside the range, and the height one below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreeStateHeights {
    stored_checkpoint: u64,
    replay: u64,
}

impl TreeStateHeights {
    /// The highest checkpoint at least one stride below the range end, so
    /// it was built by construction rather than by following the tip.
    fn within(plan: &WalletPlan) -> Option<Self> {
        let stride = u64::from(TREE_STATE_CHECKPOINT_STRIDE);
        let stored_checkpoint = (plan.to_height / stride).checked_sub(1)? * stride;
        let replay = stored_checkpoint.checked_sub(1)?;
        (replay >= plan.from_height).then_some(Self {
            stored_checkpoint,
            replay,
        })
    }
}

/// A deterministic 32-byte transaction ID no chain contains, so each probe
/// asks for a transaction the server must report as not found.
fn synthetic_missing_txid(wallet_index: u32, round_index: u32, check_index: u32) -> Vec<u8> {
    let mut txid = vec![0xEE; 32];
    txid[..4].copy_from_slice(&wallet_index.to_le_bytes());
    txid[4..8].copy_from_slice(&round_index.to_le_bytes());
    txid[8..12].copy_from_slice(&check_index.to_le_bytes());
    txid
}

/// The next entry each cycled point-read list hands out, shared by every
/// wallet of a run.
///
/// A wallet takes an entry at the moment it sends the read, so the run visits
/// every entry once before any entry repeats, however the wallets' starts
/// and rounds interleave.
#[derive(Debug, Default)]
pub(crate) struct ReadRotation {
    distinct_frontier: AtomicU64,
    mined_txid: AtomicU64,
    pending_txid: AtomicU64,
}

/// Takes the next entry of `entries`, wrapping. `None` when `entries` is
/// empty.
fn take_next<'list, Entry>(cursor: &AtomicU64, entries: &'list [Entry]) -> Option<&'list Entry> {
    if entries.is_empty() {
        return None;
    }
    cycle(entries, cursor.fetch_add(1, Ordering::Relaxed))
}

/// The entry at `position`, wrapping. `None` when `entries` is empty.
fn cycle<Entry>(entries: &[Entry], position: u64) -> Option<&Entry> {
    let len = u64::try_from(entries.len()).ok()?;
    entries.get(usize::try_from(position.checked_rem(len)?).ok()?)
}

struct PointReadContext<'run> {
    recorder: &'run CallRecorder,
    plan: &'run WalletPlan,
    settings: &'run WorkloadSettings,
    heights: Option<TreeStateHeights>,
    wallet_index: u32,
}

/// Runs `settings.rounds` rounds of point reads on one connection.
pub(super) async fn sync_wallet(
    connector: Connector,
    plan: &WalletPlan,
    settings: &WorkloadSettings,
    wallet_index: u32,
) -> Result<(), CallFailure> {
    let context = PointReadContext {
        recorder: connector.recorder(),
        plan,
        settings,
        heights: TreeStateHeights::within(plan),
        wallet_index,
    };
    let mut client = connector.connect().await?;
    for round_index in 0..settings.rounds {
        run_round(&context, &mut client, round_index).await;
    }
    Ok(())
}

async fn run_round(ctx: &PointReadContext<'_>, client: &mut LwdClient, round_index: u32) {
    let rotation = &ctx.settings.read_rotation;
    // A failed point read is recorded with its status and the round
    // continues, so every call contributes one sample.
    let _ = calls::unary(
        ctx.recorder,
        "GetLightdInfo",
        LIMITS.unary,
        client.get_lightd_info(Empty {}),
    )
    .await;
    let _ = calls::unary(
        ctx.recorder,
        "GetLatestBlock",
        LIMITS.unary,
        client.get_latest_block(ChainSpec {}),
    )
    .await;
    if let Some(heights) = ctx.heights {
        for (method, height) in [
            ("GetTreeState(stored checkpoint)", heights.stored_checkpoint),
            ("GetTreeState(replay)", heights.replay),
        ] {
            let _ = calls::get_tree_state_as(ctx.recorder, method, client, &LIMITS, height).await;
        }
    }
    if let Some(&height) = take_next(
        &rotation.distinct_frontier,
        &ctx.settings.distinct_frontier_heights,
    ) {
        let _ = calls::get_tree_state_as(
            ctx.recorder,
            "GetTreeState(distinct frontier)",
            client,
            &LIMITS,
            height,
        )
        .await;
    }
    run_transaction_reads(ctx, client, round_index).await;
    run_transparent_address_reads(ctx, client).await;
}

/// `GetTransaction` for one mined and one pending transaction ID, cycled
/// across wallets and rounds, and for each requested missing one.
async fn run_transaction_reads(ctx: &PointReadContext<'_>, client: &LwdClient, round_index: u32) {
    let settings = ctx.settings;
    let rotation = &settings.read_rotation;
    let known = [
        (
            TransactionRead::Mined,
            &rotation.mined_txid,
            &settings.mined_txids,
        ),
        (
            TransactionRead::Pending,
            &rotation.pending_txid,
            &settings.pending_txids,
        ),
    ];
    for (read, cursor, txids) in known {
        if let Some(txid) = take_next(cursor, txids) {
            let _ =
                calls::read_transaction(ctx.recorder, client.clone(), LIMITS, read, txid.clone())
                    .await;
        }
    }
    for check_index in 0..settings.missing_tx_checks {
        let txid = synthetic_missing_txid(ctx.wallet_index, round_index, check_index);
        let _ = calls::read_transaction(
            ctx.recorder,
            client.clone(),
            LIMITS,
            TransactionRead::Missing,
            txid,
        )
        .await;
    }
}

/// Transparent-address history, balance, and unspent outputs over the range.
async fn run_transparent_address_reads(ctx: &PointReadContext<'_>, client: &mut LwdClient) {
    let (recorder, plan, settings) = (ctx.recorder, ctx.plan, ctx.settings);
    if settings.transparent_addresses.is_empty() {
        return;
    }
    for address in &settings.transparent_addresses {
        let filter = TransparentAddressBlockFilter {
            address: address.clone(),
            range: Some(BlockRange {
                start: Some(BlockId {
                    height: plan.from_height,
                    hash: Vec::new(),
                }),
                end: Some(BlockId {
                    height: plan.to_height,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            }),
        };
        let _ = calls::drain_stream(
            recorder,
            "GetTaddressTxids",
            &LIMITS,
            client.get_taddress_txids(filter.clone()),
        )
        .await;
        if settings.include_taddress_transactions {
            let _ = calls::drain_stream(
                recorder,
                "GetTaddressTransactions",
                &LIMITS,
                client.get_taddress_transactions(filter),
            )
            .await;
        }
    }
    let utxos = GetAddressUtxosArg {
        addresses: settings.transparent_addresses.clone(),
        start_height: plan.from_height,
        max_entries: 0,
    };
    if settings.include_taddress_balance {
        let _ = calls::unary(
            recorder,
            "GetTaddressBalance",
            LIMITS.unary,
            client.get_taddress_balance(AddressList {
                addresses: settings.transparent_addresses.clone(),
            }),
        )
        .await;
    }
    if settings.include_address_utxos_unary {
        let _ = calls::unary(
            recorder,
            "GetAddressUtxos",
            LIMITS.unary,
            client.get_address_utxos(utxos.clone()),
        )
        .await;
    }
    let _ = calls::drain_stream(
        recorder,
        "GetAddressUtxosStream",
        &LIMITS,
        client.get_address_utxos_stream(utxos),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::{
        ReadRotation, TreeStateHeights, WalletPlan, cycle, synthetic_missing_txid, take_next,
    };

    fn plan(from_height: u64, to_height: u64) -> WalletPlan {
        WalletPlan {
            from_height,
            to_height,
            mainnet: false,
            sapling_activation_height: 1,
        }
    }

    #[test]
    fn tree_state_heights_sit_on_a_construction_checkpoint_and_one_below() {
        assert_eq!(
            TreeStateHeights::within(&plan(37, 7_400)),
            Some(TreeStateHeights {
                stored_checkpoint: 7_300,
                replay: 7_299,
            })
        );
        assert_eq!(TreeStateHeights::within(&plan(37, 199)), None);
        assert_eq!(TreeStateHeights::within(&plan(250, 300)), None);
    }

    #[test]
    fn cycling_wraps_and_skips_empty_lists() {
        assert_eq!(cycle(&[10, 20, 30], 4), Some(&20));
        assert_eq!(cycle::<u64>(&[], 4), None);
    }

    #[test]
    fn interleaved_takers_read_every_entry_before_any_repeats() {
        // Wallet 0 runs all 3 of its rounds before wallet 1 starts, as a
        // staggered start allows.
        let rotation = ReadRotation::default();
        let frontiers = [13_u64, 12, 11, 10];
        let order = (0..6)
            .filter_map(|_| take_next(&rotation.distinct_frontier, &frontiers).copied())
            .collect::<Vec<_>>();

        assert_eq!(order, [13, 12, 11, 10, 13, 12]);
    }

    #[test]
    fn each_list_rotates_on_its_own_cursor() {
        let rotation = ReadRotation::default();
        let frontiers = [1_u64, 2];
        let mined = [10_u64, 20, 30];

        assert_eq!(take_next(&rotation.distinct_frontier, &frontiers), Some(&1));
        assert_eq!(take_next(&rotation.mined_txid, &mined), Some(&10));
        assert_eq!(take_next(&rotation.mined_txid, &mined), Some(&20));
        assert_eq!(take_next(&rotation.distinct_frontier, &frontiers), Some(&2));
        assert_eq!(take_next::<u64>(&rotation.pending_txid, &[]), None);
        assert_eq!(take_next(&rotation.mined_txid, &mined), Some(&30));
    }

    #[test]
    fn missing_txids_differ_by_wallet_round_and_check() {
        let txids = [
            synthetic_missing_txid(0, 0, 0),
            synthetic_missing_txid(1, 0, 0),
            synthetic_missing_txid(0, 1, 0),
            synthetic_missing_txid(0, 0, 1),
        ];

        assert!(txids.iter().all(|txid| txid.len() == 32));
        for (index, txid) in txids.iter().enumerate() {
            assert!(!txids[..index].contains(txid));
        }
    }
}
