//! Point reads a wallet makes besides block downloads.
//!
//! Each simulated wallet repeats one round: `GetLightdInfo` and
//! `GetLatestBlock`, as wallets do to pick and follow a server; a tree state
//! at a stored construction checkpoint and one a block below it, which
//! Zinder rebuilds by replaying the 99 blocks above the checkpoint below; and,
//! for each `--transparent-address`, `GetTaddressTxids` over the range and
//! one `GetAddressUtxosStream` for all of them. Every call is recorded once,
//! without retries, so its client-observed status is the server's answer.

use std::time::Duration;

use serde::Serialize;
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, ChainSpec, Empty, GetAddressUtxosArg, TransparentAddressBlockFilter,
};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

use super::{
    WalletPlan, WorkloadSettings,
    calls::{self, CallFailure, CallLimits, Connector},
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

/// Runs `settings.rounds` rounds of point reads on one connection.
pub(super) async fn sync_wallet(
    connector: Connector,
    plan: &WalletPlan,
    settings: &WorkloadSettings,
) -> Result<(), CallFailure> {
    let recorder = connector.recorder();
    let mut client = connector.connect().await?;
    let heights = TreeStateHeights::within(plan);
    for _ in 0..settings.rounds {
        // A failed point read is recorded with its status and the round
        // continues, so every call contributes one sample.
        let _ = calls::unary(
            recorder,
            "GetLightdInfo",
            LIMITS.unary,
            client.get_lightd_info(Empty {}),
        )
        .await;
        let _ = calls::unary(
            recorder,
            "GetLatestBlock",
            LIMITS.unary,
            client.get_latest_block(ChainSpec {}),
        )
        .await;
        if let Some(heights) = heights {
            let _ = calls::get_tree_state_as(
                recorder,
                "GetTreeState(stored checkpoint)",
                &mut client,
                &LIMITS,
                heights.stored_checkpoint,
            )
            .await;
            let _ = calls::get_tree_state_as(
                recorder,
                "GetTreeState(replay)",
                &mut client,
                &LIMITS,
                heights.replay,
            )
            .await;
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
                client.get_taddress_txids(filter),
            )
            .await;
        }
        if !settings.transparent_addresses.is_empty() {
            let _ = calls::drain_stream(
                recorder,
                "GetAddressUtxosStream",
                &LIMITS,
                client.get_address_utxos_stream(GetAddressUtxosArg {
                    addresses: settings.transparent_addresses.clone(),
                    start_height: plan.from_height,
                    max_entries: 0,
                }),
            )
            .await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TreeStateHeights, WalletPlan};

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
}
