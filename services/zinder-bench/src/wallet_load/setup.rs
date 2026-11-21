//! Opt-in setup before the wallets start.
//!
//! It finds the tree-state heights and transaction IDs a `point-reads` run
//! cycles through, and observes what `GetSubtreeRoots` returns. It runs on a
//! connector that records into a throwaway recorder, so setup calls stay out
//! of the run's report, and every call has the deadlines in [`SETUP_LIMITS`],
//! including a total deadline for each stream.
//!
//! The lightwalletd protocol lets a client set `poolTypes` only after it
//! checks that the server can filter by pool, and the Ironwood pool exists
//! only from lightwallet-protocol `v0.5.0`. Setup reads both from the
//! `lightwalletProtocolVersion` the server advertised, and treats a version
//! it cannot parse strictly as one that predates both. Against a server that
//! cannot filter, frontier discovery leaves `poolTypes` empty, whose legacy
//! meaning is the shielded pools it needs, and transaction discovery is
//! refused, because such a server may return shielded data only and the
//! discovered transactions would silently leave out transparent-only ones.
//! Against a server below `v0.5.0`, setup never names Ironwood.

use std::{collections::BTreeSet, ops::ControlFlow, time::Duration};

use serde::Serialize;
use zinder_bench::BenchError;
use zinder_core::wire::{
    decode_internal_block_hash, decode_rpc_transaction_id_hex, encode_internal_transaction_id,
    encode_rpc_block_hash_hex,
};
use zinder_proto::compat::lightwalletd::{
    BlockId, ChainMetadata, ChainSpec, CompactBlock, GetMempoolTxRequest, GetSubtreeRootsArg,
    PoolType, ShieldedProtocol, SubtreeRoot,
};

use super::{
    LightwalletdWalletLoadArgs, WalletPlan,
    calls::{
        self, BlockRangeCall, CallFailure, CallLimits, Connector, LwdClient,
        tree_state_frontier_digest,
    },
};

/// Blocks read per `GetBlockRange` while discovering from the tip downward.
const DISCOVERY_WINDOW_BLOCKS: u64 = 2_000;
/// Deadlines for every setup call. The total stream deadline ends a stream
/// that keeps delivering messages, which the idle limit never would.
const SETUP_LIMITS: CallLimits = CallLimits {
    unary: Duration::from_secs(30),
    stream_open: Duration::from_secs(30),
    stream_idle: Some(Duration::from_secs(30)),
    stream_total: Some(Duration::from_secs(120)),
};
/// Most roots one `GetSubtreeRoots` request retains.
///
/// The protocol caps a pool at 65,536 subtrees, so 2^20 leaves room for a
/// faulty server's repeats while bounding memory at a few dozen MiB.
const MAX_RETAINED_ROOTS: usize = 1 << 20;
/// Bytes in a valid `SubtreeRoot.rootHash`.
const ROOT_HASH_BYTES: usize = 32;
/// Most times the subtree-root check runs while the chain tip keeps moving.
const SUBTREE_ROOTS_ATTEMPTS: u32 = 3;
/// `GetSubtreeRoots` returns roots of subtrees of 2^16 note commitments, so a
/// commitment tree of `size` leaves holds `size >> 16` completed subtrees.
const SUBTREE_LEAF_BITS: u32 = 16;
/// The first lightwallet-protocol release whose requests carry `poolTypes`.
///
/// `v0.4.0` added `poolTypes` to `BlockRange` and `GetMempoolTxRequest`
/// together with `LightdInfo.lightwalletProtocolVersion`, so a server that
/// leaves that field empty predates pool filtering.
const POOL_FILTERING_PROTOCOL_VERSION: (u64, u64, u64) = (0, 4, 0);
/// The first lightwallet-protocol release that names the Ironwood pool in
/// `PoolType` and `ShieldedProtocol`.
const IRONWOOD_PROTOCOL_VERSION: (u64, u64, u64) = (0, 5, 0);

/// A `GetSubtreeRoots` request that failed, and how many roots it delivered
/// before the failure.
#[derive(Debug, Serialize)]
pub(crate) struct SubtreeRootsRequestError {
    error: String,
    roots_before_error: u32,
}

/// What `GetSubtreeRoots` returned for one pool.
///
/// `SubtreeRoot` carries no index, so a root's position in a response that
/// starts at index 0 is the subtree index it stands for, and its `rootHash`
/// identifies it.
#[derive(Debug, Serialize)]
pub(crate) struct PoolSubtreeRoots {
    /// Roots the `max_entries = 0` request delivered, repeats included.
    returned_with_zero_max: u32,
    /// Different 32-byte `rootHash` values among them.
    distinct_roots: u32,
    /// Roots whose `rootHash` is not 32 bytes. They are counted, not kept.
    malformed_root_hashes: u32,
    /// Indexes whose root repeated one at a lower index.
    repeated_root_indexes: Vec<u32>,
    /// Whether a request for one root at index `returned_with_zero_max`
    /// delivered one, or `None` when the first request failed and the
    /// follow-up was not sent.
    root_at_returned_index: Option<bool>,
    /// `size >> 16` for this pool's commitment tree size in the
    /// `chainMetadata` of the block at the tip, or `None` unless
    /// `chain_metadata_block_is_tip`, and also when that block carried no
    /// `chainMetadata`.
    completed_subtrees_at_tip: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    zero_max_error: Option<SubtreeRootsRequestError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    follow_up_error: Option<SubtreeRootsRequestError>,
}

/// The text a failed setup request leaves in a report.
fn failure_text(failure: &CallFailure) -> String {
    format!("{}: {}", failure.label, failure.detail)
}

/// A block's height and hash as the server reported them.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ObservedBlock {
    pub(super) height: u64,
    /// RPC byte order hex for a complete hash, or the bytes as sent for a short hash.
    hash_hex: String,
}

impl ObservedBlock {
    const HASH_BYTES: usize = 32;

    fn from_wire(height: u64, hash: &[u8]) -> Result<Self, CallFailure> {
        if hash.len() > Self::HASH_BYTES {
            return Err(CallFailure {
                label: "invalid_block_hash",
                detail: format!(
                    "block hash exceeds {} bytes (received {})",
                    Self::HASH_BYTES,
                    hash.len()
                ),
            });
        }
        let hash_hex = decode_internal_block_hash(hash)
            .map_or_else(|_| hex::encode(hash), encode_rpc_block_hash_hex);
        Ok(Self { height, hash_hex })
    }

    /// Whether `other` is the same block: equal heights and equal 32-byte
    /// hashes. A missing or malformed hash identifies nothing, so two blocks
    /// that both lack one do not match.
    fn is_same_block_as(&self, other: &Self) -> bool {
        self.height == other.height
            && self.hash_hex.len() == 2 * Self::HASH_BYTES
            && self.hash_hex == other.hash_hex
    }
}

/// What `GetSubtreeRoots(max_entries = 0)` returned for every shielded pool
/// the server's protocol version names, with the chain tip around it.
///
/// It records observations only. The bench README describes how to read
/// them.
#[derive(Debug, Serialize)]
pub(crate) struct SubtreeRootsObservation {
    sapling: PoolSubtreeRoots,
    orchard: PoolSubtreeRoots,
    /// Absent against a server below lightwallet-protocol `v0.5.0`, which
    /// has no Ironwood pool to ask for.
    #[serde(skip_serializing_if = "Option::is_none")]
    ironwood: Option<PoolSubtreeRoots>,
    /// Times the probe ran. A run whose chain tip changed between the
    /// `GetLatestBlock` calls around it is repeated, and only the last run
    /// is reported.
    attempts: u32,
    /// The `GetLatestBlock` answer before the last run's requests.
    tip_before: ObservedBlock,
    /// The `GetLatestBlock` answer after them.
    tip_after: ObservedBlock,
    /// The block `GetBlock` returned for `tip_before`'s height.
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_metadata_block: Option<ObservedBlock>,
    /// Whether `chain_metadata_block` is `tip_before`: the same height and
    /// the same 32-byte hash. `false` also when either hash is missing or
    /// malformed, because the identity cannot be shown. Only a block shown
    /// to be the tip gives `completed_subtrees_at_tip`.
    chain_metadata_block_is_tip: bool,
    /// The status of a `GetBlock` request that failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_metadata_error: Option<String>,
}

/// What a server's advertised `lightwalletProtocolVersion` lets setup ask
/// for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ServedProtocol {
    /// Whether requests may carry `poolTypes`, from `v0.4.0`.
    pool_filtering: bool,
    /// Whether requests may name the Ironwood pool, from `v0.5.0`.
    ironwood: bool,
}

impl ServedProtocol {
    fn from_advertised(advertised: &str) -> Self {
        let version = ProtocolVersion::parse(advertised);
        let reaches = |release| version.is_some_and(|version| version.reaches(release));
        Self {
            pool_filtering: reaches(POOL_FILTERING_PROTOCOL_VERSION),
            ironwood: reaches(IRONWOOD_PROTOCOL_VERSION),
        }
    }

    /// The `poolTypes` that read every frontier-advancing output. Empty when
    /// the server cannot filter, which it reads as its shielded pools.
    fn frontier_pools(self) -> &'static [PoolType] {
        match (self.pool_filtering, self.ironwood) {
            (false, _) => &[],
            (true, false) => &[PoolType::Sapling, PoolType::Orchard],
            (true, true) => &[PoolType::Sapling, PoolType::Orchard, PoolType::Ironwood],
        }
    }

    /// The `poolTypes` that select every pool a transaction can touch, so
    /// discovery also finds transparent-only transactions.
    fn transaction_pools(self) -> &'static [PoolType] {
        if self.ironwood {
            &[
                PoolType::Transparent,
                PoolType::Sapling,
                PoolType::Orchard,
                PoolType::Ironwood,
            ]
        } else {
            &[PoolType::Transparent, PoolType::Sapling, PoolType::Orchard]
        }
    }
}

/// A `vMAJOR.MINOR.PATCH` semantic version, as lightwallet-protocol tags
/// its releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProtocolVersion {
    release: (u64, u64, u64),
    /// Whether a pre-release suffix such as `-rc.1` follows the release,
    /// which orders the version below that release.
    pre_release: bool,
}

impl ProtocolVersion {
    /// Parses `vMAJOR.MINOR.PATCH` with an optional semantic-version
    /// pre-release and build suffix, refusing anything else, so a version
    /// such as `v0.4.0garbage` or `v0.5.1-rc.01` never enables a feature.
    /// Numeric pre-release identifiers may not have a leading zero, and
    /// build metadata identifiers may, as semantic versioning 2.0.0 states.
    fn parse(advertised: &str) -> Option<Self> {
        let version = advertised.trim().strip_prefix('v')?;
        let (version, build) = match version.split_once('+') {
            Some((version, build)) => (version, Some(build)),
            None => (version, None),
        };
        let (core, pre_release) = match version.split_once('-') {
            Some((core, pre_release)) => (core, Some(pre_release)),
            None => (version, None),
        };
        let pre_release_valid = pre_release.is_none_or(|pre_release| {
            valid_identifiers(pre_release) && !pre_release.split('.').any(numeric_with_leading_zero)
        });
        if !pre_release_valid || !build.is_none_or(valid_identifiers) {
            return None;
        }
        let mut numbers = core.split('.').map(numeric_identifier);
        let release = (numbers.next()??, numbers.next()??, numbers.next()??);
        if numbers.next().is_some() {
            return None;
        }
        Some(Self {
            release,
            pre_release: pre_release.is_some(),
        })
    }

    /// Whether this version is `release` or a later one.
    fn reaches(self, release: (u64, u64, u64)) -> bool {
        self.release > release || (self.release == release && !self.pre_release)
    }
}

/// A semantic-version numeric identifier: ASCII digits without a leading
/// zero.
fn numeric_identifier(identifier: &str) -> Option<u64> {
    let digits = !identifier.is_empty() && identifier.bytes().all(|byte| byte.is_ascii_digit());
    if !digits || (identifier.len() > 1 && identifier.starts_with('0')) {
        return None;
    }
    identifier.parse().ok()
}

/// Whether `identifier` is all ASCII digits with a leading zero, which a
/// pre-release identifier may not be but build metadata may.
fn numeric_with_leading_zero(identifier: &str) -> bool {
    identifier.len() > 1
        && identifier.starts_with('0')
        && identifier.bytes().all(|byte| byte.is_ascii_digit())
}

/// Whether `identifiers` are dot-separated, non-empty runs of ASCII
/// alphanumerics and hyphens, as a semantic-version suffix must be.
fn valid_identifiers(identifiers: &str) -> bool {
    identifiers.split('.').all(|identifier| {
        !identifier.is_empty()
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

/// What setup found for the workload settings.
pub(super) struct SetupOutcome {
    pub(super) distinct_frontier_heights: Vec<u64>,
    pub(super) mined_txids: Vec<Vec<u8>>,
    pub(super) pending_txids: Vec<Vec<u8>>,
    pub(super) subtree_roots_verification: Option<SubtreeRootsObservation>,
}

/// Runs the setup the arguments ask for. `connector` must not record into the
/// run's recorder. `advertised_protocol_version` is the server's
/// `GetLightdInfo.lightwalletProtocolVersion`.
pub(super) async fn prepare(
    args: &LightwalletdWalletLoadArgs,
    connector: &Connector,
    plan: &WalletPlan,
    advertised_protocol_version: &str,
) -> Result<SetupOutcome, BenchError> {
    let mut mined_txids = decode_rpc_txids("--mined-txid", &args.mined_txids)?;
    let mut pending_txids = decode_rpc_txids("--pending-txid", &args.pending_txids)?;
    let served = ServedProtocol::from_advertised(advertised_protocol_version);
    if !served.pool_filtering {
        refuse_transaction_discovery(args, advertised_protocol_version)?;
    }
    let subtree_roots_verification = if args.verify_subtree_roots {
        Some(verify_subtree_roots(connector, served).await?)
    } else {
        None
    };
    let distinct_frontier_heights = if args.distinct_frontiers > 0 {
        discover_distinct_frontier_heights(
            connector,
            plan,
            args.distinct_frontiers,
            served.frontier_pools(),
        )
        .await?
    } else {
        Vec::new()
    };
    if args.discover_mined_txids > 0 {
        let pools = served.transaction_pools();
        mined_txids
            .extend(discover_mined_txids(connector, plan, args.discover_mined_txids, pools).await?);
    }
    if args.discover_mempool_txids > 0 {
        let pools = served.transaction_pools();
        pending_txids
            .extend(discover_mempool_txids(connector, args.discover_mempool_txids, pools).await?);
    }
    dedup_in_order(&mut mined_txids);
    dedup_in_order(&mut pending_txids);
    Ok(SetupOutcome {
        distinct_frontier_heights,
        mined_txids,
        pending_txids,
        subtree_roots_verification,
    })
}

/// Drops every repeat of an earlier transaction ID, keeping the first
/// occurrence's position, so a transaction both named and discovered is read
/// and counted once.
fn dedup_in_order(txids: &mut Vec<Vec<u8>>) {
    let mut seen = BTreeSet::new();
    txids.retain(|txid| seen.insert(txid.clone()));
}

/// Refuses transaction discovery against a server that cannot filter by
/// pool, because it would miss transparent-only transactions.
fn refuse_transaction_discovery(
    args: &LightwalletdWalletLoadArgs,
    advertised_protocol_version: &str,
) -> Result<(), BenchError> {
    let discovery = [
        (
            "--discover-mined-txids",
            "--mined-txid",
            args.discover_mined_txids,
        ),
        (
            "--discover-mempool-txids",
            "--pending-txid",
            args.discover_mempool_txids,
        ),
    ];
    match discovery.iter().find(|(_, _, target)| *target > 0) {
        Some((flag, replacement, _)) => Err(BenchError::invalid_argument(format!(
            "{flag} needs a server that filters by poolTypes, so transparent-only \
             transactions are found too, but the server advertises \
             lightwalletProtocolVersion `{advertised_protocol_version}`, which is not \
             v0.4.0 or later; name transactions with {replacement} instead"
        ))),
        None => Ok(()),
    }
}

/// Decodes transaction IDs given as RPC byte order hex, the form block
/// explorers and `zcash-cli` show, into the internal byte order the
/// lightwalletd protocol's `TxFilter.hash` carries.
fn decode_rpc_txids(flag: &str, hex_txids: &[String]) -> Result<Vec<Vec<u8>>, BenchError> {
    hex_txids
        .iter()
        .map(|hex_txid| {
            decode_rpc_transaction_id_hex(hex_txid)
                .map(|transaction_id| encode_internal_transaction_id(transaction_id).to_vec())
                .map_err(|error| {
                    BenchError::invalid_argument(format!(
                        "{flag} `{hex_txid}` must be 64 hex characters in RPC byte order: {error}"
                    ))
                })
        })
        .collect()
}

/// Observes what `GetSubtreeRoots(max_entries = 0)` returns for each pool.
///
/// A server can stop at a page limit without an error, so the probe also
/// asks for the root at the index after the returned ones, and it repeats
/// while the chain tip changes under it, because a new block can complete a
/// subtree.
async fn verify_subtree_roots(
    connector: &Connector,
    served: ServedProtocol,
) -> Result<SubtreeRootsObservation, BenchError> {
    let mut client = connect_setup(connector).await?;
    let mut attempts = 0;
    loop {
        attempts += 1;
        let observation = observe_subtree_roots(connector, &mut client, served, attempts).await?;
        if observation.tip_before == observation.tip_after || attempts >= SUBTREE_ROOTS_ATTEMPTS {
            return Ok(observation);
        }
    }
}

/// One run of the probe: the tip, the tip's compact block, each pool's
/// roots, and the tip again.
///
/// A failed `GetBlock`, such as `NOT_FOUND` after a rollback, is recorded
/// rather than returned, so the tip read after it decides whether the probe
/// runs again.
async fn observe_subtree_roots(
    connector: &Connector,
    client: &mut LwdClient,
    served: ServedProtocol,
    attempts: u32,
) -> Result<SubtreeRootsObservation, BenchError> {
    let tip_before = chain_tip(connector, client)
        .await
        .map_err(|failure| setup_failure(&failure))?;
    let tip_block = calls::unary(
        connector.recorder(),
        "GetBlock",
        SETUP_LIMITS.unary,
        client.get_block(BlockId {
            height: tip_before.height,
            hash: Vec::new(),
        }),
    )
    .await
    .and_then(|block| {
        ObservedBlock::from_wire(block.height, &block.hash)
            .map(|observed| (observed, block.chain_metadata))
    });
    let (chain_metadata_block, chain_metadata_error, sizes) = match tip_block {
        Ok((observed, metadata)) => {
            let sizes = observed
                .is_same_block_as(&tip_before)
                .then_some(metadata)
                .flatten();
            (Some(observed), None, sizes)
        }
        Err(failure) => (None, Some(failure_text(&failure)), None),
    };
    let completed = |size: fn(&ChainMetadata) -> u32| {
        sizes.as_ref().map(|sizes| size(sizes) >> SUBTREE_LEAF_BITS)
    };
    let sapling = completed(|sizes| sizes.sapling_commitment_tree_size);
    let orchard = completed(|sizes| sizes.orchard_commitment_tree_size);
    let ironwood = completed(|sizes| sizes.ironwood_commitment_tree_size);
    let sapling = observe_pool(connector, client, ShieldedProtocol::Sapling, sapling).await;
    let orchard = observe_pool(connector, client, ShieldedProtocol::Orchard, orchard).await;
    let ironwood = if served.ironwood {
        Some(observe_pool(connector, client, ShieldedProtocol::Ironwood, ironwood).await)
    } else {
        None
    };
    let tip_after = chain_tip(connector, client)
        .await
        .map_err(|failure| setup_failure(&failure))?;
    let chain_metadata_block_is_tip = chain_metadata_block
        .as_ref()
        .is_some_and(|block| block.is_same_block_as(&tip_before));
    Ok(SubtreeRootsObservation {
        sapling,
        orchard,
        ironwood,
        attempts,
        tip_before,
        tip_after,
        chain_metadata_block,
        chain_metadata_block_is_tip,
        chain_metadata_error,
    })
}

/// The chain tip's height and hash.
pub(super) async fn chain_tip(
    connector: &Connector,
    client: &mut LwdClient,
) -> Result<ObservedBlock, CallFailure> {
    let tip = calls::unary(
        connector.recorder(),
        "GetLatestBlock",
        SETUP_LIMITS.unary,
        client.get_latest_block(ChainSpec {}),
    )
    .await?;
    ObservedBlock::from_wire(tip.height, &tip.hash)
}

/// Asks for every root of `protocol` with `max_entries` of 0, then, unless
/// that request failed, for one root at the index after the returned ones.
///
/// The second request runs even when no root was returned, so an empty
/// response that stopped early shows too.
async fn observe_pool(
    connector: &Connector,
    client: &mut LwdClient,
    protocol: ShieldedProtocol,
    completed_subtrees_at_tip: Option<u32>,
) -> PoolSubtreeRoots {
    let request = |start_index, max_entries| GetSubtreeRootsArg {
        start_index,
        shielded_protocol: protocol as i32,
        max_entries,
    };
    let first = read_subtree_roots(
        connector,
        client,
        &SETUP_LIMITS,
        MAX_RETAINED_ROOTS,
        request(0, 0),
    )
    .await;
    let (root_at_returned_index, follow_up_error) = if first.error.is_some() {
        (None, None)
    } else {
        let follow_up = read_subtree_roots(
            connector,
            client,
            &SETUP_LIMITS,
            MAX_RETAINED_ROOTS,
            request(first.delivered, 1),
        )
        .await;
        (Some(follow_up.delivered > 0), follow_up.error)
    };
    PoolSubtreeRoots {
        returned_with_zero_max: first.delivered,
        distinct_roots: u32::try_from(first.seen.len()).unwrap_or(u32::MAX),
        malformed_root_hashes: first.malformed,
        repeated_root_indexes: first.repeated_indexes,
        root_at_returned_index,
        completed_subtrees_at_tip,
        zero_max_error: first.error,
        follow_up_error,
    }
}

/// What one `GetSubtreeRoots` request delivered.
///
/// Roots are tallied as they arrive. Only each distinct 32-byte `rootHash`
/// and each repeat position is kept, and together they never exceed
/// `budget`, so a faulty stream cannot grow memory with its length. A root
/// of another length is counted, not kept. A request that fails mid-stream,
/// runs past its total deadline, or reaches the budget still reports the
/// roots before it beside its error.
struct RootTally {
    delivered: u32,
    malformed: u32,
    seen: BTreeSet<Vec<u8>>,
    repeated_indexes: Vec<u32>,
    budget: usize,
    error: Option<SubtreeRootsRequestError>,
}

impl RootTally {
    fn new(budget: usize) -> Self {
        Self {
            delivered: 0,
            malformed: 0,
            seen: BTreeSet::new(),
            repeated_indexes: Vec::new(),
            budget,
            error: None,
        }
    }

    /// Tallies one root, and breaks, recording a resource-limit error, when
    /// keeping it would exceed the budget.
    fn record(&mut self, root_hash: Vec<u8>) -> ControlFlow<()> {
        if root_hash.len() != ROOT_HASH_BYTES {
            self.malformed = self.malformed.saturating_add(1);
        } else if self.seen.contains(&root_hash) {
            if self.retained() >= self.budget {
                return self.exhaust_budget();
            }
            self.repeated_indexes.push(self.delivered);
        } else {
            if self.retained() >= self.budget {
                return self.exhaust_budget();
            }
            self.seen.insert(root_hash);
        }
        self.delivered = self.delivered.saturating_add(1);
        ControlFlow::Continue(())
    }

    fn retained(&self) -> usize {
        self.seen.len() + self.repeated_indexes.len()
    }

    fn exhaust_budget(&mut self) -> ControlFlow<()> {
        self.error = Some(SubtreeRootsRequestError {
            error: format!(
                "resource_limit: more than {} distinct roots and repeats",
                self.budget
            ),
            roots_before_error: self.delivered,
        });
        ControlFlow::Break(())
    }
}

/// Reads one `GetSubtreeRoots` request within `limits`, retaining at most
/// `budget` distinct roots and repeat positions.
async fn read_subtree_roots(
    connector: &Connector,
    client: &mut LwdClient,
    limits: &CallLimits,
    budget: usize,
    arg: GetSubtreeRootsArg,
) -> RootTally {
    let mut tally = RootTally::new(budget);
    let outcome = calls::stream_each(
        connector.recorder(),
        "GetSubtreeRoots",
        limits,
        client.get_subtree_roots(arg),
        |root: SubtreeRoot| tally.record(root.root_hash),
    )
    .await;
    if tally.error.is_none() {
        tally.error = outcome.err().map(|failure| SubtreeRootsRequestError {
            error: failure_text(&failure),
            roots_before_error: tally.delivered,
        });
    }
    tally
}

/// Windows of `plan`'s range as inclusive `(start, end)` heights, from the
/// tip down, so discovery finds tip-era values first.
fn windows_newest_first(plan: &WalletPlan) -> impl Iterator<Item = (u64, u64)> {
    let from_height = plan.from_height;
    let mut next_end = Some(plan.to_height);
    std::iter::from_fn(move || {
        let end = next_end?;
        let start = end
            .saturating_sub(DISCOVERY_WINDOW_BLOCKS - 1)
            .max(from_height);
        next_end = (start > from_height).then(|| start - 1);
        Some((start, end))
    })
}

/// Reads blocks `start..=end` with the parts of `pools` into `on_block`.
async fn read_window(
    connector: &Connector,
    client: &mut LwdClient,
    (start, end): (u64, u64),
    pools: &[PoolType],
    mut on_block: impl FnMut(&CompactBlock),
) -> Result<(), BenchError> {
    calls::get_block_range(
        connector.recorder(),
        client,
        &SETUP_LIMITS,
        BlockRangeCall {
            start,
            end,
            pool_types: pool_types(pools),
        },
        |block, _| {
            on_block(block);
            std::future::ready(())
        },
    )
    .await
    .map_err(|failure| setup_failure(&failure))
}

fn pool_types(pools: &[PoolType]) -> Vec<i32> {
    pools.iter().map(|pool| *pool as i32).collect()
}

/// Finds up to `target` heights, newest first, whose `GetTreeState` frontiers
/// differ, reading blocks with `pools`.
///
/// Blocks without shielded outputs repeat their predecessor's frontier, so
/// the candidates are the blocks with shielded outputs plus the range's first
/// height, whose frontier holds every output below the range.
async fn discover_distinct_frontier_heights(
    connector: &Connector,
    plan: &WalletPlan,
    target: u32,
    pools: &[PoolType],
) -> Result<Vec<u64>, BenchError> {
    let target = usize::try_from(target).unwrap_or(usize::MAX);
    let mut client = connect_setup(connector).await?;
    let mut seen_digests = BTreeSet::new();
    let mut heights = Vec::new();
    for window in windows_newest_first(plan) {
        let mut candidates = Vec::new();
        read_window(connector, &mut client, window, pools, |block| {
            if block.vtx.iter().any(|tx| {
                !tx.outputs.is_empty() || !tx.actions.is_empty() || !tx.ironwood_actions.is_empty()
            }) {
                candidates.push(block.height);
            }
        })
        .await?;
        let (start, end) = window;
        if start == plan.from_height && candidates.first() != Some(&start) {
            candidates.insert(0, start);
        }
        if candidates.is_empty() {
            candidates.push(end);
        }
        for height in candidates.into_iter().rev() {
            let tree_state = calls::unary(
                connector.recorder(),
                "GetTreeState",
                SETUP_LIMITS.unary,
                client.get_tree_state(BlockId {
                    height,
                    hash: Vec::new(),
                }),
            )
            .await
            .map_err(|failure| setup_failure(&failure))?;
            if seen_digests.insert(tree_state_frontier_digest(&tree_state)) {
                heights.push(height);
                if heights.len() >= target {
                    return Ok(heights);
                }
            }
        }
    }
    Ok(heights)
}

/// Finds up to `target` distinct mined txids, newest first, from `pools`.
async fn discover_mined_txids(
    connector: &Connector,
    plan: &WalletPlan,
    target: u32,
    pools: &[PoolType],
) -> Result<Vec<Vec<u8>>, BenchError> {
    let target = usize::try_from(target).unwrap_or(usize::MAX);
    let mut client = connect_setup(connector).await?;
    let mut seen = BTreeSet::new();
    let mut txids = Vec::new();
    for window in windows_newest_first(plan) {
        let mut window_txids = Vec::new();
        read_window(connector, &mut client, window, pools, |block| {
            window_txids.extend(block.vtx.iter().map(|tx| tx.txid.clone()));
        })
        .await?;
        for txid in window_txids.into_iter().rev() {
            if !txid.is_empty() && seen.insert(txid.clone()) {
                txids.push(txid);
                if txids.len() >= target {
                    return Ok(txids);
                }
            }
        }
    }
    Ok(txids)
}

/// Finds up to `target` distinct pending txids from `GetMempoolTx`, across
/// `pools`.
async fn discover_mempool_txids(
    connector: &Connector,
    target: u32,
    pools: &[PoolType],
) -> Result<Vec<Vec<u8>>, BenchError> {
    let target = usize::try_from(target).unwrap_or(usize::MAX);
    let mut client = connect_setup(connector).await?;
    let mut seen = BTreeSet::new();
    let mut txids = Vec::new();
    calls::stream_each(
        connector.recorder(),
        "GetMempoolTx",
        &SETUP_LIMITS,
        client.get_mempool_tx(GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: pool_types(pools),
        }),
        |tx| {
            if !tx.txid.is_empty() && seen.insert(tx.txid.clone()) {
                txids.push(tx.txid);
            }
            if txids.len() >= target {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
    )
    .await
    .map_err(|failure| setup_failure(&failure))?;
    Ok(txids)
}

async fn connect_setup(connector: &Connector) -> Result<LwdClient, BenchError> {
    connector
        .connect()
        .await
        .map_err(|failure| setup_failure(&failure))
}

fn setup_failure(failure: &CallFailure) -> BenchError {
    BenchError::wallet_load_endpoint(format!(
        "setup {} failed: {}",
        failure.label, failure.detail
    ))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use zinder_proto::compat::lightwalletd::{GetSubtreeRootsArg, PoolType};

    use super::{
        CallLimits, MAX_RETAINED_ROOTS, ObservedBlock, RootTally, SETUP_LIMITS, ServedProtocol,
        WalletPlan, decode_rpc_txids, dedup_in_order, discover_mempool_txids, read_subtree_roots,
        windows_newest_first,
    };
    use crate::wallet_load::{
        calls::{CallRecorder, Connector},
        mock_streamer::{self, MockStreamer, RangeEnding},
        transport::TlsOptions,
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
    fn windows_cover_the_range_from_the_tip_down_without_overlap() {
        let windows = windows_newest_first(&plan(10, 4_500)).collect::<Vec<_>>();

        assert_eq!(windows, [(2_501, 4_500), (501, 2_500), (10, 500)]);
        assert_eq!(
            windows_newest_first(&plan(7, 7)).collect::<Vec<_>>(),
            [(7, 7)]
        );
    }

    #[test]
    fn pool_filtering_starts_at_protocol_v0_4_0_and_ironwood_at_v0_5_0() {
        let served = |pool_filtering, ironwood| ServedProtocol {
            pool_filtering,
            ironwood,
        };
        for (advertised, expected) in [
            ("v0.4.0", served(true, false)),
            ("v0.4.1", served(true, false)),
            (" v0.4.12 ", served(true, false)),
            ("v0.5.0-rc.1", served(true, false)),
            ("v0.5.0", served(true, true)),
            ("v0.6.0+build.7", served(true, true)),
            ("v1.0.0-rc.1", served(true, true)),
            ("v0.4.0-rc.1", served(false, false)),
            ("v0.3.9", served(false, false)),
            ("v0.5.0-rc.0", served(true, false)),
            ("v0.5.0-rc.0a1", served(true, false)),
            ("v0.5.0+build.01", served(true, true)),
            ("v0.5.0-rc.1+007", served(true, false)),
        ] {
            assert_eq!(
                ServedProtocol::from_advertised(advertised),
                expected,
                "{advertised}"
            );
        }
    }

    #[test]
    fn a_malformed_protocol_version_enables_nothing() {
        for malformed in [
            "",
            "unknown",
            "0.6.0",
            "v0.6",
            "v0.6.0.1",
            "v0.4.0garbage",
            "v0.5.0garbage",
            "v0.4.0 garbage",
            "v0.4.0-",
            "v0.4.0-rc..1",
            "v0.4.0+",
            "v0.4.0-rc_1",
            "v0.+4.0",
            "v0.04.0",
            "v0.4.-0",
            "v0.5.1-rc.01",
            "v0.5.0-01",
            "v0.5.0-rc.1.00+build",
            "v0.5.0+build..1",
        ] {
            assert_eq!(
                ServedProtocol::from_advertised(malformed),
                ServedProtocol {
                    pool_filtering: false,
                    ironwood: false
                },
                "{malformed}"
            );
        }
    }

    #[test]
    fn discovery_names_ironwood_only_for_servers_that_know_it() {
        let v0_4 = ServedProtocol::from_advertised("v0.4.1");
        let v0_5 = ServedProtocol::from_advertised("v0.5.0");
        let legacy = ServedProtocol::from_advertised("");

        assert_eq!(
            v0_4.frontier_pools(),
            [PoolType::Sapling, PoolType::Orchard]
        );
        assert_eq!(
            v0_4.transaction_pools(),
            [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard]
        );
        assert!(v0_5.frontier_pools().contains(&PoolType::Ironwood));
        assert!(v0_5.transaction_pools().contains(&PoolType::Ironwood));
        assert_eq!(legacy.frontier_pools(), []);
    }

    #[test]
    fn merged_txids_keep_the_first_of_each_in_order() {
        let mut txids = vec![vec![3], vec![1], vec![3], vec![2], vec![1]];

        dedup_in_order(&mut txids);

        assert_eq!(txids, [vec![3], vec![1], vec![2]]);
    }

    #[test]
    fn observed_hashes_use_rpc_hex_and_short_hashes_stay_as_sent() -> Result<(), super::CallFailure>
    {
        let mut internal = vec![0; 32];
        internal[0] = 0xab;

        assert_eq!(
            ObservedBlock::from_wire(7, &internal)?.hash_hex,
            format!("{}ab", "00".repeat(31))
        );
        assert_eq!(ObservedBlock::from_wire(7, &[1, 2])?.hash_hex, "0102");
        assert_eq!(ObservedBlock::from_wire(7, &[])?.hash_hex, "");
        Ok(())
    }

    #[test]
    fn a_block_matches_only_with_equal_heights_and_equal_32_byte_hashes()
    -> Result<(), super::CallFailure> {
        let block = ObservedBlock::from_wire(7, &[1; 32])?;
        assert!(block.is_same_block_as(&block));
        assert!(!block.is_same_block_as(&ObservedBlock::from_wire(8, &[1; 32])?));
        assert!(!block.is_same_block_as(&ObservedBlock::from_wire(7, &[2; 32])?));
        Ok(())
    }

    #[test]
    fn blocks_without_a_usable_hash_never_match() -> Result<(), super::CallFailure> {
        let missing = ObservedBlock::from_wire(7, &[])?;
        let short = ObservedBlock::from_wire(7, &[1, 2])?;
        assert!(!missing.is_same_block_as(&missing));
        assert!(!short.is_same_block_as(&short));
        assert!(!missing.is_same_block_as(&ObservedBlock::from_wire(7, &[1; 32])?));
        Ok(())
    }

    #[test]
    fn a_tally_keeps_each_distinct_root_once_and_names_repeat_positions() {
        let mut tally = RootTally::new(MAX_RETAINED_ROOTS);
        for root in [1, 2, 1, 1] {
            assert!(tally.record(vec![root; 32]).is_continue());
        }

        assert_eq!(tally.delivered, 4);
        assert_eq!(tally.seen.len(), 2);
        assert_eq!(tally.repeated_indexes, [2, 3]);
    }

    #[tokio::test]
    async fn a_root_stream_that_never_ends_stops_at_its_deadline_with_the_roots_before_it()
    -> Result<(), Box<dyn Error>> {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.subtree_root_endless = true;
        let server = mock_streamer::serve(streamer).await?;
        let connector = Connector::new(
            &server.url,
            Arc::new(CallRecorder::default()),
            &TlsOptions::default(),
        )?;
        let mut client = connector
            .connect()
            .await
            .map_err(|failure| failure.detail)?;
        let limits = CallLimits {
            stream_total: Some(Duration::from_millis(300)),
            ..SETUP_LIMITS
        };

        let tally = read_subtree_roots(
            &connector,
            &mut client,
            &limits,
            MAX_RETAINED_ROOTS,
            GetSubtreeRootsArg {
                start_index: 0,
                shielded_protocol: 0,
                max_entries: 0,
            },
        )
        .await;

        let error = tally
            .error
            .ok_or("the endless stream must hit its deadline")?;
        assert!(
            error.error.starts_with("client_timeout: stream"),
            "{}",
            error.error
        );
        assert!(tally.delivered > 0);
        assert_eq!(error.roots_before_error, tally.delivered);
        assert_eq!(tally.seen.len(), usize::try_from(tally.delivered)?);
        Ok(())
    }

    #[test]
    fn a_tally_counts_roots_that_are_not_32_bytes_without_keeping_them() {
        let mut tally = RootTally::new(MAX_RETAINED_ROOTS);
        for root in [vec![1; 31], vec![2; 32], vec![], vec![1; 33], vec![2; 32]] {
            assert!(tally.record(root).is_continue());
        }

        assert_eq!(tally.delivered, 5);
        assert_eq!(tally.malformed, 3);
        assert_eq!(tally.seen.len(), 1);
        assert_eq!(tally.repeated_indexes, [4]);
        assert!(tally.error.is_none());
    }

    #[test]
    fn a_tally_stops_with_a_resource_limit_error_when_its_budget_is_spent()
    -> Result<(), Box<dyn Error>> {
        let mut tally = RootTally::new(3);
        assert!(tally.record(vec![1; 32]).is_continue());
        assert!(tally.record(vec![1; 32]).is_continue());
        assert!(tally.record(vec![2; 32]).is_continue());
        assert!(tally.record(vec![1; 32]).is_break());

        let error = tally
            .error
            .as_ref()
            .ok_or("the exhausted budget must be reported")?;
        assert!(
            error.error.starts_with("resource_limit:"),
            "{}",
            error.error
        );
        assert_eq!(error.roots_before_error, 3);
        assert_eq!(tally.delivered, 3);
        assert_eq!(tally.retained(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn an_endless_root_stream_stops_at_the_budget_with_a_resource_limit_error()
    -> Result<(), Box<dyn Error>> {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.subtree_root_endless = true;
        let server = mock_streamer::serve(streamer).await?;
        let connector = Connector::new(
            &server.url,
            Arc::new(CallRecorder::default()),
            &TlsOptions::default(),
        )?;
        let mut client = connector
            .connect()
            .await
            .map_err(|failure| failure.detail)?;

        let tally = tokio::time::timeout(
            Duration::from_secs(20),
            read_subtree_roots(
                &connector,
                &mut client,
                &SETUP_LIMITS,
                8,
                GetSubtreeRootsArg {
                    start_index: 0,
                    shielded_protocol: 0,
                    max_entries: 0,
                },
            ),
        )
        .await?;

        let error = tally.error.ok_or("the budget must end the stream")?;
        assert!(
            error.error.starts_with("resource_limit:"),
            "{}",
            error.error
        );
        assert_eq!(tally.delivered, 8);
        assert_eq!(error.roots_before_error, 8);
        assert_eq!(tally.seen.len(), 8);
        Ok(())
    }

    #[test]
    fn setup_streams_have_a_total_deadline() {
        assert!(SETUP_LIMITS.stream_total.is_some());
    }

    #[tokio::test]
    async fn mempool_discovery_stops_at_its_target_while_the_stream_keeps_producing()
    -> Result<(), Box<dyn Error>> {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.mempool_endless = true;
        let server = mock_streamer::serve(streamer).await?;
        let connector = Connector::new(
            &server.url,
            Arc::new(CallRecorder::default()),
            &TlsOptions::default(),
        )?;

        let txids = tokio::time::timeout(
            Duration::from_secs(20),
            discover_mempool_txids(&connector, 3, &[]),
        )
        .await??;

        assert_eq!(txids.len(), 3);
        Ok(())
    }

    #[test]
    fn txid_flags_take_rpc_byte_order_and_send_internal_byte_order() {
        let rpc_hex = format!("ab01{}", "00".repeat(30));
        let mut internal = vec![0; 32];
        internal[31] = 0xab;
        internal[30] = 0x01;

        assert_eq!(
            decode_rpc_txids("--mined-txid", std::slice::from_ref(&rpc_hex)).ok(),
            Some(vec![internal])
        );
    }

    #[test]
    fn txid_flags_refuse_a_prefix_a_wrong_length_and_bad_hex() {
        for refused in [
            format!("0x{}", "ab".repeat(32)),
            "ab01".to_owned(),
            "zz".repeat(32),
            String::new(),
        ] {
            let error = decode_rpc_txids("--mined-txid", std::slice::from_ref(&refused)).err();

            assert!(
                error.is_some_and(|error| error.to_string().contains("--mined-txid")),
                "{refused}"
            );
        }
    }
}
