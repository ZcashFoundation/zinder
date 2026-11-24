//! Wallet query implementation over admitted canonical and wallet stores.

use std::{
    collections::HashSet,
    future::ready,
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::Instrument as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalHistoryBounds, ChainEpoch, ChainEpochId, ChainValuePoolsAtTip, CompactBlockArtifact,
    MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST, MinedTransaction, MinedTransactionChainContext,
    NetworkUpgradeActivations, RawTransactionBytes, StandardFee, TransactionId,
    TransparentAddressBalance, TransparentAddressScriptHash, TransparentAddressTxIndexArtifact,
    TransparentOutPoint, TransparentOutpointSpendStatus, TxStatus,
};
use zinder_proto::capabilities::{
    WALLET_BROADCAST_TRANSACTION_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
    WALLET_READ_TRANSACTION_BY_ID_V2,
};
use zinder_source::TransactionBroadcaster;
use zinder_store::{
    ArtifactFamily, BlockHashLookup, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, HeightOrder, RawBlobRetention, ServedTreeState, StreamCursorTokenV1,
};
use zinder_wallet_projection::{
    WalletAddressTransactionKey, WalletCanonicalSourceIdentity, WalletProjectionSourcePosition,
};

use crate::{
    AdmittedIngestControl, ArtifactKey, BlockHeaderAtEpoch, BlockIdAtEpoch,
    COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY, COMPACT_BLOCK_STREAM_SUB_READ_BYTES, ChainEvents,
    CompactBlock, CompactBlockRange, CompactBlockStream, DEFAULT_MAX_COMPACT_BLOCK_RANGE,
    DEFAULT_MAX_FULL_BLOCK_RANGE, FULL_BLOCK_STREAM_CHANNEL_CAPACITY,
    FULL_BLOCK_STREAM_SUB_READ_BLOCKS, FullBlock, FullBlockStream,
    NativeWalletEndpointCapabilities, OrderedBroadcaster, QueryError, RawTransaction,
    ServedTipSnapshot, SettledTipBlock, SpendStatusReadEnd, SubtreeRoots, Transaction,
    TransactionStatus, TransparentAddressTxIds, TransparentAddressTxIdsInRangeRequest,
    TransparentAddressUnspentOutputs, TransparentAddressUnspentOutputsRequest,
    TransparentOutpointSpendStatuses, TreeState, TreeStateFrontiers, UpstreamNodeCapabilities,
    VisibleTipBlock, WalletQueryApi, WalletServingPairSlot, WalletServingReadPair,
    broadcast_byte_budget::BroadcastByteBudget,
    grpc::CompactBlockStreamSlot,
    node_broadcast::EndpointVerifications,
    serving_read,
    tree_state_replay_limit::{
        DEFAULT_TREE_STATE_REPLAY_WAIT, ReaderGeneration, ReplayKey, Selection,
        TreeStateReplayLimit, read_missing_height,
    },
    validate_block_range, validate_subtree_root_range,
};

const TRANSPARENT_HISTORY_CURSOR_MAGIC: [u8; 4] = *b"zth1";
const TRANSPARENT_HISTORY_CURSOR_LEN: usize = 174;

/// Exact continuation state for one native transparent-history page.
///
/// The cursor carries the immutable wallet source that issued it. A publisher
/// can replace the serving pair between client page requests, so resuming with
/// only the ordered row key could otherwise combine histories from two forks.
#[derive(Clone, Copy)]
struct TransparentHistoryCursor {
    source: WalletCanonicalSourceIdentity,
    after: WalletAddressTransactionKey,
}

impl TransparentHistoryCursor {
    fn issue(
        source: WalletCanonicalSourceIdentity,
        after: WalletAddressTransactionKey,
    ) -> StreamCursorTokenV1 {
        let source_position = source.source_position();
        let sequence_digest = source.source_sequence_digest();
        let settled_tip = source.settled_tip();
        let mut encoded = Vec::with_capacity(TRANSPARENT_HISTORY_CURSOR_LEN);
        encoded.extend_from_slice(&TRANSPARENT_HISTORY_CURSOR_MAGIC);
        encoded.extend_from_slice(&source_position.chain_epoch_id.value().to_be_bytes());
        append_block_id(&mut encoded, source_position.tip);
        encoded.extend_from_slice(&source_position.event_sequence.to_be_bytes());
        encoded.extend_from_slice(&sequence_digest.version().value().to_be_bytes());
        encoded.extend_from_slice(&sequence_digest.block_count().to_be_bytes());
        encoded.extend_from_slice(&sequence_digest.as_bytes());
        append_block_id(&mut encoded, settled_tip);
        encoded.extend_from_slice(after.as_bytes());
        StreamCursorTokenV1::from_bytes(encoded)
    }

    fn parse(cursor: &StreamCursorTokenV1) -> Result<Self, QueryError> {
        Self::decode(cursor.as_bytes())
    }

    /// Chain epoch of the pair that issued this cursor.
    fn chain_epoch_id(self) -> ChainEpochId {
        self.source.source_position().chain_epoch_id
    }

    /// Returns the key to resume after when `expected_source` issued this
    /// cursor.
    fn resume_on(
        self,
        expected_source: WalletCanonicalSourceIdentity,
    ) -> Result<WalletAddressTransactionKey, QueryError> {
        if self.source != expected_source {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: self.chain_epoch_id(),
            });
        }
        Ok(self.after)
    }

    fn decode(encoded: &[u8]) -> Result<Self, QueryError> {
        if encoded.len() != TRANSPARENT_HISTORY_CURSOR_LEN
            || !encoded.starts_with(&TRANSPARENT_HISTORY_CURSOR_MAGIC)
        {
            return Err(invalid_transparent_history_cursor());
        }

        let chain_epoch_id = ChainEpochId::new(u64::from_be_bytes(cursor_bytes(encoded, 4..12)?));
        let tip = decode_block_id(encoded, 12)?;
        let event_sequence = u64::from_be_bytes(cursor_bytes(encoded, 48..56)?);
        let digest_version = u16::from_be_bytes(cursor_bytes(encoded, 56..58)?);
        let digest_version = CanonicalBlockFactsSequenceDigestVersion::try_from(digest_version)
            .map_err(|_| invalid_transparent_history_cursor())?;
        let digest_block_count = u64::from_be_bytes(cursor_bytes(encoded, 58..66)?);
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            digest_version,
            digest_block_count,
            cursor_bytes(encoded, 66..98)?,
        );
        let settled_tip = decode_block_id(encoded, 98)?;
        if chain_epoch_id.value() == 0
            || event_sequence == 0
            || digest.block_count() == 0
            || !settled_tip_is_within_source(tip, settled_tip)
        {
            return Err(invalid_transparent_history_cursor());
        }
        let after = WalletAddressTransactionKey::decode(&encoded[134..])
            .map_err(|_| invalid_transparent_history_cursor())?;
        Ok(Self {
            source: WalletCanonicalSourceIdentity::new(
                WalletProjectionSourcePosition::new(chain_epoch_id, tip, event_sequence),
                digest,
                settled_tip,
            ),
            after,
        })
    }
}

fn append_block_id(encoded: &mut Vec<u8>, block_id: BlockId) {
    encoded.extend_from_slice(&block_id.height.value().to_be_bytes());
    encoded.extend_from_slice(&block_id.hash.as_bytes());
}

fn decode_block_id(encoded: &[u8], start: usize) -> Result<BlockId, QueryError> {
    let height = BlockHeight::new(u32::from_be_bytes(cursor_bytes(encoded, start..start + 4)?));
    let hash = BlockHash::from_bytes(cursor_bytes(encoded, start + 4..start + 36)?);
    Ok(BlockId::new(height, hash))
}

fn settled_tip_is_within_source(source_tip: BlockId, settled_tip: BlockId) -> bool {
    settled_tip.height < source_tip.height
        || (settled_tip.height == source_tip.height && settled_tip.hash == source_tip.hash)
}

fn cursor_bytes<const N: usize>(
    encoded: &[u8],
    range: std::ops::Range<usize>,
) -> Result<[u8; N], QueryError> {
    encoded
        .get(range)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(invalid_transparent_history_cursor)
}

fn invalid_transparent_history_cursor() -> QueryError {
    QueryError::TransparentHistoryCursorInvalid {
        reason: "cursor is not a native transparent-history continuation",
    }
}

fn validate_transparent_history_cursor_key(
    key: WalletAddressTransactionKey,
    request: &TransparentAddressTxIdsInRangeRequest,
) -> Result<(), QueryError> {
    if key.address_script_hash() != request.address_script_hash {
        return Err(QueryError::TransparentHistoryCursorInvalid {
            reason: "cursor address does not match request address",
        });
    }
    let height = key.block_height();
    if height < request.start_height || height > request.end_height {
        return Err(QueryError::TransparentHistoryCursorInvalid {
            reason: "cursor height is outside request range",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, TransparentAddressScriptHash,
    };
    use zinder_wallet_projection::{
        WalletAddressTransactionKey, WalletCanonicalSourceIdentity, WalletProjectionSourcePosition,
    };

    use super::{
        QueryError, TransparentAddressTxIdsInRangeRequest, TransparentHistoryCursor,
        validate_transparent_history_cursor_key,
    };

    #[test]
    fn transparent_history_continuation_rejects_pair_replacement_and_preserves_current_paging()
    -> Result<(), Box<dyn Error>> {
        let first_page_source = source_identity(1, 0x11);
        let replacement_pair_source = source_identity(1, 0x22);
        let address = TransparentAddressScriptHash::from_bytes([0xa5; 32]);
        let first_page_after = WalletAddressTransactionKey::new(address, BlockHeight::new(12), 0);
        let second_page_after = WalletAddressTransactionKey::new(address, BlockHeight::new(13), 0);
        let first_page_cursor =
            TransparentHistoryCursor::issue(first_page_source, first_page_after);

        // The pair remains current while the client reads its second page.
        assert_eq!(
            resume_cursor(&first_page_cursor, first_page_source)?,
            first_page_after
        );
        let second_page_cursor =
            TransparentHistoryCursor::issue(first_page_source, second_page_after);
        assert_eq!(
            resume_cursor(&second_page_cursor, first_page_source)?,
            second_page_after
        );

        // A pair replacement between pages must never resume its raw key on
        // the replacement fork.
        let Err(error) = resume_cursor(&first_page_cursor, replacement_pair_source) else {
            return Err("replacement pair unexpectedly accepted prior-page cursor".into());
        };
        assert!(matches!(
            error,
            QueryError::ChainEpochPinUnavailable {
                chain_epoch_id
            } if chain_epoch_id == ChainEpochId::new(1)
        ));

        Ok(())
    }

    #[test]
    fn malformed_transparent_history_continuation_remains_invalid_input()
    -> Result<(), Box<dyn Error>> {
        let source = source_identity(1, 0x11);
        let malformed = zinder_store::StreamCursorTokenV1::from_bytes(vec![0; 40]);
        let Err(error) = resume_cursor(&malformed, source) else {
            return Err("malformed transparent-history cursor unexpectedly resumed".into());
        };
        assert!(matches!(
            error,
            QueryError::TransparentHistoryCursorInvalid { .. }
        ));
        Ok(())
    }

    #[test]
    fn structurally_impossible_transparent_history_source_is_invalid_input()
    -> Result<(), Box<dyn Error>> {
        let source = source_identity(1, 0x11);
        let after = WalletAddressTransactionKey::new(
            TransparentAddressScriptHash::from_bytes([0xa5; 32]),
            BlockHeight::new(12),
            0,
        );
        let cursor = TransparentHistoryCursor::issue(source, after);
        let mut zero_event_sequence = cursor.as_bytes().to_vec();
        let Some(event_sequence) = zero_event_sequence.get_mut(48..56) else {
            return Err("transparent-history cursor did not contain an event sequence".into());
        };
        event_sequence.fill(0);
        let zero_event_sequence =
            zinder_store::StreamCursorTokenV1::from_bytes(zero_event_sequence);

        let Err(error) = resume_cursor(&zero_event_sequence, source) else {
            return Err("zero event sequence unexpectedly resumed".into());
        };
        assert!(matches!(
            error,
            QueryError::TransparentHistoryCursorInvalid { .. }
        ));

        let mut zero_digest_block_count = cursor.as_bytes().to_vec();
        let Some(digest_block_count) = zero_digest_block_count.get_mut(58..66) else {
            return Err("transparent-history cursor did not contain a digest block count".into());
        };
        digest_block_count.fill(0);
        let zero_digest_block_count =
            zinder_store::StreamCursorTokenV1::from_bytes(zero_digest_block_count);
        let Err(error) = resume_cursor(&zero_digest_block_count, source) else {
            return Err("zero digest block count unexpectedly resumed".into());
        };
        assert!(matches!(
            error,
            QueryError::TransparentHistoryCursorInvalid { .. }
        ));

        Ok(())
    }

    #[test]
    fn transparent_history_continuation_must_match_its_request_scope() -> Result<(), Box<dyn Error>>
    {
        let request_address = TransparentAddressScriptHash::from_bytes([0xa5; 32]);
        let request = TransparentAddressTxIdsInRangeRequest {
            address_script_hash: request_address,
            start_height: BlockHeight::new(10),
            end_height: BlockHeight::new(20),
            max_entries: std::num::NonZeroU32::MIN,
            from_cursor: None,
        };
        let other_address = WalletAddressTransactionKey::new(
            TransparentAddressScriptHash::from_bytes([0xa6; 32]),
            BlockHeight::new(12),
            0,
        );
        let Err(error) = validate_transparent_history_cursor_key(other_address, &request) else {
            return Err("address-mismatched cursor unexpectedly validated".into());
        };
        assert!(matches!(
            error,
            QueryError::TransparentHistoryCursorInvalid {
                reason: "cursor address does not match request address"
            }
        ));

        let outside_range =
            WalletAddressTransactionKey::new(request_address, BlockHeight::new(21), 0);
        let Err(error) = validate_transparent_history_cursor_key(outside_range, &request) else {
            return Err("range-mismatched cursor unexpectedly validated".into());
        };
        assert!(matches!(
            error,
            QueryError::TransparentHistoryCursorInvalid {
                reason: "cursor height is outside request range"
            }
        ));

        Ok(())
    }

    fn resume_cursor(
        cursor: &zinder_store::StreamCursorTokenV1,
        expected_source: WalletCanonicalSourceIdentity,
    ) -> Result<WalletAddressTransactionKey, QueryError> {
        TransparentHistoryCursor::parse(cursor)?.resume_on(expected_source)
    }

    fn source_identity(chain_epoch: u64, hash_byte: u8) -> WalletCanonicalSourceIdentity {
        let tip = BlockId::new(BlockHeight::new(13), BlockHash::from_bytes([hash_byte; 32]));
        WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(ChainEpochId::new(chain_epoch), tip, 9),
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                13,
                [hash_byte; 32],
            ),
            tip,
        )
    }
}

/// Exact-fence wallet query over canonical and wallet-projection stores.
///
/// An epoch pin succeeds when it names the current pair's epoch or the epoch
/// of a pair the [`WalletServingPairSlot`] still retains. This composition
/// reports [`QueryError::ChainEpochPinUnavailable`] for every other epoch.
///
/// Every method captures its pair on the async side and then runs the whole
/// store read on the blocking pool. The secondaries use direct I/O, so a cache
/// miss is a synchronous `pread`; keeping one on a reactor thread would stall
/// the ops listener and the gRPC connection drivers sharing that runtime.
///
/// A tree state at a height without a stored checkpoint is reconstructed from
/// stored compact blocks. The query and its clones run at most one such
/// reconstruction per available CPU at once. A reconstruction that finds
/// every slot taken waits for one, by default up to
/// [`DEFAULT_TREE_STATE_REPLAY_WAIT`](crate::DEFAULT_TREE_STATE_REPLAY_WAIT)
/// alongside at most
/// [`DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT`](crate::DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT)
/// other requests per slot, and is otherwise refused with
/// [`QueryError::TreeStateReplayLimitReached`].
#[derive(Clone)]
pub struct WalletServingQuery<Broadcaster, NativeIngest = ()> {
    serving_pair_slot: WalletServingPairSlot,
    broadcaster: Broadcaster,
    native_ingest: NativeIngest,
    tree_state_replays: TreeStateReplayLimit,
    compact_block_stream_sub_read_bytes: usize,
    broadcast_bytes: BroadcastByteBudget,
    native_endpoint_capabilities: NativeWalletEndpointCapabilities,
    node_verifications: Option<EndpointVerifications>,
    cold_transactions: Option<Arc<crate::cold_transaction::ColdTransactionSource>>,
}

impl<Broadcaster, NativeIngest> std::fmt::Debug for WalletServingQuery<Broadcaster, NativeIngest> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pair = self.capture_pair();
        formatter
            .debug_struct("WalletServingQuery")
            .field("canonical_fence", &pair.canonical_fence())
            .field("wallet_fence", &pair.wallet_source())
            .field(
                "native_endpoint_capabilities",
                &self.native_endpoint_capabilities,
            )
            .finish_non_exhaustive()
    }
}

impl WalletServingQuery<()> {
    /// Builds a storage-only query over a swappable slot of already-admitted
    /// immutable pairs.
    ///
    /// Every [`WalletQueryApi`] method captures one `Arc` from this slot before
    /// reading. A publisher can therefore atomically replace the pair without
    /// changing the canonical or wallet reader observed by an in-flight request.
    /// The query advertises no transaction broadcast until
    /// [`WalletServingQuery::with_node_broadcast`] installs a node endpoint.
    #[must_use]
    pub fn from_serving_pair_slot(serving_pair_slot: WalletServingPairSlot) -> Self {
        let raw_blob_retention = serving_pair_slot
            .capture_ephemeral()
            .canonical()
            .raw_blob_retention();
        Self {
            native_endpoint_capabilities: NativeWalletEndpointCapabilities::for_wallet_serving_pair(
                raw_blob_retention,
            ),
            serving_pair_slot,
            broadcaster: (),
            native_ingest: (),
            tree_state_replays: TreeStateReplayLimit::for_available_parallelism(
                DEFAULT_TREE_STATE_REPLAY_WAIT,
            ),
            compact_block_stream_sub_read_bytes: COMPACT_BLOCK_STREAM_SUB_READ_BYTES,
            broadcast_bytes: BroadcastByteBudget::process(),
            node_verifications: None,
            cold_transactions: None,
        }
    }
}

impl WalletServingQuery<(), AdmittedIngestControl> {
    /// Builds the storage-only native query from an exact pair and admitted
    /// ingest control.
    ///
    /// The native instantiation is the only exact-pair query accepted by the
    /// standard native gRPC adapter. It advertises no transaction broadcast
    /// until [`WalletServingQuery::with_node_broadcast`] installs a node
    /// endpoint.
    #[must_use]
    pub fn from_admitted_native_serving_pair(
        serving_pair_slot: WalletServingPairSlot,
        ingest_control: AdmittedIngestControl,
    ) -> Self {
        let raw_blob_retention = serving_pair_slot
            .capture_ephemeral()
            .canonical()
            .raw_blob_retention();
        Self {
            native_endpoint_capabilities:
                NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
                    raw_blob_retention,
                    &ingest_control,
                ),
            serving_pair_slot,
            broadcaster: (),
            native_ingest: ingest_control,
            tree_state_replays: TreeStateReplayLimit::for_available_parallelism(
                DEFAULT_TREE_STATE_REPLAY_WAIT,
            ),
            compact_block_stream_sub_read_bytes: COMPACT_BLOCK_STREAM_SUB_READ_BYTES,
            broadcast_bytes: BroadcastByteBudget::process(),
            node_verifications: None,
            cold_transactions: None,
        }
    }
}

impl<Broadcaster> WalletServingQuery<Broadcaster, AdmittedIngestControl> {
    pub(crate) fn admitted_native_ingest(&self) -> &AdmittedIngestControl {
        &self.native_ingest
    }
}

impl<NativeIngest> WalletServingQuery<(), NativeIngest> {
    /// Broadcasts transactions through `nodes`, the operator's ordered node
    /// endpoints, or keeps the query storage-only when `nodes` is `None`.
    ///
    /// With nodes, the query advertises transaction broadcast from now on,
    /// for the life of the process, without contacting any node. A broadcast
    /// fails with [`QueryError::Node`] carrying
    /// [`zinder_source::SourceError::NodeUnavailable`] until an endpoint
    /// passes [`OrderedBroadcaster::verify`], and only verified endpoints
    /// receive it. `ServerInfo` reports the capabilities of the first
    /// verified endpoint in broadcast preference order.
    #[must_use]
    pub fn with_node_broadcast<Source>(
        self,
        nodes: Option<OrderedBroadcaster<Source>>,
    ) -> WalletServingQuery<Option<OrderedBroadcaster<Source>>, NativeIngest> {
        let native_endpoint_capabilities = if nodes.is_some() {
            self.native_endpoint_capabilities.with_node_broadcast()
        } else {
            self.native_endpoint_capabilities
        };
        WalletServingQuery {
            serving_pair_slot: self.serving_pair_slot,
            node_verifications: nodes.as_ref().map(OrderedBroadcaster::verifications),
            broadcaster: nodes,
            cold_transactions: self.cold_transactions,
            native_ingest: self.native_ingest,
            tree_state_replays: self.tree_state_replays,
            compact_block_stream_sub_read_bytes: self.compact_block_stream_sub_read_bytes,
            broadcast_bytes: self.broadcast_bytes,
            native_endpoint_capabilities,
        }
    }
}

impl<NativeIngest>
    WalletServingQuery<Option<OrderedBroadcaster<zinder_source::ZebraJsonRpcSource>>, NativeIngest>
{
    /// Enables verified mined transaction byte reads from the configured primary node
    /// when the admitted canonical store has no transaction blobs.
    #[must_use]
    pub fn with_verified_cold_transactions(mut self) -> Self {
        if !self
            .admitted_raw_blob_retention()
            .retains_transaction_blobs()
            && let Some(source) = self
                .broadcaster
                .as_ref()
                .and_then(OrderedBroadcaster::primary_source)
        {
            self.cold_transactions = Some(Arc::new(
                crate::cold_transaction::ColdTransactionSource::new(source.clone()),
            ));
            self.native_endpoint_capabilities = self
                .native_endpoint_capabilities
                .with_cold_transaction_bytes();
        }
        self
    }
    /// Monitors the query's broadcast nodes and, when configured, the primary
    /// node that supplies cold transaction bytes. Node outages degrade readiness
    /// while local reads remain available.
    #[must_use]
    pub fn spawn_node_readiness_probe(
        &self,
        readiness: crate::WalletServingReadiness,
        poll_interval: std::time::Duration,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        use crate::wallet_serving_pair_publisher::{WalletNodeProbe, spawn_wallet_node_probe};

        let probe = self.broadcaster.as_ref().map(|nodes| {
            WalletNodeProbe::new(
                nodes.clone(),
                self.serving_pair_slot.clone(),
                poll_interval,
                self.has_verified_cold_tx_source(),
            )
        });
        spawn_wallet_node_probe(probe, readiness, cancel)
    }
}

impl<Broadcaster, NativeIngest> WalletServingQuery<Broadcaster, NativeIngest> {
    /// Returns whether this query can verify mined bytes fetched from its configured node.
    #[must_use]
    pub const fn has_verified_cold_tx_source(&self) -> bool {
        self.cold_transactions.is_some()
    }

    /// Lets a tree-state reconstruction that finds every replay slot taken
    /// wait up to `wait` for one before it is refused.
    ///
    /// The wait is capped at
    /// [`MAX_TREE_STATE_REPLAY_WAIT`](crate::MAX_TREE_STATE_REPLAY_WAIT),
    /// because the reconstruction that follows it must still fit the client's
    /// deadline. `Duration::ZERO` refuses at once. The query and every clone
    /// made after this call share the new wait and the existing slots.
    #[must_use]
    pub fn with_tree_state_replay_wait(self, wait: std::time::Duration) -> Self {
        Self {
            tree_state_replays: self.tree_state_replays.with_wait(wait),
            ..self
        }
    }

    /// Lets at most `waiters_per_slot` tree-state reconstructions wait for
    /// each replay slot; a reconstruction that finds every waiting place
    /// taken is refused at once.
    ///
    /// The count is capped at
    /// [`MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT`](crate::MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT)
    /// and defaults to
    /// [`DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT`](crate::DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT).
    /// The query and every clone made after this call share the new count,
    /// the existing slots, and the requests already waiting.
    #[must_use]
    pub fn with_tree_state_replay_waiters_per_slot(self, waiters_per_slot: NonZeroU32) -> Self {
        Self {
            tree_state_replays: self
                .tree_state_replays
                .with_waiters_per_slot(waiters_per_slot),
            ..self
        }
    }

    /// Captures the current pair for a read that exposes no chain epoch.
    fn capture_pair(&self) -> Arc<WalletServingReadPair> {
        self.serving_pair_slot.capture_ephemeral()
    }

    /// Selects the pair a read pinned to `at_epoch_id` captures, or the
    /// current pair when the read is unpinned.
    fn pin(&self, at_epoch_id: Option<ChainEpochId>) -> serving_read::SlotPin<'_> {
        serving_read::SlotPin::new(&self.serving_pair_slot, at_epoch_id)
    }

    /// Returns the canonical raw-blob retention authenticated by this query's
    /// currently admitted serving pair.
    ///
    /// Pair replacement is private to the serving-pair publisher, which
    /// admits every replacement against the same process retention contract.
    /// Consumers can therefore derive immutable startup claims from this
    /// value without turning later readiness changes into discovery mutations.
    #[must_use]
    pub fn admitted_raw_blob_retention(&self) -> RawBlobRetention {
        self.capture_pair().canonical().raw_blob_retention()
    }

    /// Returns the epoch `pair` serves when it answers `requested`, the
    /// epoch a pinned read names.
    fn chain_epoch(
        pair: &WalletServingReadPair,
        requested: Option<ChainEpochId>,
    ) -> Result<ChainEpoch, QueryError> {
        Self::pinned(pair.chain_epoch(), requested)
    }

    /// Returns `chain_epoch` when it answers `requested`, the epoch a pinned
    /// read names.
    fn pinned(
        chain_epoch: ChainEpoch,
        requested: Option<ChainEpochId>,
    ) -> Result<ChainEpoch, QueryError> {
        if requested.is_some_and(|requested| requested != chain_epoch.id) {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: requested.unwrap_or(chain_epoch.id),
            });
        }
        Ok(chain_epoch)
    }

    async fn block_id_at(
        pair: &WalletServingReadPair,
        height: BlockHeight,
    ) -> Result<BlockId, QueryError> {
        pair.canonical()
            .block_header_at(height)
            .await
            .map_err(QueryError::canonical_read)?
            .map(|header| BlockId::new(height, header.block_hash))
            .ok_or(QueryError::BlockNotInBestChain)
    }

    async fn resolve_block_id_by_selector(
        pair: &WalletServingReadPair,
        selector: BlockSelector,
        chain_epoch: ChainEpoch,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        let block_id = match selector {
            BlockSelector::Height(height) if height <= chain_epoch.visible_tip_height => {
                Self::block_id_at(pair, height).await?
            }
            BlockSelector::Hash(hash) => match pair
                .canonical()
                .block_hash_lookup(hash)
                .await
                .map_err(QueryError::canonical_read)?
            {
                BlockHashLookup::Resolved(block_id) => block_id,
                BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => {
                    return Err(QueryError::BlockNotInBestChain);
                }
            },
            BlockSelector::Height(_) => {
                return Err(QueryError::BlockNotInBestChain);
            }
            _ => return Err(unsupported_block_selector()),
        };
        Ok(BlockIdAtEpoch {
            chain_epoch,
            block_id,
        })
    }
}

#[async_trait]
impl<Broadcaster, NativeIngest> WalletQueryApi for WalletServingQuery<Broadcaster, NativeIngest>
where
    Broadcaster: TransactionBroadcaster + Clone,
    NativeIngest: Send + Sync + 'static,
{
    fn compact_block_fence(&self, epoch: ChainEpoch) -> Option<crate::CompactBlockFence> {
        let pair = self.serving_pair_slot.capture_at(Some(epoch.id));
        (pair.chain_epoch() == epoch).then(|| crate::CompactBlockFence {
            event_fence: pair.canonical_fence(),
            history_bounds: pair.canonical().history_bounds(),
            source: crate::CompactBlockSource::of(&pair),
        })
    }

    fn subscribe_publications(&self) -> Option<tokio::sync::watch::Receiver<()>> {
        Some(self.serving_pair_slot.subscribe_publications())
    }

    fn current_visible_tip_height(&self) -> Option<BlockHeight> {
        Some(
            self.serving_pair_slot
                .capture_ephemeral()
                .chain_epoch()
                .visible_tip_height,
        )
    }

    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        &self.native_endpoint_capabilities
    }

    fn upstream_node_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.node_verifications
            .as_ref()
            .and_then(EndpointVerifications::verified_capabilities)
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        let activations = self.capture_pair().network_upgrade_activations().clone();
        serving_read::record("network_upgrade_activations", None, ready(Ok(activations))).await
    }

    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, QueryError> {
        let unserved = Err(unavailable(WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1));
        serving_read::record("chain_value_pools_at_tip", None, ready(unserved)).await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        // The pair read and validated its epoch when it was admitted, so the
        // tip needs no storage read and no blocking hop.
        serving_read::record("visible_tip_block", None, async {
            let pair = self.serving_pair_slot.capture_at(at_epoch_id);
            let chain_epoch = Self::pinned(pair.chain_epoch(), at_epoch_id)?;
            Ok(VisibleTipBlock {
                height: chain_epoch.visible_tip_height,
                block_hash: chain_epoch.visible_tip_hash,
                chain_epoch,
            })
        })
        .await
    }

    async fn served_tip_snapshot(&self) -> Result<ServedTipSnapshot, QueryError> {
        serving_read::record("visible_tip_block", None, async {
            let pair = self.serving_pair_slot.capture_ephemeral();
            serving_read::read_ephemeral_pair(&self.serving_pair_slot, pair, |pair| async move {
                Ok(ServedTipSnapshot {
                    chain_epoch: pair.chain_epoch(),
                    block_time_unix_seconds: pair.visible_tip_block_time().await?,
                })
            })
            .await
        })
        .await
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        // The pair read and validated its epoch when it was admitted, so the
        // settled tip needs no storage read.
        serving_read::record("settled_tip_block", None, async {
            let pair = self.serving_pair_slot.capture_at(at_epoch_id);
            let chain_epoch = Self::pinned(pair.chain_epoch(), at_epoch_id)?;
            Ok(SettledTipBlock {
                height: chain_epoch.settled_tip_height,
                block_hash: chain_epoch.settled_tip_hash,
                chain_epoch,
            })
        })
        .await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        serving_read::run(
            "block_id_by_selector",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                Self::resolve_block_id_by_selector(&pair, selector, chain_epoch).await
            },
        )
        .await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        serving_read::run(
            "block_header_by_selector",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                // A visible height needs no resolution, because the header
                // read answers whether the height is in the best chain, and
                // the reader resolves a hash and reads its header in one
                // read.
                let block_header = match selector {
                    BlockSelector::Height(height) if height <= chain_epoch.visible_tip_height => {
                        pair.canonical().block_header_at(height).await
                    }
                    BlockSelector::Hash(hash) => pair.canonical().block_header_by_hash(hash).await,
                    BlockSelector::Height(_) => return Err(QueryError::BlockNotInBestChain),
                    _ => return Err(unsupported_block_selector()),
                }
                .map_err(QueryError::canonical_read)?
                .ok_or(QueryError::BlockNotInBestChain)?
                .into_header();
                Ok(BlockHeaderAtEpoch {
                    chain_epoch,
                    block_header,
                })
            },
        )
        .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        serving_read::run(
            "compact_block_at",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let compact_block = pair
                    .canonical()
                    .compact_block_at(height)
                    .await
                    .map_err(QueryError::canonical_read)?
                    .ok_or_else(|| artifact_unavailable(ArtifactFamily::CompactBlock, height))?;
                Ok(CompactBlock {
                    chain_epoch,
                    compact_block,
                })
            },
        )
        .await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: zinder_core::BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        let requested_blocks = Some(block_range.into_iter().len());
        serving_read::record("compact_blocks_in_range", requested_blocks, async {
            validate_block_range(block_range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let compact_blocks = pair
                    .canonical()
                    .compact_blocks_in_range(block_range)
                    .await
                    .map_err(QueryError::canonical_read)?;
                Ok(CompactBlockRange {
                    chain_epoch,
                    block_range,
                    compact_blocks,
                })
            })
            .await
        })
        .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        let requested_blocks = Some(block_range.into_iter().len());
        serving_read::record("compact_blocks_in_range", requested_blocks, async {
            validate_block_range(block_range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let compact_blocks = pair
                    .canonical()
                    .compact_blocks_in_range_up_to(block_range, order, max_bytes)
                    .await
                    .map_err(QueryError::canonical_read)?;
                let block_range = match (compact_blocks.first(), compact_blocks.last()) {
                    (Some(lowest), Some(highest)) => {
                        BlockHeightRange::inclusive(lowest.height(), highest.height())
                    }
                    _ => block_range,
                };
                Ok(CompactBlockRange {
                    chain_epoch,
                    block_range,
                    compact_blocks,
                })
            })
            .await
        })
        .await
    }

    async fn compact_block_rows_with_lengths(
        &self,
        block_range: BlockHeightRange,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, QueryError> {
        let requested_blocks = Some(block_range.into_iter().len());
        serving_read::record("compact_block_rows_with_lengths", requested_blocks, async {
            validate_block_range(block_range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                let _ = Self::chain_epoch(&pair, at_epoch_id)?;
                pair.canonical()
                    .compact_block_rows_with_lengths(block_range, max_bytes)
                    .await
                    .map(Some)
                    .map_err(QueryError::canonical_read)
            })
            .await
        })
        .await
    }

    async fn compact_block_stream(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockStream, QueryError> {
        let requested_blocks = Some(block_range.into_iter().len());
        let sub_read_bytes = self.compact_block_stream_sub_read_bytes;
        // A storage read outlives a cancelled request or a dropped stream, so
        // the first read and the task issuing the later ones each run in a
        // task of their own that keeps the adapter's stream slot until they
        // end.
        let stream_slot = CompactBlockStreamSlot::current();
        serving_read::record("compact_blocks_in_range", requested_blocks, async {
            validate_block_range(block_range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
            // The first read checks the whole range against retained history,
            // so a range the epoch cannot answer fails here, before any block.
            let first_read_slot = stream_slot.clone();
            let serving_pair_slot = self.serving_pair_slot.clone();
            let first_read = tokio::spawn(
                async move {
                    let _stream_slot = first_read_slot;
                    let pin = serving_read::SlotPin::new(&serving_pair_slot, at_epoch_id);
                    serving_read::read_at(pin, move |pair| async move {
                        let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                        let first_blocks = pair
                            .canonical()
                            .compact_blocks_in_range_up_to(
                                block_range,
                                HeightOrder::Ascending,
                                sub_read_bytes,
                            )
                            .await
                            .map_err(QueryError::canonical_read)?;
                        Ok((pair, chain_epoch, first_blocks))
                    })
                    .await
                }
                .in_current_span(),
            );
            let (pair, chain_epoch, first_blocks) =
                first_read
                    .await
                    .map_err(|join_error| QueryError::BlockingTaskFailed {
                        reason: join_error.to_string(),
                    })??;
            let (blocks, receiver) = mpsc::channel(COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY);
            let range_drive = drive_serving_pair_compact_block_range(
                self.serving_pair_slot.clone(),
                pair,
                CompactBlockRangeReads {
                    last_height: block_range.end,
                    sub_read_bytes,
                },
                first_blocks,
                blocks,
            );
            tokio::spawn(
                async move {
                    let _stream_slot = stream_slot;
                    range_drive.await;
                }
                .in_current_span(),
            );
            Ok(CompactBlockStream {
                chain_epoch,
                blocks: receiver,
            })
        })
        .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        serving_read::run(
            "full_block_at",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                if !pair.canonical().raw_blob_retention().retains_block_blobs() {
                    return Err(unavailable(WALLET_READ_FULL_BLOCK_AT_V1));
                }
                let block_blob = pair
                    .canonical()
                    .block_blob_at(height)
                    .await
                    .map_err(QueryError::canonical_read)?
                    .ok_or_else(|| artifact_unavailable(ArtifactFamily::BlockBlob, height))?;
                Ok(FullBlock {
                    chain_epoch,
                    block_blob,
                })
            },
        )
        .await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        serving_read::record("full_blocks_in_range", None, async {
            validate_block_range(block_range, DEFAULT_MAX_FULL_BLOCK_RANGE)?;
            // The pair validated its epoch and its retention when it was
            // admitted, so opening the stream reads no storage.
            let pair = self.serving_pair_slot.capture_at(at_epoch_id);
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            if !pair.canonical().raw_blob_retention().retains_block_blobs() {
                return Err(unavailable(WALLET_READ_FULL_BLOCK_RANGE_V1));
            }
            let (blocks, receiver) = mpsc::channel(FULL_BLOCK_STREAM_CHANNEL_CAPACITY);
            tokio::spawn(
                drive_serving_pair_full_block_range(
                    self.serving_pair_slot.clone(),
                    pair,
                    block_range,
                    blocks,
                )
                .in_current_span(),
            );
            Ok(FullBlockStream {
                chain_epoch,
                blocks: receiver,
            })
        })
        .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        serving_read::run(
            "transaction_by_id",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                // The location, the header of its block, and the transaction's
                // retained bytes come back from one read, and a miss is a proven
                // absence over the reader's complete history.
                let Some(transaction) = pair
                    .canonical()
                    .mined_transaction(transaction_id)
                    .await
                    .map_err(QueryError::canonical_read)?
                else {
                    return Ok(TransactionStatus {
                        chain_epoch,
                        status: TxStatus::NotFound,
                    });
                };
                let location = transaction.location;
                let header = transaction
                    .block_header
                    .ok_or(QueryError::BlockNotInBestChain)?;
                let raw_transaction_bytes =
                    match (transaction.transaction_blob, &self.cold_transactions) {
                        (Some(blob), _) => Some(blob.raw_transaction_bytes),
                        (None, Some(source)) => tokio::select! {
                            biased;
                            () = pair.reads_revoked() => return Err(pair.revoked_read_error()),
                            bytes = source.fetch(pair.canonical(), location) => Some(bytes?),
                        },
                        (None, None) => None,
                    };
                let chain_context = MinedTransactionChainContext::from_response_epoch(
                    &chain_epoch,
                    location.block_height,
                    pair.network_upgrade_activations()
                        .consensus_branch_id_at(location.block_height),
                    header.block_time,
                );
                Ok(TransactionStatus {
                    chain_epoch,
                    status: TxStatus::Mined(MinedTransaction::new(
                        location,
                        chain_context,
                        raw_transaction_bytes,
                    )),
                })
            },
        )
        .await
    }

    async fn transaction_at_block_index(
        &self,
        _height: BlockHeight,
        _tx_index: u64,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        let unserved = Err(QueryError::MaterializedViewUnavailable {
            capability: WALLET_READ_TRANSACTION_BY_ID_V2,
        });
        serving_read::record("transaction_at_block_index", None, ready(unserved)).await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        serving_read::run(
            "raw_transaction",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let transaction = pair
                    .canonical()
                    .transaction_blob_by_id(transaction_id)
                    .await
                    .map_err(QueryError::canonical_read)?;
                Ok(RawTransaction {
                    chain_epoch,
                    transaction,
                })
            },
        )
        .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        serving_read::record("transparent_address_unspent_outputs", None, async {
            let pair = self.serving_pair_slot.capture_at(at_epoch_id);
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            Ok(TransparentAddressUnspentOutputs::spawn(
                self.serving_pair_slot.clone(),
                pair,
                &request,
                chain_epoch,
            ))
        })
        .await
    }

    async fn transparent_address_unspent_outputs_on_demand(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        first_page_size: NonZeroU16,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        // The pair validated its epoch when it was admitted, so opening the
        // stream reads no storage. A separate operation keeps these cheap
        // opens out of the eager open's latency series.
        serving_read::record(
            "transparent_address_unspent_outputs_on_demand",
            None,
            async {
                let pair = self.serving_pair_slot.capture_at(at_epoch_id);
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                Ok(TransparentAddressUnspentOutputs::spawn_on_demand(
                    self.serving_pair_slot.clone(),
                    pair,
                    &request,
                    chain_epoch,
                    first_page_size,
                ))
            },
        )
        .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        serving_read::record("transparent_address_tx_ids_in_range", None, async {
            // A continuation resumes on the pair that issued it, which can be
            // a retained pair after a newer one was published.
            let cursor = request
                .from_cursor
                .as_ref()
                .map(TransparentHistoryCursor::parse)
                .transpose()?;
            let pin = self.pin(
                cursor
                    .map(TransparentHistoryCursor::chain_epoch_id)
                    .or(at_epoch_id),
            );
            serving_read::read_at(pin, move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let after = cursor
                    .map(|cursor| cursor.resume_on(pair.wallet_source()))
                    .transpose()?;
                if let Some(after) = after {
                    validate_transparent_history_cursor_key(after, &request)?;
                }
                let page_size =
                    NonZeroU16::new(u16::try_from(request.max_entries.get()).unwrap_or(u16::MAX))
                        .unwrap_or(NonZeroU16::MAX);
                let page = pair
                    .wallet()
                    .address_transaction_history_range_page(
                        request.address_script_hash,
                        BlockHeightRange::inclusive(request.start_height, request.end_height),
                        after,
                        page_size,
                    )
                    .await
                    .map_err(QueryError::wallet_read)?;
                let artifacts = page
                    .transactions
                    .into_iter()
                    .map(|row| {
                        TransparentAddressTxIndexArtifact::new(
                            request.address_script_hash,
                            row.key.block_height(),
                            row.key.tx_index_in_block(),
                            row.transaction_id,
                            row.block_hash,
                        )
                    })
                    .collect();
                Ok(TransparentAddressTxIds {
                    chain_epoch,
                    artifacts,
                    next_cursor: page
                        .next_page_after
                        .map(|key| TransparentHistoryCursor::issue(pair.wallet_source(), key)),
                })
            })
            .await
        })
        .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError> {
        serving_read::record("transparent_outpoint_spend_status", None, async {
            if !(1..=MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST).contains(&outpoints.len()) {
                return Err(QueryError::TransparentOutpointCountOutOfRange {
                    requested: outpoints.len(),
                    maximum: MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST,
                });
            }
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                // The pair's wallet reader is frozen at the same source as its
                // canonical epoch, so both output families answer at that
                // epoch; a pin naming another epoch fails here instead.
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let history_bounds = pair.canonical().history_bounds();
                // Checkpointed history cannot prove an outpoint absent, so the
                // request fails at the first one the projection does not hold
                // and no outpoint after it is read.
                let end = if history_bounds.intentionally_excludes(BlockHeight::new(1)) {
                    SpendStatusReadEnd::FirstNotFound
                } else {
                    SpendStatusReadEnd::AllOutpoints
                };
                let statuses = pair
                    .wallet()
                    .outpoint_spend_statuses(outpoints, end)
                    .await
                    .map_err(QueryError::wallet_read)?
                    .into_iter()
                    .map(|status| proven_spend_status(status, history_bounds))
                    .collect::<Result<_, _>>()?;
                Ok(TransparentOutpointSpendStatuses {
                    chain_epoch,
                    statuses,
                })
            })
            .await
        })
        .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        serving_read::record("transparent_address_balance", None, async {
            let requested_address_count = addresses.len();
            let address_count = u32::try_from(requested_address_count).ok().filter(|count| {
                (1..=crate::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES).contains(count)
            });
            if address_count.is_none() {
                return Err(QueryError::TransparentBalanceAddressCountExceeded {
                    requested: requested_address_count,
                    maximum: crate::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
                });
            }
            let addresses = addresses
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let address_count = u32::try_from(addresses.len()).unwrap_or(u32::MAX);
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let confirmed_zat = pair
                    .wallet()
                    .total_address_balance(addresses)
                    .await
                    .map_err(QueryError::wallet_read)?;
                Ok(TransparentAddressBalance {
                    confirmed_zat,
                    address_count,
                    chain_epoch,
                })
            })
            .await
        })
        .await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        use std::ops::ControlFlow;

        serving_read::record("tree_state_at", None, async {
            let mut deadline = None;
            let mut carried = None;
            loop {
                let request_deadline =
                    *deadline.get_or_insert_with(|| self.tree_state_replays.deadline());
                let selected = serving_read::read_at(self.pin(at_epoch_id), |pair| async move {
                    let epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                    let key = ReplayKey::new(height, pair.canonical());
                    if let Some(ready) = self.tree_state_replays.cached_for_reader(&key, &pair) {
                        return Ok(ControlFlow::Break((epoch, ready)));
                    }
                    let generation = Arc::downgrade(&pair);
                    let slot = self.serving_pair_slot.clone();
                    let weak_for_check = generation.clone();
                    let cache_generation = ReaderGeneration::live(&generation, move || {
                        slot.is_current_weak(&weak_for_check)
                    });
                    let reader = pair.canonical_arc();
                    if let Some(ready) = self
                        .tree_state_replays
                        .ready_state(&key, &cache_generation, &reader, request_deadline)
                        .await?
                    {
                        return Ok(ControlFlow::Break((epoch, ready)));
                    }
                    Ok(ControlFlow::Continue((
                        epoch,
                        key,
                        generation,
                        cache_generation,
                    )))
                })
                .await?;
                let (epoch, key, generation, cache_generation) = match selected {
                    ControlFlow::Break((epoch, ready)) => {
                        return Ok(tree_state_response(epoch, ready.serve()));
                    }
                    ControlFlow::Continue(missing) => missing,
                };
                let subscription = match self.tree_state_replays.select(
                    &key,
                    &cache_generation,
                    request_deadline,
                    carried.take(),
                )? {
                    Selection::Ready(state) => {
                        return Ok(tree_state_response(epoch, state.serve()));
                    }
                    Selection::Waiting(subscription) => subscription,
                };
                let slot = subscription.admit(&self.tree_state_replays).await?;
                let answer = serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                    let epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                    if !generation.ptr_eq(&Arc::downgrade(&pair)) {
                        return Ok(ControlFlow::Continue(subscription.carry(&slot)));
                    }
                    let state = subscription
                        .replay(slot, |cancelled| async move {
                            read_missing_height(pair.canonical(), height, cancelled).await
                        })
                        .await?;
                    Ok(state.map_or(ControlFlow::Continue(None), |state| {
                        ControlFlow::Break(tree_state_response(epoch, state))
                    }))
                })
                .await?;
                match answer {
                    ControlFlow::Break(state) => return Ok(state),
                    ControlFlow::Continue(slot) => carried = slot,
                }
            }
        })
        .await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        serving_read::run(
            "latest_tree_state_checkpoint",
            self.pin(at_epoch_id),
            move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                let checkpoint = pair
                    .canonical()
                    .tree_state_checkpoint_at_or_before(chain_epoch.visible_tip_height)
                    .await
                    .map_err(QueryError::canonical_read)?
                    .ok_or_else(|| {
                        artifact_unavailable(
                            ArtifactFamily::TreeState,
                            chain_epoch.visible_tip_height,
                        )
                    })?;
                Ok(tree_state_response(chain_epoch, checkpoint))
            },
        )
        .await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: zinder_core::SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        serving_read::record("subtree_roots_in_range", None, async {
            validate_subtree_root_range(subtree_root_range)?;
            serving_read::read_at(self.pin(at_epoch_id), move |pair| async move {
                let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
                canonical_subtree_roots(pair.canonical(), chain_epoch, subtree_root_range).await
            })
            .await
        })
        .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        // The protocol adapter records the chain-event stream; each page read
        // here is one poll of that stream, so it records nothing itself.
        serving_read::read_at(self.pin(None), move |pair| async move {
            let event_envelopes = pair
                .canonical()
                .wallet_chain_event_history(
                    from_cursor,
                    family,
                    crate::DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS,
                )
                .await
                .map_err(QueryError::canonical_read)?;
            Ok(ChainEvents { event_envelopes })
        })
        .await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        // Resolving the start is part of establishing the adapter-recorded
        // chain-event stream.
        serving_read::read_at(self.pin(None), move |pair| async move {
            pair.canonical()
                .resolve_wallet_chain_event_stream_start(start, requested_family)
                .await
                .map_err(QueryError::canonical_read)
        })
        .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<zinder_core::TransactionBroadcastOutcome, QueryError> {
        serving_read::record("broadcast_transaction", None, async {
            if !self
                .native_endpoint_capabilities
                .contains(WALLET_BROADCAST_TRANSACTION_V1)
            {
                return Err(unavailable(WALLET_BROADCAST_TRANSACTION_V1));
            }
            let pair = self.capture_pair();
            if raw_transaction.len() > zinder_core::MAX_RAW_TRANSACTION_BYTES {
                return Err(QueryError::BroadcastTransactionTooLarge {
                    actual: raw_transaction.len(),
                    maximum: zinder_core::MAX_RAW_TRANSACTION_BYTES,
                });
            }
            let reservation = self.broadcast_bytes.try_reserve(raw_transaction.len())?;
            if let Some(node_verifications) = &self.node_verifications {
                node_verifications.track_broadcast_height(&pair);
            }
            reservation
                .hold_during(self.broadcaster.broadcast_transaction(raw_transaction))
                .await
                .map_err(QueryError::Node)
        })
        .await
    }

    async fn standard_fee(&self) -> Result<StandardFee, QueryError> {
        serving_read::record("standard_fee", None, async {
            let pair = self.capture_pair();
            if let Some(node_verifications) = &self.node_verifications {
                node_verifications.track_broadcast_height(&pair);
            }
            self.broadcaster
                .standard_fee()
                .await
                .map_err(QueryError::Node)
        })
        .await
    }
}

/// Streams one range while retaining the exact admitted pair captured by the
/// request. Each `RocksDB` multi-get runs in a short blocking task; client
/// backpressure is bounded by the response channel.
///
/// A sub-read that finds a table file of the pair deleted continues on the
/// pair that replaces it only at the range's epoch.
async fn drive_serving_pair_full_block_range(
    slot: WalletServingPairSlot,
    mut pair: Arc<WalletServingReadPair>,
    block_range: BlockHeightRange,
    block_sender: mpsc::Sender<Result<zinder_core::BlockBlobArtifact, QueryError>>,
) {
    let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
    let last_height = block_range.end.value();
    let mut sub_read_start = block_range.start.value();
    loop {
        if pair.are_reads_revoked() {
            let error = pair.revoked_read_error();
            return fail_full_block_range(pair, &block_sender, error).await;
        }
        let sub_read_end = sub_read_start
            .saturating_add(FULL_BLOCK_STREAM_SUB_READ_BLOCKS - 1)
            .min(last_height);
        let sub_range = BlockHeightRange::inclusive(
            BlockHeight::new(sub_read_start),
            BlockHeight::new(sub_read_end),
        );
        let sub_read =
            serving_read::read_pair(&slot, pair, Some(chain_epoch_id), move |pair| async move {
                let block_blobs = pair
                    .canonical()
                    .block_blobs_in_range(sub_range)
                    .await
                    .map_err(QueryError::canonical_read)?;
                Ok((pair, block_blobs))
            })
            .await;
        let block_blobs = match sub_read {
            Ok((read_pair, block_blobs)) => {
                pair = read_pair;
                block_blobs
            }
            Err(error) => {
                // The failed sub-read has already released the pair.
                let _ = block_sender.send(Err(error)).await;
                return;
            }
        };
        // A consumer that stops reading blocks this send. Revocation ends the
        // range so the stalled stream cannot keep a released pair open.
        let forwarded = tokio::select! {
            biased;
            () = pair.reads_revoked() => None,
            forwarded = forward_serving_pair_full_block_sub_range(
                &block_sender,
                sub_range,
                block_blobs,
            ) => Some(forwarded),
        };
        match forwarded {
            Some(Ok(true)) if sub_read_end == last_height => return,
            Some(Ok(true)) => sub_read_start = sub_read_end.saturating_add(1),
            Some(Ok(false)) => return,
            Some(Err(error)) => return fail_full_block_range(pair, &block_sender, error).await,
            None => {
                let error = pair.revoked_read_error();
                return fail_full_block_range(pair, &block_sender, error).await;
            }
        }
    }
}

/// Where a compact-block stream ends and how much each of its reads keeps.
#[derive(Clone, Copy)]
struct CompactBlockRangeReads {
    last_height: BlockHeight,
    sub_read_bytes: usize,
}

/// Sends `blocks`, then reads and sends the rest of the range through
/// `reads.last_height` from `pair`, one byte-capped read at a time.
///
/// Every read is pinned to `pair`'s epoch, so the whole stream answers from
/// the epoch its header names. Revocation of the pair ends the stream after
/// the blocks already sent.
async fn drive_serving_pair_compact_block_range(
    slot: WalletServingPairSlot,
    mut pair: Arc<WalletServingReadPair>,
    reads: CompactBlockRangeReads,
    mut blocks: Vec<zinder_core::CompactBlockArtifact>,
    block_sender: mpsc::Sender<Result<zinder_core::CompactBlockArtifact, QueryError>>,
) {
    let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
    loop {
        let next_height = blocks
            .last()
            .and_then(|last| last.height().next())
            .filter(|next| *next <= reads.last_height);
        // A consumer that stops reading blocks this send. Revocation ends the
        // range so the stalled stream cannot keep a released pair open.
        let forwarded = tokio::select! {
            biased;
            () = pair.reads_revoked() => None,
            forwarded = forward_compact_blocks(&block_sender, blocks) => Some(forwarded),
        };
        match forwarded {
            Some(true) => {}
            Some(false) => return,
            None => {
                let error = pair.revoked_read_error();
                return fail_compact_block_range(pair, &block_sender, error).await;
            }
        }
        let Some(next_height) = next_height else {
            return;
        };
        if pair.are_reads_revoked() {
            let error = pair.revoked_read_error();
            return fail_compact_block_range(pair, &block_sender, error).await;
        }
        let remaining = BlockHeightRange::inclusive(next_height, reads.last_height);
        let sub_read =
            serving_read::read_pair(&slot, pair, Some(chain_epoch_id), move |pair| async move {
                let blocks = pair
                    .canonical()
                    .compact_blocks_in_range_up_to(
                        remaining,
                        HeightOrder::Ascending,
                        reads.sub_read_bytes,
                    )
                    .await
                    .map_err(QueryError::canonical_read)?;
                Ok((pair, blocks))
            })
            .await;
        match sub_read {
            Ok((read_pair, read_blocks)) => {
                pair = read_pair;
                blocks = read_blocks;
            }
            Err(error) => {
                // The failed read has already released the pair.
                let _ = block_sender.send(Err(error)).await;
                return;
            }
        }
    }
}

/// Sends `blocks` in order, returning `false` once the consumer is gone.
async fn forward_compact_blocks(
    block_sender: &mpsc::Sender<Result<zinder_core::CompactBlockArtifact, QueryError>>,
    blocks: Vec<zinder_core::CompactBlockArtifact>,
) -> bool {
    for block in blocks {
        if block_sender.send(Ok(block)).await.is_err() {
            return false;
        }
    }
    true
}

/// Releases `pair` and then ends a compact-block range with `error`.
///
/// The pair is dropped first because the send can wait on a stalled client.
async fn fail_compact_block_range(
    pair: Arc<WalletServingReadPair>,
    block_sender: &mpsc::Sender<Result<zinder_core::CompactBlockArtifact, QueryError>>,
    error: QueryError,
) {
    drop(pair);
    let _ = block_sender.send(Err(error)).await;
}

/// Releases `pair` and then ends a full-block range with `error`.
///
/// The pair is dropped first because the send can wait on a stalled client.
async fn fail_full_block_range(
    pair: Arc<WalletServingReadPair>,
    block_sender: &mpsc::Sender<Result<zinder_core::BlockBlobArtifact, QueryError>>,
    error: QueryError,
) {
    drop(pair);
    let _ = block_sender.send(Err(error)).await;
}

/// Sends the blocks of one sub-read in height order.
///
/// Returns `Ok(false)` once the client has gone away, and the failure that
/// ends the range when the sub-read is short or a block is missing.
async fn forward_serving_pair_full_block_sub_range(
    block_sender: &mpsc::Sender<Result<zinder_core::BlockBlobArtifact, QueryError>>,
    sub_range: BlockHeightRange,
    block_blobs: Vec<Option<zinder_core::BlockBlobArtifact>>,
) -> Result<bool, QueryError> {
    let expected_count = sub_range.into_iter().len();
    if block_blobs.len() != expected_count {
        return Err(QueryError::ArtifactCorrupt {
            family: ArtifactFamily::BlockBlob,
            reason: format!(
                "full-block range returned {} rows for {expected_count} requested heights",
                block_blobs.len()
            ),
        });
    }
    for (height, block_blob) in sub_range.into_iter().zip(block_blobs) {
        let Some(block_blob) = block_blob else {
            return Err(artifact_unavailable(ArtifactFamily::BlockBlob, height));
        };
        if block_sender.send(Ok(block_blob)).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns `status` when it answers the request, which a `NotFound` does
/// only as a proven absence.
///
/// The projection covers exactly the canonical history the pair retains, so
/// a miss proves absence only when that history starts at height 1.
fn proven_spend_status(
    status: TransparentOutpointSpendStatus,
    history_bounds: CanonicalHistoryBounds,
) -> Result<TransparentOutpointSpendStatus, QueryError> {
    if status == TransparentOutpointSpendStatus::NotFound
        && history_bounds.intentionally_excludes(BlockHeight::new(1))
    {
        return Err(QueryError::CanonicalHistoryUnavailable {
            first_available_height: history_bounds.first_available_height(),
        });
    }
    Ok(status)
}

const fn unsupported_block_selector() -> QueryError {
    QueryError::UnsupportedBlockSelector {
        reason: "selector is not supported by the canonical reader",
    }
}

fn unavailable(capability: &'static str) -> QueryError {
    QueryError::EndpointCapabilityUnavailable { capability }
}

fn artifact_unavailable(family: ArtifactFamily, height: BlockHeight) -> QueryError {
    QueryError::ArtifactUnavailable {
        family,
        key: ArtifactKey::BlockHeight(height),
    }
}

pub(crate) async fn canonical_subtree_roots(
    reader: &dyn crate::CanonicalReader,
    chain_epoch: ChainEpoch,
    subtree_root_range: zinder_core::SubtreeRootRange,
) -> Result<SubtreeRoots, QueryError> {
    let completed_subtree_count = chain_epoch
        .tip_metadata
        .completed_subtree_count(subtree_root_range.protocol);
    if subtree_root_range.start_index.value() >= completed_subtree_count {
        return Ok(SubtreeRoots {
            chain_epoch,
            protocol: subtree_root_range.protocol,
            start_index: subtree_root_range.start_index,
            subtree_roots: Vec::new(),
        });
    }
    let available_entries = completed_subtree_count
        .saturating_sub(subtree_root_range.start_index.value())
        .min(subtree_root_range.max_entries.get());
    let available_entries =
        NonZeroU32::new(available_entries).ok_or_else(|| QueryError::ArtifactUnavailable {
            family: ArtifactFamily::SubtreeRoot,
            key: ArtifactKey::SubtreeRootIndex {
                protocol: subtree_root_range.protocol,
                index: subtree_root_range.start_index,
            },
        })?;
    let available_range = zinder_core::SubtreeRootRange::new(
        subtree_root_range.protocol,
        subtree_root_range.start_index,
        available_entries,
    );
    let subtree_roots = reader
        .subtree_roots(available_range)
        .await
        .map_err(QueryError::canonical_read)?;
    Ok(SubtreeRoots {
        chain_epoch,
        protocol: subtree_root_range.protocol,
        start_index: subtree_root_range.start_index,
        subtree_roots,
    })
}

pub(crate) fn tree_state_response(
    chain_epoch: ChainEpoch,
    tree_state: ServedTreeState,
) -> TreeState {
    let block_id = tree_state.block_id();
    let block_time_seconds = tree_state.block_time_seconds();
    let (sapling, orchard, ironwood) = tree_state.into_final_state_bytes();
    TreeState {
        chain_epoch,
        height: block_id.height,
        block_hash: block_id.hash,
        block_time_seconds,
        frontiers: TreeStateFrontiers {
            sapling_final_state: sapling.map(Vec::from),
            orchard_final_state: orchard.map(Vec::from),
            ironwood_final_state: ironwood.map(Vec::from),
        },
    }
}

#[cfg(test)]
mod tree_state_conversion_tests {
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockId, ChainEpoch, ChainEpochId,
        ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
        CommitmentTreeFrontiers, Network, ShieldedProtocol, UnixTimestampMillis,
    };
    use zinder_store::ServedTreeState;

    use super::tree_state_response;

    fn chain_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(1),
            visible_tip_hash: BlockHash::from_bytes([1; 32]),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: BlockHash::from_bytes([1; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(20),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(0),
        }
    }

    /// The response must own the checkpoint's buffers, not copies of them.
    #[test]
    fn tree_state_response_moves_each_frontier_buffer_into_the_response()
    -> Result<(), zinder_core::CommitmentTreeFrontiersError> {
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let orchard = CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard);
        let ironwood = CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood);
        let source_buffers = [
            sapling.final_state_bytes().as_ptr(),
            orchard.final_state_bytes().as_ptr(),
            ironwood.final_state_bytes().as_ptr(),
        ];
        let expected_lengths = [
            sapling.final_state_bytes().len(),
            orchard.final_state_bytes().len(),
            ironwood.final_state_bytes().len(),
        ];
        let checkpoint = CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([2; 32])),
            1_774_668_401,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(sapling),
                Some(orchard),
                Some(ironwood),
            )?,
        );

        let tree_state = tree_state_response(chain_epoch(), ServedTreeState::from(checkpoint));
        let served = [
            tree_state.frontiers.sapling_final_state,
            tree_state.frontiers.orchard_final_state,
            tree_state.frontiers.ironwood_final_state,
        ];

        for ((served, source_buffer), expected_length) in
            served.iter().zip(source_buffers).zip(expected_lengths)
        {
            let served = served.as_deref().unwrap_or_default();
            assert_eq!(served.as_ptr(), source_buffer);
            assert_eq!(served.len(), expected_length);
        }
        assert_eq!(tree_state.height, BlockHeight::new(1));
        assert_eq!(tree_state.block_time_seconds, 1_774_668_401);
        Ok(())
    }
}

#[cfg(test)]
mod full_block_range_tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use zinder_core::{BlockHeight, BlockHeightRange, Network};
    use zinder_store::RawBlobRetention;
    use zinder_testkit::{
        ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
    };

    use crate::{
        FULL_BLOCK_STREAM_CHANNEL_CAPACITY, QueryError, RocksDbCanonicalReader,
        RocksDbWalletReader, WalletQueryApi, WalletServingPairSlot, WalletServingQuery,
        WalletServingReadPair,
    };

    /// Holders of a test pair while no read holds it: the test and the slot.
    const IDLE_PAIR_HOLDERS: usize = 2;

    /// A missing-block failure must not keep the pair while it waits for room.
    ///
    /// The range runs past the retained blocks, which fill the channel of a
    /// client that reads nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_full_block_range_releases_its_pair_before_a_stalled_client_reads()
    -> Result<(), Box<dyn Error>> {
        let retained_blocks = u32::try_from(FULL_BLOCK_STREAM_CHANNEL_CAPACITY)?;
        let fixture = FullBlockPair::with_blocks(retained_blocks)?;
        let mut stream = fixture
            .query
            .full_blocks_in_range(
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(8)),
                None,
            )
            .await?;

        fixture.wait_until_released().await?;

        for _ in 0..retained_blocks {
            stream
                .blocks
                .recv()
                .await
                .ok_or("the range ended early")??;
        }
        assert!(matches!(
            stream.blocks.recv().await,
            Some(Err(QueryError::ArtifactUnavailable { .. }))
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revoking_reads_releases_a_stalled_full_block_range() -> Result<(), Box<dyn Error>> {
        let block_count = 16;
        let fixture = FullBlockPair::with_blocks(block_count)?;
        let mut stream = fixture
            .query
            .full_blocks_in_range(
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(block_count)),
                None,
            )
            .await?;
        let epoch = stream.chain_epoch.id;

        assert!(fixture.pair.revoke_reads());
        fixture.wait_until_released().await?;

        let mut delivered = 0;
        let failure = loop {
            match stream.blocks.recv().await {
                Some(Ok(_)) => delivered += 1,
                Some(Err(error)) => break error,
                None => return Err("a revoked range ended as if complete".into()),
            }
        };
        assert!(
            matches!(
                failure,
                QueryError::ChainEpochPinUnavailable { chain_epoch_id } if chain_epoch_id == epoch
            ),
            "unexpected failure {failure:?}"
        );
        assert!(delivered < block_count);
        Ok(())
    }

    struct FullBlockPair {
        pair: Arc<WalletServingReadPair>,
        query: WalletServingQuery<()>,
        _store: WalletServingStoreFixture,
    }

    impl FullBlockPair {
        fn with_blocks(block_count: u32) -> Result<Self, Box<dyn Error>> {
            let activations = sample_regtest_upgrade_activations();
            let chain = ChainFixture::new(Network::ZcashRegtest)
                .with_raw_blob_retention(RawBlobRetention::All)
                .extend_blocks(block_count);
            let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
            let (canonical, wallet) = store.take_readers()?;
            let pair = Arc::new(WalletServingReadPair::new(
                Arc::new(RocksDbCanonicalReader::new(canonical)?),
                Arc::new(RocksDbWalletReader::new(wallet)),
            )?);
            let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
                Arc::clone(&pair),
            ));
            Ok(Self {
                pair,
                query,
                _store: store,
            })
        }

        async fn wait_until_released(&self) -> Result<(), Box<dyn Error>> {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while Arc::strong_count(&self.pair) > IDLE_PAIR_HOLDERS {
                if tokio::time::Instant::now() > deadline {
                    return Err("the full-block range kept its pair".into());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tree_state_replay_limit_tests {
    use std::{error::Error, num::NonZeroU32, sync::Arc, time::Duration};

    use metrics_exporter_prometheus::PrometheusBuilder;

    use zinder_core::{BlockHeight, Network};
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    use crate::{
        QueryError, RocksDbCanonicalReader, RocksDbWalletReader, WalletQueryApi,
        WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
        observed_canonical_reader::ObservedCanonicalReader,
        tree_state_replay_limit::TreeStateReplayLimit,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn a_shared_replay_releases_its_slot_when_the_last_wallet_leaves()
    -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let (canonical, replays) =
            ObservedCanonicalReader::holding_replays(RocksDbCanonicalReader::new(canonical)?);
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(canonical),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let mut query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
        let reconstructed = BlockHeight::new(2);

        let cancelled = tokio::spawn({
            let query = query.clone();
            async move { query.tree_state_at(reconstructed, None).await }
        });
        replays.replay_started().await;
        let second = tokio::spawn({
            let query = query.clone();
            async move { query.tree_state_at(reconstructed, None).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while query.tree_state_replays.waiter_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        cancelled.abort();
        assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));
        assert!(!second.is_finished());
        assert!(query.tree_state_replays.admit().await.is_err());
        second.abort();
        assert!(second.await.is_err_and(|error| error.is_cancelled()));
        let permit = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(permit) = query.tree_state_replays.admit().await {
                    break permit;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        drop(permit);
        assert_eq!(replays.runs.load(std::sync::atomic::Ordering::Relaxed), 1);
        replays.release();
        assert_eq!(
            query.tree_state_at(reconstructed, None).await?.height,
            reconstructed
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_surviving_wallet_counts_one_answer_and_one_shared_seed_lookup()
    -> Result<(), Box<dyn Error>> {
        use std::sync::atomic::Ordering;

        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let (canonical, wallet) = store.take_readers()?;
        let (reader, gate) =
            ObservedCanonicalReader::holding_replays(RocksDbCanonicalReader::new(canonical)?);
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(reader),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let mut query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO)
            .with_waiters_per_slot(NonZeroU32::MIN);
        let height = BlockHeight::new(2);
        let request = || {
            let query = query.clone();
            tokio::spawn(async move { query.tree_state_at(height, None).await })
        };
        let first = request();
        gate.replay_started().await;
        let follower = request();
        tokio::time::timeout(Duration::from_secs(5), async {
            while query.tree_state_replays.waiter_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(matches!(
            request().await?,
            Err(QueryError::TreeStateReplayLimitReached { .. })
        ));
        first.abort();
        assert!(first.await.is_err_and(|error| error.is_cancelled()));
        assert_eq!(query.tree_state_replays.waiter_count(), 0);
        gate.release();
        assert_eq!(follower.await??.height, height);
        assert_eq!(query.tree_state_at(height, None).await?.height, height);
        assert_eq!(gate.runs.load(Ordering::Relaxed), 1);
        assert_eq!(gate.seed_lookups.load(Ordering::Relaxed), 1);
        let rendered = metrics.render();
        for expected in [
            r#"zinder_query_tree_state_reads_total{source="coalesced",height_band="0"} 1"#,
            r#"zinder_query_tree_state_reads_total{source="cached",height_band="0"} 1"#,
            r#"zinder_query_tree_state_replayed_blocks_count{height_band="0"} 1"#,
            r#"zinder_query_tree_state_replay_duration_seconds_count{height_band="0"} 1"#,
        ] {
            assert!(
                rendered.lines().any(|line| line == expected),
                "missing {expected}"
            );
        }
        assert!(!rendered.contains(r#"source="replayed""#));
        Ok(())
    }

    /// A saturated limit refuses reconstructions but still serves a stored
    /// checkpoint, and admits reconstructions again once a slot frees.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_saturated_replay_limit_refuses_only_reconstructed_heights()
    -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let mut query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
        let reconstructed = BlockHeight::new(2);
        let checkpointed_tip = BlockHeight::new(3);

        let held_slot = query.tree_state_replays.admit().await?;
        assert!(matches!(
            query.tree_state_at(reconstructed, None).await,
            Err(QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays
            }) if max_concurrent_replays == NonZeroU32::MIN
        ));
        assert_eq!(
            query.tree_state_at(checkpointed_tip, None).await?.height,
            checkpointed_tip
        );

        drop(held_slot);
        assert_eq!(
            query.tree_state_at(reconstructed, None).await?.height,
            reconstructed
        );
        Ok(())
    }

    /// A reconstruction that finds the only slot taken waits for it and is
    /// served once the slot frees within the wait.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reconstruction_waits_for_a_slot_that_frees_within_the_wait()
    -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let mut query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
        let query = query.with_tree_state_replay_wait(Duration::from_secs(5));
        let reconstructed = BlockHeight::new(2);

        let held_slot = query.tree_state_replays.admit().await?;
        let waiting = tokio::spawn({
            let query = query.clone();
            async move { query.tree_state_at(reconstructed, None).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting.is_finished(),
            "the request did not wait for the slot"
        );
        drop(held_slot);

        assert_eq!(waiting.await??.height, reconstructed);
        Ok(())
    }

    /// A subscription taken before a read wakes on the publication that
    /// replaces the pair the read saw. Neither the subscription nor the
    /// unpinned tip read marks a pair observed, so publication releases the
    /// replaced pair at once instead of retaining it for epoch pins.
    #[tokio::test]
    async fn a_publication_subscription_wakes_on_the_next_publication() -> Result<(), Box<dyn Error>>
    {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let activations = sample_regtest_upgrade_activations();
        let mut original = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let mut updated =
            WalletServingStoreFixture::from_chain(&chain.extend_blocks(1), &activations)?;
        let read_pair = |store: &mut WalletServingStoreFixture| -> Result<_, Box<dyn Error>> {
            let (canonical, wallet) = store.take_readers()?;
            Ok(Arc::new(WalletServingReadPair::new(
                Arc::new(RocksDbCanonicalReader::new(canonical)?),
                Arc::new(RocksDbWalletReader::new(wallet)),
            )?))
        };
        let first_pair = read_pair(&mut original)?;
        let first_tip = first_pair.chain_epoch().visible_tip_height;
        let replaced = Arc::downgrade(&first_pair);
        let slot = WalletServingPairSlot::new(first_pair);
        let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        let mut publications = query
            .subscribe_publications()
            .ok_or("a slot-backed query publishes replacements")?;
        let seen = query
            .current_visible_tip_height()
            .ok_or("a slot-backed query reads its tip unpinned")?;
        assert_eq!(seen, first_tip);
        assert!(!slot.capture_ephemeral().is_observed_for_epoch_pin());
        assert!(!publications.has_changed()?);

        slot.publish_for_test(read_pair(&mut updated)?);
        tokio::time::timeout(Duration::from_secs(1), publications.changed()).await??;
        assert!(
            replaced.upgrade().is_none(),
            "the replaced pair was retained for epoch pins"
        );
        let advanced = query
            .current_visible_tip_height()
            .ok_or("a slot-backed query reads its tip unpinned")?;
        assert_eq!(advanced.value(), seen.value() + 1);
        assert!(!slot.capture_ephemeral().is_observed_for_epoch_pin());
        Ok(())
    }

    #[tokio::test]
    async fn replacement_serves_the_waiting_wallet_before_a_later_wallet()
    -> Result<(), Box<dyn Error>> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let activations = sample_regtest_upgrade_activations();
        let mut original = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let mut updated =
            WalletServingStoreFixture::from_chain(&chain.extend_blocks(1), &activations)?;
        let read_pair = |store: &mut WalletServingStoreFixture| -> Result<_, Box<dyn Error>> {
            let (canonical, wallet) = store.take_readers()?;
            Ok(Arc::new(WalletServingReadPair::new(
                Arc::new(RocksDbCanonicalReader::new(canonical)?),
                Arc::new(RocksDbWalletReader::new(wallet)),
            )?))
        };
        let slot = WalletServingPairSlot::new(read_pair(&mut original)?);
        let mut query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        query.tree_state_replays =
            TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
        let occupied = query.tree_state_replays.admit().await?;
        let first = tokio::spawn({
            let query = query.clone();
            async move { query.tree_state_at(BlockHeight::new(2), None).await }
        });
        query.tree_state_replays.wait_for_waiters(1).await?;
        let later = query.tree_state_replays.admit();
        tokio::pin!(later);
        assert!(futures_util::poll!(&mut later).is_pending());
        slot.publish_for_test(read_pair(&mut updated)?);
        drop(occupied);
        let answer = first.await??;
        assert_eq!(answer.height, BlockHeight::new(2));
        assert_eq!(answer.chain_epoch.visible_tip_height, BlockHeight::new(4));
        drop(later.await?);
        Ok(())
    }

    /// A request whose pair is replaced while it waits for a rebuild slot
    /// records how the new pair answered: here from the checkpoint it
    /// stores, with no replayed blocks and no rebuild time.
    #[tokio::test]
    async fn a_pair_replaced_during_the_slot_wait_records_how_it_answered()
    -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered = recorder.handle();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        // Construction checkpoints only the tip of the first store, and
        // height 2 as the build tip of the store that live-appends block 3.
        let mut replaying = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let mut storing =
            WalletServingStoreFixture::from_chain_after_live_append(&chain, &activations)?;
        let (canonical, wallet) = replaying.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        let (canonical, wallet) = storing.take_readers()?;
        let replacement = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let replacement_epoch = replacement.canonical_fence().chain_epoch_id();
        let mut query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
        let query = query.with_tree_state_replay_wait(Duration::from_secs(5));
        let height = BlockHeight::new(2);

        let held_slot = query.tree_state_replays.admit().await?;
        let waiting = tokio::spawn({
            let query = query.clone();
            async move { query.tree_state_at(height, None).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting.is_finished(),
            "the request did not wait for the slot"
        );
        slot.publish_for_test(replacement);
        drop(held_slot);
        let tree_state = waiting.await??;

        assert_eq!(tree_state.height, height);
        assert_eq!(tree_state.chain_epoch.id, replacement_epoch);
        let rendered = rendered.render();
        assert!(
            rendered.lines().any(|line| line
                == "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"0\"} 1"),
            "the read must count as stored:\n{rendered}"
        );
        for absent in [
            "zinder_query_tree_state_reads_total{source=\"replayed\"",
            "zinder_query_tree_state_replay_duration_seconds",
        ] {
            assert!(
                !rendered.contains(absent),
                "a stored answer records no rebuild ({absent}):\n{rendered}"
            );
        }
        Ok(())
    }

    /// Reads count per caller and reconstruction work counts per rebuild.
    #[tokio::test]
    async fn tree_state_reads_record_depth_slot_and_replay_time() -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered = recorder.handle();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let mut query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);

        query.tree_state_at(BlockHeight::new(3), None).await?;
        query.tree_state_at(BlockHeight::new(2), None).await?;
        let held_slot = query.tree_state_replays.admit().await?;
        assert_eq!(
            query.tree_state_at(BlockHeight::new(2), None).await?.height,
            BlockHeight::new(2)
        );
        assert!(
            query
                .tree_state_at(BlockHeight::new(1), None)
                .await
                .is_err()
        );
        drop(held_slot);

        let rendered = rendered.render();
        for expected in [
            "zinder_query_tree_state_reads_total{source=\"stored\",height_band=\"0\"} 1",
            "zinder_query_tree_state_reads_total{source=\"replayed\",height_band=\"0\"} 1",
            "zinder_query_tree_state_reads_total{source=\"cached\",height_band=\"0\"} 1",
            "zinder_query_tree_state_replayed_blocks_count{height_band=\"0\"} 1",
            "zinder_query_tree_state_replayed_commitments_count{pool=\"sapling\",height_band=\"0\"} 1",
            "zinder_query_tree_state_replayed_commitments_count{pool=\"orchard\",height_band=\"0\"} 1",
            "zinder_query_tree_state_replayed_commitments_count{pool=\"ironwood\",height_band=\"0\"} 1",
            "zinder_query_tree_state_replay_duration_seconds_count{height_band=\"0\"} 1",
            "zinder_query_tree_state_replay_wait_duration_seconds_count 3",
            "zinder_query_tree_state_replay_admissions_total 2",
            "zinder_query_tree_state_replay_refusals_total{reason=\"wait_expired\"} 1",
        ] {
            assert!(
                rendered.lines().any(|line| line == expected),
                "missing {expected}:\n{rendered}"
            );
        }
        Ok(())
    }

    fn build_serving_pair(
        chain: &ChainFixture,
        through: u32,
        settled: u32,
    ) -> Result<(WalletServingStoreFixture, Arc<WalletServingReadPair>), Box<dyn Error>> {
        let baseline = CanonicalBaseline::new(chain)
            .through(BlockHeight::new(through))
            .with_settled_tip(BlockHeight::new(settled));
        let mut store =
            CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?
                .into_serving_store_fixture()?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        Ok((store, pair))
    }

    #[tokio::test]
    async fn settled_cache_survives_tip_advances_and_resets_on_fork() -> Result<(), Box<dyn Error>>
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let rendered = recorder.handle();
        let _local_recorder = metrics::set_default_local_recorder(&recorder);
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
        let fork_chain = chain.fork_at(BlockHeight::new(2))?.extend_blocks(4);
        let (_s1, initial) = build_serving_pair(&chain, 3, 2)?;
        let (_s2, appended) = build_serving_pair(&chain, 4, 2)?;
        let (_s3, settled_extended) = build_serving_pair(&chain, 5, 4)?;
        let (_s4, settled_fork) = build_serving_pair(&fork_chain, 5, 4)?;

        let slot = WalletServingPairSlot::new(initial);
        let mut query = WalletServingQuery::from_serving_pair_slot(slot.clone());
        query.tree_state_replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
        let height = BlockHeight::new(2);

        let first = query.tree_state_at(height, None).await?;
        slot.publish_for_test(appended);
        let second = query.tree_state_at(height, None).await?;
        slot.publish_for_test(settled_extended);
        let third = query.tree_state_at(height, None).await?;
        assert_eq!(second.frontiers, first.frontiers);
        assert_eq!(third.frontiers, first.frontiers);

        slot.publish_for_test(settled_fork);
        let rebuilt = query.tree_state_at(height, None).await?;
        assert_ne!(rebuilt.block_hash, first.block_hash);

        let report = rendered.render();
        assert!(
            report.lines().any(|line| line
                == "zinder_query_tree_state_reads_total{source=\"cached\",height_band=\"0\"} 2"),
            "expected two cached hits across visible and settled tip advances:\n{report}"
        );
        assert!(
            report.lines().any(|line| line
                == "zinder_query_tree_state_reads_total{source=\"replayed\",height_band=\"0\"} 2"),
            "expected initial replay plus one post-fork rebuild:\n{report}"
        );
        Ok(())
    }
}

#[cfg(test)]
mod block_selector_read_tests {
    use std::{error::Error, sync::Arc};

    use zinder_core::{BlockHash, BlockHeight, BlockId, BlockSelector, Network};
    use zinder_testkit::{
        ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
    };

    use crate::{
        QueryError, RocksDbCanonicalReader, RocksDbWalletReader, WalletQueryApi,
        WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
        observed_canonical_reader::ObservedCanonicalReader,
    };

    /// A header selected by hash is resolved and read in one storage read,
    /// so in one hop to the blocking pool.
    #[tokio::test]
    async fn a_header_selected_by_hash_takes_one_storage_read() -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let canonical = Arc::new(ObservedCanonicalReader::new(RocksDbCanonicalReader::new(
            canonical,
        )?));
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::<ObservedCanonicalReader>::clone(&canonical),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair));
        let block = chain
            .block_at(BlockHeight::new(2))
            .ok_or("the fixture has a block at height 2")?;

        let header = query
            .block_header_by_selector(BlockSelector::Hash(block.hash), None)
            .await?;
        assert_eq!(
            header.block_header.block_id,
            BlockId::new(block.height, block.hash)
        );
        assert_eq!(canonical.reads(), 1);

        let unknown = query
            .block_header_by_selector(BlockSelector::Hash(BlockHash::from_bytes([0xee; 32])), None)
            .await;
        assert!(matches!(unknown, Err(QueryError::BlockNotInBestChain)));
        assert_eq!(canonical.reads(), 2);
        Ok(())
    }
}

#[cfg(test)]
mod compact_block_stream_tests {
    use std::{error::Error, num::NonZeroU32, sync::Arc, time::Duration};

    use tokio_stream::StreamExt as _;
    use tonic::{Code, Request};
    use zinder_core::{BlockHeight, BlockHeightRange, ChainEpochId, Network};
    use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
    use zinder_testkit::{
        ChainFixture, IngestControlFixture, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    use crate::{
        AdmittedIngestControl, QueryError, RocksDbCanonicalReader, RocksDbWalletReader,
        WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter, WalletServingPairSlot,
        WalletServingQuery, WalletServingReadPair,
    };

    type NativeQuery = WalletServingQuery<(), AdmittedIngestControl>;

    /// The only blocking thread of a runtime built with one, held until
    /// [`release`](Self::release).
    struct HeldBlockingPool {
        release: tokio::sync::oneshot::Sender<()>,
        holder: tokio::task::JoinHandle<()>,
    }

    impl HeldBlockingPool {
        async fn hold() -> Result<Self, Box<dyn Error>> {
            let (pool_held, pool_held_receiver) = tokio::sync::oneshot::channel();
            let (release, pool_released) = tokio::sync::oneshot::channel::<()>();
            let holder = tokio::task::spawn_blocking(move || {
                let _ = pool_held.send(());
                let _ = pool_released.blocking_recv();
            });
            pool_held_receiver.await?;
            Ok(Self { release, holder })
        }

        async fn release(self) -> Result<(), Box<dyn Error>> {
            self.release
                .send(())
                .map_err(|()| "the held blocking thread stopped waiting")?;
            self.holder.await?;
            Ok(())
        }
    }

    /// A native adapter over three blocks that serves one stream at once,
    /// whose stream reads keep at most `sub_read_bytes` each.
    async fn one_stream_adapter(
        sub_read_bytes: usize,
    ) -> Result<
        (
            WalletQueryGrpcAdapter<NativeQuery>,
            WalletServingStoreFixture,
            IngestControlFixture,
        ),
        Box<dyn Error>,
    > {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let ingest_control =
            AdmittedIngestControl::connect(ingest_fixture.endpoint(), None, Network::ZcashRegtest)
                .await?;
        let mut query = WalletServingQuery::from_admitted_native_serving_pair(slot, ingest_control);
        query.compact_block_stream_sub_read_bytes = sub_read_bytes;
        let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default())
            .with_max_concurrent_compact_block_streams(NonZeroU32::MIN);
        Ok((adapter, store, ingest_fixture))
    }

    fn range_request(end_height: u32) -> Request<wallet::CompactBlocksInRangeRequest> {
        Request::new(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height,
            at_epoch_id: None,
        })
    }

    /// Asserts that opening a stream is refused at once because the only
    /// slot is still taken.
    async fn assert_slot_taken(
        adapter: &WalletQueryGrpcAdapter<NativeQuery>,
    ) -> Result<(), Box<dyn Error>> {
        let opened = tokio::time::timeout(
            Duration::from_secs(1),
            WalletQueryService::compact_blocks_in_range(adapter, range_request(3)),
        )
        .await
        .map_err(|_| "a stream took the slot a queued read still needs")?;
        match opened {
            Ok(_) => Err("a stream opened while the only slot was taken".into()),
            Err(status) if status.code() == Code::ResourceExhausted => Ok(()),
            Err(status) => Err(format!("unexpected status {status:?}").into()),
        }
    }

    /// Waits until a stream opens again once the queued read has ended.
    async fn wait_for_free_slot(
        adapter: &WalletQueryGrpcAdapter<NativeQuery>,
    ) -> Result<(), Box<dyn Error>> {
        let started = std::time::Instant::now();
        loop {
            match WalletQueryService::compact_blocks_in_range(adapter, range_request(3)).await {
                Ok(_) => return Ok(()),
                Err(status) if status.code() == Code::ResourceExhausted => {}
                Err(status) => return Err(format!("unexpected status {status:?}").into()),
            }
            if started.elapsed() > Duration::from_secs(5) {
                return Err("the stream slot was never returned".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn one_blocking_thread_runtime() -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
    }

    /// A request cancelled while its first read waits on the blocking pool
    /// keeps its stream slot until that read ends.
    #[test]
    fn a_cancelled_compact_block_stream_keeps_its_slot_until_its_read_ends()
    -> Result<(), Box<dyn Error>> {
        one_blocking_thread_runtime()?.block_on(cancel_a_stream_while_its_first_read_waits())
    }

    async fn cancel_a_stream_while_its_first_read_waits() -> Result<(), Box<dyn Error>> {
        let (adapter, _store, _ingest) =
            one_stream_adapter(crate::COMPACT_BLOCK_STREAM_SUB_READ_BYTES).await?;
        let held_pool = HeldBlockingPool::hold().await?;

        let cancelled = tokio::time::timeout(
            Duration::from_millis(100),
            WalletQueryService::compact_blocks_in_range(&adapter, range_request(3)),
        )
        .await;
        assert!(cancelled.is_err(), "the stream opened without reading");
        assert_slot_taken(&adapter).await?;

        held_pool.release().await?;
        wait_for_free_slot(&adapter).await
    }

    /// A stream the client drops while its next read waits on the blocking
    /// pool keeps its slot until that read ends.
    #[test]
    fn a_dropped_compact_block_stream_keeps_its_slot_until_its_next_read_ends()
    -> Result<(), Box<dyn Error>> {
        one_blocking_thread_runtime()?.block_on(drop_a_stream_while_its_next_read_waits())
    }

    async fn drop_a_stream_while_its_next_read_waits() -> Result<(), Box<dyn Error>> {
        let (adapter, _store, _ingest) = one_stream_adapter(1).await?;
        let mut stream = WalletQueryService::compact_blocks_in_range(&adapter, range_request(3))
            .await?
            .into_inner();
        // Held before the stream's task runs, so its second read queues.
        let held_pool = HeldBlockingPool::hold().await?;
        for _ in 0..2 {
            stream.next().await.ok_or("the stream ended early")??;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        drop(stream);
        assert_slot_taken(&adapter).await?;

        held_pool.release().await?;
        wait_for_free_slot(&adapter).await
    }

    fn query_over(
        blocks: u32,
    ) -> Result<(WalletServingQuery<()>, WalletServingStoreFixture), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(blocks);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        Ok((
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(pair)),
            store,
        ))
    }

    /// A stream whose byte budget keeps one block per read still delivers
    /// every height of its range, in order, under the epoch it names.
    #[tokio::test]
    async fn a_compact_block_stream_reads_its_range_in_byte_capped_pieces()
    -> Result<(), Box<dyn Error>> {
        let (mut query, _store) = query_over(5)?;
        query.compact_block_stream_sub_read_bytes = 1;
        let current_epoch = query.visible_tip_block(None).await?.chain_epoch;

        let mut stream = query
            .compact_block_stream(
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(5)),
                Some(current_epoch.id),
            )
            .await?;
        let mut heights = Vec::new();
        while let Some(block) = stream.blocks.recv().await {
            heights.push(block?.height().value());
        }

        assert_eq!(stream.chain_epoch, current_epoch);
        assert_eq!(heights, [1, 2, 3, 4, 5]);
        Ok(())
    }

    /// A stream pinned to an epoch its pair does not serve fails before it
    /// delivers anything.
    #[tokio::test]
    async fn a_compact_block_stream_refuses_an_unserved_pin_before_streaming()
    -> Result<(), Box<dyn Error>> {
        let (query, _store) = query_over(3)?;
        let unserved = ChainEpochId::new(u64::MAX);

        let outcome = query
            .compact_block_stream(
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3)),
                Some(unserved),
            )
            .await;

        assert!(matches!(
            outcome,
            Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == unserved
        ));
        Ok(())
    }

    /// The visible tip answers from the admitted pair even while every
    /// blocking thread is busy, and keeps its pin check.
    #[test]
    fn the_visible_tip_answers_without_the_blocking_pool() -> Result<(), Box<dyn Error>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(read_the_tip_while_the_blocking_pool_is_held())
    }

    async fn read_the_tip_while_the_blocking_pool_is_held() -> Result<(), Box<dyn Error>> {
        let (query, _store) = query_over(3)?;
        let (pool_held, pool_held_receiver) = tokio::sync::oneshot::channel();
        let (release_pool, pool_released) = tokio::sync::oneshot::channel::<()>();
        let held_pool = tokio::task::spawn_blocking(move || {
            let _ = pool_held.send(());
            let _ = pool_released.blocking_recv();
        });
        pool_held_receiver.await?;

        let tip = tokio::time::timeout(Duration::from_secs(1), query.visible_tip_block(None))
            .await
            .map_err(|_| "the visible tip waited for the blocking pool")??;
        assert_eq!(tip.height, BlockHeight::new(3));
        assert_eq!(
            query.visible_tip_block(Some(tip.chain_epoch.id)).await?,
            tip
        );
        let unserved = ChainEpochId::new(u64::MAX);
        assert!(matches!(
            query.visible_tip_block(Some(unserved)).await,
            Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == unserved
        ));

        release_pool
            .send(())
            .map_err(|()| "the held blocking thread stopped waiting")?;
        held_pool.await?;
        Ok(())
    }
}

#[cfg(test)]
mod broadcast_byte_budget_tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use zinder_core::{
        BroadcastDuplicate, MAX_RAW_TRANSACTION_BYTES, Network, RawTransactionBytes,
        TransactionBroadcastOutcome, TransactionId,
    };
    use zinder_testkit::{
        ChainFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
        sample_regtest_upgrade_activations,
    };

    use crate::{
        OrderedBroadcaster, QueryError, RocksDbCanonicalReader, RocksDbWalletReader,
        WalletQueryApi, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
        broadcast_byte_budget::BroadcastByteBudget,
    };

    /// A broadcast that does not fit the bytes other broadcasts hold is
    /// refused before it reaches the node, and a finished broadcast returns
    /// its bytes to the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_spent_broadcast_budget_refuses_until_bytes_return() -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        let broadcaster = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([7; 32]))
            .with_node_activations(activations);
        let node = OrderedBroadcaster::new(broadcaster.clone(), []);
        node.verify(&slot).await?;
        let mut query =
            WalletServingQuery::from_serving_pair_slot(slot).with_node_broadcast(Some(node));
        query.broadcast_bytes = BroadcastByteBudget::new(MAX_RAW_TRANSACTION_BYTES);
        let largest = || RawTransactionBytes::new(vec![0; MAX_RAW_TRANSACTION_BYTES]);

        let in_flight = query.broadcast_bytes.try_reserve(1)?;
        assert!(matches!(
            query.broadcast_transaction(largest()).await,
            Err(QueryError::BroadcastBudgetExhausted { budget_bytes })
                if budget_bytes == MAX_RAW_TRANSACTION_BYTES
        ));
        assert_eq!(broadcaster.call_count(), 0);

        drop(in_flight);
        query.broadcast_transaction(largest()).await?;
        query.broadcast_transaction(largest()).await?;
        assert_eq!(broadcaster.call_count(), 2);
        Ok(())
    }

    /// A broadcast cancelled while its transaction waits to be hashed on the
    /// blocking pool keeps its bytes reserved until that hashing ends.
    #[test]
    fn a_cancelled_broadcast_keeps_its_bytes_until_detached_hashing_ends()
    -> Result<(), Box<dyn Error>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(cancel_a_broadcast_while_its_hashing_waits())
    }

    async fn cancel_a_broadcast_while_its_hashing_waits() -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = store.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        // An unreachable endpoint followed by a duplicate answer makes the
        // broadcast hash the transaction to report it accepted.
        let node = OrderedBroadcaster::new(
            MockTransactionBroadcaster::node_unavailable("sendrawtransaction timed out")
                .with_node_activations(activations.clone()),
            [
                MockTransactionBroadcaster::returning(TransactionBroadcastOutcome::Duplicate(
                    BroadcastDuplicate {
                        error_code: Some(-27),
                        message: "transaction already exists in mempool".to_owned(),
                    },
                ))
                .with_node_activations(activations),
            ],
        );
        node.verify(&slot).await?;
        let mut query =
            WalletServingQuery::from_serving_pair_slot(slot).with_node_broadcast(Some(node));
        query.broadcast_bytes = BroadcastByteBudget::new(MAX_RAW_TRANSACTION_BYTES);

        // Hold the only blocking thread so the hashing task stays queued.
        let (pool_held, pool_held_receiver) = tokio::sync::oneshot::channel();
        let (release_pool, pool_released) = tokio::sync::oneshot::channel::<()>();
        let held_pool = tokio::task::spawn_blocking(move || {
            let _ = pool_held.send(());
            let _ = pool_released.blocking_recv();
        });
        pool_held_receiver.await?;

        let cancelled = tokio::time::timeout(
            Duration::from_millis(100),
            query.broadcast_transaction(RawTransactionBytes::new(vec![
                0;
                MAX_RAW_TRANSACTION_BYTES
            ])),
        )
        .await;
        assert!(cancelled.is_err(), "the broadcast finished without hashing");
        assert!(
            query.broadcast_bytes.try_reserve(1).is_err(),
            "the queued hashing task holds the transaction outside the budget"
        );

        release_pool
            .send(())
            .map_err(|()| "the held blocking thread stopped waiting")?;
        held_pool.await?;
        let started = std::time::Instant::now();
        while query.broadcast_bytes.try_reserve(1).is_err() {
            if started.elapsed() > Duration::from_secs(5) {
                return Err("the hashing task never returned its bytes".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}
