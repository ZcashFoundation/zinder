//! Wallet query implementation over admitted canonical and wallet stores.

use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion, ChainEpoch,
    ChainEpochId, ChainValuePoolsAtTip, MinedTransaction, MinedTransactionChainContext,
    NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol, TransactionId,
    TransparentAddressBalance, TransparentAddressScriptHash, TransparentAddressTxIndexArtifact,
    TransparentUnspentOutput, TxStatus,
};
use zinder_proto::capabilities::{
    WALLET_BROADCAST_TRANSACTION_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
    WALLET_READ_TRANSACTION_BY_ID_V2, WALLET_READ_TRANSPARENT_OUTPUTS_V1,
    WALLET_READ_TRANSPARENT_SPENDS_V1, WALLET_READ_TRANSPARENT_UNSPENT_OUTPUTS_V1,
    WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
};
use zinder_source::{NodeCapability, NodeSource, TransactionBroadcaster, TreeStateUpstream};
use zinder_store::{
    ArtifactFamily, BlockHashLookup, ChainEventHistoryRequest, ChainEventStreamFamily,
    ChainEventStreamResume, EventStreamStartPosition, RawBlobRetention, StreamCursorTokenV1,
};
use zinder_wallet_projection::{
    WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletCanonicalSourceIdentity,
    WalletProjectionSourcePosition,
};

use crate::{
    AdmittedIngestControl, ArtifactKey, BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents,
    CompactBlock, CompactBlockRange, DEFAULT_MAX_COMPACT_BLOCK_RANGE, DEFAULT_MAX_FULL_BLOCK_RANGE,
    FULL_BLOCK_STREAM_CHANNEL_CAPACITY, FULL_BLOCK_STREAM_SUB_READ_BLOCKS, FullBlock,
    FullBlockStream, NativeWalletEndpointCapabilities, QueryError, RawTransaction, SettledTipBlock,
    SubtreeRoots, Transaction, TransactionStatus, TransparentAddressTxIds,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TreeState, UpstreamNodeCapabilities, VisibleTipBlock,
    WalletQueryApi, WalletServingPairSlot, WalletServingReadPair, join_blocking,
    validate_block_range, validate_subtree_root_range,
};

const WALLET_READ_PAGE_SIZE: NonZeroU16 = NonZeroU16::MAX;
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

    fn resume(
        cursor: &StreamCursorTokenV1,
        expected_source: WalletCanonicalSourceIdentity,
    ) -> Result<WalletAddressTransactionKey, QueryError> {
        let decoded = Self::decode(cursor.as_bytes())?;
        if decoded.source != expected_source {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: decoded.source.source_position().chain_epoch_id,
            });
        }
        Ok(decoded.after)
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
            TransparentHistoryCursor::resume(&first_page_cursor, first_page_source)?,
            first_page_after
        );
        let second_page_cursor =
            TransparentHistoryCursor::issue(first_page_source, second_page_after);
        assert_eq!(
            TransparentHistoryCursor::resume(&second_page_cursor, first_page_source)?,
            second_page_after
        );

        // A pair replacement between pages must never resume its raw key on
        // the replacement fork.
        let Err(error) =
            TransparentHistoryCursor::resume(&first_page_cursor, replacement_pair_source)
        else {
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
        let Err(error) = TransparentHistoryCursor::resume(&malformed, source) else {
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

        let Err(error) = TransparentHistoryCursor::resume(&zero_event_sequence, source) else {
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
        let Err(error) = TransparentHistoryCursor::resume(&zero_digest_block_count, source) else {
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
            descending: false,
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
/// An epoch pin succeeds only when it names the captured pair's current
/// epoch. The `WalletQueryApi` contract permits retained historical epochs,
/// but it does not guarantee them; this secondary composition reports
/// [`QueryError::ChainEpochPinUnavailable`] for every other epoch.
///
/// Every method captures its pair on the async side and then runs the whole
/// store read on the blocking pool. The secondaries use direct I/O, so a cache
/// miss is a synchronous `pread`; keeping one on a reactor thread would stall
/// the ops listener and the gRPC connection drivers sharing that runtime.
#[derive(Clone)]
pub struct WalletServingQuery<Broadcaster, NativeIngest = ()> {
    serving_pair_slot: WalletServingPairSlot,
    broadcaster: Broadcaster,
    native_ingest: NativeIngest,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    tree_state_upstream: Option<Arc<dyn TreeStateUpstream>>,
    native_endpoint_capabilities: NativeWalletEndpointCapabilities,
    upstream_node_capabilities: Option<UpstreamNodeCapabilities>,
}

impl<Broadcaster, NativeIngest> std::fmt::Debug for WalletServingQuery<Broadcaster, NativeIngest> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pair = self.capture_pair();
        formatter
            .debug_struct("WalletServingQuery")
            .field("canonical_fence", &pair.canonical_fence())
            .field("wallet_fence", &pair.wallet_source())
            .field("tree_state_upstream", &self.tree_state_upstream.is_some())
            .field(
                "native_endpoint_capabilities",
                &self.native_endpoint_capabilities,
            )
            .finish_non_exhaustive()
    }
}

impl<Broadcaster> WalletServingQuery<Broadcaster> {
    /// Builds a query over a swappable slot of already-admitted immutable pairs.
    ///
    /// Every [`WalletQueryApi`] method captures one `Arc` from this slot before
    /// reading. A publisher can therefore atomically replace the pair without
    /// changing the canonical or wallet reader observed by an in-flight request.
    #[must_use]
    pub fn from_serving_pair_slot(
        serving_pair_slot: WalletServingPairSlot,
        broadcaster: Broadcaster,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        let raw_blob_retention = serving_pair_slot.capture().canonical().raw_blob_retention();
        let native_endpoint_capabilities =
            NativeWalletEndpointCapabilities::for_wallet_serving_pair(
                raw_blob_retention,
                zinder_source::NodeCapabilities::default(),
            );
        Self {
            serving_pair_slot,
            broadcaster,
            native_ingest: (),
            network_upgrade_activations,
            tree_state_upstream: None,
            native_endpoint_capabilities,
            upstream_node_capabilities: None,
        }
    }
}

impl<Broadcaster> WalletServingQuery<Broadcaster, AdmittedIngestControl> {
    /// Builds the native query from an exact pair and admitted ingest control.
    ///
    /// The resulting type is the only exact-pair instantiation accepted by the
    /// standard native gRPC adapter.
    #[must_use]
    pub fn from_admitted_native_serving_pair(
        serving_pair_slot: WalletServingPairSlot,
        broadcaster: Broadcaster,
        ingest_control: AdmittedIngestControl,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        let raw_blob_retention = serving_pair_slot.capture().canonical().raw_blob_retention();
        let native_endpoint_capabilities =
            NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
                raw_blob_retention,
                zinder_source::NodeCapabilities::default(),
                &ingest_control,
            );
        Self {
            serving_pair_slot,
            broadcaster,
            native_ingest: ingest_control,
            network_upgrade_activations,
            tree_state_upstream: None,
            native_endpoint_capabilities,
            upstream_node_capabilities: None,
        }
    }

    pub(crate) fn admitted_native_ingest(&self) -> &AdmittedIngestControl {
        &self.native_ingest
    }
}

impl<Broadcaster, NativeIngest> WalletServingQuery<Broadcaster, NativeIngest> {
    fn capture_pair(&self) -> Arc<WalletServingReadPair> {
        self.serving_pair_slot.capture()
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

    fn chain_epoch(
        pair: &WalletServingReadPair,
        requested: Option<ChainEpochId>,
    ) -> Result<ChainEpoch, QueryError> {
        let chain_epoch = pair.canonical().chain_epoch()?;
        if requested.is_some_and(|requested| requested != chain_epoch.id) {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: requested.unwrap_or(chain_epoch.id),
            });
        }
        Ok(chain_epoch)
    }

    fn block_id_at(
        pair: &WalletServingReadPair,
        height: BlockHeight,
    ) -> Result<BlockId, QueryError> {
        pair.canonical()
            .block_header_at(height)?
            .map(|header| BlockId::new(height, header.block_hash))
            .ok_or(QueryError::BlockNotInBestChain)
    }

    fn resolve_block_id_by_selector(
        pair: &WalletServingReadPair,
        selector: BlockSelector,
        chain_epoch: ChainEpoch,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        let block_id = match selector {
            BlockSelector::Height(height) if height <= chain_epoch.visible_tip_height => {
                Self::block_id_at(pair, height)?
            }
            BlockSelector::Hash(hash) => match pair.canonical().block_hash_lookup(hash)? {
                BlockHashLookup::Resolved(block_id) => block_id,
                BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => {
                    return Err(QueryError::BlockNotInBestChain);
                }
            },
            BlockSelector::Height(_) => {
                return Err(QueryError::BlockNotInBestChain);
            }
            _ => {
                return Err(QueryError::UnsupportedBlockSelector {
                    reason: "selector is not supported by the canonical reader",
                });
            }
        };
        Ok(BlockIdAtEpoch {
            chain_epoch,
            block_id,
        })
    }
}

impl<Source> WalletServingQuery<Source>
where
    Source: Clone + NodeSource + TransactionBroadcaster + TreeStateUpstream,
{
    /// Builds the shared exact-pair query from one probed node source.
    ///
    /// This state is suitable for the compatibility adapter. The native
    /// endpoint instead requires `from_admitted_native_sources`.
    pub fn from_probed_node_source(
        serving_pair_slot: WalletServingPairSlot,
        source: Source,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Result<Self, QueryError> {
        let node_capabilities = source.capabilities();
        if !node_capabilities.supports(NodeCapability::OpenRpcDiscovery) {
            return Err(QueryError::Node(
                zinder_source::SourceError::NodeCapabilityMissing {
                    capability: NodeCapability::OpenRpcDiscovery,
                },
            ));
        }
        let raw_blob_retention = serving_pair_slot.capture().canonical().raw_blob_retention();
        let native_endpoint_capabilities =
            NativeWalletEndpointCapabilities::for_wallet_serving_pair(
                raw_blob_retention,
                node_capabilities,
            );
        if native_endpoint_capabilities.has_node_backed_capabilities()
            && !node_capabilities.supports(NodeCapability::TipId)
        {
            return Err(QueryError::Node(
                zinder_source::SourceError::NodeCapabilityMissing {
                    capability: NodeCapability::TipId,
                },
            ));
        }
        let shared_source = Arc::new(source.clone());
        let tree_state_upstream: Arc<dyn TreeStateUpstream> = shared_source;
        Ok(Self {
            serving_pair_slot,
            broadcaster: source,
            native_ingest: (),
            network_upgrade_activations,
            tree_state_upstream: Some(tree_state_upstream),
            native_endpoint_capabilities,
            upstream_node_capabilities: Some(UpstreamNodeCapabilities::from_probed(
                node_capabilities,
            )),
        })
    }
}

impl<Source> WalletServingQuery<Source, AdmittedIngestControl>
where
    Source: Clone + NodeSource + TransactionBroadcaster + TreeStateUpstream,
{
    /// Builds the native release query from its exact admitted sources.
    pub fn from_admitted_native_sources(
        serving_pair_slot: WalletServingPairSlot,
        source: Source,
        ingest_control: AdmittedIngestControl,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Result<Self, QueryError> {
        let node_capabilities = source.capabilities();
        if !node_capabilities.supports(NodeCapability::OpenRpcDiscovery) {
            return Err(QueryError::Node(
                zinder_source::SourceError::NodeCapabilityMissing {
                    capability: NodeCapability::OpenRpcDiscovery,
                },
            ));
        }
        let raw_blob_retention = serving_pair_slot.capture().canonical().raw_blob_retention();
        let native_endpoint_capabilities =
            NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
                raw_blob_retention,
                node_capabilities,
                &ingest_control,
            );
        if native_endpoint_capabilities.has_node_backed_capabilities()
            && !node_capabilities.supports(NodeCapability::TipId)
        {
            return Err(QueryError::Node(
                zinder_source::SourceError::NodeCapabilityMissing {
                    capability: NodeCapability::TipId,
                },
            ));
        }
        let shared_source = Arc::new(source.clone());
        let tree_state_upstream: Arc<dyn TreeStateUpstream> = shared_source;
        Ok(Self {
            serving_pair_slot,
            broadcaster: source,
            native_ingest: ingest_control,
            network_upgrade_activations,
            tree_state_upstream: Some(tree_state_upstream),
            native_endpoint_capabilities,
            upstream_node_capabilities: Some(UpstreamNodeCapabilities::from_probed(
                node_capabilities,
            )),
        })
    }
}

#[async_trait]
impl<Broadcaster, NativeIngest> WalletQueryApi for WalletServingQuery<Broadcaster, NativeIngest>
where
    Broadcaster: TransactionBroadcaster + Clone,
    NativeIngest: Send + Sync + 'static,
{
    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        &self.native_endpoint_capabilities
    }

    fn upstream_node_capabilities(&self) -> Option<&UpstreamNodeCapabilities> {
        self.upstream_node_capabilities.as_ref()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        Ok((*self.network_upgrade_activations).clone())
    }

    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, QueryError> {
        Err(unavailable(WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1))
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            Ok(VisibleTipBlock {
                height: chain_epoch.visible_tip_height,
                block_hash: chain_epoch.visible_tip_hash,
                chain_epoch,
            })
        }))
        .await
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            Ok(SettledTipBlock {
                height: chain_epoch.settled_tip_height,
                block_hash: chain_epoch.settled_tip_hash,
                chain_epoch,
            })
        }))
        .await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            Self::resolve_block_id_by_selector(&pair, selector, chain_epoch)
        }))
        .await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let resolved = Self::resolve_block_id_by_selector(&pair, selector, chain_epoch)?;
            let block_header = pair
                .canonical()
                .block_header_at(resolved.block_id.height)?
                .ok_or(QueryError::BlockNotInBestChain)?
                .into_header();
            Ok(BlockHeaderAtEpoch {
                chain_epoch,
                block_header,
            })
        }))
        .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let compact_block = pair
                .canonical()
                .compact_block_at(height)?
                .ok_or_else(|| artifact_unavailable(ArtifactFamily::CompactBlock, height))?;
            Ok(CompactBlock {
                chain_epoch,
                compact_block,
            })
        }))
        .await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: zinder_core::BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        validate_block_range(block_range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let compact_blocks = pair.canonical().compact_blocks_in_range(block_range)?;
            Ok(CompactBlockRange {
                chain_epoch,
                block_range,
                compact_blocks,
            })
        }))
        .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            if !pair.canonical().raw_blob_retention().retains_block_blobs() {
                return Err(unavailable(WALLET_READ_FULL_BLOCK_AT_V1));
            }
            let block_blob = pair
                .canonical()
                .block_blob_at(height)?
                .ok_or_else(|| artifact_unavailable(ArtifactFamily::BlockBlob, height))?;
            Ok(FullBlock {
                chain_epoch,
                block_blob,
            })
        }))
        .await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        validate_block_range(block_range, DEFAULT_MAX_FULL_BLOCK_RANGE)?;
        let pair = self.capture_pair();
        let (pair, chain_epoch) = join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            if !pair.canonical().raw_blob_retention().retains_block_blobs() {
                return Err(unavailable(WALLET_READ_FULL_BLOCK_RANGE_V1));
            }
            Ok((pair, chain_epoch))
        }))
        .await?;
        let (blocks, receiver) = mpsc::channel(FULL_BLOCK_STREAM_CHANNEL_CAPACITY);
        tokio::spawn(drive_serving_pair_full_block_range(
            pair,
            block_range,
            blocks,
        ));
        Ok(FullBlockStream {
            chain_epoch,
            blocks: receiver,
        })
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        let pair = self.capture_pair();
        let network_upgrade_activations = Arc::clone(&self.network_upgrade_activations);
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            // READY admission requires transaction-location coverage to equal
            // the authenticated source count for every retention mode. Blob
            // coverage equals that count only under Transactions/All and is
            // zero under None; live append and replacement update the retained
            // families in one canonical batch. Under that admitted contract,
            // absence is a real miss.
            let Some(location) = pair.canonical().transaction_location(transaction_id)? else {
                crate::require_complete_transaction_history(pair.canonical().history_bounds())?;
                return Ok(TransactionStatus {
                    chain_epoch,
                    status: TxStatus::NotFound,
                });
            };
            let header = pair
                .canonical()
                .block_header_at(location.block_height)?
                .ok_or(QueryError::BlockNotInBestChain)?;
            let raw_transaction_bytes = pair
                .canonical()
                .transaction_blob(location)?
                .map(|blob| blob.raw_transaction_bytes);
            let chain_context = MinedTransactionChainContext::from_response_epoch(
                &chain_epoch,
                location.block_height,
                network_upgrade_activations.consensus_branch_id_at(location.block_height),
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
        }))
        .await
    }

    async fn transaction_at_block_index(
        &self,
        _height: BlockHeight,
        _tx_index: u64,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        Err(QueryError::MaterializedViewUnavailable {
            capability: WALLET_READ_TRANSACTION_BY_ID_V2,
        })
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let location = pair
                .canonical()
                .transaction_location(transaction_id)?
                .ok_or_else(|| QueryError::ArtifactUnavailable {
                    family: ArtifactFamily::TransactionLocation,
                    key: ArtifactKey::TransactionId(transaction_id),
                })?;
            let transaction = pair
                .canonical()
                .transaction_blob(location)?
                .ok_or_else(|| QueryError::ArtifactUnavailable {
                    family: ArtifactFamily::TransactionBlob,
                    key: ArtifactKey::TransactionId(transaction_id),
                })?;
            Ok(RawTransaction {
                chain_epoch,
                transaction,
            })
        }))
        .await
    }

    async fn transparent_outputs_by_outpoint(
        &self,
        _outpoints: Vec<zinder_core::TransparentOutPoint>,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<zinder_core::TransparentOutputsByOutpointResponse, QueryError> {
        Err(unavailable(WALLET_READ_TRANSPARENT_OUTPUTS_V1))
    }

    async fn transparent_spends_by_outpoint(
        &self,
        _outpoints: Vec<zinder_core::TransparentOutPoint>,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<zinder_core::TransparentSpendsByOutpointResponse, QueryError> {
        Err(unavailable(WALLET_READ_TRANSPARENT_SPENDS_V1))
    }

    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        _outpoints: Vec<zinder_core::TransparentOutPoint>,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<zinder_core::TransparentUnspentOutputsByOutpointResponse, QueryError> {
        Err(unavailable(WALLET_READ_TRANSPARENT_UNSPENT_OUTPUTS_V1))
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let mut outputs = Vec::new();
            let mut after: Option<WalletAddressUnspentOutputKey> = None;
            loop {
                let page = pair.wallet().address_unspent_outputs_page_from_height(
                    request.address_script_hash,
                    request.start_height,
                    after,
                    WALLET_READ_PAGE_SIZE,
                )?;
                outputs.extend(page.outputs.into_iter().map(|output| {
                    TransparentUnspentOutput::new(
                        output.address_script_hash,
                        output.script_pub_key,
                        output.outpoint,
                        output.value_zat,
                        output.created_at.block.height,
                        output.created_at.block.hash,
                    )
                }));
                let Some(next) = page.next_page_after else {
                    break;
                };
                after = Some(next);
            }
            Ok(TransparentAddressUnspentOutputs {
                chain_epoch,
                outputs,
            })
        }))
        .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        if request.descending {
            return Err(QueryError::MaterializedViewUnavailable {
                capability: "descending canonical transparent history",
            });
        }
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, None)?;
            let after = request
                .from_cursor
                .as_ref()
                .map(|cursor| TransparentHistoryCursor::resume(cursor, pair.wallet_source()))
                .transpose()?;
            if let Some(after) = after {
                validate_transparent_history_cursor_key(after, &request)?;
            }
            let page_size =
                NonZeroU16::new(u16::try_from(request.max_entries.get()).unwrap_or(u16::MAX))
                    .unwrap_or(NonZeroU16::MAX);
            let page = pair.wallet().address_transaction_history_range_page(
                request.address_script_hash,
                BlockHeightRange::inclusive(request.start_height, request.end_height),
                after,
                page_size,
            )?;
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
        }))
        .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        let requested_address_count = addresses.len();
        let address_count = u32::try_from(requested_address_count)
            .ok()
            .filter(|count| (1..=crate::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES).contains(count));
        if address_count.is_none() {
            return Err(QueryError::TransparentBalanceAddressCountExceeded {
                requested: requested_address_count,
                maximum: crate::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
            });
        }
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let addresses: HashSet<_> = addresses.into_iter().collect();
            let mut confirmed_zat = 0_u64;
            for address in &addresses {
                confirmed_zat =
                    confirmed_zat.saturating_add(pair.wallet().address_balance(*address)?);
            }
            Ok(TransparentAddressBalance {
                confirmed_zat,
                unconfirmed_delta_zat: 0,
                address_count: u32::try_from(addresses.len()).unwrap_or(u32::MAX),
                chain_epoch,
            })
        }))
        .await
    }

    async fn transparent_utxo_set_summary(
        &self,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<zinder_core::TransparentUtxoSetSummary, QueryError> {
        Err(unavailable(WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1))
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        if !self
            .native_endpoint_capabilities
            .contains(WALLET_READ_TREE_STATE_AT_HEIGHT_V2)
        {
            return Err(unavailable(WALLET_READ_TREE_STATE_AT_HEIGHT_V2));
        }
        let pair = self.capture_pair();
        let resolved = join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let checkpoint = pair
                .canonical()
                .tree_state_checkpoint_at_or_before(height)?;
            if let Some(checkpoint) =
                checkpoint.filter(|checkpoint| checkpoint.block_id.height == height)
            {
                return tree_state_from_checkpoint(chain_epoch, &checkpoint)
                    .map(ResolvedTreeState::Checkpoint);
            }
            let block_header = pair
                .canonical()
                .block_header_at(height)?
                .ok_or(QueryError::BlockNotInBestChain)?;
            let block_time_seconds = u32::try_from(block_header.block_time).map_err(|_| {
                QueryError::ArtifactCorrupt {
                    family: ArtifactFamily::BlockHeader,
                    reason: "canonical block time is outside the u32 range".to_owned(),
                }
            })?;
            Ok(ResolvedTreeState::UpstreamFill {
                chain_epoch,
                block_id: BlockId::new(height, block_header.block_hash),
                block_time_seconds,
            })
        }))
        .await?;
        let (chain_epoch, block_id, block_time_seconds) = match resolved {
            ResolvedTreeState::Checkpoint(tree_state) => return Ok(tree_state),
            ResolvedTreeState::UpstreamFill {
                chain_epoch,
                block_id,
                block_time_seconds,
            } => (chain_epoch, block_id, block_time_seconds),
        };
        let upstream = self
            .tree_state_upstream
            .as_ref()
            .ok_or_else(|| artifact_unavailable(ArtifactFamily::TreeState, height))?;
        let source = upstream.fetch_tree_state_for_block(block_id).await?;
        if source.block_id != block_id {
            return Err(QueryError::Node(
                zinder_source::SourceError::SourceProtocolMismatch {
                    reason: "tree-state source identity does not match the canonical block",
                },
            ));
        }
        if source.block_time_seconds != block_time_seconds {
            return Err(QueryError::Node(
                zinder_source::SourceError::SourceProtocolMismatch {
                    reason: "tree-state source time does not match the canonical block",
                },
            ));
        }
        Ok(TreeState {
            chain_epoch,
            height,
            block_hash: block_id.hash,
            block_time_seconds,
            payload_bytes: source.payload_bytes,
        })
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
            let checkpoint = pair
                .canonical()
                .tree_state_checkpoint_at_or_before(chain_epoch.visible_tip_height)?
                .ok_or_else(|| {
                    artifact_unavailable(ArtifactFamily::TreeState, chain_epoch.visible_tip_height)
                })?;
            tree_state_from_checkpoint(chain_epoch, &checkpoint)
        }))
        .await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: zinder_core::SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        validate_subtree_root_range(subtree_root_range)?;
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = Self::chain_epoch(&pair, at_epoch_id)?;
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
            let available_entries = NonZeroU32::new(available_entries).ok_or_else(|| {
                QueryError::ArtifactUnavailable {
                    family: ArtifactFamily::SubtreeRoot,
                    key: ArtifactKey::SubtreeRootIndex {
                        protocol: subtree_root_range.protocol,
                        index: subtree_root_range.start_index,
                    },
                }
            })?;
            let available_range = zinder_core::SubtreeRootRange::new(
                subtree_root_range.protocol,
                subtree_root_range.start_index,
                available_entries,
            );
            let subtree_roots = pair.canonical().subtree_roots(available_range)?;
            Ok(SubtreeRoots {
                chain_epoch,
                protocol: subtree_root_range.protocol,
                start_index: subtree_root_range.start_index,
                subtree_roots,
            })
        }))
        .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            let event_envelopes = pair
                .canonical()
                .wallet_chain_event_history(ChainEventHistoryRequest::new_for_family(
                    from_cursor.as_ref(),
                    family,
                    crate::DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS,
                ))
                .map_err(map_canonical_chain_event_error)?;
            Ok(ChainEvents { event_envelopes })
        }))
        .await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        let pair = self.capture_pair();
        join_blocking(tokio::task::spawn_blocking(move || {
            pair.canonical()
                .resolve_wallet_chain_event_stream_start(&start, requested_family)
                .map_err(map_canonical_chain_event_error)
        }))
        .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<zinder_core::TransactionBroadcastOutcome, QueryError> {
        if !self
            .native_endpoint_capabilities
            .contains(WALLET_BROADCAST_TRANSACTION_V1)
        {
            return Err(unavailable(WALLET_BROADCAST_TRANSACTION_V1));
        }
        let _pair = self.capture_pair();
        if raw_transaction.len() > zinder_core::MAX_RAW_TRANSACTION_BYTES {
            return Err(QueryError::BroadcastTransactionTooLarge {
                actual: raw_transaction.len(),
                maximum: zinder_core::MAX_RAW_TRANSACTION_BYTES,
            });
        }
        self.broadcaster
            .broadcast_transaction(raw_transaction)
            .await
            .map_err(QueryError::Node)
    }
}

/// Streams one range while retaining the exact admitted pair captured by the
/// request. Each `RocksDB` multi-get runs in a short blocking task; client
/// backpressure is bounded by the response channel.
async fn drive_serving_pair_full_block_range(
    pair: Arc<WalletServingReadPair>,
    block_range: BlockHeightRange,
    block_sender: mpsc::Sender<Result<zinder_core::BlockBlobArtifact, QueryError>>,
) {
    let last_height = block_range.end.value();
    let mut sub_read_start = block_range.start.value();
    loop {
        let sub_read_end = sub_read_start
            .saturating_add(FULL_BLOCK_STREAM_SUB_READ_BLOCKS - 1)
            .min(last_height);
        let sub_range = BlockHeightRange::inclusive(
            BlockHeight::new(sub_read_start),
            BlockHeight::new(sub_read_end),
        );
        let pair_for_read = Arc::clone(&pair);
        let block_blobs = match tokio::task::spawn_blocking(move || {
            pair_for_read.canonical().block_blobs_in_range(sub_range)
        })
        .await
        {
            Ok(Ok(block_blobs)) => block_blobs,
            Ok(Err(error)) => {
                let _ = block_sender
                    .send(Err(QueryError::CanonicalStore(error)))
                    .await;
                return;
            }
            Err(error) => {
                let _ = block_sender
                    .send(Err(QueryError::BlockingTaskFailed {
                        reason: error.to_string(),
                    }))
                    .await;
                return;
            }
        };
        if !forward_serving_pair_full_block_sub_range(&block_sender, sub_range, block_blobs).await {
            return;
        }
        if sub_read_end == last_height {
            return;
        }
        sub_read_start = sub_read_end.saturating_add(1);
    }
}

async fn forward_serving_pair_full_block_sub_range(
    block_sender: &mpsc::Sender<Result<zinder_core::BlockBlobArtifact, QueryError>>,
    sub_range: BlockHeightRange,
    block_blobs: Vec<Option<zinder_core::BlockBlobArtifact>>,
) -> bool {
    let expected_count = sub_range.into_iter().len();
    if block_blobs.len() != expected_count {
        let _ = block_sender
            .send(Err(QueryError::ArtifactCorrupt {
                family: ArtifactFamily::BlockBlob,
                reason: format!(
                    "full-block range returned {} rows for {expected_count} requested heights",
                    block_blobs.len()
                ),
            }))
            .await;
        return false;
    }
    for (height, block_blob) in sub_range.into_iter().zip(block_blobs) {
        let Some(block_blob) = block_blob else {
            let _ = block_sender
                .send(Err(artifact_unavailable(ArtifactFamily::BlockBlob, height)))
                .await;
            return false;
        };
        if block_sender.send(Ok(block_blob)).await.is_err() {
            return false;
        }
    }
    true
}

fn unavailable(capability: &'static str) -> QueryError {
    QueryError::EndpointCapabilityUnavailable { capability }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "CanonicalStoreError is non-exhaustive; unrelated store failures retain their typed source"
)]
fn map_canonical_chain_event_error(error: zinder_store::CanonicalStoreError) -> QueryError {
    match error {
        zinder_store::CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        } => QueryError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        },
        zinder_store::CanonicalStoreError::CanonicalEventCursorMalformed { reason } => {
            QueryError::ChainEventCursorInvalid { reason }
        }
        zinder_store::CanonicalStoreError::CanonicalEventCursorUnknownVersion { .. } => {
            QueryError::ChainEventCursorInvalid {
                reason: "cursor version is unsupported",
            }
        }
        other => QueryError::CanonicalStore(other),
    }
}

fn artifact_unavailable(family: ArtifactFamily, height: BlockHeight) -> QueryError {
    QueryError::ArtifactUnavailable {
        family,
        key: ArtifactKey::BlockHeight(height),
    }
}

/// Outcome of the canonical half of a tree-state read.
///
/// A sparse height resolves its payload from the node, so the canonical reads
/// that identify the block finish before the request leaves the blocking pool.
enum ResolvedTreeState {
    /// The canonical store held a checkpoint at the exact requested height.
    Checkpoint(TreeState),
    /// The block is canonical but its payload must come from the node.
    UpstreamFill {
        /// Epoch the canonical reads were pinned to.
        chain_epoch: ChainEpoch,
        /// Canonical identity the node response must match.
        block_id: BlockId,
        /// Canonical block time the node response must match.
        block_time_seconds: u32,
    },
}

fn tree_state_from_checkpoint(
    chain_epoch: ChainEpoch,
    checkpoint: &zinder_core::CommitmentTreeCheckpoint,
) -> Result<TreeState, QueryError> {
    let mut payload = Map::new();
    for (name, protocol) in [
        ("sapling", ShieldedProtocol::Sapling),
        ("orchard", ShieldedProtocol::Orchard),
        ("ironwood", ShieldedProtocol::Ironwood),
    ] {
        if let Some(frontier) = checkpoint.frontiers.get(protocol) {
            payload.insert(
                name.to_owned(),
                json!({
                    "commitments": {
                        "finalState": hex::encode(frontier.final_state_bytes())
                    }
                }),
            );
        }
    }
    let payload_bytes = serde_json::to_vec(&Value::Object(payload)).map_err(|source| {
        QueryError::ArtifactCorrupt {
            family: ArtifactFamily::TreeState,
            reason: source.to_string(),
        }
    })?;
    Ok(TreeState {
        chain_epoch,
        height: checkpoint.block_id.height,
        block_hash: checkpoint.block_id.hash,
        block_time_seconds: checkpoint.block_time_seconds,
        payload_bytes,
    })
}
