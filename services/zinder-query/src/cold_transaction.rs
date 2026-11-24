//! Bounded reads of mined transaction bytes from the verified primary node.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use parking_lot::Mutex;
use tokio::sync::Semaphore;
use zinder_core::{SerializedBytesDigest, TransactionId, TransactionLocation};
use zinder_source::{SourceError, ZebraJsonRpcSource};

use crate::{
    CanonicalReader, MinedTransactionDigest, QueryError,
    node_broadcast::{LightWalletNodeVerification, VerificationTarget},
};

const FETCH_SLOTS: usize = 32;
const CACHE_BYTES: usize = 64 * 1024 * 1024;

pub(crate) struct ColdTransactionSource {
    node: ZebraJsonRpcSource,
    verification: LightWalletNodeVerification,
    slots: Semaphore,
    cache: Mutex<TransactionCache>,
}

impl ColdTransactionSource {
    pub(crate) fn new(node: ZebraJsonRpcSource) -> Self {
        Self {
            node,
            verification: LightWalletNodeVerification::default(),
            slots: Semaphore::new(FETCH_SLOTS),
            cache: Mutex::default(),
        }
    }

    /// Verifies the node against the served tip of `reader`, so a caller
    /// that consults live state after a canonical miss knows the node follows
    /// the served chain.
    pub(crate) async fn verify_served_tip(
        &self,
        reader: &dyn CanonicalReader,
    ) -> Result<(), QueryError> {
        let _slot = self.acquire_slot()?;
        self.verify_target(reader).await
    }

    fn acquire_slot(&self) -> Result<tokio::sync::SemaphorePermit<'_>, QueryError> {
        self.slots.try_acquire().map_err(|_| {
            QueryError::Node(SourceError::NodeUnavailable {
                reason: "cold transaction fetch slots are full; retry with backoff".to_owned(),
            })
        })
    }

    async fn verify_target(&self, reader: &dyn CanonicalReader) -> Result<(), QueryError> {
        let target = VerificationTarget::from_canonical(reader).map_err(QueryError::Node)?;
        self.verification
            .verify(&self.node, &target)
            .await
            .map_err(QueryError::Node)
    }

    pub(crate) async fn fetch(
        &self,
        reader: &dyn CanonicalReader,
        location: TransactionLocation,
    ) -> Result<Vec<u8>, QueryError> {
        let mined = MinedTransactionDigest::from_location(location)?;
        let key = (location.transaction_id, mined.serialized_bytes_digest);
        let cached = self.cache.lock().get(key);
        if let Some(bytes) = cached {
            return Ok(bytes.to_vec());
        }

        let _slot = self.acquire_slot()?;
        self.verify_target(reader).await?;
        let (block, bytes) = self
            .node
            .fetch_mined_transaction_bytes(location.transaction_id)
            .await
            .map_err(QueryError::Node)?
            .ok_or_else(|| {
                QueryError::Node(SourceError::NodeUnavailable {
                    reason: "node does not mine the transaction in the served block".to_owned(),
                })
            })?;
        mined.verify_node_bytes(block, &bytes)?;
        self.cache
            .lock()
            .insert(key, Arc::from(bytes.as_slice()), CACHE_BYTES);
        Ok(bytes)
    }
}

type CacheKey = (TransactionId, SerializedBytesDigest);

#[derive(Default)]
struct TransactionCache {
    entries: HashMap<CacheKey, (u64, Arc<[u8]>)>,
    lru: BTreeMap<u64, CacheKey>,
    clock: u64,
    bytes: usize,
}

impl TransactionCache {
    fn get(&mut self, key: CacheKey) -> Option<Arc<[u8]>> {
        let (tick, bytes) = self.entries.get_mut(&key)?;
        self.lru.remove(tick);
        self.clock += 1;
        *tick = self.clock;
        self.lru.insert(*tick, key);
        Some(Arc::clone(bytes))
    }

    fn insert(&mut self, key: CacheKey, bytes: Arc<[u8]>, capacity: usize) {
        if bytes.len() > capacity || self.get(key).is_some() {
            return;
        }
        while self.bytes + bytes.len() > capacity {
            let Some((_, oldest)) = self.lru.pop_first() else {
                break;
            };
            if let Some((_, removed)) = self.entries.remove(&oldest) {
                self.bytes -= removed.len();
            }
        }
        self.clock += 1;
        self.bytes += bytes.len();
        self.lru.insert(self.clock, key);
        self.entries.insert(key, (self.clock, bytes));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{pin::pin, task::Poll, time::Duration};

    use eyre::{Result, eyre};
    use parking_lot::Condvar;
    use serde_json::json;
    use tokio::{sync::mpsc, task::JoinSet, time::timeout};
    use zinder_core::{
        BlockHeight, BlockId, Network, NetworkUpgradeActivations,
        wire::{encode_branch_id_hex, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex},
    };
    use zinder_source::NodeAuth;
    use zinder_testkit::{
        ChainFixture, JsonRpcStub, JsonRpcTestServer, RpcReply, WalletServingStoreFixture, method,
        sample_regtest_upgrade_activations,
    };

    #[tokio::test]
    async fn cold_fetch_refuses_a_thirty_third_miss_and_releases_a_cancelled_slot() -> Result<()> {
        let (_store, reader, schedule) = cold_fetch_fixture()?;
        let tip = reader.event_fence().visible_tip();
        let location = |byte| TransactionLocation {
            serialized_bytes_sha256: Some(
                SerializedBytesDigest::from_serialized_bytes(&[0x55]).as_bytes(),
            ),
            ..TransactionLocation::new(
                TransactionId::from_bytes([byte; 32]),
                tip.height,
                tip.hash,
                0,
            )
        };
        let replacement = location(33);
        let replacement_txid = encode_rpc_transaction_id_hex(replacement.transaction_id);
        let (entered, mut calls) = mpsc::unbounded_channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let stub =
            blocked_transaction_stub(tip, replacement_txid.clone(), entered, Arc::clone(&gate));
        let node = cold_fetch_node(&schedule, tip, stub)?;
        let source = Arc::new(ColdTransactionSource::new(ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            node.url(),
            NodeAuth::None,
            Duration::from_secs(20),
        )?));
        let mut misses = JoinSet::new();
        let mut spawn_miss = |byte| {
            let (source, reader, location) = (source.clone(), reader.clone(), location(byte));
            misses.spawn(async move { source.fetch(reader.as_ref(), location).await })
        };
        let first = spawn_miss(0);
        for byte in 1..32 {
            spawn_miss(byte);
        }
        timeout(Duration::from_secs(10), async {
            for _ in 0..32 {
                calls
                    .recv()
                    .await
                    .ok_or_else(|| eyre!("node never received the cold request"))?;
            }
            Ok::<_, eyre::Report>(())
        })
        .await??;

        let mut excess = pin!(source.fetch(reader.as_ref(), location(32)));
        assert!(matches!(futures_util::poll!(&mut excess),
            Poll::Ready(Err(QueryError::Node(SourceError::NodeUnavailable { reason })))
                if reason.contains("cold transaction fetch slots are full")
        ));
        assert_eq!(calls.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        first.abort();
        assert!(matches!(misses.join_next().await, Some(Err(error)) if error.is_cancelled()));
        assert_eq!(
            timeout(
                Duration::from_secs(5),
                source.fetch(reader.as_ref(), replacement)
            )
            .await??,
            [0x55]
        );
        assert_eq!(calls.try_recv()?, replacement_txid);
        assert_eq!(calls.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        let (released, ready) = &*gate;
        *released.lock() = true;
        ready.notify_all();
        while let Some(reply) = misses.join_next().await {
            assert_eq!(reply??, [0x55]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn revoking_the_pair_releases_a_transaction_stalled_at_the_node() -> Result<()> {
        use crate::{
            OrderedBroadcaster, RocksDbCanonicalReader, RocksDbWalletReader, WalletQueryApi,
            WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
        };
        use zinder_store::RawBlobRetention;
        use zinder_testkit::FixtureTransactionRows;

        let schedule = verified_node_schedule()?;
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::None)
            .extend_blocks(1);
        let block = chain
            .blocks()
            .first()
            .ok_or_else(|| eyre!("missing fixture block"))?;
        let id = TransactionId::from_bytes([0x55; 32]);
        let rows = FixtureTransactionRows::coinbase(id, block.height, block.hash, vec![0x55]);
        let mut store =
            WalletServingStoreFixture::from_chain(&chain.with_transaction_rows(rows), &schedule)?;
        let (canonical, wallet) = store.take_readers()?;
        let pair = Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?);
        let epoch = pair.canonical_fence().chain_epoch_id();
        let (entered, mut calls) = mpsc::unbounded_channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let tip = pair.canonical_fence().visible_tip();
        let stub = blocked_transaction_stub(tip, String::new(), entered, Arc::clone(&gate));
        let node = cold_fetch_node(&schedule, tip, stub)?;
        let source = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            node.url(),
            NodeAuth::None,
            Duration::from_secs(20),
        )?;
        let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
            Arc::clone(&pair),
        ))
        .with_node_broadcast(Some(OrderedBroadcaster::new(source, [])))
        .with_verified_cold_transactions();
        let idle_owners = Arc::strong_count(&pair);
        let mut requests = JoinSet::new();
        let stalled_query = query.clone();
        requests.spawn(async move { stalled_query.transaction(id, Some(epoch)).await });
        assert_eq!(
            timeout(Duration::from_secs(5), calls.recv()).await?,
            Some(encode_rpc_transaction_id_hex(id)),
        );
        assert!(Arc::strong_count(&pair) > idle_owners);

        assert!(pair.revoke_reads());
        let result = timeout(Duration::from_secs(2), requests.join_next()).await;
        // The stub stays blocked until the query has returned or the test deadline expires.
        let (released, ready) = &*gate;
        *released.lock() = true;
        ready.notify_all();

        let failure = result?.ok_or_else(|| eyre!("cold read task disappeared"))??;
        assert!(
            matches!(failure, Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == epoch),
            "unexpected transaction response: {failure:?}",
        );
        assert_eq!(Arc::strong_count(&pair), idle_owners);
        assert!(matches!(
            query.transaction(id, Some(epoch)).await,
            Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == epoch
        ));
        assert_eq!(calls.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        Ok(())
    }

    fn cold_fetch_fixture() -> Result<(
        WalletServingStoreFixture,
        Arc<crate::RocksDbCanonicalReader>,
        NetworkUpgradeActivations,
    )> {
        let schedule = verified_node_schedule()?;
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let mut store = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
        let reader = Arc::new(crate::RocksDbCanonicalReader::new(store.take_readers()?.0)?);
        Ok((store, reader, schedule))
    }

    fn verified_node_schedule() -> Result<NetworkUpgradeActivations> {
        Ok(NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            sample_regtest_upgrade_activations()
                .activations()
                .iter()
                .zip(1..)
                .map(
                    |(activation, height)| zinder_core::NetworkUpgradeActivation {
                        activation_height: BlockHeight::new(height),
                        ..activation.clone()
                    },
                )
                .collect(),
        )?)
    }

    fn blocked_transaction_stub(
        tip: BlockId,
        replacement_txid: String,
        entered: mpsc::UnboundedSender<String>,
        node_gate: Arc<(Mutex<bool>, Condvar)>,
    ) -> JsonRpcStub {
        method("getrawtransaction").reply_with(move |params| {
            // The stub server records the request only after the reply handler returns.
            let _ = entered.send(params[0].as_str().unwrap_or_default().to_owned());
            if params[0] != replacement_txid {
                let (released, ready) = &*node_gate;
                let mut released = released.lock();
                ready.wait_while_for(
                    &mut released,
                    |released| !*released,
                    Duration::from_secs(20),
                );
                if !*released {
                    return RpcReply::error("cold fetch fixture timed out");
                }
            }
            RpcReply::result(json!({
                "txid": params[0], "hex": "55", "height": tip.height.value(),
                "blockhash": encode_rpc_block_hash_hex(tip.hash),
            }))
        })
    }

    fn cold_fetch_node(
        schedule: &NetworkUpgradeActivations,
        tip: BlockId,
        fetch: JsonRpcStub,
    ) -> Result<JsonRpcTestServer> {
        let upgrades: serde_json::Map<_, _> = schedule
            .activations()
            .iter()
            .map(|activation| {
                (
                    encode_branch_id_hex(activation.branch_id),
                    json!({"name": activation.name, "status": "active",
                    "activationheight": activation.activation_height.value()}),
                )
            })
            .collect();
        JsonRpcTestServer::start([
            method("rpc.discover").reply_with(|_| {
                RpcReply::result(json!({"methods": [{"name": "sendrawtransaction"}]}))
            }),
            method("getblockchaininfo")
                .reply_with(move |_| RpcReply::result(json!({"upgrades": upgrades}))),
            method("getblockhash")
                .reply_with(move |_| RpcReply::result(json!(encode_rpc_block_hash_hex(tip.hash)))),
            fetch,
        ])
    }

    #[test]
    fn cache_evicts_by_bytes_and_recency_and_separates_digests() {
        let mut cache = TransactionCache::default();
        let key = |byte| {
            (
                TransactionId::from_bytes([1; 32]),
                SerializedBytesDigest::from_bytes([byte; 32]),
            )
        };
        cache.insert(key(1), Arc::from([1, 1]), 4);
        cache.insert(key(2), Arc::from([2, 2]), 4);
        assert_eq!(cache.get(key(1)).as_deref(), Some([1, 1].as_slice()));
        cache.insert(key(3), Arc::from([3, 3]), 4);
        assert!(cache.get(key(2)).is_none());
        assert_eq!(cache.get(key(3)).as_deref(), Some([3, 3].as_slice()));
        cache.insert(key(4), Arc::from([4; 5]), 4);
        assert!(cache.get(key(4)).is_none());
        assert_eq!(cache.bytes, 4);
    }

    #[test]
    fn missing_indexed_digest_is_data_loss() {
        let location = TransactionLocation::new(
            TransactionId::from_bytes([1; 32]),
            zinder_core::BlockHeight::new(1),
            zinder_core::BlockHash::from_bytes([2; 32]),
            0,
        );
        assert!(matches!(
            MinedTransactionDigest::from_location(location),
            Err(error) if crate::status_from_query_error(&error).code() == tonic::Code::DataLoss
        ));
    }
}
