//! Bounded reads of mined transaction bytes from the verified primary node.

use zinder_core::TransactionLocation;
use zinder_source::ZebraJsonRpcSource;

use crate::{
    CanonicalReader, ForwardedReadKind, ForwardedReadLimit, MinedTransactionDigest, QueryError,
    node_broadcast::{LightWalletNodeVerification, VerificationTarget},
};

/// Reads mined transaction bytes the store omits from the primary node,
/// through the process's forwarded-read limit and its verified byte cache.
pub(crate) struct ColdTransactionSource {
    node: ZebraJsonRpcSource,
    verification: LightWalletNodeVerification,
    reads: ForwardedReadLimit,
}

impl ColdTransactionSource {
    pub(crate) fn new(node: ZebraJsonRpcSource, reads: ForwardedReadLimit) -> Self {
        Self {
            node,
            verification: LightWalletNodeVerification::default(),
            reads,
        }
    }

    /// Verifies the node against the served schedule and tip of `reader`, so
    /// a caller that consults live state after a canonical miss knows the
    /// node follows the served chain.
    pub(crate) async fn verify_served_tip(
        &self,
        reader: &dyn CanonicalReader,
    ) -> Result<(), QueryError> {
        let target = VerificationTarget::from_canonical(reader).map_err(QueryError::Node)?;
        self.verification
            .verify(&self.node, &target)
            .await
            .map_err(QueryError::Node)
    }

    /// Returns the bytes the served chain mines at `location`, from the cache
    /// or verified against the node's containing block and the committed
    /// digest.
    pub(crate) async fn fetch(
        &self,
        reader: &dyn CanonicalReader,
        location: TransactionLocation,
    ) -> Result<Vec<u8>, QueryError> {
        let mined = MinedTransactionDigest::from_location(location)?;
        if let Some(bytes) = self.reads.cached_transaction(&mined) {
            return Ok(bytes.to_vec());
        }
        let deadline = self.reads.deadline();
        let target = VerificationTarget::from_canonical(reader).map_err(QueryError::Node)?;
        // The node reports the block that mines the transaction, and the bytes
        // must match the digest the served chain committed, so the reply binds
        // itself to the served view without a separate tip check.
        self.verification
            .verify_schedule_and_capabilities(&self.node, &target)
            .await
            .map_err(QueryError::Node)?;
        let bytes = self
            .reads
            .mined_transaction_bytes(&self.node, mined, ForwardedReadKind::Transaction, deadline)
            .await?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::Arc, time::Duration};

    use eyre::{Result, eyre};
    use parking_lot::{Condvar, Mutex};
    use serde_json::json;
    use tokio::{sync::mpsc, task::JoinSet, time::timeout};
    use zinder_core::{
        BlockHeight, BlockId, Network, NetworkUpgradeActivations, SerializedBytesDigest,
        TransactionId,
        wire::{encode_branch_id_hex, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex},
    };
    use zinder_source::NodeAuth;
    use zinder_testkit::{
        ChainFixture, JsonRpcStub, JsonRpcTestServer, RpcReply, WalletServingStoreFixture, method,
        sample_regtest_upgrade_activations,
    };

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one scenario walks admission, queueing, refusal, cancellation, and caching in order"
    )]
    async fn cold_fetches_beyond_the_share_queue_and_a_cancelled_slot_goes_to_the_oldest()
    -> Result<()> {
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
        let replacement = location(9);
        let replacement_txid = encode_rpc_transaction_id_hex(replacement.transaction_id);
        let (entered, mut calls) = mpsc::unbounded_channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let stub =
            blocked_transaction_stub(tip, replacement_txid.clone(), entered, Arc::clone(&gate));
        let node = cold_fetch_node(&schedule, tip, stub)?;
        // Four node-call slots give transaction lookups a cap of two and, at
        // one waiter per slot, two waiting places.
        let limit = ForwardedReadLimit::new(crate::ForwardedReadConfig::resolve(
            Some(4),
            Some(10_000),
            Some(1),
            None,
        )?);
        let source = Arc::new(ColdTransactionSource::new(
            ZebraJsonRpcSource::new(
                Network::ZcashRegtest,
                node.url(),
                NodeAuth::None,
                Duration::from_secs(20),
            )?,
            limit.clone(),
        ));
        let counts = || limit.node_call_counts(ForwardedReadKind::Transaction);
        let settle = |expected| async move {
            timeout(Duration::from_secs(5), async {
                while counts() != expected {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| eyre!("transaction slots never reached {expected:?}"))
        };
        let mut misses = JoinSet::new();
        let mut spawn_miss = |byte| {
            let (source, reader, location) = (source.clone(), reader.clone(), location(byte));
            misses.spawn(async move { source.fetch(reader.as_ref(), location).await })
        };
        let first = spawn_miss(0);
        spawn_miss(1);
        for _ in 0..2 {
            timeout(Duration::from_secs(10), calls.recv())
                .await?
                .ok_or_else(|| eyre!("node never received the cold request"))?;
        }
        spawn_miss(2);
        settle((2, 1)).await?;
        spawn_miss(3);
        settle((2, 2)).await?;

        let excess = timeout(
            Duration::from_secs(5),
            source.fetch(reader.as_ref(), location(4)),
        )
        .await?;
        assert!(
            matches!(
                excess,
                Err(QueryError::ForwardedReadLimitReached {
                    kind: "transaction",
                    max_concurrent: 2
                })
            ),
            "{excess:?}"
        );
        assert_eq!(calls.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        first.abort();
        assert!(matches!(misses.join_next().await, Some(Err(error)) if error.is_cancelled()));
        assert_eq!(
            timeout(Duration::from_secs(5), calls.recv()).await?,
            Some(encode_rpc_transaction_id_hex(location(2).transaction_id)),
            "the freed slot did not go to the oldest waiting miss"
        );

        let (released, ready) = &*gate;
        *released.lock() = true;
        ready.notify_all();
        while let Some(reply) = misses.join_next().await {
            assert_eq!(reply??, [0x55]);
        }
        assert_eq!(
            calls.try_recv()?,
            encode_rpc_transaction_id_hex(location(3).transaction_id)
        );
        assert_eq!(
            timeout(
                Duration::from_secs(5),
                source.fetch(reader.as_ref(), replacement)
            )
            .await??,
            [0x55]
        );
        assert_eq!(calls.try_recv()?, replacement_txid);
        assert_eq!(
            source.fetch(reader.as_ref(), replacement).await?,
            [0x55],
            "a cached transaction was refetched"
        );
        assert_eq!(calls.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(counts(), (0, 0));
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
        .with_verified_cold_transactions(crate::ForwardedReadLimit::default());
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
