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

        let _slot = self.slots.try_acquire().map_err(|_| {
            QueryError::Node(SourceError::NodeUnavailable {
                reason: "cold transaction fetch slots are full; retry with backoff".to_owned(),
            })
        })?;
        let target = VerificationTarget::from_canonical(reader).map_err(QueryError::Node)?;
        self.verification
            .verify(&self.node, &target)
            .await
            .map_err(QueryError::Node)?;
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
