//! Store key byte contract.

/// Ordered key bytes used inside `RocksDB` column families.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StoreKey(Vec<u8>);

const KEY_VERSION: u8 = 1;
const MEMPOOL_EVENT_KEY_KIND: u8 = 8;

impl StoreKey {
    /// Builds the persisted key for one mempool event sequence.
    pub(crate) fn mempool_event(event_sequence: u64) -> Self {
        let mut key = vec![KEY_VERSION, MEMPOOL_EVENT_KEY_KIND];
        key.extend_from_slice(&event_sequence.to_be_bytes());
        Self(key)
    }

    /// Returns the ordered key bytes.
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the key and returns the ordered key bytes.
    #[must_use]
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::StoreKey;

    #[test]
    fn mempool_event_key_bytes_are_stable() {
        assert_eq!(
            StoreKey::mempool_event(0x0102_0304_0506_0708).as_bytes(),
            [1, 8, 1, 2, 3, 4, 5, 6, 7, 8]
        );
    }
}
