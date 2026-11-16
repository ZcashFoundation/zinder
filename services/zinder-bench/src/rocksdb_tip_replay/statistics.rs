//! `RocksDB` statistics ticker deltas of the wallet store, per block and per stratum.
//!
//! Tickers count DB-wide, so a delta covers every column family of the wallet
//! store. `rocksdb.block.cache.data.miss` is the number of data blocks the
//! store had to read from its files; `RocksDB` has no separate data-block-read
//! ticker.

use std::collections::BTreeMap;

use rust_rocksdb::statistics::Ticker;
use serde::{Deserialize, Serialize};
use zinder_wallet_rocksdb::RocksDbWalletFollowingStore;

use super::measurement::nearest_rank;

/// Tickers the report keeps, named by `RocksDB` in the report.
pub(super) const REPORTED_TICKERS: [Ticker; 13] = [
    Ticker::NumberKeysRead,
    Ticker::NumberMultigetKeysRead,
    Ticker::NumberDbSeek,
    Ticker::BloomFilterUseful,
    Ticker::BloomFilterFullPositive,
    Ticker::BloomFilterFullTruePositive,
    Ticker::BlockCacheDataMiss,
    Ticker::BlockCacheDataHit,
    Ticker::BlockCacheFilterMiss,
    Ticker::BlockCacheFilterHit,
    Ticker::BlockCacheIndexMiss,
    Ticker::BlockCacheIndexHit,
    Ticker::BytesRead,
];

/// Names of [`REPORTED_TICKERS`], as they appear in the report.
pub(super) fn reported_ticker_names() -> Vec<String> {
    REPORTED_TICKERS
        .iter()
        .map(|ticker| ticker.name().to_owned())
        .collect()
}

/// Ticker values read at one instant.
pub(super) struct TickerSnapshot([u64; REPORTED_TICKERS.len()]);

impl TickerSnapshot {
    pub(super) fn read(wallet: &RocksDbWalletFollowingStore) -> Self {
        Self(REPORTED_TICKERS.map(|ticker| wallet.rocksdb_ticker_count(ticker)))
    }

    /// What each ticker counted between `earlier` and this snapshot.
    pub(super) fn delta_since(&self, earlier: &Self) -> TickerDeltas {
        TickerDeltas(
            REPORTED_TICKERS
                .iter()
                .zip(self.0.iter().zip(&earlier.0))
                .map(|(ticker, (now, before))| {
                    (ticker.name().to_owned(), now.saturating_sub(*before))
                })
                .collect(),
        )
    }
}

/// What each ticker counted during one block's wallet transition.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct TickerDeltas(pub(super) BTreeMap<String, u64>);

/// Nearest-rank summary of one ticker's per-block deltas.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct CountSummary {
    pub(super) total: u64,
    pub(super) mean_per_block: f64,
    pub(super) p50: u64,
    pub(super) p99: u64,
    pub(super) max: u64,
}

/// [`CountSummary`] of every reported ticker over one set of blocks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct TickerSummaries(pub(super) BTreeMap<String, CountSummary>);

impl TickerSummaries {
    /// Summarizes per-block deltas; `None` when there are none.
    pub(super) fn from_deltas(deltas: &[&TickerDeltas]) -> Option<Self> {
        if deltas.is_empty() {
            return None;
        }
        let block_count = u32::try_from(deltas.len()).unwrap_or(u32::MAX);
        let summaries = REPORTED_TICKERS
            .iter()
            .filter_map(|ticker| {
                let name = ticker.name();
                let mut counts: Vec<u64> = deltas
                    .iter()
                    .map(|block| block.0.get(name).copied().unwrap_or(0))
                    .collect();
                counts.sort_unstable();
                let total = counts
                    .iter()
                    .fold(0_u64, |sum, count| sum.saturating_add(*count));
                Some((
                    name.to_owned(),
                    CountSummary {
                        total,
                        mean_per_block: u64_to_f64(total) / f64::from(block_count),
                        p50: nearest_rank(&counts, 5_000)?,
                        p99: nearest_rank(&counts, 9_900)?,
                        max: *counts.last()?,
                    },
                ))
            })
            .collect();
        Some(Self(summaries))
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "a block range's ticker totals stay far below the f64 exact-integer range"
)]
fn u64_to_f64(amount: u64) -> f64 {
    amount as f64
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, error::Error};

    use super::{REPORTED_TICKERS, TickerDeltas, TickerSummaries};

    fn deltas(keys_read: u64) -> TickerDeltas {
        TickerDeltas(BTreeMap::from([(
            REPORTED_TICKERS[0].name().to_owned(),
            keys_read,
        )]))
    }

    #[test]
    fn summaries_total_and_rank_the_per_block_deltas() -> Result<(), Box<dyn Error>> {
        let blocks = [deltas(4), deltas(0), deltas(2), deltas(10)];
        let summaries = TickerSummaries::from_deltas(&blocks.iter().collect::<Vec<_>>())
            .ok_or("four blocks must summarize")?;

        let keys_read = summaries
            .0
            .get(REPORTED_TICKERS[0].name())
            .ok_or("NumberKeysRead must be summarized")?;
        assert_eq!(keys_read.total, 16);
        assert_eq!(keys_read.mean_per_block.to_bits(), 4.0_f64.to_bits());
        assert_eq!(keys_read.p50, 2);
        assert_eq!(keys_read.p99, 10);
        assert_eq!(keys_read.max, 10);
        assert!(TickerSummaries::from_deltas(&[]).is_none());
        Ok(())
    }
}
