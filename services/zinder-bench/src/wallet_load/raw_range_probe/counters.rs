//! Labelled server counters, read only behind a freshness witness.
//!
//! Some servers publish a gauge or ticker family on a timer, so a scrape can
//! return the last publication unchanged. Subtracting two such reads gives a
//! false zero. A snapshot therefore waits until a witness series, one the
//! server increments with every publication, has advanced past the value it
//! had when the wait began, and until every counter is present in that same
//! scrape. A server that never publishes the witness reports the counters as
//! unavailable, never as zero.

use std::{collections::BTreeMap, time::Duration};

use serde::Serialize;
use tokio::time::Instant;
use zinder_bench::metrics_scrape::MetricSample;

use super::super::server_metrics;

const POLL_INTERVAL: Duration = Duration::from_millis(150);

/// A series name and the labels a sample must carry to count, summed over
/// every other label.
#[derive(Clone, Debug)]
pub(super) struct Selector {
    /// The key the counter takes in the report.
    alias: String,
    name: String,
    labels: Vec<(String, String)>,
}

/// Parses `[ALIAS=]NAME[{label=value,...}]`. The alias defaults to the whole
/// selector.
pub(super) fn parse_selector(text: &str) -> Result<Selector, String> {
    let brace = text.find('{');
    let alias_end = text
        .find('=')
        .filter(|equals| brace.is_none_or(|brace| *equals < brace));
    let (alias, series) = alias_end.map_or((text, text), |equals| {
        (&text[..equals], &text[equals + 1..])
    });
    let (name, labels) = match series.split_once('{') {
        None => (series, Vec::new()),
        Some((name, rest)) => {
            let inner = rest
                .strip_suffix('}')
                .ok_or_else(|| format!("`{text}`: the label list must end with `}}`"))?;
            let labels = inner
                .split(',')
                .filter(|pair| !pair.is_empty())
                .map(|pair| {
                    pair.split_once('=')
                        .map(|(key, wanted)| (key.trim().to_owned(), wanted.trim().to_owned()))
                        .ok_or_else(|| format!("`{text}`: label `{pair}` is not key=value"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            (name, labels)
        }
    };
    if name.is_empty() || alias.is_empty() {
        return Err(format!("`{text}`: the series name is empty"));
    }
    Ok(Selector {
        alias: alias.to_owned(),
        name: name.to_owned(),
        labels,
    })
}

/// The `RocksDB` `MultiGet` tickers of a read-only canonical secondary.
pub(super) fn rocksdb_multiget() -> Vec<Selector> {
    [
        ("multiget_get", "rocksdb.number.multiget.get"),
        ("multiget_keys_read", "rocksdb.number.multiget.keys.read"),
        ("multiget_bytes_read", "rocksdb.number.multiget.bytes.read"),
    ]
    .into_iter()
    .map(|(alias, ticker)| Selector {
        alias: alias.to_owned(),
        name: "zinder_store_rocksdb_ticker".to_owned(),
        labels: vec![
            ("store_role".to_owned(), "canonical_secondary".to_owned()),
            ("ticker".to_owned(), ticker.to_owned()),
        ],
    })
    .collect()
}

impl Selector {
    /// The sum of every matching sample, or `None` when no sample matches.
    fn total(&self, samples: &[MetricSample]) -> Option<f64> {
        let mut matches = samples
            .iter()
            .filter(|sample| {
                sample.name == self.name
                    && self
                        .labels
                        .iter()
                        .all(|(key, wanted)| sample.label(key) == Some(wanted.as_str()))
            })
            .peekable();
        matches.peek()?;
        Some(matches.map(|sample| sample.reading).sum())
    }

    fn describe(&self) -> String {
        if self.labels.is_empty() {
            return self.name.clone();
        }
        let labels = self
            .labels
            .iter()
            .map(|(key, wanted)| format!("{key}={wanted}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}{{{labels}}}", self.name)
    }
}

/// Where and what to read.
pub(super) struct CounterSource {
    pub(super) url: String,
    pub(super) witness: Selector,
    pub(super) counters: Vec<Selector>,
    pub(super) wait: Duration,
}

/// Counter totals by alias, or why no fresh read was possible.
pub(super) type Snapshot = Result<BTreeMap<String, f64>, String>;

impl CounterSource {
    /// Waits for the witness to advance, then reads every counter from that
    /// scrape.
    pub(super) async fn fresh_snapshot(&self) -> Snapshot {
        let witness_total = |samples: &[MetricSample]| self.witness.total(samples).unwrap_or(0.0);
        let start = witness_total(&server_metrics::scrape(&self.url).await?);
        let deadline = Instant::now() + self.wait;
        while Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
            let samples = server_metrics::scrape(&self.url).await?;
            if witness_total(&samples) > start
                && let Some(totals) = self
                    .counters
                    .iter()
                    .map(|counter| Some((counter.alias.clone(), counter.total(&samples)?)))
                    .collect::<Option<BTreeMap<_, _>>>()
            {
                return Ok(totals);
            }
        }
        Err(format!(
            "witness `{}` did not advance with every counter present within {}s",
            self.witness.describe(),
            self.wait.as_secs_f64()
        ))
    }
}

/// What the server counted over the timed interval.
#[derive(Serialize)]
pub(super) struct CountersReport {
    witness: String,
    /// False when either snapshot could not be read fresh, or a counter went
    /// backwards. The counters are then absent, not zero.
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    increases: Option<BTreeMap<String, f64>>,
}

pub(super) fn report(source: &CounterSource, before: Snapshot, after: Snapshot) -> CountersReport {
    let witness = source.witness.describe();
    let increases = before.and_then(|before| {
        after?
            .into_iter()
            .map(|(alias, after)| {
                let before = before.get(&alias).copied().unwrap_or(f64::NAN);
                if after < before {
                    return Err(format!(
                        "counter `{alias}` went from {before} to {after}; the server restarted"
                    ));
                }
                Ok((alias, after - before))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    });
    match increases {
        Ok(increases) => CountersReport {
            witness,
            available: true,
            reason: None,
            increases: Some(increases),
        },
        Err(reason) => CountersReport {
            witness,
            available: false,
            reason: Some(reason),
            increases: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    use super::{CounterSource, parse_selector, report, rocksdb_multiget};

    #[test]
    fn selector_syntax_is_alias_name_and_labels() -> Result<(), Box<dyn Error>> {
        let plain = parse_selector("a_total")?;
        assert_eq!(
            (plain.alias.as_str(), plain.name.as_str()),
            ("a_total", "a_total")
        );
        let labelled = parse_selector("reads=a_total{role=x, ticker=rocksdb.number.get}")?;
        assert_eq!(labelled.alias, "reads");
        assert_eq!(labelled.labels.len(), 2);
        assert_eq!(labelled.labels[1].1, "rocksdb.number.get");
        let unaliased = parse_selector("a_total{status=ok}")?;
        assert_eq!(unaliased.alias, "a_total{status=ok}");
        assert!(parse_selector("a_total{status}").is_err());
        assert!(parse_selector("a_total{status=ok").is_err());
        assert!(parse_selector("=a").is_err());
        Ok(())
    }

    /// A metrics server whose publication witness advances every second
    /// scrape and whose multiget ticker grows by 7 per publication. Other
    /// roles and tickers carry different values that must not be summed in.
    async fn serve_metrics(
        witness_advances: bool,
    ) -> Result<(String, Arc<AtomicU64>), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/metrics", listener.local_addr()?);
        let scrapes = Arc::new(AtomicU64::new(0));
        let served = Arc::clone(&scrapes);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let scrape = served.fetch_add(1, Ordering::SeqCst) + 1;
                let publications = if witness_advances { scrape / 2 } else { 1 };
                let body = format!(
                    "zinder_publisher_refresh_total{{status=\"ok\"}} {publications}\n\
                     zinder_publisher_refresh_total{{status=\"error\"}} 100\n\
                     zinder_store_rocksdb_ticker{{store_role=\"canonical_secondary\",ticker=\"rocksdb.number.multiget.get\",shard=\"a\"}} {}\n\
                     zinder_store_rocksdb_ticker{{store_role=\"canonical_secondary\",ticker=\"rocksdb.number.multiget.get\",shard=\"b\"}} {}\n\
                     zinder_store_rocksdb_ticker{{store_role=\"canonical_primary\",ticker=\"rocksdb.number.multiget.get\"}} 1000\n",
                    publications * 5,
                    publications * 2,
                );
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await;
                let _ = stream
                    .write_all(
                        format!(
                            "HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
            }
        });
        Ok((url, scrapes))
    }

    fn source(url: String, wait: Duration) -> Result<CounterSource, Box<dyn Error>> {
        Ok(CounterSource {
            url,
            witness: parse_selector("zinder_publisher_refresh_total{status=ok}")?,
            counters: rocksdb_multiget()
                .into_iter()
                .filter(|counter| counter.alias == "multiget_get")
                .collect(),
            wait,
        })
    }

    #[tokio::test]
    async fn reads_labelled_counters_from_a_fresh_publication() -> Result<(), Box<dyn Error>> {
        let (url, _) = serve_metrics(true).await?;
        let source = source(url, Duration::from_secs(5))?;
        let before = source.fresh_snapshot().await;
        let after = source.fresh_snapshot().await;
        let report = report(&source, before, after);
        assert!(report.available, "{:?}", report.reason);
        // Only the canonical secondary's two shards count: 7 per publication,
        // and the second snapshot waited for a later publication than the first.
        let increase = report
            .increases
            .and_then(|map| map.get("multiget_get").copied());
        assert!(increase.is_some_and(|value| value >= 7.0 && value % 7.0 == 0.0));
        Ok(())
    }

    #[tokio::test]
    async fn reports_unavailable_when_the_witness_never_advances() -> Result<(), Box<dyn Error>> {
        let (url, _) = serve_metrics(false).await?;
        let source = source(url, Duration::from_millis(500))?;
        let before = source.fresh_snapshot().await;
        assert!(
            before
                .as_ref()
                .is_err_and(|reason| reason.contains("did not advance"))
        );
        let report = report(&source, before, Err("unread".to_owned()));
        assert!(!report.available);
        assert!(report.increases.is_none());
        Ok(())
    }

    #[test]
    fn a_counter_that_went_backwards_is_unavailable_not_negative() -> Result<(), Box<dyn Error>> {
        let source = source("http://unused".to_owned(), Duration::ZERO)?;
        let snapshot = |value| Ok(std::iter::once(("multiget_get".to_owned(), value)).collect());
        let report = report(&source, snapshot(9.0), snapshot(4.0));
        assert!(!report.available);
        assert!(
            report
                .reason
                .is_some_and(|reason| reason.contains("restarted"))
        );
        Ok(())
    }
}
