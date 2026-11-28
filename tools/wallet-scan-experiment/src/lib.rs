//! Comparative trial decryption with public fixture accounts and compact Testnet actions.

mod corpus;
mod scan;
mod shifted;

use corpus::{MAGIC, Pool, ScanAction};
use eyre::{Result, ensure, eyre};
use scan::{Arm, Receipt, ScanInputs, Scanner};

fn read_corpus(encoded: &[u8]) -> Result<Vec<ScanAction>> {
    let mut rows = encoded
        .strip_prefix(MAGIC)
        .ok_or_else(|| eyre!("invalid scan corpus header"))?;
    let mut actions = Vec::new();
    while !rows.is_empty() {
        let record = rows
            .split_off(..150)
            .ok_or_else(|| eyre!("truncated action"))?;
        let pool = match record[0] {
            b'O' => Pool::Orchard,
            b'I' => Pool::Ironwood,
            tag => return Err(eyre!("unknown pool tag {tag}")),
        };
        let hint = match record[149] {
            0 => None,
            32 => Some(
                rows.split_off(..32)
                    .ok_or_else(|| eyre!("truncated hint"))?
                    .try_into()?,
            ),
            n => return Err(eyre!("invalid hint length {n}")),
        };
        actions.push(ScanAction {
            pool,
            nullifier: record[1..33].try_into()?,
            cmx: record[33..65].try_into()?,
            epk: record[65..97].try_into()?,
            ciphertext: record[97..149].try_into()?,
            hint,
        });
    }
    Ok(actions)
}

/// Scan the same public corpus with each implementation and verify every receipt.
pub struct ScanComparison {
    inputs: ScanInputs,
    scanner: Scanner,
    batch_size: usize,
    expected: Vec<Receipt>,
    diagnostics: String,
}

impl ScanComparison {
    /// Construct the reference result and verify the deterministic payment fixture.
    pub fn new(encoded: &[u8], batch_size: usize) -> Result<Self> {
        ensure!(batch_size > 0, "batch size must be positive");
        let mut actions = read_corpus(encoded)?;
        let archive_actions = actions.len();
        let scanner = Scanner::new()?;
        let fixture = scanner.validate_fixture()?;
        let known = scanner.append_fixture(&mut actions)?;
        let inputs = ScanInputs::new(&actions)?;
        let expected = scanner.scan(&inputs, Arm::Stock, batch_size)?;
        ensure!(
            expected.ends_with(&known),
            "reference scanner missed a known receipt"
        );
        let diagnostics = serde_json::to_string(&serde_json::json!({
            "archived_actions": archive_actions, "actions_with_fixture": actions.len(),
            "total_receipts": expected.len(), "receipt_digest_blake2b256": scan::receipt_digest(&expected)?, "fixture": fixture
        }))?;
        Ok(Self {
            inputs,
            scanner,
            batch_size,
            expected,
            diagnostics,
        })
    }

    /// Counts and correctness checks performed before timing.
    #[must_use]
    pub fn diagnostics(&self) -> &str {
        &self.diagnostics
    }

    /// Run one arm and compare each recovered note to the reference.
    pub fn scan(&self, arm: &str) -> Result<usize> {
        let arm = match arm {
            "stock" => Arm::Stock,
            "ordinary-prefilter" => Arm::OrdinaryPrefilter,
            "hinted" => Arm::Hinted,
            _ => return Err(eyre!("unknown scanner {arm}")),
        };
        let found = self.scanner.scan(&self.inputs, arm, self.batch_size)?;
        ensure!(
            found == self.expected,
            "{arm:?} recovered different receipts"
        );
        Ok(found.len())
    }
}

#[cfg(target_arch = "wasm32")]
mod web {
    use super::ScanComparison;
    use wasm_bindgen::prelude::*;

    /// Synchronous scanner for measuring elapsed time in the browser.
    #[wasm_bindgen]
    pub struct BrowserScanner(ScanComparison);

    #[wasm_bindgen]
    impl BrowserScanner {
        /// Load an archive and verify the known payments before any timed scan.
        #[wasm_bindgen(constructor)]
        pub fn new(corpus: &[u8], batch_size: u32) -> Result<Self, String> {
            ScanComparison::new(corpus, batch_size as usize)
                .map(Self)
                .map_err(|e| e.to_string())
        }

        /// Return the fixture and archive diagnostics as JSON.
        #[must_use]
        pub fn validate(&self) -> String {
            self.0.diagnostics().to_owned()
        }

        /// Run an arm, verify receipt identity, and return its receipt count.
        pub fn scan(&self, arm: &str) -> Result<u32, String> {
            self.0
                .scan(arm)
                .and_then(|count| Ok(u32::try_from(count)?))
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::BrowserScanner;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_truncated_or_unknown_corpus_records() {
        assert!(read_corpus(b"").is_err());
        for length in [1, 148, 149] {
            let mut corpus = MAGIC.to_vec();
            corpus.extend(vec![0; length]);
            assert!(read_corpus(&corpus).is_err());
        }
        let mut corpus = MAGIC.to_vec();
        let mut record = [0; 150];
        record[0] = b'?';
        corpus.extend(record);
        assert!(read_corpus(&corpus).is_err());
        corpus[MAGIC.len()] = b'O';
        corpus[MAGIC.len() + 149] = 31;
        assert!(read_corpus(&corpus).is_err());
        corpus[MAGIC.len() + 149] = 32;
        assert!(read_corpus(&corpus).is_err());
    }

    #[test]
    fn empty_archive_still_verifies_known_receipts() -> Result<()> {
        let comparison = ScanComparison::new(MAGIC, 2)?;
        for arm in ["stock", "ordinary-prefilter", "hinted"] {
            assert_eq!(comparison.scan(arm)?, 4);
        }
        assert!(comparison.scan("unknown").is_err());
        Ok(())
    }
}
