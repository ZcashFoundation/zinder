//! Raw consensus-blob retention contract.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Raw-blob retention fixed by the primary writer after the first canonical commit.
///
/// Readers use this persisted value to check required coverage and derive
/// capabilities. A writer must open with the persisted profile and rebuild
/// the store to change blob retention.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RawBlobRetention {
    /// Neither block nor transaction blobs are retained.
    None,
    /// Both block and transaction blobs are retained (`storage.full_blocks = true`).
    All,
}

impl RawBlobRetention {
    /// Constructs retention from the binary `full_blocks` setting.
    #[must_use]
    pub const fn from_full_blocks(full_blocks: bool) -> Self {
        if full_blocks { Self::All } else { Self::None }
    }

    /// Returns whether this retention retains full raw blocks and transactions.
    #[must_use]
    pub const fn full_blocks(self) -> bool {
        matches!(self, Self::All)
    }

    /// Decodes the exact persisted configuration spelling, failing closed on
    /// retired legacy values such as `"transactions"`.
    #[must_use]
    pub fn from_kebab_case(encoded: &str) -> Option<Self> {
        match encoded {
            "none" => Some(Self::None),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    /// Returns the configuration spelling for this retention contract.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::All => "all",
        }
    }
}

impl fmt::Display for RawBlobRetention {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_kebab_case())
    }
}

#[cfg(test)]
mod tests {
    use super::RawBlobRetention;

    #[test]
    fn serde_round_trips_exact_configuration_spellings_and_refuses_transactions()
    -> Result<(), Box<dyn std::error::Error>> {
        for (retention, encoded, full_blocks) in [
            (RawBlobRetention::None, "\"none\"", false),
            (RawBlobRetention::All, "\"all\"", true),
        ] {
            assert_eq!(serde_json::to_string(&retention)?, encoded);
            assert_eq!(
                serde_json::from_str::<RawBlobRetention>(encoded)?,
                retention
            );
            assert_eq!(
                RawBlobRetention::from_kebab_case(retention.as_kebab_case()),
                Some(retention)
            );
            assert_eq!(RawBlobRetention::from_full_blocks(full_blocks), retention);
            assert_eq!(retention.full_blocks(), full_blocks);
        }
        assert!(serde_json::from_str::<RawBlobRetention>("\"transactions\"").is_err());
        assert_eq!(RawBlobRetention::from_kebab_case("transactions"), None);
        assert_eq!(RawBlobRetention::from_kebab_case("transaction"), None);
        Ok(())
    }
}
