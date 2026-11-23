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
    /// Transaction blobs are retained; block blobs are not.
    Transactions,
    /// Both block and transaction blobs are retained.
    All,
}

impl RawBlobRetention {
    /// Decodes the exact persisted configuration spelling.
    #[must_use]
    pub fn from_kebab_case(encoded: &str) -> Option<Self> {
        match encoded {
            "none" => Some(Self::None),
            "transactions" => Some(Self::Transactions),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    /// Returns the configuration spelling for this retention contract.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Transactions => "transactions",
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
    fn serde_round_trips_exact_configuration_spellings() -> Result<(), Box<dyn std::error::Error>> {
        for (retention, encoded) in [
            (RawBlobRetention::None, "\"none\""),
            (RawBlobRetention::Transactions, "\"transactions\""),
            (RawBlobRetention::All, "\"all\""),
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
        }
        assert_eq!(RawBlobRetention::from_kebab_case("transaction"), None);
        Ok(())
    }
}
