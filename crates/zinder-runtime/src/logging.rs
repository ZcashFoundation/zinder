//! Process-wide tracing subscriber installation and `RUST_LOG` validation.

use std::env::VarError;

use tracing_subscriber::{EnvFilter, fmt};

const LOG_FILTER_ENV_VAR: &str = "RUST_LOG";
const DEFAULT_LOG_FILTER: &str = "info";

/// Error returned when the standard Zinder tracing subscriber cannot be
/// installed.
///
/// Binaries report it on stderr and exit non-zero before doing any work,
/// because no subscriber exists yet to log it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TracingSubscriberError {
    /// `RUST_LOG` is set to a value that does not parse as a log filter.
    #[error("RUST_LOG={directives:?} is not a valid log filter: {reason}")]
    InvalidLogFilter {
        /// The rejected filter directives, as read from `RUST_LOG`.
        directives: String,
        /// Why the directives were rejected.
        reason: String,
    },
    /// `RUST_LOG` is set but is not valid Unicode.
    #[error("RUST_LOG is set but is not valid Unicode")]
    NonUnicodeLogFilter,
    /// The process already has a global tracing subscriber or `log` logger.
    #[error("cannot install the global tracing subscriber: {reason}")]
    GlobalSubscriberInstall {
        /// Why installation failed.
        reason: String,
    },
}

/// Installs the standard Zinder tracing subscriber as the global default.
///
/// Reads the log filter from `RUST_LOG`, writes to stderr, and includes
/// target labels. An unset, empty, or whitespace-only `RUST_LOG` selects
/// `info`. Binaries call it once at startup.
///
/// # Errors
///
/// Returns [`TracingSubscriberError::InvalidLogFilter`] when `RUST_LOG` does
/// not parse as a log filter, [`TracingSubscriberError::NonUnicodeLogFilter`]
/// when it is not valid Unicode, and
/// [`TracingSubscriberError::GlobalSubscriberInstall`] when a global
/// subscriber is already installed.
pub fn install_tracing_subscriber() -> Result<(), TracingSubscriberError> {
    let directives = match std::env::var(LOG_FILTER_ENV_VAR) {
        Ok(directives) => Some(directives),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => return Err(TracingSubscriberError::NonUnicodeLogFilter),
    };
    let env_filter = log_filter_from_directives(directives.as_deref())?;
    fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .try_init()
        .map_err(|error| TracingSubscriberError::GlobalSubscriberInstall {
            reason: error.to_string(),
        })
}

fn log_filter_from_directives(
    directives: Option<&str>,
) -> Result<EnvFilter, TracingSubscriberError> {
    let directives = directives
        .map(str::trim)
        .filter(|directives| !directives.is_empty())
        .unwrap_or(DEFAULT_LOG_FILTER);
    EnvFilter::builder().parse(directives).map_err(|error| {
        TracingSubscriberError::InvalidLogFilter {
            directives: directives.to_owned(),
            reason: error.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{TracingSubscriberError, log_filter_from_directives};

    #[test]
    fn unset_log_filter_defaults_to_info() {
        let filter = log_filter_from_directives(None);
        assert!(
            filter
                .as_ref()
                .is_ok_and(|filter| filter.to_string() == "info"),
            "{filter:?}"
        );
    }

    #[test]
    fn blank_log_filter_is_treated_as_unset() {
        for directives in ["", "   ", "\t\n"] {
            let filter = log_filter_from_directives(Some(directives));
            assert!(
                filter
                    .as_ref()
                    .is_ok_and(|filter| filter.to_string() == "info"),
                "{directives:?}: {filter:?}"
            );
        }
    }

    #[test]
    fn valid_log_filters_are_accepted() {
        for directives in [
            "debug",
            "info,zinder=debug",
            "warn,zinder::ingest=trace,tower=off",
            "zinder",
        ] {
            let filter = log_filter_from_directives(Some(directives));
            assert!(filter.is_ok(), "{directives:?}: {filter:?}");
        }
    }

    #[test]
    fn invalid_log_filters_are_rejected() {
        for directives in ["zinder=bogus", "info,zinder=verbose", "[unterminated", "{"] {
            let error = log_filter_from_directives(Some(directives)).err();
            assert!(
                matches!(
                    &error,
                    Some(TracingSubscriberError::InvalidLogFilter {
                        directives: rejected,
                        reason,
                    }) if rejected == directives && !reason.is_empty()
                ),
                "{directives:?}: {error:?}"
            );
        }
    }

    #[test]
    fn invalid_log_filter_error_names_the_variable_and_value() {
        let message = log_filter_from_directives(Some("zinder=bogus"))
            .err()
            .map_or_default(|error| error.to_string());
        assert!(
            message.starts_with("RUST_LOG=\"zinder=bogus\""),
            "{message}"
        );
        assert!(message.contains("not a valid log filter"), "{message}");
    }
}
