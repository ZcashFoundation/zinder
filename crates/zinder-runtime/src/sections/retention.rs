//! Shared `[retention]` config section.
//!
//! Owns the chain-event and mempool-event retention windows enforced by
//! the writer (`zinder-ingest`)
//! through `ServerInfo`. Before this section existed, the writer and
//! readers each carried their own copy of the same windows; operators
//! were responsible for keeping them in sync. Both planes now read one
//! `[retention]` section so the writer's enforcement and the reader's
//! advertisement cannot drift.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zinder_store::MempoolEventRetentionStepBudget;

use crate::ConfigError;

const DEFAULT_CHAIN_EVENT_RETENTION_HOURS: u64 = 168;
const DEFAULT_CHAIN_EVENT_RETENTION_CHECK_INTERVAL_MS: u64 = 60_000;
const DEFAULT_CURSOR_AT_RISK_WARNING_HOURS: u64 = 24;
const DEFAULT_MEMPOOL_MINED_RETENTION_MINUTES: u64 = 60;
const DEFAULT_MEMPOOL_INVALIDATED_RETENTION_HOURS: u64 = 24;
const DEFAULT_MEMPOOL_EVENT_RETENTION_CHECK_INTERVAL_MS: u64 = 30_000;
const DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP: u32 = 1_024;
const DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP: u64 = 16_000_000;

/// Raw and serializable `[retention]` config section.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionSection {
    /// Chain-event retention window in hours. `0` disables eviction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_event_retention_hours: Option<u64>,
    /// Chain-event retention sweep cadence in milliseconds. Must be > 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_event_retention_check_interval_ms: Option<u64>,
    /// Cursor-at-risk warning lead time in hours.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_at_risk_warning_hours: Option<u64>,
    /// Mined-mempool retention window in minutes. `0` disables retention.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mempool_mined_retention_minutes: Option<u64>,
    /// Invalidated-mempool retention window in hours. `0` disables retention.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mempool_invalidated_retention_hours: Option<u64>,
    /// Mempool-event retention sweep cadence in milliseconds. Must be > 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mempool_event_retention_check_interval_ms: Option<u64>,
    /// Maximum event rows examined by one mempool-retention step. Must be > 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mempool_event_retention_max_events_per_step: Option<u32>,
    /// Target maximum encoded bytes examined by one mempool-retention step. Must be > 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mempool_event_retention_max_encoded_bytes_per_step: Option<u64>,
}

impl RetentionSection {
    /// Returns whether every `[retention]` field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.chain_event_retention_hours.is_none()
            && self.chain_event_retention_check_interval_ms.is_none()
            && self.cursor_at_risk_warning_hours.is_none()
            && self.mempool_mined_retention_minutes.is_none()
            && self.mempool_invalidated_retention_hours.is_none()
            && self.mempool_event_retention_check_interval_ms.is_none()
            && self.mempool_event_retention_max_events_per_step.is_none()
            && self
                .mempool_event_retention_max_encoded_bytes_per_step
                .is_none()
    }

    /// Builds a [`RetentionSection`] projection from a [`ResolvedRetention`].
    #[must_use]
    pub const fn from_resolved(retention: ResolvedRetention) -> Self {
        Self {
            chain_event_retention_hours: Some(retention.chain_event_retention_hours),
            chain_event_retention_check_interval_ms: Some(
                retention.chain_event_retention_check_interval_ms,
            ),
            cursor_at_risk_warning_hours: Some(retention.cursor_at_risk_warning_hours),
            mempool_mined_retention_minutes: Some(retention.mempool_mined_retention_minutes),
            mempool_invalidated_retention_hours: Some(
                retention.mempool_invalidated_retention_hours,
            ),
            mempool_event_retention_check_interval_ms: Some(
                retention.mempool_event_retention_check_interval_ms,
            ),
            mempool_event_retention_max_events_per_step: Some(
                retention.mempool_event_retention_max_events_per_step.get(),
            ),
            mempool_event_retention_max_encoded_bytes_per_step: Some(
                retention
                    .mempool_event_retention_max_encoded_bytes_per_step
                    .get(),
            ),
        }
    }
}

/// Fully resolved retention configuration with all defaults applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRetention {
    /// Chain-event retention window in hours.
    pub chain_event_retention_hours: u64,
    /// Chain-event retention sweep cadence in milliseconds.
    pub chain_event_retention_check_interval_ms: u64,
    /// Cursor-at-risk warning lead time in hours.
    pub cursor_at_risk_warning_hours: u64,
    /// Mined-mempool retention window in minutes.
    pub mempool_mined_retention_minutes: u64,
    /// Invalidated-mempool retention window in hours.
    pub mempool_invalidated_retention_hours: u64,
    /// Mempool-event retention sweep cadence in milliseconds.
    pub mempool_event_retention_check_interval_ms: u64,
    /// Maximum event rows examined by one mempool-retention step.
    pub mempool_event_retention_max_events_per_step: NonZeroU32,
    /// Target maximum encoded bytes examined by one mempool-retention step.
    pub mempool_event_retention_max_encoded_bytes_per_step: NonZeroU64,
}

impl ResolvedRetention {
    /// Chain-event retention window as a [`Duration`], or `None` when
    /// eviction is disabled (configured value is zero).
    #[must_use]
    pub fn chain_event_window(&self) -> Option<Duration> {
        (self.chain_event_retention_hours > 0)
            .then(|| Duration::from_hours(self.chain_event_retention_hours))
    }

    /// Chain-event retention sweep cadence.
    #[must_use]
    pub fn chain_event_check_interval(&self) -> Duration {
        Duration::from_millis(self.chain_event_retention_check_interval_ms)
    }

    /// Cursor-at-risk warning lead time for chain events.
    #[must_use]
    pub fn cursor_at_risk_warning(&self) -> Duration {
        Duration::from_hours(self.cursor_at_risk_warning_hours)
    }

    /// Mined-mempool retention window, or `None` when retention is
    /// disabled.
    #[must_use]
    pub fn mempool_mined_window(&self) -> Option<Duration> {
        (self.mempool_mined_retention_minutes > 0)
            .then(|| Duration::from_mins(self.mempool_mined_retention_minutes))
    }

    /// Invalidated-mempool retention window, or `None` when retention is
    /// disabled.
    #[must_use]
    pub fn mempool_invalidated_window(&self) -> Option<Duration> {
        (self.mempool_invalidated_retention_hours > 0)
            .then(|| Duration::from_hours(self.mempool_invalidated_retention_hours))
    }

    /// Mempool-event retention sweep cadence.
    #[must_use]
    pub fn mempool_check_interval(&self) -> Duration {
        Duration::from_millis(self.mempool_event_retention_check_interval_ms)
    }

    /// Work budget for one bounded mempool-event retention step.
    #[must_use]
    pub const fn mempool_step_budget(&self) -> MempoolEventRetentionStepBudget {
        MempoolEventRetentionStepBudget::new(
            self.mempool_event_retention_max_events_per_step,
            self.mempool_event_retention_max_encoded_bytes_per_step,
        )
    }
}

/// Validates and resolves a [`RetentionSection`], applying per-field
/// defaults and cross-field invariants.
///
/// Returns [`ConfigError::Invalid`] when a sweep cadence is zero or when
/// a cursor-at-risk warning lead time exceeds the matching retention
/// window.
pub fn resolve_retention(section: RetentionSection) -> Result<ResolvedRetention, ConfigError> {
    let resolved = ResolvedRetention {
        chain_event_retention_hours: section
            .chain_event_retention_hours
            .unwrap_or(DEFAULT_CHAIN_EVENT_RETENTION_HOURS),
        chain_event_retention_check_interval_ms: section
            .chain_event_retention_check_interval_ms
            .unwrap_or(DEFAULT_CHAIN_EVENT_RETENTION_CHECK_INTERVAL_MS),
        cursor_at_risk_warning_hours: section
            .cursor_at_risk_warning_hours
            .unwrap_or(DEFAULT_CURSOR_AT_RISK_WARNING_HOURS),
        mempool_mined_retention_minutes: section
            .mempool_mined_retention_minutes
            .unwrap_or(DEFAULT_MEMPOOL_MINED_RETENTION_MINUTES),
        mempool_invalidated_retention_hours: section
            .mempool_invalidated_retention_hours
            .unwrap_or(DEFAULT_MEMPOOL_INVALIDATED_RETENTION_HOURS),
        mempool_event_retention_check_interval_ms: section
            .mempool_event_retention_check_interval_ms
            .unwrap_or(DEFAULT_MEMPOOL_EVENT_RETENTION_CHECK_INTERVAL_MS),
        mempool_event_retention_max_events_per_step: NonZeroU32::new(
            section
                .mempool_event_retention_max_events_per_step
                .unwrap_or(DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP),
        )
        .ok_or_else(|| {
            ConfigError::invalid(
                "retention.mempool_event_retention_max_events_per_step must be greater than zero",
            )
        })?,
        mempool_event_retention_max_encoded_bytes_per_step: NonZeroU64::new(
            section
                .mempool_event_retention_max_encoded_bytes_per_step
                .unwrap_or(DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP),
        )
        .ok_or_else(|| {
            ConfigError::invalid(
                "retention.mempool_event_retention_max_encoded_bytes_per_step must be greater than zero",
            )
        })?,
    };

    if resolved.chain_event_retention_check_interval_ms == 0 {
        return Err(ConfigError::invalid(
            "retention.chain_event_retention_check_interval_ms must be greater than zero",
        ));
    }
    if resolved.mempool_event_retention_check_interval_ms == 0 {
        return Err(ConfigError::invalid(
            "retention.mempool_event_retention_check_interval_ms must be greater than zero",
        ));
    }
    if resolved.chain_event_retention_hours > 0
        && resolved.cursor_at_risk_warning_hours > resolved.chain_event_retention_hours
    {
        return Err(ConfigError::invalid(
            "retention.cursor_at_risk_warning_hours must be less than or equal to \
             retention.chain_event_retention_hours",
        ));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_resolve_cleanly() -> Result<(), ConfigError> {
        let resolved = resolve_retention(RetentionSection::default())?;
        assert_eq!(
            resolved.chain_event_retention_hours,
            DEFAULT_CHAIN_EVENT_RETENTION_HOURS
        );
        assert_eq!(
            resolved.mempool_event_retention_max_events_per_step.get(),
            DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP
        );
        assert_eq!(
            resolved
                .mempool_event_retention_max_encoded_bytes_per_step
                .get(),
            DEFAULT_MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP
        );
        Ok(())
    }

    #[test]
    fn zero_chain_event_check_interval_is_rejected() {
        let outcome = resolve_retention(RetentionSection {
            chain_event_retention_check_interval_ms: Some(0),
            ..RetentionSection::default()
        });
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn warning_exceeding_chain_event_window_is_rejected() {
        let outcome = resolve_retention(RetentionSection {
            chain_event_retention_hours: Some(10),
            cursor_at_risk_warning_hours: Some(20),
            ..RetentionSection::default()
        });
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn removed_mempool_cursor_warning_setting_is_rejected() {
        let outcome =
            toml::from_str::<RetentionSection>("mempool_cursor_at_risk_warning_minutes = 12");

        assert!(outcome.is_err());
    }

    #[test]
    fn zero_mempool_retention_step_budgets_are_rejected() {
        for section in [
            RetentionSection {
                mempool_event_retention_max_events_per_step: Some(0),
                ..RetentionSection::default()
            },
            RetentionSection {
                mempool_event_retention_max_encoded_bytes_per_step: Some(0),
                ..RetentionSection::default()
            },
        ] {
            assert!(matches!(
                resolve_retention(section),
                Err(ConfigError::Invalid { .. })
            ));
        }
    }
}
