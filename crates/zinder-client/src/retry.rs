//! Bounded backoff for idempotent read RPCs.

use std::time::Duration;

use tonic::{Code, Status};

pub(super) const MAX_READ_ATTEMPTS: u8 = 4;
const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 2_000;
const PUSHBACK_HEADER: &str = "grpc-retry-pushback-ms";

pub(super) struct ReadBackoff {
    next_delay_ms: u64,
}

impl ReadBackoff {
    pub(super) const fn new() -> Self {
        Self {
            next_delay_ms: INITIAL_BACKOFF_MS,
        }
    }

    pub(super) fn delay(&mut self, status: &Status, has_typed_reason: bool) -> Option<Duration> {
        if status.code() == Code::Cancelled
            || (!has_typed_reason
                && !matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::Unknown
                        | Code::DeadlineExceeded
                        | Code::ResourceExhausted
                ))
        {
            return None;
        }

        let pushback = status.metadata().get_all(PUSHBACK_HEADER);
        let mut headers = pushback.iter();
        if let Some(header) = headers.next() {
            if headers.next().is_some() {
                return None;
            }
            let millis = header.to_str().ok()?.parse::<i32>().ok()?;
            let millis = u64::try_from(millis).ok()?;
            if millis > MAX_BACKOFF_MS {
                return None;
            }
            self.next_delay_ms = INITIAL_BACKOFF_MS;
            return Some(Duration::from_millis(millis));
        }

        let delay_ms = fastrand::u64(self.next_delay_ms / 2..=self.next_delay_ms);
        self.next_delay_ms = (self.next_delay_ms * 2).min(MAX_BACKOFF_MS);
        Some(Duration::from_millis(delay_ms))
    }
}
