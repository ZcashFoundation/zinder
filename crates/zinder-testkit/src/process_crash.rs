//! Process-abort crash harness for storage durability tests.
//!
//! A crash test runs in two processes from the same test binary. The parent
//! test calls [`run_crash_child`], which re-executes the current binary with
//! only the named child test selected and one crash point armed. The child
//! test calls [`crash_child`] to receive its store path, drives the write path
//! under test, and reaches a [`crash_point`] that aborts the process. The
//! parent then reopens the store and asserts that it recovers to exactly one
//! complete commit.
//!
//! Crash points are one-line calls in production source. The owning crate
//! resolves `crash_point` to this module only in its unit-test build; every
//! other build calls an empty function, so no other binary can abort there:
//!
//! ```ignore
//! #[cfg(test)]
//! use zinder_testkit::process_crash::crash_point;
//!
//! #[cfg(not(test))]
//! const fn crash_point(_name: &str) {}
//!
//! crash_point("canonical.live_commit.before_atomic_write");
//! ```
//!
//! # Platform support
//!
//! [`run_crash_child`] and [`ProcessCrashError`] exist only on Unix, where an
//! abort is distinguishable from an ordinary failure by its `SIGABRT` signal.
//! Zinder builds, tests, and releases only Unix targets. Parent crash tests
//! carry `#[cfg(unix)]`; [`crash_point`] and [`crash_child`] stay available on
//! every target so production call sites and child tests compile unchanged.
//!
//! # Limitation
//!
//! `std::process::abort` ends the process but leaves the operating system
//! page cache intact. A write that reached the page cache without an fsync
//! survives the abort, so these tests prove atomicity and recovery of
//! committed batches; they cannot detect a missing fsync. Power-loss
//! durability needs a fault-injecting filesystem or a host-level test.

#[cfg(unix)]
use std::process::{Command, ExitStatus, Stdio};
use std::{
    env,
    path::{Path, PathBuf},
};

const CRASH_POINT_ENV: &str = "ZINDER_TEST_CRASH_POINT";
const CRASH_STORE_PATH_ENV: &str = "ZINDER_TEST_CRASH_STORE_PATH";

/// Aborts the process when the running crash child armed `name`.
///
/// Outside a crash child this is a no-op, so ordinary test runs pass through
/// every crash point.
pub fn crash_point(name: &str) {
    if env::var(CRASH_POINT_ENV).as_deref() == Ok(name) {
        std::process::abort();
    }
}

/// Environment handed to a child test spawned by [`run_crash_child`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrashChild {
    store_path: PathBuf,
    crash_point: String,
}

impl CrashChild {
    /// Store directory the parent test owns and will reopen.
    #[must_use]
    pub fn store_path(&self) -> &Path {
        &self.store_path
    }

    /// Crash point the parent armed for this child.
    #[must_use]
    pub fn crash_point(&self) -> &str {
        &self.crash_point
    }
}

/// Returns the crash-child environment, or `None` in an ordinary test run.
///
/// A child test returns early on `None`, which keeps it a passing no-op when
/// the test runner executes it directly.
#[must_use]
pub fn crash_child() -> Option<CrashChild> {
    let store_path = env::var_os(CRASH_STORE_PATH_ENV)?;
    let crash_point = env::var(CRASH_POINT_ENV).ok()?;
    Some(CrashChild {
        store_path: PathBuf::from(store_path),
        crash_point,
    })
}

/// Runs `child_test` from the current test binary with `crash_point` armed.
///
/// `child_test` is the exact libtest name of the child, for example
/// `canonical_store::builder::tests::publication_crash_child_process`.
/// Returns `Ok` only when the child process aborted.
#[cfg(unix)]
pub fn run_crash_child(
    child_test: &str,
    store_path: &Path,
    crash_point: &str,
) -> Result<(), ProcessCrashError> {
    let spawn_error = |source| ProcessCrashError::Spawn {
        child_test: child_test.to_owned(),
        source,
    };
    let output = Command::new(env::current_exe().map_err(spawn_error)?)
        .arg("--exact")
        .arg(child_test)
        .arg("--nocapture")
        .env(CRASH_STORE_PATH_ENV, store_path)
        .env(CRASH_POINT_ENV, crash_point)
        .stdin(Stdio::null())
        .output()
        .map_err(spawn_error)?;
    if process_aborted(output.status) {
        return Ok(());
    }
    Err(ProcessCrashError::DidNotAbort {
        child_test: child_test.to_owned(),
        crash_point: crash_point.to_owned(),
        status: output.status,
        stderr_tail: output_tail(&output.stderr),
    })
}

#[cfg(unix)]
fn process_aborted(status: ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt as _;

    const SIGABRT: i32 = 6;
    status.signal() == Some(SIGABRT)
}

#[cfg(unix)]
fn output_tail(bytes: &[u8]) -> String {
    const TAIL_BYTES: usize = 2048;
    let start = bytes.len().saturating_sub(TAIL_BYTES);
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

/// Failure to run a crash child to its abort.
#[cfg(unix)]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProcessCrashError {
    /// The current test binary could not be located or spawned.
    #[error("could not spawn crash child {child_test}: {source}")]
    Spawn {
        /// Exact libtest name of the child test.
        child_test: String,
        /// Underlying spawn failure.
        #[source]
        source: std::io::Error,
    },
    /// The child exited without aborting at its crash point.
    #[error(
        "crash child {child_test} exited with {status} instead of aborting at {crash_point}; stderr tail: {stderr_tail}"
    )]
    DidNotAbort {
        /// Exact libtest name of the child test.
        child_test: String,
        /// Crash point the parent armed.
        crash_point: String,
        /// Observed child exit status.
        status: ExitStatus,
        /// Last bytes of the child's standard error.
        stderr_tail: String,
    },
}

#[cfg(all(test, unix))]
mod tests {
    use tempfile::TempDir;

    use super::{ProcessCrashError, crash_child, crash_point, run_crash_child};

    const CHILD_TEST: &str = "process_crash::tests::crash_child_fixture";
    const ARMED_POINT: &str = "testkit.process_crash.fixture_point";

    #[test]
    fn crash_child_fixture() -> Result<(), Box<dyn std::error::Error>> {
        let Some(child) = crash_child() else {
            return Ok(());
        };
        std::fs::write(child.store_path().join("reached"), child.crash_point())?;
        crash_point(ARMED_POINT);
        Err("crash child returned without reaching its crash point".into())
    }

    #[test]
    fn child_that_aborts_at_its_crash_point_is_accepted() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;

        run_crash_child(CHILD_TEST, temporary.path(), ARMED_POINT)?;

        assert_eq!(
            std::fs::read_to_string(temporary.path().join("reached"))?,
            ARMED_POINT
        );
        Ok(())
    }

    #[test]
    fn child_that_fails_without_aborting_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;

        let outcome = run_crash_child(CHILD_TEST, temporary.path(), "testkit.unreached_point");

        assert!(
            matches!(
                &outcome,
                Err(ProcessCrashError::DidNotAbort { stderr_tail, .. })
                    if stderr_tail.contains("without reaching its crash point")
            ),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    fn unknown_child_test_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;

        let outcome = run_crash_child(
            "process_crash::tests::no_such_child",
            temporary.path(),
            ARMED_POINT,
        );

        assert!(
            matches!(outcome, Err(ProcessCrashError::DidNotAbort { .. })),
            "{outcome:?}"
        );
        Ok(())
    }
}
