//! Locates a package's service binary and source tree from an integration test.
//!
//! Cargo bakes `CARGO_BIN_EXE_<name>` and `CARGO_MANIFEST_DIR` into the test at
//! compile time, so the paths name the machine that built it. A nextest archive
//! that runs elsewhere needs the runtime `NEXTEST_BIN_EXE_<name>` and
//! `CARGO_MANIFEST_DIR` that nextest sets for the extracted archive. Tests call
//! [`bin_exe!`](crate::bin_exe) and [`manifest_dir!`](crate::manifest_dir) so
//! both cases resolve without the call site knowing which one applies.

use std::{ffi::OsString, path::PathBuf};

/// Returns the runtime path when one is set and the compiled path otherwise.
///
/// Exposed for [`bin_exe!`](crate::bin_exe); call the macro instead.
#[doc(hidden)]
#[must_use]
pub fn select_binary_path(runtime_path: Option<OsString>, compiled_path: &str) -> PathBuf {
    runtime_path
        .filter(|path| !path.is_empty())
        .map_or_else(|| PathBuf::from(compiled_path), PathBuf::from)
}

/// Resolves the path of the package binary `$name` for an integration test.
///
/// Prefers the `NEXTEST_BIN_EXE_<name>` variable that nextest sets at run
/// time, and falls back to the `CARGO_BIN_EXE_<name>` path that Cargo bakes
/// in at compile time. Use it instead of `env!("CARGO_BIN_EXE_<name>")` so the
/// test also runs from a nextest archive on another machine. The name is the
/// binary target name, hyphens included, and must be a string literal.
///
/// ```ignore
/// let output = std::process::Command::new(zinder_testkit::bin_exe!("zinder-query"))
///     .arg("--version")
///     .output()?;
/// ```
#[macro_export]
macro_rules! bin_exe {
    ($name:literal) => {
        $crate::binary_path::select_binary_path(
            ::std::env::var_os(::std::concat!("NEXTEST_BIN_EXE_", $name)),
            ::std::env!(::std::concat!("CARGO_BIN_EXE_", $name)),
        )
    };
}

/// Resolves the manifest directory of the package under test.
///
/// Prefers the `CARGO_MANIFEST_DIR` that nextest sets at run time, remapped by
/// `--workspace-remap`, and falls back to the directory Cargo bakes in at
/// compile time. Use it instead of `env!("CARGO_MANIFEST_DIR")` when a test
/// reads files from the source tree.
///
/// ```ignore
/// let deploy = zinder_testkit::manifest_dir!().join("../../deploy");
/// ```
#[macro_export]
macro_rules! manifest_dir {
    () => {
        $crate::binary_path::select_binary_path(
            ::std::env::var_os("CARGO_MANIFEST_DIR"),
            ::std::env!("CARGO_MANIFEST_DIR"),
        )
    };
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::PathBuf};

    use super::select_binary_path;

    #[test]
    fn runtime_path_wins_over_compiled_path() {
        let selected = select_binary_path(Some(OsString::from("/run/bin")), "/build/bin");

        assert_eq!(selected, PathBuf::from("/run/bin"));
    }

    #[test]
    fn compiled_path_is_used_without_runtime_path() {
        assert_eq!(
            select_binary_path(None, "/build/bin"),
            PathBuf::from("/build/bin")
        );
        assert_eq!(
            select_binary_path(Some(OsString::new()), "/build/bin"),
            PathBuf::from("/build/bin")
        );
    }
}
