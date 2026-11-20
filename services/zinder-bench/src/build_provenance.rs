//! Compiler settings shared by benchmark reports.

use serde::{Deserialize, Serialize};

/// Compiler settings recorded by the build script.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct BuildProvenance {
    /// Cargo profile directory, such as `release`, or `debug` for the dev profile.
    pub(crate) cargo_profile: String,
    /// `release` or `debug`, the profile family Cargo reports to build scripts.
    pub(crate) cargo_profile_kind: String,
    pub(crate) opt_level: String,
    pub(crate) debug_info: String,
    pub(crate) debug_assertions: bool,
    /// Every rustflag Cargo passed, from the environment or Cargo configuration.
    pub(crate) rustflags: String,
    pub(crate) rustc_version: String,
    pub(crate) target: String,
    /// Explicit CPU requested by the compiler flags, if present.
    #[serde(default)]
    pub(crate) target_cpu: Option<String>,
    /// Target features enabled at compile time, as resolved by rustc.
    #[serde(default)]
    pub(crate) target_features: Vec<String>,
}

const BUILD_PROFILE: &str = env!("ZINDER_BENCH_BUILD_PROFILE");
const BUILD_PROFILE_KIND: &str = env!("ZINDER_BENCH_BUILD_PROFILE_KIND");
const BUILD_OPT_LEVEL: &str = env!("ZINDER_BENCH_BUILD_OPT_LEVEL");
const BUILD_DEBUG_INFO: &str = env!("ZINDER_BENCH_BUILD_DEBUG_INFO");
const BUILD_RUSTFLAGS: &str = env!("ZINDER_BENCH_BUILD_RUSTFLAGS");
const BUILD_RUSTC_VERSION: &str = env!("ZINDER_BENCH_BUILD_RUSTC_VERSION");
const BUILD_TARGET: &str = env!("ZINDER_BENCH_BUILD_TARGET");

impl BuildProvenance {
    pub(crate) fn of_this_binary() -> Self {
        Self {
            cargo_profile: BUILD_PROFILE.to_owned(),
            cargo_profile_kind: BUILD_PROFILE_KIND.to_owned(),
            opt_level: BUILD_OPT_LEVEL.to_owned(),
            debug_info: BUILD_DEBUG_INFO.to_owned(),
            debug_assertions: cfg!(debug_assertions),
            rustflags: BUILD_RUSTFLAGS.to_owned(),
            rustc_version: BUILD_RUSTC_VERSION.to_owned(),
            target: BUILD_TARGET.to_owned(),
            target_cpu: Some(env!("ZINDER_BENCH_BUILD_TARGET_CPU"))
                .filter(|cpu| !cpu.is_empty())
                .map(str::to_owned),
            target_features: env!("ZINDER_BENCH_BUILD_TARGET_FEATURES")
                .split(',')
                .filter(|feature| !feature.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }
}
