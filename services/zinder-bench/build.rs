//! Records the Cargo profile, optimization settings, compiler, and rustflags
//! the benchmark binary was built with, so reports can name them.

use std::{env, path::Path, process::Command};

#[allow(
    clippy::print_stdout,
    reason = "build scripts communicate with Cargo through stdout"
)]
fn main() {
    let target = env::var("TARGET").unwrap_or_default();
    let build_settings = [
        ("ZINDER_BENCH_BUILD_PROFILE", cargo_profile_directory()),
        (
            "ZINDER_BENCH_BUILD_PROFILE_KIND",
            env::var("PROFILE").unwrap_or_default(),
        ),
        (
            "ZINDER_BENCH_BUILD_OPT_LEVEL",
            env::var("OPT_LEVEL").unwrap_or_default(),
        ),
        (
            "ZINDER_BENCH_BUILD_DEBUG_INFO",
            env::var("DEBUG").unwrap_or_default(),
        ),
        ("ZINDER_BENCH_BUILD_RUSTFLAGS", encoded_rustflags()),
        ("ZINDER_BENCH_BUILD_RUSTC_VERSION", rustc_version()),
        ("ZINDER_BENCH_BUILD_TARGET", target.clone()),
    ];
    for (name, setting) in build_settings {
        println!("cargo:rustc-env={name}={setting}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTFLAGS");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    println!("cargo:rerun-if-env-changed=CARGO_BUILD_RUSTFLAGS");
    println!(
        "cargo:rerun-if-env-changed=CARGO_TARGET_{}_RUSTFLAGS",
        target.to_uppercase().replace(['-', '.'], "_")
    );
}

/// Cargo names no custom profile to build scripts, but places every profile's
/// output under `target/[<triple>/]<profile-directory>/build/<package>/out`.
fn cargo_profile_directory() -> String {
    env::var("OUT_DIR")
        .ok()
        .and_then(|out_dir| {
            Path::new(&out_dir)
                .ancestors()
                .nth(3)
                .and_then(Path::file_name)
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}

fn encoded_rustflags() -> String {
    env::var("CARGO_ENCODED_RUSTFLAGS")
        .unwrap_or_default()
        .split('\u{1f}')
        .filter(|flag| !flag.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn rustc_version() -> String {
    env::var("RUSTC")
        .ok()
        .and_then(|rustc| Command::new(rustc).arg("--version").output().ok())
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}
