//! Probe that a service binary resolves container listener settings via
//! environment variables without packaged legacy TOML configuration files.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use eyre::{Result, WrapErr as _, ensure, eyre};

/// Fails unless the repository removes legacy `deploy/config/` files and the
/// service binary resolves container listener and public-bind environment
/// variables through `--print-config`.
pub fn check_release_image_config(
    executable: impl AsRef<Path>,
    listeners: &[(&str, &str)],
    required_env: &[(&str, &str)],
) -> Result<()> {
    let executable = executable.as_ref();
    let legacy_config_dir = workspace_root(&crate::manifest_dir!())?.join("deploy/config");
    ensure!(
        !legacy_config_dir.exists(),
        "{} must be removed in zero-TOML deployments",
        legacy_config_dir.display()
    );

    let mut listener_env = Vec::with_capacity(listeners.len() + 1);
    let mut expected_lines = Vec::with_capacity(listeners.len() + 1);
    for (section, listen_addr) in listeners {
        let env_name = format!("ZINDER_{}__LISTEN_ADDR", section.to_ascii_uppercase());
        listener_env.push((env_name, (*listen_addr).to_owned()));
        expected_lines.push(format!("listen_addr = \"{listen_addr}\""));
    }
    listener_env.push((
        "ZINDER_SECURITY__ALLOW_PUBLIC_BIND".to_owned(),
        "true".to_owned(),
    ));
    expected_lines.push("allow_public_bind = true".to_owned());

    let output = Command::new(executable)
        .env_clear()
        .envs(required_env.iter().copied())
        .envs(
            listener_env
                .iter()
                .map(|(env_key, env_val)| (env_key.as_str(), env_val.as_str())),
        )
        .args(["--print-config"])
        .output()
        .wrap_err_with(|| format!("run {} --print-config", executable.display()))?;
    ensure!(
        output.status.success(),
        "{} --print-config failed: {}",
        executable.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = String::from_utf8(output.stdout)?;
    for expected_line in &expected_lines {
        ensure!(
            rendered.contains(expected_line.as_str()),
            "{} did not resolve {expected_line}: {rendered}",
            executable.display()
        );
    }
    Ok(())
}

/// The nearest ancestor of `manifest_dir` whose `Cargo.toml` declares
/// `[workspace]`.
fn workspace_root(manifest_dir: &Path) -> Result<PathBuf> {
    manifest_dir
        .ancestors()
        .find(|directory| {
            std::fs::read_to_string(directory.join("Cargo.toml"))
                .is_ok_and(|manifest| manifest.lines().any(|line| line.trim() == "[workspace]"))
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| eyre!("no workspace Cargo.toml above {}", manifest_dir.display()))
}
