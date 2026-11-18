//! Probe that a per-service release image's default configuration file holds
//! only the container listener defaults and resolves through the service
//! binary.
//!
//! Each per-service image installs one file from `deploy/config/images/` at
//! `/etc/zinder/config.toml` and passes it with `--config`. The service config
//! rejects unknown fields, and an operator's mounted file replaces the default
//! as a whole, so the file must stay parseable and must not carry settings
//! beyond the listeners and `security.allow_public_bind`.

use std::{path::Path, process::Command};

use eyre::{Result, WrapErr as _, ensure, eyre};

/// Fails unless a release image's default config holds exactly its listeners.
///
/// Apart from comments and blank lines, `deploy/config/images/<config_file_name>`
/// must consist of one `[section]` with its `listen_addr` for each
/// `(section, listen_addr)` in `listeners`, in order, followed by
/// `[security]` with `allow_public_bind = true`. `<executable> --config <file>
/// --print-config` must then resolve those values with only `required_env`
/// set.
pub fn check_release_image_config(
    executable: impl AsRef<Path>,
    config_file_name: &str,
    listeners: &[(&str, &str)],
    required_env: &[(&str, &str)],
) -> Result<()> {
    let executable = executable.as_ref();
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/config/images")
        .join(config_file_name);
    let config_text = std::fs::read_to_string(&config_path)
        .wrap_err_with(|| format!("read {}", config_path.display()))?;
    let setting_lines = config_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let mut expected_lines = Vec::new();
    for (section, listen_addr) in listeners {
        expected_lines.push(format!("[{section}]"));
        expected_lines.push(format!("listen_addr = \"{listen_addr}\""));
    }
    expected_lines.push("[security]".to_owned());
    expected_lines.push("allow_public_bind = true".to_owned());
    ensure!(
        setting_lines == expected_lines,
        "{} must hold only {expected_lines:?}, found {setting_lines:?}",
        config_path.display()
    );

    let config_arg = config_path
        .to_str()
        .ok_or_else(|| eyre!("{} is not UTF-8", config_path.display()))?;
    let output = Command::new(executable)
        .env_clear()
        .envs(required_env.iter().copied())
        .args(["--config", config_arg, "--print-config"])
        .output()
        .wrap_err_with(|| format!("run {} --print-config", executable.display()))?;
    ensure!(
        output.status.success(),
        "{} --config {} --print-config failed: {}",
        executable.display(),
        config_path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = String::from_utf8(output.stdout)?;
    for expected_line in expected_lines.iter().filter(|line| !line.starts_with('[')) {
        ensure!(
            rendered.contains(expected_line.as_str()),
            "{} did not resolve {expected_line}: {rendered}",
            executable.display()
        );
    }
    Ok(())
}
