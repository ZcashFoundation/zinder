//! Probe that a service binary's `healthcheck` subcommand follows the
//! container healthcheck contract.
//!
//! Release service images carry no shell or HTTP client, so the image
//! `HEALTHCHECK` and the Compose healthchecks run `<binary> healthcheck`. The
//! probe runs the real binary against a stub operational endpoint and checks
//! the exit status, the requested path, and the address resolution order.

use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Command, Output},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use eyre::{Result, WrapErr as _, ensure, eyre};
use parking_lot::Mutex;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(1);
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Loopback HTTP endpoint that answers every request with one fixed status
/// and records each request line.
pub struct OpsEndpointStub {
    addr: SocketAddr,
    request_lines: Arc<Mutex<Vec<String>>>,
    shutdown: Arc<AtomicBool>,
    accept_handle: Option<JoinHandle<()>>,
}

impl OpsEndpointStub {
    /// Starts a stub on an ephemeral loopback port. It runs until dropped.
    pub fn start(status_line: &'static str, body: &'static str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let request_lines = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let accept_request_lines = Arc::clone(&request_lines);
        let accept_shutdown = Arc::clone(&shutdown);
        let response = format!(
            "HTTP/1.1 {status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let accept_handle = thread::spawn(move || {
            while !accept_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Some(request_line) = answer(stream, response.as_bytes()) {
                            accept_request_lines.lock().push(request_line);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(ACCEPT_POLL_INTERVAL);
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Self {
            addr,
            request_lines,
            shutdown,
            accept_handle: Some(accept_handle),
        })
    }

    /// Address the stub listens on.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Request lines received so far, such as `GET /readyz HTTP/1.1`.
    #[must_use]
    pub fn request_lines(&self) -> Vec<String> {
        self.request_lines.lock().clone()
    }
}

impl Drop for OpsEndpointStub {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.accept_handle.take()
            && handle.join().is_err()
        {
            tracing::warn!(
                target: "zinder::testkit",
                "OpsEndpointStub accept thread panicked during shutdown"
            );
        }
    }
}

fn answer(mut stream: TcpStream, response: &[u8]) -> Option<String> {
    stream.set_nonblocking(false).ok()?;
    stream.set_read_timeout(Some(READ_TIMEOUT)).ok()?;
    let mut request = Vec::new();
    let mut chunk = [0_u8; 512];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read_bytes = stream.read(&mut chunk).ok()?;
        if read_bytes == 0 {
            break;
        }
        request.extend_from_slice(chunk.get(..read_bytes).unwrap_or_default());
    }
    stream.write_all(response).ok()?;
    let request = String::from_utf8_lossy(&request);
    request.lines().next().map(str::to_owned)
}

/// Fails unless `<executable> healthcheck` follows the container healthcheck
/// contract.
///
/// The binary runs with a cleared environment, so the check also proves that
/// the subcommand needs no service configuration. It must exit 0 when the
/// endpoint answers 200 on `/readyz` and 1 with a one-line reason on any other
/// status or a refused connection, and honor `--path`. It must resolve the
/// endpoint from `--addr`, then `ZINDER_OPS__LISTEN_ADDR`, then `[ops]
/// listen_addr` in the global `--config` file, ignoring the file's other keys
/// and falling back when the file is missing or not TOML.
pub fn check_binary_healthcheck(executable: impl AsRef<Path>) -> Result<()> {
    let executable = executable.as_ref();
    let healthy = OpsEndpointStub::start("200 OK", r#"{"status":"ready"}"#)?;
    let unhealthy = OpsEndpointStub::start(
        "503 Service Unavailable",
        r#"{"status":"not_ready","cause":"starting"}"#,
    )?;
    let healthy_addr = healthy.addr().to_string();

    let output = run_healthcheck(executable, &[], &["--addr", &healthy_addr], None)?;
    ensure_exit(executable, &output, 0, "a 200 /readyz")?;

    let output = run_healthcheck(
        executable,
        &[],
        &["--addr", &healthy_addr, "--path", "/healthz"],
        None,
    )?;
    ensure_exit(executable, &output, 0, "a 200 /healthz")?;

    let unspecified_env_addr = format!("0.0.0.0:{}", healthy.addr().port());
    let output = run_healthcheck(executable, &[], &[], Some(&unspecified_env_addr))?;
    ensure_exit(executable, &output, 0, "ZINDER_OPS__LISTEN_ADDR on 0.0.0.0")?;

    check_config_file_resolution(executable, &healthy, &unhealthy)?;

    ensure!(
        healthy.request_lines()
            == [
                "GET /readyz HTTP/1.1",
                "GET /healthz HTTP/1.1",
                "GET /readyz HTTP/1.1",
                "GET /readyz HTTP/1.1",
                "GET /readyz HTTP/1.1",
                "GET /readyz HTTP/1.1",
                "GET /readyz HTTP/1.1"
            ],
        "{} healthcheck requested {:?}",
        executable.display(),
        healthy.request_lines()
    );

    let output = run_healthcheck(
        executable,
        &[],
        &["--addr", &unhealthy.addr().to_string()],
        Some(&healthy_addr),
    )?;
    ensure_exit(executable, &output, 1, "a 503 /readyz")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        stderr.lines().count() == 1
            && stderr.contains("answered HTTP 503: {\"status\":\"not_ready\""),
        "{} healthcheck reported {stderr:?} for a 503",
        executable.display()
    );

    let refused_addr = closed_loopback_addr()?;
    let output = run_healthcheck(
        executable,
        &[],
        &["--addr", &refused_addr.to_string()],
        None,
    )?;
    ensure_exit(executable, &output, 1, "a refused connection")?;
    ensure!(
        String::from_utf8_lossy(&output.stderr).contains("could not connect"),
        "{} healthcheck reported {:?} for a refused connection",
        executable.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Runs the `--config` cases, which request `/readyz` from `healthy` four
/// times.
fn check_config_file_resolution(
    executable: &Path,
    healthy: &OpsEndpointStub,
    unhealthy: &OpsEndpointStub,
) -> Result<()> {
    let config_dir = tempfile::tempdir()?;
    let healthy_config = config_dir.path().join("healthy.toml");
    fs::write(
        &healthy_config,
        format!(
            "[network]\nname = \"not-a-network\"\n\n[ops]\nlisten_addr = \"0.0.0.0:{}\"\nnot_an_ops_key = true\n",
            healthy.addr().port()
        ),
    )?;
    let unhealthy_config = config_dir.path().join("unhealthy.toml");
    fs::write(
        &unhealthy_config,
        format!("[ops]\nlisten_addr = \"{}\"\n", unhealthy.addr()),
    )?;
    let not_toml_config = config_dir.path().join("not-toml.toml");
    fs::write(&not_toml_config, "[ops\nlisten_addr = ")?;
    let healthy_addr = healthy.addr().to_string();

    let output = run_healthcheck(
        executable,
        &["--config", path_str(&healthy_config)?],
        &[],
        None,
    )?;
    ensure_exit(
        executable,
        &output,
        0,
        "[ops] listen_addr in the --config file",
    )?;

    let output = run_healthcheck(
        executable,
        &["--config", path_str(&unhealthy_config)?],
        &[],
        Some(&healthy_addr),
    )?;
    ensure_exit(
        executable,
        &output,
        0,
        "ZINDER_OPS__LISTEN_ADDR over the --config file",
    )?;

    for unusable_config in [&not_toml_config, &config_dir.path().join("missing.toml")] {
        let output = run_healthcheck(
            executable,
            &[],
            &[
                "--config",
                path_str(unusable_config)?,
                "--addr",
                &healthy_addr,
            ],
            None,
        )?;
        ensure_exit(
            executable,
            &output,
            0,
            "--addr with an unusable --config file",
        )?;
    }
    Ok(())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre!("{} is not UTF-8", path.display()))
}

/// Runs `<executable> <global_args> healthcheck <args>` with a cleared
/// environment, setting only `ZINDER_OPS__LISTEN_ADDR` when given.
fn run_healthcheck(
    executable: &Path,
    global_args: &[&str],
    args: &[&str],
    ops_listen_addr: Option<&str>,
) -> Result<Output> {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .args(global_args)
        .arg("healthcheck")
        .args(args);
    if let Some(ops_listen_addr) = ops_listen_addr {
        command.env("ZINDER_OPS__LISTEN_ADDR", ops_listen_addr);
    }
    command.output().wrap_err_with(|| {
        format!(
            "run {} {global_args:?} healthcheck {args:?}",
            executable.display()
        )
    })
}

fn ensure_exit(executable: &Path, output: &Output, code: i32, case: &str) -> Result<()> {
    ensure!(
        output.status.code() == Some(code),
        "{} healthcheck exited with {} for {case}, expected {code}; stderr: {}",
        executable.display(),
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Port 1 is privileged and unassigned on loopback, so nothing a test binds can
/// take it, unlike a port released by a dropped listener.
fn closed_loopback_addr() -> Result<SocketAddr> {
    Ok(SocketAddr::from(([127, 0, 0, 1], 1)))
}
