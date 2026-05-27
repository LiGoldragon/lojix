//! Sandbox-OS witness — build-only pipeline test.
//!
//! Spawns the daemon binary in sandbox mode, the CLI binary sends a
//! NOTA Submit input, the daemon drives the full pipeline (auth →
//! plan → build → gc-pin → copy → activate), writes a GenerationRecord
//! to its in-memory store, and the test asserts on the printed output
//! and the daemon's recorded generations via a follow-up Query input.
//!
//! Runs entirely on `echo`-based sandbox commands so it executes
//! cleanly under `nix flake check` without any external nix install.

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

struct DaemonProcess {
    child: Child,
}

impl DaemonProcess {
    fn spawn(socket: &str, state: &str, gc: &str, sema: &str) -> Self {
        let configuration = format!(
            "([{socket}] [{state}] [{gc}] [{sema}] \
             ([nix-build-sandbox] [nix-copy-sandbox] [nixos-rebuild-sandbox]))"
        );
        let child = Command::new(env!("CARGO_BIN_EXE_lojix-next-daemon"))
            .arg(configuration)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn daemon");
        Self { child }
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct SocketWaiter {
    path: std::path::PathBuf,
}

impl SocketWaiter {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }

    fn block_until_present(&self) {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(5) {
            if self.path.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("daemon socket did not appear at {}", self.path.display());
    }
}

#[test]
fn lojix_next_build_only_pipeline_on_sandbox() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("lojix-next.sock");
    let socket_text = socket_path.to_string_lossy().into_owned();
    let state_directory = temp.path().join("state");
    let gc_root_directory = temp.path().join("gcroots");
    let sema_database_path = temp.path().join("lojix.redb");

    let _daemon = DaemonProcess::spawn(
        &socket_text,
        &state_directory.to_string_lossy(),
        &gc_root_directory.to_string_lossy(),
        &sema_database_path.to_string_lossy(),
    );
    SocketWaiter::new(socket_path.clone()).block_until_present();

    let submit_output = Command::new(env!("CARGO_BIN_EXE_lojix-next"))
        .env("LOJIX_NEXT_SOCKET", &socket_path)
        .arg("(Submit ([horizon: sandbox] [nspawn-dune] OperatorAllowlist))")
        .output()
        .expect("run submit cli");
    assert!(
        submit_output.status.success(),
        "submit stderr: {}",
        String::from_utf8_lossy(&submit_output.stderr)
    );
    let submit_text = String::from_utf8_lossy(&submit_output.stdout)
        .trim()
        .to_owned();
    assert!(
        submit_text.starts_with("(Accepted"),
        "expected Accepted output, got {submit_text}"
    );

    let query_output = Command::new(env!("CARGO_BIN_EXE_lojix-next"))
        .env("LOJIX_NEXT_SOCKET", &socket_path)
        .arg("(Query 1)")
        .output()
        .expect("run query cli");
    assert!(
        query_output.status.success(),
        "query stderr: {}",
        String::from_utf8_lossy(&query_output.stderr)
    );
    let query_text = String::from_utf8_lossy(&query_output.stdout)
        .trim()
        .to_owned();
    assert!(
        query_text.starts_with("(Snapshot"),
        "expected Snapshot output, got {query_text}"
    );
}
