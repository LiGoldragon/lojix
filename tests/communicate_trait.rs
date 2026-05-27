//! Communicate trait round-trip (iteration 2, test #4 of 6).
//!
//! Asserts: `UnixSocketCommunicate` (concrete impl of the
//! `Communicate` trait) does a full Input -> Output round trip
//! through the daemon's socket surface. The test spawns the
//! daemon binary, then drives a Submit through the trait, and
//! asserts the returned Output is an Accepted reply with a
//! DatabaseMarker.

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use lojix_next::runtime::communicate::{Communicate, UnixSocketCommunicate};
use lojix_next::{CriomeAuthorization, DeploymentRequest, HorizonView, Input, Output, TargetNode};
use tempfile::TempDir;

struct DaemonProcess {
    child: Child,
}

impl DaemonProcess {
    fn spawn(socket: &str, state: &str, gc: &str, sema_database: &str) -> Self {
        let configuration = format!(
            "([{socket}] [{state}] [{gc}] [{sema_database}] \
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

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_communicate_trait_round_trip() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("lojix-next.sock");
    let socket_text = socket_path.to_string_lossy().into_owned();
    let state_directory = temp.path().join("state");
    let gc_root_directory = temp.path().join("gcroots");
    let sema_database_path = temp.path().join("sema.redb");

    let _daemon = DaemonProcess::spawn(
        &socket_text,
        &state_directory.to_string_lossy(),
        &gc_root_directory.to_string_lossy(),
        &sema_database_path.to_string_lossy(),
    );
    SocketWaiter::new(socket_path.clone()).block_until_present();

    let mut communicate = UnixSocketCommunicate::new(socket_path);

    let output = communicate
        .send_request(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: communicate-trait".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("communicate round trip");

    let accepted = matches!(output, Output::Accepted(_));
    assert!(
        accepted,
        "Submit through Communicate must yield Accepted, got {output:?}"
    );

    let marker = output.database_marker();
    // The marker should be a non-zero counter — the Submit triggered
    // several sema-engine writes (plan, build record, generation
    // record, copy, activation), so the transaction counter is
    // strictly positive.
    assert!(
        marker.transaction_counter.0 > 0,
        "DatabaseMarker counter must be positive, got {}",
        marker.transaction_counter.0
    );
    assert!(
        !marker.state_hash.0.is_empty(),
        "DatabaseMarker state hash must be non-empty"
    );
}
