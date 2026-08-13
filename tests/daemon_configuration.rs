//! Daemon startup configuration archive tests.
//!
//! Launch tooling writes a binary rkyv startup file before exec; the daemon only
//! reads that binary file. The process witness below follows the same writer →
//! archive → daemon → two typed clients path used by the NixOS service.

use std::fs;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use lojix::DaemonConfiguration;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn daemon_configuration_round_trips_through_rkyv_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("daemon-configuration.rkyv");
    let configuration = DaemonConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("owner.sock").display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: directory.path().join("state").display().to_string(),
        store_path: directory
            .path()
            .join("configured-lojix-store.db")
            .display()
            .to_string(),
        daemon_host: "fixture-daemon".to_string(),
        effect_timeout_seconds: 60,
        test_defaults: Some(lojix::TestDefaults {
            cluster: "fixture-cluster".to_string(),
            default_vm_host: "fixture-vm-host".to_string(),
            default_mode: lojix::TestMode::Hermetic,
            test_flake: "github:fixture-owner/fixture-test-flake".to_string(),
            test_nix_system: "x86_64-linux".to_string(),
            test_output_selector: "checks.fixture-a".to_string(),
            proposal_source: String::new(),
        }),
    };

    configuration
        .write_rkyv_file(&path)
        .expect("write rkyv startup configuration");
    let decoded = DaemonConfiguration::from_rkyv_file(&path).expect("decode rkyv startup file");

    assert_eq!(decoded, configuration);
}

/// This is the production service boundary, kept process-level on purpose:
/// `lojix-write-configuration` receives the exact `ConfigurationWriteRequest`
/// archive shape CriomOS writes, `lojix-daemon` receives only that archive, and
/// the public owner and ordinary binaries construct their verified Interface
/// values before sending frames to the two sockets.
///
/// The first run starts with an absent SEMA file. The owner `Pin` deliberately
/// names a generation that cannot exist in a virgin store, so a typed
/// `PinRejected` reply proves the owner ingress and its rejection path ran;
/// the ordinary `Query` then proves the independent ordinary ingress and its
/// empty-store reply. Restarting the same configured store ensures normal
/// service termination does not leave a startup-incompatible store behind.
#[test]
fn fresh_store_daemon_serves_owner_and_ordinary_interfaces_after_restart() {
    let directory = tempfile::tempdir().expect("temporary service directory");
    let state_directory = directory.path().join("state");
    fs::create_dir(&state_directory).expect("systemd tmpfiles state directory");
    let ordinary_socket = directory.path().join("ordinary.sock");
    let owner_socket = directory.path().join("owner.sock");
    let store = state_directory.join("lojix.sema");
    let archive = directory.path().join("lojix-startup.rkyv");

    assert!(
        !store.exists(),
        "the first daemon invocation must create the configured fresh store"
    );
    write_service_configuration(
        &ordinary_socket,
        &owner_socket,
        &state_directory,
        &store,
        &archive,
    );

    let mut daemon = start_daemon(&archive);
    wait_for_socket(&ordinary_socket, &mut daemon, "ordinary");
    wait_for_socket(&owner_socket, &mut daemon, "owner");

    let owner_reply = run_client(
        env!("CARGO_BIN_EXE_meta-lojix"),
        "LOJIX_OWNER_SOCKET",
        &owner_socket,
        "Pin.(fresh-cluster fresh-node 1 fresh-pin)",
    );
    assert!(
        owner_reply.status.success(),
        "owner request must receive a typed reply: {}",
        output_text(&owner_reply)
    );
    assert!(
        String::from_utf8_lossy(&owner_reply.stdout).contains("PinRejected"),
        "a virgin store must report the typed owner rejection: {}",
        output_text(&owner_reply)
    );

    let ordinary_reply = run_client(
        env!("CARGO_BIN_EXE_lojix"),
        "LOJIX_ORDINARY_SOCKET",
        &ordinary_socket,
        "Query.ByNode.(fresh-cluster fresh-node None)",
    );
    assert!(
        ordinary_reply.status.success(),
        "ordinary request must receive a typed reply: {}",
        output_text(&ordinary_reply)
    );
    assert!(
        String::from_utf8_lossy(&ordinary_reply.stdout).contains("Queried"),
        "a virgin store must report the typed ordinary reply: {}",
        output_text(&ordinary_reply)
    );

    stop_daemon(daemon, "first daemon");
    assert!(
        store.exists(),
        "the first daemon owns the configured store path"
    );

    let mut restarted_daemon = start_daemon(&archive);
    wait_for_socket(
        &ordinary_socket,
        &mut restarted_daemon,
        "ordinary after restart",
    );
    wait_for_socket(&owner_socket, &mut restarted_daemon, "owner after restart");

    let restarted_reply = run_client(
        env!("CARGO_BIN_EXE_lojix"),
        "LOJIX_ORDINARY_SOCKET",
        &ordinary_socket,
        "Query.ByNode.(fresh-cluster fresh-node None)",
    );
    assert!(
        restarted_reply.status.success(),
        "restarted ordinary request must receive a typed reply: {}",
        output_text(&restarted_reply)
    );
    assert!(
        String::from_utf8_lossy(&restarted_reply.stdout).contains("Queried"),
        "the reopened store must remain queryable: {}",
        output_text(&restarted_reply)
    );
    stop_daemon(restarted_daemon, "restarted daemon");
}

fn write_service_configuration(
    ordinary_socket: &Path,
    owner_socket: &Path,
    state_directory: &Path,
    store: &Path,
    archive: &Path,
) {
    let request = format!(
        "ConfigurationWriteRequest.{{{} 432 {} 384 {} {} fresh-daemon 60 NoTestDefaults {}}}",
        ordinary_socket.display(),
        owner_socket.display(),
        state_directory.display(),
        store.display(),
        archive.display(),
    );
    let output = Command::new(env!("CARGO_BIN_EXE_lojix-write-configuration"))
        .arg(request)
        .output()
        .expect("run lojix-write-configuration");
    assert!(
        output.status.success(),
        "CriomOS ConfigurationWriteRequest must produce an archive: {}",
        output_text(&output)
    );
    let configuration =
        DaemonConfiguration::from_rkyv_file(archive).expect("daemon reads writer archive");
    assert_eq!(
        configuration.ordinary_socket_path,
        ordinary_socket.display().to_string()
    );
    assert_eq!(
        configuration.owner_socket_path,
        owner_socket.display().to_string()
    );
    assert_eq!(
        configuration.state_directory_path,
        state_directory.display().to_string()
    );
    assert_eq!(configuration.store_path, store.display().to_string());
    assert!(
        configuration.test_defaults.is_none(),
        "the OS service's NoTestDefaults archive remains production shaped"
    );
}

fn start_daemon(archive: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(archive)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start lojix-daemon")
}

/// A bounded connection retry is a reachability probe, not state polling: the
/// daemon is the producer of socket availability and successful `connect` is
/// the kernel's readiness witness for each independently bound listener.
fn wait_for_socket(path: &Path, daemon: &mut Child, listener: &str) {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if UnixStream::connect(path).is_ok() {
            return;
        }
        if let Some(status) = daemon.try_wait().expect("inspect daemon startup") {
            panic!("lojix-daemon exited before the {listener} listener became reachable: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "lojix-daemon did not make the {listener} listener reachable within {STARTUP_TIMEOUT:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn run_client(binary: &str, socket_environment: &str, socket: &Path, request: &str) -> Output {
    Command::new(binary)
        .env(socket_environment, socket)
        .arg(request)
        .output()
        .expect("run public Lojix client")
}

/// systemd stops `Type=simple` services with SIGTERM. The daemon has no
/// request-shaped shutdown operation, so this is the service lifecycle exit
/// the source-level witness must tolerate before the configured store reopens.
fn stop_daemon(mut daemon: Child, name: &str) {
    let process_identifier =
        rustix::process::Pid::from_raw(daemon.id() as i32).expect("child process identifier");
    rustix::process::kill_process(process_identifier, rustix::process::Signal::Term)
        .expect("send systemd-style SIGTERM");
    let output = daemon.wait_with_output().expect("wait for daemon stop");
    assert!(
        output.status.success(),
        "{name} must exit cleanly after systemd-style SIGTERM: {}",
        output_text(&output)
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("panicked at"),
        "{name} must not panic while serving the fresh-store path: {}",
        output_text(&output)
    );
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
