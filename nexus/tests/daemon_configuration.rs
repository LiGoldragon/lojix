//! Isolated process witnesses for the zero-argument Lojix Nexus.

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use lojix::daemon::NexusReadiness;
use lojix::{LegacyConfigurationArchivable as _, LegacyStartupConfiguration};
use signal::{ByteViewable, FrameBody, FrameCapacity, FrameReading, FrameWriting};
use signal::{Restorable as _, Signalizable as _};

/// The backstop, and nothing else. The success path waits on the readiness the
/// Nexus announces, not on a clock: a five-second deadline that holds on this
/// machine is a lie on a loaded remote builder, and the test that believed it
/// failed there while passing here. This bound exists only so that a Nexus
/// wedged before readiness cannot take the harness down with it, and it is far
/// above any real startup on any builder.
const READINESS_BACKSTOP: Duration = Duration::from_secs(300);

fn text(value: &str) -> String {
    value.to_owned()
}

#[test]
fn legacy_configuration_archive_remains_available_to_the_offline_migrator() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("daemon-configuration.rkyv");
    let configuration = LegacyStartupConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("owner.sock").display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory.path().join("state").display().to_string(),
        store_path: directory.path().join("lojix.sema").display().to_string(),
        daemon_host: "fixture-daemon".to_string(),
        test_defaults: None,
    };
    configuration.write_rkyv_file(&path).expect("write archive");
    assert_eq!(
        LegacyStartupConfiguration::from_rkyv_file(&path).expect("decode archive"),
        configuration
    );
}

#[test]
fn daemon_rejects_startup_arguments_before_discovering_state() {
    let directory = tempfile::tempdir().expect("temporary isolation roots");
    let output = Command::new(env!("CARGO_BIN_EXE_lojix-nexus"))
        .arg("legacy-startup.rkyv")
        .env("XDG_RUNTIME_DIR", directory.path().join("runtime"))
        .env("XDG_STATE_HOME", directory.path().join("state"))
        .output()
        .expect("run daemon with forbidden argument");
    assert!(!output.status.success());
    assert!(output_text(&output).contains("starts with no arguments"));
    assert!(!directory.path().join("state/lojix/lojix.sema").exists());
}

#[test]
fn zero_argument_daemon_persists_lifecycle_and_uses_desired_sockets_on_restart() {
    let directory = tempfile::tempdir().expect("temporary isolation roots");
    let runtime_root = directory.path().join("runtime");
    let state_root = directory.path().join("state");
    fs::create_dir_all(&runtime_root).expect("isolated runtime root");
    fs::create_dir_all(&state_root).expect("isolated state root");
    let initial_runtime = runtime_root.join("lojix");
    let initial_state = state_root.join("lojix");
    let ordinary_socket = initial_runtime.join("ordinary.sock");
    let meta_socket = initial_runtime.join("meta.sock");
    let store = initial_state.join("lojix.sema");

    let mut daemon = start_daemon(&runtime_root, &state_root);
    assert_eq!(
        announced_readiness(&mut daemon, "first start"),
        vec![
            ordinary_socket.display().to_string(),
            meta_socket.display().to_string(),
        ],
        "the Nexus must name both bound listeners when it announces readiness"
    );

    let ordinary_configuration = configuration(
        &ordinary_socket,
        &meta_socket,
        &initial_state,
        "ordinary-configured-host",
    );
    let signal_lojix::Response::Configured(ordinary_receipt) = ordinary_exchange(
        &ordinary_socket,
        signal_lojix::Query::Configure(ordinary_configuration.clone()),
    ) else {
        panic!("ordinary Configure must return a typed receipt")
    };
    assert_eq!(
        ordinary_receipt.lojix_nexus_configuration,
        ordinary_configuration
    );
    assert!(!ordinary_receipt.meta_configure_occurred);

    let next_runtime = runtime_root.join("configured");
    let next_ordinary_socket = next_runtime.join("ordinary.sock");
    let next_meta_socket = next_runtime.join("meta.sock");
    let meta_configuration = configuration(
        &next_ordinary_socket,
        &next_meta_socket,
        &initial_state,
        "meta-configured-host",
    );
    let meta_signal_lojix::Response::Configured(meta_receipt) = meta_exchange(
        &meta_socket,
        meta_signal_lojix::Query::Configure(meta_configuration.clone()),
    ) else {
        panic!("meta Configure must return a typed receipt")
    };
    assert_eq!(meta_receipt.lojix_nexus_configuration, meta_configuration);
    assert!(meta_receipt.meta_configure_occurred);

    assert!(matches!(
        ordinary_exchange(
            &ordinary_socket,
            signal_lojix::Query::Configure(ordinary_configuration),
        ),
        signal_lojix::Response::ConfigurationRejected(signal_lojix::ConfigurationRejection {
            configuration_rejection_reason:
                signal_lojix::ConfigurationRejectionReason::OrdinaryConfigureClosed,
        })
    ));

    let meta_signal_lojix::Response::ConfigurationReversed(reversed) =
        meta_exchange(&meta_socket, meta_signal_lojix::Query::ReverseConfiguration)
    else {
        panic!("meta reversal must return the desired configuration")
    };
    assert_eq!(reversed.lojix_nexus_configuration, meta_configuration);
    assert!(!reversed.meta_configure_occurred);

    stop_daemon(daemon, "configured daemon");
    assert!(store.exists(), "stable XDG-discovered Sema must persist");

    let mut restarted = start_daemon(&runtime_root, &state_root);
    assert_eq!(
        announced_readiness(&mut restarted, "restart"),
        vec![
            next_ordinary_socket.display().to_string(),
            next_meta_socket.display().to_string(),
        ],
        "a restarted Nexus must announce the desired listeners it resumed onto"
    );
    let reply = ordinary_exchange(
        &next_ordinary_socket,
        signal_lojix::Query::Query(signal_lojix::Selection::ByNode(
            signal_lojix::NodeSelector {
                cluster_name: text("fresh-cluster"),
                node_name: text("fresh-node"),
                requested_generation_artifact_option: None,
            },
        )),
    );
    assert!(matches!(reply, signal_lojix::Response::Queried(_)));
    stop_daemon(restarted, "restarted daemon");
}

fn configuration(
    ordinary_socket: &Path,
    meta_socket: &Path,
    state_directory: &Path,
    daemon_host: &str,
) -> signal_lojix::LojixNexusConfiguration {
    signal_lojix::LojixNexusConfiguration {
        ordinary_socket_path: ordinary_socket.display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: meta_socket.display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: state_directory.display().to_string(),
        daemon_host: daemon_host.to_string(),
        test_defaults_choice: signal_lojix::TestDefaultsChoice::NoTestDefaults,
    }
}

fn start_daemon(runtime_root: &Path, state_root: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_lojix-nexus"))
        .env("XDG_RUNTIME_DIR", runtime_root)
        .env("XDG_STATE_HOME", state_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start zero-argument lojix-nexus")
}

/// Wait for the event the Nexus announces when both listeners are bound, and
/// answer with the socket paths it named. The wait ends on the announcement, or
/// on the child's standard output closing — which is what happens when the
/// Nexus exits before becoming ready, so a failed startup is reported at once
/// rather than after a deadline.
fn announced_readiness(daemon: &mut Child, occasion: &str) -> Vec<String> {
    let stdout = daemon.stdout.take().expect("piped Nexus standard output");
    let (announced, arrival) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout)
            .lines()
            .map_while(std::result::Result::ok)
        {
            if line.starts_with(<lojix::NexusConfiguration as NexusReadiness>::READY) {
                let _ = announced.send(Some(line));
                return;
            }
        }
        let _ = announced.send(None);
    });
    match arrival.recv_timeout(READINESS_BACKSTOP) {
        Ok(Some(line)) => line
            .trim_start_matches('(')
            .trim_end_matches(')')
            .split_whitespace()
            .skip(1)
            .map(str::to_owned)
            .collect(),
        Ok(None) => panic!(
            "lojix-nexus closed its standard output without announcing readiness on {occasion}"
        ),
        Err(_) => panic!("lojix-nexus never announced readiness on {occasion}"),
    }
}

fn exchange(socket: &Path, bytes: Vec<u8>) -> Vec<u8> {
    let mut stream = UnixStream::connect(socket).expect("connect typed client");
    stream
        .write_frame(&FrameBody::from(bytes), FrameCapacity::default())
        .expect("write request");
    stream
        .read_frame(FrameCapacity::default())
        .expect("read response")
        .bytes()
        .to_vec()
}

fn ordinary_exchange(socket: &Path, request: signal_lojix::Query) -> signal_lojix::Response {
    let signal = request.signalize().expect("archive ordinary query");
    signal::Signal::<signal_lojix::Response>::from(exchange(socket, signal.bytes().to_vec()))
        .restore()
        .expect("restore ordinary response")
}

fn meta_exchange(socket: &Path, request: meta_signal_lojix::Query) -> meta_signal_lojix::Response {
    let signal = request.signalize().expect("archive meta query");
    signal::Signal::<meta_signal_lojix::Response>::from(exchange(socket, signal.bytes().to_vec()))
        .restore()
        .expect("restore meta response")
}

fn stop_daemon(daemon: Child, name: &str) {
    let process_identifier =
        rustix::process::Pid::from_raw(daemon.id() as i32).expect("child process identifier");
    rustix::process::kill_process(process_identifier, rustix::process::Signal::TERM)
        .expect("send service-manager SIGTERM");
    let output = daemon.wait_with_output().expect("wait for daemon stop");
    assert!(output.status.success(), "{name}: {}", output_text(&output));
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
