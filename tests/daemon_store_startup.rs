//! Daemon-startup compatibility projection tests.

use std::process::Command;

use lojix::DaemonConfiguration;
use sema_engine::{Engine, EngineOpen, SchemaVersion};

#[test]
fn incompatible_store_is_a_named_daemon_startup_rejection_before_binding_sockets() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let state_directory = directory.path().join("state");
    std::fs::create_dir(&state_directory).expect("state directory");
    let store = state_directory.join("lojix.sema");
    let legacy = Engine::open(EngineOpen::new(store, SchemaVersion::new(1)))
        .expect("create disposable schema-one store");
    drop(legacy);

    let ordinary_socket = directory.path().join("ordinary.sock");
    let owner_socket = directory.path().join("owner.sock");
    let configuration_path = directory.path().join("daemon-configuration.rkyv");
    DaemonConfiguration {
        ordinary_socket_path: ordinary_socket.display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: owner_socket.display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: state_directory.display().to_string(),
        daemon_host: "dune".to_string(),
        test_defaults: None,
    }
    .write_rkyv_file(&configuration_path)
    .expect("write daemon configuration");

    let output = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration_path)
        .output()
        .expect("run daemon against disposable incompatible store");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("(DaemonRejected (StoreStartupCompatibility ["),
        "startup failure must name its typed public category: {stderr}"
    );
    assert!(
        !ordinary_socket.exists() && !owner_socket.exists(),
        "the incompatible store must stop startup before either socket is bound"
    );
}
