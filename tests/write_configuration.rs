//! Bootstrap config-writer round trip: the DOTOS-to-rkyv `lojix-write-configuration`
//! tool produces a startup file that the daemon's
//! `DaemonConfiguration::from_rkyv_file` reads back unchanged. This is the
//! DOTOS-to-binary boundary at deploy time — the daemon never parses DOTOS.

use std::process::Command;

use lojix::DaemonConfiguration;

fn write_configuration(request: &str, output: &std::path::Path) -> DaemonConfiguration {
    let status = Command::new(env!("CARGO_BIN_EXE_lojix-write-configuration"))
        .arg(request)
        .status()
        .expect("run lojix-write-configuration");
    assert!(status.success(), "writer exited with failure");
    DaemonConfiguration::from_rkyv_file(output).expect("daemon reads back the startup file")
}

#[test]
fn write_configuration_round_trips_through_rkyv() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("startup.rkyv");
    let request = format!(
        "ConfigurationWriteRequest.{{/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix ouranos 60 TestDefaults.{{goldragon prometheus Hermetic github:LiGoldragon/CriomOS-test-cluster /var/lib/lojix/cluster.dotos}} {}}}",
        output.display()
    );

    let configuration = write_configuration(&request, &output);
    assert_eq!(
        configuration.ordinary_socket_path,
        "/run/lojix/ordinary.sock"
    );
    assert_eq!(configuration.ordinary_socket_mode, 0o660);
    assert_eq!(configuration.owner_socket_path, "/run/lojix/owner.sock");
    assert_eq!(configuration.owner_socket_mode, 0o600);
    assert_eq!(configuration.state_directory_path, "/var/lib/lojix");
    assert_eq!(configuration.daemon_host, "ouranos");
    assert_eq!(configuration.effect_timeout_seconds, 60);
    let test_defaults = configuration
        .test_defaults
        .expect("the (TestDefaults …) form lowers to a baked fixture");
    assert_eq!(test_defaults.cluster, "goldragon");
    assert_eq!(test_defaults.default_vm_host, "prometheus");
    assert_eq!(
        test_defaults.test_flake,
        "github:LiGoldragon/CriomOS-test-cluster"
    );
    assert_eq!(
        test_defaults.proposal_source,
        "/var/lib/lojix/cluster.dotos"
    );
}

/// The production posture: a `NoTestDefaults` choice lowers to `None`, so the
/// daemon bakes no per-node test fixture and a bare `(Check …)`/`(Run …)`
/// rejects with `NoTestDefaults` instead of resolving against a baked cluster.
#[test]
fn write_configuration_bakes_no_test_defaults_for_production() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("startup.rkyv");
    let request = format!(
        "ConfigurationWriteRequest.{{/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix ouranos 60 NoTestDefaults {}}}",
        output.display()
    );

    let configuration = write_configuration(&request, &output);
    assert_eq!(configuration.daemon_host, "ouranos");
    assert_eq!(configuration.effect_timeout_seconds, 60);
    assert!(
        configuration.test_defaults.is_none(),
        "a production node bakes no test-op fixture"
    );
}

#[test]
fn write_configuration_rejects_a_zero_effect_timeout() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("startup.rkyv");
    let request = format!(
        "ConfigurationWriteRequest.{{/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix ouranos 0 NoTestDefaults {}}}",
        output.display()
    );
    let status = Command::new(env!("CARGO_BIN_EXE_lojix-write-configuration"))
        .arg(request)
        .status()
        .expect("run lojix-write-configuration");
    assert!(
        !status.success(),
        "zero must not become an unbounded timeout"
    );
    assert!(
        !output.exists(),
        "writer must not emit invalid startup config"
    );
}
