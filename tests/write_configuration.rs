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
        "ConfigurationWriteRequest.{{/run/fixture-lojix/ordinary.sock 432 /run/fixture-lojix/owner.sock 384 /var/lib/fixture-lojix /var/lib/fixture-lojix/configured-lojix-store.db fixture-daemon TestDefaults.{{fixture-cluster fixture-vm-host Hermetic github:fixture-owner/fixture-test-flake x86_64-linux checks.fixture-a /var/lib/fixture-lojix/proposal.datom}} {}}}",
        output.display()
    );

    let configuration = write_configuration(&request, &output);
    assert_eq!(
        configuration.ordinary_socket_path,
        "/run/fixture-lojix/ordinary.sock"
    );
    assert_eq!(configuration.ordinary_socket_mode, 0o660);
    assert_eq!(
        configuration.owner_socket_path,
        "/run/fixture-lojix/owner.sock"
    );
    assert_eq!(configuration.owner_socket_mode, 0o600);
    assert_eq!(configuration.state_directory_path, "/var/lib/fixture-lojix");
    assert_eq!(
        configuration.store_path,
        "/var/lib/fixture-lojix/configured-lojix-store.db"
    );
    assert_eq!(configuration.daemon_host, "fixture-daemon");
    let test_defaults = configuration
        .test_defaults
        .expect("the (TestDefaults …) form lowers to a baked fixture");
    assert_eq!(test_defaults.cluster, "fixture-cluster");
    assert_eq!(test_defaults.default_vm_host, "fixture-vm-host");
    assert_eq!(
        test_defaults.test_flake,
        "github:fixture-owner/fixture-test-flake"
    );
    assert_eq!(
        test_defaults.proposal_source,
        "/var/lib/fixture-lojix/proposal.datom"
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
        "ConfigurationWriteRequest.{{/run/fixture-lojix/ordinary.sock 432 /run/fixture-lojix/owner.sock 384 /var/lib/fixture-lojix /var/lib/fixture-lojix/configured-lojix-store.db fixture-daemon NoTestDefaults {}}}",
        output.display()
    );

    let configuration = write_configuration(&request, &output);
    assert_eq!(configuration.daemon_host, "fixture-daemon");
    assert!(
        configuration.test_defaults.is_none(),
        "a production node bakes no test-op fixture"
    );
}

#[test]
fn write_configuration_requires_one_inline_object_and_never_reads_a_request_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("startup.rkyv");
    let request = format!(
        "(ConfigurationWriteRequest (/run/fixture-lojix/ordinary.sock 432 /run/fixture-lojix/owner.sock 384 /var/lib/fixture-lojix /var/lib/fixture-lojix/configured-lojix-store.db fixture-daemon NoTestDefaults {}))",
        output.display()
    );
    let request_file = directory.path().join("request.dotos");
    std::fs::write(&request_file, &request).expect("write request-file witness");

    for arguments in [
        Vec::new(),
        vec![request_file.as_os_str().to_os_string()],
        vec!["--help".into()],
        vec!["--pretty".into()],
        vec![request.clone().into(), "extra".into()],
    ] {
        let status = Command::new(env!("CARGO_BIN_EXE_lojix-write-configuration"))
            .args(arguments)
            .status()
            .expect("run writer");
        assert!(
            !status.success(),
            "writer must reject every non-single-inline invocation"
        );
    }
    assert!(
        !output.exists(),
        "the request file was not decoded or written as configuration"
    );
}
