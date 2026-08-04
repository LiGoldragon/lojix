//! Daemon startup configuration archive tests.
//!
//! Launch tooling writes a binary rkyv startup file before exec; the daemon only
//! reads that binary file.

use lojix::DaemonConfiguration;

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
