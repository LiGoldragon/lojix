//! Bootstrap config-writer round trip: the NOTA-to-rkyv `lojix-write-configuration`
//! tool produces a startup file that the daemon's
//! `DaemonConfiguration::from_rkyv_file` reads back unchanged. This is the
//! NOTA-to-binary boundary at deploy time — the daemon never parses NOTA.

use std::process::Command;

use lojix::DaemonConfiguration;

#[test]
fn write_configuration_round_trips_through_rkyv() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("startup.rkyv");
    let request = format!(
        "(ConfigurationWriteRequest (/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix (goldragon prometheus Hermetic github:LiGoldragon/CriomOS-test-cluster /var/lib/lojix/cluster.nota False 10.77.0.7 /nix/store/zzz-live-closure) {}))",
        output.display()
    );

    let status = Command::new(env!("CARGO_BIN_EXE_lojix-write-configuration"))
        .arg(&request)
        .status()
        .expect("run lojix-write-configuration");
    assert!(status.success(), "writer exited with failure");

    let configuration =
        DaemonConfiguration::from_rkyv_file(&output).expect("daemon reads back the startup file");
    assert_eq!(
        configuration.ordinary_socket_path,
        "/run/lojix/ordinary.sock"
    );
    assert_eq!(configuration.ordinary_socket_mode, 0o660);
    assert_eq!(configuration.owner_socket_path, "/run/lojix/owner.sock");
    assert_eq!(configuration.owner_socket_mode, 0o600);
    assert_eq!(configuration.state_directory_path, "/var/lib/lojix");
    assert_eq!(configuration.test_defaults.cluster, "goldragon");
    assert_eq!(configuration.test_defaults.default_vm_host, "prometheus");
    assert_eq!(
        configuration.test_defaults.test_flake,
        "github:LiGoldragon/CriomOS-test-cluster"
    );
    assert_eq!(
        configuration.test_defaults.proposal_source,
        "/var/lib/lojix/cluster.nota"
    );
    assert!(
        !configuration.test_defaults.live_enabled,
        "live stays disabled unless explicitly enabled in config"
    );
    assert_eq!(configuration.test_defaults.live_guest_ip, "10.77.0.7");
    assert_eq!(
        configuration.test_defaults.live_closure,
        "/nix/store/zzz-live-closure"
    );
}
