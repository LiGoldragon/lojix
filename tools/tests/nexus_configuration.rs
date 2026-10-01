use lojix::DurableStore as _;
use std::fs;
use std::process::Command;

use lojix::{
    LegacyConfigurationArchivable as _, LegacyConfigurationMigratable as _,
    LegacyStartupConfiguration, NexusConfiguration, NexusConfigurationState, NexusPersistable as _,
    Store, TestDefaults, TestMode,
};
use signal_lojix::{LojixNexusConfiguration, TestDefaultsChoice};

fn configuration(directory: &std::path::Path, name: &str) -> NexusConfiguration {
    LojixNexusConfiguration {
        ordinary_socket_path: directory
            .join(format!("{name}-ordinary.sock"))
            .display()
            .to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory
            .join(format!("{name}-meta.sock"))
            .display()
            .to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory
            .join(format!("{name}-state"))
            .display()
            .to_string(),
        daemon_host: format!("{name}-host"),
        test_defaults_choice: TestDefaultsChoice::NoTestDefaults,
    }
}

#[test]
fn configuration_lifecycle_persists_the_meta_marker_and_desired_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.sema");
    let default = configuration(directory.path(), "default");
    let ordinary = configuration(directory.path(), "ordinary");
    let meta = configuration(directory.path(), "meta");

    let store =
        Store::open_with_default_configuration(&path, default.clone()).expect("fresh store");
    assert_eq!(
        store.nexus_configuration_state().expect("default state"),
        NexusConfigurationState {
            desired_configuration: default,
            meta_configure_occurred: false,
        }
    );

    let ordinary_state = store
        .ordinary_configure(ordinary.clone())
        .expect("ordinary configure");
    assert_eq!(ordinary_state.desired_configuration, ordinary);
    assert!(!ordinary_state.meta_configure_occurred);

    let meta_state = store.meta_configure(meta.clone()).expect("meta configure");
    assert_eq!(meta_state.desired_configuration, meta);
    assert!(meta_state.meta_configure_occurred);
    assert!(
        store
            .ordinary_configure(configuration(directory.path(), "refused"))
            .is_err()
    );

    let reversed = store.reverse_meta_configuration().expect("meta reversal");
    assert!(!reversed.meta_configure_occurred);
    assert_eq!(reversed.desired_configuration, meta);
    drop(store);

    let reopened = Store::open_with_default_configuration(
        &path,
        configuration(directory.path(), "ignored-default"),
    )
    .expect("reopen store");
    assert_eq!(
        reopened
            .nexus_configuration_state()
            .expect("reopened state"),
        reversed
    );
    reopened
        .ordinary_configure(configuration(directory.path(), "after-reversal"))
        .expect("ordinary Configure reopens only after meta reversal");
}

#[test]
fn configuration_only_migration_refuses_v5_without_modifying_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source = directory.path().join("legacy.sema");
    let target = directory.path().join("migrated.sema");
    fs::write(
        &source,
        include_bytes!("../../tests/fixtures/lojix-v5-pre-nexus-cf231859.sema"),
    )
    .expect("materialize exact historical fixture");
    let before = fs::read(&source).expect("read source before probe");

    assert!(
        Store::open_with_default_configuration(
            &source,
            configuration(directory.path(), "untrusted-default"),
        )
        .is_err(),
        "normal startup must require explicit migration"
    );
    assert_eq!(
        fs::read(&source).expect("read source after rejected probe"),
        before,
        "startup compatibility discovery must not mutate the legacy source"
    );

    let legacy = LegacyStartupConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("meta.sock").display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory.path().join("state").display().to_string(),
        store_path: source.display().to_string(),
        daemon_host: "legacy-host".to_string(),
        test_defaults: Some(TestDefaults {
            cluster: "legacy-cluster".to_string(),
            default_vm_host: "legacy-node".to_string(),
            default_mode: TestMode::Hermetic,
            test_flake: "github:example/test".to_string(),
            test_nix_system: "x86_64-linux".to_string(),
            test_output_selector: "checks.fixture".to_string(),
            horizon_definition: None,
        }),
    };
    assert!(
        Store::migrate_configuration_copy(&source, &target, &legacy).is_err(),
        "configuration-only migration must not reinterpret schema 5 as schema 6"
    );
    assert_eq!(fs::read(&source).expect("source after refusal"), before);
    assert!(!target.exists(), "no accepted partial destination");
}

#[test]
fn configuration_only_migration_cli_refuses_v5_without_a_partial_target() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source = directory.path().join("legacy.sema");
    let target = directory.path().join("migrated.sema");
    let archive = directory.path().join("legacy.rkyv");
    fs::write(
        &source,
        include_bytes!("../../tests/fixtures/lojix-v5-pre-nexus-cf231859.sema"),
    )
    .expect("materialize historical store");
    let legacy = LegacyStartupConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("meta.sock").display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory.path().join("state").display().to_string(),
        store_path: source.display().to_string(),
        daemon_host: "legacy-host".to_string(),
        test_defaults: None,
    };
    legacy
        .write_rkyv_file(&archive)
        .expect("write legacy archive");
    let output = Command::new(env!("CARGO_BIN_EXE_lojix-migrate-configuration"))
        .arg(&archive)
        .arg(&target)
        .output()
        .expect("run one-shot migration executable");
    assert!(
        !output.status.success(),
        "configuration-only migration refuses v5"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("MigrationRejected"));
    assert_eq!(
        fs::read(&source).expect("source after refusal"),
        include_bytes!("../../tests/fixtures/lojix-v5-pre-nexus-cf231859.sema")
    );
    assert!(!target.exists(), "no partial target is accepted");
}
