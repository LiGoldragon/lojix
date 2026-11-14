use lojix::{NexusConfiguration, NexusConfigurationState, NexusPersistable as _, Store};
use signal_lojix::{LojixNexusConfiguration, TestDefaultsChoice};

fn configuration(directory: &std::path::Path, name: &str) -> NexusConfiguration {
    LojixNexusConfiguration {
        ordinary_socket_path: signal_lojix::OrdinarySocketPath(
            directory
                .join(format!("{name}-ordinary.sock"))
                .display()
                .to_string(),
        ),
        ordinary_socket_mode: signal_lojix::OrdinarySocketMode(0o660),
        owner_socket_path: signal_lojix::OwnerSocketPath(
            directory
                .join(format!("{name}-meta.sock"))
                .display()
                .to_string(),
        ),
        owner_socket_mode: signal_lojix::OwnerSocketMode(0o600),
        state_directory_path: signal_lojix::StateDirectoryPath(
            directory
                .join(format!("{name}-state"))
                .display()
                .to_string(),
        ),
        daemon_host: signal_lojix::DaemonHost(format!("{name}-host")),
        test_defaults_choice: TestDefaultsChoice::NoTestDefaults,
    }
}

#[test]
fn configuration_lifecycle_persists_the_meta_marker_and_desired_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.memory");
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
