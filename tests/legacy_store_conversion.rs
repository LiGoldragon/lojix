//! A store written by the deployed Lojix 8.1.0 (`3fc95f0c`), opened through
//! the Nexus's own startup path.
//!
//! `fixtures/lojix-8.1.0-store.sema` is the byte-for-byte blob of
//! `tests/fixtures/lojix-v5-horizon-0.12-deploy-job.sema` at `3fc95f0c`: a v5
//! store written by sema-engine `27e814a` (the engine 8.1.0 pins), with the
//! `__sema_meta`, `__sema_headers` and `__sema_engine_*` tables and sema-domain
//! schema hashes and commit-log digests. It is never edited.
//!
//! The Nexus discovers its store the way `Daemon::from_environment` does: the
//! state directory goes through `discover_default_store_path`, and the path it
//! answers is opened by `open_with_default_configuration` with the built-in
//! configuration. A legacy `lojix.sema` must come out of that as a
//! `lojix.memory` that opens and serves the records the 8.1.0 store held.

use lojix::quarantine::RowQuarantine as _;
use lojix::{DurableStore as _, LojixNexusConfigurable as _, NexusConfiguration};
use lojix::{NexusPersistable as _, Store};
use tempfile::TempDir;

const LOJIX_8_1_0_STORE: &[u8] = include_bytes!("fixtures/lojix-8.1.0-store.sema");

#[test]
fn a_lojix_8_1_0_store_opens_through_the_nexus_startup_path() {
    let directory = TempDir::new().expect("tempdir");
    let state = directory.path().join("lojix");
    std::fs::create_dir_all(&state).expect("state directory");
    let legacy = state.join("lojix.sema");
    std::fs::write(&legacy, LOJIX_8_1_0_STORE).expect("copy the 8.1.0 store");

    let path = NexusConfiguration::discover_default_store_path(state.clone())
        .expect("the Nexus discovers the legacy store");
    assert_eq!(path, state.join("lojix.memory"));

    let store = Store::open_with_default_configuration(&path, NexusConfiguration::built_in())
        .expect("the 8.1.0 store opens as Memory");
    assert!(!legacy.exists(), "the legacy name is gone once converted");
    assert!(path.exists(), "the converted store lives at lojix.memory");

    let desired = store
        .nexus_configuration_state()
        .expect("configuration row reads back")
        .desired_configuration;
    assert_eq!(desired.daemon_host, "localhost");
    assert_eq!(desired.state_directory_path, "/var/lib/lojix");
    assert_eq!(
        desired.ordinary_socket_path,
        "/run/user/1001/lojix/ordinary.sock"
    );
    assert_eq!(desired.owner_socket_path, "/run/user/1001/lojix/meta.sock");

    // The in-flight deploy job was archived in the Horizon 0.12 layout; it is
    // read back as the quarantined row holding its original bytes.
    let quarantined = store.quarantined_rows().expect("quarantine reads back");
    let deploy_job = quarantined
        .iter()
        .find(|row| row.table == "deploy-job" && row.key == "1")
        .expect("the 8.1.0 deploy-job row is carried over");
    assert!(!deploy_job.archive.is_empty());
    assert!(
        store.commit_sequence().expect("commit sequence") > 0,
        "the converted store resumes its commit history"
    );
}
