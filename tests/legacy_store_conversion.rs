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
//! configuration. A legacy `lojix.sema` comes out of that as a `lojix.memory`
//! that opens and serves the records the 8.1.0 store held, and `lojix.sema`
//! stays byte for byte as it was, the store 8.1.0 reads on a rollback.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use lojix::legacy_store::{
    LegacyConversionStage, LegacyStoreConversion, LegacyStoreConverting as _,
};
use lojix::quarantine::RowQuarantine as _;
use lojix::{DurableStore as _, Error, LojixNexusConfigurable as _, NexusConfiguration};
use lojix::{EventHistory as _, EventLogRetention, NexusPersistable as _, Store};
use memory_engine::{
    Engine, EngineOpen, SchemaVersion, VersionedHistoryAcknowledgement, VersionedHistoryRetention,
    VersionedStoreName, VersioningPolicy,
};
use tempfile::TempDir;

const LOJIX_8_1_0_STORE: &[u8] = include_bytes!("fixtures/lojix-8.1.0-store.sema");

/// The file names in a state directory, sorted.
fn directory_names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("read state directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

#[test]
fn a_lojix_8_1_0_store_opens_through_the_nexus_startup_path() {
    let directory = TempDir::new().expect("tempdir");
    let state = directory.path().join("lojix");
    std::fs::create_dir_all(&state).expect("state directory");
    let legacy = state.join("lojix.sema");
    std::fs::write(&legacy, LOJIX_8_1_0_STORE).expect("copy the 8.1.0 store");

    let path = NexusConfiguration::discover_default_store_path(state.clone())
        .expect("the Nexus converts the legacy store");
    assert_eq!(path, state.join("lojix.memory"));
    assert_eq!(
        std::fs::read(&legacy).expect("read lojix.sema"),
        LOJIX_8_1_0_STORE,
        "lojix.sema stays byte for byte as the rollback store"
    );
    assert_eq!(
        std::fs::metadata(&path)
            .expect("converted store metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        directory_names(&state),
        ["lojix.memory", "lojix.sema"],
        "no conversion copy is left behind"
    );

    let store = Store::open_with_default_configuration(&path, NexusConfiguration::built_in())
        .expect("the converted store opens as Memory");

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
    drop(store);

    let again = NexusConfiguration::discover_default_store_path(state.clone())
        .expect("the next start opens lojix.memory");
    assert_eq!(again, path);
    assert_eq!(
        std::fs::read(&legacy).expect("read lojix.sema"),
        LOJIX_8_1_0_STORE
    );
}

/// Startup over a `lojix.sema` holding `bytes` fails at `expected` and leaves
/// the directory holding only that `lojix.sema`, byte for byte.
fn assert_conversion_fails_untouched(bytes: &[u8], expected: LegacyConversionStage) {
    let directory = TempDir::new().expect("tempdir");
    let state = directory.path().join("lojix");
    std::fs::create_dir_all(&state).expect("state directory");
    let legacy = state.join("lojix.sema");
    std::fs::write(&legacy, bytes).expect("write a store that does not convert");

    let error = NexusConfiguration::discover_default_store_path(state.clone())
        .expect_err("a store that does not convert stops startup");
    match &error {
        Error::LegacyStoreConversion {
            legacy: named,
            current,
            stage,
            ..
        } => {
            assert_eq!(named, &legacy);
            assert_eq!(current, &state.join("lojix.memory"));
            assert_eq!(*stage, expected, "{error}");
        }
        other => panic!("expected LegacyStoreConversion, got {other}"),
    }
    assert_eq!(
        std::fs::read(&legacy).expect("read lojix.sema"),
        bytes,
        "lojix.sema is byte for byte as it was"
    );
    assert_eq!(
        directory_names(&state),
        ["lojix.sema"],
        "no lojix.memory and no conversion copy"
    );
}

#[test]
fn a_failed_conversion_leaves_lojix_sema_untouched_and_no_lojix_memory() {
    // A store already in the memory format under the legacy name: the
    // conversion refuses it before writing.
    let directory = TempDir::new().expect("tempdir");
    let memory = directory.path().join("written.memory");
    drop(Store::open(&memory).expect("a fresh memory store"));
    let memory_bytes = std::fs::read(&memory).expect("read the memory store");
    assert_conversion_fails_untouched(&memory_bytes, LegacyConversionStage::ConvertingCopy);
}

#[test]
fn a_truncated_legacy_store_fails_conversion_and_is_left_untouched() {
    // The 8.1.0 store cut short. The storage library panics on it rather than
    // returning an error; the panic is the conversion stage's typed failure.
    let truncated = &LOJIX_8_1_0_STORE[..LOJIX_8_1_0_STORE.len() / 3];
    assert_conversion_fails_untouched(truncated, LegacyConversionStage::ConvertingCopy);
}

/// Acceptance on a copy of a deployed store: `LOJIX_LEGACY_STORE_ACCEPTANCE`
/// names a scratch directory holding a copied `lojix.sema`. The copy is
/// converted, opened the normal way, folded into a checkpoint, compacted, and
/// opened again; the conversion receipt is printed.
#[test]
#[ignore = "needs LOJIX_LEGACY_STORE_ACCEPTANCE naming a directory with a copied lojix.sema"]
fn a_copied_deployed_store_converts_opens_folds_and_compacts() {
    let state = std::path::PathBuf::from(
        std::env::var_os("LOJIX_LEGACY_STORE_ACCEPTANCE")
            .expect("LOJIX_LEGACY_STORE_ACCEPTANCE names the scratch state directory"),
    );
    let legacy = state.join("lojix.sema");
    let current = state.join("lojix.memory");
    // A copy taken while the deployed Nexus holds the store open is marked
    // as needing repair, which a read-only open refuses. Setting
    // `LOJIX_LEGACY_STORE_ACCEPTANCE_REPAIR` repairs the scratch copy first,
    // as a clean stop of that Nexus would have left it.
    match redb::ReadOnlyDatabase::open(&legacy) {
        Ok(_) => println!("copy: closed cleanly"),
        Err(error) => {
            println!("copy: refused read-only: {error}");
            if std::env::var_os("LOJIX_LEGACY_STORE_ACCEPTANCE_REPAIR").is_some() {
                drop(redb::Database::open(&legacy).expect("the scratch copy repairs"));
                println!("copy: repaired");
            }
        }
    }
    let legacy_bytes = std::fs::read(&legacy).expect("read the copied lojix.sema");

    let receipt = LegacyStoreConversion {
        legacy: legacy.clone(),
        current: current.clone(),
    }
    .convert()
    .expect("the copied store converts");
    println!(
        "receipt: entries {} checkpoints {} segments {} kept_digests {}",
        receipt.entries(),
        receipt.checkpoints(),
        receipt.segments(),
        receipt.kept_digests().len()
    );
    assert_eq!(
        std::fs::read(&legacy).expect("reread lojix.sema"),
        legacy_bytes
    );

    let store = Store::open_with_default_configuration(&current, NexusConfiguration::built_in())
        .expect("the converted store opens the normal way");
    println!(
        "opened: commit sequence {} quarantined rows {}",
        store.commit_sequence().expect("commit sequence"),
        store.quarantined_rows().expect("quarantine reads").len()
    );
    let retired = store
        .compact_event_history(EventLogRetention::default())
        .expect("the Nexus's own compaction pass runs");
    println!("event-history compaction retired {retired} entries");
    drop(store);

    let open = || {
        Engine::open(
            EngineOpen::new(current.clone(), SchemaVersion::new(5)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("lojix"))
                    .with_retention(VersionedHistoryRetention::new(4_096)),
            ),
        )
        .expect("the converted store opens in memory-engine")
    };
    let engine = open();
    let log_before = engine.versioned_commit_log().expect("log reads").len();
    engine
        .checkpoint()
        .expect("the converted log folds into a checkpoint");
    engine
        .latest_checkpoint()
        .expect("the checkpoint verifies")
        .expect("a checkpoint exists");
    engine
        .compact_versioned_history(
            VersionedHistoryRetention::new(0),
            VersionedHistoryAcknowledgement::LocalCheckpoint,
        )
        .expect("the converted history compacts");
    let log_after = engine.versioned_commit_log().expect("log reads").len();
    println!("versioned log: {log_before} entries before compaction, {log_after} after");
    drop(engine);

    let reopened = Store::open_with_default_configuration(&current, NexusConfiguration::built_in())
        .expect("the compacted store reopens");
    let desired = reopened
        .nexus_configuration_state()
        .expect("configuration reads back")
        .desired_configuration;
    println!(
        "reopened: daemon host {} state directory {} commit sequence {}",
        desired.daemon_host,
        desired.state_directory_path,
        reopened.commit_sequence().expect("commit sequence")
    );
    assert_eq!(
        std::fs::read(&legacy).expect("reread lojix.sema"),
        legacy_bytes
    );
}
