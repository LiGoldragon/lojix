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

use std::path::Path;

use lojix::legacy_store::{
    LegacyConversionStage, LegacyStoreConversion, LegacyStoreConverting as _,
};
use lojix::quarantine::OpenedStore as _;
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
fn a_lojix_8_1_0_store_holding_a_horizon_0_12_deploy_job_is_refused_and_left_untouched() {
    // The 8.1.0 store's one in-flight deploy job was archived in the Horizon
    // 0.12 layout. A store is upgraded, never accommodated, and no upgrade
    // reads that layout, so the converted copy does not open and nothing is
    // installed; lojix.sema stays the store 8.1.0 reads.
    assert_conversion_fails_untouched(
        LOJIX_8_1_0_STORE,
        LegacyConversionStage::VerifyingConvertedCopy,
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

#[test]
fn a_store_copied_while_its_writer_holds_it_open_is_repaired_before_it_is_verified() {
    let directory = TempDir::new().expect("tempdir");
    let writer_path = directory.path().join("writer.sema");
    std::fs::write(&writer_path, LOJIX_8_1_0_STORE).expect("write the 8.1.0 store");

    // The writer holds the store open read-write, as the deployed Nexus does,
    // while the copy is taken: the copy carries redb's needs-repair flag.
    let writer = redb::Database::open(&writer_path).expect("the writer opens the store");
    let copied = directory.path().join("copied.sema");
    std::fs::copy(&writer_path, &copied).expect("copy the store under its writer");
    assert!(
        redb::ReadOnlyDatabase::open(&copied).is_err(),
        "the copy needs repair, so a read-only open refuses it"
    );
    drop(writer);
    let copied_bytes = std::fs::read(&copied).expect("read the copied store");

    // The conversion repairs its own copy and converts it; verification then
    // meets the Horizon 0.12 deploy job, as it does for a closed store.
    assert_conversion_fails_untouched(&copied_bytes, LegacyConversionStage::VerifyingConvertedCopy);
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
    // as needing repair, which a read-only open refuses; the conversion
    // repairs its own copy of it.
    match redb::ReadOnlyDatabase::open(&legacy) {
        Ok(_) => println!("copy: closed cleanly"),
        Err(error) => println!("copy: refused read-only, left to the conversion: {error}"),
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
        "opened: commit sequence {} dropped set-aside rows {}",
        store.commit_sequence().expect("commit sequence"),
        store.opening().dropped_quarantined_rows.len()
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
        desired.daemon_host.0,
        desired.state_directory_path.0,
        reopened.commit_sequence().expect("commit sequence")
    );
    assert_eq!(
        std::fs::read(&legacy).expect("reread lojix.sema"),
        legacy_bytes
    );
}
