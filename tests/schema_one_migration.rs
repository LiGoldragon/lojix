//! Public boundary checks for the v5 reset contract.

use std::ffi::OsString;

use lojix::reconstruction::StoreResetCommand;
use lojix::{DaemonConfiguration, Store};
use redb::{Database, TableDefinition};

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");

fn startup_archive(
    directory: &std::path::Path,
    store_path: &std::path::Path,
) -> std::path::PathBuf {
    let archive = directory.join("startup.rkyv");
    DaemonConfiguration {
        ordinary_socket_path: directory.join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.join("owner.sock").display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory.display().to_string(),
        store_path: store_path.display().to_string(),
        daemon_host: "fixture-daemon".to_string(),
        test_defaults: None,
    }
    .write_rkyv_file(&archive)
    .expect("write startup archive");
    archive
}

fn reset_command(archive: &std::path::Path) -> StoreResetCommand {
    StoreResetCommand::from_arguments_with_configuration(
        [OsString::from("ResetStore")],
        archive.to_path_buf(),
    )
    .expect("exact reset command")
}

#[test]
fn reset_requires_one_inline_pathless_request() {
    let archive = std::path::PathBuf::from("/missing/generated-startup.rkyv");
    assert!(
        StoreResetCommand::from_arguments_with_configuration(Vec::new(), archive.clone()).is_err()
    );
    assert!(
        StoreResetCommand::from_arguments_with_configuration(
            [OsString::from("lojix.sema")],
            archive.clone()
        )
        .is_err()
    );
    assert!(
        StoreResetCommand::from_arguments_with_configuration(
            [OsString::from("/tmp/spirit.sema")],
            archive.clone()
        )
        .is_err()
    );
    assert!(
        StoreResetCommand::from_arguments_with_configuration(
            [OsString::from("-bad")],
            archive.clone()
        )
        .is_err()
    );
    assert!(
        StoreResetCommand::from_arguments_with_configuration(
            [OsString::from("ResetStore"), OsString::from("unexpected"),],
            archive
        )
        .is_err()
    );
}

#[test]
fn v5_refuses_the_v4_deploy_submission_layout_until_explicit_reset() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("configured-lojix-store.db");
    drop(Store::open(&path).expect("create a recognised Lojix store"));
    let database = Database::open(&path).expect("open disposable legacy-shaped store");
    let write = database.begin_write().expect("begin write");
    {
        let mut metadata = write.open_table(META_TABLE).expect("metadata table");
        metadata
            .insert("schema_version", 4)
            .expect("old schema marker");
    }
    write.commit().expect("commit old schema marker");
    drop(database);

    assert!(Store::open(&path).is_err(), "v5 must not decode schema 4");
    let archive = startup_archive(directory.path(), &path);
    reset_command(&archive)
        .run()
        .expect("replace known Lojix schema");
    Store::open(&path).expect("fresh v5 store opens");
}
