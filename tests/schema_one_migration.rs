//! Public boundary checks for the v5 reset contract.

use lojix::DurableStore as _;
use lojix::{OfflineCommand as _, reconstruction::StoreResetting as _};
use std::ffi::OsString;

use lojix::Store;
use lojix::reconstruction::StoreResetCommand;
use redb::{Database, TableDefinition};

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__memory_meta");

fn reset_command(store: &std::path::Path) -> StoreResetCommand {
    StoreResetCommand::for_store(Vec::<OsString>::new(), store.to_path_buf())
        .expect("exact reset command")
}

#[test]
fn reset_takes_no_arguments() {
    let store = std::path::PathBuf::from("/missing/lojix.memory");
    assert!(StoreResetCommand::for_store(Vec::<OsString>::new(), store.clone()).is_ok());
    assert!(StoreResetCommand::for_store([OsString::from("{}")], store.clone()).is_err());
    assert!(StoreResetCommand::for_store([OsString::from("lojix.memory")], store.clone()).is_err());
    assert!(
        StoreResetCommand::for_store([OsString::from("/tmp/spirit.memory")], store.clone())
            .is_err()
    );
    assert!(StoreResetCommand::for_store([OsString::from("-bad")], store.clone()).is_err());
    assert!(
        StoreResetCommand::for_store([OsString::from("{}"), OsString::from("unexpected"),], store)
            .is_err()
    );
}

#[test]
fn v5_refuses_the_v4_deploy_submission_layout_until_explicit_reset() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("configured-lojix-store.db");
    std::fs::write(
        &path,
        include_bytes!("fixtures/lojix-v5-pre-nexus-cf231859.memory"),
    )
    .expect("materialize the exact pre-Nexus v5 layout");
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

    let before = std::fs::read(&path).expect("read schema 4 source");
    assert!(Store::open(&path).is_err(), "v5 must not decode schema 4");
    assert_eq!(
        std::fs::read(&path).expect("read source after rejected open"),
        before,
        "startup probing must not mutate an incompatible store"
    );
    reset_command(&path)
        .run()
        .expect("replace known Lojix schema");
    Store::open(&path).expect("fresh v5 store opens");
}
