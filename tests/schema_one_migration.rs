//! Public boundary checks for the v4 reset contract.

use std::ffi::OsString;

use lojix::Store;
use lojix::reconstruction::StoreResetCommand;
use redb::{Database, TableDefinition};

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");

#[test]
fn reset_requires_one_absolute_owned_store_path() {
    assert!(
        StoreResetCommand::from_arguments([OsString::from("lojix.sema")])
            .expect("argument parser only")
            .run()
            .is_err()
    );
    assert!(
        StoreResetCommand::from_arguments([OsString::from("/tmp/spirit.sema")])
            .expect("argument parser only")
            .run()
            .is_err()
    );
    assert!(StoreResetCommand::from_arguments([OsString::from("-bad")]).is_err());
    assert!(
        StoreResetCommand::from_arguments([
            OsString::from("/tmp/lojix.sema"),
            OsString::from("unexpected"),
        ])
        .is_err()
    );
}

#[test]
fn v4_refuses_an_old_schema_until_the_explicit_reset_replaces_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.sema");
    let database = Database::create(&path).expect("create disposable legacy-shaped store");
    let write = database.begin_write().expect("begin write");
    {
        let mut metadata = write.open_table(META_TABLE).expect("metadata table");
        metadata
            .insert("schema_version", 3)
            .expect("old schema marker");
    }
    write.commit().expect("commit old schema marker");
    drop(database);

    assert!(Store::open(&path).is_err(), "v4 must not decode schema 3");
    StoreResetCommand::from_arguments([path.clone().into_os_string()])
        .expect("exact reset command")
        .run()
        .expect("replace known Lojix schema");
    Store::open(&path).expect("fresh v4 store opens");
}
