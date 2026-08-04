//! Public migration command boundary checks. The byte-layout witness lives next
//! to the private v2 decoder in `reconstruction.rs`, so the test can build an
//! actual historic row without exposing that compatibility vocabulary.

use std::ffi::OsString;

use lojix::reconstruction::{MigrationOutcome, StoreMigrationCommand, StoreMigrator};

#[test]
fn missing_store_is_an_idempotent_no_op() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("missing.sema");
    assert_eq!(
        StoreMigrator::new(&path)
            .migrate()
            .expect("missing store result"),
        MigrationOutcome::NoStore { path },
    );
}

#[test]
fn command_accepts_one_positional_path_only() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("store.sema");
    assert!(StoreMigrationCommand::from_arguments([path.into_os_string()]).is_ok());
    assert!(StoreMigrationCommand::from_arguments([OsString::from("-bad")]).is_err());
    assert!(
        StoreMigrationCommand::from_arguments([
            OsString::from("one.sema"),
            OsString::from("two.sema"),
        ])
        .is_err()
    );
}
