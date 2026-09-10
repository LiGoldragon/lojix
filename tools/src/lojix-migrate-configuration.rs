//! Offline one-shot migration from the exact legacy startup archive and Sema.

use std::path::PathBuf;

use lojix::{LegacyConfigurationMigratable as _, LegacyStartupConfiguration, Store};

fn main() {
    if let Err(error) = run() {
        eprintln!("(MigrationRejected [{error}])");
        std::process::exit(2);
    }
}

fn run() -> lojix::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let Some(archive) = arguments.next() else {
        return Err(lojix::Error::ExpectedMigrationArguments);
    };
    let Some(target) = arguments.next() else {
        return Err(lojix::Error::ExpectedMigrationArguments);
    };
    if arguments.next().is_some() {
        return Err(lojix::Error::ExpectedMigrationArguments);
    }
    let configuration = LegacyStartupConfiguration::from_rkyv_file(&PathBuf::from(archive))?;
    let source = PathBuf::from(&configuration.store_path);
    drop(Store::migrate_configuration_copy(
        &source,
        &PathBuf::from(target),
        &configuration,
    )?);
    Ok(())
}
