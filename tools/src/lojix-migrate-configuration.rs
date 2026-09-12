//! Offline one-shot migration from the exact legacy startup archive and Sema.

use std::ffi::OsString;
use std::path::PathBuf;

use lojix::{
    LegacyConfigurationArchivable as _, LegacyConfigurationMigratable as _,
    LegacyStartupConfiguration, Store,
};

fn main() {
    if let Err(error) = ConfigurationMigration::from_arguments(std::env::args_os().skip(1))
        .and_then(Migrating::apply)
    {
        eprintln!("(MigrationRejected [{error}])");
        std::process::exit(2);
    }
}

/// The one migration this tool performs: the legacy startup archive to read,
/// and the store path the migrated copy is written to.
struct ConfigurationMigration {
    archive: PathBuf,
    target: PathBuf,
}

/// Reading a migration off the command line, and performing it.
trait Migrating {
    /// Exactly two positional arguments: the legacy archive, then the target
    /// store. Anything else is rejected.
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self>
    where
        Self: Sized;

    fn apply(self) -> lojix::Result<()>;
}

impl Migrating for ConfigurationMigration {
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self> {
        let mut arguments = arguments.into_iter();
        let (Some(archive), Some(target), None) =
            (arguments.next(), arguments.next(), arguments.next())
        else {
            return Err(lojix::Error::ExpectedMigrationArguments);
        };
        Ok(Self {
            archive: PathBuf::from(archive),
            target: PathBuf::from(target),
        })
    }

    fn apply(self) -> lojix::Result<()> {
        let configuration = LegacyStartupConfiguration::from_rkyv_file(&self.archive)?;
        let source = PathBuf::from(&configuration.store_path);
        drop(Store::migrate_configuration_copy(
            &source,
            &self.target,
            &configuration,
        )?);
        Ok(())
    }
}
