//! Explicit reset support for the v5 Lojix store.
//!
//! v5 intentionally has no decoder or migration path for older layouts. A
//! caller that has stopped the daemon may reconstruct a recognised pre-v5
//! Lojix store with [`StoreResetCommand`]. The reset takes no input: its store
//! is the Nexus's own, at the place the Nexus itself opens it
//! (`lojix.memory` in its state directory). It validates the durable Lojix
//! family/schema identity before unlinking it and derives only its protocol
//! sidecars from that validated primary.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};

use memory_engine::TableRegistration;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use rkyv::rancor;

use crate::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentOutboxRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, PendingTransitionIntent, StoredDeploymentRecord,
    StoredTestRun,
};
use crate::{
    DurableStore as _, Error, LojixNexusConfigurable as _, LojixRecord as _, NexusConfiguration,
    OfflineCommand, Result, Store,
};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__memory_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__memory_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const CURRENT_SCHEMA: u64 = 5;
const RECOGNISED_SCHEMAS: &[u64] = &[2, 3, 4, CURRENT_SCHEMA];
const RESETTABLE_SCHEMAS: &[u64] = &[2, 3, 4];
/// These are protocol-owned suffixes from the retired v2/v3 migration path.
/// They are only derived after the primary store has proved itself to be a
/// recognised Lojix database.
const SIDECAR_SUFFIXES: &[&str] = &[
    ".schema-pre-v3.backup",
    ".schema-v3.pending",
    ".schema-v3.pending.owner",
];

/// Result of a guarded reset. A current v5 store is observed but never
/// rewritten: callers receive [`Self::AlreadyCurrent`] without deleting any
/// data. A recognised v2/v3/v4 store is removed and recreated as v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreResetOutcome {
    Recreated {
        path: PathBuf,
        removed_sidecars: Vec<PathBuf>,
    },
    AlreadyCurrent {
        path: PathBuf,
    },
}

impl std::fmt::Display for StoreResetOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recreated {
                path,
                removed_sidecars,
            } => write!(
                formatter,
                "(LojixStoreReset path={} schema=5 removed_sidecars={})",
                path.display(),
                removed_sidecars.len(),
            ),
            Self::AlreadyCurrent { path } => {
                write!(
                    formatter,
                    "(LojixStoreAlreadyCurrent path={} schema=5)",
                    path.display()
                )
            }
        }
    }
}

/// One exact, version-aware Lojix store reset. It takes no input: no argument,
/// raw path, directory, glob, or request file. The store is the one the Nexus
/// opens, at its built-in place. That primary must be an existing regular,
/// non-symlink file with a recognised Lojix catalog; a sibling Spirit database
/// is never selected by name.
pub struct StoreResetCommand {
    store_path: PathBuf,
}

impl OfflineCommand for StoreResetCommand {
    type Outcome = Result<StoreResetOutcome>;

    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        Self::for_store(arguments, NexusConfiguration::store_path())
    }

    fn run(&self) -> Result<StoreResetOutcome> {
        self.reset()
    }
}

/// Bringing a recognised older store forward to the current schema by removing
/// and recreating it.
pub trait StoreResetting: Sized {
    /// Construct the command for an explicit store. This exists for
    /// in-process tests; the executable resets the Nexus's own store.
    fn for_store(
        arguments: impl IntoIterator<Item = OsString>,
        store_path: impl Into<PathBuf>,
    ) -> Result<Self>;

    fn reset(&self) -> Result<StoreResetOutcome>;
}

impl StoreResetting for StoreResetCommand {
    fn for_store(
        arguments: impl IntoIterator<Item = OsString>,
        store_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        if arguments.into_iter().next().is_some() {
            return Err(Error::UnexpectedResetArguments);
        }
        Ok(Self {
            store_path: store_path.into(),
        })
    }

    fn reset(&self) -> Result<StoreResetOutcome> {
        let path = self.store_path.canonical_regular_file("store")?;
        let schema = path.recognised_lojix_schema()?;
        if schema == CURRENT_SCHEMA {
            return Ok(StoreResetOutcome::AlreadyCurrent { path });
        }
        debug_assert!(RESETTABLE_SCHEMAS.contains(&schema));

        let sidecars = path.validated_sidecars()?;
        fs::remove_file(&path).map_err(|error| {
            Error::StoreMaintenance(format!("remove reset store {}: {error}", path.display()))
        })?;
        for sidecar in &sidecars {
            fs::remove_file(sidecar).map_err(|error| {
                Error::StoreMaintenance(format!(
                    "remove Lojix reset sidecar {}: {error}",
                    sidecar.display()
                ))
            })?;
        }
        // Store::open is the sole initializer. It stamps a new v5 schema and
        // proves that the replacement is usable before this command succeeds.
        drop(Store::open(&path)?);
        Ok(StoreResetOutcome::Recreated {
            path,
            removed_sidecars: sidecars,
        })
    }
}

/// What the reset command asks of the store path it resets.
trait ResetSubject {
    /// The path as an exact absolute, traversal-free, existing regular
    /// non-symlink file, with its parent directory canonicalized. `subject`
    /// names the path in the rejection.
    fn canonical_regular_file(&self, subject: &str) -> Result<PathBuf>;

    /// Every protocol-owned sidecar this store could have, existing or not.
    fn sidecars(&self) -> Vec<PathBuf>;

    /// The sidecars that do exist, each proved to be a regular non-symlink
    /// file. A sidecar of any other kind aborts the reset.
    fn validated_sidecars(&self) -> Result<Vec<PathBuf>>;

    /// The store's schema version, admitted only when the version is one lojix
    /// recognises and the catalog is wholly a Lojix layout.
    fn recognised_lojix_schema(&self) -> Result<u64>;
}

impl ResetSubject for Path {
    fn canonical_regular_file(&self, subject: &str) -> Result<PathBuf> {
        if !self.is_absolute()
            || self.file_name().is_none()
            || self
                .components()
                .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
        {
            return Err(Error::StoreMaintenance(format!(
                "reset {subject} must be an exact absolute, traversal-free file path"
            )));
        }
        let file_name = self.file_name().expect("checked above");
        let parent = self.parent().ok_or_else(|| {
            Error::StoreMaintenance(format!(
                "reset {subject} path must have an existing canonical parent directory"
            ))
        })?;
        let parent = fs::canonicalize(parent).map_err(|error| {
            Error::StoreMaintenance(format!(
                "reset {subject} parent {} must exist and be canonicalizable: {error}",
                parent.display(),
            ))
        })?;
        let metadata = fs::metadata(&parent).map_err(|error| {
            Error::StoreMaintenance(format!(
                "reset {subject} parent {} is unreadable: {error}",
                parent.display()
            ))
        })?;
        if !metadata.is_dir() {
            return Err(Error::StoreMaintenance(format!(
                "reset {subject} parent is not a directory"
            )));
        }
        let path = parent.join(file_name);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            Error::StoreMaintenance(format!(
                "reset {subject} {} must exist and be readable: {error}",
                path.display(),
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::StoreMaintenance(format!(
                "reset {subject} {} must be a regular non-symlink file",
                path.display(),
            )));
        }
        Ok(path)
    }

    fn sidecars(&self) -> Vec<PathBuf> {
        SIDECAR_SUFFIXES
            .iter()
            .map(|suffix| PathBuf::from(format!("{}{}", self.display(), suffix)))
            .collect()
    }

    fn validated_sidecars(&self) -> Result<Vec<PathBuf>> {
        let mut sidecars = Vec::new();
        for sidecar in self.sidecars() {
            let metadata = match fs::symlink_metadata(&sidecar) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(Error::StoreMaintenance(format!(
                        "inspect Lojix reset sidecar {}: {error}",
                        sidecar.display(),
                    )));
                }
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(Error::StoreMaintenance(format!(
                    "Lojix reset sidecar {} must be a regular non-symlink file",
                    sidecar.display(),
                )));
            }
            sidecars.push(sidecar);
        }
        Ok(sidecars)
    }

    fn recognised_lojix_schema(&self) -> Result<u64> {
        let database = redb::ReadOnlyDatabase::open(self).map_err(|error| {
            Error::StoreMaintenance(format!(
                "store {} did not open read-only: {error}",
                self.display(),
            ))
        })?;
        let version = database.schema_version()?;
        if !RECOGNISED_SCHEMAS.contains(&version) {
            return Err(Error::StoreMaintenance(format!(
                "reset store {} has unsupported schema {version}; refusing to remove an unrecognised file",
                self.display(),
            )));
        }
        database.validate_lojix_catalog(version)?;
        Ok(version)
    }
}

/// Reading the durable identity a reset source claims.
trait LojixStoreDatabase {
    fn schema_version(&self) -> Result<u64>;

    /// Verify the persisted memory-engine catalog belongs wholly to one of the
    /// Lojix store layouts. Schema v2 has the six core families; v3/v4/v5 add
    /// the deployment correlation families (and v3's retired quarantine
    /// family). A valid reset source must contain every core family and no
    /// foreign family.
    fn validate_lojix_catalog(&self, version: u64) -> Result<()>;
}

impl LojixStoreDatabase for redb::ReadOnlyDatabase {
    fn schema_version(&self) -> Result<u64> {
        let transaction = self.begin_read().map_err(|error| {
            Error::StoreMaintenance(format!("store metadata read failed: {error}"))
        })?;
        let table = transaction.open_table(META_TABLE).map_err(|error| {
            Error::StoreMaintenance(format!("store metadata table missing: {error}"))
        })?;
        let version = table
            .get(SCHEMA_VERSION_KEY)
            .map_err(|error| {
                Error::StoreMaintenance(format!("store schema version read failed: {error}"))
            })?
            .ok_or_else(|| {
                Error::StoreMaintenance("store schema version is missing".to_string())
            })?;
        Ok(version.value())
    }

    fn validate_lojix_catalog(&self, version: u64) -> Result<()> {
        let transaction = self.begin_read().map_err(|error| {
            Error::StoreMaintenance(format!("store catalog read failed: {error}"))
        })?;
        let table = transaction.open_table(CATALOG_TABLE).map_err(|error| {
            Error::StoreMaintenance(format!("store catalog table missing: {error}"))
        })?;
        let mut actual = BTreeSet::new();
        for row in table.iter().map_err(|error| {
            Error::StoreMaintenance(format!("store catalog iteration failed: {error}"))
        })? {
            let (_key, value) = row.map_err(|error| {
                Error::StoreMaintenance(format!("store catalog row read failed: {error}"))
            })?;
            let registration = rkyv::from_bytes::<TableRegistration, rancor::Error>(value.value())
                .map_err(|error| {
                    Error::StoreMaintenance(format!("store catalog decode failed: {error}"))
                })?;
            actual.insert(registration.lojix_identity());
        }
        let LojixCatalog(recognised) = LojixCatalog::recognised();
        let LojixCatalog(core) = LojixCatalog::core();
        let carries_deploy_jobs = actual.contains(&DeployJob::family_identity())
            || actual.contains(&LojixCatalog::earlier_deploy_job());
        if actual.is_empty()
            || !core.is_subset(&actual)
            || !carries_deploy_jobs
            || !actual.is_subset(&recognised)
        {
            return Err(Error::StoreMaintenance(format!(
                "reset store catalog is not a recognised Lojix family/schema layout for schema {version}; refusing removal"
            )));
        }
        Ok(())
    }
}

type StoreFamilyIdentity = (String, String, [u8; 32]);

/// How a memory-engine table registration names itself in a Lojix catalog.
trait CatalogRegistration {
    fn lojix_identity(&self) -> StoreFamilyIdentity;
}

impl CatalogRegistration for TableRegistration {
    fn lojix_identity(&self) -> StoreFamilyIdentity {
        (
            self.table_name().to_string(),
            self.identity().family().as_str().to_string(),
            *self.identity().schema_hash().bytes(),
        )
    }
}

/// A set of table family identities read as one store layout.
struct LojixCatalog(BTreeSet<StoreFamilyIdentity>);

impl<const COUNT: usize> From<[(&str, &str, [u8; 32]); COUNT]> for LojixCatalog {
    fn from(identities: [(&str, &str, [u8; 32]); COUNT]) -> Self {
        Self(
            identities
                .into_iter()
                .map(|(table, family, hash)| (table.to_string(), family.to_string(), hash))
                .collect(),
        )
    }
}

/// The two layouts a reset source is compared against.
trait StoreLayouts {
    /// The five families every recognised Lojix store must carry beside its
    /// deploy-job family, which it carries in one of its two layouts.
    fn core() -> Self;

    /// The deploy-job family as lojix 0.10.6 and earlier registered it.
    fn earlier_deploy_job() -> StoreFamilyIdentity;

    /// Every family a recognised Lojix store may carry — the core six plus the
    /// deployment correlation families and v3's retired quarantine family.
    fn recognised() -> Self;
}

impl StoreLayouts for LojixCatalog {
    fn earlier_deploy_job() -> StoreFamilyIdentity {
        (
            DeployJob::TABLE.to_string(),
            DeployJob::FAMILY.to_string(),
            [5; 32],
        )
    }

    fn core() -> Self {
        Self(BTreeSet::from([
            LiveGeneration::family_identity(),
            GcRoot::family_identity(),
            EventLogEntry::family_identity(),
            ContainerLifecycleRecord::family_identity(),
            StoredTestRun::family_identity(),
        ]))
    }

    fn recognised() -> Self {
        let Self(mut identities) = Self::core();
        identities.extend([
            DeployJob::family_identity(),
            LojixCatalog::earlier_deploy_job(),
            StoredDeploymentRecord::family_identity(),
            IdentifierAllocation::family_identity(),
            DeploymentOutboxRecord::family_identity(),
            PendingTransitionIntent::family_identity(),
            crate::NexusConfigurationRecord::family_identity(),
            crate::quarantine::QuarantinedRow::family_identity(),
            (
                "legacy-deployment-event-quarantine".to_string(),
                "LegacyDeploymentEventQuarantineFamily".to_string(),
                [10; 32],
            ),
        ]);
        Self(identities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset_command(store: &Path) -> StoreResetCommand {
        StoreResetCommand::for_store(Vec::<OsString>::new(), store.to_path_buf())
            .expect("exact reset command")
    }

    fn mark_pre_v5(path: &Path) {
        let database = redb::Database::open(path).expect("open recognised Lojix store");
        let write = database
            .begin_write()
            .expect("begin schema downgrade fixture");
        {
            let mut metadata = write.open_table(META_TABLE).expect("metadata table");
            metadata
                .insert(SCHEMA_VERSION_KEY, 3)
                .expect("pre-v5 schema marker");
        }
        write.commit().expect("commit schema downgrade fixture");
    }

    #[test]
    fn reset_cli_takes_no_arguments() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = directory.path().join("store.memory");
        for arguments in [
            vec![OsString::from("{}")],
            vec![OsString::from("--help")],
            vec![OsString::from("--pretty")],
            vec![OsString::from("/tmp/lojix-store.memory")],
            vec![OsString::from(
                "StoreResetRequest.{/tmp/lojix-store.memory}",
            )],
            vec![OsString::from("ResetStore")],
            vec![OsString::from("{}"), OsString::from("extra")],
        ] {
            assert!(
                StoreResetCommand::for_store(arguments, store.clone()).is_err(),
                "reset takes no arguments"
            );
        }
    }

    #[test]
    fn reset_requires_an_existing_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let missing_store = directory.path().join("configured-lojix-store.db");
        let error = reset_command(&missing_store)
            .run()
            .expect_err("an absent configured store must not be created by reset");
        assert!(error.to_string().contains("store"));
    }

    #[test]
    fn reset_returns_already_current_without_touching_v5_data_or_sidecars() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create v5 store"));
        let sidecar = path.sidecars().into_iter().next().expect("sidecar path");
        fs::write(&sidecar, "must survive with v5 primary").expect("sidecar witness");
        let before = fs::read(&path).expect("read v5 store before reset");

        assert_eq!(
            reset_command(&path).run().expect("current-store result"),
            StoreResetOutcome::AlreadyCurrent { path: path.clone() }
        );
        assert_eq!(fs::read(&path).expect("read v5 store after reset"), before);
        assert!(sidecar.exists(), "v5 sidecars are untouched too");
    }

    #[test]
    fn reset_recreates_only_a_recognised_pre_v5_lojix_store_and_its_sidecars() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create recognised Lojix source"));
        mark_pre_v5(&path);
        for sidecar in path.sidecars() {
            fs::write(sidecar, "stale Lojix sidecar").expect("write sidecar");
        }
        let spirit = directory.path().join("spirit.memory");
        fs::write(&spirit, "must survive").expect("write Spirit witness");
        let outcome = reset_command(&path).run().expect("reset pre-v5 source");
        assert!(matches!(outcome, StoreResetOutcome::Recreated { .. }));
        assert_eq!(
            redb::ReadOnlyDatabase::open(&path)
                .expect("open fresh v5 store")
                .schema_version()
                .expect("fresh v5 schema"),
            CURRENT_SCHEMA
        );
        assert_eq!(
            fs::read_to_string(spirit).expect("Spirit witness"),
            "must survive"
        );
        assert!(path.sidecars().iter().all(|sidecar| !sidecar.exists()));
    }

    #[test]
    fn reset_refuses_a_sibling_spirit_database_without_deleting_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let lojix = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&lojix).expect("create recognised Lojix source"));

        // This has a plausible memory schema marker but no Lojix family catalog:
        // a reset request that names it must fail closed and leave every byte.
        let spirit = directory.path().join("spirit.memory");
        let database = redb::Database::create(&spirit).expect("create Spirit witness");
        let write = database.begin_write().expect("write Spirit witness");
        {
            let mut metadata = write.open_table(META_TABLE).expect("Spirit metadata");
            metadata
                .insert(SCHEMA_VERSION_KEY, CURRENT_SCHEMA)
                .expect("Spirit schema marker");
        }
        write.commit().expect("commit Spirit witness");
        drop(database);
        let before = fs::read(&spirit).expect("read Spirit witness before reset");

        let error = reset_command(&spirit)
            .run()
            .expect_err("unrecognised Spirit database must not be reset");
        assert!(error.to_string().contains("catalog"));
        assert_eq!(
            fs::read(&spirit).expect("read Spirit witness after reset"),
            before
        );
        assert!(
            lojix.exists(),
            "sibling Lojix database is not the requested path"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reset_rejects_a_symlinked_store_before_deletion() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create v5 store"));
        let store_link = directory.path().join("store-link.db");
        symlink(&path, &store_link).expect("store symlink");
        assert!(reset_command(&store_link).run().is_err());
        assert!(path.exists(), "symlinked store referent is untouched");
    }
}
