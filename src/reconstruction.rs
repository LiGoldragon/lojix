//! Explicit, configuration-scoped reset support for the v4 Lojix store.
//!
//! v4 intentionally has no decoder or migration path for older layouts. A
//! caller that has stopped the daemon may reconstruct a recognised pre-v4
//! Lojix store with [`StoreResetCommand`]. The reset takes one inline DOTOS
//! request with no path. It derives the path only from the generated startup
//! archive named by the service-owned `LOJIX_CONFIGURATION` environment
//! variable, then validates the durable Lojix family/schema identity before
//! unlinking it and derives only its protocol sidecars from that validated
//! primary.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};

use dotos::{Delimiter, DotosBlock, DotosSource};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use rkyv::rancor;
use sema_engine::TableRegistration;

use crate::{DaemonConfiguration, Error, Result, Store, single_inline_dotos_argument};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const CURRENT_SCHEMA: u64 = 4;
const RECOGNISED_SCHEMAS: &[u64] = &[2, 3, CURRENT_SCHEMA];
const RESETTABLE_SCHEMAS: &[u64] = &[2, 3];
/// The reset service receives this from the NixOS module. It is deliberately
/// not inferred from a state directory or accepted as a CLI path.
pub const CONFIGURATION_ENV: &str = "LOJIX_CONFIGURATION";
/// These are protocol-owned suffixes from the retired v2/v3 migration path.
/// They are only derived after the primary store has proved itself to be a
/// recognised Lojix database.
const SIDECAR_SUFFIXES: &[&str] = &[
    ".schema-pre-v3.backup",
    ".schema-v3.pending",
    ".schema-v3.pending.owner",
];

/// Result of a guarded reset. A current v4 store is observed but never
/// rewritten: callers receive [`Self::AlreadyCurrent`] without deleting any
/// data. Only a recognised v2/v3 store is removed and recreated as v4.
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
                "(LojixStoreReset path={} schema=4 removed_sidecars={})",
                path.display(),
                removed_sidecars.len(),
            ),
            Self::AlreadyCurrent { path } => {
                write!(
                    formatter,
                    "(LojixStoreAlreadyCurrent path={} schema=4)",
                    path.display()
                )
            }
        }
    }
}

/// One exact, version-aware Lojix store reset. It accepts only inline
/// `(ResetStore)`, never a raw path, configuration path, directory, glob, or
/// request file. The archive supplied by the service environment owns the
/// store selection. That configured primary must be an existing regular,
/// non-symlink file with a recognised Lojix catalog; a sibling Spirit database
/// is never selected by name.
pub struct StoreResetCommand {
    configuration_path: PathBuf,
}

impl StoreResetCommand {
    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let configuration_path = std::env::var_os(CONFIGURATION_ENV)
            .ok_or_else(|| Error::MissingRuntimeConfiguration(CONFIGURATION_ENV.to_string()))?;
        Self::from_arguments_with_configuration(arguments, configuration_path)
    }

    /// Construct the command with an explicit generated archive. This exists
    /// for in-process tests; the executable reaches it exclusively through
    /// [`CONFIGURATION_ENV`].
    pub fn from_arguments_with_configuration(
        arguments: impl IntoIterator<Item = OsString>,
        configuration_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let text = single_inline_dotos_argument(arguments)?;
        parse_reset_request(&text)?;
        Ok(Self {
            configuration_path: configuration_path.into(),
        })
    }

    pub fn run(&self) -> Result<StoreResetOutcome> {
        let configuration_path = canonical_regular_file(&self.configuration_path, "configuration")?;
        let configuration = DaemonConfiguration::from_rkyv_file(&configuration_path)?;
        let path = canonical_regular_file(Path::new(&configuration.store_path), "store")?;
        let schema = recognised_lojix_schema(&path)?;
        if schema == CURRENT_SCHEMA {
            return Ok(StoreResetOutcome::AlreadyCurrent { path });
        }
        debug_assert!(RESETTABLE_SCHEMAS.contains(&schema));

        let sidecars = validated_sidecars(&path)?;
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
        // Store::open is the sole initializer. It stamps a new v4 schema and
        // proves that the replacement is usable before this command succeeds.
        drop(Store::open(&path)?);
        Ok(StoreResetOutcome::Recreated {
            path,
            removed_sidecars: sidecars,
        })
    }
}

/// `(ResetStore)` is a one-field parenthesised DOTOS object. It is deliberately
/// parsed structurally rather than treated as a magic string so extra fields,
/// another delimiter, and a malformed document are all rejected at the same
/// boundary.
fn parse_reset_request(text: &str) -> Result<()> {
    let root = DotosSource::new(text)
        .parse_root()
        .map_err(|error| Error::DotosRequestText(error.to_string()))?;
    let fields = DotosBlock::new(&root)
        .expect_children(Delimiter::Parenthesis, "ResetStore", 1)
        .map_err(|error| Error::DotosRequestText(error.to_string()))?;
    let request = DotosBlock::new(&fields[0])
        .parse_string()
        .map_err(|error| Error::DotosRequestText(error.to_string()))?;
    if request != "ResetStore" {
        return Err(Error::DotosRequestText(
            "expected the exact inline (ResetStore) object".to_string(),
        ));
    }
    Ok(())
}

fn canonical_regular_file(candidate: &Path, subject: &str) -> Result<PathBuf> {
    if !candidate.is_absolute()
        || candidate.file_name().is_none()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(Error::StoreMaintenance(format!(
            "reset {subject} must be an exact absolute, traversal-free file path"
        )));
    }
    let file_name = candidate.file_name().expect("checked above");
    let parent = candidate.parent().ok_or_else(|| {
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

fn sidecars_for(path: &Path) -> Vec<PathBuf> {
    SIDECAR_SUFFIXES
        .iter()
        .map(|suffix| PathBuf::from(format!("{}{}", path.display(), suffix)))
        .collect()
}

fn validated_sidecars(path: &Path) -> Result<Vec<PathBuf>> {
    let mut sidecars = Vec::new();
    for sidecar in sidecars_for(path) {
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

fn recognised_lojix_schema(path: &Path) -> Result<u64> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| {
        Error::StoreMaintenance(format!(
            "store {} did not open read-only: {error}",
            path.display(),
        ))
    })?;
    let version = schema_version_from_database(&database)?;
    if !RECOGNISED_SCHEMAS.contains(&version) {
        return Err(Error::StoreMaintenance(format!(
            "reset store {} has unsupported schema {version}; refusing to remove an unrecognised file",
            path.display(),
        )));
    }
    validate_lojix_catalog(&database, version)?;
    Ok(version)
}

#[cfg(test)]
fn schema_version(path: &Path) -> Result<u64> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| {
        Error::StoreMaintenance(format!(
            "store {} did not open read-only: {error}",
            path.display(),
        ))
    })?;
    schema_version_from_database(&database)
}

fn schema_version_from_database(database: &redb::ReadOnlyDatabase) -> Result<u64> {
    let transaction = database
        .begin_read()
        .map_err(|error| Error::StoreMaintenance(format!("store metadata read failed: {error}")))?;
    let table = transaction.open_table(META_TABLE).map_err(|error| {
        Error::StoreMaintenance(format!("store metadata table missing: {error}"))
    })?;
    let version = table
        .get(SCHEMA_VERSION_KEY)
        .map_err(|error| {
            Error::StoreMaintenance(format!("store schema version read failed: {error}"))
        })?
        .ok_or_else(|| Error::StoreMaintenance("store schema version is missing".to_string()))?;
    Ok(version.value())
}

/// Verify the persisted sema-engine catalog belongs wholly to one of the
/// Lojix store layouts. Schema v2 has the six core families; v3/v4 add the
/// deployment correlation families (and v3's retired quarantine family). A
/// valid reset source must contain every core family and no foreign family.
fn validate_lojix_catalog(database: &redb::ReadOnlyDatabase, version: u64) -> Result<()> {
    let transaction = database
        .begin_read()
        .map_err(|error| Error::StoreMaintenance(format!("store catalog read failed: {error}")))?;
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
        actual.insert(lojix_identity(&registration));
    }
    let recognised = recognised_lojix_identities();
    let core = core_lojix_identities();
    if actual.is_empty() || !core.is_subset(&actual) || !actual.is_subset(&recognised) {
        return Err(Error::StoreMaintenance(format!(
            "reset store catalog is not a recognised Lojix family/schema layout for schema {version}; refusing removal"
        )));
    }
    Ok(())
}

type StoreFamilyIdentity = (String, String, [u8; 32]);

fn lojix_identity(registration: &TableRegistration) -> StoreFamilyIdentity {
    (
        registration.table_name().to_string(),
        registration.identity().family().as_str().to_string(),
        *registration.identity().schema_hash().bytes(),
    )
}

fn core_lojix_identities() -> BTreeSet<StoreFamilyIdentity> {
    lojix_identities([
        (
            crate::LIVE_SET_TABLE.as_str(),
            crate::LIVE_SET_FAMILY,
            crate::LIVE_SET_SCHEMA_HASH,
        ),
        (
            crate::GC_ROOTS_TABLE.as_str(),
            crate::GC_ROOTS_FAMILY,
            crate::GC_ROOTS_SCHEMA_HASH,
        ),
        (
            crate::EVENT_LOG_TABLE.as_str(),
            crate::EVENT_LOG_FAMILY,
            crate::EVENT_LOG_SCHEMA_HASH,
        ),
        (
            crate::CONTAINER_LIFECYCLE_TABLE.as_str(),
            crate::CONTAINER_LIFECYCLE_FAMILY,
            crate::CONTAINER_LIFECYCLE_SCHEMA_HASH,
        ),
        (
            crate::DEPLOY_JOB_TABLE.as_str(),
            crate::DEPLOY_JOB_FAMILY,
            crate::DEPLOY_JOB_SCHEMA_HASH,
        ),
        (
            crate::TEST_RUN_TABLE.as_str(),
            crate::TEST_RUN_FAMILY,
            crate::TEST_RUN_SCHEMA_HASH,
        ),
    ])
}

fn recognised_lojix_identities() -> BTreeSet<StoreFamilyIdentity> {
    let mut identities = core_lojix_identities();
    identities.extend(lojix_identities([
        (
            crate::DEPLOYMENT_RECORD_TABLE.as_str(),
            crate::DEPLOYMENT_RECORD_FAMILY,
            crate::DEPLOYMENT_RECORD_SCHEMA_HASH,
        ),
        (
            crate::IDENTIFIER_ALLOCATION_TABLE.as_str(),
            crate::IDENTIFIER_ALLOCATION_FAMILY,
            crate::IDENTIFIER_ALLOCATION_SCHEMA_HASH,
        ),
        (
            crate::DEPLOYMENT_OUTBOX_TABLE.as_str(),
            crate::DEPLOYMENT_OUTBOX_FAMILY,
            crate::DEPLOYMENT_OUTBOX_SCHEMA_HASH,
        ),
        (
            crate::PENDING_TRANSITION_INTENT_TABLE.as_str(),
            crate::PENDING_TRANSITION_INTENT_FAMILY,
            crate::PENDING_TRANSITION_INTENT_SCHEMA_HASH,
        ),
        (
            "legacy-deployment-event-quarantine",
            "LegacyDeploymentEventQuarantineFamily",
            [10; 32],
        ),
    ]));
    identities
}

fn lojix_identities<const COUNT: usize>(
    identities: [(&str, &str, [u8; 32]); COUNT],
) -> BTreeSet<StoreFamilyIdentity> {
    identities
        .into_iter()
        .map(|(table, family, hash)| (table.to_string(), family.to_string(), hash))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn startup_archive(directory: &Path, store_path: &Path) -> PathBuf {
        let archive = directory.join("lojix-startup.rkyv");
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
        .expect("write generated startup archive");
        archive
    }

    fn reset_command(archive: &Path) -> StoreResetCommand {
        StoreResetCommand::from_arguments_with_configuration(
            [OsString::from("(ResetStore)")],
            archive.to_path_buf(),
        )
        .expect("exact reset command")
    }

    fn mark_pre_v4(path: &Path) {
        let database = redb::Database::open(path).expect("open recognised Lojix store");
        let write = database
            .begin_write()
            .expect("begin schema downgrade fixture");
        {
            let mut metadata = write.open_table(META_TABLE).expect("metadata table");
            metadata
                .insert(SCHEMA_VERSION_KEY, 3)
                .expect("pre-v4 schema marker");
        }
        write.commit().expect("commit schema downgrade fixture");
    }

    #[test]
    fn reset_cli_requires_exactly_one_inline_pathless_object() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let archive = startup_archive(directory.path(), &directory.path().join("store.sema"));
        for arguments in [
            Vec::new(),
            vec![OsString::from("--help")],
            vec![OsString::from("--pretty")],
            vec![OsString::from("/tmp/lojix-store.sema")],
            vec![OsString::from("StoreResetRequest.{/tmp/lojix-store.sema}")],
            vec![OsString::from("(ResetStore)"), OsString::from("extra")],
        ] {
            assert!(
                StoreResetCommand::from_arguments_with_configuration(arguments, archive.clone())
                    .is_err(),
                "reset must accept only one inline (ResetStore) object"
            );
        }
    }

    #[test]
    fn reset_requires_an_existing_generated_configuration_and_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let missing_archive = directory.path().join("missing.rkyv");
        assert!(reset_command(&missing_archive).run().is_err());

        let missing_store = directory.path().join("configured-lojix-store.db");
        let archive = startup_archive(directory.path(), &missing_store);
        let error = reset_command(&archive)
            .run()
            .expect_err("an absent configured store must not be created by reset");
        assert!(error.to_string().contains("store"));
    }

    #[test]
    fn reset_returns_already_current_without_touching_v4_data_or_sidecars() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create v4 store"));
        let sidecar = sidecars_for(&path)
            .into_iter()
            .next()
            .expect("sidecar path");
        fs::write(&sidecar, "must survive with v4 primary").expect("sidecar witness");
        let before = fs::read(&path).expect("read v4 store before reset");
        let archive = startup_archive(directory.path(), &path);

        assert_eq!(
            reset_command(&archive).run().expect("current-store result"),
            StoreResetOutcome::AlreadyCurrent { path: path.clone() }
        );
        assert_eq!(fs::read(&path).expect("read v4 store after reset"), before);
        assert!(sidecar.exists(), "v4 sidecars are untouched too");
    }

    #[test]
    fn reset_recreates_only_a_recognised_pre_v4_lojix_store_and_its_sidecars() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create recognised Lojix source"));
        mark_pre_v4(&path);
        for sidecar in sidecars_for(&path) {
            fs::write(sidecar, "stale Lojix sidecar").expect("write sidecar");
        }
        let spirit = directory.path().join("spirit.sema");
        fs::write(&spirit, "must survive").expect("write Spirit witness");
        let archive = startup_archive(directory.path(), &path);
        let outcome = reset_command(&archive).run().expect("reset pre-v4 source");
        assert!(matches!(outcome, StoreResetOutcome::Recreated { .. }));
        assert_eq!(
            schema_version(&path).expect("fresh v4 schema"),
            CURRENT_SCHEMA
        );
        assert_eq!(
            fs::read_to_string(spirit).expect("Spirit witness"),
            "must survive"
        );
        assert!(sidecars_for(&path).iter().all(|sidecar| !sidecar.exists()));
    }

    #[test]
    fn reset_refuses_a_sibling_spirit_database_without_deleting_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let lojix = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&lojix).expect("create recognised Lojix source"));

        // This has a plausible sema schema marker but no Lojix family catalog:
        // a reset request that names it must fail closed and leave every byte.
        let spirit = directory.path().join("spirit.sema");
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

        let archive = startup_archive(directory.path(), &spirit);
        let error = reset_command(&archive)
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
    fn reset_rejects_symlinked_configuration_and_store_before_deletion() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create v4 store"));
        let archive = startup_archive(directory.path(), &path);
        let archive_link = directory.path().join("configuration-link.rkyv");
        symlink(&archive, &archive_link).expect("configuration symlink");
        assert!(reset_command(&archive_link).run().is_err());
        assert!(
            path.exists(),
            "symlinked configuration cannot select a store"
        );

        let store_link = directory.path().join("store-link.db");
        symlink(&path, &store_link).expect("store symlink");
        let linked_archive = startup_archive(directory.path(), &store_link);
        assert!(reset_command(&linked_archive).run().is_err());
        assert!(path.exists(), "symlinked store referent is untouched");
    }
}
