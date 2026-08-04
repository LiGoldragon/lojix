//! Explicit, path-scoped reset support for the v4 Lojix store.
//!
//! v4 intentionally has no decoder or migration path for older layouts. A
//! caller that has stopped the daemon may discard its explicitly configured
//! Lojix store with [`StoreResetCommand`]. The reset takes one inline DOTOS
//! request, validates the exact supplied path and the durable Lojix
//! family/schema identity before unlinking it, and derives only its protocol
//! sidecars from that validated path.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};

use dotos::{DotosDecode, DotosSource};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use rkyv::rancor;
use sema_engine::TableRegistration;

use crate::{Error, Result, Store};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const CURRENT_SCHEMA: u64 = 4;
const RESETTABLE_SCHEMAS: &[u64] = &[2, 3, CURRENT_SCHEMA];
/// These are protocol-owned suffixes from the retired v2/v3 migration path.
/// They are only derived after the primary store has proved itself to be a
/// recognised Lojix database.
const SIDECAR_SUFFIXES: &[&str] = &[
    ".schema-pre-v3.backup",
    ".schema-v3.pending",
    ".schema-v3.pending.owner",
];

/// Result of an idempotent reset. The store is freshly initialised at v4 on
/// every successful invocation, including when it was already empty/current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreResetOutcome {
    pub path: PathBuf,
    pub removed_store: bool,
    pub removed_sidecars: Vec<PathBuf>,
}

impl std::fmt::Display for StoreResetOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "(LojixStoreReset path={} schema=4 removed_store={} removed_sidecars={})",
            self.path.display(),
            self.removed_store,
            self.removed_sidecars.len(),
        )
    }
}

/// The one inline reset request accepted by `lojix-reset-store`.
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct StoreResetRequest {
    // The surrounding reset object, not a raw CLI operand, carries this path.
    // Path ownership and filesystem safety are checked after decode.
    store_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
enum StoreResetInput {
    StoreResetRequest(StoreResetRequest),
}

impl StoreResetInput {
    fn into_request(self) -> StoreResetRequest {
        match self {
            Self::StoreResetRequest(request) => request,
        }
    }
}

/// One exact, version-aware Lojix store reset. It accepts only an inline
/// `StoreResetRequest.{<configured-absolute-path>}`, never a raw path,
/// directory, glob, traversal, or symlinked primary store. An existing primary
/// file must also carry a recognised Lojix family catalog and supported schema
/// before removal, so a sibling Spirit database is never selected by name.
pub struct StoreResetCommand {
    path: PathBuf,
}

impl StoreResetCommand {
    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let text = inline_dotos_text(arguments)?;
        let request = DotosSource::new(&text)
            .parse::<StoreResetInput>()
            .map_err(|error| Error::DotosRequestText(error.to_string()))?
            .into_request();
        Ok(Self {
            path: PathBuf::from(request.store_path),
        })
    }

    pub fn run(&self) -> Result<StoreResetOutcome> {
        let path = canonical_store_path(&self.path)?;
        let removed_store = remove_owned_regular_file(&path)?;
        let mut removed_sidecars = Vec::new();
        // A missing primary has no durable family identity to prove ownership.
        // Opening a fresh store below is safe and idempotent; leaving suffixes
        // alone avoids deleting a similarly named non-Lojix sibling.
        if removed_store {
            for sidecar in sidecars_for(&path) {
                if remove_owned_sidecar(&sidecar)? {
                    removed_sidecars.push(sidecar);
                }
            }
        }
        // Store::open is the sole initializer. It stamps a new v4 schema and
        // proves that the replacement is usable before this command succeeds.
        drop(Store::open(&path)?);
        Ok(StoreResetOutcome {
            path,
            removed_store,
            removed_sidecars,
        })
    }
}

fn inline_dotos_text(arguments: impl IntoIterator<Item = OsString>) -> Result<String> {
    let mut arguments = arguments.into_iter();
    let Some(argument) = arguments.next() else {
        return Err(Error::ExpectedSingleArgument);
    };
    if arguments.next().is_some() {
        return Err(Error::ExpectedSingleArgument);
    }
    let argument = argument
        .into_string()
        .map_err(|_| Error::InlineDotosRequired)?;
    if argument.starts_with('-') {
        return Err(Error::FlagArgument(argument));
    }
    Ok(argument)
}

fn canonical_store_path(candidate: &Path) -> Result<PathBuf> {
    if !candidate.is_absolute()
        || candidate.file_name().is_none()
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Error::StoreMaintenance(
            "reset requires the exact configured absolute, traversal-free store path".to_string(),
        ));
    }
    let file_name = candidate.file_name().expect("checked above");
    let parent = candidate.parent().ok_or_else(|| {
        Error::StoreMaintenance(
            "reset path must have an existing canonical parent directory".to_string(),
        )
    })?;
    let parent = fs::canonicalize(parent).map_err(|error| {
        Error::StoreMaintenance(format!(
            "reset parent {} must exist and be canonicalizable: {error}",
            parent.display(),
        ))
    })?;
    let metadata = fs::metadata(&parent).map_err(|error| {
        Error::StoreMaintenance(format!(
            "reset parent {} is unreadable: {error}",
            parent.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(Error::StoreMaintenance(
            "reset parent is not a directory".to_string(),
        ));
    }
    Ok(parent.join(file_name))
}

fn sidecars_for(path: &Path) -> Vec<PathBuf> {
    SIDECAR_SUFFIXES
        .iter()
        .map(|suffix| PathBuf::from(format!("{}{}", path.display(), suffix)))
        .collect()
}

fn remove_owned_regular_file(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(Error::StoreMaintenance(format!(
                "inspect reset store {}: {error}",
                path.display(),
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::StoreMaintenance(format!(
            "reset store {} must be a regular non-symlink file",
            path.display(),
        )));
    }
    recognised_lojix_schema(path)?;
    fs::remove_file(path).map_err(|error| {
        Error::StoreMaintenance(format!("remove reset store {}: {error}", path.display()))
    })?;
    Ok(true)
}

fn remove_owned_sidecar(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(Error::StoreMaintenance(format!(
                "inspect Lojix reset sidecar {}: {error}",
                path.display(),
            )));
        }
    };
    if metadata.is_dir() {
        return Err(Error::StoreMaintenance(format!(
            "Lojix reset sidecar {} is a directory; refusing broad deletion",
            path.display(),
        )));
    }
    // remove_file unlinks a symlink itself, never its referent. This is safe
    // for a stale sidecar and avoids following it into any unrelated store.
    fs::remove_file(path).map_err(|error| {
        Error::StoreMaintenance(format!(
            "remove Lojix reset sidecar {}: {error}",
            path.display()
        ))
    })?;
    Ok(true)
}

fn recognised_lojix_schema(path: &Path) -> Result<u64> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| {
        Error::StoreMaintenance(format!(
            "store {} did not open read-only: {error}",
            path.display(),
        ))
    })?;
    let version = schema_version_from_database(&database)?;
    if !RESETTABLE_SCHEMAS.contains(&version) {
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

    fn reset_request(path: &Path) -> OsString {
        OsString::from(format!("StoreResetRequest.{{{}}}", path.display()))
    }

    #[test]
    fn reset_cli_requires_one_inline_object_and_rejects_raw_paths_and_flags() {
        assert!(StoreResetCommand::from_arguments(Vec::new()).is_err());
        assert!(StoreResetCommand::from_arguments([OsString::from("--help")]).is_err());
        assert!(StoreResetCommand::from_arguments([OsString::from("--pretty")]).is_err());
        assert!(
            StoreResetCommand::from_arguments([OsString::from("/tmp/lojix-store.sema")]).is_err()
        );
        assert!(
            StoreResetCommand::from_arguments([
                reset_request(Path::new("/tmp/lojix-store.sema")),
                OsString::from("extra"),
            ])
            .is_err()
        );
    }

    #[test]
    fn reset_rejects_non_absolute_traversing_and_directory_targets() {
        for path in [
            "relative/lojix-store.sema",
            "/tmp",
            "/tmp/../tmp/lojix-store.sema",
        ] {
            let error = StoreResetCommand::from_arguments([reset_request(Path::new(path))])
                .and_then(|command| command.run())
                .expect_err("unsafe reset path must be rejected");
            assert!(
                error.to_string().contains("reset")
                    || error.to_string().contains("dot-application")
            );
        }
    }

    #[test]
    fn reset_is_idempotent_and_creates_a_fresh_v4_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        // The reset honours this configured name; it is not coupled to a
        // basename such as `lojix.sema`.
        let path = directory.path().join("configured-lojix-store.db");
        let command =
            StoreResetCommand::from_arguments([reset_request(&path)]).expect("reset command");
        let first = command.run().expect("first reset");
        assert!(!first.removed_store);
        assert_eq!(schema_version(&path).expect("v4 schema"), CURRENT_SCHEMA);
        let second = command.run().expect("idempotent reset");
        assert!(second.removed_store);
        assert_eq!(
            schema_version(&path).expect("fresh v4 schema"),
            CURRENT_SCHEMA
        );
    }

    #[test]
    fn reset_only_unlinks_its_named_sidecars() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("configured-lojix-store.db");
        drop(Store::open(&path).expect("create recognised Lojix source"));
        for sidecar in sidecars_for(&path) {
            fs::write(sidecar, "stale Lojix sidecar").expect("write sidecar");
        }
        let spirit = directory.path().join("spirit.sema");
        fs::write(&spirit, "must survive").expect("write Spirit witness");
        StoreResetCommand::from_arguments([reset_request(&path)])
            .expect("reset command")
            .run()
            .expect("reset");
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

        let error = StoreResetCommand::from_arguments([reset_request(&spirit)])
            .expect("inline request")
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
}
