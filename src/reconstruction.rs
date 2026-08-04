//! Explicit, path-scoped reset support for the v4 Lojix store.
//!
//! v4 intentionally has no decoder or migration path for older layouts. A
//! caller that has stopped the daemon may discard the one configured Lojix
//! store with [`StoreResetCommand`]; the reset only accepts an exact
//! `lojix.sema` file and its narrowly named Lojix schema sidecars.

use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};

use redb::{ReadableDatabase, TableDefinition};

use crate::{Error, Result, Store};

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const CURRENT_SCHEMA: u64 = 4;
const RESETTABLE_SCHEMAS: &[u64] = &[2, 3, CURRENT_SCHEMA];
const STORE_BASENAME: &str = "lojix.sema";
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

/// One exact, version-aware Lojix store reset. It never accepts a directory,
/// glob, relative path, parent traversal, symlinked store, or a basename other
/// than `lojix.sema`; therefore it cannot select the Spirit database.
pub struct StoreResetCommand {
    path: PathBuf,
}

impl StoreResetCommand {
    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        Ok(Self {
            path: one_path_argument(arguments)?,
        })
    }

    pub fn run(&self) -> Result<StoreResetOutcome> {
        let path = canonical_store_path(&self.path)?;
        let removed_store = remove_owned_regular_file(&path)?;
        let mut removed_sidecars = Vec::new();
        for sidecar in sidecars_for(&path) {
            if remove_owned_sidecar(&sidecar)? {
                removed_sidecars.push(sidecar);
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

fn one_path_argument(arguments: impl IntoIterator<Item = OsString>) -> Result<PathBuf> {
    let mut arguments = arguments.into_iter();
    let Some(path) = arguments.next() else {
        return Err(Error::ExpectedSingleArgument);
    };
    if arguments.next().is_some() {
        return Err(Error::ExpectedSingleArgument);
    }
    if path.to_string_lossy().starts_with('-') {
        return Err(Error::FlagArgument(path.to_string_lossy().into_owned()));
    }
    Ok(PathBuf::from(path))
}

fn canonical_store_path(candidate: &Path) -> Result<PathBuf> {
    if !candidate.is_absolute()
        || candidate
            .file_name()
            .is_none_or(|name| name != STORE_BASENAME)
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Error::StoreMaintenance(
            "reset requires one absolute, traversal-free path named lojix.sema".to_string(),
        ));
    }
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
    Ok(parent.join(STORE_BASENAME))
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
    let version = schema_version(path)?;
    if !RESETTABLE_SCHEMAS.contains(&version) {
        return Err(Error::StoreMaintenance(format!(
            "reset store {} has unsupported schema {version}; refusing to remove an unrecognised file",
            path.display(),
        )));
    }
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

fn schema_version(path: &Path) -> Result<u64> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| {
        Error::StoreMaintenance(format!(
            "store {} did not open read-only: {error}",
            path.display(),
        ))
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_rejects_broad_or_unowned_paths() {
        for path in [
            "relative/lojix.sema",
            "/tmp",
            "/tmp/spirit.sema",
            "/tmp/../tmp/lojix.sema",
        ] {
            let error = StoreResetCommand::from_arguments([OsString::from(path)])
                .expect("argument parses")
                .run()
                .expect_err("unsafe reset path must be rejected");
            assert!(error.to_string().contains("reset"));
        }
    }

    #[test]
    fn reset_is_idempotent_and_creates_a_fresh_v4_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(STORE_BASENAME);
        let command = StoreResetCommand::from_arguments([path.clone().into_os_string()])
            .expect("reset command");
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
        let path = directory.path().join(STORE_BASENAME);
        for sidecar in sidecars_for(&path) {
            fs::write(sidecar, "stale Lojix sidecar").expect("write sidecar");
        }
        let spirit = directory.path().join("spirit.sema");
        fs::write(&spirit, "must survive").expect("write Spirit witness");
        StoreResetCommand::from_arguments([path.clone().into_os_string()])
            .expect("reset command")
            .run()
            .expect("reset");
        assert_eq!(
            fs::read_to_string(spirit).expect("Spirit witness"),
            "must survive"
        );
        assert!(sidecars_for(&path).iter().all(|sidecar| !sidecar.exists()));
    }
}
