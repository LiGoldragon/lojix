//! Read-only schema-one reconstruction and crash-safe in-place migration.
//!
//! The canonical schema-one store is never opened writable. Migration first
//! validates every known row and cross-table relation, then creates a
//! byte-identical permanent backup. A fresh schema-two store is constructed at
//! a staging path, reopened through [`Store`], synced, and only then atomically
//! renamed over the canonical path.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize};
use sema_engine::TableRegistration;

use crate::schema::sema::{
    ContainerLifecycleRecord, DeployJob, EventLogEntry, GcRoot, LiveGeneration, LoggedEvent,
    StoredTestRun,
};
use crate::{Error, Result, Store};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const SCHEMA_ONE: u64 = 1;
const SCHEMA_TWO: u64 = 2;
const LIVE_SET_TABLE: &str = "live-set";
const GC_ROOTS_TABLE: &str = "gc-roots";
const EVENT_LOG_TABLE: &str = "event-log";
const CONTAINER_LIFECYCLE_TABLE: &str = "container-lifecycle";
const DEPLOY_JOB_TABLE: &str = "deploy-job";
const TEST_RUN_TABLE: &str = "test-run";
/// Counts preserved by a schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCounts {
    pub generations: usize,
    pub gc_roots: usize,
    pub event_log_entries: usize,
    pub container_lifecycle_records: usize,
    pub deploy_jobs: usize,
    pub test_runs: usize,
}

impl std::fmt::Display for StoreCounts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "generations={} roots={} events={} containers={} deploy_jobs={} test_runs={}",
            self.generations,
            self.gc_roots,
            self.event_log_entries,
            self.container_lifecycle_records,
            self.deploy_jobs,
            self.test_runs,
        )
    }
}

/// Stable paths owned by one canonical store migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPaths {
    canonical: PathBuf,
    backup: PathBuf,
    staging: PathBuf,
    staging_owner: PathBuf,
}

impl MigrationPaths {
    pub fn for_store(path: impl Into<PathBuf>) -> Self {
        let canonical = path.into();
        Self {
            backup: with_suffix(&canonical, ".schema-v1.backup"),
            staging: with_suffix(&canonical, ".schema-v2.pending"),
            staging_owner: with_suffix(&canonical, ".schema-v2.pending.owner"),
            canonical,
        }
    }

    pub fn canonical(&self) -> &Path {
        &self.canonical
    }

    pub fn backup(&self) -> &Path {
        &self.backup
    }

    pub fn staging(&self) -> &Path {
        &self.staging
    }

    pub fn staging_owner(&self) -> &Path {
        &self.staging_owner
    }
}

/// Result of an idempotent pre-start migration invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOutcome {
    NoStore {
        path: PathBuf,
    },
    AlreadyCurrent {
        path: PathBuf,
        counts: StoreCounts,
    },
    Migrated {
        path: PathBuf,
        backup: PathBuf,
        counts: StoreCounts,
    },
}

impl std::fmt::Display for MigrationOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoStore { path } => {
                write!(
                    formatter,
                    "(StoreMigrationNotNeeded {} missing)",
                    path.display()
                )
            }
            Self::AlreadyCurrent { path, counts } => write!(
                formatter,
                "(StoreMigrationNotNeeded {} schema=2 {counts})",
                path.display()
            ),
            Self::Migrated {
                path,
                backup,
                counts,
            } => write!(
                formatter,
                "(StoreMigrated {} backup={} schema=1->2 {counts})",
                path.display(),
                backup.display()
            ),
        }
    }
}

/// One-argument CLI parser for `lojix-migrate-store`.
pub struct StoreMigrationCommand {
    path: PathBuf,
}

impl StoreMigrationCommand {
    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
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
        Ok(Self {
            path: PathBuf::from(path),
        })
    }

    pub fn run(&self) -> Result<MigrationOutcome> {
        StoreMigrator::new(self.path.clone()).migrate()
    }
}

/// Idempotent schema-one to schema-two store migrator.
pub struct StoreMigrator {
    paths: MigrationPaths,
}

impl StoreMigrator {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            paths: MigrationPaths::for_store(path),
        }
    }

    pub fn paths(&self) -> &MigrationPaths {
        &self.paths
    }

    pub fn migrate(&self) -> Result<MigrationOutcome> {
        if !self.paths.canonical.exists() {
            if self.paths.backup.exists()
                || self.paths.staging.exists()
                || self.paths.staging_owner.exists()
            {
                return Err(migration_error(format!(
                    "canonical store {} is missing while migration artifacts remain",
                    self.paths.canonical.display()
                )));
            }
            return Ok(MigrationOutcome::NoStore {
                path: self.paths.canonical.clone(),
            });
        }

        match schema_version(&self.paths.canonical)? {
            SCHEMA_TWO => {
                let counts =
                    SchemaOneSnapshot::read_version(&self.paths.canonical, SCHEMA_TWO)?.counts();
                Ok(MigrationOutcome::AlreadyCurrent {
                    path: self.paths.canonical.clone(),
                    counts,
                })
            }
            SCHEMA_ONE => self.migrate_schema_one(),
            version => Err(migration_error(format!(
                "canonical store {} has schema {version}, expected schema one or two",
                self.paths.canonical.display()
            ))),
        }
    }

    fn migrate_schema_one(&self) -> Result<MigrationOutcome> {
        let snapshot = SchemaOneSnapshot::read(&self.paths.canonical)?;
        let counts = snapshot.counts();

        self.ensure_backup(&snapshot)?;
        self.prepare_staging()?;
        snapshot.reconstruct(&self.paths.staging)?;
        preserve_metadata(&self.paths.backup, &self.paths.staging)?;
        sync_file(&self.paths.staging)?;

        {
            let reopened = Store::open(&self.paths.staging)?;
            let actual = reopened.migration_counts()?;
            if actual != counts {
                return Err(migration_error(format!(
                    "reopened schema-two staging counts differ: expected {counts}, found {actual}"
                )));
            }
        }

        if !files_equal(&self.paths.canonical, &self.paths.backup)? {
            return Err(migration_error(
                "canonical schema-one store changed after backup; refusing replacement",
            ));
        }

        fs::rename(&self.paths.staging, &self.paths.canonical)?;
        sync_parent(&self.paths.canonical)?;

        let reopened = Store::open(&self.paths.canonical)?;
        let actual = reopened.migration_counts()?;
        if actual != counts {
            return Err(migration_error(format!(
                "canonical schema-two counts differ after replacement: expected {counts}, found {actual}"
            )));
        }
        remove_tool_scratch(&self.paths.staging_owner)?;

        Ok(MigrationOutcome::Migrated {
            path: self.paths.canonical.clone(),
            backup: self.paths.backup.clone(),
            counts,
        })
    }

    fn ensure_backup(&self, snapshot: &SchemaOneSnapshot) -> Result<()> {
        if self.paths.backup.exists() {
            if !files_equal(&self.paths.canonical, &self.paths.backup)? {
                return Err(migration_error(format!(
                    "existing backup {} is not byte-identical to canonical schema-one store",
                    self.paths.backup.display()
                )));
            }
            if !metadata_equal(&self.paths.canonical, &self.paths.backup)? {
                return Err(migration_error(format!(
                    "existing backup {} does not preserve canonical ownership and mode",
                    self.paths.backup.display()
                )));
            }
            let backup = SchemaOneSnapshot::read(&self.paths.backup)?;
            if backup.counts() != snapshot.counts() {
                return Err(migration_error(
                    "existing schema-one backup counts differ from canonical store",
                ));
            }
            return Ok(());
        }

        fs::hard_link(&self.paths.canonical, &self.paths.backup).map_err(|error| {
            migration_error(format!(
                "cannot create non-overwriting schema-one backup {}: {error}",
                self.paths.backup.display()
            ))
        })?;
        sync_parent(&self.paths.backup)?;
        if !same_file(&self.paths.canonical, &self.paths.backup)?
            || !files_equal(&self.paths.canonical, &self.paths.backup)?
        {
            return Err(migration_error(
                "new schema-one backup does not preserve the canonical inode and bytes",
            ));
        }
        Ok(())
    }

    fn prepare_staging(&self) -> Result<()> {
        if self.paths.staging_owner.exists() {
            if !same_file(&self.paths.backup, &self.paths.staging_owner)? {
                return Err(migration_error(format!(
                    "staging ownership marker {} conflicts with the schema-one backup",
                    self.paths.staging_owner.display()
                )));
            }
            remove_tool_scratch(&self.paths.staging)?;
            return Ok(());
        }
        if self.paths.staging.exists() {
            return Err(migration_error(format!(
                "unowned staging path {} already exists",
                self.paths.staging.display()
            )));
        }
        fs::hard_link(&self.paths.backup, &self.paths.staging_owner).map_err(|error| {
            migration_error(format!(
                "cannot create staging ownership marker {}: {error}",
                self.paths.staging_owner.display()
            ))
        })?;
        sync_parent(&self.paths.staging_owner)
    }
}

struct SchemaOneSnapshot {
    generations: Vec<LiveGeneration>,
    roots: Vec<GcRoot>,
    events: Vec<EventLogEntry>,
    containers: Vec<ContainerLifecycleRecord>,
    deploy_jobs: Vec<DeployJob>,
    test_runs: Vec<StoredTestRun>,
}

impl SchemaOneSnapshot {
    fn read(source: &Path) -> Result<Self> {
        Self::read_version(source, SCHEMA_ONE)
    }

    fn read_version(source: &Path, expected_version: u64) -> Result<Self> {
        let database = redb::ReadOnlyDatabase::open(source).map_err(|error| {
            migration_error(format!(
                "store snapshot {} did not open read-only: {error}",
                source.display()
            ))
        })?;
        let version = schema_version_from_database(&database)?;
        if version != expected_version {
            return Err(migration_error(format!(
                "source schema version is {version}, expected schema {expected_version}"
            )));
        }
        validate_catalog(&database)?;

        let generations = rows::<LiveGeneration>(&database, LIVE_SET_TABLE)?;
        let roots = rows::<GcRoot>(&database, GC_ROOTS_TABLE)?;
        let mut events = rows::<EventLogEntry>(&database, EVENT_LOG_TABLE)?;
        let containers = rows::<ContainerLifecycleRecord>(&database, CONTAINER_LIFECYCLE_TABLE)?;
        let deploy_jobs = rows::<DeployJob>(&database, DEPLOY_JOB_TABLE)?;
        let test_runs = rows::<StoredTestRun>(&database, TEST_RUN_TABLE)?;

        validate_keys(LIVE_SET_TABLE, &generations, |record| {
            *record.generation_identifier.payload()
        })?;
        validate_keys(GC_ROOTS_TABLE, &roots, |record| {
            *record.generation_identifier.payload()
        })?;
        validate_keys(EVENT_LOG_TABLE, &events, |record| {
            *record.event_log_position.payload()
        })?;
        validate_keys(CONTAINER_LIFECYCLE_TABLE, &containers, |record| {
            *record.event_log_position.payload()
        })?;
        validate_keys(DEPLOY_JOB_TABLE, &deploy_jobs, |record| {
            *record.deployment_identifier.payload()
        })?;
        validate_keys(TEST_RUN_TABLE, &test_runs, |record| {
            *record.test_run_identifier.payload()
        })?;
        events.sort_by_key(|(_key, record)| *record.event_log_position.payload());

        let snapshot = Self {
            generations: into_records(generations),
            roots: into_records(roots),
            events: into_records(events),
            containers: into_records(containers),
            deploy_jobs: into_records(deploy_jobs),
            test_runs: into_records(test_runs),
        };
        snapshot.validate_relations()?;
        Ok(snapshot)
    }

    fn counts(&self) -> StoreCounts {
        StoreCounts {
            generations: self.generations.len(),
            gc_roots: self.roots.len(),
            event_log_entries: self.events.len(),
            container_lifecycle_records: self.containers.len(),
            deploy_jobs: self.deploy_jobs.len(),
            test_runs: self.test_runs.len(),
        }
    }

    fn validate_relations(&self) -> Result<()> {
        let generations: BTreeMap<_, _> = self
            .generations
            .iter()
            .map(|generation| (*generation.generation_identifier.payload(), generation))
            .collect();
        if generations.len() != self.generations.len() {
            return Err(migration_error("duplicate generation identifier"));
        }
        let roots: BTreeMap<_, _> = self
            .roots
            .iter()
            .map(|root| (*root.generation_identifier.payload(), root))
            .collect();
        if roots.len() != self.roots.len() {
            return Err(migration_error("duplicate gc-root generation identifier"));
        }
        if generations.len() != roots.len() {
            return Err(migration_error(
                "generation/root records are not a complete one-to-one set",
            ));
        }
        for (identifier, generation) in generations {
            let root = roots.get(&identifier).ok_or_else(|| {
                migration_error(format!("generation {identifier} has no gc root"))
            })?;
            if generation.cluster_name != root.cluster_name
                || generation.node_name != root.node_name
                || generation.generation_slot != root.generation_slot
                || generation.closure_path != root.closure_path
            {
                return Err(migration_error(format!(
                    "generation/root {identifier} identity mismatch"
                )));
            }
        }

        let event_positions: BTreeSet<_> = self
            .events
            .iter()
            .map(|event| *event.event_log_position.payload())
            .collect();
        if event_positions.len() != self.events.len() {
            return Err(migration_error("duplicate event-log position"));
        }
        for event in &self.events {
            let outer = *event.event_log_position.payload();
            let nested = match &event.record {
                LoggedEvent::Deployment(record) => *record.event_log_position.payload(),
                LoggedEvent::CacheRetention(record) => *record.event_log_position.payload(),
                LoggedEvent::Container(record) => *record.event_log_position.payload(),
            };
            if nested != outer {
                return Err(migration_error(format!(
                    "event-log entry {outer} contains nested event position {nested}"
                )));
            }
        }
        for container in &self.containers {
            let position = *container.event_log_position.payload();
            let matches_event = self.events.iter().any(|event| {
                *event.event_log_position.payload() == position
                    && matches!(&event.record, LoggedEvent::Container(record) if record == container)
            });
            if !matches_event {
                return Err(migration_error(format!(
                    "container transition at event position {position} has no matching event"
                )));
            }
        }
        for event in &self.events {
            let LoggedEvent::Container(record) = &event.record else {
                continue;
            };
            if !self.containers.iter().any(|container| container == record) {
                return Err(migration_error(format!(
                    "container event at position {} has no matching container row",
                    event.event_log_position.payload()
                )));
            }
        }

        let deploy_identifiers: BTreeSet<_> = self
            .deploy_jobs
            .iter()
            .map(|job| *job.deployment_identifier.payload())
            .collect();
        if deploy_identifiers.len() != self.deploy_jobs.len() {
            return Err(migration_error("duplicate deploy-job identifier"));
        }
        let test_identifiers: BTreeSet<_> = self
            .test_runs
            .iter()
            .map(|run| *run.test_run_identifier.payload())
            .collect();
        if test_identifiers.len() != self.test_runs.len() {
            return Err(migration_error("duplicate test-run identifier"));
        }
        Ok(())
    }

    fn reconstruct(self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(migration_error(format!(
                "schema-two staging path already exists: {}",
                destination.display()
            )));
        }
        let store = Store::open(destination)?;
        store.seed_migration(
            self.generations,
            self.roots,
            self.events,
            self.containers,
            self.deploy_jobs,
            self.test_runs,
        )
    }
}

fn schema_version(path: &Path) -> Result<u64> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| {
        migration_error(format!(
            "store {} did not open read-only: {error}",
            path.display()
        ))
    })?;
    schema_version_from_database(&database)
}

fn schema_version_from_database(database: &redb::ReadOnlyDatabase) -> Result<u64> {
    let transaction = database
        .begin_read()
        .map_err(|error| migration_error(format!("store metadata read failed: {error}")))?;
    let table = transaction
        .open_table(META_TABLE)
        .map_err(|error| migration_error(format!("store metadata table missing: {error}")))?;
    let version = table
        .get(SCHEMA_VERSION_KEY)
        .map_err(|error| migration_error(format!("store schema version read failed: {error}")))?
        .ok_or_else(|| migration_error("store schema version is missing"))?;
    Ok(version.value())
}

fn validate_catalog(database: &redb::ReadOnlyDatabase) -> Result<()> {
    let transaction = database
        .begin_read()
        .map_err(|error| migration_error(format!("source catalog read failed: {error}")))?;
    let table = transaction
        .open_table(CATALOG_TABLE)
        .map_err(|error| migration_error(format!("source catalog table missing: {error}")))?;
    let mut actual = BTreeSet::new();
    for row in table
        .iter()
        .map_err(|error| migration_error(format!("source catalog iteration failed: {error}")))?
    {
        let (_key, value) =
            row.map_err(|error| migration_error(format!("source catalog row failed: {error}")))?;
        let registration = rkyv::from_bytes::<TableRegistration, rancor::Error>(value.value())
            .map_err(|error| migration_error(format!("source catalog decode failed: {error}")))?;
        actual.insert((
            registration.table_name().to_string(),
            registration.identity().family().as_str().to_string(),
            *registration.identity().schema_hash().bytes(),
        ));
    }
    let expected = [
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
    ]
    .into_iter()
    .map(|(table, family, schema_hash)| (table.to_string(), family.to_string(), schema_hash))
    .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(migration_error(format!(
            "source catalog differs from the six known Lojix table identities: found {actual:?}"
        )));
    }
    Ok(())
}

fn rows<Record>(database: &redb::ReadOnlyDatabase, name: &str) -> Result<Vec<(String, Record)>>
where
    Record: Archive,
    Record::Archived: RkyvDeserialize<Record, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
    let transaction = database
        .begin_read()
        .map_err(|error| migration_error(format!("source table {name} read failed: {error}")))?;
    let definition: TableDefinition<String, &[u8]> = TableDefinition::new(name);
    let table = match transaction.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => {
            return Err(migration_error(format!(
                "source table {name} open failed: {error}"
            )));
        }
    };
    let mut decoded = Vec::new();
    for row in table.iter().map_err(|error| {
        migration_error(format!("source table {name} iteration failed: {error}"))
    })? {
        let (key, value) = row
            .map_err(|error| migration_error(format!("source table {name} row failed: {error}")))?;
        let record = rkyv::from_bytes::<Record, rancor::Error>(value.value()).map_err(|error| {
            migration_error(format!("source table {name} decode failed: {error}"))
        })?;
        decoded.push((key.value().to_string(), record));
    }
    Ok(decoded)
}

fn validate_keys<Record>(
    table: &str,
    rows: &[(String, Record)],
    identifier: impl Fn(&Record) -> u64,
) -> Result<()> {
    for (key, record) in rows {
        let expected = identifier(record).to_string();
        if key != &expected {
            return Err(migration_error(format!(
                "source table {table} row key {key:?} does not match record identifier {expected}"
            )));
        }
    }
    Ok(())
}

fn into_records<Record>(rows: Vec<(String, Record)>) -> Vec<Record> {
    rows.into_iter().map(|(_key, record)| record).collect()
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(OsStr::new(suffix));
    PathBuf::from(value)
}

fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_read = left.read(&mut left_buffer)?;
        let right_read = right.read(&mut right_buffer)?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn same_file(left: &Path, right: &Path) -> Result<bool> {
    let left = fs::metadata(left)?;
    let right = fs::metadata(right)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

fn metadata_equal(left: &Path, right: &Path) -> Result<bool> {
    let left = fs::metadata(left)?;
    let right = fs::metadata(right)?;
    Ok(left.uid() == right.uid() && left.gid() == right.gid() && left.mode() == right.mode())
}

fn preserve_metadata(source: &Path, destination: &Path) -> Result<()> {
    let source = fs::metadata(source)?;
    let destination_metadata = fs::metadata(destination)?;
    if destination_metadata.uid() != source.uid() || destination_metadata.gid() != source.gid() {
        rustix::fs::chown(
            destination,
            Some(rustix::fs::Uid::from_raw(source.uid())),
            Some(rustix::fs::Gid::from_raw(source.gid())),
        )
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
    }
    fs::set_permissions(destination, source.permissions())?;
    Ok(())
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn remove_tool_scratch(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            sync_parent(path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn migration_error(message: impl Into<String>) -> Error {
    Error::Migration(message.into())
}
