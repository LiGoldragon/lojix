//! Read-only v2 reconstruction and crash-safe in-place migration.
//!
//! The canonical pre-v3 store is never opened writable. Migration first
//! validates every known row and cross-table relation, then creates a
//! byte-identical permanent backup. A fresh schema-three store is constructed at
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

use crate::legacy_v2 as legacy;
use crate::schema::sema::{
    ActivationEffect as V3ActivationEffect, CacheRetentionTransition as V3CacheRetentionTransition,
    CacheRetentionTransitionEvent as V3CacheRetentionTransitionEvent, ClosurePath as V3ClosurePath,
    ClusterName as V3ClusterName, CommitSequence,
    ContainerLifecycleRecord as V3ContainerLifecycleRecord, ContainerName as V3ContainerName,
    ContainerState as V3ContainerState, DeployJob as V3DeployJob,
    DeployJobPhase as V3DeployJobPhase, DeploymentEnvironment,
    DeploymentIdentifier as V3DeploymentIdentifier, DeploymentLifecycle,
    DeploymentPhase as V3DeploymentPhase, DeploymentPhaseEvent as V3DeploymentPhaseEvent,
    DeploymentRecord, DeploymentRequestIdentity, EventLogEntry as V3EventLogEntry,
    EventLogPosition as V3EventLogPosition, FlakeReference as V3FlakeReference, GcRoot as V3GcRoot,
    GenerationArtifact as V3GenerationArtifact, GenerationIdentifier as V3GenerationIdentifier,
    GenerationSlot as V3GenerationSlot, ImmutableRevision, LegacyDeploymentEventQuarantine,
    LegacyEventArchive, LiveGeneration as V3LiveGeneration, LoggedEvent as V3LoggedEvent,
    NodeName as V3NodeName, PinLabel as V3PinLabel, RequestedDeploymentAction,
    SourceRevisionPolicy as V3SourceRevisionPolicy, SourceRevisionRecord as V3SourceRevisionRecord,
    StateDigest, StateMarker, StoredTestRun as V3StoredTestRun, TestMode as V3TestMode,
    TestOutcome as V3TestOutcome, TestRunIdentifier as V3TestRunIdentifier,
    TestRunPhase as V3TestRunPhase,
};
use crate::{Error, Result, Store};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const SCHEMA_TWO: u64 = 2;
const SCHEMA_THREE: u64 = 3;
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
    pub deployment_records: usize,
    pub quarantined_legacy_deployment_events: usize,
    pub legacy_non_resumable_deploy_jobs: usize,
}

impl std::fmt::Display for StoreCounts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "generations={} roots={} events={} containers={} deploy_jobs={} test_runs={} deployment_records={} quarantined_legacy_deployment_events={} legacy_non_resumable_deploy_jobs={}",
            self.generations,
            self.gc_roots,
            self.event_log_entries,
            self.container_lifecycle_records,
            self.deploy_jobs,
            self.test_runs,
            self.deployment_records,
            self.quarantined_legacy_deployment_events,
            self.legacy_non_resumable_deploy_jobs,
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
            backup: with_suffix(&canonical, ".schema-pre-v3.backup"),
            staging: with_suffix(&canonical, ".schema-v3.pending"),
            staging_owner: with_suffix(&canonical, ".schema-v3.pending.owner"),
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
                "(StoreMigrationNotNeeded {} schema=3 {counts})",
                path.display()
            ),
            Self::Migrated {
                path,
                backup,
                counts,
            } => write!(
                formatter,
                "(StoreMigrated {} backup={} schema=v2->3 {counts})",
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

/// Idempotent pre-v3 to schema-three store migrator.
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
        if !path_entry_exists(&self.paths.canonical)? {
            if path_entry_exists(&self.paths.backup)?
                || path_entry_exists(&self.paths.staging)?
                || path_entry_exists(&self.paths.staging_owner)?
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
            SCHEMA_THREE => {
                let counts = Store::open(&self.paths.canonical)?.migration_counts()?;
                self.reconcile_current_sidecars()?;
                Ok(MigrationOutcome::AlreadyCurrent {
                    path: self.paths.canonical.clone(),
                    counts,
                })
            }
            SCHEMA_TWO => self.migrate_legacy(),
            1 => Err(migration_error(
                "schema-one stores are intentionally refused: the v3 migrator only has a frozen v2 decoder",
            )),
            version => Err(migration_error(format!(
                "canonical store {} has schema {version}, expected schema two or three",
                self.paths.canonical.display()
            ))),
        }
    }

    fn migrate_legacy(&self) -> Result<MigrationOutcome> {
        let source_version = schema_version(&self.paths.canonical)?;
        let snapshot = LegacyV2Snapshot::read_version(&self.paths.canonical, source_version)?;
        let counts = snapshot.counts();

        self.ensure_backup(&snapshot, source_version)?;
        self.prepare_staging()?;
        snapshot.reconstruct(&self.paths.staging)?;
        preserve_metadata(&self.paths.backup, &self.paths.staging)?;
        sync_file(&self.paths.staging)?;

        {
            let reopened = Store::open(&self.paths.staging)?;
            let actual = reopened.migration_counts()?;
            if actual != counts {
                return Err(migration_error(format!(
                    "reopened schema-three staging counts differ: expected {counts}, found {actual}"
                )));
            }
        }

        if !files_equal(&self.paths.canonical, &self.paths.backup)? {
            return Err(migration_error(
                "canonical pre-v3 store changed after backup; refusing replacement",
            ));
        }

        fs::rename(&self.paths.staging, &self.paths.canonical)?;
        sync_parent(&self.paths.canonical)?;

        let reopened = Store::open(&self.paths.canonical)?;
        let actual = reopened.migration_counts()?;
        if actual != counts {
            return Err(migration_error(format!(
                "canonical schema-three counts differ after replacement: expected {counts}, found {actual}"
            )));
        }
        remove_tool_scratch(&self.paths.staging_owner)?;

        Ok(MigrationOutcome::Migrated {
            path: self.paths.canonical.clone(),
            backup: self.paths.backup.clone(),
            counts,
        })
    }

    /// A schema-three canonical store normally retains only the permanent
    /// schema-two backup. The sole recoverable transient state is the narrow
    /// crash window after the atomic replacement and before the ownership
    /// marker is removed: no staging file, plus a regular schema-two owner
    /// hard-link that is exactly the permanent backup. Everything else is
    /// evidence we cannot attribute safely, so it remains untouched.
    fn reconcile_current_sidecars(&self) -> Result<()> {
        if path_entry_exists(&self.paths.staging)? {
            return Err(migration_error(format!(
                "schema-three canonical store {} has unresolved staging residue {}; refusing to guess its owner",
                self.paths.canonical.display(),
                self.paths.staging.display()
            )));
        }
        if !path_entry_exists(&self.paths.staging_owner)? {
            return Ok(());
        }
        if !path_entry_exists(&self.paths.backup)? {
            return Err(migration_error(format!(
                "schema-three canonical store {} has an unpaired staging ownership marker {} without its schema-two backup",
                self.paths.canonical.display(),
                self.paths.staging_owner.display()
            )));
        }
        if !regular_file_entry(&self.paths.backup)?
            || !regular_file_entry(&self.paths.staging_owner)?
        {
            return Err(migration_error(format!(
                "schema-three canonical store {} has a non-regular migration backup or ownership marker",
                self.paths.canonical.display()
            )));
        }
        if schema_version(&self.paths.backup)? != SCHEMA_TWO {
            return Err(migration_error(format!(
                "schema-three canonical store {} has an ownership marker whose backup is not schema two",
                self.paths.canonical.display()
            )));
        }
        if !same_file(&self.paths.backup, &self.paths.staging_owner)? {
            return Err(migration_error(format!(
                "schema-three canonical store {} has an ownership marker that does not share the schema-two backup inode",
                self.paths.canonical.display()
            )));
        }
        if !files_equal(&self.paths.backup, &self.paths.staging_owner)?
            || !metadata_equal(&self.paths.backup, &self.paths.staging_owner)?
        {
            return Err(migration_error(format!(
                "schema-three canonical store {} has an ownership marker that does not preserve the schema-two backup bytes and metadata",
                self.paths.canonical.display()
            )));
        }
        if same_file(&self.paths.canonical, &self.paths.staging_owner)? {
            return Err(migration_error(format!(
                "schema-three canonical store {} unexpectedly shares its inode with the schema-two ownership marker",
                self.paths.canonical.display()
            )));
        }

        remove_tool_scratch(&self.paths.staging_owner)
    }

    fn ensure_backup(&self, snapshot: &LegacyV2Snapshot, source_version: u64) -> Result<()> {
        if path_entry_exists(&self.paths.backup)? {
            if !files_equal(&self.paths.canonical, &self.paths.backup)? {
                return Err(migration_error(format!(
                    "existing backup {} is not byte-identical to canonical schema-two store",
                    self.paths.backup.display()
                )));
            }
            if !metadata_equal(&self.paths.canonical, &self.paths.backup)? {
                return Err(migration_error(format!(
                    "existing backup {} does not preserve canonical ownership and mode",
                    self.paths.backup.display()
                )));
            }
            let backup = LegacyV2Snapshot::read_version(&self.paths.backup, source_version)?;
            if backup.counts() != snapshot.counts() {
                return Err(migration_error(
                    "existing pre-v3 backup counts differ from canonical store",
                ));
            }
            return Ok(());
        }

        fs::hard_link(&self.paths.canonical, &self.paths.backup).map_err(|error| {
            migration_error(format!(
                "cannot create non-overwriting pre-v3 backup {}: {error}",
                self.paths.backup.display()
            ))
        })?;
        sync_parent(&self.paths.backup)?;
        if !same_file(&self.paths.canonical, &self.paths.backup)?
            || !files_equal(&self.paths.canonical, &self.paths.backup)?
        {
            return Err(migration_error(
                "new pre-v3 backup does not preserve the canonical inode and bytes",
            ));
        }
        Ok(())
    }

    fn prepare_staging(&self) -> Result<()> {
        if path_entry_exists(&self.paths.staging_owner)? {
            if !same_file(&self.paths.backup, &self.paths.staging_owner)? {
                return Err(migration_error(format!(
                    "staging ownership marker {} conflicts with the schema-two backup",
                    self.paths.staging_owner.display()
                )));
            }
            remove_tool_scratch(&self.paths.staging)?;
            return Ok(());
        }
        if path_entry_exists(&self.paths.staging)? {
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

struct LegacyV2Snapshot {
    generations: Vec<legacy::LiveGeneration>,
    roots: Vec<legacy::GcRoot>,
    events: Vec<legacy::EventLogEntry>,
    containers: Vec<legacy::ContainerLifecycleRecord>,
    deploy_jobs: Vec<legacy::DeployJob>,
    test_runs: Vec<legacy::StoredTestRun>,
}

impl LegacyV2Snapshot {
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

        let generations = rows::<legacy::LiveGeneration>(&database, LIVE_SET_TABLE)?;
        let roots = rows::<legacy::GcRoot>(&database, GC_ROOTS_TABLE)?;
        let mut events = rows::<legacy::EventLogEntry>(&database, EVENT_LOG_TABLE)?;
        let containers =
            rows::<legacy::ContainerLifecycleRecord>(&database, CONTAINER_LIFECYCLE_TABLE)?;
        let deploy_jobs = rows::<legacy::DeployJob>(&database, DEPLOY_JOB_TABLE)?;
        let test_runs = rows::<legacy::StoredTestRun>(&database, TEST_RUN_TABLE)?;

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
            event_log_entries: self
                .events
                .iter()
                .filter(|event| !matches!(event.record, legacy::LoggedEvent::Deployment(_)))
                .count(),
            container_lifecycle_records: self.containers.len(),
            deploy_jobs: self.deploy_jobs.len(),
            test_runs: self.test_runs.len(),
            deployment_records: self
                .legacy_deployment_records()
                .expect("validated legacy deployment identities")
                .len(),
            quarantined_legacy_deployment_events: self
                .events
                .iter()
                .filter(|event| matches!(event.record, legacy::LoggedEvent::Deployment(_)))
                .count(),
            legacy_non_resumable_deploy_jobs: self.deploy_jobs.len(),
        }
    }

    /// v2 did not persist a deployment-correlation table. Reconstruct only the
    /// identity facts that its durable rows actually establish; ambiguous
    /// request action, environment, and terminal state remain explicitly
    /// legacy/unknown rather than being guessed from a live generation.
    fn legacy_deployment_records(&self) -> Result<Vec<DeploymentRecord>> {
        let mut current_per_unknown_environment = BTreeMap::<(String, String), usize>::new();
        for generation in &self.generations {
            if matches!(generation.generation_slot, legacy::GenerationSlot::Current) {
                *current_per_unknown_environment
                    .entry((
                        generation.cluster_name.payload().clone(),
                        generation.node_name.payload().clone(),
                    ))
                    .or_default() += 1;
            }
        }
        let mut records = BTreeMap::new();
        for generation in &self.generations {
            let identifier = *generation.deployment_identifier.payload();
            let conflict = matches!(generation.generation_slot, legacy::GenerationSlot::Current)
                && current_per_unknown_environment
                    .get(&(
                        generation.cluster_name.payload().clone(),
                        generation.node_name.payload().clone(),
                    ))
                    .copied()
                    .unwrap_or(0)
                    > 1;
            let record = DeploymentRecord {
                deployment_identifier: deployment_identifier(&generation.deployment_identifier),
                generation_identifier: generation_identifier(&generation.generation_identifier),
                deployment_request_identity: DeploymentRequestIdentity {
                    deployment_environment: DeploymentEnvironment::LegacyUnknownEnvironment,
                    cluster_name: cluster_name(&generation.cluster_name),
                    node_name: node_name(&generation.node_name),
                    generation_artifact: generation_artifact(generation.generation_artifact),
                    requested_deployment_action: RequestedDeploymentAction::LegacyUnknownAction,
                    activation_effect: activation_effect(generation.activation_effect),
                    source_revision_policy: source_revision_policy(
                        generation.source_revision_record.source_revision_policy,
                    ),
                    optional_immutable_revision: nonempty_immutable_revision(
                        &generation.source_revision_record.string,
                    ),
                },
                optional_admission_marker: None,
                deployment_lifecycle: if conflict {
                    DeploymentLifecycle::LegacyAmbiguous
                } else {
                    DeploymentLifecycle::LegacyUnknown
                },
                optional_terminal_marker: None,
                optional_deployment_terminal: None,
            };
            if records.insert(identifier, record).is_some() {
                return Err(migration_error(format!(
                    "legacy deployment identifier {identifier} is shared by multiple generations"
                )));
            }
        }
        for job in &self.deploy_jobs {
            let identifier = *job.deployment_identifier.payload();
            if records.contains_key(&identifier) {
                continue;
            }
            records.insert(
                identifier,
                DeploymentRecord {
                    deployment_identifier: deployment_identifier(&job.deployment_identifier),
                    generation_identifier: generation_identifier(&job.generation_identifier),
                    deployment_request_identity: DeploymentRequestIdentity {
                        deployment_environment: DeploymentEnvironment::LegacyUnknownEnvironment,
                        cluster_name: cluster_name(&job.cluster_name),
                        node_name: node_name(&job.node_name),
                        generation_artifact: V3GenerationArtifact::LegacyUnknown,
                        requested_deployment_action: RequestedDeploymentAction::LegacyUnknownAction,
                        activation_effect: V3ActivationEffect::LegacyUnknown,
                        source_revision_policy: source_revision_policy(job.source_revision_policy),
                        optional_immutable_revision: job
                            .resolved_revision
                            .as_deref()
                            .and_then(nonempty_immutable_revision),
                    },
                    optional_admission_marker: None,
                    deployment_lifecycle: DeploymentLifecycle::LegacyUnknown,
                    optional_terminal_marker: None,
                    optional_deployment_terminal: None,
                },
            );
        }
        Ok(records.into_values().collect())
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
                legacy::LoggedEvent::Deployment(record) => *record.event_log_position.payload(),
                legacy::LoggedEvent::CacheRetention(record) => *record.event_log_position.payload(),
                legacy::LoggedEvent::Container(record) => *record.event_log_position.payload(),
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
                    && matches!(&event.record, legacy::LoggedEvent::Container(record) if record == container)
            });
            if !matches_event {
                return Err(migration_error(format!(
                    "container transition at event position {position} has no matching event"
                )));
            }
        }
        for event in &self.events {
            let legacy::LoggedEvent::Container(record) = &event.record else {
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
        self.legacy_deployment_records()?;
        Ok(())
    }

    fn reconstruct(self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(migration_error(format!(
                "schema-three staging path already exists: {}",
                destination.display()
            )));
        }
        let store = Store::open(destination)?;
        let deployment_records = self.legacy_deployment_records()?;
        // Legacy deployment events never had v3 correlation markers, so they
        // are retained privately for audit rather than fabricated into the
        // public journal. The receipt identifies the one migration commit that
        // performed that quarantine, not an invented historic transition.
        let migration_commit = store
            .commit_sequence()?
            .checked_add(1)
            .ok_or_else(|| migration_error("commit sequence exhausted during migration"))?;
        let migration_marker = StateMarker {
            commit_sequence: CommitSequence::new(migration_commit),
            state_digest: StateDigest::new(migration_commit),
        };
        let quarantines = self
            .events
            .iter()
            .filter(|event| matches!(event.record, legacy::LoggedEvent::Deployment(_)))
            .map(|event| {
                Ok(LegacyDeploymentEventQuarantine {
                    event_log_position: event_log_position(&event.event_log_position),
                    legacy_event_archive: legacy_event_archive(event)?,
                    state_marker: migration_marker.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let seed_commit = store.seed_migration(
            self.generations
                .into_iter()
                .map(v3_live_generation)
                .collect(),
            self.roots.into_iter().map(v3_gc_root).collect(),
            self.events
                .iter()
                .filter_map(|event| match &event.record {
                    legacy::LoggedEvent::Deployment(_) => None,
                    _ => Some(v3_event_log_entry(event.clone(), migration_marker.clone())),
                })
                .collect(),
            self.containers
                .into_iter()
                .map(v3_container_lifecycle_record)
                .collect(),
            self.deploy_jobs.into_iter().map(v3_deploy_job).collect(),
            self.test_runs.into_iter().map(v3_test_run).collect(),
            deployment_records,
            quarantines,
        )?;
        if seed_commit != migration_commit {
            return Err(migration_error(format!(
                "schema-three migration commit {seed_commit} differed from the private quarantine receipt {migration_commit}"
            )));
        }
        Ok(())
    }
}

fn deployment_identifier(value: &legacy::DeploymentIdentifier) -> V3DeploymentIdentifier {
    V3DeploymentIdentifier::new(*value.payload())
}

fn generation_identifier(value: &legacy::GenerationIdentifier) -> V3GenerationIdentifier {
    V3GenerationIdentifier::new(*value.payload())
}

fn test_run_identifier(value: &legacy::TestRunIdentifier) -> V3TestRunIdentifier {
    V3TestRunIdentifier::new(*value.payload())
}

fn event_log_position(value: &legacy::EventLogPosition) -> V3EventLogPosition {
    V3EventLogPosition::new(*value.payload())
}

fn cluster_name(value: &legacy::ClusterName) -> V3ClusterName {
    V3ClusterName::new(value.payload().clone())
}

fn node_name(value: &legacy::NodeName) -> V3NodeName {
    V3NodeName::new(value.payload().clone())
}

fn closure_path(value: &legacy::ClosurePath) -> V3ClosurePath {
    V3ClosurePath::new(value.payload().clone())
}

fn flake_reference(value: &legacy::FlakeReference) -> V3FlakeReference {
    V3FlakeReference::new(value.payload().clone())
}

fn source_revision_policy(value: legacy::SourceRevisionPolicy) -> V3SourceRevisionPolicy {
    match value {
        legacy::SourceRevisionPolicy::RequireImmutable => V3SourceRevisionPolicy::RequireImmutable,
        legacy::SourceRevisionPolicy::ResolveAndRecord => V3SourceRevisionPolicy::ResolveAndRecord,
    }
}

fn generation_artifact(value: legacy::GenerationArtifact) -> V3GenerationArtifact {
    match value {
        legacy::GenerationArtifact::CompleteHost => V3GenerationArtifact::CompleteHost,
        legacy::GenerationArtifact::BaseHost => V3GenerationArtifact::BaseHost,
        legacy::GenerationArtifact::UserEnvironment => V3GenerationArtifact::UserEnvironment,
    }
}

fn activation_effect(value: legacy::ActivationEffect) -> V3ActivationEffect {
    match value {
        legacy::ActivationEffect::LiveActivation => V3ActivationEffect::LiveActivation,
        legacy::ActivationEffect::BootProfile => V3ActivationEffect::BootProfile,
        legacy::ActivationEffect::TestActivation => V3ActivationEffect::TestActivation,
        legacy::ActivationEffect::BootOnceProfile => V3ActivationEffect::BootOnceProfile,
        legacy::ActivationEffect::ProfileOnly => V3ActivationEffect::ProfileOnly,
    }
}

fn generation_slot(value: legacy::GenerationSlot) -> V3GenerationSlot {
    match value {
        // v2 did not carry enough environment/correlation facts to prove a
        // v3 `Current`; even a singleton is historical observation only.
        legacy::GenerationSlot::Current => V3GenerationSlot::LegacyUnknown,
        legacy::GenerationSlot::BootPending => V3GenerationSlot::BootPending,
        legacy::GenerationSlot::Rollback => V3GenerationSlot::Rollback,
        legacy::GenerationSlot::Pinned => V3GenerationSlot::Pinned,
        legacy::GenerationSlot::Recent => V3GenerationSlot::Recent,
    }
}

fn source_revision_record(value: legacy::SourceRevisionRecord) -> V3SourceRevisionRecord {
    V3SourceRevisionRecord {
        source_revision_policy: source_revision_policy(value.source_revision_policy),
        requested_ref: flake_reference(&value.requested_ref),
        resolved_ref: flake_reference(&value.resolved_ref),
        string: value.string,
    }
}

fn v3_live_generation(value: legacy::LiveGeneration) -> V3LiveGeneration {
    V3LiveGeneration {
        deployment_identifier: deployment_identifier(&value.deployment_identifier),
        generation_identifier: generation_identifier(&value.generation_identifier),
        cluster_name: cluster_name(&value.cluster_name),
        node_name: node_name(&value.node_name),
        deployment_environment: DeploymentEnvironment::LegacyUnknownEnvironment,
        generation_artifact: generation_artifact(value.generation_artifact),
        activation_effect: activation_effect(value.activation_effect),
        generation_slot: generation_slot(value.generation_slot),
        closure_path: closure_path(&value.closure_path),
        source_revision_record: source_revision_record(value.source_revision_record),
    }
}

fn v3_gc_root(value: legacy::GcRoot) -> V3GcRoot {
    V3GcRoot {
        generation_identifier: generation_identifier(&value.generation_identifier),
        cluster_name: cluster_name(&value.cluster_name),
        node_name: node_name(&value.node_name),
        generation_slot: generation_slot(value.generation_slot),
        closure_path: closure_path(&value.closure_path),
        optional_pin_label: value
            .label
            .map(|label| V3PinLabel::new(label.payload().clone())),
    }
}

fn deployment_phase(value: legacy::DeploymentPhase) -> V3DeploymentPhase {
    match value {
        legacy::DeploymentPhase::Submitted => V3DeploymentPhase::Submitted,
        legacy::DeploymentPhase::Building => V3DeploymentPhase::Building,
        legacy::DeploymentPhase::Built => V3DeploymentPhase::Built,
        legacy::DeploymentPhase::Copying => V3DeploymentPhase::Copying,
        legacy::DeploymentPhase::Activating => V3DeploymentPhase::Activating,
        legacy::DeploymentPhase::Activated => V3DeploymentPhase::Activated,
        legacy::DeploymentPhase::Failed => V3DeploymentPhase::Failed,
    }
}

fn cache_retention_transition(
    value: legacy::CacheRetentionTransition,
) -> V3CacheRetentionTransition {
    match value {
        legacy::CacheRetentionTransition::Pinned => V3CacheRetentionTransition::Pinned,
        legacy::CacheRetentionTransition::Unpinned => V3CacheRetentionTransition::Unpinned,
        legacy::CacheRetentionTransition::Promoted => V3CacheRetentionTransition::Promoted,
        legacy::CacheRetentionTransition::Demoted => V3CacheRetentionTransition::Demoted,
        legacy::CacheRetentionTransition::Retired => V3CacheRetentionTransition::Retired,
        legacy::CacheRetentionTransition::Evicted => V3CacheRetentionTransition::Evicted,
    }
}

fn container_state(value: legacy::ContainerState) -> V3ContainerState {
    match value {
        legacy::ContainerState::Starting => V3ContainerState::Starting,
        legacy::ContainerState::Started => V3ContainerState::Started,
        legacy::ContainerState::Stopping => V3ContainerState::Stopping,
        legacy::ContainerState::Stopped => V3ContainerState::Stopped,
        legacy::ContainerState::Failed => V3ContainerState::Failed,
    }
}

fn v3_container_lifecycle_record(
    value: legacy::ContainerLifecycleRecord,
) -> V3ContainerLifecycleRecord {
    V3ContainerLifecycleRecord {
        cluster_name: cluster_name(&value.cluster_name),
        node_name: node_name(&value.node_name),
        container_name: V3ContainerName::new(value.container.payload().clone()),
        container_state: container_state(value.state),
        event_log_position: event_log_position(&value.event_log_position),
    }
}

fn v3_logged_event(value: legacy::LoggedEvent, marker: StateMarker) -> V3LoggedEvent {
    match value {
        legacy::LoggedEvent::Deployment(value) => {
            V3LoggedEvent::Deployment(V3DeploymentPhaseEvent {
                deployment_identifier: deployment_identifier(&value.deployment_identifier),
                generation_identifier: generation_identifier(&value.generation_identifier),
                cluster_name: cluster_name(&value.cluster_name),
                node_name: node_name(&value.node_name),
                deployment_phase: deployment_phase(value.deployment_phase),
                event_log_position: event_log_position(&value.event_log_position),
                state_marker: marker,
                optional_immutable_revision: value
                    .optional_source_revision_record
                    .as_ref()
                    .and_then(|record| nonempty_immutable_revision(&record.string)),
                optional_deployment_terminal: None,
            })
        }
        legacy::LoggedEvent::CacheRetention(value) => {
            V3LoggedEvent::CacheRetention(V3CacheRetentionTransitionEvent {
                generation_identifier: generation_identifier(&value.generation_identifier),
                cluster_name: cluster_name(&value.cluster_name),
                node_name: node_name(&value.node_name),
                cache_retention_transition: cache_retention_transition(
                    value.cache_retention_transition,
                ),
                generation_slot: generation_slot(value.generation_slot),
                optional_generation_slot: value.optional_generation_slot.map(generation_slot),
                optional_pin_label: value
                    .optional_pin_label
                    .map(|label| V3PinLabel::new(label.payload().clone())),
                event_log_position: event_log_position(&value.event_log_position),
            })
        }
        legacy::LoggedEvent::Container(value) => {
            V3LoggedEvent::Container(v3_container_lifecycle_record(value))
        }
    }
}

fn v3_event_log_entry(value: legacy::EventLogEntry, marker: StateMarker) -> V3EventLogEntry {
    let position = event_log_position(&value.event_log_position);
    V3EventLogEntry {
        event_log_position: position,
        logged_event: v3_logged_event(value.record, marker),
    }
}

fn v3_deploy_job(value: legacy::DeployJob) -> V3DeployJob {
    V3DeployJob {
        deployment_identifier: deployment_identifier(&value.deployment_identifier),
        generation_identifier: generation_identifier(&value.generation_identifier),
        cluster_name: cluster_name(&value.cluster_name),
        node_name: node_name(&value.node_name),
        // v2 lacks the original private submission and cannot be replayed
        // safely. Preserve the cursor for diagnostics, never fabricate a
        // terminal or re-drive a guessed request.
        deploy_job_phase: V3DeployJobPhase::LegacyNonResumable,
        optional_closure_path: value.closure_path.as_ref().map(closure_path),
        source_revision_policy: source_revision_policy(value.source_revision_policy),
        flake_reference: flake_reference(&value.requested_ref),
        optional_flake_reference: value.resolved_ref.as_ref().map(flake_reference),
        resolved_revision: value.resolved_revision,
        resolved_target: value.resolved_target,
        boot_once_unit: value.boot_once_unit,
        optional_generation_slot: None,
        persisted_flake_input_override_vector: Vec::new(),
        deploy_resume_stage: crate::schema::sema::DeployResumeStage::ResolveFlakeAuth,
        optional_phase_receipt: None,
        optional_deploy_submission: None,
    }
}

fn test_mode(value: legacy::TestMode) -> V3TestMode {
    match value {
        legacy::TestMode::Hermetic => V3TestMode::Hermetic,
        legacy::TestMode::Live => V3TestMode::Live,
    }
}

fn test_run_phase(value: legacy::TestRunPhase) -> V3TestRunPhase {
    match value {
        legacy::TestRunPhase::Submitted => V3TestRunPhase::Submitted,
        legacy::TestRunPhase::BringingUp => V3TestRunPhase::BringingUp,
        legacy::TestRunPhase::Deploying => V3TestRunPhase::Deploying,
        legacy::TestRunPhase::Asserting => V3TestRunPhase::Asserting,
        legacy::TestRunPhase::TearingDown => V3TestRunPhase::TearingDown,
        legacy::TestRunPhase::Completed => V3TestRunPhase::Completed,
        legacy::TestRunPhase::Failed => V3TestRunPhase::Failed,
    }
}

fn test_outcome(value: legacy::TestOutcome) -> V3TestOutcome {
    match value {
        legacy::TestOutcome::Pending => V3TestOutcome::Pending,
        legacy::TestOutcome::Passed => V3TestOutcome::Passed,
        legacy::TestOutcome::Failed(stage) => V3TestOutcome::Failed(match stage {
            legacy::FailureStage::BringUp => crate::schema::sema::FailureStage::BringUp,
            legacy::FailureStage::Deploy => crate::schema::sema::FailureStage::Deploy,
            legacy::FailureStage::Assert => crate::schema::sema::FailureStage::Assert,
            legacy::FailureStage::TearDown => crate::schema::sema::FailureStage::TearDown,
            legacy::FailureStage::HermeticCheck => crate::schema::sema::FailureStage::HermeticCheck,
        }),
    }
}

fn v3_test_run(value: legacy::StoredTestRun) -> V3StoredTestRun {
    V3StoredTestRun {
        test_run_identifier: test_run_identifier(&value.test_run_identifier),
        cluster_name: cluster_name(&value.cluster_name),
        node: node_name(&value.node_name),
        host: node_name(&value.host),
        test_mode: test_mode(value.mode),
        test_run_phase: test_run_phase(value.phase),
        test_outcome: test_outcome(value.outcome),
        optional_closure_path: value.closure_path.as_ref().map(closure_path),
    }
}

fn nonempty_immutable_revision(value: &str) -> Option<ImmutableRevision> {
    crate::immutable_revision(value)
}

/// Exact rkyv serialization of the decoded legacy event, hex encoded only
/// because the local schema's archival atom is a String. This is private
/// quarantine material, never Dotos or a public event payload.
fn legacy_event_archive(value: &legacy::EventLogEntry) -> Result<LegacyEventArchive> {
    let bytes = rkyv::to_bytes::<rancor::Error>(value)
        .map_err(|error| migration_error(format!("legacy event re-archive failed: {error}")))?;
    let mut archive = String::with_capacity(bytes.len() * 2);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut archive, "{byte:02x}").expect("write String cannot fail");
    }
    Ok(LegacyEventArchive::new(archive))
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

/// `Path::exists` treats a dangling symlink as absent. Migration sidecars are
/// evidence, so a directory entry must be noticed even when it is malformed.
fn path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn regular_file_entry(path: &Path) -> Result<bool> {
    Ok(fs::symlink_metadata(path)?.file_type().is_file())
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

#[cfg(test)]
mod tests {
    use super::*;
    use sema_engine::{
        Engine as SemaDatabase, EngineOpen, FamilyName, SchemaHash, SchemaVersion, TableDescriptor,
    };
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn legacy_immutable_revision_projection_requires_a_full_commit() {
        assert_eq!(
            nonempty_immutable_revision("sha256-unproven-nar-hash"),
            None
        );
        assert_eq!(
            nonempty_immutable_revision("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(ImmutableRevision::new(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        );
    }

    #[allow(clippy::type_complexity)]
    fn register_legacy_tables(
        database: &mut SemaDatabase,
    ) -> (
        sema_engine::TableReference<legacy::LiveGeneration>,
        sema_engine::TableReference<legacy::GcRoot>,
        sema_engine::TableReference<legacy::EventLogEntry>,
        sema_engine::TableReference<legacy::ContainerLifecycleRecord>,
        sema_engine::TableReference<legacy::DeployJob>,
        sema_engine::TableReference<legacy::StoredTestRun>,
    ) {
        let live_set = database
            .register_table(TableDescriptor::new(
                crate::LIVE_SET_TABLE,
                FamilyName::new(crate::LIVE_SET_FAMILY),
                SchemaHash::new(crate::LIVE_SET_SCHEMA_HASH),
            ))
            .expect("register legacy live set");
        let roots = database
            .register_table(TableDescriptor::new(
                crate::GC_ROOTS_TABLE,
                FamilyName::new(crate::GC_ROOTS_FAMILY),
                SchemaHash::new(crate::GC_ROOTS_SCHEMA_HASH),
            ))
            .expect("register legacy roots");
        let events = database
            .register_table(TableDescriptor::new(
                crate::EVENT_LOG_TABLE,
                FamilyName::new(crate::EVENT_LOG_FAMILY),
                SchemaHash::new(crate::EVENT_LOG_SCHEMA_HASH),
            ))
            .expect("register legacy events");
        let containers = database
            .register_table(TableDescriptor::new(
                crate::CONTAINER_LIFECYCLE_TABLE,
                FamilyName::new(crate::CONTAINER_LIFECYCLE_FAMILY),
                SchemaHash::new(crate::CONTAINER_LIFECYCLE_SCHEMA_HASH),
            ))
            .expect("register legacy containers");
        let jobs = database
            .register_table(TableDescriptor::new(
                crate::DEPLOY_JOB_TABLE,
                FamilyName::new(crate::DEPLOY_JOB_FAMILY),
                SchemaHash::new(crate::DEPLOY_JOB_SCHEMA_HASH),
            ))
            .expect("register legacy jobs");
        let test_runs = database
            .register_table(TableDescriptor::new(
                crate::TEST_RUN_TABLE,
                FamilyName::new(crate::TEST_RUN_FAMILY),
                SchemaHash::new(crate::TEST_RUN_SCHEMA_HASH),
            ))
            .expect("register legacy test runs");
        (live_set, roots, events, containers, jobs, test_runs)
    }

    fn seed_minimal_v2_store(path: &Path) {
        let mut database = SemaDatabase::open(EngineOpen::new(path, SchemaVersion::new(2)))
            .expect("open v2 store");
        let (live_set, roots, _events, _containers, _jobs, _test_runs) =
            register_legacy_tables(&mut database);
        let source_revision = legacy::SourceRevisionRecord {
            source_revision_policy: legacy::SourceRevisionPolicy::RequireImmutable,
            requested_ref: legacy::FlakeReference("github:example/fixture".to_string()),
            resolved_ref: legacy::FlakeReference(
                "github:example/fixture?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            string: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        };
        let generation = legacy::LiveGeneration {
            deployment_identifier: legacy::DeploymentIdentifier(1),
            generation_identifier: legacy::GenerationIdentifier(1),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_artifact: legacy::GenerationArtifact::BaseHost,
            activation_effect: legacy::ActivationEffect::BootProfile,
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fixture".to_string(),
            ),
            source_revision_record: source_revision,
        };
        let root = legacy::GcRoot {
            generation_identifier: legacy::GenerationIdentifier(1),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fixture".to_string(),
            ),
            label: None,
        };
        database
            .commit_atomic(
                database
                    .begin_atomic_commit()
                    .assert(live_set, generation)
                    .assert(roots, root),
            )
            .expect("seed minimal v2 rows");
    }

    fn migrate_minimal_v2_store(path: &Path) -> (StoreMigrator, StoreCounts) {
        seed_minimal_v2_store(path);
        let migrator = StoreMigrator::new(path);
        let outcome = migrator.migrate().expect("migrate v2 store");
        let MigrationOutcome::Migrated { counts, .. } = outcome else {
            panic!("expected migration outcome");
        };
        (migrator, counts)
    }

    fn metadata_fingerprint(path: &Path) -> (u32, u32, u32) {
        let metadata = fs::metadata(path).expect("read metadata");
        (metadata.uid(), metadata.gid(), metadata.mode())
    }

    #[test]
    fn schema_three_retry_reconciles_only_verified_post_replacement_owner_residue() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lojix.sema");
        seed_minimal_v2_store(&path);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640))
            .expect("set source permissions");
        let source_bytes = fs::read(&path).expect("read source bytes");
        let source_metadata = metadata_fingerprint(&path);

        let migrator = StoreMigrator::new(&path);
        let outcome = migrator.migrate().expect("migrate v2 store");
        let MigrationOutcome::Migrated { counts, .. } = outcome else {
            panic!("expected migration outcome");
        };
        let paths = migrator.paths().clone();
        assert_eq!(
            fs::read(paths.backup()).expect("read backup bytes"),
            source_bytes
        );
        assert_eq!(metadata_fingerprint(paths.backup()), source_metadata);
        assert_eq!(metadata_fingerprint(&path), source_metadata);

        // This is the exact durable state after `rename(staging, canonical)`
        // and its parent sync, but before removal of the hard-link owner.
        fs::hard_link(paths.backup(), paths.staging_owner())
            .expect("recreate post-replacement ownership marker");
        assert!(same_file(paths.backup(), paths.staging_owner()).expect("compare owner inode"));
        let backup_bytes = fs::read(paths.backup()).expect("read backup bytes");
        let canonical_metadata = metadata_fingerprint(&path);
        let backup_metadata = metadata_fingerprint(paths.backup());

        let retry = migrator
            .migrate()
            .expect("reconcile verified ownership residue");
        let MigrationOutcome::AlreadyCurrent {
            counts: retry_counts,
            ..
        } = retry
        else {
            panic!("expected schema-three inspection");
        };
        assert_eq!(retry_counts, counts);
        assert!(!path_entry_exists(paths.staging()).expect("inspect staging"));
        assert!(!path_entry_exists(paths.staging_owner()).expect("inspect owner"));
        assert_eq!(
            fs::read(paths.backup()).expect("read backup after retry"),
            backup_bytes
        );
        assert_eq!(metadata_fingerprint(&path), canonical_metadata);
        assert_eq!(metadata_fingerprint(paths.backup()), backup_metadata);
        assert_eq!(
            Store::open(&path)
                .expect("reopen canonical after retry")
                .migration_counts()
                .expect("count canonical rows after retry"),
            counts
        );

        let second_retry = migrator.migrate().expect("idempotent retry");
        let MigrationOutcome::AlreadyCurrent {
            counts: second_retry_counts,
            ..
        } = second_retry
        else {
            panic!("expected schema-three inspection");
        };
        assert_eq!(second_retry_counts, counts);
    }

    #[test]
    fn schema_three_retry_refuses_stale_unowned_staging() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lojix.sema");
        let migrator = StoreMigrator::new(&path);
        let store = Store::open(&path).expect("create schema-three store");
        drop(store);
        fs::write(migrator.paths().staging(), b"unowned stale staging")
            .expect("write stale staging");

        let error = migrator
            .migrate()
            .expect_err("stale staging must fail closed");
        assert!(error.to_string().contains("unresolved staging residue"));
        assert!(path_entry_exists(migrator.paths().staging()).expect("staging remains"));
    }

    #[test]
    fn schema_three_retry_refuses_stale_unpaired_owner() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lojix.sema");
        let migrator = StoreMigrator::new(&path);
        let store = Store::open(&path).expect("create schema-three store");
        drop(store);
        fs::write(migrator.paths().staging_owner(), b"unowned stale owner")
            .expect("write stale owner");

        let error = migrator
            .migrate()
            .expect_err("unpaired ownership marker must fail closed");
        assert!(
            error
                .to_string()
                .contains("unpaired staging ownership marker")
        );
        assert!(path_entry_exists(migrator.paths().staging_owner()).expect("owner remains"));
    }

    #[test]
    fn schema_three_retry_refuses_conflicting_backup_and_owner() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lojix.sema");
        let (migrator, _counts) = migrate_minimal_v2_store(&path);
        let paths = migrator.paths().clone();
        fs::copy(paths.backup(), paths.staging_owner()).expect("copy conflicting owner");
        assert!(!same_file(paths.backup(), paths.staging_owner()).expect("compare owner inode"));

        let error = migrator
            .migrate()
            .expect_err("copied owner must not be attributed to this migrator");
        assert!(
            error
                .to_string()
                .contains("does not share the schema-two backup inode")
        );
        assert!(path_entry_exists(paths.staging_owner()).expect("conflicting owner remains"));
    }

    #[test]
    fn v2_rows_decode_through_the_frozen_vocabulary_and_reconstruct_v3() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("lojix.sema");
        const CANONICAL_CLOSURE: &str =
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-legacy-generation";
        const UNSAFE_CLOSURE: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-private-secret";
        let mut database = SemaDatabase::open(EngineOpen::new(&path, SchemaVersion::new(2)))
            .expect("open v2 store");
        let (live_set, roots, events, containers, jobs, test_runs) =
            register_legacy_tables(&mut database);

        let source_revision = legacy::SourceRevisionRecord {
            source_revision_policy: legacy::SourceRevisionPolicy::RequireImmutable,
            requested_ref: legacy::FlakeReference("github:example/fixture".to_string()),
            resolved_ref: legacy::FlakeReference(
                "github:example/fixture?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            string: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        };
        let generation = legacy::LiveGeneration {
            deployment_identifier: legacy::DeploymentIdentifier(41),
            generation_identifier: legacy::GenerationIdentifier(42),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_artifact: legacy::GenerationArtifact::BaseHost,
            activation_effect: legacy::ActivationEffect::BootProfile,
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(CANONICAL_CLOSURE.to_string()),
            source_revision_record: source_revision.clone(),
        };
        let root = legacy::GcRoot {
            generation_identifier: legacy::GenerationIdentifier(42),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(CANONICAL_CLOSURE.to_string()),
            label: None,
        };
        // Both source rows claimed `Current` in v2, but v2 did not establish
        // the v3 environment/correlation ownership required to preserve that
        // claim. The migration must demote both, never choose one arbitrarily.
        let unsafe_generation = legacy::LiveGeneration {
            deployment_identifier: legacy::DeploymentIdentifier(43),
            generation_identifier: legacy::GenerationIdentifier(44),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_artifact: legacy::GenerationArtifact::BaseHost,
            activation_effect: legacy::ActivationEffect::BootProfile,
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(UNSAFE_CLOSURE.to_string()),
            source_revision_record: source_revision.clone(),
        };
        let unsafe_root = legacy::GcRoot {
            generation_identifier: legacy::GenerationIdentifier(44),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            generation_slot: legacy::GenerationSlot::Current,
            closure_path: legacy::ClosurePath(UNSAFE_CLOSURE.to_string()),
            label: None,
        };
        let deployment_event = legacy::EventLogEntry {
            event_log_position: legacy::EventLogPosition(17),
            record: legacy::LoggedEvent::Deployment(legacy::DeploymentPhaseEvent {
                deployment_identifier: legacy::DeploymentIdentifier(41),
                generation_identifier: legacy::GenerationIdentifier(42),
                cluster_name: legacy::ClusterName("alpha".to_string()),
                node_name: legacy::NodeName("node-1".to_string()),
                deployment_phase: legacy::DeploymentPhase::Activated,
                event_log_position: legacy::EventLogPosition(17),
                optional_phase_detail: None,
                optional_source_revision_record: Some(source_revision),
            }),
        };
        let container = legacy::ContainerLifecycleRecord {
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            container: legacy::ContainerName("migration-witness".to_string()),
            state: legacy::ContainerState::Started,
            event_log_position: legacy::EventLogPosition(18),
        };
        let container_event = legacy::EventLogEntry {
            event_log_position: legacy::EventLogPosition(18),
            record: legacy::LoggedEvent::Container(container.clone()),
        };
        let job = legacy::DeployJob {
            deployment_identifier: legacy::DeploymentIdentifier(41),
            generation_identifier: legacy::GenerationIdentifier(42),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            phase: legacy::DeployJobPhase::Activated,
            closure_path: Some(legacy::ClosurePath(
                "/nix/store/legacy-generation".to_string(),
            )),
            source_revision_policy: legacy::SourceRevisionPolicy::RequireImmutable,
            requested_ref: legacy::FlakeReference("github:example/fixture".to_string()),
            resolved_ref: None,
            resolved_revision: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
            resolved_target: Some("root@node-1.alpha.criome".to_string()),
            boot_once_unit: None,
        };
        let test_run = legacy::StoredTestRun {
            test_run_identifier: legacy::TestRunIdentifier(91),
            cluster_name: legacy::ClusterName("alpha".to_string()),
            node_name: legacy::NodeName("node-1".to_string()),
            host: legacy::NodeName("host-1".to_string()),
            mode: legacy::TestMode::Hermetic,
            phase: legacy::TestRunPhase::Completed,
            outcome: legacy::TestOutcome::Passed,
            closure_path: Some(legacy::ClosurePath("/nix/store/test-run".to_string())),
        };
        database
            .commit_atomic(
                database
                    .begin_atomic_commit()
                    .assert(live_set, generation)
                    .assert(live_set, unsafe_generation)
                    .assert(roots, root)
                    .assert(roots, unsafe_root)
                    .assert(events, deployment_event)
                    .assert(events, container_event)
                    .assert(containers, container)
                    .assert(jobs, job)
                    .assert(test_runs, test_run),
            )
            .expect("seed v2 rows");
        drop(database);

        let source_bytes = fs::read(&path).expect("read source bytes");
        let outcome = StoreMigrator::new(&path)
            .migrate()
            .expect("migrate v2 store");
        let MigrationOutcome::Migrated { backup, counts, .. } = outcome else {
            panic!("expected migration outcome");
        };
        assert_eq!(
            fs::read(&backup).expect("read preserved backup"),
            source_bytes
        );
        assert_eq!(counts.generations, 2);
        assert_eq!(counts.gc_roots, 2);
        assert_eq!(counts.deployment_records, 2);
        assert_eq!(counts.event_log_entries, 1);
        assert_eq!(counts.quarantined_legacy_deployment_events, 1);

        let store = Store::open(&path).expect("open v3 store");
        let records = store
            .deployment_records()
            .expect("read correlation records")
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| {
            matches!(
                record.deployment_lifecycle,
                DeploymentLifecycle::LegacyAmbiguous
            )
        }));
        let record = records
            .iter()
            .find(|record| *record.deployment_identifier.payload() == 41)
            .expect("reconstructed first correlation record");
        assert_eq!(
            record.deployment_lifecycle,
            DeploymentLifecycle::LegacyAmbiguous
        );
        assert!(record.optional_admission_marker.is_none());
        assert_eq!(
            record
                .deployment_request_identity
                .optional_immutable_revision
                .as_ref()
                .expect("preserved immutable revision")
                .payload(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let migrated_generations = store
            .matching_live_generations(|_| true)
            .expect("read migrated generations");
        assert!(migrated_generations.iter().all(|generation| {
            matches!(
                generation.generation_slot,
                crate::schema::sema::GenerationSlot::LegacyUnknown
            )
        }));
        let public = crate::adapters::ordinary_egress(
            crate::schema::sema::OrdinaryEgress::Queried(crate::schema::sema::GenerationListing {
                generation_vector: migrated_generations
                    .into_iter()
                    .map(|generation| crate::schema::sema::Generation {
                        generation_identifier: generation.generation_identifier,
                        deployment_identifier: generation.deployment_identifier,
                        cluster_name: generation.cluster_name,
                        node_name: generation.node_name,
                        generation_artifact: generation.generation_artifact,
                        activation_effect: generation.activation_effect,
                        generation_slot: generation.generation_slot,
                        closure_path: generation.closure_path,
                        optional_immutable_revision: None,
                    })
                    .collect(),
                deployment_record_vector: records.clone(),
                state_marker: StateMarker {
                    commit_sequence: CommitSequence::new(1),
                    state_digest: StateDigest::new(1),
                },
            }),
        )
        .expect("project migrated ordinary generations");
        let signal_lojix::schema::lib::Output::Queried(listing) = public else {
            panic!("expected ordinary generation listing");
        };
        let public_generations = &listing.payload().generation_vector;
        assert!(public_generations.iter().all(|generation| {
            matches!(
                generation.generation_slot,
                signal_lojix::schema::lib::GenerationSlot::Recent
            )
        }));
        assert_eq!(
            public_generations
                .iter()
                .find(|generation| *generation.generation_identifier.payload() == 42)
                .expect("canonical migrated generation")
                .optional_closure_path,
            Some(signal_lojix::schema::lib::ClosurePath::new(
                CANONICAL_CLOSURE
            ))
        );
        assert!(
            public_generations
                .iter()
                .find(|generation| *generation.generation_identifier.payload() == 44)
                .expect("unsafe migrated generation")
                .optional_closure_path
                .is_none()
        );
        let events = store.event_log_in_range(17, 19).expect("read events");
        assert!(
            events
                .iter()
                .all(|entry| !matches!(entry.logged_event, V3LoggedEvent::Deployment(_))),
            "legacy deployment events must not enter the public v3 journal"
        );
        let quarantine = store
            .legacy_deployment_event_quarantine()
            .expect("read private legacy quarantine");
        assert_eq!(quarantine.len(), 1);
        assert!(!quarantine[0].legacy_event_archive.payload().is_empty());
        assert_eq!(
            store.next_deployment_identifier().expect("next deployment"),
            44
        );
        assert_eq!(
            store.next_generation_identifier().expect("next generation"),
            45
        );
        assert_eq!(store.next_event_log_position().expect("next event"), 19);
        drop(store);

        let repeated = StoreMigrator::new(&path)
            .migrate()
            .expect("repeat migration is an idempotent inspection");
        let MigrationOutcome::AlreadyCurrent {
            counts: repeated_counts,
            ..
        } = repeated
        else {
            panic!("repeat migration must not rewrite a v3 store");
        };
        assert_eq!(repeated_counts, counts);
    }
}
