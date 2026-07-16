//! Typed, read-only reconstruction of a schema-one Lojix store into a new
//! final schema-two store.
//!
//! The source is opened through redb's read-only API. It is never registered,
//! compacted, or otherwise written. Every preservable row is decoded and every
//! generation/root and container/event relation is validated before the new
//! destination is created and seeded in one atomic commit.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize};

use crate::schema::sema::{
    ContainerLifecycleRecord, DeployJobPhase, EventLogEntry, GcRoot, LiveGeneration, LoggedEvent,
    StoredTestRun,
};
use crate::{Error, Result, Store};

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const SCHEMA_ONE: u64 = 1;
const LIVE_SET_TABLE: &str = "live-set";
const GC_ROOTS_TABLE: &str = "gc-roots";
const EVENT_LOG_TABLE: &str = "event-log";
const CONTAINER_LIFECYCLE_TABLE: &str = "container-lifecycle";
const DEPLOY_JOB_TABLE: &str = "deploy-job";
const TEST_RUN_TABLE: &str = "test-run";

/// A reason a schema-one deploy cursor cannot be reproduced in schema two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OmittedDeployJobReason {
    /// Schema one did not persist a `DeploySubmission`; inventing either a
    /// host or user-environment request would be unsafe.
    MissingDeploySubmission,
}

/// A deploy cursor deliberately left out of the destination store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmittedDeployJob {
    pub deployment_identifier: u64,
    pub phase: DeployJobPhase,
    pub reason: OmittedDeployJobReason,
}

/// The auditable result of one successful reconstruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconstructionReport {
    source: PathBuf,
    destination: PathBuf,
    pub generations: usize,
    pub gc_roots: usize,
    pub event_log_entries: usize,
    pub container_lifecycle_records: usize,
    pub test_runs: usize,
    pub omitted_deploy_jobs: Vec<OmittedDeployJob>,
}

impl std::fmt::Display for ReconstructionReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "(SchemaOneReconstructed {} {} generations={} roots={} events={} containers={} test_runs={} omitted_jobs={:?})",
            self.source.display(),
            self.destination.display(),
            self.generations,
            self.gc_roots,
            self.event_log_entries,
            self.container_lifecycle_records,
            self.test_runs,
            self.omitted_deploy_jobs,
        )
    }
}

impl ReconstructionReport {
    pub fn source(&self) -> &Path {
        &self.source
    }

    pub fn destination(&self) -> &Path {
        &self.destination
    }
}

/// The library surface behind `lojix-reconstruct-schema-one`.
pub struct SchemaOneReconstructor {
    source: PathBuf,
    destination: PathBuf,
}

impl SchemaOneReconstructor {
    pub fn new(source: impl Into<PathBuf>, destination: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
        }
    }

    /// Reconstruct once. An existing destination is always rejected; this
    /// makes a failed invocation idempotent and ensures no partial destination
    /// is ever treated as an input to a later invocation.
    pub fn reconstruct(&self) -> Result<ReconstructionReport> {
        if self.destination.exists() {
            return Err(Error::Reconstruction(format!(
                "destination already exists: {}",
                self.destination.display()
            )));
        }
        let snapshot = SchemaOneSnapshot::read(&self.source)?;
        snapshot.validate()?;

        // No destination open happens before all source validation succeeds.
        let destination = Store::open(self.destination.clone())?;
        if !snapshot.generations.is_empty()
            || !snapshot.roots.is_empty()
            || !snapshot.events.is_empty()
            || !snapshot.containers.is_empty()
            || !snapshot.test_runs.is_empty()
        {
            destination.seed_reconstruction(
                snapshot.generations.clone(),
                snapshot.roots.clone(),
                snapshot.events.clone(),
                snapshot.containers.clone(),
                snapshot.test_runs.clone(),
            )?;
        }

        Ok(ReconstructionReport {
            source: self.source.clone(),
            destination: self.destination.clone(),
            generations: snapshot.generations.len(),
            gc_roots: snapshot.roots.len(),
            event_log_entries: snapshot.events.len(),
            container_lifecycle_records: snapshot.containers.len(),
            test_runs: snapshot.test_runs.len(),
            omitted_deploy_jobs: snapshot.omitted_jobs,
        })
    }
}

struct SchemaOneSnapshot {
    generations: Vec<LiveGeneration>,
    roots: Vec<GcRoot>,
    events: Vec<EventLogEntry>,
    containers: Vec<ContainerLifecycleRecord>,
    test_runs: Vec<StoredTestRun>,
    omitted_jobs: Vec<OmittedDeployJob>,
}

impl SchemaOneSnapshot {
    fn read(source: &Path) -> Result<Self> {
        let database = redb::ReadOnlyDatabase::open(source).map_err(|error| {
            Error::Reconstruction(format!("source did not open read-only: {error}"))
        })?;
        let version = Self::schema_version(&database)?;
        if version != SCHEMA_ONE {
            return Err(Error::Reconstruction(format!(
                "source schema version is {version}, expected schema one"
            )));
        }
        let legacy_jobs = Self::rows::<LegacyDeployJob>(&database, DEPLOY_JOB_TABLE)?;
        Ok(Self {
            generations: Self::rows(&database, LIVE_SET_TABLE)?,
            roots: Self::rows(&database, GC_ROOTS_TABLE)?,
            events: Self::rows(&database, EVENT_LOG_TABLE)?,
            containers: Self::rows(&database, CONTAINER_LIFECYCLE_TABLE)?,
            test_runs: Self::rows(&database, TEST_RUN_TABLE)?,
            omitted_jobs: legacy_jobs
                .into_iter()
                .map(|job| OmittedDeployJob {
                    deployment_identifier: *job.deployment_identifier.payload(),
                    phase: job.phase,
                    reason: OmittedDeployJobReason::MissingDeploySubmission,
                })
                .collect(),
        })
    }

    fn schema_version(database: &redb::ReadOnlyDatabase) -> Result<u64> {
        let transaction = database.begin_read().map_err(|error| {
            Error::Reconstruction(format!("source metadata read failed: {error}"))
        })?;
        let table = transaction.open_table(META_TABLE).map_err(|error| {
            Error::Reconstruction(format!("source metadata table missing: {error}"))
        })?;
        let version = table
            .get(SCHEMA_VERSION_KEY)
            .map_err(|error| Error::Reconstruction(format!("source version read failed: {error}")))?
            .ok_or_else(|| Error::Reconstruction("source schema version is missing".to_string()))?;
        Ok(version.value())
    }

    fn rows<Record>(database: &redb::ReadOnlyDatabase, name: &str) -> Result<Vec<Record>>
    where
        Record: Archive,
        Record::Archived: RkyvDeserialize<Record, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let transaction = database.begin_read().map_err(|error| {
            Error::Reconstruction(format!("source table {name} read failed: {error}"))
        })?;
        let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(name);
        let table = match transaction.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => {
                return Err(Error::Reconstruction(format!(
                    "source table {name} open failed: {error}"
                )));
            }
        };
        let mut decoded = Vec::new();
        for row in table.iter().map_err(|error| {
            Error::Reconstruction(format!("source table {name} iteration failed: {error}"))
        })? {
            let (_key, value) = row.map_err(|error| {
                Error::Reconstruction(format!("source table {name} row failed: {error}"))
            })?;
            decoded.push(
                rkyv::from_bytes::<Record, rkyv::rancor::Error>(value.value()).map_err(
                    |error| {
                        Error::Reconstruction(format!("source table {name} decode failed: {error}"))
                    },
                )?,
            );
        }
        Ok(decoded)
    }

    fn validate(&self) -> Result<()> {
        let generations: BTreeMap<_, _> = self
            .generations
            .iter()
            .map(|generation| (*generation.generation_identifier.payload(), generation))
            .collect();
        if generations.len() != self.generations.len() {
            return Err(Error::Reconstruction(
                "duplicate generation identifier".to_string(),
            ));
        }
        let roots: BTreeMap<_, _> = self
            .roots
            .iter()
            .map(|root| (*root.generation_identifier.payload(), root))
            .collect();
        if roots.len() != self.roots.len() {
            return Err(Error::Reconstruction(
                "duplicate gc-root generation identifier".to_string(),
            ));
        }
        if generations.len() != roots.len() {
            return Err(Error::Reconstruction(
                "generation/root records are not a complete one-to-one set".to_string(),
            ));
        }
        for (identifier, generation) in generations {
            let root = roots.get(&identifier).ok_or_else(|| {
                Error::Reconstruction(format!("generation {identifier} has no gc root"))
            })?;
            if generation.cluster_name != root.cluster_name
                || generation.node_name != root.node_name
                || generation.closure_path != root.closure_path
            {
                return Err(Error::Reconstruction(format!(
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
            return Err(Error::Reconstruction(
                "duplicate event-log position".to_string(),
            ));
        }
        for container in &self.containers {
            let position = *container.event_log_position.payload();
            let matches_event = self.events.iter().any(|event| {
                *event.event_log_position.payload() == position
                    && matches!(&event.logged_event, LoggedEvent::Container(record)
                        if record == container)
            });
            if !matches_event {
                return Err(Error::Reconstruction(format!(
                    "container transition at event position {position} has no matching event"
                )));
            }
        }
        let runs: BTreeSet<_> = self
            .test_runs
            .iter()
            .map(|run| *run.test_run_identifier.payload())
            .collect();
        if runs.len() != self.test_runs.len() {
            return Err(Error::Reconstruction(
                "duplicate test-run identifier".to_string(),
            ));
        }
        Ok(())
    }
}

// Schema one persisted only this incomplete cursor. It deliberately has no
// DeploySubmission field; decoding it is how reconstruction proves that a
// restartable Host/UserEnvironment request cannot be honestly invented.
#[derive(Archive, rkyv::Serialize, rkyv::Deserialize)]
#[doc(hidden)]
pub struct LegacyDeployJob {
    deployment_identifier: signal_lojix::schema::lib::DeploymentIdentifier,
    generation_identifier: signal_lojix::schema::lib::GenerationIdentifier,
    cluster_name: signal_lojix::schema::lib::ClusterName,
    node_name: signal_lojix::schema::lib::NodeName,
    phase: DeployJobPhase,
    closure_path: Option<signal_lojix::schema::lib::ClosurePath>,
    source_revision_policy: signal_lojix::schema::lib::SourceRevisionPolicy,
    requested_ref: signal_lojix::schema::lib::FlakeReference,
    resolved_ref: Option<signal_lojix::schema::lib::FlakeReference>,
    resolved_revision: Option<String>,
    resolved_target: Option<String>,
    boot_once_unit: Option<String>,
}

#[doc(hidden)]
pub mod test_fixture {
    use super::*;

    pub fn legacy_job(deployment_identifier: u64, phase: DeployJobPhase) -> LegacyDeployJob {
        LegacyDeployJob {
            deployment_identifier: signal_lojix::schema::lib::DeploymentIdentifier::new(
                deployment_identifier,
            ),
            generation_identifier: signal_lojix::schema::lib::GenerationIdentifier::new(
                deployment_identifier,
            ),
            cluster_name: signal_lojix::schema::lib::ClusterName::new("goldragon"),
            node_name: signal_lojix::schema::lib::NodeName::new("dune"),
            phase,
            closure_path: None,
            source_revision_policy:
                signal_lojix::schema::lib::SourceRevisionPolicy::ResolveAndRecord,
            requested_ref: signal_lojix::schema::lib::FlakeReference::new("github:owner/repo"),
            resolved_ref: None,
            resolved_revision: None,
            resolved_target: None,
            boot_once_unit: None,
        }
    }
}
