//! Store — the SEMA state plane, backed by `sema-engine`.
//!
//! Per `skills/component-triad.md` §"Runtime triad - SEMA": the
//! Store owns durable state; only the Store writes it. The schema-
//! emitted `SemaCommand` / `SemaResponse` types are still the
//! mailbox; iteration 2 replaces the in-memory `Vec<...>` with a
//! `sema-engine` `Engine` backed by redb so state survives daemon
//! restart and feeds the `DatabaseMarker` (commit-counter +
//! state-hash) Nexus stamps on every reply.
//!
//! Each record family (PlanRecord, BuildRecord, CopyRecord,
//! ActivationRecord, ObservationRecord, GenerationRecord) is one
//! sema-engine `TableReference`. Records implement `EngineRecord`
//! (one impl per record family below) to give sema-engine the
//! `RecordKey`. The Store's State carries the sema-engine handle
//! plus per-family TableReferences plus the deployment/generation
//! counter records that survive restart.

use std::convert::Infallible;
use std::path::PathBuf;

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible as KameoInfallible;
use kameo::message::{Context, Message};

use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, TableDescriptor,
    TableName, TableReference,
};

use crate::error::{Error, Result};
use crate::generated::{
    ActivationKind, ActivationRecord, BuildRecord, ClosurePath, CopyRecord, DatabaseMarker,
    DeploymentIdentifier, Detail, GenerationIdentifier, GenerationRecord, GenerationSelector,
    ObservationIdentifier, ObservationRecord, PlanRecord, SemaCommand, SemaCommandIdentifier,
    SemaResponse, StateHash, TargetNode, TransactionCounter,
};

const PLANS_TABLE: TableName = TableName::new("lojix_plans");
const BUILDS_TABLE: TableName = TableName::new("lojix_builds");
const COPIES_TABLE: TableName = TableName::new("lojix_copies");
const ACTIVATIONS_TABLE: TableName = TableName::new("lojix_activations");
const OBSERVATIONS_TABLE: TableName = TableName::new("lojix_observations");
const GENERATIONS_TABLE: TableName = TableName::new("lojix_generations");
const COUNTERS_TABLE: TableName = TableName::new("lojix_counters");

/// Counter row stored in the COUNTERS_TABLE. Each counter name
/// (`deployment`, `generation`, `command`, `observation`) maps to
/// one row; the value is the next-identifier-to-allocate.
#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct CounterRow {
    pub name: String,
    pub next_value: u64,
}

impl EngineRecord for CounterRow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.name.clone())
    }
}

impl CounterRow {
    pub fn new(name: impl Into<String>, next_value: u64) -> Self {
        Self {
            name: name.into(),
            next_value,
        }
    }
}

// EngineRecord impls for every schema-emitted record family the
// Store persists. The verb (`record_key`) lives on the schema-emitted
// noun, per the schema-derived stack discipline (record 882 +
// `skills/rust/methods.md` §"Schema-generated objects are the method
// surface").

impl EngineRecord for PlanRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("plan-{:020}", self.deployment_identifier.0))
    }
}

impl EngineRecord for BuildRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("build-{:020}", self.generation_identifier.0))
    }
}

impl EngineRecord for CopyRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("copy-{:020}", self.generation_identifier.0))
    }
}

impl EngineRecord for ActivationRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("activation-{:020}", self.generation_identifier.0))
    }
}

impl EngineRecord for ObservationRecord {
    fn record_key(&self) -> RecordKey {
        // The same deployment receives many observations; suffix by
        // phase+status to keep keys distinct in the table.
        RecordKey::new(format!(
            "observation-{:020}-{}-{}",
            self.deployment_identifier.0,
            self.phase.tag(),
            self.status.tag(),
        ))
    }
}

impl EngineRecord for GenerationRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("generation-{:020}", self.generation_identifier.0))
    }
}

/// Sema-engine-backed Store actor. State carries the redb-backed
/// Engine handle plus the typed TableReferences. The actor IS the
/// Store; the methods on the actor delegate to the sema-engine
/// Engine.
pub struct Store {
    engine: Engine,
    plans_table: TableReference<PlanRecord>,
    builds_table: TableReference<BuildRecord>,
    copies_table: TableReference<CopyRecord>,
    activations_table: TableReference<ActivationRecord>,
    observations_table: TableReference<ObservationRecord>,
    generations_table: TableReference<GenerationRecord>,
    counters_table: TableReference<CounterRow>,
    database_path: PathBuf,
}

impl Store {
    /// Open or create a sema-engine database at the given path and
    /// register every record family used by lojix. Survives restart.
    pub fn open(database_path: impl Into<PathBuf>) -> Result<Self> {
        let database_path = database_path.into();
        if let Some(parent) = database_path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let mut engine = Engine::open(EngineOpen::new(&database_path, SchemaVersion::new(1)))
            .map_err(Error::Sema)?;
        let plans_table = engine
            .register_table(TableDescriptor::<PlanRecord>::new(PLANS_TABLE))
            .map_err(Error::Sema)?;
        let builds_table = engine
            .register_table(TableDescriptor::<BuildRecord>::new(BUILDS_TABLE))
            .map_err(Error::Sema)?;
        let copies_table = engine
            .register_table(TableDescriptor::<CopyRecord>::new(COPIES_TABLE))
            .map_err(Error::Sema)?;
        let activations_table = engine
            .register_table(TableDescriptor::<ActivationRecord>::new(ACTIVATIONS_TABLE))
            .map_err(Error::Sema)?;
        let observations_table = engine
            .register_table(TableDescriptor::<ObservationRecord>::new(
                OBSERVATIONS_TABLE,
            ))
            .map_err(Error::Sema)?;
        let generations_table = engine
            .register_table(TableDescriptor::<GenerationRecord>::new(GENERATIONS_TABLE))
            .map_err(Error::Sema)?;
        let counters_table = engine
            .register_table(TableDescriptor::<CounterRow>::new(COUNTERS_TABLE))
            .map_err(Error::Sema)?;
        Ok(Self {
            engine,
            plans_table,
            builds_table,
            copies_table,
            activations_table,
            observations_table,
            generations_table,
            counters_table,
            database_path,
        })
    }

    pub fn database_path(&self) -> &std::path::Path {
        &self.database_path
    }

    /// Apply a SemaCommand and yield a SemaResponse, writing the
    /// effect through sema-engine. This is the only place the
    /// schema-emitted SemaCommand contact-point lowers to a
    /// sema-engine operation; the rest of the runtime sees only
    /// SemaCommand/SemaResponse.
    pub fn apply(&mut self, command: SemaCommand) -> Result<SemaResponse> {
        match command {
            SemaCommand::RecordPlan(mut plan) => {
                let deployment = self.allocate_counter("deployment")?;
                plan.deployment_identifier = DeploymentIdentifier(deployment);
                self.assert_record(self.plans_table, &plan)?;
                let acknowledgement = self.allocate_command_identifier()?;
                Ok(SemaResponse::Acknowledged(acknowledgement))
            }
            SemaCommand::RecordBuild(build) => {
                self.assert_record(self.builds_table, &build)?;
                let deployment = self.most_recent_deployment_identifier()?;
                let generation_record = GenerationRecord {
                    generation_identifier: build.generation_identifier.clone(),
                    deployment_identifier: deployment,
                    target_node: TargetNode(String::new()),
                    closure_path: build.closure_path.clone(),
                    activation_kind: ActivationKind::Test,
                };
                self.assert_record(self.generations_table, &generation_record)?;
                let acknowledgement = self.allocate_command_identifier()?;
                Ok(SemaResponse::Acknowledged(acknowledgement))
            }
            SemaCommand::RecordCopy(copy) => {
                self.assert_record(self.copies_table, &copy)?;
                let acknowledgement = self.allocate_command_identifier()?;
                Ok(SemaResponse::Acknowledged(acknowledgement))
            }
            SemaCommand::RecordActivation(activation) => {
                self.assert_record(self.activations_table, &activation)?;
                // Promote the existing generation record with the
                // activation's target/kind, or insert a fresh one if
                // none exists.
                let mut updated_generation = self
                    .read_generation(&activation.generation_identifier)?
                    .map(|existing| GenerationRecord {
                        generation_identifier: existing.generation_identifier,
                        deployment_identifier: existing.deployment_identifier,
                        target_node: activation.target_node.clone(),
                        closure_path: existing.closure_path,
                        activation_kind: activation.activation_kind.clone(),
                    });
                if updated_generation.is_none() {
                    let deployment = self.most_recent_deployment_identifier()?;
                    updated_generation = Some(GenerationRecord {
                        generation_identifier: activation.generation_identifier.clone(),
                        deployment_identifier: deployment,
                        target_node: activation.target_node.clone(),
                        closure_path: ClosurePath(String::new()),
                        activation_kind: activation.activation_kind.clone(),
                    });
                }
                let generation_record =
                    updated_generation.expect("generation record was just constructed via Some");
                if self
                    .read_generation(&generation_record.generation_identifier)?
                    .is_some()
                {
                    self.mutate_record(self.generations_table, &generation_record)?;
                } else {
                    self.assert_record(self.generations_table, &generation_record)?;
                }
                let acknowledgement = self.allocate_command_identifier()?;
                Ok(SemaResponse::Acknowledged(acknowledgement))
            }
            SemaCommand::RecordObservation(observation) => {
                self.assert_record(self.observations_table, &observation)?;
                let acknowledgement = self.allocate_command_identifier()?;
                Ok(SemaResponse::Acknowledged(acknowledgement))
            }
            SemaCommand::QueryGeneration(selector) => self.query_generation(&selector),
        }
    }

    /// Build the current DatabaseMarker from sema-engine's transaction
    /// state. The counter is sema-engine's monotonic `CommitSequence`;
    /// the hash is a Blake3 digest over `(counter, latest_snapshot)`.
    pub fn current_database_marker(&self) -> Result<DatabaseMarker> {
        let commit_sequence = self
            .engine
            .current_commit_sequence()
            .map_err(Error::Sema)?
            .value();
        let snapshot = self.engine.latest_snapshot().map_err(Error::Sema)?.value();
        let mut hasher = blake3::Hasher::new();
        hasher.update(&commit_sequence.to_le_bytes());
        hasher.update(&snapshot.to_le_bytes());
        let hash = hasher.finalize();
        Ok(DatabaseMarker {
            transaction_counter: TransactionCounter(commit_sequence),
            state_hash: StateHash(hash.to_hex().to_string()),
        })
    }

    /// Latest stored plan for the deployment, if any.
    pub fn plan_for(&self, deployment: &DeploymentIdentifier) -> Result<Option<PlanRecord>> {
        let key = RecordKey::new(format!("plan-{:020}", deployment.0));
        let snapshot = self
            .engine
            .match_records(QueryPlan::key(self.plans_table, key))
            .map_err(Error::Sema)?;
        Ok(snapshot.records().first().cloned())
    }

    /// All stored generation records — used by tests as the testable
    /// witness that the build pipeline reached the ledger.
    pub fn generations(&self) -> Result<Vec<GenerationRecord>> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.generations_table))
            .map_err(Error::Sema)?;
        Ok(snapshot.records().to_vec())
    }

    /// All observations recorded so far.
    pub fn observations(&self) -> Result<Vec<ObservationRecord>> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.observations_table))
            .map_err(Error::Sema)?;
        Ok(snapshot.records().to_vec())
    }

    /// All stored plans.
    pub fn plans(&self) -> Result<Vec<PlanRecord>> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.plans_table))
            .map_err(Error::Sema)?;
        Ok(snapshot.records().to_vec())
    }

    pub fn allocate_generation_identifier(&mut self) -> Result<GenerationIdentifier> {
        let value = self.allocate_counter("generation")?;
        Ok(GenerationIdentifier(value))
    }

    pub fn allocate_observation_identifier(&mut self) -> Result<ObservationIdentifier> {
        let value = self.allocate_counter("observation")?;
        Ok(ObservationIdentifier(value))
    }

    fn allocate_command_identifier(&mut self) -> Result<SemaCommandIdentifier> {
        let value = self.allocate_counter("command")?;
        Ok(SemaCommandIdentifier(value))
    }

    /// Allocate one counter value, persisting the next-value into the
    /// counters table so the counter survives restart. The counters
    /// table itself is a `CounterRow` family with the counter name as
    /// the key.
    fn allocate_counter(&mut self, name: &str) -> Result<u64> {
        let key = RecordKey::new(name.to_owned());
        let existing = self
            .engine
            .match_records(QueryPlan::key(self.counters_table, key.clone()))
            .map_err(Error::Sema)?;
        let current_value = existing
            .records()
            .first()
            .map(|row| row.next_value)
            .unwrap_or(1);
        let next_row = CounterRow::new(name, current_value + 1);
        if existing.records().is_empty() {
            self.engine
                .assert(Assertion::new(self.counters_table, next_row))
                .map_err(Error::Sema)?;
        } else {
            self.engine
                .mutate(Mutation::new(self.counters_table, next_row))
                .map_err(Error::Sema)?;
        }
        Ok(current_value)
    }

    fn most_recent_deployment_identifier(&self) -> Result<DeploymentIdentifier> {
        let plans = self.plans()?;
        Ok(plans
            .last()
            .map(|plan| plan.deployment_identifier.clone())
            .unwrap_or(DeploymentIdentifier(0)))
    }

    fn read_generation(
        &self,
        generation: &GenerationIdentifier,
    ) -> Result<Option<GenerationRecord>> {
        let key = RecordKey::new(format!("generation-{:020}", generation.0));
        let snapshot = self
            .engine
            .match_records(QueryPlan::key(self.generations_table, key))
            .map_err(Error::Sema)?;
        Ok(snapshot.records().first().cloned())
    }

    fn query_generation(&self, selector: &GenerationSelector) -> Result<SemaResponse> {
        let target = selector.target_deployment();
        let all = self.generations()?;
        let record = all
            .into_iter()
            .rev()
            .find(|generation| &generation.deployment_identifier == target);
        Ok(match record {
            Some(record) => SemaResponse::GenerationLedgerEntry(record),
            None => SemaResponse::Missed(Detail(format!(
                "no generation recorded for deployment {}",
                target.0
            ))),
        })
    }

    fn assert_record<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        record: &RecordValue,
    ) -> Result<()>
    where
        RecordValue: sema_engine::EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        self.engine
            .assert(Assertion::new(table, record.clone()))
            .map_err(Error::Sema)?;
        Ok(())
    }

    fn mutate_record<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        record: &RecordValue,
    ) -> Result<()>
    where
        RecordValue: sema_engine::EngineStoredRecord + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        self.engine
            .mutate(Mutation::new(table, record.clone()))
            .map_err(Error::Sema)?;
        Ok(())
    }
}

impl Actor for Store {
    type Args = Self;
    type Error = KameoInfallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Apply a SemaCommand on the Store actor; return the SemaResponse.
pub struct Apply(pub SemaCommand);

impl Message<Apply> for Store {
    type Reply = Result<SemaResponse>;

    async fn handle(
        &mut self,
        message: Apply,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.apply(message.0)
    }
}

/// Allocate the next generation identifier; the Builder asks the
/// Store before running an external build so the identifier is
/// stable across the deploy pipeline.
pub struct AllocateGeneration;

impl Message<AllocateGeneration> for Store {
    type Reply = Result<GenerationIdentifier>;

    async fn handle(
        &mut self,
        _message: AllocateGeneration,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.allocate_generation_identifier()
    }
}

/// Read-only snapshot of generation records, for tests.
pub struct GenerationsSnapshot;

impl Message<GenerationsSnapshot> for Store {
    type Reply = Result<Vec<GenerationRecord>>;

    async fn handle(
        &mut self,
        _message: GenerationsSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.generations()
    }
}

/// Read-only snapshot of plan records, for tests.
pub struct PlansSnapshot;

impl Message<PlansSnapshot> for Store {
    type Reply = Result<Vec<PlanRecord>>;

    async fn handle(
        &mut self,
        _message: PlansSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.plans()
    }
}

/// Read the current DatabaseMarker (commit-counter + state-hash).
/// Nexus asks this immediately before stamping each reply.
pub struct CurrentDatabaseMarker;

impl Message<CurrentDatabaseMarker> for Store {
    type Reply = Result<DatabaseMarker>;

    async fn handle(
        &mut self,
        _message: CurrentDatabaseMarker,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.current_database_marker()
    }
}

impl crate::generated::Phase {
    /// Short tag used to disambiguate observation record keys
    /// inside the sema-engine table. The tag IS the verb on the
    /// schema-emitted Phase enum (per record 882 — schema-emitted
    /// types are the nouns).
    pub fn tag(&self) -> &'static str {
        match self {
            Self::PlanReceived => "plan_received",
            Self::Authorized => "authorized",
            Self::Building => "building",
            Self::CopyingClosure => "copying_closure",
            Self::Activating => "activating",
            Self::Observed => "observed",
            Self::Failed => "failed",
        }
    }
}

impl crate::generated::Status {
    /// Short tag used to disambiguate observation record keys
    /// inside the sema-engine table.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Progressing => "progressing",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

// Suppress unused import warning for Infallible used by Kameo reply
// shapes; the import is required so the typed alias above is in scope.
const _: Option<Infallible> = None;
