//! Store — the single-writer SEMA layer.
//!
//! Per `skills/component-triad.md` §"Runtime triad — SEMA":
//! the Store owns durable state; only the Store writes it; the
//! executor flow is `Engine -> Store::apply(SemaCommand) -> SemaResponse`.
//! The pilot is in-memory; the durable redb backend lands in a
//! follow-on slice without changing the Store's mailbox shape.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible as KameoInfallible;
use kameo::message::{Context, Message};

use std::convert::Infallible;

use crate::generated::{
    ActivationKind, BuildRecord, CopyRecord, DeploymentIdentifier, Detail, GenerationIdentifier,
    GenerationRecord, GenerationSelector, ObservationRecord, PlanRecord, SemaCommand,
    SemaCommandIdentifier, SemaResponse, SourceRecord,
};

/// In-memory SEMA writer for the pilot. The `State` field carries
/// the plan, source, build, copy, generation, and observation
/// registries plus the identifier counters. Naming the actor IS
/// naming the noun — this is the Store.
pub struct Store {
    plans: Vec<PlanRecord>,
    sources: Vec<SourceRecord>,
    builds: Vec<BuildRecord>,
    copies: Vec<CopyRecord>,
    generations: Vec<GenerationRecord>,
    observations: Vec<ObservationRecord>,
    next_command_identifier: u64,
    next_deployment_identifier: u64,
    next_generation_identifier: u64,
    next_observation_identifier: u64,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        Self {
            plans: Vec::new(),
            sources: Vec::new(),
            builds: Vec::new(),
            copies: Vec::new(),
            generations: Vec::new(),
            observations: Vec::new(),
            next_command_identifier: 1,
            next_deployment_identifier: 1,
            next_generation_identifier: 1,
            next_observation_identifier: 1,
        }
    }

    /// Apply a SemaCommand and yield a SemaResponse.
    ///
    /// Method-on-type per `skills/abstractions.md`: every verb the
    /// SEMA writer offers lives here, dispatched off the typed
    /// SemaCommand enum.
    pub fn apply(&mut self, command: SemaCommand) -> SemaResponse {
        match command {
            SemaCommand::RecordPlan(mut plan) => {
                let deployment = self.allocate_deployment_identifier();
                plan.deployment_identifier = deployment.clone();
                self.plans.push(plan.clone());
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::RecordBuild(build) => {
                self.builds.push(build.clone());
                let record = GenerationRecord {
                    generation_identifier: build.generation_identifier.clone(),
                    deployment_identifier: self.synthesize_deployment_identifier(),
                    target_node: crate::generated::TargetNode(String::new()),
                    closure_path: build.closure_path.clone(),
                    activation_kind: ActivationKind::Test,
                };
                self.generations.push(record);
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::RecordSource(source) => {
                self.sources.push(source);
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::RecordCopy(copy) => {
                self.copies.push(copy);
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::RecordActivation(activation) => {
                // Promote the existing build's generation entry with
                // the activation's target/kind. If none exists, create
                // one — the activation is the GC-root-anchoring event.
                if let Some(generation) = self.generations.iter_mut().find(|generation| {
                    generation.generation_identifier == activation.generation_identifier
                }) {
                    generation.target_node = activation.target_node.clone();
                    generation.activation_kind = activation.activation_kind.clone();
                } else {
                    self.generations.push(GenerationRecord {
                        generation_identifier: activation.generation_identifier.clone(),
                        deployment_identifier: self.synthesize_deployment_identifier(),
                        target_node: activation.target_node.clone(),
                        closure_path: crate::generated::ClosurePath(String::new()),
                        activation_kind: activation.activation_kind.clone(),
                    });
                }
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::RecordObservation(observation) => {
                self.observations.push(observation);
                let acknowledgement = self.allocate_command_identifier();
                SemaResponse::Acknowledged(acknowledgement)
            }
            SemaCommand::QueryGeneration(selector) => self.query_generation(&selector),
        }
    }

    /// Latest stored plan for the deployment, if any.
    pub fn plan_for(&self, deployment: &DeploymentIdentifier) -> Option<&PlanRecord> {
        self.plans
            .iter()
            .rev()
            .find(|plan| &plan.deployment_identifier == deployment)
    }

    /// All stored generation records — used by tests as the testable
    /// witness that the build pipeline reached the ledger.
    pub fn generations(&self) -> &[GenerationRecord] {
        &self.generations
    }

    /// All observations recorded so far.
    pub fn observations(&self) -> &[ObservationRecord] {
        &self.observations
    }

    /// All stored plans.
    pub fn plans(&self) -> &[PlanRecord] {
        &self.plans
    }

    pub fn sources(&self) -> &[SourceRecord] {
        &self.sources
    }

    pub fn allocate_generation_identifier(&mut self) -> GenerationIdentifier {
        let identifier = GenerationIdentifier(self.next_generation_identifier);
        self.next_generation_identifier += 1;
        identifier
    }

    pub fn allocate_observation_identifier(&mut self) -> crate::generated::ObservationIdentifier {
        let identifier = crate::generated::ObservationIdentifier(self.next_observation_identifier);
        self.next_observation_identifier += 1;
        identifier
    }

    fn allocate_command_identifier(&mut self) -> SemaCommandIdentifier {
        let identifier = SemaCommandIdentifier(self.next_command_identifier);
        self.next_command_identifier += 1;
        identifier
    }

    fn allocate_deployment_identifier(&mut self) -> DeploymentIdentifier {
        let identifier = DeploymentIdentifier(self.next_deployment_identifier);
        self.next_deployment_identifier += 1;
        identifier
    }

    /// The deployment-identifier associated with the most recently
    /// recorded plan; used when synthesizing a generation record
    /// without an explicit deployment binding.
    fn synthesize_deployment_identifier(&self) -> DeploymentIdentifier {
        self.plans
            .last()
            .map(|plan| plan.deployment_identifier.clone())
            .unwrap_or(DeploymentIdentifier(0))
    }

    fn query_generation(&self, selector: &GenerationSelector) -> SemaResponse {
        let target = selector.target_deployment();
        match self
            .generations
            .iter()
            .rev()
            .find(|generation| &generation.deployment_identifier == target)
        {
            Some(record) => SemaResponse::GenerationLedgerEntry(record.clone()),
            None => SemaResponse::Missed(Detail(format!(
                "no generation recorded for deployment {}",
                target.0
            ))),
        }
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
    type Reply = Result<SemaResponse, Infallible>;

    async fn handle(
        &mut self,
        message: Apply,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.apply(message.0))
    }
}

/// Allocate the next generation identifier; the Builder asks the
/// Store before running an external build so the identifier is
/// stable across the deploy pipeline.
pub struct AllocateGeneration;

impl Message<AllocateGeneration> for Store {
    type Reply = Result<GenerationIdentifier, Infallible>;

    async fn handle(
        &mut self,
        _message: AllocateGeneration,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.allocate_generation_identifier())
    }
}

/// Read-only snapshot of generation records, for tests.
pub struct GenerationsSnapshot;

impl Message<GenerationsSnapshot> for Store {
    type Reply = Result<Vec<GenerationRecord>, Infallible>;

    async fn handle(
        &mut self,
        _message: GenerationsSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.generations.clone())
    }
}

/// Read-only snapshot of plan records, for tests.
pub struct PlansSnapshot;

impl Message<PlansSnapshot> for Store {
    type Reply = Result<Vec<PlanRecord>, Infallible>;

    async fn handle(
        &mut self,
        _message: PlansSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.plans.clone())
    }
}

/// Read-only snapshot of staged source records, for tests.
pub struct SourcesSnapshot;

impl Message<SourcesSnapshot> for Store {
    type Reply = Result<Vec<SourceRecord>, Infallible>;

    async fn handle(
        &mut self,
        _message: SourcesSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.sources.clone())
    }
}
