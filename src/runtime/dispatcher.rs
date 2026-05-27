//! OperationDispatcher — the executor.
//!
//! Owns ActorRefs to every downstream plane (Authorization, Builder,
//! Copier, Activator, GcRootPinner, Store, ObservationFan, TraceLog)
//! and drives an Input through the full deploy pipeline, emitting the
//! corresponding Output. State carries the ActorRef set — the actor
//! IS the dispatcher; the refs ARE the topology it dispatches across.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::{Error, Result};
use crate::generated::{
    ActivationKind, BuildRecord, CopyRecord, Detail, Input, ObservationRecord, Output, Phase,
    RejectionReason, SemaCommand, Status,
};
use crate::runtime::activator::{Activator, DriveActivation};
use crate::runtime::authorization::{AuthorizationGate, AuthorizeMessage};
use crate::runtime::builder::{Builder, DriveBuild};
use crate::runtime::codec::Lowered;
use crate::runtime::copier::{ClosureCopier, DriveCopy};
use crate::runtime::gc_root::{DrivePin, GcRootPinner};
use crate::runtime::observation::{BroadcastObservation, ObservationFan};
use crate::runtime::store::{AllocateGeneration, Apply, Store};
use crate::runtime::trace::{
    AuthorizationDecision, Plane, RecordWitness, SemaCommandTag, TraceLog, TraceWitness,
};

#[derive(Clone)]
pub struct ActorReferenceSet {
    pub authorization: ActorRef<AuthorizationGate>,
    pub builder: ActorRef<Builder>,
    pub copier: ActorRef<ClosureCopier>,
    pub activator: ActorRef<Activator>,
    pub gc_root: ActorRef<GcRootPinner>,
    pub store: ActorRef<Store>,
    pub fan: ActorRef<ObservationFan>,
    pub trace: ActorRef<TraceLog>,
}

// ActorReferenceSet has only public fields; construction is via struct
// literal at the Engine spawn site. No `new` constructor is needed —
// the verb is just field assignment.

pub struct OperationDispatcher {
    refs: ActorReferenceSet,
}

impl OperationDispatcher {
    pub fn new(refs: ActorReferenceSet) -> Self {
        Self { refs }
    }

    pub fn refs(&self) -> &ActorReferenceSet {
        &self.refs
    }

    /// Drive a single Input through the full deploy pipeline, returning the Output.
    pub async fn drive(&self, input: Input) -> Result<Output> {
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::InputReceived {
                plane: Plane::OperationDispatcher,
            }))
            .await;
        match input {
            Input::Submit(request) => self.handle_submit(request).await,
            other => self.handle_through_codec(other).await,
        }
    }

    async fn handle_submit(&self, request: crate::generated::DeploymentRequest) -> Result<Output> {
        // Authorization
        let decision = self
            .refs
            .authorization
            .ask(AuthorizeMessage(request.clone()))
            .await
            .map_err(|error| Error::ActorSend(format!("authorization: {error}")))?;
        let granted = matches!(
            decision,
            crate::generated::CriomeAuthorization::Bypass
                | crate::generated::CriomeAuthorization::OperatorAllowlist
                | crate::generated::CriomeAuthorization::Criome
        );
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::AuthorizationDecided {
                plane: Plane::AuthorizationGate,
                decision: if granted {
                    AuthorizationDecision::Granted
                } else {
                    AuthorizationDecision::Denied
                },
            }))
            .await;
        if !granted {
            return Ok(Output::Rejected(RejectionReason::Unauthorized));
        }

        // Plan materialization
        let plan = request.into_plan_record();
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::PlanMaterialized {
                plane: Plane::OperationDispatcher,
            }))
            .await;

        // SEMA: record the plan (Store assigns deployment id)
        let plan_response = self
            .refs
            .store
            .ask(Apply(SemaCommand::RecordPlan(plan.clone())))
            .await
            .map_err(|error| Error::ActorSend(format!("store/RecordPlan: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::SemaApplied {
                plane: Plane::Store,
                command: SemaCommandTag::RecordPlan,
            }))
            .await;
        let _ = plan_response;

        // Read back the deployment id from store
        let stored_plans = self
            .refs
            .store
            .ask(crate::runtime::store::PlansSnapshot)
            .await
            .map_err(|error| Error::ActorSend(format!("store/Plans: {error}")))?;
        let stored = stored_plans
            .last()
            .cloned()
            .ok_or_else(|| Error::ActorSend("store has no plans after RecordPlan".to_owned()))?;
        let deployment = stored.deployment_identifier.clone();

        // Build
        let generation = self
            .refs
            .store
            .ask(AllocateGeneration)
            .await
            .map_err(|error| Error::ActorSend(format!("store/AllocateGeneration: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::BuildStarted {
                plane: Plane::Builder,
            }))
            .await;
        self.broadcast_observation(
            deployment.clone(),
            Phase::Building,
            Status::Started,
            "build start",
        )
        .await?;
        let build = self
            .refs
            .builder
            .ask(DriveBuild {
                plan: stored.clone(),
                generation,
            })
            .await
            .map_err(|error| Error::ActorSend(format!("builder: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::BuildComplete {
                plane: Plane::Builder,
            }))
            .await;
        self.broadcast_observation(
            deployment.clone(),
            Phase::Building,
            Status::Complete,
            "build complete",
        )
        .await?;
        let _ = self
            .refs
            .store
            .ask(Apply(SemaCommand::RecordBuild(build.clone())))
            .await
            .map_err(|error| Error::ActorSend(format!("store/RecordBuild: {error}")))?;

        // Pin GC root
        let pin_identifier = self
            .refs
            .gc_root
            .ask(DrivePin(build.clone()))
            .await
            .map_err(|error| Error::ActorSend(format!("gc_root: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::GcPinned {
                plane: Plane::GcRootPinner,
            }))
            .await;
        let _ = pin_identifier;

        // Copy
        self.broadcast_observation(
            deployment.clone(),
            Phase::CopyingClosure,
            Status::Started,
            "copy start",
        )
        .await?;
        let copy = self.drive_copy(&build, &stored.target_node).await?;
        let _ = self
            .refs
            .store
            .ask(Apply(SemaCommand::RecordCopy(copy.clone())))
            .await
            .map_err(|error| Error::ActorSend(format!("store/RecordCopy: {error}")))?;
        self.broadcast_observation(
            deployment.clone(),
            Phase::CopyingClosure,
            Status::Complete,
            "copy complete",
        )
        .await?;

        // Activate
        self.broadcast_observation(
            deployment.clone(),
            Phase::Activating,
            Status::Started,
            "activation start",
        )
        .await?;
        let activation = self.drive_activation(&copy).await?;
        let _ = self
            .refs
            .store
            .ask(Apply(SemaCommand::RecordActivation(activation.clone())))
            .await
            .map_err(|error| Error::ActorSend(format!("store/RecordActivation: {error}")))?;
        self.broadcast_observation(
            deployment.clone(),
            Phase::Activating,
            Status::Complete,
            "activation complete",
        )
        .await?;
        self.broadcast_observation(
            deployment.clone(),
            Phase::Observed,
            Status::Complete,
            "deploy done",
        )
        .await?;

        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::OutputEmitted {
                plane: Plane::OperationDispatcher,
            }))
            .await;
        Ok(Output::Accepted(deployment))
    }

    async fn drive_copy(
        &self,
        build: &BuildRecord,
        target_node: &crate::generated::TargetNode,
    ) -> Result<CopyRecord> {
        let copy = self
            .refs
            .copier
            .ask(DriveCopy {
                build: build.clone(),
                target_node: target_node.clone(),
            })
            .await
            .map_err(|error| Error::ActorSend(format!("copier: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::ClosureCopied {
                plane: Plane::ClosureCopier,
            }))
            .await;
        Ok(copy)
    }

    async fn drive_activation(
        &self,
        copy: &CopyRecord,
    ) -> Result<crate::generated::ActivationRecord> {
        let record = self
            .refs
            .activator
            .ask(DriveActivation {
                copy: copy.clone(),
                activation_kind: ActivationKind::Test,
            })
            .await
            .map_err(|error| Error::ActorSend(format!("activator: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::ActivationComplete {
                plane: Plane::Activator,
            }))
            .await;
        Ok(record)
    }

    async fn broadcast_observation(
        &self,
        deployment: crate::generated::DeploymentIdentifier,
        phase: Phase,
        status: Status,
        detail: &str,
    ) -> Result<()> {
        let record = ObservationRecord {
            deployment_identifier: deployment,
            phase,
            status,
            detail: Detail(detail.to_owned()),
        };
        self.refs
            .fan
            .ask(BroadcastObservation(record.clone()))
            .await
            .map_err(|error| Error::ActorSend(format!("fan: {error}")))?;
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::ObservationFanned {
                plane: Plane::ObservationFan,
            }))
            .await;
        Ok(())
    }

    async fn handle_through_codec(&self, input: Input) -> Result<Output> {
        match input.lower_to_sema_command() {
            Lowered::ForwardOnly(output) => {
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::OutputEmitted {
                        plane: Plane::OperationDispatcher,
                    }))
                    .await;
                Ok(output)
            }
            Lowered::StateInvolving(command) => {
                let tag = match &command {
                    SemaCommand::RecordPlan(_) => SemaCommandTag::RecordPlan,
                    SemaCommand::RecordBuild(_) => SemaCommandTag::RecordBuild,
                    SemaCommand::RecordCopy(_) => SemaCommandTag::RecordCopy,
                    SemaCommand::RecordActivation(_) => SemaCommandTag::RecordActivation,
                    SemaCommand::RecordObservation(_) => SemaCommandTag::RecordObservation,
                    SemaCommand::QueryGeneration(_) => SemaCommandTag::QueryGeneration,
                };
                let response = self
                    .refs
                    .store
                    .ask(Apply(command))
                    .await
                    .map_err(|error| Error::ActorSend(format!("store: {error}")))?;
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::SemaApplied {
                        plane: Plane::Store,
                        command: tag,
                    }))
                    .await;
                let output = response.into_output();
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::OutputEmitted {
                        plane: Plane::OperationDispatcher,
                    }))
                    .await;
                Ok(output)
            }
        }
    }
}

impl Actor for OperationDispatcher {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct Dispatch(pub Input);

impl Message<Dispatch> for OperationDispatcher {
    type Reply = Result<Output>;

    async fn handle(
        &mut self,
        message: Dispatch,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.drive(message.0).await
    }
}
