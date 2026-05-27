//! Nexus mail keeper — the central runtime plane.
//!
//! Per psyche records 964 + 965 + 966-970, the daemon has THREE
//! execution centers: Signal (wire/socket), Nexus (mail keeper +
//! translator), SEMA (state engine). The flow is:
//!
//! ```text
//! Signal IN
//!   -> Nexus accepts mail (Sent -> Queued -> Processing)
//!   -> Nexus translates to SEMA command
//!   -> SEMA produces state change + reply
//!   -> Nexus receives reply (Processing -> Replied)
//!   -> Nexus stamps DatabaseMarker, translates to Signal response
//! Signal OUT
//! ```
//!
//! While the Nexus holds a mail entry, the mail is in the
//! BEING-PROCESSED state. Lifecycle transitions fire push-style
//! hooks per `skills/push-not-pull.md`. The hook surface is the
//! schema-emitted `MessageSentHook` / `MessageProcessedHook`
//! traits; tests attach concrete impls and assert they fire.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::generated::{
    ActivationKind, BuildRecord, CopyRecord, DatabaseMarker, Detail, Input, MailLifecycle,
    MessageIdentifier, MessageProcessed, MessageProcessedHook, MessageSent, MessageSentHook,
    ObservationRecord, Output, Phase, RejectedReply, RejectionReason, SemaCommand, Status,
};
use crate::runtime::activator::{Activator, DriveActivation};
use crate::runtime::authorization::{AuthorizationGate, AuthorizeMessage};
use crate::runtime::builder::{Builder, DriveBuild};
use crate::runtime::codec::Lowered;
use crate::runtime::copier::{ClosureCopier, DriveCopy};
use crate::runtime::gc_root::{DrivePin, GcRootPinner};
use crate::runtime::observation::{BroadcastObservation, ObservationFan};
use crate::runtime::store::{AllocateGeneration, Apply, CurrentDatabaseMarker, Store};
use crate::runtime::trace::{
    AuthorizationDecision, Plane, RecordWitness, SemaCommandTag, TraceLog, TraceWitness,
};

/// Set of ActorRefs the Nexus mail keeper holds. The Nexus's State
/// IS this set plus the lifecycle bookkeeping; the actor IS the mail
/// keeper.
#[derive(Clone)]
pub struct NexusActorRefs {
    pub authorization: ActorRef<AuthorizationGate>,
    pub builder: ActorRef<Builder>,
    pub copier: ActorRef<ClosureCopier>,
    pub activator: ActorRef<Activator>,
    pub gc_root: ActorRef<GcRootPinner>,
    pub store: ActorRef<Store>,
    pub fan: ActorRef<ObservationFan>,
    pub trace: ActorRef<TraceLog>,
}

/// One in-flight mail entry. Nexus keeps it while the SEMA round
/// trip is happening; on reply, Nexus transitions to `Replied` and
/// stamps the database marker.
#[derive(Clone, Debug)]
pub struct MailEntry {
    identifier: MessageIdentifier,
    input: Input,
    lifecycle: MailLifecycle,
    lifecycle_path: Vec<MailLifecycle>,
}

impl MailEntry {
    pub fn new(identifier: MessageIdentifier, input: Input) -> Self {
        Self {
            identifier,
            input,
            lifecycle: MailLifecycle::Sent,
            lifecycle_path: vec![MailLifecycle::Sent],
        }
    }

    pub fn identifier(&self) -> MessageIdentifier {
        self.identifier
    }

    pub fn input(&self) -> &Input {
        &self.input
    }

    pub fn lifecycle(&self) -> MailLifecycle {
        self.lifecycle.clone()
    }

    pub fn lifecycle_path(&self) -> &[MailLifecycle] {
        &self.lifecycle_path
    }

    pub fn transition(&mut self, next: MailLifecycle) {
        self.lifecycle = next.clone();
        self.lifecycle_path.push(next);
    }
}

/// Boxed `MessageSentHook` — handed to the Nexus through
/// `AttachSentHook`. The `Arc<Mutex<...>>` shape lets a test (or any
/// other observer) keep its own handle to the hook so it can read
/// captured events after firing.
pub type SharedSentHook = Arc<Mutex<dyn MessageSentHook<Error = Infallible> + Send>>;

/// Boxed `MessageProcessedHook` for the Output reply shape.
pub type SharedProcessedHook =
    Arc<Mutex<dyn MessageProcessedHook<Output, Error = Infallible> + Send>>;

/// Pushable lifecycle hook surface. Tests + downstream observers
/// attach concrete impls; the Nexus invokes the hooks synchronously
/// on each lifecycle transition (push-not-poll).
#[derive(Clone)]
pub struct NexusHooks {
    sent: Arc<Mutex<Vec<SharedSentHook>>>,
    processed: Arc<Mutex<Vec<SharedProcessedHook>>>,
}

impl NexusHooks {
    pub fn empty() -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            processed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn attach_sent(&self, hook: SharedSentHook) {
        self.sent.lock().expect("sent hooks").push(hook);
    }

    pub fn attach_processed(&self, hook: SharedProcessedHook) {
        self.processed.lock().expect("processed hooks").push(hook);
    }

    pub fn fire_sent(&self, event: MessageSent) {
        let hooks = self.sent.lock().expect("sent hooks");
        for hook in hooks.iter() {
            let mut guard = hook.lock().expect("hook lock");
            let _ = guard.message_sent(event.clone());
        }
    }

    pub fn fire_processed(&self, event: MessageProcessed<Output>) {
        let hooks = self.processed.lock().expect("processed hooks");
        for hook in hooks.iter() {
            let mut guard = hook.lock().expect("hook lock");
            let _ = guard.message_processed(event.clone());
        }
    }
}

impl Default for NexusHooks {
    fn default() -> Self {
        Self::empty()
    }
}

/// The Nexus mail keeper actor. State carries the actor refs, the
/// lifecycle hooks, the in-flight mail log, and the next message
/// identifier counter. Nexus IS the mail keeper noun.
pub struct NexusMailKeeper {
    refs: NexusActorRefs,
    hooks: NexusHooks,
    in_flight: Vec<MailEntry>,
    completed: Vec<MailEntry>,
    next_identifier: u64,
}

impl NexusMailKeeper {
    pub fn new(refs: NexusActorRefs, hooks: NexusHooks) -> Self {
        Self {
            refs,
            hooks,
            in_flight: Vec::new(),
            completed: Vec::new(),
            next_identifier: 1,
        }
    }

    pub fn refs(&self) -> &NexusActorRefs {
        &self.refs
    }

    pub fn hooks(&self) -> &NexusHooks {
        &self.hooks
    }

    pub fn in_flight(&self) -> &[MailEntry] {
        &self.in_flight
    }

    pub fn completed(&self) -> &[MailEntry] {
        &self.completed
    }

    fn allocate_identifier(&mut self) -> MessageIdentifier {
        let identifier = MessageIdentifier(self.next_identifier);
        self.next_identifier += 1;
        identifier
    }

    /// Accept one Input, drive it through the SEMA round trip, and
    /// return the stamped Output. The mail entry's lifecycle goes
    /// Sent -> Queued -> Processing -> Replied; lifecycle hooks fire
    /// at each transition.
    pub async fn handle_mail(&mut self, input: Input) -> Result<Output> {
        let identifier = self.allocate_identifier();
        let mut entry = MailEntry::new(identifier, input.clone());

        // Sent: Nexus has just accepted the mail.
        let sent_event = input.message_sent(identifier);
        self.hooks.fire_sent(sent_event);
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::MailSent {
                plane: Plane::NexusMailKeeper,
                identifier,
            }))
            .await;

        // Queued: about to dispatch.
        entry.transition(MailLifecycle::Queued);
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::MailQueued {
                plane: Plane::NexusMailKeeper,
                identifier,
            }))
            .await;

        // Processing: in the SEMA round trip.
        entry.transition(MailLifecycle::Processing);
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::MailProcessing {
                plane: Plane::NexusMailKeeper,
                identifier,
            }))
            .await;
        self.in_flight.push(entry.clone());

        let output_result = self.drive(input).await;

        match output_result {
            Ok(output) => {
                entry.transition(MailLifecycle::Replied);
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::MailReplied {
                        plane: Plane::NexusMailKeeper,
                        identifier,
                    }))
                    .await;
                self.complete(identifier, entry);
                let processed_event = MessageProcessed::new(identifier, output.clone());
                self.hooks.fire_processed(processed_event);
                Ok(output)
            }
            Err(error) => {
                entry.transition(MailLifecycle::Failed);
                self.complete(identifier, entry);
                Err(error)
            }
        }
    }

    fn complete(&mut self, identifier: MessageIdentifier, entry: MailEntry) {
        self.in_flight.retain(|item| item.identifier != identifier);
        self.completed.push(entry);
    }

    /// Drive a single Input through the deploy pipeline, returning
    /// the Output (already stamped with DatabaseMarker).
    async fn drive(&self, input: Input) -> Result<Output> {
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::InputReceived {
                plane: Plane::NexusMailKeeper,
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
            let marker = self.current_marker().await?;
            return Ok(Output::Rejected(RejectedReply {
                rejection_reason: RejectionReason::Unauthorized,
                database_marker: marker,
            }));
        }

        // Plan materialization
        let plan = request.into_plan_record();
        let _ = self
            .refs
            .trace
            .ask(RecordWitness(TraceWitness::PlanMaterialized {
                plane: Plane::NexusMailKeeper,
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
                plane: Plane::NexusMailKeeper,
            }))
            .await;
        let marker = self.current_marker().await?;
        Ok(Output::Accepted(crate::generated::AcceptedReply {
            deployment_identifier: deployment,
            database_marker: marker,
        }))
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
            Lowered::ForwardOnly(reply) => {
                let marker = self.current_marker().await?;
                let output = reply.stamped(marker);
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::OutputEmitted {
                        plane: Plane::NexusMailKeeper,
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
                let marker = self.current_marker().await?;
                let output = response.into_output(marker);
                let _ = self
                    .refs
                    .trace
                    .ask(RecordWitness(TraceWitness::OutputEmitted {
                        plane: Plane::NexusMailKeeper,
                    }))
                    .await;
                Ok(output)
            }
        }
    }

    async fn current_marker(&self) -> Result<DatabaseMarker> {
        let marker = self
            .refs
            .store
            .ask(CurrentDatabaseMarker)
            .await
            .map_err(|error| Error::ActorSend(format!("store/CurrentDatabaseMarker: {error}")))?;
        Ok(marker)
    }
}

impl Actor for NexusMailKeeper {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Dispatch an Input through the Nexus, returning the stamped
/// Output. Tests + the root use this as the public mail surface.
pub struct DispatchMail(pub Input);

impl Message<DispatchMail> for NexusMailKeeper {
    type Reply = Result<Output>;

    async fn handle(
        &mut self,
        message: DispatchMail,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_mail(message.0).await
    }
}

/// Snapshot the in-flight + completed mail entries. Tests subscribe
/// via this to assert lifecycle transitions.
pub struct MailLog;

impl Message<MailLog> for NexusMailKeeper {
    type Reply = std::result::Result<MailLogSnapshot, std::convert::Infallible>;

    async fn handle(
        &mut self,
        _message: MailLog,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(MailLogSnapshot {
            in_flight: self.in_flight.clone(),
            completed: self.completed.clone(),
        })
    }
}

/// In-flight + completed mail log snapshot, returned to tests.
#[derive(Clone, Debug)]
pub struct MailLogSnapshot {
    pub in_flight: Vec<MailEntry>,
    pub completed: Vec<MailEntry>,
}

impl MailLogSnapshot {
    pub fn in_flight(&self) -> &[MailEntry] {
        &self.in_flight
    }

    pub fn completed(&self) -> &[MailEntry] {
        &self.completed
    }

    pub fn find_completed(&self, identifier: MessageIdentifier) -> Option<&MailEntry> {
        self.completed
            .iter()
            .find(|entry| entry.identifier == identifier)
    }
}

/// Attach a `MessageSentHook` to the Nexus. The hook fires every
/// time Nexus accepts a new mail.
pub struct AttachSentHook(pub SharedSentHook);

impl Message<AttachSentHook> for NexusMailKeeper {
    type Reply = std::result::Result<(), std::convert::Infallible>;

    async fn handle(
        &mut self,
        message: AttachSentHook,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.hooks.attach_sent(message.0);
        Ok(())
    }
}

/// Attach a `MessageProcessedHook<Output>` to the Nexus. The hook
/// fires every time Nexus stamps a final Output.
pub struct AttachProcessedHook(pub SharedProcessedHook);

impl Message<AttachProcessedHook> for NexusMailKeeper {
    type Reply = std::result::Result<(), std::convert::Infallible>;

    async fn handle(
        &mut self,
        message: AttachProcessedHook,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.hooks.attach_processed(message.0);
        Ok(())
    }
}
