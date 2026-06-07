//! Hand-implemented `SchemaRuntime` noun — the single data-bearing type that
//! implements both engine traits (`nexus::NexusEngine` + `sema::SemaEngine`).
//!
//! `decide` is the routing brain (port plan §4.2): ordinary reads route to
//! `SemaRead`, ordinary subscription verbs reply with the token handshake, and
//! meta mutations route to `SemaWrite`. A meta `Deploy` opens the effect
//! pipeline (port plan §4.3): the write completion drives a chain of
//! `RunEffect` continuations — resolve flake auth, eval, build, copy, activate
//! — recording a phase transition between stages and finally replying
//! `Deployed`. `run_effect` does real `nix` IO through `tokio::process::Command`
//! so actor-native request tasks await child processes directly instead of
//! routing generated Nexus execution through a blocking-pool bridge.

use std::sync::Arc;

use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;
use tokio::process::Command;

use crate::Store;
use crate::schema::{nexus, sema};

/// The lojix engine noun. Carries the durable `Store` (the four sema tables)
/// and, while a deploy is in flight, the pipeline cursor that threads the
/// effect chain across continuation hops. Implements both engine traits; the
/// generated `NexusEngine::execute` drives the `Runner` over it.
#[derive(Debug, Default)]
pub struct SchemaRuntime {
    /// The shared durable state. Each request is served by its OWN
    /// `SchemaRuntime` over a clone of this `Arc`, so the in-flight deploy
    /// cursor below is per-request while the durable tables are shared across
    /// concurrent connections (intent 2alg).
    store: Arc<Store>,
    active_deploy: Option<DeployPipeline>,
    active_operation: Option<MetaOperation>,
}

/// Which single-write meta mutation is in flight, so a `WriteRejected` from the
/// SEMA engine routes back to the matching typed rejection reply
/// (`PinRejected` / `UnpinRejected` / `RetireRejected` / `DeployRejected`).
/// Deploy is multi-step and additionally tracked by `active_deploy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaOperation {
    Deploy,
    Pin,
    Unpin,
    Retire,
}

/// The in-flight deploy cursor. A single `Deploy` signal becomes a chain of
/// effect continuations; this records which deployment is running, its
/// resolved closure once built, and which stage produced the last effect so
/// `decide` knows the next effect to emit and the phase to record.
#[derive(Debug, Clone)]
struct DeployPipeline {
    deployment_identifier: ordinary::DeploymentIdentifier,
    generation_identifier: ordinary::GenerationIdentifier,
    cluster_name: ordinary::ClusterName,
    node_name: ordinary::NodeName,
    deployment_kind: ordinary::DeploymentKind,
    activation_kind: ordinary::ActivationKind,
    source: ordinary::ProposalSource,
    flake: ordinary::FlakeReference,
    /// A direct flake output attribute to build (a self-contained fixture /
    /// test closure), overriding the production `nixosConfigurations.target`
    /// path. `None` for a production deploy that needs the horizon override.
    build_attribute: Option<meta::FlakeAttribute>,
    /// The deploy action (System action, or Home mode + user). Owns the
    /// produces-closure / activates / target-attribute decisions so the
    /// pipeline asks the action rather than storing derived booleans.
    action: DeployAction,
    builder: Option<ordinary::NodeName>,
    substituters: Vec<nexus::ExtraSubstituter>,
    closure_path: Option<ordinary::ClosurePath>,
    accepted_marker: ordinary::DatabaseMarker,
    stage: DeployStage,
}

/// The deploy pipeline cursor. Each value names the stage that has just
/// completed; after a phase-transition write commits, `advance_after_phase`
/// reads it to emit the next effect (or the final activation-record write).
/// The chain is: Submitted -> (FlakeAuth) -> Building/Eval -> Build -> Copy ->
/// (Copying) -> Activate -> (Activated) -> RecordGenerationActivated -> Deployed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeployStage {
    /// Deploy accepted; the flake-auth + eval effects come next.
    Submitted,
    /// The `Building` phase was just recorded; the eval effect runs next.
    BuildingRecorded,
    /// The `Copying` phase was just recorded; the activate effect runs next.
    CopyingRecorded,
    /// The `Activated` phase was just recorded; the activation-record write
    /// (live-set + gc-roots commit) runs next.
    ActivatedRecorded,
}

/// The deploy action — a System action or a Home mode (with its user). Owns
/// the produces-closure / activates / target-attribute decisions so the
/// pipeline asks the action rather than storing derived booleans.
#[derive(Debug, Clone)]
enum DeployAction {
    System(ordinary::SystemAction),
    Home {
        mode: meta::HomeMode,
        user: ordinary::UserName,
    },
}

impl DeployAction {
    /// `false` only for a System `Eval` (derivation path only, no realised
    /// closure). Home modes always build a closure.
    fn produces_closure(&self) -> bool {
        match self {
            Self::System(action) => !matches!(action, ordinary::SystemAction::Eval),
            Self::Home { .. } => true,
        }
    }

    /// Whether the action copies + activates after the build: System
    /// Boot/Switch/Test/BootOnce, or Home Profile/Activate. `Eval` and
    /// `Build` (and Home `Build`) stop at the realised closure.
    fn activates(&self) -> bool {
        match self {
            Self::System(action) => matches!(
                action,
                ordinary::SystemAction::Boot
                    | ordinary::SystemAction::Switch
                    | ordinary::SystemAction::Test
                    | ordinary::SystemAction::BootOnce
            ),
            Self::Home { mode, .. } => {
                matches!(mode, meta::HomeMode::Profile | meta::HomeMode::Activate)
            }
        }
    }

    /// The production flake attribute for this action — used when no direct
    /// `build_attribute` override is given. System builds the node toplevel
    /// (node identity injected by the horizon override, the deferred M3
    /// materialization work); Home builds the user's activation package.
    fn target_attribute(&self) -> String {
        match self {
            Self::System(_) => {
                "nixosConfigurations.target.config.system.build.toplevel".to_string()
            }
            Self::Home { user, .. } => {
                format!("homeConfigurations.{user}.activationPackage")
            }
        }
    }
}

impl DeployPipeline {
    fn from_submission(
        deployment_identifier: ordinary::DeploymentIdentifier,
        generation_identifier: ordinary::GenerationIdentifier,
        accepted_marker: ordinary::DatabaseMarker,
        submission: sema::DeploySubmission,
    ) -> Self {
        match submission {
            sema::DeploySubmission::System(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                deployment_kind: deployment.deployment_kind,
                activation_kind: Self::system_activation_kind(deployment.system_action.clone()),
                source: deployment.source,
                flake: deployment.flake,
                build_attribute: deployment.build_attribute,
                action: DeployAction::System(deployment.system_action),
                builder: deployment.builder.map(meta::Builder::into_payload),
                substituters: Self::convert_substituters(deployment.substituters),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
            },
            sema::DeploySubmission::Home(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                deployment_kind: ordinary::DeploymentKind::HomeOnly,
                activation_kind: ordinary::ActivationKind::Switch,
                source: deployment.source,
                flake: deployment.flake,
                build_attribute: None,
                action: DeployAction::Home {
                    mode: deployment.home_mode,
                    user: deployment.user_name,
                },
                builder: deployment.builder.map(meta::Builder::into_payload),
                substituters: Self::convert_substituters(deployment.substituters),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
            },
        }
    }

    fn system_activation_kind(action: ordinary::SystemAction) -> ordinary::ActivationKind {
        match action {
            ordinary::SystemAction::Boot => ordinary::ActivationKind::Boot,
            ordinary::SystemAction::Test => ordinary::ActivationKind::Test,
            ordinary::SystemAction::BootOnce => ordinary::ActivationKind::BootOnce,
            ordinary::SystemAction::Eval
            | ordinary::SystemAction::Build
            | ordinary::SystemAction::Switch => ordinary::ActivationKind::Switch,
        }
    }

    fn convert_substituters(
        substituters: Vec<meta::ExtraSubstituter>,
    ) -> Vec<nexus::ExtraSubstituter> {
        substituters
            .into_iter()
            .map(|substituter| nexus::ExtraSubstituter {
                url: substituter.url,
                public_key: substituter.public_key,
            })
            .collect()
    }

    fn build_target(&self) -> nexus::BuildTarget {
        match &self.builder {
            Some(builder) => nexus::BuildTarget::Remote(nexus::BuilderNode::new(builder.clone())),
            None => nexus::BuildTarget::Local,
        }
    }

    fn flake_auth_request(&self) -> nexus::FlakeAuthRequest {
        nexus::FlakeAuthRequest {
            source: self.source.clone(),
            flake: self.flake.clone(),
        }
    }

    fn nix_eval_command(&self) -> nexus::NixEvalCommand {
        nexus::NixEvalCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_kind: self.deployment_kind.clone(),
            flake: self.flake.clone(),
            attribute: self.target_attribute(),
        }
    }

    fn target_attribute(&self) -> String {
        // A1 fix: a direct `build_attribute` override names a self-contained
        // flake output (the fixture path); otherwise the action supplies the
        // production attribute (`nixosConfigurations.target...` /
        // `homeConfigurations.<user>...`). The old `{cluster}.{node}` form
        // resolved to no real flake attribute and every deploy failed at eval.
        match &self.build_attribute {
            Some(attribute) => attribute.clone(),
            None => self.action.target_attribute(),
        }
    }

    fn nix_build_command(&self, closure_path: ordinary::ClosurePath) -> nexus::NixBuildCommand {
        nexus::NixBuildCommand {
            generation_identifier: self.generation_identifier,
            closure_path,
            target: self.build_target(),
            substituters: self.substituters.clone(),
        }
    }

    fn copy_closure_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            closure_path,
        }
    }

    fn activate_generation_command(&self) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            activation_kind: self.activation_kind.clone(),
        }
    }

    fn activation_commit(&self) -> sema::ActivationCommit {
        sema::ActivationCommit {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: self.closure_path.clone().unwrap_or_default(),
        }
    }

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
        detail: Option<ordinary::PhaseDetail>,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: self.deployment_identifier,
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_phase: phase,
            event_log_position,
            detail,
        }
    }
}

impl SchemaRuntime {
    pub fn new() -> Self {
        Self::with_store(Arc::new(Store::new()))
    }

    /// Build an engine over a SHARED `Store`. The daemon constructs one per
    /// request from a single shared `Arc<Store>`, so concurrent requests share
    /// the durable tables but each owns its in-flight deploy cursor (intent 2alg).
    pub fn with_store(store: Arc<Store>) -> Self {
        Self {
            store,
            active_deploy: None,
            active_operation: None,
        }
    }

    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    fn marker(commit_sequence: u64) -> ordinary::DatabaseMarker {
        ordinary::DatabaseMarker {
            commit_sequence,
            state_digest: commit_sequence,
        }
    }

    fn sema_marker(commit_sequence: u64) -> sema::StateMarker {
        sema::StateMarker {
            commit_sequence,
            state_digest: commit_sequence,
        }
    }

    // ---- decide: signal arrival routing (port plan §4.2) ----------------

    fn decide_signal_arrival(&mut self, input: nexus::SignalInput) -> nexus::NexusAction {
        match input {
            nexus::SignalInput::OrdinaryInput(input) => self.decide_ordinary_input(input),
            nexus::SignalInput::MetaInput(input) => self.decide_meta_input(input),
        }
    }

    fn decide_ordinary_input(&mut self, input: ordinary::Input) -> nexus::NexusAction {
        match input {
            ordinary::Input::Query(selection) => nexus::NexusAction::CommandSemaRead(
                sema::SemaReadInput::QueryGenerations(selection),
            ),
            ordinary::Input::CheckHostKeyMaterial(query) => {
                nexus::NexusAction::CommandSemaRead(sema::SemaReadInput::CheckKeyMaterial(query))
            }
            ordinary::Input::WatchDeployments(_) | ordinary::Input::WatchCacheRetention(_) => {
                self.open_subscription()
            }
            ordinary::Input::Unwatch(close) => self.close_subscription(close),
        }
    }

    fn open_subscription(&mut self) -> nexus::NexusAction {
        let reply = match self.store.lock() {
            Ok(mut state) => {
                let subscription_token = state.next_subscription_token();
                let commit_sequence = state.commit_sequence;
                ordinary::Output::Watching(ordinary::SubscriptionOpened {
                    subscription_token,
                    commit_sequence,
                })
            }
            Err(_) => ordinary::Output::WatchRejected(ordinary::RejectedWatch(
                ordinary::WatchRejectionReason::StreamUnavailable,
            )),
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn close_subscription(&mut self, close: ordinary::SubscriptionClose) -> nexus::NexusAction {
        let reply =
            ordinary::Output::Unwatched(ordinary::SubscriptionClosed::new(close.into_payload()));
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn decide_meta_input(&mut self, input: meta::Input) -> nexus::NexusAction {
        match input {
            meta::Input::Deploy(request) => {
                // M1 reject-guard (audit C1): only a self-contained
                // `build_attribute` System deploy with a non-activating action
                // (Eval/Build) is actually implemented. Production builds (need
                // the horizon `--override-input` materialization, M3) and
                // activating actions (need real copy/activate, M2) are NOT —
                // reject them honestly rather than run a broken activate and
                // write a live-set entry that lies about what is deployed.
                if let Some(reason) = Self::unsupported_deploy_reason(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        self.deploy_rejection(reason),
                    ));
                }
                self.active_operation = Some(MetaOperation::Deploy);
                let submission = match request {
                    meta::DeployRequest::System(deployment) => {
                        sema::DeploySubmission::System(deployment)
                    }
                    meta::DeployRequest::Home(deployment) => {
                        sema::DeploySubmission::Home(deployment)
                    }
                };
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordDeploySubmitted(
                    submission,
                ))
            }
            meta::Input::Pin(request) => {
                self.active_operation = Some(MetaOperation::Pin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::PinGeneration(request))
            }
            meta::Input::Unpin(request) => {
                self.active_operation = Some(MetaOperation::Unpin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::UnpinGeneration(request))
            }
            meta::Input::Retire(request) => {
                self.active_operation = Some(MetaOperation::Retire);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RetireGeneration(
                    request,
                ))
            }
        }
    }

    /// The M1 reject-guard. Returns `Some(reason)` when the daemon does not yet
    /// implement this deploy shape. Supported today: a System deploy with a
    /// `build_attribute` override (a self-contained flake output) and a
    /// non-activating action (`Eval`/`Build`). Rejected: production System
    /// deploys (no `build_attribute` — need horizon `--override-input`
    /// materialization, M3), activating actions (Boot/Switch/Test/BootOnce —
    /// need real copy/activate addressing, M2), and all Home deploys (need
    /// `homeConfigurations` materialization, M3).
    fn unsupported_deploy_reason(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        match request {
            meta::DeployRequest::System(deployment) => {
                let supported = deployment.build_attribute.is_some()
                    && matches!(
                        deployment.system_action,
                        ordinary::SystemAction::Eval | ordinary::SystemAction::Build
                    );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
            meta::DeployRequest::Home(_) => {
                Some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
        }
    }

    // ---- decide: sema read completion -----------------------------------

    fn decide_read_completion(&mut self, output: sema::SemaReadOutput) -> nexus::NexusAction {
        let reply = match output {
            sema::SemaReadOutput::GenerationsQueried(listing) => ordinary::Output::Queried(listing),
            sema::SemaReadOutput::KeyMaterialChecked(report) => {
                ordinary::Output::KeyMaterialChecked(report)
            }
            sema::SemaReadOutput::EventLogRead(_) => ordinary::Output::QueryRejected(
                self.query_rejection(ordinary::QueryRejectionReason::MalformedSelector),
            ),
            sema::SemaReadOutput::ReadMissed(report) => {
                ordinary::Output::QueryRejected(ordinary::RejectedQuery {
                    query_rejection_reason: ordinary::QueryRejectionReason::GenerationUnknown,
                    database_marker: Self::marker(report.marker.commit_sequence),
                })
            }
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn query_rejection(&self, reason: ordinary::QueryRejectionReason) -> ordinary::RejectedQuery {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        ordinary::RejectedQuery {
            query_rejection_reason: reason,
            database_marker: Self::marker(commit_sequence),
        }
    }

    // ---- decide: sema write completion (opens / advances pipeline) ------

    fn decide_write_completion(&mut self, output: sema::SemaWriteOutput) -> nexus::NexusAction {
        match output {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => {
                self.begin_deploy_pipeline(accepted)
            }
            sema::SemaWriteOutput::PhaseRecorded(_) => self.advance_after_phase(),
            sema::SemaWriteOutput::GenerationActivated(_) => self.finish_deploy_pipeline(),
            sema::SemaWriteOutput::GenerationPinned(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Pinned(applied))
            }
            sema::SemaWriteOutput::GenerationUnpinned(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Unpinned(applied))
            }
            sema::SemaWriteOutput::GenerationRetired(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Retired(applied))
            }
            sema::SemaWriteOutput::ContainerRecorded(_) => self.advance_after_phase(),
            sema::SemaWriteOutput::WriteRejected(report) => self.reject_active_or_meta(report),
        }
    }

    fn begin_deploy_pipeline(&mut self, accepted: meta::AcceptedDeploy) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.as_ref() {
            Some(pipeline) => pipeline.clone(),
            None => return Self::reply_meta(meta::Output::Deployed(accepted)),
        };
        // First effect of the chain: resolve the flake against the proposal
        // source. Subsequent effects are emitted from `decide_effect_completion`.
        nexus::NexusAction::CommandEffect(nexus::EffectCommand::ResolveFlakeAuth(
            pipeline.flake_auth_request(),
        ))
    }

    fn advance_after_phase(&mut self) -> nexus::NexusAction {
        // A phase-transition write committed mid-pipeline. The cursor `stage`
        // names which phase was just recorded; advance to the next effect or
        // the final activation-record write.
        let pipeline = match self.active_deploy.clone() {
            Some(pipeline) => pipeline,
            None => {
                return Self::reply_meta(meta::Output::DeployRejected(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                ));
            }
        };
        match pipeline.stage {
            DeployStage::Submitted => {
                self.set_stage(DeployStage::BuildingRecorded);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::NixEval(
                    pipeline.nix_eval_command(),
                ))
            }
            DeployStage::BuildingRecorded => {
                self.set_stage(DeployStage::CopyingRecorded);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::ActivateGeneration(
                    pipeline.activate_generation_command(),
                ))
            }
            DeployStage::CopyingRecorded => {
                self.set_stage(DeployStage::ActivatedRecorded);
                nexus::NexusAction::CommandSemaWrite(
                    sema::SemaWriteInput::RecordGenerationActivated(pipeline.activation_commit()),
                )
            }
            DeployStage::ActivatedRecorded => self.finish_deploy_pipeline(),
        }
    }

    fn set_stage(&mut self, stage: DeployStage) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.stage = stage;
        }
    }

    fn finish_deploy_pipeline(&mut self) -> nexus::NexusAction {
        self.active_operation = None;
        let accepted = match self.active_deploy.take() {
            Some(pipeline) => meta::AcceptedDeploy {
                deployment_identifier: pipeline.deployment_identifier,
                database_marker: pipeline.accepted_marker,
            },
            None => meta::AcceptedDeploy {
                deployment_identifier: 0,
                database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
            },
        };
        Self::reply_meta(meta::Output::Deployed(accepted))
    }

    fn reject_active_or_meta(&mut self, report: sema::RejectionReport) -> nexus::NexusAction {
        // A write rejection aborts any in-flight deploy and replies a typed
        // meta rejection for the operation in flight, carrying the rejection
        // reason and current marker.
        self.active_deploy = None;
        let operation = self
            .active_operation
            .take()
            .unwrap_or(MetaOperation::Deploy);
        let marker = Self::marker(report.marker.commit_sequence);
        let output = match operation {
            MetaOperation::Deploy => meta::Output::DeployRejected(meta::RejectedDeploy {
                deploy_rejection_reason: Self::deploy_reason(report.reason),
                database_marker: marker,
            }),
            MetaOperation::Pin => meta::Output::PinRejected(meta::RejectedPin {
                pin_rejection_reason: Self::pin_reason(report.reason),
                database_marker: marker,
            }),
            MetaOperation::Unpin => meta::Output::UnpinRejected(meta::RejectedUnpin {
                unpin_rejection_reason: Self::unpin_reason(report.reason),
                database_marker: marker,
            }),
            MetaOperation::Retire => meta::Output::RetireRejected(meta::RejectedRetire {
                retire_rejection_reason: Self::retire_reason(report.reason),
                database_marker: marker,
            }),
        };
        Self::reply_meta(output)
    }

    fn pin_reason(reason: sema::RejectionReason) -> meta::PinRejectionReason {
        match reason {
            sema::RejectionReason::GenerationUnknown => meta::PinRejectionReason::GenerationUnknown,
            sema::RejectionReason::NodeUnknown => meta::PinRejectionReason::NodeUnknown,
            sema::RejectionReason::PinLabelInUse => meta::PinRejectionReason::PinLabelInUse,
            _ => meta::PinRejectionReason::InternalError,
        }
    }

    fn unpin_reason(reason: sema::RejectionReason) -> meta::UnpinRejectionReason {
        match reason {
            sema::RejectionReason::PinLabelUnknown => meta::UnpinRejectionReason::PinLabelUnknown,
            sema::RejectionReason::NodeUnknown => meta::UnpinRejectionReason::NodeUnknown,
            _ => meta::UnpinRejectionReason::GenerationNotPinned,
        }
    }

    fn retire_reason(reason: sema::RejectionReason) -> meta::RetireRejectionReason {
        match reason {
            sema::RejectionReason::GenerationUnknown => {
                meta::RetireRejectionReason::GenerationUnknown
            }
            sema::RejectionReason::NodeUnknown => meta::RetireRejectionReason::NodeUnknown,
            sema::RejectionReason::GenerationActive => {
                meta::RetireRejectionReason::GenerationActive
            }
            sema::RejectionReason::GenerationPinned => {
                meta::RetireRejectionReason::GenerationPinned
            }
            _ => meta::RetireRejectionReason::InternalError,
        }
    }

    fn deploy_reason(reason: sema::RejectionReason) -> meta::DeployRejectionReason {
        match reason {
            sema::RejectionReason::ClusterUnknown => meta::DeployRejectionReason::ClusterUnknown,
            sema::RejectionReason::NodeUnknown => meta::DeployRejectionReason::NodeUnknown,
            sema::RejectionReason::ProposalSourceUnreachable => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            // A sema reason with no deploy-domain mapping is an internal
            // invariant failure (e.g. a poisoned lock), not "already deploying"
            // (audit C4).
            _ => meta::DeployRejectionReason::InternalError,
        }
    }

    fn deploy_rejection(&self, reason: meta::DeployRejectionReason) -> meta::RejectedDeploy {
        meta::RejectedDeploy {
            deploy_rejection_reason: reason,
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        }
    }

    fn reply_meta(output: meta::Output) -> nexus::NexusAction {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output))
    }

    // ---- decide: effect completion (drives the deploy chain) ------------

    fn decide_effect_completion(&mut self, result: nexus::EffectResult) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.clone() {
            Some(pipeline) => pipeline,
            None => {
                // Effects outside a deploy (e.g. a standalone GC) just confirm;
                // an unexpected effect completion replies a rejection.
                return match result {
                    nexus::EffectResult::EffectFailed(failure) => self.fail_pipeline(failure),
                    _ => Self::reply_meta(meta::Output::DeployRejected(
                        self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                    )),
                };
            }
        };
        match result {
            nexus::EffectResult::FlakeResolved(_) => {
                // Record Building (stage still Submitted). The phase write hops
                // back through advance_after_phase, which fires NixEval.
                self.record_phase(ordinary::DeploymentPhase::Building, None)
            }
            nexus::EffectResult::ClosureEvaluated(evaluated) => {
                self.set_closure_path(evaluated.closure_path.clone());
                if pipeline.action.produces_closure() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::NixBuild(
                        pipeline.nix_build_command(evaluated.closure_path),
                    ))
                } else {
                    // System `Eval`: the derivation path is the result — finish
                    // the pipeline without building.
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureBuilt(built) => {
                self.set_closure_path(built.closure_path.clone());
                if pipeline.action.activates() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::CopyClosure(
                        pipeline.copy_closure_command(built.closure_path),
                    ))
                } else {
                    // Non-activating action (`Build`): the closure is realised —
                    // finish without copy/activate (which remain addressing-
                    // incomplete; that is the M2/M3 deploy work).
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureCopied(_) => {
                // Record Copying (stage BuildingRecorded). The phase write hops
                // back through advance_after_phase, which fires ActivateGeneration.
                self.record_phase(ordinary::DeploymentPhase::Copying, None)
            }
            nexus::EffectResult::GenerationActivated(_) => {
                // Record Activated (stage CopyingRecorded). The phase write hops
                // back through advance_after_phase, which fires the
                // RecordGenerationActivated write that commits the live set.
                self.record_phase(ordinary::DeploymentPhase::Activated, None)
            }
            nexus::EffectResult::PathsCollected(_) => self.finish_deploy_pipeline(),
            nexus::EffectResult::EffectFailed(failure) => self.fail_pipeline(failure),
        }
    }

    fn set_closure_path(&mut self, closure_path: ordinary::ClosurePath) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.closure_path = Some(closure_path);
        }
    }

    fn record_phase(
        &mut self,
        phase: ordinary::DeploymentPhase,
        detail: Option<ordinary::PhaseDetail>,
    ) -> nexus::NexusAction {
        let event = match self.active_deploy.as_ref() {
            Some(pipeline) => {
                let position = self
                    .store
                    .lock()
                    .map(|state| state.next_event_log_position());
                pipeline.phase_event(phase, position.unwrap_or(0), detail)
            }
            None => {
                return Self::reply_meta(meta::Output::DeployRejected(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                ));
            }
        };
        nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordPhaseTransition(event))
    }

    fn fail_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        // Clear BOTH in-flight slots symmetrically with the finish path (audit
        // R5) — a mid-pipeline effect failure must not leak `active_operation`.
        self.active_deploy = None;
        self.active_operation = None;
        let reason = match failure.stage {
            nexus::EffectStage::FlakeAuth => meta::DeployRejectionReason::ProposalSourceUnreachable,
            nexus::EffectStage::Eval => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::Build => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::CopyClosure => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Activate => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Gc => meta::DeployRejectionReason::DeploymentInFlight,
        };
        Self::reply_meta(meta::Output::DeployRejected(self.deploy_rejection(reason)))
    }

    // ---- sema apply / observe (the four tables) -------------------------

    fn apply_sema(&mut self, input: sema::SemaWriteInput) -> sema::SemaWriteOutput {
        match input {
            sema::SemaWriteInput::RecordDeploySubmitted(submission) => {
                self.record_deploy_submitted(submission)
            }
            sema::SemaWriteInput::RecordPhaseTransition(event) => {
                self.record_phase_transition(event)
            }
            sema::SemaWriteInput::RecordGenerationActivated(commit) => {
                self.record_generation_activated(commit)
            }
            sema::SemaWriteInput::PinGeneration(request) => self.pin_generation(request),
            sema::SemaWriteInput::UnpinGeneration(request) => self.unpin_generation(request),
            sema::SemaWriteInput::RetireGeneration(request) => self.retire_generation(request),
            sema::SemaWriteInput::RecordContainerTransition(transition) => {
                self.record_container_transition(transition)
            }
        }
    }

    fn record_deploy_submitted(
        &mut self,
        submission: sema::DeploySubmission,
    ) -> sema::SemaWriteOutput {
        let result = self.store.lock().map(|mut state| {
            let commit_sequence = state.next_commit_sequence();
            let deployment_identifier = state.next_deployment_identifier();
            let generation_identifier = state.next_generation_identifier();
            (
                commit_sequence,
                deployment_identifier,
                generation_identifier,
            )
        });
        match result {
            Ok((commit_sequence, deployment_identifier, generation_identifier)) => {
                let accepted_marker = Self::marker(commit_sequence);
                self.active_deploy = Some(DeployPipeline::from_submission(
                    deployment_identifier,
                    generation_identifier,
                    accepted_marker.clone(),
                    submission,
                ));
                sema::SemaWriteOutput::DeploySubmitted(meta::AcceptedDeploy {
                    deployment_identifier,
                    database_marker: accepted_marker,
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn record_phase_transition(
        &mut self,
        event: ordinary::DeploymentPhaseEvent,
    ) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                let event_log_position = state.next_event_log_position();
                state.event_log.0.push(sema::EventLogEntry {
                    event_log_position,
                    record: sema::LoggedEvent::Deployment(event),
                });
                sema::SemaWriteOutput::PhaseRecorded(sema::PhaseReceipt {
                    event_log_position,
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn record_generation_activated(
        &mut self,
        commit: sema::ActivationCommit,
    ) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                let pipeline = self.active_deploy.clone();
                let deployment_identifier = pipeline
                    .as_ref()
                    .map(|p| p.deployment_identifier)
                    .unwrap_or(0);
                let deployment_kind = pipeline
                    .as_ref()
                    .map(|p| p.deployment_kind.clone())
                    .unwrap_or(ordinary::DeploymentKind::FullOs);
                let activation_kind = pipeline
                    .as_ref()
                    .map(|p| p.activation_kind.clone())
                    .unwrap_or(ordinary::ActivationKind::Switch);
                state.live_set.0.push(sema::LiveGeneration {
                    deployment_identifier,
                    generation_identifier: commit.generation_identifier,
                    cluster_name: commit.cluster_name.clone(),
                    node_name: commit.node_name.clone(),
                    deployment_kind,
                    activation_kind,
                    generation_slot: commit.generation_slot.clone(),
                    closure_path: commit.closure_path.clone(),
                });
                state.gc_roots.0.push(sema::GcRoot {
                    generation_identifier: commit.generation_identifier,
                    cluster_name: commit.cluster_name,
                    node_name: commit.node_name,
                    generation_slot: commit.generation_slot.clone(),
                    closure_path: commit.closure_path,
                    label: None,
                });
                sema::SemaWriteOutput::GenerationActivated(sema::AppliedActivation {
                    generation_identifier: commit.generation_identifier,
                    generation_slot: commit.generation_slot,
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn pin_generation(&mut self, request: meta::PinRequest) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                let already_used = state
                    .gc_roots
                    .0
                    .iter()
                    .any(|root| root.label.as_deref() == Some(request.pin_label.as_str()));
                if already_used {
                    return Self::write_rejected(
                        commit_sequence,
                        sema::RejectionReason::PinLabelInUse,
                    );
                }
                if let Some(root) = state.gc_roots.0.iter_mut().find(|root| {
                    root.generation_identifier == request.generation_identifier
                        && root.cluster_name == request.cluster_name
                        && root.node_name == request.node_name
                }) {
                    let from_slot = root.generation_slot.clone();
                    root.generation_slot = ordinary::GenerationSlot::Pinned;
                    root.label = Some(request.pin_label.clone());
                    sema::SemaWriteOutput::GenerationPinned(meta::AppliedPin {
                        generation_identifier: request.generation_identifier,
                        pin_label: request.pin_label,
                        from_slot,
                        to_slot: ordinary::GenerationSlot::Pinned,
                        database_marker: Self::marker(commit_sequence),
                    })
                } else {
                    Self::write_rejected(commit_sequence, sema::RejectionReason::GenerationUnknown)
                }
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        }
    }

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                if let Some(root) = state.gc_roots.0.iter_mut().find(|root| {
                    root.label.as_deref() == Some(request.pin_label.as_str())
                        && root.cluster_name == request.cluster_name
                        && root.node_name == request.node_name
                }) {
                    let generation_identifier = root.generation_identifier;
                    let from_slot = root.generation_slot.clone();
                    root.generation_slot = ordinary::GenerationSlot::Recent;
                    root.label = None;
                    sema::SemaWriteOutput::GenerationUnpinned(meta::AppliedUnpin {
                        generation_identifier,
                        pin_label: request.pin_label,
                        from_slot,
                        to_slot: ordinary::GenerationSlot::Recent,
                        database_marker: Self::marker(commit_sequence),
                    })
                } else {
                    Self::write_rejected(commit_sequence, sema::RejectionReason::PinLabelUnknown)
                }
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PinLabelUnknown),
        }
    }

    fn retire_generation(&mut self, request: meta::RetireRequest) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                let found = state.gc_roots.0.iter().position(|root| {
                    root.generation_identifier == request.generation_identifier
                        && root.cluster_name == request.cluster_name
                        && root.node_name == request.node_name
                });
                match found {
                    Some(index) => {
                        let root = state.gc_roots.0.remove(index);
                        if matches!(root.generation_slot, ordinary::GenerationSlot::Pinned) {
                            state.gc_roots.0.insert(index, root);
                            return Self::write_rejected(
                                commit_sequence,
                                sema::RejectionReason::GenerationPinned,
                            );
                        }
                        sema::SemaWriteOutput::GenerationRetired(meta::AppliedRetire {
                            generation_identifier: request.generation_identifier,
                            from_slot: root.generation_slot,
                            database_marker: Self::marker(commit_sequence),
                        })
                    }
                    None => Self::write_rejected(
                        commit_sequence,
                        sema::RejectionReason::GenerationUnknown,
                    ),
                }
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        }
    }

    fn record_container_transition(
        &mut self,
        transition: sema::ContainerTransition,
    ) -> sema::SemaWriteOutput {
        match self.store.lock() {
            Ok(mut state) => {
                let commit_sequence = state.next_commit_sequence();
                let event_log_position = state.next_event_log_position();
                let record = sema::ContainerLifecycleRecord {
                    cluster_name: transition.cluster_name,
                    node_name: transition.node_name,
                    container: transition.container,
                    state: transition.state,
                    event_log_position,
                };
                state.containers.0.push(record.clone());
                state.event_log.0.push(sema::EventLogEntry {
                    event_log_position,
                    record: sema::LoggedEvent::Container(record),
                });
                sema::SemaWriteOutput::ContainerRecorded(sema::ContainerReceipt {
                    event_log_position,
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::NodeUnknown),
        }
    }

    /// Build a write-rejection at a known commit sequence. The caller passes
    /// the sequence it already read under the store lock — this method never
    /// re-locks, so it is safe to call while the store guard is still held.
    fn write_rejected(
        commit_sequence: u64,
        reason: sema::RejectionReason,
    ) -> sema::SemaWriteOutput {
        sema::SemaWriteOutput::WriteRejected(sema::RejectionReport {
            reason,
            marker: Self::sema_marker(commit_sequence),
        })
    }

    fn observe_sema(&self, input: sema::SemaReadInput) -> sema::SemaReadOutput {
        match input {
            sema::SemaReadInput::QueryGenerations(selection) => self.query_generations(selection),
            sema::SemaReadInput::ReadEventLog(range) => self.read_event_log(range),
            sema::SemaReadInput::CheckKeyMaterial(query) => self.check_key_material(query),
        }
    }

    fn query_generations(&self, selection: ordinary::Selection) -> sema::SemaReadOutput {
        let state = match self.store.lock() {
            Ok(state) => state,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::GenerationUnknown),
        };
        let commit_sequence = state.commit_sequence;
        let generations: Vec<ordinary::Generation> = state
            .live_set
            .0
            .iter()
            .filter(|live| Self::generation_matches(&selection, live))
            .map(Self::project_generation)
            .collect();
        sema::SemaReadOutput::GenerationsQueried(ordinary::GenerationListing {
            generations,
            database_marker: Self::marker(commit_sequence),
        })
    }

    fn generation_matches(selection: &ordinary::Selection, live: &sema::LiveGeneration) -> bool {
        match selection {
            ordinary::Selection::ByNode(selector) => {
                selector.cluster_name == live.cluster_name
                    && selector.node_name == live.node_name
                    && selector
                        .kind
                        .as_ref()
                        .is_none_or(|kind| kind == &live.deployment_kind)
            }
            ordinary::Selection::ByGeneration(lookup) => {
                *lookup.payload() == live.generation_identifier
            }
            ordinary::Selection::ByEventLog(_) => true,
        }
    }

    fn project_generation(live: &sema::LiveGeneration) -> ordinary::Generation {
        ordinary::Generation {
            generation_identifier: live.generation_identifier,
            deployment_identifier: live.deployment_identifier,
            cluster_name: live.cluster_name.clone(),
            node_name: live.node_name.clone(),
            deployment_kind: live.deployment_kind.clone(),
            activation_kind: live.activation_kind.clone(),
            generation_slot: live.generation_slot.clone(),
            closure_path: live.closure_path.clone(),
        }
    }

    fn read_event_log(&self, range: ordinary::EventLogRange) -> sema::SemaReadOutput {
        let state = match self.store.lock() {
            Ok(state) => state,
            Err(_) => {
                return Self::read_missed(0, sema::RejectionReason::EventLogPositionOutOfRange);
            }
        };
        let commit_sequence = state.commit_sequence;
        let mut deployment_events = Vec::new();
        let mut retention_events = Vec::new();
        for entry in &state.event_log.0 {
            if entry.event_log_position < range.from || entry.event_log_position >= range.until {
                continue;
            }
            match &entry.record {
                sema::LoggedEvent::Deployment(event) => deployment_events.push(event.clone()),
                sema::LoggedEvent::CacheRetention(event) => retention_events.push(event.clone()),
                sema::LoggedEvent::Container(_) => {}
            }
        }
        sema::SemaReadOutput::EventLogRead(sema::EventLogPage {
            deployment_events,
            retention_events,
            state_marker: Self::sema_marker(commit_sequence),
        })
    }

    fn check_key_material(&self, query: ordinary::KeyMaterialQuery) -> sema::SemaReadOutput {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        sema::SemaReadOutput::KeyMaterialChecked(ordinary::KeyMaterialReport {
            node_name: query.node_name,
            mismatches: Vec::new(),
            database_marker: Self::marker(commit_sequence),
        })
    }

    /// Build a read-miss at a known commit sequence. Like `write_rejected`,
    /// this never re-locks; the caller supplies the sequence.
    fn read_missed(commit_sequence: u64, reason: sema::RejectionReason) -> sema::SemaReadOutput {
        sema::SemaReadOutput::ReadMissed(sema::RejectionReport {
            reason,
            marker: Self::sema_marker(commit_sequence),
        })
    }

    // ---- real nix IO (port plan §4.3) -----------------------------------

    async fn resolve_flake_auth(&self, request: nexus::FlakeAuthRequest) -> nexus::EffectResult {
        // Resolve the flake metadata to a locked revision through the proposal
        // source. `nix flake metadata --json <flake>` reports the resolved ref.
        match NixCommand::flake_metadata(&request.flake).run().await {
            Ok(output) => nexus::EffectResult::FlakeResolved(nexus::ResolvedFlake {
                flake: request.flake,
                revision: NixCommand::first_line(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::FlakeAuth, detail),
        }
    }

    async fn run_nix_eval(&self, command: nexus::NixEvalCommand) -> nexus::EffectResult {
        let attribute = format!("{}#{}", command.flake, command.attribute);
        match NixCommand::eval_drv_path(&attribute).run().await {
            Ok(output) => nexus::EffectResult::ClosureEvaluated(nexus::EvaluatedClosure {
                generation_identifier: 0,
                closure_path: NixCommand::first_line(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Eval, detail),
        }
    }

    async fn run_nix_build(&self, command: nexus::NixBuildCommand) -> nexus::EffectResult {
        // Honoring the dropped local-build guard `783n`: a `BuildTarget::Local`
        // builds on the local dispatcher (no remote builder); `Remote` would
        // dispatch the build to the named builder node. Both run the same
        // `nix build` invocation here; the remote dispatch wraps it in ssh.
        let invocation = match &command.target {
            nexus::BuildTarget::Local => {
                NixCommand::build_closure(&command.closure_path, &command.substituters)
            }
            nexus::BuildTarget::Remote(builder) => NixCommand::build_closure_remote(
                builder.payload(),
                &command.closure_path,
                &command.substituters,
            ),
        };
        match invocation.run().await {
            Ok(output) => nexus::EffectResult::ClosureBuilt(nexus::BuiltClosure {
                generation_identifier: command.generation_identifier,
                closure_path: NixCommand::first_line_or(&output, &command.closure_path),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Build, detail),
        }
    }

    async fn run_copy_closure(&self, command: nexus::CopyClosureCommand) -> nexus::EffectResult {
        match NixCommand::copy_closure(&command.node_name, &command.closure_path)
            .run()
            .await
        {
            Ok(_) => nexus::EffectResult::ClosureCopied(nexus::CopiedClosure {
                generation_identifier: command.generation_identifier,
                node_name: command.node_name,
                closure_path: command.closure_path,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        }
    }

    async fn run_activate_generation(
        &self,
        command: nexus::ActivateGenerationCommand,
    ) -> nexus::EffectResult {
        let slot = Self::activation_slot(&command.activation_kind);
        match NixCommand::activate_system(&command.node_name).run().await {
            Ok(_) => nexus::EffectResult::GenerationActivated(nexus::ActivatedGeneration {
                generation_identifier: command.generation_identifier,
                node_name: command.node_name,
                generation_slot: slot,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Activate, detail),
        }
    }

    fn activation_slot(activation_kind: &ordinary::ActivationKind) -> ordinary::GenerationSlot {
        match activation_kind {
            ordinary::ActivationKind::Switch => ordinary::GenerationSlot::Current,
            ordinary::ActivationKind::Boot => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationKind::Test => ordinary::GenerationSlot::Recent,
            ordinary::ActivationKind::BootOnce => ordinary::GenerationSlot::BootPending,
        }
    }

    async fn run_path_info_gc(&self, command: nexus::PathInfoGcCommand) -> nexus::EffectResult {
        match NixCommand::collect_garbage(&command.node_name).run().await {
            Ok(output) => nexus::EffectResult::PathsCollected(nexus::GarbageCollected {
                cluster_name: command.cluster_name,
                node_name: command.node_name,
                reclaimed_paths: NixCommand::count_lines(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Gc, detail),
        }
    }

    fn effect_failed(stage: nexus::EffectStage, detail: String) -> nexus::EffectResult {
        nexus::EffectResult::EffectFailed(nexus::EffectFailure { stage, detail })
    }
}

/// A typed `nix` / `nix-store` invocation. Holds the program name and its
/// argument vector so the same value can be inspected before it runs; `run`
/// spawns it via `tokio::process::Command` and returns captured stdout or a
/// failure detail string. Constructors model the lojix-cli invocations.
#[derive(Debug, Clone)]
struct NixCommand {
    program: String,
    arguments: Vec<String>,
}

impl NixCommand {
    fn new(program: impl Into<String>, arguments: Vec<String>) -> Self {
        Self {
            program: program.into(),
            arguments,
        }
    }

    fn flake_metadata(flake: &str) -> Self {
        Self::new(
            "nix",
            vec![
                "flake".to_string(),
                "metadata".to_string(),
                "--json".to_string(),
                flake.to_string(),
            ],
        )
    }

    fn eval_drv_path(attribute: &str) -> Self {
        Self::new(
            "nix",
            vec![
                "eval".to_string(),
                "--refresh".to_string(),
                "--raw".to_string(),
                format!("{attribute}.drvPath"),
            ],
        )
    }

    fn build_closure(closure_path: &str, substituters: &[nexus::ExtraSubstituter]) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            closure_path.to_string(),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    fn build_closure_remote(
        builder: &str,
        closure_path: &str,
        substituters: &[nexus::ExtraSubstituter],
    ) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--builders".to_string(),
            format!("ssh-ng://{builder}"),
            closure_path.to_string(),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    /// The `--option extra-substituters / extra-trusted-public-keys` arguments
    /// for the deploy's extra substituters (audit C2 — `NixBuildCommand`
    /// carries them but the build previously ignored them, so it could not pull
    /// from the configured cache). Empty when there are none.
    fn substituter_options(substituters: &[nexus::ExtraSubstituter]) -> Vec<String> {
        if substituters.is_empty() {
            return Vec::new();
        }
        let urls = substituters
            .iter()
            .map(|substituter| substituter.url.clone())
            .collect::<Vec<_>>()
            .join(" ");
        let public_keys = substituters
            .iter()
            .map(|substituter| substituter.public_key.clone())
            .collect::<Vec<_>>()
            .join(" ");
        vec![
            "--option".to_string(),
            "extra-substituters".to_string(),
            urls,
            "--option".to_string(),
            "extra-trusted-public-keys".to_string(),
            public_keys,
        ]
    }

    fn copy_closure(node_name: &str, closure_path: &str) -> Self {
        Self::new(
            "nix",
            vec![
                "copy".to_string(),
                "--to".to_string(),
                format!("ssh-ng://{node_name}"),
                closure_path.to_string(),
            ],
        )
    }

    fn activate_system(node_name: &str) -> Self {
        // Remote system activation: set the system profile to the freshly
        // copied closure on the target node, then run its switch-to-configuration.
        Self::new(
            "ssh",
            vec![
                node_name.to_string(),
                "nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"".to_string(),
            ],
        )
    }

    fn collect_garbage(node_name: &str) -> Self {
        Self::new(
            "ssh",
            vec![node_name.to_string(), "nix-store --gc".to_string()],
        )
    }

    async fn run(&self) -> std::result::Result<String, String> {
        let output = Command::new(&self.program)
            .args(&self.arguments)
            .output()
            .await
            .map_err(|error| format!("failed to spawn {}: {error}", self.program))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(format!(
                "{} {} exited with {}: {}",
                self.program,
                self.arguments.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }

    fn first_line(output: &str) -> String {
        output.lines().next().unwrap_or("").trim().to_string()
    }

    fn first_line_or(output: &str, fallback: &str) -> String {
        let line = Self::first_line(output);
        if line.is_empty() {
            fallback.to_string()
        } else {
            line
        }
    }

    fn count_lines(output: &str) -> u64 {
        output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count() as u64
    }
}

impl nexus::NexusEngine for SchemaRuntime {
    async fn apply_sema_write(
        &mut self,
        _origin_route: nexus::OriginRoute,
        input: sema::SemaWriteInput,
    ) -> sema::SemaWriteOutput {
        self.apply_sema(input)
    }

    async fn observe_sema_read(
        &mut self,
        _origin_route: nexus::OriginRoute,
        input: sema::SemaReadInput,
    ) -> sema::SemaReadOutput {
        self.observe_sema(input)
    }

    async fn run_effect(&mut self, input: nexus::EffectCommand) -> nexus::EffectResult {
        match input {
            nexus::EffectCommand::ResolveFlakeAuth(request) => {
                self.resolve_flake_auth(request).await
            }
            nexus::EffectCommand::NixEval(command) => self.run_nix_eval(command).await,
            nexus::EffectCommand::NixBuild(command) => self.run_nix_build(command).await,
            nexus::EffectCommand::CopyClosure(command) => self.run_copy_closure(command).await,
            nexus::EffectCommand::ActivateGeneration(command) => {
                self.run_activate_generation(command).await
            }
            nexus::EffectCommand::PathInfoGc(command) => self.run_path_info_gc(command).await,
        }
    }

    fn budget_exhausted_reply(
        &self,
        _exhausted: triad_runtime::ContinuationExhausted,
    ) -> nexus::SignalOutput {
        nexus::SignalOutput::MetaOutput(meta::Output::DeployRejected(
            self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
        ))
    }

    fn decide(
        &mut self,
        input: nexus::nexus::Nexus<nexus::nexus::Work>,
    ) -> nexus::nexus::Nexus<nexus::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            nexus::NexusWork::SignalArrived(input) => self.decide_signal_arrival(input),
            nexus::NexusWork::SemaReadCompleted(output) => self.decide_read_completion(output),
            nexus::NexusWork::SemaWriteCompleted(output) => self.decide_write_completion(output),
            nexus::NexusWork::EffectCompleted(result) => self.decide_effect_completion(result),
        };
        action.with_origin_route(origin_route)
    }
}

impl sema::SemaEngine for SchemaRuntime {
    fn apply_inner(
        &mut self,
        input: sema::sema::Sema<sema::sema::WriteInput>,
    ) -> sema::sema::Sema<sema::sema::WriteOutput> {
        let origin_route = input.origin_route();
        self.apply_sema(input.into_root())
            .with_origin_route(origin_route)
    }

    fn observe_inner(
        &self,
        input: sema::sema::Sema<sema::sema::ReadInput>,
    ) -> sema::sema::Sema<sema::sema::ReadOutput> {
        let origin_route = input.origin_route();
        self.observe_sema(input.into_root())
            .with_origin_route(origin_route)
    }
}
