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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use horizon_lib::name::{
    ClusterName as HorizonClusterName, CriomeDomainName, NodeName as HorizonNodeName,
    UserName as HorizonUserName,
};
use horizon_lib::{ClusterProposal, Horizon, Viewpoint};
use meta_signal_lojix::schema::lib as meta;
use nota_next::NotaSource;
use signal_lojix::schema::lib as ordinary;
use tokio::process::Command;

use crate::schema::nexus::NexusEngine;
use crate::schema::{nexus, sema};
use crate::{DaemonConfiguration, Result, Store};

/// The lojix engine noun. Carries the durable `Store` (the four sema tables)
/// and, while a deploy is in flight, the pipeline cursor that threads the
/// effect chain across continuation hops. Implements both engine traits; the
/// generated `NexusEngine::execute` drives the `Runner` over it.
#[derive(Debug)]
pub struct SchemaRuntime {
    /// The shared durable state. Each request is served by its OWN
    /// `SchemaRuntime` over a clone of this `Arc`, so the in-flight deploy
    /// cursor below is per-request while the durable tables are shared across
    /// concurrent connections (intent 2alg).
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    active_deploy: Option<DeployPipeline>,
    active_operation: Option<MetaOperation>,
}

/// Runtime paths the daemon needs for production deploy materialization.
/// These are decoded once from the daemon's binary startup configuration and
/// then shared across per-request engine values.
#[derive(Debug, Clone)]
pub struct RuntimeConfiguration {
    generated_inputs_directory: PathBuf,
    /// A test-only gate that the deploy pipeline awaits before its first effect
    /// runs. `None` in production (the pipeline runs straight through). A test
    /// holds the barrier closed to prove the daemon replies the accepted handle
    /// while the pipeline is still parked, then opens it to let the pipeline
    /// complete on the daemon-owned executor — the up9b decoupling witness.
    effect_barrier: Option<EffectBarrier>,
}

/// A pipeline-pause gate the deploy job awaits once before its first effect.
/// Test-only: production [`RuntimeConfiguration`] carries `None`. Backed by a
/// semaphore that starts with zero permits (the pipeline parks on `acquire`)
/// until the test `open`s it. Lets a test prove ordering deterministically
/// without shelling out to `nix`.
#[derive(Debug, Clone)]
pub struct EffectBarrier {
    gate: Arc<tokio::sync::Semaphore>,
}

impl Default for EffectBarrier {
    fn default() -> Self {
        Self::held()
    }
}

impl EffectBarrier {
    /// A barrier that starts CLOSED — the pipeline parks at its first effect
    /// until [`Self::open`] is called.
    pub fn held() -> Self {
        Self {
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    /// Release the barrier so the parked pipeline proceeds. Idempotent enough
    /// for a test: adds a generous permit budget so every awaiter passes.
    pub fn open(&self) {
        self.gate.add_permits(1024);
    }

    /// Park until the barrier opens. The acquired permit is forgotten so the
    /// budget is not returned, keeping the barrier open for any later awaiter.
    async fn wait(&self) {
        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
    }
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

/// The synchronous outcome of [`SchemaRuntime::submit_deploy`] — the verdict the
/// daemon replies to the owner connection immediately, before the deploy
/// pipeline runs (up9q). `Accepted` carries the `AcceptedDeploy` handle (the
/// durable deployment identifier + marker) and leaves the in-flight cursor set
/// for the deploy-job actor to drive; `Rejected` is a typed up-front refusal
/// (unsupported action, or a submission write rejection) and leaves no cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploySubmissionOutcome {
    Accepted(meta::AcceptedDeploy),
    Rejected(meta::RejectedDeploy),
}

impl RuntimeConfiguration {
    pub fn from_daemon_configuration(configuration: &DaemonConfiguration) -> Self {
        Self {
            generated_inputs_directory: PathBuf::from(&configuration.state_directory_path)
                .join("generated-inputs"),
            effect_barrier: None,
        }
    }

    pub fn test_default() -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            effect_barrier: None,
        }
    }

    /// A test configuration whose deploy pipeline parks at its first effect on
    /// the given barrier — the seam the up9b decoupling tests drive.
    pub fn test_with_effect_barrier(barrier: EffectBarrier) -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            effect_barrier: Some(barrier),
        }
    }

    fn effect_barrier(&self) -> Option<&EffectBarrier> {
        self.effect_barrier.as_ref()
    }

    fn materialization_root(&self, command: &nexus::HorizonMaterializationCommand) -> PathBuf {
        let cluster = command.cluster_name.payload();
        let node = command.node_name.payload();
        self.generated_inputs_directory
            .join(cluster)
            .join(node)
            .join(Self::shape_name(&command.shape))
    }

    fn shape_name(shape: &nexus::MaterializationShape) -> &'static str {
        match shape {
            nexus::MaterializationShape::FullOs => "full-os",
            nexus::MaterializationShape::OsOnly => "os-only",
            nexus::MaterializationShape::Home(_) => "home",
        }
    }
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
    input_overrides: Vec<nexus::FlakeInputOverride>,
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

    /// The activation profile carried on the activate command — the shape that
    /// decides which target-side activation runs (System switch-to-configuration
    /// vs Home home-manager profile/activate).
    fn activation_profile(&self) -> nexus::ActivationProfile {
        match self {
            Self::System(action) => nexus::ActivationProfile::System(*action),
            Self::Home { mode, user } => {
                nexus::ActivationProfile::Home(nexus::HomeActivationProfile {
                    mode: *mode,
                    user: user.clone(),
                })
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
                format!("homeConfigurations.{}.activationPackage", user.payload())
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
                activation_kind: Self::system_activation_kind(deployment.system_action),
                source: deployment.source,
                flake: deployment.flake,
                build_attribute: deployment.build_attribute,
                action: DeployAction::System(deployment.system_action),
                builder: deployment.builder.map(meta::Builder::into_payload),
                substituters: Self::convert_substituters(deployment.substituters),
                input_overrides: Vec::new(),
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
                input_overrides: Vec::new(),
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

    fn needs_horizon_materialization(&self) -> bool {
        self.build_attribute.is_none()
    }

    fn horizon_materialization_command(&self) -> nexus::HorizonMaterializationCommand {
        nexus::HorizonMaterializationCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            source: self.source.clone(),
            shape: self.materialization_shape(),
        }
    }

    fn materialization_shape(&self) -> nexus::MaterializationShape {
        match &self.action {
            DeployAction::System(_) => match &self.deployment_kind {
                ordinary::DeploymentKind::FullOs => nexus::MaterializationShape::FullOs,
                ordinary::DeploymentKind::OsOnly => nexus::MaterializationShape::OsOnly,
                ordinary::DeploymentKind::HomeOnly => nexus::MaterializationShape::OsOnly,
            },
            DeployAction::Home { user, .. } => {
                nexus::MaterializationShape::Home(nexus::HomeMaterialization::new(user.clone()))
            }
        }
    }

    fn nix_eval_command(&self) -> nexus::NixEvalCommand {
        nexus::NixEvalCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_kind: self.deployment_kind,
            flake: self.flake.clone(),
            attribute: self.target_attribute(),
            overrides: self.input_overrides.clone(),
        }
    }

    fn target_attribute(&self) -> String {
        // A1 fix: a direct `build_attribute` override names a self-contained
        // flake output (the fixture path); otherwise the action supplies the
        // production attribute (`nixosConfigurations.target...` /
        // `homeConfigurations.<user>...`). The old `{cluster}.{node}` form
        // resolved to no real flake attribute and every deploy failed at eval.
        match &self.build_attribute {
            Some(attribute) => attribute.payload().clone(),
            None => self.action.target_attribute(),
        }
    }

    fn nix_build_command(&self, closure_path: ordinary::ClosurePath) -> nexus::NixBuildCommand {
        nexus::NixBuildCommand {
            generation_identifier: self.generation_identifier.clone(),
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
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            closure_path,
            source: self.build_target(),
        }
    }

    fn activate_generation_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            closure_path,
            activation_kind: self.activation_kind,
            profile: self.action.activation_profile(),
        }
    }

    /// The activation-record write. The closure path is mandatory: by the time
    /// the pipeline records activation it has been captured on the cursor (the
    /// activate command already required it, risk R2), so `None` here is an
    /// internal invariant failure surfaced through `activation_commit` returning
    /// `None` rather than committing an empty closure into the live set.
    fn activation_commit(&self) -> Option<sema::ActivationCommit> {
        Some(sema::ActivationCommit {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: self.closure_path.clone()?,
        })
    }

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
        detail: Option<ordinary::PhaseDetail>,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_phase: phase,
            event_log_position,
            detail,
        }
    }

    /// The resolved `root@<node>.<cluster>.criome` SSH target this deploy
    /// activates, captured on the durable job row so a resumed job knows where
    /// to poll without re-deriving from a partially-applied cursor. `None` only
    /// if the names fail horizon validation (a submitted deploy never does).
    fn resolved_target(&self) -> Option<String> {
        SshTarget::root_at_node(&self.cluster_name, &self.node_name)
            .ok()
            .map(|target| target.as_ssh_arg())
    }

    /// The BootOnce transient-unit name a resumed `Activating` job polls via
    /// `journalctl -u <unit>` instead of re-activating. Deterministic in the
    /// deployment identifier so the resumed daemon computes the same name that
    /// was persisted at submit, rather than a time/pid value it cannot
    /// reconstruct. `None` for non-BootOnce actions (which have no transient
    /// unit to poll; copy is idempotent and activation re-runs safely).
    fn boot_once_unit(&self) -> Option<String> {
        match &self.action {
            DeployAction::System(ordinary::SystemAction::BootOnce) => Some(format!(
                "lojix-boot-once-deploy-{}",
                self.deployment_identifier.payload()
            )),
            _ => None,
        }
    }

    /// The durable in-flight job row at the given phase. Written on submit and
    /// rewritten at every phase transition (up9q): the persisted phase cursor,
    /// closure path (once built), resolved target, and BootOnce unit name let a
    /// restarted daemon read the row and reconcile the in-flight deploy.
    fn deploy_job(&self, phase: sema::DeployJobPhase) -> sema::DeployJob {
        sema::DeployJob {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            phase,
            closure_path: self.closure_path.clone(),
            resolved_target: self.resolved_target(),
            boot_once_unit: self.boot_once_unit(),
        }
    }
}

/// The reconcile decision a daemon makes for one persisted in-flight deploy
/// job it reads on start (up9q). Computed from the job's persisted phase; the
/// daemon acts on it to resume the deploy rather than losing it. The LIVE
/// continuation behind each variant (actually polling journalctl, actually
/// re-running the idempotent copy) is proven on a real target at S5 — this
/// type is the read-on-start reconcile-decision scaffolding S4b lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployJobResumption {
    /// Phase reached `Activating`: the activate effect was in flight when the
    /// daemon stopped. Re-activating could double-switch, so poll the BootOnce
    /// transient unit (PID-1-owned, survives the daemon) via
    /// `journalctl -u <unit>` and adopt its outcome. Carries the unit name when
    /// the job recorded one (BootOnce); a non-BootOnce activating job has no
    /// unit and falls back to re-running the idempotent activation at S5.
    PollActivationUnit { unit: Option<String> },
    /// Phase is pre-activation (`Submitted`/`Building`/`Built`/`Copying`): no
    /// target state was mutated yet, or only the idempotent copy ran. Re-drive
    /// the pipeline from submit; copy is idempotent and re-runs safely.
    RestartPipeline,
    /// Phase is terminal (`Activated`/`Failed`): the deploy already finished
    /// (success committed its live generation, failure is final). Nothing to
    /// resume; drop the stale job row.
    AlreadyTerminal,
}

impl sema::DeployJob {
    /// The reconcile decision for this persisted job, read on daemon start. A
    /// pure projection of the persisted phase cursor — the daemon calls it once
    /// per resumed row and acts on the typed verdict (up9q resume scaffolding).
    pub fn resumption(&self) -> DeployJobResumption {
        match self.phase {
            sema::DeployJobPhase::Activating => DeployJobResumption::PollActivationUnit {
                unit: self.boot_once_unit.clone(),
            },
            sema::DeployJobPhase::Submitted
            | sema::DeployJobPhase::Building
            | sema::DeployJobPhase::Built
            | sema::DeployJobPhase::Copying => DeployJobResumption::RestartPipeline,
            sema::DeployJobPhase::Activated | sema::DeployJobPhase::Failed => {
                DeployJobResumption::AlreadyTerminal
            }
        }
    }
}

impl From<ordinary::DeploymentPhase> for sema::DeployJobPhase {
    /// Mirror the wire phase onto the durable job-row phase cursor. The two
    /// enums carry the same variants; the job row tracks the same lifecycle the
    /// event log records, so a resumed daemon reads one phase value.
    fn from(phase: ordinary::DeploymentPhase) -> Self {
        match phase {
            ordinary::DeploymentPhase::Submitted => Self::Submitted,
            ordinary::DeploymentPhase::Building => Self::Building,
            ordinary::DeploymentPhase::Built => Self::Built,
            ordinary::DeploymentPhase::Copying => Self::Copying,
            ordinary::DeploymentPhase::Activating => Self::Activating,
            ordinary::DeploymentPhase::Activated => Self::Activated,
            ordinary::DeploymentPhase::Failed => Self::Failed,
        }
    }
}

impl Default for SchemaRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRuntime {
    pub fn new() -> Self {
        Self::with_store(Arc::new(
            Store::open(Self::test_store_path()).expect("open sema store"),
        ))
    }

    /// A unique tempdir-backed `*.sema` path for a test-only `Store`. Each call
    /// names a fresh file under the temp directory (process id, a nanosecond
    /// timestamp, and a per-process counter) so parallel tests never collide and
    /// each starts from a virgin store.
    fn test_store_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanoseconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join("lojix-test-store").join(format!(
            "{}-{nanoseconds}-{unique}.sema",
            std::process::id()
        ))
    }

    /// Build an engine over a SHARED `Store`. The daemon constructs one per
    /// request from a single shared `Arc<Store>`, so concurrent requests share
    /// the durable tables but each owns its in-flight deploy cursor (intent 2alg).
    pub fn with_store(store: Arc<Store>) -> Self {
        Self::with_store_and_configuration(store, Arc::new(RuntimeConfiguration::test_default()))
    }

    pub fn with_store_and_configuration(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
    ) -> Self {
        Self {
            store,
            configuration,
            active_deploy: None,
            active_operation: None,
        }
    }

    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    /// The deployment identifier currently on this engine's in-flight cursor, if
    /// any — the durable handle the job actor uses to track the deploy and that
    /// a watcher re-observes by. `None` outside an active deploy.
    pub fn active_deployment_identifier(&self) -> Option<ordinary::DeploymentIdentifier> {
        self.active_deploy
            .as_ref()
            .map(|pipeline| pipeline.deployment_identifier.clone())
    }

    /// Run ONLY the synchronous submit step of a `Deploy` (up9q surface a): the
    /// reject-guard, restart-safe identifier issuance, in-flight job-row
    /// persistence at `Submitted`, and cursor construction. Returns the typed
    /// admission outcome immediately — the `AcceptedDeploy` handle the daemon
    /// replies before the pipeline runs, or a typed rejection. On accept the
    /// in-flight cursor is left set on `self`, so the daemon hands this engine
    /// to the deploy-job actor, which drives the pipeline via
    /// [`Self::drive_submitted_deploy`]. The pipeline does NOT run here.
    pub fn submit_deploy(&mut self, request: meta::DeployRequest) -> DeploySubmissionOutcome {
        if let Some(reason) = Self::unsupported_deploy_reason(&request) {
            return DeploySubmissionOutcome::Rejected(self.deploy_rejection(reason));
        }
        self.active_operation = Some(MetaOperation::Deploy);
        let submission = match request {
            meta::DeployRequest::System(deployment) => sema::DeploySubmission::System(deployment),
            meta::DeployRequest::Home(deployment) => sema::DeploySubmission::Home(deployment),
        };
        match self.record_deploy_submitted(submission) {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => {
                DeploySubmissionOutcome::Accepted(accepted)
            }
            sema::SemaWriteOutput::WriteRejected(report) => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(meta::RejectedDeploy {
                    deploy_rejection_reason: Self::deploy_reason(report.reason),
                    database_marker: Self::marker(report.marker.commit_sequence.into_payload()),
                })
            }
            // `record_deploy_submitted` only ever returns the two arms above;
            // any other output is an internal invariant violation surfaced as a
            // typed rejection rather than a panic.
            _ => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                )
            }
        }
    }

    /// Drive an already-submitted deploy's effect pipeline to its terminal
    /// reply (up9q surface a, the daemon-owned executor body). Requires the
    /// in-flight cursor to be set by a prior [`Self::submit_deploy`]; re-enters
    /// the generated runner at the `DeploySubmitted` continuation, so it runs
    /// the SAME flake-auth -> eval -> build -> copy -> activate -> record chain
    /// the inline path ran, updating the durable job row at every phase. The
    /// returned `meta::Output` is the terminal deploy outcome (Deployed or
    /// DeployRejected) for logging and tests; the client already has its handle
    /// and re-observes the outcome by deployment identifier.
    pub async fn drive_submitted_deploy(&mut self) -> meta::Output {
        let accepted = match self.active_deploy.as_ref() {
            Some(pipeline) => meta::AcceptedDeploy {
                deployment_identifier: pipeline.deployment_identifier.clone(),
                database_marker: pipeline.accepted_marker.clone(),
            },
            None => {
                return meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                ));
            }
        };
        let work =
            nexus::NexusWork::SemaWriteCompleted(sema::SemaWriteOutput::DeploySubmitted(accepted))
                .with_origin_route(nexus::OriginRoute::new(0));
        match self.execute(work).await.into_root() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => output,
            _ => meta::Output::DeployRejected(meta::DeployRejected::new(
                self.deploy_rejection(meta::DeployRejectionReason::InternalError),
            )),
        }
    }

    fn marker(commit_sequence: u64) -> ordinary::DatabaseMarker {
        ordinary::DatabaseMarker {
            commit_sequence: ordinary::CommitSequence::new(commit_sequence),
            state_digest: ordinary::StateDigest::new(commit_sequence),
        }
    }

    fn sema_marker(commit_sequence: u64) -> sema::StateMarker {
        sema::StateMarker {
            commit_sequence: sema::CommitSequence::new(commit_sequence),
            state_digest: sema::StateDigest::new(commit_sequence),
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
                sema::SemaReadInput::QueryGenerations(selection.into_payload()),
            ),
            ordinary::Input::CheckHostKeyMaterial(query) => nexus::NexusAction::CommandSemaRead(
                sema::SemaReadInput::CheckKeyMaterial(query.into_payload()),
            ),
            ordinary::Input::WatchDeployments(_) | ordinary::Input::WatchCacheRetention(_) => {
                self.open_subscription()
            }
            ordinary::Input::Unwatch(close) => self.close_subscription(close.into_payload()),
        }
    }

    fn open_subscription(&mut self) -> nexus::NexusAction {
        let subscription_token = self.store.next_subscription_token();
        let reply = match self.store.commit_sequence() {
            Ok(commit_sequence) => {
                ordinary::Output::Watching(ordinary::Watching::new(ordinary::SubscriptionOpened {
                    subscription_token: ordinary::SubscriptionToken::new(subscription_token),
                    commit_sequence: ordinary::CommitSequence::new(commit_sequence),
                }))
            }
            Err(_) => ordinary::Output::WatchRejected(ordinary::WatchRejected::new(
                ordinary::RejectedWatch::new(ordinary::WatchRejectionReason::StreamUnavailable),
            )),
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn close_subscription(&mut self, close: ordinary::SubscriptionClose) -> nexus::NexusAction {
        let reply = ordinary::Output::Unwatched(ordinary::Unwatched::new(
            ordinary::SubscriptionClosed::new(close.into_payload()),
        ));
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn decide_meta_input(&mut self, input: meta::Input) -> nexus::NexusAction {
        match input {
            meta::Input::Deploy(request) => {
                let request = request.into_payload();
                if let Some(reason) = Self::unsupported_deploy_reason(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
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
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::PinGeneration(
                    request.into_payload(),
                ))
            }
            meta::Input::Unpin(request) => {
                self.active_operation = Some(MetaOperation::Unpin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::UnpinGeneration(
                    request.into_payload(),
                ))
            }
            meta::Input::Retire(request) => {
                self.active_operation = Some(MetaOperation::Retire);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RetireGeneration(
                    request.into_payload(),
                ))
            }
        }
    }

    /// The deploy reject-guard. Production System/Home eval/build are
    /// implemented through Horizon materialization, and the activating actions
    /// (System Boot/Switch/Test/BootOnce, Home Profile/Activate) now construct
    /// target-safe copy + activate commands (S4a), so every declared action is
    /// supported and enters the effect pipeline. `UnsupportedDeployAction`
    /// stays in the enum for honesty on any future not-yet-implemented shape;
    /// no current action returns it.
    fn unsupported_deploy_reason(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        match request {
            meta::DeployRequest::System(deployment) => {
                let supported = matches!(
                    deployment.system_action,
                    ordinary::SystemAction::Eval
                        | ordinary::SystemAction::Build
                        | ordinary::SystemAction::Boot
                        | ordinary::SystemAction::Switch
                        | ordinary::SystemAction::Test
                        | ordinary::SystemAction::BootOnce
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
            meta::DeployRequest::Home(deployment) => {
                let supported = matches!(
                    deployment.home_mode,
                    meta::HomeMode::Build | meta::HomeMode::Profile | meta::HomeMode::Activate
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
        }
    }

    // ---- decide: sema read completion -----------------------------------

    fn decide_read_completion(&mut self, output: sema::SemaReadOutput) -> nexus::NexusAction {
        let reply = match output {
            sema::SemaReadOutput::GenerationsQueried(listing) => {
                ordinary::Output::Queried(ordinary::Queried::new(listing))
            }
            sema::SemaReadOutput::KeyMaterialChecked(report) => {
                ordinary::Output::KeyMaterialChecked(ordinary::KeyMaterialChecked::new(report))
            }
            sema::SemaReadOutput::EventLogRead(_) => {
                ordinary::Output::QueryRejected(ordinary::QueryRejected::new(
                    self.query_rejection(ordinary::QueryRejectionReason::MalformedSelector),
                ))
            }
            sema::SemaReadOutput::ReadMissed(report) => ordinary::Output::QueryRejected(
                ordinary::QueryRejected::new(ordinary::RejectedQuery {
                    query_rejection_reason: ordinary::QueryRejectionReason::GenerationUnknown,
                    database_marker: Self::marker(report.marker.commit_sequence.into_payload()),
                }),
            ),
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
                Self::reply_meta(meta::Output::Pinned(meta::Pinned::new(applied)))
            }
            sema::SemaWriteOutput::GenerationUnpinned(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Unpinned(meta::Unpinned::new(applied)))
            }
            sema::SemaWriteOutput::GenerationRetired(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Retired(meta::Retired::new(applied)))
            }
            sema::SemaWriteOutput::ContainerRecorded(_) => self.advance_after_phase(),
            sema::SemaWriteOutput::WriteRejected(report) => self.reject_active_or_meta(report),
        }
    }

    fn begin_deploy_pipeline(&mut self, accepted: meta::AcceptedDeploy) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.as_ref() {
            Some(pipeline) => pipeline.clone(),
            None => return Self::reply_meta(meta::Output::Deployed(meta::Deployed::new(accepted))),
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
                return Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                )));
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
                // The closure path is captured on the cursor by `set_closure_path`
                // during eval/build; an activating pipeline that reached this stage
                // without a closure is an internal invariant failure, not an empty
                // activation (risk R2). Fail the pipeline rather than activate "".
                let closure_path = match pipeline.closure_path.clone() {
                    Some(closure_path) => closure_path,
                    None => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            stage: nexus::EffectStage::Activate,
                            detail: "activation reached without a built closure path".to_string(),
                        });
                    }
                };
                self.set_stage(DeployStage::CopyingRecorded);
                // Mark the durable job row Activating before the activate effect
                // runs (up9q): a daemon that crashes mid-activation reads this
                // phase on restart and reconciles via the BootOnce unit rather
                // than blindly re-activating. There is no Activating event-log
                // phase today; the job row is the resume cursor for this window.
                self.persist_job_phase(sema::DeployJobPhase::Activating);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::ActivateGeneration(
                    pipeline.activate_generation_command(closure_path),
                ))
            }
            DeployStage::CopyingRecorded => {
                let commit = match pipeline.activation_commit() {
                    Some(commit) => commit,
                    None => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            stage: nexus::EffectStage::Activate,
                            detail: "activation record reached without a built closure path"
                                .to_string(),
                        });
                    }
                };
                self.set_stage(DeployStage::ActivatedRecorded);
                nexus::NexusAction::CommandSemaWrite(
                    sema::SemaWriteInput::RecordGenerationActivated(commit),
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
        // The deploy reached a terminal success: its live generation is
        // committed, so drop the in-flight job row — only deploys that still
        // need resuming stay in the mirror (up9q).
        if let Some(pipeline) = self.active_deploy.as_ref() {
            self.retire_job_row(&pipeline.clone());
        }
        let accepted = match self.active_deploy.take() {
            Some(pipeline) => meta::AcceptedDeploy {
                deployment_identifier: pipeline.deployment_identifier,
                database_marker: pipeline.accepted_marker,
            },
            None => meta::AcceptedDeploy {
                deployment_identifier: ordinary::DeploymentIdentifier::new(0),
                database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
            },
        };
        Self::reply_meta(meta::Output::Deployed(meta::Deployed::new(accepted)))
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
        let marker = Self::marker(report.marker.commit_sequence.into_payload());
        let output = match operation {
            MetaOperation::Deploy => {
                meta::Output::DeployRejected(meta::DeployRejected::new(meta::RejectedDeploy {
                    deploy_rejection_reason: Self::deploy_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Pin => {
                meta::Output::PinRejected(meta::PinRejected::new(meta::RejectedPin {
                    pin_rejection_reason: Self::pin_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Unpin => {
                meta::Output::UnpinRejected(meta::UnpinRejected::new(meta::RejectedUnpin {
                    unpin_rejection_reason: Self::unpin_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Retire => {
                meta::Output::RetireRejected(meta::RetireRejected::new(meta::RejectedRetire {
                    retire_rejection_reason: Self::retire_reason(report.reason),
                    database_marker: marker,
                }))
            }
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
                    _ => Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                    ))),
                };
            }
        };
        match result {
            nexus::EffectResult::FlakeResolved(_) => {
                if pipeline.needs_horizon_materialization() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::MaterializeHorizon(
                        pipeline.horizon_materialization_command(),
                    ))
                } else {
                    // Record Building (stage still Submitted). The phase write
                    // hops back through advance_after_phase, which fires NixEval.
                    self.record_phase(ordinary::DeploymentPhase::Building, None)
                }
            }
            nexus::EffectResult::HorizonMaterialized(inputs) => {
                self.set_input_overrides(inputs.into_payload());
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

    fn set_input_overrides(&mut self, overrides: Vec<nexus::FlakeInputOverride>) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.input_overrides = overrides;
        }
    }

    fn record_phase(
        &mut self,
        phase: ordinary::DeploymentPhase,
        detail: Option<ordinary::PhaseDetail>,
    ) -> nexus::NexusAction {
        let event = match self.active_deploy.as_ref() {
            Some(pipeline) => {
                let position = self.store.next_event_log_position().unwrap_or(0);
                pipeline.phase_event(phase, ordinary::EventLogPosition::new(position), detail)
            }
            None => {
                return Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                )));
            }
        };
        nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordPhaseTransition(event))
    }

    /// Rewrite the durable in-flight job row at `phase` from the active deploy
    /// cursor (up9q). Best-effort: a persistence error here must not abort the
    /// running deploy — the event log remains the authoritative phase record
    /// and the job row is the resume convenience. No-op when no deploy is
    /// active (e.g. a standalone effect).
    fn persist_job_phase(&self, phase: sema::DeployJobPhase) {
        if let Some(pipeline) = self.active_deploy.as_ref() {
            let _ = self.store.upsert_deploy_job(pipeline.deploy_job(phase));
        }
    }

    /// Drop the durable in-flight job row for the active deploy (a terminal
    /// transition: the deploy finished and committed its live generation, so it
    /// no longer needs resuming). Best-effort, like `persist_job_phase`.
    fn retire_job_row(&self, pipeline: &DeployPipeline) {
        let _ = self
            .store
            .retract_deploy_job(*pipeline.deployment_identifier.payload());
    }

    fn fail_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        eprintln!(
            "lojix deploy pipeline effect failed at {:?}: {}",
            failure.stage, failure.detail
        );
        // Mark the durable job row Failed before clearing the cursor (up9q): a
        // restarted daemon reads Failed and does not re-attempt — the deploy is
        // terminal. The event log already carries the failed deployment.
        self.persist_job_phase(sema::DeployJobPhase::Failed);
        // Clear BOTH in-flight slots symmetrically with the finish path (audit
        // R5) — a mid-pipeline effect failure must not leak `active_operation`.
        self.active_deploy = None;
        self.active_operation = None;
        let reason = match failure.stage {
            nexus::EffectStage::FlakeAuth => meta::DeployRejectionReason::ProposalSourceUnreachable,
            nexus::EffectStage::MaterializeHorizon => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            nexus::EffectStage::Eval => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::Build => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::CopyClosure => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Activate => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Gc => meta::DeployRejectionReason::DeploymentInFlight,
        };
        Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
            self.deploy_rejection(reason),
        )))
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
        // Submission issues the deployment + generation identifiers and opens
        // the pipeline; the durable live-set / gc-roots write happens only at
        // activation. The identifiers are issued from the persisted maxima
        // (restart-safe), and the marker reflects the current commit sequence —
        // no row is written here, so the engine's commit counter does not move.
        let identifiers = (
            self.store.commit_sequence(),
            self.store.next_deployment_identifier(),
            self.store.next_generation_identifier(),
        );
        match identifiers {
            (Ok(commit_sequence), Ok(deployment_identifier), Ok(generation_identifier)) => {
                let accepted_marker = Self::marker(commit_sequence);
                self.active_deploy = Some(DeployPipeline::from_submission(
                    deployment_identifier.into(),
                    generation_identifier.into(),
                    accepted_marker.clone(),
                    submission,
                ));
                // Persist the in-flight job row at Submitted (up9q): from this
                // point the deploy is durably recorded and resumable across a
                // daemon restart, independent of the connection that submitted
                // it and of the job actor that will drive the pipeline.
                self.persist_job_phase(sema::DeployJobPhase::Submitted);
                sema::SemaWriteOutput::DeploySubmitted(meta::AcceptedDeploy {
                    deployment_identifier: deployment_identifier.into(),
                    database_marker: accepted_marker,
                })
            }
            _ => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn record_phase_transition(
        &mut self,
        event: ordinary::DeploymentPhaseEvent,
    ) -> sema::SemaWriteOutput {
        let recorded_phase = event.deployment_phase;
        let recorded = self.store.next_event_log_position().and_then(|position| {
            self.store
                .append_event_log_entry(sema::EventLogEntry {
                    event_log_position: ordinary::EventLogPosition::new(position),
                    record: sema::LoggedEvent::Deployment(event),
                })
                .and_then(|()| self.store.commit_sequence())
                .map(|commit_sequence| (position, commit_sequence))
        });
        match recorded {
            Ok((event_log_position, commit_sequence)) => {
                // Mirror the just-committed phase onto the durable job row so a
                // restarted daemon reads the latest phase the deploy reached
                // (up9q). The event-log write above is authoritative; this keeps
                // the resume convenience row in step.
                self.persist_job_phase(sema::DeployJobPhase::from(recorded_phase));
                sema::SemaWriteOutput::PhaseRecorded(sema::PhaseReceipt {
                    event_log_position: ordinary::EventLogPosition::new(event_log_position),
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
        let pipeline = self.active_deploy.clone();
        let deployment_identifier = pipeline
            .as_ref()
            .map(|p| p.deployment_identifier.clone())
            .unwrap_or_else(|| ordinary::DeploymentIdentifier::new(0));
        let deployment_kind = pipeline
            .as_ref()
            .map(|p| p.deployment_kind)
            .unwrap_or(ordinary::DeploymentKind::FullOs);
        let activation_kind = pipeline
            .as_ref()
            .map(|p| p.activation_kind)
            .unwrap_or(ordinary::ActivationKind::Switch);
        let generation = sema::LiveGeneration {
            deployment_identifier,
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            deployment_kind,
            activation_kind,
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
        };
        let root = sema::GcRoot {
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
            label: None,
        };
        // The live-set row and the gc-root row are written as TWO sequential
        // keyed asserts (inside `Store::record_activation`). A `CommitRequest`
        // is single-table, so true cross-table atomicity is not available; the
        // sequential write is the accepted baseline. The keyed asserts are
        // fail-safe (a duplicate key errors, never clobbers), but a crash
        // between them leaves a torn write with no reopen reconciliation —
        // cross-table atomicity needs a sema-engine multi-table commit.
        let recorded = self
            .store
            .record_activation(generation, root)
            .and_then(|()| self.store.commit_sequence());
        match recorded {
            Ok(commit_sequence) => {
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
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let already_used = roots
            .iter()
            .any(|root| root.label.as_ref() == Some(&request.pin_label));
        if already_used {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelInUse);
        }
        let Some(mut root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                sema::RejectionReason::GenerationUnknown,
            );
        };
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Pinned;
        root.label = Some(request.pin_label.clone());
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationPinned(meta::AppliedPin {
                generation_identifier: request.generation_identifier,
                pin_label: request.pin_label,
                from_slot,
                to_slot: ordinary::GenerationSlot::Pinned,
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::GenerationUnknown)
            }
        }
    }

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::PinLabelUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let Some(mut root) = roots.into_iter().find(|root| {
            root.label.as_ref() == Some(&request.pin_label)
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelUnknown);
        };
        let generation_identifier = root.generation_identifier.clone();
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Recent;
        root.label = None;
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationUnpinned(meta::AppliedUnpin {
                generation_identifier,
                pin_label: request.pin_label,
                from_slot,
                to_slot: ordinary::GenerationSlot::Recent,
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelUnknown)
            }
        }
    }

    fn retire_generation(&mut self, request: meta::RetireRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let Some(root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                sema::RejectionReason::GenerationUnknown,
            );
        };
        if matches!(root.generation_slot, ordinary::GenerationSlot::Pinned) {
            return Self::write_rejected(current_sequence, sema::RejectionReason::GenerationPinned);
        }
        let committed = self
            .store
            .retract_gc_root(*request.generation_identifier.payload())
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationRetired(meta::AppliedRetire {
                generation_identifier: request.generation_identifier,
                from_slot: root.generation_slot,
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::GenerationUnknown)
            }
        }
    }

    fn record_container_transition(
        &mut self,
        transition: sema::ContainerTransition,
    ) -> sema::SemaWriteOutput {
        let recorded = self.store.next_event_log_position().and_then(|position| {
            let record = sema::ContainerLifecycleRecord {
                cluster_name: transition.cluster_name,
                node_name: transition.node_name,
                container: transition.container,
                state: transition.state,
                event_log_position: ordinary::EventLogPosition::new(position),
            };
            let entry = sema::EventLogEntry {
                event_log_position: ordinary::EventLogPosition::new(position),
                record: sema::LoggedEvent::Container(record.clone()),
            };
            self.store
                .record_container_transition(record, entry)
                .and_then(|()| self.store.commit_sequence())
                .map(|commit_sequence| (position, commit_sequence))
        });
        match recorded {
            Ok((event_log_position, commit_sequence)) => {
                sema::SemaWriteOutput::ContainerRecorded(sema::ContainerReceipt {
                    event_log_position: ordinary::EventLogPosition::new(event_log_position),
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
        let matching = self
            .store
            .matching_live_generations(|live| Self::generation_matches(&selection, live));
        let live_generations = match matching {
            Ok(live_generations) => live_generations,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::GenerationUnknown),
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let generations: Vec<ordinary::Generation> = live_generations
            .iter()
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
            generation_identifier: live.generation_identifier.clone(),
            deployment_identifier: live.deployment_identifier.clone(),
            cluster_name: live.cluster_name.clone(),
            node_name: live.node_name.clone(),
            deployment_kind: live.deployment_kind,
            activation_kind: live.activation_kind,
            generation_slot: live.generation_slot,
            closure_path: live.closure_path.clone(),
        }
    }

    fn read_event_log(&self, range: ordinary::EventLogRange) -> sema::SemaReadOutput {
        let entries = match self
            .store
            .event_log_in_range(*range.from.payload(), *range.until.payload())
        {
            Ok(entries) => entries,
            Err(_) => {
                return Self::read_missed(0, sema::RejectionReason::EventLogPositionOutOfRange);
            }
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let mut deployment_events = Vec::new();
        let mut retention_events = Vec::new();
        for entry in &entries {
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
        // Park on the test effect barrier (if any) before the first real effect
        // runs. Production carries no barrier and falls straight through; a
        // decoupling test holds it closed to prove the accepted handle is
        // replied while the pipeline is still parked here (up9b).
        if let Some(barrier) = self.configuration.effect_barrier() {
            barrier.wait().await;
        }
        // Resolve the flake metadata to a locked revision through the proposal
        // source. `nix flake metadata --json <flake>` reports the resolved ref.
        match NixCommand::flake_metadata(request.flake.payload())
            .run()
            .await
        {
            Ok(output) => nexus::EffectResult::FlakeResolved(nexus::ResolvedFlake {
                flake: request.flake,
                revision: NixCommand::first_line(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::FlakeAuth, detail),
        }
    }

    async fn run_horizon_materialization(
        &self,
        command: nexus::HorizonMaterializationCommand,
    ) -> nexus::EffectResult {
        let materialization =
            HorizonMaterialization::new(self.configuration.as_ref().clone(), command);
        match materialization.run().await {
            Ok(inputs) => nexus::EffectResult::HorizonMaterialized(inputs),
            Err(detail) => Self::effect_failed(nexus::EffectStage::MaterializeHorizon, detail),
        }
    }

    async fn run_nix_eval(&self, command: nexus::NixEvalCommand) -> nexus::EffectResult {
        let attribute = format!("{}#{}", command.flake.payload(), command.attribute);
        match NixCommand::eval_drv_path(&attribute, &command.overrides)
            .run()
            .await
        {
            Ok(output) => nexus::EffectResult::ClosureEvaluated(nexus::EvaluatedClosure {
                generation_identifier: ordinary::GenerationIdentifier::new(0),
                closure_path: ordinary::ClosurePath::new(NixCommand::first_line(&output)),
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
                NixCommand::build_closure(command.closure_path.payload(), &command.substituters)
            }
            nexus::BuildTarget::Remote(_) => NixCommand::build_closure_remote(
                command.closure_path.payload(),
                &command.substituters,
            ),
        };
        match invocation.run().await {
            Ok(output) => nexus::EffectResult::ClosureBuilt(nexus::BuiltClosure {
                generation_identifier: command.generation_identifier,
                closure_path: ordinary::ClosurePath::new(NixCommand::first_line_or(
                    &output,
                    command.closure_path.payload(),
                )),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Build, detail),
        }
    }

    async fn run_copy_closure(&self, command: nexus::CopyClosureCommand) -> nexus::EffectResult {
        let copy = match ClosureCopy::from_command(&command) {
            Ok(copy) => copy,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        };
        match copy.run().await {
            Ok(()) => nexus::EffectResult::ClosureCopied(nexus::CopiedClosure {
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
        let activation = match Activation::from_command(&command) {
            Ok(activation) => activation,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::Activate, detail),
        };
        match activation.run().await {
            Ok(()) => nexus::EffectResult::GenerationActivated(nexus::ActivatedGeneration {
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
        match NixCommand::collect_garbage(command.node_name.payload())
            .run()
            .await
        {
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

/// One Horizon materialization request. It owns the decoded Nexus command and
/// immutable daemon paths, projects cluster data through horizon-rs, and emits
/// flake input overrides for the subsequent Nix eval.
#[derive(Debug, Clone)]
struct HorizonMaterialization {
    configuration: RuntimeConfiguration,
    command: nexus::HorizonMaterializationCommand,
}

impl HorizonMaterialization {
    fn new(
        configuration: RuntimeConfiguration,
        command: nexus::HorizonMaterializationCommand,
    ) -> Self {
        Self {
            configuration,
            command,
        }
    }

    async fn run(&self) -> std::result::Result<nexus::MaterializedInputs, String> {
        self.run_inner().await.map_err(|error| error.to_string())
    }

    async fn run_inner(&self) -> Result<nexus::MaterializedInputs> {
        let proposal = ProjectableProposal::from(ProposalFile::new(&self.command.source).load()?);
        let viewpoint = HorizonViewpoint::from_command(&self.command)?;
        let horizon = proposal.project(&viewpoint)?;
        let root = MaterializationRoot::new(self.configuration.materialization_root(&self.command));
        root.prepare()?;
        MaterializedInputSet::new(root, horizon, self.command.shape.clone())
            .write()
            .await
    }
}

/// The cluster proposal file path carried by `signal-lojix::ProposalSource`.
#[derive(Debug, Clone)]
struct ProposalFile {
    path: PathBuf,
}

impl ProposalFile {
    fn new(source: &ordinary::ProposalSource) -> Self {
        Self {
            path: PathBuf::from(source.payload()),
        }
    }

    fn load(&self) -> Result<ClusterProposal> {
        let text = fs::read_to_string(&self.path)?;
        Ok(NotaSource::new(&text).parse()?)
    }
}

/// Typed Horizon viewpoint derived from the deploy command.
#[derive(Debug, Clone)]
struct HorizonViewpoint {
    cluster: HorizonClusterName,
    node: HorizonNodeName,
}

impl HorizonViewpoint {
    fn from_command(command: &nexus::HorizonMaterializationCommand) -> Result<Self> {
        Ok(Self {
            cluster: HorizonClusterName::try_new(command.cluster_name.payload().clone())?,
            node: HorizonNodeName::try_new(command.node_name.payload().clone())?,
        })
    }

    fn as_horizon_viewpoint(&self) -> Viewpoint {
        Viewpoint {
            cluster: self.cluster.clone(),
            node: self.node.clone(),
        }
    }
}

/// Projection wrapper: today's production Horizon shape still has separate
/// pan-Horizon and cluster proposals to match the old deploy stack.
#[derive(Debug, Clone)]
struct ProjectableProposal {
    cluster: ClusterProposal,
}

impl ProjectableProposal {
    fn project(&self, viewpoint: &HorizonViewpoint) -> Result<Horizon> {
        Ok(self.cluster.project(&viewpoint.as_horizon_viewpoint())?)
    }
}

impl From<ClusterProposal> for ProjectableProposal {
    fn from(cluster: ClusterProposal) -> Self {
        Self { cluster }
    }
}

/// Root directory for one materialization result.
#[derive(Debug, Clone)]
struct MaterializationRoot {
    path: PathBuf,
}

impl MaterializationRoot {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.path)?;
        Ok(())
    }

    fn input_directory(&self, name: GeneratedInputName) -> GeneratedInputDirectory {
        GeneratedInputDirectory::new(self.path.join(name.as_str()))
    }
}

/// The generated inputs for one deploy.
#[derive(Debug, Clone)]
struct MaterializedInputSet {
    root: MaterializationRoot,
    horizon: Horizon,
    shape: nexus::MaterializationShape,
}

impl MaterializedInputSet {
    fn new(
        root: MaterializationRoot,
        horizon: Horizon,
        shape: nexus::MaterializationShape,
    ) -> Self {
        Self {
            root,
            horizon,
            shape,
        }
    }

    async fn write(&self) -> Result<nexus::MaterializedInputs> {
        let mut inputs = Vec::new();
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::Horizon)
                .write_horizon(&self.horizon)?
                .to_override(GeneratedInputName::Horizon)
                .await?,
        );
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::System)
                .write_system(&self.horizon.node.system)?
                .to_override(GeneratedInputName::System)
                .await?,
        );
        if let Some(deployment) = DeploymentInput::from_shape(&self.shape) {
            inputs.push(
                self.root
                    .input_directory(GeneratedInputName::Deployment)
                    .write_deployment(&deployment)?
                    .to_override(GeneratedInputName::Deployment)
                    .await?,
            );
        }
        Ok(nexus::MaterializedInputs::new(inputs))
    }
}

/// One generated input directory containing a tiny flake.
#[derive(Debug, Clone)]
struct GeneratedInputDirectory {
    path: PathBuf,
}

impl GeneratedInputDirectory {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.path)?;
        Ok(())
    }

    fn write_horizon(&self, horizon: &Horizon) -> Result<Self> {
        self.prepare()?;
        fs::write(
            self.path.join("horizon.json"),
            serde_json::to_string_pretty(horizon)?,
        )?;
        fs::write(
            self.path.join("flake.nix"),
            "{ outputs = _: { horizon = builtins.fromJSON (builtins.readFile ./horizon.json); }; }\n",
        )?;
        Ok(self.clone())
    }

    fn write_system(&self, system: &horizon_lib::species::System) -> Result<Self> {
        self.prepare()?;
        fs::write(
            self.path.join("flake.nix"),
            format!(
                "{{ outputs = _: {{ system = \"{}\"; }}; }}\n",
                NixSystemName::from_horizon_system(system).as_str()
            ),
        )?;
        Ok(self.clone())
    }

    fn write_deployment(&self, deployment: &DeploymentInput) -> Result<Self> {
        self.prepare()?;
        fs::write(self.path.join("flake.nix"), deployment.flake_text())?;
        Ok(self.clone())
    }

    async fn to_override(&self, name: GeneratedInputName) -> Result<nexus::FlakeInputOverride> {
        let hash = NarHash::from_path(&self.path).await?;
        Ok(nexus::FlakeInputOverride {
            name: name.as_str().to_string(),
            reference: nexus::FlakeInputReference {
                url: format!("path:{}", self.path.display()),
                nix_archive_hash: hash.as_url_query_value(),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedInputName {
    Horizon,
    System,
    Deployment,
}

impl GeneratedInputName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Horizon => "horizon",
            Self::System => "system",
            Self::Deployment => "deployment",
        }
    }
}

/// Deployment-shape flake contents for CriomOS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeploymentInput {
    include_home: bool,
    include_all_firmware: bool,
}

impl DeploymentInput {
    fn from_shape(shape: &nexus::MaterializationShape) -> Option<Self> {
        match shape {
            nexus::MaterializationShape::FullOs => Some(Self {
                include_home: true,
                include_all_firmware: true,
            }),
            nexus::MaterializationShape::OsOnly => Some(Self {
                include_home: false,
                include_all_firmware: false,
            }),
            nexus::MaterializationShape::Home(_) => None,
        }
    }

    fn flake_text(&self) -> String {
        format!(
            "{{ outputs = _: {{ deployment = {{ includeHome = {}; includeAllFirmware = {}; }}; }}; }}\n",
            self.include_home, self.include_all_firmware
        )
    }
}

/// Nix platform string derived from Horizon's typed system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NixSystemName(&'static str);

impl NixSystemName {
    fn from_horizon_system(system: &horizon_lib::species::System) -> Self {
        match system {
            horizon_lib::species::System::X86_64Linux => Self("x86_64-linux"),
            horizon_lib::species::System::Aarch64Linux => Self("aarch64-linux"),
        }
    }

    fn as_str(self) -> &'static str {
        self.0
    }
}

/// SRI NAR hash for a generated input directory.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NarHash(String);

impl NarHash {
    async fn from_path(path: &Path) -> Result<Self> {
        let output = NixCommand::hash_path(path).run().await.map_err(|detail| {
            std::io::Error::other(format!("failed to hash generated input: {detail}"))
        })?;
        Ok(Self(NixCommand::first_line(&output)))
    }

    fn as_url_query_value(&self) -> String {
        self.0
            .chars()
            .flat_map(|character| match character {
                '+' => "%2B".chars().collect::<Vec<_>>(),
                '/' => "%2F".chars().collect::<Vec<_>>(),
                '=' => "%3D".chars().collect::<Vec<_>>(),
                other => vec![other],
            })
            .collect()
    }
}

/// SSH target — `<user>@<node>.<cluster>.criome` — the addressing used by
/// `ssh`, `nix copy --to ssh-ng://…`, and `--from ssh-ng://…` (ported from
/// `lojix-cli/src/host.rs`). Activate/copy address `root@<criome_domain>`,
/// NEVER a bare `NodeName`; the domain is derived from the cluster + node on
/// the deploy cursor via `CriomeDomainName::for_node` (resolving open question
/// Q1: the address is computed from cursor fields already present —
/// `<node>.<cluster>.criome` — not threaded as a new horizon-projection field).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SshTarget {
    user: String,
    domain: CriomeDomainName,
}

impl SshTarget {
    /// Resolve `root@<node>.<cluster>.criome` from the ordinary cluster + node
    /// names carried on the cursor. Errors only if the names fail horizon
    /// validation (empty / quotation mark), which a submitted deploy never has.
    fn root_at_node(
        cluster_name: &ordinary::ClusterName,
        node_name: &ordinary::NodeName,
    ) -> std::result::Result<Self, String> {
        Ok(Self {
            user: "root".to_string(),
            domain: Self::criome_domain(cluster_name, node_name)?,
        })
    }

    fn criome_domain(
        cluster_name: &ordinary::ClusterName,
        node_name: &ordinary::NodeName,
    ) -> std::result::Result<CriomeDomainName, String> {
        let cluster = HorizonClusterName::try_new(cluster_name.payload().clone())
            .map_err(|error| format!("invalid cluster name for ssh target: {error}"))?;
        let node = HorizonNodeName::try_new(node_name.payload().clone())
            .map_err(|error| format!("invalid node name for ssh target: {error}"))?;
        Ok(CriomeDomainName::for_node(&node, &cluster))
    }

    fn with_user(&self, user: &HorizonUserName) -> Self {
        Self {
            user: user.as_str().to_string(),
            domain: self.domain.clone(),
        }
    }

    fn ssh_uri(&self) -> String {
        format!("ssh-ng://{}@{}", self.user, self.domain.as_str())
    }

    fn as_ssh_arg(&self) -> String {
        format!("{}@{}", self.user, self.domain.as_str())
    }

    /// `ssh -o BatchMode=yes <user>@<domain> <remote_command>`.
    fn remote_invocation(&self, remote_command: ShellCommand) -> NixCommand {
        NixCommand::new(
            "ssh",
            vec![
                "-o".to_string(),
                "BatchMode=yes".to_string(),
                self.as_ssh_arg(),
                remote_command.into_text(),
            ],
        )
    }
}

/// A pre-quoted remote shell command body — the single string ssh runs on the
/// target. Ported from `lojix-cli/src/process.rs::ShellCommand`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellCommand(String);

impl ShellCommand {
    fn from_raw(script: impl Into<String>) -> Self {
        Self(script.into())
    }

    fn into_text(self) -> String {
        self.0
    }
}

/// A single argument rendered safe for a remote `/bin/sh -c` body. Ported from
/// `lojix-cli/src/process.rs::ShellArgument`: bare when the text is wholly
/// shell-safe, single-quoted (with `'\''` escaping) otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellArgument {
    text: String,
}

impl ShellArgument {
    fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    fn to_command_text(&self) -> String {
        let safe = !self.text.is_empty()
            && self.text.bytes().all(|byte| {
                matches!(
                    byte,
                    b'a'..=b'z'
                        | b'A'..=b'Z'
                        | b'0'..=b'9'
                        | b'-'
                        | b'_'
                        | b'.'
                        | b'/'
                        | b'='
                        | b':'
                        | b'#'
                        | b'+'
                        | b','
                )
            });
        if safe {
            return self.text.clone();
        }
        format!("'{}'", self.text.replace('\'', "'\\''"))
    }
}

/// Move a closure from the dispatcher store to the activation target. Always
/// passes `--substitute-on-destination` so the target pulls signed paths from
/// the cluster cache when available; unsigned daemon-to-daemon transfer is
/// rejected under `require-sigs` (risk R6). Remote builds use the configured
/// Nix machine file and copy their result back into the dispatcher store, so
/// the copy command never opens a direct root SSH source to the builder.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClosureCopy {
    store_path: String,
    target: SshTarget,
}

impl ClosureCopy {
    fn from_command(command: &nexus::CopyClosureCommand) -> std::result::Result<Self, String> {
        let target = SshTarget::root_at_node(&command.cluster_name, &command.node_name)?;
        Ok(Self {
            store_path: command.closure_path.payload().clone(),
            target,
        })
    }

    /// Copy is idempotent: if the closure already exists on the target, Nix
    /// exits successfully without changing the activation state.
    fn invocation(&self) -> NixCommand {
        let arguments: Vec<String> = vec![
            "copy".to_string(),
            "--substitute-on-destination".to_string(),
            "--to".to_string(),
            self.target.ssh_uri(),
            self.store_path.clone(),
        ];
        NixCommand::new("nix", arguments)
    }

    async fn run(&self) -> std::result::Result<(), String> {
        self.invocation().run().await.map(|_| ())
    }
}

/// One activation on the target node — System (switch-to-configuration +
/// optional EFI reconcile / BootOnce transient unit) or Home (home-manager
/// profile/activate). Decoded from the `ActivateGenerationCommand`; ported
/// from `lojix-cli/src/activate.rs`.
#[derive(Debug, Clone)]
enum Activation {
    System(SystemActivation),
    Home(HomeActivation),
}

impl Activation {
    fn from_command(
        command: &nexus::ActivateGenerationCommand,
    ) -> std::result::Result<Self, String> {
        let target = SshTarget::root_at_node(&command.cluster_name, &command.node_name)?;
        let store_path = command.closure_path.payload().clone();
        match &command.profile {
            nexus::ActivationProfile::System(action) => Ok(Self::System(SystemActivation {
                target,
                store_path,
                action: *action,
            })),
            nexus::ActivationProfile::Home(profile) => {
                let user = HorizonUserName::try_new(profile.user.payload().clone())
                    .map_err(|error| format!("invalid user name for home activation: {error}"))?;
                Ok(Self::Home(HomeActivation {
                    node_name: command.node_name.clone(),
                    target,
                    user,
                    store_path,
                    mode: profile.mode,
                }))
            }
        }
    }

    async fn run(&self) -> std::result::Result<(), String> {
        match self {
            Self::System(activation) => activation.run().await,
            Self::Home(activation) => activation.run().await,
        }
    }
}

/// System activation on the target (ported from
/// `lojix-cli/src/activate.rs::SystemActivation`).
///
/// `Boot`/`Switch`/`Test`: one ssh call running `switch-to-configuration
/// <action>` directly (Boot/Switch first set the system profile). `BootOnce`:
/// one ssh call wrapping the boot-once script in `systemd-run --unit=<name>
/// --collect --wait --service-type=oneshot /bin/sh -c '…'` — owned by PID 1,
/// not the dispatcher's ssh, so a network blip that kills the ssh leaves the
/// unit running on the target to completion.
#[derive(Debug, Clone)]
struct SystemActivation {
    target: SshTarget,
    store_path: String,
    action: ordinary::SystemAction,
}

impl SystemActivation {
    /// Invocation for the simple Boot/Switch/Test path. `None` for the
    /// non-simple actions (BootOnce uses `systemd_run_invocation`; Eval/Build
    /// do not activate).
    fn ssh_invocation(&self) -> Option<NixCommand> {
        let action_word = match self.action {
            ordinary::SystemAction::Boot => "boot",
            ordinary::SystemAction::Switch => "switch",
            ordinary::SystemAction::Test => "test",
            ordinary::SystemAction::BootOnce
            | ordinary::SystemAction::Eval
            | ordinary::SystemAction::Build => return None,
        };
        let store = &self.store_path;
        let remote_command = if matches!(self.action, ordinary::SystemAction::Test) {
            format!("{store}/bin/switch-to-configuration {action_word}")
        } else {
            format!(
                "nix-env -p /nix/var/nix/profiles/system --set {store} \
                 && {store}/bin/switch-to-configuration {action_word}"
            )
        };
        Some(
            self.target
                .remote_invocation(ShellCommand::from_raw(remote_command)),
        )
    }

    /// Unique transient unit name for this deploy. Includes a time + pid suffix
    /// so concurrent deploys don't collide and the operator can grep the right
    /// one in the journal after a disconnect (`unit_name()`).
    fn unit_name(&self) -> String {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let process = std::process::id();
        format!("lojix-boot-once-{seconds:x}-{process:x}")
    }

    /// The bash script that runs inside the transient unit on the target. `OLD`
    /// (the rollback target) is read from `bootctl status`'s `Current Entry`
    /// (the running generation), `NEW` is derived from the system profile's
    /// `readlink` (canonical latest). reboot 1 lands NEW, reboot 2+ returns to
    /// OLD — headless-safe rollback (`boot_once_script()`).
    fn boot_once_script(&self) -> String {
        let store = &self.store_path;
        format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             CLOSURE='{store}'\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             SYSTEM_LINK=$(readlink /nix/var/nix/profiles/system)\n\
             GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
             NEW=\"nixos-generation-$GENERATION.conf\"\n\
             [ -f \"/boot/loader/entries/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             echo \"boot-once: oneshot=$NEW persistent-default=$OLD (=running generation)\"\n",
        )
    }

    /// BootOnce ssh call: wraps the boot-once script in `systemd-run --wait`.
    /// ssh holds open as a live stdout/stderr channel; if it dies the unit runs
    /// to completion regardless (`systemd_run_invocation()`).
    fn systemd_run_invocation(&self, unit_name: &str) -> NixCommand {
        let remote_command = format!(
            "systemd-run \
             --unit={unit_name} \
             --collect \
             --wait \
             --service-type=oneshot \
             /bin/sh -c {script}",
            script = ShellArgument::new(self.boot_once_script()).to_command_text(),
        );
        self.target
            .remote_invocation(ShellCommand::from_raw(remote_command))
    }

    /// Whether this action reconciles EFI bootloader vars after activation.
    /// `Boot`/`Switch` write `loader.conf`'s default but not EFI's
    /// `LoaderEntryDefault`; reconcile claims it explicitly and clears any
    /// stale one-shot. `Test` is non-persistent; `BootOnce` is its own thing
    /// (`requires_efi_reconcile()`).
    fn requires_efi_reconcile(&self) -> bool {
        matches!(
            self.action,
            ordinary::SystemAction::Boot | ordinary::SystemAction::Switch
        )
    }

    /// `readlink /nix/var/nix/profiles/system` — stdout parsed via
    /// `SystemProfileLink` to derive the `nixos-generation-N.conf` entry.
    fn step_readlink_system_profile_invocation(&self) -> NixCommand {
        self.target.remote_invocation(ShellCommand::from_raw(
            "readlink /nix/var/nix/profiles/system",
        ))
    }

    /// `bootctl set-default <entry>` — points EFI `LoaderEntryDefault` at the
    /// just-installed generation.
    fn step_set_efi_default_invocation(&self, entry: &BootEntry) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw(format!(
                "bootctl set-default {}",
                entry.as_str()
            )))
    }

    /// `bootctl set-oneshot ''` — clears any pending EFI one-shot from a prior
    /// BootOnce so it does not hijack the next reboot.
    fn step_clear_efi_oneshot_invocation(&self) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw("bootctl set-oneshot ''"))
    }

    async fn run(&self) -> std::result::Result<(), String> {
        match self.action {
            ordinary::SystemAction::BootOnce => self.run_boot_once().await,
            _ => self.run_simple().await,
        }
    }

    async fn run_simple(&self) -> std::result::Result<(), String> {
        match self.ssh_invocation() {
            Some(invocation) => invocation.run().await.map(|_| ())?,
            None => {
                return Err(format!("no simple activation for action {:?}", self.action));
            }
        }
        if self.requires_efi_reconcile() {
            self.reconcile_efi().await?;
        }
        Ok(())
    }

    async fn reconcile_efi(&self) -> std::result::Result<(), String> {
        let output = self.step_readlink_system_profile_invocation().run().await?;
        let link = SystemProfileLink::try_new(output.trim())?;
        let entry = link.generation().boot_entry();
        self.step_set_efi_default_invocation(&entry).run().await?;
        self.step_clear_efi_oneshot_invocation().run().await?;
        Ok(())
    }

    async fn run_boot_once(&self) -> std::result::Result<(), String> {
        let unit_name = self.unit_name();
        self.systemd_run_invocation(&unit_name)
            .run()
            .await
            .map(|_| ())
    }
}

/// Home activation on the target (ported from
/// `lojix-cli/src/activate.rs::HomeActivation`). `Profile`/`Activate` set the
/// home-manager profile as the target user, then `Activate` additionally runs
/// the activation package. Includes the local fast-path: skip ssh entirely
/// when the dispatcher already is the requested user on the target node.
#[derive(Debug, Clone)]
struct HomeActivation {
    node_name: ordinary::NodeName,
    target: SshTarget,
    user: HorizonUserName,
    store_path: String,
    mode: meta::HomeMode,
}

impl HomeActivation {
    fn local_profile_invocation(&self, home: &Path) -> NixCommand {
        NixCommand::new(
            "nix-env",
            vec![
                "-p".to_string(),
                home.join(".local/state/nix/profiles/home-manager")
                    .display()
                    .to_string(),
                "--set".to_string(),
                self.store_path.clone(),
            ],
        )
    }

    fn local_activate_invocation(&self) -> NixCommand {
        NixCommand::new(format!("{}/activate", self.store_path), Vec::new())
    }

    fn remote_profile_invocation(&self) -> NixCommand {
        self.user_target()
            .remote_invocation(ShellCommand::from_raw(format!(
                "nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set {}",
                ShellArgument::new(self.store_path.clone()).to_command_text(),
            )))
    }

    fn remote_activate_invocation(&self) -> NixCommand {
        self.user_target().remote_invocation(ShellCommand::from_raw(
            ShellArgument::new(format!("{}/activate", self.store_path)).to_command_text(),
        ))
    }

    async fn run(&self) -> std::result::Result<(), String> {
        match self.mode {
            meta::HomeMode::Build => Ok(()),
            meta::HomeMode::Profile => self.run_profile().await,
            meta::HomeMode::Activate => {
                self.run_profile().await?;
                self.run_activate().await
            }
        }
    }

    async fn run_profile(&self) -> std::result::Result<(), String> {
        if !self.is_local_context().await {
            return self.remote_profile_invocation().run().await.map(|_| ());
        }
        let home = std::env::var("HOME")
            .map_err(|_| "HOME is unset for local home activation".to_string())?;
        self.local_profile_invocation(Path::new(&home))
            .run()
            .await
            .map(|_| ())
    }

    async fn run_activate(&self) -> std::result::Result<(), String> {
        if !self.is_local_context().await {
            return self.remote_activate_invocation().run().await.map(|_| ());
        }
        self.local_activate_invocation().run().await.map(|_| ())
    }

    /// The local fast-path predicate: the dispatcher is already the requested
    /// user on the target node, so activation runs locally without ssh.
    async fn is_local_context(&self) -> bool {
        self.current_user().as_deref() == Some(self.user.as_str())
            && self.current_node().await.as_deref() == Some(self.node_name.payload().as_str())
    }

    fn user_target(&self) -> SshTarget {
        self.target.with_user(&self.user)
    }

    fn current_user(&self) -> Option<String> {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .ok()
    }

    async fn current_node(&self) -> Option<String> {
        // The local-context node match compares the dispatcher's short hostname
        // against the deploy cursor's node name (the same comparison lojix-cli
        // makes with `hostname -s`).
        let output = NixCommand::new("hostname", vec!["-s".to_string()])
            .run()
            .await
            .ok()?;
        Some(output.trim().to_string())
    }
}

/// The `system-N-link` symlink target of `/nix/var/nix/profiles/system`, parsed
/// to its generation number (ported from
/// `lojix-cli/src/activate.rs::SystemProfileLink`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SystemProfileLink {
    generation: SystemGeneration,
}

impl SystemProfileLink {
    fn try_new(link: &str) -> std::result::Result<Self, String> {
        let number = link
            .strip_prefix("system-")
            .and_then(|rest| rest.strip_suffix("-link"))
            .and_then(|number| number.parse::<u64>().ok())
            .ok_or_else(|| format!("invalid system profile link: {link}"))?;
        Ok(Self {
            generation: SystemGeneration(number),
        })
    }

    fn generation(&self) -> SystemGeneration {
        self.generation
    }
}

/// A NixOS system generation number, projecting to its EFI boot-entry name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SystemGeneration(u64);

impl SystemGeneration {
    fn boot_entry(self) -> BootEntry {
        BootEntry(format!("nixos-generation-{}.conf", self.0))
    }
}

/// A systemd-boot EFI loader entry filename (`nixos-generation-N.conf`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BootEntry(String);

impl BootEntry {
    fn as_str(&self) -> &str {
        &self.0
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

    fn eval_drv_path(attribute: &str, overrides: &[nexus::FlakeInputOverride]) -> Self {
        let mut arguments = vec![
            "eval".to_string(),
            "--refresh".to_string(),
            "--raw".to_string(),
        ];
        arguments.extend(Self::override_input_options(overrides));
        arguments.push(format!("{attribute}.drvPath"));
        Self::new("nix", arguments)
    }

    fn hash_path(path: &Path) -> Self {
        Self::new(
            "nix",
            vec![
                "hash".to_string(),
                "path".to_string(),
                "--type".to_string(),
                "sha256".to_string(),
                "--sri".to_string(),
                path.display().to_string(),
            ],
        )
    }

    fn build_closure(closure_path: &str, substituters: &[nexus::ExtraSubstituter]) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            Self::output_installable(closure_path),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    fn build_closure_remote(closure_path: &str, substituters: &[nexus::ExtraSubstituter]) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--option".to_string(),
            "max-jobs".to_string(),
            "0".to_string(),
            "--builders".to_string(),
            "@/etc/nix/machines".to_string(),
            Self::output_installable(closure_path),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    /// Select the derivation's *outputs* with the `^*` installable suffix so
    /// `nix build ... --print-out-paths` returns the realised output store
    /// path. The closure path threaded in is a `.drv` path (from
    /// `eval_drv_path`'s `.drvPath`); on nix 2.4+ a bare `.drv` installable
    /// makes `--print-out-paths` print the `.drv` path itself, NOT the built
    /// output — the daemon would then copy and activate the `.drv` and
    /// activation could never succeed (Unit C live e2e on Prometheus). The
    /// `^*` selector resolves the derivation to all its outputs.
    fn output_installable(closure_path: &str) -> String {
        format!("{closure_path}^*")
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

    fn override_input_options(overrides: &[nexus::FlakeInputOverride]) -> Vec<String> {
        let mut arguments = Vec::new();
        for override_input in overrides {
            arguments.push("--override-input".to_string());
            arguments.push(override_input.name.clone());
            arguments.push(format!(
                "{}?narHash={}",
                override_input.reference.url, override_input.reference.nix_archive_hash
            ));
        }
        arguments
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

    /// The program name. Test-only inspection of the constructed invocation
    /// (the on-node execution is proven at S5).
    #[cfg(test)]
    fn program(&self) -> &str {
        &self.program
    }

    /// The arguments joined by single spaces — a flat view of the constructed
    /// argv for inspection (the remote-command body is the final argument).
    /// Test-only.
    #[cfg(test)]
    fn joined_arguments(&self) -> String {
        self.arguments.join(" ")
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
            nexus::EffectCommand::MaterializeHorizon(command) => {
                self.run_horizon_materialization(command).await
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
        nexus::SignalOutput::MetaOutput(meta::Output::DeployRejected(meta::DeployRejected::new(
            self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
        )))
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

#[cfg(test)]
mod tests {
    //! Unit/argv/snapshot tests for the S4a command port: closure-threading
    //! onto the activate command, and the faithful `lojix-cli` command shapes
    //! (`SshTarget` addressing, `ClosureCopy`, `SystemActivation`,
    //! `HomeActivation`). The construction is unit-testable here; the on-node
    //! behavior is proven later at S5 on a live VM.

    use super::*;

    const STORE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-toplevel";

    fn system_submission(action: ordinary::SystemAction) -> sema::DeploySubmission {
        sema::DeploySubmission::System(meta::SystemDeployment {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            deployment_kind: ordinary::DeploymentKind::OsOnly,
            source: ordinary::ProposalSource::new("/dev/null"),
            flake: ordinary::FlakeReference::new("github:owner/repo"),
            system_action: action,
            builder: None,
            substituters: Vec::new(),
            build_attribute: None,
        })
    }

    fn system_pipeline(action: ordinary::SystemAction) -> DeployPipeline {
        DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(1),
            ordinary::GenerationIdentifier::new(1),
            SchemaRuntime::marker(0),
            system_submission(action),
        )
    }

    fn cluster() -> ordinary::ClusterName {
        ordinary::ClusterName::new("alpha")
    }

    fn node() -> ordinary::NodeName {
        ordinary::NodeName::new("node-1")
    }

    // ---- Step 1: closure-threading onto the activate command ----

    #[test]
    fn activate_command_carries_built_closure_path() {
        // The cursor captures the built path via `set_closure_path` (the
        // `ClosureBuilt` arm); `advance_after_phase` at `BuildingRecorded` reads
        // it onto the fired ActivateGeneration command — same non-empty path,
        // never dropped between build and activate (risk R2).
        let mut engine = SchemaRuntime::new();
        engine.active_deploy = Some(system_pipeline(ordinary::SystemAction::Switch));
        let built = ordinary::ClosurePath::new(STORE);
        engine.set_closure_path(built.clone());
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::CommandEffect(nexus::EffectCommand::ActivateGeneration(
                command,
            )) => {
                assert_eq!(command.closure_path, built);
                assert!(!command.closure_path.payload().is_empty());
            }
            other => panic!("expected ActivateGeneration effect, got {other:?}"),
        }
    }

    #[test]
    fn activate_without_closure_fails_rather_than_activating_empty() {
        // No closure on the cursor at activate time is an internal invariant
        // failure, not an empty activation: the pipeline fails, never fires an
        // ActivateGeneration with an empty path (risk R2).
        let mut engine = SchemaRuntime::new();
        engine.active_deploy = Some(system_pipeline(ordinary::SystemAction::Switch));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployRejected(_),
            )) => {}
            other => panic!("expected DeployRejected, got {other:?}"),
        }
    }

    #[test]
    fn activation_commit_requires_closure_path() {
        let mut pipeline = system_pipeline(ordinary::SystemAction::Switch);
        assert!(pipeline.activation_commit().is_none());
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        let commit = pipeline.activation_commit().expect("commit with closure");
        assert_eq!(commit.closure_path.payload(), STORE);
    }

    // ---- Step 2: the reject-guard opens the activating actions ----

    #[test]
    fn guard_accepts_every_declared_action() {
        for action in [
            ordinary::SystemAction::Eval,
            ordinary::SystemAction::Build,
            ordinary::SystemAction::Boot,
            ordinary::SystemAction::Switch,
            ordinary::SystemAction::Test,
            ordinary::SystemAction::BootOnce,
        ] {
            let request = meta::DeployRequest::System(meta::SystemDeployment {
                cluster_name: cluster(),
                node_name: node(),
                deployment_kind: ordinary::DeploymentKind::OsOnly,
                source: ordinary::ProposalSource::new("/dev/null"),
                flake: ordinary::FlakeReference::new("github:owner/repo"),
                system_action: action,
                builder: None,
                substituters: Vec::new(),
                build_attribute: None,
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "System {action:?} should be supported"
            );
        }
        for mode in [
            meta::HomeMode::Build,
            meta::HomeMode::Profile,
            meta::HomeMode::Activate,
        ] {
            let request = meta::DeployRequest::Home(meta::HomeDeployment {
                cluster_name: cluster(),
                node_name: node(),
                user_name: ordinary::UserName::new("li"),
                source: ordinary::ProposalSource::new("/dev/null"),
                flake: ordinary::FlakeReference::new("github:owner/repo"),
                home_mode: mode,
                builder: None,
                substituters: Vec::new(),
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "Home {mode:?} should be supported"
            );
        }
    }

    // ---- closure build argv — `.drv^*` output selector, never the bare .drv ----

    const DERIVATION: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-nixos-system-mercury.drv";

    #[test]
    fn build_closure_selects_drv_outputs_not_the_bare_drv() {
        // The closure path threaded in is a `.drv` (from `eval_drv_path`'s
        // `.drvPath`). On nix 2.4+ a bare `.drv` installable makes
        // `--print-out-paths` print the `.drv` path itself, so the daemon would
        // copy/activate the `.drv` and activation could never succeed (Unit C
        // live e2e). The `^*` output selector pins the realised output path.
        let invocation = NixCommand::build_closure(DERIVATION, &[]);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "build installable must carry the `^*` output selector: {argv}"
        );
        // The bare `.drv` must never be handed in as its own argv token — that
        // is exactly the regression that printed/activated the `.drv`.
        assert!(
            !invocation
                .arguments
                .iter()
                .any(|argument| argument == DERIVATION),
            "bare .drv must not appear as an installable token: {argv}"
        );
    }

    #[test]
    fn remote_build_uses_daemon_machine_file_and_disables_local_fallback() {
        let invocation = NixCommand::build_closure_remote(DERIVATION, &[]);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--builders @/etc/nix/machines"), "{argv}");
        assert!(argv.contains("--option max-jobs 0"), "{argv}");
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "remote build installable must carry the `^*` output selector: {argv}"
        );
        assert!(
            !invocation
                .arguments
                .iter()
                .any(|argument| argument == DERIVATION),
            "bare .drv must not appear as an installable token: {argv}"
        );
    }

    // ---- Step 3: SSH addressing — root@<criome_domain>, never bare node ----

    #[test]
    fn ssh_target_addresses_root_at_criome_domain() {
        let target = SshTarget::root_at_node(&cluster(), &node()).expect("target");
        assert_eq!(target.as_ssh_arg(), "root@node-1.alpha.criome");
        assert_eq!(target.ssh_uri(), "ssh-ng://root@node-1.alpha.criome");
    }

    fn copy_command(source: nexus::BuildTarget) -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: cluster(),
            node_name: node(),
            closure_path: ordinary::ClosurePath::new(STORE),
            source,
        }
    }

    // ---- Step 3: copy argv — always --substitute-on-destination, target only ----

    #[test]
    fn copy_from_dispatcher_uses_to_only_with_substitute() {
        let copy =
            ClosureCopy::from_command(&copy_command(nexus::BuildTarget::Local)).expect("copy");
        let invocation = copy.invocation();
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
        assert!(!argv.contains("--from"), "{argv}");
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn copy_remote_builder_still_uses_dispatcher_to_target_copy() {
        let builder = ordinary::NodeName::new("builder-node");
        let source = nexus::BuildTarget::Remote(nexus::BuilderNode::new(builder));
        let copy = ClosureCopy::from_command(&copy_command(source)).expect("copy");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
    }

    #[test]
    fn copy_builder_equals_target_still_runs_idempotent_target_copy() {
        let source = nexus::BuildTarget::Remote(nexus::BuilderNode::new(node()));
        let copy = ClosureCopy::from_command(&copy_command(source)).expect("copy");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
    }

    fn activate_command(
        profile: nexus::ActivationProfile,
        kind: ordinary::ActivationKind,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: cluster(),
            node_name: node(),
            closure_path: ordinary::ClosurePath::new(STORE),
            activation_kind: kind,
            profile,
        }
    }

    fn system_activation(action: ordinary::SystemAction) -> SystemActivation {
        match Activation::from_command(&activate_command(
            nexus::ActivationProfile::System(action),
            ordinary::ActivationKind::Switch,
        ))
        .expect("activation")
        {
            Activation::System(activation) => activation,
            Activation::Home(_) => panic!("expected System activation"),
        }
    }

    // ---- Step 3: per-action System activation argv, no $CLOSURE token ----

    #[test]
    fn switch_activation_has_store_path_and_switch_subcommand_no_closure_token() {
        let activation = system_activation(ordinary::SystemAction::Switch);
        let invocation = activation.ssh_invocation().expect("switch invocation");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@node-1.alpha.criome"), "{argv}");
        assert!(argv.contains(STORE), "{argv}");
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration switch")),
            "{argv}"
        );
        assert!(
            argv.contains("nix-env -p /nix/var/nix/profiles/system --set"),
            "{argv}"
        );
        assert!(!argv.contains("$CLOSURE"), "no $CLOSURE token: {argv}");
    }

    #[test]
    fn boot_activation_runs_switch_to_configuration_boot_then_reconciles_efi() {
        let activation = system_activation(ordinary::SystemAction::Boot);
        let invocation = activation.ssh_invocation().expect("boot invocation");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration boot")),
            "{argv}"
        );
        assert!(!argv.contains("$CLOSURE"), "{argv}");
        assert!(activation.requires_efi_reconcile());
        // EFI reconcile commands are present and correctly shaped.
        let readlink = activation.step_readlink_system_profile_invocation();
        assert!(
            readlink
                .joined_arguments()
                .contains("readlink /nix/var/nix/profiles/system"),
            "{}",
            readlink.joined_arguments()
        );
        let entry = BootEntry("nixos-generation-42.conf".to_string());
        let set_default = activation.step_set_efi_default_invocation(&entry);
        assert!(
            set_default
                .joined_arguments()
                .contains("bootctl set-default nixos-generation-42.conf"),
            "{}",
            set_default.joined_arguments()
        );
        let clear = activation.step_clear_efi_oneshot_invocation();
        assert!(
            clear.joined_arguments().contains("bootctl set-oneshot ''"),
            "{}",
            clear.joined_arguments()
        );
    }

    #[test]
    fn test_activation_runs_switch_to_configuration_test_only_no_profile_set() {
        let activation = system_activation(ordinary::SystemAction::Test);
        let invocation = activation.ssh_invocation().expect("test invocation");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration test")),
            "{argv}"
        );
        assert!(
            !argv.contains("nix-env"),
            "test does not set the profile: {argv}"
        );
        assert!(!activation.requires_efi_reconcile());
    }

    #[test]
    fn boot_once_uses_simple_invocation_none() {
        // BootOnce is not a simple invocation; it uses the systemd-run shape.
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        assert!(activation.ssh_invocation().is_none());
    }

    // ---- Step 3: BootOnce transient-unit argv + script snapshot ----

    #[test]
    fn boot_once_systemd_run_argv_shape() {
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        let invocation = activation.systemd_run_invocation("lojix-boot-once-abc-def");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@node-1.alpha.criome"), "{argv}");
        assert!(argv.contains("systemd-run"), "{argv}");
        assert!(argv.contains("--unit=lojix-boot-once-abc-def"), "{argv}");
        assert!(argv.contains("--collect"), "{argv}");
        assert!(argv.contains("--wait"), "{argv}");
        assert!(argv.contains("--service-type=oneshot"), "{argv}");
        assert!(argv.contains("/bin/sh -c"), "{argv}");
    }

    #[test]
    fn boot_once_unit_name_has_lojix_prefix() {
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        assert!(activation.unit_name().starts_with("lojix-boot-once-"));
    }

    #[test]
    fn boot_once_script_snapshot() {
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        let expected = format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             CLOSURE='{STORE}'\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             SYSTEM_LINK=$(readlink /nix/var/nix/profiles/system)\n\
             GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
             NEW=\"nixos-generation-$GENERATION.conf\"\n\
             [ -f \"/boot/loader/entries/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             echo \"boot-once: oneshot=$NEW persistent-default=$OLD (=running generation)\"\n",
        );
        assert_eq!(activation.boot_once_script(), expected);
    }

    // ---- Step 3: system profile link parse + EFI entry derivation ----

    #[test]
    fn system_profile_link_parses_generation_and_derives_entry() {
        let link = SystemProfileLink::try_new("system-42-link").expect("link");
        assert_eq!(
            link.generation().boot_entry().as_str(),
            "nixos-generation-42.conf"
        );
        assert!(SystemProfileLink::try_new("not-a-link").is_err());
    }

    // ---- Step 3: Home activation argv ----

    fn home_activation(mode: meta::HomeMode) -> HomeActivation {
        let profile = nexus::ActivationProfile::Home(nexus::HomeActivationProfile {
            mode,
            user: ordinary::UserName::new("li"),
        });
        match Activation::from_command(&activate_command(profile, ordinary::ActivationKind::Switch))
            .expect("activation")
        {
            Activation::Home(activation) => activation,
            Activation::System(_) => panic!("expected Home activation"),
        }
    }

    #[test]
    fn home_remote_profile_addresses_user_at_criome_domain() {
        let activation = home_activation(meta::HomeMode::Profile);
        let invocation = activation.remote_profile_invocation();
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("li@node-1.alpha.criome"), "{argv}");
        assert!(
            argv.contains("nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set"),
            "{argv}"
        );
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn home_remote_activate_runs_activate_package() {
        let activation = home_activation(meta::HomeMode::Activate);
        let invocation = activation.remote_activate_invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("li@node-1.alpha.criome"), "{argv}");
        assert!(argv.contains(&format!("{STORE}/activate")), "{argv}");
    }
}
