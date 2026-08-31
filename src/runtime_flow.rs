//! Handwritten Nexus work, effects, and runner behavior.
//!
//! This is executable Rust vocabulary owned by lojix, not a projection from a
//! schema language. Public signal values have already crossed the adapter seam
//! before they appear here.

pub use crate::runtime_model::{
    ActivationEffect, ClosurePath, ClusterName, DeploymentIdentifier, FlakeReference,
    GenerationArtifact, GenerationIdentifier, GenerationSlot, HostComposition, HostDeployAction,
    MetaEgress, MetaIngress, NodeName, OrdinaryEgress, OrdinaryIngress, ProposalSource,
    SemaReadInput, SemaReadOutput, SemaWriteInput, SemaWriteOutput, SourceRevisionPolicy,
    SourceRevisionRecord, TestMode, UserEnvironmentAction, UserName,
};

macro_rules! flow_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name($inner);
        impl $name {
            pub fn new(payload: $inner) -> Self {
                Self(payload)
            }
            pub fn payload(&self) -> &$inner {
                &self.0
            }
            pub fn into_payload(self) -> $inner {
                self.0
            }
        }
        impl From<$inner> for $name {
            fn from(payload: $inner) -> Self {
                Self::new(payload)
            }
        }
    };
}

macro_rules! flow_text {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name(String);
        impl $name {
            pub fn new(payload: impl Into<String>) -> Self {
                Self(payload.into())
            }
            pub fn payload(&self) -> &String {
                &self.0
            }
            pub fn into_payload(self) -> String {
                self.0
            }
        }
        impl From<String> for $name {
            fn from(payload: String) -> Self {
                Self::new(payload)
            }
        }
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalInput {
    OrdinaryInput(OrdinaryIngress),
    MetaInput(MetaIngress),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalOutput {
    OrdinaryOutput(OrdinaryEgress),
    MetaOutput(MetaEgress),
}
flow_text!(NixStoreUri);
flow_text!(SshDestination);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentTransport {
    pub nix_store_uri: NixStoreUri,
    pub ssh_destination: SshDestination,
}
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentInputMode {
    Direct,
    Horizon,
}
flow_text!(FlakeAttribute);
flow_newtype!(DeploymentOutputSelector, FlakeAttribute);
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum ActivationBackend {
    NixosSystemdBootV1,
    HomeManagerNixProfileV1,
}
flow_text!(NixBuilderSpec);
flow_text!(NixSystem);
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildTarget {
    Local,
    Remote(NixBuilderSpec),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtraSubstituter {
    pub url: String,
    pub public_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlakeAuthRequest {
    pub proposal_source: ProposalSource,
    pub flake_reference: FlakeReference,
    pub source_revision_policy: SourceRevisionPolicy,
}
flow_newtype!(ResolvedFlake, SourceRevisionRecord);
flow_newtype!(UserEnvironmentMaterialization, UserName);
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializationShape {
    CompleteHost,
    BaseHost,
    UserEnvironment(UserEnvironmentMaterialization),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HorizonMaterializationCommand {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub proposal_source: ProposalSource,
    pub materialization_shape: MaterializationShape,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlakeInputReference {
    pub url: String,
    pub nix_archive_hash: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlakeInputOverride {
    pub string: String,
    pub flake_input_reference: FlakeInputReference,
}
flow_newtype!(MaterializedInputs, Vec<FlakeInputOverride>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NixEvalCommand {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_artifact: GenerationArtifact,
    pub flake_reference: FlakeReference,
    pub source_revision_record: SourceRevisionRecord,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub flake_input_override_vector: Vec<FlakeInputOverride>,
    pub build_target: BuildTarget,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NixBuildCommand {
    pub generation_identifier: GenerationIdentifier,
    pub closure_path: ClosurePath,
    pub build_target: BuildTarget,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyClosureCommand {
    pub generation_identifier: GenerationIdentifier,
    pub node_name: NodeName,
    pub deployment_transport: DeploymentTransport,
    pub closure_path: ClosurePath,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserEnvironmentActivationProfile {
    pub user_environment_action: UserEnvironmentAction,
    pub user_name: UserName,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationProfile {
    Host(HostDeployAction),
    UserEnvironment(UserEnvironmentActivationProfile),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivateGenerationCommand {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_transport: DeploymentTransport,
    pub closure_path: ClosurePath,
    pub activation_effect: ActivationEffect,
    pub activation_backend: ActivationBackend,
    pub activation_profile: ActivationProfile,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathInfoGcCommand {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestExecutionProfile {
    pub test_mode: TestMode,
    pub nix_system: NixSystem,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub optional_deployment_transport: Option<DeploymentTransport>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HermeticCheckCommand {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub flake_reference: FlakeReference,
    pub test_execution_profile: TestExecutionProfile,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckBuilt {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub closure_path: ClosurePath,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BringUpTestVmCommand {
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
    pub deployment_transport: DeploymentTransport,
    pub closure_path: ClosurePath,
    pub string: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TearDownTestVmCommand {
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
    pub deployment_transport: DeploymentTransport,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestVmBroughtUp {
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestVmTornDown {
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectCommand {
    ResolveFlakeAuth(FlakeAuthRequest),
    MaterializeHorizon(HorizonMaterializationCommand),
    NixEval(NixEvalCommand),
    NixBuild(NixBuildCommand),
    CopyClosure(CopyClosureCommand),
    ActivateGeneration(ActivateGenerationCommand),
    PathInfoGc(PathInfoGcCommand),
    HermeticCheck(HermeticCheckCommand),
    BringUpTestVm(BringUpTestVmCommand),
    TearDownTestVm(TearDownTestVmCommand),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluatedClosure {
    pub generation_identifier: GenerationIdentifier,
    pub closure_path: ClosurePath,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltClosure {
    pub generation_identifier: GenerationIdentifier,
    pub closure_path: ClosurePath,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopiedClosure {
    pub generation_identifier: GenerationIdentifier,
    pub node_name: NodeName,
    pub closure_path: ClosurePath,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivatedGeneration {
    pub generation_identifier: GenerationIdentifier,
    pub node_name: NodeName,
    pub generation_slot: GenerationSlot,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GarbageCollected {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub integer: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectFailure {
    pub effect_stage: EffectStage,
    pub string: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum EffectStage {
    FlakeAuth,
    MaterializeHorizon,
    Eval,
    Build,
    CopyClosure,
    Activate,
    Gc,
    HermeticCheck,
    BringUpTestVm,
    TearDownTestVm,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectResult {
    FlakeResolved(ResolvedFlake),
    HorizonMaterialized(MaterializedInputs),
    ClosureEvaluated(EvaluatedClosure),
    ClosureBuilt(BuiltClosure),
    ClosureCopied(CopiedClosure),
    GenerationActivated(ActivatedGeneration),
    /// A same-host TestActivation has been handed to PID 1. Its terminal
    /// outcome belongs to the private retained-unit observer rather than this
    /// foreground runner, which the candidate may replace.
    DetachedTestActivationDispatched,
    PathsCollected(GarbageCollected),
    HermeticCheckBuilt(CheckBuilt),
    TestVmStarted(TestVmBroughtUp),
    TestVmStopped(TestVmTornDown),
    EffectFailed(EffectFailure),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NexusWork {
    SignalArrived(SignalInput),
    SemaReadCompleted(SemaReadOutput),
    SemaWriteCompleted(SemaWriteOutput),
    EffectCompleted(EffectResult),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NexusAction {
    CommandSemaRead(SemaReadInput),
    CommandSemaWrite(SemaWriteInput),
    CommandEffect(EffectCommand),
    ReplyToSignal(SignalOutput),
    Continue(NexusWork),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Work(NexusWork),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Action(NexusAction),
}

impl NexusWork {
    pub fn with_origin_route(self, origin_route: OriginRoute) -> Nexus<Self> {
        Nexus::new(origin_route, self)
    }

    fn sema_write_completed(output: SemaWriteOutput) -> Self {
        Self::SemaWriteCompleted(output)
    }

    fn sema_read_completed(output: SemaReadOutput) -> Self {
        Self::SemaReadCompleted(output)
    }

    fn effect_completed(output: EffectResult) -> Self {
        Self::EffectCompleted(output)
    }
}

impl NexusAction {
    pub fn with_origin_route(self, origin_route: OriginRoute) -> Nexus<Self> {
        Nexus::new(origin_route, self)
    }

    fn reply_to_signal(output: SignalOutput) -> Self {
        Self::ReplyToSignal(output)
    }
}

impl EffectResult {
    pub fn flake_resolved(value: SourceRevisionRecord) -> Self {
        Self::FlakeResolved(ResolvedFlake::new(value))
    }

    pub fn horizon_materialized(value: Vec<FlakeInputOverride>) -> Self {
        Self::HorizonMaterialized(MaterializedInputs::new(value))
    }

    pub fn closure_evaluated(value: EvaluatedClosure) -> Self {
        Self::ClosureEvaluated(value)
    }

    pub fn closure_built(value: BuiltClosure) -> Self {
        Self::ClosureBuilt(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginRoute(u64);

impl OriginRoute {
    pub fn new(payload: u64) -> Self {
        Self(payload)
    }
    pub fn payload(&self) -> &u64 {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nexus<Root> {
    pub origin_route: OriginRoute,
    pub root: Root,
}

impl<Root> Nexus<Root> {
    pub fn new(origin_route: OriginRoute, root: Root) -> Self {
        Self { origin_route, root }
    }

    pub fn origin_route(&self) -> OriginRoute {
        self.origin_route.clone()
    }
    pub fn root(&self) -> &Root {
        &self.root
    }
    pub fn into_root(self) -> Root {
        self.root
    }
}

#[allow(clippy::module_inception)]
pub mod nexus {
    pub type Work = super::NexusWork;
    pub type Action = super::NexusAction;
    pub type Nexus<Root> = super::Nexus<Root>;
}

impl triad_runtime::NexusWork for NexusWork {}
impl triad_runtime::NexusEffectCommand for EffectCommand {}
impl triad_runtime::NexusEffectResult for EffectResult {}

pub type NexusRunnerNextStep =
    triad_runtime::NextStep<SignalOutput, SemaWriteInput, SemaReadInput, EffectCommand, NexusWork>;

impl triad_runtime::NexusAction for NexusAction {
    type Reply = SignalOutput;
    type SemaWrite = SemaWriteInput;
    type SemaRead = SemaReadInput;
    type Effect = EffectCommand;
    type Work = NexusWork;

    fn into_next_step(self) -> NexusRunnerNextStep {
        match self {
            Self::CommandSemaWrite(input) => triad_runtime::NextStep::SemaWrite(input),
            Self::CommandSemaRead(input) => triad_runtime::NextStep::SemaRead(input),
            Self::ReplyToSignal(output) => triad_runtime::NextStep::Reply(output),
            Self::CommandEffect(effect) => triad_runtime::NextStep::RunEffect(effect),
            Self::Continue(work) => triad_runtime::NextStep::Continue(work),
        }
    }
}

pub trait NexusEngine: Send {
    fn continuation_limit(&self) -> triad_runtime::ContinuationLimit {
        triad_runtime::ContinuationLimit::default()
    }

    fn apply_sema_write(
        &mut self,
        origin_route: OriginRoute,
        input: SemaWriteInput,
    ) -> impl std::future::Future<Output = SemaWriteOutput> + Send + '_;

    fn observe_sema_read(
        &mut self,
        origin_route: OriginRoute,
        input: SemaReadInput,
    ) -> impl std::future::Future<Output = SemaReadOutput> + Send + '_;

    fn run_effect(
        &mut self,
        input: EffectCommand,
    ) -> impl std::future::Future<Output = EffectResult> + Send + '_;

    fn budget_exhausted_reply(
        &self,
        exhausted: triad_runtime::ContinuationExhausted,
    ) -> SignalOutput;

    fn decide(&mut self, input: Nexus<NexusWork>) -> Nexus<NexusAction>;

    fn execute(
        &mut self,
        input: Nexus<NexusWork>,
    ) -> impl std::future::Future<Output = Nexus<NexusAction>> + Send + '_
    where
        Self: Sized,
    {
        async move {
            let origin_route = input.origin_route();
            let first_work = input.into_root();
            let runner = triad_runtime::Runner::new(self.continuation_limit());
            let mut adapter = NexusRunnerAdapter {
                engine: self,
                origin_route: origin_route.clone(),
            };
            let reply = runner.drive(&mut adapter, first_work).await;
            NexusAction::reply_to_signal(reply).with_origin_route(origin_route)
        }
    }
}

struct NexusRunnerAdapter<'engine, Engine> {
    engine: &'engine mut Engine,
    origin_route: OriginRoute,
}

impl<Engine: NexusEngine> triad_runtime::RunnerEngines for NexusRunnerAdapter<'_, Engine> {
    type Reply = SignalOutput;
    type SemaWrite = SemaWriteInput;
    type SemaRead = SemaReadInput;
    type Effect = EffectCommand;
    type Work = NexusWork;

    fn decide_next_step(
        &mut self,
        work: Self::Work,
    ) -> triad_runtime::runner::RunnerNextStep<Self> {
        let action = self
            .engine
            .decide(work.with_origin_route(self.origin_route.clone()))
            .into_root();
        triad_runtime::NexusAction::into_next_step(action)
    }

    async fn apply_sema_write(&mut self, write: Self::SemaWrite) -> Self::Work {
        let output = self
            .engine
            .apply_sema_write(self.origin_route.clone(), write)
            .await;
        NexusWork::sema_write_completed(output)
    }

    async fn observe_sema_read(&mut self, read: Self::SemaRead) -> Self::Work {
        let output = self
            .engine
            .observe_sema_read(self.origin_route.clone(), read)
            .await;
        NexusWork::sema_read_completed(output)
    }

    async fn run_effect(&mut self, effect: Self::Effect) -> Self::Work {
        let output = self.engine.run_effect(effect).await;
        NexusWork::effect_completed(output)
    }

    fn budget_exhausted_reply(
        &self,
        exhausted: triad_runtime::ContinuationExhausted,
    ) -> Self::Reply {
        self.engine.budget_exhausted_reply(exhausted)
    }
}
