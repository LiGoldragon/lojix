//! Handwritten Nexus work, effects, and runner behavior.
//!
//! This is executable Rust vocabulary owned by lojix, not a projection from a
//! schema language. A peer's input is answered through its contract's
//! generated dispatch before any of this runs; what is here drives the deploy
//! and test pipelines an admission hands to their actors.

use crate::Payload;
pub use crate::runtime_model::{
    ActivationBackend, ActivationEffect, ClosurePath, ClusterName, DeploymentIdentifier,
    DeploymentInputMode, DeploymentOutputSelector, DeploymentTransport, ExtraSubstituter,
    FlakeAttribute, FlakeReference, GenerationArtifact, GenerationIdentifier, GenerationSlot,
    HostComposition, HostDeployAction, NixBuilderSpec, NixStoreUri, NixSystem, NodeName, Operation,
    Outcome, PersistedFlakeInputOverride, ProposalSource, SecretsInput, SourceRevisionPolicy,
    SourceRevisionRecord, SshDestination, TestExecutionProfile, TestMode, UserEnvironmentAction,
    UserName,
};

/// What a driven pipeline ends with: the privileged contract's own output,
/// kept as executor evidence for the journal and tests. The peer that
/// submitted the job already holds its admission answer.
pub type PipelineOutput = meta_signal_lojix::Output;

macro_rules! flow_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name($inner);
        impl crate::Payload for $name {
            type Carried = $inner;

            fn new(payload: $inner) -> Self {
                Self(payload)
            }

            fn payload(&self) -> &$inner {
                &self.0
            }

            fn into_payload(self) -> $inner {
                self.0
            }
        }
        impl From<$inner> for $name {
            fn from(payload: $inner) -> Self {
                Self(payload)
            }
        }
    };
}

/// Where a deploy's closure is realized.
///
/// `Local` and `Remote` realize into the daemon host's own store (`Remote`
/// offloads the derivations to an explicit builder and imports the outputs
/// back). `TargetStore` realizes in the deployment transport's store: the
/// locally evaluated derivation closure is copied there and the target's own
/// daemon builds or substitutes it, so no output stages on the daemon host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildTarget {
    Local,
    Remote(NixBuilderSpec),
    TargetStore(TargetStoreBuild),
}
/// A build realized in the target's own store. `operator_node` is the daemon
/// host, which names the target-side GC root; `ignored_builder_option` carries
/// a request builder specification that does not apply to this build, so the
/// effect can say once that it was ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetStoreBuild {
    pub deployment_transport: DeploymentTransport,
    pub node_name: NodeName,
    pub operator_node: NodeName,
    pub ignored_builder_option: Option<NixBuilderSpec>,
}
/// Which store holds the built closure when the copy stage runs. A
/// `TargetStore` closure is already on the target, so its copy is a presence
/// check rather than a transfer.
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum ClosureOrigin {
    DaemonStore,
    TargetStore,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlakeAuthRequest {
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
#[derive(Clone, Debug, PartialEq)]
pub struct HorizonMaterializationCommand {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub horizon_definition: horizon_lib::HorizonDefinition,
    pub secrets_input: SecretsInput,
    pub materialization_shape: MaterializationShape,
}
flow_newtype!(MaterializedInputs, Vec<PersistedFlakeInputOverride>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NixEvalCommand {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_artifact: GenerationArtifact,
    pub flake_reference: FlakeReference,
    pub source_revision_record: SourceRevisionRecord,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub flake_input_override_vector: Vec<PersistedFlakeInputOverride>,
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
    pub closure_origin: ClosureOrigin,
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
#[derive(Clone, Debug, PartialEq)]
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
/// A failed effect, carrying what the stage actually reported. The evidence
/// is bounded and redacted by its producer, so it is safe to persist and to
/// hand back over the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectFailure {
    pub effect_stage: EffectStage,
    pub failure_evidence: crate::runtime_model::FailureEvidence,
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
/// What a pipeline step came back with: an operation's outcome, or an
/// effect's result.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusWork {
    Operated(Outcome),
    EffectCompleted(EffectResult),
}

/// What a pipeline decides to do next.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusAction {
    Operate(Operation),
    CommandEffect(EffectCommand),
    /// The pipeline has reached its end; what it ends with is evidence for
    /// the journal and tests.
    Finish(PipelineOutput),
}

impl From<SourceRevisionRecord> for EffectResult {
    fn from(value: SourceRevisionRecord) -> Self {
        Self::FlakeResolved(ResolvedFlake::new(value))
    }
}

impl From<Vec<PersistedFlakeInputOverride>> for EffectResult {
    fn from(value: Vec<PersistedFlakeInputOverride>) -> Self {
        Self::HorizonMaterialized(MaterializedInputs::new(value))
    }
}

impl From<EvaluatedClosure> for EffectResult {
    fn from(value: EvaluatedClosure) -> Self {
        Self::ClosureEvaluated(value)
    }
}

impl From<BuiltClosure> for EffectResult {
    fn from(value: BuiltClosure) -> Self {
        Self::ClosureBuilt(value)
    }
}

/// How many steps one pipeline may take before it is refused: every
/// operation and every effect is one step. A deploy takes about a dozen; the
/// limit bounds a decision that never reaches its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelineStepLimit(u32);

impl Default for PipelineStepLimit {
    fn default() -> Self {
        Self(32)
    }
}

/// Driving one admitted pipeline from the work it resumes at to its end:
/// decide the next action from the last step's result, carry it out, and
/// repeat until the decision is to finish or the step limit is spent.
///
/// Exception: `async fn` in a public trait. It is implemented by one type,
/// never used behind `dyn`, and its futures are awaited where the concrete
/// engine is known, so there is no `Send` bound for a caller to name.
#[allow(async_fn_in_trait)]
pub trait NexusEngine: crate::schema_runtime::Operating {
    fn step_limit(&self) -> PipelineStepLimit {
        PipelineStepLimit::default()
    }

    async fn run_effect(&mut self, input: EffectCommand) -> EffectResult;

    /// The pipeline's answer when its step limit is spent.
    fn step_limit_reached(&self, limit: PipelineStepLimit) -> PipelineOutput;

    fn decide(&mut self, input: NexusWork) -> NexusAction;

    async fn drive(&mut self, first_work: NexusWork) -> PipelineOutput {
        let limit = self.step_limit();
        let mut taken = 0;
        let mut work = first_work;
        loop {
            let action = self.decide(work);
            if !matches!(action, NexusAction::Finish(_)) {
                if taken >= limit.0 {
                    return self.step_limit_reached(limit);
                }
                taken += 1;
            }
            work = match action {
                NexusAction::Finish(output) => return output,
                NexusAction::Operate(operation) => NexusWork::Operated(self.operate(operation)),
                NexusAction::CommandEffect(effect) => {
                    NexusWork::EffectCompleted(self.run_effect(effect).await)
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An engine whose every decision is one more effect: the runner must
    /// stop it at the step limit and answer with the limit's refusal.
    struct EndlessPipeline {
        effects_run: u32,
    }

    impl crate::schema_runtime::Operating for EndlessPipeline {
        fn operate(&mut self, operation: Operation) -> Outcome {
            panic!("the endless pipeline makes no operation, got {operation:?}")
        }
    }

    impl NexusEngine for EndlessPipeline {
        async fn run_effect(&mut self, _input: EffectCommand) -> EffectResult {
            self.effects_run += 1;
            EffectResult::DetachedTestActivationDispatched
        }

        fn step_limit_reached(&self, _limit: PipelineStepLimit) -> PipelineOutput {
            PipelineOutput::Refused(signal_lojix::StoreFailure(format!(
                "stopped after {}",
                self.effects_run
            )))
        }

        fn decide(&mut self, _input: NexusWork) -> NexusAction {
            NexusAction::CommandEffect(EffectCommand::PathInfoGc(PathInfoGcCommand {
                cluster_name: ClusterName::new("cluster".into()),
                node_name: NodeName::new("node".into()),
            }))
        }
    }

    #[tokio::test]
    async fn a_pipeline_that_never_finishes_is_stopped_at_its_step_limit() {
        let mut engine = EndlessPipeline { effects_run: 0 };
        let output = engine
            .drive(NexusWork::EffectCompleted(
                EffectResult::DetachedTestActivationDispatched,
            ))
            .await;
        assert_eq!(
            output,
            PipelineOutput::Refused(signal_lojix::StoreFailure("stopped after 32".into()))
        );
    }
}
