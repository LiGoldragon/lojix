//! The pipeline runner: what a driven pipeline decides next, and the loop
//! that attempts each operation and decides again until the pipeline ends.
//!
//! Every operation a pipeline attempts, a Memory write or one of its stages,
//! and the outcome it comes to, is generated from `ethos/operation.ethos`.
//! A peer's input is answered through its contract's generated dispatch
//! before any of this runs; what is here drives the deploy and test
//! pipelines an admission hands to their actors.

pub use crate::runtime_model::{
    ActivateGenerationCommand, ActivatedGeneration, ActivationBackend, ActivationEffect,
    ActivationProfile, BringUpTestVmCommand, BuildTarget, BuiltClosure, CheckBuilt, ClosureOrigin,
    ClosurePath, ClusterName, CopiedClosure, CopyClosureCommand, DeploymentIdentifier,
    DeploymentInputMode, DeploymentOutputSelector, DeploymentTransport, EffectFailure, EffectStage,
    EvaluatedClosure, ExtraSubstituter, FlakeAttribute, FlakeAuthRequest, FlakeReference,
    GarbageCollected, GenerationArtifact, GenerationIdentifier, GenerationSlot,
    HermeticCheckCommand, HorizonMaterializationCommand, HostComposition, HostDeployAction,
    IgnoredBuilder, MaterializationShape, MaterializedInputs, NixBuildCommand, NixBuilderSpec,
    NixEvalCommand, NixStoreUri, NixSystem, NodeName, Operation, Outcome, PathInfoGcCommand,
    PersistedFlakeInputOverride, ProposalSource, ResolvedFlake, SecretsInput, SourceRevisionPolicy,
    SourceRevisionRecord, SshDestination, TargetStoreBuild, TearDownTestVmCommand,
    TestExecutionProfile, TestMode, TestVmBroughtUp, TestVmHost, TestVmTornDown,
    UserEnvironmentAction, UserEnvironmentActivationProfile, UserEnvironmentMaterialization,
    UserName,
};

/// What a driven pipeline ends with: the privileged contract's own output,
/// kept as executor evidence for the journal and tests. The peer that
/// submitted the job already holds its admission answer.
pub type PipelineOutput = meta_signal_lojix::Output;

/// What a pipeline decides to do next: attempt one operation, a Memory
/// operation or a stage, or end.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusAction {
    Operate(Operation),
    /// The pipeline has reached its end; what it ends with is evidence for
    /// the journal and tests.
    Finish(PipelineOutput),
}

/// How many steps one pipeline may take before it is refused: every
/// operation it attempts is one step. A deploy takes about a dozen; the
/// limit bounds a decision that never reaches its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelineStepLimit(u32);

impl Default for PipelineStepLimit {
    fn default() -> Self {
        Self(32)
    }
}

/// Driving one admitted pipeline from the outcome it resumes at to its end:
/// decide the next action from the last outcome, attempt it, and repeat
/// until the decision is to finish or the step limit is spent.
///
/// Exception: `async fn` in a public trait. It is implemented by one type,
/// never used behind `dyn`, and its futures are awaited where the concrete
/// engine is known, so there is no `Send` bound for a caller to name.
#[allow(async_fn_in_trait)]
pub trait NexusEngine: crate::schema_runtime::Operating {
    fn step_limit(&self) -> PipelineStepLimit {
        PipelineStepLimit::default()
    }

    /// Attempt one operation: a Memory operation is applied at once, a stage
    /// awaits the emissions it composes.
    async fn attempt(&mut self, operation: Operation) -> Outcome;

    /// The pipeline's answer when its step limit is spent.
    fn step_limit_reached(&self, limit: PipelineStepLimit) -> PipelineOutput;

    fn decide(&mut self, outcome: Outcome) -> NexusAction;

    async fn drive(&mut self, first_outcome: Outcome) -> PipelineOutput {
        let limit = self.step_limit();
        let mut taken = 0;
        let mut outcome = first_outcome;
        loop {
            let operation = match self.decide(outcome) {
                NexusAction::Finish(output) => return output,
                NexusAction::Operate(operation) => operation,
            };
            if taken >= limit.0 {
                return self.step_limit_reached(limit);
            }
            taken += 1;
            outcome = self.attempt(operation).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Payload as _;

    /// An engine whose every decision is one more stage: the runner must
    /// stop it at the step limit and answer with the limit's refusal.
    struct EndlessPipeline {
        stages_attempted: u32,
    }

    impl crate::schema_runtime::Operating for EndlessPipeline {
        fn operate(&mut self, operation: Operation) -> Outcome {
            panic!("the endless pipeline applies no Memory operation, got {operation:?}")
        }
    }

    impl NexusEngine for EndlessPipeline {
        async fn attempt(&mut self, _operation: Operation) -> Outcome {
            self.stages_attempted += 1;
            Outcome::DetachedTestActivationDispatched
        }

        fn step_limit_reached(&self, _limit: PipelineStepLimit) -> PipelineOutput {
            PipelineOutput::Refused(signal_lojix::StoreFailure(format!(
                "stopped after {}",
                self.stages_attempted
            )))
        }

        fn decide(&mut self, _outcome: Outcome) -> NexusAction {
            NexusAction::Operate(Operation::CollectingGarbage(PathInfoGcCommand {
                cluster_name: ClusterName::new("cluster".into()),
                node_name: NodeName::new("node".into()),
            }))
        }
    }

    #[tokio::test]
    async fn a_pipeline_that_never_finishes_is_stopped_at_its_step_limit() {
        let mut engine = EndlessPipeline {
            stages_attempted: 0,
        };
        let output = engine
            .drive(Outcome::DetachedTestActivationDispatched)
            .await;
        assert_eq!(
            output,
            PipelineOutput::Refused(signal_lojix::StoreFailure("stopped after 32".into()))
        );
    }
}
