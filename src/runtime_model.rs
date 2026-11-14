//! The nouns the Lojix runtime decides with.
//!
//! Every value whose shape a contract declares is the generated contract type
//! itself; every record the store keeps is generated from `ethos/memory.ethos`
//! into `crate::memory`; and every operation the engine attempts, a Memory
//! operation or a pipeline stage, with the outcome it comes to, is generated
//! from `ethos/operation.ethos` into `crate::operation`. All three are
//! re-exported here under the names the runtime reads them by. What stays
//! here is the one capability the runtime reads a carried value through, and
//! the conversions into stored records.

pub use crate::memory::{
    BootOnceUnit, ContainerLifecycleRecord, ContainerName, ContainerState, DeployJob,
    DeployJobPhase, DeployResumeStage, DeploymentOutboxRecord, EventLogEntry, FlakeInputName,
    FlakeInputUrl, GcRoot, IdentifierAllocation, LiveGeneration, LoggedEvent,
    NextDeploymentIdentifier, NextEventLogPosition, NextGenerationIdentifier,
    NexusConfigurationRecord, NixArchiveHash, OutboxDeliveryState, OutboxRetryCount,
    PendingTransitionIntent, PersistedFlakeInputOverride, PersistedFlakeInputReference,
    PhaseReceipt, RequestedReference, ResolvedReference, ResolvedRevision, SourceRevisionRecord,
    StoredDeploymentRecord, StoredTestRun, TransitionIntentState, TransitionOrdinal,
};
pub use crate::operation::{
    ActivateGenerationCommand, ActivatedGeneration, ActivationCommit, ActivationProfile,
    AppliedActivation, BringUpTestVmCommand, BuildTarget, BuiltClosure, CheckBuilt, ClosureOrigin,
    ContainerReceipt, ContainerTransition, CopiedClosure, CopyClosureCommand, EffectFailure,
    EffectStage, EvaluatedClosure, FlakeAuthRequest, GarbageCollected, HermeticCheckCommand,
    HorizonMaterializationCommand, IgnoredBuilder, MaterializationShape, MaterializedInputs,
    NixBuildCommand, NixEvalCommand, Operation, Outcome, PathInfoGcCommand, RejectionReason,
    RejectionReport, ResolvedFlake, TargetStoreBuild, TearDownTestVmCommand, TestVmBroughtUp,
    TestVmHost, TestVmTornDown, UserEnvironmentActivationProfile, UserEnvironmentMaterialization,
};
pub use meta_signal_lojix::{
    AcceptedTest, ActualizedDeploySubmission, AppliedPin, AppliedRetire, AppliedUnpin,
    DeployHandle, DeployRefusalReason, DeploySubmission, ExtraSubstituter, HostDeployment,
    NodeSelection, PinRejectionReason, PinRequest, QuickCheck, RefusedDeploy, RejectedDeploy,
    RejectedPin, RejectedRetire, RejectedTest, RejectedUnpin, RetireRejectionReason, RetireRequest,
    TestRejectionReason, TestRequest, TestRun, UnpinRejectionReason, UnpinRequest,
    UserEnvironmentDeployment,
};
pub use signal_lojix::{
    ActivationBackend, ActivationEffect, AdmissionMarker, CacheRetentionTransition,
    CacheRetentionTransitionEvent, ClosurePath, ClusterName, CommandArgument, CommandProgram,
    CommitSequence, ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason,
    DatabaseMarker, DeploymentEnvironment, DeploymentFailure, DeploymentFailureStage,
    DeploymentIdentifier, DeploymentInputMode, DeploymentLookup, DeploymentOutputSelector,
    DeploymentPhase, DeploymentPhaseEvent, DeploymentRecord, DeploymentRequestIdentity,
    DeploymentTerminal, DeploymentTerminalReason, DeploymentTransport, DeploymentWatch,
    DetailTruncated, EventLogPage, EventLogPosition, EventLogRange, ExitCode, FailedCommand,
    FailureDetail, FailureEvidence, FailureStage, FlakeAttribute, FlakeReference, Generation,
    GenerationArtifact, GenerationIdentifier, GenerationListing, GenerationLookup, GenerationSlot,
    HostComposition, HostDeployAction, HostSelection, ImmutableRevision, NixBuilderSpec,
    NixStoreUri, NixSystem, NodeName, NodeSelector, PinLabel, ProposalSource, QueryRejectionReason,
    RejectedQuery, RejectedUnwatch, RejectedWatch, RequestedDeploymentAction, SecretsDirectory,
    SecretsInput, Selection, SourceRevisionPolicy, SshDestination, StateDigest, StoreFailure,
    SubscriptionClose, SubscriptionClosed, SubscriptionOpened, SubscriptionToken, TerminalMarker,
    TestExecutionProfile, TestMode, TestOutcome, TestRunIdentifier, TestRunListing, TestRunLookup,
    TestRunPhase, TestRunRecord, TransitionMarker, UnwatchRejectionReason, UserEnvironmentAction,
    UserName, WatchRejectionReason,
};

/// A generated contract newtype, or a contract struct of one field, read
/// through the runtime's one capability for a value that carries one value.
macro_rules! carried {
    ($name:ident, $inner:ty) => {
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
    };
    ($name:ident, $field:ident: $inner:ty) => {
        impl crate::Payload for $name {
            type Carried = $inner;

            fn new($field: $inner) -> Self {
                Self { $field }
            }

            fn payload(&self) -> &$inner {
                &self.$field
            }

            fn into_payload(self) -> $inner {
                self.$field
            }
        }
    };
}

carried!(ClusterName, String);
carried!(NodeName, String);
carried!(UserName, String);
carried!(PinLabel, String);
carried!(CommandProgram, String);
carried!(CommandArgument, String);
carried!(FailureDetail, String);
carried!(ClosurePath, String);
carried!(FlakeReference, String);
carried!(NixStoreUri, String);
carried!(SshDestination, String);
carried!(FlakeAttribute, String);
carried!(NixBuilderSpec, String);
carried!(NixSystem, String);
carried!(ProposalSource, String);
carried!(SecretsDirectory, String);
carried!(ImmutableRevision, String);
carried!(DeploymentIdentifier, i64);
carried!(GenerationIdentifier, i64);
carried!(TestRunIdentifier, i64);
carried!(SubscriptionToken, i64);
carried!(EventLogPosition, i64);
carried!(ExitCode, i64);
carried!(CommitSequence, i64);
carried!(StateDigest, i64);
carried!(DetailTruncated, bool);
carried!(QuickCheck, Vec<NodeName>);
carried!(AdmissionMarker, database_marker: DatabaseMarker);
carried!(TerminalMarker, database_marker: DatabaseMarker);
carried!(TransitionMarker, database_marker: DatabaseMarker);
carried!(DeploymentOutputSelector, flake_attribute: FlakeAttribute);
carried!(GenerationLookup, generation_identifier: GenerationIdentifier);
carried!(DeploymentLookup, deployment_identifier: DeploymentIdentifier);
carried!(SubscriptionClose, subscription_token: SubscriptionToken);
carried!(SubscriptionClosed, subscription_token: SubscriptionToken);
carried!(RejectedWatch, watch_rejection_reason: WatchRejectionReason);
carried!(RejectedDeploy, deployment_record: DeploymentRecord);
carried!(ContainerName, String);
carried!(ResolvedRevision, String);
carried!(BootOnceUnit, String);
carried!(OutboxRetryCount, i64);
carried!(TransitionOrdinal, i64);
carried!(RequestedReference, FlakeReference);
carried!(ResolvedReference, FlakeReference);
carried!(ResolvedFlake, SourceRevisionRecord);
carried!(UserEnvironmentMaterialization, UserName);
carried!(MaterializedInputs, Vec<PersistedFlakeInputOverride>);

impl From<DeploymentRecord> for StoredDeploymentRecord {
    fn from(deployment_record: DeploymentRecord) -> Self {
        Self { deployment_record }
    }
}
impl From<TestRunRecord> for StoredTestRun {
    fn from(test_run_record: TestRunRecord) -> Self {
        Self { test_run_record }
    }
}
