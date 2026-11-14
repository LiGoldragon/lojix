//! The nouns the Lojix runtime decides with.
//!
//! Every value whose shape a contract declares is the generated contract type
//! itself, and every record the store keeps is generated from
//! `ethos/memory.ethos` into `crate::memory`; both are re-exported here under
//! the names the runtime has always used. What stays declared here is the
//! engine's own operation plane: its memory reads and writes and their
//! outcomes, which `operation.ethos` is to generate.

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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MemoryReadInput {
    QueryGenerations(Selection),
    ReadEventLog(EventLogRange),
    QueryTestRuns(TestRunLookup),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MemoryReadOutput {
    GenerationsQueried(GenerationListing),
    EventLogRead(EventLogPage),
    TestRunsQueried(TestRunListing),
    ReadMissed(RejectionReport),
    /// The store failed under the read; the text names the failure.
    StoreRefused(String),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum MemoryWriteInput {
    OrdinaryConfigure(crate::NexusConfiguration),
    MetaConfigure(crate::NexusConfiguration),
    ReverseConfiguration,
    RecordDeploySubmitted(ActualizedDeploySubmission),
    RecordPhaseTransition(DeploymentPhaseEvent),
    RecordGenerationActivated(ActivationCommit),
    PinGeneration(PinRequest),
    UnpinGeneration(UnpinRequest),
    RetireGeneration(RetireRequest),
    RecordContainerTransition(ContainerTransition),
    RecordTestRun(TestRunRecord),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ActivationCommit {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_environment: DeploymentEnvironment,
    pub generation_slot: GenerationSlot,
    pub closure_path: ClosurePath,
    pub source_revision_record: SourceRevisionRecord,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ContainerTransition {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub container_name: ContainerName,
    pub container_state: ContainerState,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum MemoryWriteOutput {
    OrdinaryConfigured(ConfigurationReceipt),
    MetaConfigured(ConfigurationReceipt),
    ConfigurationReversed(ConfigurationReceipt),
    OrdinaryConfigurationRejected(ConfigurationRejection),
    MetaConfigurationRejected(ConfigurationRejection),
    DeploySubmitted(DeployHandle),
    PhaseRecorded(PhaseReceipt),
    GenerationActivated(AppliedActivation),
    GenerationPinned(AppliedPin),
    GenerationUnpinned(AppliedUnpin),
    GenerationRetired(AppliedRetire),
    ContainerRecorded(ContainerReceipt),
    TestRunRecorded(AcceptedTest),
    WriteRejected(RejectionReport),
    /// The store failed under the write; the text names the failure.
    StoreRefused(String),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppliedActivation {
    pub generation_identifier: GenerationIdentifier,
    pub generation_slot: GenerationSlot,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ContainerReceipt {
    pub event_log_position: EventLogPosition,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectionReport {
    pub rejection_reason: RejectionReason,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum RejectionReason {
    GenerationUnknown,
    NodeUnknown,
    ClusterUnknown,
    PlanNotApproved,
    PinLabelInUse,
    PinLabelUnknown,
    GenerationActive,
    GenerationPinned,
    EventLogPositionOutOfRange,
    ProposalSourceUnreachable,
    /// The durable write itself failed, so no record was produced.
    DurableWriteFailed,
}
impl triad_runtime::MemoryWriteInput for MemoryWriteInput {}
impl triad_runtime::MemoryWriteOutput for MemoryWriteOutput {}
impl triad_runtime::MemoryReadInput for MemoryReadInput {}
impl triad_runtime::MemoryReadOutput for MemoryReadOutput {}
