//! Handwritten durable and decision-plane nouns for lojix.
//!
//! Every value whose shape the contracts already declare is the generated
//! contract type itself, re-exported here under the name the runtime has
//! always used: signal-lojix's and meta-signal-lojix's scalars, markers,
//! enumerations and the records that agree with them field for field. What
//! stays declared here is Lojix's own: its stored records, its decision-plane
//! inputs and outputs, and the few twins whose shape differs from the
//! contract's (Generation, the test-run listing, the deploy submission with
//! its Horizon definition inside, the engine's five-way Selection).

pub use meta_signal_lojix::{
    AcceptedTest, AppliedPin, AppliedRetire, AppliedUnpin, DeployHandle, DeployRefusalReason,
    ExtraSubstituter, NodeSelection, PinRejectionReason, PinRequest, QuickCheck, RefusedDeploy,
    RejectedDeploy, RejectedPin, RejectedRetire, RejectedTest, RejectedUnpin,
    RetireRejectionReason, RetireRequest, TestRejectionReason, TestRequest, TestRun,
    UnpinRejectionReason, UnpinRequest,
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
    FailureDetail, FailureEvidence, FailureStage, FlakeAttribute, FlakeReference,
    GenerationArtifact, GenerationIdentifier, GenerationLookup, GenerationSlot, HostComposition,
    HostDeployAction, HostSelection, ImmutableRevision, NixBuilderSpec, NixStoreUri, NixSystem,
    NodeName, NodeSelector, PinLabel, ProposalSource, QueryRejectionReason, RejectedQuery,
    RejectedUnwatch, RejectedWatch, RequestedDeploymentAction, SecretsDirectory, SecretsInput,
    SourceRevisionPolicy, SshDestination, StateDigest, SubscriptionClose, SubscriptionClosed,
    SubscriptionOpened, SubscriptionToken, TerminalMarker, TestExecutionProfile, TestMode,
    TestOutcome, TestRunIdentifier, TestRunLookup, TestRunPhase, TransitionMarker,
    UnwatchRejectionReason, UserEnvironmentAction, UserName, WatchRejectionReason,
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

macro_rules! runtime_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
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

macro_rules! runtime_text {
    ($name:ident) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name(String);
        impl crate::Payload for $name {
            type Carried = String;

            fn new(payload: String) -> Self {
                Self(payload)
            }

            fn payload(&self) -> &String {
                &self.0
            }

            fn into_payload(self) -> String {
                self.0
            }
        }
        impl From<String> for $name {
            fn from(payload: String) -> Self {
                Self(payload)
            }
        }
        impl From<&str> for $name {
            fn from(payload: &str) -> Self {
                Self(payload.to_string())
            }
        }
    };
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    ByNode(NodeSelector),
    ByGeneration(GenerationLookup),
    ByDeployment(DeploymentLookup),
    ByEventLog(EventLogRange),
    ByTestRun(TestRunLookup),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SourceRevisionRecord {
    pub source_revision_policy: SourceRevisionPolicy,
    pub requested_ref: FlakeReference,
    pub resolved_ref: FlakeReference,
    pub string: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Generation {
    pub generation_identifier: GenerationIdentifier,
    pub deployment_identifier: DeploymentIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_artifact: GenerationArtifact,
    pub activation_effect: ActivationEffect,
    pub generation_slot: GenerationSlot,
    pub closure_path: ClosurePath,
    pub immutable_revision_option: Option<ImmutableRevision>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IdentifierAllocation {
    pub next_deployment_identifier: i64,
    pub next_generation_identifier: i64,
    pub next_event_log_position: i64,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GenerationListing {
    pub generation_vector: Vec<Generation>,
    pub deployment_record_vector: Vec<DeploymentRecord>,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum OutboxDeliveryState {
    Pending,
    Dispatched,
    Acknowledged,
}
runtime_newtype!(OutboxRetryCount, i64);
runtime_newtype!(TransitionOrdinal, i64);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentOutboxRecord {
    pub deployment_identifier: DeploymentIdentifier,
    pub transition_marker: TransitionMarker,
    pub deployment_phase_event: DeploymentPhaseEvent,
    pub outbox_delivery_state: OutboxDeliveryState,
    pub outbox_retry_count: OutboxRetryCount,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PendingTransitionIntent {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_phase: DeploymentPhase,
    pub event_log_position: EventLogPosition,
    pub immutable_revision_option: Option<ImmutableRevision>,
    pub deployment_terminal_option: Option<DeploymentTerminal>,
    pub transition_ordinal: TransitionOrdinal,
    pub transition_marker_option: Option<TransitionMarker>,
    pub transition_intent_state: TransitionIntentState,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum TransitionIntentState {
    Pending,
    Bound,
    Appended,
    Acknowledged,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestRunRecord {
    pub test_run_identifier: TestRunIdentifier,
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
    pub test_mode: TestMode,
    pub test_run_phase: TestRunPhase,
    pub test_outcome: TestOutcome,
    pub closure_path_option: Option<ClosurePath>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestRunListing {
    pub test_run_record_vector: Vec<TestRunRecord>,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub struct HostDeployment {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub host_composition: HostComposition,
    pub proposal_source: ProposalSource,
    pub secrets_input: SecretsInput,
    pub flake_reference: FlakeReference,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub horizon_definition_option: Option<horizon_lib::HorizonDefinition>,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub host_deploy_action: HostDeployAction,
    pub source_revision_policy: SourceRevisionPolicy,
    pub nix_builder_spec_option: Option<NixBuilderSpec>,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub struct UserEnvironmentDeployment {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub user_name: UserName,
    pub proposal_source: ProposalSource,
    pub secrets_input: SecretsInput,
    pub flake_reference: FlakeReference,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub horizon_definition_option: Option<horizon_lib::HorizonDefinition>,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub user_environment_action: UserEnvironmentAction,
    pub source_revision_policy: SourceRevisionPolicy,
    pub nix_builder_spec_option: Option<NixBuilderSpec>,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
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
    RecordDeploySubmitted(DeploySubmission),
    RecordPhaseTransition(DeploymentPhaseEvent),
    RecordGenerationActivated(ActivationCommit),
    PinGeneration(PinRequest),
    UnpinGeneration(UnpinRequest),
    RetireGeneration(RetireRequest),
    RecordContainerTransition(ContainerTransition),
    RecordTestRun(TestRunRecord),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum DeploySubmission {
    Host(HostDeployment),
    UserEnvironment(UserEnvironmentDeployment),
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
runtime_text!(ContainerName);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum ContainerState {
    Starting,
    Started,
    Stopping,
    Stopped,
    Failed,
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
pub struct PhaseReceipt {
    pub event_log_position: EventLogPosition,
    pub database_marker: DatabaseMarker,
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum OrdinaryIngress {
    Configure(crate::NexusConfiguration),
    Query(Selection),
    WatchDeployments(DeploymentWatch),
    Unwatch(SubscriptionClose),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum OrdinaryEgress {
    Configured(ConfigurationReceipt),
    ConfigurationRejected(ConfigurationRejection),
    Queried(GenerationListing),
    DeploymentEventsQueried(EventLogPage),
    TestRunsQueried(TestRunListing),
    Watching(SubscriptionOpened),
    Unwatched(SubscriptionClosed),
    QueryRejected(RejectedQuery),
    WatchRejected(RejectedWatch),
    UnwatchRejected(RejectedUnwatch),
    /// The store failed under the request; the text names the failure.
    StoreRefused(String),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum MetaIngress {
    Configure(crate::NexusConfiguration),
    ReverseConfiguration,
    Deploy(DeploySubmission),
    Pin(PinRequest),
    Unpin(UnpinRequest),
    Retire(RetireRequest),
    Test(TestRequest),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum MetaEgress {
    Configured(ConfigurationReceipt),
    ConfigurationRejected(ConfigurationRejection),
    ConfigurationReversed(ConfigurationReceipt),
    DeployAccepted(DeployHandle),
    DeployRejected(RejectedDeploy),
    DeployRefused(RefusedDeploy),
    DeployTerminal(DeploymentRecord),
    Pinned(AppliedPin),
    PinRejected(RejectedPin),
    Unpinned(AppliedUnpin),
    UnpinRejected(RejectedUnpin),
    Retired(AppliedRetire),
    RetireRejected(RejectedRetire),
    Tested(AcceptedTest),
    TestRejected(RejectedTest),
    /// The store failed under the request; the text names the failure.
    StoreRefused(String),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LiveGeneration {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_environment: DeploymentEnvironment,
    pub generation_artifact: GenerationArtifact,
    pub activation_effect: ActivationEffect,
    pub generation_slot: GenerationSlot,
    pub closure_path: ClosurePath,
    pub source_revision_record: SourceRevisionRecord,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GcRoot {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_slot: GenerationSlot,
    pub closure_path: ClosurePath,
    pub pin_label_option: Option<PinLabel>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EventLogEntry {
    pub event_log_position: EventLogPosition,
    pub logged_event: LoggedEvent,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LoggedEvent {
    Deployment(DeploymentPhaseEvent),
    CacheRetention(CacheRetentionTransitionEvent),
    Container(ContainerLifecycleRecord),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ContainerLifecycleRecord {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub container_name: ContainerName,
    pub container_state: ContainerState,
    pub event_log_position: EventLogPosition,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PersistedFlakeInputReference {
    pub url: String,
    pub nix_archive_hash: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PersistedFlakeInputOverride {
    pub string: String,
    pub persisted_flake_input_reference: PersistedFlakeInputReference,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeployResumeStage {
    ResolveFlakeAuth,
    MaterializeHorizon,
    RecordBuilding,
    NixEval,
    NixBuild,
    CopyClosure,
    ActivateGeneration,
    RecordGenerationActivated,
    FinishDeployment,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub struct DeployJob {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deploy_job_phase: DeployJobPhase,
    pub closure_path_option: Option<ClosurePath>,
    pub source_revision_policy: SourceRevisionPolicy,
    pub flake_reference: FlakeReference,
    pub flake_reference_option: Option<FlakeReference>,
    pub resolved_revision: Option<String>,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub nix_builder_spec_option: Option<NixBuilderSpec>,
    pub boot_once_unit: Option<String>,
    pub generation_slot_option: Option<GenerationSlot>,
    pub persisted_flake_input_override_vector: Vec<PersistedFlakeInputOverride>,
    pub deploy_resume_stage: DeployResumeStage,
    pub phase_receipt_option: Option<PhaseReceipt>,
    pub deploy_submission_option: Option<DeploySubmission>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeployJobPhase {
    Submitted,
    Building,
    Built,
    Copying,
    Activating,
    Activated,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredTestRun {
    pub test_run_identifier: TestRunIdentifier,
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
    pub test_mode: TestMode,
    pub test_run_phase: TestRunPhase,
    pub test_outcome: TestOutcome,
    pub closure_path_option: Option<ClosurePath>,
}

impl triad_runtime::MemoryWriteInput for MemoryWriteInput {}
impl triad_runtime::MemoryWriteOutput for MemoryWriteOutput {}
impl triad_runtime::MemoryReadInput for MemoryReadInput {}
impl triad_runtime::MemoryReadOutput for MemoryReadOutput {}

/// A deployment record as the store keeps it: the contract's own record,
/// whole, inside a record of Lojix's own. It archives as the record it holds.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredDeploymentRecord {
    pub deployment_record: DeploymentRecord,
}
impl From<DeploymentRecord> for StoredDeploymentRecord {
    fn from(deployment_record: DeploymentRecord) -> Self {
        Self { deployment_record }
    }
}
