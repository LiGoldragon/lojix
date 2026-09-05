//! Handwritten durable and decision-plane nouns for lojix.
//!
//! These Rust values are owned by the daemon itself. Public Interface values
//! enter and leave only through `adapters`; no build-time schema language or
//! generated source participates in the runtime model.

macro_rules! runtime_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
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

macro_rules! runtime_text {
    ($name:ident) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
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

runtime_text!(ClusterName);
runtime_text!(NodeName);
runtime_text!(UserName);
runtime_text!(PinLabel);
runtime_text!(ClosurePath);
runtime_text!(FlakeReference);
runtime_text!(NixStoreUri);
runtime_text!(SshDestination);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentTransport {
    pub nix_store_uri: NixStoreUri,
    pub ssh_destination: SshDestination,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentInputMode {
    Direct,
    Horizon,
}
runtime_text!(FlakeAttribute);
runtime_newtype!(DeploymentOutputSelector, FlakeAttribute);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum ActivationBackend {
    NixosSystemdBootV1,
    HomeManagerNixProfileV1,
}
runtime_text!(NixBuilderSpec);
runtime_text!(NixSystem);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestExecutionProfile {
    pub test_mode: TestMode,
    pub nix_system: NixSystem,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub optional_deployment_transport: Option<DeploymentTransport>,
}
runtime_text!(ProposalSource);
runtime_text!(SecretsDirectory);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SecretsInput {
    NoSecrets,
    SecretsDirectory(SecretsDirectory),
}
runtime_text!(ImmutableRevision);
runtime_newtype!(DeploymentIdentifier, u64);
runtime_newtype!(GenerationIdentifier, u64);
runtime_newtype!(TestRunIdentifier, u64);
runtime_newtype!(SubscriptionToken, u64);
runtime_newtype!(EventLogPosition, u64);
runtime_newtype!(CommitSequence, u64);
runtime_newtype!(StateDigest, u64);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum SourceRevisionPolicy {
    RequireImmutable,
    ResolveAndRecord,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum GenerationArtifact {
    CompleteHost,
    BaseHost,
    UserEnvironment,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum ActivationEffect {
    LiveActivation,
    BootProfile,
    TestActivation,
    BootOnceProfile,
    ProfileOnly,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum GenerationSlot {
    Current,
    BootPending,
    Rollback,
    Pinned,
    Recent,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum TestMode {
    Hermetic,
    Live,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum TestRunPhase {
    Submitted,
    BringingUp,
    Deploying,
    Asserting,
    TearingDown,
    Completed,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum FailureStage {
    BringUp,
    Deploy,
    Assert,
    TearDown,
    HermeticCheck,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TestOutcome {
    Pending,
    Passed,
    Failed(FailureStage),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StateMarker {
    pub commit_sequence: CommitSequence,
    pub state_digest: StateDigest,
}
runtime_newtype!(DatabaseMarker, StateMarker);
runtime_newtype!(AdmissionMarker, StateMarker);
runtime_newtype!(TerminalMarker, StateMarker);
runtime_newtype!(TransitionMarker, StateMarker);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeSelector {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub optional_generation_artifact: Option<GenerationArtifact>,
}
runtime_newtype!(GenerationLookup, GenerationIdentifier);
runtime_newtype!(DeploymentLookup, DeploymentIdentifier);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EventLogRange {
    pub from: EventLogPosition,
    pub until: EventLogPosition,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestRunLookup {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub optional_test_run_identifier: Option<TestRunIdentifier>,
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
    pub optional_immutable_revision: Option<ImmutableRevision>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DeploymentEnvironment {
    HostEnvironment,
    UserEnvironment(UserName),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RequestedDeploymentAction {
    Host(HostDeployAction),
    UserEnvironment(UserEnvironmentAction),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentLifecycle {
    Submitted,
    Building,
    Built,
    Copying,
    Activating,
    Activated,
    Completed,
    Rejected,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentFailureStage {
    Admission,
    FlakeAuth,
    MaterializeHorizon,
    Eval,
    Build,
    CopyClosure,
    Activate,
    Daemon,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentTerminalReason {
    ClusterUnknown,
    NodeUnknown,
    ProposalSourceUnreachable,
    FlakeReferenceMalformed,
    InvalidDeploymentRouting,
    BuilderUnreachable,
    SubstituterUnreachable,
    DeploymentInFlight,
    UnsupportedDeployAction,
    InternalError,
    ActivationFailed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentFailure {
    pub deployment_failure_stage: DeploymentFailureStage,
    pub deployment_terminal_reason: DeploymentTerminalReason,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DeploymentTerminal {
    Succeeded,
    Rejected(DeploymentTerminalReason),
    Failed(DeploymentFailure),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentRequestIdentity {
    pub deployment_environment: DeploymentEnvironment,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_artifact: GenerationArtifact,
    pub requested_deployment_action: RequestedDeploymentAction,
    pub activation_effect: ActivationEffect,
    pub source_revision_policy: SourceRevisionPolicy,
    pub optional_immutable_revision: Option<ImmutableRevision>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentRecord {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub deployment_request_identity: DeploymentRequestIdentity,
    pub optional_admission_marker: Option<AdmissionMarker>,
    pub deployment_lifecycle: DeploymentLifecycle,
    pub optional_terminal_marker: Option<TerminalMarker>,
    pub optional_deployment_terminal: Option<DeploymentTerminal>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IdentifierAllocation {
    pub next_deployment_identifier: u64,
    pub next_generation_identifier: u64,
    pub next_event_log_position: u64,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GenerationListing {
    pub generation_vector: Vec<Generation>,
    pub deployment_record_vector: Vec<DeploymentRecord>,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyMaterialQuery {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub proposal_source: ProposalSource,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyMaterialReport {
    pub node_name: NodeName,
    pub string_vector: Vec<String>,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum DeploymentPhase {
    Submitted,
    Building,
    Built,
    Copying,
    Activating,
    Activated,
    Completed,
    Rejected,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentPhaseEvent {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_phase: DeploymentPhase,
    pub event_log_position: EventLogPosition,
    pub state_marker: StateMarker,
    pub optional_immutable_revision: Option<ImmutableRevision>,
    pub optional_deployment_terminal: Option<DeploymentTerminal>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum OutboxDeliveryState {
    Pending,
    Dispatched,
    Acknowledged,
}
runtime_newtype!(OutboxRetryCount, u64);
runtime_newtype!(TransitionOrdinal, u64);
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
    pub optional_immutable_revision: Option<ImmutableRevision>,
    pub optional_deployment_terminal: Option<DeploymentTerminal>,
    pub transition_ordinal: TransitionOrdinal,
    pub optional_transition_marker: Option<TransitionMarker>,
    pub transition_intent_state: TransitionIntentState,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum TransitionIntentState {
    Pending,
    Bound,
    Appended,
    Acknowledged,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum CacheRetentionTransition {
    Pinned,
    Unpinned,
    Promoted,
    Demoted,
    Retired,
    Evicted,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CacheRetentionTransitionEvent {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub cache_retention_transition: CacheRetentionTransition,
    pub generation_slot: GenerationSlot,
    pub optional_generation_slot: Option<GenerationSlot>,
    pub optional_pin_label: Option<PinLabel>,
    pub event_log_position: EventLogPosition,
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
    pub optional_closure_path: Option<ClosurePath>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestRunListing {
    pub test_run_record_vector: Vec<TestRunRecord>,
    pub database_marker: DatabaseMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentWatch {
    pub optional_deployment_identifier: Option<DeploymentIdentifier>,
    pub optional_cluster_name: Option<ClusterName>,
    pub optional_node_name: Option<NodeName>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CacheRetentionWatch {
    pub optional_cluster_name: Option<ClusterName>,
    pub optional_node_name: Option<NodeName>,
}
runtime_newtype!(SubscriptionClose, SubscriptionToken);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionOpened {
    pub subscription_token: SubscriptionToken,
    pub commit_sequence: CommitSequence,
}
runtime_newtype!(SubscriptionClosed, SubscriptionToken);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum HostDeployAction {
    Evaluate,
    Realize,
    SetBootProfile,
    ActivateNow,
    TestActivation,
    ScheduleBootOnce,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum HostComposition {
    CompleteHost,
    BaseHost,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum UserEnvironmentAction {
    Realize,
    SetProfile,
    ActivateNow,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum HostSelection {
    DefaultHost,
    OnHost(NodeName),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NodeSelection {
    Nodes(Vec<NodeName>),
    All,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestRun {
    pub cluster_name: ClusterName,
    pub node_selection: NodeSelection,
    pub host_selection: HostSelection,
    pub test_execution_profile: TestExecutionProfile,
}
runtime_newtype!(QuickCheck, Vec<NodeName>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TestRequest {
    Run(TestRun),
    Check(QuickCheck),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExtraSubstituter {
    pub url: String,
    pub public_key: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HostDeployment {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub host_composition: HostComposition,
    pub proposal_source: ProposalSource,
    pub secrets_input: SecretsInput,
    pub flake_reference: FlakeReference,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub host_deploy_action: HostDeployAction,
    pub source_revision_policy: SourceRevisionPolicy,
    pub optional_nix_builder_spec: Option<NixBuilderSpec>,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UserEnvironmentDeployment {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub user_name: UserName,
    pub proposal_source: ProposalSource,
    pub secrets_input: SecretsInput,
    pub flake_reference: FlakeReference,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub user_environment_action: UserEnvironmentAction,
    pub source_revision_policy: SourceRevisionPolicy,
    pub optional_nix_builder_spec: Option<NixBuilderSpec>,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PinRequest {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_identifier: GenerationIdentifier,
    pub pin_label: PinLabel,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UnpinRequest {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub pin_label: PinLabel,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetireRequest {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_identifier: GenerationIdentifier,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeployHandle {
    pub deployment_identifier: DeploymentIdentifier,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AcceptedTest {
    pub test_run_identifier: TestRunIdentifier,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppliedPin {
    pub generation_identifier: GenerationIdentifier,
    pub pin_label: PinLabel,
    pub from_slot: GenerationSlot,
    pub to_slot: GenerationSlot,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppliedUnpin {
    pub generation_identifier: GenerationIdentifier,
    pub pin_label: PinLabel,
    pub from_slot: GenerationSlot,
    pub to_slot: GenerationSlot,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppliedRetire {
    pub generation_identifier: GenerationIdentifier,
    pub generation_slot: GenerationSlot,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SemaReadInput {
    QueryGenerations(Selection),
    ReadEventLog(EventLogRange),
    CheckKeyMaterial(KeyMaterialQuery),
    QueryTestRuns(TestRunLookup),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SemaReadOutput {
    GenerationsQueried(GenerationListing),
    EventLogRead(EventLogPage),
    KeyMaterialChecked(KeyMaterialReport),
    TestRunsQueried(TestRunListing),
    ReadMissed(RejectionReport),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EventLogPage {
    pub deployment_phase_event_vector: Vec<DeploymentPhaseEvent>,
    pub cache_retention_transition_event_vector: Vec<CacheRetentionTransitionEvent>,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SemaWriteInput {
    RecordDeploySubmitted(DeploySubmission),
    RecordPhaseTransition(DeploymentPhaseEvent),
    RecordGenerationActivated(ActivationCommit),
    PinGeneration(PinRequest),
    UnpinGeneration(UnpinRequest),
    RetireGeneration(RetireRequest),
    RecordContainerTransition(ContainerTransition),
    RecordTestRun(TestRunRecord),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SemaWriteOutput {
    DeploySubmitted(DeployHandle),
    PhaseRecorded(PhaseReceipt),
    GenerationActivated(AppliedActivation),
    GenerationPinned(AppliedPin),
    GenerationUnpinned(AppliedUnpin),
    GenerationRetired(AppliedRetire),
    ContainerRecorded(ContainerReceipt),
    TestRunRecorded(AcceptedTest),
    WriteRejected(RejectionReport),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PhaseReceipt {
    pub event_log_position: EventLogPosition,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppliedActivation {
    pub generation_identifier: GenerationIdentifier,
    pub generation_slot: GenerationSlot,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ContainerReceipt {
    pub event_log_position: EventLogPosition,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectionReport {
    pub rejection_reason: RejectionReason,
    pub state_marker: StateMarker,
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
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum QueryRejectionReason {
    GenerationUnknown,
    NodeUnknown,
    EventLogPositionOutOfRange,
    MalformedSelector,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum WatchRejectionReason {
    SubscriptionLimitReached,
    MalformedWatch,
    StreamUnavailable,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum UnwatchRejectionReason {
    SubscriptionTokenUnknown,
    SubscriptionAlreadyClosed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum KeyMaterialCheckRejectionReason {
    NodeUnknown,
    ProposalSourceUnreachable,
    HostUnreachable,
    PublicationMalformed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedQuery {
    pub query_rejection_reason: QueryRejectionReason,
    pub state_marker: StateMarker,
}
runtime_newtype!(RejectedWatch, WatchRejectionReason);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedUnwatch {
    pub unwatch_rejection_reason: UnwatchRejectionReason,
    pub subscription_token: SubscriptionToken,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedKeyMaterialCheck {
    pub key_material_check_rejection_reason: KeyMaterialCheckRejectionReason,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum PinRejectionReason {
    GenerationUnknown,
    NodeUnknown,
    PinLabelInUse,
    PinSlotExhausted,
    InternalError,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum UnpinRejectionReason {
    PinLabelUnknown,
    NodeUnknown,
    GenerationNotPinned,
    InternalError,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum RetireRejectionReason {
    GenerationUnknown,
    NodeUnknown,
    GenerationActive,
    GenerationPinned,
    InternalError,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum TestRejectionReason {
    ClusterUnknown,
    NodeUnknown,
    VmHostNotDeclaredForNode,
    HostDeclaresNoVmHost,
    NoTestDefaults,
    LiveNotYetEnabled,
    SubstrateUnavailable,
    InternalError,
}
runtime_newtype!(RejectedDeploy, DeploymentRecord);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedPin {
    pub pin_rejection_reason: PinRejectionReason,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedUnpin {
    pub unpin_rejection_reason: UnpinRejectionReason,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedRetire {
    pub retire_rejection_reason: RetireRejectionReason,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RejectedTest {
    pub test_rejection_reason: TestRejectionReason,
    pub state_marker: StateMarker,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum OrdinaryIngress {
    Query(Selection),
    WatchDeployments(DeploymentWatch),
    WatchCacheRetention(CacheRetentionWatch),
    Unwatch(SubscriptionClose),
    CheckHostKeyMaterial(KeyMaterialQuery),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum OrdinaryEgress {
    Queried(GenerationListing),
    DeploymentEventsQueried(EventLogPage),
    TestRunsQueried(TestRunListing),
    Watching(SubscriptionOpened),
    Unwatched(SubscriptionClosed),
    KeyMaterialChecked(KeyMaterialReport),
    QueryRejected(RejectedQuery),
    WatchRejected(RejectedWatch),
    UnwatchRejected(RejectedUnwatch),
    KeyMaterialCheckRejected(RejectedKeyMaterialCheck),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MetaIngress {
    Deploy(DeploySubmission),
    Pin(PinRequest),
    Unpin(UnpinRequest),
    Retire(RetireRequest),
    Test(TestRequest),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MetaEgress {
    DeployAccepted(DeployHandle),
    DeployRejected(RejectedDeploy),
    DeployTerminal(DeploymentRecord),
    Pinned(AppliedPin),
    PinRejected(RejectedPin),
    Unpinned(AppliedUnpin),
    UnpinRejected(RejectedUnpin),
    Retired(AppliedRetire),
    RetireRejected(RejectedRetire),
    Tested(AcceptedTest),
    TestRejected(RejectedTest),
}
runtime_newtype!(LiveSetTable, Vec<LiveGeneration>);
runtime_newtype!(DeploymentRecordTable, Vec<DeploymentRecord>);
runtime_newtype!(IdentifierAllocationTable, Vec<IdentifierAllocation>);
runtime_newtype!(DeploymentOutboxTable, Vec<DeploymentOutboxRecord>);
runtime_newtype!(PendingTransitionIntentTable, Vec<PendingTransitionIntent>);
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
runtime_newtype!(GcRootsTable, Vec<GcRoot>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GcRoot {
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub generation_slot: GenerationSlot,
    pub closure_path: ClosurePath,
    pub optional_pin_label: Option<PinLabel>,
}
runtime_newtype!(EventLogTable, Vec<EventLogEntry>);
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
runtime_newtype!(ContainerLifecycleTable, Vec<ContainerLifecycleRecord>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ContainerLifecycleRecord {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub container_name: ContainerName,
    pub container_state: ContainerState,
    pub event_log_position: EventLogPosition,
}
runtime_newtype!(DeployJobTable, Vec<DeployJob>);
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeployJob {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deploy_job_phase: DeployJobPhase,
    pub optional_closure_path: Option<ClosurePath>,
    pub source_revision_policy: SourceRevisionPolicy,
    pub flake_reference: FlakeReference,
    pub optional_flake_reference: Option<FlakeReference>,
    pub resolved_revision: Option<String>,
    pub deployment_transport: DeploymentTransport,
    pub deployment_input_mode: DeploymentInputMode,
    pub deployment_output_selector: DeploymentOutputSelector,
    pub activation_backend: ActivationBackend,
    pub optional_nix_builder_spec: Option<NixBuilderSpec>,
    pub boot_once_unit: Option<String>,
    pub optional_generation_slot: Option<GenerationSlot>,
    pub persisted_flake_input_override_vector: Vec<PersistedFlakeInputOverride>,
    pub deploy_resume_stage: DeployResumeStage,
    pub optional_phase_receipt: Option<PhaseReceipt>,
    pub optional_deploy_submission: Option<DeploySubmission>,
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
runtime_newtype!(TestRunTable, Vec<StoredTestRun>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredTestRun {
    pub test_run_identifier: TestRunIdentifier,
    pub cluster_name: ClusterName,
    pub node: NodeName,
    pub host: NodeName,
    pub test_mode: TestMode,
    pub test_run_phase: TestRunPhase,
    pub test_outcome: TestOutcome,
    pub optional_closure_path: Option<ClosurePath>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Read(SemaReadInput),
    Write(SemaWriteInput),
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Output {
    ReadCompleted(SemaReadOutput),
    WriteCompleted(SemaWriteOutput),
}

impl triad_runtime::SemaWriteInput for SemaWriteInput {}
impl triad_runtime::SemaWriteOutput for SemaWriteOutput {}
impl triad_runtime::SemaReadInput for SemaReadInput {}
impl triad_runtime::SemaReadOutput for SemaReadOutput {}
