//! The stored records of lojix 0.10.6 (da0ad8a, the release running on
//! ouranos), frozen verbatim from its `src/runtime_model.rs`: every type
//! reachable from a family the store registers, and nothing else. They are
//! the layout every representation test compares the current records with.
#![allow(dead_code)]

macro_rules! runtime_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name(pub $inner);
    };
}

macro_rules! runtime_text {
    ($name:ident) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name(pub String);
    };
}

runtime_text!(ClusterName);
runtime_text!(NodeName);
runtime_text!(UserName);
runtime_text!(PinLabel);
runtime_text!(CommandProgram);
runtime_text!(CommandArgument);
runtime_text!(FailureDetail);
runtime_text!(ClosurePath);
runtime_text!(FlakeReference);
runtime_text!(NixStoreUri);
runtime_text!(SshDestination);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentTransport {
    pub nix_store_uri: NixStoreUri,
    pub ssh_destination: SshDestination,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DeploymentInputMode {
    Direct,
    Horizon,
}
runtime_text!(FlakeAttribute);
runtime_newtype!(DeploymentOutputSelector, FlakeAttribute);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ActivationBackend {
    NixosSystemdBootV1,
    HomeManagerNixProfileV1,
}
runtime_text!(NixBuilderSpec);
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
runtime_newtype!(EventLogPosition, u64);
runtime_newtype!(ExitCode, u64);
runtime_newtype!(CommitSequence, u64);
runtime_newtype!(StateDigest, u64);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SourceRevisionPolicy {
    RequireImmutable,
    ResolveAndRecord,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum GenerationArtifact {
    CompleteHost,
    BaseHost,
    UserEnvironment,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ActivationEffect {
    LiveActivation,
    BootProfile,
    TestActivation,
    BootOnceProfile,
    ProfileOnly,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum GenerationSlot {
    Current,
    BootPending,
    Rollback,
    Pinned,
    Recent,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TestMode {
    Hermetic,
    Live,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TestRunPhase {
    Submitted,
    BringingUp,
    Deploying,
    Asserting,
    TearingDown,
    Completed,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
runtime_newtype!(AdmissionMarker, StateMarker);
runtime_newtype!(TerminalMarker, StateMarker);
runtime_newtype!(TransitionMarker, StateMarker);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SourceRevisionRecord {
    pub source_revision_policy: SourceRevisionPolicy,
    pub requested_ref: FlakeReference,
    pub resolved_ref: FlakeReference,
    pub string: String,
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
    EvaluationFailed,
    BuildFailed,
    ClosureCopyFailed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentFailure {
    pub deployment_failure_stage: DeploymentFailureStage,
    pub deployment_terminal_reason: DeploymentTerminalReason,
    pub optional_failure_evidence: Option<FailureEvidence>,
}
/// What the failing stage actually reported. The detail is always present and
/// always bounded; the command is present only when a subprocess ran.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FailureEvidence {
    pub optional_failed_command: Option<FailedCommand>,
    pub failure_detail: FailureDetail,
    pub detail_truncated: bool,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FailedCommand {
    pub command_program: CommandProgram,
    pub command_argument_vector: Vec<CommandArgument>,
    pub optional_exit_code: Option<ExitCode>,
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TransitionIntentState {
    Pending,
    Bound,
    Appended,
    Acknowledged,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
pub enum HostDeployAction {
    Evaluate,
    Realize,
    SetBootProfile,
    ActivateNow,
    TestActivation,
    ScheduleBootOnce,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum HostComposition {
    CompleteHost,
    BaseHost,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UserEnvironmentAction {
    Realize,
    SetProfile,
    ActivateNow,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExtraSubstituter {
    pub url: String,
    pub public_key: String,
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
    pub optional_nix_builder_spec: Option<NixBuilderSpec>,
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
    pub optional_nix_builder_spec: Option<NixBuilderSpec>,
    pub extra_substituter_vector: Vec<ExtraSubstituter>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub enum DeploySubmission {
    Host(HostDeployment),
    UserEnvironment(UserEnvironmentDeployment),
}
runtime_text!(ContainerName);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ContainerState {
    Starting,
    Started,
    Stopping,
    Stopped,
    Failed,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PhaseReceipt {
    pub event_log_position: EventLogPosition,
    pub state_marker: StateMarker,
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
    pub optional_pin_label: Option<PinLabel>,
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
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
    pub optional_closure_path: Option<ClosurePath>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub struct NexusConfigurationRecord {
    pub state: nexus::ConfigurationState<signal_lojix::LojixNexusConfiguration>,
}
