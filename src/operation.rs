#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ActivationCommit {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub deployment_environment: signal_lojix::DeploymentEnvironment,
    pub generation_slot: signal_lojix::GenerationSlot,
    pub closure_path: signal_lojix::ClosurePath,
    pub source_revision_record: crate::memory::SourceRevisionRecord,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContainerTransition {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub container_name: crate::memory::ContainerName,
    pub container_state: crate::memory::ContainerState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AppliedActivation {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub generation_slot: signal_lojix::GenerationSlot,
    pub database_marker: signal_lojix::DatabaseMarker,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContainerReceipt {
    pub event_log_position: signal_lojix::EventLogPosition,
    pub database_marker: signal_lojix::DatabaseMarker,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RejectionReport {
    pub rejection_reason: RejectionReason,
    pub database_marker: signal_lojix::DatabaseMarker,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
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
    DurableWriteFailed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlakeAuthRequest {
    pub flake_reference: signal_lojix::FlakeReference,
    pub source_revision_policy: signal_lojix::SourceRevisionPolicy,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResolvedFlake(pub crate::memory::SourceRevisionRecord);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HorizonMaterializationCommand {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub horizon_definition: horizon_lib::HorizonDefinition,
    pub secrets_input: signal_lojix::SecretsInput,
    pub materialization_shape: MaterializationShape,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum MaterializationShape {
    CompleteHost,
    BaseHost,
    UserEnvironment(UserEnvironmentMaterialization),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UserEnvironmentMaterialization(pub signal_lojix::UserName);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct MaterializedInputs(
    pub std::vec::Vec<crate::memory::PersistedFlakeInputOverride>,
);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NixEvalCommand {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub generation_artifact: signal_lojix::GenerationArtifact,
    pub flake_reference: signal_lojix::FlakeReference,
    pub source_revision_record: crate::memory::SourceRevisionRecord,
    pub deployment_output_selector: signal_lojix::DeploymentOutputSelector,
    pub persisted_flake_input_override_vector: std::vec::Vec<
        crate::memory::PersistedFlakeInputOverride,
    >,
    pub build_target: BuildTarget,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BuildTarget {
    Local,
    Remote(signal_lojix::NixBuilderSpec),
    TargetStore(TargetStoreBuild),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TargetStoreBuild {
    pub deployment_transport: signal_lojix::DeploymentTransport,
    pub node_name: signal_lojix::NodeName,
    pub operator_node: crate::emission::OperatorNode,
    pub ignored_builder_option: std::option::Option<IgnoredBuilder>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct IgnoredBuilder(pub signal_lojix::NixBuilderSpec);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct EvaluatedClosure {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NixBuildCommand {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub closure_path: signal_lojix::ClosurePath,
    pub build_target: BuildTarget,
    pub extra_substituter_vector: std::vec::Vec<meta_signal_lojix::ExtraSubstituter>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BuiltClosure {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CopyClosureCommand {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub node_name: signal_lojix::NodeName,
    pub deployment_transport: signal_lojix::DeploymentTransport,
    pub closure_path: signal_lojix::ClosurePath,
    pub closure_origin: ClosureOrigin,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ClosureOrigin {
    DaemonStore,
    TargetStore,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CopiedClosure {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub node_name: signal_lojix::NodeName,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ActivateGenerationCommand {
    pub deployment_identifier: signal_lojix::DeploymentIdentifier,
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub deployment_transport: signal_lojix::DeploymentTransport,
    pub closure_path: signal_lojix::ClosurePath,
    pub activation_effect: signal_lojix::ActivationEffect,
    pub activation_backend: signal_lojix::ActivationBackend,
    pub activation_profile: ActivationProfile,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ActivationProfile {
    Host(signal_lojix::HostDeployAction),
    UserEnvironment(UserEnvironmentActivationProfile),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct UserEnvironmentActivationProfile {
    pub user_environment_action: signal_lojix::UserEnvironmentAction,
    pub user_name: signal_lojix::UserName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ActivatedGeneration {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub node_name: signal_lojix::NodeName,
    pub generation_slot: signal_lojix::GenerationSlot,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PathInfoGcCommand {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GarbageCollected {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub collected_paths: crate::emission::CollectedPaths,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HermeticCheckCommand {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub flake_reference: signal_lojix::FlakeReference,
    pub test_execution_profile: signal_lojix::TestExecutionProfile,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CheckBuilt {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub closure_path: signal_lojix::ClosurePath,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BringUpTestVmCommand {
    pub cluster_name: signal_lojix::ClusterName,
    pub guest_node: crate::emission::GuestNode,
    pub test_vm_host: TestVmHost,
    pub deployment_transport: signal_lojix::DeploymentTransport,
    pub closure_path: signal_lojix::ClosurePath,
    pub guest_address: crate::emission::GuestAddress,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TearDownTestVmCommand {
    pub cluster_name: signal_lojix::ClusterName,
    pub guest_node: crate::emission::GuestNode,
    pub test_vm_host: TestVmHost,
    pub deployment_transport: signal_lojix::DeploymentTransport,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TestVmHost(pub signal_lojix::NodeName);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TestVmBroughtUp {
    pub cluster_name: signal_lojix::ClusterName,
    pub guest_node: crate::emission::GuestNode,
    pub test_vm_host: TestVmHost,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TestVmTornDown {
    pub cluster_name: signal_lojix::ClusterName,
    pub guest_node: crate::emission::GuestNode,
    pub test_vm_host: TestVmHost,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct EffectFailure {
    pub effect_stage: EffectStage,
    pub failure_evidence: signal_lojix::FailureEvidence,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
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
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    OrdinaryConfigure(signal_lojix::LojixNexusConfiguration),
    MetaConfigure(signal_lojix::LojixNexusConfiguration),
    ReverseConfiguration,
    RecordDeploySubmitted(meta_signal_lojix::ActualizedDeploySubmission),
    RecordPhaseTransition(signal_lojix::DeploymentPhaseEvent),
    RecordGenerationActivated(ActivationCommit),
    PinGeneration(meta_signal_lojix::PinRequest),
    UnpinGeneration(meta_signal_lojix::UnpinRequest),
    RetireGeneration(meta_signal_lojix::RetireRequest),
    RecordContainerTransition(ContainerTransition),
    RecordTestRun(signal_lojix::TestRunRecord),
    QueryGenerations(signal_lojix::Selection),
    ReadEventLog(signal_lojix::EventLogRange),
    QueryTestRuns(signal_lojix::TestRunLookup),
    ResolvingFlake(FlakeAuthRequest),
    MaterializingHorizon(HorizonMaterializationCommand),
    EvaluatingClosure(NixEvalCommand),
    BuildingClosure(NixBuildCommand),
    CopyingClosure(CopyClosureCommand),
    ActivatingGeneration(ActivateGenerationCommand),
    CollectingGarbage(PathInfoGcCommand),
    BuildingHermeticCheck(HermeticCheckCommand),
    BringingUpTestVm(BringUpTestVmCommand),
    TearingDownTestVm(TearDownTestVmCommand),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    OrdinaryConfigured(signal_lojix::ConfigurationReceipt),
    MetaConfigured(signal_lojix::ConfigurationReceipt),
    ConfigurationReversed(signal_lojix::ConfigurationReceipt),
    OrdinaryConfigurationRejected(signal_lojix::ConfigurationRejection),
    MetaConfigurationRejected(signal_lojix::ConfigurationRejection),
    DeploySubmitted(meta_signal_lojix::DeployHandle),
    PhaseRecorded(crate::memory::PhaseReceipt),
    GenerationActivated(AppliedActivation),
    GenerationPinned(meta_signal_lojix::AppliedPin),
    GenerationUnpinned(meta_signal_lojix::AppliedUnpin),
    GenerationRetired(meta_signal_lojix::AppliedRetire),
    ContainerRecorded(ContainerReceipt),
    TestRunRecorded(meta_signal_lojix::AcceptedTest),
    GenerationsQueried(signal_lojix::GenerationListing),
    EventLogRead(signal_lojix::EventLogPage),
    TestRunsQueried(signal_lojix::TestRunListing),
    WriteRejected(RejectionReport),
    ReadMissed(RejectionReport),
    StoreRefused(signal_lojix::StoreFailure),
    FlakeResolved(ResolvedFlake),
    HorizonMaterialized(MaterializedInputs),
    ClosureEvaluated(EvaluatedClosure),
    ClosureBuilt(BuiltClosure),
    ClosureCopied(CopiedClosure),
    ActivationCompleted(ActivatedGeneration),
    DetachedActivationDispatched,
    PathsCollected(GarbageCollected),
    HermeticCheckBuilt(CheckBuilt),
    TestVmStarted(TestVmBroughtUp),
    TestVmStopped(TestVmTornDown),
    EffectFailed(EffectFailure),
}
