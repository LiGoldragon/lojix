#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LiveGeneration {
    pub deployment_identifier: signal_lojix::DeploymentIdentifier,
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub deployment_environment: signal_lojix::DeploymentEnvironment,
    pub generation_artifact: signal_lojix::GenerationArtifact,
    pub activation_effect: signal_lojix::ActivationEffect,
    pub generation_slot: signal_lojix::GenerationSlot,
    pub closure_path: signal_lojix::ClosurePath,
    pub source_revision_record: SourceRevisionRecord,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RequestedReference(pub signal_lojix::FlakeReference);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResolvedReference(pub signal_lojix::FlakeReference);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SourceRevisionRecord {
    pub source_revision_policy: signal_lojix::SourceRevisionPolicy,
    pub requested_reference: RequestedReference,
    pub resolved_reference: ResolvedReference,
    pub resolved_revision: ResolvedRevision,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResolvedRevision(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct GcRoot {
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub generation_slot: signal_lojix::GenerationSlot,
    pub closure_path: signal_lojix::ClosurePath,
    pub pin_label_option: std::option::Option<signal_lojix::PinLabel>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct EventLogEntry {
    pub event_log_position: signal_lojix::EventLogPosition,
    pub logged_event: LoggedEvent,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LoggedEvent {
    Deployment(signal_lojix::DeploymentPhaseEvent),
    CacheRetention(signal_lojix::CacheRetentionTransitionEvent),
    Container(ContainerLifecycleRecord),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContainerLifecycleRecord {
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub container_name: ContainerName,
    pub container_state: ContainerState,
    pub event_log_position: signal_lojix::EventLogPosition,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContainerName(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ContainerState {
    Starting,
    Started,
    Stopping,
    Stopped,
    Failed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeployJob {
    pub deployment_identifier: signal_lojix::DeploymentIdentifier,
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub deploy_job_phase: DeployJobPhase,
    pub closure_path_option: std::option::Option<signal_lojix::ClosurePath>,
    pub source_revision_policy: signal_lojix::SourceRevisionPolicy,
    pub flake_reference: signal_lojix::FlakeReference,
    pub flake_reference_option: std::option::Option<signal_lojix::FlakeReference>,
    pub resolved_revision_option: std::option::Option<ResolvedRevision>,
    pub deployment_transport: signal_lojix::DeploymentTransport,
    pub deployment_input_mode: signal_lojix::DeploymentInputMode,
    pub deployment_output_selector: signal_lojix::DeploymentOutputSelector,
    pub activation_backend: signal_lojix::ActivationBackend,
    pub nix_builder_spec_option: std::option::Option<signal_lojix::NixBuilderSpec>,
    pub boot_once_unit_option: std::option::Option<BootOnceUnit>,
    pub generation_slot_option: std::option::Option<signal_lojix::GenerationSlot>,
    pub persisted_flake_input_override_vector: std::vec::Vec<
        PersistedFlakeInputOverride,
    >,
    pub deploy_resume_stage: DeployResumeStage,
    pub phase_receipt_option: std::option::Option<PhaseReceipt>,
    pub actualized_deploy_submission_option: std::option::Option<
        meta_signal_lojix::ActualizedDeploySubmission,
    >,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum DeployJobPhase {
    Submitted,
    Building,
    Built,
    Copying,
    Activating,
    Activated,
    Failed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BootOnceUnit(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlakeInputName(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PersistedFlakeInputOverride {
    pub flake_input_name: FlakeInputName,
    pub persisted_flake_input_reference: PersistedFlakeInputReference,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlakeInputUrl(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NixArchiveHash(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PersistedFlakeInputReference {
    pub flake_input_url: FlakeInputUrl,
    pub nix_archive_hash: NixArchiveHash,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
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
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PhaseReceipt {
    pub event_log_position: signal_lojix::EventLogPosition,
    pub database_marker: signal_lojix::DatabaseMarker,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoredTestRun {
    pub test_run_record: signal_lojix::TestRunRecord,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoredDeploymentRecord {
    pub deployment_record: signal_lojix::DeploymentRecord,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NextDeploymentIdentifier(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NextGenerationIdentifier(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NextEventLogPosition(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct IdentifierAllocation {
    pub next_deployment_identifier: NextDeploymentIdentifier,
    pub next_generation_identifier: NextGenerationIdentifier,
    pub next_event_log_position: NextEventLogPosition,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeploymentOutboxRecord {
    pub deployment_identifier: signal_lojix::DeploymentIdentifier,
    pub transition_marker: signal_lojix::TransitionMarker,
    pub deployment_phase_event: signal_lojix::DeploymentPhaseEvent,
    pub outbox_delivery_state: OutboxDeliveryState,
    pub outbox_retry_count: OutboxRetryCount,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum OutboxDeliveryState {
    Pending,
    Dispatched,
    Acknowledged,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OutboxRetryCount(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PendingTransitionIntent {
    pub deployment_identifier: signal_lojix::DeploymentIdentifier,
    pub generation_identifier: signal_lojix::GenerationIdentifier,
    pub cluster_name: signal_lojix::ClusterName,
    pub node_name: signal_lojix::NodeName,
    pub deployment_phase: signal_lojix::DeploymentPhase,
    pub event_log_position: signal_lojix::EventLogPosition,
    pub immutable_revision_option: std::option::Option<signal_lojix::ImmutableRevision>,
    pub deployment_terminal_option: std::option::Option<
        signal_lojix::DeploymentTerminal,
    >,
    pub transition_ordinal: TransitionOrdinal,
    pub transition_marker_option: std::option::Option<signal_lojix::TransitionMarker>,
    pub transition_intent_state: TransitionIntentState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TransitionOrdinal(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TransitionIntentState {
    Pending,
    Bound,
    Appended,
    Acknowledged,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NexusConfigurationRecord {
    pub lojix_nexus_configuration: signal_lojix::LojixNexusConfiguration,
    pub meta_configure_occurred: signal_lojix::MetaConfigureOccurred,
}
