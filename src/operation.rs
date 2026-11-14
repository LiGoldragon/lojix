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
}
