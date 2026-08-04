//! Exact, private rkyv vocabulary for the Lojix v2 durable rows.
//!
//! This module is deliberately separate from the live Dotos schema. Migration
//! decodes a v2 store through these frozen types and explicitly translates it
//! into v3; it must never deserialize old bytes as current records merely
//! because a few fields happen to look similar.

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name(pub String);

        impl $name {
            pub fn payload(&self) -> &String {
                &self.0
            }
        }
    };
}

macro_rules! integer_newtype {
    ($name:ident) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name(pub u64);

        impl $name {
            pub fn payload(&self) -> &u64 {
                &self.0
            }
        }
    };
}

integer_newtype!(DeploymentIdentifier);
integer_newtype!(GenerationIdentifier);
integer_newtype!(TestRunIdentifier);
integer_newtype!(EventLogPosition);
string_newtype!(ClusterName);
string_newtype!(NodeName);
string_newtype!(ClosurePath);
string_newtype!(FlakeReference);
string_newtype!(PinLabel);
string_newtype!(PhaseDetail);
string_newtype!(ContainerName);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceRevisionPolicy {
    RequireImmutable,
    ResolveAndRecord,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationArtifact {
    CompleteHost,
    BaseHost,
    UserEnvironment,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationEffect {
    LiveActivation,
    BootProfile,
    TestActivation,
    BootOnceProfile,
    ProfileOnly,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationSlot {
    Current,
    BootPending,
    Rollback,
    Pinned,
    Recent,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestMode {
    Hermetic,
    Live,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestRunPhase {
    Submitted,
    BringingUp,
    Deploying,
    Asserting,
    TearingDown,
    Completed,
    Failed,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentPhase {
    Submitted,
    Building,
    Built,
    Copying,
    Activating,
    Activated,
    Failed,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheRetentionTransition {
    Pinned,
    Unpinned,
    Promoted,
    Demoted,
    Retired,
    Evicted,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    Starting,
    Started,
    Stopping,
    Stopped,
    Failed,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
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
pub struct SourceRevisionRecord {
    pub source_revision_policy: SourceRevisionPolicy,
    pub requested_ref: FlakeReference,
    pub resolved_ref: FlakeReference,
    pub string: String,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LiveGeneration {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
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
    pub label: Option<PinLabel>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeploymentPhaseEvent {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub deployment_phase: DeploymentPhase,
    pub event_log_position: EventLogPosition,
    pub optional_phase_detail: Option<PhaseDetail>,
    pub optional_source_revision_record: Option<SourceRevisionRecord>,
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
pub struct ContainerLifecycleRecord {
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub container: ContainerName,
    pub state: ContainerState,
    pub event_log_position: EventLogPosition,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LoggedEvent {
    Deployment(DeploymentPhaseEvent),
    CacheRetention(CacheRetentionTransitionEvent),
    Container(ContainerLifecycleRecord),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EventLogEntry {
    pub event_log_position: EventLogPosition,
    pub record: LoggedEvent,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeployJob {
    pub deployment_identifier: DeploymentIdentifier,
    pub generation_identifier: GenerationIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub phase: DeployJobPhase,
    pub closure_path: Option<ClosurePath>,
    pub source_revision_policy: SourceRevisionPolicy,
    pub requested_ref: FlakeReference,
    pub resolved_ref: Option<FlakeReference>,
    pub resolved_revision: Option<String>,
    pub resolved_target: Option<String>,
    pub boot_once_unit: Option<String>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredTestRun {
    pub test_run_identifier: TestRunIdentifier,
    pub cluster_name: ClusterName,
    pub node_name: NodeName,
    pub host: NodeName,
    pub mode: TestMode,
    pub phase: TestRunPhase,
    pub outcome: TestOutcome,
    pub closure_path: Option<ClosurePath>,
}
