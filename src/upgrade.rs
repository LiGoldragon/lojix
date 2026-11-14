//! The one family whose stored layout moved: the deploy job.
//!
//! Lojix 0.10.6 kept a deploy job's submission as its own copy of the meta
//! contract's, with the Horizon definition inside each deployment. The
//! generated `DeployJob` holds the contract's `ActualizedDeploySubmission`
//! whole, the definition beside the deployment, so the family moves from
//! schema hash `[5; 32]` to `[14; 32]` and memory-engine upgrades it on open,
//! every row in one write. The 0.10.6 record is frozen here, hand-written,
//! as an earlier layout is; everything it holds that did not move is the
//! current type, whose bytes `tests/memory_representation.rs` proves equal.

use memory_engine::{Decision, MemoryDefinition, MemoryVersion, SchemaHash};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::LojixRecord;
use crate::runtime_model::{
    ActivationBackend, ClosurePath, ClusterName, DeployJob, DeployJobPhase, DeployResumeStage,
    DeploymentIdentifier, DeploymentInputMode, DeploymentOutputSelector, DeploymentTransport,
    ExtraSubstituter, FlakeReference, GenerationIdentifier, GenerationSlot, HostComposition,
    HostDeployAction, NixBuilderSpec, NodeName, PersistedFlakeInputOverride,
    PersistedFlakeInputReference, PhaseReceipt, ProposalSource, ResolvedRevision, SecretsInput,
    SourceRevisionPolicy, UserEnvironmentAction, UserName,
};

/// The deploy job as lojix 0.10.6 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct EarlierDeployJob {
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
    pub persisted_flake_input_override_vector: Vec<EarlierFlakeInputOverride>,
    pub deploy_resume_stage: DeployResumeStage,
    pub phase_receipt_option: Option<PhaseReceipt>,
    pub deploy_submission_option: Option<EarlierDeploySubmission>,
}

/// A flake input override as lojix 0.10.6 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct EarlierFlakeInputOverride {
    pub string: String,
    pub persisted_flake_input_reference: EarlierFlakeInputReference,
}

/// A flake input reference as lojix 0.10.6 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct EarlierFlakeInputReference {
    pub url: String,
    pub nix_archive_hash: String,
}

/// A deploy submission as lojix 0.10.6 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub enum EarlierDeploySubmission {
    Host(EarlierHostDeployment),
    UserEnvironment(EarlierUserEnvironmentDeployment),
}

/// A host deployment as lojix 0.10.6 stored it, its Horizon definition inside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct EarlierHostDeployment {
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

/// A user-environment deployment as lojix 0.10.6 stored it, its Horizon
/// definition inside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct EarlierUserEnvironmentDeployment {
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

/// The 0.10.6 deploy job is version 1 of the family, under the schema hash
/// 0.10.6 registered it with.
impl MemoryDefinition for EarlierDeployJob {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::new([5; 32])
    }
}

/// The generated deploy job is version 2.
impl MemoryDefinition for DeployJob {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::new(<DeployJob as LojixRecord>::SCHEMA_HASH)
    }
}

/// The upgrade moves every field across unchanged but the submission, whose
/// Horizon definition leaves the deployment and stands beside it. It cannot
/// refuse a row.
impl memory_engine::UpgradeFrom<EarlierDeployJob> for DeployJob {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(earlier: EarlierDeployJob) -> Decision<Self, Self::Refusal> {
        Decision::Ready(DeployJob {
            deployment_identifier: earlier.deployment_identifier,
            generation_identifier: earlier.generation_identifier,
            cluster_name: earlier.cluster_name,
            node_name: earlier.node_name,
            deploy_job_phase: earlier.deploy_job_phase,
            closure_path_option: earlier.closure_path_option,
            source_revision_policy: earlier.source_revision_policy,
            flake_reference: earlier.flake_reference,
            flake_reference_option: earlier.flake_reference_option,
            resolved_revision_option: earlier.resolved_revision.map(ResolvedRevision),
            deployment_transport: earlier.deployment_transport,
            deployment_input_mode: earlier.deployment_input_mode,
            deployment_output_selector: earlier.deployment_output_selector,
            activation_backend: earlier.activation_backend,
            nix_builder_spec_option: earlier.nix_builder_spec_option,
            boot_once_unit_option: earlier
                .boot_once_unit
                .map(crate::runtime_model::BootOnceUnit),
            generation_slot_option: earlier.generation_slot_option,
            persisted_flake_input_override_vector: earlier
                .persisted_flake_input_override_vector
                .into_iter()
                .map(|flake_input| PersistedFlakeInputOverride {
                    flake_input_name: crate::runtime_model::FlakeInputName(flake_input.string),
                    persisted_flake_input_reference: PersistedFlakeInputReference {
                        flake_input_url: crate::runtime_model::FlakeInputUrl(
                            flake_input.persisted_flake_input_reference.url,
                        ),
                        nix_archive_hash: crate::runtime_model::NixArchiveHash(
                            flake_input.persisted_flake_input_reference.nix_archive_hash,
                        ),
                    },
                })
                .collect(),
            deploy_resume_stage: earlier.deploy_resume_stage,
            phase_receipt_option: earlier.phase_receipt_option,
            actualized_deploy_submission_option: earlier
                .deploy_submission_option
                .map(meta_signal_lojix::ActualizedDeploySubmission::from),
        })
    }
}

/// An earlier submission becomes the contract's: the deployment as the meta
/// contract declares it, and its Horizon definition beside it.
impl From<EarlierDeploySubmission> for meta_signal_lojix::ActualizedDeploySubmission {
    fn from(earlier: EarlierDeploySubmission) -> Self {
        match earlier {
            EarlierDeploySubmission::Host(host) => Self {
                horizon_definition_option: host.horizon_definition_option,
                deploy_submission: meta_signal_lojix::DeploySubmission::Host(
                    meta_signal_lojix::HostDeployment {
                        cluster_name: host.cluster_name,
                        node_name: host.node_name,
                        host_composition: host.host_composition,
                        proposal_source: host.proposal_source,
                        secrets_input: host.secrets_input,
                        flake_reference: host.flake_reference,
                        deployment_transport: host.deployment_transport,
                        deployment_input_mode: host.deployment_input_mode,
                        deployment_output_selector: host.deployment_output_selector,
                        activation_backend: host.activation_backend,
                        host_deploy_action: host.host_deploy_action,
                        source_revision_policy: host.source_revision_policy,
                        nix_builder_spec_option: host.nix_builder_spec_option,
                        extra_substituter_vector: host.extra_substituter_vector,
                    },
                ),
            },
            EarlierDeploySubmission::UserEnvironment(user) => Self {
                horizon_definition_option: user.horizon_definition_option,
                deploy_submission: meta_signal_lojix::DeploySubmission::UserEnvironment(
                    meta_signal_lojix::UserEnvironmentDeployment {
                        cluster_name: user.cluster_name,
                        node_name: user.node_name,
                        user_name: user.user_name,
                        proposal_source: user.proposal_source,
                        secrets_input: user.secrets_input,
                        flake_reference: user.flake_reference,
                        deployment_transport: user.deployment_transport,
                        deployment_input_mode: user.deployment_input_mode,
                        deployment_output_selector: user.deployment_output_selector,
                        activation_backend: user.activation_backend,
                        user_environment_action: user.user_environment_action,
                        source_revision_policy: user.source_revision_policy,
                        nix_builder_spec_option: user.nix_builder_spec_option,
                        extra_substituter_vector: user.extra_substituter_vector,
                    },
                ),
            },
        }
    }
}
