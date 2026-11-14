//! Every stored record archives to the bytes lojix 0.10.6 wrote for it, and
//! each version reads the other's bytes back as the same value.
//!
//! The 0.10.6 records are frozen verbatim in `frozen/v0_10_6.rs`. For each
//! family the store registers, one value is built in both, with every option
//! set and every vector filled, and the two archives are compared byte for
//! byte; then the 0.10.6 bytes are read as the current record and the current
//! bytes as the 0.10.6 record, each equal to the value built on its own side.
//! Every enumeration in the stored closure is compared variant by variant, so
//! a tag that moved would show here first.

mod common;
#[path = "frozen/v0_10_6.rs"]
mod frozen;

use lojix::Payload as _;
use lojix::runtime_model as current;
use lojix::upgrade as earlier;
use memory_engine::{Decision, UpgradeFrom as _};

/// Archive both values, compare the bytes, and read each side's bytes as the
/// other side's type.
macro_rules! both_ways {
    ($current:expr, $frozen:expr, $current_type:ty, $frozen_type:ty) => {{
        let current_value: $current_type = $current;
        let frozen_value: $frozen_type = $frozen;
        let current_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&current_value).expect("archive current");
        let frozen_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&frozen_value).expect("archive 0.10.6");
        assert_eq!(
            current_bytes.as_slice(),
            frozen_bytes.as_slice(),
            "{} archives unlike 0.10.6's {}",
            stringify!($current_type),
            stringify!($frozen_type)
        );
        assert_eq!(
            rkyv::from_bytes::<$current_type, rkyv::rancor::Error>(&frozen_bytes)
                .expect("0.10.6 bytes read as the current record"),
            current_value
        );
        assert_eq!(
            rkyv::from_bytes::<$frozen_type, rkyv::rancor::Error>(&current_bytes)
                .expect("current bytes read as the 0.10.6 record"),
            frozen_value
        );
        current_bytes.len()
    }};
}

/// Every variant of a unit enumeration, in both versions.
macro_rules! tags {
    ($current:ident = $frozen:ident [ $($variant:ident)+ ]) => {
        $(
            both_ways!(
                current::$current::$variant,
                frozen::$frozen::$variant,
                current::$current,
                frozen::$frozen
            );
        )+
    };
}

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const CLOSURE: &str = "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system-toplevel";

fn marker(sequence: i64) -> current::DatabaseMarker {
    current::DatabaseMarker {
        commit_sequence: current::CommitSequence(sequence),
        state_digest: current::StateDigest(sequence + 1),
    }
}
fn frozen_marker(sequence: i64) -> frozen::StateMarker {
    frozen::StateMarker {
        commit_sequence: frozen::CommitSequence(sequence as u64),
        state_digest: frozen::StateDigest(sequence as u64 + 1),
    }
}

fn failure() -> current::DeploymentTerminal {
    current::DeploymentTerminal::Failed(current::DeploymentFailure {
        deployment_failure_stage: current::DeploymentFailureStage::Activate,
        deployment_terminal_reason: current::DeploymentTerminalReason::ActivationFailed,
        failure_evidence_option: Some(current::FailureEvidence {
            failed_command_option: Some(current::FailedCommand {
                command_program: current::CommandProgram("ssh".into()),
                command_argument_vector: vec![
                    current::CommandArgument("root@ouranos".into()),
                    current::CommandArgument("switch-to-configuration".into()),
                ],
                exit_code_option: Some(current::ExitCode(255)),
            }),
            failure_detail: current::FailureDetail("activation failed".into()),
            detail_truncated: current::DetailTruncated(true),
        }),
    })
}
fn frozen_failure() -> frozen::DeploymentTerminal {
    frozen::DeploymentTerminal::Failed(frozen::DeploymentFailure {
        deployment_failure_stage: frozen::DeploymentFailureStage::Activate,
        deployment_terminal_reason: frozen::DeploymentTerminalReason::ActivationFailed,
        optional_failure_evidence: Some(frozen::FailureEvidence {
            optional_failed_command: Some(frozen::FailedCommand {
                command_program: frozen::CommandProgram("ssh".into()),
                command_argument_vector: vec![
                    frozen::CommandArgument("root@ouranos".into()),
                    frozen::CommandArgument("switch-to-configuration".into()),
                ],
                optional_exit_code: Some(frozen::ExitCode(255)),
            }),
            failure_detail: frozen::FailureDetail("activation failed".into()),
            detail_truncated: true,
        }),
    })
}

fn source_revision() -> current::SourceRevisionRecord {
    current::SourceRevisionRecord {
        source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
        requested_reference: current::RequestedReference(current::FlakeReference(
            "github:owner/repo?ref=main".into(),
        )),
        resolved_reference: current::ResolvedReference(current::FlakeReference(format!(
            "github:owner/repo?rev={REVISION}"
        ))),
        resolved_revision: current::ResolvedRevision(REVISION.into()),
    }
}
fn frozen_source_revision() -> frozen::SourceRevisionRecord {
    frozen::SourceRevisionRecord {
        source_revision_policy: frozen::SourceRevisionPolicy::ResolveAndRecord,
        requested_ref: frozen::FlakeReference("github:owner/repo?ref=main".into()),
        resolved_ref: frozen::FlakeReference(format!("github:owner/repo?rev={REVISION}")),
        string: REVISION.into(),
    }
}

fn phase_event(phase: current::DeploymentPhase) -> current::DeploymentPhaseEvent {
    current::DeploymentPhaseEvent {
        deployment_identifier: current::DeploymentIdentifier(578),
        generation_identifier: current::GenerationIdentifier(577),
        cluster_name: current::ClusterName("goldragon".into()),
        node_name: current::NodeName("ouranos".into()),
        deployment_phase: phase,
        event_log_position: current::EventLogPosition(2018),
        transition_marker: current::TransitionMarker {
            database_marker: marker(13974),
        },
        immutable_revision_option: Some(current::ImmutableRevision(REVISION.into())),
        deployment_terminal_option: Some(failure()),
    }
}
fn frozen_phase_event(phase: frozen::DeploymentPhase) -> frozen::DeploymentPhaseEvent {
    frozen::DeploymentPhaseEvent {
        deployment_identifier: frozen::DeploymentIdentifier(578),
        generation_identifier: frozen::GenerationIdentifier(577),
        cluster_name: frozen::ClusterName("goldragon".into()),
        node_name: frozen::NodeName("ouranos".into()),
        deployment_phase: phase,
        event_log_position: frozen::EventLogPosition(2018),
        state_marker: frozen_marker(13974),
        optional_immutable_revision: Some(frozen::ImmutableRevision(REVISION.into())),
        optional_deployment_terminal: Some(frozen_failure()),
    }
}

fn transport() -> current::DeploymentTransport {
    current::DeploymentTransport {
        nix_store_uri: current::NixStoreUri("ssh-ng://root@ouranos".into()),
        ssh_destination: current::SshDestination("root@ouranos".into()),
    }
}
fn frozen_transport() -> frozen::DeploymentTransport {
    frozen::DeploymentTransport {
        nix_store_uri: frozen::NixStoreUri("ssh-ng://root@ouranos".into()),
        ssh_destination: frozen::SshDestination("root@ouranos".into()),
    }
}

fn earlier_host_submission() -> earlier::EarlierDeploySubmission {
    earlier::EarlierDeploySubmission::Host(earlier::EarlierHostDeployment {
        cluster_name: current::ClusterName("goldragon".into()),
        node_name: current::NodeName("ouranos".into()),
        host_composition: current::HostComposition::BaseHost,
        proposal_source: current::ProposalSource("/home/li/goldragon".into()),
        secrets_input: current::SecretsInput::SecretsDirectory(current::SecretsDirectory(
            "/home/li/goldragon/secrets".into(),
        )),
        flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS".into()),
        deployment_transport: transport(),
        deployment_input_mode: current::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::hosted_pair()),
        deployment_output_selector: current::DeploymentOutputSelector {
            flake_attribute: current::FlakeAttribute("nixosConfigurations.ouranos".into()),
        },
        activation_backend: current::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: current::HostDeployAction::ScheduleBootOnce,
        source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
        nix_builder_spec_option: Some(current::NixBuilderSpec("ssh-ng://prometheus".into())),
        extra_substituter_vector: vec![current::ExtraSubstituter {
            first_string: "https://cache.example".into(),
            second_string: "cache.example-1:key".into(),
        }],
    })
}
fn frozen_host_submission() -> frozen::DeploySubmission {
    frozen::DeploySubmission::Host(frozen::HostDeployment {
        cluster_name: frozen::ClusterName("goldragon".into()),
        node_name: frozen::NodeName("ouranos".into()),
        host_composition: frozen::HostComposition::BaseHost,
        proposal_source: frozen::ProposalSource("/home/li/goldragon".into()),
        secrets_input: frozen::SecretsInput::SecretsDirectory(frozen::SecretsDirectory(
            "/home/li/goldragon/secrets".into(),
        )),
        flake_reference: frozen::FlakeReference("github:LiGoldragon/CriomOS".into()),
        deployment_transport: frozen_transport(),
        deployment_input_mode: frozen::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::hosted_pair()),
        deployment_output_selector: frozen::DeploymentOutputSelector(frozen::FlakeAttribute(
            "nixosConfigurations.ouranos".into(),
        )),
        activation_backend: frozen::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: frozen::HostDeployAction::ScheduleBootOnce,
        source_revision_policy: frozen::SourceRevisionPolicy::ResolveAndRecord,
        optional_nix_builder_spec: Some(frozen::NixBuilderSpec("ssh-ng://prometheus".into())),
        extra_substituter_vector: vec![frozen::ExtraSubstituter {
            url: "https://cache.example".into(),
            public_key: "cache.example-1:key".into(),
        }],
    })
}

fn earlier_user_submission() -> earlier::EarlierDeploySubmission {
    earlier::EarlierDeploySubmission::UserEnvironment(earlier::EarlierUserEnvironmentDeployment {
        cluster_name: current::ClusterName("goldragon".into()),
        node_name: current::NodeName("ouranos".into()),
        user_name: current::UserName("li".into()),
        proposal_source: current::ProposalSource("/home/li/goldragon".into()),
        secrets_input: current::SecretsInput::NoSecrets,
        flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS-home".into()),
        deployment_transport: transport(),
        deployment_input_mode: current::DeploymentInputMode::Direct,
        horizon_definition_option: None,
        deployment_output_selector: current::DeploymentOutputSelector {
            flake_attribute: current::FlakeAttribute("homeConfigurations.li".into()),
        },
        activation_backend: current::ActivationBackend::HomeManagerNixProfileV1,
        user_environment_action: current::UserEnvironmentAction::ActivateNow,
        source_revision_policy: current::SourceRevisionPolicy::RequireImmutable,
        nix_builder_spec_option: None,
        extra_substituter_vector: Vec::new(),
    })
}
fn frozen_user_submission() -> frozen::DeploySubmission {
    frozen::DeploySubmission::UserEnvironment(frozen::UserEnvironmentDeployment {
        cluster_name: frozen::ClusterName("goldragon".into()),
        node_name: frozen::NodeName("ouranos".into()),
        user_name: frozen::UserName("li".into()),
        proposal_source: frozen::ProposalSource("/home/li/goldragon".into()),
        secrets_input: frozen::SecretsInput::NoSecrets,
        flake_reference: frozen::FlakeReference("github:LiGoldragon/CriomOS-home".into()),
        deployment_transport: frozen_transport(),
        deployment_input_mode: frozen::DeploymentInputMode::Direct,
        horizon_definition_option: None,
        deployment_output_selector: frozen::DeploymentOutputSelector(frozen::FlakeAttribute(
            "homeConfigurations.li".into(),
        )),
        activation_backend: frozen::ActivationBackend::HomeManagerNixProfileV1,
        user_environment_action: frozen::UserEnvironmentAction::ActivateNow,
        source_revision_policy: frozen::SourceRevisionPolicy::RequireImmutable,
        optional_nix_builder_spec: None,
        extra_substituter_vector: Vec::new(),
    })
}

#[test]
fn every_stored_enumeration_keeps_its_tags() {
    tags!(DeploymentInputMode = DeploymentInputMode [ Direct Horizon ]);
    tags!(ActivationBackend = ActivationBackend [ NixosSystemdBootV1 HomeManagerNixProfileV1 ]);
    tags!(SourceRevisionPolicy = SourceRevisionPolicy [ RequireImmutable ResolveAndRecord ]);
    tags!(GenerationArtifact = GenerationArtifact [ CompleteHost BaseHost UserEnvironment ]);
    tags!(ActivationEffect = ActivationEffect [
        LiveActivation BootProfile TestActivation BootOnceProfile ProfileOnly
    ]);
    tags!(GenerationSlot = GenerationSlot [ Current BootPending Rollback Pinned Recent ]);
    tags!(TestMode = TestMode [ Hermetic Live ]);
    tags!(TestRunPhase = TestRunPhase [
        Submitted BringingUp Deploying Asserting TearingDown Completed Failed
    ]);
    tags!(FailureStage = FailureStage [ BringUp Deploy Assert TearDown HermeticCheck ]);
    tags!(DeploymentPhase = DeploymentPhase [
        Submitted Building Built Copying Activating Activated Completed Rejected Failed
    ]);
    tags!(DeploymentPhase = DeploymentLifecycle [
        Submitted Building Built Copying Activating Activated Completed Rejected Failed
    ]);
    tags!(DeploymentFailureStage = DeploymentFailureStage [
        Admission FlakeAuth MaterializeHorizon Eval Build CopyClosure Activate Daemon
    ]);
    tags!(DeploymentTerminalReason = DeploymentTerminalReason [
        ClusterUnknown NodeUnknown ProposalSourceUnreachable FlakeReferenceMalformed
        InvalidDeploymentRouting BuilderUnreachable SubstituterUnreachable DeploymentInFlight
        UnsupportedDeployAction InternalError ActivationFailed EvaluationFailed BuildFailed
        ClosureCopyFailed
    ]);
    tags!(CacheRetentionTransition = CacheRetentionTransition [
        Pinned Unpinned Promoted Demoted Retired Evicted
    ]);
    tags!(HostDeployAction = HostDeployAction [
        Evaluate Realize SetBootProfile ActivateNow TestActivation ScheduleBootOnce
    ]);
    tags!(HostComposition = HostComposition [ CompleteHost BaseHost ]);
    tags!(UserEnvironmentAction = UserEnvironmentAction [ Realize SetProfile ActivateNow ]);
    tags!(OutboxDeliveryState = OutboxDeliveryState [ Pending Dispatched Acknowledged ]);
    tags!(TransitionIntentState = TransitionIntentState [ Pending Bound Appended Acknowledged ]);
    tags!(ContainerState = ContainerState [ Starting Started Stopping Stopped Failed ]);
    tags!(DeployResumeStage = DeployResumeStage [
        ResolveFlakeAuth MaterializeHorizon RecordBuilding NixEval NixBuild CopyClosure
        ActivateGeneration RecordGenerationActivated FinishDeployment
    ]);
    tags!(DeployJobPhase = DeployJobPhase [
        Submitted Building Built Copying Activating Activated Failed
    ]);
}

#[test]
fn every_variant_carrying_a_value_keeps_its_tag_and_payload() {
    both_ways!(
        current::TestOutcome::Pending,
        frozen::TestOutcome::Pending,
        current::TestOutcome,
        frozen::TestOutcome
    );
    both_ways!(
        current::TestOutcome::Passed,
        frozen::TestOutcome::Passed,
        current::TestOutcome,
        frozen::TestOutcome
    );
    both_ways!(
        current::TestOutcome::Failed(current::FailureStage::TearDown),
        frozen::TestOutcome::Failed(frozen::FailureStage::TearDown),
        current::TestOutcome,
        frozen::TestOutcome
    );
    both_ways!(
        current::DeploymentTerminal::Succeeded,
        frozen::DeploymentTerminal::Succeeded,
        current::DeploymentTerminal,
        frozen::DeploymentTerminal
    );
    both_ways!(
        current::DeploymentTerminal::Rejected(current::DeploymentTerminalReason::NodeUnknown),
        frozen::DeploymentTerminal::Rejected(frozen::DeploymentTerminalReason::NodeUnknown),
        current::DeploymentTerminal,
        frozen::DeploymentTerminal
    );
    both_ways!(
        failure(),
        frozen_failure(),
        current::DeploymentTerminal,
        frozen::DeploymentTerminal
    );
    both_ways!(
        current::RequestedDeploymentAction::Host(current::HostDeployAction::TestActivation),
        frozen::RequestedDeploymentAction::Host(frozen::HostDeployAction::TestActivation),
        current::RequestedDeploymentAction,
        frozen::RequestedDeploymentAction
    );
    both_ways!(
        current::RequestedDeploymentAction::UserEnvironment(
            current::UserEnvironmentAction::SetProfile
        ),
        frozen::RequestedDeploymentAction::UserEnvironment(
            frozen::UserEnvironmentAction::SetProfile
        ),
        current::RequestedDeploymentAction,
        frozen::RequestedDeploymentAction
    );
    both_ways!(
        current::DeploymentEnvironment::HostEnvironment,
        frozen::DeploymentEnvironment::HostEnvironment,
        current::DeploymentEnvironment,
        frozen::DeploymentEnvironment
    );
    both_ways!(
        current::DeploymentEnvironment::UserEnvironment(current::UserName("li".into())),
        frozen::DeploymentEnvironment::UserEnvironment(frozen::UserName("li".into())),
        current::DeploymentEnvironment,
        frozen::DeploymentEnvironment
    );
}

#[test]
fn a_live_generation_and_its_gc_root_archive_as_0_10_6_wrote_them() {
    both_ways!(
        current::LiveGeneration {
            deployment_identifier: current::DeploymentIdentifier(578),
            generation_identifier: current::GenerationIdentifier(577),
            cluster_name: current::ClusterName("goldragon".into()),
            node_name: current::NodeName("ouranos".into()),
            deployment_environment: current::DeploymentEnvironment::UserEnvironment(
                current::UserName("li".into())
            ),
            generation_artifact: current::GenerationArtifact::UserEnvironment,
            activation_effect: current::ActivationEffect::ProfileOnly,
            generation_slot: current::GenerationSlot::Current,
            closure_path: current::ClosurePath(CLOSURE.into()),
            source_revision_record: source_revision(),
        },
        frozen::LiveGeneration {
            deployment_identifier: frozen::DeploymentIdentifier(578),
            generation_identifier: frozen::GenerationIdentifier(577),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node_name: frozen::NodeName("ouranos".into()),
            deployment_environment: frozen::DeploymentEnvironment::UserEnvironment(
                frozen::UserName("li".into())
            ),
            generation_artifact: frozen::GenerationArtifact::UserEnvironment,
            activation_effect: frozen::ActivationEffect::ProfileOnly,
            generation_slot: frozen::GenerationSlot::Current,
            closure_path: frozen::ClosurePath(CLOSURE.into()),
            source_revision_record: frozen_source_revision(),
        },
        current::LiveGeneration,
        frozen::LiveGeneration
    );
    both_ways!(
        current::GcRoot {
            generation_identifier: current::GenerationIdentifier(577),
            cluster_name: current::ClusterName("goldragon".into()),
            node_name: current::NodeName("ouranos".into()),
            generation_slot: current::GenerationSlot::Pinned,
            closure_path: current::ClosurePath(CLOSURE.into()),
            pin_label_option: Some(current::PinLabel("known-good".into())),
        },
        frozen::GcRoot {
            generation_identifier: frozen::GenerationIdentifier(577),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node_name: frozen::NodeName("ouranos".into()),
            generation_slot: frozen::GenerationSlot::Pinned,
            closure_path: frozen::ClosurePath(CLOSURE.into()),
            optional_pin_label: Some(frozen::PinLabel("known-good".into())),
        },
        current::GcRoot,
        frozen::GcRoot
    );
}

#[test]
fn each_event_log_entry_archives_as_0_10_6_wrote_it() {
    both_ways!(
        current::EventLogEntry {
            event_log_position: current::EventLogPosition(2018),
            logged_event: current::LoggedEvent::Deployment(phase_event(
                current::DeploymentPhase::Failed
            )),
        },
        frozen::EventLogEntry {
            event_log_position: frozen::EventLogPosition(2018),
            logged_event: frozen::LoggedEvent::Deployment(frozen_phase_event(
                frozen::DeploymentPhase::Failed
            )),
        },
        current::EventLogEntry,
        frozen::EventLogEntry
    );
    both_ways!(
        current::EventLogEntry {
            event_log_position: current::EventLogPosition(2019),
            logged_event: current::LoggedEvent::CacheRetention(
                current::CacheRetentionTransitionEvent {
                    generation_identifier: current::GenerationIdentifier(577),
                    cluster_name: current::ClusterName("goldragon".into()),
                    node_name: current::NodeName("ouranos".into()),
                    cache_retention_transition: current::CacheRetentionTransition::Demoted,
                    generation_slot: current::GenerationSlot::Recent,
                    generation_slot_option: Some(current::GenerationSlot::Rollback),
                    pin_label_option: Some(current::PinLabel("known-good".into())),
                    event_log_position: current::EventLogPosition(2019),
                }
            ),
        },
        frozen::EventLogEntry {
            event_log_position: frozen::EventLogPosition(2019),
            logged_event: frozen::LoggedEvent::CacheRetention(
                frozen::CacheRetentionTransitionEvent {
                    generation_identifier: frozen::GenerationIdentifier(577),
                    cluster_name: frozen::ClusterName("goldragon".into()),
                    node_name: frozen::NodeName("ouranos".into()),
                    cache_retention_transition: frozen::CacheRetentionTransition::Demoted,
                    generation_slot: frozen::GenerationSlot::Recent,
                    optional_generation_slot: Some(frozen::GenerationSlot::Rollback),
                    optional_pin_label: Some(frozen::PinLabel("known-good".into())),
                    event_log_position: frozen::EventLogPosition(2019),
                }
            ),
        },
        current::EventLogEntry,
        frozen::EventLogEntry
    );
    both_ways!(
        current::EventLogEntry {
            event_log_position: current::EventLogPosition(2020),
            logged_event: current::LoggedEvent::Container(container()),
        },
        frozen::EventLogEntry {
            event_log_position: frozen::EventLogPosition(2020),
            logged_event: frozen::LoggedEvent::Container(frozen_container()),
        },
        current::EventLogEntry,
        frozen::EventLogEntry
    );
}

fn container() -> current::ContainerLifecycleRecord {
    current::ContainerLifecycleRecord {
        cluster_name: current::ClusterName("goldragon".into()),
        node_name: current::NodeName("ouranos".into()),
        container_name: current::ContainerName::new("vm-beacon".into()),
        container_state: current::ContainerState::Stopping,
        event_log_position: current::EventLogPosition(2020),
    }
}
fn frozen_container() -> frozen::ContainerLifecycleRecord {
    frozen::ContainerLifecycleRecord {
        cluster_name: frozen::ClusterName("goldragon".into()),
        node_name: frozen::NodeName("ouranos".into()),
        container_name: frozen::ContainerName("vm-beacon".into()),
        container_state: frozen::ContainerState::Stopping,
        event_log_position: frozen::EventLogPosition(2020),
    }
}

#[test]
fn a_container_record_archives_as_0_10_6_wrote_it() {
    both_ways!(
        container(),
        frozen_container(),
        current::ContainerLifecycleRecord,
        frozen::ContainerLifecycleRecord
    );
}

#[test]
fn the_earlier_deploy_job_the_upgrade_reads_is_0_10_6_s_layout() {
    let full = both_ways!(
        earlier::EarlierDeployJob {
            deployment_identifier: current::DeploymentIdentifier(579),
            generation_identifier: current::GenerationIdentifier(578),
            cluster_name: current::ClusterName("goldragon".into()),
            node_name: current::NodeName("ouranos".into()),
            deploy_job_phase: current::DeployJobPhase::Activating,
            closure_path_option: Some(current::ClosurePath(CLOSURE.into())),
            source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
            flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS".into()),
            flake_reference_option: Some(current::FlakeReference(format!(
                "github:LiGoldragon/CriomOS?rev={REVISION}"
            ))),
            resolved_revision: Some(REVISION.into()),
            deployment_transport: transport(),
            deployment_input_mode: current::DeploymentInputMode::Horizon,
            deployment_output_selector: current::DeploymentOutputSelector {
                flake_attribute: current::FlakeAttribute("nixosConfigurations.ouranos".into()),
            },
            activation_backend: current::ActivationBackend::NixosSystemdBootV1,
            nix_builder_spec_option: Some(current::NixBuilderSpec("ssh-ng://prometheus".into())),
            boot_once_unit: Some("lojix-boot-once-579.service".into()),
            generation_slot_option: Some(current::GenerationSlot::BootPending),
            persisted_flake_input_override_vector: vec![earlier::EarlierFlakeInputOverride {
                string: "horizon".into(),
                persisted_flake_input_reference: earlier::EarlierFlakeInputReference {
                    url: "path:/var/lib/lojix/generated-inputs/horizon".into(),
                    nix_archive_hash: "sha256-AAAA".into(),
                },
            }],
            deploy_resume_stage: current::DeployResumeStage::ActivateGeneration,
            phase_receipt_option: Some(current::PhaseReceipt {
                event_log_position: current::EventLogPosition(2017),
                database_marker: marker(13970),
            }),
            deploy_submission_option: Some(earlier_host_submission()),
        },
        frozen::DeployJob {
            deployment_identifier: frozen::DeploymentIdentifier(579),
            generation_identifier: frozen::GenerationIdentifier(578),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node_name: frozen::NodeName("ouranos".into()),
            deploy_job_phase: frozen::DeployJobPhase::Activating,
            optional_closure_path: Some(frozen::ClosurePath(CLOSURE.into())),
            source_revision_policy: frozen::SourceRevisionPolicy::ResolveAndRecord,
            flake_reference: frozen::FlakeReference("github:LiGoldragon/CriomOS".into()),
            optional_flake_reference: Some(frozen::FlakeReference(format!(
                "github:LiGoldragon/CriomOS?rev={REVISION}"
            ))),
            resolved_revision: Some(REVISION.into()),
            deployment_transport: frozen_transport(),
            deployment_input_mode: frozen::DeploymentInputMode::Horizon,
            deployment_output_selector: frozen::DeploymentOutputSelector(frozen::FlakeAttribute(
                "nixosConfigurations.ouranos".into(),
            )),
            activation_backend: frozen::ActivationBackend::NixosSystemdBootV1,
            optional_nix_builder_spec: Some(frozen::NixBuilderSpec("ssh-ng://prometheus".into())),
            boot_once_unit: Some("lojix-boot-once-579.service".into()),
            optional_generation_slot: Some(frozen::GenerationSlot::BootPending),
            persisted_flake_input_override_vector: vec![frozen::PersistedFlakeInputOverride {
                string: "horizon".into(),
                persisted_flake_input_reference: frozen::PersistedFlakeInputReference {
                    url: "path:/var/lib/lojix/generated-inputs/horizon".into(),
                    nix_archive_hash: "sha256-AAAA".into(),
                },
            }],
            deploy_resume_stage: frozen::DeployResumeStage::ActivateGeneration,
            optional_phase_receipt: Some(frozen::PhaseReceipt {
                event_log_position: frozen::EventLogPosition(2017),
                state_marker: frozen_marker(13970),
            }),
            optional_deploy_submission: Some(frozen_host_submission()),
        },
        earlier::EarlierDeployJob,
        frozen::DeployJob
    );
    assert!(
        full > 1000,
        "the Horizon definition is carried: {full} bytes"
    );
    both_ways!(
        earlier::EarlierDeployJob {
            deployment_identifier: current::DeploymentIdentifier(580),
            generation_identifier: current::GenerationIdentifier(579),
            cluster_name: current::ClusterName("goldragon".into()),
            node_name: current::NodeName("ouranos".into()),
            deploy_job_phase: current::DeployJobPhase::Submitted,
            closure_path_option: None,
            source_revision_policy: current::SourceRevisionPolicy::RequireImmutable,
            flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS-home".into()),
            flake_reference_option: None,
            resolved_revision: None,
            deployment_transport: transport(),
            deployment_input_mode: current::DeploymentInputMode::Direct,
            deployment_output_selector: current::DeploymentOutputSelector {
                flake_attribute: current::FlakeAttribute("homeConfigurations.li".into()),
            },
            activation_backend: current::ActivationBackend::HomeManagerNixProfileV1,
            nix_builder_spec_option: None,
            boot_once_unit: None,
            generation_slot_option: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: current::DeployResumeStage::ResolveFlakeAuth,
            phase_receipt_option: None,
            deploy_submission_option: Some(earlier_user_submission()),
        },
        frozen::DeployJob {
            deployment_identifier: frozen::DeploymentIdentifier(580),
            generation_identifier: frozen::GenerationIdentifier(579),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node_name: frozen::NodeName("ouranos".into()),
            deploy_job_phase: frozen::DeployJobPhase::Submitted,
            optional_closure_path: None,
            source_revision_policy: frozen::SourceRevisionPolicy::RequireImmutable,
            flake_reference: frozen::FlakeReference("github:LiGoldragon/CriomOS-home".into()),
            optional_flake_reference: None,
            resolved_revision: None,
            deployment_transport: frozen_transport(),
            deployment_input_mode: frozen::DeploymentInputMode::Direct,
            deployment_output_selector: frozen::DeploymentOutputSelector(frozen::FlakeAttribute(
                "homeConfigurations.li".into(),
            )),
            activation_backend: frozen::ActivationBackend::HomeManagerNixProfileV1,
            optional_nix_builder_spec: None,
            boot_once_unit: None,
            optional_generation_slot: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: frozen::DeployResumeStage::ResolveFlakeAuth,
            optional_phase_receipt: None,
            optional_deploy_submission: Some(frozen_user_submission()),
        },
        earlier::EarlierDeployJob,
        frozen::DeployJob
    );
}

#[test]
fn a_test_run_archives_as_0_10_6_wrote_it() {
    both_ways!(
        current::StoredTestRun {
            test_run_record: current::TestRunRecord {
                test_run_identifier: current::TestRunIdentifier(12),
                cluster_name: current::ClusterName("goldragon".into()),
                first_node_name: current::NodeName("beacon".into()),
                second_node_name: current::NodeName("atlas".into()),
                test_mode: current::TestMode::Live,
                test_run_phase: current::TestRunPhase::Failed,
                test_outcome: current::TestOutcome::Failed(current::FailureStage::Assert),
                closure_path_option: Some(current::ClosurePath(CLOSURE.into())),
            },
        },
        frozen::StoredTestRun {
            test_run_identifier: frozen::TestRunIdentifier(12),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node: frozen::NodeName("beacon".into()),
            host: frozen::NodeName("atlas".into()),
            test_mode: frozen::TestMode::Live,
            test_run_phase: frozen::TestRunPhase::Failed,
            test_outcome: frozen::TestOutcome::Failed(frozen::FailureStage::Assert),
            optional_closure_path: Some(frozen::ClosurePath(CLOSURE.into())),
        },
        current::StoredTestRun,
        frozen::StoredTestRun
    );
}

#[test]
fn a_deployment_record_is_held_whole_and_archives_as_0_10_6_wrote_it() {
    both_ways!(
        current::StoredDeploymentRecord {
            deployment_record: current::DeploymentRecord {
                deployment_identifier: current::DeploymentIdentifier(578),
                generation_identifier: current::GenerationIdentifier(577),
                deployment_request_identity: current::DeploymentRequestIdentity {
                    deployment_environment: current::DeploymentEnvironment::HostEnvironment,
                    cluster_name: current::ClusterName("goldragon".into()),
                    node_name: current::NodeName("ouranos".into()),
                    generation_artifact: current::GenerationArtifact::BaseHost,
                    requested_deployment_action: current::RequestedDeploymentAction::Host(
                        current::HostDeployAction::SetBootProfile,
                    ),
                    activation_effect: current::ActivationEffect::BootProfile,
                    source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
                    immutable_revision_option: Some(current::ImmutableRevision(REVISION.into())),
                },
                admission_marker_option: Some(current::AdmissionMarker {
                    database_marker: marker(13960),
                }),
                deployment_phase: current::DeploymentPhase::Failed,
                terminal_marker_option: Some(current::TerminalMarker {
                    database_marker: marker(13974),
                }),
                deployment_terminal_option: Some(failure()),
            },
        },
        frozen::DeploymentRecord {
            deployment_identifier: frozen::DeploymentIdentifier(578),
            generation_identifier: frozen::GenerationIdentifier(577),
            deployment_request_identity: frozen::DeploymentRequestIdentity {
                deployment_environment: frozen::DeploymentEnvironment::HostEnvironment,
                cluster_name: frozen::ClusterName("goldragon".into()),
                node_name: frozen::NodeName("ouranos".into()),
                generation_artifact: frozen::GenerationArtifact::BaseHost,
                requested_deployment_action: frozen::RequestedDeploymentAction::Host(
                    frozen::HostDeployAction::SetBootProfile,
                ),
                activation_effect: frozen::ActivationEffect::BootProfile,
                source_revision_policy: frozen::SourceRevisionPolicy::ResolveAndRecord,
                optional_immutable_revision: Some(frozen::ImmutableRevision(REVISION.into())),
            },
            optional_admission_marker: Some(frozen::AdmissionMarker(frozen_marker(13960))),
            deployment_lifecycle: frozen::DeploymentLifecycle::Failed,
            optional_terminal_marker: Some(frozen::TerminalMarker(frozen_marker(13974))),
            optional_deployment_terminal: Some(frozen_failure()),
        },
        current::StoredDeploymentRecord,
        frozen::DeploymentRecord
    );
}

#[test]
fn the_identifier_allocation_archives_as_0_10_6_wrote_it() {
    both_ways!(
        current::IdentifierAllocation {
            next_deployment_identifier: current::NextDeploymentIdentifier(579),
            next_generation_identifier: current::NextGenerationIdentifier(578),
            next_event_log_position: current::NextEventLogPosition(2019),
        },
        frozen::IdentifierAllocation {
            next_deployment_identifier: 579,
            next_generation_identifier: 578,
            next_event_log_position: 2019,
        },
        current::IdentifierAllocation,
        frozen::IdentifierAllocation
    );
}

#[test]
fn an_outbox_row_and_a_transition_intent_archive_as_0_10_6_wrote_them() {
    both_ways!(
        current::DeploymentOutboxRecord {
            deployment_identifier: current::DeploymentIdentifier(578),
            transition_marker: current::TransitionMarker {
                database_marker: marker(13974),
            },
            deployment_phase_event: phase_event(current::DeploymentPhase::Completed),
            outbox_delivery_state: current::OutboxDeliveryState::Dispatched,
            outbox_retry_count: current::OutboxRetryCount::new(3),
        },
        frozen::DeploymentOutboxRecord {
            deployment_identifier: frozen::DeploymentIdentifier(578),
            transition_marker: frozen::TransitionMarker(frozen_marker(13974)),
            deployment_phase_event: frozen_phase_event(frozen::DeploymentPhase::Completed),
            outbox_delivery_state: frozen::OutboxDeliveryState::Dispatched,
            outbox_retry_count: frozen::OutboxRetryCount(3),
        },
        current::DeploymentOutboxRecord,
        frozen::DeploymentOutboxRecord
    );
    both_ways!(
        current::PendingTransitionIntent {
            deployment_identifier: current::DeploymentIdentifier(578),
            generation_identifier: current::GenerationIdentifier(577),
            cluster_name: current::ClusterName("goldragon".into()),
            node_name: current::NodeName("ouranos".into()),
            deployment_phase: current::DeploymentPhase::Activated,
            event_log_position: current::EventLogPosition(2018),
            immutable_revision_option: Some(current::ImmutableRevision(REVISION.into())),
            deployment_terminal_option: Some(current::DeploymentTerminal::Succeeded),
            transition_ordinal: current::TransitionOrdinal::new(6),
            transition_marker_option: Some(current::TransitionMarker {
                database_marker: marker(13974),
            }),
            transition_intent_state: current::TransitionIntentState::Appended,
        },
        frozen::PendingTransitionIntent {
            deployment_identifier: frozen::DeploymentIdentifier(578),
            generation_identifier: frozen::GenerationIdentifier(577),
            cluster_name: frozen::ClusterName("goldragon".into()),
            node_name: frozen::NodeName("ouranos".into()),
            deployment_phase: frozen::DeploymentPhase::Activated,
            event_log_position: frozen::EventLogPosition(2018),
            optional_immutable_revision: Some(frozen::ImmutableRevision(REVISION.into())),
            optional_deployment_terminal: Some(frozen::DeploymentTerminal::Succeeded),
            transition_ordinal: frozen::TransitionOrdinal(6),
            optional_transition_marker: Some(frozen::TransitionMarker(frozen_marker(13974))),
            transition_intent_state: frozen::TransitionIntentState::Appended,
        },
        current::PendingTransitionIntent,
        frozen::PendingTransitionIntent
    );
}

/// The 0.10.6 host deployment as the contract now carries it.
fn contract_host_submission() -> meta_signal_lojix::ActualizedDeploySubmission {
    meta_signal_lojix::ActualizedDeploySubmission {
        deploy_submission: meta_signal_lojix::DeploySubmission::Host(
            meta_signal_lojix::HostDeployment {
                cluster_name: current::ClusterName("goldragon".into()),
                node_name: current::NodeName("ouranos".into()),
                host_composition: current::HostComposition::BaseHost,
                proposal_source: current::ProposalSource("/home/li/goldragon".into()),
                secrets_input: current::SecretsInput::SecretsDirectory(current::SecretsDirectory(
                    "/home/li/goldragon/secrets".into(),
                )),
                flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS".into()),
                deployment_transport: transport(),
                deployment_input_mode: current::DeploymentInputMode::Horizon,
                deployment_output_selector: current::DeploymentOutputSelector {
                    flake_attribute: current::FlakeAttribute("nixosConfigurations.ouranos".into()),
                },
                activation_backend: current::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: current::HostDeployAction::ScheduleBootOnce,
                source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
                nix_builder_spec_option: Some(current::NixBuilderSpec(
                    "ssh-ng://prometheus".into(),
                )),
                extra_substituter_vector: vec![current::ExtraSubstituter {
                    first_string: "https://cache.example".into(),
                    second_string: "cache.example-1:key".into(),
                }],
            },
        ),
        horizon_definition_option: Some(common::hosted_pair()),
    }
}

#[test]
fn the_upgrade_moves_a_0_10_6_deploy_job_s_horizon_definition_beside_its_deployment() {
    let frozen_job = frozen::DeployJob {
        deployment_identifier: frozen::DeploymentIdentifier(579),
        generation_identifier: frozen::GenerationIdentifier(578),
        cluster_name: frozen::ClusterName("goldragon".into()),
        node_name: frozen::NodeName("ouranos".into()),
        deploy_job_phase: frozen::DeployJobPhase::Activating,
        optional_closure_path: Some(frozen::ClosurePath(CLOSURE.into())),
        source_revision_policy: frozen::SourceRevisionPolicy::ResolveAndRecord,
        flake_reference: frozen::FlakeReference("github:LiGoldragon/CriomOS".into()),
        optional_flake_reference: None,
        resolved_revision: Some(REVISION.into()),
        deployment_transport: frozen_transport(),
        deployment_input_mode: frozen::DeploymentInputMode::Horizon,
        deployment_output_selector: frozen::DeploymentOutputSelector(frozen::FlakeAttribute(
            "nixosConfigurations.ouranos".into(),
        )),
        activation_backend: frozen::ActivationBackend::NixosSystemdBootV1,
        optional_nix_builder_spec: None,
        boot_once_unit: Some("lojix-boot-once-579.service".into()),
        optional_generation_slot: None,
        persisted_flake_input_override_vector: vec![frozen::PersistedFlakeInputOverride {
            string: "horizon".into(),
            persisted_flake_input_reference: frozen::PersistedFlakeInputReference {
                url: "path:/var/lib/lojix/generated-inputs/horizon".into(),
                nix_archive_hash: "sha256-AAAA".into(),
            },
        }],
        deploy_resume_stage: frozen::DeployResumeStage::ActivateGeneration,
        optional_phase_receipt: None,
        optional_deploy_submission: Some(frozen_host_submission()),
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frozen_job).expect("archive 0.10.6");
    let earlier_job = rkyv::from_bytes::<earlier::EarlierDeployJob, rkyv::rancor::Error>(&bytes)
        .expect("the upgrade reads the 0.10.6 row");
    let upgraded = match current::DeployJob::upgrade_from(earlier_job) {
        Decision::Ready(upgraded) => upgraded,
        Decision::Refused(never) => match never {},
    };
    let expected = current::DeployJob {
        deployment_identifier: current::DeploymentIdentifier(579),
        generation_identifier: current::GenerationIdentifier(578),
        cluster_name: current::ClusterName("goldragon".into()),
        node_name: current::NodeName("ouranos".into()),
        deploy_job_phase: current::DeployJobPhase::Activating,
        closure_path_option: Some(current::ClosurePath(CLOSURE.into())),
        source_revision_policy: current::SourceRevisionPolicy::ResolveAndRecord,
        flake_reference: current::FlakeReference("github:LiGoldragon/CriomOS".into()),
        flake_reference_option: None,
        resolved_revision_option: Some(current::ResolvedRevision(REVISION.into())),
        deployment_transport: transport(),
        deployment_input_mode: current::DeploymentInputMode::Horizon,
        deployment_output_selector: current::DeploymentOutputSelector {
            flake_attribute: current::FlakeAttribute("nixosConfigurations.ouranos".into()),
        },
        activation_backend: current::ActivationBackend::NixosSystemdBootV1,
        nix_builder_spec_option: None,
        boot_once_unit_option: Some(current::BootOnceUnit("lojix-boot-once-579.service".into())),
        generation_slot_option: None,
        persisted_flake_input_override_vector: vec![current::PersistedFlakeInputOverride {
            flake_input_name: current::FlakeInputName("horizon".into()),
            persisted_flake_input_reference: current::PersistedFlakeInputReference {
                flake_input_url: current::FlakeInputUrl(
                    "path:/var/lib/lojix/generated-inputs/horizon".into(),
                ),
                nix_archive_hash: current::NixArchiveHash("sha256-AAAA".into()),
            },
        }],
        deploy_resume_stage: current::DeployResumeStage::ActivateGeneration,
        phase_receipt_option: None,
        actualized_deploy_submission_option: Some(contract_host_submission()),
    };
    assert_eq!(upgraded, expected);
    let again = rkyv::to_bytes::<rkyv::rancor::Error>(&upgraded).expect("archive upgraded");
    assert_eq!(
        rkyv::from_bytes::<current::DeployJob, rkyv::rancor::Error>(&again)
            .expect("the upgraded row reads back"),
        expected
    );
    assert!(
        rkyv::from_bytes::<current::DeployJob, rkyv::rancor::Error>(&bytes).is_err(),
        "a 0.10.6 row is not read as the current deploy job without its upgrade"
    );
}

#[test]
fn the_configuration_record_archives_as_0_10_6_wrote_it() {
    use nexus::Configurable as _;
    for closed in [false, true] {
        let mut state = nexus::ConfigurationState::from_default(
            <lojix::NexusConfiguration as lojix::LojixNexusConfigurable>::built_in(),
        );
        if closed {
            state.meta_configure(
                <lojix::NexusConfiguration as lojix::LojixNexusConfigurable>::built_in(),
            );
        }
        both_ways!(
            current::NexusConfigurationRecord::from(state.clone()),
            frozen::NexusConfigurationRecord { state },
            current::NexusConfigurationRecord,
            frozen::NexusConfigurationRecord
        );
    }
}
