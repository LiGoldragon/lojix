//! Direct typed boundary between generated contracts and the runtime model.
//!
//! The daemon maps named fields without a Datom or textual intermediate.

use crate::Payload;
use crate::inspected_text::{NixStorePath, StoreItemShape};
use crate::runtime_model as memory;
use meta_signal_lojix as owner;
use signal_lojix as ordinary;

#[derive(Debug, thiserror::Error)]
#[error("generated contract value cannot be represented by the lojix runtime model")]
pub struct WireShapeError;

pub trait Lowerable<T> {
    fn lower(self) -> Result<T, WireShapeError>;
}
pub trait Raisable<T> {
    fn raise(self) -> Result<T, WireShapeError>;
}
impl Lowerable<horizon_lib::HorizonDefinition> for horizon_lib::HorizonDefinition {
    fn lower(self) -> Result<horizon_lib::HorizonDefinition, WireShapeError> {
        Ok(self)
    }
}
impl Raisable<horizon_lib::HorizonDefinition> for horizon_lib::HorizonDefinition {
    fn raise(self) -> Result<horizon_lib::HorizonDefinition, WireShapeError> {
        Ok(self)
    }
}
impl Lowerable<ordinary::LojixNexusConfiguration> for ordinary::LojixNexusConfiguration {
    fn lower(self) -> Result<ordinary::LojixNexusConfiguration, WireShapeError> {
        Ok(self)
    }
}

impl From<WireShapeError> for crate::Error {
    fn from(error: WireShapeError) -> Self {
        Self::Wire(error.to_string())
    }
}
impl Lowerable<String> for String {
    fn lower(self) -> Result<String, WireShapeError> {
        Ok(self)
    }
}
impl Lowerable<bool> for bool {
    fn lower(self) -> Result<bool, WireShapeError> {
        Ok(self)
    }
}
impl Raisable<bool> for bool {
    fn raise(self) -> Result<bool, WireShapeError> {
        Ok(self)
    }
}
impl Raisable<String> for String {
    fn raise(self) -> Result<String, WireShapeError> {
        Ok(self)
    }
}

impl<P, R> Lowerable<Option<R>> for Option<P>
where
    P: Lowerable<R>,
{
    fn lower(self) -> Result<Option<R>, WireShapeError> {
        self.map(Lowerable::lower).transpose()
    }
}
impl<P, R> Raisable<Option<P>> for Option<R>
where
    R: Raisable<P>,
{
    fn raise(self) -> Result<Option<P>, WireShapeError> {
        self.map(Raisable::raise).transpose()
    }
}
impl<P, R> Lowerable<Vec<R>> for Vec<P>
where
    P: Lowerable<R>,
{
    fn lower(self) -> Result<Vec<R>, WireShapeError> {
        self.into_iter().map(Lowerable::lower).collect()
    }
}
impl<P, R> Raisable<Vec<P>> for Vec<R>
where
    R: Raisable<P>,
{
    fn raise(self) -> Result<Vec<P>, WireShapeError> {
        self.into_iter().map(Raisable::raise).collect()
    }
}

macro_rules! text_value {
    ($runtime:ident) => {
        impl Lowerable<memory::$runtime> for ordinary::$runtime {
            fn lower(self) -> Result<memory::$runtime, WireShapeError> {
                Ok(memory::$runtime::new(self.0))
            }
        }
        impl Raisable<ordinary::$runtime> for memory::$runtime {
            fn raise(self) -> Result<ordinary::$runtime, WireShapeError> {
                Ok(ordinary::$runtime(self.into_payload()))
            }
        }
    };
}
macro_rules! integer_value {
    ($runtime:ident) => {
        impl Lowerable<memory::$runtime> for ordinary::$runtime {
            fn lower(self) -> Result<memory::$runtime, WireShapeError> {
                Ok(memory::$runtime::new(
                    u64::try_from(self.0).map_err(|_| WireShapeError)?,
                ))
            }
        }
        impl Raisable<ordinary::$runtime> for memory::$runtime {
            fn raise(self) -> Result<ordinary::$runtime, WireShapeError> {
                i64::try_from(self.into_payload())
                    .map(ordinary::$runtime)
                    .map_err(|_| WireShapeError)
            }
        }
    };
}
impl Lowerable<bool> for ordinary::DetailTruncated {
    fn lower(self) -> Result<bool, WireShapeError> {
        Ok(self.0)
    }
}
impl Raisable<ordinary::DetailTruncated> for bool {
    fn raise(self) -> Result<ordinary::DetailTruncated, WireShapeError> {
        Ok(ordinary::DetailTruncated(self))
    }
}
macro_rules! shared_unit_enum { ($name:ident { $($variant:ident),+ $(,)? }) => {
    impl Lowerable<memory::$name> for ordinary::$name {
        fn lower(self) -> Result<memory::$name, WireShapeError> { Ok(match self { $(ordinary::$name::$variant => memory::$name::$variant),+ }) }
    }
    impl Raisable<ordinary::$name> for memory::$name {
        fn raise(self) -> Result<ordinary::$name, WireShapeError> { Ok(match self { $(memory::$name::$variant => ordinary::$name::$variant),+ }) }
    }
}; }
macro_rules! owner_unit_enum { ($name:ident { $($variant:ident),+ $(,)? }) => {
    impl Lowerable<memory::$name> for owner::$name {
        fn lower(self) -> Result<memory::$name, WireShapeError> { Ok(match self { $(owner::$name::$variant => memory::$name::$variant),+ }) }
    }
    impl Raisable<owner::$name> for memory::$name {
        fn raise(self) -> Result<owner::$name, WireShapeError> { Ok(match self { $(memory::$name::$variant => owner::$name::$variant),+ }) }
    }
}; }
macro_rules! shared_struct { ($public:ident => $runtime:ident { $($pf:ident => $rf:ident),+ $(,)? }) => {
    impl Lowerable<memory::$runtime> for ordinary::$public {
        fn lower(self) -> Result<memory::$runtime, WireShapeError> { Ok(memory::$runtime { $($rf: self.$pf.lower()?),+ }) }
    }
    impl Raisable<ordinary::$public> for memory::$runtime {
        fn raise(self) -> Result<ordinary::$public, WireShapeError> { Ok(ordinary::$public { $($pf: self.$rf.raise()?),+ }) }
    }
}; }
macro_rules! owner_struct { ($public:ident => $runtime:ident { $($pf:ident => $rf:ident),+ $(,)? }) => {
    impl Lowerable<memory::$runtime> for owner::$public {
        fn lower(self) -> Result<memory::$runtime, WireShapeError> { Ok(memory::$runtime { $($rf: self.$pf.lower()?),+ }) }
    }
    impl Raisable<owner::$public> for memory::$runtime {
        fn raise(self) -> Result<owner::$public, WireShapeError> { Ok(owner::$public { $($pf: self.$rf.raise()?),+ }) }
    }
}; }
macro_rules! owner_struct_with_none {
    ($public:ident => $runtime:ident { $($pf:ident => $rf:ident),+ $(,)? }, $runtime_only:ident) => {
        impl Lowerable<memory::$runtime> for owner::$public {
            fn lower(self) -> Result<memory::$runtime, WireShapeError> {
                Ok(memory::$runtime {
                    $($rf: self.$pf.lower()?),+,
                    $runtime_only: None,
                })
            }
        }
        impl Raisable<owner::$public> for memory::$runtime {
            fn raise(self) -> Result<owner::$public, WireShapeError> {
                Ok(owner::$public { $($pf: self.$rf.raise()?),+ })
            }
        }
    };
}
macro_rules! shared_raise_struct { ($public:ident => $runtime:ident { $($pf:ident => $rf:ident),+ $(,)? }) => {
    impl Raisable<ordinary::$public> for memory::$runtime {
        fn raise(self) -> Result<ordinary::$public, WireShapeError> { Ok(ordinary::$public { $($pf: self.$rf.raise()?),+ }) }
    }
}; }
macro_rules! owner_raise_struct { ($public:ident => $runtime:ident { $($pf:ident => $rf:ident),+ $(,)? }) => {
    impl Raisable<owner::$public> for memory::$runtime {
        fn raise(self) -> Result<owner::$public, WireShapeError> { Ok(owner::$public { $($pf: self.$rf.raise()?),+ }) }
    }
}; }

text_value!(ClusterName);
text_value!(NodeName);
text_value!(UserName);
text_value!(PinLabel);
text_value!(CommandProgram);
text_value!(CommandArgument);
text_value!(FailureDetail);
text_value!(ClosurePath);
text_value!(FlakeReference);
text_value!(NixStoreUri);
text_value!(SshDestination);
text_value!(FlakeAttribute);
text_value!(NixBuilderSpec);
text_value!(NixSystem);
text_value!(ProposalSource);
text_value!(SecretsDirectory);
text_value!(ImmutableRevision);
integer_value!(DeploymentIdentifier);
integer_value!(GenerationIdentifier);
integer_value!(TestRunIdentifier);
integer_value!(SubscriptionToken);
integer_value!(EventLogPosition);
integer_value!(ExitCode);
integer_value!(CommitSequence);
integer_value!(StateDigest);

shared_unit_enum!(UserEnvironmentAction {
    ActivateNow,
    Realize,
    SetProfile
});
shared_unit_enum!(CacheRetentionTransition {
    Demoted,
    Retired,
    Pinned,
    Promoted,
    Unpinned,
    Evicted
});
shared_unit_enum!(FailureStage {
    HermeticCheck,
    BringUp,
    Assert,
    Deploy,
    TearDown
});
shared_unit_enum!(HostDeployAction {
    TestActivation,
    ScheduleBootOnce,
    Realize,
    SetBootProfile,
    Evaluate,
    ActivateNow
});
shared_unit_enum!(UnwatchRejectionReason {
    SubscriptionTokenUnknown,
    SubscriptionAlreadyClosed
});
shared_unit_enum!(GenerationSlot {
    Pinned,
    Recent,
    Rollback,
    BootPending,
    Current
});
shared_unit_enum!(HostComposition {
    CompleteHost,
    BaseHost
});
shared_unit_enum!(DeploymentPhase {
    Built,
    Completed,
    Failed,
    Copying,
    Rejected,
    Activated,
    Submitted,
    Building,
    Activating
});
shared_unit_enum!(DeploymentInputMode { Horizon, Direct });
shared_unit_enum!(TestRunPhase {
    Submitted,
    BringingUp,
    TearingDown,
    Completed,
    Deploying,
    Asserting,
    Failed
});
shared_unit_enum!(WatchRejectionReason {
    MalformedWatch,
    SubscriptionLimitReached,
    StreamUnavailable
});
shared_unit_enum!(ActivationBackend {
    HomeManagerNixProfileV1,
    NixosSystemdBootV1
});
shared_unit_enum!(GenerationArtifact {
    BaseHost,
    CompleteHost,
    UserEnvironment
});
impl Raisable<ordinary::DeploymentPhase> for memory::DeploymentLifecycle {
    fn raise(self) -> Result<ordinary::DeploymentPhase, WireShapeError> {
        Ok(match self {
            Self::Failed => ordinary::DeploymentPhase::Failed,
            Self::Rejected => ordinary::DeploymentPhase::Rejected,
            Self::Completed => ordinary::DeploymentPhase::Completed,
            Self::Building => ordinary::DeploymentPhase::Building,
            Self::Activating => ordinary::DeploymentPhase::Activating,
            Self::Submitted => ordinary::DeploymentPhase::Submitted,
            Self::Copying => ordinary::DeploymentPhase::Copying,
            Self::Activated => ordinary::DeploymentPhase::Activated,
            Self::Built => ordinary::DeploymentPhase::Built,
        })
    }
}
shared_unit_enum!(ActivationEffect {
    ProfileOnly,
    BootOnceProfile,
    TestActivation,
    LiveActivation,
    BootProfile
});
shared_unit_enum!(TestMode { Hermetic, Live });
shared_unit_enum!(QueryRejectionReason {
    MalformedSelector,
    EventLogPositionOutOfRange,
    GenerationUnknown,
    NodeUnknown
});
shared_unit_enum!(SourceRevisionPolicy {
    ResolveAndRecord,
    RequireImmutable
});
shared_unit_enum!(DeploymentFailureStage {
    Build,
    Eval,
    MaterializeHorizon,
    Daemon,
    Activate,
    CopyClosure,
    Admission,
    FlakeAuth
});
shared_unit_enum!(DeploymentTerminalReason {
    NodeUnknown,
    FlakeReferenceMalformed,
    ProposalSourceUnreachable,
    DeploymentInFlight,
    InvalidDeploymentRouting,
    UnsupportedDeployAction,
    InternalError,
    ClusterUnknown,
    ActivationFailed,
    BuilderUnreachable,
    SubstituterUnreachable,
    EvaluationFailed,
    BuildFailed,
    ClosureCopyFailed
});
owner_unit_enum!(PinRejectionReason {
    PinSlotExhausted,
    InternalError,
    NodeUnknown,
    PinLabelInUse,
    GenerationUnknown
});
owner_unit_enum!(RetireRejectionReason {
    NodeUnknown,
    GenerationUnknown,
    GenerationPinned,
    InternalError,
    GenerationActive
});
owner_unit_enum!(UnpinRejectionReason {
    GenerationNotPinned,
    PinLabelUnknown,
    InternalError,
    NodeUnknown
});
owner_unit_enum!(DeployRefusalReason {
    ContinuationBudgetExhausted,
    NoCorrelatedDeployment,
    DurableWriteFailed
});
owner_unit_enum!(TestRejectionReason {
    SubstrateUnavailable,
    NoTestDefaults,
    ClusterUnknown,
    HostDeclaresNoVmHost,
    LiveNotYetEnabled,
    NodeUnknown,
    VmHostNotDeclaredForNode,
    InternalError
});

macro_rules! shared_unary_enum { ($name:ident { $($variant:ident($public:ty => $runtime:ty)),+ $(,)? }) => {
    impl Lowerable<memory::$name> for ordinary::$name {
        fn lower(self) -> Result<memory::$name, WireShapeError> { Ok(match self { $(ordinary::$name::$variant(value) => memory::$name::$variant(<$public as Lowerable<$runtime>>::lower(value)?)),+ }) }
    }
    impl Raisable<ordinary::$name> for memory::$name {
        fn raise(self) -> Result<ordinary::$name, WireShapeError> { Ok(match self { $(memory::$name::$variant(value) => ordinary::$name::$variant(<$runtime as Raisable<$public>>::raise(value)?)),+ }) }
    }
}; }
macro_rules! owner_unary_enum { ($name:ident { $($variant:ident($public:ty => $runtime:ty)),+ $(,)? }) => {
    impl Lowerable<memory::$name> for owner::$name {
        fn lower(self) -> Result<memory::$name, WireShapeError> { Ok(match self { $(owner::$name::$variant(value) => memory::$name::$variant(<$public as Lowerable<$runtime>>::lower(value)?)),+ }) }
    }
    impl Raisable<owner::$name> for memory::$name {
        fn raise(self) -> Result<owner::$name, WireShapeError> { Ok(match self { $(memory::$name::$variant(value) => owner::$name::$variant(<$runtime as Raisable<$public>>::raise(value)?)),+ }) }
    }
}; }

impl Lowerable<memory::SecretsInput> for ordinary::SecretsInput {
    fn lower(self) -> Result<memory::SecretsInput, WireShapeError> {
        Ok(match self {
            Self::NoSecrets => memory::SecretsInput::NoSecrets,
            Self::SecretsDirectory(value) => memory::SecretsInput::SecretsDirectory(value.lower()?),
        })
    }
}
impl Raisable<ordinary::SecretsInput> for memory::SecretsInput {
    fn raise(self) -> Result<ordinary::SecretsInput, WireShapeError> {
        Ok(match self {
            Self::NoSecrets => ordinary::SecretsInput::NoSecrets,
            Self::SecretsDirectory(value) => {
                ordinary::SecretsInput::SecretsDirectory(value.raise()?)
            }
        })
    }
}
shared_unary_enum!(RequestedDeploymentAction {
    Host(ordinary::HostDeployAction => memory::HostDeployAction),
    UserEnvironment(ordinary::UserEnvironmentAction => memory::UserEnvironmentAction)
});
impl Lowerable<memory::TestOutcome> for ordinary::TestOutcome {
    fn lower(self) -> Result<memory::TestOutcome, WireShapeError> {
        Ok(match self {
            Self::Pending => memory::TestOutcome::Pending,
            Self::Passed => memory::TestOutcome::Passed,
            Self::Failed(value) => memory::TestOutcome::Failed(value.lower()?),
        })
    }
}
impl Raisable<ordinary::TestOutcome> for memory::TestOutcome {
    fn raise(self) -> Result<ordinary::TestOutcome, WireShapeError> {
        Ok(match self {
            Self::Pending => ordinary::TestOutcome::Pending,
            Self::Passed => ordinary::TestOutcome::Passed,
            Self::Failed(value) => ordinary::TestOutcome::Failed(value.raise()?),
        })
    }
}
impl Lowerable<memory::DeploymentEnvironment> for ordinary::DeploymentEnvironment {
    fn lower(self) -> Result<memory::DeploymentEnvironment, WireShapeError> {
        Ok(match self {
            Self::HostEnvironment => memory::DeploymentEnvironment::HostEnvironment,
            Self::UserEnvironment(value) => {
                memory::DeploymentEnvironment::UserEnvironment(value.lower()?)
            }
        })
    }
}
impl Raisable<ordinary::DeploymentEnvironment> for memory::DeploymentEnvironment {
    fn raise(self) -> Result<ordinary::DeploymentEnvironment, WireShapeError> {
        Ok(match self {
            Self::HostEnvironment => ordinary::DeploymentEnvironment::HostEnvironment,
            Self::UserEnvironment(value) => {
                ordinary::DeploymentEnvironment::UserEnvironment(value.raise()?)
            }
        })
    }
}
impl Lowerable<memory::HostSelection> for ordinary::HostSelection {
    fn lower(self) -> Result<memory::HostSelection, WireShapeError> {
        Ok(match self {
            Self::DefaultHost => memory::HostSelection::DefaultHost,
            Self::OnHost(value) => memory::HostSelection::OnHost(value.lower()?),
        })
    }
}
impl Raisable<ordinary::HostSelection> for memory::HostSelection {
    fn raise(self) -> Result<ordinary::HostSelection, WireShapeError> {
        Ok(match self {
            Self::DefaultHost => ordinary::HostSelection::DefaultHost,
            Self::OnHost(value) => ordinary::HostSelection::OnHost(value.raise()?),
        })
    }
}
impl Lowerable<memory::DeploymentTerminal> for ordinary::DeploymentTerminal {
    fn lower(self) -> Result<memory::DeploymentTerminal, WireShapeError> {
        Ok(match self {
            Self::Succeeded => memory::DeploymentTerminal::Succeeded,
            Self::Failed(value) => memory::DeploymentTerminal::Failed(value.lower()?),
            Self::Rejected(value) => memory::DeploymentTerminal::Rejected(value.lower()?),
        })
    }
}
impl Raisable<ordinary::DeploymentTerminal> for memory::DeploymentTerminal {
    fn raise(self) -> Result<ordinary::DeploymentTerminal, WireShapeError> {
        Ok(match self {
            Self::Succeeded => ordinary::DeploymentTerminal::Succeeded,
            Self::Failed(value) => ordinary::DeploymentTerminal::Failed(value.raise()?),
            Self::Rejected(value) => ordinary::DeploymentTerminal::Rejected(value.raise()?),
        })
    }
}
impl Lowerable<memory::Selection> for ordinary::Selection {
    fn lower(self) -> Result<memory::Selection, WireShapeError> {
        Ok(match self {
            Self::ByNode(value) => memory::Selection::ByNode(value.lower()?),
            Self::ByGeneration(value) => memory::Selection::ByGeneration(value.lower()?),
            Self::ByDeployment(value) => memory::Selection::ByDeployment(value.lower()?),
        })
    }
}
owner_unary_enum!(TestRequest {
    Run(owner::TestRun => memory::TestRun), Check(owner::QuickCheck => memory::QuickCheck)
});
owner_unary_enum!(DeploySubmission {
    UserEnvironment(owner::UserEnvironmentDeployment => memory::UserEnvironmentDeployment),
    Host(owner::HostDeployment => memory::HostDeployment)
});
impl Lowerable<memory::NodeSelection> for owner::NodeSelection {
    fn lower(self) -> Result<memory::NodeSelection, WireShapeError> {
        Ok(match self {
            Self::All => memory::NodeSelection::All,
            Self::Nodes(value) => memory::NodeSelection::Nodes(value.lower()?),
        })
    }
}
impl Raisable<owner::NodeSelection> for memory::NodeSelection {
    fn raise(self) -> Result<owner::NodeSelection, WireShapeError> {
        Ok(match self {
            Self::All => owner::NodeSelection::All,
            Self::Nodes(value) => owner::NodeSelection::Nodes(value.raise()?),
        })
    }
}

shared_struct!(DeploymentTransport => DeploymentTransport { nix_store_uri => nix_store_uri, ssh_destination => ssh_destination });
shared_struct!(TestExecutionProfile => TestExecutionProfile { test_mode => test_mode, nix_system => nix_system, deployment_output_selector => deployment_output_selector, deployment_transport_option => optional_deployment_transport });
shared_struct!(DatabaseMarker => StateMarker { commit_sequence => commit_sequence, state_digest => state_digest });
shared_struct!(NodeSelector => NodeSelector { cluster_name => cluster_name, node_name => node_name, generation_artifact_option => optional_generation_artifact });
shared_struct!(EventLogRange => EventLogRange { first_event_log_position => from, second_event_log_position => until });
shared_struct!(TestRunLookup => TestRunLookup { cluster_name => cluster_name, node_name => node_name, test_run_identifier_option => optional_test_run_identifier });
shared_struct!(DeploymentWatch => DeploymentWatch { deployment_identifier_option => optional_deployment_identifier, cluster_name_option => optional_cluster_name, node_name_option => optional_node_name });
shared_struct!(DeploymentFailure => DeploymentFailure { deployment_failure_stage => deployment_failure_stage, deployment_terminal_reason => deployment_terminal_reason, failure_evidence_option => optional_failure_evidence });
shared_struct!(FailureEvidence => FailureEvidence { failed_command_option => optional_failed_command, failure_detail => failure_detail, detail_truncated => detail_truncated });
shared_struct!(FailedCommand => FailedCommand { command_program => command_program, command_argument_vector => command_argument_vector, exit_code_option => optional_exit_code });

impl Lowerable<memory::GenerationLookup> for ordinary::GenerationLookup {
    fn lower(self) -> Result<memory::GenerationLookup, WireShapeError> {
        Ok(memory::GenerationLookup::new(
            self.generation_identifier.lower()?,
        ))
    }
}
impl Raisable<ordinary::GenerationLookup> for memory::GenerationLookup {
    fn raise(self) -> Result<ordinary::GenerationLookup, WireShapeError> {
        Ok(ordinary::GenerationLookup {
            generation_identifier: self.into_payload().raise()?,
        })
    }
}
impl Lowerable<memory::DeploymentLookup> for ordinary::DeploymentLookup {
    fn lower(self) -> Result<memory::DeploymentLookup, WireShapeError> {
        Ok(memory::DeploymentLookup::new(
            self.deployment_identifier.lower()?,
        ))
    }
}
impl Raisable<ordinary::DeploymentLookup> for memory::DeploymentLookup {
    fn raise(self) -> Result<ordinary::DeploymentLookup, WireShapeError> {
        Ok(ordinary::DeploymentLookup {
            deployment_identifier: self.into_payload().raise()?,
        })
    }
}
impl Lowerable<memory::SubscriptionClose> for ordinary::SubscriptionClose {
    fn lower(self) -> Result<memory::SubscriptionClose, WireShapeError> {
        Ok(memory::SubscriptionClose::new(
            self.subscription_token.lower()?,
        ))
    }
}
impl Raisable<ordinary::SubscriptionClose> for memory::SubscriptionClose {
    fn raise(self) -> Result<ordinary::SubscriptionClose, WireShapeError> {
        Ok(ordinary::SubscriptionClose {
            subscription_token: self.into_payload().raise()?,
        })
    }
}
impl Lowerable<memory::DeploymentOutputSelector> for ordinary::DeploymentOutputSelector {
    fn lower(self) -> Result<memory::DeploymentOutputSelector, WireShapeError> {
        Ok(memory::DeploymentOutputSelector::new(
            self.flake_attribute.lower()?,
        ))
    }
}
impl Raisable<ordinary::DeploymentOutputSelector> for memory::DeploymentOutputSelector {
    fn raise(self) -> Result<ordinary::DeploymentOutputSelector, WireShapeError> {
        Ok(ordinary::DeploymentOutputSelector {
            flake_attribute: self.into_payload().raise()?,
        })
    }
}
impl Lowerable<memory::QuickCheck> for owner::QuickCheck {
    fn lower(self) -> Result<memory::QuickCheck, WireShapeError> {
        Ok(memory::QuickCheck::new(self.0.lower()?))
    }
}
impl Raisable<owner::QuickCheck> for memory::QuickCheck {
    fn raise(self) -> Result<owner::QuickCheck, WireShapeError> {
        self.into_payload().raise().map(owner::QuickCheck)
    }
}

owner_struct!(PinRequest => PinRequest { cluster_name => cluster_name, node_name => node_name, generation_identifier => generation_identifier, pin_label => pin_label });
owner_struct!(UnpinRequest => UnpinRequest { cluster_name => cluster_name, node_name => node_name, pin_label => pin_label });
owner_struct!(RetireRequest => RetireRequest { cluster_name => cluster_name, node_name => node_name, generation_identifier => generation_identifier });
owner_struct!(TestRun => TestRun { cluster_name => cluster_name, node_selection => node_selection, host_selection => host_selection, test_execution_profile => test_execution_profile });
owner_struct!(ExtraSubstituter => ExtraSubstituter { first_string => url, second_string => public_key });
owner_struct_with_none!(HostDeployment => HostDeployment {
    cluster_name => cluster_name, node_name => node_name, host_composition => host_composition,
    proposal_source => proposal_source, secrets_input => secrets_input, flake_reference => flake_reference,
    deployment_transport => deployment_transport, deployment_input_mode => deployment_input_mode,
    deployment_output_selector => deployment_output_selector, activation_backend => activation_backend,
    host_deploy_action => host_deploy_action, source_revision_policy => source_revision_policy,
    nix_builder_spec_option => optional_nix_builder_spec, extra_substituter_vector => extra_substituter_vector
}, horizon_definition_option);
owner_struct_with_none!(UserEnvironmentDeployment => UserEnvironmentDeployment {
    cluster_name => cluster_name, node_name => node_name, user_name => user_name,
    proposal_source => proposal_source, secrets_input => secrets_input, flake_reference => flake_reference,
    deployment_transport => deployment_transport, deployment_input_mode => deployment_input_mode,
    deployment_output_selector => deployment_output_selector, activation_backend => activation_backend,
    user_environment_action => user_environment_action, source_revision_policy => source_revision_policy,
    nix_builder_spec_option => optional_nix_builder_spec, extra_substituter_vector => extra_substituter_vector
}, horizon_definition_option);

impl Lowerable<memory::DeploySubmission> for owner::ActualizedDeploySubmission {
    fn lower(self) -> Result<memory::DeploySubmission, WireShapeError> {
        let mut submission = self.deploy_submission.lower()?;
        match &mut submission {
            memory::DeploySubmission::Host(value) => {
                value.horizon_definition_option = self.horizon_definition_option;
            }
            memory::DeploySubmission::UserEnvironment(value) => {
                value.horizon_definition_option = self.horizon_definition_option;
            }
        }
        Ok(submission)
    }
}

impl Raisable<ordinary::DatabaseMarker> for memory::DatabaseMarker {
    fn raise(self) -> Result<ordinary::DatabaseMarker, WireShapeError> {
        self.into_payload().raise()
    }
}
// A marker is its own struct on the wire, holding the DatabaseMarker it marks.
macro_rules! marker_wrapper {
    ($runtime:ident) => {
        impl Raisable<ordinary::$runtime> for memory::$runtime {
            fn raise(self) -> Result<ordinary::$runtime, WireShapeError> {
                Ok(ordinary::$runtime {
                    database_marker: self.into_payload().raise()?,
                })
            }
        }
    };
}
marker_wrapper!(AdmissionMarker);
marker_wrapper!(TerminalMarker);
impl Raisable<ordinary::TransitionMarker> for memory::StateMarker {
    fn raise(self) -> Result<ordinary::TransitionMarker, WireShapeError> {
        Ok(ordinary::TransitionMarker {
            database_marker: self.raise()?,
        })
    }
}

shared_raise_struct!(DeploymentRequestIdentity => DeploymentRequestIdentity {
    deployment_environment => deployment_environment, cluster_name => cluster_name,
    node_name => node_name, generation_artifact => generation_artifact,
    requested_deployment_action => requested_deployment_action, activation_effect => activation_effect,
    source_revision_policy => source_revision_policy, immutable_revision_option => optional_immutable_revision
});
shared_raise_struct!(DeploymentRecord => DeploymentRecord {
    deployment_identifier => deployment_identifier, generation_identifier => generation_identifier,
    deployment_request_identity => deployment_request_identity, admission_marker_option => optional_admission_marker,
    deployment_phase => deployment_lifecycle, terminal_marker_option => optional_terminal_marker,
    deployment_terminal_option => optional_deployment_terminal
});
shared_raise_struct!(DeploymentPhaseEvent => DeploymentPhaseEvent {
    deployment_identifier => deployment_identifier, generation_identifier => generation_identifier,
    cluster_name => cluster_name, node_name => node_name, deployment_phase => deployment_phase,
    event_log_position => event_log_position, transition_marker => state_marker,
    immutable_revision_option => optional_immutable_revision, deployment_terminal_option => optional_deployment_terminal
});
shared_raise_struct!(CacheRetentionTransitionEvent => CacheRetentionTransitionEvent {
    generation_identifier => generation_identifier, cluster_name => cluster_name, node_name => node_name,
    cache_retention_transition => cache_retention_transition, generation_slot => generation_slot,
    generation_slot_option => optional_generation_slot, pin_label_option => optional_pin_label,
    event_log_position => event_log_position
});
shared_raise_struct!(RejectedQuery => RejectedQuery { query_rejection_reason => query_rejection_reason, database_marker => state_marker });
shared_raise_struct!(RejectedUnwatch => RejectedUnwatch { unwatch_rejection_reason => unwatch_rejection_reason, subscription_token => subscription_token });
shared_raise_struct!(SubscriptionOpened => SubscriptionOpened { subscription_token => subscription_token, commit_sequence => commit_sequence });
shared_raise_struct!(GenerationListing => GenerationListing { generation_vector => generation_vector, deployment_record_vector => deployment_record_vector, database_marker => state_marker });
shared_raise_struct!(EventLogPage => EventLogPage { deployment_phase_event_vector => deployment_phase_event_vector, cache_retention_transition_event_vector => cache_retention_transition_event_vector, database_marker => state_marker });
shared_raise_struct!(TestRunListing => TestRunListing { test_run_record_vector => test_run_record_vector, database_marker => database_marker });

owner_raise_struct!(DeployHandle => DeployHandle { deployment_identifier => deployment_identifier, database_marker => state_marker });
owner_raise_struct!(AcceptedTest => AcceptedTest { test_run_identifier => test_run_identifier, database_marker => state_marker });
owner_raise_struct!(AppliedPin => AppliedPin { generation_identifier => generation_identifier, pin_label => pin_label, first_generation_slot => from_slot, second_generation_slot => to_slot, database_marker => state_marker });
owner_raise_struct!(AppliedUnpin => AppliedUnpin { generation_identifier => generation_identifier, pin_label => pin_label, first_generation_slot => from_slot, second_generation_slot => to_slot, database_marker => state_marker });
owner_raise_struct!(AppliedRetire => AppliedRetire { generation_identifier => generation_identifier, generation_slot => generation_slot, database_marker => state_marker });
owner_raise_struct!(RejectedPin => RejectedPin { pin_rejection_reason => pin_rejection_reason, database_marker => state_marker });
owner_raise_struct!(RejectedUnpin => RejectedUnpin { unpin_rejection_reason => unpin_rejection_reason, database_marker => state_marker });
owner_raise_struct!(RejectedRetire => RejectedRetire { retire_rejection_reason => retire_rejection_reason, database_marker => state_marker });
owner_raise_struct!(RejectedTest => RejectedTest { test_rejection_reason => test_rejection_reason, database_marker => state_marker });
owner_raise_struct!(RefusedDeploy => RefusedDeploy { deploy_refusal_reason => deploy_refusal_reason, database_marker => state_marker });

impl Raisable<ordinary::RejectedWatch> for memory::RejectedWatch {
    fn raise(self) -> Result<ordinary::RejectedWatch, WireShapeError> {
        Ok(ordinary::RejectedWatch {
            watch_rejection_reason: self.into_payload().raise()?,
        })
    }
}
impl Raisable<ordinary::SubscriptionClosed> for memory::SubscriptionClosed {
    fn raise(self) -> Result<ordinary::SubscriptionClosed, WireShapeError> {
        Ok(ordinary::SubscriptionClosed {
            subscription_token: self.into_payload().raise()?,
        })
    }
}
impl Raisable<owner::RejectedDeploy> for memory::RejectedDeploy {
    fn raise(self) -> Result<owner::RejectedDeploy, WireShapeError> {
        Ok(owner::RejectedDeploy {
            deployment_record: self.into_payload().raise()?,
        })
    }
}

impl Raisable<ordinary::Generation> for memory::Generation {
    fn raise(self) -> Result<ordinary::Generation, WireShapeError> {
        let closure_path_option = NixStorePath::from(self.closure_path.payload().as_str())
            .is_canonical_item_root()
            .then_some(self.closure_path)
            .raise()?;
        Ok(ordinary::Generation {
            generation_identifier: self.generation_identifier.raise()?,
            deployment_identifier: self.deployment_identifier.raise()?,
            cluster_name: self.cluster_name.raise()?,
            node_name: self.node_name.raise()?,
            generation_artifact: self.generation_artifact.raise()?,
            activation_effect: self.activation_effect.raise()?,
            generation_slot: self.generation_slot.raise()?,
            closure_path_option,
            immutable_revision_option: self.optional_immutable_revision.raise()?,
        })
    }
}
impl Raisable<ordinary::TestRunRecord> for memory::TestRunRecord {
    fn raise(self) -> Result<ordinary::TestRunRecord, WireShapeError> {
        let closure_path_option = self
            .optional_closure_path
            .filter(|path| NixStorePath::from(path.payload().as_str()).is_canonical_item_root())
            .raise()?;
        Ok(ordinary::TestRunRecord {
            test_run_identifier: self.test_run_identifier.raise()?,
            cluster_name: self.cluster_name.raise()?,
            first_node_name: self.node.raise()?,
            second_node_name: self.host.raise()?,
            test_mode: self.test_mode.raise()?,
            test_run_phase: self.test_run_phase.raise()?,
            test_outcome: self.test_outcome.raise()?,
            closure_path_option,
        })
    }
}

impl Lowerable<memory::OrdinaryIngress> for ordinary::Input {
    fn lower(self) -> Result<memory::OrdinaryIngress, WireShapeError> {
        Ok(match self {
            ordinary::Input::Configure(value) => memory::OrdinaryIngress::Configure(value.lower()?),
            ordinary::Input::WatchDeployments(value) => {
                memory::OrdinaryIngress::WatchDeployments(value.lower()?)
            }
            ordinary::Input::Query(value) => memory::OrdinaryIngress::Query(value.lower()?),
            ordinary::Input::QueryDeploymentEvents(value) => {
                memory::OrdinaryIngress::Query(memory::Selection::ByEventLog(value.lower()?))
            }
            ordinary::Input::QueryTestRuns(value) => {
                memory::OrdinaryIngress::Query(memory::Selection::ByTestRun(value.lower()?))
            }
            ordinary::Input::Unwatch(value) => memory::OrdinaryIngress::Unwatch(value.lower()?),
        })
    }
}
impl Lowerable<memory::MetaIngress> for owner::Input {
    fn lower(self) -> Result<memory::MetaIngress, WireShapeError> {
        Ok(match self {
            owner::Input::Configure(value) => memory::MetaIngress::Configure(value.lower()?),
            owner::Input::ReverseConfiguration => memory::MetaIngress::ReverseConfiguration,
            owner::Input::Retire(value) => memory::MetaIngress::Retire(value.lower()?),
            owner::Input::Pin(value) => memory::MetaIngress::Pin(value.lower()?),
            owner::Input::Deploy(value) => memory::MetaIngress::Deploy(value.lower()?),
            owner::Input::Test(value) => memory::MetaIngress::Test(value.lower()?),
            owner::Input::Unpin(value) => memory::MetaIngress::Unpin(value.lower()?),
        })
    }
}

impl Raisable<ordinary::Output> for memory::OrdinaryEgress {
    fn raise(self) -> Result<ordinary::Output, WireShapeError> {
        Ok(match self {
            memory::OrdinaryEgress::Configured(value) => {
                ordinary::Output::Configured(value.raise()?)
            }
            memory::OrdinaryEgress::ConfigurationRejected(value) => {
                ordinary::Output::ConfigurationRejected(value.raise()?)
            }
            memory::OrdinaryEgress::TestRunsQueried(value) => {
                ordinary::Output::TestRunsQueried(value.raise()?)
            }
            memory::OrdinaryEgress::UnwatchRejected(value) => {
                ordinary::Output::UnwatchRejected(value.raise()?)
            }
            memory::OrdinaryEgress::QueryRejected(value) => {
                ordinary::Output::QueryRejected(value.raise()?)
            }
            memory::OrdinaryEgress::Watching(value) => {
                ordinary::Output::DeploymentsWatched(value.raise()?)
            }
            memory::OrdinaryEgress::Queried(value) => ordinary::Output::Queried(value.raise()?),
            memory::OrdinaryEgress::DeploymentEventsQueried(value) => {
                ordinary::Output::DeploymentEventsQueried(value.raise()?)
            }
            memory::OrdinaryEgress::Unwatched(value) => ordinary::Output::Unwatched(value.raise()?),
            memory::OrdinaryEgress::WatchRejected(value) => {
                ordinary::Output::WatchRejected(value.raise()?)
            }
            memory::OrdinaryEgress::StoreRefused(detail) => {
                ordinary::Output::Refused(ordinary::Refusal::Store(detail))
            }
        })
    }
}
impl Raisable<owner::Output> for memory::MetaEgress {
    fn raise(self) -> Result<owner::Output, WireShapeError> {
        Ok(match self {
            memory::MetaEgress::Configured(value) => owner::Output::Configured(value.raise()?),
            memory::MetaEgress::ConfigurationRejected(value) => {
                owner::Output::ConfigurationRejected(value.raise()?)
            }
            memory::MetaEgress::ConfigurationReversed(value) => {
                owner::Output::ConfigurationReversed(value.raise()?)
            }
            memory::MetaEgress::PinRejected(value) => owner::Output::PinRejected(value.raise()?),
            memory::MetaEgress::DeployRefused(value) => {
                owner::Output::DeployRefused(value.raise()?)
            }
            memory::MetaEgress::DeployRejected(value) => {
                owner::Output::DeployRejected(value.raise()?)
            }
            memory::MetaEgress::DeployAccepted(value) => {
                owner::Output::DeployAccepted(value.raise()?)
            }
            memory::MetaEgress::TestRejected(value) => owner::Output::TestRejected(value.raise()?),
            memory::MetaEgress::Unpinned(value) => owner::Output::Unpinned(value.raise()?),
            memory::MetaEgress::Tested(value) => owner::Output::Tested(value.raise()?),
            memory::MetaEgress::UnpinRejected(value) => {
                owner::Output::UnpinRejected(value.raise()?)
            }
            memory::MetaEgress::DeployTerminal(value) => {
                owner::Output::DeployTerminal(value.raise()?)
            }
            memory::MetaEgress::Pinned(value) => owner::Output::Pinned(value.raise()?),
            memory::MetaEgress::RetireRejected(value) => {
                owner::Output::RetireRejected(value.raise()?)
            }
            memory::MetaEgress::Retired(value) => owner::Output::Retired(value.raise()?),
            memory::MetaEgress::StoreRefused(detail) => {
                owner::Output::Refused(ordinary::Refusal::Store(detail))
            }
        })
    }
}

impl Raisable<ordinary::ConfigurationReceipt> for memory::ConfigurationReceipt {
    fn raise(self) -> Result<ordinary::ConfigurationReceipt, WireShapeError> {
        Ok(ordinary::ConfigurationReceipt {
            lojix_nexus_configuration: self.configuration,
            meta_configure_occurred: ordinary::MetaConfigureOccurred(self.meta_configure_occurred),
        })
    }
}

impl Raisable<ordinary::ConfigurationRejection> for memory::ConfigurationRejection {
    fn raise(self) -> Result<ordinary::ConfigurationRejection, WireShapeError> {
        Ok(ordinary::ConfigurationRejection {
            configuration_rejection_reason: match self.reason {
                memory::ConfigurationRejectionReason::OrdinaryConfigureClosed => {
                    ordinary::ConfigurationRejectionReason::OrdinaryConfigureClosed
                }
                memory::ConfigurationRejectionReason::InvalidConfiguration => {
                    ordinary::ConfigurationRejectionReason::InvalidConfiguration
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> memory::StateMarker {
        memory::StateMarker {
            commit_sequence: memory::CommitSequence::new(7),
            state_digest: memory::StateDigest::new(7),
        }
    }
    fn generation(path: &str) -> memory::Generation {
        memory::Generation {
            generation_identifier: memory::GenerationIdentifier::new(1),
            deployment_identifier: memory::DeploymentIdentifier::new(1),
            cluster_name: memory::ClusterName::from("alpha"),
            node_name: memory::NodeName::from("node-1"),
            generation_artifact: memory::GenerationArtifact::BaseHost,
            activation_effect: memory::ActivationEffect::LiveActivation,
            generation_slot: memory::GenerationSlot::Current,
            closure_path: memory::ClosurePath::from(path),
            optional_immutable_revision: None,
        }
    }
    #[test]
    fn ordinary_projection_keeps_only_canonical_store_item_roots() {
        let valid = "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system-toplevel";
        let visible = memory::OrdinaryEgress::Queried(memory::GenerationListing {
            generation_vector: vec![generation(valid)],
            deployment_record_vector: Vec::new(),
            state_marker: marker(),
        })
        .raise()
        .unwrap();
        assert!(format!("{visible:?}").contains(valid));
        for private in [
            "/home/li/private",
            "/nix/store/short-system",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-secret",
        ] {
            let visible = memory::OrdinaryEgress::Queried(memory::GenerationListing {
                generation_vector: vec![generation(private)],
                deployment_record_vector: Vec::new(),
                state_marker: marker(),
            })
            .raise()
            .unwrap();
            assert!(!format!("{visible:?}").contains(private));
        }
    }
    #[test]
    fn a_store_failure_is_answered_as_a_store_refusal_on_either_socket() {
        let detail = String::from("read durable generations: the file is gone");
        assert_eq!(
            memory::OrdinaryEgress::StoreRefused(detail.clone())
                .raise()
                .unwrap(),
            ordinary::Output::Refused(ordinary::Refusal::Store(detail.clone()))
        );
        assert_eq!(
            memory::MetaEgress::StoreRefused(detail.clone())
                .raise()
                .unwrap(),
            owner::Output::Refused(ordinary::Refusal::Store(detail))
        );
    }
    #[test]
    fn the_split_query_inputs_lower_to_the_selections_the_engine_reads() {
        let range = ordinary::EventLogRange {
            first_event_log_position: ordinary::EventLogPosition(1),
            second_event_log_position: ordinary::EventLogPosition(9),
        };
        assert!(matches!(
            ordinary::Input::QueryDeploymentEvents(range)
                .lower()
                .unwrap(),
            memory::OrdinaryIngress::Query(memory::Selection::ByEventLog(_))
        ));
        let lookup = ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName("alpha".into()),
            node_name: ordinary::NodeName("node-1".into()),
            test_run_identifier_option: None,
        };
        assert!(matches!(
            ordinary::Input::QueryTestRuns(lookup).lower().unwrap(),
            memory::OrdinaryIngress::Query(memory::Selection::ByTestRun(_))
        ));
    }
    #[test]
    fn negative_public_identifiers_are_rejected_without_wrapping() {
        assert!(
            ordinary::Input::Unwatch(ordinary::SubscriptionClose {
                subscription_token: ordinary::SubscriptionToken(-1)
            })
            .lower()
            .is_err()
        );
    }
}
