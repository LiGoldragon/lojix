//! Structural boundary between encoded public Interfaces and lojix-owned runtime nouns.
//!
//! The public crates own identity, ordering, archive behavior, and Signal roles.
//! Lojix owns only its readable runtime model. Translation crosses the four
//! encoded roots through the producer-owned structural behavior; no readable
//! public alias vocabulary is reproduced here.

use crate::runtime_model as sema;
use sema::*;
use datom_codec::{Conceivable, Datom, Incorporable, IncorporationBudget};
use protos::Situation;

/// The daemon's runtime model is independent from the generated public types.
/// This private structural form is the current Datom grammar at that boundary;
/// it is never archived, sent, or accepted as a public compatibility protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeDatom {
    Text(String),
    Atom(String),
    Vector(Vec<RuntimeDatom>),
    Struct(Vec<RuntimeDatom>),
    Variant { name: String, body: Box<RuntimeDatom> },
}

#[derive(Debug, thiserror::Error)]
#[error("runtime value does not match the current generated Datom contract")]
struct WireShapeError;

trait WireShape: Sized {
    fn to_wire(&self) -> RuntimeDatom;
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError>;
}

impl RuntimeDatom {
    fn from_datom(value: &Datom) -> Self {
        match value {
            Datom::Text(value) => Self::Text(value.to_string()),
            Datom::Word(value) => Self::Atom(value.as_ref().to_owned()),
            Datom::Meaning(value) => Self::Text(value.to_string()),
            Datom::Vector(values) => Self::Vector(values.iter().map(Self::from_datom).collect()),
            Datom::Struct(values) => Self::Struct(values.iter().map(Self::from_datom).collect()),
            Datom::Variant(name, body) => Self::Variant { name: name.as_ref().to_owned(), body: Box::new(Self::from_datom(body)) },
        }
    }

    fn into_datom(self) -> Result<Datom, WireShapeError> {
        let symbol = |name: String| protos::Symbol::try_from(name).map_err(|_| WireShapeError);
        match self {
            Self::Text(value) => Ok(Datom::Text(protos::Text::try_from(value).map_err(|_| WireShapeError)?)),
            Self::Atom(value) => Ok(Datom::Word(datom_codec::DatomWord::try_from(value.as_str()).map_err(|_| WireShapeError)?)),
            Self::Vector(values) => values.into_iter().map(Self::into_datom).collect::<Result<Vec<_>, _>>().map(Datom::Vector),
            Self::Struct(values) => values.into_iter().map(Self::into_datom).collect::<Result<Vec<_>, _>>().map(Datom::Struct),
            Self::Variant { name, body } => Ok(Datom::Variant(symbol(name)?, Box::new(body.into_datom()?))),
        }
    }
}

impl WireShape for String {
    fn to_wire(&self) -> RuntimeDatom { RuntimeDatom::Text(self.clone()) }
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> { match value { RuntimeDatom::Text(value) => Ok(value), _ => Err(WireShapeError) } }
}
impl WireShape for u64 {
    fn to_wire(&self) -> RuntimeDatom { RuntimeDatom::Atom(self.to_string()) }
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> { match value { RuntimeDatom::Atom(value) => value.parse().map_err(|_| WireShapeError), _ => Err(WireShapeError) } }
}
impl WireShape for bool {
    fn to_wire(&self) -> RuntimeDatom { RuntimeDatom::Atom(if *self { "True" } else { "False" }.to_owned()) }
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> { match value { RuntimeDatom::Atom(value) if value == "True" => Ok(true), RuntimeDatom::Atom(value) if value == "False" => Ok(false), _ => Err(WireShapeError) } }
}
impl<T: WireShape> WireShape for Vec<T> {
    fn to_wire(&self) -> RuntimeDatom { RuntimeDatom::Vector(self.iter().map(WireShape::to_wire).collect()) }
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> { let RuntimeDatom::Vector(values) = value else { return Err(WireShapeError) }; values.into_iter().map(T::from_wire).collect() }
}
impl<T: WireShape> WireShape for Option<T> {
    fn to_wire(&self) -> RuntimeDatom { match self { Some(value) => RuntimeDatom::Variant { name: "Some".to_owned(), body: Box::new(value.to_wire()) }, None => RuntimeDatom::Atom("None".to_owned()) } }
    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> { match value { RuntimeDatom::Atom(name) if name == "None" => Ok(None), RuntimeDatom::Variant { name, body } if name == "Some" => Ok(Some(T::from_wire(*body)?)), _ => Err(WireShapeError) } }
}

fn current_datom<T: Conceivable<Datom, Fault = std::convert::Infallible>>(value: &T) -> RuntimeDatom {
    let datom = value.conceive().expect("generated Datom ascent is infallible").1;
    RuntimeDatom::from_datom(&datom)
}

fn generated_root<T: datom_codec::Datomic>(value: RuntimeDatom) -> crate::Result<T> {
    let value = value.into_datom().map_err(|error| crate::Error::Wire(error.to_string()))?;
    value.incorporate(
        &Situation { extent: protos::Extent(0, 0), children: Vec::new() },
        IncorporationBudget::try_from(16_384).expect("positive Datom budget"),
    ).map_err(|error| crate::Error::Wire(format!("{error:?}")))
}

macro_rules! wire_newtype {
    ($name:ident, $inner:ty) => {
        impl WireShape for sema::$name {
            fn to_wire(&self) -> RuntimeDatom {
                self.payload().to_wire()
            }
            fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
                Ok(Self::new(<$inner as WireShape>::from_wire(value)?))
            }
        }
    };
}

// The public contract sometimes gives a one-field product its own noun.  The
// runtime keeps those nouns as compact newtypes, while this adapter preserves
// the product boundary at the wire crossing.
macro_rules! wire_product_newtype {
    ($name:ident, $inner:ty) => {
        impl WireShape for sema::$name {
            fn to_wire(&self) -> RuntimeDatom {
                RuntimeDatom::Struct(vec![self.payload().to_wire()])
            }

            fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
                let RuntimeDatom::Struct(mut fields) = value else {
                    return Err(WireShapeError);
                };
                if fields.len() != 1 {
                    return Err(WireShapeError);
                }
                let field = fields.pop().ok_or(WireShapeError)?;
                Ok(Self::new(<$inner as WireShape>::from_wire(field)?))
            }
        }
    };
}

macro_rules! wire_struct {
    ($name:ident { $($field:ident: $field_type:ty),* $(,)? }) => {
        impl WireShape for sema::$name {
            fn to_wire(&self) -> RuntimeDatom {
                RuntimeDatom::Struct(vec![$(self.$field.to_wire()),*])
            }
            fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
                let RuntimeDatom::Struct(fields) = value else { return Err(WireShapeError) };
                let mut fields = fields.into_iter();
                let result = Self {
                    $($field: <$field_type as WireShape>::from_wire(
                        fields.next().ok_or(WireShapeError)?,
                    )?),*
                };
                if fields.next().is_some() { return Err(WireShapeError); }
                Ok(result)
            }
        }
    };
}

macro_rules! wire_enum {
    ($name:ident {
        unit { $($unit_ordinal:literal => $unit:ident),* $(,)? }
        unary { $($unary_ordinal:literal => $unary:ident($payload:ty)),* $(,)? }
    }) => {
        impl WireShape for sema::$name {
            fn to_wire(&self) -> RuntimeDatom {
                match self {
                    $(Self::$unit => RuntimeDatom::Atom(stringify!($unit).to_owned()),)*
                    $(Self::$unary(payload) => RuntimeDatom::Variant { name: stringify!($unary).to_owned(), body: Box::new(payload.to_wire()) },)*
                }
            }
            fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
                match value {
                    $(RuntimeDatom::Atom(name) if name == stringify!($unit) => Ok(Self::$unit),)*
                    $(RuntimeDatom::Variant { name, body } if name == stringify!($unary) => Ok(Self::$unary(<$payload as WireShape>::from_wire(*body)?)),)*
                    _ => Err(WireShapeError),
                }
            }
        }
    };
}

wire_enum!(UserEnvironmentAction { unit { 0 => ActivateNow, 1 => Realize, 2 => SetProfile } unary {  } });
wire_enum!(OrdinaryEgress { unit {  } unary { 0 => TestRunsQueried(TestRunListing), 1 => UnwatchRejected(RejectedUnwatch), 2 => QueryRejected(RejectedQuery), 3 => Watching(SubscriptionOpened), 4 => KeyMaterialCheckRejected(RejectedKeyMaterialCheck), 5 => Queried(GenerationListing), 6 => DeploymentEventsQueried(EventLogPage), 7 => Unwatched(SubscriptionClosed), 8 => KeyMaterialChecked(KeyMaterialReport), 9 => WatchRejected(RejectedWatch) } });
wire_newtype!(GenerationIdentifier, u64);
wire_enum!(CacheRetentionTransition { unit { 0 => Demoted, 1 => Retired, 2 => Pinned, 3 => Promoted, 4 => Unpinned, 5 => Evicted } unary {  } });
wire_newtype!(CommitSequence, u64);
wire_struct!(DeploymentPhaseEvent { deployment_identifier: DeploymentIdentifier, generation_identifier: GenerationIdentifier, cluster_name: ClusterName, node_name: NodeName, deployment_phase: DeploymentPhase, event_log_position: EventLogPosition, state_marker: StateMarker, optional_immutable_revision: Option<ImmutableRevision>, optional_deployment_terminal: Option<DeploymentTerminal> });
wire_struct!(DeploymentWatch { optional_deployment_identifier: Option<DeploymentIdentifier>, optional_cluster_name: Option<ClusterName>, optional_node_name: Option<NodeName> });
wire_newtype!(ProposalSource, String);
wire_enum!(RequestedDeploymentAction { unit {  } unary { 0 => Host(HostDeployAction), 1 => UserEnvironment(UserEnvironmentAction) } });
wire_newtype!(NodeName, String);
wire_struct!(RejectedQuery {
    query_rejection_reason: QueryRejectionReason,
    state_marker: StateMarker
});
wire_product_newtype!(GenerationLookup, GenerationIdentifier);
wire_enum!(TestOutcome { unit { 1 => Pending, 2 => Passed } unary { 0 => Failed(FailureStage) } });
wire_enum!(KeyMaterialCheckRejectionReason { unit { 0 => ProposalSourceUnreachable, 1 => HostUnreachable, 2 => PublicationMalformed, 3 => NodeUnknown } unary {  } });
wire_struct!(TestRunLookup { cluster_name: ClusterName, node_name: NodeName, optional_test_run_identifier: Option<TestRunIdentifier> });
wire_newtype!(SubscriptionToken, u64);
wire_newtype!(NixSystem, String);
wire_struct!(DeploymentRecord { deployment_identifier: DeploymentIdentifier, generation_identifier: GenerationIdentifier, deployment_request_identity: DeploymentRequestIdentity, optional_admission_marker: Option<AdmissionMarker>, deployment_lifecycle: DeploymentLifecycle, optional_terminal_marker: Option<TerminalMarker>, optional_deployment_terminal: Option<DeploymentTerminal> });
wire_enum!(FailureStage { unit { 0 => HermeticCheck, 1 => BringUp, 2 => Assert, 3 => Deploy, 4 => TearDown } unary {  } });
wire_enum!(HostDeployAction { unit { 0 => TestActivation, 1 => ScheduleBootOnce, 2 => Realize, 3 => SetBootProfile, 4 => Evaluate, 5 => ActivateNow } unary {  } });
wire_newtype!(PinLabel, String);
wire_struct!(GenerationListing { generation_vector: Vec<Generation>, deployment_record_vector: Vec<DeploymentRecord>, state_marker: StateMarker });
wire_product_newtype!(DeploymentLookup, DeploymentIdentifier);
wire_enum!(UnwatchRejectionReason { unit { 0 => SubscriptionTokenUnknown, 1 => SubscriptionAlreadyClosed } unary {  } });
wire_enum!(GenerationSlot { unit { 0 => Pinned, 1 => Recent, 2 => Rollback, 3 => BootPending, 4 => Current } unary {  } });
wire_newtype!(TestRunIdentifier, u64);
wire_enum!(HostComposition { unit { 0 => CompleteHost, 1 => BaseHost } unary {  } });
wire_struct!(CacheRetentionWatch { optional_cluster_name: Option<ClusterName>, optional_node_name: Option<NodeName> });
wire_newtype!(FlakeAttribute, String);
wire_enum!(DeploymentPhase { unit { 0 => Built, 1 => Completed, 2 => Failed, 3 => Copying, 4 => Rejected, 5 => Activated, 6 => Submitted, 7 => Building, 8 => Activating } unary {  } });
wire_newtype!(DatabaseMarker, StateMarker);
wire_newtype!(AdmissionMarker, StateMarker);
wire_newtype!(SshDestination, String);
wire_newtype!(UserName, String);
wire_struct!(DeploymentRequestIdentity { deployment_environment: DeploymentEnvironment, cluster_name: ClusterName, node_name: NodeName, generation_artifact: GenerationArtifact, requested_deployment_action: RequestedDeploymentAction, activation_effect: ActivationEffect, source_revision_policy: SourceRevisionPolicy, optional_immutable_revision: Option<ImmutableRevision> });
wire_newtype!(TransitionMarker, StateMarker);
wire_newtype!(EventLogPosition, u64);
wire_newtype!(RejectedWatch, WatchRejectionReason);
wire_struct!(CacheRetentionTransitionEvent { generation_identifier: GenerationIdentifier, cluster_name: ClusterName, node_name: NodeName, cache_retention_transition: CacheRetentionTransition, generation_slot: GenerationSlot, optional_generation_slot: Option<GenerationSlot>, optional_pin_label: Option<PinLabel>, event_log_position: EventLogPosition });
wire_newtype!(NixBuilderSpec, String);
wire_enum!(DeploymentInputMode { unit { 0 => Horizon, 1 => Direct } unary {  } });
wire_enum!(TestRunPhase { unit { 0 => Submitted, 1 => BringingUp, 2 => TearingDown, 3 => Completed, 4 => Deploying, 5 => Asserting, 6 => Failed } unary {  } });
wire_struct!(DeploymentTransport {
    nix_store_uri: NixStoreUri,
    ssh_destination: SshDestination
});
wire_struct!(TestExecutionProfile { test_mode: TestMode, nix_system: NixSystem, deployment_output_selector: DeploymentOutputSelector, optional_deployment_transport: Option<DeploymentTransport> });
wire_struct!(SubscriptionOpened {
    subscription_token: SubscriptionToken,
    commit_sequence: CommitSequence
});
wire_enum!(WatchRejectionReason { unit { 0 => MalformedWatch, 1 => SubscriptionLimitReached, 2 => StreamUnavailable } unary {  } });
wire_enum!(ActivationBackend { unit { 0 => HomeManagerNixProfileV1, 1 => NixosSystemdBootV1 } unary {  } });
wire_enum!(GenerationArtifact { unit { 0 => BaseHost, 1 => CompleteHost, 2 => UserEnvironment } unary {  } });
wire_struct!(RejectedUnwatch {
    unwatch_rejection_reason: UnwatchRejectionReason,
    subscription_token: SubscriptionToken
});
wire_struct!(EventLogPage { deployment_phase_event_vector: Vec<DeploymentPhaseEvent>, cache_retention_transition_event_vector: Vec<CacheRetentionTransitionEvent>, state_marker: StateMarker });
wire_newtype!(SubscriptionClose, SubscriptionToken);
wire_newtype!(TerminalMarker, StateMarker);
wire_enum!(DeploymentEnvironment { unit { 0 => HostEnvironment } unary { 1 => UserEnvironment(UserName) } });
wire_newtype!(ImmutableRevision, String);
wire_enum!(HostSelection { unit { 1 => DefaultHost } unary { 0 => OnHost(NodeName) } });
wire_struct!(EventLogRange {
    from: EventLogPosition,
    until: EventLogPosition
});
wire_enum!(DeploymentLifecycle { unit { 0 => Failed, 1 => Rejected, 2 => Completed, 3 => Building, 4 => Activating, 5 => Submitted, 6 => Copying, 7 => Activated, 8 => Built } unary {  } });
// The public signal schema declares `DeploymentOutputSelector.{FlakeAttribute}`
// as a one-field product.  Keep the readable runtime noun compact, but retain
// that product boundary when crossing the verified wire contract.
impl WireShape for sema::DeploymentOutputSelector {
    fn to_wire(&self) -> RuntimeDatom {
        RuntimeDatom::Struct(vec![self.payload().to_wire()])
    }

    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
        let RuntimeDatom::Struct(mut fields) = value else {
            return Err(WireShapeError);
        };
        if fields.len() != 1 {
            return Err(WireShapeError);
        }
        let field = fields.pop().ok_or(WireShapeError)?;
        Ok(Self::new(sema::FlakeAttribute::from_wire(field)?))
    }
}
wire_newtype!(ClosurePath, String);
wire_enum!(ActivationEffect { unit { 0 => ProfileOnly, 1 => BootOnceProfile, 2 => TestActivation, 3 => LiveActivation, 4 => BootProfile } unary {  } });
wire_newtype!(SubscriptionClosed, SubscriptionToken);
wire_enum!(OrdinaryIngress { unit {  } unary { 0 => CheckHostKeyMaterial(KeyMaterialQuery), 1 => WatchDeployments(DeploymentWatch), 2 => Query(Selection), 3 => WatchCacheRetention(CacheRetentionWatch), 4 => Unwatch(SubscriptionClose) } });
wire_enum!(TestMode { unit { 0 => Hermetic, 1 => Live } unary {  } });
wire_newtype!(StateDigest, u64);
wire_enum!(QueryRejectionReason { unit { 0 => MalformedSelector, 1 => EventLogPositionOutOfRange, 2 => GenerationUnknown, 3 => NodeUnknown } unary {  } });
wire_newtype!(NixStoreUri, String);
wire_struct!(TestRunListing { test_run_record_vector: Vec<TestRunRecord>, database_marker: DatabaseMarker });
wire_enum!(DeploymentTerminal { unit { 2 => Succeeded } unary { 0 => Failed(DeploymentFailure), 1 => Rejected(DeploymentTerminalReason) } });
wire_enum!(SourceRevisionPolicy { unit { 0 => ResolveAndRecord, 1 => RequireImmutable } unary {  } });
wire_struct!(KeyMaterialQuery {
    cluster_name: ClusterName,
    node_name: NodeName,
    proposal_source: ProposalSource
});
wire_newtype!(ClusterName, String);
wire_newtype!(DeploymentIdentifier, u64);
wire_struct!(DeploymentFailure {
    deployment_failure_stage: DeploymentFailureStage,
    deployment_terminal_reason: DeploymentTerminalReason
});
wire_enum!(DeploymentFailureStage { unit { 0 => Build, 1 => Eval, 2 => MaterializeHorizon, 3 => Daemon, 4 => Activate, 5 => CopyClosure, 6 => Admission, 7 => FlakeAuth } unary {  } });
wire_struct!(RejectedKeyMaterialCheck {
    key_material_check_rejection_reason: KeyMaterialCheckRejectionReason,
    state_marker: StateMarker
});
wire_newtype!(FlakeReference, String);
wire_enum!(DeploymentTerminalReason { unit { 0 => NodeUnknown, 1 => FlakeReferenceMalformed, 2 => ProposalSourceUnreachable, 3 => DeploymentInFlight, 4 => InvalidDeploymentRouting, 5 => UnsupportedDeployAction, 6 => InternalError, 7 => ClusterUnknown, 8 => ActivationFailed, 9 => BuilderUnreachable, 10 => SubstituterUnreachable } unary {  } });
wire_enum!(Selection { unit {  } unary { 0 => ByNode(NodeSelector), 1 => ByTestRun(TestRunLookup), 2 => ByDeployment(DeploymentLookup), 3 => ByGeneration(GenerationLookup), 4 => ByEventLog(EventLogRange) } });
wire_struct!(UserEnvironmentDeployment { cluster_name: ClusterName, node_name: NodeName, user_name: UserName, proposal_source: ProposalSource, flake_reference: FlakeReference, deployment_transport: DeploymentTransport, deployment_input_mode: DeploymentInputMode, deployment_output_selector: DeploymentOutputSelector, activation_backend: ActivationBackend, user_environment_action: UserEnvironmentAction, source_revision_policy: SourceRevisionPolicy, optional_nix_builder_spec: Option<NixBuilderSpec>, extra_substituter_vector: Vec<ExtraSubstituter> });
wire_struct!(HostDeployment { cluster_name: ClusterName, node_name: NodeName, host_composition: HostComposition, proposal_source: ProposalSource, flake_reference: FlakeReference, deployment_transport: DeploymentTransport, deployment_input_mode: DeploymentInputMode, deployment_output_selector: DeploymentOutputSelector, activation_backend: ActivationBackend, host_deploy_action: HostDeployAction, source_revision_policy: SourceRevisionPolicy, optional_nix_builder_spec: Option<NixBuilderSpec>, extra_substituter_vector: Vec<ExtraSubstituter> });
wire_struct!(AppliedPin {
    generation_identifier: GenerationIdentifier,
    pin_label: PinLabel,
    from_slot: GenerationSlot,
    to_slot: GenerationSlot,
    state_marker: StateMarker
});
wire_enum!(PinRejectionReason { unit { 0 => PinSlotExhausted, 1 => InternalError, 2 => NodeUnknown, 3 => PinLabelInUse, 4 => GenerationUnknown } unary {  } });
wire_enum!(NodeSelection { unit { 0 => All } unary { 1 => Nodes(Vec<NodeName>) } });
wire_enum!(RetireRejectionReason { unit { 0 => NodeUnknown, 1 => GenerationUnknown, 2 => GenerationPinned, 3 => InternalError, 4 => GenerationActive } unary {  } });
wire_struct!(RejectedTest {
    test_rejection_reason: TestRejectionReason,
    state_marker: StateMarker
});
// `RejectedDeploy.{DeploymentRecord}` is a one-field product in the owner
// contract, even though the runtime keeps the record as a compact newtype.
impl WireShape for sema::RejectedDeploy {
    fn to_wire(&self) -> RuntimeDatom {
        RuntimeDatom::Struct(vec![self.payload().to_wire()])
    }

    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
        let RuntimeDatom::Struct(mut fields) = value else {
            return Err(WireShapeError);
        };
        if fields.len() != 1 {
            return Err(WireShapeError);
        }
        let field = fields.pop().ok_or(WireShapeError)?;
        Ok(Self::new(sema::DeploymentRecord::from_wire(field)?))
    }
}
wire_enum!(UnpinRejectionReason { unit { 0 => GenerationNotPinned, 1 => PinLabelUnknown, 2 => InternalError, 3 => NodeUnknown } unary {  } });
wire_struct!(PinRequest {
    cluster_name: ClusterName,
    node_name: NodeName,
    generation_identifier: GenerationIdentifier,
    pin_label: PinLabel
});
wire_struct!(RejectedUnpin {
    unpin_rejection_reason: UnpinRejectionReason,
    state_marker: StateMarker
});
wire_enum!(TestRequest { unit {  } unary { 0 => Run(TestRun), 1 => Check(QuickCheck) } });
wire_struct!(RejectedPin {
    pin_rejection_reason: PinRejectionReason,
    state_marker: StateMarker
});
wire_struct!(UnpinRequest {
    cluster_name: ClusterName,
    node_name: NodeName,
    pin_label: PinLabel
});
wire_struct!(ExtraSubstituter {
    url: String,
    public_key: String
});
wire_enum!(MetaEgress { unit {  } unary { 0 => PinRejected(RejectedPin), 1 => DeployRejected(RejectedDeploy), 2 => DeployAccepted(DeployHandle), 3 => TestRejected(RejectedTest), 4 => Unpinned(AppliedUnpin), 5 => Tested(AcceptedTest), 6 => UnpinRejected(RejectedUnpin), 7 => DeployTerminal(DeploymentRecord), 8 => Pinned(AppliedPin), 9 => RetireRejected(RejectedRetire), 10 => Retired(AppliedRetire) } });
wire_struct!(DeployHandle {
    deployment_identifier: DeploymentIdentifier,
    state_marker: StateMarker
});
wire_struct!(RetireRequest {
    cluster_name: ClusterName,
    node_name: NodeName,
    generation_identifier: GenerationIdentifier
});
wire_enum!(DeploySubmission { unit {  } unary { 0 => UserEnvironment(UserEnvironmentDeployment), 1 => Host(HostDeployment) } });
wire_newtype!(QuickCheck, Vec<NodeName>);
wire_struct!(AcceptedTest {
    test_run_identifier: TestRunIdentifier,
    state_marker: StateMarker
});
wire_struct!(AppliedUnpin {
    generation_identifier: GenerationIdentifier,
    pin_label: PinLabel,
    from_slot: GenerationSlot,
    to_slot: GenerationSlot,
    state_marker: StateMarker
});
wire_enum!(TestRejectionReason { unit { 0 => SubstrateUnavailable, 1 => NoTestDefaults, 2 => ClusterUnknown, 3 => HostDeclaresNoVmHost, 4 => LiveNotYetEnabled, 5 => NodeUnknown, 6 => VmHostNotDeclaredForNode, 7 => InternalError } unary {  } });
wire_struct!(RejectedRetire {
    retire_rejection_reason: RetireRejectionReason,
    state_marker: StateMarker
});
wire_enum!(MetaIngress { unit {  } unary { 0 => Retire(RetireRequest), 1 => Pin(PinRequest), 2 => Deploy(DeploySubmission), 3 => Test(TestRequest), 4 => Unpin(UnpinRequest) } });
wire_struct!(AppliedRetire {
    generation_identifier: GenerationIdentifier,
    generation_slot: GenerationSlot,
    state_marker: StateMarker
});
wire_struct!(TestRun {
    cluster_name: ClusterName,
    node_selection: NodeSelection,
    host_selection: HostSelection,
    test_execution_profile: TestExecutionProfile
});

wire_struct!(StateMarker {
    commit_sequence: sema::CommitSequence,
    state_digest: sema::StateDigest
});

wire_struct!(NodeSelector {
    cluster_name: ClusterName,
    node_name: NodeName,
    optional_generation_artifact: Option<GenerationArtifact>
});

impl WireShape for sema::Generation {
    fn to_wire(&self) -> RuntimeDatom {
        let closure = canonical_nix_store_root(self.closure_path.payload())
            .then(|| self.closure_path.clone())
            .to_wire();
        RuntimeDatom::Struct(vec![
            self.generation_identifier.to_wire(),
            self.deployment_identifier.to_wire(),
            self.cluster_name.to_wire(),
            self.node_name.to_wire(),
            self.generation_artifact.to_wire(),
            self.activation_effect.to_wire(),
            self.generation_slot.to_wire(),
            closure,
            self.optional_immutable_revision.to_wire(),
        ])
    }

    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
        let RuntimeDatom::Struct(fields) = value else {
            return Err(WireShapeError);
        };
        let mut fields = fields.into_iter();
        let generation_identifier =
            sema::GenerationIdentifier::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let deployment_identifier =
            sema::DeploymentIdentifier::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let cluster_name = sema::ClusterName::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let node_name = sema::NodeName::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let generation_artifact =
            sema::GenerationArtifact::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let activation_effect =
            sema::ActivationEffect::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let generation_slot =
            sema::GenerationSlot::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let closure_path =
            Option::<sema::ClosurePath>::from_wire(fields.next().ok_or(WireShapeError)?)?
                .ok_or(WireShapeError)?;
        let optional_immutable_revision =
            Option::<sema::ImmutableRevision>::from_wire(fields.next().ok_or(WireShapeError)?)?;
        if fields.next().is_some() {
            return Err(WireShapeError);
        }
        Ok(Self {
            generation_identifier,
            deployment_identifier,
            cluster_name,
            node_name,
            generation_artifact,
            activation_effect,
            generation_slot,
            closure_path,
            optional_immutable_revision,
        })
    }
}

impl WireShape for sema::TestRunRecord {
    fn to_wire(&self) -> RuntimeDatom {
        let closure = self
            .optional_closure_path
            .clone()
            .filter(|path| canonical_nix_store_root(path.payload()))
            .to_wire();
        RuntimeDatom::Struct(vec![
            self.test_run_identifier.to_wire(),
            self.cluster_name.to_wire(),
            self.node.to_wire(),
            self.host.to_wire(),
            self.test_mode.to_wire(),
            self.test_run_phase.to_wire(),
            self.test_outcome.to_wire(),
            closure,
        ])
    }

    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
        let RuntimeDatom::Struct(fields) = value else {
            return Err(WireShapeError);
        };
        let mut fields = fields.into_iter();
        let result = Self {
            test_run_identifier: sema::TestRunIdentifier::from_wire(
                fields.next().ok_or(WireShapeError)?,
            )?,
            cluster_name: sema::ClusterName::from_wire(fields.next().ok_or(WireShapeError)?)?,
            node: sema::NodeName::from_wire(fields.next().ok_or(WireShapeError)?)?,
            host: sema::NodeName::from_wire(fields.next().ok_or(WireShapeError)?)?,
            test_mode: sema::TestMode::from_wire(fields.next().ok_or(WireShapeError)?)?,
            test_run_phase: sema::TestRunPhase::from_wire(fields.next().ok_or(WireShapeError)?)?,
            test_outcome: sema::TestOutcome::from_wire(fields.next().ok_or(WireShapeError)?)?,
            optional_closure_path: Option::<sema::ClosurePath>::from_wire(
                fields.next().ok_or(WireShapeError)?,
            )?,
        };
        if fields.next().is_some() {
            return Err(WireShapeError);
        }
        Ok(result)
    }
}

impl WireShape for sema::KeyMaterialReport {
    fn to_wire(&self) -> RuntimeDatom {
        RuntimeDatom::Struct(vec![
            self.node_name.to_wire(),
            RuntimeDatom::Vector(Vec::new()),
            self.state_marker.to_wire(),
        ])
    }

    fn from_wire(value: RuntimeDatom) -> Result<Self, WireShapeError> {
        let RuntimeDatom::Struct(fields) = value else {
            return Err(WireShapeError);
        };
        let mut fields = fields.into_iter();
        let node_name = sema::NodeName::from_wire(fields.next().ok_or(WireShapeError)?)?;
        let RuntimeDatom::Vector(_) = fields.next().ok_or(WireShapeError)? else {
            return Err(WireShapeError);
        };
        let state_marker = sema::StateMarker::from_wire(fields.next().ok_or(WireShapeError)?)?;
        if fields.next().is_some() {
            return Err(WireShapeError);
        }
        Ok(Self {
            node_name,
            string_vector: Vec::new(),
            state_marker,
        })
    }
}

fn canonical_nix_store_root(value: &str) -> bool {
    let Some(item) = value.strip_prefix("/nix/store/") else {
        return false;
    };
    let Some((hash, name)) = item.split_once('-') else {
        return false;
    };
    hash.len() == 32
        && hash.bytes().all(|byte| {
            matches!(byte, b'0'..=b'9' | b'a'..=b'z') && !matches!(byte, b'e' | b'o' | b't' | b'u')
        })
        && !name.is_empty()
        && !name.contains("..")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'_' | b'-'))
        && !credential_like(value)
}

fn credential_like(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "credential",
        "apikey",
        "api-key",
        "api_key",
        "auth",
    ]
    .into_iter()
    .any(|term| value.contains(term))
}

pub fn ordinary_ingress(value: signal_lojix::Request) -> crate::Result<sema::OrdinaryIngress> {
    sema::OrdinaryIngress::from_wire(current_datom(&value))
        .map_err(|error| crate::Error::Wire(error.to_string()))
}

pub fn meta_ingress(value: meta_signal_lojix::Request) -> crate::Result<sema::MetaIngress> {
    sema::MetaIngress::from_wire(current_datom(&value))
        .map_err(|error| crate::Error::Wire(error.to_string()))
}

pub fn ordinary_egress(
    value: sema::OrdinaryEgress,
) -> crate::Result<signal_lojix::Response> {
    generated_root(value.to_wire())
}

pub fn meta_egress(
    value: sema::MetaEgress,
) -> crate::Result<meta_signal_lojix::Response> {
    generated_root(value.to_wire())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> sema::StateMarker {
        sema::StateMarker {
            commit_sequence: sema::CommitSequence::new(7),
            state_digest: sema::StateDigest::new(7),
        }
    }

    fn generation(path: &str) -> sema::Generation {
        sema::Generation {
            generation_identifier: sema::GenerationIdentifier::new(1),
            deployment_identifier: sema::DeploymentIdentifier::new(1),
            cluster_name: sema::ClusterName::new("alpha"),
            node_name: sema::NodeName::new("node-1"),
            generation_artifact: sema::GenerationArtifact::BaseHost,
            activation_effect: sema::ActivationEffect::LiveActivation,
            generation_slot: sema::GenerationSlot::Current,
            closure_path: sema::ClosurePath::new(path),
            optional_immutable_revision: None,
        }
    }

    #[test]
    fn ordinary_projection_keeps_only_canonical_store_item_roots() {
        let valid = "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system-toplevel";
        let visible = ordinary_egress(sema::OrdinaryEgress::Queried(sema::GenerationListing {
            generation_vector: vec![generation(valid)],
            deployment_record_vector: Vec::new(),
            state_marker: marker(),
        }))
        .expect("project canonical listing");
        assert!(format!("{visible:?}").contains(valid));

        for private in [
            "/home/li/private",
            "/nix/store/short-system",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-secret",
        ] {
            let visible = ordinary_egress(sema::OrdinaryEgress::Queried(sema::GenerationListing {
                generation_vector: vec![generation(private)],
                deployment_record_vector: Vec::new(),
                state_marker: marker(),
            }))
            .expect("project private listing");
            assert!(!format!("{visible:?}").contains(private));
        }
    }

    #[test]
    fn public_key_report_drops_private_runtime_text() {
        let private = "token=raw-secret path=/srv/private";
        let visible = ordinary_egress(sema::OrdinaryEgress::KeyMaterialChecked(
            sema::KeyMaterialReport {
                node_name: sema::NodeName::new("node-1"),
                string_vector: vec![private.to_owned()],
                state_marker: marker(),
            },
        ))
        .expect("project key report");
        assert!(!format!("{visible:?}").contains(private));
    }
}
