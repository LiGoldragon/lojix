//! Explicit boundary translation between public Signal contracts and the
//! daemon's private SEMA vocabulary.
//!
//! The engine, persistence layer, and generated Nexus runner use only
//! `schema::sema` values.  This module is deliberately the sole place where a
//! public contract type is lowered or raised; in particular it never exposes
//! a local `SourceRevisionRecord` on the wire.

use crate::schema::sema;

use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;

macro_rules! scalar {
    ($name:ident, $public:path, $local:path) => {
        fn $name(value: $public) -> $local {
            <$local>::new(value.into_payload())
        }
    };
}

scalar!(cluster_name, ordinary::ClusterName, sema::ClusterName);
scalar!(node_name, ordinary::NodeName, sema::NodeName);
scalar!(user_name, ordinary::UserName, sema::UserName);
scalar!(pin_label, ordinary::PinLabel, sema::PinLabel);
scalar!(
    flake_reference,
    ordinary::FlakeReference,
    sema::FlakeReference
);
scalar!(
    proposal_source,
    ordinary::ProposalSource,
    sema::ProposalSource
);
scalar!(
    deployment_identifier,
    ordinary::DeploymentIdentifier,
    sema::DeploymentIdentifier
);
scalar!(
    generation_identifier,
    ordinary::GenerationIdentifier,
    sema::GenerationIdentifier
);
scalar!(
    test_run_identifier,
    ordinary::TestRunIdentifier,
    sema::TestRunIdentifier
);
scalar!(
    subscription_token,
    ordinary::SubscriptionToken,
    sema::SubscriptionToken
);
scalar!(
    event_log_position,
    ordinary::EventLogPosition,
    sema::EventLogPosition
);

fn requested_generation_artifact(
    value: ordinary::RequestedGenerationArtifact,
) -> sema::GenerationArtifact {
    match value {
        ordinary::RequestedGenerationArtifact::CompleteHost => {
            sema::GenerationArtifact::CompleteHost
        }
        ordinary::RequestedGenerationArtifact::BaseHost => sema::GenerationArtifact::BaseHost,
        ordinary::RequestedGenerationArtifact::UserEnvironment => {
            sema::GenerationArtifact::UserEnvironment
        }
    }
}

fn host_composition(value: ordinary::HostComposition) -> sema::HostComposition {
    match value {
        ordinary::HostComposition::CompleteHost => sema::HostComposition::CompleteHost,
        ordinary::HostComposition::BaseHost => sema::HostComposition::BaseHost,
    }
}

fn host_deploy_action(value: ordinary::HostDeployAction) -> sema::HostDeployAction {
    match value {
        ordinary::HostDeployAction::Evaluate => sema::HostDeployAction::Evaluate,
        ordinary::HostDeployAction::Realize => sema::HostDeployAction::Realize,
        ordinary::HostDeployAction::SetBootProfile => sema::HostDeployAction::SetBootProfile,
        ordinary::HostDeployAction::ActivateNow => sema::HostDeployAction::ActivateNow,
        ordinary::HostDeployAction::TestActivation => sema::HostDeployAction::TestActivation,
        ordinary::HostDeployAction::ScheduleBootOnce => sema::HostDeployAction::ScheduleBootOnce,
    }
}

fn user_environment_action(value: ordinary::UserEnvironmentAction) -> sema::UserEnvironmentAction {
    match value {
        ordinary::UserEnvironmentAction::Realize => sema::UserEnvironmentAction::Realize,
        ordinary::UserEnvironmentAction::SetProfile => sema::UserEnvironmentAction::SetProfile,
        ordinary::UserEnvironmentAction::ActivateNow => sema::UserEnvironmentAction::ActivateNow,
    }
}

fn source_revision_policy(value: ordinary::SourceRevisionPolicy) -> sema::SourceRevisionPolicy {
    match value {
        ordinary::SourceRevisionPolicy::RequireImmutable => {
            sema::SourceRevisionPolicy::RequireImmutable
        }
        ordinary::SourceRevisionPolicy::ResolveAndRecord => {
            sema::SourceRevisionPolicy::ResolveAndRecord
        }
    }
}

fn test_mode(value: ordinary::TestMode) -> sema::TestMode {
    match value {
        ordinary::TestMode::Hermetic => sema::TestMode::Hermetic,
        ordinary::TestMode::Live => sema::TestMode::Live,
    }
}

fn host_selection(value: ordinary::HostSelection) -> sema::HostSelection {
    match value {
        ordinary::HostSelection::DefaultHost => sema::HostSelection::DefaultHost,
        ordinary::HostSelection::OnHost(node) => sema::HostSelection::OnHost(node_name(node)),
    }
}

fn selection(value: ordinary::Selection) -> sema::Selection {
    match value {
        ordinary::Selection::ByNode(selector) => sema::Selection::ByNode(sema::NodeSelector {
            cluster_name: cluster_name(selector.cluster_name),
            node_name: node_name(selector.node_name),
            optional_generation_artifact: selector
                .optional_requested_generation_artifact
                .map(requested_generation_artifact),
        }),
        ordinary::Selection::ByGeneration(lookup) => sema::Selection::ByGeneration(
            sema::GenerationLookup::new(generation_identifier(lookup.into_payload())),
        ),
        ordinary::Selection::ByDeployment(lookup) => sema::Selection::ByDeployment(
            sema::DeploymentLookup::new(deployment_identifier(lookup.into_payload())),
        ),
        ordinary::Selection::ByEventLog(range) => {
            sema::Selection::ByEventLog(sema::EventLogRange {
                from: event_log_position(range.from),
                until: event_log_position(range.until),
            })
        }
        ordinary::Selection::ByTestRun(lookup) => sema::Selection::ByTestRun(sema::TestRunLookup {
            cluster_name: cluster_name(lookup.cluster_name),
            node_name: node_name(lookup.node_name),
            optional_test_run_identifier: lookup
                .optional_test_run_identifier
                .map(test_run_identifier),
        }),
    }
}

fn deployment_watch(value: ordinary::DeploymentWatch) -> sema::DeploymentWatch {
    sema::DeploymentWatch {
        optional_deployment_identifier: value
            .optional_deployment_identifier
            .map(deployment_identifier),
        optional_cluster_name: value.optional_cluster_name.map(cluster_name),
        optional_node_name: value.optional_node_name.map(node_name),
    }
}

fn cache_retention_watch(value: ordinary::CacheRetentionWatch) -> sema::CacheRetentionWatch {
    sema::CacheRetentionWatch {
        optional_cluster_name: value.optional_cluster_name.map(cluster_name),
        optional_node_name: value.optional_node_name.map(node_name),
    }
}

fn subscription_close(value: ordinary::SubscriptionClose) -> sema::SubscriptionClose {
    sema::SubscriptionClose::new(subscription_token(value.into_payload()))
}

fn key_material_query(value: ordinary::KeyMaterialQuery) -> sema::KeyMaterialQuery {
    sema::KeyMaterialQuery {
        cluster_name: cluster_name(value.cluster_name),
        node_name: node_name(value.node_name),
        proposal_source: proposal_source(value.proposal_source),
    }
}

/// Lower an unprivileged request before it enters the local engine.
pub fn ordinary_ingress(value: ordinary::Input) -> sema::OrdinaryIngress {
    match value {
        ordinary::Input::Query(payload) => {
            sema::OrdinaryIngress::Query(selection(payload.into_payload()))
        }
        ordinary::Input::WatchDeployments(payload) => {
            sema::OrdinaryIngress::WatchDeployments(deployment_watch(payload.into_payload()))
        }
        ordinary::Input::WatchCacheRetention(payload) => {
            sema::OrdinaryIngress::WatchCacheRetention(cache_retention_watch(
                payload.into_payload(),
            ))
        }
        ordinary::Input::Unwatch(payload) => {
            sema::OrdinaryIngress::Unwatch(subscription_close(payload.into_payload()))
        }
        ordinary::Input::CheckHostKeyMaterial(payload) => {
            sema::OrdinaryIngress::CheckHostKeyMaterial(key_material_query(payload.into_payload()))
        }
    }
}

fn node_selection(value: meta::NodeSelection) -> sema::NodeSelection {
    match value {
        meta::NodeSelection::Nodes(nodes) => {
            sema::NodeSelection::Nodes(nodes.into_iter().map(node_name).collect())
        }
        meta::NodeSelection::All => sema::NodeSelection::All,
    }
}

fn test_request(value: meta::TestRequest) -> sema::TestRequest {
    match value {
        meta::TestRequest::Run(run) => sema::TestRequest::Run(sema::TestRun {
            cluster_name: cluster_name(run.cluster_name),
            node_selection: node_selection(run.node_selection),
            host_selection: host_selection(run.host_selection),
            test_mode: test_mode(run.test_mode),
        }),
        meta::TestRequest::Check(check) => sema::TestRequest::Check(sema::QuickCheck::new(
            check.into_payload().into_iter().map(node_name).collect(),
        )),
    }
}

fn builder(value: meta::Builder) -> sema::Builder {
    sema::Builder::new(node_name(value.into_payload()))
}

fn flake_attribute(value: meta::FlakeAttribute) -> sema::FlakeAttribute {
    sema::FlakeAttribute::new(value.into_payload())
}

fn extra_substituter(value: meta::ExtraSubstituter) -> sema::ExtraSubstituter {
    sema::ExtraSubstituter {
        url: value.url,
        public_key: value.public_key,
    }
}

fn deploy_request(value: meta::DeployRequest) -> sema::DeploySubmission {
    match value {
        meta::DeployRequest::Host(deployment) => {
            sema::DeploySubmission::Host(sema::HostDeployment {
                cluster_name: cluster_name(deployment.cluster_name),
                node_name: node_name(deployment.node_name),
                host_composition: host_composition(deployment.host_composition),
                proposal_source: proposal_source(deployment.proposal_source),
                flake_reference: flake_reference(deployment.flake_reference),
                host_deploy_action: host_deploy_action(deployment.host_deploy_action),
                source_revision_policy: source_revision_policy(deployment.source_revision_policy),
                optional_builder: deployment.optional_builder.map(builder),
                extra_substituter_vector: deployment
                    .extra_substituter_vector
                    .into_iter()
                    .map(extra_substituter)
                    .collect(),
                optional_flake_attribute: deployment.optional_flake_attribute.map(flake_attribute),
            })
        }
        meta::DeployRequest::UserEnvironment(deployment) => {
            sema::DeploySubmission::UserEnvironment(sema::UserEnvironmentDeployment {
                cluster_name: cluster_name(deployment.cluster_name),
                node_name: node_name(deployment.node_name),
                user_name: user_name(deployment.user_name),
                proposal_source: proposal_source(deployment.proposal_source),
                flake_reference: flake_reference(deployment.flake_reference),
                user_environment_action: user_environment_action(
                    deployment.user_environment_action,
                ),
                source_revision_policy: source_revision_policy(deployment.source_revision_policy),
                optional_builder: deployment.optional_builder.map(builder),
                extra_substituter_vector: deployment
                    .extra_substituter_vector
                    .into_iter()
                    .map(extra_substituter)
                    .collect(),
            })
        }
    }
}

/// Lower a privileged request before it enters the local engine.
pub fn meta_ingress(value: meta::Input) -> sema::MetaIngress {
    match value {
        meta::Input::Deploy(payload) => {
            sema::MetaIngress::Deploy(deploy_request(payload.into_payload()))
        }
        meta::Input::Pin(payload) => {
            let request = payload.into_payload();
            sema::MetaIngress::Pin(sema::PinRequest {
                cluster_name: cluster_name(request.cluster_name),
                node_name: node_name(request.node_name),
                generation_identifier: generation_identifier(request.generation_identifier),
                pin_label: pin_label(request.pin_label),
            })
        }
        meta::Input::Unpin(payload) => {
            let request = payload.into_payload();
            sema::MetaIngress::Unpin(sema::UnpinRequest {
                cluster_name: cluster_name(request.cluster_name),
                node_name: node_name(request.node_name),
                pin_label: pin_label(request.pin_label),
            })
        }
        meta::Input::Retire(payload) => {
            let request = payload.into_payload();
            sema::MetaIngress::Retire(sema::RetireRequest {
                cluster_name: cluster_name(request.cluster_name),
                node_name: node_name(request.node_name),
                generation_identifier: generation_identifier(request.generation_identifier),
            })
        }
        meta::Input::Test(payload) => sema::MetaIngress::Test(test_request(payload.into_payload())),
    }
}

macro_rules! outward_scalar {
    ($name:ident, $local:path, $public:path) => {
        fn $name(value: $local) -> $public {
            <$public>::new(value.into_payload())
        }
    };
}

outward_scalar!(
    public_cluster_name,
    sema::ClusterName,
    ordinary::ClusterName
);
outward_scalar!(public_node_name, sema::NodeName, ordinary::NodeName);
outward_scalar!(public_user_name, sema::UserName, ordinary::UserName);
outward_scalar!(public_pin_label, sema::PinLabel, ordinary::PinLabel);
outward_scalar!(
    public_immutable_revision,
    sema::ImmutableRevision,
    ordinary::ImmutableRevision
);
outward_scalar!(
    public_deployment_identifier,
    sema::DeploymentIdentifier,
    ordinary::DeploymentIdentifier
);
outward_scalar!(
    public_generation_identifier,
    sema::GenerationIdentifier,
    ordinary::GenerationIdentifier
);
outward_scalar!(
    public_test_run_identifier,
    sema::TestRunIdentifier,
    ordinary::TestRunIdentifier
);
outward_scalar!(
    public_subscription_token,
    sema::SubscriptionToken,
    ordinary::SubscriptionToken
);
outward_scalar!(
    public_event_log_position,
    sema::EventLogPosition,
    ordinary::EventLogPosition
);

fn public_marker(value: sema::StateMarker) -> ordinary::DatabaseMarker {
    ordinary::DatabaseMarker {
        commit_sequence: ordinary::CommitSequence::new(value.commit_sequence.into_payload()),
        state_digest: ordinary::StateDigest::new(value.state_digest.into_payload()),
    }
}

/// Ordinary egress is a privacy boundary: only a canonical immutable Nix
/// store-item root can leave it. Local persistence may retain other strings
/// for diagnostics/migration, but they never become a public closure path.
fn public_closure_path(value: sema::ClosurePath) -> Option<ordinary::ClosurePath> {
    canonical_nix_store_root(value.payload())
        .then(|| ordinary::ClosurePath::new(value.into_payload()))
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

fn public_generation_artifact(
    value: sema::GenerationArtifact,
) -> crate::Result<ordinary::GenerationArtifact> {
    match value {
        sema::GenerationArtifact::CompleteHost => Ok(ordinary::GenerationArtifact::CompleteHost),
        sema::GenerationArtifact::BaseHost => Ok(ordinary::GenerationArtifact::BaseHost),
        sema::GenerationArtifact::UserEnvironment => {
            Ok(ordinary::GenerationArtifact::UserEnvironment)
        }
        sema::GenerationArtifact::LegacyUnknown => {
            Ok(ordinary::GenerationArtifact::LegacyUnknownArtifact)
        }
    }
}

fn public_activation_effect(
    value: sema::ActivationEffect,
) -> crate::Result<ordinary::ActivationEffect> {
    match value {
        sema::ActivationEffect::LiveActivation => Ok(ordinary::ActivationEffect::LiveActivation),
        sema::ActivationEffect::BootProfile => Ok(ordinary::ActivationEffect::BootProfile),
        sema::ActivationEffect::TestActivation => Ok(ordinary::ActivationEffect::TestActivation),
        sema::ActivationEffect::BootOnceProfile => Ok(ordinary::ActivationEffect::BootOnceProfile),
        sema::ActivationEffect::ProfileOnly => Ok(ordinary::ActivationEffect::ProfileOnly),
        sema::ActivationEffect::LegacyUnknown => {
            Ok(ordinary::ActivationEffect::LegacyUnknownActivationEffect)
        }
    }
}

fn public_generation_slot(value: sema::GenerationSlot) -> crate::Result<ordinary::GenerationSlot> {
    match value {
        sema::GenerationSlot::Current => Ok(ordinary::GenerationSlot::Current),
        sema::GenerationSlot::BootPending => Ok(ordinary::GenerationSlot::BootPending),
        sema::GenerationSlot::Rollback => Ok(ordinary::GenerationSlot::Rollback),
        sema::GenerationSlot::Pinned => Ok(ordinary::GenerationSlot::Pinned),
        sema::GenerationSlot::Recent => Ok(ordinary::GenerationSlot::Recent),
        // Migration-only slots are deliberately projected as non-current
        // history. They never grant a legacy row ownership of the live slot.
        sema::GenerationSlot::LegacyUnknown | sema::GenerationSlot::LegacyAmbiguous => {
            Ok(ordinary::GenerationSlot::Recent)
        }
    }
}

fn public_test_mode(value: sema::TestMode) -> ordinary::TestMode {
    match value {
        sema::TestMode::Hermetic => ordinary::TestMode::Hermetic,
        sema::TestMode::Live => ordinary::TestMode::Live,
    }
}

fn public_test_run_phase(value: sema::TestRunPhase) -> ordinary::TestRunPhase {
    match value {
        sema::TestRunPhase::Submitted => ordinary::TestRunPhase::Submitted,
        sema::TestRunPhase::BringingUp => ordinary::TestRunPhase::BringingUp,
        sema::TestRunPhase::Deploying => ordinary::TestRunPhase::Deploying,
        sema::TestRunPhase::Asserting => ordinary::TestRunPhase::Asserting,
        sema::TestRunPhase::TearingDown => ordinary::TestRunPhase::TearingDown,
        sema::TestRunPhase::Completed => ordinary::TestRunPhase::Completed,
        sema::TestRunPhase::Failed => ordinary::TestRunPhase::Failed,
    }
}

fn public_failure_stage(value: sema::FailureStage) -> ordinary::FailureStage {
    match value {
        sema::FailureStage::BringUp => ordinary::FailureStage::BringUp,
        sema::FailureStage::Deploy => ordinary::FailureStage::Deploy,
        sema::FailureStage::Assert => ordinary::FailureStage::Assert,
        sema::FailureStage::TearDown => ordinary::FailureStage::TearDown,
        sema::FailureStage::HermeticCheck => ordinary::FailureStage::HermeticCheck,
    }
}

fn public_test_outcome(value: sema::TestOutcome) -> ordinary::TestOutcome {
    match value {
        sema::TestOutcome::Pending => ordinary::TestOutcome::Pending,
        sema::TestOutcome::Passed => ordinary::TestOutcome::Passed,
        sema::TestOutcome::Failed(stage) => {
            ordinary::TestOutcome::Failed(public_failure_stage(stage))
        }
    }
}

fn public_deployment_phase(value: sema::DeploymentPhase) -> ordinary::DeploymentPhase {
    match value {
        sema::DeploymentPhase::Submitted => ordinary::DeploymentPhase::Submitted,
        sema::DeploymentPhase::Building => ordinary::DeploymentPhase::Building,
        sema::DeploymentPhase::Built => ordinary::DeploymentPhase::Built,
        sema::DeploymentPhase::Copying => ordinary::DeploymentPhase::Copying,
        sema::DeploymentPhase::Activating => ordinary::DeploymentPhase::Activating,
        sema::DeploymentPhase::Activated => ordinary::DeploymentPhase::Activated,
        sema::DeploymentPhase::Completed => ordinary::DeploymentPhase::Completed,
        sema::DeploymentPhase::Rejected => ordinary::DeploymentPhase::Rejected,
        sema::DeploymentPhase::Failed => ordinary::DeploymentPhase::Failed,
    }
}

fn public_cache_transition(
    value: sema::CacheRetentionTransition,
) -> ordinary::CacheRetentionTransition {
    match value {
        sema::CacheRetentionTransition::Pinned => ordinary::CacheRetentionTransition::Pinned,
        sema::CacheRetentionTransition::Unpinned => ordinary::CacheRetentionTransition::Unpinned,
        sema::CacheRetentionTransition::Promoted => ordinary::CacheRetentionTransition::Promoted,
        sema::CacheRetentionTransition::Demoted => ordinary::CacheRetentionTransition::Demoted,
        sema::CacheRetentionTransition::Retired => ordinary::CacheRetentionTransition::Retired,
        sema::CacheRetentionTransition::Evicted => ordinary::CacheRetentionTransition::Evicted,
    }
}

fn public_generation(value: sema::Generation) -> crate::Result<ordinary::Generation> {
    Ok(ordinary::Generation {
        generation_identifier: public_generation_identifier(value.generation_identifier),
        deployment_identifier: public_deployment_identifier(value.deployment_identifier),
        cluster_name: public_cluster_name(value.cluster_name),
        node_name: public_node_name(value.node_name),
        generation_artifact: public_generation_artifact(value.generation_artifact)?,
        activation_effect: public_activation_effect(value.activation_effect)?,
        generation_slot: public_generation_slot(value.generation_slot)?,
        optional_closure_path: public_closure_path(value.closure_path),
        optional_immutable_revision: value
            .optional_immutable_revision
            .map(public_immutable_revision),
    })
}

fn public_test_run_record(value: sema::TestRunRecord) -> ordinary::TestRunRecord {
    ordinary::TestRunRecord {
        test_run_identifier: public_test_run_identifier(value.test_run_identifier),
        cluster_name: public_cluster_name(value.cluster_name),
        node: public_node_name(value.node),
        host: public_node_name(value.host),
        test_mode: public_test_mode(value.test_mode),
        test_run_phase: public_test_run_phase(value.test_run_phase),
        test_outcome: public_test_outcome(value.test_outcome),
        optional_closure_path: value.optional_closure_path.and_then(public_closure_path),
    }
}

fn public_deployment_environment(
    value: sema::DeploymentEnvironment,
) -> crate::Result<ordinary::DeploymentEnvironment> {
    match value {
        sema::DeploymentEnvironment::HostEnvironment => {
            Ok(ordinary::DeploymentEnvironment::HostEnvironment)
        }
        sema::DeploymentEnvironment::UserEnvironment(user) => Ok(
            ordinary::DeploymentEnvironment::UserEnvironment(public_user_name(user)),
        ),
        sema::DeploymentEnvironment::LegacyUnknownEnvironment => {
            Ok(ordinary::DeploymentEnvironment::LegacyUnknownEnvironment)
        }
    }
}

fn public_host_action(value: sema::HostDeployAction) -> ordinary::HostDeployAction {
    match value {
        sema::HostDeployAction::Evaluate => ordinary::HostDeployAction::Evaluate,
        sema::HostDeployAction::Realize => ordinary::HostDeployAction::Realize,
        sema::HostDeployAction::SetBootProfile => ordinary::HostDeployAction::SetBootProfile,
        sema::HostDeployAction::ActivateNow => ordinary::HostDeployAction::ActivateNow,
        sema::HostDeployAction::TestActivation => ordinary::HostDeployAction::TestActivation,
        sema::HostDeployAction::ScheduleBootOnce => ordinary::HostDeployAction::ScheduleBootOnce,
    }
}

fn public_user_action(value: sema::UserEnvironmentAction) -> ordinary::UserEnvironmentAction {
    match value {
        sema::UserEnvironmentAction::Realize => ordinary::UserEnvironmentAction::Realize,
        sema::UserEnvironmentAction::SetProfile => ordinary::UserEnvironmentAction::SetProfile,
        sema::UserEnvironmentAction::ActivateNow => ordinary::UserEnvironmentAction::ActivateNow,
    }
}

fn public_requested_action(
    value: sema::RequestedDeploymentAction,
) -> crate::Result<ordinary::RequestedDeploymentAction> {
    match value {
        sema::RequestedDeploymentAction::Host(action) => Ok(
            ordinary::RequestedDeploymentAction::Host(public_host_action(action)),
        ),
        sema::RequestedDeploymentAction::UserEnvironment(action) => Ok(
            ordinary::RequestedDeploymentAction::UserEnvironment(public_user_action(action)),
        ),
        sema::RequestedDeploymentAction::LegacyUnknownAction => {
            Ok(ordinary::RequestedDeploymentAction::LegacyUnknownAction)
        }
    }
}

fn public_source_policy(value: sema::SourceRevisionPolicy) -> ordinary::SourceRevisionPolicy {
    match value {
        sema::SourceRevisionPolicy::RequireImmutable => {
            ordinary::SourceRevisionPolicy::RequireImmutable
        }
        sema::SourceRevisionPolicy::ResolveAndRecord => {
            ordinary::SourceRevisionPolicy::ResolveAndRecord
        }
    }
}

fn public_deployment_lifecycle(
    value: sema::DeploymentLifecycle,
) -> crate::Result<ordinary::DeploymentLifecycle> {
    match value {
        sema::DeploymentLifecycle::Submitted => Ok(ordinary::DeploymentLifecycle::Submitted),
        sema::DeploymentLifecycle::Building => Ok(ordinary::DeploymentLifecycle::Building),
        sema::DeploymentLifecycle::Built => Ok(ordinary::DeploymentLifecycle::Built),
        sema::DeploymentLifecycle::Copying => Ok(ordinary::DeploymentLifecycle::Copying),
        sema::DeploymentLifecycle::Activating => Ok(ordinary::DeploymentLifecycle::Activating),
        sema::DeploymentLifecycle::Activated => Ok(ordinary::DeploymentLifecycle::Activated),
        sema::DeploymentLifecycle::Completed => Ok(ordinary::DeploymentLifecycle::Completed),
        sema::DeploymentLifecycle::Rejected => Ok(ordinary::DeploymentLifecycle::Rejected),
        sema::DeploymentLifecycle::Failed => Ok(ordinary::DeploymentLifecycle::Failed),
        sema::DeploymentLifecycle::LegacyUnknown => {
            Ok(ordinary::DeploymentLifecycle::LegacyUnknown)
        }
        sema::DeploymentLifecycle::LegacyAmbiguous => {
            Ok(ordinary::DeploymentLifecycle::LegacyAmbiguous)
        }
    }
}

fn public_terminal_reason(
    value: sema::DeploymentTerminalReason,
) -> ordinary::DeploymentTerminalReason {
    match value {
        sema::DeploymentTerminalReason::ClusterUnknown => {
            ordinary::DeploymentTerminalReason::ClusterUnknown
        }
        sema::DeploymentTerminalReason::NodeUnknown => {
            ordinary::DeploymentTerminalReason::NodeUnknown
        }
        sema::DeploymentTerminalReason::ProposalSourceUnreachable => {
            ordinary::DeploymentTerminalReason::ProposalSourceUnreachable
        }
        sema::DeploymentTerminalReason::FlakeReferenceMalformed => {
            ordinary::DeploymentTerminalReason::FlakeReferenceMalformed
        }
        sema::DeploymentTerminalReason::BuilderUnreachable => {
            ordinary::DeploymentTerminalReason::BuilderUnreachable
        }
        sema::DeploymentTerminalReason::SubstituterUnreachable => {
            ordinary::DeploymentTerminalReason::SubstituterUnreachable
        }
        sema::DeploymentTerminalReason::DeploymentInFlight => {
            ordinary::DeploymentTerminalReason::DeploymentInFlight
        }
        sema::DeploymentTerminalReason::UnsupportedDeployAction => {
            ordinary::DeploymentTerminalReason::UnsupportedDeployAction
        }
        sema::DeploymentTerminalReason::InternalError => {
            ordinary::DeploymentTerminalReason::InternalError
        }
        sema::DeploymentTerminalReason::ActivationFailed => {
            ordinary::DeploymentTerminalReason::ActivationFailed
        }
    }
}

fn public_deployment_failure_stage(
    value: sema::DeploymentFailureStage,
) -> ordinary::DeploymentFailureStage {
    match value {
        sema::DeploymentFailureStage::Admission => ordinary::DeploymentFailureStage::Admission,
        sema::DeploymentFailureStage::FlakeAuth => ordinary::DeploymentFailureStage::FlakeAuth,
        sema::DeploymentFailureStage::MaterializeHorizon => {
            ordinary::DeploymentFailureStage::MaterializeHorizon
        }
        sema::DeploymentFailureStage::Eval => ordinary::DeploymentFailureStage::Eval,
        sema::DeploymentFailureStage::Build => ordinary::DeploymentFailureStage::Build,
        sema::DeploymentFailureStage::CopyClosure => ordinary::DeploymentFailureStage::CopyClosure,
        sema::DeploymentFailureStage::Activate => ordinary::DeploymentFailureStage::Activate,
        sema::DeploymentFailureStage::Daemon => ordinary::DeploymentFailureStage::Daemon,
    }
}

fn public_terminal(value: sema::DeploymentTerminal) -> crate::Result<ordinary::DeploymentTerminal> {
    match value {
        sema::DeploymentTerminal::Succeeded => Ok(ordinary::DeploymentTerminal::Succeeded),
        sema::DeploymentTerminal::Rejected(reason) => Ok(ordinary::DeploymentTerminal::Rejected(
            public_terminal_reason(reason),
        )),
        sema::DeploymentTerminal::Failed(failure) => Ok(ordinary::DeploymentTerminal::Failed(
            ordinary::DeploymentFailure {
                deployment_failure_stage: public_deployment_failure_stage(
                    failure.deployment_failure_stage,
                ),
                deployment_terminal_reason: public_terminal_reason(
                    failure.deployment_terminal_reason,
                ),
            },
        )),
        sema::DeploymentTerminal::LegacyUnknown => Ok(ordinary::DeploymentTerminal::LegacyUnknown),
    }
}

fn public_deployment_record(
    value: sema::DeploymentRecord,
) -> crate::Result<ordinary::DeploymentRecord> {
    let identity = value.deployment_request_identity;
    Ok(ordinary::DeploymentRecord {
        deployment_identifier: public_deployment_identifier(value.deployment_identifier),
        generation_identifier: public_generation_identifier(value.generation_identifier),
        deployment_request_identity: ordinary::DeploymentRequestIdentity {
            deployment_environment: public_deployment_environment(identity.deployment_environment)?,
            cluster_name: public_cluster_name(identity.cluster_name),
            node_name: public_node_name(identity.node_name),
            generation_artifact: public_generation_artifact(identity.generation_artifact)?,
            requested_deployment_action: public_requested_action(
                identity.requested_deployment_action,
            )?,
            activation_effect: public_activation_effect(identity.activation_effect)?,
            source_revision_policy: public_source_policy(identity.source_revision_policy),
            optional_immutable_revision: identity
                .optional_immutable_revision
                .map(public_immutable_revision),
        },
        optional_admission_marker: value
            .optional_admission_marker
            .map(|marker| ordinary::AdmissionMarker::new(public_marker(marker.into_payload()))),
        deployment_lifecycle: public_deployment_lifecycle(value.deployment_lifecycle)?,
        optional_terminal_marker: value
            .optional_terminal_marker
            .map(|marker| ordinary::TerminalMarker::new(public_marker(marker.into_payload()))),
        optional_deployment_terminal: value
            .optional_deployment_terminal
            .map(public_terminal)
            .transpose()?,
    })
}

fn public_query_reason(value: sema::QueryRejectionReason) -> ordinary::QueryRejectionReason {
    match value {
        sema::QueryRejectionReason::GenerationUnknown => {
            ordinary::QueryRejectionReason::GenerationUnknown
        }
        sema::QueryRejectionReason::NodeUnknown => ordinary::QueryRejectionReason::NodeUnknown,
        sema::QueryRejectionReason::EventLogPositionOutOfRange => {
            ordinary::QueryRejectionReason::EventLogPositionOutOfRange
        }
        sema::QueryRejectionReason::MalformedSelector => {
            ordinary::QueryRejectionReason::MalformedSelector
        }
    }
}

fn public_watch_reason(value: sema::WatchRejectionReason) -> ordinary::WatchRejectionReason {
    match value {
        sema::WatchRejectionReason::SubscriptionLimitReached => {
            ordinary::WatchRejectionReason::SubscriptionLimitReached
        }
        sema::WatchRejectionReason::MalformedWatch => {
            ordinary::WatchRejectionReason::MalformedWatch
        }
        sema::WatchRejectionReason::StreamUnavailable => {
            ordinary::WatchRejectionReason::StreamUnavailable
        }
    }
}

fn public_unwatch_reason(value: sema::UnwatchRejectionReason) -> ordinary::UnwatchRejectionReason {
    match value {
        sema::UnwatchRejectionReason::SubscriptionTokenUnknown => {
            ordinary::UnwatchRejectionReason::SubscriptionTokenUnknown
        }
        sema::UnwatchRejectionReason::SubscriptionAlreadyClosed => {
            ordinary::UnwatchRejectionReason::SubscriptionAlreadyClosed
        }
    }
}

fn public_key_reason(
    value: sema::KeyMaterialCheckRejectionReason,
) -> ordinary::KeyMaterialCheckRejectionReason {
    match value {
        sema::KeyMaterialCheckRejectionReason::NodeUnknown => {
            ordinary::KeyMaterialCheckRejectionReason::NodeUnknown
        }
        sema::KeyMaterialCheckRejectionReason::ProposalSourceUnreachable => {
            ordinary::KeyMaterialCheckRejectionReason::ProposalSourceUnreachable
        }
        sema::KeyMaterialCheckRejectionReason::HostUnreachable => {
            ordinary::KeyMaterialCheckRejectionReason::HostUnreachable
        }
        sema::KeyMaterialCheckRejectionReason::PublicationMalformed => {
            ordinary::KeyMaterialCheckRejectionReason::PublicationMalformed
        }
    }
}

fn public_phase_event(
    value: sema::DeploymentPhaseEvent,
) -> crate::Result<ordinary::DeploymentPhaseEvent> {
    Ok(ordinary::DeploymentPhaseEvent {
        deployment_identifier: public_deployment_identifier(value.deployment_identifier),
        generation_identifier: public_generation_identifier(value.generation_identifier),
        cluster_name: public_cluster_name(value.cluster_name),
        node_name: public_node_name(value.node_name),
        deployment_phase: public_deployment_phase(value.deployment_phase),
        event_log_position: public_event_log_position(value.event_log_position),
        transition_marker: ordinary::TransitionMarker::new(public_marker(value.state_marker)),
        optional_immutable_revision: value
            .optional_immutable_revision
            .map(public_immutable_revision),
        optional_deployment_terminal: value
            .optional_deployment_terminal
            .map(public_terminal)
            .transpose()?,
    })
}

fn public_retention_event(
    value: sema::CacheRetentionTransitionEvent,
) -> crate::Result<ordinary::CacheRetentionTransitionEvent> {
    Ok(ordinary::CacheRetentionTransitionEvent {
        generation_identifier: public_generation_identifier(value.generation_identifier),
        cluster_name: public_cluster_name(value.cluster_name),
        node_name: public_node_name(value.node_name),
        cache_retention_transition: public_cache_transition(value.cache_retention_transition),
        generation_slot: public_generation_slot(value.generation_slot)?,
        optional_generation_slot: value
            .optional_generation_slot
            .map(public_generation_slot)
            .transpose()?,
        optional_pin_label: value.optional_pin_label.map(public_pin_label),
        event_log_position: public_event_log_position(value.event_log_position),
    })
}

/// Raise a local ordinary result into the peer-callable public contract.
pub fn ordinary_egress(value: sema::OrdinaryEgress) -> crate::Result<ordinary::Output> {
    Ok(match value {
        sema::OrdinaryEgress::Queried(listing) => {
            ordinary::Output::Queried(ordinary::QueriedPayload::new(ordinary::GenerationListing {
                generation_vector: listing
                    .generation_vector
                    .into_iter()
                    .map(public_generation)
                    .collect::<crate::Result<_>>()?,
                deployment_record_vector: listing
                    .deployment_record_vector
                    .into_iter()
                    .map(public_deployment_record)
                    .collect::<crate::Result<_>>()?,
                database_marker: public_marker(listing.state_marker),
            }))
        }
        sema::OrdinaryEgress::DeploymentEventsQueried(page) => {
            ordinary::Output::DeploymentEventsQueried(
                ordinary::DeploymentEventsQueriedPayload::new(ordinary::EventLogPage {
                    deployment_phase_event_vector: page
                        .deployment_phase_event_vector
                        .into_iter()
                        .map(public_phase_event)
                        .collect::<crate::Result<_>>()?,
                    cache_retention_transition_event_vector: page
                        .cache_retention_transition_event_vector
                        .into_iter()
                        .map(public_retention_event)
                        .collect::<crate::Result<_>>()?,
                    database_marker: public_marker(page.state_marker),
                }),
            )
        }
        sema::OrdinaryEgress::TestRunsQueried(listing) => ordinary::Output::TestRunsQueried(
            ordinary::TestRunsQueriedPayload::new(ordinary::TestRunListing {
                test_run_record_vector: listing
                    .test_run_record_vector
                    .into_iter()
                    .map(public_test_run_record)
                    .collect(),
                database_marker: public_marker(listing.database_marker.into_payload()),
            }),
        ),
        sema::OrdinaryEgress::Watching(opened) => ordinary::Output::Watching(
            ordinary::WatchingPayload::new(ordinary::SubscriptionOpened {
                subscription_token: public_subscription_token(opened.subscription_token),
                commit_sequence: ordinary::CommitSequence::new(
                    opened.commit_sequence.into_payload(),
                ),
            }),
        ),
        sema::OrdinaryEgress::Unwatched(closed) => {
            ordinary::Output::Unwatched(ordinary::UnwatchedPayload::new(
                ordinary::SubscriptionClosed::new(public_subscription_token(closed.into_payload())),
            ))
        }
        sema::OrdinaryEgress::KeyMaterialChecked(report) => ordinary::Output::KeyMaterialChecked(
            ordinary::KeyMaterialCheckedPayload::new(ordinary::KeyMaterialReport {
                node_name: public_node_name(report.node_name),
                // Local diagnostics deliberately remain daemon-side; their
                // strings may contain host details and are not a public schema.
                key_material_mismatch_vector: Vec::new(),
                database_marker: public_marker(report.state_marker),
            }),
        ),
        sema::OrdinaryEgress::QueryRejected(rejected) => ordinary::Output::QueryRejected(
            ordinary::QueryRejectedPayload::new(ordinary::RejectedQuery {
                query_rejection_reason: public_query_reason(rejected.query_rejection_reason),
                database_marker: public_marker(rejected.state_marker),
            }),
        ),
        sema::OrdinaryEgress::WatchRejected(rejected) => {
            ordinary::Output::WatchRejected(ordinary::WatchRejectedPayload::new(
                ordinary::RejectedWatch::new(public_watch_reason(rejected.into_payload())),
            ))
        }
        sema::OrdinaryEgress::UnwatchRejected(rejected) => ordinary::Output::UnwatchRejected(
            ordinary::UnwatchRejectedPayload::new(ordinary::RejectedUnwatch {
                unwatch_rejection_reason: public_unwatch_reason(rejected.unwatch_rejection_reason),
                subscription_token: public_subscription_token(rejected.subscription_token),
            }),
        ),
        sema::OrdinaryEgress::KeyMaterialCheckRejected(rejected) => {
            ordinary::Output::KeyMaterialCheckRejected(
                ordinary::KeyMaterialCheckRejectedPayload::new(
                    ordinary::RejectedKeyMaterialCheck {
                        key_material_check_rejection_reason: public_key_reason(
                            rejected.key_material_check_rejection_reason,
                        ),
                        database_marker: public_marker(rejected.state_marker),
                    },
                ),
            )
        }
    })
}

fn public_pin_reason(value: sema::PinRejectionReason) -> meta::PinRejectionReason {
    match value {
        sema::PinRejectionReason::GenerationUnknown => meta::PinRejectionReason::GenerationUnknown,
        sema::PinRejectionReason::NodeUnknown => meta::PinRejectionReason::NodeUnknown,
        sema::PinRejectionReason::PinLabelInUse => meta::PinRejectionReason::PinLabelInUse,
        sema::PinRejectionReason::PinSlotExhausted => meta::PinRejectionReason::PinSlotExhausted,
        sema::PinRejectionReason::InternalError => meta::PinRejectionReason::InternalError,
    }
}

fn public_unpin_reason(value: sema::UnpinRejectionReason) -> meta::UnpinRejectionReason {
    match value {
        sema::UnpinRejectionReason::PinLabelUnknown => meta::UnpinRejectionReason::PinLabelUnknown,
        sema::UnpinRejectionReason::NodeUnknown => meta::UnpinRejectionReason::NodeUnknown,
        sema::UnpinRejectionReason::GenerationNotPinned => {
            meta::UnpinRejectionReason::GenerationNotPinned
        }
        sema::UnpinRejectionReason::InternalError => meta::UnpinRejectionReason::InternalError,
    }
}

fn public_retire_reason(value: sema::RetireRejectionReason) -> meta::RetireRejectionReason {
    match value {
        sema::RetireRejectionReason::GenerationUnknown => {
            meta::RetireRejectionReason::GenerationUnknown
        }
        sema::RetireRejectionReason::NodeUnknown => meta::RetireRejectionReason::NodeUnknown,
        sema::RetireRejectionReason::GenerationActive => {
            meta::RetireRejectionReason::GenerationActive
        }
        sema::RetireRejectionReason::GenerationPinned => {
            meta::RetireRejectionReason::GenerationPinned
        }
        sema::RetireRejectionReason::InternalError => meta::RetireRejectionReason::InternalError,
    }
}

fn public_test_reason(value: sema::TestRejectionReason) -> meta::TestRejectionReason {
    match value {
        sema::TestRejectionReason::ClusterUnknown => meta::TestRejectionReason::ClusterUnknown,
        sema::TestRejectionReason::NodeUnknown => meta::TestRejectionReason::NodeUnknown,
        sema::TestRejectionReason::VmHostNotDeclaredForNode => {
            meta::TestRejectionReason::VmHostNotDeclaredForNode
        }
        sema::TestRejectionReason::HostDeclaresNoVmHost => {
            meta::TestRejectionReason::HostDeclaresNoVmHost
        }
        sema::TestRejectionReason::NoTestDefaults => meta::TestRejectionReason::NoTestDefaults,
        sema::TestRejectionReason::LiveNotYetEnabled => {
            meta::TestRejectionReason::LiveNotYetEnabled
        }
        sema::TestRejectionReason::SubstrateUnavailable => {
            meta::TestRejectionReason::SubstrateUnavailable
        }
        sema::TestRejectionReason::InternalError => meta::TestRejectionReason::InternalError,
    }
}

/// Raise a local privileged result into the owner-only public contract.
pub fn meta_egress(value: sema::MetaEgress) -> crate::Result<meta::Output> {
    Ok(match value {
        sema::MetaEgress::DeployAccepted(handle) => {
            meta::Output::DeployAccepted(meta::DeployAcceptedPayload::new(meta::DeployHandle {
                deployment_identifier: public_deployment_identifier(handle.deployment_identifier),
                database_marker: public_marker(handle.state_marker),
            }))
        }
        sema::MetaEgress::DeployRejected(rejected) => {
            meta::Output::DeployRejected(meta::DeployRejectedPayload::new(
                meta::RejectedDeploy::new(public_deployment_record(rejected.into_payload())?),
            ))
        }
        sema::MetaEgress::DeployTerminal(record) => meta::Output::DeployTerminal(
            meta::DeployTerminalPayload::new(public_deployment_record(record)?),
        ),
        sema::MetaEgress::Pinned(applied) => {
            meta::Output::Pinned(meta::PinnedPayload::new(meta::AppliedPin {
                generation_identifier: public_generation_identifier(applied.generation_identifier),
                pin_label: public_pin_label(applied.pin_label),
                from_slot: public_generation_slot(applied.from_slot)?,
                to_slot: public_generation_slot(applied.to_slot)?,
                database_marker: public_marker(applied.state_marker),
            }))
        }
        sema::MetaEgress::PinRejected(rejected) => {
            meta::Output::PinRejected(meta::PinRejectedPayload::new(meta::RejectedPin {
                pin_rejection_reason: public_pin_reason(rejected.pin_rejection_reason),
                database_marker: public_marker(rejected.state_marker),
            }))
        }
        sema::MetaEgress::Unpinned(applied) => {
            meta::Output::Unpinned(meta::UnpinnedPayload::new(meta::AppliedUnpin {
                generation_identifier: public_generation_identifier(applied.generation_identifier),
                pin_label: public_pin_label(applied.pin_label),
                from_slot: public_generation_slot(applied.from_slot)?,
                to_slot: public_generation_slot(applied.to_slot)?,
                database_marker: public_marker(applied.state_marker),
            }))
        }
        sema::MetaEgress::UnpinRejected(rejected) => {
            meta::Output::UnpinRejected(meta::UnpinRejectedPayload::new(meta::RejectedUnpin {
                unpin_rejection_reason: public_unpin_reason(rejected.unpin_rejection_reason),
                database_marker: public_marker(rejected.state_marker),
            }))
        }
        sema::MetaEgress::Retired(applied) => {
            meta::Output::Retired(meta::RetiredPayload::new(meta::AppliedRetire {
                generation_identifier: public_generation_identifier(applied.generation_identifier),
                generation_slot: public_generation_slot(applied.generation_slot)?,
                database_marker: public_marker(applied.state_marker),
            }))
        }
        sema::MetaEgress::RetireRejected(rejected) => {
            meta::Output::RetireRejected(meta::RetireRejectedPayload::new(meta::RejectedRetire {
                retire_rejection_reason: public_retire_reason(rejected.retire_rejection_reason),
                database_marker: public_marker(rejected.state_marker),
            }))
        }
        sema::MetaEgress::Tested(accepted) => {
            meta::Output::Tested(meta::TestedPayload::new(meta::AcceptedTest {
                test_run_identifier: public_test_run_identifier(accepted.test_run_identifier),
                database_marker: public_marker(accepted.state_marker),
            }))
        }
        sema::MetaEgress::TestRejected(rejected) => {
            meta::Output::TestRejected(meta::TestRejectedPayload::new(meta::RejectedTest {
                test_rejection_reason: public_test_reason(rejected.test_rejection_reason),
                database_marker: public_marker(rejected.state_marker),
            }))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{canonical_nix_store_root, meta_egress, ordinary_egress, public_closure_path};
    use crate::schema::sema;

    fn marker() -> sema::StateMarker {
        sema::StateMarker {
            commit_sequence: sema::CommitSequence::new(7),
            state_digest: sema::StateDigest::new(7),
        }
    }

    fn deployment_record() -> sema::DeploymentRecord {
        sema::DeploymentRecord {
            deployment_identifier: sema::DeploymentIdentifier::new(1),
            generation_identifier: sema::GenerationIdentifier::new(1),
            deployment_request_identity: sema::DeploymentRequestIdentity {
                deployment_environment: sema::DeploymentEnvironment::HostEnvironment,
                cluster_name: sema::ClusterName::new("alpha"),
                node_name: sema::NodeName::new("node-1"),
                generation_artifact: sema::GenerationArtifact::BaseHost,
                requested_deployment_action: sema::RequestedDeploymentAction::Host(
                    sema::HostDeployAction::Realize,
                ),
                activation_effect: sema::ActivationEffect::LiveActivation,
                source_revision_policy: sema::SourceRevisionPolicy::ResolveAndRecord,
                optional_immutable_revision: None,
            },
            optional_admission_marker: None,
            deployment_lifecycle: sema::DeploymentLifecycle::Failed,
            optional_terminal_marker: Some(sema::TerminalMarker::new(marker())),
            optional_deployment_terminal: Some(sema::DeploymentTerminal::Failed(
                sema::DeploymentFailure {
                    deployment_failure_stage: sema::DeploymentFailureStage::Eval,
                    deployment_terminal_reason:
                        sema::DeploymentTerminalReason::FlakeReferenceMalformed,
                },
            )),
        }
    }

    #[test]
    fn ordinary_closure_projection_allows_only_a_canonical_store_item_root() {
        let valid = "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system-toplevel";
        assert!(canonical_nix_store_root(valid));
        assert_eq!(
            public_closure_path(sema::ClosurePath::new(valid)),
            Some(signal_lojix::schema::lib::ClosurePath::new(valid))
        );
    }

    #[test]
    fn ordinary_closure_projection_omits_private_or_noncanonical_paths() {
        for value in [
            "/home/li/private",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system/bin/switch",
            "/nix/store/short-system",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-private-secret",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-name/../escape",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-name\nleak",
        ] {
            assert!(
                public_closure_path(sema::ClosurePath::new(value)).is_none(),
                "ordinary projection must omit an unsafe private closure path"
            );
        }
    }

    #[test]
    fn public_outputs_are_typed_and_omit_raw_source_reference_error_and_path_text() {
        let private_text = "proposal=/srv/private/cluster.dotos ref=github:owner/repo?token=raw-secret error=raw failure path=/tmp/private";
        let private_path = "/srv/private/generated-input";
        let queried = ordinary_egress(sema::OrdinaryEgress::Queried(sema::GenerationListing {
            generation_vector: vec![sema::Generation {
                generation_identifier: sema::GenerationIdentifier::new(1),
                deployment_identifier: sema::DeploymentIdentifier::new(1),
                cluster_name: sema::ClusterName::new("alpha"),
                node_name: sema::NodeName::new("node-1"),
                generation_artifact: sema::GenerationArtifact::BaseHost,
                activation_effect: sema::ActivationEffect::LiveActivation,
                generation_slot: sema::GenerationSlot::Current,
                closure_path: sema::ClosurePath::new(private_path),
                optional_immutable_revision: None,
            }],
            deployment_record_vector: vec![deployment_record()],
            state_marker: marker(),
        }))
        .expect("project ordinary output");
        let signal_lojix::schema::lib::Output::Queried(listing) = &queried else {
            panic!("expected ordinary listing");
        };
        assert!(
            listing.payload().generation_vector[0]
                .optional_closure_path
                .is_none()
        );

        let checked = ordinary_egress(sema::OrdinaryEgress::KeyMaterialChecked(
            sema::KeyMaterialReport {
                node_name: sema::NodeName::new("node-1"),
                string_vector: vec![private_text.to_string()],
                state_marker: marker(),
            },
        ))
        .expect("project key-material output");
        let signal_lojix::schema::lib::Output::KeyMaterialChecked(report) = &checked else {
            panic!("expected typed key-material report");
        };
        assert!(report.payload().key_material_mismatch_vector.is_empty());

        let terminal = meta_egress(sema::MetaEgress::DeployTerminal(deployment_record()))
            .expect("project terminal output");
        let printed = format!("{queried:?}{checked:?}{terminal:?}");
        for forbidden in [
            private_text,
            private_path,
            "proposal_source",
            "flake_reference",
            "source_revision_record",
            "string_vector",
        ] {
            assert!(
                !printed.contains(forbidden),
                "public output must not expose raw private field or value"
            );
        }
    }
}
