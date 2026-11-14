//! Correlation-engine routing witnesses. These stay on the local runtime side
//! of the generated Datom boundary so they can prove durable state without a
//! shell effect.

use lojix::DurableStore as _;
use lojix::Payload as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use std::path::Path;

use lojix::runtime_model as memory;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};

mod common;

fn host_submission(proposal_source: &Path) -> memory::DeploySubmission {
    memory::DeploySubmission::Host(memory::HostDeployment {
        cluster_name: memory::ClusterName::new("alpha".into()),
        node_name: memory::NodeName::new("node-1".into()),
        host_composition: memory::HostComposition::BaseHost,
        proposal_source: memory::ProposalSource::new(proposal_source.display().to_string()),
        secrets_input: memory::SecretsInput::NoSecrets,
        flake_reference: memory::FlakeReference::new("github:example/fixture".into()),
        deployment_transport: memory::DeploymentTransport {
            nix_store_uri: memory::NixStoreUri::new("ssh-ng://fixture-copy.invalid".into()),
            ssh_destination: memory::SshDestination::new(
                "fixture-login@fixture-activate.invalid".into(),
            ),
        },
        deployment_input_mode: memory::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::read_horizon(proposal_source)),
        deployment_output_selector: memory::DeploymentOutputSelector::new(
            memory::FlakeAttribute::new("checks.fixture-a".into()),
        ),
        activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: memory::HostDeployAction::Realize,
        source_revision_policy: memory::SourceRevisionPolicy::ResolveAndRecord,
        nix_builder_spec_option: None,
        extra_substituter_vector: Vec::new(),
    })
}

#[test]
fn accepted_submission_creates_a_correlated_durable_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("horizon-definition.datom");
    common::write_single_node(&proposal_source);
    let mut engine = SchemaRuntime::new();
    let accepted = match engine.submit_deploy(host_submission(&proposal_source)) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        other => panic!("expected accepted submission, got {other:?}"),
    };
    let identifier = *accepted.deployment_identifier.payload();
    assert_ne!(identifier, 0);
    let records = engine.store().deployment_records().expect("read records");
    let record = records
        .into_iter()
        .find(|record| *record.deployment_identifier.payload() == identifier)
        .expect("accepted submission has a durable record");
    assert!(record.admission_marker_option.is_some());
    assert_eq!(record.deployment_phase, memory::DeploymentPhase::Submitted);
    assert!(record.deployment_terminal_option.is_none());
}

#[test]
fn capacity_rejection_is_a_correlated_terminal_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("horizon-definition.datom");
    common::write_single_node(&proposal_source);
    let engine = SchemaRuntime::new();
    let lojix::schema_runtime::DeploySubmissionOutcome::Rejected(rejected) =
        engine.reject_deployment_in_flight(host_submission(&proposal_source))
    else {
        panic!("a healthy store rejects with the record it allocated")
    };
    let record = rejected.into_payload();
    assert_ne!(*record.deployment_identifier.payload(), 0);
    assert!(record.admission_marker_option.is_none());
    assert_eq!(record.deployment_phase, memory::DeploymentPhase::Rejected);
    assert!(matches!(
        record.deployment_terminal_option,
        Some(memory::DeploymentTerminal::Rejected(
            memory::DeploymentTerminalReason::DeploymentInFlight
        ))
    ));
    assert!(record.terminal_marker_option.is_some());
}
