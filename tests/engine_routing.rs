//! Correlation-engine routing witnesses. These stay on the local runtime side
//! of the generated Datom boundary so they can prove durable state without a
//! shell effect.

use std::fs;
use std::path::Path;

use datom_codec::Textualizable;
use horizon_lib::*;
use lojix::runtime_model as sema;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};

fn write_proposal(path: &Path) {
    fn text(value: &str) -> protos::Text {
        protos::Text::try_from(value).expect("fixture text")
    }
    let node = NodeDefinition(
        text("node-1"),
        NodeVariant::Live(LiveDefinition()),
        Magnitude::Max,
        Magnitude::Max,
        MachineDefinition::Metal(
            Architecture::X86_64,
            Hardware(4.into(), None, None, None, None, None),
        ),
        NodeEnvironment(Keyboard::Qwerty, None),
        NodeNetwork(vec![], None, None, vec![], None),
        NodeKeys(text("ssh-ed25519 AAAAfixture"), None, None),
        Some(true),
        vec![],
    );
    let definition = HorizonDefinition(
        HorizonConfiguration(vec![], DomainConfiguration(text("criome"), vec![])),
        ClusterDefinition(
            text("alpha"),
            vec![node],
            vec![],
            vec![],
            vec![],
            ClusterTrust(Magnitude::Max, vec![], vec![], vec![]),
        ),
    );
    fs::write(path, definition.textualize()).expect("write HorizonDefinition");
}

fn host_submission(proposal_source: &Path) -> sema::DeploySubmission {
    sema::DeploySubmission::Host(sema::HostDeployment {
        cluster_name: sema::ClusterName::new("alpha"),
        node_name: sema::NodeName::new("node-1"),
        host_composition: sema::HostComposition::BaseHost,
        proposal_source: sema::ProposalSource::new(proposal_source.display().to_string()),
        secrets_input: sema::SecretsInput::NoSecrets,
        flake_reference: sema::FlakeReference::new("github:example/fixture"),
        deployment_transport: sema::DeploymentTransport {
            nix_store_uri: sema::NixStoreUri::new("ssh-ng://fixture-copy.invalid"),
            ssh_destination: sema::SshDestination::new("fixture-login@fixture-activate.invalid"),
        },
        deployment_input_mode: sema::DeploymentInputMode::Horizon,
        deployment_output_selector: sema::DeploymentOutputSelector::new(sema::FlakeAttribute::new(
            "checks.fixture-a",
        )),
        activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: sema::HostDeployAction::Realize,
        source_revision_policy: sema::SourceRevisionPolicy::ResolveAndRecord,
        optional_nix_builder_spec: None,
        extra_substituter_vector: Vec::new(),
    })
}

#[test]
fn accepted_submission_creates_a_correlated_durable_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("horizon-definition.datom");
    write_proposal(&proposal_source);
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
    assert!(record.optional_admission_marker.is_some());
    assert_eq!(
        record.deployment_lifecycle,
        sema::DeploymentLifecycle::Submitted
    );
    assert!(record.optional_deployment_terminal.is_none());
}

#[test]
fn capacity_rejection_is_a_correlated_terminal_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("horizon-definition.datom");
    write_proposal(&proposal_source);
    let engine = SchemaRuntime::new();
    let rejected = engine.reject_deployment_in_flight(host_submission(&proposal_source));
    let record = rejected.into_payload();
    assert_ne!(*record.deployment_identifier.payload(), 0);
    assert!(record.optional_admission_marker.is_none());
    assert_eq!(
        record.deployment_lifecycle,
        sema::DeploymentLifecycle::Rejected
    );
    assert!(matches!(
        record.optional_deployment_terminal,
        Some(sema::DeploymentTerminal::Rejected(
            sema::DeploymentTerminalReason::DeploymentInFlight
        ))
    ));
    assert!(record.optional_terminal_marker.is_some());
}
