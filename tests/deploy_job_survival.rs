use std::fs;
use std::path::Path;
use std::sync::Arc;

use datom_codec::Textualizable;
use horizon_lib::*;
use lojix::Store;
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
fn accepted_deploy_job_survives_a_store_reopen_with_its_correlation_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.sema");
    let proposal_source = directory.path().join("horizon-definition.datom");
    write_proposal(&proposal_source);
    let store = Arc::new(Store::open(&path).expect("open store"));
    let mut runtime = SchemaRuntime::with_store(store.clone());
    let accepted = match runtime.submit_deploy(host_submission(&proposal_source)) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        other => panic!("expected admission, got {other:?}"),
    };
    let identifier = *accepted.deployment_identifier.payload();
    assert_eq!(store.deploy_jobs().expect("job row").len(), 1);
    drop(runtime);
    drop(store);

    let resumed = Store::open(&path).expect("reopen store");
    let job = resumed
        .deploy_jobs()
        .expect("read surviving job")
        .into_iter()
        .next()
        .expect("one surviving job");
    assert_eq!(*job.deployment_identifier.payload(), identifier);
    assert_eq!(job.deploy_job_phase, sema::DeployJobPhase::Submitted);
    assert!(
        resumed
            .deployment_records()
            .expect("read correlations")
            .iter()
            .any(|record| *record.deployment_identifier.payload() == identifier)
    );
}
