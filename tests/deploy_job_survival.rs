use lojix::Payload as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use lojix::{DeploymentLedger as _, DurableStore as _};
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_model as sema;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};

mod common;

fn host_submission(proposal_source: &Path) -> sema::DeploySubmission {
    sema::DeploySubmission::Host(sema::HostDeployment {
        cluster_name: sema::ClusterName::from("alpha"),
        node_name: sema::NodeName::from("node-1"),
        host_composition: sema::HostComposition::BaseHost,
        proposal_source: sema::ProposalSource::new(proposal_source.display().to_string()),
        secrets_input: sema::SecretsInput::NoSecrets,
        flake_reference: sema::FlakeReference::from("github:example/fixture"),
        deployment_transport: sema::DeploymentTransport {
            nix_store_uri: sema::NixStoreUri::from("ssh-ng://fixture-copy.invalid"),
            ssh_destination: sema::SshDestination::from("fixture-login@fixture-activate.invalid"),
        },
        deployment_input_mode: sema::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::read_horizon(proposal_source)),
        deployment_output_selector: sema::DeploymentOutputSelector::new(
            sema::FlakeAttribute::from("checks.fixture-a"),
        ),
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
    common::write_single_node(&proposal_source);
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
            .records::<lojix::runtime_model::DeploymentRecord>()
            .expect("read correlations")
            .iter()
            .any(|record| *record.deployment_identifier.payload() == identifier)
    );
}
