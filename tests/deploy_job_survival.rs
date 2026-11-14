use lojix::Payload as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use lojix::{DeploymentLedger as _, DurableStore as _};
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_model as memory;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};

mod common;

fn host_submission(proposal_source: &Path) -> memory::ActualizedDeploySubmission {
    memory::ActualizedDeploySubmission {
        horizon_definition_option: Some(common::read_horizon(proposal_source)),
        deploy_submission: memory::DeploySubmission::Host(memory::HostDeployment {
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
            deployment_output_selector: memory::DeploymentOutputSelector::new(
                memory::FlakeAttribute::new("checks.fixture-a".into()),
            ),
            activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
            host_deploy_action: memory::HostDeployAction::Realize,
            source_revision_policy: memory::SourceRevisionPolicy::ResolveAndRecord,
            nix_builder_spec_option: None,
            extra_substituter_vector: Vec::new(),
        }),
    }
}

#[test]
fn accepted_deploy_job_survives_a_store_reopen_with_its_correlation_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.memory");
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
    assert_eq!(job.deploy_job_phase, memory::DeployJobPhase::Submitted);
    assert!(
        resumed
            .deployment_records()
            .expect("read correlations")
            .iter()
            .any(|record| *record.deployment_identifier.payload() == identifier)
    );
}
