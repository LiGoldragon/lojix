//! Generated Datom and portable Signal smoke witnesses for Lojix's owner contract.

use lojix::DurableStore as _;
use lojix::Payload as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};
use signal::{ByteViewable, Restorable, Signal, Signalizable};

fn text(value: &str) -> String {
    value.to_owned()
}

fn host_deploy(action: signal_lojix::HostDeployAction) -> meta_signal_lojix::Input {
    host_deploy_with_secrets(action, signal_lojix::SecretsInput::NoSecrets)
}

fn host_deploy_with_secrets(
    action: signal_lojix::HostDeployAction,
    secrets_input: signal_lojix::SecretsInput,
) -> meta_signal_lojix::Input {
    meta_signal_lojix::Input::Deploy(meta_signal_lojix::ActualizedDeploySubmission {
        deploy_submission: meta_signal_lojix::DeploySubmission::Host(
            meta_signal_lojix::HostDeployment {
                cluster_name: signal_lojix::ClusterName(text("fixture-cluster")),
                node_name: signal_lojix::NodeName(text("fixture-node")),
                host_composition: signal_lojix::HostComposition::BaseHost,
                proposal_source: signal_lojix::ProposalSource(text("/dev/null")),
                secrets_input,
                flake_reference: signal_lojix::FlakeReference(text(
                    "github:fixture-owner/fixture-flake",
                )),
                deployment_transport: signal_lojix::DeploymentTransport {
                    nix_store_uri: signal_lojix::NixStoreUri(text("ssh-ng://fixture-copy.invalid")),
                    ssh_destination: signal_lojix::SshDestination(text(
                        "fixture-login@fixture-activate.invalid",
                    )),
                },
                deployment_input_mode: signal_lojix::DeploymentInputMode::Direct,
                deployment_output_selector: signal_lojix::DeploymentOutputSelector {
                    flake_attribute: signal_lojix::FlakeAttribute(text("checks.fixture-a")),
                },
                activation_backend: signal_lojix::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: action,
                source_revision_policy: signal_lojix::SourceRevisionPolicy::ResolveAndRecord,
                nix_builder_spec_option: None,
                extra_substituter_vector: Vec::new(),
            },
        ),
        horizon_definition_option: None,
    })
}

/// The submission an owner `Deploy` carries, as the engine receives it.
fn submission(input: meta_signal_lojix::Input) -> meta_signal_lojix::ActualizedDeploySubmission {
    let meta_signal_lojix::Input::Deploy(submission) = input else {
        panic!("the fixture is a Deploy");
    };
    submission
}

#[test]
fn owner_deploy_carries_a_nonempty_secret_reference_without_reading_it() {
    let input = host_deploy_with_secrets(
        signal_lojix::HostDeployAction::Realize,
        signal_lojix::SecretsInput::SecretsDirectory(signal_lojix::SecretsDirectory(
            "fixture-secret-directory".into(),
        )),
    );
    let lojix::runtime_model::DeploySubmission::Host(host) = submission(input).deploy_submission
    else {
        panic!("owner Host deploy is a Host submission");
    };
    assert!(matches!(
        host.secrets_input,
        lojix::runtime_model::SecretsInput::SecretsDirectory(_)
    ));
}

#[test]
fn owner_deploy_reaches_the_engine_with_explicit_no_secrets() {
    let input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let lojix::runtime_model::DeploySubmission::Host(host) = submission(input).deploy_submission
    else {
        panic!("owner Host deploy is a Host submission");
    };
    assert_eq!(
        host.deployment_output_selector.payload().payload(),
        "checks.fixture-a"
    );
    assert!(matches!(
        host.secrets_input,
        lojix::runtime_model::SecretsInput::NoSecrets
    ));
}

#[test]
fn owner_deploy_rejection_is_answered_deploy_rejected() {
    let mut input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let meta_signal_lojix::Input::Deploy(actualized) = &mut input else {
        unreachable!()
    };
    let meta_signal_lojix::DeploySubmission::Host(host) = &mut actualized.deploy_submission else {
        unreachable!()
    };
    host.deployment_input_mode = signal_lojix::DeploymentInputMode::Horizon;
    let mut engine = SchemaRuntime::new();
    let outcome = engine.submit_deploy(submission(input));
    assert!(matches!(outcome, DeploySubmissionOutcome::Rejected(_)));
    assert!(matches!(
        meta_signal_lojix::Output::from(outcome),
        meta_signal_lojix::Output::DeployRejected(_)
    ));
}

#[test]
fn owner_request_round_trips_fresh_portable_signal_bytes() {
    let input = host_deploy(signal_lojix::HostDeployAction::Evaluate);
    let bytes = input.signalize().expect("signalize query").bytes().to_vec();
    let received = Signal::<meta_signal_lojix::Input>::from(bytes);
    assert_eq!(received.restore().expect("restore peer bytes"), input);
}

#[tokio::test]
#[ignore = "hits the network and runs nix evaluation; run with --ignored"]
async fn fixture_eval_reserves_a_durable_deployment_before_effects() {
    let request = submission(host_deploy(signal_lojix::HostDeployAction::Evaluate));
    let mut engine = SchemaRuntime::new();

    let handle = match engine.submit_deploy(request) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        other => panic!("fixture deploy was not accepted: {other:?}"),
    };
    let records = engine
        .store()
        .deployment_records()
        .expect("read durable deployment records");
    assert!(
        records
            .iter()
            .any(|record| record.deployment_identifier == handle.deployment_identifier),
        "accepted handle must name a durable correlation record before effects begin"
    );
}
