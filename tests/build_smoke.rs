//! Generated Datom and portable Signal smoke witnesses for Lojix's owner contract.

use lojix::adapters::{Lowerable as _, Raisable as _};
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};
use signal::{ByteViewable, Restorable, Signal, Signalizable};

fn text(value: &str) -> String {
    value.to_owned()
}

fn host_deploy(action: signal_lojix::HostDeployAction) -> meta_signal_lojix::Query {
    host_deploy_with_secrets(action, signal_lojix::SecretsInput::NoSecrets)
}

fn host_deploy_with_secrets(
    action: signal_lojix::HostDeployAction,
    secrets_input: signal_lojix::SecretsInput,
) -> meta_signal_lojix::Query {
    meta_signal_lojix::Query::Deploy(meta_signal_lojix::ActualizedDeploySubmission {
        deploy_submission: meta_signal_lojix::DeploySubmission::Host(
            meta_signal_lojix::HostDeployment {
                cluster_name: text("fixture-cluster"),
                node_name: text("fixture-node"),
                host_composition: signal_lojix::HostComposition::BaseHost,
                proposal_source: text("/dev/null"),
                secrets_input,
                flake_reference: text("github:fixture-owner/fixture-flake"),
                deployment_transport: signal_lojix::DeploymentTransport {
                    nix_store_uri: text("ssh-ng://fixture-copy.invalid"),
                    ssh_destination: text("fixture-login@fixture-activate.invalid"),
                },
                deployment_input_mode: signal_lojix::DeploymentInputMode::Direct,
                deployment_output_selector: signal_lojix::DeploymentOutputSelector {
                    flake_attribute: text("checks.fixture-a"),
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

#[test]
fn owner_deploy_lowers_nonempty_secret_reference_without_reading_it() {
    let input = host_deploy_with_secrets(
        signal_lojix::HostDeployAction::Realize,
        signal_lojix::SecretsInput::SecretsDirectory("fixture-secret-directory".into()),
    );
    let lojix::runtime_model::MetaIngress::Deploy(lojix::runtime_model::DeploySubmission::Host(
        host,
    )) = input.lower().expect("generated owner request lowers")
    else {
        panic!("owner Host deploy must lower to a runtime Host submission");
    };
    assert!(matches!(
        host.secrets_input,
        lojix::runtime_model::SecretsInput::SecretsDirectory(_)
    ));
}

#[test]
fn owner_deploy_lowers_to_runtime_shape_with_explicit_no_secrets() {
    let input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let lojix::runtime_model::MetaIngress::Deploy(lojix::runtime_model::DeploySubmission::Host(
        host,
    )) = input.lower().expect("generated owner request lowers")
    else {
        panic!("owner Host deploy must lower to a runtime Host submission");
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
fn owner_deploy_rejection_lowers_to_public_shape() {
    let mut input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let meta_signal_lojix::Query::Deploy(actualized) = &mut input else {
        unreachable!()
    };
    let meta_signal_lojix::DeploySubmission::Host(host) = &mut actualized.deploy_submission else {
        unreachable!()
    };
    host.deployment_input_mode = signal_lojix::DeploymentInputMode::Horizon;
    let lojix::runtime_model::MetaIngress::Deploy(request) =
        input.lower().expect("generated owner request lowers")
    else {
        panic!("owner Host deploy must lower to a runtime Host submission");
    };
    let mut engine = SchemaRuntime::new();
    let DeploySubmissionOutcome::Rejected(record) = engine.submit_deploy(request) else {
        panic!("fixture Horizon source must produce a typed rejection");
    };
    lojix::runtime_model::MetaEgress::DeployRejected(record)
        .raise()
        .expect("typed owner rejection must match the public egress shape");
}

#[test]
fn owner_request_round_trips_fresh_portable_signal_bytes() {
    let input = host_deploy(signal_lojix::HostDeployAction::Evaluate);
    let bytes = input.signalize().expect("signalize query").bytes().to_vec();
    let received = Signal::<meta_signal_lojix::Query>::from(bytes);
    assert_eq!(received.restore().expect("restore peer bytes"), input);
}

#[tokio::test]
#[ignore = "hits the network and runs nix evaluation; run with --ignored"]
async fn fixture_eval_reserves_a_durable_deployment_before_effects() {
    let input = host_deploy(signal_lojix::HostDeployAction::Evaluate);
    let lojix::runtime_model::MetaIngress::Deploy(request) =
        input.lower().expect("generated owner request lowers")
    else {
        panic!("fixture must lower to Deploy");
    };
    let mut engine = SchemaRuntime::new();

    let handle = match engine.submit_deploy(request) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        DeploySubmissionOutcome::Rejected(rejected) => {
            panic!("fixture deploy rejected: {rejected:?}")
        }
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
