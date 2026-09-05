//! Generated Datom and bound-frame smoke witnesses for Lojix's owner contract.

use lojix::adapters;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};
use meta_signal_lojix::WireConversion;
use signal_frame::{
    BoundExchangeFrame, ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, LaneSequence,
    Request as FrameRequest, RootCode, SessionEpoch, VariantCode, WireRoute,
};

fn text(value: &str) -> protos::Text {
    protos::Text::try_from(value).expect("fixture text")
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn host_deploy(action: signal_lojix::HostDeployAction) -> meta_signal_lojix::Request {
    meta_signal_lojix::Request::Deploy(meta_signal_lojix::DeployRequest::Host(
        meta_signal_lojix::HostDeployment(
            text("fixture-cluster"),
            text("fixture-node"),
            signal_lojix::HostComposition::BaseHost,
            text("/dev/null"),
            signal_lojix::SecretsInput::NoSecrets,
            text("github:fixture-owner/fixture-flake"),
            signal_lojix::DeploymentTransport(
                text("ssh-ng://fixture-copy.invalid"),
                text("fixture-login@fixture-activate.invalid"),
            ),
            signal_lojix::DeploymentInputMode::Direct,
            signal_lojix::DeploymentOutputSelector(text("checks.fixture-a")),
            signal_lojix::ActivationBackend::NixosSystemdBootV1,
            action,
            signal_lojix::SourceRevisionPolicy::ResolveAndRecord,
            None,
            Vec::new(),
        ),
    ))
}

#[test]
fn owner_deploy_lowers_to_runtime_shape_with_explicit_no_secrets() {
    let input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let lojix::runtime_model::MetaIngress::Deploy(lojix::runtime_model::DeploySubmission::Host(
        host,
    )) = adapters::meta_ingress(input).expect("generated owner request lowers")
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
    let input = host_deploy(signal_lojix::HostDeployAction::ActivateNow);
    let lojix::runtime_model::MetaIngress::Deploy(request) =
        adapters::meta_ingress(input).expect("generated owner request lowers")
    else {
        panic!("owner Host deploy must lower to a runtime Host submission");
    };
    let mut engine = SchemaRuntime::new();
    let DeploySubmissionOutcome::Rejected(record) = engine.submit_deploy(request) else {
        panic!("fixture Horizon source must produce a typed rejection");
    };
    adapters::meta_egress(lojix::runtime_model::MetaEgress::DeployRejected(record))
        .expect("typed owner rejection must match the public egress shape");
}

#[test]
fn owner_request_uses_a_bound_structural_frame() {
    let input = host_deploy(signal_lojix::HostDeployAction::Evaluate);
    let expected_exchange = exchange();
    let expected_route = WireRoute::new(RootCode::new(0), VariantCode::new(2));
    let bytes = BoundExchangeFrame::<
        meta_signal_lojix::MetaLojixWire,
        meta_signal_lojix::RequestWire,
        meta_signal_lojix::ResponseWire,
    >::new(
        expected_route,
        ExchangeFrameBody::Request {
            exchange: expected_exchange,
            request: FrameRequest::from_payload(input.clone().into_wire()),
        },
    )
    .encode_length_prefixed()
    .expect("encode bound structural request frame");

    let frame = BoundExchangeFrame::<
        meta_signal_lojix::MetaLojixWire,
        meta_signal_lojix::RequestWire,
        meta_signal_lojix::ResponseWire,
    >::decode_length_prefixed(&bytes)
    .expect("validate owner contract binding and archive");
    assert_eq!(frame.short_header().route(), expected_route);
    let ExchangeFrameBody::Request { exchange, request } = frame.into_body() else {
        panic!("frame must contain a request");
    };
    assert_eq!(exchange, expected_exchange);
    assert_eq!(
        meta_signal_lojix::Request::try_from_wire(request.payloads().clone().into_head())
            .expect("recover generated request from structural wire"),
        input
    );
}

#[tokio::test]
#[ignore = "hits the network and runs nix evaluation; run with --ignored"]
async fn fixture_eval_reserves_a_durable_deployment_before_effects() {
    let input = host_deploy(signal_lojix::HostDeployAction::Evaluate);
    let lojix::runtime_model::MetaIngress::Deploy(request) =
        adapters::meta_ingress(input).expect("generated owner request lowers")
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
