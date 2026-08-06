//! Dotos-frame smoke witnesses for the public Lojix contracts.

#![cfg(feature = "dotos-text")]

use dotos::DotosSource;
use lojix::adapters;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, SessionEpoch};

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn dune_host_deploy(action: &str) -> meta::z2VW7Q {
    let source = format!(
        "Deploy.Host.(fixture-cluster fixture-node BaseHost /dev/null github:fixture-owner/fixture-flake (ssh-ng://fixture-copy.invalid fixture-login@fixture-activate.invalid) Direct (checks.fixture-a) NixosSystemdBootV1 {action} ResolveAndRecord None [])"
    );
    DotosSource::new(&source)
        .parse()
        .expect("owner deploy Interface Dotos")
}

#[test]
fn owner_dotos_request_frame_roundtrips_with_its_exchange() {
    let input = dune_host_deploy("Evaluate");
    let expected_exchange = exchange();

    let bytes = input
        .clone()
        .encode_request_frame(expected_exchange)
        .expect("encode Dotos request frame");
    let (actual_exchange, decoded) =
        meta::ContractMarker::decode_single_request(&bytes).expect("decode Dotos request frame");

    assert_eq!(actual_exchange, expected_exchange);
    assert_eq!(decoded, input);
}

#[tokio::test]
#[ignore = "hits the network and runs nix evaluation; run with --ignored"]
async fn fixture_eval_reserves_a_durable_deployment_before_effects() {
    let input = dune_host_deploy("Evaluate");
    let local = adapters::meta_ingress(input);
    let lojix::runtime_model::MetaIngress::Deploy(request) = local else {
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
