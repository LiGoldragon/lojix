//! Dotos-frame smoke witnesses for the public Lojix contracts.

use lojix::adapters;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, SessionEpoch};
use signal_lojix::schema::lib as ordinary;

const FIXTURE_FLAKE: &str = "github:LiGoldragon/CriomOS-test-cluster";
const FIXTURE_ATTRIBUTE: &str = "dune-nspawn-toplevel";

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn dune_host_deploy(action: ordinary::HostDeployAction) -> meta::Input {
    meta::Input::deploy(meta::DeployRequest::Host(meta::HostDeployment {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_name: ordinary::NodeName::new("dune"),
        host_composition: ordinary::HostComposition::BaseHost,
        proposal_source: ordinary::ProposalSource::new("/dev/null"),
        flake_reference: ordinary::FlakeReference::new(FIXTURE_FLAKE),
        host_deploy_action: action,
        source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
        optional_builder: None,
        extra_substituter_vector: Vec::new(),
        optional_flake_attribute: Some(meta::FlakeAttribute::new(FIXTURE_ATTRIBUTE)),
    }))
}

#[test]
fn owner_dotos_request_frame_roundtrips_with_its_exchange() {
    let input = dune_host_deploy(ordinary::HostDeployAction::Evaluate);
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
    let input = dune_host_deploy(ordinary::HostDeployAction::Evaluate);
    let local = adapters::meta_ingress(input);
    let lojix::schema::sema::MetaIngress::Deploy(request) = local else {
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
