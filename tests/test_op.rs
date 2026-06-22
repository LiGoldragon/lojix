//! Contained deploy/test POC tests.
//!
//! The safe test surface is ordinary `signal-lojix`: a test submits
//! `DeployContained`, observes with `VerifyContained` or `Query(ByContainedRun)`, and
//! releases with `Release`. Production deployment stays on `meta-signal-lojix`.

use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::SchemaRuntime;
use signal_lojix::schema::lib as ordinary;

fn run(engine: &mut SchemaRuntime, input: nexus::SignalInput) -> nexus::SignalOutput {
    let work = nexus::NexusWork::SignalArrived(input).with_origin_route(nexus::OriginRoute::new(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(async { engine.execute(work).await.into_root() }) {
        nexus::NexusAction::ReplyToSignal(output) => output,
        other => panic!("expected ReplyToSignal, got {other:?}"),
    }
}

fn ordinary_reply(output: nexus::SignalOutput) -> ordinary::Output {
    match output {
        nexus::SignalOutput::OrdinaryOutput(output) => output,
        nexus::SignalOutput::MetaOutput(output) => panic!("expected ordinary, got {output:?}"),
    }
}

#[derive(Debug, Clone)]
struct ContainedClusterTest {
    cluster: ordinary::ClusterName,
    flake: ordinary::FlakeReference,
    source: ordinary::ProposalSource,
    nodes: Vec<ordinary::NodeName>,
}

impl ContainedClusterTest {
    fn new(cluster: &str, flake: &str) -> Self {
        Self {
            cluster: ordinary::ClusterName::new(cluster),
            flake: ordinary::FlakeReference::new(flake),
            source: ordinary::ProposalSource::new("cluster-test"),
            nodes: Vec::new(),
        }
    }

    fn hermetic(mut self, node: &str) -> Self {
        self.nodes.push(ordinary::NodeName::new(node));
        self
    }

    fn deploy_inputs(&self) -> Vec<ordinary::Input> {
        self.nodes
            .iter()
            .cloned()
            .map(|node| {
                ordinary::Input::DeployContained(ordinary::DeployContained::new(
                    ordinary::DeployContainedRequest {
                        node_profile: ordinary::NodeProfile {
                            cluster_name: self.cluster.clone(),
                            node_name: node,
                            kind: None.into(),
                        },
                        contained_target: ordinary::ContainedTarget::HermeticVm,
                        source: Some(self.source.clone()).into(),
                        flake_reference: self.flake.clone(),
                    },
                ))
            })
            .collect()
    }
}

fn contained_request(node: &str) -> ordinary::DeployContainedRequest {
    ordinary::DeployContainedRequest {
        node_profile: ordinary::NodeProfile {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new(node),
            kind: None.into(),
        },
        contained_target: ordinary::ContainedTarget::HermeticVm,
        source: Some(ordinary::ProposalSource::new("test")).into(),
        flake_reference: ordinary::FlakeReference::new(
            "github:LiGoldragon/CriomOS-test-cluster/main",
        ),
    }
}

fn deploy_contained(engine: &mut SchemaRuntime, node: &str) -> ordinary::AcceptedContainedDeploy {
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::DeployContained(
        ordinary::DeployContained::new(contained_request(node)),
    ));
    match ordinary_reply(run(engine, input)) {
        ordinary::Output::ContainedDeployed(accepted) => accepted.into_payload(),
        other => panic!("expected ContainedDeployed, got {other:?}"),
    }
}

fn query_runs(engine: &mut SchemaRuntime, node: &str) -> Vec<ordinary::ContainedRunRecord> {
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(ordinary::Query::new(
        ordinary::Selection::ByContainedRun(ordinary::ContainedRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new(node),
            run: None.into(),
        }),
    )));
    match ordinary_reply(run(engine, input)) {
        ordinary::Output::ContainedRunsQueried(listing) => {
            listing.into_payload().runs.into_payload()
        }
        other => panic!("expected ContainedRunsQueried, got {other:?}"),
    }
}

#[test]
fn deploy_contained_is_ordinary_and_records_pending_run() {
    let mut engine = SchemaRuntime::new();
    let accepted = deploy_contained(&mut engine, "criome");

    assert_eq!(*accepted.contained_run_identifier.payload(), 1);
    let runs = query_runs(&mut engine, "criome");
    assert_eq!(runs.len(), 1);
    assert!(matches!(
        runs[0].target,
        ordinary::ContainedTarget::HermeticVm
    ));
    assert_eq!(
        runs[0].contained_run_phase,
        ordinary::ContainedRunPhase::Submitted
    );
    assert_eq!(
        runs[0].contained_outcome,
        ordinary::ContainedOutcome::Pending
    );
}

#[test]
fn check_and_release_use_the_contained_run_handle() {
    let mut engine = SchemaRuntime::new();
    let accepted = deploy_contained(&mut engine, "spirit");

    let check = nexus::SignalInput::OrdinaryInput(ordinary::Input::VerifyContained(
        ordinary::VerifyContained::new(ordinary::ContainedVerification {
            contained_run_identifier: accepted.contained_run_identifier.clone(),
            verification_body: ordinary::VerificationBody::Gate,
        }),
    ));
    match ordinary_reply(run(&mut engine, check)) {
        ordinary::Output::ContainedVerified(report) => {
            let report = report.into_payload();
            assert_eq!(
                report.contained_run_phase,
                ordinary::ContainedRunPhase::Completed
            );
            assert_eq!(report.contained_outcome, ordinary::ContainedOutcome::Passed);
        }
        other => panic!("expected ContainedVerified, got {other:?}"),
    }
    let runs = query_runs(&mut engine, "spirit");
    assert_eq!(
        runs[0].contained_run_phase,
        ordinary::ContainedRunPhase::Completed
    );
    assert_eq!(runs[0].contained_outcome, ordinary::ContainedOutcome::Passed);

    let release =
        nexus::SignalInput::OrdinaryInput(ordinary::Input::Release(ordinary::Release::new(
            ordinary::ContainedRelease::new(accepted.contained_run_identifier),
        )));
    match ordinary_reply(run(&mut engine, release)) {
        ordinary::Output::Released(released) => assert!(released.into_payload().released),
        other => panic!("expected Released, got {other:?}"),
    }
}

#[test]
fn verify_contained_steps_fail_closed_when_gate_case_is_not_1_of_1() {
    let mut engine = SchemaRuntime::new();
    let accepted = deploy_contained(&mut engine, "criome");
    let check = nexus::SignalInput::OrdinaryInput(ordinary::Input::VerifyContained(
        ordinary::VerifyContained::new(ordinary::ContainedVerification {
            contained_run_identifier: accepted.contained_run_identifier,
            verification_body: ordinary::VerificationBody::steps(vec![
                ordinary::VerificationStep::gate_case(ordinary::GateCaseStep {
                    component_kind: ordinary::ComponentKind::Criome,
                    gate_outcome: ordinary::GateOutcome::AuthorizedShips,
                    threshold_spec: ordinary::ThresholdSpec::NoGate,
                }),
            ]),
        }),
    ));

    match ordinary_reply(run(&mut engine, check)) {
        ordinary::Output::ContainedVerified(report) => {
            let report = report.into_payload();
            assert_eq!(
                report.contained_run_phase,
                ordinary::ContainedRunPhase::Failed
            );
            assert_eq!(
                report.contained_outcome,
                ordinary::ContainedOutcome::Failed(ordinary::FailureStage::Assert)
            );
        }
        other => panic!("expected ContainedVerified, got {other:?}"),
    }
}

#[test]
fn non_hermetic_targets_are_typed_rejections_in_the_poc() {
    let mut engine = SchemaRuntime::new();
    let mut request = contained_request("router");
    request.contained_target = ordinary::ContainedTarget::VmHostGuest(
        ordinary::VmHostGuestTarget::new(ordinary::HostSelection::DefaultHost),
    );
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::DeployContained(
        ordinary::DeployContained::new(request),
    ));

    match ordinary_reply(run(&mut engine, input)) {
        ordinary::Output::DeployContainedRejected(rejected) => {
            assert_eq!(
                rejected.into_payload().deploy_contained_rejection_reason,
                ordinary::DeployContainedRejectionReason::SubstrateUnavailable
            );
        }
        other => panic!("expected DeployContainedRejected, got {other:?}"),
    }
}

#[test]
fn criome_spirit_router_cluster_interface_is_compact() {
    let inputs =
        ContainedClusterTest::new("goldragon", "github:LiGoldragon/CriomOS-test-cluster/main")
            .hermetic("criome")
            .hermetic("spirit")
            .hermetic("router")
            .deploy_inputs();

    assert_eq!(inputs.len(), 3);
    assert!(
        inputs
            .iter()
            .all(|input| matches!(input, ordinary::Input::DeployContained(_)))
    );
}
