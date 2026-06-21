//! Contained deploy/test POC tests.
//!
//! The safe test surface is ordinary `signal-lojix`: a test submits
//! `DeployContained`, observes with `CheckContained` or `Query(ByTestRun)`, and
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
                            kind: None,
                        },
                        contained_target: ordinary::ContainedTarget::HermeticVm,
                        source: self.source.clone(),
                        flake: self.flake.clone(),
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
            kind: None,
        },
        contained_target: ordinary::ContainedTarget::HermeticVm,
        source: ordinary::ProposalSource::new("test"),
        flake: ordinary::FlakeReference::new("github:LiGoldragon/CriomOS-test-cluster/main"),
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

fn query_runs(engine: &mut SchemaRuntime, node: &str) -> Vec<ordinary::TestRunRecord> {
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(ordinary::Query::new(
        ordinary::Selection::ByTestRun(ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new(node),
            run: None,
        }),
    )));
    match ordinary_reply(run(engine, input)) {
        ordinary::Output::TestRunsQueried(listing) => listing.into_payload().runs,
        other => panic!("expected TestRunsQueried, got {other:?}"),
    }
}

#[test]
fn deploy_contained_is_ordinary_and_records_pending_run() {
    let mut engine = SchemaRuntime::new();
    let accepted = deploy_contained(&mut engine, "criome");

    assert_eq!(*accepted.test_run_identifier.payload(), 1);
    let runs = query_runs(&mut engine, "criome");
    assert_eq!(runs.len(), 1);
    assert!(matches!(
        runs[0].target,
        ordinary::ContainedTarget::HermeticVm
    ));
    assert_eq!(runs[0].phase, ordinary::TestRunPhase::Submitted);
    assert_eq!(runs[0].outcome, ordinary::TestOutcome::Pending);
}

#[test]
fn check_and_release_use_the_contained_run_handle() {
    let mut engine = SchemaRuntime::new();
    let accepted = deploy_contained(&mut engine, "spirit");

    let check = nexus::SignalInput::OrdinaryInput(ordinary::Input::CheckContained(
        ordinary::CheckContained::new(ordinary::ContainedCheck::new(
            accepted.test_run_identifier.clone(),
        )),
    ));
    match ordinary_reply(run(&mut engine, check)) {
        ordinary::Output::ContainedChecked(report) => {
            let report = report.into_payload();
            assert_eq!(report.phase, ordinary::TestRunPhase::Submitted);
            assert_eq!(report.outcome, ordinary::TestOutcome::Pending);
        }
        other => panic!("expected ContainedChecked, got {other:?}"),
    }

    let release =
        nexus::SignalInput::OrdinaryInput(ordinary::Input::Release(ordinary::Release::new(
            ordinary::ContainedRelease::new(accepted.test_run_identifier),
        )));
    match ordinary_reply(run(&mut engine, release)) {
        ordinary::Output::Released(released) => assert!(released.into_payload().released),
        other => panic!("expected Released, got {other:?}"),
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
                ordinary::DeployContainedRejectionReason::LiveNotYetEnabled
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
