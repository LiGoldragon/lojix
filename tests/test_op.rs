//! Contained deploy/test POC tests.
//!
//! The safe test surface is ordinary `signal-lojix`: a test submits
//! `DeployContained`, observes with `VerifyContained` or `Query(ByContainedRun)`, and
//! releases with `Release`. Production deployment stays on `meta-signal-lojix`.

use std::sync::Arc;

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use lojix::{CriomeGateConfiguration, DaemonConfiguration, Store, TestDefaults};
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

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

    fn request(&self) -> ordinary::RunContainedClusterRequest {
        ordinary::RunContainedClusterRequest {
            cluster_name: self.cluster.clone(),
            cluster_members: self
                .nodes
                .iter()
                .cloned()
                .map(ordinary::ClusterMember::Member)
                .collect::<Vec<_>>()
                .into(),
            contained_target: ordinary::ContainedTarget::HermeticVm,
            source: Some(self.source.clone()).into(),
            flake_reference: self.flake.clone(),
            verification_body: ordinary::VerificationBody::Gate,
        }
    }

    fn cluster_input(&self) -> ordinary::Input {
        ordinary::Input::RunContainedCluster(ordinary::RunContainedCluster::new(self.request()))
    }
}

fn cluster_test() -> ContainedClusterTest {
    ContainedClusterTest::new("goldragon", "github:LiGoldragon/CriomOS-test-cluster/main")
        .hermetic("criome")
        .hermetic("spirit")
        .hermetic("router")
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

fn query_clusters(engine: &mut SchemaRuntime) -> ordinary::ClusterRunListing {
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(ordinary::Query::new(
        ordinary::Selection::ByClusterRun(ordinary::ClusterRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            cluster_run: None.into(),
        }),
    )));
    match ordinary_reply(run(engine, input)) {
        ordinary::Output::ClusterRunsQueried(listing) => listing.into_payload(),
        other => panic!("expected ClusterRunsQueried, got {other:?}"),
    }
}

fn engine_with_criome_socket(socket_path: &str) -> SchemaRuntime {
    let directory = std::env::temp_dir().join(format!(
        "lojix-criome-gate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).expect("create test directory");
    let store = Arc::new(Store::open(directory.join("lojix.sema")).expect("open store"));
    let configuration = RuntimeConfiguration::from_daemon_configuration(&DaemonConfiguration {
        ordinary_socket_path: directory.join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.join("owner.sock").display().to_string(),
        owner_socket_mode: 0o600,
        state_directory_path: directory.join("state").display().to_string(),
        daemon_host: "ouranos".to_string(),
        test_defaults: TestDefaults {
            cluster: "goldragon".to_string(),
            default_vm_host: "prometheus".to_string(),
            proposal_source: String::new(),
        },
        criome_gate: CriomeGateConfiguration::LocalWitness {
            socket_path: socket_path.to_string(),
        },
    });
    SchemaRuntime::with_store_and_configuration(store, Arc::new(configuration))
}

fn spawn_local_criome(directory: &TempDir) -> std::path::PathBuf {
    let socket = directory.path().join("criome.sock");
    let store = StoreLocation::new(directory.path().join("criome.sema"));
    let bound = CriomeDaemon::new(socket.clone(), store)
        .bind()
        .expect("criome daemon binds its Unix socket");
    std::thread::spawn(move || {
        let _ = bound.serve_forever();
    });
    socket
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
        runs[0].proposal_source,
        ordinary::ProposalSource::new("test")
    );
    assert_eq!(
        runs[0].flake_reference,
        ordinary::FlakeReference::new("github:LiGoldragon/CriomOS-test-cluster/main")
    );
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
    assert_eq!(
        runs[0].contained_outcome,
        ordinary::ContainedOutcome::Passed
    );

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
fn enabled_criome_gate_fails_closed_when_socket_is_missing() {
    let mut engine = engine_with_criome_socket("/tmp/lojix-missing-criome.sock");
    let accepted = deploy_contained(&mut engine, "criome");
    let check = nexus::SignalInput::OrdinaryInput(ordinary::Input::VerifyContained(
        ordinary::VerifyContained::new(ordinary::ContainedVerification {
            contained_run_identifier: accepted.contained_run_identifier,
            verification_body: ordinary::VerificationBody::Gate,
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
fn enabled_criome_gate_accepts_live_1_of_1_socket() {
    let directory = tempfile::tempdir().expect("criome temp dir");
    let criome_socket = spawn_local_criome(&directory);
    let mut engine = engine_with_criome_socket(&criome_socket.display().to_string());
    let accepted = deploy_contained(&mut engine, "criome");
    let check = nexus::SignalInput::OrdinaryInput(ordinary::Input::VerifyContained(
        ordinary::VerifyContained::new(ordinary::ContainedVerification {
            contained_run_identifier: accepted.contained_run_identifier,
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
fn criome_spirit_router_cluster_runs_as_daemon_owned_root() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(cluster_test().cluster_input());

    match ordinary_reply(run(&mut engine, input)) {
        ordinary::Output::ContainedClusterRan(report) => {
            let report = report.into_payload();
            assert_eq!(*report.cluster_run_identifier.payload(), 1);
            assert_eq!(
                report.cluster_run_phase,
                ordinary::ClusterRunPhase::Completed
            );
            assert_eq!(report.cluster_outcome, ordinary::ClusterOutcome::Passed);
        }
        other => panic!("expected ContainedClusterRan, got {other:?}"),
    }

    let listing = query_clusters(&mut engine);
    let cluster_runs = listing.cluster_runs.into_payload();
    let member_runs = listing.runs.into_payload();
    assert_eq!(cluster_runs.len(), 1);
    assert_eq!(member_runs.len(), 3);
    assert_eq!(
        cluster_runs[0].member_runs.payload(),
        &vec![
            ordinary::ContainedRunIdentifier::new(1),
            ordinary::ContainedRunIdentifier::new(2),
            ordinary::ContainedRunIdentifier::new(3),
        ]
    );
    assert!(
        member_runs
            .iter()
            .all(|run| run.contained_run_phase == ordinary::ContainedRunPhase::Completed)
    );
    assert!(
        member_runs
            .iter()
            .all(|run| run.contained_outcome == ordinary::ContainedOutcome::Passed)
    );
}
