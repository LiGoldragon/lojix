//! Test-op contract tests (report 54, Unit 2a). Drive the generated
//! `NexusEngine::execute` runner over `SchemaRuntime` for the meta `Test` op
//! and the ordinary `(Query (ByTestRun …))` read, proving:
//!
//! - `(Check <node>)` lowers to a full resolved TestRun via the configured
//!   `TestDefaults` — cluster, host, and mode all filled from config (decision
//!   D, the routine form);
//! - `(Run …)` carries cluster/host/mode explicitly;
//! - a Test write returns `AcceptedTest` AND lands a durable, queryable Pending
//!   `TestRunRecord` (status Submitted / outcome Pending) — the Unit-2a stub
//!   records Accepted honestly, never a faked pass.
//!
//! The REAL hermetic / live dispatch is Unit 2b; these tests assert the
//! contract + plumbing only.

use std::sync::Arc;

use lojix::Store;
use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
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

fn meta_reply(output: nexus::SignalOutput) -> meta::Output {
    match output {
        nexus::SignalOutput::MetaOutput(output) => output,
        nexus::SignalOutput::OrdinaryOutput(output) => panic!("expected meta, got {output:?}"),
    }
}

fn ordinary_reply(output: nexus::SignalOutput) -> ordinary::Output {
    match output {
        nexus::SignalOutput::OrdinaryOutput(output) => output,
        nexus::SignalOutput::MetaOutput(output) => panic!("expected ordinary, got {output:?}"),
    }
}

/// A fresh engine over its own tempdir store, configured with the `test_default`
/// TestDefaults (cluster goldragon, default host prometheus, mode Hermetic).
fn engine() -> SchemaRuntime {
    SchemaRuntime::new()
}

fn check_request(node: &str) -> nexus::SignalInput {
    nexus::SignalInput::MetaInput(meta::Input::Test(meta::Test::new(
        meta::TestRequest::Check(meta::QuickCheck::new(vec![ordinary::NodeName::new(node)])),
    )))
}

fn run_request(
    cluster: &str,
    node: &str,
    host: ordinary::HostSelection,
    mode: ordinary::TestMode,
) -> nexus::SignalInput {
    nexus::SignalInput::MetaInput(meta::Input::Test(meta::Test::new(meta::TestRequest::Run(
        meta::TestRun {
            cluster_name: ordinary::ClusterName::new(cluster),
            node_selection: meta::NodeSelection::Nodes(vec![ordinary::NodeName::new(node)]),
            host_selection: host,
            test_mode: mode,
        },
    ))))
}

fn accepted_identifier(output: nexus::SignalOutput) -> u64 {
    match meta_reply(output) {
        meta::Output::Tested(accepted) => *accepted.payload().test_run_identifier.payload(),
        other => panic!("expected Tested(AcceptedTest), got {other:?}"),
    }
}

fn query_runs(
    engine: &mut SchemaRuntime,
    cluster: &str,
    node: &str,
) -> Vec<ordinary::TestRunRecord> {
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(ordinary::Query::new(
        ordinary::Selection::ByTestRun(ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName::new(cluster),
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
fn check_shorthand_returns_accepted_test() {
    let mut engine = engine();
    let identifier = accepted_identifier(run(&mut engine, check_request("mercury")));
    assert_eq!(identifier, 1, "first test run mints identifier 1");
}

#[test]
fn check_shorthand_lowers_to_full_run_via_test_defaults() {
    let mut engine = engine();
    // (Check mercury) carries only the node; cluster/host/mode must come from
    // the configured TestDefaults (goldragon / prometheus / Hermetic).
    accepted_identifier(run(&mut engine, check_request("mercury")));
    let runs = query_runs(&mut engine, "goldragon", "mercury");
    assert_eq!(runs.len(), 1, "exactly one run recorded");
    let record = &runs[0];
    assert_eq!(
        record.cluster_name.payload(),
        "goldragon",
        "cluster from defaults"
    );
    assert_eq!(
        record.node_name.payload(),
        "mercury",
        "node from the request"
    );
    assert_eq!(
        record.host.payload(),
        "prometheus",
        "host from DefaultHost -> default_vm_host"
    );
    assert_eq!(
        record.mode,
        ordinary::TestMode::Hermetic,
        "mode from default_mode"
    );
}

#[test]
fn accepted_test_lands_a_queryable_pending_record() {
    let mut engine = engine();
    let identifier = accepted_identifier(run(&mut engine, check_request("mercury")));
    let runs = query_runs(&mut engine, "goldragon", "mercury");
    assert_eq!(runs.len(), 1);
    let record = &runs[0];
    assert_eq!(
        *record.test_run_identifier.payload(),
        identifier,
        "queryable by the accepted id"
    );
    assert_eq!(
        record.phase,
        ordinary::TestRunPhase::Submitted,
        "stub records Submitted phase"
    );
    assert_eq!(
        record.outcome,
        ordinary::TestOutcome::Pending,
        "stub records Pending — never a faked pass"
    );
    assert!(
        record.closure_path.is_none(),
        "no closure under test yet (Unit 2b)"
    );
}

#[test]
fn run_full_form_carries_explicit_selection() {
    let mut engine = engine();
    // The full (Run …) form on a different cluster/host/mode than the defaults.
    let input = run_request(
        "alpha",
        "node-1",
        ordinary::HostSelection::OnHost(ordinary::NodeName::new("prometheus")),
        ordinary::TestMode::Live,
    );
    accepted_identifier(run(&mut engine, input));
    let runs = query_runs(&mut engine, "alpha", "node-1");
    assert_eq!(runs.len(), 1);
    let record = &runs[0];
    assert_eq!(record.cluster_name.payload(), "alpha", "explicit cluster");
    assert_eq!(
        record.host.payload(),
        "prometheus",
        "explicit OnHost override"
    );
    assert_eq!(record.mode, ordinary::TestMode::Live, "explicit Live mode");
}

#[test]
fn run_default_host_resolves_to_config_default() {
    let mut engine = engine();
    let input = run_request(
        "goldragon",
        "mercury",
        ordinary::HostSelection::DefaultHost,
        ordinary::TestMode::Hermetic,
    );
    accepted_identifier(run(&mut engine, input));
    let runs = query_runs(&mut engine, "goldragon", "mercury");
    assert_eq!(
        runs[0].host.payload(),
        "prometheus",
        "DefaultHost -> default_vm_host"
    );
}

#[test]
fn second_test_run_mints_a_fresh_identifier() {
    let mut engine = engine();
    let first = accepted_identifier(run(&mut engine, check_request("mercury")));
    let second = accepted_identifier(run(&mut engine, check_request("venus")));
    assert_eq!(first, 1);
    assert_eq!(second, 2, "restart-safe identifier issuance increments");
    assert_eq!(query_runs(&mut engine, "goldragon", "mercury").len(), 1);
    assert_eq!(query_runs(&mut engine, "goldragon", "venus").len(), 1);
}

#[test]
fn query_by_run_identifier_filters_to_one() {
    let mut engine = engine();
    accepted_identifier(run(&mut engine, check_request("mercury")));
    let second = accepted_identifier(run(&mut engine, check_request("mercury")));
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(ordinary::Query::new(
        ordinary::Selection::ByTestRun(ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("mercury"),
            run: Some(ordinary::TestRunIdentifier::new(second)),
        }),
    )));
    let runs = match ordinary_reply(run(&mut engine, input)) {
        ordinary::Output::TestRunsQueried(listing) => listing.into_payload().runs,
        other => panic!("expected TestRunsQueried, got {other:?}"),
    };
    assert_eq!(runs.len(), 1, "filtered to the named run");
    assert_eq!(*runs[0].test_run_identifier.payload(), second);
}

#[test]
fn test_run_table_survives_store_reopen() {
    // Prove the durable plane: record a test through one engine over a shared
    // store, drop it, reopen the store, and read the row back — the Pending
    // record persists across a daemon restart (the self-resume property).
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    let identifier = {
        let store = Arc::new(Store::open(path.clone()).expect("open store"));
        let mut engine = SchemaRuntime::with_store_and_configuration(
            store,
            Arc::new(RuntimeConfiguration::test_default()),
        );
        accepted_identifier(run(&mut engine, check_request("mercury")))
    };
    let store = Arc::new(Store::open(path.clone()).expect("reopen store"));
    let mut engine = SchemaRuntime::with_store_and_configuration(
        store,
        Arc::new(RuntimeConfiguration::test_default()),
    );
    let runs = query_runs(&mut engine, "goldragon", "mercury");
    assert_eq!(runs.len(), 1, "the run persisted across reopen");
    assert_eq!(*runs[0].test_run_identifier.payload(), identifier);
    assert_eq!(runs[0].outcome, ordinary::TestOutcome::Pending);
}
