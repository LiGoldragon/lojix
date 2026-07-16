//! Test-op contract tests (report 54, Unit 2a). Drive the generated
//! `NexusEngine::execute` runner over `SchemaRuntime` for the meta `Test` op
//! and the ordinary `(Query (ByTestRun …))` read, proving:
//!
//! - `(Check <node>)` lowers to a full resolved TestRun via the configured
//!   `TestDefaults` — cluster, host, and mode all filled from config (decision
//!   D, the routine form);
//! - `(Run …)` carries cluster/host/mode explicitly;
//! - a Test write returns `AcceptedTest` AND lands a durable, queryable Pending
//!   `TestRunRecord` (status Submitted / outcome Pending) — the submit records
//!   Accepted honestly, never a faked pass.
//!
//! Unit 2b adds the REAL dispatch proofs (the `#[ignore]` ones run `nix`):
//! `(Check [mercury])` ACTUALLY nix-builds `vm-mercury` through the decoupled
//! executor and records `Passed` with the realised out-path; a missing check is
//! recorded `Failed(HermeticCheck)`, never faked; the full daemon-socket
//! roundtrip proves the same over the real owner + ordinary sockets. A Live
//! `(Run … Live)` is rejected at submit (`LiveNotYetEnabled`) so it can never
//! record a `Passed` the unimplemented live chain never earned. The host/node
//! selection guarantee is proven against a real projected cluster fixture:
//! a member `OnHost` is accepted, a non-member is `VmHostNotDeclaredForNode`,
//! an unknown node is `NodeUnknown`, and `All` sweeps the hosted-Pod nodes.

use std::sync::Arc;

use lojix::Store;
use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;

fn run(engine: &mut SchemaRuntime, input: nexus::RoutedMail) -> nexus::RoutedReply {
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

fn meta_reply(output: nexus::RoutedReply) -> meta::Output {
    match output {
        nexus::RoutedReply::Meta(output) => output,
        nexus::RoutedReply::Ordinary(output) => panic!("expected meta, got {output:?}"),
    }
}

fn ordinary_reply(output: nexus::RoutedReply) -> ordinary::Output {
    match output {
        nexus::RoutedReply::Ordinary(output) => output,
        nexus::RoutedReply::Meta(output) => panic!("expected ordinary, got {output:?}"),
    }
}

/// A fresh engine over its own tempdir store, configured with the `test_default`
/// TestDefaults (cluster goldragon, default host prometheus, mode Hermetic).
fn engine() -> SchemaRuntime {
    SchemaRuntime::new()
}

fn check_request(node: &str) -> nexus::RoutedMail {
    nexus::RoutedMail::Meta(meta::Input::Test(meta::Test::new(
        meta::TestRequest::Check(meta::QuickCheck::new(vec![ordinary::NodeName::new(node)])),
    )))
}

fn run_request(
    cluster: &str,
    node: &str,
    host: ordinary::HostSelection,
    mode: ordinary::TestMode,
) -> nexus::RoutedMail {
    nexus::RoutedMail::Meta(meta::Input::Test(meta::Test::new(meta::TestRequest::Run(
        meta::TestRun {
            cluster_name: ordinary::ClusterName::new(cluster),
            node_selection: meta::NodeSelection::Nodes(vec![ordinary::NodeName::new(node)]),
            host_selection: host,
            test_mode: mode,
        },
    ))))
}

fn accepted_identifier(output: nexus::RoutedReply) -> u64 {
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
    let input = nexus::RoutedMail::Ordinary(ordinary::Input::Query(ordinary::Query::new(
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
    // The full (Run …) form on a different cluster/host than the defaults. The
    // mode is Hermetic: the explicit-selection plumbing is mode-agnostic, and a
    // Live submit is now rejected up front (see `live_run_is_rejected_*`), so
    // this proves the cluster/host override on the path that actually records.
    let input = run_request(
        "alpha",
        "node-1",
        ordinary::HostSelection::OnHost(ordinary::NodeName::new("prometheus")),
        ordinary::TestMode::Hermetic,
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
    assert_eq!(
        record.mode,
        ordinary::TestMode::Hermetic,
        "explicit Hermetic mode"
    );
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
    let input = nexus::RoutedMail::Ordinary(ordinary::Input::Query(ordinary::Query::new(
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

// ---- Unit 2b: the REAL hermetic dispatch, proven end-to-end ----

/// Drive a `Test` through the daemon's decoupled-executor path: the synchronous
/// `submit_test` (records Pending, returns AcceptedTest) then the real dispatch
/// `drive_submitted_test` (the actual `nix build`), returning the terminal
/// outcome. This is the in-process witness of what the `TestJobs` actor runs.
fn submit_and_drive(engine: &mut SchemaRuntime, request: meta::TestRequest) -> meta::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async {
        let accepted = engine.submit_test(request).await;
        assert!(
            matches!(
                accepted,
                lojix::schema_runtime::TestSubmissionOutcome::Accepted(_)
            ),
            "submit must accept before the real dispatch runs, got {accepted:?}"
        );
        engine.drive_submitted_test().await
    })
}

fn check_test_request(node: &str) -> meta::TestRequest {
    meta::TestRequest::Check(meta::QuickCheck::new(vec![ordinary::NodeName::new(node)]))
}

/// The REAL hermetic proof (Unit 2b Done): `(Check [mercury])` actually
/// nix-builds `vm-mercury`, the durable row transitions to Passed with the
/// realised check out-path, and `(Query (ByTestRun …))` returns the Passed
/// record. Ignored because it runs `nix build` against the network.
#[test]
#[ignore = "ACTUALLY nix-builds vm-mercury (slow, hits the network); run with --ignored"]
fn hermetic_check_mercury_actually_builds_and_records_passed() {
    let mut engine = engine();
    let terminal = submit_and_drive(&mut engine, check_test_request("mercury"));
    assert!(
        matches!(terminal, meta::Output::Tested(_)),
        "the terminal reply is Tested, got {terminal:?}"
    );
    let runs = query_runs(&mut engine, "goldragon", "mercury");
    assert_eq!(runs.len(), 1, "exactly one run recorded");
    let record = &runs[0];
    assert_eq!(
        record.outcome,
        ordinary::TestOutcome::Passed,
        "the real nix-build of vm-mercury Passed"
    );
    assert_eq!(record.phase, ordinary::TestRunPhase::Completed);
    let closure = record
        .closure_path
        .as_ref()
        .expect("Passed carries the realised check out-path");
    assert!(
        closure.payload().starts_with("/nix/store/"),
        "the closure is a real store out-path, got {closure:?}"
    );
    eprintln!(
        "HERMETIC mercury Passed with out-path {}",
        closure.payload()
    );
}

/// The REAL FAILED proof (Unit 2b Done): a node whose `vm-<node>` check does not
/// exist / fails to build is recorded as `Failed(HermeticCheck)`, NOT faked as a
/// pass. Uses a bogus node name so the check attribute is absent and `nix build`
/// exits non-zero.
#[test]
#[ignore = "ACTUALLY runs `nix build` against a missing check (hits the network); run with --ignored"]
fn hermetic_check_missing_node_records_failed_not_faked() {
    let mut engine = engine();
    let terminal = submit_and_drive(&mut engine, check_test_request("no-such-node"));
    eprintln!("FAILED-case terminal reply: {terminal:?}");
    let runs = query_runs(&mut engine, "goldragon", "no-such-node");
    assert_eq!(runs.len(), 1);
    let record = &runs[0];
    assert_eq!(
        record.outcome,
        ordinary::TestOutcome::Failed(ordinary::FailureStage::HermeticCheck),
        "a failed nix-build is recorded Failed(HermeticCheck), never a faked pass"
    );
    assert_eq!(record.phase, ordinary::TestRunPhase::Failed);
    assert!(
        record.closure_path.is_none(),
        "a failed check has no closure"
    );
}

/// LIVE honesty (report 54 Unit 2b fix 1): the live deploy-into-VM + assert
/// chain is unimplemented, so a Live `(Run … Live)` is REJECTED at submit with
/// `LiveNotYetEnabled` — it never mints an accepted run, never records a row,
/// and so can never yield a `Passed` it did not earn. The synchronous submit
/// path is exercised directly (the up-front verdict the owner socket replies).
#[test]
fn live_run_is_rejected_at_submit_never_a_faked_pass() {
    use lojix::schema_runtime::TestSubmissionOutcome;

    let mut engine = engine();
    let request = meta::TestRequest::Run(meta::TestRun {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_selection: meta::NodeSelection::Nodes(vec![ordinary::NodeName::new("mercury")]),
        host_selection: ordinary::HostSelection::DefaultHost,
        test_mode: ordinary::TestMode::Live,
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let outcome = runtime.block_on(async { engine.submit_test(request).await });
    match outcome {
        TestSubmissionOutcome::Rejected(rejected) => assert_eq!(
            rejected.test_rejection_reason,
            meta::TestRejectionReason::LiveNotYetEnabled,
            "a Live submit is rejected LiveNotYetEnabled, not accepted"
        ),
        TestSubmissionOutcome::Accepted(accepted) => {
            panic!("a Live run must NOT be accepted, got {accepted:?}")
        }
    }
    // Nothing was recorded — a rejected submit leaves no row to fake a pass.
    assert!(
        query_runs(&mut engine, "goldragon", "mercury").is_empty(),
        "a rejected Live submit records no test-run row"
    );
}

/// The fully-online HERMETIC proof (Unit 2b Done): spawn the actual
/// `lojix-daemon`, submit `(Test (Check [mercury]))` over the REAL owner socket
/// (the same wire path `meta-lojix` uses), let the decoupled `TestJobs` actor
/// ACTUALLY nix-build vm-mercury, then poll `(Query (ByTestRun …))` over the
/// REAL ordinary socket until the durable row reaches Passed with the realised
/// out-path. Exercises the daemon process, rkyv config, two-socket bind, frame
/// codec, the decoupled test-job executor, real `nix` IO, and the durable query.
#[test]
#[ignore = "spawns lojix-daemon, binds sockets, ACTUALLY nix-builds vm-mercury; run with --ignored"]
fn daemon_socket_roundtrip_hermetic_check_mercury_passes() {
    use std::io::ErrorKind;
    use std::os::unix::net::UnixStream;
    use std::process::Command;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use triad_runtime::{FrameBody, LengthPrefixedCodec};

    let dir = tempfile::tempdir().expect("tempdir");
    let ordinary_socket = dir.path().join("ordinary.sock");
    let owner_socket = dir.path().join("owner.sock");
    let configuration =
        write_test_daemon_configuration(dir.path(), &ordinary_socket, &owner_socket);

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration)
        .spawn()
        .expect("spawn lojix-daemon");

    let codec = LengthPrefixedCodec::default();
    let connect = |path: &std::path::Path, deadline: Instant, daemon: &mut std::process::Child| {
        loop {
            match UnixStream::connect(path) {
                Ok(stream) => return stream,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::NotFound | ErrorKind::ConnectionRefused
                    ) =>
                {
                    if let Ok(Some(status)) = daemon.try_wait() {
                        panic!("daemon exited early with {status}");
                    }
                    if Instant::now() > deadline {
                        daemon.kill().ok();
                        panic!("socket never became connectable: {}", path.display());
                    }
                    sleep(Duration::from_millis(50));
                }
                Err(error) => {
                    daemon.kill().ok();
                    panic!("connect failed: {error}");
                }
            }
        }
    };

    // 1. (Test (Check [mercury])) over the REAL owner socket → AcceptedTest.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut owner = connect(&owner_socket, deadline, &mut daemon);
    let test_input = meta::Input::Test(meta::Test::new(check_test_request("mercury")));
    let frame = FrameBody::new(test_input.encode_signal_frame().expect("encode test"));
    codec.write_body(&mut owner, &frame).expect("write test");
    let reply = codec.read_body(&mut owner).expect("read test reply");
    let (_, output) = meta::Output::decode_signal_frame(reply.bytes()).expect("decode test reply");
    let accepted = match output {
        meta::Output::Tested(accepted) => *accepted.payload().test_run_identifier.payload(),
        other => {
            daemon.kill().ok();
            panic!("expected AcceptedTest, got {other:?}");
        }
    };
    eprintln!("DAEMON accepted test run {accepted}");

    // 2. Poll (Query (ByTestRun (goldragon mercury None))) over the REAL
    //    ordinary socket until the decoupled dispatch reaches a terminal
    //    outcome — the real `nix build vm-mercury` runs on the daemon.
    let query = ordinary::Input::Query(ordinary::Query::new(ordinary::Selection::ByTestRun(
        ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("mercury"),
            run: None,
        },
    )));
    let query_frame = query.encode_signal_frame().expect("encode query");
    let build_deadline = Instant::now() + Duration::from_secs(540);
    let terminal = loop {
        let mut ordinary = connect(&ordinary_socket, deadline, &mut daemon);
        codec
            .write_body(&mut ordinary, &FrameBody::new(query_frame.clone()))
            .expect("write query");
        let reply = codec.read_body(&mut ordinary).expect("read query reply");
        let (_, output) =
            ordinary::Output::decode_signal_frame(reply.bytes()).expect("decode query reply");
        let runs = match output {
            ordinary::Output::TestRunsQueried(listing) => listing.into_payload().runs,
            other => {
                daemon.kill().ok();
                panic!("expected TestRunsQueried, got {other:?}");
            }
        };
        if let Some(record) = runs.into_iter().next()
            && !matches!(record.outcome, ordinary::TestOutcome::Pending)
        {
            break record;
        }
        if Instant::now() > build_deadline {
            daemon.kill().ok();
            panic!("the test never reached a terminal outcome");
        }
        sleep(Duration::from_millis(500));
    };

    daemon.kill().ok();
    daemon.wait().ok();

    assert_eq!(
        terminal.outcome,
        ordinary::TestOutcome::Passed,
        "the real daemon nix-built vm-mercury and recorded Passed"
    );
    let closure = terminal
        .closure_path
        .as_ref()
        .expect("Passed carries the realised out-path");
    assert!(
        closure.payload().starts_with("/nix/store/"),
        "the out-path is a real store path: {closure:?}"
    );
    eprintln!(
        "DAEMON SOCKET hermetic mercury Passed with out-path {}",
        closure.payload()
    );
}

/// Write the daemon's rkyv startup configuration for the integration proof:
/// both socket paths, a state dir, and the `TestDefaults` pointing at the
/// CriomOS-test-cluster `horizon-test-vm` flake (the auto-pickup `vm-<node>`
/// suite).
fn write_test_daemon_configuration(
    directory: &std::path::Path,
    ordinary_socket: &std::path::Path,
    owner_socket: &std::path::Path,
) -> std::path::PathBuf {
    let configuration = lojix::DaemonConfiguration {
        ordinary_socket_path: ordinary_socket.display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: owner_socket.display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: directory.join("state").display().to_string(),
        daemon_host: "ouranos".to_string(),
        test_defaults: Some(lojix::TestDefaults {
            cluster: "goldragon".to_string(),
            default_vm_host: "prometheus".to_string(),
            default_mode: lojix::TestMode::Hermetic,
            test_flake: "github:LiGoldragon/CriomOS-test-cluster/horizon-test-vm".to_string(),
            proposal_source: String::new(),
        }),
    };
    let path = directory.join("daemon-configuration.rkyv");
    configuration
        .write_rkyv_file(&path)
        .expect("write daemon configuration");
    path
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

// ---- Unit 2b fix 2: the host/node selection guarantee, proven ----
//
// The 2a/2b code wires `validate_host_for_node` / the `OnHost` rejection /
// `NodeUnknown` / the `All` sweep, but all of it is gated on a NON-EMPTY
// `proposal_source` that every other test leaves `""`. These tests configure a
// real proposal projection (the fixture cluster, `beacon` a Pod hosted on
// `atlas`, `atlas` bare metal) and prove the rejection guarantee end-to-end.

/// The host/node-selection fixture proposal (a `beacon` Pod hosted on `atlas`).
const HOST_SET_FIXTURE: &str = include_str!("fixtures/host-set-cluster.nota");

/// A fresh engine whose `TestDefaults` carry a real `proposal_source` — the
/// fixture proposal written to a tempfile — so `(OnHost …)`/`All` resolve and
/// validate against the projected host-sets. `default_vm_host` is `atlas` so an
/// `All` + `DefaultHost` run lands on a host every hosted Pod declares. Returns
/// the tempdir too so it outlives the engine (it owns the fixture file).
fn engine_with_projection() -> (tempfile::TempDir, SchemaRuntime) {
    let directory = tempfile::tempdir().expect("tempdir");
    let proposal_path = directory.path().join("cluster.nota");
    std::fs::write(&proposal_path, HOST_SET_FIXTURE).expect("write proposal fixture");
    let configuration = lojix::DaemonConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("owner.sock").display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: directory.path().join("state").display().to_string(),
        daemon_host: "ouranos".to_string(),
        test_defaults: Some(lojix::TestDefaults {
            cluster: "goldragon".to_string(),
            default_vm_host: "atlas".to_string(),
            default_mode: lojix::TestMode::Hermetic,
            test_flake: "github:LiGoldragon/CriomOS-test-cluster/horizon-test-vm".to_string(),
            proposal_source: proposal_path.display().to_string(),
        }),
    };
    let store = Arc::new(
        Store::open(directory.path().join("lojix.sema")).expect("open store over the projection"),
    );
    let engine = SchemaRuntime::with_store_and_configuration(
        store,
        Arc::new(RuntimeConfiguration::from_daemon_configuration(
            &configuration,
        )),
    );
    (directory, engine)
}

fn submit_verdict(engine: &mut SchemaRuntime, request: meta::TestRequest) -> nexus::RoutedReply {
    run(
        engine,
        nexus::RoutedMail::Meta(meta::Input::Test(meta::Test::new(request))),
    )
}

fn rejection_reason(output: nexus::RoutedReply) -> meta::TestRejectionReason {
    match meta_reply(output) {
        meta::Output::TestRejected(rejected) => rejected.into_payload().test_rejection_reason,
        other => panic!("expected TestRejected, got {other:?}"),
    }
}

fn host_run(node: &str, host: &str) -> meta::TestRequest {
    meta::TestRequest::Run(meta::TestRun {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_selection: meta::NodeSelection::Nodes(vec![ordinary::NodeName::new(node)]),
        host_selection: ordinary::HostSelection::OnHost(ordinary::NodeName::new(host)),
        test_mode: ordinary::TestMode::Hermetic,
    })
}

/// (a) A member `OnHost`: `beacon` declares host-set `{atlas}`, so a run
/// `(Run goldragon [beacon] (OnHost atlas) Hermetic)` is ACCEPTED against the
/// projection.
#[test]
fn member_on_host_is_accepted_against_the_projection() {
    let (_directory, mut engine) = engine_with_projection();
    accepted_identifier(submit_verdict(&mut engine, host_run("beacon", "atlas")));
    let runs = query_runs(&mut engine, "goldragon", "beacon");
    assert_eq!(runs.len(), 1, "the accepted member run is recorded");
    assert_eq!(
        runs[0].host.payload(),
        "atlas",
        "the validated member host is recorded"
    );
}

/// (b) A non-member `OnHost`: `prometheus` is not in `beacon`'s declared
/// host-set `{atlas}`, so the run is REJECTED `VmHostNotDeclaredForNode` — and
/// records no row.
#[test]
fn non_member_on_host_is_rejected_vm_host_not_declared() {
    let (_directory, mut engine) = engine_with_projection();
    let reason = rejection_reason(submit_verdict(
        &mut engine,
        host_run("beacon", "prometheus"),
    ));
    assert_eq!(
        reason,
        meta::TestRejectionReason::VmHostNotDeclaredForNode,
        "a host the node does not declare is rejected"
    );
    assert!(
        query_runs(&mut engine, "goldragon", "beacon").is_empty(),
        "a rejected run records no row"
    );
}

/// (c) An unknown node: `mars` is absent from the projection, so any run for it
/// is REJECTED `NodeUnknown`.
#[test]
fn unknown_node_is_rejected_node_unknown() {
    let (_directory, mut engine) = engine_with_projection();
    let reason = rejection_reason(submit_verdict(&mut engine, host_run("mars", "atlas")));
    assert_eq!(
        reason,
        meta::TestRejectionReason::NodeUnknown,
        "a node absent from the projection is rejected"
    );
}

/// (d) `All` resolves to the cluster's test-VM (hosted-Pod) nodes: only
/// `beacon` declares a `super_node`, so `All` + `DefaultHost` (= `atlas`)
/// resolves to and accepts exactly `[beacon]` — and the bare-metal `atlas` is
/// not swept.
#[test]
fn all_resolves_to_the_hosted_pod_nodes() {
    let (_directory, mut engine) = engine_with_projection();
    let request = meta::TestRequest::Run(meta::TestRun {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_selection: meta::NodeSelection::All,
        host_selection: ordinary::HostSelection::DefaultHost,
        test_mode: ordinary::TestMode::Hermetic,
    });
    // The submit admits this submit's FIRST resolved target; the fan-out tail
    // is the daemon's per-node loop. With one hosted Pod the first target IS
    // beacon, proving `All` projected the hosted-Pod set (not the bare-metal
    // atlas, and not an empty NodeUnknown rejection).
    accepted_identifier(submit_verdict(&mut engine, request));
    assert_eq!(
        query_runs(&mut engine, "goldragon", "beacon").len(),
        1,
        "All swept the hosted-Pod node beacon"
    );
    assert!(
        query_runs(&mut engine, "goldragon", "atlas").is_empty(),
        "All did not sweep the bare-metal atlas (no super_node)"
    );
}

/// The production posture, end to end: a daemon started with NO baked test
/// fixture (`test_defaults: None` — the `NoTestDefaults` writer form) REJECTS a
/// bare `(Check …)` with `NoTestDefaults` rather than resolving it against a
/// per-node baked cluster. This is the runtime half of the
/// deployment-independence decision; test fixtures live only in test code and a
/// production node bakes none.
#[test]
fn check_without_configured_defaults_is_rejected_no_test_defaults() {
    let directory = tempfile::tempdir().expect("tempdir");
    let configuration = lojix::DaemonConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("owner.sock").display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: directory.path().join("state").display().to_string(),
        daemon_host: "ouranos".to_string(),
        test_defaults: None,
    };
    let store = Arc::new(
        Store::open(directory.path().join("lojix.sema")).expect("open store without defaults"),
    );
    let mut engine = SchemaRuntime::with_store_and_configuration(
        store,
        Arc::new(RuntimeConfiguration::from_daemon_configuration(
            &configuration,
        )),
    );
    let reason = rejection_reason(run(&mut engine, check_request("mercury")));
    assert_eq!(
        reason,
        meta::TestRejectionReason::NoTestDefaults,
        "a production daemon with no baked fixture rejects a bare (Check …)"
    );
    assert!(
        query_runs(&mut engine, "goldragon", "mercury").is_empty(),
        "a rejected check records no test-run row"
    );
}
