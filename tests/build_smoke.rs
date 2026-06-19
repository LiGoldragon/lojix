//! End-to-end build/evaluate smoke test for the new lojix stack.
//!
//! Drives a real `Deploy` through the engine's full Nexus pipeline
//! (SignalArrived -> RecordDeploySubmitted -> ResolveFlakeAuth -> Building ->
//! NixEval -> [NixBuild] -> Deployed) with REAL `nix` IO via
//! `tokio::process::Command`. The target is the self-contained fixture
//! `github:LiGoldragon/CriomOS-test-cluster#dune-nspawn-toplevel` (a
//! `fixtureSystem "dune"` toplevel with its fixture horizon baked in), so it
//! needs no `--override-input` materialization — that is the deferred M3
//! production-cutover work (report 27).
//!
//! Both tests are `#[ignore]` because they hit the network and run `nix`. Run
//! explicitly:
//!   cargo test --test build_smoke -- --ignored --nocapture

use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::SchemaRuntime;
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;

const FIXTURE_FLAKE: &str = "github:LiGoldragon/CriomOS-test-cluster";
const FIXTURE_ATTRIBUTE: &str = "dune-nspawn-toplevel";

fn write_daemon_configuration(
    directory: &std::path::Path,
    ordinary_socket: &std::path::Path,
    owner_socket: &std::path::Path,
    owner_socket_mode: u32,
) -> std::path::PathBuf {
    let configuration = lojix::DaemonConfiguration {
        ordinary_socket_path: ordinary_socket.display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: owner_socket.display().to_string(),
        owner_socket_mode,
        state_directory_path: directory.join("state").display().to_string(),
        test_defaults: lojix::TestDefaults {
            cluster: "goldragon".to_string(),
            default_vm_host: "prometheus".to_string(),
            default_mode: lojix::TestMode::Hermetic,
            test_flake: "github:LiGoldragon/CriomOS-test-cluster".to_string(),
            proposal_source: String::new(),
            live_enabled: false,
            live_guest_ip: String::new(),
        },
    };
    let path = directory.join("daemon-configuration.rkyv");
    configuration
        .write_rkyv_file(&path)
        .expect("write daemon configuration");
    path
}

/// A System deploy of the self-contained `dune` fixture closure, with the
/// `build_attribute` override naming the directly-buildable flake output (no
/// horizon materialization). The proposal source is unused on this path
/// (no projection), so it is a placeholder.
fn dune_system_deploy(action: ordinary::SystemAction) -> meta::Input {
    meta::Input::Deploy(meta::Deploy::new(meta::DeployRequest::System(
        meta::SystemDeployment {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("dune"),
            deployment_kind: ordinary::DeploymentKind::OsOnly,
            source: ordinary::ProposalSource::new("/dev/null"),
            flake: ordinary::FlakeReference::new(FIXTURE_FLAKE),
            system_action: action,
            builder: None,
            substituters: Vec::new(),
            build_attribute: Some(meta::FlakeAttribute::new(FIXTURE_ATTRIBUTE)),
        },
    )))
}

/// Drive one meta `Input` through a fresh engine and return the meta reply.
fn drive(input: meta::Input) -> meta::Output {
    let mut engine = SchemaRuntime::new();
    let work = nexus::NexusWork::SignalArrived(nexus::SignalInput::MetaInput(input))
        .with_origin_route(nexus::OriginRoute::new(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(async { engine.execute(work).await.into_root() }) {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => output,
        other => panic!("expected a meta reply from the engine, got {other:?}"),
    }
}

#[test]
#[ignore = "hits the network and runs `nix eval`; run with --ignored"]
fn eval_dune_fixture_through_the_engine() {
    match drive(dune_system_deploy(ordinary::SystemAction::Eval)) {
        meta::Output::Deployed(accepted) => {
            eprintln!(
                "EVAL reached Deployed: deployment {} at commit {}",
                accepted.payload().deployment_identifier.payload(),
                accepted.payload().database_marker.commit_sequence.payload(),
            );
        }
        other => panic!("eval did not reach Deployed: {other:?}"),
    }
}

#[test]
#[ignore = "hits the network and BUILDS the closure via `nix build` (slow); run with --ignored"]
fn build_dune_fixture_through_the_engine() {
    match drive(dune_system_deploy(ordinary::SystemAction::Build)) {
        meta::Output::Deployed(accepted) => {
            eprintln!(
                "BUILD reached Deployed: deployment {} realised at commit {}",
                accepted.payload().deployment_identifier.payload(),
                accepted.payload().database_marker.commit_sequence.payload(),
            );
        }
        other => panic!("build did not reach Deployed: {other:?}"),
    }
}

/// The fully-online proof: spawn the actual `lojix-daemon` binary, let it bind
/// its two authority-tiered unix sockets, and round-trip an Eval deploy over
/// the real owner socket with the length-prefixed frame codec — the same wire
/// path the CLI uses. Exercises the daemon process, rkyv config decode,
/// two-socket bind, frame codec, the full pipeline, and real `nix` IO.
#[test]
#[ignore = "spawns the lojix-daemon binary, binds sockets, runs `nix eval`; run with --ignored"]
fn daemon_binary_socket_roundtrip_eval() {
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
        write_daemon_configuration(dir.path(), &ordinary_socket, &owner_socket, 0o660);

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration)
        .spawn()
        .expect("spawn lojix-daemon");

    // Connect-retry until the owner socket is listening (or the daemon dies).
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut stream = loop {
        match UnixStream::connect(&owner_socket) {
            Ok(stream) => break stream,
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
                    panic!(
                        "owner socket never became connectable at {}",
                        owner_socket.display()
                    );
                }
                sleep(Duration::from_millis(50));
            }
            Err(error) => {
                daemon.kill().ok();
                panic!("connecting to the owner socket failed: {error}");
            }
        }
    };

    let codec = LengthPrefixedCodec::default();
    let input = dune_system_deploy(ordinary::SystemAction::Eval);
    let frame = FrameBody::new(input.encode_signal_frame().expect("encode request"));
    codec
        .write_body(&mut stream, &frame)
        .expect("write request frame");
    let reply = codec.read_body(&mut stream).expect("read reply frame");
    let (_, output) = meta::Output::decode_signal_frame(reply.bytes()).expect("decode reply");

    daemon.kill().ok();
    daemon.wait().ok();

    match output {
        meta::Output::Deployed(accepted) => {
            eprintln!(
                "DAEMON SOCKET roundtrip reached Deployed: deployment {}",
                accepted.payload().deployment_identifier.payload(),
            );
        }
        other => panic!("daemon socket roundtrip did not reach Deployed: {other:?}"),
    }
}

/// The owner socket carries the privileged Deploy/Pin/Unpin/Retire surface, so
/// a configuration whose owner-socket mode would grant "other" access must be
/// refused at startup (audit R3) — the daemon exits instead of binding it.
#[test]
#[ignore = "spawns the lojix-daemon binary; run with --ignored"]
fn permissive_owner_socket_mode_is_refused() {
    use std::process::Command;

    let dir = tempfile::tempdir().expect("tempdir");
    let ordinary_socket = dir.path().join("ordinary.sock");
    let owner_socket = dir.path().join("owner.sock");
    // owner mode 0o666 grants other read+write — must be refused.
    let configuration =
        write_daemon_configuration(dir.path(), &ordinary_socket, &owner_socket, 0o666);
    let output = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration)
        .output()
        .expect("run lojix-daemon");

    assert!(
        !output.status.success(),
        "the daemon must refuse a permissive owner-socket mode",
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("other-access"),
        "expected an owner-mode rejection on stderr, got: {stderr}",
    );
}

/// A hostile oversized length prefix must be bounded-rejected (no ~4 GiB
/// allocation) AND must not take the daemon down — a subsequent valid request
/// still succeeds (audit R1).
#[test]
#[ignore = "spawns the lojix-daemon binary, sends a hostile frame; run with --ignored"]
fn oversized_frame_is_bounded_and_daemon_survives() {
    use std::io::{ErrorKind, Write};
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::process::{Child, Command};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use triad_runtime::{FrameBody, LengthPrefixedCodec};

    fn connect(path: &Path, daemon: &mut Child, deadline: Instant) -> UnixStream {
        loop {
            match UnixStream::connect(path) {
                Ok(stream) => return stream,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::NotFound | ErrorKind::ConnectionRefused
                    ) =>
                {
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
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let ordinary_socket = dir.path().join("ordinary.sock");
    let owner_socket = dir.path().join("owner.sock");
    let configuration =
        write_daemon_configuration(dir.path(), &ordinary_socket, &owner_socket, 0o660);
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration)
        .spawn()
        .expect("spawn lojix-daemon");

    let deadline = Instant::now() + Duration::from_secs(15);

    // Hostile frame: a 4-byte 0xFFFFFFFF length prefix (declaring ~4 GiB) and no
    // body. The bounded cap rejects it before any allocation; the daemon drops
    // the connection and stays up.
    {
        let mut hostile = connect(&owner_socket, &mut daemon, deadline);
        hostile.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).ok();
        // dropping `hostile` here closes the connection
    }

    // The daemon must still serve a valid request afterwards.
    let mut stream = connect(&owner_socket, &mut daemon, deadline);
    let codec = LengthPrefixedCodec::default();
    let input = dune_system_deploy(ordinary::SystemAction::Eval);
    let frame = FrameBody::new(input.encode_signal_frame().expect("encode request"));
    codec
        .write_body(&mut stream, &frame)
        .expect("write request");
    let reply = codec
        .read_body(&mut stream)
        .expect("read reply — daemon survived the hostile frame");
    let (_, output) = meta::Output::decode_signal_frame(reply.bytes()).expect("decode reply");

    daemon.kill().ok();
    daemon.wait().ok();

    assert!(
        matches!(output, meta::Output::Deployed(_)),
        "daemon should serve a valid request after a hostile frame, got {output:?}",
    );
}

/// The deployer must serve connections concurrently (intent 2alg): while a real
/// owner deploy (a multi-second `nix` eval) is in flight, an ordinary read on
/// the other socket must be answered promptly, not queued behind the deploy.
#[test]
#[ignore = "spawns the daemon, runs a real deploy concurrently with a query; run with --ignored"]
fn concurrent_requests_are_served_in_parallel() {
    use std::io::ErrorKind;
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};

    use triad_runtime::{FrameBody, LengthPrefixedCodec};

    fn connect(path: &Path, deadline: Instant) -> UnixStream {
        loop {
            match UnixStream::connect(path) {
                Ok(stream) => return stream,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::NotFound | ErrorKind::ConnectionRefused
                    ) =>
                {
                    if Instant::now() > deadline {
                        panic!("socket never became connectable: {}", path.display());
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Err(error) => panic!("connect failed: {error}"),
            }
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let ordinary_socket = dir.path().join("ordinary.sock");
    let owner_socket = dir.path().join("owner.sock");
    let configuration =
        write_daemon_configuration(dir.path(), &ordinary_socket, &owner_socket, 0o660);
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&configuration)
        .spawn()
        .expect("spawn lojix-daemon");

    let deadline = Instant::now() + Duration::from_secs(15);

    // Start a real owner deploy (Eval of the dune fixture — multiple seconds of
    // `nix`) on its own client thread; the daemon keeps the ordinary socket
    // responsive while the owner request is isolated behind its listener gate.
    let owner_socket_path = owner_socket.clone();
    let deploy = thread::spawn(move || {
        let mut stream = connect(&owner_socket_path, deadline);
        let codec = LengthPrefixedCodec::default();
        let input = dune_system_deploy(ordinary::SystemAction::Eval);
        let frame = FrameBody::new(input.encode_signal_frame().expect("encode deploy"));
        codec.write_body(&mut stream, &frame).expect("write deploy");
        let reply = codec.read_body(&mut stream).expect("read deploy reply");
        let (_, output) = meta::Output::decode_signal_frame(reply.bytes()).expect("decode deploy");
        matches!(output, meta::Output::Deployed(_))
    });

    // Let the deploy reach its `nix` work, then time an ordinary query: if the
    // daemon is concurrent it returns promptly; if serial it would block until
    // the deploy finishes.
    thread::sleep(Duration::from_millis(1500));
    let query_started = Instant::now();
    let mut query_stream = connect(&ordinary_socket, deadline);
    let codec = LengthPrefixedCodec::default();
    let query = ordinary::Input::Query(ordinary::Query::new(ordinary::Selection::ByNode(
        ordinary::NodeSelector {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            kind: None,
        },
    )));
    let frame = FrameBody::new(query.encode_signal_frame().expect("encode query"));
    codec
        .write_body(&mut query_stream, &frame)
        .expect("write query");
    let reply = codec
        .read_body(&mut query_stream)
        .expect("read query reply");
    let query_latency = query_started.elapsed();
    let (_, query_output) =
        ordinary::Output::decode_signal_frame(reply.bytes()).expect("decode query");

    let deployed = deploy.join().expect("deploy thread");
    daemon.kill().ok();
    daemon.wait().ok();

    assert!(
        matches!(query_output, ordinary::Output::Queried(_)),
        "expected a Queried reply, got {query_output:?}",
    );
    assert!(deployed, "the owner deploy should still reach Deployed");
    assert!(
        query_latency < Duration::from_secs(4),
        "the ordinary query must be served concurrently with the in-flight deploy, \
         but took {query_latency:?} (serial behaviour)",
    );
    eprintln!("CONCURRENT: ordinary query answered in {query_latency:?} while a deploy ran");
}
