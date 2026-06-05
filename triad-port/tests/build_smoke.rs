//! End-to-end build/evaluate smoke test for the new lojix stack.
//!
//! Drives a real `Deploy` through the engine's full Nexus pipeline
//! (SignalArrived -> RecordDeploySubmitted -> ResolveFlakeAuth -> Building ->
//! NixEval -> [NixBuild] -> Deployed) with REAL `nix` IO via
//! `std::process::Command`. The target is the self-contained fixture
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

/// A System deploy of the self-contained `dune` fixture closure, with the
/// `build_attribute` override naming the directly-buildable flake output (no
/// horizon materialization). The proposal source is unused on this path
/// (no projection), so it is a placeholder.
fn dune_system_deploy(action: ordinary::SystemAction) -> meta::Input {
    meta::Input::Deploy(meta::DeployRequest::System(meta::SystemDeployment {
        cluster_name: "goldragon".to_string(),
        node_name: "dune".to_string(),
        deployment_kind: ordinary::DeploymentKind::OsOnly,
        source: "/dev/null".to_string(),
        flake: FIXTURE_FLAKE.to_string(),
        system_action: action,
        builder: None,
        substituters: Vec::new(),
        build_attribute: Some(FIXTURE_ATTRIBUTE.to_string()),
    }))
}

/// Drive one meta `Input` through a fresh engine and return the meta reply.
fn drive(input: meta::Input) -> meta::Output {
    let mut engine = SchemaRuntime::new();
    let work = nexus::NexusWork::SignalArrived(nexus::SignalInput::MetaInput(input))
        .with_origin_route(nexus::OriginRoute(0));
    match engine.execute(work).into_root() {
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
                accepted.deployment_identifier, accepted.database_marker.commit_sequence,
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
                accepted.deployment_identifier, accepted.database_marker.commit_sequence,
            );
        }
        other => panic!("build did not reach Deployed: {other:?}"),
    }
}

/// The fully-online proof: spawn the actual `lojix-daemon` binary, let it bind
/// its two authority-tiered unix sockets, and round-trip an Eval deploy over
/// the real owner socket with the length-prefixed frame codec — the same wire
/// path the CLI uses. Exercises the daemon process, config decode (inline
/// NOTA), two-socket bind, frame codec, the full pipeline, and real `nix` IO.
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
    // Mode 432 == 0o660 (cluster-operator group), per the daemon's owned
    // surface. A top-level NOTA record decodes as a parenthesized positional
    // field list with NO type-name head (nota-config decodes the known type),
    // so: `([ordinary] mode [owner] mode)`.
    let config = format!(
        "([{}] 432 [{}] 432)",
        ordinary_socket.display(),
        owner_socket.display(),
    );

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_lojix-daemon"))
        .arg(&config)
        .spawn()
        .expect("spawn lojix-daemon");

    // Connect-retry until the owner socket is listening (or the daemon dies).
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut stream = loop {
        match UnixStream::connect(&owner_socket) {
            Ok(stream) => break stream,
            Err(error)
                if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) =>
            {
                if let Ok(Some(status)) = daemon.try_wait() {
                    panic!("daemon exited early with {status}");
                }
                if Instant::now() > deadline {
                    daemon.kill().ok();
                    panic!("owner socket never became connectable at {}", owner_socket.display());
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
    codec.write_body(&mut stream, &frame).expect("write request frame");
    let reply = codec.read_body(&mut stream).expect("read reply frame");
    let (_, output) = meta::Output::decode_signal_frame(reply.bytes()).expect("decode reply");

    daemon.kill().ok();
    daemon.wait().ok();

    match output {
        meta::Output::Deployed(accepted) => {
            eprintln!(
                "DAEMON SOCKET roundtrip reached Deployed: deployment {}",
                accepted.deployment_identifier,
            );
        }
        other => panic!("daemon socket roundtrip did not reach Deployed: {other:?}"),
    }
}
