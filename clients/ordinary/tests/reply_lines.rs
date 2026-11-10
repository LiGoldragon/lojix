//! The `lojix` binary prints each reply frame as one Datom line until the
//! Nexus closes the connection, and exits 0. It holds no knowledge of how many
//! replies an input has; a stand-in Nexus here decides that by when it closes.

use std::os::unix::net::UnixListener;
use std::process::Command;
use std::thread;

use datom_codec::Datomizable;
use lojix_client::ReplyLine as _;
use protos::{Protosizable, Textualizable};
use signal::{ByteViewable, FrameBody, FrameCapacity, FrameReading, FrameWriting, Signalizable};

fn observed(phase: signal_lojix::DeploymentPhase, terminal: bool) -> signal_lojix::Response {
    signal_lojix::Response::DeploymentPhaseObserved(signal_lojix::DeploymentPhaseEvent {
        deployment_identifier: 4,
        generation_identifier: 2,
        cluster_name: "alpha".into(),
        node_name: "beacon".into(),
        deployment_phase: phase,
        event_log_position: 9,
        transition_marker: signal_lojix::DatabaseMarker {
            commit_sequence: 3,
            state_digest: 3,
        },
        immutable_revision_option: None,
        deployment_terminal_option: terminal.then_some(signal_lojix::DeploymentTerminal::Succeeded),
    })
}

/// Run `lojix` against a stand-in Nexus that answers its one request with
/// `replies` and then closes; return the client's exit status and its lines.
fn run_against(replies: Vec<signal_lojix::Response>) -> (std::process::ExitStatus, Vec<String>) {
    let directory = tempfile::tempdir().expect("socket directory");
    let socket = directory.path().join("ordinary.sock");
    let listener = UnixListener::bind(&socket).expect("bind stand-in Nexus");
    let nexus = thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("accept the client");
        peer.read_frame(FrameCapacity::default())
            .expect("read the client's one request");
        for reply in replies {
            let body = reply.signalize().expect("archive reply");
            peer.write_frame(
                &FrameBody::from(body.bytes().to_vec()),
                FrameCapacity::default(),
            )
            .expect("write reply");
        }
    });
    let output = Command::new(env!("CARGO_BIN_EXE_lojix"))
        .env("LOJIX_ORDINARY_SOCKET", &socket)
        .arg(
            signal_lojix::Query::Unwatch(signal_lojix::SubscriptionClose {
                subscription_token: 1,
            })
            .datomize(vec![])
            .protosize()
            .textualize(),
        )
        .output()
        .expect("run lojix");
    nexus.join().expect("stand-in Nexus");
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout)
        .expect("utf-8 output")
        .lines()
        .map(str::to_owned)
        .collect();
    (output.status, lines)
}

#[test]
fn each_reply_is_one_line_until_the_nexus_closes() {
    let (status, lines) = run_against(vec![
        signal_lojix::Response::Watching(signal_lojix::SubscriptionOpened {
            subscription_token: 1,
            commit_sequence: 3,
        }),
        observed(signal_lojix::DeploymentPhase::Building, false),
        observed(signal_lojix::DeploymentPhase::Completed, true),
    ]);
    assert_eq!(status.code(), Some(0));
    assert_eq!(lines.len(), 3, "{lines:?}");
    assert!(lines[0].starts_with("Watching."), "{}", lines[0]);
    assert!(
        lines[1].starts_with("DeploymentPhaseObserved."),
        "{}",
        lines[1]
    );
    assert!(
        lines[2].starts_with("DeploymentPhaseObserved."),
        "{}",
        lines[2]
    );
}

#[test]
fn one_reply_and_a_close_is_one_line() {
    let unwatched = signal_lojix::Response::Unwatched(signal_lojix::SubscriptionClosed {
        subscription_token: 1,
    });
    let (status, lines) = run_against(vec![unwatched.clone()]);
    assert_eq!(status.code(), Some(0));
    assert_eq!(lines, vec![unwatched.line()]);
}
