//! Process witnesses for the streaming deployment watch.
//!
//! A real `lojix-nexus` runs with isolated XDG roots and fake `nix` and `ssh`
//! programs first on its PATH, so a deploy it is given runs its whole
//! pipeline without a network. The ordinary client library — what the `lojix`
//! binary runs — writes one input and prints each reply as one line until the
//! Nexus closes the connection.

use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use datom_codec::Datomizable;
use lojix::client::{NexusSocket as _, SocketExchange};
use lojix::daemon::NexusReadiness;
use lojix_client::{Client, Invocable as _, ReplyLine as _};
use protos::{Protosizable, Textualizable};
use signal::{ByteViewable, FrameBody, FrameCapacity, FrameReading, FrameWriting};
use signal::{Restorable as _, Signalizable as _};

#[path = "../../tests/common/mod.rs"]
mod common;

/// The backstop, and nothing else: every wait below ends on what the Nexus
/// says, and this bound only keeps a wedged Nexus from hanging the harness.
const BACKSTOP: Duration = Duration::from_secs(300);

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";
const DERIVATION: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-home-manager-generation.drv";

/// A running Nexus, the sockets it announced, and the lines of its log.
struct RunningNexus {
    child: Child,
    ordinary_socket: PathBuf,
    meta_socket: PathBuf,
    log: mpsc::Receiver<String>,
    directory: tempfile::TempDir,
}

/// One client watching: each reply it printed, as the typed reply and its
/// line, and the client's own end once the Nexus closes.
struct WatchingClient {
    replies: mpsc::Receiver<(signal_lojix::Response, String)>,
    ended: thread::JoinHandle<lojix::Result<()>>,
}

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

/// Fake `nix` and `ssh` that answer every step of a user-environment deploy
/// as succeeding.
fn fake_programs(directory: &Path) {
    fs::create_dir_all(directory).expect("create fake command directory");
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-watch-test=' ;;\n  eval) printf '%s\\n' '{DERIVATION}' ;;\n  build) printf '%s\\n' '{OUTPUT}' ;;\nesac\n"
        ),
    );
    write_executable(&directory.join("ssh"), "#!/bin/sh\nexit 0\n");
}

fn start_nexus() -> RunningNexus {
    let directory = tempfile::tempdir().expect("temporary isolation roots");
    let runtime_root = directory.path().join("runtime");
    let state_root = directory.path().join("state");
    let programs = directory.path().join("programs");
    fs::create_dir_all(&runtime_root).expect("isolated runtime root");
    fs::create_dir_all(&state_root).expect("isolated state root");
    fake_programs(&programs);
    let mut path = OsString::from(programs.as_os_str());
    if let Some(inherited) = std::env::var_os("PATH") {
        path.push(":");
        path.push(inherited);
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_lojix-nexus"))
        .env("XDG_RUNTIME_DIR", &runtime_root)
        .env("XDG_STATE_HOME", &state_root)
        .env("PATH", path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start zero-argument lojix-nexus");
    let sockets = announced_readiness(&mut child);
    let stderr = child.stderr.take().expect("piped Nexus standard error");
    let (logged, log) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr)
            .lines()
            .map_while(std::result::Result::ok)
        {
            let _ = logged.send(line);
        }
    });
    RunningNexus {
        child,
        ordinary_socket: PathBuf::from(&sockets[0]),
        meta_socket: PathBuf::from(&sockets[1]),
        log,
        directory,
    }
}

fn announced_readiness(daemon: &mut Child) -> Vec<String> {
    let stdout = daemon.stdout.take().expect("piped Nexus standard output");
    let (announced, arrival) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout)
            .lines()
            .map_while(std::result::Result::ok)
        {
            if line.starts_with(<lojix::NexusConfiguration as NexusReadiness>::READY) {
                let _ = announced.send(Some(line));
                return;
            }
        }
        let _ = announced.send(None);
    });
    match arrival.recv_timeout(BACKSTOP) {
        Ok(Some(line)) => line
            .trim_start_matches('(')
            .trim_end_matches(')')
            .split_whitespace()
            .skip(1)
            .map(str::to_owned)
            .collect(),
        Ok(None) => panic!("lojix-nexus closed its standard output without announcing readiness"),
        Err(_) => panic!("lojix-nexus never announced readiness"),
    }
}

fn stop_nexus(mut nexus: RunningNexus) {
    let process_identifier =
        rustix::process::Pid::from_raw(nexus.child.id() as i32).expect("child process identifier");
    rustix::process::kill_process(process_identifier, rustix::process::Signal::TERM)
        .expect("send service-manager SIGTERM");
    let status = nexus.child.wait().expect("wait for daemon stop");
    assert!(status.success(), "lojix-nexus stopped with {status}");
}

/// The input as the `lojix` binary takes it: one inline Datom argument.
fn client(query: &signal_lojix::Query) -> Client {
    let text = query.clone().datomize(vec![]).protosize().textualize();
    Client::from_arguments([OsString::from(text)]).expect("the client reads its inline datom")
}

fn watch(nexus: &RunningNexus, query: signal_lojix::Query) -> WatchingClient {
    let socket = SocketExchange::at_path(nexus.ordinary_socket.display().to_string());
    let client = client(&query);
    let (printed, replies) = mpsc::channel();
    let ended = thread::spawn(move || {
        client.run(&socket, &mut |reply| {
            let line = reply.line();
            let _ = printed.send((reply, line));
            Ok(())
        })
    });
    WatchingClient { replies, ended }
}

fn next_reply(watching: &WatchingClient, occasion: &str) -> (signal_lojix::Response, String) {
    watching
        .replies
        .recv_timeout(BACKSTOP)
        .unwrap_or_else(|_| panic!("no reply line for {occasion}"))
}

fn deployment_watch(node: &str) -> signal_lojix::Query {
    signal_lojix::Query::WatchDeployments(signal_lojix::DeploymentWatch {
        deployment_identifier_option: None,
        cluster_name_option: Some("alpha".into()),
        node_name_option: Some(node.into()),
    })
}

fn exchange(socket: &Path, bytes: Vec<u8>) -> Vec<u8> {
    let mut stream = UnixStream::connect(socket).expect("connect typed client");
    stream
        .write_frame(&FrameBody::from(bytes), FrameCapacity::default())
        .expect("write request");
    stream
        .read_frame(FrameCapacity::default())
        .expect("read response")
        .bytes()
        .to_vec()
}

fn ordinary_exchange(socket: &Path, request: signal_lojix::Query) -> signal_lojix::Response {
    let signal = request.signalize().expect("archive ordinary query");
    signal::Signal::<signal_lojix::Response>::from(exchange(socket, signal.bytes().to_vec()))
        .restore()
        .expect("restore ordinary response")
}

fn meta_exchange(socket: &Path, request: meta_signal_lojix::Query) -> meta_signal_lojix::Response {
    let signal = request.signalize().expect("archive meta query");
    signal::Signal::<meta_signal_lojix::Response>::from(exchange(socket, signal.bytes().to_vec()))
        .restore()
        .expect("restore meta response")
}

fn unwatch(nexus: &RunningNexus, subscription_token: i64) -> signal_lojix::Response {
    ordinary_exchange(
        &nexus.ordinary_socket,
        signal_lojix::Query::Unwatch(signal_lojix::SubscriptionClose { subscription_token }),
    )
}

/// A user-environment deploy of `beacon` in cluster `alpha`, carrying its
/// Horizon definition.
fn user_environment_deploy(nexus: &RunningNexus) -> meta_signal_lojix::Query {
    let source = nexus.directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    meta_signal_lojix::Query::Deploy(meta_signal_lojix::ActualizedDeploySubmission {
        deploy_submission: meta_signal_lojix::DeploySubmission::UserEnvironment(
            meta_signal_lojix::UserEnvironmentDeployment {
                cluster_name: "alpha".into(),
                node_name: "beacon".into(),
                user_name: "bird".into(),
                proposal_source: source.display().to_string(),
                secrets_input: signal_lojix::SecretsInput::NoSecrets,
                flake_reference: FLAKE.into(),
                deployment_transport: signal_lojix::DeploymentTransport {
                    nix_store_uri: "ssh-ng://root@fixture-target.invalid".into(),
                    ssh_destination: "root@fixture-target.invalid".into(),
                },
                deployment_input_mode: signal_lojix::DeploymentInputMode::Horizon,
                deployment_output_selector: signal_lojix::DeploymentOutputSelector {
                    flake_attribute: "packages.x86_64-linux.fixture-home".into(),
                },
                activation_backend: signal_lojix::ActivationBackend::HomeManagerNixProfileV1,
                user_environment_action: signal_lojix::UserEnvironmentAction::ActivateNow,
                source_revision_policy: signal_lojix::SourceRevisionPolicy::RequireImmutable,
                nix_builder_spec_option: None,
                extra_substituter_vector: Vec::new(),
            },
        ),
        horizon_definition_option: Some(common::read_horizon(&source)),
    })
}

fn watching_token(reply: &signal_lojix::Response, line: &str) -> i64 {
    assert!(line.starts_with("Watching."), "first line: {line}");
    match reply {
        signal_lojix::Response::Watching(opened) => opened.subscription_token,
        other => panic!("a watch opens with Watching, got {other:?}"),
    }
}

#[test]
fn a_watch_streams_watching_then_each_phase_of_a_deployment_through_its_terminal() {
    let nexus = start_nexus();
    let watching = watch(&nexus, deployment_watch("beacon"));
    let elsewhere = watch(&nexus, deployment_watch("atlas"));
    let (reply, line) = next_reply(&watching, "the beacon watch opening");
    let token = watching_token(&reply, &line);
    // The lines as `lojix` prints them, shown in the check's log.
    println!("{line}");
    let (reply, line) = next_reply(&elsewhere, "the atlas watch opening");
    let elsewhere_token = watching_token(&reply, &line);

    let meta_signal_lojix::Response::DeployAccepted(accepted) =
        meta_exchange(&nexus.meta_socket, user_environment_deploy(&nexus))
    else {
        panic!("the fixture deploy is accepted")
    };

    let mut observed = Vec::new();
    loop {
        let (reply, line) = next_reply(&watching, "a phase event of the deploy");
        println!("{line}");
        assert!(
            line.starts_with("DeploymentPhaseObserved."),
            "each event is one observed line: {line}"
        );
        let signal_lojix::Response::DeploymentPhaseObserved(event) = reply else {
            panic!("a watch writes only observed events after Watching")
        };
        assert_eq!(event.deployment_identifier, accepted.deployment_identifier);
        let terminal = event.deployment_terminal_option.is_some();
        observed.push(event);
        if terminal {
            break;
        }
    }
    assert!(
        observed.len() >= 2,
        "a deploy run through to its terminal passes more than one phase: {observed:?}"
    );
    assert!(
        observed
            .windows(2)
            .all(|pair| pair[0].event_log_position < pair[1].event_log_position),
        "events arrive in commit order: {observed:?}"
    );
    let last = observed.last().expect("a terminal event");
    assert_eq!(
        last.deployment_phase,
        signal_lojix::DeploymentPhase::Completed
    );
    assert_eq!(
        last.deployment_terminal_option,
        Some(signal_lojix::DeploymentTerminal::Succeeded)
    );

    // Unwatch ends each subscription and the Nexus closes its connection: the
    // client's stream ends cleanly. The atlas watch saw no event of beacon.
    assert_eq!(
        unwatch(&nexus, token),
        signal_lojix::Response::Unwatched(signal_lojix::SubscriptionClosed {
            subscription_token: token
        })
    );
    watching
        .ended
        .join()
        .expect("watching client thread")
        .expect("the client ends cleanly when the Nexus closes");
    assert!(watching.replies.try_recv().is_err());
    assert!(matches!(
        unwatch(&nexus, elsewhere_token),
        signal_lojix::Response::Unwatched(_)
    ));
    elsewhere
        .ended
        .join()
        .expect("elsewhere client thread")
        .expect("the client ends cleanly when the Nexus closes");
    assert!(
        elsewhere.replies.try_recv().is_err(),
        "a watch of another node receives none of this deploy's events"
    );
    stop_nexus(nexus);
}

#[test]
fn an_ordinary_query_answers_one_line_and_the_nexus_closes() {
    let nexus = start_nexus();
    let socket = SocketExchange::at_path(nexus.ordinary_socket.display().to_string());
    let mut lines = Vec::new();
    client(&signal_lojix::Query::Query(
        signal_lojix::Selection::ByNode(signal_lojix::NodeSelector {
            cluster_name: "alpha".into(),
            node_name: "beacon".into(),
            requested_generation_artifact_option: None,
        }),
    ))
    .run(&socket, &mut |reply| {
        lines.push(reply.line());
        Ok(())
    })
    .expect("an ordinary query ends when the Nexus closes after its reply");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with("Queried."), "{}", lines[0]);
    stop_nexus(nexus);
}

#[test]
fn a_watcher_whose_peer_closes_is_dropped() {
    let nexus = start_nexus();
    let mut stream = UnixStream::connect(&nexus.ordinary_socket).expect("connect watcher");
    let request = deployment_watch("beacon")
        .signalize()
        .expect("archive watch");
    stream
        .write_frame(
            &FrameBody::from(request.bytes().to_vec()),
            FrameCapacity::default(),
        )
        .expect("write watch");
    let opened = stream
        .read_frame(FrameCapacity::default())
        .expect("read Watching");
    let signal_lojix::Response::Watching(opened) =
        signal::Signal::<signal_lojix::Response>::from(opened.bytes().to_vec())
            .restore()
            .expect("restore Watching")
    else {
        panic!("a watch opens with Watching")
    };
    let token = opened.subscription_token;
    assert_eq!(
        unwatch(&nexus, token + 1_000),
        signal_lojix::Response::UnwatchRejected(signal_lojix::RejectedUnwatch {
            unwatch_rejection_reason:
                signal_lojix::UnwatchRejectionReason::SubscriptionTokenUnknown,
            subscription_token: token + 1_000,
        })
    );

    drop(stream);
    let ended = format!("lojix-nexus: DeploymentWatchEnded.{{ {token} PeerClosed }}");
    loop {
        let line = nexus
            .log
            .recv_timeout(BACKSTOP)
            .expect("the Nexus logs the watch its peer closed");
        if line == ended {
            break;
        }
    }
    assert_eq!(
        unwatch(&nexus, token),
        signal_lojix::Response::UnwatchRejected(signal_lojix::RejectedUnwatch {
            unwatch_rejection_reason:
                signal_lojix::UnwatchRejectionReason::SubscriptionAlreadyClosed,
            subscription_token: token,
        }),
        "the dropped watcher is no longer live"
    );
    stop_nexus(nexus);
}
