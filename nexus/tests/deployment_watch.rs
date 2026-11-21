//! Process witnesses for the streaming deployment watch.
//!
//! A real `lojix-nexus` runs with isolated XDG roots and fake `nix` and `ssh`
//! programs first on its PATH, so a deploy it is given runs its whole
//! pipeline without a network. The ordinary client library — what the `lojix`
//! binary runs — writes one input and prints each reply as one line until the
//! Nexus closes the connection.

use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read as _};
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
use lojix::runtime_model as memory;
use lojix::{
    DeploymentLedger as _, DurableStore as _, GenerationLedger as _, NexusPersistable as _,
    Payload as _, Store,
};
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

const BIRD_CLOSURE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bird-home-manager";
const BIRD_FILES: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bird-home-manager-files";
const OWL_CLOSURE: &str = "/nix/store/cccccccccccccccccccccccccccccccc-owl-home-manager";
const OWL_FILES: &str = "/nix/store/dddddddddddddddddddddddddddddddd-owl-home-manager-files";
const CRANE_CLOSURE: &str = "/nix/store/gggggggggggggggggggggggggggggggg-crane-home-manager";
const CRANE_FILES: &str = "/nix/store/ffffffffffffffffffffffffffffffff-crane-home-manager-files";
const EAGLE_CLOSURE: &str = "/nix/store/hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh-eagle-home-manager";
const EAGLE_FILES: &str = "/nix/store/iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-eagle-home-manager-files";

fn user_identity(user: &str) -> memory::DeploymentRequestIdentity {
    memory::DeploymentRequestIdentity {
        deployment_environment: memory::DeploymentEnvironment::UserEnvironment(
            memory::UserName::from(user),
        ),
        cluster_name: memory::ClusterName::from("alpha"),
        node_name: memory::NodeName::from("beacon"),
        generation_artifact: memory::GenerationArtifact::UserEnvironment,
        requested_deployment_action: memory::RequestedDeploymentAction::UserEnvironment(
            memory::UserEnvironmentAction::ActivateNow,
        ),
        activation_effect: memory::ActivationEffect::LiveActivation,
        source_revision_policy: memory::SourceRevisionPolicy::RequireImmutable,
        optional_immutable_revision: Some(memory::ImmutableRevision::from(REVISION)),
    }
}

fn user_deploy_job(user: &str) -> memory::DeployJob {
    memory::DeployJob {
        deployment_identifier: memory::DeploymentIdentifier::new(0),
        generation_identifier: memory::GenerationIdentifier::new(0),
        cluster_name: memory::ClusterName::from("alpha"),
        node_name: memory::NodeName::from("beacon"),
        deploy_job_phase: memory::DeployJobPhase::Submitted,
        optional_closure_path: None,
        source_revision_policy: memory::SourceRevisionPolicy::RequireImmutable,
        flake_reference: memory::FlakeReference::from(FLAKE),
        optional_flake_reference: Some(memory::FlakeReference::from(FLAKE)),
        resolved_revision: Some(REVISION.to_string()),
        deployment_transport: memory::DeploymentTransport {
            nix_store_uri: memory::NixStoreUri::from("ssh-ng://fixture-target.invalid"),
            ssh_destination: memory::SshDestination::from(format!("{user}@fixture-target.invalid")),
        },
        deployment_input_mode: memory::DeploymentInputMode::Direct,
        deployment_output_selector: memory::DeploymentOutputSelector::new(
            memory::FlakeAttribute::from("packages.x86_64-linux.fixture-home"),
        ),
        activation_backend: memory::ActivationBackend::HomeManagerNixProfileV1,
        optional_nix_builder_spec: None,
        boot_once_unit: None,
        optional_generation_slot: Some(memory::GenerationSlot::Current),
        persisted_flake_input_override_vector: Vec::new(),
        deploy_resume_stage: memory::DeployResumeStage::ResolveFlakeAuth,
        optional_phase_receipt: None,
        optional_deploy_submission: None,
    }
}

fn user_retirement_identity(user: &str) -> memory::DeploymentRequestIdentity {
    memory::DeploymentRequestIdentity {
        deployment_environment: memory::DeploymentEnvironment::UserEnvironment(
            memory::UserName::from(user),
        ),
        cluster_name: memory::ClusterName::from("alpha"),
        node_name: memory::NodeName::from("beacon"),
        generation_artifact: memory::GenerationArtifact::UserEnvironment,
        requested_deployment_action: memory::RequestedDeploymentAction::UserEnvironmentRetirement,
        activation_effect: memory::ActivationEffect::UserEnvironmentRetirement,
        source_revision_policy: memory::SourceRevisionPolicy::NoFlakeSource,
        optional_immutable_revision: None,
    }
}

fn user_retirement_job(user: &str) -> memory::UserEnvironmentRetirementJob {
    memory::UserEnvironmentRetirementJob {
        deployment_identifier: memory::DeploymentIdentifier::new(0),
        generation_identifier: memory::GenerationIdentifier::new(0),
        cluster_name: memory::ClusterName::from("alpha"),
        node_name: memory::NodeName::from("beacon"),
        user_name: memory::UserName::from(user),
        deployment_transport: memory::DeploymentTransport {
            nix_store_uri: memory::NixStoreUri::from("ssh-ng://fixture-target.invalid"),
            ssh_destination: memory::SshDestination::from(format!("{user}@fixture-target.invalid")),
        },
    }
}

fn seed_user_environment(store: &Store, user: &str, closure: &str) -> u64 {
    let identity = user_identity(user);
    let record = store
        .allocate_deployment_record(identity.clone(), user_deploy_job(user))
        .expect("seed retained user deployment record");
    store
        .terminalize_deployment(
            *record.deployment_identifier.payload(),
            memory::DeploymentTerminal::Succeeded,
        )
        .expect("complete seed user deployment");
    let closure_path = memory::ClosurePath::from(closure);
    store
        .record_activation(
            memory::LiveGeneration {
                deployment_identifier: record.deployment_identifier.clone(),
                generation_identifier: record.generation_identifier.clone(),
                cluster_name: identity.cluster_name.clone(),
                node_name: identity.node_name.clone(),
                deployment_environment: identity.deployment_environment.clone(),
                generation_artifact: identity.generation_artifact,
                activation_effect: identity.activation_effect,
                generation_slot: memory::GenerationSlot::Current,
                closure_path: closure_path.clone(),
                source_revision_record: memory::SourceRevisionRecord {
                    source_revision_policy: memory::SourceRevisionPolicy::RequireImmutable,
                    requested_ref: memory::FlakeReference::from(FLAKE),
                    resolved_ref: memory::FlakeReference::from(FLAKE),
                    string: REVISION.to_string(),
                },
            },
            memory::GcRoot {
                generation_identifier: record.generation_identifier.clone(),
                cluster_name: identity.cluster_name,
                node_name: identity.node_name,
                generation_slot: memory::GenerationSlot::Current,
                closure_path,
                optional_pin_label: None,
            },
        )
        .expect("seed standalone current and durable root");
    *record.generation_identifier.payload()
}

fn seed_target_home(
    targets: &Path,
    user: &str,
    closure: &str,
    files: &str,
    state: &str,
    generation_identifier: u64,
) {
    let home = targets.join(user);
    let profile_directory = home.join(".local/state/nix/profiles");
    let unit_directory = home.join(".config/systemd/user");
    fs::create_dir_all(&profile_directory).expect("target profile directory");
    fs::create_dir_all(&unit_directory).expect("target user unit directory");
    fs::write(
        profile_directory.join(".lojix-profile-retirement.lock"),
        "fixture lock",
    )
    .expect("shared target lock");
    std::os::unix::fs::symlink(closure, profile_directory.join("home-manager"))
        .expect("standalone profile selector");
    std::os::unix::fs::symlink(closure, profile_directory.join("home-manager-7-link"))
        .expect("standalone profile generation");
    std::os::unix::fs::symlink(closure, profile_directory.join("profile-1-link"))
        .expect("unrelated profile generation");
    let keepalive = home.join(format!(
        ".local/state/lojix/gcroots/beacon/generation-{generation_identifier}"
    ));
    fs::create_dir_all(keepalive.parent().expect("keepalive parent"))
        .expect("independent target keepalive directory");
    std::os::unix::fs::symlink(closure, keepalive).expect("independent target keepalive root");
    fs::write(home.join("target-node"), "beacon\n").expect("target logical identity");
    fs::write(home.join("references"), format!("{files}\n")).expect("closure references");
    match state {
        "live" => {
            fs::write(home.join("manager-state"), "running\n").expect("running manager");
            fs::write(home.join("manager-status"), "0\n").expect("running status");
            fs::write(
                home.join("active-units"),
                "home-manager.service loaded active running Home Manager\n",
            )
            .expect("active standalone unit");
            fs::write(
                home.join("service-properties"),
                format!(
                    "FragmentPath={files}/systemd/user/home-manager.service\nDropInPaths=\nExecStart=\nMainPID=0\n"
                ),
            )
            .expect("standalone service provenance");
            std::os::unix::fs::symlink(
                format!("{files}/systemd/user/home-manager.service"),
                unit_directory.join("home-manager.service"),
            )
            .expect("effective standalone user unit");
        }
        "offline" => {
            fs::write(home.join("manager-state"), "offline\n").expect("offline manager");
            fs::write(home.join("manager-status"), "1\n").expect("offline status");
            fs::write(home.join("active-units"), "").expect("no active units");
            fs::write(home.join("service-properties"), "").expect("empty service properties");
        }
        "host-live" => {
            let host_unit = home.join("host-driven.service");
            fs::write(&host_unit, "[Service]\nExecStart=/usr/bin/sleep 1\n")
                .expect("host-driven service source");
            std::os::unix::fs::symlink(&host_unit, unit_directory.join("host-driven.service"))
                .expect("effective host-driven user unit");
            fs::write(home.join("manager-state"), "running\n").expect("running manager");
            fs::write(home.join("manager-status"), "0\n").expect("running status");
            fs::write(
                home.join("active-units"),
                "host.target loaded active active Host target\nhost-driven.service loaded active running Host service\n",
            )
            .expect("active host-driven units");
            fs::write(
                home.join("service-properties"),
                format!(
                    "FragmentPath={}\nDropInPaths=\nExecStart=\nMainPID=0\n",
                    host_unit.display()
                ),
            )
            .expect("host-driven unit provenance");
        }
        "unknown" => {
            fs::write(home.join("manager-state"), "unrecognized\n").expect("manager state");
            fs::write(home.join("manager-status"), "1\n").expect("manager status");
            fs::write(home.join("active-units"), "").expect("active units");
            fs::write(home.join("service-properties"), "").expect("service properties");
        }
        _ => unreachable!("known retirement fixture state"),
    }
}

fn make_target_environment_offline(targets: &Path, user: &str) {
    let home = targets.join(user);
    fs::write(home.join("manager-state"), "offline\n").expect("stop fixture user manager");
    fs::write(home.join("manager-status"), "1\n").expect("record fixture offline status");
    fs::write(home.join("active-units"), "").expect("clear fixture active units");
    fs::write(home.join("service-properties"), "").expect("clear fixture service properties");
    let standalone_unit = home.join(".config/systemd/user/home-manager.service");
    if fs::symlink_metadata(&standalone_unit).is_ok() {
        fs::remove_file(standalone_unit).expect("remove inactive standalone fixture unit");
    }
}

fn retirement_programs(directory: &Path, targets: &Path) {
    fake_programs(directory);
    let targets = targets.display();
    let real_readlink = std::env::split_paths(&std::env::var_os("PATH").expect("test PATH"))
        .map(|directory| directory.join("readlink"))
        .find(|candidate| candidate.is_file())
        .expect("real readlink on test PATH");
    write_executable(
        &directory.join("ssh"),
        &format!(
            "#!/bin/sh\nset -eu\ndestination=\"$3\"\nprintf '%s\\n' \"$destination\" >> '{programs}/ssh.log'\nuser=\"${{destination%%@*}}\"\nhome='{targets}/'$user\nremote=\"$4\"\ntest -d \"$home\"\nHOME=\"$home\" PATH='{programs}':\"$PATH\" bash -c \"$remote\"\n",
            programs = directory.display()
        ),
    );
    write_executable(
        &directory.join("hostname"),
        "#!/bin/sh\ncat \"$HOME/target-node\"\n",
    );
    write_executable(
        &directory.join("nix-store"),
        "#!/bin/sh\ncat \"$HOME/references\"\n",
    );
    write_executable(
        &directory.join("systemctl"),
        "#!/bin/sh\ncase \"$*\" in *'is-system-running'*) state=$(cat \"$HOME/manager-state\"); if [ \"$state\" = fail ]; then printf 'systemctl failed\\n'; exit 1; fi; cat \"$HOME/manager-state\"; exit \"$(cat \"$HOME/manager-status\")\";; *'list-units'*) cat \"$HOME/active-units\";; *'show'*) cat \"$HOME/service-properties\";; *) exit 90;; esac\n",
    );
    write_executable(
        &directory.join("readlink"),
        &format!(
            "#!/bin/sh\npath=\"$3\"\ncase \"$path\" in /nix/store/*) printf '%s\\n' \"$path\";; *) raw=$(\"{}\" -- \"$path\" 2>/dev/null) || raw=; case \"$raw\" in /nix/store/*) printf '%s\\n' \"$raw\";; *) exec '{}' -e -- \"$path\";; esac;; esac\n",
            real_readlink.display(),
            real_readlink.display()
        ),
    );
}

fn start_retirement_nexus() -> (RunningNexus, PathBuf, Vec<(String, u64)>, u64) {
    let directory = tempfile::tempdir().expect("temporary retirement roots");
    let runtime_root = directory.path().join("runtime");
    let state_root = directory.path().join("state");
    let targets = directory.path().join("targets");
    let programs = directory.path().join("programs");
    fs::create_dir_all(&runtime_root).expect("isolated runtime root");
    fs::create_dir_all(&state_root.join("lojix")).expect("isolated state root");
    fs::create_dir_all(&targets).expect("isolated target fixture root");
    retirement_programs(&programs, &targets);
    let store_path = state_root.join("lojix/lojix.memory");
    let store = Store::open_with_default_configuration(
        &store_path,
        lojix::NexusConfiguration {
            ordinary_socket_path: runtime_root
                .join("lojix/ordinary.sock")
                .display()
                .to_string(),
            ordinary_socket_mode: 0o660,
            owner_socket_path: runtime_root.join("lojix/meta.sock").display().to_string(),
            owner_socket_mode: 0o600,
            state_directory_path: state_root.join("lojix").display().to_string(),
            daemon_host: "localhost".to_string(),
            test_defaults_choice: signal_lojix::TestDefaultsChoice::NoTestDefaults,
        },
    )
    .expect("open fixture store");
    let seeded = vec![
        (
            "bird".to_string(),
            seed_user_environment(&store, "bird", BIRD_CLOSURE),
        ),
        (
            "owl".to_string(),
            seed_user_environment(&store, "owl", OWL_CLOSURE),
        ),
        (
            "crane".to_string(),
            seed_user_environment(&store, "crane", CRANE_CLOSURE),
        ),
        (
            "eagle".to_string(),
            seed_user_environment(&store, "eagle", EAGLE_CLOSURE),
        ),
        (
            "badger".to_string(),
            seed_user_environment(&store, "badger", BIRD_CLOSURE),
        ),
    ];
    for (user, closure, files, state) in [
        ("bird", BIRD_CLOSURE, BIRD_FILES, "host-live"),
        ("owl", OWL_CLOSURE, OWL_FILES, "live"),
        ("crane", CRANE_CLOSURE, CRANE_FILES, "unknown"),
        ("eagle", EAGLE_CLOSURE, EAGLE_FILES, "offline"),
        ("badger", BIRD_CLOSURE, BIRD_FILES, "offline"),
    ] {
        let generation_identifier = seeded
            .iter()
            .find(|(seeded_user, _)| seeded_user == user)
            .expect("each user has a seeded current generation")
            .1;
        seed_target_home(&targets, user, closure, files, state, generation_identifier);
    }
    let eagle_root = store
        .records::<memory::GcRoot>()
        .expect("read fixture roots before pinning")
        .into_iter()
        .find(|root| *root.generation_identifier.payload() == seeded[3].1)
        .expect("eagle root seeded");
    store
        .mutate_gc_root(memory::GcRoot {
            generation_identifier: eagle_root.generation_identifier,
            cluster_name: eagle_root.cluster_name,
            node_name: eagle_root.node_name,
            generation_slot: memory::GenerationSlot::Pinned,
            closure_path: eagle_root.closure_path,
            optional_pin_label: Some(memory::PinLabel::from("retirement-fixture")),
        })
        .expect("pin eagle root before retirement fixture");
    let interrupted_retirement = store
        .allocate_user_environment_retirement(
            user_retirement_identity("badger"),
            user_retirement_job("badger"),
        )
        .expect("persist interrupted retirement before starting fixture Nexus");
    let interrupted_retirement_identifier = *interrupted_retirement.deployment_identifier.payload();
    drop(store);

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
        .expect("start retirement fixture Nexus");
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
    (
        RunningNexus {
            child,
            ordinary_socket: PathBuf::from(&sockets[0]),
            meta_socket: PathBuf::from(&sockets[1]),
            log,
            directory,
        },
        targets,
        seeded,
        interrupted_retirement_identifier,
    )
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
        Ok(None) => {
            let status = daemon.try_wait().ok().flatten();
            let mut stderr = String::new();
            if let Some(mut child_stderr) = daemon.stderr.take() {
                let _ = child_stderr.read_to_string(&mut stderr);
            }
            panic!(
                "lojix-nexus closed its standard output without announcing readiness (status {status:?}): {stderr}"
            );
        }
        Err(_) => panic!("lojix-nexus never announced readiness"),
    }
}

fn stop_nexus_process(child: &mut Child) {
    let process_identifier =
        rustix::process::Pid::from_raw(child.id() as i32).expect("child process identifier");
    rustix::process::kill_process(process_identifier, rustix::process::Signal::TERM)
        .expect("send service-manager SIGTERM");
    let status = child.wait().expect("wait for daemon stop");
    assert!(status.success(), "lojix-nexus stopped with {status}");
}

fn stop_nexus(mut nexus: RunningNexus) {
    stop_nexus_process(&mut nexus.child);
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

fn retire_user_environment(user: &str) -> meta_signal_lojix::Query {
    meta_signal_lojix::Query::Retire(meta_signal_lojix::RetirementRequest::UserEnvironment(
        meta_signal_lojix::UserEnvironmentRetirement {
            cluster_name: "alpha".into(),
            node_name: "beacon".into(),
            user_name: user.to_string(),
            deployment_transport: signal_lojix::DeploymentTransport {
                nix_store_uri: "ssh-ng://fixture-target.invalid".into(),
                ssh_destination: format!("{user}@fixture-target.invalid"),
            },
        },
    ))
}

fn terminal_event_for(
    watching: &WatchingClient,
    identifier: i64,
) -> signal_lojix::DeploymentPhaseEvent {
    loop {
        let (reply, line) = next_reply(watching, "a retirement terminal event");
        assert!(
            line.starts_with("DeploymentPhaseObserved."),
            "retirement watch line: {line}"
        );
        let signal_lojix::Response::DeploymentPhaseObserved(event) = reply else {
            panic!("the ordinary watch returns phase observations")
        };
        if event.deployment_identifier == identifier && event.deployment_terminal_option.is_some() {
            return event;
        }
    }
}

fn deployment_query(nexus: &RunningNexus, identifier: i64) -> signal_lojix::GenerationListing {
    match ordinary_exchange(
        &nexus.ordinary_socket,
        signal_lojix::Query::Query(signal_lojix::Selection::ByDeployment(
            signal_lojix::DeploymentLookup {
                deployment_identifier: identifier,
            },
        )),
    ) {
        signal_lojix::Response::Queried(listing) => listing,
        other => panic!("expected a persisted deployment query response: {other:?}"),
    }
}

fn node_query(nexus: &RunningNexus) -> signal_lojix::GenerationListing {
    match ordinary_exchange(
        &nexus.ordinary_socket,
        signal_lojix::Query::Query(signal_lojix::Selection::ByNode(
            signal_lojix::NodeSelector {
                cluster_name: "alpha".into(),
                node_name: "beacon".into(),
                requested_generation_artifact_option: None,
            },
        )),
    ) {
        signal_lojix::Response::Queried(listing) => listing,
        other => panic!("expected ordinary by-node query response: {other:?}"),
    }
}

fn retire_and_observe(
    nexus: &RunningNexus,
    watching: &WatchingClient,
    user: &str,
) -> (i64, signal_lojix::DeploymentPhaseEvent) {
    let meta_signal_lojix::Response::DeployAccepted(accepted) =
        meta_exchange(&nexus.meta_socket, retire_user_environment(user))
    else {
        panic!("owner socket accepts retirement before the target check")
    };
    let identifier = accepted.deployment_identifier;
    let terminal = terminal_event_for(watching, identifier);
    (identifier, terminal)
}

#[test]
fn owner_socket_retirement_reaches_terminal_and_ordinary_query_watch_reads_it() {
    let (nexus, targets, seeded, interrupted_retirement_identifier) = start_retirement_nexus();
    let seeded_ids: std::collections::HashMap<_, _> = seeded.iter().cloned().collect();
    let interrupted_record = deployment_query(&nexus, interrupted_retirement_identifier as i64)
        .deployment_record_vector
        .into_iter()
        .find(|record| record.deployment_identifier == interrupted_retirement_identifier as i64)
        .expect("interrupted retirement failure remains queryable by its admitted id");
    assert!(interrupted_record.terminal_marker_option.is_some());
    match interrupted_record.deployment_terminal_option.as_ref() {
        Some(signal_lojix::DeploymentTerminal::Failed(failure)) => {
            assert_eq!(
                failure.deployment_failure_stage,
                signal_lojix::DeploymentFailureStage::Daemon
            );
            assert_eq!(
                failure.deployment_terminal_reason,
                signal_lojix::DeploymentTerminalReason::EnvironmentRetirementFailed
            );
            assert!(failure.failure_evidence_option.is_some());
        }
        other => panic!("startup records an interrupted failure, got {other:?}"),
    }
    let badger_profile = targets
        .join("badger")
        .join(".local/state/nix/profiles/home-manager");
    assert!(fs::symlink_metadata(&badger_profile).is_ok());
    assert!(
        fs::read_to_string(nexus.directory.path().join("programs/ssh.log"))
            .unwrap_or_default()
            .lines()
            .all(|destination| !destination.starts_with("badger@")),
        "startup never probes or mutates the interrupted target"
    );
    let current_before_retry = node_query(&nexus);
    assert!(
        current_before_retry
            .generation_vector
            .iter()
            .any(|generation| { generation.generation_identifier == seeded_ids["badger"] as i64 })
    );
    assert!(
        fs::symlink_metadata(targets.join("badger").join(format!(
            ".local/state/lojix/gcroots/beacon/generation-{}",
            seeded_ids["badger"]
        )))
        .is_ok()
    );
    let watching = watch(&nexus, deployment_watch("beacon"));
    let (opened, line) = next_reply(&watching, "retirement watch opening");
    let token = watching_token(&opened, &line);

    let cases = [
        (
            "bird",
            signal_lojix::DeploymentPhase::Completed,
            signal_lojix::DeploymentTerminal::Succeeded,
        ),
        (
            "owl",
            signal_lojix::DeploymentPhase::Rejected,
            signal_lojix::DeploymentTerminal::Rejected(
                signal_lojix::DeploymentTerminalReason::EnvironmentLive,
            ),
        ),
        (
            "crane",
            signal_lojix::DeploymentPhase::Failed,
            signal_lojix::DeploymentTerminal::Failed(signal_lojix::DeploymentFailure {
                deployment_failure_stage: signal_lojix::DeploymentFailureStage::Daemon,
                deployment_terminal_reason:
                    signal_lojix::DeploymentTerminalReason::EnvironmentRetirementFailed,
                failure_evidence_option: None,
            }),
        ),
        (
            "eagle",
            signal_lojix::DeploymentPhase::Rejected,
            signal_lojix::DeploymentTerminal::Rejected(
                signal_lojix::DeploymentTerminalReason::EnvironmentPinned,
            ),
        ),
        (
            "ghost",
            signal_lojix::DeploymentPhase::Rejected,
            signal_lojix::DeploymentTerminal::Rejected(
                signal_lojix::DeploymentTerminalReason::UnknownEnvironment,
            ),
        ),
    ];

    let mut retired_ids = Vec::new();
    for (user, expected_phase, expected_terminal) in cases {
        let (identifier, terminal) = retire_and_observe(&nexus, &watching, user);
        assert_eq!(terminal.deployment_phase, expected_phase);
        match (&terminal.deployment_terminal_option, &expected_terminal) {
            (
                Some(signal_lojix::DeploymentTerminal::Failed(actual)),
                signal_lojix::DeploymentTerminal::Failed(expected),
            ) => {
                assert_eq!(
                    actual.deployment_terminal_reason,
                    expected.deployment_terminal_reason
                );
            }
            (Some(actual), expected) => assert_eq!(actual, expected),
            (actual, expected) => {
                panic!("unexpected retirement terminal {actual:?}, expected {expected:?}")
            }
        }

        let listing = deployment_query(&nexus, identifier);
        let record = listing
            .deployment_record_vector
            .iter()
            .find(|record| record.deployment_identifier == identifier)
            .expect("terminal remains queryable by deployment id");
        match (&record.deployment_terminal_option, &expected_terminal) {
            (
                Some(signal_lojix::DeploymentTerminal::Failed(actual)),
                signal_lojix::DeploymentTerminal::Failed(expected),
            ) => assert_eq!(
                actual.deployment_terminal_reason,
                expected.deployment_terminal_reason
            ),
            (actual, expected) => assert_eq!(actual.as_ref(), Some(expected)),
        }
        assert!(record.terminal_marker_option.is_some());
        retired_ids.push(identifier);

        if seeded_ids.contains_key(user) {
            let profile_directory = targets.join(user).join(".local/state/nix/profiles");
            let selector = profile_directory.join("home-manager");
            let generation = profile_directory.join("home-manager-7-link");
            if user == "bird" {
                assert!(fs::symlink_metadata(&selector).is_err());
                assert!(fs::symlink_metadata(&generation).is_err());
            } else {
                assert!(fs::symlink_metadata(&selector).is_ok());
                assert!(fs::symlink_metadata(&generation).is_ok());
            }
            let keepalive = targets.join(user).join(format!(
                ".local/state/lojix/gcroots/beacon/generation-{}",
                seeded_ids[user]
            ));
            assert!(
                fs::symlink_metadata(keepalive).is_ok(),
                "retirement leaves the independently created target keepalive symlink alone"
            );
            assert!(
                fs::symlink_metadata(profile_directory.join("profile-1-link")).is_ok(),
                "retirement leaves another profile family untouched"
            );
        } else {
            assert!(
                fs::symlink_metadata(targets.join(user)).is_err(),
                "an unknown environment is rejected before any target path is touched"
            );
        }
    }

    let ssh_log = fs::read_to_string(nexus.directory.path().join("programs/ssh.log"))
        .expect("read fake target invocation log");
    assert!(ssh_log.contains("bird@fixture-target.invalid"));
    assert!(ssh_log.contains("owl@fixture-target.invalid"));
    assert!(ssh_log.contains("crane@fixture-target.invalid"));
    assert!(
        !ssh_log.contains("eagle@fixture-target.invalid"),
        "the pinned environment is rejected before any target connection: {ssh_log}"
    );
    assert!(
        !ssh_log.contains("badger@fixture-target.invalid"),
        "startup failure does not resume or probe a pending target operation: {ssh_log}"
    );
    assert!(
        !ssh_log.contains("ghost@fixture-target.invalid"),
        "unknown environment is rejected before target contact: {ssh_log}"
    );

    let current_before_retry = node_query(&nexus);
    let current_before_retry_ids: Vec<_> = current_before_retry
        .generation_vector
        .iter()
        .map(|generation| generation.generation_identifier)
        .collect();
    assert!(!current_before_retry_ids.contains(&(seeded_ids["bird"] as i64)));
    for user in ["owl", "crane", "eagle", "badger"] {
        assert!(
            current_before_retry_ids.contains(&(seeded_ids[user] as i64)),
            "{user} remains Current before its explicit retry"
        );
    }

    let mut retry_ids = Vec::new();
    for user in ["owl", "crane", "badger"] {
        make_target_environment_offline(&targets, user);
        let (identifier, terminal) = retire_and_observe(&nexus, &watching, user);
        assert_eq!(
            terminal.deployment_phase,
            signal_lojix::DeploymentPhase::Completed
        );
        assert_eq!(
            terminal.deployment_terminal_option,
            Some(signal_lojix::DeploymentTerminal::Succeeded)
        );
        let listing = deployment_query(&nexus, identifier);
        let record = listing
            .deployment_record_vector
            .iter()
            .find(|record| record.deployment_identifier == identifier)
            .expect("explicit retry remains queryable by deployment id");
        assert!(record.terminal_marker_option.is_some());
        assert_eq!(
            record.deployment_terminal_option,
            Some(signal_lojix::DeploymentTerminal::Succeeded)
        );
        retry_ids.push(identifier);

        let profile_directory = targets.join(user).join(".local/state/nix/profiles");
        assert!(fs::symlink_metadata(profile_directory.join("home-manager")).is_err());
        assert!(fs::symlink_metadata(profile_directory.join("home-manager-7-link")).is_err());
        assert!(fs::symlink_metadata(profile_directory.join("profile-1-link")).is_ok());
        let keepalive = targets.join(user).join(format!(
            ".local/state/lojix/gcroots/beacon/generation-{}",
            seeded_ids[user]
        ));
        assert!(fs::symlink_metadata(keepalive).is_ok());
    }
    retired_ids.extend(retry_ids);

    let by_node = node_query(&nexus);
    let generation_ids: Vec<_> = by_node
        .generation_vector
        .iter()
        .map(|generation| generation.generation_identifier)
        .collect();
    assert!(!generation_ids.contains(&(seeded_ids["bird"] as i64)));
    assert!(!generation_ids.contains(&(seeded_ids["owl"] as i64)));
    assert!(!generation_ids.contains(&(seeded_ids["crane"] as i64)));
    assert!(!generation_ids.contains(&(seeded_ids["badger"] as i64)));
    assert!(generation_ids.contains(&(seeded_ids["eagle"] as i64)));
    for identifier in retired_ids {
        assert!(
            by_node
                .deployment_record_vector
                .iter()
                .any(|record| record.deployment_identifier == identifier)
        );
    }

    assert_eq!(
        unwatch(&nexus, token),
        signal_lojix::Response::Unwatched(signal_lojix::SubscriptionClosed {
            subscription_token: token
        })
    );
    watching
        .ended
        .join()
        .expect("retirement watch client")
        .expect("watch closes cleanly after unwatch");
    let store_path = nexus.directory.path().join("state/lojix/lojix.memory");
    let mut nexus = nexus;
    stop_nexus_process(&mut nexus.child);
    let store = Store::open(store_path).expect("reopen retirement fixture Memory");
    let roots = store
        .records::<memory::GcRoot>()
        .expect("read durable roots");
    assert!(
        !roots
            .iter()
            .any(|root| *root.generation_identifier.payload() == seeded_ids["bird"])
    );
    assert!(roots.iter().any(|root| {
        *root.generation_identifier.payload() == seeded_ids["eagle"]
            && root.generation_slot == memory::GenerationSlot::Pinned
    }));
    for user in ["bird", "owl", "crane", "badger"] {
        assert!(
            !roots
                .iter()
                .any(|root| *root.generation_identifier.payload() == seeded_ids[user]),
            "successful retirement removes {user}'s Lojix root"
        );
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
