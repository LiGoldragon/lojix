//! Focused deploy-transport witnesses using hermetic fake Nix and SSH programs.
//!
//! These tests execute the same submit/drive pipeline as the daemon-owned job
//! actor. They prove the production order without opening a network connection:
//! local immutable evaluation/build, target copy, root-mediated Home Manager
//! profile set, then target-user activation. They also prove bounded timeout
//! cancellation reaches a child process group and leaves a terminal job row.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use lojix::Store;
use lojix::schema::sema::DeployJobPhase;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str = "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";
const FIXTURE_PROPOSAL: &str = include_str!("fixtures/host-set-cluster.nota");

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

fn fake_programs(directory: &Path, fail_copy: bool, fail_activation: bool) {
    fs::create_dir_all(directory).expect("create fake command directory");
    let copy_failure = if fail_copy { "copy) exit 41 ;;" } else { "" };
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf 'nix' >> \"$dir/commands\"\nfor arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$dir/commands\"; done\nprintf '\\n' >> \"$dir/commands\"\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-transport-test=' ;;\n  eval) printf '%s\\n' '/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-home-manager-generation.drv' ;;\n  build) printf '%s\\n' '{OUTPUT}' ;;\n  {copy_failure}\nesac\n"
        ),
    );
    let activation_failure = if fail_activation {
        "case \"$*\" in */activate*) exit 42 ;; esac\n"
    } else {
        ""
    };
    write_executable(
        &directory.join("ssh"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf 'ssh' >> \"$dir/commands\"\nfor arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$dir/commands\"; done\nprintf '\\n' >> \"$dir/commands\"\n{activation_failure}exit 0\n"
        ),
    );
}

fn user_environment_request(source: &Path) -> meta::DeployRequest {
    meta::DeployRequest::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("beacon"),
        user_name: ordinary::UserName::new("bird"),
        source: ordinary::ProposalSource::new(source.display().to_string()),
        flake: ordinary::FlakeReference::new(FLAKE),
        user_environment_action: meta::UserEnvironmentAction::ActivateNow,
        source_revision_policy: meta::SourceRevisionPolicy::RequireImmutable,
        builder: None,
        substituters: Vec::new(),
    })
}

fn runtime(directory: &Path, programs: &Path, timeout: Duration) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.sema")).expect("open test store"));
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
        directory.join("generated-inputs"),
        programs.to_path_buf(),
        timeout,
    ));
    SchemaRuntime::with_store_and_configuration(store, configuration)
}

async fn submit_and_drive(
    engine: &mut SchemaRuntime,
    request: meta::DeployRequest,
) -> meta::Output {
    match engine.submit_deploy(request) {
        lojix::schema_runtime::DeploySubmissionOutcome::Accepted(_) => {}
        other => panic!("fixture request was not accepted: {other:?}"),
    }
    engine.drive_submitted_deploy().await
}

fn command_lines(programs: &Path) -> Vec<String> {
    fs::read_to_string(programs.join("commands"))
        .expect("read fake command log")
        .lines()
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn home_transport_is_local_build_then_copy_profile_and_activate_with_exact_identity() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("datom.nota");
    fs::write(&source, FIXTURE_PROPOSAL).expect("write fixture proposal");
    let mut engine = runtime(directory.path(), &programs, Duration::from_secs(2));

    assert!(matches!(
        submit_and_drive(&mut engine, user_environment_request(&source)).await,
        meta::Output::DeployAccepted(_)
    ));

    let commands = command_lines(&programs);
    let eval = commands
        .iter()
        .position(|line| line.starts_with("nix <eval>"))
        .expect("local eval");
    let build = commands
        .iter()
        .position(|line| line.starts_with("nix <build>"))
        .expect("local build");
    let copy = commands
        .iter()
        .position(|line| line.starts_with("nix <copy>"))
        .expect("closure copy");
    let profile = commands
        .iter()
        .position(|line| line.starts_with("ssh ") && line.contains("nix-env -p"))
        .expect("root-mediated profile set");
    let activate = commands
        .iter()
        .position(|line| line.starts_with("ssh ") && line.contains("/activate"))
        .expect("root-mediated activation");
    assert!(eval < build && build < copy && copy < profile && profile < activate);
    assert!(
        !commands[eval].contains("--store"),
        "eval must stay local: {}",
        commands[eval]
    );
    assert!(commands[eval].contains("narHash="), "{}", commands[eval]);
    assert!(commands[copy].contains(OUTPUT), "{}", commands[copy]);
    assert!(commands[copy].contains("ssh-ng://root@beacon.alpha.criome"));
    assert!(commands[profile].contains("runuser --login --command"));
    assert!(commands[profile].contains(OUTPUT));
    assert!(commands[activate].contains("runuser --login --command"));
    assert!(commands[activate].contains(OUTPUT));

    let generations = engine
        .store()
        .matching_live_generations(|_| true)
        .expect("read current generation");
    assert_eq!(generations.len(), 1);
    let generation = &generations[0];
    assert_eq!(generation.closure_path.payload(), OUTPUT);
    assert_eq!(
        generation.source_revision_record.policy,
        ordinary::SourceRevisionPolicy::RequireImmutable
    );
    assert_eq!(
        generation.source_revision_record.requested_ref.payload(),
        FLAKE
    );
    assert_eq!(
        generation.source_revision_record.resolved_ref.payload(),
        FLAKE
    );
    assert_eq!(
        generation.source_revision_record.resolved_revision,
        REVISION
    );
    assert_eq!(
        generation.generation_slot,
        ordinary::GenerationSlot::Current
    );
}

#[tokio::test]
async fn copy_and_activation_failures_are_terminal_rejections() {
    for (fail_copy, fail_activation, expected) in [
        (true, false, meta::DeployRejectionReason::BuilderUnreachable),
        (false, true, meta::DeployRejectionReason::ActivationFailed),
    ] {
        let directory = tempfile::tempdir().expect("tempdir");
        let programs = directory.path().join("programs");
        fake_programs(&programs, fail_copy, fail_activation);
        let source = directory.path().join("datom.nota");
        fs::write(&source, FIXTURE_PROPOSAL).expect("write fixture proposal");
        let mut engine = runtime(directory.path(), &programs, Duration::from_secs(2));

        match submit_and_drive(&mut engine, user_environment_request(&source)).await {
            meta::Output::DeployRejected(rejected) => {
                assert_eq!(rejected.payload().deploy_rejection_reason, expected)
            }
            other => panic!("expected terminal rejection, got {other:?}"),
        }
        let jobs = engine.store().deploy_jobs().expect("read durable job row");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].phase, DeployJobPhase::Failed);
    }
}

#[tokio::test]
async fn timeout_kills_the_whole_session_group_reaps_and_rejects() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fs::create_dir_all(&programs).expect("create fake command directory");
    write_executable(
        &programs.join("nix"),
        "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\n( trap 'touch \"$dir/descendant-terminated\"; exit 0' TERM; while :; do sleep 1; done ) &\necho $! > \"$dir/descendant-pid\"\nwhile :; do sleep 1; done\n",
    );
    let store = Arc::new(Store::open(directory.path().join("lojix.sema")).expect("open store"));
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
        directory.path().join("generated-inputs"),
        programs.clone(),
        Duration::from_millis(100),
    ));
    let mut engine = SchemaRuntime::with_store_and_configuration(store, configuration);
    let request = meta::DeployRequest::Host(meta::HostDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("beacon"),
        host_composition: ordinary::HostComposition::BaseHost,
        source: ordinary::ProposalSource::new("/dev/null"),
        flake: ordinary::FlakeReference::new(FLAKE),
        host_deploy_action: ordinary::HostDeployAction::Evaluate,
        source_revision_policy: meta::SourceRevisionPolicy::RequireImmutable,
        builder: None,
        substituters: Vec::new(),
        build_attribute: Some(meta::FlakeAttribute::new("fixture")),
    });

    match submit_and_drive(&mut engine, request).await {
        meta::Output::DeployRejected(_) => {}
        other => panic!("timeout must terminally reject, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        programs.join("descendant-terminated").exists(),
        "TERM must reach the descendant in the timed-out command's process group"
    );
    let jobs = engine.store().deploy_jobs().expect("read durable job row");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].phase, DeployJobPhase::Failed);
}
