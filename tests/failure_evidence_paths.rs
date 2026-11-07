//! Every path to a failed deployment carries its evidence.
//!
//! The live record held `ActivationFailed` terminals whose detail was empty.
//! A failed command kept only its stderr, so a step that printed its cause on
//! stdout — Home Manager's `errorEcho` writes to stdout, and
//! `checkLinkTargets` prints a collision with it before `exit 1` — left
//! nothing; a process killed by a signal left no exit code and no statement;
//! and an activation handed to its own `systemd-run --wait` unit printed into
//! that unit's journal, so the record held only `systemd-run`'s summary.
//!
//! These tests drive the same submit/drive pipeline the daemon-owned job actor
//! drives, against hermetic fake `nix` and `ssh` programs, and read the
//! durable record.

use lojix::DurableStore as _;
use lojix::Payload as _;
use lojix::schema_runtime::DaemonRuntime as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_model as meta;
use lojix::runtime_model as ordinary;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};

mod common;

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const HOME_OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";
const SYSTEM_OUTPUT: &str = "/nix/store/cccccccccccccccccccccccccccccccc-nixos-system-beacon";
const NODE: &str = "beacon";
/// The durable detail bound the Nexus keeps.
const DETAIL_BOUND: usize = 4096;

/// What Home Manager's `checkLinkTargets` prints, on stdout, when a file is in
/// the way of a link it would create.
const COLLISION: &str = "Existing file '/home/bird/.config/herdr/config.toml' is in the way of '/nix/store/dddddddddddddddddddddddddddddddd-home-manager-files/.config/herdr/config.toml'";

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

/// Fake `nix` and `ssh`. Each logs its argv to `commands`, then runs the
/// given `case "$*"` arms before succeeding the way the pipeline expects.
fn fake_programs(directory: &Path, output: &str, nix_cases: &str, ssh_cases: &str) {
    fs::create_dir_all(directory).expect("create fake command directory");
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf 'nix %s\\n' \"$*\" >> \"$dir/commands\"\ncase \"$*\" in\n{nix_cases}\nesac\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-evidence-test=' ;;\n  eval) printf '%s\\n' '/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-generation.drv' ;;\n  build) printf '%s\\n' '{output}' ;;\nesac\n"
        ),
    );
    write_executable(
        &directory.join("ssh"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf 'ssh %s\\n' \"$*\" >> \"$dir/commands\"\ncase \"$*\" in\n{ssh_cases}\nesac\nexit 0\n"
        ),
    );
}

fn transport() -> ordinary::DeploymentTransport {
    ordinary::DeploymentTransport {
        nix_store_uri: ordinary::NixStoreUri::from("ssh-ng://fixture-copy.invalid"),
        ssh_destination: ordinary::SshDestination::from("root@fixture-activate.invalid"),
    }
}

fn user_environment_request(source: &Path) -> meta::DeploySubmission {
    meta::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::from("alpha"),
        node_name: ordinary::NodeName::from(NODE),
        user_name: ordinary::UserName::from("bird"),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        secrets_input: ordinary::SecretsInput::NoSecrets,
        flake_reference: ordinary::FlakeReference::from(FLAKE),
        deployment_transport: transport(),
        deployment_input_mode: ordinary::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::read_horizon(source)),
        deployment_output_selector: ordinary::DeploymentOutputSelector::new(
            ordinary::FlakeAttribute::from("homeConfigurations.bird.activationPackage"),
        ),
        activation_backend: ordinary::ActivationBackend::HomeManagerNixProfileV1,
        user_environment_action: ordinary::UserEnvironmentAction::ActivateNow,
        source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
        optional_nix_builder_spec: None,
        extra_substituter_vector: vec![],
    })
}

fn host_request(action: ordinary::HostDeployAction) -> meta::DeploySubmission {
    meta::DeploySubmission::Host(meta::HostDeployment {
        cluster_name: ordinary::ClusterName::from("alpha"),
        node_name: ordinary::NodeName::from(NODE),
        host_composition: ordinary::HostComposition::BaseHost,
        proposal_source: ordinary::ProposalSource::from("/dev/null"),
        secrets_input: ordinary::SecretsInput::NoSecrets,
        flake_reference: ordinary::FlakeReference::from(FLAKE),
        deployment_transport: transport(),
        deployment_input_mode: ordinary::DeploymentInputMode::Direct,
        horizon_definition_option: None,
        deployment_output_selector: ordinary::DeploymentOutputSelector::new(
            ordinary::FlakeAttribute::from(
                "nixosConfigurations.beacon.config.system.build.toplevel",
            ),
        ),
        activation_backend: ordinary::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: action,
        source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
        optional_nix_builder_spec: None,
        extra_substituter_vector: vec![],
    })
}

/// A Nexus running on `daemon_host`, its effects resolved in `programs`.
fn runtime(directory: &Path, programs: &Path, daemon_host: &str) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.memory")).expect("open test store"));
    let configuration = Arc::new(
        RuntimeConfiguration::test_with_effect_program_directory(
            directory.join("generated-inputs"),
            programs.to_path_buf(),
        )
        .with_daemon_host(ordinary::NodeName::from(daemon_host)),
    );
    SchemaRuntime::with_store_and_configuration(store, configuration)
}

/// What one driven deployment left: its terminal failure and the commands
/// the fakes saw.
struct Driven {
    failure: ordinary::DeploymentFailure,
    commands: Vec<String>,
}

async fn drive(directory: &Path, daemon_host: &str, request: meta::DeploySubmission) -> Driven {
    let programs = directory.join("programs");
    let mut engine = runtime(directory, &programs, daemon_host);
    match engine.submit_deploy(request) {
        lojix::schema_runtime::DeploySubmissionOutcome::Accepted(_) => {}
        other => panic!("fixture request was not accepted: {other:?}"),
    }
    let identifier = engine
        .active_deployment_identifier()
        .expect("an accepted deployment has its identifier");
    engine.drive_submitted_deploy().await;
    let record = engine
        .store()
        .records::<lojix::runtime_model::DeploymentRecord>()
        .expect("read durable deployment records")
        .into_iter()
        .find(|record| record.deployment_identifier == identifier)
        .expect("the driven deployment has a durable record");
    let failure = match record.optional_deployment_terminal {
        Some(ordinary::DeploymentTerminal::Failed(failure)) => failure,
        other => panic!("expected a failed terminal, got {other:?}"),
    };
    let commands = fs::read_to_string(programs.join("commands"))
        .expect("read fake command log")
        .lines()
        .map(str::to_owned)
        .collect();
    Driven { failure, commands }
}

async fn failed_user_environment(directory: &Path, activation_arm: &str) -> Driven {
    fake_programs(
        &directory.join("programs"),
        HOME_OUTPUT,
        "",
        &format!("  */activate*)\n{activation_arm}\n  ;;"),
    );
    let source = directory.join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    drive(directory, "daemon-host", user_environment_request(&source)).await
}

fn evidence(failure: &ordinary::DeploymentFailure) -> &ordinary::FailureEvidence {
    failure
        .optional_failure_evidence
        .as_ref()
        .expect("a failed deployment carries what the stage reported")
}

fn exit_code(evidence: &ordinary::FailureEvidence) -> Option<u64> {
    evidence
        .optional_failed_command
        .as_ref()
        .expect("a stage that ran a subprocess names it")
        .optional_exit_code
        .as_ref()
        .map(|code| *code.payload())
}

#[tokio::test]
async fn an_activation_that_prints_its_cause_on_stdout_keeps_it() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driven = failed_user_environment(
        directory.path(),
        &format!(
            "    printf '%s\\n' 'Starting Home Manager activation' 'Activating checkLinkTargets' \"{COLLISION}\" 'Please do one of the following:'\n    exit 1"
        ),
    )
    .await;
    assert_eq!(
        driven.failure.deployment_terminal_reason,
        ordinary::DeploymentTerminalReason::ActivationFailed
    );
    let evidence = evidence(&driven.failure);
    let detail = evidence.failure_detail.payload();
    assert_eq!(exit_code(evidence), Some(1));
    assert!(detail.contains("exited with status 1"), "got {detail:?}");
    assert!(detail.contains("--- stdout ---"), "got {detail:?}");
    assert!(detail.contains(COLLISION), "got {detail:?}");
    assert!(
        detail.contains("Activating checkLinkTargets"),
        "the step that failed must be recoverable, got {detail:?}"
    );
    assert!(
        detail.contains("--- stderr: empty ---"),
        "an empty stream is said to be empty, got {detail:?}"
    );
    assert!(!evidence.detail_truncated);
}

#[tokio::test]
async fn an_activation_killed_by_a_signal_says_so() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driven = failed_user_environment(
        directory.path(),
        "    printf '%s\\n' 'Activating linkGeneration'\n    kill -9 $$",
    )
    .await;
    let evidence = evidence(&driven.failure);
    let detail = evidence.failure_detail.payload();
    assert_eq!(
        exit_code(evidence),
        None,
        "a process killed by a signal has no exit code"
    );
    assert!(detail.contains("killed by signal 9"), "got {detail:?}");
    assert!(
        detail.contains("Activating linkGeneration"),
        "got {detail:?}"
    );
}

#[tokio::test]
async fn both_streams_keep_their_tails_within_the_bound() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driven = failed_user_environment(
        directory.path(),
        "    i=0; while [ $i -lt 400 ]; do printf 'stdout line %04d\\n' $i; printf 'stderr line %04d\\n' $i >&2; i=$((i+1)); done\n    exit 3",
    )
    .await;
    let evidence = evidence(&driven.failure);
    let detail = evidence.failure_detail.payload();
    assert!(detail.len() <= DETAIL_BOUND, "{} bytes", detail.len());
    assert!(evidence.detail_truncated, "a cut stream is a truncation");
    assert!(detail.contains("exited with status 3"), "got {detail:?}");
    assert!(detail.contains("--- stdout (tail) ---"), "got {detail:?}");
    assert!(detail.contains("--- stderr (tail) ---"), "got {detail:?}");
    assert!(detail.contains("stdout line 0399"), "got {detail:?}");
    assert!(detail.contains("stderr line 0399"), "got {detail:?}");
    assert!(
        detail.lines().all(|line| line.starts_with("stdout line ")
            || line.starts_with("stderr line ")
            || line.starts_with("--- ")
            || line == "exited with status 3"),
        "a stream's tail starts on a line boundary, got {detail:?}"
    );
}

#[tokio::test]
async fn a_tail_cut_inside_a_multibyte_character_is_kept_whole() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driven = failed_user_environment(
        directory.path(),
        "    i=0; while [ $i -lt 3001 ]; do printf 'é' >&2; i=$((i+1)); done\n    printf 'a' >&2\n    exit 1",
    )
    .await;
    let evidence = evidence(&driven.failure);
    let detail = evidence.failure_detail.payload();
    assert!(detail.len() <= DETAIL_BOUND, "{} bytes", detail.len());
    assert!(evidence.detail_truncated);
    assert!(
        detail.ends_with("éa"),
        "got the tail {:?}",
        &detail[detail.len() - 8..]
    );
}

/// A self-switch hands the switch to its own `systemd-run --wait` unit; what
/// the switch printed is in that unit's journal.
#[tokio::test]
async fn a_self_switch_that_fails_carries_its_unit_journal() {
    let directory = tempfile::tempdir().expect("tempdir");
    fake_programs(
        &directory.path().join("programs"),
        SYSTEM_OUTPUT,
        "",
        "  *journalctl*)\n    printf '%s\\n' 'activating the configuration...' 'Failed to install bootloader' 'error: switch-to-configuration exited with status 4'\n    exit 0 ;;\n  *systemd-run*)\n    printf '%s\\n' 'Running as unit: lojix-self-switch-deploy-1.service' '          Finished with result: exit-code' 'Main processes terminated with: code=exited, status=4/NOPERMISSION' >&2\n    exit 4 ;;",
    );
    let driven = drive(
        directory.path(),
        NODE,
        host_request(ordinary::HostDeployAction::ActivateNow),
    )
    .await;
    assert_eq!(
        driven.failure.deployment_failure_stage,
        ordinary::DeploymentFailureStage::Activate
    );
    let evidence = evidence(&driven.failure);
    let detail = evidence.failure_detail.payload();
    assert_eq!(exit_code(evidence), Some(4));
    let journal_read = driven
        .commands
        .iter()
        .find(|line| line.contains("journalctl"))
        .unwrap_or_else(|| panic!("the unit's journal was read, got {:?}", driven.commands));
    assert!(
        journal_read.contains("root@fixture-activate.invalid")
            && journal_read.contains("--unit=lojix-self-switch-deploy-"),
        "the journal is read on the unit's node, for that unit, got {journal_read:?}"
    );
    assert!(detail.contains("status=4/NOPERMISSION"), "got {detail:?}");
    assert!(
        detail.contains("--- journal of lojix-self-switch-deploy-"),
        "got {detail:?}"
    );
    assert!(
        detail.contains("Failed to install bootloader"),
        "got {detail:?}"
    );
}

#[tokio::test]
async fn a_boot_once_that_fails_carries_its_unit_journal() {
    let directory = tempfile::tempdir().expect("tempdir");
    fake_programs(
        &directory.path().join("programs"),
        SYSTEM_OUTPUT,
        "",
        "  *journalctl*)\n    printf '%s\\n' 'boot-once: no loader.conf default' \n    exit 0 ;;\n  *systemd-run*)\n    printf '%s\\n' 'Finished with result: exit-code' >&2\n    exit 1 ;;",
    );
    let driven = drive(
        directory.path(),
        NODE,
        host_request(ordinary::HostDeployAction::ScheduleBootOnce),
    )
    .await;
    let detail = evidence(&driven.failure).failure_detail.payload().clone();
    assert!(
        detail.contains("--- journal of lojix-boot-once-deploy-"),
        "got {detail:?}"
    );
    assert!(
        detail.contains("boot-once: no loader.conf default"),
        "got {detail:?}"
    );
}

#[tokio::test]
async fn an_unreadable_unit_journal_says_why() {
    let directory = tempfile::tempdir().expect("tempdir");
    fake_programs(
        &directory.path().join("programs"),
        SYSTEM_OUTPUT,
        "",
        "  *journalctl*)\n    printf '%s\\n' 'No journal files were opened due to insufficient permissions.' >&2\n    exit 1 ;;\n  *systemd-run*)\n    exit 4 ;;",
    );
    let driven = drive(
        directory.path(),
        NODE,
        host_request(ordinary::HostDeployAction::ActivateNow),
    )
    .await;
    let detail = evidence(&driven.failure).failure_detail.payload().clone();
    assert!(detail.contains("unavailable ---"), "got {detail:?}");
    assert!(
        detail.contains("insufficient permissions"),
        "got {detail:?}"
    );
}

/// Evaluation, build and copy fail through the same command carrier: each
/// keeps the command, its status, and both streams.
#[tokio::test]
async fn evaluation_build_and_copy_failures_keep_both_streams() {
    for (subcommand, stage, reason) in [
        (
            "eval",
            ordinary::DeploymentFailureStage::Eval,
            ordinary::DeploymentTerminalReason::EvaluationFailed,
        ),
        (
            "build",
            ordinary::DeploymentFailureStage::Build,
            ordinary::DeploymentTerminalReason::BuildFailed,
        ),
        (
            "copy",
            ordinary::DeploymentFailureStage::CopyClosure,
            ordinary::DeploymentTerminalReason::ClosureCopyFailed,
        ),
    ] {
        let directory = tempfile::tempdir().expect("tempdir");
        fake_programs(
            &directory.path().join("programs"),
            SYSTEM_OUTPUT,
            &format!(
                "  {subcommand}*)\n    printf '%s\\n' 'progress of {subcommand}'\n    printf '%s\\n' 'error: cause of {subcommand}' >&2\n    exit 1 ;;"
            ),
            "",
        );
        let driven = drive(
            directory.path(),
            NODE,
            host_request(ordinary::HostDeployAction::ActivateNow),
        )
        .await;
        assert_eq!(driven.failure.deployment_failure_stage, stage);
        assert_eq!(driven.failure.deployment_terminal_reason, reason);
        let evidence = evidence(&driven.failure);
        let detail = evidence.failure_detail.payload();
        let command = evidence
            .optional_failed_command
            .as_ref()
            .expect("the failed command is named");
        assert_eq!(command.command_program.payload(), "nix");
        assert_eq!(
            command
                .command_argument_vector
                .first()
                .map(|argument| argument.payload().as_str()),
            Some(subcommand)
        );
        assert_eq!(exit_code(evidence), Some(1));
        assert!(
            detail.contains(&format!("progress of {subcommand}")),
            "{subcommand}: got {detail:?}"
        );
        assert!(
            detail.contains(&format!("error: cause of {subcommand}")),
            "{subcommand}: got {detail:?}"
        );
    }
}
