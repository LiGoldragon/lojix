//! What a failed deployment leaves behind.
//!
//! A deployment that fails must record what actually failed. Until this
//! witness existed, Lojix collapsed every stage failure into one of eleven
//! generic reasons and discarded the subprocess stderr that named the cause,
//! so `Query.ByDeployment` could say which phase broke and never what broke.
//!
//! These tests drive the same submit/drive pipeline the daemon-owned job actor
//! drives, against hermetic fake `nix` and `ssh` programs, and assert on the
//! durable record and the event log — not on the reply the submitter happened
//! to receive.

use lojix::Payload as _;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_flow::{NexusEngine, OriginRoute};
use lojix::runtime_model as meta;
use lojix::runtime_model as ordinary;
use lojix::runtime_model as sema;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};

mod common;

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";

/// The line a real activation prints that names the substep that failed.
const ACTIVATION_CAUSE: &str = "Activating vscodiumManagedExtensions";
/// A line the durable record must never keep, planted in the same stderr.
const CREDENTIAL_LINE: &str = "using auth token ghp_ffffffffffffffffffffffffffffffffffff";

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

/// A `nix` that always succeeds and an `ssh` whose activation step fails with
/// a realistic multi-line stderr, one line of which names a credential.
fn fake_programs(directory: &Path) {
    fake_programs_with_activation(directory, true)
}

/// The same fixture, with the activation step succeeding, for the case where
/// the deployment must reach the live set.
fn succeeding_programs(directory: &Path) {
    fake_programs_with_activation(directory, false)
}

fn fake_programs_with_activation(directory: &Path, fail_activation: bool) {
    fs::create_dir_all(directory).expect("create fake command directory");
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-evidence-test=' ;;\n  eval) printf '%s\\n' '/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-home-manager-generation.drv' ;;\n  build) printf '%s\\n' '{OUTPUT}' ;;\nesac\n"
        ),
    );
    let activation_failure = if fail_activation {
        format!(
            "case \"$*\" in\n  */activate*)\n    printf '%s\\n' '{CREDENTIAL_LINE}' >&2\n    printf '%s\\n' '{ACTIVATION_CAUSE}' >&2\n    printf '%s\\n' 'Error: extensions are managed and inconsistent' >&2\n    exit 42 ;;\nesac\n"
        )
    } else {
        String::new()
    };
    write_executable(
        &directory.join("ssh"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf '%s\\n' \"$*\" >> \"$dir/commands\"\n{activation_failure}exit 0\n"
        ),
    );
}

fn transport(nix_store_uri: &str, ssh_destination: &str) -> ordinary::DeploymentTransport {
    ordinary::DeploymentTransport {
        nix_store_uri: ordinary::NixStoreUri::from(nix_store_uri),
        ssh_destination: ordinary::SshDestination::from(ssh_destination),
    }
}

fn user_environment_request(source: &Path) -> meta::DeploySubmission {
    meta::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::from("alpha"),
        node_name: ordinary::NodeName::from("beacon"),
        user_name: ordinary::UserName::from("bird"),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        secrets_input: ordinary::SecretsInput::NoSecrets,
        flake_reference: ordinary::FlakeReference::from(FLAKE),
        deployment_transport: transport(
            "ssh-ng://fixture-copy.invalid",
            "root@fixture-activate.invalid",
        ),
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

fn runtime(directory: &Path, programs: &Path) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.sema")).expect("open test store"));
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
        directory.join("generated-inputs"),
        programs.to_path_buf(),
    ));
    SchemaRuntime::with_store_and_configuration(store, configuration)
}

/// Drive one deployment whose activation fails, and hand back the engine and
/// the fake-command log for inspection.
async fn failed_user_environment_activation(
    directory: &Path,
) -> (SchemaRuntime, ordinary::DeploymentIdentifier, Vec<String>) {
    let programs = directory.join("programs");
    fake_programs(&programs);
    let source = directory.join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory, &programs);
    match engine.submit_deploy(user_environment_request(&source)) {
        lojix::schema_runtime::DeploySubmissionOutcome::Accepted(_) => {}
        other => panic!("fixture request was not accepted: {other:?}"),
    }
    let identifier = engine
        .active_deployment_identifier()
        .expect("an accepted deployment has its identifier");
    engine.drive_submitted_deploy().await;
    let commands = fs::read_to_string(programs.join("commands"))
        .expect("read fake command log")
        .lines()
        .map(str::to_owned)
        .collect();
    (engine, identifier, commands)
}

fn terminal_failure(record: &ordinary::DeploymentRecord) -> &ordinary::DeploymentFailure {
    match record
        .optional_deployment_terminal
        .as_ref()
        .expect("a driven deployment reaches a terminal")
    {
        ordinary::DeploymentTerminal::Failed(failure) => failure,
        other => panic!("expected a failed terminal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_failed_activation_names_the_command_and_its_exit_status() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (engine, identifier, _commands) =
        failed_user_environment_activation(directory.path()).await;

    let record = engine
        .store()
        .deployment_records()
        .expect("read durable deployment records")
        .into_iter()
        .find(|record| record.deployment_identifier == identifier)
        .expect("the driven deployment has a durable record");

    let failure = terminal_failure(&record);
    assert_eq!(
        failure.deployment_failure_stage,
        ordinary::DeploymentFailureStage::Activate
    );
    assert_eq!(
        failure.deployment_terminal_reason,
        ordinary::DeploymentTerminalReason::ActivationFailed
    );

    let evidence = failure
        .optional_failure_evidence
        .as_ref()
        .expect("a failed activation carries what the stage reported");
    let command = evidence
        .optional_failed_command
        .as_ref()
        .expect("an activation that ran a subprocess names it");
    assert_eq!(command.command_program.payload(), "ssh");
    assert_eq!(
        command
            .optional_exit_code
            .as_ref()
            .map(|code| *code.payload()),
        Some(42),
        "the exit status the activation actually reported"
    );
    assert!(
        evidence.failure_detail.payload().contains(ACTIVATION_CAUSE),
        "the substep that failed must be recoverable from the record, got {:?}",
        evidence.failure_detail.payload()
    );
}

#[tokio::test]
async fn the_durable_detail_drops_the_credential_line_it_was_printed_beside() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (engine, identifier, _commands) =
        failed_user_environment_activation(directory.path()).await;

    let record = engine
        .store()
        .deployment_records()
        .expect("read durable deployment records")
        .into_iter()
        .find(|record| record.deployment_identifier == identifier)
        .expect("the driven deployment has a durable record");
    let failure = terminal_failure(&record);
    let evidence = failure
        .optional_failure_evidence
        .as_ref()
        .expect("a failed activation carries what the stage reported");

    assert!(
        !evidence.failure_detail.payload().contains("ghp_"),
        "a credential line printed beside the cause must not become durable"
    );
    assert!(
        !evidence.failure_detail.payload().contains(CREDENTIAL_LINE),
        "a credential line printed beside the cause must not become durable"
    );
    assert!(
        evidence.detail_truncated,
        "dropping a line is a truncation, and the record must say so"
    );
}

#[tokio::test]
async fn the_event_log_carries_the_same_evidence_as_the_record() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (mut engine, identifier, _commands) =
        failed_user_environment_activation(directory.path()).await;

    let page = match engine
        .observe_sema_read(
            OriginRoute::new(0),
            sema::SemaReadInput::ReadEventLog(ordinary::EventLogRange {
                from: ordinary::EventLogPosition::new(0),
                until: ordinary::EventLogPosition::new(u64::MAX),
            }),
        )
        .await
    {
        sema::SemaReadOutput::EventLogRead(page) => page,
        other => panic!("expected an event-log page, got {other:?}"),
    };

    let terminal_event = page
        .deployment_phase_event_vector
        .iter()
        .find(|event| {
            event.deployment_identifier == identifier
                && event.optional_deployment_terminal.is_some()
        })
        .expect("the terminal transition is journalled");
    let failure = match terminal_event
        .optional_deployment_terminal
        .as_ref()
        .expect("filtered on its presence")
    {
        ordinary::DeploymentTerminal::Failed(failure) => failure,
        other => panic!("expected a failed terminal in the journal, got {other:?}"),
    };
    let evidence = failure
        .optional_failure_evidence
        .as_ref()
        .expect("the journalled terminal carries the same evidence");
    assert!(evidence.failure_detail.payload().contains(ACTIVATION_CAUSE));
}

#[tokio::test]
async fn a_by_deployment_query_answers_with_that_deployment() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (mut engine, identifier, _commands) =
        failed_user_environment_activation(directory.path()).await;

    let listing = match engine
        .observe_sema_read(
            OriginRoute::new(0),
            sema::SemaReadInput::QueryGenerations(ordinary::Selection::ByDeployment(
                ordinary::DeploymentLookup::new(identifier.clone()),
            )),
        )
        .await
    {
        sema::SemaReadOutput::GenerationsQueried(listing) => listing,
        other => panic!("expected a generation listing, got {other:?}"),
    };

    let record = listing
        .deployment_record_vector
        .iter()
        .find(|record| record.deployment_identifier == identifier)
        .expect("a ByDeployment query answers with that deployment's record");
    let evidence = terminal_failure(record)
        .optional_failure_evidence
        .as_ref()
        .expect("the queried record carries the evidence");
    assert!(evidence.failure_detail.payload().contains(ACTIVATION_CAUSE));
}

/// The ledger says the deployment failed at Activate. It does not say the
/// target was left untouched: the profile-set step ran and succeeded before
/// activation failed. These are two different facts and the test keeps them
/// apart, because a retry that assumes the target is unchanged is unsafe.
#[tokio::test]
async fn the_profile_advanced_although_the_deployment_failed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (engine, identifier, commands) = failed_user_environment_activation(directory.path()).await;

    assert!(
        commands
            .iter()
            .any(|line| line.contains("nix-env") || line.contains("--set")),
        "the fixture must reach the profile-set step, got {commands:?}"
    );
    assert!(
        commands.iter().any(|line| line.contains("activate")),
        "the fixture must reach the activation step, got {commands:?}"
    );

    let record = engine
        .store()
        .deployment_records()
        .expect("read durable deployment records")
        .into_iter()
        .find(|record| record.deployment_identifier == identifier)
        .expect("the driven deployment has a durable record");
    assert_eq!(
        record.deployment_lifecycle,
        ordinary::DeploymentLifecycle::Failed,
        "Lojix ledger state is Failed while the target's profile has moved"
    );
    assert!(
        engine
            .store()
            .matching_live_generations(|live| live.deployment_identifier == identifier)
            .expect("read the live set")
            .is_empty(),
        "a failed activation must not enter the live set, however far the \
         target itself advanced"
    );
}

/// A documented selector that always answered with nothing is not a selector.
/// `Query.ByDeployment` matched deployment records but never the generation
/// the deployment produced, so the one question an operator asks after a
/// deployment — what did it put on the node — had no answer.
#[tokio::test]
async fn a_by_deployment_query_answers_with_the_generation_it_produced() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    succeeding_programs(&programs);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);
    match engine.submit_deploy(user_environment_request(&source)) {
        lojix::schema_runtime::DeploySubmissionOutcome::Accepted(_) => {}
        other => panic!("fixture request was not accepted: {other:?}"),
    }
    let identifier = engine
        .active_deployment_identifier()
        .expect("an accepted deployment has its identifier");
    engine.drive_submitted_deploy().await;

    let listing = match engine
        .observe_sema_read(
            OriginRoute::new(0),
            sema::SemaReadInput::QueryGenerations(ordinary::Selection::ByDeployment(
                ordinary::DeploymentLookup::new(identifier.clone()),
            )),
        )
        .await
    {
        sema::SemaReadOutput::GenerationsQueried(listing) => listing,
        other => panic!("expected a generation listing, got {other:?}"),
    };

    assert!(
        listing
            .generation_vector
            .iter()
            .any(|generation| generation.deployment_identifier == identifier),
        "a ByDeployment query must answer with that deployment's generation, \
         got {:?}",
        listing.generation_vector
    );
}
