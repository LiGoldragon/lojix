//! What the Nexus says when it cannot do what was asked.
//!
//! Three claims are witnessed here, each of which the engine previously made
//! falsely or not at all.
//!
//! A closure copy that fails reported `BuilderUnreachable`. `nix copy` engages
//! no builder, so that reason was false however the copy failed.
//!
//! A deploy-shaped refusal with no deployment behind it — the continuation
//! budget exhausted, a completion arriving with no correlated cursor, a
//! durable write failing before the record exists — aborted the daemon,
//! because the only deploy refusal on the wire carried a `DeploymentRecord`
//! and there was none to carry. It is now `DeployRefused`, which names what
//! happened and claims no deployment.
//!
//! A terminal record's lifecycle was passed in beside its terminal and the two
//! were free to disagree. The lifecycle is now read from the terminal, so they
//! cannot.

use lojix::Payload as _;
use lojix::runtime_flow::{Routable as _, Routed as _};
use lojix::schema_runtime::DaemonRuntime as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use lojix::{DeploymentLedger as _, DurableStore as _};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_flow as nexus;
use lojix::runtime_flow::{NexusEngine, OriginRoute};
use lojix::runtime_model as meta;
use lojix::runtime_model as ordinary;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};

mod common;

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";
/// What a real `nix copy` prints when it cannot reach the destination store.
const COPY_CAUSE: &str = "error: cannot open connection to remote store ssh-ng://fixture-copy";

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

/// A `nix` that resolves, evaluates and builds, then fails the copy — the one
/// stage under test — and an `ssh` that would succeed if it were ever reached.
fn programs_failing_the_copy(directory: &Path) {
    fs::create_dir_all(directory).expect("create fake command directory");
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-copy-test=' ;;\n  eval) printf '%s\\n' '/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-home-manager-generation.drv' ;;\n  build) printf '%s\\n' '{OUTPUT}' ;;\n  copy) printf '%s\\n' '{COPY_CAUSE}' >&2 ; exit 1 ;;\nesac\n"
        ),
    );
    write_executable(&directory.join("ssh"), "#!/bin/sh\nset -eu\nexit 0\n");
}

fn transport(nix_store_uri: &str, ssh_destination: &str) -> ordinary::DeploymentTransport {
    ordinary::DeploymentTransport {
        nix_store_uri: ordinary::NixStoreUri::new(nix_store_uri.into()),
        ssh_destination: ordinary::SshDestination::new(ssh_destination.into()),
    }
}

fn user_environment_request(source: &Path) -> meta::DeploySubmission {
    meta::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::new("alpha".into()),
        node_name: ordinary::NodeName::new("beacon".into()),
        user_name: ordinary::UserName::new("bird".into()),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        secrets_input: ordinary::SecretsInput::NoSecrets,
        flake_reference: ordinary::FlakeReference::new(FLAKE.into()),
        deployment_transport: transport(
            "ssh-ng://fixture-copy.invalid",
            "root@fixture-activate.invalid",
        ),
        deployment_input_mode: ordinary::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::read_horizon(source)),
        deployment_output_selector: ordinary::DeploymentOutputSelector::new(
            ordinary::FlakeAttribute::new("homeConfigurations.bird.activationPackage".into()),
        ),
        activation_backend: ordinary::ActivationBackend::HomeManagerNixProfileV1,
        user_environment_action: ordinary::UserEnvironmentAction::ActivateNow,
        source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
        nix_builder_spec_option: None,
        extra_substituter_vector: vec![],
    })
}

/// A daemon running on the deployed node, so the closure is built in its own
/// store and the copy stage really transfers it (the stage under test).
fn runtime(directory: &Path, programs: &Path) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.memory")).expect("open test store"));
    let configuration = Arc::new(
        RuntimeConfiguration::test_with_effect_program_directory(
            directory.join("generated-inputs"),
            programs.to_path_buf(),
        )
        .with_daemon_host(ordinary::NodeName::new("beacon".into())),
    );
    SchemaRuntime::with_store_and_configuration(store, configuration)
}

fn empty_runtime(directory: &Path) -> SchemaRuntime {
    let programs = directory.join("programs");
    fs::create_dir_all(&programs).expect("create fake command directory");
    runtime(directory, &programs)
}

fn admission_identity() -> ordinary::DeploymentRequestIdentity {
    ordinary::DeploymentRequestIdentity {
        deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
        cluster_name: ordinary::ClusterName::new("cluster".into()),
        node_name: ordinary::NodeName::new("node".into()),
        generation_artifact: ordinary::GenerationArtifact::BaseHost,
        requested_deployment_action: ordinary::RequestedDeploymentAction::Host(
            ordinary::HostDeployAction::Evaluate,
        ),
        activation_effect: ordinary::ActivationEffect::ProfileOnly,
        source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
        immutable_revision_option: Some(ordinary::ImmutableRevision::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        )),
    }
}

fn admission_job() -> ordinary::DeployJob {
    ordinary::DeployJob {
        deployment_identifier: ordinary::DeploymentIdentifier::new(0),
        generation_identifier: ordinary::GenerationIdentifier::new(0),
        cluster_name: ordinary::ClusterName::new("cluster".into()),
        node_name: ordinary::NodeName::new("node".into()),
        deploy_job_phase: ordinary::DeployJobPhase::Submitted,
        closure_path_option: None,
        source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
        flake_reference: ordinary::FlakeReference::new(
            "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        ),
        flake_reference_option: None,
        resolved_revision: None,
        deployment_transport: transport(
            "ssh-ng://fixture-copy.invalid",
            "fixture-login@fixture-activate.invalid",
        ),
        deployment_input_mode: ordinary::DeploymentInputMode::Direct,
        deployment_output_selector: ordinary::DeploymentOutputSelector::new(
            ordinary::FlakeAttribute::new("checks.fixture-a".into()),
        ),
        activation_backend: ordinary::ActivationBackend::NixosSystemdBootV1,
        nix_builder_spec_option: None,
        boot_once_unit: None,
        generation_slot_option: None,
        persisted_flake_input_override_vector: Vec::new(),
        deploy_resume_stage: ordinary::DeployResumeStage::ResolveFlakeAuth,
        phase_receipt_option: None,
        deploy_submission_option: None,
    }
}

fn terminal_failure(record: &ordinary::DeploymentRecord) -> &ordinary::DeploymentFailure {
    match record
        .deployment_terminal_option
        .as_ref()
        .expect("a driven deployment reaches a terminal")
    {
        ordinary::DeploymentTerminal::Failed(failure) => failure,
        other => panic!("expected a failed terminal, got {other:?}"),
    }
}

fn refusal(output: &meta::MetaEgress) -> &meta::RefusedDeploy {
    match output {
        meta::MetaEgress::DeployRefused(refused) => refused,
        other => panic!("expected an uncorrelated deploy refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_failed_closure_copy_names_the_copy_stage_and_not_a_builder() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    programs_failing_the_copy(&programs);
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
        ordinary::DeploymentFailureStage::CopyClosure,
        "the copy is what failed"
    );
    assert_eq!(
        failure.deployment_terminal_reason,
        ordinary::DeploymentTerminalReason::ClosureCopyFailed,
        "a copy engages no builder, so it cannot be a builder that was unreachable"
    );
    let evidence = failure
        .failure_evidence_option
        .as_ref()
        .expect("a failed copy carries what the stage reported");
    let command = evidence
        .failed_command_option
        .as_ref()
        .expect("a copy that ran a subprocess names it");
    assert_eq!(command.command_program.payload(), "nix");
    assert!(
        command
            .command_argument_vector
            .iter()
            .any(|argument| argument.payload() == "copy"),
        "the recorded command is the copy itself, got {:?}",
        command.command_argument_vector
    );
    assert!(
        evidence.failure_detail.payload().contains(COPY_CAUSE),
        "what the copy printed must be recoverable from the record, got {:?}",
        evidence.failure_detail.payload()
    );
}

#[tokio::test]
async fn an_effect_completion_with_no_deployment_behind_it_is_refused() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut engine = empty_runtime(directory.path());

    let work = nexus::NexusWork::EffectCompleted(nexus::EffectResult::EffectFailed(
        nexus::EffectFailure {
            effect_stage: nexus::EffectStage::CopyClosure,
            failure_evidence: ordinary::FailureEvidence {
                failed_command_option: None,
                failure_detail: ordinary::FailureDetail::new("an effect nobody asked for".into()),
                detail_truncated: ordinary::DetailTruncated(false),
            },
        },
    ))
    .with_origin_route(OriginRoute::new(0));

    let output = match engine.execute(work).await.into_root() {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => output,
        other => panic!("the runner must always terminate with a reply, got {other:?}"),
    };

    assert_eq!(
        refusal(&output).deploy_refusal_reason,
        ordinary::DeployRefusalReason::NoCorrelatedDeployment
    );
    assert!(
        engine
            .store()
            .deployment_records()
            .expect("read durable deployment records")
            .is_empty(),
        "a refusal that names no deployment must not invent one"
    );
}

#[tokio::test]
async fn driving_a_deploy_with_no_cursor_is_refused() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut engine = empty_runtime(directory.path());

    let output = engine.drive_submitted_deploy().await;

    assert_eq!(
        refusal(&output).deploy_refusal_reason,
        ordinary::DeployRefusalReason::NoCorrelatedDeployment
    );
}

#[test]
fn a_terminal_record_takes_its_lifecycle_from_its_terminal() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(directory.path().join("lojix.memory")).expect("open test store");

    for (terminal, expected_lifecycle) in [
        (
            ordinary::DeploymentTerminal::Succeeded,
            ordinary::DeploymentPhase::Completed,
        ),
        (
            ordinary::DeploymentTerminal::Failed(ordinary::DeploymentFailure {
                deployment_failure_stage: ordinary::DeploymentFailureStage::CopyClosure,
                deployment_terminal_reason: ordinary::DeploymentTerminalReason::ClosureCopyFailed,
                failure_evidence_option: None,
            }),
            ordinary::DeploymentPhase::Failed,
        ),
        (
            ordinary::DeploymentTerminal::Rejected(ordinary::DeploymentTerminalReason::NodeUnknown),
            ordinary::DeploymentPhase::Rejected,
        ),
    ] {
        let record = store
            .allocate_deployment_record(admission_identity(), admission_job())
            .expect("admit deployment");
        let identifier = *record.deployment_identifier.payload();
        let terminal_record = store
            .terminalize_deployment(identifier, terminal.clone())
            .expect("terminalize deployment");
        assert_eq!(
            terminal_record.deployment_phase, expected_lifecycle,
            "a {terminal:?} terminal is a {expected_lifecycle:?} deployment"
        );
        assert_eq!(
            terminal_record.deployment_terminal_option.as_ref(),
            Some(&terminal)
        );
    }
}
