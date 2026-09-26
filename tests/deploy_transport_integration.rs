//! Focused deploy-transport witnesses using hermetic fake Nix and SSH programs.
//!
//! These tests execute the same submit/drive pipeline as the daemon-owned job
//! actor. They prove the production order without opening a network connection.
//!
//! When the deployed node is the daemon host: local immutable evaluation and
//! build, target copy, root-mediated Home Manager profile set, then
//! target-user activation (unchanged since lojix 8.0.0).
//!
//! When the deployed node is any other node: local evaluation, the derivation
//! closure copied to the transport store, the build in that store, a GC root
//! on the target, a copy stage that only checks the output is there, then
//! the same activation.

use lojix::Payload as _;
use lojix::schema_runtime::DaemonRuntime as _;
use lojix::schema_runtime::{DeployDriving as _, RuntimeCore as _};
use lojix::{DeploymentLedger as _, DurableStore as _, GenerationLedger as _};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use lojix::Store;
use lojix::runtime_model as ordinary;
use lojix::runtime_model as meta;
use lojix::schema_runtime::{DeploySubmissionOutcome, RuntimeConfiguration, SchemaRuntime};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";
const DERIVATION: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-home-manager-generation.drv";
/// The node every fixture request deploys. A runtime on this node is the
/// daemon host case; the default test runtime runs on `daemon-host`.
const TARGET_NODE: &str = "beacon";

mod common;

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write fake command");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make fake executable");
}

fn fake_programs(directory: &Path, fail_copy: bool, fail_activation: bool) {
    fake_programs_with(
        directory,
        FakeFailures {
            copy: fail_copy,
            activation: fail_activation,
            ..FakeFailures::default()
        },
    );
}

/// Which fake effect exits nonzero. `copy` fails every `nix copy`;
/// `derivation_copy` only `nix copy --derivation` (a `.drv` that is not in
/// the daemon host's store); `presence` fails `nix path-info` (an output the
/// target does not hold).
#[derive(Default, Clone, Copy)]
struct FakeFailures {
    copy: bool,
    derivation_copy: bool,
    presence: bool,
    activation: bool,
}

fn fake_programs_with(directory: &Path, failures: FakeFailures) {
    fs::create_dir_all(directory).expect("create fake command directory");
    let copy_failure = if failures.copy {
        "copy) exit 41 ;;"
    } else if failures.derivation_copy {
        "copy) [ \"$2\" = --derivation ] && exit 43 ;;"
    } else {
        ""
    };
    let presence_failure = if failures.presence {
        "path-info) exit 44 ;;"
    } else {
        ""
    };
    write_executable(
        &directory.join("nix"),
        &format!(
            "#!/bin/sh\nset -eu\ndir=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\nprintf 'nix' >> \"$dir/commands\"\nfor arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$dir/commands\"; done\nprintf '\\n' >> \"$dir/commands\"\ncase \"$1\" in\n  flake) printf '%s\\n' '{{\"url\":\"{FLAKE}\",\"locked\":{{\"rev\":\"{REVISION}\"}}}}' ;;\n  hash) printf '%s\\n' 'sha256-transport-test=' ;;\n  eval) printf '%s\\n' '{DERIVATION}' ;;\n  build) printf '%s\\n' '{OUTPUT}' ;;\n  {copy_failure}\n  {presence_failure}\nesac\n"
        ),
    );
    let fail_activation = failures.activation;
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

fn user_environment_request(
    source: &Path,
    nix_store_uri: &str,
    ssh_destination: &str,
) -> meta::DeploySubmission {
    user_environment_request_with_secrets(
        source,
        nix_store_uri,
        ssh_destination,
        ordinary::SecretsInput::NoSecrets,
    )
}

fn user_environment_request_with_secrets(
    source: &Path,
    nix_store_uri: &str,
    ssh_destination: &str,
    secrets_input: ordinary::SecretsInput,
) -> meta::DeploySubmission {
    meta::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::from("alpha"),
        node_name: ordinary::NodeName::from(TARGET_NODE),
        user_name: ordinary::UserName::from("bird"),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        secrets_input,
        flake_reference: ordinary::FlakeReference::from(FLAKE),
        deployment_transport: transport(nix_store_uri, ssh_destination),
        deployment_input_mode: ordinary::DeploymentInputMode::Horizon,
        horizon_definition_option: Some(common::read_horizon(source)),
        deployment_output_selector: selector("packages.x86_64-linux.fixture-home"),
        activation_backend: ordinary::ActivationBackend::HomeManagerNixProfileV1,
        user_environment_action: meta::UserEnvironmentAction::ActivateNow,
        source_revision_policy: meta::SourceRevisionPolicy::RequireImmutable,
        optional_nix_builder_spec: None,
        extra_substituter_vector: Vec::new(),
    })
}

fn transport(nix_store_uri: &str, ssh_destination: &str) -> ordinary::DeploymentTransport {
    ordinary::DeploymentTransport {
        nix_store_uri: ordinary::NixStoreUri::from(nix_store_uri),
        ssh_destination: ordinary::SshDestination::from(ssh_destination),
    }
}

fn selector(value: &str) -> ordinary::DeploymentOutputSelector {
    ordinary::DeploymentOutputSelector::new(ordinary::FlakeAttribute::from(value))
}

/// A daemon running on the deployed node itself: the daemon-host path.
fn runtime(directory: &Path, programs: &Path) -> SchemaRuntime {
    runtime_on(directory, programs, TARGET_NODE)
}

/// A daemon running on another node: the target-store path.
fn remote_runtime(directory: &Path, programs: &Path) -> SchemaRuntime {
    runtime_on(directory, programs, "daemon-host")
}

fn runtime_on(directory: &Path, programs: &Path, daemon_host: &str) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.sema")).expect("open test store"));
    let configuration = Arc::new(
        RuntimeConfiguration::test_with_effect_program_directory(
            directory.join("generated-inputs"),
            programs.to_path_buf(),
        )
        .with_daemon_host(ordinary::NodeName::from(daemon_host)),
    );
    SchemaRuntime::with_store_and_configuration(store, configuration)
}

async fn submit_and_drive(
    engine: &mut SchemaRuntime,
    request: meta::DeploySubmission,
) -> meta::MetaEgress {
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
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://fixture-copy-a.invalid",
                "root@fixture-activate-a.invalid",
            ),
        )
        .await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
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
    assert!(commands[copy].contains("ssh-ng://fixture-copy-a.invalid"));
    assert!(commands[profile].contains("root@fixture-activate-a.invalid"));
    assert!(commands[activate].contains("root@fixture-activate-a.invalid"));
    assert!(commands[profile].contains("runuser --login --command"));
    assert!(commands[profile].contains(OUTPUT));
    assert!(commands[activate].contains("runuser --login --command"));
    assert!(commands[activate].contains(OUTPUT));

    assert!(
        commands.iter().all(|line| !line.contains("--derivation")
            && !line.contains("--store")
            && !line.starts_with("nix <path-info>")
            && !line.contains("gcroots")),
        "the daemon-host path must not realize in a target store: {commands:?}"
    );
    assert!(
        commands[copy].starts_with("nix <copy> <--substitute-on-destination>"),
        "{}",
        commands[copy]
    );

    let generations = engine
        .store()
        .matching_live_generations(|_| true)
        .expect("read current generation");
    assert_eq!(generations.len(), 1);
    let generation = &generations[0];
    assert_eq!(generation.closure_path.payload(), OUTPUT);
    assert_eq!(
        generation.source_revision_record.source_revision_policy,
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
    assert_eq!(generation.source_revision_record.string, REVISION);
    assert_eq!(
        generation.generation_slot,
        ordinary::GenerationSlot::Current
    );
    let secrets_flake = directory
        .path()
        .join("generated-inputs/alpha/beacon/user-environment/secrets/flake.nix");
    let secrets_text =
        fs::read_to_string(secrets_flake).expect("read generated empty secrets input");
    assert!(secrets_text.contains("sopsFiles = {"), "{secrets_text}");
    assert!(
        !secrets_text.contains(&source.display().to_string()),
        "public generated input must not retain the Horizon path or a private-input path"
    );
}

#[tokio::test]
async fn explicit_empty_secrets_directory_is_accepted_without_public_path_leakage() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let secrets = directory.path().join("caller-owned-secrets");
    fs::create_dir_all(&secrets).expect("create explicit empty secrets directory");
    let mut engine = runtime(directory.path(), &programs);

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request_with_secrets(
                &source,
                "ssh-ng://fixture-copy-secrets.invalid",
                "root@fixture-activate-secrets.invalid",
                ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::new(
                    secrets.display().to_string(),
                )),
            ),
        )
        .await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));
    let generated = directory
        .path()
        .join("generated-inputs/alpha/beacon/user-environment/secrets/flake.nix");
    let generated = fs::read_to_string(generated).expect("read generated secrets flake");
    assert!(generated.contains("sopsFiles = {"), "{generated}");
    assert!(
        !generated.contains(&secrets.display().to_string()),
        "the public generated input names no caller-owned private path"
    );
}

#[tokio::test]
async fn invalid_explicit_secrets_inputs_fail_before_effects() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("tempdir");
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let existing_file = directory.path().join("not-a-directory");
    fs::write(&existing_file, "fixture").expect("write non-directory");
    let existing_directory = directory.path().join("real-directory");
    fs::create_dir_all(&existing_directory).expect("create real directory");
    let link = directory.path().join("directory-link");
    symlink(&existing_directory, &link).expect("make symlink witness");

    let inputs = [
        ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::from("relative")),
        ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::new(
            directory.path().join("missing").display().to_string(),
        )),
        ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::new(
            existing_file.display().to_string(),
        )),
        ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::new(
            link.display().to_string(),
        )),
    ];
    for input in inputs {
        let programs = tempfile::tempdir().expect("program directory");
        fake_programs(programs.path(), false, false);
        let mut engine = runtime(directory.path(), programs.path());
        let outcome = submit_and_drive(
            &mut engine,
            user_environment_request_with_secrets(
                &source,
                "ssh-ng://fixture-copy-invalid-secrets.invalid",
                "root@fixture-activate-invalid-secrets.invalid",
                input,
            ),
        )
        .await;
        assert!(matches!(
            outcome,
            meta::MetaEgress::DeployTerminal(record)
                if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Failed(_)))
        ));
    }
}

#[tokio::test]
async fn second_arbitrary_transport_flow_preserves_both_request_values() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);
    let nix_store_uri = "ssh-ng://fixture-copy-b.invalid:2244?compress=true";
    let ssh_destination = "root@fixture-activate-b.invalid";

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request(&source, nix_store_uri, ssh_destination),
        )
        .await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));

    let commands = command_lines(&programs);
    let copy = commands
        .iter()
        .find(|line| line.starts_with("nix <copy>"))
        .expect("closure copy");
    let ssh: Vec<_> = commands
        .iter()
        .filter(|line| line.starts_with("ssh "))
        .collect();
    assert!(copy.contains(nix_store_uri), "{copy}");
    assert_eq!(ssh.len(), 2, "{ssh:?}");
    assert!(
        ssh.iter().all(|line| line.contains(ssh_destination)),
        "{ssh:?}"
    );
}

#[tokio::test]
async fn matched_user_remote_activation_runs_directly_without_runuser() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);
    let ssh_destination = "bird@fixture-activate-matched.invalid";

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request(&source, "ssh-ng://fixture-copy-matched.invalid", ssh_destination),
        )
        .await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));

    let ssh: Vec<_> = command_lines(&programs)
        .into_iter()
        .filter(|line| line.starts_with("ssh "))
        .collect();
    assert_eq!(ssh.len(), 2, "{ssh:?}");
    assert!(
        ssh.iter().all(|line| line.contains(ssh_destination)),
        "{ssh:?}"
    );
    assert!(ssh.iter().all(|line| !line.contains("runuser")), "{ssh:?}");
    assert!(
        ssh.iter().any(|line| line.contains("nix-env -p")),
        "{ssh:?}"
    );
    assert!(ssh.iter().any(|line| line.contains("/activate")), "{ssh:?}");
}

#[test]
fn mismatched_unprivileged_remote_login_is_rejected_before_effects() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);

    let outcome = engine.submit_deploy(user_environment_request(
        &source,
        "ssh-ng://fixture-copy-mismatch.invalid",
        "other@fixture-activate-mismatch.invalid",
    ));
    let DeploySubmissionOutcome::Rejected(rejected) = outcome else {
        panic!("mismatched unprivileged login must be rejected at admission");
    };
    assert!(matches!(
        rejected.into_payload().optional_deployment_terminal,
        Some(meta::DeploymentTerminal::Rejected(
            meta::DeploymentTerminalReason::InvalidDeploymentRouting
        ))
    ));
    assert!(engine.store().deploy_jobs().expect("job rows").is_empty());
    assert!(
        !programs.join("commands").exists(),
        "routing rejection must run no Nix, copy, profile, or activation command"
    );
}

#[tokio::test]
async fn copy_and_activation_failures_are_terminal_rejections() {
    for (fail_copy, fail_activation) in [(true, false), (false, true)] {
        let directory = tempfile::tempdir().expect("tempdir");
        let programs = directory.path().join("programs");
        fake_programs(&programs, fail_copy, fail_activation);
        let source = directory.path().join("horizon-definition.datom");
        common::write_hosted_pair(&source);
        let mut engine = runtime(directory.path(), &programs);

        match submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://fixture-copy-a.invalid",
                "root@fixture-activate-a.invalid",
            ),
        )
        .await
        {
            meta::MetaEgress::DeployTerminal(record) => {
                assert_eq!(
                    record.deployment_lifecycle,
                    meta::DeploymentLifecycle::Failed
                )
            }
            other => panic!("expected terminal rejection, got {other:?}"),
        }
        assert!(
            engine
                .store()
                .deploy_jobs()
                .expect("read durable job rows")
                .is_empty()
        );
    }
}

// ---- a node other than the daemon host realizes in its own store ----

fn position(commands: &[String], what: &str, predicate: impl Fn(&str) -> bool) -> usize {
    commands
        .iter()
        .position(|line| predicate(line))
        .unwrap_or_else(|| panic!("no {what} in {commands:?}"))
}

fn terminal_failure(outcome: meta::MetaEgress) -> meta::DeploymentFailure {
    let meta::MetaEgress::DeployTerminal(record) = outcome else {
        panic!("expected a terminal deployment, got {outcome:?}");
    };
    let Some(meta::DeploymentTerminal::Failed(failure)) = record.optional_deployment_terminal
    else {
        panic!("expected a failed deployment, got {record:?}");
    };
    failure
}

#[tokio::test]
async fn remote_node_builds_in_its_own_store_roots_the_output_and_skips_the_transfer() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = remote_runtime(directory.path(), &programs);
    let store_uri = "ssh-ng://root@fixture-target.invalid";
    let destination = "root@fixture-target.invalid";

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request(&source, store_uri, destination),
        )
        .await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));

    let commands = command_lines(&programs);
    let eval = position(&commands, "local eval", |line| {
        line.starts_with("nix <eval>")
    });
    let derivation_copy = position(&commands, "derivation copy", |line| {
        line.starts_with("nix <copy> <--derivation>")
    });
    let build = position(&commands, "target-store build", |line| {
        line.starts_with("nix <build>")
    });
    let root = position(&commands, "target GC root", |line| {
        line.starts_with("ssh ") && line.contains("nix-store --add-root")
    });
    let presence = position(&commands, "presence check", |line| {
        line.starts_with("nix <path-info>")
    });
    let profile = position(&commands, "profile set", |line| {
        line.starts_with("ssh ") && line.contains("nix-env -p")
    });
    let activate = position(&commands, "activation", |line| {
        line.starts_with("ssh ") && line.contains("/activate")
    });
    assert!(
        eval < derivation_copy
            && derivation_copy < build
            && build < root
            && root < presence
            && presence < profile
            && profile < activate,
        "{commands:?}"
    );

    assert!(
        !commands[eval].contains("--store") && !commands[eval].contains("ssh-ng"),
        "eval must stay in the daemon host's store: {}",
        commands[eval]
    );
    assert_eq!(
        commands[derivation_copy],
        format!("nix <copy> <--derivation> <--to> <{store_uri}> <{DERIVATION}>")
    );
    assert_eq!(
        commands[build],
        format!(
            "nix <build> <--no-link> <--print-out-paths> <--store> <{store_uri}> <{DERIVATION}^*>"
        )
    );
    assert!(commands[root].contains(destination), "{}", commands[root]);
    assert!(
        commands[root].contains(&format!(
            "nix-store --add-root /nix/var/nix/gcroots/lojix/daemon-host/generation-1 --realise {OUTPUT}"
        )),
        "{}",
        commands[root]
    );
    assert_eq!(
        commands[presence],
        format!("nix <path-info> <--store> <{store_uri}> <{OUTPUT}>")
    );
    assert!(
        commands
            .iter()
            .all(|line| !line.contains("--substitute-on-destination")),
        "the output is already on the target; nothing is transferred: {commands:?}"
    );
    assert!(
        commands
            .iter()
            .all(|line| !line.contains("--builders") && !line.contains("max-jobs")),
        "{commands:?}"
    );

    let generations = engine
        .store()
        .matching_live_generations(|_| true)
        .expect("read current generation");
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0].closure_path.payload(), OUTPUT);
}

#[tokio::test]
async fn remote_realize_leaves_the_rooted_closure_on_the_target_and_nothing_more() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = remote_runtime(directory.path(), &programs);
    let mut request = user_environment_request(
        &source,
        "ssh-ng://root@fixture-target.invalid",
        "root@fixture-target.invalid",
    );
    let meta::DeploySubmission::UserEnvironment(deployment) = &mut request else {
        unreachable!()
    };
    deployment.user_environment_action = meta::UserEnvironmentAction::Realize;

    assert!(matches!(
        submit_and_drive(&mut engine, request).await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));
    let commands = command_lines(&programs);
    let effects: Vec<_> = commands
        .iter()
        .filter(|line| !line.starts_with("nix <flake>") && !line.starts_with("nix <hash>"))
        .collect();
    assert_eq!(effects.len(), 4, "{effects:?}");
    assert!(effects[0].starts_with("nix <eval>"), "{effects:?}");
    assert!(
        effects[1].starts_with("nix <copy> <--derivation>"),
        "{effects:?}"
    );
    assert!(effects[2].starts_with("nix <build>") && effects[2].contains("<--store>"));
    assert!(effects[3].starts_with("ssh ") && effects[3].contains("nix-store --add-root"));
}

#[tokio::test]
async fn remote_node_ignores_the_request_builder() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = remote_runtime(directory.path(), &programs);
    let mut request = user_environment_request(
        &source,
        "ssh-ng://root@fixture-target.invalid",
        "root@fixture-target.invalid",
    );
    let meta::DeploySubmission::UserEnvironment(deployment) = &mut request else {
        unreachable!()
    };
    deployment.optional_nix_builder_spec =
        Some(ordinary::NixBuilderSpec::from("@/etc/nix/machines"));

    assert!(matches!(
        submit_and_drive(&mut engine, request).await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));
    let commands = command_lines(&programs);
    assert!(
        commands
            .iter()
            .all(|line| !line.contains("/etc/nix/machines")
                && !line.contains("--builders")
                && !line.contains("max-jobs")),
        "{commands:?}"
    );
    assert!(
        commands
            .iter()
            .any(|line| line.starts_with("nix <build>") && line.contains("<--store>")),
        "{commands:?}"
    );
}

#[tokio::test]
async fn daemon_host_builder_still_offloads_through_the_local_client() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = runtime(directory.path(), &programs);
    let mut request = user_environment_request(
        &source,
        "ssh-ng://fixture-copy-a.invalid",
        "root@fixture-activate-a.invalid",
    );
    let meta::DeploySubmission::UserEnvironment(deployment) = &mut request else {
        unreachable!()
    };
    deployment.optional_nix_builder_spec =
        Some(ordinary::NixBuilderSpec::from("@/etc/nix/machines"));

    assert!(matches!(
        submit_and_drive(&mut engine, request).await,
        meta::MetaEgress::DeployTerminal(record)
            if matches!(record.optional_deployment_terminal, Some(meta::DeploymentTerminal::Succeeded))
    ));
    let commands = command_lines(&programs);
    let build = position(&commands, "local build", |line| {
        line.starts_with("nix <build>")
    });
    assert!(
        commands[build].contains("<--builders> <@/etc/nix/machines>")
            && !commands[build].contains("<--store>"),
        "{}",
        commands[build]
    );
}

#[tokio::test]
async fn missing_local_derivation_fails_the_build_stage_before_any_remote_build() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs_with(
        &programs,
        FakeFailures {
            derivation_copy: true,
            ..FakeFailures::default()
        },
    );
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = remote_runtime(directory.path(), &programs);

    let failure = terminal_failure(
        submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://root@fixture-target.invalid",
                "root@fixture-target.invalid",
            ),
        )
        .await,
    );
    assert_eq!(
        failure.deployment_failure_stage,
        ordinary::DeploymentFailureStage::Build
    );
    let command = failure
        .optional_failure_evidence
        .and_then(|evidence| evidence.optional_failed_command)
        .expect("the failing command is recorded");
    let argv: Vec<_> = command
        .command_argument_vector
        .iter()
        .map(|argument| argument.payload().clone())
        .collect();
    assert_eq!(argv[..2], ["copy".to_string(), "--derivation".to_string()]);
    let commands = command_lines(&programs);
    assert!(
        commands
            .iter()
            .all(|line| !line.starts_with("nix <build>") && !line.starts_with("ssh ")),
        "{commands:?}"
    );
}

#[tokio::test]
async fn output_missing_on_the_target_fails_the_copy_stage_before_activation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs_with(
        &programs,
        FakeFailures {
            presence: true,
            ..FakeFailures::default()
        },
    );
    let source = directory.path().join("horizon-definition.datom");
    common::write_hosted_pair(&source);
    let mut engine = remote_runtime(directory.path(), &programs);

    let failure = terminal_failure(
        submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://root@fixture-target.invalid",
                "root@fixture-target.invalid",
            ),
        )
        .await,
    );
    assert_eq!(
        failure.deployment_failure_stage,
        ordinary::DeploymentFailureStage::CopyClosure
    );
    let commands = command_lines(&programs);
    assert!(
        commands
            .iter()
            .all(|line| !line.contains("nix-env -p") && !line.contains("/activate")),
        "{commands:?}"
    );
}
