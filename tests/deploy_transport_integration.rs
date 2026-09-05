//! Focused deploy-transport witnesses using hermetic fake Nix and SSH programs.
//!
//! These tests execute the same submit/drive pipeline as the daemon-owned job
//! actor. They prove the production order without opening a network connection:
//! local immutable evaluation/build, target copy, root-mediated Home Manager
//! profile set, then target-user activation.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use datom_codec::Textualizable;
use horizon_lib::*;
use lojix::Store;
use lojix::runtime_model as ordinary;
use lojix::runtime_model as meta;
use lojix::schema_runtime::{DeploySubmissionOutcome, RuntimeConfiguration, SchemaRuntime};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";

fn write_fixture_proposal(path: &Path) {
    fn text(value: &str) -> protos::Text {
        protos::Text::try_from(value).expect("fixture text")
    }
    let hardware = || Hardware(4.into(), None, None, None, None, None);
    let node = |name: &str, machine| {
        NodeDefinition(
            text(name),
            NodeVariant::Live(LiveDefinition()),
            Magnitude::Max,
            Magnitude::Max,
            machine,
            NodeEnvironment(Keyboard::Qwerty, None),
            NodeNetwork(vec![], None, None, vec![], None),
            NodeKeys(text("ssh-ed25519 AAAAfixture"), None, None),
            Some(true),
            vec![],
        )
    };
    let atlas = node(
        "atlas",
        MachineDefinition::Metal(Architecture::X86_64, hardware()),
    );
    // A current `VirtualMachine` is the hosted guest replacement for the old
    // Pod fixture; it remains a VM with an explicit cluster host.
    let beacon = node(
        "beacon",
        MachineDefinition::VirtualMachine(
            VirtualMachineHost::Cluster(text("atlas"), vec![], Some(text("operator")), None),
            hardware(),
            Some(20.into()),
        ),
    );
    let definition = HorizonDefinition(
        HorizonConfiguration(vec![], DomainConfiguration(text("criome"), vec![])),
        ClusterDefinition(
            text("alpha"),
            vec![atlas, beacon],
            vec![],
            vec![],
            vec![],
            ClusterTrust(Magnitude::Max, vec![], vec![], vec![]),
        ),
    );
    fs::write(path, definition.textualize()).expect("write HorizonDefinition");
}

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
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("beacon"),
        user_name: ordinary::UserName::new("bird"),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        secrets_input,
        flake_reference: ordinary::FlakeReference::new(FLAKE),
        deployment_transport: transport(nix_store_uri, ssh_destination),
        deployment_input_mode: ordinary::DeploymentInputMode::Horizon,
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
        nix_store_uri: ordinary::NixStoreUri::new(nix_store_uri),
        ssh_destination: ordinary::SshDestination::new(ssh_destination),
    }
}

fn selector(value: &str) -> ordinary::DeploymentOutputSelector {
    ordinary::DeploymentOutputSelector::new(ordinary::FlakeAttribute::new(value))
}

fn runtime(directory: &Path, programs: &Path) -> SchemaRuntime {
    let store = Arc::new(Store::open(directory.join("lojix.sema")).expect("open test store"));
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
        directory.join("generated-inputs"),
        programs.to_path_buf(),
    ));
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
    write_fixture_proposal(&source);
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
    assert!(commands[copy].contains("ssh-ng://root@fixture-copy-a.invalid"));
    assert!(commands[profile].contains("root@fixture-activate-a.invalid"));
    assert!(commands[activate].contains("root@fixture-activate-a.invalid"));
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
    write_fixture_proposal(&source);
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
    write_fixture_proposal(&source);
    let existing_file = directory.path().join("not-a-directory");
    fs::write(&existing_file, "fixture").expect("write non-directory");
    let existing_directory = directory.path().join("real-directory");
    fs::create_dir_all(&existing_directory).expect("create real directory");
    let link = directory.path().join("directory-link");
    symlink(&existing_directory, &link).expect("make symlink witness");

    let inputs = [
        ordinary::SecretsInput::SecretsDirectory(ordinary::SecretsDirectory::new("relative")),
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
    write_fixture_proposal(&source);
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
    assert!(
        copy.contains("ssh-ng://root@fixture-copy-b.invalid:2244?compress=true"),
        "{copy}"
    );
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
    write_fixture_proposal(&source);
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
    write_fixture_proposal(&source);
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
        write_fixture_proposal(&source);
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
