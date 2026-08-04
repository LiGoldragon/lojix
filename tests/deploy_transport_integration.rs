//! Focused deploy-transport witnesses using hermetic fake Nix and SSH programs.
//!
//! These tests execute the same submit/drive pipeline as the daemon-owned job
//! actor. They prove the production order without opening a network connection:
//! local immutable evaluation/build, target copy, root-mediated Home Manager
//! profile set, then target-user activation. They also prove bounded timeout
//! cancellation reaches a child process group and leaves a terminal job row.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dotos::DotosEncode;
use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::domain::DomainConfiguration;
use horizon_lib::io::Io;
use horizon_lib::machine::Machine;
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::{NodeName as HorizonNodeName, UserName as HorizonUserName};
use horizon_lib::proposal::{ClusterProposal, ClusterTrust, NodeProposal, NodePubKeys};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};
use lojix::Store;
use lojix::schema::sema as ordinary;
use lojix::schema::sema as meta;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const FLAKE: &str =
    "github:fixture-owner/fixture-flake?rev=0123456789abcdef0123456789abcdef01234567";
const OUTPUT: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-home-manager-generation";

fn proposal_node(species: MachineSpecies, super_node: Option<&str>) -> NodeProposal {
    NodeProposal {
        species: NodeSpecies::EdgeTesting,
        size: Magnitude::Large,
        trust: Magnitude::Max,
        machine: Machine {
            species,
            arch: Some(Arch::X86_64),
            cores: 4,
            model: None,
            mother_board: None,
            super_node: super_node.map(|name| HorizonNodeName::try_new(name).expect("node name")),
            super_user: super_node
                .map(|_| HorizonUserName::try_new("operator").expect("user name")),
            chip_gen: None,
            ram_gb: None,
            disk_gb: None,
            location: None,
            super_nodes: Vec::new(),
        },
        io: Io {
            keyboard: Keyboard::Qwerty,
            bootloader: Bootloader::Uefi,
            disks: BTreeMap::new(),
            swap_devices: Vec::new(),
            compressed_swap: None,
        },
        pub_keys: NodePubKeys {
            ssh: SshPubKey::try_new("AAA=").expect("ssh key"),
            nix: Some(
                NixPubKey::try_new("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                    .expect("nix key"),
            ),
            yggdrasil: Some(horizon_lib::proposal::YggPubKeyEntry {
                pub_key: YggPubKey::try_new("a".repeat(64)).expect("ygg key"),
                address: YggAddress::try_new("200::1").expect("ygg address"),
                subnet: YggSubnet::try_new("300:ca41:6b12:fba").expect("ygg subnet"),
            }),
        },
        link_local_ips: Vec::new(),
        node_ip: None,
        wireguard_pub_key: None,
        nordvpn: false,
        wifi_cert: false,
        wireguard_untrusted_proxies: Vec::new(),
        wants_printing: false,
        wants_hw_video_accel: false,
        router_interfaces: None,
        online: None,
        services: Vec::new(),
    }
}

fn fixture_proposal() -> ClusterProposal {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        HorizonNodeName::try_new("atlas").expect("node name"),
        proposal_node(MachineSpecies::Metal, None),
    );
    nodes.insert(
        HorizonNodeName::try_new("beacon").expect("node name"),
        proposal_node(MachineSpecies::Pod, Some("atlas")),
    );
    ClusterProposal {
        nodes,
        users: BTreeMap::new(),
        domains: BTreeMap::new(),
        trust: ClusterTrust {
            cluster: Magnitude::Max,
            clusters: BTreeMap::new(),
            nodes: BTreeMap::new(),
            users: BTreeMap::new(),
        },
        domain_configuration: DomainConfiguration::default(),
    }
}

fn write_fixture_proposal(path: &Path) {
    fs::write(path, fixture_proposal().to_dotos()).expect("write Dotos fixture proposal");
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
    meta::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("beacon"),
        user_name: ordinary::UserName::new("bird"),
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
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
    let source = directory.path().join("datom.dotos");
    write_fixture_proposal(&source);
    let mut engine = runtime(directory.path(), &programs, Duration::from_secs(2));

    assert!(matches!(
        submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://fixture-copy-a.invalid",
                "fixture-a@fixture-activate-a.invalid",
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
    assert!(commands[profile].contains("fixture-a@fixture-activate-a.invalid"));
    assert!(commands[activate].contains("fixture-a@fixture-activate-a.invalid"));
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
}

#[tokio::test]
async fn second_arbitrary_transport_flow_preserves_both_request_values() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fake_programs(&programs, false, false);
    let source = directory.path().join("datom.dotos");
    write_fixture_proposal(&source);
    let mut engine = runtime(directory.path(), &programs, Duration::from_secs(2));
    let nix_store_uri = "ssh-ng://fixture-copy-b.invalid:2244?compress=true";
    let ssh_destination = "fixture-b@fixture-activate-b.invalid";

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
async fn copy_and_activation_failures_are_terminal_rejections() {
    for (fail_copy, fail_activation) in [(true, false), (false, true)] {
        let directory = tempfile::tempdir().expect("tempdir");
        let programs = directory.path().join("programs");
        fake_programs(&programs, fail_copy, fail_activation);
        let source = directory.path().join("datom.dotos");
        write_fixture_proposal(&source);
        let mut engine = runtime(directory.path(), &programs, Duration::from_secs(2));

        match submit_and_drive(
            &mut engine,
            user_environment_request(
                &source,
                "ssh-ng://fixture-copy-a.invalid",
                "fixture-a@fixture-activate-a.invalid",
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

#[tokio::test]
async fn timeout_kills_the_whole_session_group_reaps_and_rejects() {
    let directory = tempfile::tempdir().expect("tempdir");
    let programs = directory.path().join("programs");
    fs::create_dir_all(&programs).expect("create fake command directory");
    let source = directory.path().join("datom.dotos");
    write_fixture_proposal(&source);
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
    let request = meta::DeploySubmission::Host(meta::HostDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("beacon"),
        host_composition: ordinary::HostComposition::BaseHost,
        proposal_source: ordinary::ProposalSource::new(source.display().to_string()),
        flake_reference: ordinary::FlakeReference::new(FLAKE),
        deployment_transport: transport(
            "ssh-ng://fixture-copy-b.invalid",
            "fixture-b@fixture-activate-b.invalid",
        ),
        deployment_input_mode: ordinary::DeploymentInputMode::Direct,
        deployment_output_selector: selector("checks.fixture-timeout"),
        activation_backend: ordinary::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: ordinary::HostDeployAction::Evaluate,
        source_revision_policy: meta::SourceRevisionPolicy::RequireImmutable,
        optional_nix_builder_spec: None,
        extra_substituter_vector: Vec::new(),
    });

    match submit_and_drive(&mut engine, request).await {
        meta::MetaEgress::DeployTerminal(record)
            if matches!(
                record.deployment_lifecycle,
                meta::DeploymentLifecycle::Failed
            ) => {}
        other => panic!("timeout must terminally reject, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        programs.join("descendant-terminated").exists(),
        "TERM must reach the descendant in the timed-out command's process group"
    );
    assert!(
        engine
            .store()
            .deploy_jobs()
            .expect("read durable job rows")
            .is_empty()
    );
}
