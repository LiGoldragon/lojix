//! Engine-routing tests: drive the generated `NexusEngine::execute` runner
//! over `SchemaRuntime` for the non-IO paths (reads, subscription handshake,
//! and the GC-roots mutations). The deploy pipeline shells out to real `nix`,
//! so it is exercised only in a live environment, not here.

use std::collections::BTreeMap;

use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::io::Io;
use horizon_lib::machine::Machine;
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::NodeName;
use horizon_lib::proposal::{ClusterProposal, ClusterTrust, NodeProposal, NodePubKeys};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};
use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::SchemaRuntime;
use meta_signal_lojix::schema::lib as meta;
use nota_next::NotaEncode;
use signal_lojix::schema::lib as ordinary;

fn run(engine: &mut SchemaRuntime, input: nexus::SignalInput) -> nexus::SignalOutput {
    let work = nexus::NexusWork::SignalArrived(input).with_origin_route(nexus::OriginRoute(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(async { engine.execute(work).await.into_root() }) {
        nexus::NexusAction::ReplyToSignal(output) => output,
        other => panic!("expected ReplyToSignal, got {other:?}"),
    }
}

fn ordinary_reply(output: nexus::SignalOutput) -> ordinary::Output {
    match output {
        nexus::SignalOutput::OrdinaryOutput(output) => output,
        nexus::SignalOutput::MetaOutput(output) => panic!("expected ordinary, got {output:?}"),
    }
}

fn meta_reply(output: nexus::SignalOutput) -> meta::Output {
    match output {
        nexus::SignalOutput::MetaOutput(output) => output,
        nexus::SignalOutput::OrdinaryOutput(output) => panic!("expected meta, got {output:?}"),
    }
}

#[test]
fn query_empty_live_set_returns_empty_listing() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(
        ordinary::Selection::ByNode(ordinary::NodeSelector {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            kind: None,
        }),
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Queried(listing) => assert!(listing.generations.is_empty()),
        other => panic!("expected Queried, got {other:?}"),
    }
}

#[test]
fn watch_deployments_mints_subscription_token() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::WatchDeployments(
        ordinary::DeploymentWatch {
            deployment: None,
            cluster: None,
            node: None,
        },
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Watching(opened) => assert_eq!(opened.subscription_token, 1),
        other => panic!("expected Watching, got {other:?}"),
    }
}

#[test]
fn check_host_key_material_reports_no_mismatches() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::CheckHostKeyMaterial(
        ordinary::KeyMaterialQuery {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            source: "github:owner/repo".to_string(),
        },
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::KeyMaterialChecked(report) => {
            assert_eq!(report.node_name, "node-1");
            assert!(report.mismatches.is_empty());
        }
        other => panic!("expected KeyMaterialChecked, got {other:?}"),
    }
}

#[test]
fn pin_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Pin(meta::PinRequest {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        generation_identifier: 42,
        pin_label: "keep".to_string(),
    }));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::PinRejected(_) => {}
        other => panic!("expected PinRejected for unknown generation, got {other:?}"),
    }
}

#[test]
fn retire_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Retire(meta::RetireRequest {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        generation_identifier: 7,
    }));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::RetireRejected(_) => {}
        other => panic!("expected RetireRejected for unknown generation, got {other:?}"),
    }
}

/// A System deploy submission with the given `build_attribute` and action.
fn system_deployment(
    build_attribute: Option<&str>,
    action: ordinary::SystemAction,
) -> meta::SystemDeployment {
    meta::SystemDeployment {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        deployment_kind: ordinary::DeploymentKind::OsOnly,
        source: "/dev/null".to_string(),
        flake: "github:owner/repo".to_string(),
        system_action: action,
        builder: None,
        substituters: Vec::new(),
        build_attribute: build_attribute.map(str::to_string),
    }
}

fn deploy_rejection_reason(output: nexus::SignalOutput) -> meta::DeployRejectionReason {
    match meta_reply(output) {
        meta::Output::DeployRejected(rejected) => rejected.deploy_rejection_reason,
        other => panic!("expected DeployRejected, got {other:?}"),
    }
}

// ---- Deploy guard: activating actions remain rejected because copy/activate
// is not target-safe yet. Production System/Home build paths are no longer
// rejected as unsupported; they proceed into the effect pipeline and fail here
// only because these tests use intentionally bogus proposal sources.

#[test]
fn activating_deploy_is_rejected_until_activate_lands() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::System(
        system_deployment(Some("dune-nspawn-toplevel"), ordinary::SystemAction::Switch),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::UnsupportedDeployAction,
    );
}

#[test]
fn production_deploy_without_build_attribute_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::System(
        system_deployment(None, ordinary::SystemAction::Build),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
fn home_build_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::Home(
        meta::HomeDeployment {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            user_name: "li".to_string(),
            source: "/dev/null".to_string(),
            flake: "github:owner/repo".to_string(),
            home_mode: meta::HomeMode::Build,
            builder: None,
            substituters: Vec::new(),
        },
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
#[ignore = "runs real `nix flake metadata` and `nix eval`; cheap but external"]
fn production_eval_materializes_horizon_inputs_and_returns_deployed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let cluster_path = directory.path().join("cluster.nota");
    std::fs::write(&cluster_path, fixture_cluster_proposal().to_nota()).expect("write cluster");
    let flake_directory = directory.path().join("flake");
    FixtureFlake::new(flake_directory).write();

    let mut engine = SchemaRuntime::new();
    let mut deployment = system_deployment(None, ordinary::SystemAction::Eval);
    deployment.source = cluster_path.display().to_string();
    deployment.flake = format!("path:{}", directory.path().join("flake").display());
    let input =
        nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::System(deployment)));

    match meta_reply(run(&mut engine, input)) {
        meta::Output::Deployed(accepted) => {
            assert_eq!(accepted.deployment_identifier, 1);
            assert_eq!(accepted.database_marker.commit_sequence, 1);
        }
        other => panic!("expected Deployed, got {other:?}"),
    }
}

struct FixtureFlake {
    directory: std::path::PathBuf,
}

impl FixtureFlake {
    fn new(directory: std::path::PathBuf) -> Self {
        Self { directory }
    }

    fn write(&self) {
        self.write_stub_input("horizon", "horizon = { node = { name = \"stub\"; }; };");
        self.write_stub_input("system", "system = \"x86_64-linux\";");
        self.write_stub_input(
            "deployment",
            "deployment = { includeHome = false; includeAllFirmware = true; };",
        );
        std::fs::create_dir_all(&self.directory).expect("flake dir");
        std::fs::write(
            self.directory.join("flake.nix"),
            r#"{
  inputs.horizon.url = "path:./horizon";
  inputs.system.url = "path:./system";
  inputs.deployment.url = "path:./deployment";
  outputs = inputs: {
    nixosConfigurations.target.config.system.build.toplevel = derivation {
      name = "lojix-materialization-eval";
      system = inputs.system.system;
      builder = "/bin/sh";
      args = [ "-c" "echo ok > $out" ];
    };
  };
}
"#,
        )
        .expect("fixture flake");
    }

    fn write_stub_input(&self, name: &str, output: &str) {
        let directory = self.directory.join(name);
        std::fs::create_dir_all(&directory).expect("stub dir");
        std::fs::write(
            directory.join("flake.nix"),
            format!("{{ outputs = _: {{ {output} }}; }}\n"),
        )
        .expect("stub flake");
    }
}

fn fixture_cluster_proposal() -> ClusterProposal {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        NodeName::try_new("node-1").unwrap(),
        NodeProposal {
            species: NodeSpecies::EdgeTesting,
            size: Magnitude::Large,
            trust: Magnitude::Max,
            machine: Machine {
                species: MachineSpecies::Metal,
                arch: Some(Arch::X86_64),
                cores: 4,
                model: None,
                mother_board: None,
                super_node: None,
                super_user: None,
                chip_gen: None,
                ram_gb: None,
            },
            io: Io {
                keyboard: Keyboard::Qwerty,
                bootloader: Bootloader::Uefi,
                disks: BTreeMap::new(),
                swap_devices: Vec::new(),
                compressed_swap: None,
            },
            pub_keys: NodePubKeys {
                ssh: SshPubKey::try_new("AAA=").unwrap(),
                nix: Some(
                    NixPubKey::try_new("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap(),
                ),
                yggdrasil: Some(horizon_lib::proposal::YggPubKeyEntry {
                    pub_key: YggPubKey::try_new("a".repeat(64)).unwrap(),
                    address: YggAddress::try_new("200::1").unwrap(),
                    subnet: YggSubnet::try_new("300:ca41:6b12:fba").unwrap(),
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
        },
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
    }
}
