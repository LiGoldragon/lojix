//! Engine-routing tests: drive the generated `NexusEngine::execute` runner
//! over `SchemaRuntime` for the non-IO paths (reads, subscription handshake,
//! and the GC-roots mutations). The deploy pipeline shells out to real `nix`,
//! so it is exercised only in a live environment, not here.

use std::collections::BTreeMap;
use std::sync::Arc;

use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::domain::DomainConfiguration;
use horizon_lib::io::Io;
use horizon_lib::machine::Machine;
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::NodeName;
use horizon_lib::proposal::{ClusterProposal, ClusterTrust, NodeProposal, NodePubKeys};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};
use lojix::Store;
use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema::sema;
use lojix::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use meta_signal_lojix::schema::lib as meta;
use nota::NotaEncode;
use signal_lojix::schema::lib as ordinary;

fn run(engine: &mut SchemaRuntime, input: nexus::RoutedMail) -> nexus::RoutedReply {
    let work = nexus::NexusWork::SignalArrived(input).with_origin_route(nexus::OriginRoute::new(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(async { engine.execute(work).await.into_root() }) {
        nexus::NexusAction::ReplyToSignal(output) => output,
        other => panic!("expected ReplyToSignal, got {other:?}"),
    }
}

fn ordinary_reply(output: nexus::RoutedReply) -> ordinary::Output {
    match output {
        nexus::RoutedReply::Ordinary(output) => output,
        nexus::RoutedReply::Meta(output) => panic!("expected ordinary, got {output:?}"),
    }
}

fn meta_reply(output: nexus::RoutedReply) -> meta::Output {
    match output {
        nexus::RoutedReply::Meta(output) => output,
        nexus::RoutedReply::Ordinary(output) => panic!("expected meta, got {output:?}"),
    }
}

#[test]
fn query_empty_live_set_returns_empty_listing() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Ordinary(ordinary::Input::Query(ordinary::QueryPayload::new(
        ordinary::Selection::ByNode(ordinary::NodeSelector {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            optional_generation_artifact: None,
        }),
    )));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Queried(listing) => {
            assert!(listing.payload().generation_vector.is_empty())
        }
        other => panic!("expected Queried, got {other:?}"),
    }
}

#[test]
fn query_by_event_log_returns_typed_deployment_events() {
    let mut engine = SchemaRuntime::new();
    engine
        .store()
        .append_event_log_entry(sema::EventLogEntry {
            event_log_position: ordinary::EventLogPosition::new(0),
            logged_event: sema::LoggedEvent::Deployment(ordinary::DeploymentPhaseEvent {
                deployment_identifier: ordinary::DeploymentIdentifier::new(7),
                generation_identifier: ordinary::GenerationIdentifier::new(9),
                cluster_name: ordinary::ClusterName::new("alpha"),
                node_name: ordinary::NodeName::new("node-1"),
                deployment_phase: ordinary::DeploymentPhase::Submitted,
                event_log_position: ordinary::EventLogPosition::new(0),
                optional_phase_detail: None,
                optional_source_revision_record: None,
            }),
        })
        .expect("append event");

    let input = nexus::RoutedMail::Ordinary(ordinary::Input::Query(ordinary::QueryPayload::new(
        ordinary::Selection::ByEventLog(ordinary::EventLogRange {
            from: ordinary::EventLogPosition::new(0),
            until: ordinary::EventLogPosition::new(1),
        }),
    )));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::DeploymentEventsQueried(page) => {
            let page = page.payload();
            assert_eq!(page.deployment_phase_event_vector.len(), 1);
            assert!(page.cache_retention_transition_event_vector.is_empty());
        }
        other => panic!("expected DeploymentEventsQueried, got {other:?}"),
    }
}

#[test]
fn watch_deployments_mints_subscription_token() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Ordinary(ordinary::Input::WatchDeployments(
        ordinary::WatchDeploymentsPayload::new(ordinary::DeploymentWatch {
            optional_deployment_identifier: None,
            optional_cluster_name: None,
            optional_node_name: None,
        }),
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Watching(opened) => {
            assert_eq!(*opened.payload().subscription_token.payload(), 1)
        }
        other => panic!("expected Watching, got {other:?}"),
    }
}

#[test]
fn check_host_key_material_reports_no_mismatches() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Ordinary(ordinary::Input::CheckHostKeyMaterial(
        ordinary::CheckHostKeyMaterialPayload::new(ordinary::KeyMaterialQuery {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            proposal_source: ordinary::ProposalSource::new("github:owner/repo"),
        }),
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::KeyMaterialChecked(report) => {
            assert_eq!(report.payload().node_name.payload(), "node-1");
            assert!(report.payload().key_material_mismatch_vector.is_empty());
        }
        other => panic!("expected KeyMaterialChecked, got {other:?}"),
    }
}

#[test]
fn pin_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input =
        nexus::RoutedMail::Meta(meta::Input::Pin(meta::PinPayload::new(meta::PinRequest {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            generation_identifier: ordinary::GenerationIdentifier::new(42),
            pin_label: ordinary::PinLabel::new("keep"),
        })));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::PinRejected(_) => {}
        other => panic!("expected PinRejected for unknown generation, got {other:?}"),
    }
}

#[test]
fn retire_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Meta(meta::Input::Retire(meta::RetirePayload::new(
        meta::RetireRequest {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            generation_identifier: ordinary::GenerationIdentifier::new(7),
        },
    )));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::RetireRejected(_) => {}
        other => panic!("expected RetireRejected for unknown generation, got {other:?}"),
    }
}

/// A host deploy submission with the given `build_attribute` and action.
fn host_deployment(
    optional_flake_attribute: Option<&str>,
    action: ordinary::HostDeployAction,
) -> meta::HostDeployment {
    meta::HostDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("node-1"),
        host_composition: ordinary::HostComposition::BaseHost,
        proposal_source: ordinary::ProposalSource::new("/dev/null"),
        flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
        host_deploy_action: action,
        source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
        optional_builder: None,
        extra_substituter_vector: Vec::new(),
        optional_flake_attribute: optional_flake_attribute.map(meta::FlakeAttribute::new),
    }
}

fn deploy_rejection_reason(output: nexus::RoutedReply) -> meta::DeployRejectionReason {
    match meta_reply(output) {
        meta::Output::DeployRejected(rejected) => rejected.payload().deploy_rejection_reason,
        other => panic!("expected DeployRejected, got {other:?}"),
    }
}

// ---- Deploy guard: every declared action now enters the effect pipeline
// (S4a opened the activating actions — host SetBootProfile/ActivateNow/TestActivation/ScheduleBootOnce, user-environment
// Profile/Activate — by making copy + activate target-safe). These tests drive
// the cursor with intentionally bogus proposal sources, so an opened action
// reaches the pipeline and fails at the IO stage with ProposalSourceUnreachable
// rather than being rejected up front as UnsupportedDeployAction.

#[test]
fn activating_deploy_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Meta(meta::Input::Deploy(meta::DeployPayload::new(
        meta::DeployRequest::Host(host_deployment(
            None,
            ordinary::HostDeployAction::ActivateNow,
        )),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
fn user_environment_activate_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Meta(meta::Input::Deploy(meta::DeployPayload::new(
        meta::DeployRequest::UserEnvironment(meta::UserEnvironmentDeployment {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            user_name: ordinary::UserName::new("li"),
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
            user_environment_action: meta::UserEnvironmentAction::ActivateNow,
            source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
            optional_builder: None,
            extra_substituter_vector: Vec::new(),
        }),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
fn production_deploy_without_build_attribute_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Meta(meta::Input::Deploy(meta::DeployPayload::new(
        meta::DeployRequest::Host(host_deployment(None, ordinary::HostDeployAction::Realize)),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
fn user_environment_realize_enters_effect_pipeline() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::RoutedMail::Meta(meta::Input::Deploy(meta::DeployPayload::new(
        meta::DeployRequest::UserEnvironment(meta::UserEnvironmentDeployment {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            user_name: ordinary::UserName::new("li"),
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
            user_environment_action: meta::UserEnvironmentAction::Realize,
            source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
            optional_builder: None,
            extra_substituter_vector: Vec::new(),
        }),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::ProposalSourceUnreachable,
    );
}

#[test]
#[ignore = "runs real `nix flake metadata` and `nix eval`; cheap but external"]
fn production_eval_materializes_horizon_inputs_and_returns_deploy_accepted() {
    let directory = tempfile::tempdir().expect("tempdir");
    let cluster_path = directory.path().join("cluster.nota");
    std::fs::write(&cluster_path, fixture_cluster_proposal().to_nota()).expect("write cluster");
    let flake_directory = directory.path().join("flake");
    FixtureFlake::new(flake_directory).write();

    // The fixture node is the daemon host for this non-production smoke test,
    // so evaluation stays in the local store. A synthetic fixture must not
    // require public DNS for `node-1.alpha.criome` merely to prove Horizon
    // materialization and the generated override inputs.
    let configuration = lojix::DaemonConfiguration {
        ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        owner_socket_path: directory.path().join("owner.sock").display().to_string(),
        owner_socket_mode: 0o660,
        state_directory_path: directory.path().join("state").display().to_string(),
        daemon_host: "node-1".to_string(),
        test_defaults: None,
    };
    let store =
        Arc::new(Store::open(directory.path().join("lojix.sema")).expect("open fixture store"));
    let mut engine = SchemaRuntime::with_store_and_configuration(
        store,
        Arc::new(RuntimeConfiguration::from_daemon_configuration(
            &configuration,
        )),
    );
    let mut deployment = host_deployment(None, ordinary::HostDeployAction::Evaluate);
    deployment.proposal_source = ordinary::ProposalSource::new(cluster_path.display().to_string());
    deployment.flake_reference =
        ordinary::FlakeReference::new(format!("path:{}", directory.path().join("flake").display()));
    let input = nexus::RoutedMail::Meta(meta::Input::Deploy(meta::DeployPayload::new(
        meta::DeployRequest::Host(deployment),
    )));

    match meta_reply(run(&mut engine, input)) {
        meta::Output::DeployAccepted(accepted) => {
            assert_eq!(*accepted.payload().deployment_identifier.payload(), 1);
            assert_eq!(
                *accepted.payload().database_marker.commit_sequence.payload(),
                0,
                "the first durable commit uses the zero-based commit identity",
            );
        }
        other => panic!("expected DeployAccepted, got {other:?}"),
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

        // Resolve the synthetic fixture before lojix records its immutable
        // path identity. Otherwise `nix flake metadata` creates flake.lock
        // after hashing the path and the subsequent eval correctly rejects
        // the now-mutated source with a NAR hash mismatch.
        let status = std::process::Command::new("nix")
            .args(["flake", "lock"])
            .arg(format!("path:{}", self.directory.display()))
            .status()
            .expect("run nix flake lock for fixture");
        assert!(status.success(), "lock the synthetic fixture flake");
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
        domain_configuration: DomainConfiguration::default(),
    }
}

#[test]
fn routed_contact_preserves_origin_through_reply_mapping() {
    let origin = nexus::OriginRoute::new(41);
    let routed_mail =
        nexus::RoutedMail::Ordinary(ordinary::Input::Unwatch(ordinary::UnwatchPayload::new(
            ordinary::SubscriptionClose::new(ordinary::SubscriptionToken::new(9)),
        )));
    let routed_reply = nexus::NexusWork::SignalArrived(routed_mail)
        .with_origin_route(origin)
        .map_root(|_| {
            nexus::NexusAction::ReplyToSignal(nexus::RoutedReply::Ordinary(
                ordinary::Output::Unwatched(ordinary::UnwatchedPayload::new(
                    ordinary::SubscriptionClosed::new(ordinary::SubscriptionToken::new(9)),
                )),
            ))
        });
    assert_eq!(routed_reply.origin_route().payload(), 41);
    assert!(matches!(
        routed_reply.into_root(),
        nexus::NexusAction::ReplyToSignal(nexus::RoutedReply::Ordinary(_))
    ));
}
